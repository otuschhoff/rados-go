package objecter

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	wire "github.com/otuschhoff/rados-go/internal/encoding"
	"github.com/otuschhoff/rados-go/internal/maps"
	"github.com/otuschhoff/rados-go/internal/msgr"
	"github.com/otuschhoff/rados-go/internal/osd"
	"github.com/otuschhoff/rados-go/internal/protocol"
)

type fakeRawHashRouter struct {
	mu    sync.Mutex
	route Route
}

func (router *fakeRawHashRouter) Route(Target) (Route, error) {
	return Route{}, errors.New("ordinary object route used for PGNLS")
}

func (router *fakeRawHashRouter) RouteRawHash(poolID int64, hash uint32) (Route, error) {
	router.mu.Lock()
	defer router.mu.Unlock()
	if router.route.PG.Pool != uint64(poolID) || router.route.RawHash != hash {
		return Route{}, errors.New("raw hash route mismatch")
	}
	return router.route, nil
}

func (router *fakeRawHashRouter) set(route Route) {
	router.mu.Lock()
	router.route = route
	router.mu.Unlock()
}

func TestPGNLSUsesRawHashRouteAndRetriesEAGAIN(t *testing.T) {
	route0 := testRoute(t, 10, 0, "192.0.2.10:6800")
	route1 := testRoute(t, 11, 1, "192.0.2.11:6800")
	const cursorHash = uint32(8)
	router := &fakeRawHashRouter{route: route0}
	refreshes := 0
	source := &fakeMapSource{refresh: func() {
		refreshes++
		router.set(route1)
	}}
	var requests []msgr.Message
	created := make(map[int32]*fakeSession)
	client := newTestClient(t, source, router, func(id int32, _ protocol.EntityAddrVec) (session, error) {
		active := &fakeSession{submit: func(_ context.Context, request msgr.Message) (msgr.Message, error) {
			requests = append(requests, request)
			if len(requests) == 1 {
				return testPGNLSReply(t, route0, 0, -11, nil), nil
			}
			page := encodePGNLSPageForTest(t,
				osd.HObject{Object: "next", Snapshot: osd.NoSnap, Hash: 9, Pool: 7},
				[]osd.ListEntry{{Namespace: "ns", Object: "object", Locator: "key"}},
			)
			return testPGNLSReply(t, route1, 1, 0, page), nil
		}}
		created[id] = active
		return active, nil
	})
	defer client.Close()

	cursor := osd.HObject{Snapshot: osd.NoSnap, Hash: cursorHash, Pool: 7}
	page, err := client.PGNLS(context.Background(), 7, "ns", cursor, 3)
	if err != nil {
		t.Fatal(err)
	}
	want := osd.ListPage{
		Next:    osd.HObject{Object: "next", Snapshot: osd.NoSnap, Hash: 9, Pool: 7},
		Entries: []osd.ListEntry{{Namespace: "ns", Object: "object", Locator: "key"}},
	}
	if !reflect.DeepEqual(page, want) || refreshes != 1 || len(requests) != 2 || len(created) != 2 || created[0].stop {
		t.Fatalf("page=%+v refreshes=%d requests=%d sessions=%d oldStopped=%t", page, refreshes, len(requests), len(created), created[0].stop)
	}
	assertPGNLSRequest(t, requests[0], route0, cursor, "ns", 3, route0.Epoch, 0)
	assertPGNLSRequest(t, requests[1], route1, cursor, "ns", 3, route0.Epoch, 1)
}

func TestPGNLSWaitsForPrimaryToRecover(t *testing.T) {
	const cursorHash = uint32(8)
	unavailable := Route{Epoch: 10, PG: maps.PG{Pool: 7}, RawHash: cursorHash, Primary: -1}
	recovered := testRoute(t, 11, 1, "192.0.2.11:6800")
	recovered.RawHash = cursorHash
	router := &fakeRawHashRouter{route: unavailable}
	source := &fakeMapSource{refresh: func() { router.set(recovered) }}
	var request msgr.Message
	client := newTestClient(t, source, router, func(int32, protocol.EntityAddrVec) (session, error) {
		return &fakeSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
			request = message
			page := encodePGNLSPageForTest(t, osd.HObject{Object: "next", Snapshot: osd.NoSnap, Hash: 9, Pool: 7}, nil)
			return testPGNLSReply(t, recovered, 0, 0, page), nil
		}}, nil
	})
	defer client.Close()

	cursor := osd.HObject{Snapshot: osd.NoSnap, Hash: cursorHash, Pool: 7}
	if _, err := client.PGNLS(context.Background(), 7, "ns", cursor, 3); err != nil {
		t.Fatal(err)
	}
	assertPGNLSRequest(t, request, recovered, cursor, "ns", 3, recovered.Epoch, 0)
}

func TestValidateEnumerationPage(t *testing.T) {
	start := osd.HObject{Object: "middle", Snapshot: osd.NoSnap, Pool: 7}
	next := osd.HObject{Snapshot: osd.NoSnap, Hash: 1, Pool: 7}
	entry := osd.HObject{Object: "object", Snapshot: osd.NoSnap, Hash: 2, Pool: 7}
	if err := validateEnumerationPage(7, start, next, []osd.HObject{entry}); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		next    osd.HObject
		entries []osd.HObject
	}{
		{name: "no progress", next: start},
		{name: "wrong pool", next: osd.HObject{Snapshot: osd.NoSnap, Hash: 1, Pool: 8}},
		{name: "entry at next", next: next, entries: []osd.HObject{next}},
		{name: "entry before start", next: next, entries: []osd.HObject{{Object: "before", Snapshot: osd.NoSnap, Pool: 7}}},
		{name: "unordered", next: osd.HObject{Snapshot: osd.NoSnap, Hash: 3, Pool: 7}, entries: []osd.HObject{entry, {Snapshot: osd.NoSnap, Hash: 4, Pool: 7}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := validateEnumerationPage(7, start, test.next, test.entries); !errors.Is(err, osd.ErrMalformedReply) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestEnumeratePagesAccumulatesAcrossEmptyPG(t *testing.T) {
	start := enumerationCursor(0, "")
	middle := enumerationCursor(2, "")
	finish := osd.HObject{Max: true}
	calls := 0
	result, err := enumeratePages(7, start, finish, 2, func(cursor osd.HObject, count uint64) (osd.ListPage, []osd.HObject, error) {
		calls++
		if calls == 1 {
			if cursor != start || count != 2 {
				t.Fatalf("first request cursor=%+v count=%d", cursor, count)
			}
			return osd.ListPage{Next: middle}, nil, nil
		}
		entries := []osd.ListEntry{{Object: "one"}, {Object: "two"}}
		cursors := []osd.HObject{enumerationCursor(2, "one"), enumerationCursor(1, "two")}
		return osd.ListPage{Next: finish, Entries: entries}, cursors, nil
	})
	if err != nil || calls != 2 || len(result.Entries) != 2 || result.Next != finish {
		t.Fatalf("result=%+v calls=%d error=%v", result, calls, err)
	}
}

func TestEnumeratePagesClipsExclusiveEnd(t *testing.T) {
	start := enumerationCursor(0, "")
	end := enumerationCursor(1, "")
	entries := []osd.ListEntry{{Object: "before"}, {Object: "at-end"}, {Object: "after"}}
	cursors := []osd.HObject{enumerationCursor(2, "before"), end, enumerationCursor(1, "after")}
	result, err := enumeratePages(7, start, end, 3, func(osd.HObject, uint64) (osd.ListPage, []osd.HObject, error) {
		return osd.ListPage{Next: osd.HObject{Max: true}, Entries: entries}, cursors, nil
	})
	if err != nil || len(result.Entries) != 1 || result.Entries[0].Object != "before" || result.Next != end {
		t.Fatalf("result=%+v error=%v", result, err)
	}
}

func TestEnumeratePagesReconstructsOverfullCursor(t *testing.T) {
	start := enumerationCursor(0, "")
	entries := []osd.ListEntry{{Object: "one"}, {Object: "two"}, {Object: "three"}}
	cursors := []osd.HObject{enumerationCursor(4, "one"), enumerationCursor(2, "two"), enumerationCursor(6, "three")}
	result, err := enumeratePages(7, start, osd.HObject{Max: true}, 2, func(osd.HObject, uint64) (osd.ListPage, []osd.HObject, error) {
		return osd.ListPage{Next: osd.HObject{Max: true}, Entries: entries}, cursors, nil
	})
	if err != nil || len(result.Entries) != 2 || result.Next != cursors[2] {
		t.Fatalf("result=%+v error=%v", result, err)
	}
}

func enumerationCursor(hash uint32, object string) osd.HObject {
	return osd.HObject{Object: object, Snapshot: osd.NoSnap, Hash: hash, Pool: 7}
}

func assertPGNLSRequest(t testing.TB, message msgr.Message, route Route, cursor osd.HObject, namespace string, count uint64, startEpoch uint32, retry int32) {
	t.Helper()
	decoder := wire.NewDecoder(message.Front, wire.Limits{MaxBytes: 4096})
	_, spg := decoder.Versioned(1)
	pg, err := decodeRequestPGForTest(spg)
	if err != nil || spg.Uint8() != 0xff || pg != route.PG || decoder.Uint32() != cursor.Hash || decoder.Uint32() != route.Epoch {
		t.Fatal("PGNLS route metadata mismatch")
	}
	wantFlags := osd.FlagRead | osd.FlagPGOp | osd.FlagIgnoreOverlay
	if retry > 0 {
		wantFlags |= osd.FlagRetry
	}
	if flags := decoder.Uint32(); flags != wantFlags {
		t.Fatalf("flags=%#x", flags)
	}
	_, requestID := decoder.Versioned(2)
	requestID.Raw(uint32(requestID.Remaining()))
	decoder.Raw(36)
	_, locator := decoder.Versioned(6)
	if locator.Int64() != cursor.Pool || locator.Int32() != -1 || locator.String() != "" || locator.String() != namespace || locator.Int64() != -1 || locator.Remaining() != 0 {
		t.Fatal("PGNLS locator mismatch")
	}
	if decoder.String() != "" || decoder.Uint16() != 1 || decoder.Uint16() != osd.OpPGNList {
		t.Fatal("PGNLS object or operation mismatch")
	}
	decoder.Uint32()
	if decoder.Uint64() != count || decoder.Uint32() != startEpoch {
		t.Fatal("PGNLS descriptor mismatch")
	}
	decoder.Raw(16)
	payloadLength := decoder.Uint32()
	if decoder.Uint64() != osd.NoSnap || decoder.Uint64() != 0 || decoder.Uint32() != 0 || decoder.Int32() != retry || decoder.Uint64() != uint64(protocol.FeatureOSDClient) || decoder.Remaining() != 0 {
		t.Fatal("PGNLS request tail mismatch")
	}
	payload := wire.NewDecoder(message.Data, wire.Limits{MaxBytes: 4096})
	decodedCursor := decodeHObjectForTest(t, payload)
	if payloadLength != uint32(len(message.Data)) || !reflect.DeepEqual(decodedCursor, cursor) || payload.Remaining() != 0 {
		t.Fatalf("cursor=%+v payloadLength=%d", decodedCursor, payloadLength)
	}
}

func testPGNLSReply(t testing.TB, route Route, retry, result int32, data []byte) msgr.Message {
	t.Helper()
	front := wire.NewEncoder(4096)
	front.String("")
	front.Uint8(1)
	front.Uint64(route.PG.Pool)
	front.Uint32(route.PG.Seed)
	front.Int32(-1)
	front.Int64(0)
	front.Int32(result)
	front.Uint32(0)
	front.Uint64(0)
	front.Uint32(route.Epoch)
	front.Uint32(1)
	front.Uint16(osd.OpPGNList)
	front.Uint32(0)
	front.Raw(make([]byte, 28))
	front.Uint32(uint32(len(data)))
	front.Int32(-1)
	front.Int32(0)
	front.Uint32(0)
	front.Uint64(0)
	front.Uint64(0)
	front.Bool(false)
	front.Int64(0)
	front.Int64(0)
	front.Int64(0)
	encoded, err := front.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	return msgr.Message{Header: msgr.MessageHeader{Type: protocol.MessageOSDOpReply, Version: 8, CompatVersion: 2}, Front: encoded, Data: data, Lengths: msgr.MessageLengths{Front: uint32(len(encoded)), Data: uint32(len(data))}}
}

func encodePGNLSPageForTest(t testing.TB, next osd.HObject, entries []osd.ListEntry) []byte {
	t.Helper()
	encoder := wire.NewEncoder(4096)
	encoder.Versioned(1, 1, func(payload *wire.Encoder) {
		encodeHObjectForTest(payload, next)
		payload.Uint32(uint32(len(entries)))
		for _, entry := range entries {
			payload.String(entry.Namespace)
			payload.String(entry.Object)
			payload.String(entry.Locator)
		}
	})
	data, err := encoder.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func encodeHObjectForTest(encoder *wire.Encoder, object osd.HObject) {
	encoder.Versioned(4, 3, func(payload *wire.Encoder) {
		payload.String(object.Key)
		payload.String(object.Object)
		payload.Uint64(object.Snapshot)
		payload.Uint32(object.Hash)
		payload.Bool(object.Max)
		payload.String(object.Namespace)
		payload.Int64(object.Pool)
	})
}

func decodeHObjectForTest(t testing.TB, decoder *wire.Decoder) osd.HObject {
	t.Helper()
	version, payload := decoder.Versioned(4)
	if version != 4 {
		t.Fatalf("hobject version=%d", version)
	}
	object := osd.HObject{Key: payload.String(), Object: payload.String(), Snapshot: payload.Uint64(), Hash: payload.Uint32(), Max: payload.Bool(), Namespace: payload.String(), Pool: payload.Int64()}
	if err := payload.Finish(); err != nil || payload.Remaining() != 0 {
		t.Fatalf("hobject error=%v remaining=%d", err, payload.Remaining())
	}
	return object
}
