package objecter

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"sync"
	"testing"
	"time"

	wire "github.com/otuschhoff/go-librados/internal/encoding"
	"github.com/otuschhoff/go-librados/internal/maps"
	"github.com/otuschhoff/go-librados/internal/msgr"
	"github.com/otuschhoff/go-librados/internal/osd"
	"github.com/otuschhoff/go-librados/internal/protocol"
)

type fakeMapSource struct{ refresh func() }

func (source *fakeMapSource) OSDMap() *maps.OSDMap { return nil }
func (source *fakeMapSource) RefreshOSDMap(context.Context, uint32) error {
	if source.refresh != nil {
		source.refresh()
	}
	return nil
}

type fakeRouter struct {
	mu    sync.Mutex
	route Route
}

func (router *fakeRouter) Route(Target) (Route, error) {
	router.mu.Lock()
	defer router.mu.Unlock()
	return router.route, nil
}

func (router *fakeRouter) set(route Route) {
	router.mu.Lock()
	router.route = route
	router.mu.Unlock()
}

type fakeSession struct {
	submit func(context.Context, msgr.Message) (msgr.Message, error)
	stop   bool
}

func (session *fakeSession) Submit(ctx context.Context, message msgr.Message) (msgr.Message, error) {
	return session.submit(ctx, message)
}
func (session *fakeSession) Stop() { session.stop = true }

func TestReadRecoversToNewPrimary(t *testing.T) {
	route0 := testRoute(t, 10, 0, "192.0.2.10:6800")
	route1 := testRoute(t, 11, 1, "192.0.2.11:6800")
	router := &fakeRouter{route: route0}
	maps := &fakeMapSource{refresh: func() { router.set(route1) }}
	created := make(map[int32]*fakeSession)
	client := newTestClient(t, maps, router, func(id int32, _ protocol.EntityAddrVec) (session, error) {
		fake := &fakeSession{}
		if id == 0 {
			fake.submit = func(context.Context, msgr.Message) (msgr.Message, error) {
				return msgr.Message{}, errors.New("primary stopped")
			}
		} else {
			fake.submit = func(context.Context, msgr.Message) (msgr.Message, error) {
				return testReply(t, 11, 42, 0, []byte("data")), nil
			}
		}
		created[id] = fake
		return fake, nil
	})
	defer client.Close()
	result, err := client.Read(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, 0, 4)
	if err != nil {
		t.Fatal(err)
	}
	if string(result.Data) != "data" || result.Version != 42 || !created[0].stop {
		t.Fatalf("result=%+v old_stopped=%t", result, created[0].stop)
	}
}

func TestQueueSaturationDoesNotInvalidateSession(t *testing.T) {
	route := testRoute(t, 10, 0, "192.0.2.10:6800")
	active := &fakeSession{submit: func(context.Context, msgr.Message) (msgr.Message, error) {
		return msgr.Message{}, msgr.ErrQueueSaturated
	}}
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: route}, func(int32, protocol.EntityAddrVec) (session, error) {
		return active, nil
	})
	defer client.Close()

	_, err := client.Mutate(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, osd.Operation{Code: osd.OpWriteFull, Data: []byte("data"), Length: 4})
	if !errors.Is(err, msgr.ErrQueueSaturated) {
		t.Fatalf("error=%v, want queue saturation", err)
	}
	if active.stop {
		t.Fatal("locally saturated session was invalidated")
	}
}

func TestStatDecodesMetadata(t *testing.T) {
	route := testRoute(t, 10, 0, "192.0.2.10:6800")
	data := make([]byte, 16)
	data[0] = 9
	data[8] = 7
	data[12] = 5
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: route}, func(int32, protocol.EntityAddrVec) (session, error) {
		return &fakeSession{submit: func(context.Context, msgr.Message) (msgr.Message, error) {
			return testReplyOperation(t, 10, 3, 0, osd.OpStat, data), nil
		}}, nil
	})
	defer client.Close()
	result, err := client.Stat(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: 0})
	if err != nil {
		t.Fatal(err)
	}
	if result.Size != 9 || !result.ModificationTime.Equal(time.Unix(7, 5).UTC()) || result.Version != 3 {
		t.Fatalf("stat=%+v", result)
	}
}

func TestReadReturnsWireAndMalformedErrors(t *testing.T) {
	route := testRoute(t, 10, 0, "192.0.2.10:6800")
	for _, test := range []struct {
		name  string
		reply msgr.Message
		code  int32
	}{
		{name: "missing", reply: testReply(t, 10, 0, -2, nil), code: -2},
		{name: "corrupt", reply: msgr.Message{Header: msgr.MessageHeader{Type: protocol.MessageOSDOpReply, Version: 8}, Front: []byte{1}, Lengths: msgr.MessageLengths{Front: 1}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: route}, func(int32, protocol.EntityAddrVec) (session, error) {
				return &fakeSession{submit: func(context.Context, msgr.Message) (msgr.Message, error) { return test.reply, nil }}, nil
			})
			defer client.Close()
			_, err := client.Read(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, 0, 1)
			if test.code != 0 {
				var wireError protocol.WireErrno
				if !errors.As(err, &wireError) || int32(wireError) != test.code {
					t.Fatalf("error=%v", err)
				}
			} else if err == nil {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestReadCancellationStopsRecovery(t *testing.T) {
	route := testRoute(t, 10, 0, "192.0.2.10:6800")
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: route}, func(int32, protocol.EntityAddrVec) (session, error) {
		return &fakeSession{submit: func(ctx context.Context, _ msgr.Message) (msgr.Message, error) {
			<-ctx.Done()
			return msgr.Message{}, ctx.Err()
		}}, nil
	})
	defer client.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.Read(ctx, Target{PoolID: 7, Snapshot: osd.NoSnap}, 0, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
}

func TestReadRejectsResponseBeyondConfiguredLimit(t *testing.T) {
	route := testRoute(t, 10, 0, "192.0.2.10:6800")
	created := 0
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: route}, func(int32, protocol.EntityAddrVec) (session, error) {
		created++
		return nil, errors.New("must not create session")
	})
	defer client.Close()
	maximum := uint64(4096) - readReplyFrontBytes - uint64(len("object"))
	if _, err := client.Read(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, 0, maximum+1); !errors.Is(err, wire.ErrLimitExceeded) {
		t.Fatalf("error=%v", err)
	}
	if created != 0 {
		t.Fatalf("sessions created=%d", created)
	}
}

func TestReadRedirectRecoveryIsBoundedAndSetsRetryMetadata(t *testing.T) {
	route := testRoute(t, 10, 0, "192.0.2.10:6800")
	var retries []int32
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: route}, func(int32, protocol.EntityAddrVec) (session, error) {
		return &fakeSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
			retries = append(retries, decodeRequestRetryForTest(t, message))
			return testRedirectReply(t, 10), nil
		}}, nil
	})
	defer client.Close()
	_, err := client.Read(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, 0, 1)
	if !errors.Is(err, ErrRecovery) {
		t.Fatalf("error=%v", err)
	}
	want := []int32{0, 1, 2}
	if len(retries) != len(want) {
		t.Fatalf("retries=%v", retries)
	}
	for index := range want {
		if retries[index] != want[index] {
			t.Fatalf("retries=%v want=%v", retries, want)
		}
	}
}

func TestReadRedirectPreservesEmptyObjectAndSetsBypassFlags(t *testing.T) {
	route := testRoute(t, 10, 0, "192.0.2.10:6800")
	var requests []msgr.Message
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: route}, func(int32, protocol.EntityAddrVec) (session, error) {
		return &fakeSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
			requests = append(requests, message)
			if len(requests) == 1 {
				return testRedirectReply(t, 10), nil
			}
			return testReply(t, 10, 7, 0, []byte("ok")), nil
		}}, nil
	})
	defer client.Close()
	result, err := client.Read(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, 0, 2)
	if err != nil || string(result.Data) != "ok" {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	object, flags, retry := decodeRequestIdentityForTest(t, requests[1])
	wantFlags := osd.FlagRead | osd.FlagRetry | osd.FlagRedirected | osd.FlagIgnoreCache | osd.FlagIgnoreOverlay
	if object != "object" || flags != wantFlags || retry != 1 {
		t.Fatalf("object=%q flags=%#x retry=%d", object, flags, retry)
	}
}

func TestReadRetriesEAGAINWithMapRefresh(t *testing.T) {
	route := testRoute(t, 10, 0, "192.0.2.10:6800")
	refreshes := 0
	source := &fakeMapSource{refresh: func() { refreshes++ }}
	attempts := 0
	client := newTestClient(t, source, &fakeRouter{route: route}, func(int32, protocol.EntityAddrVec) (session, error) {
		return &fakeSession{submit: func(context.Context, msgr.Message) (msgr.Message, error) {
			attempts++
			if attempts == 1 {
				return testReply(t, 10, 0, -11, nil), nil
			}
			return testReply(t, 10, 9, 0, []byte("ok")), nil
		}}, nil
	})
	defer client.Close()
	result, err := client.Read(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, 0, 2)
	if err != nil || string(result.Data) != "ok" || attempts != 2 || refreshes != 1 {
		t.Fatalf("result=%+v error=%v attempts=%d refreshes=%d", result, err, attempts, refreshes)
	}
}

func TestReadRejectsMismatchedReplyIdentity(t *testing.T) {
	route := testRoute(t, 10, 0, "192.0.2.10:6800")
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: route}, func(int32, protocol.EntityAddrVec) (session, error) {
		return &fakeSession{submit: func(context.Context, msgr.Message) (msgr.Message, error) {
			message := testReply(t, 10, 1, 0, []byte("data"))
			message.Front[4] = 'X'
			return message, nil
		}}, nil
	})
	defer client.Close()
	if _, err := client.Read(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, 0, 4); !errors.Is(err, osd.ErrMalformedReply) {
		t.Fatalf("error=%v", err)
	}
}

func TestReadRejectsMismatchedReplyAttempt(t *testing.T) {
	route := testRoute(t, 10, 0, "192.0.2.10:6800")
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: route}, func(int32, protocol.EntityAddrVec) (session, error) {
		return &fakeSession{submit: func(context.Context, msgr.Message) (msgr.Message, error) {
			return testReplyRetry(t, 10, 1, 0, osd.OpRead, 1, []byte("data")), nil
		}}, nil
	})
	defer client.Close()
	if _, err := client.Read(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, 0, 4); !errors.Is(err, osd.ErrMalformedReply) {
		t.Fatalf("error=%v", err)
	}
}

func TestReadInvalidatesSessionAfterMismatchedOperation(t *testing.T) {
	route := testRoute(t, 10, 0, "192.0.2.10:6800")
	created := 0
	first := &fakeSession{submit: func(context.Context, msgr.Message) (msgr.Message, error) {
		return testReplyOperation(t, 10, 1, 0, osd.OpStat, make([]byte, 16)), nil
	}}
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: route}, func(int32, protocol.EntityAddrVec) (session, error) {
		created++
		if created == 1 {
			return first, nil
		}
		return &fakeSession{submit: func(context.Context, msgr.Message) (msgr.Message, error) {
			return testReply(t, 10, 2, 0, []byte("ok")), nil
		}}, nil
	})
	defer client.Close()
	target := Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}
	if _, err := client.Read(context.Background(), target, 0, 2); !errors.Is(err, osd.ErrMalformedReply) || !first.stop {
		t.Fatalf("error=%v stopped=%t", err, first.stop)
	}
	result, err := client.Read(context.Background(), target, 0, 2)
	if err != nil || string(result.Data) != "ok" || created != 2 {
		t.Fatalf("result=%+v error=%v created=%d", result, err, created)
	}
}

func TestReadRejectsDataBeyondRequestedLength(t *testing.T) {
	for _, length := range []uint64{0, 1} {
		t.Run(fmt.Sprintf("length=%d", length), func(t *testing.T) {
			route := testRoute(t, 10, 0, "192.0.2.10:6800")
			active := &fakeSession{submit: func(context.Context, msgr.Message) (msgr.Message, error) {
				return testReply(t, 10, 1, 0, []byte("too much")), nil
			}}
			client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: route}, func(int32, protocol.EntityAddrVec) (session, error) { return active, nil })
			defer client.Close()
			if _, err := client.Read(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, 0, length); !errors.Is(err, osd.ErrMalformedReply) || !active.stop {
				t.Fatalf("error=%v stopped=%t", err, active.stop)
			}
		})
	}
}

func newTestClient(t *testing.T, source MapSource, router Router, factory SessionFactory) *Client {
	t.Helper()
	client, err := New(Config{Maps: source, Router: router, SessionFactory: factory, MessageLimits: osd.Limits{MaxBytes: 4096, MaxOperations: 4}, MaxAttempts: 3, RefreshWait: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func testRoute(t *testing.T, epoch uint32, primary int32, endpoint string) Route {
	t.Helper()
	address, err := protocol.IPv4EntityAddr(protocol.AddressV2, 1, netip.MustParseAddrPort(endpoint))
	if err != nil {
		t.Fatal(err)
	}
	return Route{Epoch: epoch, PG: maps.PG{Pool: 7, Seed: 8, Preferred: -1}, RawHash: 8, Primary: primary, Addresses: protocol.EntityAddrVec{address}}
}

func testReply(t *testing.T, epoch uint32, version uint64, result int32, data []byte) msgr.Message {
	return testReplyOperation(t, epoch, version, result, osd.OpRead, data)
}

func testReplyOperation(t *testing.T, epoch uint32, version uint64, result int32, operation uint16, data []byte) msgr.Message {
	return testReplyRetry(t, epoch, version, result, operation, -1, data)
}

func testReplyRetry(t *testing.T, epoch uint32, version uint64, result int32, operation uint16, retry int32, data []byte) msgr.Message {
	flags := int64(0)
	if operation&0x2000 != 0 {
		flags = int64(osd.FlagOnDisk)
	}
	return testReplyRetryFlags(t, epoch, version, result, operation, retry, flags, data)
}

func testReplyRetryFlags(t *testing.T, epoch uint32, version uint64, result int32, operation uint16, retry int32, flags int64, data []byte) msgr.Message {
	t.Helper()
	front := wire.NewEncoder(4096)
	front.String("object")
	front.Uint8(1)
	front.Uint64(7)
	front.Uint32(8)
	front.Int32(retry)
	front.Int64(flags)
	front.Int32(result)
	front.Uint32(0)
	front.Uint64(0)
	front.Uint32(epoch)
	front.Uint32(1)
	front.Uint16(operation)
	front.Uint32(0)
	front.Uint64(0)
	front.Uint64(uint64(len(data)))
	front.Uint64(0)
	front.Uint32(0)
	front.Uint32(uint32(len(data)))
	front.Int32(-1)
	front.Int32(0)
	front.Uint32(0)
	front.Uint64(0)
	front.Uint64(version)
	front.Bool(false)
	front.Int64(0)
	front.Int64(0)
	front.Int64(0)
	encoded, err := front.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	return msgr.Message{Header: msgr.MessageHeader{Type: protocol.MessageOSDOpReply, Version: 8, CompatVersion: 2}, Front: encoded, Data: append([]byte(nil), data...), Lengths: msgr.MessageLengths{Front: uint32(len(encoded)), Data: uint32(len(data))}}
}

func testRedirectReply(t *testing.T, epoch uint32) msgr.Message {
	t.Helper()
	message := testReply(t, epoch, 0, 0, nil)
	front := wire.NewEncoder(4096)
	front.Raw(message.Front[:len(message.Front)-25])
	front.Bool(true)
	front.Versioned(1, 1, func(redirect *wire.Encoder) {
		redirect.Versioned(6, 3, func(locator *wire.Encoder) {
			locator.Int64(7)
			locator.Int32(-1)
			locator.String("")
			locator.String("")
			locator.Int64(-1)
		})
		redirect.String("object")
		redirect.Uint32(0)
	})
	front.Int64(0)
	front.Int64(0)
	front.Int64(0)
	encoded, err := front.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	message.Front = encoded
	message.Lengths.Front = uint32(len(encoded))
	return message
}

func decodeRequestPGForTest(decoder *wire.Decoder) (maps.PG, error) {
	if decoder.Uint8() != 1 {
		return maps.PG{}, wire.ErrUnsupportedVersion
	}
	return maps.PG{Pool: decoder.Uint64(), Seed: decoder.Uint32(), Preferred: decoder.Int32()}, decoder.Finish()
}

func decodeRequestRetryForTest(t testing.TB, message msgr.Message) int32 {
	t.Helper()
	decoder := wire.NewDecoder(message.Front, wire.Limits{MaxBytes: 4096})
	_, spg := decoder.Versioned(1)
	_, _ = decodeRequestPGForTest(spg)
	spg.Uint8()
	decoder.Uint32()
	decoder.Uint32()
	decoder.Uint32()
	_, requestID := decoder.Versioned(2)
	requestID.Uint8()
	requestID.Uint64()
	requestID.Uint64()
	requestID.Int32()
	for range 3 {
		decoder.Int64()
	}
	decoder.Uint32()
	decoder.Uint32()
	decoder.Uint32()
	_, locator := decoder.Versioned(6)
	locator.Int64()
	locator.Int32()
	_ = locator.String()
	_ = locator.String()
	locator.Int64()
	_ = decoder.String()
	count := decoder.Uint16()
	for range count {
		decoder.Uint16()
		decoder.Uint32()
		decoder.Uint64()
		decoder.Uint64()
		decoder.Uint64()
		decoder.Uint32()
		decoder.Uint32()
	}
	decoder.Uint64()
	decoder.Uint64()
	decoder.Uint32()
	return decoder.Int32()
}

func decodeRequestIdentityForTest(t testing.TB, message msgr.Message) (string, uint32, int32) {
	t.Helper()
	decoder := wire.NewDecoder(message.Front, wire.Limits{MaxBytes: 4096})
	_, spg := decoder.Versioned(1)
	_, _ = decodeRequestPGForTest(spg)
	spg.Uint8()
	decoder.Uint32()
	decoder.Uint32()
	flags := decoder.Uint32()
	_, requestID := decoder.Versioned(2)
	requestID.Raw(uint32(requestID.Remaining()))
	for range 3 {
		decoder.Int64()
	}
	decoder.Uint32()
	decoder.Uint32()
	decoder.Uint32()
	_, locator := decoder.Versioned(6)
	locator.Raw(uint32(locator.Remaining()))
	object := decoder.String()
	count := decoder.Uint16()
	for range count {
		decoder.Raw(38)
	}
	decoder.Uint64()
	decoder.Uint64()
	decoder.Uint32()
	return object, flags, decoder.Int32()
}
