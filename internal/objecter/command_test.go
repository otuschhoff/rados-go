package objecter

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net/netip"
	"sync"
	"testing"

	wire "github.com/otuschhoff/rados-go/internal/encoding"
	"github.com/otuschhoff/rados-go/internal/maps"
	"github.com/otuschhoff/rados-go/internal/msgr"
	"github.com/otuschhoff/rados-go/internal/osd"
	"github.com/otuschhoff/rados-go/internal/protocol"
)

// fakeCommandRouter implements Router and osdCommandRouter so tests can drive
// OSDCommand and PGCommand without a real OSD map.
type fakeCommandRouter struct {
	mu       sync.Mutex
	osd      map[int32]Route
	pg       map[maps.PG]Route
	osdErr   map[int32]error
	pgErr    map[maps.PG]error
	fallback Route
}

func (router *fakeCommandRouter) Route(Target) (Route, error) {
	router.mu.Lock()
	defer router.mu.Unlock()
	return router.fallback, nil
}

func (router *fakeCommandRouter) RouteOSD(id int32) (Route, error) {
	router.mu.Lock()
	defer router.mu.Unlock()
	if err, ok := router.osdErr[id]; ok {
		return Route{}, err
	}
	route, ok := router.osd[id]
	if !ok {
		return Route{}, ErrUnknownOSD
	}
	return route, nil
}

func (router *fakeCommandRouter) RoutePG(pg maps.PG) (Route, error) {
	router.mu.Lock()
	defer router.mu.Unlock()
	if err, ok := router.pgErr[pg]; ok {
		return Route{}, err
	}
	route, ok := router.pg[pg]
	if !ok {
		return Route{}, ErrNoPrimary
	}
	return route, nil
}

func (router *fakeCommandRouter) setPG(pg maps.PG, route Route) {
	router.mu.Lock()
	defer router.mu.Unlock()
	if router.pg == nil {
		router.pg = map[maps.PG]Route{}
	}
	router.pg[pg] = route
}

func testAddress(t *testing.T, endpoint string) protocol.EntityAddr {
	t.Helper()
	address, err := protocol.IPv4EntityAddr(protocol.AddressV2, 1, netip.MustParseAddrPort(endpoint))
	if err != nil {
		t.Fatal(err)
	}
	return address
}

func commandReplyMessage(t *testing.T, tid uint64, result int32, status string, output []byte) msgr.Message {
	t.Helper()
	front := bytes.Buffer{}
	binary.Write(&front, binary.LittleEndian, result)
	binary.Write(&front, binary.LittleEndian, uint32(len(status)))
	front.WriteString(status)
	return msgr.Message{
		Header:  msgr.MessageHeader{Type: protocol.MessageCommandReply, Version: 1, CompatVersion: 1, TransactionID: tid},
		Front:   front.Bytes(),
		Data:    append([]byte(nil), output...),
		Lengths: msgr.MessageLengths{Front: uint32(front.Len()), Data: uint32(len(output))},
	}
}

func TestOSDCommandRoutesToLookedUpAddress(t *testing.T) {
	address := testAddress(t, "192.0.2.10:6800")
	router := &fakeCommandRouter{osd: map[int32]Route{7: {Epoch: 5, Primary: 7, Addresses: protocol.EntityAddrVec{address}}}}
	var seen msgr.Message
	var createdFor int32
	client := newTestClient(t, &fakeMapSource{}, router, func(id int32, addresses protocol.EntityAddrVec) (session, error) {
		createdFor = id
		if len(addresses) != 1 || addresses[0].SocketData == nil {
			t.Fatalf("addresses=%+v", addresses)
		}
		return &fakeSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
			seen = message
			return commandReplyMessage(t, message.Header.TransactionID, 0, "ok", []byte("pong")), nil
		}}, nil
	})
	defer client.Close()

	result, err := client.OSDCommand(context.Background(), 7, []string{"ping"}, []byte("payload"))
	if err != nil {
		t.Fatal(err)
	}
	if createdFor != 7 || result.Result != 0 || result.Status != "ok" || string(result.Output) != "pong" {
		t.Fatalf("createdFor=%d result=%+v", createdFor, result)
	}
	if seen.Header.Type != protocol.MessageCommand || string(seen.Data) != "payload" {
		t.Fatalf("request header=%+v data=%s", seen.Header, seen.Data)
	}
}

func TestOSDCommandRejectsBadInputWithoutRouting(t *testing.T) {
	router := &fakeCommandRouter{}
	client := newTestClient(t, &fakeMapSource{}, router, func(int32, protocol.EntityAddrVec) (session, error) {
		t.Fatal("must not create session")
		return nil, nil
	})
	defer client.Close()
	if _, err := client.OSDCommand(context.Background(), -1, []string{"x"}, nil); !errors.Is(err, wire.ErrMalformed) {
		t.Fatalf("negative id err=%v", err)
	}
	if _, err := client.OSDCommand(context.Background(), 0, nil, nil); !errors.Is(err, wire.ErrMalformed) {
		t.Fatalf("empty command err=%v", err)
	}
}

func TestOSDCommandReturnsUnknownOSDError(t *testing.T) {
	router := &fakeCommandRouter{}
	client := newTestClient(t, &fakeMapSource{}, router, func(int32, protocol.EntityAddrVec) (session, error) {
		t.Fatal("must not open a session for a nonexistent OSD")
		return nil, nil
	})
	defer client.Close()
	if _, err := client.OSDCommand(context.Background(), 42, []string{"ping"}, nil); !errors.Is(err, ErrUnknownOSD) {
		t.Fatalf("err=%v", err)
	}
}

func TestOSDCommandRefreshesMapOnEAGAIN(t *testing.T) {
	address := testAddress(t, "192.0.2.20:6800")
	router := &fakeCommandRouter{osd: map[int32]Route{3: {Epoch: 4, Primary: 3, Addresses: protocol.EntityAddrVec{address}}}}
	refreshes := 0
	source := &fakeMapSource{refresh: func() { refreshes++ }}
	attempt := 0
	client := newTestClient(t, source, router, func(int32, protocol.EntityAddrVec) (session, error) {
		return &fakeSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
			attempt++
			if attempt == 1 {
				return commandReplyMessage(t, message.Header.TransactionID, -11, "try again", nil), nil
			}
			return commandReplyMessage(t, message.Header.TransactionID, 0, "ok", []byte("done")), nil
		}}, nil
	})
	defer client.Close()

	result, err := client.OSDCommand(context.Background(), 3, []string{"pg-repair"}, nil)
	if err != nil || string(result.Output) != "done" || attempt != 2 || refreshes < 1 {
		t.Fatalf("result=%+v err=%v attempt=%d refreshes=%d", result, err, attempt, refreshes)
	}
}

func TestOSDCommandPreservesServerErrno(t *testing.T) {
	address := testAddress(t, "192.0.2.30:6800")
	router := &fakeCommandRouter{osd: map[int32]Route{2: {Epoch: 6, Primary: 2, Addresses: protocol.EntityAddrVec{address}}}}
	client := newTestClient(t, &fakeMapSource{}, router, func(int32, protocol.EntityAddrVec) (session, error) {
		return &fakeSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
			return commandReplyMessage(t, message.Header.TransactionID, -13, "operation not permitted", []byte("audit-log")), nil
		}}, nil
	})
	defer client.Close()

	result, err := client.OSDCommand(context.Background(), 2, []string{"scrub"}, nil)
	if !errors.Is(err, protocol.WireErrno(-13)) {
		t.Fatalf("err=%v", err)
	}
	if result.Result != -13 || result.Status != "operation not permitted" || string(result.Output) != "audit-log" {
		t.Fatalf("result=%+v", result)
	}
}

func TestOSDCommandRetriesInvalidateSessionOnSubmitError(t *testing.T) {
	address := testAddress(t, "192.0.2.40:6800")
	router := &fakeCommandRouter{osd: map[int32]Route{4: {Epoch: 7, Primary: 4, Addresses: protocol.EntityAddrVec{address}}}}
	created := 0
	client := newTestClient(t, &fakeMapSource{}, router, func(int32, protocol.EntityAddrVec) (session, error) {
		created++
		if created == 1 {
			return &fakeSession{submit: func(context.Context, msgr.Message) (msgr.Message, error) {
				return msgr.Message{}, errors.New("session broke")
			}}, nil
		}
		return &fakeSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
			return commandReplyMessage(t, message.Header.TransactionID, 0, "ok", []byte("recovered")), nil
		}}, nil
	})
	defer client.Close()

	result, err := client.OSDCommand(context.Background(), 4, []string{"status"}, nil)
	if err != nil || string(result.Output) != "recovered" || created != 2 {
		t.Fatalf("result=%+v err=%v created=%d", result, err, created)
	}
}

func TestOSDCommandDoesNotRetryUnknownOutcome(t *testing.T) {
	address := testAddress(t, "192.0.2.41:6800")
	router := &fakeCommandRouter{osd: map[int32]Route{4: {Epoch: 7, Primary: 4, Addresses: protocol.EntityAddrVec{address}}}}
	attempts := 0
	client := newTestClient(t, &fakeMapSource{}, router, func(int32, protocol.EntityAddrVec) (session, error) {
		return &fakeSession{submit: func(context.Context, msgr.Message) (msgr.Message, error) {
			attempts++
			return msgr.Message{}, msgr.ErrOutcomeUnknown
		}}, nil
	})
	defer client.Close()

	if _, err := client.OSDCommand(context.Background(), 4, []string{"mutating-command"}, nil); !errors.Is(err, msgr.ErrOutcomeUnknown) {
		t.Fatalf("err=%v", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts=%d", attempts)
	}
}

func TestPGCommandRoutesToActingPrimary(t *testing.T) {
	address := testAddress(t, "192.0.2.50:6800")
	pg := maps.PG{Pool: 7, Seed: 0x1a, Preferred: -1}
	router := &fakeCommandRouter{}
	router.setPG(pg, Route{Epoch: 10, PG: pg, Primary: 9, Addresses: protocol.EntityAddrVec{address}})
	var seenPrimary int32
	client := newTestClient(t, &fakeMapSource{}, router, func(id int32, _ protocol.EntityAddrVec) (session, error) {
		seenPrimary = id
		return &fakeSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
			return commandReplyMessage(t, message.Header.TransactionID, 0, "ok", []byte("pg-ok")), nil
		}}, nil
	})
	defer client.Close()

	result, err := client.PGCommandString(context.Background(), "7.1a", []string{"query"}, nil)
	if err != nil || seenPrimary != 9 || string(result.Output) != "pg-ok" {
		t.Fatalf("result=%+v err=%v primary=%d", result, err, seenPrimary)
	}
}

func TestPGCommandFollowsMapChange(t *testing.T) {
	first := testAddress(t, "192.0.2.60:6800")
	second := testAddress(t, "192.0.2.61:6800")
	pg := maps.PG{Pool: 3, Seed: 5, Preferred: -1}
	router := &fakeCommandRouter{}
	router.setPG(pg, Route{Epoch: 10, PG: pg, Primary: 1, Addresses: protocol.EntityAddrVec{first}})
	source := &fakeMapSource{refresh: func() {
		router.setPG(pg, Route{Epoch: 11, PG: pg, Primary: 2, Addresses: protocol.EntityAddrVec{second}})
	}}
	factoryFor := make(map[int32]bool)
	client := newTestClient(t, source, router, func(id int32, _ protocol.EntityAddrVec) (session, error) {
		factoryFor[id] = true
		if id == 1 {
			return &fakeSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
				return commandReplyMessage(t, message.Header.TransactionID, -11, "try again", nil), nil
			}}, nil
		}
		return &fakeSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
			return commandReplyMessage(t, message.Header.TransactionID, 0, "ok", []byte("done")), nil
		}}, nil
	})
	defer client.Close()

	result, err := client.PGCommand(context.Background(), pg, []string{"scrub"}, nil)
	if err != nil || string(result.Output) != "done" || !factoryFor[1] || !factoryFor[2] {
		t.Fatalf("result=%+v err=%v sessions=%v", result, err, factoryFor)
	}
}

func TestPGCommandStringRejectsMalformed(t *testing.T) {
	router := &fakeCommandRouter{}
	client := newTestClient(t, &fakeMapSource{}, router, func(int32, protocol.EntityAddrVec) (session, error) {
		t.Fatal("must not open a session for malformed PG")
		return nil, nil
	})
	defer client.Close()
	if _, err := client.PGCommandString(context.Background(), "bogus", []string{"x"}, nil); !errors.Is(err, wire.ErrMalformed) {
		t.Fatalf("err=%v", err)
	}
}

func TestCommandInvalidatesSessionOnMalformedReply(t *testing.T) {
	address := testAddress(t, "192.0.2.70:6800")
	router := &fakeCommandRouter{osd: map[int32]Route{5: {Epoch: 3, Primary: 5, Addresses: protocol.EntityAddrVec{address}}}}
	var active *fakeSession
	client := newTestClient(t, &fakeMapSource{}, router, func(int32, protocol.EntityAddrVec) (session, error) {
		active = &fakeSession{submit: func(context.Context, msgr.Message) (msgr.Message, error) {
			return msgr.Message{Header: msgr.MessageHeader{Type: protocol.MessageCommand}}, nil
		}}
		return active, nil
	})
	defer client.Close()

	if _, err := client.OSDCommand(context.Background(), 5, []string{"noop"}, nil); !errors.Is(err, osd.ErrMalformedCommandReply) {
		t.Fatalf("err=%v", err)
	}
	if active == nil || !active.stop {
		t.Fatalf("session was not invalidated: active=%v", active)
	}
}

func TestCommandRejectsTransactionIDMismatch(t *testing.T) {
	address := testAddress(t, "192.0.2.80:6800")
	router := &fakeCommandRouter{osd: map[int32]Route{6: {Epoch: 3, Primary: 6, Addresses: protocol.EntityAddrVec{address}}}}
	client := newTestClient(t, &fakeMapSource{}, router, func(int32, protocol.EntityAddrVec) (session, error) {
		return &fakeSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
			return commandReplyMessage(t, message.Header.TransactionID+1, 0, "ok", nil), nil
		}}, nil
	})
	defer client.Close()
	if _, err := client.OSDCommand(context.Background(), 6, []string{"noop"}, nil); !errors.Is(err, osd.ErrMalformedCommandReply) {
		t.Fatalf("err=%v", err)
	}
}
