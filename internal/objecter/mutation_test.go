package objecter

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/otuschhoff/go-librados/internal/msgr"
	"github.com/otuschhoff/go-librados/internal/osd"
	"github.com/otuschhoff/go-librados/internal/protocol"
)

type routeFunc func(Target) (Route, error)

func (route routeFunc) Route(target Target) (Route, error) {
	return route(target)
}

func TestCompoundMutationPreservesOrderAndRetryIdentity(t *testing.T) {
	route := testRoute(t, 10, 0, "192.0.2.10:6800")
	var requests []msgr.Message
	created := 0
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: route}, func(int32, protocol.EntityAddrVec) (session, error) {
		created++
		attempt := created
		return &fakeSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
			requests = append(requests, message)
			if attempt == 1 {
				return msgr.Message{}, errors.New("not sent")
			}
			return testReplyOperations(t, 10, 29, int64(osd.FlagOnDisk), []osd.OperationResult{
				{Operation: osd.OpWrite, Data: []byte("write-result")},
				{Operation: osd.OpSetXattr, Data: []byte("xattr-result")},
			}), nil
		}}, nil
	})
	defer client.Close()

	result, err := client.MutateOperations(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, []osd.Operation{
		{Code: osd.OpWrite, Length: 3, Data: []byte("abc")},
		{Code: osd.OpSetXattr, XattrNameLength: 1, XattrValueLength: 2, Data: []byte("nvv")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Version != 29 || len(result.Operations) != 2 || string(result.Operations[0].Data) != "write-result" || string(result.Operations[1].Data) != "xattr-result" {
		t.Fatalf("result=%+v", result)
	}
	if len(requests) != 2 || requests[0].Header.TransactionID == 0 || requests[0].Header.TransactionID != requests[1].Header.TransactionID {
		t.Fatalf("request identities=%d/%d", requests[0].Header.TransactionID, requests[1].Header.TransactionID)
	}
	for index, request := range requests {
		if string(request.Data) != "abcnvv" {
			t.Fatalf("request %d data=%q", index, request.Data)
		}
		codes, flags, retry := decodeRequestOperationsForTest(t, request)
		if len(codes) != 2 || codes[0] != osd.OpWrite || codes[1] != osd.OpSetXattr || flags&osd.FlagReturnVector == 0 || retry != int32(index) {
			t.Fatalf("request %d codes=%v flags=%#x retry=%d", index, codes, flags, retry)
		}
	}
}

func TestClassOperationsUseReadModeWithConservativeOutcome(t *testing.T) {
	route := testRoute(t, 10, 0, "192.0.2.10:6800")
	var request msgr.Message
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: route}, func(int32, protocol.EntityAddrVec) (session, error) {
		return &fakeSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
			request = message
			return testReplyOperation(t, 10, 19, 0, osd.OpCall, []byte("output")), nil
		}}, nil
	})
	defer client.Close()
	operation := osd.Operation{Code: osd.OpCall, ClassNameLength: 4, MethodNameLength: 10, ClassInputLength: 0, Length: 14, Data: []byte("locklist_locks")}
	result, err := client.ClassOperations(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, []osd.Operation{operation})
	if err != nil || string(result.Data) != "output" {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	_, flags, _ := decodeRequestOperationsForTest(t, request)
	if flags&osd.FlagRead == 0 || flags&(osd.FlagWrite|osd.FlagOnDisk) != 0 {
		t.Fatalf("class flags=%#x", flags)
	}
}

func TestClassOperationOutcomeUnknownIsObservable(t *testing.T) {
	route := testRoute(t, 10, 0, "192.0.2.10:6800")
	requests := 0
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: route}, func(int32, protocol.EntityAddrVec) (session, error) {
		return &fakeSession{submit: func(_ context.Context, _ msgr.Message) (msgr.Message, error) {
			requests++
			return msgr.Message{}, msgr.ErrOutcomeUnknown
		}}, nil
	})
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	operation := osd.Operation{Code: osd.OpCall, ClassNameLength: 4, MethodNameLength: 10, Length: 14, Data: []byte("locklist_locks")}
	_, err := client.ClassOperations(ctx, Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, []osd.Operation{operation})
	if !errors.Is(err, msgr.ErrOutcomeUnknown) || requests != 1 {
		t.Fatalf("class error=%v requests=%d", err, requests)
	}
}

func TestWriteClassOperationUsesDurableWriteMode(t *testing.T) {
	route := testRoute(t, 10, 0, "192.0.2.10:6800")
	var request msgr.Message
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: route}, func(int32, protocol.EntityAddrVec) (session, error) {
		return &fakeSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
			request = message
			return testReplyOperations(t, 10, 19, int64(osd.FlagOnDisk), []osd.OperationResult{{Operation: osd.OpCall}}), nil
		}}, nil
	})
	defer client.Close()
	operation := osd.Operation{Code: osd.OpCall, ClassNameLength: 4, MethodNameLength: 4, ClassInputLength: 0, Length: 8, Data: []byte("locklock")}
	if _, err := client.MutateOperations(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, []osd.Operation{operation}); err != nil {
		t.Fatal(err)
	}
	_, flags, _ := decodeRequestOperationsForTest(t, request)
	if flags&osd.FlagWrite == 0 || flags&osd.FlagOnDisk == 0 || flags&osd.FlagRead != 0 {
		t.Fatalf("write class flags=%#x", flags)
	}
}

func TestMutationRetryPreservesIdentityAndPayload(t *testing.T) {
	route := testRoute(t, 10, 0, "192.0.2.10:6800")
	var mu sync.Mutex
	var requests []msgr.Message
	created := 0
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: route}, func(int32, protocol.EntityAddrVec) (session, error) {
		created++
		attempt := created
		return &fakeSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
			mu.Lock()
			requests = append(requests, message)
			mu.Unlock()
			if attempt == 1 {
				return msgr.Message{}, errors.New("not sent")
			}
			return testReplyOperation(t, 10, 19, 0, osd.OpAppend, nil), nil
		}}, nil
	})
	defer client.Close()
	payload := []byte("abc")
	result, err := client.Mutate(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, osd.Operation{Code: osd.OpAppend, Length: 3, Data: payload})
	if err != nil || result.Version != 19 {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	if len(requests) != 2 || requests[0].Header.TransactionID == 0 || requests[0].Header.TransactionID != requests[1].Header.TransactionID {
		t.Fatalf("request identities=%d/%d", requests[0].Header.TransactionID, requests[1].Header.TransactionID)
	}
	if string(requests[0].Data) != "abc" || string(requests[1].Data) != "abc" {
		t.Fatalf("payloads=%q/%q", requests[0].Data, requests[1].Data)
	}
}

func TestWriteSameUsesDurableMutationPath(t *testing.T) {
	route := testRoute(t, 10, 0, "192.0.2.10:6800")
	var request msgr.Message
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: route}, func(int32, protocol.EntityAddrVec) (session, error) {
		return &fakeSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
			request = message
			return testReplyRetryFlags(t, 10, 19, 0, osd.OpWriteSame, -1, int64(osd.FlagOnDisk), nil), nil
		}}, nil
	})
	defer client.Close()

	result, err := client.Mutate(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, osd.Operation{Code: osd.OpWriteSame, Offset: 4, Length: 12, PatternLength: 3, Data: []byte("abc")})
	if err != nil || result.Version != 19 {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	codes, flags, _ := decodeRequestOperationsForTest(t, request)
	if len(codes) != 1 || codes[0] != osd.OpWriteSame || flags&osd.FlagWrite == 0 || flags&osd.FlagOnDisk == 0 || string(request.Data) != "abc" {
		t.Fatalf("codes=%v flags=%#x data=%q", codes, flags, request.Data)
	}
}

func TestSetAllocationHintUsesDurableMutationPath(t *testing.T) {
	route := testRoute(t, 10, 0, "192.0.2.10:6800")
	var request msgr.Message
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: route}, func(int32, protocol.EntityAddrVec) (session, error) {
		return &fakeSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
			request = message
			return testReplyRetryFlags(t, 10, 19, 0, osd.OpSetAllocationHint, -1, int64(osd.FlagOnDisk), nil), nil
		}}, nil
	})
	defer client.Close()

	operation := osd.Operation{Code: osd.OpSetAllocationHint, Flags: osd.OpFlagFailOK, ExpectedObjectSize: 64, ExpectedWriteSize: 8}
	if _, err := client.Mutate(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, operation); err != nil {
		t.Fatal(err)
	}
	codes, flags, _ := decodeRequestOperationsForTest(t, request)
	if len(codes) != 1 || codes[0] != osd.OpSetAllocationHint || flags&osd.FlagWrite == 0 || flags&osd.FlagOnDisk == 0 || len(request.Data) != 0 {
		t.Fatalf("codes=%v flags=%#x data=%x", codes, flags, request.Data)
	}
}

func TestMutationUnknownOutcomeIsNotRetried(t *testing.T) {
	route := testRoute(t, 10, 0, "192.0.2.10:6800")
	attempts := 0
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: route}, func(int32, protocol.EntityAddrVec) (session, error) {
		return &fakeSession{submit: func(context.Context, msgr.Message) (msgr.Message, error) {
			attempts++
			return msgr.Message{}, msgr.ErrOutcomeUnknown
		}}, nil
	})
	defer client.Close()
	_, err := client.Mutate(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, osd.Operation{Code: osd.OpDelete})
	if !errors.Is(err, msgr.ErrOutcomeUnknown) || attempts != 1 {
		t.Fatalf("error=%v attempts=%d", err, attempts)
	}
	if err := client.Flush(context.Background()); !errors.Is(err, msgr.ErrOutcomeUnknown) {
		t.Fatalf("flush error=%v", err)
	}
}

func TestMutationUnknownOutcomeRemapsWithSameIdentity(t *testing.T) {
	route0 := testRoute(t, 10, 0, "192.0.2.10:6800")
	route1 := testRoute(t, 11, 0, "192.0.2.10:6800")
	route2 := testRoute(t, 12, 1, "192.0.2.11:6800")
	router := &fakeRouter{route: route0}
	refreshes := 0
	source := &fakeMapSource{refresh: func() {
		refreshes++
		if refreshes == 1 {
			router.set(route1)
		} else {
			router.set(route2)
		}
	}}
	var identities []uint64
	client := newTestClient(t, source, router, func(id int32, _ protocol.EntityAddrVec) (session, error) {
		return &fakeSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
			identities = append(identities, message.Header.TransactionID)
			if id == 0 {
				return msgr.Message{}, errors.Join(msgr.ErrOutcomeUnknown, msgr.ErrSessionClosed)
			}
			return testReplyOperation(t, 12, 21, 0, osd.OpAppend, nil), nil
		}}, nil
	})
	defer client.Close()
	result, err := client.Mutate(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, osd.Operation{Code: osd.OpAppend, Length: 1, Data: []byte("x")})
	if err != nil || result.Version != 21 || len(identities) != 2 || identities[0] != identities[1] {
		t.Fatalf("result=%+v error=%v identities=%v", result, err, identities)
	}
}

func TestMutationReconnectExhaustionRemapsWithSameIdentity(t *testing.T) {
	route0 := testRoute(t, 10, 0, "192.0.2.10:6800")
	route1 := testRoute(t, 11, 1, "192.0.2.11:6800")
	router := &fakeRouter{route: route0}
	source := &fakeMapSource{refresh: func() { router.set(route1) }}
	var identities []uint64
	client := newTestClient(t, source, router, func(id int32, _ protocol.EntityAddrVec) (session, error) {
		return &fakeSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
			identities = append(identities, message.Header.TransactionID)
			if id == 0 {
				return msgr.Message{}, errors.Join(msgr.ErrOutcomeUnknown, msgr.ErrReconnectExhausted)
			}
			return testReplyOperation(t, 11, 21, 0, osd.OpAppend, nil), nil
		}}, nil
	})
	defer client.Close()
	result, err := client.Mutate(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, osd.Operation{Code: osd.OpAppend, Length: 1, Data: []byte("x")})
	if err != nil || result.Version != 21 || len(identities) != 2 || identities[0] != identities[1] {
		t.Fatalf("result=%+v error=%v identities=%v", result, err, identities)
	}
}

func TestMutationStaleMapRetriesSamePrimaryWithSameIdentity(t *testing.T) {
	route0 := testRoute(t, 10, 0, "192.0.2.10:6800")
	route1 := testRoute(t, 11, 0, "192.0.2.10:6800")
	router := &fakeRouter{route: route0}
	source := &fakeMapSource{refresh: func() { router.set(route1) }}
	var identities []uint64
	created := 0
	client := newTestClient(t, source, router, func(int32, protocol.EntityAddrVec) (session, error) {
		created++
		attempt := created
		return &fakeSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
			identities = append(identities, message.Header.TransactionID)
			if attempt == 1 {
				return msgr.Message{}, errors.Join(msgr.ErrOutcomeUnknown, ErrStaleMap)
			}
			return testReplyOperation(t, 11, 22, 0, osd.OpAppend, nil), nil
		}}, nil
	})
	defer client.Close()
	result, err := client.Mutate(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, osd.Operation{Code: osd.OpAppend, Length: 1, Data: []byte("x")})
	if err != nil || result.Version != 22 || len(identities) != 2 || identities[0] != identities[1] {
		t.Fatalf("result=%+v error=%v identities=%v", result, err, identities)
	}
}

func TestMutationOutcomeUnknownSurvivesControlPlaneFailure(t *testing.T) {
	route0 := testRoute(t, 10, 0, "192.0.2.10:6800")
	route1 := testRoute(t, 11, 1, "192.0.2.11:6800")
	routeFailure := errors.New("route failed")
	sessionFailure := errors.New("session failed")

	for _, test := range []struct {
		name       string
		second     func() (Route, error)
		factoryErr error
		want       error
	}{
		{name: "route", second: func() (Route, error) { return Route{}, routeFailure }, want: routeFailure},
		{name: "no primary", second: func() (Route, error) { return Route{}, nil }, want: ErrNoPrimary},
		{name: "session", second: func() (Route, error) { return route1, nil }, factoryErr: sessionFailure, want: sessionFailure},
	} {
		t.Run(test.name, func(t *testing.T) {
			routes := 0
			router := routeFunc(func(Target) (Route, error) {
				routes++
				if routes == 1 {
					return route0, nil
				}
				return test.second()
			})
			client := newTestClient(t, &fakeMapSource{}, router, func(id int32, _ protocol.EntityAddrVec) (session, error) {
				if id == route1.Primary && test.factoryErr != nil {
					return nil, test.factoryErr
				}
				return &fakeSession{submit: func(context.Context, msgr.Message) (msgr.Message, error) {
					return msgr.Message{}, errors.Join(msgr.ErrOutcomeUnknown, msgr.ErrReconnectExhausted)
				}}, nil
			})
			defer client.Close()

			_, err := client.Mutate(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, osd.Operation{Code: osd.OpAppend, Length: 1, Data: []byte("x")})
			if !errors.Is(err, msgr.ErrOutcomeUnknown) || !errors.Is(err, test.want) {
				t.Fatalf("error=%v, want outcome unknown and %v", err, test.want)
			}
		})
	}
}

func TestMutationEAGAINAllocatesNewIdentity(t *testing.T) {
	route := testRoute(t, 10, 0, "192.0.2.10:6800")
	var identities []uint64
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: route}, func(int32, protocol.EntityAddrVec) (session, error) {
		return &fakeSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
			identities = append(identities, message.Header.TransactionID)
			if len(identities) == 1 {
				return testReplyOperation(t, 10, 0, -11, osd.OpWrite, nil), nil
			}
			return testReplyOperation(t, 10, 20, 0, osd.OpWrite, nil), nil
		}}, nil
	})
	defer client.Close()
	_, err := client.Mutate(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, osd.Operation{Code: osd.OpWrite, Length: 1, Data: []byte("x")})
	if err != nil || len(identities) != 2 || identities[0] == identities[1] {
		t.Fatalf("error=%v identities=%v", err, identities)
	}
}

func TestMutationMalformedReplyIsOutcomeUnknown(t *testing.T) {
	route := testRoute(t, 10, 0, "192.0.2.10:6800")
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: route}, func(int32, protocol.EntityAddrVec) (session, error) {
		return &fakeSession{submit: func(context.Context, msgr.Message) (msgr.Message, error) {
			return msgr.Message{Header: msgr.MessageHeader{Type: protocol.MessageOSDOpReply, Version: 8}, Front: []byte{1}}, nil
		}}, nil
	})
	defer client.Close()
	_, err := client.Mutate(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, osd.Operation{Code: osd.OpDelete})
	if !errors.Is(err, msgr.ErrOutcomeUnknown) {
		t.Fatalf("error=%v", err)
	}
}

func TestMutationRequiresDurableReply(t *testing.T) {
	route := testRoute(t, 10, 0, "192.0.2.10:6800")
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: route}, func(int32, protocol.EntityAddrVec) (session, error) {
		return &fakeSession{submit: func(context.Context, msgr.Message) (msgr.Message, error) {
			return testReplyRetryFlags(t, 10, 19, 0, osd.OpWrite, -1, int64(osd.FlagAck), nil), nil
		}}, nil
	})
	defer client.Close()

	_, err := client.Mutate(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, osd.Operation{Code: osd.OpWrite})
	if !errors.Is(err, msgr.ErrOutcomeUnknown) {
		t.Fatalf("error=%v", err)
	}
}

func TestMutationOutcomeUnknownSurvivesLaterRecoveryErrors(t *testing.T) {
	route := testRoute(t, 10, 0, "192.0.2.10:6800")
	laterFailure := errors.New("later transport failure")
	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "queue saturation", err: msgr.ErrQueueSaturated},
		{name: "transport failure", err: laterFailure},
		{name: "cancellation", err: context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			attempts := 0
			client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: route}, func(int32, protocol.EntityAddrVec) (session, error) {
				return &fakeSession{submit: func(context.Context, msgr.Message) (msgr.Message, error) {
					attempts++
					if attempts == 1 {
						return msgr.Message{}, errors.Join(msgr.ErrOutcomeUnknown, msgr.ErrReconnectExhausted)
					}
					return msgr.Message{}, test.err
				}}, nil
			})
			defer client.Close()

			_, err := client.Mutate(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, osd.Operation{Code: osd.OpAppend, Length: 1, Data: []byte("x")})
			if !errors.Is(err, msgr.ErrOutcomeUnknown) || !errors.Is(err, test.err) {
				t.Fatalf("error=%v, want outcome unknown and %v", err, test.err)
			}
			if err := client.Flush(context.Background()); !errors.Is(err, msgr.ErrOutcomeUnknown) {
				t.Fatalf("flush error=%v", err)
			}
		})
	}
}

func TestMutationAdmissionCopiesPayloadAndBoundsCapacity(t *testing.T) {
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{}, func(int32, protocol.EntityAddrVec) (session, error) { return nil, errors.New("unused") })
	client.config.MaxMutations = 1
	client.config.MaxMutationBytes = 3
	defer client.Close()
	payload := []byte("abc")
	sequence, _, owned, err := client.admitMutation(context.Background(), osd.Operation{Code: osd.OpWrite, Length: 3, Data: payload})
	if err != nil {
		t.Fatal(err)
	}
	payload[0] = 'x'
	if string(owned.Data) != "abc" {
		t.Fatalf("owned payload=%q", owned.Data)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, _, _, err := client.admitMutation(ctx, osd.Operation{Code: osd.OpDelete}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("capacity error=%v", err)
	}
	client.completeMutation(sequence, nil)
	if _, _, _, err := client.admitMutation(context.Background(), osd.Operation{Code: osd.OpDelete}); err != nil {
		t.Fatalf("released admission error=%v", err)
	}
}

func TestFlushUsesAdmissionWatermark(t *testing.T) {
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{}, func(int32, protocol.EntityAddrVec) (session, error) { return nil, errors.New("unused") })
	defer client.Close()
	first, _, _, err := client.admitMutation(context.Background(), osd.Operation{Code: osd.OpDelete})
	if err != nil {
		t.Fatal(err)
	}
	flushDone := make(chan error, 1)
	go func() { flushDone <- client.Flush(context.Background()) }()
	time.Sleep(10 * time.Millisecond)
	second, _, _, err := client.admitMutation(context.Background(), osd.Operation{Code: osd.OpDelete})
	if err != nil {
		t.Fatal(err)
	}
	client.completeMutation(first, nil)
	select {
	case err := <-flushDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("flush waited for mutation admitted after its watermark")
	}
	client.completeMutation(second, nil)
}

func TestFlushDrainsWatermarkBeforeReportingOutcomeUnknown(t *testing.T) {
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{}, func(int32, protocol.EntityAddrVec) (session, error) { return nil, errors.New("unused") })
	defer client.Close()
	first, _, _, err := client.admitMutation(context.Background(), osd.Operation{Code: osd.OpDelete})
	if err != nil {
		t.Fatal(err)
	}
	second, _, _, err := client.admitMutation(context.Background(), osd.Operation{Code: osd.OpDelete})
	if err != nil {
		t.Fatal(err)
	}
	flushDone := make(chan error, 1)
	go func() { flushDone <- client.Flush(context.Background()) }()
	time.Sleep(10 * time.Millisecond)
	client.completeMutation(first, msgr.ErrOutcomeUnknown)
	select {
	case err := <-flushDone:
		t.Fatalf("flush returned before watermark drained: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	client.completeMutation(second, nil)
	select {
	case err := <-flushDone:
		if !errors.Is(err, msgr.ErrOutcomeUnknown) {
			t.Fatalf("flush error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("flush did not return after watermark drained")
	}
}

func TestMutationRejectsSnapshotAndMalformedExtent(t *testing.T) {
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{}, func(int32, protocol.EntityAddrVec) (session, error) { return nil, errors.New("unused") })
	defer client.Close()
	for _, test := range []struct {
		target    Target
		operation osd.Operation
	}{
		{target: Target{Snapshot: 1}, operation: osd.Operation{Code: osd.OpDelete}},
		{target: Target{Snapshot: osd.NoSnap}, operation: osd.Operation{Code: osd.OpWrite, Length: 2, Data: []byte("x")}},
		{target: Target{Snapshot: osd.NoSnap}, operation: osd.Operation{Code: osd.OpZero, Offset: ^uint64(0), Length: 2}},
		{target: Target{Snapshot: osd.NoSnap}, operation: osd.Operation{Code: osd.OpDelete, Data: []byte("unexpected")}},
		{target: Target{Snapshot: osd.NoSnap}, operation: osd.Operation{Code: osd.OpSetXattr, XattrNameLength: 1, XattrValueLength: 2, Data: []byte("short")}},
	} {
		if _, err := client.Mutate(context.Background(), test.target, test.operation); err == nil {
			t.Fatalf("accepted target=%+v operation=%+v", test.target, test.operation)
		}
	}
}

func TestMutationIdentityExhaustionFailsClosed(t *testing.T) {
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{}, func(int32, protocol.EntityAddrVec) (session, error) { return nil, errors.New("unused") })
	defer client.Close()
	client.nextTransaction = ^uint64(0)
	if _, _, _, err := client.admitMutation(context.Background(), osd.Operation{Code: osd.OpDelete}); err == nil {
		t.Fatal("transaction identity exhaustion accepted")
	}
	client.nextTransaction = 1
	client.nextMutation = ^uint64(0)
	if _, _, _, err := client.admitMutation(context.Background(), osd.Operation{Code: osd.OpDelete}); err == nil {
		t.Fatal("mutation sequence exhaustion accepted")
	}
}
