package objecter

import (
	"context"
	"encoding/binary"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	wire "github.com/otuschhoff/rados-go/internal/encoding"
	"github.com/otuschhoff/rados-go/internal/msgr"
	"github.com/otuschhoff/rados-go/internal/osd"
	"github.com/otuschhoff/rados-go/internal/protocol"
)

type notificationFakeSession struct {
	*fakeSession
	notifications   chan osd.WatchNotification
	interruptions   chan error
	notificationErr error
}

func (session *notificationFakeSession) Notifications() <-chan osd.WatchNotification {
	return session.notifications
}

func (session *notificationFakeSession) NotificationError() error {
	if session.notificationErr != nil {
		return session.notificationErr
	}
	return msgr.ErrSessionClosed
}

func (session *notificationFakeSession) Interruptions() <-chan error {
	return session.interruptions
}

func TestWatchRejectsUnboundedQueue(t *testing.T) {
	client := &Client{}
	_, err := client.Watch(context.Background(), Target{Snapshot: osd.NoSnap}, maxWatchQueue+1, 1)
	if !errors.Is(err, wire.ErrMalformed) {
		t.Fatalf("error=%v", err)
	}
}

func TestWatchDispatchOverflowIsObservable(t *testing.T) {
	route := testRoute(t, 10, 0, "192.0.2.10:6800")
	active := &notificationFakeSession{notifications: make(chan osd.WatchNotification, 4)}
	active.fakeSession = &fakeSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
		codes, _, _ := decodeRequestOperationsForTest(t, message)
		return testReplyOperations(t, 10, 1, int64(osd.FlagOnDisk), []osd.OperationResult{{Operation: codes[0]}}), nil
	}}
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: route}, func(int32, protocol.EntityAddrVec) (session, error) { return active, nil })
	defer client.Close()
	watch, err := client.Watch(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, 1, 3600)
	if err != nil {
		t.Fatal(err)
	}
	active.notifications <- osd.WatchNotification{Opcode: osd.WatchEventNotify, Cookie: watch.Cookie(), NotifyID: 1}
	active.notifications <- osd.WatchNotification{Opcode: osd.WatchEventNotify, Cookie: watch.Cookie(), NotifyID: 2}
	select {
	case <-watch.Done():
	case <-time.After(time.Second):
		t.Fatal("watch did not stop after overflow")
	}
	if err := <-watch.Errors(); !errors.Is(err, msgr.ErrQueueSaturated) || !errors.Is(err, ErrWatchInterrupted) {
		t.Fatalf("watch error=%v", err)
	}
}

func TestNotifyPreservesPartialTimeoutResult(t *testing.T) {
	route := testRoute(t, 10, 0, "192.0.2.10:6800")
	active := &notificationFakeSession{notifications: make(chan osd.WatchNotification, 1)}
	active.fakeSession = &fakeSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
		codes, _, _ := decodeRequestOperationsForTest(t, message)
		var notifyID [8]byte
		binary.LittleEndian.PutUint64(notifyID[:], 77)
		go func() {
			active.notifications <- osd.WatchNotification{Opcode: osd.WatchEventComplete, Cookie: decodeNotifyCookieForTest(t, message), NotifyID: 77, Result: -110, Data: notifyResultForTest(t)}
		}()
		return testReplyOperation(t, 10, 1, 0, codes[0], notifyID[:]), nil
	}}
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: route}, func(int32, protocol.EntityAddrVec) (session, error) { return active, nil })
	defer client.Close()
	notification, err := client.Notify(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, []byte("data"), 1)
	if err == nil || notification.NotifyID != 77 || len(notification.Data) == 0 {
		t.Fatalf("notification=%+v error=%v", notification, err)
	}
}

func TestCoordinationMalformedRepliesAreOutcomeUnknown(t *testing.T) {
	route := testRoute(t, 10, 0, "192.0.2.10:6800")
	for _, test := range []struct {
		name   string
		invoke func(*Client) error
	}{
		{name: "ack", invoke: func(client *Client) error {
			watch := &Watch{client: client, target: Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, cookie: 1}
			return watch.Ack(context.Background(), 2, nil)
		}},
		{name: "notify", invoke: func(client *Client) error {
			_, err := client.Notify(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, nil, 1)
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: route}, func(int32, protocol.EntityAddrVec) (session, error) {
				return &fakeSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
					codes, _, _ := decodeRequestOperationsForTest(t, message)
					if codes[0] == osd.OpNotifyAck {
						return testReplyOperation(t, 10, 1, 0, osd.OpRead, nil), nil
					}
					return testReplyOperation(t, 10, 1, 0, codes[0], []byte{1}), nil
				}}, nil
			})
			defer client.Close()
			if err := test.invoke(client); !errors.Is(err, msgr.ErrOutcomeUnknown) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestWatchAckUsesReadRouting(t *testing.T) {
	route := testRoute(t, 10, 0, "192.0.2.10:6800")
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: route}, func(int32, protocol.EntityAddrVec) (session, error) {
		return &fakeSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
			codes, flags, _ := decodeRequestOperationsForTest(t, message)
			if len(codes) != 1 || codes[0] != osd.OpNotifyAck || flags&osd.FlagRead == 0 || flags&(osd.FlagWrite|osd.FlagOnDisk) != 0 {
				t.Fatalf("codes=%v flags=%#x", codes, flags)
			}
			return testReplyOperation(t, 10, 1, 0, osd.OpNotifyAck, nil), nil
		}}, nil
	})
	defer client.Close()
	watch := &Watch{client: client, target: Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, cookie: 1}
	if err := watch.Ack(context.Background(), 2, nil); err != nil {
		t.Fatal(err)
	}
}

func TestCoordinationTransportOutcomeUnknownIsPreserved(t *testing.T) {
	route := testRoute(t, 10, 0, "192.0.2.10:6800")
	for _, test := range []struct {
		name   string
		invoke func(context.Context, *Client) error
	}{
		{name: "ack", invoke: func(ctx context.Context, client *Client) error {
			watch := &Watch{client: client, target: Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, cookie: 1}
			return watch.Ack(ctx, 2, nil)
		}},
		{name: "notify", invoke: func(ctx context.Context, client *Client) error {
			_, err := client.Notify(ctx, Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, nil, 1)
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: route}, func(int32, protocol.EntityAddrVec) (session, error) {
				return &fakeSession{submit: func(context.Context, msgr.Message) (msgr.Message, error) {
					return msgr.Message{}, errors.Join(msgr.ErrOutcomeUnknown, msgr.ErrReconnectExhausted)
				}}, nil
			})
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			if err := test.invoke(ctx, client); !errors.Is(err, msgr.ErrOutcomeUnknown) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestClientCloseSettlesPendingNotify(t *testing.T) {
	route := testRoute(t, 10, 0, "192.0.2.10:6800")
	admitted := make(chan struct{})
	active := &notificationFakeSession{notifications: make(chan osd.WatchNotification)}
	active.fakeSession = &fakeSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
		var notifyID [8]byte
		binary.LittleEndian.PutUint64(notifyID[:], 1)
		close(admitted)
		return testReplyOperation(t, 10, 1, 0, osd.OpNotify, notifyID[:]), nil
	}}
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: route}, func(int32, protocol.EntityAddrVec) (session, error) { return active, nil })
	done := make(chan error, 1)
	go func() {
		_, err := client.Notify(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, nil, 1)
		done <- err
	}()
	<-admitted
	client.Close()
	select {
	case err := <-done:
		if !errors.Is(err, ErrClosed) || !errors.Is(err, msgr.ErrOutcomeUnknown) {
			t.Fatalf("pending notify error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending notify did not settle on close")
	}
}

func TestBeginShutdownAccountsForPendingNotify(t *testing.T) {
	route := testRoute(t, 10, 0, "192.0.2.10:6800")
	submitted := make(chan struct{})
	active := &notificationFakeSession{notifications: make(chan osd.WatchNotification)}
	active.fakeSession = &fakeSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
		var notifyID [8]byte
		binary.LittleEndian.PutUint64(notifyID[:], 1)
		close(submitted)
		return testReplyOperation(t, 10, 1, 0, osd.OpNotify, notifyID[:]), nil
	}}
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: route}, func(int32, protocol.EntityAddrVec) (session, error) { return active, nil })
	notifyDone := make(chan error, 1)
	go func() {
		_, err := client.Notify(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, nil, 1)
		notifyDone <- err
	}()
	<-submitted
	client.BeginShutdown()
	if err := client.Flush(context.Background()); !errors.Is(err, msgr.ErrOutcomeUnknown) {
		t.Fatalf("flush error=%v", err)
	}
	if err := <-notifyDone; !errors.Is(err, ErrClosed) || !errors.Is(err, msgr.ErrOutcomeUnknown) {
		t.Fatalf("notify error=%v", err)
	}
	client.Close()
}

func TestWatchRegistrationWaitsForPrimaryBeyondAttemptBudget(t *testing.T) {
	recovered := testRoute(t, 11, 0, "192.0.2.10:6800")
	router := &fakeRouter{route: Route{Epoch: 10, Primary: -1}}
	refreshes := 0
	source := &fakeMapSource{refresh: func() {
		refreshes++
		if refreshes == 5 {
			router.set(recovered)
		}
	}}
	active := &notificationFakeSession{notifications: make(chan osd.WatchNotification)}
	active.fakeSession = &fakeSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
		_ = decodeWatchOperationForTest(t, message)
		return testReplyOperation(t, 11, 1, int32(osd.FlagOnDisk), osd.OpWatch, nil), nil
	}}
	client := newTestClient(t, source, router, func(int32, protocol.EntityAddrVec) (session, error) { return active, nil })
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	watch, err := client.Watch(ctx, Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, 1, 30)
	if err != nil {
		t.Fatal(err)
	}
	if refreshes != 5 || watch.primaryOSD() != recovered.Primary {
		t.Fatalf("refreshes=%d primary=%d", refreshes, watch.primaryOSD())
	}
	if err := watch.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestWatchRegistrationTracksSuccessfulRoute(t *testing.T) {
	initial := testRoute(t, 10, 0, "192.0.2.10:6800")
	registered := testRoute(t, 11, 1, "192.0.2.11:6800")
	router := &advancingRouter{routes: []Route{initial, registered}}
	active := &notificationFakeSession{notifications: make(chan osd.WatchNotification)}
	active.fakeSession = &fakeSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
		_ = decodeWatchOperationForTest(t, message)
		return testReplyOperation(t, registered.Epoch, 1, int32(osd.FlagOnDisk), osd.OpWatch, nil), nil
	}}
	client := newTestClient(t, &fakeMapSource{}, router, func(osdID int32, _ protocol.EntityAddrVec) (session, error) {
		if osdID != registered.Primary {
			t.Fatalf("opened OSD %d, want %d", osdID, registered.Primary)
		}
		return active, nil
	})
	defer client.Close()

	watch, err := client.Watch(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, 1, 30)
	if err != nil {
		t.Fatal(err)
	}
	if watch.primaryOSD() != registered.Primary {
		t.Fatalf("watch primary=%d, want successful registration primary %d", watch.primaryOSD(), registered.Primary)
	}
	if err := watch.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestWatchRecoveryOutlivesAttemptBudgetWithStableIdentity(t *testing.T) {
	const failedReconnects = 5
	route := testRoute(t, 10, 0, "192.0.2.10:6800")
	type request struct {
		operation  uint8
		cookie     uint64
		generation uint32
	}
	requests := make(chan request, failedReconnects+4)
	var reconnects atomic.Int32
	active := &notificationFakeSession{notifications: make(chan osd.WatchNotification, 2)}
	active.fakeSession = &fakeSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
		operation, cookie, generation := decodeWatchRequestForTest(t, message)
		requests <- request{operation: operation, cookie: cookie, generation: generation}
		result := int32(osd.FlagOnDisk)
		if operation == osd.WatchOperationReconnect && reconnects.Add(1) <= failedReconnects {
			result = -107
		}
		return testReplyOperation(t, 10, 1, result, osd.OpWatch, nil), nil
	}}
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: route}, func(int32, protocol.EntityAddrVec) (session, error) { return active, nil })
	defer client.Close()
	watch, err := client.Watch(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, 2, 30)
	if err != nil {
		t.Fatal(err)
	}
	initial := <-requests
	if initial.operation != osd.WatchOperationRegister || initial.generation != 0 || initial.cookie != watch.Cookie() {
		t.Fatalf("initial request=%+v watch_cookie=%d", initial, watch.Cookie())
	}
	active.notifications <- osd.WatchNotification{Opcode: osd.WatchEventDisconnect, Cookie: watch.Cookie()}
	if err := <-watch.Errors(); !errors.Is(err, ErrWatchInterrupted) {
		t.Fatalf("interruption error=%v", err)
	}
	for expectedGeneration := uint32(1); expectedGeneration <= failedReconnects+1; expectedGeneration++ {
		select {
		case attempted := <-requests:
			if attempted.operation != osd.WatchOperationReconnect || attempted.cookie != initial.cookie || attempted.cookie != watch.Cookie() || attempted.generation != expectedGeneration {
				t.Fatalf("request=%+v initial_cookie=%d watch_cookie=%d want_generation=%d", attempted, initial.cookie, watch.Cookie(), expectedGeneration)
			}
		case <-time.After(time.Second):
			t.Fatalf("reconnect generation %d was not attempted", expectedGeneration)
		}
	}
	active.notifications <- osd.WatchNotification{Opcode: osd.WatchEventNotify, Cookie: watch.Cookie(), NotifyID: 77, Data: []byte("after recovery")}
	select {
	case event := <-watch.Events():
		if event.NotifyID != 77 || string(event.Data) != "after recovery" {
			t.Fatalf("event=%+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("notification was not delivered after recovery")
	}
	select {
	case duplicate := <-watch.Events():
		t.Fatalf("duplicate event=%+v", duplicate)
	case <-time.After(20 * time.Millisecond):
	}
	if err := watch.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestWatchRecoveryStopsOnTerminalObjectError(t *testing.T) {
	route := testRoute(t, 10, 0, "192.0.2.10:6800")
	reconnectStarted := make(chan struct{})
	releaseReconnect := make(chan struct{})
	active := &notificationFakeSession{notifications: make(chan osd.WatchNotification, 1)}
	active.fakeSession = &fakeSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
		if decodeWatchOperationForTest(t, message) == osd.WatchOperationReconnect {
			close(reconnectStarted)
			<-releaseReconnect
			return testReplyOperation(t, 10, 1, -2, osd.OpWatch, nil), nil
		}
		return testReplyOperation(t, 10, 1, int32(osd.FlagOnDisk), osd.OpWatch, nil), nil
	}}
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: route}, func(int32, protocol.EntityAddrVec) (session, error) { return active, nil })
	watch, err := client.Watch(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, 1, 30)
	if err != nil {
		t.Fatal(err)
	}
	active.notifications <- osd.WatchNotification{Opcode: osd.WatchEventDisconnect, Cookie: watch.Cookie()}
	<-reconnectStarted
	if err := <-watch.Errors(); !errors.Is(err, ErrWatchInterrupted) {
		t.Fatalf("interruption error=%v", err)
	}
	close(releaseReconnect)
	select {
	case <-watch.Done():
	case <-time.After(time.Second):
		t.Fatal("terminal object error did not stop watch")
	}
	if err := <-watch.Errors(); !errors.Is(err, protocol.WireErrno(-2)) {
		t.Fatalf("terminal error=%v", err)
	}
	client.Close()
}

func TestWatchRecoveryStopsPromptlyOnClose(t *testing.T) {
	for _, closeClient := range []bool{false, true} {
		name := "watch"
		if closeClient {
			name = "client"
		}
		t.Run(name, func(t *testing.T) {
			route := testRoute(t, 10, 0, "192.0.2.10:6800")
			started := make(chan struct{})
			var startedOnce atomic.Bool
			active := &notificationFakeSession{notifications: make(chan osd.WatchNotification, 1)}
			active.fakeSession = &fakeSession{submit: func(ctx context.Context, message msgr.Message) (msgr.Message, error) {
				operation := decodeWatchOperationForTest(t, message)
				if operation == osd.WatchOperationReconnect {
					if startedOnce.CompareAndSwap(false, true) {
						close(started)
					}
					<-ctx.Done()
					return msgr.Message{}, ctx.Err()
				}
				return testReplyOperation(t, 10, 1, int32(osd.FlagOnDisk), osd.OpWatch, nil), nil
			}}
			client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: route}, func(int32, protocol.EntityAddrVec) (session, error) { return active, nil })
			watch, err := client.Watch(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, 1, 30)
			if err != nil {
				t.Fatal(err)
			}
			active.notifications <- osd.WatchNotification{Opcode: osd.WatchEventDisconnect, Cookie: watch.Cookie()}
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("recovery did not start")
			}
			closed := make(chan error, 1)
			go func() {
				if closeClient {
					client.Close()
					closed <- nil
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				closed <- watch.Close(ctx)
			}()
			select {
			case err := <-closed:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("close did not cancel watch recovery")
			}
			if !closeClient {
				client.Close()
			}
		})
	}
}

func TestWatchReconnectsWhenPrimaryChanges(t *testing.T) {
	route0 := testRoute(t, 10, 0, "192.0.2.10:6800")
	route1 := testRoute(t, 11, 1, "192.0.2.11:6800")
	var remapped atomic.Bool
	router := routeFunc(func(Target) (Route, error) {
		if remapped.Load() {
			return route1, nil
		}
		return route0, nil
	})
	operations := make(chan uint8, 4)
	factory := func(primary int32, _ protocol.EntityAddrVec) (session, error) {
		return &notificationFakeSession{
			fakeSession: &fakeSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
				operation := decodeWatchOperationForTest(t, message)
				operations <- operation
				epoch := uint32(10)
				if primary == 1 {
					epoch = 11
				}
				return testReplyOperation(t, epoch, 1, int32(osd.FlagOnDisk), osd.OpWatch, nil), nil
			}},
			notifications: make(chan osd.WatchNotification),
		}, nil
	}
	client := newTestClient(t, &fakeMapSource{}, router, factory)
	defer client.Close()
	watch, err := client.Watch(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if operation := <-operations; operation != osd.WatchOperationRegister {
		t.Fatalf("initial operation=%d", operation)
	}
	remapped.Store(true)
	select {
	case operation := <-operations:
		if operation != osd.WatchOperationReconnect {
			t.Fatalf("remap operation=%d", operation)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("watch did not reconnect after primary change")
	}
	if err := watch.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestWatchReconnectsAfterDisconnectEvent(t *testing.T) {
	route := testRoute(t, 10, 0, "192.0.2.10:6800")
	operations := make(chan uint8, 4)
	active := &notificationFakeSession{notifications: make(chan osd.WatchNotification, 1)}
	active.fakeSession = &fakeSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
		operations <- decodeWatchOperationForTest(t, message)
		return testReplyOperation(t, 10, 1, int32(osd.FlagOnDisk), osd.OpWatch, nil), nil
	}}
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: route}, func(int32, protocol.EntityAddrVec) (session, error) { return active, nil })
	defer client.Close()
	watch, err := client.Watch(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, 1, 30)
	if err != nil {
		t.Fatal(err)
	}
	if operation := <-operations; operation != osd.WatchOperationRegister {
		t.Fatalf("initial operation=%d", operation)
	}
	active.notifications <- osd.WatchNotification{Opcode: osd.WatchEventDisconnect, Cookie: watch.Cookie()}
	select {
	case err := <-watch.Errors():
		if !errors.Is(err, ErrWatchInterrupted) {
			t.Fatalf("disconnect error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("disconnect did not report possible event loss")
	}
	select {
	case operation := <-operations:
		if operation != osd.WatchOperationReconnect {
			t.Fatalf("disconnect operation=%d", operation)
		}
	case <-time.After(time.Second):
		t.Fatal("watch did not reconnect after disconnect")
	}
	select {
	case <-watch.Done():
		t.Fatal("disconnect terminated watch")
	default:
	}
	<-watch.ops
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := watch.Close(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled close error=%v", err)
	}
	watch.ops <- struct{}{}
	if err := watch.Close(context.Background()); err != nil {
		t.Fatalf("retry close: %v", err)
	}
	if operation := <-operations; operation != osd.WatchOperationUnwatch {
		t.Fatalf("close retry operation=%d", operation)
	}
}

func TestWatchReconnectsAfterTransportReset(t *testing.T) {
	route := testRoute(t, 10, 0, "192.0.2.10:6800")
	operations := make(chan uint8, 4)
	active := &notificationFakeSession{notifications: make(chan osd.WatchNotification), interruptions: make(chan error, 1)}
	active.fakeSession = &fakeSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
		operation := decodeWatchOperationForTest(t, message)
		_, flags, _ := decodeRequestOperationsForTest(t, message)
		if flags&(osd.FlagRead|osd.FlagWrite) != osd.FlagRead|osd.FlagWrite || flags&osd.FlagOnDisk != 0 {
			t.Errorf("watch operation %d flags=%#x", operation, flags)
		}
		operations <- operation
		return testReplyOperation(t, 10, 1, int32(osd.FlagOnDisk), osd.OpWatch, nil), nil
	}}
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: route}, func(int32, protocol.EntityAddrVec) (session, error) { return active, nil })
	defer client.Close()
	watch, err := client.Watch(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, 1, 30)
	if err != nil {
		t.Fatal(err)
	}
	if operation := <-operations; operation != osd.WatchOperationRegister {
		t.Fatalf("initial operation=%d", operation)
	}
	active.interruptions <- msgr.ErrSessionDisconnected
	select {
	case err := <-watch.Errors():
		if !errors.Is(err, ErrWatchInterrupted) {
			t.Fatalf("reset error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("reset did not report interruption")
	}
	select {
	case operation := <-operations:
		if operation != osd.WatchOperationReconnect {
			t.Fatalf("reset operation=%d", operation)
		}
	case <-time.After(time.Second):
		t.Fatal("watch did not reconnect after transport reset")
	}
	if err := watch.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestWatchReportsFailedPingBeforeReregistering(t *testing.T) {
	route := testRoute(t, 10, 0, "192.0.2.10:6800")
	operations := make(chan uint8, 4)
	active := &notificationFakeSession{notifications: make(chan osd.WatchNotification)}
	active.fakeSession = &fakeSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
		operation := decodeWatchOperationForTest(t, message)
		operations <- operation
		if operation == osd.WatchOperationPing {
			return testReplyOperation(t, 10, 1, -5, osd.OpWatch, nil), nil
		}
		return testReplyOperation(t, 10, 1, 0, osd.OpWatch, nil), nil
	}}
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: route}, func(int32, protocol.EntityAddrVec) (session, error) { return active, nil })
	defer client.Close()
	watch, err := client.Watch(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if operation := <-operations; operation != osd.WatchOperationRegister {
		t.Fatalf("initial operation=%d", operation)
	}
	for _, expected := range []uint8{osd.WatchOperationPing, osd.WatchOperationReconnect} {
		select {
		case operation := <-operations:
			if operation != expected {
				t.Fatalf("operation=%d want=%d", operation, expected)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("operation %d was not attempted", expected)
		}
	}
	select {
	case err := <-watch.Errors():
		if !errors.Is(err, ErrWatchInterrupted) {
			t.Fatalf("watch error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("failed ping did not report possible event loss")
	}
	select {
	case <-watch.Done():
		t.Fatal("successful recovery terminated watch")
	default:
	}
}

func TestWatchCloseIsBoundedWhileOperationIsActive(t *testing.T) {
	client := &Client{watches: make(map[uint64]*Watch)}
	watch := &Watch{client: client, cookie: 1, events: make(chan osd.WatchNotification, 1), errors: make(chan error, 1), done: make(chan struct{}), ops: make(chan struct{}, 1)}
	client.watches[watch.cookie] = watch
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := watch.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close error=%v", err)
	}
	select {
	case <-watch.Done():
	default:
		t.Fatal("bounded close did not terminate local watch")
	}
}

func TestWatchDispatchConcurrentStopDoesNotPanic(t *testing.T) {
	const cookie = 1
	watch := &Watch{cookie: cookie, events: make(chan osd.WatchNotification, 1), errors: make(chan error, 1), done: make(chan struct{})}
	client := &Client{watches: map[uint64]*Watch{cookie: watch}}
	active := &notificationFakeSession{fakeSession: &fakeSession{}, notifications: make(chan osd.WatchNotification, 128)}
	client.sessions = map[int32]sessionEntry{0: {session: active}}
	dispatched := make(chan struct{})
	go func() {
		client.dispatchNotifications(0, active, active)
		close(dispatched)
	}()
	for index := 0; index < cap(active.notifications); index++ {
		active.notifications <- osd.WatchNotification{Opcode: osd.WatchEventNotify, Cookie: cookie, NotifyID: uint64(index + 1)}
	}
	watch.stop(ErrClosed)
	close(active.notifications)
	select {
	case <-dispatched:
	case <-time.After(time.Second):
		t.Fatal("notification dispatcher did not stop")
	}
}

func TestWatchSessionFailureReportsPossibleLossAndReconnects(t *testing.T) {
	for _, failure := range []error{msgr.ErrSessionClosed, msgr.ErrQueueSaturated} {
		t.Run(failure.Error(), func(t *testing.T) {
			active := &notificationFakeSession{fakeSession: &fakeSession{}, notifications: make(chan osd.WatchNotification), notificationErr: failure}
			watch := &Watch{route: Route{Primary: 2}, errors: make(chan error, 1), reconnect: make(chan struct{}, 1)}
			client := &Client{sessions: map[int32]sessionEntry{2: {session: active}}, watches: map[uint64]*Watch{1: watch}}
			closed := make(chan struct{})
			go func() {
				client.dispatchNotifications(2, active, active)
				close(closed)
			}()
			close(active.notifications)
			select {
			case <-closed:
			case <-time.After(time.Second):
				t.Fatal("dispatcher did not stop")
			}
			if err := <-watch.errors; !errors.Is(err, ErrWatchInterrupted) || !errors.Is(err, failure) {
				t.Fatalf("watch error=%v", err)
			}
			select {
			case <-watch.reconnect:
			default:
				t.Fatal("watch reconnect was not requested")
			}
		})
	}
}

func TestSessionInvalidationReportsWatchInterruptionBeforeDispatcherCloses(t *testing.T) {
	active := &notificationFakeSession{fakeSession: &fakeSession{}, notifications: make(chan osd.WatchNotification)}
	watch := &Watch{route: Route{Primary: 2}, errors: make(chan error, 1), reconnect: make(chan struct{}, 1)}
	client := &Client{sessions: map[int32]sessionEntry{2: {session: active}}, watches: map[uint64]*Watch{1: watch}}
	client.invalidate(2, active)
	if err := <-watch.errors; !errors.Is(err, ErrWatchInterrupted) || !errors.Is(err, msgr.ErrSessionClosed) {
		t.Fatalf("watch error=%v", err)
	}
	select {
	case <-watch.reconnect:
	default:
		t.Fatal("watch reconnect was not requested")
	}
	close(active.notifications)
}

func decodeNotifyCookieForTest(t testing.TB, message msgr.Message) uint64 {
	t.Helper()
	decoder := wire.NewDecoder(message.Front, wire.Limits{MaxBytes: 4096})
	_, spg := decoder.Versioned(1)
	_, _ = decodeRequestPGForTest(spg)
	spg.Uint8()
	decoder.Uint32()
	decoder.Uint32()
	decoder.Uint32()
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
	_ = decoder.String()
	decoder.Uint16()
	decoder.Uint16()
	decoder.Uint32()
	return decoder.Uint64()
}

func decodeWatchOperationForTest(t testing.TB, message msgr.Message) uint8 {
	operation, _, _ := decodeWatchRequestForTest(t, message)
	return operation
}

func decodeWatchRequestForTest(t testing.TB, message msgr.Message) (uint8, uint64, uint32) {
	t.Helper()
	decoder := wire.NewDecoder(message.Front, wire.Limits{MaxBytes: 4096})
	_, spg := decoder.Versioned(1)
	_, _ = decodeRequestPGForTest(spg)
	spg.Uint8()
	decoder.Uint32()
	decoder.Uint32()
	decoder.Uint32()
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
	_ = decoder.String()
	if decoder.Uint16() != 1 || decoder.Uint16() != osd.OpWatch {
		t.Fatal("request is not a single watch operation")
	}
	decoder.Uint32()
	cookie := decoder.Uint64()
	decoder.Uint64()
	operation := decoder.Uint8()
	generation := decoder.Uint32()
	return operation, cookie, generation
}

func notifyResultForTest(t testing.TB) []byte {
	t.Helper()
	encoder := wire.NewEncoder(128)
	encoder.Uint32(1)
	encoder.Uint64(1)
	encoder.Uint64(2)
	encoder.Bytes([]byte("ack"))
	encoder.Uint32(1)
	encoder.Uint64(3)
	encoder.Uint64(4)
	data, err := encoder.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	return data
}
