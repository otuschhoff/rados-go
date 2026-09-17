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
			watch := &Watch{primary: 2, errors: make(chan error, 1), reconnect: make(chan struct{}, 1)}
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
	watch := &Watch{primary: 2, errors: make(chan error, 1), reconnect: make(chan struct{}, 1)}
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
	decoder.Uint64()
	decoder.Uint64()
	return decoder.Uint8()
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
