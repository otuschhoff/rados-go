package objecter

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/otuschhoff/go-librados/internal/maps"
	"github.com/otuschhoff/go-librados/internal/msgr"
	"github.com/otuschhoff/go-librados/internal/osd"
	"github.com/otuschhoff/go-librados/internal/protocol"
)

var backoffTestLimits = osd.Limits{MaxBytes: 4096, MaxOperations: 4}

func TestOSDSessionBackoffBlocksUntilUnblock(t *testing.T) {
	transport := newFakeOSDTransport()
	session := newOSDSession(transport, backoffTestLimits, time.Second)
	pg := maps.PG{Pool: 7, Seed: 3, Preferred: -1}
	object := osd.HObject{Object: "object", Snapshot: osd.NoSnap, Hash: 5, Pool: 7}
	block := osd.Backoff{PG: pg, Shard: -1, MapEpoch: 9, Operation: osd.BackoffBlock, ID: 42, Begin: object, End: object}
	transport.incoming <- encodeBackoffMessage(t, block)
	select {
	case ack := <-transport.sent:
		if ack.Header.Type != protocol.MessageOSDBackoff {
			t.Fatalf("ack type=%d", ack.Header.Type)
		}
	case <-time.After(time.Second):
		t.Fatal("backoff was not acknowledged")
	}
	waited := make(chan error, 1)
	go func() { waited <- session.Wait(context.Background(), pg, object) }()
	select {
	case err := <-waited:
		t.Fatalf("blocked wait returned early: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	unblock := block
	unblock.Operation = osd.BackoffUnblock
	transport.incoming <- encodeBackoffMessage(t, unblock)
	select {
	case err := <-waited:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("unblock did not wake waiter")
	}
}

func TestOSDSessionBackoffIsSelectiveAndCancelable(t *testing.T) {
	transport := newFakeOSDTransport()
	session := newOSDSession(transport, backoffTestLimits, time.Second)
	pg := maps.PG{Pool: 7, Seed: 3, Preferred: -1}
	object := osd.HObject{Object: "object", Snapshot: osd.NoSnap, Hash: 5, Pool: 7}
	transport.incoming <- encodeBackoffMessage(t, osd.Backoff{PG: pg, Shard: -1, Operation: osd.BackoffBlock, ID: 1, Begin: object, End: object})
	<-transport.sent
	if err := session.Wait(context.Background(), maps.PG{Pool: 7, Seed: 4, Preferred: -1}, object); err != nil {
		t.Fatalf("unrelated PG wait: %v", err)
	}
	if err := session.Wait(context.Background(), pg, osd.HObject{Object: "other", Snapshot: osd.NoSnap, Hash: 5, Pool: 7}); err != nil {
		t.Fatalf("unrelated object wait: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := session.Wait(ctx, pg, object); !errors.Is(err, context.Canceled) {
		t.Fatalf("wait error=%v", err)
	}
}

func TestOSDSessionMalformedBackoffFailsSession(t *testing.T) {
	transport := newFakeOSDTransport()
	session := newOSDSession(transport, backoffTestLimits, time.Second)
	transport.incoming <- msgr.Message{Header: msgr.MessageHeader{Type: protocol.MessageOSDBackoff, Version: 1, CompatVersion: 1}, Front: []byte{1}}
	select {
	case <-transport.done:
	case <-time.After(time.Second):
		t.Fatal("malformed backoff did not stop transport")
	}
	err := session.Wait(context.Background(), maps.PG{}, osd.HObject{})
	if err == nil || errors.Is(err, msgr.ErrSessionClosed) {
		t.Fatalf("wait error=%v", err)
	}
}

func TestOSDSessionMapMessageForcesRefreshRecovery(t *testing.T) {
	transport := newFakeOSDTransport()
	session := newOSDSession(transport, backoffTestLimits, time.Second)
	transport.incoming <- msgr.Message{Header: msgr.MessageHeader{Type: protocol.MessageOSDMap}}
	select {
	case <-transport.done:
	case <-time.After(time.Second):
		t.Fatal("OSD map did not stop stale session")
	}
	if err := session.Wait(context.Background(), maps.PG{}, osd.HObject{}); !errors.Is(err, ErrStaleMap) {
		t.Fatalf("wait error=%v", err)
	}
}

func TestOSDSessionOrdersBackoffAgainstRequestAdmission(t *testing.T) {
	transport := newFakeOSDTransport()
	transport.admissionStarted = make(chan struct{})
	transport.admit = make(chan struct{})
	session := newOSDSession(transport, backoffTestLimits, time.Second)
	pg := maps.PG{Pool: 7, Seed: 3, Preferred: -1}
	object := osd.HObject{Object: "object", Snapshot: osd.NoSnap, Hash: 5, Pool: 7}
	result := make(chan error, 1)
	go func() {
		_, err := session.SubmitTarget(context.Background(), pg, object, msgr.Message{})
		result <- err
	}()
	<-transport.admissionStarted
	transport.incoming <- encodeBackoffMessage(t, osd.Backoff{PG: pg, Shard: -1, Operation: osd.BackoffBlock, ID: 1, Begin: object, End: object})
	select {
	case <-transport.sent:
		t.Fatal("backoff registered before the earlier request was admitted")
	case <-time.After(20 * time.Millisecond):
	}
	close(transport.admit)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	select {
	case <-transport.sent:
	case <-time.After(time.Second):
		t.Fatal("backoff was not processed after admission")
	}
}

func TestOSDSessionTerminalErrorWakesBackoffWaiter(t *testing.T) {
	transport := newFakeOSDTransport()
	session := newOSDSession(transport, backoffTestLimits, time.Second)
	pg := maps.PG{Pool: 7, Seed: 3, Preferred: -1}
	object := osd.HObject{Object: "object", Snapshot: osd.NoSnap, Hash: 5, Pool: 7}
	transport.incoming <- encodeBackoffMessage(t, osd.Backoff{PG: pg, Shard: -1, Operation: osd.BackoffBlock, ID: 1, Begin: object, End: object})
	<-transport.sent
	waited := make(chan error, 1)
	go func() { waited <- session.Wait(context.Background(), pg, object) }()
	terminalErr := errors.New("terminal transport failure")
	transport.terminal <- terminalErr
	select {
	case err := <-waited:
		if !errors.Is(err, terminalErr) {
			t.Fatalf("wait error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("terminal error did not wake waiter")
	}
}

func TestClientCloseJoinsRealOSDSessionDispatcher(t *testing.T) {
	transport := newFakeOSDTransport()
	session := newOSDSession(transport, backoffTestLimits, time.Second)
	client := &Client{
		done:             make(chan struct{}),
		sessions:         map[int32]sessionEntry{1: {session: session}},
		pendingMutations: make(map[uint64]uint64),
		mutationChanged:  make(chan struct{}),
		watches:          make(map[uint64]*Watch),
		notifies:         make(map[uint64]chan notifyCompletion),
	}
	client.workers.Add(1)
	go func() {
		defer client.workers.Done()
		client.dispatchNotifications(1, session, session)
	}()
	closed := make(chan struct{})
	go func() {
		client.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("client close did not join real OSD session workers")
	}
	select {
	case <-session.Notifications():
	case <-time.After(time.Second):
		t.Fatal("OSD session notification channel did not close")
	}
}

func encodeBackoffMessage(t testing.TB, backoff osd.Backoff) msgr.Message {
	t.Helper()
	block := backoff
	if block.Operation == osd.BackoffUnblock {
		block.Operation = osd.BackoffBlock
	}
	message, err := osd.EncodeBackoffAcknowledgment(block, backoffTestLimits)
	if err != nil {
		t.Fatal(err)
	}
	message.Front[28] = backoff.Operation
	return message
}

type fakeOSDTransport struct {
	incoming         chan msgr.Message
	sent             chan msgr.Message
	done             chan struct{}
	terminal         chan error
	stopOnce         sync.Once
	admissionStarted chan struct{}
	admit            chan struct{}
}

func newFakeOSDTransport() *fakeOSDTransport {
	return &fakeOSDTransport{incoming: make(chan msgr.Message, 4), sent: make(chan msgr.Message, 4), done: make(chan struct{}), terminal: make(chan error, 1)}
}

func (transport *fakeOSDTransport) Submit(context.Context, msgr.Message) (msgr.Message, error) {
	return msgr.Message{}, nil
}

func (transport *fakeOSDTransport) SubmitAdmitted(_ context.Context, _ msgr.Message, admitted func()) (msgr.Message, error) {
	if transport.admissionStarted != nil {
		close(transport.admissionStarted)
		<-transport.admit
	}
	admitted()
	return msgr.Message{}, nil
}

func (transport *fakeOSDTransport) Send(ctx context.Context, message msgr.Message) error {
	select {
	case transport.sent <- message:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (transport *fakeOSDTransport) Incoming() <-chan msgr.Message { return transport.incoming }
func (transport *fakeOSDTransport) Terminal() <-chan error        { return transport.terminal }
func (transport *fakeOSDTransport) Done() <-chan struct{}         { return transport.done }
func (transport *fakeOSDTransport) Stop() {
	transport.stopOnce.Do(func() { close(transport.done) })
}
