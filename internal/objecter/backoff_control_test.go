package objecter

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	wire "github.com/otuschhoff/rados-go/internal/encoding"
	"github.com/otuschhoff/rados-go/internal/maps"
	"github.com/otuschhoff/rados-go/internal/msgr"
	"github.com/otuschhoff/rados-go/internal/osd"
	"github.com/otuschhoff/rados-go/internal/protocol"
)

type backoffControlTransport struct {
	reads  chan msgr.Frame
	writes chan msgr.Frame
	done   chan struct{}
	once   sync.Once
}

func newBackoffControlTransport() *backoffControlTransport {
	return &backoffControlTransport{reads: make(chan msgr.Frame, 8), writes: make(chan msgr.Frame, 8), done: make(chan struct{})}
}

func (transport *backoffControlTransport) ReadFrame() (msgr.Frame, error) {
	select {
	case frame := <-transport.reads:
		return frame, nil
	case <-transport.done:
		return msgr.Frame{}, io.EOF
	}
}

func (transport *backoffControlTransport) WriteFrame(frame msgr.Frame) error {
	select {
	case transport.writes <- frame:
		return nil
	case <-transport.done:
		return io.EOF
	}
}

func (transport *backoffControlTransport) Close() error {
	transport.once.Do(func() { close(transport.done) })
	return nil
}

func TestBackoffControlProgressUnderApplicationSaturation(t *testing.T) {
	for _, budget := range []string{"count", "bytes"} {
		t.Run(budget, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			transport := newBackoffControlTransport()
			limits := msgr.Limits{MaxSegmentBytes: 4096, MaxFrameBytes: 8192}
			config := msgr.SessionConfig{Limits: limits, MaxQueuedMessages: 1, MaxRetainedBytes: 8192, MaxInFlightTransactions: 1, MaxHandshakeTransitions: 8, EventBuffer: 16}
			if budget == "bytes" {
				config.MaxQueuedMessages = 8
				config.MaxRetainedBytes = uint64(msgr.MessageHeaderSize + 128)
			}
			raw, err := msgr.NewSession(transport, nil, config)
			if err != nil {
				t.Fatal(err)
			}
			session := newOSDSession(raw, backoffTestLimits, time.Second)
			defer session.Stop()
			admitted := make(chan struct{})
			result := make(chan error, 1)
			go func() {
				reply, err := raw.SubmitAdmitted(ctx, msgr.Message{Header: msgr.MessageHeader{Type: protocol.MessageOSDOp}, Data: make([]byte, 128), Lengths: msgr.MessageLengths{Data: 128}}, func() { close(admitted) })
				if err == nil && string(reply.Front) != "reply" {
					err = errors.New("application reply payload changed")
				}
				result <- err
			}()
			select {
			case <-admitted:
			case <-ctx.Done():
				t.Fatal("application admission timed out")
			}
			var request msgr.Message
			select {
			case frame := <-transport.writes:
				request, err = msgr.DecodeMessage(frame, limits)
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("application write timed out")
			}
			object := osd.HObject{Object: "blocked", Snapshot: osd.NoSnap, Hash: 5, Pool: 7}
			block := osd.Backoff{PG: maps.PG{Pool: 7, Seed: 3, Preferred: -1}, Shard: -1, Operation: osd.BackoffBlock, ID: 42, Begin: object, End: object}
			message := encodeBackoffMessage(t, block)
			message.Header.Sequence = 1
			frame, err := msgr.EncodeMessage(message, limits)
			if err != nil {
				t.Fatal(err)
			}
			transport.reads <- frame
			for {
				select {
				case frame := <-transport.writes:
					if frame.Tag != msgr.TagMessage {
						continue
					}
					ack, err := msgr.DecodeMessage(frame, limits)
					if err != nil || ack.Header.Type != protocol.MessageOSDBackoff || ack.Front[28] != osd.BackoffAckBlock {
						t.Fatalf("unexpected control message: %+v, %v", ack.Header, err)
					}
					select {
					case err := <-result:
						t.Fatalf("unrelated application terminated before its reply: %v", err)
					default:
					}
					reply := msgr.Message{Header: msgr.MessageHeader{Type: protocol.MessageOSDOpReply, Sequence: 2, TransactionID: request.Header.TransactionID, AckSequence: ack.Header.Sequence}, Front: []byte("reply"), Lengths: msgr.MessageLengths{Front: 5}}
					replyFrame, err := msgr.EncodeMessage(reply, limits)
					if err != nil {
						t.Fatal(err)
					}
					transport.reads <- replyFrame
					if err := controlAwait(t, ctx, result); err != nil {
						t.Fatalf("application reply failed after control ACK: %v", err)
					}
					return
				case <-raw.Done():
					t.Fatalf("ordinary %s saturation stopped session: %v", budget, session.NotificationError())
				case <-ctx.Done():
					t.Fatal("backoff ACK made no progress with application budget full")
				}
			}
		})
	}
}

func TestBackoffStateLimits(t *testing.T) {
	object := osd.HObject{Object: "bounded", Snapshot: osd.NoSnap, Hash: 5, Pool: 7}
	block := osd.Backoff{PG: maps.PG{Pool: 7, Seed: 1, Preferred: -1}, Shard: -1, Operation: osd.BackoffBlock, ID: 1, Begin: object, End: object}
	for _, bound := range []string{"count", "bytes"} {
		t.Run(bound, func(t *testing.T) {
			session := &osdSession{backoffs: make(map[uint64]osd.Backoff), maxBackoffs: 1, maxBackoffBytes: backoffCharge(block)}
			if err := session.addBackoff(block); err != nil {
				t.Fatal(err)
			}
			if err := session.addBackoff(block); err != nil || session.backoffBytes != backoffCharge(block) {
				t.Fatalf("duplicate charge: %d, %v", session.backoffBytes, err)
			}
			other := block
			if bound == "count" {
				other.ID++
				session.maxBackoffBytes *= 2
			} else {
				other.Begin.Key = "larger replacement"
			}
			if err := session.addBackoff(other); !errors.Is(err, msgr.ErrQueueSaturated) {
				t.Fatalf("overflow = %v", err)
			}
			if len(session.backoffs) != 1 || session.backoffs[block.ID] != block || session.backoffBytes != backoffCharge(block) {
				t.Fatal("rejected block changed installed state")
			}
			session.removeBackoff(block.ID)
			if session.backoffBytes != 0 {
				t.Fatal("unblock did not release charged bytes")
			}
		})
	}
}

func TestBackoffStateOverflowStopsBeforeACK(t *testing.T) {
	for _, bound := range []string{"count", "bytes"} {
		t.Run(bound, func(t *testing.T) {
			transport := newFakeOSDTransport()
			session := newOSDSessionWithBackoffLimits(transport, backoffTestLimits, time.Second, 1, 8<<20)
			defer session.Stop()
			object := osd.HObject{Object: "bounded", Snapshot: osd.NoSnap, Hash: 5, Pool: 7}
			block := osd.Backoff{PG: maps.PG{Pool: 7, Seed: 1, Preferred: -1}, Shard: -1, Operation: osd.BackoffBlock, ID: 1, Begin: object, End: object}
			if bound == "bytes" {
				session.mu.Lock()
				session.maxBackoffs = 2
				session.maxBackoffBytes = backoffCharge(block)
				session.mu.Unlock()
			}
			transport.incoming <- encodeBackoffMessage(t, block)
			select {
			case <-transport.sent:
			case <-time.After(time.Second):
				t.Fatal("first block not acknowledged")
			}
			block.ID++
			transport.incoming <- encodeBackoffMessage(t, block)
			select {
			case <-session.dispatcherDone:
			case <-time.After(time.Second):
				t.Fatal("overflow did not stop affected session")
			}
			select {
			case <-transport.sent:
				t.Fatal("uninstalled block acknowledged")
			default:
			}
			if !errors.Is(session.NotificationError(), msgr.ErrQueueSaturated) || session.backoffBytes != 0 {
				t.Fatal("overflow error or terminal accounting changed")
			}
		})
	}
}

func TestBackoffStateDuplicateOverlapAndReplacementAccounting(t *testing.T) {
	const population = 4096
	session := &osdSession{backoffs: make(map[uint64]osd.Backoff)}
	object := osd.HObject{Object: "overlapping", Snapshot: osd.NoSnap, Hash: 5, Pool: 7}
	verify := func(expected int) {
		t.Helper()
		if len(session.backoffs) != expected {
			t.Fatalf("active IDs = %d, want %d", len(session.backoffs), expected)
		}
		indexed := 0
		for pg, ranges := range session.backoffsByPG {
			if len(ranges) == 0 {
				t.Fatal("empty PG index retained")
			}
			for id, backoff := range ranges {
				current, exists := session.backoffs[id]
				if !exists || current != backoff || backoff.PG != pg {
					t.Fatalf("PG index diverged for ID %d", id)
				}
				indexed++
			}
		}
		if indexed != expected {
			t.Fatalf("indexed IDs = %d, want %d", indexed, expected)
		}
	}
	for index := 0; index < population; index++ {
		backoff := osd.Backoff{PG: maps.PG{Pool: 7, Seed: uint32(index % 4), Preferred: -1}, Shard: -1, Operation: osd.BackoffBlock, ID: uint64(index), Begin: object, End: object}
		if err := session.addBackoff(backoff); err != nil {
			t.Fatal(err)
		}
		if err := session.addBackoff(backoff); err != nil {
			t.Fatal(err)
		}
	}
	verify(population)
	for index := 0; index < population; index++ {
		backoff := session.backoffs[uint64(index)]
		backoff.PG.Seed += 4
		backoff.Begin.Key, backoff.End.Key = "replacement", "replacement"
		if err := session.addBackoff(backoff); err != nil {
			t.Fatal(err)
		}
	}
	verify(population)
	if len(session.backoffsByPG) != 4 {
		t.Fatalf("replacement retained old PG indexes: %d", len(session.backoffsByPG))
	}
	if _, exists := session.removeBackoff(population + 1); exists {
		t.Fatal("unknown unblock removed active state")
	}
	for index := 0; index < population; index++ {
		if _, exists := session.removeBackoff(uint64(index)); !exists {
			t.Fatalf("active ID %d disappeared", index)
		}
		if _, exists := session.removeBackoff(uint64(index)); exists {
			t.Fatalf("duplicate unblock removed ID %d twice", index)
		}
	}
	verify(0)
}

func TestBackoffStateOverflowPreservesDispatchedUnknownOutcome(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	transport := newBackoffControlTransport()
	limits := msgr.Limits{MaxSegmentBytes: 4096, MaxFrameBytes: 8192}
	raw, err := msgr.NewSession(transport, nil, msgr.SessionConfig{Limits: limits, MaxQueuedMessages: 8, MaxRetainedBytes: 8192, MaxInFlightTransactions: 8, MaxHandshakeTransitions: 8, EventBuffer: 16})
	if err != nil {
		t.Fatal(err)
	}
	session := newOSDSessionWithBackoffLimits(raw, backoffTestLimits, time.Second, 1, 8<<20)
	defer session.Stop()
	object := osd.HObject{Object: "dispatched", Snapshot: osd.NoSnap, Hash: 5, Pool: 7}
	result := make(chan error, 1)
	go func() {
		_, err := session.SubmitTarget(ctx, maps.PG{Pool: 7, Seed: 9, Preferred: -1}, object, msgr.Message{Header: msgr.MessageHeader{Type: protocol.MessageOSDOp}, Data: []byte("mutation"), Lengths: msgr.MessageLengths{Data: 8}})
		result <- err
	}()
	select {
	case <-transport.writes:
	case <-ctx.Done():
		t.Fatal("request not dispatched")
	}
	for index := 1; index <= 2; index++ {
		message := encodeBackoffMessage(t, osd.Backoff{PG: maps.PG{Pool: 7, Seed: 3, Preferred: -1}, Shard: -1, Operation: osd.BackoffBlock, ID: uint64(index), Begin: object, End: object})
		message.Header.Sequence = uint64(index)
		frame, err := msgr.EncodeMessage(message, limits)
		if err != nil {
			t.Fatal(err)
		}
		transport.reads <- frame
		if index == 1 {
			for {
				select {
				case sent := <-transport.writes:
					if sent.Tag != msgr.TagMessage {
						continue
					}
					ack, err := msgr.DecodeMessage(sent, limits)
					if err != nil {
						t.Fatal(err)
					}
					if ack.Header.Type != protocol.MessageOSDBackoff {
						t.Fatal("unexpected application replay")
					}
				case <-ctx.Done():
					t.Fatal("installed block not acknowledged")
				}
				break
			}
		}
	}
	select {
	case err := <-result:
		if !errors.Is(err, msgr.ErrOutcomeUnknown) || !errors.Is(err, msgr.ErrQueueSaturated) {
			t.Fatalf("dispatched outcome = %v", err)
		}
	case <-ctx.Done():
		t.Fatal("overflow did not terminate dispatched request")
	}
}

type controlSend struct {
	ctx     context.Context
	message msgr.Message
	release chan error
}

type gatedBackoffTransport struct {
	*fakeOSDTransport
	calls chan controlSend
}

func newGatedBackoffTransport() *gatedBackoffTransport {
	base := newFakeOSDTransport()
	base.incoming = make(chan msgr.Message)
	return &gatedBackoffTransport{fakeOSDTransport: base, calls: make(chan controlSend, 32)}
}

func (transport *gatedBackoffTransport) Send(ctx context.Context, message msgr.Message) error {
	call := controlSend{ctx: ctx, message: message, release: make(chan error, 1)}
	select {
	case transport.calls <- call:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-call.release:
		return err
	case <-ctx.Done():
		return errors.Join(msgr.ErrOutcomeUnknown, ctx.Err())
	}
}

func controlAwait[Value any](t *testing.T, ctx context.Context, channel <-chan Value) Value {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-ctx.Done():
		t.Fatal("control test deadline exceeded")
		var zero Value
		return zero
	}
}

func controlContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func controlBlock(id uint64) osd.Backoff {
	object := osd.HObject{Object: "blocked", Snapshot: osd.NoSnap, Hash: 5, Pool: 7}
	return osd.Backoff{PG: maps.PG{Pool: 7, Seed: 3, Preferred: -1}, Shard: -1, MapEpoch: 9, Operation: osd.BackoffBlock, ID: id, Begin: object, End: object}
}

func controlInject(t *testing.T, ctx context.Context, transport *gatedBackoffTransport, message msgr.Message) {
	t.Helper()
	for _, next := range []msgr.Message{message, {}} {
		select {
		case transport.incoming <- next:
		case <-ctx.Done():
			t.Fatal("receive dispatcher stalled")
		}
	}
}

func controlAssertReleased(t *testing.T, session *osdSession) {
	t.Helper()
	session.ackMu.Lock()
	defer session.ackMu.Unlock()
	if session.ackCount != 0 || session.ackBytes != 0 || len(session.ackQueue) != 0 || session.ackActive != nil {
		t.Fatalf("ACK memory retained: count=%d bytes=%d queue=%d active=%v", session.ackCount, session.ackBytes, len(session.ackQueue), session.ackActive != nil)
	}
	select {
	case <-session.notifications:
	default:
		t.Fatal("Stop returned before notification closure")
	}
}

func TestBackoffControlStalledWriterProcessesOverlappingUnblocks(t *testing.T) {
	ctx := controlContext(t)
	transport := newGatedBackoffTransport()
	session := newOSDSession(transport, backoffTestLimits, time.Hour)
	t.Cleanup(session.Stop)
	first := controlBlock(1)
	controlInject(t, ctx, transport, encodeBackoffMessage(t, first))
	active := controlAwait(t, ctx, transport.calls)
	second := controlBlock(2)
	controlInject(t, ctx, transport, encodeBackoffMessage(t, second))
	first.Operation = osd.BackoffUnblock
	controlInject(t, ctx, transport, encodeBackoffMessage(t, first))
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := session.Wait(canceled, second.PG, second.Begin); !errors.Is(err, context.Canceled) {
		t.Fatalf("overlapping backoff removed: %v", err)
	}
	second.Operation = osd.BackoffUnblock
	controlInject(t, ctx, transport, encodeBackoffMessage(t, second))
	if err := session.Wait(ctx, second.PG, second.Begin); err != nil {
		t.Fatalf("unblock stalled behind ACK: %v", err)
	}
	active.release <- nil
	next := controlAwait(t, ctx, transport.calls)
	for index, call := range []controlSend{active, next} {
		expected, err := osd.EncodeBackoffAcknowledgment(controlBlock(uint64(index+1)), backoffTestLimits)
		if err != nil || !bytes.Equal(call.message.Front, expected.Front) || call.message.Header != expected.Header {
			t.Fatalf("ACK %d changed original encoding: %v", index, err)
		}
	}
	next.release <- nil
	session.Stop()
	controlAssertReleased(t, session)
}

func TestBackoffControlReserveIncludesActiveAndQueued(t *testing.T) {
	for _, budget := range []string{"count", "bytes"} {
		t.Run(budget, func(t *testing.T) {
			ctx := controlContext(t)
			transport := newGatedBackoffTransport()
			session := newOSDSession(transport, backoffTestLimits, time.Hour)
			t.Cleanup(session.Stop)
			message := encodeBackoffMessage(t, controlBlock(1))
			if budget == "bytes" {
				message.Front = make([]byte, int(msgr.MaxControlRetainedBytes)-msgr.MessageHeaderSize)
			}
			if err := session.enqueueACK(message); err != nil {
				t.Fatal(err)
			}
			active := controlAwait(t, ctx, transport.calls)
			if budget == "count" {
				for index := 1; index < msgr.MaxControlMessages; index++ {
					if err := session.enqueueACK(message); err != nil {
						t.Fatalf("reserve entry %d rejected: %v", index, err)
					}
				}
			} else {
				message.Front[0] = 99
				if active.message.Front[0] != 0 {
					t.Fatal("enqueued ACK aliases caller payload")
				}
			}
			if err := session.enqueueACK(msgr.Message{}); !errors.Is(err, msgr.ErrQueueSaturated) {
				t.Fatalf("active ACK excluded from %s reserve: %v", budget, err)
			}
			transport.incoming <- encodeBackoffMessage(t, controlBlock(99))
			controlAwait(t, ctx, session.dispatcherDone)
			if !errors.Is(session.NotificationError(), msgr.ErrQueueSaturated) {
				t.Fatalf("reserve overflow did not fail-stop: %v", session.NotificationError())
			}
			controlAssertReleased(t, session)
		})
	}
}

func TestBackoffControlQueuedDeadlineStartsAtEnqueue(t *testing.T) {
	ctx := controlContext(t)
	transport := newGatedBackoffTransport()
	session := newOSDSession(transport, backoffTestLimits, time.Hour)
	t.Cleanup(session.Stop)
	if err := session.enqueueACK(encodeBackoffMessage(t, controlBlock(1))); err != nil {
		t.Fatal(err)
	}
	active := controlAwait(t, ctx, transport.calls)
	session.ackTimeout = 20 * time.Millisecond
	if err := session.enqueueACK(encodeBackoffMessage(t, controlBlock(2))); err != nil {
		t.Fatal(err)
	}
	session.ackMu.Lock()
	queuedCtx := session.ackQueue[0].ctx
	session.ackMu.Unlock()
	controlAwait(t, ctx, queuedCtx.Done())
	active.release <- nil
	controlAwait(t, ctx, session.dispatcherDone)
	if !errors.Is(session.NotificationError(), context.DeadlineExceeded) {
		t.Fatalf("queued deadline lost: %v", session.NotificationError())
	}
	select {
	case <-transport.calls:
		t.Fatal("expired queued ACK was sent")
	default:
	}
	controlAssertReleased(t, session)
}

func TestBackoffControlStalledWriterTimeoutAndStop(t *testing.T) {
	for _, exit := range []string{"timeout", "stop", "map", "done"} {
		t.Run(exit, func(t *testing.T) {
			ctx := controlContext(t)
			transport := newGatedBackoffTransport()
			timeout := time.Hour
			if exit == "timeout" {
				timeout = 20 * time.Millisecond
			}
			session := newOSDSession(transport, backoffTestLimits, timeout)
			t.Cleanup(session.Stop)
			controlInject(t, ctx, transport, encodeBackoffMessage(t, controlBlock(1)))
			active := controlAwait(t, ctx, transport.calls)
			switch exit {
			case "stop":
				stopped := make(chan struct{})
				go func() { session.Stop(); close(stopped) }()
				controlAwait(t, ctx, stopped)
			case "map":
				select {
				case transport.incoming <- msgr.Message{Header: msgr.MessageHeader{Type: protocol.MessageOSDMap}}:
				case <-ctx.Done():
					t.Fatal("map receive stalled")
				}
			case "done":
				transport.Stop()
			}
			controlAwait(t, ctx, session.dispatcherDone)
			controlAwait(t, ctx, active.ctx.Done())
			err := session.NotificationError()
			if errors.Is(err, msgr.ErrOutcomeUnknown) {
				t.Fatalf("ACK classified unrelated mutations as unknown: %v", err)
			}
			if exit == "timeout" && !errors.Is(err, context.DeadlineExceeded) || exit == "map" && !errors.Is(err, ErrStaleMap) {
				t.Fatalf("wrong failure for %s: %v", exit, err)
			}
			controlAssertReleased(t, session)
		})
	}
}

func TestBackoffControlResetCancelsOldGeneration(t *testing.T) {
	ctx := controlContext(t)
	transport := newGatedBackoffTransport()
	observed := make(chan bool, 4)
	session := newOSDSession(transport, backoffTestLimits, time.Hour, func(available bool, _ error) { observed <- available })
	t.Cleanup(session.Stop)
	controlInject(t, ctx, transport, encodeBackoffMessage(t, controlBlock(1)))
	old := controlAwait(t, ctx, transport.calls)
	controlInject(t, ctx, transport, encodeBackoffMessage(t, controlBlock(2)))
	transport.resets <- struct{}{}
	controlAwait(t, ctx, session.interruptions)
	controlAwait(t, ctx, old.ctx.Done())
	if controlAwait(t, ctx, observed) {
		t.Fatal("reset reported available")
	}
	transport.events <- msgr.SessionEvent{Kind: msgr.EventReconnectOK}
	if !controlAwait(t, ctx, observed) {
		t.Fatal("reconnect not processed")
	}
	if err := session.Wait(ctx, controlBlock(1).PG, controlBlock(1).Begin); err != nil {
		t.Fatalf("old ACK cancellation failed healthy generation: %v", err)
	}
	controlInject(t, ctx, transport, encodeBackoffMessage(t, controlBlock(3)))
	fresh := controlAwait(t, ctx, transport.calls)
	expected, _ := osd.EncodeBackoffAcknowledgment(controlBlock(3), backoffTestLimits)
	if !bytes.Equal(fresh.message.Front, expected.Front) {
		t.Fatal("old queued ACK crossed reset")
	}
	fresh.release <- nil
	controlInject(t, ctx, transport, msgr.Message{})
	session.mu.Lock()
	err := session.err
	session.mu.Unlock()
	if err != nil {
		t.Fatalf("old generation result stopped recovered session: %v", err)
	}
	session.Stop()
	controlAssertReleased(t, session)
}

type resetHeldSession struct {
	*msgr.Session
	resets chan struct{}
}

func (session *resetHeldSession) Resets() <-chan struct{} { return session.resets }

type integratedControlGate struct {
	*backoffControlTransport
	gateType uint16
	started  chan msgr.Message
	release  chan struct{}
}

func (transport *integratedControlGate) WriteFrame(frame msgr.Frame) error {
	if frame.Tag == msgr.TagMessage {
		message, err := msgr.DecodeMessage(frame, integratedControlLimits)
		if err != nil {
			return err
		}
		if message.Header.Type == transport.gateType {
			select {
			case transport.started <- message:
			case <-transport.done:
				return io.EOF
			}
			select {
			case <-transport.release:
			case <-transport.done:
				return io.EOF
			}
		}
	}
	return transport.backoffControlTransport.WriteFrame(frame)
}

var integratedControlLimits = msgr.Limits{MaxSegmentBytes: 4096, MaxFrameBytes: 8192}

func integratedControlSession(t *testing.T, gateType uint16, timeout time.Duration) (*msgr.Session, *osdSession, *integratedControlGate, *backoffControlTransport) {
	t.Helper()
	first := &integratedControlGate{backoffControlTransport: newBackoffControlTransport(), gateType: gateType, started: make(chan msgr.Message, 8), release: make(chan struct{})}
	second := newBackoffControlTransport()
	config := msgr.SessionConfig{Limits: integratedControlLimits, MaxQueuedMessages: 8, MaxRetainedBytes: 65536, MaxInFlightTransactions: 8, MaxReconnectAttempts: 2, MaxHandshakeTransitions: 8, EventBuffer: 32, ReconnectPolicy: msgr.ReplayPending, ClientCookie: 11, ServerCookie: 22, InitialReconnectBackoff: time.Nanosecond, MaxReconnectBackoff: time.Nanosecond}
	raw, err := msgr.NewSession(first, msgr.ConnectorFunc(func(context.Context) (msgr.Transport, error) { return second, nil }), config)
	if err != nil {
		t.Fatal(err)
	}
	session := newOSDSession(&resetHeldSession{Session: raw, resets: make(chan struct{}, 1)}, backoffTestLimits, timeout)
	t.Cleanup(session.Stop)
	return raw, session, first, second
}

func integratedControlWait(t *testing.T, ctx context.Context, condition func() bool) {
	t.Helper()
	for !condition() {
		select {
		case <-ctx.Done():
			t.Fatal("integrated control barrier timed out")
		default:
			runtime.Gosched()
		}
	}
}

func integratedControlInject(t *testing.T, transport *backoffControlTransport, message msgr.Message) {
	t.Helper()
	frame, err := msgr.EncodeMessage(message, integratedControlLimits)
	if err != nil {
		t.Fatal(err)
	}
	transport.reads <- frame
}

func integratedControlReconnect(t *testing.T, ctx context.Context, first *integratedControlGate, second *backoffControlTransport) {
	t.Helper()
	first.Close()
	frame := controlAwait(t, ctx, second.writes)
	payload, err := msgr.DecodeControl(frame, integratedControlLimits)
	if _, ok := payload.(msgr.SessionReconnect); err != nil || !ok {
		t.Fatalf("reconnect handshake = %T, %v", payload, err)
	}
	frame, err = msgr.EncodeControl(msgr.SessionReconnectOK{}, integratedControlLimits)
	if err != nil {
		t.Fatal(err)
	}
	second.reads <- frame
}

func integratedControlStallReceiver(t *testing.T, ctx context.Context, raw *msgr.Session, session *osdSession, first *integratedControlGate) func() {
	t.Helper()
	session.mu.Lock()
	var once sync.Once
	unlock := func() { once.Do(session.mu.Unlock) }
	t.Cleanup(unlock)
	message := encodeBackoffMessage(t, controlBlock(99))
	message.Header.Sequence = 2
	integratedControlInject(t, first.backoffControlTransport, message)
	integratedControlWait(t, ctx, func() bool {
		snapshot, err := raw.Snapshot(ctx)
		return err == nil && snapshot.LastInboundSequence == 2 && len(raw.Incoming()) == 0
	})
	return unlock
}

func TestBackoffControlIntegratedNoACKAcrossOwnerReset(t *testing.T) {
	for _, stage := range []string{"unsent", "started"} {
		t.Run(stage, func(t *testing.T) {
			ctx := controlContext(t)
			gateType := uint16(protocol.MessageOSDBackoff)
			if stage == "unsent" {
				gateType = protocol.MessageOSDOp
			}
			raw, session, first, second := integratedControlSession(t, gateType, time.Hour)
			applicationResult := make(chan error, 1)
			go func() {
				_, err := raw.Submit(ctx, msgr.Message{Header: msgr.MessageHeader{Type: protocol.MessageOSDOp}, Front: []byte("mutation"), Lengths: msgr.MessageLengths{Front: 8}})
				applicationResult <- err
			}()
			var original msgr.Message
			if stage == "unsent" {
				original = controlAwait(t, ctx, first.started)
			} else {
				frame := controlAwait(t, ctx, first.writes)
				var err error
				original, err = msgr.DecodeMessage(frame, integratedControlLimits)
				if err != nil {
					t.Fatal(err)
				}
			}
			message := encodeBackoffMessage(t, controlBlock(1))
			message.Header.Sequence = 1
			integratedControlInject(t, first.backoffControlTransport, message)
			integratedControlWait(t, ctx, func() bool {
				snapshot, err := raw.Snapshot(ctx)
				return err == nil && snapshot.ControlQueued == 1
			})
			if stage == "started" {
				controlAwait(t, ctx, first.started)
			}
			unlock := integratedControlStallReceiver(t, ctx, raw, session, first)
			integratedControlReconnect(t, ctx, first, second)
			marker := make(chan error, 1)
			go func() {
				marker <- raw.Send(ctx, msgr.Message{Header: msgr.MessageHeader{Type: protocol.MessageOSDOp}, Front: []byte("marker"), Lengths: msgr.MessageLengths{Front: 6}})
			}()
			replayed := false
			for {
				frame := controlAwait(t, ctx, second.writes)
				if frame.Tag != msgr.TagMessage {
					continue
				}
				written, err := msgr.DecodeMessage(frame, integratedControlLimits)
				if err != nil {
					t.Fatal(err)
				}
				if written.Header.Type == protocol.MessageOSDBackoff {
					t.Fatalf("stale %s ACK transmitted on recovered transport before wrapper cancellation", stage)
				}
				if string(written.Front) == "mutation" {
					replayed = true
					if written.Header.Sequence != original.Header.Sequence || written.Header.TransactionID != original.Header.TransactionID {
						t.Fatal("application replay identity changed")
					}
				}
				if string(written.Front) == "marker" {
					break
				}
			}
			if !replayed || controlAwait(t, ctx, marker) != nil {
				t.Fatal("application replay or fresh send failed")
			}
			unlock()
			integratedControlWait(t, ctx, func() bool {
				session.ackMu.Lock()
				defer session.ackMu.Unlock()
				return session.ackCount == 0
			})
			go func() {
				marker <- raw.Send(ctx, msgr.Message{Header: msgr.MessageHeader{Type: protocol.MessageOSDOp}, Front: []byte("after"), Lengths: msgr.MessageLengths{Front: 5}})
			}()
			for {
				frame := controlAwait(t, ctx, second.writes)
				if frame.Tag != msgr.TagMessage {
					continue
				}
				written, err := msgr.DecodeMessage(frame, integratedControlLimits)
				if err != nil || written.Header.Type == protocol.MessageOSDBackoff {
					t.Fatalf("delayed inbound BLOCK ACK crossed reset: %+v, %v", written.Header, err)
				}
				if string(written.Front) == "after" {
					break
				}
			}
			if err := controlAwait(t, ctx, marker); err != nil {
				t.Fatal(err)
			}
			session.Stop()
			if err := controlAwait(t, ctx, applicationResult); !errors.Is(err, msgr.ErrOutcomeUnknown) {
				t.Fatalf("sent mutation terminal outcome = %v", err)
			}
		})
	}
}

func TestBackoffControlIntegratedStaleDeadlineAheadOfReset(t *testing.T) {
	for _, stage := range []string{"unsent", "sent"} {
		t.Run(stage, func(t *testing.T) { testBackoffControlIntegratedStaleDeadline(t, stage) })
	}
}

func testBackoffControlIntegratedStaleDeadline(t *testing.T, stage string) {
	ctx := controlContext(t)
	raw, session, first, second := integratedControlSession(t, protocol.MessageOSDBackoff, 100*time.Millisecond)
	mutationCtx, cancelMutation := context.WithCancel(ctx)
	defer cancelMutation()
	mutationResult := make(chan error, 1)
	mutation := msgr.Message{Header: msgr.MessageHeader{Type: protocol.MessageOSDOp}, Front: []byte("mutation"), Lengths: msgr.MessageLengths{Front: 8}}
	startMutation := func() {
		go func() {
			_, err := session.Submit(mutationCtx, mutation)
			mutationResult <- err
		}()
	}
	var original msgr.Message
	if stage == "sent" {
		startMutation()
		frame := controlAwait(t, ctx, first.writes)
		var err error
		original, err = msgr.DecodeMessage(frame, integratedControlLimits)
		if err != nil {
			t.Fatal(err)
		}
	}
	message := encodeBackoffMessage(t, controlBlock(1))
	message.Header.Sequence = 1
	integratedControlInject(t, first.backoffControlTransport, message)
	controlAwait(t, ctx, first.started)
	session.ackMu.Lock()
	deadline := session.ackActive.ctx
	session.ackMu.Unlock()
	if stage == "unsent" {
		startMutation()
		integratedControlWait(t, ctx, func() bool {
			snapshot, err := raw.Snapshot(ctx)
			return err == nil && snapshot.Queued == 1
		})
	}
	unlock := integratedControlStallReceiver(t, ctx, raw, session, first)
	controlAwait(t, ctx, deadline.Done())
	integratedControlWait(t, ctx, func() bool { return len(session.ackResults) == 1 })
	if stage == "unsent" {
		cancelMutation()
		if err := controlAwait(t, ctx, mutationResult); !errors.Is(err, context.Canceled) || errors.Is(err, msgr.ErrOutcomeUnknown) {
			t.Fatalf("unsent mutation outcome contaminated by ACK failure: %v", err)
		}
	}
	integratedControlReconnect(t, ctx, first, second)
	unlock()
	encoder := wire.NewEncoder(30)
	encoder.Uint8(1)
	encoder.Uint8(osd.WatchEventNotify)
	encoder.Uint64(1)
	encoder.Uint64(0)
	encoder.Uint64(1)
	encoder.Bytes(nil)
	front, err := encoder.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	notification := msgr.Message{Header: msgr.MessageHeader{Sequence: 3, Type: protocol.MessageWatchNotify, Version: 1, CompatVersion: 1}, Front: front, Lengths: msgr.MessageLengths{Front: uint32(len(front))}}
	integratedControlInject(t, second, notification)
	select {
	case <-session.notifications:
		select {
		case <-raw.Done():
			t.Fatalf("stale deadline stopped recovered raw session: %v", session.NotificationError())
		default:
		}
	case <-raw.Done():
		t.Fatalf("stale ACK deadline selected before reset stopped recovered session: %v", session.NotificationError())
	case <-ctx.Done():
		t.Fatal("recovered notification did not progress")
	}
	if stage == "sent" {
		for {
			frame := controlAwait(t, ctx, second.writes)
			if frame.Tag != msgr.TagMessage {
				continue
			}
			replayed, err := msgr.DecodeMessage(frame, integratedControlLimits)
			if err != nil || replayed.Header.Type != protocol.MessageOSDOp || replayed.Header.Sequence != original.Header.Sequence || replayed.Header.TransactionID != original.Header.TransactionID {
				t.Fatalf("mutation replay after stale ACK deadline = %+v, %v", replayed.Header, err)
			}
			break
		}
		cancelMutation()
		if err := controlAwait(t, ctx, mutationResult); !errors.Is(err, context.Canceled) || !errors.Is(err, msgr.ErrOutcomeUnknown) || errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("sent mutation lost its own unknown/canceled outcome: %v", err)
		}
	}
	session.mu.Lock()
	err = session.err
	session.mu.Unlock()
	if err != nil {
		t.Fatalf("stale ACK error failed healthy wrapper: %v", err)
	}
}

func TestBackoffControlFallbackPendingResetBeforeError(t *testing.T) {
	ctx := controlContext(t)
	transport := newGatedBackoffTransport()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	session := newOSDSession(transport, backoffTestLimits, time.Hour, func(available bool, _ error) {
		if !available {
			once.Do(func() { close(entered); <-release })
		}
	})
	t.Cleanup(session.Stop)
	t.Cleanup(func() { close(release) })
	transport.resets <- struct{}{}
	controlAwait(t, ctx, entered)
	session.ackMu.Lock()
	generation := session.ackGeneration
	session.ackMu.Unlock()
	session.ackResults <- backoffACKResult{err: context.DeadlineExceeded, generation: generation}
	transport.resets <- struct{}{}
	release <- struct{}{}
	controlAwait(t, ctx, session.interruptions)
	controlInject(t, ctx, transport, msgr.Message{})
	session.mu.Lock()
	err := session.err
	session.mu.Unlock()
	if err != nil {
		t.Fatalf("pending fallback reset lost to ACK error: %v", err)
	}
}

type authoritativeBackoffTransport struct {
	*gatedBackoffTransport
	generation atomic.Uint64
	returnGate chan struct{}
}

func newAuthoritativeBackoffTransport() *authoritativeBackoffTransport {
	transport := &authoritativeBackoffTransport{gatedBackoffTransport: newGatedBackoffTransport()}
	transport.generation.Store(1)
	transport.resets = make(chan struct{})
	return transport
}

func (transport *authoritativeBackoffTransport) ControlGeneration() uint64 {
	return transport.generation.Load()
}

func (transport *authoritativeBackoffTransport) SendControlGeneration(ctx context.Context, message msgr.Message, generation uint64) error {
	if generation != transport.ControlGeneration() {
		return msgr.ErrSessionDisconnected
	}
	err := transport.Send(ctx, message)
	if transport.returnGate != nil {
		select {
		case <-transport.returnGate:
		case <-transport.done:
		}
	}
	return err
}

func authoritativeControlInject(t *testing.T, ctx context.Context, transport *authoritativeBackoffTransport, message msgr.Message) {
	t.Helper()
	for _, next := range []msgr.Message{message, {}} {
		select {
		case transport.incoming <- next:
		case <-transport.done:
			t.Fatal("backoff stopped authoritative transport")
		case <-ctx.Done():
			t.Fatal("authoritative receive dispatcher stalled")
		}
	}
}

func authoritativeBlockMessage(t *testing.T, id, generation uint64) msgr.Message {
	t.Helper()
	message := encodeBackoffMessage(t, controlBlock(id))
	message.TransportGeneration = generation
	return message
}

func TestBackoffControlAuthoritativeInitialGeneration(t *testing.T) {
	transport := newAuthoritativeBackoffTransport()
	transport.generation.Store(7)
	session := newOSDSession(transport, backoffTestLimits, time.Hour)
	t.Cleanup(session.Stop)
	session.ackMu.Lock()
	generation := session.ackGeneration
	session.ackMu.Unlock()
	if generation != 7 {
		t.Fatalf("initial wrapper generation = %d, authoritative generation = 7", generation)
	}
}

func TestBackoffControlAuthoritativeDelayedDuplicateReset(t *testing.T) {
	ctx := controlContext(t)
	transport := newAuthoritativeBackoffTransport()
	observed := make(chan bool, 8)
	session := newOSDSession(transport, backoffTestLimits, time.Hour, func(available bool, _ error) { observed <- available })
	t.Cleanup(session.Stop)
	transport.generation.Store(2)
	authoritativeControlInject(t, ctx, transport, authoritativeBlockMessage(t, 1, 2))
	fresh := controlAwait(t, ctx, transport.calls)
	for index := 0; index < 2; index++ {
		select {
		case transport.resets <- struct{}{}:
		case <-ctx.Done():
			t.Fatal("reset dispatcher stalled")
		}
		authoritativeControlInject(t, ctx, transport, msgr.Message{})
		if fresh.ctx.Err() != nil {
			t.Fatalf("delayed reset %d canceled current ACK: %v", index, fresh.ctx.Err())
		}
		session.mu.Lock()
		_, blocked := session.backoffs[1]
		session.mu.Unlock()
		if !blocked {
			t.Fatalf("delayed reset %d cleared current BLOCK", index)
		}
	}
	if len(observed) != 1 || len(session.interruptions) != 1 {
		t.Fatalf("generation transition notifications: observations=%d interruptions=%d", len(observed), len(session.interruptions))
	}
	if controlAwait(t, ctx, observed) {
		t.Fatal("generation transition reported available")
	}
	transport.events <- msgr.SessionEvent{Kind: msgr.EventReconnectOK}
	if !controlAwait(t, ctx, observed) {
		t.Fatal("recovered generation not reported available")
	}
	select {
	case transport.resets <- struct{}{}:
	case <-ctx.Done():
		t.Fatal("duplicate reset after recovery stalled")
	}
	authoritativeControlInject(t, ctx, transport, msgr.Message{})
	if len(observed) != 0 || fresh.ctx.Err() != nil {
		t.Fatal("duplicate reset interrupted recovered generation")
	}
	fresh.release <- nil
	integratedControlWait(t, ctx, func() bool {
		session.ackMu.Lock()
		defer session.ackMu.Unlock()
		return session.ackCount == 0
	})
	session.Stop()
	controlAssertReleased(t, session)
}

func TestBackoffControlAuthoritativeStaleBlockFullReserve(t *testing.T) {
	for _, stamp := range []uint64{0, 1} {
		t.Run(fmt.Sprint(stamp), func(t *testing.T) {
			ctx := controlContext(t)
			transport := newAuthoritativeBackoffTransport()
			transport.returnGate = make(chan struct{})
			session := newOSDSession(transport, backoffTestLimits, time.Hour)
			t.Cleanup(session.Stop)
			authoritativeControlInject(t, ctx, transport, authoritativeBlockMessage(t, 1, 1))
			active := controlAwait(t, ctx, transport.calls)
			for index := 1; index < msgr.MaxControlMessages; index++ {
				if err := session.enqueueACK(authoritativeBlockMessage(t, uint64(index+1), 1)); err != nil {
					t.Fatal(err)
				}
			}
			transport.generation.Store(2)
			authoritativeControlInject(t, ctx, transport, authoritativeBlockMessage(t, 99, stamp))
			authoritativeControlInject(t, ctx, transport, authoritativeBlockMessage(t, 100, 2))
			controlAwait(t, ctx, active.ctx.Done())
			session.mu.Lock()
			_, stale := session.backoffs[99]
			_, fresh := session.backoffs[100]
			session.mu.Unlock()
			if stale || !fresh {
				t.Fatalf("backoffs after transition: stale=%v fresh=%v", stale, fresh)
			}
			session.ackMu.Lock()
			count, queued := session.ackCount, len(session.ackQueue)
			session.ackMu.Unlock()
			if count != 2 || queued != 1 {
				t.Fatalf("old active accounting or queued release lost: count=%d queued=%d", count, queued)
			}
			close(transport.returnGate)
			freshACK := controlAwait(t, ctx, transport.calls)
			if freshACK.message.TransportGeneration != 2 {
				t.Fatal("old queued ACK crossed generation transition")
			}
			freshACK.release <- nil
			session.Stop()
			controlAssertReleased(t, session)
		})
	}
}

func TestBackoffControlAuthoritativeStaleUnblockReusedID(t *testing.T) {
	ctx := controlContext(t)
	transport := newAuthoritativeBackoffTransport()
	transport.generation.Store(2)
	session := newOSDSession(transport, backoffTestLimits, time.Hour)
	t.Cleanup(session.Stop)
	authoritativeControlInject(t, ctx, transport, authoritativeBlockMessage(t, 1, 2))
	active := controlAwait(t, ctx, transport.calls)
	unblock := controlBlock(1)
	unblock.Operation = osd.BackoffUnblock
	message := encodeBackoffMessage(t, unblock)
	message.TransportGeneration = 1
	authoritativeControlInject(t, ctx, transport, message)
	session.mu.Lock()
	_, blocked := session.backoffs[1]
	session.mu.Unlock()
	if !blocked || active.ctx.Err() != nil {
		t.Fatal("stale UNBLOCK removed current reused ID or canceled its ACK")
	}
	active.release <- nil
}

func TestBackoffControlAuthoritativeStaleRejectedBeforeDecode(t *testing.T) {
	ctx := controlContext(t)
	transport := newAuthoritativeBackoffTransport()
	transport.generation.Store(2)
	session := newOSDSession(transport, backoffTestLimits, time.Hour)
	t.Cleanup(session.Stop)
	for _, stamp := range []uint64{0, 1} {
		message := msgr.Message{Header: msgr.MessageHeader{Type: protocol.MessageOSDBackoff}, Front: []byte{255}, TransportGeneration: stamp}
		authoritativeControlInject(t, ctx, transport, message)
	}
	session.mu.Lock()
	err := session.err
	session.mu.Unlock()
	if err != nil {
		t.Fatalf("obsolete malformed backoff decoded: %v", err)
	}
	session.ackMu.Lock()
	count := session.ackCount
	session.ackMu.Unlock()
	if count != 0 {
		t.Fatal("obsolete malformed backoff reserved an ACK")
	}
}

func TestBackoffControlAuthoritativeAdvanceDuringRegistration(t *testing.T) {
	ctx := controlContext(t)
	transport := newAuthoritativeBackoffTransport()
	session := newOSDSession(transport, backoffTestLimits, time.Hour)
	t.Cleanup(session.Stop)
	session.mu.Lock()
	select {
	case transport.incoming <- authoritativeBlockMessage(t, 1, 1):
	case <-ctx.Done():
		session.mu.Unlock()
		t.Fatal("registration did not start")
	}
	transport.generation.Store(2)
	session.mu.Unlock()
	authoritativeControlInject(t, ctx, transport, msgr.Message{})
	session.mu.Lock()
	_, blocked := session.backoffs[1]
	session.mu.Unlock()
	if blocked {
		t.Fatal("old stamp registered after authoritative generation advanced")
	}
	select {
	case <-transport.calls:
		t.Fatal("old stamp reserved an ACK after registration barrier")
	default:
	}
}
