package msgr

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/otuschhoff/rados-go/internal/protocol"
)

func controlTestMessage(size int) Message {
	front := make([]byte, size)
	front[28] = 2
	return Message{
		Header:  MessageHeader{Type: protocol.MessageOSDBackoff, Version: 1, CompatVersion: 1},
		Lengths: MessageLengths{Front: uint32(size)},
		Front:   front,
	}
}

func TestSessionDispatchSelectionNoControls(t *testing.T) {
	for _, test := range []struct {
		name    string
		pending []*pendingRequest
		want    int
	}{
		{name: "empty", want: -1},
		{name: "first-unsent", pending: []*pendingRequest{{seq: 1}, {seq: 3}, {}}, want: 0},
		{name: "sent-prefix", pending: []*pendingRequest{{seq: 1, sent: true}, {seq: 3}, {}}, want: 1},
		{name: "unsequenced-admission-order", pending: []*pendingRequest{{seq: 1, sent: true}, {}, {}}, want: 1},
		{name: "all-sent", pending: []*pendingRequest{{seq: 1, sent: true}, {seq: 3, sent: true}}, want: -1},
	} {
		t.Run(test.name, func(t *testing.T) {
			owner := &sessionOwner{pending: test.pending}
			indexSessionFixture(owner)
			for _, pending := range owner.pending {
				pending.request = &submitCommand{}
				if pending.sent {
					owner.inFlight++
				}
			}
			var want *pendingRequest
			if test.want >= 0 {
				want = owner.pending[test.want]
			}
			if got := owner.nextPendingWrite(); got != want {
				t.Fatalf("selected %p, want %p", got, want)
			}
		})
	}
}

func TestSessionDispatchSelectionControls(t *testing.T) {
	application := &pendingRequest{request: &submitCommand{}, seq: 3}
	replayedControl := &pendingRequest{request: &submitCommand{control: true}, seq: 2}
	freshControl := &pendingRequest{request: &submitCommand{control: true}}
	freshApplication := &pendingRequest{request: &submitCommand{}}
	owner := &sessionOwner{
		pending:      []*pendingRequest{freshApplication, application, freshControl, replayedControl},
		controlCount: 2,
	}
	indexSessionFixture(owner)
	for _, want := range []*pendingRequest{replayedControl, application, freshControl, freshApplication} {
		if got := owner.nextPendingWrite(); got != want {
			t.Fatalf("selected %p, want %p", got, want)
		}
		markSessionPendingSent(owner, want)
	}
	if got := owner.nextPendingWrite(); got != nil {
		t.Fatalf("selected sent request %p", got)
	}
}

func TestSessionDispatchPiggybacksLatestAcknowledgment(t *testing.T) {
	for _, replay := range []bool{false, true} {
		t.Run(fmt.Sprintf("replay=%t", replay), func(t *testing.T) {
			owner := newUnitSessionOwner(t)
			owner.writeTasks = make(chan writeTask, 1)
			owner.lastInbound = 7
			owner.queueAcknowledgment(7)
			t.Cleanup(owner.clearPendingAcknowledgment)
			command := &submitCommand{
				ctx:     context.Background(),
				message: Message{Header: MessageHeader{AckSequence: 99}},
				result:  make(chan submitResult, 1),
			}
			owner.submit(command)
			pending := owner.byRequest[command]
			if replay {
				pending.seq = 3
				pending.message.Header.Sequence = 3
				pending.message.Header.AckSequence = 2
				owner.addReplay(pending)
			}
			owner.dispatch()
			select {
			case task := <-owner.writeTasks:
				message, err := DecodeMessage(task.frame, owner.config.Limits)
				if err != nil {
					t.Fatal(err)
				}
				if message.Header.AckSequence != 7 {
					t.Fatalf("acknowledgment = %d, want 7", message.Header.AckSequence)
				}
				if replay && message.Header.Sequence != 3 {
					t.Fatalf("replay sequence = %d, want 3", message.Header.Sequence)
				}
				if owner.pendingAck != 0 || owner.ackTimerC != nil || len(owner.controlQueue) != 0 {
					t.Fatal("piggyback left an acknowledgment pending")
				}
			default:
				t.Fatal("message was not dispatched")
			}
		})
	}
}

func TestSessionAcknowledgmentDeadlineAndControlPriority(t *testing.T) {
	owner := newUnitSessionOwner(t)
	t.Cleanup(owner.clearPendingAcknowledgment)
	owner.writeTasks = make(chan writeTask, 1)
	owner.queueAcknowledgment(4)
	deadline := owner.ackTimerC
	owner.queueAcknowledgment(7)
	if owner.ackTimerC != deadline || len(owner.controlQueue) != 0 {
		t.Fatal("acknowledgment restarted deadline or queued an immediate frame")
	}
	owner.queueControl(Keepalive2{})
	owner.dispatch()
	task := <-owner.writeTasks
	if task.frame.Tag != TagKeepalive2 || owner.pendingAck != 7 {
		t.Fatal("pending acknowledgment blocked or displaced control traffic")
	}
	select {
	case <-deadline:
	case <-time.After(time.Second):
		t.Fatal("idle acknowledgment timer did not fire")
	}
	owner.flushPendingAcknowledgment()
	owner.writeBusy = false
	owner.dispatch()
	task = <-owner.writeTasks
	payload, err := DecodeControl(task.frame, owner.config.Limits)
	if err != nil {
		t.Fatal(err)
	}
	if acknowledgment, ok := payload.(Ack); !ok || acknowledgment.Sequence != 7 {
		t.Fatalf("idle acknowledgment = %#v, want sequence 7", payload)
	}
	if owner.pendingAck != 0 || owner.ackTimerC != nil {
		t.Fatal("idle flush retained pending state")
	}
}

func TestSessionAcknowledgmentLifecycle(t *testing.T) {
	for _, test := range []struct {
		name string
		stop func(*sessionOwner)
	}{
		{name: "terminal", stop: func(owner *sessionOwner) { owner.failTerminal(ErrIntegrity) }},
		{name: "new-identity", stop: func(owner *sessionOwner) { owner.resetForNewIdentity() }},
	} {
		t.Run(test.name, func(t *testing.T) {
			owner := newUnitSessionOwner(t)
			owner.queueAcknowledgment(7)
			test.stop(owner)
			if owner.pendingAck != 0 || owner.ackTimerC != nil || len(owner.controlQueue) != 0 {
				t.Fatal("lifecycle transition retained stale acknowledgment")
			}
			owner.flushPendingAcknowledgment()
			if len(owner.controlQueue) != 0 {
				t.Fatal("stale timer flush produced an acknowledgment")
			}
		})
	}
}

func TestSessionDispatchSelectionSaturatedReplay(t *testing.T) {
	owner := newUnitSessionOwner(t)
	owner.config.MaxInFlightTransactions = 1
	owner.writeTasks = make(chan writeTask, 1)
	inFlight := &pendingRequest{request: &submitCommand{ctx: context.Background()}, seq: 1, sent: true}
	application := &pendingRequest{request: &submitCommand{ctx: context.Background()}, seq: 2}
	control := &pendingRequest{request: &submitCommand{ctx: context.Background(), control: true}, seq: 3}
	owner.pending = []*pendingRequest{inFlight, control, application}
	owner.inFlight = 1
	owner.controlCount = 1
	owner.replay = []*pendingRequest{inFlight, application, control}
	indexSessionFixture(owner)
	if got := owner.nextPendingWrite(); got != application {
		t.Fatalf("selected %p, want older application %p", got, application)
	}
	owner.dispatch()
	if owner.writeBusy || application.sent || control.sent || len(owner.writeTasks) != 0 {
		t.Fatal("control bypassed saturated older application replay")
	}
}

func BenchmarkSessionDispatchSelection(b *testing.B) {
	for _, controls := range []bool{false, true} {
		for _, depth := range []int{64, 1024, 4096} {
			b.Run(fmt.Sprintf("controls=%t/depth=%d", controls, depth), func(b *testing.B) {
				requests := make([]pendingRequest, depth)
				owner := &sessionOwner{pending: make([]*pendingRequest, depth)}
				for index := range requests {
					requests[index].request = &submitCommand{}
					requests[index].seq = uint64(index + 1)
					owner.pending[index] = &requests[index]
				}
				indexSessionFixture(owner)
				want := owner.pending[0]
				if controls {
					owner.controlCount = 1
					owner.pending[depth-1].request.control = true
					owner.pending[depth-1].seq = 1
					owner.pending[0].seq = uint64(depth)
					want = owner.pending[depth-1]
				}
				b.ReportAllocs()
				b.ResetTimer()
				for iteration := 0; iteration < b.N; iteration++ {
					if got := owner.nextPendingWrite(); got != want {
						b.Fatalf("selected %p, want %p", got, want)
					}
				}
			})
		}
	}
}

func TestSessionControlBypassesApplicationBudgets(t *testing.T) {
	transport := newFakeTransport()
	config := testSessionConfig(t)
	application := testMessage("application")
	config.MaxQueuedMessages = 1
	config.MaxInFlightTransactions = 1
	config.MaxRetainedBytes = retainedMessageBytes(application)
	session := newTestSession(t, transport, nil, config)
	defer session.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := submitAsync(session, ctx, application)
	decodeWrittenMessage(t, transport)
	if err := session.SendControl(context.Background(), controlTestMessage(29)); err != nil {
		t.Fatalf("control under application saturation: %v", err)
	}
	control := decodeWrittenMessage(t, transport)
	if control.Header.Type != protocol.MessageOSDBackoff || control.Header.Sequence != 2 {
		t.Fatalf("control header = %+v", control.Header)
	}
	snapshot := getSnapshot(t, session)
	if snapshot.InFlight != 1 || snapshot.RetainedBytes != config.MaxRetainedBytes {
		t.Fatalf("application accounting = %+v", snapshot)
	}
	cancel()
	waitOutcome(t, result)
}

func TestSessionInlineFaultDoesNotRestampIncoming(t *testing.T) {
	owner := newUnitSessionOwner(t)
	owner.session.controlGeneration.Store(1)
	owner.config.MaxQueuedMessages = 1
	owner.connectorRequests = make(chan connectRequest, 1)
	owner.controlQueue = []Frame{{}}
	message := testMessage("old transport")
	message.Header.Type = protocol.MessageOSDBackoff
	message.Header.Sequence = 1
	owner.handleMessage(message)
	if generation := owner.session.ControlGeneration(); generation != 2 {
		t.Fatalf("fault did not advance generation: %d", generation)
	}
	select {
	case incoming := <-owner.session.incoming:
		t.Fatalf("old transport message delivered after invalidation with generation %d", incoming.TransportGeneration)
	default:
	}
}

func TestSessionInlineFaultPreservesMatchedReply(t *testing.T) {
	owner := newUnitSessionOwner(t)
	owner.session.controlGeneration.Store(1)
	owner.config.MaxQueuedMessages = 1
	owner.config.ReconnectPolicy = ReplayPending
	owner.serverCookie = 22
	owner.connectorRequests = make(chan connectRequest, 1)
	owner.controlQueue = []Frame{{}}
	request := &submitCommand{ctx: context.Background(), message: testMessage("mutation"), result: make(chan submitResult, 1)}
	owner.submit(request)
	pending := owner.byRequest[request]
	pending.seq = owner.takeSequence()
	markSessionPendingSent(owner, pending)
	pending.mayHaveExecuted = true
	owner.addReplay(pending)
	reply := testMessage("durable reply")
	reply.Header.Sequence = 1
	reply.Header.AckSequence = pending.seq
	reply.Header.TransactionID = pending.message.Header.TransactionID
	owner.handleMessage(reply)
	select {
	case result := <-request.result:
		if result.err != nil || string(result.message.Front) != "durable reply" {
			t.Fatalf("received reply lost to ACK overflow: %+v", result)
		}
	default:
		t.Fatal("matched reply discarded after inline ACK fault")
	}
	if len(owner.pending) != 0 || len(owner.replay) != 0 || owner.retainedBytes != 0 {
		t.Fatal("completed request retained after inline ACK fault")
	}
	if generation := owner.session.ControlGeneration(); generation != 2 {
		t.Fatalf("ACK overflow did not invalidate transport: %d", generation)
	}
}

type controlGateTransport struct {
	*fakeTransport
	started chan Frame
	release chan error
}

func newControlGateTransport() *controlGateTransport {
	return &controlGateTransport{
		fakeTransport: newFakeTransport(),
		started:       make(chan Frame, 32),
		release:       make(chan error, 32),
	}
}

func (transport *controlGateTransport) WriteFrame(frame Frame) error {
	if frame.Tag != TagMessage {
		return transport.fakeTransport.WriteFrame(frame)
	}
	select {
	case transport.started <- cloneFrame(frame):
	case <-transport.closed:
		return ErrSessionClosed
	}
	select {
	case err := <-transport.release:
		if err != nil {
			return err
		}
		return transport.fakeTransport.WriteFrame(frame)
	case <-transport.closed:
		return ErrSessionClosed
	}
}

func controlAsync(session *Session, ctx context.Context, message Message) <-chan submitOutcome {
	result := make(chan submitOutcome, 1)
	go func() { result <- submitOutcome{err: session.SendControl(ctx, message)} }()
	return result
}

func waitControlStarted(t *testing.T, transport *controlGateTransport) Message {
	t.Helper()
	select {
	case frame := <-transport.started:
		message, err := DecodeMessage(frame, sessionTestLimits)
		if err != nil {
			t.Fatal(err)
		}
		return message
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for gated write")
		return Message{}
	}
}

func TestSessionControlReserves(t *testing.T) {
	for _, test := range []struct {
		name  string
		count int
		size  int
	}{
		{name: "count", count: MaxControlMessages, size: 29},
		{name: "bytes", count: 2, size: int(MaxControlRetainedBytes/2) - MessageHeaderSize},
	} {
		t.Run(test.name, func(t *testing.T) {
			transport := newControlGateTransport()
			config := testSessionConfig(t)
			config.Limits.MaxSegmentBytes = 2 << 20
			config.Limits.MaxFrameBytes = 3 << 20
			config.MaxQueuedMessages = 2
			application := testMessage("application")
			config.MaxRetainedBytes = 2 * retainedMessageBytes(application)
			session := newTestSession(t, transport, nil, config)
			defer session.Stop()
			applicationResult := submitAsync(session, context.Background(), application)
			waitControlStarted(t, transport)
			results := make([]<-chan submitOutcome, test.count)
			cancels := make([]context.CancelFunc, test.count)
			for index := range results {
				ctx, cancel := context.WithCancel(context.Background())
				cancels[index] = cancel
				defer cancel()
				results[index] = controlAsync(session, ctx, controlTestMessage(test.size))
				waitSnapshot(t, session, func(snapshot SessionSnapshot) bool { return snapshot.ControlQueued == index+1 })
			}
			snapshot := getSnapshot(t, session)
			if snapshot.ControlRetainedBytes != uint64(test.count*(MessageHeaderSize+test.size)) {
				t.Fatalf("reserve bytes = %+v", snapshot)
			}
			if err := session.SendControl(context.Background(), controlTestMessage(29)); !errors.Is(err, ErrQueueSaturated) {
				t.Fatalf("reserve exhaustion = %v", err)
			}
			queuedApplication := submitAsync(session, context.Background(), application)
			waitSnapshot(t, session, func(snapshot SessionSnapshot) bool { return snapshot.Queued == 1 })
			for index, cancel := range cancels {
				cancel()
				if outcome := waitOutcome(t, results[index]); !errors.Is(outcome.err, context.Canceled) || errors.Is(outcome.err, ErrOutcomeUnknown) {
					t.Fatalf("prewrite cancellation = %v", outcome.err)
				}
			}
			snapshot = getSnapshot(t, session)
			if snapshot.ControlQueued != 0 || snapshot.ControlRetainedBytes != 0 || snapshot.RetainedBytes != config.MaxRetainedBytes {
				t.Fatalf("released reserves = %+v", snapshot)
			}
			replacementCtx, replacementCancel := context.WithCancel(context.Background())
			defer replacementCancel()
			replacement := controlAsync(session, replacementCtx, controlTestMessage(test.size))
			waitSnapshot(t, session, func(snapshot SessionSnapshot) bool { return snapshot.ControlQueued == 1 })
			replacementCancel()
			waitOutcome(t, replacement)
			session.Stop()
			waitOutcome(t, applicationResult)
			waitOutcome(t, queuedApplication)
		})
	}
}

func TestSessionControlStartedCancellationAndStop(t *testing.T) {
	transport := newControlGateTransport()
	session := newTestSession(t, transport, nil, testSessionConfig(t))
	defer session.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := controlAsync(session, ctx, controlTestMessage(29))
	waitControlStarted(t, transport)
	snapshot := getSnapshot(t, session)
	if snapshot.ControlQueued != 1 || snapshot.ControlRetainedBytes != MessageHeaderSize+29 || snapshot.InFlight != 0 || snapshot.RetainedBytes != 0 {
		t.Fatalf("started control accounting = %+v", snapshot)
	}
	select {
	case outcome := <-started:
		t.Fatalf("returned before write completion: %+v", outcome)
	default:
	}
	cancel()
	if outcome := waitOutcome(t, started); !errors.Is(outcome.err, context.Canceled) || !errors.Is(outcome.err, ErrOutcomeUnknown) {
		t.Fatalf("started cancellation = %v", outcome.err)
	}
	snapshot = getSnapshot(t, session)
	if snapshot.ControlQueued != 0 || snapshot.ControlRetainedBytes != 0 || snapshot.Replay != 0 {
		t.Fatalf("canceled control accounting = %+v", snapshot)
	}
	queued := controlAsync(session, context.Background(), controlTestMessage(29))
	waitSnapshot(t, session, func(snapshot SessionSnapshot) bool { return snapshot.ControlQueued == 1 })
	stopped := make(chan struct{})
	go func() { session.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Stop did not close stalled writer")
	}
	if outcome := waitOutcome(t, queued); !errors.Is(outcome.err, ErrSessionClosed) || errors.Is(outcome.err, ErrOutcomeUnknown) {
		t.Fatalf("queued stop = %v", outcome.err)
	}
	select {
	case <-transport.closed:
	default:
		t.Fatal("transport remains open")
	}
}

func TestSessionControlOwnershipAndPriority(t *testing.T) {
	transport := newControlGateTransport()
	config := testSessionConfig(t)
	config.MaxInFlightTransactions = 1
	session := newTestSession(t, transport, nil, config)
	defer session.Stop()
	first := submitAsync(session, context.Background(), testMessage("first"))
	waitControlStarted(t, transport)
	second := submitAsync(session, context.Background(), testMessage("second"))
	waitSnapshot(t, session, func(snapshot SessionSnapshot) bool { return snapshot.Queued == 1 })
	message := controlTestMessage(30)
	message.Front[29] = 7
	control := controlAsync(session, context.Background(), message)
	waitSnapshot(t, session, func(snapshot SessionSnapshot) bool { return snapshot.ControlQueued == 1 })
	message.Front[28] = 0
	message.Front[29] = 9
	transport.release <- nil
	written := waitControlStarted(t, transport)
	if written.Header.Type != protocol.MessageOSDBackoff || written.Header.Sequence != 2 || written.Front[28] != 2 || written.Front[29] != 7 {
		t.Fatalf("owned control did not overtake unsequenced application: %+v", written)
	}
	transport.release <- nil
	if outcome := waitOutcome(t, control); outcome.err != nil {
		t.Fatal(outcome.err)
	}
	snapshot := getSnapshot(t, session)
	if snapshot.ControlQueued != 0 || snapshot.ControlRetainedBytes != 0 || snapshot.InFlight != 1 || snapshot.Queued != 1 || snapshot.Replay != 1 {
		t.Fatalf("completed control accounting = %+v", snapshot)
	}
	session.Stop()
	waitOutcome(t, first)
	waitOutcome(t, second)
}

func TestSessionControlReconnectSequenceOrder(t *testing.T) {
	first := newControlGateTransport()
	second := newControlGateTransport()
	config := testSessionConfig(t)
	config.ClientCookie = 11
	config.ServerCookie = 22
	session := newTestSession(t, first, oneTransportConnector(second), config)
	defer session.Stop()
	application := submitAsync(session, context.Background(), testMessage("first"))
	originalApplication := waitControlStarted(t, first)
	queuedApplication := submitAsync(session, context.Background(), testMessage("queued"))
	waitSnapshot(t, session, func(snapshot SessionSnapshot) bool { return snapshot.Queued == 1 })
	control := controlAsync(session, context.Background(), controlTestMessage(29))
	waitSnapshot(t, session, func(snapshot SessionSnapshot) bool { return snapshot.ControlQueued == 1 })
	first.release <- nil
	originalControl := waitControlStarted(t, first)
	first.fail(errors.New("disconnect during control write"))
	if _, ok := decodeWrittenControl(t, second.fakeTransport).(SessionReconnect); !ok {
		t.Fatal("expected reconnect handshake")
	}
	freshControl := controlAsync(session, context.Background(), controlTestMessage(30))
	waitSnapshot(t, session, func(snapshot SessionSnapshot) bool { return snapshot.ControlQueued == 2 })
	second.inject(controlFrame(t, SessionReconnectOK{}))
	for index, original := range []Message{originalApplication, originalControl} {
		written := waitControlStarted(t, second)
		if written.Header.Sequence != uint64(index+1) || written.Header.TransactionID != original.Header.TransactionID || written.Header.Type != original.Header.Type {
			t.Fatalf("replay %d = %+v, original = %+v", index, written.Header, original.Header)
		}
		second.release <- nil
	}
	if outcome := waitOutcome(t, control); outcome.err != nil {
		t.Fatalf("replayed control = %v", outcome.err)
	}
	written := waitControlStarted(t, second)
	if written.Header.Sequence != 3 || written.Header.Type != protocol.MessageOSDBackoff {
		t.Fatalf("fresh control = %+v", written.Header)
	}
	second.release <- nil
	if outcome := waitOutcome(t, freshControl); outcome.err != nil {
		t.Fatal(outcome.err)
	}
	written = waitControlStarted(t, second)
	if written.Header.Sequence != 4 || string(written.Front) != "queued" {
		t.Fatalf("fresh application = %+v", written)
	}
	second.release <- nil
	session.Stop()
	waitOutcome(t, application)
	waitOutcome(t, queuedApplication)
}

func TestSessionControlRequiresWriteCompletionDespiteMatchingTID(t *testing.T) {
	transport := newControlGateTransport()
	session := newTestSession(t, transport, nil, testSessionConfig(t))
	defer session.Stop()
	result := controlAsync(session, context.Background(), controlTestMessage(29))
	written := waitControlStarted(t, transport)
	response := testMessage("unsolicited")
	response.Header.Sequence = 1
	response.Header.TransactionID = written.Header.TransactionID
	transport.inject(messageFrame(t, response))
	select {
	case incoming := <-session.Incoming():
		if incoming.Header.TransactionID != written.Header.TransactionID {
			t.Fatal("wrong incoming transaction")
		}
	case outcome := <-result:
		t.Fatalf("matching TID completed control before write: %+v", outcome)
	case <-time.After(time.Second):
		t.Fatal("matching TID was not handled")
	}
	snapshot := getSnapshot(t, session)
	if snapshot.ControlQueued != 1 || snapshot.ControlRetainedBytes != MessageHeaderSize+29 {
		t.Fatalf("control released before write completion: %+v", snapshot)
	}
	transport.release <- nil
	if outcome := waitOutcome(t, result); outcome.err != nil {
		t.Fatal(outcome.err)
	}
}

func TestSessionControlCompletedWriteIsNotReplayed(t *testing.T) {
	first := newFakeTransport()
	second := newFakeTransport()
	config := testSessionConfig(t)
	config.ClientCookie, config.ServerCookie = 11, 22
	session := newTestSession(t, first, oneTransportConnector(second), config)
	defer session.Stop()
	if err := session.SendControl(context.Background(), controlTestMessage(29)); err != nil {
		t.Fatal(err)
	}
	decodeWrittenMessage(t, first)
	first.fail(ErrSessionDisconnected)
	if _, ok := decodeWrittenControl(t, second).(SessionReconnect); !ok {
		t.Fatal("expected reconnect handshake")
	}
	result := controlAsync(session, context.Background(), controlTestMessage(30))
	waitSnapshot(t, session, func(snapshot SessionSnapshot) bool { return snapshot.ControlQueued == 1 })
	second.inject(controlFrame(t, SessionReconnectOK{}))
	written := decodeWrittenMessage(t, second)
	if written.Header.Sequence != 2 || len(written.Front) != 30 {
		t.Fatalf("completed control replayed: %+v", written)
	}
	if outcome := waitOutcome(t, result); outcome.err != nil {
		t.Fatal(outcome.err)
	}
}

func controlUnitSubmit(owner *sessionOwner, message Message) *submitCommand {
	command := &submitCommand{ctx: context.Background(), message: message, oneWay: true, control: true, result: make(chan submitResult, 1)}
	owner.submit(command)
	return command
}

func TestSessionControlRejectsUnrestrictedBypass(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*Message)
	}{
		{"type", func(message *Message) { message.Header.Type = 0 }},
		{"version", func(message *Message) { message.Header.Version = 2 }},
		{"compat", func(message *Message) { message.Header.CompatVersion = 2 }},
		{"short", func(message *Message) { message.Front = message.Front[:28]; message.Lengths.Front = 28 }},
		{"op", func(message *Message) { message.Front[28] = 1 }},
		{"middle", func(message *Message) { message.Middle = []byte{1}; message.Lengths.Middle = 1 }},
		{"data", func(message *Message) { message.Data = []byte{1}; message.Lengths.Data = 1 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			owner := newUnitSessionOwner(t)
			owner.retainedBytes = owner.config.MaxRetainedBytes
			message := controlTestMessage(29)
			test.mutate(&message)
			command := controlUnitSubmit(owner, message)
			if result := <-command.result; !errors.Is(result.err, ErrUnsupportedPayload) {
				t.Fatalf("invalid control = %v", result.err)
			}
			if len(owner.pending) != 0 || owner.controlBytes != 0 || owner.controlCount != 0 {
				t.Fatal("invalid control retained")
			}
		})
	}
	owner := newUnitSessionOwner(t)
	owner.config.Limits.MaxSegmentBytes = 2 << 20
	owner.config.Limits.MaxFrameBytes = 3 << 20
	owner.retainedBytes = owner.config.MaxRetainedBytes
	command := controlUnitSubmit(owner, controlTestMessage(int(MaxControlRetainedBytes)-MessageHeaderSize+1))
	if result := <-command.result; !errors.Is(result.err, ErrQueueSaturated) {
		t.Fatalf("oversized control = %v", result.err)
	}
	if command.message.Front != nil || len(owner.pending) != 0 || owner.controlBytes != 0 {
		t.Fatal("oversized control retained")
	}
	message := controlTestMessage(29)
	message.Lengths.Front++
	if result := <-controlUnitSubmit(owner, message).result; !errors.Is(result.err, ErrMalformed) {
		t.Fatalf("mismatched lengths = %v", result.err)
	}
}

func assertControlOwnerEmpty(t *testing.T, owner *sessionOwner) {
	t.Helper()
	if len(owner.pending) != 0 || len(owner.replay) != 0 || len(owner.byRequest) != 0 || len(owner.byTID) != 0 ||
		owner.controlCount != 0 || owner.controlBytes != 0 || owner.retainedBytes != 0 {
		t.Fatalf("owner retained terminal references: %+v", owner.snapshot())
	}
}

func TestSessionControlTerminalAndResetRelease(t *testing.T) {
	for _, test := range []struct {
		name string
		fail func(*sessionOwner)
	}{
		{"terminal", func(owner *sessionOwner) { owner.failTerminal(ErrReconnectExhausted) }},
		{"full-reset", func(owner *sessionOwner) { owner.handleReset(true) }},
		{"identity-reset", func(owner *sessionOwner) { owner.resetForNewIdentity() }},
		{"stop", func(owner *sessionOwner) {
			owner.connectorCancel = func() {}
			owner.stop(make(chan struct{}))
		}},
		{"fail-pending", func(owner *sessionOwner) {
			owner.config.ReconnectPolicy = FailPending
			owner.connectorRequests = make(chan connectRequest, 1)
			owner.handleFault(ErrSessionDisconnected)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			owner := newUnitSessionOwner(t)
			owner.session.resets = make(chan struct{}, 1)
			started := controlUnitSubmit(owner, controlTestMessage(29))
			pending := owner.byRequest[started]
			pending.seq, pending.sent, pending.mayHaveExecuted = 1, true, true
			owner.addReplay(pending)
			queued := controlUnitSubmit(owner, controlTestMessage(29))
			test.fail(owner)
			assertControlOwnerEmpty(t, owner)
			if result := <-started.result; !errors.Is(result.err, ErrOutcomeUnknown) {
				t.Fatalf("started terminal result = %v", result.err)
			}
			if result := <-queued.result; result.err == nil || errors.Is(result.err, ErrOutcomeUnknown) {
				t.Fatalf("queued terminal result = %v", result.err)
			}
		})
	}
}

func TestSessionControlSequenceOverflowTerminal(t *testing.T) {
	owner := newUnitSessionOwner(t)
	owner.nextOutbound = math.MaxUint64
	owner.writeTasks = make(chan writeTask, 1)
	started := controlUnitSubmit(owner, controlTestMessage(29))
	owner.dispatch()
	task := <-owner.writeTasks
	if task.seq != math.MaxUint64 || !owner.sequenceExhausted {
		t.Fatalf("last sequence = %d", task.seq)
	}
	owner.writeBusy = false
	queued := controlUnitSubmit(owner, controlTestMessage(29))
	owner.dispatch()
	if !errors.Is(owner.terminalErr, ErrTransitionLimit) {
		t.Fatalf("terminal error = %v", owner.terminalErr)
	}
	assertControlOwnerEmpty(t, owner)
	if result := <-started.result; !errors.Is(result.err, ErrOutcomeUnknown) || !errors.Is(result.err, ErrTransitionLimit) {
		t.Fatalf("started overflow = %v", result.err)
	}
	if result := <-queued.result; !errors.Is(result.err, ErrTransitionLimit) || errors.Is(result.err, ErrOutcomeUnknown) {
		t.Fatalf("queued overflow = %v", result.err)
	}
}

func TestSessionControlReplayCancellationUnknown(t *testing.T) {
	owner := newUnitSessionOwner(t)
	owner.writeTasks = make(chan writeTask, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	command := &submitCommand{ctx: ctx, message: controlTestMessage(29), oneWay: true, control: true, result: make(chan submitResult, 1)}
	owner.submit(command)
	pending := owner.byRequest[command]
	pending.seq, pending.mayHaveExecuted = 1, true
	owner.addReplay(pending)
	cancel()
	owner.dispatch()
	if result := <-command.result; !errors.Is(result.err, context.Canceled) || !errors.Is(result.err, ErrOutcomeUnknown) {
		t.Fatalf("canceled replay = %v", result.err)
	}
	assertControlOwnerEmpty(t, owner)
}

func TestSessionScopedControlBoundaries(t *testing.T) {
	for _, boundary := range []string{"fault", "renewal", "partial-reset", "full-reset", "terminal", "stop"} {
		t.Run(boundary, func(t *testing.T) {
			owner := newUnitSessionOwner(t)
			owner.session.controlGeneration.Store(1)
			owner.session.resets = make(chan struct{}, 1)
			owner.connectorRequests = make(chan connectRequest, 1)
			owner.writeTasks = make(chan writeTask, 1)
			started := &submitCommand{ctx: context.Background(), message: controlTestMessage(29), oneWay: true, control: true, controlGeneration: 1, result: make(chan submitResult, 1)}
			owner.submit(started)
			owner.dispatch()
			<-owner.writeTasks
			queued := &submitCommand{ctx: context.Background(), message: controlTestMessage(29), oneWay: true, control: true, controlGeneration: 1, result: make(chan submitResult, 1)}
			owner.submit(queued)
			switch boundary {
			case "fault":
				owner.handleFault(ErrSessionDisconnected)
			case "renewal":
				owner.handleFault(ErrSessionRenewal)
			case "partial-reset":
				owner.handleReset(false)
			case "full-reset":
				owner.handleReset(true)
			case "terminal":
				owner.failTerminal(ErrReconnectExhausted)
			case "stop":
				owner.connectorCancel = func() {}
				owner.stop(make(chan struct{}))
			}
			if owner.session.ControlGeneration() != 2 {
				t.Fatalf("boundary generation = %d", owner.session.ControlGeneration())
			}
			for _, command := range []*submitCommand{started, queued} {
				if result := <-command.result; !errors.Is(result.err, ErrControlInvalidated) || errors.Is(result.err, ErrOutcomeUnknown) {
					t.Fatalf("scoped result = %v", result.err)
				}
			}
			assertControlOwnerEmpty(t, owner)
			obsolete := &submitCommand{ctx: context.Background(), message: Message{}, control: true, controlGeneration: 1, result: make(chan submitResult, 1)}
			owner.submit(obsolete)
			if result := <-obsolete.result; !errors.Is(result.err, ErrControlInvalidated) {
				t.Fatalf("obsolete token validated payload or terminal state first: %v", result.err)
			}
		})
	}
}

type controlOwnedTransport struct{ *fakeTransport }

func (*controlOwnedTransport) OwnsReadFrames() bool { return true }

func TestSessionControlGenerationMetadata(t *testing.T) {
	for _, owned := range []bool{false, true} {
		t.Run(fmt.Sprint(owned), func(t *testing.T) {
			transport := newFakeTransport()
			var connection Transport = transport
			if owned {
				connection = &controlOwnedTransport{transport}
			}
			session := newTestSession(t, connection, nil, testSessionConfig(t))
			defer session.Stop()
			if session.ControlGeneration() != 1 {
				t.Fatal("initial control generation is not one")
			}
			message := testMessage("unsolicited")
			message.Header.Sequence = 1
			plain := messageFrame(t, message)
			message.TransportGeneration = 123
			stamped := messageFrame(t, message)
			if !reflect.DeepEqual(plain, stamped) || cloneMessage(message).TransportGeneration != 123 {
				t.Fatal("metadata changed wire encoding or clone lost metadata")
			}
			for _, decode := range []func(Frame, Limits) (Message, error){DecodeMessage, decodeOwnedMessage} {
				decoded, err := decode(stamped, sessionTestLimits)
				if err != nil || decoded.TransportGeneration != 0 {
					t.Fatalf("direct wire decode metadata = %d, %v", decoded.TransportGeneration, err)
				}
			}
			transport.inject(stamped)
			select {
			case incoming := <-session.Incoming():
				if incoming.TransportGeneration != 1 {
					t.Fatalf("owner inbound stamp = %d", incoming.TransportGeneration)
				}
			case <-time.After(time.Second):
				t.Fatal("incoming message not delivered")
			}
			if err := session.SendControlGeneration(context.Background(), controlTestMessage(29), 0); !errors.Is(err, ErrControlInvalidated) {
				t.Fatalf("zero token = %v", err)
			}
		})
	}
}

func TestSessionScopedControlReconnectSequenceGap(t *testing.T) {
	first := newControlGateTransport()
	second := newFakeTransport()
	config := testSessionConfig(t)
	config.ClientCookie, config.ServerCookie = 11, 22
	session := newTestSession(t, first, oneTransportConnector(second), config)
	defer session.Stop()
	application := submitAsync(session, context.Background(), testMessage("mutation"))
	original := waitControlStarted(t, first)
	first.release <- nil
	control := make(chan submitOutcome, 1)
	go func() {
		control <- submitOutcome{err: session.SendControlGeneration(context.Background(), controlTestMessage(29), 1)}
	}()
	if written := waitControlStarted(t, first); written.Header.Sequence != 2 {
		t.Fatal("scoped control sequence not assigned")
	}
	first.fail(ErrSessionDisconnected)
	decodeWrittenControl(t, second)
	if outcome := waitOutcome(t, control); !errors.Is(outcome.err, ErrControlInvalidated) || errors.Is(outcome.err, ErrOutcomeUnknown) {
		t.Fatalf("started scoped control fault = %v", outcome.err)
	}
	if session.ControlGeneration() != 2 {
		t.Fatal("fault generation did not advance")
	}
	if err := session.SendControlGeneration(context.Background(), Message{}, 1); !errors.Is(err, ErrControlInvalidated) {
		t.Fatalf("stale admission = %v", err)
	}
	second.inject(controlFrame(t, SessionReconnectOK{}))
	replayed := decodeWrittenMessage(t, second)
	if replayed.Header.Sequence != original.Header.Sequence || replayed.Header.TransactionID != original.Header.TransactionID {
		t.Fatal("application replay identity changed")
	}
	if err := session.SendControlGeneration(context.Background(), controlTestMessage(29), 2); err != nil {
		t.Fatal(err)
	}
	fresh := decodeWrittenMessage(t, second)
	if fresh.Header.Sequence != 3 {
		t.Fatalf("dropped assigned control did not preserve sequence gap: %d", fresh.Header.Sequence)
	}
	reply := testMessage("reply")
	reply.Header.Sequence, reply.Header.AckSequence, reply.Header.TransactionID = 1, 3, original.Header.TransactionID
	second.inject(messageFrame(t, reply))
	if outcome := waitOutcome(t, application); outcome.err != nil || string(outcome.message.Front) != "reply" {
		t.Fatalf("peer acknowledgment across sequence gap = %+v", outcome)
	}
	if snapshot := getSnapshot(t, session); snapshot.Replay != 0 || snapshot.ControlQueued != 0 || snapshot.RetainedBytes != 0 {
		t.Fatalf("reconnect retained requests: %+v", snapshot)
	}
}
