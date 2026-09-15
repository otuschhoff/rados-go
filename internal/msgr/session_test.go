package msgr

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/otuschhoff/go-librados/internal/protocol"
)

type fakeRead struct {
	frame Frame
	err   error
}

var sessionTestLimits = Limits{MaxSegmentBytes: 4096, MaxFrameBytes: 8192, MaxAddresses: 4, MaxAuthBytes: 64}

type fakeTransport struct {
	reads  chan fakeRead
	writes chan Frame
	closed chan struct{}
	once   sync.Once
}

func newFakeTransport() *fakeTransport {
	return &fakeTransport{
		reads:  make(chan fakeRead, 32),
		writes: make(chan Frame, 32),
		closed: make(chan struct{}),
	}
}

func (transport *fakeTransport) ReadFrame() (Frame, error) {
	select {
	case read := <-transport.reads:
		return read.frame, read.err
	case <-transport.closed:
		return Frame{}, ErrSessionClosed
	}
}

func (transport *fakeTransport) WriteFrame(frame Frame) error {
	select {
	case transport.writes <- cloneFrame(frame):
		return nil
	case <-transport.closed:
		return ErrSessionClosed
	}
}

func (transport *fakeTransport) Close() error {
	transport.once.Do(func() { close(transport.closed) })
	return nil
}

func (transport *fakeTransport) inject(frame Frame) { transport.reads <- fakeRead{frame: frame} }
func (transport *fakeTransport) fail(err error)     { transport.reads <- fakeRead{err: err} }

type blockingWriteTransport struct {
	*fakeTransport
	writeStarted chan struct{}
	writeOnce    sync.Once
}

func newBlockingWriteTransport() *blockingWriteTransport {
	return &blockingWriteTransport{fakeTransport: newFakeTransport(), writeStarted: make(chan struct{})}
}

func (transport *blockingWriteTransport) WriteFrame(Frame) error {
	transport.writeOnce.Do(func() { close(transport.writeStarted) })
	<-transport.closed
	return ErrSessionClosed
}

type submitOutcome struct {
	message Message
	err     error
}

func TestSessionAckDoesNotCompleteAndIncomingSequenceRules(t *testing.T) {
	transport := newFakeTransport()
	session := newTestSession(t, transport, nil, testSessionConfig(t))
	defer session.Stop()

	result := submitAsync(session, context.Background(), testMessage("request"))
	sent := decodeWrittenMessage(t, transport)
	if sent.Header.Sequence != 1 || sent.Header.TransactionID == 0 {
		t.Fatalf("sent header = %+v", sent.Header)
	}

	transport.inject(controlFrame(t, Ack{Sequence: sent.Header.Sequence}))
	waitEvent(t, session, EventAcknowledged)
	select {
	case outcome := <-result:
		t.Fatalf("ack completed transaction: %+v", outcome)
	default:
	}
	snapshot := getSnapshot(t, session)
	if snapshot.Replay != 0 || snapshot.InFlight != 1 {
		t.Fatalf("snapshot after ack = %+v", snapshot)
	}

	response := testMessage("response")
	response.Header.Sequence = 2
	response.Header.TransactionID = sent.Header.TransactionID
	transport.inject(messageFrame(t, response))
	gap := waitEvent(t, session, EventSequenceGap)
	if gap.Expected != 1 || gap.Sequence != 2 {
		t.Fatalf("gap event = %+v", gap)
	}
	outcome := waitOutcome(t, result)
	if outcome.err != nil || string(outcome.message.Front) != "response" {
		t.Fatalf("submit outcome = %+v", outcome)
	}
	ack, ok := decodeWrittenControl(t, transport).(Ack)
	if !ok || ack.Sequence != 2 {
		t.Fatalf("response ack = %#v", ack)
	}

	transport.inject(messageFrame(t, response))
	duplicate := waitEvent(t, session, EventDuplicateDropped)
	if duplicate.Sequence != 2 {
		t.Fatalf("duplicate event = %+v", duplicate)
	}

	timestamp := Timestamp{Seconds: 123, Nanoseconds: 456}
	transport.inject(controlFrame(t, Keepalive2{Timestamp: timestamp}))
	echo, ok := decodeWrittenControl(t, transport).(Keepalive2Ack)
	if !ok || echo.Timestamp != timestamp {
		t.Fatalf("keepalive echo = %#v", echo)
	}
	transport.inject(controlFrame(t, Keepalive2Ack{Timestamp: timestamp}))
	keepalive := waitEvent(t, session, EventKeepaliveAck)
	if keepalive.Time != timestamp {
		t.Fatalf("keepalive event = %+v", keepalive)
	}
}

func TestSessionSendCompletesAfterWrite(t *testing.T) {
	transport := newFakeTransport()
	session := newTestSession(t, transport, nil, testSessionConfig(t))
	defer session.Stop()

	done := make(chan error, 1)
	go func() { done <- session.Send(context.Background(), testMessage("one-way")) }()
	sent := decodeWrittenMessage(t, transport)
	if string(sent.Front) != "one-way" || sent.Header.TransactionID == 0 {
		t.Fatalf("sent = %+v", sent)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Send did not complete after transport write")
	}
	snapshot := getSnapshot(t, session)
	if snapshot.InFlight != 0 || snapshot.Replay != 0 || snapshot.RetainedBytes != 0 {
		t.Fatalf("one-way send retained state: %+v", snapshot)
	}
}

func TestSessionQueueBoundsAndCancellation(t *testing.T) {
	config := testSessionConfig(t)
	config.MaxQueuedMessages = 2
	config.MaxInFlightTransactions = 1
	transport := newFakeTransport()
	session := newTestSession(t, transport, nil, config)
	defer session.Stop()

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	secondCtx, cancelSecond := context.WithCancel(context.Background())
	first := submitAsync(session, firstCtx, testMessage("one"))
	_ = decodeWrittenMessage(t, transport)
	second := submitAsync(session, secondCtx, testMessage("two"))
	waitSnapshot(t, session, func(snapshot SessionSnapshot) bool {
		return snapshot.InFlight == 1 && snapshot.Queued == 1
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := session.Submit(ctx, testMessage("three")); !errors.Is(err, ErrQueueSaturated) {
		t.Fatalf("count saturation error = %v", err)
	}

	cancelSecond()
	if err := waitOutcome(t, second).err; !errors.Is(err, context.Canceled) {
		t.Fatalf("queued cancellation error = %v", err)
	}
	cancelFirst()
	if err := waitOutcome(t, first).err; !errors.Is(err, context.Canceled) || !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("in-flight cancellation error = %v", err)
	}
	waitSnapshot(t, session, func(snapshot SessionSnapshot) bool {
		return snapshot.Queued == 0 && snapshot.InFlight == 0 && snapshot.Replay == 0 && snapshot.RetainedBytes == 0
	})

	byteConfig := testSessionConfig(t)
	byteConfig.MaxRetainedBytes = retainedMessageBytes(testMessage("1234"))
	byteTransport := newFakeTransport()
	byteSession := newTestSession(t, byteTransport, nil, byteConfig)
	defer byteSession.Stop()
	byteCtx, cancelByte := context.WithCancel(context.Background())
	retained := submitAsync(byteSession, byteCtx, testMessage("1234"))
	_ = decodeWrittenMessage(t, byteTransport)
	if _, err := byteSession.Submit(ctx, testMessage("x")); !errors.Is(err, ErrQueueSaturated) {
		t.Fatalf("byte saturation error = %v", err)
	}
	cancelByte()
	_ = waitOutcome(t, retained)
}

func TestSessionAdmissionOwnsOneBoundedClone(t *testing.T) {
	owner := newUnitSessionOwner(t)
	original := testMessage("payload")
	request := &submitCommand{
		ctx:      context.Background(),
		message:  original,
		admitted: make(chan struct{}),
		result:   make(chan submitResult, 1),
	}
	owner.submit(request)
	select {
	case <-request.admitted:
	default:
		t.Fatal("owner did not complete admission")
	}
	if request.message.Front != nil || len(owner.pending) != 1 {
		t.Fatalf("request retained caller message or was not admitted: request=%+v pending=%d", request.message, len(owner.pending))
	}
	original.Front[0] = 'X'
	if got := string(owner.pending[0].message.Front); got != "payload" {
		t.Fatalf("owned message changed with caller buffer: %q", got)
	}

	rejected := testMessage("too large")
	owner.config.MaxRetainedBytes = 1
	rejectedRequest := &submitCommand{ctx: context.Background(), message: rejected, result: make(chan submitResult, 1)}
	owner.submit(rejectedRequest)
	if err := (<-rejectedRequest.result).err; !errors.Is(err, ErrQueueSaturated) {
		t.Fatalf("rejected admission error = %v", err)
	}
	if rejectedRequest.message.Front != nil {
		t.Fatal("rejected request retained caller message")
	}
}

func TestSessionReconnectReplaysOriginalIdentity(t *testing.T) {
	firstTransport := newFakeTransport()
	secondTransport := newFakeTransport()
	connects := make(chan Transport, 1)
	connects <- secondTransport
	connector := ConnectorFunc(func(ctx context.Context) (Transport, error) {
		select {
		case transport := <-connects:
			return transport, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	config := testSessionConfig(t)
	config.ClientCookie = 11
	config.ServerCookie = 22
	config.GlobalSequence = 33
	config.ConnectSequence = 4
	session := newTestSession(t, firstTransport, connector, config)
	defer session.Stop()

	result := submitAsync(session, context.Background(), testMessage("request"))
	original := decodeWrittenMessage(t, firstTransport)
	firstTransport.fail(errors.New("read failed"))
	reconnect, ok := decodeWrittenControl(t, secondTransport).(SessionReconnect)
	if !ok {
		t.Fatalf("reconnect frame type = %T", reconnect)
	}
	if reconnect.ClientCookie != 11 || reconnect.ServerCookie != 22 || reconnect.GlobalSequence != 34 || reconnect.ConnectSequence != 5 {
		t.Fatalf("reconnect = %+v", reconnect)
	}

	secondTransport.inject(controlFrame(t, SessionReconnectOK{MessageSequence: 0}))
	replayed := decodeWrittenMessage(t, secondTransport)
	if replayed.Header.Sequence != original.Header.Sequence || replayed.Header.TransactionID != original.Header.TransactionID {
		t.Fatalf("replayed header = %+v, original = %+v", replayed.Header, original.Header)
	}
	response := testMessage("done")
	response.Header.Sequence = 1
	response.Header.TransactionID = original.Header.TransactionID
	secondTransport.inject(messageFrame(t, response))
	if outcome := waitOutcome(t, result); outcome.err != nil {
		t.Fatal(outcome.err)
	}
}

func TestSessionNewAuthenticatedIdentityResetsMessengerState(t *testing.T) {
	first := newFakeTransport()
	second := newFakeTransport()
	transports := make(chan Transport, 2)
	transports <- &authenticatedFakeTransport{fakeTransport: first, globalID: 10}
	transports <- &authenticatedFakeTransport{fakeTransport: second, globalID: 11}
	connector := ConnectorFunc(func(ctx context.Context) (Transport, error) {
		select {
		case transport := <-transports:
			return transport, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	config := testSessionConfig(t)
	session := newTestSession(t, nil, connector, config)
	defer session.Stop()

	firstIdent := decodeWrittenControl(t, first).(ClientIdent)
	if firstIdent.GlobalID != 10 {
		t.Fatalf("first global ID = %d", firstIdent.GlobalID)
	}
	first.inject(controlFrame(t, ServerIdent{Addresses: protocol.EntityAddrVec{config.ClientIdent.TargetAddress}, Cookie: 22}))
	for event := waitEvent(t, session, EventStateChanged); event.State != StateReady; event = waitEvent(t, session, EventStateChanged) {
	}
	result := submitAsync(session, context.Background(), testMessage("request"))
	_ = decodeWrittenMessage(t, first)
	first.fail(errors.New("identity expired"))
	secondIdent, ok := decodeWrittenControl(t, second).(ClientIdent)
	if !ok || secondIdent.GlobalID != 11 || secondIdent.GlobalSequence != firstIdent.GlobalSequence+1 {
		t.Fatalf("replacement identity frame = %+v", secondIdent)
	}
	if outcome := waitOutcome(t, result); !errors.Is(outcome.err, ErrSessionDisconnected) {
		t.Fatalf("old identity request error = %v", outcome.err)
	}
}

func TestSessionLossyFaultDoesNotReplaySentRequest(t *testing.T) {
	for _, policy := range []ReconnectPolicy{FailPending, ReplayPending} {
		t.Run(fmt.Sprintf("policy-%d", policy), func(t *testing.T) {
			first := newFakeTransport()
			second := newFakeTransport()
			transports := make(chan Transport, 2)
			transports <- first
			transports <- second
			var connects atomic.Int32
			config := testSessionConfig(t)
			config.ReconnectPolicy = policy
			session := newTestSession(t, nil, ConnectorFunc(func(ctx context.Context) (Transport, error) {
				connects.Add(1)
				select {
				case transport := <-transports:
					return transport, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}), config)
			defer session.Stop()

			_ = decodeWrittenControl(t, first).(ClientIdent)
			first.inject(controlFrame(t, ServerIdent{
				Addresses: protocol.EntityAddrVec{config.ClientIdent.TargetAddress},
				Flags:     ConnectionFlagLossy,
			}))
			waitSnapshot(t, session, func(snapshot SessionSnapshot) bool { return snapshot.State == StateReady })
			result := submitAsync(session, context.Background(), testMessage("request"))
			_ = decodeWrittenMessage(t, first)
			first.fail(errors.New("reply lost"))
			if outcome := waitOutcome(t, result); !errors.Is(outcome.err, ErrOutcomeUnknown) {
				t.Fatalf("lossy request error = %v", outcome.err)
			}
			if _, ok := decodeWrittenControl(t, second).(ClientIdent); !ok {
				t.Fatal("lossy replacement did not start a fresh ClientIdent session")
			}
			if got := connects.Load(); got != 2 {
				t.Fatalf("lossy session connection attempts = %d, want 2", got)
			}
		})
	}
}

func TestSessionDefaultGlobalSequencesAreUniqueAndNonzero(t *testing.T) {
	const count = 32
	sequences := make(chan uint64, count)
	var waitGroup sync.WaitGroup
	for range count {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			transport := newFakeTransport()
			config := testSessionConfig(t)
			config.GlobalSequence = 0
			config.ClientIdent.GlobalSequence = 0
			config.GlobalSequenceSource = nil
			session := newTestSession(t, nil, oneTransportConnector(transport), config)
			defer session.Stop()
			sequences <- decodeWrittenControl(t, transport).(ClientIdent).GlobalSequence
		}()
	}
	waitGroup.Wait()
	close(sequences)
	seen := make(map[uint64]struct{}, count)
	for sequence := range sequences {
		if sequence == 0 {
			t.Fatal("default global sequence is zero")
		}
		if _, duplicate := seen[sequence]; duplicate {
			t.Fatalf("duplicate global sequence %d", sequence)
		}
		seen[sequence] = struct{}{}
	}
}

func TestSessionExplicitGlobalSequenceAdvancesDefaultAllocator(t *testing.T) {
	minimum := processGlobalSequences.value.Load() + 100
	explicitTransport := newFakeTransport()
	explicitConfig := testSessionConfig(t)
	explicitConfig.GlobalSequence = minimum
	explicitConfig.GlobalSequenceSource = nil
	explicit := newTestSession(t, nil, oneTransportConnector(explicitTransport), explicitConfig)
	defer explicit.Stop()
	explicitSequence := decodeWrittenControl(t, explicitTransport).(ClientIdent).GlobalSequence
	if explicitSequence < minimum {
		t.Fatalf("explicit sequence = %d, want at least %d", explicitSequence, minimum)
	}

	defaultTransport := newFakeTransport()
	defaultConfig := testSessionConfig(t)
	defaultConfig.GlobalSequenceSource = nil
	following := newTestSession(t, nil, oneTransportConnector(defaultTransport), defaultConfig)
	defer following.Stop()
	defaultSequence := decodeWrittenControl(t, defaultTransport).(ClientIdent).GlobalSequence
	if defaultSequence <= explicitSequence {
		t.Fatalf("default sequence = %d, want greater than explicit %d", defaultSequence, explicitSequence)
	}
}

func TestSessionRejectsNonAdvancingGlobalSequenceSource(t *testing.T) {
	for _, returned := range []uint64{0, 3, 4} {
		config := testSessionConfig(t)
		config.GlobalSequence = 5
		config.GlobalSequenceSource = globalSequenceSourceFunc(func(uint64) (uint64, error) { return returned, nil })
		if _, err := NewSession(nil, nil, config); !errors.Is(err, ErrMalformed) {
			t.Fatalf("source result %d error = %v, want ErrMalformed", returned, err)
		}
	}
	config := testSessionConfig(t)
	config.GlobalSequence = 5
	config.GlobalSequenceSource = globalSequenceSourceFunc(func(after uint64) (uint64, error) { return after + 1, nil })
	session := newTestSession(t, nil, nil, config)
	session.Stop()
}

type globalSequenceSourceFunc func(uint64) (uint64, error)

func (function globalSequenceSourceFunc) Next(after uint64) (uint64, error) { return function(after) }

func TestSessionRejectsConflictingGlobalSequences(t *testing.T) {
	config := testSessionConfig(t)
	config.GlobalSequence = 1
	config.ClientIdent.GlobalSequence = 2
	if _, err := NewSession(nil, nil, config); !errors.Is(err, ErrMalformed) {
		t.Fatalf("conflicting sequence error = %v", err)
	}
}

func TestSessionAckedRequestCompletesFromPeerReplayAfterReconnect(t *testing.T) {
	firstTransport := newFakeTransport()
	secondTransport := newFakeTransport()
	config := testSessionConfig(t)
	config.ClientCookie = 11
	config.ServerCookie = 22
	session := newTestSession(t, firstTransport, oneTransportConnector(secondTransport), config)
	defer session.Stop()

	result := submitAsync(session, context.Background(), testMessage("request"))
	sent := decodeWrittenMessage(t, firstTransport)
	firstTransport.inject(controlFrame(t, Ack{Sequence: sent.Header.Sequence}))
	waitEvent(t, session, EventAcknowledged)
	firstTransport.fail(errors.New("response lost with connection"))
	_ = decodeWrittenControl(t, secondTransport).(SessionReconnect)
	secondTransport.inject(controlFrame(t, SessionReconnectOK{MessageSequence: sent.Header.Sequence}))

	select {
	case frame := <-secondTransport.writes:
		if frame.Tag == TagMessage {
			t.Fatal("transport-acknowledged request was replayed")
		}
	default:
	}
	response := testMessage("replayed response")
	response.Header.Sequence = 1
	response.Header.TransactionID = sent.Header.TransactionID
	secondTransport.inject(messageFrame(t, response))
	if outcome := waitOutcome(t, result); outcome.err != nil || string(outcome.message.Front) != "replayed response" {
		t.Fatalf("submit outcome = %+v", outcome)
	}
}

func TestSessionMessageHeaderAcknowledgementPreventsReplay(t *testing.T) {
	firstTransport := newFakeTransport()
	secondTransport := newFakeTransport()
	config := testSessionConfig(t)
	config.ClientCookie = 11
	config.ServerCookie = 22
	session := newTestSession(t, firstTransport, oneTransportConnector(secondTransport), config)
	defer session.Stop()

	result := submitAsync(session, context.Background(), testMessage("request"))
	sent := decodeWrittenMessage(t, firstTransport)
	peerMessage := testMessage("unrelated")
	peerMessage.Header.Sequence = 1
	peerMessage.Header.TransactionID = sent.Header.TransactionID + 1
	peerMessage.Header.AckSequence = sent.Header.Sequence
	firstTransport.inject(messageFrame(t, peerMessage))
	_ = decodeWrittenControl(t, firstTransport).(Ack)

	firstTransport.fail(errors.New("response lost with connection"))
	_ = decodeWrittenControl(t, secondTransport).(SessionReconnect)
	secondTransport.inject(controlFrame(t, SessionReconnectOK{MessageSequence: sent.Header.Sequence}))
	select {
	case frame := <-secondTransport.writes:
		if frame.Tag == TagMessage {
			t.Fatal("message-header-acknowledged request was replayed")
		}
	default:
	}
	response := testMessage("response")
	response.Header.Sequence = 2
	response.Header.TransactionID = sent.Header.TransactionID
	secondTransport.inject(messageFrame(t, response))
	if outcome := waitOutcome(t, result); outcome.err != nil {
		t.Fatal(outcome.err)
	}
}

func TestSessionDeliversUnsolicitedMessages(t *testing.T) {
	transport := newFakeTransport()
	session := newTestSession(t, transport, nil, testSessionConfig(t))
	defer session.Stop()

	message := testMessage("map update")
	message.Header.Sequence = 1
	message.Header.TransactionID = 99
	frame := messageFrame(t, message)
	transport.inject(frame)

	select {
	case incoming := <-session.Incoming():
		frame.Segments[1].Data[0] = 'X'
		if string(incoming.Front) != "map update" || incoming.Header.TransactionID != 99 {
			t.Fatalf("incoming message = %+v", incoming)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for unsolicited message")
	}
	ack, ok := decodeWrittenControl(t, transport).(Ack)
	if !ok || ack.Sequence != 1 {
		t.Fatalf("unsolicited message ack = %#v", ack)
	}
}

func TestSessionKeepsMatchedRepliesOutOfIncoming(t *testing.T) {
	transport := newFakeTransport()
	session := newTestSession(t, transport, nil, testSessionConfig(t))
	defer session.Stop()

	result := submitAsync(session, context.Background(), testMessage("request"))
	sent := decodeWrittenMessage(t, transport)
	reply := testMessage("reply")
	reply.Header.Sequence = 1
	reply.Header.TransactionID = sent.Header.TransactionID
	transport.inject(messageFrame(t, reply))

	if outcome := waitOutcome(t, result); outcome.err != nil || string(outcome.message.Front) != "reply" {
		t.Fatalf("submit outcome = %+v", outcome)
	}
	select {
	case message := <-session.Incoming():
		t.Fatalf("matched reply delivered as unsolicited: %+v", message)
	default:
	}
}

func TestSessionIsolatesLateReplyAfterCancellation(t *testing.T) {
	transport := newFakeTransport()
	session := newTestSession(t, transport, nil, testSessionConfig(t))
	defer session.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	firstResult := submitAsync(session, ctx, testMessage("first request"))
	first := decodeWrittenMessage(t, transport)
	cancel()
	if err := waitOutcome(t, firstResult).err; !errors.Is(err, context.Canceled) || !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("canceled request error = %v", err)
	}

	secondResult := submitAsync(session, context.Background(), testMessage("second request"))
	second := decodeWrittenMessage(t, transport)
	late := testMessage("late first reply")
	late.Header.Sequence = 1
	late.Header.TransactionID = first.Header.TransactionID
	transport.inject(messageFrame(t, late))
	if incoming := <-session.Incoming(); string(incoming.Front) != "late first reply" {
		t.Fatalf("late reply = %+v", incoming)
	}
	select {
	case outcome := <-secondResult:
		t.Fatalf("late reply completed second request: %+v", outcome)
	default:
	}
	reply := testMessage("second reply")
	reply.Header.Sequence = 2
	reply.Header.TransactionID = second.Header.TransactionID
	transport.inject(messageFrame(t, reply))
	if outcome := waitOutcome(t, secondResult); outcome.err != nil || string(outcome.message.Front) != "second reply" {
		t.Fatalf("second outcome = %+v", outcome)
	}
}

func TestSessionFailsExplicitlyWhenIncomingQueueIsFull(t *testing.T) {
	transport := newFakeTransport()
	config := testSessionConfig(t)
	config.MaxQueuedMessages = 1
	session := newTestSession(t, transport, nil, config)
	defer session.Stop()

	first := testMessage("first")
	first.Header.Sequence = 1
	transport.inject(messageFrame(t, first))
	_ = decodeWrittenControl(t, transport).(Ack)

	second := testMessage("second")
	second.Header.Sequence = 2
	transport.inject(messageFrame(t, second))
	event := waitEvent(t, session, EventTransportFault)
	if !errors.Is(event.Err, ErrQueueSaturated) {
		t.Fatalf("overflow error = %v, want %v", event.Err, ErrQueueSaturated)
	}
	if incoming := <-session.Incoming(); string(incoming.Front) != "first" {
		t.Fatalf("retained incoming message = %+v", incoming)
	}
	if _, err := session.Submit(context.Background(), testMessage("late")); !errors.Is(err, ErrQueueSaturated) {
		t.Fatalf("submit after incoming overflow = %v", err)
	}
}

func TestSessionIncomingSurvivesReconnectAndClosesOnStop(t *testing.T) {
	firstTransport := newFakeTransport()
	secondTransport := newFakeTransport()
	config := testSessionConfig(t)
	config.ClientCookie = 11
	config.ServerCookie = 22
	session := newTestSession(t, firstTransport, oneTransportConnector(secondTransport), config)

	first := testMessage("before reconnect")
	first.Header.Sequence = 1
	firstTransport.inject(messageFrame(t, first))
	if incoming := <-session.Incoming(); string(incoming.Front) != "before reconnect" {
		t.Fatalf("first incoming message = %+v", incoming)
	}
	_ = decodeWrittenControl(t, firstTransport).(Ack)

	firstTransport.fail(errors.New("fault"))
	_ = decodeWrittenControl(t, secondTransport).(SessionReconnect)
	secondTransport.inject(controlFrame(t, SessionReconnectOK{MessageSequence: 0}))
	waitEvent(t, session, EventReconnectOK)
	second := testMessage("after reconnect")
	second.Header.Sequence = 2
	secondTransport.inject(messageFrame(t, second))
	if incoming := <-session.Incoming(); string(incoming.Front) != "after reconnect" {
		t.Fatalf("second incoming message = %+v", incoming)
	}

	session.Stop()
	select {
	case _, ok := <-session.Incoming():
		if ok {
			t.Fatal("incoming channel remained open after stop")
		}
	case <-time.After(time.Second):
		t.Fatal("incoming channel did not close after stop")
	}
}

func TestSessionUsesAuthenticatedTransportGlobalID(t *testing.T) {
	transport := newFakeTransport()
	authenticated := &authenticatedFakeTransport{fakeTransport: transport, globalID: 4100}
	config := testSessionConfig(t)
	session := newTestSession(t, nil, oneTransportConnector(authenticated), config)
	defer session.Stop()

	ident := decodeWrittenControl(t, transport).(ClientIdent)
	if ident.GlobalID != 4100 {
		t.Fatalf("client ident global id = %d, want 4100", ident.GlobalID)
	}
	transport.inject(controlFrame(t, ServerIdent{
		Addresses:         protocol.EntityAddrVec{config.ClientIdent.TargetAddress},
		GlobalID:          52,
		SupportedFeatures: 71,
		Cookie:            93,
	}))
	for event := waitEvent(t, session, EventStateChanged); event.State != StateReady; event = waitEvent(t, session, EventStateChanged) {
	}
	snapshot := getSnapshot(t, session)
	if snapshot.AuthenticatedGlobalID != 4100 || snapshot.ServerGlobalID != 52 || snapshot.ServerFeatures != 71 || snapshot.ServerCookie != 93 || !reflect.DeepEqual(snapshot.ServerAddresses, protocol.EntityAddrVec{config.ClientIdent.TargetAddress}) {
		t.Fatalf("negotiated snapshot = %+v", snapshot)
	}
	snapshot.ServerAddresses[0].SocketData[0] ^= 0xff
	if next := getSnapshot(t, session); reflect.DeepEqual(snapshot.ServerAddresses, next.ServerAddresses) {
		t.Fatal("mutating a snapshot changed the retained server addresses")
	}
}

func TestSessionRejectsInvalidServerIdent(t *testing.T) {
	tests := []struct {
		name  string
		ident ServerIdent
		want  error
	}{
		{name: "missing target address", ident: ServerIdent{}},
		{name: "unsupported required feature", ident: ServerIdent{RequiredFeatures: 1 << 63}, want: ErrUnsupportedFeature},
		{name: "missing client required feature", ident: ServerIdent{SupportedFeatures: 1}, want: ErrUnsupportedFeature},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			owner := newUnitSessionOwner(t)
			owner.state = StateConnecting
			owner.config.MaxReconnectAttempts = 0
			if test.name == "missing client required feature" {
				owner.config.ClientIdent.RequiredFeatures = 2
			}
			if test.ident.RequiredFeatures != 0 {
				test.ident.Addresses = protocol.EntityAddrVec{owner.config.ClientIdent.TargetAddress}
			}
			owner.handleControl(test.ident)
			if owner.state != StateDisconnected || owner.terminalErr == nil {
				t.Fatalf("invalid server ident left state=%d error=%v", owner.state, owner.terminalErr)
			}
			if test.want != nil && !errors.Is(owner.terminalErr, test.want) {
				t.Fatalf("error=%v want=%v", owner.terminalErr, test.want)
			}
		})
	}
}

func TestSessionRejectsAcknowledgmentBeyondOutboundSequence(t *testing.T) {
	for _, test := range []struct {
		name   string
		state  SessionState
		handle func(*sessionOwner)
	}{
		{name: "ack", state: StateReady, handle: func(owner *sessionOwner) { owner.handleControl(Ack{Sequence: 2}) }},
		{name: "message header", state: StateReady, handle: func(owner *sessionOwner) {
			owner.handleMessage(Message{Header: MessageHeader{Sequence: 1, AckSequence: 2}})
		}},
		{name: "reconnect ok", state: StateReconnecting, handle: func(owner *sessionOwner) { owner.handleControl(SessionReconnectOK{MessageSequence: 2}) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			owner := newUnitSessionOwner(t)
			owner.state = test.state
			owner.config.MaxReconnectAttempts = 0
			owner.nextOutbound = 2
			test.handle(owner)
			if owner.state != StateDisconnected || owner.terminalErr == nil {
				t.Fatalf("oversized acknowledgment left state=%d error=%v", owner.state, owner.terminalErr)
			}
		})
	}
}

func TestSessionReconnectTransitionsAndResets(t *testing.T) {
	t.Run("retry partial reset", func(t *testing.T) {
		firstTransport := newFakeTransport()
		secondTransport := newFakeTransport()
		connector := oneTransportConnector(secondTransport)
		config := testSessionConfig(t)
		config.ClientCookie = 101
		config.ServerCookie = 202
		config.GlobalSequence = 3
		session := newTestSession(t, firstTransport, connector, config)
		defer session.Stop()

		ctx, cancel := context.WithCancel(context.Background())
		result := submitAsync(session, ctx, testMessage("pending"))
		original := decodeWrittenMessage(t, firstTransport)
		firstTransport.fail(errors.New("fault"))
		_ = decodeWrittenControl(t, secondTransport).(SessionReconnect)

		secondTransport.inject(controlFrame(t, SessionRetry{ConnectSequence: 7}))
		retry := decodeWrittenControl(t, secondTransport).(SessionReconnect)
		if retry.ConnectSequence != 8 {
			t.Fatalf("retry connect sequence = %d", retry.ConnectSequence)
		}
		secondTransport.inject(controlFrame(t, SessionRetryGlobal{GlobalSequence: 10}))
		globalRetry := decodeWrittenControl(t, secondTransport).(SessionReconnect)
		if globalRetry.GlobalSequence != 11 {
			t.Fatalf("retry global sequence = %d", globalRetry.GlobalSequence)
		}

		secondTransport.inject(controlFrame(t, SessionReset{Full: false}))
		ident := decodeWrittenControl(t, secondTransport).(ClientIdent)
		if ident.Cookie != 101 || ident.GlobalSequence != 11 {
			t.Fatalf("partial reset ident = %+v", ident)
		}
		secondTransport.inject(controlFrame(t, ServerIdent{Addresses: protocol.EntityAddrVec{config.ClientIdent.TargetAddress}, Cookie: 303}))
		replayed := decodeWrittenMessage(t, secondTransport)
		if replayed.Header.Sequence != original.Header.Sequence || replayed.Header.TransactionID != original.Header.TransactionID {
			t.Fatalf("partial reset replay = %+v, original = %+v", replayed.Header, original.Header)
		}
		snapshot := getSnapshot(t, session)
		if snapshot.State != StateReady || snapshot.ClientCookie != 101 || snapshot.ServerCookie != 303 || snapshot.ConnectSequence != 0 || snapshot.LastInboundSequence != 0 {
			t.Fatalf("partial reset snapshot = %+v", snapshot)
		}
		cancel()
		_ = waitOutcome(t, result)
	})

	t.Run("full reset", func(t *testing.T) {
		firstTransport := newFakeTransport()
		secondTransport := newFakeTransport()
		config := testSessionConfig(t)
		config.ClientCookie = 1
		config.ServerCookie = 2
		config.GlobalSequence = 3
		config.CookieSource = CookieSourceFunc(func() (uint64, error) { return 9, nil })
		session := newTestSession(t, firstTransport, oneTransportConnector(secondTransport), config)
		defer session.Stop()

		result := submitAsync(session, context.Background(), testMessage("pending"))
		_ = decodeWrittenMessage(t, firstTransport)
		firstTransport.fail(errors.New("fault"))
		_ = decodeWrittenControl(t, secondTransport).(SessionReconnect)
		secondTransport.inject(controlFrame(t, SessionReset{Full: true}))
		if err := waitOutcome(t, result).err; !errors.Is(err, ErrSessionDisconnected) {
			t.Fatalf("full reset result = %v", err)
		}
		ident := decodeWrittenControl(t, secondTransport).(ClientIdent)
		if ident.Cookie != 9 || ident.GlobalSequence != 4 {
			t.Fatalf("full reset ident = %+v", ident)
		}
		snapshot := getSnapshot(t, session)
		if snapshot.ClientCookie != 9 || snapshot.ServerCookie != 0 || snapshot.GlobalSequence != 4 || snapshot.NextOutboundSequence != 1 || snapshot.Replay != 0 || snapshot.RetainedBytes != 0 {
			t.Fatalf("full reset snapshot = %+v", snapshot)
		}
	})
}

func TestSessionCookieSourceInitialResetAndDirectReady(t *testing.T) {
	t.Run("connector identification and full reset", func(t *testing.T) {
		firstTransport := newFakeTransport()
		secondTransport := newFakeTransport()
		connects := make(chan Transport, 2)
		connects <- firstTransport
		connects <- secondTransport
		cookies := make(chan uint64, 2)
		cookies <- 41
		cookies <- 42
		config := testSessionConfig(t)
		config.GlobalSequence = 17
		config.CookieSource = CookieSourceFunc(func() (uint64, error) { return <-cookies, nil })
		session := newTestSession(t, nil, ConnectorFunc(func(context.Context) (Transport, error) { return <-connects, nil }), config)
		defer session.Stop()

		initial := decodeWrittenControl(t, firstTransport).(ClientIdent)
		if initial.Cookie != 41 || initial.GlobalSequence != 17 {
			t.Fatalf("initial ident = %+v", initial)
		}
		firstTransport.inject(controlFrame(t, ServerIdent{Addresses: protocol.EntityAddrVec{config.ClientIdent.TargetAddress}, Cookie: 90}))
		waitSnapshot(t, session, func(snapshot SessionSnapshot) bool { return snapshot.State == StateReady })
		firstTransport.fail(errors.New("fault"))
		_ = decodeWrittenControl(t, secondTransport).(SessionReconnect)
		secondTransport.inject(controlFrame(t, SessionReset{Full: true}))
		reset := decodeWrittenControl(t, secondTransport).(ClientIdent)
		if reset.Cookie != 42 || reset.GlobalSequence != 18 {
			t.Fatalf("reset ident = %+v", reset)
		}
	})

	t.Run("direct ready transport", func(t *testing.T) {
		called := make(chan struct{}, 1)
		config := testSessionConfig(t)
		config.CookieSource = CookieSourceFunc(func() (uint64, error) {
			called <- struct{}{}
			return 1, nil
		})
		session := newTestSession(t, newFakeTransport(), nil, config)
		defer session.Stop()
		if snapshot := getSnapshot(t, session); snapshot.State != StateReady {
			t.Fatalf("direct transport state = %d", snapshot.State)
		}
		select {
		case <-called:
			t.Fatal("direct ready transport generated an identification cookie")
		default:
		}
	})
}

func TestSessionWaitReconnectAndStop(t *testing.T) {
	firstTransport := newFakeTransport()
	secondTransport := newFakeTransport()
	thirdTransport := newFakeTransport()
	connects := make(chan Transport, 2)
	connects <- secondTransport
	connects <- thirdTransport
	connector := ConnectorFunc(func(ctx context.Context) (Transport, error) {
		select {
		case transport := <-connects:
			return transport, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	config := testSessionConfig(t)
	config.ServerCookie = 2
	session := newTestSession(t, firstTransport, connector, config)

	firstTransport.fail(errors.New("fault"))
	_ = decodeWrittenControl(t, secondTransport).(SessionReconnect)
	secondTransport.inject(controlFrame(t, Wait{}))
	waitEvent(t, session, EventWait)
	_ = decodeWrittenControl(t, thirdTransport).(SessionReconnect)
	session.Stop()
	select {
	case <-thirdTransport.closed:
	case <-time.After(time.Second):
		t.Fatal("active transport was not closed")
	}
	if _, err := session.Submit(context.Background(), testMessage("late")); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("submit after stop = %v", err)
	}
}

func TestSessionReconnectAttemptsAreBounded(t *testing.T) {
	attempts := 0
	connector := ConnectorFunc(func(context.Context) (Transport, error) {
		attempts++
		return nil, errors.New("connect failed")
	})
	config := testSessionConfig(t)
	config.MaxReconnectAttempts = 2
	session := newTestSession(t, nil, connector, config)
	defer session.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := session.Submit(ctx, testMessage("pending")); !errors.Is(err, ErrReconnectExhausted) {
		t.Fatalf("pending submit error = %v", err)
	}
	if attempts != 2 {
		t.Fatalf("connector attempts = %d, want 2", attempts)
	}
	if _, err := session.Submit(ctx, testMessage("late")); !errors.Is(err, ErrReconnectExhausted) {
		t.Fatalf("late submit error = %v", err)
	}
}

func TestSessionMalformedPreReadyPeersConsumeReconnectBudget(t *testing.T) {
	transports := make(chan *fakeTransport, 2)
	first := newFakeTransport()
	second := newFakeTransport()
	transports <- first
	transports <- second
	attempts := 0
	connector := ConnectorFunc(func(context.Context) (Transport, error) {
		attempts++
		return <-transports, nil
	})
	config := testSessionConfig(t)
	config.MaxReconnectAttempts = 2
	config.CookieSource = CookieSourceFunc(func() (uint64, error) { return uint64(attempts), nil })
	session := newTestSession(t, nil, connector, config)
	defer session.Stop()
	result := submitAsync(session, context.Background(), testMessage("pending"))

	_ = decodeWrittenControl(t, first).(ClientIdent)
	message := testMessage("not ready")
	message.Header.Sequence = 1
	first.inject(messageFrame(t, message))
	_ = decodeWrittenControl(t, second).(ClientIdent)
	second.inject(controlFrame(t, Keepalive2{}))
	if err := waitOutcome(t, result).err; !errors.Is(err, ErrReconnectExhausted) {
		t.Fatalf("pending error = %v", err)
	}
	if attempts != 2 {
		t.Fatalf("connector attempts = %d, want 2", attempts)
	}
	snapshot := getSnapshot(t, session)
	if snapshot.LastInboundSequence != 0 || snapshot.InFlight != 0 || snapshot.State != StateDisconnected {
		t.Fatalf("malformed pre-ready frames mutated session data: %+v", snapshot)
	}
}

func TestSessionReconnectFramesRejectReadyOnlyControls(t *testing.T) {
	first := newFakeTransport()
	second := newFakeTransport()
	config := testSessionConfig(t)
	config.ServerCookie = 2
	config.MaxReconnectAttempts = 1
	session := newTestSession(t, first, oneTransportConnector(second), config)
	defer session.Stop()
	result := submitAsync(session, context.Background(), testMessage("pending"))
	_ = decodeWrittenMessage(t, first)
	first.fail(errors.New("fault"))
	_ = decodeWrittenControl(t, second).(SessionReconnect)
	second.inject(controlFrame(t, Ack{Sequence: ^uint64(0)}))
	if err := waitOutcome(t, result).err; !errors.Is(err, ErrReconnectExhausted) {
		t.Fatalf("pending error = %v", err)
	}
	if snapshot := getSnapshot(t, session); snapshot.Replay != 0 || snapshot.LastInboundSequence != 0 {
		t.Fatalf("reconnecting ack mutated protocol state: %+v", snapshot)
	}
}

func TestSessionReadyOnlyFramesFaultOutsideReady(t *testing.T) {
	message := testMessage("not ready")
	message.Header.Sequence = 1
	tests := []struct {
		name  string
		state SessionState
		frame Frame
	}{
		{name: "message connecting", state: StateConnecting, frame: messageFrame(t, message)},
		{name: "ack reconnecting", state: StateReconnecting, frame: controlFrame(t, Ack{Sequence: 1})},
		{name: "keepalive connecting", state: StateConnecting, frame: controlFrame(t, Keepalive2{})},
		{name: "keepalive ack reconnecting", state: StateReconnecting, frame: controlFrame(t, Keepalive2Ack{})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			owner := newUnitSessionOwner(t)
			owner.state = test.state
			owner.config.MaxReconnectAttempts = 0
			owner.handleFrame(test.frame)
			if owner.lastInbound != 0 || owner.terminalErr == nil || owner.state != StateDisconnected {
				t.Fatalf("ready-only frame mutated protocol state: state=%d inbound=%d terminal=%v", owner.state, owner.lastInbound, owner.terminalErr)
			}
		})
	}
}

func TestSessionConnectSequenceExhaustionIsTerminal(t *testing.T) {
	first := newFakeTransport()
	second := newFakeTransport()
	config := testSessionConfig(t)
	config.ServerCookie = 2
	config.ConnectSequence = ^uint64(0)
	session := newTestSession(t, first, oneTransportConnector(second), config)
	defer session.Stop()
	current := submitAsync(session, context.Background(), testMessage("current"))
	_ = decodeWrittenMessage(t, first)
	first.fail(errors.New("fault"))
	if err := waitOutcome(t, current).err; !errors.Is(err, ErrTransitionLimit) {
		t.Fatalf("current error = %v", err)
	}
	if _, err := session.Submit(context.Background(), testMessage("future")); !errors.Is(err, ErrTransitionLimit) {
		t.Fatalf("future error = %v", err)
	}
	select {
	case <-second.closed:
	default:
		t.Fatal("transport returned at exhaustion was not closed")
	}
}

func TestSessionSequenceAndTIDExhaustionFailClosed(t *testing.T) {
	t.Run("sequence", func(t *testing.T) {
		owner := newUnitSessionOwner(t)
		owner.nextOutbound = ^uint64(0)
		if sequence, err := owner.allocateSequence(); err != nil || sequence != ^uint64(0) {
			t.Fatalf("last sequence = %d, %v", sequence, err)
		}
		if sequence, err := owner.allocateSequence(); !errors.Is(err, ErrTransitionLimit) || sequence != 0 {
			t.Fatalf("wrapped sequence = %d, %v", sequence, err)
		}
	})

	t.Run("transaction id through submit", func(t *testing.T) {
		owner := newUnitSessionOwner(t)
		owner.nextTID = ^uint64(0)
		first := &submitCommand{ctx: context.Background(), message: testMessage("first"), result: make(chan submitResult, 1)}
		owner.submit(first)
		if len(owner.pending) != 1 || owner.pending[0].message.Header.TransactionID != ^uint64(0) {
			t.Fatalf("last transaction id was not admitted: %+v", owner.pending)
		}
		second := &submitCommand{ctx: context.Background(), message: testMessage("second"), result: make(chan submitResult, 1)}
		owner.submit(second)
		if err := (<-second.result).err; !errors.Is(err, ErrTransitionLimit) {
			t.Fatalf("wrapped transaction id error = %v", err)
		}
		if err := (<-first.result).err; !errors.Is(err, ErrTransitionLimit) {
			t.Fatalf("existing request exhaustion error = %v", err)
		}
		if owner.terminalErr == nil || owner.nextTID != 0 {
			t.Fatalf("transaction exhaustion was not sticky: terminal=%v next=%d", owner.terminalErr, owner.nextTID)
		}
	})
}

func TestSessionDroppedEventsAreObservable(t *testing.T) {
	t.Run("zero capacity snapshot", func(t *testing.T) {
		owner := newUnitSessionOwner(t)
		owner.session.events = make(chan SessionEvent)
		owner.emit(SessionEvent{Kind: EventAcknowledged})
		if snapshot := owner.snapshot(); snapshot.DroppedEvents != 1 {
			t.Fatalf("dropped events = %d, want 1", snapshot.DroppedEvents)
		}
	})

	t.Run("overflow event", func(t *testing.T) {
		owner := newUnitSessionOwner(t)
		owner.session.events = make(chan SessionEvent, 1)
		owner.emit(SessionEvent{Kind: EventAcknowledged})
		owner.emit(SessionEvent{Kind: EventKeepaliveAck})
		<-owner.session.events
		owner.emit(SessionEvent{Kind: EventWait})
		overflow := <-owner.session.events
		if overflow.Kind != EventOverflow || overflow.DroppedEvents != 1 {
			t.Fatalf("overflow event = %+v", overflow)
		}
		if snapshot := owner.snapshot(); snapshot.DroppedEvents != 2 {
			t.Fatalf("sticky dropped events = %d, want 2", snapshot.DroppedEvents)
		}
	})
}

func TestSessionStopCancelsConnectorAndClosesLateTransport(t *testing.T) {
	entered := make(chan struct{})
	late := newFakeTransport()
	connector := ConnectorFunc(func(ctx context.Context) (Transport, error) {
		close(entered)
		<-ctx.Done()
		return late, nil
	})
	session := newTestSession(t, nil, connector, testSessionConfig(t))
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("connector was not called")
	}
	stopped := make(chan struct{})
	go func() {
		session.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("stop did not cancel blocked connector")
	}
	select {
	case <-late.closed:
	default:
		t.Fatal("late connector transport was not closed")
	}
}

func TestSessionStopInterruptsBlockedTransportPumps(t *testing.T) {
	t.Run("read", func(t *testing.T) {
		transport := newFakeTransport()
		session := newTestSession(t, transport, nil, testSessionConfig(t))
		stopped := make(chan struct{})
		go func() {
			session.Stop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(time.Second):
			t.Fatal("blocked read was not interrupted by close")
		}
	})

	t.Run("write", func(t *testing.T) {
		transport := newBlockingWriteTransport()
		session := newTestSession(t, transport, nil, testSessionConfig(t))
		result := submitAsync(session, context.Background(), testMessage("blocked"))
		select {
		case <-transport.writeStarted:
		case <-time.After(time.Second):
			t.Fatal("write did not block")
		}
		stopped := make(chan struct{})
		go func() {
			session.Stop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(time.Second):
			t.Fatal("blocked write was not interrupted by close")
		}
		if err := waitOutcome(t, result).err; !errors.Is(err, ErrSessionClosed) {
			t.Fatalf("blocked write submit error = %v", err)
		}
	})
}

func testSessionConfig(t *testing.T) SessionConfig {
	t.Helper()
	address, err := protocol.IPv4EntityAddr(protocol.AddressV2, 1, netip.MustParseAddrPort("127.0.0.1:3300"))
	if err != nil {
		t.Fatal(err)
	}
	return SessionConfig{
		Limits:                  sessionTestLimits,
		MaxQueuedMessages:       8,
		MaxRetainedBytes:        1 << 20,
		MaxInFlightTransactions: 4,
		MaxReconnectAttempts:    4,
		MaxHandshakeTransitions: 8,
		EventBuffer:             64,
		ReconnectPolicy:         ReplayPending,
		GlobalSequenceSource:    &atomicGlobalSequenceSource{},
		ClientIdent: ClientIdent{
			Addresses:     protocol.EntityAddrVec{address},
			TargetAddress: address,
		},
	}
}

type authenticatedFakeTransport struct {
	*fakeTransport
	globalID uint64
}

func (transport *authenticatedFakeTransport) AuthenticatedGlobalID() uint64 {
	return transport.globalID
}

func newTestSession(t *testing.T, transport Transport, connector Connector, config SessionConfig) *Session {
	t.Helper()
	session, err := NewSession(transport, connector, config)
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func newUnitSessionOwner(t *testing.T) *sessionOwner {
	t.Helper()
	config := testSessionConfig(t)
	return &sessionOwner{
		session:      &Session{commands: make(chan any), events: make(chan SessionEvent, config.EventBuffer), incoming: make(chan Message, config.MaxQueuedMessages), done: make(chan struct{})},
		config:       config,
		state:        StateReady,
		byRequest:    make(map[*submitCommand]*pendingRequest),
		byTID:        make(map[uint64]*pendingRequest),
		nextOutbound: 1,
		nextTID:      1,
	}
}

func oneTransportConnector(transport Transport) Connector {
	return ConnectorFunc(func(context.Context) (Transport, error) { return transport, nil })
}

func testMessage(front string) Message {
	return Message{Lengths: MessageLengths{Front: uint32(len(front))}, Front: []byte(front)}
}

func messageFrame(t *testing.T, message Message) Frame {
	t.Helper()
	frame, err := EncodeMessage(message, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

func controlFrame(t *testing.T, payload any) Frame {
	t.Helper()
	frame, err := EncodeControl(payload, sessionTestLimits)
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

func cloneFrame(frame Frame) Frame {
	clone := Frame{Tag: frame.Tag, Segments: make([]Segment, len(frame.Segments))}
	for index, segment := range frame.Segments {
		clone.Segments[index] = Segment{Alignment: segment.Alignment, Data: append([]byte(nil), segment.Data...)}
	}
	return clone
}

func submitAsync(session *Session, ctx context.Context, message Message) <-chan submitOutcome {
	result := make(chan submitOutcome, 1)
	go func() {
		response, err := session.Submit(ctx, message)
		result <- submitOutcome{message: response, err: err}
	}()
	return result
}

func decodeWrittenMessage(t *testing.T, transport *fakeTransport) Message {
	t.Helper()
	frame := waitWrittenFrame(t, transport)
	message, err := DecodeMessage(frame, testLimits)
	if err != nil {
		t.Fatalf("decode written message: %v (frame tag %d)", err, frame.Tag)
	}
	return message
}

func decodeWrittenControl(t *testing.T, transport *fakeTransport) any {
	t.Helper()
	frame := waitWrittenFrame(t, transport)
	payload, err := DecodeControl(frame, sessionTestLimits)
	if err != nil {
		t.Fatalf("decode written control: %v (frame tag %d)", err, frame.Tag)
	}
	return payload
}

func waitWrittenFrame(t *testing.T, transport *fakeTransport) Frame {
	t.Helper()
	select {
	case frame := <-transport.writes:
		return frame
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for written frame")
		return Frame{}
	}
}

func waitOutcome(t *testing.T, result <-chan submitOutcome) submitOutcome {
	t.Helper()
	select {
	case outcome := <-result:
		return outcome
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for submit result")
		return submitOutcome{}
	}
}

func waitEvent(t *testing.T, session *Session, kind EventKind) SessionEvent {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		select {
		case event := <-session.Events():
			if event.Kind == kind {
				return event
			}
		case <-deadline:
			t.Fatalf("timed out waiting for event %d", kind)
		}
	}
}

func getSnapshot(t *testing.T, session *Session) SessionSnapshot {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	snapshot, err := session.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func waitSnapshot(t *testing.T, session *Session, match func(SessionSnapshot) bool) SessionSnapshot {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		snapshot := getSnapshot(t, session)
		if match(snapshot) {
			return snapshot
		}
		if time.Now().After(deadline) {
			t.Fatalf("snapshot did not reach expected state: %+v", snapshot)
		}
	}
}

func TestCloneMessageDoesNotRetainCallerBuffers(t *testing.T) {
	message := testMessage("payload")
	clone := cloneMessage(message)
	message.Front[0] = 'X'
	if reflect.DeepEqual(message, clone) || string(clone.Front) != "payload" {
		t.Fatalf("clone retained caller buffer: original=%q clone=%q", message.Front, clone.Front)
	}
}
