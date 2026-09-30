package msgr

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/otuschhoff/rados-go/internal/protocol"
)

var (
	ErrSessionClosed       = errors.New("messenger session closed")
	ErrSessionDisconnected = errors.New("messenger session disconnected")
	ErrSessionRenewal      = errors.New("messenger session credential renewal")
	ErrQueueSaturated      = errors.New("messenger outbound queue saturated")
	ErrTooManyInFlight     = errors.New("messenger in-flight limit reached")
	ErrTransitionLimit     = errors.New("messenger handshake transition limit reached")
	ErrReconnectExhausted  = errors.New("messenger reconnect attempts exhausted")
	ErrOutcomeUnknown      = errors.New("messenger request outcome unknown")
	ErrControlInvalidated  = errors.New("messenger control generation invalidated")
)

// Transport is an authenticated, framed messenger connection. Close must
// interrupt any blocked ReadFrame or WriteFrame call.
type Transport interface {
	ReadFrame() (Frame, error)
	WriteFrame(Frame) error
	Close() error
}

type AuthenticatedTransport interface {
	Transport
	AuthenticatedGlobalID() uint64
}

type CredentialIdentityTransport interface {
	Transport
	CredentialIdentity() [32]byte
}

type RenewalTransport interface {
	Transport
	RenewalDue() <-chan struct{}
}

// Connector returns a freshly authenticated transport. Session state and
// replay state remain owned by Session; crypto counters remain in Transport.
type Connector interface {
	Connect(context.Context) (Transport, error)
}

type ConnectorFunc func(context.Context) (Transport, error)

func (function ConnectorFunc) Connect(ctx context.Context) (Transport, error) {
	return function(ctx)
}

type CookieSource interface {
	Cookie() (uint64, error)
}

type CookieSourceFunc func() (uint64, error)

func (function CookieSourceFunc) Cookie() (uint64, error) { return function() }

type randomCookieSource struct{}

func (randomCookieSource) Cookie() (uint64, error) {
	for {
		var data [8]byte
		if _, err := rand.Read(data[:]); err != nil {
			return 0, err
		}
		if cookie := binary.LittleEndian.Uint64(data[:]); cookie != 0 {
			return cookie, nil
		}
	}
}

type ReconnectPolicy uint8

const (
	FailPending ReconnectPolicy = iota
	ReplayPending
)

type SessionState uint8

const (
	StateDisconnected SessionState = iota
	StateConnecting
	StateReconnecting
	StateReady
	StateWait
	StateStopped
)

type EventKind uint8

const (
	EventStateChanged EventKind = iota + 1
	EventSequenceGap
	EventDuplicateDropped
	EventAcknowledged
	EventKeepaliveAck
	EventTransportFault
	EventSessionReset
	EventRetry
	EventRetryGlobal
	EventWait
	EventReconnectOK
	EventOverflow
	EventCredentialRenewal
)

type SessionEvent struct {
	Kind          EventKind
	State         SessionState
	Sequence      uint64
	Expected      uint64
	Err           error
	Full          bool
	Time          Timestamp
	DroppedEvents uint64
}

type SessionSnapshot struct {
	State                 SessionState
	AuthenticatedGlobalID uint64
	ServerGlobalID        int64
	ServerAddresses       protocol.EntityAddrVec
	ServerFeatures        uint64
	ServerFlags           uint64
	NextOutboundSequence  uint64
	LastInboundSequence   uint64
	NextTransactionID     uint64
	ClientCookie          uint64
	ServerCookie          uint64
	GlobalSequence        uint64
	ConnectSequence       uint64
	Queued                int
	InFlight              int
	Replay                int
	RetainedBytes         uint64
	QueuedReceiveMessages int
	RetainedReceiveBytes  uint64
	ControlQueued         int
	ControlRetainedBytes  uint64
	ReconnectAttempts     int
	HandshakeTransitions  int
	DroppedEvents         uint64
}

type SessionDiagnosticKind uint8

const (
	DiagnosticCredentialRenewalDue SessionDiagnosticKind = iota + 1
	DiagnosticCredentialRenewalCompleted
	DiagnosticSessionClosed
)

type SessionDiagnostic struct {
	Kind       SessionDiagnosticKind
	Service    string
	ServiceID  int32
	SessionID  uint64
	Generation uint64
	Timestamp  time.Time
}

type SessionDiagnosticObserver func(SessionDiagnostic)

type SessionConfig struct {
	Limits                    Limits
	ReceiveBudget             *ReceiveBudget
	MaxQueuedReceiveBytes     uint64
	MaxQueuedMessages         int
	MaxRetainedBytes          uint64
	MaxInFlightTransactions   int
	MaxReconnectAttempts      int
	InitialReconnectBackoff   time.Duration
	MaxReconnectBackoff       time.Duration
	MaxHandshakeTransitions   int
	EventBuffer               int
	ReconnectPolicy           ReconnectPolicy
	ClientIdent               ClientIdent
	ClientCookie              uint64
	ServerCookie              uint64
	GlobalSequence            uint64
	GlobalSequenceSource      GlobalSequenceSource
	ConnectSequence           uint64
	CookieSource              CookieSource
	DiagnosticObserver        SessionDiagnosticObserver
	ModeObserver              ModeObserver
	DiagnosticService         string
	DiagnosticServiceID       int32
	DiagnosticSessionID       uint64
	DiagnosticSessionIDSource func() uint64
	DiagnosticNow             func() time.Time
	reconnectWait             func(context.Context, time.Duration) error
}

const (
	defaultInitialReconnectBackoff = 200 * time.Millisecond
	defaultMaxReconnectBackoff     = 15 * time.Second
)

type GlobalSequenceSource interface {
	Next(after uint64) (uint64, error)
}

type atomicGlobalSequenceSource struct{ value atomic.Uint64 }

func (source *atomicGlobalSequenceSource) Next(after uint64) (uint64, error) {
	for {
		current := source.value.Load()
		base := max(current, after)
		if base == math.MaxUint64 {
			return 0, ErrTransitionLimit
		}
		if source.value.CompareAndSwap(current, base+1) {
			return base + 1, nil
		}
	}
}

var processGlobalSequences atomicGlobalSequenceSource

func nextGlobalSequence(source GlobalSequenceSource, after uint64) (uint64, error) {
	next, err := source.Next(after)
	if err != nil {
		return 0, err
	}
	if next <= after {
		return 0, fmt.Errorf("%w: global sequence %d does not advance past %d", ErrMalformed, next, after)
	}
	return next, nil
}

type Session struct {
	controlGeneration atomic.Uint64
	timingActive      atomic.Int64
	commands          chan any
	events            chan SessionEvent
	resets            chan struct{}
	incoming          chan Message
	terminal          chan error
	done              chan struct{}
	stopped           chan struct{}
	stopOnce          sync.Once
}

type submitCommand struct {
	controlGeneration uint64
	ctx               context.Context
	message           Message
	oneWay            bool
	control           bool
	admitted          chan struct{}
	result            chan submitResult
}

type submitResult struct {
	message Message
	err     error
}

func (result submitResult) take() (Message, error) {
	result.message.receiveLease.release()
	result.message.receiveLease = nil
	return result.message, result.err
}

type cancelCommand struct {
	request *submitCommand
	err     error
}

type snapshotCommand struct{ result chan SessionSnapshot }
type stopCommand struct{ done chan struct{} }

type pumpFrame struct {
	generation uint64
	frame      Frame
	receivedAt time.Time
	err        error
}

type pumpWriteResult struct {
	generation uint64
	taskID     uint64
	request    *submitCommand
	err        error
}

type pumpFault struct {
	generation uint64
	err        error
}

type renewalDue struct{ generation uint64 }

type connectRequest struct {
	generation uint64
	ctx        context.Context
	delay      time.Duration
}

type connectResult struct {
	generation uint64
	transport  Transport
	err        error
}

type writeTask struct {
	id            uint64
	frame         Frame
	request       *submitCommand
	seq           uint64
	transactionID uint64
}

type pendingRequest struct {
	request         *submitCommand
	message         Message
	bytes           uint64
	seq             uint64
	sent            bool
	mayHaveExecuted bool
}

type sessionOwner struct {
	session         *Session
	config          SessionConfig
	frameReceivedAt time.Time
	receiveQueue    []Message
	receiveBytes    uint64

	state         SessionState
	transport     Transport
	transportDone chan struct{}
	generation    uint64
	writeTasks    chan writeTask
	writeBusy     bool
	nextWriteID   uint64
	controlQueue  []Frame
	pending       []*pendingRequest
	byRequest     map[*submitCommand]*pendingRequest
	byTID         map[uint64]*pendingRequest
	replay        []*pendingRequest
	retainedBytes uint64
	controlCount  int
	controlBytes  uint64

	nextOutbound          uint64
	sequenceExhausted     bool
	lastInbound           uint64
	nextTID               uint64
	tidExhausted          bool
	clientCookie          uint64
	serverCookie          uint64
	globalSeq             uint64
	connectSeq            uint64
	authenticatedGlobalID uint64
	serverGlobalID        int64
	serverAddresses       protocol.EntityAddrVec
	serverFeatures        uint64
	serverFlags           uint64
	connectedOnce         bool

	reconnectAttempts int
	transitions       int
	connectPending    bool
	partialReset      bool
	terminalErr       error
	renewalPending    bool
	renewalInProgress bool
	credentialKnown   bool
	credentialChanged bool
	credentialID      [32]byte
	droppedEvents     uint64
	reportedDrops     uint64

	connectorRequests chan connectRequest
	connectorResults  chan connectResult
	frames            chan pumpFrame
	writes            chan pumpWriteResult
	faults            chan pumpFault
	renewals          chan renewalDue
	pumpWG            sync.WaitGroup
	connectorContext  context.Context
	connectorCancel   context.CancelFunc
}

func NewSession(transport Transport, connector Connector, config SessionConfig) (*Session, error) {
	if config.MaxQueuedMessages <= 0 || config.MaxRetainedBytes == 0 || config.MaxInFlightTransactions <= 0 {
		return nil, fmt.Errorf("%w: session limits must be positive", ErrQueueSaturated)
	}
	if config.MaxReconnectAttempts < 0 || config.InitialReconnectBackoff < 0 || config.MaxReconnectBackoff < 0 || config.MaxHandshakeTransitions <= 0 || config.EventBuffer < 0 {
		return nil, fmt.Errorf("%w: invalid session configuration", ErrMalformed)
	}
	if config.InitialReconnectBackoff == 0 {
		config.InitialReconnectBackoff = defaultInitialReconnectBackoff
	}
	if config.MaxReconnectBackoff == 0 {
		config.MaxReconnectBackoff = defaultMaxReconnectBackoff
	}
	if config.InitialReconnectBackoff > config.MaxReconnectBackoff {
		return nil, fmt.Errorf("%w: initial reconnect backoff exceeds maximum", ErrMalformed)
	}
	if config.reconnectWait == nil {
		config.reconnectWait = waitReconnectBackoff
	}
	if config.Limits.MaxSegmentBytes == 0 || config.Limits.MaxFrameBytes == 0 {
		return nil, fmt.Errorf("%w: frame limits must be positive", ErrMalformed)
	}
	if config.DiagnosticObserver != nil && config.DiagnosticSessionIDSource != nil {
		config.DiagnosticSessionID = config.DiagnosticSessionIDSource()
	}
	if config.DiagnosticObserver != nil && (config.DiagnosticService == "" || config.DiagnosticSessionID == 0) {
		return nil, fmt.Errorf("%w: diagnostic service and session ID are required", ErrMalformed)
	}
	if config.DiagnosticNow == nil {
		config.DiagnosticNow = time.Now
	}
	if config.GlobalSequence != 0 && config.ClientIdent.GlobalSequence != 0 && config.GlobalSequence != config.ClientIdent.GlobalSequence {
		return nil, fmt.Errorf("%w: conflicting global sequences", ErrMalformed)
	}
	if config.GlobalSequence == 0 {
		config.GlobalSequence = config.ClientIdent.GlobalSequence
	}
	if config.GlobalSequenceSource == nil {
		config.GlobalSequenceSource = &processGlobalSequences
	}
	minimumAfter := uint64(0)
	if config.GlobalSequence > 0 {
		minimumAfter = config.GlobalSequence - 1
	}
	var err error
	config.GlobalSequence, err = nextGlobalSequence(config.GlobalSequenceSource, minimumAfter)
	if err != nil {
		return nil, err
	}
	config.ClientIdent.GlobalSequence = config.GlobalSequence
	if config.CookieSource == nil {
		config.CookieSource = randomCookieSource{}
	}

	if err := config.ReceiveBudget.admit(); err != nil {
		return nil, err
	}
	incomingSize := config.MaxQueuedMessages
	if config.ReceiveBudget != nil || config.MaxQueuedReceiveBytes != 0 {
		incomingSize = 0
	}
	session := &Session{
		commands: make(chan any),
		events:   make(chan SessionEvent, config.EventBuffer),
		resets:   make(chan struct{}, 1),
		incoming: make(chan Message, incomingSize),
		terminal: make(chan error, 1),
		done:     make(chan struct{}),
		stopped:  make(chan struct{}),
	}
	session.controlGeneration.Store(1)
	owner := &sessionOwner{
		session:           session,
		config:            config,
		state:             StateDisconnected,
		transport:         transport,
		byRequest:         make(map[*submitCommand]*pendingRequest),
		byTID:             make(map[uint64]*pendingRequest),
		nextOutbound:      1,
		nextTID:           1,
		clientCookie:      config.ClientCookie,
		serverCookie:      config.ServerCookie,
		globalSeq:         config.GlobalSequence,
		connectedOnce:     transport != nil,
		connectSeq:        config.ConnectSequence,
		connectorRequests: make(chan connectRequest),
		connectorResults:  make(chan connectResult),
		frames:            make(chan pumpFrame, 1),
		writes:            make(chan pumpWriteResult),
		faults:            make(chan pumpFault, 2),
		renewals:          make(chan renewalDue),
	}
	connectorCtx, connectorCancel := context.WithCancel(context.Background())
	if transport != nil {
		observeTransportMode(config, transport)
	}
	owner.connectorContext = connectorCtx
	owner.connectorCancel = connectorCancel
	if credentialTransport, ok := transport.(CredentialIdentityTransport); ok {
		owner.credentialID = credentialTransport.CredentialIdentity()
		owner.credentialKnown = true
	}
	owner.pumpWG.Add(1)
	go owner.connectorPump(connectorCtx, connector)
	go owner.run()
	return session, nil
}

func (session *Session) Submit(ctx context.Context, message Message) (Message, error) {
	return session.submit(ctx, message, false, false, nil)
}

func (session *Session) OwnsReplyMessages() bool { return true }

// SubmitAdmitted invokes admitted after the request is registered by the
// session owner and before waiting for its reply.
func (session *Session) SubmitAdmitted(ctx context.Context, message Message, admitted func()) (Message, error) {
	return session.submit(ctx, message, false, false, admitted)
}

// Send transmits a one-way message and returns after its frame has been
// accepted by the transport. Callers must resubmit it after reconnect.
func (session *Session) Send(ctx context.Context, message Message) error {
	_, err := session.submit(ctx, message, true, false, nil)
	return err
}

func (session *Session) submit(ctx context.Context, message Message, oneWay, control bool, admitted func()) (Message, error) {
	return session.submitGeneration(ctx, message, oneWay, control, admitted, 0)
}

func (session *Session) submitGeneration(ctx context.Context, message Message, oneWay, control bool, admitted func(), generation uint64) (Message, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if timing := requestTiming(ctx); timing != nil {
		timing.bindTransaction(message.Header.TransactionID)
		session.timingActive.Add(1)
		defer session.timingActive.Add(-1)
		defer recordRequestTiming(ctx, "submit_return", 0)
	}
	recordRequestTiming(ctx, "submit_enter", message.Header.TransactionID)
	request := &submitCommand{ctx: ctx, message: message, oneWay: oneWay, control: control, controlGeneration: generation, admitted: make(chan struct{}), result: make(chan submitResult, 1)}
	select {
	case session.commands <- request:
	case <-ctx.Done():
		return Message{}, ctx.Err()
	case <-session.done:
		return Message{}, ErrSessionClosed
	}
	select {
	case <-request.admitted:
		if admitted != nil {
			admitted()
		}
	case <-session.done:
		select {
		case <-request.admitted:
			if admitted != nil {
				admitted()
			}
			result := <-request.result
			return result.take()
		default:
			return Message{}, ErrSessionClosed
		}
	}
	select {
	case result := <-request.result:
		return result.take()
	case <-ctx.Done():
		select {
		case session.commands <- cancelCommand{request: request, err: ctx.Err()}:
		case <-session.done:
			result := <-request.result
			return result.take()
		}
		result := <-request.result
		return result.take()
	case <-session.done:
		result := <-request.result
		return result.take()
	}
}

func (session *Session) Snapshot(ctx context.Context) (SessionSnapshot, error) {
	result := make(chan SessionSnapshot, 1)
	select {
	case session.commands <- snapshotCommand{result: result}:
	case <-ctx.Done():
		return SessionSnapshot{}, ctx.Err()
	case <-session.done:
		return SessionSnapshot{State: StateStopped}, ErrSessionClosed
	}
	select {
	case snapshot := <-result:
		return snapshot, nil
	case <-ctx.Done():
		return SessionSnapshot{}, ctx.Err()
	case <-session.done:
		return SessionSnapshot{State: StateStopped}, ErrSessionClosed
	}
}

func (session *Session) Events() <-chan SessionEvent { return session.events }
func (session *Session) Resets() <-chan struct{}     { return session.resets }
func (session *Session) Incoming() <-chan Message    { return session.incoming }
func (session *Session) Terminal() <-chan error      { return session.terminal }
func (session *Session) Done() <-chan struct{}       { return session.done }

func (session *Session) Stop() {
	session.stopOnce.Do(func() {
		done := make(chan struct{})
		select {
		case session.commands <- stopCommand{done: done}:
			<-done
		case <-session.done:
		}
	})
	if session.stopped != nil {
		<-session.stopped
	}
}

func (owner *sessionOwner) run() {
	if owner.transport != nil {
		owner.startTransport(owner.transport, StateReady)
	} else {
		owner.beginReconnect()
	}
	for {
		owner.dispatch()
		var delivery chan Message
		var message Message
		if len(owner.receiveQueue) != 0 {
			delivery = owner.session.incoming
			message = owner.receiveQueue[0]
			message.receiveLease = nil
		}
		select {
		case delivery <- message:
			queued := owner.receiveQueue[0]
			owner.receiveBytes -= receiveMessageBytes(queued)
			queued.receiveLease.release()
			owner.receiveQueue[0] = Message{}
			owner.receiveQueue = owner.receiveQueue[1:]
		case command := <-owner.session.commands:
			switch value := command.(type) {
			case *submitCommand:
				owner.submit(value)
			case cancelCommand:
				owner.cancel(value)
			case snapshotCommand:
				value.result <- owner.snapshot()
			case stopCommand:
				owner.stop(value.done)
				return
			}
		case incoming := <-owner.frames:
			if incoming.generation == owner.generation && owner.terminalErr == nil {
				if incoming.err != nil {
					if errors.Is(incoming.err, ErrReceiveBudgetExceeded) {
						owner.failTerminal(incoming.err)
					} else {
						owner.handleFault(incoming.err)
					}
				} else {
					owner.frameReceivedAt = incoming.receivedAt
					owner.handleFrame(incoming.frame)
				}
			} else {
				incoming.frame.receiveLease.release()
			}
		case written := <-owner.writes:
			if written.generation == owner.generation {
				owner.writeBusy = false
				if written.err != nil {
					owner.handleFault(written.err)
				} else if written.request != nil {
					if pending := owner.byRequest[written.request]; pending != nil && pending.request.oneWay {
						owner.removePending(pending)
						pending.request.result <- submitResult{}
					}
				}
			}
		case fault := <-owner.faults:
			if fault.generation == owner.generation {
				owner.handleFault(fault.err)
			}
		case renewal := <-owner.renewals:
			if renewal.generation == owner.generation {
				owner.renewalPending = true
				if !owner.renewalInProgress {
					owner.renewalInProgress = true
					owner.emit(SessionEvent{Kind: EventCredentialRenewal})
					owner.emitDiagnostic(DiagnosticCredentialRenewalDue)
				}
			}
		case connected := <-owner.connectorResults:
			owner.handleConnected(connected)
		}
	}
}

func (owner *sessionOwner) submit(command *submitCommand) {
	defer func() {
		command.message = Message{}
		if command.admitted != nil {
			close(command.admitted)
		}
	}()
	if command.controlGeneration != 0 && command.controlGeneration != owner.session.ControlGeneration() {
		command.result <- submitResult{err: ErrControlInvalidated}
		return
	}
	if err := command.ctx.Err(); err != nil {
		command.result <- submitResult{err: err}
		return
	}
	if owner.terminalErr != nil {
		command.result <- submitResult{err: owner.terminalErr}
		return
	}
	if command.control {
		if err := validateControlMessage(command.message); err != nil {
			command.result <- submitResult{err: err}
			return
		}
	}
	messageBytes, err := admissionMessageBytes(command.message, owner.config.Limits)
	if err != nil {
		command.result <- submitResult{err: err}
		return
	}
	if command.control && (owner.controlCount >= MaxControlMessages || owner.controlBytes > MaxControlRetainedBytes || messageBytes > MaxControlRetainedBytes-owner.controlBytes) ||
		!command.control && (len(owner.pending)-owner.controlCount >= owner.config.MaxQueuedMessages || owner.retainedBytes > owner.config.MaxRetainedBytes || messageBytes > owner.config.MaxRetainedBytes-owner.retainedBytes) {
		command.result <- submitResult{err: ErrQueueSaturated}
		return
	}
	message := cloneMessage(command.message)
	if message.Header.TransactionID == 0 {
		transactionID, err := owner.takeTID()
		if err != nil {
			owner.failTerminal(err)
			command.result <- submitResult{err: err}
			return
		}
		message.Header.TransactionID = transactionID
	} else if _, exists := owner.byTID[message.Header.TransactionID]; exists {
		command.result <- submitResult{err: fmt.Errorf("%w: duplicate transaction id %d", ErrMalformed, message.Header.TransactionID)}
		return
	}
	pending := &pendingRequest{request: command, message: message, bytes: messageBytes}
	owner.pending = append(owner.pending, pending)
	owner.byRequest[command] = pending
	owner.byTID[message.Header.TransactionID] = pending
	if command.control {
		owner.controlCount++
		owner.controlBytes += messageBytes
	} else {
		owner.retainedBytes += messageBytes
	}
	if timing := requestTiming(command.ctx); timing != nil {
		timing.bindTransaction(message.Header.TransactionID)
	}
	recordRequestTiming(command.ctx, "admitted", message.Header.TransactionID)
}

func (owner *sessionOwner) cancel(command cancelCommand) {
	pending := owner.byRequest[command.request]
	if pending == nil {
		return
	}
	owner.removePending(pending)
	if pending.mayHaveExecuted {
		command.request.result <- submitResult{err: fmt.Errorf("%w: %w", ErrOutcomeUnknown, command.err)}
		return
	}
	command.request.result <- submitResult{err: command.err}
}

func (owner *sessionOwner) dispatch() {
	if owner.writeBusy || owner.writeTasks == nil || owner.state == StateDisconnected || owner.state == StateWait || owner.state == StateStopped {
		return
	}
	if len(owner.controlQueue) > 0 {
		frame := owner.controlQueue[0]
		owner.controlQueue = owner.controlQueue[1:]
		owner.sendWrite(writeTask{frame: frame})
		return
	}
	if owner.state != StateReady {
		return
	}
	pending := owner.nextPendingWrite()
	if owner.renewalPending && (pending == nil || !pending.request.control) {
		if owner.inFlightCount() == 0 {
			owner.renewalPending = false
			owner.handleFault(ErrSessionRenewal)
		}
		return
	}
	if pending != nil {
		if pending.request.controlGeneration != 0 && pending.request.controlGeneration != owner.session.ControlGeneration() {
			owner.removePending(pending)
			pending.request.result <- submitResult{err: ErrControlInvalidated}
			return
		}
		if !pending.request.control && owner.inFlightCount() >= owner.config.MaxInFlightTransactions {
			return
		}
		if err := pending.request.ctx.Err(); err != nil {
			if pending.request.control {
				owner.cancel(cancelCommand{request: pending.request, err: err})
				return
			}
			owner.removePending(pending)
			pending.request.result <- submitResult{err: err}
			return
		}
		if pending.seq == 0 {
			sequence, err := owner.allocateSequence()
			if err != nil {
				owner.failTerminal(err)
				return
			}
			pending.seq = sequence
			pending.message.Header.Sequence = pending.seq
		}
		frame, err := EncodeMessage(pending.message, owner.config.Limits)
		if err != nil {
			owner.removePending(pending)
			pending.request.result <- submitResult{err: err}
			return
		}
		pending.sent = true
		pending.mayHaveExecuted = true
		if !containsPending(owner.replay, pending) {
			owner.replay = append(owner.replay, pending)
		}
		owner.sendWrite(writeTask{frame: frame, request: pending.request, seq: pending.seq, transactionID: pending.message.Header.TransactionID})
		return
	}
}

func (owner *sessionOwner) sendWrite(task writeTask) {
	owner.nextWriteID++
	task.id = owner.nextWriteID
	owner.writeBusy = true
	owner.writeTasks <- task
}

func (owner *sessionOwner) handleFrame(frame Frame) {
	defer func() { frame.receiveLease.release() }()
	if frame.Tag == TagMessage {
		if owner.state != StateReady {
			owner.handleFault(fmt.Errorf("%w: message in state %d", ErrMalformed, owner.state))
			return
		}
		decode := DecodeMessage
		if transport, ok := owner.transport.(interface{ OwnsReadFrames() bool }); frame.receiveLease != nil || ok && transport.OwnsReadFrames() {
			decode = decodeOwnedMessage
		}
		message, err := decode(frame, owner.config.Limits)
		if err != nil {
			owner.handleFault(err)
			return
		}
		message.receiveLease = frame.receiveLease
		frame.receiveLease = nil
		owner.handleMessage(message)
		return
	}
	if frame.receiveLease != nil {
		switch frame.Tag {
		case TagAuthRequest, TagAuthReplyMore, TagAuthRequestMore, TagAuthDone, TagAuthSignature:
			var copyBytes uint64
			for _, segment := range frame.Segments {
				copyBytes += 2*uint64(len(segment.Data)) + 64
			}
			if err := frame.receiveLease.resize(frame.receiveLease.bytes + copyBytes); err != nil {
				owner.failTerminal(err)
				return
			}
		}
	}
	payload, err := DecodeControl(frame, owner.config.Limits)
	if err != nil {
		owner.handleFault(err)
		return
	}
	owner.handleControl(payload)
}

func (owner *sessionOwner) handleMessage(message Message) {
	defer func() { message.receiveLease.release() }()
	transportGeneration := owner.session.ControlGeneration()
	if !owner.acceptAcknowledgment(message.Header.AckSequence) {
		return
	}
	sequence := message.Header.Sequence
	if sequence <= owner.lastInbound {
		owner.emit(SessionEvent{Kind: EventDuplicateDropped, Sequence: sequence})
		return
	}
	if owner.lastInbound != math.MaxUint64 && sequence != owner.lastInbound+1 {
		owner.emit(SessionEvent{Kind: EventSequenceGap, Sequence: sequence, Expected: owner.lastInbound + 1})
	}
	owner.lastInbound = sequence
	owner.trimReplay(message.Header.AckSequence)
	if pending := owner.byTID[message.Header.TransactionID]; pending != nil && !pending.request.control {
		if timing := requestTiming(pending.request.ctx); timing != nil {
			timing.Record("frame_received", message.Header.TransactionID, owner.frameReceivedAt)
		}
		owner.removePending(pending)
		recordRequestTiming(pending.request.ctx, "reply_delivered", message.Header.TransactionID)
		pending.request.result <- submitResult{message: message}
		message.receiveLease = nil
		owner.queueControl(Ack{Sequence: sequence})
		return
	}
	owner.queueControl(Ack{Sequence: sequence})
	if owner.session.ControlGeneration() != transportGeneration {
		return
	}
	message.TransportGeneration = transportGeneration
	if owner.config.ReceiveBudget != nil || owner.config.MaxQueuedReceiveBytes != 0 {
		bytes := receiveMessageBytes(message)
		limit := owner.config.MaxQueuedReceiveBytes
		if len(owner.receiveQueue) >= owner.config.MaxQueuedMessages || limit != 0 && (owner.receiveBytes > limit || bytes > limit-owner.receiveBytes) {
			owner.failTerminal(fmt.Errorf("%w: unsolicited receive queue is full", ErrQueueSaturated))
			return
		}
		owner.receiveQueue = append(owner.receiveQueue, message)
		owner.receiveBytes += bytes
		message.receiveLease = nil
		return
	}
	select {
	case owner.session.incoming <- message:
	default:
		owner.failTerminal(fmt.Errorf("%w: unsolicited message queue is full", ErrQueueSaturated))
	}
}

func (owner *sessionOwner) handleControl(payload any) {
	switch value := payload.(type) {
	case Ack:
		if !owner.readyControl("ack") {
			return
		}
		if !owner.acceptAcknowledgment(value.Sequence) {
			return
		}
		owner.trimReplay(value.Sequence)
		owner.emit(SessionEvent{Kind: EventAcknowledged, Sequence: value.Sequence})
	case Keepalive2:
		if !owner.readyControl("keepalive2") {
			return
		}
		owner.queueControl(Keepalive2Ack(value))
	case Keepalive2Ack:
		if !owner.readyControl("keepalive2 ack") {
			return
		}
		owner.emit(SessionEvent{Kind: EventKeepaliveAck, Time: value.Timestamp})
	case SessionReset:
		if !owner.transitionAllowed(StateReconnecting) {
			return
		}
		owner.handleReset(value.Full)
	case SessionRetry:
		if !owner.transitionAllowed(StateReconnecting) {
			return
		}
		if value.ConnectSequence == math.MaxUint64 {
			owner.failTerminal(ErrTransitionLimit)
			return
		}
		owner.connectSeq = value.ConnectSequence + 1
		owner.emit(SessionEvent{Kind: EventRetry, Sequence: owner.connectSeq})
		owner.sendReconnect()
	case SessionRetryGlobal:
		if !owner.transitionAllowed(StateReconnecting) {
			return
		}
		next, err := nextGlobalSequence(owner.config.GlobalSequenceSource, max(owner.globalSeq, value.GlobalSequence))
		if err != nil {
			owner.failTerminal(ErrTransitionLimit)
			return
		}
		owner.globalSeq = next
		owner.emit(SessionEvent{Kind: EventRetryGlobal, Sequence: owner.globalSeq})
		owner.sendReconnect()
	case Wait:
		if owner.state != StateConnecting && owner.state != StateReconnecting {
			owner.handleFault(fmt.Errorf("%w: wait in state %d", ErrMalformed, owner.state))
			return
		}
		owner.transitions++
		owner.setState(StateWait)
		owner.emit(SessionEvent{Kind: EventWait})
		owner.handleFault(ErrSessionDisconnected)
	case SessionReconnectOK:
		if !owner.transitionAllowed(StateReconnecting) {
			return
		}
		if !owner.acceptAcknowledgment(value.MessageSequence) {
			return
		}
		owner.trimReplay(value.MessageSequence)
		owner.prepareReplay()
		owner.partialReset = false
		owner.reconnectAttempts = 0
		owner.setState(StateReady)
		owner.emit(SessionEvent{Kind: EventReconnectOK, Sequence: value.MessageSequence})
		owner.completeRenewal()
	case ServerIdent:
		if owner.state != StateConnecting {
			owner.handleFault(fmt.Errorf("%w: server ident in state %d", ErrMalformed, owner.state))
			return
		}
		owner.transitions++
		if owner.transitions > owner.config.MaxHandshakeTransitions {
			owner.handleFault(ErrTransitionLimit)
			return
		}
		if unsupported := value.RequiredFeatures &^ owner.config.ClientIdent.SupportedFeatures; unsupported != 0 {
			owner.failTerminal(fmt.Errorf("%w: server requires unsupported features %#x", ErrUnsupportedFeature, unsupported))
			return
		}
		if missing := owner.config.ClientIdent.RequiredFeatures &^ value.SupportedFeatures; missing != 0 {
			owner.failTerminal(fmt.Errorf("%w: server lacks required features %#x", ErrUnsupportedFeature, missing))
			return
		}
		if !containsEntityEndpoint(value.Addresses, owner.config.ClientIdent.TargetAddress) {
			owner.handleFault(fmt.Errorf("%w: server ident does not contain target address", ErrMalformed))
			return
		}
		owner.serverCookie = value.Cookie
		owner.serverGlobalID = value.GlobalID
		owner.serverAddresses = cloneEntityAddresses(value.Addresses)
		owner.serverFeatures = value.SupportedFeatures
		owner.serverFlags = value.Flags
		owner.partialReset = false
		owner.reconnectAttempts = 0
		owner.failSentUnknown(ErrSessionDisconnected)
		owner.setState(StateReady)
		owner.completeRenewal()
	case IdentMissingFeatures:
		owner.failTerminal(fmt.Errorf("%w: server requires missing features %#x", ErrUnsupportedPayload, value.Features))
	default:
		owner.handleFault(fmt.Errorf("%w: unexpected session control %T", ErrUnsupportedPayload, payload))
	}
}

func (owner *sessionOwner) completeRenewal() {
	if !owner.renewalInProgress || !owner.credentialChanged {
		return
	}
	owner.renewalInProgress = false
	owner.credentialChanged = false
	owner.emitDiagnostic(DiagnosticCredentialRenewalCompleted)
}

func (owner *sessionOwner) readyControl(name string) bool {
	if owner.state == StateReady {
		return true
	}
	owner.handleFault(fmt.Errorf("%w: %s in state %d", ErrMalformed, name, owner.state))
	return false
}

func (owner *sessionOwner) acceptAcknowledgment(sequence uint64) bool {
	if !owner.sequenceExhausted && sequence >= owner.nextOutbound && sequence != 0 {
		owner.handleFault(fmt.Errorf("%w: acknowledgment %d exceeds highest outbound sequence", ErrMalformed, sequence))
		return false
	}
	return true
}

func cloneEntityAddresses(addresses protocol.EntityAddrVec) protocol.EntityAddrVec {
	cloned := make(protocol.EntityAddrVec, len(addresses))
	for index, address := range addresses {
		cloned[index] = address
		cloned[index].SocketData = append([]byte(nil), address.SocketData...)
	}
	return cloned
}

func (owner *sessionOwner) transitionAllowed(want SessionState) bool {
	if owner.state != want {
		owner.handleFault(fmt.Errorf("%w: transition in state %d", ErrMalformed, owner.state))
		return false
	}
	owner.transitions++
	if owner.transitions > owner.config.MaxHandshakeTransitions {
		owner.handleFault(ErrTransitionLimit)
		return false
	}
	return true
}

func (owner *sessionOwner) handleReset(full bool) {
	owner.invalidateControls()
	owner.emit(SessionEvent{Kind: EventSessionReset, Full: full})
	owner.signalReset()
	owner.serverCookie = 0
	owner.serverFlags = 0
	owner.connectSeq = 0
	owner.lastInbound = 0
	if full {
		if err := owner.refreshClientCookie(); err != nil {
			owner.failTerminal(err)
			return
		}
		owner.nextOutbound = 1
		owner.sequenceExhausted = false
		owner.nextTID = 1
		owner.tidExhausted = false
		owner.failAll(ErrSessionDisconnected)
		owner.replay = nil
		owner.retainedBytes = 0
		owner.partialReset = false
	} else {
		owner.partialReset = true
		owner.prepareReplay()
	}
	owner.setState(StateConnecting)
	owner.queueClientIdent()
}

func (owner *sessionOwner) queueClientIdent() {
	ident := owner.config.ClientIdent
	ident.Cookie = owner.clientCookie
	ident.GlobalSequence = owner.globalSeq
	owner.queueControl(ident)
}

func containsEntityEndpoint(addresses protocol.EntityAddrVec, target protocol.EntityAddr) bool {
	targetEndpoint, ok := target.AddrPort()
	if !ok {
		return false
	}
	for _, address := range addresses {
		if endpoint, ok := address.AddrPort(); ok && endpoint == targetEndpoint {
			return true
		}
	}
	return false
}

func (owner *sessionOwner) refreshClientCookie() error {
	source := owner.config.CookieSource
	if source == nil {
		source = randomCookieSource{}
	}
	cookie, err := source.Cookie()
	if err != nil {
		return fmt.Errorf("%w: generate client cookie: %v", ErrSessionDisconnected, err)
	}
	if cookie == 0 {
		return fmt.Errorf("%w: client cookie must be nonzero", ErrMalformed)
	}
	owner.clientCookie = cookie
	return nil
}

func (owner *sessionOwner) sendReconnect() {
	owner.controlQueue = nil
	owner.queueControl(SessionReconnect{
		Addresses:       owner.config.ClientIdent.Addresses,
		ClientCookie:    owner.clientCookie,
		ServerCookie:    owner.serverCookie,
		GlobalSequence:  owner.globalSeq,
		ConnectSequence: owner.connectSeq,
		MessageSequence: owner.lastInbound,
	})
}

func (owner *sessionOwner) queueControl(payload any) {
	frame, err := EncodeControl(payload, owner.config.Limits)
	if err != nil {
		owner.handleFault(err)
		return
	}
	if len(owner.controlQueue) >= owner.config.MaxQueuedMessages {
		owner.handleFault(ErrQueueSaturated)
		return
	}
	owner.controlQueue = append(owner.controlQueue, frame)
}

func (owner *sessionOwner) trimReplay(sequence uint64) {
	kept := owner.replay[:0]
	for _, pending := range owner.replay {
		if pending.seq > sequence {
			kept = append(kept, pending)
		}
	}
	clear(owner.replay[len(kept):])
	owner.replay = kept
}

func (owner *sessionOwner) prepareReplay() {
	for _, pending := range owner.replay {
		if owner.byRequest[pending.request] != nil {
			pending.sent = false
		}
	}
}

func (owner *sessionOwner) handleFault(err error) {
	if owner.terminalErr != nil || owner.state == StateStopped || owner.state == StateDisconnected && owner.connectPending {
		return
	}
	owner.invalidateControls()
	owner.emit(SessionEvent{Kind: EventTransportFault, Err: err})
	owner.signalReset()
	owner.stopTransportRenewal()
	if owner.transport != nil {
		_ = owner.transport.Close()
		owner.transport = nil
	}
	if owner.writeTasks != nil {
		close(owner.writeTasks)
	}
	owner.writeTasks = nil
	owner.writeBusy = false
	owner.controlQueue = nil
	if owner.serverFlags&ConnectionFlagLossy != 0 {
		owner.failSentUnknown(err)
		owner.resetForNewIdentity()
	} else if owner.config.ReconnectPolicy == FailPending {
		owner.failAll(fmt.Errorf("%w: %v", ErrSessionDisconnected, err))
	} else if owner.serverCookie == 0 {
		owner.failSentUnknown(err)
	} else {
		owner.prepareReplay()
	}
	owner.setState(StateDisconnected)
	owner.beginReconnect()
}

func (owner *sessionOwner) signalReset() {
	select {
	case owner.session.resets <- struct{}{}:
	default:
	}
}

func (owner *sessionOwner) failTerminal(err error) {
	owner.releaseReceiveQueue()
	owner.generation++
	if owner.connectorCancel != nil {
		owner.connectorCancel()
	}
	owner.invalidateControls()
	owner.terminalErr = err
	select {
	case owner.session.terminal <- err:
	default:
	}
	owner.emit(SessionEvent{Kind: EventTransportFault, Err: err})
	owner.stopTransportRenewal()
	if owner.transport != nil {
		_ = owner.transport.Close()
		owner.transport = nil
	}
	if owner.writeTasks != nil {
		close(owner.writeTasks)
	}
	owner.writeTasks = nil
	owner.writeBusy = false
	owner.controlQueue = nil
	owner.connectPending = false
	owner.failAll(err)
	owner.setState(StateDisconnected)
}

func (owner *sessionOwner) beginReconnect() {
	if owner.config.MaxReconnectAttempts > 0 && owner.reconnectAttempts >= owner.config.MaxReconnectAttempts {
		owner.failTerminal(ErrReconnectExhausted)
		return
	}
	delay := owner.nextReconnectBackoff()
	owner.reconnectAttempts++
	owner.connectPending = true
	owner.generation++
	request := connectRequest{generation: owner.generation, ctx: owner.connectorContext, delay: delay}
	owner.connectorRequests <- request
}

func (owner *sessionOwner) nextReconnectBackoff() time.Duration {
	if !owner.connectedOnce && owner.reconnectAttempts == 0 {
		return 0
	}
	steps := owner.reconnectAttempts
	if !owner.connectedOnce {
		steps--
	}
	delay := owner.config.InitialReconnectBackoff
	for steps > 0 && delay < owner.config.MaxReconnectBackoff {
		if delay > owner.config.MaxReconnectBackoff/2 {
			return owner.config.MaxReconnectBackoff
		}
		delay *= 2
		steps--
	}
	return min(delay, owner.config.MaxReconnectBackoff)
}

func (owner *sessionOwner) handleConnected(result connectResult) {
	if result.generation != owner.generation || !owner.connectPending {
		if result.transport != nil {
			_ = result.transport.Close()
		}
		return
	}
	owner.connectPending = false
	if result.err != nil || result.transport == nil {
		if result.err == nil {
			result.err = ErrSessionDisconnected
		}
		owner.emit(SessionEvent{Kind: EventTransportFault, Err: result.err})
		owner.beginReconnect()
		return
	}
	owner.terminalErr = nil
	owner.transitions = 0
	if owner.connectedOnce {
		next, err := nextGlobalSequence(owner.config.GlobalSequenceSource, owner.globalSeq)
		if err != nil {
			_ = result.transport.Close()
			owner.failTerminal(ErrTransitionLimit)
			return
		}
		owner.globalSeq = next
	}
	owner.connectedOnce = true
	observeTransportMode(owner.config, result.transport)
	identityChanged := false
	if authenticated, ok := result.transport.(AuthenticatedTransport); ok {
		globalID := authenticated.AuthenticatedGlobalID()
		if globalID > math.MaxInt64 {
			_ = result.transport.Close()
			owner.failTerminal(ErrMalformed)
			return
		}
		identityChanged = owner.authenticatedGlobalID != 0 && owner.authenticatedGlobalID != globalID
		owner.config.ClientIdent.GlobalID = int64(globalID)
		owner.authenticatedGlobalID = globalID
	}
	owner.credentialChanged = true
	if credentialTransport, ok := result.transport.(CredentialIdentityTransport); ok {
		credentialID := credentialTransport.CredentialIdentity()
		owner.credentialChanged = !owner.credentialKnown || credentialID != owner.credentialID
		owner.credentialID = credentialID
		owner.credentialKnown = true
	}
	if identityChanged {
		owner.resetForNewIdentity()
	}
	if owner.serverCookie != 0 {
		if owner.connectSeq == math.MaxUint64 {
			_ = result.transport.Close()
			owner.failTerminal(ErrTransitionLimit)
			return
		}
		owner.connectSeq++
		owner.startTransport(result.transport, StateReconnecting)
		owner.sendReconnect()
	} else {
		if err := owner.refreshClientCookie(); err != nil {
			_ = result.transport.Close()
			owner.failTerminal(err)
			return
		}
		owner.connectSeq = 0
		owner.startTransport(result.transport, StateConnecting)
		owner.queueClientIdent()
	}
}

func (owner *sessionOwner) resetForNewIdentity() {
	owner.serverCookie = 0
	owner.serverFlags = 0
	owner.connectSeq = 0
	owner.lastInbound = 0
	owner.nextOutbound = 1
	owner.sequenceExhausted = false
	owner.nextTID = 1
	owner.tidExhausted = false
	owner.failAll(ErrSessionDisconnected)
	owner.partialReset = false
}

func (owner *sessionOwner) failSentUnknown(cause error) {
	for _, pending := range append([]*pendingRequest(nil), owner.pending...) {
		if !pending.sent {
			continue
		}
		owner.removePending(pending)
		pending.request.result <- submitResult{err: fmt.Errorf("%w: %w", ErrOutcomeUnknown, cause)}
	}
	owner.replay = nil
}

func (owner *sessionOwner) startTransport(transport Transport, state SessionState) {
	owner.stopTransportRenewal()
	owner.transport = transport
	owner.generation++
	owner.writeTasks = make(chan writeTask)
	owner.transportDone = make(chan struct{})
	generation := owner.generation
	owner.pumpWG.Add(2)
	go owner.readPump(generation, transport)
	go owner.writePump(generation, transport, owner.writeTasks)
	if renewable, ok := transport.(RenewalTransport); ok {
		if due := renewable.RenewalDue(); due != nil {
			owner.pumpWG.Add(1)
			go owner.renewalPump(generation, due, owner.transportDone)
		}
	}
	owner.setState(state)
}

func (owner *sessionOwner) stopTransportRenewal() {
	if owner.transportDone != nil {
		close(owner.transportDone)
		owner.transportDone = nil
	}
}

func (owner *sessionOwner) renewalPump(generation uint64, due, transportDone <-chan struct{}) {
	defer owner.pumpWG.Done()
	select {
	case <-due:
		select {
		case owner.renewals <- renewalDue{generation: generation}:
		case <-transportDone:
		case <-owner.session.done:
		}
	case <-transportDone:
	case <-owner.session.done:
	}
}

func (owner *sessionOwner) emitDiagnostic(kind SessionDiagnosticKind) {
	if owner.config.DiagnosticObserver == nil {
		return
	}
	owner.config.DiagnosticObserver(SessionDiagnostic{
		Kind:       kind,
		Service:    owner.config.DiagnosticService,
		ServiceID:  owner.config.DiagnosticServiceID,
		SessionID:  owner.config.DiagnosticSessionID,
		Generation: owner.generation,
		Timestamp:  owner.config.DiagnosticNow().UTC(),
	})
}

func (owner *sessionOwner) readPump(generation uint64, transport Transport) {
	defer owner.pumpWG.Done()
	for {
		var frame Frame
		var err error
		if owner.config.ReceiveBudget != nil {
			frame, err = ReadTransportFrame(transport, owner.config.ReceiveBudget, owner.config.Limits)
		} else {
			frame, err = transport.ReadFrame()
		}
		if err == nil && owner.config.ReceiveBudget == nil {
			owned, ok := transport.(interface{ OwnsReadFrames() bool })
			if !ok || !owned.OwnsReadFrames() {
				segments := make([]Segment, len(frame.Segments))
				for index, segment := range frame.Segments {
					segments[index] = Segment{Alignment: segment.Alignment, Data: append([]byte(nil), segment.Data...)}
				}
				frame.Segments = segments
			}
		}
		var receivedAt time.Time
		if owner.session.timingActive.Load() > 0 {
			receivedAt = time.Now()
		}
		select {
		case owner.frames <- pumpFrame{generation: generation, frame: frame, receivedAt: receivedAt, err: err}:
		case <-owner.session.done:
			frame.receiveLease.release()
			return
		}
		if err != nil {
			return
		}
	}
}

func (owner *sessionOwner) writePump(generation uint64, transport Transport, tasks <-chan writeTask) {
	defer owner.pumpWG.Done()
	for {
		select {
		case task, ok := <-tasks:
			if !ok {
				return
			}
			if task.request != nil {
				recordRequestTiming(task.request.ctx, "write_begin", task.transactionID)
			}
			err := transport.WriteFrame(task.frame)
			if task.request != nil {
				recordRequestTiming(task.request.ctx, "write_end", task.transactionID)
			}
			select {
			case owner.writes <- pumpWriteResult{generation: generation, taskID: task.id, request: task.request, err: err}:
			case <-owner.session.done:
				return
			}
			if err != nil {
				return
			}
		case <-owner.session.done:
			return
		}
	}
}

func (owner *sessionOwner) reportFault(fault pumpFault) {
	select {
	case owner.faults <- fault:
	case <-owner.session.done:
	}
}

func (owner *sessionOwner) connectorPump(ctx context.Context, connector Connector) {
	defer owner.pumpWG.Done()
	for {
		select {
		case request := <-owner.connectorRequests:
			var transport Transport
			var err error
			if request.delay > 0 {
				err = owner.config.reconnectWait(request.ctx, request.delay)
			}
			if err == nil && connector == nil {
				err = ErrSessionDisconnected
			} else if err == nil {
				transport, err = connector.Connect(request.ctx)
			}
			select {
			case owner.connectorResults <- connectResult{generation: request.generation, transport: transport, err: err}:
			case <-ctx.Done():
				if transport != nil {
					_ = transport.Close()
				}
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func waitReconnectBackoff(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (owner *sessionOwner) stop(done chan struct{}) {
	owner.invalidateControls()
	owner.setState(StateStopped)
	if owner.renewalInProgress {
		owner.emitDiagnostic(DiagnosticSessionClosed)
	}
	owner.failAll(ErrSessionClosed)
	owner.connectorCancel()
	owner.stopTransportRenewal()
	if owner.transport != nil {
		_ = owner.transport.Close()
	}
	close(owner.session.done)
	owner.pumpWG.Wait()
	for {
		select {
		case incoming := <-owner.frames:
			incoming.frame.receiveLease.release()
		default:
			owner.releaseReceiveQueue()
			owner.transport = nil
			close(owner.session.events)
			close(owner.session.incoming)
			owner.config.ReceiveBudget.retire()
			if owner.session.stopped != nil {
				close(owner.session.stopped)
			}
			close(done)
			return
		}
	}
}

func receiveMessageBytes(message Message) uint64 {
	if message.receiveLease != nil {
		return message.receiveLease.bytes
	}
	return uint64(cap(message.Front)) + uint64(cap(message.Middle)) + uint64(cap(message.Data))
}

func (owner *sessionOwner) releaseReceiveQueue() {
	for index := range owner.receiveQueue {
		owner.receiveQueue[index].receiveLease.release()
		owner.receiveQueue[index] = Message{}
	}
	owner.receiveQueue = nil
	owner.receiveBytes = 0
}

func (owner *sessionOwner) failAll(err error) {
	for _, pending := range owner.pending {
		resultErr := err
		if pending.mayHaveExecuted && !errors.Is(err, ErrOutcomeUnknown) {
			resultErr = fmt.Errorf("%w: %w", ErrOutcomeUnknown, err)
		}
		pending.request.result <- submitResult{err: resultErr}
	}
	owner.pending = nil
	owner.replay = nil
	clear(owner.byRequest)
	clear(owner.byTID)
	owner.retainedBytes = 0
	owner.controlCount = 0
	owner.controlBytes = 0
}

func (owner *sessionOwner) removePending(target *pendingRequest) {
	for index, pending := range owner.pending {
		if pending == target {
			copy(owner.pending[index:], owner.pending[index+1:])
			owner.pending[len(owner.pending)-1] = nil
			owner.pending = owner.pending[:len(owner.pending)-1]
			break
		}
	}
	for index, pending := range owner.replay {
		if pending == target {
			copy(owner.replay[index:], owner.replay[index+1:])
			owner.replay[len(owner.replay)-1] = nil
			owner.replay = owner.replay[:len(owner.replay)-1]
			break
		}
	}
	delete(owner.byRequest, target.request)
	delete(owner.byTID, target.message.Header.TransactionID)
	if target.request.control {
		owner.controlCount--
		owner.controlBytes -= target.bytes
	} else {
		owner.retainedBytes -= target.bytes
	}
}

func (owner *sessionOwner) inFlightCount() int {
	count := 0
	for _, pending := range owner.pending {
		if pending.sent && !pending.request.control {
			count++
		}
	}
	return count
}

func (owner *sessionOwner) allocateSequence() (uint64, error) {
	if owner.sequenceExhausted {
		return 0, ErrTransitionLimit
	}
	sequence := owner.nextOutbound
	if sequence == 0 {
		sequence = 1
	}
	if sequence == math.MaxUint64 {
		owner.sequenceExhausted = true
		owner.nextOutbound = 0
	} else {
		owner.nextOutbound = sequence + 1
	}
	return sequence, nil
}

func (owner *sessionOwner) takeSequence() uint64 {
	sequence, _ := owner.allocateSequence()
	return sequence
}

func (owner *sessionOwner) takeTID() (uint64, error) {
	for !owner.tidExhausted {
		transactionID := owner.nextTID
		if transactionID == 0 {
			transactionID = 1
		}
		if transactionID == math.MaxUint64 {
			owner.tidExhausted = true
			owner.nextTID = 0
		} else {
			owner.nextTID = transactionID + 1
		}
		if _, exists := owner.byTID[transactionID]; !exists {
			return transactionID, nil
		}
	}
	return 0, ErrTransitionLimit
}

func (owner *sessionOwner) setState(state SessionState) {
	if owner.state == state {
		return
	}
	owner.state = state
	owner.emit(SessionEvent{Kind: EventStateChanged, State: state})
}

func (owner *sessionOwner) emit(event SessionEvent) {
	event.State = owner.state
	if owner.droppedEvents > owner.reportedDrops {
		overflow := SessionEvent{Kind: EventOverflow, State: owner.state, DroppedEvents: owner.droppedEvents}
		select {
		case owner.session.events <- overflow:
			owner.reportedDrops = owner.droppedEvents
		default:
		}
	}
	select {
	case owner.session.events <- event:
	default:
		owner.droppedEvents++
	}
}

func (owner *sessionOwner) snapshot() SessionSnapshot {
	queued := 0
	for _, pending := range owner.pending {
		if !pending.sent && !pending.request.control {
			queued++
		}
	}
	return SessionSnapshot{
		State:                 owner.state,
		AuthenticatedGlobalID: owner.authenticatedGlobalID,
		ServerGlobalID:        owner.serverGlobalID,
		ServerAddresses:       cloneEntityAddresses(owner.serverAddresses),
		ServerFeatures:        owner.serverFeatures,
		ServerFlags:           owner.serverFlags,
		NextOutboundSequence:  owner.nextOutbound,
		LastInboundSequence:   owner.lastInbound,
		NextTransactionID:     owner.nextTID,
		ClientCookie:          owner.clientCookie,
		ServerCookie:          owner.serverCookie,
		GlobalSequence:        owner.globalSeq,
		ConnectSequence:       owner.connectSeq,
		Queued:                queued,
		InFlight:              owner.inFlightCount(),
		Replay:                len(owner.replay),
		RetainedBytes:         owner.retainedBytes,
		QueuedReceiveMessages: len(owner.receiveQueue),
		RetainedReceiveBytes:  owner.receiveBytes,
		ControlQueued:         owner.controlCount,
		ControlRetainedBytes:  owner.controlBytes,
		ReconnectAttempts:     owner.reconnectAttempts,
		HandshakeTransitions:  owner.transitions,
		DroppedEvents:         owner.droppedEvents,
	}
}

func retainedMessageBytes(message Message) uint64 {
	return MessageHeaderSize + uint64(len(message.Front)) + uint64(len(message.Middle)) + uint64(len(message.Data))
}

func admissionMessageBytes(message Message, limits Limits) (uint64, error) {
	frontLength, err := checkedUint32Length(len(message.Front))
	if err != nil {
		return 0, err
	}
	middleLength, err := checkedUint32Length(len(message.Middle))
	if err != nil {
		return 0, err
	}
	dataLength, err := checkedUint32Length(len(message.Data))
	if err != nil {
		return 0, err
	}
	actual := MessageLengths{Front: frontLength, Middle: middleLength, Data: dataLength}
	if message.Lengths != actual {
		return 0, fmt.Errorf("%w: message lengths %+v do not match payloads %+v", ErrMalformed, message.Lengths, actual)
	}
	if message.Header.DataPrePaddingLength > dataLength {
		return 0, fmt.Errorf("%w: data pre-padding %d exceeds data length %d", ErrMalformed, message.Header.DataPrePaddingLength, dataLength)
	}
	var header [MessageHeaderSize]byte
	segments := []Segment{
		{Alignment: messageAlignments[0], Data: header[:]},
		{Alignment: messageAlignments[1], Data: message.Front},
		{Alignment: messageAlignments[2], Data: message.Middle},
		{Alignment: messageAlignments[3], Data: message.Data},
	}
	if err := validateMessageSegments(segments, limits); err != nil {
		return 0, err
	}
	return retainedMessageBytes(message), nil
}

func cloneMessage(message Message) Message {
	message.receiveLease = nil
	message.Front = append([]byte(nil), message.Front...)
	message.Middle = append([]byte(nil), message.Middle...)
	message.Data = append([]byte(nil), message.Data...)
	return message
}

func containsPending(items []*pendingRequest, target *pendingRequest) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
}
