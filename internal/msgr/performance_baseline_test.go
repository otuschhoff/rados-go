package msgr

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"runtime"
	"sync"
	"testing"
	"time"
)

var performanceLimits = Limits{MaxSegmentBytes: 8 << 20, MaxFrameBytes: 16 << 20, MaxAddresses: 4, MaxAuthBytes: 64}

func performanceConfig(depth int) SessionConfig {
	return SessionConfig{
		Limits: performanceLimits, MaxQueuedMessages: depth,
		MaxRetainedBytes: uint64(depth) * (8 << 20), MaxInFlightTransactions: depth,
		MaxReconnectAttempts: 2, MaxHandshakeTransitions: 8, EventBuffer: 64,
		ReconnectPolicy: ReplayPending, ClientCookie: 11, ServerCookie: 22,
		GlobalSequenceSource: &atomicGlobalSequenceSource{},
		reconnectWait:        func(context.Context, time.Duration) error { return nil },
	}
}

type performanceQueue struct {
	owner    *sessionOwner
	requests []*submitCommand
	storage  []*pendingRequest
	replay   []*pendingRequest
}

func newPerformanceQueue(depth int) *performanceQueue {
	config := performanceConfig(depth)
	fixture := &performanceQueue{
		owner: &sessionOwner{
			session: &Session{events: make(chan SessionEvent, 64), incoming: make(chan Message, depth)},
			config:  config, state: StateReady, nextOutbound: 1, nextTID: 1,
			byRequest: make(map[*submitCommand]*pendingRequest, depth), byTID: make(map[uint64]*pendingRequest, depth),
			pending: make([]*pendingRequest, 0, depth), replay: make([]*pendingRequest, 0, depth),
		},
		requests: make([]*submitCommand, depth),
	}
	for index := range fixture.requests {
		fixture.requests[index] = &submitCommand{ctx: context.Background(), result: make(chan submitResult, 1)}
	}
	return fixture
}

func (fixture *performanceQueue) fill() {
	owner := fixture.owner
	for _, request := range fixture.requests {
		request.message = Message{Front: []byte{1}, Lengths: MessageLengths{Front: 1}}
		owner.submit(request)
		pending := owner.byRequest[request]
		pending.seq = owner.takeSequence()
		pending.message.Header.Sequence = pending.seq
		markSessionPendingSent(owner, pending)
		pending.mayHaveExecuted = true
		owner.replay = append(owner.replay, pending)
	}
	fixture.storage = owner.pending[:cap(owner.pending)]
	fixture.replay = owner.replay[:cap(owner.replay)]
}

func (fixture *performanceQueue) batch(tb testing.TB, drain bool) {
	fixture.fill()
	fixture.finish(tb, drain)
}

func (fixture *performanceQueue) finish(tb testing.TB, drain bool) {
	owner := fixture.owner
	if drain {
		owner.failAll(ErrSessionClosed)
	} else {
		for _, request := range fixture.requests {
			pending := owner.byRequest[request]
			owner.handleMessage(Message{Header: MessageHeader{
				Sequence: owner.lastInbound + 1, TransactionID: pending.message.Header.TransactionID,
			}})
		}
	}
	for _, request := range fixture.requests {
		result := <-request.result
		if drain && result.err == nil || !drain && result.err != nil {
			tb.Fatalf("queue completion drain=%t err=%v", drain, result.err)
		}
	}
	owner.controlQueue = nil
	if drain {
		fixture.storage, fixture.replay = nil, nil
	}
}

func BenchmarkPerformanceQueue(benchmark *testing.B) {
	for _, depth := range []int{1, 64, 1024, 4096} {
		for _, drain := range []bool{false, true} {
			operation := "complete"
			if drain {
				operation = "drain"
			}
			benchmark.Run(fmt.Sprintf("depth=%d/%s", depth, operation), func(benchmark *testing.B) {
				fixture := newPerformanceQueue(depth)
				benchmark.Log("One op is a full depth-sized batch: owner admission/refill, synthetic sent/replay registration, real reply completion or failAll, and result-channel drain. No Session pumps, dispatch, wire encoding, network, or authentication; refill remains timed.")
				benchmark.ReportAllocs()
				for benchmark.Loop() {
					fixture.batch(benchmark, drain)
				}
				benchmark.ReportMetric(float64(depth), "depth")
				benchmark.ReportMetric(float64(depth), "requests/op")
			})
		}
	}
}

func performanceCodecs(tb testing.TB, mode string) (Codec, Codec) {
	tb.Helper()
	if mode == "crc" {
		return CRCCodec{WithDataCRC: true}, CRCCodec{WithDataCRC: true}
	}
	client := performanceClientCodec(tb, mode)
	peer, err := NewSecureCodec(testSecureSecret(), true)
	if err != nil {
		tb.Fatal(err)
	}
	return client, peer
}

func performanceClientCodec(tb testing.TB, mode string) Codec {
	tb.Helper()
	if mode == "crc" {
		return CRCCodec{WithDataCRC: true}
	}
	codec, err := NewSecureCodec(testSecureSecret(), false)
	if err != nil {
		tb.Fatal(err)
	}
	return codec
}

type performanceWireTransport struct {
	codec  Codec
	reads  chan fakeRead
	wires  chan []byte
	closed chan struct{}
	once   sync.Once
}

func newPerformanceWireTransport(codec Codec) *performanceWireTransport {
	return &performanceWireTransport{codec: codec, reads: make(chan fakeRead, 1), wires: make(chan []byte, 1), closed: make(chan struct{})}
}

func (transport *performanceWireTransport) ReadFrame() (Frame, error) {
	select {
	case read := <-transport.reads:
		return read.frame, read.err
	case <-transport.closed:
		return Frame{}, ErrSessionClosed
	}
}

func (*performanceWireTransport) OwnsReadFrames() bool { return true }

func (transport *performanceWireTransport) WriteFrame(frame Frame) error {
	wire, err := transport.codec.Encode(frame, performanceLimits)
	if err != nil {
		return err
	}
	select {
	case transport.wires <- wire:
		return nil
	case <-transport.closed:
		return ErrSessionClosed
	}
}

func (transport *performanceWireTransport) Close() error {
	transport.once.Do(func() { close(transport.closed) })
	return nil
}

func performanceReceive(tb testing.TB, ctx context.Context, transport *performanceWireTransport, peer Codec) Frame {
	tb.Helper()
	select {
	case wire := <-transport.wires:
		frame, err := peer.Read(bytes.NewReader(wire), performanceLimits)
		if err != nil {
			tb.Fatal(err)
		}
		return frame
	case <-ctx.Done():
		tb.Fatal("timed out awaiting encoded send:", ctx.Err())
		return Frame{}
	}
}

func performanceInject(tb testing.TB, transport *performanceWireTransport, payload any) {
	tb.Helper()
	frame, err := EncodeControl(payload, performanceLimits)
	if err != nil {
		tb.Fatal(err)
	}
	transport.reads <- fakeRead{frame: frame}
}

func performanceSubmissionBatch(tb testing.TB, mode string, payload []byte, replay bool, mutate bool) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clientCodec, peer := performanceCodecs(tb, mode)
	transport := newPerformanceWireTransport(clientCodec)
	defer transport.Close()
	var next *performanceWireTransport
	var nextPeer Codec
	var connector Connector
	if replay {
		nextCodec, receiver := performanceCodecs(tb, mode)
		nextPeer = receiver
		next = newPerformanceWireTransport(nextCodec)
		defer next.Close()
		connector = ConnectorFunc(func(context.Context) (Transport, error) { return next, nil })
	}
	session, err := NewSession(transport, connector, performanceConfig(8))
	if err != nil {
		tb.Fatal(err)
	}
	defer session.Stop()
	result := make(chan submitResult, 1)
	go func() {
		message, err := session.Submit(ctx, Message{Data: payload, Lengths: MessageLengths{Data: uint32(len(payload))}})
		result <- submitResult{message: message, err: err}
	}()
	frame := performanceReceive(tb, ctx, transport, peer)
	message, err := decodeOwnedMessage(frame, performanceLimits)
	if err != nil || message.Header.Sequence != 1 || message.Header.TransactionID == 0 || !bytes.Equal(message.Data, payload) {
		tb.Fatalf("initial send: header=%+v err=%v payload bytes=%d", message.Header, err, len(message.Data))
	}
	original := payload[0]
	if mutate {
		payload[0] ^= 0xff
		defer func() { payload[0] = original }()
	}
	if replay {
		transport.reads <- fakeRead{err: io.EOF}
		reconnectFrame := performanceReceive(tb, ctx, next, nextPeer)
		control, err := DecodeControl(reconnectFrame, performanceLimits)
		if reconnect, ok := control.(SessionReconnect); err != nil || !ok || reconnect.ClientCookie != 11 || reconnect.ServerCookie != 22 {
			tb.Fatalf("reconnect send = %#v, err=%v", control, err)
		}
		performanceInject(tb, next, SessionReconnectOK{MessageSequence: 0})
		replayedFrame := performanceReceive(tb, ctx, next, nextPeer)
		replayed, err := decodeOwnedMessage(replayedFrame, performanceLimits)
		if err != nil || replayed.Header != message.Header || !bytes.Equal(replayed.Data, message.Data) || replayed.Data[0] != original {
			tb.Fatalf("replay changed admitted request: err=%v header=%+v", err, replayed.Header)
		}
		transport, peer = next, nextPeer
	}
	response := Message{Header: MessageHeader{Sequence: 1, TransactionID: message.Header.TransactionID, AckSequence: 1}, Front: []byte("ok"), Lengths: MessageLengths{Front: 2}}
	responseFrame, err := EncodeMessage(response, performanceLimits)
	if err != nil {
		tb.Fatal(err)
	}
	transport.reads <- fakeRead{frame: responseFrame}
	select {
	case outcome := <-result:
		if outcome.err != nil || string(outcome.message.Front) != "ok" {
			tb.Fatalf("submit completion = %+v", outcome)
		}
	case <-ctx.Done():
		tb.Fatal("timed out awaiting Submit completion")
	}
	ackFrame := performanceReceive(tb, ctx, transport, peer)
	control, err := DecodeControl(ackFrame, performanceLimits)
	if ack, ok := control.(Ack); err != nil || !ok || ack.Sequence != 1 {
		tb.Fatalf("completion ack = %#v, err=%v", control, err)
	}
	snapshot, err := session.Snapshot(ctx)
	if err != nil || snapshot.Queued != 0 || snapshot.InFlight != 0 || snapshot.Replay != 0 || snapshot.RetainedBytes != 0 {
		tb.Fatalf("completed session retained work: %+v, err=%v", snapshot, err)
	}
	select {
	case <-transport.wires:
		tb.Fatal("unexpected extra send after completion")
	default:
	}
}

func BenchmarkPerformanceSubmissionReplay(benchmark *testing.B) {
	for _, mode := range []string{"crc", "secure"} {
		for _, size := range []int{4 << 10, 64 << 10, 4 << 20} {
			for _, replay := range []bool{false, true} {
				path := "initial"
				sends := 1
				if replay {
					path, sends = "initial+replay", 2
				}
				benchmark.Run(fmt.Sprintf("%s/bytes=%d/%s", mode, size, path), func(benchmark *testing.B) {
					payload := bytes.Repeat([]byte{0x5a}, size)
					benchmark.Log("One op is a complete request lifecycle. Timed: codec/session/fixture creation, actual Session.Submit admission copy, pumps, EncodeMessage, real CRC/AEAD wire encode, peer decode/send validation, optional channel-forced fault + reconnect control + replay, small injected reply, completion ACK, snapshot, Stop. Excluded: connection establishment, authentication, sockets, inbound wire decode, OSD processing, backoff delays. MB/s counts outbound application payload across message sends, not control/wire bytes.")
					benchmark.SetBytes(int64(size * sends))
					benchmark.ReportAllocs()
					for benchmark.Loop() {
						performanceSubmissionBatch(benchmark, mode, payload, replay, false)
					}
					benchmark.ReportMetric(float64(sends), "message-sends/op")
					benchmark.ReportMetric(1, "requests/op")
				})
			}
		}
	}
}

type performanceIdleConn struct {
	recordingConn
	entered   chan struct{}
	closed    chan struct{}
	readOnce  sync.Once
	closeOnce sync.Once
}

func (connection *performanceIdleConn) Read([]byte) (int, error) {
	connection.readOnce.Do(func() { close(connection.entered) })
	<-connection.closed
	return 0, io.EOF
}

func (connection *performanceIdleConn) Close() error {
	connection.closeOnce.Do(func() { close(connection.closed) })
	return nil
}

type performanceIdleSet struct {
	sessions   []*Session
	transports []*connTransport
}

func newPerformanceIdleSet(tb testing.TB, mode string, count int) *performanceIdleSet {
	tb.Helper()
	fixture := &performanceIdleSet{sessions: make([]*Session, 0, count), transports: make([]*connTransport, 0, count)}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for index := 0; index < count; index++ {
		codec := performanceClientCodec(tb, mode)
		connection := &performanceIdleConn{entered: make(chan struct{}), closed: make(chan struct{})}
		transport, err := NewConnTransport(connection, codec, performanceLimits)
		if err != nil {
			fixture.stop()
			tb.Fatal(err)
		}
		session, err := NewSession(transport, nil, performanceConfig(64))
		if err != nil {
			transport.Close()
			fixture.stop()
			tb.Fatal(err)
		}
		fixture.sessions = append(fixture.sessions, session)
		fixture.transports = append(fixture.transports, transport.(*connTransport))
		select {
		case <-connection.entered:
		case <-ctx.Done():
			fixture.stop()
			tb.Fatal("idle read pump did not start")
		}
		snapshot, err := session.Snapshot(ctx)
		if err != nil || snapshot.State != StateReady || snapshot.RetainedBytes != 0 {
			fixture.stop()
			tb.Fatalf("idle session = %+v err=%v", snapshot, err)
		}
	}
	return fixture
}

func (fixture *performanceIdleSet) stop() {
	for _, session := range fixture.sessions {
		session.Stop()
	}
}

func performanceIdleHeap(tb testing.TB, mode string, count int) int64 {
	tb.Helper()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	fixture := newPerformanceIdleSet(tb, mode, count)
	runtime.GC()
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(fixture)
	fixture.stop()
	return int64(after.HeapAlloc) - int64(before.HeapAlloc)
}

func BenchmarkPerformanceIdleConnections(benchmark *testing.B) {
	for _, mode := range []string{"crc", "secure"} {
		for _, count := range []int{1, 16, 128} {
			benchmark.Run(fmt.Sprintf("%s/connections=%d", mode, count), func(benchmark *testing.B) {
				benchmark.Log("One op constructs, readies, snapshots, and stops a full idle connection batch: built-in 512KiB reader, codec and Session config(queue/in-flight=64,event-buffer=64), pumps plus channel-backed net.Conn. No sockets/connect/auth. B/op measures lifecycle allocations including fixture/synchronization. retained-heap-B/batch is a separate post-GC live HeapAlloc delta with fixtures kept alive; includes fixture/runtime heap noise, no harness subtraction; excludes stack-in-use and OS/RSS.")
				retained := performanceIdleHeap(benchmark, mode, count)
				benchmark.ReportAllocs()
				for benchmark.Loop() {
					fixture := newPerformanceIdleSet(benchmark, mode, count)
					fixture.stop()
				}
				benchmark.ReportMetric(float64(count), "connections/op")
				benchmark.ReportMetric(float64(count*(512<<10)), "reader-B/batch")
				benchmark.ReportMetric(float64(retained), "retained-heap-B/batch")
			})
		}
	}
}

func TestPerformanceQueueFixture(t *testing.T) {
	for _, depth := range []int{1, 64, 1024, 4096} {
		for _, drain := range []bool{false, true} {
			t.Run(fmt.Sprintf("depth=%d/drain=%t", depth, drain), func(t *testing.T) {
				fixture := newPerformanceQueue(depth)
				for iteration := 0; iteration < 2; iteration++ {
					owner := fixture.owner
					fixture.fill()
					if len(owner.pending) != depth || len(owner.replay) != depth || owner.inFlightCount() != depth || len(owner.byRequest) != depth || len(owner.byTID) != depth || owner.retainedBytes != uint64(depth)*(MessageHeaderSize+1) {
						t.Fatalf("fixture did not populate actual pending/replay indexes at depth=%d: %+v", depth, owner.snapshot())
					}
					fixture.finish(t, drain)
					if len(owner.pending) != 0 || len(owner.replay) != 0 || len(owner.byRequest) != 0 || len(owner.byTID) != 0 || owner.retainedBytes != 0 {
						t.Fatal("queue fixture did not drain bookkeeping")
					}
					if !drain {
						for index := range fixture.storage {
							if fixture.storage[index] != nil || fixture.replay[index] != nil {
								t.Fatal("completion retained backing references")
							}
						}
					}
				}
			})
		}
	}
}

func TestPerformanceSubmissionReplayFixture(t *testing.T) {
	for _, mode := range []string{"crc", "secure"} {
		for _, size := range []int{4 << 10, 64 << 10, 4 << 20} {
			for _, replay := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/bytes=%d/replay=%t", mode, size, replay), func(t *testing.T) {
					performanceSubmissionBatch(t, mode, bytes.Repeat([]byte{0x5a}, size), replay, true)
				})
			}
		}
	}
}

func TestPerformanceIdleConnectionsFixture(t *testing.T) {
	for _, mode := range []string{"crc", "secure"} {
		for _, count := range []int{1, 16, 128} {
			t.Run(fmt.Sprintf("%s/count=%d", mode, count), func(t *testing.T) {
				fixture := newPerformanceIdleSet(t, mode, count)
				defer fixture.stop()
				for _, transport := range fixture.transports {
					reader, ok := transport.reader.(*bufio.Reader)
					if !ok || reader.Size() != 512<<10 || !transport.OwnsReadFrames() {
						t.Fatal("idle fixture omitted built-in owned buffered reader")
					}
				}
				fixture.stop()
				for _, session := range fixture.sessions {
					select {
					case <-session.Done():
					default:
						t.Fatal("idle session did not stop")
					}
				}
			})
		}
	}
}
