package msgr

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const phase4StalledPayload = 4 << 20
const phase4StalledDepth = 8

type phase4StalledMemory struct {
	HeapAlloc  uint64 `json:"heap_alloc_bytes"`
	HeapInuse  uint64 `json:"heap_inuse_bytes"`
	StackInuse uint64 `json:"stack_inuse_bytes"`
	RSS        uint64 `json:"rss_bytes"`
	Goroutines int    `json:"goroutines"`
}

type phase4StalledState struct {
	Name                  string              `json:"state"`
	Memory                phase4StalledMemory `json:"memory"`
	HeapDelta             int64               `json:"heap_alloc_delta_bytes"`
	RSSDelta              int64               `json:"rss_delta_bytes"`
	GoroutineDelta        int                 `json:"goroutine_delta"`
	QueueLength           int                 `json:"incoming_length"`
	LastInbound           uint64              `json:"last_inbound_sequence"`
	SnapshotRetained      uint64              `json:"snapshot_retained_bytes"`
	QueuePayload          uint64              `json:"queue_payload_logical_bytes"`
	QueueCapacity         uint64              `json:"queue_payload_capacity_bytes"`
	ReadAheadFrames       int                 `json:"read_ahead_frames"`
	ReadAheadPayload      uint64              `json:"read_ahead_payload_logical_bytes"`
	ReadAheadCapacity     uint64              `json:"read_ahead_segment_capacity_bytes"`
	ActiveFrames          int                 `json:"active_frames"`
	ActivePayload         uint64              `json:"active_payload_logical_bytes"`
	ActiveCapacity        uint64              `json:"active_segment_capacity_bytes"`
	AppMessages           int                 `json:"application_retained_messages"`
	AppPayload            uint64              `json:"application_payload_logical_bytes"`
	AppCapacity           uint64              `json:"application_payload_capacity_bytes"`
	ReaderStorage         uint64              `json:"reader_storage_bytes"`
	OutputFrameCapacity   uint64              `json:"returned_frame_segment_capacity_bytes"`
	OutputMessageCapacity uint64              `json:"returned_message_payload_capacity_bytes"`
	Error                 string              `json:"error,omitempty"`
}

type phase4StalledReport struct {
	Schema           int                  `json:"schema_version"`
	GoVersion        string               `json:"go_version"`
	GOOS             string               `json:"goos"`
	GOARCH           string               `json:"goarch"`
	QueueDepth       int                  `json:"queue_depth"`
	PayloadBytes     int                  `json:"payload_bytes"`
	MaxRetainedBytes uint64               `json:"configured_max_retained_bytes"`
	Scope            string               `json:"scope"`
	QueueProgress    []phase4StalledState `json:"queue_progress"`
	States           []phase4StalledState `json:"states"`
	DecodeStates     []phase4StalledState `json:"decode_states"`
}

func phase4StalledCapture(t *testing.T, name string, baseline phase4StalledMemory) phase4StalledState {
	t.Helper()
	runtime.GC()
	runtime.GC()
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	output, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(os.Getpid())).Output()
	if err != nil {
		t.Fatal(err)
	}
	rss, err := strconv.ParseUint(strings.TrimSpace(string(output)), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	state := phase4StalledState{Name: name, Memory: phase4StalledMemory{
		HeapAlloc: memory.HeapAlloc, HeapInuse: memory.HeapInuse, StackInuse: memory.StackInuse,
		RSS: rss * 1024, Goroutines: runtime.NumGoroutine(),
	}}
	state.HeapDelta = int64(state.Memory.HeapAlloc) - int64(baseline.HeapAlloc)
	state.RSSDelta = int64(state.Memory.RSS) - int64(baseline.RSS)
	state.GoroutineDelta = state.Memory.Goroutines - baseline.Goroutines
	return state
}

func phase4StalledFrame(t *testing.T, sequence uint64) Frame {
	t.Helper()
	message := Message{Header: MessageHeader{Sequence: sequence},
		Data: make([]byte, phase4StalledPayload), Lengths: MessageLengths{Data: phase4StalledPayload}}
	for index := range message.Data {
		message.Data[index] = byte(sequence)
	}
	frame, err := EncodeMessage(message, performanceLimits)
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

func phase4StalledFrameCapacity(frame Frame) uint64 {
	var capacity uint64
	for _, segment := range frame.Segments {
		capacity += uint64(cap(segment.Data))
	}
	return capacity
}

type phase4StalledTransport struct {
	*performanceWireTransport
	entered        chan int
	active         chan uint64
	readsCompleted int
}

func (transport *phase4StalledTransport) ReadFrame() (Frame, error) {
	transport.entered <- transport.readsCompleted + 1
	frame, err := transport.performanceWireTransport.ReadFrame()
	transport.readsCompleted++
	if err == nil && transport.readsCompleted == phase4StalledDepth+2 {
		transport.active <- phase4StalledFrameCapacity(frame)
		<-transport.closed
		runtime.KeepAlive(frame)
		return Frame{}, ErrSessionClosed
	}
	return frame, err
}

func phase4StalledWait[T any](t *testing.T, ctx context.Context, channel <-chan T) T {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-ctx.Done():
		t.Fatal("stalled measurement timed out:", ctx.Err())
	}
	var zero T
	return zero
}

func phase4StalledStop(t *testing.T, session *Session) {
	t.Helper()
	done := make(chan struct{})
	go func() { session.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Error("Session.Stop exceeded cleanup deadline")
	}
}

func phase4StalledInject(t *testing.T, ctx context.Context, transport *phase4StalledTransport, sequence uint64) {
	t.Helper()
	frame := phase4StalledFrame(t, sequence)
	select {
	case transport.reads <- fakeRead{frame: frame}:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case <-transport.closed:
		t.Fatal("transport closed before injection")
	}
}

func phase4StalledQueue(t *testing.T, report *phase4StalledReport) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	baseline := phase4StalledCapture(t, "baseline", phase4StalledMemory{})
	baseline.HeapDelta, baseline.RSSDelta, baseline.GoroutineDelta = 0, 0, 0
	report.States = append(report.States, baseline)
	transport := &phase4StalledTransport{performanceWireTransport: newPerformanceWireTransport(CRCCodec{WithDataCRC: true}),
		entered: make(chan int, 32), active: make(chan uint64, 1)}
	config := performanceConfig(phase4StalledDepth)
	config.MaxRetainedBytes = phase4StalledPayload
	session, err := NewSession(transport, nil, config)
	if err != nil {
		t.Fatal(err)
	}
	blockedResult := make(chan SessionSnapshot)
	blocked := false
	var drains sync.WaitGroup
	drains.Add(1)
	go func() {
		defer drains.Done()
		for {
			select {
			case <-transport.wires:
			case <-transport.closed:
				return
			}
		}
	}()
	defer func() {
		if blocked {
			select {
			case <-blockedResult:
			case <-time.After(5 * time.Second):
				t.Error("owner gate did not release")
			}
		}
		_ = transport.Close()
		phase4StalledStop(t, session)
		drains.Wait()
	}()
	var snapshot SessionSnapshot
	for sequence := uint64(1); sequence <= phase4StalledDepth; sequence++ {
		phase4StalledInject(t, ctx, transport, sequence)
		for {
			snapshot, err = session.Snapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if snapshot.LastInboundSequence == sequence {
				break
			}
		}
		if len(session.incoming) != int(sequence) || snapshot.RetainedBytes != 0 {
			t.Fatalf("queue admission sequence=%d length=%d snapshot=%+v", sequence, len(session.incoming), snapshot)
		}
		state := phase4StalledCapture(t, "queue_filled", baseline.Memory)
		state.QueueLength, state.LastInbound = len(session.incoming), snapshot.LastInboundSequence
		state.QueuePayload = sequence * phase4StalledPayload
		state.QueueCapacity = state.QueuePayload
		report.QueueProgress = append(report.QueueProgress, state)
		runtime.KeepAlive(session)
	}
	filled := report.QueueProgress[len(report.QueueProgress)-1]
	filled.Name = "filled"
	report.States = append(report.States, filled)
	select {
	case session.commands <- snapshotCommand{result: blockedResult}:
		blocked = true
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	phase4StalledInject(t, ctx, transport, 9)
	for phase4StalledWait(t, ctx, transport.entered) != 10 {
	}
	phase4StalledInject(t, ctx, transport, 10)
	activeCapacity := phase4StalledWait(t, ctx, transport.active)
	state := phase4StalledCapture(t, "owner_blocked_read_ahead_and_active_frame", baseline.Memory)
	state.QueueLength, state.LastInbound = len(session.incoming), snapshot.LastInboundSequence
	state.QueuePayload, state.QueueCapacity = filled.QueuePayload, filled.QueueCapacity
	state.ReadAheadFrames, state.ActiveFrames = 1, 1
	state.ReadAheadPayload, state.ActivePayload = phase4StalledPayload, phase4StalledPayload
	state.ReadAheadCapacity, state.ActiveCapacity = activeCapacity, activeCapacity
	report.States = append(report.States, state)
	runtime.KeepAlive(session)
	runtime.KeepAlive(transport)
	_ = phase4StalledWait(t, ctx, blockedResult)
	blocked = false
	terminal := phase4StalledWait(t, ctx, session.Terminal())
	if !errors.Is(terminal, ErrQueueSaturated) {
		t.Fatalf("ninth frame terminal error: %v", terminal)
	}
	for {
		snapshot, err = session.Snapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.State == StateDisconnected && snapshot.LastInboundSequence == 9 {
			break
		}
	}
	state = phase4StalledCapture(t, "terminal_retained", baseline.Memory)
	state.QueueLength, state.LastInbound = len(session.incoming), snapshot.LastInboundSequence
	state.QueuePayload, state.QueueCapacity, state.Error = filled.QueuePayload, filled.QueueCapacity, terminal.Error()
	report.States = append(report.States, state)
	runtime.KeepAlive(session)
	phase4StalledStop(t, session)
	drains.Wait()
	state = phase4StalledCapture(t, "post_stop_queued", baseline.Memory)
	state.QueueLength = len(session.incoming)
	state.QueuePayload, state.QueueCapacity = filled.QueuePayload, filled.QueueCapacity
	if state.QueueLength != phase4StalledDepth {
		t.Fatalf("Stop changed buffered queue length: %d", state.QueueLength)
	}
	report.States = append(report.States, state)
	runtime.KeepAlive(session)
	retained := phase4StalledDrain(t, session)
	state = phase4StalledCapture(t, "drained_application_retained", baseline.Memory)
	state.AppMessages = len(retained)
	for _, message := range retained {
		state.AppPayload += uint64(len(message.Front) + len(message.Middle) + len(message.Data))
		state.AppCapacity += uint64(cap(message.Front) + cap(message.Middle) + cap(message.Data))
	}
	if state.AppMessages != phase4StalledDepth || state.AppPayload != filled.QueuePayload || state.AppCapacity != filled.QueueCapacity {
		t.Fatalf("application retention: %+v", state)
	}
	report.States = append(report.States, state)
	runtime.KeepAlive(retained)
	runtime.KeepAlive(session)
	clear(retained)
	retained = nil
	state = phase4StalledCapture(t, "post_release_session_still_held", baseline.Memory)
	state.QueueLength = len(session.incoming)
	if state.HeapDelta > 4<<20 {
		t.Fatalf("released heap did not return near baseline: %+v", state)
	}
	report.States = append(report.States, state)
	runtime.KeepAlive(session)
	runtime.KeepAlive(transport)
}

func phase4StalledDrain(t *testing.T, session *Session) []Message {
	t.Helper()
	var messages []Message
	for message := range session.Incoming() {
		if message.Header.Sequence != uint64(len(messages)+1) || len(message.Data) != phase4StalledPayload ||
			message.Data[0] != byte(message.Header.Sequence) || message.Data[len(message.Data)-1] != byte(message.Header.Sequence) {
			t.Fatal("drained message identity or payload changed")
		}
		messages = append(messages, message)
	}
	return messages
}

type phase4StalledDecodeConn struct {
	net.Conn
	entered chan struct{}
	once    sync.Once
}

func (connection *phase4StalledDecodeConn) Read(buffer []byte) (int, error) {
	if len(buffer) == phase4StalledPayload {
		connection.once.Do(func() { close(connection.entered) })
	}
	return connection.Conn.Read(buffer)
}

func phase4StalledPrefix(t *testing.T) []byte {
	t.Helper()
	frame := phase4StalledFrame(t, 1)
	wire, err := (CRCCodec{WithDataCRC: true}).Encode(frame, performanceLimits)
	if err != nil {
		t.Fatal(err)
	}
	return append([]byte(nil), wire[:PreambleSize+MessageHeaderSize+4]...)
}

func phase4StalledDecode(t *testing.T, report *phase4StalledReport) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	baseline := phase4StalledCapture(t, "baseline", phase4StalledMemory{})
	baseline.HeapDelta, baseline.RSSDelta, baseline.GoroutineDelta = 0, 0, 0
	report.DecodeStates = append(report.DecodeStates, baseline)
	client, peer := net.Pipe()
	defer client.Close()
	defer peer.Close()
	if err := client.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := peer.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	connection := &phase4StalledDecodeConn{Conn: client, entered: make(chan struct{})}
	transport, err := NewConnTransport(connection, CRCCodec{WithDataCRC: true}, performanceLimits)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan fakeRead, 1)
	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		frame, readErr := transport.ReadFrame()
		result <- fakeRead{frame: frame, err: readErr}
	}()
	defer func() { _ = client.Close(); _ = peer.Close(); workers.Wait() }()
	phase4StalledWritePrefix(t, peer)
	phase4StalledWait(t, ctx, connection.entered)
	state := phase4StalledCapture(t, "crc_prefix_active_decode_before_admission", baseline.Memory)
	state.ReaderStorage = uint64(transport.(*connTransport).reader.(*bufio.Reader).Size())
	state.ActiveFrames, state.ActivePayload = 1, phase4StalledPayload
	state.ActiveCapacity = phase4StalledPayload + MessageHeaderSize
	if state.HeapDelta < int64(state.ActiveCapacity+state.ReaderStorage) {
		t.Fatalf("missing pre-admission decode allocation: %+v", state)
	}
	report.DecodeStates = append(report.DecodeStates, state)
	runtime.KeepAlive(transport)
	_ = peer.Close()
	outcome := phase4StalledWait(t, ctx, result)
	workers.Wait()
	message, decodeErr := decodeOwnedMessage(outcome.frame, performanceLimits)
	if outcome.err == nil || decodeErr == nil || len(outcome.frame.Segments) != 0 || message.Data != nil {
		t.Fatal("partial decode returned retained output")
	}
	state = phase4StalledCapture(t, "decode_error_outputs_retained", baseline.Memory)
	state.ReaderStorage = 512 << 10
	state.Error = outcome.err.Error() + "; " + decodeErr.Error()
	state.OutputFrameCapacity = phase4StalledFrameCapacity(outcome.frame)
	state.OutputMessageCapacity = uint64(cap(message.Front) + cap(message.Middle) + cap(message.Data))
	report.DecodeStates = append(report.DecodeStates, state)
	runtime.KeepAlive(outcome)
	runtime.KeepAlive(message)
	runtime.KeepAlive(transport)
	_ = transport.Close()
	transport = nil
	state = phase4StalledCapture(t, "decode_post_release", baseline.Memory)
	if state.HeapDelta > 4<<20 {
		t.Fatalf("decode release heap did not return near baseline: %+v", state)
	}
	report.DecodeStates = append(report.DecodeStates, state)
}

func phase4StalledWritePrefix(t *testing.T, peer net.Conn) {
	t.Helper()
	prefix := phase4StalledPrefix(t)
	if written, err := peer.Write(prefix); err != nil || written != len(prefix) {
		t.Fatalf("prefix write: %d, %v", written, err)
	}
}

func TestPhase4StalledCapture(t *testing.T) {
	if os.Getenv("P4_STALLED_CAPTURE") != "1" {
		t.Skip("opt-in measurement: P4_STALLED_CAPTURE=1 P4_STALLED_OUT=<json path>")
	}
	output := os.Getenv("P4_STALLED_OUT")
	if output == "" {
		t.Fatal("P4_STALLED_OUT is required")
	}
	report := phase4StalledReport{Schema: 1, GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH,
		QueueDepth: phase4StalledDepth, PayloadBytes: phase4StalledPayload, MaxRetainedBytes: phase4StalledPayload,
		Scope: "Measurement only; no production budget change. Owned fake frames traverse the actual session handleMessage/incoming queue, with no reader storage. Owner snapshot-response gate holds one pump read-ahead frame; transport gate holds another owned frame before return (not a codec allocation). Separate net.Pipe built-in CRC prefix probe blocks a real 4MiB segment Read after allocation, before message admission; reader is 512KiB. Frame segment capacities include headers; message capacities exclude headers. Logical/capacity counts exclude metadata, allocator rounding, runtime and fixture overhead. Forced-GC heap and process RSS are distinct; RSS need not return after release. Stopped incoming channel retains buffered messages until drained; application outputs remain live until explicitly released. Receive channel has no dequeue accounting interception. Parent production budget and shutdown-release design remain pending."}
	phase4StalledQueue(t, &report)
	phase4StalledDecode(t, &report)
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(output, append(encoded, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("stalled measurement written to %s", output)
}
