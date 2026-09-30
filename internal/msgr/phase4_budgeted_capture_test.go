package msgr

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

type budgetedRecord struct {
	phase4ResourceRecord
	LiveLedger  ReceiveBudgetSnapshot `json:"live_ledger"`
	AfterLedger ReceiveBudgetSnapshot `json:"after_ledger"`
}

type phase4BudgetedStalledRecord struct {
	Mode            string                 `json:"mode"`
	Repeat          int                    `json:"repeat"`
	PayloadBytes    int                    `json:"payload_bytes"`
	QueueDepth      int                    `json:"queue_depth"`
	QueueByteLimit  uint64                 `json:"queue_byte_limit"`
	SharedByteLimit uint64                 `json:"shared_byte_limit"`
	QueuedMessages  int                    `json:"queued_messages"`
	QueuedBytes     uint64                 `json:"queued_bytes"`
	BeforeLive      phase4ResourceSnapshot `json:"before_live"`
	LiveQueue       phase4ResourceSnapshot `json:"live_queue"`
	AfterTerminal   phase4ResourceSnapshot `json:"after_terminal"`
	PostClose       phase4ResourceSnapshot `json:"post_close"`
	LiveLedger      ReceiveBudgetSnapshot  `json:"live_ledger"`
	TerminalLedger  ReceiveBudgetSnapshot  `json:"terminal_ledger"`
	AfterLedger     ReceiveBudgetSnapshot  `json:"after_ledger"`
	PeakReadBytes   uint64                 `json:"peak_read_bytes"`
	TerminalError   string                 `json:"terminal_error"`
}

type phase4BudgetedReport struct {
	Schema                int                           `json:"schema_version"`
	SourceHEAD            string                        `json:"source_head"`
	SourceFile            string                        `json:"source_file"`
	SourceSHA256          string                        `json:"source_sha256"`
	GoVersion             string                        `json:"go_version"`
	GOOS                  string                        `json:"goos"`
	GOARCH                string                        `json:"goarch"`
	GOMAXPROCS            int                           `json:"gomaxprocs"`
	PID                   int                           `json:"pid"`
	MaxSessions           int                           `json:"max_sessions"`
	SharedReceiveBytes    uint64                        `json:"shared_receive_bytes"`
	MaxQueuedReceiveBytes uint64                        `json:"max_queued_receive_bytes"`
	QueueDepth            int                           `json:"queue_depth"`
	ReaderBytes           int                           `json:"reader_bytes_per_session"`
	MaxControlBytes       uint64                        `json:"max_control_bytes"`
	Repeats               int                           `json:"repeats"`
	SourceScope           string                        `json:"source_scope"`
	StalledScope          string                        `json:"stalled_scope"`
	Records               []budgetedRecord              `json:"records"`
	Summaries             []phase4ResourceSummary       `json:"summaries"`
	Stalled               []phase4BudgetedStalledRecord `json:"stalled_records"`
}

func phase4BudgetedConfig(t *testing.T, slots int, receiveBytes uint64) SessionConfig {
	t.Helper()
	budget, err := NewReceiveBudget(slots, receiveBytes)
	if err != nil {
		t.Fatal(err)
	}
	config := performanceConfig(256)
	config.ReceiveBudget = budget
	config.MaxQueuedReceiveBytes = 64 << 20
	return config
}

func newPhase4BudgetedFixture(t *testing.T, infrastructure, mode string, count int, config SessionConfig) *phase4ResourceFixture {
	t.Helper()
	fixture := &phase4ResourceFixture{idle: &performanceIdleSet{}}
	ready := false
	defer func() {
		if !ready {
			fixture.stop()
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if infrastructure == "tcp_loopback" {
		listener, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		fixture.listener = listener
		if err := listener.(*net.TCPListener).SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
			t.Fatal(err)
		}
	} else if infrastructure != "fake" {
		t.Fatalf("unknown infrastructure %q", infrastructure)
	}
	for range count {
		var connection net.Conn
		var entered <-chan struct{}
		if infrastructure == "fake" {
			idle := &performanceIdleConn{entered: make(chan struct{}), closed: make(chan struct{})}
			connection, entered = idle, idle.entered
		} else {
			client, err := (&net.Dialer{}).DialContext(ctx, "tcp4", fixture.listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			peer, err := fixture.listener.Accept()
			if err != nil {
				_ = client.Close()
				t.Fatal(err)
			}
			fixture.peers = append(fixture.peers, peer)
			started := make(chan struct{})
			fixture.drains.Add(1)
			go func() {
				defer fixture.drains.Done()
				close(started)
				_, _ = io.Copy(io.Discard, peer)
			}()
			<-started
			gate := &phase4ReadGateConn{Conn: client, entered: make(chan struct{})}
			connection, entered = gate, gate.entered
		}
		transport, err := NewConnTransport(connection, performanceClientCodec(t, mode), performanceLimits)
		if err != nil {
			_ = connection.Close()
			t.Fatal(err)
		}
		session, err := NewSession(transport, nil, config)
		if err != nil {
			_ = transport.Close()
			t.Fatal(err)
		}
		fixture.idle.sessions = append(fixture.idle.sessions, session)
		fixture.idle.transports = append(fixture.idle.transports, transport.(*connTransport))
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("idle read pump did not start:", ctx.Err())
		}
		snapshot, err := session.Snapshot(ctx)
		if err != nil || snapshot.State != StateReady || snapshot.RetainedBytes != 0 || snapshot.RetainedReceiveBytes != 0 {
			t.Fatalf("idle session = %+v err=%v", snapshot, err)
		}
	}
	ready = true
	return fixture
}

func phase4BudgetedReaderRetired(t *testing.T, transport *connTransport) {
	t.Helper()
	transport.readMu.Lock()
	defer transport.readMu.Unlock()
	if !transport.closed || transport.reader != nil {
		t.Fatal("stopped transport retained its physical reader")
	}
}

func phase4BudgetedCapture(t *testing.T, infrastructure, mode string, count, repeat int) budgetedRecord {
	t.Helper()
	config := phase4BudgetedConfig(t, 256, 256<<20)
	budget := config.ReceiveBudget
	runtime.GC()
	runtime.GC()
	record := budgetedRecord{phase4ResourceRecord: phase4ResourceRecord{
		Infrastructure: infrastructure, Mode: mode, Connections: count, Repeat: repeat, BeforeLive: phase4Snapshot(t),
	}}
	fixture := newPhase4BudgetedFixture(t, infrastructure, mode, count, config)
	defer fixture.stop()
	idle := fixture.idle
	for _, transport := range idle.transports {
		reader, ok := transport.reader.(*bufio.Reader)
		if !ok || reader.Size() != 512<<10 {
			t.Fatal("fixture did not use the approved 512 KiB reader")
		}
		record.ReaderSizes = append(record.ReaderSizes, reader.Size())
		record.ReaderStorage += uint64(reader.Size())
	}
	record.LiveLedger = budget.Snapshot()
	wantControl := uint64(0)
	if mode == "secure" {
		wantControl = uint64(count * securePreamble)
	}
	if record.LiveLedger != (ReceiveBudgetSnapshot{Sessions: count, ControlBytes: wantControl}) || record.ReaderStorage != uint64(count)*(512<<10) {
		t.Fatalf("idle ledger=%+v reader storage=%d", record.LiveLedger, record.ReaderStorage)
	}
	runtime.GC()
	record.Live = phase4Snapshot(t)
	runtime.KeepAlive(fixture)
	fixture.stop()
	for _, transport := range idle.transports {
		phase4BudgetedReaderRetired(t, transport)
	}
	idle.sessions, idle.transports = nil, nil
	idle = nil
	record.AfterLedger = budget.Snapshot()
	if record.AfterLedger != (ReceiveBudgetSnapshot{}) || fixture.idle != nil || fixture.listener != nil || fixture.peers != nil {
		t.Fatalf("stop retained fixture or budget: %+v", record.AfterLedger)
	}
	fixture = nil
	runtime.GC()
	runtime.GC()
	record.PostClose = phase4Snapshot(t)
	runtime.KeepAlive(budget)
	record.LiveDelta = phase4Delta(record.Live, record.BeforeLive)
	record.PostCloseDelta = phase4Delta(record.PostClose, record.BeforeLive)
	return record
}

func TestPhase4BudgetedAdmissionBoundary(t *testing.T) {
	for _, mode := range []string{"crc", "secure"} {
		t.Run(mode, func(t *testing.T) {
			config := phase4BudgetedConfig(t, 2, 256<<20)
			fixture := newPhase4BudgetedFixture(t, "fake", mode, 2, config)
			defer fixture.stop()
			connection := &performanceIdleConn{entered: make(chan struct{}), closed: make(chan struct{})}
			transport, err := NewConnTransport(connection, performanceClientCodec(t, mode), performanceLimits)
			if err != nil {
				t.Fatal(err)
			}
			defer transport.Close()
			if session, err := NewSession(transport, nil, config); !errors.Is(err, ErrReceiveBudgetExceeded) {
				if session != nil {
					session.Stop()
				}
				t.Fatalf("third admission error = %v", err)
			}
			if got := config.ReceiveBudget.Snapshot().Sessions; got != 2 {
				t.Fatalf("rejection changed admitted count: %d", got)
			}
			fixture.stop()
			if got := config.ReceiveBudget.Snapshot(); got != (ReceiveBudgetSnapshot{}) {
				t.Fatalf("retirement leaked %+v", got)
			}
			replacement := newPhase4BudgetedFixture(t, "fake", mode, 2, config)
			replacement.stop()
			if got := config.ReceiveBudget.Snapshot(); got != (ReceiveBudgetSnapshot{}) {
				t.Fatal(got)
			}
		})
	}
}

type phase4BudgetedPeakConn struct {
	net.Conn
	budget *ReceiveBudget
	mu     sync.Mutex
	peak   uint64
}

func (connection *phase4BudgetedPeakConn) observe() {
	snapshot := connection.budget.Snapshot()
	connection.mu.Lock()
	connection.peak = max(connection.peak, snapshot.RetainedBytes)
	connection.mu.Unlock()
}

func (connection *phase4BudgetedPeakConn) Read(buffer []byte) (int, error) {
	connection.observe()
	count, err := connection.Conn.Read(buffer)
	connection.observe()
	return count, err
}

func phase4BudgetedStalledCapture(t *testing.T, mode string, repeat int) phase4BudgetedStalledRecord {
	t.Helper()
	const payloadBytes = 4 << 20
	config := phase4BudgetedConfig(t, 256, 16<<20)
	config.MaxQueuedMessages = 8
	config.MaxQueuedReceiveBytes = 9 << 20
	budget := config.ReceiveBudget
	runtime.GC()
	runtime.GC()
	record := phase4BudgetedStalledRecord{Mode: mode, Repeat: repeat, PayloadBytes: payloadBytes,
		QueueDepth: config.MaxQueuedMessages, QueueByteLimit: config.MaxQueuedReceiveBytes,
		SharedByteLimit: 16 << 20, BeforeLive: phase4Snapshot(t)}
	clientCodec, peerCodec := performanceCodecs(t, mode)
	local, peer := net.Pipe()
	defer func() {
		if peer != nil {
			_ = peer.Close()
		}
	}()
	if err := peer.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
		t.Fatal(err)
	}
	connection := &phase4BudgetedPeakConn{Conn: local, budget: budget}
	transport, err := NewConnTransport(connection, clientCodec, performanceLimits)
	if err != nil {
		_ = local.Close()
		t.Fatal(err)
	}
	session, err := NewSession(transport, nil, config)
	if err != nil {
		_ = transport.Close()
		t.Fatal(err)
	}
	defer func() {
		if session != nil {
			session.Stop()
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	send := func(sequence uint64, acknowledge bool) {
		t.Helper()
		message := Message{Header: MessageHeader{Sequence: sequence}, Data: bytes.Repeat([]byte{0x5a}, payloadBytes), Lengths: MessageLengths{Data: payloadBytes}}
		frame, err := EncodeMessage(message, performanceLimits)
		if err != nil {
			t.Fatal(err)
		}
		wire, err := peerCodec.Encode(frame, performanceLimits)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := peer.Write(wire); err != nil {
			t.Fatal(err)
		}
		if acknowledge {
			ack, err := peerCodec.Read(peer, performanceLimits)
			if err != nil || ack.Tag != TagAck {
				t.Fatalf("ack tag=%v err=%v", ack.Tag, err)
			}
		}
	}
	send(1, true)
	send(2, true)
	snapshot, err := session.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	record.QueuedMessages, record.QueuedBytes = snapshot.QueuedReceiveMessages, snapshot.RetainedReceiveBytes
	record.LiveLedger = budget.Snapshot()
	if record.QueuedMessages != 2 || record.QueuedBytes <= 2*payloadBytes || record.QueuedBytes > 9<<20 || record.LiveLedger.RetainedBytes != record.QueuedBytes || record.LiveLedger.Sessions != 1 {
		t.Fatalf("stalled snapshot=%+v ledger=%+v", snapshot, record.LiveLedger)
	}
	runtime.GC()
	record.LiveQueue = phase4Snapshot(t)
	send(3, false)
	select {
	case err := <-session.Terminal():
		if !errors.Is(err, ErrQueueSaturated) || errors.Is(err, ErrReceiveBudgetExceeded) {
			t.Fatalf("third message terminal error=%v", err)
		}
		record.TerminalError = err.Error()
	case <-ctx.Done():
		t.Fatal("saturation did not terminate:", ctx.Err())
	}
	snapshot, err = session.Snapshot(ctx)
	if err != nil || snapshot.QueuedReceiveMessages != 0 || snapshot.RetainedReceiveBytes != 0 {
		t.Fatalf("terminal snapshot=%+v err=%v", snapshot, err)
	}
	phase4BudgetedReaderRetired(t, transport.(*connTransport))
	connection.mu.Lock()
	record.PeakReadBytes = connection.peak
	connection.mu.Unlock()
	record.TerminalLedger = budget.Snapshot()
	if record.PeakReadBytes < 3*payloadBytes || record.PeakReadBytes > 16<<20 || record.TerminalLedger != (ReceiveBudgetSnapshot{Sessions: 1}) {
		t.Fatalf("peak=%d terminal ledger=%+v", record.PeakReadBytes, record.TerminalLedger)
	}
	runtime.GC()
	record.AfterTerminal = phase4Snapshot(t)
	session.Stop()
	_ = peer.Close()
	record.AfterLedger = budget.Snapshot()
	if record.AfterLedger != (ReceiveBudgetSnapshot{}) {
		t.Fatal(record.AfterLedger)
	}
	session, transport, connection, peer = nil, nil, nil, nil
	runtime.GC()
	runtime.GC()
	record.PostClose = phase4Snapshot(t)
	runtime.KeepAlive(budget)
	return record
}

func TestPhase4BudgetedStalledCapture(t *testing.T) {
	if os.Getenv("P4_BUDGETED_CAPTURE") != "1" {
		t.Skip("set P4_BUDGETED_CAPTURE=1 for budgeted resource measurements")
	}
	for _, mode := range []string{"crc", "secure"} {
		t.Run(mode, func(t *testing.T) {
			record := phase4BudgetedStalledCapture(t, mode, 1)
			encoded, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			t.Log(string(encoded))
		})
	}
}

func TestPhase4BudgetedCapture(t *testing.T) {
	if os.Getenv("P4_BUDGETED_CAPTURE") != "1" {
		t.Skip("set P4_BUDGETED_CAPTURE=1 for serial budgeted fake/TCP-loopback measurements")
	}
	head, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate budgeted capture source")
	}
	sourceBytes, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	report := phase4BudgetedReport{Schema: 1, SourceHEAD: strings.TrimSpace(string(head)),
		SourceFile: "internal/msgr/phase4_budgeted_capture_test.go", SourceSHA256: fmt.Sprintf("%x", sha256.Sum256(sourceBytes)),
		GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH,
		GOMAXPROCS: runtime.GOMAXPROCS(0), PID: os.Getpid(), MaxSessions: 256, SharedReceiveBytes: 256 << 20,
		MaxQueuedReceiveBytes: 64 << 20, QueueDepth: 256, ReaderBytes: 512 << 10,
		MaxControlBytes: 256 * receiveControlBytesPerSession, Repeats: 3,
		SourceScope:  "Opt-in idle ready Sessions using public ReceiveBudget and SessionConfig settings, one fresh shared budget per measurement. Production connTransport and CRC/secure codecs with fixed test key; existing server cookie, no protocol handshake/authentication, incoming traffic, idle timeout, application output, native comparison or cluster. Direct transports are constructed before NewSession; transport construction admission is explicitly excluded. Fake uses channel-backed net.Conn; TCP loopback includes listener, both socket ends and peer drain goroutines. Reader capacity is separate from the receive ledger: 128 readers allocate 64 MiB; secure idle decoders hold 96 control bytes each, CRC idle prefix waits hold zero payload bytes. Fixed controls have a separate 256*3*96 allowance. Heap includes fixture/runtime overhead; RSS is separately sampled by ps and is not a budget claim. Warm ps; live follows GC, baseline/post-close two GCs; Stop joins pumps and drains, physical readers are nil, fixture fields cleared, budget pointer kept alive post-close. Source hash identifies this file only; HEAD does not describe dirty dependencies. Counts through 128 characterize growth, not a 256-session capacity measurement; separate two-slot test proves admission boundary.",
		StalledScope: "Real net.Pipe built-in CRC/secure decoding, ready cookie, no auth. Queue depth 8, queue byte cap intentionally 9 MiB to fit two 4 MiB bodies plus backing overhead; shared cap 16 MiB, stricter than the approved 256 MiB. Third message saturates the queue and releases internal receive storage. PeakReadBytes samples ledger at actual connection Read boundaries, including active decoder and queued backing, not an allocator/RSS peak. Peer payload/frame/wire construction and reader capacity are outside the receive byte budget and included in noisy heap observations. No application receives or retained application outputs are measured."}
	_ = phase4Snapshot(t)
	for _, infrastructure := range []string{"fake", "tcp_loopback"} {
		for _, mode := range []string{"crc", "secure"} {
			for _, count := range []int{1, 16, 64, 128} {
				var records []phase4ResourceRecord
				for repeat := 1; repeat <= report.Repeats; repeat++ {
					record := phase4BudgetedCapture(t, infrastructure, mode, count, repeat)
					report.Records = append(report.Records, record)
					records = append(records, record.phase4ResourceRecord)
				}
				report.Summaries = append(report.Summaries, phase4ResourceSummary{Infrastructure: infrastructure,
					Mode: mode, Connections: count, MedianLive: phase4MedianDelta(records, false),
					MedianPostClose: phase4MedianDelta(records, true), ReaderStorage: records[0].ReaderStorage})
			}
		}
	}
	for _, mode := range []string{"crc", "secure"} {
		for repeat := 1; repeat <= report.Repeats; repeat++ {
			report.Stalled = append(report.Stalled, phase4BudgetedStalledCapture(t, mode, repeat))
		}
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(encoded))
	if destination := os.Getenv("P4_BUDGETED_OUT"); destination != "" {
		file, err := os.Create(destination)
		if err != nil {
			t.Fatal(err)
		}
		encoder := json.NewEncoder(file)
		encoder.SetIndent("", "  ")
		writeErr := encoder.Encode(report)
		closeErr := file.Close()
		if writeErr != nil || closeErr != nil {
			t.Fatalf("write budgeted capture: encode=%v close=%v", writeErr, closeErr)
		}
	}
}
