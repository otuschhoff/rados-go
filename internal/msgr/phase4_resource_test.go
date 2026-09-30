package msgr

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type phase4ResourceSnapshot struct {
	HeapAlloc  uint64 `json:"heap_alloc_bytes"`
	HeapInuse  uint64 `json:"heap_inuse_bytes"`
	StackInuse uint64 `json:"stack_inuse_bytes"`
	RSS        uint64 `json:"rss_bytes"`
	Goroutines int    `json:"goroutines"`
}

type phase4ResourceDelta struct {
	HeapAlloc  int64 `json:"heap_alloc_bytes"`
	HeapInuse  int64 `json:"heap_inuse_bytes"`
	RSS        int64 `json:"rss_bytes"`
	Goroutines int   `json:"goroutines"`
}

type phase4ResourceRecord struct {
	Infrastructure string                 `json:"infrastructure"`
	Mode           string                 `json:"mode"`
	Connections    int                    `json:"connections"`
	Repeat         int                    `json:"repeat"`
	BeforeLive     phase4ResourceSnapshot `json:"before_live"`
	Live           phase4ResourceSnapshot `json:"live"`
	PostClose      phase4ResourceSnapshot `json:"post_close"`
	LiveDelta      phase4ResourceDelta    `json:"live_delta"`
	PostCloseDelta phase4ResourceDelta    `json:"post_close_delta"`
	ReaderSizes    []int                  `json:"reader_sizes_bytes"`
	ReaderStorage  uint64                 `json:"reader_storage_bytes"`
}

type phase4ResourceSummary struct {
	Infrastructure  string              `json:"infrastructure"`
	Mode            string              `json:"mode"`
	Connections     int                 `json:"connections"`
	MedianLive      phase4ResourceDelta `json:"median_live_delta"`
	MedianPostClose phase4ResourceDelta `json:"median_post_close_delta"`
	ReaderStorage   uint64              `json:"reader_storage_bytes"`
}

type phase4ResourceSlope struct {
	Infrastructure string  `json:"infrastructure"`
	Mode           string  `json:"mode"`
	From           int     `json:"from_connections"`
	To             int     `json:"to_connections"`
	LiveHeap       float64 `json:"live_heap_bytes_per_connection"`
	PostCloseHeap  float64 `json:"post_close_heap_bytes_per_connection"`
	LiveRSS        float64 `json:"live_rss_bytes_per_connection"`
	PostCloseRSS   float64 `json:"post_close_rss_bytes_per_connection"`
}

type phase4ResourceReport struct {
	Schema                  int                     `json:"schema_version"`
	SourceHEAD              string                  `json:"source_head"`
	SourceFile              string                  `json:"source_file"`
	SourceSHA256            string                  `json:"source_sha256"`
	GoVersion               string                  `json:"go_version"`
	GOOS                    string                  `json:"goos"`
	GOARCH                  string                  `json:"goarch"`
	GOMAXPROCS              int                     `json:"gomaxprocs"`
	PID                     int                     `json:"pid"`
	QueueDepth              int                     `json:"queue_depth"`
	MaxRetainedBytes        uint64                  `json:"max_retained_bytes"`
	MaxInFlightTransactions int                     `json:"max_in_flight_transactions"`
	MaxSegmentBytes         uint64                  `json:"max_segment_bytes"`
	MaxFrameBytes           uint64                  `json:"max_frame_bytes"`
	EventBuffer             int                     `json:"event_buffer"`
	Repeats                 int                     `json:"repeats"`
	Scope                   string                  `json:"scope"`
	Records                 []phase4ResourceRecord  `json:"records"`
	Summaries               []phase4ResourceSummary `json:"summaries"`
	Slopes                  []phase4ResourceSlope   `json:"slopes"`
}

type phase4ReadGateConn struct {
	net.Conn
	entered chan struct{}
	once    sync.Once
}

func (connection *phase4ReadGateConn) Read(buffer []byte) (int, error) {
	connection.once.Do(func() { close(connection.entered) })
	return connection.Conn.Read(buffer)
}

type phase4ResourceFixture struct {
	idle     *performanceIdleSet
	listener net.Listener
	peers    []net.Conn
	drains   sync.WaitGroup
}

func (fixture *phase4ResourceFixture) stop() {
	if fixture.idle != nil {
		fixture.idle.stop()
		fixture.idle = nil
	}
	for _, peer := range fixture.peers {
		_ = peer.Close()
	}
	fixture.drains.Wait()
	fixture.peers = nil
	if fixture.listener != nil {
		_ = fixture.listener.Close()
		fixture.listener = nil
	}
}

func newPhase4ResourceFixture(t *testing.T, infrastructure, mode string, count int) *phase4ResourceFixture {
	t.Helper()
	if infrastructure == "fake" {
		return &phase4ResourceFixture{idle: newPerformanceIdleSet(t, mode, count)}
	}
	fixture := &phase4ResourceFixture{idle: &performanceIdleSet{}}
	ready := false
	defer func() {
		if !ready {
			fixture.stop()
		}
	}()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fixture.listener = listener
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := listener.(*net.TCPListener).SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < count; index++ {
		client, err := (&net.Dialer{}).DialContext(ctx, "tcp4", listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		peer, err := listener.Accept()
		if err != nil {
			_ = client.Close()
			t.Fatal(err)
		}
		fixture.peers = append(fixture.peers, peer)
		drainStarted := make(chan struct{})
		fixture.drains.Add(1)
		go func() {
			defer fixture.drains.Done()
			close(drainStarted)
			_, _ = io.Copy(io.Discard, peer)
		}()
		<-drainStarted
		connection := &phase4ReadGateConn{Conn: client, entered: make(chan struct{})}
		transport, err := NewConnTransport(connection, performanceClientCodec(t, mode), performanceLimits)
		if err != nil {
			_ = client.Close()
			t.Fatal(err)
		}
		session, err := NewSession(transport, nil, performanceConfig(64))
		if err != nil {
			_ = transport.Close()
			t.Fatal(err)
		}
		fixture.idle.sessions = append(fixture.idle.sessions, session)
		fixture.idle.transports = append(fixture.idle.transports, transport.(*connTransport))
		select {
		case <-connection.entered:
		case <-ctx.Done():
			t.Fatal("loopback read pump did not start:", ctx.Err())
		}
		snapshot, err := session.Snapshot(ctx)
		if err != nil || snapshot.State != StateReady || snapshot.RetainedBytes != 0 {
			t.Fatalf("loopback idle session = %+v err=%v", snapshot, err)
		}
	}
	ready = true
	return fixture
}

func phase4Snapshot(t *testing.T) phase4ResourceSnapshot {
	t.Helper()
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	output, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(os.Getpid())).Output()
	if err != nil {
		t.Fatalf("ps RSS: %v", err)
	}
	rss, err := strconv.ParseUint(strings.TrimSpace(string(output)), 10, 64)
	if err != nil || rss > ^uint64(0)/1024 {
		t.Fatalf("invalid ps RSS %q: %v", output, err)
	}
	return phase4ResourceSnapshot{HeapAlloc: memory.HeapAlloc, HeapInuse: memory.HeapInuse,
		StackInuse: memory.StackInuse, RSS: rss * 1024, Goroutines: runtime.NumGoroutine()}
}

func phase4Delta(after, before phase4ResourceSnapshot) phase4ResourceDelta {
	return phase4ResourceDelta{HeapAlloc: int64(after.HeapAlloc) - int64(before.HeapAlloc),
		HeapInuse: int64(after.HeapInuse) - int64(before.HeapInuse), RSS: int64(after.RSS) - int64(before.RSS),
		Goroutines: after.Goroutines - before.Goroutines}
}

func phase4Capture(t *testing.T, infrastructure, mode string, count, repeat int) phase4ResourceRecord {
	t.Helper()
	runtime.GC()
	runtime.GC()
	record := phase4ResourceRecord{Infrastructure: infrastructure, Mode: mode, Connections: count, Repeat: repeat,
		BeforeLive: phase4Snapshot(t)}
	fixture := newPhase4ResourceFixture(t, infrastructure, mode, count)
	defer func() {
		if fixture != nil {
			fixture.stop()
		}
	}()
	for _, transport := range fixture.idle.transports {
		reader, ok := transport.reader.(*bufio.Reader)
		if !ok {
			t.Fatal("fixture did not use the built-in buffered reader")
		}
		record.ReaderSizes = append(record.ReaderSizes, reader.Size())
		record.ReaderStorage += uint64(reader.Size())
	}
	runtime.GC()
	record.Live = phase4Snapshot(t)
	runtime.KeepAlive(fixture)
	fixture.stop()
	fixture = nil
	runtime.GC()
	runtime.GC()
	record.PostClose = phase4Snapshot(t)
	record.LiveDelta = phase4Delta(record.Live, record.BeforeLive)
	record.PostCloseDelta = phase4Delta(record.PostClose, record.BeforeLive)
	return record
}

func phase4Median(values []int64) int64 {
	sort.Slice(values, func(first, second int) bool { return values[first] < values[second] })
	return values[len(values)/2]
}

func phase4MedianDelta(records []phase4ResourceRecord, postClose bool) phase4ResourceDelta {
	var heap, inuse, rss, goroutines []int64
	for _, record := range records {
		delta := record.LiveDelta
		if postClose {
			delta = record.PostCloseDelta
		}
		heap = append(heap, delta.HeapAlloc)
		inuse = append(inuse, delta.HeapInuse)
		rss = append(rss, delta.RSS)
		goroutines = append(goroutines, int64(delta.Goroutines))
	}
	return phase4ResourceDelta{HeapAlloc: phase4Median(heap), HeapInuse: phase4Median(inuse),
		RSS: phase4Median(rss), Goroutines: int(phase4Median(goroutines))}
}

func TestPhase4ResourceCapture(t *testing.T) {
	if os.Getenv("P4_RESOURCE_CAPTURE") != "1" {
		t.Skip("set P4_RESOURCE_CAPTURE=1 for serial fake/TCP-loopback resource measurements")
	}
	head, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate resource capture source")
	}
	sourceBytes, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	config := performanceConfig(64)
	report := phase4ResourceReport{Schema: 1, SourceHEAD: strings.TrimSpace(string(head)),
		SourceFile: "internal/msgr/phase4_resource_test.go", SourceSHA256: fmt.Sprintf("%x", sha256.Sum256(sourceBytes)),
		GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH,
		GOMAXPROCS: runtime.GOMAXPROCS(0), PID: os.Getpid(), QueueDepth: config.MaxQueuedMessages,
		MaxRetainedBytes: config.MaxRetainedBytes, MaxInFlightTransactions: config.MaxInFlightTransactions,
		MaxSegmentBytes: uint64(config.Limits.MaxSegmentBytes), MaxFrameBytes: uint64(config.Limits.MaxFrameBytes),
		EventBuffer: config.EventBuffer, Repeats: 3,
		Scope: "Idle ready Sessions only; fake channel-backed net.Conn versus real IPv4 TCP loopback. Both use production connTransport, eager readers, and CRC/secure codecs (fixed test key). SessionConfig has an existing server cookie: no handshake, authentication, native cluster, or incoming traffic. Includes fixture/runtime heap; loopback additionally includes listener, both socket ends and one peer drain goroutine per connection. Reader Size reports allocated capacity, not resident/touched pages. Heap sampled before ps; ps RSS is KiB converted to bytes and sampled separately. Live follows GC; baseline and post-close follow two GCs, all pumps/peer drains joined and fixture references dropped. RSS is noisy and not asserted. Repeated local baseline deltas and endpoint slopes are descriptive, not budgets. Stalled unsolicited-message retention is not measured."}
	_ = phase4Snapshot(t)
	for _, infrastructure := range []string{"fake", "tcp_loopback"} {
		for _, mode := range []string{"crc", "secure"} {
			start := len(report.Summaries)
			for _, count := range []int{1, 16, 64, 128} {
				var records []phase4ResourceRecord
				for repeat := 1; repeat <= report.Repeats; repeat++ {
					record := phase4Capture(t, infrastructure, mode, count, repeat)
					records = append(records, record)
					report.Records = append(report.Records, record)
					encoded, err := json.Marshal(record)
					if err != nil {
						t.Fatal(err)
					}
					t.Log(string(encoded))
				}
				report.Summaries = append(report.Summaries, phase4ResourceSummary{Infrastructure: infrastructure,
					Mode: mode, Connections: count, MedianLive: phase4MedianDelta(records, false),
					MedianPostClose: phase4MedianDelta(records, true), ReaderStorage: records[0].ReaderStorage})
			}
			first, last := report.Summaries[start], report.Summaries[len(report.Summaries)-1]
			span := float64(last.Connections - first.Connections)
			report.Slopes = append(report.Slopes, phase4ResourceSlope{Infrastructure: infrastructure, Mode: mode,
				From: first.Connections, To: last.Connections,
				LiveHeap:      float64(last.MedianLive.HeapAlloc-first.MedianLive.HeapAlloc) / span,
				PostCloseHeap: float64(last.MedianPostClose.HeapAlloc-first.MedianPostClose.HeapAlloc) / span,
				LiveRSS:       float64(last.MedianLive.RSS-first.MedianLive.RSS) / span,
				PostCloseRSS:  float64(last.MedianPostClose.RSS-first.MedianPostClose.RSS) / span})
		}
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(encoded))
	if destination := os.Getenv("P4_RESOURCE_OUT"); destination != "" {
		file, err := os.Create(destination)
		if err != nil {
			t.Fatal(err)
		}
		encoder := json.NewEncoder(file)
		encoder.SetIndent("", "  ")
		writeErr := encoder.Encode(report)
		closeErr := file.Close()
		if writeErr != nil || closeErr != nil {
			t.Fatalf("write resource capture: encode=%v close=%v", writeErr, closeErr)
		}
	}
}
