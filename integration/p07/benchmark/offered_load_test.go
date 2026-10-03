package main

import (
	"bytes"
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type manualOfferedClock struct {
	mu       sync.Mutex
	now      time.Time
	waits    []time.Time
	stall    time.Duration
	end      time.Time
	finished chan struct{}
}

func (clock *manualOfferedClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}
func (clock *manualOfferedClock) Wait(ctx context.Context, until time.Time) error {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.waits = append(clock.waits, until)
	if clock.now.Before(until) {
		clock.now = until
	}
	clock.now = clock.now.Add(clock.stall)
	clock.stall = 0
	if clock.finished != nil && until.Equal(clock.end) {
		close(clock.finished)
		clock.finished = nil
	}
	return ctx.Err()
}

func TestOfferedConfig(t *testing.T) {
	valid := map[string]string{"P07_OFFERED_LOAD": "1", "P07_OFFERED_RATE": "1000", "P07_READ_DIAGNOSTIC": "1", "P07_READ_INTO": "1", "P07_BACKGROUND_WORKERS": "8", "GOMAXPROCS": "10", "GOGC": "100", "GOMEMLIMIT": "off"}
	for _, rate := range []string{"1000", "2000", "4000"} {
		valid["P07_OFFERED_RATE"] = rate
		config, err := parseOfferedConfig(func(name string) string { return valid[name] })
		if err != nil || config.Workers != 16 || config.Queue != 128 || config.Window != 8*time.Second || config.AllocRate != 100 {
			t.Fatalf("config=%+v err=%v", config, err)
		}
	}
	for name, value := range map[string]string{"P07_OFFERED_LOAD": "0", "P07_OFFERED_RATE": "", "P07_FIXED_ALLOC_RATE": "99", "P07_TIMING_FILE": "timing", "P07_SCRATCH_SLOTS": "4", "P07_READ_SIZE": "4096", "P07_READ_CONCURRENCY": "32", "GOGC": "off", "P07_BACKGROUND_WORKERS": "0"} {
		t.Run(name, func(t *testing.T) {
			_, err := parseOfferedConfig(func(key string) string {
				if key == name {
					return value
				}
				return valid[key]
			})
			if err == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
	for _, name := range []string{"P07_OFFERED_RATE", "P07_FIXED_ALLOC_RATE"} {
		if _, err := parseOfferedConfig(func(key string) string {
			if key == name {
				return "1000"
			}
			return ""
		}); err == nil {
			t.Fatal("orphan limit accepted")
		}
	}
}

func TestOfferedParityRates(t *testing.T) {
	valid := map[string]string{"P07_OFFERED_LOAD": "1", "P07_READ_DIAGNOSTIC": "1", "P07_READ_INTO": "1", "P07_BACKGROUND_WORKERS": "8", "GOMAXPROCS": "10", "GOGC": "100", "GOMEMLIMIT": "off", "P07_PARITY_NAMESPACE": "p07-parity-offered", "P07_OFFERED_FACTORIAL": "1", "P07_OFFERED_CASE": "none"}
	get := func(name string) string { return valid[name] }
	for _, rate := range []string{"8000", "16000", "32000", "64000"} {
		valid["P07_OFFERED_RATE"] = rate
		if config, err := parseOfferedConfig(get); err != nil || config.CPUWorkers != 0 || config.AllocationWorkers != 0 {
			t.Fatalf("rate=%s config=%+v err=%v", rate, config, err)
		}
	}
	for name, value := range map[string]string{"P07_PARITY_NAMESPACE": "", "P07_OFFERED_FACTORIAL": "", "P07_OFFERED_CASE": "both", "P07_OFFERED_RATE": "128000"} {
		original := valid[name]
		valid[name] = value
		if _, err := parseOfferedConfig(get); err == nil {
			t.Fatalf("accepted high rate with %s=%q", name, value)
		}
		valid[name] = original
	}
}

func TestOfferedAbsoluteAfterStall(t *testing.T) {
	start := time.Now().Add(time.Hour)
	clock := &manualOfferedClock{now: start, stall: 75 * time.Millisecond}
	config := offeredConfig{Rate: 1000, Window: 100 * time.Millisecond, Deadline: 500 * time.Millisecond, LagLimit: 50 * time.Millisecond, Workers: 16, Queue: 128}
	result := runOffered(context.Background(), config, clock, func(context.Context, int) error { return nil }, nil)
	if !result.DeliveryInvalid || result.Expected != 100 || result.Success != 100 {
		t.Fatalf("result=%+v", result)
	}
	for index := 0; index < 100; index++ {
		if !clock.waits[index].Equal(offeredArrival(start, index, 1000)) {
			t.Fatalf("reset schedule at %d", index)
		}
	}
	if result.AllOutcomeP99NS < int64(50*time.Millisecond) {
		t.Fatal("latency ignored scheduled arrival")
	}
}

func TestOfferedOverflowCancellationAndJoin(t *testing.T) {
	clock := &manualOfferedClock{now: time.Now().Add(time.Hour)}
	ctx, cancel := context.WithCancel(context.Background())
	config := offeredConfig{Rate: 1000, Window: time.Second, Deadline: 500 * time.Millisecond, LagLimit: 50 * time.Millisecond, Workers: 16, Queue: 128}
	defer cancel()
	var calls atomic.Int32
	result := runOffered(ctx, config, clock, func(context.Context, int) error {
		calls.Add(1)
		return nil
	}, func(time.Time) { cancel() })
	if calls.Load() != 16*readWarmupPerWorker || !result.Failed || result.Attempted != 0 {
		t.Fatalf("unexpected reads after cancellation: %+v", result)
	}
	if result.Canceled != result.Expected {
		t.Fatal("missing canceled slots")
	}
}

func TestOfferedQueueOverflow(t *testing.T) {
	start := time.Now().Add(time.Hour)
	finished := make(chan struct{})
	clock := &manualOfferedClock{now: start, end: start.Add(time.Second), finished: finished}
	config := offeredConfig{Rate: 1000, Window: time.Second, Deadline: 2 * time.Second, LagLimit: 50 * time.Millisecond, Workers: 16, Queue: 128}
	var calls, active, peak atomic.Int32
	result := runOffered(context.Background(), config, clock, func(context.Context, int) error {
		if calls.Add(1) <= 16*readWarmupPerWorker {
			return nil
		}
		current := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); current > old; old = peak.Load() {
			if peak.CompareAndSwap(old, current) {
				break
			}
		}
		<-finished
		return nil
	}, nil)
	if result.Overload == 0 || result.Success+result.Timeouts+result.Overload != result.Expected {
		t.Fatalf("overflow accounting: %+v", result)
	}
	if peak.Load() > 16 || active.Load() != 0 {
		t.Fatal("workers exceeded bound or failed to join")
	}
}

func TestOfferedActiveCancellationDrains(t *testing.T) {
	clock := &manualOfferedClock{now: time.Now().Add(time.Hour)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	config := offeredConfig{Rate: 1000, Window: time.Second, Deadline: 10 * time.Second, LagLimit: 50 * time.Millisecond, Workers: 16, Queue: 128}
	var calls, active atomic.Int32
	result := runOffered(ctx, config, clock, func(callCtx context.Context, worker int) error {
		if calls.Add(1) <= 16*readWarmupPerWorker {
			return nil
		}
		active.Add(1)
		defer active.Add(-1)
		cancel()
		<-callCtx.Done()
		return callCtx.Err()
	}, nil)
	if result.Attempted == 0 || result.Canceled == 0 || active.Load() != 0 {
		t.Fatalf("active cancellation did not join: %+v", result)
	}
	if result.Success+result.Timeouts+result.Overload+result.Errors+result.Canceled != result.Expected || !result.Failed {
		t.Fatal("canceled drain lost slots")
	}
}

func TestFixedAllocationSchedule(t *testing.T) {
	start := time.Now()
	completed, missed, late := fixedAllocationDue(start.Add(200*time.Millisecond), start, 100, 800, 0)
	if completed != 8 || missed != 13 || late != int64(200*time.Millisecond) {
		t.Fatalf("%d %d %d", completed, missed, late)
	}
	completed, missed, _ = fixedAllocationDue(start.Add(210*time.Millisecond), start, 100, 800, 21)
	if completed != 1 || missed != 0 {
		t.Fatal("schedule reset or hash-dependent rate")
	}
	if completed, missed, _ := fixedAllocationDue(start.Add(9*time.Second), start, 100, 800, 0); completed+missed != 800 {
		t.Fatal("lost ticks")
	}
}

func TestOfferedAbsoluteDeadline(t *testing.T) {
	start := time.Now().Add(time.Hour)
	clock := &manualOfferedClock{now: start}
	config := offeredConfig{Rate: 10, Window: time.Second, Deadline: 2 * time.Second, LagLimit: 50 * time.Millisecond, Workers: 1, Queue: 128}
	var deadlines []time.Time
	result := runOffered(context.Background(), config, clock, func(ctx context.Context, worker int) error {
		if deadline, ok := ctx.Deadline(); ok {
			deadlines = append(deadlines, deadline)
		}
		return nil
	}, nil)
	if result.Success != 10 || len(deadlines) != 10 {
		t.Fatalf("missing deadlines: %+v", result)
	}
	for index, deadline := range deadlines {
		if !deadline.Equal(offeredArrival(start, index, config.Rate).Add(config.Deadline)) {
			t.Fatalf("deadline reset at %d", index)
		}
	}
}

func TestFixedBackgroundMissedAndJoined(t *testing.T) {
	start := time.Now().Add(time.Hour)
	clock := &manualOfferedClock{now: start}
	config := offeredConfig{Window: 8 * time.Second, AllocRate: 100, LagLimit: 50 * time.Millisecond}
	arm, stop := startFixedBackgroundWork(clock, config)
	arm(start)
	clock.mu.Lock()
	clock.now = start.Add(9 * time.Second)
	clock.mu.Unlock()
	stats := stop()
	if stats.Expected != 6400 || stats.Completed+stats.Missed != stats.Expected || stats.Missed == 0 || !stats.Invalid {
		t.Fatalf("lost allocation accounting: %+v", stats)
	}
	for _, worker := range stats.Workers {
		if worker.HashIterations == 0 || worker.Expected != 800 || worker.Completed+worker.Missed != 800 || worker.Bytes != uint64(worker.Completed)*65536 {
			t.Fatalf("background not ready/accounted: %+v", worker)
		}
	}
	if again := stop(); again.Completed != stats.Completed || again.Missed != stats.Missed {
		t.Fatal("stop not idempotent/joined")
	}
}

func TestOfferedFailureJSONBeforeError(t *testing.T) {
	var output bytes.Buffer
	result := offeredResult{Expected: 8, Failed: true}
	if err := writeOfferedReport(&output, result, result.Failed); err == nil {
		t.Fatal("failure exit missing")
	}
	var decoded offeredResult
	if err := json.Unmarshal(output.Bytes(), &decoded); err != nil || decoded.Expected != 8 || !decoded.Failed {
		t.Fatalf("missing failure JSON: %v %s", err, output.Bytes())
	}
	if err := writeOfferedReport(&output, make(chan int), false); err == nil {
		t.Fatal("marshal failure ignored")
	}
}

func TestOfferedFailureAwareSLO(t *testing.T) {
	result := offeredResult{Config: offeredConfig{Window: 8 * time.Second}, Expected: 7, TotalWindowDrainNS: int64(9 * time.Second), Outcomes: []offeredOutcome{
		{Kind: "success", LatencyNS: 1, Admitted: true, Attempted: true},
		{Kind: "success", LatencyNS: int64(600 * time.Millisecond), Admitted: true, Attempted: true, DeadlineMiss: true},
		{Kind: "timeout", LatencyNS: int64(700 * time.Millisecond), Admitted: true, Attempted: true, DeadlineMiss: true},
		{Kind: "overload", LatencyNS: 2},
		{Kind: "error", LatencyNS: 3, Admitted: true, Attempted: true},
		{Kind: "canceled", LatencyNS: 4, Admitted: true},
		{Kind: "canceled", LatencyNS: 5},
	}}
	result.summarize()
	if result.Success != 2 || result.SLOFailures != 6 || !result.Failed || result.Admitted != 5 || result.Attempted != 4 {
		t.Fatalf("failure-aware accounting: %+v", result)
	}
	if result.SuccessP99NS != int64(600*time.Millisecond) || result.AllOutcomeP99NS != int64(700*time.Millisecond) {
		t.Fatal("p99 dropped failed outcomes")
	}
	if result.SuccessBytes != 2*65536 || result.WindowBytesPerSecond != float64(2*65536)/8 || result.TotalBytesPerSecond != float64(2*65536)/9 {
		t.Fatal("throughput denominator or failed bytes manipulated")
	}
}
