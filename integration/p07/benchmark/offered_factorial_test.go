package main

import (
	"context"
	"sync"
	"testing"
	"time"
)

type timingOfferedClock struct {
	*manualOfferedClock
	readEntered <-chan struct{}
}

func (clock *timingOfferedClock) Wait(ctx context.Context, until time.Time) error {
	if until.Equal(clock.end) {
		select {
		case <-clock.readEntered:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return clock.manualOfferedClock.Wait(ctx, until)
}

func TestOfferedTimingPhases(t *testing.T) {
	start := time.Now().Add(time.Hour)
	finished := make(chan struct{})
	readEntered := make(chan struct{})
	clock := &timingOfferedClock{
		manualOfferedClock: &manualOfferedClock{now: start, end: start.Add(100 * time.Millisecond), finished: finished},
		readEntered:        readEntered,
	}
	config := offeredConfig{Rate: 10, Window: 100 * time.Millisecond, Deadline: time.Second, LagLimit: 50 * time.Millisecond, Workers: 1, Queue: 1}
	var calls int
	result := runOffered(context.Background(), config, clock, func(context.Context, int) error {
		calls++
		if calls > readWarmupPerWorker {
			close(readEntered)
			<-finished
			clock.mu.Lock()
			clock.now = clock.now.Add(17 * time.Millisecond)
			clock.mu.Unlock()
		}
		return nil
	}, nil)
	outcome := result.Outcomes[0]
	if outcome.Kind != "success" || outcome.ServiceNS != int64(117*time.Millisecond) || outcome.ReadStartNS != 0 || outcome.ReturnedNS != int64(117*time.Millisecond) || outcome.ScheduledNS != 0 || outcome.EnqueuedNS != 0 {
		t.Fatalf("timing: %+v", outcome)
	}
	if outcome.LatencyNS != outcome.DeliveryNS+outcome.QueueNS+outcome.DispatchNS+outcome.ServiceNS || outcome.ReturnedNS-outcome.ScheduledNS != outcome.LatencyNS {
		t.Fatalf("phase identity: %+v", outcome)
	}
	if result.QueueHighWaterSampled < 0 || result.QueueHighWaterSampled > config.Queue {
		t.Fatal("queue sample exceeds capacity")
	}
	clock.mu.Lock()
	after := clock.now
	clock.mu.Unlock()
	if after.Sub(start).Nanoseconds() != result.TotalWindowDrainNS {
		t.Fatal("result returned before worker completion")
	}
}

func TestOfferedMissingPhaseSentinels(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		start := time.Now().Add(time.Hour)
		clock := &manualOfferedClock{now: start}
		ctx, cancel := context.WithCancel(context.Background())
		config := offeredConfig{Rate: 1000, Window: time.Second, Deadline: time.Nanosecond, LagLimit: 50 * time.Millisecond, Workers: 1, Queue: 1}
		result := runOffered(ctx, config, clock, func(context.Context, int) error { return nil }, func(time.Time) {
			if canceled {
				cancel()
			}
		})
		cancel()
		for _, outcome := range result.Outcomes {
			if !outcome.Attempted && (outcome.ReadStartNS != -1 || outcome.ServiceNS != -1 || outcome.DispatchNS != -1) {
				t.Fatalf("nonattempted timing: %+v", outcome)
			}
			if !outcome.Admitted && (outcome.EnqueuedNS != -1 || outcome.WorkerStartNS != -1 || outcome.QueueNS != -1) {
				t.Fatalf("rejected timing: %+v", outcome)
			}
			if outcome.ReturnedNS < outcome.ScheduledNS || outcome.LatencyNS != outcome.ReturnedNS-outcome.ScheduledNS {
				t.Fatalf("lifetime: %+v", outcome)
			}
		}
		if result.QueueHighWaterSampled > config.Queue {
			t.Fatal("queue sample exceeds capacity")
		}
	}
}

func TestOfferedFactorialConfig(t *testing.T) {
	base := map[string]string{"P07_OFFERED_LOAD": "1", "P07_OFFERED_RATE": "1000", "P07_READ_DIAGNOSTIC": "1", "P07_READ_INTO": "1", "P07_BACKGROUND_WORKERS": "8", "GOMAXPROCS": "10", "GOGC": "100", "GOMEMLIMIT": "off"}
	get := func(name string) string { return base[name] }
	config, err := parseOfferedConfig(get)
	if err != nil || config.Factorial || config.LoadCase != "both" || config.CPUWorkers != 8 || config.AllocationWorkers != 8 {
		t.Fatalf("historical defaults: %+v %v", config, err)
	}
	base["P07_OFFERED_FACTORIAL"] = "1"
	for name, workers := range map[string][2]int{"none": {0, 0}, "cpu": {8, 0}, "alloc": {0, 8}, "both": {8, 8}} {
		base["P07_OFFERED_CASE"] = name
		config, err := parseOfferedConfig(get)
		if err != nil || !config.Factorial || config.LoadCase != name || config.CPUWorkers != workers[0] || config.AllocationWorkers != workers[1] {
			t.Fatalf("case %s: %+v %v", name, config, err)
		}
	}
	for _, pair := range [][2]string{{"", "cpu"}, {"0", "both"}, {"1", ""}, {"1", "neither"}} {
		base["P07_OFFERED_FACTORIAL"], base["P07_OFFERED_CASE"] = pair[0], pair[1]
		if _, err := parseOfferedConfig(get); err == nil {
			t.Fatalf("invalid factorial accepted: %v", pair)
		}
	}
	delete(base, "P07_OFFERED_LOAD")
	if _, err := parseOfferedConfig(get); err == nil {
		t.Fatal("orphan factorial accepted")
	}
}

func TestFixedFactorialIsolationAndJoin(t *testing.T) {
	for name, workers := range map[string][2]int{"none": {0, 0}, "cpu": {8, 0}, "alloc": {0, 8}, "both": {8, 8}} {
		t.Run(name, func(t *testing.T) {
			start := time.Now().Add(time.Hour)
			clock := &manualOfferedClock{now: start}
			config := offeredConfig{Factorial: true, LoadCase: name, CPUWorkers: workers[0], AllocationWorkers: workers[1], Window: 8 * time.Second, AllocRate: 100, LagLimit: 50 * time.Millisecond}
			arm, stop := startFixedFactorialBackgroundWork(clock, config)
			clock.mu.Lock()
			clock.now = start.Add(9 * time.Second)
			clock.mu.Unlock()
			arm(start)
			stats := stop()
			if stats.CPUWorkers != workers[0] || stats.AllocationWorkers != workers[1] || len(stats.Workers) != workers[0]+workers[1] || stats.Expected != workers[1]*800 || stats.Missed != stats.Expected || stats.Completed != 0 || stats.Invalid != (workers[1] > 0) {
				t.Fatalf("isolation/accounting: %+v", stats)
			}
			if stats.HashIterations != stats.WarmupHashIterations+stats.WindowHashIterations+stats.DrainHashIterations || (workers[0] == 0 && stats.HashIterations != 0) || (workers[0] > 0 && stats.HashIterations < uint64(workers[0])) {
				t.Fatalf("CPU phases: %+v", stats)
			}
			for _, worker := range stats.Workers {
				if worker.Role == "alloc" && worker.HashIterations != 0 || worker.Role == "cpu" && (worker.Expected != 0 || worker.Bytes != 0) {
					t.Fatalf("coupled worker: %+v", worker)
				}
			}
			if again := stop(); again.HashIterations != stats.HashIterations || again.Missed != stats.Missed {
				t.Fatal("stop returned asynchronous results")
			}
		})
	}
}

func TestFixedFactorialAllocationPacing(t *testing.T) {
	start := time.Now().Add(time.Hour)
	finished := make(chan struct{})
	clock := &manualOfferedClock{now: start, end: start.Add(90 * time.Millisecond), finished: finished}
	config := offeredConfig{AllocationWorkers: 1, Window: 100 * time.Millisecond, AllocRate: 100, LagLimit: 50 * time.Millisecond}
	arm, stop := startFixedFactorialBackgroundWork(clock, config)
	arm(start)
	<-finished
	stats := stop()
	if stats.Expected != 10 || stats.Completed+stats.Missed != 10 || stats.Bytes != uint64(stats.Completed)*65536 || stats.HashIterations != 0 || stats.MaxLatenessNS != 0 {
		t.Fatalf("paced allocation: %+v", stats)
	}
	if len(clock.waits) != 10 {
		t.Fatalf("allocation worker did not wait for each tick: %d", len(clock.waits))
	}
	for index, wait := range clock.waits {
		if !wait.Equal(offeredArrival(start, index, 100)) {
			t.Fatal("allocation schedule drifted")
		}
	}
}

type phaseOfferedClock struct {
	mu        sync.Mutex
	now       time.Time
	remaining int
	seen      chan struct{}
}

func (clock *phaseOfferedClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	if clock.remaining > 0 {
		clock.remaining--
		if clock.remaining == 0 {
			close(clock.seen)
		}
	}
	return clock.now
}

func (clock *phaseOfferedClock) Wait(ctx context.Context, until time.Time) error {
	return ctx.Err()
}

func (clock *phaseOfferedClock) phase(at time.Time) <-chan struct{} {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.now = at
	clock.remaining = 20
	clock.seen = make(chan struct{})
	return clock.seen
}

func TestFixedFactorialCPUCompletedPhases(t *testing.T) {
	start := time.Now().Add(time.Hour)
	clock := &phaseOfferedClock{now: start}
	config := offeredConfig{CPUWorkers: 1, Window: 8 * time.Second}
	arm, stop := startFixedFactorialBackgroundWork(clock, config)
	defer stop()
	<-clock.phase(start)
	arm(start)
	<-clock.phase(start.Add(time.Millisecond))
	<-clock.phase(start.Add(9 * time.Second))
	stats := stop()
	if stats.WarmupHashIterations == 0 || stats.WindowHashIterations == 0 || stats.DrainHashIterations == 0 || stats.HashIterations != stats.WarmupHashIterations+stats.WindowHashIterations+stats.DrainHashIterations {
		t.Fatalf("completed CPU work phases: %+v", stats)
	}
	if stats.Expected != 0 || stats.Bytes != 0 {
		t.Fatal("CPU workload allocated payload buffers")
	}
}

type canceledOfferedClock struct{ now time.Time }

func (clock canceledOfferedClock) Now() time.Time { return clock.now }

func (clock canceledOfferedClock) Wait(ctx context.Context, until time.Time) error {
	return ctx.Err()
}

func TestOfferedCancellationBeforeArrival(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	config := offeredConfig{Rate: 10, Window: time.Second, Deadline: time.Second, Workers: 1, Queue: 1}
	result := runOffered(ctx, config, canceledOfferedClock{now: time.Now()}, func(context.Context, int) error { return nil }, func(time.Time) { cancel() })
	if result.Canceled != result.Expected || result.Attempted != 0 {
		t.Fatalf("pre-arrival cancellation: %+v", result)
	}
	for _, outcome := range result.Outcomes[1:] {
		if outcome.DeliveryNS != -1 || outcome.EnqueuedNS != -1 || outcome.WorkerStartNS != -1 || outcome.ReadStartNS != -1 || outcome.ServiceNS != -1 || outcome.ReturnedNS != 0 || outcome.LatencyNS != 0 {
			t.Fatalf("unreached arrival phases: %+v", outcome)
		}
	}
}
