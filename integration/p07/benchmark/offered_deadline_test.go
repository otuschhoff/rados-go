package main

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestOfferedActiveDeadlineExpiration(t *testing.T) {
	for _, returnNil := range []bool{false, true} {
		name := "context_error"
		if returnNil {
			name = "nil_error"
		}
		t.Run(name, func(t *testing.T) {
			testOfferedActiveContextCompletion(t, false, returnNil)
		})
	}
}

func TestOfferedActiveCancellation(t *testing.T) {
	for _, returnNil := range []bool{false, true} {
		name := "context_error"
		if returnNil {
			name = "nil_error"
		}
		t.Run(name, func(t *testing.T) {
			testOfferedActiveContextCompletion(t, true, returnNil)
		})
	}
}

func testOfferedActiveContextCompletion(t *testing.T, cancelActive, returnNil bool) {
	t.Helper()
	watchdog, stopWatchdog := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopWatchdog()
	ctx, cancel := context.WithCancel(watchdog)
	defer cancel()
	config := offeredConfig{Rate: 100, Window: 10 * time.Millisecond, Deadline: 30 * time.Millisecond, LagLimit: 50 * time.Millisecond, Workers: 1, Queue: 1}
	started := make(chan time.Time, 1)
	entered := make(chan time.Time, 1)
	contextDone := make(chan error, 1)
	release := make(chan struct{})
	exited := make(chan struct{})
	results := make(chan offeredResult, 1)
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	var calls, active atomic.Int32
	var destination [64]byte
	go func() {
		results <- runOffered(ctx, config, realOfferedClock{}, func(readCtx context.Context, worker int) error {
			if calls.Add(1) <= readWarmupPerWorker {
				return nil
			}
			active.Add(1)
			defer close(exited)
			defer active.Add(-1)
			deadline, _ := readCtx.Deadline()
			entered <- deadline
			<-readCtx.Done()
			contextDone <- readCtx.Err()
			<-release
			for index := range destination {
				destination[index] = byte(index + 1)
			}
			if returnNil {
				return nil
			}
			return readCtx.Err()
		}, func(start time.Time) { started <- start })
	}()
	var deadline time.Time
	select {
	case deadline = <-entered:
	case result := <-results:
		t.Fatalf("runner returned without an active read: %+v", result)
	case <-watchdog.Done():
		t.Fatal("active read did not enter")
	}
	start := <-started
	if !deadline.Equal(start.Add(config.Deadline)) {
		t.Fatalf("active deadline = %v, want %v", deadline, start.Add(config.Deadline))
	}
	wantErr, wantKind := context.DeadlineExceeded, "timeout"
	if cancelActive {
		cancel()
		wantErr, wantKind = context.Canceled, "canceled"
	}
	select {
	case err := <-contextDone:
		if !errors.Is(err, wantErr) {
			t.Fatalf("active context error = %v, want %v", err, wantErr)
		}
	case <-watchdog.Done():
		t.Fatal("active read did not observe context completion")
	}
	select {
	case result := <-results:
		t.Fatalf("runner returned while reader still owned destination: %+v", result)
	default:
	}
	if active.Load() != 1 {
		t.Fatal("reader was not active before release")
	}
	unblock()
	var result offeredResult
	select {
	case result = <-results:
	case <-watchdog.Done():
		t.Fatal("runner did not join the released reader")
	}
	select {
	case <-exited:
	default:
		t.Fatal("runner returned before reader exit")
	}
	if active.Load() != 0 || calls.Load() != readWarmupPerWorker+1 {
		t.Fatalf("reader lifecycle: active=%d calls=%d", active.Load(), calls.Load())
	}
	for index, value := range destination {
		if value != byte(index+1) {
			t.Fatalf("destination[%d] = %d before reader joined", index, value)
		}
	}
	if result.Expected != 1 || result.Admitted != 1 || result.Attempted != 1 || len(result.Outcomes) != 1 || result.Success != 0 || result.Errors != 0 || result.Overload != 0 || result.SLOFailures != 1 || !result.Failed {
		t.Fatalf("active completion accounting: %+v", result)
	}
	if cancelActive && (result.Canceled != 1 || result.Timeouts != 0) || !cancelActive && (result.Timeouts != 1 || result.Canceled != 0) {
		t.Fatalf("active %s accounting: %+v", wantKind, result)
	}
	outcome := result.Outcomes[0]
	if outcome.Kind != wantKind || !outcome.Admitted || !outcome.Attempted || outcome.ReadStartNS < 0 || outcome.ServiceNS < 0 || outcome.ServiceNS != outcome.ReturnedNS-outcome.ReadStartNS || outcome.LatencyNS != outcome.ReturnedNS-outcome.ScheduledNS {
		t.Fatalf("active %s outcome: %+v", wantKind, outcome)
	}
	if outcome.DeadlineMiss != (outcome.LatencyNS >= int64(config.Deadline)) || !cancelActive && !outcome.DeadlineMiss {
		t.Fatalf("active %s deadline miss: %+v", wantKind, outcome)
	}
}
