//go:build linux

package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestCollectQualificationPhaseMinimumsAndUnknownRetries(t *testing.T) {
	for _, sample := range []struct {
		name       string
		minimum    qualificationPhase
		operations uint64
	}{
		{"count_first", qualificationPhase{2, 100 * time.Nanosecond, 10}, 10},
		{"time_first", qualificationPhase{10, time.Nanosecond, 10}, 10},
	} {
		t.Run(sample.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			clock := time.Now()
			capture, err := collectQualificationPhase(ctx, 1, sample.minimum,
				func(context.Context, int, uint64) (qualificationOperationResult, error) {
					clock = clock.Add(10 * time.Nanosecond)
					return qualificationOperationResult{}, nil
				}, func() time.Time { return clock })
			if err != nil || capture.SuccessfulOperations != sample.operations || capture.UnexpectedFailures != 0 || capture.Censored != 0 {
				t.Fatalf("capture=%+v err=%v", capture, err)
			}
			if !sample.minimum.complete(capture.SuccessfulOperations, time.Duration(capture.ElapsedNS)) {
				t.Fatalf("premature completion: %+v", capture)
			}
			for ordinal, record := range capture.Records {
				if record.Ordinal != uint64(ordinal) || record.Worker != 0 || record.EndNS-record.StartNS != 10 || !record.Success || record.RetryCount != nil {
					t.Fatalf("malformed record or invented retry count: %+v", record)
				}
			}
		})
	}
}

func TestCollectQualificationPhaseRetainsFailures(t *testing.T) {
	for _, failure := range []error{errors.New("EIO"), context.DeadlineExceeded, context.Canceled} {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		clock := time.Now()
		capture, err := collectQualificationPhase(ctx, 1, qualificationPhase{2, time.Nanosecond, 2},
			func(context.Context, int, uint64) (qualificationOperationResult, error) {
				clock = clock.Add(time.Nanosecond)
				return qualificationOperationResult{}, failure
			}, func() time.Time { return clock })
		cancel()
		if !errors.Is(err, failure) || capture.SuccessfulOperations != 0 || capture.UnexpectedFailures != 1 || len(capture.Records) != 1 {
			t.Fatalf("failure not retained: capture=%+v err=%v", capture, err)
		}
		record := capture.Records[0]
		if record.Error == nil || *record.Error != failure.Error() || record.Success || record.Timeout != errors.Is(failure, context.DeadlineExceeded) {
			t.Fatalf("wrong failure record: %+v", record)
		}
	}
	if _, err := collectQualificationPhase(context.Background(), 1, qualificationPhase{1, time.Second, 1}, nil, time.Now); err == nil {
		t.Fatal("accepted unbounded collection")
	}
}

func TestCollectQualificationPhaseConcurrentWorkers(t *testing.T) {
	const concurrency = 16
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var arrived atomic.Int32
	barrier := make(chan struct{})
	begin := time.Now()
	var elapsed atomic.Int64
	capture, err := collectQualificationPhase(ctx, concurrency, qualificationPhase{3, time.Nanosecond, 3},
		func(ctx context.Context, worker int, ordinal uint64) (qualificationOperationResult, error) {
			if ordinal == 0 {
				if arrived.Add(1) == concurrency {
					close(barrier)
				}
				select {
				case <-barrier:
				case <-ctx.Done():
					return qualificationOperationResult{}, ctx.Err()
				}
			}
			elapsed.Add(1)
			retries := ordinal
			return qualificationOperationResult{RetryCount: &retries}, nil
		}, func() time.Time { return begin.Add(time.Duration(elapsed.Load())) })
	if err != nil || capture.SuccessfulOperations != 3*concurrency || len(capture.Records) != 3*concurrency || capture.UnexpectedFailures != 0 {
		t.Fatalf("concurrent capture=%+v err=%v", capture, err)
	}
	for worker := 0; worker < concurrency; worker++ {
		var preceding int64
		for ordinal := 0; ordinal < 3; ordinal++ {
			record := capture.Records[worker*3+ordinal]
			if record.Worker != worker || record.Ordinal != uint64(ordinal) || record.StartNS < preceding || record.EndNS <= record.StartNS || record.RetryCount == nil || *record.RetryCount != uint64(ordinal) {
				t.Fatalf("invalid concurrent record: %+v", record)
			}
			preceding = record.EndNS
		}
	}
}

func TestCollectQualificationPhaseRecordLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, limit := range []uint64{0, qualificationRecordLimit + 1, ^uint64(0)} {
		if _, err := collectQualificationPhase(ctx, 1, qualificationPhase{1, time.Second, limit},
			func(context.Context, int, uint64) (qualificationOperationResult, error) {
				t.Error("operation called with an invalid record limit")
				return qualificationOperationResult{}, nil
			}, time.Now); err == nil {
			t.Fatalf("accepted invalid record limit %d", limit)
		}
	}
	clock := time.Now()
	capture, err := collectQualificationPhase(ctx, 1, qualificationPhase{1, time.Second, 3},
		func(context.Context, int, uint64) (qualificationOperationResult, error) {
			clock = clock.Add(time.Nanosecond)
			return qualificationOperationResult{}, nil
		}, func() time.Time { return clock })
	if err == nil || !strings.Contains(err.Error(), "record limit") || len(capture.Records) != 3 || capture.SuccessfulOperations != 3 {
		t.Fatalf("record limit not enforced or evidence lost: capture=%+v err=%v", capture, err)
	}
}

func TestCollectQualificationPhaseCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	capture, err := collectQualificationPhase(ctx, 16, qualificationPhase{1, time.Second, 1},
		func(ctx context.Context, worker int, ordinal uint64) (qualificationOperationResult, error) {
			cancel()
			<-ctx.Done()
			return qualificationOperationResult{}, ctx.Err()
		}, time.Now)
	if !errors.Is(err, context.Canceled) || capture.SuccessfulOperations != 0 || len(capture.Records) == 0 || capture.Censored != uint64(len(capture.Records)) {
		t.Fatalf("cancellation capture=%+v err=%v", capture, err)
	}
}

func TestCollectQualificationPhaseResourceBoundary(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	clock := time.Now()
	called := 0
	failure := errors.New("resource snapshot failed")
	capture, err := collectQualificationPhase(ctx, 1, qualificationPhase{1, time.Nanosecond, 1},
		func(context.Context, int, uint64) (qualificationOperationResult, error) {
			clock = clock.Add(time.Nanosecond)
			return qualificationOperationResult{}, nil
		}, func() time.Time { return clock }, func() error {
			called++
			clock = clock.Add(time.Hour)
			return failure
		})
	if !errors.Is(err, failure) || called != 1 || capture.ElapsedNS != 1 || capture.SuccessfulOperations != 1 || len(capture.Records) != 1 {
		t.Fatalf("resource failure/window not preserved: capture=%+v calls=%d err=%v", capture, called, err)
	}
}

func TestQualificationRSSSampler(t *testing.T) {
	for _, interval := range []time.Duration{0, -1, 2 * time.Second} {
		if _, err := startQualificationRSSSampler(interval, func() (uint64, error) { return 1, nil }, time.Now); err == nil {
			t.Fatalf("invalid cadence accepted: %v", interval)
		}
	}
	for _, failedRead := range []bool{false, true} {
		var clock, reads atomic.Int64
		base := time.Now()
		sampled := make(chan struct{})
		sampler, err := startQualificationRSSSampler(time.Millisecond, func() (uint64, error) {
			if reads.Add(1) == 2 {
				close(sampled)
				if failedRead {
					return 0, errors.New("unavailable RSS")
				}
			}
			return 4096, nil
		}, func() time.Time { return base.Add(time.Duration(clock.Add(100))) })
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-sampled:
		case <-time.After(time.Second):
			sampler.stopAndJoin()
			t.Fatal("RSS sampler did not run")
		}
		memory, err := sampler.finish(base.Add(150), 100)
		if failedRead {
			if err == nil {
				t.Fatal("RSS read failure accepted")
			}
		} else if err != nil || len(memory.Samples) < 2 || memory.Idle.AtNS != -50 || memory.Idle.RSSBytes != 4096 {
			t.Fatalf("RSS observations not retained: memory=%+v err=%v", memory, err)
		}
	}
	base := time.Now()
	for _, observations := range [][]qualificationRSSObservation{
		{{base.Add(time.Nanosecond), 1}, {base.Add(10 * time.Millisecond), 1}},
		{{base.Add(-2 * time.Millisecond), 1}, {base.Add(10 * time.Millisecond), 1}},
		{{base.Add(-time.Nanosecond), 1}, {base.Add(10 * time.Millisecond), 1}},
	} {
		sampler := &qualificationRSSSampler{interval: time.Millisecond, stop: make(chan struct{}), done: make(chan struct{}), observed: observations}
		close(sampler.done)
		if _, err := sampler.finish(base, 10*time.Millisecond); err == nil {
			t.Fatal("invalid RSS coverage/gap accepted")
		}
	}
}

func TestCollectQualificationPhaseDeadlineAndCensoring(t *testing.T) {
	for _, step := range []time.Duration{0, -time.Nanosecond} {
		clockParent, clockCancel := context.WithTimeout(context.Background(), time.Second)
		clock := time.Now()
		invalid, invalidErr := collectQualificationPhase(clockParent, 1, qualificationPhase{1, time.Nanosecond, 1},
			func(context.Context, int, uint64) (qualificationOperationResult, error) {
				clock = clock.Add(step)
				return qualificationOperationResult{}, nil
			}, func() time.Time { return clock })
		clockCancel()
		if invalidErr == nil || invalid.SuccessfulOperations != 0 || len(invalid.Records) != 1 || invalid.Records[0].Success {
			t.Fatalf("invalid operation clock accepted: capture=%+v err=%v", invalid, invalidErr)
		}
	}
	budgetParent, budgetCancel := context.WithTimeout(context.Background(), time.Minute)
	defer budgetCancel()
	budgetClock := time.Now()
	budget, budgetErr := collectQualificationPhase(budgetParent, 1, qualificationPhase{1, time.Nanosecond, 1},
		func(context.Context, int, uint64) (qualificationOperationResult, error) {
			budgetClock = budgetClock.Add(time.Nanosecond)
			return qualificationOperationResult{}, nil
		}, func() time.Time { return budgetClock })
	if budgetErr != nil || len(budget.Records) != 1 || budget.Records[0].TimeoutDeadlineNS != int64(30*time.Second) {
		t.Fatalf("operation budget not bounded from recorded start: capture=%+v err=%v", budget, budgetErr)
	}
	parent, parentCancel := context.WithTimeout(context.Background(), time.Second)
	defer parentCancel()
	clock := time.Now()
	operationDeadline := clock.Add(time.Nanosecond)
	late, lateErr := collectQualificationPhase(parent, 1, qualificationPhase{1, time.Nanosecond, 1},
		func(context.Context, int, uint64) (qualificationOperationResult, error) {
			clock = clock.Add(2 * time.Nanosecond)
			return qualificationOperationResult{Deadline: &operationDeadline}, nil
		}, func() time.Time { return clock })
	if !errors.Is(lateErr, context.DeadlineExceeded) || late.SuccessfulOperations != 0 || len(late.Records) != 1 || late.Records[0].TimeoutDeadlineNS != 1 || !late.Records[0].Timeout {
		t.Fatalf("late success was accepted: capture=%+v err=%v", late, lateErr)
	}
	for _, censored := range []bool{false, true} {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second))
		capture, err := collectQualificationPhase(ctx, 1, qualificationPhase{1, time.Second, 1},
			func(context.Context, int, uint64) (qualificationOperationResult, error) {
				if censored {
					return qualificationOperationResult{Censored: true}, nil
				}
				return qualificationOperationResult{}, context.DeadlineExceeded
			}, time.Now)
		cancel()
		if err == nil || capture.SuccessfulOperations != 0 || capture.Censored != 1 || capture.UnexpectedFailures != 1 || len(capture.Records) != 1 {
			t.Fatalf("incomplete outcome lost: capture=%+v err=%v", capture, err)
		}
		if capture.Records[0].Timeout != !censored || capture.Records[0].TimeoutDeadlineNS <= 0 {
			t.Fatalf("invalid deadline outcome: %+v", capture.Records[0])
		}
	}
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	capture, err := collectQualificationPhase(ctx, 1, qualificationPhase{1, time.Second, 1},
		func(context.Context, int, uint64) (qualificationOperationResult, error) {
			t.Error("started an operation after deadline")
			return qualificationOperationResult{}, nil
		}, time.Now)
	if !errors.Is(err, context.DeadlineExceeded) || len(capture.Records) != 0 {
		t.Fatalf("expired collection not rejected: capture=%+v err=%v", capture, err)
	}
	bounded, boundedCancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer boundedCancel()
	capture, err = collectQualificationPhase(bounded, 16, qualificationPhase{1, time.Second, 1},
		func(ctx context.Context, worker int, ordinal uint64) (qualificationOperationResult, error) {
			<-ctx.Done()
			return qualificationOperationResult{}, ctx.Err()
		}, time.Now)
	if !errors.Is(err, context.DeadlineExceeded) || capture.SuccessfulOperations != 0 || capture.Censored != uint64(len(capture.Records)) {
		t.Fatalf("active deadline collection not rejected: capture=%+v err=%v", capture, err)
	}
}

func TestQualificationPhaseRequiresBothMinimums(t *testing.T) {
	for _, concurrency := range []int{1, 16, 64, 256} {
		warmup, measured, err := qualificationPhases(concurrency)
		if err != nil {
			t.Fatal(err)
		}
		for _, phase := range []qualificationPhase{warmup, measured} {
			if phase.minimumOperations*uint64(concurrency) < 10000 || phase.minimumOperations == 0 {
				t.Fatalf("invalid per-worker population: %+v", phase)
			}
			if phase.maximumOperations < phase.minimumOperations || phase.maximumOperations*uint64(concurrency) > 1000000 {
				t.Fatalf("invalid record budget: %+v", phase)
			}
			for _, sample := range []struct {
				operations uint64
				elapsed    time.Duration
				complete   bool
			}{
				{phase.minimumOperations - 1, phase.minimumDuration + time.Second, false},
				{phase.minimumOperations, phase.minimumDuration - time.Nanosecond, false},
				{phase.minimumOperations + 1, -time.Second, false},
				{phase.minimumOperations, phase.minimumDuration, true},
			} {
				if actual := phase.complete(sample.operations, sample.elapsed); actual != sample.complete {
					t.Fatalf("concurrency=%d phase=%+v sample=%+v got=%v", concurrency, phase, sample, actual)
				}
			}
		}
		if measured.minimumOperations*uint64(concurrency) < 100000 {
			t.Fatal("measured population below contract")
		}
	}
	for _, concurrency := range []int{-1, 0, 257} {
		if _, _, err := qualificationPhases(concurrency); err == nil {
			t.Fatalf("accepted concurrency %d", concurrency)
		}
	}
}

func TestCollectQualificationAttemptSmoke(t *testing.T) {
	for _, stage := range []string{"success", "setup", "warmup", "measured", "verify", "cleanup"} {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			clock := time.Now()
			failure := errors.New(stage)
			var calls, cleanups int
			actions := qualificationActions{
				prepare: func(context.Context) error {
					if stage == "setup" {
						return failure
					}
					return nil
				},
				operation: func(context.Context, int, uint64) (qualificationOperationResult, error) {
					calls++
					clock = clock.Add(time.Nanosecond)
					if stage == "warmup" && calls == 2 || stage == "measured" && calls == 4 {
						return qualificationOperationResult{}, failure
					}
					return qualificationOperationResult{}, nil
				},
				verify: func(context.Context) error {
					if stage == "verify" {
						return failure
					}
					return nil
				},
				cleanup: func(ctx context.Context) error {
					cleanups++
					if _, bounded := ctx.Deadline(); !bounded || ctx.Err() != nil {
						t.Fatal("cleanup context is not independently bounded and live")
					}
					if stage == "cleanup" {
						return failure
					}
					return nil
				},
			}
			attempt, err := collectQualificationAttempt(ctx, 1, qualificationPhase{2, time.Nanosecond, 2}, qualificationPhase{3, time.Nanosecond, 3}, actions, func() time.Time { return clock })
			if cleanups != 1 || attempt.CleanupVerified != (stage != "cleanup") || (stage == "success") != (err == nil) || err != nil && !errors.Is(err, failure) {
				t.Fatalf("attempt=%+v err=%v cleanups=%d", attempt, err, cleanups)
			}
			if stage == "warmup" && (len(attempt.Warmup.Records) != 2 || len(attempt.Measured.Records) != 0) {
				t.Fatal("warmup failure was lost or measured work ran")
			}
			if stage == "measured" && (len(attempt.Warmup.Records) != 2 || len(attempt.Measured.Records) != 2) {
				t.Fatal("measured failure evidence was lost")
			}
			if stage == "success" && (calls != 5 || attempt.Warmup.SuccessfulOperations != 2 || attempt.Measured.SuccessfulOperations != 3 || !attempt.PayloadVerified) {
				t.Fatal("wrong successful smoke population")
			}
		})
	}
}

func TestParityFixtureContract(t *testing.T) {
	for _, value := range []string{"", "p07-parity-20261002", "p07-parity-" + strings.Repeat("a", 53)} {
		actual, err := parityNamespace(value)
		if err != nil || actual != value {
			t.Fatalf("namespace %q: got %q, %v", value, actual, err)
		}
	}
	for _, value := range []string{"default", "p07-parity-/escape", "p07-parity-ABC", "p07-parity-" + strings.Repeat("a", 54)} {
		if _, err := parityNamespace(value); err == nil {
			t.Fatalf("accepted invalid namespace %q", value)
		}
	}
	if actual := parityObjectName(1048576, 16, "mixed", 3); actual != "p07-parity-1048576-c16-mixed-w3" {
		t.Fatalf("unexpected fixture name %q", actual)
	}
}

func TestParityPayloadMatchesNativeWorkerSeed(t *testing.T) {
	t.Setenv("P07_READ_DIAGNOSTIC", "")
	t.Setenv("P07_SEED_ONLY", "")
	t.Setenv("P07_PARITY_NAMESPACE", "p07-parity-payload-check")
	for _, worker := range []uint64{0, 1, 15, 255} {
		state := uint64(1) ^ worker<<19 ^ uint64(0x9E3779B97F4A7C15)
		expected := make([]byte, 4096)
		for index := range expected {
			state ^= state << 7
			state ^= state >> 9
			state ^= state << 8
			expected[index] = byte(state)
		}
		if !bytes.Equal(makePayload(4096, 1, worker), expected) {
			t.Fatalf("parity payload differs from native worker %d", worker)
		}
	}
	matched := makePayload(4096, 1, 1)
	t.Setenv("P07_PARITY_NAMESPACE", "")
	if bytes.Equal(makePayload(4096, 1, 1), matched) {
		t.Fatal("ordinary benchmark payload seed was unintentionally changed")
	}
}

func TestParseQualificationConfig(t *testing.T) {
	if config, err := parseQualificationConfig(func(string) string { return "" }); err != nil || config != nil {
		t.Fatalf("disabled configuration=%+v err=%v", config, err)
	}
	valid := map[string]string{
		"P07_QUALIFICATION_FILE": "/private/attempt.json", "P07_QUALIFICATION_ROUND": "1", "P07_QUALIFICATION_LEG": "r1-l1-go", "P07_QUALIFICATION_SEED": "42",
		"P07_MODE_EVIDENCE_FILE": "/private/modes.json", "P07_PARITY_NAMESPACE": "p07-parity-qualification", "P07_MATRIX_SIZE": "4096", "P07_MATRIX_CONCURRENCY": "16", "P07_MATRIX_WORKLOAD": "mixed",
		"P07_READ_INTO": "1", "GOMAXPROCS": "10", "GOGC": "100", "GOMEMLIMIT": "off",
	}
	env := func(name string) string { return valid[name] }
	config, err := parseQualificationConfig(env)
	if err != nil || config.Round != 1 || config.Leg != "r1-l1-go" || config.Seed != 42 || config.file != valid["P07_QUALIFICATION_FILE"] {
		t.Fatalf("configuration=%+v err=%v", config, err)
	}
	valid["P07_PGO_DIAGNOSTIC"] = "1"
	valid["P07_PGO_PROFILE_FILE"] = "/private/training.pprof"
	if diagnostic, err := parseQualificationConfig(env); err != nil || !diagnostic.pgoDiagnostic {
		t.Fatalf("explicit PGO diagnostic configuration=%+v err=%v", diagnostic, err)
	}
	valid["P07_PGO_PROFILE_FILE"] = "/private/./modes.json"
	if _, err := parseQualificationConfig(env); err == nil {
		t.Fatal("training profile allowed to overwrite mode evidence")
	}
	valid["P07_PGO_PROFILE_FILE"] = "/private/training.pprof"
	delete(valid, "P07_PGO_DIAGNOSTIC")
	if _, err := parseQualificationConfig(env); err == nil {
		t.Fatal("training profile allowed outside PGO diagnostic mode")
	}
	delete(valid, "P07_PGO_PROFILE_FILE")
	for _, sample := range []struct{ name, value string }{
		{"P07_QUALIFICATION_FILE", ""}, {"P07_QUALIFICATION_ROUND", "0"}, {"P07_QUALIFICATION_ROUND", "9007199254740992"}, {"P07_QUALIFICATION_SEED", ""}, {"P07_QUALIFICATION_SEED", "9007199254740992"},
		{"P07_QUALIFICATION_LEG", "../escape"}, {"P07_PARITY_NAMESPACE", ""}, {"P07_READ_INTO", "0"}, {"P07_MODE_EVIDENCE_FILE", ""}, {"P07_MODE_EVIDENCE_FILE", "/private/./attempt.json"},
		{"GOMAXPROCS", "16"}, {"GOGC", "off"}, {"GOMEMLIMIT", "1GiB"}, {"P07_MATRIX_SIZE", ""}, {"P07_MATRIX_CONCURRENCY", ""}, {"P07_MATRIX_WORKLOAD", ""},
		{"P07_MATRIX_OPERATIONS_PER_WORKER", "256"}, {"P07_RETENTION_WINDOWS", "16"}, {"P07_CPU_PROFILE", "/private/profile"}, {"P07_OFFERED_LOAD", "1"}, {"P07_SCRATCH_SLOTS", "1"},
		{"P07_PGO_DIAGNOSTIC", "0"},
		{"P07_PGO_PROFILE_FILE", "/private/profile.pprof"},
	} {
		modified := func(name string) string {
			if name == sample.name {
				return sample.value
			}
			return valid[name]
		}
		if _, err := parseQualificationConfig(modified); err == nil {
			t.Fatalf("accepted invalid %s=%q", sample.name, sample.value)
		}
	}
}

func TestPGODiagnosticPhasesDoNotChangeQualification(t *testing.T) {
	for _, concurrency := range []int{1, 16, 256} {
		warmup, measured, err := pgoDiagnosticPhases(concurrency)
		if err != nil || warmup.minimumDuration != time.Second || measured.minimumDuration != 8*time.Second || warmup.minimumOperations*uint64(concurrency) < 1000 || measured.minimumOperations*uint64(concurrency) < 10000 {
			t.Fatalf("diagnostic phases=%+v/%+v err=%v", warmup, measured, err)
		}
		warmup, measured, err = qualificationPhases(concurrency)
		if err != nil || warmup.minimumDuration != 10*time.Second || measured.minimumDuration != 60*time.Second || warmup.minimumOperations*uint64(concurrency) < 10000 || measured.minimumOperations*uint64(concurrency) < 100000 {
			t.Fatal("PGO diagnostic changed qualification minima")
		}
	}
}
