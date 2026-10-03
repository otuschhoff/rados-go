//go:build linux

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	rados "github.com/otuschhoff/rados-go"
	"github.com/otuschhoff/rados-go/internal/msgr"
)

const qualificationRecordLimit = 1000000

type qualificationPhase struct {
	minimumOperations uint64
	minimumDuration   time.Duration
	maximumOperations uint64
}

func qualificationPhases(concurrency int) (qualificationPhase, qualificationPhase, error) {
	if concurrency < 1 || concurrency > 256 {
		return qualificationPhase{}, qualificationPhase{}, fmt.Errorf("qualification concurrency must be 1..256")
	}
	workers := uint64(concurrency)
	warmup := qualificationPhase{minimumOperations: (10000 + workers - 1) / workers, minimumDuration: 10 * time.Second, maximumOperations: qualificationRecordLimit / workers}
	measured := qualificationPhase{minimumOperations: (100000 + workers - 1) / workers, minimumDuration: 60 * time.Second, maximumOperations: qualificationRecordLimit / workers}
	return warmup, measured, nil
}

func (phase qualificationPhase) complete(successful uint64, elapsed time.Duration) bool {
	return successful >= phase.minimumOperations && elapsed >= phase.minimumDuration
}

func pgoDiagnosticPhases(concurrency int) (qualificationPhase, qualificationPhase, error) {
	warmup, measured, err := qualificationPhases(concurrency)
	if err != nil {
		return warmup, measured, err
	}
	workers := uint64(concurrency)
	warmup.minimumOperations, warmup.minimumDuration = (1000+workers-1)/workers, time.Second
	measured.minimumOperations, measured.minimumDuration = (10000+workers-1)/workers, 8*time.Second
	return warmup, measured, nil
}

type qualificationOperation struct {
	Round             int     `json:"round"`
	Leg               string  `json:"leg"`
	OperationID       string  `json:"operation_id"`
	Object            string  `json:"object"`
	Type              string  `json:"type"`
	Worker            int     `json:"worker"`
	Ordinal           uint64  `json:"ordinal"`
	StartNS           int64   `json:"start_ns"`
	EndNS             int64   `json:"end_ns"`
	Success           bool    `json:"success"`
	Error             *string `json:"error"`
	Timeout           bool    `json:"timeout"`
	Censored          bool    `json:"censored"`
	RetryCount        *uint64 `json:"retry_count"`
	TimeoutDeadlineNS int64   `json:"timeout_deadline_ns"`
}

type qualificationOperationResult struct {
	RetryCount *uint64
	Censored   bool
	Deadline   *time.Time
}

type qualificationPhaseCapture struct {
	begin                time.Time
	ElapsedNS            int64                    `json:"elapsed_ns"`
	SuccessfulOperations uint64                   `json:"successful_operations"`
	UnexpectedFailures   uint64                   `json:"unexpected_failures"`
	Censored             uint64                   `json:"censored"`
	OperationsPerWorker  []uint64                 `json:"operations_per_worker"`
	Records              []qualificationOperation `json:"records"`
}

func collectQualificationPhase(parent context.Context, concurrency int, phase qualificationPhase,
	operation func(context.Context, int, uint64) (qualificationOperationResult, error), now func() time.Time,
	after ...func() error,
) (qualificationPhaseCapture, error) {
	var capture qualificationPhaseCapture
	if _, _, err := qualificationPhases(concurrency); err != nil {
		return capture, err
	}
	deadline, bounded := parent.Deadline()
	if !bounded || phase.minimumOperations == 0 || phase.minimumDuration <= 0 || phase.maximumOperations < phase.minimumOperations || phase.maximumOperations > qualificationRecordLimit/uint64(concurrency) || operation == nil || now == nil || len(after) > 1 || len(after) == 1 && after[0] == nil {
		return capture, fmt.Errorf("qualification collection requires a deadline, positive minima, a sufficient record limit and operation/clock hooks")
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	workers := make([][]qualificationOperation, concurrency)
	start := make(chan struct{})
	var ready, finished sync.WaitGroup
	var first sync.Once
	var failure error
	var begin time.Time
	ready.Add(concurrency)
	finished.Add(concurrency)
	for worker := 0; worker < concurrency; worker++ {
		go func(worker int) {
			defer finished.Done()
			ready.Done()
			<-start
			var successful uint64
			for !phase.complete(successful, now().Sub(begin)) {
				if err := ctx.Err(); err != nil {
					first.Do(func() { failure = err })
					return
				}
				ordinal := uint64(len(workers[worker]))
				if ordinal >= phase.maximumOperations {
					first.Do(func() {
						failure = fmt.Errorf("qualification record limit reached for worker %d before both minima", worker)
						cancel()
					})
					return
				}
				started := now()
				result, err := operation(ctx, worker, ordinal)
				ended := now()
				if !ended.After(started) || started.Before(begin) {
					err = errors.Join(err, fmt.Errorf("invalid qualification operation clock"))
				}
				operationDeadline := deadline
				if maximum := started.Add(30 * time.Second); maximum.Before(operationDeadline) {
					operationDeadline = maximum
				}
				if result.Deadline != nil && result.Deadline.Before(operationDeadline) {
					operationDeadline = *result.Deadline
				}
				if err == nil && ended.After(operationDeadline) {
					err = context.DeadlineExceeded
				}
				record := qualificationOperation{Worker: worker, Ordinal: ordinal,
					StartNS: started.Sub(begin).Nanoseconds(), EndNS: ended.Sub(begin).Nanoseconds(),
					Success: err == nil && !result.Censored, Censored: result.Censored,
					TimeoutDeadlineNS: operationDeadline.Sub(begin).Nanoseconds()}
				if result.RetryCount != nil {
					count := *result.RetryCount
					record.RetryCount = &count
				}
				if err != nil {
					message := err.Error()
					record.Error = &message
					record.Timeout = errors.Is(err, context.DeadlineExceeded)
					record.Censored = record.Censored || record.Timeout || errors.Is(err, context.Canceled)
				}
				workers[worker] = append(workers[worker], record)
				if err != nil || record.Censored {
					if err == nil {
						err = fmt.Errorf("censored qualification operation")
					}
					first.Do(func() { failure = err; cancel() })
					return
				}
				successful++
			}
		}(worker)
	}
	ready.Wait()
	begin = now()
	capture.begin = begin
	close(start)
	finished.Wait()
	capture.ElapsedNS = now().Sub(begin).Nanoseconds()
	if capture.ElapsedNS < 0 {
		failure = errors.Join(failure, fmt.Errorf("invalid qualification phase clock"))
	}
	if len(after) == 1 {
		failure = errors.Join(failure, after[0]())
	}
	for _, records := range workers {
		capture.OperationsPerWorker = append(capture.OperationsPerWorker, uint64(len(records)))
		for _, record := range records {
			if record.Success {
				capture.SuccessfulOperations++
			} else {
				capture.UnexpectedFailures++
			}
			if record.Censored {
				capture.Censored++
			}
		}
		capture.Records = append(capture.Records, records...)
	}
	return capture, failure
}

type qualificationActions struct {
	prepare        func(context.Context) error
	operation      func(context.Context, int, uint64) (qualificationOperationResult, error)
	beforeMeasured func() error
	afterMeasured  func() error
	verify         func(context.Context) error
	cleanup        func(context.Context) error
}

type qualificationAttempt struct {
	Warmup          qualificationPhaseCapture `json:"warmup"`
	Measured        qualificationPhaseCapture `json:"measured"`
	PayloadVerified bool                      `json:"payload_verified"`
	CleanupVerified bool                      `json:"cleanup_verified"`
	Memory          *qualificationMemory      `json:"memory,omitempty"`
}

type qualificationRSSSample struct {
	AtNS     int64  `json:"at_ns"`
	RSSBytes uint64 `json:"rss_bytes"`
}

type qualificationMemory struct {
	Clock      string                   `json:"clock"`
	IntervalNS int64                    `json:"interval_ns"`
	Idle       qualificationIdleMemory  `json:"idle"`
	Samples    []qualificationRSSSample `json:"samples"`
}

type qualificationIdleMemory struct {
	Connected     bool   `json:"connected"`
	EquallyWarmed bool   `json:"equally_warmed"`
	AtNS          int64  `json:"at_ns"`
	RSSBytes      uint64 `json:"rss_bytes"`
}

type qualificationRSSObservation struct {
	at    time.Time
	bytes uint64
}

type qualificationRSSSampler struct {
	interval time.Duration
	read     func() (uint64, error)
	now      func() time.Time
	stop     chan struct{}
	stopOnce sync.Once
	done     chan struct{}
	observed []qualificationRSSObservation
	baseline qualificationRSSObservation
	err      error
}

func startQualificationRSSSampler(interval time.Duration, read func() (uint64, error), now func() time.Time) (*qualificationRSSSampler, error) {
	if interval <= 0 || interval > time.Second || read == nil || now == nil {
		return nil, fmt.Errorf("qualification RSS requires bounded cadence and read/clock hooks")
	}
	sampler := &qualificationRSSSampler{interval: interval, read: read, now: now, stop: make(chan struct{}), done: make(chan struct{})}
	if err := sampler.observe(); err != nil {
		return nil, err
	}
	sampler.baseline = sampler.observed[0]
	go func() {
		defer close(sampler.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-sampler.stop:
				sampler.err = errors.Join(sampler.err, sampler.observe())
				return
			case <-ticker.C:
				if err := sampler.observe(); err != nil {
					sampler.err = err
					<-sampler.stop
					return
				}
			}
		}
	}()
	return sampler, nil
}

func (sampler *qualificationRSSSampler) observe() error {
	if len(sampler.observed) >= 10000 {
		return fmt.Errorf("qualification RSS sample limit reached")
	}
	at := sampler.now()
	value, err := sampler.read()
	if err != nil || value == 0 {
		return errors.Join(err, fmt.Errorf("qualification RSS observation unavailable"))
	}
	if len(sampler.observed) > 0 && !at.After(sampler.observed[len(sampler.observed)-1].at) {
		return fmt.Errorf("qualification RSS clock is not increasing")
	}
	sampler.observed = append(sampler.observed, qualificationRSSObservation{at: at, bytes: value})
	return nil
}

func (sampler *qualificationRSSSampler) stopAndJoin() {
	sampler.stopOnce.Do(func() { close(sampler.stop) })
	<-sampler.done
}

func (sampler *qualificationRSSSampler) finish(begin time.Time, elapsed time.Duration) (*qualificationMemory, error) {
	sampler.stopAndJoin()
	memory := &qualificationMemory{Clock: "measurement_relative_ns", IntervalNS: sampler.interval.Nanoseconds()}
	for _, observed := range sampler.observed {
		memory.Samples = append(memory.Samples, qualificationRSSSample{AtNS: observed.at.Sub(begin).Nanoseconds(), RSSBytes: observed.bytes})
	}
	if len(memory.Samples) > 0 {
		first := memory.Samples[0]
		memory.Idle = qualificationIdleMemory{Connected: true, EquallyWarmed: true, AtNS: first.AtNS, RSSBytes: first.RSSBytes}
	}
	err := sampler.err
	if begin.IsZero() || len(memory.Samples) < 2 || memory.Samples[0].AtNS > 0 || memory.Samples[0].AtNS < -memory.IntervalNS {
		err = errors.Join(err, fmt.Errorf("qualification RSS baseline/start coverage unavailable"))
	}
	for index := 1; index < len(memory.Samples); index++ {
		if memory.Samples[index].AtNS-memory.Samples[index-1].AtNS > 2*memory.IntervalNS {
			err = errors.Join(err, fmt.Errorf("qualification RSS sampling gap"))
		}
	}
	if len(memory.Samples) > 0 {
		last := memory.Samples[len(memory.Samples)-1].AtNS
		if last < elapsed.Nanoseconds() || last > elapsed.Nanoseconds()+memory.IntervalNS {
			err = errors.Join(err, fmt.Errorf("qualification RSS end coverage unavailable"))
		}
	}
	return memory, err
}

func collectQualificationAttempt(ctx context.Context, concurrency int, warmup, measured qualificationPhase,
	actions qualificationActions, now func() time.Time,
) (attempt qualificationAttempt, resultErr error) {
	if _, bounded := ctx.Deadline(); !bounded || actions.prepare == nil || actions.operation == nil || actions.verify == nil || actions.cleanup == nil || now == nil {
		return attempt, fmt.Errorf("qualification attempt requires bounded context and all workload hooks")
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		cleanupErr := actions.cleanup(cleanupCtx)
		attempt.CleanupVerified = cleanupErr == nil
		if cleanupErr != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("qualification cleanup: %w", cleanupErr))
		}
	}()
	if err := actions.prepare(ctx); err != nil {
		return attempt, fmt.Errorf("qualification setup: %w", err)
	}
	var err error
	attempt.Warmup, err = collectQualificationPhase(ctx, concurrency, warmup, actions.operation, now)
	if err != nil {
		return attempt, fmt.Errorf("qualification warmup: %w", err)
	}
	if actions.beforeMeasured != nil {
		if err := actions.beforeMeasured(); err != nil {
			return attempt, fmt.Errorf("qualification measured baseline: %w", err)
		}
	}
	var after []func() error
	if actions.afterMeasured != nil {
		after = append(after, actions.afterMeasured)
	}
	attempt.Measured, err = collectQualificationPhase(ctx, concurrency, measured, actions.operation, now, after...)
	if err != nil {
		return attempt, fmt.Errorf("qualification measured phase: %w", err)
	}
	if err := actions.verify(ctx); err != nil {
		return attempt, fmt.Errorf("qualification payload verification: %w", err)
	}
	attempt.PayloadVerified = true
	return attempt, nil
}

type qualificationConfig struct {
	file          string
	pgoDiagnostic bool
	profileFile   string
	Round         int    `json:"round"`
	Leg           string `json:"leg"`
	Seed          uint64 `json:"seed"`
}

func parseQualificationConfig(env func(string) string) (*qualificationConfig, error) {
	file := env("P07_QUALIFICATION_FILE")
	if file == "" {
		for _, name := range []string{"P07_QUALIFICATION_ROUND", "P07_QUALIFICATION_LEG", "P07_QUALIFICATION_SEED", "P07_PGO_DIAGNOSTIC", "P07_PGO_PROFILE_FILE"} {
			if env(name) != "" {
				return nil, fmt.Errorf("%s requires P07_QUALIFICATION_FILE", name)
			}
		}
		return nil, nil
	}
	pgoDiagnostic := env("P07_PGO_DIAGNOSTIC")
	if pgoDiagnostic != "" && pgoDiagnostic != "1" {
		return nil, fmt.Errorf("P07_PGO_DIAGNOSTIC must be explicitly 1")
	}
	for _, name := range []string{"P07_READ_DIAGNOSTIC", "P07_SEED_ONLY", "P07_OFFERED_LOAD", "P07_RETENTION_WINDOWS", "P07_BACKGROUND_WORKERS", "P07_CPU_PROFILE", "P07_MEMORY_PROFILE", "P07_TRACE_FILE", "P07_RESOURCE_FILE", "P07_TIMING_FILE", "P07_MATRIX_OPERATIONS_PER_WORKER", "P07_OPERATIONS_PER_WORKER", "P07_SCRATCH_SLOTS", "P07_ADMISSION_WINDOW", "P07_BACKGROUND_ALLOCATIONS"} {
		if env(name) != "" {
			return nil, fmt.Errorf("qualification cannot use %s", name)
		}
	}
	namespace, err := parityNamespace(env("P07_PARITY_NAMESPACE"))
	if err != nil || namespace == "" || env("P07_READ_INTO") != "1" || env("GOMAXPROCS") != "10" || env("GOGC") != "100" || env("GOMEMLIMIT") != "off" {
		return nil, fmt.Errorf("qualification requires a parity namespace, ReadInto and explicit P10/GOGC100/GOMEMLIMIToff")
	}
	matrix, err := parseMatrixExperiment(env, false)
	if err != nil || matrix.size == 0 || matrix.concurrency == 0 || matrix.workload == "" {
		return nil, fmt.Errorf("qualification requires explicit matrix size, concurrency and workload")
	}
	modeFile := env("P07_MODE_EVIDENCE_FILE")
	outputPath, outputErr := filepath.Abs(file)
	modePath, modeErr := filepath.Abs(modeFile)
	if modeFile == "" || outputErr != nil || modeErr != nil || outputPath == modePath {
		return nil, fmt.Errorf("qualification requires a distinct fresh mode evidence file")
	}
	profileFile := env("P07_PGO_PROFILE_FILE")
	if profileFile != "" {
		profilePath, err := filepath.Abs(profileFile)
		if pgoDiagnostic != "1" || err != nil || profilePath == outputPath || profilePath == modePath {
			return nil, fmt.Errorf("PGO training profile requires diagnostic mode and a distinct fresh file")
		}
	}
	round, err := strconv.Atoi(env("P07_QUALIFICATION_ROUND"))
	if err != nil || round < 1 || uint64(round) > 9007199254740991 {
		return nil, fmt.Errorf("qualification round must be a positive safe integer")
	}
	seed, err := strconv.ParseUint(env("P07_QUALIFICATION_SEED"), 10, 53)
	if err != nil {
		return nil, fmt.Errorf("qualification seed must be an unsigned safe integer")
	}
	leg := env("P07_QUALIFICATION_LEG")
	if len(leg) < 1 || len(leg) > 80 {
		return nil, fmt.Errorf("qualification leg must fit 1..80 lowercase ASCII characters")
	}
	for _, character := range leg {
		if character != '-' && (character < 'a' || character > 'z') && (character < '0' || character > '9') {
			return nil, fmt.Errorf("qualification leg must contain lowercase ASCII letters, digits and hyphens")
		}
	}
	return &qualificationConfig{file: file, pgoDiagnostic: pgoDiagnostic == "1", profileFile: profileFile, Round: round, Seed: seed, Leg: leg}, nil
}

func runQualification(parent context.Context, pool rados.Pool, config qualificationConfig, matrix matrixExperiment) (resultErr error) {
	file, err := os.OpenFile(config.file, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("create fresh qualification capture: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, file.Close()) }()
	var profileFile *os.File
	profileActive := false
	stopProfile := func() error {
		if profileActive {
			pprof.StopCPUProfile()
			profileActive = false
		}
		if profileFile != nil {
			err := profileFile.Close()
			profileFile = nil
			return err
		}
		return nil
	}
	defer func() { resultErr = errors.Join(resultErr, stopProfile()) }()
	if config.profileFile != "" {
		profileFile, err = os.OpenFile(config.profileFile, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
	}
	ctx, cancel := context.WithTimeout(parent, 15*time.Minute)
	defer cancel()
	warmup, measured, err := qualificationPhases(matrix.concurrency)
	if config.pgoDiagnostic {
		warmup, measured, err = pgoDiagnosticPhases(matrix.concurrency)
	}
	if err != nil {
		return err
	}
	objects := make([]rados.ObjectRef, matrix.concurrency)
	owned := make([]bool, matrix.concurrency)
	payloads, destinations := make([][]byte, matrix.concurrency), make([][]byte, matrix.concurrency)
	for worker := range objects {
		objects[worker] = pool.Object(parityObjectName(matrix.size, matrix.concurrency, matrix.workload, worker))
		payloads[worker] = makePayload(matrix.size, 1, uint64(worker))
		destinations[worker] = make([]byte, int(matrix.size))
	}
	var measuredBefore resourceSnapshot
	var windowStart, windowEnd time.Time
	var rssSampler *qualificationRSSSampler
	var measuredResources resources
	var rssBefore, rssAfter, rssCleanup uint64
	actions := qualificationActions{
		prepare: func(ctx context.Context) error {
			for worker, object := range objects {
				if _, err := object.Stat(ctx); !errors.Is(err, rados.ErrNotFound) {
					return fmt.Errorf("qualification requires an absent fixture for worker %d: %v", worker, err)
				}
				owned[worker] = true
				if err := writeSeed(ctx, object, payloads[worker]); err != nil {
					return err
				}
			}
			return nil
		},
		operation: func(ctx context.Context, worker int, ordinal uint64) (qualificationOperationResult, error) {
			operationCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			deadline, _ := operationCtx.Deadline()
			operationCtx, observation := msgr.WithRequestAttempts(operationCtx)
			result := qualificationOperationResult{Deadline: &deadline}
			observed := func() qualificationOperationResult {
				if count, known := observation.RetryCount(); known {
					result.RetryCount = &count
				}
				return result
			}
			if matrix.workload == "write" || matrix.workload == "mixed" && ordinal%2 == 1 {
				_, err := objects[worker].WriteFull(operationCtx, payloads[worker])
				return observed(), err
			}
			count, _, err := objects[worker].ReadInto(operationCtx, 0, destinations[worker])
			if err == nil && !bytes.Equal(destinations[worker][:count], payloads[worker]) {
				err = fmt.Errorf("qualification read payload mismatch for worker %d", worker)
			}
			return observed(), err
		},
		beforeMeasured: func() error {
			var err error
			if profileFile != nil {
				if err = pprof.StartCPUProfile(profileFile); err != nil {
					return err
				}
				profileActive = true
			}
			rssSampler, err = startQualificationRSSSampler(100*time.Millisecond, residentBytes, time.Now)
			if err != nil {
				return err
			}
			rssBefore = rssSampler.baseline.bytes
			measuredBefore, err = snapshotResources()
			if err != nil {
				rssSampler.stopAndJoin()
			} else {
				windowStart = time.Now()
			}
			return err
		},
		afterMeasured: func() error {
			windowEnd = time.Now()
			rssSampler.stopAndJoin()
			after, err := snapshotResources()
			if err == nil {
				measuredResources, err = deltaResources(measuredBefore, after)
			}
			if err == nil {
				rssAfter, err = residentBytes()
			}
			err = errors.Join(err, stopProfile())
			return errors.Join(err, rssSampler.err)
		},
		verify: func(ctx context.Context) error {
			for worker, object := range objects {
				count, _, err := object.ReadInto(ctx, 0, destinations[worker])
				if err != nil || !bytes.Equal(destinations[worker][:count], payloads[worker]) {
					return fmt.Errorf("qualification final payload worker %d: %w", worker, errors.Join(err, errors.New("payload not verified")))
				}
			}
			return nil
		},
		cleanup: func(ctx context.Context) error {
			var failures []error
			for worker, object := range objects {
				if !owned[worker] {
					continue
				}
				if _, err := object.Remove(ctx); err != nil && !errors.Is(err, rados.ErrNotFound) {
					failures = append(failures, err)
				}
				if _, err := object.Stat(ctx); !errors.Is(err, rados.ErrNotFound) {
					failures = append(failures, fmt.Errorf("qualification fixture removal not verified: %v", err))
				}
			}
			var err error
			rssCleanup, err = residentBytes()
			return errors.Join(append(failures, err)...)
		},
	}
	attempt, collectionErr := collectQualificationAttempt(ctx, matrix.concurrency, warmup, measured, actions, time.Now)
	if rssSampler != nil {
		var memoryErr error
		attempt.Memory, memoryErr = rssSampler.finish(attempt.Measured.begin, time.Duration(attempt.Measured.ElapsedNS))
		collectionErr = errors.Join(collectionErr, memoryErr)
	}
	for _, phase := range []*qualificationPhaseCapture{&attempt.Warmup, &attempt.Measured} {
		for index := range phase.Records {
			record := &phase.Records[index]
			record.Round, record.Leg = config.Round, config.Leg
			phaseName := "measured"
			if phase == &attempt.Warmup {
				phaseName = "warmup"
			}
			record.OperationID = fmt.Sprintf("%s-%s-w%d-op%d", config.Leg, phaseName, record.Worker, record.Ordinal)
			record.Object = parityObjectName(matrix.size, matrix.concurrency, matrix.workload, record.Worker)
			record.Type = matrix.workload
			if matrix.workload == "mixed" {
				record.Type = "read"
				if record.Ordinal%2 == 1 {
					record.Type = "write"
				}
			}
		}
	}
	latencies := make([]uint64, 0, len(attempt.Measured.Records))
	for _, record := range attempt.Measured.Records {
		latency := record.EndNS - record.StartNS
		if latency < 0 {
			latency = 0
		}
		latencies = append(latencies, uint64(latency))
	}
	sort.Slice(latencies, func(left, right int) bool { return latencies[left] < latencies[right] })
	operations := uint64(len(latencies))
	result := row{SizeBytes: matrix.size, Concurrency: matrix.concurrency, Workload: matrix.workload,
		Operations: operations, Bytes: operations * matrix.size, ElapsedNS: uint64(attempt.Measured.ElapsedNS),
		P50NS: percentile(latencies, .5), P95NS: percentile(latencies, .95), P99NS: percentile(latencies, .99),
		Parity: &parityMetrics{Resources: measuredResources, RSSBefore: rssBefore, RSSAfter: rssAfter, RSSCleanup: rssCleanup,
			PayloadVerified: attempt.PayloadVerified, CleanupVerified: attempt.CleanupVerified}}
	if result.ElapsedNS > 0 {
		result.IOPS = float64(operations) * float64(time.Second) / float64(result.ElapsedNS)
		result.ThroughputBytesPerSecond = result.IOPS * float64(matrix.size)
	}
	status := "sustained_go_capture_unqualified"
	if config.pgoDiagnostic {
		status = "pgo_diagnostic_go_capture_unqualified"
	}
	var failure *string
	if collectionErr != nil {
		status = "failed"
		message := collectionErr.Error()
		failure = &message
	}
	outputErr := json.NewEncoder(file).Encode(struct {
		Status      string               `json:"status"`
		Identity    qualificationConfig  `json:"identity"`
		Report      report               `json:"report"`
		Attempt     qualificationAttempt `json:"attempt"`
		Error       *string              `json:"error"`
		Window      map[string]string    `json:"measurement_window"`
		Limitations []string             `json:"limitations"`
	}{status, config, report{Implementation: "go", Transport: "secure", Environment: environment{GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, GoVersion: runtime.Version(), GOMAXPROCS: runtime.GOMAXPROCS(0)}, Rows: []row{result}}, attempt, failure,
		map[string]string{"clock": "realtime", "start_ns": strconv.FormatInt(windowStart.UnixNano(), 10), "end_ns": strconv.FormatInt(windowEnd.UnixNano(), 10)},
		[]string{"Retry observation counts request preparations beyond the first plus messenger replay dispatches; missing dispatch coverage stays unknown", "RSS interval samples include harness retention; library-only RSS is not established", "Measured CPU includes worker start, RSS sampling, retry observation and record retention; excludes warmup, final verification and cleanup", "Source/binary, placement/health and native pairing require an outer evidence driver; no qualification acceptance"}})
	return errors.Join(collectionErr, outputErr)
}

type parityMetrics struct {
	Resources       resources `json:"measured_resources"`
	RSSBefore       uint64    `json:"rss_before_bytes"`
	RSSAfter        uint64    `json:"rss_after_bytes"`
	RSSCleanup      uint64    `json:"rss_after_cleanup_bytes"`
	HeapPostGC      uint64    `json:"heap_after_gc_bytes"`
	PayloadVerified bool      `json:"payload_verified"`
	CleanupVerified bool      `json:"cleanup_verified"`
}

func residentBytes() (uint64, error) {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 3 && fields[0] == "VmRSS:" && fields[2] == "kB" {
			value, err := strconv.ParseUint(fields[1], 10, 64)
			return value * 1024, err
		}
	}
	return 0, fmt.Errorf("VmRSS unavailable")
}

func parityNamespace(value string) (string, error) {
	if value == "" {
		return "", nil
	}
	if len(value) > 64 || !strings.HasPrefix(value, "p07-parity-") {
		return "", fmt.Errorf("parity namespace must start with p07-parity- and fit 64 bytes")
	}
	for _, character := range value {
		if character != '-' && (character < 'a' || character > 'z') && (character < '0' || character > '9') {
			return "", fmt.Errorf("parity namespace must contain only lowercase ASCII letters, digits and hyphens")
		}
	}
	return value, nil
}

func parityObjectName(size uint64, concurrency int, workload string, worker int) string {
	return fmt.Sprintf("p07-parity-%d-c%d-%s-w%d", size, concurrency, workload, worker)
}
