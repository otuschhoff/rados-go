//go:build linux

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"time"

	rados "github.com/otuschhoff/rados-go"
)

func runOfferedDiagnostic(ctx context.Context, client *rados.Client, pool rados.Pool, config offeredConfig, transport string) error {
	shape := readShape{size: 65536, concurrency: 16}
	reads := make([]func(context.Context) error, config.Workers)
	for worker := 0; worker < config.Workers; worker++ {
		object := pool.Object(shape.objectName(worker))
		destination := make([]byte, int(shape.size))
		payload := makePayload(shape.size, 1, uint64(worker))
		reads[worker] = func(readCtx context.Context) error {
			count, _, err := object.ReadInto(readCtx, 0, destination)
			if err != nil {
				return err
			}
			if count != len(destination) || !bytes.Equal(destination, payload) {
				return fmt.Errorf("offered ReadInto payload mismatch")
			}
			return nil
		}
	}
	before, beforeErr := snapshotResources()
	observationCtx, finishObservation, observationErr := beginOfferedObservation(ctx)
	if observationErr != nil {
		return errors.Join(observationErr, writeOfferedSetupFailure(config, transport, observationErr))
	}
	defer finishObservation()
	clock := realOfferedClock{}
	background := startFixedBackgroundWork
	if config.Factorial {
		background = startFixedFactorialBackgroundWork
	}
	arm, stop := background(clock, config)
	defer stop()
	read := func(readCtx context.Context, worker int) error { return reads[worker](readCtx) }
	if config.Observation {
		read = wrapOfferedObservedRead(read)
	}
	result := runOffered(observationCtx, config, clock, read, arm)
	result.Background = stop()
	observationErr = finishObservation()
	result.Failed = result.Failed || result.Background.Invalid
	result.RowsOperationsBasis = "expected_arrivals; bytes and throughput count successful reads only"
	result.ResourceLimitations = "process-wide deltas include warmup, issuance, drain, background join and harness bookkeeping; exclude client setup, worker buffers and JSON export; RSS is process lifetime high-water, not isolated library cost; no matched native comparison or qualification claim"
	if config.Factorial {
		result.ResourceLimitations += "; independent CPU workers hash throughout warmup, issuance and drain; window_hash_iterations counts hashes completed during issuance; allocation-only workers wait on absolute timers; CPU timing reads add harness overhead; ten-P runtime is diagnostic, not library tuning"
	}
	after, afterErr := snapshotResources()
	var res resources
	resourceErr := errors.Join(beforeErr, afterErr)
	if resourceErr == nil {
		res, resourceErr = deltaResources(before, after)
	}
	if resourceErr != nil {
		result.Failed = true
		if result.FailureReason != "" {
			result.FailureReason += "; "
		}
		result.FailureReason += resourceErr.Error()
	}
	if resourceErr == nil {
		gcCycles := after.memstats.NumGC - before.memstats.NumGC
		gcPause := after.memstats.PauseTotalNs - before.memstats.PauseTotalNs
		res.GCCycles, res.GCPauseNS = &gcCycles, &gcPause
	}
	if config.Observation {
		observationErr = errors.Join(observationErr, client.Close(), exportOfferedObservation(observationCtx))
		result.ResourceLimitations += "; instrumented leg: runtime trace and per-attempt transport collector enabled; excluded from primary comparisons"
	}
	if observationErr != nil {
		result.Failed = true
		result.FailureReason = errors.Join(errors.New(result.FailureReason), observationErr).Error()
	}
	return writeOfferedReport(os.Stdout, offeredDiagnosticReport(result, res, transport), result.Failed)
}

func offeredDiagnosticReport(result offeredResult, res resources, transport string) report {
	shape := readShape{size: 65536, concurrency: 16}
	cpuWorkers, allocations := 8, true
	if result.Config.Factorial {
		cpuWorkers, allocations = result.Config.CPUWorkers, result.Config.AllocationWorkers > 0
	}
	output := report{
		Implementation: "go", Transport: transport,
		Environment: environment{GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, GoVersion: runtime.Version(), GOMAXPROCS: runtime.GOMAXPROCS(0)},
		Resources:   res,
		Rows: []row{{SizeBytes: shape.size, Concurrency: shape.concurrency, Workload: "read", Operations: uint64(result.Expected), Bytes: result.SuccessBytes,
			ElapsedNS: uint64(result.TotalWindowDrainNS), ThroughputBytesPerSecond: result.WindowBytesPerSecond, IOPS: result.WindowIOPS, P99NS: uint64(result.AllOutcomeP99NS)}},
		Diagnostic:  &diagnosticMethodology{WarmupOperations: 128, ResourceScope: "warmup_issuance_drain_background_join_and_harness", ObjectSet: shape.objectSet(), BackgroundCPUWorkers: cpuWorkers, ReadAPI: "read_into", ScratchSlots: 4, AdmissionWindow: 16, BackgroundAllocations: allocations},
		OfferedLoad: &result,
	}
	return output
}

func writeOfferedSetupFailure(config offeredConfig, transport string, setupErr error) error {
	result := offeredResult{Config: config, Expected: config.Rate * 8, FailureReason: "setup: " + setupErr.Error(), DeliveryInvalid: true,
		RowsOperationsBasis: "expected_arrivals; setup failed before issuance", ResourceLimitations: "setup failed; resource deltas unavailable"}
	result.Outcomes = make([]offeredOutcome, result.Expected)
	for index := range result.Outcomes {
		outcome := missingOfferedOutcome(int64(index) * int64(time.Second) / int64(config.Rate))
		outcome.Kind = "canceled"
		result.Outcomes[index] = outcome
	}
	result.Background = fixedBackgroundStats{Expected: 6400, Missed: 6400, ExpectedBytes: 6400 * 65536, DurationNS: int64(config.Window), Invalid: true, Workers: make([]fixedBackgroundWorker, 8)}
	for index := range result.Background.Workers {
		result.Background.Workers[index] = fixedBackgroundWorker{Expected: 800, Missed: 800}
	}
	result.Background.CPUWorkers, result.Background.AllocationWorkers = 8, 8
	if config.Factorial {
		expected := config.AllocationWorkers * int(config.Window*time.Duration(config.AllocRate)/time.Second)
		result.Background = fixedBackgroundStats{CPUWorkers: config.CPUWorkers, AllocationWorkers: config.AllocationWorkers, Expected: expected, Missed: expected, ExpectedBytes: uint64(expected) * 65536, DurationNS: int64(config.Window), Invalid: expected > 0, Workers: make([]fixedBackgroundWorker, config.CPUWorkers+config.AllocationWorkers)}
		for index := range result.Background.Workers {
			if index < config.CPUWorkers {
				result.Background.Workers[index].Role = "cpu"
			} else {
				result.Background.Workers[index] = fixedBackgroundWorker{Role: "alloc", Expected: expected / config.AllocationWorkers, Missed: expected / config.AllocationWorkers}
			}
		}
	}
	result.summarize()
	return writeOfferedReport(os.Stdout, offeredDiagnosticReport(result, resources{}, transport), result.Failed)
}
