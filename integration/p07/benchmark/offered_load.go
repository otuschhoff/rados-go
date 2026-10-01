package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"sync"
	"time"
)

type offeredConfig struct {
	Observation       bool          `json:"instrumented"`
	Factorial         bool          `json:"factorial"`
	LoadCase          string        `json:"load_case"`
	CPUWorkers        int           `json:"cpu_workers"`
	AllocationWorkers int           `json:"allocation_workers"`
	Rate              int           `json:"rate_ops_per_second"`
	Window            time.Duration `json:"issuance_window_ns"`
	Deadline          time.Duration `json:"deadline_ns"`
	LagLimit          time.Duration `json:"delivery_lag_limit_ns"`
	Workers           int           `json:"workers"`
	Queue             int           `json:"queue_capacity"`
	AllocRate         int           `json:"allocations_per_worker_per_second"`
	GOMAXPROCS        int           `json:"gomaxprocs"`
	GOGC              int           `json:"gogc"`
	MemoryLimit       string        `json:"gomemlimit"`
}

func parseOfferedConfig(env func(string) string) (*offeredConfig, error) {
	marker := env("P07_OFFERED_LOAD")
	if marker == "" {
		if env("P07_OFFERED_RATE")+env("P07_FIXED_ALLOC_RATE")+env("P07_OFFERED_FACTORIAL")+env("P07_OFFERED_CASE") != "" {
			return nil, fmt.Errorf("offered limits require P07_OFFERED_LOAD=1")
		}
		return nil, nil
	}
	if marker != "1" || env("P07_READ_DIAGNOSTIC") != "1" || env("P07_READ_INTO") != "1" {
		return nil, fmt.Errorf("offered load requires diagnostic ReadInto mode")
	}
	for _, name := range []string{"P07_TIMING_FILE", "P07_TRACE_FILE", "P07_CPU_PROFILE", "P07_MEMORY_PROFILE", "P07_RESOURCE_FILE", "P07_MODE_EVIDENCE_FILE", "P07_PROFILE_KIND", "P07_SCRATCH_SLOTS", "P07_ADMISSION_WINDOW", "P07_OPERATIONS_PER_WORKER", "P07_BACKGROUND_ALLOCATIONS", "P07_SEED_ONLY"} {
		if env(name) != "" {
			return nil, fmt.Errorf("offered load cannot use %s", name)
		}
	}
	if (env("P07_READ_SIZE") != "" && env("P07_READ_SIZE") != "65536") || (env("P07_READ_CONCURRENCY") != "" && env("P07_READ_CONCURRENCY") != "16") || env("P07_BACKGROUND_WORKERS") != "8" || env("GOMAXPROCS") != "10" || env("GOGC") != "100" || env("GOMEMLIMIT") != "off" {
		return nil, fmt.Errorf("offered load requires 64KiB/16, eight background workers, GOMAXPROCS=10 GOGC=100 GOMEMLIMIT=off")
	}
	rate, err := strconv.Atoi(env("P07_OFFERED_RATE"))
	if err != nil || (rate != 1000 && rate != 2000 && rate != 4000) {
		return nil, fmt.Errorf("offered rate must be 1000, 2000 or 4000")
	}
	alloc := env("P07_FIXED_ALLOC_RATE")
	if alloc != "" && alloc != "100" {
		return nil, fmt.Errorf("fixed allocation rate must be 100")
	}
	config := &offeredConfig{Observation: env("P07_OFFERED_OBSERVATION_DIR") != "", LoadCase: "both", CPUWorkers: 8, AllocationWorkers: 8, Rate: rate, Window: 8 * time.Second, Deadline: 500 * time.Millisecond, LagLimit: 50 * time.Millisecond, Workers: 16, Queue: 128, AllocRate: 100, GOMAXPROCS: 10, GOGC: 100, MemoryLimit: "off"}
	if env("P07_OFFERED_FACTORIAL") == "" {
		if env("P07_OFFERED_CASE") != "" {
			return nil, fmt.Errorf("offered case requires P07_OFFERED_FACTORIAL=1")
		}
		return config, nil
	}
	if env("P07_OFFERED_FACTORIAL") != "1" {
		return nil, fmt.Errorf("factorial marker must be 1")
	}
	config.Factorial = true
	config.LoadCase = env("P07_OFFERED_CASE")
	switch config.LoadCase {
	case "none":
		config.CPUWorkers, config.AllocationWorkers = 0, 0
	case "cpu":
		config.AllocationWorkers = 0
	case "alloc":
		config.CPUWorkers = 0
	case "both":
	default:
		return nil, fmt.Errorf("factorial case must be none, cpu, alloc or both")
	}
	return config, nil
}

type offeredClock interface {
	Now() time.Time
	Wait(context.Context, time.Time) error
}

type realOfferedClock struct{}

func (realOfferedClock) Now() time.Time { return time.Now() }
func (realOfferedClock) Wait(ctx context.Context, until time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	delay := time.Until(until)
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type offeredOutcome struct {
	ScheduledNS   int64  `json:"scheduled_ns"`
	EnqueuedNS    int64  `json:"enqueued_ns"`
	WorkerStartNS int64  `json:"worker_start_ns"`
	ReadStartNS   int64  `json:"read_start_ns"`
	ReturnedNS    int64  `json:"returned_ns"`
	DeliveryNS    int64  `json:"delivery_delay_ns"`
	QueueNS       int64  `json:"queue_delay_ns"`
	DispatchNS    int64  `json:"dispatch_ns"`
	ServiceNS     int64  `json:"service_ns"`
	Kind          string `json:"kind"`
	LatencyNS     int64  `json:"latency_ns"`
	Admitted      bool   `json:"admitted"`
	Attempted     bool   `json:"attempted"`
	DeadlineMiss  bool   `json:"deadline_miss"`
}

type offeredResult struct {
	QueueHighWaterSampled  int                  `json:"queue_high_water_sampled"`
	TimingMethodology      string               `json:"timing_methodology"`
	Config                 offeredConfig        `json:"config"`
	Expected               int                  `json:"expected"`
	Admitted               int                  `json:"admitted"`
	Attempted              int                  `json:"attempted"`
	Success                int                  `json:"success"`
	Timeouts               int                  `json:"timeouts"`
	Overload               int                  `json:"overload"`
	Errors                 int                  `json:"errors"`
	Canceled               int                  `json:"canceled"`
	SLOFailures            int                  `json:"slo_failures"`
	SuccessP99NS           int64                `json:"success_p99_ns"`
	AllOutcomeP99NS        int64                `json:"all_outcome_p99_ns"`
	MaxDeliveryLagNS       int64                `json:"max_delivery_lag_ns"`
	DeliveryInvalid        bool                 `json:"delivery_invalid"`
	Failed                 bool                 `json:"failed"`
	FailureReason          string               `json:"failure_reason,omitempty"`
	RowsOperationsBasis    string               `json:"rows_operations_basis"`
	ScratchCountersEnabled bool                 `json:"scratch_counters_enabled"`
	ResourceLimitations    string               `json:"resource_limitations"`
	WarmupNS               int64                `json:"warmup_ns"`
	ActualIssuanceNS       int64                `json:"actual_issuance_ns"`
	TotalWindowDrainNS     int64                `json:"total_window_drain_ns"`
	DrainNS                int64                `json:"drain_ns"`
	SuccessBytes           uint64               `json:"success_bytes"`
	WindowIOPS             float64              `json:"success_ops_per_issuance_second"`
	WindowBytesPerSecond   float64              `json:"success_bytes_per_issuance_second"`
	TotalBytesPerSecond    float64              `json:"success_bytes_per_window_and_drain_second"`
	Background             fixedBackgroundStats `json:"background"`
	Outcomes               []offeredOutcome     `json:"outcomes"`
}

func offeredArrival(start time.Time, index, rate int) time.Time {
	return start.Add(time.Duration(int64(index) * int64(time.Second) / int64(rate)))
}

func offeredP99(values []int64) int64 {
	if len(values) == 0 {
		return 0
	}
	sort.Slice(values, func(left, right int) bool { return values[left] < values[right] })
	return values[(99*len(values)+99)/100-1]
}

func (result *offeredResult) summarize() {
	all := make([]int64, 0, result.Expected)
	success := make([]int64, 0, result.Expected)
	for _, outcome := range result.Outcomes {
		all = append(all, outcome.LatencyNS)
		if outcome.Admitted {
			result.Admitted++
		}
		if outcome.Attempted {
			result.Attempted++
		}
		switch outcome.Kind {
		case "success":
			result.Success++
			success = append(success, outcome.LatencyNS)
		case "timeout":
			result.Timeouts++
		case "overload":
			result.Overload++
		case "canceled":
			result.Canceled++
		default:
			result.Errors++
		}
		if outcome.Kind != "success" || outcome.DeadlineMiss {
			result.SLOFailures++
		}
	}
	result.SuccessP99NS = offeredP99(success)
	result.AllOutcomeP99NS = offeredP99(all)
	result.SuccessBytes = uint64(result.Success) * 65536
	result.WindowIOPS = float64(result.Success) / result.Config.Window.Seconds()
	result.WindowBytesPerSecond = float64(result.SuccessBytes) / result.Config.Window.Seconds()
	if result.TotalWindowDrainNS > 0 {
		result.TotalBytesPerSecond = float64(result.SuccessBytes) / time.Duration(result.TotalWindowDrainNS).Seconds()
	}
	result.Failed = result.DeliveryInvalid || result.SLOFailures != 0
}

func missingOfferedOutcome(scheduled int64) offeredOutcome {
	return offeredOutcome{ScheduledNS: scheduled, EnqueuedNS: -1, WorkerStartNS: -1, ReadStartNS: -1, ReturnedNS: -1, DeliveryNS: -1, QueueNS: -1, DispatchNS: -1, ServiceNS: -1}
}

type offeredJob struct {
	index      int
	enqueuedNS int64
}

func runOffered(ctx context.Context, config offeredConfig, clock offeredClock, read func(context.Context, int) error, onStart func(time.Time)) offeredResult {
	result := offeredResult{Config: config, Expected: int(int64(config.Window) * int64(config.Rate) / int64(time.Second))}
	result.TimingMethodology = "monotonic offsets from issuance start; -1 means phase not reached; enqueue stamped before nonblocking send; service spans read call only; dispatch includes context setup; returned marks read return or rejection decision; pre-arrival cancellation has no delivery phase and legacy latency clamps to zero; queue high-water samples channel length after send and may underestimate; clock reads and timestamp writes add harness overhead"
	result.Outcomes = make([]offeredOutcome, result.Expected)
	queue := make(chan offeredJob, config.Queue)
	begin := make(chan struct{})
	var ready, joined sync.WaitGroup
	ready.Add(config.Workers)
	joined.Add(config.Workers)
	var start time.Time
	var warmupErr error
	var warmupMu sync.Mutex
	warmupStart := clock.Now()
	for worker := 0; worker < config.Workers; worker++ {
		go func(workerID int) {
			defer joined.Done()
			for iteration := 0; iteration < readWarmupPerWorker; iteration++ {
				if err := read(ctx, workerID); err != nil {
					warmupMu.Lock()
					warmupErr = errors.Join(warmupErr, err)
					warmupMu.Unlock()
					break
				}
			}
			ready.Done()
			<-begin
			for job := range queue {
				index := job.index
				workerStart := clock.Now().Sub(start).Nanoseconds()
				arrival := offeredArrival(start, index, config.Rate)
				deadline := arrival.Add(config.Deadline)
				outcome := missingOfferedOutcome(arrival.Sub(start).Nanoseconds())
				outcome.Admitted = true
				outcome.EnqueuedNS = job.enqueuedNS
				outcome.WorkerStartNS = workerStart
				outcome.DeliveryNS = job.enqueuedNS - outcome.ScheduledNS
				outcome.QueueNS = workerStart - job.enqueuedNS
				if ctx.Err() != nil {
					outcome.Kind = "canceled"
				} else if !clock.Now().Before(deadline) {
					outcome.Kind = "timeout"
				} else {
					operationCtx, cancel := context.WithDeadline(ctx, deadline)
					if config.Observation {
						operationCtx = withOfferedArrivalIndex(operationCtx, index)
					}
					outcome.Attempted = true
					outcome.ReadStartNS = clock.Now().Sub(start).Nanoseconds()
					outcome.DispatchNS = outcome.ReadStartNS - workerStart
					err := read(operationCtx, workerID)
					outcome.ReturnedNS = clock.Now().Sub(start).Nanoseconds()
					outcome.ServiceNS = outcome.ReturnedNS - outcome.ReadStartNS
					contextErr := operationCtx.Err()
					cancel()
					switch {
					case errors.Is(err, context.Canceled) || errors.Is(contextErr, context.Canceled):
						outcome.Kind = "canceled"
					case errors.Is(err, context.DeadlineExceeded) || errors.Is(contextErr, context.DeadlineExceeded):
						outcome.Kind = "timeout"
					case err != nil:
						outcome.Kind = "error"
					default:
						outcome.Kind = "success"
					}
				}
				if outcome.ReturnedNS < 0 {
					outcome.ReturnedNS = clock.Now().Sub(start).Nanoseconds()
				}
				outcome.LatencyNS = max(int64(0), outcome.ReturnedNS-outcome.ScheduledNS)
				outcome.DeadlineMiss = outcome.LatencyNS >= int64(config.Deadline)
				result.Outcomes[index] = outcome
			}
		}(worker)
	}
	ready.Wait()
	start = clock.Now()
	result.WarmupNS = start.Sub(warmupStart).Nanoseconds()
	if onStart != nil {
		onStart(start)
	}
	close(begin)
	for index := 0; index < result.Expected; index++ {
		arrival := offeredArrival(start, index, config.Rate)
		waitErr := clock.Wait(ctx, arrival)
		enqueued := clock.Now().Sub(start).Nanoseconds()
		lag := max(int64(0), enqueued-arrival.Sub(start).Nanoseconds())
		outcome := missingOfferedOutcome(arrival.Sub(start).Nanoseconds())
		outcome.ReturnedNS = enqueued
		outcome.LatencyNS = lag
		outcome.DeliveryNS = lag
		if enqueued < outcome.ScheduledNS {
			outcome.DeliveryNS = -1
		}
		outcome.DeadlineMiss = lag >= int64(config.Deadline)
		result.MaxDeliveryLagNS = max(result.MaxDeliveryLagNS, lag)
		if lag > int64(config.LagLimit) {
			result.DeliveryInvalid = true
		}
		if waitErr != nil || ctx.Err() != nil || warmupErr != nil {
			outcome.Kind = "canceled"
			result.Outcomes[index] = outcome
			continue
		}
		select {
		case queue <- offeredJob{index: index, enqueuedNS: enqueued}:
			result.QueueHighWaterSampled = max(result.QueueHighWaterSampled, len(queue))
		default:
			outcome.Kind = "overload"
			result.Outcomes[index] = outcome
		}
	}
	_ = clock.Wait(ctx, start.Add(config.Window))
	result.ActualIssuanceNS = max(int64(0), clock.Now().Sub(start).Nanoseconds())
	close(queue)
	joined.Wait()
	result.TotalWindowDrainNS = max(int64(0), clock.Now().Sub(start).Nanoseconds())
	result.DrainNS = max(int64(0), result.TotalWindowDrainNS-int64(config.Window))
	result.summarize()
	if warmupErr != nil {
		result.FailureReason = "warmup: " + warmupErr.Error()
	}
	return result
}

func writeOfferedReport(writer io.Writer, output any, failed bool) error {
	if err := json.NewEncoder(writer).Encode(output); err != nil {
		return err
	}
	if failed {
		return fmt.Errorf("offered-load diagnostic failed; inspect offered_load outcomes and delivery")
	}
	return nil
}
