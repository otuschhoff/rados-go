package main

import (
	"context"
	"crypto/sha256"
	"runtime"
	"sync"
	"time"
)

type fixedBackgroundWorker struct {
	Role                 string `json:"role,omitempty"`
	WarmupHashIterations uint64 `json:"warmup_hash_iterations"`
	WindowHashIterations uint64 `json:"window_hash_iterations"`
	DrainHashIterations  uint64 `json:"drain_hash_iterations"`
	Expected             int    `json:"expected"`
	Completed            int    `json:"completed"`
	Missed               int    `json:"missed"`
	Bytes                uint64 `json:"bytes"`
	HashIterations       uint64 `json:"hash_iterations"`
	MaxLatenessNS        int64  `json:"max_lateness_ns"`
}
type fixedBackgroundStats struct {
	CPUWorkers           int                     `json:"cpu_workers"`
	AllocationWorkers    int                     `json:"allocation_workers"`
	WarmupHashIterations uint64                  `json:"warmup_hash_iterations"`
	WindowHashIterations uint64                  `json:"window_hash_iterations"`
	DrainHashIterations  uint64                  `json:"drain_hash_iterations"`
	Workers              []fixedBackgroundWorker `json:"workers"`
	Expected             int                     `json:"expected"`
	Completed            int                     `json:"completed"`
	Missed               int                     `json:"missed"`
	Bytes                uint64                  `json:"bytes"`
	DurationNS           int64                   `json:"allocation_window_ns"`
	CPUActiveNS          int64                   `json:"cpu_active_ns"`
	HashIterations       uint64                  `json:"hash_iterations"`
	MaxLatenessNS        int64                   `json:"max_lateness_ns"`
	ExpectedBytes        uint64                  `json:"expected_bytes"`
	Invalid              bool                    `json:"delivery_invalid"`
}

func fixedAllocationDue(now, start time.Time, rate, expected, next int) (int, int, int64) {
	if now.Before(start) || next >= expected {
		return 0, 0, 0
	}
	due := min(expected, int(now.Sub(start)*time.Duration(rate)/time.Second)+1)
	count := max(0, due-next)
	completed := min(8, count)
	lateness := max(int64(0), now.Sub(offeredArrival(start, next, rate)).Nanoseconds())
	return completed, count - completed, lateness
}

func startFixedBackgroundWork(clock offeredClock, config offeredConfig) (func(time.Time), func() fixedBackgroundStats) {
	const count = 8
	armed := make(chan time.Time, count)
	done := make(chan struct{})
	var ready, joined sync.WaitGroup
	ready.Add(count)
	joined.Add(count)
	stats := fixedBackgroundStats{CPUWorkers: count, AllocationWorkers: count, Workers: make([]fixedBackgroundWorker, count), DurationNS: int64(config.Window)}
	started := clock.Now()
	for worker := 0; worker < count; worker++ {
		go func(workerID int) {
			defer joined.Done()
			var payload [4096]byte
			var retained [8][]byte
			var start time.Time
			next := 0
			expected := int(config.Window * time.Duration(config.AllocRate) / time.Second)
			local := fixedBackgroundWorker{Expected: expected}
			initial := sha256.Sum256(payload[:])
			payload[0] = initial[0]
			local.HashIterations++
			ready.Done()
			for {
				select {
				case <-done:
					local.Missed += expected - next
					stats.Workers[workerID] = local
					runtime.KeepAlive(retained)
					return
				default:
				}
				if start.IsZero() {
					select {
					case start = <-armed:
					default:
					}
				}
				sum := sha256.Sum256(payload[:])
				payload[0] = sum[0]
				local.HashIterations++
				if !start.IsZero() {
					now := clock.Now()
					if !now.Before(start.Add(config.Window)) {
						local.Missed += expected - next
						next = expected
					} else {
						allocations, missed, late := fixedAllocationDue(now, start, config.AllocRate, expected, next)
						local.MaxLatenessNS = max(local.MaxLatenessNS, late)
						for allocation := 0; allocation < allocations; allocation++ {
							if !clock.Now().Before(start.Add(config.Window)) {
								local.Missed += allocations - allocation
								break
							}
							buffer := make([]byte, 64<<10)
							for offset := 0; offset < len(buffer); offset += 4096 {
								buffer[offset] = sum[0]
							}
							retained[local.Completed%len(retained)] = buffer
							local.Completed++
							local.Bytes += uint64(len(buffer))
						}
						local.Missed += missed
						next += allocations + missed
					}
				}
				runtime.KeepAlive(retained)
			}
		}(worker)
	}
	ready.Wait()
	var armOnce, stopOnce sync.Once
	return func(start time.Time) {
			armOnce.Do(func() {
				for worker := 0; worker < count; worker++ {
					armed <- start
				}
			})
		}, func() fixedBackgroundStats {
			stopOnce.Do(func() {
				close(done)
				joined.Wait()
				stats.CPUActiveNS = max(int64(0), clock.Now().Sub(started).Nanoseconds())
				for _, worker := range stats.Workers {
					stats.Expected += worker.Expected
					stats.Completed += worker.Completed
					stats.Missed += worker.Missed
					stats.Bytes += worker.Bytes
					stats.HashIterations += worker.HashIterations
					stats.MaxLatenessNS = max(stats.MaxLatenessNS, worker.MaxLatenessNS)
					if worker.Missed != 0 || worker.MaxLatenessNS > int64(config.LagLimit) {
						stats.Invalid = true
					}
				}
				stats.ExpectedBytes = uint64(stats.Expected) * 65536
			})
			return stats
		}
}

func startFixedFactorialBackgroundWork(clock offeredClock, config offeredConfig) (func(time.Time), func() fixedBackgroundStats) {
	ctx, cancel := context.WithCancel(context.Background())
	begin := make(chan struct{})
	var start time.Time
	var ready, joined sync.WaitGroup
	count := config.CPUWorkers + config.AllocationWorkers
	ready.Add(count)
	joined.Add(count)
	stats := fixedBackgroundStats{CPUWorkers: config.CPUWorkers, AllocationWorkers: config.AllocationWorkers, Workers: make([]fixedBackgroundWorker, count), DurationNS: int64(config.Window)}
	started := clock.Now()
	for worker := 0; worker < count; worker++ {
		go func(workerID int) {
			defer joined.Done()
			local := fixedBackgroundWorker{}
			defer func() { stats.Workers[workerID] = local }()
			if workerID < config.CPUWorkers {
				local.Role = "cpu"
				var payload [4096]byte
				var measuredStart time.Time
				sum := sha256.Sum256(payload[:])
				payload[0] = sum[0]
				local.HashIterations++
				local.WarmupHashIterations++
				ready.Done()
				for {
					select {
					case <-ctx.Done():
						return
					default:
					}
					if measuredStart.IsZero() {
						select {
						case <-begin:
							measuredStart = start
						default:
						}
					}
					sum := sha256.Sum256(payload[:])
					payload[0] = sum[0]
					local.HashIterations++
					completedAt := clock.Now()
					if measuredStart.IsZero() {
						select {
						case <-begin:
							measuredStart = start
						default:
						}
					}
					switch {
					case measuredStart.IsZero() || completedAt.Before(measuredStart):
						local.WarmupHashIterations++
					case completedAt.Before(measuredStart.Add(config.Window)):
						local.WindowHashIterations++
					default:
						local.DrainHashIterations++
					}
				}
			}
			local.Role = "alloc"
			expected := int(config.Window * time.Duration(config.AllocRate) / time.Second)
			local.Expected = expected
			next := 0
			var retained [8][]byte
			defer func() {
				local.Missed += expected - next
				runtime.KeepAlive(retained)
			}()
			ready.Done()
			select {
			case <-begin:
			case <-ctx.Done():
				return
			}
			for next < expected {
				if clock.Wait(ctx, offeredArrival(start, next, config.AllocRate)) != nil {
					return
				}
				now := clock.Now()
				if !now.Before(start.Add(config.Window)) {
					return
				}
				allocations, missed, late := fixedAllocationDue(now, start, config.AllocRate, expected, next)
				local.MaxLatenessNS = max(local.MaxLatenessNS, late)
				for allocation := 0; allocation < allocations; allocation++ {
					if ctx.Err() != nil || !clock.Now().Before(start.Add(config.Window)) {
						return
					}
					buffer := make([]byte, 64<<10)
					for offset := 0; offset < len(buffer); offset += 4096 {
						buffer[offset] = byte(workerID + 1)
					}
					retained[local.Completed%len(retained)] = buffer
					local.Completed++
					local.Bytes += uint64(len(buffer))
					next++
				}
				local.Missed += missed
				next += missed
			}
		}(worker)
	}
	ready.Wait()
	var armOnce, stopOnce sync.Once
	return func(at time.Time) {
			armOnce.Do(func() { start = at; close(begin) })
		}, func() fixedBackgroundStats {
			stopOnce.Do(func() {
				cancel()
				joined.Wait()
				if config.CPUWorkers > 0 {
					stats.CPUActiveNS = max(int64(0), clock.Now().Sub(started).Nanoseconds())
				}
				for _, worker := range stats.Workers {
					stats.Expected += worker.Expected
					stats.Completed += worker.Completed
					stats.Missed += worker.Missed
					stats.Bytes += worker.Bytes
					stats.HashIterations += worker.HashIterations
					stats.WarmupHashIterations += worker.WarmupHashIterations
					stats.WindowHashIterations += worker.WindowHashIterations
					stats.DrainHashIterations += worker.DrainHashIterations
					stats.MaxLatenessNS = max(stats.MaxLatenessNS, worker.MaxLatenessNS)
					stats.Invalid = stats.Invalid || worker.Missed != 0 || worker.MaxLatenessNS > int64(config.LagLimit)
				}
				stats.ExpectedBytes = uint64(stats.Expected) * 65536
			})
			return stats
		}
}
