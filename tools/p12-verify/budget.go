package main

import (
	"fmt"
	"math"
)

const (
	minimumNativeThroughputRatio = 0.10
	maximumNativeP99Ratio        = 8.0
	maximumRSSBytes              = 2684354560
	maximumAllocations           = 1000000
	maximumAllocatedBytes        = 42949672960
)

var approvedBenchmarkBudget = benchmarkBudget{
	MinimumNativeThroughputRatio: minimumNativeThroughputRatio,
	MaximumNativeP99Ratio:        maximumNativeP99Ratio,
	MaximumRSSBytes:              maximumRSSBytes,
	MaximumAllocations:           maximumAllocations,
	MaximumAllocatedBytes:        maximumAllocatedBytes,
}

type benchmarkBudget struct {
	MinimumNativeThroughputRatio float64
	MaximumNativeP99Ratio        float64
	MaximumRSSBytes              uint64
	MaximumAllocations           uint64
	MaximumAllocatedBytes        uint64
}

type benchmarkRun struct {
	Implementation string         `json:"implementation"`
	Transport      string         `json:"transport"`
	Environment    environment    `json:"environment"`
	Resources      resources      `json:"resources"`
	Rows           []benchmarkRow `json:"rows"`
}

type environment struct {
	GOOS      *string `json:"GOOS,omitempty"`
	GOARCH    *string `json:"GOARCH,omitempty"`
	GoVersion *string `json:"go_version,omitempty"`
	Library   *string `json:"library,omitempty"`
}

type resources struct {
	CPUUserNS      uint64  `json:"cpu_user_ns"`
	CPUSystemNS    uint64  `json:"cpu_system_ns"`
	Allocations    *uint64 `json:"allocations"`
	AllocatedBytes *uint64 `json:"allocated_bytes"`
	MaxRSSBytes    uint64  `json:"max_rss_bytes"`
}

type benchmarkRow struct {
	SizeBytes                uint64  `json:"size_bytes"`
	Concurrency              int     `json:"concurrency"`
	Workload                 string  `json:"workload"`
	Operations               uint64  `json:"operations"`
	Bytes                    uint64  `json:"bytes"`
	ElapsedNS                uint64  `json:"elapsed_ns"`
	ThroughputBytesPerSecond float64 `json:"throughput_bytes_per_second"`
	IOPS                     float64 `json:"iops"`
	P50NS                    uint64  `json:"p50_ns"`
	P95NS                    uint64  `json:"p95_ns"`
	P99NS                    uint64  `json:"p99_ns"`
}

func evaluateBenchmarkBudget(runs []benchmarkRun, budget benchmarkBudget) error {
	indexed := make(map[string]benchmarkRun, len(runs))
	for _, run := range runs {
		key := run.Implementation + "/" + run.Transport
		if _, exists := indexed[key]; exists {
			return fmt.Errorf("duplicate benchmark run %s", key)
		}
		indexed[key] = run
	}
	for _, transport := range []string{"secure", "crc"} {
		goRun, goOK := indexed["go/"+transport]
		nativeRun, nativeOK := indexed["native/"+transport]
		if !goOK || !nativeOK {
			return fmt.Errorf("missing benchmark pair for %s transport", transport)
		}
		if err := evaluateResources(goRun, budget); err != nil {
			return fmt.Errorf("go/%s: %w", transport, err)
		}
		nativeRows := make(map[string]benchmarkRow, len(nativeRun.Rows))
		for _, row := range nativeRun.Rows {
			nativeRows[rowKey(row)] = row
		}
		for _, goRow := range goRun.Rows {
			nativeRow, ok := nativeRows[rowKey(goRow)]
			if !ok {
				return fmt.Errorf("go/%s: missing native row %s", transport, rowKey(goRow))
			}
			if !finitePositive(goRow.ThroughputBytesPerSecond) || !finitePositive(nativeRow.ThroughputBytesPerSecond) || goRow.P99NS == 0 || nativeRow.P99NS == 0 {
				return fmt.Errorf("%s: invalid benchmark metric", rowKey(goRow))
			}
			throughputRatio := goRow.ThroughputBytesPerSecond / nativeRow.ThroughputBytesPerSecond
			if throughputRatio < budget.MinimumNativeThroughputRatio {
				return fmt.Errorf("go/%s %s: throughput ratio %.3f is below %.3f", transport, rowKey(goRow), throughputRatio, budget.MinimumNativeThroughputRatio)
			}
			p99Ratio := float64(goRow.P99NS) / float64(nativeRow.P99NS)
			if p99Ratio > budget.MaximumNativeP99Ratio {
				return fmt.Errorf("go/%s %s: p99 ratio %.3f exceeds %.3f", transport, rowKey(goRow), p99Ratio, budget.MaximumNativeP99Ratio)
			}
		}
		if len(goRun.Rows) != len(nativeRows) {
			return fmt.Errorf("go/%s: benchmark row count differs from native", transport)
		}
	}
	return nil
}

func evaluateResources(run benchmarkRun, budget benchmarkBudget) error {
	if run.Resources.MaxRSSBytes == 0 || run.Resources.MaxRSSBytes > budget.MaximumRSSBytes {
		return fmt.Errorf("max RSS %d exceeds budget %d", run.Resources.MaxRSSBytes, budget.MaximumRSSBytes)
	}
	if run.Resources.Allocations == nil || *run.Resources.Allocations > budget.MaximumAllocations {
		return fmt.Errorf("allocations are missing or exceed budget %d", budget.MaximumAllocations)
	}
	if run.Resources.AllocatedBytes == nil || *run.Resources.AllocatedBytes > budget.MaximumAllocatedBytes {
		return fmt.Errorf("allocated bytes are missing or exceed budget %d", budget.MaximumAllocatedBytes)
	}
	return nil
}

func rowKey(row benchmarkRow) string {
	return fmt.Sprintf("size=%d/concurrency=%d/workload=%s", row.SizeBytes, row.Concurrency, row.Workload)
}

func finitePositive(value float64) bool {
	return value > 0 && !math.IsInf(value, 0) && !math.IsNaN(value)
}
