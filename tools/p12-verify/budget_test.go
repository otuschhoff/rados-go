package main

import (
	"strings"
	"testing"
)

func TestEvaluateBenchmarkBudget(t *testing.T) {
	budget := benchmarkBudget{MinimumNativeThroughputRatio: 0.5, MaximumNativeP99Ratio: 2, MaximumRSSBytes: 2000, MaximumAllocations: 200, MaximumAllocatedBytes: 2000}
	if err := evaluateBenchmarkBudget(budgetRuns(), budget); err != nil {
		t.Fatalf("valid benchmark rejected: %v", err)
	}

	tests := map[string]struct {
		mutate func([]benchmarkRun)
		want   string
	}{
		"throughput":      {func(runs []benchmarkRun) { runs[0].Rows[0].ThroughputBytesPerSecond = 49 }, "throughput ratio"},
		"p99":             {func(runs []benchmarkRun) { runs[0].Rows[0].P99NS = 201 }, "p99 ratio"},
		"RSS":             {func(runs []benchmarkRun) { runs[0].Resources.MaxRSSBytes = 2001 }, "max RSS"},
		"allocations":     {func(runs []benchmarkRun) { *runs[0].Resources.Allocations = 201 }, "allocations"},
		"allocated bytes": {func(runs []benchmarkRun) { *runs[0].Resources.AllocatedBytes = 2001 }, "allocated bytes"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			runs := budgetRuns()
			test.mutate(runs)
			if err := evaluateBenchmarkBudget(runs, budget); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("budget failure not identified: %v", err)
			}
		})
	}
}

func budgetRuns() []benchmarkRun {
	row := benchmarkRow{SizeBytes: 4096, Concurrency: 1, Workload: "read", ThroughputBytesPerSecond: 100, P99NS: 100}
	newGoRun := func(transport string) benchmarkRun {
		allocations, allocatedBytes := uint64(100), uint64(1000)
		return benchmarkRun{Implementation: "go", Transport: transport, Resources: resources{Allocations: &allocations, AllocatedBytes: &allocatedBytes, MaxRSSBytes: 1000}, Rows: []benchmarkRow{row}}
	}
	return []benchmarkRun{
		newGoRun("secure"),
		{Implementation: "native", Transport: "secure", Rows: []benchmarkRow{row}},
		newGoRun("crc"),
		{Implementation: "native", Transport: "crc", Rows: []benchmarkRow{row}},
	}
}
