//go:build linux

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"

	rados "github.com/otuschhoff/rados-go"
)

func parseRetentionWindows(env func(string) string) (int, error) {
	value := env("P07_RETENTION_WINDOWS")
	if value == "" {
		return 0, nil
	}
	windows, err := strconv.Atoi(value)
	if err != nil || windows < 6 || windows > 64 {
		return 0, fmt.Errorf("retention windows must be 6..64")
	}
	namespace, err := parityNamespace(env("P07_PARITY_NAMESPACE"))
	if err != nil || namespace == "" {
		return 0, fmt.Errorf("retention requires a valid parity namespace")
	}
	for _, name := range []string{"P07_READ_DIAGNOSTIC", "P07_SEED_ONLY", "P07_OFFERED_LOAD", "P07_BACKGROUND_WORKERS", "P07_CPU_PROFILE", "P07_MEMORY_PROFILE", "P07_TRACE_FILE", "P07_RESOURCE_FILE"} {
		if env(name) != "" {
			return 0, fmt.Errorf("retention cannot use %s", name)
		}
	}
	matrix, err := parseMatrixExperiment(env, false)
	if err != nil || matrix.size == 0 || matrix.concurrency == 0 || matrix.workload == "" || matrix.operations <= 2 {
		return 0, fmt.Errorf("retention requires explicit sustained matrix settings")
	}
	return windows, nil
}

type retentionWindow struct {
	Index           int    `json:"index"`
	Operations      uint64 `json:"operations"`
	Size            uint64 `json:"size_bytes"`
	Concurrency     int    `json:"concurrency"`
	Workload        string `json:"workload"`
	RSSCleanup      uint64 `json:"rss_after_cleanup_bytes"`
	HeapPostGC      uint64 `json:"heap_after_gc_bytes"`
	PayloadVerified bool   `json:"payload_verified"`
	CleanupVerified bool   `json:"cleanup_verified"`
}

func collectRetention(windows int, run func() (row, error)) ([]retentionWindow, error) {
	results := make([]retentionWindow, 0, windows)
	for index := 0; index < windows; index++ {
		result, err := run()
		if err != nil {
			return nil, fmt.Errorf("retention window %d: %w", index, err)
		}
		if result.Parity == nil || !result.Parity.PayloadVerified || !result.Parity.CleanupVerified {
			return nil, fmt.Errorf("retention window %d failed verification", index)
		}
		results = append(results, retentionWindow{Index: index, Operations: result.Operations, Size: result.SizeBytes, Concurrency: result.Concurrency, Workload: result.Workload, RSSCleanup: result.Parity.RSSCleanup, HeapPostGC: result.Parity.HeapPostGC, PayloadVerified: true, CleanupVerified: true})
	}
	return results, nil
}

func runRetention(ctx context.Context, pool rados.Pool, windows int, size uint64, concurrency int, workload string, output io.Writer) error {
	results, err := collectRetention(windows, func() (row, error) {
		return runRow(ctx, pool, 1, size, concurrency, workload)
	})
	if err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(struct {
		Implementation string            `json:"implementation"`
		Transport      string            `json:"transport"`
		Windows        []retentionWindow `json:"windows"`
		Limitations    string            `json:"limitations"`
	}{"go", "secure", results, "One connected client, fixed fixtures; post-GC Go heap and native glibc allocations are different metrics; RSS includes caches; bounded windows are not endurance guarantees"})
}
