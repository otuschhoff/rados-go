//go:build linux

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	rados "github.com/otuschhoff/rados-go"
)

func enduranceWorkload(window int) string {
	return []string{"read", "write", "mixed"}[window%3]
}

func validateEnduranceConfig(root string, config qualificationConfig, matrix matrixExperiment) error {
	if !filepath.IsAbs(root) || !config.pgoDiagnostic || config.profileFile != "" || config.Round != 1 || config.Seed != 1101 || config.Leg != "endurance-w0" || config.file != filepath.Join(root, "window0.capture.json") || matrix.size != 1048576 || matrix.concurrency != 16 || matrix.workload != "read" {
		return fmt.Errorf("endurance requires its fixed diagnostic identity and fresh absolute root")
	}
	return nil
}

func runEndurance(ctx context.Context, client *rados.Client, pool rados.Pool, root string, config qualificationConfig) error {
	if err := validateEnduranceConfig(root, config, matrixExperiment{size: 1048576, concurrency: 16, workload: "read"}); err != nil {
		return err
	}
	type observation struct {
		Index int    `json:"index"`
		RSS   uint64 `json:"rss_bytes"`
		Heap  uint64 `json:"heap_after_gc_bytes"`
		GC    uint32 `json:"gc_cycles"`
		Pause uint64 `json:"gc_pause_total_ns"`
	}
	windows := make([]observation, 0, 15)
	for window := 0; window < 15; window++ {
		config.file = filepath.Join(root, fmt.Sprintf("window%d.capture.json", window))
		config.Leg = fmt.Sprintf("endurance-w%d", window)
		if err := runQualification(ctx, pool, config, matrixExperiment{size: 1048576, concurrency: 16, workload: enduranceWorkload(window)}); err != nil {
			return err
		}
		runtime.GC()
		var memory runtime.MemStats
		runtime.ReadMemStats(&memory)
		rss, err := residentBytes()
		if err != nil {
			return err
		}
		windows = append(windows, observation{window, rss, memory.HeapAlloc, memory.NumGC, memory.PauseTotalNs})
	}
	if err := client.Close(); err != nil {
		return err
	}
	runtime.GC()
	start := time.Now()
	idle := make([]qualificationRSSSample, 0, 32)
	for index := 0; index < 32; index++ {
		rss, err := residentBytes()
		if err != nil {
			return err
		}
		idle = append(idle, qualificationRSSSample{AtNS: time.Since(start).Nanoseconds(), RSSBytes: rss})
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	file, err := os.OpenFile(filepath.Join(root, "endurance.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	return errors.Join(json.NewEncoder(file).Encode(struct {
		Implementation string                   `json:"implementation"`
		Windows        []observation            `json:"windows"`
		Idle           []qualificationRSSSample `json:"post_close_idle"`
	}{"go", windows, idle}), file.Close())
}
