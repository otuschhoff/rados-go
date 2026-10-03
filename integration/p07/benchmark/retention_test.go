//go:build linux

package main

import (
	"errors"
	"testing"
)

func TestRetentionConfig(t *testing.T) {
	valid := map[string]string{"P07_RETENTION_WINDOWS": "16", "P07_PARITY_NAMESPACE": "p07-parity-retention", "P07_MATRIX_SIZE": "4194304", "P07_MATRIX_CONCURRENCY": "16", "P07_MATRIX_WORKLOAD": "write", "P07_MATRIX_OPERATIONS_PER_WORKER": "256"}
	get := func(name string) string { return valid[name] }
	if windows, err := parseRetentionWindows(get); err != nil || windows != 16 {
		t.Fatalf("windows=%d err=%v", windows, err)
	}
	for _, invalid := range []string{"5", "65", "bad"} {
		valid["P07_RETENTION_WINDOWS"] = invalid
		if _, err := parseRetentionWindows(get); err == nil {
			t.Fatalf("accepted windows %q", invalid)
		}
	}
	valid["P07_RETENTION_WINDOWS"] = "16"
	for _, name := range []string{"P07_PARITY_NAMESPACE", "P07_MATRIX_SIZE", "P07_MATRIX_CONCURRENCY", "P07_MATRIX_WORKLOAD"} {
		original := valid[name]
		valid[name] = ""
		if _, err := parseRetentionWindows(get); err == nil {
			t.Fatalf("accepted missing %s", name)
		}
		valid[name] = original
	}
	valid["P07_READ_DIAGNOSTIC"] = "1"
	if _, err := parseRetentionWindows(get); err == nil {
		t.Fatal("accepted diagnostic retention")
	}
}

func TestRetentionWindows(t *testing.T) {
	calls := 0
	run := func() (row, error) {
		calls++
		return row{Operations: 256, SizeBytes: 4194304, Concurrency: 1, Workload: "write", Parity: &parityMetrics{RSSCleanup: 123, HeapPostGC: 42, PayloadVerified: true, CleanupVerified: true}}, nil
	}
	results, err := collectRetention(6, run)
	if err != nil || calls != 6 || len(results) != 6 || results[5].Index != 5 || results[5].HeapPostGC != 42 {
		t.Fatalf("results=%+v calls=%d err=%v", results, calls, err)
	}
	if _, err := collectRetention(6, func() (row, error) { return row{}, errors.New("failed") }); err == nil {
		t.Fatal("ignored failed window")
	}
	if _, err := collectRetention(6, func() (row, error) { return row{Parity: &parityMetrics{PayloadVerified: true}}, nil }); err == nil {
		t.Fatal("ignored cleanup failure")
	}
}
