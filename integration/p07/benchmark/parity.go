//go:build linux

package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

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
