package main

import (
	"context"
	"fmt"
	"strconv"
)

type scratchStatistics struct {
	Hits          uint64 `json:"hits"`
	Misses        uint64 `json:"misses"`
	Bypasses      uint64 `json:"bypasses"`
	RetainedBytes uint64 `json:"shared_receive_retained_bytes"`
}

type readExperiment struct {
	operations     int
	slots          int
	window         int
	allocationLoad bool
}

func parseReadExperiment(operations, slots, window, allocation string, diagnostic bool) (readExperiment, error) {
	value := readExperiment{operations: 256, window: 16}
	if !diagnostic && operations+slots+window+allocation != "" {
		return value, fmt.Errorf("read experiments require diagnostic mode")
	}
	for _, item := range []struct {
		text             string
		destination      *int
		minimum, maximum int
	}{{operations, &value.operations, 256, 4096}, {slots, &value.slots, 1, 8}, {window, &value.window, 1, 64}} {
		if item.text == "" {
			continue
		}
		parsed, err := strconv.Atoi(item.text)
		if err != nil || parsed < item.minimum || parsed > item.maximum {
			return value, fmt.Errorf("invalid read experiment limit")
		}
		*item.destination = parsed
	}
	if allocation != "" && allocation != "0" && allocation != "1" {
		return value, fmt.Errorf("invalid allocation workload")
	}
	value.allocationLoad = allocation == "1"
	return value, nil
}

type matrixExperiment struct {
	operations  int
	size        uint64
	concurrency int
	workload    string
}

func parseMatrixExperiment(getenv func(string) string, diagnostic bool) (matrixExperiment, error) {
	value := matrixExperiment{operations: 2}
	operations, size, concurrency, workload := getenv("P07_MATRIX_OPERATIONS_PER_WORKER"), getenv("P07_MATRIX_SIZE"), getenv("P07_MATRIX_CONCURRENCY"), getenv("P07_MATRIX_WORKLOAD")
	if diagnostic && operations+size+concurrency+workload != "" {
		return value, fmt.Errorf("matrix overrides require normal workload mode")
	}
	if operations != "" {
		for _, character := range operations {
			if character < '0' || character > '9' {
				return value, fmt.Errorf("invalid matrix operation count")
			}
		}
		parsed, err := strconv.Atoi(operations)
		if err != nil || parsed < 256 || parsed > 4096 {
			return value, fmt.Errorf("matrix operation count must be 256..4096")
		}
		value.operations = parsed
	}
	if size != "" {
		switch size {
		case "4096", "65536", "1048576", "4194304":
			value.size, _ = strconv.ParseUint(size, 10, 64)
		default:
			return value, fmt.Errorf("invalid matrix payload size")
		}
	}
	if concurrency != "" {
		switch concurrency {
		case "1", "16", "32", "64", "128", "256":
			value.concurrency, _ = strconv.Atoi(concurrency)
		default:
			return value, fmt.Errorf("invalid matrix concurrency")
		}
	}
	if workload != "" && workload != "read" && workload != "write" && workload != "mixed" {
		return value, fmt.Errorf("invalid matrix workload")
	}
	value.workload = workload
	return value, nil
}

type admissionKey struct{}

func readAdmissionContext(ctx context.Context, window, concurrency int) context.Context {
	if window < concurrency {
		return context.WithValue(ctx, admissionKey{}, make(chan struct{}, window))
	}
	return ctx
}

func acquireRead(ctx context.Context) (func(), error) {
	gate, _ := ctx.Value(admissionKey{}).(chan struct{})
	if gate == nil {
		return func() {}, nil
	}
	select {
	case gate <- struct{}{}:
		return func() { <-gate }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
