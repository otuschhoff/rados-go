package main

import (
	"fmt"
	"strconv"
)

const readWarmupPerWorker = 8

type readShape struct {
	size        uint64
	concurrency int
}

func parseReadShape(size, concurrency string, diagnostic, seedOnly bool) (readShape, error) {
	shape := readShape{size: 65536, concurrency: 16}
	if size+concurrency != "" && !diagnostic && !seedOnly {
		return shape, fmt.Errorf("read shape overrides require diagnostic or seed-only mode")
	}
	if size != "" {
		switch size {
		case "4096", "65536", "1048576", "4194304":
			shape.size, _ = strconv.ParseUint(size, 10, 64)
		default:
			return shape, fmt.Errorf("invalid diagnostic read size")
		}
	}
	if concurrency != "" {
		switch concurrency {
		case "16", "32", "64":
			shape.concurrency, _ = strconv.Atoi(concurrency)
		default:
			return shape, fmt.Errorf("invalid diagnostic read concurrency")
		}
	}
	return shape, nil
}

func (shape readShape) operationCount(perWorker int) int {
	return shape.concurrency * perWorker
}

func (shape readShape) objectSet() string {
	return fmt.Sprintf("%s0..%d", shape.objectPrefix(), shape.concurrency-1)
}

func (shape readShape) objectPrefix() string {
	if shape.size != 65536 {
		return fmt.Sprintf("p07-shared-read-size-%d-", shape.size)
	}
	return "p07-shared-read-"
}

func (shape readShape) objectName(workerID int) string {
	return fmt.Sprintf("%s%d", shape.objectPrefix(), workerID)
}

func (shape readShape) matches(size uint64, concurrency int, workload string) bool {
	return size == shape.size && concurrency == shape.concurrency && workload == "read"
}
