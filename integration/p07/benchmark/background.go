package main

import (
	"crypto/sha256"
	"fmt"
	"runtime"
	"strconv"
	"sync"
)

func startBackgroundCPU(value string) (int, func(), error) {
	return startBackgroundWork(value, false)
}

func startBackgroundWork(value string, allocate bool) (int, func(), error) {
	if value == "" {
		if allocate {
			return 0, nil, fmt.Errorf("allocation workload requires positive workers")
		}
		return 0, func() {}, nil
	}
	count, err := strconv.Atoi(value)
	if err != nil || count < 0 || count > 32 {
		return 0, nil, fmt.Errorf("background CPU workers must be between 0 and 32")
	}
	if allocate && count == 0 {
		return 0, nil, fmt.Errorf("allocation workload requires positive workers")
	}
	done := make(chan struct{})
	var workers sync.WaitGroup
	var ready sync.WaitGroup
	ready.Add(count)
	for worker := 0; worker < count; worker++ {
		workers.Go(func() {
			var payload [4096]byte
			var retained [8][]byte
			iterations := 0
			ready.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				sum := sha256.Sum256(payload[:])
				payload[0] = sum[0]
				if allocate && iterations%64 == 0 {
					buffer := make([]byte, 64<<10)
					for index := 0; index < len(buffer); index += 4096 {
						buffer[index] = sum[0]
					}
					retained[(iterations/64)%len(retained)] = buffer
				}
				iterations++
				runtime.KeepAlive(retained)
				runtime.KeepAlive(sum)
			}
		})
	}
	ready.Wait()
	var stop sync.Once
	return count, func() { stop.Do(func() { close(done); workers.Wait() }) }, nil
}
