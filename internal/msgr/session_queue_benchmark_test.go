package msgr

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkSessionQueuedACK(benchmark *testing.B) {
	for _, count := range []int{100, 1000} {
		benchmark.Run(fmt.Sprintf("count=%d", count), func(benchmark *testing.B) {
			owner := &sessionOwner{config: performanceConfig(count)}
			owner.controlQueue = make([]Frame, 0, count)
			benchmark.ReportAllocs()
			for benchmark.Loop() {
				owner.controlQueue = owner.controlQueue[:0]
				for sequence := 1; sequence <= count; sequence++ {
					owner.queueControl(Ack{Sequence: uint64(sequence)})
				}
			}
			benchmark.ReportMetric(float64(len(owner.controlQueue)), "queued-frames/op")
		})
	}
}

func BenchmarkSessionSaturatedDispatch(benchmark *testing.B) {
	for _, depth := range []int{64, 1024, 4096} {
		benchmark.Run(fmt.Sprintf("depth=%d", depth), func(benchmark *testing.B) {
			owner := &sessionOwner{
				config: performanceConfig(depth), state: StateReady,
				writeTasks: make(chan writeTask, 1), pending: make([]*pendingRequest, depth),
			}
			owner.config.MaxInFlightTransactions = depth - 1
			owner.inFlight = depth - 1
			for index := range owner.pending {
				owner.pending[index] = &pendingRequest{
					request: &submitCommand{ctx: context.Background()}, sent: index != 0,
				}
			}
			benchmark.ReportAllocs()
			for benchmark.Loop() {
				owner.dispatch()
			}
			if owner.writeBusy || len(owner.writeTasks) != 0 || owner.pending[0].sent || owner.nextOutbound != 0 {
				benchmark.Fatal("saturated dispatch mutated state or sent a frame")
			}
		})
	}
}
