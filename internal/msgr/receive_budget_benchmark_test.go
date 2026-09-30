package msgr

import (
	"bytes"
	"runtime"
	"sync"
	"testing"
)

func BenchmarkReceiveBudgetSecureRead(benchmark *testing.B) {
	const batchSize = 100
	secret := testSecureSecret()
	sender, err := NewSecureCodec(secret, true)
	if err != nil {
		benchmark.Fatal(err)
	}
	message := Message{
		Lengths: MessageLengths{Front: 128, Middle: 32, Data: 65536},
		Front:   make([]byte, 128), Middle: make([]byte, 32), Data: make([]byte, 65536),
	}
	frame, err := EncodeMessage(message, performanceLimits)
	if err != nil {
		benchmark.Fatal(err)
	}
	var wire bytes.Buffer
	for range batchSize {
		encoded, err := sender.Encode(frame, performanceLimits)
		if err != nil {
			benchmark.Fatal(err)
		}
		wire.Write(encoded)
	}
	for _, enableBudget := range []bool{false, true} {
		name := "Unbudgeted"
		if enableBudget {
			name = "EnableBudget"
		}
		benchmark.Run(name, func(benchmark *testing.B) {
			var budget *ReceiveBudget
			if enableBudget {
				budget, err = NewReceiveBudget(1, 1<<20)
				if err != nil {
					benchmark.Fatal(err)
				}
			}
			benchmark.ReportAllocs()
			benchmark.SetBytes(64000)
			benchmark.ResetTimer()
			for completed := 0; completed < benchmark.N; {
				benchmark.StopTimer()
				decoder, err := NewSecureCodec(secret, false)
				if err != nil {
					benchmark.Fatal(err)
				}
				transport, err := NewConnTransport(&budgetBytesConn{Reader: bytes.NewReader(wire.Bytes())}, decoder, performanceLimits)
				if err != nil {
					benchmark.Fatal(err)
				}
				count := min(batchSize, benchmark.N-completed)
				benchmark.StartTimer()
				for range count {
					frame, err := ReadTransportFrame(transport, budget, performanceLimits)
					if err != nil {
						benchmark.Fatal(err)
					}
					decoded, err := decodeOwnedMessage(frame, performanceLimits)
					frame.receiveLease.release()
					if err != nil {
						benchmark.Fatal(err)
					}
					if len(decoded.Front) != 128 || len(decoded.Middle) != 32 || len(decoded.Data) != 65536 {
						benchmark.Fatal("decoded payload lengths changed")
					}
					runtime.KeepAlive(decoded)
				}
				completed += count
			}
			benchmark.StopTimer()
			if budget != nil && budget.Snapshot() != (ReceiveBudgetSnapshot{}) {
				benchmark.Fatal("receive budget leaked")
			}
		})
	}
}

func BenchmarkReceiveBudgetLeaseLifecycle(benchmark *testing.B) {
	budget, err := NewReceiveBudget(1, 64)
	if err != nil {
		benchmark.Fatal(err)
	}
	lease := receiveLease{budget: budget}
	benchmark.ReportAllocs()
	benchmark.ResetTimer()
	for iteration := 0; iteration < benchmark.N; iteration++ {
		if err := lease.resize(64); err != nil {
			benchmark.Fatal(err)
		}
		lease.release()
	}
	benchmark.StopTimer()
	if budget.Snapshot() != (ReceiveBudgetSnapshot{}) {
		benchmark.Fatal("receive budget leaked")
	}
}

func TestReceiveBudgetLeaseConcurrentRelease(t *testing.T) {
	var nilLease *receiveLease
	nilLease.release()
	budget, err := NewReceiveBudget(1, 128)
	if err != nil {
		t.Fatal(err)
	}
	occupied := &receiveLease{budget: budget}
	if err := occupied.resize(64); err != nil {
		t.Fatal(err)
	}
	defer occupied.release()
	for range 100 {
		lease := &receiveLease{budget: budget}
		if err := lease.resize(64); err != nil {
			t.Fatal(err)
		}
		if err := (budgetReader{lease: lease}).reserveReceivePrelude(); err != nil {
			t.Fatal(err)
		}
		var carriers sync.WaitGroup
		for range 8 {
			carriers.Go(func() {
				lease.release()
				lease.releaseControl()
				_ = budget.Snapshot()
			})
		}
		carriers.Wait()
		lease.release()
		if got := budget.Snapshot(); got != (ReceiveBudgetSnapshot{RetainedBytes: 64}) {
			t.Fatalf("release changed another lease: %+v", got)
		}
	}
}
