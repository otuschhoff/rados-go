package msgr

import (
	"sync"
	"testing"
)

func TestReceiveScratchDefaultInventory(t *testing.T) {
	budget, _ := NewReceiveBudget(1, 1<<20)
	if budget.scratchSlots.Load() != 4 || budget.scratchMetrics.Load() {
		t.Fatal("default inventory or disabled diagnostics changed")
	}
}

func TestReceiveScratchConcurrentInventories(t *testing.T) {
	for _, slots := range []int{1, 2, 4, 8} {
		budget, _ := NewReceiveBudget(1, 8<<20)
		if err := budget.ConfigureScratchDiagnostic(slots); err != nil {
			t.Fatal(err)
		}
		scratch := &receiveScratch{}
		var workers sync.WaitGroup
		for worker := 0; worker < 8; worker++ {
			workers.Go(func() {
				for iteration := 0; iteration < 64; iteration++ {
					lease, backing := scratchRead(t, scratch, budget, 65568)
					backing[0] = 42
					lease.reclaim()
				}
			})
		}
		workers.Wait()
		if got := budget.Snapshot(); got.ScratchHits+got.ScratchMisses != 8*64 || got.ScratchBypasses != 0 {
			t.Fatalf("concurrent counters=%+v", got)
		}
		scratch.mu.Lock()
		if len(scratch.idle) > slots {
			t.Fatal("inventory exceeded slot limit")
		}
		scratch.mu.Unlock()
		if budget.Snapshot().RetainedBytes > uint64(slots*73728) {
			t.Fatal("idle charge exceeds capacity")
		}
		scratch.close()
		if budget.Snapshot().RetainedBytes != 0 {
			t.Fatal("concurrent inventory leaked charge")
		}
	}
}

func TestReceiveScratchDiagnosticInventories(t *testing.T) {
	for _, slots := range []int{1, 2, 4, 8} {
		budget, _ := NewReceiveBudget(1, 8<<20)
		if err := budget.ConfigureScratchDiagnostic(slots); err != nil {
			t.Fatal(err)
		}
		scratch := &receiveScratch{}
		leases := make([]*receiveLease, slots+1)
		for index := range leases {
			lease, backing := scratchRead(t, scratch, budget, 65568)
			for offset := range backing[:cap(backing)] {
				backing[:cap(backing)][offset] = 42
			}
			leases[index] = lease
		}
		for _, lease := range leases {
			lease.reclaim()
		}
		if len(scratch.idle) != slots || budget.Snapshot().RetainedBytes != uint64(slots*73728) {
			t.Fatalf("slots=%d snapshot=%+v", slots, budget.Snapshot())
		}
		if cap(scratch.idle) != 8 {
			t.Fatal("inventory exceeded inline storage")
		}
		for _, idle := range scratch.idle {
			if cap(idle.backing) != 73728 || !allZero(idle.backing) {
				t.Fatal("capacity changed or inventory retained plaintext")
			}
		}
		for index := 0; index < slots; index++ {
			lease, backing := scratchRead(t, scratch, budget, 65568)
			if backing[0] != 0 {
				t.Fatal("inventory retained plaintext")
			}
			leases[index] = lease
		}
		if got := budget.Snapshot(); got.ScratchHits != uint64(slots) || got.ScratchMisses != uint64(slots+1) {
			t.Fatalf("counters=%+v", got)
		}
		scratch.close()
		for index := 0; index < slots; index++ {
			leases[index].reclaim()
		}
		if budget.Snapshot().RetainedBytes != 0 {
			t.Fatal("inventory close leaked backing")
		}
	}
}

func TestReceiveScratchDiagnosticRejectsActiveReconfiguration(t *testing.T) {
	budget, _ := NewReceiveBudget(1, 1<<20)
	for _, slots := range []int{0, 9} {
		if budget.ConfigureScratchDiagnostic(slots) == nil {
			t.Fatal("invalid inventory accepted")
		}
	}
	if err := budget.admit(); err != nil {
		t.Fatal(err)
	}
	defer budget.retire()
	if budget.ConfigureScratchDiagnostic(4) == nil {
		t.Fatal("active configuration changed")
	}
}

func TestReceiveScratchDiagnosticRejectsChargedReconfiguration(t *testing.T) {
	budget, _ := NewReceiveBudget(1, 1<<20)
	lease := &receiveLease{budget: budget}
	if err := lease.resize(65536); err != nil {
		t.Fatal(err)
	}
	defer lease.release()
	if budget.ConfigureScratchDiagnostic(8) == nil {
		t.Fatal("charged configuration changed")
	}
}
