package msgr

import (
	"bytes"
	"errors"
	"strconv"
	"sync"
	"testing"
)

func scratchRead(t *testing.T, scratch *receiveScratch, budget *ReceiveBudget, size int) (*receiveLease, []byte) {
	t.Helper()
	lease := &receiveLease{budget: budget}
	if err := lease.resize(uint64(size)); err != nil {
		t.Fatal(err)
	}
	backing, err := scratch.get(lease, size)
	if err != nil {
		t.Fatal(err)
	}
	return lease, backing
}

func TestReceiveScratchReuseIsChargedAndWiped(t *testing.T) {
	for _, size := range []int{4096, 4160, 65568} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			testReceiveScratchReuseIsChargedAndWiped(t, size)
		})
	}
}

func testReceiveScratchReuseIsChargedAndWiped(t *testing.T, size int) {
	t.Helper()
	budget, _ := NewReceiveBudget(1, 1<<20)
	scratch := &receiveScratch{}
	lease, backing := scratchRead(t, scratch, budget, size)
	backing[0] = 42
	address := &backing[0]
	lease.reclaim()
	if budget.Snapshot().RetainedBytes != uint64(cap(backing)) {
		t.Fatal("idle scratch is not charged")
	}
	lease, next := scratchRead(t, scratch, budget, size)
	if &next[0] != address || next[0] != 0 {
		t.Fatal("scratch not reused or plaintext not wiped")
	}
	lease.reclaim()
	scratch.close()
	if budget.Snapshot().RetainedBytes != 0 {
		t.Fatal("close retained scratch")
	}
}

func TestReceiveScratchOrdinaryHandoffNeverRecycles(t *testing.T) {
	for _, size := range []int{4096, 4160, 65568} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			testReceiveScratchOrdinaryHandoffNeverRecycles(t, size)
		})
	}
}

func testReceiveScratchOrdinaryHandoffNeverRecycles(t *testing.T, size int) {
	t.Helper()
	budget, _ := NewReceiveBudget(1, 1<<20)
	scratch := &receiveScratch{}
	lease, backing := scratchRead(t, scratch, budget, size)
	backing[0] = 42
	lease.release()
	if scratch.idle != nil || budget.Snapshot().RetainedBytes != 0 {
		t.Fatal("ordinary handoff recycled caller backing")
	}
	nextLease, next := scratchRead(t, scratch, budget, size)
	next[0] = 21
	if backing[0] != 42 {
		t.Fatal("caller backing overwritten")
	}
	scratch.close()
	nextLease.reclaim()
	if budget.Snapshot().RetainedBytes != 0 {
		t.Fatal("retired active scratch remained charged")
	}
}

type scratchConn struct{ budgetBytesConn }

func (*scratchConn) Close() error { return nil }

func TestSecureScratchMixedOwnership(t *testing.T) {
	for _, size := range []int{4096, 65536} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			testSecureScratchMixedOwnership(t, size)
		})
	}
}

func testSecureScratchMixedOwnership(t *testing.T, size int) {
	t.Helper()
	encoder, decoder := performanceCodecs(t, "secure")
	var wire []byte
	for index := 0; index < 3; index++ {
		encoded, err := encoder.Encode(Frame{Tag: TagMessage, Segments: []Segment{{Alignment: DefaultAlignment, Data: make([]byte, 80)}, {Alignment: DefaultAlignment, Data: bytes.Repeat([]byte{byte(index + 1)}, size)}}}, performanceLimits)
		if err != nil {
			t.Fatal(err)
		}
		wire = append(wire, encoded...)
	}
	transport, err := NewConnTransport(&scratchConn{budgetBytesConn{Reader: bytes.NewReader(wire)}}, decoder, performanceLimits)
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()
	budget, _ := NewReceiveBudget(1, 1<<20)
	first, err := ReadTransportFrame(transport, budget, performanceLimits)
	if err != nil {
		t.Fatal(err)
	}
	address := &first.Segments[1].Data[0]
	_, release, err := (submitResult{message: Message{Data: first.Segments[1].Data, receiveLease: first.receiveLease}}).borrow()
	if err != nil {
		t.Fatal(err)
	}
	release()
	second, err := ReadTransportFrame(transport, budget, performanceLimits)
	if err != nil || &second.Segments[1].Data[0] != address {
		t.Fatalf("reuse err=%v", err)
	}
	owned, err := (submitResult{message: Message{Data: second.Segments[1].Data, receiveLease: second.receiveLease}}).take()
	if err != nil {
		t.Fatal(err)
	}
	third, err := ReadTransportFrame(transport, budget, performanceLimits)
	if err != nil {
		t.Fatal(err)
	}
	third.receiveLease.reclaim()
	if !bytes.Equal(owned.Data, bytes.Repeat([]byte{2}, size)) {
		t.Fatal("ordinary Read backing reused")
	}
}

func TestSecureScratchAuthenticationFailureWipesBacking(t *testing.T) {
	encoder, decoder := performanceCodecs(t, "secure")
	wire, err := encoder.Encode(Frame{Tag: TagMessage, Segments: []Segment{{Alignment: DefaultAlignment, Data: make([]byte, 80)}, {Alignment: DefaultAlignment, Data: bytes.Repeat([]byte{42}, 65536)}}}, performanceLimits)
	if err != nil {
		t.Fatal(err)
	}
	wire[len(wire)-1] ^= 1
	transport, _ := NewConnTransport(&scratchConn{budgetBytesConn{Reader: bytes.NewReader(wire)}}, decoder, performanceLimits)
	budget, _ := NewReceiveBudget(1, 1<<20)
	_, err = ReadTransportFrame(transport, budget, performanceLimits)
	if !errors.Is(err, ErrIntegrity) {
		t.Fatalf("error=%v", err)
	}
	idle := transport.(*connTransport).scratch.idle
	if len(idle) != 1 || !allZero(idle[0].backing) {
		t.Fatal("failed authentication retained nonzero scratch")
	}
	transport.Close()
	if budget.Snapshot().RetainedBytes != 0 {
		t.Fatal("failed authentication leaked budget")
	}
}

func TestReceiveScratchConcurrentReleaseAndClose(t *testing.T) {
	budget, _ := NewReceiveBudget(1, 8<<20)
	scratch := &receiveScratch{}
	var workers sync.WaitGroup
	for index := 0; index < 8; index++ {
		workers.Go(func() {
			for iteration := 0; iteration < 32; iteration++ {
				lease, backing := scratchRead(t, scratch, budget, 65568)
				backing[0] = 42
				lease.reclaim()
			}
		})
	}
	scratch.close()
	workers.Wait()
	if budget.Snapshot().RetainedBytes != 0 {
		t.Fatal("close/release race leaked backing")
	}
}

func TestReceiveScratchTightBudgetFallback(t *testing.T) {
	budget, _ := NewReceiveBudget(1, 65568)
	scratch := &receiveScratch{}
	lease, backing := scratchRead(t, scratch, budget, 65568)
	if lease.scratch != nil || cap(backing) != 65568 || budget.Snapshot().RetainedBytes != 65568 {
		t.Fatal("tight-budget exact allocation changed")
	}
	lease.reclaim()
	if budget.Snapshot().RetainedBytes != 0 {
		t.Fatal("fallback leaked backing")
	}
}

func TestReceiveScratchIdleEvictionAtCapacity(t *testing.T) {
	budget, _ := NewReceiveBudget(1, 80<<10)
	scratch := &receiveScratch{}
	lease, _ := scratchRead(t, scratch, budget, 65568)
	lease.reclaim()
	next := &receiveLease{budget: budget}
	reader := budgetReader{lease: next, scratch: scratch}
	if err := reader.reserveReceiveBytes(65568); err != nil {
		t.Fatal(err)
	}
	if scratch.idle != nil || budget.Snapshot().RetainedBytes != 65568 {
		t.Fatal("idle scratch was not evicted before saturation")
	}
	next.release()
	scratch.close()
}

func TestSecureScratchOutstandingBorrowAcrossClose(t *testing.T) {
	budget, _ := NewReceiveBudget(2, 1<<20)
	old, replacement := &receiveScratch{}, &receiveScratch{}
	lease, backing := scratchRead(t, old, budget, 65568)
	backing[0] = 42
	old.close()
	next, data := scratchRead(t, replacement, budget, 65568)
	data[0] = 21
	if backing[0] != 42 {
		t.Fatal("old-generation borrow changed")
	}
	lease.reclaim()
	next.reclaim()
	replacement.close()
	if budget.Snapshot().RetainedBytes != 0 {
		t.Fatal("old generation leaked backing")
	}
}

func TestReceiveScratchLargeRecordDoesNotRetain(t *testing.T) {
	budget, _ := NewReceiveBudget(1, 8<<20)
	scratch := &receiveScratch{}
	lease, backing := scratchRead(t, scratch, budget, 4<<20)
	if lease.scratch != nil || cap(backing) != 4<<20 {
		t.Fatal("large record entered scratch slot")
	}
	lease.reclaim()
	if scratch.idle != nil || budget.Snapshot().RetainedBytes != 0 {
		t.Fatal("large record remained charged")
	}
}
