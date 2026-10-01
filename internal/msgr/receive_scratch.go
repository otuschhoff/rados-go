package msgr

import "sync"

const maxReceiveScratch = 128 << 10

type receiveScratch struct {
	mu      sync.Mutex
	idle    []*receiveLease
	storage [8]*receiveLease
	closed  bool
}

func (scratch *receiveScratch) get(lease *receiveLease, size int) ([]byte, error) {
	if size < 32<<10 || size > maxReceiveScratch {
		lease.budget.recordScratch(false, true)
		return make([]byte, size), nil
	}
	capacity := (size + 8191) &^ 8191
	scratch.mu.Lock()
	defer scratch.mu.Unlock()
	var backing []byte
	if len(scratch.idle) != 0 {
		index := len(scratch.idle) - 1
		idle := scratch.idle[index]
		scratch.idle[index] = nil
		scratch.idle = scratch.idle[:index]
		if index == 0 {
			scratch.idle = nil
		}
		if idle.budget == lease.budget && cap(idle.backing) >= size {
			backing = idle.backing
			capacity = cap(backing)
		}
		idle.release()
	}
	extra := uint64(capacity - size)
	lease.extra = extra
	if err := lease.resize(lease.bytes); err != nil {
		lease.extra = 0
		lease.budget.recordScratch(false, true)
		return make([]byte, size), nil
	}
	lease.budget.recordScratch(backing != nil, false)
	if backing == nil {
		backing = make([]byte, capacity)
	}
	lease.backing, lease.scratch = backing, scratch
	return backing[:size], nil
}

func (scratch *receiveScratch) put(lease *receiveLease) {
	clear(lease.backing)
	scratch.mu.Lock()
	defer scratch.mu.Unlock()
	if scratch.closed || len(scratch.idle) >= int(lease.budget.scratchSlots.Load()) {
		lease.release()
		return
	}
	lease.releaseControl()
	budget := lease.budget
	budget.mu.Lock()
	capacity := uint64(cap(lease.backing))
	budget.bytes = budget.bytes - lease.bytes + capacity
	if scratch.idle == nil {
		scratch.idle = scratch.storage[:0]
	}
	scratch.idle = append(scratch.idle, &receiveLease{budget: budget, bytes: capacity, backing: lease.backing})
	lease.bytes, lease.extra, lease.backing, lease.scratch = 0, 0, nil, nil
	budget.mu.Unlock()
}

func (scratch *receiveScratch) dropIdle() {
	scratch.mu.Lock()
	defer scratch.mu.Unlock()
	for index, lease := range scratch.idle {
		lease.release()
		scratch.idle[index] = nil
	}
	scratch.idle = nil
}

func (scratch *receiveScratch) close() {
	scratch.mu.Lock()
	defer scratch.mu.Unlock()
	scratch.closed = true
	for index, lease := range scratch.idle {
		lease.release()
		scratch.idle[index] = nil
	}
	scratch.idle = nil
}
