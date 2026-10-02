package msgr

import (
	"sort"
	"testing"
)

func indexSessionFixture(owner *sessionOwner) {
	owner.pendingHead, owner.pendingTail = nil, nil
	entries := owner.pending
	owner.pending = owner.pending[:0]
	for _, pending := range entries {
		owner.addPending(pending)
	}
	for index, pending := range owner.replay {
		pending.inReplay, pending.replayIndex = true, index
	}
	ordered := append([]*pendingRequest(nil), owner.replay...)
	sort.SliceStable(ordered, func(first, second int) bool { return ordered[first].seq < ordered[second].seq })
	owner.replayHead, owner.replayTail = nil, nil
	for _, pending := range ordered {
		pending.replayPrevious, pending.replayNext = owner.replayTail, nil
		if owner.replayTail == nil {
			owner.replayHead = pending
		} else {
			owner.replayTail.replayNext = pending
		}
		owner.replayTail = pending
	}
}

func TestTrimReplayClearsBackingReferences(t *testing.T) {
	first := &pendingRequest{seq: 1}
	second := &pendingRequest{seq: 3}
	third := &pendingRequest{seq: 2}
	storage := []*pendingRequest{first, second, third}
	owner := &sessionOwner{replay: storage, pending: []*pendingRequest{first, second, third}}
	indexSessionFixture(owner)
	owner.trimReplay(2)
	if len(owner.replay) != 1 || owner.replay[0] != second || storage[1] != nil || storage[2] != nil {
		t.Fatal("acknowledged replay entries remain retained")
	}
	if len(owner.pending) != 3 || owner.pending[0] != first || owner.pending[2] != third {
		t.Fatal("acknowledgment removed requests still awaiting replies")
	}
	owner.trimReplay(3)
	if len(owner.replay) != 0 || storage[0] != nil {
		t.Fatal("last replay entry remains retained")
	}
}

func TestRemovePendingClearsBackingReferences(t *testing.T) {
	first := &pendingRequest{request: &submitCommand{}, message: Message{Header: MessageHeader{TransactionID: 1}}, bytes: 3}
	second := &pendingRequest{request: &submitCommand{}, message: Message{Header: MessageHeader{TransactionID: 2}}, bytes: 4}
	pendingStorage := []*pendingRequest{first, second}
	replayStorage := []*pendingRequest{first, second}
	owner := &sessionOwner{
		pending: pendingStorage, replay: replayStorage, retainedBytes: 7,
		byRequest: map[*submitCommand]*pendingRequest{first.request: first, second.request: second},
		byTID:     map[uint64]*pendingRequest{1: first, 2: second},
	}
	indexSessionFixture(owner)
	owner.removePending(first)
	if len(owner.pending) != 1 || owner.pending[0] != second || len(owner.replay) != 1 || owner.replay[0] != second {
		t.Fatal("removal changed remaining request order")
	}
	if pendingStorage[1] != nil || replayStorage[1] != nil || owner.retainedBytes != 4 {
		t.Fatal("removed request storage remains reachable")
	}
	owner.removePending(second)
	if pendingStorage[0] != nil || replayStorage[0] != nil || len(owner.byRequest) != 0 || len(owner.byTID) != 0 || owner.retainedBytes != 0 {
		t.Fatal("completed requests remain retained")
	}
}
