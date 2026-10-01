package msgr

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"testing"
)

type queueBurstFixture struct {
	owner           *sessionOwner
	entries         []pendingRequest
	pending, replay []*pendingRequest
	order           []int
}

func newQueueBurstFixture(depth int, order string) *queueBurstFixture {
	fixture := &queueBurstFixture{
		owner:   &sessionOwner{session: &Session{events: make(chan SessionEvent, 64)}, config: performanceConfig(depth)},
		entries: make([]pendingRequest, depth), pending: make([]*pendingRequest, depth),
		replay: make([]*pendingRequest, depth), order: make([]int, depth),
	}
	fixture.owner.byRequest = make(map[*submitCommand]*pendingRequest, depth)
	fixture.owner.byTID = make(map[uint64]*pendingRequest, depth)
	fixture.owner.controlQueue = make([]Frame, 0, depth)
	for index := range fixture.entries {
		fixture.entries[index] = pendingRequest{
			request: &submitCommand{ctx: context.Background(), result: make(chan submitResult, 1)},
			message: Message{Header: MessageHeader{TransactionID: uint64(index + 1), Sequence: uint64(index + 1)}},
			bytes:   MessageHeaderSize, seq: uint64(index + 1), mayHaveExecuted: true,
		}
		fixture.order[index] = index
	}
	if order == "reverse" {
		for index := range fixture.order {
			fixture.order[index] = depth - 1 - index
		}
	} else if order == "random" {
		rand.New(rand.NewSource(1)).Shuffle(depth, func(first, second int) {
			fixture.order[first], fixture.order[second] = fixture.order[second], fixture.order[first]
		})
	}
	return fixture
}

func (fixture *queueBurstFixture) reset() {
	owner := fixture.owner
	clear(owner.byRequest)
	clear(owner.byTID)
	owner.pending, owner.replay = fixture.pending, fixture.replay
	owner.controlQueue = owner.controlQueue[:0]
	owner.state, owner.lastInbound, owner.nextOutbound = StateReady, 0, uint64(len(fixture.entries)+1)
	owner.retainedBytes, owner.inFlight = uint64(len(fixture.entries))*MessageHeaderSize, len(fixture.entries)
	for index := range fixture.entries {
		pending := &fixture.entries[index]
		pending.sent = true
		fixture.pending[index], fixture.replay[index] = pending, pending
		owner.byRequest[pending.request] = pending
		owner.byTID[pending.message.Header.TransactionID] = pending
	}
}

func (fixture *queueBurstFixture) finish(operation string) {
	owner := fixture.owner
	if operation == "fault-drain" {
		owner.failAll(ErrSessionDisconnected)
	} else {
		for _, index := range fixture.order {
			pending := &fixture.entries[index]
			if operation == "cancel" {
				owner.cancel(cancelCommand{request: pending.request, err: context.Canceled})
			} else {
				owner.handleMessage(Message{Header: MessageHeader{Sequence: owner.lastInbound + 1, TransactionID: pending.message.Header.TransactionID}})
			}
		}
	}
	for index := range fixture.entries {
		<-fixture.entries[index].request.result
	}
}

func queueBurstMetrics(benchmark *testing.B, requests, actions int) {
	benchmark.ReportMetric(float64(requests), "requests/op")
	benchmark.ReportMetric(float64(actions), "actions/op")
	benchmark.ReportMetric(float64(benchmark.Elapsed().Nanoseconds())/float64(benchmark.N)/float64(requests), "ns/request")
}

func BenchmarkQueueBurst(benchmark *testing.B) {
	for _, depth := range []int{128, 1024, 4096} {
		for _, operation := range []string{"complete", "cancel", "fault-drain", "reset-only"} {
			orders := []string{"head", "reverse", "random"}
			if operation == "fault-drain" || operation == "reset-only" {
				orders = []string{"admission"}
			}
			for _, order := range orders {
				benchmark.Run(fmt.Sprintf("%s/depth=%d/order=%s/timed-reset", operation, depth, order), func(benchmark *testing.B) {
					fixture := newQueueBurstFixture(depth, order)
					benchmark.Log("One op is a depth-sized owner-only batch. Prebuilt requests/channels/payload/arrays excluded by B.Loop; timed reset rebuilds maps, slices, sent count, and replay. Completion includes handleMessage, ACK=0 replay scan, removePending, ACK encoding/queueing and result drain; cancel includes sent outcome-unknown and result drain; fault-drain measures failAll's fault cleanup, not reconnect/pumps. Reset-only measures the same reset without work. No per-iteration timer stops.")
					benchmark.ReportAllocs()
					for benchmark.Loop() {
						fixture.reset()
						if operation != "reset-only" {
							fixture.finish(operation)
						}
					}
					queueBurstMetrics(benchmark, depth, depth)
				})
			}
		}
		for _, width := range []int{1, 32, depth} {
			benchmark.Run(fmt.Sprintf("ack-trim/depth=%d/width=%d/timed-reset", depth, width), func(benchmark *testing.B) {
				fixture := newQueueBurstFixture(depth, "head")
				benchmark.Log("One op resets a prebuilt sent/replay batch (timed) and handles cumulative ACK controls in width-sized groups. Includes validation, trimReplay and event emission; pending requests remain live. No replies/admission/pumps. Width=depth is one cumulative ACK; width=1 rescans each shrinking suffix.")
				benchmark.ReportAllocs()
				for benchmark.Loop() {
					fixture.reset()
					for sequence := width; sequence <= depth; sequence += width {
						fixture.owner.handleControl(Ack{Sequence: uint64(sequence)})
					}
				}
				queueBurstMetrics(benchmark, depth, depth/width)
			})
		}
	}
}

func TestQueueBurstOrdersAndBacking(t *testing.T) {
	for _, depth := range []int{128, 1024, 4096} {
		for _, order := range []string{"head", "reverse", "random"} {
			for _, operation := range []string{"complete", "cancel", "fault-drain"} {
				t.Run(fmt.Sprintf("%s/%d/%s", operation, depth, order), func(t *testing.T) {
					fixture := newQueueBurstFixture(depth, order)
					for iteration := 0; iteration < 2; iteration++ {
						fixture.reset()
						fixture.finish(operation)
						owner := fixture.owner
						if len(owner.pending) != 0 || len(owner.replay) != 0 || len(owner.byTID) != 0 || len(owner.byRequest) != 0 || owner.inFlight != 0 || owner.retainedBytes != 0 {
							t.Fatal("burst retained bookkeeping")
						}
						if operation != "fault-drain" {
							for index := range fixture.pending {
								if fixture.pending[index] != nil || fixture.replay[index] != nil {
									t.Fatalf("retained backing slot %d", index)
								}
							}
						}
					}
				})
			}
		}
	}
}

func TestQueueBurstACKBatchPreservesPending(t *testing.T) {
	fixture := newQueueBurstFixture(128, "head")
	fixture.reset()
	for sequence := uint64(32); sequence <= 128; sequence += 32 {
		fixture.owner.handleControl(Ack{Sequence: sequence})
		if len(fixture.owner.replay) != 128-int(sequence) || len(fixture.owner.pending) != 128 || fixture.owner.inFlight != 128 {
			t.Fatal("ACK changed live request accounting")
		}
		for index, pending := range fixture.owner.replay {
			if pending.seq != sequence+uint64(index)+1 {
				t.Fatal("ACK reordered replay suffix")
			}
		}
		for _, pending := range fixture.replay[len(fixture.owner.replay):] {
			if pending != nil {
				t.Fatal("ACK retained trimmed backing")
			}
		}
	}
	fixture.owner.cancel(cancelCommand{request: fixture.entries[127].request, err: context.Canceled})
	result := <-fixture.entries[127].request.result
	if !errors.Is(result.err, context.Canceled) || !errors.Is(result.err, ErrOutcomeUnknown) || fixture.owner.inFlight != 127 {
		t.Fatal("ACKed sent cancellation lost outcome or count")
	}
}
