package objecter

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/otuschhoff/rados-go/internal/maps"
	"github.com/otuschhoff/rados-go/internal/msgr"
	"github.com/otuschhoff/rados-go/internal/osd"
)

func newBackoffBurstSession(ranges int) (*osdSession, *fakeOSDTransport) {
	transport := newFakeOSDTransport()
	session := &osdSession{
		raw: transport, backoffs: make(map[uint64]osd.Backoff, ranges),
		submissions: make(map[uint64]*targetSubmission), changed: make(chan struct{}),
	}
	for index := 0; index < ranges; index++ {
		object := osd.HObject{Object: "blocked", Pool: 7, Snapshot: osd.NoSnap}
		session.addBackoff(osd.Backoff{
			ID: uint64(index + 1), PG: maps.PG{Pool: 7, Seed: uint32(index + 1), Preferred: -1},
			Shard: -1, Operation: osd.BackoffBlock, Begin: object, End: object,
		})
	}
	return session, transport
}

func backoffBurstMetrics(benchmark *testing.B, requests int) {
	benchmark.ReportMetric(float64(requests), "requests/op")
	benchmark.ReportMetric(float64(benchmark.Elapsed().Nanoseconds())/float64(benchmark.N)/float64(requests), "ns/request")
}

func BenchmarkBackoffBurst(benchmark *testing.B) {
	for _, ranges := range []int{1, 128, 1024, 4096} {
		for _, waiters := range []int{1, 128, 1024, 4096} {
			benchmark.Run(fmt.Sprintf("wait-rescan/ranges=%d/waiters=%d/setup-excluded", ranges, waiters), func(benchmark *testing.B) {
				session, _ := newBackoffBurstSession(ranges)
				pg := maps.PG{Pool: 7, Seed: 0, Preferred: -1}
				object := osd.HObject{Object: "unrelated", Pool: 7, Snapshot: osd.NoSnap}
				benchmark.Log("One op is W synchronous real Wait calls with B unrelated-PG ranges. Measures indexed predicate lookup under the mutex, not blocked goroutines or scheduler wake latency. Fixture construction excluded by B.Loop; no reset required.")
				benchmark.ReportAllocs()
				for benchmark.Loop() {
					for waiter := 0; waiter < waiters; waiter++ {
						if err := session.Wait(context.Background(), pg, object); err != nil {
							benchmark.Fatal(err)
						}
					}
				}
				backoffBurstMetrics(benchmark, waiters)
				benchmark.ReportMetric(0, "range-visits/op")
			})
		}
		benchmark.Run(fmt.Sprintf("submit-register/ranges=%d/batch=128/setup-excluded", ranges), func(benchmark *testing.B) {
			session, _ := newBackoffBurstSession(ranges)
			pg := maps.PG{Pool: 7, Seed: 0, Preferred: -1}
			object := osd.HObject{Object: "unrelated", Pool: 7, Snapshot: osd.NoSnap}
			benchmark.Log("One op is 128 real SubmitTarget calls: PG-indexed lookup, child context, submission registration, immediate fake transport admission/unlock and reply, deregistration and cancel. Prebuilt fixture excluded; no wire/pumps/network, outstanding submissions or sleeping goroutines.")
			benchmark.ReportAllocs()
			for benchmark.Loop() {
				for request := 0; request < 128; request++ {
					if _, err := session.SubmitTarget(context.Background(), pg, object, msgr.Message{}); err != nil {
						benchmark.Fatal(err)
					}
				}
			}
			backoffBurstMetrics(benchmark, 128)
		})
		for _, submissions := range []int{1, 128, 1024, 4096} {
			benchmark.Run(fmt.Sprintf("unblock/ranges=%d/submissions=%d/timed-reset", ranges, submissions), func(benchmark *testing.B) {
				fixture, transport := newBackoffBurstSession(ranges)
				transport.incoming = make(chan msgr.Message)
				session := newOSDSession(transport, backoffTestLimits, time.Second)
				defer session.Stop()
				session.mu.Lock()
				session.backoffs = fixture.backoffs
				session.backoffsByPG = fixture.backoffsByPG
				blocked := session.backoffs[1]
				entries := make([]targetSubmission, submissions)
				cancellations := 0
				for index := range entries {
					entries[index] = targetSubmission{pg: maps.PG{Pool: 7, Seed: 0, Preferred: -1}, object: blocked.Begin, cancel: func() { cancellations++ }}
					if index == 0 {
						entries[index].pg = blocked.PG
					}
					session.addSubmission(uint64(index+1), &entries[index])
				}
				session.mu.Unlock()
				unblock := blocked
				unblock.Operation = osd.BackoffUnblock
				message := encodeBackoffMessage(benchmark, unblock)
				benchmark.Log("One op is one real receive-dispatch unblock: timed restore of one range and S resend flags, unbuffered injection, decode, ID deletion, PG-indexed admitted-submission lookup (exactly one match/cancel), affected-PG notification, and unbuffered second-message barrier. Two fixed session workers, no S goroutines; setup/message encoding excluded. S flag resets remain linear and timed. Does NOT include waiter rescans, ACK writer progress or scheduler wake fanout. ns/request means per fixture submission, not per unblock lookup.")
				benchmark.ReportAllocs()
				for benchmark.Loop() {
					session.mu.Lock()
					session.addBackoff(blocked)
					for index := range entries {
						entries[index].resend = false
					}
					cancellations = 0
					session.mu.Unlock()
					transport.incoming <- message
					transport.incoming <- msgr.Message{}
					session.mu.Lock()
					if cancellations != 1 || !entries[0].resend || len(session.backoffs) != ranges-1 {
						benchmark.Fatal("unblock missed registered target")
					}
					session.mu.Unlock()
				}
				backoffBurstMetrics(benchmark, submissions)
				benchmark.ReportMetric(1, "unblocks/op")
				benchmark.ReportMetric(1, "cancels/op")
			})
		}
	}
}

type backoffBurstWaitContext struct {
	context.Context
	entered chan struct{}
}

func (ctx backoffBurstWaitContext) Done() <-chan struct{} {
	ctx.entered <- struct{}{}
	return ctx.Context.Done()
}

func TestBackoffBurstUnblockDoesNotWakeUnrelatedWaiters(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fixture, transport := newBackoffBurstSession(128)
	transport.incoming = make(chan msgr.Message)
	session := newOSDSession(transport, backoffTestLimits, time.Second)
	defer session.Stop()
	session.mu.Lock()
	session.backoffs = fixture.backoffs
	session.backoffsByPG = fixture.backoffsByPG
	first, second := session.backoffs[1], session.backoffs[2]
	changed := session.changed
	session.mu.Unlock()
	const group = 16
	entered := make([]chan struct{}, group*2)
	results := make([]chan error, group*2)
	for index := range entered {
		entered[index], results[index] = make(chan struct{}, 4), make(chan error, 1)
		blocked := first
		if index >= group {
			blocked = second
		}
		go func(index int, blocked osd.Backoff) {
			results[index] <- session.Wait(backoffBurstWaitContext{Context: ctx, entered: entered[index]}, blocked.PG, blocked.Begin)
		}(index, blocked)
	}
	await := func(signal <-chan struct{}) {
		t.Helper()
		select {
		case <-signal:
		case <-ctx.Done():
			t.Fatal("waiter handshake timed out")
		}
	}
	for _, signal := range entered {
		await(signal)
	}
	session.mu.Lock()
	unrelatedChanged := session.backoffWaiters[second.PG].changed
	session.mu.Unlock()
	first.Operation = osd.BackoffUnblock
	select {
	case transport.incoming <- encodeBackoffMessage(t, first):
	case <-ctx.Done():
		t.Fatal("unblock injection timed out")
	}
	await(changed)
	for index := 0; index < group; index++ {
		select {
		case err := <-results[index]:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("matched waiter did not finish")
		}
	}
	for index := group; index < group*2; index++ {
		select {
		case err := <-results[index]:
			t.Fatalf("unrelated blocked waiter returned: %v", err)
		default:
		}
	}
	session.mu.Lock()
	if session.backoffWaiters[second.PG].changed != unrelatedChanged {
		t.Fatal("unrelated PG notification changed")
	}
	if len(session.backoffs) != 127 || session.backoffs[2].ID != 2 {
		t.Fatal("unblock removed unrelated ranges")
	}
	session.mu.Unlock()
	cancel()
	for index := group; index < group*2; index++ {
		select {
		case err := <-results[index]:
			if !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("cancel did not drain waiter")
		}
	}
	session.mu.Lock()
	if len(session.backoffWaiters) != 0 {
		t.Fatal("cancellation retained PG subscriptions")
	}
	session.mu.Unlock()
	t.Log("32 waiters registered; one unblock releases 16 matching waiters without notifying 16 unrelated waiters; cancellation removes all subscriptions")
}
