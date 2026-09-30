package msgr

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestRequestTimingCorrelatesSessionStages(t *testing.T) {
	for _, transactionID := range []uint64{0, 42} {
		t.Run(fmt.Sprint(transactionID), func(t *testing.T) {
			testRequestTimingStages(t, transactionID)
		})
	}
}

func testRequestTimingStages(t *testing.T, transactionID uint64) {
	t.Helper()
	transport := newFakeTransport()
	session := newTestSession(t, transport, nil, testSessionConfig(t))
	defer session.Stop()
	ctx, timing := WithRequestTiming(context.Background())
	message := testMessage("timed")
	message.Header.TransactionID = transactionID
	result := submitAsync(session, ctx, message)
	request := decodeWrittenMessage(t, transport)
	response := testMessage("response")
	response.Header.Sequence = 1
	response.Header.AckSequence = request.Header.Sequence
	response.Header.TransactionID = request.Header.TransactionID
	transport.inject(messageFrame(t, response))
	if outcome := waitOutcome(t, result); outcome.err != nil {
		t.Fatal(outcome.err)
	}
	session.Stop()
	events := timing.Events()
	stages := make(map[string]RequestTimingEvent)
	for _, event := range events {
		if event.At.IsZero() {
			t.Fatalf("missing timestamp: %+v", event)
		}
		if event.TransactionID != request.Header.TransactionID {
			t.Fatalf("incorrect transaction: %+v", event)
		}
		stages[event.Stage] = event
	}
	for _, stage := range []string{"read_enter", "submit_enter", "admitted", "write_begin", "write_end", "frame_received", "reply_delivered", "submit_return"} {
		if _, ok := stages[stage]; !ok {
			t.Fatalf("missing %s in %+v", stage, events)
		}
	}
	for _, pair := range [][2]string{{"read_enter", "submit_enter"}, {"submit_enter", "admitted"}, {"admitted", "write_begin"}, {"write_begin", "write_end"}, {"frame_received", "reply_delivered"}, {"reply_delivered", "submit_return"}} {
		if stages[pair[1]].At.Before(stages[pair[0]].At) {
			t.Fatalf("stage order reversed: %v", pair)
		}
	}
	if active := session.timingActive.Load(); active != 0 {
		t.Fatalf("timing still active: %d", active)
	}
}

func TestRequestTimingRecordsCanceledReturn(t *testing.T) {
	transport := newFakeTransport()
	session := newTestSession(t, transport, nil, testSessionConfig(t))
	defer session.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx, timing := WithRequestTiming(ctx)
	result := submitAsync(session, ctx, testMessage("cancel"))
	request := decodeWrittenMessage(t, transport)
	cancel()
	if outcome := waitOutcome(t, result); !errors.Is(outcome.err, context.Canceled) {
		t.Fatalf("outcome = %+v", outcome)
	}
	events := timing.Events()
	last := events[len(events)-1]
	if last.Stage != "submit_return" || last.TransactionID != request.Header.TransactionID {
		t.Fatalf("return event = %+v", last)
	}
	if session.timingActive.Load() != 0 {
		t.Fatal("timing remains active after cancellation")
	}
}
