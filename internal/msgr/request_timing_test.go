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

func TestRequestAttemptsObservePreparedRetriesAndReplay(t *testing.T) {
	ctx, observation := WithRequestAttempts(context.Background())
	if _, known := observation.RetryCount(); known {
		t.Fatal("unobserved requests reported as zero retries")
	}
	RecordPreparedRequest(ctx)
	if _, known := observation.RetryCount(); known {
		t.Fatal("missing messenger observation reported as known")
	}
	recordRequestDispatch(ctx, false)
	if retries, known := observation.RetryCount(); !known || retries != 0 {
		t.Fatalf("first request: retries=%d known=%t", retries, known)
	}
	recordRequestDispatch(ctx, true)
	RecordPreparedRequest(ctx)
	recordRequestDispatch(ctx, false)
	if retries, known := observation.RetryCount(); !known || retries != 2 {
		t.Fatalf("prepared retry plus replay: retries=%d known=%t", retries, known)
	}
	otherCtx, other := WithRequestAttempts(ctx)
	RecordPreparedRequest(otherCtx)
	recordRequestDispatch(otherCtx, false)
	if retries, known := other.RetryCount(); !known || retries != 0 {
		t.Fatalf("isolated observation: retries=%d known=%t", retries, known)
	}
	if retries, known := observation.RetryCount(); !known || retries != 2 {
		t.Fatal("nested operation contaminated parent observation")
	}
}

func TestRequestAttemptsCorrelateActualSessionDispatch(t *testing.T) {
	transport := newFakeTransport()
	session := newTestSession(t, transport, nil, testSessionConfig(t))
	defer session.Stop()
	ctx, observation := WithRequestAttempts(context.Background())
	RecordPreparedRequest(ctx)
	result := submitAsync(session, ctx, testMessage("observed"))
	request := decodeWrittenMessage(t, transport)
	response := testMessage("response")
	response.Header.Sequence = 1
	response.Header.AckSequence = request.Header.Sequence
	response.Header.TransactionID = request.Header.TransactionID
	transport.inject(messageFrame(t, response))
	if outcome := waitOutcome(t, result); outcome.err != nil {
		t.Fatal(outcome.err)
	}
	if retries, known := observation.RetryCount(); !known || retries != 0 {
		t.Fatalf("actual session dispatch: retries=%d known=%t", retries, known)
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
