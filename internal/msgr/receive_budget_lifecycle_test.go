package msgr

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

func TestReceiveBudgetControlPipelineAtCapacity(t *testing.T) {
	for _, mode := range []string{"crc", "secure"} {
		t.Run(mode, func(t *testing.T) {
			encoder, decoder := performanceCodecs(t, mode)
			var wire []byte
			for sequence := uint64(1); sequence <= 3; sequence++ {
				encoded, err := encoder.Encode(controlFrame(t, Ack{Sequence: sequence}), performanceLimits)
				if err != nil {
					t.Fatal(err)
				}
				wire = append(wire, encoded...)
			}
			budget, _ := NewReceiveBudget(1, 1)
			occupied := &receiveLease{budget: budget}
			if err := occupied.resize(1); err != nil {
				t.Fatal(err)
			}
			defer occupied.release()
			transport, _ := NewConnTransport(&budgetBytesConn{Reader: bytes.NewReader(wire)}, decoder, performanceLimits)
			var frames []Frame
			for range 3 {
				frame, err := ReadTransportFrame(transport, budget, performanceLimits)
				if err != nil {
					t.Fatal(err)
				}
				frames = append(frames, frame)
			}
			if snapshot := budget.Snapshot(); snapshot.RetainedBytes != 1 || snapshot.ControlBytes != receiveControlBytesPerSession {
				t.Fatal(snapshot)
			}
			for _, frame := range frames {
				frame.receiveLease.release()
			}
			if snapshot := budget.Snapshot(); snapshot.ControlBytes != 0 {
				t.Fatal(snapshot)
			}
		})
	}
}

func TestReceiveBudgetReconnectReleasesGenerationQueue(t *testing.T) {
	budget, _ := NewReceiveBudget(1, 8192)
	config := testSessionConfig(t)
	config.ReceiveBudget = budget
	config.MaxQueuedReceiveBytes = 4096
	config.ClientCookie, config.ServerCookie = 11, 22
	first, second := newFakeTransport(), newFakeTransport()
	session := newTestSession(t, first, oneTransportConnector(second), config)
	defer session.Stop()
	message := testMessage("old generation")
	message.Header.Sequence = 1
	first.inject(messageFrame(t, message))
	decodeWrittenControl(t, first)
	if snapshot := getSnapshot(t, session); snapshot.QueuedReceiveMessages != 1 {
		t.Fatal(snapshot)
	}
	first.fail(io.EOF)
	if _, ok := decodeWrittenControl(t, second).(SessionReconnect); !ok {
		t.Fatal("missing reconnect")
	}
	second.inject(controlFrame(t, SessionReconnectOK{}))
	waitEvent(t, session, EventReconnectOK)
	if snapshot := getSnapshot(t, session); snapshot.QueuedReceiveMessages != 0 || snapshot.RetainedReceiveBytes != 0 {
		t.Fatalf("old queue survived: %+v", snapshot)
	}
	if snapshot := budget.Snapshot(); snapshot.RetainedBytes != 0 || snapshot.Sessions != 1 {
		t.Fatalf("reconnect ledger %+v", snapshot)
	}
	message.Front = []byte("new generation")
	message.Header.Sequence = 2
	second.inject(messageFrame(t, message))
	decodeWrittenControl(t, second)
	received := <-session.Incoming()
	if string(received.Front) != "new generation" || received.TransportGeneration != session.ControlGeneration() {
		t.Fatalf("wrong generation delivery: %+v", received)
	}
	session.Stop()
	if snapshot := budget.Snapshot(); snapshot != (ReceiveBudgetSnapshot{}) {
		t.Fatalf("reconnect stop leaked %+v", snapshot)
	}
}

func TestReceiveBudgetCanceledReplyReclamation(t *testing.T) {
	budget, _ := NewReceiveBudget(1, 8192)
	config := testSessionConfig(t)
	config.ReceiveBudget = budget
	config.MaxQueuedReceiveBytes = 4096
	transport := newFakeTransport()
	session := newTestSession(t, transport, nil, config)
	defer session.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	result := submitAsync(session, ctx, testMessage("request"))
	sent := decodeWrittenMessage(t, transport)
	cancel()
	if outcome := waitOutcome(t, result); !errors.Is(outcome.err, context.Canceled) {
		t.Fatal(outcome.err)
	}
	reply := testMessage("late reply")
	reply.Header.Sequence = 1
	reply.Header.TransactionID = sent.Header.TransactionID
	transport.inject(messageFrame(t, reply))
	decodeWrittenControl(t, transport)
	if snapshot := getSnapshot(t, session); snapshot.QueuedReceiveMessages != 1 {
		t.Fatal(snapshot)
	}
	session.Stop()
	if snapshot := budget.Snapshot(); snapshot != (ReceiveBudgetSnapshot{}) {
		t.Fatalf("late reply leaked %+v", snapshot)
	}
}

func TestReceiveBudgetAuthControlCopyReservation(t *testing.T) {
	budget, _ := NewReceiveBudget(1, 50)
	config := testSessionConfig(t)
	config.ReceiveBudget = budget
	transport := newFakeTransport()
	session := newTestSession(t, transport, nil, config)
	defer session.Stop()
	transport.inject(controlFrame(t, AuthReplyMore{AuthPayload: make([]byte, 30)}))
	select {
	case err := <-session.Terminal():
		if !errors.Is(err, ErrReceiveBudgetExceeded) {
			t.Fatalf("copy headroom error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("auth copy reservation did not fail terminal")
	}
	session.Stop()
	if snapshot := budget.Snapshot(); snapshot != (ReceiveBudgetSnapshot{}) {
		t.Fatalf("auth copy failure leaked %+v", snapshot)
	}
}
