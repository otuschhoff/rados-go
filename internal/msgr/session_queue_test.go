package msgr

import (
	"context"
	"testing"
	"time"
)

func markSessionPendingSent(owner *sessionOwner, pending *pendingRequest) {
	if !pending.sent && !pending.request.control {
		owner.inFlight++
	}
	pending.sent = true
}

func assertSessionInFlight(t *testing.T, owner *sessionOwner) {
	t.Helper()
	want := 0
	for _, pending := range owner.pending {
		if pending.sent && !pending.request.control {
			want++
		}
	}
	if owner.inFlightCount() != want || owner.snapshot().InFlight != want {
		t.Fatalf("in-flight = %d, snapshot = %d, independent scan = %d", owner.inFlightCount(), owner.snapshot().InFlight, want)
	}
}

func TestSessionInFlightTransitions(t *testing.T) {
	owner := newUnitSessionOwner(t)
	owner.writeTasks = make(chan writeTask, 1)
	request := &submitCommand{ctx: context.Background(), message: testMessage("application"), result: make(chan submitResult, 1)}
	owner.submit(request)
	assertSessionInFlight(t, owner)
	owner.dispatch()
	<-owner.writeTasks
	assertSessionInFlight(t, owner)
	pending := owner.byRequest[request]
	owner.handleControl(Ack{Sequence: pending.seq})
	assertSessionInFlight(t, owner)
	if len(owner.replay) != 0 || owner.inFlightCount() != 1 {
		t.Fatal("ACK freed application awaiting reply")
	}
	owner.handleMessage(Message{Header: MessageHeader{Sequence: 1, TransactionID: pending.message.Header.TransactionID}})
	assertSessionInFlight(t, owner)
	if result := <-request.result; result.err != nil {
		t.Fatal(result.err)
	}

	owner.writeBusy = false
	owner.controlQueue = nil
	oneWay := &submitCommand{ctx: context.Background(), message: testMessage("one-way"), oneWay: true, result: make(chan submitResult, 1)}
	owner.submit(oneWay)
	owner.dispatch()
	<-owner.writeTasks
	assertSessionInFlight(t, owner)
	if owner.inFlightCount() != 1 {
		t.Fatal("ordinary one-way request did not count before completion")
	}
	owner.removePending(owner.byRequest[oneWay])
	assertSessionInFlight(t, owner)
	owner.writeBusy = false
	control := controlUnitSubmit(owner, controlTestMessage(29))
	owner.dispatch()
	<-owner.writeTasks
	assertSessionInFlight(t, owner)
	if owner.inFlightCount() != 0 {
		t.Fatal("control counted as application")
	}
	owner.removePending(owner.byRequest[control])
	assertSessionInFlight(t, owner)
}

func newCountedSessionOwner(t *testing.T) *sessionOwner {
	t.Helper()
	owner := newUnitSessionOwner(t)
	owner.connectorRequests = make(chan connectRequest, 1)
	owner.writeTasks = make(chan writeTask, 1)
	owner.serverCookie = 22
	for index := 0; index < 4; index++ {
		request := &submitCommand{ctx: context.Background(), message: testMessage("application"), result: make(chan submitResult, 1)}
		if index == 2 {
			request.control, request.oneWay = true, true
			request.message = controlTestMessage(29)
		}
		owner.submit(request)
		assertSessionInFlight(t, owner)
		if index < 3 {
			owner.dispatch()
			<-owner.writeTasks
			owner.writeBusy = false
			assertSessionInFlight(t, owner)
		}
	}
	owner.handleControl(Ack{Sequence: 1})
	assertSessionInFlight(t, owner)
	if owner.inFlightCount() != 2 || len(owner.replay) != 2 {
		t.Fatal("incorrect counted fixture")
	}
	return owner
}

func TestSessionInFlightCleanupTransitions(t *testing.T) {
	for _, test := range []struct {
		name string
		act  func(*sessionOwner)
		want int
	}{
		{"cancel-sent", func(owner *sessionOwner) {
			owner.cancel(cancelCommand{request: owner.pending[0].request, err: context.Canceled})
		}, 1},
		{"cancel-unsent", func(owner *sessionOwner) {
			owner.cancel(cancelCommand{request: owner.pending[3].request, err: context.Canceled})
		}, 2},
		{"cancel-control", func(owner *sessionOwner) {
			owner.cancel(cancelCommand{request: owner.pending[2].request, err: context.Canceled})
		}, 2},
		{"replay-fault", func(owner *sessionOwner) { owner.handleFault(ErrSessionDisconnected) }, 1},
		{"read-malformed-fault", func(owner *sessionOwner) { owner.handleFrame(Frame{Tag: 0}) }, 1},
		{"fail-pending-fault", func(owner *sessionOwner) {
			owner.config.ReconnectPolicy = FailPending
			owner.handleFault(ErrSessionDisconnected)
		}, 0},
		{"lossy-fault", func(owner *sessionOwner) {
			owner.serverFlags = ConnectionFlagLossy
			owner.handleFault(ErrSessionDisconnected)
		}, 0},
		{"no-cookie-fault", func(owner *sessionOwner) { owner.serverCookie = 0; owner.handleFault(ErrSessionDisconnected) }, 0},
		{"partial-reset", func(owner *sessionOwner) { owner.handleReset(false) }, 1},
		{"full-reset", func(owner *sessionOwner) { owner.handleReset(true) }, 0},
		{"new-identity", func(owner *sessionOwner) { owner.resetForNewIdentity() }, 0},
		{"terminal", func(owner *sessionOwner) { owner.failTerminal(ErrMalformed) }, 0},
		{"fail-all", func(owner *sessionOwner) { owner.failAll(ErrSessionClosed) }, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			owner := newCountedSessionOwner(t)
			test.act(owner)
			assertSessionInFlight(t, owner)
			if owner.inFlightCount() != test.want {
				t.Fatalf("count = %d, want %d", owner.inFlightCount(), test.want)
			}
			owner.prepareReplay()
			assertSessionInFlight(t, owner)
			owner.prepareReplay()
			assertSessionInFlight(t, owner)
			owner.failAll(ErrSessionClosed)
			assertSessionInFlight(t, owner)
			owner.failAll(ErrSessionClosed)
			assertSessionInFlight(t, owner)
		})
	}
}

func TestSessionInFlightReplayIdempotentAndRenewal(t *testing.T) {
	owner := newCountedSessionOwner(t)
	owner.replay = append(owner.replay, &pendingRequest{request: &submitCommand{}, sent: true})
	for iteration := 0; iteration < 2; iteration++ {
		owner.prepareReplay()
		assertSessionInFlight(t, owner)
		if owner.inFlightCount() != 1 {
			t.Fatal("replay preparation decremented inactive, control, or already-unsent request")
		}
	}
	owner.replay = owner.replay[:len(owner.replay)-1]
	owner.dispatch()
	<-owner.writeTasks
	owner.writeBusy = false
	assertSessionInFlight(t, owner)
	if owner.inFlightCount() != 2 {
		t.Fatal("replayed application not counted")
	}
	owner.dispatch()
	<-owner.writeTasks
	owner.writeBusy = false
	assertSessionInFlight(t, owner)
	owner.removePending(owner.pending[2])
	assertSessionInFlight(t, owner)
	owner.handleControl(Ack{Sequence: 3})
	owner.renewalPending = true
	owner.dispatch()
	assertSessionInFlight(t, owner)
	if owner.state != StateReady || !owner.renewalPending {
		t.Fatal("renewal ignored ACKed applications awaiting replies")
	}
	for owner.pending[0].sent {
		owner.cancel(cancelCommand{request: owner.pending[0].request, err: context.Canceled})
		assertSessionInFlight(t, owner)
	}
	owner.dispatch()
	assertSessionInFlight(t, owner)
	if owner.state != StateDisconnected || owner.renewalPending {
		t.Fatal("renewal did not proceed after application drain")
	}
}

func TestSessionInFlightOneWayLateCompletion(t *testing.T) {
	owner := newUnitSessionOwner(t)
	owner.connectorRequests = make(chan connectRequest, 1)
	owner.connectorCancel = func() {}
	owner.frames = make(chan pumpFrame)
	owner.writes = make(chan pumpWriteResult)
	owner.writeTasks = make(chan writeTask, 1)
	request := &submitCommand{ctx: context.Background(), message: testMessage("one-way"), oneWay: true, result: make(chan submitResult, 1)}
	owner.submit(request)
	go owner.run()
	defer owner.session.Stop()
	generation := (<-owner.connectorRequests).generation
	select {
	case <-owner.writeTasks:
	case <-time.After(time.Second):
		t.Fatal("one-way write did not dispatch")
	}
	if snapshot := getSnapshot(t, owner.session); snapshot.InFlight != 1 {
		t.Fatalf("dispatched one-way in-flight = %d, want 1", snapshot.InFlight)
	}
	owner.writes <- pumpWriteResult{generation: generation - 1, request: request}
	if snapshot := getSnapshot(t, owner.session); snapshot.InFlight != 1 {
		t.Fatalf("stale completion in-flight = %d, want 1", snapshot.InFlight)
	}
	owner.writes <- pumpWriteResult{generation: generation, request: request}
	if snapshot := getSnapshot(t, owner.session); snapshot.InFlight != 0 {
		t.Fatalf("completed one-way in-flight = %d, want 0", snapshot.InFlight)
	}
	select {
	case result := <-request.result:
		if result.err != nil {
			t.Fatal(result.err)
		}
	default:
		t.Fatal("one-way completion did not report success")
	}
	owner.writes <- pumpWriteResult{generation: generation, request: request}
	if snapshot := getSnapshot(t, owner.session); snapshot.InFlight != 0 {
		t.Fatalf("late completion in-flight = %d, want 0", snapshot.InFlight)
	}
	select {
	case result := <-request.result:
		t.Fatalf("late completion reported twice: %+v", result)
	default:
	}
}
