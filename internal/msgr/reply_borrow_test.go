package msgr

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBorrowedReplyHoldsReceiveLease(t *testing.T) {
	budget, err := NewReceiveBudget(1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	lease := &receiveLease{budget: budget}
	if err := lease.resize(64); err != nil {
		t.Fatal(err)
	}
	message, release, err := (submitResult{message: Message{Data: []byte("reply"), receiveLease: lease}}).borrow()
	if err != nil || string(message.Data) != "reply" || budget.Snapshot().RetainedBytes != 64 {
		t.Fatalf("reply=%q budget=%+v err=%v", message.Data, budget.Snapshot(), err)
	}
	release()
	release()
	if budget.Snapshot().RetainedBytes != 0 {
		t.Fatal("borrowed reply retained backing after release")
	}
}

type borrowedFrameTransport struct {
	*fakeTransport
	scratch receiveScratch
}

func (transport *borrowedFrameTransport) ReadFrameWithBudget(*ReceiveBudget, Limits) (Frame, error) {
	return transport.ReadFrame()
}

func (transport *borrowedFrameTransport) Close() error {
	err := transport.fakeTransport.Close()
	transport.scratch.close()
	return err
}

func TestBorrowedReplyCancellationAfterPublicationAndStop(t *testing.T) {
	budget, _ := NewReceiveBudget(1, 1<<20)
	config := testSessionConfig(t)
	config.Limits = performanceLimits
	config.ReceiveBudget = budget
	transport := &borrowedFrameTransport{fakeTransport: newFakeTransport()}
	session := newTestSession(t, transport, nil, config)
	defer session.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	admitted, resume := make(chan struct{}), make(chan struct{})
	finished := make(chan submitResult, 1)
	released := make(chan struct{})
	releaseFinished := make(chan struct{})
	go func() {
		message, release, err := session.SubmitBorrowedAdmitted(ctx, testMessage("request"), func() { close(admitted); <-resume })
		finished <- submitResult{message: message, err: err}
		<-released
		release()
		close(releaseFinished)
	}()
	<-admitted
	request := decodeWrittenMessage(t, transport.fakeTransport)
	lease, backing := scratchRead(t, &transport.scratch, budget, 65568)
	backing[0] = 42
	reply := testMessage("reply")
	reply.Header.Sequence, reply.Header.TransactionID = 1, request.Header.TransactionID
	reply.Data, reply.Lengths.Data = backing[:65536], 65536
	frame, err := EncodeMessage(reply, performanceLimits)
	if err != nil {
		t.Fatal(err)
	}
	frame.receiveLease = lease
	transport.inject(frame)
	waitSnapshot(t, session, func(snapshot SessionSnapshot) bool { return snapshot.InFlight == 0 })
	cancel()
	close(resume)
	select {
	case result := <-finished:
		if result.err != nil || len(result.message.Data) != 65536 || result.message.Data[0] != 42 || budget.Snapshot().RetainedBytes == 0 {
			t.Fatalf("result=%+v budget=%+v", result, budget.Snapshot())
		}
	case <-time.After(time.Second):
		t.Fatal("published borrowed reply lost during cancellation")
	}
	session.Stop()
	if budget.Snapshot().RetainedBytes == 0 {
		t.Fatal("Stop retired outstanding borrowed backing")
	}
	close(released)
	select {
	case <-releaseFinished:
		if budget.Snapshot().RetainedBytes != 0 {
			t.Fatal("borrow release leaked after Stop")
		}
	case <-time.After(time.Second):
		t.Fatal("borrow release did not complete after Stop")
	}
}

func TestBorrowedReplyErrorReleasesReceiveLease(t *testing.T) {
	budget, _ := NewReceiveBudget(1, 1024)
	lease := &receiveLease{budget: budget}
	if err := lease.resize(64); err != nil {
		t.Fatal(err)
	}
	_, release, err := (submitResult{message: Message{receiveLease: lease}, err: ErrSessionClosed}).borrow()
	if !errors.Is(err, ErrSessionClosed) || budget.Snapshot().RetainedBytes != 0 {
		t.Fatalf("budget=%+v err=%v", budget.Snapshot(), err)
	}
	release()
}
