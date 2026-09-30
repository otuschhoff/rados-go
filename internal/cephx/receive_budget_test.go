package cephx

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/otuschhoff/rados-go/internal/msgr"
)

type budgetForwardTransport struct {
	budget *msgr.ReceiveBudget
	limits msgr.Limits
	err    error
}

func TestReadBudgetAuthTransportSecureApplicationHandoff(t *testing.T) {
	secret := bytes.Repeat([]byte{0x5a}, ConnectionSecretSizeSecure)
	sender, err := msgr.NewSecureCodec(secret, false)
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := msgr.NewSecureCodec(secret, true)
	if err != nil {
		t.Fatal(err)
	}
	limits := msgr.Limits{MaxSegmentBytes: 4096, MaxFrameBytes: 8192}
	left, peer := net.Pipe()
	defer peer.Close()
	base, err := msgr.NewConnTransport(left, receiver, limits)
	if err != nil {
		t.Fatal(err)
	}
	auth := newAuthTransport(base, AuthMetadata{}, time.Now().Add(time.Hour), time.Now())
	defer auth.Close()
	if !auth.OwnsReadFrames() {
		t.Fatal("auth wrapper lost secure frame ownership")
	}
	budget, err := msgr.NewReceiveBudget(1, 16384)
	if err != nil {
		t.Fatal(err)
	}
	config := msgr.SessionConfig{
		Limits: limits, ReceiveBudget: budget, MaxQueuedReceiveBytes: 8192,
		MaxQueuedMessages: 4, MaxRetainedBytes: 8192, MaxInFlightTransactions: 4,
		MaxReconnectAttempts: 1, MaxHandshakeTransitions: 8, EventBuffer: 16,
	}
	session, err := msgr.NewSession(auth, nil, config)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Stop()
	front := bytes.Repeat([]byte("f"), 1024)
	middle := bytes.Repeat([]byte("m"), 33)
	message := msgr.Message{Header: msgr.MessageHeader{Sequence: 1}, Front: front, Middle: middle,
		Lengths: msgr.MessageLengths{Front: uint32(len(front)), Middle: uint32(len(middle))}}
	frame, err := msgr.EncodeMessage(message, limits)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := sender.Encode(frame, limits)
	if err != nil {
		t.Fatal(err)
	}
	acknowledged := make(chan error, 1)
	go func() {
		if _, err := peer.Write(wire); err != nil {
			acknowledged <- err
			return
		}
		ack, err := sender.Read(peer, limits)
		if err == nil && ack.Tag != msgr.TagAck {
			err = errors.New("expected secure message acknowledgment")
		}
		acknowledged <- err
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	select {
	case err := <-acknowledged:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("secure message was not acknowledged")
	}
	clear(wire)
	snapshot, err := session.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.QueuedReceiveMessages != 1 || snapshot.RetainedReceiveBytes <= uint64(len(front)+len(middle)) {
		t.Fatalf("secure queue did not retain real backing: %+v", snapshot)
	}
	if ledger := budget.Snapshot(); ledger.RetainedBytes != snapshot.RetainedReceiveBytes || ledger.Sessions != 1 {
		t.Fatalf("secure backing not charged through wrapper: %+v", ledger)
	}
	var received msgr.Message
	select {
	case received = <-session.Incoming():
	case <-ctx.Done():
		t.Fatal("secure application handoff blocked")
	}
	if _, err := session.Snapshot(ctx); err != nil {
		t.Fatal(err)
	}
	if ledger := budget.Snapshot(); ledger.RetainedBytes != 0 {
		t.Fatalf("application backing remained charged: %+v", ledger)
	}
	session.Stop()
	if !bytes.Equal(received.Front, front) || !bytes.Equal(received.Middle, middle) {
		t.Fatal("application views changed after ciphertext overwrite or Close")
	}
	if ledger := budget.Snapshot(); ledger != (msgr.ReceiveBudgetSnapshot{}) {
		t.Fatalf("secure Stop leaked: %+v", ledger)
	}
	if _, err := auth.ReadFrameWithBudget(budget, limits); !errors.Is(err, msgr.ErrSessionClosed) {
		t.Fatalf("auth read after Close: %v", err)
	}
	select {
	case <-auth.RenewalDue():
		t.Fatal("Close fired renewal instead of canceling it")
	default:
	}
}

func (*budgetForwardTransport) ReadFrame() (msgr.Frame, error) {
	return msgr.Frame{}, errors.New("unbudgeted read")
}
func (*budgetForwardTransport) WriteFrame(msgr.Frame) error { return nil }
func (*budgetForwardTransport) Close() error                { return nil }
func (transport *budgetForwardTransport) ReadFrameWithBudget(budget *msgr.ReceiveBudget, limits msgr.Limits) (msgr.Frame, error) {
	transport.budget, transport.limits = budget, limits
	return msgr.Frame{}, transport.err
}

func TestAuthTransportForwardsReceiveBudget(t *testing.T) {
	budget, err := msgr.NewReceiveBudget(1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	limits := msgr.Limits{MaxSegmentBytes: 256, MaxFrameBytes: 512}
	base := &budgetForwardTransport{err: msgr.ErrReceiveBudgetExceeded}
	auth := newAuthTransport(base, AuthMetadata{}, time.Time{}, time.Time{})
	defer auth.Close()
	_, err = msgr.ReadTransportFrame(auth, budget, limits)
	if !errors.Is(err, msgr.ErrReceiveBudgetExceeded) || base.budget != budget || base.limits != limits {
		t.Fatalf("forwarding budget=%p limits=%+v err=%v", base.budget, base.limits, err)
	}
}
