package msgr

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"testing"
	"time"
)

type budgetStressRenewalTransport struct {
	Transport
	due chan struct{}
}

func (transport *budgetStressRenewalTransport) RenewalDue() <-chan struct{} {
	return transport.due
}

func (transport *budgetStressRenewalTransport) ReadFrameWithBudget(budget *ReceiveBudget, limits Limits) (Frame, error) {
	return ReadTransportFrame(transport.Transport, budget, limits)
}

type budgetStressGeneration struct {
	transport *connTransport
	wrapper   *budgetStressRenewalTransport
	peer      net.Conn
	codec     Codec
}

func TestReceiveBudgetSecureReconnectStress(t *testing.T) {
	const cycles = 16
	const payloadSize = 512 << 10
	budget, err := NewReceiveBudget(1, 2<<20)
	if err != nil {
		t.Fatal(err)
	}
	config := testSessionConfig(t)
	config.Limits.MaxSegmentBytes = 1 << 20
	config.Limits.MaxFrameBytes = 2 << 20
	config.ReceiveBudget = budget
	config.MaxQueuedReceiveBytes = 1 << 20
	config.ClientCookie, config.ServerCookie = 11, 22
	config.reconnectWait = func(context.Context, time.Duration) error { return nil }
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	newGeneration := func() (budgetStressGeneration, error) {
		secret := bytes.Repeat([]byte{0x5a}, 64)
		peerCodec, err := NewSecureCodec(secret, false)
		if err != nil {
			return budgetStressGeneration{}, err
		}
		localCodec, err := NewSecureCodec(secret, true)
		if err != nil {
			return budgetStressGeneration{}, err
		}
		local, peer := net.Pipe()
		_ = peer.SetDeadline(time.Now().Add(15 * time.Second))
		base, err := NewConnTransport(local, localCodec, config.Limits)
		if err != nil {
			_ = local.Close()
			_ = peer.Close()
			return budgetStressGeneration{}, err
		}
		wrapper := &budgetStressRenewalTransport{Transport: base, due: make(chan struct{})}
		return budgetStressGeneration{transport: base.(*connTransport), wrapper: wrapper, peer: peer, codec: peerCodec}, nil
	}
	first, err := newGeneration()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.peer.Close() })
	generations := make(chan budgetStressGeneration, 1)
	connector := ConnectorFunc(func(connectCtx context.Context) (Transport, error) {
		generation, err := newGeneration()
		if err != nil {
			return nil, err
		}
		select {
		case generations <- generation:
			return generation.wrapper, nil
		case <-connectCtx.Done():
			_ = generation.peer.Close()
			_ = generation.wrapper.Close()
			return nil, connectCtx.Err()
		}
	})
	session, err := NewSession(first.wrapper, connector, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(session.Stop)
	current := first
	var retained []Message
	assertClosed := func(generation budgetStressGeneration) {
		t.Helper()
		generation.transport.readMu.Lock()
		closed, reader := generation.transport.closed, generation.transport.reader
		generation.transport.readMu.Unlock()
		if !closed || reader != nil {
			t.Fatal("retired Secure transport retained reader")
		}
		select {
		case <-generation.wrapper.due:
			t.Fatal("transport Close fired dormant renewal notification")
		default:
		}
	}
	for cycle := range cycles {
		message := testMessage("secure reconnect stress")
		message.Header.Sequence = uint64(cycle + 1)
		message.Data = bytes.Repeat([]byte{byte(cycle + 1)}, payloadSize)
		message.Lengths.Data = uint32(len(message.Data))
		frame, err := EncodeMessage(message, config.Limits)
		if err != nil {
			t.Fatal(err)
		}
		wire, err := current.codec.Encode(frame, config.Limits)
		if err != nil {
			t.Fatal(err)
		}
		acknowledged := make(chan error, 1)
		go func(generation budgetStressGeneration) {
			_, err := generation.peer.Write(wire)
			if err == nil {
				var ack Frame
				ack, err = generation.codec.Read(generation.peer, config.Limits)
				if err == nil && ack.Tag != TagAck {
					err = fmt.Errorf("expected Ack, got %v", ack.Tag)
				}
			}
			acknowledged <- err
		}(current)
		select {
		case err := <-acknowledged:
			if err != nil {
				t.Fatalf("cycle %d: %v", cycle, err)
			}
		case <-ctx.Done():
			t.Fatal("Secure message acknowledgment stalled")
		}
		clear(wire)
		snapshot, err := session.Snapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		ledger := budget.Snapshot()
		if snapshot.QueuedReceiveMessages != 1 || snapshot.RetainedReceiveBytes < payloadSize || ledger.RetainedBytes < snapshot.RetainedReceiveBytes || ledger.RetainedBytes > 2<<20 || ledger.ControlBytes > receiveControlBytesPerSession || ledger.Sessions != 1 {
			t.Fatalf("cycle %d stalled delivery snapshot=%+v ledger=%+v", cycle, snapshot, ledger)
		}
		if cycle%2 == 0 {
			select {
			case received := <-session.Incoming():
				if received.TransportGeneration != session.ControlGeneration() {
					t.Fatal("wrong transport generation delivered")
				}
				retained = append(retained, received)
			case <-ctx.Done():
				t.Fatal("application handoff stalled")
			}
		}
		old := current
		_ = old.peer.Close()
		select {
		case current = <-generations:
			peer := current.peer
			t.Cleanup(func() { _ = peer.Close() })
		case <-ctx.Done():
			t.Fatal("Secure reconnect connector stalled")
		}
		assertClosed(old)
		reconnect, err := current.codec.Read(current.peer, config.Limits)
		if err != nil || reconnect.Tag != TagSessionReconnect {
			t.Fatalf("cycle %d reconnect tag=%v error=%v", cycle, reconnect.Tag, err)
		}
		wire, err = current.codec.Encode(controlFrame(t, SessionReconnectOK{}), config.Limits)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := current.peer.Write(wire); err != nil {
			t.Fatal(err)
		}
		waitEvent(t, session, EventReconnectOK)
		snapshot, err = session.Snapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		ledger = budget.Snapshot()
		if snapshot.QueuedReceiveMessages != 0 || snapshot.RetainedReceiveBytes != 0 || ledger.RetainedBytes != 0 || ledger.Sessions != 1 || ledger.ControlBytes > receiveControlBytesPerSession {
			t.Fatalf("cycle %d generation reclamation snapshot=%+v ledger=%+v", cycle, snapshot, ledger)
		}
	}
	session.Stop()
	assertClosed(current)
	if ledger := budget.Snapshot(); ledger != (ReceiveBudgetSnapshot{}) {
		t.Fatalf("Stop did not join pumps/watchers and reclaim ledger: %+v", ledger)
	}
	for index, message := range retained {
		if len(message.Data) != payloadSize || !bytes.Equal(message.Data, bytes.Repeat([]byte{byte(2*index + 1)}, payloadSize)) {
			t.Fatalf("application-owned payload %d changed across reconnect/Stop", index)
		}
	}
}
