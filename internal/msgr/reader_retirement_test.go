package msgr

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

type retirementConn struct {
	*recordingConn
	entered chan struct{}
	closed  chan struct{}
	release chan struct{}
	once    sync.Once
}

func (connection *retirementConn) Read([]byte) (int, error) {
	close(connection.entered)
	<-connection.closed
	<-connection.release
	return 0, net.ErrClosed
}

func (connection *retirementConn) Close() error {
	connection.once.Do(func() { close(connection.closed) })
	return nil
}

func (*retirementConn) Write([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func TestReadBudgetCloseJoinsReaderRetirement(t *testing.T) {
	for _, budgeted := range []bool{false, true} {
		name := "plain"
		if budgeted {
			name = "budgeted"
		}
		t.Run(name, func(t *testing.T) {
			connection := &retirementConn{recordingConn: &recordingConn{}, entered: make(chan struct{}), closed: make(chan struct{}), release: make(chan struct{})}
			transport, err := NewConnTransport(connection, CRCCodec{}, performanceLimits)
			if err != nil {
				t.Fatal(err)
			}
			budget, err := NewReceiveBudget(1, 8192)
			if err != nil {
				t.Fatal(err)
			}
			read := func() error {
				if budgeted {
					_, err := transport.(*connTransport).ReadFrameWithBudget(budget, performanceLimits)
					return err
				}
				_, err := transport.ReadFrame()
				return err
			}
			readDone := make(chan error, 1)
			go func() { readDone <- read() }()
			<-connection.entered
			closeDone := make(chan error, 1)
			go func() { closeDone <- transport.Close() }()
			<-connection.closed
			select {
			case err := <-closeDone:
				close(connection.release)
				<-readDone
				t.Fatalf("Close returned before active decoder retired: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
			close(connection.release)
			if err := <-readDone; err == nil {
				t.Fatal("closed read succeeded")
			}
			if err := <-closeDone; err != nil {
				t.Fatal(err)
			}
			if transport.(*connTransport).reader != nil {
				t.Fatal("closed transport retained reader storage")
			}
			if err := read(); !errors.Is(err, ErrSessionClosed) {
				t.Fatalf("read after close: %v", err)
			}
			if snapshot := budget.Snapshot(); snapshot != (ReceiveBudgetSnapshot{}) {
				t.Fatalf("close leaked receive storage: %+v", snapshot)
			}
		})
	}
}

func TestReadBudgetReconnectJoinsOldReaderBeforeReplacement(t *testing.T) {
	connection := &retirementConn{recordingConn: &recordingConn{}, entered: make(chan struct{}), closed: make(chan struct{}), release: make(chan struct{})}
	first, err := NewConnTransport(connection, CRCCodec{}, performanceLimits)
	if err != nil {
		t.Fatal(err)
	}
	budget, _ := NewReceiveBudget(1, 8192)
	config := testSessionConfig(t)
	config.ReceiveBudget = budget
	config.Limits = performanceLimits
	config.ClientCookie, config.ServerCookie = 11, 22
	replacement := make(chan *connTransport, 1)
	left, peer := net.Pipe()
	defer peer.Close()
	go func() { _, _ = io.Copy(io.Discard, peer) }()
	connector := ConnectorFunc(func(context.Context) (Transport, error) {
		if first.(*connTransport).reader != nil {
			return nil, errors.New("replacement began before reader retirement")
		}
		transport, err := NewConnTransport(left, CRCCodec{}, performanceLimits)
		if err == nil {
			replacement <- transport.(*connTransport)
		}
		return transport, err
	})
	session := newTestSession(t, first, connector, config)
	var releaseOnce sync.Once
	defer func() {
		releaseOnce.Do(func() { close(connection.release) })
		session.Stop()
		_ = left.Close()
	}()
	<-connection.entered
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	sent := make(chan error, 1)
	go func() { sent <- session.Send(ctx, testMessage("write fault")) }()
	select {
	case <-connection.closed:
	case <-ctx.Done():
		t.Fatal("write fault did not close old connection")
	}
	select {
	case <-replacement:
		t.Fatal("replacement allocated while old reader was active")
	case <-time.After(20 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(connection.release) })
	select {
	case next := <-replacement:
		if next.reader == nil || first.(*connTransport).reader != nil {
			t.Fatal("reader lifetime did not transfer at Close")
		}
	case <-ctx.Done():
		t.Fatal("replacement did not start after old reader retired")
	}
	if err := <-sent; err == nil {
		t.Fatal("faulted write succeeded")
	}
	session.Stop()
	if snapshot := budget.Snapshot(); snapshot != (ReceiveBudgetSnapshot{}) {
		t.Fatalf("reconnect stop leaked: %+v", snapshot)
	}
}

func TestReadBudgetConcurrentCopiedLeaseRelease(t *testing.T) {
	budget, _ := NewReceiveBudget(1, 8192)
	lease := &receiveLease{budget: budget}
	if err := lease.resize(1024); err != nil {
		t.Fatal(err)
	}
	if err := (budgetReader{lease: lease}).reserveReceivePrelude(); err != nil {
		t.Fatal(err)
	}
	frame := Frame{receiveLease: lease}
	message := Message{receiveLease: lease}
	var workers sync.WaitGroup
	for range 32 {
		workers.Add(2)
		go func(copy Frame) { defer workers.Done(); copy.receiveLease.release() }(frame)
		go func(copy Message) { defer workers.Done(); copy.receiveLease.release() }(message)
	}
	workers.Wait()
	if snapshot := budget.Snapshot(); snapshot != (ReceiveBudgetSnapshot{}) {
		t.Fatalf("concurrent release leaked or underflowed: %+v", snapshot)
	}
}

type reusableBudgetCodec struct {
	storage []byte
	frame   Frame
}

func (*reusableBudgetCodec) Encode(Frame, Limits) ([]byte, error) { return nil, nil }
func (codec *reusableBudgetCodec) Read(io.Reader, Limits) (Frame, error) {
	if codec.frame.Tag != 0 {
		return codec.frame, nil
	}
	return Frame{Tag: TagAck, Segments: []Segment{{Alignment: DefaultAlignment, Data: codec.storage}}}, nil
}

func TestReadBudgetCustomCodecDetachesAndValidates(t *testing.T) {
	codec := &reusableBudgetCodec{storage: []byte("reusable")}
	transport, _ := NewConnTransport(&recordingConn{}, codec, performanceLimits)
	defer transport.Close()
	if transport.(*connTransport).OwnsReadFrames() {
		t.Fatal("custom codec claimed owned frames")
	}
	budget, _ := NewReceiveBudget(1, uint64(len(codec.storage)))
	frame, err := ReadTransportFrame(transport, budget, performanceLimits)
	if err != nil {
		t.Fatal(err)
	}
	defer frame.receiveLease.release()
	codec.storage[0] = 'X'
	if string(frame.Segments[0].Data) != "reusable" || budget.Snapshot().RetainedBytes != uint64(len(codec.storage)) {
		t.Fatal("custom codec storage was not detached and charged")
	}
	if _, err := ReadTransportFrame(transport, budget, performanceLimits); !errors.Is(err, ErrReceiveBudgetExceeded) {
		t.Fatalf("copy admitted without headroom: %v", err)
	}
	frame.receiveLease.release()
	codec.frame = Frame{Tag: TagAck, Segments: []Segment{{Alignment: 3, Data: codec.storage}}}
	if _, err := ReadTransportFrame(transport, budget, performanceLimits); !errors.Is(err, ErrMalformed) {
		t.Fatalf("invalid custom frame accepted: %v", err)
	}
	if snapshot := budget.Snapshot(); snapshot != (ReceiveBudgetSnapshot{}) {
		t.Fatalf("custom codec failure leaked: %+v", snapshot)
	}
}

func TestReadBudgetStaleLeasedPumpFrameAfterReplacement(t *testing.T) {
	budget, _ := NewReceiveBudget(1, 8192)
	if err := budget.admit(); err != nil {
		t.Fatal(err)
	}
	owner := newFuzzSessionOwner(t)
	owner.config.ReceiveBudget = budget
	owner.session.incoming = make(chan Message)
	owner.session.resets = make(chan struct{}, 1)
	owner.session.terminal = make(chan error, 1)
	owner.transport = newFakeTransport()
	owner.generation = 7
	owner.lastInbound = 42
	owner.session.controlGeneration.Store(17)
	owner.frames = make(chan pumpFrame)
	owner.writes = make(chan pumpWriteResult)
	owner.faults = make(chan pumpFault)
	owner.renewals = make(chan renewalDue)
	owner.session.stopped = make(chan struct{})
	go owner.run()
	defer owner.session.Stop()
	getSnapshot(t, owner.session)
	lease := &receiveLease{budget: budget}
	if err := lease.resize(1024); err != nil {
		t.Fatal(err)
	}
	if err := (budgetReader{lease: lease}).reserveReceivePrelude(); err != nil {
		t.Fatal(err)
	}
	message := testMessage("stale generation")
	message.Header.Sequence = 99
	frame := messageFrame(t, message)
	frame.receiveLease = lease
	select {
	case owner.frames <- pumpFrame{generation: 7, frame: frame}:
	case <-time.After(time.Second):
		t.Fatal("stale pump frame blocked")
	}
	if snapshot := getSnapshot(t, owner.session); snapshot.QueuedReceiveMessages != 0 || snapshot.LastInboundSequence != 42 || snapshot.State != StateReady {
		t.Fatalf("stale frame changed owner: %+v", snapshot)
	}
	if generation := owner.session.ControlGeneration(); generation != 17 {
		t.Fatalf("stale frame overrode current generation: %d", generation)
	}
	if snapshot := budget.Snapshot(); snapshot.RetainedBytes != 0 || snapshot.ControlBytes != 0 || snapshot.Sessions != 1 {
		t.Fatalf("stale frame lease survived replacement: %+v", snapshot)
	}
	owner.session.Stop()
	if snapshot := budget.Snapshot(); snapshot != (ReceiveBudgetSnapshot{}) {
		t.Fatalf("stale-frame stop leaked: %+v", snapshot)
	}
}
