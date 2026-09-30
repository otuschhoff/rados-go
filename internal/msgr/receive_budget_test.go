package msgr

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestReceiveBudgetCodecPrefixReservation(t *testing.T) {
	for _, mode := range []string{"crc", "secure"} {
		t.Run(mode, func(t *testing.T) {
			encoder, decoder := performanceCodecs(t, mode)
			frame := Frame{Tag: TagAck, Segments: []Segment{{Alignment: DefaultAlignment, Data: make([]byte, 1024)}}}
			wire, err := encoder.Encode(frame, performanceLimits)
			if err != nil {
				t.Fatal(err)
			}
			prefix := PreambleSize
			if mode == "secure" {
				prefix = securePreamble
			}
			budget, _ := NewReceiveBudget(1, 100)
			lease := &receiveLease{budget: budget}
			_, err = decoder.Read(budgetReader{Reader: bytes.NewReader(wire[:prefix]), lease: lease}, performanceLimits)
			if !errors.Is(err, ErrReceiveBudgetExceeded) {
				t.Fatalf("prefix error = %v", err)
			}
			lease.release()
			if got := budget.Snapshot().RetainedBytes; got != 0 {
				t.Fatalf("leaked %d bytes", got)
			}
		})
	}
}

func TestReceiveBudgetCodecBackingAndFailures(t *testing.T) {
	for _, mode := range []string{"crc", "secure"} {
		for _, size := range []int{1, 48, 49, 1024} {
			t.Run(mode+"/"+strconv.Itoa(size), func(t *testing.T) {
				for _, corrupt := range []bool{false, true} {
					encoder, decoder := performanceCodecs(t, mode)
					frame := Frame{Tag: TagMessage, Segments: []Segment{
						{Alignment: DefaultAlignment, Data: make([]byte, size)},
						{Alignment: DefaultAlignment, Data: make([]byte, 33)},
					}}
					wire, err := encoder.Encode(frame, performanceLimits)
					if err != nil {
						t.Fatal(err)
					}
					if corrupt {
						corruptIndex := len(wire) - 1
						if mode == "crc" {
							corruptIndex = len(wire) - 12
						}
						wire[corruptIndex] ^= 1
					}
					connection := &budgetBytesConn{Reader: bytes.NewReader(wire)}
					transport, err := NewConnTransport(connection, decoder, performanceLimits)
					if err != nil {
						t.Fatal(err)
					}
					budget, _ := NewReceiveBudget(1, 8192)
					got, err := ReadTransportFrame(transport, budget, performanceLimits)
					if corrupt {
						if err == nil {
							t.Fatal("corruption accepted")
						}
						if snapshot := budget.Snapshot(); snapshot.RetainedBytes != 0 || snapshot.ControlBytes != 0 {
							t.Fatalf("error leaked: %+v", snapshot)
						}
						continue
					}
					if err != nil {
						t.Fatal(err)
					}
					want := uint64(size + 33)
					if mode == "secure" {
						first := paddedSecureLength(uint64(size))
						if first <= secureInlineSize {
							first = securePreamble
						} else {
							first += securePreamble
						}
						want = first + paddedSecureLength(33) + secureBlockSize + secureTagSize
					}
					if snapshot := budget.Snapshot(); snapshot.RetainedBytes != want || snapshot.ControlBytes != 0 {
						t.Fatalf("backing snapshot = %+v want %d", snapshot, want)
					}
					got.receiveLease.release()
					if snapshot := budget.Snapshot(); snapshot != (ReceiveBudgetSnapshot{}) {
						t.Fatalf("release leaked: %+v", snapshot)
					}
				}
			})
		}
	}
}

type budgetBytesConn struct {
	net.Conn
	*bytes.Reader
}

func (connection *budgetBytesConn) Read(data []byte) (int, error) {
	return connection.Reader.Read(data)
}

func TestReceiveBudgetSecureControlAtCapacity(t *testing.T) {
	encoder, decoder := performanceCodecs(t, "secure")
	wire, err := encoder.Encode(controlFrame(t, Ack{Sequence: 1}), performanceLimits)
	if err != nil {
		t.Fatal(err)
	}
	budget, _ := NewReceiveBudget(1, 1)
	occupied := &receiveLease{budget: budget}
	if err := occupied.resize(1); err != nil {
		t.Fatal(err)
	}
	defer occupied.release()
	transport, _ := NewConnTransport(&budgetBytesConn{Reader: bytes.NewReader(wire)}, decoder, performanceLimits)
	frame, err := ReadTransportFrame(transport, budget, performanceLimits)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot := budget.Snapshot(); snapshot.RetainedBytes != 1 || snapshot.ControlBytes != securePreamble {
		t.Fatal(snapshot)
	}
	frame.receiveLease.release()
	if snapshot := budget.Snapshot(); snapshot.ControlBytes != 0 {
		t.Fatal(snapshot)
	}
}

func TestReceiveBudgetSessionQueueDeliveryAndSaturation(t *testing.T) {
	for _, saturation := range []string{"bytes", "messages", "shared", "delivery"} {
		t.Run(saturation, func(t *testing.T) {
			budget, _ := NewReceiveBudget(2, 8192)
			config := testSessionConfig(t)
			config.ReceiveBudget = budget
			config.MaxQueuedReceiveBytes = 100
			if saturation == "messages" {
				config.MaxQueuedMessages = 1
				config.MaxQueuedReceiveBytes = 8192
			}
			if saturation == "shared" {
				budget.maxBytes = 100
				config.MaxQueuedReceiveBytes = 8192
			}
			transport := newFakeTransport()
			session := newTestSession(t, transport, nil, config)
			defer session.Stop()
			message := testMessage("retained")
			message.Header.Sequence = 1
			transport.inject(messageFrame(t, message))
			decodeWrittenControl(t, transport)
			snapshot := getSnapshot(t, session)
			if snapshot.QueuedReceiveMessages != 1 || snapshot.RetainedReceiveBytes != MessageHeaderSize+8 {
				t.Fatalf("queue snapshot = %+v", snapshot)
			}
			if got := budget.Snapshot().RetainedBytes; got != MessageHeaderSize+8 {
				t.Fatalf("budget retained %d", got)
			}
			if saturation == "delivery" {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				var received Message
				select {
				case received = <-session.Incoming():
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				getSnapshot(t, session)
				if got := budget.Snapshot().RetainedBytes; got != 0 {
					t.Fatalf("application ownership retained %d", got)
				}
				session.Stop()
				if string(received.Front) != "retained" {
					t.Fatal("application view changed")
				}
				if got := budget.Snapshot(); got != (ReceiveBudgetSnapshot{}) {
					t.Fatal(got)
				}
				return
			}
			message.Front = make([]byte, 60)
			message.Lengths.Front = 60
			message.Header.Sequence = 2
			transport.inject(messageFrame(t, message))
			select {
			case err := <-session.Terminal():
				if !errors.Is(err, ErrQueueSaturated) {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("saturation did not fail terminal")
			}
			session.Stop()
			if got := budget.Snapshot(); got != (ReceiveBudgetSnapshot{}) {
				t.Fatalf("stop leaked %+v", got)
			}
		})
	}
}

func TestReceiveBudgetAdmissionAndRetiringConnector(t *testing.T) {
	budget, _ := NewReceiveBudget(4, 8192)
	gate := make(chan struct{})
	entered := make(chan struct{}, 4)
	late := make(chan *fakeTransport, 4)
	connector := ConnectorFunc(func(context.Context) (Transport, error) {
		entered <- struct{}{}
		<-gate
		transport := newFakeTransport()
		late <- transport
		return transport, nil
	})
	config := testSessionConfig(t)
	config.ReceiveBudget = budget
	var workers sync.WaitGroup
	admitted := make(chan *Session, 32)
	rejected := make(chan error, 32)
	for range 32 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			session, err := NewSession(nil, connector, config)
			if err != nil {
				rejected <- err
			} else {
				admitted <- session
			}
		}()
	}
	workers.Wait()
	if len(admitted) != 4 || len(rejected) != 28 {
		t.Fatalf("admitted=%d rejected=%d", len(admitted), len(rejected))
	}
	for len(rejected) > 0 {
		if err := <-rejected; !errors.Is(err, ErrReceiveBudgetExceeded) {
			t.Fatal(err)
		}
	}
	var sessions []*Session
	for range 4 {
		sessions = append(sessions, <-admitted)
		<-entered
	}
	var stops sync.WaitGroup
	for _, session := range sessions {
		stops.Add(1)
		go func() { defer stops.Done(); session.Stop() }()
	}
	for _, session := range sessions {
		select {
		case <-session.Done():
		case <-time.After(time.Second):
			t.Fatal("stop did not begin")
		}
	}
	if got := budget.Snapshot().Sessions; got != 4 {
		t.Fatalf("released retiring slots early: %d", got)
	}
	if session, err := NewSession(nil, connector, config); err == nil {
		session.Stop()
		t.Fatal("retiring admission accepted")
	}
	close(gate)
	stops.Wait()
	for range 4 {
		select {
		case <-((<-late).closed):
		default:
			t.Fatal("late transport not closed")
		}
	}
	if got := budget.Snapshot(); got != (ReceiveBudgetSnapshot{}) {
		t.Fatalf("retirement leaked %+v", got)
	}
	transport := newFakeTransport()
	session := newTestSession(t, transport, nil, config)
	session.Stop()
}

func TestReceiveBudgetReplyCarrierAndDuplicateDrop(t *testing.T) {
	budget, _ := NewReceiveBudget(1, 8192)
	fixture := newPerformanceQueue(4)
	owner := fixture.owner
	owner.config.ReceiveBudget = budget
	owner.config.MaxQueuedReceiveBytes = 8192
	owner.session.controlGeneration.Store(1)
	request := &submitCommand{ctx: context.Background(), message: testMessage("request"), result: make(chan submitResult, 1)}
	owner.submit(request)
	reply := testMessage("reply")
	reply.Header.Sequence = 1
	reply.Header.TransactionID = owner.byRequest[request].message.Header.TransactionID
	frame, err := detachReceiveFrame(messageFrame(t, reply), budget, performanceLimits)
	if err != nil {
		t.Fatal(err)
	}
	owner.handleFrame(frame)
	if got := budget.Snapshot().RetainedBytes; got != MessageHeaderSize+5 {
		t.Fatalf("buffered reply unaccounted: %d", got)
	}
	received, err := (<-request.result).take()
	if err != nil || string(received.Front) != "reply" {
		t.Fatalf("reply=%+v err=%v", received, err)
	}
	if got := budget.Snapshot().RetainedBytes; got != 0 {
		t.Fatalf("delivered reply retained %d", got)
	}
	frame, err = detachReceiveFrame(messageFrame(t, reply), budget, performanceLimits)
	if err != nil {
		t.Fatal(err)
	}
	owner.handleFrame(frame)
	if got := budget.Snapshot().RetainedBytes; got != 0 {
		t.Fatalf("duplicate leaked %d", got)
	}
}

func TestReceiveBudgetCustomTransportLimitsAndDetachment(t *testing.T) {
	budget, _ := NewReceiveBudget(1, 8192)
	storage := make([]byte, 1, 4096)
	storage[0] = 'a'
	transport := &orderedReadTransport{frames: []Frame{{Tag: TagAck, Segments: []Segment{{Alignment: DefaultAlignment, Data: storage}}}}}
	frame, err := ReadTransportFrame(transport, budget, sessionTestLimits)
	if err != nil {
		t.Fatal(err)
	}
	storage[0] = 'b'
	if string(frame.Segments[0].Data) != "a" || cap(frame.Segments[0].Data) != 1 {
		t.Fatal("custom backing not detached")
	}
	frame.receiveLease.release()
	transport.frames = []Frame{{Tag: TagAck, Segments: []Segment{{Alignment: DefaultAlignment, Data: make([]byte, 4097)}}}}
	if _, err := ReadTransportFrame(transport, budget, sessionTestLimits); !errors.Is(err, ErrLimitExceeded) {
		t.Fatal(err)
	}
	if snapshot := budget.Snapshot(); snapshot != (ReceiveBudgetSnapshot{}) {
		t.Fatal(snapshot)
	}
}

func TestReceiveBudgetReadAheadShutdown(t *testing.T) {
	budget, _ := NewReceiveBudget(1, 8192)
	owner := &sessionOwner{config: SessionConfig{ReceiveBudget: budget, Limits: sessionTestLimits}, session: &Session{done: make(chan struct{})}, frames: make(chan pumpFrame, 1)}
	transport := newFakeTransport()
	for sequence := uint64(1); sequence <= 3; sequence++ {
		message := testMessage("retained")
		message.Header.Sequence = sequence
		transport.inject(messageFrame(t, message))
	}
	owner.pumpWG.Add(1)
	go owner.readPump(1, transport)
	first := receivePumpFrame(t, owner.frames)
	first.frame.receiveLease.release()
	second := receivePumpFrame(t, owner.frames)
	second.frame.receiveLease.release()
	close(owner.session.done)
	transport.Close()
	owner.pumpWG.Wait()
	select {
	case leftover := <-owner.frames:
		leftover.frame.receiveLease.release()
	default:
	}
	if snapshot := budget.Snapshot(); snapshot != (ReceiveBudgetSnapshot{}) {
		t.Fatalf("read-ahead leaked %+v", snapshot)
	}
}

func TestReceiveBudgetLimits(t *testing.T) {
	if _, err := NewReceiveBudget(0, 1); err == nil {
		t.Fatal("zero sessions accepted")
	}
	if _, err := NewReceiveBudget(1, 0); err == nil {
		t.Fatal("zero bytes accepted")
	}
	budget, _ := NewReceiveBudget(1, 100)
	if err := budget.admit(); err != nil {
		t.Fatal(err)
	}
	if err := budget.admit(); !errors.Is(err, ErrReceiveBudgetExceeded) {
		t.Fatal(err)
	}
	lease := &receiveLease{budget: budget}
	if err := lease.resize(100); err != nil {
		t.Fatal(err)
	}
	if err := lease.resize(101); !errors.Is(err, ErrQueueSaturated) {
		t.Fatal(err)
	}
	lease.release()
	budget.retire()
	if got := budget.Snapshot(); got != (ReceiveBudgetSnapshot{}) {
		t.Fatal(got)
	}
}
