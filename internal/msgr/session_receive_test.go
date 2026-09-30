package msgr

import (
	"errors"
	"io"
	"testing"
	"time"
)

type orderedReadTransport struct {
	frames []Frame
}

func (transport *orderedReadTransport) ReadFrame() (Frame, error) {
	if len(transport.frames) == 0 {
		return Frame{}, io.EOF
	}
	frame := transport.frames[0]
	transport.frames = transport.frames[1:]
	return frame, nil
}

func (*orderedReadTransport) WriteFrame(Frame) error { return nil }
func (*orderedReadTransport) Close() error           { return nil }

func TestReadPumpDeliversFramesBeforeReadFault(t *testing.T) {
	owner := &sessionOwner{session: &Session{done: make(chan struct{})}, frames: make(chan pumpFrame, 1)}
	defer func() { close(owner.session.done); owner.pumpWG.Wait() }()
	transport := &orderedReadTransport{frames: []Frame{{Tag: TagAck}, {Tag: TagKeepalive2}}}
	owner.pumpWG.Add(1)
	go owner.readPump(7, transport)
	for _, tag := range []Tag{TagAck, TagKeepalive2} {
		received := receivePumpFrame(t, owner.frames)
		if received.generation != 7 || received.frame.Tag != tag || received.err != nil {
			t.Fatalf("received=%+v want tag=%d", received, tag)
		}
	}
	received := receivePumpFrame(t, owner.frames)
	if received.generation != 7 || !errors.Is(received.err, io.EOF) {
		t.Fatalf("fault=%+v", received)
	}
	owner.pumpWG.Wait()
}

func receivePumpFrame(t *testing.T, frames <-chan pumpFrame) pumpFrame {
	t.Helper()
	select {
	case frame := <-frames:
		return frame
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for receive event")
		return pumpFrame{}
	}
}

type reusingReadTransport struct {
	orderedReadTransport
	storage []byte
	reads   int
}

func (transport *reusingReadTransport) ReadFrame() (Frame, error) {
	if transport.reads == 2 {
		return Frame{}, io.EOF
	}
	transport.storage[0] = byte('a' + transport.reads)
	transport.reads++
	return Frame{Tag: TagAck, Segments: []Segment{{Alignment: DefaultAlignment, Data: transport.storage}}}, nil
}

func TestReadPumpCopiesReusableTransportStorage(t *testing.T) {
	owner := &sessionOwner{session: &Session{done: make(chan struct{})}, frames: make(chan pumpFrame, 1)}
	defer func() { close(owner.session.done); owner.pumpWG.Wait() }()
	transport := &reusingReadTransport{storage: make([]byte, 1)}
	owner.pumpWG.Add(1)
	go owner.readPump(1, transport)
	first := receivePumpFrame(t, owner.frames)
	second := receivePumpFrame(t, owner.frames)
	if string(first.frame.Segments[0].Data) != "a" || string(second.frame.Segments[0].Data) != "b" {
		t.Fatalf("reused receive storage: first=%q second=%q", first.frame.Segments[0].Data, second.frame.Segments[0].Data)
	}
	if received := receivePumpFrame(t, owner.frames); !errors.Is(received.err, io.EOF) {
		t.Fatalf("fault=%+v", received)
	}
}

func TestReadPumpShutdownWithFullReceiveQueue(t *testing.T) {
	owner := &sessionOwner{session: &Session{done: make(chan struct{})}, frames: make(chan pumpFrame, 1)}
	owner.frames <- pumpFrame{}
	owner.pumpWG.Add(1)
	go owner.readPump(1, &orderedReadTransport{frames: []Frame{{Tag: TagAck}}})
	close(owner.session.done)
	finished := make(chan struct{})
	go func() { owner.pumpWG.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("read pump blocked during shutdown with a full queue")
	}
}
