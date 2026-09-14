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

func TestConnTransportCRCRoundTrip(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	client, err := NewConnTransport(left, CRCCodec{WithDataCRC: true}, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewConnTransport(right, CRCCodec{WithDataCRC: true}, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	defer server.Close()

	frame := Frame{Tag: TagMessage, Segments: []Segment{{Alignment: DefaultAlignment, Data: []byte("hello")}}}
	readResult := make(chan Frame, 1)
	readErr := make(chan error, 1)
	go func() {
		got, err := server.ReadFrame()
		if err != nil {
			readErr <- err
			return
		}
		readResult <- got
	}()

	if err := client.WriteFrame(frame); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-readErr:
		t.Fatal(err)
	case got := <-readResult:
		assertFrameEqual(t, got, frame)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for read")
	}
}

func TestConnTransportSecureRoundTrip(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	secret := testSecureSecret()
	clientCodec, err := NewSecureCodec(secret, false)
	if err != nil {
		t.Fatal(err)
	}
	serverCodec, err := NewSecureCodec(secret, true)
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewConnTransport(left, clientCodec, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewConnTransport(right, serverCodec, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	defer server.Close()

	frame := Frame{Tag: TagKeepalive2Ack, Segments: []Segment{{Alignment: DefaultAlignment, Data: []byte("ok")}}}
	go func() {
		_ = client.WriteFrame(frame)
	}()
	got, err := server.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	assertFrameEqual(t, got, frame)
}

func TestConnTransportSerializesStatefulEncodingAndWrites(t *testing.T) {
	connection := &recordingConn{}
	codec := &blockingCodec{firstEntered: make(chan struct{}), releaseFirst: make(chan struct{})}
	transport, err := NewConnTransport(connection, codec, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	firstDone := make(chan error, 1)
	secondDone := make(chan error, 1)
	go func() {
		firstDone <- transport.WriteFrame(Frame{Tag: 1})
	}()
	<-codec.firstEntered
	go func() {
		secondDone <- transport.WriteFrame(Frame{Tag: 2})
	}()
	close(codec.releaseFirst)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	if got := connection.bytes(); string(got) != "\x01\x02" {
		t.Fatalf("wire order = %x", got)
	}
}

func TestConnTransportCloseInterruptsBlockingRead(t *testing.T) {
	left, right := net.Pipe()
	defer right.Close()

	transport, err := NewConnTransport(left, CRCCodec{WithDataCRC: true}, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()

	readDone := make(chan error, 1)
	go func() {
		_, err := transport.ReadFrame()
		readDone <- err
	}()

	select {
	case <-time.After(50 * time.Millisecond):
	case err := <-readDone:
		t.Fatalf("read returned too early: %v", err)
	}
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-readDone:
		if err == nil {
			t.Fatal("read returned nil error after close")
		}
	case <-time.After(time.Second):
		t.Fatal("close did not interrupt blocking read")
	}
}

func TestConnTransportWriteHandlesShortWrites(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	codec := CRCCodec{WithDataCRC: true}
	wire, err := codec.Encode(Frame{Tag: TagAck, Segments: []Segment{{Alignment: DefaultAlignment, Data: []byte("z")}}}, testLimits)
	if err != nil {
		t.Fatal(err)
	}

	transport, err := NewConnTransport(&shortWriteConn{Conn: left, max: 7}, codec, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()

	serverRead := make(chan []byte, 1)
	go func() {
		data := make([]byte, len(wire))
		_, err := io.ReadFull(right, data)
		if err != nil {
			serverRead <- nil
			return
		}
		serverRead <- data
	}()

	frame := Frame{Tag: TagAck, Segments: []Segment{{Alignment: DefaultAlignment, Data: []byte("z")}}}
	if err := transport.WriteFrame(frame); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-serverRead:
		if got == nil || len(got) != len(wire) {
			t.Fatalf("short write test did not receive full frame, got %d bytes", len(got))
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for server read")
	}
}

func TestNewConnTransportRejectsNilCodecOrConn(t *testing.T) {
	if _, err := NewConnTransport(nil, CRCCodec{WithDataCRC: true}, testLimits); !errors.Is(err, ErrMalformed) {
		t.Fatalf("nil conn error = %v", err)
	}
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	if _, err := NewConnTransport(left, nil, testLimits); !errors.Is(err, ErrNilCodec) {
		t.Fatalf("nil codec error = %v", err)
	}
}

type shortWriteConn struct {
	net.Conn
	max int
}

type blockingCodec struct {
	mu           sync.Mutex
	calls        int
	firstEntered chan struct{}
	releaseFirst chan struct{}
}

func (codec *blockingCodec) Encode(frame Frame, _ Limits) ([]byte, error) {
	codec.mu.Lock()
	codec.calls++
	call := codec.calls
	codec.mu.Unlock()
	if call == 1 {
		close(codec.firstEntered)
		<-codec.releaseFirst
	}
	return []byte{byte(frame.Tag)}, nil
}

func (*blockingCodec) Read(io.Reader, Limits) (Frame, error) { return Frame{}, io.EOF }

type recordingConn struct {
	sync.Mutex
	wire []byte
}

func (conn *recordingConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (conn *recordingConn) Close() error                     { return nil }
func (conn *recordingConn) LocalAddr() net.Addr              { return nil }
func (conn *recordingConn) RemoteAddr() net.Addr             { return nil }
func (conn *recordingConn) SetDeadline(time.Time) error      { return nil }
func (conn *recordingConn) SetReadDeadline(time.Time) error  { return nil }
func (conn *recordingConn) SetWriteDeadline(time.Time) error { return nil }
func (conn *recordingConn) Write(data []byte) (int, error) {
	conn.Lock()
	defer conn.Unlock()
	conn.wire = append(conn.wire, data...)
	return len(data), nil
}
func (conn *recordingConn) bytes() []byte {
	conn.Lock()
	defer conn.Unlock()
	return append([]byte(nil), conn.wire...)
}

func (conn *shortWriteConn) Write(data []byte) (int, error) {
	if len(data) > conn.max {
		data = data[:conn.max]
	}
	return conn.Conn.Write(data)
}

func TestConnTransportCloseIsIdempotent(t *testing.T) {
	left, right := net.Pipe()
	defer right.Close()
	transport, err := NewConnTransport(left, CRCCodec{WithDataCRC: true}, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestConnTransportReadUnblocksOnPeerClose(t *testing.T) {
	left, right := net.Pipe()
	transport, err := NewConnTransport(left, CRCCodec{WithDataCRC: true}, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	readDone := make(chan error, 1)
	go func() {
		_, err := transport.ReadFrame()
		readDone <- err
	}()
	_ = right.Close()
	select {
	case err := <-readDone:
		if err == nil {
			t.Fatal("expected read error")
		}
	case <-ctx.Done():
		t.Fatal("read did not unblock on peer close")
	}
}
