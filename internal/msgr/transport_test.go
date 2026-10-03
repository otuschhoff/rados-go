package msgr

import (
	"bufio"
	"bytes"
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

func TestConnTransportFrameOwnership(t *testing.T) {
	secure, err := NewSecureCodec(testSecureSecret(), false)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name       string
		codec      Codec
		owned      bool
		writeOwned bool
	}{
		{name: "crc", codec: CRCCodec{}, owned: true},
		{name: "secure", codec: secure, owned: true, writeOwned: true},
		{name: "custom", codec: &blockingCodec{}, owned: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			transport := &connTransport{codec: test.codec}
			if transport.OwnsReadFrames() != test.owned {
				t.Fatalf("ownership=%v want=%v", transport.OwnsReadFrames(), test.owned)
			}
			if transport.OwnsWriteFrames() != test.writeOwned {
				t.Fatalf("write ownership=%v want=%v", transport.OwnsWriteFrames(), test.writeOwned)
			}
		})
	}
}

type countedReadConn struct {
	*recordingConn
	reader *bytes.Reader
	reads  int
}

func (connection *countedReadConn) Read(data []byte) (int, error) {
	connection.reads++
	return connection.reader.Read(data)
}

func TestConnTransportReadAheadPreservesFrames(t *testing.T) {
	codec := CRCCodec{WithDataCRC: true}
	frame := Frame{Tag: TagAck, Segments: []Segment{{Alignment: DefaultAlignment, Data: []byte("hello")}}}
	wire, err := codec.Encode(frame, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	connection := &countedReadConn{recordingConn: &recordingConn{}, reader: bytes.NewReader(bytes.Repeat(wire, 3))}
	transport, err := NewConnTransport(connection, codec, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 3; index++ {
		decoded, err := transport.ReadFrame()
		if err != nil {
			t.Fatal(err)
		}
		assertFrameEqual(t, decoded, frame)
		decoded.Segments[0].Data[0] = 'X'
	}
	if connection.reads != 1 {
		t.Fatalf("socket reads=%d want=1", connection.reads)
	}
}

func TestConnTransportSecureReadAhead(t *testing.T) {
	sender, err := NewSecureCodec(testSecureSecret(), false)
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := NewSecureCodec(testSecureSecret(), true)
	if err != nil {
		t.Fatal(err)
	}
	frame := Frame{Tag: TagAck, Segments: []Segment{{Alignment: DefaultAlignment, Data: []byte("hello")}}}
	var stream []byte
	for index := 0; index < 3; index++ {
		wire, err := sender.Encode(frame, testLimits)
		if err != nil {
			t.Fatal(err)
		}
		stream = append(stream, wire...)
	}
	connection := &countedReadConn{recordingConn: &recordingConn{}, reader: bytes.NewReader(stream)}
	transport, err := NewConnTransport(connection, receiver, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 3; index++ {
		decoded, err := transport.ReadFrame()
		if err != nil {
			t.Fatal(err)
		}
		assertFrameEqual(t, decoded, frame)
		decoded.Segments[0].Data[0] = 'X'
	}
	if _, err := transport.ReadFrame(); err == nil || connection.reads != 2 {
		t.Fatalf("EOF err=%v socket reads=%d want=2", err, connection.reads)
	}
}

func TestConnTransportReadAheadBoundsAndCustomReader(t *testing.T) {
	for _, maxFrame := range []uint64{64 << 10, 16 << 20} {
		limits := testLimits
		limits.MaxFrameBytes = maxFrame
		transport, err := NewConnTransport(&recordingConn{}, CRCCodec{}, limits)
		if err != nil {
			t.Fatal(err)
		}
		reader := transport.(*connTransport).reader.(*bufio.Reader)
		if reader.Size() > 512<<10 || uint64(reader.Size()) > maxFrame {
			t.Fatalf("read-ahead=%d exceeds frame limit=%d", reader.Size(), maxFrame)
		}
	}
	connection := &recordingConn{}
	transport, err := NewConnTransport(connection, &blockingCodec{}, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	if transport.(*connTransport).reader != connection {
		t.Fatal("custom codec reader identity changed")
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

func TestConnTransportSecureWriteCache(t *testing.T) {
	limits := testLimits
	limits.MaxFrameBytes = 16 << 20
	limits.MaxSegmentBytes = 16 << 20
	handle, err := NewConnTransport(&recordingConn{}, mustSecureCodec(t, testSecureSecret(), false), limits)
	if err != nil {
		t.Fatal(err)
	}
	transport := handle.(*connTransport)
	frame := Frame{Tag: TagAck, Segments: []Segment{{Alignment: DefaultAlignment, Data: []byte("ceph")}}}
	if err := transport.WriteFrame(frame); err != nil {
		t.Fatal(err)
	}
	buffer := transport.writeWire
	if len(buffer) == 0 || cap(buffer) > secureWriteCacheLimit {
		t.Fatal("missing or oversized secure cache")
	}
	if err := transport.WriteFrame(frame); err != nil || &buffer[0] != &transport.writeWire[0] {
		t.Fatalf("secure buffer was not reused: %v", err)
	}
	large := Frame{Tag: TagMessage, Segments: []Segment{{Alignment: DefaultAlignment, Data: make([]byte, secureWriteCacheLimit+1)}}}
	if err := transport.WriteFrame(large); err != nil || &buffer[0] != &transport.writeWire[0] || cap(transport.writeWire) > secureWriteCacheLimit {
		t.Fatalf("oversized frame replaced bounded cache: %v", err)
	}
	if err := transport.Close(); err != nil || transport.writeWire != nil {
		t.Fatalf("close retained secure cache: %v", err)
	}
	left, right := net.Pipe()
	_ = right.Close()
	handle, err = NewConnTransport(left, mustSecureCodec(t, testSecureSecret(), false), testLimits)
	if err != nil {
		t.Fatal(err)
	}
	transport = handle.(*connTransport)
	defer transport.Close()
	if err := transport.WriteFrame(frame); err == nil || transport.writeWire != nil {
		t.Fatalf("failed write retained cache: %v", err)
	}
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
