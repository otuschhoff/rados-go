package msgr

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
)

var ErrNilCodec = errors.New("messenger codec is nil")

// Codec encodes and decodes framed messenger traffic.
type Codec interface {
	Encode(Frame, Limits) ([]byte, error)
	Read(io.Reader, Limits) (Frame, error)
}

type connTransport struct {
	conn      net.Conn
	reader    io.Reader
	codec     Codec
	limits    Limits
	readMu    sync.Mutex
	closed    bool
	writeMu   sync.Mutex
	closeOnce sync.Once
	scratch   receiveScratch
}

// NewConnTransport binds a network connection to one messenger codec.
func NewConnTransport(conn net.Conn, codec Codec, limits Limits) (Transport, error) {
	if conn == nil {
		return nil, fmt.Errorf("%w: nil connection", ErrMalformed)
	}
	if codec == nil {
		return nil, ErrNilCodec
	}
	reader := io.Reader(conn)
	switch codec.(type) {
	case CRCCodec, *SecureCodec:
		bufferSize := uint64(512 << 10)
		if limits.MaxFrameBytes < bufferSize {
			bufferSize = limits.MaxFrameBytes
		}
		reader = bufio.NewReaderSize(conn, int(bufferSize))
	}
	return &connTransport{conn: conn, reader: reader, codec: codec, limits: limits}, nil
}

func (transport *connTransport) ReadFrame() (Frame, error) {
	transport.readMu.Lock()
	defer transport.readMu.Unlock()
	if transport.closed {
		return Frame{}, ErrSessionClosed
	}
	return transport.codec.Read(transport.reader, transport.limits)
}

func (transport *connTransport) ReadFrameWithBudget(budget *ReceiveBudget, limits Limits) (Frame, error) {
	transport.readMu.Lock()
	defer transport.readMu.Unlock()
	if transport.closed {
		return Frame{}, ErrSessionClosed
	}
	limits.MaxFrameBytes = min(limits.MaxFrameBytes, transport.limits.MaxFrameBytes)
	limits.MaxSegmentBytes = min(limits.MaxSegmentBytes, transport.limits.MaxSegmentBytes)
	if budget == nil {
		return transport.codec.Read(transport.reader, limits)
	}
	if !transport.OwnsReadFrames() {
		frame, err := transport.codec.Read(transport.reader, limits)
		if err != nil {
			return Frame{}, err
		}
		return detachReceiveFrame(frame, budget, limits)
	}
	var reservation *budgetReceiveReservation
	if _, secure := transport.codec.(*SecureCodec); secure {
		packet := &budgetSecureReservation{}
		reservation = &packet.budgetReceiveReservation
		reservation.reader.prelude = packet.prelude[:]
	} else {
		reservation = &budgetReceiveReservation{}
	}
	lease := &reservation.lease
	lease.budget = budget
	reservation.reader.Reader = transport.reader
	reservation.reader.lease = lease
	if _, secure := transport.codec.(*SecureCodec); secure {
		reservation.reader.scratch = &transport.scratch
	}
	frame, err := transport.codec.Read(&reservation.reader, limits)
	reservation.reader.Reader = nil
	if err != nil {
		lease.reclaim()
		return Frame{}, err
	}
	frame.receiveLease = lease
	return frame, nil
}

func (transport *connTransport) OwnsReadFrames() bool {
	switch transport.codec.(type) {
	case CRCCodec, *SecureCodec:
		return true
	default:
		return false
	}
}

func (transport *connTransport) OwnsWriteFrames() bool {
	_, secure := transport.codec.(*SecureCodec)
	return secure
}

func (transport *connTransport) WriteFrame(frame Frame) error {
	transport.writeMu.Lock()
	defer transport.writeMu.Unlock()
	wire, err := transport.codec.Encode(frame, transport.limits)
	if err != nil {
		return err
	}
	for len(wire) > 0 {
		written, writeErr := transport.conn.Write(wire)
		if written > 0 {
			wire = wire[written:]
		}
		if writeErr != nil {
			return writeErr
		}
		if written == 0 {
			return io.ErrUnexpectedEOF
		}
	}
	return nil
}

func (transport *connTransport) Close() error {
	var err error
	transport.closeOnce.Do(func() {
		err = transport.conn.Close()
		transport.readMu.Lock()
		defer transport.readMu.Unlock()
		transport.closed = true
		transport.reader = nil
		transport.scratch.close()
	})
	return err
}
