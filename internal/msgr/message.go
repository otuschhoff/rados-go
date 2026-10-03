package msgr

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	wire "github.com/otuschhoff/rados-go/internal/encoding"
)

const MessageHeaderSize = 41

var messageAlignments = [...]uint16{
	DefaultAlignment,
	DefaultAlignment,
	DefaultAlignment,
	PageAlignment,
}

// MessageHeader is the packed ceph_msg_header2 used by messenger v2.
type MessageHeader struct {
	Sequence             uint64
	TransactionID        uint64
	Type                 uint16
	Priority             uint16
	Version              uint16
	DataPrePaddingLength uint32
	DataOffset           uint16
	AckSequence          uint64
	Flags                uint8
	CompatVersion        uint16
	Reserved             uint16
}

type MessageLengths struct {
	Front  uint32
	Middle uint32
	Data   uint32
}

type MessageLease struct {
	references    atomic.Int64
	reclaim       func()
	paddedPayload []byte
	plaintextMu   sync.Mutex
}

const LeasedPayloadPadding = secureBlockSize
const LeasedPayloadPrefix = 1024

func NewMessageLease(reclaim func()) *MessageLease {
	lease := &MessageLease{reclaim: reclaim}
	lease.references.Store(1)
	return lease
}

func NewPaddedMessageLease(storage, payload []byte, reclaim func()) *MessageLease {
	if len(payload) == 0 || len(payload)%LeasedPayloadPadding != 0 || len(storage) != LeasedPayloadPrefix+len(payload)+LeasedPayloadPadding || &payload[0] != &storage[LeasedPayloadPrefix] {
		panic("msgr: invalid padded message payload")
	}
	lease := NewMessageLease(reclaim)
	lease.paddedPayload = storage
	clear(storage[len(storage)-LeasedPayloadPadding:])
	storage[len(storage)-LeasedPayloadPadding] = LateStatusComplete
	return lease
}

func (lease *MessageLease) securePlaintext(segments []Segment) []byte {
	if lease == nil || len(lease.paddedPayload) == 0 || len(segments) < 2 {
		return nil
	}
	payload := segments[len(segments)-1].Data
	if len(payload) == 0 || LeasedPayloadPrefix+len(payload)+LeasedPayloadPadding != len(lease.paddedPayload) || &payload[0] != &lease.paddedPayload[LeasedPayloadPrefix] {
		return nil
	}
	prefixSize := 0
	for _, segment := range segments[1 : len(segments)-1] {
		padded := paddedSecureLength(uint64(len(segment.Data)))
		if padded > uint64(LeasedPayloadPrefix-prefixSize) {
			return nil
		}
		prefixSize += int(padded)
	}
	plaintext := lease.paddedPayload[LeasedPayloadPrefix-prefixSize:]
	clear(plaintext[:prefixSize])
	offset := 0
	for _, segment := range segments[1 : len(segments)-1] {
		copy(plaintext[offset:], segment.Data)
		offset += int(paddedSecureLength(uint64(len(segment.Data))))
	}
	return plaintext
}

func (lease *MessageLease) Retain() {
	for {
		references := lease.references.Load()
		if references <= 0 || references == math.MaxInt64 {
			panic("msgr: invalid message lease retention")
		}
		if lease.references.CompareAndSwap(references, references+1) {
			return
		}
	}
}

func (lease *MessageLease) Release() {
	references := lease.references.Add(-1)
	if references < 0 {
		panic("msgr: message lease released twice")
	}
	if references == 0 {
		reclaim := lease.reclaim
		lease.reclaim = nil
		lease.paddedPayload = nil
		if reclaim != nil {
			reclaim()
		}
	}
}

type Message struct {
	Header              MessageHeader
	Lengths             MessageLengths
	Front               []byte
	Middle              []byte
	Data                []byte
	TransportGeneration uint64
	receiveLease        *receiveLease
	transferred         bool
	payloadLease        *MessageLease
}

// TakeMessageOwnership transfers exclusive ownership of all message buffers to
// the receiving session, including when admission fails. The caller must never
// mutate or reuse those buffers after submission.
func TakeMessageOwnership(message Message) Message {
	message.transferred = true
	return message
}

// RetainImmutableMessage permits the session to retain buffers without cloning.
// All holders must keep them immutable permanently, including after rejection
// or cancellation; immutable buffers may be shared by independent retries.
func RetainImmutableMessage(message Message) Message {
	message.transferred = true
	return message
}

func RetainLeasedMessage(message Message, lease *MessageLease) Message {
	if lease == nil {
		panic("msgr: nil message lease")
	}
	message.transferred = true
	message.payloadLease = lease
	return message
}

func EncodeMessageHeader(header MessageHeader) [MessageHeaderSize]byte {
	encoder := wire.NewEncoder(MessageHeaderSize)
	encoder.Uint64(header.Sequence)
	encoder.Uint64(header.TransactionID)
	encoder.Uint16(header.Type)
	encoder.Uint16(header.Priority)
	encoder.Uint16(header.Version)
	encoder.Uint32(header.DataPrePaddingLength)
	encoder.Uint16(header.DataOffset)
	encoder.Uint64(header.AckSequence)
	encoder.Uint8(header.Flags)
	encoder.Uint16(header.CompatVersion)
	encoder.Uint16(header.Reserved)
	data, err := encoder.BytesResult()
	if err != nil || len(data) != MessageHeaderSize {
		panic("msgr: invalid fixed message header encoding")
	}
	var result [MessageHeaderSize]byte
	copy(result[:], data)
	return result
}

func DecodeMessageHeader(data []byte) (MessageHeader, error) {
	if len(data) != MessageHeaderSize {
		return MessageHeader{}, fmt.Errorf("%w: message header length %d", wire.ErrMalformed, len(data))
	}
	decoder := wire.NewDecoder(data, wire.Limits{})
	header := MessageHeader{
		Sequence:             decoder.Uint64(),
		TransactionID:        decoder.Uint64(),
		Type:                 decoder.Uint16(),
		Priority:             decoder.Uint16(),
		Version:              decoder.Uint16(),
		DataPrePaddingLength: decoder.Uint32(),
		DataOffset:           decoder.Uint16(),
		AckSequence:          decoder.Uint64(),
		Flags:                decoder.Uint8(),
		CompatVersion:        decoder.Uint16(),
		Reserved:             decoder.Uint16(),
	}
	if err := decoder.Finish(); err != nil {
		return MessageHeader{}, err
	}
	if decoder.Remaining() != 0 {
		return MessageHeader{}, fmt.Errorf("%w: trailing message header bytes", wire.ErrMalformed)
	}
	return header, nil
}

func EncodeMessage(message Message, limits Limits) (Frame, error) {
	return encodeMessage(message, limits, true)
}

func encodeOwnedMessage(message Message, limits Limits) (Frame, error) {
	return encodeMessage(message, limits, false)
}

func encodeMessage(message Message, limits Limits, copyPayloads bool) (Frame, error) {
	frontLength, err := checkedUint32Length(len(message.Front))
	if err != nil {
		return Frame{}, err
	}
	middleLength, err := checkedUint32Length(len(message.Middle))
	if err != nil {
		return Frame{}, err
	}
	dataLength, err := checkedUint32Length(len(message.Data))
	if err != nil {
		return Frame{}, err
	}
	actual := MessageLengths{Front: frontLength, Middle: middleLength, Data: dataLength}
	if message.Lengths != actual {
		return Frame{}, fmt.Errorf("%w: message lengths %+v do not match payloads %+v", wire.ErrMalformed, message.Lengths, actual)
	}
	if message.Header.DataPrePaddingLength > dataLength {
		return Frame{}, fmt.Errorf("%w: data pre-padding %d exceeds data length %d", wire.ErrMalformed, message.Header.DataPrePaddingLength, dataLength)
	}

	header := EncodeMessageHeader(message.Header)
	segments := []Segment{
		{Alignment: messageAlignments[0], Data: header[:]},
		{Alignment: messageAlignments[1], Data: message.Front},
		{Alignment: messageAlignments[2], Data: message.Middle},
		{Alignment: messageAlignments[3], Data: message.Data},
	}
	if err := validateMessageSegments(segments, limits); err != nil {
		return Frame{}, err
	}
	for index := range segments {
		if copyPayloads {
			segments[index].Data = append([]byte(nil), segments[index].Data...)
		} else {
			segments[index].Data = segments[index].Data[:len(segments[index].Data):len(segments[index].Data)]
		}
	}
	return Frame{Tag: TagMessage, Segments: segments}, nil
}

func DecodeMessage(frame Frame, limits Limits) (Message, error) {
	return decodeMessage(frame, limits, true)
}

func decodeOwnedMessage(frame Frame, limits Limits) (Message, error) {
	return decodeMessage(frame, limits, false)
}

func decodeMessage(frame Frame, limits Limits, copyPayloads bool) (Message, error) {
	if frame.Tag != TagMessage {
		return Message{}, fmt.Errorf("%w: message tag %d", wire.ErrMalformed, frame.Tag)
	}
	if len(frame.Segments) < 1 || len(frame.Segments) > len(messageAlignments) {
		return Message{}, fmt.Errorf("%w: message segment count %d", wire.ErrMalformed, len(frame.Segments))
	}
	if err := validateMessageSegments(frame.Segments, limits); err != nil {
		return Message{}, err
	}
	header, err := DecodeMessageHeader(frame.Segments[0].Data)
	if err != nil {
		return Message{}, err
	}
	dataLength := uint32(0)
	if len(frame.Segments) > 3 {
		dataLength = uint32(len(frame.Segments[3].Data))
	}
	if header.DataPrePaddingLength > dataLength {
		return Message{}, fmt.Errorf("%w: data pre-padding exceeds data segment", wire.ErrMalformed)
	}

	var payloads [3][]byte
	for index := 1; index < len(frame.Segments); index++ {
		data := frame.Segments[index].Data
		if copyPayloads {
			payloads[index-1] = append([]byte(nil), data...)
		} else if len(data) != 0 {
			payloads[index-1] = data[:len(data):len(data)]
		}
	}
	return Message{
		Header: header,
		Lengths: MessageLengths{
			Front:  uint32(len(payloads[0])),
			Middle: uint32(len(payloads[1])),
			Data:   uint32(len(payloads[2])),
		},
		Front:  payloads[0],
		Middle: payloads[1],
		Data:   payloads[2],
	}, nil
}

func validateMessageSegments(segments []Segment, limits Limits) error {
	total := uint64(0)
	for index, segment := range segments {
		if segment.Alignment != messageAlignments[index] {
			return fmt.Errorf("%w: message segment %d alignment %d, want %d", wire.ErrMalformed, index, segment.Alignment, messageAlignments[index])
		}
		length := uint64(len(segment.Data))
		if length > uint64(limits.MaxSegmentBytes) || total > math.MaxUint64-length {
			return wire.ErrLimitExceeded
		}
		total += length
	}
	if total > limits.MaxFrameBytes {
		return wire.ErrLimitExceeded
	}
	return nil
}
