package msgr

import (
	"fmt"
	"math"

	wire "github.com/otuschhoff/go-librados/internal/encoding"
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

type Message struct {
	Header  MessageHeader
	Lengths MessageLengths
	Front   []byte
	Middle  []byte
	Data    []byte
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
		segments[index].Data = append([]byte(nil), segments[index].Data...)
	}
	return Frame{Tag: TagMessage, Segments: segments}, nil
}

func DecodeMessage(frame Frame, limits Limits) (Message, error) {
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
		payloads[index-1] = append([]byte(nil), frame.Segments[index].Data...)
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
