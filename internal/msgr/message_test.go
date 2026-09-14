package msgr

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	wire "github.com/otuschhoff/go-librados/internal/encoding"
)

func TestMessageHeaderExactBytes(t *testing.T) {
	header := MessageHeader{
		Sequence:             0x0807060504030201,
		TransactionID:        0x100f0e0d0c0b0a09,
		Type:                 0x1211,
		Priority:             0x1413,
		Version:              0x1615,
		DataPrePaddingLength: 0x1a191817,
		DataOffset:           0x1c1b,
		AckSequence:          0x24232221201f1e1d,
		Flags:                0x25,
		CompatVersion:        0x2726,
		Reserved:             0x2928,
	}
	want := []byte{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
		0x11, 0x12, 0x13, 0x14, 0x15, 0x16,
		0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c,
		0x1d, 0x1e, 0x1f, 0x20, 0x21, 0x22, 0x23, 0x24,
		0x25, 0x26, 0x27, 0x28, 0x29,
	}
	encoded := EncodeMessageHeader(header)
	if !bytes.Equal(encoded[:], want) {
		t.Fatalf("header = %x, want %x", encoded, want)
	}
	decoded, err := DecodeMessageHeader(encoded[:])
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, header) {
		t.Fatalf("decoded = %#v, want %#v", decoded, header)
	}
	if _, err := DecodeMessageHeader(append(encoded[:], 0)); !errors.Is(err, wire.ErrMalformed) {
		t.Fatalf("trailing header error = %v", err)
	}
}

func TestMessageRoundTrip(t *testing.T) {
	want := Message{
		Header:  MessageHeader{Sequence: 1, TransactionID: 2, Type: 3, DataPrePaddingLength: 1, DataOffset: 4, AckSequence: 5},
		Lengths: MessageLengths{Front: 5, Middle: 6, Data: 4},
		Front:   []byte("front"),
		Middle:  []byte("middle"),
		Data:    []byte{0, 1, 2, 3},
	}
	frame, err := EncodeMessage(want, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	if len(frame.Segments) != 4 {
		t.Fatalf("segment count = %d", len(frame.Segments))
	}
	for index, alignment := range messageAlignments {
		if frame.Segments[index].Alignment != alignment {
			t.Fatalf("segment %d alignment = %d", index, frame.Segments[index].Alignment)
		}
	}
	got, err := DecodeMessage(frame, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded = %#v, want %#v", got, want)
	}
}

func TestMessageAcceptsOmittedTrailingEmptySegments(t *testing.T) {
	header := EncodeMessageHeader(MessageHeader{})
	got, err := DecodeMessage(Frame{Tag: TagMessage, Segments: []Segment{{Alignment: 8, Data: header[:]}}}, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	if got.Lengths != (MessageLengths{}) || got.Front != nil || got.Middle != nil || got.Data != nil {
		t.Fatalf("decoded = %#v", got)
	}
}

func TestMessageRejectsPrePaddingWithoutDataSegment(t *testing.T) {
	header := EncodeMessageHeader(MessageHeader{DataPrePaddingLength: 1})
	for segmentCount := 1; segmentCount <= 3; segmentCount++ {
		segments := []Segment{{Alignment: 8, Data: header[:]}}
		for len(segments) < segmentCount {
			segments = append(segments, Segment{Alignment: 8})
		}
		if _, err := DecodeMessage(Frame{Tag: TagMessage, Segments: segments}, testLimits); !errors.Is(err, wire.ErrMalformed) {
			t.Fatalf("segment count %d error = %v", segmentCount, err)
		}
	}
}

func TestMessageRejectsMalformedLengthsAndAlignments(t *testing.T) {
	message := Message{Lengths: MessageLengths{Front: 2}, Front: []byte{1}}
	if _, err := EncodeMessage(message, testLimits); !errors.Is(err, wire.ErrMalformed) {
		t.Fatalf("length error = %v", err)
	}

	header := EncodeMessageHeader(MessageHeader{})
	tests := []Frame{
		{Tag: TagMessage, Segments: []Segment{{Alignment: 8, Data: header[:]}, {Alignment: 16}}},
		{Tag: TagMessage, Segments: []Segment{{Alignment: 8, Data: header[:]}, {Alignment: 8}, {Alignment: 8}, {Alignment: 8}}},
		{Tag: TagMessage, Segments: []Segment{{Alignment: 8, Data: header[:MessageHeaderSize-1]}}},
	}
	for index, frame := range tests {
		if _, err := DecodeMessage(frame, testLimits); !errors.Is(err, wire.ErrMalformed) {
			t.Fatalf("case %d error = %v", index, err)
		}
	}

	prePadded := EncodeMessageHeader(MessageHeader{DataPrePaddingLength: 2})
	if _, err := DecodeMessage(Frame{Tag: TagMessage, Segments: []Segment{
		{Alignment: 8, Data: prePadded[:]},
		{Alignment: 8},
		{Alignment: 8},
		{Alignment: 4096, Data: []byte{0}},
	}}, testLimits); !errors.Is(err, wire.ErrMalformed) {
		t.Fatalf("pre-padding error = %v", err)
	}
}
