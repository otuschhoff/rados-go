package msgr

import (
	"bytes"
	"encoding/hex"
	"errors"
	"testing"
)

var testLimits = Limits{MaxSegmentBytes: 4096, MaxFrameBytes: 8192}

func TestCRCFrameRoundTrip(t *testing.T) {
	frame := Frame{Tag: TagMessage, Segments: []Segment{
		{Alignment: DefaultAlignment, Data: []byte("header")},
		{Alignment: DefaultAlignment},
		{Alignment: DefaultAlignment, Data: []byte("middle")},
	}}
	data, err := EncodeCRC(frame, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := ReadCRC(&oneByteReader{data: data}, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Tag != frame.Tag || len(decoded.Segments) != 3 {
		t.Fatalf("decoded = %#v", decoded)
	}
	for index := range decoded.Segments {
		if decoded.Segments[index].Alignment != frame.Segments[index].Alignment || !bytes.Equal(decoded.Segments[index].Data, frame.Segments[index].Data) {
			t.Fatalf("segment %d = %#v", index, decoded.Segments[index])
		}
	}
}

func TestCRCCodecDeterministicVectors(t *testing.T) {
	oneSegment := Frame{Tag: TagAck, Segments: []Segment{
		{Alignment: DefaultAlignment, Data: []byte("ceph")},
	}}
	multiSegment := Frame{Tag: TagMessage, Segments: []Segment{
		{Alignment: DefaultAlignment, Data: []byte("header")},
		{Alignment: DefaultAlignment},
		{Alignment: DefaultAlignment, Data: []byte("middle")},
		{Alignment: PageAlignment, Data: []byte{0, 1, 2, 3}},
	}}
	tests := []struct {
		name  string
		codec CRCCodec
		frame Frame
		wire  string
	}{
		{
			name:  "enabled one segment",
			codec: CRCCodec{WithDataCRC: true},
			frame: oneSegment,
			wire:  "14010400000008000000000000000000000000000000000000000000aff5577263657068296fa6a8",
		},
		{
			name:  "disabled one segment",
			codec: CRCCodec{WithDataCRC: false},
			frame: oneSegment,
			wire:  "14010400000008000000000000000000000000000000000000000000aff557726365706800000000",
		},
		{
			name:  "enabled multiple segments",
			codec: CRCCodec{WithDataCRC: true},
			frame: multiSegment,
			wire:  "11040600000008000000000008000600000008000400000000100000cdabc64868656164657237628ab46d6964646c65000102030effffffff78e828209baeab6e",
		},
		{
			name:  "disabled multiple segments",
			codec: CRCCodec{WithDataCRC: false},
			frame: multiSegment,
			wire:  "11040600000008000000000008000600000008000400000000100000cdabc648686561646572000000006d6964646c65000102030e000000000000000000000000",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			want, err := hex.DecodeString(test.wire)
			if err != nil {
				t.Fatal(err)
			}
			got, err := test.codec.Encode(test.frame, testLimits)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("encoded frame = %x, want %x", got, want)
			}
			decoded, err := test.codec.Read(&oneByteReader{data: append([]byte(nil), want...)}, testLimits)
			if err != nil {
				t.Fatal(err)
			}
			assertFrameEqual(t, decoded, test.frame)
		})
	}
}

func TestCRCCodecDataCRCCompatibility(t *testing.T) {
	frame := Frame{Tag: TagMessage, Segments: []Segment{
		{Alignment: DefaultAlignment, Data: []byte("header")},
		{Alignment: DefaultAlignment},
		{Alignment: DefaultAlignment, Data: []byte("middle")},
	}}
	enabled := CRCCodec{WithDataCRC: true}
	disabled := CRCCodec{WithDataCRC: false}

	for _, offset := range []int{PreambleSize, PreambleSize + len("header") + 4} {
		wire, err := enabled.Encode(frame, testLimits)
		if err != nil {
			t.Fatal(err)
		}
		wire[offset] ^= 1
		if _, err := enabled.Read(bytes.NewReader(wire), testLimits); !errors.Is(err, ErrIntegrity) {
			t.Fatalf("enabled payload corruption at %d: error = %v", offset, err)
		}

		wire, err = disabled.Encode(frame, testLimits)
		if err != nil {
			t.Fatal(err)
		}
		wire[offset] ^= 1
		if _, err := disabled.Read(bytes.NewReader(wire), testLimits); err != nil {
			t.Fatalf("disabled payload corruption at %d: %v", offset, err)
		}
	}

	wire, err := disabled.Encode(frame, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	wire[PreambleSize+len("header")] = 1
	if _, err := disabled.Read(bytes.NewReader(wire), testLimits); err != nil {
		t.Fatalf("disabled data CRC field: %v", err)
	}

	wire, err = disabled.Encode(frame, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	wire[2] ^= 1
	if _, err := disabled.Read(bytes.NewReader(wire), testLimits); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("disabled preamble corruption: error = %v", err)
	}
}

func TestCRCCodecLateStatusReservedHighNibble(t *testing.T) {
	frame := Frame{Tag: TagMessage, Segments: []Segment{
		{Alignment: DefaultAlignment, Data: []byte("header")},
		{Alignment: DefaultAlignment, Data: []byte("middle")},
	}}
	for _, codec := range []CRCCodec{{WithDataCRC: true}, {WithDataCRC: false}} {
		wire, err := codec.Encode(frame, testLimits)
		if err != nil {
			t.Fatal(err)
		}
		statusOffset := len(wire) - 13
		if wire[statusOffset] != LateStatusComplete {
			t.Fatalf("encoded late status = %#x", wire[statusOffset])
		}
		wire[statusOffset] |= 0xf0
		if _, err := codec.Read(bytes.NewReader(wire), testLimits); err != nil {
			t.Fatalf("late status reserved high nibble: %v", err)
		}
	}
}

func TestCRCFrameRejectsCorruptionBeforeAllocation(t *testing.T) {
	data, err := EncodeCRC(Frame{Tag: TagAck, Segments: []Segment{{Alignment: DefaultAlignment, Data: []byte("payload")}}}, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	data[2] = 0xff
	data[3] = 0xff
	data[4] = 0xff
	data[5] = 0x7f
	if _, err := ReadCRC(bytes.NewReader(data), testLimits); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("error = %v", err)
	}

	data, err = EncodeCRC(Frame{Tag: TagAck, Segments: []Segment{{Alignment: DefaultAlignment, Data: []byte("payload")}}}, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	data[PreambleSize] ^= 1
	if _, err := ReadCRC(bytes.NewReader(data), testLimits); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("error = %v", err)
	}
}

func TestCRCFrameRejectsLimitsAndAbortedStatus(t *testing.T) {
	frame := Frame{Tag: TagMessage, Segments: []Segment{
		{Alignment: DefaultAlignment},
		{Alignment: DefaultAlignment, Data: []byte("front")},
	}}
	data, err := EncodeCRC(frame, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ReadCRC(bytes.NewReader(data), Limits{MaxSegmentBytes: 4, MaxFrameBytes: 8192}); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("limit error = %v", err)
	}
	data[len(data)-13] = LateStatusAborted
	if _, err := ReadCRC(bytes.NewReader(data), testLimits); !errors.Is(err, ErrAborted) {
		t.Fatalf("abort error = %v", err)
	}
}

type oneByteReader struct{ data []byte }

func (reader *oneByteReader) Read(output []byte) (int, error) {
	if len(reader.data) == 0 {
		return 0, errors.New("unexpected end")
	}
	output[0] = reader.data[0]
	reader.data = reader.data[1:]
	return 1, nil
}
