package msgr

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestP02BannerFixture(t *testing.T) {
	want := readP02Fixture(t, "banner-rev1.bin")
	banner := ClientBanner()
	if got := banner.Encode(); !bytes.Equal(got, want) {
		t.Errorf("encoded banner = %x, want %x", got, want)
	}
	decoded, err := ReadBanner(&oneByteReader{data: append([]byte(nil), want...)}, 16)
	if err != nil {
		t.Fatal(err)
	}
	if decoded != banner {
		t.Fatalf("decoded banner = %#v, want %#v", decoded, banner)
	}
}

func TestP02CRCFixtures(t *testing.T) {
	tests := []struct {
		name  string
		frame Frame
	}{
		{
			name: "crc-one-segment.bin",
			frame: Frame{Tag: TagAck, Segments: []Segment{
				{Alignment: DefaultAlignment, Data: []byte("ceph")},
			}},
		},
		{
			name: "crc-four-segment.bin",
			frame: Frame{Tag: TagMessage, Segments: []Segment{
				{Alignment: DefaultAlignment, Data: []byte("header")},
				{Alignment: DefaultAlignment, Data: []byte{}},
				{Alignment: DefaultAlignment, Data: []byte("middle")},
				{Alignment: PageAlignment, Data: []byte{0, 1, 2, 3}},
			}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			want := readP02Fixture(t, test.name)
			got, err := EncodeCRC(test.frame, testLimits)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Errorf("encoded frame = %x, want %x", got, want)
			}
			decoded, err := ReadCRC(&oneByteReader{data: append([]byte(nil), want...)}, testLimits)
			if err != nil {
				t.Fatal(err)
			}
			assertFrameEqual(t, decoded, test.frame)
		})
	}
}

func TestP02SecureFixtures(t *testing.T) {
	tests := []struct {
		name  string
		frame Frame
	}{
		{
			name: "secure-one-segment.bin",
			frame: Frame{Tag: TagAck, Segments: []Segment{
				{Alignment: DefaultAlignment, Data: []byte("ceph")},
			}},
		},
		{
			name: "secure-multi-record.bin",
			frame: Frame{Tag: TagMessage, Segments: []Segment{
				{Alignment: DefaultAlignment, Data: fixtureSequence(63)},
				{Alignment: DefaultAlignment, Data: []byte{}},
				{Alignment: DefaultAlignment, Data: []byte("middle")},
				{Alignment: PageAlignment, Data: fixtureSequence(32)},
			}},
		},
	}
	secret := fixtureSequence(64)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			want := readP02Fixture(t, test.name)
			encoder, err := NewSecureCodec(secret, false)
			if err != nil {
				t.Fatal(err)
			}
			got, err := encoder.Encode(test.frame, testLimits)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Errorf("encoded frame = %x, want %x", got, want)
			}
			decoder, err := NewSecureCodec(secret, true)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := decoder.Read(&oneByteReader{data: append([]byte(nil), want...)}, testLimits)
			if err != nil {
				t.Fatal(err)
			}
			assertFrameEqual(t, decoded, test.frame)
		})
	}
}

func TestP02UpstreamFrameAssemblerCRCFixtures(t *testing.T) {
	oneSegment := Frame{Tag: TagAck, Segments: []Segment{
		{Alignment: DefaultAlignment, Data: []byte("ceph")},
	}}
	fourSegments := Frame{Tag: TagMessage, Segments: []Segment{
		{Alignment: DefaultAlignment, Data: []byte("header")},
		{Alignment: DefaultAlignment},
		{Alignment: DefaultAlignment, Data: []byte("middle")},
		{Alignment: PageAlignment, Data: []byte{0, 1, 2, 3}},
	}}
	tests := []struct {
		name  string
		codec CRCCodec
		frame Frame
	}{
		{"upstream-crc-one-segment.bin", CRCCodec{WithDataCRC: true}, oneSegment},
		{"upstream-crc-disabled-one-segment.bin", CRCCodec{WithDataCRC: false}, oneSegment},
		{"upstream-crc-four-segment.bin", CRCCodec{WithDataCRC: true}, fourSegments},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertP02CRCFixture(t, test.codec, test.frame, readP02UpstreamFixture(t, test.name))
		})
	}
}

func TestP02UpstreamFrameAssemblerControlAndMessageFixtures(t *testing.T) {
	ack, err := EncodeControl(Ack{Sequence: 0x0102030405060708}, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	assertP02CRCFixture(t, CRCCodec{WithDataCRC: true}, ack,
		readP02UpstreamFixture(t, "upstream-ack-control.bin"))

	message, err := EncodeMessage(Message{
		Header: MessageHeader{
			Sequence:             0x0102030405060708,
			TransactionID:        0x1112131415161718,
			Type:                 0x2122,
			Priority:             0x3132,
			Version:              0x4142,
			DataPrePaddingLength: 2,
			DataOffset:           0x5152,
			AckSequence:          0x6162636465666768,
			Flags:                0x71,
			CompatVersion:        0x8182,
		},
		Lengths: MessageLengths{Front: 5, Middle: 3, Data: 2},
		Front:   []byte("front"),
		Middle:  []byte("mid"),
		Data:    []byte{0, 1},
	}, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	assertP02CRCFixture(t, CRCCodec{WithDataCRC: true}, message,
		readP02UpstreamFixture(t, "upstream-message-frame.bin"))
}

func TestP02UpstreamFrameAssemblerSecureFixtures(t *testing.T) {
	tests := []struct {
		name  string
		frame Frame
	}{
		{
			name: "upstream-secure-one-segment.bin",
			frame: Frame{Tag: TagAck, Segments: []Segment{
				{Alignment: DefaultAlignment, Data: []byte("ceph")},
			}},
		},
		{
			name: "upstream-secure-multi-record.bin",
			frame: Frame{Tag: TagMessage, Segments: []Segment{
				{Alignment: DefaultAlignment, Data: fixtureSequence(63)},
				{Alignment: DefaultAlignment},
				{Alignment: DefaultAlignment, Data: []byte("middle")},
				{Alignment: PageAlignment, Data: fixtureSequence(32)},
			}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			want := readP02UpstreamFixture(t, test.name)
			encoder, err := NewSecureCodec(fixtureSequence(64), false)
			if err != nil {
				t.Fatal(err)
			}
			got, err := encoder.Encode(test.frame, testLimits)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Errorf("encoded frame = %x, want %x", got, want)
			}
			decoder, err := NewSecureCodec(fixtureSequence(64), true)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := decoder.Read(&oneByteReader{data: append([]byte(nil), want...)}, testLimits)
			if err != nil {
				t.Fatal(err)
			}
			assertFrameEqual(t, decoded, test.frame)
		})
	}
}

func assertP02CRCFixture(t *testing.T, codec CRCCodec, frame Frame, want []byte) {
	t.Helper()
	got, err := codec.Encode(frame, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("encoded frame = %x, want %x", got, want)
	}
	decoded, err := codec.Read(&oneByteReader{data: append([]byte(nil), want...)}, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	assertFrameEqual(t, decoded, frame)
}

func readP02Fixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "p02", name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func readP02UpstreamFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "p02", "upstream", name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func fixtureSequence(size int) []byte {
	data := make([]byte, size)
	for index := range data {
		data[index] = byte(index)
	}
	return data
}
