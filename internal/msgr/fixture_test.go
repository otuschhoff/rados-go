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

func readP02Fixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "p02", name))
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
