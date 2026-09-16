package osd

import (
	"errors"
	"testing"
)

func TestDecodeChecksum(t *testing.T) {
	data := []byte{2, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8}
	result, err := DecodeChecksum(data, ChecksumCRC32C, 64)
	if err != nil {
		t.Fatal(err)
	}
	if string(result) != string(data) {
		t.Fatalf("result=%x want=%x", result, data)
	}
	data[4] = 9
	if result[4] != 1 {
		t.Fatal("result retained the response buffer")
	}
	empty, err := DecodeChecksum([]byte{0, 0, 0, 0}, ChecksumCRC32C, 64)
	if err != nil || empty == nil || string(empty) != "\x00\x00\x00\x00" {
		t.Fatalf("empty=%x error=%v", empty, err)
	}
}

func TestDecodeChecksumRejectsMalformedResults(t *testing.T) {
	for _, test := range []struct {
		name string
		data []byte
		kind uint8
	}{
		{name: "unknown type", data: []byte{0, 0, 0, 0}, kind: 3},
		{name: "missing count", data: nil, kind: ChecksumCRC32C},
		{name: "truncated count", data: []byte{1, 0, 0}, kind: ChecksumCRC32C},
		{name: "truncated value", data: []byte{1, 0, 0, 0, 1}, kind: ChecksumCRC32C},
		{name: "trailing value", data: []byte{0, 0, 0, 0, 1}, kind: ChecksumCRC32C},
		{name: "wrong width", data: []byte{1, 0, 0, 0, 1, 2, 3, 4}, kind: ChecksumXXHash64},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := DecodeChecksum(test.data, test.kind, 64); !errors.Is(err, ErrMalformedReply) {
				t.Fatalf("error=%v", err)
			}
		})
	}
	if _, err := DecodeChecksum([]byte{1, 0, 0, 0, 1, 2, 3, 4}, ChecksumCRC32C, 7); !errors.Is(err, ErrMalformedReply) {
		t.Fatalf("limit error=%v", err)
	}
}
