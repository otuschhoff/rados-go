package osd

import (
	"errors"
	"testing"

	wire "github.com/otuschhoff/rados-go/internal/encoding"
)

func TestDecodeSparseRead(t *testing.T) {
	encoded := encodeSparseReadFixture(t, [][2]uint64{{10, 2}, {15, 3}}, []byte("abcde"))
	extents, err := DecodeSparseRead(encoded, 10, 8, 128, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(extents) != 2 || extents[0].Offset != 10 || string(extents[0].Data) != "ab" || extents[1].Offset != 15 || string(extents[1].Data) != "cde" {
		t.Fatalf("extents=%+v", extents)
	}
	encoded[len(encoded)-1] = 'X'
	if string(extents[1].Data) != "cde" {
		t.Fatal("result retained the response buffer")
	}
	extents[0].Data[0] = 'Y'
	if string(extents[1].Data) != "cde" {
		t.Fatal("extent data slices share mutable storage")
	}
}

func TestDecodeSparseReadAcceptsEncodedEmptyResult(t *testing.T) {
	encoded := encodeSparseReadFixture(t, nil, nil)
	extents, err := DecodeSparseRead(encoded, 0, 16, 128, 4)
	if err != nil || len(extents) != 0 {
		t.Fatalf("extents=%v error=%v", extents, err)
	}
}

func TestDecodeSparseReadRejectsMalformedResults(t *testing.T) {
	valid := encodeSparseReadFixture(t, [][2]uint64{{10, 2}}, []byte("ab"))
	tests := []struct {
		name string
		data []byte
		max  uint32
	}{
		{name: "empty", data: nil, max: 4},
		{name: "truncated extent", data: valid[:12], max: 4},
		{name: "too many extents", data: []byte{2, 0, 0, 0}, max: 1},
		{name: "zero length", data: encodeSparseReadFixture(t, [][2]uint64{{10, 0}}, nil), max: 4},
		{name: "before request", data: encodeSparseReadFixture(t, [][2]uint64{{9, 1}}, []byte("a")), max: 4},
		{name: "past request", data: encodeSparseReadFixture(t, [][2]uint64{{17, 2}}, []byte("ab")), max: 4},
		{name: "overlap", data: encodeSparseReadFixture(t, [][2]uint64{{10, 3}, {12, 1}}, []byte("abcd")), max: 4},
		{name: "unordered", data: encodeSparseReadFixture(t, [][2]uint64{{12, 1}, {10, 1}}, []byte("ab")), max: 4},
		{name: "short data", data: encodeSparseReadFixture(t, [][2]uint64{{10, 2}}, []byte("a")), max: 4},
		{name: "long data", data: encodeSparseReadFixture(t, [][2]uint64{{10, 2}}, []byte("abc")), max: 4},
		{name: "trailing", data: append(valid, 0), max: 4},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := DecodeSparseRead(test.data, 10, 8, 128, test.max); !errors.Is(err, ErrMalformedReply) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func encodeSparseReadFixture(t *testing.T, extents [][2]uint64, data []byte) []byte {
	t.Helper()
	encoder := wire.NewEncoder(256)
	encoder.Uint32(uint32(len(extents)))
	for _, extent := range extents {
		encoder.Uint64(extent[0])
		encoder.Uint64(extent[1])
	}
	encoder.Bytes(data)
	encoded, err := encoder.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
