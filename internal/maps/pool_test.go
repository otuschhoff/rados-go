package maps

import (
	"errors"
	"testing"

	wire "github.com/otuschhoff/rados-go/internal/encoding"
)

func TestDecodePoolSnapshotsRejectsDuplicateAndMismatchedIDs(t *testing.T) {
	tests := []struct {
		name  string
		keys  []uint64
		ids   []uint64
		names []string
	}{
		{"duplicate id", []uint64{2, 2}, []uint64{2, 2}, []string{"a", "b"}},
		{"mismatched id", []uint64{2}, []uint64{3}, []string{"a"}},
		{"duplicate name", []uint64{2, 3}, []uint64{2, 3}, []string{"a", "a"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoder := wire.NewEncoder(1024)
			encoder.Uint32(uint32(len(test.keys)))
			for index, key := range test.keys {
				encoder.Uint64(key)
				encoder.Versioned(2, 2, func(snapshot *wire.Encoder) {
					snapshot.Uint64(test.ids[index])
					snapshot.Uint32(1)
					snapshot.Uint32(2)
					snapshot.String(test.names[index])
				})
			}
			data, err := encoder.BytesResult()
			if err != nil {
				t.Fatal(err)
			}
			decoder := wire.NewDecoder(data, wire.Limits{MaxBytes: 1024})
			if _, err := decodePoolSnapshots(decoder, Limits{MaxBytes: 1024, MaxCollectionEntries: 8}); !errors.Is(err, ErrMalformedMap) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}
