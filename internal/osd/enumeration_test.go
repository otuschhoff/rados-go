package osd

import (
	"errors"
	"math"
	"reflect"
	"testing"

	wire "github.com/otuschhoff/rados-go/internal/encoding"
)

func TestPGNLSOperationCodec(t *testing.T) {
	cursor := HObject{Key: "key", Object: "object", Snapshot: NoSnap, Hash: 0x12345678, Namespace: "ns", Pool: 7}
	operation, err := EncodePGNLSOperation(cursor, 19, 23, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if operation.Code != OpPGNList || operation.ListCount != 19 || operation.ListStartEpoch != 23 {
		t.Fatalf("operation=%+v", operation)
	}
	decoder := wire.NewDecoder(operation.Data, wire.Limits{MaxBytes: 4096})
	decoded, err := decodeHObject(decoder)
	if err != nil || decoder.Remaining() != 0 || !reflect.DeepEqual(decoded, cursor) {
		t.Fatalf("cursor=%+v err=%v remaining=%d", decoded, err, decoder.Remaining())
	}
	if _, err := EncodePGNLSOperation(cursor, 0, 23, 4096); !errors.Is(err, wire.ErrMalformed) {
		t.Fatalf("zero count error=%v", err)
	}
}

func TestDecodePGNLSPage(t *testing.T) {
	next := HObject{Object: "next", Snapshot: NoSnap, Hash: 9, Pool: 7}
	encoder := wire.NewEncoder(4096)
	encoder.Versioned(1, 1, func(payload *wire.Encoder) {
		encodeHObject(payload, next)
		payload.Uint32(2)
		payload.String("")
		payload.String("one")
		payload.String("")
		payload.String("ns")
		payload.String("two")
		payload.String("locator")
	})
	data, err := encoder.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	page, err := DecodePGNLSPage(data, 4096, 2)
	if err != nil {
		t.Fatal(err)
	}
	want := ListPage{Next: next, Entries: []ListEntry{{Object: "one"}, {Namespace: "ns", Object: "two", Locator: "locator"}}}
	if !reflect.DeepEqual(page, want) {
		t.Fatalf("page=%+v want=%+v", page, want)
	}
	if _, err := DecodePGNLSPage(data, 4096, 1); !errors.Is(err, wire.ErrLimitExceeded) {
		t.Fatalf("entry limit error=%v", err)
	}
}

func TestHObjectBitwiseOrdering(t *testing.T) {
	left := HObject{Hash: 1, Pool: 7}
	right := HObject{Hash: 2, Pool: 7}
	if ReverseBits(1) != 0x80000000 || ReverseBits(2) != 0x40000000 {
		t.Fatal("bit reversal mismatch")
	}
	if CompareHObject(left, right) <= 0 {
		t.Fatal("hobject order is not bit-reversed by hash")
	}
}

func FuzzDecodePGNLSPage(f *testing.F) {
	encoder := wire.NewEncoder(256)
	encoder.Versioned(1, 1, func(payload *wire.Encoder) {
		encodeHObject(payload, HObject{Max: true, Pool: math.MinInt64})
		payload.Uint32(1)
		payload.String("namespace")
		payload.String("object")
		payload.String("locator")
	})
	seed, err := encoder.BytesResult()
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodePGNLSPage(data, 4096, 64)
	})
}

func TestCompareHObjectCanonicalizesMaxSentinel(t *testing.T) {
	decoded := HObject{Snapshot: 0, Hash: 0, Max: true, Pool: math.MinInt64}
	local := HObject{Max: true}
	if CompareHObject(decoded, local) != 0 || CompareHObject(local, decoded) != 0 {
		t.Fatal("max sentinels with ignored payload fields must compare equal")
	}
}
