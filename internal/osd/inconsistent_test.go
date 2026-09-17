package osd

import (
	"testing"

	wire "github.com/otuschhoff/rados-go/internal/encoding"
)

func TestScrubListRequestAndResultWire(t *testing.T) {
	start := InconsistentObject{Object: "before", Namespace: "ns", Snapshot: 9}
	operation, err := EncodeScrubList(17, start, 25, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if operation.Code != OpScrubList {
		t.Fatalf("code=%#x", operation.Code)
	}
	request := wire.NewDecoder(operation.Data, wire.Limits{MaxBytes: 4096})
	version, payload := request.Versioned(1)
	if version != 1 || payload.Uint32() != 17 || payload.Uint32() != 0 || payload.String() != "before" || payload.String() != "ns" || payload.Uint64() != 9 || payload.Uint64() != 25 {
		t.Fatal("unexpected scrub-list request")
	}
	if err := payload.Finish(); err != nil {
		t.Fatal(err)
	}

	itemEncoder := wire.NewEncoder(4096)
	itemEncoder.Versioned(2, 2, func(item *wire.Encoder) {
		item.Uint64(1 << 4)
		item.Versioned(1, 1, func(object *wire.Encoder) {
			object.String("broken")
			object.String("ns")
			object.String("key")
			object.Uint64(3)
		})
		item.Uint64(8)
		item.Uint32(1)
		item.Versioned(1, 1, func(shard *wire.Encoder) {
			shard.Int32(4)
			shard.Int8(1)
		})
		item.Versioned(3, 3, func(info *wire.Encoder) {
			info.Uint64(1 << 1)
			info.Bool(true)
		})
		item.Uint64(1 << 3)
	})
	encodedItem, err := itemEncoder.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	resultEncoder := wire.NewEncoder(4096)
	resultEncoder.Versioned(1, 1, func(result *wire.Encoder) {
		result.Uint32(18)
		result.Uint32(1)
		result.Bytes(encodedItem)
	})
	encodedResult, err := resultEncoder.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	interval, objects, err := DecodeScrubList(encodedResult, 4096, 8)
	if err != nil {
		t.Fatal(err)
	}
	if interval != 18 || len(objects) != 1 || objects[0].Object != "broken" || objects[0].Namespace != "ns" || objects[0].Locator != "key" || objects[0].Snapshot != 3 {
		t.Fatalf("interval=%d objects=%+v", interval, objects)
	}
	if len(objects[0].Shards) != 1 || objects[0].Shards[0] != 4 {
		t.Fatalf("shards=%v", objects[0].Shards)
	}
	wantErrors := []string{"data_digest_mismatch", "shard_missing", "shard_read_error"}
	if len(objects[0].Errors) != len(wantErrors) {
		t.Fatalf("errors=%v", objects[0].Errors)
	}
	for index := range wantErrors {
		if objects[0].Errors[index] != wantErrors[index] {
			t.Fatalf("errors=%v", objects[0].Errors)
		}
	}
}
