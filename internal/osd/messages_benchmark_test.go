package osd

import (
	"testing"

	"github.com/otuschhoff/rados-go/internal/maps"
	"github.com/otuschhoff/rados-go/internal/msgr"
)

var benchmarkRequestMessage msgr.Message

func BenchmarkEncodeRequestFullFrontSingleOpRead(b *testing.B) {
	request := Request{
		MapEpoch: 9, PG: maps.PG{Pool: 7, Seed: 3, Preferred: -1},
		ObjectHash: 0x12345678, PoolID: 7, Object: "object",
		Locator: "locator", Namespace: "namespace", Snapshot: NoSnap,
		TransactionID: 41, ClientGlobalID: 23, ClientIncarnation: 17,
		Retry: -1, Features: 0x1122334455667788,
		Operations: []Operation{{Code: OpRead, Offset: 11, Length: 4096}},
	}
	limits := Limits{MaxBytes: 64 << 20, MaxOperations: 64}
	b.ReportAllocs()
	for b.Loop() {
		message, err := EncodeRequest(request, limits)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkRequestMessage = message
	}
}
