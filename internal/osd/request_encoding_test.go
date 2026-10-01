package osd

import (
	"bytes"
	"encoding/hex"
	"errors"
	"math"
	"strings"
	"testing"

	wire "github.com/otuschhoff/rados-go/internal/encoding"
	"github.com/otuschhoff/rados-go/internal/maps"
)

func TestEncodeRequestFullFrontGolden(t *testing.T) {
	request := Request{
		MapEpoch: 9, PG: maps.PG{Pool: 7, Seed: 3, Preferred: -1},
		ObjectHash: 0x12345678, PoolID: 7, Object: "object",
		Locator: "locator", Namespace: "namespace", Snapshot: NoSnap,
		TransactionID: 41, ClientGlobalID: 23, ClientIncarnation: 17,
		Retry: -1, Features: 0x1122334455667788,
		Operations: []Operation{{Code: OpRead, Offset: 11, Length: 4096}},
	}
	want, err := hex.DecodeString(
		"01011200000001070000000000000003000000ffffffffff" +
			"785634120900000010000000" +
			"020215000000081700000000000000290000000000000011000000" +
			"000000000000000000000000000000000000000000000000" +
			"110000000000000000000000" +
			"06032c0000000700000000000000ffffffff070000006c6f6361746f72" +
			"090000006e616d657370616365ffffffffffffffff" +
			"060000006f626a6563740100" +
			"0112000000000b00000000000000001000000000000000000000000000000000000000000000" +
			"feffffffffffffff000000000000000000000000ffffffff8877665544332211")
	if err != nil {
		t.Fatal(err)
	}
	message, err := EncodeRequest(request, testLimits)
	if err != nil || !bytes.Equal(message.Front, want) {
		t.Fatalf("front=%x want=%x err=%v", message.Front, want, err)
	}
	if size, err := requestFrontSize(request, uint64(len(want))); err != nil || size != uint64(len(want)) {
		t.Fatalf("size=%d want=%d err=%v", size, len(want), err)
	}
}

func TestEncodeRequestFrontLimits(t *testing.T) {
	for _, test := range []struct {
		name    string
		request Request
	}{
		{"empty strings", Request{Operations: []Operation{{Code: OpRead}}}},
		{"strings", Request{Object: "object", Locator: "locator", Namespace: "namespace", Operations: []Operation{{Code: OpStat}}}},
		{"multiple operations", Request{Operations: []Operation{{Code: OpRead}, {Code: OpStat}, {Code: OpGetXattrs}}}},
		{"snapshots and data", Request{SnapshotSequence: 9, WriteSnapshots: []uint64{9, 7, 2}, Operations: []Operation{{Code: OpWriteFull, Data: []byte("abc")}}}},
		{"beyond hint cap", Request{Object: strings.Repeat("x", 8192), Operations: []Operation{{Code: OpRead}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			message, err := EncodeRequest(test.request, Limits{MaxBytes: math.MaxUint32, MaxOperations: 4})
			if err != nil {
				t.Fatal(err)
			}
			size, err := requestFrontSize(test.request, math.MaxUint32)
			if err != nil || size != uint64(len(message.Front)) {
				t.Fatalf("size=%d front=%d err=%v", size, len(message.Front), err)
			}
			total := uint32(len(message.Front) + len(message.Data))
			exact, err := EncodeRequest(test.request, Limits{MaxBytes: total, MaxOperations: 4})
			if err != nil || !bytes.Equal(exact.Front, message.Front) || !bytes.Equal(exact.Data, message.Data) {
				t.Fatalf("exact budget changed output: %v", err)
			}
			for _, budget := range []uint32{0, 1, total - 1} {
				if _, err := EncodeRequest(test.request, Limits{MaxBytes: budget, MaxOperations: 4}); !errors.Is(err, wire.ErrLimitExceeded) {
					t.Fatalf("budget=%d err=%v", budget, err)
				}
			}
		})
	}
	request := Request{Object: strings.Repeat("x", 8192), Operations: []Operation{{Code: OpRead}}}
	if _, err := requestFrontSize(request, 512); !errors.Is(err, wire.ErrLimitExceeded) {
		t.Fatalf("oversized string err=%v", err)
	}
	for _, budget := range []uint64{0, 170, 171, 208} {
		if _, err := requestFrontSize(Request{Operations: []Operation{{Code: OpRead}}}, budget); !errors.Is(err, wire.ErrLimitExceeded) {
			t.Fatalf("front budget=%d err=%v", budget, err)
		}
	}
}

func TestEncodeRequestOwnershipAndReplay(t *testing.T) {
	request := Request{
		Object: "object", SnapshotSequence: 9, WriteSnapshots: []uint64{9, 7},
		Operations: []Operation{{Code: OpWriteFull, Data: []byte("abc")}},
	}
	first, err := EncodeRequest(request, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	front := bytes.Clone(first.Front)
	data := bytes.Clone(first.Data)
	replay, err := EncodeRequest(request, testLimits)
	if err != nil || !bytes.Equal(first.Front, replay.Front) || !bytes.Equal(first.Data, replay.Data) {
		t.Fatalf("replay changed frame: %v", err)
	}
	request.Operations[0].Data[0] = 'z'
	request.Operations[0].Offset = 99
	request.WriteSnapshots[0] = 8
	if !bytes.Equal(first.Front, front) || !bytes.Equal(first.Data, data) {
		t.Fatal("caller mutation changed owned frame")
	}
	replay.Front[0] ^= 0xff
	replay.Data[0] = 'q'
	if !bytes.Equal(first.Front, front) || !bytes.Equal(first.Data, data) {
		t.Fatal("replayed frames alias")
	}
}
