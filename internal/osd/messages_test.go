package osd

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"reflect"
	"testing"

	wire "github.com/otuschhoff/rados-go/internal/encoding"
	"github.com/otuschhoff/rados-go/internal/maps"
	"github.com/otuschhoff/rados-go/internal/msgr"
	"github.com/otuschhoff/rados-go/internal/protocol"
)

var testLimits = Limits{MaxBytes: 4096, MaxOperations: 4}

func TestEncodeReadRequestV8(t *testing.T) {
	if NoSnap != uint64(0xfffffffffffffffe) {
		t.Fatalf("NoSnap=%#x", NoSnap)
	}
	request := Request{
		MapEpoch: 9, PG: maps.PG{Pool: 7, Seed: 3, Preferred: -1}, ObjectHash: 0x12345678,
		PoolID: 7, Object: "object", Locator: "locator", Namespace: "namespace", Snapshot: NoSnap, Retry: -1, Features: 0x1122334455667788,
		Operations: []Operation{{Code: OpRead, Offset: 11, Length: 22}},
	}
	message, err := EncodeRequest(request, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	if message.Header.Type != protocol.MessageOSDOp || message.Header.Version != 8 || message.Header.CompatVersion != 3 || message.Lengths.Front != uint32(len(message.Front)) {
		t.Fatalf("header=%+v lengths=%+v", message.Header, message.Lengths)
	}
	decoder := wire.NewDecoder(message.Front, wire.Limits{MaxBytes: testLimits.MaxBytes})
	spgVersion, spg := decoder.Versioned(1)
	pg, err := decodePG(spg)
	if err != nil || spgVersion != 1 || pg != request.PG || spg.Uint8() != 0xff || spg.Remaining() != 0 {
		t.Fatalf("pg=%+v err=%v", pg, err)
	}
	if decoder.Uint32() != request.ObjectHash || decoder.Uint32() != 9 || decoder.Uint32() != FlagRead {
		t.Fatal("map epoch or flags mismatch")
	}
	requestIDVersion, requestID := decoder.Versioned(2)
	if requestIDVersion != 2 || requestID.Uint8() != uint8(protocol.EntityClient) || requestID.Uint64() != 0 || requestID.Uint64() != 0 || requestID.Int32() != 0 || requestID.Remaining() != 0 || !bytes.Equal(decoder.Raw(24), make([]byte, 24)) || decoder.Uint32() != 0 {
		t.Fatal("request identity or client incarnation mismatch")
	}
	if !bytes.Equal(decoder.Raw(8), make([]byte, 8)) {
		t.Fatal("mtime mismatch")
	}
	version, locator := decoder.Versioned(6)
	if version != 6 || locator.Int64() != 7 || locator.Int32() != -1 || locator.String() != "locator" || locator.String() != "namespace" || locator.Int64() != -1 || locator.Remaining() != 0 {
		t.Fatal("locator mismatch")
	}
	if decoder.String() != "object" || decoder.Uint16() != 1 {
		t.Fatal("object or operation count mismatch")
	}
	code, payloadLength := decodeOperation(decoder)
	if code != OpRead || payloadLength != 0 || decoder.Uint64() != NoSnap || decoder.Uint64() != 0 || decoder.Uint32() != 0 || decoder.Int32() != -1 || decoder.Uint64() != request.Features || decoder.Remaining() != 0 {
		t.Fatalf("operation or request tail mismatch")
	}
}

func TestEncodeRequestIncludesErasureShard(t *testing.T) {
	request := Request{
		PG: maps.PG{Pool: 7, Seed: 3, Preferred: -1}, Shard: 2, Sharded: true,
		PoolID: 7, Snapshot: NoSnap, Retry: -1, Operations: []Operation{{Code: OpStat}},
	}
	message, err := EncodeRequest(request, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	decoder := wire.NewDecoder(message.Front, wire.Limits{MaxBytes: testLimits.MaxBytes})
	_, spg := decoder.Versioned(1)
	if pg, err := decodePG(spg); err != nil || pg != request.PG || spg.Uint8() != 2 || spg.Remaining() != 0 {
		t.Fatalf("EC spg decode: pg=%+v err=%v", pg, err)
	}
}

func TestEncodeRequestPreservesSnapshotZero(t *testing.T) {
	message, err := EncodeRequest(Request{PG: maps.PG{Pool: 1, Preferred: -1}, PoolID: 1, Snapshot: 0, Retry: -1, Operations: []Operation{{Code: OpStat}}}, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot := binary.LittleEndian.Uint64(message.Front[len(message.Front)-32:]); snapshot != 0 {
		t.Fatalf("snapshot=%d", snapshot)
	}
}

func TestEncodeRequestPreservesWriteSnapshotContext(t *testing.T) {
	message, err := EncodeRequest(Request{
		PG: maps.PG{Pool: 1, Preferred: -1}, PoolID: 1, Snapshot: NoSnap,
		SnapshotSequence: 9, WriteSnapshots: []uint64{9, 7, 2}, Retry: -1,
		Operations: []Operation{{Code: OpWriteFull, Data: []byte("x")}},
	}, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	tail := message.Front[len(message.Front)-56:]
	if binary.LittleEndian.Uint64(tail) != NoSnap || binary.LittleEndian.Uint64(tail[8:]) != 9 || binary.LittleEndian.Uint32(tail[16:]) != 3 || binary.LittleEndian.Uint64(tail[20:]) != 9 || binary.LittleEndian.Uint64(tail[28:]) != 7 || binary.LittleEndian.Uint64(tail[36:]) != 2 || int32(binary.LittleEndian.Uint32(tail[44:])) != -1 {
		t.Fatal("write snapshot context was not preserved")
	}
}

func TestEncodeRequestRejectsInvalidWriteSnapshotContext(t *testing.T) {
	for _, snapshots := range [][]uint64{{8, 9}, {9, 9}, {10}} {
		_, err := EncodeRequest(Request{
			PG: maps.PG{Pool: 1, Preferred: -1}, PoolID: 1, Snapshot: NoSnap,
			SnapshotSequence: 9, WriteSnapshots: snapshots,
			Operations: []Operation{{Code: OpWriteFull, Data: []byte("x")}},
		}, testLimits)
		if !errors.Is(err, wire.ErrMalformed) {
			t.Fatalf("snapshots %v: error = %v", snapshots, err)
		}
	}
}

func TestEncodeMutationRequestsV8(t *testing.T) {
	tests := []struct {
		name      string
		operation Operation
		data      []byte
	}{
		{name: "write", operation: Operation{Code: OpWrite, Offset: 9, Length: 3, Data: []byte("abc")}, data: []byte("abc")},
		{name: "write full", operation: Operation{Code: OpWriteFull, Length: 3, Data: []byte("abc")}, data: []byte("abc")},
		{name: "append", operation: Operation{Code: OpAppend, Length: 3, Data: []byte("abc")}, data: []byte("abc")},
		{name: "truncate", operation: Operation{Code: OpTruncate, Offset: 9}},
		{name: "zero", operation: Operation{Code: OpZero, Offset: 9, Length: 3}},
		{name: "delete", operation: Operation{Code: OpDelete}},
		{name: "create", operation: Operation{Code: OpCreate}},
		{name: "exclusive create", operation: Operation{Code: OpCreate, Flags: OpFlagExclusive}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			message, err := EncodeRequest(Request{
				PG: maps.PG{Pool: 1, Preferred: -1}, PoolID: 1, Snapshot: NoSnap,
				TransactionID: 41, ClientGlobalID: 23, ClientIncarnation: 17, Retry: 2, Operations: []Operation{test.operation},
			}, testLimits)
			if err != nil {
				t.Fatal(err)
			}
			if message.Header.TransactionID != 41 || !bytes.Equal(message.Data, test.data) || message.Lengths.Data != uint32(len(test.data)) {
				t.Fatalf("header=%+v data=%x lengths=%+v", message.Header, message.Data, message.Lengths)
			}
			decoder := wire.NewDecoder(message.Front, wire.Limits{MaxBytes: testLimits.MaxBytes})
			_, spg := decoder.Versioned(1)
			spg.Raw(18)
			decoder.Uint32()
			decoder.Uint32()
			if flags := decoder.Uint32(); flags != FlagWrite|FlagOnDisk {
				t.Fatalf("flags=%#x", flags)
			}
			_, requestID := decoder.Versioned(2)
			if entityType, globalID, transactionID := requestID.Uint8(), requestID.Uint64(), requestID.Uint64(); entityType != uint8(protocol.EntityClient) || globalID != 23 || transactionID != 41 {
				t.Fatalf("request identity=%d/%d/%d", entityType, globalID, transactionID)
			}
			if incarnation := requestID.Int32(); incarnation != 17 {
				t.Fatalf("incarnation=%d", incarnation)
			}
			decoder.Raw(24)
			if clientIncarnation := decoder.Int32(); clientIncarnation != 17 {
				t.Fatalf("client incarnation=%d", clientIncarnation)
			}
			decoder.Raw(8)
			_, locator := decoder.Versioned(6)
			locator.Raw(uint32(locator.Remaining()))
			_ = decoder.String()
			decoder.Uint16()
			code, payloadLength := decodeOperation(decoder)
			if code != test.operation.Code || payloadLength != uint32(len(test.data)) {
				t.Fatalf("code=%#x payload=%d", code, payloadLength)
			}
		})
	}
}

func TestEncodeMutationRejectsPayloadMismatchAndLimit(t *testing.T) {
	request := Request{PG: maps.PG{Pool: 1, Preferred: -1}, PoolID: 1, Operations: []Operation{{Code: OpWrite, Length: 1, PayloadLength: 2, Data: []byte("x")}}}
	if _, err := EncodeRequest(request, testLimits); !errors.Is(err, wire.ErrMalformed) {
		t.Fatalf("payload mismatch error=%v", err)
	}
	request.Operations[0].PayloadLength = 0
	request.Operations[0].Data = make([]byte, testLimits.MaxBytes)
	if _, err := EncodeRequest(request, testLimits); !errors.Is(err, wire.ErrLimitExceeded) {
		t.Fatalf("oversize payload error=%v", err)
	}
}

func TestEncodeOperationUnionFixtures(t *testing.T) {
	tests := []struct {
		name      string
		operation Operation
		union     []byte
	}{
		{name: "extent", operation: Operation{Code: OpCompareExtent, Offset: 0x0102030405060708, Length: 9}, union: []byte{8, 7, 6, 5, 4, 3, 2, 1, 9}},
		{name: "sparse read", operation: Operation{Code: OpSparseRead, Offset: 0x0102030405060708, Length: 9}, union: []byte{8, 7, 6, 5, 4, 3, 2, 1, 9}},
		{name: "xattr", operation: Operation{Code: OpCompareXattr, XattrNameLength: 3, XattrValueLength: 5, CompareOperator: 6, CompareMode: 7}, union: []byte{3, 0, 0, 0, 5, 0, 0, 0, 6, 7}},
		{name: "assert version", operation: Operation{Code: OpAssertVer, AssertVersion: 0x0102030405060708}, union: []byte{0, 0, 0, 0, 0, 0, 0, 0, 8, 7, 6, 5, 4, 3, 2, 1}},
		{name: "rollback", operation: Operation{Code: OpRollback, SnapshotID: 0x0102030405060708}, union: []byte{8, 7, 6, 5, 4, 3, 2, 1}},
		{name: "class call", operation: Operation{Code: OpCall, ClassNameLength: 4, MethodNameLength: 6, ClassInputLength: 0x01020304}, union: []byte{4, 6, 0, 4, 3, 2, 1}},
		{name: "watch", operation: Operation{Code: OpWatch, WatchCookie: 0x0102030405060708, WatchVersion: 9, WatchOperation: WatchOperationReconnect, WatchGeneration: 10, WatchTimeout: 11}, union: []byte{8, 7, 6, 5, 4, 3, 2, 1, 9, 0, 0, 0, 0, 0, 0, 0, 5, 10, 0, 0, 0, 11, 0, 0, 0}},
		{name: "notify", operation: Operation{Code: OpNotify, WatchCookie: 0x0102030405060708}, union: []byte{8, 7, 6, 5, 4, 3, 2, 1}},
		{name: "pg list", operation: Operation{Code: OpPGNList, ListCount: 9, ListStartEpoch: 10}, union: []byte{9, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0}},
		{name: "set allocation hint", operation: Operation{Code: OpSetAllocationHint, ExpectedObjectSize: 0x0102030405060708, ExpectedWriteSize: 9, AllocationFlags: 10}, union: []byte{8, 7, 6, 5, 4, 3, 2, 1, 9, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0}},
		{name: "write same", operation: Operation{Code: OpWriteSame, Offset: 0x0102030405060708, Length: 16, PatternLength: 4}, union: []byte{8, 7, 6, 5, 4, 3, 2, 1, 16, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0}},
		{name: "checksum", operation: Operation{Code: OpChecksum, Offset: 0x0102030405060708, Length: 16, ChunkSize: 4, ChecksumType: 2}, union: []byte{8, 7, 6, 5, 4, 3, 2, 1, 16, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 2}},
		{name: "copy from", operation: Operation{Code: OpCopyFrom, SourceSnapshotID: 0x0102030405060708, SourceVersion: 9, SourceFadviseFlags: 10}, union: []byte{8, 7, 6, 5, 4, 3, 2, 1, 9, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0}},
		{name: "copy from2", operation: Operation{Code: OpCopyFrom2, SourceSnapshotID: 0x0102030405060708, SourceVersion: 9, SourceFadviseFlags: 10}, union: []byte{8, 7, 6, 5, 4, 3, 2, 1, 9, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoder := wire.NewEncoder(64)
			encodeOperation(encoder, test.operation)
			encoded, err := encoder.BytesResult()
			if err != nil {
				t.Fatal(err)
			}
			if len(encoded) != int(operationDescriptorSize) {
				t.Fatalf("descriptor length=%d", len(encoded))
			}
			union := encoded[6:34]
			want := append(append([]byte(nil), test.union...), make([]byte, 28-len(test.union))...)
			if !bytes.Equal(union, want) {
				t.Fatalf("union=%x want=%x", union, want)
			}
		})
	}
}

func TestEncodeCopyFromSourceExactBytes(t *testing.T) {
	encoded, err := EncodeCopyFromSource("source", 7, "locator", "namespace", 11, 13, true, 1024)
	if err != nil {
		t.Fatal(err)
	}
	decoder := wire.NewDecoder(encoded, wire.Limits{MaxBytes: 1024})
	if decoder.String() != "source" {
		t.Fatal("source object was not preserved")
	}
	version, locator := decoder.Versioned(6)
	if version != 6 || locator.Int64() != 7 || locator.Int32() != -1 || locator.String() != "locator" || locator.String() != "namespace" || locator.Int64() != -1 || locator.Remaining() != 0 {
		t.Fatalf("source locator = %x", encoded)
	}
	if decoder.Uint32() != 11 || decoder.Uint64() != 13 || decoder.Remaining() != 0 {
		t.Fatalf("copy-from2 suffix = %x", encoded)
	}
	if _, err := EncodeCopyFromSource("", 7, "", "", 0, 0, false, 1024); !errors.Is(err, wire.ErrMalformed) {
		t.Fatalf("empty source error = %v", err)
	}
}

func TestEncodeChecksumRequestValidation(t *testing.T) {
	valid := Operation{Code: OpChecksum, Offset: 1, Length: 8, ChunkSize: 4, ChecksumType: 2, Data: []byte{1, 2, 3, 4}}
	request := func(operation Operation) Request {
		return Request{PG: maps.PG{Pool: 1, Preferred: -1}, PoolID: 1, Snapshot: NoSnap, Operations: []Operation{operation}}
	}
	message, err := EncodeRequest(request(valid), testLimits)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(message.Data, valid.Data) {
		t.Fatalf("seed=%x want=%x", message.Data, valid.Data)
	}
	invalid := []Operation{
		{Code: OpChecksum, Length: 1, ChecksumType: 3, Data: make([]byte, 4)},
		{Code: OpChecksum, Length: 1, ChecksumType: 0, Data: make([]byte, 8)},
		{Code: OpChecksum, Length: 1, ChecksumType: 1, Data: make([]byte, 4)},
		{Code: OpChecksum, Length: uint64(math.MaxInt32) + 1, ChecksumType: 2, Data: make([]byte, 4)},
		{Code: OpChecksum, Offset: math.MaxUint64, Length: 2, ChecksumType: 2, Data: make([]byte, 4)},
		{Code: OpChecksum, ChunkSize: 1, ChecksumType: 2, Data: make([]byte, 4)},
		{Code: OpChecksum, Length: 5, ChunkSize: 2, ChecksumType: 2, Data: make([]byte, 4)},
	}
	for _, operation := range invalid {
		if _, err := EncodeRequest(request(operation), testLimits); !errors.Is(err, wire.ErrMalformed) {
			t.Fatalf("operation=%+v error=%v", operation, err)
		}
	}
}

func TestEncodeSetAllocationHintRequiresFailOK(t *testing.T) {
	operation := Operation{Code: OpSetAllocationHint, Flags: OpFlagFailOK, ExpectedObjectSize: 64, ExpectedWriteSize: 8}
	if _, err := EncodeRequest(Request{PG: maps.PG{Pool: 1, Preferred: -1}, PoolID: 1, Snapshot: NoSnap, Operations: []Operation{operation}}, testLimits); err != nil {
		t.Fatal(err)
	}
	operation.Flags = 0
	if _, err := EncodeRequest(Request{PG: maps.PG{Pool: 1, Preferred: -1}, PoolID: 1, Snapshot: NoSnap, Operations: []Operation{operation}}, testLimits); !errors.Is(err, wire.ErrMalformed) {
		t.Fatalf("missing FAILOK error=%v", err)
	}
}

func TestEncodeWriteSameRequest(t *testing.T) {
	operation := Operation{Code: OpWriteSame, Offset: 9, Length: 12, PatternLength: 3, Data: []byte("abc")}
	message, err := EncodeRequest(Request{PG: maps.PG{Pool: 1, Preferred: -1}, PoolID: 1, Snapshot: NoSnap, Operations: []Operation{operation}}, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(message.Data, operation.Data) {
		t.Fatalf("data=%x want=%x", message.Data, operation.Data)
	}
}

func TestEncodeWriteSameRejectsInvalidExtents(t *testing.T) {
	tests := []Operation{
		{Code: OpWriteSame, Length: 4},
		{Code: OpWriteSame, Length: 4, PatternLength: 3, Data: []byte("abc")},
		{Code: OpWriteSame, PatternLength: 3, Data: []byte("abc")},
		{Code: OpWriteSame, Length: 4, PatternLength: 4, Data: []byte("abc")},
		{Code: OpWriteSame, Offset: ^uint64(0) - 1, Length: 4, PatternLength: 1, Data: []byte("x")},
	}
	for _, operation := range tests {
		request := Request{PG: maps.PG{Pool: 1, Preferred: -1}, PoolID: 1, Operations: []Operation{operation}}
		if _, err := EncodeRequest(request, testLimits); !errors.Is(err, wire.ErrMalformed) {
			t.Fatalf("operation %+v: error=%v", operation, err)
		}
	}
}

func TestEncodeClassCallRequest(t *testing.T) {
	operation := Operation{
		Code: OpCall, ClassNameLength: 4, MethodNameLength: 10, ClassInputLength: 3,
		Data: []byte("locklist_locks\x00\x01\x02"),
	}
	message, err := EncodeRequest(Request{
		PG: maps.PG{Pool: 1, Preferred: -1}, PoolID: 1, Snapshot: NoSnap,
		Operations: []Operation{operation},
	}, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(message.Data, operation.Data) {
		t.Fatalf("data=%x want=%x", message.Data, operation.Data)
	}
	operation.ClassInputLength++
	if _, err := EncodeRequest(Request{PG: maps.PG{Pool: 1, Preferred: -1}, PoolID: 1, Operations: []Operation{operation}}, testLimits); !errors.Is(err, wire.ErrMalformed) {
		t.Fatalf("malformed class call error=%v", err)
	}
}

func TestEncodeRequestRejectsMalformedXattr(t *testing.T) {
	request := Request{PG: maps.PG{Pool: 1, Preferred: -1}, PoolID: 1, Operations: []Operation{{Code: OpSetXattr, XattrNameLength: 2, XattrValueLength: 2, Data: []byte("abc")}}}
	if _, err := EncodeRequest(request, testLimits); !errors.Is(err, wire.ErrMalformed) {
		t.Fatalf("malformed xattr error=%v", err)
	}
}

func TestDecodeReadReplyV8(t *testing.T) {
	front := wire.NewEncoder(testLimits.MaxBytes)
	front.String("object")
	encodePG(front, maps.PG{Pool: 7, Seed: 3, Preferred: -1})
	front.Int64(1)
	front.Int32(0)
	encodeEVersion(front, 8, 10)
	front.Uint32(12)
	front.Uint32(1)
	encodeOperation(front, Operation{Code: OpRead, Offset: 11, Length: 3, PayloadLength: 3})
	front.Int32(0)
	front.Int32(0)
	encodeEVersion(front, 12, 13)
	front.Uint64(14)
	front.Bool(false)
	front.Int64(0)
	front.Int64(0)
	front.Int64(0)
	encoded, err := front.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	message := msgr.Message{
		Header:  msgr.MessageHeader{Type: protocol.MessageOSDOpReply, Version: 8, CompatVersion: 2},
		Lengths: msgr.MessageLengths{Front: uint32(len(encoded)), Data: 3}, Front: encoded, Data: []byte("abc"),
	}
	reply, err := DecodeReply(message, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	if reply.Object != "object" || reply.PG.Seed != 3 || reply.MapEpoch != 12 || reply.Version != 14 || reply.Result != 0 || !reflect.DeepEqual(reply.Operations, []OperationResult{{Operation: OpRead, Code: 0, Data: []byte("abc")}}) {
		t.Fatalf("reply=%+v", reply)
	}
	message.Data = []byte("ab")
	message.Lengths.Data = 2
	if _, err := DecodeReply(message, testLimits); !errors.Is(err, ErrMalformedReply) {
		t.Fatalf("short data error=%v", err)
	}
}

func TestOSDCodecsRejectInvalidInput(t *testing.T) {
	if _, err := EncodeRequest(Request{PoolID: 1, Operations: []Operation{{Code: 1}}}, testLimits); !errors.Is(err, wire.ErrUnsupportedVersion) {
		t.Fatalf("unsupported operation error=%v", err)
	}
	message := msgr.Message{Header: msgr.MessageHeader{Type: protocol.MessageOSDOpReply, Version: 8, CompatVersion: 2}, Front: []byte{1}, Lengths: msgr.MessageLengths{Front: 1}}
	if _, err := DecodeReply(message, testLimits); err == nil {
		t.Fatal("truncated reply accepted")
	}
}

func TestDecodeReplyRedirect(t *testing.T) {
	front := wire.NewEncoder(testLimits.MaxBytes)
	front.String("source")
	encodePG(front, maps.PG{Pool: 7, Preferred: -1})
	front.Int64(0)
	front.Int32(0)
	encodeEVersion(front, 0, 0)
	front.Uint32(12)
	front.Uint32(0)
	front.Int32(-1)
	encodeEVersion(front, 0, 0)
	front.Uint64(1)
	front.Bool(true)
	front.Versioned(1, 1, func(redirect *wire.Encoder) {
		encodeLocator(redirect, 8, "key", "ns")
		redirect.String("target")
		redirect.Uint32(0)
	})
	front.Int64(0)
	front.Int64(0)
	front.Int64(0)
	encoded, err := front.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	reply, err := DecodeReply(msgr.Message{Header: msgr.MessageHeader{Type: protocol.MessageOSDOpReply, Version: 8, CompatVersion: 2}, Front: encoded, Lengths: msgr.MessageLengths{Front: uint32(len(encoded))}}, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	want := &Redirect{Pool: 8, Locator: "key", Namespace: "ns", Object: "target"}
	if !reflect.DeepEqual(reply.Redirect, want) {
		t.Fatalf("redirect=%+v want=%+v", reply.Redirect, want)
	}
}

func FuzzDecodeReply(f *testing.F) {
	f.Add([]byte{1}, []byte{})
	f.Add([]byte{}, []byte("data"))
	f.Fuzz(func(t *testing.T, front, data []byte) {
		if len(front)+len(data) > int(testLimits.MaxBytes) {
			t.Skip()
		}
		_, _ = DecodeReply(msgr.Message{Header: msgr.MessageHeader{Type: protocol.MessageOSDOpReply, Version: 8, CompatVersion: 2}, Front: front, Data: data}, testLimits)
	})
}
