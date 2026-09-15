package osd

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"

	wire "github.com/otuschhoff/go-librados/internal/encoding"
	"github.com/otuschhoff/go-librados/internal/maps"
	"github.com/otuschhoff/go-librados/internal/msgr"
	"github.com/otuschhoff/go-librados/internal/protocol"
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
	if requestIDVersion != 2 || !bytes.Equal(requestID.Raw(21), make([]byte, 21)) || requestID.Remaining() != 0 || !bytes.Equal(decoder.Raw(24), make([]byte, 24)) || decoder.Uint32() != 0 {
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

func TestEncodeRequestPreservesSnapshotZero(t *testing.T) {
	message, err := EncodeRequest(Request{PG: maps.PG{Pool: 1, Preferred: -1}, PoolID: 1, Snapshot: 0, Retry: -1, Operations: []Operation{{Code: OpStat}}}, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot := binary.LittleEndian.Uint64(message.Front[len(message.Front)-32:]); snapshot != 0 {
		t.Fatalf("snapshot=%d", snapshot)
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
				TransactionID: 41, ClientIncarnation: 17, Retry: 2, Operations: []Operation{test.operation},
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
			requestID.Raw(17)
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
