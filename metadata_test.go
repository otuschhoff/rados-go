package rados

import (
	"encoding/binary"
	"errors"
	"testing"

	wire "github.com/otuschhoff/go-librados/internal/encoding"
	"github.com/otuschhoff/go-librados/internal/objecter"
	"github.com/otuschhoff/go-librados/internal/osd"
)

func TestWriteOpCopiesInputAndFreezes(t *testing.T) {
	write := []byte("write")
	value := []byte("value")
	key := []byte("key")
	op := NewWriteOp()
	op.Write(4, write)
	op.SetOMAP([]OMAPEntry{{Key: key, Value: value}})
	compareIndex := op.CompareExtent(0, []byte("compare"))
	write[0], key[0], value[0] = 'X', 'X', 'X'

	operations, err := op.freeze()
	if err != nil {
		t.Fatal(err)
	}
	if compareIndex != 2 || len(operations) != 3 || string(operations[0].Data) != "write" {
		t.Fatalf("index=%d operations=%+v", compareIndex, operations)
	}
	entries, err := osd.DecodeMetadataMap(operations[1].Data, uint32(len(operations[1].Data)), 1)
	if err != nil || len(entries) != 1 || string(entries[0].Key) != "key" || string(entries[0].Value) != "value" {
		t.Fatalf("entries=%+v error=%v", entries, err)
	}
	if _, err := op.freeze(); !errors.Is(err, wire.ErrMalformed) {
		t.Fatalf("second freeze error=%v", err)
	}
	if index := op.add(osd.Operation{Code: osd.OpDelete}); index != -1 {
		t.Fatalf("post-freeze index=%d", index)
	}
}

func TestReadOpIndexesAndDefersValidation(t *testing.T) {
	op := NewReadOp()
	if index := op.Read(0, 8); index != 0 {
		t.Fatalf("read index=%d", index)
	}
	op.AssertVersion(9)
	if index := op.GetXAttr("name"); index != 2 {
		t.Fatalf("xattr index=%d", index)
	}
	if index := op.ListOMAP("", 0); index != -1 {
		t.Fatalf("invalid OMAP index=%d", index)
	}
	operations, err := op.freeze()
	if !errors.Is(err, wire.ErrMalformed) || len(operations) != 3 {
		t.Fatalf("operations=%+v error=%v", operations, err)
	}
}

func TestOperationFlagsPreserveInternalFlagsAndRejectUnknownValues(t *testing.T) {
	read := NewReadOp()
	index := read.GetXAttr("optional")
	read.SetFlags(index, SubOpFlagFailOK)
	readOperations, err := read.freeze()
	if err != nil || readOperations[index].Flags != osd.OpFlagFailOK {
		t.Fatalf("read operations=%+v error=%v", readOperations, err)
	}

	write := NewWriteOp()
	write.Create(true)
	write.SetFlags(0, SubOpFlagFailOK)
	writeOperations, err := write.freeze()
	if err != nil || writeOperations[0].Flags != osd.OpFlagExclusive|osd.OpFlagFailOK {
		t.Fatalf("write operations=%+v error=%v", writeOperations, err)
	}

	invalid := NewReadOp()
	invalid.Read(0, 1)
	invalid.SetFlags(0, SubOpFlags(1<<31))
	if _, err := invalid.freeze(); !errors.Is(err, wire.ErrMalformed) {
		t.Fatalf("unknown flags error=%v", err)
	}
}

func TestOperationsRejectNULXAttrNames(t *testing.T) {
	read := NewReadOp()
	if index := read.GetXAttr("bad\x00name"); index != -1 {
		t.Fatalf("read index=%d", index)
	}
	if _, err := read.freeze(); !errors.Is(err, wire.ErrMalformed) {
		t.Fatalf("read error=%v", err)
	}
	write := NewWriteOp()
	write.SetXAttr("bad\x00name", nil)
	if _, err := write.freeze(); !errors.Is(err, wire.ErrMalformed) {
		t.Fatalf("write error=%v", err)
	}
}

func TestWriteOpEncodesOMAPRange(t *testing.T) {
	op := NewWriteOp()
	op.RemoveOMAPRange([]byte("a"), []byte("z"))
	operations, err := op.freeze()
	if err != nil || len(operations) != 1 || operations[0].Code != osd.OpOmapRemoveRange || operations[0].Length != uint64(len(operations[0].Data)) {
		t.Fatalf("operations=%+v error=%v", operations, err)
	}
}

func TestPublicOperationResultDecodesValueOnlyForStat(t *testing.T) {
	payload := make([]byte, 16)
	binary.LittleEndian.PutUint64(payload, 42)
	result := objecter.Result{Operations: []objecter.OperationResult{{Data: payload}, {Data: payload}}}
	public := publicOperationResult("read", "object", []osd.Operation{{Code: osd.OpRead}, {Code: osd.OpStat}}, result)
	if public.Results[0].Value != 0 || public.Results[1].Value != 42 {
		t.Fatalf("results=%+v", public.Results)
	}
}
