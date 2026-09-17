package rados

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"strings"
	"sync"

	wire "github.com/otuschhoff/rados-go/internal/encoding"
	"github.com/otuschhoff/rados-go/internal/objecter"
	"github.com/otuschhoff/rados-go/internal/osd"
	"github.com/otuschhoff/rados-go/internal/protocol"
)

const maxErrno = int32(4095)

type XAttr struct {
	Name  string
	Value []byte
}

type OMAPEntry struct {
	Key   []byte
	Value []byte
}

type Page[T any] struct {
	Values []T
	More   bool
}

type SubOpFlags uint32

const SubOpFlagFailOK SubOpFlags = SubOpFlags(osd.OpFlagFailOK)

type ReadOp struct {
	mu         sync.Mutex
	operations []osd.Operation
	err        error
	executed   bool
}

type WriteOp struct {
	mu         sync.Mutex
	operations []osd.Operation
	err        error
	executed   bool
}

func NewReadOp() *ReadOp   { return &ReadOp{} }
func NewWriteOp() *WriteOp { return &WriteOp{} }

func (object ObjectRef) GetXAttr(ctx context.Context, name string) ([]byte, error) {
	if !validXAttrName(name) || len(name) > math.MaxUint32 {
		return nil, object.invalidOperation("get xattr")
	}
	result, err := object.executeRead(ctx, "get xattr", []osd.Operation{{Code: osd.OpGetXattr, XattrNameLength: uint32(len(name)), Data: []byte(name)}})
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), result.Results[0].Data...), nil
}

func (object ObjectRef) SetXAttr(ctx context.Context, name string, value []byte) (OpResult, error) {
	if !validXAttrName(name) || len(name) > math.MaxUint32 || len(value) > math.MaxUint32 || uint64(len(name))+uint64(len(value)) > math.MaxUint32 {
		return OpResult{}, object.invalidOperation("set xattr")
	}
	data := append([]byte(name), value...)
	return object.executeWrite(ctx, "set xattr", []osd.Operation{{Code: osd.OpSetXattr, XattrNameLength: uint32(len(name)), XattrValueLength: uint32(len(value)), Data: data}})
}

func (object ObjectRef) RemoveXAttr(ctx context.Context, name string) (OpResult, error) {
	if !validXAttrName(name) || len(name) > math.MaxUint32 {
		return OpResult{}, object.invalidOperation("remove xattr")
	}
	return object.executeWrite(ctx, "remove xattr", []osd.Operation{{Code: osd.OpRemoveXattr, XattrNameLength: uint32(len(name)), Data: []byte(name)}})
}

func (object ObjectRef) ListXAttrs(ctx context.Context) ([]XAttr, error) {
	result, err := object.executeRead(ctx, "list xattrs", []osd.Operation{{Code: osd.OpGetXattrs}})
	if err != nil {
		return nil, err
	}
	data := result.Results[0].Data
	entries, err := osd.DecodeMetadataMap(data, decodeLimit(data), uint32(len(data)/8+1))
	if err != nil {
		return nil, object.pool.client.wrapError("list xattrs", object.safeTarget(), err)
	}
	attributes := make([]XAttr, len(entries))
	for index, entry := range entries {
		attributes[index] = XAttr{Name: string(entry.Key), Value: append([]byte(nil), entry.Value...)}
	}
	return attributes, nil
}

func (object ObjectRef) ListOMAP(ctx context.Context, after string, limit uint64) (Page[OMAPEntry], error) {
	if limit == 0 {
		return Page[OMAPEntry]{}, object.invalidOperation("list OMAP")
	}
	payload, err := osd.EncodeOMAPListRequest([]byte(after), limit, math.MaxUint32)
	if err != nil {
		return Page[OMAPEntry]{}, object.pool.client.wrapError("list OMAP", object.safeTarget(), err)
	}
	result, err := object.executeRead(ctx, "list OMAP", []osd.Operation{{Code: osd.OpOmapGetValues, Length: uint64(len(payload)), Data: payload}})
	if err != nil {
		return Page[OMAPEntry]{}, err
	}
	data := result.Results[0].Data
	maximum := uint32(min(limit, uint64(math.MaxUint32)))
	entries, more, err := osd.DecodeOMAPPage(data, decodeLimit(data), maximum)
	if err != nil {
		return Page[OMAPEntry]{}, object.pool.client.wrapError("list OMAP", object.safeTarget(), err)
	}
	page := Page[OMAPEntry]{Values: make([]OMAPEntry, len(entries)), More: more}
	for index, entry := range entries {
		page.Values[index] = OMAPEntry{Key: append([]byte(nil), entry.Key...), Value: append([]byte(nil), entry.Value...)}
	}
	return page, nil
}

func (object ObjectRef) GetOMAPHeader(ctx context.Context) ([]byte, error) {
	result, err := object.executeRead(ctx, "get OMAP header", []osd.Operation{{Code: osd.OpOmapGetHeader}})
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), result.Results[0].Data...), nil
}

func (object ObjectRef) GetOMAP(ctx context.Context, keys [][]byte) ([]OMAPEntry, error) {
	payload, err := osd.EncodeMetadataKeys(keys, math.MaxUint32)
	if err != nil {
		return nil, object.pool.client.wrapError("get OMAP", object.safeTarget(), err)
	}
	result, err := object.executeRead(ctx, "get OMAP", []osd.Operation{{Code: osd.OpOmapGetValuesByKeys, Length: uint64(len(payload)), Data: payload}})
	if err != nil {
		return nil, err
	}
	data := result.Results[0].Data
	entries, err := osd.DecodeMetadataMap(data, decodeLimit(data), uint32(len(keys)))
	if err != nil {
		return nil, object.pool.client.wrapError("get OMAP", object.safeTarget(), err)
	}
	values := make([]OMAPEntry, len(entries))
	for index, entry := range entries {
		values[index] = OMAPEntry{Key: append([]byte(nil), entry.Key...), Value: append([]byte(nil), entry.Value...)}
	}
	return values, nil
}

func (object ObjectRef) Exec(ctx context.Context, class, method string, input []byte) (ClassResult, error) {
	operation, err := classOperation(class, method, input)
	if err != nil {
		return ClassResult{}, object.invalidOperationWith("execute class", err)
	}
	result, executeErr := object.executeRead(ctx, "execute class", []osd.Operation{operation})
	var output ClassResult
	if len(result.Results) != 0 {
		output = ClassResult{Data: append([]byte(nil), result.Results[0].Data...), Code: result.Results[0].Code}
	}
	return output, executeErr
}

func (op *ReadOp) Read(offset, length uint64) int {
	return op.add(osd.Operation{Code: osd.OpRead, Offset: offset, Length: length})
}

func (op *ReadOp) Stat() int { return op.add(osd.Operation{Code: osd.OpStat}) }

func (op *ReadOp) AssertExists() { op.add(osd.Operation{Code: osd.OpStat}) }

func (op *ReadOp) AssertVersion(version uint64) {
	op.add(osd.Operation{Code: osd.OpAssertVer, AssertVersion: version})
}

func (op *ReadOp) GetXAttr(name string) int {
	if !validXAttrName(name) || len(name) > math.MaxUint32 {
		return op.fail(wire.ErrMalformed)
	}
	return op.add(osd.Operation{Code: osd.OpGetXattr, XattrNameLength: uint32(len(name)), Data: []byte(name)})
}

func (op *ReadOp) GetOMAPHeader() int {
	return op.add(osd.Operation{Code: osd.OpOmapGetHeader})
}

func (op *ReadOp) ListOMAP(after string, limit uint64) int {
	if limit == 0 {
		return op.fail(wire.ErrMalformed)
	}
	payload, err := osd.EncodeOMAPListRequest([]byte(after), limit, math.MaxUint32)
	if err != nil {
		return op.fail(err)
	}
	return op.add(osd.Operation{Code: osd.OpOmapGetValues, Length: uint64(len(payload)), Data: payload})
}

func (op *ReadOp) Exec(class, method string, input []byte) int {
	operation, err := classOperation(class, method, input)
	if err != nil {
		return op.fail(err)
	}
	return op.add(operation)
}

func (op *ReadOp) SetFlags(index int, flags SubOpFlags) {
	op.setFlags(index, flags)
}

func (op *WriteOp) Create(exclusive bool) {
	flags := uint32(0)
	if exclusive {
		flags = osd.OpFlagExclusive
	}
	op.add(osd.Operation{Code: osd.OpCreate, Flags: flags})
}

func (op *WriteOp) Write(offset uint64, data []byte) {
	op.add(osd.Operation{Code: osd.OpWrite, Offset: offset, Length: uint64(len(data)), Data: append([]byte(nil), data...)})
}

func (op *WriteOp) WriteFull(data []byte) {
	op.add(osd.Operation{Code: osd.OpWriteFull, Length: uint64(len(data)), Data: append([]byte(nil), data...)})
}

func (op *WriteOp) Append(data []byte) {
	op.add(osd.Operation{Code: osd.OpAppend, Length: uint64(len(data)), Data: append([]byte(nil), data...)})
}

func (op *WriteOp) Truncate(size uint64) {
	op.add(osd.Operation{Code: osd.OpTruncate, Offset: size})
}

func (op *WriteOp) Zero(offset, length uint64) {
	op.add(osd.Operation{Code: osd.OpZero, Offset: offset, Length: length})
}

func (op *WriteOp) Remove() { op.add(osd.Operation{Code: osd.OpDelete}) }

func (op *WriteOp) AssertVersion(version uint64) {
	op.add(osd.Operation{Code: osd.OpAssertVer, AssertVersion: version})
}

func (op *WriteOp) CompareExtent(offset uint64, data []byte) int {
	return op.add(osd.Operation{Code: osd.OpCompareExtent, Offset: offset, Length: uint64(len(data)), Data: append([]byte(nil), data...)})
}

func (op *WriteOp) SetXAttr(name string, value []byte) {
	if !validXAttrName(name) || len(name) > math.MaxUint32 || len(value) > math.MaxUint32 || uint64(len(name))+uint64(len(value)) > math.MaxUint32 {
		op.fail(wire.ErrMalformed)
		return
	}
	data := append([]byte(name), value...)
	op.add(osd.Operation{Code: osd.OpSetXattr, XattrNameLength: uint32(len(name)), XattrValueLength: uint32(len(value)), Data: data})
}

func (op *WriteOp) RemoveXAttr(name string) {
	if !validXAttrName(name) || len(name) > math.MaxUint32 {
		op.fail(wire.ErrMalformed)
		return
	}
	op.add(osd.Operation{Code: osd.OpRemoveXattr, XattrNameLength: uint32(len(name)), Data: []byte(name)})
}

func (op *WriteOp) SetOMAP(values []OMAPEntry) {
	entries := make([]osd.MetadataEntry, len(values))
	for index, value := range values {
		entries[index] = osd.MetadataEntry{Key: append([]byte(nil), value.Key...), Value: append([]byte(nil), value.Value...)}
	}
	payload, err := osd.EncodeMetadataMap(entries, math.MaxUint32)
	if err != nil {
		op.fail(err)
		return
	}
	op.add(osd.Operation{Code: osd.OpOmapSetValues, Length: uint64(len(payload)), Data: payload})
}

func (op *WriteOp) RemoveOMAP(keys [][]byte) {
	payload, err := osd.EncodeMetadataKeys(keys, math.MaxUint32)
	if err != nil {
		op.fail(err)
		return
	}
	op.add(osd.Operation{Code: osd.OpOmapRemoveKeys, Length: uint64(len(payload)), Data: payload})
}

func (op *WriteOp) RemoveOMAPRange(begin, end []byte) {
	payload, err := osd.EncodeOMAPRange(begin, end, math.MaxUint32)
	if err != nil {
		op.fail(err)
		return
	}
	op.add(osd.Operation{Code: osd.OpOmapRemoveRange, Length: uint64(len(payload)), Data: payload})
}

func (op *WriteOp) ClearOMAP() { op.add(osd.Operation{Code: osd.OpOmapClear}) }

func (op *WriteOp) SetOMAPHeader(value []byte) {
	data := append([]byte(nil), value...)
	op.add(osd.Operation{Code: osd.OpOmapSetHeader, Length: uint64(len(data)), Data: data})
}

func (op *WriteOp) CompareOMAP(key, value []byte) int {
	payload, err := osd.EncodeOMAPCompare(key, value, 1, math.MaxUint32)
	if err != nil {
		return op.fail(err)
	}
	return op.add(osd.Operation{Code: osd.OpOmapCompare, Length: uint64(len(payload)), Data: payload})
}

func (op *WriteOp) Exec(class, method string, input []byte) int {
	operation, err := classOperation(class, method, input)
	if err != nil {
		return op.fail(err)
	}
	return op.add(operation)
}

func (op *WriteOp) SetFlags(index int, flags SubOpFlags) {
	op.setFlags(index, flags)
}

func (object ObjectRef) ExecuteRead(ctx context.Context, op *ReadOp) (OpResult, error) {
	if op == nil {
		return OpResult{}, object.invalidOperation("execute read")
	}
	operations, err := op.freeze()
	if err != nil || len(operations) == 0 {
		return OpResult{}, object.invalidOperationWith("execute read", err)
	}
	return object.executeRead(ctx, "execute read", operations)
}

func (object ObjectRef) ExecuteWrite(ctx context.Context, op *WriteOp) (OpResult, error) {
	if op == nil {
		return OpResult{}, object.invalidOperation("execute write")
	}
	operations, err := op.freeze()
	if err != nil || len(operations) == 0 {
		return OpResult{}, object.invalidOperationWith("execute write", err)
	}
	return object.executeWrite(ctx, "execute write", operations)
}

func (object ObjectRef) executeRead(ctx context.Context, operation string, operations []osd.Operation) (OpResult, error) {
	objects, operationCtx, cancel, err := object.begin(ctx)
	if err != nil {
		return OpResult{}, object.wrapBeginError(operation, err)
	}
	defer cancel()
	result, executeErr := objects.ReadOperations(operationCtx, object.target(), operations)
	public := publicOperationResult(operation, object.safeTarget(), operations, result)
	if executeErr != nil {
		return public, object.pool.client.wrapError(operation, object.safeTarget(), executeErr)
	}
	return public, nil
}

func (object ObjectRef) executeWrite(ctx context.Context, operation string, operations []osd.Operation) (OpResult, error) {
	objects, operationCtx, cancel, err := object.begin(ctx)
	if err != nil {
		return OpResult{}, object.wrapBeginError(operation, err)
	}
	defer cancel()
	result, executeErr := objects.MutateOperations(operationCtx, object.target(), operations)
	public := publicOperationResult(operation, object.safeTarget(), operations, result)
	if executeErr != nil {
		return public, object.pool.client.wrapError(operation, object.safeTarget(), executeErr)
	}
	return public, nil
}

func publicOperationResult(operation, target string, operations []osd.Operation, result objecter.Result) OpResult {
	public := OpResult{Version: result.Version, Results: make([]SubOpResult, len(result.Operations))}
	for index, item := range result.Operations {
		public.Results[index].Data = append([]byte(nil), item.Data...)
		public.Results[index].Code = item.Code
		if item.Code < 0 {
			public.Results[index].Err = &OpError{Op: operation, Target: target, Code: item.Code, Err: publicErrorFor(protocol.WireErrno(item.Code).Class())}
		}
		if item.Code > 0 {
			public.Results[index].Value = uint64(item.Code)
		}
		if item.Code <= -maxErrno {
			public.Results[index].Value = uint64(-maxErrno - item.Code)
		} else if index < len(operations) && operations[index].Code == osd.OpStat && len(item.Data) == 16 {
			public.Results[index].Value = binary.LittleEndian.Uint64(item.Data[:8])
		}
	}
	return public
}

func (op *ReadOp) add(operation osd.Operation) int {
	op.mu.Lock()
	defer op.mu.Unlock()
	index := len(op.operations)
	if op.executed {
		op.err = errors.Join(op.err, wire.ErrMalformed)
		return -1
	}
	op.operations = append(op.operations, operation)
	return index
}

func (op *ReadOp) fail(err error) int {
	op.mu.Lock()
	defer op.mu.Unlock()
	op.err = errors.Join(op.err, err)
	return -1
}

func (op *ReadOp) setFlags(index int, flags SubOpFlags) {
	op.mu.Lock()
	defer op.mu.Unlock()
	if op.executed || index < 0 || index >= len(op.operations) || flags&^SubOpFlagFailOK != 0 {
		op.err = errors.Join(op.err, wire.ErrMalformed)
		return
	}
	op.operations[index].Flags = op.operations[index].Flags&osd.OpFlagExclusive | uint32(flags)
}

func (op *ReadOp) freeze() ([]osd.Operation, error) {
	op.mu.Lock()
	defer op.mu.Unlock()
	if op.executed {
		return nil, wire.ErrMalformed
	}
	op.executed = true
	return cloneOperations(op.operations), op.err
}

func (op *WriteOp) add(operation osd.Operation) int {
	op.mu.Lock()
	defer op.mu.Unlock()
	index := len(op.operations)
	if op.executed {
		op.err = errors.Join(op.err, wire.ErrMalformed)
		return -1
	}
	op.operations = append(op.operations, operation)
	return index
}

func (op *WriteOp) fail(err error) int {
	op.mu.Lock()
	defer op.mu.Unlock()
	op.err = errors.Join(op.err, err)
	return -1
}

func (op *WriteOp) setFlags(index int, flags SubOpFlags) {
	op.mu.Lock()
	defer op.mu.Unlock()
	if op.executed || index < 0 || index >= len(op.operations) || flags&^SubOpFlagFailOK != 0 {
		op.err = errors.Join(op.err, wire.ErrMalformed)
		return
	}
	op.operations[index].Flags = op.operations[index].Flags&osd.OpFlagExclusive | uint32(flags)
}

func (op *WriteOp) freeze() ([]osd.Operation, error) {
	op.mu.Lock()
	defer op.mu.Unlock()
	if op.executed {
		return nil, wire.ErrMalformed
	}
	op.executed = true
	return cloneOperations(op.operations), op.err
}

func cloneOperations(source []osd.Operation) []osd.Operation {
	result := make([]osd.Operation, len(source))
	for index, operation := range source {
		result[index] = operation
		result[index].Data = append([]byte(nil), operation.Data...)
	}
	return result
}

func decodeLimit(data []byte) uint32 {
	if len(data) == 0 {
		return 1
	}
	return uint32(len(data))
}

func validXAttrName(name string) bool {
	return name != "" && !strings.ContainsRune(name, '\x00')
}

func classOperation(class, method string, input []byte) (osd.Operation, error) {
	if class == "" || method == "" || len(class) > math.MaxUint8 || len(method) > math.MaxUint8 || len(input) > math.MaxUint32 ||
		strings.ContainsRune(class, '\x00') || strings.ContainsRune(method, '\x00') || uint64(len(class))+uint64(len(method))+uint64(len(input)) > math.MaxUint32 {
		return osd.Operation{}, wire.ErrMalformed
	}
	data := make([]byte, 0, len(class)+len(method)+len(input))
	data = append(data, class...)
	data = append(data, method...)
	data = append(data, input...)
	return osd.Operation{
		Code: osd.OpCall, ClassNameLength: uint8(len(class)), MethodNameLength: uint8(len(method)),
		ClassInputLength: uint32(len(input)), Length: uint64(len(data)), Data: data,
	}, nil
}

func (object ObjectRef) invalidOperation(operation string) error {
	return object.invalidOperationWith(operation, nil)
}

func (object ObjectRef) invalidOperationWith(operation string, err error) error {
	if err == nil {
		err = ErrInvalidArgument
	} else {
		err = errors.Join(ErrInvalidArgument, err)
	}
	return &OpError{Op: operation, Target: object.safeTarget(), Err: err}
}
