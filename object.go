package rados

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/otuschhoff/go-librados/internal/objecter"
	"github.com/otuschhoff/go-librados/internal/osd"
)

type Pool struct {
	client    *Client
	id        int64
	name      string
	namespace string
	locator   string
	snapshot  uint64
}

type ObjectRef struct {
	pool Pool
	name string
}

type ObjectInfo struct {
	Size    uint64
	ModTime time.Time
	Version uint64
}

type OpResult struct {
	Version uint64
	Results []SubOpResult
}

type SubOpResult struct {
	Data  []byte
	Value uint64
	Err   error
}

type ObjectEntry struct {
	Name      string
	Namespace string
	Locator   string
}

type ObjectCursor struct {
	poolID    int64
	namespace string
	value     string
	end       bool
}

type ObjectPage struct {
	Values []ObjectEntry
	Next   ObjectCursor
	More   bool
}

const (
	objectCursorMaxBytes = uint32(1024)
	maxCursorPartitions  = uint32(1 << 20)
)

func (pool Pool) ID() int64    { return pool.id }
func (pool Pool) Name() string { return pool.name }

func (pool Pool) WithNamespace(namespace string) Pool {
	pool.namespace = namespace
	return pool
}

func (pool Pool) WithLocator(locator string) Pool {
	pool.locator = locator
	return pool
}

func (pool Pool) WithReadSnapshot(id uint64) Pool {
	pool.snapshot = id
	return pool
}

func (pool Pool) Object(name string) ObjectRef { return ObjectRef{pool: pool, name: name} }

func (pool Pool) BeginObjectCursor() ObjectCursor {
	return newObjectCursor(pool.id, pool.namespace, osd.HObject{Pool: math.MinInt64})
}

func (pool Pool) EndObjectCursor() ObjectCursor {
	return ObjectCursor{poolID: pool.id, namespace: pool.namespace, end: true}
}

func (cursor ObjectCursor) IsEnd() bool { return cursor.end }

func CompareObjectCursors(left, right ObjectCursor) (int, error) {
	if !left.valid() || !right.valid() || left.poolID != right.poolID || left.namespace != right.namespace {
		return 0, ErrInvalidArgument
	}
	if left.end || right.end {
		switch {
		case left.end == right.end:
			return 0, nil
		case left.end:
			return 1, nil
		default:
			return -1, nil
		}
	}
	leftObject, leftErr := left.hobject()
	rightObject, rightErr := right.hobject()
	if leftErr != nil || rightErr != nil {
		return 0, ErrInvalidArgument
	}
	return osd.CompareHObject(leftObject, rightObject), nil
}

func (pool Pool) SplitCursor(begin, end ObjectCursor, partitions uint32) ([]ObjectCursor, error) {
	if partitions == 0 || partitions > maxCursorPartitions || !pool.ownsCursor(begin) || !pool.ownsCursor(end) {
		return nil, ErrInvalidArgument
	}
	comparison, err := CompareObjectCursors(begin, end)
	if err != nil || comparison > 0 {
		return nil, ErrInvalidArgument
	}
	startObject, err := begin.hobject()
	if err != nil {
		return nil, ErrInvalidArgument
	}
	finishObject, err := end.hobject()
	if err != nil {
		return nil, ErrInvalidArgument
	}
	startHash := uint64(osd.ReverseBits(startObject.Hash))
	finishHash := uint64(1) << 32
	if !finishObject.IsMax() {
		finishHash = uint64(osd.ReverseBits(finishObject.Hash))
	}
	difference := finishHash - startHash
	boundaries := make([]ObjectCursor, partitions+1)
	boundaries[0] = begin
	boundaries[partitions] = end
	for index := uint32(1); index < partitions; index++ {
		reversed := startHash + difference/uint64(partitions)*uint64(index) + difference%uint64(partitions)*uint64(index)/uint64(partitions)
		if reversed >= uint64(1)<<32 {
			boundaries[index] = pool.EndObjectCursor()
			continue
		}
		boundaries[index] = newObjectCursor(pool.id, pool.namespace, osd.HObject{Snapshot: osd.NoSnap, Hash: osd.ReverseBits(uint32(reversed)), Pool: pool.id})
	}
	return boundaries, nil
}

func (pool Pool) ListObjects(ctx context.Context, after ObjectCursor, limit uint64) (ObjectPage, error) {
	return pool.ListObjectsRange(ctx, after, pool.EndObjectCursor(), limit)
}

func (pool Pool) ListObjectsRange(ctx context.Context, after, end ObjectCursor, limit uint64) (ObjectPage, error) {
	if pool.client == nil || !pool.ownsCursor(after) || !pool.ownsCursor(end) || limit == 0 || limit > uint64(math.MaxInt) {
		return ObjectPage{}, &OpError{Op: "list objects", Target: fmt.Sprintf("pool %d", pool.id), Err: ErrInvalidArgument}
	}
	comparison, err := CompareObjectCursors(after, end)
	if err != nil || comparison > 0 {
		return ObjectPage{}, &OpError{Op: "list objects", Target: fmt.Sprintf("pool %d", pool.id), Err: ErrInvalidArgument}
	}
	_, objects, err := pool.client.active()
	if err != nil {
		return ObjectPage{}, pool.client.wrapError("list objects", fmt.Sprintf("pool %d", pool.id), err)
	}
	if limit > objects.MaxEnumerationEntries() {
		return ObjectPage{}, &OpError{Op: "list objects", Target: fmt.Sprintf("pool %d", pool.id), Err: ErrInvalidArgument}
	}
	if comparison == 0 {
		return ObjectPage{Values: []ObjectEntry{}, Next: end}, nil
	}
	start, err := after.hobject()
	if err != nil {
		return ObjectPage{}, &OpError{Op: "list objects", Target: fmt.Sprintf("pool %d", pool.id), Err: errors.Join(ErrInvalidArgument, err)}
	}
	finish, err := end.hobject()
	if err != nil {
		return ObjectPage{}, &OpError{Op: "list objects", Target: fmt.Sprintf("pool %d", pool.id), Err: errors.Join(ErrInvalidArgument, err)}
	}
	operationCtx, cancel := pool.client.operationContext(ctx)
	defer cancel()
	result, err := objects.Enumerate(operationCtx, pool.id, pool.namespace, start, finish, limit)
	if err != nil {
		return ObjectPage{}, pool.client.wrapError("list objects", fmt.Sprintf("pool %d", pool.id), err)
	}
	page := ObjectPage{Values: make([]ObjectEntry, len(result.Entries))}
	for index, entry := range result.Entries {
		page.Values[index] = ObjectEntry{Name: entry.Object, Namespace: entry.Namespace, Locator: entry.Locator}
	}
	if result.Next.IsMax() {
		page.Next = pool.EndObjectCursor()
	} else {
		page.Next = newObjectCursor(pool.id, pool.namespace, result.Next)
	}
	page.More, err = cursorBefore(page.Next, end)
	if err != nil {
		return ObjectPage{}, &OpError{Op: "list objects", Target: fmt.Sprintf("pool %d", pool.id), Err: ErrInvalidArgument}
	}
	return page, nil
}

func (pool Pool) ownsCursor(cursor ObjectCursor) bool {
	return cursor.valid() && cursor.poolID == pool.id && cursor.namespace == pool.namespace
}

func cursorBefore(left, right ObjectCursor) (bool, error) {
	comparison, err := CompareObjectCursors(left, right)
	return comparison < 0, err
}

func (cursor ObjectCursor) valid() bool { return cursor.end || cursor.value != "" }

func (cursor ObjectCursor) hobject() (osd.HObject, error) {
	if cursor.end {
		return osd.HObject{Max: true}, nil
	}
	if cursor.value == "" {
		return osd.HObject{}, ErrInvalidArgument
	}
	object, err := osd.UnmarshalHObject([]byte(cursor.value), objectCursorMaxBytes)
	if err != nil || object.IsMax() || (!object.IsMin() && (object.Pool != cursor.poolID || object.Snapshot != osd.NoSnap)) {
		return osd.HObject{}, ErrInvalidArgument
	}
	return object, nil
}

func newObjectCursor(poolID int64, namespace string, object osd.HObject) ObjectCursor {
	value, err := osd.MarshalHObject(object, objectCursorMaxBytes)
	if err != nil {
		panic("fixed-size object cursor exceeded its encoding bound")
	}
	return ObjectCursor{poolID: poolID, namespace: namespace, value: string(value)}
}

func (object ObjectRef) Read(ctx context.Context, offset, length uint64) ([]byte, ObjectInfo, error) {
	objects, operationCtx, cancel, err := object.begin(ctx)
	if err != nil {
		return nil, ObjectInfo{}, object.wrapBeginError("read", err)
	}
	defer cancel()
	result, err := objects.Read(operationCtx, object.target(), offset, length)
	if err != nil {
		return nil, ObjectInfo{}, object.pool.client.wrapError("read", object.safeTarget(), err)
	}
	return result.Data, ObjectInfo{Version: result.Version}, nil
}

func (object ObjectRef) Stat(ctx context.Context) (ObjectInfo, error) {
	objects, operationCtx, cancel, err := object.begin(ctx)
	if err != nil {
		return ObjectInfo{}, object.wrapBeginError("stat", err)
	}
	defer cancel()
	result, err := objects.Stat(operationCtx, object.target())
	if err != nil {
		return ObjectInfo{}, object.pool.client.wrapError("stat", object.safeTarget(), err)
	}
	return ObjectInfo{Size: result.Size, ModTime: result.ModificationTime, Version: result.Version}, nil
}

func (object ObjectRef) Write(ctx context.Context, offset uint64, data []byte) (OpResult, error) {
	return object.mutate(ctx, "write", osd.Operation{Code: osd.OpWrite, Offset: offset, Length: uint64(len(data)), Data: data})
}

func (object ObjectRef) WriteFull(ctx context.Context, data []byte) (OpResult, error) {
	return object.mutate(ctx, "write full", osd.Operation{Code: osd.OpWriteFull, Length: uint64(len(data)), Data: data})
}

func (object ObjectRef) Append(ctx context.Context, data []byte) (OpResult, error) {
	return object.mutate(ctx, "append", osd.Operation{Code: osd.OpAppend, Length: uint64(len(data)), Data: data})
}

func (object ObjectRef) Truncate(ctx context.Context, size uint64) (OpResult, error) {
	return object.mutate(ctx, "truncate", osd.Operation{Code: osd.OpTruncate, Offset: size})
}

func (object ObjectRef) Zero(ctx context.Context, offset, length uint64) (OpResult, error) {
	return object.mutate(ctx, "zero", osd.Operation{Code: osd.OpZero, Offset: offset, Length: length})
}

func (object ObjectRef) Remove(ctx context.Context) (OpResult, error) {
	return object.mutate(ctx, "remove", osd.Operation{Code: osd.OpDelete})
}

func (object ObjectRef) Create(ctx context.Context, exclusive bool) (OpResult, error) {
	operation := osd.Operation{Code: osd.OpCreate}
	if exclusive {
		operation.Flags = osd.OpFlagExclusive
	}
	return object.mutate(ctx, "create", operation)
}

func (object ObjectRef) mutate(ctx context.Context, operation string, request osd.Operation) (OpResult, error) {
	objects, operationCtx, cancel, err := object.begin(ctx)
	if err != nil {
		return OpResult{}, object.wrapBeginError(operation, err)
	}
	defer cancel()
	result, err := objects.Mutate(operationCtx, object.target(), request)
	if err != nil {
		return OpResult{}, object.pool.client.wrapError(operation, object.safeTarget(), err)
	}
	return OpResult{Version: result.Version}, nil
}

func (object ObjectRef) begin(ctx context.Context) (*objecter.Client, context.Context, context.CancelFunc, error) {
	if object.pool.client == nil {
		return nil, nil, nil, &OpError{Err: ErrInvalidArgument}
	}
	_, objects, err := object.pool.client.active()
	if err != nil {
		return nil, nil, nil, err
	}
	operationCtx, cancel := object.pool.client.operationContext(ctx)
	return objects, operationCtx, cancel, nil
}

func (object ObjectRef) target() objecter.Target {
	return objecter.Target{PoolID: object.pool.id, Object: object.name, Locator: object.pool.locator, Namespace: object.pool.namespace, Snapshot: object.pool.snapshot}
}

func (object ObjectRef) safeTarget() string { return fmt.Sprintf("pool %d object", object.pool.id) }

func (object ObjectRef) wrapBeginError(operation string, err error) error {
	if object.pool.client == nil {
		return &OpError{Op: operation, Target: object.safeTarget(), Err: ErrInvalidArgument}
	}
	return object.pool.client.wrapError(operation, object.safeTarget(), err)
}
