package rados

import (
	"context"
	"fmt"
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
