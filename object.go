package rados

import (
	"context"
	"fmt"
	"time"

	"github.com/otuschhoff/go-librados/internal/objecter"
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
