package rados

import (
	"context"
	"fmt"
	"time"

	"github.com/otuschhoff/go-librados/internal/maps"
	"github.com/otuschhoff/go-librados/internal/mon"
	"github.com/otuschhoff/go-librados/internal/osd"
)

type Snapshot struct {
	ID        uint64
	Name      string
	CreatedAt time.Time
}

func (pool Pool) CreateSnapshot(ctx context.Context, name string) error {
	if name == "" {
		return pool.invalidSnapshotOperation("create snapshot")
	}
	_, err := pool.applySnapshotOperation(ctx, "create snapshot", mon.PoolOperationCreateSnapshot, 0, name)
	return err
}

func (pool Pool) RemoveSnapshot(ctx context.Context, name string) error {
	if name == "" {
		return pool.invalidSnapshotOperation("remove snapshot")
	}
	_, err := pool.applySnapshotOperation(ctx, "remove snapshot", mon.PoolOperationDeleteSnapshot, 0, name)
	return err
}

func (pool Pool) CreateSelfManagedSnapshot(ctx context.Context) (uint64, error) {
	reply, err := pool.applySnapshotOperation(ctx, "create self-managed snapshot", mon.PoolOperationCreateSelfManaged, 0, "")
	if err != nil {
		return 0, err
	}
	snapshotID, err := mon.DecodeAllocatedSnapshotID(reply.ResponseData, 8)
	if err != nil {
		return 0, pool.client.wrapError("create self-managed snapshot", pool.safeTarget(), err)
	}
	return snapshotID, nil
}

func (pool Pool) RemoveSelfManagedSnapshot(ctx context.Context, snapshotID uint64) error {
	if snapshotID == 0 {
		return pool.invalidSnapshotOperation("remove self-managed snapshot")
	}
	_, err := pool.applySnapshotOperation(ctx, "remove self-managed snapshot", mon.PoolOperationDeleteSelfManaged, snapshotID, "")
	return err
}

func (pool Pool) ListSnapshots(ctx context.Context) ([]Snapshot, error) {
	metadata, done, err := pool.metadata(ctx, "list snapshots")
	if err != nil {
		return nil, err
	}
	defer done()
	snapshots := metadata.Snapshots()
	result := make([]Snapshot, len(snapshots))
	for index, snapshot := range snapshots {
		result[index] = publicSnapshot(snapshot)
	}
	return result, nil
}

func (pool Pool) LookupSnapshot(ctx context.Context, name string) (Snapshot, error) {
	if name == "" {
		return Snapshot{}, pool.invalidSnapshotOperation("lookup snapshot")
	}
	metadata, done, err := pool.metadata(ctx, "lookup snapshot")
	if err != nil {
		return Snapshot{}, err
	}
	defer done()
	for _, snapshot := range metadata.Snapshots() {
		if snapshot.Name == name {
			return publicSnapshot(snapshot), nil
		}
	}
	return Snapshot{}, &OpError{Op: "lookup snapshot", Target: pool.safeTarget(), Code: -2}
}

func (pool Pool) UsesSelfManagedSnapshots(ctx context.Context) (bool, error) {
	metadata, done, err := pool.metadata(ctx, "get snapshot mode")
	if err != nil {
		return false, err
	}
	defer done()
	return metadata.UsesSelfManagedSnapshots(), nil
}

func (pool Pool) IsErasureCoded(ctx context.Context) (bool, error) {
	metadata, done, err := pool.metadata(ctx, "get erasure coding mode")
	if err != nil {
		return false, err
	}
	defer done()
	return metadata.IsErasureCoded(), nil
}

func (pool Pool) RequiresAlignment(ctx context.Context) (bool, error) {
	metadata, done, err := pool.metadata(ctx, "get alignment requirement")
	if err != nil {
		return false, err
	}
	defer done()
	return metadata.RequiresAlignment(), nil
}

func (pool Pool) RequiredAlignment(ctx context.Context) (uint64, error) {
	metadata, done, err := pool.metadata(ctx, "get required alignment")
	if err != nil {
		return 0, err
	}
	defer done()
	return uint64(metadata.StripeWidth()), nil
}

func (object ObjectRef) RollbackToSnapshot(ctx context.Context, name string) (OpResult, error) {
	snapshot, err := object.pool.LookupSnapshot(ctx, name)
	if err != nil {
		return OpResult{}, err
	}
	return object.rollback(ctx, "rollback snapshot", snapshot.ID)
}

func (object ObjectRef) RollbackToSelfManagedSnapshot(ctx context.Context, snapshotID uint64) (OpResult, error) {
	if snapshotID == 0 {
		return OpResult{}, object.invalidOperation("rollback self-managed snapshot")
	}
	return object.rollback(ctx, "rollback self-managed snapshot", snapshotID)
}

func (object ObjectRef) rollback(ctx context.Context, operation string, snapshotID uint64) (OpResult, error) {
	return object.mutate(ctx, operation, osd.Operation{Code: osd.OpRollback, SnapshotID: snapshotID})
}

func (pool Pool) applySnapshotOperation(ctx context.Context, operation string, code mon.PoolOperation, snapshotID uint64, name string) (mon.PoolOperationReply, error) {
	if pool.client == nil {
		return mon.PoolOperationReply{}, pool.invalidSnapshotOperation(operation)
	}
	monitorClient, _, done, err := pool.client.beginOperation()
	if err != nil {
		return mon.PoolOperationReply{}, pool.client.wrapError(operation, pool.safeTarget(), err)
	}
	defer done()
	operationCtx, cancel := pool.client.operationContext(ctx)
	defer cancel()
	reply, err := monitorClient.ApplyPoolOperation(operationCtx, pool.id, code, snapshotID, name)
	if err != nil {
		return reply, pool.client.wrapError(operation, pool.safeTarget(), err)
	}
	return reply, nil
}

func (pool Pool) metadata(ctx context.Context, operation string) (maps.Pool, func(), error) {
	if pool.client == nil {
		return maps.Pool{}, nil, pool.invalidSnapshotOperation(operation)
	}
	monitorClient, _, done, err := pool.client.beginOperation()
	if err != nil {
		return maps.Pool{}, nil, pool.client.wrapError(operation, pool.safeTarget(), err)
	}
	operationCtx, cancel := pool.client.operationContext(ctx)
	select {
	case <-operationCtx.Done():
		cancel()
		done()
		return maps.Pool{}, nil, pool.client.wrapError(operation, pool.safeTarget(), operationCtx.Err())
	default:
	}
	metadata, ok := monitorClient.PoolByName(pool.name)
	if !ok || metadata.ID() != pool.id {
		cancel()
		done()
		return maps.Pool{}, nil, &OpError{Op: operation, Target: pool.safeTarget(), Code: -2}
	}
	return metadata, func() { cancel(); done() }, nil
}

func publicSnapshot(snapshot maps.PoolSnapshot) Snapshot {
	return Snapshot{ID: snapshot.ID, Name: snapshot.Name, CreatedAt: time.Unix(int64(snapshot.Timestamp.Seconds), int64(snapshot.Timestamp.Nanoseconds))}
}

func (pool Pool) invalidSnapshotOperation(operation string) error {
	return &OpError{Op: operation, Target: pool.safeTarget(), Err: ErrInvalidArgument}
}

func (pool Pool) safeTarget() string { return fmt.Sprintf("pool %d", pool.id) }
