package objecter

import (
	"context"
	"errors"
	"math"
	"sync/atomic"

	wire "github.com/otuschhoff/rados-go/internal/encoding"
	"github.com/otuschhoff/rados-go/internal/msgr"
	"github.com/otuschhoff/rados-go/internal/osd"
)

func (client *Client) Mutate(ctx context.Context, target Target, operation osd.Operation) (Result, error) {
	return client.MutateOperations(ctx, target, []osd.Operation{operation})
}

func (client *Client) MutateOperations(ctx context.Context, target Target, operations []osd.Operation) (Result, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if target.Snapshot != osd.NoSnap || len(operations) == 0 || uint64(len(operations)) > uint64(client.config.MessageLimits.MaxOperations) {
		return Result{}, wire.ErrMalformed
	}
	mutation := false
	for _, operation := range operations {
		if operation.Length > math.MaxUint64-operation.Offset || !isCompoundWriteOperation(operation.Code) || !validCompoundPayload(operation) {
			return Result{}, wire.ErrMalformed
		}
		mutation = mutation || isMutationOperation(operation.Code)
	}
	if !mutation {
		return Result{}, wire.ErrMalformed
	}
	sequence, transactionID, owned, lease, err := client.admitMutationOperationsWithLease(ctx, operations, true)
	if err != nil {
		return Result{}, err
	}
	if lease != nil {
		defer lease.Release()
	}
	result, mutationErr := client.executeRoutedResultInto(ctx, target, owned, transactionID, true, true, osd.FlagWrite|osd.FlagOnDisk, client.config.Router.Route, true, nil, false, true, lease)
	client.completeMutation(sequence, mutationErr)
	return result, mutationErr
}

func (client *Client) ClassOperations(ctx context.Context, target Target, operations []osd.Operation) (Result, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if target.Snapshot != osd.NoSnap || len(operations) == 0 || uint64(len(operations)) > uint64(client.config.MessageLimits.MaxOperations) {
		return Result{}, wire.ErrMalformed
	}
	classCall := false
	for _, operation := range operations {
		if !isReadOperation(operation.Code) && operation.Code != osd.OpCall {
			return Result{}, wire.ErrMalformed
		}
		if operation.Code == osd.OpCall && !validCompoundPayload(operation) {
			return Result{}, wire.ErrMalformed
		}
		classCall = classCall || operation.Code == osd.OpCall
	}
	if !classCall {
		return Result{}, wire.ErrMalformed
	}
	sequence, transactionID, owned, err := client.admitMutationOperations(ctx, operations)
	if err != nil {
		return Result{}, err
	}
	result, classErr := client.executeRoutedOperationsWithOwnership(ctx, target, owned, transactionID, true, false, 0, client.config.Router.Route, true)
	client.completeMutation(sequence, classErr)
	return result, classErr
}

func validCompoundPayload(operation osd.Operation) bool {
	switch operation.Code {
	case osd.OpWrite, osd.OpWriteFull, osd.OpAppend, osd.OpCompareExtent,
		osd.OpOmapSetValues, osd.OpOmapSetHeader, osd.OpOmapRemoveKeys, osd.OpOmapRemoveRange, osd.OpOmapCompare, osd.OpCall,
		osd.OpCopyFrom, osd.OpCopyFrom2:
		return uint64(len(operation.Data)) == operation.Length
	case osd.OpWriteSame:
		return operation.PatternLength != 0 && operation.PatternLength == uint64(len(operation.Data)) && operation.Length != 0 && operation.Length%operation.PatternLength == 0
	case osd.OpSetXattr, osd.OpCompareXattr:
		return uint64(operation.XattrNameLength)+uint64(operation.XattrValueLength) == uint64(len(operation.Data))
	case osd.OpRemoveXattr:
		return operation.XattrValueLength == 0 && uint64(operation.XattrNameLength) == uint64(len(operation.Data))
	default:
		return len(operation.Data) == 0
	}
}

func isMutationOperation(code uint16) bool {
	switch code {
	case osd.OpWrite, osd.OpWriteFull, osd.OpAppend, osd.OpTruncate, osd.OpZero, osd.OpDelete, osd.OpCreate,
		osd.OpRollback, osd.OpCopyFrom, osd.OpCopyFrom2, osd.OpSetAllocationHint, osd.OpWriteSame,
		osd.OpOmapSetValues, osd.OpOmapSetHeader, osd.OpOmapClear, osd.OpOmapRemoveKeys, osd.OpOmapRemoveRange,
		osd.OpSetXattr, osd.OpRemoveXattr, osd.OpCall, osd.OpWatch:
		return true
	default:
		return false
	}
}

func isCompoundWriteOperation(code uint16) bool {
	if isMutationOperation(code) {
		return true
	}
	switch code {
	case osd.OpAssertVer, osd.OpCompareExtent, osd.OpOmapCompare, osd.OpCompareXattr:
		return true
	default:
		return false
	}
}

func (client *Client) admitMutation(ctx context.Context, operation osd.Operation) (uint64, uint64, osd.Operation, error) {
	sequence, transactionID, operations, err := client.admitMutationOperations(ctx, []osd.Operation{operation})
	if err != nil {
		return 0, 0, osd.Operation{}, err
	}
	return sequence, transactionID, operations[0], nil
}

func (client *Client) admitMutationOperations(ctx context.Context, operations []osd.Operation) (uint64, uint64, []osd.Operation, error) {
	sequence, transactionID, owned, _, err := client.admitMutationOperationsWithLease(ctx, operations, false)
	return sequence, transactionID, owned, err
}

func (client *Client) admitMutationOperationsWithLease(ctx context.Context, operations []osd.Operation, reusable bool) (uint64, uint64, []osd.Operation, *msgr.MessageLease, error) {
	bytes := uint64(0)
	for _, operation := range operations {
		if uint64(len(operation.Data)) > math.MaxUint64-bytes {
			return 0, 0, nil, nil, wire.ErrLimitExceeded
		}
		bytes += uint64(len(operation.Data))
	}
	if bytes > client.config.MaxMutationBytes {
		return 0, 0, nil, nil, wire.ErrLimitExceeded
	}
	for {
		client.mu.Lock()
		if client.closed || client.mutationClosed {
			client.mu.Unlock()
			return 0, 0, nil, nil, ErrClosed
		}
		if len(client.pendingMutations) < client.config.MaxMutations && bytes <= client.config.MaxMutationBytes-client.retainedMutationBytes {
			if client.nextMutation == math.MaxUint64 || client.nextTransaction == math.MaxUint64 {
				client.mu.Unlock()
				return 0, 0, nil, nil, wire.ErrLimitExceeded
			}
			client.nextMutation++
			sequence := client.nextMutation
			transactionID := client.takeTransactionIDLocked()
			owned := make([]osd.Operation, len(operations))
			var lease *msgr.MessageLease
			for index, operation := range operations {
				owned[index] = operation
				var buffer []byte
				cache := client.mutationBuffers
				if reusable && len(operations) == 1 && cache != nil {
					buffer = cache.take(len(operation.Data))
				}
				if buffer != nil {
					if len(buffer) == msgr.LeasedPayloadPrefix+len(operation.Data)+msgr.LeasedPayloadPadding {
						payload := buffer[msgr.LeasedPayloadPrefix : msgr.LeasedPayloadPrefix+len(operation.Data)]
						copy(payload, operation.Data)
						owned[index].Data = payload
						lease = msgr.NewPaddedMessageLease(buffer, payload, func() { cache.put(buffer) })
					} else {
						copy(buffer, operation.Data)
						owned[index].Data = buffer
						lease = msgr.NewMessageLease(func() { cache.put(buffer) })
					}
				} else {
					owned[index].Data = append([]byte(nil), operation.Data...)
				}
				owned[index].PayloadLength = uint32(len(operation.Data))
			}
			client.pendingMutations[sequence] = bytes
			client.retainedMutationBytes += bytes
			client.mu.Unlock()
			return sequence, transactionID, owned, lease, nil
		}
		changed := client.mutationChanged
		client.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return 0, 0, nil, nil, ctx.Err()
		}
	}
}

const mutationBufferCacheLimit = 8 << 20

type mutationBufferCache struct {
	closed atomic.Bool
	bytes  atomic.Int64
	small  chan []byte
	medium chan []byte
	large  chan []byte
	huge   chan []byte
}

func newMutationBufferCache() *mutationBufferCache {
	return &mutationBufferCache{
		small: make(chan []byte, 8), medium: make(chan []byte, 8),
		large: make(chan []byte, 8), huge: make(chan []byte, 2),
	}
}

func (cache *mutationBufferCache) bucket(size int) chan []byte {
	switch size {
	case 4096:
		return cache.small
	case 65536:
		return cache.medium
	case 1048576:
		return cache.large
	case 4194304:
		return cache.huge
	default:
		return nil
	}
}

func (cache *mutationBufferCache) take(size int) []byte {
	bucket := cache.bucket(size)
	if bucket == nil || cache.closed.Load() {
		return nil
	}
	select {
	case buffer := <-bucket:
		cache.bytes.Add(-int64(cap(buffer)))
		return buffer
	default:
		if size == 4194304 {
			return make([]byte, size)
		}
		return make([]byte, msgr.LeasedPayloadPrefix+size+msgr.LeasedPayloadPadding)
	}
}

func (cache *mutationBufferCache) put(buffer []byte) {
	bucket := cache.bucket(len(buffer))
	if bucket == nil {
		bucket = cache.bucket(len(buffer) - msgr.LeasedPayloadPrefix - msgr.LeasedPayloadPadding)
	}
	if bucket == nil || cap(buffer) != len(buffer) || cache.closed.Load() {
		return
	}
	reserved := false
	for attempt := 0; attempt < 8; attempt++ {
		retained := cache.bytes.Load()
		if retained+int64(cap(buffer)) > mutationBufferCacheLimit {
			return
		}
		if cache.bytes.CompareAndSwap(retained, retained+int64(cap(buffer))) {
			reserved = true
			break
		}
	}
	if !reserved {
		return
	}
	select {
	case bucket <- buffer:
	default:
		cache.bytes.Add(-int64(cap(buffer)))
	}
}

func (client *Client) completeMutation(sequence uint64, err error) {
	client.mu.Lock()
	bytes, ok := client.pendingMutations[sequence]
	if ok {
		delete(client.pendingMutations, sequence)
		client.retainedMutationBytes -= bytes
		if errors.Is(err, msgr.ErrOutcomeUnknown) && (client.unknownMutation == 0 || sequence < client.unknownMutation) {
			client.unknownMutation = sequence
		}
		client.notifyMutationWaitersLocked()
	}
	client.mu.Unlock()
}

func (client *Client) Flush(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	client.mu.Lock()
	watermark := client.nextMutation
	client.mu.Unlock()
	for {
		client.mu.Lock()
		waiting := false
		for sequence := range client.pendingMutations {
			if sequence <= watermark {
				waiting = true
				break
			}
		}
		if !waiting {
			unknown := client.unknownMutation != 0 && client.unknownMutation <= watermark
			client.mu.Unlock()
			if unknown {
				return msgr.ErrOutcomeUnknown
			}
			return nil
		}
		changed := client.mutationChanged
		client.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			client.mu.Lock()
			unknown := client.unknownMutation != 0 && client.unknownMutation <= watermark
			client.mu.Unlock()
			if unknown {
				return errors.Join(msgr.ErrOutcomeUnknown, ctx.Err())
			}
			return ctx.Err()
		}
	}
}

func (client *Client) BeginShutdown() {
	client.mu.Lock()
	if !client.mutationClosed {
		client.mutationClosed = true
		for _, completion := range client.notifies {
			select {
			case completion <- notifyCompletion{err: errors.Join(msgr.ErrOutcomeUnknown, ErrClosed)}:
			default:
			}
		}
		client.notifyMutationWaitersLocked()
	}
	client.mu.Unlock()
}

func (client *Client) takeTransactionID() (uint64, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.nextTransaction == math.MaxUint64 {
		return 0, wire.ErrLimitExceeded
	}
	return client.takeTransactionIDLocked(), nil
}

func (client *Client) takeTransactionIDLocked() uint64 {
	transactionID := client.nextTransaction
	if transactionID == 0 {
		transactionID = 1
	}
	client.nextTransaction = transactionID + 1
	return transactionID
}

func (client *Client) notifyMutationWaitersLocked() {
	close(client.mutationChanged)
	client.mutationChanged = make(chan struct{})
}
