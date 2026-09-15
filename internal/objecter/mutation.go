package objecter

import (
	"context"
	"errors"
	"math"

	wire "github.com/otuschhoff/go-librados/internal/encoding"
	"github.com/otuschhoff/go-librados/internal/msgr"
	"github.com/otuschhoff/go-librados/internal/osd"
)

func (client *Client) Mutate(ctx context.Context, target Target, operation osd.Operation) (Result, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if target.Snapshot != osd.NoSnap || !isMutationOperation(operation.Code) || operation.Length > math.MaxUint64-operation.Offset {
		return Result{}, wire.ErrMalformed
	}
	if operation.Code == osd.OpWrite || operation.Code == osd.OpWriteFull || operation.Code == osd.OpAppend {
		if uint64(len(operation.Data)) != operation.Length {
			return Result{}, wire.ErrMalformed
		}
	} else if len(operation.Data) != 0 {
		return Result{}, wire.ErrMalformed
	}
	sequence, transactionID, owned, err := client.admitMutation(ctx, operation)
	if err != nil {
		return Result{}, err
	}
	result, mutationErr := client.executeOperation(ctx, target, owned, transactionID, true)
	client.completeMutation(sequence, mutationErr)
	return result, mutationErr
}

func isMutationOperation(code uint16) bool {
	switch code {
	case osd.OpWrite, osd.OpWriteFull, osd.OpAppend, osd.OpTruncate, osd.OpZero, osd.OpDelete, osd.OpCreate:
		return true
	default:
		return false
	}
}

func (client *Client) admitMutation(ctx context.Context, operation osd.Operation) (uint64, uint64, osd.Operation, error) {
	bytes := uint64(len(operation.Data))
	if bytes > client.config.MaxMutationBytes {
		return 0, 0, osd.Operation{}, wire.ErrLimitExceeded
	}
	for {
		client.mu.Lock()
		if client.closed || client.mutationClosed {
			client.mu.Unlock()
			return 0, 0, osd.Operation{}, ErrClosed
		}
		if len(client.pendingMutations) < client.config.MaxMutations && bytes <= client.config.MaxMutationBytes-client.retainedMutationBytes {
			if client.nextMutation == math.MaxUint64 || client.nextTransaction == math.MaxUint64 {
				client.mu.Unlock()
				return 0, 0, osd.Operation{}, wire.ErrLimitExceeded
			}
			client.nextMutation++
			sequence := client.nextMutation
			transactionID := client.takeTransactionIDLocked()
			operation.Data = append([]byte(nil), operation.Data...)
			operation.PayloadLength = uint32(len(operation.Data))
			client.pendingMutations[sequence] = bytes
			client.retainedMutationBytes += bytes
			client.mu.Unlock()
			return sequence, transactionID, operation, nil
		}
		changed := client.mutationChanged
		client.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return 0, 0, osd.Operation{}, ctx.Err()
		}
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
