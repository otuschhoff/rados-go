package msgr

import (
	"context"
	"fmt"

	"github.com/otuschhoff/rados-go/internal/protocol"
)

const (
	MaxControlMessages             = 16
	MaxControlRetainedBytes uint64 = 1 << 20
)

// SendControl writes an OSD backoff ACK using fixed, per-session reserves.
// It returns after transport write completion, not peer acknowledgment. The
// authenticated transport and reconnect policy are shared with Send. Completed
// writes are not replayed; callers must resubmit after reconnect. Cancellation
// after a write starts reports ErrOutcomeUnknown. Full backoff payload validation
// remains the caller's responsibility.
func (session *Session) SendControl(ctx context.Context, message Message) error {
	_, err := session.submit(ctx, message, true, true, nil)
	return err
}

func (session *Session) ControlGeneration() uint64 { return session.controlGeneration.Load() }

func (session *Session) SendControlGeneration(ctx context.Context, message Message, generation uint64) error {
	if generation == 0 {
		return ErrControlInvalidated
	}
	_, err := session.submitGeneration(ctx, message, true, true, nil, generation)
	return err
}

func (owner *sessionOwner) invalidateControls() {
	owner.releaseReceiveQueue()
	owner.session.controlGeneration.Add(1)
	for index := 0; index < len(owner.pending); {
		pending := owner.pending[index]
		if pending.request.controlGeneration != 0 {
			owner.removePending(pending)
			pending.request.result <- submitResult{err: ErrControlInvalidated}
		} else {
			index++
		}
	}
}

func validateControlMessage(message Message) error {
	if message.Header.Type != protocol.MessageOSDBackoff || message.Header.Version != 1 || message.Header.CompatVersion > 1 ||
		len(message.Front) < 29 || message.Front[28] != 2 || len(message.Middle) != 0 || len(message.Data) != 0 {
		return fmt.Errorf("%w: control lane requires a version 1 OSD backoff ACK without middle or data", ErrUnsupportedPayload)
	}
	return nil
}

func (owner *sessionOwner) nextPendingWrite() *pendingRequest {
	if owner.controlCount == 0 {
		for _, pending := range owner.pending {
			if !pending.sent {
				return pending
			}
		}
		return nil
	}
	var replay, control, application *pendingRequest
	for _, pending := range owner.pending {
		if pending.sent {
			continue
		}
		if pending.seq != 0 {
			if replay == nil || pending.seq < replay.seq {
				replay = pending
			}
		} else if pending.request.control {
			if control == nil {
				control = pending
			}
		} else if application == nil {
			application = pending
		}
	}
	if replay != nil {
		return replay
	}
	if control != nil {
		return control
	}
	return application
}
