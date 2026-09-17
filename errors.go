package rados

import (
	"errors"
	"fmt"

	"github.com/otuschhoff/rados-go/internal/protocol"
)

var (
	ErrNotFound         = errors.New("rados: not found")
	ErrExists           = errors.New("rados: already exists")
	ErrPermission       = errors.New("rados: permission denied")
	ErrUnsupported      = errors.New("rados: unsupported")
	ErrInvalidArgument  = errors.New("rados: invalid argument")
	ErrQuotaOrFull      = errors.New("rados: quota exceeded or cluster full")
	ErrConflict         = errors.New("rados: conflict")
	ErrTimeout          = errors.New("rados: timeout")
	ErrCanceled         = errors.New("rados: canceled")
	ErrClosed           = errors.New("rados: closed")
	ErrOutcomeUnknown   = errors.New("rados: outcome unknown")
	ErrWatchInterrupted = errors.New("rados: watch interrupted; events may have been lost")
)

// OpError describes a failed RADOS operation. Code is the signed Linux errno
// received from Ceph and remains zero when no wire result was received.
type OpError struct {
	Op     string
	Target string
	Code   int32
	Err    error
}

func (e *OpError) Error() string {
	if e == nil {
		return "<nil>"
	}
	description := "rados operation failed"
	if e.Op != "" {
		description = "rados " + e.Op + " failed"
	}
	if e.Target != "" {
		description += " for " + e.Target
	}
	if e.Code != 0 {
		description += fmt.Sprintf(" (Ceph errno %d)", e.Code)
	}
	if e.Err != nil {
		description += ": " + e.Err.Error()
	}
	return description
}

func (e *OpError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func (e *OpError) Is(target error) bool {
	if e == nil {
		return false
	}
	if errors.Is(e.Err, target) {
		return true
	}
	return target == publicErrorFor(protocol.WireErrno(e.Code).Class())
}

func publicErrorFor(class protocol.ErrorClass) error {
	switch class {
	case protocol.ErrorNotFound:
		return ErrNotFound
	case protocol.ErrorExists:
		return ErrExists
	case protocol.ErrorPermission:
		return ErrPermission
	case protocol.ErrorUnsupported:
		return ErrUnsupported
	case protocol.ErrorInvalid:
		return ErrInvalidArgument
	case protocol.ErrorQuotaOrFull:
		return ErrQuotaOrFull
	case protocol.ErrorConflict:
		return ErrConflict
	case protocol.ErrorTimeout:
		return ErrTimeout
	case protocol.ErrorCanceled:
		return ErrCanceled
	default:
		return nil
	}
}
