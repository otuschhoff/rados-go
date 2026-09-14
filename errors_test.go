package rados

import (
	"context"
	"errors"
	"testing"
)

func TestOpErrorMatchesWireClassification(t *testing.T) {
	tests := []struct {
		code int32
		want error
	}{
		{-2, ErrNotFound},
		{-17, ErrExists},
		{-13, ErrPermission},
		{-95, ErrUnsupported},
		{-22, ErrInvalidArgument},
		{-28, ErrQuotaOrFull},
		{-16, ErrConflict},
		{-110, ErrTimeout},
		{-125, ErrCanceled},
	}
	for _, test := range tests {
		err := &OpError{Op: "read", Target: "pool/object", Code: test.code}
		if !errors.Is(err, test.want) {
			t.Fatalf("code %d does not match %v", test.code, test.want)
		}
		var operationError *OpError
		if !errors.As(err, &operationError) || operationError.Code != test.code {
			t.Fatalf("errors.As failed for code %d", test.code)
		}
	}
}

func TestOpErrorPreservesCause(t *testing.T) {
	err := &OpError{Op: "write", Err: errors.Join(ErrOutcomeUnknown, context.DeadlineExceeded)}
	if !errors.Is(err, ErrOutcomeUnknown) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error does not preserve causes: %v", err)
	}
}
