package rados

import (
	"context"
	"errors"
	"testing"
)

func TestReadIntoAdmissionFailurePreservesDestination(t *testing.T) {
	for _, object := range []ObjectRef{{}, {pool: Pool{client: &Client{closing: true}}}} {
		destination := []byte("canary")
		count, info, err := object.ReadInto(context.Background(), 0, destination)
		if err == nil || count != 0 || info != (ObjectInfo{}) || string(destination) != "canary" {
			t.Fatalf("count=%d info=%+v destination=%q err=%v", count, info, destination, err)
		}
		var operation *OpError
		if !errors.As(err, &operation) || operation.Op != "read into" {
			t.Fatalf("missing operation context: %v", err)
		}
	}
}
