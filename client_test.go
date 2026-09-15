package rados

import (
	"context"
	"errors"
	"testing"
	"time"

	wire "github.com/otuschhoff/go-librados/internal/encoding"
	"github.com/otuschhoff/go-librados/internal/msgr"
	"github.com/otuschhoff/go-librados/internal/osd"
)

func TestWrapErrorDistinguishesPeerAndCallerFailures(t *testing.T) {
	client := &Client{}
	peer := client.wrapError("read", "object", osd.ErrMalformedReply)
	if errors.Is(peer, ErrInvalidArgument) {
		t.Fatalf("peer corruption classified as invalid argument: %v", peer)
	}
	caller := client.wrapError("read", "object", wire.ErrMalformed)
	if !errors.Is(caller, ErrInvalidArgument) {
		t.Fatalf("caller error=%v", caller)
	}
	unsupported := client.wrapError("read", "object", msgr.ErrUnsupportedFeature)
	if !errors.Is(unsupported, ErrUnsupported) {
		t.Fatalf("unsupported error=%v", unsupported)
	}
	unknown := client.wrapError("append", "object", msgr.ErrOutcomeUnknown)
	if !errors.Is(unknown, ErrOutcomeUnknown) || !errors.Is(unknown, msgr.ErrOutcomeUnknown) {
		t.Fatalf("unknown outcome error=%v", unknown)
	}
}

func TestWrapErrorPreservesOutcomeUnknownWithTimeout(t *testing.T) {
	client := &Client{}
	err := client.wrapError("append", "object", errors.Join(msgr.ErrOutcomeUnknown, context.DeadlineExceeded))
	if !errors.Is(err, ErrOutcomeUnknown) || !errors.Is(err, ErrTimeout) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error=%v", err)
	}
}

const testPublicKey = "AQB7AAAAyAEAABAAMTIzNDU2Nzg5MDEyMzQ1Ng=="

func TestNewCopiesConfigAndAppliesFiniteTimeouts(t *testing.T) {
	config := Config{Monitors: []string{"127.0.0.1:3300"}, Entity: "client.test", Key: []byte(testPublicKey)}
	client, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	config.Monitors[0] = "changed"
	config.Key[0] = 'x'
	if client.config.Monitors[0] != "127.0.0.1:3300" || string(client.config.Key) != testPublicKey {
		t.Fatal("caller mutation changed client configuration")
	}
	if client.config.DialTimeout != 10*time.Second || client.config.HandshakeTimeout != 15*time.Second || client.config.OperationTimeout != 30*time.Second {
		t.Fatalf("timeouts=%s/%s/%s", client.config.DialTimeout, client.config.HandshakeTimeout, client.config.OperationTimeout)
	}
}

func TestNewRejectsInvalidConfiguration(t *testing.T) {
	for _, config := range []Config{
		{},
		{Monitors: []string{"127.0.0.1:3300"}, Entity: "client.test", Key: []byte("invalid")},
		{Monitors: []string{"127.0.0.1:3300"}, Entity: "client.test", Key: []byte(testPublicKey), ClusterFSID: "invalid"},
		{Monitors: []string{"127.0.0.1:3300"}, Entity: "client.test", Key: []byte(testPublicKey), OperationTimeout: -1},
	} {
		if _, err := New(config); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("config=%+v error=%v", config, err)
		}
	}
}

func TestPoolViewsAreImmutableAndPreserveSnapshotZero(t *testing.T) {
	base := Pool{id: 7, name: "data", namespace: "original", locator: "base", snapshot: ^uint64(0)}
	derived := base.WithNamespace("next").WithLocator("key").WithReadSnapshot(0)
	if base.namespace != "original" || base.locator != "base" || base.snapshot != ^uint64(0) {
		t.Fatalf("base changed=%+v", base)
	}
	object := derived.Object("name")
	target := object.target()
	if target.PoolID != 7 || target.Namespace != "next" || target.Locator != "key" || target.Snapshot != 0 || target.Object != "name" {
		t.Fatalf("target=%+v", target)
	}
}

func TestCloseIsIdempotentAndRejectsWork(t *testing.T) {
	client, err := New(Config{Monitors: []string{"127.0.0.1:3300"}, Entity: "client.test", Key: []byte(testPublicKey)})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := client.OpenPool(t.Context(), "data"); !errors.Is(err, ErrClosed) {
		t.Fatalf("error=%v", err)
	}
	object := Pool{client: client, id: 7}.Object("retained")
	_, _, readErr := object.Read(t.Context(), 0, 1)
	var readOp *OpError
	if !errors.As(readErr, &readOp) || readOp.Op != "read" || readOp.Target != "pool 7 object" || !errors.Is(readErr, ErrClosed) {
		t.Fatalf("read error=%#v", readErr)
	}
	_, statErr := object.Stat(t.Context())
	var statOp *OpError
	if !errors.As(statErr, &statOp) || statOp.Op != "stat" || statOp.Target != "pool 7 object" || !errors.Is(statErr, ErrClosed) {
		t.Fatalf("stat error=%#v", statErr)
	}
	mutations := []struct {
		operation string
		invoke    func() error
	}{
		{operation: "write", invoke: func() error { _, err := object.Write(t.Context(), 1, []byte("x")); return err }},
		{operation: "write full", invoke: func() error { _, err := object.WriteFull(t.Context(), []byte("x")); return err }},
		{operation: "append", invoke: func() error { _, err := object.Append(t.Context(), []byte("x")); return err }},
		{operation: "truncate", invoke: func() error { _, err := object.Truncate(t.Context(), 1); return err }},
		{operation: "zero", invoke: func() error { _, err := object.Zero(t.Context(), 1, 1); return err }},
		{operation: "remove", invoke: func() error { _, err := object.Remove(t.Context()); return err }},
		{operation: "create", invoke: func() error { _, err := object.Create(t.Context(), true); return err }},
	}
	for _, mutation := range mutations {
		err := mutation.invoke()
		var operationError *OpError
		if !errors.As(err, &operationError) || operationError.Op != mutation.operation || operationError.Target != "pool 7 object" || !errors.Is(err, ErrClosed) {
			t.Fatalf("%s error=%#v", mutation.operation, err)
		}
	}
}
