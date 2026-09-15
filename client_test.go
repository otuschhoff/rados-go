package rados

import (
	"context"
	"errors"
	"testing"
	"time"

	wire "github.com/otuschhoff/go-librados/internal/encoding"
	"github.com/otuschhoff/go-librados/internal/mon"
	"github.com/otuschhoff/go-librados/internal/msgr"
	"github.com/otuschhoff/go-librados/internal/objecter"
	"github.com/otuschhoff/go-librados/internal/osd"
	"github.com/otuschhoff/go-librados/internal/protocol"
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

func TestWrapErrorPreservesOutcomeUnknownWithClose(t *testing.T) {
	client := &Client{}
	err := client.wrapError("notify", "object", errors.Join(msgr.ErrOutcomeUnknown, objecter.ErrClosed))
	if !errors.Is(err, ErrOutcomeUnknown) || !errors.Is(err, ErrClosed) || !errors.Is(err, msgr.ErrOutcomeUnknown) || !errors.Is(err, objecter.ErrClosed) {
		t.Fatalf("error=%v", err)
	}
}

func TestWrapErrorPreservesNestedWireMetadata(t *testing.T) {
	client := &Client{}
	inner := client.wrapError("execute write", "pool/object", protocol.WireErrno(-16))
	err := client.wrapError("lock", "pool/object", inner)
	var operation *OpError
	if !errors.As(err, &operation) || operation.Op != "lock" || operation.Target != "pool/object" || operation.Code != -16 || !errors.Is(err, ErrConflict) {
		t.Fatalf("error=%v operation=%+v", err, operation)
	}
}

func TestWrapErrorClassifiesWatchInterruption(t *testing.T) {
	err := (&Client{}).wrapError("watch", "pool/object", errors.Join(objecter.ErrWatchInterrupted, protocol.WireErrno(-110)))
	var operation *OpError
	if !errors.Is(err, ErrWatchInterrupted) || !errors.Is(err, objecter.ErrWatchInterrupted) || !errors.As(err, &operation) || operation.Code != -110 || !errors.Is(err, ErrTimeout) {
		t.Fatalf("error=%v", err)
	}
}

func TestClientCloseWaitsForPublicWorkers(t *testing.T) {
	client := &Client{}
	if !client.startWorker() {
		t.Fatal("worker was not admitted")
	}
	release := make(chan struct{})
	go func() {
		<-release
		client.workers.Done()
	}()
	closed := make(chan struct{})
	go func() {
		_ = client.Close()
		close(closed)
	}()
	concurrentClosed := make(chan struct{})
	go func() {
		_ = client.Close()
		close(concurrentClosed)
	}()
	select {
	case <-closed:
		t.Fatal("close returned while a public worker was active")
	case <-time.After(20 * time.Millisecond):
	}
	select {
	case <-concurrentClosed:
		t.Fatal("concurrent close returned while a public worker was active")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("close did not return after the public worker exited")
	}
	select {
	case <-concurrentClosed:
	case <-time.After(time.Second):
		t.Fatal("concurrent close did not return after the public worker exited")
	}
}

func TestClientRejectsWorkWhileClosing(t *testing.T) {
	client := &Client{closing: true, closeDone: make(chan struct{})}
	if _, _, err := client.active(); !errors.Is(err, ErrClosed) {
		t.Fatalf("active error=%v", err)
	}
	if client.startWorker() {
		t.Fatal("worker admitted while closing")
	}
}

func TestClientCloseWaitsForAdmittedOperation(t *testing.T) {
	client := &Client{connected: true, monitor: &mon.Client{}, objects: &objecter.Client{}}
	_, _, done, err := client.beginOperation()
	if err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	client.monitor = nil
	client.objects = nil
	client.mu.Unlock()
	closed := make(chan struct{})
	go func() {
		_ = client.Close()
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("close returned while an operation was admitted")
	case <-time.After(20 * time.Millisecond):
	}
	done()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("close did not return after the operation completed")
	}
}

func TestShutdownBeforeConnectIsTerminal(t *testing.T) {
	client, err := New(Config{Monitors: []string{"127.0.0.1:3300"}, Entity: "client.test", Key: []byte(testPublicKey)})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := client.Connect(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("connect after shutdown error=%v", err)
	}
}

func TestConcurrentShutdownHonorsContext(t *testing.T) {
	client := &Client{closing: true, closeDone: make(chan struct{}), config: Config{OperationTimeout: time.Second}}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := client.Shutdown(ctx); !errors.Is(err, ErrTimeout) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown error=%v", err)
	}
}

func TestShutdownAfterCloseIsIdempotent(t *testing.T) {
	client, err := New(Config{Monitors: []string{"127.0.0.1:3300"}, Entity: "client.test", Key: []byte(testPublicKey)})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	for range 10000 {
		if err := client.Shutdown(context.Background()); err != nil {
			t.Fatalf("shutdown after close error=%v", err)
		}
	}
}

func TestConnectGateHonorsContextAndClientClose(t *testing.T) {
	newClient := func(t *testing.T) *Client {
		t.Helper()
		client, err := New(Config{Monitors: []string{"127.0.0.1:3300"}, Entity: "client.test", Key: []byte(testPublicKey), OperationTimeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		<-client.connectGate
		return client
	}
	t.Run("caller timeout", func(t *testing.T) {
		client := newClient(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		if err := client.Connect(ctx); !errors.Is(err, ErrTimeout) || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("connect error=%v", err)
		}
		client.connectGate <- struct{}{}
		_ = client.Close()
	})
	t.Run("client close", func(t *testing.T) {
		client := newClient(t)
		connectDone := make(chan error, 1)
		go func() { connectDone <- client.Connect(context.Background()) }()
		time.Sleep(20 * time.Millisecond)
		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
		if err := <-connectDone; !errors.Is(err, ErrCanceled) {
			t.Fatalf("connect error=%v", err)
		}
		client.connectGate <- struct{}{}
	})
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
