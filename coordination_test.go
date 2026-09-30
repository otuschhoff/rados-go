package rados

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	wire "github.com/otuschhoff/rados-go/internal/encoding"
	"github.com/otuschhoff/rados-go/internal/msgr"
)

func TestWatchRejectsUnboundedQueue(t *testing.T) {
	_, _, err := (ObjectRef{}).Watch(context.Background(), MaxWatchQueue+1)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("error=%v", err)
	}
}

func TestZeroValueLockMethodsReturnInvalidArgument(t *testing.T) {
	object := ObjectRef{}
	for _, test := range []struct {
		name   string
		invoke func() error
	}{
		{name: "lock", invoke: func() error {
			return object.Lock(context.Background(), "name", LockExclusive, LockOptions{Cookie: "cookie"})
		}},
		{name: "unlock", invoke: func() error { return object.Unlock(context.Background(), "name", "cookie") }},
		{name: "list", invoke: func() error { _, err := object.ListLockers(context.Background(), "name"); return err }},
		{name: "break", invoke: func() error { return object.BreakLock(context.Background(), "name", "client.1", "cookie") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.invoke(); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestParseLockClient(t *testing.T) {
	client, ok := parseLockClient("client.42")
	if !ok || client != 42 {
		t.Fatalf("client=%d ok=%t", client, ok)
	}
	for _, value := range []string{"", "osd.42", "client.-1", "client.1x"} {
		if _, ok := parseLockClient(value); ok {
			t.Fatalf("accepted %q", value)
		}
	}
}

func TestWatchForwardErrorWrapsInternalError(t *testing.T) {
	watch := &Watch{
		errors: make(chan error, 1),
		object: ObjectRef{pool: Pool{client: &Client{}, name: "pool"}, name: "object"},
	}
	watch.forwardError(msgr.ErrQueueSaturated)
	err := <-watch.errors
	var operationError *OpError
	if !errors.As(err, &operationError) || operationError.Op != "watch" || operationError.Target != "pool 0 object" || !errors.Is(err, msgr.ErrQueueSaturated) {
		t.Fatalf("watch error=%v", err)
	}
}

func TestDecodeNotifyReplyPreservesMetadataAndRejectsEmpty(t *testing.T) {
	if _, err := decodeNotifyReply(nil); err == nil {
		t.Fatal("empty notify completion accepted")
	}
	encoder := wire.NewEncoder(256)
	encoder.Uint32(1)
	encoder.Uint64(11)
	encoder.Uint64(12)
	encoder.Bytes([]byte{0, 0xff})
	encoder.Uint32(1)
	encoder.Uint64(21)
	encoder.Uint64(22)
	data, err := encoder.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	reply, err := decodeNotifyReply(data)
	if err != nil || len(reply.Acknowledged) != 1 || len(reply.TimedOut) != 1 {
		t.Fatalf("reply=%+v error=%v", reply, err)
	}
	ack := reply.Acknowledged[0]
	timedOut := reply.TimedOut[0]
	if ack.Client != 11 || ack.Cookie != 12 || !bytes.Equal(ack.Data, []byte{0, 0xff}) || timedOut.Client != 21 || timedOut.Cookie != 22 {
		t.Fatalf("reply metadata=%+v", reply)
	}
}

func TestNotifyTimeoutPolicy(t *testing.T) {
	if got := durationSeconds(0); got != 0 {
		t.Fatalf("durationSeconds(0)=%d", got)
	}
	if got := durationSeconds(1500 * time.Millisecond); got != 2 {
		t.Fatalf("durationSeconds(1.5s)=%d", got)
	}

	unlimited := &Client{}
	unlimitedCtx, unlimitedCancel := unlimited.notifyContext(context.Background())
	defer unlimitedCancel()
	if _, ok := unlimitedCtx.Deadline(); ok {
		t.Fatal("unlimited notify has deadline")
	}

	finite := &Client{config: Config{OperationTimeout: time.Minute}}
	start := time.Now()
	finiteCtx, finiteCancel := finite.notifyContext(context.Background())
	defer finiteCancel()
	deadline, ok := finiteCtx.Deadline()
	if !ok || deadline.Before(start.Add(60*time.Second)) || deadline.After(start.Add(62*time.Second)) {
		t.Fatalf("finite notify deadline=%v", deadline)
	}

	callerCtx, callerCancel := context.WithTimeout(context.Background(), time.Hour)
	defer callerCancel()
	callerDeadline, _ := callerCtx.Deadline()
	configuredCtx, configuredCancel := finite.notifyContext(callerCtx)
	defer configuredCancel()
	configuredDeadline, ok := configuredCtx.Deadline()
	if !ok || !configuredDeadline.Equal(callerDeadline) {
		t.Fatalf("notify deadline=%v, want caller deadline %v", configuredDeadline, callerDeadline)
	}
}
