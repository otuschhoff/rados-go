package msgr

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSessionTransferredMessageAdmission(t *testing.T) {
	for _, marker := range []func(Message) Message{nil, TakeMessageOwnership, RetainImmutableMessage} {
		transferred := marker != nil
		owner := newUnitSessionOwner(t)
		message := testMessage("front")
		message.Data = []byte("payload")
		message.Lengths.Data = uint32(len(message.Data))
		if transferred {
			message = marker(message)
		}
		command := &submitCommand{ctx: context.Background(), message: message, result: make(chan submitResult, 1)}
		owner.submit(command)
		pending := owner.byRequest[command]
		if pending == nil {
			t.Fatal("message not admitted")
		}
		if same := &pending.message.Data[0] == &message.Data[0]; same != transferred {
			t.Fatalf("transferred=%t buffer reused=%t", transferred, same)
		}
		if pending.message.transferred || len(command.message.Data) != 0 {
			t.Fatal("ownership marker or command references retained")
		}
		if !transferred {
			message.Data[0] = 'X'
			if string(pending.message.Data) != "payload" {
				t.Fatal("default submission lost defensive ownership")
			}
		}
		owner.cancel(cancelCommand{request: command, err: context.Canceled})
		if len(owner.pending) != 0 || owner.retainedBytes != 0 || len(owner.byRequest) != 0 || len(owner.byTID) != 0 {
			t.Fatal("cancel retained transferred payload")
		}
	}
}

func TestSessionRegisteredAdmissionOwnership(t *testing.T) {
	for _, rejected := range []bool{false, true} {
		owner := newUnitSessionOwner(t)
		ctx, cancel := context.WithCancel(context.Background())
		if rejected {
			cancel()
		}
		command := &submitCommand{ctx: ctx, message: TakeMessageOwnership(testMessage("owned")), admitted: make(chan struct{}), result: make(chan submitResult, 1)}
		called := 0
		command.registered = func() {
			called++
			if len(command.message.Front) != 0 || command.registered != nil {
				t.Fatal("registration notification retained command references")
			}
			if (owner.byRequest[command] == nil) != rejected {
				t.Fatal("notification preceded the admission decision")
			}
		}
		owner.submit(command)
		cancel()
		if called != 1 {
			t.Fatalf("registration notifications = %d, want 1", called)
		}
		select {
		case <-command.admitted:
		default:
			t.Fatal("registration cleanup did not complete")
		}
	}
}

func TestSessionRegisteredSubmission(t *testing.T) {
	for _, borrowed := range []bool{false, true} {
		for _, outcome := range []string{"reply", "cancel", "stop", "reject"} {
			t.Run(fmt.Sprintf("borrowed=%t/%s", borrowed, outcome), func(t *testing.T) {
				transport := newFakeTransport()
				config := testSessionConfig(t)
				if outcome == "reject" {
					config.MaxRetainedBytes = 1
				}
				session := newTestSession(t, transport, nil, config)
				defer session.Stop()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				var calls atomic.Uint32
				notified := make(chan struct{}, 1)
				registered := func() { calls.Add(1); notified <- struct{}{} }
				results := make(chan submitOutcome, 1)
				go func() {
					var message Message
					var err error
					if borrowed {
						var release func()
						message, release, err = session.SubmitBorrowedRegistered(ctx, testMessage("request"), registered)
						release()
					} else {
						message, err = session.SubmitRegistered(ctx, testMessage("request"), registered)
					}
					results <- submitOutcome{message: message, err: err}
				}()
				select {
				case <-notified:
				case <-time.After(time.Second):
					t.Fatal("registration notification did not run")
				}
				if outcome != "reject" {
					request := decodeWrittenMessage(t, transport)
					switch outcome {
					case "reply":
						reply := testMessage("response")
						reply.Header.Sequence = 1
						reply.Header.AckSequence = request.Header.Sequence
						reply.Header.TransactionID = request.Header.TransactionID
						transport.inject(messageFrame(t, reply))
					case "cancel":
						cancel()
					case "stop":
						session.Stop()
					}
				}
				result := waitOutcome(t, results)
				if outcome == "reply" {
					if result.err != nil || string(result.message.Front) != "response" {
						t.Fatalf("reply = %+v", result)
					}
				} else if result.err == nil {
					t.Fatal("non-successful outcome returned success")
				}
				if outcome == "reject" && !errors.Is(result.err, ErrQueueSaturated) {
					t.Fatalf("rejection = %v", result.err)
				}
				if calls.Load() != 1 {
					t.Fatalf("registration notifications = %d, want 1", calls.Load())
				}
			})
		}
	}
}

func TestSessionBufferedWriteCompletion(t *testing.T) {
	for _, occupied := range []bool{false, true} {
		t.Run(fmt.Sprintf("occupied=%t", occupied), func(t *testing.T) {
			owner := newUnitSessionOwner(t)
			owner.writes = make(chan pumpWriteResult, 1)
			if occupied {
				owner.writes <- pumpWriteResult{generation: 2, taskID: 3}
			}
			transport := newFakeTransport()
			tasks := make(chan writeTask, 1)
			command := &submitCommand{ctx: context.Background()}
			tasks <- writeTask{id: 7, frame: controlFrame(t, Ack{Sequence: 1}), request: command}
			close(tasks)
			finished := make(chan struct{})
			owner.pumpWG.Add(1)
			go func() { owner.writePump(4, transport, tasks); close(finished) }()
			select {
			case <-transport.writes:
			case <-time.After(time.Second):
				t.Fatal("writer did not send frame")
			}
			if occupied {
				close(owner.session.done)
			}
			select {
			case <-finished:
			case <-time.After(time.Second):
				t.Fatal("writer completion remained blocked on the owner")
			}
			completion := <-owner.writes
			if occupied {
				if completion.generation != 2 || completion.taskID != 3 {
					t.Fatal("stop overwrote an older completion")
				}
			} else if completion.generation != 4 || completion.taskID != 7 || completion.request != command || completion.err != nil {
				t.Fatalf("completion = %+v", completion)
			}
			owner.pumpWG.Wait()
		})
	}
}

func TestMessageLeaseReclaimsAfterEveryHolder(t *testing.T) {
	var reclaimed atomic.Uint32
	lease := NewMessageLease(func() { reclaimed.Add(1) })
	var holders sync.WaitGroup
	for index := 0; index < 32; index++ {
		lease.Retain()
		holders.Add(1)
		go func() { defer holders.Done(); lease.Release() }()
	}
	holders.Wait()
	if reclaimed.Load() != 0 {
		t.Fatal("lease reclaimed while producer still held it")
	}
	lease.Release()
	if reclaimed.Load() != 1 || lease.reclaim != nil {
		t.Fatal("final release failed to reclaim exactly once or clear callback")
	}
	for _, action := range []func(){lease.Retain, lease.Release} {
		func() {
			defer func() {
				if recover() == nil {
					t.Error("invalid lease reuse did not fail")
				}
			}()
			action()
		}()
	}
}

func TestSessionLeasedPayloadSurvivesCanceledWrite(t *testing.T) {
	testSessionLeasedPayloadSurvivesCanceledWrite(t, false)
}

func TestSessionPaddedPayloadSurvivesCanceledWrite(t *testing.T) {
	testSessionLeasedPayloadSurvivesCanceledWrite(t, true)
}

func testSessionLeasedPayloadSurvivesCanceledWrite(t *testing.T, padded bool) {
	t.Helper()
	owner := newUnitSessionOwner(t)
	owner.writeTasks = make(chan writeTask, 1)
	var reclaimed atomic.Uint32
	message := testMessage("request")
	message.Data = []byte("payload")
	var lease *MessageLease
	if padded {
		storage := make([]byte, LeasedPayloadPrefix+16+LeasedPayloadPadding)
		message.Data = storage[LeasedPayloadPrefix : LeasedPayloadPrefix+16]
		copy(message.Data, "payload")
		lease = NewPaddedMessageLease(storage, message.Data, func() { reclaimed.Add(1) })
	} else {
		lease = NewMessageLease(func() { reclaimed.Add(1) })
	}
	message.Lengths.Data = uint32(len(message.Data))
	command := &submitCommand{ctx: context.Background(), message: RetainLeasedMessage(message, lease), result: make(chan submitResult, 1)}
	owner.submit(command)
	owner.dispatch()
	task := <-owner.writeTasks
	owner.cancel(cancelCommand{request: command, err: context.Canceled})
	lease.Release()
	if reclaimed.Load() != 0 || task.frame.payloadLease == nil {
		t.Fatal("cancellation reclaimed an active writer payload")
	}
	if padded && (len(lease.paddedPayload) != LeasedPayloadPrefix+len(message.Data)+LeasedPayloadPadding || &lease.paddedPayload[LeasedPayloadPrefix] != &message.Data[0]) {
		t.Fatal("cancellation discarded the active writer's direct plaintext")
	}
	owner.writes = make(chan pumpWriteResult, 1)
	tasks := make(chan writeTask, 1)
	tasks <- task
	close(tasks)
	owner.pumpWG.Add(1)
	owner.writePump(1, newFakeTransport(), tasks)
	if reclaimed.Load() != 1 || lease.reclaim != nil || lease.paddedPayload != nil {
		t.Fatal("completed writer did not reclaim its final payload reference")
	}
}

func TestSessionLeasedPayloadRejected(t *testing.T) {
	owner := newUnitSessionOwner(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var reclaimed atomic.Uint32
	lease := NewMessageLease(func() { reclaimed.Add(1) })
	command := &submitCommand{ctx: ctx, message: RetainLeasedMessage(testMessage("request"), lease), result: make(chan submitResult, 1)}
	owner.submit(command)
	lease.Release()
	if reclaimed.Load() != 1 || len(command.message.Front) != 0 || command.message.payloadLease != nil {
		t.Fatal("rejected admission retained a payload lease")
	}
}

func TestSessionTransferredMessageRejected(t *testing.T) {
	owner := newUnitSessionOwner(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	command := &submitCommand{ctx: ctx, message: TakeMessageOwnership(testMessage("owned")), result: make(chan submitResult, 1)}
	owner.submit(command)
	if !errors.Is((<-command.result).err, context.Canceled) || len(command.message.Front) != 0 || len(owner.pending) != 0 {
		t.Fatal("rejected transfer retained payload or lost cancellation")
	}
}
