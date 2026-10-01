package objecter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/otuschhoff/rados-go/internal/maps"
	"github.com/otuschhoff/rados-go/internal/msgr"
	"github.com/otuschhoff/rados-go/internal/osd"
	"github.com/otuschhoff/rados-go/internal/protocol"
)

type osdSession struct {
	raw            osdTransport
	limits         osd.Limits
	ackTimeout     time.Duration
	ackMu          sync.Mutex
	ackQueue       []*backoffACK
	ackActive      *backoffACK
	ackCount       int
	ackBytes       uint64
	ackGeneration  uint64
	ackWake        chan struct{}
	ackResults     chan backoffACKResult
	ackCtx         context.Context
	ackCancel      context.CancelFunc
	ackDone        chan struct{}
	dispatcherDone chan struct{}

	mu             sync.Mutex
	backoffs       map[uint64]osd.Backoff
	submissions    map[uint64]*targetSubmission
	nextSubmission uint64
	changed        chan struct{}
	err            error
	notifications  chan osd.WatchNotification
	interruptions  chan error
	observe        func(bool, error)
}

type targetSubmission struct {
	pg     maps.PG
	object osd.HObject
	cancel context.CancelFunc
	resend bool
}

type backoffACK struct {
	message    msgr.Message
	ctx        context.Context
	cancel     context.CancelFunc
	bytes      uint64
	generation uint64
}

type backoffACKResult struct {
	err                 error
	generation          uint64
	transportGeneration uint64
}

type generationControlTransport interface {
	ControlGeneration() uint64
	SendControlGeneration(context.Context, msgr.Message, uint64) error
}

type osdTransport interface {
	Submit(context.Context, msgr.Message) (msgr.Message, error)
	SubmitAdmitted(context.Context, msgr.Message, func()) (msgr.Message, error)
	Send(context.Context, msgr.Message) error
	Incoming() <-chan msgr.Message
	Terminal() <-chan error
	Events() <-chan msgr.SessionEvent
	Resets() <-chan struct{}
	Done() <-chan struct{}
	Stop()
}

func newOSDSession(raw osdTransport, limits osd.Limits, ackTimeout time.Duration, observers ...func(bool, error)) *osdSession {
	var observe func(bool, error)
	if len(observers) != 0 {
		observe = observers[0]
	}
	session := &osdSession{raw: raw, limits: limits, ackTimeout: ackTimeout, backoffs: make(map[uint64]osd.Backoff), submissions: make(map[uint64]*targetSubmission), changed: make(chan struct{}), notifications: make(chan osd.WatchNotification, 128), interruptions: make(chan error, 1), observe: observe}
	if scoped, ok := raw.(generationControlTransport); ok {
		session.ackGeneration = scoped.ControlGeneration()
	}
	session.ackCtx, session.ackCancel = context.WithCancel(context.Background())
	session.ackWake = make(chan struct{}, 1)
	session.ackResults = make(chan backoffACKResult, 1)
	session.ackDone = make(chan struct{})
	session.dispatcherDone = make(chan struct{})
	go session.sendACKs()
	go session.receive()
	return session
}

func (session *osdSession) Notifications() <-chan osd.WatchNotification { return session.notifications }
func (session *osdSession) Interruptions() <-chan error                 { return session.interruptions }

func (session *osdSession) NotificationError() error {
	return session.failure(msgr.ErrSessionClosed)
}

func (session *osdSession) Submit(ctx context.Context, message msgr.Message) (msgr.Message, error) {
	return session.raw.Submit(ctx, message)
}

func (session *osdSession) OwnsReplyMessages() bool {
	owned, ok := session.raw.(interface{ OwnsReplyMessages() bool })
	return ok && owned.OwnsReplyMessages()
}

func (session *osdSession) SubmitTarget(ctx context.Context, pg maps.PG, object osd.HObject, message msgr.Message) (msgr.Message, error) {
	result, release, err := session.submitTarget(ctx, pg, object, message, false)
	release()
	return result, err
}

func (session *osdSession) SubmitBorrowedTarget(ctx context.Context, pg maps.PG, object osd.HObject, message msgr.Message) (msgr.Message, func(), error) {
	return session.submitTarget(ctx, pg, object, message, true)
}

func (session *osdSession) submitTarget(ctx context.Context, pg maps.PG, object osd.HObject, message msgr.Message, borrowed bool) (msgr.Message, func(), error) {
	noRelease := func() {}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		session.mu.Lock()
		if session.err != nil {
			err := session.err
			session.mu.Unlock()
			return msgr.Message{}, noRelease, err
		}
		blocked := false
		for _, backoff := range session.backoffs {
			if backoff.PG == pg && backoff.Contains(object) {
				blocked = true
				break
			}
		}
		if !blocked {
			attemptCtx, cancel := context.WithCancel(ctx)
			session.nextSubmission++
			for session.nextSubmission == 0 || session.submissions[session.nextSubmission] != nil {
				session.nextSubmission++
			}
			id := session.nextSubmission
			submission := &targetSubmission{pg: pg, object: object, cancel: cancel}
			session.submissions[id] = submission
			var unlock sync.Once
			var result msgr.Message
			var err error
			release := noRelease
			admitted := func() { unlock.Do(session.mu.Unlock) }
			if source, ok := session.raw.(interface {
				SubmitBorrowedAdmitted(context.Context, msgr.Message, func()) (msgr.Message, func(), error)
			}); borrowed && ok {
				result, release, err = source.SubmitBorrowedAdmitted(attemptCtx, message, admitted)
			} else {
				result, err = session.raw.SubmitAdmitted(attemptCtx, message, admitted)
			}
			unlock.Do(session.mu.Unlock)
			session.mu.Lock()
			delete(session.submissions, id)
			resend := submission.resend
			session.mu.Unlock()
			cancel()
			if resend && ctx.Err() == nil {
				release()
				continue
			}
			if err != nil {
				err = errors.Join(err, session.failure(err))
			}
			return result, release, err
		}
		changed := session.changed
		session.mu.Unlock()
		select {
		case <-ctx.Done():
			return msgr.Message{}, noRelease, ctx.Err()
		case <-session.raw.Done():
			return msgr.Message{}, noRelease, session.failure(msgr.ErrSessionClosed)
		case <-changed:
		}
	}
}

func (session *osdSession) Stop() {
	session.ackCancel()
	session.raw.Stop()
	<-session.dispatcherDone
}

func (session *osdSession) enqueueACK(message msgr.Message) error {
	session.ackMu.Lock()
	defer session.ackMu.Unlock()
	retained := uint64(msgr.MessageHeaderSize + len(message.Front))
	if session.ackCount >= msgr.MaxControlMessages || retained > msgr.MaxControlRetainedBytes-session.ackBytes {
		return fmt.Errorf("backoff ACK reserve: %w", msgr.ErrQueueSaturated)
	}
	message.Front = append([]byte(nil), message.Front...)
	ctx, cancel := context.WithDeadline(session.ackCtx, time.Now().Add(session.ackTimeout))
	session.ackQueue = append(session.ackQueue, &backoffACK{message: message, ctx: ctx, cancel: cancel, bytes: retained, generation: session.ackGeneration})
	session.ackCount++
	session.ackBytes += retained
	select {
	case session.ackWake <- struct{}{}:
	default:
	}
	return nil
}

func (session *osdSession) resetACKs(generation uint64) {
	session.ackMu.Lock()
	defer session.ackMu.Unlock()
	session.ackGeneration = generation
	if session.ackActive != nil {
		session.ackActive.cancel()
	}
	for index, ack := range session.ackQueue {
		ack.cancel()
		session.ackCount--
		session.ackBytes -= ack.bytes
		session.ackQueue[index] = nil
	}
	session.ackQueue = nil
}

func (session *osdSession) sendACKs() {
	defer close(session.ackDone)
	scoped, authoritative := session.raw.(generationControlTransport)
	send := session.raw.Send
	if control, ok := session.raw.(interface {
		SendControl(context.Context, msgr.Message) error
	}); ok {
		send = control.SendControl
	}
	for {
		select {
		case <-session.ackCtx.Done():
			return
		case <-session.raw.Done():
			return
		case <-session.ackWake:
		}
		for {
			session.ackMu.Lock()
			if session.ackCtx.Err() != nil || len(session.ackQueue) == 0 {
				session.ackMu.Unlock()
				break
			}
			ack := session.ackQueue[0]
			session.ackQueue[0] = nil
			session.ackQueue = session.ackQueue[1:]
			session.ackActive = ack
			session.ackMu.Unlock()
			err := ack.ctx.Err()
			transportGeneration := ack.message.TransportGeneration
			if err == nil {
				if authoritative {
					err = scoped.SendControlGeneration(ack.ctx, ack.message, transportGeneration)
				} else {
					err = send(ack.ctx, ack.message)
				}
			}
			if errors.Is(err, msgr.ErrOutcomeUnknown) {
				err = errors.Join(fmt.Errorf("backoff ACK write failed: %v", err), ack.ctx.Err())
			}
			ack.cancel()
			ack.message = msgr.Message{}
			session.ackMu.Lock()
			session.ackActive = nil
			session.ackCount--
			session.ackBytes -= ack.bytes
			current := ack.generation == session.ackGeneration
			session.ackMu.Unlock()
			if err != nil && current {
				select {
				case session.ackResults <- backoffACKResult{err: err, generation: ack.generation, transportGeneration: transportGeneration}:
				case <-session.ackCtx.Done():
					return
				case <-session.raw.Done():
					return
				}
			}
		}
	}
}

func (session *osdSession) Wait(ctx context.Context, pg maps.PG, object osd.HObject) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		session.mu.Lock()
		if session.err != nil {
			err := session.err
			session.mu.Unlock()
			return err
		}
		blocked := false
		for _, backoff := range session.backoffs {
			if backoff.PG == pg && backoff.Contains(object) {
				blocked = true
				break
			}
		}
		if !blocked {
			session.mu.Unlock()
			return nil
		}
		changed := session.changed
		session.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-session.raw.Done():
			return session.failure(msgr.ErrSessionClosed)
		case <-changed:
		}
	}
}

func (session *osdSession) receive() {
	defer func() {
		session.ackCancel()
		session.ackMu.Lock()
		generation := session.ackGeneration + 1
		session.ackMu.Unlock()
		session.resetACKs(generation)
		<-session.ackDone
		close(session.notifications)
		close(session.dispatcherDone)
	}()
	events := session.raw.Events()
	wasUnavailable := false
	scoped, authoritative := session.raw.(generationControlTransport)
	reconcile := func() bool {
		session.ackMu.Lock()
		generation := session.ackGeneration + 1
		if authoritative {
			generation = scoped.ControlGeneration()
			if generation == session.ackGeneration {
				session.ackMu.Unlock()
				return false
			}
		}
		session.ackMu.Unlock()
		session.resetACKs(generation)
		clear(session.backoffs)
		close(session.changed)
		session.changed = make(chan struct{})
		return true
	}
	interrupted := func() {
		wasUnavailable = true
		if session.observe != nil {
			session.observe(false, msgr.ErrSessionDisconnected)
		}
		select {
		case session.interruptions <- msgr.ErrSessionDisconnected:
		default:
		}
	}
	reset := func() {
		session.mu.Lock()
		changed := reconcile()
		session.mu.Unlock()
		if changed {
			interrupted()
		}
	}
	for {
		select {
		case result := <-session.ackResults:
			select {
			case <-session.raw.Resets():
				reset()
			default:
			}
			session.mu.Lock()
			changed := false
			if authoritative {
				changed = reconcile()
			}
			session.ackMu.Lock()
			current := result.generation == session.ackGeneration
			session.ackMu.Unlock()
			if authoritative && result.transportGeneration != scoped.ControlGeneration() {
				current = false
			}
			if current && session.err == nil {
				session.err = result.err
				close(session.changed)
				session.changed = make(chan struct{})
			}
			session.mu.Unlock()
			if changed {
				interrupted()
			}
			if current {
				session.raw.Stop()
				return
			}
		case message, ok := <-session.raw.Incoming():
			if !ok {
				session.fail(msgr.ErrSessionClosed)
				return
			}
			if message.Header.Type == protocol.MessageOSDMap {
				session.fail(ErrStaleMap)
				session.raw.Stop()
				return
			}
			if message.Header.Type == protocol.MessageWatchNotify {
				notification, err := osd.DecodeWatchNotification(message, session.limits)
				if err != nil {
					session.fail(err)
					session.raw.Stop()
					return
				}
				select {
				case session.notifications <- notification:
				default:
					session.fail(msgr.ErrQueueSaturated)
					session.raw.Stop()
					return
				}
				continue
			}
			if message.Header.Type != protocol.MessageOSDBackoff {
				continue
			}
			if authoritative && (message.TransportGeneration == 0 || message.TransportGeneration != scoped.ControlGeneration()) {
				continue
			}
			backoff, err := osd.DecodeBackoff(message, session.limits)
			var ack msgr.Message
			if err == nil && backoff.Operation == osd.BackoffBlock {
				ack, err = osd.EncodeBackoffAcknowledgment(backoff, session.limits)
				ack.TransportGeneration = message.TransportGeneration
			}
			session.mu.Lock()
			changed := false
			if authoritative {
				changed = reconcile()
			}
			current := !authoritative || message.TransportGeneration == scoped.ControlGeneration()
			if current && err == nil {
				if backoff.Operation == osd.BackoffBlock {
					session.backoffs[backoff.ID] = backoff
					err = session.enqueueACK(ack)
				} else {
					blocked, ok := session.backoffs[backoff.ID]
					delete(session.backoffs, backoff.ID)
					if ok {
						for _, submission := range session.submissions {
							if submission.pg == blocked.PG && blocked.Contains(submission.object) {
								submission.resend = true
								submission.cancel()
							}
						}
					}
				}
				close(session.changed)
				session.changed = make(chan struct{})
			}
			if err != nil && authoritative {
				changed = reconcile() || changed
				current = message.TransportGeneration == scoped.ControlGeneration()
			}
			if current && err != nil && session.err == nil {
				session.err = err
				close(session.changed)
				session.changed = make(chan struct{})
			}
			session.mu.Unlock()
			if changed {
				interrupted()
			}
			if current && err != nil {
				session.raw.Stop()
				return
			}
		case <-session.raw.Done():
			err := msgr.ErrSessionClosed
			select {
			case terminal := <-session.raw.Terminal():
				if terminal != nil {
					err = terminal
				}
			default:
			}
			session.fail(err)
			return
		case err, ok := <-session.raw.Terminal():
			if !ok || err == nil {
				err = msgr.ErrSessionClosed
			}
			session.fail(err)
			session.raw.Stop()
			return
		case <-session.raw.Resets():
			reset()
		case event, ok := <-events:
			if !ok {
				events = nil
				continue
			}
			recovered := event.Kind == msgr.EventReconnectOK || event.Kind == msgr.EventStateChanged && event.State == msgr.StateReady
			if wasUnavailable && recovered {
				wasUnavailable = false
				if session.observe != nil {
					session.observe(true, nil)
				}
			}
		}
	}
}

func (session *osdSession) fail(err error) {
	if err == nil {
		err = errors.New("OSD session failed")
	}
	session.mu.Lock()
	if session.err == nil {
		session.err = err
		close(session.changed)
		session.changed = make(chan struct{})
	}
	session.mu.Unlock()
}

func (session *osdSession) failure(fallback error) error {
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.err != nil {
		return session.err
	}
	return fallback
}
