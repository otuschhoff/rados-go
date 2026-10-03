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

	mu              sync.Mutex
	backoffs        map[uint64]osd.Backoff
	maxBackoffs     int
	maxBackoffBytes uint64
	backoffBytes    uint64
	backoffsByPG    map[maps.PG]map[uint64]osd.Backoff
	backoffWaiters  map[maps.PG]*backoffWaiters
	submissions     map[uint64]*targetSubmission
	submissionsByPG map[maps.PG]*targetSubmission
	nextSubmission  uint64
	changed         chan struct{}
	err             error
	notifications   chan osd.WatchNotification
	interruptions   chan error
	observe         func(bool, error)
}

type backoffWaiters struct {
	changed chan struct{}
	count   int
}

type targetSubmission struct {
	previous *targetSubmission
	next     *targetSubmission
	pg       maps.PG
	object   osd.HObject
	cancel   context.CancelFunc
	resend   bool
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
	return newOSDSessionWithBackoffLimits(raw, limits, ackTimeout, 4096, 8<<20, observers...)
}

func newOSDSessionWithBackoffLimits(raw osdTransport, limits osd.Limits, ackTimeout time.Duration, count int, bytes uint64, observers ...func(bool, error)) *osdSession {
	var observe func(bool, error)
	if len(observers) != 0 {
		observe = observers[0]
	}
	session := &osdSession{raw: raw, limits: limits, ackTimeout: ackTimeout, backoffs: make(map[uint64]osd.Backoff), submissions: make(map[uint64]*targetSubmission), changed: make(chan struct{}), notifications: make(chan osd.WatchNotification, 128), interruptions: make(chan error, 1), observe: observe}
	session.maxBackoffs, session.maxBackoffBytes = count, bytes
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
		for _, backoff := range session.backoffsByPG[pg] {
			if backoff.Contains(object) {
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
			session.addSubmission(id, submission)
			var unlock sync.Once
			var result msgr.Message
			var err error
			release := noRelease
			admitted := func() { unlock.Do(session.mu.Unlock) }
			if source, ok := session.raw.(interface {
				SubmitRegistered(context.Context, msgr.Message, func()) (msgr.Message, error)
				SubmitBorrowedRegistered(context.Context, msgr.Message, func()) (msgr.Message, func(), error)
			}); ok {
				if borrowed {
					result, release, err = source.SubmitBorrowedRegistered(attemptCtx, message, admitted)
				} else {
					result, err = source.SubmitRegistered(attemptCtx, message, admitted)
				}
			} else if source, ok := session.raw.(interface {
				SubmitBorrowedAdmitted(context.Context, msgr.Message, func()) (msgr.Message, func(), error)
			}); borrowed && ok {
				result, release, err = source.SubmitBorrowedAdmitted(attemptCtx, message, admitted)
			} else {
				result, err = session.raw.SubmitAdmitted(attemptCtx, message, admitted)
			}
			unlock.Do(session.mu.Unlock)
			session.mu.Lock()
			session.removeSubmission(id)
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
		waiters, changed := session.subscribeBackoff(pg)
		session.mu.Unlock()
		if err := session.waitBackoff(ctx, pg, waiters, changed); err != nil {
			return msgr.Message{}, noRelease, err
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
		for _, backoff := range session.backoffsByPG[pg] {
			if backoff.Contains(object) {
				blocked = true
				break
			}
		}
		if !blocked {
			session.mu.Unlock()
			return nil
		}
		waiters, changed := session.subscribeBackoff(pg)
		session.mu.Unlock()
		if err := session.waitBackoff(ctx, pg, waiters, changed); err != nil {
			return err
		}
	}
}

func (session *osdSession) subscribeBackoff(pg maps.PG) (*backoffWaiters, <-chan struct{}) {
	if session.backoffWaiters == nil {
		session.backoffWaiters = make(map[maps.PG]*backoffWaiters)
	}
	waiters := session.backoffWaiters[pg]
	if waiters == nil {
		waiters = &backoffWaiters{changed: make(chan struct{})}
		session.backoffWaiters[pg] = waiters
	}
	waiters.count++
	return waiters, waiters.changed
}

func (session *osdSession) waitBackoff(ctx context.Context, pg maps.PG, waiters *backoffWaiters, changed <-chan struct{}) error {
	defer func() {
		session.mu.Lock()
		waiters.count--
		if waiters.count == 0 {
			delete(session.backoffWaiters, pg)
		}
		session.mu.Unlock()
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-session.raw.Done():
		return session.failure(msgr.ErrSessionClosed)
	case <-changed:
		return nil
	}
}

func (session *osdSession) wakeBackoffPG(pg maps.PG) {
	if waiters := session.backoffWaiters[pg]; waiters != nil {
		close(waiters.changed)
		waiters.changed = make(chan struct{})
	}
}

func (session *osdSession) wakeAllBackoffs() {
	for pg := range session.backoffWaiters {
		session.wakeBackoffPG(pg)
	}
}

func (session *osdSession) addSubmission(id uint64, submission *targetSubmission) {
	session.removeSubmission(id)
	if session.submissionsByPG == nil {
		session.submissionsByPG = make(map[maps.PG]*targetSubmission)
	}
	submission.previous, submission.next = nil, session.submissionsByPG[submission.pg]
	if submission.next != nil {
		submission.next.previous = submission
	}
	session.submissions[id] = submission
	session.submissionsByPG[submission.pg] = submission
}

func (session *osdSession) removeSubmission(id uint64) {
	submission := session.submissions[id]
	if submission == nil {
		return
	}
	delete(session.submissions, id)
	if submission.previous == nil {
		session.submissionsByPG[submission.pg] = submission.next
	} else {
		submission.previous.next = submission.next
	}
	if submission.next != nil {
		submission.next.previous = submission.previous
	}
	submission.previous, submission.next = nil, nil
	if session.submissionsByPG[submission.pg] == nil {
		delete(session.submissionsByPG, submission.pg)
	}
}

func backoffCharge(backoff osd.Backoff) uint64 {
	return 1024 + uint64(len(backoff.Begin.Key)) + uint64(len(backoff.Begin.Object)) + uint64(len(backoff.Begin.Namespace)) + uint64(len(backoff.End.Key)) + uint64(len(backoff.End.Object)) + uint64(len(backoff.End.Namespace))
}

func (session *osdSession) addBackoff(backoff osd.Backoff) error {
	count, bytes := session.maxBackoffs, session.maxBackoffBytes
	if count == 0 {
		count = 4096
	}
	if bytes == 0 {
		bytes = 8 << 20
	}
	previous, exists := session.backoffs[backoff.ID]
	retained := session.backoffBytes
	if exists {
		retained -= backoffCharge(previous)
	}
	charge := backoffCharge(backoff)
	if !exists && len(session.backoffs) >= count || retained > bytes || charge > bytes-retained {
		return fmt.Errorf("active backoff state: %w", msgr.ErrQueueSaturated)
	}
	session.removeBackoff(backoff.ID)
	if session.backoffsByPG == nil {
		session.backoffsByPG = make(map[maps.PG]map[uint64]osd.Backoff)
	}
	if session.backoffsByPG[backoff.PG] == nil {
		session.backoffsByPG[backoff.PG] = make(map[uint64]osd.Backoff)
	}
	session.backoffs[backoff.ID] = backoff
	session.backoffBytes += charge
	session.backoffsByPG[backoff.PG][backoff.ID] = backoff
	session.wakeBackoffPG(backoff.PG)
	return nil
}

func (session *osdSession) removeBackoff(id uint64) (osd.Backoff, bool) {
	backoff, exists := session.backoffs[id]
	if exists {
		session.backoffBytes -= backoffCharge(backoff)
		delete(session.backoffs, id)
		ranges := session.backoffsByPG[backoff.PG]
		delete(ranges, id)
		if len(ranges) == 0 {
			delete(session.backoffsByPG, backoff.PG)
		}
		session.wakeBackoffPG(backoff.PG)
	}
	return backoff, exists
}

func (session *osdSession) receive() {
	defer func() {
		session.ackCancel()
		session.ackMu.Lock()
		generation := session.ackGeneration + 1
		session.ackMu.Unlock()
		session.resetACKs(generation)
		<-session.ackDone
		session.fail(msgr.ErrSessionClosed)
		session.mu.Lock()
		session.backoffs = nil
		session.backoffsByPG = nil
		session.backoffBytes = 0
		session.mu.Unlock()
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
		clear(session.backoffsByPG)
		session.backoffBytes = 0
		session.wakeAllBackoffs()
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
				session.wakeAllBackoffs()
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
					err = session.addBackoff(backoff)
					if err == nil {
						err = session.enqueueACK(ack)
					}
				} else {
					blocked, ok := session.removeBackoff(backoff.ID)
					if ok {
						for submission := session.submissionsByPG[blocked.PG]; submission != nil; submission = submission.next {
							if blocked.Contains(submission.object) {
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
				session.wakeAllBackoffs()
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
		session.wakeAllBackoffs()
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
