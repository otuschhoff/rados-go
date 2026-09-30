package objecter

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/otuschhoff/rados-go/internal/maps"
	"github.com/otuschhoff/rados-go/internal/msgr"
	"github.com/otuschhoff/rados-go/internal/osd"
	"github.com/otuschhoff/rados-go/internal/protocol"
)

type osdSession struct {
	raw        osdTransport
	limits     osd.Limits
	ackTimeout time.Duration

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

func (session *osdSession) SubmitTarget(ctx context.Context, pg maps.PG, object osd.HObject, message msgr.Message) (msgr.Message, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		session.mu.Lock()
		if session.err != nil {
			err := session.err
			session.mu.Unlock()
			return msgr.Message{}, err
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
			result, err := session.raw.SubmitAdmitted(attemptCtx, message, func() { unlock.Do(session.mu.Unlock) })
			unlock.Do(session.mu.Unlock)
			session.mu.Lock()
			delete(session.submissions, id)
			resend := submission.resend
			session.mu.Unlock()
			cancel()
			if resend && ctx.Err() == nil {
				continue
			}
			if err != nil {
				err = errors.Join(err, session.failure(err))
			}
			return result, err
		}
		changed := session.changed
		session.mu.Unlock()
		select {
		case <-ctx.Done():
			return msgr.Message{}, ctx.Err()
		case <-session.raw.Done():
			return msgr.Message{}, session.failure(msgr.ErrSessionClosed)
		case <-changed:
		}
	}
}

func (session *osdSession) Stop() {
	session.raw.Stop()
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
	defer close(session.notifications)
	events := session.raw.Events()
	wasUnavailable := false
	for {
		select {
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
			backoff, err := osd.DecodeBackoff(message, session.limits)
			if err != nil {
				session.fail(err)
				session.raw.Stop()
				return
			}
			if backoff.Operation == osd.BackoffBlock {
				session.update(func() { session.backoffs[backoff.ID] = backoff })
				ack, err := osd.EncodeBackoffAcknowledgment(backoff, session.limits)
				if err == nil {
					ctx, cancel := context.WithTimeout(context.Background(), session.ackTimeout)
					err = session.raw.Send(ctx, ack)
					cancel()
				}
				if err != nil {
					session.fail(err)
					session.raw.Stop()
					return
				}
			} else {
				session.update(func() {
					blocked, ok := session.backoffs[backoff.ID]
					delete(session.backoffs, backoff.ID)
					if !ok {
						return
					}
					for _, submission := range session.submissions {
						if submission.pg == blocked.PG && blocked.Contains(submission.object) {
							submission.resend = true
							submission.cancel()
						}
					}
				})
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
			wasUnavailable = true
			session.update(func() { clear(session.backoffs) })
			if session.observe != nil {
				session.observe(false, msgr.ErrSessionDisconnected)
			}
			select {
			case session.interruptions <- msgr.ErrSessionDisconnected:
			default:
			}
		case event, ok := <-events:
			if !ok {
				events = nil
				continue
			}
			recovered := event.Kind == msgr.EventReconnectOK || event.Kind == msgr.EventStateChanged && event.State == msgr.StateReady
			if wasUnavailable && recovered && session.observe != nil {
				wasUnavailable = false
				session.observe(true, nil)
			}
		}
	}
}

func (session *osdSession) update(change func()) {
	session.mu.Lock()
	change()
	close(session.changed)
	session.changed = make(chan struct{})
	session.mu.Unlock()
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
