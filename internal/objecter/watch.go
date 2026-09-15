package objecter

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"sync"
	"time"

	wire "github.com/otuschhoff/go-librados/internal/encoding"
	"github.com/otuschhoff/go-librados/internal/msgr"
	"github.com/otuschhoff/go-librados/internal/osd"
	"github.com/otuschhoff/go-librados/internal/protocol"
)

const maxWatchQueue = uint32(65536)

type Watch struct {
	client    *Client
	target    Target
	cookie    uint64
	timeout   uint32
	primaryMu sync.RWMutex
	primary   int32

	events    chan osd.WatchNotification
	errors    chan error
	done      chan struct{}
	reconnect chan struct{}
	stopOnce  sync.Once
	ops       chan struct{}
	unwatched bool
	workers   sync.WaitGroup
}

func (watch *Watch) Cookie() uint64                       { return watch.cookie }
func (watch *Watch) Events() <-chan osd.WatchNotification { return watch.events }
func (watch *Watch) Errors() <-chan error                 { return watch.errors }
func (watch *Watch) Done() <-chan struct{}                { return watch.done }

func (watch *Watch) primaryOSD() int32 {
	watch.primaryMu.RLock()
	defer watch.primaryMu.RUnlock()
	return watch.primary
}

func (watch *Watch) setPrimary(osdID int32) {
	watch.primaryMu.Lock()
	watch.primary = osdID
	watch.primaryMu.Unlock()
}

func (client *Client) Watch(ctx context.Context, target Target, queue, timeout uint32) (*Watch, error) {
	if queue == 0 || queue > maxWatchQueue || target.Snapshot != osd.NoSnap {
		return nil, wire.ErrMalformed
	}
	cookie, err := randomWatchCookie()
	if err != nil {
		return nil, err
	}
	route, err := client.config.Router.Route(target)
	if err != nil {
		return nil, err
	}
	watch := &Watch{client: client, target: target, cookie: cookie, timeout: timeout, primary: route.Primary, events: make(chan osd.WatchNotification, queue), errors: make(chan error, 1), done: make(chan struct{}), reconnect: make(chan struct{}, 1), ops: make(chan struct{}, 1)}
	watch.ops <- struct{}{}
	client.mu.Lock()
	if client.closed {
		client.mu.Unlock()
		return nil, ErrClosed
	}
	client.watches[cookie] = watch
	watch.workers.Add(1)
	client.mu.Unlock()
	if err := client.watchOperation(ctx, target, cookie, osd.WatchOperationRegister, 0, timeout); err != nil {
		client.removeWatch(cookie)
		watch.workers.Done()
		return nil, err
	}
	go func() {
		defer watch.workers.Done()
		watch.keepalive()
	}()
	return watch, nil
}

func (watch *Watch) Ack(ctx context.Context, notifyID uint64, data []byte) error {
	payload, err := osd.EncodeNotifyAck(notifyID, watch.cookie, data, watch.client.config.MessageLimits.MaxBytes)
	if err != nil {
		return err
	}
	_, err = watch.client.executeTrackedOutcomeSensitive(ctx, watch.target, osd.Operation{Code: osd.OpNotifyAck, WatchCookie: watch.cookie, Length: uint64(len(payload)), Data: payload})
	return err
}

func (watch *Watch) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	watch.stopOnce.Do(func() {
		watch.client.removeWatch(watch.cookie)
		close(watch.done)
	})
	select {
	case <-watch.ops:
		defer func() { watch.ops <- struct{}{} }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if watch.unwatched {
		return nil
	}
	err := watch.client.watchOperation(ctx, watch.target, watch.cookie, osd.WatchOperationUnwatch, 0, 0)
	if err == nil {
		watch.unwatched = true
	}
	return err
}

func (watch *Watch) stop(err error) {
	watch.stopOnce.Do(func() {
		if err != nil {
			select {
			case watch.errors <- err:
			default:
			}
		}
		close(watch.done)
	})
}

func (watch *Watch) wait() { watch.workers.Wait() }

func (watch *Watch) keepalive() {
	period := time.Duration(watch.timeout) * time.Second / 3
	if period <= 0 {
		period = 10 * time.Second
	}
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	generation := uint32(0)
	for {
		forceReconnect := false
		select {
		case <-watch.done:
			return
		case <-ticker.C:
		case <-watch.reconnect:
			forceReconnect = true
		}
		<-watch.ops
		select {
		case <-watch.done:
			watch.ops <- struct{}{}
			return
		default:
		}
		route, routeErr := watch.client.config.Router.Route(watch.target)
		routeChanged := routeErr == nil && route.Primary != watch.primaryOSD()
		if routeChanged {
			watch.reportInterruption(ErrStaleMap)
		}
		if forceReconnect || routeChanged {
			generation++
			ctx, cancel := context.WithTimeout(context.Background(), watch.client.config.RefreshWait*time.Duration(watch.client.config.MaxAttempts))
			err := watch.client.watchOperation(ctx, watch.target, watch.cookie, osd.WatchOperationReconnect, generation, watch.timeout)
			cancel()
			if err == nil {
				if routeErr == nil {
					watch.setPrimary(route.Primary)
				}
				watch.ops <- struct{}{}
				continue
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), watch.client.config.RefreshWait)
		err := watch.client.watchOperation(ctx, watch.target, watch.cookie, osd.WatchOperationPing, generation, 0)
		cancel()
		if err == nil {
			watch.ops <- struct{}{}
			continue
		}
		watch.reportInterruption(err)
		generation++
		ctx, cancel = context.WithTimeout(context.Background(), watch.client.config.RefreshWait*time.Duration(watch.client.config.MaxAttempts))
		err = watch.client.watchOperation(ctx, watch.target, watch.cookie, osd.WatchOperationReconnect, generation, watch.timeout)
		if err != nil {
			err = watch.client.watchOperation(ctx, watch.target, watch.cookie, osd.WatchOperationRegister, 0, watch.timeout)
			generation = 0
		}
		if err == nil {
			if route, routeErr := watch.client.config.Router.Route(watch.target); routeErr == nil {
				watch.setPrimary(route.Primary)
			}
		}
		cancel()
		watch.ops <- struct{}{}
		if err != nil {
			watch.client.removeWatch(watch.cookie)
			watch.stop(err)
			return
		}
	}
}

func (client *Client) watchOperation(ctx context.Context, target Target, cookie uint64, operation uint8, generation, timeout uint32) error {
	op := osd.Operation{Code: osd.OpWatch, WatchCookie: cookie, WatchOperation: operation, WatchGeneration: generation, WatchTimeout: timeout}
	_, err := client.Mutate(ctx, target, op)
	return err
}

func (client *Client) Notify(ctx context.Context, target Target, data []byte, timeout uint32) (notification osd.WatchNotification, notifyErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	cookie, err := randomWatchCookie()
	if err != nil {
		return osd.WatchNotification{}, err
	}
	payload, err := osd.EncodeNotifyRequest(timeout, data, client.config.MessageLimits.MaxBytes)
	if err != nil {
		return osd.WatchNotification{}, err
	}
	completion := make(chan notifyCompletion, 1)
	client.mu.Lock()
	if client.closed {
		client.mu.Unlock()
		return osd.WatchNotification{}, ErrClosed
	}
	client.notifies[cookie] = completion
	client.mu.Unlock()
	defer client.removeNotify(cookie)
	operation := osd.Operation{Code: osd.OpNotify, WatchCookie: cookie, Length: uint64(len(payload)), Data: payload}
	sequence, transactionID, operation, err := client.admitMutation(ctx, operation)
	if err != nil {
		return osd.WatchNotification{}, err
	}
	defer func() { client.completeMutation(sequence, notifyErr) }()
	result, err := client.executeRoutedOperations(ctx, target, []osd.Operation{operation}, transactionID, true, false, 0, client.config.Router.Route)
	if err != nil {
		return osd.WatchNotification{}, err
	}
	if len(result.Data) != 8 {
		return osd.WatchNotification{}, errors.Join(msgr.ErrOutcomeUnknown, osd.ErrMalformedReply)
	}
	notifyID := binary.LittleEndian.Uint64(result.Data)
	select {
	case completed := <-completion:
		if completed.err != nil {
			return osd.WatchNotification{}, completed.err
		}
		notification := completed.notification
		if notification.Result < 0 {
			return notification, protocol.WireErrno(notification.Result)
		}
		if notification.NotifyID != notifyID {
			return notification, errors.Join(msgr.ErrOutcomeUnknown, osd.ErrMalformedReply)
		}
		return notification, nil
	case <-ctx.Done():
		return osd.WatchNotification{}, errors.Join(msgr.ErrOutcomeUnknown, ctx.Err())
	}
}

func (client *Client) dispatchNotifications(osdID int32, active session, source notificationSession) {
	for {
		var notification osd.WatchNotification
		var ok bool
		select {
		case <-client.done:
			return
		case notification, ok = <-source.Notifications():
			if !ok {
				client.interruptSessionWatches(osdID, active, source.NotificationError())
				return
			}
		}
		client.mu.Lock()
		if notification.Opcode == osd.WatchEventComplete {
			completion := client.notifies[notification.Cookie]
			client.mu.Unlock()
			if completion != nil {
				select {
				case completion <- notifyCompletion{notification: notification}:
				default:
				}
			}
			continue
		}
		watch := client.watches[notification.Cookie]
		client.mu.Unlock()
		if watch == nil {
			continue
		}
		if notification.Opcode == osd.WatchEventDisconnect {
			watch.interrupt(msgr.ErrSessionClosed)
			continue
		}
		select {
		case <-watch.done:
			continue
		default:
		}
		select {
		case watch.events <- notification:
		case <-watch.done:
		default:
			client.removeWatch(notification.Cookie)
			watch.stop(errors.Join(ErrWatchInterrupted, msgr.ErrQueueSaturated))
		}
	}
}

func (client *Client) interruptSessionWatches(osdID int32, active session, err error) {
	client.mu.Lock()
	current, ok := client.sessions[osdID]
	if client.closed || !ok || current.session != active {
		client.mu.Unlock()
		return
	}
	delete(client.sessions, osdID)
	watches := make([]*Watch, 0)
	for _, watch := range client.watches {
		if watch.primaryOSD() == osdID {
			watches = append(watches, watch)
		}
	}
	client.mu.Unlock()
	active.Stop()
	for _, watch := range watches {
		watch.interrupt(err)
	}
}

func (watch *Watch) interrupt(err error) {
	watch.reportInterruption(err)
	select {
	case watch.reconnect <- struct{}{}:
	default:
	}
}

func (watch *Watch) reportInterruption(err error) {
	select {
	case watch.errors <- errors.Join(ErrWatchInterrupted, err):
	default:
	}
}

func (client *Client) removeWatch(cookie uint64) {
	client.mu.Lock()
	if client.watches != nil {
		delete(client.watches, cookie)
	}
	client.mu.Unlock()
}

func (client *Client) removeNotify(cookie uint64) {
	client.mu.Lock()
	if client.notifies != nil {
		delete(client.notifies, cookie)
	}
	client.mu.Unlock()
}

func randomWatchCookie() (uint64, error) {
	for {
		var data [8]byte
		if _, err := rand.Read(data[:]); err != nil {
			return 0, err
		}
		if cookie := binary.LittleEndian.Uint64(data[:]); cookie != 0 {
			return cookie, nil
		}
	}
}
