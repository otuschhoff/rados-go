package rados

import (
	"context"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/otuschhoff/go-librados/internal/objecter"
	"github.com/otuschhoff/go-librados/internal/osd"
)

type LockMode uint8

const (
	LockExclusive LockMode = LockMode(osd.LockTypeExclusive)
	LockShared    LockMode = LockMode(osd.LockTypeShared)
)

type LockOptions struct {
	Cookie      string
	Tag         string
	Description string
	Duration    time.Duration
	Renew       bool
}

type Locker struct {
	Client      string
	Cookie      string
	Address     string
	Description string
	Expiration  time.Time
	Mode        LockMode
	Tag         string
}

type WatchEvent struct {
	NotifyID uint64
	Cookie   uint64
	Notifier uint64
	Data     []byte
}

type Watcher struct {
	Client  string
	Address string
	Cookie  uint64
	Timeout time.Duration
}

type NotifyAcknowledgment struct {
	Client uint64
	Cookie uint64
	Data   []byte
}

type NotifyTimeout struct {
	Client uint64
	Cookie uint64
}

type NotifyReply struct {
	Acknowledged []NotifyAcknowledgment
	TimedOut     []NotifyTimeout
}

// MaxWatchQueue is the largest event queue accepted by Watch.
const MaxWatchQueue = uint32(65536)

type Watch struct {
	inner  *objecter.Watch
	events chan WatchEvent
	errors chan error
	object ObjectRef
}

func (watch *Watch) Cookie() uint64 {
	if watch == nil || watch.inner == nil {
		return 0
	}
	return watch.inner.Cookie()
}

func (object ObjectRef) Watch(ctx context.Context, queue uint32) (*Watch, <-chan WatchEvent, error) {
	if queue == 0 || queue > MaxWatchQueue {
		return nil, nil, object.invalidOperation("watch")
	}
	objects, operationCtx, cancel, err := object.begin(ctx)
	if err != nil {
		return nil, nil, object.wrapBeginError("watch", err)
	}
	defer cancel()
	timeout := durationSeconds(object.pool.client.config.OperationTimeout)
	inner, err := objects.Watch(operationCtx, object.target(), queue, timeout)
	if err != nil {
		return nil, nil, object.pool.client.wrapError("watch", object.safeTarget(), err)
	}
	watch := &Watch{inner: inner, events: make(chan WatchEvent), errors: make(chan error, 1), object: object}
	if !object.pool.client.startWorker() {
		_ = inner.Close(context.Background())
		return nil, nil, object.pool.client.wrapError("watch", object.safeTarget(), ErrClosed)
	}
	go func() {
		defer object.pool.client.workers.Done()
		watch.forward()
	}()
	return watch, watch.events, nil
}

func (watch *Watch) Ack(ctx context.Context, notifyID uint64, data []byte) error {
	if watch == nil || watch.inner == nil {
		return ErrInvalidArgument
	}
	if _, _, done, err := watch.object.pool.client.beginOperation(); err != nil {
		return watch.object.pool.client.wrapError("ack watch", watch.object.safeTarget(), err)
	} else {
		defer done()
	}
	operationCtx, cancel := watch.object.pool.client.operationContext(ctx)
	defer cancel()
	err := watch.inner.Ack(operationCtx, notifyID, data)
	return watch.object.pool.client.wrapError("ack watch", watch.object.safeTarget(), err)
}

func (watch *Watch) Close(ctx context.Context) error {
	if watch == nil || watch.inner == nil {
		return ErrInvalidArgument
	}
	if _, _, done, err := watch.object.pool.client.beginOperation(); err != nil {
		return watch.object.pool.client.wrapError("close watch", watch.object.safeTarget(), err)
	} else {
		defer done()
	}
	operationCtx, cancel := watch.object.pool.client.operationContext(ctx)
	defer cancel()
	err := watch.inner.Close(operationCtx)
	return watch.object.pool.client.wrapError("close watch", watch.object.safeTarget(), err)
}

func (watch *Watch) Errors() <-chan error {
	if watch == nil || watch.inner == nil {
		return nil
	}
	return watch.errors
}

func (watch *Watch) Done() <-chan struct{} {
	if watch == nil || watch.inner == nil {
		return nil
	}
	return watch.inner.Done()
}

func (watch *Watch) forward() {
	defer close(watch.events)
	defer close(watch.errors)
	for {
		select {
		case event, ok := <-watch.inner.Events():
			if !ok {
				return
			}
			select {
			case <-watch.inner.Done():
				return
			default:
			}
			converted := WatchEvent{NotifyID: event.NotifyID, Cookie: event.Cookie, Notifier: event.NotifierGID, Data: append([]byte(nil), event.Data...)}
			select {
			case watch.events <- converted:
			case <-watch.inner.Done():
				return
			}
		case err := <-watch.inner.Errors():
			watch.forwardError(err)
		case <-watch.inner.Done():
			select {
			case err := <-watch.inner.Errors():
				watch.forwardError(err)
			default:
			}
			return
		}
	}
}

func (watch *Watch) forwardError(err error) {
	if err == nil {
		return
	}
	wrapped := watch.object.pool.client.wrapError("watch", watch.object.safeTarget(), err)
	select {
	case watch.errors <- wrapped:
	default:
	}
}

func (object ObjectRef) Notify(ctx context.Context, data []byte) (NotifyReply, error) {
	if object.pool.client == nil {
		return NotifyReply{}, object.wrapBeginError("notify", &OpError{Err: ErrInvalidArgument})
	}
	_, objects, done, err := object.pool.client.beginOperation()
	if err != nil {
		return NotifyReply{}, object.wrapBeginError("notify", err)
	}
	defer done()
	serverTimeout := object.pool.client.config.OperationTimeout
	if ctx == nil {
		ctx = context.Background()
	}
	operationCtx, cancel := context.WithTimeout(ctx, serverTimeout+time.Second)
	defer cancel()
	notification, notifyErr := objects.Notify(operationCtx, object.target(), append([]byte(nil), data...), durationSeconds(serverTimeout))
	if notifyErr != nil && len(notification.Data) == 0 {
		return NotifyReply{}, object.pool.client.wrapError("notify", object.safeTarget(), notifyErr)
	}
	reply, decodeErr := decodeNotifyReply(notification.Data)
	if decodeErr != nil {
		return reply, object.pool.client.wrapError("notify", object.safeTarget(), decodeErr)
	}
	if notifyErr != nil {
		return reply, object.pool.client.wrapError("notify", object.safeTarget(), notifyErr)
	}
	return reply, nil
}

func decodeNotifyReply(data []byte) (NotifyReply, error) {
	acknowledged, timedOut, err := osd.DecodeNotifyResult(data, decodeLimit(data), uint32(len(data)/16+1))
	reply := NotifyReply{Acknowledged: make([]NotifyAcknowledgment, len(acknowledged)), TimedOut: make([]NotifyTimeout, len(timedOut))}
	for index, item := range acknowledged {
		reply.Acknowledged[index] = NotifyAcknowledgment{Client: item.Client, Cookie: item.Cookie, Data: append([]byte(nil), item.Data...)}
	}
	for index, item := range timedOut {
		reply.TimedOut[index] = NotifyTimeout{Client: item.Client, Cookie: item.Cookie}
	}
	return reply, err
}

func (object ObjectRef) ListWatchers(ctx context.Context) ([]Watcher, error) {
	result, err := object.executeRead(ctx, "list watchers", []osd.Operation{{Code: osd.OpListWatchers}})
	if err != nil {
		return nil, err
	}
	data := result.Results[0].Data
	items, err := osd.DecodeWatchers(data, decodeLimit(data), uint32(len(data)/17+1))
	if err != nil {
		return nil, object.pool.client.wrapError("list watchers", object.safeTarget(), err)
	}
	watchers := make([]Watcher, len(items))
	for index, item := range items {
		watchers[index] = Watcher{Client: osd.FormatClient(item.Client), Address: item.Address, Cookie: item.Cookie, Timeout: time.Duration(item.TimeoutSeconds) * time.Second}
	}
	return watchers, nil
}

func durationSeconds(duration time.Duration) uint32 {
	seconds := duration / time.Second
	if duration%time.Second != 0 {
		seconds++
	}
	if seconds <= 0 {
		return 1
	}
	if seconds > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(seconds)
}

func (object ObjectRef) Lock(ctx context.Context, name string, mode LockMode, options LockOptions) error {
	if object.pool.client == nil {
		return object.wrapBeginError("lock", &OpError{Err: ErrInvalidArgument})
	}
	if mode != LockExclusive && mode != LockShared {
		return object.invalidOperation("lock")
	}
	payload, err := osd.EncodeLockRequest(name, uint8(mode), options.Cookie, options.Tag, options.Description, options.Duration, options.Renew, math.MaxUint32)
	if err != nil {
		return object.invalidOperationWith("lock", err)
	}
	op := NewWriteOp()
	op.Exec("lock", "lock", payload)
	_, err = object.ExecuteWrite(ctx, op)
	return object.pool.client.wrapError("lock", object.safeTarget(), err)
}

func (object ObjectRef) Unlock(ctx context.Context, name, cookie string) error {
	if object.pool.client == nil {
		return object.wrapBeginError("unlock", &OpError{Err: ErrInvalidArgument})
	}
	payload, err := osd.EncodeUnlockRequest(name, cookie, math.MaxUint32)
	if err != nil {
		return object.invalidOperationWith("unlock", err)
	}
	op := NewWriteOp()
	op.Exec("lock", "unlock", payload)
	_, err = object.ExecuteWrite(ctx, op)
	return object.pool.client.wrapError("unlock", object.safeTarget(), err)
}

func (object ObjectRef) ListLockers(ctx context.Context, name string) ([]Locker, error) {
	if object.pool.client == nil {
		return nil, object.wrapBeginError("list lockers", &OpError{Err: ErrInvalidArgument})
	}
	payload, err := osd.EncodeGetLockInfoRequest(name, math.MaxUint32)
	if err != nil {
		return nil, object.invalidOperationWith("list lockers", err)
	}
	result, err := object.Exec(ctx, "lock", "get_info", payload)
	if err != nil {
		return nil, object.pool.client.wrapError("list lockers", object.safeTarget(), err)
	}
	info, err := osd.DecodeLockInfo(result.Data, decodeLimit(result.Data), uint32(len(result.Data)/13+1))
	if err != nil {
		return nil, object.pool.client.wrapError("list lockers", object.safeTarget(), err)
	}
	lockers := make([]Locker, len(info.Holders))
	for index, holder := range info.Holders {
		lockers[index] = Locker{Client: osd.FormatClient(holder.Client), Cookie: holder.Cookie, Address: holder.Address, Description: holder.Description, Expiration: holder.Expiration, Mode: LockMode(info.LockType), Tag: info.Tag}
	}
	return lockers, nil
}

func (object ObjectRef) BreakLock(ctx context.Context, name, client, cookie string) error {
	if object.pool.client == nil {
		return object.wrapBeginError("break lock", &OpError{Err: ErrInvalidArgument})
	}
	clientID, ok := parseLockClient(client)
	if !ok {
		return object.invalidOperation("break lock")
	}
	payload, err := osd.EncodeBreakLockRequest(name, clientID, cookie, math.MaxUint32)
	if err != nil {
		return object.invalidOperationWith("break lock", err)
	}
	op := NewWriteOp()
	op.Exec("lock", "break_lock", payload)
	_, err = object.ExecuteWrite(ctx, op)
	return object.pool.client.wrapError("break lock", object.safeTarget(), err)
}

func parseLockClient(value string) (uint64, bool) {
	if !strings.HasPrefix(value, "client.") {
		return 0, false
	}
	client, err := strconv.ParseUint(strings.TrimPrefix(value, "client."), 10, 63)
	return client, err == nil
}
