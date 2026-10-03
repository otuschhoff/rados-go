package msgr

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type requestTimingKey struct{}

type requestAttemptsKey struct{}

type RequestAttempts struct {
	prepared   atomic.Uint64
	dispatched atomic.Uint64
	replayed   atomic.Uint64
}

func WithRequestAttempts(ctx context.Context) (context.Context, *RequestAttempts) {
	if ctx == nil {
		ctx = context.Background()
	}
	observation := &RequestAttempts{}
	return context.WithValue(ctx, requestAttemptsKey{}, observation), observation
}

func RecordPreparedRequest(ctx context.Context) {
	if observation, _ := ctx.Value(requestAttemptsKey{}).(*RequestAttempts); observation != nil {
		observation.prepared.Add(1)
	}
}

func recordRequestDispatch(ctx context.Context, replay bool) {
	if observation, _ := ctx.Value(requestAttemptsKey{}).(*RequestAttempts); observation != nil {
		observation.dispatched.Add(1)
		if replay {
			observation.replayed.Add(1)
		}
	}
}

func (observation *RequestAttempts) RetryCount() (uint64, bool) {
	prepared, dispatched, replayed := observation.prepared.Load(), observation.dispatched.Load(), observation.replayed.Load()
	if prepared == 0 || dispatched < replayed || dispatched-replayed != prepared || prepared-1 > ^uint64(0)-replayed {
		return 0, false
	}
	return prepared - 1 + replayed, true
}

type RequestTimingEvent struct {
	Stage         string    `json:"stage"`
	TransactionID uint64    `json:"transaction_id"`
	At            time.Time `json:"at"`
}

type RequestTiming struct {
	mu            sync.Mutex
	events        []RequestTimingEvent
	transactionID uint64
}

func WithRequestTiming(ctx context.Context) (context.Context, *RequestTiming) {
	timing := &RequestTiming{events: make([]RequestTimingEvent, 0, 8)}
	timing.Record("read_enter", 0, time.Now())
	return context.WithValue(ctx, requestTimingKey{}, timing), timing
}

func (timing *RequestTiming) Record(stage string, transactionID uint64, at time.Time) {
	timing.mu.Lock()
	if transactionID == 0 {
		transactionID = timing.transactionID
	}
	timing.events = append(timing.events, RequestTimingEvent{Stage: stage, TransactionID: transactionID, At: at})
	timing.mu.Unlock()
}

func (timing *RequestTiming) bindTransaction(transactionID uint64) {
	timing.mu.Lock()
	timing.transactionID = transactionID
	for index := range timing.events {
		if timing.events[index].TransactionID == 0 {
			timing.events[index].TransactionID = transactionID
		}
	}
	timing.mu.Unlock()
}

func (timing *RequestTiming) Events() []RequestTimingEvent {
	timing.mu.Lock()
	defer timing.mu.Unlock()
	return append([]RequestTimingEvent(nil), timing.events...)
}

func requestTiming(ctx context.Context) *RequestTiming {
	timing, _ := ctx.Value(requestTimingKey{}).(*RequestTiming)
	return timing
}

func recordRequestTiming(ctx context.Context, stage string, transactionID uint64) {
	if timing := requestTiming(ctx); timing != nil {
		timing.Record(stage, transactionID, time.Now())
	}
}
