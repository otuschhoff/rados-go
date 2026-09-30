package msgr

import (
	"context"
	"sync"
	"time"
)

type requestTimingKey struct{}

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
