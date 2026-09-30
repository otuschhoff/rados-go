package objecter

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/otuschhoff/rados-go/internal/maps"
	"github.com/otuschhoff/rados-go/internal/msgr"
	"github.com/otuschhoff/rados-go/internal/osd"
	"github.com/otuschhoff/rados-go/internal/protocol"
)

const lifecycleTimeout = 3 * time.Second

type lifecycleGate struct {
	entered     chan struct{}
	release     chan struct{}
	enterOnce   sync.Once
	releaseOnce sync.Once
}

func newLifecycleGate() *lifecycleGate {
	return &lifecycleGate{entered: make(chan struct{}), release: make(chan struct{})}
}

func (gate *lifecycleGate) block() {
	gate.enterOnce.Do(func() { close(gate.entered) })
	<-gate.release
}

func (gate *lifecycleGate) unblock() {
	gate.releaseOnce.Do(func() { close(gate.release) })
}

func lifecycleReceive[Value any](t *testing.T, channel <-chan Value, label string) Value {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-time.After(lifecycleTimeout):
		t.Fatalf("timed out waiting for %s", label)
		var zero Value
		return zero
	}
}

func lifecycleCleanup(t *testing.T, client *Client, gates ...*lifecycleGate) {
	t.Helper()
	t.Cleanup(func() {
		for _, gate := range gates {
			gate.unblock()
		}
		closed := lifecycleClose(client)
		select {
		case <-closed:
		case <-time.After(lifecycleTimeout):
			t.Error("cleanup Close failed to join workers after releasing all gates")
		}
	})
}

func lifecycleClose(client *Client) <-chan struct{} {
	closed := make(chan struct{})
	go func() { client.Close(); close(closed) }()
	return closed
}

func lifecycleGet(t *testing.T, client *Client, id int32, addresses protocol.EntityAddrVec) (session, error) {
	t.Helper()
	result := make(chan lifecycleResult, 1)
	go func() {
		active, err := client.getSession(id, addresses)
		result <- lifecycleResult{active, err}
	}()
	value := lifecycleReceive(t, result, "session acquisition")
	return value.active, value.err
}

func lifecycleInvalidate(t *testing.T, client *Client, id int32, active session) {
	t.Helper()
	done := make(chan struct{})
	go func() { client.invalidate(id, active); close(done) }()
	lifecycleReceive(t, done, "session invalidation")
}

func lifecycleBlocked(t *testing.T, channel <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-channel:
		t.Fatalf("%s returned before gated cleanup completed", label)
	case <-time.After(20 * time.Millisecond):
	}
}

type lifecycleResult struct {
	active session
	err    error
}

type lifecycleWaiters struct {
	client  *Client
	id      int32
	pending *sessionCreation
	count   int
	changed chan struct{}
}

type lifecycleContext struct {
	context.Context
	waiters *lifecycleWaiters
	once    sync.Once
}

func (ctx *lifecycleContext) Done() <-chan struct{} {
	ctx.once.Do(func() {
		ctx.waiters.client.mu.Lock()
		pending := ctx.waiters.client.creations[ctx.waiters.id]
		if ctx.waiters.pending == nil {
			ctx.waiters.pending = pending
		}
		if pending != nil && pending == ctx.waiters.pending {
			ctx.waiters.count++
		}
		ctx.waiters.client.mu.Unlock()
		ctx.waiters.changed <- struct{}{}
	})
	return ctx.Context.Done()
}

func newLifecycleWaiters(client *Client, id int32) *lifecycleWaiters {
	return &lifecycleWaiters{client: client, id: id, changed: make(chan struct{}, 64)}
}

func (waiters *lifecycleWaiters) start(ctx context.Context, addresses protocol.EntityAddrVec) <-chan lifecycleResult {
	result := make(chan lifecycleResult, 1)
	go func() {
		active, err := waiters.client.getSessionContext(&lifecycleContext{Context: ctx, waiters: waiters}, waiters.id, addresses)
		result <- lifecycleResult{active: active, err: err}
	}()
	return result
}

func (waiters *lifecycleWaiters) joined(t *testing.T, want int) {
	t.Helper()
	timer := time.NewTimer(lifecycleTimeout)
	defer timer.Stop()
	for {
		waiters.client.mu.Lock()
		count := waiters.count
		pending := waiters.client.creations[waiters.id]
		valid := pending != nil && pending == waiters.pending
		waiters.client.mu.Unlock()
		if count == want && valid {
			return
		}
		select {
		case <-waiters.changed:
		case <-timer.C:
			t.Fatalf("waiters joined=%d want=%d same pending=%t", count, want, valid)
		}
	}
}

func lifecycleWant(t *testing.T, result lifecycleResult, active session, err error) {
	t.Helper()
	if result.active != active || (err == nil && result.err != nil) || (err != nil && !errors.Is(result.err, err)) {
		t.Fatalf("session=%p error=%v; want session=%p error=%v", result.active, result.err, active, err)
	}
}

func lifecycleEmpty(t *testing.T, client *Client) {
	t.Helper()
	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.sessions) != 0 || len(client.creations) != 0 {
		t.Fatalf("sessions=%d creations=%d after cleanup", len(client.sessions), len(client.creations))
	}
}

func lifecycleObserve(client *Client) <-chan OSDSessionEvent {
	events := make(chan OSDSessionEvent, 16)
	client.config.ObserveSession = func(event OSDSessionEvent) { events <- event }
	return events
}

func lifecycleNoEvent(t *testing.T, events <-chan OSDSessionEvent) {
	t.Helper()
	select {
	case event := <-events:
		t.Fatalf("unexpected session observer event: %+v", event)
	default:
	}
}

func lifecycleAvailable(t *testing.T, events <-chan OSDSessionEvent) {
	t.Helper()
	event := lifecycleReceive(t, events, "installed session availability")
	if event.OSDID != 0 || !event.Available || event.Err != nil {
		t.Fatalf("availability event=%+v", event)
	}
}

func TestSessionLifecycleSharedFactory(t *testing.T) {
	gate := newLifecycleGate()
	active := &creationTestSession{}
	var calls atomic.Int32
	address := protocol.EntityAddrVec{testAddress(t, "192.0.2.10:6800")}
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{}, func(int32, protocol.EntityAddrVec) (session, error) {
		calls.Add(1)
		gate.block()
		return active, nil
	})
	lifecycleCleanup(t, client, gate)
	waiters := newLifecycleWaiters(client, 0)
	const count = 16
	results := make([]<-chan lifecycleResult, count)
	for index := range results {
		results[index] = waiters.start(context.Background(), address)
	}
	lifecycleReceive(t, gate.entered, "factory entry")
	waiters.joined(t, count)
	if calls.Load() != 1 {
		t.Fatalf("factory calls=%d want=1", calls.Load())
	}
	gate.unblock()
	for _, result := range results {
		lifecycleWant(t, lifecycleReceive(t, result, "shared result"), active, nil)
	}
	cached, err := lifecycleGet(t, client, 0, address)
	lifecycleWant(t, lifecycleResult{cached, err}, active, nil)
	if calls.Load() != 1 {
		t.Fatalf("factory retried after shared success: %d", calls.Load())
	}
}

func TestSessionLifecycleCancellationDoesNotCancelCreation(t *testing.T) {
	for _, first := range []bool{false, true} {
		name := "later waiter"
		if first {
			name = "first caller"
		}
		t.Run(name, func(t *testing.T) {
			gate := newLifecycleGate()
			active := &creationTestSession{}
			var calls atomic.Int32
			address := protocol.EntityAddrVec{testAddress(t, "192.0.2.10:6800")}
			client := newTestClient(t, &fakeMapSource{}, &fakeRouter{}, func(int32, protocol.EntityAddrVec) (session, error) {
				calls.Add(1)
				gate.block()
				return active, nil
			})
			lifecycleCleanup(t, client, gate)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			waiters := newLifecycleWaiters(client, 0)
			var canceled, survivor <-chan lifecycleResult
			if first {
				canceled = waiters.start(ctx, address)
			} else {
				survivor = waiters.start(context.Background(), address)
			}
			lifecycleReceive(t, gate.entered, "factory entry")
			waiters.joined(t, 1)
			if first {
				survivor = waiters.start(context.Background(), address)
			} else {
				canceled = waiters.start(ctx, address)
			}
			waiters.joined(t, 2)
			cancel()
			lifecycleWant(t, lifecycleReceive(t, canceled, "canceled waiter"), nil, context.Canceled)
			select {
			case result := <-survivor:
				t.Fatalf("survivor returned while factory gated: %+v", result)
			default:
			}
			gate.unblock()
			lifecycleWant(t, lifecycleReceive(t, survivor, "surviving waiter"), active, nil)
			if calls.Load() != 1 {
				t.Fatalf("factory calls=%d want=1", calls.Load())
			}
		})
	}
}

func TestSessionLifecycleFactoryFailureFanout(t *testing.T) {
	failure := errors.New("lifecycle factory failed")
	for _, kind := range []string{"error", "partial result", "nil result"} {
		t.Run(kind, func(t *testing.T) {
			gate := newLifecycleGate()
			stop := newLifecycleGate()
			var calls, stops atomic.Int32
			partial := &creationTestSession{stop: func() { stops.Add(1); stop.block() }}
			wantErr := failure
			if kind == "nil result" {
				wantErr = msgr.ErrSessionClosed
			}
			address := protocol.EntityAddrVec{testAddress(t, "192.0.2.10:6800")}
			client := newTestClient(t, &fakeMapSource{}, &fakeRouter{}, func(int32, protocol.EntityAddrVec) (session, error) {
				calls.Add(1)
				gate.block()
				if kind == "partial result" {
					return partial, failure
				}
				if kind == "nil result" {
					return nil, nil
				}
				return nil, failure
			})
			lifecycleCleanup(t, client, gate, stop)
			events := lifecycleObserve(client)
			waiters := newLifecycleWaiters(client, 0)
			results := make([]<-chan lifecycleResult, 12)
			for index := range results {
				results[index] = waiters.start(context.Background(), address)
			}
			lifecycleReceive(t, gate.entered, "factory entry")
			waiters.joined(t, len(results))
			gate.unblock()
			if kind == "partial result" {
				lifecycleReceive(t, stop.entered, "partial resource Stop")
				for _, result := range results {
					select {
					case value := <-result:
						t.Fatalf("published before partial Stop: %+v", value)
					default:
					}
				}
				stop.unblock()
			}
			for _, result := range results {
				value := lifecycleReceive(t, result, "factory failure")
				lifecycleWant(t, value, nil, wantErr)
				if value.err != wantErr {
					t.Fatalf("failure identity changed: %v", value.err)
				}
			}
			if calls.Load() != 1 {
				t.Fatalf("joined waiters retried factory: %d", calls.Load())
			}
			wantStops := int32(0)
			if kind == "partial result" {
				wantStops = 1
			}
			if stops.Load() != wantStops {
				t.Fatalf("partial Stops=%d want=%d", stops.Load(), wantStops)
			}
			lifecycleEmpty(t, client)
			lifecycleNoEvent(t, events)
		})
	}
}

func TestSessionLifecycleCloseJoinsFactoryAndLosingResource(t *testing.T) {
	factory, stop := newLifecycleGate(), newLifecycleGate()
	var stops atomic.Int32
	active := &creationTestSession{stop: func() { stops.Add(1); stop.block() }}
	address := protocol.EntityAddrVec{testAddress(t, "192.0.2.10:6800")}
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{}, func(int32, protocol.EntityAddrVec) (session, error) {
		factory.block()
		return active, nil
	})
	lifecycleCleanup(t, client, factory, stop)
	events := lifecycleObserve(client)
	waiters := newLifecycleWaiters(client, 0)
	result := waiters.start(context.Background(), address)
	lifecycleReceive(t, factory.entered, "factory entry")
	waiters.joined(t, 1)
	closed := lifecycleClose(client)
	lifecycleReceive(t, client.done, "Close admission barrier")
	secondClose := lifecycleClose(client)
	lifecycleWant(t, lifecycleReceive(t, result, "closed caller"), nil, ErrClosed)
	lifecycleBlocked(t, closed, "Close during factory")
	lifecycleBlocked(t, secondClose, "concurrent Close during factory")
	factory.unblock()
	lifecycleReceive(t, stop.entered, "losing resource Stop")
	lifecycleBlocked(t, closed, "Close during losing Stop")
	lifecycleBlocked(t, secondClose, "concurrent Close during losing Stop")
	client.mu.Lock()
	pending := client.creations[0]
	installed := len(client.sessions)
	client.mu.Unlock()
	if pending == nil || installed != 0 {
		t.Fatalf("pending=%p installed=%d during losing Stop", pending, installed)
	}
	stop.unblock()
	lifecycleReceive(t, closed, "Close joining creator worker")
	lifecycleReceive(t, secondClose, "concurrent Close joining creator worker")
	if stops.Load() != 1 {
		t.Fatalf("losing Stops=%d want=1", stops.Load())
	}
	lifecycleEmpty(t, client)
	lifecycleNoEvent(t, events)
	cached, err := lifecycleGet(t, client, 0, address)
	lifecycleWant(t, lifecycleResult{cached, err}, nil, ErrClosed)
}

func TestSessionLifecycleCloseJoinsInvalidation(t *testing.T) {
	stop := newLifecycleGate()
	var stops atomic.Int32
	active := &creationTestSession{stop: func() { stops.Add(1); stop.block() }}
	address := protocol.EntityAddrVec{testAddress(t, "192.0.2.10:6800")}
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{}, func(int32, protocol.EntityAddrVec) (session, error) { return active, nil })
	lifecycleCleanup(t, client, stop)
	cached, err := lifecycleGet(t, client, 0, address)
	lifecycleWant(t, lifecycleResult{cached, err}, active, nil)
	invalidated := make(chan struct{})
	go func() { client.invalidate(0, active); close(invalidated) }()
	lifecycleReceive(t, stop.entered, "invalidation Stop")
	closed := lifecycleClose(client)
	lifecycleReceive(t, client.done, "Close admission barrier")
	secondClose := lifecycleClose(client)
	lifecycleBlocked(t, closed, "Close joining invalidation")
	lifecycleBlocked(t, secondClose, "concurrent Close joining invalidation")
	stop.unblock()
	lifecycleReceive(t, invalidated, "invalidation completion")
	lifecycleReceive(t, closed, "Close completion")
	lifecycleReceive(t, secondClose, "concurrent Close completion")
	if stops.Load() != 1 {
		t.Fatalf("invalidation Stops=%d want=1", stops.Load())
	}
	lifecycleEmpty(t, client)
}

type lifecycleGenerationSource struct {
	mu         sync.Mutex
	generation uint64
}

func (*lifecycleGenerationSource) OSDMap() *maps.OSDMap                        { return nil }
func (*lifecycleGenerationSource) RefreshOSDMap(context.Context, uint32) error { return nil }
func (source *lifecycleGenerationSource) OSDSessionGeneration(int32) uint64 {
	source.mu.Lock()
	defer source.mu.Unlock()
	return source.generation
}
func (source *lifecycleGenerationSource) set(generation uint64) {
	source.mu.Lock()
	source.generation = generation
	source.mu.Unlock()
}

type lifecycleMapSource struct {
	mu         sync.Mutex
	osdMap     *maps.OSDMap
	generation uint64
}

func (source *lifecycleMapSource) OSDMap() *maps.OSDMap {
	source.mu.Lock()
	defer source.mu.Unlock()
	return source.osdMap
}

func (*lifecycleMapSource) RefreshOSDMap(context.Context, uint32) error { return nil }

func (source *lifecycleMapSource) OSDSessionGeneration(int32) uint64 {
	source.mu.Lock()
	defer source.mu.Unlock()
	return source.generation
}

func (source *lifecycleMapSource) publish(osdMap *maps.OSDMap) {
	source.mu.Lock()
	source.osdMap = osdMap
	source.mu.Unlock()
}

func TestSessionLifecycleEarlyTargetErrorSupersedesPendingCreation(t *testing.T) {
	for _, kind := range []string{"map address", "map down", "unusable addresses", "empty addresses"} {
		t.Run(kind, func(t *testing.T) {
			factory := newLifecycleGate()
			var calls, oldStops atomic.Int32
			old := &creationTestSession{stop: func() { oldStops.Add(1) }}
			fresh := &creationTestSession{}
			addressesA := protocol.EntityAddrVec{testAddress(t, "192.0.2.10:6800")}
			addressesB := protocol.EntityAddrVec{testAddress(t, "192.0.2.11:6800")}
			source := &lifecycleMapSource{generation: 1}
			source.publish(commandTestOSDMap(t, []uint32{3}, []protocol.EntityAddrVec{addressesA}))
			client := newTestClient(t, source, &fakeRouter{}, func(int32, protocol.EntityAddrVec) (session, error) {
				if calls.Add(1) == 1 {
					factory.block()
					return old, nil
				}
				return fresh, nil
			})
			lifecycleCleanup(t, client, factory)
			waiters := newLifecycleWaiters(client, 0)
			original := waiters.start(context.Background(), addressesA)
			lifecycleReceive(t, factory.entered, "A1 factory entry")
			waiters.joined(t, 1)

			wantErr := ErrStaleMap
			switch kind {
			case "map address":
				source.publish(commandTestOSDMapEpoch(t, 10, []uint32{3}, []protocol.EntityAddrVec{addressesB}))
			case "map down":
				wantErr = ErrNoPrimary
				source.publish(commandTestOSDMapEpoch(t, 10, []uint32{1}, []protocol.EntityAddrVec{addressesA}))
			case "unusable addresses":
				wantErr = ErrNoPrimary
				unusable := addressesA[0]
				unusable.Type = protocol.AddressLegacy
				source.publish(commandTestOSDMapEpoch(t, 10, []uint32{3}, []protocol.EntityAddrVec{{unusable}}))
			case "empty addresses":
				wantErr = ErrNoPrimary
				source.publish(commandTestOSDMapEpoch(t, 10, []uint32{3}, []protocol.EntityAddrVec{{}}))
			}
			observed := make(chan lifecycleResult, 1)
			go func() {
				active, err := client.getSessionContext(context.Background(), 0, addressesA)
				observed <- lifecycleResult{active: active, err: err}
			}()
			lifecycleWant(t, lifecycleReceive(t, observed, "early authoritative target error"), nil, wantErr)

			source.publish(commandTestOSDMapEpoch(t, 11, []uint32{3}, []protocol.EntityAddrVec{addressesA}))
			restored := waiters.start(context.Background(), addressesA)
			waiters.joined(t, 2)
			if calls.Load() != 1 || oldStops.Load() != 0 {
				t.Fatalf("before releasing A1: factory calls=%d Stops=%d; want calls=1 Stops=0", calls.Load(), oldStops.Load())
			}
			factory.unblock()
			for _, waiter := range []struct {
				label  string
				result <-chan lifecycleResult
			}{
				{label: "original A waiter", result: original},
				{label: "restored A waiter", result: restored},
			} {
				result := lifecycleReceive(t, waiter.result, waiter.label)
				if result.active != nil || !errors.Is(result.err, ErrStaleMap) {
					t.Errorf("%s: session=%p error=%v; want nil session and ErrStaleMap after authoritative %v", waiter.label, result.active, result.err, wantErr)
				}
			}
			if oldStops.Load() != 1 {
				t.Errorf("discarded A1 Stops=%d; want=1", oldStops.Load())
			}
			active, err := lifecycleGet(t, client, 0, addressesA)
			if active != fresh || err != nil || calls.Load() != 2 {
				t.Errorf("fresh A acquisition: session=%p error=%v factory calls=%d; want A2=%p error=nil calls=2", active, err, calls.Load(), fresh)
			}
		})
	}
}

func TestSessionLifecycleMapWatcherOnlyABARevokesPendingCreation(t *testing.T) {
	for _, kind := range []string{"map address", "map down"} {
		t.Run(kind, func(t *testing.T) {
			factory, stop := newLifecycleGate(), newLifecycleGate()
			var calls, oldStops atomic.Int32
			old := &creationTestSession{stop: func() { oldStops.Add(1); stop.block() }}
			fresh := &creationTestSession{}
			addressesA := protocol.EntityAddrVec{testAddress(t, "192.0.2.10:6800")}
			addressesB := protocol.EntityAddrVec{testAddress(t, "192.0.2.11:6800")}
			source := &lifecycleMapSource{generation: 1}
			source.publish(commandTestOSDMap(t, []uint32{3}, []protocol.EntityAddrVec{addressesA}))
			client := newTestClient(t, source, &fakeRouter{}, func(int32, protocol.EntityAddrVec) (session, error) {
				if calls.Add(1) == 1 {
					factory.block()
					return old, nil
				}
				return fresh, nil
			})
			lifecycleCleanup(t, client, factory, stop)
			events := lifecycleObserve(client)
			waiters := newLifecycleWaiters(client, 0)
			original := waiters.start(context.Background(), addressesA)
			lifecycleReceive(t, factory.entered, "A1 factory entry")
			waiters.joined(t, 1)
			changedMap := commandTestOSDMapEpoch(t, 10, []uint32{3}, []protocol.EntityAddrVec{addressesB})
			if kind == "map down" {
				changedMap = commandTestOSDMapEpoch(t, 10, []uint32{1}, []protocol.EntityAddrVec{addressesA})
			}
			source.publish(changedMap)
			client.invalidateUnusableSessions(changedMap)
			client.mu.Lock()
			superseded := client.creations[0].superseded
			client.mu.Unlock()
			if !superseded {
				t.Fatal("watcher path did not revoke gated A1 creation")
			}
			source.publish(commandTestOSDMapEpoch(t, 11, []uint32{3}, []protocol.EntityAddrVec{addressesA}))
			factory.unblock()
			lifecycleReceive(t, stop.entered, "discarded A1 Stop after restoring A")
			lifecycleNoEvent(t, events)
			select {
			case result := <-original:
				t.Fatalf("A1 completed before discarded Stop returned: %+v", result)
			default:
			}
			if calls.Load() != 1 {
				t.Fatalf("factory calls=%d before discarded Stop completed; want=1", calls.Load())
			}
			stop.unblock()
			lifecycleWant(t, lifecycleReceive(t, original, "watcher-revoked A1 result"), nil, ErrStaleMap)
			lifecycleEmpty(t, client)
			active, err := lifecycleGet(t, client, 0, addressesA)
			lifecycleWant(t, lifecycleResult{active, err}, fresh, nil)
			cached, err := lifecycleGet(t, client, 0, addressesA)
			lifecycleWant(t, lifecycleResult{cached, err}, fresh, nil)
			if calls.Load() != 2 || oldStops.Load() != 1 {
				t.Fatalf("factory calls=%d discarded A1 Stops=%d; want calls=2 Stops=1", calls.Load(), oldStops.Load())
			}
			lifecycleAvailable(t, events)
			lifecycleNoEvent(t, events)
		})
	}
}

func TestSessionLifecycleTargetChangesDuringFactory(t *testing.T) {
	for _, kind := range []string{"caller address", "generation", "generation ABA", "map address", "map down"} {
		t.Run(kind, func(t *testing.T) {
			factory, stop := newLifecycleGate(), newLifecycleGate()
			var calls, oldStops atomic.Int32
			old := &creationTestSession{stop: func() { oldStops.Add(1); stop.block() }}
			fresh := &creationTestSession{}
			oldAddress := protocol.EntityAddrVec{testAddress(t, "192.0.2.10:6800")}
			newAddress := protocol.EntityAddrVec{testAddress(t, "192.0.2.11:6800")}
			generation := &lifecycleGenerationSource{generation: 1}
			mapSource := newNotifyingMapSource()
			var source MapSource = &fakeMapSource{}
			if kind == "generation" || kind == "generation ABA" {
				source = generation
			}
			if kind == "map address" || kind == "map down" {
				initial := commandTestOSDMap(t, []uint32{3}, []protocol.EntityAddrVec{oldAddress})
				state, found := initial.OSDState(0)
				if !found || !state.Exists || !state.Up {
					t.Fatalf("fixture must start existing/up: %+v found=%t", state, found)
				}
				mapSource.publish(initial)
				source = mapSource
			}
			client := newTestClient(t, source, &fakeRouter{}, func(int32, protocol.EntityAddrVec) (session, error) {
				if calls.Add(1) == 1 {
					factory.block()
					return old, nil
				}
				return fresh, nil
			})
			lifecycleCleanup(t, client, factory, stop)
			events := lifecycleObserve(client)
			waiters := newLifecycleWaiters(client, 0)
			original := waiters.start(context.Background(), oldAddress)
			lifecycleReceive(t, factory.entered, "old factory")
			waiters.joined(t, 1)
			var newer, restored <-chan lifecycleResult
			switch kind {
			case "caller address":
				newer = waiters.start(context.Background(), newAddress)
				waiters.joined(t, 2)
			case "generation":
				generation.set(2)
			case "generation ABA":
				generation.set(2)
				newer = waiters.start(context.Background(), oldAddress)
				waiters.joined(t, 2)
				generation.set(1)
				restored = waiters.start(context.Background(), oldAddress)
				waiters.joined(t, 3)
			case "map address":
				mapSource.publish(commandTestOSDMapEpoch(t, 10, []uint32{3}, []protocol.EntityAddrVec{newAddress}))
			case "map down":
				mapSource.publish(commandTestOSDMapEpoch(t, 10, []uint32{1}, []protocol.EntityAddrVec{oldAddress}))
			}
			if kind == "caller address" || kind == "generation ABA" {
				client.mu.Lock()
				superseded := client.creations[0].superseded
				client.mu.Unlock()
				if !superseded {
					t.Fatal("observed target mismatch did not supersede old creation")
				}
			}
			factory.unblock()
			lifecycleReceive(t, stop.entered, "discarded old resource Stop")
			lifecycleNoEvent(t, events)
			if calls.Load() != 1 {
				t.Fatalf("replacement started before old resource teardown: calls=%d", calls.Load())
			}
			stop.unblock()
			lifecycleWant(t, lifecycleReceive(t, original, "stale original result"), nil, ErrStaleMap)
			if restored != nil {
				lifecycleWant(t, lifecycleReceive(t, restored, "ABA matching waiter"), nil, ErrStaleMap)
			}
			if newer != nil {
				lifecycleWant(t, lifecycleReceive(t, newer, "superseding caller result"), fresh, nil)
			} else if kind != "map down" {
				address := oldAddress
				if kind == "map address" {
					address = newAddress
				}
				active, err := lifecycleGet(t, client, 0, address)
				lifecycleWant(t, lifecycleResult{active, err}, fresh, nil)
			} else {
				active, err := lifecycleGet(t, client, 0, oldAddress)
				lifecycleWant(t, lifecycleResult{active, err}, nil, ErrNoPrimary)
			}
			wantCalls := int32(2)
			if kind == "map down" {
				wantCalls = 1
			}
			if calls.Load() != wantCalls || oldStops.Load() != 1 {
				t.Fatalf("calls=%d want=%d old Stops=%d", calls.Load(), wantCalls, oldStops.Load())
			}
			client.mu.Lock()
			entry, cached := client.sessions[0]
			client.mu.Unlock()
			if kind == "map down" {
				if cached {
					t.Fatal("down OSD cached a session")
				}
			} else if !cached || entry.session != fresh {
				t.Fatalf("cached=%t session=%p want fresh=%p", cached, entry.session, fresh)
			}
			if kind != "map down" {
				lifecycleAvailable(t, events)
			}
			lifecycleNoEvent(t, events)
		})
	}
}

func TestSessionLifecycleReadCancellationDuringFactory(t *testing.T) {
	gate := newLifecycleGate()
	var submits atomic.Int32
	active := &lifecycleReadSession{submits: &submits}
	route := testRoute(t, 10, 0, "192.0.2.10:6800")
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: route}, func(int32, protocol.EntityAddrVec) (session, error) {
		gate.block()
		return active, nil
	})
	lifecycleCleanup(t, client, gate)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waiters := newLifecycleWaiters(client, 0)
	read := make(chan error, 1)
	go func() {
		_, err := client.Read(&lifecycleContext{Context: ctx, waiters: waiters}, Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, 0, 4)
		read <- err
	}()
	lifecycleReceive(t, gate.entered, "Read factory")
	waiters.joined(t, 1)
	survivor := waiters.start(context.Background(), route.Addresses)
	waiters.joined(t, 2)
	cancel()
	if err := lifecycleReceive(t, read, "canceled Read"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Read error=%v", err)
	}
	gate.unblock()
	lifecycleWant(t, lifecycleReceive(t, survivor, "surviving session caller"), active, nil)
	if submits.Load() != 0 {
		t.Fatalf("canceled Read submitted %d requests", submits.Load())
	}
}

type lifecycleReadSession struct {
	creationTestSession
	submits *atomic.Int32
}

func (active *lifecycleReadSession) Submit(context.Context, msgr.Message) (msgr.Message, error) {
	active.submits.Add(1)
	return msgr.Message{}, errors.New("unexpected submission after canceled creation wait")
}

func TestSessionLifecycleObserverAvailabilityBeforeInvalidation(t *testing.T) {
	stop := newLifecycleGate()
	var stops atomic.Int32
	active := &creationTestSession{stop: func() { stops.Add(1); stop.block() }}
	address := protocol.EntityAddrVec{testAddress(t, "192.0.2.10:6800")}
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{}, func(int32, protocol.EntityAddrVec) (session, error) { return active, nil })
	lifecycleCleanup(t, client, stop)
	events := lifecycleObserve(client)
	cached, err := lifecycleGet(t, client, 0, address)
	lifecycleWant(t, lifecycleResult{cached, err}, active, nil)
	invalidated := make(chan struct{})
	go func() { client.invalidate(0, active); close(invalidated) }()
	lifecycleReceive(t, stop.entered, "invalidation after observer error")
	lifecycleAvailable(t, events)
	event := lifecycleReceive(t, events, "session invalidation observer error")
	if event.OSDID != 0 || event.Available || !errors.Is(event.Err, msgr.ErrSessionDisconnected) {
		t.Fatalf("invalidation event=%+v", event)
	}
	stop.unblock()
	lifecycleReceive(t, invalidated, "observer invalidation completion")
	lifecycleInvalidate(t, client, 0, active)
	if stops.Load() != 1 {
		t.Fatalf("duplicate invalidation Stops=%d", stops.Load())
	}
	lifecycleNoEvent(t, events)
	lifecycleEmpty(t, client)
}

type lifecycleNotificationSession struct {
	*notificationFakeSession
	iterations chan struct{}
	stops      atomic.Int32
}

func newLifecycleNotificationSession() *lifecycleNotificationSession {
	return &lifecycleNotificationSession{
		notificationFakeSession: &notificationFakeSession{
			fakeSession:   &fakeSession{submit: func(context.Context, msgr.Message) (msgr.Message, error) { return msgr.Message{}, nil }},
			notifications: make(chan osd.WatchNotification),
			interruptions: make(chan error),
		},
		iterations: make(chan struct{}, 16),
	}
}

func (active *lifecycleNotificationSession) Stop() { active.stops.Add(1) }

func (active *lifecycleNotificationSession) Notifications() <-chan osd.WatchNotification {
	active.iterations <- struct{}{}
	return active.notifications
}

type lifecycleObserverSession struct {
	*lifecycleNotificationSession
	stopGate *lifecycleGate
}

func (active *lifecycleObserverSession) Stop() {
	active.stops.Add(1)
	active.stopGate.block()
}

func TestSessionLifecycleBlockedInitialObserver(t *testing.T) {
	for _, kind := range []string{"invalidation", "concurrent Close"} {
		t.Run(kind, func(t *testing.T) {
			observer, stop := newLifecycleGate(), newLifecycleGate()
			active := &lifecycleObserverSession{lifecycleNotificationSession: newLifecycleNotificationSession(), stopGate: stop}
			var otherStops atomic.Int32
			other := &creationTestSession{stop: func() { otherStops.Add(1) }}
			address := protocol.EntityAddrVec{testAddress(t, "192.0.2.10:6800")}
			client := newTestClient(t, &fakeMapSource{}, &fakeRouter{}, func(id int32, _ protocol.EntityAddrVec) (session, error) {
				if id == 1 {
					return other, nil
				}
				return active, nil
			})
			lifecycleCleanup(t, client, observer, stop)
			cached, err := lifecycleGet(t, client, 1, address)
			lifecycleWant(t, lifecycleResult{cached, err}, other, nil)
			events := make(chan OSDSessionEvent, 16)
			var availabilityCalls atomic.Int32
			client.config.ObserveSession = func(event OSDSessionEvent) {
				if event.OSDID == 0 && event.Available {
					availabilityCalls.Add(1)
					observer.block()
				}
				events <- event
			}
			waiters := newLifecycleWaiters(client, 0)
			result := waiters.start(context.Background(), address)
			lifecycleReceive(t, observer.entered, "initial availability callback entry")
			waiters.joined(t, 1)
			client.mu.Lock()
			pending := client.creations[0]
			installed := client.sessions[0].session
			client.mu.Unlock()
			if installed != active || pending == nil {
				t.Fatalf("installed=%p pending=%p while callback blocked; want active=%p and pending creation", installed, pending, active)
			}
			lifecycleBlocked(t, pending.done, "creation during initial callback")
			lifecycleNoEvent(t, events)
			cached, err = lifecycleGet(t, client, 1, address)
			lifecycleWant(t, lifecycleResult{cached, err}, other, nil)

			if kind == "invalidation" {
				started, invalidated := make(chan struct{}), make(chan struct{})
				go func() { close(started); client.invalidate(0, active); close(invalidated) }()
				lifecycleReceive(t, started, "invalidation caller start")
				lifecycleBlocked(t, invalidated, "invalidation during initial callback")
				if active.stops.Load() != 0 {
					t.Fatal("invalidation stopped session before initial callback returned")
				}
				lifecycleNoEvent(t, events)
				cached, err = lifecycleGet(t, client, 1, address)
				lifecycleWant(t, lifecycleResult{cached, err}, other, nil)
				observer.unblock()
				lifecycleReceive(t, pending.done, "creation completion after callback")
				lifecycleWant(t, lifecycleReceive(t, result, "initial session result"), active, nil)
				lifecycleReceive(t, stop.entered, "invalidation Stop after callback")
				lifecycleAvailable(t, events)
				event := lifecycleReceive(t, events, "ordered invalidation event")
				if event.OSDID != 0 || event.Available || !errors.Is(event.Err, msgr.ErrSessionDisconnected) {
					t.Fatalf("invalidation event=%+v", event)
				}
				lifecycleBlocked(t, invalidated, "invalidation joining Stop")
				stop.unblock()
				lifecycleReceive(t, invalidated, "invalidation completion")
				lifecycleInvalidate(t, client, 0, active)
			} else {
				closed := lifecycleClose(client)
				lifecycleReceive(t, client.done, "Close admission barrier during callback")
				secondClose := lifecycleClose(client)
				lifecycleWant(t, lifecycleReceive(t, result, "closed initial waiter"), nil, ErrClosed)
				lifecycleReceive(t, stop.entered, "Close Stop during initial callback")
				lifecycleBlocked(t, closed, "Close joining gated Stop")
				lifecycleBlocked(t, secondClose, "concurrent Close joining gated Stop")
				stop.unblock()
				lifecycleBlocked(t, closed, "Close joining initial callback after Stop")
				lifecycleBlocked(t, secondClose, "concurrent Close joining initial callback after Stop")
				lifecycleNoEvent(t, events)
				observer.unblock()
				lifecycleReceive(t, closed, "Close joining creator after callback")
				lifecycleReceive(t, secondClose, "concurrent Close completion after callback")
				lifecycleReceive(t, pending.done, "closed creation completion")
				lifecycleAvailable(t, events)
				if pending.result != nil || !errors.Is(pending.err, ErrClosed) {
					t.Fatalf("closed creation result=%p error=%v; want nil and ErrClosed", pending.result, pending.err)
				}
				select {
				case <-active.iterations:
					t.Fatal("notification dispatcher started after Close during initial callback")
				default:
				}
				if otherStops.Load() != 1 {
					t.Fatalf("unrelated cached session Stops=%d want=1", otherStops.Load())
				}
				lifecycleEmpty(t, client)
				cached, err = lifecycleGet(t, client, 0, address)
				lifecycleWant(t, lifecycleResult{cached, err}, nil, ErrClosed)
			}
			if availabilityCalls.Load() != 1 || active.stops.Load() != 1 {
				t.Fatalf("initial availability callbacks=%d Stops=%d; want one each", availabilityCalls.Load(), active.stops.Load())
			}
			lifecycleNoEvent(t, events)
		})
	}
}

func lifecycleSendNotification(t *testing.T, active *lifecycleNotificationSession, notification osd.WatchNotification) {
	t.Helper()
	select {
	case active.notifications <- notification:
	case <-time.After(lifecycleTimeout):
		t.Fatal("dispatcher did not receive notification")
	}
	lifecycleReceive(t, active.iterations, "dispatcher processed notification and reentered select")
}

func TestSessionLifecycleNotificationsRequireCurrentOwner(t *testing.T) {
	old, current := newLifecycleNotificationSession(), newLifecycleNotificationSession()
	oldAddress := protocol.EntityAddrVec{testAddress(t, "192.0.2.10:6800")}
	newAddress := protocol.EntityAddrVec{testAddress(t, "192.0.2.11:6800")}
	var calls atomic.Int32
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{}, func(int32, protocol.EntityAddrVec) (session, error) {
		if calls.Add(1) == 1 {
			return old, nil
		}
		return current, nil
	})
	lifecycleCleanup(t, client)
	active, err := lifecycleGet(t, client, 0, oldAddress)
	lifecycleWant(t, lifecycleResult{active, err}, old, nil)
	lifecycleReceive(t, old.iterations, "old dispatcher initial select")
	active, err = lifecycleGet(t, client, 0, newAddress)
	lifecycleWant(t, lifecycleResult{active, err}, current, nil)
	lifecycleReceive(t, current.iterations, "current dispatcher initial select")
	if old.stops.Load() != 1 {
		t.Fatalf("replacement did not stop old owner: %d", old.stops.Load())
	}
	lifetime, cancel := context.WithCancel(context.Background())
	defer cancel()
	watch := &Watch{
		client: client, cookie: 41, lifetime: lifetime, cancel: cancel, route: Route{Primary: 0},
		events: make(chan osd.WatchNotification, 4), errors: make(chan error, 1),
		done: make(chan struct{}), reconnect: make(chan struct{}, 1),
	}
	completion := make(chan notifyCompletion, 4)
	client.mu.Lock()
	client.watches[watch.cookie] = watch
	client.notifies[42] = completion
	client.mu.Unlock()
	lifecycleSendNotification(t, old, osd.WatchNotification{Opcode: osd.WatchEventNotify, Cookie: watch.cookie, NotifyID: 1})
	lifecycleSendNotification(t, old, osd.WatchNotification{Opcode: osd.WatchEventComplete, Cookie: 42, NotifyID: 2})
	select {
	case event := <-watch.Events():
		t.Fatalf("stale owner delivered watch event: %+v", event)
	default:
	}
	select {
	case result := <-completion:
		t.Fatalf("stale owner delivered notify completion: %+v", result)
	default:
	}
	lifecycleSendNotification(t, current, osd.WatchNotification{Opcode: osd.WatchEventNotify, Cookie: watch.cookie, NotifyID: 3})
	watchEvent := lifecycleReceive(t, watch.Events(), "current owner watch event")
	if watchEvent.NotifyID != 3 || watchEvent.Cookie != watch.cookie {
		t.Fatalf("watch event=%+v", watchEvent)
	}
	lifecycleSendNotification(t, current, osd.WatchNotification{Opcode: osd.WatchEventComplete, Cookie: 42, NotifyID: 4})
	notifyResult := lifecycleReceive(t, completion, "current owner notify completion")
	if notifyResult.err != nil || notifyResult.notification.NotifyID != 4 || notifyResult.notification.Cookie != 42 {
		t.Fatalf("completion=%+v", notifyResult)
	}
	lifecycleInvalidate(t, client, 0, old)
	if old.stops.Load() != 1 || current.stops.Load() != 0 {
		t.Fatalf("stale invalidation Stops old=%d current=%d", old.stops.Load(), current.stops.Load())
	}
	lifecycleReceive(t, lifecycleClose(client), "Close joining both notification dispatchers")
	if current.stops.Load() != 1 {
		t.Fatalf("current owner Stops=%d want=1", current.stops.Load())
	}
	lifecycleEmpty(t, client)
}

func TestSessionLifecycleCloseDuringReplacementStopSkipsFactory(t *testing.T) {
	stop := newLifecycleGate()
	var calls, stops atomic.Int32
	old := &creationTestSession{stop: func() { stops.Add(1); stop.block() }}
	oldAddress := protocol.EntityAddrVec{testAddress(t, "192.0.2.10:6800")}
	newAddress := protocol.EntityAddrVec{testAddress(t, "192.0.2.11:6800")}
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{}, func(int32, protocol.EntityAddrVec) (session, error) {
		calls.Add(1)
		return old, nil
	})
	lifecycleCleanup(t, client, stop)
	active, err := lifecycleGet(t, client, 0, oldAddress)
	lifecycleWant(t, lifecycleResult{active, err}, old, nil)
	waiters := newLifecycleWaiters(client, 0)
	replacement := waiters.start(context.Background(), newAddress)
	lifecycleReceive(t, stop.entered, "replacement old-session Stop")
	waiters.joined(t, 1)
	closed := lifecycleClose(client)
	lifecycleReceive(t, client.done, "Close during replacement teardown")
	lifecycleWant(t, lifecycleReceive(t, replacement, "closed replacement caller"), nil, ErrClosed)
	lifecycleBlocked(t, closed, "Close joining replacement teardown")
	stop.unblock()
	lifecycleReceive(t, closed, "Close after replacement teardown")
	if calls.Load() != 1 || stops.Load() != 1 {
		t.Fatalf("factory calls=%d old Stops=%d; want one each and no post-Close factory", calls.Load(), stops.Load())
	}
	lifecycleEmpty(t, client)
}
