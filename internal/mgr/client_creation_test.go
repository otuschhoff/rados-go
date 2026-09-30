package mgr

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSessionCreationCoalescesSameTargetMisses(t *testing.T) {
	source := &fakeMgrMapSource{mgrMap: testMgrMap(t, mgrMapFixture{})}
	gate := make(chan struct{})
	var release sync.Once
	var entries atomic.Int32
	entered := make(chan struct{}, 16)
	client := newTestManagerClient(t, source, func(ctx context.Context, _ ActiveTarget) (session, error) {
		entries.Add(1)
		entered <- struct{}{}
		select {
		case <-gate:
			return &fakeMgrSession{}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	defer client.Close()
	defer release.Do(func() { close(gate) })
	target, err := client.currentTarget()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := make(chan struct{})
	results := make(chan error, 16)
	for range 16 {
		go func() {
			<-start
			_, err := client.getSession(ctx, target)
			results <- err
		}()
	}
	close(start)
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("factory did not start")
	}
	waitManagerWaiters(t, client, target, 16)
	release.Do(func() { close(gate) })
	for range 16 {
		select {
		case err := <-results:
			if err != nil {
				t.Errorf("getSession: %v", err)
			}
		case <-ctx.Done():
			t.Fatal("callers did not finish")
		}
	}
	if count := entries.Load(); count != 1 {
		t.Fatalf("factory entries=%d, want 1", count)
	}
}

func waitManagerWaiters(t *testing.T, client *Client, target ActiveTarget, count int) {
	t.Helper()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		client.mu.Lock()
		found := false
		for _, creation := range client.creations {
			if sameTarget(creation.target, target) && !creation.abandoned && creation.waiters == count {
				found = true
			}
		}
		client.mu.Unlock()
		if found {
			return
		}
		select {
		case <-ticker.C:
		case <-timer.C:
			t.Fatalf("target %s did not reach %d waiters", target.Name, count)
		}
	}
}

type managerCreationResult struct {
	active sessionEntry
	err    error
}

func startManagerCreation(client *Client, ctx context.Context, target ActiveTarget) <-chan managerCreationResult {
	result := make(chan managerCreationResult, 1)
	go func() {
		active, err := client.getSession(ctx, target)
		result <- managerCreationResult{active: active, err: err}
	}()
	return result
}

func receiveManagerCreation(t *testing.T, result <-chan managerCreationResult) managerCreationResult {
	t.Helper()
	select {
	case outcome := <-result:
		return outcome
	case <-time.After(time.Second):
		t.Fatal("manager creation did not finish")
		return managerCreationResult{}
	}
}

func managerTarget(t *testing.T, client *Client) ActiveTarget {
	t.Helper()
	target, err := client.currentTarget()
	if err != nil {
		t.Fatal(err)
	}
	return target
}

func TestSessionCreationWaiterCancellation(t *testing.T) {
	for _, canceledFirst := range []bool{true, false} {
		name := "later waiter"
		if canceledFirst {
			name = "first caller"
		}
		t.Run(name, func(t *testing.T) {
			source := &fakeMgrMapSource{mgrMap: testMgrMap(t, mgrMapFixture{})}
			gate := make(chan struct{})
			var release sync.Once
			factoryContext := make(chan context.Context, 1)
			created := &fakeMgrSession{}
			client := newTestManagerClient(t, source, func(ctx context.Context, _ ActiveTarget) (session, error) {
				factoryContext <- ctx
				select {
				case <-gate:
					return created, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			})
			defer client.Close()
			defer release.Do(func() { close(gate) })
			type contextKey struct{}
			ctx, cancel := context.WithCancel(context.WithValue(context.Background(), contextKey{}, "caller value"))
			defer cancel()
			firstCtx, secondCtx := context.Context(context.Background()), context.Context(ctx)
			if canceledFirst {
				firstCtx, secondCtx = ctx, context.Background()
			}
			target := managerTarget(t, client)
			first := startManagerCreation(client, firstCtx, target)
			waitManagerWaiters(t, client, target, 1)
			second := startManagerCreation(client, secondCtx, target)
			waitManagerWaiters(t, client, target, 2)
			cancel()
			canceled, surviving := second, first
			if canceledFirst {
				canceled, surviving = first, second
			}
			if outcome := receiveManagerCreation(t, canceled); !errors.Is(outcome.err, context.Canceled) {
				t.Fatalf("canceled waiter: %v", outcome.err)
			}
			waitManagerWaiters(t, client, target, 1)
			select {
			case factoryCtx := <-factoryContext:
				if factoryCtx.Err() != nil {
					t.Fatalf("surviving waiter lost factory context: %v", factoryCtx.Err())
				}
				if canceledFirst && factoryCtx.Value(contextKey{}) != "caller value" {
					t.Fatal("factory context lost caller values")
				}
			case <-time.After(time.Second):
				t.Fatal("factory did not start")
			}
			release.Do(func() { close(gate) })
			if outcome := receiveManagerCreation(t, surviving); outcome.err != nil || outcome.active.session != created {
				t.Fatalf("surviving waiter: %+v", outcome)
			}
		})
	}
}

func TestSessionCreationAllWaitersCancelAndRetry(t *testing.T) {
	source := &fakeMgrMapSource{mgrMap: testMgrMap(t, mgrMapFixture{})}
	var entries atomic.Int32
	factoryStarted := make(chan struct{})
	factoryCanceled := make(chan struct{})
	client := newTestManagerClient(t, source, func(ctx context.Context, _ ActiveTarget) (session, error) {
		if entries.Add(1) == 1 {
			close(factoryStarted)
			<-ctx.Done()
			close(factoryCanceled)
			return nil, ctx.Err()
		}
		return &fakeMgrSession{}, nil
	})
	defer client.Close()
	target := managerTarget(t, client)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := startManagerCreation(client, ctx, target)
	waitManagerWaiters(t, client, target, 1)
	select {
	case <-factoryStarted:
	case <-time.After(time.Second):
		t.Fatal("factory did not start")
	}
	cancel()
	if outcome := receiveManagerCreation(t, result); !errors.Is(outcome.err, context.Canceled) {
		t.Fatalf("canceled waiter: %v", outcome.err)
	}
	select {
	case <-factoryCanceled:
	case <-time.After(time.Second):
		t.Fatal("last waiter did not cancel factory")
	}
	if outcome := receiveManagerCreation(t, startManagerCreation(client, context.Background(), target)); outcome.err != nil {
		t.Fatalf("retry: %v", outcome.err)
	}
	if entries.Load() != 2 {
		t.Fatalf("factory entries=%d", entries.Load())
	}
}

func TestSessionCreationFailureFanoutAndRetry(t *testing.T) {
	source := &fakeMgrMapSource{mgrMap: testMgrMap(t, mgrMapFixture{})}
	gate := make(chan struct{})
	var release sync.Once
	var entries atomic.Int32
	failure := errors.New("factory failure")
	partial := &fakeMgrSession{}
	client := newTestManagerClient(t, source, func(ctx context.Context, _ ActiveTarget) (session, error) {
		if entries.Add(1) == 1 {
			select {
			case <-gate:
				return partial, failure
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return &fakeMgrSession{}, nil
	})
	defer client.Close()
	defer release.Do(func() { close(gate) })
	target := managerTarget(t, client)
	var results []<-chan managerCreationResult
	for range 8 {
		results = append(results, startManagerCreation(client, context.Background(), target))
	}
	waitManagerWaiters(t, client, target, 8)
	release.Do(func() { close(gate) })
	for _, result := range results {
		if outcome := receiveManagerCreation(t, result); !errors.Is(outcome.err, failure) {
			t.Fatalf("failure fanout: %v", outcome.err)
		}
	}
	if entries.Load() != 1 || partial.stopCount() != 1 {
		t.Fatalf("entries=%d partial stops=%d", entries.Load(), partial.stopCount())
	}
	if outcome := receiveManagerCreation(t, startManagerCreation(client, context.Background(), target)); outcome.err != nil {
		t.Fatalf("retry: %v", outcome.err)
	}
	if entries.Load() != 2 {
		t.Fatalf("retry entries=%d", entries.Load())
	}
}

type gatedManagerSession struct {
	fakeMgrSession
	stopEntered chan struct{}
	stopGate    chan struct{}
}

func (active *gatedManagerSession) Stop() {
	active.fakeMgrSession.Stop()
	close(active.stopEntered)
	<-active.stopGate
}

func managerSignal(t *testing.T, signal <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatal(message)
	}
}

func startManagerClose(client *Client) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		client.Close()
		close(done)
	}()
	return done
}

func managerCloseBlocked(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
		t.Fatal("Close returned before gated resources joined")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestSessionCreationTargetChangeAndCloseJoinFactoryAndLoserStop(t *testing.T) {
	source := &fakeMgrMapSource{mgrMap: testMgrMap(t, mgrMapFixture{name: "a"})}
	factoryGate := make(chan struct{})
	factoryEntered := make(chan struct{})
	var releaseFactory, releaseStop sync.Once
	loser := &gatedManagerSession{stopEntered: make(chan struct{}), stopGate: make(chan struct{})}
	cached := &fakeMgrSession{}
	client := newTestManagerClient(t, source, func(_ context.Context, target ActiveTarget) (session, error) {
		if target.Name == "a" {
			close(factoryEntered)
			<-factoryGate
			return loser, nil
		}
		return cached, nil
	})
	defer client.Close()
	defer releaseStop.Do(func() { close(loser.stopGate) })
	defer releaseFactory.Do(func() { close(factoryGate) })
	oldTarget := managerTarget(t, client)
	oldResult := startManagerCreation(client, context.Background(), oldTarget)
	managerSignal(t, factoryEntered, "old factory did not start")
	source.set(testMgrMap(t, mgrMapFixture{name: "b", gid: 8, endpoint: "192.0.2.51:7001"}))
	newTarget := managerTarget(t, client)
	if outcome := receiveManagerCreation(t, startManagerCreation(client, context.Background(), newTarget)); outcome.err != nil || outcome.active.session != cached {
		t.Fatalf("new target blocked by old factory: %+v", outcome)
	}
	if outcome := receiveManagerCreation(t, startManagerCreation(client, context.Background(), oldTarget)); !errors.Is(outcome.err, ErrNoActiveManager) {
		t.Fatalf("stale request: %v", outcome.err)
	}
	if outcome := receiveManagerCreation(t, startManagerCreation(client, context.Background(), newTarget)); outcome.err != nil || outcome.active.session != cached {
		t.Fatalf("cached access blocked by old factory: %+v", outcome)
	}
	closed := startManagerClose(client)
	managerSignal(t, client.done, "Close did not start")
	if outcome := receiveManagerCreation(t, oldResult); !errors.Is(outcome.err, ErrClosed) {
		t.Fatalf("closed waiter: %v", outcome.err)
	}
	managerCloseBlocked(t, closed)
	releaseFactory.Do(func() { close(factoryGate) })
	managerSignal(t, loser.stopEntered, "late factory session not stopped")
	secondClose := startManagerClose(client)
	managerCloseBlocked(t, closed)
	managerCloseBlocked(t, secondClose)
	if _, err := client.getSession(context.Background(), newTarget); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-Close getSession: %v", err)
	}
	releaseStop.Do(func() { close(loser.stopGate) })
	managerSignal(t, closed, "Close did not join loser Stop")
	managerSignal(t, secondClose, "concurrent Close did not join")
	client.mu.Lock()
	installed, workers := client.session.session, len(client.creations)
	client.mu.Unlock()
	if installed != nil || workers != 0 || cached.stopCount() != 1 || loser.stopCount() != 1 {
		t.Fatalf("installed=%v workers=%d cached stops=%d loser stops=%d", installed, workers, cached.stopCount(), loser.stopCount())
	}
}

func TestSessionCreationRejectsMapChangeWithoutNewCaller(t *testing.T) {
	for _, fixture := range []mgrMapFixture{
		{name: "a", endpoint: "192.0.2.51:7001"},
		{name: "b", gid: 8},
		{unavailable: true},
	} {
		t.Run(fixture.name+fixture.endpoint, func(t *testing.T) {
			source := &fakeMgrMapSource{mgrMap: testMgrMap(t, mgrMapFixture{name: "a"})}
			gate := make(chan struct{})
			entered := make(chan struct{})
			var release sync.Once
			created := &fakeMgrSession{}
			client := newTestManagerClient(t, source, func(ctx context.Context, _ ActiveTarget) (session, error) {
				close(entered)
				select {
				case <-gate:
					return created, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			})
			defer client.Close()
			defer release.Do(func() { close(gate) })
			result := startManagerCreation(client, context.Background(), managerTarget(t, client))
			managerSignal(t, entered, "factory did not start")
			source.set(testMgrMap(t, fixture))
			release.Do(func() { close(gate) })
			if outcome := receiveManagerCreation(t, result); !errors.Is(outcome.err, ErrNoActiveManager) || outcome.active.session != nil {
				t.Fatalf("stale creation: %+v", outcome)
			}
			if created.stopCount() != 1 {
				t.Fatalf("stale session stops=%d", created.stopCount())
			}
		})
	}
}

func TestSessionReplacementStopAllowsCachedProgressAndCloseJoins(t *testing.T) {
	source := &fakeMgrMapSource{mgrMap: testMgrMap(t, mgrMapFixture{name: "a"})}
	old := &gatedManagerSession{stopEntered: make(chan struct{}), stopGate: make(chan struct{})}
	var release sync.Once
	var middleEntries atomic.Int32
	cached := &fakeMgrSession{}
	client := newTestManagerClient(t, source, func(_ context.Context, target ActiveTarget) (session, error) {
		switch target.Name {
		case "a":
			return old, nil
		case "b":
			middleEntries.Add(1)
			return &fakeMgrSession{}, nil
		default:
			return cached, nil
		}
	})
	defer client.Close()
	defer release.Do(func() { close(old.stopGate) })
	if outcome := receiveManagerCreation(t, startManagerCreation(client, context.Background(), managerTarget(t, client))); outcome.err != nil {
		t.Fatal(outcome.err)
	}
	source.set(testMgrMap(t, mgrMapFixture{name: "b", gid: 8}))
	middle := startManagerCreation(client, context.Background(), managerTarget(t, client))
	managerSignal(t, old.stopEntered, "replacement did not stop old session")
	source.set(testMgrMap(t, mgrMapFixture{name: "c", gid: 9}))
	target := managerTarget(t, client)
	for range 2 {
		if outcome := receiveManagerCreation(t, startManagerCreation(client, context.Background(), target)); outcome.err != nil || outcome.active.session != cached {
			t.Fatalf("cached progress blocked by replacement Stop: %+v", outcome)
		}
	}
	closed := startManagerClose(client)
	managerSignal(t, client.done, "Close did not start")
	if outcome := receiveManagerCreation(t, middle); !errors.Is(outcome.err, ErrClosed) {
		t.Fatalf("replacement waiter: %v", outcome.err)
	}
	managerCloseBlocked(t, closed)
	release.Do(func() { close(old.stopGate) })
	managerSignal(t, closed, "Close did not join replacement Stop")
	if middleEntries.Load() != 0 || old.stopCount() != 1 || cached.stopCount() != 1 {
		t.Fatalf("middle entries=%d old stops=%d cached stops=%d", middleEntries.Load(), old.stopCount(), cached.stopCount())
	}
}

func TestSessionInvalidationStopJoinedByClose(t *testing.T) {
	source := &fakeMgrMapSource{mgrMap: testMgrMap(t, mgrMapFixture{})}
	active := &gatedManagerSession{stopEntered: make(chan struct{}), stopGate: make(chan struct{})}
	var release sync.Once
	client := newTestManagerClient(t, source, func(context.Context, ActiveTarget) (session, error) { return active, nil })
	defer client.Close()
	defer release.Do(func() { close(active.stopGate) })
	outcome := receiveManagerCreation(t, startManagerCreation(client, context.Background(), managerTarget(t, client)))
	if outcome.err != nil {
		t.Fatal(outcome.err)
	}
	invalidated := make(chan struct{})
	go func() {
		client.invalidate(outcome.active)
		close(invalidated)
	}()
	managerSignal(t, active.stopEntered, "invalidation did not stop session")
	closed := startManagerClose(client)
	managerSignal(t, client.done, "Close did not start")
	managerCloseBlocked(t, closed)
	release.Do(func() { close(active.stopGate) })
	managerSignal(t, invalidated, "invalidation did not finish")
	managerSignal(t, closed, "Close did not join invalidation Stop")
	if active.stopCount() != 1 {
		t.Fatalf("stop count=%d", active.stopCount())
	}
}

func TestSessionCreationStaleRequestAbandonsObservedTarget(t *testing.T) {
	for _, observation := range []struct {
		name    string
		fixture mgrMapFixture
	}{
		{name: "different target", fixture: mgrMapFixture{name: "b", gid: 8}},
		{name: "unavailable", fixture: mgrMapFixture{unavailable: true}},
	} {
		t.Run(observation.name, func(t *testing.T) {
			original := testMgrMap(t, mgrMapFixture{name: "a"})
			source := &fakeMgrMapSource{mgrMap: original}
			gates := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
			var releases [2]sync.Once
			var entries atomic.Int32
			contexts := make(chan context.Context, 2)
			created := [2]*fakeMgrSession{{}, {}}
			client := newTestManagerClient(t, source, func(ctx context.Context, _ ActiveTarget) (session, error) {
				index := int(entries.Add(1)) - 1
				if index >= len(gates) {
					return nil, errors.New("unexpected creation attempt")
				}
				contexts <- ctx
				<-gates[index]
				return created[index], nil
			})
			defer client.Close()
			defer func() {
				for index := range gates {
					releases[index].Do(func() { close(gates[index]) })
				}
			}()
			target := managerTarget(t, client)
			first := startManagerCreation(client, context.Background(), target)
			var firstContext context.Context
			select {
			case firstContext = <-contexts:
			case <-time.After(time.Second):
				t.Fatal("first factory did not start")
			}
			source.set(testMgrMap(t, observation.fixture))
			if active, err := client.getSession(context.Background(), target); !errors.Is(err, ErrNoActiveManager) || active.session != nil {
				t.Fatalf("stale request: active=%+v err=%v", active, err)
			}
			if !errors.Is(firstContext.Err(), context.Canceled) {
				t.Fatalf("observed target change did not cancel first factory: %v", firstContext.Err())
			}
			source.set(original)
			second := startManagerCreation(client, context.Background(), target)
			select {
			case secondContext := <-contexts:
				if secondContext.Err() != nil {
					t.Fatalf("fresh factory canceled: %v", secondContext.Err())
				}
			case <-time.After(time.Second):
				t.Fatal("restored target reused abandoned first factory")
			}
			releases[1].Do(func() { close(gates[1]) })
			if outcome := receiveManagerCreation(t, second); outcome.err != nil || outcome.active.session != created[1] {
				t.Fatalf("fresh creation: %+v", outcome)
			}
			releases[0].Do(func() { close(gates[0]) })
			if outcome := receiveManagerCreation(t, first); !errors.Is(outcome.err, ErrNoActiveManager) || outcome.active.session != nil {
				t.Fatalf("abandoned creation: %+v", outcome)
			}
			if created[0].stopCount() != 1 || created[1].stopCount() != 0 || entries.Load() != 2 {
				t.Fatalf("old stops=%d fresh stops=%d entries=%d", created[0].stopCount(), created[1].stopCount(), entries.Load())
			}
			if active, err := client.getSession(context.Background(), target); err != nil || active.session != created[1] {
				t.Fatalf("cached fresh session: active=%+v err=%v", active, err)
			}
		})
	}
}

func TestSessionCachedTargetRequiresAuthoritativeMatch(t *testing.T) {
	source := &fakeMgrMapSource{mgrMap: testMgrMap(t, mgrMapFixture{name: "a"})}
	cached := &fakeMgrSession{}
	client := newTestManagerClient(t, source, func(context.Context, ActiveTarget) (session, error) {
		return cached, nil
	})
	defer client.Close()
	target := managerTarget(t, client)
	if active, err := client.getSession(context.Background(), target); err != nil || active.session != cached {
		t.Fatalf("initial session: active=%+v err=%v", active, err)
	}
	for _, fixture := range []mgrMapFixture{{name: "b", gid: 8}, {unavailable: true}} {
		source.set(testMgrMap(t, fixture))
		if active, err := client.getSession(context.Background(), target); !errors.Is(err, ErrNoActiveManager) || active.session != nil {
			t.Fatalf("stale cached request: active=%+v err=%v", active, err)
		}
	}
}

func TestSessionCreationRapidTargetReuseRejectsOldAttempt(t *testing.T) {
	source := &fakeMgrMapSource{mgrMap: testMgrMap(t, mgrMapFixture{name: "a"})}
	gates := []chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	created := []*fakeMgrSession{{}, {}, {}}
	var releases [3]sync.Once
	var entries atomic.Int32
	contexts := make(chan context.Context, 3)
	client := newTestManagerClient(t, source, func(ctx context.Context, _ ActiveTarget) (session, error) {
		index := int(entries.Add(1)) - 1
		if index >= len(gates) {
			return nil, errors.New("unexpected creation attempt")
		}
		contexts <- ctx
		<-gates[index]
		return created[index], nil
	})
	defer client.Close()
	defer func() {
		for index := range gates {
			releases[index].Do(func() { close(gates[index]) })
		}
	}()
	var results []<-chan managerCreationResult
	var factoryContexts []context.Context
	for index, fixture := range []mgrMapFixture{{name: "a"}, {name: "b", gid: 8}, {name: "a", epoch: 44}} {
		source.set(testMgrMap(t, fixture))
		results = append(results, startManagerCreation(client, context.Background(), managerTarget(t, client)))
		select {
		case ctx := <-contexts:
			factoryContexts = append(factoryContexts, ctx)
		case <-time.After(time.Second):
			t.Fatalf("factory attempt %d did not start", index)
		}
	}
	for _, ctx := range factoryContexts[:2] {
		managerSignal(t, ctx.Done(), "superseded factory context was not canceled")
	}
	if factoryContexts[2].Err() != nil {
		t.Fatal("latest factory was canceled")
	}
	releases[2].Do(func() { close(gates[2]) })
	if outcome := receiveManagerCreation(t, results[2]); outcome.err != nil || outcome.active.session != created[2] {
		t.Fatalf("latest creation: %+v", outcome)
	}
	for _, index := range []int{1, 0} {
		releases[index].Do(func() { close(gates[index]) })
		if outcome := receiveManagerCreation(t, results[index]); !errors.Is(outcome.err, ErrNoActiveManager) || outcome.active.session != nil {
			t.Fatalf("superseded creation %d: %+v", index, outcome)
		}
		if created[index].stopCount() != 1 {
			t.Fatalf("superseded session %d stops=%d", index, created[index].stopCount())
		}
	}
	if outcome := receiveManagerCreation(t, startManagerCreation(client, context.Background(), managerTarget(t, client))); outcome.err != nil || outcome.active.session != created[2] {
		t.Fatalf("old attempt overwrote latest session: %+v", outcome)
	}
	client.Close()
	if entries.Load() != 3 || created[2].stopCount() != 1 {
		t.Fatalf("entries=%d latest stops=%d", entries.Load(), created[2].stopCount())
	}
}

func TestSessionCreationEpochOnlyChangeReusesSession(t *testing.T) {
	source := &fakeMgrMapSource{mgrMap: testMgrMap(t, mgrMapFixture{})}
	var entries atomic.Int32
	created := &fakeMgrSession{}
	client := newTestManagerClient(t, source, func(context.Context, ActiveTarget) (session, error) {
		entries.Add(1)
		return created, nil
	})
	defer client.Close()
	if outcome := receiveManagerCreation(t, startManagerCreation(client, context.Background(), managerTarget(t, client))); outcome.err != nil {
		t.Fatal(outcome.err)
	}
	source.set(testMgrMap(t, mgrMapFixture{epoch: 43}))
	if outcome := receiveManagerCreation(t, startManagerCreation(client, context.Background(), managerTarget(t, client))); outcome.err != nil || outcome.active.session != created {
		t.Fatalf("epoch-only update: %+v", outcome)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.getSession(ctx, managerTarget(t, client)); !errors.Is(err, context.Canceled) {
		t.Fatalf("already-canceled cached access: %v", err)
	}
	if entries.Load() != 1 {
		t.Fatalf("epoch-only update factory entries=%d", entries.Load())
	}
}

func TestSessionCreationCloseCancelsFactoryContext(t *testing.T) {
	source := &fakeMgrMapSource{mgrMap: testMgrMap(t, mgrMapFixture{})}
	entered := make(chan struct{})
	exited := make(chan struct{})
	client := newTestManagerClient(t, source, func(ctx context.Context, _ ActiveTarget) (session, error) {
		close(entered)
		<-ctx.Done()
		close(exited)
		return nil, ctx.Err()
	})
	defer client.Close()
	result := startManagerCreation(client, context.Background(), managerTarget(t, client))
	managerSignal(t, entered, "factory did not start")
	closed := startManagerClose(client)
	managerSignal(t, closed, "Close did not cancel and join factory")
	managerSignal(t, exited, "factory did not exit")
	if outcome := receiveManagerCreation(t, result); !errors.Is(outcome.err, ErrClosed) {
		t.Fatalf("closed creation: %v", outcome.err)
	}
}

func TestSessionCreationCanceledAttemptCannotReplaceRetry(t *testing.T) {
	source := &fakeMgrMapSource{mgrMap: testMgrMap(t, mgrMapFixture{})}
	factoryGate := make(chan struct{})
	factoryContext := make(chan context.Context, 1)
	loser := &gatedManagerSession{stopEntered: make(chan struct{}), stopGate: make(chan struct{})}
	cached := &fakeMgrSession{}
	var entries atomic.Int32
	var releaseFactory, releaseStop sync.Once
	client := newTestManagerClient(t, source, func(ctx context.Context, _ ActiveTarget) (session, error) {
		if entries.Add(1) == 1 {
			factoryContext <- ctx
			<-factoryGate
			return loser, nil
		}
		return cached, nil
	})
	defer client.Close()
	defer releaseStop.Do(func() { close(loser.stopGate) })
	defer releaseFactory.Do(func() { close(factoryGate) })
	target := managerTarget(t, client)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := startManagerCreation(client, ctx, target)
	var factoryCtx context.Context
	select {
	case factoryCtx = <-factoryContext:
	case <-time.After(time.Second):
		t.Fatal("factory did not start")
	}
	cancel()
	if outcome := receiveManagerCreation(t, first); !errors.Is(outcome.err, context.Canceled) {
		t.Fatalf("canceled waiter: %v", outcome.err)
	}
	managerSignal(t, factoryCtx.Done(), "abandoned factory context not canceled")
	if outcome := receiveManagerCreation(t, startManagerCreation(client, context.Background(), target)); outcome.err != nil || outcome.active.session != cached {
		t.Fatalf("retry blocked by abandoned factory: %+v", outcome)
	}
	releaseFactory.Do(func() { close(factoryGate) })
	managerSignal(t, loser.stopEntered, "late canceled factory session not stopped")
	if outcome := receiveManagerCreation(t, startManagerCreation(client, context.Background(), target)); outcome.err != nil || outcome.active.session != cached {
		t.Fatalf("cached access blocked or replaced by loser Stop: %+v", outcome)
	}
	closed := startManagerClose(client)
	managerSignal(t, client.done, "Close did not start")
	managerCloseBlocked(t, closed)
	releaseStop.Do(func() { close(loser.stopGate) })
	managerSignal(t, closed, "Close did not join abandoned attempt")
	if entries.Load() != 2 || loser.stopCount() != 1 || cached.stopCount() != 1 {
		t.Fatalf("entries=%d loser stops=%d cached stops=%d", entries.Load(), loser.stopCount(), cached.stopCount())
	}
}
