package rados

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/otuschhoff/rados-go/internal/msgr"
)

func TestClientReceiveBudgetSharedServiceAdmission(t *testing.T) {
	client, err := New(Config{
		Monitors: []string{"127.0.0.1:3300"}, Entity: "client.test", Key: []byte(testPublicKey),
		MaxSessions: 2, MaxReceiveBytes: 2 << 20, MaxQueuedReceiveBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var calls atomic.Int32
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	connector := msgr.ConnectorFunc(func(connectCtx context.Context) (msgr.Transport, error) {
		calls.Add(1)
		close(entered)
		<-connectCtx.Done()
		close(canceled)
		<-release
		return nil, connectCtx.Err()
	})
	base := msgr.SessionConfig{
		Limits:        msgr.Limits{MaxSegmentBytes: 1 << 20, MaxFrameBytes: 2 << 20},
		ReceiveBudget: client.receiveBudget, MaxQueuedReceiveBytes: client.config.MaxQueuedReceiveBytes,
		MaxQueuedMessages: 4, MaxRetainedBytes: 1 << 20, MaxInFlightTransactions: 4,
		MaxHandshakeTransitions: 8, EventBuffer: 16,
	}
	newService := func(service string, connector msgr.Connector) (*msgr.Session, error) {
		config := base
		config.DiagnosticService = service
		return msgr.NewSession(nil, connector, config)
	}
	monitor, err := newService("mon", connector)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { unblock(); monitor.Stop() })
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("monitor connector did not start")
	}
	osdConnector := msgr.ConnectorFunc(func(connectCtx context.Context) (msgr.Transport, error) {
		<-connectCtx.Done()
		return nil, connectCtx.Err()
	})
	osd, err := newService("osd", osdConnector)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(osd.Stop)
	var managerCalls atomic.Int32
	managerConnector := msgr.ConnectorFunc(func(connectCtx context.Context) (msgr.Transport, error) {
		managerCalls.Add(1)
		<-connectCtx.Done()
		return nil, connectCtx.Err()
	})
	assertRejected := func() {
		t.Helper()
		unexpected, err := newService("mgr", managerConnector)
		if unexpected != nil {
			unexpected.Stop()
			t.Fatal("manager admitted above shared session limit")
		}
		if !errors.Is(err, msgr.ErrReceiveBudgetExceeded) || !errors.Is(err, msgr.ErrQueueSaturated) {
			t.Fatalf("manager admission error = %v", err)
		}
		if managerCalls.Load() != 0 {
			t.Fatal("rejected admission invoked manager connector")
		}
		if ledger := client.receiveBudget.Snapshot(); ledger.Sessions != 2 {
			t.Fatalf("active sessions evicted: %+v", ledger)
		}
		select {
		case <-osd.Done():
			t.Fatal("OSD evicted by manager admission")
		default:
		}
	}
	assertRejected()
	select {
	case <-monitor.Done():
		t.Fatal("monitor evicted by manager admission")
	default:
	}
	var stops sync.WaitGroup
	for range 8 {
		stops.Add(1)
		go func() { defer stops.Done(); monitor.Stop() }()
	}
	select {
	case <-canceled:
	case <-ctx.Done():
		t.Fatal("Stop did not cancel monitor connector")
	}
	assertRejected()
	unblock()
	joined := make(chan struct{})
	go func() { stops.Wait(); close(joined) }()
	select {
	case <-joined:
	case <-ctx.Done():
		t.Fatal("concurrent Stop calls did not join")
	}
	manager, err := newService("mgr", managerConnector)
	if err != nil {
		t.Fatalf("joined Stop did not free shared slot: %v", err)
	}
	t.Cleanup(manager.Stop)
	if ledger := client.receiveBudget.Snapshot(); ledger.Sessions != 2 || calls.Load() != 1 {
		t.Fatalf("replacement admission ledger=%+v monitor calls=%d", ledger, calls.Load())
	}
	manager.Stop()
	osd.Stop()
	if ledger := client.receiveBudget.Snapshot(); ledger != (msgr.ReceiveBudgetSnapshot{}) {
		t.Fatalf("joined service shutdown leaked ledger: %+v", ledger)
	}
}
