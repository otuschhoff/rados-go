package mon

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	wire "github.com/otuschhoff/go-librados/internal/encoding"
	"github.com/otuschhoff/go-librados/internal/maps"
	"github.com/otuschhoff/go-librados/internal/msgr"
	"github.com/otuschhoff/go-librados/internal/protocol"
)

type fakeMonitorSession struct {
	incoming    chan msgr.Message
	events      chan msgr.SessionEvent
	terminal    chan error
	done        chan struct{}
	sends       chan msgr.Message
	submits     chan msgr.Message
	submitReply msgr.Message
	submitErr   error
	once        sync.Once
}

func TestAuthoritySessionPublishesOnceAfterFSIDAcceptance(t *testing.T) {
	observed := 0
	active := &authoritySession{publish: func() { observed++ }}
	if observed != 0 {
		t.Fatal("authority published before accepted FSID")
	}
	publishAuthority(active)
	publishAuthority(active)
	if observed != 1 {
		t.Fatalf("observed=%d", observed)
	}
}

func newFakeMonitorSession() *fakeMonitorSession {
	return &fakeMonitorSession{incoming: make(chan msgr.Message, 8), events: make(chan msgr.SessionEvent, 8), terminal: make(chan error, 1), done: make(chan struct{}), sends: make(chan msgr.Message, 8), submits: make(chan msgr.Message, 8)}
}

func (session *fakeMonitorSession) Send(_ context.Context, message msgr.Message) error {
	session.sends <- message
	return nil
}

func (session *fakeMonitorSession) Submit(_ context.Context, message msgr.Message) (msgr.Message, error) {
	session.submits <- message
	return session.submitReply, session.submitErr
}
func (session *fakeMonitorSession) Incoming() <-chan msgr.Message    { return session.incoming }
func (session *fakeMonitorSession) Events() <-chan msgr.SessionEvent { return session.events }
func (session *fakeMonitorSession) Terminal() <-chan error           { return session.terminal }
func (session *fakeMonitorSession) Done() <-chan struct{}            { return session.done }
func (session *fakeMonitorSession) Stop() {
	session.once.Do(func() { close(session.done) })
}

func TestClientSubscribesPublishesAndFailsOver(t *testing.T) {
	first, second := newFakeMonitorSession(), newFakeMonitorSession()
	sessions := []session{first, second}
	factoryCalls := 0
	factory := func(context.Context, Endpoint) (session, error) {
		if factoryCalls >= len(sessions) {
			return nil, errors.New("no scripted session")
		}
		value := sessions[factoryCalls]
		factoryCalls++
		return value, nil
	}
	client, err := NewClient(testClientConfig(), factory)
	if err != nil {
		t.Fatal(err)
	}
	connectResult := make(chan error, 1)
	go func() { connectResult <- client.Connect(context.Background()) }()
	assertSubscription(t, <-first.sends, 0, 0)
	first.incoming <- lifecycleMonMapMessage(t, testFSID(), 1)
	first.incoming <- lifecycleOSDMapBatchMessage(t, testFSID(), 1)
	select {
	case err := <-connectResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("client did not become ready")
	}
	if client.MonMap().Epoch() != 1 || client.OSDMap().Epoch() != 1 {
		t.Fatal("client did not publish initial maps")
	}

	first.incoming <- lifecycleOSDMapGapMessage(t, testFSID(), 3)
	select {
	case err := <-client.Errors():
		if !errors.Is(err, ErrMapGap) {
			t.Fatalf("gap error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("map gap was not reported")
	}
	assertFullMapSubscription(t, <-first.sends)
	first.incoming <- lifecycleOSDMapBatchMessage(t, testFSID(), 3)
	assertSubscription(t, <-first.sends, 2, 4)
	if client.OSDMap().Epoch() != 3 {
		t.Fatal("full-map recovery was not published")
	}

	first.terminal <- msgr.ErrReconnectExhausted
	assertSubscription(t, <-second.sends, 2, 4)
	foreign := testFSID()
	foreign[0] ^= 0xff
	second.incoming <- subscribeAckMessage(t, foreign)
	select {
	case err := <-client.Errors():
		if !errors.Is(err, ErrForeignCluster) {
			t.Fatalf("foreign fsid error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("foreign fsid was not reported")
	}
	client.Close()
}

func TestClientBecomesReadyWhenMonMapArrivesAfterOSDMap(t *testing.T) {
	active := newFakeMonitorSession()
	client, err := NewClient(testClientConfig(), func(context.Context, Endpoint) (session, error) {
		return active, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	connectResult := make(chan error, 1)
	go func() { connectResult <- client.Connect(context.Background()) }()
	assertSubscription(t, <-active.sends, 0, 0)
	active.incoming <- lifecycleOSDMapBatchMessage(t, testFSID(), 1)
	active.incoming <- lifecycleMonMapMessage(t, testFSID(), 1)
	select {
	case err := <-connectResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("client did not become ready after reversed map delivery")
	}
	client.Close()
}

func TestClientPacesTerminalFailover(t *testing.T) {
	first, second := newFakeMonitorSession(), newFakeMonitorSession()
	opened := make(chan struct{}, 2)
	config := testClientConfig()
	config.RetryDelay = 50 * time.Millisecond
	var calls int
	client, err := NewClient(config, func(context.Context, Endpoint) (session, error) {
		calls++
		opened <- struct{}{}
		if calls == 1 {
			return first, nil
		}
		return second, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	go client.Connect(context.Background())
	<-opened
	assertSubscription(t, <-first.sends, 0, 0)
	started := time.Now()
	first.terminal <- msgr.ErrReconnectExhausted
	select {
	case <-opened:
		t.Fatal("terminal failover ignored retry delay")
	case <-time.After(config.RetryDelay / 2):
	}
	select {
	case <-opened:
		if elapsed := time.Since(started); elapsed < config.RetryDelay {
			t.Fatalf("failover opened after %v, want at least %v", elapsed, config.RetryDelay)
		}
	case <-time.After(time.Second):
		t.Fatal("terminal failover did not open replacement")
	}
	client.Close()
}

func TestClientReportsUnsupportedTerminalFailure(t *testing.T) {
	active := newFakeMonitorSession()
	client, err := NewClient(testClientConfig(), func(context.Context, Endpoint) (session, error) {
		return active, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	connectResult := make(chan error, 1)
	go func() { connectResult <- client.Connect(context.Background()) }()
	assertSubscription(t, <-active.sends, 0, 0)
	active.terminal <- fmt.Errorf("missing features: %w", msgr.ErrUnsupportedPayload)
	select {
	case err := <-connectResult:
		if !errors.Is(err, msgr.ErrUnsupportedPayload) {
			t.Fatalf("Connect error = %v, want unsupported payload", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Connect did not report unsupported terminal failure")
	}
	client.Close()
}

func TestClientRejectsForeignSessionAndFailsOver(t *testing.T) {
	foreignSession, expectedSession := newFakeMonitorSession(), newFakeMonitorSession()
	sessions := []session{foreignSession, expectedSession}
	var calls int
	config := testClientConfig()
	expected := testFSID()
	config.ExpectedFSID = &expected
	client, err := NewClient(config, func(context.Context, Endpoint) (session, error) {
		value := sessions[calls]
		calls++
		return value, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	connectResult := make(chan error, 1)
	go func() { connectResult <- client.Connect(context.Background()) }()
	assertSubscription(t, <-foreignSession.sends, 0, 0)
	foreign := expected
	foreign[0] ^= 0xff
	foreignSession.incoming <- subscribeAckMessage(t, foreign)
	select {
	case err := <-client.Errors():
		if !errors.Is(err, ErrForeignCluster) {
			t.Fatalf("foreign error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("foreign cluster was not reported")
	}
	assertSubscription(t, <-expectedSession.sends, 0, 0)
	expectedSession.incoming <- lifecycleMonMapMessage(t, expected, 1)
	expectedSession.incoming <- lifecycleOSDMapBatchMessage(t, expected, 1)
	select {
	case err := <-connectResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("client did not become ready after foreign-session failover")
	}
	client.Close()
}

func TestClientFailsOverWhenActiveMonitorLeavesMonMap(t *testing.T) {
	first, replacement := newFakeMonitorSession(), newFakeMonitorSession()
	var opened []netip.AddrPort
	client, err := NewClient(testClientConfig(), func(_ context.Context, endpoint Endpoint) (session, error) {
		opened = append(opened, endpoint.Address)
		if len(opened) == 1 {
			return first, nil
		}
		return replacement, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	go client.Connect(context.Background())
	assertSubscription(t, <-first.sends, 0, 0)
	first.incoming <- lifecycleMonMapWithMonitors(t, testFSID(), 2, []testMonitor{{name: "replacement", address: "192.0.2.3:3300", priority: 1, weight: 5}})
	select {
	case err := <-client.Errors():
		if !errors.Is(err, errMonitorRemoved) {
			t.Fatalf("removal error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("active monitor removal was not reported")
	}
	assertSubscription(t, <-replacement.sends, 3, 0)
	if len(opened) != 2 || opened[1] != netip.MustParseAddrPort("192.0.2.3:3300") {
		t.Fatalf("opened endpoints = %v", opened)
	}
	client.Close()
}

func TestMonMapCandidatesHonorPriorityAndWeight(t *testing.T) {
	client, err := NewClient(testClientConfig(), func(context.Context, Endpoint) (session, error) { return nil, errors.New("unused") })
	if err != nil {
		t.Fatal(err)
	}
	message := lifecycleMonMapWithMonitors(t, testFSID(), 2, []testMonitor{
		{name: "low-weight", address: "192.0.2.4:3300", priority: 1, weight: 2},
		{name: "high-priority", address: "192.0.2.5:3300", priority: 2, weight: 100},
		{name: "high-weight", address: "192.0.2.6:3300", priority: 1, weight: 9},
	})
	monMap, err := DecodeMonMap(message, client.config.MapLimits)
	if err != nil {
		t.Fatal(err)
	}
	if !client.replaceMonMapEndpoints(monMap) {
		t.Fatal("client without an active endpoint reported removal")
	}
	want := []netip.AddrPort{
		netip.MustParseAddrPort("192.0.2.6:3300"),
		netip.MustParseAddrPort("192.0.2.4:3300"),
		netip.MustParseAddrPort("192.0.2.5:3300"),
	}
	for index := range want {
		if client.config.Endpoints[index].Address != want[index] {
			t.Fatalf("candidate %d = %s, want %s", index, client.config.Endpoints[index].Address, want[index])
		}
	}
	client.Close()
}

func TestClientConcurrentConnectSharesStartupRecovery(t *testing.T) {
	active := newFakeMonitorSession()
	var calls int
	client, err := NewClient(testClientConfig(), func(context.Context, Endpoint) (session, error) {
		calls++
		if calls <= 2 {
			return nil, errors.New("dial failed")
		}
		return active, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 2)
	for range 2 {
		go func() { results <- client.Connect(context.Background()) }()
	}
	assertSubscription(t, <-active.sends, 0, 0)
	active.incoming <- lifecycleMonMapMessage(t, testFSID(), 1)
	active.incoming <- lifecycleOSDMapBatchMessage(t, testFSID(), 1)
	for range 2 {
		select {
		case err := <-results:
			if err != nil {
				t.Fatalf("Connect error = %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("concurrent Connect remained blocked")
		}
	}
	client.Close()
}

func TestClientConnectSurvivesFailoverRetryBeforeReady(t *testing.T) {
	first, recovered := newFakeMonitorSession(), newFakeMonitorSession()
	config := testClientConfig()
	config.RetryDelay = time.Millisecond
	var calls int
	client, err := NewClient(config, func(context.Context, Endpoint) (session, error) {
		calls++
		switch calls {
		case 1:
			return first, nil
		case 2, 3:
			return nil, errors.New("temporary dial failure")
		default:
			return recovered, nil
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	connectResult := make(chan error, 1)
	go func() { connectResult <- client.Connect(context.Background()) }()
	assertSubscription(t, <-first.sends, 0, 0)
	first.terminal <- msgr.ErrReconnectExhausted
	assertSubscription(t, <-recovered.sends, 0, 0)
	recovered.incoming <- lifecycleMonMapMessage(t, testFSID(), 1)
	recovered.incoming <- lifecycleOSDMapBatchMessage(t, testFSID(), 1)
	select {
	case err := <-connectResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Connect did not recover after endpoint retry")
	}
	client.Close()
}

func TestClientResendsPendingFullMapRequestAfterFailover(t *testing.T) {
	first, replacement := newFakeMonitorSession(), newFakeMonitorSession()
	config := testClientConfig()
	config.RetryDelay = time.Millisecond
	var calls int
	client, err := NewClient(config, func(context.Context, Endpoint) (session, error) {
		calls++
		if calls == 1 {
			return first, nil
		}
		return replacement, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	go client.Connect(context.Background())
	assertSubscription(t, <-first.sends, 0, 0)
	first.incoming <- lifecycleMonMapMessage(t, testFSID(), 1)
	first.incoming <- lifecycleOSDMapBatchMessage(t, testFSID(), 1)
	first.incoming <- lifecycleOSDMapGapMessage(t, testFSID(), 3)
	select {
	case err := <-client.Errors():
		if !errors.Is(err, ErrMapGap) {
			t.Fatalf("gap error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("map gap was not reported")
	}
	assertFullMapSubscription(t, <-first.sends)
	first.terminal <- msgr.ErrReconnectExhausted
	assertSubscription(t, <-replacement.sends, 2, 2)
	assertFullMapSubscription(t, <-replacement.sends)
	client.Close()
}

func TestClientSuccessfulMapResponseFinishesRefresh(t *testing.T) {
	active := newFakeMonitorSession()
	client, err := NewClient(testClientConfig(), func(context.Context, Endpoint) (session, error) {
		return active, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	fsid := testFSID()
	base, err := maps.DecodeOSDMap(encodeEmptyOSDMap(t, fsid, 1), client.config.MapLimits)
	if err != nil {
		t.Fatal(err)
	}
	client.pinnedFSID = &fsid
	client.osdMap.Store(base)
	client.refreshPending = true
	if _, err := client.handleMessage(active, lifecycleOSDMapBatchMessage(t, fsid, 1)); err != nil {
		t.Fatal(err)
	}
	assertSubscription(t, <-active.sends, 0, 2)
	if client.refreshPending {
		t.Fatal("successful map response left refresh pending")
	}
	client.Close()
}

func TestClientCloseCancelsBlockedStartupAndWaiters(t *testing.T) {
	factoryStarted := make(chan struct{})
	client, err := NewClient(testClientConfig(), func(ctx context.Context, _ Endpoint) (session, error) {
		select {
		case <-factoryStarted:
		default:
			close(factoryStarted)
		}
		<-ctx.Done()
		return nil, ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	connectResult := make(chan error, 1)
	go func() { connectResult <- client.Connect(context.Background()) }()
	<-factoryStarted
	closed := make(chan struct{})
	go func() {
		client.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close remained blocked by session factory")
	}
	select {
	case err := <-connectResult:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("Connect error = %v, want %v", err, ErrClosed)
		}
	case <-time.After(time.Second):
		t.Fatal("Connect remained blocked after Close")
	}
}

func TestClientCloseStopsSessionOpenedDuringFailover(t *testing.T) {
	first, replacement := newFakeMonitorSession(), newFakeMonitorSession()
	replacementStarted := make(chan struct{})
	config := testClientConfig()
	config.RetryDelay = time.Millisecond
	var calls int
	client, err := NewClient(config, func(ctx context.Context, _ Endpoint) (session, error) {
		calls++
		if calls == 1 {
			return first, nil
		}
		close(replacementStarted)
		<-ctx.Done()
		return replacement, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	go client.Connect(context.Background())
	assertSubscription(t, <-first.sends, 0, 0)
	first.terminal <- msgr.ErrReconnectExhausted
	<-replacementStarted
	closed := make(chan struct{})
	go func() {
		client.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close remained blocked during failover")
	}
	select {
	case <-replacement.done:
	default:
		t.Fatal("replacement session was not stopped")
	}
}

func TestClientCloseBeforeConnect(t *testing.T) {
	client, err := NewClient(testClientConfig(), func(context.Context, Endpoint) (session, error) {
		t.Fatal("factory called after Close")
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	client.Close()
	if err := client.Connect(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Connect error = %v, want %v", err, ErrClosed)
	}
}

func TestApplyBatchDetectsAdvertisedGap(t *testing.T) {
	client, err := NewClient(testClientConfig(), func(context.Context, Endpoint) (session, error) { return nil, errors.New("unused") })
	if err != nil {
		t.Fatal(err)
	}
	base, err := maps.DecodeOSDMap(encodeEmptyOSDMap(t, testFSID(), 2), client.config.MapLimits)
	if err != nil {
		t.Fatal(err)
	}
	client.osdMap.Store(base)
	tests := []OSDMapBatch{
		{FSID: testFSID(), NewestMap: 4},
		{FSID: testFSID(), TrimLowerBound: 3, NewestMap: 4, FullMaps: map[uint32][]byte{1: encodeEmptyOSDMap(t, testFSID(), 1)}},
	}
	for _, batch := range tests {
		if _, err := client.applyBatch(batch); !errors.Is(err, ErrMapGap) {
			t.Fatalf("applyBatch error = %v, want %v", err, ErrMapGap)
		}
	}
	client.Close()
}

func TestReadOnlyCommandAllowlist(t *testing.T) {
	allowed := []string{`{"prefix":"status","format":"json"}`}
	if !isReadOnlyCommand(allowed) {
		t.Fatal("status command rejected")
	}
	for _, command := range [][]string{{`{"prefix":"osd pool delete"}`}, {`not json`}, {`{"prefix":"status"}`, `extra`}} {
		if isReadOnlyCommand(command) {
			t.Fatalf("unsafe command accepted: %v", command)
		}
	}
}

func TestReadOnlyCommandReturnsWireError(t *testing.T) {
	active := newFakeMonitorSession()
	front := wire.NewEncoder(1024)
	encodePaxosHeader(front, 3)
	front.Int32(-13)
	front.String("permission denied")
	front.Uint32(1)
	front.String(`{"prefix":"status"}`)
	encoded, err := front.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	active.submitReply = frontMessage(protocol.MessageMonCommandAck, 1, 0, encoded)
	client, err := NewClient(testClientConfig(), func(context.Context, Endpoint) (session, error) { return active, nil })
	if err != nil {
		t.Fatal(err)
	}
	fsid := testFSID()
	client.pinnedFSID = &fsid
	client.session = active
	reply, err := client.ReadOnlyCommand(context.Background(), []string{`{"prefix":"status"}`}, nil)
	if reply.Result != -13 || !errors.Is(err, protocol.WireErrno(-13)) {
		t.Fatalf("reply=%+v error=%v", reply, err)
	}
	client.Close()
}

func TestCommandAllowsAdministrativePrefix(t *testing.T) {
	active := newFakeMonitorSession()
	command := `{"prefix":"osd pool create","pool":"p11-test"}`
	front := wire.NewEncoder(1024)
	encodePaxosHeader(front, 4)
	front.Int32(0)
	front.String("")
	front.Uint32(1)
	front.String(command)
	encoded, err := front.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	active.submitReply = frontMessage(protocol.MessageMonCommandAck, 1, 0, encoded)
	client, err := NewClient(testClientConfig(), func(context.Context, Endpoint) (session, error) { return active, nil })
	if err != nil {
		t.Fatal(err)
	}
	fsid := testFSID()
	client.pinnedFSID = &fsid
	client.session = active
	if _, err := client.ReadOnlyCommand(context.Background(), []string{command}, nil); !errors.Is(err, ErrReadOnlyCommand) {
		t.Fatalf("read-only error = %v, want %v", err, ErrReadOnlyCommand)
	}
	reply, err := client.Command(context.Background(), []string{command}, []byte("input"))
	if err != nil || reply.Version != 4 {
		t.Fatalf("reply=%+v error=%v", reply, err)
	}
	request := <-active.submits
	if string(request.Data) != "input" {
		t.Fatalf("request input = %q", request.Data)
	}
	client.Close()
}

func TestApplyPoolOperationValidatesReplyAndCurrentMap(t *testing.T) {
	active := newFakeMonitorSession()
	fsid := testFSID()
	active.submitReply = encodePoolOperationReply(t, fsid, 0, 4, nil)
	client, err := NewClient(testClientConfig(), func(context.Context, Endpoint) (session, error) { return active, nil })
	if err != nil {
		t.Fatal(err)
	}
	client.pinnedFSID = &fsid
	client.session = active
	current, err := maps.DecodeOSDMap(encodeEmptyOSDMap(t, fsid, 4), client.config.MapLimits)
	if err != nil {
		t.Fatal(err)
	}
	client.osdMap.Store(current)
	reply, err := client.ApplyPoolOperation(context.Background(), 7, PoolOperationCreateSnapshot, 0, "daily")
	if err != nil {
		t.Fatal(err)
	}
	if reply.Epoch != 4 {
		t.Fatalf("reply = %+v", reply)
	}
	request := <-active.submits
	if request.Header.Type != protocol.MessagePoolOp {
		t.Fatalf("submitted message type = %d", request.Header.Type)
	}
	select {
	case unexpected := <-active.sends:
		t.Fatalf("unexpected map refresh: %+v", unexpected.Header)
	default:
	}
	client.Close()
}

func TestApplyPoolOperationRejectsWireErrorAndForeignFSID(t *testing.T) {
	fsid := testFSID()
	for _, test := range []struct {
		name    string
		reply   msgr.Message
		wantErr error
	}{
		{"wire error", encodePoolOperationReply(t, fsid, -13, 9, nil), protocol.WireErrno(-13)},
		{"foreign fsid", encodePoolOperationReply(t, maps.FSID{99}, 0, 9, nil), ErrForeignCluster},
	} {
		t.Run(test.name, func(t *testing.T) {
			active := newFakeMonitorSession()
			active.submitReply = test.reply
			client, err := NewClient(testClientConfig(), func(context.Context, Endpoint) (session, error) { return active, nil })
			if err != nil {
				t.Fatal(err)
			}
			client.pinnedFSID = &fsid
			client.session = active
			_, err = client.ApplyPoolOperation(context.Background(), 7, PoolOperationCreateSnapshot, 0, "daily")
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("error = %v, want %v", err, test.wantErr)
			}
			client.Close()
		})
	}
}

func TestResolveSeedsExplicitDNSAndSRV(t *testing.T) {
	resolver := fakeResolver{
		ips: map[string][]netip.Addr{"mon.example": {netip.MustParseAddr("192.0.2.2")}, "srv.example": {netip.MustParseAddr("2001:db8::2")}},
		srv: []*net.SRV{{Target: "srv.example.", Port: 4400}},
	}
	endpoints, err := ResolveSeeds(context.Background(), []string{"v2:192.0.2.1:3300/7", "mon.example", "dns-srv:example"}, resolver, SeedLimits{MaxSeeds: 4, MaxAddresses: 4})
	if err != nil {
		t.Fatal(err)
	}
	want := []netip.AddrPort{netip.MustParseAddrPort("192.0.2.1:3300"), netip.MustParseAddrPort("192.0.2.2:3300"), netip.MustParseAddrPort("[2001:db8::2]:4400")}
	for index := range want {
		if endpoints[index].Address != want[index] {
			t.Fatalf("endpoint %d = %s, want %s", index, endpoints[index].Address, want[index])
		}
	}
	if _, err := ResolveSeeds(context.Background(), []string{"v1:192.0.2.1:6789"}, resolver, SeedLimits{MaxSeeds: 1, MaxAddresses: 1}); !errors.Is(err, wire.ErrUnsupportedVersion) {
		t.Fatalf("v1 error = %v", err)
	}
}

type fakeResolver struct {
	ips map[string][]netip.Addr
	srv []*net.SRV
}

func (resolver fakeResolver) LookupNetIP(_ context.Context, _, host string) ([]netip.Addr, error) {
	return resolver.ips[host], nil
}
func (resolver fakeResolver) LookupSRV(context.Context, string, string, string) (string, []*net.SRV, error) {
	return "", resolver.srv, nil
}

func testClientConfig() ClientConfig {
	return ClientConfig{
		Endpoints:       []Endpoint{{Address: netip.MustParseAddrPort("192.0.2.1:3300")}, {Address: netip.MustParseAddrPort("192.0.2.2:3300")}},
		MapLimits:       maps.Limits{MaxBytes: 64 << 10, MaxMonitors: 8, MaxAddresses: 8, MaxLocations: 8, MaxPools: 8, MaxOSDs: 64, MaxPGMappings: 128, MaxCollectionEntries: 128},
		MessageLimits:   MessageLimits{MaxBytes: 64 << 10, MaxMaps: 8},
		CommandItems:    8,
		SubscribePeriod: time.Hour,
		RetryDelay:      time.Millisecond,
	}
}

func assertSubscription(t *testing.T, message msgr.Message, monStart, osdStart uint64) {
	t.Helper()
	decoder := wire.NewDecoder(message.Front, wire.Limits{MaxBytes: 1024})
	if decoder.Uint32() != 3 || decoder.String() != "mgrmap" || decoder.Uint64() != 0 {
		t.Fatalf("unexpected mgrmap subscription: %x", message.Front)
	}
	decoder.Uint8()
	if decoder.String() != "monmap" || decoder.Uint64() != monStart {
		t.Fatalf("unexpected monmap subscription: %x", message.Front)
	}
	decoder.Uint8()
	if decoder.String() != "osdmap" || decoder.Uint64() != osdStart {
		t.Fatalf("unexpected osdmap subscription: %x", message.Front)
	}
}

func assertFullMapSubscription(t *testing.T, message msgr.Message) {
	t.Helper()
	decoder := wire.NewDecoder(message.Front, wire.Limits{MaxBytes: 1024})
	if decoder.Uint32() != 1 || decoder.String() != "osdmap" || decoder.Uint64() != 0 || decoder.Uint8() != SubscribeOnce {
		t.Fatalf("unexpected full-map subscription: %x", message.Front)
	}
}

func testFSID() maps.FSID {
	return maps.FSID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
}

func lifecycleMonMapMessage(t *testing.T, fsid maps.FSID, epoch uint32) msgr.Message {
	t.Helper()
	encoder := wire.NewEncoder(1024)
	encoder.Versioned(9, 6, func(payload *wire.Encoder) {
		payload.Raw(fsid[:])
		payload.Uint32(epoch)
		payload.Raw(make([]byte, 16))
		for range 2 {
			payload.Versioned(1, 1, func(features *wire.Encoder) { features.Uint64(0) })
		}
		payload.Uint32(0)
		payload.Uint32(0)
		payload.Uint8(20)
		payload.Uint32(0)
		payload.Uint8(1)
		payload.Uint32(0)
		payload.Bool(false)
		payload.String("")
		payload.Uint32(0)
	})
	encoded, _ := encoder.BytesResult()
	outer := wire.NewEncoder(2048)
	outer.Bytes(encoded)
	front, _ := outer.BytesResult()
	return frontMessage(protocol.MessageMonMap, 0, 0, front)
}

type testMonitor struct {
	name     string
	address  string
	priority uint16
	weight   uint16
}

func lifecycleMonMapWithMonitors(t *testing.T, fsid maps.FSID, epoch uint32, monitors []testMonitor) msgr.Message {
	t.Helper()
	encoder := wire.NewEncoder(64 << 10)
	encoder.Versioned(9, 6, func(payload *wire.Encoder) {
		payload.Raw(fsid[:])
		payload.Uint32(epoch)
		payload.Raw(make([]byte, 16))
		for range 2 {
			payload.Versioned(1, 1, func(features *wire.Encoder) { features.Uint64(0) })
		}
		payload.Uint32(uint32(len(monitors)))
		for _, monitor := range monitors {
			address, err := protocol.IPv4EntityAddr(protocol.AddressV2, 1, netip.MustParseAddrPort(monitor.address))
			if err != nil {
				t.Fatal(err)
			}
			payload.String(monitor.name)
			payload.Versioned(5, 1, func(info *wire.Encoder) {
				info.String(monitor.name)
				if err := (protocol.EntityAddrVec{address}).Encode(info, protocol.FeatureMessageAddress2|protocol.FeatureServerNautilus); err != nil {
					t.Fatal(err)
				}
				info.Uint16(monitor.priority)
				info.Uint16(monitor.weight)
				info.Uint32(0)
			})
		}
		payload.Uint32(uint32(len(monitors)))
		for _, monitor := range monitors {
			payload.String(monitor.name)
		}
		payload.Uint8(20)
		payload.Uint32(0)
		payload.Uint8(1)
		payload.Uint32(0)
		payload.Bool(false)
		payload.String("")
		payload.Uint32(0)
	})
	encoded, err := encoder.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	outer := wire.NewEncoder(64 << 10)
	outer.Bytes(encoded)
	front, err := outer.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	return frontMessage(protocol.MessageMonMap, 0, 0, front)
}

func lifecycleOSDMapBatchMessage(t *testing.T, fsid maps.FSID, epoch uint32) msgr.Message {
	t.Helper()
	full := encodeEmptyOSDMap(t, fsid, epoch)
	encoder := wire.NewEncoder(64 << 10)
	encoder.Raw(fsid[:])
	encoder.Uint32(0)
	encoder.Uint32(1)
	encoder.Uint32(epoch)
	encoder.Bytes(full)
	encoder.Uint32(0)
	encoder.Uint32(epoch)
	encoder.Uint32(0)
	front, _ := encoder.BytesResult()
	return frontMessage(protocol.MessageOSDMap, 4, 3, front)
}

func lifecycleOSDMapGapMessage(t *testing.T, fsid maps.FSID, epoch uint32) msgr.Message {
	t.Helper()
	encoder := wire.NewEncoder(1024)
	encoder.Raw(fsid[:])
	encoder.Uint32(1)
	encoder.Uint32(epoch)
	encoder.Bytes(nil)
	encoder.Uint32(0)
	encoder.Uint32(0)
	encoder.Uint32(epoch)
	encoder.Uint32(0)
	front, _ := encoder.BytesResult()
	return frontMessage(protocol.MessageOSDMap, 4, 3, front)
}

func encodeEmptyOSDMap(t *testing.T, fsid maps.FSID, epoch uint32) []byte {
	t.Helper()
	encoder := wire.NewEncoder(64 << 10)
	encoder.Versioned(8, 7, func(wrapper *wire.Encoder) {
		wrapper.Versioned(10, 1, func(client *wire.Encoder) {
			client.Raw(fsid[:])
			client.Uint32(epoch)
			client.Raw(make([]byte, 16))
			for range 2 {
				client.Uint32(0)
			}
			client.Int32(0)
			client.Uint32(0)
			client.Int32(0)
			for range 3 {
				client.Uint32(0)
			}
			client.Uint32(0)
			client.Uint32(0)
			client.Uint32(0)
			client.Bytes(nil)
			for range 3 {
				client.Uint32(0)
			}
			client.Uint32(0)
			client.Uint32(0)
			client.Uint32(0)
			client.Raw(make([]byte, 16))
			client.Uint32(0)
		})
		wrapper.Versioned(12, 1, func(*wire.Encoder) {})
		wrapper.Uint32(0)
	})
	data, _ := encoder.BytesResult()
	crcOffset := len(data) - 4
	binary.LittleEndian.PutUint32(data[crcOffset:], wire.CRC32C(^uint32(0), data[:crcOffset]))
	return data
}

func subscribeAckMessage(t *testing.T, fsid maps.FSID) msgr.Message {
	t.Helper()
	encoder := wire.NewEncoder(32)
	encoder.Uint32(60)
	encoder.Raw(fsid[:])
	front, _ := encoder.BytesResult()
	return frontMessage(protocol.MessageMonSubscribeAck, 0, 0, front)
}
