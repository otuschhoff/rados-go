package cephx

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	wire "github.com/otuschhoff/rados-go/internal/encoding"
	"github.com/otuschhoff/rados-go/internal/msgr"
	"github.com/otuschhoff/rados-go/internal/protocol"
)

var connectorTestMsgLimits = msgr.Limits{
	MaxSegmentBytes: 1 << 16,
	MaxFrameBytes:   1 << 20,
	MaxAddresses:    8,
	MaxAuthBytes:    1 << 16,
}

type renewalTestTransport struct {
	closed chan struct{}
	once   sync.Once
}

func (transport *renewalTestTransport) ReadFrame() (msgr.Frame, error) {
	<-transport.closed
	return msgr.Frame{}, msgr.ErrSessionClosed
}

func (transport *renewalTestTransport) WriteFrame(msgr.Frame) error { return nil }

func (transport *renewalTestTransport) Close() error {
	transport.once.Do(func() { close(transport.closed) })
	return nil
}

func TestConnectorSecureHandshakeAndTransport(t *testing.T) {
	credential, target := testConnectorIdentity(t)
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()

	now := time.Now().UTC()
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- runScriptedServerHandshake(t, serverConn, credential, target, now, ConModeSecure, false, false)
	}()

	connector, err := NewConnector(ConnectorConfig{
		Network:          "tcp",
		Address:          targetAddrString(t, target),
		TargetAddress:    target,
		Credential:       credential,
		Dial:             oneShotDial(clientConn),
		MessageLimits:    connectorTestMsgLimits,
		Limits:           defaultTestLimits(),
		HandshakeTimeout: 2 * time.Second,
		Now:              func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	transport, err := connector.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()
	metaTransport, ok := transport.(MetadataTransport)
	if !ok {
		t.Fatal("transport does not expose auth metadata")
	}
	metadata := metaTransport.AuthMetadata()
	if metadata.GlobalID != 77 || metadata.Mode != ConModeSecure || metadata.Method != AuthMethodCephX {
		t.Fatalf("metadata = %+v", metadata)
	}
	if len(metadata.Tickets) < 2 {
		t.Fatalf("expected ticket metadata, got %+v", metadata.Tickets)
	}
	if connector.NeedsRenewal(uint32(protocol.EntityMonitor), now) {
		t.Fatal("fresh monitor ticket unexpectedly needs renewal")
	}
	authorizer, err := connector.BuildServiceAuthorizer(uint32(protocol.EntityMonitor))
	if err != nil || len(authorizer.Payload) == 0 {
		t.Fatalf("retained monitor authorizer = %d bytes, error = %v", len(authorizer.Payload), err)
	}

	frame, err := transport.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	if frame.Tag != msgr.TagAck {
		t.Fatalf("post-auth frame tag = %d", frame.Tag)
	}

	if err := transport.WriteFrame(msgr.Frame{Tag: msgr.TagKeepalive2Ack, Segments: []msgr.Segment{{Alignment: msgr.DefaultAlignment, Data: []byte("ok")}}}); err != nil {
		t.Fatal(err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestConnectorFailedReconnectPreservesAuthenticatedState(t *testing.T) {
	credential, target := testConnectorIdentity(t)
	clientConn, serverConn := net.Pipe()
	now := time.Now().UTC()
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- runScriptedServerHandshake(t, serverConn, credential, target, now, ConModeSecure, false, false)
	}()
	dials := 0
	connector, err := NewConnector(ConnectorConfig{
		Address: targetAddrString(t, target), TargetAddress: target, Credential: credential,
		Dial: func(context.Context, string, string) (net.Conn, error) {
			dials++
			if dials == 1 {
				return clientConn, nil
			}
			return nil, errors.New("reconnect unavailable")
		},
		MessageLimits: connectorTestMsgLimits, Limits: defaultTestLimits(),
		HandshakeTimeout: 2 * time.Second, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	transport, err := connector.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transport.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	if err := transport.WriteFrame(msgr.Frame{Tag: msgr.TagKeepalive2Ack, Segments: []msgr.Segment{{Alignment: msgr.DefaultAlignment, Data: []byte("ok")}}}); err != nil {
		t.Fatal(err)
	}
	_ = transport.Close()
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
	before := connector.AuthMetadata()
	if _, err := connector.Connect(context.Background()); err == nil {
		t.Fatal("failed reconnect unexpectedly succeeded")
	}
	after := connector.AuthMetadata()
	if before.GlobalID != after.GlobalID || before.Mode != ConModeSecure || after.Mode != before.Mode || len(before.Tickets) != len(after.Tickets) {
		t.Fatalf("state changed after failed reconnect: before=%+v after=%+v", before, after)
	}
}

func TestConnectorConcurrentMetadataAndAuthorizers(t *testing.T) {
	credential, target := testConnectorIdentity(t)
	clientConn, serverConn := net.Pipe()
	now := time.Now().UTC()
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- runScriptedServerHandshake(t, serverConn, credential, target, now, ConModeSecure, false, false)
	}()
	nowCalls := 0
	connector, err := NewConnector(ConnectorConfig{
		Address: targetAddrString(t, target), TargetAddress: target, Credential: credential,
		Dial: oneShotDial(clientConn), MessageLimits: connectorTestMsgLimits, Limits: defaultTestLimits(),
		HandshakeTimeout: 2 * time.Second,
		Now: func() time.Time {
			nowCalls++
			return now
		},
		Rand: bytes.NewReader(bytes.Repeat([]byte{1}, 4096)),
	})
	if err != nil {
		t.Fatal(err)
	}
	transport, err := connector.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	errorsSeen := make(chan error, 32)
	for index := 0; index < 16; index++ {
		group.Add(2)
		go func() {
			defer group.Done()
			if connector.AuthMetadata().GlobalID != 77 {
				errorsSeen <- errors.New("unexpected global ID")
			}
		}()
		go func() {
			defer group.Done()
			if _, err := connector.BuildServiceAuthorizer(uint32(protocol.EntityMonitor)); err != nil {
				errorsSeen <- err
			}
		}()
	}
	group.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		t.Fatal(err)
	}
	if nowCalls == 0 {
		t.Fatal("injected clock was not used")
	}
	if _, err := transport.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	if err := transport.WriteFrame(msgr.Frame{Tag: msgr.TagKeepalive2Ack, Segments: []msgr.Segment{{Alignment: msgr.DefaultAlignment, Data: []byte("ok")}}}); err != nil {
		t.Fatal(err)
	}
	_ = transport.Close()
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestConnectorDoesNotPublishReclaimHintAsAuthenticated(t *testing.T) {
	credential, target := testConnectorIdentity(t)
	connector, err := NewConnector(ConnectorConfig{
		Address: targetAddrString(t, target), TargetAddress: target, Credential: credential,
		GlobalID: 77, MessageLimits: connectorTestMsgLimits, Limits: defaultTestLimits(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if metadata := connector.AuthMetadata(); metadata.GlobalID != 0 || metadata.Method != 0 || metadata.Mode != 0 || metadata.Tickets != nil {
		t.Fatalf("pre-authentication metadata = %+v", metadata)
	}
}

func TestConnectorRejectsNegativeTimeouts(t *testing.T) {
	credential, target := testConnectorIdentity(t)
	for _, config := range []ConnectorConfig{
		{DialTimeout: -time.Nanosecond},
		{HandshakeTimeout: -time.Nanosecond},
	} {
		config.Address = targetAddrString(t, target)
		config.TargetAddress = target
		config.Credential = credential
		config.MessageLimits = connectorTestMsgLimits
		config.Limits = defaultTestLimits()
		if _, err := NewConnector(config); !errors.Is(err, ErrConnectConfig) {
			t.Fatalf("negative timeout error = %v", err)
		}
	}
}

func TestAuthTransportSignalsEarliestRenewalWithoutClosing(t *testing.T) {
	base := &renewalTestTransport{closed: make(chan struct{})}
	now := time.Now()
	transport := newAuthTransport(base, AuthMetadata{Tickets: map[uint32]TicketMetadata{
		1: {RenewAfter: now.Add(time.Hour)},
		2: {RenewAfter: now.Add(10 * time.Millisecond)},
	}}, now.Add(10*time.Millisecond), now)
	defer transport.Close()

	select {
	case <-transport.RenewalDue():
	case <-time.After(time.Second):
		t.Fatal("authenticated transport did not signal renewal")
	}
	select {
	case <-base.closed:
		t.Fatal("authenticated transport closed before the session drained")
	default:
	}
}

func TestAuthTransportCredentialIdentityTracksTickets(t *testing.T) {
	metadata := AuthMetadata{GlobalID: 7, Tickets: map[uint32]TicketMetadata{
		2: {SecretID: 20, Fingerprint: [sha256.Size]byte{2}},
		1: {SecretID: 10, Fingerprint: [sha256.Size]byte{1}},
	}}
	first := newAuthTransport(&renewalTestTransport{closed: make(chan struct{})}, metadata, time.Time{}, time.Now())
	reordered := AuthMetadata{GlobalID: 7, Tickets: map[uint32]TicketMetadata{1: metadata.Tickets[1], 2: metadata.Tickets[2]}}
	second := newAuthTransport(&renewalTestTransport{closed: make(chan struct{})}, reordered, time.Time{}, time.Now())
	changed := reordered
	changed.Tickets = copyMetadata(reordered).Tickets
	changedTicket := changed.Tickets[2]
	changedTicket.Fingerprint[0]++
	changed.Tickets[2] = changedTicket
	third := newAuthTransport(&renewalTestTransport{closed: make(chan struct{})}, changed, time.Time{}, time.Now())
	defer first.Close()
	defer second.Close()
	defer third.Close()

	if first.CredentialIdentity() != second.CredentialIdentity() {
		t.Fatal("credential identity depends on ticket map iteration order")
	}
	if first.CredentialIdentity() == third.CredentialIdentity() {
		t.Fatal("credential identity did not change with ticket fingerprint")
	}
	refreshed := copyMetadata(reordered)
	refreshedTicket := refreshed.Tickets[2]
	refreshedTicket.ExpiresAt = time.Now().Add(time.Hour)
	refreshedTicket.RenewAfter = refreshedTicket.ExpiresAt.Add(-time.Minute)
	refreshed.Tickets[2] = refreshedTicket
	fourth := newAuthTransport(&renewalTestTransport{closed: make(chan struct{})}, refreshed, time.Time{}, time.Now())
	defer fourth.Close()
	if first.CredentialIdentity() == fourth.CredentialIdentity() {
		t.Fatal("credential identity did not change with renewed validity")
	}
}

func TestConnectorSerializesConnect(t *testing.T) {
	credential, target := testConnectorIdentity(t)
	firstDialStarted := make(chan struct{})
	secondCallStarted := make(chan struct{})
	secondDialStarted := make(chan struct{})
	releaseFirstDial := make(chan struct{})
	var dialMu sync.Mutex
	dialCount := 0
	connector, err := NewConnector(ConnectorConfig{
		Address: targetAddrString(t, target), TargetAddress: target, Credential: credential,
		Dial: func(context.Context, string, string) (net.Conn, error) {
			dialMu.Lock()
			dialCount++
			current := dialCount
			dialMu.Unlock()
			if current == 1 {
				close(firstDialStarted)
				<-releaseFirstDial
			} else {
				close(secondDialStarted)
			}
			return nil, errors.New("expected dial failure")
		},
		MessageLimits: connectorTestMsgLimits, Limits: defaultTestLimits(),
	})
	if err != nil {
		t.Fatal(err)
	}

	results := make(chan error, 2)
	go func() {
		_, connectErr := connector.Connect(context.Background())
		results <- connectErr
	}()
	<-firstDialStarted
	go func() {
		close(secondCallStarted)
		_, connectErr := connector.Connect(context.Background())
		results <- connectErr
	}()
	<-secondCallStarted
	select {
	case <-secondDialStarted:
		t.Fatal("second Connect entered Dial while the first was active")
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseFirstDial)
	for range 2 {
		if connectErr := <-results; connectErr == nil {
			t.Fatal("Connect unexpectedly succeeded")
		}
	}
	select {
	case <-secondDialStarted:
	default:
		t.Fatal("second Connect never entered Dial")
	}
}

func TestConnectorCanceledWhileWaitingToConnect(t *testing.T) {
	credential, target := testConnectorIdentity(t)
	firstDialStarted := make(chan struct{})
	releaseFirstDial := make(chan struct{})
	connector, err := NewConnector(ConnectorConfig{
		Address: targetAddrString(t, target), TargetAddress: target, Credential: credential,
		Dial: func(context.Context, string, string) (net.Conn, error) {
			close(firstDialStarted)
			<-releaseFirstDial
			return nil, errors.New("expected dial failure")
		},
		MessageLimits: connectorTestMsgLimits, Limits: defaultTestLimits(),
	})
	if err != nil {
		t.Fatal(err)
	}
	firstResult := make(chan error, 1)
	go func() {
		_, connectErr := connector.Connect(context.Background())
		firstResult <- connectErr
	}()
	<-firstDialStarted
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, connectErr := connector.Connect(ctx); !errors.Is(connectErr, context.Canceled) {
		t.Fatalf("queued Connect error = %v, want context cancellation", connectErr)
	}
	close(releaseFirstDial)
	if connectErr := <-firstResult; connectErr == nil {
		t.Fatal("first Connect unexpectedly succeeded")
	}
}

func TestConnectorExpiredAuthTicketClearsReclaimIdentity(t *testing.T) {
	credential, target := testConnectorIdentity(t)
	now := time.Now().UTC()
	connector, err := NewConnector(ConnectorConfig{
		Address: targetAddrString(t, target), TargetAddress: target, Credential: credential,
		MessageLimits: connectorTestMsgLimits, Limits: defaultTestLimits(), Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	connector.state.globalID = 77
	connector.state.tickets = map[uint32]ServiceTicket{
		uint32(protocol.EntityAuth): {
			ServiceID: uint32(protocol.EntityAuth), ExpiresAt: now,
			Ticket: TicketBlob{SecretID: 1, Blob: []byte("expired")},
		},
	}
	if _, err := connector.BuildServiceAuthorizer(uint32(protocol.EntityAuth)); !errors.Is(err, ErrExpiredTicket) {
		t.Fatalf("expired auth authorizer error = %v", err)
	}
	globalID, ticket, key := connector.renewalSnapshot(now)
	if globalID != 0 || ticket.Blob != nil || key != nil {
		t.Fatalf("expired reclaim state = global_id=%d ticket=%v key=%v", globalID, ticket, key)
	}
	metadata := connector.AuthMetadata()
	if metadata.GlobalID != 0 || len(metadata.Tickets) != 0 {
		t.Fatalf("expired published state = %+v", metadata)
	}
}

func TestValidAuthenticatedGlobalID(t *testing.T) {
	if validAuthenticatedGlobalID(0) || validAuthenticatedGlobalID(math.MaxUint64) || !validAuthenticatedGlobalID(math.MaxInt64) {
		t.Fatal("authenticated global ID bounds are invalid")
	}
}

func TestConnectorDefaultsRequestServiceTickets(t *testing.T) {
	config := (ConnectorConfig{}).withDefaults()
	want := uint32(protocol.EntityAuth | protocol.EntityMonitor | protocol.EntityOSD | protocol.EntityManager)
	if config.RequestedKeys != want {
		t.Fatalf("requested keys=%#x want=%#x", config.RequestedKeys, want)
	}
}

func TestConnectorFragmentedHandshake(t *testing.T) {
	credential, target := testConnectorIdentity(t)
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()
	now := time.Now().UTC()
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- runScriptedServerHandshake(t, serverConn, credential, target, now, ConModeSecure, true, false)
	}()

	connector, err := NewConnector(ConnectorConfig{
		Address:          targetAddrString(t, target),
		TargetAddress:    target,
		Credential:       credential,
		Dial:             oneShotDial(clientConn),
		MessageLimits:    connectorTestMsgLimits,
		Limits:           defaultTestLimits(),
		HandshakeTimeout: 2 * time.Second,
		Now:              func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	transport, err := connector.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()
	if _, err := transport.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	if err := transport.WriteFrame(msgr.Frame{Tag: msgr.TagKeepalive2Ack, Segments: []msgr.Segment{{Alignment: msgr.DefaultAlignment, Data: []byte("ok")}}}); err != nil {
		t.Fatal(err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestConnectorRejectsDowngradeByDefault(t *testing.T) {
	credential, target := testConnectorIdentity(t)
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()
	now := time.Now().UTC()
	go func() {
		_ = runScriptedServerHandshake(t, serverConn, credential, target, now, ConModeCRC, false, false)
	}()

	connector, err := NewConnector(ConnectorConfig{
		Address:          targetAddrString(t, target),
		TargetAddress:    target,
		Credential:       credential,
		Dial:             oneShotDial(clientConn),
		MessageLimits:    connectorTestMsgLimits,
		Limits:           defaultTestLimits(),
		HandshakeTimeout: 2 * time.Second,
		Now:              func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = connector.Connect(context.Background())
	if !errors.Is(err, ErrAuthDowngrade) {
		t.Fatalf("error = %v", err)
	}
}

func TestClassifyAuthBadMethod(t *testing.T) {
	rejected := classifyAuthBadMethod(msgr.AuthBadMethod{
		Method: AuthMethodCephX, Result: -13,
		AllowedMethods: []uint32{AuthMethodCephX}, AllowedModes: []uint32{ConModeSecure, ConModeCRC},
	}, []uint32{ConModeSecure})
	if !errors.Is(rejected, ErrAuthRejected) || errors.Is(rejected, ErrAuthDowngrade) {
		t.Fatalf("credential rejection = %v", rejected)
	}

	downgrade := classifyAuthBadMethod(msgr.AuthBadMethod{
		Method: AuthMethodCephX, Result: -95,
		AllowedMethods: []uint32{AuthMethodCephX}, AllowedModes: []uint32{ConModeCRC},
	}, []uint32{ConModeSecure})
	if !errors.Is(downgrade, ErrAuthDowngrade) || errors.Is(downgrade, ErrAuthRejected) {
		t.Fatalf("downgrade = %v", downgrade)
	}
}

func TestConnectorAllowsCRCWhenConfigured(t *testing.T) {
	credential, target := testConnectorIdentity(t)
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()
	now := time.Now().UTC()
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- runScriptedServerHandshake(t, serverConn, credential, target, now, ConModeCRC, false, false)
	}()

	connector, err := NewConnector(ConnectorConfig{
		Address:          targetAddrString(t, target),
		TargetAddress:    target,
		Credential:       credential,
		Dial:             oneShotDial(clientConn),
		MessageLimits:    connectorTestMsgLimits,
		Limits:           defaultTestLimits(),
		HandshakeTimeout: 2 * time.Second,
		AllowCRC:         true,
		Now:              func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	transport, err := connector.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()
	if _, err := transport.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestConnectorRejectsWrongServerSignature(t *testing.T) {
	credential, target := testConnectorIdentity(t)
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()
	now := time.Now().UTC()
	go func() {
		_ = runScriptedServerHandshake(t, serverConn, credential, target, now, ConModeSecure, false, true)
	}()

	connector, err := NewConnector(ConnectorConfig{
		Address:          targetAddrString(t, target),
		TargetAddress:    target,
		Credential:       credential,
		Dial:             oneShotDial(clientConn),
		MessageLimits:    connectorTestMsgLimits,
		Limits:           defaultTestLimits(),
		HandshakeTimeout: 2 * time.Second,
		Now:              func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = connector.Connect(context.Background())
	if !errors.Is(err, ErrAuthSignature) {
		t.Fatalf("error = %v", err)
	}
}

func TestConnectorContextCancellation(t *testing.T) {
	credential, target := testConnectorIdentity(t)
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()

	go func() {
		_, _ = msgr.ReadBanner(serverConn, 64)
		<-make(chan struct{})
	}()

	connector, err := NewConnector(ConnectorConfig{
		Address:          targetAddrString(t, target),
		TargetAddress:    target,
		Credential:       credential,
		Dial:             oneShotDial(clientConn),
		MessageLimits:    connectorTestMsgLimits,
		Limits:           defaultTestLimits(),
		HandshakeTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	_, err = connector.Connect(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancellation error = %v", err)
	}
}

func TestConnectorHandshakeDeadline(t *testing.T) {
	credential, target := testConnectorIdentity(t)
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()

	go func() {
		<-make(chan struct{})
	}()

	connector, err := NewConnector(ConnectorConfig{
		Address:          targetAddrString(t, target),
		TargetAddress:    target,
		Credential:       credential,
		Dial:             oneShotDial(clientConn),
		MessageLimits:    connectorTestMsgLimits,
		Limits:           defaultTestLimits(),
		HandshakeTimeout: 120 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = connector.Connect(context.Background())
	if err == nil {
		t.Fatal("expected handshake timeout")
	}
}

func TestCancelWatcherStopsBeforeSuccessfulReturn(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	stop := watchCancel(ctx, clientConn)
	stop()
	stop()
	cancel()
	if err := clientConn.SetDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := serverConn.Write([]byte{1})
		done <- err
	}()
	var value [1]byte
	if _, err := clientConn.Read(value[:]); err != nil {
		t.Fatalf("stopped cancellation watcher poisoned connection: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	_ = clientConn.Close()
	_ = serverConn.Close()
}

func runScriptedServerHandshake(
	t *testing.T,
	serverConn net.Conn,
	credential Credential,
	target protocol.EntityAddr,
	now time.Time,
	mode uint32,
	fragment bool,
	wrongServerSignature bool,
) error {
	t.Helper()
	tap := newTranscriptConn(serverConn)
	codec := msgr.CRCCodec{WithDataCRC: true}

	if _, err := msgr.ReadBanner(tap, 64); err != nil {
		return fmt.Errorf("read client banner: %w", err)
	}
	if err := writeFragmented(tap, msgr.ClientBanner().Encode(), fragment); err != nil {
		return fmt.Errorf("write banner: %w", err)
	}

	helloPayload, err := readControl(codec, tap, connectorTestMsgLimits)
	if err != nil {
		return fmt.Errorf("read hello: %w", err)
	}
	hello, ok := helloPayload.(msgr.Hello)
	if !ok {
		return fmt.Errorf("hello type %T", helloPayload)
	}
	if hello.EntityType != protocol.EntityClient {
		return fmt.Errorf("client entity=%d", hello.EntityType)
	}
	if err := writeControlWithFragment(codec, tap, connectorTestMsgLimits, msgr.Hello{EntityType: protocol.EntityMonitor, PeerAddress: target}, fragment); err != nil {
		return fmt.Errorf("write hello: %w", err)
	}

	requestPayload, err := readControl(codec, tap, connectorTestMsgLimits)
	if err != nil {
		return fmt.Errorf("read auth request: %w", err)
	}
	request, ok := requestPayload.(msgr.AuthRequest)
	if !ok {
		return fmt.Errorf("auth request type %T", requestPayload)
	}
	if request.Method != AuthMethodCephX {
		return fmt.Errorf("auth method=%d", request.Method)
	}
	if len(request.PreferredModes) == 0 || request.PreferredModes[0] != ConModeSecure {
		return fmt.Errorf("preferred modes=%v", request.PreferredModes)
	}
	challengePayload := wire.NewEncoder(defaultTestLimits().MaxAuthBytes)
	challengePayload.Uint8(1)
	challengePayload.Uint64(0x0102030405060708)
	challengeBytes, err := challengePayload.BytesResult()
	if err != nil {
		return err
	}
	if err := writeControlWithFragment(codec, tap, connectorTestMsgLimits, msgr.AuthReplyMore{AuthPayload: challengeBytes}, fragment); err != nil {
		return fmt.Errorf("write auth reply more: %w", err)
	}

	morePayload, err := readControl(codec, tap, connectorTestMsgLimits)
	if err != nil {
		return fmt.Errorf("read auth request more: %w", err)
	}
	more, ok := morePayload.(msgr.AuthRequestMore)
	if !ok {
		return fmt.Errorf("auth request more type %T", morePayload)
	}
	decoder := wire.NewDecoder(more.AuthPayload, wire.Limits{MaxBytes: defaultTestLimits().MaxAuthBytes})
	if got := decoder.Uint16(); got != cephxGetAuthSessionKey {
		return fmt.Errorf("request more type=%#x", got)
	}
	if err := decoder.Finish(); err != nil {
		return err
	}

	authDonePayload, sessionKey, connectionSecret, err := scriptedAuthDonePayload(t, credential, now, mode)
	if err != nil {
		return err
	}
	if err := writeControlWithFragment(codec, tap, connectorTestMsgLimits, msgr.AuthDone{GlobalID: 77, ConnectionMode: mode, AuthPayload: authDonePayload}, fragment); err != nil {
		return fmt.Errorf("write auth done: %w", err)
	}
	signatureCodec := msgr.Codec(codec)
	if mode == ConModeSecure {
		signatureCodec, err = msgr.NewSecureCodec(connectionSecret, true)
		if err != nil {
			return err
		}
	}

	txTranscript := tap.txBytes()
	rxTranscript := tap.rxBytes()
	clientSigPayload, err := readControl(signatureCodec, tap, connectorTestMsgLimits)
	if err != nil {
		return fmt.Errorf("read client signature: %w", err)
	}
	clientSig, ok := clientSigPayload.(msgr.AuthSignature)
	if !ok {
		return fmt.Errorf("auth signature type %T", clientSigPayload)
	}
	expectedClientSig := TranscriptSignature(sessionKey, txTranscript)
	if clientSig.Signature != expectedClientSig {
		return fmt.Errorf("client signature mismatch")
	}

	tap.disableCapture()
	serverSig := TranscriptSignature(sessionKey, rxTranscript)
	if wrongServerSignature {
		serverSig[0] ^= 0xff
	}
	if err := writeControl(signatureCodec, tap, connectorTestMsgLimits, msgr.AuthSignature{Signature: serverSig}); err != nil {
		return fmt.Errorf("write server signature: %w", err)
	}

	if mode == ConModeSecure {
		if err := writeWireFrame(signatureCodec, tap, connectorTestMsgLimits, msgr.Frame{Tag: msgr.TagAck, Segments: []msgr.Segment{{Alignment: msgr.DefaultAlignment, Data: []byte("secure")}}}); err != nil {
			return fmt.Errorf("write secure post-auth frame: %w", err)
		}
		if _, err := signatureCodec.Read(tap, connectorTestMsgLimits); err != nil {
			return fmt.Errorf("read secure post-auth frame: %w", err)
		}
		return nil
	}

	if err := writeWireFrame(codec, tap, connectorTestMsgLimits, msgr.Frame{Tag: msgr.TagAck, Segments: []msgr.Segment{{Alignment: msgr.DefaultAlignment, Data: []byte("crc")}}}); err != nil {
		return fmt.Errorf("write crc post-auth frame: %w", err)
	}
	return nil
}

func scriptedAuthDonePayload(t *testing.T, credential Credential, now time.Time, mode uint32) ([]byte, CryptoKey, []byte, error) {
	t.Helper()
	principalSecret := credential.Secret()
	authSessionKey := mustSecretKey(t, "abcdefghijklmnop")
	monSessionKey := mustSecretKey(t, "QRSTUVWXabcdefgh")
	authTicket := ServiceTicket{
		ServiceID:  uint32(protocol.EntityAuth),
		SessionKey: authSessionKey,
		Ticket:     TicketBlob{SecretID: 7, Blob: []byte{1, 2, 3}},
	}
	monTicket := ServiceTicket{
		ServiceID:  uint32(protocol.EntityMonitor),
		SessionKey: monSessionKey,
		Ticket:     TicketBlob{SecretID: 8, Blob: []byte{4, 5}},
	}
	mainReply := encodeServiceTicketReply(t, principalSecret, []ServiceTicket{authTicket}, 60*time.Second, defaultTestLimits())
	extraReply := encodeServiceTicketReply(t, authSessionKey, []ServiceTicket{monTicket}, 60*time.Second, defaultTestLimits())

	payload := wire.NewEncoder(defaultTestLimits().MaxAuthBytes)
	payload.Uint16(cephxGetAuthSessionKey)
	payload.Int32(0)
	payload.Raw(mainReply)
	var connectionSecret []byte
	if mode == ConModeSecure {
		connectionSecret = bytesRepeat(0x5a, ConnectionSecretSizeSecure)
		connectionPlain := wire.NewEncoder(defaultTestLimits().MaxAuthBytes)
		connectionPlain.Bytes(connectionSecret)
		connectionPlainBytes, err := connectionPlain.BytesResult()
		if err != nil {
			return nil, CryptoKey{}, nil, err
		}
		connectionBlob, err := encodeEncryptEnvelope(authSessionKey, connectionPlainBytes, defaultTestLimits())
		if err != nil {
			return nil, CryptoKey{}, nil, err
		}
		payload.Bytes(connectionBlob)
		payload.Bytes(extraReply)
	}
	payloadBytes, err := payload.BytesResult()
	if err != nil {
		return nil, CryptoKey{}, nil, err
	}
	_ = now
	return payloadBytes, authSessionKey, connectionSecret, nil
}

func testConnectorIdentity(t *testing.T) (Credential, protocol.EntityAddr) {
	t.Helper()
	credential, err := ParseKey("client.test", testEncodedKey, 64)
	if err != nil {
		t.Fatal(err)
	}
	target, err := protocol.IPv4EntityAddr(protocol.AddressV2, 1, netip.MustParseAddrPort("198.51.100.1:3300"))
	if err != nil {
		t.Fatal(err)
	}
	return credential, target
}

func targetAddrString(t *testing.T, target protocol.EntityAddr) string {
	t.Helper()
	endpoint, ok := target.AddrPort()
	if !ok {
		t.Fatal("target has no addrport")
	}
	return endpoint.String()
}

func oneShotDial(conn net.Conn) func(context.Context, string, string) (net.Conn, error) {
	used := false
	return func(context.Context, string, string) (net.Conn, error) {
		if used {
			return nil, errors.New("dial already used")
		}
		used = true
		return conn, nil
	}
}

func writeControlWithFragment(codec msgr.Codec, writer net.Conn, limits msgr.Limits, payload any, fragment bool) error {
	frame, err := msgr.EncodeControl(payload, limits)
	if err != nil {
		return err
	}
	wireFrame, err := codec.Encode(frame, limits)
	if err != nil {
		return err
	}
	return writeFragmented(writer, wireFrame, fragment)
}

func writeWireFrame(codec msgr.Codec, writer net.Conn, limits msgr.Limits, frame msgr.Frame) error {
	wireFrame, err := codec.Encode(frame, limits)
	if err != nil {
		return err
	}
	return writeAll(writer, wireFrame)
}

func writeFragmented(conn net.Conn, data []byte, fragment bool) error {
	if !fragment {
		return writeAll(conn, data)
	}
	for _, value := range data {
		if _, err := conn.Write([]byte{value}); err != nil {
			return err
		}
	}
	return nil
}

func bytesRepeat(value byte, count int) []byte {
	out := make([]byte, count)
	for index := range out {
		out[index] = value
	}
	return out
}
