package cephx

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"testing"
	"time"

	wire "github.com/otuschhoff/go-librados/internal/encoding"
	"github.com/otuschhoff/go-librados/internal/msgr"
	"github.com/otuschhoff/go-librados/internal/protocol"
)

func TestServiceConnectorSecureHandshake(t *testing.T) {
	for _, challenge := range []bool{false, true} {
		t.Run(fmt.Sprintf("challenge=%t", challenge), func(t *testing.T) {
			testServiceConnectorSecureHandshake(t, challenge)
		})
	}
}

func testServiceConnectorSecureHandshake(t *testing.T, challenge bool) {
	now := time.Now().UTC()
	credential, _ := testConnectorIdentity(t)
	osdAddress, err := protocol.IPv4EntityAddr(protocol.AddressV2, 2, netip.MustParseAddrPort("198.51.100.2:6800"))
	if err != nil {
		t.Fatal(err)
	}
	ticket := ServiceTicket{
		ServiceID: uint32(protocol.EntityOSD), SessionKey: mustSecretKey(t, "osd-session-key!"),
		Ticket: TicketBlob{SecretID: 9, Blob: []byte("osd-ticket")}, ExpiresAt: now.Add(time.Hour), RenewAfter: now.Add(30 * time.Minute),
	}
	authority, err := NewConnector(ConnectorConfig{
		Address: "198.51.100.1:3300", Credential: credential, MessageLimits: connectorTestMsgLimits,
		Limits: defaultTestLimits(), Now: func() time.Time { return now }, Rand: bytes.NewReader(bytes.Repeat([]byte{1}, 64)),
	})
	if err != nil {
		t.Fatal(err)
	}
	authority.state = connectorState{globalID: 77, mode: ConModeSecure, tickets: map[uint32]ServiceTicket{uint32(protocol.EntityOSD): ticket}}
	clientConn, serverConn := net.Pipe()
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- runScriptedOSDHandshake(serverConn, osdAddress, ticket, 77, scriptedOSDOptions{challenge: challenge})
	}()
	connector, err := NewServiceConnector(ServiceConnectorConfig{
		Authority: authority, Address: "198.51.100.2:6800", TargetAddress: osdAddress, Dial: oneShotDial(clientConn),
		MessageLimits: connectorTestMsgLimits, HandshakeTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	transport, err := connector.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if authenticated, ok := transport.(msgr.AuthenticatedTransport); !ok || authenticated.AuthenticatedGlobalID() != 77 {
		t.Fatalf("authenticated transport=%T", transport)
	}
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestServiceConnectorRejectsInvalidOSDAuthorization(t *testing.T) {
	tests := []struct {
		name    string
		options scriptedOSDOptions
		want    error
	}{
		{name: "peer masquerading", options: scriptedOSDOptions{helloEntity: protocol.EntityMonitor}, want: ErrPeerEntity},
		{name: "malformed challenge", options: scriptedOSDOptions{challenge: true, malformedChallenge: true}, want: ErrAuthHandshake},
		{name: "wrong global ID", options: scriptedOSDOptions{globalIDDelta: 1}, want: ErrAuthHandshake},
		{name: "wrong nonce", options: scriptedOSDOptions{badNonce: true}, want: ErrAuthHandshake},
		{name: "malformed connection secret", options: scriptedOSDOptions{malformedSecret: true}, want: ErrAuthHandshake},
		{name: "CRC downgrade", options: scriptedOSDOptions{connectionMode: ConModeCRC}, want: ErrAuthDowngrade},
		{name: "wrong transcript signature", options: scriptedOSDOptions{badSignature: true}, want: ErrAuthSignature},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			now := time.Now().UTC()
			credential, _ := testConnectorIdentity(t)
			osdAddress, err := protocol.IPv4EntityAddr(protocol.AddressV2, 2, netip.MustParseAddrPort("198.51.100.2:6800"))
			if err != nil {
				t.Fatal(err)
			}
			ticket := ServiceTicket{ServiceID: uint32(protocol.EntityOSD), SessionKey: mustSecretKey(t, "osd-session-key!"), Ticket: TicketBlob{SecretID: 9, Blob: []byte("osd-ticket")}, ExpiresAt: now.Add(time.Hour), RenewAfter: now.Add(30 * time.Minute)}
			authority, err := NewConnector(ConnectorConfig{Address: "198.51.100.1:3300", Credential: credential, MessageLimits: connectorTestMsgLimits, Limits: defaultTestLimits(), Now: func() time.Time { return now }, Rand: bytes.NewReader(bytes.Repeat([]byte{1}, 64))})
			if err != nil {
				t.Fatal(err)
			}
			authority.state = connectorState{globalID: 77, mode: ConModeSecure, tickets: map[uint32]ServiceTicket{uint32(protocol.EntityOSD): ticket}}
			clientConn, serverConn := net.Pipe()
			serverErr := make(chan error, 1)
			go func() { serverErr <- runScriptedOSDHandshake(serverConn, osdAddress, ticket, 77, test.options) }()
			connector, err := NewServiceConnector(ServiceConnectorConfig{Authority: authority, Address: "198.51.100.2:6800", TargetAddress: osdAddress, Dial: oneShotDial(clientConn), MessageLimits: connectorTestMsgLimits, HandshakeTimeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			transport, err := connector.Connect(context.Background())
			if transport != nil || !errors.Is(err, test.want) {
				t.Fatalf("transport=%T error=%v want=%v", transport, err, test.want)
			}
			select {
			case <-serverErr:
			case <-time.After(time.Second):
				t.Fatal("server did not observe rejected connection closure")
			}
		})
	}
}

func TestServiceConnectorRejectsMissingAndExpiredOSDTickets(t *testing.T) {
	now := time.Now().UTC()
	credential, _ := testConnectorIdentity(t)
	for _, test := range []struct {
		name   string
		ticket *ServiceTicket
		want   error
	}{
		{name: "missing", want: ErrMissingTicket},
		{name: "expired", ticket: &ServiceTicket{ServiceID: uint32(protocol.EntityOSD), SessionKey: mustSecretKey(t, "osd-session-key!"), Ticket: TicketBlob{SecretID: 9, Blob: []byte("ticket")}, ExpiresAt: now}, want: ErrExpiredTicket},
	} {
		t.Run(test.name, func(t *testing.T) {
			authority, err := NewConnector(ConnectorConfig{Address: "198.51.100.1:3300", Credential: credential, MessageLimits: connectorTestMsgLimits, Limits: defaultTestLimits(), Now: func() time.Time { return now }})
			if err != nil {
				t.Fatal(err)
			}
			tickets := make(map[uint32]ServiceTicket)
			if test.ticket != nil {
				tickets[uint32(protocol.EntityOSD)] = *test.ticket
			}
			authority.state = connectorState{globalID: 77, tickets: tickets}
			connector, err := NewServiceConnector(ServiceConnectorConfig{Authority: authority, Address: "198.51.100.2:6800", MessageLimits: connectorTestMsgLimits, Dial: func(context.Context, string, string) (net.Conn, error) {
				t.Fatal("dialed without valid ticket")
				return nil, nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			transport, err := connector.Connect(context.Background())
			if transport != nil || !errors.Is(err, ErrAuthHandshake) || !errors.Is(err, test.want) {
				t.Fatalf("transport=%T error=%v want=%v", transport, err, test.want)
			}
		})
	}
}

func TestServiceConnectorResolvesAuthorityForEveryConnect(t *testing.T) {
	calls := 0
	connector, err := NewServiceConnector(ServiceConnectorConfig{
		AuthoritySource: func() *Connector { calls++; return nil },
		Address:         "198.51.100.2:6800", MessageLimits: connectorTestMsgLimits,
	})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if transport, err := connector.Connect(context.Background()); transport != nil || !errors.Is(err, ErrMissingTicket) {
			t.Fatalf("transport=%T error=%v", transport, err)
		}
	}
	if calls != 2 {
		t.Fatalf("authority source calls=%d", calls)
	}
}

type scriptedOSDOptions struct {
	challenge          bool
	malformedChallenge bool
	helloEntity        protocol.EntityType
	globalIDDelta      uint64
	badNonce           bool
	malformedSecret    bool
	connectionMode     uint32
	badSignature       bool
}

func runScriptedOSDHandshake(conn net.Conn, target protocol.EntityAddr, ticket ServiceTicket, globalID uint64, options scriptedOSDOptions) error {
	defer conn.Close()
	tap := newTranscriptConn(conn)
	crcCodec := msgr.CRCCodec{WithDataCRC: true}
	if _, err := msgr.ReadBanner(tap, 64); err != nil {
		return err
	}
	if err := writeAll(tap, msgr.ClientBanner().Encode()); err != nil {
		return err
	}
	if _, err := readControl(crcCodec, tap, connectorTestMsgLimits); err != nil {
		return err
	}
	entity := options.helloEntity
	if entity == 0 {
		entity = protocol.EntityOSD
	}
	if err := writeControl(crcCodec, tap, connectorTestMsgLimits, msgr.Hello{EntityType: entity, PeerAddress: target}); err != nil {
		return err
	}
	requestPayload, err := readControl(crcCodec, tap, connectorTestMsgLimits)
	if err != nil {
		return err
	}
	request, ok := requestPayload.(msgr.AuthRequest)
	if !ok || request.Method != AuthMethodCephX || len(request.AuthPayload) == 0 {
		return fmt.Errorf("OSD auth request=%T", requestPayload)
	}
	if options.challenge {
		plain := wire.NewEncoder(defaultTestLimits().MaxAuthBytes)
		if options.malformedChallenge {
			plain.Uint8(1)
		} else {
			plain.Uint8(1)
			plain.Uint64(19)
		}
		plainBytes, err := plain.BytesResult()
		if err != nil {
			return err
		}
		encrypted, err := encryptWithMagicUsage(ticket.SessionKey, plainBytes, keyUsageAuthorizeChallenge, defaultTestLimits())
		if err != nil {
			return err
		}
		if err := writeControl(crcCodec, tap, connectorTestMsgLimits, msgr.AuthReplyMore{AuthPayload: encrypted}); err != nil {
			return err
		}
		if _, err := readControl(crcCodec, tap, connectorTestMsgLimits); err != nil {
			return err
		}
	}
	nonce := uint64(0x0101010101010101)
	if options.badNonce {
		nonce--
	}
	secretSize := ConnectionSecretSizeSecure
	if options.malformedSecret {
		secretSize--
	}
	connectionSecret := bytes.Repeat([]byte{0x51}, secretSize)
	reply := wire.NewEncoder(defaultTestLimits().MaxAuthBytes)
	reply.Uint8(2)
	reply.Uint64(nonce + 1)
	reply.Bytes(connectionSecret)
	replyBytes, err := reply.BytesResult()
	if err != nil {
		return err
	}
	replyPayload, err := encodeEncryptEnvelopeUsage(ticket.SessionKey, replyBytes, keyUsageAuthorizeReply, defaultTestLimits())
	if err != nil {
		return err
	}
	mode := options.connectionMode
	if mode == 0 {
		mode = ConModeSecure
	}
	if err := writeControl(crcCodec, tap, connectorTestMsgLimits, msgr.AuthDone{GlobalID: globalID + options.globalIDDelta, ConnectionMode: mode, AuthPayload: replyPayload}); err != nil {
		return err
	}
	secureCodec, err := msgr.NewSecureCodec(connectionSecret, true)
	if err != nil {
		return err
	}
	txTranscript, rxTranscript := tap.txBytes(), tap.rxBytes()
	signaturePayload, err := readControl(secureCodec, tap, connectorTestMsgLimits)
	if err != nil {
		return err
	}
	signature, ok := signaturePayload.(msgr.AuthSignature)
	if !ok || !VerifyTranscriptSignature(ticket.SessionKey, txTranscript, signature.Signature) {
		return ErrAuthSignature
	}
	tap.disableCapture()
	serverSignature := TranscriptSignature(ticket.SessionKey, rxTranscript)
	if options.badSignature {
		serverSignature[0] ^= 1
	}
	if err := writeControl(secureCodec, tap, connectorTestMsgLimits, msgr.AuthSignature{Signature: serverSignature}); err != nil {
		return err
	}
	_, _ = secureCodec.Read(tap, connectorTestMsgLimits)
	return nil
}
