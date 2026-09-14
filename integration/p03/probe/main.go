package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/netip"
	"os"
	"time"

	"github.com/otuschhoff/go-librados/internal/cephx"
	"github.com/otuschhoff/go-librados/internal/msgr"
	"github.com/otuschhoff/go-librados/internal/protocol"
)

type report struct {
	AuthenticatedMode  string   `json:"authenticated_mode"`
	GlobalID           uint64   `json:"global_id"`
	Reconnected        bool     `json:"reconnected"`
	TicketRenewed      bool     `json:"ticket_renewed"`
	InitialTicketHash  string   `json:"initial_ticket_sha256"`
	RenewedTicketHash  string   `json:"renewed_ticket_sha256"`
	ExpiryRejected     bool     `json:"expiry_rejected"`
	ExpiredReconnect   bool     `json:"expired_reconnect"`
	RenewedGlobalID    uint64   `json:"renewed_global_id"`
	PostExpiryGlobalID uint64   `json:"post_expiry_global_id"`
	ServerGlobalID     int64    `json:"server_global_id"`
	ServerAddresses    []string `json:"server_addresses"`
	ServerFeatures     uint64   `json:"server_features"`
	ServerCookie       uint64   `json:"server_cookie"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "p03 probe:", err)
		os.Exit(1)
	}
}

func run() error {
	monitorFlag := flag.String("monitor", "", "monitor v2 endpoint")
	clientFlag := flag.String("client-address", "", "client v2 address advertised to the monitor")
	entityFlag := flag.String("entity", "client.p03", "Ceph client entity")
	keyringFlag := flag.String("keyring", "", "path to the client keyring")
	timeoutFlag := flag.Duration("timeout", 10*time.Second, "total probe timeout")
	lifecycleFlag := flag.Bool("exercise-lifecycle", false, "exercise reconnect, renewal, and expiry")
	flag.Parse()
	if *monitorFlag == "" || *clientFlag == "" || *keyringFlag == "" || *timeoutFlag <= 0 {
		return errors.New("monitor, client-address, keyring, and a positive timeout are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeoutFlag)
	defer cancel()

	monitorEndpoint, err := netip.ParseAddrPort(*monitorFlag)
	if err != nil {
		return fmt.Errorf("parse monitor endpoint: %w", err)
	}
	clientEndpoint, err := netip.ParseAddrPort(*clientFlag)
	if err != nil {
		return fmt.Errorf("parse client endpoint: %w", err)
	}
	monitorAddress, err := entityAddress(monitorEndpoint)
	if err != nil {
		return fmt.Errorf("monitor address: %w", err)
	}
	clientAddress, err := entityAddress(clientEndpoint)
	if err != nil {
		return fmt.Errorf("client address: %w", err)
	}
	keyring, err := os.ReadFile(*keyringFlag)
	if err != nil {
		return fmt.Errorf("read keyring: %w", err)
	}
	credential, err := cephx.ParseKeyring(keyring, *entityFlag, cephx.DefaultMaxKeyring)
	if err != nil {
		return fmt.Errorf("parse keyring: %w", err)
	}

	messageLimits := msgr.Limits{
		MaxSegmentBytes: 8 << 20,
		MaxFrameBytes:   32 << 20,
		MaxAddresses:    64,
		MaxAuthBytes:    1 << 20,
	}
	connector, err := cephx.NewConnector(cephx.ConnectorConfig{
		Address:          monitorEndpoint.String(),
		TargetAddress:    monitorAddress,
		Credential:       credential,
		HandshakeTimeout: *timeoutFlag,
		MessageLimits:    messageLimits,
	})
	if err != nil {
		return err
	}
	session, result, metadata, err := startAndIdentify(ctx, connector, monitorAddress, clientAddress, monitorEndpoint, messageLimits)
	if err != nil {
		return err
	}
	defer session.Stop()
	if *lifecycleFlag {
		firstTicket, ok := metadata.Tickets[uint32(protocol.EntityMonitor)]
		if !ok || firstTicket.ExpiresAt.IsZero() || firstTicket.RenewAfter.IsZero() {
			return errors.New("authenticated session has no monitor ticket lifecycle")
		}
		secondSnapshot, secondMetadata, err := waitForRenewed(ctx, session, connector, firstTicket)
		if err != nil {
			return fmt.Errorf("live renewal: %w", err)
		}
		if secondMetadata.GlobalID != metadata.GlobalID {
			return fmt.Errorf("renewal changed global ID from %d to %d", metadata.GlobalID, secondMetadata.GlobalID)
		}
		secondTicket, ok := secondMetadata.Tickets[uint32(protocol.EntityMonitor)]
		if !ok || sameTicketIdentity(firstTicket, secondTicket) {
			return errors.New("reconnect did not renew the monitor ticket")
		}
		authTicket, ok := secondMetadata.Tickets[uint32(protocol.EntityAuth)]
		if !ok || authTicket.ExpiresAt.IsZero() {
			return errors.New("reconnect did not return an auth ticket expiry")
		}
		result, err = reportFromSnapshot(secondSnapshot, secondMetadata, monitorEndpoint)
		if err != nil {
			return err
		}
		result.Reconnected = true
		result.TicketRenewed = true
		result.RenewedGlobalID = secondMetadata.GlobalID
		result.InitialTicketHash = hex.EncodeToString(firstTicket.Fingerprint[:])
		result.RenewedTicketHash = hex.EncodeToString(secondTicket.Fingerprint[:])
		expiresAt := secondTicket.ExpiresAt
		if authTicket.ExpiresAt.After(expiresAt) {
			expiresAt = authTicket.ExpiresAt
		}
		session.Stop()
		wait := time.Until(expiresAt) + 100*time.Millisecond
		if err := waitFor(ctx, wait); err != nil {
			return fmt.Errorf("ticket expiry wait: %w", err)
		}
		if _, err := connector.BuildServiceAuthorizer(uint32(protocol.EntityMonitor)); !errors.Is(err, cephx.ErrExpiredTicket) {
			return fmt.Errorf("expired monitor ticket: %v", err)
		}
		thirdSession, _, thirdMetadata, err := startAndIdentify(ctx, connector, monitorAddress, clientAddress, monitorEndpoint, messageLimits)
		if err != nil {
			return fmt.Errorf("post-expiry reconnect: %w", err)
		}
		thirdSession.Stop()
		if thirdMetadata.GlobalID == 0 {
			return errors.New("post-expiry reconnect returned a zero global ID")
		}
		if thirdMetadata.GlobalID == secondMetadata.GlobalID {
			return fmt.Errorf("post-expiry reconnect reused global ID %d", thirdMetadata.GlobalID)
		}
		result.ExpiryRejected = true
		result.ExpiredReconnect = true
		result.RenewedGlobalID = secondMetadata.GlobalID
		result.PostExpiryGlobalID = thirdMetadata.GlobalID
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}

func sameTicketIdentity(first, second cephx.TicketMetadata) bool {
	return first.SecretID == second.SecretID && first.Fingerprint == second.Fingerprint
}

func startAndIdentify(ctx context.Context, connector *cephx.Connector, monitorAddress, clientAddress protocol.EntityAddr, monitorEndpoint netip.AddrPort, messageLimits msgr.Limits) (*msgr.Session, report, cephx.AuthMetadata, error) {
	features := uint64(protocol.FeatureMessageAddress2)
	session, err := msgr.NewSession(nil, connector, msgr.SessionConfig{
		Limits:                  messageLimits,
		MaxQueuedMessages:       16,
		MaxRetainedBytes:        1 << 20,
		MaxInFlightTransactions: 16,
		MaxReconnectAttempts:    1,
		MaxHandshakeTransitions: 16,
		EventBuffer:             16,
		ClientIdent: msgr.ClientIdent{
			Addresses:         protocol.EntityAddrVec{clientAddress},
			TargetAddress:     monitorAddress,
			SupportedFeatures: features,
			RequiredFeatures:  features,
		},
	})
	if err != nil {
		return nil, report{}, cephx.AuthMetadata{}, fmt.Errorf("create session: %w", err)
	}
	snapshot, err := waitForReady(ctx, session)
	if err != nil {
		session.Stop()
		return nil, report{}, cephx.AuthMetadata{}, err
	}
	metadata := connector.AuthMetadata()
	result, err := reportFromSnapshot(snapshot, metadata, monitorEndpoint)
	if err != nil {
		session.Stop()
		return nil, report{}, cephx.AuthMetadata{}, err
	}
	return session, result, metadata, nil
}

func reportFromSnapshot(snapshot msgr.SessionSnapshot, metadata cephx.AuthMetadata, monitorEndpoint netip.AddrPort) (report, error) {
	if metadata.Mode != cephx.ConModeSecure {
		return report{}, fmt.Errorf("unexpected authenticated mode %d", metadata.Mode)
	}
	if snapshot.AuthenticatedGlobalID != metadata.GlobalID {
		return report{}, fmt.Errorf("session global ID %d does not match connector %d", snapshot.AuthenticatedGlobalID, metadata.GlobalID)
	}
	addresses, containsTarget := reportAddresses(snapshot.ServerAddresses, monitorEndpoint)
	if !containsTarget {
		return report{}, errors.New("session accepted a server identity without the target monitor address")
	}
	return report{
		AuthenticatedMode: "secure",
		GlobalID:          metadata.GlobalID,
		ServerGlobalID:    snapshot.ServerGlobalID,
		ServerAddresses:   addresses,
		ServerFeatures:    snapshot.ServerFeatures,
		ServerCookie:      snapshot.ServerCookie,
	}, nil
}

func waitForRenewed(ctx context.Context, session *msgr.Session, connector *cephx.Connector, original cephx.TicketMetadata) (msgr.SessionSnapshot, cephx.AuthMetadata, error) {
	var lastFault error
	for {
		select {
		case event, ok := <-session.Events():
			if !ok {
				return msgr.SessionSnapshot{}, cephx.AuthMetadata{}, msgr.ErrSessionClosed
			}
			if event.Kind == msgr.EventTransportFault {
				lastFault = event.Err
			}
			if event.Kind != msgr.EventStateChanged || event.State != msgr.StateReady {
				continue
			}
			snapshot, err := session.Snapshot(ctx)
			if err != nil {
				return msgr.SessionSnapshot{}, cephx.AuthMetadata{}, err
			}
			metadata := connector.AuthMetadata()
			if renewed, ok := metadata.Tickets[uint32(protocol.EntityMonitor)]; ok && !sameTicketIdentity(original, renewed) {
				return snapshot, metadata, nil
			}
		case <-ctx.Done():
			snapshot, _ := session.Snapshot(context.Background())
			return msgr.SessionSnapshot{}, cephx.AuthMetadata{}, fmt.Errorf("%w (state: %d, server flags: %#x, reconnect attempts: %d, last transport fault: %v)", ctx.Err(), snapshot.State, snapshot.ServerFlags, snapshot.ReconnectAttempts, lastFault)
		}
	}
}

func waitFor(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func waitForReady(ctx context.Context, session *msgr.Session) (msgr.SessionSnapshot, error) {
	for {
		select {
		case event, ok := <-session.Events():
			if !ok {
				return msgr.SessionSnapshot{}, msgr.ErrSessionClosed
			}
			if event.Kind == msgr.EventStateChanged && event.State == msgr.StateReady {
				return session.Snapshot(ctx)
			}
			if event.Kind == msgr.EventTransportFault && event.Err != nil {
				return msgr.SessionSnapshot{}, fmt.Errorf("session handshake: %w", event.Err)
			}
		case <-ctx.Done():
			return msgr.SessionSnapshot{}, fmt.Errorf("session handshake: %w", ctx.Err())
		}
	}
}

func entityAddress(endpoint netip.AddrPort) (protocol.EntityAddr, error) {
	if endpoint.Addr().Unmap().Is4() {
		return protocol.IPv4EntityAddr(protocol.AddressV2, 0, endpoint)
	}
	return protocol.IPv6EntityAddr(protocol.AddressV2, 0, endpoint, 0, 0)
}

func reportAddresses(addresses protocol.EntityAddrVec, target netip.AddrPort) ([]string, bool) {
	reported := make([]string, 0, len(addresses))
	containsTarget := false
	for _, address := range addresses {
		endpoint, ok := address.AddrPort()
		if !ok {
			continue
		}
		reported = append(reported, endpoint.String())
		containsTarget = containsTarget || endpoint == target
	}
	return reported, containsTarget
}
