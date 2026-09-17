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
	"strings"
	"time"

	"github.com/otuschhoff/rados-go/internal/cephx"
	"github.com/otuschhoff/rados-go/internal/maps"
	"github.com/otuschhoff/rados-go/internal/mon"
	"github.com/otuschhoff/rados-go/internal/msgr"
	"github.com/otuschhoff/rados-go/internal/protocol"
)

type report struct {
	FSID                    string   `json:"fsid"`
	InitialEpoch            uint32   `json:"initial_epoch"`
	MutationEpoch           uint32   `json:"mutation_epoch"`
	FailoverEpoch           uint32   `json:"failover_epoch"`
	Pools                   []string `json:"pools"`
	IncrementalObserved     bool     `json:"incremental_observed"`
	FullIncrementEquivalent bool     `json:"full_incremental_equivalent"`
	MonitorLossRecovered    bool     `json:"monitor_loss_recovered"`
	PostFailoverCommand     bool     `json:"post_failover_command"`
	ForeignFSIDRejected     bool     `json:"foreign_fsid_rejected"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "p04 probe:", err)
		os.Exit(1)
	}
}

func run() error {
	monitorFlag := flag.String("monitors", "", "comma-separated monitor endpoints")
	foreignFlag := flag.String("foreign-monitor", "", "foreign monitor endpoint")
	clientFlag := flag.String("client-address", "", "client address")
	keyringFlag := flag.String("keyring", "", "client keyring")
	fsidFlag := flag.String("fsid", "", "expected FSID")
	controlFlag := flag.String("control", "", "synchronization directory")
	timeoutFlag := flag.Duration("timeout", 45*time.Second, "total timeout")
	flag.Parse()
	if *monitorFlag == "" || *foreignFlag == "" || *clientFlag == "" || *keyringFlag == "" || *fsidFlag == "" || *controlFlag == "" {
		return errors.New("all flags are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeoutFlag)
	defer cancel()
	expected, err := parseFSID(*fsidFlag)
	if err != nil {
		return err
	}
	keyring, err := os.ReadFile(*keyringFlag)
	if err != nil {
		return err
	}
	credential, err := cephx.ParseKeyring(keyring, "client.p04", cephx.DefaultMaxKeyring)
	if err != nil {
		return err
	}
	clientEndpoint, err := netip.ParseAddrPort(*clientFlag)
	if err != nil {
		return err
	}
	clientAddress, err := protocol.IPv4EntityAddr(protocol.AddressV2, 0, clientEndpoint)
	if err != nil {
		return err
	}
	endpoints, err := parseEndpoints(strings.Split(*monitorFlag, ","))
	if err != nil {
		return err
	}
	client, err := newClient(expected, credential, clientAddress, endpoints)
	if err != nil {
		return err
	}
	defer client.Close()
	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	initial := client.OSDMap()
	if initial == nil || initial.FSID() != expected {
		return errors.New("invalid initial map")
	}
	pools, err := commandPools(ctx, client)
	if err != nil {
		return err
	}
	if !contains(pools, "p04-initial") {
		return fmt.Errorf("initial pools = %v", pools)
	}
	if err := os.WriteFile(*controlFlag+"/ready", nil, 0o600); err != nil {
		return err
	}
	mutation, err := waitForPool(ctx, client, "p04-mutated", initial.Epoch())
	if err != nil {
		return err
	}
	fullClient, err := newClient(expected, credential, clientAddress, endpoints)
	if err != nil {
		return err
	}
	if err := fullClient.Connect(ctx); err != nil {
		fullClient.Close()
		return err
	}
	full := fullClient.OSDMap()
	for full.Epoch() < mutation.Epoch() {
		select {
		case <-ctx.Done():
			fullClient.Close()
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
			full = fullClient.OSDMap()
		}
	}
	equivalent := mutation.AppliedIncremental() && !full.AppliedIncremental() && mutation.Equivalent(full)
	fullClient.Close()
	if !equivalent {
		return fmt.Errorf("incremental epoch %d differs from full epoch %d", mutation.Epoch(), full.Epoch())
	}
	if err := os.WriteFile(*controlFlag+"/mutated", nil, 0o600); err != nil {
		return err
	}
	failover, err := waitForPool(ctx, client, "p04-failover", mutation.Epoch())
	if err != nil {
		return err
	}
	if _, err := client.ReadOnlyCommand(ctx, []string{`{"prefix":"status","format":"json"}`}, nil); err != nil {
		return fmt.Errorf("post-failover command: %w", err)
	}
	foreignEndpoints, err := parseEndpoints([]string{*foreignFlag})
	if err != nil {
		return err
	}
	foreignRejected, err := rejectForeign(ctx, expected, credential, clientAddress, foreignEndpoints)
	if err != nil {
		return err
	}
	pools, err = commandPools(ctx, client)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(report{*fsidFlag, initial.Epoch(), mutation.Epoch(), failover.Epoch(), pools, mutation.Epoch() > initial.Epoch(), equivalent, failover.Epoch() > mutation.Epoch(), true, foreignRejected})
}

func newClient(expected maps.FSID, credential cephx.Credential, address protocol.EntityAddr, endpoints []mon.Endpoint) (*mon.Client, error) {
	limits := msgr.Limits{MaxSegmentBytes: 32 << 20, MaxFrameBytes: 64 << 20, MaxAddresses: 64, MaxAuthBytes: 1 << 20}
	factory := mon.NewAuthenticatedSessionFactory(cephx.ConnectorConfig{Credential: credential, HandshakeTimeout: 5 * time.Second, MessageLimits: limits}, msgr.SessionConfig{Limits: limits, MaxQueuedMessages: 64, MaxRetainedBytes: 64 << 20, MaxInFlightTransactions: 32, MaxReconnectAttempts: 1, MaxHandshakeTransitions: 32, EventBuffer: 0, ClientIdent: msgr.ClientIdent{Addresses: protocol.EntityAddrVec{address}, SupportedFeatures: uint64(protocol.FeatureMonitorClient), RequiredFeatures: uint64(protocol.FeatureMessageAddress2)}})
	return mon.NewClient(mon.ClientConfig{Endpoints: endpoints, ExpectedFSID: &expected, Hostname: "p04-probe", MapLimits: maps.Limits{MaxBytes: 64 << 20, MaxMonitors: 64, MaxAddresses: 64, MaxLocations: 64, MaxPools: 4096, MaxOSDs: 65536, MaxPGMappings: 1 << 20, MaxCollectionEntries: 1 << 20}, MessageLimits: mon.MessageLimits{MaxBytes: 64 << 20, MaxMaps: 1024}, CommandItems: 64, SubscribePeriod: 2 * time.Second, RetryDelay: 100 * time.Millisecond}, factory)
}

func waitForPool(ctx context.Context, client *mon.Client, name string, after uint32) (*maps.OSDMap, error) {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if current := client.OSDMap(); current != nil && current.Epoch() > after {
			if _, ok := current.PoolByName(name); ok {
				return current, nil
			}
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("wait for %s: %w", name, ctx.Err())
		case <-ticker.C:
		}
	}
}

func commandPools(ctx context.Context, client *mon.Client) ([]string, error) {
	reply, err := client.ReadOnlyCommand(ctx, []string{`{"prefix":"osd pool ls","format":"json"}`}, nil)
	if err != nil {
		return nil, err
	}
	var pools []string
	if err := json.Unmarshal(reply.Data, &pools); err != nil {
		return nil, err
	}
	for _, name := range pools {
		if _, ok := client.PoolByName(name); !ok {
			return nil, fmt.Errorf("pool %q absent from map", name)
		}
	}
	return pools, nil
}

func rejectForeign(ctx context.Context, expected maps.FSID, credential cephx.Credential, address protocol.EntityAddr, endpoints []mon.Endpoint) (bool, error) {
	client, err := newClient(expected, credential, address, endpoints)
	if err != nil {
		return false, err
	}
	defer client.Close()
	result := make(chan error, 1)
	go func() { result <- client.Connect(ctx) }()
	select {
	case err := <-client.Errors():
		if !errors.Is(err, mon.ErrForeignCluster) {
			return false, err
		}
		return true, nil
	case err := <-result:
		return false, fmt.Errorf("foreign connect completed: %w", err)
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func parseEndpoints(values []string) ([]mon.Endpoint, error) {
	result := make([]mon.Endpoint, 0, len(values))
	for _, value := range values {
		endpoint, err := netip.ParseAddrPort(value)
		if err != nil {
			return nil, err
		}
		address, err := protocol.IPv4EntityAddr(protocol.AddressV2, 0, endpoint)
		if err != nil {
			return nil, err
		}
		result = append(result, mon.Endpoint{Address: endpoint, EntityAddress: address})
	}
	return result, nil
}

func parseFSID(value string) (maps.FSID, error) {
	var result maps.FSID
	decoded, err := hex.DecodeString(strings.ReplaceAll(value, "-", ""))
	if err != nil || len(decoded) != len(result) {
		return result, fmt.Errorf("invalid FSID %q", value)
	}
	copy(result[:], decoded)
	return result, nil
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
