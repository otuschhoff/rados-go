package rados

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/otuschhoff/go-librados/internal/cephx"
	wire "github.com/otuschhoff/go-librados/internal/encoding"
	"github.com/otuschhoff/go-librados/internal/maps"
	"github.com/otuschhoff/go-librados/internal/mon"
	"github.com/otuschhoff/go-librados/internal/msgr"
	"github.com/otuschhoff/go-librados/internal/objecter"
	"github.com/otuschhoff/go-librados/internal/osd"
	"github.com/otuschhoff/go-librados/internal/protocol"
)

const (
	defaultDialTimeout      = 10 * time.Second
	defaultHandshakeTimeout = 15 * time.Second
	defaultOperationTimeout = 30 * time.Second
)

type SecurityMode uint8

const (
	SecurityModeSecure SecurityMode = iota
	SecurityModeCRC
)

type Config struct {
	Monitors         []string
	Entity           string
	ClusterFSID      string
	Key              []byte
	SecurityMode     SecurityMode
	DialTimeout      time.Duration
	HandshakeTimeout time.Duration
	OperationTimeout time.Duration
}

type Client struct {
	config     Config
	credential cephx.Credential
	expected   *maps.FSID

	connectMu sync.Mutex
	mu        sync.Mutex
	closed    bool
	connected bool
	monitor   *mon.Client
	objects   *objecter.Client
	authority atomic.Pointer[cephx.Connector]
}

func New(config Config) (*Client, error) {
	if len(config.Monitors) == 0 || config.Entity == "" || len(config.Key) == 0 || config.DialTimeout < 0 || config.HandshakeTimeout < 0 || config.OperationTimeout < 0 || config.SecurityMode > SecurityModeCRC {
		return nil, &OpError{Op: "new", Err: ErrInvalidArgument}
	}
	config.Monitors = append([]string(nil), config.Monitors...)
	config.Key = append([]byte(nil), config.Key...)
	if config.DialTimeout == 0 {
		config.DialTimeout = defaultDialTimeout
	}
	if config.HandshakeTimeout == 0 {
		config.HandshakeTimeout = defaultHandshakeTimeout
	}
	if config.OperationTimeout == 0 {
		config.OperationTimeout = defaultOperationTimeout
	}
	credential, err := cephx.ParseKey(config.Entity, string(config.Key), cephx.DefaultMaxKeyBytes)
	if err != nil {
		return nil, &OpError{Op: "new", Err: errors.Join(ErrInvalidArgument, err)}
	}
	var expected *maps.FSID
	if config.ClusterFSID != "" {
		fsid, err := parsePublicFSID(config.ClusterFSID)
		if err != nil {
			return nil, &OpError{Op: "new", Err: errors.Join(ErrInvalidArgument, err)}
		}
		expected = &fsid
	}
	return &Client{config: config, credential: credential, expected: expected}, nil
}

func (client *Client) Connect(ctx context.Context) error {
	client.connectMu.Lock()
	defer client.connectMu.Unlock()
	client.mu.Lock()
	if client.closed {
		client.mu.Unlock()
		return &OpError{Op: "connect", Err: ErrClosed}
	}
	if client.connected {
		client.mu.Unlock()
		return nil
	}
	client.mu.Unlock()
	operationCtx, cancel := client.operationContext(ctx)
	defer cancel()
	endpoints, err := mon.ResolveSeeds(operationCtx, client.config.Monitors, nil, mon.SeedLimits{MaxSeeds: 64, MaxAddresses: 64})
	if err != nil {
		return client.wrapError("connect", "monitors", err)
	}
	clientAddress, err := protocol.IPv4EntityAddr(protocol.AddressV2, 0, netip.MustParseAddrPort("0.0.0.0:0"))
	if err != nil {
		return client.wrapError("connect", "client", err)
	}
	messageLimits := msgr.Limits{MaxSegmentBytes: 32 << 20, MaxFrameBytes: 64 << 20, MaxAddresses: 64, MaxAuthBytes: 1 << 20}
	sessionConfig := msgr.SessionConfig{
		Limits: messageLimits, MaxQueuedMessages: 128, MaxRetainedBytes: 320 << 20, MaxInFlightTransactions: 64,
		MaxReconnectAttempts: 2, MaxHandshakeTransitions: 32, EventBuffer: 16,
		ClientIdent: msgr.ClientIdent{Addresses: protocol.EntityAddrVec{clientAddress}, SupportedFeatures: uint64(protocol.FeatureMonitorClient), RequiredFeatures: uint64(protocol.FeatureMessageAddress2)},
	}
	connectorConfig := cephx.ConnectorConfig{
		Credential: client.credential, DialTimeout: client.config.DialTimeout, HandshakeTimeout: client.config.HandshakeTimeout,
		MessageLimits: messageLimits, AllowCRC: client.config.SecurityMode == SecurityModeCRC,
	}
	factory := mon.NewAuthenticatedSessionFactoryWithObserver(connectorConfig, sessionConfig, func(connector *cephx.Connector) { client.authority.Store(connector) })
	monitorClient, err := mon.NewClient(mon.ClientConfig{
		Endpoints: endpoints, ExpectedFSID: client.expected, Hostname: "go-librados",
		MapLimits:     maps.Limits{MaxBytes: 64 << 20, MaxMonitors: 64, MaxAddresses: 64, MaxLocations: 64, MaxPools: 4096, MaxOSDs: 65536, MaxPGMappings: 1 << 20, MaxCollectionEntries: 1 << 20},
		MessageLimits: mon.MessageLimits{MaxBytes: 64 << 20, MaxMaps: 1024}, CommandItems: 64, SubscribePeriod: 5 * time.Second, RetryDelay: 100 * time.Millisecond,
	}, factory)
	if err != nil {
		return client.wrapError("connect", "monitors", err)
	}
	client.mu.Lock()
	if client.closed {
		client.mu.Unlock()
		monitorClient.Close()
		return &OpError{Op: "connect", Err: ErrClosed}
	}
	client.monitor = monitorClient
	client.mu.Unlock()
	if err := monitorClient.Connect(operationCtx); err != nil {
		monitorClient.Close()
		client.mu.Lock()
		if client.monitor == monitorClient {
			client.monitor = nil
		}
		client.mu.Unlock()
		return client.wrapError("connect", "monitors", err)
	}
	objectClient, err := objecter.New(objecter.Config{
		Maps: monitorClient, AuthoritySource: func() *cephx.Connector { return client.authority.Load() }, ClientAddresses: protocol.EntityAddrVec{clientAddress},
		ServiceConnector: cephx.ServiceConnectorConfig{DialTimeout: client.config.DialTimeout, HandshakeTimeout: client.config.HandshakeTimeout, MessageLimits: messageLimits, AllowCRC: client.config.SecurityMode == SecurityModeCRC},
		Session:          sessionConfig, MessageLimits: osd.Limits{MaxBytes: 32 << 20, MaxOperations: 16}, MaxAttempts: 4, RefreshWait: 2 * time.Second,
	})
	if err != nil {
		monitorClient.Close()
		return client.wrapError("connect", "OSDs", err)
	}
	client.mu.Lock()
	if client.closed {
		client.mu.Unlock()
		objectClient.Close()
		monitorClient.Close()
		return &OpError{Op: "connect", Err: ErrClosed}
	}
	client.objects = objectClient
	client.connected = true
	client.mu.Unlock()
	return nil
}

func (client *Client) OpenPool(ctx context.Context, name string) (Pool, error) {
	monitorClient, _, err := client.active()
	if err != nil {
		return Pool{}, err
	}
	operationCtx, cancel := client.operationContext(ctx)
	defer cancel()
	select {
	case <-operationCtx.Done():
		return Pool{}, client.wrapError("open pool", "pool", operationCtx.Err())
	default:
	}
	pool, ok := monitorClient.PoolByName(name)
	if !ok {
		return Pool{}, &OpError{Op: "open pool", Target: "pool", Code: -2}
	}
	return Pool{client: client, id: pool.ID(), name: pool.Name(), snapshot: osd.NoSnap}, nil
}

func (client *Client) OpenPoolByID(ctx context.Context, id int64) (Pool, error) {
	monitorClient, _, err := client.active()
	if err != nil {
		return Pool{}, err
	}
	operationCtx, cancel := client.operationContext(ctx)
	defer cancel()
	select {
	case <-operationCtx.Done():
		return Pool{}, client.wrapError("open pool", "pool", operationCtx.Err())
	default:
	}
	osdMap := monitorClient.OSDMap()
	pool, ok := osdMap.PoolByID(id)
	if !ok {
		return Pool{}, &OpError{Op: "open pool", Target: "pool", Code: -2}
	}
	return Pool{client: client, id: pool.ID(), name: pool.Name(), snapshot: osd.NoSnap}, nil
}

func (client *Client) Flush(ctx context.Context) error {
	_, objects, err := client.active()
	if err != nil {
		return err
	}
	operationCtx, cancel := client.operationContext(ctx)
	defer cancel()
	if err := objects.Flush(operationCtx); err != nil {
		return client.wrapError("flush", "client", err)
	}
	return nil
}

func (client *Client) Shutdown(ctx context.Context) error {
	_, objects, err := client.active()
	if err != nil {
		if errors.Is(err, ErrClosed) {
			return client.Close()
		}
		return err
	}
	objects.BeginShutdown()
	operationCtx, cancel := client.operationContext(ctx)
	defer cancel()
	if err := objects.Flush(operationCtx); err != nil {
		_ = client.Close()
		return client.wrapError("shutdown", "client", err)
	}
	return client.Close()
}

func (client *Client) Close() error {
	client.mu.Lock()
	if client.closed {
		client.mu.Unlock()
		return nil
	}
	client.closed = true
	objects, monitorClient := client.objects, client.monitor
	client.objects, client.monitor = nil, nil
	client.connected = false
	client.mu.Unlock()
	if objects != nil {
		objects.Close()
	}
	if monitorClient != nil {
		monitorClient.Close()
	}
	return nil
}

func (client *Client) FSID() string {
	client.mu.Lock()
	monitorClient := client.monitor
	client.mu.Unlock()
	if monitorClient == nil || monitorClient.OSDMap() == nil {
		return ""
	}
	value := monitorClient.OSDMap().FSID()
	hexValue := hex.EncodeToString(value[:])
	return fmt.Sprintf("%s-%s-%s-%s-%s", hexValue[:8], hexValue[8:12], hexValue[12:16], hexValue[16:20], hexValue[20:])
}

func (client *Client) InstanceID() uint64 {
	if authority := client.authority.Load(); authority != nil {
		return authority.AuthMetadata().GlobalID
	}
	return 0
}

func (client *Client) active() (*mon.Client, *objecter.Client, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closed {
		return nil, nil, &OpError{Err: ErrClosed}
	}
	if !client.connected || client.monitor == nil || client.objects == nil {
		return nil, nil, &OpError{Err: ErrClosed}
	}
	return client.monitor, client.objects, nil
}

func (client *Client) operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, client.config.OperationTimeout)
}

func (client *Client) wrapError(op, target string, err error) error {
	if err == nil {
		return nil
	}
	var wireErr protocol.WireErrno
	if errors.As(err, &wireErr) {
		return &OpError{Op: op, Target: target, Code: int32(wireErr), Err: err}
	}
	classified := err
	if errors.Is(err, msgr.ErrOutcomeUnknown) {
		classified = errors.Join(ErrOutcomeUnknown, classified)
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		classified = errors.Join(ErrTimeout, classified)
	case errors.Is(err, context.Canceled):
		classified = errors.Join(ErrCanceled, classified)
	case errors.Is(err, mon.ErrClosed), errors.Is(err, objecter.ErrClosed), errors.Is(err, msgr.ErrSessionClosed):
		classified = errors.Join(ErrClosed, err)
	case errors.Is(err, msgr.ErrUnsupportedFeature), errors.Is(err, wire.ErrUnsupportedVersion):
		classified = errors.Join(ErrUnsupported, err)
	case errors.Is(err, wire.ErrLimitExceeded), errors.Is(err, wire.ErrMalformed):
		classified = errors.Join(ErrInvalidArgument, err)
	}
	return &OpError{Op: op, Target: target, Err: classified}
}

func parsePublicFSID(value string) (maps.FSID, error) {
	decoded, err := hex.DecodeString(strings.ReplaceAll(strings.TrimSpace(value), "-", ""))
	if err != nil || len(decoded) != 16 {
		return maps.FSID{}, errors.New("invalid cluster FSID")
	}
	var fsid maps.FSID
	copy(fsid[:], decoded)
	return fsid, nil
}
