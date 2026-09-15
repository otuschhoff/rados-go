// Package objecter routes and executes bounded Ceph object requests.
package objecter

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/otuschhoff/go-librados/internal/cephx"
	wire "github.com/otuschhoff/go-librados/internal/encoding"
	"github.com/otuschhoff/go-librados/internal/maps"
	"github.com/otuschhoff/go-librados/internal/msgr"
	"github.com/otuschhoff/go-librados/internal/osd"
	"github.com/otuschhoff/go-librados/internal/protocol"
)

var (
	ErrClosed        = errors.New("objecter closed")
	ErrNoPrimary     = errors.New("object has no acting primary")
	ErrRecovery      = errors.New("read recovery exhausted")
	ErrMalformedStat = errors.New("malformed stat result")
	ErrStaleMap      = errors.New("OSD supplied a newer map")
)

const readReplyFrontBytes = uint64(144)

type MapSource interface {
	OSDMap() *maps.OSDMap
	RefreshOSDMap(context.Context, uint32) error
}

type Route struct {
	Epoch     uint32
	PG        maps.PG
	RawHash   uint32
	Primary   int32
	Addresses protocol.EntityAddrVec
}

type Router interface {
	Route(Target) (Route, error)
}

type Target struct {
	PoolID    int64
	Object    string
	Locator   string
	Namespace string
	Snapshot  uint64
}

type Result struct {
	Data             []byte
	Size             uint64
	ModificationTime time.Time
	Version          uint64
}

type session interface {
	Submit(context.Context, msgr.Message) (msgr.Message, error)
	Stop()
}

type backoffWaiter interface {
	Wait(context.Context, maps.PG, osd.HObject) error
}

type targetSubmitter interface {
	SubmitTarget(context.Context, maps.PG, osd.HObject, msgr.Message) (msgr.Message, error)
}

type SessionFactory func(int32, protocol.EntityAddrVec) (session, error)

type Config struct {
	Maps             MapSource
	Router           Router
	Authority        *cephx.Connector
	AuthoritySource  func() *cephx.Connector
	ServiceConnector cephx.ServiceConnectorConfig
	Session          msgr.SessionConfig
	ClientAddresses  protocol.EntityAddrVec
	MessageLimits    osd.Limits
	MaxAttempts      int
	RefreshWait      time.Duration
	SessionFactory   SessionFactory
}

type Client struct {
	config   Config
	mu       sync.Mutex
	sessions map[int32]sessionEntry
	closed   bool
}

type sessionEntry struct {
	address string
	session session
}

func New(config Config) (*Client, error) {
	if config.Maps == nil || config.MessageLimits.MaxBytes == 0 || config.MessageLimits.MaxOperations == 0 || config.MaxAttempts <= 0 || config.RefreshWait <= 0 {
		return nil, wire.ErrLimitExceeded
	}
	if config.Router == nil {
		config.Router = mapRouter{source: config.Maps}
	}
	if config.SessionFactory == nil {
		if config.AuthoritySource == nil {
			config.AuthoritySource = func() *cephx.Connector { return config.Authority }
		}
		if config.AuthoritySource() == nil || len(config.ClientAddresses) == 0 {
			return nil, wire.ErrMalformed
		}
		config.SessionFactory = productionSessionFactory(config)
	}
	config.ClientAddresses = cloneAddresses(config.ClientAddresses)
	return &Client{config: config, sessions: make(map[int32]sessionEntry)}, nil
}

func (client *Client) Read(ctx context.Context, target Target, offset, length uint64) (Result, error) {
	minimumReplyBytes := readReplyFrontBytes + uint64(len(target.Object))
	if length > math.MaxUint64-offset || minimumReplyBytes > uint64(client.config.MessageLimits.MaxBytes) || length > uint64(client.config.MessageLimits.MaxBytes)-minimumReplyBytes {
		return Result{}, wire.ErrLimitExceeded
	}
	return client.execute(ctx, target, osd.Operation{Code: osd.OpRead, Offset: offset, Length: length})
}

func (client *Client) Stat(ctx context.Context, target Target) (Result, error) {
	result, err := client.execute(ctx, target, osd.Operation{Code: osd.OpStat})
	if err != nil {
		return Result{}, err
	}
	if len(result.Data) != 16 {
		return Result{}, ErrMalformedStat
	}
	result.Size = binary.LittleEndian.Uint64(result.Data[:8])
	seconds := binary.LittleEndian.Uint32(result.Data[8:12])
	nanoseconds := binary.LittleEndian.Uint32(result.Data[12:])
	if nanoseconds >= 1_000_000_000 {
		return Result{}, ErrMalformedStat
	}
	result.ModificationTime = time.Unix(int64(seconds), int64(nanoseconds)).UTC()
	result.Data = nil
	return result, nil
}

func (client *Client) execute(ctx context.Context, target Target, operation osd.Operation) (Result, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	currentTarget := target
	requestFlags := uint32(0)
	var lastErr error
	for attempt := 0; attempt < client.config.MaxAttempts; attempt++ {
		route, err := client.config.Router.Route(currentTarget)
		if err != nil {
			return Result{}, err
		}
		if route.Primary < 0 || len(route.Addresses) == 0 {
			return Result{}, ErrNoPrimary
		}
		if attempt > 0 {
			requestFlags |= osd.FlagRetry
		}
		active, err := client.getSession(route.Primary, route.Addresses)
		if err != nil {
			return Result{}, err
		}
		request, err := osd.EncodeRequest(osd.Request{
			MapEpoch: route.Epoch, PG: route.PG, ObjectHash: route.RawHash,
			PoolID: currentTarget.PoolID, Object: currentTarget.Object, Locator: currentTarget.Locator,
			Namespace: currentTarget.Namespace, Snapshot: currentTarget.Snapshot, Retry: int32(attempt), Flags: requestFlags,
			Features: uint64(protocol.FeatureOSDClient), Operations: []osd.Operation{operation},
		}, client.config.MessageLimits)
		if err != nil {
			return Result{}, err
		}
		object := osd.HObject{Key: currentTarget.Locator, Object: currentTarget.Object, Snapshot: currentTarget.Snapshot, Hash: route.RawHash, Namespace: currentTarget.Namespace, Pool: currentTarget.PoolID}
		var message msgr.Message
		if submitter, ok := active.(targetSubmitter); ok {
			message, err = submitter.SubmitTarget(ctx, route.PG, object, request)
		} else {
			if waiter, ok := active.(backoffWaiter); ok {
				if err := waiter.Wait(ctx, route.PG, object); err != nil {
					if ctx.Err() != nil {
						return Result{}, ctx.Err()
					}
					lastErr = err
					client.invalidate(route.Primary, active)
					client.refresh(ctx, route.Epoch)
					continue
				}
			}
			message, err = active.Submit(ctx, request)
		}
		if err != nil {
			if ctx.Err() != nil {
				return Result{}, ctx.Err()
			}
			lastErr = err
			client.invalidate(route.Primary, active)
			client.refresh(ctx, route.Epoch)
			continue
		}
		reply, err := osd.DecodeReply(message, client.config.MessageLimits)
		if err != nil {
			client.invalidate(route.Primary, active)
			return Result{}, fmt.Errorf("%w: %v", osd.ErrMalformedReply, err)
		}
		if reply.Object != currentTarget.Object || reply.PG != route.PG {
			client.invalidate(route.Primary, active)
			return Result{}, osd.ErrMalformedReply
		}
		if reply.Retry >= 0 && reply.Retry != int32(attempt) {
			client.invalidate(route.Primary, active)
			return Result{}, osd.ErrMalformedReply
		}
		if reply.Redirect != nil {
			currentTarget.PoolID = reply.Redirect.Pool
			if reply.Redirect.Object != "" {
				currentTarget.Object = reply.Redirect.Object
			}
			currentTarget.Locator = reply.Redirect.Locator
			currentTarget.Namespace = reply.Redirect.Namespace
			requestFlags |= osd.FlagRedirected | osd.FlagIgnoreCache | osd.FlagIgnoreOverlay
			lastErr = errors.New("OSD redirected read")
			continue
		}
		if reply.Result == -11 {
			lastErr = protocol.WireErrno(reply.Result)
			client.refresh(ctx, route.Epoch)
			continue
		}
		if reply.Result != 0 {
			return Result{}, protocol.WireErrno(reply.Result)
		}
		if len(reply.Operations) != 1 {
			client.invalidate(route.Primary, active)
			return Result{}, osd.ErrMalformedReply
		}
		if reply.Operations[0].Operation != operation.Code {
			client.invalidate(route.Primary, active)
			return Result{}, osd.ErrMalformedReply
		}
		if reply.Operations[0].Code == -11 {
			lastErr = protocol.WireErrno(reply.Operations[0].Code)
			client.refresh(ctx, route.Epoch)
			continue
		}
		if reply.Operations[0].Code != 0 {
			return Result{}, protocol.WireErrno(reply.Operations[0].Code)
		}
		if operation.Code == osd.OpRead && uint64(len(reply.Operations[0].Data)) > operation.Length {
			client.invalidate(route.Primary, active)
			return Result{}, osd.ErrMalformedReply
		}
		return Result{Data: append([]byte(nil), reply.Operations[0].Data...), Version: reply.Version}, nil
	}
	return Result{}, fmt.Errorf("%w: %w", ErrRecovery, lastErr)
}

type mapRouter struct{ source MapSource }

func (router mapRouter) Route(target Target) (Route, error) {
	osdMap := router.source.OSDMap()
	if osdMap == nil {
		return Route{}, ErrNoPrimary
	}
	placement, err := osdMap.PlaceObject(target.PoolID, target.Object, target.Locator, target.Namespace)
	if err != nil {
		return Route{}, err
	}
	addresses, ok := osdMap.OSDClientAddresses(placement.ActingPrimary)
	if !ok {
		return Route{}, ErrNoPrimary
	}
	return Route{Epoch: osdMap.Epoch(), PG: placement.PG, RawHash: placement.RawHash, Primary: placement.ActingPrimary, Addresses: addresses}, nil
}

func (client *Client) refresh(ctx context.Context, epoch uint32) {
	refreshCtx, cancel := context.WithTimeout(ctx, client.config.RefreshWait)
	defer cancel()
	_ = client.config.Maps.RefreshOSDMap(refreshCtx, epoch)
}

func (client *Client) getSession(osdID int32, addresses protocol.EntityAddrVec) (session, error) {
	address, ok := selectAddress(addresses)
	if !ok {
		return nil, ErrNoPrimary
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closed {
		return nil, ErrClosed
	}
	if existing, ok := client.sessions[osdID]; ok {
		if existing.address == address {
			return existing.session, nil
		}
		existing.session.Stop()
		delete(client.sessions, osdID)
	}
	created, err := client.config.SessionFactory(osdID, addresses)
	if err != nil {
		return nil, err
	}
	client.sessions[osdID] = sessionEntry{address: address, session: created}
	return created, nil
}

func (client *Client) invalidate(osdID int32, failed session) {
	client.mu.Lock()
	defer client.mu.Unlock()
	if existing, ok := client.sessions[osdID]; ok && existing.session == failed {
		delete(client.sessions, osdID)
		failed.Stop()
	}
}

func (client *Client) Close() {
	client.mu.Lock()
	if client.closed {
		client.mu.Unlock()
		return
	}
	client.closed = true
	sessions := client.sessions
	client.sessions = nil
	client.mu.Unlock()
	for _, entry := range sessions {
		entry.session.Stop()
	}
}

func productionSessionFactory(config Config) SessionFactory {
	return func(_ int32, addresses protocol.EntityAddrVec) (session, error) {
		selected, ok := selectEntityAddress(addresses)
		if !ok {
			return nil, ErrNoPrimary
		}
		endpoint, _ := selected.AddrPort()
		serviceConfig := config.ServiceConnector
		if config.AuthoritySource() == nil {
			return nil, cephx.ErrMissingTicket
		}
		serviceConfig.Authority = nil
		serviceConfig.AuthoritySource = config.AuthoritySource
		serviceConfig.Address = endpoint.String()
		serviceConfig.TargetAddress = selected
		connector, err := cephx.NewServiceConnector(serviceConfig)
		if err != nil {
			return nil, err
		}
		sessionConfig := config.Session
		sessionConfig.ClientIdent.Addresses = cloneAddresses(config.ClientAddresses)
		sessionConfig.ClientIdent.TargetAddress = selected
		sessionConfig.ClientIdent.SupportedFeatures = uint64(protocol.FeatureOSDClient)
		sessionConfig.ClientIdent.RequiredFeatures = uint64(protocol.FeatureOSDReplyMux | protocol.FeaturePGID64 | protocol.FeatureNewOSDOpReplyEncoding | protocol.FeatureMessageAddress2)
		raw, err := msgr.NewSession(nil, connector, sessionConfig)
		if err != nil {
			return nil, err
		}
		return newOSDSession(raw, config.MessageLimits, config.RefreshWait), nil
	}
}

func selectAddress(addresses protocol.EntityAddrVec) (string, bool) {
	address, ok := selectEntityAddress(addresses)
	if !ok {
		return "", false
	}
	endpoint, ok := address.AddrPort()
	return endpoint.String(), ok
}

func selectEntityAddress(addresses protocol.EntityAddrVec) (protocol.EntityAddr, bool) {
	for _, address := range addresses {
		if address.Type != protocol.AddressV2 {
			continue
		}
		if endpoint, ok := address.AddrPort(); ok && endpoint.Port() != 0 {
			return address, true
		}
	}
	return protocol.EntityAddr{}, false
}

func cloneAddresses(addresses protocol.EntityAddrVec) protocol.EntityAddrVec {
	cloned := make(protocol.EntityAddrVec, len(addresses))
	for index, address := range addresses {
		cloned[index] = address
		cloned[index].SocketData = append([]byte(nil), address.SocketData...)
	}
	return cloned
}
