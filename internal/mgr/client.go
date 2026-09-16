package mgr

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/otuschhoff/go-librados/internal/cephx"
	wire "github.com/otuschhoff/go-librados/internal/encoding"
	"github.com/otuschhoff/go-librados/internal/maps"
	"github.com/otuschhoff/go-librados/internal/msgr"
	"github.com/otuschhoff/go-librados/internal/protocol"
)

var (
	ErrClosed                     = errors.New("manager client closed")
	ErrNoActiveManager            = errors.New("no active manager")
	ErrUnsupportedManagerFeatures = errors.New("unsupported manager features")
)

type MapSource interface {
	MgrMap() *maps.MgrMap
}

type SessionFactory func(context.Context, ActiveTarget) (session, error)

type Config struct {
	Maps             MapSource
	FSID             maps.FSID
	AuthoritySource  func() *cephx.Connector
	ServiceConnector cephx.ServiceConnectorConfig
	Session          msgr.SessionConfig
	ClientAddresses  protocol.EntityAddrVec
	MessageLimits    uint32
	RetryDelay       time.Duration
	MaxAttempts      int
	SessionFactory   SessionFactory
}

type Client struct {
	config  Config
	factory SessionFactory
	done    chan struct{}

	mu        sync.Mutex
	closed    bool
	closeDone chan struct{}
	session   sessionEntry
	nextTID   uint64
}

type ActiveTarget struct {
	Epoch    uint32
	GID      uint64
	Name     string
	Address  protocol.EntityAddr
	Features uint64
}

type session interface {
	Submit(context.Context, msgr.Message) (msgr.Message, error)
	Stop()
}

type sessionEntry struct {
	target  ActiveTarget
	session session
}

type submitResult struct {
	message msgr.Message
	err     error
}

func New(config Config) (*Client, error) {
	if config.Maps == nil || config.MessageLimits == 0 || config.RetryDelay <= 0 || config.MaxAttempts <= 0 {
		return nil, fmt.Errorf("%w: invalid manager client configuration", wire.ErrMalformed)
	}
	factory := config.SessionFactory
	if factory == nil {
		if len(config.ClientAddresses) == 0 {
			return nil, fmt.Errorf("%w: missing client addresses", wire.ErrMalformed)
		}
		factory = productionSessionFactory(config)
	}
	return &Client{config: config, factory: factory, done: make(chan struct{}), nextTID: 1}, nil
}

func (client *Client) Command(ctx context.Context, command []string, input []byte) (CommandReply, error) {
	if len(command) == 0 {
		return CommandReply{}, wire.ErrMalformed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	tid, err := client.nextTransactionID()
	if err != nil {
		return CommandReply{}, err
	}
	request, err := EncodeCommand(client.config.FSID, command, input, client.config.MessageLimits)
	if err != nil {
		return CommandReply{}, err
	}
	request.Header.TransactionID = tid

	var lastErr error
	for attempt := 0; attempt < client.config.MaxAttempts; attempt++ {
		target, err := client.currentTarget()
		if err != nil {
			if !errors.Is(err, ErrNoActiveManager) {
				return CommandReply{}, err
			}
			lastErr = err
			if err := client.waitRetry(ctx); err != nil {
				return CommandReply{}, err
			}
			continue
		}
		active, err := client.getSession(ctx, target)
		if err != nil {
			if errors.Is(err, ErrClosed) || ctx.Err() != nil {
				return CommandReply{}, preserveContextErr(ctx, err)
			}
			lastErr = err
			if attempt+1 < client.config.MaxAttempts {
				if err := client.waitRetry(ctx); err != nil {
					return CommandReply{}, err
				}
				continue
			}
			break
		}
		reply, retry, err := client.submitPendingAware(ctx, active, request)
		if retry {
			lastErr = ErrNoActiveManager
			continue
		}
		if err != nil {
			if errors.Is(err, ErrClosed) || ctx.Err() != nil {
				return CommandReply{}, preserveContextErr(ctx, err)
			}
			client.invalidate(active)
			lastErr = err
			if attempt+1 < client.config.MaxAttempts {
				if err := client.waitRetry(ctx); err != nil {
					return CommandReply{}, err
				}
				continue
			}
			break
		}
		if reply.Header.TransactionID != tid {
			client.invalidate(active)
			return CommandReply{}, fmt.Errorf("%w: manager command reply tid %d expected %d", wire.ErrMalformed, reply.Header.TransactionID, tid)
		}
		decoded, err := DecodeCommandReply(reply, client.config.MessageLimits)
		if err != nil {
			client.invalidate(active)
			return CommandReply{}, err
		}
		if decoded.Result < 0 {
			return decoded, protocol.WireErrno(decoded.Result)
		}
		return decoded, nil
	}
	if lastErr == nil {
		lastErr = ErrNoActiveManager
	}
	return CommandReply{}, lastErr
}

func (client *Client) Close() {
	client.mu.Lock()
	if client.closed {
		closeDone := client.closeDone
		client.mu.Unlock()
		if closeDone != nil {
			<-closeDone
		}
		return
	}
	client.closed = true
	client.closeDone = make(chan struct{})
	closeDone := client.closeDone
	active := client.session
	client.session = sessionEntry{}
	close(client.done)
	client.mu.Unlock()
	if active.session != nil {
		active.session.Stop()
	}
	close(closeDone)
}

func (client *Client) submitPendingAware(ctx context.Context, active sessionEntry, message msgr.Message) (msgr.Message, bool, error) {
	submitCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	result := make(chan submitResult, 1)
	go func() {
		reply, err := active.session.Submit(submitCtx, message)
		result <- submitResult{message: reply, err: err}
	}()
	ticker := time.NewTicker(client.config.RetryDelay)
	defer ticker.Stop()
	for {
		select {
		case reply := <-result:
			return reply.message, false, reply.err
		case <-ctx.Done():
			return msgr.Message{}, false, ctx.Err()
		case <-client.done:
			return msgr.Message{}, false, ErrClosed
		case <-ticker.C:
			current, err := client.currentTarget()
			if err == nil && sameTarget(current, active.target) {
				continue
			}
			cancel()
			client.invalidate(active)
			return msgr.Message{}, true, nil
		}
	}
}

func (client *Client) currentTarget() (ActiveTarget, error) {
	mgrMap := client.config.Maps.MgrMap()
	if mgrMap == nil || !mgrMap.Available() || mgrMap.ActiveGID() == 0 || mgrMap.ActiveName() == "" {
		return ActiveTarget{}, ErrNoActiveManager
	}
	features := protocol.GlobalFeatures(mgrMap.ActiveFeatures())
	if !features.Has(protocol.FeatureServerOctopusMask) {
		return ActiveTarget{}, fmt.Errorf("%w: active manager features %#x", ErrUnsupportedManagerFeatures, mgrMap.ActiveFeatures())
	}
	address, ok := selectEntityAddress(mgrMap.ActiveAddresses())
	if !ok {
		return ActiveTarget{}, ErrNoActiveManager
	}
	return ActiveTarget{Epoch: mgrMap.Epoch(), GID: mgrMap.ActiveGID(), Name: mgrMap.ActiveName(), Address: address, Features: mgrMap.ActiveFeatures()}, nil
}

func (client *Client) getSession(ctx context.Context, target ActiveTarget) (sessionEntry, error) {
	client.mu.Lock()
	if client.closed {
		client.mu.Unlock()
		return sessionEntry{}, ErrClosed
	}
	if client.session.session != nil && sameTarget(client.session.target, target) {
		active := client.session
		client.mu.Unlock()
		return active, nil
	}
	stale := client.session
	client.session = sessionEntry{}
	client.mu.Unlock()
	if stale.session != nil {
		stale.session.Stop()
	}

	created, err := client.factory(ctx, target)
	if err != nil {
		return sessionEntry{}, err
	}
	current, err := client.currentTarget()
	if err != nil {
		created.Stop()
		return sessionEntry{}, err
	}
	if !sameTarget(current, target) {
		created.Stop()
		return sessionEntry{}, ErrNoActiveManager
	}

	client.mu.Lock()
	if client.closed {
		client.mu.Unlock()
		created.Stop()
		return sessionEntry{}, ErrClosed
	}
	if client.session.session != nil {
		if sameTarget(client.session.target, target) {
			active := client.session
			client.mu.Unlock()
			created.Stop()
			return active, nil
		}
		replaced := client.session
		client.session = sessionEntry{target: target, session: created}
		client.mu.Unlock()
		if replaced.session != nil {
			replaced.session.Stop()
		}
		return sessionEntry{target: target, session: created}, nil
	}
	client.session = sessionEntry{target: target, session: created}
	client.mu.Unlock()
	return sessionEntry{target: target, session: created}, nil
}

func (client *Client) invalidate(active sessionEntry) {
	if active.session == nil {
		return
	}
	client.mu.Lock()
	if client.session.session == active.session && sameTarget(client.session.target, active.target) {
		client.session = sessionEntry{}
		client.mu.Unlock()
		active.session.Stop()
		return
	}
	client.mu.Unlock()
}

func (client *Client) nextTransactionID() (uint64, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closed {
		return 0, ErrClosed
	}
	if client.nextTID == 0 {
		return 0, msgr.ErrTransitionLimit
	}
	tid := client.nextTID
	client.nextTID++
	return tid, nil
}

func (client *Client) waitRetry(ctx context.Context) error {
	timer := time.NewTimer(client.config.RetryDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-client.done:
		return ErrClosed
	case <-timer.C:
		return nil
	}
}

func productionSessionFactory(config Config) SessionFactory {
	return func(ctx context.Context, target ActiveTarget) (session, error) {
		endpoint, ok := target.Address.AddrPort()
		if !ok || endpoint.Port() == 0 {
			return nil, ErrNoActiveManager
		}
		serviceConfig := config.ServiceConnector
		if config.AuthoritySource != nil {
			serviceConfig.Authority = nil
			serviceConfig.AuthoritySource = config.AuthoritySource
		}
		serviceConfig.ServiceType = protocol.EntityManager
		serviceConfig.Address = endpoint.String()
		serviceConfig.TargetAddress = target.Address
		connector, err := cephx.NewServiceConnector(serviceConfig)
		if err != nil {
			return nil, err
		}
		sessionConfig := config.Session
		sessionConfig.ClientIdent.Addresses = cloneAddresses(config.ClientAddresses)
		sessionConfig.ClientIdent.TargetAddress = cloneAddress(target.Address)
		sessionConfig.ClientIdent.SupportedFeatures = uint64(protocol.FeatureMonitorClient | protocol.FeatureMessageAddress2 | protocol.FeatureServerOctopusMask)
		sessionConfig.ClientIdent.RequiredFeatures = uint64(protocol.FeatureMessageAddress2 | protocol.FeatureServerOctopusMask)
		sessionConfig.ReconnectPolicy = msgr.ReplayPending
		return msgr.NewSession(nil, connector, sessionConfig)
	}
}

func preserveContextErr(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

func sameTarget(left, right ActiveTarget) bool {
	return left.GID == right.GID && left.Name == right.Name && sameAddress(left.Address, right.Address)
}

func sameAddress(left, right protocol.EntityAddr) bool {
	return left.Type == right.Type && left.Nonce == right.Nonce && left.Family == right.Family && bytes.Equal(left.SocketData, right.SocketData)
}

func selectEntityAddress(addresses protocol.EntityAddrVec) (protocol.EntityAddr, bool) {
	for _, address := range addresses {
		if address.Type != protocol.AddressV2 {
			continue
		}
		endpoint, ok := address.AddrPort()
		if ok && endpoint.Port() != 0 {
			return cloneAddress(address), true
		}
	}
	return protocol.EntityAddr{}, false
}

func cloneAddresses(addresses protocol.EntityAddrVec) protocol.EntityAddrVec {
	cloned := make(protocol.EntityAddrVec, len(addresses))
	for index, address := range addresses {
		cloned[index] = cloneAddress(address)
	}
	return cloned
}

func cloneAddress(address protocol.EntityAddr) protocol.EntityAddr {
	cloned := address
	cloned.SocketData = append([]byte(nil), address.SocketData...)
	return cloned
}
