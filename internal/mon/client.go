package mon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/otuschhoff/go-librados/internal/cephx"
	wire "github.com/otuschhoff/go-librados/internal/encoding"
	"github.com/otuschhoff/go-librados/internal/maps"
	"github.com/otuschhoff/go-librados/internal/msgr"
	"github.com/otuschhoff/go-librados/internal/protocol"
)

var (
	ErrClosed          = errors.New("monitor client closed")
	ErrForeignCluster  = errors.New("monitor returned a foreign cluster fsid")
	ErrMapGap          = errors.New("osdmap epoch gap")
	ErrReadOnlyCommand = errors.New("monitor command is not in the read-only allowlist")
	errMonitorRemoved  = errors.New("active monitor removed from monmap")
)

type session interface {
	Send(context.Context, msgr.Message) error
	Submit(context.Context, msgr.Message) (msgr.Message, error)
	Incoming() <-chan msgr.Message
	Events() <-chan msgr.SessionEvent
	Terminal() <-chan error
	Done() <-chan struct{}
	Stop()
}

type SessionFactory func(context.Context, Endpoint) (session, error)

type ClientConfig struct {
	Endpoints       []Endpoint
	ExpectedFSID    *maps.FSID
	Hostname        string
	MapLimits       maps.Limits
	MessageLimits   MessageLimits
	CommandItems    uint32
	SubscribePeriod time.Duration
	RetryDelay      time.Duration
}

type Client struct {
	config  ClientConfig
	factory SessionFactory

	ctx              context.Context
	cancel           context.CancelFunc
	done             chan struct{}
	errors           chan error
	ready            chan struct{}
	readyFailure     chan struct{}
	startDone        chan struct{}
	once             sync.Once
	readyFailureOnce sync.Once

	mu             sync.Mutex
	session        session
	nextEndpoint   int
	started        bool
	runStarted     bool
	startErr       error
	readyErr       error
	closed         bool
	refreshPending bool
	seedEndpoints  []Endpoint
	activeEndpoint Endpoint
	hasActive      bool
	pinnedFSID     *maps.FSID
	monMap         atomic.Pointer[maps.MonMap]
	osdMap         atomic.Pointer[maps.OSDMap]
}

func NewClient(config ClientConfig, factory SessionFactory) (*Client, error) {
	if len(config.Endpoints) == 0 || factory == nil || config.SubscribePeriod <= 0 || config.RetryDelay < 0 || config.CommandItems == 0 {
		return nil, fmt.Errorf("%w: invalid monitor client configuration", wire.ErrMalformed)
	}
	if err := validateMapLimits(config.MapLimits); err != nil {
		return nil, err
	}
	if config.MessageLimits.MaxBytes == 0 || config.MessageLimits.MaxMaps == 0 {
		return nil, wire.ErrLimitExceeded
	}
	ctx, cancel := context.WithCancel(context.Background())
	config.Endpoints = append([]Endpoint(nil), config.Endpoints...)
	client := &Client{config: config, factory: factory, ctx: ctx, cancel: cancel, done: make(chan struct{}), errors: make(chan error, 16), ready: make(chan struct{}), readyFailure: make(chan struct{}), startDone: make(chan struct{}), seedEndpoints: append([]Endpoint(nil), config.Endpoints...)}
	if config.ExpectedFSID != nil {
		fsid := *config.ExpectedFSID
		client.pinnedFSID = &fsid
	}
	return client, nil
}

func NewAuthenticatedSessionFactory(connector cephx.ConnectorConfig, sessionConfig msgr.SessionConfig) SessionFactory {
	return NewAuthenticatedSessionFactoryWithObserver(connector, sessionConfig, nil)
}

func NewAuthenticatedSessionFactoryWithObserver(connector cephx.ConnectorConfig, sessionConfig msgr.SessionConfig, observe func(*cephx.Connector)) SessionFactory {
	return func(_ context.Context, endpoint Endpoint) (session, error) {
		connectorConfig := connector
		connectorConfig.Address = endpoint.Address.String()
		connectorConfig.TargetAddress = endpoint.EntityAddress
		authenticated, err := cephx.NewConnector(connectorConfig)
		if err != nil {
			return nil, err
		}
		config := sessionConfig
		config.ClientIdent.Addresses = cloneEntityAddresses(sessionConfig.ClientIdent.Addresses)
		config.ClientIdent.TargetAddress = endpoint.EntityAddress
		opened, err := msgr.NewSession(nil, authenticated, config)
		if err != nil {
			return nil, err
		}
		if observe == nil {
			return opened, nil
		}
		return &authoritySession{Session: opened, publish: func() { observe(authenticated) }}, nil
	}
}

type authoritySession struct {
	*msgr.Session
	publish     func()
	publishOnce sync.Once
}

func (session *authoritySession) publishAuthority() {
	session.publishOnce.Do(session.publish)
}

func publishAuthority(active session) {
	if publisher, ok := active.(interface{ publishAuthority() }); ok {
		publisher.publishAuthority()
	}
}

func (client *Client) Connect(ctx context.Context) error {
	client.mu.Lock()
	if client.closed {
		client.mu.Unlock()
		return ErrClosed
	}
	if !client.started {
		client.started = true
		go client.start()
	}
	client.mu.Unlock()
	select {
	case <-client.startDone:
		client.mu.Lock()
		err := client.startErr
		client.mu.Unlock()
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-client.done:
		return ErrClosed
	}
	select {
	case <-client.ready:
		return nil
	case <-client.readyFailure:
		client.mu.Lock()
		err := client.readyErr
		client.mu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-client.done:
		return ErrClosed
	}
}

func (client *Client) start() {
	var active session
	var endpoint Endpoint
	var err error
	for {
		active, endpoint, err = client.openNext(client.ctx)
		if err == nil || client.ctx.Err() != nil {
			break
		}
		client.reportNonfatal(err)
		timer := time.NewTimer(client.config.RetryDelay)
		select {
		case <-client.ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
	client.mu.Lock()
	if client.closed {
		err = ErrClosed
	}
	if err == nil {
		client.session = active
		client.activeEndpoint = endpoint
		client.hasActive = true
		client.runStarted = true
		go client.run(active)
	}
	client.startErr = err
	close(client.startDone)
	client.mu.Unlock()
	if err != nil && active != nil {
		active.Stop()
	}
}

func (client *Client) Close() {
	client.once.Do(func() {
		client.cancel()
		client.mu.Lock()
		client.closed = true
		starting := client.started
		client.mu.Unlock()
		if starting {
			<-client.startDone
		}
		client.mu.Lock()
		active := client.session
		runStarted := client.runStarted
		client.session = nil
		client.mu.Unlock()
		if active != nil {
			active.Stop()
		}
		if runStarted {
			<-client.done
		} else {
			close(client.done)
		}
	})
}

func (client *Client) Done() <-chan struct{} { return client.done }
func (client *Client) Errors() <-chan error  { return client.errors }
func (client *Client) MonMap() *maps.MonMap  { return client.monMap.Load() }
func (client *Client) OSDMap() *maps.OSDMap  { return client.osdMap.Load() }

func (client *Client) PoolByName(name string) (maps.Pool, bool) {
	osdMap := client.osdMap.Load()
	if osdMap == nil {
		return maps.Pool{}, false
	}
	return osdMap.PoolByName(name)
}

func (client *Client) RefreshOSDMap(ctx context.Context, after uint32) error {
	if err := client.requestFullMap(); err != nil {
		return err
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if current := client.osdMap.Load(); current != nil && current.Epoch() > after {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-client.done:
			return ErrClosed
		case <-ticker.C:
		}
	}
}

func (client *Client) ReadOnlyCommand(ctx context.Context, command []string, input []byte) (CommandReply, error) {
	if !isReadOnlyCommand(command) {
		return CommandReply{}, ErrReadOnlyCommand
	}
	fsid, ok := client.currentFSID()
	if !ok {
		return CommandReply{}, fmt.Errorf("%w: cluster identity unavailable", ErrClosed)
	}
	message, err := EncodeCommand(fsid, command, input, client.config.MessageLimits.MaxBytes)
	if err != nil {
		return CommandReply{}, err
	}
	client.mu.Lock()
	active := client.session
	client.mu.Unlock()
	if active == nil {
		return CommandReply{}, ErrClosed
	}
	reply, err := active.Submit(ctx, message)
	if err != nil {
		return CommandReply{}, err
	}
	decoded, err := DecodeCommandReply(reply, client.config.MessageLimits.MaxBytes, client.config.CommandItems)
	if err != nil {
		return CommandReply{}, err
	}
	if decoded.Result != 0 {
		return decoded, protocol.WireErrno(decoded.Result)
	}
	return decoded, nil
}

func (client *Client) run(active session) {
	defer close(client.done)
	timer := time.NewTimer(client.config.SubscribePeriod)
	if err := client.subscribe(active); err != nil {
		client.reportNonfatal(err)
	}
	defer timer.Stop()
	for {
		select {
		case <-client.ctx.Done():
			return
		case _, ok := <-active.Events():
			if !ok {
				if !client.failover(&active) {
					return
				}
				continue
			}
		case <-active.Done():
			if !client.failover(&active) {
				return
			}
		case err := <-active.Terminal():
			if errors.Is(err, msgr.ErrUnsupportedPayload) {
				active.Stop()
				client.report(err)
				return
			}
			if !client.waitRetry() {
				return
			}
			if !client.failover(&active) {
				return
			}
		case message, ok := <-active.Incoming():
			if !ok {
				if !client.failover(&active) {
					return
				}
				continue
			}
			interval, err := client.handleMessage(active, message)
			if err != nil {
				recoverable := errors.Is(err, ErrMapGap) || errors.Is(err, ErrForeignCluster) || errors.Is(err, errMonitorRemoved)
				if recoverable {
					client.reportNonfatal(err)
				} else {
					client.report(err)
				}
				if (errors.Is(err, ErrForeignCluster) || errors.Is(err, errMonitorRemoved)) && !client.failover(&active) {
					return
				}
				continue
			}
			if interval > 0 {
				timer.Reset(interval)
			}
		case <-timer.C:
			if err := client.subscribe(active); err != nil {
				client.reportNonfatal(err)
			}
			timer.Reset(client.config.SubscribePeriod)
		}
	}
}

func (client *Client) handleMessage(active session, message msgr.Message) (time.Duration, error) {
	switch message.Header.Type {
	case protocol.MessageMonSubscribeAck:
		ack, err := DecodeSubscribeAck(message, client.config.MessageLimits.MaxBytes)
		if err != nil {
			return 0, fmt.Errorf("decode subscribe acknowledgment: %w", err)
		}
		if err := client.pinFSID(ack.FSID); err != nil {
			return 0, err
		}
		publishAuthority(active)
		if ack.IntervalSeconds == 0 {
			return client.config.SubscribePeriod, nil
		}
		return time.Duration(ack.IntervalSeconds) * time.Second / 2, nil
	case protocol.MessageMonMap:
		monMap, err := DecodeMonMap(message, client.config.MapLimits)
		if err != nil {
			return 0, fmt.Errorf("decode monitor map: %w", err)
		}
		if err := client.pinFSID(monMap.FSID()); err != nil {
			return 0, err
		}
		publishAuthority(active)
		current := client.monMap.Load()
		if current == nil || monMap.Epoch() > current.Epoch() {
			activePresent := client.replaceMonMapEndpoints(monMap)
			client.monMap.Store(monMap)
			if client.osdMap.Load() != nil {
				client.onceReady()
			}
			if !activePresent {
				return 0, errMonitorRemoved
			}
		}
	case protocol.MessageOSDMap:
		batch, err := DecodeOSDMapBatch(message, client.config.MessageLimits)
		if err != nil {
			return 0, fmt.Errorf("decode OSD map batch: %w", err)
		}
		if err := client.pinFSID(batch.FSID); err != nil {
			return 0, err
		}
		publishAuthority(active)
		appliedFull, err := client.applyBatch(batch)
		if err != nil {
			if errors.Is(err, ErrMapGap) {
				if refreshErr := client.requestFullMap(); refreshErr != nil {
					return 0, errors.Join(err, refreshErr)
				}
			}
			return 0, err
		}
		if appliedFull && client.finishRefresh() {
			if err := client.subscribe(active); err != nil {
				return 0, err
			}
		}
	}
	return 0, nil
}

func (client *Client) waitRetry() bool {
	timer := time.NewTimer(client.config.RetryDelay)
	defer timer.Stop()
	select {
	case <-client.ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (client *Client) applyBatch(batch OSDMapBatch) (bool, error) {
	current := client.osdMap.Load()
	appliedFull := false
	fullEpochs := sortedEpochs(batch.FullMaps)
	for _, epoch := range fullEpochs {
		if current != nil && epoch <= current.Epoch() {
			continue
		}
		decoded, err := maps.DecodeOSDMap(batch.FullMaps[epoch], client.config.MapLimits)
		if err != nil {
			return false, fmt.Errorf("decode full OSD map epoch %d: %w", epoch, err)
		}
		if decoded.Epoch() != epoch || decoded.FSID() != batch.FSID {
			return false, fmt.Errorf("%w: full map envelope identity mismatch", maps.ErrMalformedMap)
		}
		current = decoded
		appliedFull = true
	}
	for _, epoch := range sortedEpochs(batch.Incrementals) {
		if current != nil && epoch <= current.Epoch() {
			continue
		}
		if current == nil || epoch != current.Epoch()+1 {
			return false, fmt.Errorf("%w: received epoch %d", ErrMapGap, epoch)
		}
		incremental, err := maps.DecodeOSDMapIncremental(batch.Incrementals[epoch], client.config.MapLimits)
		if err != nil {
			return false, fmt.Errorf("decode incremental OSD map epoch %d: %w", epoch, err)
		}
		if incremental.Epoch() != epoch || incremental.FSID() != batch.FSID {
			return false, fmt.Errorf("%w: incremental envelope identity mismatch", maps.ErrMalformedMap)
		}
		current, err = maps.ApplyOSDMapIncremental(current, incremental, client.config.MapLimits)
		if err != nil {
			return false, err
		}
	}
	if batch.NewestMap != 0 && (current == nil || current.Epoch() < batch.NewestMap) {
		return false, fmt.Errorf("%w: current epoch %d, trim lower bound %d, newest %d", ErrMapGap, mapEpoch(current), batch.TrimLowerBound, batch.NewestMap)
	}
	if current != nil && batch.TrimLowerBound != 0 && current.Epoch() < batch.TrimLowerBound {
		return false, fmt.Errorf("%w: current epoch %d below trim lower bound %d", ErrMapGap, current.Epoch(), batch.TrimLowerBound)
	}
	if current != nil {
		client.osdMap.Store(current)
		if client.monMap.Load() != nil {
			client.onceReady()
		}
	}
	return appliedFull, nil
}

func mapEpoch(osdMap *maps.OSDMap) uint32 {
	if osdMap == nil {
		return 0
	}
	return osdMap.Epoch()
}

func (client *Client) finishRefresh() bool {
	client.mu.Lock()
	defer client.mu.Unlock()
	if !client.refreshPending {
		return false
	}
	client.refreshPending = false
	return true
}

func (client *Client) subscribe(active session) error {
	monStart, osdStart := uint64(0), uint64(0)
	if current := client.monMap.Load(); current != nil {
		monStart = uint64(current.Epoch()) + 1
	}
	if current := client.osdMap.Load(); current != nil {
		osdStart = uint64(current.Epoch()) + 1
	}
	message, err := EncodeSubscribe(map[string]Subscription{"monmap": {Start: monStart}, "osdmap": {Start: osdStart}}, client.config.Hostname, client.config.MessageLimits.MaxBytes)
	if err != nil {
		return err
	}
	return active.Send(client.ctx, message)
}

func (client *Client) requestFullMap() error {
	client.mu.Lock()
	if client.refreshPending {
		client.mu.Unlock()
		return nil
	}
	active := client.session
	client.refreshPending = active != nil
	client.mu.Unlock()
	if active == nil {
		return ErrClosed
	}
	message, err := EncodeSubscribe(map[string]Subscription{"osdmap": {Start: 0, Flags: SubscribeOnce}}, client.config.Hostname, client.config.MessageLimits.MaxBytes)
	if err != nil {
		return err
	}
	if err := active.Send(client.ctx, message); err != nil {
		client.mu.Lock()
		client.refreshPending = false
		client.mu.Unlock()
		return err
	}
	return nil
}

func (client *Client) failover(active *session) bool {
	(*active).Stop()
	for {
		if client.ctx.Err() != nil {
			return false
		}
		next, endpoint, err := client.openNext(client.ctx)
		client.mu.Lock()
		closed := client.closed
		refreshPending := client.refreshPending
		if err == nil && !closed {
			client.session = next
			client.activeEndpoint = endpoint
			client.hasActive = true
			client.refreshPending = false
		}
		client.mu.Unlock()
		if err == nil && closed {
			next.Stop()
			return false
		}
		if err == nil {
			*active = next
			if err := client.subscribe(next); err != nil {
				client.reportNonfatal(err)
			}
			if refreshPending {
				if err := client.requestFullMap(); err != nil {
					client.reportNonfatal(err)
				}
			}
			return true
		}
		client.reportNonfatal(err)
		timer := time.NewTimer(client.config.RetryDelay)
		select {
		case <-client.ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
		}
	}
}

func (client *Client) openNext(ctx context.Context) (session, Endpoint, error) {
	var failures []error
	for range client.config.Endpoints {
		endpoint := client.config.Endpoints[client.nextEndpoint]
		client.nextEndpoint = (client.nextEndpoint + 1) % len(client.config.Endpoints)
		opened, err := client.factory(ctx, endpoint)
		if err == nil {
			return opened, endpoint, nil
		}
		failures = append(failures, err)
	}
	return nil, Endpoint{}, errors.Join(failures...)
}

func (client *Client) pinFSID(fsid maps.FSID) error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.pinnedFSID == nil {
		copy := fsid
		client.pinnedFSID = &copy
		return nil
	}
	if *client.pinnedFSID != fsid {
		return ErrForeignCluster
	}
	return nil
}

func (client *Client) replaceMonMapEndpoints(monMap *maps.MonMap) bool {
	client.mu.Lock()
	defer client.mu.Unlock()
	seen := make(map[netip.AddrPort]struct{})
	discovered := make([]Endpoint, 0, monMap.MonitorCount())
	activePresent := false
	for _, name := range monMap.Ranks() {
		monitor, ok := monMap.Monitor(name)
		if !ok {
			continue
		}
		for _, address := range monitor.Addresses {
			endpoint, ok := address.AddrPort()
			if !ok || address.Type != protocol.AddressV2 {
				continue
			}
			endpoint = netip.AddrPortFrom(endpoint.Addr().Unmap(), endpoint.Port())
			if _, exists := seen[endpoint]; exists {
				continue
			}
			seen[endpoint] = struct{}{}
			discovered = append(discovered, Endpoint{Address: endpoint, EntityAddress: address, priority: monitor.Priority, weight: monitor.Weight})
			if client.hasActive && endpoint == client.activeEndpoint.Address {
				activePresent = true
			}
		}
	}
	if len(discovered) == 0 {
		return true
	}
	sort.SliceStable(discovered, func(left, right int) bool {
		if discovered[left].priority != discovered[right].priority {
			return discovered[left].priority < discovered[right].priority
		}
		return discovered[left].weight > discovered[right].weight
	})
	for _, endpoint := range client.seedEndpoints {
		if _, exists := seen[endpoint.Address]; !exists {
			discovered = append(discovered, endpoint)
		}
	}
	client.config.Endpoints = discovered
	client.nextEndpoint = 0
	return !client.hasActive || activePresent
}

func (client *Client) currentFSID() (maps.FSID, bool) {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.pinnedFSID == nil {
		return maps.FSID{}, false
	}
	return *client.pinnedFSID, true
}

func (client *Client) onceReady() {
	select {
	case <-client.ready:
	default:
		close(client.ready)
	}
}

func (client *Client) report(err error) {
	if err == nil {
		return
	}
	select {
	case client.errors <- err:
	default:
	}
	select {
	case <-client.ready:
		return
	default:
	}
	client.readyFailureOnce.Do(func() {
		client.mu.Lock()
		client.readyErr = err
		client.mu.Unlock()
		close(client.readyFailure)
	})
}

func (client *Client) reportNonfatal(err error) {
	if err == nil {
		return
	}
	select {
	case client.errors <- err:
	default:
	}
}

func sortedEpochs(values map[uint32][]byte) []uint32 {
	epochs := make([]uint32, 0, len(values))
	for epoch := range values {
		epochs = append(epochs, epoch)
	}
	sort.Slice(epochs, func(i, j int) bool { return epochs[i] < epochs[j] })
	return epochs
}

func isReadOnlyCommand(command []string) bool {
	if len(command) != 1 {
		return false
	}
	var request struct {
		Prefix string `json:"prefix"`
	}
	if json.Unmarshal([]byte(command[0]), &request) != nil {
		return false
	}
	switch request.Prefix {
	case "status", "health", "fsid", "version", "report", "quorum_status", "mon dump", "osd dump", "osd pool ls", "df":
		return true
	default:
		return false
	}
}

func validateMapLimits(limits maps.Limits) error {
	if limits.MaxBytes == 0 || limits.MaxMonitors == 0 || limits.MaxAddresses == 0 || limits.MaxLocations == 0 || limits.MaxPools == 0 || limits.MaxOSDs == 0 || limits.MaxPGMappings == 0 || limits.MaxCollectionEntries == 0 {
		return wire.ErrLimitExceeded
	}
	return nil
}

func cloneEntityAddresses(addresses protocol.EntityAddrVec) protocol.EntityAddrVec {
	result := make(protocol.EntityAddrVec, len(addresses))
	for index, address := range addresses {
		result[index] = address
		result[index].SocketData = append([]byte(nil), address.SocketData...)
	}
	return result
}
