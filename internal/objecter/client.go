// Package objecter routes and executes bounded Ceph object requests.
package objecter

import (
	"context"
	"crypto/rand"
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
	ErrNoSortBitwise = errors.New("OSD map does not enable SORTBITWISE")
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

type rawHashRouter interface {
	RouteRawHash(int64, uint32) (Route, error)
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
	Operations       []OperationResult
}

type OperationResult struct {
	Data []byte
	Code int32
}

type EnumerationResult struct {
	Entries []osd.ListEntry
	Next    osd.HObject
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
	Maps              MapSource
	Router            Router
	Authority         *cephx.Connector
	AuthoritySource   func() *cephx.Connector
	ServiceConnector  cephx.ServiceConnectorConfig
	Session           msgr.SessionConfig
	ClientAddresses   protocol.EntityAddrVec
	MessageLimits     osd.Limits
	MaxAttempts       int
	RefreshWait       time.Duration
	MaxMutations      int
	MaxMutationBytes  uint64
	ClientIncarnation int32
	SessionFactory    SessionFactory
}

type Client struct {
	config                Config
	mu                    sync.Mutex
	sessions              map[int32]sessionEntry
	closed                bool
	mutationClosed        bool
	nextTransaction       uint64
	nextMutation          uint64
	pendingMutations      map[uint64]uint64
	retainedMutationBytes uint64
	unknownMutation       uint64
	mutationChanged       chan struct{}
}

type sessionEntry struct {
	address string
	session session
}

func (client *Client) MaxEnumerationEntries() uint64 {
	return uint64(client.config.MessageLimits.MaxBytes / 12)
}

func New(config Config) (*Client, error) {
	if config.Maps == nil || config.MessageLimits.MaxBytes == 0 || config.MessageLimits.MaxOperations == 0 || config.MaxAttempts <= 0 || config.RefreshWait <= 0 {
		return nil, wire.ErrLimitExceeded
	}
	if config.Router == nil {
		config.Router = mapRouter{source: config.Maps}
	}
	if config.MaxMutations == 0 {
		config.MaxMutations = 64
	}
	if config.MaxMutationBytes == 0 {
		config.MaxMutationBytes = uint64(config.MessageLimits.MaxBytes) * uint64(config.MaxMutations)
	}
	if config.MaxMutations < 0 || config.MaxMutationBytes == 0 {
		return nil, wire.ErrLimitExceeded
	}
	if config.ClientIncarnation == 0 {
		var value [4]byte
		if _, err := rand.Read(value[:]); err != nil {
			return nil, err
		}
		config.ClientIncarnation = int32(binary.LittleEndian.Uint32(value[:])&math.MaxInt32 | 1)
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
	return &Client{config: config, sessions: make(map[int32]sessionEntry), nextTransaction: 1, pendingMutations: make(map[uint64]uint64), mutationChanged: make(chan struct{})}, nil
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

func (client *Client) ReadOperations(ctx context.Context, target Target, operations []osd.Operation) (Result, error) {
	if len(operations) == 0 {
		return Result{}, wire.ErrMalformed
	}
	for _, operation := range operations {
		if !isReadOperation(operation.Code) {
			return Result{}, wire.ErrMalformed
		}
	}
	return client.executeOperations(ctx, target, operations, 0, false)
}

func isReadOperation(code uint16) bool {
	switch code {
	case osd.OpRead, osd.OpStat, osd.OpAssertVer, osd.OpOmapGetKeys, osd.OpOmapGetValues,
		osd.OpOmapGetValuesByKeys, osd.OpOmapGetHeader, osd.OpOmapCompare,
		osd.OpCompareExtent, osd.OpGetXattr, osd.OpGetXattrs, osd.OpCompareXattr:
		return true
	default:
		return false
	}
}

func (client *Client) execute(ctx context.Context, target Target, operation osd.Operation) (Result, error) {
	return client.executeOperation(ctx, target, operation, 0, false)
}

func (client *Client) executeOperation(ctx context.Context, target Target, operation osd.Operation, transactionID uint64, mutation bool) (Result, error) {
	return client.executeOperations(ctx, target, []osd.Operation{operation}, transactionID, mutation)
}

func (client *Client) executeOperations(ctx context.Context, target Target, operations []osd.Operation, transactionID uint64, mutation bool) (Result, error) {
	return client.executeRoutedOperations(ctx, target, operations, transactionID, mutation, 0, client.config.Router.Route)
}

func (client *Client) executeRoutedOperations(ctx context.Context, target Target, operations []osd.Operation, transactionID uint64, mutation bool, initialFlags uint32, routeTarget func(Target) (Route, error)) (Result, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if transactionID == 0 {
		var err error
		transactionID, err = client.takeTransactionID()
		if err != nil {
			return Result{}, err
		}
	}
	if len(operations) == 0 || uint64(len(operations)) > uint64(client.config.MessageLimits.MaxOperations) {
		return Result{}, wire.ErrLimitExceeded
	}
	currentTarget := target
	requestFlags := initialFlags
	if len(operations) > 1 {
		requestFlags |= osd.FlagReturnVector
	}
	var lastErr error
	for attempt := 0; attempt < client.config.MaxAttempts; attempt++ {
		route, err := routeTarget(currentTarget)
		if err != nil {
			return Result{}, preserveOutcomeUnknown(lastErr, err)
		}
		if route.Primary < 0 || len(route.Addresses) == 0 {
			return Result{}, preserveOutcomeUnknown(lastErr, ErrNoPrimary)
		}
		if attempt > 0 {
			requestFlags |= osd.FlagRetry
		}
		active, err := client.getSession(route.Primary, route.Addresses)
		if err != nil {
			return Result{}, preserveOutcomeUnknown(lastErr, err)
		}
		var clientGlobalID uint64
		if client.config.AuthoritySource != nil {
			if authority := client.config.AuthoritySource(); authority != nil {
				clientGlobalID = authority.AuthMetadata().GlobalID
			}
		}
		request, err := osd.EncodeRequest(osd.Request{
			MapEpoch: route.Epoch, PG: route.PG, ObjectHash: route.RawHash,
			PoolID: currentTarget.PoolID, Object: currentTarget.Object, Locator: currentTarget.Locator,
			Namespace: currentTarget.Namespace, Snapshot: currentTarget.Snapshot, TransactionID: transactionID, ClientGlobalID: clientGlobalID, ClientIncarnation: client.config.ClientIncarnation, Retry: int32(attempt), Flags: requestFlags,
			Features: uint64(protocol.FeatureOSDClient), Operations: operations,
		}, client.config.MessageLimits)
		if err != nil {
			return Result{}, preserveOutcomeUnknown(lastErr, err)
		}
		object := osd.HObject{Key: currentTarget.Locator, Object: currentTarget.Object, Snapshot: currentTarget.Snapshot, Hash: route.RawHash, Namespace: currentTarget.Namespace, Pool: currentTarget.PoolID}
		var message msgr.Message
		if submitter, ok := active.(targetSubmitter); ok {
			message, err = submitter.SubmitTarget(ctx, route.PG, object, request)
		} else {
			if waiter, ok := active.(backoffWaiter); ok {
				if err := waiter.Wait(ctx, route.PG, object); err != nil {
					if ctx.Err() != nil {
						return Result{}, preserveOutcomeUnknown(lastErr, ctx.Err())
					}
					lastErr = preserveOutcomeUnknown(lastErr, err)
					client.invalidate(route.Primary, active)
					client.refresh(ctx, route.Epoch)
					continue
				}
			}
			message, err = active.Submit(ctx, request)
		}
		if err != nil {
			if errors.Is(err, msgr.ErrQueueSaturated) {
				return Result{}, preserveOutcomeUnknown(lastErr, err)
			}
			if mutation && errors.Is(err, msgr.ErrOutcomeUnknown) {
				lastErr = preserveOutcomeUnknown(lastErr, err)
				client.invalidate(route.Primary, active)
				if !errors.Is(err, msgr.ErrReconnectExhausted) && !errors.Is(err, ErrStaleMap) {
					changed, refreshErr := client.waitForPrimaryChange(ctx, currentTarget, route)
					if !changed {
						return Result{}, errors.Join(err, refreshErr)
					}
				} else {
					client.refresh(ctx, route.Epoch)
				}
				continue
			}
			if ctx.Err() != nil {
				return Result{}, preserveOutcomeUnknown(lastErr, ctx.Err())
			}
			lastErr = preserveOutcomeUnknown(lastErr, err)
			client.invalidate(route.Primary, active)
			client.refresh(ctx, route.Epoch)
			continue
		}
		reply, err := osd.DecodeReply(message, client.config.MessageLimits)
		if err != nil {
			client.invalidate(route.Primary, active)
			if mutation {
				return Result{}, fmt.Errorf("%w: %v", msgr.ErrOutcomeUnknown, err)
			}
			return Result{}, fmt.Errorf("%w: %v", osd.ErrMalformedReply, err)
		}
		if reply.Object != currentTarget.Object || reply.PG != route.PG {
			client.invalidate(route.Primary, active)
			if mutation {
				return Result{}, fmt.Errorf("%w: %v", msgr.ErrOutcomeUnknown, osd.ErrMalformedReply)
			}
			return Result{}, osd.ErrMalformedReply
		}
		if reply.Retry >= 0 && reply.Retry != int32(attempt) {
			client.invalidate(route.Primary, active)
			if mutation {
				return Result{}, fmt.Errorf("%w: %v", msgr.ErrOutcomeUnknown, osd.ErrMalformedReply)
			}
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
			if mutation {
				transactionID, err = client.takeTransactionID()
				if err != nil {
					return Result{}, err
				}
			}
			continue
		}
		if reply.Result == -11 {
			lastErr = protocol.WireErrno(reply.Result)
			client.refresh(ctx, route.Epoch)
			if mutation {
				transactionID, err = client.takeTransactionID()
				if err != nil {
					return Result{}, err
				}
			}
			continue
		}
		if len(reply.Operations) != len(operations) {
			client.invalidate(route.Primary, active)
			if mutation {
				return Result{}, fmt.Errorf("%w: %v", msgr.ErrOutcomeUnknown, osd.ErrMalformedReply)
			}
			return Result{}, osd.ErrMalformedReply
		}
		retry := reply.Result == -11
		for index := range operations {
			if reply.Operations[index].Operation != operations[index].Code {
				client.invalidate(route.Primary, active)
				if mutation {
					return Result{}, fmt.Errorf("%w: %v", msgr.ErrOutcomeUnknown, osd.ErrMalformedReply)
				}
				return Result{}, osd.ErrMalformedReply
			}
			retry = retry || reply.Operations[index].Code == -11
		}
		if retry {
			lastErr = protocol.WireErrno(-11)
			client.refresh(ctx, route.Epoch)
			if mutation {
				transactionID, err = client.takeTransactionID()
				if err != nil {
					return Result{}, err
				}
			}
			continue
		}
		if mutation && reply.Result == 0 && uint64(reply.Flags)&uint64(osd.FlagOnDisk) == 0 {
			client.invalidate(route.Primary, active)
			return Result{}, fmt.Errorf("%w: mutation reply is not durable", msgr.ErrOutcomeUnknown)
		}
		result := Result{Version: reply.Version, Operations: make([]OperationResult, len(reply.Operations))}
		for index, operationResult := range reply.Operations {
			if operations[index].Code == osd.OpRead && uint64(len(operationResult.Data)) > operations[index].Length {
				client.invalidate(route.Primary, active)
				return Result{}, osd.ErrMalformedReply
			}
			result.Operations[index] = OperationResult{Data: append([]byte(nil), operationResult.Data...), Code: operationResult.Code}
		}
		result.Data = append([]byte(nil), result.Operations[0].Data...)
		if reply.Result < 0 {
			return result, protocol.WireErrno(reply.Result)
		}
		for index, operationResult := range result.Operations {
			if operationResult.Code < 0 && operations[index].Flags&osd.OpFlagFailOK == 0 {
				return result, protocol.WireErrno(operationResult.Code)
			}
		}
		return result, nil
	}
	return Result{}, fmt.Errorf("%w: %w", ErrRecovery, lastErr)
}

func (client *Client) PGNLS(ctx context.Context, poolID int64, namespace string, cursor osd.HObject, count uint64) (osd.ListPage, error) {
	if count == 0 || (!cursor.IsMin() && (cursor.Pool != poolID || cursor.Snapshot != osd.NoSnap || cursor.IsMax())) {
		return osd.ListPage{}, wire.ErrMalformed
	}
	route, err := client.routeRawHash(poolID, cursor.Hash)
	if err != nil {
		return osd.ListPage{}, err
	}
	operation, err := osd.EncodePGNLSOperation(cursor, count, route.Epoch, client.config.MessageLimits.MaxBytes)
	if err != nil {
		return osd.ListPage{}, err
	}
	first := true
	routeTarget := func(Target) (Route, error) {
		if first {
			first = false
			return route, nil
		}
		return client.routeRawHash(poolID, cursor.Hash)
	}
	result, err := client.executeRoutedOperations(ctx, Target{PoolID: poolID, Namespace: namespace, Snapshot: osd.NoSnap}, []osd.Operation{operation}, 0, false, osd.FlagPGOp|osd.FlagIgnoreOverlay, routeTarget)
	if err != nil {
		return osd.ListPage{}, err
	}
	return osd.DecodePGNLSPage(result.Data, client.config.MessageLimits.MaxBytes, client.config.MessageLimits.MaxBytes/12)
}

func (client *Client) Enumerate(ctx context.Context, poolID int64, namespace string, start, end osd.HObject, limit uint64) (EnumerationResult, error) {
	if limit == 0 || limit > client.MaxEnumerationEntries() || start.IsMax() || (!end.IsMax() && osd.CompareHObject(start, end) > 0) {
		return EnumerationResult{}, wire.ErrMalformed
	}
	return enumeratePages(poolID, start, end, limit, func(cursor osd.HObject, count uint64) (osd.ListPage, []osd.HObject, error) {
		page, err := client.PGNLS(ctx, poolID, namespace, cursor, count)
		if err != nil {
			return osd.ListPage{}, nil, err
		}
		entryCursors, err := client.enumerationEntryCursors(poolID, namespace, page.Entries)
		return page, entryCursors, err
	})
}

func enumeratePages(poolID int64, start, end osd.HObject, limit uint64, fetch func(osd.HObject, uint64) (osd.ListPage, []osd.HObject, error)) (EnumerationResult, error) {
	if osd.CompareHObject(start, end) == 0 {
		return EnumerationResult{Next: end}, nil
	}
	result := EnumerationResult{Entries: make([]osd.ListEntry, 0, limit), Next: start}
	for uint64(len(result.Entries)) < limit && osd.CompareHObject(result.Next, end) < 0 {
		remaining := limit - uint64(len(result.Entries))
		page, entryCursors, err := fetch(result.Next, remaining)
		if err != nil {
			return EnumerationResult{}, err
		}
		if len(entryCursors) != len(page.Entries) {
			return EnumerationResult{}, osd.ErrMalformedReply
		}
		if err := validateEnumerationPage(poolID, result.Next, page.Next, entryCursors); err != nil {
			return EnumerationResult{}, err
		}
		next := page.Next
		entryCount := len(page.Entries)
		if osd.CompareHObject(next, end) > 0 {
			next = end
			for entryCount > 0 && osd.CompareHObject(entryCursors[entryCount-1], end) >= 0 {
				entryCount--
			}
		}
		available := int(limit - uint64(len(result.Entries)))
		if entryCount > available {
			next = entryCursors[available]
			entryCount = available
		}
		result.Entries = append(result.Entries, page.Entries[:entryCount]...)
		result.Next = next
	}
	return result, nil
}

func (client *Client) enumerationEntryCursors(poolID int64, namespace string, entries []osd.ListEntry) ([]osd.HObject, error) {
	osdMap := client.config.Maps.OSDMap()
	if osdMap == nil {
		return nil, ErrNoPrimary
	}
	if _, ok := osdMap.PoolByID(poolID); !ok {
		return nil, protocol.WireErrno(-2)
	}
	result := make([]osd.HObject, len(entries))
	for index, entry := range entries {
		if namespace != "\x01" && entry.Namespace != namespace {
			return nil, osd.ErrMalformedReply
		}
		placement, err := osdMap.MapObject(poolID, entry.Object, entry.Locator, entry.Namespace)
		if err != nil {
			return nil, err
		}
		result[index] = osd.HObject{Key: entry.Locator, Object: entry.Object, Snapshot: osd.NoSnap, Hash: placement.RawHash, Namespace: entry.Namespace, Pool: poolID}
	}
	return result, nil
}

func validateEnumerationPage(poolID int64, start, next osd.HObject, entries []osd.HObject) error {
	if !next.IsMax() && (next.IsMin() || next.Snapshot != osd.NoSnap || next.Pool != poolID) {
		return osd.ErrMalformedReply
	}
	previous := start
	for _, entry := range entries {
		if osd.CompareHObject(entry, previous) < 0 || (!next.IsMax() && osd.CompareHObject(entry, next) >= 0) {
			return osd.ErrMalformedReply
		}
		previous = entry
	}
	if !next.IsMax() && osd.CompareHObject(next, start) <= 0 {
		return osd.ErrMalformedReply
	}
	return nil
}

func (client *Client) routeRawHash(poolID int64, hash uint32) (Route, error) {
	if router, ok := client.config.Router.(rawHashRouter); ok {
		return router.RouteRawHash(poolID, hash)
	}
	osdMap := client.config.Maps.OSDMap()
	if osdMap == nil {
		return Route{}, ErrNoPrimary
	}
	return mapRouter{source: client.config.Maps}.RouteRawHash(poolID, hash)
}

func preserveOutcomeUnknown(previous, current error) error {
	if errors.Is(previous, msgr.ErrOutcomeUnknown) {
		return errors.Join(previous, current)
	}
	return current
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

func (router mapRouter) RouteRawHash(poolID int64, hash uint32) (Route, error) {
	osdMap := router.source.OSDMap()
	if osdMap == nil {
		return Route{}, ErrNoPrimary
	}
	if !osdMap.SortBitwise() {
		return Route{}, ErrNoSortBitwise
	}
	if _, ok := osdMap.PoolByID(poolID); !ok {
		return Route{}, protocol.WireErrno(-2)
	}
	placement, err := osdMap.PlaceRawHash(poolID, hash)
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

func (client *Client) waitForPrimaryChange(ctx context.Context, target Target, failed Route) (bool, error) {
	epoch := failed.Epoch
	var lastErr error
	for range client.config.MaxAttempts {
		refreshCtx, cancel := context.WithTimeout(ctx, client.config.RefreshWait)
		err := client.config.Maps.RefreshOSDMap(refreshCtx, epoch)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return false, ctx.Err()
			}
			lastErr = err
			continue
		}
		refreshed, err := client.config.Router.Route(target)
		if err != nil {
			return false, err
		}
		if refreshed.Primary != failed.Primary {
			return true, nil
		}
		if refreshed.Epoch <= epoch {
			lastErr = fmt.Errorf("OSD map did not advance past epoch %d", epoch)
			continue
		}
		epoch = refreshed.Epoch
	}
	if lastErr != nil {
		return false, fmt.Errorf("OSD map refresh after epoch %d with acting primary %d unchanged: %w", epoch, failed.Primary, lastErr)
	}
	return false, fmt.Errorf("acting primary %d unchanged through epoch %d", failed.Primary, epoch)
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
	client.mutationClosed = true
	client.notifyMutationWaitersLocked()
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
		sessionConfig.ReconnectPolicy = msgr.ReplayPending
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
