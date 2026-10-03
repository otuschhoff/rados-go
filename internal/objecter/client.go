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

	"github.com/otuschhoff/rados-go/internal/cephx"
	wire "github.com/otuschhoff/rados-go/internal/encoding"
	"github.com/otuschhoff/rados-go/internal/maps"
	"github.com/otuschhoff/rados-go/internal/msgr"
	"github.com/otuschhoff/rados-go/internal/osd"
	"github.com/otuschhoff/rados-go/internal/protocol"
)

var (
	ErrClosed           = errors.New("objecter closed")
	ErrNoPrimary        = errors.New("object has no acting primary")
	ErrRecovery         = errors.New("read recovery exhausted")
	ErrMalformedStat    = errors.New("malformed stat result")
	ErrStaleMap         = errors.New("OSD supplied a newer map")
	ErrNoSortBitwise    = errors.New("OSD map does not enable SORTBITWISE")
	ErrWatchInterrupted = errors.New("watch interrupted; events may have been lost")
)

const (
	readReplyFrontBytes     = uint64(144)
	maxTrackedRouteAttempts = 4096
)

type MapSource interface {
	OSDMap() *maps.OSDMap
	RefreshOSDMap(context.Context, uint32) error
}

type osdMapWaiter interface {
	WaitForOSDMap(context.Context, uint32) error
}

type osdSessionGenerationSource interface {
	OSDSessionGeneration(int32) uint64
}

type Route struct {
	Epoch     uint32
	PG        maps.PG
	RawHash   uint32
	Primary   int32
	Shard     int8
	Sharded   bool
	Addresses protocol.EntityAddrVec
}

type Router interface {
	Route(Target) (Route, error)
}

type rawHashRouter interface {
	RouteRawHash(int64, uint32) (Route, error)
}

type Target struct {
	PoolID           int64
	Object           string
	Locator          string
	Namespace        string
	Snapshot         uint64
	SnapshotSequence uint64
	WriteSnapshots   []uint64
}

type Result struct {
	Data             []byte
	Size             uint64
	ModificationTime time.Time
	Version          uint64
	Operations       []OperationResult
	route            Route
}

type SparseReadResult struct {
	Extents []osd.SparseExtent
	Version uint64
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

type notificationSession interface {
	Notifications() <-chan osd.WatchNotification
	NotificationError() error
}

type sessionInterruptionSource interface {
	Interruptions() <-chan error
}

type backoffWaiter interface {
	Wait(context.Context, maps.PG, osd.HObject) error
}

type targetSubmitter interface {
	SubmitTarget(context.Context, maps.PG, osd.HObject, msgr.Message) (msgr.Message, error)
}

type SessionFactory func(int32, protocol.EntityAddrVec) (session, error)

type OSDSessionEvent struct {
	OSDID     int32
	Available bool
	Err       error
}

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
	UnlimitedRetries  bool
	RefreshWait       time.Duration
	MaxMutations      int
	MaxMutationBytes  uint64
	MaxBackoffs       int
	MaxBackoffBytes   uint64
	ClientIncarnation int32
	SessionFactory    SessionFactory
	ObserveSession    func(OSDSessionEvent)
}

type Client struct {
	config                Config
	mu                    sync.Mutex
	done                  chan struct{}
	sessions              map[int32]sessionEntry
	creations             map[int32]*sessionCreation
	closed                bool
	closeDone             chan struct{}
	mutationClosed        bool
	nextTransaction       uint64
	nextMutation          uint64
	pendingMutations      map[uint64]uint64
	retainedMutationBytes uint64
	unknownMutation       uint64
	mutationChanged       chan struct{}
	mutationBuffers       *mutationBufferCache
	watches               map[uint64]*Watch
	notifies              map[uint64]chan notifyCompletion
	mapWatchCancel        context.CancelFunc
	routeAttempts         map[uint64]*routedAttempt
	nextRouteAttempt      uint64
	workers               sync.WaitGroup
}

type routedAttempt struct {
	route    Route
	resolve  func() (Route, error)
	cancel   context.CancelFunc
	remapped bool
}

type notifyCompletion struct {
	notification osd.WatchNotification
	err          error
}

type sessionEntry struct {
	address    string
	generation uint64
	session    session
}

type sessionCreation struct {
	address    string
	generation uint64
	superseded bool
	done       chan struct{}
	result     session
	err        error
}

func (client *Client) MaxEnumerationEntries() uint64 {
	return uint64(client.config.MessageLimits.MaxBytes / 12)
}

func New(config Config) (*Client, error) {
	if config.MaxBackoffs < 0 {
		return nil, wire.ErrLimitExceeded
	}
	if config.MaxBackoffs == 0 {
		config.MaxBackoffs = 4096
	}
	if config.MaxBackoffBytes == 0 {
		config.MaxBackoffBytes = 8 << 20
	}
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
	productionSessions := config.SessionFactory == nil
	if productionSessions {
		if config.AuthoritySource == nil {
			config.AuthoritySource = func() *cephx.Connector { return config.Authority }
		}
		if config.AuthoritySource() == nil || len(config.ClientAddresses) == 0 {
			return nil, wire.ErrMalformed
		}
		config.SessionFactory = productionSessionFactory(config)
	}
	config.ClientAddresses = cloneAddresses(config.ClientAddresses)
	client := &Client{config: config, done: make(chan struct{}), sessions: make(map[int32]sessionEntry), nextTransaction: 1, pendingMutations: make(map[uint64]uint64), mutationChanged: make(chan struct{}), watches: make(map[uint64]*Watch), notifies: make(map[uint64]chan notifyCompletion)}
	if productionSessions {
		client.mutationBuffers = newMutationBufferCache()
	}
	if waiter, ok := config.Maps.(osdMapWaiter); ok {
		watchCtx, cancel := context.WithCancel(context.Background())
		client.mapWatchCancel = cancel
		client.routeAttempts = make(map[uint64]*routedAttempt)
		client.workers.Add(1)
		go client.watchOSDMaps(watchCtx, waiter)
	}
	return client, nil
}

func (client *Client) Read(ctx context.Context, target Target, offset, length uint64) (Result, error) {
	minimumReplyBytes := readReplyFrontBytes + uint64(len(target.Object))
	if length > math.MaxUint64-offset || minimumReplyBytes > uint64(client.config.MessageLimits.MaxBytes) || length > uint64(client.config.MessageLimits.MaxBytes)-minimumReplyBytes {
		return Result{}, wire.ErrLimitExceeded
	}
	return client.executeRoutedResult(ctx, target, []osd.Operation{{Code: osd.OpRead, Offset: offset, Length: length}}, 0, false, false, 0, client.config.Router.Route, false)
}

func (client *Client) ReadInto(ctx context.Context, target Target, offset uint64, destination []byte) (Result, error) {
	length := uint64(len(destination))
	minimumReplyBytes := readReplyFrontBytes + uint64(len(target.Object))
	if length > math.MaxUint64-offset || minimumReplyBytes > uint64(client.config.MessageLimits.MaxBytes) || length > uint64(client.config.MessageLimits.MaxBytes)-minimumReplyBytes {
		return Result{}, wire.ErrLimitExceeded
	}
	return client.executeRoutedResultInto(ctx, target, []osd.Operation{{Code: osd.OpRead, Offset: offset, Length: length}}, 0, false, false, 0, client.config.Router.Route, false, destination, true, false, nil)
}

func (client *Client) SparseRead(ctx context.Context, target Target, offset, length uint64) (SparseReadResult, error) {
	if length > math.MaxInt32 || length > math.MaxUint64-offset {
		return SparseReadResult{}, wire.ErrLimitExceeded
	}
	result, err := client.execute(ctx, target, osd.Operation{Code: osd.OpSparseRead, Offset: offset, Length: length})
	if err != nil {
		return SparseReadResult{}, err
	}
	extents, err := osd.DecodeSparseRead(result.Data, offset, length, client.config.MessageLimits.MaxBytes, client.config.MessageLimits.MaxBytes/16)
	if err != nil {
		return SparseReadResult{}, err
	}
	return SparseReadResult{Extents: extents, Version: result.Version}, nil
}

func (client *Client) Checksum(ctx context.Context, target Target, kind uint8, seed []byte, offset, length, chunk uint64) ([]byte, error) {
	if chunk > math.MaxUint32 {
		return nil, wire.ErrLimitExceeded
	}
	result, err := client.execute(ctx, target, osd.Operation{Code: osd.OpChecksum, Offset: offset, Length: length, ChunkSize: uint32(chunk), ChecksumType: kind, Data: seed})
	if err != nil {
		return nil, err
	}
	return osd.DecodeChecksum(result.Data, kind, client.config.MessageLimits.MaxBytes)
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
	classCall := false
	for _, operation := range operations {
		if !isReadOperation(operation.Code) && operation.Code != osd.OpCall {
			return Result{}, wire.ErrMalformed
		}
		classCall = classCall || operation.Code == osd.OpCall
	}
	if classCall {
		return client.ClassOperations(ctx, target, operations)
	}
	return client.executeOperations(ctx, target, operations, 0, false)
}

func isReadOperation(code uint16) bool {
	switch code {
	case osd.OpRead, osd.OpStat, osd.OpSparseRead, osd.OpAssertVer, osd.OpOmapGetKeys, osd.OpOmapGetValues,
		osd.OpOmapGetValuesByKeys, osd.OpOmapGetHeader, osd.OpOmapCompare,
		osd.OpChecksum, osd.OpCompareExtent, osd.OpGetXattr, osd.OpGetXattrs, osd.OpCompareXattr, osd.OpListWatchers, osd.OpScrubList:
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
	return client.executeRoutedOperations(ctx, target, operations, transactionID, mutation, mutation, 0, client.config.Router.Route)
}

func (client *Client) executeRoutedOperations(ctx context.Context, target Target, operations []osd.Operation, transactionID uint64, outcomeSensitive, durable bool, initialFlags uint32, routeTarget func(Target) (Route, error)) (Result, error) {
	return client.executeRoutedOperationsWithOwnership(ctx, target, operations, transactionID, outcomeSensitive, durable, initialFlags, routeTarget, false)
}

func (client *Client) executeRoutedOperationsWithOwnership(ctx context.Context, target Target, operations []osd.Operation, transactionID uint64, outcomeSensitive, durable bool, initialFlags uint32, routeTarget func(Target) (Route, error), immutable bool) (Result, error) {
	return client.executeRoutedResultInto(ctx, target, operations, transactionID, outcomeSensitive, durable, initialFlags, routeTarget, true, nil, false, immutable, nil)
}

func (client *Client) executeRoutedResult(ctx context.Context, target Target, operations []osd.Operation, transactionID uint64, outcomeSensitive, durable bool, initialFlags uint32, routeTarget func(Target) (Route, error), retainOperations bool) (Result, error) {
	return client.executeRoutedResultInto(ctx, target, operations, transactionID, outcomeSensitive, durable, initialFlags, routeTarget, retainOperations, nil, false, false, nil)
}

func (client *Client) executeRoutedResultInto(ctx context.Context, target Target, operations []osd.Operation, transactionID uint64, outcomeSensitive, durable bool, initialFlags uint32, routeTarget func(Target) (Route, error), retainOperations bool, destination []byte, readInto, immutable bool, payloadLease *msgr.MessageLease) (Result, error) {
	releaseReply := func() {}
	defer func() { releaseReply() }()
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
	_, boundedByContext := ctx.Deadline()
	for attempt := 0; client.config.UnlimitedRetries || boundedByContext || attempt < client.config.MaxAttempts; attempt++ {
		releaseReply()
		releaseReply = func() {}
		if err := ctx.Err(); err != nil {
			return Result{}, preserveOutcomeUnknown(lastErr, err)
		}
		route, err := client.waitForRoute(ctx, currentTarget, routeTarget)
		if err != nil {
			return Result{}, preserveOutcomeUnknown(lastErr, err)
		}
		if attempt > 0 {
			requestFlags |= osd.FlagRetry
		}
		active, err := client.getSessionContext(ctx, route.Primary, route.Addresses)
		if err != nil {
			if errors.Is(err, ErrStaleMap) {
				continue
			}
			return Result{}, preserveOutcomeUnknown(lastErr, err)
		}
		var clientGlobalID uint64
		if client.config.AuthoritySource != nil {
			if authority := client.config.AuthoritySource(); authority != nil {
				clientGlobalID = authority.InstanceID()
			}
		}
		encode := osd.EncodeRequest
		if immutable {
			encode = osd.EncodeImmutableRequest
		}
		request, err := encode(osd.Request{
			MapEpoch: route.Epoch, PG: route.PG, ObjectHash: route.RawHash, Shard: route.Shard, Sharded: route.Sharded,
			PoolID: currentTarget.PoolID, Object: currentTarget.Object, Locator: currentTarget.Locator,
			Namespace: currentTarget.Namespace, Snapshot: currentTarget.Snapshot, SnapshotSequence: currentTarget.SnapshotSequence,
			WriteSnapshots: currentTarget.WriteSnapshots, TransactionID: transactionID, ClientGlobalID: clientGlobalID, ClientIncarnation: client.config.ClientIncarnation, Retry: int32(attempt), Flags: requestFlags,
			Features: uint64(protocol.FeatureOSDClient), Operations: operations, ExplicitFlags: explicitCoordinationFlags(operations),
		}, client.config.MessageLimits)
		if err != nil {
			return Result{}, preserveOutcomeUnknown(lastErr, err)
		}
		object := osd.HObject{Key: currentTarget.Locator, Object: currentTarget.Object, Snapshot: currentTarget.Snapshot, Hash: route.RawHash, Namespace: currentTarget.Namespace, Pool: currentTarget.PoolID}
		msgr.RecordPreparedRequest(ctx)
		if payloadLease != nil {
			request = msgr.RetainLeasedMessage(request, payloadLease)
		} else if immutable {
			request = msgr.RetainImmutableMessage(request)
		} else {
			request = msgr.TakeMessageOwnership(request)
		}
		attemptTarget := currentTarget
		submitCtx, finishSubmit, err := client.beginRoutedAttempt(ctx, route, func() (Route, error) {
			return routeTarget(attemptTarget)
		})
		if err != nil {
			return Result{}, preserveOutcomeUnknown(lastErr, err)
		}
		var message msgr.Message
		if submitter, ok := active.(interface {
			SubmitBorrowedTarget(context.Context, maps.PG, osd.HObject, msgr.Message) (msgr.Message, func(), error)
		}); readInto && ok {
			message, releaseReply, err = submitter.SubmitBorrowedTarget(submitCtx, route.PG, object, request)
		} else if submitter, ok := active.(targetSubmitter); ok {
			message, err = submitter.SubmitTarget(submitCtx, route.PG, object, request)
		} else {
			if waiter, ok := active.(backoffWaiter); ok {
				err = waiter.Wait(submitCtx, route.PG, object)
			}
			if err == nil {
				message, err = active.Submit(submitCtx, request)
			}
		}
		remapped := finishSubmit()
		if remapped && err != nil && ctx.Err() == nil {
			if outcomeSensitive && errors.Is(err, msgr.ErrOutcomeUnknown) {
				lastErr = preserveOutcomeUnknown(lastErr, err)
			}
			continue
		}
		if err != nil {
			select {
			case <-client.done:
				if outcomeSensitive && errors.Is(err, msgr.ErrOutcomeUnknown) {
					lastErr = preserveOutcomeUnknown(lastErr, err)
				}
				return Result{}, preserveOutcomeUnknown(lastErr, ErrClosed)
			default:
			}
			if errors.Is(err, msgr.ErrQueueSaturated) {
				return Result{}, preserveOutcomeUnknown(lastErr, err)
			}
			if outcomeSensitive && errors.Is(err, msgr.ErrOutcomeUnknown) {
				lastErr = preserveOutcomeUnknown(lastErr, err)
				client.invalidate(route.Primary, active)
				if !errors.Is(err, msgr.ErrReconnectExhausted) && !errors.Is(err, msgr.ErrSessionDisconnected) && !errors.Is(err, ErrStaleMap) {
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
		decode := osd.DecodeReply
		if owned, ok := active.(interface{ OwnsReplyMessages() bool }); ok && owned.OwnsReplyMessages() && !retainOperations {
			decode = osd.DecodeOwnedReply
		}
		reply, err := decode(message, client.config.MessageLimits)
		if err != nil {
			client.invalidate(route.Primary, active)
			if outcomeSensitive {
				return Result{}, fmt.Errorf("%w: %v", msgr.ErrOutcomeUnknown, err)
			}
			return Result{}, fmt.Errorf("%w: %v", osd.ErrMalformedReply, err)
		}
		if reply.Object != currentTarget.Object || reply.PG != route.PG {
			client.invalidate(route.Primary, active)
			if outcomeSensitive {
				return Result{}, fmt.Errorf("%w: %v", msgr.ErrOutcomeUnknown, osd.ErrMalformedReply)
			}
			return Result{}, osd.ErrMalformedReply
		}
		if reply.Retry >= 0 && reply.Retry != int32(attempt) {
			client.invalidate(route.Primary, active)
			if outcomeSensitive {
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
			if outcomeSensitive {
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
			if outcomeSensitive {
				transactionID, err = client.takeTransactionID()
				if err != nil {
					return Result{}, err
				}
			}
			continue
		}
		if len(reply.Operations) != len(operations) {
			client.invalidate(route.Primary, active)
			if outcomeSensitive {
				return Result{}, fmt.Errorf("%w: %v", msgr.ErrOutcomeUnknown, osd.ErrMalformedReply)
			}
			return Result{}, osd.ErrMalformedReply
		}
		retry := reply.Result == -11
		for index := range operations {
			if reply.Operations[index].Operation != operations[index].Code {
				client.invalidate(route.Primary, active)
				if outcomeSensitive {
					return Result{}, fmt.Errorf("%w: %v", msgr.ErrOutcomeUnknown, osd.ErrMalformedReply)
				}
				return Result{}, osd.ErrMalformedReply
			}
			retry = retry || reply.Operations[index].Code == -11
		}
		if retry {
			lastErr = protocol.WireErrno(-11)
			client.refresh(ctx, route.Epoch)
			if outcomeSensitive {
				transactionID, err = client.takeTransactionID()
				if err != nil {
					return Result{}, err
				}
			}
			continue
		}
		if durable && reply.Result == 0 && uint64(reply.Flags)&uint64(osd.FlagOnDisk) == 0 {
			client.invalidate(route.Primary, active)
			return Result{}, fmt.Errorf("%w: mutation reply is not durable", msgr.ErrOutcomeUnknown)
		}
		result := Result{Version: reply.Version, route: route}
		if retainOperations {
			result.Operations = make([]OperationResult, len(reply.Operations))
		}
		for index, operationResult := range reply.Operations {
			if operations[index].Code == osd.OpRead && uint64(len(operationResult.Data)) > operations[index].Length {
				client.invalidate(route.Primary, active)
				return Result{}, osd.ErrMalformedReply
			}
			if retainOperations {
				result.Operations[index] = OperationResult{Data: operationResult.Data, Code: operationResult.Code}
			}
		}
		if retainOperations {
			result.Data = append([]byte(nil), result.Operations[0].Data...)
		} else if !readInto {
			result.Data = reply.Operations[0].Data
		}
		if reply.Result < 0 {
			return result, protocol.WireErrno(reply.Result)
		}
		for index, operationResult := range reply.Operations {
			if operationResult.Code < 0 && operations[index].Flags&osd.OpFlagFailOK == 0 {
				return result, protocol.WireErrno(operationResult.Code)
			}
		}
		if readInto {
			if err := ctx.Err(); err != nil {
				return Result{}, err
			}
			data := reply.Operations[0].Data
			if len(data) > len(destination) {
				return Result{}, osd.ErrMalformedReply
			}
			copy(destination, data)
			result.Data = destination[:len(data)]
		}
		return result, nil
	}
	return Result{}, fmt.Errorf("%w: %w", ErrRecovery, lastErr)
}

func explicitCoordinationFlags(operations []osd.Operation) bool {
	for _, operation := range operations {
		switch operation.Code {
		case osd.OpWatch, osd.OpNotify, osd.OpNotifyAck:
		default:
			return false
		}
	}
	return len(operations) > 0
}

func (client *Client) PGNLS(ctx context.Context, poolID int64, namespace string, cursor osd.HObject, count uint64) (osd.ListPage, error) {
	if count == 0 || (!cursor.IsMin() && (cursor.Pool != poolID || cursor.Snapshot != osd.NoSnap || cursor.IsMax())) {
		return osd.ListPage{}, wire.ErrMalformed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	route, err := client.waitForResolvedRoute(ctx, func() (Route, error) {
		return client.routeRawHash(poolID, cursor.Hash)
	})
	if err != nil {
		return osd.ListPage{}, err
	}
	operation, err := osd.EncodePGNLSOperation(cursor, count, route.Epoch, client.config.MessageLimits.MaxBytes)
	if err != nil {
		return osd.ListPage{}, err
	}
	routeTarget := func(Target) (Route, error) {
		return client.routeRawHash(poolID, cursor.Hash)
	}
	result, err := client.executeRoutedOperations(ctx, Target{PoolID: poolID, Namespace: namespace, Snapshot: osd.NoSnap}, []osd.Operation{operation}, 0, false, false, osd.FlagPGOp|osd.FlagIgnoreOverlay, routeTarget)
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

func (client *Client) waitForRoute(ctx context.Context, target Target, routeTarget func(Target) (Route, error)) (Route, error) {
	return client.waitForResolvedRoute(ctx, func() (Route, error) { return routeTarget(target) })
}

func (client *Client) waitForResolvedRoute(ctx context.Context, resolve func() (Route, error)) (Route, error) {
	_, boundedByContext := ctx.Deadline()
	for refreshes := 0; ; refreshes++ {
		route, err := resolve()
		if err != nil && !errors.Is(err, ErrNoPrimary) {
			return Route{}, err
		}
		if err == nil && route.Primary >= 0 && len(route.Addresses) != 0 {
			return route, nil
		}
		if !client.config.UnlimitedRetries && !boundedByContext && refreshes >= client.config.MaxAttempts {
			return Route{}, ErrNoPrimary
		}
		epoch := route.Epoch
		if current := client.config.Maps.OSDMap(); current != nil && current.Epoch() > epoch {
			epoch = current.Epoch()
		}
		refreshCtx, cancel := context.WithTimeout(ctx, client.config.RefreshWait)
		refreshErr := client.config.Maps.RefreshOSDMap(refreshCtx, epoch)
		cancel()
		if ctx.Err() != nil {
			return Route{}, ctx.Err()
		}
		if refreshErr != nil && !errors.Is(refreshErr, context.DeadlineExceeded) {
			return Route{}, refreshErr
		}
	}
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
	return Route{Epoch: osdMap.Epoch(), PG: placement.PG, RawHash: placement.RawHash, Primary: placement.ActingPrimary, Shard: placement.PrimaryShard, Sharded: placement.Sharded, Addresses: addresses}, nil
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
	return Route{Epoch: osdMap.Epoch(), PG: placement.PG, RawHash: placement.RawHash, Primary: placement.ActingPrimary, Shard: placement.PrimaryShard, Sharded: placement.Sharded, Addresses: addresses}, nil
}

func (client *Client) refresh(ctx context.Context, epoch uint32) {
	refreshCtx, cancel := context.WithTimeout(ctx, client.config.RefreshWait)
	defer cancel()
	_ = client.config.Maps.RefreshOSDMap(refreshCtx, epoch)
}

func (client *Client) waitForPrimaryChange(ctx context.Context, target Target, failed Route) (bool, error) {
	epoch := failed.Epoch
	var lastErr error
	for attempt := 0; client.config.UnlimitedRetries || attempt < client.config.MaxAttempts; attempt++ {
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
	return client.getSessionContext(context.Background(), osdID, addresses)
}

func (client *Client) sessionTarget(osdID int32, addresses protocol.EntityAddrVec) (string, uint64, error) {
	address, ok := selectAddress(addresses)
	if !ok {
		return "", 0, ErrNoPrimary
	}
	generation := uint64(0)
	if source, ok := client.config.Maps.(osdSessionGenerationSource); ok {
		generation = source.OSDSessionGeneration(osdID)
	}
	if current := client.config.Maps.OSDMap(); current != nil {
		state, stateKnown := current.OSDState(osdID)
		if stateKnown && (!state.Exists || !state.Up) {
			client.supersedeCreation(osdID)
			return "", 0, ErrNoPrimary
		}
		currentEndpoint, addressesKnown := current.OSDClientEndpoint(osdID)
		if stateKnown && !addressesKnown {
			client.supersedeCreation(osdID)
			return "", 0, ErrNoPrimary
		}
		if addressesKnown {
			if !currentEndpoint.IsValid() {
				client.supersedeCreation(osdID)
				return "", 0, ErrNoPrimary
			}
			currentAddress := currentEndpoint.String()
			if currentAddress != address {
				client.mu.Lock()
				if pending := client.creations[osdID]; pending != nil && (pending.address != currentAddress || pending.generation != generation) {
					pending.superseded = true
				}
				client.mu.Unlock()
				return "", 0, ErrStaleMap
			}
		}
	}
	return address, generation, nil
}

func (client *Client) supersedeCreation(osdID int32) {
	client.mu.Lock()
	if pending := client.creations[osdID]; pending != nil {
		pending.superseded = true
	}
	client.mu.Unlock()
}

func (client *Client) getSessionContext(ctx context.Context, osdID int32, addresses protocol.EntityAddrVec) (session, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		address, generation, err := client.sessionTarget(osdID, addresses)
		if err != nil {
			return nil, err
		}
		client.mu.Lock()
		if client.closed {
			client.mu.Unlock()
			return nil, ErrClosed
		}
		if pending := client.creations[osdID]; pending != nil {
			matched := pending.address == address && pending.generation == generation
			if !matched {
				pending.superseded = true
			}
			client.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-client.done:
				return nil, ErrClosed
			case <-pending.done:
			}
			if !matched {
				continue
			}
			return client.creationResult(ctx, osdID, addresses, pending)
		}
		if existing, ok := client.sessions[osdID]; ok && existing.address == address && existing.generation == generation {
			client.mu.Unlock()
			return existing.session, nil
		}
		pending := &sessionCreation{address: address, generation: generation, done: make(chan struct{})}
		if client.creations == nil {
			client.creations = make(map[int32]*sessionCreation)
		}
		client.creations[osdID] = pending
		old := client.sessions[osdID].session
		delete(client.sessions, osdID)
		var interrupted []*Watch
		if old != nil {
			interrupted = client.watchesForPrimaryLocked(osdID)
		}
		client.workers.Add(1)
		client.mu.Unlock()
		go client.createSession(osdID, cloneAddresses(addresses), pending, old, interrupted)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-client.done:
			return nil, ErrClosed
		case <-pending.done:
			return client.creationResult(ctx, osdID, addresses, pending)
		}
	}
}

func (client *Client) creationResult(ctx context.Context, osdID int32, addresses protocol.EntityAddrVec, pending *sessionCreation) (session, error) {
	client.mu.Lock()
	closed := client.closed
	client.mu.Unlock()
	if closed {
		return nil, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if pending.err != nil {
		return nil, pending.err
	}
	address, generation, err := client.sessionTarget(osdID, addresses)
	if err != nil || address != pending.address || generation != pending.generation {
		return nil, ErrStaleMap
	}
	return pending.result, nil
}

func (client *Client) createSession(osdID int32, addresses protocol.EntityAddrVec, pending *sessionCreation, old session, interrupted []*Watch) {
	defer client.workers.Done()
	if old != nil {
		old.Stop()
	}
	client.mu.Lock()
	closed := client.closed
	client.mu.Unlock()
	var created session
	var err error
	if closed {
		err = ErrClosed
	} else {
		created, err = client.config.SessionFactory(osdID, addresses)
	}
	address, generation, targetErr := client.sessionTarget(osdID, addresses)
	client.mu.Lock()
	if client.closed {
		err = ErrClosed
	} else if pending.superseded || targetErr != nil || address != pending.address || generation != pending.generation {
		err = ErrStaleMap
	}
	if err == nil && created == nil {
		err = msgr.ErrSessionClosed
	}
	if err == nil {
		client.sessions[osdID] = sessionEntry{address: address, generation: generation, session: created}
	}
	client.mu.Unlock()
	if err != nil && created != nil {
		created.Stop()
	}
	if err == nil && client.config.ObserveSession != nil {
		client.config.ObserveSession(OSDSessionEvent{OSDID: osdID, Available: true})
	}
	for _, watch := range interrupted {
		watch.interrupt(msgr.ErrSessionClosed)
	}
	client.mu.Lock()
	if client.closed && err == nil {
		err = ErrClosed
	}
	if err == nil {
		pending.result = created
		if notifications, ok := created.(notificationSession); ok {
			client.workers.Add(1)
			go func() {
				defer client.workers.Done()
				client.dispatchNotifications(osdID, created, notifications)
			}()
		}
	}
	pending.err = err
	delete(client.creations, osdID)
	close(pending.done)
	client.mu.Unlock()
}

func (client *Client) invalidate(osdID int32, failed session) {
	client.mu.Lock()
	if pending := client.creations[osdID]; pending != nil {
		client.mu.Unlock()
		<-pending.done
		client.invalidate(osdID, failed)
		return
	}
	var interrupted []*Watch
	removed := false
	if existing, ok := client.sessions[osdID]; ok && existing.session == failed {
		delete(client.sessions, osdID)
		interrupted = client.watchesForPrimaryLocked(osdID)
		removed = true
		client.workers.Add(1)
	}
	client.mu.Unlock()
	if !removed {
		return
	}
	defer client.workers.Done()
	if client.config.ObserveSession != nil {
		client.config.ObserveSession(OSDSessionEvent{OSDID: osdID, Err: msgr.ErrSessionDisconnected})
	}
	failed.Stop()
	for _, watch := range interrupted {
		watch.interrupt(msgr.ErrSessionClosed)
	}
}

func (client *Client) watchesForPrimaryLocked(osdID int32) []*Watch {
	watches := make([]*Watch, 0)
	for _, watch := range client.watches {
		if watch.primaryOSD() == osdID {
			watches = append(watches, watch)
		}
	}
	return watches
}

func (client *Client) beginRoutedAttempt(ctx context.Context, route Route, resolve func() (Route, error)) (context.Context, func() bool, error) {
	client.mu.Lock()
	if client.closed {
		client.mu.Unlock()
		return nil, nil, ErrClosed
	}
	if client.routeAttempts == nil {
		client.mu.Unlock()
		return ctx, func() bool { return false }, nil
	}
	if len(client.routeAttempts) >= maxTrackedRouteAttempts {
		client.mu.Unlock()
		return nil, nil, msgr.ErrQueueSaturated
	}
	attemptCtx, cancel := context.WithCancel(ctx)
	client.nextRouteAttempt++
	for client.nextRouteAttempt == 0 || client.routeAttempts[client.nextRouteAttempt] != nil {
		client.nextRouteAttempt++
	}
	id := client.nextRouteAttempt
	attempt := &routedAttempt{route: route, resolve: resolve, cancel: cancel}
	client.routeAttempts[id] = attempt
	client.mu.Unlock()
	if current := client.config.Maps.OSDMap(); current != nil && current.Epoch() > route.Epoch {
		updated, err := resolve()
		if err != nil || !sameRelevantRoute(route, updated) {
			client.remapAttempt(id, attempt)
		}
	}
	return attemptCtx, func() bool {
		client.mu.Lock()
		if client.routeAttempts[id] == attempt {
			delete(client.routeAttempts, id)
		}
		remapped := attempt.remapped
		client.mu.Unlock()
		cancel()
		return remapped
	}, nil
}

func (client *Client) watchOSDMaps(ctx context.Context, waiter osdMapWaiter) {
	defer client.workers.Done()
	after := uint32(0)
	if current := client.config.Maps.OSDMap(); current != nil {
		after = current.Epoch()
		client.invalidateUnusableSessions(current)
		client.cancelRemappedAttempts()
	}
	for {
		if err := waiter.WaitForOSDMap(ctx, after); err != nil {
			return
		}
		current := client.config.Maps.OSDMap()
		if current == nil || current.Epoch() <= after {
			continue
		}
		after = current.Epoch()
		client.invalidateUnusableSessions(current)
		client.cancelRemappedAttempts()
	}
}

func (client *Client) invalidateUnusableSessions(osdMap *maps.OSDMap) {
	client.mu.Lock()
	sessions := make(map[int32]sessionEntry, len(client.sessions))
	for osdID, entry := range client.sessions {
		sessions[osdID] = entry
	}
	creations := make(map[int32]*sessionCreation, len(client.creations))
	for osdID, pending := range client.creations {
		creations[osdID] = pending
	}
	client.mu.Unlock()
	for osdID, pending := range creations {
		state, stateOK := osdMap.OSDState(osdID)
		addresses, addressesOK := osdMap.OSDClientAddresses(osdID)
		address, addressOK := selectAddress(addresses)
		unusable := stateOK && (!state.Exists || !state.Up || !addressesOK || !addressOK)
		replaced := addressesOK && addressOK && address != pending.address
		if source, ok := client.config.Maps.(osdSessionGenerationSource); ok {
			replaced = replaced || source.OSDSessionGeneration(osdID) != pending.generation
		}
		if unusable || replaced {
			client.mu.Lock()
			if client.creations[osdID] == pending {
				pending.superseded = true
			}
			client.mu.Unlock()
		}
	}
	for osdID, entry := range sessions {
		state, stateOK := osdMap.OSDState(osdID)
		addresses, addressesOK := osdMap.OSDClientAddresses(osdID)
		address, addressOK := selectAddress(addresses)
		unusable := stateOK && (!state.Exists || !state.Up || !addressesOK || !addressOK)
		replaced := addressesOK && addressOK && address != entry.address
		if unusable || replaced {
			client.invalidate(osdID, entry.session)
		}
	}
}

func (client *Client) cancelRemappedAttempts() {
	client.mu.Lock()
	type registeredAttempt struct {
		id      uint64
		attempt *routedAttempt
	}
	attempts := make([]registeredAttempt, 0, len(client.routeAttempts))
	for id, attempt := range client.routeAttempts {
		attempts = append(attempts, registeredAttempt{id: id, attempt: attempt})
	}
	watches := make([]*Watch, 0, len(client.watches))
	for _, watch := range client.watches {
		watches = append(watches, watch)
	}
	client.mu.Unlock()

	for _, registered := range attempts {
		updated, err := registered.attempt.resolve()
		if err == nil && sameRelevantRoute(registered.attempt.route, updated) {
			continue
		}
		client.remapAttempt(registered.id, registered.attempt)
	}
	for _, watch := range watches {
		updated, err := client.config.Router.Route(watch.target)
		if err != nil || !sameRelevantRoute(watch.currentRoute(), updated) {
			watch.interrupt(ErrStaleMap)
		}
	}
}

func (client *Client) remapAttempt(id uint64, attempt *routedAttempt) {
	client.mu.Lock()
	if client.routeAttempts[id] == attempt && !attempt.remapped {
		attempt.remapped = true
		attempt.cancel()
	}
	client.mu.Unlock()
}

func sameRelevantRoute(left, right Route) bool {
	if left.Primary != right.Primary || left.PG != right.PG || left.Shard != right.Shard || left.Sharded != right.Sharded {
		return false
	}
	leftAddress, leftOK := selectAddress(left.Addresses)
	rightAddress, rightOK := selectAddress(right.Addresses)
	return leftOK && rightOK && leftAddress == rightAddress
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
	if client.mutationBuffers != nil {
		client.mutationBuffers.closed.Store(true)
		client.mutationBuffers = nil
	}
	client.closeDone = make(chan struct{})
	closeDone := client.closeDone
	client.mutationClosed = true
	client.notifyMutationWaitersLocked()
	sessions := client.sessions
	client.sessions = nil
	watches := client.watches
	notifies := client.notifies
	mapWatchCancel := client.mapWatchCancel
	routeCancels := make([]context.CancelFunc, 0, len(client.routeAttempts))
	for _, attempt := range client.routeAttempts {
		routeCancels = append(routeCancels, attempt.cancel)
	}
	client.routeAttempts = nil
	client.watches = nil
	client.notifies = nil
	if client.done != nil {
		close(client.done)
	}
	client.mu.Unlock()
	if mapWatchCancel != nil {
		mapWatchCancel()
	}
	for _, cancel := range routeCancels {
		cancel()
	}
	for _, watch := range watches {
		watch.stop(ErrClosed)
	}
	for _, completion := range notifies {
		select {
		case completion <- notifyCompletion{err: errors.Join(msgr.ErrOutcomeUnknown, ErrClosed)}:
		default:
		}
	}
	for _, entry := range sessions {
		entry.session.Stop()
	}
	for _, watch := range watches {
		watch.wait()
	}
	client.workers.Wait()
	close(closeDone)
}

func productionSessionFactory(config Config) SessionFactory {
	return func(osdID int32, addresses protocol.EntityAddrVec) (session, error) {
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
		sessionConfig.DiagnosticService = "osd"
		sessionConfig.DiagnosticServiceID = osdID
		raw, err := msgr.NewSession(nil, connector, sessionConfig)
		if err != nil {
			return nil, err
		}
		return newOSDSessionWithBackoffLimits(raw, config.MessageLimits, config.RefreshWait, config.MaxBackoffs, config.MaxBackoffBytes, func(available bool, err error) {
			if config.ObserveSession != nil {
				config.ObserveSession(OSDSessionEvent{OSDID: osdID, Available: available, Err: err})
			}
		}), nil
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
