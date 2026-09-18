package objecter

import (
	"context"
	"errors"
	"fmt"

	wire "github.com/otuschhoff/rados-go/internal/encoding"
	"github.com/otuschhoff/rados-go/internal/maps"
	"github.com/otuschhoff/rados-go/internal/msgr"
	"github.com/otuschhoff/rados-go/internal/osd"
	"github.com/otuschhoff/rados-go/internal/protocol"
)

// ErrUnknownOSD is returned when a caller-supplied OSD id has no client
// address in the current map. Mirrors the -ENOENT/-ENXIO map-check outcomes
// used by Objecter::_calc_command_target for absent or downed OSDs.
var ErrUnknownOSD = errors.New("objecter: OSD does not exist in current map")

// CommandResult is the caller-visible outcome of a generic OSD/PG command.
// Result carries the server's signed Linux errno exactly as received. Status
// is the human-readable diagnostic string. Output is the attached data blob.
// All three fields are populated even when Result is negative so the caller
// can preserve the server's diagnostic on error.
type CommandResult struct {
	Result int32
	Status string
	Output []byte
}

// osdCommandRouter is an optional Router extension used by OSDCommand and
// PGCommand to resolve explicit OSD ids and PGs to acting primaries. The
// default map-backed Router implements it; tests may substitute a fake.
type osdCommandRouter interface {
	RouteOSD(int32) (Route, error)
	RoutePG(maps.PG) (Route, error)
}

// OSDCommand submits an MCommand to the given OSD id, encoding the caller's
// argv and input blob per src/messages/MCommand.h at 7f793731. The reply is
// decoded and returned along with the server's Result, Status, and Output.
// Retries follow the same map-refresh semantics as read paths: on -EAGAIN the
// current OSD map is refreshed and the request is resent until its deadline.
func (client *Client) OSDCommand(ctx context.Context, osdID int32, command []string, input []byte) (CommandResult, error) {
	if osdID < 0 {
		return CommandResult{}, fmt.Errorf("%w: negative OSD id", wire.ErrMalformed)
	}
	if len(command) == 0 {
		return CommandResult{}, fmt.Errorf("%w: empty command", wire.ErrMalformed)
	}
	router, ok := client.config.Router.(osdCommandRouter)
	if !ok {
		router = commandMapRouter{source: client.config.Maps}
	}
	return client.submitCommand(ctx, command, input, func() (Route, error) { return router.RouteOSD(osdID) })
}

// PGCommand submits an MCommand to the acting primary of the supplied PG,
// routed through the current OSD map's placement decision for that pool/seed.
// Semantics match Objecter::pg_command_'s call chain into _calc_command_target
// via _calc_target. Retries follow the same map-refresh semantics as OSDCommand.
func (client *Client) PGCommand(ctx context.Context, pg maps.PG, command []string, input []byte) (CommandResult, error) {
	if len(command) == 0 {
		return CommandResult{}, fmt.Errorf("%w: empty command", wire.ErrMalformed)
	}
	if pg.Preferred == 0 {
		pg.Preferred = -1
	}
	router, ok := client.config.Router.(osdCommandRouter)
	if !ok {
		router = commandMapRouter{source: client.config.Maps}
	}
	return client.submitCommand(ctx, command, input, func() (Route, error) { return router.RoutePG(pg) })
}

// PGCommandString parses text as a canonical "poolID.seed" PG string and
// forwards to PGCommand.
func (client *Client) PGCommandString(ctx context.Context, pgString string, command []string, input []byte) (CommandResult, error) {
	pg, err := osd.ParsePG(pgString)
	if err != nil {
		return CommandResult{}, err
	}
	return client.PGCommand(ctx, pg, command, input)
}

func (client *Client) submitCommand(ctx context.Context, command []string, input []byte, resolve func() (Route, error)) (CommandResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	transactionID, err := client.takeTransactionID()
	if err != nil {
		return CommandResult{}, err
	}
	var fsid maps.FSID
	if osdMap := client.config.Maps.OSDMap(); osdMap != nil {
		fsid = osdMap.FSID()
	}
	var lastErr error
	_, boundedByContext := ctx.Deadline()
	for attempt := 0; boundedByContext || attempt < client.config.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return CommandResult{}, preserveOutcomeUnknown(lastErr, err)
		}
		route, err := client.waitForResolvedRoute(ctx, resolve)
		if err != nil {
			return CommandResult{}, preserveOutcomeUnknown(lastErr, err)
		}
		active, err := client.getSession(route.Primary, route.Addresses)
		if err != nil {
			return CommandResult{}, preserveOutcomeUnknown(lastErr, err)
		}
		request, err := osd.EncodeCommandRequest(osd.CommandRequest{
			FSID:          fsid,
			TransactionID: transactionID,
			Command:       command,
			Input:         input,
		}, client.config.MessageLimits.MaxBytes)
		if err != nil {
			return CommandResult{}, preserveOutcomeUnknown(lastErr, err)
		}
		reply, err := active.Submit(ctx, request)
		if err != nil {
			if errors.Is(err, msgr.ErrQueueSaturated) {
				return CommandResult{}, preserveOutcomeUnknown(lastErr, err)
			}
			if errors.Is(err, msgr.ErrOutcomeUnknown) {
				return CommandResult{}, err
			}
			if ctx.Err() != nil {
				return CommandResult{}, preserveOutcomeUnknown(lastErr, ctx.Err())
			}
			lastErr = preserveOutcomeUnknown(lastErr, err)
			client.invalidate(route.Primary, active)
			client.refresh(ctx, route.Epoch)
			continue
		}
		decoded, err := osd.DecodeCommandReply(reply, client.config.MessageLimits.MaxBytes)
		if err != nil {
			client.invalidate(route.Primary, active)
			return CommandResult{}, err
		}
		if decoded.TransactionID != transactionID {
			client.invalidate(route.Primary, active)
			return CommandResult{}, fmt.Errorf("%w: reply tid %d expected %d", osd.ErrMalformedCommandReply, decoded.TransactionID, transactionID)
		}
		result := CommandResult{Result: decoded.Result, Status: decoded.Status, Output: decoded.Output}
		if decoded.Result == -11 {
			lastErr = protocol.WireErrno(decoded.Result)
			client.refresh(ctx, route.Epoch)
			continue
		}
		if decoded.Result < 0 {
			return result, protocol.WireErrno(decoded.Result)
		}
		return result, nil
	}
	if lastErr == nil {
		lastErr = ErrRecovery
	}
	return CommandResult{}, fmt.Errorf("%w: %w", ErrRecovery, lastErr)
}

// commandMapRouter routes commands via the current OSD map. It is used when
// the caller-supplied Router does not implement osdCommandRouter.
type commandMapRouter struct{ source MapSource }

func (router commandMapRouter) RouteOSD(osdID int32) (Route, error) {
	osdMap := router.source.OSDMap()
	if osdMap == nil {
		return Route{}, ErrNoPrimary
	}
	addresses, ok := osdMap.OSDClientAddresses(osdID)
	if !ok || len(addresses) == 0 {
		return Route{}, fmt.Errorf("%w: osd.%d", ErrUnknownOSD, osdID)
	}
	return Route{Epoch: osdMap.Epoch(), Primary: osdID, Addresses: addresses}, nil
}

func (router commandMapRouter) RoutePG(pg maps.PG) (Route, error) {
	osdMap := router.source.OSDMap()
	if osdMap == nil {
		return Route{}, ErrNoPrimary
	}
	if _, ok := osdMap.PoolByID(int64(pg.Pool)); !ok {
		return Route{}, protocol.WireErrno(-2)
	}
	placement, err := osdMap.PlaceRawHash(int64(pg.Pool), pg.Seed)
	if err != nil {
		return Route{}, err
	}
	addresses, ok := osdMap.OSDClientAddresses(placement.ActingPrimary)
	if !ok || len(addresses) == 0 {
		return Route{}, ErrNoPrimary
	}
	return Route{
		Epoch:     osdMap.Epoch(),
		PG:        placement.PG,
		RawHash:   placement.RawHash,
		Primary:   placement.ActingPrimary,
		Shard:     placement.PrimaryShard,
		Sharded:   placement.Sharded,
		Addresses: addresses,
	}, nil
}

// Route is used by the map-backed Router path elsewhere in this package and
// is deliberately not re-exported. RouteOSD and RoutePG expose it via the
// osdCommandRouter interface implemented by mapRouter and by test fakes.
func (router mapRouter) RouteOSD(osdID int32) (Route, error) {
	return commandMapRouter(router).RouteOSD(osdID)
}

func (router mapRouter) RoutePG(pg maps.PG) (Route, error) {
	return commandMapRouter(router).RoutePG(pg)
}
