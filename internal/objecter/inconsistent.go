package objecter

import (
	"context"
	"errors"

	wire "github.com/otuschhoff/go-librados/internal/encoding"
	"github.com/otuschhoff/go-librados/internal/maps"
	"github.com/otuschhoff/go-librados/internal/osd"
	"github.com/otuschhoff/go-librados/internal/protocol"
)

func (client *Client) ListInconsistentObjects(ctx context.Context, pg maps.PG) ([]osd.InconsistentObject, error) {
	const pageSize = uint64(1024)
	maximum := client.MaxEnumerationEntries()
	if maximum == 0 {
		return nil, wire.ErrLimitExceeded
	}
	var result []osd.InconsistentObject
	var start osd.InconsistentObject
	var interval uint32
	for {
		operation, err := osd.EncodeScrubList(interval, start, pageSize, client.config.MessageLimits.MaxBytes)
		if err != nil {
			return nil, err
		}
		target := Target{PoolID: int64(pg.Pool), Snapshot: osd.NoSnap}
		router, ok := client.config.Router.(osdCommandRouter)
		if !ok {
			router = commandMapRouter{source: client.config.Maps}
		}
		reply, err := client.executeRoutedOperations(ctx, target, []osd.Operation{operation}, 0, false, false, osd.FlagPGOp, func(Target) (Route, error) {
			return router.RoutePG(pg)
		})
		if err != nil {
			return nil, err
		}
		if len(reply.Operations) != 1 {
			return nil, wire.ErrMalformed
		}
		if reply.Operations[0].Code < 0 {
			return nil, protocol.WireErrno(reply.Operations[0].Code)
		}
		nextInterval, page, err := osd.DecodeScrubList(reply.Operations[0].Data, client.config.MessageLimits.MaxBytes, uint32(min(maximum, uint64(^uint32(0)))))
		if err != nil {
			return nil, err
		}
		if interval != 0 && interval != nextInterval {
			return nil, protocol.WireErrno(-11)
		}
		interval = nextInterval
		if uint64(len(result))+uint64(len(page)) > maximum {
			return nil, wire.ErrLimitExceeded
		}
		result = append(result, page...)
		if len(page) < int(pageSize) {
			return result, nil
		}
		next := page[len(page)-1]
		if next.Object == start.Object && next.Namespace == start.Namespace && next.Locator == start.Locator && next.Snapshot == start.Snapshot {
			return nil, errors.New("scrub listing did not advance")
		}
		start = next
	}
}
