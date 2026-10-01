package main

import "context"

type offeredArrivalIndexKey struct{}

func withOfferedArrivalIndex(ctx context.Context, index int) context.Context {
	return context.WithValue(ctx, offeredArrivalIndexKey{}, index)
}
