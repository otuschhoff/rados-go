//go:build linux

package main

import (
	"context"
	"os"
	"runtime"
	"testing"
	"time"
)

func TestOfferedObservationTraceFixture(t *testing.T) {
	dir := os.Getenv("P07_TRACE_FIXTURE_DIR")
	if dir == "" {
		t.Skip("set P07_TRACE_FIXTURE_DIR to retain actual trace fixture")
	}
	t.Setenv("P07_OFFERED_LOAD", "1")
	t.Setenv("P07_OFFERED_FACTORIAL", "1")
	t.Setenv("P07_OFFERED_OBSERVATION_DIR", dir)
	t.Setenv("P07_OFFERED_OBSERVATION_LABEL", "instrumented")
	ctx, finish, err := beginOfferedObservation(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	err = offeredObservedRead(ctx, 7, func(context.Context) error {
		observation := ctx.Value(offeredObservationKey{}).(*offeredObservation)
		timing := observation.calls[0].timing
		timing.Record("write_end", 19, time.Now())
		runtime.GC()
		for iteration := 0; iteration < 20; iteration++ {
			runtime.Gosched()
		}
		timing.Record("read_frame_end", 19, time.Now())
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := finish(); err != nil {
		t.Fatal(err)
	}
	if err := exportOfferedObservation(ctx); err != nil {
		t.Fatal(err)
	}
}
