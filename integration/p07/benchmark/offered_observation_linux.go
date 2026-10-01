//go:build linux

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/trace"
	"strconv"
	"sync"
	"time"

	"github.com/otuschhoff/rados-go/internal/msgr"
)

type offeredObservationKey struct{}

type offeredObservedCall struct {
	Index   int                    `json:"index"`
	BeginNS int64                  `json:"begin_ns"`
	EndNS   int64                  `json:"end_ns"`
	Error   string                 `json:"error,omitempty"`
	Events  []offeredObservedEvent `json:"events"`
	timing  *msgr.RequestTiming
}

type offeredObservedEvent struct {
	Stage         string `json:"stage"`
	TransactionID uint64 `json:"transaction_id"`
	ElapsedNS     int64  `json:"elapsed_ns"`
}

type offeredObservation struct {
	mu                sync.Mutex
	start             time.Time
	dir, label        string
	active            int
	stopped, exported bool
	stopErr           error
	output            *traceOutput
	file              *os.File
	calls             []*offeredObservedCall
	indices           map[int]bool
	syncPoints        []offeredObservationSync
}

type offeredObservationSync struct {
	ID       int   `json:"id"`
	BeforeNS int64 `json:"before_ns"`
	AfterNS  int64 `json:"after_ns"`
}

func validateOfferedObservationConfig(env func(string) string) error {
	dir := env("P07_OFFERED_OBSERVATION_DIR")
	label := env("P07_OFFERED_OBSERVATION_LABEL")
	if dir == "" {
		if label != "" {
			return fmt.Errorf("observation label requires observation directory")
		}
		return nil
	}
	if env("P07_OFFERED_LOAD") != "1" || env("P07_OFFERED_FACTORIAL") != "1" || label != "instrumented" {
		return fmt.Errorf("observation requires factorial offered load and explicit instrumented label; never primary")
	}
	if !filepath.IsAbs(dir) || filepath.Clean(dir) != dir {
		return fmt.Errorf("observation directory must be a fresh clean absolute path")
	}
	return nil
}

func beginOfferedObservation(ctx context.Context) (context.Context, func() error, error) {
	if err := validateOfferedObservationConfig(os.Getenv); err != nil {
		return ctx, nil, err
	}
	dir := os.Getenv("P07_OFFERED_OBSERVATION_DIR")
	label := os.Getenv("P07_OFFERED_OBSERVATION_LABEL")
	if dir == "" {
		return ctx, func() error { return nil }, nil
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		return ctx, nil, fmt.Errorf("create fresh observation directory: %w", err)
	}
	file, err := os.OpenFile(filepath.Join(dir, "trace.out"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return ctx, nil, err
	}
	observation := &offeredObservation{dir: dir, label: label, file: file, start: time.Now(), indices: make(map[int]bool), calls: make([]*offeredObservedCall, 0)}
	observation.output = &traceOutput{writer: file}
	if err := trace.Start(observation.output); err != nil {
		return ctx, nil, errors.Join(err, file.Close())
	}
	ctx = context.WithValue(ctx, offeredObservationKey{}, observation)
	observation.syncLog(ctx)
	finish := func() error {
		observation.mu.Lock()
		defer observation.mu.Unlock()
		if observation.stopped {
			return observation.stopErr
		}
		if observation.active != 0 {
			return fmt.Errorf("join offered readers before stopping observation")
		}
		observation.syncLog(ctx)
		trace.Stop()
		observation.stopErr = errors.Join(observation.output.Err(), file.Close())
		observation.stopped = true
		return observation.stopErr
	}
	return ctx, finish, nil
}

func (observation *offeredObservation) syncLog(ctx context.Context) {
	now := time.Now()
	point := offeredObservationSync{ID: len(observation.syncPoints), BeforeNS: now.Sub(observation.start).Nanoseconds()}
	trace.Log(ctx, "p07/sync", strconv.Itoa(point.ID))
	point.AfterNS = time.Since(observation.start).Nanoseconds()
	observation.syncPoints = append(observation.syncPoints, point)
}

func offeredObservedRead(ctx context.Context, index int, read func(context.Context) error) error {
	observation, _ := ctx.Value(offeredObservationKey{}).(*offeredObservation)
	if observation == nil {
		return read(ctx)
	}
	if index < 0 {
		return fmt.Errorf("observation requires a nonnegative arrival index")
	}
	observation.mu.Lock()
	if observation.stopped || observation.indices[index] {
		observation.mu.Unlock()
		return fmt.Errorf("observation stopped or duplicate arrival index %d", index)
	}
	observation.indices[index] = true
	observation.active++
	call := &offeredObservedCall{Index: index, BeginNS: time.Since(observation.start).Nanoseconds()}
	observation.calls = append(observation.calls, call)
	observation.mu.Unlock()
	taskCtx, task := trace.NewTask(ctx, "p07/read")
	trace.Log(taskCtx, "p07/index", strconv.Itoa(index))
	timingCtx, timing := msgr.WithRequestTiming(taskCtx)
	call.timing = timing
	defer func() {
		timing.Record("read_return", 0, time.Now())
		task.End()
		observation.mu.Lock()
		call.EndNS = time.Since(observation.start).Nanoseconds()
		observation.active--
		observation.mu.Unlock()
	}()
	err := read(timingCtx)
	if err != nil {
		call.Error = err.Error()
	}
	return err
}

func wrapOfferedObservedRead(read func(context.Context, int) error) func(context.Context, int) error {
	return func(ctx context.Context, worker int) error {
		index, ok := ctx.Value(offeredArrivalIndexKey{}).(int)
		if !ok {
			return read(ctx, worker)
		}
		return offeredObservedRead(ctx, index, func(readCtx context.Context) error { return read(readCtx, worker) })
	}
}

func exportOfferedObservation(ctx context.Context) error {
	observation, _ := ctx.Value(offeredObservationKey{}).(*offeredObservation)
	if observation == nil {
		return nil
	}
	observation.mu.Lock()
	defer observation.mu.Unlock()
	if !observation.stopped || observation.active != 0 {
		return fmt.Errorf("stop observation after joins before export")
	}
	if observation.exported {
		return fmt.Errorf("observation already exported")
	}
	for _, call := range observation.calls {
		for _, event := range call.timing.Events() {
			call.Events = append(call.Events, offeredObservedEvent{Stage: event.Stage, TransactionID: event.TransactionID, ElapsedNS: event.At.Sub(observation.start).Nanoseconds()})
		}
	}
	file, err := os.OpenFile(filepath.Join(observation.dir, "timing.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	observation.exported = true
	data := struct {
		Schema    int                      `json:"schema"`
		Label     string                   `json:"label"`
		GoVersion string                   `json:"go_version"`
		StartWall string                   `json:"start_wall"`
		Sync      []offeredObservationSync `json:"sync"`
		Calls     []*offeredObservedCall   `json:"calls"`
	}{1, observation.label, runtime.Version(), observation.start.UTC().Format(time.RFC3339Nano), observation.syncPoints, observation.calls}
	return errors.Join(observation.stopErr, json.NewEncoder(file).Encode(data), file.Close())
}
