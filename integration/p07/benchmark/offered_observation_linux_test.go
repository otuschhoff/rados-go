//go:build linux

package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestOfferedObservationLifecycle(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "observation")
	t.Setenv("P07_OFFERED_LOAD", "1")
	t.Setenv("P07_OFFERED_FACTORIAL", "1")
	t.Setenv("P07_OFFERED_OBSERVATION_DIR", dir)
	t.Setenv("P07_OFFERED_OBSERVATION_LABEL", "instrumented")
	ctx, finish, err := beginOfferedObservation(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	if err := exportOfferedObservation(ctx); err == nil {
		t.Fatal("export before stop accepted")
	}
	read := wrapOfferedObservedRead(func(context.Context, int) error { return nil })
	if err := read(ctx, 0); err != nil {
		t.Fatal(err)
	}
	if err := read(withOfferedArrivalIndex(ctx, 42), 0); err != nil {
		t.Fatal(err)
	}
	if err := read(withOfferedArrivalIndex(ctx, 42), 1); err == nil {
		t.Fatal("duplicate accepted")
	}
	if err := finish(); err != nil {
		t.Fatal(err)
	}
	if err := finish(); err != nil {
		t.Fatal(err)
	}
	if err := exportOfferedObservation(ctx); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "timing.json"))
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		Calls []offeredObservedCall `json:"calls"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Calls) != 1 || result.Calls[0].Index != 42 || len(result.Calls[0].Events) != 2 {
		t.Fatalf("bad export: %s", data)
	}
	if _, _, err := beginOfferedObservation(context.Background()); err == nil {
		t.Fatal("existing directory accepted")
	}
}

func TestOfferedObservationRejectsPrimary(t *testing.T) {
	t.Setenv("P07_OFFERED_OBSERVATION_DIR", filepath.Join(t.TempDir(), "observation"))
	t.Setenv("P07_OFFERED_OBSERVATION_LABEL", "primary")
	if _, _, err := beginOfferedObservation(context.Background()); err == nil {
		t.Fatal("primary accepted")
	}
}

func TestOfferedObservationActiveJoin(t *testing.T) {
	t.Setenv("P07_OFFERED_LOAD", "1")
	t.Setenv("P07_OFFERED_FACTORIAL", "1")
	t.Setenv("P07_OFFERED_OBSERVATION_DIR", filepath.Join(t.TempDir(), "observation"))
	t.Setenv("P07_OFFERED_OBSERVATION_LABEL", "instrumented")
	ctx, finish, err := beginOfferedObservation(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	entered, release, joined := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		joined <- offeredObservedRead(ctx, 3, func(context.Context) error { close(entered); <-release; return context.Canceled })
	}()
	<-entered
	stopErr := finish()
	close(release)
	if err := <-joined; err != context.Canceled {
		t.Fatal(err)
	}
	if stopErr == nil {
		t.Fatal("stop accepted active reader")
	}
	if err := finish(); err != nil {
		t.Fatal(err)
	}
	if err := offeredObservedRead(ctx, 4, func(context.Context) error { t.Fatal("read after stop"); return nil }); err == nil {
		t.Fatal("stopped read accepted")
	}
	if err := exportOfferedObservation(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestOfferedObservationConfig(t *testing.T) {
	valid := map[string]string{"P07_OFFERED_LOAD": "1", "P07_OFFERED_FACTORIAL": "1", "P07_OFFERED_OBSERVATION_LABEL": "instrumented", "P07_OFFERED_OBSERVATION_DIR": "/tmp/fresh-p07-observation"}
	if err := validateOfferedObservationConfig(func(name string) string { return valid[name] }); err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]string{"P07_OFFERED_LOAD": "", "P07_OFFERED_FACTORIAL": "", "P07_OFFERED_OBSERVATION_LABEL": "primary", "P07_OFFERED_OBSERVATION_DIR": "relative"} {
		if err := validateOfferedObservationConfig(func(key string) string {
			if key == name {
				return value
			}
			return valid[key]
		}); err == nil {
			t.Fatalf("accepted %s", name)
		}
	}
}

func TestOfferedObservationNoAttempts(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "observation")
	t.Setenv("P07_OFFERED_LOAD", "1")
	t.Setenv("P07_OFFERED_FACTORIAL", "1")
	t.Setenv("P07_OFFERED_OBSERVATION_DIR", dir)
	t.Setenv("P07_OFFERED_OBSERVATION_LABEL", "instrumented")
	ctx, finish, err := beginOfferedObservation(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	if err := finish(); err != nil {
		t.Fatal(err)
	}
	if err := exportOfferedObservation(ctx); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "timing.json"))
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		Calls []offeredObservedCall `json:"calls"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	if result.Calls == nil || len(result.Calls) != 0 {
		t.Fatal("empty attempts must export an array")
	}
}
