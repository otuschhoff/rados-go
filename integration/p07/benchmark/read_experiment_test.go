package main

import (
	"context"
	"testing"
)

func TestReadExperimentLimits(t *testing.T) {
	value, err := parseReadExperiment("1024", "4", "8", "1", true)
	if err != nil || value.operations != 1024 || value.slots != 4 || value.window != 8 || !value.allocationLoad {
		t.Fatalf("value=%+v err=%v", value, err)
	}
	for _, values := range [][4]string{{"0", "", "", ""}, {"4097", "", "", ""}, {"", "0", "", ""}, {"", "9", "", ""}, {"", "", "0", ""}, {"", "", "65", ""}, {"", "", "", "bad"}} {
		if _, err := parseReadExperiment(values[0], values[1], values[2], values[3], true); err == nil {
			t.Fatal("invalid experiment accepted")
		}
	}
	if _, err := parseReadExperiment("1024", "", "", "", false); err == nil {
		t.Fatal("normal mode accepted experimental count")
	}
	value, err = parseReadExperiment("1024", "8", "64", "", true)
	if err != nil || value.slots != 8 || value.window != 64 {
		t.Fatalf("boundary value=%+v err=%v", value, err)
	}
}

func TestReadAdmissionShape(t *testing.T) {
	for _, concurrency := range []int{16, 32, 64} {
		ctx := readAdmissionContext(context.Background(), concurrency, concurrency)
		if ctx.Value(admissionKey{}) != nil {
			t.Fatal("full window installed a gate")
		}
		ctx = readAdmissionContext(context.Background(), 8, concurrency)
		if gate, ok := ctx.Value(admissionKey{}).(chan struct{}); !ok || cap(gate) != 8 {
			t.Fatal("restricted window missing")
		}
	}
}

func TestReadAdmissionCancellationAndRelease(t *testing.T) {
	gate := make(chan struct{}, 1)
	ctx := context.WithValue(context.Background(), admissionKey{}, gate)
	release, err := acquireRead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := acquireRead(canceled); err != context.Canceled {
		t.Fatalf("err=%v", err)
	}
	release()
	if len(gate) != 0 {
		t.Fatal("admission leaked")
	}
}
