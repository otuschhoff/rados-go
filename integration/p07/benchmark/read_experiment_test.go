package main

import (
	"context"
	"testing"
)

func TestMatrixExperimentLimits(t *testing.T) {
	values := map[string]string{"P07_MATRIX_OPERATIONS_PER_WORKER": "1024", "P07_MATRIX_SIZE": "4194304", "P07_MATRIX_CONCURRENCY": "128", "P07_MATRIX_WORKLOAD": "mixed"}
	getenv := func(name string) string { return values[name] }
	value, err := parseMatrixExperiment(getenv, false)
	if err != nil || value.operations != 1024 || value.size != 4194304 || value.concurrency != 128 || value.workload != "mixed" {
		t.Fatalf("value=%+v err=%v", value, err)
	}
	if _, err := parseMatrixExperiment(getenv, true); err == nil {
		t.Fatal("diagnostic accepted matrix overrides")
	}
	for name, invalid := range map[string][]string{
		"P07_MATRIX_OPERATIONS_PER_WORKER": {"0", "255", "4097", "+256", " 256", "99999999999999999999"},
		"P07_MATRIX_SIZE":                  {"0", "65537"}, "P07_MATRIX_CONCURRENCY": {"0", "65", "257"}, "P07_MATRIX_WORKLOAD": {"delete", "READ"},
	} {
		previous := values[name]
		for _, text := range invalid {
			values[name] = text
			if _, err := parseMatrixExperiment(getenv, false); err == nil {
				t.Fatalf("invalid %s=%q accepted", name, text)
			}
		}
		values[name] = previous
	}
	value, err = parseMatrixExperiment(func(string) string { return "" }, false)
	if err != nil || value.operations != 2 || value.size != 0 || value.concurrency != 0 || value.workload != "" {
		t.Fatal("default matrix changed")
	}
}

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
