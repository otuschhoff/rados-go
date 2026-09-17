package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/otuschhoff/rados-go/internal/p12fuzzcontract"
	"github.com/otuschhoff/rados-go/internal/p12fuzzevidence"
)

func TestPendingRunWritesCompleteTruthfulFailedReport(t *testing.T) {
	root := seedRunnerRoot(t)
	passed, err := runWithClock(context.Background(), root, p12fuzzevidence.ReportPath, p12fuzzcontract.SmokeProfile, p12fuzzcontract.SmokeBudget, true, budgetClock(p12fuzzcontract.SmokeBudget))
	if err != nil || passed {
		t.Fatalf("pending run = passed %v, err %v", passed, err)
	}
	value, _, err := p12fuzzevidence.Read(filepath.Join(root, p12fuzzevidence.ReportPath))
	if err != nil {
		t.Fatal(err)
	}
	if len(value.Targets) != len(p12fuzzcontract.Targets()) || value.Status != "failed" {
		t.Fatalf("pending report is incomplete: %#v", value)
	}
	for _, target := range value.Targets {
		if target.ExitStatus != -1 || target.Pass || target.Output != "not run: P12 fuzz evidence is pending\n" {
			t.Fatalf("pending target is not truthful: %#v", target)
		}
	}
	if err := p12fuzzevidence.Validate(value, root, false); err != nil {
		t.Fatalf("pending report is not semantically valid failed evidence: %v", err)
	}
}

func TestInterruptedRunWritesEveryTargetAndFails(t *testing.T) {
	root := seedRunnerRoot(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	passed, err := runWithClock(ctx, root, p12fuzzevidence.ReportPath, p12fuzzcontract.CertifyingProfile, p12fuzzcontract.CertifyingBudget, false, budgetClock(p12fuzzcontract.CertifyingBudget))
	if err != nil || passed {
		t.Fatalf("interrupted run = passed %v, err %v", passed, err)
	}
	value, _, err := p12fuzzevidence.Read(filepath.Join(root, p12fuzzevidence.ReportPath))
	if err != nil {
		t.Fatal(err)
	}
	if len(value.Targets) != 31 || value.Status != "failed" {
		t.Fatalf("interrupted report is incomplete: targets=%d status=%s", len(value.Targets), value.Status)
	}
	for _, target := range value.Targets {
		if target.ExitStatus != -1 || target.Pass || !strings.Contains(target.Output, "interrupted") {
			t.Fatalf("interrupted target is not truthful: %#v", target)
		}
	}
}

func budgetClock(budget time.Duration) func() time.Time {
	started := time.Date(2026, 9, 16, 10, 0, 0, 0, time.UTC)
	calls := 0
	return func() time.Time {
		value := started.Add(time.Duration(calls) * budget)
		calls++
		return value
	}
}

func seedRunnerRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	path := filepath.Join(root, "go.mod")
	if err := os.WriteFile(path, []byte("module example.invalid/p12-fuzz-test\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return root
}
