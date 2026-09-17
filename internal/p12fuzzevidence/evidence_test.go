package p12fuzzevidence

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/otuschhoff/go-librados/internal/p12fuzzcontract"
)

func TestValidateExactEvidenceAndCertificationBoundary(t *testing.T) {
	root := seedSource(t)
	value := validReport(t, root, p12fuzzcontract.CertifyingProfile)
	if err := Validate(value, root, true); err != nil {
		t.Fatalf("valid certifying evidence rejected: %v", err)
	}
	smoke := validReport(t, root, p12fuzzcontract.SmokeProfile)
	if err := Validate(smoke, root, false); err != nil {
		t.Fatalf("valid smoke evidence rejected: %v", err)
	}
	if err := Validate(smoke, root, true); err == nil || !strings.Contains(err.Error(), "10m") {
		t.Fatalf("smoke evidence certified: %v", err)
	}
}

func TestValidateRejectsInexactEvidence(t *testing.T) {
	tests := map[string]func(*Report){
		"missing target":        func(value *Report) { value.Targets = value.Targets[:len(value.Targets)-1] },
		"extra target":          func(value *Report) { value.Targets = append(value.Targets, value.Targets[0]) },
		"duplicate or order":    func(value *Report) { value.Targets[1] = value.Targets[0] },
		"substituted command":   func(value *Report) { value.Targets[0].Command += " -count=1" },
		"substituted toolchain": func(value *Report) { value.Targets[0].Toolchain = "go1.27.0" },
		"substituted platform":  func(value *Report) { value.Targets[0].Platform = "linux/amd64" },
		"linux host platform":   func(value *Report) { value.Platform = "linux/amd64" },
		"substituted budget":    func(value *Report) { value.Targets[0].Budget = "60s" },
		"output hash":           func(value *Report) { value.Targets[0].OutputSHA256 = strings.Repeat("0", 64) },
		"outside envelope":      func(value *Report) { value.Targets[0].StartedAt = "2026-09-16T09:59:59Z" },
		"zero target duration":  func(value *Report) { value.Targets[0].FinishedAt = value.Targets[0].StartedAt },
		"short target duration": func(value *Report) {
			started, _ := time.Parse(time.RFC3339Nano, value.Targets[0].StartedAt)
			value.Targets[0].FinishedAt = Timestamp(started.Add(p12fuzzcontract.CertifyingBudget - time.Nanosecond))
		},
		"overlapping targets": func(value *Report) { value.Targets[1].StartedAt = value.Targets[0].StartedAt },
		"gap between targets": func(value *Report) {
			started, _ := time.Parse(time.RFC3339Nano, value.Targets[1].StartedAt)
			value.Targets[1].StartedAt = Timestamp(started.Add(time.Nanosecond))
		},
		"short report duration": func(value *Report) {
			finished, _ := time.Parse(time.RFC3339Nano, value.FinishedAt)
			value.StartedAt = Timestamp(finished.Add(-time.Duration(len(value.Targets))*p12fuzzcontract.CertifyingBudget + time.Nanosecond))
		},
		"failed target passed": func(value *Report) { value.Targets[0].ExitStatus = 1 },
		"false report status":  func(value *Report) { value.Status = "failed" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			root := seedSource(t)
			value := validReport(t, root, p12fuzzcontract.CertifyingProfile)
			mutate(&value)
			if err := Validate(value, root, false); err == nil {
				t.Fatal("inexact fuzz evidence accepted")
			}
		})
	}
}

func TestDecodeRejectsUnknownAndTrailingJSON(t *testing.T) {
	root := seedSource(t)
	data, err := json.Marshal(validReport(t, root, p12fuzzcontract.SmokeProfile))
	if err != nil {
		t.Fatal(err)
	}
	unknown := bytes.Replace(data, []byte(`"schema_version":1`), []byte(`"schema_version":1,"unexpected":true`), 1)
	for _, input := range [][]byte{unknown, append(data, []byte(` {}`)...)} {
		if _, err := Decode(bytes.NewReader(input)); err == nil {
			t.Fatal("invalid JSON accepted")
		}
	}
}

func TestSourceArtifactsExcludeOnlyGeneratedEvidence(t *testing.T) {
	root := seedSource(t)
	for _, path := range []string{ReportPath, "docs/p12/qualification-report.json", "docs/p12/human-review.json", "integration/p12/report.json", "docs/p12/release-artifacts/archive", "docs/p12/fuzz-report.json.backup"} {
		writeTestFile(t, root, path, path)
	}
	artifacts, err := SourceArtifacts(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, excluded := range []string{ReportPath, "docs/p12/qualification-report.json", "docs/p12/human-review.json", "integration/p12/report.json", "docs/p12/release-artifacts/archive"} {
		if artifacts[excluded] != "" {
			t.Fatalf("generated evidence %q was hashed", excluded)
		}
	}
	if artifacts["docs/p12/fuzz-report.json.backup"] == "" {
		t.Fatal("similarly named source was excluded")
	}
}

func validReport(t *testing.T, root, profile string) Report {
	t.Helper()
	budget, _ := p12fuzzcontract.Budget(profile)
	platform := runtime.GOOS + "/" + runtime.GOARCH
	artifacts, err := SourceArtifacts(root)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Date(2026, 9, 16, 10, 0, 0, 0, time.UTC)
	finished := started.Add(time.Duration(len(p12fuzzcontract.Targets())) * budget)
	value := Report{SchemaVersion: 1, Status: "passed", Profile: profile, Command: Command + " -profile " + profile, Toolchain: p12fuzzcontract.Toolchain, Platform: platform, StartedAt: Timestamp(started), FinishedAt: Timestamp(finished), Source: Source{Identity: "content-addressed-artifacts", Artifacts: artifacts}}
	for _, target := range p12fuzzcontract.Targets() {
		arguments := p12fuzzcontract.Command(target, budget)
		targetFinished := started.Add(budget)
		value.Targets = append(value.Targets, TargetExecution{Package: target.Package, Name: target.Name, Budget: p12fuzzcontract.BudgetText(budget), Command: "CGO_ENABLED=0 GOTOOLCHAIN=" + p12fuzzcontract.Toolchain + " " + strings.Join(arguments, " "), Toolchain: p12fuzzcontract.Toolchain, Platform: platform, StartedAt: Timestamp(started), FinishedAt: Timestamp(targetFinished), ExitStatus: 0, Pass: true, OutputSHA256: HashOutput("")})
		started = targetFinished
	}
	return value
}

func seedSource(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeTestFile(t, root, "go.mod", "module example.invalid/test\n")
	return root
}

func writeTestFile(t *testing.T, root, path, content string) {
	t.Helper()
	fullPath := filepath.Join(root, filepath.FromSlash(path))
	if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fullPath, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}
