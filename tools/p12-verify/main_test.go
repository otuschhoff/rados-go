package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/otuschhoff/rados-go/internal/p12qualcontract"
)

func TestDecodeReportRejectsMalformedUnknownAndTrailingJSON(t *testing.T) {
	data := readFixture(t)
	tests := map[string][]byte{
		"malformed": []byte(`{"schema_version":`),
		"unknown":   bytes.Replace(data, []byte(`"schema_version": 2,`), []byte(`"schema_version": 2, "unexpected": true,`), 1),
		"trailing":  append(append([]byte(nil), data...), []byte(` {}`)...),
	}
	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeReport(bytes.NewReader(input)); err == nil {
				t.Fatal("invalid JSON was accepted")
			}
		})
	}
}

func TestValidateQualificationFileRejectsUnknownFailedAndStaleEvidence(t *testing.T) {
	root := t.TempDir()
	value := validQualificationReport(t, root)
	path := filepath.Join(t.TempDir(), "qualification-report.json")
	write := func(data []byte) {
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	write(bytes.Replace(data, []byte(`"schema_version":1`), []byte(`"schema_version":1,"unexpected":true`), 1))
	if err := validateQualificationFile(path, root); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("qualification with unknown field was accepted: %v", err)
	}

	value.Status = "failed"
	data, err = json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	write(data)
	if err := validateQualificationFile(path, root); err == nil || !strings.Contains(err.Error(), "not passed") {
		t.Fatalf("failed qualification was accepted: %v", err)
	}

	value.Status = "passed"
	for sourcePath := range value.Source.Artifacts {
		value.Source.Artifacts[sourcePath] = strings.Repeat("0", 64)
		break
	}
	data, err = json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	write(data)
	if err := validateQualificationFile(path, root); err == nil || !strings.Contains(err.Error(), "hash mismatch") {
		t.Fatalf("stale qualification was accepted: %v", err)
	}
}

func TestQuickReportRejectedByDefaultAndAllowedExplicitly(t *testing.T) {
	value := loadFixture(t)
	if certified, err := validateReport(value, "../..", false); err == nil || certified || !strings.Contains(err.Error(), "non-certifying") {
		t.Fatalf("quick report was not rejected by default: certified=%v err=%v", certified, err)
	}
	if certified, err := validateReport(value, "../..", true); err != nil || certified {
		t.Fatalf("quick evidence was not validated without certification: certified=%v err=%v", certified, err)
	}
}

func TestQualificationEvidenceRequiredAndCurrent(t *testing.T) {
	value := loadFixture(t)
	value.Status = "candidate"
	root := t.TempDir()
	if err := validateQualification(value, root); err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("missing qualification evidence accepted: %v", err)
	}

	path := filepath.Join(root, "docs/p12/qualification-report.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	nested := validQualificationReport(t, root)
	data, err := json.MarshalIndent(nested, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	value.Qualification = &qualificationBinding{Path: "docs/p12/qualification-report.json", Status: "passed", SHA256: hex.EncodeToString(digest[:])}
	if err := validateQualification(value, root); err != nil {
		t.Fatalf("current qualification evidence rejected: %v", err)
	}

	value.Qualification.SHA256 = strings.Repeat("0", 64)
	if err := validateQualification(value, root); err == nil || !strings.Contains(err.Error(), "hash mismatch") {
		t.Fatalf("mismatched qualification hash accepted: %v", err)
	}
}

func TestQualificationRejectsSkippedAndStaleSourceEvidence(t *testing.T) {
	root := t.TempDir()
	for _, path := range []string{".github/workflows/p00.yml", ".gitignore", releaseArtifactsPath + "/artifact", releaseArtifactsPath + "-copy/artifact"} {
		fullPath := filepath.Join(root, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(fullPath, []byte(path), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	value := validQualificationReport(t, root)
	if _, included := value.Source.Artifacts[releaseArtifactsPath+"/artifact"]; included {
		t.Fatal("retained release artifact entered qualification source hashing")
	}
	for _, path := range []string{".github/workflows/p00.yml", ".gitignore", releaseArtifactsPath + "-copy/artifact"} {
		if value.Source.Artifacts[path] == "" {
			t.Fatalf("qualification verifier excluded source %q", path)
		}
	}

	value.Checks[0].Pass = false
	if err := validateQualificationReport(value, root); err == nil || !strings.Contains(err.Error(), "skipped") {
		t.Fatalf("skipped qualification check accepted: %v", err)
	}
	value.Checks[0].Pass = true

	for path := range value.Source.Artifacts {
		value.Source.Artifacts[path] = strings.Repeat("0", 64)
		break
	}
	if err := validateQualificationReport(value, root); err == nil || !strings.Contains(err.Error(), "hash mismatch") {
		t.Fatalf("stale qualification source hash accepted: %v", err)
	}
}

func TestCandidateSourceSetExcludesOnlyGeneratedOutputs(t *testing.T) {
	root := t.TempDir()
	for _, directory := range []string{"internal", "examples", "tools", "integration/p12", "docs/p12/release-artifacts", "docs/p12/release-artifacts-copy"} {
		if err := os.MkdirAll(filepath.Join(root, filepath.FromSlash(directory)), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, path := range []string{"docs/p12/release-artifacts/generated.txt", "docs/p12/release-artifacts-copy/retained.txt", "docs/p12/fuzz-report.json", "docs/p12/human-review.json", "docs/p12/reviewer-trust.json", "integration/p12/reviewer-trust.schema.json"} {
		if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(path)), []byte(path), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	paths, err := expectedSourceArtifacts(root)
	if err != nil {
		t.Fatal(err)
	}
	foundLookalike := false
	foundRequired := map[string]bool{".github/workflows/p00.yml": false, ".gitignore": false, "docs/p12/reviewer-trust.json": false, "integration/p12/reviewer-trust.schema.json": false}
	for _, path := range paths {
		if path == releaseArtifactsPath || strings.HasPrefix(path, releaseArtifactsPath+"/") {
			t.Fatalf("retained release artifact entered final source hashing: %s", path)
		}
		if path == "docs/p12/release-artifacts-copy/retained.txt" {
			foundLookalike = true
		}
		if path == humanReviewPath {
			t.Fatal("detached generated human review entered candidate source hashing")
		}
		if path == "docs/p12/fuzz-report.json" {
			t.Fatal("generated fuzz report entered candidate source hashing")
		}
		if _, required := foundRequired[path]; required {
			foundRequired[path] = true
		}
	}
	if !foundLookalike {
		t.Fatal("similarly named release artifact directory was excluded")
	}
	for path, found := range foundRequired {
		if !found {
			t.Fatalf("final source set excluded %q", path)
		}
	}
}

func TestQualificationRejectsInexactExecutionEvidence(t *testing.T) {
	tests := map[string]func(*qualificationReport){
		"substituted command":   func(value *qualificationReport) { value.Checks[0].Command += " -count=1" },
		"substituted toolchain": func(value *qualificationReport) { value.RuntimeMatrix[0].Toolchain = "go1.99.0" },
		"substituted platform":  func(value *qualificationReport) { value.RuntimeMatrix[0].Platform = "linux/amd64" },
		"mismatched output hash": func(value *qualificationReport) {
			value.Checks[0].OutputSHA256 = strings.Repeat("0", 64)
		},
		"malformed timestamp": func(value *qualificationReport) { value.Checks[0].StartedAt = "not-a-timestamp" },
		"zero duration":       func(value *qualificationReport) { value.Checks[0].FinishedAt = value.Checks[0].StartedAt },
		"negative duration": func(value *qualificationReport) {
			value.Checks[0].StartedAt, value.Checks[0].FinishedAt = value.Checks[0].FinishedAt, value.Checks[0].StartedAt
		},
		"outside report envelope": func(value *qualificationReport) { value.Checks[0].StartedAt = "2026-09-16T09:59:59Z" },
		"duplicate check":         func(value *qualificationReport) { value.Checks[len(value.Checks)-1] = value.Checks[0] },
		"missing check":           func(value *qualificationReport) { value.Checks = value.Checks[:len(value.Checks)-1] },
		"extra check": func(value *qualificationReport) {
			value.Checks = append(value.Checks, validQualificationCheck(p12qualcontract.Spec{ID: "unexpected", Command: "unexpected", Toolchain: "host", Platform: "host"}))
		},
		"duplicate runtime": func(value *qualificationReport) {
			value.RuntimeMatrix[len(value.RuntimeMatrix)-1] = value.RuntimeMatrix[0]
		},
		"reordered checks": func(value *qualificationReport) {
			value.Checks[0], value.Checks[1] = value.Checks[1], value.Checks[0]
		},
		"overlapping checks": func(value *qualificationReport) {
			value.Checks[1].StartedAt = value.Checks[0].StartedAt
		},
		"mismatched observed Go version": func(value *qualificationReport) {
			for index := range value.Checks {
				if value.Checks[index].ID == "go-version-latest" {
					value.Checks[index].Output = "go version go1.27.0 darwin/arm64\n"
					digest := sha256.Sum256([]byte(value.Checks[index].Output))
					value.Checks[index].OutputSHA256 = hex.EncodeToString(digest[:])
				}
			}
		},
		"missing observation": func(value *qualificationReport) { value.RuntimeMatrix[0].Observation = nil },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			value := validQualificationReport(t, root)
			mutate(&value)
			if err := validateQualificationReport(value, root); err == nil {
				t.Fatal("inexact qualification evidence was accepted")
			}
		})
	}
}

func validQualificationReport(t *testing.T, root string) qualificationReport {
	t.Helper()
	for phase := 3; phase <= 11; phase++ {
		if phase == 5 {
			path := filepath.Join(root, "testdata/p05/evidence.manifest.json")
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte("p05"), 0o644); err != nil {
				t.Fatal(err)
			}
			continue
		}
		path := filepath.Join(root, fmt.Sprintf("docs/p%02d/integration-report.json", phase))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, fmt.Appendf(nil, "p%02d", phase), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	var value qualificationReport
	value.SchemaVersion = 1
	value.Status = "passed"
	value.Command = "./integration/p12/qualify.sh"
	value.StartedAt = "2026-09-16T10:00:00Z"
	value.FinishedAt = "2026-09-16T10:01:00Z"
	value.Source.Identity = "content-addressed-artifacts"
	artifacts, err := qualificationSourceArtifacts(root)
	if err != nil {
		t.Fatal(err)
	}
	value.Source.Artifacts = artifacts
	for _, spec := range p12qualcontract.Checks(p12qualcontract.DefaultPins(), "v0.1.0") {
		check := validQualificationCheck(spec)
		if spec.ID == "go-version-latest" {
			check.Output = "go version " + p12qualcontract.LatestGo + " darwin/arm64\n"
			digest := sha256.Sum256([]byte(check.Output))
			check.OutputSHA256 = hex.EncodeToString(digest[:])
		}
		value.Checks = append(value.Checks, check)
	}
	for _, spec := range p12qualcontract.Runtimes(p12qualcontract.DefaultPins()) {
		parts := strings.Split(spec.Platform, "/")
		value.RuntimeMatrix = append(value.RuntimeMatrix, qualificationRuntime{qualificationCheck: validQualificationCheck(spec), Observation: &qualificationObservation{GOOS: parts[0], GOARCH: parts[1], ModulePath: p12qualcontract.Module, ModuleVersion: "(devel)"}})
	}
	next := time.Date(2026, 9, 16, 10, 0, 0, 0, time.UTC)
	stamp := func(check *qualificationCheck) {
		check.StartedAt = next.Format(time.RFC3339Nano)
		next = next.Add(time.Second)
		check.FinishedAt = next.Format(time.RFC3339Nano)
	}
	for index := range value.Checks[:len(value.Checks)-10] {
		stamp(&value.Checks[index])
	}
	for index := range value.RuntimeMatrix {
		stamp(&value.RuntimeMatrix[index].qualificationCheck)
	}
	for index := len(value.Checks) - 10; index < len(value.Checks); index++ {
		stamp(&value.Checks[index])
	}
	for phase := 3; phase <= 11; phase++ {
		name := fmt.Sprintf("p%02d", phase)
		priorArtifacts, err := expectedPriorArtifacts(root, name)
		if err != nil {
			t.Fatal(err)
		}
		value.PriorReports = append(value.PriorReports, qualificationPrior{Phase: name, VerifierCommand: "CGO_ENABLED=0 GOTOOLCHAIN=" + p12qualcontract.LatestGo + " go run ./tools/" + name + "-verify", CheckID: "verify-" + name, Artifacts: priorArtifacts})
	}
	release := map[string]string{"a": strings.Repeat("1", 64), "b": strings.Repeat("2", 64), "c": strings.Repeat("3", 64), "d": strings.Repeat("4", 64)}
	value.Release = qualificationRelease{Version: "v0.1.0", Reproducible: true, Runs: []map[string]string{cloneMap(release), cloneMap(release)}, Artifacts: release}
	return value
}

func validQualificationCheck(spec p12qualcontract.Spec) qualificationCheck {
	digest := sha256.Sum256(nil)
	return qualificationCheck{ID: spec.ID, Command: spec.Command, Toolchain: spec.Toolchain, Platform: spec.Platform, StartedAt: "2026-09-16T10:00:00Z", FinishedAt: "2026-09-16T10:00:01Z", ExitStatus: 0, Pass: true, OutputSHA256: hex.EncodeToString(digest[:])}
}

func TestReleaseEvidenceDistinguishesQuickAndCertifyingReports(t *testing.T) {
	quick := loadFixture(t)
	if err := validateRelease(quick, t.TempDir()); err != nil {
		t.Fatalf("quick release evidence rejected: %v", err)
	}
	version := "v1.2.3"
	root := seedRetainedRelease(t, version)
	artifactPath := releaseArtifactsPath
	quick.Status = "candidate"
	quick.Release = releaseEvidence{Performed: true, Version: &version, Path: &artifactPath, Reproducible: true, Artifacts: hashRetainedRelease(t, root)}
	if err := validateRelease(quick, root); err != nil {
		t.Fatalf("certifying release evidence rejected: %v", err)
	}
	quick.Release.Reproducible = false
	if err := validateRelease(quick, root); err == nil {
		t.Fatal("non-reproducible release evidence accepted")
	}
	placeholder := placeholderReleaseVersion
	quick.Release.Reproducible = true
	quick.Release.Version = &placeholder
	if err := validateRelease(quick, root); err == nil {
		t.Fatal("placeholder release version accepted for a candidate")
	}
}

func TestReleaseEvidenceRejectsChangedRetainedBytesAndExtraFiles(t *testing.T) {
	version := "v1.2.3"
	root := seedRetainedRelease(t, version)
	artifactPath := releaseArtifactsPath
	value := report{Status: "candidate", Release: releaseEvidence{Performed: true, Version: &version, Path: &artifactPath, Reproducible: true, Artifacts: hashRetainedRelease(t, root)}}
	archive := filepath.Join(root, filepath.FromSlash(releaseArtifactsPath), "rados-go-v1.2.3.tar.gz")
	if err := os.WriteFile(archive, []byte("changed"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := validateRelease(value, root); err == nil || !strings.Contains(err.Error(), "hash mismatch") {
		t.Fatalf("changed retained bytes accepted: %v", err)
	}
	root = seedRetainedRelease(t, version)
	value.Release.Artifacts = hashRetainedRelease(t, root)
	if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(releaseArtifactsPath), "extra"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := validateRelease(value, root); err == nil || !strings.Contains(err.Error(), "exactly four") {
		t.Fatalf("extra retained file accepted: %v", err)
	}
}

func seedRetainedRelease(t *testing.T, version string) string {
	t.Helper()
	root := t.TempDir()
	for name, content := range map[string]string{
		"LICENSE": "license", "THIRD_PARTY_NOTICES": "notices", "README.md": "readme", "SECURITY.md": "security",
		"go.mod": "module github.com/otuschhoff/rados-go\n", "go.sum": "sum", "client.go": "package rados\n",
		"internal/source.go": "package internal\n", "examples/basic/main.go": "package main\n",
	} {
		fullPath := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(fullPath, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	output := filepath.Join(root, filepath.FromSlash(releaseArtifactsPath))
	command := exec.Command("go", "run", "../p12-release", "-root", root, "-out", output, "-version", version)
	command.Env = append(os.Environ(), "CGO_ENABLED=0", "GOTOOLCHAIN="+p12qualcontract.LatestGo)
	if data, err := command.CombinedOutput(); err != nil {
		t.Fatalf("generate retained release: %v\n%s", err, data)
	}
	return root
}

func hashRetainedRelease(t *testing.T, root string) map[string]string {
	t.Helper()
	result := make(map[string]string)
	entries, err := os.ReadDir(filepath.Join(root, filepath.FromSlash(releaseArtifactsPath)))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(releaseArtifactsPath), entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(data)
		result[entry.Name()] = hex.EncodeToString(digest[:])
	}
	return result
}

func TestProbeRejectsCounterResourceAndOrderFailures(t *testing.T) {
	valid := loadFixture(t).Probe.Secure
	tests := map[string]func(*probeReport){
		"counter":          func(value *probeReport) { value.Writes-- },
		"configured count": func(value *probeReport) { value.MaximumConfiguredSampleCount = 1 },
		"sample order":     func(value *probeReport) { value.Samples[1].ElapsedNS = value.Samples[0].ElapsedNS },
		"RSS growth": func(value *probeReport) {
			value.Samples[1].RSSBytes = value.Samples[0].RSSBytes + maximumProbeRSSGrowth + 1
		},
		"heap growth": func(value *probeReport) {
			value.Samples[1].HeapBytes = value.Samples[0].HeapBytes + maximumProbeHeapGrowth + 1
		},
		"goroutine growth": func(value *probeReport) {
			value.Samples[1].Goroutines = value.Samples[0].Goroutines + maximumProbeGoroutineGrowth + 1
		},
		"renewal generation": func(value *probeReport) {
			value.CredentialRenewals[0].CompletedGeneration = value.CredentialRenewals[0].DueGeneration
		},
		"missing OSD renewal": func(value *probeReport) { value.CredentialRenewals = value.CredentialRenewals[:1] },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			value := valid
			value.Samples = append([]resourceSample(nil), valid.Samples...)
			mutate(&value)
			if err := validateProbe("secure", value); err == nil {
				t.Fatal("invalid probe was accepted")
			}
		})
	}
}

func TestCredentialRenewalsAllowQueuedOverlap(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	renewals := []credentialRenewal{
		{Service: "monitor", SessionID: 1, DueGeneration: 2, CompletedGeneration: 4, DueAt: now, CompletedAt: now.Add(time.Second)},
		{Service: "osd", ServiceID: 2, SessionID: 2, DueGeneration: 8, CompletedGeneration: 12, DueAt: now, CompletedAt: now.Add(2 * time.Second)},
		{Service: "osd", ServiceID: 2, SessionID: 2, DueGeneration: 10, CompletedGeneration: 14, DueAt: now.Add(time.Second), CompletedAt: now.Add(3 * time.Second)},
	}
	if err := validateCredentialRenewals("secure", renewals); err != nil {
		t.Fatalf("queued overlapping renewals rejected: %v", err)
	}
}

func TestCredentialRenewalsRejectGenerationRegressions(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	valid := []credentialRenewal{
		{Service: "monitor", SessionID: 1, DueGeneration: 2, CompletedGeneration: 4, DueAt: now, CompletedAt: now.Add(time.Second)},
		{Service: "osd", ServiceID: 2, SessionID: 2, DueGeneration: 8, CompletedGeneration: 12, DueAt: now, CompletedAt: now.Add(2 * time.Second)},
		{Service: "osd", ServiceID: 2, SessionID: 2, DueGeneration: 10, CompletedGeneration: 14, DueAt: now.Add(time.Second), CompletedAt: now.Add(3 * time.Second)},
	}
	for name, mutate := range map[string]func([]credentialRenewal){
		"due":       func(values []credentialRenewal) { values[2].DueGeneration = values[1].DueGeneration },
		"completed": func(values []credentialRenewal) { values[2].CompletedGeneration = values[1].CompletedGeneration },
	} {
		t.Run(name, func(t *testing.T) {
			values := append([]credentialRenewal(nil), valid...)
			mutate(values)
			if err := validateCredentialRenewals("secure", values); err == nil {
				t.Fatal("generation regression was accepted")
			}
		})
	}
}

func TestChurnRejectsCounterAndFinalStateFailures(t *testing.T) {
	tests := map[string]func(*report){
		"recovery without restart": func(value *report) { value.Churn.MonitorRecoveries = value.Churn.MonitorRestarts + 1 },
		"OSD count":                func(value *report) { value.Churn.FinalOSDStat.NumUpOSDs = 2 },
		"remapped PG":              func(value *report) { value.Churn.FinalOSDStat.NumRemappedPGs = 1 },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			value := loadFixture(t)
			mutate(&value)
			if err := validateChurn(value); err == nil {
				t.Fatal("invalid churn evidence was accepted")
			}
		})
	}
}

func TestValidateArtifactsRequiresExactCurrentMap(t *testing.T) {
	root := t.TempDir()
	seedSourceTree(t, root)
	sourceArtifacts, err := expectedSourceArtifacts(root)
	if err != nil {
		t.Fatal(err)
	}
	want := make(map[string]string, len(sourceArtifacts))
	for index, path := range sourceArtifacts {
		data := []byte{byte(index + 1)}
		fullPath := filepath.Join(root, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(fullPath, data, 0o644); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(data)
		want[path] = hex.EncodeToString(digest[:])
	}
	if err := validateArtifacts(want, root); err != nil {
		t.Fatalf("exact artifact map rejected: %v", err)
	}

	for name, mutate := range map[string]func(map[string]string){
		"missing":    func(value map[string]string) { delete(value, sourceArtifacts[0]) },
		"extra":      func(value map[string]string) { value["unexpected"] = strings.Repeat("0", 64) },
		"wrong hash": func(value map[string]string) { value[sourceArtifacts[0]] = strings.Repeat("0", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			value := cloneMap(want)
			mutate(value)
			if err := validateArtifacts(value, root); err == nil {
				t.Fatal("inexact artifact map was accepted")
			}
		})
	}
}

func seedSourceTree(t *testing.T, root string) {
	t.Helper()
	for _, path := range []string{
		"Makefile", "SPEC.md", "README.md", "LICENSE", "THIRD_PARTY_NOTICES", "SECURITY.md", "go.mod", "go.sum",
		"docs/p00/api-inventory.csv", "docs/p00/compatibility.md", "docs/p00/licensing.md",
		"integration/p07/benchmark/main.go", "integration/p07/benchmark/main_unsupported.go", "integration/p07/native_benchmark.c",
		"internal/source.go", "examples/example.go", "tools/tool.go", "integration/p12/reproduce.sh", "docs/p12/README.md",
	} {
		fullPath := filepath.Join(root, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(fullPath, []byte("seed"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func TestValidateCertificationRequiresEnduranceAndExactBenchmark(t *testing.T) {
	value := loadFixture(t)
	value.Status = "candidate"
	value.Command = "./integration/p12/reproduce.sh"
	for _, probe := range []*probeReport{&value.Probe.Secure, &value.Probe.CRC} {
		probe.RequestedDurationNS = minimumCertifyingDurationNS
		probe.ElapsedNS = minimumCertifyingDurationNS
		probe.Reconnects = 24
		probe.LongestConnectionNS = minimumLongestConnectionNS + 1
	}
	value.Churn.MonitorRestarts, value.Churn.MonitorRecoveries = 1, 1
	value.Churn.OSDRestarts, value.Churn.OSDRecoveries = 1, 1
	value.Benchmark.Performed = true
	value.Benchmark.Runs = validBenchmarkRuns("arm64")
	if err := validateCertification(value); err != nil {
		t.Fatalf("valid synthetic certification structure rejected: %v", err)
	}

	value.Probe.Secure.Reconnects = 23
	if err := validateCertification(value); err == nil {
		t.Fatal("insufficient reconnect evidence was accepted")
	}
	value.Probe.Secure.Reconnects = 24
	value.Probe.Secure.ElapsedNS = uint64(48 * time.Hour)
	if err := validateCertification(value); err == nil {
		t.Fatal("reconnect count below elapsed-hour threshold was accepted")
	}
	value.Probe.Secure.Reconnects = 48
	value.Benchmark.Runs[0].Rows = value.Benchmark.Runs[0].Rows[:35]
	if err := validateCertification(value); err == nil {
		t.Fatal("incomplete benchmark matrix was accepted")
	}
}

func validBenchmarkRuns(architecture string) []benchmarkRun {
	allocations := uint64(100)
	allocatedBytes := uint64(1000)
	goos, version, library := "linux", "go-test", "librados.so.2"
	rows := make([]benchmarkRow, 0, 36)
	for _, size := range []uint64{4096, 65536, 1048576, 4194304} {
		for _, concurrency := range []int{1, 16, 64} {
			for _, workload := range []string{"read", "write", "mixed"} {
				operations := uint64(2 * concurrency)
				elapsed := uint64(1_000_000)
				rows = append(rows, benchmarkRow{
					SizeBytes: size, Concurrency: concurrency, Workload: workload,
					Operations: operations, Bytes: size * operations, ElapsedNS: elapsed,
					ThroughputBytesPerSecond: float64(size*operations) * 1e9 / float64(elapsed),
					IOPS:                     float64(operations) * 1e9 / float64(elapsed), P50NS: 10, P95NS: 20, P99NS: 30,
				})
			}
		}
	}
	newRows := func() []benchmarkRow { return append([]benchmarkRow(nil), rows...) }
	goEnvironment := environment{GOOS: &goos, GOARCH: &architecture, GoVersion: &version}
	nativeEnvironment := environment{Library: &library}
	goResources := resources{CPUUserNS: 1, Allocations: &allocations, AllocatedBytes: &allocatedBytes, MaxRSSBytes: 1000}
	nativeResources := resources{CPUUserNS: 1, MaxRSSBytes: 1000}
	return []benchmarkRun{
		{Implementation: "go", Transport: "secure", Environment: goEnvironment, Resources: goResources, Rows: newRows()},
		{Implementation: "go", Transport: "crc", Environment: goEnvironment, Resources: goResources, Rows: newRows()},
		{Implementation: "native", Transport: "secure", Environment: nativeEnvironment, Resources: nativeResources, Rows: newRows()},
		{Implementation: "native", Transport: "crc", Environment: nativeEnvironment, Resources: nativeResources, Rows: newRows()},
	}
}

func loadFixture(t *testing.T) report {
	t.Helper()
	value, err := readReport("../../integration/p12/report.json")
	if err != nil {
		t.Fatal(err)
	}
	value.Source.Artifacts = currentArtifactHashes(t, "../..")
	value.Release = releaseEvidence{Artifacts: map[string]string{}}
	if len(value.Probe.Secure.CredentialRenewals) == 0 {
		due := time.Unix(100, 0).UTC()
		for _, probe := range []*probeReport{&value.Probe.Secure, &value.Probe.CRC} {
			probe.CredentialRenewals = []credentialRenewal{
				{Service: "monitor", SessionID: 1, DueGeneration: 2, CompletedGeneration: 4, DueAt: due, CompletedAt: due.Add(time.Second)},
				{Service: "osd", ServiceID: 1, SessionID: 2, DueGeneration: 2, CompletedGeneration: 4, DueAt: due, CompletedAt: due.Add(time.Second)},
			}
		}
	}
	return value
}

func currentArtifactHashes(t *testing.T, root string) map[string]string {
	t.Helper()
	paths, err := expectedSourceArtifacts(root)
	if err != nil {
		t.Fatal(err)
	}
	result := make(map[string]string, len(paths))
	for _, path := range paths {
		data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(path)))
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(data)
		result[path] = hex.EncodeToString(digest[:])
	}
	return result
}

func readFixture(t *testing.T) []byte {
	t.Helper()
	data, err := os.ReadFile("../../integration/p12/report.json")
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func cloneMap(value map[string]string) map[string]string {
	result := make(map[string]string, len(value))
	for key, item := range value {
		result[key] = item
	}
	return result
}
