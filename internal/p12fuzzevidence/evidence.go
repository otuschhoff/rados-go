// Package p12fuzzevidence defines and validates observed P12 fuzz evidence.
package p12fuzzevidence

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/otuschhoff/go-librados/internal/p12fuzzcontract"
)

const (
	ReportPath = "docs/p12/fuzz-report.json"
	Command    = "CGO_ENABLED=0 GOTOOLCHAIN=go1.27.1 go run ./tools/p12-fuzz"
)

type Report struct {
	SchemaVersion int               `json:"schema_version"`
	Status        string            `json:"status"`
	Profile       string            `json:"profile"`
	Command       string            `json:"command"`
	Toolchain     string            `json:"toolchain"`
	Platform      string            `json:"platform"`
	StartedAt     string            `json:"started_at"`
	FinishedAt    string            `json:"finished_at"`
	Source        Source            `json:"source"`
	Targets       []TargetExecution `json:"targets"`
}

type Source struct {
	Identity  string            `json:"identity"`
	Artifacts map[string]string `json:"artifacts"`
}

type TargetExecution struct {
	Package      string `json:"package"`
	Name         string `json:"name"`
	Budget       string `json:"budget"`
	Command      string `json:"command"`
	Toolchain    string `json:"toolchain"`
	Platform     string `json:"platform"`
	StartedAt    string `json:"started_at"`
	FinishedAt   string `json:"finished_at"`
	ExitStatus   int    `json:"exit_status"`
	Pass         bool   `json:"pass"`
	Output       string `json:"output"`
	OutputSHA256 string `json:"output_sha256"`
}

func Decode(reader io.Reader) (Report, error) {
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	var value Report
	if err := decoder.Decode(&value); err != nil {
		return value, fmt.Errorf("decode strict fuzz JSON: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return value, errors.New("invalid fuzz JSON framing")
	}
	return value, nil
}

func Read(path string) (Report, []byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Report{}, nil, err
	}
	value, err := Decode(strings.NewReader(string(data)))
	return value, data, err
}

func ValidateFile(path, root string, requireCertifying bool) error {
	value, _, err := Read(path)
	if err != nil {
		return err
	}
	return Validate(value, root, requireCertifying)
}

func Validate(value Report, root string, requireCertifying bool) error {
	budget, knownProfile := p12fuzzcontract.Budget(value.Profile)
	if value.SchemaVersion != 1 || !knownProfile || (value.Status != "passed" && value.Status != "failed") {
		return errors.New("invalid fuzz report envelope")
	}
	wantCommand := Command + " -profile " + value.Profile
	pendingCommand := wantCommand + " -record-pending"
	if (value.Command != wantCommand && value.Command != pendingCommand) || value.Command == pendingCommand && value.Status != "failed" || value.Toolchain != p12fuzzcontract.Toolchain || !validPlatform(value.Platform) {
		return errors.New("fuzz report command, toolchain, or platform is not exact")
	}
	reportStarted, reportFinished, err := parseTimes(value.StartedAt, value.FinishedAt)
	if err != nil {
		return fmt.Errorf("fuzz report envelope: %w", err)
	}
	expectedSource, err := SourceArtifacts(root)
	if err != nil {
		return err
	}
	if value.Source.Identity != "content-addressed-artifacts" || !equalHashes(value.Source.Artifacts, expectedSource) {
		return errors.New("fuzz report source artifacts do not match the current tree")
	}
	targets := p12fuzzcontract.Targets()
	if len(value.Targets) != len(targets) {
		return fmt.Errorf("fuzz report has %d targets, want exactly %d", len(value.Targets), len(targets))
	}
	minimumReportDuration := budget * time.Duration(len(targets))
	if reportFinished.Sub(reportStarted) < minimumReportDuration {
		return fmt.Errorf("fuzz report duration is shorter than the immutable profile budget total %s", minimumReportDuration)
	}
	allPassed := true
	previousFinished := time.Time{}
	for index, target := range targets {
		execution := value.Targets[index]
		arguments := p12fuzzcontract.Command(target, budget)
		wantTargetCommand := "CGO_ENABLED=0 GOTOOLCHAIN=" + p12fuzzcontract.Toolchain + " " + strings.Join(arguments, " ")
		if execution.Package != target.Package || execution.Name != target.Name || execution.Budget != p12fuzzcontract.BudgetText(budget) || execution.Command != wantTargetCommand || execution.Toolchain != value.Toolchain || execution.Platform != value.Platform {
			return fmt.Errorf("fuzz target %d does not match the immutable contract", index)
		}
		started, finished, err := parseTimes(execution.StartedAt, execution.FinishedAt)
		if err != nil || started.Before(reportStarted) || finished.After(reportFinished) || finished.Sub(started) < budget {
			return fmt.Errorf("fuzz target %s has invalid or out-of-envelope timestamps", target.Name)
		}
		if index > 0 && !started.Equal(previousFinished) {
			return fmt.Errorf("fuzz target %s is not sequential and adjacent to the prior contract target", target.Name)
		}
		previousFinished = finished
		digest := sha256.Sum256([]byte(execution.Output))
		if execution.OutputSHA256 != hex.EncodeToString(digest[:]) {
			return fmt.Errorf("fuzz target %s output hash mismatch", target.Name)
		}
		if execution.Pass != (execution.ExitStatus == 0) {
			return fmt.Errorf("fuzz target %s has inconsistent exit status and pass", target.Name)
		}
		allPassed = allPassed && execution.Pass
	}
	if (value.Status == "passed") != allPassed {
		return errors.New("fuzz report status does not match target results")
	}
	if requireCertifying && (value.Profile != p12fuzzcontract.CertifyingProfile || value.Status != "passed") {
		return errors.New("certification requires a passed 10m certifying fuzz report")
	}
	return nil
}

func SourceArtifacts(root string) (map[string]string, error) {
	result := make(map[string]string)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		if entry.IsDir() {
			if relative == ".git" || relative == "docs/p12/release-artifacts" {
				return filepath.SkipDir
			}
			return nil
		}
		if !entry.Type().IsRegular() || slices.Contains([]string{ReportPath, "docs/p12/qualification-report.json", "docs/p12/human-review.json", "integration/p12/report.json"}, relative) {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		digest := sha256.Sum256(data)
		result[relative] = hex.EncodeToString(digest[:])
		return nil
	})
	return result, err
}

func Write(path string, value Report) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".fuzz-report-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if _, err := temporary.Write(data); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryPath, path)
}

func Timestamp(value time.Time) string { return value.UTC().Format(time.RFC3339Nano) }

func HashOutput(output string) string {
	digest := sha256.Sum256([]byte(output))
	return hex.EncodeToString(digest[:])
}

func parseTimes(startedAt, finishedAt string) (time.Time, time.Time, error) {
	started, startErr := time.Parse(time.RFC3339Nano, startedAt)
	finished, finishErr := time.Parse(time.RFC3339Nano, finishedAt)
	if startErr != nil || finishErr != nil || !finished.After(started) {
		return time.Time{}, time.Time{}, errors.New("invalid or non-positive timestamps")
	}
	return started, finished, nil
}

func validPlatform(platform string) bool {
	return slices.Contains([]string{"darwin/amd64", "darwin/arm64"}, platform)
}

func equalHashes(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for path, digest := range right {
		if left[path] != digest {
			return false
		}
	}
	return true
}
