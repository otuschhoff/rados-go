package main

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
	"sort"
	"strings"
	"time"

	"github.com/otuschhoff/rados-go/internal/p12fuzzevidence"
	"github.com/otuschhoff/rados-go/internal/p12qualcontract"
)

type qualificationReport struct {
	SchemaVersion int    `json:"schema_version"`
	Status        string `json:"status"`
	Command       string `json:"command"`
	StartedAt     string `json:"started_at"`
	FinishedAt    string `json:"finished_at"`
	Source        struct {
		Identity  string            `json:"identity"`
		Artifacts map[string]string `json:"artifacts"`
	} `json:"source"`
	Checks        []qualificationCheck   `json:"checks"`
	RuntimeMatrix []qualificationRuntime `json:"runtime_matrix"`
	PriorReports  []qualificationPrior   `json:"prior_reports"`
	Release       qualificationRelease   `json:"release"`
}

type qualificationCheck struct {
	ID           string `json:"id"`
	Command      string `json:"command"`
	Toolchain    string `json:"toolchain"`
	Platform     string `json:"platform"`
	StartedAt    string `json:"started_at"`
	FinishedAt   string `json:"finished_at"`
	ExitStatus   int    `json:"exit_status"`
	Pass         bool   `json:"pass"`
	OutputSHA256 string `json:"output_sha256"`
	Output       string `json:"output"`
}

type qualificationRuntime struct {
	qualificationCheck
	Observation *qualificationObservation `json:"observation"`
}

type qualificationObservation struct {
	GOOS          string `json:"goos"`
	GOARCH        string `json:"goarch"`
	ModulePath    string `json:"module_path"`
	ModuleVersion string `json:"module_version"`
	CGOEnabled    bool   `json:"cgo_enabled"`
}

type qualificationPrior struct {
	Phase           string            `json:"phase"`
	VerifierCommand string            `json:"verifier_command"`
	CheckID         string            `json:"check_id"`
	Artifacts       map[string]string `json:"artifacts"`
}

type qualificationRelease struct {
	Version      string              `json:"version"`
	Reproducible bool                `json:"reproducible"`
	Runs         []map[string]string `json:"runs"`
	Artifacts    map[string]string   `json:"artifacts"`
}

func decodeQualification(reader io.Reader) (qualificationReport, error) {
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	var value qualificationReport
	if err := decoder.Decode(&value); err != nil {
		return value, fmt.Errorf("decode strict qualification JSON: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return value, errors.New("invalid qualification JSON framing")
	}
	return value, nil
}

func validateQualificationFile(path, root string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("read qualification report: %w", err)
	}
	defer file.Close()
	value, err := decodeQualification(file)
	if err != nil {
		return err
	}
	return validateQualificationReport(value, root)
}

func validateQualificationReport(value qualificationReport, root string) error {
	if value.SchemaVersion != 1 || value.Status != "passed" || value.Command != "./integration/p12/qualify.sh" {
		return errors.New("qualification report is not passed with the exact schema and command")
	}
	reportStarted, reportFinished, err := parseQualificationTimes(value.StartedAt, value.FinishedAt)
	if err != nil {
		return err
	}
	if value.Source.Identity != "content-addressed-artifacts" {
		return errors.New("qualification source identity is invalid")
	}
	expectedSource, err := qualificationSourceArtifacts(root)
	if err != nil {
		return err
	}
	if err := validateExactHashes("qualification source", value.Source.Artifacts, expectedSource); err != nil {
		return err
	}
	checkSpecs := p12qualcontract.Checks(p12qualcontract.DefaultPins(), "v0.0.0-p12")
	if len(value.Checks) != len(checkSpecs) {
		return fmt.Errorf("qualification has %d checks, want exactly %d", len(value.Checks), len(checkSpecs))
	}
	seen := make(map[string]bool, len(value.Checks))
	for index, check := range value.Checks {
		spec := checkSpecs[index]
		if seen[check.ID] || check.ID != spec.ID {
			return fmt.Errorf("qualification has invalid or duplicate check %q", check.ID)
		}
		seen[check.ID] = true
		if err := validateQualificationCheck(check, spec, reportStarted, reportFinished); err != nil {
			return fmt.Errorf("qualification check %s: %w", check.ID, err)
		}
	}
	if err := validateQualificationRuntimes(value.RuntimeMatrix, reportStarted, reportFinished); err != nil {
		return err
	}
	if err := validateQualificationChronology(value.Checks, value.RuntimeMatrix, reportStarted); err != nil {
		return err
	}
	if err := validateQualificationPrior(value.PriorReports, root, seen); err != nil {
		return err
	}
	return validateQualificationRelease(value.Release)
}

func validateQualificationCheck(check qualificationCheck, spec p12qualcontract.Spec, reportStarted, reportFinished time.Time) error {
	if check.ID != spec.ID || check.Command != spec.Command || check.Toolchain != spec.Toolchain || check.Platform != spec.Platform || check.ExitStatus != 0 || !check.Pass {
		return errors.New("missing, skipped, failed, or malformed observed execution")
	}
	digest := sha256.Sum256([]byte(check.Output))
	if check.OutputSHA256 != hex.EncodeToString(digest[:]) {
		return errors.New("output hash mismatch")
	}
	if check.ID == "go-version-latest" && !observedQualificationGoVersion(check.Output, spec.Toolchain) {
		return errors.New("observed Go version does not match the declared toolchain")
	}
	started, finished, err := parseQualificationTimes(check.StartedAt, check.FinishedAt)
	if err != nil {
		return err
	}
	if started.Before(reportStarted) || finished.After(reportFinished) {
		return errors.New("execution timestamps fall outside the report envelope")
	}
	return nil
}

func observedQualificationGoVersion(output, toolchain string) bool {
	fields := strings.Fields(output)
	return len(fields) >= 3 && fields[0] == "go" && fields[1] == "version" && fields[2] == toolchain
}

func validateQualificationRuntimes(values []qualificationRuntime, reportStarted, reportFinished time.Time) error {
	specs := p12qualcontract.Runtimes(p12qualcontract.DefaultPins())
	if len(values) != len(specs) {
		return fmt.Errorf("runtime matrix has %d entries, want exactly four", len(values))
	}
	seen := make(map[string]bool, len(values))
	for index, value := range values {
		spec := specs[index]
		if seen[value.ID] || value.ID != spec.ID {
			return fmt.Errorf("runtime matrix has invalid or duplicate check %q", value.ID)
		}
		seen[value.ID] = true
		if err := validateQualificationCheck(value.qualificationCheck, spec, reportStarted, reportFinished); err != nil {
			return fmt.Errorf("runtime %s: %w", value.ID, err)
		}
		observation := value.Observation
		if observation == nil || observation.GOOS+"/"+observation.GOARCH != spec.Platform || observation.ModulePath != p12qualcontract.Module || observation.ModuleVersion == "" || observation.CGOEnabled {
			return fmt.Errorf("runtime %s has missing or mismatched execution evidence", value.Platform)
		}
	}
	return nil
}

func validateQualificationChronology(checks []qualificationCheck, runtimes []qualificationRuntime, reportStarted time.Time) error {
	split := len(checks) - 10
	ordered := make([]qualificationCheck, 0, len(checks)+len(runtimes))
	ordered = append(ordered, checks[:split]...)
	for _, runtime := range runtimes {
		ordered = append(ordered, runtime.qualificationCheck)
	}
	ordered = append(ordered, checks[split:]...)
	previousFinished := reportStarted
	for _, execution := range ordered {
		started, finished, err := parseQualificationTimes(execution.StartedAt, execution.FinishedAt)
		if err != nil {
			return fmt.Errorf("qualification execution %s: %w", execution.ID, err)
		}
		if started.Before(previousFinished) {
			return fmt.Errorf("qualification execution %s overlaps or precedes its predecessor", execution.ID)
		}
		previousFinished = finished
	}
	return nil
}

func validateQualificationPrior(values []qualificationPrior, root string, checks map[string]bool) error {
	if len(values) != 9 {
		return fmt.Errorf("qualification binds %d prior phases, want nine", len(values))
	}
	seen := make(map[string]bool, 9)
	for _, value := range values {
		if seen[value.Phase] || value.CheckID != "verify-"+value.Phase || !checks[value.CheckID] || value.VerifierCommand != "CGO_ENABLED=0 GOTOOLCHAIN="+p12qualcontract.LatestGo+" go run ./tools/"+value.Phase+"-verify" {
			return fmt.Errorf("prior phase %s has invalid verifier binding", value.Phase)
		}
		expected, err := expectedPriorArtifacts(root, value.Phase)
		if err != nil {
			return err
		}
		if err := validateExactHashes("prior phase "+value.Phase, value.Artifacts, expected); err != nil {
			return err
		}
		seen[value.Phase] = true
	}
	for phase := 3; phase <= 11; phase++ {
		if !seen[fmt.Sprintf("p%02d", phase)] {
			return fmt.Errorf("missing prior phase p%02d", phase)
		}
	}
	return nil
}

func validateQualificationRelease(value qualificationRelease) error {
	if value.Version != "v0.0.0-p12" || !value.Reproducible || len(value.Runs) != 2 || len(value.Artifacts) != 4 {
		return errors.New("qualification lacks two reproducible release runs")
	}
	for _, run := range value.Runs {
		if err := validateExactHashes("release run", run, value.Artifacts); err != nil {
			return err
		}
	}
	for name, digest := range value.Artifacts {
		if !validSHA256(digest) || name == "" {
			return errors.New("qualification release has invalid artifact evidence")
		}
	}
	return nil
}

func qualificationSourceArtifacts(root string) (map[string]string, error) {
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
			if relative == ".git" || relative == releaseArtifactsPath {
				return filepath.SkipDir
			}
			return nil
		}
		if !entry.Type().IsRegular() || relative == "docs/p12/qualification-report.json" || relative == "docs/p12/human-review.json" || relative == "integration/p12/report.json" || relative == p12fuzzevidence.ReportPath {
			return nil
		}
		digest, err := qualificationHashFile(path)
		if err != nil {
			return err
		}
		result[relative] = digest
		return nil
	})
	return result, err
}

func expectedPriorArtifacts(root, phase string) (map[string]string, error) {
	paths := []string{"docs/" + phase + "/integration-report.json"}
	if phase == "p05" {
		matches, err := filepath.Glob(filepath.Join(root, "testdata/p05/*"))
		if err != nil {
			return nil, err
		}
		paths = paths[:0]
		for _, match := range matches {
			if info, err := os.Stat(match); err == nil && info.Mode().IsRegular() {
				relative, _ := filepath.Rel(root, match)
				paths = append(paths, filepath.ToSlash(relative))
			}
		}
		sort.Strings(paths)
	}
	result := make(map[string]string, len(paths))
	for _, path := range paths {
		digest, err := qualificationHashFile(filepath.Join(root, filepath.FromSlash(path)))
		if err != nil {
			return nil, fmt.Errorf("read prior phase artifact %s: %w", path, err)
		}
		result[path] = digest
	}
	return result, nil
}

func validateExactHashes(label string, actual, expected map[string]string) error {
	if len(actual) != len(expected) {
		return fmt.Errorf("%s artifact map has %d entries, want exactly %d", label, len(actual), len(expected))
	}
	for path, digest := range expected {
		if actual[path] != digest {
			return fmt.Errorf("%s hash mismatch for %s", label, path)
		}
	}
	return nil
}

func qualificationHashFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

func parseQualificationTimes(start, finish string) (time.Time, time.Time, error) {
	started, startErr := time.Parse(time.RFC3339Nano, start)
	finished, finishErr := time.Parse(time.RFC3339Nano, finish)
	if startErr != nil || finishErr != nil || !finished.After(started) {
		return time.Time{}, time.Time{}, errors.New("invalid or non-positive-duration timestamps")
	}
	return started, finished, nil
}

func validSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
