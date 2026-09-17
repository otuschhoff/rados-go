// Command p12-verify validates the checked-in P12 endurance evidence.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/otuschhoff/rados-go/internal/p12fuzzevidence"
)

const (
	cephRepository = "https://github.com/ceph/ceph.git"
	cephCommit     = "7f793731f1b39eb4f465e960113d2363c311b964"
	cephVersion    = "ceph version 20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964) tentacle (stable)"

	imageAMD64 = "quay.io/ceph/ceph@sha256:09ee90f6f3e0c7b9954f71d214ee05e9bbaaaea3716b1dd619603283b829f8b8"
	imageARM64 = "quay.io/ceph/ceph@sha256:6e6bc7b28fa1b334108a3646af5533dfb50db508efdf5b358eb7dd0dd37a48aa"
	monAMD64   = "10814919731d782d6510bdb0d9064cb7c04bd7a563e235aa3fa94e07f17d1215"
	monARM64   = "4b44a5660fdcc6bf7701f64fa41f707b8ff619d09042219d071e6d9ac45ab81b"
	osdAMD64   = "00bf5abfda185998e1b6a42bd58410c161345826a9d74e8b86e83d1aead9c319"
	osdARM64   = "5fbad656b7f1bd900a0c40990a81be8dff523555ed0b2c957e38f29beb33608a"

	minimumCertifyingDurationNS = uint64(24 * time.Hour)
	minimumLongestConnectionNS  = uint64(15 * time.Minute)
	maximumProbeRSSGrowth       = uint64(256 << 20)
	maximumProbeHeapGrowth      = uint64(128 << 20)
	maximumProbeGoroutineGrowth = uint64(256)
	maximumProbeSamples         = 2000
	placeholderReleaseVersion  = "v0.0.0-p12"
)

type report struct {
	SchemaVersion int                   `json:"schema_version"`
	Status        string                `json:"status"`
	Command       string                `json:"command"`
	StartedAt     string                `json:"started_at"`
	FinishedAt    string                `json:"finished_at"`
	Qualification *qualificationBinding `json:"qualification"`
	Fuzz          *fuzzBinding          `json:"fuzz"`
	Reviews       *reviewBinding        `json:"reviews"`
	Source        struct {
		Repository string            `json:"repository"`
		Identity   string            `json:"identity"`
		Artifacts  map[string]string `json:"artifacts"`
	} `json:"source"`
	Server struct {
		Repository         string `json:"repository"`
		SourceAnchorCommit string `json:"source_anchor_commit"`
		Version            string `json:"version"`
		Image              string `json:"image"`
		Platform           string `json:"platform"`
		Binaries           struct {
			MonitorSHA256 string `json:"mon_sha256"`
			OSDSHA256     string `json:"osd_sha256"`
		} `json:"binaries"`
	} `json:"server"`
	Cluster struct {
		FSID     string   `json:"fsid"`
		Network  string   `json:"network"`
		Monitors []string `json:"monitors"`
		OSDs     int      `json:"osds"`
		Pool     struct {
			Name    string `json:"name"`
			Size    int    `json:"size"`
			MinSize int    `json:"min_size"`
			PGNum   int    `json:"pg_num"`
		} `json:"pool"`
		ExternalDefaults        bool     `json:"external_defaults"`
		ServiceTicketTTLSeconds uint64   `json:"service_ticket_ttl_seconds"`
		Transports              []string `json:"transports"`
	} `json:"cluster"`
	Probe struct {
		Secure probeReport `json:"secure"`
		CRC    probeReport `json:"crc"`
	} `json:"probe"`
	Churn struct {
		MonitorRestarts   uint64 `json:"monitor_restarts"`
		MonitorRecoveries uint64 `json:"monitor_recoveries"`
		OSDRestarts       uint64 `json:"osd_restarts"`
		OSDRecoveries     uint64 `json:"osd_recoveries"`
		FinalOSDStat      struct {
			Epoch          uint64 `json:"epoch"`
			NumOSDs        uint64 `json:"num_osds"`
			NumUpOSDs      uint64 `json:"num_up_osds"`
			OSDUpSince     uint64 `json:"osd_up_since"`
			NumInOSDs      uint64 `json:"num_in_osds"`
			OSDInSince     uint64 `json:"osd_in_since"`
			NumRemappedPGs uint64 `json:"num_remapped_pgs"`
		} `json:"final_osd_stat"`
		FinalHealth struct {
			Status string                 `json:"status"`
			Checks map[string]healthCheck `json:"checks"`
			Mutes  []json.RawMessage      `json:"mutes"`
		} `json:"final_health"`
	} `json:"churn"`
	Benchmark struct {
		Performed bool           `json:"performed"`
		Runs      []benchmarkRun `json:"runs"`
	} `json:"benchmark"`
	Release releaseEvidence `json:"release"`
}

type qualificationBinding struct {
	Path   string `json:"path"`
	Status string `json:"status"`
	SHA256 string `json:"sha256"`
}

type fuzzBinding struct {
	Path    string `json:"path"`
	Status  string `json:"status"`
	Profile string `json:"profile"`
	SHA256  string `json:"sha256"`
}

type reviewBinding struct {
	Path   string `json:"path"`
	Status string `json:"status"`
	SHA256 string `json:"sha256"`
}

type releaseEvidence struct {
	Performed    bool              `json:"performed"`
	Version      *string           `json:"version"`
	Path         *string           `json:"path"`
	Reproducible bool              `json:"reproducible"`
	Artifacts    map[string]string `json:"artifacts"`
}

var releaseVersionPattern = regexp.MustCompile(`^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$`)

type healthCheck struct {
	Severity string `json:"severity"`
	Summary  struct {
		Message string `json:"message"`
		Count   uint64 `json:"count"`
	} `json:"summary"`
	Muted bool `json:"muted"`
}

type probeReport struct {
	Transport                    string              `json:"transport"`
	RequestedDurationNS          uint64              `json:"requested_duration_ns"`
	ElapsedNS                    uint64              `json:"elapsed_ns"`
	MonotonicDurationSatisfied   bool                `json:"monotonic_duration_satisfied"`
	Operations                   uint64              `json:"operations"`
	Writes                       uint64              `json:"writes"`
	AppendOnceVerifications      uint64              `json:"append_once_verifications"`
	DuplicateMutationsDetected   uint64              `json:"duplicate_mutations_detected"`
	Reads                        uint64              `json:"reads"`
	Stats                        uint64              `json:"stats"`
	Removes                      uint64              `json:"removes"`
	Reconnects                   uint64              `json:"reconnects"`
	LongestConnectionNS          uint64              `json:"longest_connection_ns"`
	CredentialRenewals           []credentialRenewal `json:"credential_renewals"`
	Samples                      []resourceSample    `json:"samples"`
	InflightMeasurement          string              `json:"inflight_measurement"`
	MaximumConfiguredSampleCount uint64              `json:"maximum_configured_sample_count"`
}

type credentialRenewal struct {
	Service             string    `json:"service"`
	ServiceID           int32     `json:"service_id"`
	SessionID           uint64    `json:"session_id"`
	DueGeneration       uint64    `json:"due_generation"`
	CompletedGeneration uint64    `json:"completed_generation"`
	DueAt               time.Time `json:"due_at"`
	CompletedAt         time.Time `json:"completed_at"`
}

type resourceSample struct {
	ElapsedNS  uint64  `json:"elapsed_ns"`
	RSSBytes   uint64  `json:"rss_bytes"`
	Goroutines uint64  `json:"goroutines"`
	HeapBytes  uint64  `json:"heap_bytes"`
	Inflight   *uint64 `json:"inflight"`
}

func main() {
	reportPath := flag.String("report", "integration/p12/report.json", "P12 report to validate")
	allowNonCertifying := flag.Bool("allow-non-certifying", false, "validate non-certifying harness evidence")
	checkQualification := flag.String("check-qualification", "", "validate a passed qualification report against the current tree and exit")
	checkFuzz := flag.String("check-fuzz", "", "validate a fuzz report against the current tree and exit")
	requireCertifyingFuzz := flag.Bool("require-certifying-fuzz", false, "require a passed 10m fuzz profile with -check-fuzz")
	checkHumanReview := flag.String("check-human-review", "", "validate an approved human-review report and exit")
	reviewerTrust := flag.String("reviewer-trust", reviewerTrustPath, "reviewer trust policy for human-review verification")
	reviewerTrustSHA256 := flag.String("reviewer-trust-sha256", os.Getenv("P12_REVIEWER_TRUST_SHA256"), "externally trusted lowercase SHA-256 of the reviewer trust policy")
	printReviewPayload := flag.String("print-review-payload", "", "print the canonical payload for one review role")
	flag.Parse()
	provided := make(map[string]bool)
	flag.Visit(func(value *flag.Flag) { provided[value.Name] = true })
	if flag.NArg() != 0 {
		fatalf("unexpected positional arguments")
	}
	if *checkFuzz != "" {
		if provided["report"] || provided["allow-non-certifying"] || provided["check-qualification"] || provided["check-human-review"] || provided["reviewer-trust"] || provided["reviewer-trust-sha256"] || provided["print-review-payload"] {
			fatalf("fuzz verification failed: -check-fuzz cannot be combined with other report modes")
		}
		value, _, err := p12fuzzevidence.Read(*checkFuzz)
		if err != nil {
			fatalf("fuzz verification failed: %v", err)
		}
		if err := p12fuzzevidence.Validate(value, ".", *requireCertifyingFuzz); err != nil {
			fatalf("fuzz verification failed: %v", err)
		}
		fmt.Printf("P12 fuzz evidence validated: status=%s profile=%s\n", value.Status, value.Profile)
		return
	}
	if *requireCertifyingFuzz {
		fatalf("-require-certifying-fuzz requires -check-fuzz")
	}
	if *checkQualification != "" {
		if provided["report"] || provided["allow-non-certifying"] || provided["check-human-review"] || provided["reviewer-trust"] || provided["reviewer-trust-sha256"] || provided["print-review-payload"] {
			fatalf("qualification verification failed: -check-qualification cannot be combined with report or human-review options")
		}
		if err := validateQualificationFile(*checkQualification, "."); err != nil {
			fatalf("qualification verification failed: %v", err)
		}
		fmt.Println("P12 qualification evidence validated")
		return
	}
	if *checkHumanReview != "" {
		if provided["report"] || provided["allow-non-certifying"] {
			fatalf("human-review verification failed: -check-human-review cannot be combined with report options")
		}
		if *printReviewPayload != "" {
			value, err := readHumanReview(*checkHumanReview)
			if err != nil {
				fatalf("human-review payload failed: %v", err)
			}
			for _, review := range value.Reviews {
				if review.Role == *printReviewPayload {
					payload, err := canonicalPayload(value, review)
					if err != nil {
						fatalf("human-review payload failed: %v", err)
					}
					_, _ = os.Stdout.Write(payload)
					return
				}
			}
			fatalf("human-review payload failed: role %q is absent", *printReviewPayload)
		}
		if err := validateTrustPolicyDigest(*reviewerTrust, *reviewerTrustSHA256); err != nil {
			fatalf("human-review verification failed: %v", err)
		}
		if err := validateHumanReviewFile(*checkHumanReview, *reviewerTrust, *reportPath, "."); err != nil {
			fatalf("human-review verification failed: %v", err)
		}
		fmt.Println("P12 human-review evidence validated")
		return
	}
	if *printReviewPayload != "" || provided["reviewer-trust"] || provided["reviewer-trust-sha256"] {
		fatalf("human-review options require -check-human-review")
	}

	value, err := readReport(*reportPath)
	if err != nil {
		fatalf("read P12 report: %v", err)
	}
	if value.Status == "candidate" {
		if err := validateTrustPolicyDigest(reviewerTrustPath, *reviewerTrustSHA256); err != nil {
			fatalf("P12 verification failed: %v", err)
		}
	}
	certified, err := validateReport(value, ".", *allowNonCertifying)
	if err != nil {
		fatalf("P12 verification failed: %v", err)
	}
	if certified {
		fmt.Println("P12 certification verified")
	} else {
		fmt.Println("P12 non-certifying evidence validated")
	}
}

func readReport(path string) (report, error) {
	file, err := os.Open(path)
	if err != nil {
		return report{}, err
	}
	defer file.Close()
	return decodeReport(file)
}

func decodeReport(reader io.Reader) (report, error) {
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	var value report
	if err := decoder.Decode(&value); err != nil {
		return report{}, fmt.Errorf("decode strict JSON: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return report{}, errors.New("invalid JSON framing: trailing content")
	}
	return value, nil
}

func validateReport(value report, root string, allowNonCertifying bool) (bool, error) {
	if err := validateEnvelope(value); err != nil {
		return false, err
	}
	if value.Status == "non-certifying" && !allowNonCertifying {
		return false, errors.New("report is non-certifying; use -allow-non-certifying only to validate quick harness evidence")
	}
	if err := validateIdentity(value); err != nil {
		return false, err
	}
	if err := validateArtifacts(value.Source.Artifacts, root); err != nil {
		return false, err
	}
	if err := validateProbe("secure", value.Probe.Secure); err != nil {
		return false, err
	}
	if err := validateProbe("crc", value.Probe.CRC); err != nil {
		return false, err
	}
	if err := validateChurn(value); err != nil {
		return false, err
	}
	if err := validateRelease(value, root); err != nil {
		return false, err
	}
	if err := validateQualification(value, root); err != nil {
		return false, err
	}
	if err := validateFuzz(value, root); err != nil {
		return false, err
	}
	if err := validateDetachedReviews(value, root); err != nil {
		return false, err
	}

	switch value.Status {
	case "candidate":
		if err := validateCertification(value); err != nil {
			return false, err
		}
		return true, nil
	case "non-certifying":
		if value.Command == "./integration/p12/reproduce.sh" || value.Benchmark.Performed || len(value.Benchmark.Runs) != 0 {
			return false, errors.New("invalid non-certifying report shape")
		}
		return false, nil
	default:
		return false, fmt.Errorf("invalid report status %q", value.Status)
	}
}

func validateQualification(value report, root string) error {
	if value.Status == "non-certifying" {
		if value.Qualification != nil {
			return errors.New("non-certifying report must record qualification as null")
		}
		return nil
	}
	if value.Qualification == nil {
		return errors.New("candidate report is missing qualification evidence")
	}
	binding := value.Qualification
	if binding.Path != "docs/p12/qualification-report.json" || binding.Status != "passed" {
		return errors.New("candidate report has invalid qualification identity or status")
	}
	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(binding.Path)))
	if err != nil {
		return fmt.Errorf("read qualification report: %w", err)
	}
	digest := sha256.Sum256(data)
	if binding.SHA256 != hex.EncodeToString(digest[:]) {
		return errors.New("qualification report hash mismatch")
	}
	nested, err := decodeQualification(strings.NewReader(string(data)))
	if err != nil {
		return err
	}
	return validateQualificationReport(nested, root)
}

func validateFuzz(value report, root string) error {
	if value.Status == "non-certifying" {
		if value.Fuzz != nil {
			return errors.New("non-certifying report must record fuzz as null")
		}
		return nil
	}
	if value.Fuzz == nil {
		return errors.New("candidate report is missing fuzz evidence")
	}
	binding := value.Fuzz
	if binding.Path != p12fuzzevidence.ReportPath || binding.Status != "passed" || binding.Profile != "certifying" {
		return errors.New("candidate report has invalid fuzz identity, status, or profile")
	}
	path := filepath.Join(root, filepath.FromSlash(binding.Path))
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read fuzz report: %w", err)
	}
	digest := sha256.Sum256(data)
	if binding.SHA256 != hex.EncodeToString(digest[:]) {
		return errors.New("fuzz report hash mismatch")
	}
	return p12fuzzevidence.ValidateFile(path, root, true)
}

func validateRelease(value report, root string) error {
	release := value.Release
	if value.Status == "non-certifying" {
		if release.Performed || release.Version != nil || release.Path != nil || release.Reproducible || len(release.Artifacts) != 0 {
			return errors.New("non-certifying report must record release generation as not performed")
		}
		return nil
	}
	if !release.Performed || release.Version == nil || *release.Version == placeholderReleaseVersion || release.Path == nil || *release.Path != releaseArtifactsPath || !release.Reproducible || !releaseVersionPattern.MatchString(*release.Version) {
		return errors.New("certifying report lacks reproducible semantic-version release evidence")
	}
	return validateReleaseArtifacts(root, *release.Version, release.Artifacts)
}

func validateEnvelope(value report) error {
	started, startErr := time.Parse("2006-01-02T15:04:05Z", value.StartedAt)
	finished, finishErr := time.Parse("2006-01-02T15:04:05Z", value.FinishedAt)
	if value.SchemaVersion != 2 || startErr != nil || finishErr != nil || !finished.After(started) {
		return errors.New("invalid report schema or monotonic timestamps")
	}
	maximumElapsed := max(value.Probe.Secure.ElapsedNS, value.Probe.CRC.ElapsedNS)
	if uint64(finished.Sub(started)) < maximumElapsed {
		return errors.New("report timestamps do not cover probe elapsed time")
	}
	return nil
}

func validateIdentity(value report) error {
	expectedImage := map[string]string{"linux/amd64": imageAMD64, "linux/arm64": imageARM64}[value.Server.Platform]
	expectedMon := map[string]string{"linux/amd64": monAMD64, "linux/arm64": monARM64}[value.Server.Platform]
	expectedOSD := map[string]string{"linux/amd64": osdAMD64, "linux/arm64": osdARM64}[value.Server.Platform]
	if expectedImage == "" || value.Server.Repository != cephRepository || value.Server.SourceAnchorCommit != cephCommit || value.Server.Version != cephVersion || value.Server.Image != expectedImage || value.Server.Binaries.MonitorSHA256 != expectedMon || value.Server.Binaries.OSDSHA256 != expectedOSD {
		return errors.New("server image or binary identity does not match the pinned platform")
	}
	if value.Source.Repository != "https://github.com/otuschhoff/rados-go.git" || value.Source.Identity != "content-addressed-artifacts" {
		return errors.New("invalid source identity")
	}
	cluster := value.Cluster
	if cluster.FSID != "31111111-2222-4333-8444-121212121212" || cluster.Network != "172.30.112.0/24" || !slices.Equal(cluster.Monitors, []string{"v2:172.30.112.10:3300"}) || cluster.OSDs != 3 || cluster.Pool.Name != "p12-data" || cluster.Pool.Size != 2 || cluster.Pool.MinSize != 1 || cluster.Pool.PGNum != 16 || cluster.ExternalDefaults || cluster.ServiceTicketTTLSeconds < 1 || cluster.ServiceTicketTTLSeconds > 3600 || !slices.Equal(cluster.Transports, []string{"secure", "crc"}) {
		return errors.New("invalid exact P12 cluster identity")
	}
	return nil
}

func validateArtifacts(actual map[string]string, root string) error {
	sourceArtifacts, err := expectedSourceArtifacts(root)
	if err != nil {
		return err
	}
	if len(actual) != len(sourceArtifacts) {
		return fmt.Errorf("source artifact map has %d entries, want exactly %d", len(actual), len(sourceArtifacts))
	}
	for _, path := range sourceArtifacts {
		data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(path)))
		if err != nil {
			return fmt.Errorf("read source artifact %q: %w", path, err)
		}
		digest := sha256.Sum256(data)
		want := hex.EncodeToString(digest[:])
		if actual[path] != want {
			return fmt.Errorf("source artifact hash mismatch for %s", path)
		}
	}
	return nil
}

func expectedSourceArtifacts(root string) ([]string, error) {
	paths := []string{
		".github/workflows/p00.yml", ".gitignore", "Makefile", "SPEC.md", "README.md", "LICENSE", "THIRD_PARTY_NOTICES", "SECURITY.md", "go.mod", "go.sum",
		"docs/p00/api-inventory.csv", "docs/p00/compatibility.md", "docs/p00/licensing.md",
		"integration/p07/benchmark/main.go", "integration/p07/benchmark/main_unsupported.go", "integration/p07/native_benchmark.c",
	}
	rootEntries, err := os.ReadDir(root)
	if err != nil {
		return nil, fmt.Errorf("read source root: %w", err)
	}
	for _, entry := range rootEntries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".go") {
			paths = append(paths, entry.Name())
		}
	}
	for _, directory := range []string{"internal", "examples", "tools", "integration/p12", "docs/p12"} {
		err := filepath.WalkDir(filepath.Join(root, directory), func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			relative = filepath.ToSlash(relative)
			if entry.IsDir() && relative == releaseArtifactsPath {
				return filepath.SkipDir
			}
			if entry.IsDir() {
				return nil
			}
			if relative == "integration/p12/report.json" || relative == p12fuzzevidence.ReportPath {
				return nil
			}
			if relative == humanReviewPath {
				return nil
			}
			if directory == "internal" || directory == "examples" || directory == "tools" {
				if !strings.HasSuffix(relative, ".go") {
					return nil
				}
			}
			paths = append(paths, relative)
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("walk source artifacts %s: %w", directory, err)
		}
	}
	slices.Sort(paths)
	return paths, nil
}

func validateProbe(transport string, probe probeReport) error {
	if probe.Transport != transport || probe.RequestedDurationNS == 0 || probe.ElapsedNS < probe.RequestedDurationNS || !probe.MonotonicDurationSatisfied || probe.Operations == 0 {
		return fmt.Errorf("%s probe has invalid identity, duration, or operation count", transport)
	}
	for name, counter := range map[string]uint64{"writes": probe.Writes, "append verifications": probe.AppendOnceVerifications, "reads": probe.Reads, "stats": probe.Stats, "removes": probe.Removes} {
		if counter != probe.Operations {
			return fmt.Errorf("%s probe %s counter %d does not equal operations %d", transport, name, counter, probe.Operations)
		}
	}
	if probe.DuplicateMutationsDetected != 0 || probe.Reconnects > probe.Operations || probe.LongestConnectionNS == 0 || probe.LongestConnectionNS > probe.ElapsedNS {
		return fmt.Errorf("%s probe has invalid mutation, reconnect, or connection counters", transport)
	}
	if err := validateCredentialRenewals(transport, probe.CredentialRenewals); err != nil {
		return err
	}
	if probe.InflightMeasurement != "unavailable through the public API; reported as null" {
		return fmt.Errorf("%s probe has invalid inflight evidence", transport)
	}
	if len(probe.Samples) < 2 || len(probe.Samples) > maximumProbeSamples || uint64(len(probe.Samples)) > probe.MaximumConfiguredSampleCount || probe.MaximumConfiguredSampleCount < 2 || probe.MaximumConfiguredSampleCount > maximumProbeSamples {
		return fmt.Errorf("%s probe sample count is outside configured bounds", transport)
	}
	first := probe.Samples[0]
	previousElapsed := uint64(0)
	for index, sample := range probe.Samples {
		if sample.RSSBytes == 0 || sample.HeapBytes == 0 || sample.Goroutines == 0 || sample.Inflight != nil || sample.ElapsedNS > probe.ElapsedNS || index > 0 && sample.ElapsedNS <= previousElapsed {
			return fmt.Errorf("%s probe resource sample %d is invalid or unordered", transport, index)
		}
		if growth(sample.RSSBytes, first.RSSBytes) > maximumProbeRSSGrowth || growth(sample.HeapBytes, first.HeapBytes) > maximumProbeHeapGrowth || growth(sample.Goroutines, first.Goroutines) > maximumProbeGoroutineGrowth {
			return fmt.Errorf("%s probe resource sample %d exceeds an explicit growth ceiling", transport, index)
		}
		previousElapsed = sample.ElapsedNS
	}
	return nil
}

func validateCredentialRenewals(transport string, renewals []credentialRenewal) error {
	hasMonitor, hasOSD := false, false
	previousSessionID := uint64(0)
	lastDueGeneration := make(map[uint64]uint64)
	lastCompletedGeneration := make(map[uint64]uint64)
	sessionServices := make(map[uint64]string)
	for index, renewal := range renewals {
		if renewal.SessionID == 0 || index > 0 && renewal.SessionID < previousSessionID || renewal.DueGeneration == 0 || renewal.DueGeneration >= renewal.CompletedGeneration || renewal.DueAt.IsZero() || renewal.DueAt.After(renewal.CompletedAt) {
			return fmt.Errorf("%s credential renewal %d is invalid or unordered", transport, index)
		}
		if previousDue, exists := lastDueGeneration[renewal.SessionID]; exists && renewal.DueGeneration <= previousDue {
			return fmt.Errorf("%s credential renewal %d does not advance due generation", transport, index)
		}
		if previousCompleted, exists := lastCompletedGeneration[renewal.SessionID]; exists && renewal.CompletedGeneration <= previousCompleted {
			return fmt.Errorf("%s credential renewal %d does not advance session generation", transport, index)
		}
		if service, exists := sessionServices[renewal.SessionID]; exists && service != renewal.Service {
			return fmt.Errorf("%s credential renewal %d changes session service", transport, index)
		}
		switch renewal.Service {
		case "monitor":
			if renewal.ServiceID != 0 {
				return fmt.Errorf("%s monitor renewal has service ID %d", transport, renewal.ServiceID)
			}
			hasMonitor = true
		case "osd":
			if renewal.ServiceID < 0 {
				return fmt.Errorf("%s OSD renewal has service ID %d", transport, renewal.ServiceID)
			}
			hasOSD = true
		default:
			return fmt.Errorf("%s credential renewal has service %q", transport, renewal.Service)
		}
		lastDueGeneration[renewal.SessionID] = renewal.DueGeneration
		lastCompletedGeneration[renewal.SessionID] = renewal.CompletedGeneration
		sessionServices[renewal.SessionID] = renewal.Service
		previousSessionID = renewal.SessionID
	}
	if !hasMonitor || !hasOSD {
		return fmt.Errorf("%s probe lacks completed monitor and OSD credential renewals", transport)
	}
	return nil
}

func growth(current, baseline uint64) uint64 {
	if current <= baseline {
		return 0
	}
	return current - baseline
}

func validateChurn(value report) error {
	churn := value.Churn
	if churn.MonitorRecoveries > churn.MonitorRestarts || churn.OSDRecoveries > churn.OSDRestarts {
		return errors.New("churn recovery counters exceed restart counters")
	}
	final := churn.FinalOSDStat
	if final.Epoch == 0 || final.NumOSDs != 3 || final.NumUpOSDs != 3 || final.NumInOSDs != 3 || final.OSDUpSince == 0 || final.OSDInSince == 0 || final.NumRemappedPGs != 0 {
		return errors.New("final cluster does not have exactly three OSDs up/in and zero remapped PGs")
	}
	if churn.FinalHealth.Status != "HEALTH_OK" || len(churn.FinalHealth.Checks) != 0 || churn.FinalHealth.Mutes == nil {
		return errors.New("final cluster health is not HEALTH_OK")
	}
	return nil
}

func validateCertification(value report) error {
	if value.Command != "./integration/p12/reproduce.sh" {
		return errors.New("candidate report has a non-certifying command")
	}
	for name, probe := range map[string]probeReport{"secure": value.Probe.Secure, "crc": value.Probe.CRC} {
		minimumReconnects := probe.ElapsedNS / uint64(time.Hour)
		if probe.RequestedDurationNS < minimumCertifyingDurationNS || probe.ElapsedNS < minimumCertifyingDurationNS || probe.Reconnects < minimumReconnects || probe.LongestConnectionNS <= minimumLongestConnectionNS {
			return fmt.Errorf("%s probe does not prove 24-hour, reconnect, and long-connection requirements", name)
		}
	}
	if value.Churn.MonitorRestarts < 1 || value.Churn.MonitorRecoveries < 1 || value.Churn.OSDRestarts < 1 || value.Churn.OSDRecoveries < 1 {
		return errors.New("candidate report does not prove monitor and OSD churn with recovery")
	}
	if !value.Benchmark.Performed {
		return errors.New("candidate report did not perform the benchmark")
	}
	if err := validateBenchmark(value.Benchmark.Runs, value.Server.Platform); err != nil {
		return err
	}
	if err := evaluateBenchmarkBudget(value.Benchmark.Runs, approvedBenchmarkBudget); err != nil {
		return fmt.Errorf("benchmark budget: %w", err)
	}
	return nil
}

func validateBenchmark(runs []benchmarkRun, platform string) error {
	if len(runs) != 4 {
		return fmt.Errorf("benchmark has %d runs, want exactly four", len(runs))
	}
	wantArch := strings.TrimPrefix(platform, "linux/")
	seen := make(map[string]bool, 4)
	for _, run := range runs {
		key := run.Implementation + "/" + run.Transport
		if seen[key] || !slices.Contains([]string{"go/secure", "go/crc", "native/secure", "native/crc"}, key) {
			return fmt.Errorf("invalid or duplicate benchmark run %q", key)
		}
		seen[key] = true
		if run.Resources.MaxRSSBytes == 0 || run.Resources.CPUUserNS == 0 && run.Resources.CPUSystemNS == 0 {
			return fmt.Errorf("%s has invalid resource counters", key)
		}
		if run.Implementation == "go" {
			if run.Environment.GOOS == nil || run.Environment.GOARCH == nil || run.Environment.GoVersion == nil || run.Environment.Library != nil || *run.Environment.GOOS != "linux" || *run.Environment.GOARCH != wantArch || strings.TrimSpace(*run.Environment.GoVersion) == "" || run.Resources.Allocations == nil || run.Resources.AllocatedBytes == nil {
				return fmt.Errorf("%s has invalid Go environment or allocation counters", key)
			}
		} else if run.Environment.Library == nil || *run.Environment.Library != "librados.so.2" || run.Environment.GOOS != nil || run.Environment.GOARCH != nil || run.Environment.GoVersion != nil || run.Resources.Allocations != nil || run.Resources.AllocatedBytes != nil {
			return fmt.Errorf("%s has invalid native environment or allocation counters", key)
		}
		if err := validateRows(run); err != nil {
			return fmt.Errorf("%s: %w", key, err)
		}
	}
	return nil
}

func validateRows(run benchmarkRun) error {
	if len(run.Rows) != 36 {
		return fmt.Errorf("has %d rows, want exactly 36", len(run.Rows))
	}
	seen := make(map[string]bool, 36)
	for _, row := range run.Rows {
		if !slices.Contains([]uint64{4096, 65536, 1048576, 4194304}, row.SizeBytes) || !slices.Contains([]int{1, 16, 64}, row.Concurrency) || !slices.Contains([]string{"read", "write", "mixed"}, row.Workload) {
			return fmt.Errorf("invalid matrix coordinate %s", rowKey(row))
		}
		key := rowKey(row)
		if seen[key] {
			return fmt.Errorf("duplicate matrix coordinate %s", key)
		}
		seen[key] = true
		expectedOperations := uint64(2 * row.Concurrency)
		if row.Operations != expectedOperations || row.Bytes != row.SizeBytes*row.Operations || row.ElapsedNS == 0 || !finitePositive(row.ThroughputBytesPerSecond) || !finitePositive(row.IOPS) || row.P50NS == 0 || row.P50NS > row.P95NS || row.P95NS > row.P99NS {
			return fmt.Errorf("invalid counters or metrics at %s", key)
		}
		expectedThroughput := float64(row.Bytes) * 1e9 / float64(row.ElapsedNS)
		expectedIOPS := float64(row.Operations) * 1e9 / float64(row.ElapsedNS)
		if !metricsEqual(row.ThroughputBytesPerSecond, expectedThroughput) || !metricsEqual(row.IOPS, expectedIOPS) {
			return fmt.Errorf("derived metrics disagree with counters at %s", key)
		}
	}
	return nil
}

func metricsEqual(actual, expected float64) bool {
	return math.Abs(actual-expected) <= math.Max(1e-6, math.Abs(expected)*1e-6)
}

func fatalf(format string, values ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", values...)
	os.Exit(1)
}
