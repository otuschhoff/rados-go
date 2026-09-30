package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

const nativeCommit = "7f793731f1b39eb4f465e960113d2363c311b964"
const historicalRevision = "dde29cde727dc963238acc4fa13a5a277a9f5c80"

var packages = []string{"./internal/maps", "./internal/msgr"}
var historical = []string{"docs/p07/integration-report.json", "integration/p07/DIAGNOSTICS.md", "docs/p12/performance.md"}

type options struct {
	out, benchtime string
	count          int
}

type commandRecord struct {
	Args         []string          `json:"args"`
	Environment  map[string]string `json:"environment,omitempty"`
	Directory    string            `json:"directory"`
	Started      string            `json:"started_at"`
	DurationNS   int64             `json:"duration_ns"`
	ExitCode     int               `json:"exit_code"`
	Error        string            `json:"error,omitempty"`
	Stdout       string            `json:"stdout_file"`
	Stderr       string            `json:"stderr_file"`
	StdoutSHA256 string            `json:"stdout_sha256"`
	StderrSHA256 string            `json:"stderr_sha256"`
}

type sourceFile struct {
	Path    string `json:"path"`
	SHA256  string `json:"sha256,omitempty"`
	Deleted bool   `json:"deleted,omitempty"`
}

type benchmark struct {
	Package    string             `json:"package"`
	Name       string             `json:"name"`
	Iterations uint64             `json:"iterations"`
	Metrics    map[string]float64 `json:"metrics"`
}

type nativeSource struct {
	Path       string           `json:"path"`
	URL        string           `json:"url"`
	Commit     string           `json:"commit"`
	SHA256     string           `json:"sha256"`
	HTTPStatus int              `json:"http_status"`
	Symbols    map[string][]int `json:"symbol_lines"`
	Statements []string         `json:"statements"`
}

type manifest struct {
	Publication              *publicationRecord `json:"publication,omitempty"`
	HistoricalRevision       string             `json:"historical_revision"`
	HistoricalSourceRevision string             `json:"historical_source_revision"`
	SchemaVersion            int                `json:"schema_version"`
	Status                   string             `json:"status"`
	Started                  string             `json:"started_at"`
	Finished                 string             `json:"finished_at"`
	Root                     string             `json:"repository"`
	HEAD                     string             `json:"git_head"`
	Dirty                    bool               `json:"dirty"`
	Source                   []sourceFile       `json:"source_files"`
	SourceSHA256             string             `json:"source_manifest_sha256"`
	SourceVerified           bool               `json:"source_verified_after_commands"`
	Artifacts                []sourceFile       `json:"artifacts"`
	Commands                 []commandRecord    `json:"commands"`
	FailureCount             int                `json:"failure_count"`
	Errors                   []string           `json:"errors"`
	Benchmarks               []benchmark        `json:"benchmarks"`
	Native                   []nativeSource     `json:"native_sources"`
	Metadata                 map[string]any     `json:"metadata"`
	Scopes                   map[string]string  `json:"scopes"`
	Limits                   map[string]any     `json:"limits"`
}

type runner struct {
	root, out, git, goTool string
	source                 string
	environment            []string
	effective              map[string]string
	report                 manifest
	client                 *http.Client
}

func main() {
	var opts options
	var frozenWorker bool
	var exportFrom string
	flag.StringVar(&exportFrom, "export-from", "", "export compact evidence from an existing private capture to -out; requires publication review")
	flag.BoolVar(&frozenWorker, "frozen-worker", false, "internal frozen snapshot worker")
	flag.StringVar(&opts.out, "out", "", "fresh absolute directory (parent must exist)")
	flag.StringVar(&opts.benchtime, "benchtime", "100ms", "positive benchmark duration")
	flag.IntVar(&opts.count, "count", 5, "repetitions per cell")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "unexpected positional arguments")
		os.Exit(1)
	}
	var err error
	if exportFrom != "" && frozenWorker {
		err = errors.New("export-from cannot be combined with frozen-worker")
	} else if exportFrom != "" {
		err = exportEvidence(exportFrom, opts.out)
	} else if frozenWorker {
		err = runFrozen(opts)
	} else {
		err = run(opts)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(opts options) (resultErr error) {
	duration, err := time.ParseDuration(opts.benchtime)
	if err != nil || duration <= 0 || opts.count < 1 || opts.count > 100 {
		return errors.New("benchtime must be a positive duration; count must be 1..100")
	}
	environment, effective, err := benchmarkEnvironment()
	if err != nil {
		return err
	}
	git, err := exec.LookPath("git")
	if err != nil {
		return err
	}
	goTool, err := exec.LookPath("go")
	if err != nil {
		return err
	}
	rootOutput, err := exec.Command(git, "rev-parse", "--show-toplevel").Output()
	if err != nil {
		return fmt.Errorf("find repository: %w", err)
	}
	root, err := filepath.EvalSymlinks(strings.TrimSpace(string(rootOutput)))
	if err != nil {
		return err
	}
	out, err := freshDirectory(opts.out)
	if err != nil {
		return err
	}
	worker := &runner{root: root, out: out, git: git, goTool: goTool, client: &http.Client{Timeout: 60 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error {
		return errors.New("native source redirects are not allowed")
	}}}
	worker.environment, worker.effective = environment, effective
	worker.report = manifest{SchemaVersion: 1, Status: "failed", Started: time.Now().UTC().Format(time.RFC3339Nano), Root: root,
		Metadata: map[string]any{"goos": runtime.GOOS, "goarch": runtime.GOARCH, "runner_go_version": runtime.Version(), "logical_cpus": runtime.NumCPU(), "runner_gomaxprocs": runtime.GOMAXPROCS(0), "benchtime": opts.benchtime, "count": opts.count},
		Scopes: map[string]string{
			"Placement":        "PlaceRawHash on immutable synthetic maps: 4100 OSDs, 3 plus 0/64/1024/4096 unrelated buckets; includes topology decode/validation and placement.",
			"Incremental":      "Pool rename with independent cloned state; OSDs=4/64/1024/4096 or override PGs=64/1024/4096 (five tables); not a live cluster update.",
			"Queue":            "Full depth=1/64/1024/4096 batch, timed refill plus reply completion or failAll and result drain; no pumps, encoding, network or auth.",
			"SubmissionReplay": "CRC/secure, 4096/65536/4194304 bytes, initial or initial+replay lifecycle including creation, admission, pumps, wire encode, peer validation, ACK and Stop; no sockets/auth, inbound wire decode or OSD processing. MB/s counts outbound application payload across sends, not wire bytes.",
			"IdleConnections":  "CRC/secure batches=1/16/128: creation/readiness/snapshot/Stop, 512KiB reader per connection, queue/in-flight=64/events=64. B/op is lifecycle allocation; retained-heap-B/batch is separate noisy post-GC live heap, not RSS, stacks or harness-subtracted memory.",
			"Historical":       "Frozen P07/P12 evidence is historical, not regenerated or certified. P07 throughput/latency and whole-process CPU/RSS have different scopes from these fixture benchmarks; CRC labels do not prove matched negotiated modes.",
			"Native":           "Pinned native source symbol references only, no native runtime measurement, parity, lock-free or allocation-free claim.",
			"Writes":           "Only the fresh output directory is written by the runner; Go may write its normal build/module caches. No parent docs or historical inputs are rewritten.",
		},
		Limits: map[string]any{"expected_cells": 37, "categories": []string{"Placement", "Incremental", "Queue", "SubmissionReplay", "IdleConnections"}, "command_timeout": "30m", "native_fetch_timeout": "60s", "native_max_bytes": 8 << 20, "source_policy": "All Git selected tracked files (including fixtures/configs/LICENSE), plus nonignored untracked .go/.c/.sh/.md, go.mod/go.sum/Makefile and historical report; excludes output prefix and docs/performance-phase0/evidence; no symlinks or secret paths; private checked source/ archive", "certification": false, "cpu_rss_measured": false,
			"messenger_fixture":   map[string]any{"max_segment_bytes": 8 << 20, "max_frame_bytes": 16 << 20, "max_addresses": 4, "max_auth_bytes": 64, "max_queued_messages": "depth (queue), 8 (submission), 64 (idle)", "max_in_flight_transactions": "same as max_queued_messages", "max_retained_bytes": "max_queued_messages * 8MiB", "max_reconnect_attempts": 2, "max_handshake_transitions": 8, "event_buffer": 64, "reconnect_wait": "disabled by fixture", "submission_timeout": "10s"},
			"placement_fixture":   map[string]any{"max_bytes": "encoded topology length", "max_buckets": "3 + unrelated buckets", "max_rules": 1, "max_items": 4100, "max_names": 1},
			"incremental_fixture": map[string]any{"max_bytes": "topology bytes + OSDs*128 + override PGs*256 + 4096", "max_pools": 1, "max_osds": "fixture OSD count", "max_addresses": 1, "max_pg_mappings": "max(override PGs, 1)", "max_collection_entries": 4096},
		},
	}
	handedOff := false
	defer func() {
		if !handedOff {
			resultErr = worker.complete(resultErr)
		}
	}()
	identity, err := executableIdentity()
	if err != nil {
		return err
	}
	worker.report.Metadata["controller_identity"] = identity
	worker.report.Metadata["benchmark_environment"] = effective
	head, err := worker.command(git, "rev-parse", "HEAD")
	if err != nil {
		return err
	}
	worker.report.HEAD = strings.TrimSpace(string(head))
	status, err := worker.command(git, "status", "--porcelain=v1", "-z", "--untracked-files=all")
	if err != nil {
		return err
	}
	worker.report.Dirty = len(status) != 0
	worker.report.Source, err = worker.snapshot()
	if err != nil {
		return err
	}
	sourceData, err := json.MarshalIndent(worker.report.Source, "", "  ")
	if err != nil {
		return err
	}
	if err := worker.artifact("source-manifest.json", append(sourceData, '\n')); err != nil {
		return err
	}
	worker.report.SourceSHA256 = digest(append(sourceData, '\n'))
	if err := worker.freezeSource(); err != nil {
		return err
	}
	paths := []string{"diff", "HEAD", "--binary", "--no-ext-diff", "--no-textconv", "--"}
	for _, entry := range worker.report.Source {
		paths = append(paths, entry.Path)
	}
	diff, err := worker.command(git, paths...)
	if err != nil {
		return err
	}
	if err := worker.artifact("git-diff.binary.patch", diff); err != nil {
		return err
	}
	if err := worker.freezeHistorical(historicalRevision); err != nil {
		return err
	}
	return worker.handoff(opts, &handedOff)
}

func (worker *runner) freezeHistorical(revision string) error {
	worker.report.HistoricalRevision = revision
	worker.report.HistoricalSourceRevision = revision
	for _, name := range historical {
		data, err := worker.command(worker.git, "show", revision+":"+name)
		if err != nil {
			return fmt.Errorf("freeze historical %s at %s: %w", name, revision, err)
		}
		if err := worker.artifact(filepath.Join("historical", name), data); err != nil {
			return err
		}
	}
	return nil
}

func (worker *runner) measure(opts options) error {
	goTool, git := worker.goTool, worker.git
	commands := [][]string{{"uname", "-a"}, {"hostname"}, {goTool, "version"}, {goTool, "env", "-json", "GOOS", "GOARCH", "GOVERSION", "GOTOOLCHAIN", "CGO_ENABLED", "GOAMD64", "GOARM64"}}
	if runtime.GOOS == "darwin" {
		commands = append(commands, []string{"sysctl", "hw.model", "machdep.cpu.brand_string", "hw.ncpu", "hw.physicalcpu", "hw.logicalcpu", "hw.memsize"})
	} else if runtime.GOOS == "linux" {
		commands = append(commands, []string{"lscpu"}, []string{"cat", "/proc/meminfo"})
	}
	for _, args := range commands {
		if _, err := worker.command(args[0], args[1:]...); err != nil {
			return err
		}
	}
	fixtureArgs := append([]string{"test", "-mod=readonly", "-json", "-count=1", "-run", "^TestPerformance"}, packages...)
	fixtureOutput, err := worker.command(goTool, fixtureArgs...)
	if err != nil {
		return err
	}
	if _, err := validateOutput(fixtureOutput, false, 1); err != nil {
		return err
	}
	benchArgs := append([]string{"test", "-mod=readonly", "-json", "-run", "^$", "-bench", "^BenchmarkPerformance", "-benchmem", "-count", strconv.Itoa(opts.count), "-benchtime", opts.benchtime}, packages...)
	benchOutput, err := worker.command(goTool, benchArgs...)
	if err != nil {
		return err
	}
	worker.report.Benchmarks, err = validateOutput(benchOutput, true, opts.count)
	if err != nil {
		return err
	}
	for _, spec := range nativeSpecs() {
		source, err := worker.fetchNative(spec)
		worker.report.Native = append(worker.report.Native, source)
		if err != nil {
			return err
		}
	}
	if err := worker.verifyFrozen(); err != nil {
		return err
	}
	after, err := worker.snapshot()
	if err != nil {
		return err
	}
	if err := verifySnapshot(worker.report.Source, after); err != nil {
		return err
	}
	headAfter, err := worker.command(git, "rev-parse", "HEAD")
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(headAfter)) != worker.report.HEAD {
		return errors.New("Git HEAD changed during evidence run")
	}
	worker.report.SourceVerified = true
	return nil
}

func freshDirectory(name string) (string, error) {
	if !filepath.IsAbs(name) || filepath.Clean(name) != name {
		return "", errors.New("out must be a clean absolute nonexisting directory")
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(name))
	if err != nil {
		return "", fmt.Errorf("output parent: %w", err)
	}
	name = filepath.Join(parent, filepath.Base(name))
	if err := os.Mkdir(name, 0700); err != nil {
		return "", fmt.Errorf("create fresh output: %w", err)
	}
	return name, nil
}

func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func (worker *runner) write(name string, data []byte) error {
	if filepath.IsAbs(name) || filepath.Clean(name) != name || name == ".." || strings.HasPrefix(name, ".."+string(filepath.Separator)) {
		return errors.New("unsafe artifact path")
	}
	target := filepath.Join(worker.out, name)
	if err := os.MkdirAll(filepath.Dir(target), 0700); err != nil {
		return err
	}
	file, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(data)
	return errors.Join(writeErr, file.Close())
}

func (worker *runner) artifact(name string, data []byte) error {
	if err := worker.write(name, data); err != nil {
		return err
	}
	worker.report.Artifacts = append(worker.report.Artifacts, sourceFile{Path: filepath.ToSlash(name), SHA256: digest(data)})
	return nil
}

func (worker *runner) fail(err error) {
	worker.report.FailureCount++
	worker.report.Errors = append(worker.report.Errors, err.Error())
}

func (worker *runner) command(program string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	start := time.Now()
	command := exec.CommandContext(ctx, program, args...)
	command.Dir = worker.root
	if program == worker.goTool && worker.source != "" {
		command.Dir = worker.source
		command.Env = worker.environment
	}
	command.WaitDelay = 5 * time.Second
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	err := command.Run()
	index := len(worker.report.Commands)
	record := commandRecord{Args: append([]string{program}, args...), Directory: command.Dir, Started: start.UTC().Format(time.RFC3339Nano), DurationNS: time.Since(start).Nanoseconds(), ExitCode: 0,
		Stdout: fmt.Sprintf("commands/%03d.stdout", index), Stderr: fmt.Sprintf("commands/%03d.stderr", index), StdoutSHA256: digest(stdout.Bytes()), StderrSHA256: digest(stderr.Bytes())}
	if program == worker.goTool {
		record.Environment = worker.effective
	}
	if err != nil {
		record.ExitCode = -1
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			record.ExitCode = exit.ExitCode()
		}
		record.Error = err.Error()
	}
	worker.report.Commands = append(worker.report.Commands, record)
	writeErr := errors.Join(worker.artifact(record.Stdout, stdout.Bytes()), worker.artifact(record.Stderr, stderr.Bytes()))
	if err != nil {
		err = fmt.Errorf("command %q failed (exit %d): %w", record.Args, record.ExitCode, err)
	}
	return stdout.Bytes(), errors.Join(err, writeErr)
}

func relevant(name string) bool {
	base := filepath.Base(name)
	if base == "go.mod" || base == "go.sum" || base == "Makefile" || name == historical[0] {
		return true
	}
	switch filepath.Ext(name) {
	case ".go", ".c", ".sh", ".md":
		return true
	}
	return false
}

func excluded(root, out, name string) bool {
	if name == "docs/performance-phase0/evidence" || strings.HasPrefix(name, "docs/performance-phase0/evidence/") {
		return true
	}
	rel, err := filepath.Rel(root, out)
	if err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		prefix := filepath.ToSlash(rel)
		return name == prefix || strings.HasPrefix(name, prefix+"/")
	}
	return false
}

func regularRead(root, name string) ([]byte, error) {
	if filepath.IsAbs(name) || filepath.ToSlash(filepath.Clean(name)) != name || name == ".." || strings.HasPrefix(name, "../") {
		return nil, errors.New("unsafe source path")
	}
	target := filepath.Join(root, filepath.FromSlash(name))
	real, err := filepath.EvalSymlinks(target)
	if err != nil {
		return nil, err
	}
	if real != target {
		return nil, errors.New("source symlinks are not allowed")
	}
	info, err := os.Lstat(target)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("source must be regular file")
	}
	return os.ReadFile(target)
}

func (worker *runner) snapshot() ([]sourceFile, error) {
	tracked, err := worker.command(worker.git, "ls-files", "-z", "--cached")
	if err != nil {
		return nil, err
	}
	untracked, err := worker.command(worker.git, "ls-files", "-z", "--others", "--exclude-standard")
	if err != nil {
		return nil, err
	}
	trackedNames := map[string]bool{}
	for _, name := range strings.Split(string(tracked), "\x00") {
		trackedNames[name] = true
	}
	data := append(tracked, untracked...)
	seen := map[string]bool{}
	var files []sourceFile
	for _, name := range strings.Split(string(data), "\x00") {
		if name == "" || seen[name] || (!trackedNames[name] && !relevant(name)) || secretPath(name) || excluded(worker.root, worker.out, name) {
			continue
		}
		seen[name] = true
		content, err := regularRead(worker.root, name)
		entry := sourceFile{Path: name}
		if errors.Is(err, os.ErrNotExist) {
			entry.Deleted = true
		} else if err != nil {
			return nil, fmt.Errorf("hash %s: %w", name, err)
		} else {
			entry.SHA256 = digest(content)
		}
		files = append(files, entry)
	}
	sort.Slice(files, func(left, right int) bool { return files[left].Path < files[right].Path })
	if len(files) == 0 {
		return nil, errors.New("empty source manifest")
	}
	return files, nil
}

func verifySnapshot(before, after []sourceFile) error {
	if !reflect.DeepEqual(before, after) {
		return errors.New("source manifest changed during evidence run")
	}
	return nil
}

func expectedCells() map[string]string {
	cells := map[string]string{}
	add := func(pkg, name string) {
		cells[pkg+":"+name] = strings.Split(strings.TrimPrefix(name, "BenchmarkPerformance"), "/")[0]
	}
	maps := "github.com/otuschhoff/rados-go/internal/maps"
	msgr := "github.com/otuschhoff/rados-go/internal/msgr"
	for _, count := range []int{0, 64, 1024, 4096} {
		add(maps, fmt.Sprintf("BenchmarkPerformancePlacement/UnrelatedBuckets=%d", count))
	}
	for _, count := range []int{4, 64, 1024, 4096} {
		add(maps, fmt.Sprintf("BenchmarkPerformanceIncremental/OSDs=%d/OverridePGs=0", count))
	}
	for _, count := range []int{64, 1024, 4096} {
		add(maps, fmt.Sprintf("BenchmarkPerformanceIncremental/OSDs=4/OverridePGs=%d", count))
	}
	for _, depth := range []int{1, 64, 1024, 4096} {
		for _, operation := range []string{"complete", "drain"} {
			add(msgr, fmt.Sprintf("BenchmarkPerformanceQueue/depth=%d/%s", depth, operation))
		}
	}
	for _, mode := range []string{"crc", "secure"} {
		for _, size := range []int{4096, 65536, 4194304} {
			for _, path := range []string{"initial", "initial+replay"} {
				add(msgr, fmt.Sprintf("BenchmarkPerformanceSubmissionReplay/%s/bytes=%d/%s", mode, size, path))
			}
		}
		for _, count := range []int{1, 16, 128} {
			add(msgr, fmt.Sprintf("BenchmarkPerformanceIdleConnections/%s/connections=%d", mode, count))
		}
	}
	return cells
}

func expectedFixtures() map[string]bool {
	fixtures := map[string]bool{}
	for key := range expectedCells() {
		key = strings.Replace(key, "BenchmarkPerformancePlacement", "TestPerformancePlacementFixtures", 1)
		key = strings.Replace(key, "BenchmarkPerformanceIncremental", "TestPerformanceIncrementalFixtures", 1)
		key = strings.Replace(key, "BenchmarkPerformanceQueue", "TestPerformanceQueueFixture", 1)
		if strings.HasSuffix(key, "/complete") {
			key = strings.TrimSuffix(key, "/complete") + "/drain=false"
		} else if strings.HasSuffix(key, "/drain") {
			key = strings.TrimSuffix(key, "/drain") + "/drain=true"
		}
		key = strings.Replace(key, "BenchmarkPerformanceSubmissionReplay", "TestPerformanceSubmissionReplayFixture", 1)
		key = strings.Replace(key, "/initial+replay", "/replay=true", 1)
		key = strings.Replace(key, "/initial", "/replay=false", 1)
		key = strings.Replace(key, "BenchmarkPerformanceIdleConnections", "TestPerformanceIdleConnectionsFixture", 1)
		key = strings.Replace(key, "/connections=", "/count=", 1)
		fixtures[key] = true
		parts := strings.SplitN(key, ":", 2)
		fixtures[parts[0]+":"+strings.Split(parts[1], "/")[0]] = true
	}
	return fixtures
}

var cpuSuffix = regexp.MustCompile(`-[0-9]+$`)

func validateOutput(data []byte, benchmarks bool, count int) ([]benchmark, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	packagePass := map[string]int{}
	testPass := map[string]int{}
	outputs := map[string]*strings.Builder{}
	validPackages := map[string]bool{"github.com/otuschhoff/rados-go/internal/maps": true, "github.com/otuschhoff/rados-go/internal/msgr": true}
	for {
		var event struct{ Action, Package, Test, Output string }
		if err := decoder.Decode(&event); err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("invalid Go test JSON: %w", err)
		}
		if !validPackages[event.Package] {
			return nil, fmt.Errorf("unexpected package %q", event.Package)
		}
		switch event.Action {
		case "fail", "skip":
			return nil, fmt.Errorf("test action %s: %s %s", event.Action, event.Package, event.Test)
		case "pass":
			if event.Test == "" {
				packagePass[event.Package]++
			} else {
				testPass[event.Package+":"+event.Test]++
			}
		case "output":
			if outputs[event.Package] == nil {
				outputs[event.Package] = &strings.Builder{}
			}
			outputs[event.Package].WriteString(event.Output)
		case "start", "run", "pause", "cont", "bench":
		default:
			return nil, fmt.Errorf("unexpected test action %q", event.Action)
		}
	}
	for pkg := range validPackages {
		if packagePass[pkg] != 1 {
			return nil, fmt.Errorf("expected one package pass for %s, got %d", pkg, packagePass[pkg])
		}
	}
	if !benchmarks {
		fixtures := expectedFixtures()
		for key := range fixtures {
			if testPass[key] != 1 {
				return nil, fmt.Errorf("missing fixture pass %s", key)
			}
		}
		for key := range testPass {
			if !fixtures[key] {
				return nil, fmt.Errorf("unexpected fixture %s", key)
			}
		}
		return nil, nil
	}
	expected, seen, categories := expectedCells(), map[string]int{}, map[string]bool{}
	var rows []benchmark
	for pkg, output := range outputs {
		scanner := bufio.NewScanner(strings.NewReader(output.String()))
		scanner.Buffer(make([]byte, 4096), 1<<20)
		for scanner.Scan() {
			fields := strings.Fields(scanner.Text())
			if len(fields) == 0 || !strings.HasPrefix(fields[0], "Benchmark") {
				continue
			}
			name := cpuSuffix.ReplaceAllString(fields[0], "")
			key := pkg + ":" + name
			category, ok := expected[key]
			if !ok && len(fields) == 1 {
				for cell := range expected {
					if strings.HasPrefix(cell, key+"/") {
						ok = true
						break
					}
				}
			}
			if !ok {
				return nil, fmt.Errorf("unexpected benchmark %s", key)
			}
			if len(fields) == 1 {
				continue
			}
			if len(fields) < 8 || len(fields)%2 != 0 {
				return nil, fmt.Errorf("malformed benchmark row %s", key)
			}
			iterations, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil || iterations == 0 {
				return nil, fmt.Errorf("invalid iterations %s", key)
			}
			row := benchmark{Package: pkg, Name: name, Iterations: iterations, Metrics: map[string]float64{}}
			for index := 2; index < len(fields); index += 2 {
				value, err := strconv.ParseFloat(fields[index], 64)
				unit := fields[index+1]
				if _, duplicate := row.Metrics[unit]; duplicate || err != nil || math.IsNaN(value) || math.IsInf(value, 0) || (value < 0 && unit != "retained-heap-B/batch") {
					return nil, fmt.Errorf("invalid metric %s %s", key, unit)
				}
				row.Metrics[unit] = value
			}
			for _, unit := range []string{"ns/op", "B/op", "allocs/op"} {
				if _, ok := row.Metrics[unit]; !ok {
					return nil, fmt.Errorf("missing %s for %s", unit, key)
				}
			}
			if row.Metrics["ns/op"] <= 0 {
				return nil, fmt.Errorf("nonpositive time %s", key)
			}
			if category == "SubmissionReplay" {
				for _, unit := range []string{"MB/s", "message-sends/op", "requests/op"} {
					if row.Metrics[unit] <= 0 {
						return nil, fmt.Errorf("missing or nonpositive %s for %s", unit, key)
					}
				}
			}
			seen[key]++
			categories[category] = true
			rows = append(rows, row)
		}
		if err := scanner.Err(); err != nil {
			return nil, err
		}
	}
	for key := range expected {
		if seen[key] != count {
			return nil, fmt.Errorf("benchmark %s: expected %d samples, got %d", key, count, seen[key])
		}
	}
	if len(categories) != 5 {
		return nil, errors.New("expected exactly five benchmark categories")
	}
	sort.SliceStable(rows, func(left, right int) bool {
		return rows[left].Package+rows[left].Name < rows[right].Package+rows[right].Name
	})
	return rows, nil
}

type nativeSpec struct {
	path       string
	symbols    []string
	statements []string
}

func nativeSpecs() []nativeSpec {
	return []nativeSpec{
		{"src/osdc/Objecter.cc", []string{"_calc_target", "lookup_pg_mapping", "update_pg_mapping", "_send_op", "handle_osd_backoff"}, []string{"Objecter.cc contains target calculation, PG mapping lookup/update, operation send and OSD backoff handling symbols; references do not measure runtime costs."}},
		{"src/osd/OSDMap.cc", []string{"_pg_to_raw_osds", "crush->do_rule", "apply_incremental"}, []string{"OSDMap.cc contains raw PG placement, a CRUSH do_rule call and incremental map application symbols."}},
		{"src/msg/async/ProtocolV2.cc", []string{"handle_auth_done", "ready", "auth_meta->con_mode", "ceph_con_mode_name"}, []string{"ProtocolV2.cc references authentication completion, readiness, connection mode metadata and connection mode naming; benchmark labels alone do not establish negotiated mode."}},
	}
}

func (worker *runner) fetchNative(spec nativeSpec) (nativeSource, error) {
	source := nativeSource{Path: spec.path, Commit: nativeCommit, URL: "https://raw.githubusercontent.com/ceph/ceph/" + nativeCommit + "/" + spec.path, Symbols: map[string][]int{}, Statements: spec.statements}
	response, err := worker.client.Get(source.URL)
	if err != nil {
		return source, fmt.Errorf("fetch %s: %w", spec.path, err)
	}
	defer response.Body.Close()
	source.HTTPStatus = response.StatusCode
	if response.StatusCode != http.StatusOK {
		return source, fmt.Errorf("fetch %s: HTTP %d", spec.path, response.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, (8<<20)+1))
	if err != nil {
		return source, err
	}
	if len(data) > 8<<20 {
		return source, errors.New("native source exceeds size limit")
	}
	source.SHA256 = digest(data)
	patterns := make(map[string]*regexp.Regexp, len(spec.symbols))
	for _, symbol := range spec.symbols {
		patterns[symbol] = regexp.MustCompile(`\b` + regexp.QuoteMeta(symbol) + `\b`)
	}
	for index, line := range strings.Split(string(data), "\n") {
		for _, symbol := range spec.symbols {
			if patterns[symbol].MatchString(line) {
				source.Symbols[symbol] = append(source.Symbols[symbol], index+1)
			}
		}
	}
	for _, symbol := range spec.symbols {
		if len(source.Symbols[symbol]) == 0 {
			return source, fmt.Errorf("native source %s missing symbol %s", spec.path, symbol)
		}
	}
	return source, nil
}
