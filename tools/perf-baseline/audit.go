package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
)

func (worker *runner) complete(resultErr error) error {
	if resultErr != nil {
		worker.fail(resultErr)
	}
	worker.report.Finished = time.Now().UTC().Format(time.RFC3339Nano)
	if worker.report.FailureCount == 0 {
		worker.report.Status = "passed"
	}
	data, err := json.MarshalIndent(worker.report, "", "  ")
	if err == nil {
		err = worker.write("manifest.json", append(data, '\n'))
	}
	return errors.Join(resultErr, err)
}

func executableIdentity() (map[string]any, error) {
	name, err := os.Executable()
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(name)
	if err != nil {
		return nil, err
	}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return nil, errors.New("runner build information unavailable")
	}
	return map[string]any{"executable": name, "sha256": digest(data), "build_info": info}, nil
}

func (worker *runner) handoff(opts options, handedOff *bool) error {
	binary := filepath.Join(worker.out, "frozen-runner")
	if _, err := worker.command(worker.goTool, "build", "-mod=readonly", "-trimpath", "-buildvcs=false", "-o", binary, "./tools/perf-baseline"); err != nil {
		return err
	}
	data, err := regularRead(worker.out, "frozen-runner")
	if err != nil {
		return err
	}
	if err := os.Chmod(binary, 0700); err != nil {
		return err
	}
	worker.report.Artifacts = append(worker.report.Artifacts, sourceFile{Path: "frozen-runner", SHA256: digest(data)})
	worker.report.Metadata["frozen_runner_sha256"] = digest(data)
	worker.report.Metadata["identity_scope"] = "Controller hash/build info identifies bootstrap only; fixture commands are issued by the checked worker rebuilt from source/. No exact archived-source claim for controller."
	worker.report.Metadata["go_tool"] = worker.goTool
	worker.report.Metadata["git_tool"] = worker.git
	worker.report.Metadata["worker_args"] = []string{binary, "-frozen-worker", "-out", worker.out, "-count", strconv.Itoa(opts.count), "-benchtime", opts.benchtime}
	state, err := json.Marshal(worker.report)
	if err != nil {
		return err
	}
	if err := worker.artifact("controller-state.json", state); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, binary, "-frozen-worker", "-out", worker.out, "-count", strconv.Itoa(opts.count), "-benchtime", opts.benchtime)
	command.Dir, command.Env = worker.source, worker.environment
	command.Stdout, command.Stderr = os.Stdout, os.Stderr
	command.WaitDelay = 5 * time.Second
	err = command.Run()
	if _, statErr := os.Stat(filepath.Join(worker.out, "manifest.json")); statErr == nil {
		*handedOff = true
	}
	return err
}

func runFrozen(opts options) (resultErr error) {
	data, err := regularRead(opts.out, "controller-state.json")
	if err != nil {
		return err
	}
	worker := &runner{out: opts.out, source: filepath.Join(opts.out, "source")}
	if err := json.Unmarshal(data, &worker.report); err != nil {
		return err
	}
	defer func() { resultErr = worker.complete(resultErr) }()
	worker.report.Artifacts = append(worker.report.Artifacts, sourceFile{Path: "controller-state.json", SHA256: digest(data)})
	worker.root = worker.report.Root
	worker.goTool, _ = worker.report.Metadata["go_tool"].(string)
	worker.git, _ = worker.report.Metadata["git_tool"].(string)
	if worker.goTool == "" || worker.git == "" {
		return errors.New("missing frozen tool identities")
	}
	worker.environment, worker.effective, err = benchmarkEnvironment()
	if err != nil {
		return err
	}
	identity, err := executableIdentity()
	if err != nil {
		return err
	}
	if identity["sha256"] != worker.report.Metadata["frozen_runner_sha256"] {
		return errors.New("frozen worker executable hash mismatch")
	}
	worker.report.Metadata["frozen_worker_identity"] = identity
	worker.report.Metadata["frozen_worker_gomaxprocs"] = runtime.GOMAXPROCS(0)
	if err := worker.verifyFrozen(); err != nil {
		return err
	}
	worker.client = &http.Client{Timeout: 60 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error {
		return errors.New("native source redirects are not allowed")
	}}
	return worker.measure(opts)
}

func secretPath(name string) bool {
	for _, part := range strings.Split(strings.ToLower(name), "/") {
		if part == ".git" || part == ".env" || strings.HasPrefix(part, ".env.") || part == "secrets" || strings.HasSuffix(part, ".key") || strings.HasSuffix(part, ".keyring") || strings.HasSuffix(part, ".pem") || strings.HasSuffix(part, ".p12") || strings.HasSuffix(part, ".pfx") {
			return true
		}
	}
	return false
}

func benchmarkEnvironment() ([]string, map[string]string, error) {
	for _, name := range []string{"GOFLAGS", "GOEXPERIMENT"} {
		if os.Getenv(name) != "" {
			return nil, nil, fmt.Errorf("%s must be empty for audited benchmarks", name)
		}
	}
	effective := map[string]string{
		"GOFLAGS": "", "GOENV": "off", "GOWORK": "off", "CGO_ENABLED": "0", "GOEXPERIMENT": "",
		"GOOS": runtime.GOOS, "GOARCH": runtime.GOARCH, "GOTOOLCHAIN": "go1.27.1",
		"GOGC": "100", "GOMEMLIMIT": "off", "GODEBUG": "", "GOMAXPROCS": strconv.Itoa(runtime.NumCPU()),
	}
	var environment []string
	for _, name := range []string{"PATH", "HOME", "TMPDIR", "SYSTEMROOT", "GOCACHE", "GOMODCACHE", "GOPROXY", "GOSUMDB", "SSL_CERT_FILE", "SSL_CERT_DIR"} {
		if value, ok := os.LookupEnv(name); ok {
			environment = append(environment, name+"="+value)
		}
	}
	for name, value := range effective {
		environment = append(environment, name+"="+value)
	}
	return environment, effective, nil
}

func (worker *runner) freezeSource() error {
	worker.source = filepath.Join(worker.out, "source")
	if err := os.Mkdir(worker.source, 0700); err != nil {
		return err
	}
	for _, entry := range worker.report.Source {
		if entry.Deleted {
			continue
		}
		data, err := regularRead(worker.root, entry.Path)
		if err != nil {
			return err
		}
		if digest(data) != entry.SHA256 {
			return fmt.Errorf("source changed before archive: %s", entry.Path)
		}
		if err := worker.artifact(filepath.Join("source", entry.Path), data); err != nil {
			return err
		}
	}
	return worker.verifyFrozen()
}

func (worker *runner) verifyFrozen() error {
	expected := map[string]string{}
	for _, entry := range worker.report.Source {
		if !entry.Deleted {
			expected[entry.Path] = entry.SHA256
		}
	}
	err := filepath.WalkDir(worker.source, func(target string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		name, err := filepath.Rel(worker.source, target)
		if err != nil {
			return err
		}
		name = filepath.ToSlash(name)
		data, err := regularRead(worker.source, name)
		if err != nil {
			return err
		}
		if expected[name] == "" || digest(data) != expected[name] {
			return fmt.Errorf("frozen source mismatch: %s", name)
		}
		delete(expected, name)
		return nil
	})
	if err != nil {
		return err
	}
	if len(expected) != 0 {
		return errors.New("frozen source files missing")
	}
	return nil
}
