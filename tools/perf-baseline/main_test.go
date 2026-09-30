package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func benchmarkOutput(count int) []byte {
	var output strings.Builder
	for key := range expectedCells() {
		pkg, name, _ := strings.Cut(key, ":")
		for sample := 0; sample < count; sample++ {
			data, _ := json.Marshal(map[string]string{"Action": "output", "Package": pkg, "Output": fmt.Sprintf("%s-10\t100\t1.5 ns/op\t0 B/op\t0 allocs/op\t1 MB/s\t1 message-sends/op\t1 requests/op\n", name)})
			output.Write(data)
			output.WriteByte('\n')
		}
	}
	for _, pkg := range []string{"maps", "msgr"} {
		fmt.Fprintf(&output, "{\"Action\":\"pass\",\"Package\":\"github.com/otuschhoff/rados-go/internal/%s\"}\n", pkg)
	}
	return []byte(output.String())
}

func TestValidateBenchmarks(t *testing.T) {
	if len(expectedCells()) != 37 {
		t.Fatal("incorrect cell contract")
	}
	valid := benchmarkOutput(5)
	rows, err := validateOutput(valid, true, 5)
	if err != nil || len(rows) != 185 {
		t.Fatalf("rows=%d err=%v", len(rows), err)
	}
	progress, err := json.Marshal(map[string]string{"Action": "output", "Package": "github.com/otuschhoff/rados-go/internal/maps", "Output": "BenchmarkPerformancePlacement\nBenchmarkPerformancePlacement/UnrelatedBuckets=0\n"})
	if err != nil {
		t.Fatal(err)
	}
	withProgress := append(append([]byte{}, valid...), progress...)
	if rows, err := validateOutput(withProgress, true, 5); err != nil || len(rows) != 185 {
		t.Fatalf("progress lines: %d %v", len(rows), err)
	}
	for name, data := range map[string][]byte{
		"missing samples":     benchmarkOutput(4),
		"extra samples":       benchmarkOutput(6),
		"malformed JSON":      append(append([]byte{}, valid...), []byte("garbage")...),
		"skip":                append(append([]byte{}, valid...), []byte("{\"Action\":\"skip\",\"Package\":\"github.com/otuschhoff/rados-go/internal/maps\"}")...),
		"fail":                append(append([]byte{}, valid...), []byte("{\"Action\":\"fail\",\"Package\":\"github.com/otuschhoff/rados-go/internal/maps\"}")...),
		"unexpected":          []byte(strings.Replace(string(valid), "BenchmarkPerformancePlacement", "BenchmarkUnexpected", 1)),
		"NaN":                 []byte(strings.Replace(string(valid), "1.5 ns/op", "NaN ns/op", 1)),
		"missing allocations": []byte(strings.Replace(string(valid), "0 allocs/op", "0 other", 1)),
		"zero time":           []byte(strings.Replace(string(valid), "1.5 ns/op", "0 ns/op", 1)),
		"zero iterations":     []byte(strings.Replace(string(valid), `\t100\t`, `\t0\t`, 1)),
		"negative allocation": []byte(strings.Replace(string(valid), "0 B/op", "-1 B/op", 1)),
		"duplicate metric":    []byte(strings.Replace(string(valid), "0 B/op", "0 ns/op", 1)),
		"unknown action":      append(append([]byte{}, valid...), []byte(`{"Action":"unknown","Package":"github.com/otuschhoff/rados-go/internal/maps"}`)...),
		"missing package":     []byte(strings.Replace(string(valid), `"Action":"pass"`, `"Action":"start"`, 1)),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := validateOutput(data, true, 5); err == nil {
				t.Fatal("invalid output accepted")
			}
		})
	}
}

func TestFreshDirectory(t *testing.T) {
	parent, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Join(parent, "fresh")
	if _, err := freshDirectory(name); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(name)
	if err != nil || info.Mode().Perm() != 0700 {
		t.Fatalf("private mode: %v %v", info, err)
	}
	for _, bad := range []string{name, "relative", parent + "/../unsafe", filepath.Join(parent, "missing", "out")} {
		if _, err := freshDirectory(bad); err == nil {
			t.Fatalf("accepted %s", bad)
		}
	}
	link := filepath.Join(parent, "link")
	if err := os.Symlink(name, link); err != nil {
		t.Fatal(err)
	}
	resolved, err := freshDirectory(filepath.Join(link, "out"))
	if err != nil || resolved != filepath.Join(name, "out") {
		t.Fatalf("canonical parent: %s %v", resolved, err)
	}
	if _, err := freshDirectory(link); err == nil {
		t.Fatal("existing symlink accepted")
	}
}

func TestSourcePolicyAndMutation(t *testing.T) {
	for _, name := range []string{"client.go", "integration/p07/native_benchmark.c", "Makefile", "go.mod", "go.sum", historical[0]} {
		if !relevant(name) {
			t.Fatal(name)
		}
	}
	for _, name := range []string{"client.key", "ceph.keyring", ".env", "secret.json", "random-report.json"} {
		if relevant(name) {
			t.Fatal(name)
		}
	}
	root := "/repo"
	for _, name := range []string{"docs/performance-phase0/evidence/x.go", "out/x.md", "out"} {
		if !excluded(root, "/repo/out", name) {
			t.Fatal(name)
		}
	}
	if excluded(root, "/repo/out", "outside.go") || excluded(root, "/elsewhere/out", "client.go") {
		t.Fatal("prefix overmatched")
	}
	before := []sourceFile{{Path: "x.go", SHA256: digest([]byte("before"))}}
	if err := verifySnapshot(before, before); err != nil {
		t.Fatal(err)
	}
	for _, after := range [][]sourceFile{{{Path: "x.go", SHA256: digest([]byte("after"))}}, {}, {{Path: "x.go", Deleted: true}}} {
		if verifySnapshot(before, after) == nil {
			t.Fatal("mutation accepted")
		}
	}
}

func TestFixtureValidation(t *testing.T) {
	var output strings.Builder
	for key := range expectedFixtures() {
		pkg, name, _ := strings.Cut(key, ":")
		data, _ := json.Marshal(map[string]string{"Action": "pass", "Package": pkg, "Test": name})
		output.Write(data)
		output.WriteByte('\n')
	}
	for _, pkg := range []string{"maps", "msgr"} {
		fmt.Fprintf(&output, "{\"Action\":\"pass\",\"Package\":\"github.com/otuschhoff/rados-go/internal/%s\"}\n", pkg)
	}
	if _, err := validateOutput([]byte(output.String()), false, 1); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{
		strings.Replace(output.String(), "TestPerformancePlacementFixtures", "TestUnexpected", 1),
		strings.Replace(output.String(), "\"pass\"", "\"skip\"", 1),
		strings.Replace(output.String(), "\"pass\"", "\"fail\"", 1),
	} {
		if _, err := validateOutput([]byte(bad), false, 1); err == nil {
			t.Fatal("invalid fixture output accepted")
		}
	}
}

func TestCommandFailureAndPrivateArtifacts(t *testing.T) {
	worker := &runner{root: t.TempDir(), out: t.TempDir()}
	data, err := worker.command("/bin/sh", "-c", "printf raw-out; printf raw-err >&2; exit 7")
	if err == nil || string(data) != "raw-out" {
		t.Fatalf("silent failure: %s %v", data, err)
	}
	record := worker.report.Commands[0]
	if record.ExitCode != 7 || record.Error == "" {
		t.Fatalf("missing exit status: %+v", record)
	}
	stderr, err := os.ReadFile(filepath.Join(worker.out, record.Stderr))
	if err != nil || string(stderr) != "raw-err" || record.StderrSHA256 != digest(stderr) {
		t.Fatal("stderr not preserved")
	}
	info, err := os.Stat(filepath.Join(worker.out, record.Stdout))
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("artifact not private")
	}
	if err := worker.write("../escape", nil); err == nil {
		t.Fatal("artifact traversal accepted")
	}
	if err := worker.write(record.Stdout, nil); err == nil {
		t.Fatal("artifact overwritten")
	}
	if _, err := worker.command(filepath.Join(worker.root, "missing-program")); err == nil || worker.report.Commands[1].ExitCode != -1 {
		t.Fatal("missing command silently accepted")
	}
}

func TestSnapshotGitCoverage(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	git, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"init", "-q", root}, {"-C", root, "config", "user.email", "test@example.invalid"}, {"-C", root, "config", "user.name", "test"}} {
		if output, err := exec.Command(git, args...).CombinedOutput(); err != nil {
			t.Fatalf("%s: %v", output, err)
		}
	}
	for name, content := range map[string]string{"tracked.go": "package main", "untracked.go": "package other", "ignored.go": "ignored", ".gitignore": "ignored.go\nignored-secret.go\n", "ignored-secret.go": "do not copy", "secret.key": "do not copy", "fixture.json": "{}", "fixture.bin": "\x00\xff", "go.sum": "checksums", "LICENSE": "license"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if output, err := exec.Command(git, "-C", root, "add", "tracked.go", ".gitignore", "fixture.json", "fixture.bin", "go.sum", "LICENSE", "secret.key").CombinedOutput(); err != nil {
		t.Fatalf("%s: %v", output, err)
	}
	out := filepath.Join(root, "evidence")
	if err := os.Mkdir(out, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(out, "generated.md"), []byte("excluded"), 0600); err != nil {
		t.Fatal(err)
	}
	worker := &runner{root: root, out: out, git: git}
	before, err := worker.snapshot()
	if err != nil || len(before) != 7 {
		t.Fatalf("snapshot: %+v %v", before, err)
	}
	if err := os.WriteFile(filepath.Join(root, "untracked.go"), []byte("mutation"), 0600); err != nil {
		t.Fatal(err)
	}
	after, err := worker.snapshot()
	if err != nil || verifySnapshot(before, after) == nil {
		t.Fatal("real source mutation not caught")
	}
	if err := os.Symlink("tracked.go", filepath.Join(root, "link.go")); err != nil {
		t.Fatal(err)
	}
	if _, err := worker.snapshot(); err == nil {
		t.Fatal("source symlink accepted")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (transport roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}

func TestNativeProvenance(t *testing.T) {
	spec := nativeSpecs()[0]
	text := strings.Join(spec.symbols, "\n")
	for _, code := range []int{200, 404} {
		worker := &runner{client: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			if !strings.Contains(request.URL.String(), nativeCommit+"/"+spec.path) {
				t.Fatal("unpinned fetch")
			}
			return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader(text))}, nil
		})}}
		source, err := worker.fetchNative(spec)
		if code == 404 {
			if err == nil || source.HTTPStatus != 404 {
				t.Fatal("HTTP failure silently accepted")
			}
			continue
		}
		if err != nil || source.SHA256 != digest([]byte(text)) || source.Symbols[spec.symbols[0]][0] != 1 {
			t.Fatalf("provenance: %+v %v", source, err)
		}
		spec.symbols = append(spec.symbols, "absent")
		if _, err := worker.fetchNative(spec); err == nil {
			t.Fatal("missing native symbol accepted")
		}
		spec.symbols = spec.symbols[:len(spec.symbols)-1]
	}
}

func TestFailureManifest(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(root, "bin")
	if err := os.Mkdir(bin, 0700); err != nil {
		t.Fatal(err)
	}
	gitScript := fmt.Sprintf("#!/bin/sh\nif [ \"$*\" = 'rev-parse --show-toplevel' ]; then printf '%%s\\n' '%s'; exit 0; fi\nprintf git-failed >&2\nexit 7\n", root)
	for name, script := range map[string]string{"git": gitScript, "go": "#!/bin/sh\nexit 1\n"} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(script), 0700); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", bin)
	out := filepath.Join(root, "evidence")
	if err := run(options{out: out, benchtime: "100ms", count: 5}); err == nil {
		t.Fatal("failed command accepted")
	}
	data, err := os.ReadFile(filepath.Join(out, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var report manifest
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatal(err)
	}
	if report.Status != "failed" || report.FailureCount != 1 || len(report.Errors) != 1 || len(report.Commands) != 1 || report.Commands[0].ExitCode != 7 || report.SourceVerified {
		t.Fatalf("incomplete failure evidence: %+v", report)
	}
	stderr, err := os.ReadFile(filepath.Join(out, report.Commands[0].Stderr))
	if err != nil || string(stderr) != "git-failed" {
		t.Fatal("raw failure stderr lost")
	}
	if err := run(options{out: out, benchtime: "100ms", count: 5}); err == nil {
		t.Fatal("existing output accepted")
	}
	after, err := os.ReadFile(filepath.Join(out, "manifest.json"))
	if err != nil || string(after) != string(data) {
		t.Fatal("existing failure evidence overwritten")
	}
}
