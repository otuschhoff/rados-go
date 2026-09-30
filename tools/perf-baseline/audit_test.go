package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestBenchmarkEnvironment(t *testing.T) {
	t.Setenv("GOEXPERIMENT", "")
	for _, flags := range []string{"-race", "-cover", "-overlay=/tmp/overlay.json", "-modfile=/tmp/go.mod", "-toolexec=external"} {
		t.Setenv("GOFLAGS", flags)
		if _, _, err := benchmarkEnvironment(); err == nil {
			t.Fatalf("accepted %s", flags)
		}
	}
	t.Setenv("GOFLAGS", "")
	t.Setenv("GOGC", "off")
	t.Setenv("GODEBUG", "gctrace=1")
	_, effective, err := benchmarkEnvironment()
	if err != nil || effective["GOGC"] != "100" || effective["GODEBUG"] != "" || effective["GOWORK"] != "off" || effective["GOENV"] != "off" {
		t.Fatalf("environment: %v %v", effective, err)
	}
}

func TestFrozenReconstruction(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	out, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	worker := &runner{root: root, out: out}
	for name, content := range map[string]string{"go.mod": "module fixture\n", "go.sum": "checksum\n", "testdata/input.json": "{}", "testdata/input.bin": "\x00\xff", "LICENSE": "license", "untracked.go": "package fixture"} {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(worker.root, name)), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(worker.root, name), []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		worker.report.Source = append(worker.report.Source, sourceFile{Path: name, SHA256: digest([]byte(content))})
	}
	if err := worker.freezeSource(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(worker.root, "untracked.go"), []byte("changed live checkout"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := worker.verifyFrozen(); err != nil {
		t.Fatal(err)
	}
	for _, entry := range worker.report.Source {
		info, err := os.Stat(filepath.Join(worker.source, entry.Path))
		if err != nil || info.Mode().Perm() != 0600 {
			t.Fatalf("private archive: %v %v", info, err)
		}
	}
	if err := os.WriteFile(filepath.Join(worker.source, "untracked.go"), []byte("mutation"), 0600); err != nil {
		t.Fatal(err)
	}
	if worker.verifyFrozen() == nil {
		t.Fatal("frozen mutation accepted")
	}
}

func TestSecretPaths(t *testing.T) {
	for _, name := range []string{".env", ".env.local", "secrets/config.go", "config.key", "ceph.keyring", "cert.pem", "secret.p12", "secret.pfx", ".git/config"} {
		if !secretPath(name) {
			t.Fatalf("secret accepted: %s", name)
		}
	}
	for _, name := range []string{"go.sum", "testdata/fixture.bin", "testdata/fixture.json", ".gitignore", "LICENSE"} {
		if secretPath(name) {
			t.Fatalf("source excluded: %s", name)
		}
	}
}

func TestFrozenCommandsIgnoreLiveCheckout(t *testing.T) {
	t.Setenv("GOFLAGS", "")
	t.Setenv("GOEXPERIMENT", "")
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	out, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	goTool, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	worker := &runner{root: root, out: out, goTool: goTool}
	worker.environment, worker.effective, err = benchmarkEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	contents := map[string]string{
		"go.mod":             "module frozenfixture\n\ngo 1.26.8\n",
		"fixture_test.go":    "package frozenfixture\nimport (\"os\"; \"testing\")\nfunc TestArchived(t *testing.T) { data, err := os.ReadFile(\"testdata/input.bin\"); if err != nil || string(data) != \"frozen\" { t.Fatal(string(data), err) } }\n",
		"testdata/input.bin": "frozen",
	}
	for name, content := range contents {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(root, name)), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, name), []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		worker.report.Source = append(worker.report.Source, sourceFile{Path: name, SHA256: digest([]byte(content))})
	}
	if err := worker.freezeSource(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "fixture_test.go"), []byte("invalid live checkout"), 0600); err != nil {
		t.Fatal(err)
	}
	if output, err := worker.command(goTool, "test", "-mod=readonly", "-count=1", "./..."); err != nil {
		t.Fatalf("frozen test: %s %v", output, err)
	}
	record := worker.report.Commands[0]
	if record.Directory != worker.source || record.Environment["GOWORK"] != "off" || record.Environment["GOFLAGS"] != "" {
		t.Fatalf("command escaped snapshot: %+v", record)
	}
	if err := worker.verifyFrozen(); err != nil {
		t.Fatal(err)
	}
}
