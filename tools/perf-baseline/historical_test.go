package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestHistoricalFreezeIgnoresWorktree(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	git, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	gitCommand := func(args ...string) string {
		t.Helper()
		command := exec.Command(git, args...)
		command.Dir = root
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
		return strings.TrimSpace(string(output))
	}
	gitCommand("init", "--quiet")
	for _, name := range historical {
		target := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(target), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(target, []byte("reviewed "+name+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	gitCommand("add", ".")
	gitCommand("-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "-c", "commit.gpgsign=false", "commit", "--quiet", "-m", "reviewed")
	revision := gitCommand("rev-parse", "HEAD")
	for _, name := range historical {
		if err := os.WriteFile(filepath.Join(root, name), []byte("current "+name+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	out, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	worker := &runner{root: root, out: out, git: git}
	if err := worker.freezeHistorical(revision); err != nil {
		t.Fatal(err)
	}
	if worker.report.HistoricalRevision != revision || worker.report.HistoricalSourceRevision != revision {
		t.Fatalf("missing historical revision: %+v", worker.report)
	}
	snapshot, err := worker.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range historical {
		archived, err := regularRead(worker.out, "historical/"+name)
		if err != nil || string(archived) != "reviewed "+name+"\n" {
			t.Fatalf("historical %s: %q, %v", name, archived, err)
		}
		current, err := regularRead(root, name)
		if err != nil || string(current) != "current "+name+"\n" {
			t.Fatalf("current %s: %q, %v", name, current, err)
		}
		found := false
		for _, entry := range snapshot {
			if entry.Path == name {
				found = entry.SHA256 == digest(current) && entry.SHA256 != digest(archived)
			}
		}
		if !found {
			t.Fatalf("current source hash missing for %s", name)
		}
	}
	if err := worker.freezeHistorical(strings.Repeat("0", 40)); err == nil {
		t.Fatal("missing revision accepted")
	}
}
