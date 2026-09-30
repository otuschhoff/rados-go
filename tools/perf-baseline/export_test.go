package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestCompactEvidenceExport(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	worker := &runner{out: root}
	worker.report = manifest{HistoricalRevision: historicalRevision, HistoricalSourceRevision: historicalRevision, Status: "failed", FailureCount: 1, Errors: []string{"preserved failure"}}
	source := []byte("[]\n")
	worker.report.SourceSHA256 = digest(source)
	if err := worker.artifact("source-manifest.json", source); err != nil {
		t.Fatal(err)
	}
	for _, name := range historical {
		if err := worker.artifact("historical/"+name, []byte("reviewed")); err != nil {
			t.Fatal(err)
		}
	}
	stdout := []byte("all raw repetitions, including failures\n")
	if err := worker.artifact("commands/000.stdout", stdout); err != nil {
		t.Fatal(err)
	}
	worker.report.Commands = []commandRecord{{Args: []string{"/tool/go", "test", "-json"}, Stdout: "commands/000.stdout", StdoutSHA256: digest(stdout), Stderr: "commands/000.stderr", StderrSHA256: digest([]byte("private")), ExitCode: 1}}
	worker.report.Artifacts = append(worker.report.Artifacts, sourceFile{Path: "source/private.go", SHA256: digest([]byte("private"))}, sourceFile{Path: "commands/000.stderr", SHA256: digest([]byte("private"))})
	data, err := json.Marshal(worker.report)
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.write("manifest.json", data); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(root, "compact")
	if err := exportEvidence(root, out); err != nil {
		t.Fatal(err)
	}
	exported, err := regularRead(out, "manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	var report manifest
	if err := json.Unmarshal(exported, &report); err != nil {
		t.Fatal(err)
	}
	if report.Publication == nil || report.Publication.PrivateManifestSHA256 != digest(data) || report.Publication.OmittedArtifactCount != 2 || report.Publication.Omissions == "" || report.Status != "failed" || report.FailureCount != 1 || report.Commands[0].Stderr != "" {
		t.Fatalf("export claims: %+v", report)
	}
	for _, artifact := range report.Artifacts {
		content, err := regularRead(out, artifact.Path)
		if err != nil || digest(content) != artifact.SHA256 {
			t.Fatalf("export artifact %s: %v", artifact.Path, err)
		}
	}
	if _, err := os.Stat(filepath.Join(out, "source")); !os.IsNotExist(err) {
		t.Fatalf("private archive exported: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "commands/000.stdout"), []byte("tampered"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := exportEvidence(root, filepath.Join(root, "tampered-export")); err == nil {
		t.Fatal("tampered raw evidence accepted")
	}
}
