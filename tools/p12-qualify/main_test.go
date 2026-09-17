package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSourceArtifactsRejectsGeneratedReportsAndChangesWithSource(t *testing.T) {
	root := t.TempDir()
	for path, content := range map[string]string{
		".git/objects/generated":                      "git metadata",
		".github/workflows/p00.yml":                   "name: release control\n",
		".gitignore":                                  "dist/\n",
		"main.go":                                     "package sample\n",
		"docs/p12/fuzz-report.json":                   "generated fuzz evidence",
		"docs/p12/qualification-report.json":          "generated",
		"docs/p12/qualification-report.json.backup":   "source",
		"docs/p12/human-review.json":                  "generated review evidence",
		"docs/p12/reviewer-trust.json":                "pending trust policy",
		"integration/p12/reviewer-trust.schema.json":  "trust schema",
		"docs/p12/release-artifacts/archive":          "generated release evidence",
		"docs/p12/release-artifacts-copy/archive":     "source",
		"integration/p12/report.json":                 "generated",
		"integration/p12/report.json.release-control": "source",
	} {
		fullPath := filepath.Join(root, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(fullPath, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	first, err := sourceArtifacts(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{".github/workflows/p00.yml", ".gitignore", "main.go", "docs/p12/qualification-report.json.backup", "docs/p12/reviewer-trust.json", "docs/p12/release-artifacts-copy/archive", "integration/p12/report.json.release-control", "integration/p12/reviewer-trust.schema.json"} {
		if first[path] == "" {
			t.Fatalf("release-control source %q was excluded: %#v", path, first)
		}
	}
	for _, path := range []string{".git/objects/generated", "docs/p12/fuzz-report.json", "docs/p12/qualification-report.json", "docs/p12/human-review.json", "docs/p12/release-artifacts/archive", "integration/p12/report.json"} {
		if _, included := first[path]; included {
			t.Fatalf("generated artifact %q was included: %#v", path, first)
		}
	}
	if len(first) != 8 {
		t.Fatalf("unexpected source map: %#v", first)
	}
	if err := os.WriteFile(filepath.Join(root, "main.go"), []byte("package changed\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	second, err := sourceArtifacts(root)
	if err != nil {
		t.Fatal(err)
	}
	if first["main.go"] == second["main.go"] {
		t.Fatal("source change did not change its hash")
	}
}

func TestInventoryRejectsReviewRequired(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inventory.csv")
	data := "source_symbol,disposition,phase\nok,implemented,P01\n"
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := validateInventory(path); err != nil {
		t.Fatalf("complete inventory rejected: %v", err)
	}
	if err := os.WriteFile(path, []byte(strings.Replace(data, "implemented", "review-required", 1)), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := validateInventory(path); err == nil {
		t.Fatal("review-required inventory accepted")
	}
}

func TestObservedGoVersionRequiresExactSelectedToolchain(t *testing.T) {
	if !observedGoVersion("go version go1.27.1 darwin/arm64\n", latestGo) {
		t.Fatal("exact selected Go version rejected")
	}
	for _, output := range []string{"go version go1.27.0 darwin/arm64\n", "go1.27.1\n", ""} {
		if observedGoVersion(output, latestGo) {
			t.Fatalf("inexact selected Go version accepted: %q", output)
		}
	}
}
