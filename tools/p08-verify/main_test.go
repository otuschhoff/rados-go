package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadReportRejectsUnknownFieldsWithoutPanic(t *testing.T) {
	if os.Getenv("P08_VERIFY_MALFORMED_HELPER") == "1" {
		readReport(os.Getenv("P08_VERIFY_REPORT"))
		return
	}
	path := filepath.Join(t.TempDir(), "report.json")
	if err := os.WriteFile(path, []byte(`{"unknown":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(os.Args[0], "-test.run=^TestReadReportRejectsUnknownFieldsWithoutPanic$")
	command.Env = append(os.Environ(), "P08_VERIFY_MALFORMED_HELPER=1", "P08_VERIFY_REPORT="+path)
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatal("malformed report accepted")
	}
	text := string(output)
	if !strings.Contains(text, "decode P08 report") || strings.Contains(text, "panic:") {
		t.Fatalf("unexpected verifier output: %s", text)
	}
}

func TestMapsEqualRejectsMissingAndChangedArtifacts(t *testing.T) {
	expected := map[string]string{"a": "one", "b": "two"}
	if !mapsEqual(expected, map[string]string{"a": "one", "b": "two"}) {
		t.Fatal("equal maps rejected")
	}
	if mapsEqual(expected, map[string]string{"a": "one"}) || mapsEqual(expected, map[string]string{"a": "one", "b": "changed"}) {
		t.Fatal("invalid artifact map accepted")
	}
}
