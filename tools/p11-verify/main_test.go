package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadReportRejectsUnknownFieldsWithoutPanic(t *testing.T) {
	if os.Getenv("P11_VERIFY_MALFORMED_HELPER") == "1" {
		readReport(os.Getenv("P11_VERIFY_REPORT"))
		return
	}
	path := filepath.Join(t.TempDir(), "report.json")
	if err := os.WriteFile(path, []byte(`{"unknown":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(os.Args[0], "-test.run=^TestReadReportRejectsUnknownFieldsWithoutPanic$")
	command.Env = append(os.Environ(), "P11_VERIFY_MALFORMED_HELPER=1", "P11_VERIFY_REPORT="+path)
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatal("malformed report accepted")
	}
	if text := string(output); !strings.Contains(text, "decode P11 report") || strings.Contains(text, "panic:") {
		t.Fatalf("unexpected verifier output: %s", text)
	}
}

func TestReadReportRejectsTrailingJSONWithoutPanic(t *testing.T) {
	if os.Getenv("P11_VERIFY_TRAILING_HELPER") == "1" {
		readReport(os.Getenv("P11_VERIFY_REPORT"))
		return
	}
	path := filepath.Join(t.TempDir(), "report.json")
	if err := os.WriteFile(path, []byte(`{} {}`), 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(os.Args[0], "-test.run=^TestReadReportRejectsTrailingJSONWithoutPanic$")
	command.Env = append(os.Environ(), "P11_VERIFY_TRAILING_HELPER=1", "P11_VERIFY_REPORT="+path)
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatal("trailing JSON accepted")
	}
	if text := string(output); !strings.Contains(text, "invalid P11 report JSON framing") || strings.Contains(text, "panic:") {
		t.Fatalf("unexpected verifier output: %s", text)
	}
}

func TestMapsEqualRejectsMissingExtraAndChangedArtifacts(t *testing.T) {
	expected := map[string]string{"a": "one", "b": "two"}
	if !mapsEqual(expected, map[string]string{"a": "one", "b": "two"}) {
		t.Fatal("equal maps rejected")
	}
	if mapsEqual(expected, map[string]string{"a": "one"}) || mapsEqual(expected, map[string]string{"a": "one", "b": "two", "c": "three"}) || mapsEqual(expected, map[string]string{"a": "one", "b": "changed"}) {
		t.Fatal("invalid artifact map accepted")
	}
}

func TestAllChecksRequiresEveryResult(t *testing.T) {
	value := checks{true, true, true, true, true, true, true, true, true, true, true}
	if !allChecks(value) {
		t.Fatal("complete checks rejected")
	}
	value.PGCommand = false
	if allChecks(value) {
		t.Fatal("incomplete checks accepted")
	}
}
