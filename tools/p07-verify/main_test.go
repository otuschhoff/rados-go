package main

import (
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadReportRejectsUnknownFieldsWithoutPanic(t *testing.T) {
	if os.Getenv("P07_VERIFY_MALFORMED_HELPER") == "1" {
		readReport(os.Getenv("P07_VERIFY_REPORT"))
		return
	}

	path := filepath.Join(t.TempDir(), "report.json")
	if err := os.WriteFile(path, []byte(`{"unknown":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(os.Args[0], "-test.run=^TestReadReportRejectsUnknownFieldsWithoutPanic$")
	command.Env = append(os.Environ(), "P07_VERIFY_MALFORMED_HELPER=1", "P07_VERIFY_REPORT="+path)
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatal("malformed report accepted")
	}
	text := string(output)
	if !strings.Contains(text, "decode P07 report") || strings.Contains(text, "panic:") {
		t.Fatalf("unexpected verifier output: %s", text)
	}
}

func TestMetricsEqualAllowsSerializationRoundingOnly(t *testing.T) {
	expected := float64(65536*32) * 1e9 / float64(123456789)
	rounded := math.Round(expected*1e6) / 1e6
	if !metricsEqual(rounded, expected) {
		t.Fatalf("native serialization rounding rejected: actual=%v expected=%v", rounded, expected)
	}
	if metricsEqual(rounded+0.01, expected) {
		t.Fatal("tampered metric accepted")
	}
}
