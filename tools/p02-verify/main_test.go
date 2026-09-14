package main

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

var validManifest = []byte(`{
  "schema_version": 1,
  "fixture": "fixture.bin",
  "sha256": "0000000000000000000000000000000000000000000000000000000000000000",
  "kind": "native-oracle",
  "source": {"repository": "https://example.com/repo.git", "commit": "0000000000000000000000000000000000000000", "paths": ["source"]},
  "generator": {"tool": "tool", "version": "1", "command": ["tool"], "image": "quay.io/ceph/ceph@sha256:0000000000000000000000000000000000000000000000000000000000000000"},
  "secrets": {"contains_secrets": false, "synthetic_only": true},
	"license": {"upstream_expression": "test", "redistribution": "pending", "reviewed_by": "pending human review"}
}`)

var validEvidence = []byte(`{
	"schema_version": 1,
	"status": "incomplete/pending-human-review",
	"source": {"repository": "https://example.com/repo.git", "tag": "v1", "commit": "0000000000000000000000000000000000000000"},
	"local_toolchain": {"compiler": "compiler", "openssl": "openssl"},
	"commands": [{"command": "command", "result": "passed"}],
	"fixtures": {"fixture.bin": "0000000000000000000000000000000000000000000000000000000000000000"},
	"docker_reproduction": {"command": "command", "result": "canceled", "reason": "reason"},
	"upstream_oracle": {"kind": "Ceph FrameAssembler", "result": "passed", "source_tag": "v1.1", "source_commit": "1111111111111111111111111111111111111111", "runtime_package": "ceph-common-1", "libraries": {"linux/amd64": "0000000000000000000000000000000000000000000000000000000000000000"}, "oracle_sha256": "0000000000000000000000000000000000000000000000000000000000000000", "dockerfile_sha256": "0000000000000000000000000000000000000000000000000000000000000000", "reproduction_command": "command"}
}`)

func TestValidateManifestDocument(t *testing.T) {
	if err := validateManifestDocument(validManifest); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		data []byte
	}{
		{"missing top-level field", bytes.Replace(validManifest, []byte(`  "kind": "native-oracle",`+"\n"), nil, 1)},
		{"missing nested field", bytes.Replace(validManifest, []byte(`"contains_secrets": false, `), nil, 1)},
		{"unknown top-level field", bytes.Replace(validManifest, []byte(`"schema_version": 1`), []byte(`"schema_version": 1, "unknown": true`), 1)},
		{"unknown nested field", bytes.Replace(validManifest, []byte(`"tool": "tool"`), []byte(`"tool": "tool", "unknown": true`), 1)},
		{"trailing value", append(append([]byte(nil), validManifest...), []byte(` {}`)...)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := validateManifestDocument(test.data); err == nil {
				t.Fatal("invalid manifest accepted")
			}
		})
	}
}

func TestValidateEvidenceDocument(t *testing.T) {
	if err := validateEvidenceDocument(validEvidence); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		data []byte
	}{
		{"missing top-level field", bytes.Replace(validEvidence, []byte("\t\"status\": \"incomplete/pending-human-review\",\n"), nil, 1)},
		{"missing nested field", bytes.Replace(validEvidence, []byte(`"compiler": "compiler", `), nil, 1)},
		{"unknown top-level field", bytes.Replace(validEvidence, []byte(`"schema_version": 1`), []byte(`"schema_version": 1, "unknown": true`), 1)},
		{"unknown nested field", bytes.Replace(validEvidence, []byte(`"result": "canceled"`), []byte(`"result": "canceled", "unknown": true`), 1)},
		{"missing command field", bytes.Replace(validEvidence, []byte(`"command": "command", `), nil, 1)},
		{"unknown command field", bytes.Replace(validEvidence, []byte(`"result": "passed"`), []byte(`"result": "passed", "unknown": true`), 1)},
		{"trailing value", append(append([]byte(nil), validEvidence...), []byte(` {}`)...)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := validateEvidenceDocument(test.data); err == nil {
				t.Fatal("invalid evidence accepted")
			}
		})
	}
}

func TestValidateArtifactNames(t *testing.T) {
	expected := map[string]fixtureExpectation{"fixture.bin": {}}
	if err := validateArtifactNames([]string{"fixture.bin", "fixture.bin.json"}, expected); err != nil {
		t.Fatal(err)
	}
	for _, names := range [][]string{
		{"fixture.bin"},
		{"fixture.bin.json"},
		{"fixture.bin", "fixture.bin.json", "extra.bin"},
	} {
		if err := validateArtifactNames(names, expected); err == nil {
			t.Fatalf("invalid artifact set accepted: %v", names)
		}
	}
}

func TestExpectedFixturesUseExactPaths(t *testing.T) {
	expected := expectedFixtures()
	upstream := expectedUpstreamFixtures()
	if !slices.Equal(expected["banner-rev1.bin"].paths, bannerPaths) ||
		!slices.Equal(expected["crc-one-segment.bin"].paths, crcPaths) ||
		!slices.Equal(expected["secure-multi-record.bin"].paths, securePaths) {
		t.Fatal("fixture source paths do not match their protocol mechanisms")
	}
	if expected["banner-rev1.bin"].license != bannerLicense ||
		expected["crc-one-segment.bin"].license != compositeLicense ||
		expected["secure-multi-record.bin"].license != compositeLicense {
		t.Fatal("fixture licenses do not match their source paths")
	}
	if !slices.Equal(upstream["upstream-ack-control.bin"].paths, crcPaths) ||
		!slices.Equal(upstream["upstream-secure-one-segment.bin"].paths, securePaths) ||
		!upstream["upstream-message-frame.bin"].upstream {
		t.Fatal("upstream fixture provenance does not match its protocol mechanism")
	}
}

func TestCheckExecutableMode(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "reproduce.sh")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"), 0755); err != nil {
		t.Fatal(err)
	}
	checkExecutable(path)
}
