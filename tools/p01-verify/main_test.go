package main

import (
	"bytes"
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
  "license": {"upstream_expression": "test", "redistribution": "approved", "reviewed_by": "human"}
}`)

func TestValidateManifestDocument(t *testing.T) {
	if err := validateManifestDocument(validManifest); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		data []byte
	}{
		{"missing required", bytes.Replace(validManifest, []byte(`"contains_secrets": false, `), nil, 1)},
		{"unknown field", bytes.Replace(validManifest, []byte(`"schema_version": 1`), []byte(`"schema_version": 1, "unknown": true`), 1)},
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

func TestValidateArtifactNames(t *testing.T) {
	expected := map[string]fixtureExpectation{"fixture.bin": {}}
	if err := validateArtifactNames([]string{"fixture.bin", "fixture.bin.json"}, expected); err != nil {
		t.Fatal(err)
	}
	for _, names := range [][]string{
		{"fixture.bin"},
		{"fixture.bin", "fixture.bin.json", "orphan.bin.json"},
	} {
		if err := validateArtifactNames(names, expected); err == nil {
			t.Fatalf("invalid artifact set accepted: %v", names)
		}
	}
}

func TestEvidencePinsControlFixtureExpectations(t *testing.T) {
	pins := evidencePins{image: "pinned-image"}
	want := expectedFixtures(pins)["entity-name-mon-new.bin"].command
	if !slices.Contains(want, pins.image) {
		t.Fatalf("fixture command does not use evidence image: %v", want)
	}
}
