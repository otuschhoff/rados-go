package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestValidDistinctHashes(t *testing.T) {
	first := strings.Repeat("01", 32)
	second := strings.Repeat("02", 32)
	if !validDistinctHashes(first, second) {
		t.Fatal("distinct SHA-256 values rejected")
	}
	for _, test := range []struct {
		name   string
		first  string
		second string
	}{
		{name: "equal", first: first, second: first},
		{name: "malformed", first: "not-hex", second: second},
		{name: "short", first: "01", second: second},
	} {
		t.Run(test.name, func(t *testing.T) {
			if validDistinctHashes(test.first, test.second) {
				t.Fatal("invalid ticket hashes accepted")
			}
		})
	}
}

func TestExpectedImageForPlatformRejectsUnknown(t *testing.T) {
	if image, ok := expectedImageForPlatform("linux/s390x"); ok || image != "" {
		t.Fatalf("unknown platform image = %q, known = %v", image, ok)
	}
	if image, ok := expectedImageForPlatform("linux/arm64"); !ok || image != qualificationARM64 {
		t.Fatalf("arm64 image = %q, known = %v", image, ok)
	}
	for _, image := range []string{"", "quay.io/ceph/ceph:v20.2.4"} {
		if validServerImage("linux/arm64", image) {
			t.Fatalf("non-digest image %q accepted", image)
		}
	}
}

func TestReviewEvidenceIsClosedAndRequired(t *testing.T) {
	valid := `{"status":"approved with no findings","reviewed_by":"Oliver Tuschhoff","reviewed_at":"2026-09-14","reviewed_tree":"repository a3c1b3d9d255effd183c493101d330dcb379a9d6 plus content-addressed artifacts in docs/p03/integration-report.json","crypto_and_wire":"approved with no findings","connector_and_deadlines":"approved with no findings","ticket_lifecycle":"approved with no findings","secrets_and_logs":"approved with no findings","license_redistribution":"approved for redistribution","scope_boundary":"P04 not started"}`
	for _, test := range []struct {
		name string
		json string
		want bool
	}{
		{name: "valid", json: valid, want: true},
		{name: "unknown field", json: strings.TrimSuffix(valid, "}") + `,"approval":"yes"}`},
		{name: "missing field", json: strings.Replace(valid, `,"scope_boundary":"P04 not started"`, "", 1)},
		{name: "malformed", json: strings.TrimSuffix(valid, "}")},
	} {
		t.Run(test.name, func(t *testing.T) {
			var review reviewEvidence
			decoder := json.NewDecoder(bytes.NewBufferString(test.json))
			decoder.DisallowUnknownFields()
			err := decoder.Decode(&review)
			if got := err == nil && validCompletedReview(review); got != test.want {
				t.Fatalf("accepted = %v, want %v (error %v)", got, test.want, err)
			}
		})
	}
}
