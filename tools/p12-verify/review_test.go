package main

/* Obsolete schema-v1 review tests retained temporarily during migration.

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDecodeHumanReviewRejectsMalformedUnknownAndTrailingJSON(t *testing.T) {
	valid := marshalHumanReview(t, validHumanReview())
	tests := map[string][]byte{
		"malformed": []byte(`{"schema_version":`),
		"unknown":   bytes.Replace(valid, []byte(`"schema_version": 1,`), []byte(`"schema_version": 1, "unexpected": true,`), 1),
		"trailing":  append(append([]byte(nil), valid...), []byte(` {}`)...),
	}
	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeHumanReview(bytes.NewReader(input)); err == nil {
				t.Fatal("invalid human-review JSON was accepted")
			}
		})
	}
}

func TestCheckedInHumanReviewIsTruthfullyPendingAndUnsigned(t *testing.T) {
	file, err := os.Open("../../docs/p12/human-review.json")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	value, err := decodeHumanReview(file)
	if err != nil {
		t.Fatal(err)
	}
	if value.Status != "pending" || value.ReviewedCandidate != nil || len(value.Reviews) != 0 {
		t.Fatalf("pending record contains candidate or approval evidence: %#v", value)
	}
	if err := validateHumanReview(value, nil); err == nil || !strings.Contains(err.Error(), "pending") {
		t.Fatalf("pending record was accepted as approved: %v", err)
	}
}

func TestValidateHumanReviewRequiresExactCompleteApprovals(t *testing.T) {
	qualification := validReviewQualification()
	if err := validateHumanReview(validHumanReview(), qualification); err != nil {
		t.Fatalf("valid human review rejected: %v", err)
	}
	tests := map[string]func(*humanReviewReport){
		"pending":           func(value *humanReviewReport) { value.Status = "pending" },
		"roles declaration": func(value *humanReviewReport) { value.RequiredRoles[0] = "release-owner" },
		"missing candidate": func(value *humanReviewReport) { value.ReviewedCandidate = nil },
		"candidate repository": func(value *humanReviewReport) {
			value.ReviewedCandidate.Repository = "https://example.invalid/repository"
		},
		"qualification mismatch": func(value *humanReviewReport) { value.ReviewedCandidate.Qualification.SHA256 = strings.Repeat("b", 64) },
		"missing role":           func(value *humanReviewReport) { value.Reviews = value.Reviews[:3] },
		"duplicate role":         func(value *humanReviewReport) { value.Reviews[3].Role = value.Reviews[0].Role },
		"unnamed":                func(value *humanReviewReport) { value.Reviews[0].ReviewerName = " " },
		"no affiliation":         func(value *humanReviewReport) { value.Reviews[0].ReviewerAffiliation = "" },
		"invalid timestamp":      func(value *humanReviewReport) { value.Reviews[0].StartedAt = "not-a-time" },
		"reversed timestamps":    func(value *humanReviewReport) { value.Reviews[0].CompletedAt = "2026-09-16T09:59:59Z" },
		"not approved":           func(value *humanReviewReport) { value.Reviews[0].Decision = "pending" },
		"critical finding":       func(value *humanReviewReport) { value.Reviews[0].UnresolvedFindings.Critical = 1 },
		"high finding":           func(value *humanReviewReport) { value.Reviews[0].UnresolvedFindings.High = 1 },
		"unsigned":               func(value *humanReviewReport) { value.Reviews[0].ApprovalReference = " " },
		"unauditable approval":   func(value *humanReviewReport) { value.Reviews[0].ApprovalReference = "approved by reviewer" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			value := validHumanReview()
			mutate(&value)
			if err := validateHumanReview(value, qualification); err == nil {
				t.Fatal("invalid human review was accepted")
			}
		})
	}
}

func TestPassedReportRequiresCurrentApprovedReviewBinding(t *testing.T) {
	root := t.TempDir()
	value := report{Status: "passed", Qualification: validReviewQualification()}
	if err := validateReviews(value, root); err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("missing review binding accepted: %v", err)
	}

	path := filepath.Join(root, "docs/p12/human-review.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(review humanReviewReport) string {
		data := marshalHumanReview(t, review)
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(data)
		return hex.EncodeToString(digest[:])
	}

	value.Reviews = &reviewBinding{Path: "docs/p12/human-review.json", Status: "pending", SHA256: strings.Repeat("a", 64)}
	if err := validateReviews(value, root); err == nil || !strings.Contains(err.Error(), "status") {
		t.Fatalf("pending binding accepted: %v", err)
	}

	value.Reviews.Status = "approved"
	value.Reviews.SHA256 = write(validHumanReview())
	if err := validateReviews(value, root); err != nil {
		t.Fatalf("valid bound human review rejected: %v", err)
	}
	value.Reviews.SHA256 = strings.Repeat("0", 64)
	if err := validateReviews(value, root); err == nil || !strings.Contains(err.Error(), "hash mismatch") {
		t.Fatalf("mismatched review hash accepted: %v", err)
	}

	pending := humanReviewReport{SchemaVersion: 1, Status: "pending", RequiredRoles: append([]string(nil), requiredReviewRoles...), Reviews: []humanReviewRecord{}}
	value.Reviews.SHA256 = write(pending)
	if err := validateReviews(value, root); err == nil || !strings.Contains(err.Error(), "pending") {
		t.Fatalf("bound pending review accepted: %v", err)
	}
}

func validReviewQualification() *qualificationBinding {
	return &qualificationBinding{Path: "docs/p12/qualification-report.json", Status: "passed", SHA256: strings.Repeat("a", 64)}
}

func validHumanReview() humanReviewReport {
	qualification := *validReviewQualification()
	value := humanReviewReport{
		SchemaVersion: 1,
		Status:        "approved",
		RequiredRoles: append([]string(nil), requiredReviewRoles...),
		ReviewedCandidate: &reviewedCandidate{
			Repository:     "https://github.com/otuschhoff/go-librados.git",
			SourceIdentity: "content-addressed-artifacts",
			Qualification:  qualification,
		},
	}
	for _, role := range requiredReviewRoles {
		review := humanReviewRecord{
			Role: role, ReviewerName: "Accountable Reviewer", ReviewerAffiliation: "Example Organization",
			StartedAt: "2026-09-16T10:00:00Z", CompletedAt: "2026-09-16T10:01:00Z",
			Decision: "approved", ApprovalReference: "https://approvals.example.invalid/p12/123",
		}
		value.Reviews = append(value.Reviews, review)
	}
	return value
}

func marshalHumanReview(t *testing.T, value humanReviewReport) []byte {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return append(data, '\n')
}
*/
