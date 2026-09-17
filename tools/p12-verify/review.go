package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

const (
	humanReviewPath     = "docs/p12/human-review.json"
	reviewerTrustPath   = "docs/p12/reviewer-trust.json"
	candidateReportPath = "integration/p12/report.json"
)

var requiredReviewRoles = []string{"security", "distributed-systems", "license/notices", "release-owner"}

type humanReviewReport struct {
	SchemaVersion     int                 `json:"schema_version"`
	Status            string              `json:"status"`
	RequiredRoles     []string            `json:"required_roles"`
	ReviewedCandidate *reviewedCandidate  `json:"reviewed_candidate"`
	Reviews           []humanReviewRecord `json:"reviews"`
}

type reviewedCandidate struct {
	Report        fileBinding          `json:"report"`
	Qualification qualificationBinding `json:"qualification"`
	Fuzz          fuzzBinding          `json:"fuzz"`
	Release       reviewedRelease      `json:"release"`
}

type fileBinding struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

type reviewedRelease struct {
	Version   string            `json:"version"`
	Artifacts map[string]string `json:"artifacts"`
}

type findingCounts struct {
	Critical int `json:"critical"`
	High     int `json:"high"`
	Medium   int `json:"medium"`
	Low      int `json:"low"`
}

type humanReviewRecord struct {
	Role                string        `json:"role"`
	KeyID               string        `json:"key_id"`
	ReviewerIdentity    string        `json:"reviewer_identity"`
	ReviewerAffiliation string        `json:"reviewer_affiliation"`
	StartedAt           string        `json:"started_at"`
	CompletedAt         string        `json:"completed_at"`
	Decision            string        `json:"decision"`
	UnresolvedFindings  findingCounts `json:"unresolved_findings"`
	ApprovalReference   string        `json:"approval_reference"`
	Signature           string        `json:"signature"`
}

type reviewerTrustPolicy struct {
	SchemaVersion int          `json:"schema_version"`
	Status        string       `json:"status"`
	Keys          []trustedKey `json:"keys"`
}

type trustedKey struct {
	KeyID               string `json:"key_id"`
	Role                string `json:"role"`
	ReviewerIdentity    string `json:"reviewer_identity"`
	ReviewerAffiliation string `json:"reviewer_affiliation"`
	PublicKey           string `json:"public_key"`
}

type canonicalReviewPayload struct {
	SchemaVersion       int               `json:"schema_version"`
	Role                string            `json:"role"`
	KeyID               string            `json:"key_id"`
	ReviewerIdentity    string            `json:"reviewer_identity"`
	ReviewerAffiliation string            `json:"reviewer_affiliation"`
	ReviewedReportPath  string            `json:"reviewed_report_path"`
	ReviewedReportHash  string            `json:"reviewed_report_sha256"`
	QualificationPath   string            `json:"qualification_path"`
	QualificationHash   string            `json:"qualification_sha256"`
	FuzzPath            string            `json:"fuzz_path"`
	FuzzHash            string            `json:"fuzz_sha256"`
	FuzzProfile         string            `json:"fuzz_profile"`
	ReleaseVersion      string            `json:"release_version"`
	ReleaseArtifacts    map[string]string `json:"release_artifact_sha256"`
	Decision            string            `json:"decision"`
	StartedAt           string            `json:"started_at"`
	CompletedAt         string            `json:"completed_at"`
	UnresolvedFindings  findingCounts     `json:"unresolved_findings"`
	ApprovalReference   string            `json:"approval_reference"`
}

func decodeStrictJSON(reader io.Reader, target any, kind string) error {
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode strict %s JSON: %w", kind, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("invalid %s JSON framing", kind)
	}
	return nil
}

func decodeHumanReview(reader io.Reader) (humanReviewReport, error) {
	var value humanReviewReport
	err := decodeStrictJSON(reader, &value, "human-review")
	return value, err
}

func decodeReviewerTrust(reader io.Reader) (reviewerTrustPolicy, error) {
	var value reviewerTrustPolicy
	err := decodeStrictJSON(reader, &value, "reviewer-trust")
	return value, err
}

func readHumanReview(path string) (humanReviewReport, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return humanReviewReport{}, fmt.Errorf("read human-review report: %w", err)
	}
	return decodeHumanReview(bytes.NewReader(data))
}

func validateTrustPolicyDigest(path, expected string) error {
	if len(expected) != sha256.Size*2 || expected != strings.ToLower(expected) {
		return errors.New("P12_REVIEWER_TRUST_SHA256 must contain the externally trusted lowercase reviewer-policy SHA-256")
	}
	if _, err := hex.DecodeString(expected); err != nil {
		return errors.New("P12_REVIEWER_TRUST_SHA256 must contain the externally trusted lowercase reviewer-policy SHA-256")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read reviewer trust policy for external anchoring: %w", err)
	}
	digest := sha256.Sum256(data)
	if hex.EncodeToString(digest[:]) != expected {
		return errors.New("reviewer trust policy does not match the externally trusted SHA-256")
	}
	return nil
}

func validateHumanReviewFile(path, trustPath, reportPath, root string) error {
	review, err := readHumanReview(path)
	if err != nil {
		return err
	}
	trustData, err := os.ReadFile(trustPath)
	if err != nil {
		return fmt.Errorf("read reviewer trust policy: %w", err)
	}
	var trust reviewerTrustPolicy
	if err := decodeStrictJSON(bytes.NewReader(trustData), &trust, "reviewer-trust"); err != nil {
		return err
	}
	reportData, err := os.ReadFile(reportPath)
	if err != nil {
		return fmt.Errorf("read candidate report: %w", err)
	}
	candidate, err := decodeReport(bytes.NewReader(reportData))
	if err != nil {
		return err
	}
	return validateHumanReview(review, trust, candidate, reportData)
}

func validateDetachedReviews(value report, root string) error {
	if value.Reviews != nil {
		return errors.New("endurance report reviews must always be null")
	}
	if value.Status == "non-certifying" {
		return nil
	}
	return validateHumanReviewFile(
		filepath.Join(root, filepath.FromSlash(humanReviewPath)),
		filepath.Join(root, filepath.FromSlash(reviewerTrustPath)),
		filepath.Join(root, filepath.FromSlash(candidateReportPath)), root,
	)
}

func validateHumanReview(value humanReviewReport, trust reviewerTrustPolicy, candidate report, reportData []byte) error {
	if value.SchemaVersion != 2 || value.Status != "approved" {
		return errors.New("human-review report is pending or has an invalid schema")
	}
	if !slices.Equal(value.RequiredRoles, requiredReviewRoles) || value.ReviewedCandidate == nil {
		return errors.New("human-review report lacks the exact roles or candidate binding")
	}
	if candidate.Status != "candidate" || candidate.Reviews != nil || candidate.Qualification == nil || candidate.Fuzz == nil || candidate.Release.Version == nil {
		return errors.New("reviewed report is not an immutable review-independent candidate")
	}
	reportDigest := sha256.Sum256(reportData)
	want := reviewedCandidate{
		Report:        fileBinding{Path: candidateReportPath, SHA256: hex.EncodeToString(reportDigest[:])},
		Qualification: *candidate.Qualification,
		Fuzz:          *candidate.Fuzz,
		Release:       reviewedRelease{Version: *candidate.Release.Version, Artifacts: candidate.Release.Artifacts},
	}
	if !equalReviewedCandidate(*value.ReviewedCandidate, want) {
		return errors.New("human-review candidate hash, qualification, version, or artifacts do not match the report")
	}
	keys, err := validateTrustPolicy(trust)
	if err != nil {
		return err
	}
	if len(value.Reviews) != len(requiredReviewRoles) {
		return fmt.Errorf("human-review report has %d reviews, want exactly %d", len(value.Reviews), len(requiredReviewRoles))
	}
	finished, err := time.Parse("2006-01-02T15:04:05Z", candidate.FinishedAt)
	if err != nil {
		return errors.New("candidate completion timestamp is invalid")
	}
	seen := make(map[string]bool, len(requiredReviewRoles))
	for _, review := range value.Reviews {
		if !slices.Contains(requiredReviewRoles, review.Role) || seen[review.Role] {
			return fmt.Errorf("human-review report has invalid or duplicate role %q", review.Role)
		}
		seen[review.Role] = true
		key, ok := keys[review.KeyID]
		if !ok || key.Role != review.Role || key.ReviewerIdentity != review.ReviewerIdentity || key.ReviewerAffiliation != review.ReviewerAffiliation {
			return fmt.Errorf("human review %q does not match its trusted key identity and role", review.Role)
		}
		started, startErr := time.Parse(time.RFC3339, review.StartedAt)
		completed, completeErr := time.Parse(time.RFC3339, review.CompletedAt)
		if startErr != nil || completeErr != nil || !started.After(finished) || completed.Before(started) || review.Decision != "approved" || review.UnresolvedFindings.Critical != 0 || review.UnresolvedFindings.High != 0 || !validApprovalReference(review.ApprovalReference) {
			return fmt.Errorf("human review %q is incomplete, predates the candidate, or has unresolved critical/high findings", review.Role)
		}
		publicKey, _ := base64.StdEncoding.DecodeString(key.PublicKey)
		signature, signatureErr := base64.StdEncoding.DecodeString(review.Signature)
		payload, payloadErr := canonicalPayload(value, review)
		if signatureErr != nil || base64.StdEncoding.EncodeToString(signature) != review.Signature || payloadErr != nil || len(signature) != ed25519.SignatureSize || !ed25519.Verify(ed25519.PublicKey(publicKey), payload, signature) {
			return fmt.Errorf("human review %q has an invalid Ed25519 signature", review.Role)
		}
	}
	return nil
}

func validateTrustPolicy(value reviewerTrustPolicy) (map[string]trustedKey, error) {
	if value.SchemaVersion != 1 || value.Status != "active" {
		return nil, errors.New("reviewer trust policy is pending or has an invalid schema")
	}
	keys := make(map[string]trustedKey, len(value.Keys))
	roles := make(map[string]bool, len(requiredReviewRoles))
	publicKeys := make(map[string]bool, len(requiredReviewRoles))
	reviewers := make(map[string]bool, len(requiredReviewRoles))
	for _, key := range value.Keys {
		decoded, err := base64.StdEncoding.DecodeString(key.PublicKey)
		reviewer := normalizedReviewer(key.ReviewerIdentity, key.ReviewerAffiliation)
		if !validToken(key.KeyID) || keys[key.KeyID].KeyID != "" || !slices.Contains(requiredReviewRoles, key.Role) || roles[key.Role] || !nonBlankExact(key.ReviewerIdentity) || !nonBlankExact(key.ReviewerAffiliation) || err != nil || len(decoded) != ed25519.PublicKeySize || base64.StdEncoding.EncodeToString(decoded) != key.PublicKey || publicKeys[string(decoded)] || reviewers[reviewer] {
			return nil, errors.New("reviewer trust policy has a duplicate, malformed, or unauthorized key")
		}
		keys[key.KeyID] = key
		roles[key.Role] = true
		publicKeys[string(decoded)] = true
		reviewers[reviewer] = true
	}
	if len(keys) != len(requiredReviewRoles) {
		return nil, errors.New("reviewer trust policy must authorize exactly one key for each required role")
	}
	return keys, nil
}

func canonicalPayload(value humanReviewReport, review humanReviewRecord) ([]byte, error) {
	if value.ReviewedCandidate == nil {
		return nil, errors.New("human-review report lacks a candidate binding")
	}
	candidate := value.ReviewedCandidate
	payload := canonicalReviewPayload{
		SchemaVersion: value.SchemaVersion, Role: review.Role, KeyID: review.KeyID,
		ReviewerIdentity: review.ReviewerIdentity, ReviewerAffiliation: review.ReviewerAffiliation,
		ReviewedReportPath: candidate.Report.Path, ReviewedReportHash: candidate.Report.SHA256,
		QualificationPath: candidate.Qualification.Path, QualificationHash: candidate.Qualification.SHA256,
		FuzzPath: candidate.Fuzz.Path, FuzzHash: candidate.Fuzz.SHA256, FuzzProfile: candidate.Fuzz.Profile,
		ReleaseVersion: candidate.Release.Version, ReleaseArtifacts: candidate.Release.Artifacts,
		Decision: review.Decision, StartedAt: review.StartedAt, CompletedAt: review.CompletedAt,
		UnresolvedFindings: review.UnresolvedFindings, ApprovalReference: review.ApprovalReference,
	}
	data, err := json.Marshal(payload)
	return append(data, '\n'), err
}

func equalReviewedCandidate(left, right reviewedCandidate) bool {
	if left.Report != right.Report || left.Qualification != right.Qualification || left.Fuzz != right.Fuzz || left.Release.Version != right.Release.Version || len(left.Release.Artifacts) != len(right.Release.Artifacts) {
		return false
	}
	for name, hash := range right.Release.Artifacts {
		if left.Release.Artifacts[name] != hash {
			return false
		}
	}
	return true
}

func validApprovalReference(value string) bool {
	if value != strings.TrimSpace(value) || strings.ContainsAny(value, " \t\r\n") {
		return false
	}
	reference, err := url.Parse(value)
	return err == nil && (reference.Scheme == "http" || reference.Scheme == "https") && reference.Host != ""
}

func validToken(value string) bool {
	return value != "" && value == strings.TrimSpace(value) && !strings.ContainsAny(value, " \t\r\n")
}

func nonBlankExact(value string) bool {
	return value == strings.TrimSpace(value) && value != ""
}

func normalizedReviewer(identity, affiliation string) string {
	normalize := func(value string) string {
		return strings.ToLower(strings.Join(strings.Fields(value), " "))
	}
	return normalize(identity) + "\x00" + normalize(affiliation)
}
