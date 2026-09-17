// Command p03-verify validates checked-in P03 evidence and fixture provenance.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

const (
	projectRepository   = "https://github.com/otuschhoff/rados-go.git"
	repository          = "https://github.com/ceph/ceph.git"
	baselineCommit      = "69f84cc2651aa259a15bc192ddaabd3baba07489"
	qualificationCommit = "7f793731f1b39eb4f465e960113d2363c311b964"
	qualificationImage  = "quay.io/ceph/ceph@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9"
	qualificationAMD64  = "quay.io/ceph/ceph@sha256:09ee90f6f3e0c7b9954f71d214ee05e9bbaaaea3716b1dd619603283b829f8b8"
	qualificationARM64  = "quay.io/ceph/ceph@sha256:6e6bc7b28fa1b334108a3646af5533dfb50db508efdf5b358eb7dd0dd37a48aa"
	monitorAMD64SHA256  = "10814919731d782d6510bdb0d9064cb7c04bd7a563e235aa3fa94e07f17d1215"
	monitorARM64SHA256  = "4b44a5660fdcc6bf7701f64fa41f707b8ff619d09042219d071e6d9ac45ab81b"
)

var artifactHashes = map[string]string{
	"testdata/p03/cephx-encoding-vectors.json":               "6b7c66e654f2f11a2effa2c51119b4cdcf19eba642952d8eaa292de3182f1cc4",
	"testdata/p03/cephx-encoding-vectors.json.manifest.json": "fa72e75e9780ea2d63533d561acac35a57ce44e58dbee30e4466cdfc2d43586e",
	"testdata/p03/crypto-vectors.json":                       "9baad25768d0d32c238cb1c98bc2d6c08f66f84dde724b92b5d518dce50b4591",
	"testdata/p03/crypto-vectors.json.manifest.json":         "87e796f2079d36430deb5602b41ee9a366c29f1c3374277440e0525005016e1d",
	"integration/p03/reproduce-fixtures.sh":                  "7fde6e6631885b908cf4ae31ef658f9f0b2be66ec546937948399a4c5db98794",
	"integration/p03/reproduce.sh":                           "afc999ecc5b195ec93ba8c75487b16f5dc217ae9767df64fff35cb4771fced57",
	"integration/p03/report.schema.json":                     "a402b69c0522220ad8f32aa7ea6c19e3e285106f20451e674574b2ef796fd9be",
	"integration/p03/rfc8009-oracle/main.go":                 "acd8b9a14e8bfb7b43b9fcdbbf1ed6331f4df1b32bc64acf9b6ffa17823fdda8",
	"integration/p03/probe/main.go":                          "898b974ee575d1d1a51ecdac5c8abd51dd6ad3f9506c418371d15b1aeb1c842e",
	"integration/p03/legacy-crush.txt":                       "5b07d221477e01a50ad37ab113cb8511ada2895bd3e848504518d0ef634f13ea",
}

var manifestPaths = map[string][]string{
	"cephx-encoding-vectors.json": {"src/auth/cephx/CephxProtocol.h", "src/auth/Crypto.h", "src/auth/Crypto.cc"},
	"crypto-vectors.json":         {"src/auth/Crypto.h", "src/auth/Crypto.cc", "src/auth/cephx/CephxProtocol.cc"},
}

var manifestSourceHashes = map[string]string{
	"src/auth/cephx/CephxProtocol.h":  "563822ae66e9bf6c040980717d4e7db4f30b186dc7cc9e597fd0f10152e9fbd1",
	"src/auth/Crypto.h":               "b4e5e3b74f8b1049492fc09dffc3a8297cd2f38aa128231b4209e77ae8c88c54",
	"src/auth/Crypto.cc":              "17ad1f129765c32f359d3990ba5e30f9e076275bd84c045b9eaea04ee8e9eaf5",
	"src/auth/cephx/CephxProtocol.cc": "00a046914b2a62af8c0b5b08cd9067f639998c6a5880d955e8a797c3f869531d",
}

var manifestReferences = map[string][]sourceReference{
	"cephx-encoding-vectors.json": {
		{Type: "ceph-source", Locator: repository, Revision: qualificationCommit, Role: "wire structures and ceph-dencoder fixture generation"},
	},
	"crypto-vectors.json": {
		{Type: "ceph-source", Locator: repository, Revision: qualificationCommit, Role: "CephX AES-CBC and challenge behavior"},
		{Type: "rfc", Locator: "https://www.rfc-editor.org/rfc/rfc8009", Revision: "RFC 8009 section A", Role: "published AES256-CTS-HMAC-SHA384-192 test vector"},
		{Type: "openssl-runtime", Locator: qualificationImage, Revision: "OpenSSL 3.5.7", Role: "independent AES-128-CBC runtime result"},
		{Type: "local-oracle", Locator: "integration/p03/rfc8009-oracle/main.go", Revision: "sha256:acd8b9a14e8bfb7b43b9fcdbbf1ed6331f4df1b32bc64acf9b6ffa17823fdda8", Role: "standard-library RFC 8009 verification"},
		{Type: "production-dependency", Locator: "https://github.com/otuschhoff/gokrb5", Revision: "v8.5.3", Role: "production RFC 8009 implementation compared with the independent oracle"},
	},
}

type manifest struct {
	SchemaVersion int    `json:"schema_version"`
	Fixture       string `json:"fixture"`
	SHA256        string `json:"sha256"`
	Kind          string `json:"kind"`
	Source        struct {
		Repository string            `json:"repository"`
		Commit     string            `json:"commit"`
		Paths      []string          `json:"paths"`
		Files      map[string]string `json:"files"`
		References []sourceReference `json:"references"`
	} `json:"source"`
	Generator struct {
		Tool    string   `json:"tool"`
		Version string   `json:"version"`
		Command []string `json:"command"`
		Image   string   `json:"image"`
	} `json:"generator"`
	Secrets struct {
		ContainsSecrets bool `json:"contains_secrets"`
		SyntheticOnly   bool `json:"synthetic_only"`
	} `json:"secrets"`
	License struct {
		UpstreamExpression string `json:"upstream_expression"`
		Redistribution     string `json:"redistribution"`
		ReviewedBy         string `json:"reviewed_by"`
	} `json:"license"`
}

type sourceReference struct {
	Type     string `json:"type"`
	Locator  string `json:"locator"`
	Revision string `json:"revision"`
	Role     string `json:"role"`
}

type evidence struct {
	SchemaVersion int    `json:"schema_version"`
	Status        string `json:"status"`
	Source        struct {
		Repository          string `json:"repository"`
		BaselineCommit      string `json:"baseline_commit"`
		QualificationCommit string `json:"qualification_commit"`
		Image               string `json:"image"`
	} `json:"source"`
	Dependency struct {
		Module                     string `json:"module"`
		Version                    string `json:"version"`
		ImportedPackage            string `json:"imported_package"`
		License                    string `json:"license"`
		Purpose                    string `json:"purpose"`
		SupportedGo                string `json:"supported_go"`
		MaintenanceSecurityPosture string `json:"maintenance_security_posture"`
		TransitiveGraph            string `json:"transitive_graph"`
		CgoFFI                     string `json:"cgo_ffi"`
		ReplacementCost            string `json:"replacement_cost"`
	} `json:"dependency"`
	Artifacts   map[string]string `json:"artifacts"`
	RealMonitor map[string]string `json:"real_monitor"`
	Review      reviewEvidence    `json:"review"`
}

type reviewEvidence struct {
	Status                string `json:"status"`
	ReviewedBy            string `json:"reviewed_by"`
	ReviewedAt            string `json:"reviewed_at"`
	ReviewedTree          string `json:"reviewed_tree"`
	CryptoAndWire         string `json:"crypto_and_wire"`
	ConnectorAndDeadlines string `json:"connector_and_deadlines"`
	TicketLifecycle       string `json:"ticket_lifecycle"`
	SecretsAndLogs        string `json:"secrets_and_logs"`
	LicenseRedistribution string `json:"license_redistribution"`
	ScopeBoundary         string `json:"scope_boundary"`
}

type integrationReport struct {
	SchemaVersion int               `json:"schema_version"`
	Status        string            `json:"status"`
	Command       string            `json:"command"`
	StartedAt     string            `json:"started_at"`
	FinishedAt    string            `json:"finished_at"`
	Source        integrationSource `json:"source"`
	Server        struct {
		Repository          string `json:"repository"`
		SourceAnchorCommit  string `json:"source_anchor_commit"`
		Image               string `json:"image"`
		Platform            string `json:"platform"`
		HostPlatform        string `json:"host_platform"`
		RuntimePackage      string `json:"runtime_package"`
		MonitorBinarySHA256 string `json:"monitor_binary_sha256"`
	} `json:"server"`
	Scenarios map[string]string `json:"scenarios"`
	Probe     struct {
		AuthenticatedMode  string   `json:"authenticated_mode"`
		GlobalID           uint64   `json:"global_id"`
		Reconnected        bool     `json:"reconnected"`
		TicketRenewed      bool     `json:"ticket_renewed"`
		InitialTicketHash  string   `json:"initial_ticket_sha256"`
		RenewedTicketHash  string   `json:"renewed_ticket_sha256"`
		ExpiryRejected     bool     `json:"expiry_rejected"`
		ExpiredReconnect   bool     `json:"expired_reconnect"`
		RenewedGlobalID    uint64   `json:"renewed_global_id"`
		PostExpiryGlobalID uint64   `json:"post_expiry_global_id"`
		ServerGlobalID     uint64   `json:"server_global_id"`
		ServerAddresses    []string `json:"server_addresses"`
		ServerFeatures     uint64   `json:"server_features"`
		ServerCookie       uint64   `json:"server_cookie"`
	} `json:"probe"`
}

type integrationSource struct {
	Repository       string            `json:"repository"`
	RepositoryCommit string            `json:"repository_commit"`
	Dirty            bool              `json:"dirty"`
	Identity         string            `json:"identity"`
	Artifacts        map[string]string `json:"artifacts"`
}

func main() {
	for path, expected := range artifactHashes {
		checkHash(path, expected)
	}
	checkManifest("cephx-encoding-vectors.json")
	checkManifest("crypto-vectors.json")
	checkEvidence()
	checkIntegrationReport()
	checkExecutable("integration/p03/reproduce.sh")
	checkExecutable("integration/p03/reproduce-fixtures.sh")
	checkFixtureDirectory()
	fmt.Println("P03 verification passed")
}

func checkManifest(name string) {
	path := filepath.Join("testdata", "p03", name+".manifest.json")
	var value manifest
	decodeStrict(path, &value)
	if value.SchemaVersion != 1 || value.Fixture != name || value.SHA256 != artifactHashes[filepath.Join("testdata", "p03", name)] {
		fatalf("%s identity is invalid", path)
	}
	if value.Kind != "upstream-derived" && value.Kind != "synthetic" || value.Source.Repository != repository || value.Source.Commit != qualificationCommit || !slices.Equal(value.Source.Paths, manifestPaths[name]) {
		fatalf("%s source provenance is invalid", path)
	}
	if len(value.Source.Files) != len(value.Source.Paths) {
		fatalf("%s source hashes are incomplete", path)
	}
	for _, sourcePath := range value.Source.Paths {
		if value.Source.Files[sourcePath] != manifestSourceHashes[sourcePath] {
			fatalf("%s source hash is invalid for %s", path, sourcePath)
		}
	}
	if !slices.Equal(value.Source.References, manifestReferences[name]) {
		fatalf("%s typed source provenance is invalid", path)
	}
	if value.Generator.Tool == "" || value.Generator.Version == "" || value.Generator.Image != qualificationImage || !slices.Equal(value.Generator.Command, []string{"./integration/p03/reproduce-fixtures.sh"}) {
		fatalf("%s generator provenance is invalid", path)
	}
	expectedLicense := map[string]string{"cephx-encoding-vectors.json": "LGPL-2.1-or-later", "crypto-vectors.json": "RFC-REFERENCE AND Apache-2.0"}[name]
	if value.Secrets.ContainsSecrets || !value.Secrets.SyntheticOnly || value.License.UpstreamExpression != expectedLicense || value.License.Redistribution != "approved" || value.License.ReviewedBy != "Oliver Tuschhoff" {
		fatalf("%s secret or license metadata is invalid", path)
	}
}

func checkEvidence() {
	var value evidence
	decodeStrict("docs/p03/evidence.json", &value)
	if value.SchemaVersion != 1 || value.Status != "complete" || value.Source.Repository != repository || value.Source.BaselineCommit != baselineCommit || value.Source.QualificationCommit != qualificationCommit || value.Source.Image != qualificationImage {
		fatalf("P03 source evidence is invalid")
	}
	if value.Dependency.Module != "github.com/otuschhoff/gokrb5/v8" || value.Dependency.Version != "v8.5.3" || value.Dependency.ImportedPackage != "github.com/otuschhoff/gokrb5/v8/crypto" || value.Dependency.License != "Apache-2.0" || value.Dependency.Purpose == "" || value.Dependency.SupportedGo != "1.26.0" || value.Dependency.MaintenanceSecurityPosture == "" || value.Dependency.TransitiveGraph != "docs/p03/modules.txt" || value.Dependency.CgoFFI == "" || value.Dependency.ReplacementCost == "" {
		fatalf("P03 dependency evidence is invalid")
	}
	if len(value.Artifacts) != len(artifactHashes) {
		fatalf("P03 artifact evidence is incomplete")
	}
	for path, hash := range artifactHashes {
		if value.Artifacts[path] != hash {
			fatalf("P03 artifact evidence mismatch for %s", path)
		}
	}
	if len(value.RealMonitor) != 2 || value.RealMonitor["report"] != "docs/p03/integration-report.json" || value.RealMonitor["command"] != "make integration-p03" {
		fatalf("P03 real-monitor evidence reference is invalid")
	}
	if !validCompletedReview(value.Review) {
		fatalf("P03 human review metadata is invalid")
	}
}

func validCompletedReview(value reviewEvidence) bool {
	return value.Status == "approved with no findings" && value.ReviewedBy == "Oliver Tuschhoff" && value.ReviewedAt == "2026-09-14" &&
		value.ReviewedTree == "repository a3c1b3d9d255effd183c493101d330dcb379a9d6 plus content-addressed artifacts in docs/p03/integration-report.json" &&
		value.CryptoAndWire == "approved with no findings" && value.ConnectorAndDeadlines == "approved with no findings" &&
		value.TicketLifecycle == "approved with no findings" && value.SecretsAndLogs == "approved with no findings" &&
		value.LicenseRedistribution == "approved for redistribution" && value.ScopeBoundary == "P04 not started"
}

func checkIntegrationReport() {
	var value integrationReport
	path := os.Getenv("P03_REPORT")
	if path == "" {
		path = "docs/p03/integration-report.json"
	}
	decodeStrict(path, &value)
	if value.SchemaVersion != 1 || value.Status != "passed" || value.Command != "make integration-p03" {
		fatalf("P03 integration report identity is invalid")
	}
	startedAt, startErr := time.Parse(time.RFC3339, value.StartedAt)
	finishedAt, finishErr := time.Parse(time.RFC3339, value.FinishedAt)
	if startErr != nil || finishErr != nil || finishedAt.Before(startedAt) {
		fatalf("P03 integration report timestamps are invalid")
	}
	if value.Source.Repository != projectRepository || !validHex(value.Source.RepositoryCommit, 20) || value.Source.Identity != "content-addressed-artifacts" || !validRepositoryAnchor(value.Source.RepositoryCommit) {
		fatalf("P03 integration report source identity is invalid")
	}
	if !validServerImage(value.Server.Platform, value.Server.Image) || value.Server.Repository != repository || value.Server.SourceAnchorCommit != qualificationCommit || value.Server.HostPlatform != value.Server.Platform || value.Server.RuntimePackage != "ceph-mon-20.2.4-0.el9" || value.Server.MonitorBinarySHA256 != expectedMonitorHash(value.Server.Platform) {
		fatalf("P03 integration report server identity is invalid")
	}
	scenarios := []string{"secure_authentication", "client_server_ident", "same_session_renewal", "global_id_reuse", "ticket_renewal", "expired_ticket_rejection", "post_expiry_reconnect", "valid_wrong_key_rejection", "secure_to_crc_downgrade_rejection"}
	if len(value.Scenarios) != len(scenarios) {
		fatalf("P03 integration report scenario set is invalid")
	}
	for _, scenario := range scenarios {
		if value.Scenarios[scenario] != "passed" {
			fatalf("P03 integration scenario %s is not passed", scenario)
		}
	}
	if value.Probe.AuthenticatedMode != "secure" || value.Probe.GlobalID == 0 || !value.Probe.Reconnected || !value.Probe.TicketRenewed || !validDistinctHashes(value.Probe.InitialTicketHash, value.Probe.RenewedTicketHash) || !value.Probe.ExpiryRejected || !value.Probe.ExpiredReconnect || value.Probe.GlobalID != value.Probe.RenewedGlobalID || value.Probe.PostExpiryGlobalID == 0 || value.Probe.RenewedGlobalID == value.Probe.PostExpiryGlobalID || len(value.Probe.ServerAddresses) == 0 {
		fatalf("P03 integration probe result is invalid")
	}
	paths := integrationInputPaths()
	expectedArtifacts := len(paths)
	if _, ok := value.Source.Artifacts["Makefile"]; ok {
		expectedArtifacts++
	}
	if len(value.Source.Artifacts) != expectedArtifacts {
		fatalf("P03 integration source artifact set is invalid")
	}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		must(err)
		digest := sha256.Sum256(data)
		if value.Source.Artifacts[path] != hex.EncodeToString(digest[:]) {
			fatalf("P03 integration report does not cover current %s", path)
		}
	}
}

func expectedImageForPlatform(platform string) (string, bool) {
	image, ok := map[string]string{"linux/amd64": qualificationAMD64, "linux/arm64": qualificationARM64}[platform]
	return image, ok
}

func validServerImage(platform, image string) bool {
	expected, ok := expectedImageForPlatform(platform)
	return ok && image != "" && image == expected
}

func expectedMonitorHash(platform string) string {
	return map[string]string{"linux/amd64": monitorAMD64SHA256, "linux/arm64": monitorARM64SHA256}[platform]
}

func validRepositoryAnchor(commit string) bool {
	if err := exec.Command("git", "cat-file", "-e", commit+"^{commit}").Run(); err != nil {
		return false
	}
	return exec.Command("git", "merge-base", "--is-ancestor", commit, "HEAD").Run() == nil
}

func validHex(value string, bytes int) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == bytes
}

func validDistinctHashes(first, second string) bool {
	if first == second {
		return false
	}
	firstBytes, firstErr := hex.DecodeString(first)
	secondBytes, secondErr := hex.DecodeString(second)
	return firstErr == nil && secondErr == nil && len(firstBytes) == sha256.Size && len(secondBytes) == sha256.Size
}

func integrationInputPaths() []string {
	paths := []string{"go.mod", "go.sum", "integration/p03/legacy-crush.txt", "integration/p03/probe/main.go", "integration/p03/report.schema.json", "integration/p03/reproduce.sh"}
	for _, root := range []string{"internal/cephx", "internal/encoding", "internal/msgr", "internal/protocol"} {
		err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !entry.IsDir() && strings.HasSuffix(path, ".go") {
				paths = append(paths, filepath.ToSlash(path))
			}
			return nil
		})
		must(err)
	}
	slices.Sort(paths)
	return paths
}

func checkFixtureDirectory() {
	entries, err := os.ReadDir("testdata/p03")
	must(err)
	want := []string{"cephx-encoding-vectors.json", "cephx-encoding-vectors.json.manifest.json", "crypto-vectors.json", "crypto-vectors.json.manifest.json"}
	got := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			fatalf("unexpected P03 fixture directory %s", entry.Name())
		}
		got = append(got, entry.Name())
	}
	slices.Sort(got)
	if !slices.Equal(got, want) {
		fatalf("P03 fixture set is %v, want %v", got, want)
	}
}

func decodeStrict(path string, target any) {
	data, err := os.ReadFile(path)
	must(err)
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	must(decoder.Decode(target))
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		fatalf("%s contains trailing JSON", path)
	}
}

func checkHash(path, expected string) {
	data, err := os.ReadFile(path)
	must(err)
	digest := sha256.Sum256(data)
	if actual := hex.EncodeToString(digest[:]); actual != expected {
		fatalf("%s hash is %s, want %s", path, actual, expected)
	}
}

func checkExecutable(path string) {
	info, err := os.Stat(path)
	must(err)
	if !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		fatalf("%s must be executable", path)
	}
}

func must(err error) {
	if err != nil {
		fatalf("%v", err)
	}
}

func fatalf(format string, arguments ...any) {
	fmt.Fprintf(os.Stderr, "p03-verify: "+format+"\n", arguments...)
	os.Exit(1)
}
