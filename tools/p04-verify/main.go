// Command p04-verify validates checked-in P04 evidence.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

const (
	repository = "https://github.com/ceph/ceph.git"
	commit     = "7f793731f1b39eb4f465e960113d2363c311b964"
	imageAMD64 = "quay.io/ceph/ceph@sha256:09ee90f6f3e0c7b9954f71d214ee05e9bbaaaea3716b1dd619603283b829f8b8"
	imageARM64 = "quay.io/ceph/ceph@sha256:6e6bc7b28fa1b334108a3646af5533dfb50db508efdf5b358eb7dd0dd37a48aa"
)

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
		References []struct {
			Type     string `json:"type"`
			Locator  string `json:"locator"`
			Revision string `json:"revision"`
			Role     string `json:"role"`
		} `json:"references"`
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

type report struct {
	SchemaVersion int    `json:"schema_version"`
	Status        string `json:"status"`
	Command       string `json:"command"`
	StartedAt     string `json:"started_at"`
	FinishedAt    string `json:"finished_at"`
	Source        struct {
		Repository string            `json:"repository"`
		Identity   string            `json:"identity"`
		Artifacts  map[string]string `json:"artifacts"`
	} `json:"source"`
	Server struct {
		Repository         string `json:"repository"`
		SourceAnchorCommit string `json:"source_anchor_commit"`
		Image              string `json:"image"`
		Platform           string `json:"platform"`
	} `json:"server"`
	Scenarios map[string]string `json:"scenarios"`
	Probe     struct {
		FSID                    string   `json:"fsid"`
		InitialEpoch            uint32   `json:"initial_epoch"`
		MutationEpoch           uint32   `json:"mutation_epoch"`
		FailoverEpoch           uint32   `json:"failover_epoch"`
		Pools                   []string `json:"pools"`
		IncrementalObserved     bool     `json:"incremental_observed"`
		FullIncrementEquivalent bool     `json:"full_incremental_equivalent"`
		MonitorLossRecovered    bool     `json:"monitor_loss_recovered"`
		PostFailoverCommand     bool     `json:"post_failover_command"`
		ForeignFSIDRejected     bool     `json:"foreign_fsid_rejected"`
	} `json:"probe"`
}

func main() {
	paths, err := filepath.Glob("testdata/p04/*.manifest.json")
	must(err)
	if len(paths) != 3 {
		fatalf("found %d fixture manifests, want 3", len(paths))
	}
	for _, path := range paths {
		checkManifest(path)
	}
	var value report
	decode("docs/p04/integration-report.json", &value)
	start, startErr := time.Parse(time.RFC3339, value.StartedAt)
	finish, finishErr := time.Parse(time.RFC3339, value.FinishedAt)
	expectedImage := map[string]string{"linux/amd64": imageAMD64, "linux/arm64": imageARM64}[value.Server.Platform]
	if value.SchemaVersion != 1 || value.Status != "passed" || value.Command != "make integration-p04" || startErr != nil || finishErr != nil || finish.Before(start) || value.Server.Repository != repository || value.Server.SourceAnchorCommit != commit || value.Server.Image != expectedImage {
		fatalf("invalid integration report identity")
	}
	if value.Source.Repository != "https://github.com/otuschhoff/rados-go.git" || value.Source.Identity != "content-addressed-artifacts" || !maps.Equal(value.Source.Artifacts, implementationHashes()) {
		fatalf("integration report source artifacts do not match the current tree")
	}
	for _, scenario := range []string{"m0", "pool_report", "map_change", "monitor_loss", "foreign_fsid", "convergence", "read_only_command"} {
		if value.Scenarios[scenario] != "passed" {
			fatalf("scenario %s did not pass", scenario)
		}
	}
	probe := value.Probe
	if probe.FSID != "11111111-2222-4333-8444-555555555555" || !(probe.InitialEpoch < probe.MutationEpoch && probe.MutationEpoch < probe.FailoverEpoch) || !probe.IncrementalObserved || !probe.FullIncrementEquivalent || !probe.MonitorLossRecovered || !probe.PostFailoverCommand || !probe.ForeignFSIDRejected {
		fatalf("invalid probe evidence")
	}
	for _, pool := range []string{"p04-initial", "p04-mutated", "p04-failover"} {
		if !slices.Contains(probe.Pools, pool) {
			fatalf("missing pool %s", pool)
		}
	}
	fmt.Println("P04 verification passed")
}

func checkManifest(path string) {
	var value manifest
	decode(path, &value)
	data, err := os.ReadFile(filepath.Join(filepath.Dir(path), value.Fixture))
	must(err)
	sum := sha256.Sum256(data)
	expectedPaths := map[string][]string{
		"monmap-v9.bin":             {"src/mon/MonMap.h", "src/mon/MonMap.cc"},
		"osdmap-v8.bin":             {"src/osd/OSDMap.h", "src/osd/OSDMap.cc"},
		"osdmap-incremental-v8.bin": {"src/osd/OSDMap.h", "src/osd/OSDMap.cc"},
	}[value.Fixture]
	expectedHashes := map[string]string{
		"src/mon/MonMap.h":  "0e7f2900cca378f4cad87cb406d594ba63fb0bb0b77c3f00fabe0ff59a94d430",
		"src/mon/MonMap.cc": "3d6c7bd935f3b4042b796f6f164d75da3c9a6763ed87e2cedacfc68764958b98",
		"src/osd/OSDMap.h":  "fd23f57b98235d6c36af2f2559c4bf3082c47d93a463b4cd6c84b7f91267cf36",
		"src/osd/OSDMap.cc": "36b74d8c998da9c93b7b0c9ba3ecac5d3e9df26bfb99c2f22095b37da44e0dc8",
	}
	if value.SchemaVersion != 1 || value.Kind != "upstream-derived" || value.Source.Repository != repository || value.Source.Commit != commit || value.SHA256 != hex.EncodeToString(sum[:]) || !slices.Equal(value.Source.Paths, expectedPaths) || value.Generator.Tool != "ceph-dencoder" || value.Generator.Version != "ceph version 20.2.4" || value.Generator.Image != imageARM64 || !slices.Equal(value.Generator.Command, []string{"./integration/p04/reproduce-fixtures.sh"}) || value.Secrets.ContainsSecrets || !value.Secrets.SyntheticOnly || value.License.UpstreamExpression != "LGPL-2.1-or-later" || value.License.Redistribution != "pending" || value.License.ReviewedBy != "pending" {
		fatalf("invalid manifest %s", path)
	}
	if len(value.Source.Files) != len(expectedPaths) || len(value.Source.References) != 1 {
		fatalf("incomplete provenance in %s", path)
	}
	for _, sourcePath := range expectedPaths {
		if value.Source.Files[sourcePath] != expectedHashes[sourcePath] {
			fatalf("invalid source hash for %s in %s", sourcePath, path)
		}
	}
	reference := value.Source.References[0]
	if reference.Type != "ceph-source" || reference.Locator != repository || reference.Revision != commit || reference.Role == "" {
		fatalf("invalid source reference in %s", path)
	}
}

func implementationHashes() map[string]string {
	var paths []string
	for _, directory := range []string{"internal/cephx", "internal/encoding", "internal/maps", "internal/mon", "internal/msgr", "internal/protocol"} {
		must(filepath.WalkDir(directory, func(path string, entry fs.DirEntry, err error) error {
			if err == nil && !entry.IsDir() && strings.HasSuffix(path, ".go") {
				paths = append(paths, path)
			}
			return err
		}))
	}
	paths = append(paths, "Makefile", "go.mod", "go.sum", "integration/p04/probe/main.go", "integration/p04/report.schema.json", "integration/p04/reproduce-fixtures.sh", "integration/p04/reproduce.sh")
	result := make(map[string]string, len(paths))
	for _, path := range paths {
		data, err := os.ReadFile(path)
		must(err)
		sum := sha256.Sum256(data)
		result[path] = hex.EncodeToString(sum[:])
	}
	return result
}
func decode(path string, target any) {
	file, err := os.Open(path)
	must(err)
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	must(decoder.Decode(target))
}
func must(err error) {
	if err != nil {
		panic(err)
	}
}
func fatalf(format string, values ...any) { fmt.Fprintf(os.Stderr, format+"\n", values...); os.Exit(1) }
