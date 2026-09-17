// Command p06-verify validates checked-in P06 real-cluster evidence.
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
	cephRepository = "https://github.com/ceph/ceph.git"
	cephCommit     = "7f793731f1b39eb4f465e960113d2363c311b964"
	imageAMD64     = "quay.io/ceph/ceph@sha256:09ee90f6f3e0c7b9954f71d214ee05e9bbaaaea3716b1dd619603283b829f8b8"
	imageARM64     = "quay.io/ceph/ceph@sha256:6e6bc7b28fa1b334108a3646af5533dfb50db508efdf5b358eb7dd0dd37a48aa"
	binaryAMD64    = "00bf5abfda185998e1b6a42bd58410c161345826a9d74e8b86e83d1aead9c319"
	binaryARM64    = "5fbad656b7f1bd900a0c40990a81be8dff523555ed0b2c957e38f29beb33608a"
	cephVersion    = "ceph version 20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964) tentacle (stable)"
)

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
		Version            string `json:"version"`
		Image              string `json:"image"`
		Platform           string `json:"platform"`
		BinarySHA256       string `json:"binary_sha256"`
	} `json:"server"`
	Cluster struct {
		FSID     string `json:"fsid"`
		OSDs     int    `json:"osds"`
		Pool     string `json:"pool"`
		Replicas int    `json:"replicas"`
	} `json:"cluster"`
	Scenarios map[string]string `json:"scenarios"`
	Probe     struct {
		RangedRead    bool   `json:"ranged_read"`
		FullRead      bool   `json:"full_read"`
		EmptyRead     bool   `json:"empty_read"`
		NamespaceRead bool   `json:"namespace_read"`
		LocatorRead   bool   `json:"locator_read"`
		Stat          bool   `json:"stat"`
		Missing       bool   `json:"missing"`
		PrimaryChange bool   `json:"primary_change"`
		Version       uint64 `json:"version"`
	} `json:"probe"`
}

func main() {
	file, err := os.Open("docs/p06/integration-report.json")
	must(err)
	defer file.Close()
	var value report
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	must(decoder.Decode(&value))
	start, startErr := time.Parse(time.RFC3339, value.StartedAt)
	finish, finishErr := time.Parse(time.RFC3339, value.FinishedAt)
	expectedImage := map[string]string{"linux/amd64": imageAMD64, "linux/arm64": imageARM64}[value.Server.Platform]
	expectedBinary := map[string]string{"linux/amd64": binaryAMD64, "linux/arm64": binaryARM64}[value.Server.Platform]
	if value.SchemaVersion != 1 || value.Status != "passed" || value.Command != "make integration-p06" || startErr != nil || finishErr != nil || finish.Before(start) || value.Server.Repository != cephRepository || value.Server.SourceAnchorCommit != cephCommit || value.Server.Version != cephVersion || value.Server.Image != expectedImage || value.Server.BinarySHA256 != expectedBinary {
		fatalf("invalid P06 report identity")
	}
	if value.Source.Repository != "https://github.com/otuschhoff/rados-go.git" || value.Source.Identity != "content-addressed-artifacts" || !maps.Equal(value.Source.Artifacts, implementationHashes()) {
		fatalf("P06 report source artifacts do not match the current tree")
	}
	if value.Cluster.FSID != "11111111-2222-4333-8444-666666666666" || value.Cluster.OSDs != 3 || value.Cluster.Pool != "p06-data" || value.Cluster.Replicas != 2 {
		fatalf("invalid P06 cluster identity")
	}
	for _, scenario := range []string{"native_contents", "ranged_read", "empty_read", "namespace_read", "locator_read", "stat_metadata", "missing_object", "operation_version", "primary_change"} {
		if value.Scenarios[scenario] != "passed" {
			fatalf("scenario %s did not pass", scenario)
		}
	}
	probe := value.Probe
	if !probe.RangedRead || !probe.FullRead || !probe.EmptyRead || !probe.NamespaceRead || !probe.LocatorRead || !probe.Stat || !probe.Missing || !probe.PrimaryChange || probe.Version == 0 {
		fatalf("invalid P06 probe results")
	}
	fmt.Println("P06 verification passed")
}

func implementationHashes() map[string]string {
	var paths []string
	for _, directory := range []string{"internal/cephx", "internal/crush", "internal/encoding", "internal/maps", "internal/mon", "internal/msgr", "internal/objecter", "internal/osd", "internal/protocol"} {
		must(filepath.WalkDir(directory, func(path string, entry fs.DirEntry, err error) error {
			if err == nil && !entry.IsDir() && strings.HasSuffix(path, ".go") {
				paths = append(paths, path)
			}
			return err
		}))
	}
	paths = append(paths, "Makefile", "README.md", "SPEC.md", "client.go", "client_test.go", "doc.go", "errors.go", "errors_test.go", "object.go", "integration/README.md", "integration/p06/probe/main.go", "integration/p06/report.schema.json", "integration/p06/reproduce.sh", "tools/p06-verify/main.go", "docs/p06/tasks.md", "docs/p06/provenance.md", "go.mod", "go.sum")
	slices.Sort(paths)
	result := make(map[string]string, len(paths))
	for _, path := range paths {
		data, err := os.ReadFile(path)
		must(err)
		sum := sha256.Sum256(data)
		result[path] = hex.EncodeToString(sum[:])
	}
	return result
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func fatalf(format string, values ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", values...)
	os.Exit(1)
}
