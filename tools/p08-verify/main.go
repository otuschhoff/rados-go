// Command p08-verify validates checked-in P08 real-cluster evidence.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

const (
	cephRepository = "https://github.com/ceph/ceph.git"
	cephCommit     = "7f793731f1b39eb4f465e960113d2363c311b964"
	cephVersion    = "ceph version 20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964) tentacle (stable)"

	imageAMD64     = "quay.io/ceph/ceph@sha256:09ee90f6f3e0c7b9954f71d214ee05e9bbaaaea3716b1dd619603283b829f8b8"
	imageARM64     = "quay.io/ceph/ceph@sha256:6e6bc7b28fa1b334108a3646af5533dfb50db508efdf5b358eb7dd0dd37a48aa"
	monBinaryAMD64 = "10814919731d782d6510bdb0d9064cb7c04bd7a563e235aa3fa94e07f17d1215"
	monBinaryARM64 = "4b44a5660fdcc6bf7701f64fa41f707b8ff619d09042219d071e6d9ac45ab81b"
	osdBinaryAMD64 = "00bf5abfda185998e1b6a42bd58410c161345826a9d74e8b86e83d1aead9c319"
	osdBinaryARM64 = "5fbad656b7f1bd900a0c40990a81be8dff523555ed0b2c957e38f29beb33608a"

	libradosPackageAMD64 = "librados2-20.2.4-0.el9.x86_64"
	libradosPackageARM64 = "librados2-20.2.4-0.el9.aarch64"
	libradosSHAAMD64     = "0e592bc86a68b4f7bb50c1b22f645776f4c3c8e8487d2789a3573acc6ee7e887"
	libradosSHAARM64     = "b630ea9d633d724bacfb116815915c852374d0a8ca351bdc488f0bbae4fb4bac"
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
		Binaries           struct {
			MonitorSHA256 string `json:"mon_sha256"`
			OSDSHA256     string `json:"osd_sha256"`
		} `json:"binaries"`
	} `json:"server"`
	NativeRuntime struct {
		Soname  string `json:"soname"`
		Path    string `json:"path"`
		Package string `json:"package"`
		SHA256  string `json:"sha256"`
	} `json:"native_runtime"`
	Cluster struct {
		FSID           string `json:"fsid"`
		OSDs           int    `json:"osds"`
		Pool           string `json:"pool"`
		Replicas       int    `json:"replicas"`
		OSDDeviceBytes uint64 `json:"osd_device_bytes"`
	} `json:"cluster"`
	Scenarios struct {
		MetadataInteroperability        string `json:"metadata_interoperability"`
		CompoundAtomicity               string `json:"compound_atomicity"`
		CrossClientContention           string `json:"cross_client_contention"`
		EnumerationConformance          string `json:"enumeration_conformance"`
		NamespaceIsolation              string `json:"namespace_isolation"`
		CursorPaginationAndPartitioning string `json:"cursor_pagination_and_partitioning"`
	} `json:"scenarios"`
	Probe struct {
		NativeMetadata        bool `json:"native_metadata"`
		BinaryMetadata        bool `json:"binary_metadata"`
		OMAPPagination        bool `json:"omap_pagination"`
		CompoundRead          bool `json:"compound_read"`
		CompoundAtomicity     bool `json:"compound_atomicity"`
		CrossClientContention bool `json:"cross_client_contention"`
		Enumeration           bool `json:"enumeration"`
		Namespaces            bool `json:"namespaces"`
		CursorContinuation    bool `json:"cursor_continuation"`
		CursorPartitioning    bool `json:"cursor_partitioning"`
	} `json:"probe"`
	Native struct {
		Seed struct {
			NativeBinaryMetadata bool `json:"native_binary_metadata"`
			NativeCompoundSeed   bool `json:"native_compound_seed"`
		} `json:"seed"`
		Verify struct {
			GoBinaryMetadata        bool `json:"go_binary_metadata"`
			GoOMAPNativeRead        bool `json:"go_omap_native_read"`
			GoEnumerationNativeRead bool `json:"go_enumeration_native_read"`
			NamespaceFiltering      bool `json:"namespace_filtering"`
		} `json:"verify"`
	} `json:"native"`
}

func main() {
	value := readReport("docs/p08/integration-report.json")
	validateIdentity(value)
	validateSource(value)
	validateResults(value)
	fmt.Println("P08 verification passed")
}

func readReport(path string) report {
	file, err := os.Open(path)
	if err != nil {
		fatalf("open P08 report %q: %v", path, err)
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	var value report
	if err := decoder.Decode(&value); err != nil {
		fatalf("decode P08 report %q: %v", path, err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		fatalf("invalid P08 report JSON framing (trailing content)")
	}
	return value
}

func validateIdentity(value report) {
	start, startErr := time.Parse(time.RFC3339, value.StartedAt)
	finish, finishErr := time.Parse(time.RFC3339, value.FinishedAt)
	if value.SchemaVersion != 1 || value.Status != "passed" || value.Command != "make integration-p08" || startErr != nil || finishErr != nil || finish.Before(start) {
		fatalf("invalid P08 report envelope")
	}
	expectedImage := map[string]string{"linux/amd64": imageAMD64, "linux/arm64": imageARM64}[value.Server.Platform]
	expectedMon := map[string]string{"linux/amd64": monBinaryAMD64, "linux/arm64": monBinaryARM64}[value.Server.Platform]
	expectedOSD := map[string]string{"linux/amd64": osdBinaryAMD64, "linux/arm64": osdBinaryARM64}[value.Server.Platform]
	expectedPackage := map[string]string{"linux/amd64": libradosPackageAMD64, "linux/arm64": libradosPackageARM64}[value.Server.Platform]
	expectedRuntime := map[string]string{"linux/amd64": libradosSHAAMD64, "linux/arm64": libradosSHAARM64}[value.Server.Platform]
	if expectedImage == "" || value.Server.Repository != cephRepository || value.Server.SourceAnchorCommit != cephCommit || value.Server.Version != cephVersion || value.Server.Image != expectedImage || value.Server.Binaries.MonitorSHA256 != expectedMon || value.Server.Binaries.OSDSHA256 != expectedOSD {
		fatalf("invalid P08 server identity")
	}
	if value.NativeRuntime.Soname != "librados.so.2" || !filepath.IsAbs(value.NativeRuntime.Path) || value.NativeRuntime.Package != expectedPackage || value.NativeRuntime.SHA256 != expectedRuntime {
		fatalf("invalid P08 native runtime identity")
	}
	if value.Cluster.FSID != "11111111-2222-4333-8444-888888888888" || value.Cluster.OSDs != 3 || value.Cluster.Pool != "p08-data" || value.Cluster.Replicas != 2 || value.Cluster.OSDDeviceBytes != 8589934592 {
		fatalf("invalid P08 cluster identity")
	}
}

func validateSource(value report) {
	if value.Source.Repository != "https://github.com/otuschhoff/go-librados.git" || value.Source.Identity != "content-addressed-artifacts" || !mapsEqual(value.Source.Artifacts, implementationHashes()) {
		fatalf("P08 report source artifacts do not match the current tree")
	}
}

func validateResults(value report) {
	statuses := []string{value.Scenarios.MetadataInteroperability, value.Scenarios.CompoundAtomicity, value.Scenarios.CrossClientContention, value.Scenarios.EnumerationConformance, value.Scenarios.NamespaceIsolation, value.Scenarios.CursorPaginationAndPartitioning}
	for _, status := range statuses {
		if status != "passed" {
			fatalf("P08 scenario status is not passed")
		}
	}
	probe := value.Probe
	if !probe.NativeMetadata || !probe.BinaryMetadata || !probe.OMAPPagination || !probe.CompoundRead || !probe.CompoundAtomicity || !probe.CrossClientContention || !probe.Enumeration || !probe.Namespaces || !probe.CursorContinuation || !probe.CursorPartitioning {
		fatalf("invalid P08 probe evidence")
	}
	if !value.Native.Seed.NativeBinaryMetadata || !value.Native.Seed.NativeCompoundSeed || !value.Native.Verify.GoBinaryMetadata || !value.Native.Verify.GoOMAPNativeRead || !value.Native.Verify.GoEnumerationNativeRead || !value.Native.Verify.NamespaceFiltering {
		fatalf("invalid P08 native evidence")
	}
}

func implementationHashes() map[string]string {
	var paths []string
	for _, directory := range []string{"internal/cephx", "internal/crush", "internal/encoding", "internal/maps", "internal/mon", "internal/msgr", "internal/objecter", "internal/osd", "internal/protocol"} {
		err := filepath.WalkDir(directory, func(path string, entry fs.DirEntry, err error) error {
			if err == nil && !entry.IsDir() && strings.HasSuffix(path, ".go") {
				paths = append(paths, path)
			}
			return err
		})
		if err != nil {
			fatalf("walk P08 source directory %q: %v", directory, err)
		}
	}
	paths = append(paths, "Makefile", "README.md", "SPEC.md", "client.go", "client_test.go", "doc.go", "errors.go", "errors_test.go", "object.go", "metadata.go", "enumeration_test.go", "metadata_test.go", "docs/p00/api-inventory.csv", "integration/README.md", "integration/p08/native_driver.c", "integration/p08/probe/main.go", "integration/p08/report.schema.json", "integration/p08/reproduce.sh", "tools/p08-verify/main.go", "tools/p08-verify/main_test.go", "docs/p08/tasks.md", "docs/p08/provenance.md", "go.mod", "go.sum")
	slices.Sort(paths)
	result := make(map[string]string, len(paths))
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			fatalf("read P08 source artifact %q: %v", path, err)
		}
		sum := sha256.Sum256(data)
		result[path] = hex.EncodeToString(sum[:])
	}
	return result
}

func mapsEqual(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

func fatalf(format string, values ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", values...)
	os.Exit(1)
}
