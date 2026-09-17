// Command p10-verify validates checked-in P10 real-cluster evidence.
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

type poolIdentity struct {
	Name    string `json:"name"`
	Size    int    `json:"size"`
	MinSize int    `json:"min_size"`
	PGNum   int    `json:"pg_num"`
}

type probeReport struct {
	NamedCreateListLookup  bool   `json:"named_create_list_lookup"`
	NamedReadSnapshot      bool   `json:"named_read_snapshot"`
	NamedRollback          bool   `json:"named_rollback"`
	NamedRemove            bool   `json:"named_remove"`
	SelfManagedCreate      bool   `json:"self_managed_create"`
	SelfManagedWriteCtx    bool   `json:"self_managed_write_context"`
	SelfManagedRead        bool   `json:"self_managed_read"`
	SelfManagedRollback    bool   `json:"self_managed_rollback"`
	SelfManagedRemove      bool   `json:"self_managed_remove"`
	WriteSame              bool   `json:"write_same"`
	Checksum               bool   `json:"checksum"`
	ChecksumHex            string `json:"checksum_hex"`
	AllocationHint         bool   `json:"allocation_hint"`
	SparseRead             bool   `json:"sparse_read"`
	CopyFrom               bool   `json:"copy_from"`
	CopyFrom2              bool   `json:"copy_from2"`
	ReplicatedCapabilities bool   `json:"replicated_capabilities"`
	ECCapabilities         bool   `json:"ec_capabilities"`
	ECWriteRead            bool   `json:"ec_write_read"`
	ECOverwriteRejected    bool   `json:"ec_overwrite_rejected"`
	ECWriteSameRejected    bool   `json:"ec_write_same_rejected"`
	ECChecksum             bool   `json:"ec_checksum"`
	ECAllocationHint       bool   `json:"ec_allocation_hint"`
	ECSparseRead           bool   `json:"ec_sparse_read"`
	ECCopyFrom             bool   `json:"ec_copy_from"`
	ECOMAPRejected         bool   `json:"ec_omap_rejected"`
	ECAlignmentEvidence    bool   `json:"ec_alignment_evidence"`
	RequiredAlignment      uint64 `json:"required_alignment"`
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
		FSID            string       `json:"fsid"`
		Network         string       `json:"network"`
		OSDs            int          `json:"osds"`
		ObjectStore     string       `json:"objectstore"`
		OSDDeviceBytes  uint64       `json:"osd_device_bytes"`
		ReplicatedRule  string       `json:"replicated_rule"`
		NamedPool       poolIdentity `json:"named_pool"`
		SelfManagedPool poolIdentity `json:"self_managed_pool"`
		ECPool          struct {
			Name              string `json:"name"`
			Size              int    `json:"size"`
			MinSize           int    `json:"min_size"`
			PGNum             int    `json:"pg_num"`
			Profile           string `json:"profile"`
			Rule              string `json:"rule"`
			Plugin            string `json:"plugin"`
			K                 int    `json:"k"`
			M                 int    `json:"m"`
			FailureDomain     string `json:"failure_domain"`
			AllowECOverwrites bool   `json:"allow_ec_overwrites"`
			StripeUnit        uint64 `json:"stripe_unit"`
			StripeWidth       uint64 `json:"stripe_width"`
		} `json:"ec_pool"`
		Client struct {
			Entity  string `json:"entity"`
			MonCaps string `json:"mon_caps"`
			OSDCaps string `json:"osd_caps"`
		} `json:"client"`
	} `json:"cluster"`
	Scenarios struct {
		NamedSnapshots         string `json:"named_snapshots"`
		SelfManagedSnapshots   string `json:"self_managed_snapshots"`
		SpecializedIO          string `json:"specialized_io"`
		ErasureCodedIO         string `json:"erasure_coded_io"`
		NativeInteroperability string `json:"native_interoperability"`
	} `json:"scenarios"`
	Probe  probeReport `json:"probe"`
	Native struct {
		Seed struct {
			NamedMetadata  bool `json:"named_create_list_lookup_name_stamp"`
			NamedLifecycle bool `json:"named_read_rollback_remove"`
			SelfLifecycle  bool `json:"self_managed_create_write_read_rollback_remove"`
		} `json:"seed"`
		Verify struct {
			GoNamedSnapshot bool   `json:"go_named_snapshot"`
			GoNamedHead     bool   `json:"go_named_head"`
			GoCopy          bool   `json:"go_copy"`
			GoCopyFrom2     bool   `json:"go_copy_from2"`
			GoECCopy        bool   `json:"go_ec_copy"`
			GoChecksumHex   string `json:"go_checksum_hex"`
		} `json:"verify"`
	} `json:"native"`
}

func main() {
	value := readReport("docs/p10/integration-report.json")
	validateIdentity(value)
	validateSource(value)
	validateResults(value)
	fmt.Println("P10 verification passed")
}

func readReport(path string) report {
	file, err := os.Open(path)
	if err != nil {
		fatalf("open P10 report %q: %v", path, err)
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	var value report
	if err := decoder.Decode(&value); err != nil {
		fatalf("decode P10 report %q: %v", path, err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		fatalf("invalid P10 report JSON framing (trailing content)")
	}
	return value
}

func validateIdentity(value report) {
	start, startErr := time.Parse(time.RFC3339, value.StartedAt)
	finish, finishErr := time.Parse(time.RFC3339, value.FinishedAt)
	if value.SchemaVersion != 1 || value.Status != "passed" || value.Command != "make integration-p10" || startErr != nil || finishErr != nil || finish.Before(start) {
		fatalf("invalid P10 report envelope")
	}
	expectedImage := map[string]string{"linux/amd64": imageAMD64, "linux/arm64": imageARM64}[value.Server.Platform]
	expectedMon := map[string]string{"linux/amd64": monBinaryAMD64, "linux/arm64": monBinaryARM64}[value.Server.Platform]
	expectedOSD := map[string]string{"linux/amd64": osdBinaryAMD64, "linux/arm64": osdBinaryARM64}[value.Server.Platform]
	expectedPackage := map[string]string{"linux/amd64": libradosPackageAMD64, "linux/arm64": libradosPackageARM64}[value.Server.Platform]
	expectedRuntime := map[string]string{"linux/amd64": libradosSHAAMD64, "linux/arm64": libradosSHAARM64}[value.Server.Platform]
	if expectedImage == "" || value.Server.Repository != cephRepository || value.Server.SourceAnchorCommit != cephCommit || value.Server.Version != cephVersion || value.Server.Image != expectedImage || value.Server.Binaries.MonitorSHA256 != expectedMon || value.Server.Binaries.OSDSHA256 != expectedOSD {
		fatalf("invalid P10 server identity")
	}
	if value.NativeRuntime.Soname != "librados.so.2" || !filepath.IsAbs(value.NativeRuntime.Path) || value.NativeRuntime.Package != expectedPackage || value.NativeRuntime.SHA256 != expectedRuntime {
		fatalf("invalid P10 native runtime identity")
	}
	cluster := value.Cluster
	if cluster.FSID != "21000000-2222-4333-8444-101010101010" || cluster.Network != "172.30.110.0/24" || cluster.OSDs != 3 || cluster.ObjectStore != "bluestore" || cluster.OSDDeviceBytes != 8589934592 || cluster.ReplicatedRule != "p10-replicated-rule" {
		fatalf("invalid P10 cluster identity")
	}
	if cluster.NamedPool != (poolIdentity{Name: "p10-named", Size: 2, MinSize: 1, PGNum: 16}) || cluster.SelfManagedPool != (poolIdentity{Name: "p10-self", Size: 2, MinSize: 1, PGNum: 16}) {
		fatalf("invalid P10 replicated pools")
	}
	ec := cluster.ECPool
	if ec.Name != "p10-ec" || ec.Size != 3 || ec.MinSize != 2 || ec.PGNum != 16 || ec.Profile != "p10-ec-profile" || ec.Rule != "p10-ec-rule" || ec.Plugin != "jerasure" || ec.K != 2 || ec.M != 1 || ec.FailureDomain != "osd" || ec.AllowECOverwrites || ec.StripeUnit != 4096 || ec.StripeWidth != 8192 {
		fatalf("invalid P10 EC pool")
	}
	if cluster.Client.Entity != "client.p10" || cluster.Client.MonCaps != "allow rw" || cluster.Client.OSDCaps != "allow rwx pool=p10-named, allow rwx pool=p10-self, allow rwx pool=p10-ec" {
		fatalf("invalid P10 client caps")
	}
}

func validateSource(value report) {
	if value.Source.Repository != "https://github.com/otuschhoff/rados-go.git" || value.Source.Identity != "content-addressed-artifacts" || !mapsEqual(value.Source.Artifacts, implementationHashes()) {
		fatalf("P10 report source artifacts do not match the current tree")
	}
}

func validateResults(value report) {
	statuses := []string{value.Scenarios.NamedSnapshots, value.Scenarios.SelfManagedSnapshots, value.Scenarios.SpecializedIO, value.Scenarios.ErasureCodedIO, value.Scenarios.NativeInteroperability}
	for _, status := range statuses {
		if status != "passed" {
			fatalf("P10 scenario status is not passed")
		}
	}
	probe := value.Probe
	booleans := []bool{probe.NamedCreateListLookup, probe.NamedReadSnapshot, probe.NamedRollback, probe.NamedRemove, probe.SelfManagedCreate, probe.SelfManagedWriteCtx, probe.SelfManagedRead, probe.SelfManagedRollback, probe.SelfManagedRemove, probe.WriteSame, probe.Checksum, probe.AllocationHint, probe.SparseRead, probe.CopyFrom, probe.CopyFrom2, probe.ReplicatedCapabilities, probe.ECCapabilities, probe.ECWriteRead, probe.ECOverwriteRejected, probe.ECWriteSameRejected, probe.ECChecksum, probe.ECAllocationHint, probe.ECSparseRead, probe.ECCopyFrom, probe.ECOMAPRejected, probe.ECAlignmentEvidence}
	for _, passed := range booleans {
		if !passed {
			fatalf("invalid P10 probe evidence")
		}
	}
	if probe.ChecksumHex != "02000000f5be862af5be862a" || probe.RequiredAlignment == 0 || probe.RequiredAlignment != value.Cluster.ECPool.StripeWidth {
		fatalf("invalid P10 probe data")
	}
	seed, verify := value.Native.Seed, value.Native.Verify
	if !seed.NamedMetadata || !seed.NamedLifecycle || !seed.SelfLifecycle || !verify.GoNamedSnapshot || !verify.GoNamedHead || !verify.GoCopy || !verify.GoCopyFrom2 || !verify.GoECCopy || verify.GoChecksumHex != value.Probe.ChecksumHex {
		fatalf("invalid P10 native evidence")
	}
}

func implementationHashes() map[string]string {
	var paths []string
	for _, directory := range []string{"internal", "testdata/p10"} {
		err := filepath.WalkDir(directory, func(path string, entry fs.DirEntry, err error) error {
			if err == nil && !entry.IsDir() && (directory == "testdata/p10" || strings.HasSuffix(path, ".go")) {
				paths = append(paths, path)
			}
			return err
		})
		if err != nil {
			fatalf("walk P10 source directory %q: %v", directory, err)
		}
	}
	root, err := filepath.Glob("*.go")
	if err != nil {
		fatalf("list P10 root source: %v", err)
	}
	paths = append(paths, root...)
	paths = append(paths, "Makefile", "SPEC.md", "go.mod", "go.sum", "tools/api-inventory/main.go", "tools/api-inventory/main_test.go", "integration/p10/native_driver.c", "integration/p10/probe/main.go", "integration/p10/report.schema.json", "integration/p10/reproduce.sh", "tools/p10-verify/main.go", "tools/p10-verify/main_test.go", "docs/p00/api-inventory.csv", "docs/p01/public-api.md", "docs/p10/tasks.md", "docs/p10/provenance.md")
	slices.Sort(paths)
	result := make(map[string]string, len(paths))
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			fatalf("read P10 source artifact %q: %v", path, err)
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
