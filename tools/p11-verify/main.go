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
	monAMD64       = "10814919731d782d6510bdb0d9064cb7c04bd7a563e235aa3fa94e07f17d1215"
	monARM64       = "4b44a5660fdcc6bf7701f64fa41f707b8ff619d09042219d071e6d9ac45ab81b"
	mgrAMD64       = "4efc53a7d2a88ae6d12e2811166d564e9861e0a6b317f273dd3dd80549b5a660"
	mgrARM64       = "adf29ed6c3570427e6f2c539a850771a1b234f97bb7584abfec6ed201b10277d"
	osdAMD64       = "00bf5abfda185998e1b6a42bd58410c161345826a9d74e8b86e83d1aead9c319"
	osdARM64       = "5fbad656b7f1bd900a0c40990a81be8dff523555ed0b2c957e38f29beb33608a"
	libradosAMD64  = "0e592bc86a68b4f7bb50c1b22f645776f4c3c8e8487d2789a3573acc6ee7e887"
	libradosARM64  = "b630ea9d633d724bacfb116815915c852374d0a8ca351bdc488f0bbae4fb4bac"
)

type clientIdentity struct {
	Entity  string `json:"entity"`
	MonCaps string `json:"mon_caps"`
	MgrCaps string `json:"mgr_caps"`
	OSDCaps string `json:"osd_caps"`
}
type checks struct {
	ClusterStats        bool `json:"cluster_stats"`
	PoolStats           bool `json:"pool_stats"`
	MonitorCommand      bool `json:"monitor_command"`
	ManagerCommand      bool `json:"manager_command"`
	OSDCommand          bool `json:"osd_command"`
	PGCommand           bool `json:"pg_command"`
	PoolCreateDelete    bool `json:"pool_create_delete"`
	ApplicationMetadata bool `json:"application_metadata"`
	SessionAddresses    bool `json:"session_addresses"`
	Blocklist           bool `json:"blocklist"`
	InconsistentPGs     bool `json:"inconsistent_pgs"`
}
type adminChecks struct {
	ClusterStats                bool     `json:"cluster_stats"`
	PoolStats                   bool     `json:"pool_stats"`
	MonitorCommand              bool     `json:"monitor_command"`
	ManagerCommand              bool     `json:"manager_command"`
	OSDCommand                  bool     `json:"osd_command"`
	PGCommand                   bool     `json:"pg_command"`
	PoolCreateDelete            bool     `json:"pool_create_delete"`
	ApplicationMetadata         bool     `json:"application_metadata"`
	SessionAddresses            []string `json:"session_addresses"`
	Blocklist                   bool     `json:"blocklist"`
	InconsistentPGs             bool     `json:"inconsistent_pgs"`
	InconsistentObjects         bool     `json:"inconsistent_objects"`
	CommandErrorOutputPreserved bool     `json:"command_error_output_preserved"`
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
			ManagerSHA256 string `json:"mgr_sha256"`
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
		Network        string `json:"network"`
		OSDs           int    `json:"osds"`
		ObjectStore    string `json:"objectstore"`
		OSDDeviceBytes uint64 `json:"osd_device_bytes"`
		ManagerDaemons int    `json:"manager_daemons"`
		Pool           struct {
			Name    string `json:"name"`
			Size    int    `json:"size"`
			MinSize int    `json:"min_size"`
			PGNum   int    `json:"pg_num"`
		} `json:"pool"`
		AdminClient clientIdentity `json:"admin_client"`
		IOClient    clientIdentity `json:"io_client"`
	} `json:"cluster"`
	Scenarios struct {
		Administration                string `json:"administration"`
		NativeConformance             string `json:"native_conformance"`
		ManagerFailover               string `json:"manager_failover"`
		ManagerLossIO                 string `json:"manager_loss_io"`
		LeastPrivilege                string `json:"least_privilege"`
		DestructiveResourceValidation string `json:"destructive_resource_validation"`
	} `json:"scenarios"`
	Probe struct {
		Admin    adminChecks `json:"admin"`
		Recovery struct {
			ManagerBeforeFailover bool `json:"manager_before_failover"`
			ManagerAfterFailover  bool `json:"manager_after_failover"`
			IOAfterManagerLoss    bool `json:"io_after_manager_loss"`
		} `json:"recovery"`
		LeastPrivilege struct {
			WriteReadWithoutManager bool `json:"write_read_without_manager"`
		} `json:"least_privilege"`
	} `json:"probe"`
	Native checks `json:"native"`
}

func main() {
	value := readReport("docs/p11/integration-report.json")
	validateIdentity(value)
	validateSource(value)
	validateResults(value)
	fmt.Println("P11 verification passed")
}

func readReport(path string) report {
	file, err := os.Open(path)
	if err != nil {
		fatalf("open P11 report %q: %v", path, err)
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	var value report
	if err := decoder.Decode(&value); err != nil {
		fatalf("decode P11 report %q: %v", path, err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		fatalf("invalid P11 report JSON framing (trailing content)")
	}
	return value
}

func validateIdentity(value report) {
	start, startErr := time.Parse(time.RFC3339, value.StartedAt)
	finish, finishErr := time.Parse(time.RFC3339, value.FinishedAt)
	if value.SchemaVersion != 1 || value.Status != "passed" || value.Command != "make integration-p11" || startErr != nil || finishErr != nil || finish.Before(start) {
		fatalf("invalid P11 report envelope")
	}
	expectedImage := map[string]string{"linux/amd64": imageAMD64, "linux/arm64": imageARM64}[value.Server.Platform]
	expectedMon := map[string]string{"linux/amd64": monAMD64, "linux/arm64": monARM64}[value.Server.Platform]
	expectedMgr := map[string]string{"linux/amd64": mgrAMD64, "linux/arm64": mgrARM64}[value.Server.Platform]
	expectedOSD := map[string]string{"linux/amd64": osdAMD64, "linux/arm64": osdARM64}[value.Server.Platform]
	expectedLibrados := map[string]string{"linux/amd64": libradosAMD64, "linux/arm64": libradosARM64}[value.Server.Platform]
	expectedPackage := map[string]string{"linux/amd64": "librados2-20.2.4-0.el9.x86_64", "linux/arm64": "librados2-20.2.4-0.el9.aarch64"}[value.Server.Platform]
	if expectedImage == "" || value.Server.Repository != cephRepository || value.Server.SourceAnchorCommit != cephCommit || value.Server.Version != cephVersion || value.Server.Image != expectedImage || value.Server.Binaries.MonitorSHA256 != expectedMon || value.Server.Binaries.ManagerSHA256 != expectedMgr || value.Server.Binaries.OSDSHA256 != expectedOSD {
		fatalf("invalid P11 server identity")
	}
	if value.NativeRuntime.Soname != "librados.so.2" || !filepath.IsAbs(value.NativeRuntime.Path) || value.NativeRuntime.Package != expectedPackage || value.NativeRuntime.SHA256 != expectedLibrados {
		fatalf("invalid P11 native runtime identity")
	}
	cluster := value.Cluster
	if cluster.FSID != "21111111-2222-4333-8444-111111111111" || cluster.Network != "172.30.111.0/24" || cluster.OSDs != 1 || cluster.ObjectStore != "bluestore" || cluster.OSDDeviceBytes != 4294967296 || cluster.ManagerDaemons != 2 {
		fatalf("invalid P11 cluster identity")
	}
	if cluster.Pool.Name != "p11-data" || cluster.Pool.Size != 1 || cluster.Pool.MinSize != 1 || cluster.Pool.PGNum != 8 {
		fatalf("invalid P11 pool identity")
	}
	if cluster.AdminClient != (clientIdentity{Entity: "client.p11-admin", MonCaps: "allow *", MgrCaps: "allow *", OSDCaps: "allow *"}) {
		fatalf("invalid P11 admin identity")
	}
	if cluster.IOClient != (clientIdentity{Entity: "client.p11-io", MonCaps: "allow r", MgrCaps: "", OSDCaps: "allow rw pool=p11-data"}) {
		fatalf("invalid P11 least-privilege identity")
	}
}

func validateSource(value report) {
	if value.Source.Repository != "https://github.com/otuschhoff/rados-go.git" || value.Source.Identity != "content-addressed-artifacts" || !mapsEqual(value.Source.Artifacts, implementationHashes()) {
		fatalf("P11 report source artifacts do not match the current tree")
	}
}

func validateResults(value report) {
	for _, status := range []string{value.Scenarios.Administration, value.Scenarios.NativeConformance, value.Scenarios.ManagerFailover, value.Scenarios.ManagerLossIO, value.Scenarios.LeastPrivilege, value.Scenarios.DestructiveResourceValidation} {
		if status != "passed" {
			fatalf("P11 scenario status is not passed")
		}
	}
	admin := value.Probe.Admin
	adminResult := checks{ClusterStats: admin.ClusterStats, PoolStats: admin.PoolStats, MonitorCommand: admin.MonitorCommand, ManagerCommand: admin.ManagerCommand, OSDCommand: admin.OSDCommand, PGCommand: admin.PGCommand, PoolCreateDelete: admin.PoolCreateDelete, ApplicationMetadata: admin.ApplicationMetadata, SessionAddresses: len(admin.SessionAddresses) > 0, Blocklist: admin.Blocklist, InconsistentPGs: admin.InconsistentPGs}
	if !allChecks(adminResult) || !admin.InconsistentObjects || !admin.CommandErrorOutputPreserved {
		fatalf("invalid P11 Go administrative evidence")
	}
	if !allChecks(value.Native) {
		fatalf("invalid P11 native evidence")
	}
	if !value.Probe.Recovery.ManagerBeforeFailover || !value.Probe.Recovery.ManagerAfterFailover || !value.Probe.Recovery.IOAfterManagerLoss || !value.Probe.LeastPrivilege.WriteReadWithoutManager {
		fatalf("invalid P11 recovery evidence")
	}
}

func allChecks(value checks) bool {
	return value.ClusterStats && value.PoolStats && value.MonitorCommand && value.ManagerCommand && value.OSDCommand && value.PGCommand && value.PoolCreateDelete && value.ApplicationMetadata && value.SessionAddresses && value.Blocklist && value.InconsistentPGs
}

func implementationHashes() map[string]string {
	var paths []string
	if err := filepath.WalkDir("internal", func(path string, entry fs.DirEntry, err error) error {
		if err == nil && !entry.IsDir() && strings.HasSuffix(path, ".go") {
			paths = append(paths, path)
		}
		return err
	}); err != nil {
		fatalf("walk P11 source: %v", err)
	}
	root, err := filepath.Glob("*.go")
	if err != nil {
		fatalf("list P11 root source: %v", err)
	}
	paths = append(paths, root...)
	paths = append(paths, "Makefile", "SPEC.md", "go.mod", "go.sum", "tools/api-inventory/main.go", "tools/api-inventory/main_test.go", "integration/p11/native_driver.c", "integration/p11/probe/main.go", "integration/p11/report.schema.json", "integration/p11/reproduce.sh", "tools/p11-verify/main.go", "tools/p11-verify/main_test.go", "docs/p00/api-inventory.csv", "docs/p01/public-api.md", "docs/p11/tasks.md", "docs/p11/provenance.md")
	slices.Sort(paths)
	result := make(map[string]string, len(paths))
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			fatalf("read P11 source artifact %q: %v", path, err)
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
func fatalf(format string, values ...any) { fmt.Fprintf(os.Stderr, format+"\n", values...); os.Exit(1) }
