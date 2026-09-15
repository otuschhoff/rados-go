// Command p09-verify validates checked-in P09 real-cluster evidence.
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
		ClassExecution              string `json:"class_execution"`
		AmbiguousClassExecution     string `json:"ambiguous_class_execution"`
		LockInteroperability        string `json:"lock_interoperability"`
		LockLeaseLifecycle          string `json:"lock_lease_lifecycle"`
		WatchNotifyInteroperability string `json:"watch_notify_interoperability"`
		PartialTimeoutResults       string `json:"partial_timeout_results"`
		RemapReregistration         string `json:"remap_reregistration"`
		OSDRestart                  string `json:"osd_restart"`
		LostWatchObservability      string `json:"lost_watch_observability"`
		BoundedShutdown             string `json:"bounded_shutdown"`
	} `json:"scenarios"`
	Probe struct {
		ClassExecution bool `json:"class_execution"`
		LockContention bool `json:"lock_contention"`
		LockRenew      bool `json:"lock_renew"`
		LockBreak      bool `json:"lock_break"`
		LockShared     bool `json:"lock_shared"`
		LockExpiry     bool `json:"lock_expiry"`
		WatchAck       bool `json:"watch_ack"`
		NotifyTimeout  bool `json:"notify_timeout"`
		NativeLocks    bool `json:"native_locks"`
		NativeWatch    bool `json:"native_watch"`
		NativeNotify   bool `json:"native_notify"`
		WatchRemap     bool `json:"watch_remap"`
		OSDRestart     bool `json:"osd_restart"`
		WatchShutdown  bool `json:"watch_shutdown"`
		ClientShutdown bool `json:"client_shutdown"`
	} `json:"probe"`
	Native struct {
		Seed struct {
			NativeExec        bool   `json:"native_exec"`
			NativeExecResult  int    `json:"native_exec_result"`
			NativeExecOutput  string `json:"native_exec_output"`
			NativeLockSeed    bool   `json:"native_lock_seed"`
			NativeLockRenew   bool   `json:"native_lock_renew"`
			NativeLockRelease bool   `json:"native_lock_release"`
			NativeLockExpiry  bool   `json:"native_lock_expiry"`
		} `json:"seed"`
		Watch struct {
			GoNotifyNativeWatch    bool `json:"go_notify_native_watch"`
			NativeWatchRemap       bool `json:"native_watch_remap"`
			NativeWatchRestart     bool `json:"native_watch_restart"`
			NativeWatchSameCookie  bool `json:"native_watch_same_cookie"`
			NativeWatchExactlyOnce bool `json:"native_watch_exactly_once"`
			NativeLockShared       bool `json:"native_lock_shared"`
			NativeSharedRelease    bool `json:"native_shared_release"`
		} `json:"watch"`
		Notify struct {
			NativeNotifyGoWatch bool `json:"native_notify_go_watch"`
		} `json:"notify"`
		Verify struct {
			GoLockNativeRead  bool `json:"go_lock_native_read"`
			NativeBreakGoLock bool `json:"native_break_go_lock"`
		} `json:"verify"`
	} `json:"native"`
}

func main() {
	value := readReport("docs/p09/integration-report.json")
	validateIdentity(value)
	validateSource(value)
	validateResults(value)
	fmt.Println("P09 verification passed")
}

func readReport(path string) report {
	file, err := os.Open(path)
	if err != nil {
		fatalf("open P09 report %q: %v", path, err)
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	var value report
	if err := decoder.Decode(&value); err != nil {
		fatalf("decode P09 report %q: %v", path, err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		fatalf("invalid P09 report JSON framing (trailing content)")
	}
	return value
}

func validateIdentity(value report) {
	start, startErr := time.Parse(time.RFC3339, value.StartedAt)
	finish, finishErr := time.Parse(time.RFC3339, value.FinishedAt)
	if value.SchemaVersion != 1 || value.Status != "passed" || value.Command != "make integration-p09" || startErr != nil || finishErr != nil || finish.Before(start) {
		fatalf("invalid P09 report envelope")
	}
	expectedImage := map[string]string{"linux/amd64": imageAMD64, "linux/arm64": imageARM64}[value.Server.Platform]
	expectedMon := map[string]string{"linux/amd64": monBinaryAMD64, "linux/arm64": monBinaryARM64}[value.Server.Platform]
	expectedOSD := map[string]string{"linux/amd64": osdBinaryAMD64, "linux/arm64": osdBinaryARM64}[value.Server.Platform]
	expectedPackage := map[string]string{"linux/amd64": libradosPackageAMD64, "linux/arm64": libradosPackageARM64}[value.Server.Platform]
	expectedRuntime := map[string]string{"linux/amd64": libradosSHAAMD64, "linux/arm64": libradosSHAARM64}[value.Server.Platform]
	if expectedImage == "" || value.Server.Repository != cephRepository || value.Server.SourceAnchorCommit != cephCommit || value.Server.Version != cephVersion || value.Server.Image != expectedImage || value.Server.Binaries.MonitorSHA256 != expectedMon || value.Server.Binaries.OSDSHA256 != expectedOSD {
		fatalf("invalid P09 server identity")
	}
	if value.NativeRuntime.Soname != "librados.so.2" || !filepath.IsAbs(value.NativeRuntime.Path) || value.NativeRuntime.Package != expectedPackage || value.NativeRuntime.SHA256 != expectedRuntime {
		fatalf("invalid P09 native runtime identity")
	}
	if value.Cluster.FSID != "11111111-2222-4333-8444-999999999999" || value.Cluster.OSDs != 3 || value.Cluster.Pool != "p09-data" || value.Cluster.Replicas != 2 || value.Cluster.OSDDeviceBytes != 8589934592 {
		fatalf("invalid P09 cluster identity")
	}
}

func validateSource(value report) {
	if value.Source.Repository != "https://github.com/otuschhoff/go-librados.git" || value.Source.Identity != "content-addressed-artifacts" || !mapsEqual(value.Source.Artifacts, implementationHashes()) {
		fatalf("P09 report source artifacts do not match the current tree")
	}
}

func validateResults(value report) {
	statuses := []string{
		value.Scenarios.ClassExecution,
		value.Scenarios.AmbiguousClassExecution,
		value.Scenarios.LockInteroperability,
		value.Scenarios.LockLeaseLifecycle,
		value.Scenarios.WatchNotifyInteroperability,
		value.Scenarios.PartialTimeoutResults,
		value.Scenarios.RemapReregistration,
		value.Scenarios.OSDRestart,
		value.Scenarios.LostWatchObservability,
		value.Scenarios.BoundedShutdown,
	}
	for _, status := range statuses {
		if status != "passed" {
			fatalf("P09 scenario status is not passed")
		}
	}
	probe := value.Probe
	if !probe.ClassExecution || !probe.LockContention || !probe.LockRenew || !probe.LockBreak || !probe.LockShared || !probe.LockExpiry || !probe.WatchAck || !probe.NotifyTimeout || !probe.NativeLocks || !probe.NativeWatch || !probe.NativeNotify || !probe.WatchRemap || !probe.OSDRestart || !probe.WatchShutdown || !probe.ClientShutdown {
		fatalf("invalid P09 probe evidence")
	}
	seed := value.Native.Seed
	watch := value.Native.Watch
	verify := value.Native.Verify
	if !seed.NativeExec || seed.NativeExecResult <= 0 || seed.NativeExecOutput == "" || !seed.NativeLockSeed || !seed.NativeLockRenew || !seed.NativeLockRelease || !seed.NativeLockExpiry || !watch.GoNotifyNativeWatch || !watch.NativeWatchRemap || !watch.NativeWatchRestart || !watch.NativeWatchSameCookie || !watch.NativeWatchExactlyOnce || !watch.NativeLockShared || !watch.NativeSharedRelease || !value.Native.Notify.NativeNotifyGoWatch || !verify.GoLockNativeRead || !verify.NativeBreakGoLock {
		fatalf("invalid P09 native evidence")
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
			fatalf("walk P09 source directory %q: %v", directory, err)
		}
	}
	paths = append(paths,
		"Makefile", "README.md", "SPEC.md", "client.go", "client_test.go", "coordination.go", "coordination_test.go", "doc.go", "errors.go", "errors_test.go", "object.go", "metadata.go", "enumeration_test.go", "metadata_test.go",
		"docs/p00/api-inventory.csv", "integration/README.md", "integration/p09/native_driver.c", "integration/p09/probe/main.go", "integration/p09/report.schema.json", "integration/p09/reproduce.sh", "tools/p09-verify/main.go", "tools/p09-verify/main_test.go", "docs/p09/tasks.md", "docs/p09/provenance.md", "go.mod", "go.sum",
	)
	slices.Sort(paths)
	result := make(map[string]string, len(paths))
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			fatalf("read P09 source artifact %q: %v", path, err)
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
