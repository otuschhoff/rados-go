// Command p07-verify validates checked-in P07 real-cluster evidence.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"math"
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
		GoCRUD                   string `json:"go_crud"`
		NativeCRUD               string `json:"native_crud"`
		NativeSeedGoMutateVerify string `json:"native_seed_go_mutate_native_verify"`
		GoWriteNativeRead        string `json:"go_write_native_read"`
		ExclusiveCreate          string `json:"exclusive_create"`
		MissingSemantics         string `json:"missing_semantics"`
		Flush                    string `json:"flush"`
		PrimaryChangeAppendOnce  string `json:"primary_change_append_once"`
	} `json:"scenarios"`
	Probe struct {
		Create        bool   `json:"create"`
		Exclusive     bool   `json:"exclusive"`
		Write         bool   `json:"write"`
		WriteFull     bool   `json:"write_full"`
		Append        bool   `json:"append"`
		Truncate      bool   `json:"truncate"`
		Zero          bool   `json:"zero"`
		Remove        bool   `json:"remove"`
		Flush         bool   `json:"flush"`
		PrimaryChange bool   `json:"primary_change"`
		Version       uint64 `json:"version"`
	} `json:"probe"`
	Native struct {
		Seed struct {
			NativeCRUD bool `json:"native_crud"`
			MixedSeed  bool `json:"mixed_seed"`
		} `json:"seed"`
		Verify struct {
			MixedGoNative     bool `json:"mixed_go_native"`
			GoWriteNativeRead bool `json:"go_write_native_read"`
			RemapAppendOnce   bool `json:"remap_append_once"`
		} `json:"verify"`
	} `json:"native"`
	Benchmark struct {
		ExecutionEnvironment struct {
			Kernel              string `json:"kernel"`
			CPUModel            string `json:"cpu_model"`
			LogicalCPUs         int    `json:"logical_cpus"`
			CPUMax              string `json:"cpu_max"`
			MemoryMax           string `json:"memory_max"`
			DockerServerVersion string `json:"docker_server_version"`
		} `json:"execution_environment"`
		Methodology struct {
			SizesBytes            []uint64 `json:"sizes_bytes"`
			Concurrency           []int    `json:"concurrency"`
			Workloads             []string `json:"workloads"`
			OperationsPerWorker   uint64   `json:"operations_per_worker"`
			Transports            []string `json:"transports"`
			AllocationMeasurement struct {
				Go     string `json:"go"`
				Native string `json:"native"`
			} `json:"allocation_measurement"`
			ResultsAreBaselineNotParity bool `json:"results_are_baseline_not_parity_claim"`
		} `json:"methodology"`
		Runs []benchmarkRun `json:"runs"`
	} `json:"benchmark"`
}

type benchmarkRun struct {
	Implementation string      `json:"implementation"`
	Transport      string      `json:"transport"`
	Environment    environment `json:"environment"`
	Resources      resources   `json:"resources"`
	Rows           []row       `json:"rows"`
}

type environment struct {
	GOOS      *string `json:"GOOS,omitempty"`
	GOARCH    *string `json:"GOARCH,omitempty"`
	GoVersion *string `json:"go_version,omitempty"`
	Library   *string `json:"library,omitempty"`
}

type resources struct {
	CPUUserNS      uint64  `json:"cpu_user_ns"`
	CPUSystemNS    uint64  `json:"cpu_system_ns"`
	Allocations    *uint64 `json:"allocations"`
	AllocatedBytes *uint64 `json:"allocated_bytes"`
	MaxRSSBytes    uint64  `json:"max_rss_bytes"`
}

type row struct {
	SizeBytes                uint64  `json:"size_bytes"`
	Concurrency              int     `json:"concurrency"`
	Workload                 string  `json:"workload"`
	Operations               uint64  `json:"operations"`
	Bytes                    uint64  `json:"bytes"`
	ElapsedNS                uint64  `json:"elapsed_ns"`
	ThroughputBytesPerSecond float64 `json:"throughput_bytes_per_second"`
	IOPS                     float64 `json:"iops"`
	P50NS                    uint64  `json:"p50_ns"`
	P95NS                    uint64  `json:"p95_ns"`
	P99NS                    uint64  `json:"p99_ns"`
}

func main() {
	value := readReport("docs/p07/integration-report.json")
	validateIdentity(value)
	validateSource(value)
	validateClusterAndScenarios(value)
	validateProbeAndNative(value)
	validateBenchmark(value)
	fmt.Println("P07 verification passed")
}

func readReport(path string) report {
	file, err := os.Open(path)
	if err != nil {
		fatalf("open P07 report %q: %v", path, err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()

	var value report
	if err := decoder.Decode(&value); err != nil {
		fatalf("decode P07 report %q: %v", path, err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		fatalf("invalid P07 report JSON framing (trailing content)")
	}
	return value
}

func validateIdentity(value report) {
	start, startErr := time.Parse(time.RFC3339, value.StartedAt)
	finish, finishErr := time.Parse(time.RFC3339, value.FinishedAt)
	if value.SchemaVersion != 1 || value.Status != "passed" || value.Command != "make integration-p07" || startErr != nil || finishErr != nil || finish.Before(start) {
		fatalf("invalid P07 report envelope")
	}

	expectedImage := map[string]string{"linux/amd64": imageAMD64, "linux/arm64": imageARM64}[value.Server.Platform]
	expectedMonBinary := map[string]string{"linux/amd64": monBinaryAMD64, "linux/arm64": monBinaryARM64}[value.Server.Platform]
	expectedOSDBinary := map[string]string{"linux/amd64": osdBinaryAMD64, "linux/arm64": osdBinaryARM64}[value.Server.Platform]
	expectedRuntimePackage := map[string]string{"linux/amd64": libradosPackageAMD64, "linux/arm64": libradosPackageARM64}[value.Server.Platform]
	expectedRuntimeSHA := map[string]string{"linux/amd64": libradosSHAAMD64, "linux/arm64": libradosSHAARM64}[value.Server.Platform]

	if expectedImage == "" || expectedMonBinary == "" || expectedOSDBinary == "" || expectedRuntimePackage == "" || expectedRuntimeSHA == "" {
		fatalf("unsupported P07 platform %q", value.Server.Platform)
	}
	if value.Server.Repository != cephRepository || value.Server.SourceAnchorCommit != cephCommit || value.Server.Version != cephVersion || value.Server.Image != expectedImage || value.Server.Binaries.MonitorSHA256 != expectedMonBinary || value.Server.Binaries.OSDSHA256 != expectedOSDBinary {
		fatalf("invalid P07 server identity")
	}
	if value.NativeRuntime.Soname != "librados.so.2" || !filepath.IsAbs(value.NativeRuntime.Path) || value.NativeRuntime.Package == "" || len(value.NativeRuntime.SHA256) != 64 {
		fatalf("invalid P07 native runtime shape")
	}
	if value.NativeRuntime.Package != expectedRuntimePackage || value.NativeRuntime.SHA256 != expectedRuntimeSHA {
		fatalf("invalid P07 native runtime identity")
	}
}

func validateSource(value report) {
	if value.Source.Repository != "https://github.com/otuschhoff/go-librados.git" || value.Source.Identity != "content-addressed-artifacts" {
		fatalf("invalid P07 source identity")
	}
	if !mapsEqual(value.Source.Artifacts, implementationHashes()) {
		fatalf("P07 report source artifacts do not match the current tree")
	}
}

func validateClusterAndScenarios(value report) {
	if value.Cluster.FSID != "11111111-2222-4333-8444-777777777777" || value.Cluster.OSDs != 3 || value.Cluster.Pool != "p07-data" || value.Cluster.Replicas != 2 || value.Cluster.OSDDeviceBytes != 8589934592 {
		fatalf("invalid P07 cluster identity")
	}

	scenarios := []string{
		value.Scenarios.GoCRUD,
		value.Scenarios.NativeCRUD,
		value.Scenarios.NativeSeedGoMutateVerify,
		value.Scenarios.GoWriteNativeRead,
		value.Scenarios.ExclusiveCreate,
		value.Scenarios.MissingSemantics,
		value.Scenarios.Flush,
		value.Scenarios.PrimaryChangeAppendOnce,
	}
	for _, status := range scenarios {
		if status != "passed" {
			fatalf("P07 scenario status is not passed")
		}
	}
}

func validateProbeAndNative(value report) {
	probe := value.Probe
	if !probe.Create || !probe.Exclusive || !probe.Write || !probe.WriteFull || !probe.Append || !probe.Truncate || !probe.Zero || !probe.Remove || !probe.Flush || !probe.PrimaryChange || probe.Version == 0 {
		fatalf("invalid P07 probe results")
	}

	if !value.Native.Seed.NativeCRUD || !value.Native.Seed.MixedSeed {
		fatalf("invalid P07 native seed evidence")
	}
	if !value.Native.Verify.MixedGoNative || !value.Native.Verify.GoWriteNativeRead || !value.Native.Verify.RemapAppendOnce {
		fatalf("invalid P07 native verify evidence")
	}
}

func validateBenchmark(value report) {
	execution := value.Benchmark.ExecutionEnvironment
	if strings.TrimSpace(execution.Kernel) == "" || strings.TrimSpace(execution.CPUModel) == "" || execution.LogicalCPUs < 1 || strings.TrimSpace(execution.CPUMax) == "" || strings.TrimSpace(execution.MemoryMax) == "" || strings.TrimSpace(execution.DockerServerVersion) == "" {
		fatalf("invalid P07 benchmark execution environment")
	}
	method := value.Benchmark.Methodology
	if !slices.Equal(method.SizesBytes, []uint64{4096, 65536, 1048576, 4194304}) {
		fatalf("invalid P07 benchmark sizes")
	}
	if !slices.Equal(method.Concurrency, []int{1, 16, 64}) {
		fatalf("invalid P07 benchmark concurrency")
	}
	if !slices.Equal(method.Workloads, []string{"read", "write", "mixed"}) {
		fatalf("invalid P07 benchmark workloads")
	}
	if method.OperationsPerWorker != 2 || !slices.Equal(method.Transports, []string{"secure", "crc"}) || !method.ResultsAreBaselineNotParity {
		fatalf("invalid P07 benchmark methodology")
	}
	if method.AllocationMeasurement.Go != "runtime.MemStats deltas" || method.AllocationMeasurement.Native != "unavailable from the dynamically loaded librados ABI" {
		fatalf("invalid P07 allocation measurement methodology")
	}
	if len(value.Benchmark.Runs) != 4 {
		fatalf("invalid P07 benchmark run count")
	}

	runSeen := map[string]int{}
	expectedGOARCH := map[string]string{"linux/amd64": "amd64", "linux/arm64": "arm64"}[value.Server.Platform]

	for _, run := range value.Benchmark.Runs {
		key := run.Implementation + "/" + run.Transport
		runSeen[key]++

		if run.Implementation != "go" && run.Implementation != "native" {
			fatalf("invalid run implementation %q", run.Implementation)
		}
		if run.Transport != "secure" && run.Transport != "crc" {
			fatalf("invalid run transport %q", run.Transport)
		}

		env := run.Environment
		if run.Resources.MaxRSSBytes == 0 || run.Resources.CPUUserNS == 0 && run.Resources.CPUSystemNS == 0 {
			fatalf("missing resource counters for %s/%s", run.Implementation, run.Transport)
		}
		if run.Implementation == "go" {
			if env.GOOS == nil || env.GOARCH == nil || env.GoVersion == nil || env.Library != nil {
				fatalf("invalid go benchmark environment shape")
			}
			if *env.GOOS != "linux" || (*env.GOARCH != "amd64" && *env.GOARCH != "arm64") || strings.TrimSpace(*env.GoVersion) == "" {
				fatalf("invalid go benchmark environment values")
			}
			if *env.GOARCH != expectedGOARCH {
				fatalf("go GOARCH %q does not match server platform %q", *env.GOARCH, value.Server.Platform)
			}
			if run.Resources.Allocations == nil || run.Resources.AllocatedBytes == nil || *run.Resources.Allocations == 0 || *run.Resources.AllocatedBytes == 0 {
				fatalf("go benchmark resources must include allocation counters")
			}
		} else {
			if env.Library == nil || *env.Library != "librados.so.2" || env.GOOS != nil || env.GOARCH != nil || env.GoVersion != nil {
				fatalf("invalid native benchmark environment")
			}
			if run.Resources.Allocations != nil || run.Resources.AllocatedBytes != nil {
				fatalf("native benchmark allocations must be null")
			}
		}

		validateRows(run)
	}

	expectedRuns := []string{"go/secure", "go/crc", "native/secure", "native/crc"}
	for _, key := range expectedRuns {
		if runSeen[key] != 1 {
			fatalf("expected exactly one %s run", key)
		}
	}
}

func validateRows(run benchmarkRun) {
	if len(run.Rows) != 36 {
		fatalf("invalid row count for %s/%s", run.Implementation, run.Transport)
	}

	validSizes := map[uint64]bool{4096: true, 65536: true, 1048576: true, 4194304: true}
	validConcurrency := map[int]bool{1: true, 16: true, 64: true}
	validWorkloads := map[string]bool{"read": true, "write": true, "mixed": true}
	seen := make(map[string]bool, len(run.Rows))

	for _, row := range run.Rows {
		if !validSizes[row.SizeBytes] || !validConcurrency[row.Concurrency] || !validWorkloads[row.Workload] {
			fatalf("invalid row dimensions in %s/%s", run.Implementation, run.Transport)
		}
		expectedOps := uint64(2 * row.Concurrency)
		if row.Operations != expectedOps {
			fatalf("invalid operations for size=%d concurrency=%d workload=%s", row.SizeBytes, row.Concurrency, row.Workload)
		}
		expectedBytes := row.SizeBytes * row.Operations
		if row.Bytes != expectedBytes {
			fatalf("invalid bytes for size=%d concurrency=%d workload=%s", row.SizeBytes, row.Concurrency, row.Workload)
		}
		if row.ElapsedNS == 0 || row.P50NS == 0 || row.P95NS == 0 || row.P99NS == 0 {
			fatalf("non-positive latency metrics for size=%d concurrency=%d workload=%s", row.SizeBytes, row.Concurrency, row.Workload)
		}
		if row.P50NS > row.P95NS || row.P95NS > row.P99NS {
			fatalf("latency ordering invalid for size=%d concurrency=%d workload=%s", row.SizeBytes, row.Concurrency, row.Workload)
		}
		if !isFinitePositive(row.IOPS) || !isFinitePositive(row.ThroughputBytesPerSecond) {
			fatalf("non-finite throughput metrics for size=%d concurrency=%d workload=%s", row.SizeBytes, row.Concurrency, row.Workload)
		}
		expectedThroughput := float64(row.Bytes) * 1e9 / float64(row.ElapsedNS)
		expectedIOPS := float64(row.Operations) * 1e9 / float64(row.ElapsedNS)
		if !metricsEqual(row.ThroughputBytesPerSecond, expectedThroughput) || !metricsEqual(row.IOPS, expectedIOPS) {
			fatalf("derived metrics do not match counters for size=%d concurrency=%d workload=%s", row.SizeBytes, row.Concurrency, row.Workload)
		}

		key := fmt.Sprintf("%d/%d/%s", row.SizeBytes, row.Concurrency, row.Workload)
		if seen[key] {
			fatalf("duplicate benchmark row %s in %s/%s", key, run.Implementation, run.Transport)
		}
		seen[key] = true
	}

	for _, size := range []uint64{4096, 65536, 1048576, 4194304} {
		for _, conc := range []int{1, 16, 64} {
			for _, workload := range []string{"read", "write", "mixed"} {
				key := fmt.Sprintf("%d/%d/%s", size, conc, workload)
				if !seen[key] {
					fatalf("missing benchmark row %s in %s/%s", key, run.Implementation, run.Transport)
				}
			}
		}
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
			fatalf("walk P07 source directory %q: %v", directory, err)
		}
	}
	paths = append(paths,
		"Makefile",
		"README.md",
		"SPEC.md",
		"client.go",
		"client_test.go",
		"doc.go",
		"errors.go",
		"errors_test.go",
		"object.go",
		"docs/p00/api-inventory.csv",
		"integration/README.md",
		"integration/p07/benchmark/main.go",
		"integration/p07/benchmark/main_unsupported.go",
		"integration/p07/native_benchmark.c",
		"integration/p07/native_driver.c",
		"integration/p07/probe/main.go",
		"integration/p07/report.schema.json",
		"integration/p07/reproduce.sh",
		"tools/p07-verify/main.go",
		"tools/p07-verify/main_test.go",
		"docs/p07/tasks.md",
		"docs/p07/provenance.md",
		"go.mod",
		"go.sum",
	)
	slices.Sort(paths)

	result := make(map[string]string, len(paths))
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			fatalf("read P07 source artifact %q: %v", path, err)
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

func isFinitePositive(value float64) bool {
	return value > 0 && !math.IsInf(value, 0) && !math.IsNaN(value)
}

func metricsEqual(actual, expected float64) bool {
	return math.Abs(actual-expected) <= 0.0001
}

func fatalf(format string, values ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", values...)
	os.Exit(1)
}
