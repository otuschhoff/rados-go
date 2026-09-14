// Command p00-verify validates the checked-in P00 evidence and optional reports.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

type evidence struct {
	SchemaVersion int    `json:"schema_version"`
	RecordedAt    string `json:"recorded_at"`
	Project       struct {
		ModulePath    string `json:"module_path"`
		PublicPackage string `json:"public_package"`
		Repository    string `json:"repository"`
	} `json:"project"`
	Ceph struct {
		Repository     string `json:"repository"`
		SourceBaseline struct {
			Tag       string            `json:"tag"`
			TagObject string            `json:"tag_object"`
			Commit    string            `json:"commit"`
			Files     map[string]string `json:"files"`
		} `json:"source_baseline"`
		QualificationRelease struct {
			Tag       string `json:"tag"`
			TagObject string `json:"tag_object"`
			Commit    string `json:"commit"`
		} `json:"qualification_release"`
	} `json:"ceph"`
	Images map[string]struct {
		Reference       string `json:"reference"`
		AMD64           string `json:"amd64"`
		ARM64           string `json:"arm64"`
		RuntimePackages string `json:"runtime_packages,omitempty"`
	} `json:"images"`
	Oracle struct {
		Compiler         string `json:"compiler"`
		CephDevel        string `json:"ceph_devel"`
		CephPPDevel      string `json:"cephpp_devel"`
		DevelRepository  string `json:"devel_repository"`
		CephDevelAMD64   string `json:"ceph_devel_amd64_sha256"`
		CephDevelARM64   string `json:"ceph_devel_arm64_sha256"`
		CephPPDevelAMD64 string `json:"cephpp_devel_amd64_sha256"`
		CephPPDevelARM64 string `json:"cephpp_devel_arm64_sha256"`
		Librados         string `json:"librados"`
		SourceSHA256     string `json:"source_sha256"`
		DockerfileSHA256 string `json:"dockerfile_sha256"`
	} `json:"oracle"`
	Go map[string]struct {
		Version    string `json:"version"`
		LinuxAMD64 string `json:"linux_amd64_sha256"`
		LinuxARM64 string `json:"linux_arm64_sha256"`
	} `json:"go"`
	Inventory struct {
		C         int    `json:"c_exported_functions"`
		CPP       int    `json:"cpp_public_operations"`
		Generator string `json:"generator"`
		Output    string `json:"output"`
	} `json:"inventory"`
}

type report struct {
	SchemaVersion int    `json:"schema_version"`
	Status        string `json:"status"`
	StartedAt     string `json:"started_at"`
	FinishedAt    string `json:"finished_at"`
	Source        struct {
		Baseline      string `json:"baseline_commit"`
		Qualification string `json:"qualification_commit"`
		Repository    string `json:"repository_commit"`
	} `json:"source"`
	Server struct {
		Image   string `json:"image"`
		Version string `json:"version"`
	} `json:"server"`
	Oracle struct {
		ImageID          string `json:"image_id"`
		SourceSHA256     string `json:"source_sha256"`
		DockerfileSHA256 string `json:"dockerfile_sha256"`
		Compiler         string `json:"compiler"`
		CephDevel        string `json:"ceph_devel"`
		CephPPDevel      string `json:"cephpp_devel"`
		DevelRepository  string `json:"devel_repository"`
		CephDevelAMD64   string `json:"ceph_devel_amd64_sha256"`
		CephDevelARM64   string `json:"ceph_devel_arm64_sha256"`
		CephPPDevelAMD64 string `json:"cephpp_devel_amd64_sha256"`
		CephPPDevelARM64 string `json:"cephpp_devel_arm64_sha256"`
		Librados         string `json:"librados"`
	} `json:"oracle"`
	Client struct {
		OS         string   `json:"os"`
		Arch       string   `json:"architecture"`
		GoVersions []string `json:"go_versions"`
	} `json:"client"`
	Cluster struct {
		FSID       string `json:"fsid"`
		Replicated string `json:"replicated_profile"`
		EC         string `json:"ec_profile"`
	} `json:"cluster"`
	Tests struct {
		NativeCRUD struct {
			Status   string `json:"status"`
			Evidence struct {
				Status         string `json:"status"`
				Pool           string `json:"pool"`
				Object         string `json:"object"`
				PayloadHex     string `json:"payload_hex"`
				WriteVersion   int64  `json:"write_version"`
				ReadVersion    int64  `json:"read_version"`
				PGHashPosition int64  `json:"pg_hash_position"`
			} `json:"evidence"`
		} `json:"native_crud"`
		ObjectMapping struct {
			Status   string         `json:"status"`
			Evidence map[string]any `json:"evidence"`
		} `json:"object_mapping"`
		Cleanup struct {
			Status   string `json:"status"`
			Evidence struct {
				ClusterStateRemoved bool `json:"cluster_state_removed"`
				LoopDevicesDetached bool `json:"loop_devices_detached"`
				OracleImageRemoved  bool `json:"oracle_image_removed"`
				RunnerStateRemoved  bool `json:"runner_state_removed"`
			} `json:"evidence"`
		} `json:"cleanup"`
	} `json:"tests"`
}

var (
	digestRE              = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	hashRE                = regexp.MustCompile(`^[0-9a-f]{64}$`)
	imageRE               = regexp.MustCompile(`^quay\.io/ceph/ceph@sha256:[0-9a-f]{64}$`)
	commitRE              = regexp.MustCompile(`^[0-9a-f]{40}$`)
	uuidRE                = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	reportProvenancePaths = []string{
		"Makefile",
		"docs/p00/evidence.json",
		"integration/manifest.schema.json",
		"integration/p00/preflight.sh",
		"integration/p00/run.sh",
		"integration/p00/oracle/Dockerfile",
		"integration/p00/oracle/main.cc",
		"tools/p00-verify/main.go",
	}
)

func main() {
	reportPath := flag.String("report", "", "optional integration report to validate")
	cephSource := flag.String("ceph-source", "", "optional pinned Ceph checkout used to verify evidence hashes")
	flag.Parse()

	manifest := loadEvidence("docs/p00/evidence.json")
	checkPins(manifest)
	checkInventory("docs/p00/api-inventory.csv", manifest.Inventory.C, manifest.Inventory.CPP)
	checkRequiredFiles()
	checkShell()
	if *cephSource != "" {
		checkSourceHashes(*cephSource, manifest)
	}
	if *reportPath != "" {
		checkReport(*reportPath, manifest)
	}
	fmt.Println("P00 verification passed")
}

func loadEvidence(path string) evidence {
	var value evidence
	decodeStrict(path, &value)
	return value
}

func checkPins(value evidence) {
	if value.SchemaVersion != 1 || value.Project.ModulePath != "github.com/otuschhoff/go-librados" || value.Project.PublicPackage != "rados" {
		fatalf("unexpected evidence schema or project identity")
	}
	if value.Ceph.Repository != "https://github.com/ceph/ceph.git" || !commitRE.MatchString(value.Ceph.SourceBaseline.TagObject) || !commitRE.MatchString(value.Ceph.QualificationRelease.TagObject) || !commitRE.MatchString(value.Ceph.SourceBaseline.Commit) || !commitRE.MatchString(value.Ceph.QualificationRelease.Commit) {
		fatalf("Ceph commits must be full lowercase SHA-1 values")
	}
	for name, image := range value.Images {
		if !imageRE.MatchString(image.Reference) || !digestRE.MatchString(image.AMD64) || !digestRE.MatchString(image.ARM64) {
			fatalf("image %s is not fully digest-pinned", name)
		}
	}
	for _, name := range []string{"minimum", "latest"} {
		toolchain, ok := value.Go[name]
		if !ok || !regexp.MustCompile(`^go1\.[0-9]+\.[0-9]+$`).MatchString(toolchain.Version) || !hashRE.MatchString(toolchain.LinuxAMD64) || !hashRE.MatchString(toolchain.LinuxARM64) {
			fatalf("Go %s toolchain pin is incomplete", name)
		}
	}
	if value.Oracle.Compiler == "" || value.Oracle.CephDevel == "" || value.Oracle.CephPPDevel == "" || value.Oracle.DevelRepository != "https://download.ceph.com/rpm-20.2.4/el9" || value.Oracle.Librados == "" || !hashRE.MatchString(value.Oracle.CephDevelAMD64) || !hashRE.MatchString(value.Oracle.CephDevelARM64) || !hashRE.MatchString(value.Oracle.CephPPDevelAMD64) || !hashRE.MatchString(value.Oracle.CephPPDevelARM64) || !hashRE.MatchString(value.Oracle.SourceSHA256) || !hashRE.MatchString(value.Oracle.DockerfileSHA256) {
		fatalf("oracle toolchain or source pins are incomplete")
	}
	checkHash("integration/p00/oracle/main.cc", value.Oracle.SourceSHA256)
	checkHash("integration/p00/oracle/Dockerfile", value.Oracle.DockerfileSHA256)
	checkHash("integration/p00/oracle/centos-stream.repo", "83240f736d211998dd5bbb89773696b9e11463d24dac6eeb8eb93125d4540d3b")
}

func checkInventory(path string, wantC, wantCPP int) {
	file, err := os.Open(path)
	must(err)
	defer file.Close()
	reader := csv.NewReader(file)
	header, err := reader.Read()
	must(err)
	wantHeader := []string{"language", "owner", "source_symbol", "source_signature", "go_equivalent", "disposition", "phase", "prerequisites", "semantic_difference", "conformance_test"}
	if strings.Join(header, "\x00") != strings.Join(wantHeader, "\x00") {
		fatalf("unexpected inventory header")
	}
	counts := map[string]int{}
	seen := map[string]bool{}
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		must(err)
		if len(row) != len(wantHeader) {
			fatalf("inventory row has %d columns", len(row))
		}
		for index, value := range row {
			if strings.TrimSpace(value) == "" {
				fatalf("inventory %s has empty %s", row[2], wantHeader[index])
			}
			if strings.Contains(value, "TBD") || strings.Contains(value, "review-required") || strings.Contains(value, "unclassified") {
				fatalf("inventory %s retains placeholder %q", row[2], value)
			}
		}
		key := row[0] + "\x00" + row[1] + "\x00" + row[2] + "\x00" + row[3]
		if seen[key] {
			fatalf("duplicate inventory declaration %s", row[2])
		}
		seen[key] = true
		counts[row[0]]++
	}
	if counts["C"] != wantC || counts["C++"] != wantCPP {
		fatalf("inventory count mismatch: C=%d/%d C++=%d/%d", counts["C"], wantC, counts["C++"], wantCPP)
	}
}

func checkRequiredFiles() {
	required := []string{
		"docs/p00/compatibility.md", "docs/p00/protocol-sources.md", "docs/p00/licensing.md",
		"docs/p00/threat-model.md", "docs/p00/tasks.md", "docs/p00/task-handoff.md", "integration/manifest.schema.json",
		"testdata/manifest.schema.json", "testdata/README.md", "integration/p00/oracle/Dockerfile", "integration/p00/oracle/centos-stream.repo", "integration/p00/oracle/main.cc",
	}
	for _, path := range required {
		info, err := os.Stat(path)
		if err != nil || info.Size() == 0 {
			fatalf("required P00 artifact missing or empty: %s", path)
		}
	}
	var schema any
	decodeStrict("integration/manifest.schema.json", &schema)
	decodeStrict("testdata/manifest.schema.json", &schema)
}

func checkShell() {
	for _, path := range []string{"integration/p00/preflight.sh", "integration/p00/run.sh"} {
		command := exec.Command("sh", "-n", path)
		if output, err := command.CombinedOutput(); err != nil {
			fatalf("%s shell syntax: %v: %s", path, err, output)
		}
	}
	runner := string(readFile("integration/p00/run.sh"))
	for _, marker := range []string{"P00_DISPOSABLE_CLUSTER", "--require-linux-host", "git status --porcelain --untracked-files=all", ".images.qualification.reference", "rm-cluster --force --zap-osds"} {
		if !strings.Contains(runner, marker) {
			fatalf("runner lacks safety marker %q", marker)
		}
	}
}

func checkSourceHashes(root string, value evidence) {
	for path, expected := range value.Ceph.SourceBaseline.Files {
		data := readFile(filepath.Join(root, filepath.FromSlash(path)))
		digest := sha256.Sum256(data)
		actual := "sha256:" + hex.EncodeToString(digest[:])
		if actual != expected {
			fatalf("source hash mismatch for %s: %s", path, actual)
		}
	}
}

func checkReport(path string, manifest evidence) {
	var value report
	decodeStrict(path, &value)
	if value.SchemaVersion != 1 || value.Status != "passed" {
		fatalf("report must be schema 1 with passed status")
	}
	if value.Source.Baseline != manifest.Ceph.SourceBaseline.Commit || value.Source.Qualification != manifest.Ceph.QualificationRelease.Commit || !commitRE.MatchString(value.Source.Repository) {
		fatalf("report source pins do not match evidence manifest")
	}
	if err := verifyReportCommit(value.Source.Repository, reportProvenancePaths); err != nil {
		fatalf("report repository provenance: %v", err)
	}
	if value.Server.Image != manifest.Images["qualification"].Reference || value.Server.Version == "" {
		fatalf("report server identity does not match qualification image")
	}
	if !digestRE.MatchString(value.Oracle.ImageID) || value.Oracle.SourceSHA256 != manifest.Oracle.SourceSHA256 || value.Oracle.DockerfileSHA256 != manifest.Oracle.DockerfileSHA256 || value.Oracle.Compiler != manifest.Oracle.Compiler || value.Oracle.CephDevel != manifest.Oracle.CephDevel || value.Oracle.CephPPDevel != manifest.Oracle.CephPPDevel || value.Oracle.DevelRepository != manifest.Oracle.DevelRepository || value.Oracle.CephDevelAMD64 != manifest.Oracle.CephDevelAMD64 || value.Oracle.CephDevelARM64 != manifest.Oracle.CephDevelARM64 || value.Oracle.CephPPDevelAMD64 != manifest.Oracle.CephPPDevelAMD64 || value.Oracle.CephPPDevelARM64 != manifest.Oracle.CephPPDevelARM64 || value.Oracle.Librados != manifest.Oracle.Librados {
		fatalf("report oracle build provenance is incomplete")
	}
	if !uuidRE.MatchString(value.Cluster.FSID) || value.Cluster.Replicated == "" || value.Cluster.EC == "" {
		fatalf("report cluster identity/profiles are incomplete")
	}
	if value.Client.OS == "" || value.Client.Arch == "" || len(value.Client.GoVersions) != 2 || value.Client.GoVersions[0] != manifest.Go["minimum"].Version || value.Client.GoVersions[1] != manifest.Go["latest"].Version {
		fatalf("report client matrix is incomplete")
	}
	started, err := time.Parse(time.RFC3339, value.StartedAt)
	must(err)
	finished, err := time.Parse(time.RFC3339, value.FinishedAt)
	must(err)
	if finished.Before(started) {
		fatalf("report finished before it started")
	}
	if value.Tests.NativeCRUD.Status != "passed" || value.Tests.ObjectMapping.Status != "passed" || value.Tests.Cleanup.Status != "passed" {
		fatalf("report tests are absent or not passed")
	}
	crud := value.Tests.NativeCRUD.Evidence
	if crud.Status != "passed" || crud.Pool == "" || crud.Object == "" || !regexp.MustCompile(`^[0-9a-f]+$`).MatchString(crud.PayloadHex) || crud.WriteVersion < 1 || crud.ReadVersion < 1 || crud.PGHashPosition < 0 {
		fatalf("native CRUD evidence is incomplete")
	}
	mapping := value.Tests.ObjectMapping.Evidence
	if stringValue(mapping["pool"]) != crud.Pool || stringValue(mapping["objname"]) != crud.Object || stringValue(mapping["pgid"]) == "" || len(array(mapping["up"])) == 0 || len(array(mapping["acting"])) == 0 || number(mapping["acting_primary"]) < 0 {
		fatalf("object mapping evidence is incomplete or targets a different object")
	}
	cleanup := value.Tests.Cleanup.Evidence
	if !cleanup.ClusterStateRemoved || !cleanup.LoopDevicesDetached || !cleanup.OracleImageRemoved || !cleanup.RunnerStateRemoved {
		fatalf("cleanup evidence is incomplete")
	}
}

func verifyReportCommit(commit string, paths []string) error {
	if output, err := exec.Command("git", "cat-file", "-e", commit+"^{commit}").CombinedOutput(); err != nil {
		return fmt.Errorf("commit %s does not exist: %s", commit, strings.TrimSpace(string(output)))
	}
	if output, err := exec.Command("git", "merge-base", "--is-ancestor", commit, "HEAD").CombinedOutput(); err != nil {
		return fmt.Errorf("commit %s is not an ancestor of HEAD: %s", commit, strings.TrimSpace(string(output)))
	}
	rootOutput, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		return fmt.Errorf("locate repository root: %w", err)
	}
	root := strings.TrimSpace(string(rootOutput))
	for _, path := range paths {
		committed, err := exec.Command("git", "show", commit+":"+path).Output()
		if err != nil {
			return fmt.Errorf("read %s at commit %s: %w", path, commit, err)
		}
		current, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(path)))
		if err != nil {
			return fmt.Errorf("read current %s: %w", path, err)
		}
		if !bytes.Equal(committed, current) {
			return fmt.Errorf("%s differs from commit %s; rerun the live smoke", path, commit)
		}
	}
	return nil
}

func checkHash(path, expected string) {
	digest := sha256.Sum256(readFile(path))
	actual := hex.EncodeToString(digest[:])
	if actual != expected {
		fatalf("hash mismatch for %s: %s", path, actual)
	}
}

func number(value any) float64 {
	number, ok := value.(float64)
	if !ok {
		return -1
	}
	return number
}

func array(value any) []any {
	array, _ := value.([]any)
	return array
}

func stringValue(value any) string {
	text, _ := value.(string)
	return text
}

func decodeStrict(path string, destination any) {
	file, err := os.Open(path)
	must(err)
	defer file.Close()
	must(decodeStrictJSON(file, destination))
}

func decodeStrictJSON(reader io.Reader, destination any) error {
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return fmt.Errorf("contains trailing JSON values")
	}
	return nil
}

func readFile(path string) []byte {
	data, err := os.ReadFile(path)
	must(err)
	return data
}

func must(err error) {
	if err != nil {
		fatalf("%v", err)
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "p00-verify: "+format+"\n", args...)
	os.Exit(1)
}
