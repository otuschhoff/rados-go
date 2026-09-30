// Command p13-verify validates checked-in P13 lifecycle evidence.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

const (
	cephVersion = "ceph version 20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964) tentacle (stable)"
	imageAMD64  = "quay.io/ceph/ceph@sha256:09ee90f6f3e0c7b9954f71d214ee05e9bbaaaea3716b1dd619603283b829f8b8"
	imageARM64  = "quay.io/ceph/ceph@sha256:6e6bc7b28fa1b334108a3646af5533dfb50db508efdf5b358eb7dd0dd37a48aa"
)

var scenarioContracts = []struct {
	id, pool, operation, fault, finalData, transport string
	errno                                            int
}{
	{"p13-baseline-size3", "p13-size3", "read", "none", "p13-size3-baseline", "secure", 0},
	{"p13-baseline-size1", "p13-size1", "read", "none", "p13-size1-baseline", "secure", 0},
	{"p13-size3-one-down", "p13-size3", "read", "osd-down", "stable", "secure", 0},
	{"p13-size3-below-min-recovery", "p13-size3", "read", "two-osds-down", "parked", "secure", 0},
	{"p13-crc-below-min-recovery", "p13-size3", "read", "two-osds-down", "parked", "crc", 0},
	{"p13-size1-loss-recovery", "p13-size1", "read", "sole-osd-down", "sole", "secure", 0},
	{"p13-silent-primary-map-wakeup", "p13-size3", "read", "paused-primary-then-down-out", "silent", "secure", 0},
	{"p13-flaky-stop-start", "p13-size3", "append", "bounded-stop-start", "", "secure", 0},
	{"p13-noout-read", "p13-size3", "read", "global-noout-and-primary-restart", "watch", "secure", 0},
	{"p13-noout-write", "p13-size3", "write", "global-noout-and-primary-restart", "maintenance", "secure", 0},
	{"p13-noout-pg-command", "cluster", "pg-command", "global-noout-and-primary-restart", "", "secure", 0},
	{"p13-noout-long-watch", "p13-size3", "watch", "global-noout-and-primary-restart", "watch", "secure", 0},
	{"p13-add-osd-remap", "p13-size3", "read", "add-osd-3", "remap", "secure", 0},
	{"p13-explicit-command-down", "cluster", "osd-command", "osd-3-down", "", "secure", 6},
	{"p13-out-down-remove", "p13-size3", "read", "osd-3-out-down-purge", "remap", "secure", 0},
	{"p13-explicit-command-absent", "cluster", "osd-command", "osd-3-absent", "", "secure", 2},
	{"p13-destroy-recreate", "p13-size3", "read", "destroy-recreate-osd-3-new-uuid-address", "remap", "secure", 0},
	{"p13-mon-offline-failover", "cluster", "monitor-command", "mon-a-offline", "", "secure", 0},
	{"p13-mon-unreliable", "cluster", "monitor-command", "bounded-mon-stop-start", "", "secure", 0},
	{"p13-mon-changed", "cluster", "monitor-command", "mon-c-location-change", "", "secure", 0},
	{"p13-mon-removed", "cluster", "monitor-command", "mon-c-removed", "", "secure", 0},
	{"p13-mon-added", "cluster", "monitor-command", "mon-c-added", "", "secure", 0},
}

var sha256Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

type mapping struct {
	Epoch      uint64  `json:"epoch"`
	PGID       string  `json:"pgid"`
	Acting     []int32 `json:"acting"`
	Primary    int32   `json:"primary"`
	PGState    string  `json:"pg_state"`
	ObservedAt string  `json:"observed_at"`
}

type operationResult struct {
	Implementation     string              `json:"implementation"`
	Transport          string              `json:"transport"`
	Operation          string              `json:"operation"`
	Object             string              `json:"object"`
	StartedAt          string              `json:"started_at"`
	FinishedAt         string              `json:"finished_at"`
	ElapsedNS          int64               `json:"elapsed_ns"`
	Completed          bool                `json:"completed"`
	Errno              int                 `json:"errno"`
	Outcome            string              `json:"outcome"`
	Version            uint64              `json:"version"`
	Data               string              `json:"data"`
	MutationMarker     string              `json:"mutation_marker,omitempty"`
	WatchCookie        uint64              `json:"watch_cookie,omitempty"`
	WatchEvents        int                 `json:"watch_events,omitempty"`
	WatchInterruptions int                 `json:"watch_interruptions,omitempty"`
	Changes            []changeObservation `json:"changes,omitempty"`
}

type publicOSDState struct {
	ID        int32    `json:"id"`
	Exists    bool     `json:"exists"`
	Up        bool     `json:"up"`
	In        bool     `json:"in"`
	Destroyed bool     `json:"destroyed"`
	Addresses []string `json:"addresses"`
}

type publicMONState struct {
	Name      string            `json:"name"`
	Rank      int               `json:"rank"`
	Addresses []string          `json:"addresses"`
	Priority  uint16            `json:"priority"`
	Weight    uint16            `json:"weight"`
	Location  map[string]string `json:"location"`
}

type changeObservation struct {
	Sequence    uint64          `json:"sequence"`
	ObservedAt  string          `json:"observed_at"`
	Component   string          `json:"component"`
	Source      string          `json:"source"`
	Kind        string          `json:"kind"`
	Epoch       uint32          `json:"epoch"`
	OSD         *publicOSDState `json:"osd,omitempty"`
	PreviousOSD *publicOSDState `json:"previous_osd,omitempty"`
	MON         *publicMONState `json:"mon,omitempty"`
	PreviousMON *publicMONState `json:"previous_mon,omitempty"`
	Error       string          `json:"error,omitempty"`
}

type osdState struct {
	ID        int32  `json:"id"`
	Exists    bool   `json:"exists"`
	Up        bool   `json:"up"`
	In        bool   `json:"in"`
	Destroyed bool   `json:"destroyed"`
	UUID      string `json:"uuid"`
	Address   string `json:"address"`
}

type lifecycle struct {
	Before    osdState  `json:"before"`
	Destroyed *osdState `json:"destroyed,omitempty"`
	After     osdState  `json:"after"`
}

type monitorState struct {
	Name     string            `json:"name"`
	Rank     int               `json:"rank"`
	Address  string            `json:"address"`
	Priority uint16            `json:"priority"`
	Weight   uint16            `json:"weight"`
	Location map[string]string `json:"location"`
}

type monitorMap struct {
	Epoch    uint32         `json:"epoch"`
	Monitors []monitorState `json:"monitors"`
}

type monitorLifecycle struct {
	Before monitorMap `json:"before"`
	After  monitorMap `json:"after"`
}

type faultInterval struct {
	InjectedAt string `json:"injected_at"`
	ReleasedAt string `json:"released_at"`
}

type scenario struct {
	ID               string            `json:"id"`
	Status           string            `json:"status"`
	Pool             string            `json:"pool"`
	Operation        string            `json:"operation"`
	Fault            string            `json:"fault"`
	InjectedAt       string            `json:"injected_at"`
	ReleasedAt       string            `json:"released_at"`
	Before           mapping           `json:"before"`
	After            mapping           `json:"after"`
	Go               operationResult   `json:"go"`
	Native           operationResult   `json:"native"`
	FinalData        string            `json:"final_data"`
	MutationMarkers  []string          `json:"mutation_markers"`
	Notes            string            `json:"notes"`
	Lifecycle        *lifecycle        `json:"lifecycle"`
	MonitorLifecycle *monitorLifecycle `json:"monitor_lifecycle"`
	FaultIntervals   []faultInterval   `json:"fault_intervals"`
}

type report struct {
	SchemaVersion int     `json:"schema_version"`
	Status        string  `json:"status"`
	Command       string  `json:"command"`
	Mode          string  `json:"mode"`
	StartedAt     string  `json:"started_at"`
	FinishedAt    string  `json:"finished_at"`
	Failure       *string `json:"failure"`
	Source        struct {
		Repository string            `json:"repository"`
		Artifacts  map[string]string `json:"artifacts"`
	} `json:"source"`
	Server *struct {
		Image    string `json:"image"`
		Platform string `json:"platform"`
		Version  string `json:"version"`
	} `json:"server"`
	NativeRuntime *struct {
		Soname string `json:"soname"`
		Path   string `json:"path"`
		SHA256 string `json:"sha256"`
	} `json:"native_runtime"`
	Cluster *struct {
		FSID        string   `json:"fsid"`
		Subnet      string   `json:"subnet"`
		Monitors    []string `json:"monitors"`
		Manager     string   `json:"manager"`
		InitialOSDs int      `json:"initial_osds"`
		DeviceBytes uint64   `json:"device_bytes"`
		Pools       []struct {
			Name    string `json:"name"`
			Size    int    `json:"size"`
			MinSize int    `json:"min_size"`
		} `json:"pools"`
		User string `json:"user"`
	} `json:"cluster"`
	Scenarios          []scenario       `json:"scenarios"`
	ChangeObservations *operationResult `json:"change_observations"`
	Unqualified        struct {
		MultiHostMaintenance    string `json:"multi_host_maintenance"`
		ProbabilisticPacketLoss string `json:"probabilistic_packet_loss"`
	} `json:"unqualified"`
}

func main() {
	path := flag.String("report", "docs/p13/integration-report.json", "P13 report to validate")
	allowFailed := flag.Bool("allow-failed", false, "accept a structurally valid failed report")
	allowQuick := flag.Bool("allow-quick", false, "accept a passed quick report as non-certifying evidence")
	flag.Parse()
	if flag.NArg() != 0 {
		fatalf("unexpected positional arguments")
	}
	value, err := readReport(*path)
	if err != nil {
		fatalf("P13 verification failed: %v", err)
	}
	if err := validate(value, *allowFailed, *allowQuick); err != nil {
		fatalf("P13 verification failed: %v", err)
	}
	fmt.Printf("P13 verification passed: status=%s mode=%s scenarios=%d\n", value.Status, value.Mode, len(value.Scenarios))
}

func readReport(path string) (report, error) {
	file, err := os.Open(path)
	if err != nil {
		return report{}, err
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	var value report
	if err := decoder.Decode(&value); err != nil {
		return report{}, fmt.Errorf("decode report: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return report{}, fmt.Errorf("trailing JSON content")
	}
	return value, nil
}

func validate(value report, allowFailed, allowQuick bool) error {
	started, err := time.Parse(time.RFC3339Nano, value.StartedAt)
	if err != nil {
		return fmt.Errorf("invalid started_at")
	}
	finished, err := time.Parse(time.RFC3339Nano, value.FinishedAt)
	if err != nil || finished.Before(started) {
		return fmt.Errorf("invalid report chronology")
	}
	if value.SchemaVersion != 1 || value.Source.Repository != "https://github.com/otuschhoff/rados-go.git" {
		return fmt.Errorf("invalid report envelope")
	}
	if value.Unqualified.MultiHostMaintenance != "unqualified: disposable harness uses one Docker host" || value.Unqualified.ProbabilisticPacketLoss != "unqualified: no host-global or probabilistic network fault injection is used" {
		return fmt.Errorf("invalid qualification boundaries")
	}
	if value.Status == "failed" {
		if !allowFailed || value.Failure == nil || *value.Failure == "" || value.Server != nil || value.NativeRuntime != nil || value.Cluster != nil || len(value.Scenarios) != 0 || value.ChangeObservations != nil {
			return fmt.Errorf("failed report is not accepted")
		}
		return nil
	}
	if value.Status != "passed" || value.Failure != nil || value.Server == nil || value.NativeRuntime == nil || value.Cluster == nil || value.ChangeObservations == nil {
		return fmt.Errorf("report is not certifying")
	}
	validFull := value.Mode == "full" && value.Command == "./integration/p13/reproduce.sh"
	validQuick := allowQuick && value.Mode == "quick" && value.Command == "./integration/p13/reproduce.sh --quick"
	if !validFull && !validQuick {
		return fmt.Errorf("quick evidence is not certifying")
	}
	expectedImage := map[string]string{"linux/amd64": imageAMD64, "linux/arm64": imageARM64}[value.Server.Platform]
	if expectedImage == "" || value.Server.Image != expectedImage || value.Server.Version != cephVersion {
		return fmt.Errorf("invalid server identity")
	}
	if value.NativeRuntime.Soname != "librados.so.2" || !filepath.IsAbs(value.NativeRuntime.Path) || !sha256Pattern.MatchString(value.NativeRuntime.SHA256) {
		return fmt.Errorf("invalid native runtime identity")
	}
	cluster := value.Cluster
	expectedDeviceBytes := uint64(8589934592)
	if value.Mode == "quick" {
		expectedDeviceBytes = 2147483648
	}
	if cluster.FSID != "13131313-2222-4333-8444-131313131313" || cluster.Subnet != "172.30.113.0/24" || !stringsEqual(cluster.Monitors, []string{"p13-mon-a", "p13-mon-b", "p13-mon-c"}) || cluster.Manager != "p13-mgr-x" || cluster.InitialOSDs != 3 || cluster.DeviceBytes != expectedDeviceBytes || cluster.User != "client.p13" || len(cluster.Pools) != 2 || cluster.Pools[0].Name != "p13-size3" || cluster.Pools[0].Size != 3 || cluster.Pools[0].MinSize != 2 || cluster.Pools[1].Name != "p13-size1" || cluster.Pools[1].Size != 1 || cluster.Pools[1].MinSize != 1 {
		return fmt.Errorf("invalid cluster topology")
	}
	if !mapsEqual(value.Source.Artifacts, implementationHashes()) {
		return fmt.Errorf("source artifacts do not match current tree")
	}
	if len(value.Scenarios) != len(scenarioContracts) {
		return fmt.Errorf("scenario count=%d want=%d", len(value.Scenarios), len(scenarioContracts))
	}
	markers := make(map[string]bool)
	for index, item := range value.Scenarios {
		contract := scenarioContracts[index]
		if item.ID != contract.id || item.Pool != contract.pool || item.Operation != contract.operation || item.Fault != contract.fault || contract.finalData != "" && item.FinalData != contract.finalData || item.Go.Transport != contract.transport || item.Native.Transport != contract.transport {
			return fmt.Errorf("scenario %d does not match its contract", index)
		}
		if err := validateScenario(item, contract.errno, value.Mode, started, finished, markers); err != nil {
			return fmt.Errorf("scenario %s: %w", item.ID, err)
		}
	}
	if err := validateChangeObservations(*value.ChangeObservations, started, finished); err != nil {
		return fmt.Errorf("change observations: %w", err)
	}
	return nil
}

func validateScenario(value scenario, expectedErrno int, mode string, reportStart, reportFinish time.Time, markers map[string]bool) error {
	injected, err := time.Parse(time.RFC3339Nano, value.InjectedAt)
	if err != nil {
		return fmt.Errorf("invalid injection timestamp")
	}
	released, err := time.Parse(time.RFC3339Nano, value.ReleasedAt)
	if err != nil || released.Before(injected) || injected.Before(reportStart) || released.After(reportFinish) {
		return fmt.Errorf("invalid fault chronology")
	}
	minimumPending, minimumWatchOutage := 5*time.Second, 12*time.Second
	flakyCycles := 4
	if mode == "quick" {
		minimumPending, minimumWatchOutage = 2*time.Second, 10*time.Second
		flakyCycles = 2
	}
	before, beforeErr := time.Parse(time.RFC3339Nano, value.Before.ObservedAt)
	after, afterErr := time.Parse(time.RFC3339Nano, value.After.ObservedAt)
	if beforeErr != nil || afterErr != nil || after.Before(before) || value.Before.Epoch == 0 || value.After.Epoch < value.Before.Epoch || value.Before.PGID == "" || value.After.PGID != value.Before.PGID || !validMapping(value.Before, value.Pool) || !validMapping(value.After, value.Pool) || !strings.Contains(value.Before.PGState, "active") || !strings.Contains(value.After.PGState, "active") {
		return fmt.Errorf("invalid map observations")
	}
	if value.Status != "passed" || value.Operation == "" || value.Fault == "" || value.Notes == "" {
		return fmt.Errorf("invalid scenario envelope")
	}
	for implementation, result := range map[string]operationResult{"go": value.Go, "native": value.Native} {
		expectedCompleted, expectedOutcome := expectedErrno == 0, "success"
		if expectedErrno != 0 {
			expectedOutcome = "error"
		}
		if result.Implementation != implementation || result.Completed != expectedCompleted || result.Errno != expectedErrno || result.Outcome != expectedOutcome || result.Operation != value.Operation || result.Object == "" || result.Object != value.Go.Object || result.Object != value.Native.Object {
			return fmt.Errorf("invalid %s result", implementation)
		}
		operationStart, startErr := time.Parse(time.RFC3339Nano, result.StartedAt)
		operationFinish, finishErr := time.Parse(time.RFC3339Nano, result.FinishedAt)
		wallElapsed := operationFinish.Sub(operationStart)
		if startErr != nil || finishErr != nil || operationFinish.Before(operationStart) || operationStart.Before(reportStart.Add(-time.Second)) || operationFinish.After(reportFinish.Add(time.Second)) || result.ElapsedNS < 0 || absDuration(wallElapsed-time.Duration(result.ElapsedNS)) > 2*time.Second {
			return fmt.Errorf("invalid %s chronology", implementation)
		}
		if value.Operation == "watch" {
			if operationStart.After(injected.Add(time.Second)) || operationFinish.Before(released.Add(-time.Second)) {
				return fmt.Errorf("%s watch does not span the fault", implementation)
			}
		} else if operationStart.Before(injected.Add(-time.Second)) || operationFinish.After(released.Add(time.Second)) {
			return fmt.Errorf("%s operation falls outside the scenario", implementation)
		}
	}
	if value.Operation == "read" && (value.Go.Operation != "read" || value.Native.Operation != "read" || value.Go.Data != value.Native.Data || value.Go.Version != value.Native.Version || value.Go.Data != value.FinalData) {
		return fmt.Errorf("go/native result mismatch")
	}
	if value.Operation == "append" {
		if value.Go.Operation != "append" || value.Native.Operation != "append" || len(value.MutationMarkers) != 2*flakyCycles || len(value.FaultIntervals) != flakyCycles || markerBytes(value.MutationMarkers) != len(value.FinalData) {
			return fmt.Errorf("invalid mutation pairing")
		}
		var previousCycleReleased time.Time
		if value.FinalData != strings.Join(value.MutationMarkers, "") {
			return fmt.Errorf("mutation markers do not match final object order")
		}
		for cycle := 1; cycle <= flakyCycles; cycle++ {
			goMarker, nativeMarker := fmt.Sprintf("go-marker-%d;", cycle), fmt.Sprintf("native-marker-%d;", cycle)
			first, second := value.MutationMarkers[2*(cycle-1)], value.MutationMarkers[2*(cycle-1)+1]
			if first != goMarker || second != nativeMarker {
				if first != nativeMarker || second != goMarker {
					return fmt.Errorf("invalid mutation cycle %d", cycle)
				}
			}
			interval := value.FaultIntervals[cycle-1]
			cycleInjected, injectErr := time.Parse(time.RFC3339Nano, interval.InjectedAt)
			cycleReleased, releaseErr := time.Parse(time.RFC3339Nano, interval.ReleasedAt)
			if injectErr != nil || releaseErr != nil || cycleInjected.Before(injected) || cycleReleased.After(released) || cycleReleased.Sub(cycleInjected) < minimumPending || !previousCycleReleased.IsZero() && cycleInjected.Before(previousCycleReleased) {
				return fmt.Errorf("invalid mutation cycle %d chronology", cycle)
			}
			previousCycleReleased = cycleReleased
		}
		if value.Go.MutationMarker != fmt.Sprintf("go-marker-%d;", flakyCycles) || value.Native.MutationMarker != fmt.Sprintf("native-marker-%d;", flakyCycles) {
			return fmt.Errorf("final mutation result does not match the last cycle")
		}
	} else if len(value.FaultIntervals) != 0 {
		return fmt.Errorf("unexpected fault intervals")
	}
	if value.Operation == "write" && value.FinalData == "" {
		return fmt.Errorf("write final data is empty")
	}
	if value.Operation == "pg-command" && (value.Go.Data == "" || value.Native.Data == "" || value.Go.Data != value.Native.Data) {
		return fmt.Errorf("PG command output is empty")
	}
	if value.Operation == "monitor-command" && (value.Go.Data == "" || value.Native.Data == "") {
		return fmt.Errorf("monitor command output is empty")
	}
	if value.Operation == "osd-command" && (value.FinalData != "" || len(value.MutationMarkers) != 0) {
		return fmt.Errorf("invalid command result payload")
	}
	if value.ID == "p13-size3-below-min-recovery" || value.ID == "p13-crc-below-min-recovery" || value.ID == "p13-size1-loss-recovery" || value.ID == "p13-silent-primary-map-wakeup" {
		if released.Sub(injected) < minimumPending || value.Go.ElapsedNS < int64(minimumPending) || value.Native.ElapsedNS < int64(minimumPending) {
			return fmt.Errorf("outage did not keep both clients pending")
		}
	}
	if value.ID == "p13-noout-long-watch" && released.Sub(injected) < minimumWatchOutage {
		return fmt.Errorf("watch outage is shorter than the required duration")
	}
	if value.ID == "p13-add-osd-remap" && (!contains(value.After.Acting, 3) || value.Before.PGID != value.After.PGID || slicesEqual(value.Before.Acting, value.After.Acting)) {
		return fmt.Errorf("OSD add did not remap the observed PG onto osd.3")
	}
	if value.ID == "p13-out-down-remove" && contains(value.After.Acting, 3) {
		return fmt.Errorf("retired OSD remains in acting set")
	}
	if value.ID == "p13-noout-long-watch" && (value.Go.WatchCookie == 0 || value.Go.WatchEvents < 2 || value.Go.WatchInterruptions < 1) {
		return fmt.Errorf("watch recovery was not observed")
	}
	if value.ID == "p13-noout-long-watch" && (value.Native.Operation != "watch" || value.Native.WatchCookie == 0 || value.Native.WatchEvents < 2) {
		return fmt.Errorf("native watch recovery was not observed")
	}
	if err := validateLifecycle(value); err != nil {
		return err
	}
	for _, marker := range value.MutationMarkers {
		if marker == "" || markers[marker] {
			return fmt.Errorf("duplicate or empty mutation marker %q", marker)
		}
		markers[marker] = true
		if count(value.FinalData, marker) != 1 {
			return fmt.Errorf("mutation marker %q count is not one", marker)
		}
	}
	return nil
}

func validateLifecycle(value scenario) error {
	monitorScenario := strings.HasPrefix(value.ID, "p13-mon-")
	if monitorScenario {
		if value.Lifecycle != nil || value.MonitorLifecycle == nil {
			return fmt.Errorf("missing monitor lifecycle observation")
		}
		return validateMonitorLifecycle(value.ID, *value.MonitorLifecycle)
	}
	if value.MonitorLifecycle != nil {
		return fmt.Errorf("unexpected monitor lifecycle observation")
	}
	if value.ID != "p13-explicit-command-down" && value.ID != "p13-out-down-remove" && value.ID != "p13-explicit-command-absent" && value.ID != "p13-destroy-recreate" {
		if value.Lifecycle != nil {
			return fmt.Errorf("unexpected lifecycle observation")
		}
		return nil
	}
	if value.Lifecycle == nil || value.Lifecycle.Before.ID != 3 || value.Lifecycle.After.ID != 3 {
		return fmt.Errorf("missing osd.3 lifecycle observation")
	}
	before, after := value.Lifecycle.Before, value.Lifecycle.After
	switch value.ID {
	case "p13-explicit-command-down":
		if !before.Exists || !before.Up || !before.In || !after.Exists || after.Up {
			return fmt.Errorf("invalid down lifecycle transition")
		}
	case "p13-out-down-remove", "p13-explicit-command-absent":
		if !before.Exists || after.Exists || after.Up || after.In || after.UUID != "" || after.Address != "" {
			return fmt.Errorf("invalid removal lifecycle transition")
		}
	case "p13-destroy-recreate":
		destroyed := value.Lifecycle.Destroyed
		if !before.Exists || !before.Up || before.Destroyed || destroyed == nil || destroyed.ID != 3 || !destroyed.Exists || destroyed.Up || destroyed.In || !destroyed.Destroyed || !after.Exists || !after.Up || after.Destroyed || before.UUID == "" || after.UUID == "" || before.UUID == after.UUID || before.Address == "" || after.Address == "" || before.Address == after.Address {
			return fmt.Errorf("invalid recreation lifecycle transition")
		}
	}
	return nil
}

func validateMonitorLifecycle(id string, value monitorLifecycle) error {
	if value.Before.Epoch == 0 || value.After.Epoch < value.Before.Epoch || !validMonitors(value.Before.Monitors) || !validMonitors(value.After.Monitors) {
		return fmt.Errorf("invalid monitor map observation")
	}
	beforeC, hadBefore := findMonitor(value.Before.Monitors, "c")
	afterC, hasAfter := findMonitor(value.After.Monitors, "c")
	switch id {
	case "p13-mon-offline-failover", "p13-mon-unreliable":
		if len(value.Before.Monitors) != 3 || len(value.After.Monitors) != 3 || !hadBefore || !hasAfter {
			return fmt.Errorf("monitor outage changed authoritative membership")
		}
	case "p13-mon-changed":
		if !hadBefore || !hasAfter || beforeC.Location["rack"] == afterC.Location["rack"] || afterC.Location["rack"] != "r1" || value.After.Epoch <= value.Before.Epoch {
			return fmt.Errorf("monitor location change was not observed")
		}
	case "p13-mon-removed":
		if !hadBefore || hasAfter || len(value.After.Monitors) != 2 || value.After.Epoch <= value.Before.Epoch {
			return fmt.Errorf("monitor removal was not observed")
		}
	case "p13-mon-added":
		if hadBefore || !hasAfter || len(value.Before.Monitors) != 2 || len(value.After.Monitors) != 3 || value.After.Epoch <= value.Before.Epoch {
			return fmt.Errorf("monitor addition was not observed")
		}
	default:
		return fmt.Errorf("unknown monitor lifecycle scenario")
	}
	return nil
}

func validateChangeObservations(value operationResult, reportStart, reportFinish time.Time) error {
	if value.Implementation != "go" || value.Transport != "secure" || value.Operation != "cluster-changes" || !value.Completed || value.Errno != 0 || value.Outcome != "success" || len(value.Changes) == 0 {
		return fmt.Errorf("invalid subscription result")
	}
	initialMONs := make(map[string]bool)
	available, unavailable, osdAuthoritative, addedC := 0, 0, 0, 0
	changedC, removedC := false, false
	var previousSequence uint64
	var previousTime time.Time
	for _, change := range value.Changes {
		observedAt, err := time.Parse(time.RFC3339Nano, change.ObservedAt)
		if err != nil || observedAt.Before(reportStart) || observedAt.After(reportFinish) || change.Sequence != previousSequence+1 || !previousTime.IsZero() && observedAt.Before(previousTime) {
			return fmt.Errorf("invalid event order or chronology")
		}
		previousSequence, previousTime = change.Sequence, observedAt
		if change.Source == "observed" {
			if change.Epoch != 0 || change.Kind != "available" && change.Kind != "unavailable" {
				return fmt.Errorf("invalid observed event")
			}
			if change.Kind == "available" {
				available++
			} else {
				unavailable++
			}
		} else if change.Source != "authoritative" || change.Epoch == 0 || change.Kind == "available" || change.Kind == "unavailable" {
			return fmt.Errorf("invalid authoritative event")
		}
		switch change.Component {
		case "mon":
			if change.MON == nil && change.PreviousMON == nil {
				return fmt.Errorf("monitor event has no state")
			}
			name := ""
			if change.MON != nil {
				name = change.MON.Name
			}
			if name == "" && change.PreviousMON != nil {
				name = change.PreviousMON.Name
			}
			if change.Source == "authoritative" && change.Kind == "added" {
				initialMONs[name] = true
				if name == "c" {
					addedC++
				}
			}
			if name == "c" && change.Source == "authoritative" && change.Kind == "changed" {
				changedC = true
			}
			if name == "c" && change.Source == "authoritative" && change.Kind == "removed" {
				removedC = true
			}
		case "osd":
			if change.OSD == nil && change.PreviousOSD == nil {
				return fmt.Errorf("OSD event has no state")
			}
			if change.Source == "authoritative" {
				osdAuthoritative++
			}
		default:
			return fmt.Errorf("invalid event component %q", change.Component)
		}
	}
	if !initialMONs["a"] || !initialMONs["b"] || !initialMONs["c"] || addedC < 2 || !changedC || !removedC || available < 3 || unavailable < 2 || osdAuthoritative == 0 {
		return fmt.Errorf("required MON/OSD lifecycle events are incomplete")
	}
	return nil
}

func validMonitors(monitors []monitorState) bool {
	seen := make(map[string]bool, len(monitors))
	for _, monitor := range monitors {
		if monitor.Name == "" || monitor.Rank < 0 || monitor.Address == "" || seen[monitor.Name] {
			return false
		}
		seen[monitor.Name] = true
	}
	return true
}

func findMonitor(monitors []monitorState, name string) (monitorState, bool) {
	for _, monitor := range monitors {
		if monitor.Name == name {
			return monitor, true
		}
	}
	return monitorState{}, false
}

func validMapping(value mapping, pool string) bool {
	maximum := 3
	if pool == "p13-size1" {
		maximum = 1
	}
	if len(value.Acting) == 0 || len(value.Acting) > maximum || value.Primary < 0 || value.Primary > 3 || !contains(value.Acting, value.Primary) {
		return false
	}
	seen := make(map[int32]bool, len(value.Acting))
	for _, id := range value.Acting {
		if id < 0 || id > 3 || seen[id] {
			return false
		}
		seen[id] = true
	}
	return true
}

func absDuration(value time.Duration) time.Duration {
	if value < 0 {
		return -value
	}
	return value
}

func count(value, substring string) int {
	result := 0
	for {
		index := len(value)
		for offset := 0; offset+len(substring) <= len(value); offset++ {
			if value[offset:offset+len(substring)] == substring {
				index = offset
				break
			}
		}
		if index == len(value) {
			return result
		}
		result++
		value = value[index+len(substring):]
	}
}

func markerBytes(markers []string) int {
	total := 0
	for _, marker := range markers {
		total += len(marker)
	}
	return total
}

func implementationHashes() map[string]string {
	result := make(map[string]string)
	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		path = filepath.ToSlash(strings.TrimPrefix(path, "./"))
		if !entry.Type().IsRegular() || entry.Name() == ".DS_Store" || path == "docs/p13/integration-report.json" || strings.HasPrefix(path, "docs/p13/.integration-report.") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(data)
		result[path] = hex.EncodeToString(sum[:])
		return nil
	})
	if err != nil {
		fatalf("walk source artifacts: %v", err)
	}
	return result
}

func contains(values []int32, expected int32) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func slicesEqual(left, right []int32) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func stringsEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
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
