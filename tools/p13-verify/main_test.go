package main

import (
	"os"
	"testing"
	"time"
)

func TestReadReportRejectsUnknownAndTrailingJSON(t *testing.T) {
	for _, input := range []string{
		`{"schema_version":1,"unknown":true}`,
		`{"schema_version":1}{}`,
	} {
		path := t.TempDir() + "/report.json"
		if err := os.WriteFile(path, []byte(input), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := readReport(path); err == nil {
			t.Fatalf("accepted %q", input)
		}
	}
}

func TestValidateFailedReportRequiresOptIn(t *testing.T) {
	failure := "injected failure"
	value := report{
		SchemaVersion: 1,
		Status:        "failed",
		Command:       "./integration/p13/reproduce.sh --quick",
		Mode:          "quick",
		StartedAt:     "2026-01-01T00:00:00Z",
		FinishedAt:    "2026-01-01T00:00:01Z",
		Failure:       &failure,
	}
	value.Source.Repository = "https://github.com/otuschhoff/rados-go.git"
	value.Unqualified.MultiHostMaintenance = "unqualified: disposable harness uses one Docker host"
	value.Unqualified.ProbabilisticPacketLoss = "unqualified: no host-global or probabilistic network fault injection is used"
	if err := validate(value, false, false); err == nil {
		t.Fatal("failed report passed without opt-in")
	}
	if err := validate(value, true, false); err != nil {
		t.Fatalf("failed report with opt-in: %v", err)
	}
}

func TestValidateScenarioRejectsCriticalMutations(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	finish := start.Add(time.Minute)
	validResult := operationResult{Transport: "secure", Operation: "read", Object: "object", StartedAt: start.Format(time.RFC3339Nano), FinishedAt: finish.Format(time.RFC3339Nano), ElapsedNS: int64(time.Minute), Completed: true, Outcome: "success", Version: 1, Data: "value"}
	value := scenario{
		ID: "p13-baseline-size3", Status: "passed", Pool: "p13-size3", Operation: "read", Fault: "none",
		InjectedAt: start.Format(time.RFC3339Nano), ReleasedAt: finish.Format(time.RFC3339Nano),
		Before: mapping{Epoch: 1, PGID: "1.0", Acting: []int32{0}, Primary: 0, PGState: "active+clean", ObservedAt: start.Format(time.RFC3339Nano)},
		After:  mapping{Epoch: 2, PGID: "1.0", Acting: []int32{0}, Primary: 0, PGState: "active+clean", ObservedAt: finish.Format(time.RFC3339Nano)},
		Go:     validResult, Native: validResult, FinalData: "value", Notes: "paired",
	}
	value.Go.Implementation = "go"
	value.Native.Implementation = "native"
	if err := validateScenario(value, 0, "full", start, finish, make(map[string]bool)); err != nil {
		t.Fatalf("valid scenario: %v", err)
	}
	mutations := []struct {
		name   string
		mutate func(*scenario)
	}{
		{"nonzero errno", func(value *scenario) { value.Go.Errno = 5 }},
		{"native mismatch", func(value *scenario) { value.Native.Data = "different" }},
		{"object substitution", func(value *scenario) { value.Native.Object = "other" }},
		{"reverse chronology", func(value *scenario) { value.ReleasedAt = start.Add(-time.Second).Format(time.RFC3339Nano) }},
		{"elapsed mismatch", func(value *scenario) { value.Go.ElapsedNS = 1 }},
		{"changed pg", func(value *scenario) { value.After.PGID = "1.1" }},
		{"primary outside acting", func(value *scenario) { value.After.Primary = 1 }},
		{"unknown osd", func(value *scenario) { value.After.Acting = []int32{99}; value.After.Primary = 99 }},
		{"duplicate acting osd", func(value *scenario) { value.After.Acting = []int32{0, 0} }},
		{"duplicate marker", func(value *scenario) { value.MutationMarkers = []string{"x", "x"}; value.FinalData = "xx" }},
	}
	for _, test := range mutations {
		t.Run(test.name, func(t *testing.T) {
			mutated := value
			test.mutate(&mutated)
			if err := validateScenario(mutated, 0, "full", start, finish, make(map[string]bool)); err == nil {
				t.Fatal("mutation was accepted")
			}
		})
	}
}

func TestValidateScenarioRequiresNativeWatch(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	finish := start.Add(14 * time.Second)
	watchResult := operationResult{Transport: "secure", Operation: "watch", Object: "watched", StartedAt: start.Format(time.RFC3339Nano), FinishedAt: finish.Format(time.RFC3339Nano), ElapsedNS: int64(14 * time.Second), Completed: true, Outcome: "success", WatchCookie: 1, WatchEvents: 2}
	value := scenario{
		ID: "p13-noout-long-watch", Status: "passed", Pool: "p13-size3", Operation: "watch", Fault: "global-noout-and-primary-restart",
		InjectedAt: start.Add(time.Second).Format(time.RFC3339Nano), ReleasedAt: finish.Add(-time.Second).Format(time.RFC3339Nano),
		Before: mapping{Epoch: 1, PGID: "1.0", Acting: []int32{0, 1, 2}, Primary: 0, PGState: "active+clean", ObservedAt: start.Format(time.RFC3339Nano)},
		After:  mapping{Epoch: 2, PGID: "1.0", Acting: []int32{0, 1, 2}, Primary: 0, PGState: "active+clean", ObservedAt: finish.Format(time.RFC3339Nano)},
		Go:     watchResult, Native: watchResult, FinalData: "watch", Notes: "paired",
	}
	value.Go.Implementation = "go"
	value.Go.WatchInterruptions = 1
	value.Native.Implementation = "native"
	if err := validateScenario(value, 0, "full", start, finish, make(map[string]bool)); err != nil {
		t.Fatalf("valid watch: %v", err)
	}
	value.ReleasedAt = finish.Add(-3 * time.Second).Format(time.RFC3339Nano)
	if err := validateScenario(value, 0, "full", start, finish, make(map[string]bool)); err == nil {
		t.Fatal("short full-mode watch outage was accepted")
	}
	value.ReleasedAt = finish.Add(-time.Second).Format(time.RFC3339Nano)
	value.Native.WatchEvents = 0
	if err := validateScenario(value, 0, "full", start, finish, make(map[string]bool)); err == nil {
		t.Fatal("missing native watch events were accepted")
	}
}

func TestValidateScenarioRequiresPairedMutationMarkers(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	finish := start.Add(time.Minute)
	result := operationResult{Transport: "secure", Operation: "append", Object: "object", StartedAt: start.Format(time.RFC3339Nano), FinishedAt: finish.Format(time.RFC3339Nano), ElapsedNS: int64(time.Minute), Completed: true, Outcome: "success", Version: 1}
	value := scenario{
		ID: "p13-flaky-stop-start", Status: "passed", Pool: "p13-size3", Operation: "append", Fault: "bounded-stop-start",
		InjectedAt: start.Format(time.RFC3339Nano), ReleasedAt: finish.Format(time.RFC3339Nano),
		Before: mapping{Epoch: 1, PGID: "1.0", Acting: []int32{0, 1, 2}, Primary: 0, PGState: "active+clean", ObservedAt: start.Format(time.RFC3339Nano)},
		After:  mapping{Epoch: 2, PGID: "1.0", Acting: []int32{1, 2, 0}, Primary: 1, PGState: "active+clean", ObservedAt: finish.Format(time.RFC3339Nano)},
		Go: result, Native: result, FinalData: "go-marker-1;native-marker-1;go-marker-2;native-marker-2;",
		MutationMarkers: []string{"go-marker-1;", "native-marker-1;", "go-marker-2;", "native-marker-2;"}, Notes: "paired",
		FaultIntervals: []faultInterval{
			{InjectedAt: start.Add(time.Second).Format(time.RFC3339Nano), ReleasedAt: start.Add(3 * time.Second).Format(time.RFC3339Nano)},
			{InjectedAt: start.Add(4 * time.Second).Format(time.RFC3339Nano), ReleasedAt: start.Add(6 * time.Second).Format(time.RFC3339Nano)},
		},
	}
	value.Go.Implementation, value.Go.MutationMarker = "go", "go-marker-2;"
	value.Native.Implementation, value.Native.MutationMarker = "native", "native-marker-2;"
	if err := validateScenario(value, 0, "quick", start, finish, make(map[string]bool)); err != nil {
		t.Fatalf("valid append: %v", err)
	}
	value.FinalData = "native-marker-1;go-marker-1;go-marker-2;native-marker-2;"
	if err := validateScenario(value, 0, "quick", start, finish, make(map[string]bool)); err == nil {
		t.Fatal("marker metadata order differing from final data was accepted")
	}
	value.FinalData = "go-marker-1;native-marker-1;go-marker-2;native-marker-2;"
	value.FaultIntervals[1].ReleasedAt = start.Add(5 * time.Second).Format(time.RFC3339Nano)
	if err := validateScenario(value, 0, "quick", start, finish, make(map[string]bool)); err == nil {
		t.Fatal("short mutation fault interval was accepted")
	}
	value.FaultIntervals[1].ReleasedAt = start.Add(6 * time.Second).Format(time.RFC3339Nano)
	value.Native.MutationMarker = "forged;"
	if err := validateScenario(value, 0, "quick", start, finish, make(map[string]bool)); err == nil {
		t.Fatal("unbound native marker was accepted")
	}
}

func TestValidateMonitorLifecycleTransitions(t *testing.T) {
	three := []monitorState{{Name: "a", Rank: 0, Address: "172.30.113.10:3300"}, {Name: "b", Rank: 1, Address: "172.30.113.12:3300"}, {Name: "c", Rank: 2, Address: "172.30.113.13:3300"}}
	two := append([]monitorState(nil), three[:2]...)
	changed := append([]monitorState(nil), three...)
	changed[2].Location = map[string]string{"rack": "r1"}
	tests := []struct {
		id     string
		before []monitorState
		after  []monitorState
	}{
		{"p13-mon-offline-failover", three, three},
		{"p13-mon-unreliable", three, three},
		{"p13-mon-changed", three, changed},
		{"p13-mon-removed", three, two},
		{"p13-mon-added", two, three},
	}
	for _, test := range tests {
		t.Run(test.id, func(t *testing.T) {
			value := monitorLifecycle{Before: monitorMap{Epoch: 1, Monitors: test.before}, After: monitorMap{Epoch: 2, Monitors: test.after}}
			if err := validateMonitorLifecycle(test.id, value); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestValidateChangeObservationsRequiresOrderedMONAndOSDEvidence(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	mon := func(name string) *publicMONState { return &publicMONState{Name: name, Rank: 0} }
	events := []changeObservation{
		{Component: "mon", Source: "observed", Kind: "available", MON: mon("a")},
		{Component: "mon", Source: "authoritative", Kind: "added", Epoch: 1, MON: mon("a")},
		{Component: "mon", Source: "authoritative", Kind: "added", Epoch: 1, MON: mon("b")},
		{Component: "mon", Source: "authoritative", Kind: "added", Epoch: 1, MON: mon("c")},
		{Component: "osd", Source: "authoritative", Kind: "added", Epoch: 1, OSD: &publicOSDState{ID: 0, Exists: true}},
		{Component: "mon", Source: "observed", Kind: "unavailable", MON: mon("a")},
		{Component: "mon", Source: "observed", Kind: "available", MON: mon("b")},
		{Component: "mon", Source: "observed", Kind: "unavailable", MON: mon("b")},
		{Component: "mon", Source: "observed", Kind: "available", MON: mon("c")},
		{Component: "mon", Source: "authoritative", Kind: "changed", Epoch: 2, MON: mon("c"), PreviousMON: mon("c")},
		{Component: "mon", Source: "authoritative", Kind: "removed", Epoch: 3, PreviousMON: mon("c")},
		{Component: "mon", Source: "authoritative", Kind: "added", Epoch: 4, MON: mon("c")},
	}
	for index := range events {
		events[index].Sequence = uint64(index + 1)
		events[index].ObservedAt = start.Add(time.Duration(index) * time.Millisecond).Format(time.RFC3339Nano)
	}
	value := operationResult{Implementation: "go", Transport: "secure", Operation: "cluster-changes", Completed: true, Outcome: "success", Changes: events}
	if err := validateChangeObservations(value, start, start.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	value.Changes[1].Sequence = 99
	if err := validateChangeObservations(value, start, start.Add(time.Minute)); err == nil {
		t.Fatal("out-of-order observations were accepted")
	}
}
