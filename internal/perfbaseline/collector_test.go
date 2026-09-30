package perfbaseline

import (
	"sync"
	"testing"
)

func TestModeCollector(t *testing.T) {
	collector := NewModeCollector("secure")
	collector.Observe("monitor", 0, 2)
	collector.Observe("osd", 3, 2)
	snapshot := collector.Evidence()
	if err := ValidateModeEvidence(snapshot); err != nil {
		t.Fatal(err)
	}
	snapshot.Connections[0].Actual = "crc"
	if err := ValidateModeEvidence(collector.Evidence()); err != nil {
		t.Fatal("snapshot aliases collector")
	}
	collector.Observe("osd", 3, 1)
	if err := ValidateModeEvidence(collector.Evidence()); err == nil {
		t.Fatal("mixed reconnect modes accepted")
	}
	unknown := NewModeCollector("secure")
	unknown.Observe("monitor", 0, 0)
	unknown.Observe("osd", 0, 99)
	if err := ValidateModeEvidence(unknown.Evidence()); err == nil {
		t.Fatal("unknown accepted")
	}
}

func TestModeCollectorConcurrent(t *testing.T) {
	collector := NewModeCollector("crc")
	collector.Observe("monitor", 0, 1)
	var workers sync.WaitGroup
	for index := range 32 {
		workers.Add(1)
		go func() { defer workers.Done(); collector.Observe("osd", int32(index), 1); _ = collector.Evidence() }()
	}
	workers.Wait()
	if err := ValidateModeEvidence(collector.Evidence()); err != nil {
		t.Fatal(err)
	}
}
