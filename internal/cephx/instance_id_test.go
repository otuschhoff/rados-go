package cephx

import (
	"sync"
	"testing"
)

func TestConnectorInstanceID(t *testing.T) {
	connector := &Connector{}
	for _, state := range []connectorState{{}, {globalID: 77}, {globalID: 77, mode: ConModeSecure}, {globalID: 88, mode: ConModeCRC}} {
		connector.state = state
		if got, want := connector.InstanceID(), connector.AuthMetadata().GlobalID; got != want {
			t.Fatalf("identity=%d metadata=%d", got, want)
		}
	}
	if count := testing.AllocsPerRun(100, func() { _ = connector.InstanceID() }); count != 0 {
		t.Fatalf("scalar identity allocated %g times", count)
	}
}

func TestConnectorInstanceIDConcurrentState(t *testing.T) {
	connector := &Connector{}
	var workers sync.WaitGroup
	workers.Go(func() {
		for iteration := 0; iteration < 1000; iteration++ {
			connector.mu.Lock()
			connector.state = connectorState{globalID: 77, mode: ConModeSecure}
			connector.mu.Unlock()
		}
	})
	for iteration := 0; iteration < 1000; iteration++ {
		if got := connector.InstanceID(); got != 0 && got != 77 {
			t.Fatalf("invalid identity %d", got)
		}
	}
	workers.Wait()
}
