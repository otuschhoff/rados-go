package perfbaseline

import (
	"fmt"
	"sync"
)

type ModeCollector struct {
	mu       sync.Mutex
	evidence ModeEvidence
}

func NewModeCollector(requested string) *ModeCollector {
	return &ModeCollector{evidence: ModeEvidence{Implementation: "go", Requested: requested, Connections: []ConnectionMode{}}}
}

func (collector *ModeCollector) Observe(service string, serviceID int32, mode uint32) {
	actual := "unknown"
	switch mode {
	case 1:
		actual = "crc"
	case 2:
		actual = "secure"
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	collector.evidence.Connections = append(collector.evidence.Connections, ConnectionMode{
		Service: service, Connection: fmt.Sprintf("%s.%d/%d", service, serviceID, len(collector.evidence.Connections)+1), Actual: actual, Source: "go-auth-metadata",
	})
}

func (collector *ModeCollector) Evidence() ModeEvidence {
	collector.mu.Lock()
	defer collector.mu.Unlock()
	evidence := collector.evidence
	evidence.Connections = append([]ConnectionMode{}, evidence.Connections...)
	return evidence
}
