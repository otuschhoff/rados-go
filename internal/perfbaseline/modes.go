package perfbaseline

import "fmt"

type ConnectionMode struct {
	Service    string `json:"service"`
	Connection string `json:"connection"`
	Actual     string `json:"actual"`
	Source     string `json:"source"`
}

type ModeEvidence struct {
	Implementation string           `json:"implementation"`
	Requested      string           `json:"requested"`
	Connections    []ConnectionMode `json:"connections"`
}

func ValidateModeEvidence(evidence ModeEvidence) error {
	if evidence.Implementation != "go" && evidence.Implementation != "native" {
		return fmt.Errorf("unknown implementation %q", evidence.Implementation)
	}
	if evidence.Requested != "secure" && evidence.Requested != "crc" {
		return fmt.Errorf("unknown requested mode %q", evidence.Requested)
	}
	expectedSource := "go-auth-metadata"
	if evidence.Implementation == "native" {
		expectedSource = "ceph-ready-log"
	}
	seen := make(map[string]bool)
	services := make(map[string]bool)
	for _, connection := range evidence.Connections {
		if connection.Service != "monitor" && connection.Service != "osd" {
			return fmt.Errorf("unknown service %q", connection.Service)
		}
		if connection.Connection == "" || connection.Source != expectedSource {
			return fmt.Errorf("missing connection identity or untrusted source %q", connection.Source)
		}
		identity := connection.Service + ":" + connection.Connection
		if seen[identity] {
			return fmt.Errorf("duplicate connection %q", identity)
		}
		seen[identity] = true
		services[connection.Service] = true
		if connection.Actual != "secure" && connection.Actual != "crc" {
			return fmt.Errorf("unknown actual mode %q for %s", connection.Actual, identity)
		}
		if connection.Actual != evidence.Requested {
			return fmt.Errorf("requested %s but negotiated %s for %s", evidence.Requested, connection.Actual, identity)
		}
	}
	if !services["monitor"] || !services["osd"] {
		return fmt.Errorf("actual monitor and OSD negotiation evidence required")
	}
	return nil
}

func ValidateModePair(goEvidence, nativeEvidence ModeEvidence) error {
	if goEvidence.Implementation != "go" || nativeEvidence.Implementation != "native" {
		return fmt.Errorf("pair must contain Go followed by native evidence")
	}
	for _, evidence := range []ModeEvidence{goEvidence, nativeEvidence} {
		if err := ValidateModeEvidence(evidence); err != nil {
			return fmt.Errorf("%s: %w", evidence.Implementation, err)
		}
	}
	if goEvidence.Requested != nativeEvidence.Requested {
		return fmt.Errorf("mixed requested modes: Go %s, native %s", goEvidence.Requested, nativeEvidence.Requested)
	}
	return nil
}
