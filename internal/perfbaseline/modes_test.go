package perfbaseline

import "testing"

func modeEvidence(implementation, mode string) ModeEvidence {
	source := "go-auth-metadata"
	if implementation == "native" {
		source = "ceph-ready-log"
	}
	return ModeEvidence{Implementation: implementation, Requested: mode, Connections: []ConnectionMode{
		{Service: "monitor", Connection: "1", Actual: mode, Source: source},
		{Service: "osd", Connection: "2", Actual: mode, Source: source},
	}}
}

func TestValidateModePair(t *testing.T) {
	for _, mode := range []string{"secure", "crc"} {
		if err := ValidateModePair(modeEvidence("go", mode), modeEvidence("native", mode)); err != nil {
			t.Fatal(err)
		}
	}
	cases := map[string]func(*ModeEvidence, *ModeEvidence){
		"unknown actual":         func(goMode, native *ModeEvidence) { goMode.Connections[0].Actual = "unknown" },
		"CRC prefers secure":     func(goMode, native *ModeEvidence) { goMode.Requested = "crc" },
		"mixed actual":           func(goMode, native *ModeEvidence) { native.Connections[1].Actual = "crc" },
		"mixed requested":        func(goMode, native *ModeEvidence) { *native = modeEvidence("native", "crc") },
		"missing OSD":            func(goMode, native *ModeEvidence) { goMode.Connections = goMode.Connections[:1] },
		"config is not evidence": func(goMode, native *ModeEvidence) { native.Connections[0].Source = "ms_client_mode" },
		"unknown request":        func(goMode, native *ModeEvidence) { native.Requested = "unknown" },
		"duplicate": func(goMode, native *ModeEvidence) {
			goMode.Connections = append(goMode.Connections, goMode.Connections[0])
		},
		"no connections": func(goMode, native *ModeEvidence) { native.Connections = nil },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			goMode, native := modeEvidence("go", "secure"), modeEvidence("native", "secure")
			mutate(&goMode, &native)
			if err := ValidateModePair(goMode, native); err == nil {
				t.Fatal("invalid claim accepted")
			}
		})
	}
}
