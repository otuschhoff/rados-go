package perfbaseline

import (
	"strings"
	"testing"
)

func TestNativeCaptureSanitizedLiveLogs(t *testing.T) {
	const monitor = "2026-09-30T12:26:49.420+0000 ffff8eeab120  1 --2- 192.0.2.2:0/123 >> v2:192.0.2.10:3300/0 conn(0x123 0x456 secure :-1 s=READY pgs=60 gs=1 cs=0 l=1 c_cookie=0 s_cookie=0 reconnecting=0 rev1=1 crypto rx=0x789 tx=0xabc comp rx=0 tx=0).ready entity=mon.0 client_cookie=0 server_cookie=0 in_seq=0 out_seq=0\n"
	const osd = "2026-09-30T12:26:49.423+0000 ffff8e69b120  1 --2- 192.0.2.2:0/123 >> v2:192.0.2.21:6800/456 conn(0xdef 0xabc secure :-1 s=READY pgs=5 gs=2 cs=0 l=1 c_cookie=0 s_cookie=0 reconnecting=0 rev1=1 crypto rx=0x123 tx=0x456 comp rx=0 tx=0).ready entity=osd.1 client_cookie=0 server_cookie=0 in_seq=0 out_seq=0\n"
	for _, spacing := range []string{"", " ", "\t", " \t "} {
		for _, mode := range []string{"secure", "crc"} {
			t.Run(mode+"/"+spacing, func(t *testing.T) {
				log := monitor + osd
				if mode == "crc" {
					log = monitor + strings.Replace(osd, "secure :-1", "crc :-1", 1)
					log = strings.Replace(log, "crypto rx=0x123 tx=0x456", "crypto rx=0 tx=0", 1)
				}
				log = strings.ReplaceAll(log, ").ready", ")."+spacing+"ready")
				evidence, err := CaptureNativeModes(strings.NewReader(log), mode)
				if err != nil {
					t.Fatal(err)
				}
				if len(evidence.Connections) != 2 || evidence.Connections[0].Service != "monitor" || evidence.Connections[0].Actual != "secure" || evidence.Connections[1].Service != "osd" || evidence.Connections[1].Actual != mode {
					t.Fatalf("incorrect actual modes: %+v", evidence)
				}
				err = ValidateModeEvidence(evidence)
				if (err == nil) != (mode == "secure") {
					t.Fatalf("mode %s: validation error = %v", mode, err)
				}
			})
		}
	}
	for _, peer := range []string{"mon.0", "osd.1"} {
		for _, record := range []string{
			"--2- unsupported).ready entity=" + peer,
			"--2- unsupported). ready entity=" + peer,
			"--2- unsupported).\tready entity=" + peer,
			strings.Replace(strings.Replace(monitor, "mon.0", peer, 1), ").ready", ") ready", 1),
		} {
			_, err := CaptureNativeModes(strings.NewReader(monitor+osd+record+"\n"), "secure")
			if err == nil || !strings.Contains(err.Error(), "unsupported native ready record at line 3") {
				t.Fatalf("unsupported relevant record silently skipped: %q, error = %v", record, err)
			}
		}
	}
	log := monitor + osd + "unsupported).ready entity=mgr.0\nunsupported). ready entity=client.0\n"
	evidence, err := CaptureNativeModes(strings.NewReader(log), "secure")
	if err != nil || len(evidence.Connections) != 2 {
		t.Fatalf("irrelevant records affected capture: %+v, %v", evidence, err)
	}
}

func TestNativeCapturePinnedProtocolV2(t *testing.T) {
	const source = "https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/msg/async/ProtocolV2.cc"
	for _, mode := range []string{"secure", "crc"} {
		var log strings.Builder
		for _, peer := range []string{"mon.0", "osd.1"} {
			log.WriteString("--2- [v2:local] >> [v2:peer] conn(0x123 0x456 ")
			log.WriteString(mode)
			log.WriteString(" :0 s=READY pgs=1 gs=2 cs=0 l=0 c_cookie=123 s_cookie=456 reconnecting=0 rev1=1 crypto rx=0x789 tx=0xabc comp rx=0 tx=0). ready entity=")
			log.WriteString(peer)
			log.WriteString(" client_cookie=7b server_cookie=1c8 in_seq=0 out_seq=0\n")
		}
		evidence, err := CaptureNativeModes(strings.NewReader(log.String()), mode)
		if err != nil {
			t.Fatalf("%s: %v", source, err)
		}
		if err := ValidateModeEvidence(evidence); err != nil {
			t.Fatalf("%s: %v", source, err)
		}
	}
}

func TestNativeCapture(t *testing.T) {
	log := "config ms_client_mode=crc\n" +
		"--2- [v2:local] >> [v2:peer] conn(0x123 0x456 secure :0 s=READY pgs=1 cs=0 l=0 rev1=1 crypto rx=0 tx=0). ready entity=mon.0 client_cookie=123\n" +
		"--2- [v2:local] >> [v2:peer] conn(0x789 0xabc secure :0 s=READY pgs=1 cs=0 l=0 rev1=1 crypto rx=0 tx=0). ready entity=osd.1 client_cookie=456\n"
	evidence, err := CaptureNativeModes(strings.NewReader(log), "secure")
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateModeEvidence(evidence); err != nil {
		t.Fatal(err)
	}
	if len(evidence.Connections) != 2 || strings.Contains(evidence.Connections[0].Connection, "cookie") {
		t.Fatalf("evidence: %+v", evidence)
	}
	for _, input := range []string{
		"config ms_client_mode=secure\n",
		strings.ReplaceAll(log, "secure :", "unknown :"),
		strings.Replace(log, "osd.1", "mgr.1", 1),
		strings.Replace(log, "secure :", "crc :", 1),
		strings.Replace(log, "conn(0x123", "conn(unsupported", 1),
	} {
		evidence, err := CaptureNativeModes(strings.NewReader(input), "secure")
		if err == nil {
			err = ValidateModeEvidence(evidence)
		}
		if err == nil {
			t.Fatalf("unsupported or mismatched input accepted: %q", input)
		}
	}
}
