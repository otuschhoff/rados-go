//go:build linux

package main

import "testing"

func TestEnduranceConfig(t *testing.T) {
	valid := map[string]string{"P11_ENDURANCE_ROOT": "/private/endurance", "P07_QUALIFICATION_FILE": "/private/endurance/window0.capture.json", "P07_QUALIFICATION_ROUND": "1", "P07_QUALIFICATION_LEG": "endurance-w0", "P07_QUALIFICATION_SEED": "1101", "P07_PGO_DIAGNOSTIC": "1", "P07_PARITY_NAMESPACE": "p07-parity-endurance", "P07_READ_INTO": "1", "GOMAXPROCS": "10", "GOGC": "100", "GOMEMLIMIT": "off", "P07_MODE_EVIDENCE_FILE": "/private/endurance/modes.json", "P07_MATRIX_SIZE": "1048576", "P07_MATRIX_CONCURRENCY": "16", "P07_MATRIX_WORKLOAD": "read"}
	if _, err := parseQualificationConfig(func(key string) string { return valid[key] }); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range [][2]string{{"P11_ENDURANCE_ROOT", "relative"}, {"P07_QUALIFICATION_FILE", ""}, {"P07_QUALIFICATION_FILE", "/private/wrong.json"}, {"P07_QUALIFICATION_ROUND", "2"}, {"P07_QUALIFICATION_SEED", "1102"}, {"P07_QUALIFICATION_LEG", "wrong"}, {"P07_PGO_DIAGNOSTIC", ""}, {"P07_PGO_PROFILE_FILE", "/private/profile"}, {"P07_MATRIX_SIZE", "4096"}, {"P07_MATRIX_CONCURRENCY", "1"}, {"P07_MATRIX_WORKLOAD", "write"}} {
		t.Run(invalid[0]+"="+invalid[1], func(t *testing.T) {
			_, err := parseQualificationConfig(func(key string) string {
				if key == invalid[0] {
					return invalid[1]
				}
				return valid[key]
			})
			if err == nil {
				t.Fatal("invalid endurance configuration accepted before connect")
			}
		})
	}
	for index := 0; index < 12; index++ {
		if got := enduranceWorkload(index); got != []string{"read", "write", "mixed"}[index%3] {
			t.Fatal(got)
		}
	}
}
