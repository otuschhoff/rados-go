package main

import "testing"

func TestPinnedFormats(t *testing.T) {
	if !commitRE.MatchString("69f84cc2651aa259a15bc192ddaabd3baba07489") {
		t.Fatal("valid commit rejected")
	}
	if commitRE.MatchString("v20.2.0") {
		t.Fatal("tag accepted as immutable commit")
	}
	if !digestRE.MatchString("sha256:1228c3d05e45fbc068a8c33614e4409b6dac688bcc77369b06009b5830fa8d86") {
		t.Fatal("valid digest rejected")
	}
}

func TestUUIDFormat(t *testing.T) {
	if !uuidRE.MatchString("54c9af74-7db8-4b8c-8d48-8bd8f91f25a4") {
		t.Fatal("valid UUID rejected")
	}
	if uuidRE.MatchString("not-a-cluster") {
		t.Fatal("invalid UUID accepted")
	}
}
