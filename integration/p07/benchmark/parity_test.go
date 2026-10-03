//go:build linux

package main

import (
	"strings"
	"testing"
)

func TestParityFixtureContract(t *testing.T) {
	for _, value := range []string{"", "p07-parity-20261002", "p07-parity-" + strings.Repeat("a", 53)} {
		actual, err := parityNamespace(value)
		if err != nil || actual != value {
			t.Fatalf("namespace %q: got %q, %v", value, actual, err)
		}
	}
	for _, value := range []string{"default", "p07-parity-/escape", "p07-parity-ABC", "p07-parity-" + strings.Repeat("a", 54)} {
		if _, err := parityNamespace(value); err == nil {
			t.Fatalf("accepted invalid namespace %q", value)
		}
	}
	if actual := parityObjectName(1048576, 16, "mixed", 3); actual != "p07-parity-1048576-c16-mixed-w3" {
		t.Fatalf("unexpected fixture name %q", actual)
	}
}
