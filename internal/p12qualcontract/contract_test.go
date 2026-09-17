package p12qualcontract

import (
	"strings"
	"testing"
)

func TestQualificationContractCardinality(t *testing.T) {
	if got := len(Checks(DefaultPins(), "v0.0.0-test")); got != 29 {
		t.Fatalf("qualification check count = %d, want 29; update the strict report schema with the contract", got)
	}
	if got := len(Runtimes(DefaultPins())); got != 4 {
		t.Fatalf("qualification runtime count = %d, want 4; update the strict report schema with the contract", got)
	}
}

func TestDeterministicReleasePinsLatestToolchain(t *testing.T) {
	specs := Checks(DefaultPins(), "v0.0.0-test")
	spec := specs[len(specs)-1]
	prefix := "CGO_ENABLED=0 GOTOOLCHAIN=" + LatestGo + " go run ./tools/p12-release"
	if spec.ID != "deterministic-release" || strings.Count(spec.Command, prefix) != 2 || spec.Toolchain != LatestGo {
		t.Fatalf("deterministic release is not exactly pinned to %s: %#v", LatestGo, spec)
	}
}

func TestEveryGoCommandPinsItsDeclaredToolchain(t *testing.T) {
	specs := append(Checks(DefaultPins(), "v0.0.0-test"), Runtimes(DefaultPins())...)
	verbs := []string{"build", "list", "mod", "run", "test", "version", "vet"}
	for _, spec := range specs {
		invocations := 0
		for _, verb := range verbs {
			invocations += strings.Count(spec.Command, "go "+verb)
		}
		if invocations == 0 {
			continue
		}
		toolchains := strings.Count(spec.Command, "GOTOOLCHAIN="+spec.Toolchain)
		cgoSettings := strings.Count(spec.Command, "CGO_ENABLED=0") + strings.Count(spec.Command, "CGO_ENABLED=1")
		if toolchains != invocations || cgoSettings != invocations || strings.Contains(spec.Command, "GOTOOLCHAIN="+spec.Toolchain+" CGO_ENABLED=") {
			t.Fatalf("check %s has an unpinned or inconsistently ordered Go command: %q", spec.ID, spec.Command)
		}
	}
}
