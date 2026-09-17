package p12fuzzcontract

import (
	"slices"
	"testing"
	"time"
)

func TestContractIsExactAndImmutable(t *testing.T) {
	targets := Targets()
	if len(targets) != 31 {
		t.Fatalf("target count = %d, want 31", len(targets))
	}
	if targets[0] != (Target{Package: ".", Name: "FuzzParseConfig"}) || targets[len(targets)-1] != (Target{Package: "./internal/protocol", Name: "FuzzEntityAddrVec"}) {
		t.Fatalf("unexpected target boundaries: %#v", targets)
	}
	targets[0].Name = "changed"
	if Targets()[0].Name != "FuzzParseConfig" {
		t.Fatal("Targets exposed mutable contract storage")
	}
	seen := make(map[Target]bool, len(targets))
	for _, target := range Targets() {
		if target.Package == "" || target.Name == "" || seen[target] {
			t.Fatalf("invalid or duplicate target: %#v", target)
		}
		seen[target] = true
	}
}

func TestProfilesAndCommandArePinned(t *testing.T) {
	if budget, ok := Budget(SmokeProfile); !ok || budget != 60*time.Second {
		t.Fatalf("smoke budget = %v, %v", budget, ok)
	}
	if budget, ok := Budget(CertifyingProfile); !ok || budget != 10*time.Minute {
		t.Fatalf("certifying budget = %v, %v", budget, ok)
	}
	if _, ok := Budget("test"); ok {
		t.Fatal("unknown production profile accepted")
	}
	want := []string{"go", "test", "./internal/maps", "-run", "^$", "-fuzz", "^FuzzDecodeMonMap$", "-fuzztime=10m"}
	if got := Command(Target{Package: "./internal/maps", Name: "FuzzDecodeMonMap"}, CertifyingBudget); !slices.Equal(got, want) {
		t.Fatalf("command = %#v, want %#v", got, want)
	}
}