package crush

import "testing"

func TestP00ObjectHashVector(t *testing.T) {
	hash := ObjectHash("p00-smoke-object", "", "")
	if hash != 0x96fc93a8 {
		t.Fatalf("hash = %#x, want %#x", hash, uint32(0x96fc93a8))
	}
	if pg := StableMod(hash, 32); pg != 8 {
		t.Fatalf("pg = %d, want 8", pg)
	}
}

func TestObjectHashIdentity(t *testing.T) {
	object := ObjectHash("object", "", "namespace")
	locator := ObjectHash("object", "locator", "namespace")
	if object == locator {
		t.Fatal("locator did not replace object hash key")
	}
	if locator != ObjectHash("different", "locator", "namespace") {
		t.Fatal("object name affected locator-key hash")
	}
	if object == ObjectHash("object", "", "") {
		t.Fatal("namespace did not affect object hash")
	}
}

func TestStableModNonPowerOfTwo(t *testing.T) {
	for value := uint32(0); value < 256; value++ {
		mapped := StableMod(value, 12)
		if mapped >= 12 {
			t.Fatalf("StableMod(%d, 12) = %d", value, mapped)
		}
		if value < 8 && mapped != value {
			t.Fatalf("StableMod(%d, 12) = %d", value, mapped)
		}
	}
}

func TestHash32TripleVectors(t *testing.T) {
	tests := []struct {
		a, b, c uint32
		want    uint32
	}{
		{a: 0, b: 0, c: 0, want: 0x7a3bf3b2},
		{a: 1, b: 2, c: 3, want: 0x735ad42b},
		{a: 0xdeadbeef, b: 0xffffffff, c: 17, want: 0xe8247cea},
		{a: 123456789, b: 0xfffffffe, c: 42, want: 0x6d589f4b},
	}
	for _, test := range tests {
		if got := Hash32Triple(test.a, test.b, test.c); got != test.want {
			t.Fatalf("Hash32Triple(%#x, %#x, %#x) = %#x, want %#x", test.a, test.b, test.c, got, test.want)
		}
	}
}
