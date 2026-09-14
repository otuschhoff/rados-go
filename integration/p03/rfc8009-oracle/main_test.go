package main

import (
	"bytes"
	"encoding/hex"
	"testing"
)

func TestRFC8009AES256Vector(t *testing.T) {
	key := decodeHex(t, "6d404d37faf79f9df0d33568d320669800eb4836472ea8a026d16b7182460c52")
	ciphertext := decodeHex(t, "4ed7b37c2bcac8f74f23c1cf07e62bc7b75fb3f637b9f559c7f664f69eab7b6092237526ea0d1f61cb20d69d10f2")
	plaintext, err := decrypt(key, ciphertext, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(plaintext, []byte{0, 1, 2, 3, 4, 5}) {
		t.Fatalf("plaintext = %x", plaintext)
	}
	if _, err := decrypt(key, ciphertext, 3); err == nil {
		t.Fatal("wrong key usage unexpectedly succeeded")
	}
	ciphertext[0] ^= 1
	if _, err := decrypt(key, ciphertext, 2); err == nil {
		t.Fatal("tampered ciphertext unexpectedly succeeded")
	}
}

func decodeHex(t *testing.T, value string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(value)
	if err != nil {
		t.Fatal(err)
	}
	return decoded
}
