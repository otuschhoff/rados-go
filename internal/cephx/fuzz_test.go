package cephx

import (
	"testing"
	"time"
)

func FuzzParseServerChallenge(f *testing.F) {
	limits := defaultTestLimits()
	f.Add([]byte{1, 1, 0, 0, 0, 0, 0, 0, 0})
	f.Add([]byte{})
	f.Add([]byte{0xff, 1, 2})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = ParseServerChallenge(data, limits)
	})
}

func FuzzParseKey(f *testing.F) {
	f.Add(testEncodedKey)
	f.Add("")
	f.Add("%%not-base64%%")
	f.Fuzz(func(t *testing.T, encoded string) {
		_, _ = ParseKey("client.fuzz", encoded, 256)
	})
}

func FuzzParseKeyring(f *testing.F) {
	f.Add([]byte("[client.fuzz]\nkey = " + testEncodedKey + "\n"))
	f.Add([]byte{})
	f.Add([]byte("key = invalid\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = ParseKeyring(data, "client.fuzz", 4096)
	})
}

func FuzzParseAuthSessionReply(f *testing.F) {
	limits := defaultTestLimits()
	secret := mustSecretKey(f, "1234567890123456")
	f.Add([]byte{})
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6})
	f.Add([]byte{0, 1, 0, 0, 1, 0, 0, 0})
	f.Fuzz(func(t *testing.T, payload []byte) {
		_, _ = ParseAuthSessionReply(payload, secret, nil, ConModeSecure, time.Unix(0, 0), limits)
	})
}

func FuzzVerifyAuthorizerReply(f *testing.F) {
	limits := defaultTestLimits()
	secret := mustSecretKey(f, "abcdefghijklmnop")
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0})
	f.Add([]byte{1, 0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, payload []byte) {
		_, _ = VerifyAuthorizerReply(payload, secret, 1, limits)
	})
}

func FuzzAddAuthorizerChallenge(f *testing.F) {
	limits := defaultTestLimits()
	secret := mustSecretKey(f, "abcdefghijklmnop")
	authorizer := Authorizer{Base: []byte{1, 2, 3}, Payload: []byte{4, 5}, Nonce: 1, ServiceID: 1}
	f.Add([]byte{})
	f.Add([]byte{1, 0, 0, 0})
	f.Fuzz(func(t *testing.T, payload []byte) {
		_, _ = AddAuthorizerChallenge(authorizer, payload, secret, limits)
	})
}
