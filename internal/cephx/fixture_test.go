package cephx

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	wire "github.com/otuschhoff/go-librados/internal/encoding"
)

type p03EncodingCorpus struct {
	SchemaVersion int `json:"schema_version"`
	Vectors       []struct {
		Type         string `json:"type"`
		TestInstance int    `json:"test_instance"`
		Hex          string `json:"hex"`
	} `json:"vectors"`
}

type p03CryptoCorpus struct {
	SchemaVersion int `json:"schema_version"`
	Vectors       struct {
		CephAESCBC struct {
			KeyHex        string `json:"key_hex"`
			PlaintextHex  string `json:"plaintext_hex"`
			CiphertextHex string `json:"ciphertext_hex"`
		} `json:"ceph_aes_cbc"`
		RFC8009AES256 struct {
			KeyUsage      uint32 `json:"key_usage"`
			KeyHex        string `json:"key_hex"`
			CiphertextHex string `json:"ciphertext_hex"`
			PlaintextHex  string `json:"plaintext_hex"`
		} `json:"rfc8009_aes256"`
		AES256Challenge struct {
			KeyHex          string `json:"key_hex"`
			ServerChallenge uint64 `json:"server_challenge"`
			ClientChallenge uint64 `json:"client_challenge"`
			ResultHexLE     string `json:"result_hex_le"`
		} `json:"aes256_challenge"`
	} `json:"vectors"`
}

func TestP03CephDencoderFixtureParity(t *testing.T) {
	var corpus p03EncodingCorpus
	readP03JSON(t, "cephx-encoding-vectors.json", &corpus)
	if corpus.SchemaVersion != 1 || len(corpus.Vectors) != 4 {
		t.Fatalf("invalid encoding corpus header")
	}
	vectors := make(map[string][]byte, len(corpus.Vectors))
	for _, vector := range corpus.Vectors {
		if vector.TestInstance != 0 {
			t.Fatalf("%s test instance = %d", vector.Type, vector.TestInstance)
		}
		vectors[vector.Type] = decodeHex(t, vector.Hex)
	}
	if challenge, err := ParseServerChallenge(vectors["CephXServerChallenge"], defaultTestLimits()); err != nil || challenge != 1 {
		t.Fatalf("server challenge = %d, error = %v", challenge, err)
	}
	credential, err := ParseKey("client.test", base64.StdEncoding.EncodeToString(vectors["CryptoKey"]), 64)
	if err != nil || credential.Secret().Type() != CryptoAES || !bytes.Equal(credential.Secret().Bytes(), []byte("1234567890123456")) {
		t.Fatalf("crypto key fixture = %v, error = %v", credential, err)
	}
	ticketDecoder := wire.NewDecoder(vectors["CephXTicketBlob"], wire.Limits{MaxBytes: defaultTestLimits().MaxAuthBytes})
	ticket, err := decodeTicketBlob(ticketDecoder, defaultTestLimits())
	if err != nil || ticket.SecretID != 123 || string(ticket.Blob) != "this is a blob" || ticketDecoder.Finish() != nil {
		t.Fatalf("ticket fixture = %+v, error = %v", ticket, err)
	}
	response := wire.NewDecoder(vectors["CephXResponseHeader"], wire.Limits{MaxBytes: defaultTestLimits().MaxAuthBytes})
	if requestType, status := response.Uint16(), response.Int32(); requestType != 1 || status != 0 || response.Finish() != nil {
		t.Fatalf("response header fixture = type %d status %d", requestType, status)
	}
}

func TestP03CryptoFixtureParity(t *testing.T) {
	var corpus p03CryptoCorpus
	readP03JSON(t, "crypto-vectors.json", &corpus)
	if corpus.SchemaVersion != 1 {
		t.Fatal("invalid crypto corpus header")
	}
	cbc := corpus.Vectors.CephAESCBC
	cbcKey := cryptoKeyFromHex(t, CryptoAES, cbc.KeyHex)
	ciphertext, err := encryptCBC(cbcKey, decodeHex(t, cbc.PlaintextHex), defaultTestLimits())
	if err != nil || !bytes.Equal(ciphertext, decodeHex(t, cbc.CiphertextHex)) {
		t.Fatalf("Ceph AES-CBC fixture mismatch: %x, %v", ciphertext, err)
	}
	rfc := corpus.Vectors.RFC8009AES256
	rfcKey := cryptoKeyFromHex(t, CryptoAES256KRB5, rfc.KeyHex)
	plaintext, err := decryptPayload(rfcKey, decodeHex(t, rfc.CiphertextHex), rfc.KeyUsage, defaultTestLimits())
	if err != nil || !bytes.Equal(plaintext, decodeHex(t, rfc.PlaintextHex)) {
		t.Fatalf("RFC 8009 fixture mismatch: %x, %v", plaintext, err)
	}
	challenge := corpus.Vectors.AES256Challenge
	challengeKey := cryptoKeyFromHex(t, CryptoAES256KRB5, challenge.KeyHex)
	result, err := calcClientServerChallenge(challengeKey, challenge.ServerChallenge, challenge.ClientChallenge, defaultTestLimits())
	want := decodeHex(t, challenge.ResultHexLE)
	if err != nil || len(want) != 8 || result != uint64(want[0])|uint64(want[1])<<8|uint64(want[2])<<16|uint64(want[3])<<24|uint64(want[4])<<32|uint64(want[5])<<40|uint64(want[6])<<48|uint64(want[7])<<56 {
		t.Fatalf("AES256 challenge fixture = %#x, error = %v", result, err)
	}
}

func readP03JSON(t *testing.T, name string, target any) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "p03", name))
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		t.Fatal(err)
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

func cryptoKeyFromHex(t *testing.T, keyType uint16, value string) CryptoKey {
	t.Helper()
	decoded := decodeHex(t, value)
	key := CryptoKey{typeID: keyType, size: uint8(len(decoded))}
	copy(key.secret[:], decoded)
	return key
}
