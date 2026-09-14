package cephx

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

const testEncodedKey = "AQB7AAAAyAEAABAAMTIzNDU2Nzg5MDEyMzQ1Ng=="

const testEncodedAES256Key = "AgBm8qdqnvU7HiAAg6prN8XJ47FG9AprWpB72EwKyLfFC7UgnMYvcnFI29M="

func TestParseKey(t *testing.T) {
	credential, err := ParseKey("client.test", testEncodedKey, 64)
	if err != nil {
		t.Fatal(err)
	}
	if credential.Entity() != "client.test" || !credential.Created().Equal(time.Unix(123, 456).UTC()) {
		t.Fatalf("credential metadata = %q %v", credential.Entity(), credential.Created())
	}
	secret := credential.Secret()
	if secret.Type() != CryptoAES || string(secret.Bytes()) != "1234567890123456" {
		t.Fatalf("secret type/size = %d/%d", secret.Type(), len(secret.Bytes()))
	}
	if got := string(secret.Bytes()); got != "1234567890123456" {
		t.Fatalf("secret = %q", got)
	}
	if strings.Contains(credential.String(), testEncodedKey) || strings.Contains(credential.String(), "123456") {
		t.Fatalf("credential string exposes secret: %q", credential.String())
	}
}

func TestSecretBearingValuesRedactFormatting(t *testing.T) {
	credential, err := ParseKey("client.test", testEncodedKey, 64)
	if err != nil {
		t.Fatal(err)
	}
	secret := credential.Secret()
	values := []any{
		credential,
		secret,
		TicketBlob{SecretID: 7, Blob: []byte("ticket-secret")},
		ServiceTicket{ServiceID: 1, SessionKey: secret, Ticket: TicketBlob{SecretID: 7, Blob: []byte("ticket-secret")}},
		AuthSessionReply{AuthSessionKey: secret, ConnectionSecret: []byte("connection-secret"), Tickets: map[uint32]ServiceTicket{1: {SessionKey: secret}}},
		Authorizer{Base: []byte("base-secret"), Payload: []byte("payload-secret"), Nonce: 123, ServiceID: 1},
	}
	for _, value := range values {
		for _, formatted := range []string{fmt.Sprintf("%v", value), fmt.Sprintf("%+v", value), fmt.Sprintf("%#v", value)} {
			for _, forbidden := range []string{"1234567890123456", "ticket-secret", "connection-secret", "base-secret", "payload-secret"} {
				if strings.Contains(formatted, forbidden) {
					t.Fatalf("%T formatting exposed %q: %s", value, forbidden, formatted)
				}
			}
		}
	}
}

func TestParseAES256KRB5Key(t *testing.T) {
	credential, err := ParseKey("client.p03", testEncodedAES256Key, 64)
	if err != nil {
		t.Fatal(err)
	}
	secret := credential.Secret()
	if secret.Type() != CryptoAES256KRB5 || len(secret.Bytes()) != AES256KeySize {
		t.Fatalf("secret type/size = %d/%d", secret.Type(), len(secret.Bytes()))
	}
}

func TestParseKeyRejectsMalformedValues(t *testing.T) {
	valid, err := base64.StdEncoding.DecodeString(testEncodedKey)
	if err != nil {
		t.Fatal(err)
	}
	longAES := append([]byte(nil), valid...)
	longAES[10] = AESKeySize + 1
	longAES = append(longAES, 0)
	validAES256, err := base64.StdEncoding.DecodeString(testEncodedAES256Key)
	if err != nil {
		t.Fatal(err)
	}
	longAES256 := append([]byte(nil), validAES256...)
	longAES256[10] = AES256KeySize + 1
	longAES256 = append(longAES256, 0)
	tests := []struct {
		name   string
		entity string
		data   string
	}{
		{name: "entity", entity: "osd.1", data: testEncodedKey},
		{name: "base64", entity: "client.test", data: "%%%"},
		{name: "trailing", entity: "client.test", data: base64.StdEncoding.EncodeToString(append(valid, 0))},
		{name: "short secret", entity: "client.test", data: base64.StdEncoding.EncodeToString(append(valid[:10], 15, 0))},
		{name: "long AES secret", entity: "client.test", data: base64.StdEncoding.EncodeToString(longAES)},
		{name: "long AES256 secret", entity: "client.test", data: base64.StdEncoding.EncodeToString(longAES256)},
		{name: "oversized text", entity: "client.test", data: strings.Repeat("A", 130)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ParseKey(test.entity, test.data, 64); !errors.Is(err, ErrInvalidCredential) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestParseKeyring(t *testing.T) {
	data := []byte("# generated\n[client.other]\n key = bad\n\n[client.test]\n key = '" + testEncodedKey + "' ; comment\n caps mon = allow r\n caps_osd = allow rw pool=test\n auid = 0\n")
	credential, err := ParseKeyring(data, "client.test", 4096)
	if err != nil {
		t.Fatal(err)
	}
	secret := credential.Secret()
	if credential.Entity() != "client.test" || string(secret.Bytes()) != "1234567890123456" {
		t.Fatalf("credential = %v", credential)
	}
}

func TestParseKeyringRejectsUnsupportedInput(t *testing.T) {
	tests := []struct {
		name string
		data string
		err  error
	}{
		{name: "missing", data: "[client.other]\nkey = " + testEncodedKey + "\n", err: ErrCredentialNotFound},
		{name: "property", data: "[client.test]\nunknown = value\n", err: ErrInvalidKeyring},
		{name: "duplicate key", data: "[client.test]\nkey = x\nkey = y\n", err: ErrInvalidKeyring},
		{name: "duplicate entity", data: "[client.test]\nkey = x\n[client.test]\nkey = y\n", err: ErrInvalidKeyring},
		{name: "outside section", data: "key = x\n", err: ErrInvalidKeyring},
		{name: "oversized", data: strings.Repeat("x", 65), err: ErrInvalidKeyring},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ParseKeyring([]byte(test.data), "client.test", 64); !errors.Is(err, test.err) {
				t.Fatalf("error = %v, want %v", err, test.err)
			}
		})
	}
}
