package cephx

import (
	"bytes"
	"crypto/aes"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	wire "github.com/otuschhoff/rados-go/internal/encoding"
	"github.com/otuschhoff/rados-go/internal/protocol"
)

func mustSecretKey(t testing.TB, value string) CryptoKey {
	t.Helper()
	if len(value) != AESKeySize {
		t.Fatalf("invalid test key length: %d", len(value))
	}
	key := CryptoKey{typeID: CryptoAES, size: AESKeySize}
	copy(key.secret[:], []byte(value))
	return key
}

func mustAES256Key(t testing.TB, encoded string) CryptoKey {
	t.Helper()
	secret, err := hex.DecodeString(encoded)
	if err != nil || len(secret) != AES256KeySize {
		t.Fatalf("invalid AES256 test key: length=%d error=%v", len(secret), err)
	}
	key := CryptoKey{typeID: CryptoAES256KRB5, size: AES256KeySize}
	copy(key.secret[:], secret)
	return key
}

func defaultTestLimits() Limits {
	limits := DefaultLimits()
	limits.MaxAuthBytes = 1 << 16
	limits.MaxEncryptBytes = 1 << 16
	limits.MaxDecryptBytes = 1 << 16
	limits.MaxTicketBlobBytes = 1 << 16
	limits.MaxTickets = 16
	limits.MaxModes = 16
	return limits
}

func TestEncryptCBCVector(t *testing.T) {
	key := mustSecretKey(t, "1234567890123456")
	plaintext := []byte("abc")
	limits := defaultTestLimits()

	ciphertext, err := encryptCBC(key, plaintext, limits)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := hex.EncodeToString(ciphertext), "0ba3d1290cc47bb370aa355b24a7d152"; got != want {
		t.Fatalf("ciphertext = %s, want %s", got, want)
	}
	roundTrip, err := decryptCBC(key, ciphertext, limits)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(roundTrip, plaintext) {
		t.Fatalf("round trip = %x, want %x", roundTrip, plaintext)
	}
}

func TestDecryptCBCRejectsMalformedPadding(t *testing.T) {
	key := mustSecretKey(t, "1234567890123456")
	ciphertext, err := encryptCBC(key, make([]byte, aes.BlockSize), defaultTestLimits())
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		lastPad byte
	}{
		{name: "zero length", lastPad: 0},
		{name: "inconsistent bytes", lastPad: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			malformed := append([]byte(nil), ciphertext...)
			malformed[aes.BlockSize-1] ^= aes.BlockSize ^ test.lastPad
			if _, err := decryptCBC(key, malformed, defaultTestLimits()); !errors.Is(err, ErrMalformedPayload) {
				t.Fatalf("decryptCBC error = %v, want %v", err, ErrMalformedPayload)
			}
		})
	}
}

func TestAES256KRB5RFC8009Vector(t *testing.T) {
	key := mustAES256Key(t, "6d404d37faf79f9df0d33568d320669800eb4836472ea8a026d16b7182460c52")
	ciphertext, err := hex.DecodeString("4ed7b37c2bcac8f74f23c1cf07e62bc7b75fb3f637b9f559c7f664f69eab7b6092237526ea0d1f61cb20d69d10f2")
	if err != nil {
		t.Fatal(err)
	}
	plaintext, err := decryptPayload(key, ciphertext, 2, defaultTestLimits())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(plaintext, []byte{0, 1, 2, 3, 4, 5}) {
		t.Fatalf("plaintext = %x", plaintext)
	}
	if _, err := decryptPayload(key, ciphertext, 3, defaultTestLimits()); err == nil {
		t.Fatal("wrong key usage unexpectedly succeeded")
	}
}

func TestAES256KRB5ChallengeHash(t *testing.T) {
	key := mustAES256Key(t, "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
	got, err := calcClientServerChallenge(key, 0x1122334455667788, 0x0102030405060708, defaultTestLimits())
	if err != nil {
		t.Fatal(err)
	}
	if want := uint64(0xff603040e5eb4b04); got != want {
		t.Fatalf("challenge key = %#x, want %#x", got, want)
	}
}

func TestBuildInitialPayload(t *testing.T) {
	limits := defaultTestLimits()
	payload, err := BuildInitialPayload("client.admin", 42, limits)
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{
		AuthModeMon,
		byte(protocol.EntityClient), 0, 0, 0,
		5, 0, 0, 0, 'a', 'd', 'm', 'i', 'n',
		42, 0, 0, 0, 0, 0, 0, 0,
	}
	if !bytes.Equal(payload, want) {
		t.Fatalf("payload = %x, want %x", payload, want)
	}
}

func TestServerChallengeParseAndBuildChallengeRequest(t *testing.T) {
	limits := defaultTestLimits()
	challengePayload := []byte{1, 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11}
	serverChallenge, err := ParseServerChallenge(challengePayload, limits)
	if err != nil {
		t.Fatal(err)
	}
	if serverChallenge != 0x1122334455667788 {
		t.Fatalf("server challenge = %#x", serverChallenge)
	}

	credential, err := ParseKey("client.test", testEncodedKey, 64)
	if err != nil {
		t.Fatal(err)
	}
	request, err := BuildChallengeRequest(
		credential,
		serverChallenge,
		0x0102030405060708,
		TicketBlob{},
		uint32(protocol.EntityAuth|protocol.EntityMonitor),
		limits,
	)
	if err != nil {
		t.Fatal(err)
	}

	decoder := wire.NewDecoder(request, wire.Limits{MaxBytes: limits.MaxAuthBytes})
	if got := decoder.Uint16(); got != cephxGetAuthSessionKey {
		t.Fatalf("request type = %#x", got)
	}
	if got := decoder.Uint8(); got != 3 {
		t.Fatalf("authenticate version = %d", got)
	}
	if got := decoder.Uint64(); got != 0x0102030405060708 {
		t.Fatalf("client challenge = %#x", got)
	}
	wireKey := decoder.Uint64()
	if wireKey != 0x46eab36ae3203b77 {
		t.Fatalf("challenge key = %#x", wireKey)
	}
	ticketVersion := decoder.Uint8()
	secretID := decoder.Uint64()
	ticketBlob := decoder.Bytes()
	requestedKeys := decoder.Uint32()
	if err := decoder.Finish(); err != nil || decoder.Remaining() != 0 {
		t.Fatalf("decode error=%v remaining=%d", err, decoder.Remaining())
	}
	if ticketVersion != 1 || secretID != 0 || len(ticketBlob) != 0 || requestedKeys != uint32(protocol.EntityAuth|protocol.EntityMonitor) {
		t.Fatalf("ticket/requested mismatch")
	}
}

func encodeServiceTicketReply(
	t *testing.T,
	decryptKey CryptoKey,
	entries []ServiceTicket,
	validity time.Duration,
	limits Limits,
) []byte {
	t.Helper()
	encoder := wire.NewEncoder(limits.MaxAuthBytes)
	encoder.Uint8(1)
	encoder.Uint32(uint32(len(entries)))
	for _, entry := range entries {
		encoder.Uint32(entry.ServiceID)
		encoder.Uint8(1)

		msgA := wire.NewEncoder(limits.MaxAuthBytes)
		msgA.Uint8(1)
		msgA.Uint16(entry.SessionKey.typeID)
		msgA.Uint32(0)
		msgA.Uint32(0)
		msgA.Uint16(uint16(entry.SessionKey.size))
		msgA.Raw(entry.SessionKey.secret[:entry.SessionKey.size])
		msgA.Uint32(uint32(validity / time.Second))
		msgA.Uint32(uint32(validity % time.Second))
		msgABytes, err := msgA.BytesResult()
		if err != nil {
			t.Fatal(err)
		}
		encryptedMsgA, err := encodeEncryptEnvelopeUsage(decryptKey, msgABytes, keyUsageTicketSessionKey, limits)
		if err != nil {
			t.Fatal(err)
		}
		encoder.Raw(encryptedMsgA)

		encoder.Uint8(0)
		ticketEncoder := wire.NewEncoder(limits.MaxAuthBytes)
		encodeTicketBlob(ticketEncoder, entry.Ticket)
		ticketBytes, err := ticketEncoder.BytesResult()
		if err != nil {
			t.Fatal(err)
		}
		encoder.Bytes(ticketBytes)
	}
	out, err := encoder.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestParseAuthSessionReplySecureWithExtraTickets(t *testing.T) {
	limits := defaultTestLimits()
	now := time.Unix(1000, 0).UTC()
	principalSecret := mustSecretKey(t, "1234567890123456")
	authSessionKey := mustSecretKey(t, "abcdefghijklmnop")
	monSessionKey := mustSecretKey(t, "QRSTUVWXabcdefgh")

	authTicket := ServiceTicket{
		ServiceID:  uint32(protocol.EntityAuth),
		SessionKey: authSessionKey,
		Ticket:     TicketBlob{SecretID: 7, Blob: []byte{1, 2, 3}},
	}
	monTicket := ServiceTicket{
		ServiceID:  uint32(protocol.EntityMonitor),
		SessionKey: monSessionKey,
		Ticket:     TicketBlob{SecretID: 8, Blob: []byte{4, 5}},
	}
	mainReply := encodeServiceTicketReply(t, principalSecret, []ServiceTicket{authTicket}, 60*time.Second, limits)
	extraReply := encodeServiceTicketReply(t, authSessionKey, []ServiceTicket{monTicket}, 60*time.Second, limits)

	connectionSecret := bytes.Repeat([]byte{0x5a}, ConnectionSecretSizeSecure)
	connectionPlain := wire.NewEncoder(limits.MaxAuthBytes)
	connectionPlain.Bytes(connectionSecret)
	connectionPlainBytes, err := connectionPlain.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	connectionBlob, err := encodeEncryptEnvelope(authSessionKey, connectionPlainBytes, limits)
	if err != nil {
		t.Fatal(err)
	}

	payload := wire.NewEncoder(limits.MaxAuthBytes)
	payload.Uint16(cephxGetAuthSessionKey)
	payload.Int32(0)
	payload.Raw(mainReply)
	payload.Bytes(connectionBlob)
	payload.Bytes(extraReply)
	payloadBytes, err := payload.BytesResult()
	if err != nil {
		t.Fatal(err)
	}

	parsed, err := ParseAuthSessionReply(payloadBytes, principalSecret, nil, ConModeSecure, now, limits)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.RequestType != cephxGetAuthSessionKey {
		t.Fatalf("request type = %#x", parsed.RequestType)
	}
	if parsed.AuthSessionKey != authSessionKey {
		t.Fatal("auth session key mismatch")
	}
	if !bytes.Equal(parsed.ConnectionSecret, connectionSecret) {
		t.Fatal("connection secret mismatch")
	}
	if len(parsed.Tickets) != 2 {
		t.Fatalf("ticket count = %d", len(parsed.Tickets))
	}
	if got := parsed.Tickets[uint32(protocol.EntityMonitor)].SessionKey; got != monSessionKey {
		t.Fatal("monitor ticket session key mismatch")
	}
}

func TestParseAuthSessionReplyRejectsInvalidExtraTicketSet(t *testing.T) {
	limits := defaultTestLimits()
	now := time.Unix(1000, 0).UTC()
	principalSecret := mustSecretKey(t, "1234567890123456")
	authSessionKey := mustSecretKey(t, "abcdefghijklmnop")
	authTicket := ServiceTicket{ServiceID: uint32(protocol.EntityAuth), SessionKey: authSessionKey, Ticket: TicketBlob{SecretID: 7, Blob: []byte{1}}}
	monitorTicket := ServiceTicket{ServiceID: uint32(protocol.EntityMonitor), SessionKey: mustSecretKey(t, "QRSTUVWXabcdefgh"), Ticket: TicketBlob{SecretID: 8, Blob: []byte{2}}}
	mainReply := encodeServiceTicketReply(t, principalSecret, []ServiceTicket{authTicket}, time.Minute, limits)
	connectionPlain := wire.NewEncoder(limits.MaxAuthBytes)
	connectionPlain.Bytes(bytes.Repeat([]byte{0x5a}, ConnectionSecretSizeSecure))
	connectionBytes, err := connectionPlain.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	connectionBlob, err := encodeEncryptEnvelope(authSessionKey, connectionBytes, limits)
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		name        string
		extra       ServiceTicket
		maxTickets  uint32
		expectedErr error
	}{
		{name: "duplicate service", extra: authTicket, maxTickets: limits.MaxTickets, expectedErr: ErrMalformedPayload},
		{name: "aggregate limit", extra: monitorTicket, maxTickets: 1, expectedErr: wire.ErrLimitExceeded},
	} {
		t.Run(test.name, func(t *testing.T) {
			testLimits := limits
			testLimits.MaxTickets = test.maxTickets
			extraReply := encodeServiceTicketReply(t, authSessionKey, []ServiceTicket{test.extra}, time.Minute, testLimits)
			payload := wire.NewEncoder(testLimits.MaxAuthBytes)
			payload.Uint16(cephxGetAuthSessionKey)
			payload.Int32(0)
			payload.Raw(mainReply)
			payload.Bytes(connectionBlob)
			payload.Bytes(extraReply)
			payloadBytes, encodeErr := payload.BytesResult()
			if encodeErr != nil {
				t.Fatal(encodeErr)
			}
			if _, parseErr := ParseAuthSessionReply(payloadBytes, principalSecret, nil, ConModeSecure, now, testLimits); !errors.Is(parseErr, test.expectedErr) {
				t.Fatalf("error = %v, want %v", parseErr, test.expectedErr)
			}
		})
	}
}

func TestAES256AuthSessionAndAuthorizerKeyUsages(t *testing.T) {
	limits := defaultTestLimits()
	now := time.Unix(3000, 0).UTC()
	principal := mustAES256Key(t, "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
	authKey := mustAES256Key(t, "202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f")
	monitorKey := mustAES256Key(t, "404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f")
	authTicket := ServiceTicket{ServiceID: uint32(protocol.EntityAuth), SessionKey: authKey, Ticket: TicketBlob{SecretID: 7, Blob: []byte("auth-ticket")}}
	monitorTicket := ServiceTicket{ServiceID: uint32(protocol.EntityMonitor), SessionKey: monitorKey, Ticket: TicketBlob{SecretID: 8, Blob: []byte("monitor-ticket")}}
	mainReply := encodeServiceTicketReply(t, principal, []ServiceTicket{authTicket}, time.Minute, limits)
	extraReply := encodeServiceTicketReply(t, authKey, []ServiceTicket{monitorTicket}, time.Minute, limits)
	connectionSecret := bytes.Repeat([]byte{0x42}, ConnectionSecretSizeSecure)
	connectionPlain := wire.NewEncoder(limits.MaxAuthBytes)
	connectionPlain.Bytes(connectionSecret)
	connectionBytes, _ := connectionPlain.BytesResult()
	connectionBlob, err := encodeEncryptEnvelopeUsage(authKey, connectionBytes, keyUsageAuthConnectionSecret, limits)
	if err != nil {
		t.Fatal(err)
	}
	payload := wire.NewEncoder(limits.MaxAuthBytes)
	payload.Uint16(cephxGetAuthSessionKey)
	payload.Int32(0)
	payload.Raw(mainReply)
	payload.Bytes(connectionBlob)
	payload.Bytes(extraReply)
	payloadBytes, _ := payload.BytesResult()
	reply, err := ParseAuthSessionReply(payloadBytes, principal, nil, ConModeSecure, now, limits)
	if err != nil || reply.AuthSessionKey != authKey || !bytes.Equal(reply.ConnectionSecret, connectionSecret) || reply.Tickets[uint32(protocol.EntityMonitor)].SessionKey != monitorKey {
		t.Fatalf("AES256 auth reply = %+v, error = %v", reply, err)
	}

	monitorTicket.ExpiresAt = now.Add(time.Minute)
	authorizer, err := BuildAuthorizer(uint32(protocol.EntityMonitor), 77, monitorTicket, now, bytes.NewReader([]byte{1, 2, 3, 4, 5, 6, 7, 8}), limits)
	if err != nil {
		t.Fatal(err)
	}
	challengePlain := wire.NewEncoder(limits.MaxAuthBytes)
	challengePlain.Uint8(1)
	challengePlain.Uint64(9)
	challengeBytes, _ := challengePlain.BytesResult()
	challenge, err := encryptWithMagicUsage(monitorKey, challengeBytes, keyUsageAuthorizeChallenge, limits)
	if err != nil {
		t.Fatal(err)
	}
	updated, err := AddAuthorizerChallenge(authorizer, challenge, monitorKey, limits)
	if err != nil {
		t.Fatal(err)
	}
	wrongChallenge, err := encryptWithMagicUsage(monitorKey, challengeBytes, keyUsageAuthorizeReply, limits)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := AddAuthorizerChallenge(authorizer, wrongChallenge, monitorKey, limits); err == nil {
		t.Fatal("wrong challenge key usage unexpectedly succeeded")
	}
	tail := wire.NewDecoder(updated.Payload[len(updated.Base):], wire.Limits{MaxBytes: limits.MaxAuthBytes})
	if _, err := decodeDecryptEnvelopeUsage(monitorKey, tail, keyUsageAuthorize, limits); err != nil {
		t.Fatal(err)
	}
	replyPlain := wire.NewEncoder(limits.MaxAuthBytes)
	replyPlain.Uint8(2)
	replyPlain.Uint64(authorizer.Nonce + 1)
	replyPlain.Bytes(connectionSecret)
	replyPlainBytes, _ := replyPlain.BytesResult()
	replyPayload := mustEncryptUsage(t, monitorKey, replyPlainBytes, keyUsageAuthorizeReply, limits)
	if secret, err := VerifyAuthorizerReply(replyPayload, monitorKey, authorizer.Nonce, limits); err != nil || !bytes.Equal(secret, connectionSecret) {
		t.Fatalf("AES256 authorizer reply secret = %x, error = %v", secret, err)
	}
}

func mustEncryptUsage(t *testing.T, key CryptoKey, plaintext []byte, usage uint32, limits Limits) []byte {
	t.Helper()
	payload, err := encodeEncryptEnvelopeUsage(key, plaintext, usage, limits)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func TestParseRenewedAuthTicketEncryptedWithPreviousSessionKey(t *testing.T) {
	t.Run("AES128", func(t *testing.T) {
		testRenewedAuthTicket(t,
			mustSecretKey(t, "1234567890123456"),
			mustSecretKey(t, "old-auth-key-123"),
			mustSecretKey(t, "new-auth-key-123"),
		)
	})
	t.Run("AES256KRB5", func(t *testing.T) {
		testRenewedAuthTicket(t,
			mustAES256Key(t, "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"),
			mustAES256Key(t, "202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f"),
			mustAES256Key(t, "404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f"),
		)
	})
}

func testRenewedAuthTicket(t *testing.T, principalSecret, oldAuthKey, newAuthKey CryptoKey) {
	t.Helper()
	limits := defaultTestLimits()
	now := time.Unix(1000, 0).UTC()
	ticket := ServiceTicket{ServiceID: uint32(protocol.EntityAuth), SessionKey: newAuthKey, Ticket: TicketBlob{SecretID: 9, Blob: []byte("renewed")}}

	encoder := wire.NewEncoder(limits.MaxAuthBytes)
	encoder.Uint16(cephxGetAuthSessionKey)
	encoder.Int32(0)
	encoder.Uint8(1)
	encoder.Uint32(1)
	encoder.Uint32(ticket.ServiceID)
	encoder.Uint8(1)
	msgA := wire.NewEncoder(limits.MaxAuthBytes)
	msgA.Uint8(1)
	msgA.Uint16(newAuthKey.typeID)
	msgA.Uint32(0)
	msgA.Uint32(0)
	msgA.Uint16(uint16(newAuthKey.size))
	msgA.Raw(newAuthKey.secret[:newAuthKey.size])
	msgA.Uint32(60)
	msgA.Uint32(0)
	msgABytes, _ := msgA.BytesResult()
	encryptedMsgA, err := encodeEncryptEnvelopeUsage(principalSecret, msgABytes, keyUsageTicketSessionKey, limits)
	if err != nil {
		t.Fatal(err)
	}
	encoder.Raw(encryptedMsgA)
	encoder.Uint8(1)
	ticketEncoder := wire.NewEncoder(limits.MaxAuthBytes)
	encodeTicketBlob(ticketEncoder, ticket.Ticket)
	ticketBytes, _ := ticketEncoder.BytesResult()
	ticketWrapper := wire.NewEncoder(limits.MaxAuthBytes)
	ticketWrapper.Bytes(ticketBytes)
	wrapperBytes, _ := ticketWrapper.BytesResult()
	encryptedTicket, err := encodeEncryptEnvelopeUsage(oldAuthKey, wrapperBytes, keyUsageTicketBlob, limits)
	if err != nil {
		t.Fatal(err)
	}
	encoder.Raw(encryptedTicket)
	payload, _ := encoder.BytesResult()

	reply, err := ParseAuthSessionReply(payload, principalSecret, &oldAuthKey, ConModeCRC, now, limits)
	if err != nil {
		t.Fatal(err)
	}
	if reply.AuthSessionKey != newAuthKey || string(reply.Tickets[ticket.ServiceID].Ticket.Blob) != "renewed" {
		t.Fatal("renewed auth ticket mismatch")
	}
	if _, err := ParseAuthSessionReply(payload, principalSecret, nil, ConModeCRC, now, limits); !errors.Is(err, ErrMissingTicket) {
		t.Fatalf("missing previous key error = %v", err)
	}
}

func TestParseAuthSessionReplyRejectsNonAuthSessionType(t *testing.T) {
	limits := defaultTestLimits()
	payload := wire.NewEncoder(limits.MaxAuthBytes)
	payload.Uint16(cephxGetPrincipalSessionKey)
	payload.Int32(0)
	payloadBytes, err := payload.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	principalSecret := mustSecretKey(t, "1234567890123456")
	if _, err := ParseAuthSessionReply(payloadBytes, principalSecret, nil, ConModeCRC, time.Unix(0, 0), limits); !errors.Is(err, ErrUnsupportedType) {
		t.Fatalf("error = %v", err)
	}
}

func TestBuildAuthorizerChallengeAndVerifyReply(t *testing.T) {
	limits := defaultTestLimits()
	now := time.Unix(2000, 0).UTC()
	ticket := ServiceTicket{
		ServiceID:  uint32(protocol.EntityMonitor),
		SessionKey: mustSecretKey(t, "abcdefghijklmnop"),
		Ticket:     TicketBlob{SecretID: 1, Blob: []byte{0xaa, 0xbb}},
		ExpiresAt:  now.Add(time.Minute),
	}
	authorizer, err := BuildAuthorizer(
		uint32(protocol.EntityMonitor),
		123,
		ticket,
		now,
		bytes.NewReader([]byte{1, 2, 3, 4, 5, 6, 7, 8}),
		limits,
	)
	if err != nil {
		t.Fatal(err)
	}
	if authorizer.Nonce != 0x0807060504030201 {
		t.Fatalf("nonce = %#x", authorizer.Nonce)
	}

	challengePlain := wire.NewEncoder(limits.MaxAuthBytes)
	challengePlain.Uint8(1)
	challengePlain.Uint64(0x10)
	challengeBytes, err := challengePlain.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	challengeEnc, err := encryptWithMagic(ticket.SessionKey, challengeBytes, limits)
	if err != nil {
		t.Fatal(err)
	}
	updated, err := AddAuthorizerChallenge(authorizer, challengeEnc, ticket.SessionKey, limits)
	if err != nil {
		t.Fatal(err)
	}

	trailing := updated.Payload[len(updated.Base):]
	tailDecoder := wire.NewDecoder(trailing, wire.Limits{MaxBytes: limits.MaxAuthBytes})
	authorizeBody, err := decodeDecryptEnvelope(ticket.SessionKey, tailDecoder, limits)
	if err != nil {
		t.Fatal(err)
	}
	authorizeDecoder := wire.NewDecoder(authorizeBody, wire.Limits{MaxBytes: limits.MaxAuthBytes})
	if version := authorizeDecoder.Uint8(); version != 2 {
		t.Fatalf("authorize version = %d", version)
	}
	if got := authorizeDecoder.Uint64(); got != authorizer.Nonce {
		t.Fatalf("nonce = %#x", got)
	}
	if !authorizeDecoder.Bool() {
		t.Fatal("missing challenge bit")
	}
	if plusOne := authorizeDecoder.Uint64(); plusOne != 0x11 {
		t.Fatalf("challenge+1 = %#x", plusOne)
	}

	connectionSecret := bytes.Repeat([]byte{0x42}, ConnectionSecretSizeSecure)
	replyPlain := wire.NewEncoder(limits.MaxAuthBytes)
	replyPlain.Uint8(2)
	replyPlain.Uint64(authorizer.Nonce + 1)
	replyPlain.Bytes(connectionSecret)
	replyPlainBytes, err := replyPlain.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	replyPayload, err := encodeEncryptEnvelope(ticket.SessionKey, replyPlainBytes, limits)
	if err != nil {
		t.Fatal(err)
	}
	secret, err := VerifyAuthorizerReply(replyPayload, ticket.SessionKey, authorizer.Nonce, limits)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(secret, connectionSecret) {
		t.Fatal("reply secret mismatch")
	}
}

func TestTranscriptSignature(t *testing.T) {
	key := mustSecretKey(t, "abcdefghijklmnop")
	sig := TranscriptSignature(key, []byte("transcript"))
	if !VerifyTranscriptSignature(key, []byte("transcript"), sig) {
		t.Fatal("signature verify failed")
	}
	if VerifyTranscriptSignature(key, []byte("changed"), sig) {
		t.Fatal("signature verify unexpectedly succeeded")
	}
}

func TestDecodeAuthBadMethod(t *testing.T) {
	limits := defaultTestLimits()
	encoder := wire.NewEncoder(limits.MaxAuthBytes)
	encoder.Uint32(AuthMethodCephX)
	encoder.Int32(-13)
	encoder.Uint32(2)
	encoder.Uint32(AuthMethodCephX)
	encoder.Uint32(9)
	encoder.Uint32(2)
	encoder.Uint32(ConModeSecure)
	encoder.Uint32(ConModeCRC)
	payload, err := encoder.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	method, result, methods, modes, err := DecodeAuthBadMethod(payload, limits)
	if err != nil {
		t.Fatal(err)
	}
	if method != AuthMethodCephX || result != -13 {
		t.Fatalf("method/result = %d/%d", method, result)
	}
	if len(methods) != 2 || methods[0] != AuthMethodCephX {
		t.Fatalf("methods = %v", methods)
	}
	if len(modes) != 2 || modes[0] != ConModeSecure {
		t.Fatalf("modes = %v", modes)
	}
}

func TestRejectsExpiredTicketAndMalformedCiphertext(t *testing.T) {
	limits := defaultTestLimits()
	now := time.Unix(10, 0).UTC()
	ticket := ServiceTicket{
		ServiceID:  uint32(protocol.EntityMonitor),
		SessionKey: mustSecretKey(t, "abcdefghijklmnop"),
		ExpiresAt:  now,
	}
	_, err := BuildAuthorizer(uint32(protocol.EntityMonitor), 1, ticket, now, nil, limits)
	if !errors.Is(err, ErrExpiredTicket) {
		t.Fatalf("expired error = %v", err)
	}
	ticket.ExpiresAt = time.Time{}
	_, err = BuildAuthorizer(uint32(protocol.EntityMonitor), 1, ticket, now, nil, limits)
	if !errors.Is(err, ErrExpiredTicket) {
		t.Fatalf("missing expiry error = %v", err)
	}
	if _, err := decryptCBC(ticket.SessionKey, []byte{1, 2, 3, 4}, limits); err == nil {
		t.Fatal("expected decrypt error")
	}
	plaintextEncoder := wire.NewEncoder(limits.MaxConnectionSecretBytes)
	plaintextEncoder.Bytes([]byte("payload"))
	plaintext, err := plaintextEncoder.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := encodeEncryptEnvelope(ticket.SessionKey, plaintext, limits)
	if err != nil {
		t.Fatal(err)
	}
	envelope = append(envelope, 0)
	if _, err := decryptStringEnvelope(ticket.SessionKey, envelope, 0, limits); !errors.Is(err, ErrMalformedPayload) {
		t.Fatalf("trailing envelope error = %v", err)
	}
}
