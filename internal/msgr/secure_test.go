package msgr

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"testing"
)

func TestSecureDeterministicVector(t *testing.T) {
	secret := testSecureSecret()
	codec := mustSecureCodec(t, secret, false)
	wire, err := codec.Encode(Frame{Tag: TagAck, Segments: []Segment{{Alignment: DefaultAlignment, Data: []byte("ceph")}}}, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	const expected = "4de81a477793cc7cbb381480b30cc69431dabdf879b50b483d403cfd1ce7066a25a34695d34ff7965036a3b4f02908ecd7f8cc4f2688a28ab01f761aeee6fdba01654a9d4a848c689172d4521fcf81081e2681d78b52b306bad9cd41eee13858"
	if got := hex.EncodeToString(wire); got != expected {
		t.Fatalf("secure vector = %s", got)
	}
}

func TestSecureRoundTripCrossedDirections(t *testing.T) {
	secret := testSecureSecret()
	client := mustSecureCodec(t, secret, false)
	server := mustSecureCodec(t, secret, true)
	frame := Frame{Tag: TagMessage, Segments: []Segment{
		{Alignment: DefaultAlignment, Data: bytes.Repeat([]byte{0x5a}, 63)},
		{Alignment: PageAlignment},
		{Alignment: DefaultAlignment, Data: []byte("middle")},
	}}
	wire, err := client.Encode(frame, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(wire), 96+16+16+16+16+16; got != want {
		t.Fatalf("wire length = %d, want %d", got, want)
	}
	decoded, err := server.Read(&oneByteReader{data: wire}, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	assertFrameEqual(t, decoded, frame)

	reply := Frame{Tag: TagKeepalive2Ack, Segments: []Segment{{Alignment: DefaultAlignment, Data: []byte("reply")}}}
	wire, err = server.Encode(reply, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err = client.Read(bytes.NewReader(wire), testLimits)
	if err != nil {
		t.Fatal(err)
	}
	assertFrameEqual(t, decoded, reply)
}

func TestSecureRejectsCorruptAuthenticationTags(t *testing.T) {
	secret := testSecureSecret()
	frame := Frame{Tag: TagMessage, Segments: []Segment{
		{Alignment: DefaultAlignment, Data: bytes.Repeat([]byte{1}, 49)},
		{Alignment: DefaultAlignment, Data: []byte("tail")},
	}}
	wire, err := mustSecureCodec(t, secret, false).Encode(frame, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	for _, offset := range []int{securePreamble - 1, securePreamble + 31, len(wire) - 1} {
		corrupt := append([]byte(nil), wire...)
		corrupt[offset] ^= 0x80
		if _, err := mustSecureCodec(t, secret, true).Read(bytes.NewReader(corrupt), testLimits); !errors.Is(err, ErrIntegrity) {
			t.Fatalf("corruption at %d: error = %v", offset, err)
		}
	}
}

func TestSecureAuthenticationFailureConsumesRXNonce(t *testing.T) {
	secret := testSecureSecret()
	binary.LittleEndian.PutUint64(secret[32:40], math.MaxUint64)
	frame := Frame{Tag: TagAck, Segments: []Segment{{Alignment: DefaultAlignment}}}
	wire, err := mustSecureCodec(t, secret, false).Encode(frame, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	wire[len(wire)-1] ^= 0x80
	server := mustSecureCodec(t, secret, true)
	if _, err := server.Read(bytes.NewReader(wire), testLimits); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("authentication error = %v", err)
	}
	if _, err := server.Read(bytes.NewReader(nil), testLimits); !errors.Is(err, ErrCounterExhausted) {
		t.Fatalf("post-failure exhaustion error = %v", err)
	}
}

func TestSecureRejectsAuthenticatedMalformedFields(t *testing.T) {
	secret := testSecureSecret()
	tests := []struct {
		name string
		wire func(*testing.T) []byte
	}{
		{
			name: "tag 21",
			wire: func(t *testing.T) []byte {
				return sealTestPreamble(t, secret, TagCompressionRequest, []segmentDescriptor{{length: 0, alignment: DefaultAlignment}}, nil)
			},
		},
		{
			name: "tag 22",
			wire: func(t *testing.T) []byte {
				return sealTestPreamble(t, secret, TagCompressionDone, []segmentDescriptor{{length: 0, alignment: DefaultAlignment}}, nil)
			},
		},
		{
			name: "invalid alignment",
			wire: func(t *testing.T) []byte {
				return sealTestPreamble(t, secret, TagAck, []segmentDescriptor{{length: 0, alignment: 3}}, nil)
			},
		},
		{
			name: "inline padding",
			wire: func(t *testing.T) []byte {
				return sealTestPreamble(t, secret, TagAck, []segmentDescriptor{{length: 1, alignment: DefaultAlignment}}, func(plain []byte) {
					plain[PreambleSize+1] = 1
				})
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := mustSecureCodec(t, secret, true).Read(bytes.NewReader(test.wire(t)), testLimits); !errors.Is(err, ErrMalformed) {
				t.Fatalf("error = %v", err)
			}
		})
	}

	for _, status := range []byte{0x02, LateStatusComplete} {
		name := "invalid status"
		if status == LateStatusComplete {
			name = "nonzero epilogue padding"
		}
		t.Run(name, func(t *testing.T) {
			wire := sealTestMultiSegment(t, secret, status, status == LateStatusComplete)
			if _, err := mustSecureCodec(t, secret, true).Read(bytes.NewReader(wire), testLimits); !errors.Is(err, ErrMalformed) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestSecureChecksLimitsBeforeVariablePayload(t *testing.T) {
	secret := testSecureSecret()
	descriptors := []segmentDescriptor{{length: 2048, alignment: DefaultAlignment}}
	wire := sealTestPreamble(t, secret, TagMessage, descriptors, nil)
	if _, err := mustSecureCodec(t, secret, true).Read(bytes.NewReader(wire), Limits{MaxSegmentBytes: 1024, MaxFrameBytes: 8192}); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("segment limit error = %v", err)
	}
	if _, err := mustSecureCodec(t, secret, true).Read(bytes.NewReader(wire), Limits{MaxSegmentBytes: 4096, MaxFrameBytes: 100}); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("frame limit error = %v", err)
	}
}

func TestSecureTXAndRXCounterExhaustion(t *testing.T) {
	secret := testSecureSecret()
	binary.LittleEndian.PutUint64(secret[32:40], math.MaxUint64-1)
	client := mustSecureCodec(t, secret, false)
	server := mustSecureCodec(t, secret, true)
	frame := Frame{Tag: TagAck, Segments: []Segment{{Alignment: DefaultAlignment}}}
	for range 2 {
		wire, err := client.Encode(frame, testLimits)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := server.Read(bytes.NewReader(wire), testLimits); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := client.Encode(frame, testLimits); !errors.Is(err, ErrCounterExhausted) {
		t.Fatalf("TX exhaustion error = %v", err)
	}
	if _, err := server.Read(bytes.NewReader(nil), testLimits); !errors.Is(err, ErrCounterExhausted) {
		t.Fatalf("RX exhaustion error = %v", err)
	}

	client = mustSecureCodec(t, secret, false)
	multiRecord := Frame{Tag: TagMessage, Segments: []Segment{
		{Alignment: DefaultAlignment, Data: bytes.Repeat([]byte{1}, 49)},
		{Alignment: DefaultAlignment, Data: []byte{2}},
	}}
	if _, err := client.Encode(multiRecord, testLimits); !errors.Is(err, ErrCounterExhausted) {
		t.Fatalf("frame preflight error = %v", err)
	}
	if _, err := client.Encode(frame, testLimits); err != nil {
		t.Fatalf("failed preflight consumed counter: %v", err)
	}
}

func testSecureSecret() []byte {
	secret := make([]byte, secureSecretSize)
	for index := range secret {
		secret[index] = byte(index)
	}
	return secret
}

func mustSecureCodec(t *testing.T, secret []byte, server bool) *SecureCodec {
	t.Helper()
	codec, err := NewSecureCodec(secret, server)
	if err != nil {
		t.Fatal(err)
	}
	return codec
}

func assertFrameEqual(t *testing.T, got, want Frame) {
	t.Helper()
	if got.Tag != want.Tag || len(got.Segments) != len(want.Segments) {
		t.Fatalf("frame = %#v, want %#v", got, want)
	}
	for index := range want.Segments {
		if got.Segments[index].Alignment != want.Segments[index].Alignment || !bytes.Equal(got.Segments[index].Data, want.Segments[index].Data) {
			t.Fatalf("segment %d = %#v, want %#v", index, got.Segments[index], want.Segments[index])
		}
	}
}

func sealTestPreamble(t *testing.T, secret []byte, tag Tag, descriptors []segmentDescriptor, mutate func([]byte)) []byte {
	t.Helper()
	codec := mustSecureCodec(t, secret, false)
	preamble := encodePreamble(tag, descriptors)
	plaintext := make([]byte, PreambleSize+secureInlineSize)
	copy(plaintext, preamble[:])
	if mutate != nil {
		mutate(plaintext)
	}
	wire, err := codec.tx.sealLocked(plaintext)
	if err != nil {
		t.Fatal(err)
	}
	return wire
}

func sealTestMultiSegment(t *testing.T, secret []byte, status byte, nonzeroPadding bool) []byte {
	t.Helper()
	codec := mustSecureCodec(t, secret, false)
	descriptors := []segmentDescriptor{
		{length: 0, alignment: DefaultAlignment},
		{length: 1, alignment: DefaultAlignment},
	}
	preamble := encodePreamble(TagMessage, descriptors)
	first := make([]byte, PreambleSize+secureInlineSize)
	copy(first, preamble[:])
	wire, err := codec.tx.sealLocked(first)
	if err != nil {
		t.Fatal(err)
	}
	remaining := make([]byte, secureBlockSize+secureBlockSize)
	remaining[0] = 7
	remaining[secureBlockSize] = status
	if nonzeroPadding {
		remaining[secureBlockSize+1] = 1
	}
	sealed, err := codec.tx.sealLocked(remaining)
	if err != nil {
		t.Fatal(err)
	}
	return append(wire, sealed...)
}
