package msgr

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
)

func TestSecureLeasedPayloadEncoding(t *testing.T) {
	for _, size := range []int{4096, 65536, 1048576, 4194304} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			backing := bytes.Repeat([]byte{0xa5}, LeasedPayloadPrefix+size+LeasedPayloadPadding)
			payload := backing[LeasedPayloadPrefix : LeasedPayloadPrefix+size]
			original := append([]byte(nil), payload...)
			lease := NewPaddedMessageLease(backing, payload, nil)
			defer lease.Release()
			front := bytes.Repeat([]byte{0x13}, 80)
			frame, err := encodeOwnedMessage(Message{Lengths: MessageLengths{Front: uint32(len(front)), Data: uint32(size)}, Front: front, Data: payload}, performanceLimits)
			if err != nil {
				t.Fatal(err)
			}
			if plaintext := lease.securePlaintext(frame.Segments); len(plaintext) != len(front)+size+LeasedPayloadPadding || &plaintext[len(front)] != &payload[0] || !bytes.Equal(plaintext[:len(front)], front) || plaintext[len(front)+size] != LateStatusComplete || !allZero(plaintext[len(front)+size+1:]) {
				t.Fatal("real message layout did not select direct leased plaintext")
			}
			ordinary := mustSecureCodec(t, testSecureSecret(), false)
			leased := mustSecureCodec(t, testSecureSecret(), false)
			receiver := mustSecureCodec(t, testSecureSecret(), true)
			for iteration := 0; iteration < 2; iteration++ {
				expected, err := ordinary.Encode(frame, performanceLimits)
				if err != nil {
					t.Fatal(err)
				}
				frame.payloadLease = lease
				actual, err := leased.encodeInto(frame, performanceLimits, bytes.Repeat([]byte{0xcc}, len(expected)))
				frame.payloadLease = nil
				if err != nil || !bytes.Equal(actual, expected) {
					t.Fatalf("leased ciphertext differs: %v", err)
				}
				decoded, err := receiver.Read(bytes.NewReader(actual), performanceLimits)
				if err != nil || !bytes.Equal(decoded.Segments[3].Data, original) || !bytes.Equal(payload, original) || backing[LeasedPayloadPrefix+size] != LateStatusComplete || !allZero(backing[LeasedPayloadPrefix+size+1:]) {
					t.Fatalf("leased input changed or ciphertext did not authenticate: %v", err)
				}
			}
			frame.Segments[1].Data = bytes.Repeat([]byte{1}, LeasedPayloadPrefix+1)
			if lease.securePlaintext(frame.Segments) != nil {
				t.Fatal("oversized prefix incorrectly selected direct plaintext")
			}
			expected, err := ordinary.Encode(frame, performanceLimits)
			if err != nil {
				t.Fatal(err)
			}
			frame.payloadLease = lease
			actual, err := leased.Encode(frame, performanceLimits)
			frame.payloadLease = nil
			if err != nil || !bytes.Equal(actual, expected) {
				t.Fatalf("oversized prefix fallback changed ciphertext: %v", err)
			}
			frame.Segments[1].Data = front
			frame.Segments[3].Data = append([]byte(nil), original...)
			if lease.securePlaintext(frame.Segments) != nil {
				t.Fatal("unrelated payload incorrectly selected direct plaintext")
			}
			expected, err = ordinary.Encode(frame, performanceLimits)
			if err != nil {
				t.Fatal(err)
			}
			frame.payloadLease = lease
			actual, err = leased.Encode(frame, performanceLimits)
			if err != nil || !bytes.Equal(actual, expected) {
				t.Fatalf("unrelated payload fallback changed ciphertext: %v", err)
			}
		})
	}
}

func TestSecurePaddedMessageLeaseRejectsInvalidAllocation(t *testing.T) {
	for index, payload := range [][]byte{nil, make([]byte, 16), make([]byte, 1, 17), make([]byte, 17, 33)} {
		t.Run(fmt.Sprint(index), func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("invalid padded allocation was accepted")
				}
			}()
			NewPaddedMessageLease(payload, payload, nil)
		})
	}
}

func TestSecureLeasedPayloadConcurrentWriters(t *testing.T) {
	storage := make([]byte, LeasedPayloadPrefix+4096+LeasedPayloadPadding)
	payload := storage[LeasedPayloadPrefix : LeasedPayloadPrefix+4096]
	copy(payload, bytes.Repeat([]byte{0x5a}, len(payload)))
	lease := NewPaddedMessageLease(storage, payload, nil)
	defer lease.Release()
	start := make(chan struct{})
	failures := make(chan error, 16)
	var writers sync.WaitGroup
	for index := 0; index < 16; index++ {
		front := bytes.Repeat([]byte{byte(index + 1)}, 17+index)
		middle := bytes.Repeat([]byte{byte(index + 32)}, 19)
		frame, err := encodeOwnedMessage(Message{Lengths: MessageLengths{Front: uint32(len(front)), Middle: uint32(len(middle)), Data: uint32(len(payload))}, Front: front, Middle: middle, Data: payload}, performanceLimits)
		if err != nil {
			t.Fatal(err)
		}
		frame.payloadLease = lease
		sender := mustSecureCodec(t, testSecureSecret(), false)
		receiver := mustSecureCodec(t, testSecureSecret(), true)
		lease.Retain()
		writers.Add(1)
		go func() {
			defer writers.Done()
			defer lease.Release()
			<-start
			for iteration := 0; iteration < 16; iteration++ {
				ciphertext, err := sender.Encode(frame, performanceLimits)
				if err != nil {
					failures <- err
					return
				}
				decoded, err := receiver.Read(bytes.NewReader(ciphertext), performanceLimits)
				if err != nil {
					failures <- err
					return
				}
				if !bytes.Equal(decoded.Segments[1].Data, front) || !bytes.Equal(decoded.Segments[2].Data, middle) || !bytes.Equal(decoded.Segments[3].Data, payload) {
					failures <- errors.New("concurrent leased prefix or payload changed")
					return
				}
			}
		}()
	}
	close(start)
	writers.Wait()
	close(failures)
	for failure := range failures {
		t.Error(failure)
	}
}

func TestSecureDeterministicVector(t *testing.T) {
	secret := testSecureSecret()
	codec := mustSecureCodec(t, secret, false)
	wire, err := codec.Encode(Frame{Tag: TagAck, Segments: []Segment{{Alignment: DefaultAlignment, Data: []byte("ceph")}}}, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	const expected = "4de81a477793cc7cbb381480b30cc69431dabdf879b50b483d403cfd7520cf9d25a34695d34ff7965036a3b4f02908ecd7f8cc4f2688a28ab01f761aeee6fdba01654a9d4a848c689172d4521fcf810804495e6587c7f08a8c5bc025ec8d1369"
	if got := hex.EncodeToString(wire); got != expected {
		t.Fatalf("secure vector = %s", got)
	}
}

func TestSecureEncodePreservesCallerBuffers(t *testing.T) {
	secret := testSecureSecret()
	sender := mustSecureCodec(t, secret, false)
	receiver := mustSecureCodec(t, secret, true)
	first := bytes.Repeat([]byte{0x5a}, 63)
	tail := bytes.Repeat([]byte{0xa5}, 97)
	frame := Frame{Tag: TagMessage, Segments: []Segment{
		{Alignment: DefaultAlignment, Data: first},
		{Alignment: PageAlignment, Data: tail},
		{Alignment: DefaultAlignment},
	}}
	wire, err := sender.Encode(frame, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, bytes.Repeat([]byte{0x5a}, 63)) || !bytes.Equal(tail, bytes.Repeat([]byte{0xa5}, 97)) || len(frame.Segments) != 3 {
		t.Fatal("encoding modified caller frame")
	}
	first[0] = 0
	tail[0] = 0
	decoded, err := receiver.Read(bytes.NewReader(wire), testLimits)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Segments[0].Data[0] != 0x5a || decoded.Segments[1].Data[0] != 0xa5 {
		t.Fatal("wire aliases caller buffers")
	}
}

func TestSecureEncodeRecordBoundaries(t *testing.T) {
	for _, firstSize := range []int{0, 47, 48, 49, 63, 64, 65} {
		for _, tailSize := range []int{0, 1, 15, 16, 17} {
			t.Run(fmt.Sprintf("first=%d/tail=%d", firstSize, tailSize), func(t *testing.T) {
				frame := Frame{Tag: TagMessage, Segments: []Segment{
					{Alignment: DefaultAlignment, Data: bytes.Repeat([]byte{0x5a}, firstSize)},
					{Alignment: PageAlignment, Data: bytes.Repeat([]byte{0xa5}, tailSize)},
				}}
				if tailSize == 0 {
					frame.Segments = frame.Segments[:1]
				}
				_, _, wireSize, records, err := prepareSecureFrame(frame, testLimits)
				if err != nil {
					t.Fatal(err)
				}
				limits := testLimits
				limits.MaxFrameBytes = wireSize
				sender := mustSecureCodec(t, testSecureSecret(), false)
				receiver := mustSecureCodec(t, testSecureSecret(), true)
				sender.tx.counter = math.MaxUint64 - records + 1
				receiver.rx.counter = sender.tx.counter
				wire, err := sender.Encode(frame, limits)
				if err != nil || uint64(len(wire)) != wireSize {
					t.Fatalf("wire size=%d want=%d err=%v", len(wire), wireSize, err)
				}
				decoded, err := receiver.Read(bytes.NewReader(wire), limits)
				if err != nil {
					t.Fatal(err)
				}
				assertFrameEqual(t, decoded, frame)
				if _, err := sender.Encode(frame, limits); !errors.Is(err, ErrCounterExhausted) {
					t.Fatalf("post-exhaustion error=%v", err)
				}
			})
		}
	}
}

func TestSecureEncodeIntoDirtyBuffer(t *testing.T) {
	for _, firstSize := range []int{0, 47, 48, 49, 63, 64, 65} {
		for _, tailSize := range []int{0, 1, 15, 16, 17, 97} {
			frame := Frame{Tag: TagMessage, Segments: []Segment{
				{Alignment: DefaultAlignment, Data: bytes.Repeat([]byte{0x5a}, firstSize)},
				{Alignment: PageAlignment, Data: bytes.Repeat([]byte{0xa5}, tailSize)},
			}}
			fresh := mustSecureCodec(t, testSecureSecret(), false)
			reused := mustSecureCodec(t, testSecureSecret(), false)
			for iteration := 0; iteration < 2; iteration++ {
				want, err := fresh.Encode(frame, testLimits)
				if err != nil {
					t.Fatal(err)
				}
				buffer := bytes.Repeat([]byte{0xff}, len(want)+128)
				got, err := reused.encodeInto(frame, testLimits, buffer)
				if err != nil || !bytes.Equal(got, want) || &got[0] != &buffer[0] {
					t.Fatalf("first=%d tail=%d iteration=%d reused encoding mismatch: %v", firstSize, tailSize, iteration, err)
				}
			}
		}
	}
}

func TestSecureReadOwnsSegmentBuffers(t *testing.T) {
	secret := testSecureSecret()
	sender := mustSecureCodec(t, secret, true)
	receiver := mustSecureCodec(t, secret, false)
	frame := Frame{Tag: TagMessage, Segments: []Segment{
		{Alignment: DefaultAlignment, Data: bytes.Repeat([]byte{1}, 64)},
		{Alignment: DefaultAlignment, Data: bytes.Repeat([]byte{2}, 32)},
		{Alignment: DefaultAlignment, Data: bytes.Repeat([]byte{3}, 32)},
	}}
	wire, err := sender.Encode(frame, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	first, err := receiver.Read(bytes.NewReader(wire), testLimits)
	if err != nil {
		t.Fatal(err)
	}
	for index, segment := range first.Segments {
		if cap(segment.Data) != len(segment.Data) {
			t.Fatalf("segment %d exposes neighboring storage", index)
		}
	}
	_ = append(first.Segments[1].Data, 99)
	assertFrameEqual(t, first, frame)
	wire, err = sender.Encode(frame, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	second, err := receiver.Read(bytes.NewReader(wire), testLimits)
	if err != nil {
		t.Fatal(err)
	}
	second.Segments[1].Data[0] = 42
	assertFrameEqual(t, first, frame)
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
