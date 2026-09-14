package msgr

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
)

var fuzzLimits = Limits{
	MaxSegmentBytes: 256,
	MaxFrameBytes:   1024,
	MaxAddresses:    2,
	MaxAuthBytes:    64,
}

const (
	maxFuzzWireBytes      = 2048
	maxSessionScriptBytes = 24
)

func FuzzBanner(f *testing.F) {
	f.Add(ClientBanner().Encode())
	f.Add([]byte("ceph v1\n\x10\x00"))
	f.Add([]byte(bannerPrefix + "\x41\x00"))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > maxFuzzWireBytes {
			return
		}
		banner, err := ReadBanner(bytes.NewReader(data), 64)
		if err != nil {
			return
		}
		encoded := banner.Encode()
		decoded, err := ReadBanner(bytes.NewReader(encoded), 64)
		if err != nil {
			t.Fatalf("re-read encoded banner: %v", err)
		}
		if decoded != banner {
			t.Fatalf("banner roundtrip = %#v, want %#v", decoded, banner)
		}
	})
}

func FuzzCRCFrame(f *testing.F) {
	valid := fuzzMustCRC(f, Frame{Tag: TagMessage, Segments: []Segment{
		{Alignment: DefaultAlignment, Data: []byte("header")},
		{Alignment: PageAlignment, Data: []byte("data")},
	}})
	f.Add(valid)
	f.Add(valid[:PreambleSize-1])
	corrupt := append([]byte(nil), valid...)
	corrupt[PreambleSize] ^= 0x80
	f.Add(corrupt)
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > maxFuzzWireBytes {
			return
		}
		frame, err := ReadCRC(bytes.NewReader(data), fuzzLimits)
		if err != nil {
			return
		}
		encoded, err := EncodeCRC(frame, fuzzLimits)
		if err != nil {
			t.Fatalf("encode decoded CRC frame: %v", err)
		}
		decoded, err := ReadCRC(bytes.NewReader(encoded), fuzzLimits)
		if err != nil {
			t.Fatalf("re-read encoded CRC frame: %v", err)
		}
		assertFuzzFrameEqual(t, decoded, frame)
	})
}

func FuzzSecureFrame(f *testing.F) {
	valid := fuzzMustSecure(f, Frame{Tag: TagMessage, Segments: []Segment{
		{Alignment: DefaultAlignment, Data: bytes.Repeat([]byte{0x5a}, 49)},
		{Alignment: PageAlignment, Data: []byte("tail")},
	}})
	f.Add(valid)
	f.Add(valid[:securePreamble-1])
	corrupt := append([]byte(nil), valid...)
	corrupt[len(corrupt)-1] ^= 1
	f.Add(corrupt)
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > maxFuzzWireBytes {
			return
		}
		secret := fuzzSecureSecret()
		_, inputReader := fuzzCrossedCodecs(t, secret)
		frame, err := inputReader.Read(bytes.NewReader(data), fuzzLimits)
		if err != nil {
			return
		}

		writer, reader := fuzzCrossedCodecs(t, secret)
		encoded, err := writer.Encode(frame, fuzzLimits)
		if err != nil {
			t.Fatalf("encode decoded secure frame: %v", err)
		}
		decoded, err := reader.Read(bytes.NewReader(encoded), fuzzLimits)
		if err != nil {
			t.Fatalf("re-read encoded secure frame: %v", err)
		}
		assertFuzzFrameEqual(t, decoded, frame)
	})
}

func FuzzControlPayload(f *testing.F) {
	for _, payload := range []any{
		Ack{Sequence: 7},
		SessionReset{Full: true},
		Keepalive2{Timestamp: Timestamp{Seconds: 12, Nanoseconds: 34}},
		AuthPayload{Tag: TagAuthDone, Payload: []byte("auth")},
		Wait{},
	} {
		frame := fuzzMustControl(f, payload)
		f.Add(uint8(frame.Tag), frame.Segments[0].Data)
	}
	f.Add(uint8(TagSessionReset), []byte{2})
	f.Add(uint8(TagAck), []byte{1, 2})
	f.Add(uint8(0xff), []byte{})

	f.Fuzz(func(t *testing.T, tag uint8, data []byte) {
		if len(data) > int(fuzzLimits.MaxSegmentBytes)+1 {
			return
		}
		frame := Frame{Tag: Tag(tag), Segments: []Segment{{Alignment: DefaultAlignment, Data: data}}}
		payload, err := DecodeControl(frame, fuzzLimits)
		if err != nil {
			return
		}
		encoded, err := EncodeControl(payload, fuzzLimits)
		if err != nil {
			t.Fatalf("encode decoded control payload: %v", err)
		}
		decoded, err := DecodeControl(encoded, fuzzLimits)
		if err != nil {
			t.Fatalf("re-decode control payload: %v", err)
		}
		if !reflect.DeepEqual(decoded, payload) {
			t.Fatalf("control roundtrip = %#v, want %#v", decoded, payload)
		}
	})
}

func FuzzMessageFrame(f *testing.F) {
	message := Message{
		Header:  MessageHeader{Sequence: 1, TransactionID: 2, DataPrePaddingLength: 1},
		Lengths: MessageLengths{Front: 5, Middle: 3, Data: 2},
		Front:   []byte("front"),
		Middle:  []byte("mid"),
		Data:    []byte{0, 1},
	}
	frame, err := EncodeMessage(message, fuzzLimits)
	if err != nil {
		f.Fatal(err)
	}
	valid := fuzzMustCRC(f, frame)
	f.Add(valid)
	f.Add(valid[:PreambleSize])
	f.Add([]byte{byte(TagMessage), 4})
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > maxFuzzWireBytes {
			return
		}
		frame, err := ReadCRC(bytes.NewReader(data), fuzzLimits)
		if err != nil {
			return
		}
		message, err := DecodeMessage(frame, fuzzLimits)
		if err != nil {
			return
		}
		encodedFrame, err := EncodeMessage(message, fuzzLimits)
		if err != nil {
			t.Fatalf("encode decoded message: %v", err)
		}
		decoded, err := DecodeMessage(encodedFrame, fuzzLimits)
		if err != nil {
			t.Fatalf("re-decode message: %v", err)
		}
		if !reflect.DeepEqual(decoded, message) {
			t.Fatalf("message roundtrip = %#v, want %#v", decoded, message)
		}
	})
}

func FuzzSessionScript(f *testing.F) {
	f.Add([]byte{0x00, 0x08, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07})
	f.Add([]byte{0x00, 0x01})
	f.Add([]byte{0x00, 0x08, 0x16})
	f.Add([]byte{0x03, 0x05, 0x07})
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, script []byte) {
		if len(script) > maxSessionScriptBytes {
			return
		}
		owner := newFuzzSessionOwner(t)
		done := make(chan struct{})
		defer owner.stop(done)

		for _, instruction := range script {
			operand := uint64(instruction >> 4)
			switch instruction & 0x0f {
			case 0:
				payload := []byte{byte(operand), byte(len(owner.pending))}
				owner.submit(&submitCommand{
					ctx:     context.Background(),
					message: Message{Lengths: MessageLengths{Front: uint32(len(payload))}, Front: payload},
					result:  make(chan submitResult, 1),
				})
			case 1:
				if len(owner.pending) > 0 {
					owner.cancel(cancelCommand{request: owner.pending[0].request, err: context.Canceled})
				}
			case 2:
				owner.handleFrame(fuzzMustControl(t, Ack{Sequence: operand}))
			case 3:
				owner.handleFrame(fuzzMustControl(t, Keepalive2{Timestamp: Timestamp{Seconds: uint32(operand)}}))
			case 4:
				transactionID := uint64(0)
				if len(owner.pending) > 0 {
					transactionID = owner.pending[0].message.Header.TransactionID
				}
				message := Message{Header: MessageHeader{Sequence: operand + 1, TransactionID: transactionID}}
				owner.handleFrame(fuzzMustMessageFrame(t, message))
			case 5:
				message := Message{Header: MessageHeader{Sequence: owner.lastInbound}}
				owner.handleFrame(fuzzMustMessageFrame(t, message))
			case 6:
				owner.state = StateReconnecting
				owner.handleFrame(fuzzMustControl(t, SessionReset{Full: operand&1 != 0}))
			case 7:
				owner.handleFrame(Frame{Tag: 0})
			case 8:
				if owner.inFlightCount() < owner.config.MaxInFlightTransactions {
					for _, pending := range owner.pending {
						if !pending.sent {
							pending.sent = true
							pending.seq = owner.takeSequence()
							pending.message.Header.Sequence = pending.seq
							if !containsPending(owner.replay, pending) {
								owner.replay = append(owner.replay, pending)
							}
							break
						}
					}
				}
			}
			assertFuzzSessionInvariants(t, owner)
		}
	})
}

func fuzzMustCRC(tb testing.TB, frame Frame) []byte {
	tb.Helper()
	data, err := EncodeCRC(frame, fuzzLimits)
	if err != nil {
		tb.Fatal(err)
	}
	return data
}

func fuzzSecureSecret() []byte {
	secret := make([]byte, secureSecretSize)
	for index := range secret {
		secret[index] = byte(index)
	}
	return secret
}

func fuzzCrossedCodecs(tb testing.TB, secret []byte) (*SecureCodec, *SecureCodec) {
	tb.Helper()
	client, err := NewSecureCodec(secret, false)
	if err != nil {
		tb.Fatal(err)
	}
	server, err := NewSecureCodec(secret, true)
	if err != nil {
		tb.Fatal(err)
	}
	return client, server
}

func fuzzMustSecure(tb testing.TB, frame Frame) []byte {
	tb.Helper()
	client, _ := fuzzCrossedCodecs(tb, fuzzSecureSecret())
	data, err := client.Encode(frame, fuzzLimits)
	if err != nil {
		tb.Fatal(err)
	}
	return data
}

func fuzzMustControl(tb testing.TB, payload any) Frame {
	tb.Helper()
	frame, err := EncodeControl(payload, fuzzLimits)
	if err != nil {
		tb.Fatal(err)
	}
	return frame
}

func fuzzMustMessageFrame(tb testing.TB, message Message) Frame {
	tb.Helper()
	frame, err := EncodeMessage(message, fuzzLimits)
	if err != nil {
		tb.Fatal(err)
	}
	return frame
}

func assertFuzzFrameEqual(t *testing.T, got, want Frame) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("frame roundtrip = %#v, want %#v", got, want)
	}
}

func newFuzzSessionOwner(t *testing.T) *sessionOwner {
	t.Helper()
	config := testSessionConfig(t)
	config.Limits = fuzzLimits
	config.MaxQueuedMessages = 8
	config.MaxRetainedBytes = 2048
	config.MaxInFlightTransactions = 4
	config.MaxReconnectAttempts = 0
	config.MaxHandshakeTransitions = 8
	config.EventBuffer = 64

	session := &Session{
		commands: make(chan any),
		events:   make(chan SessionEvent, config.EventBuffer),
		done:     make(chan struct{}),
	}
	_, cancel := context.WithCancel(context.Background())
	return &sessionOwner{
		session:         session,
		config:          config,
		state:           StateReady,
		byRequest:       make(map[*submitCommand]*pendingRequest),
		byTID:           make(map[uint64]*pendingRequest),
		nextOutbound:    1,
		nextTID:         1,
		clientCookie:    config.ClientCookie,
		serverCookie:    config.ServerCookie,
		globalSeq:       config.GlobalSequence,
		connectSeq:      config.ConnectSequence,
		connectorCancel: cancel,
	}
}

func assertFuzzSessionInvariants(t *testing.T, owner *sessionOwner) {
	t.Helper()
	if len(owner.pending) > owner.config.MaxQueuedMessages {
		t.Fatalf("pending count %d exceeds limit %d", len(owner.pending), owner.config.MaxQueuedMessages)
	}
	if owner.inFlightCount() > owner.config.MaxInFlightTransactions {
		t.Fatalf("in-flight count %d exceeds limit %d", owner.inFlightCount(), owner.config.MaxInFlightTransactions)
	}
	var retained uint64
	for _, pending := range owner.pending {
		retained += pending.bytes
		if owner.byRequest[pending.request] != pending || owner.byTID[pending.message.Header.TransactionID] != pending {
			t.Fatal("pending request indexes are inconsistent")
		}
	}
	if retained != owner.retainedBytes || retained > owner.config.MaxRetainedBytes {
		t.Fatalf("retained bytes = %d, tracked = %d, limit = %d", retained, owner.retainedBytes, owner.config.MaxRetainedBytes)
	}
	for _, replay := range owner.replay {
		if owner.byRequest[replay.request] != replay || replay.seq == 0 {
			t.Fatal("replay entry is not an active sequenced request")
		}
	}
	if owner.state == StateStopped {
		t.Fatal(errors.New("session stopped before script completed"))
	}
}
