package msgr

import (
	"bytes"
	"errors"
	"net/netip"
	"reflect"
	"testing"

	wire "github.com/otuschhoff/rados-go/internal/encoding"
	"github.com/otuschhoff/rados-go/internal/protocol"
)

var controlTestLimits = Limits{
	MaxSegmentBytes: 4096,
	MaxFrameBytes:   8192,
	MaxAddresses:    4,
	MaxAuthBytes:    64,
}

func TestHelloExactBytes(t *testing.T) {
	address, err := protocol.IPv4EntityAddr(protocol.AddressV2, 0x04030201, netip.MustParseAddrPort("192.0.2.1:3300"))
	if err != nil {
		t.Fatal(err)
	}
	frame, err := EncodeControl(Hello{EntityType: protocol.EntityClient, PeerAddress: address}, controlTestLimits)
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{
		0x08,
		0x01, 0x01, 0x01, 0x1c, 0x00, 0x00, 0x00,
		0x02, 0x00, 0x00, 0x00,
		0x01, 0x02, 0x03, 0x04,
		0x10, 0x00, 0x00, 0x00,
		0x02, 0x00, 0x0c, 0xe4, 0xc0, 0x00, 0x02, 0x01,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}
	if frame.Tag != TagHello || len(frame.Segments) != 1 || frame.Segments[0].Alignment != DefaultAlignment {
		t.Fatalf("frame = %#v", frame)
	}
	if !bytes.Equal(frame.Segments[0].Data, want) {
		t.Fatalf("payload = %x, want %x", frame.Segments[0].Data, want)
	}
}

func TestControlRoundTrips(t *testing.T) {
	address, err := protocol.IPv4EntityAddr(protocol.AddressV2, 7, netip.MustParseAddrPort("198.51.100.2:6789"))
	if err != nil {
		t.Fatal(err)
	}
	addresses := protocol.EntityAddrVec{address}
	tests := []any{
		Hello{EntityType: protocol.EntityOSD, PeerAddress: address},
		ClientIdent{Addresses: addresses, TargetAddress: address, GlobalID: -1, GlobalSequence: 2, SupportedFeatures: 3, RequiredFeatures: 4, Flags: 5, Cookie: 6},
		ServerIdent{Addresses: addresses, GlobalID: 1, GlobalSequence: 2, SupportedFeatures: 3, RequiredFeatures: 4, Flags: 5, Cookie: 6},
		IdentMissingFeatures{Features: 9},
		SessionReconnect{Addresses: addresses, ClientCookie: 1, ServerCookie: 2, GlobalSequence: 3, ConnectSequence: 4, MessageSequence: 5},
		SessionReset{Full: true},
		SessionRetry{ConnectSequence: 11},
		SessionRetryGlobal{GlobalSequence: 12},
		SessionReconnectOK{MessageSequence: 13},
		Wait{},
		Keepalive2{Timestamp: Timestamp{Seconds: 14, Nanoseconds: 15}},
		Keepalive2Ack{Timestamp: Timestamp{Seconds: 16, Nanoseconds: 17}},
		Ack{Sequence: 18},
		AuthRequest{Method: 2, PreferredModes: []uint32{2, 1}, AuthPayload: []byte{1, 2, 3}},
		AuthBadMethod{Method: 2, Result: -13, AllowedMethods: []uint32{2}, AllowedModes: []uint32{2, 1}},
		AuthReplyMore{AuthPayload: []byte{4, 5}},
		AuthRequestMore{AuthPayload: []byte{6, 7}},
		AuthDone{GlobalID: 42, ConnectionMode: 2, AuthPayload: []byte{8, 9}},
		AuthSignature{Signature: [32]byte{1, 2, 3}},
	}
	for _, want := range tests {
		t.Run(reflect.TypeOf(want).Name(), func(t *testing.T) {
			frame, err := EncodeControl(want, controlTestLimits)
			if err != nil {
				t.Fatal(err)
			}
			got, err := DecodeControl(frame, controlTestLimits)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("decoded = %#v, want %#v", got, want)
			}
		})
	}
}

func TestAuthPayloadTagsAreBoundedAndClassified(t *testing.T) {
	for tag := TagAuthRequest; tag <= TagAuthSignature; tag++ {
		want := AuthPayload{Tag: tag, Payload: []byte{1, 2, 3}}
		frame, err := EncodeControl(want, controlTestLimits)
		if err != nil {
			t.Fatal(err)
		}
		if frame.Tag != tag {
			t.Fatalf("tag = %d, want %d", frame.Tag, tag)
		}
	}
	encoder := wire.NewEncoder(4096)
	encoder.Uint64(1)
	encoder.Uint32(2)
	encoder.Bytes(make([]byte, 65))
	data, err := encoder.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	_, err = DecodeControl(Frame{Tag: TagAuthDone, Segments: []Segment{{Alignment: DefaultAlignment, Data: data}}}, controlTestLimits)
	if !errors.Is(err, wire.ErrLimitExceeded) {
		t.Fatalf("auth limit error = %v", err)
	}
}

func TestControlRejectsMalformedPayloads(t *testing.T) {
	tests := []struct {
		name  string
		frame Frame
		want  error
	}{
		{"trailing bytes", Frame{Tag: TagAck, Segments: []Segment{{Alignment: DefaultAlignment, Data: make([]byte, 9)}}}, wire.ErrMalformed},
		{"malformed bool", Frame{Tag: TagSessionReset, Segments: []Segment{{Alignment: DefaultAlignment, Data: []byte{2}}}}, wire.ErrMalformed},
		{"wrong alignment", Frame{Tag: TagWait, Segments: []Segment{{Alignment: 16}}}, ErrMalformed},
		{"extra segment", Frame{Tag: TagWait, Segments: []Segment{{Alignment: 8}, {Alignment: 8}}}, ErrMalformed},
		{"compression", Frame{Tag: TagCompressionRequest, Segments: []Segment{{Alignment: 8}}}, ErrUnsupportedPayload},
		{"oversized vector", Frame{Tag: TagServerIdent, Segments: []Segment{{Alignment: 8, Data: []byte{2, 5, 0, 0, 0}}}}, wire.ErrLimitExceeded},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := DecodeControl(test.frame, controlTestLimits); !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
}
