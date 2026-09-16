package mgr

import (
	"bytes"
	"errors"
	"net/netip"
	"testing"

	wire "github.com/otuschhoff/go-librados/internal/encoding"
	"github.com/otuschhoff/go-librados/internal/maps"
	"github.com/otuschhoff/go-librados/internal/msgr"
	"github.com/otuschhoff/go-librados/internal/protocol"
)

func TestEncodeManagerCommandExactBytes(t *testing.T) {
	fsid := maps.FSID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	command := []string{`{"prefix":"dashboard get","format":"json"}`, `{"target":"mgr.active"}`}
	message, err := EncodeCommand(fsid, command, []byte("input"), 1024)
	if err != nil {
		t.Fatal(err)
	}
	encoder := wire.NewEncoder(1024)
	encoder.Raw(fsid[:])
	encoder.Uint32(2)
	encoder.String(command[0])
	encoder.String(command[1])
	want, err := encoder.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	if message.Header.Type != protocol.MessageMgrCommand || message.Header.Version != 1 || message.Header.CompatVersion != 0 || !bytes.Equal(message.Front, want) || string(message.Data) != "input" {
		t.Fatalf("command = %+v front=%x want=%x", message.Header, message.Front, want)
	}
	message.Data[0] = 'X'
	if string([]byte("input")) != "input" {
		t.Fatal("impossible input mutation check")
	}
	if string(message.Data) != "Xnput" {
		t.Fatal("expected test-local message mutation")
	}
}

func TestDecodeManagerCommandReply(t *testing.T) {
	encoder := wire.NewEncoder(128)
	protocol.WireErrno(-13).Encode(encoder)
	encoder.String("permission denied")
	front, err := encoder.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	message := frontMessage(protocol.MessageMgrCommandReply, 1, 1, front)
	message.Data = []byte("details")
	message.Lengths.Data = uint32(len(message.Data))
	reply, err := DecodeCommandReply(message, 128)
	if err != nil {
		t.Fatal(err)
	}
	if reply.Result != -13 || reply.Status != "permission denied" || string(reply.Data) != "details" {
		t.Fatalf("reply = %+v", reply)
	}
	message.Data[0] = 'X'
	if string(reply.Data) != "details" {
		t.Fatal("reply data aliases message storage")
	}
}

func TestDecodeManagerCommandReplyRejectsInvalidInput(t *testing.T) {
	encoder := wire.NewEncoder(64)
	protocol.WireErrno(0).Encode(encoder)
	encoder.String("ok")
	front, _ := encoder.BytesResult()
	tests := []struct {
		name    string
		message msgr.Message
		want    error
	}{
		{name: "trailing", message: frontMessage(protocol.MessageMgrCommandReply, 1, 1, append(front, 1)), want: wire.ErrMalformed},
		{name: "truncated", message: frontMessage(protocol.MessageMgrCommandReply, 1, 1, front[:len(front)-1]), want: wire.ErrMalformed},
		{name: "unsupported header", message: frontMessage(protocol.MessageMgrCommandReply, 2, 2, front), want: wire.ErrMalformed},
		{name: "wrong type", message: frontMessage(protocol.MessageMgrCommand, 1, 0, front), want: wire.ErrMalformed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			message := test.message
			if test.name != "trailing" && test.name != "truncated" {
				message.Data = []byte("x")
				message.Lengths.Data = uint32(len(message.Data))
			}
			_, err := DecodeCommandReply(message, 64)
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
	if _, err := EncodeCommand(maps.FSID{}, []string{"a"}, make([]byte, 65), 64); !errors.Is(err, wire.ErrLimitExceeded) {
		t.Fatalf("encode limit error = %v", err)
	}
}

func TestDecodeManagerMapMessage(t *testing.T) {
	encoded := mapsEncodeMgrMapForMessage(t)
	message := frontMessage(protocol.MessageMgrMap, 1, 1, encoded)
	mgrMap, err := DecodeMgrMap(message, maps.Limits{MaxBytes: 16 << 10, MaxAddresses: 8, MaxCollectionEntries: 32})
	if err != nil {
		t.Fatal(err)
	}
	if mgrMap.Epoch() != 42 || mgrMap.ActiveName() != "active" {
		t.Fatalf("mgrmap = epoch:%d name:%q", mgrMap.Epoch(), mgrMap.ActiveName())
	}
	if _, err := DecodeMgrMap(frontMessage(protocol.MessageMgrMap, 1, 1, append(encoded, 0)), maps.Limits{MaxBytes: 16 << 10, MaxAddresses: 8, MaxCollectionEntries: 32}); !errors.Is(err, maps.ErrMalformedMap) {
		t.Fatalf("trailing mgrmap error = %v", err)
	}
}

func mapsEncodeMgrMapForMessage(t *testing.T) []byte {
	t.Helper()
	active, err := protocol.IPv4EntityAddr(protocol.AddressV2, 3, netip.MustParseAddrPort("192.0.2.50:7000"))
	if err != nil {
		t.Fatal(err)
	}
	standby, err := protocol.IPv4EntityAddr(protocol.AddressV2, 4, netip.MustParseAddrPort("192.0.2.51:7001"))
	if err != nil {
		t.Fatal(err)
	}
	features := protocol.FeatureMessageAddress2 | protocol.FeatureServerNautilusMask
	encoder := wire.NewEncoder(16 << 10)
	encoder.Versioned(14, 6, func(payload *wire.Encoder) {
		payload.Uint32(42)
		if err := (protocol.EntityAddrVec{active}).Encode(payload, features); err != nil {
			t.Fatal(err)
		}
		payload.Uint64(7)
		payload.Bool(true)
		payload.String("active")
		payload.Uint32(1)
		payload.Uint64(9)
		payload.Versioned(4, 1, func(standbyPayload *wire.Encoder) {
			standbyPayload.Uint64(9)
			standbyPayload.String("standby")
			payloadStringSet(standbyPayload, []string{"dashboard"})
			payloadModuleInfos(standbyPayload, true)
			standbyPayload.Uint64(0x20)
		})
		payloadStringSet(payload, []string{"dashboard"})
		payload.Uint32(1)
		payload.String("dashboard")
		payload.String("https://192.0.2.50")
		payloadModuleInfos(payload, false)
		payload.Uint32(123)
		payload.Uint32(456)
		payload.Uint32(1)
		payload.Uint32(18)
		payloadStringSet(payload, []string{"status"})
		payload.Uint64(0x80)
		payload.Uint32(17)
		payload.Uint32(1)
		if err := (protocol.EntityAddrVec{standby}).Encode(payload, features); err != nil {
			t.Fatal(err)
		}
		payload.Uint32(1)
		payload.String("client.admin")
		payload.Uint64(1)
		payloadStringSet(payload, []string{"crash"})
	})
	data, err := encoder.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func payloadStringSet(encoder *wire.Encoder, values []string) {
	encoder.Uint32(uint32(len(values)))
	for _, value := range values {
		encoder.String(value)
	}
}

func payloadModuleInfos(encoder *wire.Encoder, withoutOption bool) {
	encoder.Uint32(1)
	encoder.Versioned(2, 1, func(payload *wire.Encoder) {
		payload.String("dashboard")
		payload.Bool(true)
		payload.String("")
		if withoutOption {
			payload.Uint32(0)
			return
		}
		payload.Uint32(1)
		payload.String("refresh")
		payload.Versioned(1, 1, func(option *wire.Encoder) {
			option.String("refresh")
			option.Uint8(0)
			option.Uint8(1)
			option.Uint32(0)
			option.String("10")
			option.String("1")
			option.String("60")
			payloadStringSet(option, []string{"10", "20"})
			option.String("desc")
			option.String("long desc")
			payloadStringSet(option, []string{"perf"})
			payloadStringSet(option, []string{"other"})
		})
	})
}
