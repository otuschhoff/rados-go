package maps

import (
	"bytes"
	"errors"
	"net/netip"
	"testing"

	wire "github.com/otuschhoff/go-librados/internal/encoding"
	"github.com/otuschhoff/go-librados/internal/protocol"
)

var testMgrMapLimits = Limits{MaxBytes: 16 << 10, MaxAddresses: 8, MaxCollectionEntries: 32}

func TestDecodeMgrMapV14(t *testing.T) {
	data := encodeTestMgrMap(t, testMgrMapOptions{})
	mgrMap, err := DecodeMgrMap(data, testMgrMapLimits)
	if err != nil {
		t.Fatal(err)
	}
	if mgrMap.Epoch() != 42 || !mgrMap.Available() || mgrMap.ActiveGID() != 7 || mgrMap.ActiveName() != "active" {
		t.Fatalf("mgrmap summary = epoch:%d available:%t gid:%d name:%q", mgrMap.Epoch(), mgrMap.Available(), mgrMap.ActiveGID(), mgrMap.ActiveName())
	}
	addresses := mgrMap.ActiveAddresses()
	if len(addresses) != 1 {
		t.Fatalf("active addresses = %v", addresses)
	}
	endpoint, ok := addresses[0].AddrPort()
	if !ok || endpoint != netip.MustParseAddrPort("192.0.2.50:7000") {
		t.Fatalf("active endpoint = %v found=%t", endpoint, ok)
	}

	addresses[0].SocketData[0] ^= 0xff
	again := mgrMap.ActiveAddresses()
	if again[0].SocketData[0] == addresses[0].SocketData[0] {
		t.Fatal("caller mutation changed immutable mgr addresses")
	}
	data[10] ^= 0xff
	if mgrMap.Epoch() != 42 || mgrMap.ActiveName() != "active" {
		t.Fatal("decoded mgrmap aliases caller storage")
	}
}

func TestDecodeMgrMapAcceptsCompatibleExtension(t *testing.T) {
	data := encodeTestMgrMap(t, testMgrMapOptions{version: 15, outerTail: []byte{1, 2, 3}})
	mgrMap, err := DecodeMgrMap(data, testMgrMapLimits)
	if err != nil {
		t.Fatal(err)
	}
	if mgrMap.ActiveGID() != 7 {
		t.Fatalf("gid = %d", mgrMap.ActiveGID())
	}
}

func TestDecodeMgrMapRejectsInvalidInput(t *testing.T) {
	valid := encodeTestMgrMap(t, testMgrMapOptions{})
	tests := []struct {
		name   string
		data   []byte
		limits Limits
		want   error
	}{
		{name: "truncated", data: valid[:len(valid)-1], limits: testMgrMapLimits, want: wire.ErrMalformed},
		{name: "trailing", data: append(append([]byte(nil), valid...), 1), limits: testMgrMapLimits, want: ErrMalformedMap},
		{name: "legacy version", data: encodeTestMgrMap(t, testMgrMapOptions{version: 5, compat: 1}), limits: testMgrMapLimits, want: wire.ErrUnsupportedVersion},
		{name: "limit", data: valid, limits: Limits{MaxBytes: 16 << 10, MaxAddresses: 8, MaxCollectionEntries: 0}, want: wire.ErrLimitExceeded},
		{name: "availability flag", data: encodeTestMgrMap(t, testMgrMapOptions{availableByte: 2}), limits: testMgrMapLimits, want: ErrMalformedMap},
		{name: "client name mismatch", data: encodeTestMgrMap(t, testMgrMapOptions{omitClientName: true}), limits: testMgrMapLimits, want: ErrMalformedMap},
		{name: "invalid timestamp", data: encodeTestMgrMap(t, testMgrMapOptions{activeChangeNsec: 1_000_000_000}), limits: testMgrMapLimits, want: ErrMalformedMap},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := DecodeMgrMap(test.data, test.limits)
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
}

type testMgrMapOptions struct {
	version          uint8
	compat           uint8
	outerTail        []byte
	availableByte    uint8
	omitClientName   bool
	activeChangeNsec uint32
}

func encodeTestMgrMap(t *testing.T, options testMgrMapOptions) []byte {
	t.Helper()
	if options.version == 0 {
		options.version = 14
	}
	if options.compat == 0 {
		options.compat = 6
	}
	if options.availableByte == 0 {
		options.availableByte = 1
	}
	if options.activeChangeNsec == 0 {
		options.activeChangeNsec = 456
	}
	active, err := protocol.IPv4EntityAddr(protocol.AddressV2, 3, netip.MustParseAddrPort("192.0.2.50:7000"))
	if err != nil {
		t.Fatal(err)
	}
	standby, err := protocol.IPv4EntityAddr(protocol.AddressV2, 4, netip.MustParseAddrPort("192.0.2.51:7001"))
	if err != nil {
		t.Fatal(err)
	}
	features := protocol.FeatureMessageAddress2 | protocol.FeatureServerNautilusMask
	encoder := wire.NewEncoder(testMgrMapLimits.MaxBytes)
	encoder.Versioned(options.version, options.compat, func(payload *wire.Encoder) {
		payload.Uint32(42)
		if err := (protocol.EntityAddrVec{active}).Encode(payload, features); err != nil {
			t.Fatal(err)
		}
		payload.Uint64(7)
		payload.Uint8(options.availableByte)
		payload.String("active")
		payload.Uint32(1)
		payload.Uint64(9)
		payload.Versioned(4, 1, func(standbyPayload *wire.Encoder) {
			standbyPayload.Uint64(9)
			standbyPayload.String("standby")
			encodeStringSet(standbyPayload, []string{"dashboard"})
			encodeModuleInfoVector(standbyPayload, []moduleInfoFixture{{name: "dashboard", canRun: true}})
			standbyPayload.Uint64(0x20)
		})
		encodeStringSet(payload, []string{"dashboard"})
		encodeStringMap(payload, map[string]string{"dashboard": "https://192.0.2.50"})
		encodeModuleInfoVector(payload, []moduleInfoFixture{{name: "dashboard", canRun: true, withOption: true}})
		payload.Uint32(123)
		payload.Uint32(options.activeChangeNsec)
		encodeAlwaysOnModules(payload, map[uint32][]string{18: {"status"}})
		payload.Uint64(0x80)
		payload.Uint32(17)
		payload.Uint32(1)
		if err := (protocol.EntityAddrVec{standby}).Encode(payload, features); err != nil {
			t.Fatal(err)
		}
		if options.omitClientName {
			payload.Uint32(0)
		} else {
			payload.Uint32(1)
			payload.String("client.admin")
		}
		payload.Uint64(1)
		encodeStringSet(payload, []string{"crash"})
		payload.Raw(options.outerTail)
	})
	data, err := encoder.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	return data
}

type moduleInfoFixture struct {
	name       string
	canRun     bool
	withOption bool
}

func encodeModuleInfoVector(encoder *wire.Encoder, modules []moduleInfoFixture) {
	encoder.Uint32(uint32(len(modules)))
	for _, module := range modules {
		encoder.Versioned(2, 1, func(payload *wire.Encoder) {
			payload.String(module.name)
			payload.Bool(module.canRun)
			payload.String("")
			if module.withOption {
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
					encodeStringSet(option, []string{"10", "20"})
					option.String("desc")
					option.String("long desc")
					encodeStringSet(option, []string{"perf"})
					encodeStringSet(option, []string{"other"})
				})
				return
			}
			payload.Uint32(0)
		})
	}
}

func encodeStringSet(encoder *wire.Encoder, values []string) {
	encoder.Uint32(uint32(len(values)))
	for _, value := range values {
		encoder.String(value)
	}
}

func encodeStringMap(encoder *wire.Encoder, values map[string]string) {
	encoder.Uint32(uint32(len(values)))
	for key, value := range values {
		encoder.String(key)
		encoder.String(value)
	}
}

func encodeAlwaysOnModules(encoder *wire.Encoder, values map[uint32][]string) {
	encoder.Uint32(uint32(len(values)))
	for release, modules := range values {
		encoder.Uint32(release)
		encodeStringSet(encoder, modules)
	}
}

func TestDecodeMgrMapExactCurrentBytes(t *testing.T) {
	data := encodeTestMgrMap(t, testMgrMapOptions{})
	want := []byte{14, 6}
	if !bytes.Equal(data[:2], want) {
		t.Fatalf("mgrmap prefix = %x, want %x", data[:2], want)
	}
}
