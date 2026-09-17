package maps

import (
	"errors"
	"net/netip"
	"testing"

	wire "github.com/otuschhoff/rados-go/internal/encoding"
	"github.com/otuschhoff/rados-go/internal/protocol"
)

var testMapLimits = Limits{
	MaxBytes:     16 << 10,
	MaxMonitors:  8,
	MaxAddresses: 4,
	MaxLocations: 4,
}

func TestDecodeMonMapV9(t *testing.T) {
	data := encodeTestMonMap(t, testMonMapOptions{})
	monMap, err := DecodeMonMap(data, testMapLimits)
	if err != nil {
		t.Fatal(err)
	}
	if monMap.Epoch() != 42 || monMap.MonitorCount() != 1 || monMap.MinimumMonitorRelease() != 20 {
		t.Fatalf("monmap summary: epoch=%d monitors=%d release=%d", monMap.Epoch(), monMap.MonitorCount(), monMap.MinimumMonitorRelease())
	}
	monitor, ok := monMap.Monitor("a")
	if !ok || monitor.Priority != 3 || monitor.Weight != 4 || monitor.Location["host"] != "node-a" {
		t.Fatalf("monitor = %+v, found=%t", monitor, ok)
	}
	endpoint, ok := monitor.Addresses[0].AddrPort()
	if !ok || endpoint != netip.MustParseAddrPort("192.0.2.10:3300") {
		t.Fatalf("monitor endpoint = %v, valid=%t", endpoint, ok)
	}
	if monMap.LastChanged() != (UTime{Seconds: 123, Nanoseconds: 456}) || monMap.Created() != (UTime{Seconds: 100, Nanoseconds: 200}) {
		t.Fatalf("timestamps = %+v %+v", monMap.LastChanged(), monMap.Created())
	}
	if monMap.PersistentFeatures() != 0x1234 || monMap.OptionalFeatures() != 0x40 || monMap.ElectionStrategy() != 3 || !monMap.StretchModeEnabled() || monMap.TiebreakerMonitor() != "a" {
		t.Fatalf("modern fields were not decoded")
	}

	monitor.Addresses[0].SocketData[0] ^= 0xff
	monitor.Location["host"] = "changed"
	ranks := monMap.Ranks()
	ranks[0] = "changed"
	next, _ := monMap.Monitor("a")
	if next.Location["host"] != "node-a" || next.Addresses[0].SocketData[0] == monitor.Addresses[0].SocketData[0] || monMap.Ranks()[0] != "a" {
		t.Fatal("caller mutation changed immutable monmap state")
	}
}

func TestDecodeMonMapAcceptsCompatibleExtension(t *testing.T) {
	data := encodeTestMonMap(t, testMonMapOptions{version: 10, outerTail: []byte{1, 2, 3}, monitorVersion: 6, monitorTail: []byte{4, 5}})
	monMap, err := DecodeMonMap(data, testMapLimits)
	if err != nil {
		t.Fatal(err)
	}
	if monMap.Epoch() != 42 {
		t.Fatalf("epoch = %d", monMap.Epoch())
	}
}

func TestDecodeMonitorFeaturesRejectsTruncatedPayload(t *testing.T) {
	decoder := wire.NewDecoder([]byte{1, 1, 7, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7}, wire.Limits{MaxBytes: 64})
	if _, err := decodeMonitorFeatures(decoder); !errors.Is(err, wire.ErrMalformed) {
		t.Fatalf("error = %v, want %v", err, wire.ErrMalformed)
	}
}

func TestDecodeMonMapRejectsInvalidInput(t *testing.T) {
	valid := encodeTestMonMap(t, testMonMapOptions{})
	tests := []struct {
		name   string
		data   []byte
		limits Limits
		want   error
	}{
		{name: "truncated", data: valid[:len(valid)-1], limits: testMapLimits, want: wire.ErrMalformed},
		{name: "trailing", data: append(append([]byte(nil), valid...), 1), limits: testMapLimits, want: ErrMalformedMap},
		{name: "monitor limit", data: valid, limits: Limits{MaxBytes: 16 << 10, MaxMonitors: 0, MaxAddresses: 4, MaxLocations: 4}, want: wire.ErrLimitExceeded},
		{name: "duplicate rank", data: encodeTestMonMap(t, testMonMapOptions{ranks: []string{"a", "a"}}), limits: testMapLimits, want: ErrMalformedMap},
		{name: "unknown rank", data: encodeTestMonMap(t, testMonMapOptions{ranks: []string{"b"}}), limits: testMapLimits, want: ErrMalformedMap},
		{name: "invalid timestamp", data: encodeTestMonMap(t, testMonMapOptions{nanoseconds: 1_000_000_000}), limits: testMapLimits, want: ErrMalformedMap},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := DecodeMonMap(test.data, test.limits)
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v (length=%d prefix=%x)", err, test.want, len(test.data), test.data[:min(6, len(test.data))])
			}
		})
	}
}

type testMonMapOptions struct {
	version        uint8
	outerTail      []byte
	monitorVersion uint8
	monitorTail    []byte
	ranks          []string
	nanoseconds    uint32
}

func encodeTestMonMap(t *testing.T, options testMonMapOptions) []byte {
	t.Helper()
	if options.version == 0 {
		options.version = 9
	}
	if options.monitorVersion == 0 {
		options.monitorVersion = 5
	}
	if options.ranks == nil {
		options.ranks = []string{"a"}
	}
	if options.nanoseconds == 0 {
		options.nanoseconds = 456
	}
	address, err := protocol.IPv4EntityAddr(protocol.AddressV2, 7, netip.MustParseAddrPort("192.0.2.10:3300"))
	if err != nil {
		t.Fatal(err)
	}
	encoder := wire.NewEncoder(testMapLimits.MaxBytes)
	encoder.Versioned(options.version, 6, func(payload *wire.Encoder) {
		payload.Raw([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})
		payload.Uint32(42)
		payload.Uint32(123)
		payload.Uint32(options.nanoseconds)
		payload.Uint32(100)
		payload.Uint32(200)
		encodeTestMonFeatures(payload, 0x1234)
		encodeTestMonFeatures(payload, 0x40)
		payload.Uint32(1)
		payload.String("a")
		payload.Versioned(options.monitorVersion, 1, func(info *wire.Encoder) {
			info.String("a")
			if err := (protocol.EntityAddrVec{address}).Encode(info, protocol.FeatureMessageAddress2|protocol.FeatureServerNautilus); err != nil {
				t.Fatal(err)
			}
			info.Uint16(3)
			info.Uint16(4)
			info.Uint32(1)
			info.String("host")
			info.String("node-a")
			info.Raw(options.monitorTail)
		})
		payload.Uint32(uint32(len(options.ranks)))
		for _, rank := range options.ranks {
			payload.String(rank)
		}
		payload.Uint8(20)
		payload.Uint32(1)
		payload.Uint32(2)
		payload.Uint8(3)
		payload.Uint32(1)
		payload.String("z")
		payload.Bool(true)
		payload.String("a")
		payload.Uint32(1)
		payload.String("z")
		payload.Raw(options.outerTail)
	})
	data, err := encoder.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func encodeTestMonFeatures(encoder *wire.Encoder, features uint64) {
	encoder.Versioned(1, 1, func(payload *wire.Encoder) {
		payload.Uint64(features)
	})
}
