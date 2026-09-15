package maps

import (
	"encoding/binary"
	"errors"
	"net/netip"
	"testing"

	wire "github.com/otuschhoff/go-librados/internal/encoding"
	"github.com/otuschhoff/go-librados/internal/protocol"
)

var testOSDMapLimits = Limits{
	MaxBytes:             64 << 10,
	MaxPools:             16,
	MaxOSDs:              64,
	MaxAddresses:         8,
	MaxPGMappings:        128,
	MaxCollectionEntries: 128,
}

func TestDecodeOSDMapV8(t *testing.T) {
	data := encodeTestOSDMap(t, 11)
	osdMap, err := DecodeOSDMap(data, testOSDMapLimits)
	if err != nil {
		t.Fatal(err)
	}
	if osdMap.Epoch() != 11 || osdMap.PoolCount() != 1 || osdMap.FSID()[15] != 15 {
		t.Fatalf("osdmap epoch=%d pools=%d fsid=%x", osdMap.Epoch(), osdMap.PoolCount(), osdMap.FSID())
	}
	pool, ok := osdMap.PoolByName("data")
	if !ok || pool.ID() != 7 || pool.Type() != 1 || pool.Size() != 3 || pool.MinimumSize() != 2 || pool.PGCount() != 32 || pool.PlacementPGCount() != 16 || pool.ErasureCodeProfile() != "ec-profile" {
		t.Fatalf("pool = %+v found=%t", pool, ok)
	}
	metadata := pool.ApplicationMetadata()
	metadata["rados"]["key"] = "changed"
	again, _ := osdMap.PoolByID(7)
	if again.ApplicationMetadata()["rados"]["key"] != "value" {
		t.Fatal("caller mutation changed immutable pool metadata")
	}
	data[20] ^= 0xff
	if osdMap.Epoch() != 11 || again.Name() != "data" {
		t.Fatal("caller mutation changed decoded map")
	}
}

func TestDecodeOSDMapRejectsCRCAndLimits(t *testing.T) {
	valid := encodeTestOSDMap(t, 11)
	corrupt := append([]byte(nil), valid...)
	corrupt[20] ^= 0x80
	tests := []struct {
		name   string
		data   []byte
		limits Limits
		want   error
	}{
		{name: "crc", data: corrupt, limits: testOSDMapLimits, want: ErrMalformedMap},
		{name: "truncated", data: valid[:len(valid)-1], limits: testOSDMapLimits, want: wire.ErrMalformed},
		{name: "pool limit", data: valid, limits: Limits{MaxBytes: 64 << 10, MaxPools: 0, MaxOSDs: 64, MaxAddresses: 8, MaxPGMappings: 128, MaxCollectionEntries: 128}, want: wire.ErrLimitExceeded},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := DecodeOSDMap(test.data, test.limits); !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestOSDClientAddressesAreImmutable(t *testing.T) {
	address, err := protocol.IPv4EntityAddr(protocol.AddressV2, 9, netip.MustParseAddrPort("192.0.2.8:6800"))
	if err != nil {
		t.Fatal(err)
	}
	osdMap := &OSDMap{clientAddresses: []protocol.EntityAddrVec{{address}}}
	addresses, ok := osdMap.OSDClientAddresses(0)
	if !ok || len(addresses) != 1 {
		t.Fatalf("addresses=%v found=%t", addresses, ok)
	}
	addresses[0].SocketData[0] ^= 0xff
	again, ok := osdMap.OSDClientAddresses(0)
	if !ok || again[0].SocketData[0] == addresses[0].SocketData[0] {
		t.Fatal("caller mutation changed immutable OSD address")
	}
	for _, id := range []int32{-1, 1} {
		if _, ok := osdMap.OSDClientAddresses(id); ok {
			t.Fatalf("out-of-range OSD %d found", id)
		}
	}
}

func encodeTestOSDMap(t *testing.T, epoch uint32) []byte {
	return encodeTestOSDMapNamed(t, epoch, "data")
}

func encodeTestOSDMapNamed(t *testing.T, epoch uint32, poolName string) []byte {
	t.Helper()
	encoder := wire.NewEncoder(testOSDMapLimits.MaxBytes)
	encoder.Versioned(8, 7, func(wrapper *wire.Encoder) {
		wrapper.Versioned(10, 1, func(client *wire.Encoder) {
			client.Raw([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})
			client.Uint32(epoch)
			client.Raw(make([]byte, 16))
			client.Uint32(1)
			client.Int64(7)
			encodeTestPool(client)
			client.Uint32(1)
			client.Int64(7)
			client.String(poolName)
			client.Int32(7)
			client.Uint32(0)
			client.Int32(0)
			for range 3 {
				client.Uint32(0)
			}
			client.Uint32(0)
			client.Uint32(0)
			client.Uint32(0)
			client.Bytes([]byte("crush"))
			for range 3 {
				client.Uint32(0)
			}
			client.Uint32(9)
			client.Uint32(0)
			client.Uint32(0)
			client.Raw(make([]byte, 16))
			client.Uint32(0)
		})
		wrapper.Versioned(12, 1, func(*wire.Encoder) {})
		wrapper.Uint32(0)
	})
	data, err := encoder.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	crcOffset := len(data) - 4
	binary.LittleEndian.PutUint32(data[crcOffset:], wire.CRC32C(^uint32(0), data[:crcOffset]))
	return data
}

func encodeTestPool(encoder *wire.Encoder) {
	encoder.Versioned(32, 5, func(pool *wire.Encoder) {
		pool.Uint8(1)
		pool.Uint8(3)
		pool.Uint8(2)
		pool.Uint8(objectHashRJenkins)
		pool.Uint32(32)
		pool.Uint32(16)
		pool.Uint32(0)
		pool.Uint32(0)
		pool.Uint32(10)
		pool.Uint64(0)
		pool.Uint32(0)
		pool.Uint32(0)
		pool.Uint32(0)
		pool.Uint64(0)
		pool.Uint64(0x20)
		pool.Uint32(0)
		pool.Uint8(2)
		pool.Uint64(0)
		pool.Uint64(0)
		pool.Uint32(0)
		pool.Int64(-1)
		pool.Uint8(0)
		pool.Int64(-1)
		pool.Int64(-1)
		pool.Uint32(0)
		pool.Versioned(1, 1, func(*wire.Encoder) {})
		pool.Uint32(0)
		pool.Uint32(0)
		pool.Uint32(4096)
		pool.Uint64(0)
		pool.Uint64(0)
		for range 4 {
			pool.Uint32(0)
		}
		pool.String("ec-profile")
		pool.Uint32(0)
		pool.Uint32(0)
		pool.Uint64(0)
		pool.Uint32(0)
		pool.Uint32(0)
		pool.Bool(false)
		pool.Bool(true)
		pool.Uint32(0)
		pool.Uint32(1)
		pool.Versioned(2, 1, func(options *wire.Encoder) { options.Uint32(0) })
		pool.Uint32(0)
		pool.Uint32(1)
		pool.String("rados")
		pool.Uint32(1)
		pool.String("key")
		pool.String("value")
		pool.Raw(make([]byte, 8))
		for range 6 {
			pool.Uint32(0)
		}
		pool.Uint8(0)
		pool.Versioned(1, 1, func(*wire.Encoder) {})
		pool.Bool(false)
		pool.Uint8(0)
		pool.Uint8(0)
	})
}
