package osd

import (
	"errors"
	"net/netip"
	"testing"
	"time"

	wire "github.com/otuschhoff/go-librados/internal/encoding"
	"github.com/otuschhoff/go-librados/internal/protocol"
)

func TestEncodeLockRequest(t *testing.T) {
	data, err := EncodeLockRequest("name", LockTypeShared, "cookie", "tag", "description", 3*time.Second+4*time.Nanosecond, true, 4096)
	if err != nil {
		t.Fatal(err)
	}
	decoder := wire.NewDecoder(data, wire.Limits{MaxBytes: 4096})
	version, payload := decoder.Versioned(1)
	if version != 1 || payload.String() != "name" || payload.Uint8() != LockTypeShared || payload.String() != "cookie" || payload.String() != "tag" || payload.String() != "description" || payload.Uint32() != 3 || payload.Uint32() != 4 || payload.Uint8() != LockFlagMayRenew || payload.Remaining() != 0 {
		t.Fatal("lock request fields mismatch")
	}
	if _, err := EncodeLockRequest("", LockTypeExclusive, "cookie", "", "", 0, false, 4096); !errors.Is(err, wire.ErrMalformed) {
		t.Fatalf("invalid lock error=%v", err)
	}
}

func TestDecodeLockHolders(t *testing.T) {
	address, err := protocol.IPv4EntityAddr(protocol.AddressLegacy, 7, netip.MustParseAddrPort("192.0.2.8:6800"))
	if err != nil {
		t.Fatal(err)
	}
	encoder := wire.NewEncoder(4096)
	encoder.Versioned(1, 1, func(reply *wire.Encoder) {
		reply.Uint32(1)
		reply.Versioned(1, 1, func(id *wire.Encoder) {
			id.Uint8(uint8(protocol.EntityClient))
			id.Int64(42)
			id.String("cookie")
		})
		reply.Versioned(1, 1, func(info *wire.Encoder) {
			info.Uint32(9)
			info.Uint32(10)
			if err := address.Encode(info, protocol.FeatureMessageAddress2); err != nil {
				t.Fatal(err)
			}
			info.String("description")
		})
		reply.Uint8(LockTypeExclusive)
		reply.String("tag")
	})
	data, err := encoder.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	lockInfo, err := DecodeLockInfo(data, 4096, 2)
	if err != nil || len(lockInfo.Holders) != 1 || lockInfo.LockType != LockTypeExclusive || lockInfo.Tag != "tag" {
		t.Fatalf("lock info=%+v error=%v", lockInfo, err)
	}
	holder := lockInfo.Holders[0]
	if holder.Client != 42 || holder.Cookie != "cookie" || holder.Address != "192.0.2.8:6800" || holder.Description != "description" || !holder.Expiration.Equal(time.Unix(9, 10).UTC()) {
		t.Fatalf("holder=%+v", holder)
	}
	if _, err := DecodeLockHolders(data, 4096, 0); !errors.Is(err, wire.ErrLimitExceeded) {
		t.Fatalf("holder limit error=%v", err)
	}
}

func TestDecodeLockInfoRejectsInvalidMode(t *testing.T) {
	for _, test := range []struct {
		name     string
		holders  uint32
		lockType uint8
	}{
		{name: "unknown", lockType: 3},
		{name: "none with holder", holders: 1, lockType: LockTypeNone},
	} {
		t.Run(test.name, func(t *testing.T) {
			encoder := wire.NewEncoder(4096)
			encoder.Versioned(1, 1, func(reply *wire.Encoder) {
				reply.Uint32(test.holders)
				if test.holders != 0 {
					reply.Versioned(1, 1, func(id *wire.Encoder) {
						id.Uint8(uint8(protocol.EntityClient))
						id.Int64(42)
						id.String("cookie")
					})
					reply.Versioned(1, 1, func(info *wire.Encoder) {
						info.Uint32(0)
						info.Uint32(0)
						address, err := protocol.IPv4EntityAddr(protocol.AddressLegacy, 7, netip.MustParseAddrPort("192.0.2.8:6800"))
						if err != nil {
							t.Fatal(err)
						}
						if err := address.Encode(info, protocol.FeatureMessageAddress2); err != nil {
							t.Fatal(err)
						}
						info.String("")
					})
				}
				reply.Uint8(test.lockType)
				reply.String("")
			})
			data, err := encoder.BytesResult()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := DecodeLockInfo(data, 4096, 1); !errors.Is(err, ErrMalformedReply) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func FuzzDecodeLockHolders(f *testing.F) {
	address, err := protocol.IPv4EntityAddr(protocol.AddressLegacy, 7, netip.MustParseAddrPort("192.0.2.8:6800"))
	if err != nil {
		f.Fatal(err)
	}
	encoder := wire.NewEncoder(512)
	encoder.Versioned(1, 1, func(reply *wire.Encoder) {
		reply.Uint32(1)
		reply.Versioned(1, 1, func(id *wire.Encoder) {
			id.Uint8(uint8(protocol.EntityClient))
			id.Int64(42)
			id.String("cookie")
		})
		reply.Versioned(1, 1, func(info *wire.Encoder) {
			info.Uint32(9)
			info.Uint32(10)
			if err := address.Encode(info, protocol.FeatureMessageAddress2); err != nil {
				f.Fatal(err)
			}
			info.String("description")
		})
		reply.Uint8(LockTypeExclusive)
		reply.String("")
	})
	seed, err := encoder.BytesResult()
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeLockHolders(data, 4096, 64)
	})
}
