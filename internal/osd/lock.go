package osd

import (
	"fmt"
	"math"
	"net/netip"
	"time"

	wire "github.com/otuschhoff/go-librados/internal/encoding"
	"github.com/otuschhoff/go-librados/internal/protocol"
)

const (
	LockTypeNone      = uint8(0)
	LockTypeExclusive = uint8(1)
	LockTypeShared    = uint8(2)
	LockFlagMayRenew  = uint8(1)
)

type LockHolder struct {
	Client      uint64
	Cookie      string
	Address     string
	Description string
	Expiration  time.Time
}

type LockInfo struct {
	Holders  []LockHolder
	LockType uint8
	Tag      string
}

func EncodeLockRequest(name string, lockType uint8, cookie, tag, description string, duration time.Duration, renew bool, maxBytes uint32) ([]byte, error) {
	if name == "" || cookie == "" || duration < 0 || uint64(duration/time.Second) > math.MaxUint32 {
		return nil, wire.ErrMalformed
	}
	flags := uint8(0)
	if renew {
		flags = LockFlagMayRenew
	}
	encoder := wire.NewEncoder(maxBytes)
	encoder.Versioned(1, 1, func(payload *wire.Encoder) {
		payload.String(name)
		payload.Uint8(lockType)
		payload.String(cookie)
		payload.String(tag)
		payload.String(description)
		encodeDuration(payload, duration)
		payload.Uint8(flags)
	})
	return encoder.BytesResult()
}

func EncodeUnlockRequest(name, cookie string, maxBytes uint32) ([]byte, error) {
	if name == "" || cookie == "" {
		return nil, wire.ErrMalformed
	}
	encoder := wire.NewEncoder(maxBytes)
	encoder.Versioned(1, 1, func(payload *wire.Encoder) {
		payload.String(name)
		payload.String(cookie)
	})
	return encoder.BytesResult()
}

func EncodeBreakLockRequest(name string, client uint64, cookie string, maxBytes uint32) ([]byte, error) {
	if name == "" || cookie == "" {
		return nil, wire.ErrMalformed
	}
	encoder := wire.NewEncoder(maxBytes)
	encoder.Versioned(1, 1, func(payload *wire.Encoder) {
		payload.String(name)
		payload.Uint8(uint8(protocol.EntityClient))
		payload.Int64(int64(client))
		payload.String(cookie)
	})
	return encoder.BytesResult()
}

func EncodeGetLockInfoRequest(name string, maxBytes uint32) ([]byte, error) {
	if name == "" {
		return nil, wire.ErrMalformed
	}
	encoder := wire.NewEncoder(maxBytes)
	encoder.Versioned(1, 1, func(payload *wire.Encoder) { payload.String(name) })
	return encoder.BytesResult()
}

func DecodeLockHolders(data []byte, maxBytes, maxHolders uint32) ([]LockHolder, error) {
	info, err := DecodeLockInfo(data, maxBytes, maxHolders)
	return info.Holders, err
}

func DecodeLockInfo(data []byte, maxBytes, maxHolders uint32) (LockInfo, error) {
	decoder := wire.NewDecoder(data, wire.Limits{MaxBytes: maxBytes})
	version, payload := decoder.Versioned(1)
	if err := decoder.Finish(); err != nil || version != 1 {
		return LockInfo{}, wire.ErrUnsupportedVersion
	}
	count := payload.Uint32()
	if count > maxHolders || uint64(count)*13 > payload.Remaining() {
		return LockInfo{}, wire.ErrLimitExceeded
	}
	holders := make([]LockHolder, 0, count)
	for range count {
		holder, err := decodeLockHolder(payload)
		if err != nil {
			return LockInfo{}, err
		}
		holders = append(holders, holder)
	}
	lockType := payload.Uint8()
	tag := payload.String()
	if err := payload.Finish(); err != nil || payload.Remaining() != 0 {
		return LockInfo{}, ErrMalformedReply
	}
	if lockType != LockTypeNone && lockType != LockTypeExclusive && lockType != LockTypeShared || lockType == LockTypeNone && len(holders) != 0 {
		return LockInfo{}, ErrMalformedReply
	}
	return LockInfo{Holders: holders, LockType: lockType, Tag: tag}, nil
}

func decodeLockHolder(decoder *wire.Decoder) (LockHolder, error) {
	idVersion, id := decoder.Versioned(1)
	if err := decoder.Finish(); err != nil || idVersion != 1 || id.Uint8() != uint8(protocol.EntityClient) {
		return LockHolder{}, ErrMalformedReply
	}
	client := id.Int64()
	cookie := id.String()
	if err := id.Finish(); err != nil || id.Remaining() != 0 || client < 0 {
		return LockHolder{}, ErrMalformedReply
	}
	infoVersion, info := decoder.Versioned(1)
	if err := decoder.Finish(); err != nil || infoVersion != 1 {
		return LockHolder{}, ErrMalformedReply
	}
	seconds, nanoseconds := info.Uint32(), info.Uint32()
	if nanoseconds >= 1_000_000_000 {
		return LockHolder{}, ErrMalformedReply
	}
	address, err := protocol.DecodeEntityAddr(info)
	if err != nil {
		return LockHolder{}, err
	}
	description := info.String()
	if err := info.Finish(); err != nil || info.Remaining() != 0 {
		return LockHolder{}, ErrMalformedReply
	}
	addressText := ""
	if endpoint, ok := address.AddrPort(); ok && endpoint != (netip.AddrPort{}) {
		addressText = endpoint.String()
	}
	expiration := time.Time{}
	if seconds != 0 || nanoseconds != 0 {
		expiration = time.Unix(int64(seconds), int64(nanoseconds)).UTC()
	}
	return LockHolder{Client: uint64(client), Cookie: cookie, Address: addressText, Description: description, Expiration: expiration}, nil
}

func encodeDuration(encoder *wire.Encoder, duration time.Duration) {
	encoder.Uint32(uint32(duration / time.Second))
	encoder.Uint32(uint32(duration % time.Second))
}

func FormatClient(client uint64) string { return fmt.Sprintf("client.%d", client) }
