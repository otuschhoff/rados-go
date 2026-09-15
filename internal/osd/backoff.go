package osd

import (
	"fmt"
	"math"

	wire "github.com/otuschhoff/go-librados/internal/encoding"
	"github.com/otuschhoff/go-librados/internal/maps"
	"github.com/otuschhoff/go-librados/internal/msgr"
	"github.com/otuschhoff/go-librados/internal/protocol"
)

const (
	BackoffBlock    = uint8(1)
	BackoffAckBlock = uint8(2)
	BackoffUnblock  = uint8(3)
)

type HObject struct {
	Key       string
	Object    string
	Snapshot  uint64
	Hash      uint32
	Max       bool
	Namespace string
	Pool      int64
}

func (object HObject) IsMin() bool {
	return object.Snapshot == 0 && object.Hash == 0 && !object.Max && object.Pool == math.MinInt64
}

func (object HObject) IsMax() bool { return object.Max }

func MarshalHObject(object HObject, maxBytes uint32) ([]byte, error) {
	encoder := wire.NewEncoder(maxBytes)
	encodeHObject(encoder, object)
	return encoder.BytesResult()
}

func UnmarshalHObject(data []byte, maxBytes uint32) (HObject, error) {
	decoder := wire.NewDecoder(data, wire.Limits{MaxBytes: maxBytes})
	object, err := decodeHObject(decoder)
	if err != nil {
		return HObject{}, err
	}
	if err := decoder.Finish(); err != nil || decoder.Remaining() != 0 {
		return HObject{}, ErrMalformedReply
	}
	return object, nil
}

type Backoff struct {
	PG        maps.PG
	Shard     int8
	MapEpoch  uint32
	Operation uint8
	ID        uint64
	Begin     HObject
	End       HObject
}

func DecodeBackoff(message msgr.Message, limits Limits) (Backoff, error) {
	if limits.MaxBytes == 0 || message.Header.Type != protocol.MessageOSDBackoff || message.Header.Version != 1 || message.Header.CompatVersion > 1 || len(message.Middle) != 0 || len(message.Data) != 0 || uint64(len(message.Front)) > uint64(limits.MaxBytes) {
		return Backoff{}, ErrMalformedReply
	}
	decoder := wire.NewDecoder(message.Front, wire.Limits{MaxBytes: limits.MaxBytes})
	pg, shard, err := decodeSPG(decoder)
	if err != nil {
		return Backoff{}, err
	}
	backoff := Backoff{PG: pg, Shard: shard, MapEpoch: decoder.Uint32(), Operation: decoder.Uint8(), ID: decoder.Uint64()}
	if backoff.Operation != BackoffBlock && backoff.Operation != BackoffUnblock {
		return Backoff{}, fmt.Errorf("%w: backoff operation %d", wire.ErrUnsupportedVersion, backoff.Operation)
	}
	backoff.Begin, err = decodeHObject(decoder)
	if err != nil {
		return Backoff{}, err
	}
	backoff.End, err = decodeHObject(decoder)
	if err != nil {
		return Backoff{}, err
	}
	if err := decoder.Finish(); err != nil || decoder.Remaining() != 0 {
		return Backoff{}, ErrMalformedReply
	}
	return backoff, nil
}

func EncodeBackoffAcknowledgment(backoff Backoff, limits Limits) (msgr.Message, error) {
	if limits.MaxBytes == 0 || backoff.Operation != BackoffBlock {
		return msgr.Message{}, wire.ErrMalformed
	}
	encoder := wire.NewEncoder(limits.MaxBytes)
	encodeSPGWithShard(encoder, backoff.PG, backoff.Shard)
	encoder.Uint32(backoff.MapEpoch)
	encoder.Uint8(BackoffAckBlock)
	encoder.Uint64(backoff.ID)
	encodeHObject(encoder, backoff.Begin)
	encodeHObject(encoder, backoff.End)
	front, err := encoder.BytesResult()
	if err != nil {
		return msgr.Message{}, err
	}
	return msgr.Message{Header: msgr.MessageHeader{Type: protocol.MessageOSDBackoff, Version: 1, CompatVersion: 1}, Front: front, Lengths: msgr.MessageLengths{Front: uint32(len(front))}}, nil
}

func (backoff Backoff) Contains(target HObject) bool {
	if CompareHObject(backoff.Begin, backoff.End) == 0 {
		return CompareHObject(target, backoff.Begin) == 0
	}
	return CompareHObject(backoff.Begin, target) <= 0 && CompareHObject(target, backoff.End) < 0
}

func decodeSPG(decoder *wire.Decoder) (maps.PG, int8, error) {
	version, payload := decoder.Versioned(1)
	if err := decoder.Finish(); err != nil || version != 1 {
		return maps.PG{}, 0, wire.ErrUnsupportedVersion
	}
	pg, err := decodePG(payload)
	if err != nil {
		return maps.PG{}, 0, err
	}
	shard := int8(payload.Uint8())
	if err := payload.Finish(); err != nil || payload.Remaining() != 0 {
		return maps.PG{}, 0, ErrMalformedReply
	}
	return pg, shard, nil
}

func decodeHObject(decoder *wire.Decoder) (HObject, error) {
	version, payload := decoder.Versioned(4)
	if err := decoder.Finish(); err != nil || version < 3 || version > 4 {
		return HObject{}, wire.ErrUnsupportedVersion
	}
	object := HObject{Key: payload.String(), Object: payload.String(), Snapshot: payload.Uint64(), Hash: payload.Uint32(), Max: payload.Bool()}
	if version >= 4 {
		object.Namespace = payload.String()
		object.Pool = payload.Int64()
	}
	if err := payload.Finish(); err != nil || payload.Remaining() != 0 {
		return HObject{}, ErrMalformedReply
	}
	return object, nil
}

func encodeHObject(encoder *wire.Encoder, object HObject) {
	encoder.Versioned(4, 3, func(payload *wire.Encoder) {
		payload.String(object.Key)
		payload.String(object.Object)
		payload.Uint64(object.Snapshot)
		payload.Uint32(object.Hash)
		payload.Bool(object.Max)
		payload.String(object.Namespace)
		payload.Int64(object.Pool)
	})
}

func encodeSPGWithShard(encoder *wire.Encoder, pg maps.PG, shard int8) {
	encoder.Versioned(1, 1, func(payload *wire.Encoder) {
		encodePG(payload, pg)
		payload.Uint8(uint8(shard))
	})
}

func CompareHObject(left, right HObject) int {
	if left.Max && right.Max {
		return 0
	}
	if result := compareBool(left.Max, right.Max); result != 0 {
		return result
	}
	if result := compareOrdered(left.Pool, right.Pool); result != 0 {
		return result
	}
	if result := compareOrdered(ReverseBits(left.Hash), ReverseBits(right.Hash)); result != 0 {
		return result
	}
	if result := compareOrdered(left.Namespace, right.Namespace); result != 0 {
		return result
	}
	if left.Key != "" || right.Key != "" {
		if result := compareOrdered(effectiveKey(left), effectiveKey(right)); result != 0 {
			return result
		}
	}
	if result := compareOrdered(left.Object, right.Object); result != 0 {
		return result
	}
	return compareOrdered(left.Snapshot, right.Snapshot)
}

func compareHObject(left, right HObject) int { return CompareHObject(left, right) }

func effectiveKey(object HObject) string {
	if object.Key != "" {
		return object.Key
	}
	return object.Object
}

func compareBool(left, right bool) int {
	if left == right {
		return 0
	}
	if !left {
		return -1
	}
	return 1
}

func compareOrdered[T ~int64 | ~uint32 | ~uint64 | ~string](left, right T) int {
	if left < right {
		return -1
	}
	if left > right {
		return 1
	}
	return 0
}

func ReverseBits(value uint32) uint32 {
	value = value>>1&0x55555555 | value&0x55555555<<1
	value = value>>2&0x33333333 | value&0x33333333<<2
	value = value>>4&0x0f0f0f0f | value&0x0f0f0f0f<<4
	value = value>>8&0x00ff00ff | value&0x00ff00ff<<8
	return value>>16 | value<<16
}
