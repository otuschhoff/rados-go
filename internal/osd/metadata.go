package osd

import (
	"bytes"
	"fmt"
	"math"
	"sort"

	wire "github.com/otuschhoff/go-librados/internal/encoding"
)

type MetadataEntry struct {
	Key   []byte
	Value []byte
}

func EncodeOMAPListRequest(after []byte, limit uint64, maxBytes uint32) ([]byte, error) {
	encoder := wire.NewEncoder(maxBytes)
	encoder.Bytes(after)
	encoder.Uint64(limit)
	encoder.Bytes(nil)
	return encoder.BytesResult()
}

func EncodeMetadataMap(entries []MetadataEntry, maxBytes uint32) ([]byte, error) {
	if len(entries) > math.MaxUint32 {
		return nil, wire.ErrLimitExceeded
	}
	owned := make([]MetadataEntry, len(entries))
	for index, entry := range entries {
		owned[index] = MetadataEntry{Key: append([]byte(nil), entry.Key...), Value: append([]byte(nil), entry.Value...)}
	}
	sort.Slice(owned, func(left, right int) bool { return bytes.Compare(owned[left].Key, owned[right].Key) < 0 })
	encoder := wire.NewEncoder(maxBytes)
	encoder.Uint32(uint32(len(owned)))
	for index, entry := range owned {
		if index > 0 && bytes.Equal(owned[index-1].Key, entry.Key) {
			return nil, fmt.Errorf("%w: duplicate metadata key", wire.ErrMalformed)
		}
		encoder.Bytes(entry.Key)
		encoder.Bytes(entry.Value)
	}
	return encoder.BytesResult()
}

func EncodeMetadataKeys(keys [][]byte, maxBytes uint32) ([]byte, error) {
	if len(keys) > math.MaxUint32 {
		return nil, wire.ErrLimitExceeded
	}
	owned := make([][]byte, len(keys))
	for index, key := range keys {
		owned[index] = append([]byte(nil), key...)
	}
	sort.Slice(owned, func(left, right int) bool { return bytes.Compare(owned[left], owned[right]) < 0 })
	encoder := wire.NewEncoder(maxBytes)
	encoder.Uint32(uint32(len(owned)))
	for index, key := range owned {
		if index > 0 && bytes.Equal(owned[index-1], key) {
			return nil, fmt.Errorf("%w: duplicate metadata key", wire.ErrMalformed)
		}
		encoder.Bytes(key)
	}
	return encoder.BytesResult()
}

func EncodeOMAPRange(begin, end []byte, maxBytes uint32) ([]byte, error) {
	if bytes.Compare(begin, end) >= 0 {
		return nil, wire.ErrMalformed
	}
	encoder := wire.NewEncoder(maxBytes)
	encoder.Bytes(begin)
	encoder.Bytes(end)
	return encoder.BytesResult()
}

func EncodeOMAPCompare(key, value []byte, comparison int32, maxBytes uint32) ([]byte, error) {
	encoder := wire.NewEncoder(maxBytes)
	encoder.Uint32(1)
	encoder.Bytes(key)
	encoder.Bytes(value)
	encoder.Int32(comparison)
	return encoder.BytesResult()
}

func DecodeMetadataMap(data []byte, maxBytes, maxEntries uint32) ([]MetadataEntry, error) {
	decoder := wire.NewDecoder(data, wire.Limits{MaxBytes: maxBytes})
	entries, err := decodeMetadataMap(decoder, maxEntries)
	if err != nil {
		return nil, err
	}
	if decoder.Remaining() != 0 {
		return nil, fmt.Errorf("%w: trailing metadata bytes", wire.ErrMalformed)
	}
	return entries, nil
}

func DecodeOMAPPage(data []byte, maxBytes, maxEntries uint32) ([]MetadataEntry, bool, error) {
	decoder := wire.NewDecoder(data, wire.Limits{MaxBytes: maxBytes})
	entries, err := decodeMetadataMap(decoder, maxEntries)
	if err != nil {
		return nil, false, err
	}
	more := decoder.Bool()
	if err := decoder.Finish(); err != nil {
		return nil, false, err
	}
	if decoder.Remaining() != 0 {
		return nil, false, fmt.Errorf("%w: trailing OMAP page bytes", wire.ErrMalformed)
	}
	return entries, more, nil
}

func decodeMetadataMap(decoder *wire.Decoder, maxEntries uint32) ([]MetadataEntry, error) {
	count := decoder.Uint32()
	if err := decoder.Finish(); err != nil {
		return nil, err
	}
	if count > maxEntries || uint64(count)*8 > decoder.Remaining() {
		return nil, wire.ErrLimitExceeded
	}
	entries := make([]MetadataEntry, 0, count)
	seen := make(map[string]struct{}, count)
	for range count {
		entry := MetadataEntry{Key: decoder.Bytes(), Value: decoder.Bytes()}
		if err := decoder.Finish(); err != nil {
			return nil, err
		}
		key := string(entry.Key)
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("%w: duplicate metadata key", wire.ErrMalformed)
		}
		if len(entries) > 0 && bytes.Compare(entries[len(entries)-1].Key, entry.Key) >= 0 {
			return nil, fmt.Errorf("%w: unordered metadata key", wire.ErrMalformed)
		}
		seen[key] = struct{}{}
		entries = append(entries, entry)
	}
	return entries, nil
}
