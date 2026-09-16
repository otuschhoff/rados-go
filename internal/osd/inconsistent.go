package osd

import (
	"fmt"
	"sort"

	wire "github.com/otuschhoff/go-librados/internal/encoding"
)

type InconsistentObject struct {
	Object    string
	Namespace string
	Locator   string
	Snapshot  uint64
	Shards    []int
	Errors    []string
}

func EncodeScrubList(interval uint32, start InconsistentObject, maximum uint64, maxBytes uint32) (Operation, error) {
	if maximum == 0 || maxBytes == 0 {
		return Operation{}, wire.ErrLimitExceeded
	}
	encoder := wire.NewEncoder(maxBytes)
	encoder.Versioned(1, 1, func(payload *wire.Encoder) {
		payload.Uint32(interval)
		payload.Uint32(0)
		payload.String(start.Object)
		payload.String(start.Namespace)
		payload.Uint64(start.Snapshot)
		payload.Uint64(maximum)
	})
	data, err := encoder.BytesResult()
	if err != nil {
		return Operation{}, err
	}
	return Operation{Code: OpScrubList, Data: data}, nil
}

func DecodeScrubList(data []byte, maxBytes, maxEntries uint32) (uint32, []InconsistentObject, error) {
	if maxBytes == 0 || maxEntries == 0 {
		return 0, nil, wire.ErrLimitExceeded
	}
	decoder := wire.NewDecoder(data, wire.Limits{MaxBytes: maxBytes})
	version, payload := decoder.Versioned(1)
	if err := decoder.Finish(); err != nil || version != 1 {
		return 0, nil, scrubDecodeError(err, wire.ErrUnsupportedVersion)
	}
	interval := payload.Uint32()
	count := payload.Uint32()
	if count > maxEntries {
		return 0, nil, wire.ErrLimitExceeded
	}
	objects := make([]InconsistentObject, count)
	for index := range objects {
		encoded := payload.Bytes()
		item, err := decodeInconsistentObject(encoded, maxBytes, maxEntries)
		if err != nil {
			return 0, nil, err
		}
		objects[index] = item
	}
	if err := payload.Finish(); err != nil || payload.Remaining() != 0 || decoder.Remaining() != 0 {
		return 0, nil, scrubDecodeError(err, wire.ErrMalformed)
	}
	return interval, objects, nil
}

func decodeInconsistentObject(data []byte, maxBytes, maxEntries uint32) (InconsistentObject, error) {
	decoder := wire.NewDecoder(data, wire.Limits{MaxBytes: maxBytes})
	version, payload := decoder.Versioned(2)
	if err := decoder.Finish(); err != nil || version != 2 {
		return InconsistentObject{}, scrubDecodeError(err, wire.ErrUnsupportedVersion)
	}
	objectErrors := payload.Uint64()
	objectVersion, object := payload.Versioned(1)
	if err := payload.Finish(); err != nil || objectVersion != 1 {
		return InconsistentObject{}, scrubDecodeError(err, wire.ErrUnsupportedVersion)
	}
	result := InconsistentObject{Object: object.String(), Namespace: object.String(), Locator: object.String(), Snapshot: object.Uint64()}
	if err := object.Finish(); err != nil || object.Remaining() != 0 {
		return InconsistentObject{}, scrubDecodeError(err, wire.ErrMalformed)
	}
	payload.Uint64()
	shardCount := payload.Uint32()
	if shardCount > maxEntries {
		return InconsistentObject{}, wire.ErrLimitExceeded
	}
	shardErrors := uint64(0)
	for range shardCount {
		shardVersion, shard := payload.Versioned(1)
		if err := payload.Finish(); err != nil || shardVersion != 1 {
			return InconsistentObject{}, scrubDecodeError(err, wire.ErrUnsupportedVersion)
		}
		osdID := shard.Int32()
		shard.Int8()
		if err := shard.Finish(); err != nil || shard.Remaining() != 0 {
			return InconsistentObject{}, scrubDecodeError(err, wire.ErrMalformed)
		}
		infoVersion, info := payload.Versioned(3)
		if err := payload.Finish(); err != nil || infoVersion != 3 {
			return InconsistentObject{}, scrubDecodeError(err, wire.ErrUnsupportedVersion)
		}
		errors := info.Uint64()
		shardErrors |= errors
		info.Bool()
		if errors&(1<<1) == 0 {
			attributes := info.Uint32()
			if attributes > maxEntries {
				return InconsistentObject{}, wire.ErrLimitExceeded
			}
			for range attributes {
				_ = info.String()
				info.Bytes()
			}
			info.Uint64()
			info.Bool()
			info.Uint32()
			info.Bool()
			info.Uint32()
			info.Bool()
		}
		if err := info.Finish(); err != nil || info.Remaining() != 0 {
			return InconsistentObject{}, scrubDecodeError(err, wire.ErrMalformed)
		}
		result.Shards = append(result.Shards, int(osdID))
	}
	shardErrors |= payload.Uint64()
	if err := payload.Finish(); err != nil || payload.Remaining() != 0 {
		return InconsistentObject{}, scrubDecodeError(err, wire.ErrMalformed)
	}
	result.Errors = append(errorNames(objectErrors, objectErrorNames), errorNames(shardErrors, shardErrorNames)...)
	sort.Ints(result.Shards)
	sort.Strings(result.Errors)
	return result, nil
}

var objectErrorNames = map[uint64]string{
	1 << 1: "object_info_inconsistency", 1 << 4: "data_digest_mismatch", 1 << 5: "omap_digest_mismatch",
	1 << 6: "size_mismatch", 1 << 7: "attr_value_mismatch", 1 << 8: "attr_name_mismatch",
	1 << 9: "snapset_inconsistency", 1 << 10: "hinfo_inconsistency", 1 << 11: "size_too_large",
}

var shardErrorNames = map[uint64]string{
	1 << 1: "shard_missing", 1 << 2: "shard_stat_error", 1 << 3: "shard_read_error",
	1 << 9: "data_digest_mismatch_info", 1 << 10: "omap_digest_mismatch_info", 1 << 11: "size_mismatch_info",
	1 << 12: "shard_ec_hash_mismatch", 1 << 13: "shard_ec_size_mismatch", 1 << 14: "info_missing",
	1 << 15: "info_corrupted", 1 << 16: "snapset_missing", 1 << 17: "snapset_corrupted",
	1 << 18: "object_size_info_mismatch", 1 << 19: "hinfo_missing", 1 << 20: "hinfo_corrupted",
}

func errorNames(bits uint64, names map[uint64]string) []string {
	result := make([]string, 0, len(names))
	known := uint64(0)
	for bit, name := range names {
		known |= bit
		if bits&bit != 0 {
			result = append(result, name)
		}
	}
	if unknown := bits &^ known; unknown != 0 {
		result = append(result, fmt.Sprintf("unknown_0x%x", unknown))
	}
	return result
}

func scrubDecodeError(actual, fallback error) error {
	if actual != nil {
		return actual
	}
	return fallback
}
