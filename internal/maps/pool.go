package maps

import (
	"fmt"
	"math"
	"sort"

	wire "github.com/otuschhoff/rados-go/internal/encoding"
)

type PoolOptionType int32

const (
	PoolOptionString PoolOptionType = iota
	PoolOptionInteger
	PoolOptionDouble
)

type PoolOption struct {
	Type    PoolOptionType
	String  string
	Integer int64
	Double  float64
}

type PoolSnapshot struct {
	ID        uint64
	Name      string
	Timestamp UTime
}

const (
	poolTypeReplicated                  = 1
	poolTypeErasure                     = 3
	poolFlagECOverwrites         uint64 = 1 << 2
	poolFlagSelfManagedSnapshots uint64 = 1 << 13
	poolFlagPoolSnapshots        uint64 = 1 << 14
	poolFlagECOptimizations      uint64 = 1 << 19
)

type Pool struct {
	id                  int64
	name                string
	poolType            uint8
	size                uint8
	minimumSize         uint8
	crushRule           uint8
	objectHash          uint8
	pgCount             uint32
	placementPGCount    uint32
	stripeWidth         uint32
	flags               uint64
	snapshotSequence    uint64
	snapshots           map[uint64]PoolSnapshot
	erasureCodeProfile  string
	nonprimaryShards    [2]uint64
	applicationMetadata map[string]map[string]string
	options             map[int32]PoolOption
}

func decodePool(decoder *wire.Decoder, limits Limits) (Pool, error) {
	version, payload := decoder.Versioned(32)
	if err := decoder.Finish(); err != nil {
		return Pool{}, err
	}
	if version < 5 {
		return Pool{}, fmt.Errorf("%w: pool version %d", wire.ErrUnsupportedVersion, version)
	}
	pool := Pool{
		poolType:         payload.Uint8(),
		size:             payload.Uint8(),
		crushRule:        payload.Uint8(),
		objectHash:       payload.Uint8(),
		pgCount:          payload.Uint32(),
		placementPGCount: payload.Uint32(),
	}
	payload.Uint32()
	payload.Uint32()
	payload.Uint32()
	pool.snapshotSequence = payload.Uint64()
	payload.Uint32()
	var err error
	pool.snapshots, err = decodePoolSnapshots(payload, limits)
	if err != nil {
		return Pool{}, err
	}
	if err := consumeIntervals(payload, limits.MaxCollectionEntries); err != nil {
		return Pool{}, err
	}
	payload.Uint64()
	if version >= 4 {
		pool.flags = payload.Uint64()
		payload.Uint32()
	}
	if version >= 7 {
		pool.minimumSize = payload.Uint8()
	} else {
		pool.minimumSize = pool.size - pool.size/2
	}
	if version >= 8 {
		payload.Uint64()
		payload.Uint64()
	}
	if version >= 9 {
		if err := consumeUint64Set(payload, limits.MaxCollectionEntries); err != nil {
			return Pool{}, err
		}
		payload.Int64()
		payload.Uint8()
		payload.Int64()
		payload.Int64()
	}
	if version >= 10 {
		if _, err := decodeStringMap(payload, limits.MaxCollectionEntries); err != nil {
			return Pool{}, err
		}
	}
	if version >= 11 {
		if err := skipVersioned(payload, 1); err != nil {
			return Pool{}, err
		}
		payload.Uint32()
		payload.Uint32()
	}
	if version >= 12 {
		pool.stripeWidth = payload.Uint32()
	}
	if version >= 13 {
		payload.Uint64()
		payload.Uint64()
		for range 4 {
			payload.Uint32()
		}
	}
	if version >= 14 {
		pool.erasureCodeProfile = payload.String()
	}
	if version >= 15 {
		payload.Uint32()
	}
	if version >= 16 {
		payload.Uint32()
	}
	if version >= 17 {
		payload.Uint64()
	}
	if version >= 19 {
		payload.Uint32()
	}
	if version >= 20 {
		payload.Uint32()
	}
	if version >= 21 {
		payload.Bool()
	}
	if version >= 22 {
		payload.Bool()
	}
	if version >= 23 {
		payload.Uint32()
		payload.Uint32()
	}
	if version >= 24 {
		var err error
		pool.options, err = decodePoolOptions(payload, limits)
		if err != nil {
			return Pool{}, err
		}
	}
	if version >= 25 {
		payload.Uint32()
	}
	if version >= 26 {
		var err error
		pool.applicationMetadata, err = decodeNestedStringMap(payload, limits.MaxCollectionEntries)
		if err != nil {
			return Pool{}, err
		}
	}
	if version >= 27 {
		decodeUTime(payload)
	}
	if version >= 28 {
		for range 6 {
			payload.Uint32()
		}
		payload.Uint8()
	}
	if version >= 29 {
		if err := skipVersioned(payload, 1); err != nil {
			return Pool{}, err
		}
	}
	if version == 30 {
		for range 3 {
			payload.Uint32()
		}
		payload.Int32()
	}
	if version >= 31 {
		present := payload.Bool()
		if present {
			for range 4 {
				payload.Uint32()
			}
		}
	}
	if version >= 32 {
		for index := range pool.nonprimaryShards {
			value, err := decodeUnsignedVarint(payload)
			if err != nil {
				return Pool{}, err
			}
			pool.nonprimaryShards[index] = value
		}
	}
	if err := payload.Finish(); err != nil {
		return Pool{}, err
	}
	return pool, nil
}

func decodePoolOptions(decoder *wire.Decoder, limits Limits) (map[int32]PoolOption, error) {
	version, payload := decoder.Versioned(2)
	if err := decoder.Finish(); err != nil {
		return nil, err
	}
	if version < 1 {
		return nil, fmt.Errorf("%w: pool options version %d", wire.ErrUnsupportedVersion, version)
	}
	count, err := boundedCount(payload, limits.MaxCollectionEntries, 8)
	if err != nil {
		return nil, err
	}
	result := make(map[int32]PoolOption, count)
	for range count {
		key := payload.Int32()
		optionType := PoolOptionType(payload.Int32())
		option := PoolOption{Type: optionType}
		switch optionType {
		case PoolOptionString:
			option.String = payload.String()
		case PoolOptionInteger:
			if version >= 2 {
				option.Integer = payload.Int64()
			} else {
				option.Integer = int64(payload.Int32())
			}
		case PoolOptionDouble:
			option.Double = math.Float64frombits(payload.Uint64())
		default:
			return nil, fmt.Errorf("%w: pool option type %d", wire.ErrUnsupportedVersion, optionType)
		}
		if _, exists := result[key]; exists {
			return nil, fmt.Errorf("%w: duplicate pool option %d", ErrMalformedMap, key)
		}
		result[key] = option
	}
	if err := payload.Finish(); err != nil {
		return nil, err
	}
	return result, nil
}

func decodePoolSnapshots(decoder *wire.Decoder, limits Limits) (map[uint64]PoolSnapshot, error) {
	count, err := boundedCount(decoder, limits.MaxCollectionEntries, 14)
	if err != nil {
		return nil, err
	}
	result := make(map[uint64]PoolSnapshot, count)
	names := make(map[string]struct{}, count)
	for range count {
		key := decoder.Uint64()
		version, payload := decoder.Versioned(2)
		if err := decoder.Finish(); err != nil {
			return nil, err
		}
		if version < 2 {
			return nil, fmt.Errorf("%w: pool snapshot version %d", wire.ErrUnsupportedVersion, version)
		}
		snapshot := PoolSnapshot{ID: payload.Uint64(), Timestamp: decodeUTime(payload), Name: payload.String()}
		if err := payload.Finish(); err != nil {
			return nil, err
		}
		if key != snapshot.ID {
			return nil, fmt.Errorf("%w: pool snapshot key %d != id %d", ErrMalformedMap, key, snapshot.ID)
		}
		if _, exists := result[key]; exists {
			return nil, fmt.Errorf("%w: duplicate pool snapshot id %d", ErrMalformedMap, key)
		}
		if _, exists := names[snapshot.Name]; exists {
			return nil, fmt.Errorf("%w: duplicate pool snapshot name %q", ErrMalformedMap, snapshot.Name)
		}
		result[key] = snapshot
		names[snapshot.Name] = struct{}{}
	}
	return result, decoder.Finish()
}

func consumeIntervals(decoder *wire.Decoder, maximum uint32) error {
	_, err := decodeIntervals(decoder, maximum)
	return err
}

func decodeIntervals(decoder *wire.Decoder, maximum uint32) ([]Interval, error) {
	count, err := boundedCount(decoder, maximum, 16)
	if err != nil {
		return nil, err
	}
	result := make([]Interval, count)
	for index := range result {
		result[index] = Interval{Start: decoder.Uint64(), Length: decoder.Uint64()}
	}
	return result, decoder.Finish()
}

func consumeUint64Set(decoder *wire.Decoder, maximum uint32) error {
	count, err := boundedCount(decoder, maximum, 8)
	if err != nil {
		return err
	}
	for range count {
		decoder.Uint64()
	}
	return decoder.Finish()
}

func decodeStringMap(decoder *wire.Decoder, maximum uint32) (map[string]string, error) {
	count, err := boundedCount(decoder, maximum, 8)
	if err != nil {
		return nil, err
	}
	result := make(map[string]string, count)
	for range count {
		key, value := decoder.String(), decoder.String()
		if _, exists := result[key]; exists {
			return nil, fmt.Errorf("%w: duplicate string key %q", ErrMalformedMap, key)
		}
		result[key] = value
	}
	return result, decoder.Finish()
}

func decodeNestedStringMap(decoder *wire.Decoder, maximum uint32) (map[string]map[string]string, error) {
	count, err := boundedCount(decoder, maximum, 8)
	if err != nil {
		return nil, err
	}
	result := make(map[string]map[string]string, count)
	for range count {
		key := decoder.String()
		value, err := decodeStringMap(decoder, maximum)
		if err != nil {
			return nil, err
		}
		if _, exists := result[key]; exists {
			return nil, fmt.Errorf("%w: duplicate string key %q", ErrMalformedMap, key)
		}
		result[key] = value
	}
	return result, decoder.Finish()
}

func skipVersioned(decoder *wire.Decoder, localVersion uint8) error {
	_, payload := decoder.Versioned(localVersion)
	if err := decoder.Finish(); err != nil {
		return err
	}
	return payload.Finish()
}

func decodeUnsignedVarint(decoder *wire.Decoder) (uint64, error) {
	var result uint64
	for index := 0; index < 10; index++ {
		value := decoder.Uint8()
		if err := decoder.Finish(); err != nil {
			return 0, err
		}
		if value&0x80 == 0 {
			if index == 9 && value > 1 {
				return 0, wire.ErrMalformed
			}
			return result | uint64(value)<<uint(7*index), nil
		}
		result |= uint64(value&0x7f) << uint(7*index)
	}
	return 0, wire.ErrMalformed
}

func (pool Pool) ID() int64                { return pool.id }
func (pool Pool) Name() string             { return pool.name }
func (pool Pool) Type() uint8              { return pool.poolType }
func (pool Pool) Size() uint8              { return pool.size }
func (pool Pool) MinimumSize() uint8       { return pool.minimumSize }
func (pool Pool) CrushRule() uint8         { return pool.crushRule }
func (pool Pool) ObjectHash() uint8        { return pool.objectHash }
func (pool Pool) PGCount() uint32          { return pool.pgCount }
func (pool Pool) PlacementPGCount() uint32 { return pool.placementPGCount }
func (pool Pool) StripeWidth() uint32      { return pool.stripeWidth }
func (pool Pool) Flags() uint64            { return pool.flags }
func (pool Pool) SnapshotSequence() uint64 { return pool.snapshotSequence }
func (pool Pool) UsesPoolSnapshots() bool  { return pool.flags&poolFlagPoolSnapshots != 0 }
func (pool Pool) UsesSelfManagedSnapshots() bool {
	return pool.flags&poolFlagSelfManagedSnapshots != 0
}
func (pool Pool) ErasureCodeProfile() string { return pool.erasureCodeProfile }
func (pool Pool) IsErasureCoded() bool       { return pool.poolType == poolTypeErasure }
func (pool Pool) AllowsECOverwrites() bool   { return pool.flags&poolFlagECOverwrites != 0 }
func (pool Pool) RequiresAlignment() bool {
	return pool.IsErasureCoded() && !pool.AllowsECOverwrites()
}
func (pool Pool) Snapshots() []PoolSnapshot {
	result := make([]PoolSnapshot, 0, len(pool.snapshots))
	for _, snapshot := range pool.snapshots {
		result = append(result, snapshot)
	}
	sort.Slice(result, func(left, right int) bool { return result[left].ID < result[right].ID })
	return result
}
func (pool Pool) ApplicationMetadata() map[string]map[string]string {
	return cloneNestedStringsMap(pool.applicationMetadata)
}
func (pool Pool) Options() map[int32]PoolOption {
	result := make(map[int32]PoolOption, len(pool.options))
	for key, value := range pool.options {
		result[key] = value
	}
	return result
}

func cloneNestedStringsMap(values map[string]map[string]string) map[string]map[string]string {
	result := make(map[string]map[string]string, len(values))
	for key, value := range values {
		result[key] = cloneStringsMap(value)
	}
	return result
}
