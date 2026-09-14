package maps

import (
	"fmt"
	"reflect"

	wire "github.com/otuschhoff/go-librados/internal/encoding"
	"github.com/otuschhoff/go-librados/internal/protocol"
)

type OSDMap struct {
	fsid                FSID
	epoch               uint32
	created             UTime
	modified            UTime
	pools               map[int64]Pool
	nameToID            map[string]int64
	poolMax             int64
	flags               uint32
	maxOSD              int32
	osdState            []uint32
	osdWeight           []uint32
	clientAddresses     []protocol.EntityAddrVec
	pgTemp              map[PG][]int32
	primaryTemp         map[PG]int32
	primaryAffinity     []uint32
	crushData           []byte
	erasureCodeProfiles map[string]map[string]string
	pgUpmap             map[PG][]int32
	pgUpmapItems        map[PG][]OSDRemap
	crushVersion        uint32
	newRemovedSnapshots map[int64][]Interval
	newPurgedSnapshots  map[int64][]Interval
	lastUpChange        UTime
	lastInChange        UTime
	pgUpmapPrimaries    map[PG]int32
	crc                 uint32
	crcVerified         bool
	appliedIncremental  bool
}

type PG struct {
	Pool      uint64
	Seed      uint32
	Preferred int32
}

type OSDRemap struct {
	From int32
	To   int32
}

type Interval struct {
	Start  uint64
	Length uint64
}

const defaultPrimaryAffinity = uint32(0x10000)

func DecodeOSDMap(data []byte, limits Limits) (*OSDMap, error) {
	if err := validateOSDMapLimits(limits); err != nil {
		return nil, err
	}
	decoder := wire.NewDecoder(data, wire.Limits{MaxBytes: limits.MaxBytes})
	wrapperVersion, wrapper := decoder.Versioned(8)
	if err := decoder.Finish(); err != nil {
		return nil, err
	}
	if wrapperVersion < 8 {
		return nil, fmt.Errorf("%w: osdmap wrapper version %d", wire.ErrUnsupportedVersion, wrapperVersion)
	}

	clientVersion, client := wrapper.Versioned(10)
	if err := wrapper.Finish(); err != nil {
		return nil, err
	}
	if clientVersion < 10 {
		return nil, fmt.Errorf("%w: osdmap client version %d", wire.ErrUnsupportedVersion, clientVersion)
	}
	result, err := decodeOSDMapClient(client, limits)
	if err != nil {
		return nil, err
	}
	if err := client.Finish(); err != nil {
		return nil, err
	}

	_, extended := wrapper.Versioned(12)
	if err := wrapper.Finish(); err != nil {
		return nil, err
	}
	if err := extended.Finish(); err != nil {
		return nil, err
	}
	crcOffset := uint64(6) + wrapper.Position()
	result.crc = wrapper.Uint32()
	if err := wrapper.Finish(); err != nil {
		return nil, err
	}
	if crcOffset+4 > uint64(len(data)) {
		return nil, wire.ErrMalformed
	}
	actual := wire.CRC32C(^uint32(0), data[:crcOffset])
	actual = wire.CRC32C(actual, data[crcOffset+4:])
	if actual != result.crc {
		return nil, fmt.Errorf("%w: osdmap crc got %#08x want %#08x", ErrMalformedMap, result.crc, actual)
	}
	result.crcVerified = true
	if err := decoder.Finish(); err != nil {
		return nil, err
	}
	if decoder.Remaining() != 0 {
		return nil, fmt.Errorf("%w: trailing osdmap bytes", ErrMalformedMap)
	}
	return result, nil
}

func decodeOSDMapClient(decoder *wire.Decoder, limits Limits) (*OSDMap, error) {
	result := &OSDMap{pools: make(map[int64]Pool), nameToID: make(map[string]int64)}
	copy(result.fsid[:], decoder.Raw(16))
	result.epoch = decoder.Uint32()
	result.created = decodeUTime(decoder)
	result.modified = decodeUTime(decoder)

	poolCount, err := boundedCount(decoder, limits.MaxPools, 14)
	if err != nil {
		return nil, err
	}
	for range poolCount {
		id := decoder.Int64()
		pool, err := decodePool(decoder, limits)
		if err != nil {
			return nil, err
		}
		if _, exists := result.pools[id]; exists {
			return nil, fmt.Errorf("%w: duplicate pool id %d", ErrMalformedMap, id)
		}
		pool.id = id
		result.pools[id] = pool
	}

	nameCount, err := boundedCount(decoder, limits.MaxPools, 12)
	if err != nil {
		return nil, err
	}
	for range nameCount {
		id, name := decoder.Int64(), decoder.String()
		pool, exists := result.pools[id]
		if !exists {
			return nil, fmt.Errorf("%w: name for unknown pool %d", ErrMalformedMap, id)
		}
		if _, exists := result.nameToID[name]; exists {
			return nil, fmt.Errorf("%w: duplicate pool name %q", ErrMalformedMap, name)
		}
		pool.name = name
		result.pools[id] = pool
		result.nameToID[name] = id
	}
	if len(result.nameToID) != len(result.pools) {
		return nil, fmt.Errorf("%w: missing pool names", ErrMalformedMap)
	}

	result.poolMax = int64(decoder.Int32())
	result.flags = decoder.Uint32()
	result.maxOSD = decoder.Int32()
	result.osdState, err = decodeUint32Vector(decoder, limits.MaxOSDs)
	if err != nil {
		return nil, err
	}
	result.osdWeight, err = decodeUint32Vector(decoder, limits.MaxOSDs)
	if err != nil {
		return nil, err
	}
	result.clientAddresses, err = decodeAddressVector(decoder, limits)
	if err != nil {
		return nil, err
	}
	result.pgTemp, err = decodePGVectorMap(decoder, limits)
	if err != nil {
		return nil, err
	}
	result.primaryTemp, err = decodePGIntMap(decoder, limits)
	if err != nil {
		return nil, err
	}
	result.primaryAffinity, err = decodeUint32Vector(decoder, limits.MaxOSDs)
	if err != nil {
		return nil, err
	}
	if result.maxOSD < 0 || uint32(result.maxOSD) > limits.MaxOSDs || len(result.osdState) != int(result.maxOSD) || len(result.osdWeight) != int(result.maxOSD) || len(result.clientAddresses) != int(result.maxOSD) || (len(result.primaryAffinity) != 0 && len(result.primaryAffinity) != int(result.maxOSD)) {
		return nil, fmt.Errorf("%w: inconsistent osd vectors for max_osd %d", ErrMalformedMap, result.maxOSD)
	}
	result.crushData = decoder.Bytes()
	result.erasureCodeProfiles, err = decodeNestedStringMap(decoder, limits.MaxCollectionEntries)
	if err != nil {
		return nil, err
	}
	result.pgUpmap, err = decodePGVectorMap(decoder, limits)
	if err != nil {
		return nil, err
	}
	result.pgUpmapItems, err = decodePGRemapItems(decoder, limits)
	if err != nil {
		return nil, err
	}
	result.crushVersion = decoder.Uint32()
	result.newRemovedSnapshots, err = decodePoolIntervalMap(decoder, limits)
	if err != nil {
		return nil, err
	}
	result.newPurgedSnapshots, err = decodePoolIntervalMap(decoder, limits)
	if err != nil {
		return nil, err
	}
	result.lastUpChange = decodeUTime(decoder)
	result.lastInChange = decodeUTime(decoder)
	result.pgUpmapPrimaries, err = decodePGIntMap(decoder, limits)
	if err != nil {
		return nil, err
	}
	return result, decoder.Finish()
}

func validateOSDMapLimits(limits Limits) error {
	if limits.MaxBytes == 0 || limits.MaxPools == 0 || limits.MaxOSDs == 0 || limits.MaxAddresses == 0 || limits.MaxPGMappings == 0 || limits.MaxCollectionEntries == 0 {
		return wire.ErrLimitExceeded
	}
	return nil
}

func decodeUint32Vector(decoder *wire.Decoder, maximum uint32) ([]uint32, error) {
	count, err := boundedCount(decoder, maximum, 4)
	if err != nil {
		return nil, err
	}
	result := make([]uint32, count)
	for index := range result {
		result[index] = decoder.Uint32()
	}
	return result, decoder.Finish()
}

func decodeAddressVector(decoder *wire.Decoder, limits Limits) ([]protocol.EntityAddrVec, error) {
	count, err := boundedCount(decoder, limits.MaxOSDs, 5)
	if err != nil {
		return nil, err
	}
	result := make([]protocol.EntityAddrVec, 0, count)
	for range count {
		addresses, err := protocol.DecodeEntityAddrVec(decoder, limits.MaxAddresses)
		if err != nil {
			return nil, err
		}
		result = append(result, addresses)
	}
	return result, decoder.Finish()
}

func decodePG(decoder *wire.Decoder) (PG, error) {
	if decoder.Uint8() != 1 {
		return PG{}, fmt.Errorf("%w: unsupported pg identifier version", wire.ErrUnsupportedVersion)
	}
	result := PG{Pool: decoder.Uint64(), Seed: decoder.Uint32(), Preferred: decoder.Int32()}
	return result, decoder.Finish()
}

func decodePGVectorMap(decoder *wire.Decoder, limits Limits) (map[PG][]int32, error) {
	count, err := boundedCount(decoder, limits.MaxPGMappings, 21)
	if err != nil {
		return nil, err
	}
	result := make(map[PG][]int32, count)
	for range count {
		pg, err := decodePG(decoder)
		if err != nil {
			return nil, err
		}
		inner, err := boundedCount(decoder, limits.MaxOSDs, 4)
		if err != nil {
			return nil, err
		}
		values := make([]int32, inner)
		for index := range values {
			values[index] = decoder.Int32()
		}
		if _, exists := result[pg]; exists {
			return nil, fmt.Errorf("%w: duplicate pg mapping", ErrMalformedMap)
		}
		result[pg] = values
	}
	return result, decoder.Finish()
}

func decodePGIntMap(decoder *wire.Decoder, limits Limits) (map[PG]int32, error) {
	count, err := boundedCount(decoder, limits.MaxPGMappings, 21)
	if err != nil {
		return nil, err
	}
	result := make(map[PG]int32, count)
	for range count {
		pg, err := decodePG(decoder)
		if err != nil {
			return nil, err
		}
		value := decoder.Int32()
		if _, exists := result[pg]; exists {
			return nil, fmt.Errorf("%w: duplicate pg mapping", ErrMalformedMap)
		}
		result[pg] = value
	}
	return result, decoder.Finish()
}

func decodePGRemapItems(decoder *wire.Decoder, limits Limits) (map[PG][]OSDRemap, error) {
	count, err := boundedCount(decoder, limits.MaxPGMappings, 21)
	if err != nil {
		return nil, err
	}
	result := make(map[PG][]OSDRemap, count)
	for range count {
		pg, err := decodePG(decoder)
		if err != nil {
			return nil, err
		}
		inner, err := boundedCount(decoder, limits.MaxOSDs, 8)
		if err != nil {
			return nil, err
		}
		values := make([]OSDRemap, inner)
		for index := range values {
			values[index] = OSDRemap{From: decoder.Int32(), To: decoder.Int32()}
		}
		if _, exists := result[pg]; exists {
			return nil, fmt.Errorf("%w: duplicate pg remap", ErrMalformedMap)
		}
		result[pg] = values
	}
	return result, decoder.Finish()
}

func decodePoolIntervalMap(decoder *wire.Decoder, limits Limits) (map[int64][]Interval, error) {
	count, err := boundedCount(decoder, limits.MaxPools, 12)
	if err != nil {
		return nil, err
	}
	result := make(map[int64][]Interval, count)
	for range count {
		poolID := decoder.Int64()
		intervals, err := decodeIntervals(decoder, limits.MaxCollectionEntries)
		if err != nil {
			return nil, err
		}
		if _, exists := result[poolID]; exists {
			return nil, fmt.Errorf("%w: duplicate pool interval set", ErrMalformedMap)
		}
		result[poolID] = intervals
	}
	return result, decoder.Finish()
}

func (osdMap *OSDMap) FSID() FSID               { return osdMap.fsid }
func (osdMap *OSDMap) Epoch() uint32            { return osdMap.epoch }
func (osdMap *OSDMap) CRC() uint32              { return osdMap.crc }
func (osdMap *OSDMap) CRCVerified() bool        { return osdMap.crcVerified }
func (osdMap *OSDMap) AppliedIncremental() bool { return osdMap.appliedIncremental }
func (osdMap *OSDMap) PoolCount() int           { return len(osdMap.pools) }
func (osdMap *OSDMap) PoolByID(id int64) (Pool, bool) {
	pool, ok := osdMap.pools[id]
	return clonePool(pool), ok
}
func (osdMap *OSDMap) PoolByName(name string) (Pool, bool) {
	id, ok := osdMap.nameToID[name]
	if !ok {
		return Pool{}, false
	}
	return osdMap.PoolByID(id)
}
func (osdMap *OSDMap) CrushData() []byte { return append([]byte(nil), osdMap.crushData...) }

// Equivalent reports whether two snapshots contain identical retained map
// state. CRC verification provenance is not part of map state.
func (osdMap *OSDMap) Equivalent(other *OSDMap) bool {
	if osdMap == nil || other == nil {
		return osdMap == other
	}
	left, right := cloneOSDMap(osdMap), cloneOSDMap(other)
	left.crcVerified = false
	right.crcVerified = false
	left.appliedIncremental = false
	right.appliedIncremental = false
	return reflect.DeepEqual(left, right)
}

func clonePool(pool Pool) Pool {
	pool.applicationMetadata = cloneNestedStringsMap(pool.applicationMetadata)
	pool.options = pool.Options()
	return pool
}

func cloneOSDMap(source *OSDMap) *OSDMap {
	result := *source
	result.pools = make(map[int64]Pool, len(source.pools))
	for id, pool := range source.pools {
		result.pools[id] = clonePool(pool)
	}
	result.nameToID = make(map[string]int64, len(source.nameToID))
	for name, id := range source.nameToID {
		result.nameToID[name] = id
	}
	result.osdState = append([]uint32(nil), source.osdState...)
	result.osdWeight = append([]uint32(nil), source.osdWeight...)
	result.primaryAffinity = append([]uint32(nil), source.primaryAffinity...)
	result.clientAddresses = make([]protocol.EntityAddrVec, len(source.clientAddresses))
	for index, addresses := range source.clientAddresses {
		result.clientAddresses[index] = cloneAddressVector(addresses)
	}
	result.pgTemp = clonePGVectors(source.pgTemp)
	result.primaryTemp = clonePGInts(source.primaryTemp)
	result.crushData = append([]byte(nil), source.crushData...)
	result.erasureCodeProfiles = cloneNestedStringsMap(source.erasureCodeProfiles)
	result.pgUpmap = clonePGVectors(source.pgUpmap)
	result.pgUpmapItems = clonePGRemaps(source.pgUpmapItems)
	result.newRemovedSnapshots = cloneIntervalMap(source.newRemovedSnapshots)
	result.newPurgedSnapshots = cloneIntervalMap(source.newPurgedSnapshots)
	result.pgUpmapPrimaries = clonePGInts(source.pgUpmapPrimaries)
	return &result
}

func cloneAddressVector(addresses protocol.EntityAddrVec) protocol.EntityAddrVec {
	return cloneAddresses(addresses)
}

func clonePGVectors(source map[PG][]int32) map[PG][]int32 {
	result := make(map[PG][]int32, len(source))
	for pg, values := range source {
		result[pg] = append([]int32(nil), values...)
	}
	return result
}

func clonePGInts(source map[PG]int32) map[PG]int32 {
	result := make(map[PG]int32, len(source))
	for pg, value := range source {
		result[pg] = value
	}
	return result
}

func clonePGRemaps(source map[PG][]OSDRemap) map[PG][]OSDRemap {
	result := make(map[PG][]OSDRemap, len(source))
	for pg, values := range source {
		result[pg] = append([]OSDRemap(nil), values...)
	}
	return result
}

func cloneIntervalMap(source map[int64][]Interval) map[int64][]Interval {
	result := make(map[int64][]Interval, len(source))
	for poolID, values := range source {
		result[poolID] = append([]Interval(nil), values...)
	}
	return result
}
