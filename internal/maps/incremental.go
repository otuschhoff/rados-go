package maps

import (
	"fmt"

	wire "github.com/otuschhoff/go-librados/internal/encoding"
	"github.com/otuschhoff/go-librados/internal/protocol"
)

type OSDMapIncremental struct {
	fsid                FSID
	epoch               uint32
	modified            UTime
	newPoolMax          int64
	newFlags            int32
	fullMap             []byte
	crushData           []byte
	newMaxOSD           int32
	newPools            map[int64]Pool
	newPoolNames        map[int64]string
	oldPools            []int64
	newUpClient         map[int32]protocol.EntityAddrVec
	newState            map[int32]uint32
	newWeight           map[int32]uint32
	newPGTemp           map[PG][]int32
	newPrimaryTemp      map[PG]int32
	newPrimaryAffinity  map[int32]uint32
	newErasureProfiles  map[string]map[string]string
	oldErasureProfiles  []string
	newPGUpmap          map[PG][]int32
	oldPGUpmap          []PG
	newPGUpmapItems     map[PG][]OSDRemap
	oldPGUpmapItems     []PG
	newRemovedSnapshots map[int64][]Interval
	newPurgedSnapshots  map[int64][]Interval
	newLastUpChange     UTime
	newLastInChange     UTime
	newPGUpmapPrimaries map[PG]int32
	oldPGUpmapPrimaries []PG
	incrementalCRC      uint32
	fullCRC             uint32
}

func DecodeOSDMapIncremental(data []byte, limits Limits) (*OSDMapIncremental, error) {
	if err := validateOSDMapLimits(limits); err != nil {
		return nil, err
	}
	decoder := wire.NewDecoder(data, wire.Limits{MaxBytes: limits.MaxBytes})
	wrapperVersion, wrapper := decoder.Versioned(8)
	if err := decoder.Finish(); err != nil {
		return nil, err
	}
	if wrapperVersion < 8 {
		return nil, fmt.Errorf("%w: incremental wrapper version %d", wire.ErrUnsupportedVersion, wrapperVersion)
	}
	clientVersion, client := wrapper.Versioned(9)
	if err := wrapper.Finish(); err != nil {
		return nil, err
	}
	if clientVersion < 9 {
		return nil, fmt.Errorf("%w: incremental client version %d", wire.ErrUnsupportedVersion, clientVersion)
	}
	result, err := decodeIncrementalClient(client, limits)
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
	result.incrementalCRC = wrapper.Uint32()
	result.fullCRC = wrapper.Uint32()
	if err := wrapper.Finish(); err != nil {
		return nil, err
	}
	if crcOffset+4 > uint64(len(data)) {
		return nil, wire.ErrMalformed
	}
	actual := wire.CRC32C(^uint32(0), data[:crcOffset])
	actual = wire.CRC32C(actual, data[crcOffset+4:])
	if actual != result.incrementalCRC {
		return nil, fmt.Errorf("%w: incremental crc got %#08x want %#08x", ErrMalformedMap, result.incrementalCRC, actual)
	}
	if err := decoder.Finish(); err != nil {
		return nil, err
	}
	if decoder.Remaining() != 0 {
		return nil, fmt.Errorf("%w: trailing incremental bytes", ErrMalformedMap)
	}
	return result, nil
}

func decodeIncrementalClient(decoder *wire.Decoder, limits Limits) (*OSDMapIncremental, error) {
	result := &OSDMapIncremental{newPools: make(map[int64]Pool), newPoolNames: make(map[int64]string)}
	copy(result.fsid[:], decoder.Raw(16))
	result.epoch = decoder.Uint32()
	result.modified = decodeUTime(decoder)
	result.newPoolMax = decoder.Int64()
	result.newFlags = decoder.Int32()
	result.fullMap = decoder.Bytes()
	result.crushData = decoder.Bytes()
	result.newMaxOSD = decoder.Int32()
	if result.newMaxOSD > int32(limits.MaxOSDs) {
		return nil, wire.ErrLimitExceeded
	}
	count, err := boundedCount(decoder, limits.MaxPools, 14)
	if err != nil {
		return nil, err
	}
	for range count {
		id := decoder.Int64()
		pool, err := decodePool(decoder, limits)
		if err != nil {
			return nil, err
		}
		if _, exists := result.newPools[id]; exists {
			return nil, fmt.Errorf("%w: duplicate new pool %d", ErrMalformedMap, id)
		}
		pool.id = id
		result.newPools[id] = pool
	}
	count, err = boundedCount(decoder, limits.MaxPools, 12)
	if err != nil {
		return nil, err
	}
	for range count {
		id, name := decoder.Int64(), decoder.String()
		if _, exists := result.newPoolNames[id]; exists {
			return nil, fmt.Errorf("%w: duplicate renamed pool %d", ErrMalformedMap, id)
		}
		result.newPoolNames[id] = name
	}
	count, err = boundedCount(decoder, limits.MaxPools, 8)
	if err != nil {
		return nil, err
	}
	seenOld := make(map[int64]struct{}, count)
	for range count {
		id := decoder.Int64()
		if _, exists := seenOld[id]; exists {
			return nil, fmt.Errorf("%w: duplicate removed pool %d", ErrMalformedMap, id)
		}
		seenOld[id] = struct{}{}
		result.oldPools = append(result.oldPools, id)
	}
	result.newUpClient, err = decodeIntAddressMap(decoder, limits)
	if err != nil {
		return nil, err
	}
	result.newState, err = decodeIntUintMap(decoder, limits.MaxOSDs)
	if err != nil {
		return nil, err
	}
	result.newWeight, err = decodeIntUintMap(decoder, limits.MaxOSDs)
	if err != nil {
		return nil, err
	}
	result.newPGTemp, err = decodePGVectorMap(decoder, limits)
	if err != nil {
		return nil, err
	}
	result.newPrimaryTemp, err = decodePGIntMap(decoder, limits)
	if err != nil {
		return nil, err
	}
	result.newPrimaryAffinity, err = decodeIntUintMap(decoder, limits.MaxOSDs)
	if err != nil {
		return nil, err
	}
	result.newErasureProfiles, err = decodeNestedStringMap(decoder, limits.MaxCollectionEntries)
	if err != nil {
		return nil, err
	}
	result.oldErasureProfiles, err = decodeStrings(decoder, limits.MaxCollectionEntries)
	if err != nil {
		return nil, err
	}
	result.newPGUpmap, err = decodePGVectorMap(decoder, limits)
	if err != nil {
		return nil, err
	}
	result.oldPGUpmap, err = decodePGSet(decoder, limits)
	if err != nil {
		return nil, err
	}
	result.newPGUpmapItems, err = decodePGRemapItems(decoder, limits)
	if err != nil {
		return nil, err
	}
	result.oldPGUpmapItems, err = decodePGSet(decoder, limits)
	if err != nil {
		return nil, err
	}
	result.newRemovedSnapshots, err = decodePoolIntervalMap(decoder, limits)
	if err != nil {
		return nil, err
	}
	result.newPurgedSnapshots, err = decodePoolIntervalMap(decoder, limits)
	if err != nil {
		return nil, err
	}
	result.newLastUpChange = decodeUTime(decoder)
	result.newLastInChange = decodeUTime(decoder)
	result.newPGUpmapPrimaries, err = decodePGIntMap(decoder, limits)
	if err != nil {
		return nil, err
	}
	result.oldPGUpmapPrimaries, err = decodePGSet(decoder, limits)
	if err != nil {
		return nil, err
	}
	return result, decoder.Finish()
}

func ApplyOSDMapIncremental(current *OSDMap, incremental *OSDMapIncremental, limits Limits) (*OSDMap, error) {
	if current == nil || incremental == nil {
		return nil, fmt.Errorf("%w: nil map or incremental", ErrMalformedMap)
	}
	if incremental.fsid != current.fsid {
		return nil, fmt.Errorf("%w: incremental fsid mismatch", ErrMalformedMap)
	}
	if incremental.epoch != current.epoch+1 {
		return nil, fmt.Errorf("%w: incremental epoch %d after %d", ErrMalformedMap, incremental.epoch, current.epoch)
	}
	if len(incremental.fullMap) != 0 {
		replacement, err := DecodeOSDMap(incremental.fullMap, limits)
		if err != nil {
			return nil, err
		}
		if replacement.fsid != current.fsid || replacement.epoch != incremental.epoch || replacement.crc != incremental.fullCRC {
			return nil, fmt.Errorf("%w: replacement full map identity or crc mismatch", ErrMalformedMap)
		}
		return replacement, nil
	}
	next := cloneOSDMap(current)
	next.epoch = incremental.epoch
	next.modified = incremental.modified
	next.crc = incremental.fullCRC
	next.crcVerified = false
	next.appliedIncremental = true
	if incremental.newPoolMax != -1 {
		next.poolMax = incremental.newPoolMax
	}
	if incremental.newFlags >= 0 {
		next.flags = uint32(incremental.newFlags)
	}
	if incremental.newMaxOSD >= 0 {
		next.maxOSD = incremental.newMaxOSD
		next.resizeOSDs(int(incremental.newMaxOSD))
	}
	if len(incremental.crushData) != 0 {
		next.crushData = append([]byte(nil), incremental.crushData...)
		next.crushVersion++
	}
	for id, pool := range incremental.newPools {
		if old, exists := next.pools[id]; exists {
			pool.name = old.name
		}
		next.pools[id] = clonePool(pool)
	}
	for id, name := range incremental.newPoolNames {
		pool, exists := next.pools[id]
		if !exists {
			return nil, fmt.Errorf("%w: rename for unknown pool %d", ErrMalformedMap, id)
		}
		if oldID, exists := next.nameToID[name]; exists && oldID != id {
			return nil, fmt.Errorf("%w: duplicate pool name %q", ErrMalformedMap, name)
		}
		delete(next.nameToID, pool.name)
		pool.name = name
		next.pools[id] = pool
		next.nameToID[name] = id
	}
	for _, id := range incremental.oldPools {
		if pool, exists := next.pools[id]; exists {
			delete(next.nameToID, pool.name)
			delete(next.pools, id)
		}
	}
	for osd, weight := range incremental.newWeight {
		if err := next.validateOSD(osd); err != nil {
			return nil, err
		}
		next.osdWeight[osd] = weight
		if weight != 0 {
			next.osdState[osd] &^= (1 << 2) | (1 << 3)
		}
	}
	for osd, affinity := range incremental.newPrimaryAffinity {
		if err := next.validateOSD(osd); err != nil {
			return nil, err
		}
		next.primaryAffinity[osd] = affinity
	}
	for osd, state := range incremental.newState {
		if err := next.validateOSD(osd); err != nil {
			return nil, err
		}
		if state == 0 {
			state = 1 << 1
		}
		if next.osdState[osd]&(1<<0) != 0 && state&(1<<0) != 0 {
			next.osdState[osd] = 0
			next.primaryAffinity[osd] = defaultPrimaryAffinity
			next.clientAddresses[osd] = nil
		} else {
			next.osdState[osd] ^= state
		}
	}
	for osd, addresses := range incremental.newUpClient {
		if err := next.validateOSD(osd); err != nil {
			return nil, err
		}
		next.osdState[osd] |= (1 << 0) | (1 << 1)
		next.osdState[osd] &^= 1 << 12
		next.clientAddresses[osd] = cloneAddressVector(addresses)
	}
	applyPGVectors(next.pgTemp, incremental.newPGTemp, true)
	applyPGInts(next.primaryTemp, incremental.newPrimaryTemp, -1)
	for _, name := range incremental.oldErasureProfiles {
		delete(next.erasureCodeProfiles, name)
	}
	for name, profile := range incremental.newErasureProfiles {
		next.erasureCodeProfiles[name] = cloneStringsMap(profile)
	}
	applyPGVectors(next.pgUpmap, incremental.newPGUpmap, false)
	for _, pg := range incremental.oldPGUpmap {
		delete(next.pgUpmap, pg)
	}
	for pg, items := range incremental.newPGUpmapItems {
		next.pgUpmapItems[pg] = append([]OSDRemap(nil), items...)
	}
	for _, pg := range incremental.oldPGUpmapItems {
		delete(next.pgUpmapItems, pg)
	}
	next.newRemovedSnapshots = cloneIntervalMap(incremental.newRemovedSnapshots)
	next.newPurgedSnapshots = cloneIntervalMap(incremental.newPurgedSnapshots)
	if incremental.newLastUpChange != (UTime{}) {
		next.lastUpChange = incremental.newLastUpChange
	}
	if incremental.newLastInChange != (UTime{}) {
		next.lastInChange = incremental.newLastInChange
	}
	for pg, primary := range incremental.newPGUpmapPrimaries {
		next.pgUpmapPrimaries[pg] = primary
	}
	for _, pg := range incremental.oldPGUpmapPrimaries {
		delete(next.pgUpmapPrimaries, pg)
	}
	for id, pool := range next.pools {
		if pool.name == "" {
			return nil, fmt.Errorf("%w: pool %d has no name", ErrMalformedMap, id)
		}
	}
	return next, nil
}

func (osdMap *OSDMap) resizeOSDs(size int) {
	osdMap.osdState = resizeUint32s(osdMap.osdState, size)
	osdMap.osdWeight = resizeUint32s(osdMap.osdWeight, size)
	if len(osdMap.primaryAffinity) != 0 {
		for len(osdMap.primaryAffinity) < size {
			osdMap.primaryAffinity = append(osdMap.primaryAffinity, defaultPrimaryAffinity)
		}
		osdMap.primaryAffinity = osdMap.primaryAffinity[:size]
	}
	if len(osdMap.clientAddresses) > size {
		osdMap.clientAddresses = osdMap.clientAddresses[:size]
	} else {
		osdMap.clientAddresses = append(osdMap.clientAddresses, make([]protocol.EntityAddrVec, size-len(osdMap.clientAddresses))...)
	}
}

func (osdMap *OSDMap) validateOSD(osd int32) error {
	if osd < 0 || int(osd) >= len(osdMap.osdState) {
		return fmt.Errorf("%w: osd %d outside map size", ErrMalformedMap, osd)
	}
	return nil
}

func resizeUint32s(values []uint32, size int) []uint32 {
	if len(values) > size {
		return values[:size]
	}
	return append(values, make([]uint32, size-len(values))...)
}

func applyPGVectors(target map[PG][]int32, changes map[PG][]int32, emptyDeletes bool) {
	for pg, values := range changes {
		if emptyDeletes && len(values) == 0 {
			delete(target, pg)
		} else {
			target[pg] = append([]int32(nil), values...)
		}
	}
}

func applyPGInts(target map[PG]int32, changes map[PG]int32, deleted int32) {
	for pg, value := range changes {
		if value == deleted {
			delete(target, pg)
		} else {
			target[pg] = value
		}
	}
}

func decodeIntAddressMap(decoder *wire.Decoder, limits Limits) (map[int32]protocol.EntityAddrVec, error) {
	count, err := boundedCount(decoder, limits.MaxOSDs, 9)
	if err != nil {
		return nil, err
	}
	result := make(map[int32]protocol.EntityAddrVec, count)
	for range count {
		osd := decoder.Int32()
		addresses, err := protocol.DecodeEntityAddrVec(decoder, limits.MaxAddresses)
		if err != nil {
			return nil, err
		}
		if _, exists := result[osd]; exists {
			return nil, fmt.Errorf("%w: duplicate osd address %d", ErrMalformedMap, osd)
		}
		result[osd] = addresses
	}
	return result, decoder.Finish()
}

func decodeIntUintMap(decoder *wire.Decoder, maximum uint32) (map[int32]uint32, error) {
	count, err := boundedCount(decoder, maximum, 8)
	if err != nil {
		return nil, err
	}
	result := make(map[int32]uint32, count)
	for range count {
		key, value := decoder.Int32(), decoder.Uint32()
		if _, exists := result[key]; exists {
			return nil, fmt.Errorf("%w: duplicate osd value %d", ErrMalformedMap, key)
		}
		result[key] = value
	}
	return result, decoder.Finish()
}

func decodePGSet(decoder *wire.Decoder, limits Limits) ([]PG, error) {
	count, err := boundedCount(decoder, limits.MaxPGMappings, 17)
	if err != nil {
		return nil, err
	}
	result := make([]PG, 0, count)
	seen := make(map[PG]struct{}, count)
	for range count {
		pg, err := decodePG(decoder)
		if err != nil {
			return nil, err
		}
		if _, exists := seen[pg]; exists {
			return nil, fmt.Errorf("%w: duplicate pg set entry", ErrMalformedMap)
		}
		seen[pg] = struct{}{}
		result = append(result, pg)
	}
	return result, decoder.Finish()
}

func (incremental *OSDMapIncremental) FSID() FSID             { return incremental.fsid }
func (incremental *OSDMapIncremental) Epoch() uint32          { return incremental.epoch }
func (incremental *OSDMapIncremental) IncrementalCRC() uint32 { return incremental.incrementalCRC }
func (incremental *OSDMapIncremental) FullCRC() uint32        { return incremental.fullCRC }
