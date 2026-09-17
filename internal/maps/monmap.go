// Package maps decodes and publishes immutable Ceph cluster maps.
package maps

import (
	"errors"
	"fmt"

	wire "github.com/otuschhoff/rados-go/internal/encoding"
	"github.com/otuschhoff/rados-go/internal/protocol"
)

var ErrMalformedMap = errors.New("malformed Ceph map")

type Limits struct {
	MaxBytes             uint32
	MaxMonitors          uint32
	MaxAddresses         uint32
	MaxLocations         uint32
	MaxPools             uint32
	MaxOSDs              uint32
	MaxPGMappings        uint32
	MaxCollectionEntries uint32
}

type FSID [16]byte

type UTime struct {
	Seconds     uint32
	Nanoseconds uint32
}

type Monitor struct {
	Name      string
	Addresses protocol.EntityAddrVec
	Priority  uint16
	Weight    uint16
	Location  map[string]string
}

type MonMap struct {
	fsid               FSID
	epoch              uint32
	lastChanged        UTime
	created            UTime
	persistentFeatures uint64
	optionalFeatures   uint64
	monitors           map[string]Monitor
	ranks              []string
	minMonitorRelease  uint8
	removedRanks       []uint32
	electionStrategy   uint8
	disallowedLeaders  []string
	stretchMode        bool
	tiebreakerMonitor  string
	stretchMarkedDown  []string
}

func DecodeMonMap(data []byte, limits Limits) (*MonMap, error) {
	if limits.MaxBytes == 0 || limits.MaxMonitors == 0 || limits.MaxAddresses == 0 || limits.MaxLocations == 0 {
		return nil, fmt.Errorf("%w: map limits must be positive", wire.ErrLimitExceeded)
	}
	decoder := wire.NewDecoder(data, wire.Limits{MaxBytes: limits.MaxBytes})
	version, payload := decoder.Versioned(9)
	if err := decoder.Finish(); err != nil {
		return nil, err
	}
	if version < 6 {
		return nil, fmt.Errorf("%w: monmap version %d", wire.ErrUnsupportedVersion, version)
	}

	result := &MonMap{monitors: make(map[string]Monitor)}
	copy(result.fsid[:], payload.Raw(16))
	result.epoch = payload.Uint32()
	result.lastChanged = decodeUTime(payload)
	result.created = decodeUTime(payload)
	var err error
	result.persistentFeatures, err = decodeMonitorFeatures(payload)
	if err != nil {
		return nil, err
	}
	result.optionalFeatures, err = decodeMonitorFeatures(payload)
	if err != nil {
		return nil, err
	}

	count, err := boundedCount(payload, limits.MaxMonitors, 5)
	if err != nil {
		return nil, err
	}
	for range count {
		key := payload.String()
		monitor, err := decodeMonitor(payload, limits)
		if err != nil {
			return nil, err
		}
		if key != monitor.Name {
			return nil, fmt.Errorf("%w: monitor key %q does not match name %q", ErrMalformedMap, key, monitor.Name)
		}
		if _, exists := result.monitors[key]; exists {
			return nil, fmt.Errorf("%w: duplicate monitor %q", ErrMalformedMap, key)
		}
		result.monitors[key] = monitor
	}

	result.ranks, err = decodeStrings(payload, limits.MaxMonitors)
	if err != nil {
		return nil, err
	}
	if len(result.ranks) != len(result.monitors) {
		return nil, fmt.Errorf("%w: %d ranks for %d monitors", ErrMalformedMap, len(result.ranks), len(result.monitors))
	}
	seenRanks := make(map[string]struct{}, len(result.ranks))
	for _, name := range result.ranks {
		if _, exists := result.monitors[name]; !exists {
			return nil, fmt.Errorf("%w: rank references unknown monitor %q", ErrMalformedMap, name)
		}
		if _, exists := seenRanks[name]; exists {
			return nil, fmt.Errorf("%w: duplicate rank for monitor %q", ErrMalformedMap, name)
		}
		seenRanks[name] = struct{}{}
	}
	if version >= 7 {
		result.minMonitorRelease = payload.Uint8()
	}
	if version >= 8 {
		result.removedRanks, err = decodeUint32s(payload, limits.MaxMonitors)
		if err != nil {
			return nil, err
		}
		result.electionStrategy = payload.Uint8()
		result.disallowedLeaders, err = decodeStrings(payload, limits.MaxMonitors)
		if err != nil {
			return nil, err
		}
	}
	if version >= 9 {
		result.stretchMode = payload.Bool()
		result.tiebreakerMonitor = payload.String()
		result.stretchMarkedDown, err = decodeStrings(payload, limits.MaxMonitors)
		if err != nil {
			return nil, err
		}
	}
	if err := payload.Finish(); err != nil {
		return nil, err
	}
	if err := decoder.Finish(); err != nil {
		return nil, err
	}
	if decoder.Remaining() != 0 {
		return nil, fmt.Errorf("%w: trailing monmap bytes", ErrMalformedMap)
	}
	if result.lastChanged.Nanoseconds >= 1_000_000_000 || result.created.Nanoseconds >= 1_000_000_000 {
		return nil, fmt.Errorf("%w: invalid monmap timestamp", ErrMalformedMap)
	}
	return result, nil
}

func decodeMonitor(decoder *wire.Decoder, limits Limits) (Monitor, error) {
	version, payload := decoder.Versioned(5)
	if err := decoder.Finish(); err != nil {
		return Monitor{}, err
	}
	if version < 1 {
		return Monitor{}, fmt.Errorf("%w: monitor info version %d", wire.ErrUnsupportedVersion, version)
	}
	monitor := Monitor{Name: payload.String()}
	addresses, err := protocol.DecodeEntityAddrVec(payload, limits.MaxAddresses)
	if err != nil {
		return Monitor{}, err
	}
	monitor.Addresses = addresses
	if version >= 2 {
		monitor.Priority = payload.Uint16()
	}
	if version >= 4 {
		monitor.Weight = payload.Uint16()
	}
	if version >= 5 {
		count, err := boundedCount(payload, limits.MaxLocations, 8)
		if err != nil {
			return Monitor{}, err
		}
		monitor.Location = make(map[string]string, count)
		for range count {
			key, value := payload.String(), payload.String()
			if _, exists := monitor.Location[key]; exists {
				return Monitor{}, fmt.Errorf("%w: duplicate monitor location %q", ErrMalformedMap, key)
			}
			monitor.Location[key] = value
		}
	}
	return monitor, payload.Finish()
}

func decodeMonitorFeatures(decoder *wire.Decoder) (uint64, error) {
	_, payload := decoder.Versioned(1)
	features := payload.Uint64()
	return features, payload.Finish()
}

func decodeUTime(decoder *wire.Decoder) UTime {
	return UTime{Seconds: decoder.Uint32(), Nanoseconds: decoder.Uint32()}
}

func boundedCount(decoder *wire.Decoder, maximum uint32, minimumBytes uint64) (uint32, error) {
	count := decoder.Uint32()
	if err := decoder.Finish(); err != nil {
		return 0, err
	}
	if count > maximum {
		return 0, wire.ErrLimitExceeded
	}
	if minimumBytes != 0 && uint64(count) > decoder.Remaining()/minimumBytes {
		return 0, wire.ErrMalformed
	}
	return count, nil
}

func decodeStrings(decoder *wire.Decoder, maximum uint32) ([]string, error) {
	count, err := boundedCount(decoder, maximum, 4)
	if err != nil {
		return nil, err
	}
	values := make([]string, 0, count)
	for range count {
		values = append(values, decoder.String())
	}
	return values, decoder.Finish()
}

func decodeUint32s(decoder *wire.Decoder, maximum uint32) ([]uint32, error) {
	count, err := boundedCount(decoder, maximum, 4)
	if err != nil {
		return nil, err
	}
	values := make([]uint32, count)
	for index := range values {
		values[index] = decoder.Uint32()
	}
	return values, decoder.Finish()
}

func (monMap *MonMap) FSID() FSID                   { return monMap.fsid }
func (monMap *MonMap) Epoch() uint32                { return monMap.epoch }
func (monMap *MonMap) LastChanged() UTime           { return monMap.lastChanged }
func (monMap *MonMap) Created() UTime               { return monMap.created }
func (monMap *MonMap) PersistentFeatures() uint64   { return monMap.persistentFeatures }
func (monMap *MonMap) OptionalFeatures() uint64     { return monMap.optionalFeatures }
func (monMap *MonMap) MinimumMonitorRelease() uint8 { return monMap.minMonitorRelease }
func (monMap *MonMap) ElectionStrategy() uint8      { return monMap.electionStrategy }
func (monMap *MonMap) StretchModeEnabled() bool     { return monMap.stretchMode }
func (monMap *MonMap) TiebreakerMonitor() string    { return monMap.tiebreakerMonitor }
func (monMap *MonMap) MonitorCount() int            { return len(monMap.monitors) }
func (monMap *MonMap) Monitor(name string) (Monitor, bool) {
	monitor, ok := monMap.monitors[name]
	if !ok {
		return Monitor{}, false
	}
	monitor.Addresses = cloneAddresses(monitor.Addresses)
	monitor.Location = cloneStringsMap(monitor.Location)
	return monitor, true
}
func (monMap *MonMap) Ranks() []string        { return append([]string(nil), monMap.ranks...) }
func (monMap *MonMap) RemovedRanks() []uint32 { return append([]uint32(nil), monMap.removedRanks...) }
func (monMap *MonMap) DisallowedLeaders() []string {
	return append([]string(nil), monMap.disallowedLeaders...)
}
func (monMap *MonMap) StretchMarkedDown() []string {
	return append([]string(nil), monMap.stretchMarkedDown...)
}

func cloneAddresses(addresses protocol.EntityAddrVec) protocol.EntityAddrVec {
	result := make(protocol.EntityAddrVec, len(addresses))
	for index, address := range addresses {
		result[index] = address
		result[index].SocketData = append([]byte(nil), address.SocketData...)
	}
	return result
}

func cloneStringsMap(values map[string]string) map[string]string {
	result := make(map[string]string, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}
