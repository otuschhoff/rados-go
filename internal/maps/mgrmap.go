package maps

import (
	"fmt"

	wire "github.com/otuschhoff/go-librados/internal/encoding"
	"github.com/otuschhoff/go-librados/internal/protocol"
)

type MgrMap struct {
	epoch          uint32
	activeGID      uint64
	available      bool
	activeName     string
	activeAddrs    protocol.EntityAddrVec
	activeFeatures uint64
}

func DecodeMgrMap(data []byte, limits Limits) (*MgrMap, error) {
	if limits.MaxBytes == 0 || limits.MaxAddresses == 0 || limits.MaxCollectionEntries == 0 {
		return nil, fmt.Errorf("%w: mgrmap limits must be positive", wire.ErrLimitExceeded)
	}
	decoder := wire.NewDecoder(data, wire.Limits{MaxBytes: limits.MaxBytes})
	version, payload := decoder.Versioned(14)
	if err := decoder.Finish(); err != nil {
		return nil, err
	}
	if version < 6 {
		return nil, fmt.Errorf("%w: mgrmap version %d", wire.ErrUnsupportedVersion, version)
	}

	result := &MgrMap{epoch: payload.Uint32()}
	addresses, err := protocol.DecodeEntityAddrVec(payload, limits.MaxAddresses)
	if err != nil {
		return nil, err
	}
	result.activeAddrs = addresses
	result.activeGID = payload.Uint64()
	available, err := decodeCanonicalBool(payload, "mgrmap availability")
	if err != nil {
		return nil, err
	}
	result.available = available
	result.activeName = payload.String()
	if err := consumeMgrStandbys(payload, version, limits); err != nil {
		return nil, err
	}
	if version >= 6 {
		if err := consumeStringSet(payload, limits.MaxCollectionEntries); err != nil {
			return nil, err
		}
	}
	if version >= 3 {
		if _, err := decodeStringMap(payload, limits.MaxCollectionEntries); err != nil {
			return nil, err
		}
	}
	if version >= 4 {
		if err := consumeModuleInfoVector(payload, limits); err != nil {
			return nil, err
		}
	}
	if version >= 7 {
		changed := decodeUTime(payload)
		if changed.Nanoseconds >= 1_000_000_000 {
			return nil, fmt.Errorf("%w: invalid mgrmap timestamp", ErrMalformedMap)
		}
	}
	if version >= 8 {
		if err := consumeAlwaysOnModules(payload, limits.MaxCollectionEntries); err != nil {
			return nil, err
		}
	}
	if version >= 9 {
		result.activeFeatures = payload.Uint64()
	}
	if version >= 10 {
		payload.Uint32()
	}
	if version >= 11 {
		count, err := boundedCount(payload, limits.MaxCollectionEntries, 1)
		if err != nil {
			return nil, err
		}
		for range count {
			if _, err := protocol.DecodeEntityAddrVec(payload, limits.MaxAddresses); err != nil {
				return nil, err
			}
		}
		if version >= 12 {
			names, err := decodeStrings(payload, limits.MaxCollectionEntries)
			if err != nil {
				return nil, err
			}
			if len(names) != int(count) {
				return nil, fmt.Errorf("%w: mgrmap client name/address count mismatch", ErrMalformedMap)
			}
		}
	}
	if version >= 13 {
		payload.Uint64()
	}
	if version >= 14 {
		if err := consumeStringSet(payload, limits.MaxCollectionEntries); err != nil {
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
		return nil, fmt.Errorf("%w: trailing mgrmap bytes", ErrMalformedMap)
	}
	return result, nil
}

func consumeMgrStandbys(decoder *wire.Decoder, version uint8, limits Limits) error {
	count, err := boundedCount(decoder, limits.MaxCollectionEntries, 14)
	if err != nil {
		return err
	}
	seen := make(map[uint64]struct{}, count)
	for range count {
		key := decoder.Uint64()
		if _, exists := seen[key]; exists {
			return fmt.Errorf("%w: duplicate mgr standby %d", ErrMalformedMap, key)
		}
		gid, err := consumeMgrStandby(decoder, limits)
		if err != nil {
			return err
		}
		if key != gid {
			return fmt.Errorf("%w: standby key %d != gid %d", ErrMalformedMap, key, gid)
		}
		seen[key] = struct{}{}
	}
	if version < 6 {
		return fmt.Errorf("%w: mgrmap version %d", wire.ErrUnsupportedVersion, version)
	}
	return decoder.Finish()
}

func consumeMgrStandby(decoder *wire.Decoder, limits Limits) (uint64, error) {
	version, payload := decoder.Versioned(4)
	if err := decoder.Finish(); err != nil {
		return 0, err
	}
	if version < 1 {
		return 0, fmt.Errorf("%w: mgr standby version %d", wire.ErrUnsupportedVersion, version)
	}
	gid := payload.Uint64()
	_ = payload.String()
	if version >= 2 {
		if err := consumeStringSet(payload, limits.MaxCollectionEntries); err != nil {
			return 0, err
		}
	}
	if version >= 3 {
		if err := consumeModuleInfoVector(payload, limits); err != nil {
			return 0, err
		}
	}
	if version >= 4 {
		payload.Uint64()
	}
	return gid, payload.Finish()
}

func consumeModuleInfoVector(decoder *wire.Decoder, limits Limits) error {
	count, err := boundedCount(decoder, limits.MaxCollectionEntries, 6)
	if err != nil {
		return err
	}
	seen := make(map[string]struct{}, count)
	for range count {
		name, err := consumeModuleInfo(decoder, limits)
		if err != nil {
			return err
		}
		if _, exists := seen[name]; exists {
			return fmt.Errorf("%w: duplicate mgr module %q", ErrMalformedMap, name)
		}
		seen[name] = struct{}{}
	}
	return decoder.Finish()
}

func consumeModuleInfo(decoder *wire.Decoder, limits Limits) (string, error) {
	version, payload := decoder.Versioned(2)
	if err := decoder.Finish(); err != nil {
		return "", err
	}
	if version < 1 {
		return "", fmt.Errorf("%w: mgr module-info version %d", wire.ErrUnsupportedVersion, version)
	}
	name := payload.String()
	if _, err := decodeCanonicalBool(payload, "mgr module can_run"); err != nil {
		return "", err
	}
	_ = payload.String()
	if version >= 2 {
		if err := consumeModuleOptionMap(payload, limits); err != nil {
			return "", err
		}
	}
	return name, payload.Finish()
}

func consumeModuleOptionMap(decoder *wire.Decoder, limits Limits) error {
	count, err := boundedCount(decoder, limits.MaxCollectionEntries, 10)
	if err != nil {
		return err
	}
	seen := make(map[string]struct{}, count)
	for range count {
		key := decoder.String()
		if _, exists := seen[key]; exists {
			return fmt.Errorf("%w: duplicate mgr module option %q", ErrMalformedMap, key)
		}
		name, err := consumeModuleOption(decoder, limits.MaxCollectionEntries)
		if err != nil {
			return err
		}
		if name != key {
			return fmt.Errorf("%w: module option key %q != name %q", ErrMalformedMap, key, name)
		}
		seen[key] = struct{}{}
	}
	return decoder.Finish()
}

func consumeModuleOption(decoder *wire.Decoder, maximum uint32) (string, error) {
	version, payload := decoder.Versioned(1)
	if err := decoder.Finish(); err != nil {
		return "", err
	}
	if version < 1 {
		return "", fmt.Errorf("%w: mgr module-option version %d", wire.ErrUnsupportedVersion, version)
	}
	name := payload.String()
	payload.Uint8()
	payload.Uint8()
	payload.Uint32()
	_ = payload.String()
	_ = payload.String()
	_ = payload.String()
	if err := consumeStringSet(payload, maximum); err != nil {
		return "", err
	}
	_ = payload.String()
	_ = payload.String()
	if err := consumeStringSet(payload, maximum); err != nil {
		return "", err
	}
	if err := consumeStringSet(payload, maximum); err != nil {
		return "", err
	}
	return name, payload.Finish()
}

func consumeStringSet(decoder *wire.Decoder, maximum uint32) error {
	count, err := boundedCount(decoder, maximum, 4)
	if err != nil {
		return err
	}
	seen := make(map[string]struct{}, count)
	for range count {
		value := decoder.String()
		if _, exists := seen[value]; exists {
			return fmt.Errorf("%w: duplicate string %q", ErrMalformedMap, value)
		}
		seen[value] = struct{}{}
	}
	return decoder.Finish()
}

func consumeAlwaysOnModules(decoder *wire.Decoder, maximum uint32) error {
	count, err := boundedCount(decoder, maximum, 8)
	if err != nil {
		return err
	}
	seen := make(map[uint32]struct{}, count)
	for range count {
		release := decoder.Uint32()
		if _, exists := seen[release]; exists {
			return fmt.Errorf("%w: duplicate mgr release %d", ErrMalformedMap, release)
		}
		if err := consumeStringSet(decoder, maximum); err != nil {
			return err
		}
		seen[release] = struct{}{}
	}
	return decoder.Finish()
}

func decodeCanonicalBool(decoder *wire.Decoder, name string) (bool, error) {
	value := decoder.Uint8()
	if err := decoder.Finish(); err != nil {
		return false, err
	}
	if value > 1 {
		return false, fmt.Errorf("%w: %s flag", ErrMalformedMap, name)
	}
	return value == 1, nil
}

func (mgrMap *MgrMap) Epoch() uint32 { return mgrMap.epoch }

func (mgrMap *MgrMap) Available() bool { return mgrMap.available }

func (mgrMap *MgrMap) ActiveGID() uint64 { return mgrMap.activeGID }

func (mgrMap *MgrMap) ActiveName() string { return mgrMap.activeName }

func (mgrMap *MgrMap) ActiveFeatures() uint64 { return mgrMap.activeFeatures }

func (mgrMap *MgrMap) ActiveAddresses() protocol.EntityAddrVec {
	return cloneAddresses(mgrMap.activeAddrs)
}
