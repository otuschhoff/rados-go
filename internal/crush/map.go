package crush

import (
	"errors"
	"fmt"

	wire "github.com/otuschhoff/go-librados/internal/encoding"
)

var ErrUnsupported = errors.New("unsupported CRUSH feature")

const (
	Magic                  = 0x00010000
	BucketStraw2           = 5
	HashRJenkins1          = 0
	RuleTake               = 1
	RuleChooseFirstN       = 2
	RuleChooseIndep        = 3
	RuleEmit               = 4
	RuleChooseleafFirstN   = 6
	RuleChooseleafIndep    = 7
	RuleSetChooseTries     = 8
	RuleSetChooseleafTries = 9
	RuleTypeReplicated     = 1
	RuleTypeErasure        = 3
)

type DecodeLimits struct {
	MaxBytes   uint32
	MaxBuckets uint32
	MaxRules   uint32
	MaxItems   uint32
	MaxNames   uint32
}

const (
	maxCertifiedRetries = 1000
	maxCertifiedDepth   = 64
)

type Bucket struct {
	ID          int32
	Type        uint16
	Weight      uint32
	Items       []int32
	ItemWeights []uint32
}

type RuleStep struct {
	Operation uint32
	Argument1 int32
	Argument2 int32
}

type Rule struct {
	Type    uint8
	MinSize uint8
	MaxSize uint8
	Steps   []RuleStep
}

type Map struct {
	MaxDevices               int32
	Buckets                  map[int32]Bucket
	Rules                    map[uint32]Rule
	ChooseLocalTries         uint32
	ChooseLocalFallbackTries uint32
	ChooseTotalTries         uint32
	ChooseleafDescendOnce    uint32
	ChooseleafVaryR          uint8
	StrawCalcVersion         uint8
	AllowedBucketAlgorithms  uint32
	ChooseleafStable         uint8
	MSRDescents              uint32
	MSRCollisionTries        uint32
	classShadowBuckets       map[int32]struct{}
}

func DecodeMap(data []byte, limits DecodeLimits) (*Map, error) {
	if limits.MaxBytes == 0 || limits.MaxBuckets == 0 || limits.MaxRules == 0 || limits.MaxItems == 0 || limits.MaxNames == 0 {
		return nil, wire.ErrLimitExceeded
	}
	decoder := wire.NewDecoder(data, wire.Limits{MaxBytes: limits.MaxBytes})
	if decoder.Uint32() != Magic {
		return nil, fmt.Errorf("%w: bad CRUSH magic", wire.ErrMalformed)
	}
	maxBuckets := decoder.Int32()
	maxRules := decoder.Uint32()
	maxDevices := decoder.Int32()
	if err := decoder.Finish(); err != nil {
		return nil, err
	}
	if maxBuckets < 0 || uint32(maxBuckets) > limits.MaxBuckets || maxRules > limits.MaxRules || maxDevices < 0 || uint32(maxDevices) > limits.MaxItems {
		return nil, wire.ErrLimitExceeded
	}
	result := &Map{
		MaxDevices: maxDevices, Buckets: make(map[int32]Bucket), Rules: make(map[uint32]Rule),
		ChooseLocalTries: 2, ChooseLocalFallbackTries: 5, ChooseTotalTries: 19,
		AllowedBucketAlgorithms: (1 << 1) | (1 << 2) | (1 << 4),
	}
	for index := int32(0); index < maxBuckets; index++ {
		algorithm := decoder.Uint32()
		if algorithm == 0 {
			continue
		}
		if algorithm != BucketStraw2 {
			return nil, fmt.Errorf("%w: bucket algorithm %d", ErrUnsupported, algorithm)
		}
		bucket := Bucket{ID: decoder.Int32(), Type: decoder.Uint16()}
		encodedAlgorithm, hash := decoder.Uint8(), decoder.Uint8()
		bucket.Weight = decoder.Uint32()
		count := decoder.Uint32()
		if err := decoder.Finish(); err != nil {
			return nil, err
		}
		if bucket.ID != -1-index || encodedAlgorithm != BucketStraw2 || hash != HashRJenkins1 {
			return nil, fmt.Errorf("%w: invalid straw2 bucket %d", wire.ErrMalformed, bucket.ID)
		}
		if count > limits.MaxItems {
			return nil, wire.ErrLimitExceeded
		}
		if uint64(count)*8 > decoder.Remaining() {
			return nil, wire.ErrMalformed
		}
		bucket.Items = make([]int32, count)
		bucket.ItemWeights = make([]uint32, count)
		for itemIndex := range bucket.Items {
			bucket.Items[itemIndex] = decoder.Int32()
		}
		for itemIndex := range bucket.ItemWeights {
			bucket.ItemWeights[itemIndex] = decoder.Uint32()
		}
		result.Buckets[bucket.ID] = bucket
	}
	for index := uint32(0); index < maxRules; index++ {
		if decoder.Uint32() == 0 {
			continue
		}
		count := decoder.Uint32()
		if count > limits.MaxItems {
			return nil, wire.ErrLimitExceeded
		}
		if uint64(count)*12+4 > decoder.Remaining() {
			return nil, wire.ErrMalformed
		}
		ruleID := decoder.Uint8()
		rule := Rule{Type: decoder.Uint8(), MinSize: decoder.Uint8(), MaxSize: decoder.Uint8(), Steps: make([]RuleStep, count)}
		if uint32(ruleID) != index {
			return nil, fmt.Errorf("%w: rule %d identity", wire.ErrMalformed, index)
		}
		for stepIndex := range rule.Steps {
			step := RuleStep{Operation: decoder.Uint32(), Argument1: decoder.Int32(), Argument2: decoder.Int32()}
			rule.Steps[stepIndex] = step
		}
		result.Rules[index] = rule
	}
	for range 3 {
		if err := consumeNameMap(decoder, limits.MaxNames); err != nil {
			return nil, err
		}
	}
	if decoder.Remaining() >= 12 {
		result.ChooseLocalTries = decoder.Uint32()
		result.ChooseLocalFallbackTries = decoder.Uint32()
		result.ChooseTotalTries = decoder.Uint32()
		if result.ChooseLocalTries > maxCertifiedRetries || result.ChooseLocalFallbackTries > maxCertifiedRetries || result.ChooseTotalTries >= maxCertifiedRetries {
			return nil, fmt.Errorf("%w: retry tunables exceed certified limit", ErrUnsupported)
		}
	}
	if decoder.Remaining() >= 4 {
		result.ChooseleafDescendOnce = decoder.Uint32()
	}
	if decoder.Remaining() >= 1 {
		result.ChooseleafVaryR = decoder.Uint8()
	}
	if decoder.Remaining() >= 1 {
		result.StrawCalcVersion = decoder.Uint8()
	}
	if decoder.Remaining() >= 4 {
		result.AllowedBucketAlgorithms = decoder.Uint32()
	}
	if decoder.Remaining() >= 1 {
		result.ChooseleafStable = decoder.Uint8()
	}
	if decoder.Remaining() != 0 {
		if err := consumeIntMap(decoder, limits.MaxNames); err != nil {
			return nil, err
		}
		if err := consumeNameMap(decoder, limits.MaxNames); err != nil {
			return nil, err
		}
		shadowBuckets, err := consumeNestedIntMap(decoder, limits.MaxNames)
		if err != nil {
			return nil, err
		}
		result.classShadowBuckets = shadowBuckets
		if decoder.Uint32() != 0 {
			return nil, fmt.Errorf("%w: choose arguments", ErrUnsupported)
		}
	}
	if decoder.Remaining() >= 8 {
		result.MSRDescents = decoder.Uint32()
		result.MSRCollisionTries = decoder.Uint32()
	}
	if decoder.Remaining() != 0 {
		return nil, wire.ErrMalformed
	}
	if err := decoder.Finish(); err != nil {
		return nil, err
	}
	if err := result.validateGraph(); err != nil {
		return nil, err
	}
	return result, nil
}

func consumeNameMap(decoder *wire.Decoder, maximum uint32) error {
	count := decoder.Uint32()
	if count > maximum {
		return wire.ErrLimitExceeded
	}
	for range count {
		decoder.Int32()
		_ = decoder.String()
	}
	return decoder.Finish()
}

func consumeIntMap(decoder *wire.Decoder, maximum uint32) error {
	count := decoder.Uint32()
	if count > maximum {
		return wire.ErrLimitExceeded
	}
	if uint64(count)*8 > decoder.Remaining() {
		return wire.ErrMalformed
	}
	for range count {
		decoder.Int32()
		decoder.Int32()
	}
	return decoder.Finish()
}

func consumeNestedIntMap(decoder *wire.Decoder, maximum uint32) (map[int32]struct{}, error) {
	count := decoder.Uint32()
	if count > maximum {
		return nil, wire.ErrLimitExceeded
	}
	values := make(map[int32]struct{})
	for range count {
		decoder.Int32()
		inner := decoder.Uint32()
		if inner > maximum {
			return nil, wire.ErrLimitExceeded
		}
		if uint64(inner)*8 > decoder.Remaining() {
			return nil, wire.ErrMalformed
		}
		for range inner {
			decoder.Int32()
			values[decoder.Int32()] = struct{}{}
		}
	}
	return values, decoder.Finish()
}

func (crushMap *Map) validateGraph() error {
	states := make(map[int32]uint8, len(crushMap.Buckets))
	heights := make(map[int32]int, len(crushMap.Buckets))
	var visit func(int32, int) (int, error)
	visit = func(id int32, depth int) (int, error) {
		if depth > maxCertifiedDepth {
			return 0, fmt.Errorf("%w: bucket graph exceeds certified depth %d", ErrUnsupported, maxCertifiedDepth)
		}
		if states[id] == 1 {
			return 0, fmt.Errorf("%w: cyclic bucket graph", wire.ErrMalformed)
		}
		if states[id] == 2 {
			if depth+heights[id]-1 > maxCertifiedDepth {
				return 0, fmt.Errorf("%w: bucket graph exceeds certified depth %d", ErrUnsupported, maxCertifiedDepth)
			}
			return heights[id], nil
		}
		states[id] = 1
		height := 1
		for _, item := range crushMap.Buckets[id].Items {
			if item >= 0 {
				if item >= crushMap.MaxDevices {
					return 0, fmt.Errorf("%w: device %d exceeds max", wire.ErrMalformed, item)
				}
				continue
			}
			if _, ok := crushMap.Buckets[item]; !ok {
				return 0, fmt.Errorf("%w: missing bucket %d", wire.ErrMalformed, item)
			}
			childHeight, err := visit(item, depth+1)
			if err != nil {
				return 0, err
			}
			if childHeight+1 > height {
				height = childHeight + 1
			}
		}
		states[id] = 2
		heights[id] = height
		return height, nil
	}
	for id := range crushMap.Buckets {
		if _, err := visit(id, 1); err != nil {
			return err
		}
	}
	return nil
}
