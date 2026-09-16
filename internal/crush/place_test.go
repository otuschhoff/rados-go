package crush

import (
	"errors"
	"reflect"
	"testing"
)

func TestCrushLnPinnedVectors(t *testing.T) {
	tests := []struct {
		input uint32
		want  uint64
	}{
		{input: 0, want: 0x0000000000000000},
		{input: 1, want: 0x0000100000000000},
		{input: 2, want: 0x0000195c01a39fbd},
		{input: 255, want: 0x0000800000000000},
		{input: 256, want: 0x000080171e3b6d7a},
		{input: 32767, want: 0x0000f00000000000},
		{input: 65534, want: 0x0000fffffd61ad10},
		{input: 65535, want: 0x0000fffff0000000},
	}
	for _, test := range tests {
		if got := crushLn(test.input); got != test.want {
			t.Fatalf("crushLn(%d) = %#x, want %#x", test.input, got, test.want)
		}
	}
}

func TestPlaceChooseFirstN(t *testing.T) {
	crushMap := testPlacementMap()
	weights := []uint32{0x10000, 0x10000, 0x10000, 0x10000}
	for seed := range uint32(32) {
		got, err := crushMap.Place(0, uint32(seed), 2, weights)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 2 || got[0] == got[1] || got[0] < 0 || got[0] > 3 || got[1] < 0 || got[1] > 3 {
			t.Fatalf("seed %d invalid placement = %v", seed, got)
		}
	}
}

func TestPlaceChooseIndep(t *testing.T) {
	crushMap := testPlacementMap()
	crushMap.Rules[0] = Rule{Type: RuleTypeErasure, Steps: []RuleStep{
		{Operation: RuleSetChooseleafTries, Argument1: 5},
		{Operation: RuleSetChooseTries, Argument1: 100},
		{Operation: RuleTake, Argument1: -1},
		{Operation: RuleChooseIndep},
		{Operation: RuleEmit},
	}}
	weights := []uint32{0x10000, 0x10000, 0x10000, 0x10000}
	for seed := range uint32(32) {
		got, err := crushMap.Place(0, seed, 3, weights)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 3 || got[0] == got[1] || got[0] == got[2] || got[1] == got[2] {
			t.Fatalf("seed %d invalid independent placement = %v", seed, got)
		}
	}
}

func TestPlaceRejectsChooseleafIndep(t *testing.T) {
	crushMap := testPlacementMap()
	crushMap.Rules[0] = Rule{Type: RuleTypeErasure, Steps: []RuleStep{
		{Operation: RuleSetChooseleafTries, Argument1: 5},
		{Operation: RuleSetChooseTries, Argument1: 100},
		{Operation: RuleTake, Argument1: -1},
		{Operation: RuleChooseleafIndep, Argument2: 1},
		{Operation: RuleEmit},
	}}
	if _, err := crushMap.Place(0, 7, 2, []uint32{0x10000, 0x10000, 0x10000, 0x10000}); !errors.Is(err, ErrPlacement) {
		t.Fatalf("error=%v want ErrPlacement", err)
	}
}

func TestPlaceFiltersOSDWeights(t *testing.T) {
	crushMap := testPlacementMap()
	got, err := crushMap.Place(0, 7, 2, []uint32{0, 0, 0, 0x10000})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []int32{3}) {
		t.Fatalf("placement = %v, want [3]", got)
	}
}

func TestPlaceRejectsInvalidRule(t *testing.T) {
	if _, err := testPlacementMap().Place(9, 0, 1, []uint32{0x10000}); !errors.Is(err, ErrPlacement) {
		t.Fatalf("error = %v, want ErrPlacement", err)
	}
}

func TestPlaceRejectsInvalidReplicaCount(t *testing.T) {
	for _, replicas := range []int{-1, 0, maxCertifiedReplicas + 1} {
		if _, err := testPlacementMap().Place(0, 0, replicas, []uint32{0x10000}); !errors.Is(err, ErrPlacement) {
			t.Fatalf("replicas=%d error=%v want ErrPlacement", replicas, err)
		}
	}
}

func TestPlaceRejectsCyclicBucketGraph(t *testing.T) {
	crushMap := testPlacementMap()
	bucket := crushMap.Buckets[-2]
	bucket.Items[0] = -1
	crushMap.Buckets[-2] = bucket
	if _, err := crushMap.Place(0, 0, 1, []uint32{0x10000}); !errors.Is(err, ErrPlacement) {
		t.Fatalf("error=%v want ErrPlacement", err)
	}
}

func TestPlaceRejectsOutsideCertifiedProfile(t *testing.T) {
	tests := []func(*Map){
		func(value *Map) { value.ChooseTotalTries = 51 },
		func(value *Map) {
			value.Rules[0] = Rule{Type: RuleTypeReplicated, Steps: []RuleStep{{Operation: RuleTake, Argument1: -1}, {Operation: RuleChooseleafFirstN, Argument2: 1}, {Operation: RuleEmit}}}
		},
		func(value *Map) { value.classShadowBuckets = map[int32]struct{}{-1: {}} },
		func(value *Map) { value.Rules[0].Steps[0].Argument1 = -99 },
	}
	for index, mutate := range tests {
		crushMap := testPlacementMap()
		mutate(crushMap)
		if _, err := crushMap.Place(0, 1, 2, []uint32{0x10000, 0x10000, 0x10000, 0x10000}); !errors.Is(err, ErrPlacement) {
			t.Fatalf("case %d error=%v want ErrPlacement", index, err)
		}
	}
}

func TestPlaceRejectsExcessiveBucketDepth(t *testing.T) {
	crushMap := testPlacementMap()
	crushMap.Buckets = make(map[int32]Bucket, maxCertifiedDepth+1)
	for depth := int32(1); depth <= maxCertifiedDepth+1; depth++ {
		item := int32(0)
		if depth <= maxCertifiedDepth {
			item = -depth - 1
		}
		crushMap.Buckets[-depth] = Bucket{ID: -depth, Type: 1, Items: []int32{item}, ItemWeights: []uint32{0x10000}}
	}
	if _, err := crushMap.Place(0, 1, 1, []uint32{0x10000}); !errors.Is(err, ErrPlacement) {
		t.Fatalf("error=%v want ErrPlacement", err)
	}
}

func testPlacementMap() *Map {
	return &Map{
		MaxDevices: 4,
		Buckets: map[int32]Bucket{
			-1: {ID: -1, Type: 2, Weight: 4 * 0x10000, Items: []int32{-2, -3}, ItemWeights: []uint32{2 * 0x10000, 2 * 0x10000}},
			-2: {ID: -2, Type: 1, Weight: 2 * 0x10000, Items: []int32{0, 1}, ItemWeights: []uint32{0x10000, 0x10000}},
			-3: {ID: -3, Type: 1, Weight: 2 * 0x10000, Items: []int32{2, 3}, ItemWeights: []uint32{0x10000, 0x10000}},
		},
		Rules: map[uint32]Rule{
			0: {Type: RuleTypeReplicated, Steps: []RuleStep{
				{Operation: RuleTake, Argument1: -1},
				{Operation: RuleChooseFirstN, Argument1: 0, Argument2: 0},
				{Operation: RuleEmit},
			}},
		},
		ChooseLocalTries: 0, ChooseLocalFallbackTries: 0, ChooseTotalTries: 50,
		ChooseleafDescendOnce: 1, ChooseleafVaryR: 1, ChooseleafStable: 1,
	}
}
