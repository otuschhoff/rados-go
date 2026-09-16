package maps

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/otuschhoff/go-librados/internal/crush"
	wire "github.com/otuschhoff/go-librados/internal/encoding"
)

func TestMapObjectP00OracleVector(t *testing.T) {
	osdMap := &OSDMap{pools: map[int64]Pool{2: {
		id: 2, objectHash: objectHashRJenkins, pgCount: 32, placementPGCount: 32,
		flags: poolFlagHashPSPool,
	}}}
	placement, err := osdMap.MapObject(2, "p00-smoke-object", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if placement.RawHash != 0x96fc93a8 || placement.RawPG.Seed != 0x96fc93a8 || placement.PG.Seed != 8 {
		t.Fatalf("placement = %+v", placement)
	}
}

func TestMapRawHashMatchesObjectPlacement(t *testing.T) {
	osdMap := &OSDMap{pools: map[int64]Pool{7: {
		id: 7, objectHash: objectHashRJenkins, pgCount: 12, placementPGCount: 8,
		flags: poolFlagHashPSPool,
	}}}
	objectPlacement, err := osdMap.MapObject(7, "object", "locator", "namespace")
	if err != nil {
		t.Fatal(err)
	}
	rawPlacement, err := osdMap.MapRawHash(7, objectPlacement.RawHash)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rawPlacement, objectPlacement) {
		t.Fatalf("raw=%+v object=%+v", rawPlacement, objectPlacement)
	}
}

func TestSortBitwiseFlag(t *testing.T) {
	if (&OSDMap{}).SortBitwise() {
		t.Fatal("SORTBITWISE reported for an unset map flag")
	}
	if !(&OSDMap{flags: osdMapFlagSortBitwise}).SortBitwise() {
		t.Fatal("SORTBITWISE flag was not reported")
	}
}

func TestMapObjectIdentityInputs(t *testing.T) {
	osdMap := &OSDMap{pools: map[int64]Pool{7: {
		id: 7, objectHash: objectHashRJenkins, pgCount: 12, placementPGCount: 8,
		flags: poolFlagHashPSPool,
	}}}
	plain, err := osdMap.MapObject(7, "object", "", "namespace")
	if err != nil {
		t.Fatal(err)
	}
	located, err := osdMap.MapObject(7, "different", "locator", "namespace")
	if err != nil {
		t.Fatal(err)
	}
	locatedAgain, err := osdMap.MapObject(7, "object", "locator", "namespace")
	if err != nil {
		t.Fatal(err)
	}
	if plain.RawHash == located.RawHash || located.RawHash != locatedAgain.RawHash {
		t.Fatalf("plain=%+v located=%+v locatedAgain=%+v", plain, located, locatedAgain)
	}
	if plain.PG.Seed >= 12 {
		t.Fatalf("invalid stable mapping: %+v", plain)
	}
}

func TestMapObjectRejectsUnsupportedPool(t *testing.T) {
	tests := []OSDMap{
		{pools: map[int64]Pool{}},
		{pools: map[int64]Pool{1: {id: 1, objectHash: 99, pgCount: 1, placementPGCount: 1}}},
		{pools: map[int64]Pool{1: {id: 1, objectHash: objectHashRJenkins, flags: poolFlagHashPSPool}}},
		{pools: map[int64]Pool{1: {id: 1, objectHash: objectHashRJenkins, pgCount: 1, placementPGCount: 1}}},
	}
	for index := range tests {
		if _, err := tests[index].MapObject(1, "object", "", ""); !errors.Is(err, ErrUnsupportedPlacement) {
			t.Fatalf("case %d error = %v", index, err)
		}
	}
}

func TestPlaceObjectAppliesUpAndActingOverrides(t *testing.T) {
	crushData := encodePlacementCrushMap(t)
	base := &OSDMap{
		pools:  map[int64]Pool{2: {id: 2, poolType: poolTypeReplicated, size: 2, crushRule: 0, objectHash: objectHashRJenkins, pgCount: 32, placementPGCount: 32, flags: poolFlagHashPSPool}},
		maxOSD: 4, osdState: []uint32{3, 3, 3, 1}, osdWeight: []uint32{0x10000, 0x10000, 0x10000, 0x10000},
		primaryAffinity: []uint32{defaultPrimaryAffinity, defaultPrimaryAffinity, 0, defaultPrimaryAffinity}, crushData: crushData,
	}
	identity, err := base.MapObject(2, "p00-smoke-object", "", "")
	if err != nil {
		t.Fatal(err)
	}
	base.pgUpmapItems = map[PG][]OSDRemap{identity.PG: {{From: 0, To: 2}}}
	base.pgUpmapPrimaries = map[PG]int32{identity.PG: 2}
	base.pgTemp = map[PG][]int32{identity.PG: {1, 3}}
	base.primaryTemp = map[PG]int32{identity.PG: 1}
	placement, err := base.PlaceObject(2, "p00-smoke-object", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(placement.Up, []int32{0, 2}) || placement.UpPrimary != 0 {
		t.Fatalf("up=%v primary=%d raw=%v", placement.Up, placement.UpPrimary, placement.Raw)
	}
	if !reflect.DeepEqual(placement.Acting, []int32{1}) || placement.ActingPrimary != 1 {
		t.Fatalf("acting=%v primary=%d", placement.Acting, placement.ActingPrimary)
	}
}

func TestPlaceObjectSupportsErasurePoolShardSet(t *testing.T) {
	osdMap := &OSDMap{
		pools:  map[int64]Pool{2: {id: 2, poolType: poolTypeErasure, size: 2, crushRule: 0, objectHash: objectHashRJenkins, pgCount: 32, placementPGCount: 32, flags: poolFlagHashPSPool}},
		maxOSD: 4, osdState: []uint32{3, 3, 3, 3}, osdWeight: []uint32{0x10000, 0x10000, 0x10000, 0x10000},
		crushData: encodePlacementCrushMap(t),
	}
	placement, err := osdMap.PlaceObject(2, "object", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(placement.Raw) != 2 || len(placement.Up) != 2 || len(placement.Acting) != 2 || placement.ActingPrimary != placement.Acting[0] || !placement.Sharded || placement.PrimaryShard != 0 {
		t.Fatalf("placement = %+v", placement)
	}
}

func TestPlaceObjectRejectsInvalidReplicaAndTemporaryPrimary(t *testing.T) {
	base := &OSDMap{
		pools:     map[int64]Pool{2: {id: 2, poolType: poolTypeReplicated, size: 0, crushRule: 0, objectHash: objectHashRJenkins, pgCount: 32, placementPGCount: 32, flags: poolFlagHashPSPool}},
		maxOSD:    4,
		osdState:  []uint32{3, 3, 3, 3},
		osdWeight: []uint32{0x10000, 0x10000, 0x10000, 0x10000},
		crushData: encodePlacementCrushMap(t),
	}
	if _, err := base.PlaceObject(2, "object", "", ""); !errors.Is(err, ErrUnsupportedPlacement) {
		t.Fatalf("zero replicas error=%v", err)
	}
	pool := base.pools[2]
	pool.size = 2
	base.pools[2] = pool
	identity, err := base.MapObject(2, "object", "", "")
	if err != nil {
		t.Fatal(err)
	}
	base.primaryTemp = map[PG]int32{identity.PG: 3}
	base.osdState[3] = 1
	if _, err := base.PlaceObject(2, "object", "", ""); !errors.Is(err, ErrUnsupportedPlacement) {
		t.Fatalf("invalid temporary primary error=%v", err)
	}
	base.primaryTemp = nil
	base.pgTemp = map[PG][]int32{identity.PG: {0, 0}}
	if _, err := base.PlaceObject(2, "object", "", ""); !errors.Is(err, ErrUnsupportedPlacement) {
		t.Fatalf("duplicate temporary set error=%v", err)
	}
	base.pgTemp = nil
	base.pgUpmap = map[PG][]int32{identity.PG: {0, 99}}
	if _, err := base.PlaceObject(2, "object", "", ""); !errors.Is(err, ErrUnsupportedPlacement) {
		t.Fatalf("nonexistent upmap target error=%v", err)
	}
}

func TestPlaceObjectP00LiveOracleVector(t *testing.T) {
	base := &OSDMap{
		pools:  map[int64]Pool{2: {id: 2, poolType: poolTypeReplicated, size: 3, crushRule: 0, objectHash: objectHashRJenkins, pgCount: 32, placementPGCount: 32, flags: poolFlagHashPSPool}},
		maxOSD: 3, osdState: []uint32{3, 3, 3}, osdWeight: []uint32{0x10000, 0x10000, 0x10000},
		crushData: encodeP00CrushMap(t),
	}
	placement, err := base.PlaceObject(2, "p00-smoke-object", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if placement.PG.Seed != 8 || !reflect.DeepEqual(placement.Up, []int32{0, 2, 1}) || placement.UpPrimary != 0 || !reflect.DeepEqual(placement.Acting, []int32{0, 2, 1}) || placement.ActingPrimary != 0 {
		t.Fatalf("placement=%+v", placement)
	}
}

func TestP05OSDMapToolObjectCorpus(t *testing.T) {
	crushData, err := os.ReadFile("../../testdata/p05/crushmap.bin")
	if err != nil {
		t.Fatal(err)
	}
	osdMap := &OSDMap{
		fsid: FSID{1}, epoch: 1,
		pools:    map[int64]Pool{1: {id: 1, name: "p05", poolType: poolTypeReplicated, size: 3, crushRule: 0, objectHash: objectHashRJenkins, pgCount: 256, placementPGCount: 256, flags: poolFlagHashPSPool}},
		nameToID: map[string]int64{"p05": 1},
		maxOSD:   4, osdState: []uint32{3, 3, 3, 3}, osdWeight: []uint32{0x10000, 0x10000, 0x10000, 0x10000}, crushData: crushData,
	}
	verifyObjectMappings(t, osdMap, "../../testdata/p05/object-mappings.txt")
	pool := osdMap.pools[1]
	pool.pgCount, pool.placementPGCount = 32, 32
	osdMap, err = ApplyOSDMapIncremental(osdMap, &OSDMapIncremental{
		fsid: osdMap.fsid, epoch: 2, newPoolMax: -1, newFlags: -1, newMaxOSD: -1,
		newPools: map[int64]Pool{1: pool},
	}, testOSDMapLimits)
	if err != nil {
		t.Fatal(err)
	}
	verifyObjectMappings(t, osdMap, "../../testdata/p05/object-mappings-pg32.txt")
	osdMap, err = ApplyOSDMapIncremental(osdMap, &OSDMapIncremental{
		fsid: osdMap.fsid, epoch: 3, newPoolMax: -1, newFlags: -1, newMaxOSD: -1,
		newWeight: map[int32]uint32{1: 0},
	}, testOSDMapLimits)
	if err != nil {
		t.Fatal(err)
	}
	verifyObjectMappings(t, osdMap, "../../testdata/p05/object-mappings-osd1-out.txt")
	upmapMap := &OSDMap{
		pools:  map[int64]Pool{1: {id: 1, poolType: poolTypeReplicated, size: 3, crushRule: 0, objectHash: objectHashRJenkins, pgCount: 256, placementPGCount: 256, flags: poolFlagHashPSPool}},
		maxOSD: 4, osdState: []uint32{3, 3, 3, 3}, osdWeight: []uint32{0x10000, 0x10000, 0x10000, 0x10000}, crushData: crushData,
		pgUpmapItems: readUpmapCommands(t, "../../testdata/p05/upmap-commands.txt"),
	}
	verifyObjectMappings(t, upmapMap, "../../testdata/p05/object-mappings-upmap.txt")
}

func readUpmapCommands(t *testing.T, path string) map[PG][]OSDRemap {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	result := make(map[PG][]OSDRemap)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var seedHex string
		var from, to int32
		if _, err := fmt.Sscanf(scanner.Text(), "ceph osd pg-upmap-items 1.%s %d %d", &seedHex, &from, &to); err != nil {
			t.Fatalf("parse %q: %v", scanner.Text(), err)
		}
		seed, err := strconv.ParseUint(seedHex, 16, 32)
		if err != nil {
			t.Fatal(err)
		}
		pg := PG{Pool: 1, Seed: uint32(seed), Preferred: -1}
		result[pg] = append(result[pg], OSDRemap{From: from, To: to})
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func verifyObjectMappings(t *testing.T, osdMap *OSDMap, path string) {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	count := 0
	for scanner.Scan() {
		var object, pgHex, encoded string
		if _, err := fmt.Sscanf(scanner.Text(), "object '%s -> 1.%s -> %s", &object, &pgHex, &encoded); err != nil {
			t.Fatalf("parse %q: %v", scanner.Text(), err)
		}
		object = strings.TrimSuffix(object, "'")
		pgValue, err := strconv.ParseUint(pgHex, 16, 32)
		if err != nil {
			t.Fatal(err)
		}
		want := parseOSDList(t, encoded)
		placement, err := osdMap.PlaceObject(1, object, "", "")
		if err != nil {
			t.Fatalf("%s: %v", object, err)
		}
		if placement.PG.Seed != uint32(pgValue) || !reflect.DeepEqual(placement.Up, want) || !reflect.DeepEqual(placement.Acting, want) || placement.UpPrimary != want[0] || placement.ActingPrimary != want[0] {
			t.Fatalf("%s placement=%+v want pg=%x osds=%v", object, placement, pgValue, want)
		}
		count++
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 128 {
		t.Fatalf("rows=%d want=128", count)
	}
}

func parseOSDList(t *testing.T, encoded string) []int32 {
	t.Helper()
	encoded = strings.TrimSuffix(strings.TrimPrefix(encoded, "["), "]")
	result := make([]int32, 0, 3)
	for _, value := range strings.Split(encoded, ",") {
		parsed, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, int32(parsed))
	}
	return result
}

func TestRejectedFullUpmapSkipsLaterOverrides(t *testing.T) {
	osdMap := &OSDMap{
		osdWeight:        []uint32{0x10000, 0, 0x10000},
		pgUpmap:          map[PG][]int32{{Pool: 2, Seed: 8, Preferred: -1}: {1, 2}},
		pgUpmapItems:     map[PG][]OSDRemap{{Pool: 2, Seed: 8, Preferred: -1}: {{From: 0, To: 2}}},
		pgUpmapPrimaries: map[PG]int32{{Pool: 2, Seed: 8, Preferred: -1}: 2},
	}
	pg := PG{Pool: 2, Seed: 8, Preferred: -1}
	if got := osdMap.applyUpmap(pg, []int32{0, 2}); !reflect.DeepEqual(got, []int32{0, 2}) {
		t.Fatalf("mapping=%v want unchanged source", got)
	}
}

func TestUpmapItemsCanReplaceNonexistentRawOSD(t *testing.T) {
	osdMap := &OSDMap{
		osdState:     []uint32{0, 3, 3},
		osdWeight:    []uint32{0x10000, 0x10000, 0x10000},
		pgUpmapItems: map[PG][]OSDRemap{{Pool: 2, Seed: 8, Preferred: -1}: {{From: 0, To: 2}}},
	}
	pg := PG{Pool: 2, Seed: 8, Preferred: -1}
	if err := osdMap.validateUpmap(pg); err != nil {
		t.Fatal(err)
	}
	raw := []int32{0, 1}
	mapped := osdMap.applyUpmap(pg, raw)
	if !reflect.DeepEqual(raw, []int32{0, 1}) || !reflect.DeepEqual(osdMap.onlyUp(mapped), []int32{2, 1}) {
		t.Fatalf("raw=%v mapped=%v up=%v", raw, mapped, osdMap.onlyUp(mapped))
	}
}

func encodePlacementCrushMap(t *testing.T) []byte {
	t.Helper()
	encoder := wire.NewEncoder(4096)
	encoder.Uint32(crush.Magic)
	encoder.Int32(3)
	encoder.Uint32(1)
	encoder.Int32(4)
	for _, bucket := range []struct {
		id     int32
		typeID uint16
		items  []int32
	}{{-1, 2, []int32{-2, -3}}, {-2, 1, []int32{0, 1}}, {-3, 1, []int32{2, 3}}} {
		encoder.Uint32(crush.BucketStraw2)
		encoder.Int32(bucket.id)
		encoder.Uint16(bucket.typeID)
		encoder.Uint8(crush.BucketStraw2)
		encoder.Uint8(crush.HashRJenkins1)
		encoder.Uint32(uint32(len(bucket.items)) * 0x10000)
		encoder.Uint32(uint32(len(bucket.items)))
		for _, item := range bucket.items {
			encoder.Int32(item)
		}
		for range bucket.items {
			encoder.Uint32(0x10000)
		}
	}
	encoder.Uint32(1)
	encoder.Uint32(3)
	encoder.Uint8(0)
	encoder.Uint8(crush.RuleTypeReplicated)
	encoder.Uint8(1)
	encoder.Uint8(10)
	for _, step := range []crush.RuleStep{{Operation: crush.RuleTake, Argument1: -1}, {Operation: crush.RuleChooseFirstN}, {Operation: crush.RuleEmit}} {
		encoder.Uint32(step.Operation)
		encoder.Int32(step.Argument1)
		encoder.Int32(step.Argument2)
	}
	for range 3 {
		encoder.Uint32(0)
	}
	encoder.Uint32(0)
	encoder.Uint32(0)
	encoder.Uint32(50)
	encoder.Uint32(1)
	encoder.Uint8(1)
	encoder.Uint8(1)
	encoder.Uint32(1 << crush.BucketStraw2)
	encoder.Uint8(1)
	for range 4 {
		encoder.Uint32(0)
	}
	encoder.Uint32(100)
	encoder.Uint32(100)
	data, err := encoder.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func encodeP00CrushMap(t *testing.T) []byte {
	t.Helper()
	encoder := wire.NewEncoder(4096)
	encoder.Uint32(crush.Magic)
	encoder.Int32(2)
	encoder.Uint32(1)
	encoder.Int32(3)
	for _, bucket := range []struct {
		id     int32
		typeID uint16
		items  []int32
	}{{-1, 10, []int32{-2}}, {-2, 1, []int32{0, 1, 2}}} {
		encoder.Uint32(crush.BucketStraw2)
		encoder.Int32(bucket.id)
		encoder.Uint16(bucket.typeID)
		encoder.Uint8(crush.BucketStraw2)
		encoder.Uint8(crush.HashRJenkins1)
		encoder.Uint32(uint32(len(bucket.items)) * 0x10000)
		encoder.Uint32(uint32(len(bucket.items)))
		for _, item := range bucket.items {
			encoder.Int32(item)
		}
		for range bucket.items {
			encoder.Uint32(0x10000)
		}
	}
	encoder.Uint32(1)
	encoder.Uint32(3)
	encoder.Uint8(0)
	encoder.Uint8(crush.RuleTypeReplicated)
	encoder.Uint8(1)
	encoder.Uint8(10)
	for _, step := range []crush.RuleStep{{Operation: crush.RuleTake, Argument1: -1}, {Operation: crush.RuleChooseFirstN}, {Operation: crush.RuleEmit}} {
		encoder.Uint32(step.Operation)
		encoder.Int32(step.Argument1)
		encoder.Int32(step.Argument2)
	}
	for range 3 {
		encoder.Uint32(0)
	}
	encoder.Uint32(0)
	encoder.Uint32(0)
	encoder.Uint32(50)
	encoder.Uint32(1)
	encoder.Uint8(1)
	encoder.Uint8(1)
	encoder.Uint32(1 << crush.BucketStraw2)
	encoder.Uint8(1)
	for range 4 {
		encoder.Uint32(0)
	}
	encoder.Uint32(100)
	encoder.Uint32(100)
	data, err := encoder.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	return data
}
