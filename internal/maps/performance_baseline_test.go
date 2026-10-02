package maps

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/otuschhoff/rados-go/internal/crush"
	wire "github.com/otuschhoff/rados-go/internal/encoding"
	"github.com/otuschhoff/rados-go/internal/protocol"
)

type performanceIncrementalCase struct {
	name          string
	osdCount      int
	overrideCount int
}

func performanceIncrementalCases() []performanceIncrementalCase {
	return []performanceIncrementalCase{
		{"OSDs=4/OverridePGs=0", 4, 0},
		{"OSDs=64/OverridePGs=0", 64, 0},
		{"OSDs=1024/OverridePGs=0", 1024, 0},
		{"OSDs=4096/OverridePGs=0", 4096, 0},
		{"OSDs=4/OverridePGs=64", 4, 64},
		{"OSDs=4/OverridePGs=1024", 4, 1024},
		{"OSDs=4/OverridePGs=4096", 4, 4096},
	}
}

func performanceMapFixture(helper testing.TB, osdCount, overrideCount int, topology []byte) *OSDMap {
	helper.Helper()
	if osdCount < 4 || overrideCount < 0 || overrideCount > 4096 {
		helper.Fatal("invalid performance fixture dimensions")
	}
	base := &OSDMap{
		fsid: FSID{1}, epoch: 11, poolMax: 2, maxOSD: int32(osdCount),
		pools: map[int64]Pool{2: {
			id: 2, name: "data", poolType: poolTypeReplicated, size: 2,
			crushRule: 0, objectHash: objectHashRJenkins, pgCount: 4096,
			placementPGCount: 4096, flags: poolFlagHashPSPool,
			applicationMetadata: map[string]map[string]string{"rados": {"purpose": "baseline"}},
			options:             make(map[int32]PoolOption), snapshots: make(map[uint64]PoolSnapshot),
		}},
		nameToID: map[string]int64{"data": 2},
		osdState: make([]uint32, osdCount), osdWeight: make([]uint32, osdCount),
		primaryAffinity: make([]uint32, osdCount), clientAddresses: make([]protocol.EntityAddrVec, osdCount),
		crushData: topology, pgTemp: make(map[PG][]int32, overrideCount),
		primaryTemp: make(map[PG]int32, overrideCount), pgUpmap: make(map[PG][]int32, overrideCount),
		pgUpmapItems: make(map[PG][]OSDRemap, overrideCount), pgUpmapPrimaries: make(map[PG]int32, overrideCount),
		erasureCodeProfiles: make(map[string]map[string]string),
		newRemovedSnapshots: make(map[int64][]Interval), newPurgedSnapshots: make(map[int64][]Interval),
	}
	for osdIndex := range osdCount {
		base.osdState[osdIndex] = osdStateExists | osdStateUp
		base.osdWeight[osdIndex] = 0x10000
		base.primaryAffinity[osdIndex] = defaultPrimaryAffinity
		base.clientAddresses[osdIndex] = protocol.EntityAddrVec{{
			Type: protocol.AddressV2, Nonce: uint32(osdIndex), Family: 2, SocketData: make([]byte, 16),
		}}
	}
	for pgIndex := range overrideCount {
		pg := PG{Pool: 2, Seed: uint32(pgIndex), Preferred: -1}
		base.pgTemp[pg] = []int32{0, 2}
		base.primaryTemp[pg] = 0
		base.pgUpmap[pg] = []int32{0, 2}
		base.pgUpmapItems[pg] = []OSDRemap{{From: 0, To: 2}}
		base.pgUpmapPrimaries[pg] = 0
	}
	return base
}

func performanceRenameFixture(base *OSDMap, overrideCount int) (*OSDMapIncremental, Limits) {
	return &OSDMapIncremental{
		fsid: base.fsid, epoch: base.epoch + 1, newPoolMax: -1, newFlags: -1, newMaxOSD: -1,
		newPoolNames: map[int64]string{2: "archive"},
	}, Limits{
		MaxBytes: uint32(len(base.crushData)) + uint32(base.maxOSD)*128 + uint32(overrideCount)*256 + 4096,
		MaxPools: 1, MaxOSDs: uint32(base.maxOSD), MaxAddresses: 1,
		MaxPGMappings: uint32(max(overrideCount, 1)), MaxCollectionEntries: 4096,
	}
}

func encodePerformancePlacementCrush(helper testing.TB, unrelatedBuckets int) []byte {
	helper.Helper()
	if unrelatedBuckets < 0 || unrelatedBuckets > 4096 {
		helper.Fatal("invalid unrelated bucket count")
	}
	encoder := wire.NewEncoder(256 + uint32(unrelatedBuckets)*28)
	encoder.Uint32(crush.Magic)
	encoder.Int32(int32(3 + unrelatedBuckets))
	encoder.Uint32(1)
	encoder.Int32(int32(4 + unrelatedBuckets))
	for bucketIndex := range 3 + unrelatedBuckets {
		typeID := uint16(1)
		items := []int32{int32(bucketIndex + 1)}
		switch bucketIndex {
		case 0:
			typeID, items = 2, []int32{-2, -3}
		case 1:
			items = []int32{0, 1}
		case 2:
			items = []int32{2, 3}
		}
		encoder.Uint32(crush.BucketStraw2)
		encoder.Int32(-1 - int32(bucketIndex))
		encoder.Uint16(typeID)
		encoder.Uint8(crush.BucketStraw2)
		encoder.Uint8(crush.HashRJenkins1)
		encoder.Uint32(uint32(len(items)) * 0x10000)
		encoder.Uint32(uint32(len(items)))
		for _, item := range items {
			encoder.Int32(item)
		}
		for range items {
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
	for range 5 {
		encoder.Uint32(0)
	}
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
	topology, err := encoder.BytesResult()
	if err != nil {
		helper.Fatal(err)
	}
	return topology
}

func TestPerformancePlacementFixtures(test *testing.T) {
	reference := performanceMapFixture(test, 4100, 0, encodePlacementCrushMap(test))
	for _, unrelatedBuckets := range []int{0, 64, 1024, 4096} {
		test.Run(fmt.Sprintf("UnrelatedBuckets=%d", unrelatedBuckets), func(test *testing.T) {
			base := performanceMapFixture(test, 4100, 0, encodePerformancePlacementCrush(test, unrelatedBuckets))
			decoded, err := crush.DecodeMap(base.crushData, crush.DecodeLimits{
				MaxBytes: uint32(len(base.crushData)), MaxBuckets: uint32(3 + unrelatedBuckets),
				MaxRules: 1, MaxItems: 4100, MaxNames: 1,
			})
			if err != nil {
				test.Fatal(err)
			}
			if len(decoded.Buckets) != 3+unrelatedBuckets || decoded.MaxDevices != int32(4+unrelatedBuckets) || decoded.MaxDevices > base.maxOSD || decoded.ChooseTotalTries != 50 {
				test.Fatal("incorrect topology dimensions or tunables")
			}
			for bucketIndex := range unrelatedBuckets {
				bucket := decoded.Buckets[-4-int32(bucketIndex)]
				if !reflect.DeepEqual(bucket.Items, []int32{4 + int32(bucketIndex)}) {
					test.Fatalf("invalid unrelated bucket: %+v", bucket)
				}
			}
			for hashIndex := range 64 {
				hash := uint32(hashIndex) * 2654435761
				want, err := reference.PlaceRawHash(2, hash)
				if err != nil {
					test.Fatal(err)
				}
				got, err := base.PlaceRawHash(2, hash)
				if err != nil {
					test.Fatal(err)
				}
				if !reflect.DeepEqual(got, want) || len(got.Raw) != 2 {
					test.Fatalf("hash=%d route=%+v want=%+v", hash, got, want)
				}
			}
		})
	}
}

func TestPerformanceIncrementalFixtures(test *testing.T) {
	for _, fixtureCase := range performanceIncrementalCases() {
		test.Run(fixtureCase.name, func(test *testing.T) {
			base := performanceMapFixture(test, fixtureCase.osdCount, fixtureCase.overrideCount, encodePlacementCrushMap(test))
			incremental, limits := performanceRenameFixture(base, fixtureCase.overrideCount)
			if err := validateOSDMapLimits(limits); err != nil {
				test.Fatal(err)
			}
			for pgIndex := range max(fixtureCase.overrideCount, 1) {
				if _, err := base.PlaceRawHash(2, uint32(pgIndex)); err != nil {
					test.Fatalf("invalid PG %d: %v", pgIndex, err)
				}
			}
			next, err := ApplyOSDMapIncremental(base, incremental, limits)
			if err != nil {
				test.Fatal(err)
			}
			if next.epoch != 12 || next.pools[2].name != "archive" || next.nameToID["archive"] != 2 || len(next.nameToID) != 1 {
				test.Fatal("rename did not apply")
			}
			unchanged := cloneOSDMap(next)
			unchanged.epoch, unchanged.pools, unchanged.nameToID = base.epoch, base.pools, base.nameToID
			unchanged.appliedIncremental = base.appliedIncremental
			if !unchanged.Equivalent(base) {
				test.Fatal("rename changed unrelated map state")
			}
			before := performanceMapFixture(test, fixtureCase.osdCount, fixtureCase.overrideCount, encodePlacementCrushMap(test))
			retained := cloneOSDMap(next)
			addresses, _ := next.OSDClientAddresses(0)
			addresses[0].SocketData[0] = 1
			next.CrushData()[0] ^= 1
			pool, _ := next.PoolByID(2)
			pool.applicationMetadata["rados"]["purpose"] = "changed"
			if !next.Equivalent(retained) {
				test.Fatal("public mutable getter aliases published snapshot")
			}
			pg := PG{Pool: 2, Seed: 0, Preferred: -1}
			changes := &OSDMapIncremental{
				fsid: next.fsid, epoch: next.epoch + 1, newPoolMax: -1, newFlags: -1,
				newMaxOSD: next.maxOSD + 1,
				newPools:  map[int64]Pool{2: pool}, newPoolNames: map[int64]string{2: "final"},
				newState: map[int32]uint32{0: osdStateExists}, newWeight: map[int32]uint32{1: 0},
				newPrimaryAffinity: map[int32]uint32{2: 0}, newUpClient: map[int32]protocol.EntityAddrVec{3: addresses},
				newPGTemp: map[PG][]int32{pg: {1, 3}}, newPrimaryTemp: map[PG]int32{pg: 1},
				newPGUpmap: map[PG][]int32{pg: {1, 3}}, newPGUpmapItems: map[PG][]OSDRemap{pg: {{From: 0, To: 3}}},
				newPGUpmapPrimaries: map[PG]int32{pg: 1},
			}
			advanced, err := ApplyOSDMapIncremental(next, changes, limits)
			if err != nil {
				test.Fatal(err)
			}
			pool.applicationMetadata["rados"]["purpose"] = "caller mutation"
			addresses[0].SocketData[0] = 2
			if advanced.pools[2].applicationMetadata["rados"]["purpose"] != "changed" || advanced.clientAddresses[3][0].SocketData[0] != 1 {
				test.Fatal("new incremental aliases caller-owned data")
			}
			failed := &OSDMapIncremental{
				fsid: next.fsid, epoch: next.epoch + 1, newPoolMax: -1, newFlags: -1, newMaxOSD: -1,
				newWeight: map[int32]uint32{0: 0}, newPrimaryAffinity: map[int32]uint32{next.maxOSD: 0},
			}
			if _, err := ApplyOSDMapIncremental(next, failed, limits); err == nil {
				test.Fatal("invalid incremental succeeded")
			}
			if !base.Equivalent(before) || !next.Equivalent(retained) || incremental.newPoolNames[2] != "archive" {
				test.Fatal("later or failed incremental mutated an earlier snapshot")
			}
		})
	}
}

func BenchmarkPerformancePlacement(benchmark *testing.B) {
	for _, unrelatedBuckets := range []int{0, 64, 1024, 4096} {
		benchmark.Run(fmt.Sprintf("UnrelatedBuckets=%d", unrelatedBuckets), func(benchmark *testing.B) {
			base := performanceMapFixture(benchmark, 4100, 0, encodePerformancePlacementCrush(benchmark, unrelatedBuckets))
			benchmark.ReportAllocs()
			benchmark.ResetTimer()
			for iteration := 0; iteration < benchmark.N; iteration++ {
				if _, err := base.PlaceRawHash(2, 0x12345678); err != nil {
					benchmark.Fatal(err)
				}
			}
			benchmark.StopTimer()
			benchmark.ReportMetric(float64(len(base.crushData)), "topology-B")
			benchmark.ReportMetric(float64(3+unrelatedBuckets), "buckets")
			benchmark.ReportMetric(float64(base.maxOSD), "osds")
		})
	}
}

func BenchmarkPerformanceIncremental(benchmark *testing.B) {
	for _, fixtureCase := range performanceIncrementalCases() {
		benchmark.Run(fixtureCase.name, func(benchmark *testing.B) {
			base := performanceMapFixture(benchmark, fixtureCase.osdCount, fixtureCase.overrideCount, encodePlacementCrushMap(benchmark))
			incremental, limits := performanceRenameFixture(base, fixtureCase.overrideCount)
			benchmark.ReportAllocs()
			benchmark.ResetTimer()
			for iteration := 0; iteration < benchmark.N; iteration++ {
				if _, err := ApplyOSDMapIncremental(base, incremental, limits); err != nil {
					benchmark.Fatal(err)
				}
			}
			benchmark.StopTimer()
			benchmark.ReportMetric(float64(len(base.crushData)), "topology-B")
			benchmark.ReportMetric(float64(fixtureCase.osdCount), "osds")
			benchmark.ReportMetric(float64(12*fixtureCase.osdCount), "osd-scalar-B")
			benchmark.ReportMetric(float64(16*fixtureCase.osdCount), "socket-B")
			benchmark.ReportMetric(float64(fixtureCase.overrideCount), "override-pgs")
			benchmark.ReportMetric(float64(5*fixtureCase.overrideCount), "override-entries")
			benchmark.ReportMetric(float64(32*fixtureCase.overrideCount), "override-value-B")
		})
	}
}
