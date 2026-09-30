package maps

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync"
	"testing"

	"github.com/otuschhoff/rados-go/internal/crush"
)

func TestPlacementReuseConcurrentRoutes(t *testing.T) {
	for _, warm := range []bool{false, true} {
		t.Run(fmt.Sprintf("warm=%t", warm), func(t *testing.T) {
			base := performanceMapFixture(t, 4, 0, encodePlacementCrushMap(t))
			reference := performanceMapFixture(t, 4, 0, encodePlacementCrushMap(t))
			want := make([]ObjectPlacement, 32)
			for index := range want {
				var err error
				want[index], err = reference.PlaceRawHash(2, uint32(index)*2654435761)
				if err != nil {
					t.Fatal(err)
				}
			}
			if warm {
				if _, err := base.PlaceRawHash(2, 0); err != nil {
					t.Fatal(err)
				}
			}
			start := make(chan struct{})
			var workers sync.WaitGroup
			for index := range want {
				workers.Go(func() {
					<-start
					for range 8 {
						got, err := base.PlaceRawHash(2, uint32(index)*2654435761)
						if err != nil || !reflect.DeepEqual(got, want[index]) {
							t.Errorf("hash %d: %+v/%v", index, got, err)
							return
						}
						got.Raw[0], got.Up[0], got.Acting[0] = -91, -92, -93
					}
				})
			}
			close(start)
			workers.Wait()
			state := base.crushPlacementState()
			first, err := state.decode()
			second, againErr := state.decode()
			if err != nil || againErr != nil || first == nil || first != second {
				t.Fatal("decoded map not reused")
			}
		})
	}
}

func TestPlacementReuseColdIncrementalConcurrentRoutes(t *testing.T) {
	base := performanceMapFixture(t, 4, 0, encodePlacementCrushMap(t))
	incremental, limits := performanceRenameFixture(base, 0)
	next, err := ApplyOSDMapIncremental(base, incremental, limits)
	if err != nil {
		t.Fatal(err)
	}
	reference := performanceMapFixture(t, 4, 0, encodePlacementCrushMap(t))
	want := make([]ObjectPlacement, 32)
	for index := range want {
		want[index], err = reference.PlaceRawHash(2, uint32(index)*2654435761)
		if err != nil {
			t.Fatal(err)
		}
	}
	if base.placementState == nil || base.placementState.placementMap != nil || base.placementState.err != nil {
		t.Fatal("incremental did not retain a cold payload state")
	}
	start := make(chan struct{})
	var ready, workers sync.WaitGroup
	for _, snapshot := range []*OSDMap{base, next} {
		for index := range want {
			ready.Add(1)
			workers.Go(func() {
				ready.Done()
				<-start
				got, routeErr := snapshot.PlaceRawHash(2, uint32(index)*2654435761)
				if routeErr != nil || !reflect.DeepEqual(got, want[index]) {
					t.Errorf("epoch %d hash %d: %+v/%v want %+v", snapshot.epoch, index, got, routeErr, want[index])
				}
			})
		}
	}
	ready.Wait()
	close(start)
	workers.Wait()
	baseState, nextState := base.crushPlacementState(), next.crushPlacementState()
	if baseState != nextState || baseState.placementMap == nil || baseState.placementMap != nextState.placementMap {
		t.Fatal("cold snapshots did not share the decoded map")
	}
}

func TestPlacementReuseMalformedError(t *testing.T) {
	base := performanceMapFixture(t, 4, 0, []byte("malformed CRUSH"))
	errorsByWorker := make([]error, 32)
	var workers sync.WaitGroup
	for index := range errorsByWorker {
		workers.Go(func() { _, errorsByWorker[index] = base.PlaceRawHash(2, uint32(index)) })
	}
	workers.Wait()
	for _, err := range errorsByWorker {
		if !errors.Is(err, ErrUnsupportedPlacement) || err != errorsByWorker[0] {
			t.Fatalf("error not memoized: %v", err)
		}
	}
	state := base.crushPlacementState()
	if state.placementMap != nil || state.err != errorsByWorker[0] {
		t.Fatal("malformed payload state not retained")
	}
	_, err := base.PlaceRawHash(2, 99)
	if err != state.err {
		t.Fatal("warm error changed")
	}
}

func TestPlacementReuseSlicesAreIndependent(t *testing.T) {
	base := performanceMapFixture(t, 4, 0, encodePlacementCrushMap(t))
	want, err := base.PlaceRawHash(2, 0)
	if err != nil {
		t.Fatal(err)
	}
	got, err := base.PlaceRawHash(2, 0)
	if err != nil {
		t.Fatal(err)
	}
	got.Raw[0] = -91
	if !reflect.DeepEqual(got.Up, want.Up) || !reflect.DeepEqual(got.Acting, want.Acting) {
		t.Fatal("raw aliases up or acting")
	}
	got.Up[0] = -92
	if !reflect.DeepEqual(got.Acting, want.Acting) {
		t.Fatal("up aliases acting")
	}
	got.Acting[0] = -93
	again, err := base.PlaceRawHash(2, 0)
	if err != nil || !reflect.DeepEqual(again, want) {
		t.Fatal("returned slices alias another call")
	}
}

func TestPlacementReuseEquivalentIgnoresCache(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		t.Run(fmt.Sprintf("malformed=%t", malformed), func(t *testing.T) {
			data := encodePlacementCrushMap(t)
			if malformed {
				data = []byte("malformed CRUSH")
			}
			base := performanceMapFixture(t, 4, 0, data)
			other := performanceMapFixture(t, 4, 0, append([]byte(nil), data...))
			if !base.Equivalent(other) {
				t.Fatal("cold snapshots differ")
			}
			if _, err := base.PlaceRawHash(2, 0); (err != nil) != malformed {
				t.Fatalf("placement error: %v", err)
			}
			if !base.Equivalent(other) {
				t.Fatal("cache changed snapshot equivalence")
			}
			var workers sync.WaitGroup
			workers.Go(func() {
				for range 32 {
					_, _ = other.PlaceRawHash(2, 0)
				}
			})
			workers.Go(func() {
				for range 32 {
					if !base.Equivalent(other) {
						t.Error("concurrent snapshots differ")
					}
				}
			})
			workers.Wait()
		})
	}
}

func TestPlacementReuseIncremental(t *testing.T) {
	for _, warm := range []bool{false, true} {
		for _, equalBytes := range []bool{false, true} {
			t.Run(fmt.Sprintf("warm=%t/equalReplacement=%t", warm, equalBytes), func(t *testing.T) {
				base := performanceMapFixture(t, 4, 0, encodePlacementCrushMap(t))
				if warm {
					if _, err := base.PlaceRawHash(2, 0); err != nil {
						t.Fatal(err)
					}
				}
				incremental, limits := performanceRenameFixture(base, 0)
				if equalBytes {
					incremental.crushData = append([]byte(nil), base.crushData...)
				}
				next, err := ApplyOSDMapIncremental(base, incremental, limits)
				if err != nil {
					t.Fatal(err)
				}
				state := base.crushPlacementState()
				if next.crushPlacementState() != state {
					t.Fatal("unchanged payload did not share state")
				}
				if !warm && (state.placementMap != nil || state.err != nil) {
					t.Fatal("clone decoded cold payload")
				}
				for seed := range uint32(32) {
					want, err := base.PlaceRawHash(2, seed)
					if err != nil {
						t.Fatal(err)
					}
					got, err := next.PlaceRawHash(2, seed)
					if err != nil || !reflect.DeepEqual(got, want) {
						t.Fatalf("seed %d: %v/%v want %v", seed, got, err, want)
					}
				}
				if next.placementState.placementMap != state.placementMap {
					t.Fatal("snapshots decoded separately")
				}
			})
		}
	}
}

func TestPlacementReuseChangedPayload(t *testing.T) {
	replacementData, err := os.ReadFile("../../testdata/p05/crushmap.bin")
	if err != nil {
		t.Fatal(err)
	}
	for _, warm := range []bool{false, true} {
		t.Run(fmt.Sprintf("warm=%t", warm), func(t *testing.T) {
			base := performanceMapFixture(t, 4, 0, encodePlacementCrushMap(t))
			if warm {
				if _, err := base.PlaceRawHash(2, 0); err != nil {
					t.Fatal(err)
				}
			}
			incremental, limits := performanceRenameFixture(base, 0)
			incremental.crushData = replacementData
			next, err := ApplyOSDMapIncremental(base, incremental, limits)
			if err != nil {
				t.Fatal(err)
			}
			if next.crushPlacementState() == base.crushPlacementState() {
				t.Fatal("changed payload reused old state")
			}
			reference := performanceMapFixture(t, 4, 0, append([]byte(nil), replacementData...))
			changed := false
			for seed := range uint32(64) {
				old, err := base.PlaceRawHash(2, seed)
				if err != nil {
					t.Fatal(err)
				}
				want, err := reference.PlaceRawHash(2, seed)
				if err != nil {
					t.Fatal(err)
				}
				got, err := next.PlaceRawHash(2, seed)
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("new route: %+v/%v want %+v", got, err, want)
				}
				changed = changed || !reflect.DeepEqual(old.Raw, got.Raw)
			}
			if !changed || next.placementState.placementMap == base.placementState.placementMap {
				t.Fatal("replacement did not change placement")
			}
		})
	}
}

func TestPlacementReusePayloadErrorTransitions(t *testing.T) {
	valid := encodePlacementCrushMap(t)
	malformed := []byte("malformed CRUSH")
	for _, test := range []struct {
		name                            string
		before, after                   []byte
		beforeMalformed, afterMalformed bool
		shared                          bool
	}{
		{"malformed-to-valid", malformed, valid, true, false, false},
		{"valid-to-malformed", valid, malformed, false, true, false},
		{"unchanged-malformed", malformed, nil, true, true, true},
		{"equal-malformed", malformed, append([]byte(nil), malformed...), true, true, true},
	} {
		for _, warm := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/warm=%t", test.name, warm), func(t *testing.T) {
				base := performanceMapFixture(t, 4, 0, test.before)
				var old ObjectPlacement
				var oldErr error
				if warm {
					old, oldErr = base.PlaceRawHash(2, 7)
					if errors.Is(oldErr, ErrUnsupportedPlacement) != test.beforeMalformed || (oldErr != nil && !test.beforeMalformed) {
						t.Fatalf("old route: %+v/%v", old, oldErr)
					}
				}
				incremental, limits := performanceRenameFixture(base, 0)
				incremental.crushData = test.after
				next, err := ApplyOSDMapIncremental(base, incremental, limits)
				if err != nil {
					t.Fatal(err)
				}
				if (base.crushPlacementState() == next.crushPlacementState()) != test.shared {
					t.Fatal("unexpected payload state identity")
				}
				if !warm {
					old, oldErr = base.PlaceRawHash(2, 7)
					if errors.Is(oldErr, ErrUnsupportedPlacement) != test.beforeMalformed || (oldErr != nil && !test.beforeMalformed) {
						t.Fatalf("old route: %+v/%v", old, oldErr)
					}
				}
				got, routeErr := next.PlaceRawHash(2, 7)
				if test.afterMalformed {
					if !errors.Is(routeErr, ErrUnsupportedPlacement) {
						t.Fatalf("new error: %v", routeErr)
					}
					if test.shared && routeErr != oldErr {
						t.Fatal("unchanged malformed clone lost cached error identity")
					}
					if _, againErr := next.PlaceRawHash(2, 19); againErr != routeErr {
						t.Fatal("new malformed error was not memoized")
					}
				} else {
					reference := performanceMapFixture(t, 4, 0, append([]byte(nil), valid...))
					want, referenceErr := reference.PlaceRawHash(2, 7)
					if routeErr != nil || referenceErr != nil || !reflect.DeepEqual(got, want) {
						t.Fatalf("replacement route: %+v/%v want %+v/%v", got, routeErr, want, referenceErr)
					}
				}
				again, againErr := base.PlaceRawHash(2, 7)
				if againErr != oldErr || !reflect.DeepEqual(again, old) || !reflect.DeepEqual(base.crushData, test.before) {
					t.Fatal("replacement changed old snapshot")
				}
			})
		}
	}
}

func TestPlacementReuseSnapshotMetadata(t *testing.T) {
	p10Data, err := os.ReadFile("../../testdata/p10/crushmap.bin")
	if err != nil {
		t.Fatal(err)
	}
	for _, fixture := range []struct {
		name                 string
		data                 []byte
		poolType, size, rule uint8
		flags                uint64
	}{
		{"replicated", encodePlacementCrushMap(t), poolTypeReplicated, 2, 0, poolFlagHashPSPool},
		{"p10-erasure", p10Data, poolTypeErasure, 3, 2, poolFlagHashPSPool},
		{"p10-erasure-optimized", p10Data, poolTypeErasure, 3, 2, poolFlagHashPSPool | poolFlagECOptimizations},
	} {
		for _, change := range []string{"weight", "affinity", "down", "upmap", "upmap-items", "upmap-primary", "pg-temp", "primary-temp", "optimized-pg-temp-primary"} {
			if change == "optimized-pg-temp-primary" && fixture.flags&poolFlagECOptimizations == 0 {
				continue
			}
			t.Run(fixture.name+"/"+change, func(t *testing.T) {
				makeBase := func() *OSDMap {
					base := performanceMapFixture(t, 4, 0, append([]byte(nil), fixture.data...))
					pool := base.pools[2]
					pool.poolType, pool.size, pool.crushRule, pool.flags = fixture.poolType, fixture.size, fixture.rule, fixture.flags
					if fixture.flags&poolFlagECOptimizations != 0 {
						pool.nonprimaryShards = [2]uint64{1 << 1}
					}
					base.pools[2] = pool
					return base
				}
				base := makeBase()
				old, err := base.PlaceRawHash(2, 7)
				if err != nil || len(old.Up) != int(fixture.size) {
					t.Fatalf("base route: %+v/%v", old, err)
				}
				incremental, limits := performanceRenameFixture(base, 0)
				primary, secondary := old.UpPrimary, old.Up[1]
				switch change {
				case "weight":
					incremental.newWeight = map[int32]uint32{primary: 0}
				case "affinity":
					incremental.newPrimaryAffinity = map[int32]uint32{primary: 0}
				case "down":
					incremental.newState = map[int32]uint32{primary: osdStateUp}
				case "upmap":
					replacement := append([]int32(nil), old.Up...)
					replacement[0], replacement[1] = replacement[1], replacement[0]
					incremental.newPGUpmap = map[PG][]int32{old.PG: replacement}
				case "upmap-items":
					incremental.newPGUpmapItems = map[PG][]OSDRemap{old.PG: {{From: primary, To: 3}}}
				case "upmap-primary":
					incremental.newPGUpmapPrimaries = map[PG]int32{old.PG: secondary}
				case "pg-temp":
					replacement := append([]int32(nil), old.Up...)
					replacement[0], replacement[1] = replacement[1], replacement[0]
					incremental.newPGTemp = map[PG][]int32{old.PG: replacement}
				case "primary-temp":
					incremental.newPrimaryTemp = map[PG]int32{old.PG: secondary}
				case "optimized-pg-temp-primary":
					incremental.newPGTemp = map[PG][]int32{old.PG: {old.Up[1], old.Up[0], old.Up[2]}}
					incremental.newPrimaryTemp = map[PG]int32{old.PG: old.Up[2]}
				}
				next, err := ApplyOSDMapIncremental(base, incremental, limits)
				if err != nil {
					t.Fatal(err)
				}
				reference, err := ApplyOSDMapIncremental(makeBase(), incremental, limits)
				if err != nil {
					t.Fatal(err)
				}
				got, err := next.PlaceRawHash(2, 7)
				want, referenceErr := reference.PlaceRawHash(2, 7)
				if err != nil || referenceErr != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("incremental route: %+v/%v want %+v/%v", got, err, want, referenceErr)
				}
				if reflect.DeepEqual(got, old) {
					t.Fatal("metadata increment did not change the route")
				}
				decoded, err := crush.DecodeMap(fixture.data, crush.DecodeLimits{MaxBytes: 1 << 20, MaxBuckets: 1024, MaxRules: 256, MaxItems: 65536, MaxNames: 65536})
				if err != nil {
					t.Fatal(err)
				}
				raw, err := decoded.Place(uint32(fixture.rule), got.PlacementSeed, int(fixture.size), next.osdWeight)
				if err != nil || !reflect.DeepEqual(got.Raw, raw) {
					t.Fatalf("legacy CRUSH reference: %v/%v got %v", raw, err, got.Raw)
				}
				if change == "affinity" || change == "primary-temp" || change == "upmap-primary" {
					if got.ActingPrimary != secondary {
						t.Fatalf("primary=%d want=%d", got.ActingPrimary, secondary)
					}
				}
				if fixture.poolType == poolTypeErasure {
					if !got.Sharded || got.PrimaryShard < 0 || int(got.PrimaryShard) >= len(got.Acting) || got.Acting[got.PrimaryShard] != got.ActingPrimary {
						t.Fatalf("incorrect EC primary shard: %+v", got)
					}
					if (change == "affinity" || change == "primary-temp") && (!reflect.DeepEqual(got.Acting, old.Acting) || got.PrimaryShard != 1) {
						t.Fatalf("EC primary override reordered shards: %+v", got)
					}
					if change == "optimized-pg-temp-primary" && (!reflect.DeepEqual(got.Acting, []int32{old.Up[1], old.Up[2], old.Up[0]}) || got.ActingPrimary != old.Up[2] || got.PrimaryShard != 1) {
						t.Fatalf("optimized EC primary-first translation: %+v", got)
					}
				}
				if next.crushPlacementState() != base.crushPlacementState() || next.placementState.placementMap != base.placementState.placementMap || next.placementState.placementMap == reference.placementState.placementMap {
					t.Fatal("unexpected shared or reference decode identity")
				}
				oldAgain, err := base.PlaceRawHash(2, 7)
				if err != nil || !reflect.DeepEqual(oldAgain, old) {
					t.Fatalf("old snapshot changed: %+v/%v want %+v", oldAgain, err, old)
				}
				for _, result := range [][]int32{got.Raw, got.Up, got.Acting} {
					for index := range result {
						result[index] = -91
					}
					if !reflect.DeepEqual(oldAgain, old) {
						t.Fatal("returned slices alias across snapshots")
					}
				}
				for _, route := range []*ObjectPlacement{&oldAgain} {
					for _, result := range [][]int32{route.Raw, route.Up, route.Acting} {
						for index := range result {
							result[index] = -91
						}
					}
				}
				for _, snapshot := range []struct {
					snapshot *OSDMap
					want     ObjectPlacement
				}{{base, old}, {next, want}} {
					again, err := snapshot.snapshot.PlaceRawHash(2, 7)
					if err != nil || !reflect.DeepEqual(again, snapshot.want) {
						t.Fatal("returned slices alias snapshot metadata or another snapshot")
					}
				}
			})
		}
	}
}

func TestPlacementReuseP05GoldenSnapshots(t *testing.T) {
	data, err := os.ReadFile("../../testdata/p05/crushmap.bin")
	if err != nil {
		t.Fatal(err)
	}
	base := &OSDMap{
		fsid: FSID{1}, epoch: 1, poolMax: 1,
		pools: map[int64]Pool{1: {id: 1, name: "p05", poolType: poolTypeReplicated, size: 3, crushRule: 0,
			objectHash: objectHashRJenkins, pgCount: 256, placementPGCount: 256, flags: poolFlagHashPSPool}},
		nameToID: map[string]int64{"p05": 1}, maxOSD: 4,
		osdState: []uint32{3, 3, 3, 3}, osdWeight: []uint32{0x10000, 0x10000, 0x10000, 0x10000}, crushData: data,
	}
	verifyObjectMappings(t, base, "../../testdata/p05/object-mappings.txt")
	pool := base.pools[1]
	pool.pgCount, pool.placementPGCount = 32, 32
	pg32, err := ApplyOSDMapIncremental(base, &OSDMapIncremental{
		fsid: base.fsid, epoch: 2, newPoolMax: -1, newFlags: -1, newMaxOSD: -1, newPools: map[int64]Pool{1: pool},
	}, testOSDMapLimits)
	if err != nil {
		t.Fatal(err)
	}
	verifyObjectMappings(t, pg32, "../../testdata/p05/object-mappings-pg32.txt")
	out, err := ApplyOSDMapIncremental(pg32, &OSDMapIncremental{
		fsid: base.fsid, epoch: 3, newPoolMax: -1, newFlags: -1, newMaxOSD: -1, newWeight: map[int32]uint32{1: 0},
	}, testOSDMapLimits)
	if err != nil {
		t.Fatal(err)
	}
	verifyObjectMappings(t, out, "../../testdata/p05/object-mappings-osd1-out.txt")
	upmap, err := ApplyOSDMapIncremental(base, &OSDMapIncremental{
		fsid: base.fsid, epoch: 2, newPoolMax: -1, newFlags: -1, newMaxOSD: -1,
		newPGUpmapItems: readUpmapCommands(t, "../../testdata/p05/upmap-commands.txt"),
	}, p04FixtureLimits)
	if err != nil {
		t.Fatal(err)
	}
	verifyObjectMappings(t, upmap, "../../testdata/p05/object-mappings-upmap.txt")
	verifyObjectMappings(t, base, "../../testdata/p05/object-mappings.txt")
	verifyObjectMappings(t, pg32, "../../testdata/p05/object-mappings-pg32.txt")
	for _, snapshot := range []*OSDMap{pg32, out, upmap} {
		if snapshot.crushPlacementState() != base.crushPlacementState() || snapshot.placementState.placementMap != base.placementState.placementMap {
			t.Fatal("golden snapshots did not reuse decoded payload")
		}
	}
}

func TestPlacementReuseP10GoldenWeights(t *testing.T) {
	data, err := os.ReadFile("../../testdata/p10/crushmap.bin")
	if err != nil {
		t.Fatal(err)
	}
	base := performanceMapFixture(t, 4, 0, data)
	incremental, limits := performanceRenameFixture(base, 0)
	incremental.newWeight = map[int32]uint32{1: 0}
	next, err := ApplyOSDMapIncremental(base, incremental, limits)
	if err != nil {
		t.Fatal(err)
	}
	verify := func(snapshot *OSDMap, path string) {
		t.Helper()
		file, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		decoded, err := snapshot.crushPlacementState().decode()
		if err != nil {
			t.Fatal(err)
		}
		scanner := bufio.NewScanner(file)
		count := 0
		for scanner.Scan() {
			var seed uint32
			var encoded string
			if _, err := fmt.Sscanf(scanner.Text(), "CRUSH rule 2 x %d %s", &seed, &encoded); err != nil {
				t.Fatal(err)
			}
			want := parseOSDList(t, encoded)
			got, err := decoded.Place(2, seed, 3, snapshot.osdWeight)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("%s seed %d: %v/%v want %v", path, seed, got, err, want)
			}
			count++
		}
		if err := scanner.Err(); err != nil {
			t.Fatal(err)
		}
		if count != 256 {
			t.Fatalf("golden rows=%d want=256", count)
		}
	}
	verify(base, "../../testdata/p10/mappings.txt")
	verify(next, "../../testdata/p10/mappings-osd1-out.txt")
	verify(base, "../../testdata/p10/mappings.txt")
	if base.crushPlacementState() != next.crushPlacementState() || base.placementState.placementMap != next.placementState.placementMap {
		t.Fatal("P10 weight increment did not share decoded payload")
	}
}

func TestPlacementReuseFullReplacement(t *testing.T) {
	base, err := DecodeOSDMap(encodeTestOSDMapNamed(t, 11, "data"), testOSDMapLimits)
	if err != nil {
		t.Fatal(err)
	}
	fullData := encodeTestOSDMapNamed(t, 12, "archive")
	full, err := DecodeOSDMap(fullData, testOSDMapLimits)
	if err != nil {
		t.Fatal(err)
	}
	incremental := &OSDMapIncremental{fsid: base.fsid, epoch: 12, fullMap: fullData, fullCRC: full.CRC()}
	next, err := ApplyOSDMapIncremental(base, incremental, testOSDMapLimits)
	if err != nil {
		t.Fatal(err)
	}
	if next.crushPlacementState() == base.crushPlacementState() {
		t.Fatal("full replacement shared state")
	}
}

func BenchmarkPlacementReuseWarm(benchmark *testing.B) {
	for _, unrelated := range []int{0, 64, 1024, 4096} {
		benchmark.Run(fmt.Sprintf("UnrelatedBuckets=%d", unrelated), func(benchmark *testing.B) {
			base := performanceMapFixture(benchmark, 4100, 0, encodePerformancePlacementCrush(benchmark, unrelated))
			want, err := performanceMapFixture(benchmark, 4100, 0, encodePlacementCrushMap(benchmark)).PlaceRawHash(2, 0x12345678)
			if err != nil {
				benchmark.Fatal(err)
			}
			got, err := base.PlaceRawHash(2, 0x12345678)
			if err != nil || !reflect.DeepEqual(got, want) {
				benchmark.Fatalf("warm route %+v/%v want %+v", got, err, want)
			}
			benchmark.ReportAllocs()
			benchmark.ResetTimer()
			for iteration := 0; iteration < benchmark.N; iteration++ {
				if _, err := base.PlaceRawHash(2, 0x12345678); err != nil {
					benchmark.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkPlacementReuseColdDecode(benchmark *testing.B) {
	for _, unrelated := range []int{0, 64, 1024, 4096} {
		benchmark.Run(fmt.Sprintf("UnrelatedBuckets=%d", unrelated), func(benchmark *testing.B) {
			data := encodePerformancePlacementCrush(benchmark, unrelated)
			weights := make([]uint32, 4100)
			for index := range weights {
				weights[index] = 0x10000
			}
			benchmark.ReportAllocs()
			benchmark.ResetTimer()
			for iteration := 0; iteration < benchmark.N; iteration++ {
				state := &crushState{data: data}
				placementMap, err := state.decode()
				if err != nil {
					benchmark.Fatal(err)
				}
				if _, err := placementMap.Place(0, 0x12345678, 2, weights); err != nil {
					benchmark.Fatal(err)
				}
			}
		})
	}
}
