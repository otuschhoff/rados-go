package crush

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"

	wire "github.com/otuschhoff/rados-go/internal/encoding"
)

func TestPlacementMapMatchesMutableMap(t *testing.T) {
	data := encodeTestMap(t, BucketStraw2, RuleChooseFirstN)
	immutable, err := DecodePlacementMap(data, testDecodeLimits)
	if err != nil {
		t.Fatal(err)
	}
	mutable, err := DecodeMap(data, testDecodeLimits)
	if err != nil {
		t.Fatal(err)
	}
	weights := []uint32{0x10000, 0x10000, 0x10000}
	for seed := uint32(0); seed < 32; seed++ {
		want, wantErr := mutable.Place(0, seed, 2, weights)
		got, gotErr := immutable.Place(0, seed, 2, weights)
		if wantErr != nil || gotErr != nil || len(want) != 2 || len(got) != 2 || !reflect.DeepEqual(got, want) {
			t.Fatalf("seed %d: got %v/%v, want %v/%v", seed, got, gotErr, want, wantErr)
		}
		got[0] = -123
		again, err := immutable.Place(0, seed, 2, weights)
		if err != nil || !reflect.DeepEqual(again, want) {
			t.Fatalf("result alias: %v/%v", again, err)
		}
	}
}

func TestPlacementMapOwnsDecodedData(t *testing.T) {
	data := encodeTestMap(t, BucketStraw2, RuleChooseFirstN)
	immutable, err := DecodePlacementMap(data, testDecodeLimits)
	if err != nil {
		t.Fatal(err)
	}
	weights := []uint32{0x10000, 0x10000, 0x10000}
	want, err := immutable.Place(0, 17, 2, weights)
	if err != nil || len(want) != 2 {
		t.Fatalf("initial placement: %v/%v", want, err)
	}
	clear(data)
	got, err := immutable.Place(0, 17, 2, weights)
	if err != nil || len(got) != 2 || !reflect.DeepEqual(got, want) {
		t.Fatalf("input alias: %v/%v", got, err)
	}
}

func TestPlacementMapRuleErrorsAreIndependent(t *testing.T) {
	data := encodePlacementRuleProfiles(t)
	immutable, err := DecodePlacementMap(data, testDecodeLimits)
	if err != nil {
		t.Fatal(err)
	}
	mutable, err := DecodeMap(data, testDecodeLimits)
	if err != nil {
		t.Fatal(err)
	}
	for _, ruleID := range []uint32{1, 2} {
		_, first := immutable.Place(ruleID, 0, 2, nil)
		_, again := immutable.Place(ruleID, 1, 2, nil)
		_, mutableErr := mutable.Place(ruleID, 0, 2, nil)
		if !errors.Is(first, ErrPlacement) || first != again || mutableErr == nil || first.Error() != mutableErr.Error() {
			t.Fatalf("rule %d errors: %v / %v / %v", ruleID, first, again, mutableErr)
		}
	}
	weights := []uint32{0x10000, 0x10000, 0x10000}
	got, err := immutable.Place(0, 19, 2, weights)
	want, wantErr := mutable.Place(0, 19, 2, weights)
	if err != nil || wantErr != nil || len(got) != 2 || len(want) != 2 || !reflect.DeepEqual(got, want) {
		t.Fatalf("supported rule: %v/%v want %v/%v", got, err, want, wantErr)
	}
}

func encodePlacementRuleProfiles(t *testing.T) []byte {
	t.Helper()
	encoder := wire.NewEncoder(testDecodeLimits.MaxBytes)
	encoder.Uint32(Magic)
	encoder.Int32(1)
	encoder.Uint32(3)
	encoder.Int32(3)
	encoder.Uint32(BucketStraw2)
	encoder.Int32(-1)
	encoder.Uint16(1)
	encoder.Uint8(BucketStraw2)
	encoder.Uint8(HashRJenkins1)
	encoder.Uint32(3 * 0x10000)
	encoder.Uint32(3)
	for item := range int32(3) {
		encoder.Int32(item)
	}
	for range 3 {
		encoder.Uint32(0x10000)
	}
	for ruleID := range uint8(3) {
		encoder.Uint32(1)
		encoder.Uint32(3)
		encoder.Uint8(ruleID)
		ruleType := uint8(RuleTypeReplicated)
		if ruleID == 2 {
			ruleType = 99
		}
		encoder.Uint8(ruleType)
		encoder.Uint8(1)
		encoder.Uint8(10)
		choose := uint32(RuleChooseFirstN)
		if ruleID == 1 {
			choose = RuleChooseleafIndep
		}
		for _, step := range []RuleStep{{Operation: RuleTake, Argument1: -1}, {Operation: choose}, {Operation: RuleEmit}} {
			encoder.Uint32(step.Operation)
			encoder.Int32(step.Argument1)
			encoder.Int32(step.Argument2)
		}
	}
	for range 5 {
		encoder.Uint32(0)
	}
	encoder.Uint32(50)
	encoder.Uint32(1)
	encoder.Uint8(1)
	encoder.Uint8(1)
	encoder.Uint32(1 << BucketStraw2)
	encoder.Uint8(1)
	data, err := encoder.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestPlacementMapInvalidInputs(t *testing.T) {
	for _, immutable := range []*PlacementMap{nil, {}} {
		if _, err := immutable.Place(0, 0, 2, nil); !errors.Is(err, ErrPlacement) {
			t.Fatalf("zero map: %v", err)
		}
	}
	data := encodeTestMap(t, BucketStraw2, RuleChooseFirstN)
	immutable, err := DecodePlacementMap(data, testDecodeLimits)
	if err != nil {
		t.Fatal(err)
	}
	for _, replicas := range []int{-1, 0, maxCertifiedReplicas + 1} {
		if _, err := immutable.Place(0, 0, replicas, nil); !errors.Is(err, ErrPlacement) {
			t.Fatalf("replicas %d: %v", replicas, err)
		}
	}
	if _, err := immutable.Place(99, 0, 2, nil); !errors.Is(err, ErrPlacement) {
		t.Fatalf("missing rule: %v", err)
	}
	if _, err := DecodePlacementMap(data[:20], testDecodeLimits); !errors.Is(err, wire.ErrMalformed) {
		t.Fatalf("truncated: %v", err)
	}
	if _, err := DecodePlacementMap(data, DecodeLimits{}); !errors.Is(err, wire.ErrLimitExceeded) {
		t.Fatalf("limits: %v", err)
	}
}

func TestMutableMapStillRevalidates(t *testing.T) {
	weights := []uint32{0x10000, 0x10000, 0x10000}
	for _, mutation := range []string{"item-shape", "weight-shape", "nil-items", "nil-weights", "nil-buckets", "bucket-id", "bucket-key", "missing-bucket", "graph", "rule", "tunables"} {
		t.Run(mutation, func(t *testing.T) {
			mutable, err := DecodeMap(encodeTestMap(t, BucketStraw2, RuleChooseFirstN), testDecodeLimits)
			if err != nil {
				t.Fatal(err)
			}
			if got, err := mutable.Place(0, 0, 2, weights); err != nil || len(got) != 2 {
				t.Fatalf("initial placement: %v/%v", got, err)
			}
			switch mutation {
			case "item-shape", "weight-shape", "nil-items", "nil-weights", "bucket-id":
				bucket := mutable.Buckets[-1]
				switch mutation {
				case "item-shape":
					bucket.Items = bucket.Items[:1]
				case "weight-shape":
					bucket.ItemWeights = bucket.ItemWeights[:1]
				case "nil-items":
					bucket.Items = nil
				case "nil-weights":
					bucket.ItemWeights = nil
				case "bucket-id":
					bucket.ID = -2
				}
				mutable.Buckets[-1] = bucket
			case "nil-buckets":
				mutable.Buckets = nil
			case "bucket-key":
				bucket := mutable.Buckets[-1]
				bucket.ID = 0
				mutable.Buckets[0] = bucket
			case "missing-bucket":
				mutable.Buckets[-1].Items[0] = -2
			case "graph":
				mutable.Buckets[-1].Items[0] = -1
			case "rule":
				mutable.Rules[0].Steps[1].Operation = RuleChooseleafIndep
			case "tunables":
				mutable.ChooseTotalTries = 51
			}
			if _, err := mutable.Place(0, 0, 2, weights); !errors.Is(err, ErrPlacement) {
				t.Fatalf("mutation accepted: %v", err)
			}
		})
	}
}

func TestPlacementMapGoldenCorpus(t *testing.T) {
	for _, corpus := range []struct {
		phase   string
		rule    uint32
		weights []uint32
	}{
		{"p05", 0, []uint32{0x10000, 0x10000, 0x10000, 0x10000}},
		{"p10", 2, []uint32{0x10000, 0x10000, 0x10000}},
	} {
		t.Run(corpus.phase, func(t *testing.T) {
			data, err := os.ReadFile("../../testdata/" + corpus.phase + "/crushmap.bin")
			if err != nil {
				t.Fatal(err)
			}
			immutable, err := DecodePlacementMap(data, DecodeLimits{MaxBytes: 1 << 20, MaxBuckets: 1024, MaxRules: 256, MaxItems: 65536, MaxNames: 65536})
			if err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"mappings.txt", "mappings-osd1-out.txt"} {
				weights := append([]uint32(nil), corpus.weights...)
				if name == "mappings-osd1-out.txt" {
					weights[1] = 0
				}
				file, err := os.Open("../../testdata/" + corpus.phase + "/" + name)
				if err != nil {
					t.Fatal(err)
				}
				scanner := bufio.NewScanner(file)
				count := 0
				for scanner.Scan() {
					var rule, seed uint32
					var encoded string
					if _, err := fmt.Sscanf(scanner.Text(), "CRUSH rule %d x %d %s", &rule, &seed, &encoded); err != nil {
						t.Fatal(err)
					}
					want := make([]int32, 0, 3)
					for _, value := range strings.Split(strings.Trim(encoded, "[]"), ",") {
						if value == "" {
							continue
						}
						parsed, err := strconv.ParseInt(value, 10, 32)
						if err != nil {
							t.Fatal(err)
						}
						want = append(want, int32(parsed))
					}
					got, err := immutable.Place(corpus.rule, seed, 3, weights)
					if rule != corpus.rule || err != nil || !reflect.DeepEqual(got, want) {
						t.Fatalf("%s seed %d: %v/%v want %v", name, seed, got, err, want)
					}
					count++
				}
				readErr := scanner.Err()
				file.Close()
				if readErr != nil || count != 256 {
					t.Fatalf("oracle rows %d: %v", count, readErr)
				}
			}
		})
	}
}
