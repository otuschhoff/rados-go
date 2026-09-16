package crush

import (
	"bufio"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestP10ErasureCrushtoolCorpus(t *testing.T) {
	data, err := os.ReadFile("../../testdata/p10/crushmap.bin")
	if err != nil {
		t.Fatal(err)
	}
	crushMap, err := DecodeMap(data, DecodeLimits{MaxBytes: 1 << 20, MaxBuckets: 1024, MaxRules: 256, MaxItems: 65536, MaxNames: 65536})
	if err != nil {
		t.Fatal(err)
	}
	verifyP10CrushtoolMappings(t, crushMap, "../../testdata/p10/mappings.txt", []uint32{0x10000, 0x10000, 0x10000})
	verifyP10CrushtoolMappings(t, crushMap, "../../testdata/p10/mappings-osd1-out.txt", []uint32{0x10000, 0, 0x10000})
}

func verifyP10CrushtoolMappings(t *testing.T, crushMap *Map, path string, weights []uint32) {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	count := 0
	for scanner.Scan() {
		var seed uint32
		var encoded string
		if _, err := fmt.Sscanf(scanner.Text(), "CRUSH rule 2 x %d %s", &seed, &encoded); err != nil {
			t.Fatalf("parse oracle row %q: %v", scanner.Text(), err)
		}
		encoded = strings.TrimSuffix(strings.TrimPrefix(encoded, "["), "]")
		want := make([]int32, 0, 3)
		if encoded != "" {
			for _, value := range strings.Split(encoded, ",") {
				parsed, err := strconv.ParseInt(value, 10, 32)
				if err != nil {
					t.Fatalf("parse oracle row %q: %v", scanner.Text(), err)
				}
				want = append(want, int32(parsed))
			}
		}
		got, err := crushMap.Place(2, seed, 3, weights)
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("seed %d placement=%v want=%v", seed, got, want)
		}
		count++
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 256 {
		t.Fatalf("oracle rows=%d want=256", count)
	}
}
