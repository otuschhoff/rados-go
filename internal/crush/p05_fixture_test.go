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

func TestP05CrushtoolCorpus(t *testing.T) {
	data, err := os.ReadFile("../../testdata/p05/crushmap.bin")
	if err != nil {
		t.Fatal(err)
	}
	crushMap, err := DecodeMap(data, DecodeLimits{MaxBytes: 1 << 20, MaxBuckets: 1024, MaxRules: 256, MaxItems: 65536, MaxNames: 65536})
	if err != nil {
		t.Fatal(err)
	}
	verifyCrushtoolMappings(t, crushMap, "../../testdata/p05/mappings.txt", []uint32{0x10000, 0x10000, 0x10000, 0x10000})
	verifyCrushtoolMappings(t, crushMap, "../../testdata/p05/mappings-osd1-out.txt", []uint32{0x10000, 0, 0x10000, 0x10000})
}

func verifyCrushtoolMappings(t *testing.T, crushMap *Map, path string, weights []uint32) {
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
		if _, err := fmt.Sscanf(scanner.Text(), "CRUSH rule 0 x %d %s", &seed, &encoded); err != nil {
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
		got, err := crushMap.Place(0, seed, 3, weights)
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

func FuzzDecodeMap(f *testing.F) {
	data, err := os.ReadFile("../../testdata/p05/crushmap.bin")
	if err != nil {
		f.Fatal(err)
	}
	f.Add(data)
	f.Fuzz(func(t *testing.T, input []byte) {
		_, _ = DecodeMap(input, DecodeLimits{MaxBytes: 1 << 20, MaxBuckets: 1024, MaxRules: 256, MaxItems: 65536, MaxNames: 65536})
	})
}

func FuzzDecodeAndPlace(f *testing.F) {
	data, err := os.ReadFile("../../testdata/p05/crushmap.bin")
	if err != nil {
		f.Fatal(err)
	}
	f.Add(data, uint32(0), uint8(3))
	f.Fuzz(func(t *testing.T, input []byte, seed uint32, replicas uint8) {
		crushMap, err := DecodeMap(input, DecodeLimits{MaxBytes: 1 << 20, MaxBuckets: 1024, MaxRules: 256, MaxItems: 65536, MaxNames: 65536})
		if err == nil {
			_, _ = crushMap.Place(0, seed, int(replicas), []uint32{0x10000, 0x10000, 0x10000, 0x10000})
		}
	})
}
