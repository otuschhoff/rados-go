package maps

import (
	"os"
	"testing"
)

var p04FixtureLimits = Limits{
	MaxBytes: 32 << 20, MaxMonitors: 64, MaxAddresses: 64, MaxLocations: 64,
	MaxPools: 4096, MaxOSDs: 65536, MaxPGMappings: 1 << 20, MaxCollectionEntries: 1 << 20,
}

func TestP04CephDencoderFixtures(t *testing.T) {
	tests := []struct {
		name   string
		path   string
		decode func([]byte, Limits) error
	}{
		{name: "monmap", path: "../../testdata/p04/monmap-v9.bin", decode: func(data []byte, limits Limits) error {
			_, err := DecodeMonMap(data, limits)
			return err
		}},
		{name: "osdmap", path: "../../testdata/p04/osdmap-v8.bin", decode: func(data []byte, limits Limits) error {
			_, err := DecodeOSDMap(data, limits)
			return err
		}},
		{name: "incremental", path: "../../testdata/p04/osdmap-incremental-v8.bin", decode: func(data []byte, limits Limits) error {
			_, err := DecodeOSDMapIncremental(data, limits)
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data, err := os.ReadFile(test.path)
			if err != nil {
				t.Fatal(err)
			}
			if err := test.decode(data, p04FixtureLimits); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func FuzzDecodeMonMap(f *testing.F) {
	addFixtureSeed(f, "../../testdata/p04/monmap-v9.bin")
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeMonMap(data, p04FixtureLimits)
	})
}

func FuzzDecodeOSDMap(f *testing.F) {
	addFixtureSeed(f, "../../testdata/p04/osdmap-v8.bin")
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeOSDMap(data, p04FixtureLimits)
	})
}

func FuzzDecodeOSDMapIncremental(f *testing.F) {
	addFixtureSeed(f, "../../testdata/p04/osdmap-incremental-v8.bin")
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeOSDMapIncremental(data, p04FixtureLimits)
	})
}

func addFixtureSeed(f *testing.F, path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(data)
}
