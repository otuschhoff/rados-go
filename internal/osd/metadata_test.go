package osd

import (
	"bytes"
	"encoding/hex"
	"errors"
	"testing"

	wire "github.com/otuschhoff/go-librados/internal/encoding"
)

func TestMetadataPayloadFixtures(t *testing.T) {
	if OpOmapGetHeader != 0x1213 || OpOmapGetValuesByKeys != 0x1214 || OpOmapRemoveRange != 0x222c {
		t.Fatal("OMAP opcode fixture mismatch")
	}
	t.Run("list request", func(t *testing.T) {
		got, err := EncodeOMAPListRequest([]byte("after"), 3, 128)
		assertMetadataFixture(t, got, err, "050000006166746572030000000000000000000000")
	})
	t.Run("map sorted", func(t *testing.T) {
		got, err := EncodeMetadataMap([]MetadataEntry{{Key: []byte("b"), Value: []byte("two")}, {Key: []byte("a"), Value: []byte("one")}}, 128)
		assertMetadataFixture(t, got, err, "020000000100000061030000006f6e6501000000620300000074776f")
	})
	t.Run("set sorted", func(t *testing.T) {
		got, err := EncodeMetadataKeys([][]byte{[]byte("b"), []byte("a")}, 128)
		assertMetadataFixture(t, got, err, "0200000001000000610100000062")
	})
	t.Run("compare", func(t *testing.T) {
		got, err := EncodeOMAPCompare([]byte("key"), []byte("value"), 1, 128)
		assertMetadataFixture(t, got, err, "01000000030000006b65790500000076616c756501000000")
	})
}

func TestDecodeOMAPPageFixture(t *testing.T) {
	data, err := hex.DecodeString("020000000100000061030000006f6e6501000000620300000074776f01")
	if err != nil {
		t.Fatal(err)
	}
	entries, more, err := DecodeOMAPPage(data, 128, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !more || len(entries) != 2 || string(entries[0].Key) != "a" || string(entries[0].Value) != "one" || string(entries[1].Key) != "b" || string(entries[1].Value) != "two" {
		t.Fatalf("entries=%+v more=%t", entries, more)
	}
}

func TestMetadataCodecsRejectMalformedContainers(t *testing.T) {
	duplicate, _ := hex.DecodeString("02000000010000006100000000010000006100000000")
	unordered, _ := hex.DecodeString("02000000010000006200000000010000006100000000")
	tooMany, _ := hex.DecodeString("02000000")
	trailing, _ := hex.DecodeString("0000000000")
	for _, test := range []struct {
		name string
		data []byte
		max  uint32
		want error
	}{
		{name: "duplicate", data: duplicate, max: 2, want: wire.ErrMalformed},
		{name: "unordered", data: unordered, max: 2, want: wire.ErrMalformed},
		{name: "count", data: tooMany, max: 1, want: wire.ErrLimitExceeded},
		{name: "trailing", data: trailing, max: 1, want: wire.ErrMalformed},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := DecodeMetadataMap(test.data, 128, test.max)
			if !errors.Is(err, test.want) {
				t.Fatalf("error=%v want=%v", err, test.want)
			}
		})
	}
	if _, err := EncodeMetadataKeys([][]byte{[]byte("same"), []byte("same")}, 128); !errors.Is(err, wire.ErrMalformed) {
		t.Fatalf("duplicate encode error=%v", err)
	}
	if _, _, err := DecodeOMAPPage([]byte{0, 0, 0, 0}, 128, 1); err == nil {
		t.Fatal("OMAP page without continuation flag accepted")
	}
}

func assertMetadataFixture(t *testing.T, got []byte, err error, wantHex string) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	want, err := hex.DecodeString(wantHex)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("payload=%x want=%x", got, want)
	}
}

func TestOMAPRangeEncoding(t *testing.T) {
	data, err := EncodeOMAPRange([]byte{0, 'a'}, []byte{0xff}, 64)
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{2, 0, 0, 0, 0, 'a', 1, 0, 0, 0, 0xff}
	if !bytes.Equal(data, want) {
		t.Fatalf("range=%x want=%x", data, want)
	}
	if _, err := EncodeOMAPRange([]byte("z"), []byte("a"), 64); !errors.Is(err, wire.ErrMalformed) {
		t.Fatalf("reversed range error=%v", err)
	}
}

func FuzzMetadataDecoders(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0, 0})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeMetadataMap(data, 4096, 64)
		_, _, _ = DecodeOMAPPage(data, 4096, 64)
	})
}
