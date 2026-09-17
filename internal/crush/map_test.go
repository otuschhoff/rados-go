package crush

import (
	"errors"
	"testing"

	wire "github.com/otuschhoff/rados-go/internal/encoding"
)

var testDecodeLimits = DecodeLimits{MaxBytes: 4096, MaxBuckets: 16, MaxRules: 16, MaxItems: 64, MaxNames: 64}

func TestDecodeStraw2Map(t *testing.T) {
	decoded, err := DecodeMap(encodeTestMap(t, BucketStraw2, RuleChooseleafFirstN), testDecodeLimits)
	if err != nil {
		t.Fatal(err)
	}
	bucket := decoded.Buckets[-1]
	rule := decoded.Rules[0]
	if decoded.MaxDevices != 3 || bucket.Type != 1 || len(bucket.Items) != 3 || bucket.Items[2] != 2 || bucket.ItemWeights[0] != 0x10000 {
		t.Fatalf("map=%+v bucket=%+v", decoded, bucket)
	}
	if len(rule.Steps) != 3 || rule.Steps[1].Operation != RuleChooseleafFirstN || decoded.ChooseTotalTries != 50 || decoded.ChooseleafStable != 1 {
		t.Fatalf("rule=%+v map=%+v", rule, decoded)
	}
}

func TestDecodeMapRejectsUnsupportedFeaturesAndBounds(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want error
	}{
		{name: "bucket", data: encodeTestMap(t, 4, RuleChooseleafFirstN), want: ErrUnsupported},
		{name: "truncated", data: encodeTestMap(t, BucketStraw2, RuleChooseleafFirstN)[:20], want: wire.ErrMalformed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := DecodeMap(test.data, testDecodeLimits); !errors.Is(err, test.want) {
				t.Fatalf("error=%v want=%v", err, test.want)
			}
		})
	}
}

func TestUnsupportedRuleRejectsAtExecution(t *testing.T) {
	decoded, err := DecodeMap(encodeTestMap(t, BucketStraw2, 7), testDecodeLimits)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decoded.Place(0, 1, 2, []uint32{0x10000, 0x10000, 0x10000}); !errors.Is(err, ErrPlacement) {
		t.Fatalf("error=%v want ErrPlacement", err)
	}
}

func encodeTestMap(t *testing.T, algorithm, chooseOperation uint32) []byte {
	t.Helper()
	encoder := wire.NewEncoder(testDecodeLimits.MaxBytes)
	encoder.Uint32(Magic)
	encoder.Int32(1)
	encoder.Uint32(1)
	encoder.Int32(3)
	encoder.Uint32(algorithm)
	if algorithm != 0 {
		encoder.Int32(-1)
		encoder.Uint16(1)
		encoder.Uint8(uint8(algorithm))
		encoder.Uint8(HashRJenkins1)
		encoder.Uint32(3 * 0x10000)
		encoder.Uint32(3)
		for item := range int32(3) {
			encoder.Int32(item)
		}
		for range 3 {
			encoder.Uint32(0x10000)
		}
	}
	encoder.Uint32(1)
	encoder.Uint32(3)
	encoder.Uint8(0)
	encoder.Uint8(RuleTypeReplicated)
	encoder.Uint8(1)
	encoder.Uint8(10)
	encoder.Uint32(RuleTake)
	encoder.Int32(-1)
	encoder.Int32(0)
	encoder.Uint32(chooseOperation)
	encoder.Int32(0)
	encoder.Int32(0)
	encoder.Uint32(RuleEmit)
	encoder.Int32(0)
	encoder.Int32(0)
	for range 3 {
		encoder.Uint32(0)
	}
	encoder.Uint32(0)
	encoder.Uint32(0)
	encoder.Uint32(50)
	encoder.Uint32(1)
	encoder.Uint8(1)
	encoder.Uint8(1)
	encoder.Uint32(1 << BucketStraw2)
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
