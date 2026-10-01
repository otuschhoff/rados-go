package encoding

import "testing"

var benchmarkEncodingResult []byte

func BenchmarkVersionedNested(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		encoder := NewEncoder(4096)
		encoder.Versioned(3, 1, func(outer *Encoder) {
			outer.Uint64(42)
			outer.Versioned(2, 1, func(inner *Encoder) {
				inner.String("object")
				inner.Uint64(4096)
			})
		})
		result, err := encoder.BytesResult()
		if err != nil {
			b.Fatal(err)
		}
		benchmarkEncodingResult = result
	}
}
