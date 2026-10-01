package encoding

import (
	"bytes"
	"errors"
	"math"
	"testing"
)

func TestEncoderResultOwnership(t *testing.T) {
	encoder := NewEncoderWithCapacity(64, 64)
	encoder.Raw([]byte{1, 2, 3})
	firstCopy, _ := encoder.BytesResult()
	secondCopy, _ := encoder.BytesResult()
	firstCopy[0] = 9
	taken, err := encoder.TakeBytesResult()
	if err != nil || !bytes.Equal(taken, []byte{1, 2, 3}) || !bytes.Equal(secondCopy, taken) {
		t.Fatalf("copies=%x/%x transfer=%x err=%v", firstCopy, secondCopy, taken, err)
	}
	if encoder.buf != nil || cap(taken) != len(taken) {
		t.Fatal("transfer did not detach and restrict capacity")
	}
	encoder.Raw([]byte{4, 5, 6})
	second, err := encoder.TakeBytesResult()
	if err != nil || !bytes.Equal(second, []byte{4, 5, 6}) || !bytes.Equal(taken, []byte{1, 2, 3}) {
		t.Fatalf("first=%x second=%x err=%v", taken, second, err)
	}
	second[0] = 8
	taken[1] = 7
	if !bytes.Equal(secondCopy, []byte{1, 2, 3}) || !bytes.Equal(firstCopy, []byte{9, 2, 3}) || taken[0] != 1 || second[1] != 5 {
		t.Fatal("results alias")
	}
	encoder.Uint64(1)
	if !bytes.Equal(taken, []byte{1, 7, 3}) {
		t.Fatal("later writes changed transferred bytes")
	}
}

func TestEncoderCapacityAndTransferLimits(t *testing.T) {
	for _, test := range []struct {
		name     string
		max      uint32
		hint     uint64
		capacity int
	}{
		{"zero", 0, math.MaxUint64, 0},
		{"no hint", 8, 0, 0},
		{"small hint", 64, 7, 7},
		{"limit clamps hint", 8, math.MaxUint64, 8},
		{"large hint", math.MaxUint32, math.MaxUint64, 4096},
	} {
		t.Run(test.name, func(t *testing.T) {
			encoder := NewEncoderWithCapacity(test.max, test.hint)
			if cap(encoder.buf) != test.capacity {
				t.Fatalf("capacity=%d want=%d", cap(encoder.buf), test.capacity)
			}
			if data, err := encoder.TakeBytesResult(); data != nil || err != nil {
				t.Fatalf("empty=%x err=%v", data, err)
			}
		})
	}
	encoder := NewEncoderWithCapacity(4, 1)
	encoder.Uint32(42)
	if data, err := encoder.TakeBytesResult(); len(data) != 4 || err != nil {
		t.Fatalf("exact limit=%x err=%v", data, err)
	}
	encoder.Uint32(43)
	encoder.Uint8(1)
	for attempt := 0; attempt < 2; attempt++ {
		if data, err := encoder.TakeBytesResult(); data != nil || !errors.Is(err, ErrLimitExceeded) || encoder.buf != nil {
			t.Fatalf("failed transfer=%x err=%v", data, err)
		}
		encoder.Uint8(2)
		if data, err := encoder.BytesResult(); data != nil || !errors.Is(err, ErrLimitExceeded) {
			t.Fatalf("sticky copy=%x err=%v", data, err)
		}
	}
}

func TestVersionedNestedGoldenAndRetainedChild(t *testing.T) {
	encoder := NewEncoder(64)
	var child *Encoder
	var snapshot []byte
	encoder.Versioned(3, 1, func(outer *Encoder) {
		outer.Uint8(0xaa)
		outer.Versioned(2, 1, func(inner *Encoder) {
			child = inner
			inner.Uint16(0x0201)
			snapshot, _ = inner.BytesResult()
		})
	})
	want := []byte{3, 1, 9, 0, 0, 0, 0xaa, 2, 1, 2, 0, 0, 0, 1, 2}
	if data, err := child.BytesResult(); err != nil || !bytes.Equal(data, []byte{1, 2}) {
		t.Fatalf("retained child=%x err=%v", data, err)
	}
	child.Raw([]byte{9, 9})
	child.buf[0] = 7
	if !bytes.Equal(snapshot, []byte{1, 2}) {
		t.Fatalf("child mutation changed prior snapshot: %x", snapshot)
	}
	snapshot[1] = 6
	childResult, err := child.TakeBytesResult()
	if err != nil || !bytes.Equal(childResult, []byte{7, 2, 9, 9}) {
		t.Fatalf("appended child=%x err=%v", childResult, err)
	}
	childResult[0] = 8
	data, err := encoder.TakeBytesResult()
	if err != nil || !bytes.Equal(data, want) {
		t.Fatalf("nested=%x want=%x err=%v", data, want, err)
	}
	for _, maxBytes := range []uint32{0, 5, 6, 7} {
		bounded := NewEncoder(maxBytes)
		bounded.Versioned(1, 1, func(payload *Encoder) {
			payload.Versioned(1, 1, func(inner *Encoder) { inner.Uint16(1) })
		})
		if data, err := bounded.TakeBytesResult(); data != nil || !errors.Is(err, ErrLimitExceeded) {
			t.Fatalf("max=%d nested=%x err=%v", maxBytes, data, err)
		}
	}
}

func TestVersionedPreservesFirstError(t *testing.T) {
	first := errors.New("first error")
	encoder := NewEncoder(0)
	encoder.err = first
	called := false
	encoder.Versioned(1, 1, func(payload *Encoder) {
		called = true
		payload.Uint8(1)
	})
	if data, err := encoder.TakeBytesResult(); data != nil || err != first || !called {
		t.Fatalf("data=%x err=%v callback=%v", data, err, called)
	}
}

func TestVersionedEmptyAndExactLimit(t *testing.T) {
	for _, test := range []struct {
		name    string
		max     uint32
		payload []byte
		want    []byte
	}{
		{"empty", 6, nil, []byte{1, 1, 0, 0, 0, 0}},
		{"exact", 8, []byte{1, 2}, []byte{1, 1, 2, 0, 0, 0, 1, 2}},
	} {
		t.Run(test.name, func(t *testing.T) {
			encoder := NewEncoderWithCapacity(test.max, math.MaxUint64)
			var retained *Encoder
			var snapshot []byte
			encoder.Versioned(1, 1, func(child *Encoder) {
				retained = child
				child.Raw(test.payload)
				snapshot, _ = child.BytesResult()
			})
			if data, err := retained.BytesResult(); !bytes.Equal(data, test.payload) || err != nil {
				t.Fatalf("retained child=%x want=%x err=%v", data, test.payload, err)
			}
			retained.Raw([]byte{9, 9})
			childWant := append(append([]byte(nil), test.payload...), 9, 9)
			if data, err := retained.BytesResult(); err != nil || !bytes.Equal(data, childWant) {
				t.Fatalf("appended child=%x want=%x err=%v", data, childWant, err)
			}
			retained.buf[0] = 8
			if !bytes.Equal(snapshot, test.payload) {
				t.Fatalf("child mutation changed prior snapshot: %x", snapshot)
			}
			data, err := encoder.TakeBytesResult()
			if err != nil || !bytes.Equal(data, test.want) {
				t.Fatalf("envelope=%x want=%x err=%v", data, test.want, err)
			}
		})
	}
	zero := NewEncoderWithCapacity(0, math.MaxUint64)
	zero.Uint8(1)
	if data, err := zero.TakeBytesResult(); data != nil || !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("zero limit=%x err=%v", data, err)
	}
}
