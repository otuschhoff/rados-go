package msgr

import (
	"bytes"
	"testing"
)

func BenchmarkSecureRead64KiB(benchmark *testing.B) {
	secret := testSecureSecret()
	sender, err := NewSecureCodec(secret, true)
	if err != nil {
		benchmark.Fatal(err)
	}
	frame := Frame{Tag: TagMessage, Segments: []Segment{
		{Alignment: DefaultAlignment, Data: make([]byte, MessageHeaderSize)},
		{Alignment: DefaultAlignment, Data: make([]byte, 128)},
		{Alignment: DefaultAlignment},
		{Alignment: PageAlignment, Data: make([]byte, 65536)},
	}}
	limits := Limits{MaxSegmentBytes: 1 << 20, MaxFrameBytes: 2 << 20}
	wire, err := sender.Encode(frame, limits)
	if err != nil {
		benchmark.Fatal(err)
	}
	receiver, err := NewSecureCodec(secret, false)
	if err != nil {
		benchmark.Fatal(err)
	}
	reader := bytes.NewReader(wire)
	initialCounter := receiver.rx.counter
	benchmark.ReportAllocs()
	benchmark.SetBytes(65536)
	benchmark.ResetTimer()
	for iteration := 0; iteration < benchmark.N; iteration++ {
		receiver.rx.counter = initialCounter
		reader.Reset(wire)
		if _, err := receiver.Read(reader, limits); err != nil {
			benchmark.Fatal(err)
		}
	}
}
