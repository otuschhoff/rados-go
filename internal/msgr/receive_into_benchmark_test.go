package msgr

import (
	"bufio"
	"bytes"
	"fmt"
	"testing"
)

func BenchmarkSecureReceiveInto(benchmark *testing.B) {
	for _, size := range []int{64 << 10, 4 << 20} {
		for _, borrowed := range []bool{false, true} {
			benchmark.Run(fmt.Sprintf("bytes=%d/into=%t", size, borrowed), func(benchmark *testing.B) {
				encoder, decoder := performanceCodecs(benchmark, "secure")
				connection := &scratchConn{budgetBytesConn{Reader: bytes.NewReader(nil)}}
				transport, err := NewConnTransport(connection, decoder, performanceLimits)
				if err != nil {
					benchmark.Fatal(err)
				}
				defer transport.Close()
				budget, _ := NewReceiveBudget(1, 32<<20)
				payload := bytes.Repeat([]byte{42}, size)
				destination := make([]byte, size)
				frame := Frame{Tag: TagMessage, Segments: []Segment{{Alignment: DefaultAlignment, Data: make([]byte, 80)}, {Alignment: DefaultAlignment, Data: payload}}}
				benchmark.ReportAllocs()
				benchmark.SetBytes(int64(size))
				for benchmark.Loop() {
					benchmark.StopTimer()
					wire, err := encoder.Encode(frame, performanceLimits)
					if err != nil {
						benchmark.Fatal(err)
					}
					connection.Reader.Reset(wire)
					transport.(*connTransport).reader.(*bufio.Reader).Reset(connection)
					benchmark.StartTimer()
					received, err := ReadTransportFrame(transport, budget, performanceLimits)
					if err != nil {
						benchmark.Fatal(err)
					}
					result := submitResult{message: Message{Data: received.Segments[1].Data, receiveLease: received.receiveLease}}
					if borrowed {
						message, release, err := result.borrow()
						if err != nil {
							benchmark.Fatal(err)
						}
						copy(destination, message.Data)
						if destination[size-1] != 42 {
							benchmark.Fatal("invalid destination copy")
						}
						release()
					} else {
						message, err := result.take()
						if err != nil || message.Data[size-1] != 42 {
							benchmark.Fatal("invalid receive")
						}
					}
				}
			})
		}
	}
}
