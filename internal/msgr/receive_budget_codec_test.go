package msgr

import (
	"bytes"
	"errors"
	"io"
	"net"
	"runtime"
	"strconv"
	"testing"
	"time"
)

func TestReceiveBudgetSecurePreludeBacking(t *testing.T) {
	for _, embedded := range []bool{false, true} {
		for _, size := range []int{1, 48, 49, 1024} {
			t.Run(strconv.FormatBool(embedded)+"/"+strconv.Itoa(size), func(t *testing.T) {
				encoder, decoder := performanceCodecs(t, "secure")
				data := bytes.Repeat([]byte{0x5a}, size)
				wire, err := encoder.Encode(Frame{Tag: TagMessage, Segments: []Segment{{Alignment: DefaultAlignment, Data: data}}}, performanceLimits)
				if err != nil {
					t.Fatal(err)
				}
				budget, _ := NewReceiveBudget(1, 8192)
				packet := &budgetSecureReservation{}
				packet.lease.budget = budget
				packet.reader = budgetReader{Reader: bytes.NewReader(wire), lease: &packet.lease}
				if embedded {
					packet.reader.prelude = packet.prelude[:]
				}
				defer packet.lease.release()
				frame, err := decoder.Read(&packet.reader, performanceLimits)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(frame.Segments[0].Data, data) {
					t.Fatal("decoded first segment changed")
				}
				want := uint64(securePreamble)
				if size <= secureInlineSize {
					if embedded && &frame.Segments[0].Data[0] != &packet.prelude[PreambleSize] {
						t.Fatal("inline segment does not use the reservation prelude")
					}
				} else {
					want = paddedSecureLength(uint64(size))
					if embedded {
						want += securePreamble
					}
				}
				if snapshot := budget.Snapshot(); snapshot != (ReceiveBudgetSnapshot{RetainedBytes: want}) {
					t.Fatalf("retained backing = %+v want %d", snapshot, want)
				}
			})
		}
	}
}

func TestReceiveBudgetSecurePreludeLifetime(t *testing.T) {
	encoder, decoder := performanceCodecs(t, "secure")
	var wire []byte
	for sequence := uint64(1); sequence <= 3; sequence++ {
		frame, err := EncodeMessage(Message{Header: MessageHeader{Sequence: sequence}}, performanceLimits)
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := encoder.Encode(frame, performanceLimits)
		if err != nil {
			t.Fatal(err)
		}
		wire = append(wire, encoded...)
	}
	budget, _ := NewReceiveBudget(1, 8192)
	transport, _ := NewConnTransport(&budgetBytesConn{Reader: bytes.NewReader(wire)}, decoder, performanceLimits)
	var views [][]byte
	for sequence := uint64(1); sequence <= 3; sequence++ {
		frame, err := ReadTransportFrame(transport, budget, performanceLimits)
		if err != nil {
			t.Fatal(err)
		}
		views = append(views, frame.Segments[0].Data)
		message, err := decodeOwnedMessage(frame, performanceLimits)
		frame.receiveLease.release()
		frame.receiveLease = nil
		if err != nil || message.Header.Sequence != sequence {
			t.Fatalf("application handoff: message=%+v err=%v", message, err)
		}
		if snapshot := budget.Snapshot(); snapshot != (ReceiveBudgetSnapshot{}) {
			t.Fatalf("handoff leaked %+v", snapshot)
		}
		runtime.GC()
		for index, view := range views {
			header, err := DecodeMessageHeader(view)
			if err != nil || header.Sequence != uint64(index+1) {
				t.Fatalf("previous application view changed: header=%+v err=%v", header, err)
			}
			if index > 0 && &view[0] == &views[index-1][0] {
				t.Fatal("consecutive reads reused the prelude")
			}
		}
	}
	views[0][0] ^= 0xff
	if header, err := DecodeMessageHeader(views[1]); err != nil || header.Sequence != 2 {
		t.Fatal("mutating an application view changed a subsequent frame")
	}
}

func TestReceiveBudgetDecodePeakBoundaries(t *testing.T) {
	for _, mode := range []string{"crc", "secure"} {
		for _, size := range []int{1, 48, 49, 1024} {
			for _, additional := range []bool{false, true} {
				t.Run(mode+"/"+strconv.Itoa(size)+"/"+strconv.FormatBool(additional), func(t *testing.T) {
					segments := []Segment{{Alignment: DefaultAlignment, Data: make([]byte, size)}}
					if additional {
						segments = append(segments, Segment{Alignment: DefaultAlignment, Data: make([]byte, 33)})
					}
					peak := uint64(size)
					if additional {
						peak += 33
					}
					if mode == "secure" {
						peak = securePreamble
						padded := paddedSecureLength(uint64(size))
						if padded > secureInlineSize {
							peak += padded + padded - secureInlineSize + secureTagSize
						}
						if additional {
							peak += paddedSecureLength(33) + secureBlockSize + secureTagSize
						}
					}
					for _, limit := range []uint64{peak - 1, peak} {
						if limit == 0 {
							continue
						}
						encoder, decoder := performanceCodecs(t, mode)
						wire, err := encoder.Encode(Frame{Tag: TagMessage, Segments: segments}, performanceLimits)
						if err != nil {
							t.Fatal(err)
						}
						budget, _ := NewReceiveBudget(1, limit)
						transport, _ := NewConnTransport(&budgetBytesConn{Reader: bytes.NewReader(wire)}, decoder, performanceLimits)
						frame, err := ReadTransportFrame(transport, budget, performanceLimits)
						if limit < peak && !errors.Is(err, ErrReceiveBudgetExceeded) {
							t.Fatalf("limit=%d peak=%d err=%v", limit, peak, err)
						}
						if limit == peak && err != nil {
							t.Fatalf("exact peak=%d rejected: %v", peak, err)
						}
						frame.receiveLease.release()
						if snapshot := budget.Snapshot(); snapshot != (ReceiveBudgetSnapshot{}) {
							t.Fatalf("boundary leaked %+v", snapshot)
						}
					}
				})
			}
		}
	}
}

func TestReceiveBudgetCodecOversizeAndTruncation(t *testing.T) {
	for _, mode := range []string{"crc", "secure"} {
		for _, oversized := range []bool{false, true} {
			t.Run(mode+"/"+strconv.FormatBool(oversized), func(t *testing.T) {
				encoder, decoder := performanceCodecs(t, mode)
				wire, err := encoder.Encode(Frame{Tag: TagMessage, Segments: []Segment{{Alignment: DefaultAlignment, Data: make([]byte, 1024)}}}, performanceLimits)
				if err != nil {
					t.Fatal(err)
				}
				prefix := PreambleSize
				if mode == "secure" {
					prefix = securePreamble
				}
				limits := performanceLimits
				if oversized {
					limits.MaxFrameBytes = uint64(len(wire) - 1)
				}
				budget, _ := NewReceiveBudget(1, 8192)
				transport, _ := NewConnTransport(&budgetBytesConn{Reader: bytes.NewReader(wire[:prefix])}, decoder, limits)
				_, err = ReadTransportFrame(transport, budget, limits)
				if err == nil {
					t.Fatal("truncated or oversized frame accepted")
				}
				if oversized && !errors.Is(err, ErrLimitExceeded) {
					t.Fatalf("oversize prefix = %v", err)
				}
				if snapshot := budget.Snapshot(); snapshot != (ReceiveBudgetSnapshot{}) {
					t.Fatalf("error leaked %+v", snapshot)
				}
			})
		}
	}
}

func TestReceiveBudgetBuiltInSessionStop(t *testing.T) {
	for _, mode := range []string{"crc", "secure"} {
		t.Run(mode, func(t *testing.T) {
			encoder, decoder := performanceCodecs(t, mode)
			client, peer := net.Pipe()
			defer peer.Close()
			transport, err := NewConnTransport(client, decoder, performanceLimits)
			if err != nil {
				t.Fatal(err)
			}
			budget, _ := NewReceiveBudget(1, 8192)
			config := performanceConfig(4)
			config.ReceiveBudget = budget
			config.MaxQueuedReceiveBytes = 4096
			session := newTestSession(t, transport, nil, config)
			defer session.Stop()
			drained := make(chan struct{})
			go func() { _, _ = io.Copy(io.Discard, peer); close(drained) }()
			message := testMessage("application")
			message.Header.Sequence = 1
			frame, err := EncodeMessage(message, performanceLimits)
			if err != nil {
				t.Fatal(err)
			}
			wire, err := encoder.Encode(frame, performanceLimits)
			if err != nil {
				t.Fatal(err)
			}
			written := make(chan error, 1)
			go func() { _, err := peer.Write(wire); written <- err }()
			select {
			case err := <-written:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("wire receive blocked")
			}
			select {
			case received := <-session.Incoming():
				getSnapshot(t, session)
				if got := budget.Snapshot().RetainedBytes; got != 0 {
					t.Fatalf("delivered payload retained %d", got)
				}
				session.Stop()
				if string(received.Front) != "application" {
					t.Fatal("application backing changed")
				}
			case <-time.After(time.Second):
				t.Fatal("built-in delivery blocked")
			}
			if snapshot := budget.Snapshot(); snapshot != (ReceiveBudgetSnapshot{}) {
				t.Fatalf("built-in stop leaked %+v", snapshot)
			}
			peer.Close()
			<-drained
		})
	}
}
