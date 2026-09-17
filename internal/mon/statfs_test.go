package mon

import (
	"errors"
	"testing"

	wire "github.com/otuschhoff/rados-go/internal/encoding"
	"github.com/otuschhoff/rados-go/internal/maps"
	"github.com/otuschhoff/rados-go/internal/msgr"
	"github.com/otuschhoff/rados-go/internal/protocol"
)

func TestStatFSWire(t *testing.T) {
	fsid := maps.FSID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	request, err := EncodeStatFS(fsid, 42, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if request.Header.Type != protocol.MessageStatFS || request.Header.Version != 2 || request.Header.CompatVersion != 1 || len(request.Front) != 35 {
		t.Fatalf("request=%+v front=%x", request.Header, request.Front)
	}
	encoder := wire.NewEncoder(1024)
	encoder.Raw(fsid[:])
	encoder.Uint64(43)
	encoder.Uint64(100)
	encoder.Uint64(40)
	encoder.Uint64(60)
	encoder.Uint64(7)
	front, err := encoder.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	reply, err := DecodeStatFSReply(frontMessage(protocol.MessageStatFSReply, 1, 1, front), 1024)
	if err != nil {
		t.Fatal(err)
	}
	if reply.FSID != fsid || reply.Version != 43 || reply.KB != 100 || reply.KBUsed != 40 || reply.KBAvailable != 60 || reply.Objects != 7 {
		t.Fatalf("reply=%+v", reply)
	}
}

func TestStatFSReplyRejectsMalformedFraming(t *testing.T) {
	message := msgr.Message{Header: msgr.MessageHeader{Type: protocol.MessageStatFSReply, Version: 1, CompatVersion: 1}, Front: make([]byte, 55), Lengths: msgr.MessageLengths{Front: 55}}
	if _, err := DecodeStatFSReply(message, 1024); !errors.Is(err, wire.ErrMalformed) {
		t.Fatalf("err=%v", err)
	}
	message = frontMessage(protocol.MessageStatFSReply, 2, 2, make([]byte, 56))
	if _, err := DecodeStatFSReply(message, 1024); !errors.Is(err, wire.ErrUnsupportedVersion) {
		t.Fatalf("unsupported compatibility error = %v", err)
	}
}

func TestGetPoolStatsReplyDecodesCurrentWireLayout(t *testing.T) {
	fsid := maps.FSID{1, 2, 3, 4}
	encoder := wire.NewEncoder(16 << 10)
	encodePaxosHeader(encoder, 9)
	encoder.Raw(fsid[:])
	encoder.Uint32(1)
	encoder.String("data")
	encoder.Versioned(7, 5, func(pool *wire.Encoder) {
		pool.Versioned(2, 2, func(collection *wire.Encoder) {
			collection.Versioned(20, 14, func(sum *wire.Encoder) {
				for index := 0; index < 40; index++ {
					value := uint64(0)
					switch index {
					case 0:
						value = 100
					case 1:
						value = 3
					case 8:
						value = 5
					case 10:
						value = 7
					case 22:
						value = 11
					case 37:
						value = 13
					}
					if index >= 28 && index <= 31 {
						sum.Int32(int32(value))
					} else {
						sum.Uint64(value)
					}
				}
			})
			collection.Uint32(0)
		})
		pool.Int64(0)
		pool.Int64(0)
		pool.Int32(0)
		pool.Int32(0)
		pool.Versioned(1, 1, func(store *wire.Encoder) {
			for index := 0; index < 10; index++ {
				value := uint64(0)
				if index == 3 {
					value = 200
				}
				if index == 8 {
					value = 17
				}
				store.Uint64(value)
			}
		})
		pool.Int32(1)
	})
	encoder.Bool(true)
	front, err := encoder.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	message := frontMessage(protocol.MessageGetPoolStatsReply, 2, 1, front)
	reply, err := DecodeGetPoolStatsReply(message, 16<<10, 4)
	if err != nil {
		t.Fatal(err)
	}
	stats := reply.Pools["data"]
	if reply.FSID != fsid || reply.Version != 9 || stats.BytesUsed != 217 || stats.Objects != 3 || stats.ReadBytes != 5<<10 || stats.WriteBytes != 7<<10 {
		t.Fatalf("reply=%+v", reply)
	}
}
