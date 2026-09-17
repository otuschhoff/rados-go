package osd

import (
	"errors"
	"net/netip"
	"testing"

	wire "github.com/otuschhoff/rados-go/internal/encoding"
	"github.com/otuschhoff/rados-go/internal/msgr"
	"github.com/otuschhoff/rados-go/internal/protocol"
)

func TestWatchNotifyCodecs(t *testing.T) {
	encoder := wire.NewEncoder(4096)
	encoder.Uint8(1)
	encoder.Uint8(WatchEventNotify)
	encoder.Uint64(8)
	encoder.Uint64(9)
	encoder.Uint64(10)
	encoder.Bytes([]byte("payload"))
	encoder.Int32(0)
	encoder.Uint64(11)
	front, err := encoder.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	notification, err := DecodeWatchNotification(msgr.Message{Header: msgr.MessageHeader{Type: protocol.MessageWatchNotify, Version: 3, CompatVersion: 1}, Front: front}, Limits{MaxBytes: 4096, MaxOperations: 4})
	if err != nil || notification.Cookie != 8 || notification.NotifyID != 10 || notification.NotifierGID != 11 || string(notification.Data) != "payload" {
		t.Fatalf("notification=%+v error=%v", notification, err)
	}
}

func TestDecodeNotifyResult(t *testing.T) {
	encoder := wire.NewEncoder(4096)
	encoder.Uint32(1)
	encoder.Uint64(12)
	encoder.Uint64(13)
	encoder.Bytes([]byte("ack"))
	encoder.Uint32(1)
	encoder.Uint64(14)
	encoder.Uint64(15)
	data, err := encoder.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	acknowledged, timedOut, err := DecodeNotifyResult(data, 4096, 2)
	if err != nil || len(acknowledged) != 1 || acknowledged[0].Client != 12 || string(acknowledged[0].Data) != "ack" || len(timedOut) != 1 || timedOut[0].Cookie != 15 {
		t.Fatalf("acks=%+v timeouts=%+v error=%v", acknowledged, timedOut, err)
	}
	if _, _, err := DecodeNotifyResult(data, 4096, 1); !errors.Is(err, wire.ErrLimitExceeded) {
		t.Fatalf("entry limit error=%v", err)
	}
}

func TestDecodeWatchers(t *testing.T) {
	address, err := protocol.IPv4EntityAddr(protocol.AddressLegacy, 1, netip.MustParseAddrPort("192.0.2.9:6800"))
	if err != nil {
		t.Fatal(err)
	}
	encoder := wire.NewEncoder(4096)
	encoder.Versioned(1, 1, func(reply *wire.Encoder) {
		reply.Uint32(1)
		reply.Versioned(2, 1, func(item *wire.Encoder) {
			item.Uint8(uint8(protocol.EntityClient))
			item.Int64(21)
			item.Uint64(22)
			item.Uint32(30)
			if err := address.Encode(item, protocol.FeatureMessageAddress2); err != nil {
				t.Fatal(err)
			}
		})
	})
	data, err := encoder.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	watchers, err := DecodeWatchers(data, 4096, 1)
	if err != nil || len(watchers) != 1 || watchers[0].Client != 21 || watchers[0].Cookie != 22 || watchers[0].TimeoutSeconds != 30 || watchers[0].Address != "192.0.2.9:6800" {
		t.Fatalf("watchers=%+v error=%v", watchers, err)
	}
	if _, err := DecodeWatchers(data, 4096, 0); !errors.Is(err, wire.ErrLimitExceeded) {
		t.Fatalf("watcher limit error=%v", err)
	}
}

func FuzzDecodeWatchNotification(f *testing.F) {
	encoder := wire.NewEncoder(256)
	encoder.Uint8(1)
	encoder.Uint8(WatchEventNotify)
	encoder.Uint64(8)
	encoder.Uint64(9)
	encoder.Uint64(10)
	encoder.Bytes([]byte("payload"))
	encoder.Int32(0)
	encoder.Uint64(11)
	seed, err := encoder.BytesResult()
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeWatchNotification(msgr.Message{Header: msgr.MessageHeader{Type: protocol.MessageWatchNotify, Version: 3, CompatVersion: 1}, Front: data}, Limits{MaxBytes: 4096, MaxOperations: 16})
	})
}

func FuzzDecodeNotifyResult(f *testing.F) {
	encoder := wire.NewEncoder(256)
	encoder.Uint32(1)
	encoder.Uint64(12)
	encoder.Uint64(13)
	encoder.Bytes([]byte("ack"))
	encoder.Uint32(1)
	encoder.Uint64(14)
	encoder.Uint64(15)
	seed, err := encoder.BytesResult()
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _ = DecodeNotifyResult(data, 4096, 64)
	})
}

func FuzzDecodeWatchers(f *testing.F) {
	address, err := protocol.IPv4EntityAddr(protocol.AddressLegacy, 1, netip.MustParseAddrPort("192.0.2.9:6800"))
	if err != nil {
		f.Fatal(err)
	}
	encoder := wire.NewEncoder(256)
	encoder.Versioned(1, 1, func(reply *wire.Encoder) {
		reply.Uint32(1)
		reply.Versioned(2, 1, func(item *wire.Encoder) {
			item.Uint8(uint8(protocol.EntityClient))
			item.Int64(21)
			item.Uint64(22)
			item.Uint32(30)
			if err := address.Encode(item, protocol.FeatureMessageAddress2); err != nil {
				f.Fatal(err)
			}
		})
	})
	seed, err := encoder.BytesResult()
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeWatchers(data, 4096, 64)
	})
}
