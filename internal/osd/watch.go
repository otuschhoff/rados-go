package osd

import (
	"fmt"
	"net/netip"

	wire "github.com/otuschhoff/go-librados/internal/encoding"
	"github.com/otuschhoff/go-librados/internal/msgr"
	"github.com/otuschhoff/go-librados/internal/protocol"
)

const (
	WatchOperationUnwatch   = uint8(0)
	WatchOperationRegister  = uint8(3)
	WatchOperationReconnect = uint8(5)
	WatchOperationPing      = uint8(7)

	WatchEventNotify     = uint8(1)
	WatchEventComplete   = uint8(2)
	WatchEventDisconnect = uint8(3)
)

type WatchNotification struct {
	Cookie      uint64
	Version     uint64
	NotifyID    uint64
	Opcode      uint8
	Data        []byte
	Result      int32
	NotifierGID uint64
}

type NotifyAcknowledgment struct {
	Client uint64
	Cookie uint64
	Data   []byte
}

type NotifyTimeout struct {
	Client uint64
	Cookie uint64
}

type Watcher struct {
	Client         uint64
	Cookie         uint64
	TimeoutSeconds uint32
	Address        string
}

func EncodeNotifyRequest(timeoutSeconds uint32, data []byte, maxBytes uint32) ([]byte, error) {
	encoder := wire.NewEncoder(maxBytes)
	encoder.Uint32(1)
	encoder.Uint32(timeoutSeconds)
	encoder.Bytes(data)
	return encoder.BytesResult()
}

func EncodeNotifyAck(notifyID, cookie uint64, data []byte, maxBytes uint32) ([]byte, error) {
	if notifyID == 0 || cookie == 0 {
		return nil, wire.ErrMalformed
	}
	encoder := wire.NewEncoder(maxBytes)
	encoder.Uint64(notifyID)
	encoder.Uint64(cookie)
	encoder.Bytes(data)
	return encoder.BytesResult()
}

func DecodeWatchNotification(message msgr.Message, limits Limits) (WatchNotification, error) {
	if limits.MaxBytes == 0 || message.Header.Type != protocol.MessageWatchNotify || message.Header.Version < 1 || message.Header.Version > 3 || message.Header.CompatVersion > 1 || len(message.Middle) != 0 || uint64(len(message.Front))+uint64(len(message.Data)) > uint64(limits.MaxBytes) {
		return WatchNotification{}, ErrMalformedReply
	}
	decoder := wire.NewDecoder(message.Front, wire.Limits{MaxBytes: limits.MaxBytes})
	messageVersion := decoder.Uint8()
	notification := WatchNotification{Opcode: decoder.Uint8(), Cookie: decoder.Uint64(), Version: decoder.Uint64(), NotifyID: decoder.Uint64()}
	if messageVersion < 1 || notification.Cookie == 0 || notification.Opcode < WatchEventNotify || notification.Opcode > WatchEventDisconnect {
		return WatchNotification{}, ErrMalformedReply
	}
	if messageVersion >= 1 {
		frontData := decoder.Bytes()
		if len(frontData) != 0 && len(message.Data) != 0 {
			return WatchNotification{}, ErrMalformedReply
		}
		notification.Data = append([]byte(nil), frontData...)
	}
	if message.Header.Version >= 2 {
		notification.Result = decoder.Int32()
	}
	if message.Header.Version >= 3 {
		notification.NotifierGID = decoder.Uint64()
	}
	if err := decoder.Finish(); err != nil || decoder.Remaining() != 0 {
		return WatchNotification{}, ErrMalformedReply
	}
	if len(message.Data) != 0 {
		notification.Data = append([]byte(nil), message.Data...)
	}
	return notification, nil
}

func DecodeNotifyResult(data []byte, maxBytes, maxEntries uint32) ([]NotifyAcknowledgment, []NotifyTimeout, error) {
	decoder := wire.NewDecoder(data, wire.Limits{MaxBytes: maxBytes})
	ackCount := decoder.Uint32()
	if ackCount > maxEntries || uint64(ackCount)*20 > decoder.Remaining() {
		return nil, nil, wire.ErrLimitExceeded
	}
	acknowledged := make([]NotifyAcknowledgment, 0, ackCount)
	for range ackCount {
		acknowledged = append(acknowledged, NotifyAcknowledgment{Client: decoder.Uint64(), Cookie: decoder.Uint64(), Data: append([]byte(nil), decoder.Bytes()...)})
	}
	timeoutCount := decoder.Uint32()
	if timeoutCount > maxEntries-uint32(len(acknowledged)) || uint64(timeoutCount)*16 > decoder.Remaining() {
		return nil, nil, wire.ErrLimitExceeded
	}
	timedOut := make([]NotifyTimeout, 0, timeoutCount)
	for range timeoutCount {
		timedOut = append(timedOut, NotifyTimeout{Client: decoder.Uint64(), Cookie: decoder.Uint64()})
	}
	if err := decoder.Finish(); err != nil || decoder.Remaining() != 0 {
		return nil, nil, fmt.Errorf("%w: notify completion", ErrMalformedReply)
	}
	return acknowledged, timedOut, nil
}

func DecodeWatchers(data []byte, maxBytes, maxWatchers uint32) ([]Watcher, error) {
	decoder := wire.NewDecoder(data, wire.Limits{MaxBytes: maxBytes})
	version, payload := decoder.Versioned(1)
	if err := decoder.Finish(); err != nil || version != 1 {
		return nil, wire.ErrUnsupportedVersion
	}
	count := payload.Uint32()
	if count > maxWatchers || uint64(count)*17 > payload.Remaining() {
		return nil, wire.ErrLimitExceeded
	}
	watchers := make([]Watcher, 0, count)
	for range count {
		itemVersion, item := payload.Versioned(2)
		if err := payload.Finish(); err != nil || itemVersion < 1 || itemVersion > 2 || item.Uint8() != uint8(protocol.EntityClient) {
			return nil, ErrMalformedReply
		}
		client := item.Int64()
		watcher := Watcher{Cookie: item.Uint64(), TimeoutSeconds: item.Uint32()}
		if client < 0 || watcher.Cookie == 0 {
			return nil, ErrMalformedReply
		}
		watcher.Client = uint64(client)
		if itemVersion >= 2 {
			address, err := protocol.DecodeEntityAddr(item)
			if err != nil {
				return nil, err
			}
			if endpoint, ok := address.AddrPort(); ok && endpoint != (netip.AddrPort{}) {
				watcher.Address = endpoint.String()
			}
		}
		if err := item.Finish(); err != nil || item.Remaining() != 0 {
			return nil, ErrMalformedReply
		}
		watchers = append(watchers, watcher)
	}
	if err := payload.Finish(); err != nil || payload.Remaining() != 0 {
		return nil, ErrMalformedReply
	}
	return watchers, nil
}
