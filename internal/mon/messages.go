// Package mon implements monitor protocol messages and lifecycle management.
package mon

import (
	"fmt"
	"sort"

	wire "github.com/otuschhoff/go-librados/internal/encoding"
	"github.com/otuschhoff/go-librados/internal/maps"
	"github.com/otuschhoff/go-librados/internal/msgr"
	"github.com/otuschhoff/go-librados/internal/protocol"
)

const SubscribeOnce uint8 = 1

type Subscription struct {
	Start uint64
	Flags uint8
}

type SubscribeAck struct {
	IntervalSeconds uint32
	FSID            maps.FSID
}

type OSDMapBatch struct {
	FSID           maps.FSID
	Incrementals   map[uint32][]byte
	FullMaps       map[uint32][]byte
	TrimLowerBound uint32
	NewestMap      uint32
}

type CommandReply struct {
	Version uint64
	Result  int32
	Status  string
	Command []string
	Data    []byte
}

type PoolOperation uint32

const (
	PoolOperationCreateSnapshot    PoolOperation = 0x11
	PoolOperationDeleteSnapshot    PoolOperation = 0x12
	PoolOperationCreateSelfManaged PoolOperation = 0x21
	PoolOperationDeleteSelfManaged PoolOperation = 0x22
)

type PoolOperationReply struct {
	Version      uint64
	FSID         maps.FSID
	Result       int32
	Epoch        uint32
	ResponseData []byte
}

type MessageLimits struct {
	MaxBytes uint32
	MaxMaps  uint32
}

func EncodeSubscribe(subscriptions map[string]Subscription, hostname string, maxBytes uint32) (msgr.Message, error) {
	if maxBytes == 0 || uint64(len(subscriptions)) > uint64(^uint32(0)) {
		return msgr.Message{}, wire.ErrLimitExceeded
	}
	names := make([]string, 0, len(subscriptions))
	for name := range subscriptions {
		names = append(names, name)
	}
	sort.Strings(names)
	encoder := wire.NewEncoder(maxBytes)
	encoder.Uint32(uint32(len(names)))
	for _, name := range names {
		item := subscriptions[name]
		encoder.String(name)
		encoder.Uint64(item.Start)
		encoder.Uint8(item.Flags)
	}
	encoder.String(hostname)
	front, err := encoder.BytesResult()
	if err != nil {
		return msgr.Message{}, err
	}
	return frontMessage(protocol.MessageMonSubscribe, 3, 1, front), nil
}

func EncodeCommand(fsid maps.FSID, command []string, input []byte, maxBytes uint32) (msgr.Message, error) {
	if maxBytes == 0 || uint64(len(command)) > uint64(^uint32(0)) || uint64(len(input)) > uint64(maxBytes) {
		return msgr.Message{}, wire.ErrLimitExceeded
	}
	encoder := wire.NewEncoder(maxBytes)
	encodePaxosHeader(encoder, 0)
	encoder.Raw(fsid[:])
	encoder.Uint32(uint32(len(command)))
	for _, value := range command {
		encoder.String(value)
	}
	front, err := encoder.BytesResult()
	if err != nil {
		return msgr.Message{}, err
	}
	message := frontMessage(protocol.MessageMonCommand, 1, 0, front)
	message.Data = append([]byte(nil), input...)
	message.Lengths.Data = uint32(len(message.Data))
	return message, nil
}

func EncodePoolOperation(fsid maps.FSID, pool uint32, operation PoolOperation, snapID uint64, name string, version uint64, maxBytes uint32) (msgr.Message, error) {
	if maxBytes == 0 {
		return msgr.Message{}, wire.ErrLimitExceeded
	}
	switch operation {
	case PoolOperationCreateSnapshot, PoolOperationDeleteSnapshot:
		if name == "" || snapID != 0 {
			return msgr.Message{}, fmt.Errorf("%w: named snapshot operation arguments", wire.ErrMalformed)
		}
	case PoolOperationCreateSelfManaged:
		if name != "" || snapID != 0 {
			return msgr.Message{}, fmt.Errorf("%w: self-managed snapshot allocation arguments", wire.ErrMalformed)
		}
	case PoolOperationDeleteSelfManaged:
		if name != "" || snapID == 0 {
			return msgr.Message{}, fmt.Errorf("%w: self-managed snapshot removal arguments", wire.ErrMalformed)
		}
	default:
		return msgr.Message{}, fmt.Errorf("%w: pool operation %d", wire.ErrMalformed, operation)
	}
	encoder := wire.NewEncoder(maxBytes)
	encodePaxosHeader(encoder, version)
	encoder.Raw(fsid[:])
	encoder.Uint32(pool)
	encoder.Uint32(uint32(operation))
	encoder.Uint64(0)
	encoder.Uint64(snapID)
	encoder.String(name)
	encoder.Uint8(0)
	encoder.Int16(0)
	front, err := encoder.BytesResult()
	if err != nil {
		return msgr.Message{}, err
	}
	return frontMessage(protocol.MessagePoolOp, 4, 2, front), nil
}

func DecodePoolOperationReply(message msgr.Message, maxBytes uint32) (PoolOperationReply, error) {
	if err := validateFrontMessage(message, protocol.MessagePoolOpReply, maxBytes); err != nil {
		return PoolOperationReply{}, err
	}
	if message.Header.CompatVersion > 1 {
		return PoolOperationReply{}, fmt.Errorf("%w: pool operation reply compat=%d", wire.ErrUnsupportedVersion, message.Header.CompatVersion)
	}
	decoder := wire.NewDecoder(message.Front, wire.Limits{MaxBytes: maxBytes})
	result := PoolOperationReply{Version: decodePaxosHeader(decoder)}
	copy(result.FSID[:], decoder.Raw(16))
	result.Result = decoder.Int32()
	result.Epoch = decoder.Uint32()
	hasResponseData := decoder.Uint8()
	if hasResponseData > 1 {
		return PoolOperationReply{}, fmt.Errorf("%w: pool operation response-data flag", wire.ErrMalformed)
	}
	if hasResponseData == 1 {
		result.ResponseData = decoder.Bytes()
	}
	if err := finishExact(decoder, "pool operation reply"); err != nil {
		return PoolOperationReply{}, err
	}
	return result, nil
}

func DecodeAllocatedSnapshotID(data []byte, maxBytes uint32) (uint64, error) {
	if maxBytes == 0 || uint64(len(data)) > uint64(maxBytes) {
		return 0, wire.ErrLimitExceeded
	}
	decoder := wire.NewDecoder(data, wire.Limits{MaxBytes: maxBytes})
	snapshotID := decoder.Uint64()
	if snapshotID == 0 {
		return 0, fmt.Errorf("%w: zero allocated snapshot id", wire.ErrMalformed)
	}
	if err := finishExact(decoder, "allocated snapshot id"); err != nil {
		return 0, err
	}
	return snapshotID, nil
}

func DecodeCommandReply(message msgr.Message, maxBytes, maxCommandItems uint32) (CommandReply, error) {
	if maxCommandItems == 0 {
		return CommandReply{}, wire.ErrLimitExceeded
	}
	if err := validateMessagePayload(message, protocol.MessageMonCommandAck, maxBytes); err != nil {
		return CommandReply{}, err
	}
	decoder := wire.NewDecoder(message.Front, wire.Limits{MaxBytes: maxBytes})
	result := CommandReply{Version: decodePaxosHeader(decoder), Result: decoder.Int32(), Status: decoder.String()}
	count := decoder.Uint32()
	if err := decoder.Finish(); err != nil {
		return CommandReply{}, err
	}
	if count > maxCommandItems || uint64(count) > decoder.Remaining()/4 {
		return CommandReply{}, wire.ErrLimitExceeded
	}
	result.Command = make([]string, 0, count)
	for range count {
		result.Command = append(result.Command, decoder.String())
	}
	if err := finishExact(decoder, "monitor command acknowledgement"); err != nil {
		return CommandReply{}, err
	}
	result.Data = append([]byte(nil), message.Data...)
	return result, nil
}

func encodePaxosHeader(encoder *wire.Encoder, version uint64) {
	encoder.Uint64(version)
	encoder.Int16(-1)
	encoder.Uint64(0)
}

func decodePaxosHeader(decoder *wire.Decoder) uint64 {
	version := decoder.Uint64()
	decoder.Int16()
	decoder.Uint64()
	return version
}

func DecodeSubscribeAck(message msgr.Message, maxBytes uint32) (SubscribeAck, error) {
	if err := validateFrontMessage(message, protocol.MessageMonSubscribeAck, maxBytes); err != nil {
		return SubscribeAck{}, err
	}
	decoder := wire.NewDecoder(message.Front, wire.Limits{MaxBytes: maxBytes})
	result := SubscribeAck{IntervalSeconds: decoder.Uint32()}
	copy(result.FSID[:], decoder.Raw(16))
	if err := finishExact(decoder, "subscribe acknowledgement"); err != nil {
		return SubscribeAck{}, err
	}
	return result, nil
}

func DecodeMonMap(message msgr.Message, limits maps.Limits) (*maps.MonMap, error) {
	if err := validateFrontMessage(message, protocol.MessageMonMap, limits.MaxBytes); err != nil {
		return nil, err
	}
	decoder := wire.NewDecoder(message.Front, wire.Limits{MaxBytes: limits.MaxBytes})
	encoded := decoder.Bytes()
	if err := finishExact(decoder, "monmap message"); err != nil {
		return nil, err
	}
	return maps.DecodeMonMap(encoded, limits)
}

func DecodeOSDMapBatch(message msgr.Message, limits MessageLimits) (OSDMapBatch, error) {
	if limits.MaxBytes == 0 || limits.MaxMaps == 0 {
		return OSDMapBatch{}, wire.ErrLimitExceeded
	}
	if err := validateFrontMessage(message, protocol.MessageOSDMap, limits.MaxBytes); err != nil {
		return OSDMapBatch{}, err
	}
	if message.Header.Version < 3 || message.Header.CompatVersion > 4 {
		return OSDMapBatch{}, fmt.Errorf("%w: osdmap message version=%d compat=%d", wire.ErrUnsupportedVersion, message.Header.Version, message.Header.CompatVersion)
	}
	decoder := wire.NewDecoder(message.Front, wire.Limits{MaxBytes: limits.MaxBytes})
	result := OSDMapBatch{}
	copy(result.FSID[:], decoder.Raw(16))
	var err error
	result.Incrementals, err = decodeMapBlobs(decoder, limits.MaxMaps)
	if err != nil {
		return OSDMapBatch{}, err
	}
	result.FullMaps, err = decodeMapBlobs(decoder, limits.MaxMaps)
	if err != nil {
		return OSDMapBatch{}, err
	}
	result.TrimLowerBound = decoder.Uint32()
	result.NewestMap = decoder.Uint32()
	if message.Header.Version >= 4 {
		gapRemovedSnaps := decoder.Uint32()
		if gapRemovedSnaps != 0 {
			return OSDMapBatch{}, fmt.Errorf("%w: retired gap snapshot entries", wire.ErrUnsupportedVersion)
		}
	}
	if err := finishExact(decoder, "osdmap message"); err != nil {
		return OSDMapBatch{}, err
	}
	return result, nil
}

func decodeMapBlobs(decoder *wire.Decoder, maximum uint32) (map[uint32][]byte, error) {
	count := decoder.Uint32()
	if err := decoder.Finish(); err != nil {
		return nil, err
	}
	if count > maximum {
		return nil, wire.ErrLimitExceeded
	}
	if uint64(count) > decoder.Remaining()/8 {
		return nil, wire.ErrMalformed
	}
	values := make(map[uint32][]byte, count)
	for range count {
		epoch := decoder.Uint32()
		value := decoder.Bytes()
		if _, exists := values[epoch]; exists {
			return nil, fmt.Errorf("%w: duplicate osdmap epoch %d", maps.ErrMalformedMap, epoch)
		}
		values[epoch] = value
	}
	return values, decoder.Finish()
}

func frontMessage(messageType, version, compat uint16, front []byte) msgr.Message {
	return msgr.Message{
		Header:  msgr.MessageHeader{Type: messageType, Version: version, CompatVersion: compat},
		Lengths: msgr.MessageLengths{Front: uint32(len(front))},
		Front:   front,
	}
}

func validateFrontMessage(message msgr.Message, messageType uint16, maxBytes uint32) error {
	if maxBytes == 0 || uint64(len(message.Front)) > uint64(maxBytes) {
		return wire.ErrLimitExceeded
	}
	if message.Header.Type != messageType {
		return fmt.Errorf("%w: message type %d, want %d", wire.ErrMalformed, message.Header.Type, messageType)
	}
	if len(message.Middle) != 0 || len(message.Data) != 0 || message.Lengths != (msgr.MessageLengths{Front: uint32(len(message.Front))}) {
		return fmt.Errorf("%w: monitor message segment lengths", wire.ErrMalformed)
	}
	return nil
}

func validateMessagePayload(message msgr.Message, messageType uint16, maxBytes uint32) error {
	if maxBytes == 0 || uint64(len(message.Front))+uint64(len(message.Data)) > uint64(maxBytes) {
		return wire.ErrLimitExceeded
	}
	if message.Header.Type != messageType || len(message.Middle) != 0 || message.Lengths != (msgr.MessageLengths{Front: uint32(len(message.Front)), Data: uint32(len(message.Data))}) {
		return fmt.Errorf("%w: monitor message payload", wire.ErrMalformed)
	}
	return nil
}

func finishExact(decoder *wire.Decoder, name string) error {
	if err := decoder.Finish(); err != nil {
		return err
	}
	if decoder.Remaining() != 0 {
		return fmt.Errorf("%w: trailing %s bytes", wire.ErrMalformed, name)
	}
	return nil
}
