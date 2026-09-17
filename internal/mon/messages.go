// Package mon implements monitor protocol messages and lifecycle management.
package mon

import (
	"fmt"
	"sort"

	wire "github.com/otuschhoff/rados-go/internal/encoding"
	"github.com/otuschhoff/rados-go/internal/maps"
	"github.com/otuschhoff/rados-go/internal/msgr"
	"github.com/otuschhoff/rados-go/internal/protocol"
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
	PoolOperationCreate            PoolOperation = 0x01
	PoolOperationDelete            PoolOperation = 0x02
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

type StatFSReply struct {
	FSID        maps.FSID
	Version     uint64
	KB          uint64
	KBUsed      uint64
	KBAvailable uint64
	Objects     uint64
}

type PoolStats struct {
	BytesUsed  uint64
	Objects    uint64
	ReadBytes  uint64
	WriteBytes uint64
}

type PoolStatsReply struct {
	FSID    maps.FSID
	Version uint64
	Pools   map[string]PoolStats
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

func EncodeStatFS(fsid maps.FSID, version uint64, maxBytes uint32) (msgr.Message, error) {
	if maxBytes == 0 {
		return msgr.Message{}, wire.ErrLimitExceeded
	}
	encoder := wire.NewEncoder(maxBytes)
	encodePaxosHeader(encoder, version)
	encoder.Raw(fsid[:])
	encoder.Uint8(0)
	front, err := encoder.BytesResult()
	if err != nil {
		return msgr.Message{}, err
	}
	return frontMessage(protocol.MessageStatFS, 2, 1, front), nil
}

func DecodeStatFSReply(message msgr.Message, maxBytes uint32) (StatFSReply, error) {
	if err := validateFrontMessage(message, protocol.MessageStatFSReply, maxBytes); err != nil {
		return StatFSReply{}, err
	}
	if message.Header.Version < 1 || message.Header.CompatVersion > 1 {
		return StatFSReply{}, fmt.Errorf("%w: statfs reply version=%d compat=%d", wire.ErrUnsupportedVersion, message.Header.Version, message.Header.CompatVersion)
	}
	decoder := wire.NewDecoder(message.Front, wire.Limits{MaxBytes: maxBytes})
	var result StatFSReply
	copy(result.FSID[:], decoder.Raw(16))
	result.Version = decoder.Uint64()
	result.KB = decoder.Uint64()
	result.KBUsed = decoder.Uint64()
	result.KBAvailable = decoder.Uint64()
	result.Objects = decoder.Uint64()
	if err := decoder.Finish(); err != nil {
		return StatFSReply{}, err
	}
	if decoder.Remaining() != 0 || len(message.Middle) != 0 || len(message.Data) != 0 {
		return StatFSReply{}, wire.ErrMalformed
	}
	return result, nil
}

func EncodeGetPoolStats(fsid maps.FSID, version uint64, pools []string, maxBytes uint32) (msgr.Message, error) {
	if maxBytes == 0 || len(pools) == 0 || uint64(len(pools)) > uint64(^uint32(0)) {
		return msgr.Message{}, wire.ErrLimitExceeded
	}
	encoder := wire.NewEncoder(maxBytes)
	encodePaxosHeader(encoder, version)
	encoder.Raw(fsid[:])
	encoder.Uint32(uint32(len(pools)))
	for _, pool := range pools {
		if pool == "" {
			return msgr.Message{}, wire.ErrMalformed
		}
		encoder.String(pool)
	}
	front, err := encoder.BytesResult()
	if err != nil {
		return msgr.Message{}, err
	}
	return frontMessage(protocol.MessageGetPoolStats, 1, 0, front), nil
}

func DecodeGetPoolStatsReply(message msgr.Message, maxBytes, maxPools uint32) (PoolStatsReply, error) {
	if maxPools == 0 {
		return PoolStatsReply{}, wire.ErrLimitExceeded
	}
	if err := validateFrontMessage(message, protocol.MessageGetPoolStatsReply, maxBytes); err != nil {
		return PoolStatsReply{}, err
	}
	if message.Header.Version < 1 || message.Header.Version > 2 || message.Header.CompatVersion > 1 {
		return PoolStatsReply{}, wire.ErrUnsupportedVersion
	}
	decoder := wire.NewDecoder(message.Front, wire.Limits{MaxBytes: maxBytes})
	result := PoolStatsReply{Version: decodePaxosHeader(decoder)}
	copy(result.FSID[:], decoder.Raw(16))
	count := decoder.Uint32()
	if count > maxPools {
		return PoolStatsReply{}, wire.ErrLimitExceeded
	}
	type rawPoolStats struct {
		numBytes, objects, readKB, writeKB, hitSetBytes, omapBytes int64
		allocated, omapAllocated                                   int64
	}
	raw := make(map[string]rawPoolStats, count)
	for range count {
		name := decoder.String()
		if _, exists := raw[name]; exists {
			return PoolStatsReply{}, wire.ErrMalformed
		}
		stats, err := decodePoolStats(decoder, maxPools)
		if err != nil {
			return PoolStatsReply{}, err
		}
		raw[name] = stats
	}
	perPool := false
	if message.Header.Version >= 2 {
		perPool = decoder.Bool()
	}
	if err := decoder.Finish(); err != nil {
		return PoolStatsReply{}, err
	}
	if decoder.Remaining() != 0 || len(message.Middle) != 0 || len(message.Data) != 0 {
		return PoolStatsReply{}, wire.ErrMalformed
	}
	result.Pools = make(map[string]PoolStats, len(raw))
	for name, stats := range raw {
		values := []int64{stats.numBytes, stats.objects, stats.readKB, stats.writeKB, stats.hitSetBytes, stats.omapBytes, stats.allocated, stats.omapAllocated}
		for _, value := range values {
			if value < 0 {
				return PoolStatsReply{}, wire.ErrMalformed
			}
		}
		used, overflow := addUint64(uint64(stats.numBytes), uint64(stats.hitSetBytes), uint64(stats.omapBytes))
		if perPool {
			used, overflow = addUint64(uint64(stats.allocated), uint64(stats.omapAllocated))
		}
		if overflow || uint64(stats.readKB) > ^uint64(0)>>10 || uint64(stats.writeKB) > ^uint64(0)>>10 {
			return PoolStatsReply{}, wire.ErrLimitExceeded
		}
		result.Pools[name] = PoolStats{BytesUsed: used, Objects: uint64(stats.objects), ReadBytes: uint64(stats.readKB) << 10, WriteBytes: uint64(stats.writeKB) << 10}
	}
	return result, nil
}

func addUint64(values ...uint64) (uint64, bool) {
	var result uint64
	for _, value := range values {
		if value > ^uint64(0)-result {
			return 0, true
		}
		result += value
	}
	return result, false
}

func decodePoolStats(decoder *wire.Decoder, maxEntries uint32) (struct {
	numBytes, objects, readKB, writeKB, hitSetBytes, omapBytes int64
	allocated, omapAllocated                                   int64
}, error) {
	var result struct {
		numBytes, objects, readKB, writeKB, hitSetBytes, omapBytes int64
		allocated, omapAllocated                                   int64
	}
	version, payload := decoder.Versioned(7)
	if err := decoder.Finish(); err != nil || version != 7 {
		return result, firstError(err, wire.ErrUnsupportedVersion)
	}
	collectionVersion, collection := payload.Versioned(2)
	if err := payload.Finish(); err != nil || collectionVersion != 2 {
		return result, firstError(err, wire.ErrUnsupportedVersion)
	}
	sumVersion, sum := collection.Versioned(20)
	if err := collection.Finish(); err != nil || sumVersion != 20 {
		return result, firstError(err, wire.ErrUnsupportedVersion)
	}
	for index := 0; index < 40; index++ {
		var value int64
		if index >= 28 && index <= 31 {
			value = int64(sum.Int32())
		} else {
			value = sum.Int64()
		}
		switch index {
		case 0:
			result.numBytes = value
		case 1:
			result.objects = value
		case 8:
			result.readKB = value
		case 10:
			result.writeKB = value
		case 22:
			result.hitSetBytes = value
		case 37:
			result.omapBytes = value
		}
	}
	if err := sum.Finish(); err != nil || sum.Remaining() != 0 {
		return result, firstError(err, wire.ErrMalformed)
	}
	categories := collection.Uint32()
	if categories > maxEntries {
		return result, wire.ErrLimitExceeded
	}
	for range categories {
		_ = collection.String()
		if err := skipObjectStatSum(collection); err != nil {
			return result, err
		}
	}
	if err := collection.Finish(); err != nil || collection.Remaining() != 0 {
		return result, firstError(err, wire.ErrMalformed)
	}
	payload.Int64()
	payload.Int64()
	payload.Int32()
	payload.Int32()
	storeVersion, store := payload.Versioned(1)
	if err := payload.Finish(); err != nil || storeVersion != 1 {
		return result, firstError(err, wire.ErrUnsupportedVersion)
	}
	store.Uint64()
	store.Uint64()
	store.Uint64()
	result.allocated = store.Int64()
	store.Int64()
	store.Int64()
	store.Int64()
	store.Int64()
	result.omapAllocated = store.Int64()
	store.Int64()
	if err := store.Finish(); err != nil || store.Remaining() != 0 {
		return result, firstError(err, wire.ErrMalformed)
	}
	payload.Int32()
	if err := payload.Finish(); err != nil || payload.Remaining() != 0 {
		return result, firstError(err, wire.ErrMalformed)
	}
	return result, nil
}

func skipObjectStatSum(decoder *wire.Decoder) error {
	version, payload := decoder.Versioned(20)
	if err := decoder.Finish(); err != nil || version != 20 {
		return firstError(err, wire.ErrUnsupportedVersion)
	}
	for index := 0; index < 40; index++ {
		if index >= 28 && index <= 31 {
			payload.Int32()
		} else {
			payload.Int64()
		}
	}
	if err := payload.Finish(); err != nil || payload.Remaining() != 0 {
		return firstError(err, wire.ErrMalformed)
	}
	return nil
}

func firstError(actual, fallback error) error {
	if actual != nil {
		return actual
	}
	return fallback
}

func EncodePoolOperation(fsid maps.FSID, pool uint32, operation PoolOperation, snapID uint64, name string, version uint64, maxBytes uint32) (msgr.Message, error) {
	if maxBytes == 0 {
		return msgr.Message{}, wire.ErrLimitExceeded
	}
	switch operation {
	case PoolOperationCreate:
		if pool != 0 || name == "" || snapID != 0 {
			return msgr.Message{}, fmt.Errorf("%w: pool creation arguments", wire.ErrMalformed)
		}
	case PoolOperationDelete:
		if pool == 0 || name != "delete" || snapID != 0 {
			return msgr.Message{}, fmt.Errorf("%w: pool deletion arguments", wire.ErrMalformed)
		}
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
