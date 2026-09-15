// Package osd implements the read-only Ceph OSD protocol and request lifecycle.
package osd

import (
	"errors"
	"fmt"

	wire "github.com/otuschhoff/go-librados/internal/encoding"
	"github.com/otuschhoff/go-librados/internal/maps"
	"github.com/otuschhoff/go-librados/internal/msgr"
	"github.com/otuschhoff/go-librados/internal/protocol"
)

var ErrMalformedReply = errors.New("malformed OSD reply")

const (
	OpRead                = uint16(0x1201)
	OpStat                = uint16(0x1202)
	OpNotify              = uint16(0x1206)
	OpNotifyAck           = uint16(0x1207)
	OpListWatchers        = uint16(0x1209)
	OpAssertVer           = uint16(0x1208)
	OpOmapGetKeys         = uint16(0x1211)
	OpOmapGetValues       = uint16(0x1212)
	OpOmapGetHeader       = uint16(0x1213)
	OpOmapGetValuesByKeys = uint16(0x1214)
	OpOmapCompare         = uint16(0x1219)
	OpCompareExtent       = uint16(0x1220)
	OpGetXattr            = uint16(0x1301)
	OpGetXattrs           = uint16(0x1302)
	OpCompareXattr        = uint16(0x1303)
	OpCall                = uint16(0x1401)
	OpPGList              = uint16(0x1501)
	OpPGNList             = uint16(0x1505)
	OpWrite               = uint16(0x2201)
	OpWriteFull           = uint16(0x2202)
	OpTruncate            = uint16(0x2203)
	OpZero                = uint16(0x2204)
	OpDelete              = uint16(0x2205)
	OpAppend              = uint16(0x2206)
	OpWatch               = uint16(0x220f)
	OpCreate              = uint16(0x220d)
	OpOmapSetValues       = uint16(0x2215)
	OpOmapSetHeader       = uint16(0x2216)
	OpOmapClear           = uint16(0x2217)
	OpOmapRemoveKeys      = uint16(0x2218)
	OpOmapRemoveRange     = uint16(0x222c)
	OpSetXattr            = uint16(0x2301)
	OpRemoveXattr         = uint16(0x2304)

	OpFlagExclusive = uint32(0x0001)
	OpFlagFailOK    = uint32(0x0002)

	FlagAck           = uint32(0x0001)
	FlagWrite         = uint32(0x0020)
	FlagOnDisk        = uint32(0x0004)
	FlagRead          = uint32(0x0010)
	FlagRetry         = uint32(0x0008)
	FlagPGOp          = uint32(0x0400)
	FlagIgnoreCache   = uint32(0x8000)
	FlagIgnoreOverlay = uint32(0x20000)
	FlagRedirected    = uint32(0x200000)
	FlagReturnVector  = uint32(0x04000000)

	NoSnap                  = ^uint64(1)
	operationDescriptorSize = uint64(38)
)

type Operation struct {
	Code             uint16
	Flags            uint32
	Offset           uint64
	Length           uint64
	XattrNameLength  uint32
	XattrValueLength uint32
	CompareOperator  uint8
	CompareMode      uint8
	ClassNameLength  uint8
	MethodNameLength uint8
	ClassInputLength uint32
	WatchCookie      uint64
	WatchVersion     uint64
	WatchOperation   uint8
	WatchGeneration  uint32
	WatchTimeout     uint32
	AssertVersion    uint64
	ListCount        uint64
	ListStartEpoch   uint32
	PayloadLength    uint32
	Data             []byte
}

type Request struct {
	MapEpoch          uint32
	PG                maps.PG
	ObjectHash        uint32
	PoolID            int64
	Object            string
	Locator           string
	Namespace         string
	Snapshot          uint64
	TransactionID     uint64
	ClientGlobalID    uint64
	ClientIncarnation int32
	Retry             int32
	Flags             uint32
	Features          uint64
	Operations        []Operation
}

type OperationResult struct {
	Operation uint16
	Code      int32
	Data      []byte
}

type Redirect struct {
	Pool      int64
	Locator   string
	Namespace string
	Object    string
}

type Reply struct {
	Object     string
	PG         maps.PG
	Flags      int64
	Result     int32
	MapEpoch   uint32
	Retry      int32
	Version    uint64
	Redirect   *Redirect
	Operations []OperationResult
}

type Limits struct {
	MaxBytes      uint32
	MaxOperations uint32
}

func EncodeRequest(request Request, limits Limits) (msgr.Message, error) {
	if limits.MaxBytes == 0 || limits.MaxOperations == 0 || len(request.Operations) == 0 || uint64(len(request.Operations)) > uint64(limits.MaxOperations) || len(request.Operations) > int(^uint16(0)) || request.PoolID < 0 {
		return msgr.Message{}, wire.ErrLimitExceeded
	}
	mutation := false
	dataLength := uint64(0)
	for index := range request.Operations {
		operation := &request.Operations[index]
		if !supportedOperation(operation.Code) {
			return msgr.Message{}, fmt.Errorf("%w: operation %#x", wire.ErrUnsupportedVersion, operation.Code)
		}
		if isMutation(operation.Code) {
			mutation = true
		}
		if len(operation.Data) > int(^uint32(0)) {
			return msgr.Message{}, wire.ErrLimitExceeded
		}
		if operation.PayloadLength != 0 && operation.PayloadLength != uint32(len(operation.Data)) {
			return msgr.Message{}, wire.ErrMalformed
		}
		if err := validateOperation(*operation); err != nil {
			return msgr.Message{}, err
		}
		operation.PayloadLength = uint32(len(operation.Data))
		dataLength += uint64(len(operation.Data))
		if dataLength > uint64(limits.MaxBytes) {
			return msgr.Message{}, wire.ErrLimitExceeded
		}
	}
	encoder := wire.NewEncoder(limits.MaxBytes)
	encodeSPG(encoder, request.PG)
	encoder.Uint32(request.ObjectHash)
	encoder.Uint32(request.MapEpoch)
	flags := FlagRead
	if mutation || request.Flags&FlagWrite != 0 {
		flags = FlagWrite | FlagOnDisk
	}
	encoder.Uint32(flags | request.Flags)
	encodeRequestID(encoder, request.ClientGlobalID, request.TransactionID, request.ClientIncarnation)
	encodeTrace(encoder)
	encoder.Int32(request.ClientIncarnation)
	encodeUTime(encoder, 0, 0)
	encodeLocator(encoder, request.PoolID, request.Locator, request.Namespace)
	encoder.String(request.Object)
	encoder.Uint16(uint16(len(request.Operations)))
	for _, operation := range request.Operations {
		encodeOperation(encoder, operation)
	}
	encoder.Uint64(request.Snapshot)
	encoder.Uint64(0)
	encoder.Uint32(0)
	encoder.Int32(request.Retry)
	encoder.Uint64(request.Features)
	front, err := encoder.BytesResult()
	if err != nil {
		return msgr.Message{}, err
	}
	if uint64(len(front))+dataLength > uint64(limits.MaxBytes) {
		return msgr.Message{}, wire.ErrLimitExceeded
	}
	data := make([]byte, 0, dataLength)
	for _, operation := range request.Operations {
		data = append(data, operation.Data...)
	}
	return msgr.Message{Header: msgr.MessageHeader{TransactionID: request.TransactionID, Type: protocol.MessageOSDOp, Version: 8, CompatVersion: 3}, Front: front, Data: data, Lengths: msgr.MessageLengths{Front: uint32(len(front)), Data: uint32(len(data))}}, nil
}

func supportedOperation(code uint16) bool {
	switch code {
	case OpRead, OpStat, OpNotify, OpNotifyAck, OpListWatchers, OpAssertVer, OpOmapGetKeys, OpOmapGetValues,
		OpOmapGetValuesByKeys, OpOmapGetHeader, OpOmapCompare, OpCompareExtent,
		OpGetXattr, OpGetXattrs, OpCompareXattr, OpCall, OpPGList, OpPGNList,
		OpWrite, OpWriteFull, OpTruncate, OpZero, OpDelete, OpAppend, OpWatch, OpCreate,
		OpOmapSetValues, OpOmapSetHeader, OpOmapClear, OpOmapRemoveKeys,
		OpOmapRemoveRange, OpSetXattr, OpRemoveXattr:
		return true
	default:
		return false
	}
}

func validateOperation(operation Operation) error {
	switch operation.Code {
	case OpPGNList:
		if operation.ListCount == 0 || len(operation.Data) == 0 {
			return wire.ErrMalformed
		}
	case OpGetXattr, OpRemoveXattr:
		if operation.XattrValueLength != 0 || uint64(operation.XattrNameLength) != uint64(len(operation.Data)) {
			return wire.ErrMalformed
		}
	case OpSetXattr, OpCompareXattr:
		if uint64(operation.XattrNameLength)+uint64(operation.XattrValueLength) != uint64(len(operation.Data)) {
			return wire.ErrMalformed
		}
	case OpCall:
		if operation.ClassNameLength == 0 || operation.MethodNameLength == 0 ||
			uint64(operation.ClassNameLength)+uint64(operation.MethodNameLength)+uint64(operation.ClassInputLength) != uint64(len(operation.Data)) {
			return wire.ErrMalformed
		}
	case OpWatch:
		if operation.WatchCookie == 0 || (operation.WatchOperation != WatchOperationUnwatch && operation.WatchOperation != WatchOperationRegister && operation.WatchOperation != WatchOperationReconnect && operation.WatchOperation != WatchOperationPing) || len(operation.Data) != 0 {
			return wire.ErrMalformed
		}
	case OpNotify, OpNotifyAck:
		if operation.WatchCookie == 0 || len(operation.Data) == 0 {
			return wire.ErrMalformed
		}
	}
	return nil
}

func isMutation(code uint16) bool { return code&0x2000 != 0 }

func DecodeReply(message msgr.Message, limits Limits) (Reply, error) {
	if limits.MaxBytes == 0 || limits.MaxOperations == 0 || message.Header.Type != protocol.MessageOSDOpReply || message.Header.Version < 4 || message.Header.CompatVersion > 8 || uint64(len(message.Front))+uint64(len(message.Middle))+uint64(len(message.Data)) > uint64(limits.MaxBytes) {
		return Reply{}, ErrMalformedReply
	}
	decoder := wire.NewDecoder(message.Front, wire.Limits{MaxBytes: limits.MaxBytes})
	reply := Reply{Object: decoder.String()}
	var err error
	reply.PG, err = decodePG(decoder)
	if err != nil {
		return Reply{}, err
	}
	reply.Flags = decoder.Int64()
	reply.Result = decoder.Int32()
	decodeEVersion(decoder)
	reply.MapEpoch = decoder.Uint32()
	count := decoder.Uint32()
	if err := decoder.Finish(); err != nil {
		return Reply{}, err
	}
	if count > limits.MaxOperations || uint64(count)*operationDescriptorSize > decoder.Remaining() {
		return Reply{}, wire.ErrLimitExceeded
	}
	reply.Operations = make([]OperationResult, count)
	lengths := make([]uint32, count)
	for index := range lengths {
		reply.Operations[index].Operation, lengths[index] = decodeOperation(decoder)
	}
	reply.Retry = decoder.Int32()
	for index := range reply.Operations {
		reply.Operations[index].Code = decoder.Int32()
	}
	decodeEVersion(decoder)
	reply.Version = decoder.Uint64()
	if message.Header.Version == 6 {
		return Reply{}, fmt.Errorf("%w: legacy redirect encoding", wire.ErrUnsupportedVersion)
	}
	if message.Header.Version >= 7 {
		if decoder.Bool() {
			redirect, err := decodeRedirect(decoder)
			if err != nil {
				return Reply{}, err
			}
			reply.Redirect = &redirect
		}
	}
	if message.Header.Version >= 8 {
		if err := decodeTrace(decoder); err != nil {
			return Reply{}, err
		}
	}
	if err := decoder.Finish(); err != nil {
		return Reply{}, fmt.Errorf("%w: front: %v", ErrMalformedReply, err)
	}
	if decoder.Remaining() != 0 {
		return Reply{}, fmt.Errorf("%w: %d trailing front bytes", ErrMalformedReply, decoder.Remaining())
	}
	offset := uint64(0)
	for index, length := range lengths {
		if uint64(length) > uint64(len(message.Data))-offset {
			return Reply{}, fmt.Errorf("%w: operation %d needs %d data bytes after offset %d", ErrMalformedReply, index, length, offset)
		}
		reply.Operations[index].Data = append([]byte(nil), message.Data[offset:offset+uint64(length)]...)
		offset += uint64(length)
	}
	if offset != uint64(len(message.Data)) {
		return Reply{}, fmt.Errorf("%w: %d trailing data bytes for operation lengths %v", ErrMalformedReply, uint64(len(message.Data))-offset, lengths)
	}
	return reply, nil
}

func encodePG(encoder *wire.Encoder, pg maps.PG) {
	encoder.Uint8(1)
	encoder.Uint64(pg.Pool)
	encoder.Uint32(pg.Seed)
	encoder.Int32(-1)
}

func encodeSPG(encoder *wire.Encoder, pg maps.PG) {
	encoder.Versioned(1, 1, func(payload *wire.Encoder) {
		encodePG(payload, pg)
		payload.Uint8(0xff)
	})
}

func decodePG(decoder *wire.Decoder) (maps.PG, error) {
	if decoder.Uint8() != 1 {
		return maps.PG{}, wire.ErrUnsupportedVersion
	}
	pg := maps.PG{Pool: decoder.Uint64(), Seed: decoder.Uint32(), Preferred: decoder.Int32()}
	return pg, decoder.Finish()
}

func encodeOperation(encoder *wire.Encoder, operation Operation) {
	encoder.Uint16(operation.Code)
	encoder.Uint32(operation.Flags)
	switch operation.Code {
	case OpGetXattr, OpGetXattrs, OpCompareXattr, OpSetXattr, OpRemoveXattr:
		encoder.Uint32(operation.XattrNameLength)
		encoder.Uint32(operation.XattrValueLength)
		encoder.Uint8(operation.CompareOperator)
		encoder.Uint8(operation.CompareMode)
		encoder.Raw(make([]byte, 18))
	case OpAssertVer:
		encoder.Uint64(0)
		encoder.Uint64(operation.AssertVersion)
		encoder.Raw(make([]byte, 12))
	case OpCall:
		encoder.Uint8(operation.ClassNameLength)
		encoder.Uint8(operation.MethodNameLength)
		encoder.Uint8(0)
		encoder.Uint32(operation.ClassInputLength)
		encoder.Raw(make([]byte, 21))
	case OpWatch:
		encoder.Uint64(operation.WatchCookie)
		encoder.Uint64(operation.WatchVersion)
		encoder.Uint8(operation.WatchOperation)
		encoder.Uint32(operation.WatchGeneration)
		encoder.Uint32(operation.WatchTimeout)
		encoder.Raw(make([]byte, 3))
	case OpNotify, OpNotifyAck:
		encoder.Uint64(operation.WatchCookie)
		encoder.Raw(make([]byte, 20))
	case OpPGList, OpPGNList:
		encoder.Uint64(operation.ListCount)
		encoder.Uint32(operation.ListStartEpoch)
		encoder.Raw(make([]byte, 16))
	default:
		encoder.Uint64(operation.Offset)
		encoder.Uint64(operation.Length)
		encoder.Uint64(0)
		encoder.Uint32(0)
	}
	encoder.Uint32(operation.PayloadLength)
}

func decodeOperation(decoder *wire.Decoder) (uint16, uint32) {
	code := decoder.Uint16()
	decoder.Uint32()
	decoder.Raw(28)
	return code, decoder.Uint32()
}

func encodeLocator(encoder *wire.Encoder, pool int64, key, namespace string) {
	encoder.Versioned(6, 3, func(payload *wire.Encoder) {
		payload.Int64(pool)
		payload.Int32(-1)
		payload.String(key)
		payload.String(namespace)
		payload.Int64(-1)
	})
}

func decodeRedirect(decoder *wire.Decoder) (Redirect, error) {
	version, payload := decoder.Versioned(1)
	if err := decoder.Finish(); err != nil {
		return Redirect{}, err
	}
	if version != 1 {
		return Redirect{}, wire.ErrUnsupportedVersion
	}
	locatorVersion, locator := payload.Versioned(6)
	if err := payload.Finish(); err != nil {
		return Redirect{}, err
	}
	if locatorVersion < 3 {
		return Redirect{}, wire.ErrUnsupportedVersion
	}
	redirect := Redirect{Pool: locator.Int64()}
	locator.Int32()
	redirect.Locator = locator.String()
	redirect.Namespace = locator.String()
	if locatorVersion >= 6 {
		locator.Int64()
	}
	if err := locator.Finish(); err != nil || locator.Remaining() != 0 {
		return Redirect{}, ErrMalformedReply
	}
	redirect.Object = payload.String()
	legacyLength := payload.Uint32()
	if uint64(legacyLength) > payload.Remaining() {
		return Redirect{}, ErrMalformedReply
	}
	payload.Raw(legacyLength)
	if err := payload.Finish(); err != nil || payload.Remaining() != 0 {
		return Redirect{}, ErrMalformedReply
	}
	return redirect, nil
}

func encodeRequestID(encoder *wire.Encoder, globalID, transactionID uint64, incarnation int32) {
	encoder.Versioned(2, 2, func(requestID *wire.Encoder) {
		requestID.Uint8(uint8(protocol.EntityClient))
		requestID.Uint64(globalID)
		requestID.Uint64(transactionID)
		requestID.Int32(incarnation)
	})
}

func encodeEVersion(encoder *wire.Encoder, epoch uint32, version uint64) {
	encoder.Uint32(epoch)
	encoder.Uint64(version)
}

func decodeEVersion(decoder *wire.Decoder) {
	decoder.Uint32()
	decoder.Uint64()
}

func encodeUTime(encoder *wire.Encoder, seconds, nanoseconds uint32) {
	encoder.Uint32(seconds)
	encoder.Uint32(nanoseconds)
}

func decodeTrace(decoder *wire.Decoder) error {
	decoder.Int64()
	decoder.Int64()
	decoder.Int64()
	return decoder.Finish()
}

func encodeTrace(encoder *wire.Encoder) {
	encoder.Int64(0)
	encoder.Int64(0)
	encoder.Int64(0)
}
