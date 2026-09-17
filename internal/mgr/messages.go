// Package mgr implements the focused manager protocol slice used by later manager clients.
package mgr

import (
	"fmt"

	wire "github.com/otuschhoff/rados-go/internal/encoding"
	"github.com/otuschhoff/rados-go/internal/maps"
	"github.com/otuschhoff/rados-go/internal/msgr"
	"github.com/otuschhoff/rados-go/internal/protocol"
)

type CommandReply struct {
	Result protocol.WireErrno
	Status string
	Data   []byte
}

func EncodeCommand(fsid maps.FSID, command []string, input []byte, maxBytes uint32) (msgr.Message, error) {
	if maxBytes == 0 || len(command) == 0 || uint64(len(command)) > uint64(^uint32(0)) || uint64(len(input)) > uint64(maxBytes) {
		return msgr.Message{}, wire.ErrLimitExceeded
	}
	encoder := wire.NewEncoder(maxBytes)
	encoder.Raw(fsid[:])
	encoder.Uint32(uint32(len(command)))
	for _, item := range command {
		encoder.String(item)
	}
	front, err := encoder.BytesResult()
	if err != nil {
		return msgr.Message{}, err
	}
	if uint64(len(front))+uint64(len(input)) > uint64(maxBytes) {
		return msgr.Message{}, wire.ErrLimitExceeded
	}
	message := frontMessage(protocol.MessageMgrCommand, 1, 0, front)
	message.Data = append([]byte(nil), input...)
	message.Lengths.Data = uint32(len(message.Data))
	return message, nil
}

func DecodeCommandReply(message msgr.Message, maxBytes uint32) (CommandReply, error) {
	if err := validateMessagePayload(message, protocol.MessageMgrCommandReply, 1, 1, maxBytes); err != nil {
		return CommandReply{}, err
	}
	decoder := wire.NewDecoder(message.Front, wire.Limits{MaxBytes: maxBytes})
	result := CommandReply{Result: protocol.DecodeWireErrno(decoder), Status: decoder.String()}
	if err := finishExact(decoder, "manager command reply"); err != nil {
		return CommandReply{}, err
	}
	result.Data = append([]byte(nil), message.Data...)
	return result, nil
}

func DecodeMgrMap(message msgr.Message, limits maps.Limits) (*maps.MgrMap, error) {
	if err := validateFrontMessage(message, protocol.MessageMgrMap, 1, 1, limits.MaxBytes); err != nil {
		return nil, err
	}
	return maps.DecodeMgrMap(message.Front, limits)
}

func frontMessage(messageType, version, compat uint16, front []byte) msgr.Message {
	return msgr.Message{
		Header:  msgr.MessageHeader{Type: messageType, Version: version, CompatVersion: compat},
		Lengths: msgr.MessageLengths{Front: uint32(len(front))},
		Front:   front,
	}
}

func validateFrontMessage(message msgr.Message, messageType, version, compat uint16, maxBytes uint32) error {
	if maxBytes == 0 || uint64(len(message.Front)) > uint64(maxBytes) {
		return wire.ErrLimitExceeded
	}
	if message.Header.Type != messageType || message.Header.Version != version || message.Header.CompatVersion != compat {
		return fmt.Errorf("%w: manager message type=%d version=%d compat=%d", wire.ErrUnsupportedVersion, message.Header.Type, message.Header.Version, message.Header.CompatVersion)
	}
	if len(message.Middle) != 0 || len(message.Data) != 0 || message.Lengths != (msgr.MessageLengths{Front: uint32(len(message.Front))}) {
		return fmt.Errorf("%w: manager message segment lengths", wire.ErrMalformed)
	}
	return nil
}

func validateMessagePayload(message msgr.Message, messageType, version, compat uint16, maxBytes uint32) error {
	if maxBytes == 0 || uint64(len(message.Front))+uint64(len(message.Data)) > uint64(maxBytes) {
		return wire.ErrLimitExceeded
	}
	if message.Header.Type != messageType || message.Header.Version != version || message.Header.CompatVersion != compat || len(message.Middle) != 0 || message.Lengths != (msgr.MessageLengths{Front: uint32(len(message.Front)), Data: uint32(len(message.Data))}) {
		return fmt.Errorf("%w: manager message payload", wire.ErrMalformed)
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
