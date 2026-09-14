package msgr

import (
	"errors"
	"fmt"
	"math"

	wire "github.com/otuschhoff/go-librados/internal/encoding"
	"github.com/otuschhoff/go-librados/internal/protocol"
)

var ErrUnsupportedPayload = errors.New("unsupported messenger payload")

var controlFeatures = protocol.FeatureMessageAddress2 | protocol.FeatureServerNautilusMask

const ConnectionFlagLossy uint64 = 1

type Hello struct {
	EntityType  protocol.EntityType
	PeerAddress protocol.EntityAddr
}

type ClientIdent struct {
	Addresses         protocol.EntityAddrVec
	TargetAddress     protocol.EntityAddr
	GlobalID          int64
	GlobalSequence    uint64
	SupportedFeatures uint64
	RequiredFeatures  uint64
	Flags             uint64
	Cookie            uint64
}

type ServerIdent struct {
	Addresses         protocol.EntityAddrVec
	GlobalID          int64
	GlobalSequence    uint64
	SupportedFeatures uint64
	RequiredFeatures  uint64
	Flags             uint64
	Cookie            uint64
}

type IdentMissingFeatures struct{ Features uint64 }

type SessionReconnect struct {
	Addresses       protocol.EntityAddrVec
	ClientCookie    uint64
	ServerCookie    uint64
	GlobalSequence  uint64
	ConnectSequence uint64
	MessageSequence uint64
}

type SessionReset struct{ Full bool }
type SessionRetry struct{ ConnectSequence uint64 }
type SessionRetryGlobal struct{ GlobalSequence uint64 }
type SessionReconnectOK struct{ MessageSequence uint64 }
type Wait struct{}

type Timestamp struct {
	Seconds     uint32
	Nanoseconds uint32
}

type Keepalive2 struct{ Timestamp Timestamp }
type Keepalive2Ack struct{ Timestamp Timestamp }
type Ack struct{ Sequence uint64 }

type AuthRequest struct {
	Method         uint32
	PreferredModes []uint32
	AuthPayload    []byte
}

type AuthBadMethod struct {
	Method         uint32
	Result         int32
	AllowedMethods []uint32
	AllowedModes   []uint32
}

type AuthReplyMore struct{ AuthPayload []byte }
type AuthRequestMore struct{ AuthPayload []byte }

type AuthDone struct {
	GlobalID       uint64
	ConnectionMode uint32
	AuthPayload    []byte
}

type AuthSignature struct{ Signature [32]byte }

// AuthPayload preserves the complete control segment for auth tags. CephX
// interpretation belongs to the authentication layer.
type AuthPayload struct {
	Tag     Tag
	Payload []byte
}

func EncodeControl(payload any, limits Limits) (Frame, error) {
	tag, err := controlTag(payload)
	if err != nil {
		return Frame{}, err
	}
	if tag == TagCompressionRequest || tag == TagCompressionDone {
		return Frame{}, fmt.Errorf("%w: compression tag %d", ErrUnsupportedPayload, tag)
	}

	encoder := wire.NewEncoder(limits.MaxSegmentBytes)
	switch value := payload.(type) {
	case Hello:
		encoder.Uint8(uint8(value.EntityType))
		err = value.PeerAddress.Encode(encoder, controlFeatures)
	case ClientIdent:
		if uint64(len(value.Addresses)) > uint64(limits.MaxAddresses) {
			return Frame{}, wire.ErrLimitExceeded
		}
		err = value.Addresses.Encode(encoder, controlFeatures)
		if err == nil {
			err = value.TargetAddress.Encode(encoder, controlFeatures)
		}
		encoder.Int64(value.GlobalID)
		encoder.Uint64(value.GlobalSequence)
		encoder.Uint64(value.SupportedFeatures)
		encoder.Uint64(value.RequiredFeatures)
		encoder.Uint64(value.Flags)
		encoder.Uint64(value.Cookie)
	case ServerIdent:
		if uint64(len(value.Addresses)) > uint64(limits.MaxAddresses) {
			return Frame{}, wire.ErrLimitExceeded
		}
		err = value.Addresses.Encode(encoder, controlFeatures)
		encoder.Int64(value.GlobalID)
		encoder.Uint64(value.GlobalSequence)
		encoder.Uint64(value.SupportedFeatures)
		encoder.Uint64(value.RequiredFeatures)
		encoder.Uint64(value.Flags)
		encoder.Uint64(value.Cookie)
	case IdentMissingFeatures:
		encoder.Uint64(value.Features)
	case SessionReconnect:
		if uint64(len(value.Addresses)) > uint64(limits.MaxAddresses) {
			return Frame{}, wire.ErrLimitExceeded
		}
		err = value.Addresses.Encode(encoder, controlFeatures)
		encoder.Uint64(value.ClientCookie)
		encoder.Uint64(value.ServerCookie)
		encoder.Uint64(value.GlobalSequence)
		encoder.Uint64(value.ConnectSequence)
		encoder.Uint64(value.MessageSequence)
	case SessionReset:
		encoder.Bool(value.Full)
	case SessionRetry:
		encoder.Uint64(value.ConnectSequence)
	case SessionRetryGlobal:
		encoder.Uint64(value.GlobalSequence)
	case SessionReconnectOK:
		encoder.Uint64(value.MessageSequence)
	case Wait:
	case Keepalive2:
		encodeTimestamp(encoder, value.Timestamp)
	case Keepalive2Ack:
		encodeTimestamp(encoder, value.Timestamp)
	case Ack:
		encoder.Uint64(value.Sequence)
	case AuthRequest:
		if uint64(len(value.AuthPayload)) > uint64(limits.MaxAuthBytes) {
			return Frame{}, wire.ErrLimitExceeded
		}
		encoder.Uint32(value.Method)
		if err = encodeUint32Slice(encoder, value.PreferredModes); err != nil {
			break
		}
		encoder.Bytes(value.AuthPayload)
	case AuthBadMethod:
		encoder.Uint32(value.Method)
		encoder.Int32(value.Result)
		if err = encodeUint32Slice(encoder, value.AllowedMethods); err != nil {
			break
		}
		err = encodeUint32Slice(encoder, value.AllowedModes)
	case AuthReplyMore:
		if uint64(len(value.AuthPayload)) > uint64(limits.MaxAuthBytes) {
			return Frame{}, wire.ErrLimitExceeded
		}
		encoder.Bytes(value.AuthPayload)
	case AuthRequestMore:
		if uint64(len(value.AuthPayload)) > uint64(limits.MaxAuthBytes) {
			return Frame{}, wire.ErrLimitExceeded
		}
		encoder.Bytes(value.AuthPayload)
	case AuthDone:
		if uint64(len(value.AuthPayload)) > uint64(limits.MaxAuthBytes) {
			return Frame{}, wire.ErrLimitExceeded
		}
		encoder.Uint64(value.GlobalID)
		encoder.Uint32(value.ConnectionMode)
		encoder.Bytes(value.AuthPayload)
	case AuthSignature:
		encoder.Raw(value.Signature[:])
	case AuthPayload:
		if uint64(len(value.Payload)) > uint64(limits.MaxAuthBytes) {
			return Frame{}, wire.ErrLimitExceeded
		}
		encoder.Raw(value.Payload)
	}
	if err != nil {
		return Frame{}, err
	}
	data, err := encoder.BytesResult()
	if err != nil {
		return Frame{}, err
	}
	return Frame{Tag: tag, Segments: []Segment{{Alignment: DefaultAlignment, Data: data}}}, nil
}

func DecodeControl(frame Frame, limits Limits) (any, error) {
	if frame.Tag == TagCompressionRequest || frame.Tag == TagCompressionDone {
		return nil, fmt.Errorf("%w: compression tag %d", ErrUnsupportedPayload, frame.Tag)
	}
	if frame.Tag == TagMessage || frame.Tag < TagHello || frame.Tag > TagAck {
		return nil, fmt.Errorf("%w: control tag %d", ErrMalformed, frame.Tag)
	}
	if len(frame.Segments) != 1 || frame.Segments[0].Alignment != DefaultAlignment {
		return nil, fmt.Errorf("%w: control payload requires one segment at alignment %d", ErrMalformed, DefaultAlignment)
	}
	data := frame.Segments[0].Data
	if uint64(len(data)) > uint64(limits.MaxSegmentBytes) {
		return nil, wire.ErrLimitExceeded
	}
	decoder := wire.NewDecoder(data, wire.Limits{MaxBytes: limits.MaxSegmentBytes})
	var payload any
	var err error
	switch frame.Tag {
	case TagHello:
		value := Hello{EntityType: protocol.EntityType(decoder.Uint8())}
		value.PeerAddress, err = protocol.DecodeEntityAddr(decoder)
		payload = value
	case TagClientIdent:
		value := ClientIdent{}
		value.Addresses, err = protocol.DecodeEntityAddrVec(decoder, limits.MaxAddresses)
		if err == nil {
			value.TargetAddress, err = protocol.DecodeEntityAddr(decoder)
		}
		value.GlobalID = decoder.Int64()
		value.GlobalSequence = decoder.Uint64()
		value.SupportedFeatures = decoder.Uint64()
		value.RequiredFeatures = decoder.Uint64()
		value.Flags = decoder.Uint64()
		value.Cookie = decoder.Uint64()
		payload = value
	case TagServerIdent:
		value := ServerIdent{}
		value.Addresses, err = protocol.DecodeEntityAddrVec(decoder, limits.MaxAddresses)
		value.GlobalID = decoder.Int64()
		value.GlobalSequence = decoder.Uint64()
		value.SupportedFeatures = decoder.Uint64()
		value.RequiredFeatures = decoder.Uint64()
		value.Flags = decoder.Uint64()
		value.Cookie = decoder.Uint64()
		payload = value
	case TagIdentMissingFeatures:
		payload = IdentMissingFeatures{Features: decoder.Uint64()}
	case TagSessionReconnect:
		value := SessionReconnect{}
		value.Addresses, err = protocol.DecodeEntityAddrVec(decoder, limits.MaxAddresses)
		value.ClientCookie = decoder.Uint64()
		value.ServerCookie = decoder.Uint64()
		value.GlobalSequence = decoder.Uint64()
		value.ConnectSequence = decoder.Uint64()
		value.MessageSequence = decoder.Uint64()
		payload = value
	case TagSessionReset:
		encoded := decoder.Uint8()
		if encoded > 1 {
			err = fmt.Errorf("%w: bool value %d", wire.ErrMalformed, encoded)
		}
		payload = SessionReset{Full: encoded == 1}
	case TagSessionRetry:
		payload = SessionRetry{ConnectSequence: decoder.Uint64()}
	case TagSessionRetryGlobal:
		payload = SessionRetryGlobal{GlobalSequence: decoder.Uint64()}
	case TagSessionReconnectOK:
		payload = SessionReconnectOK{MessageSequence: decoder.Uint64()}
	case TagWait:
		payload = Wait{}
	case TagKeepalive2:
		payload, err = decodeKeepalive(decoder, false)
	case TagKeepalive2Ack:
		payload, err = decodeKeepalive(decoder, true)
	case TagAck:
		payload = Ack{Sequence: decoder.Uint64()}
	case TagAuthRequest:
		payload, err = decodeAuthRequest(decoder, limits)
	case TagAuthBadMethod:
		payload, err = decodeAuthBadMethod(decoder, limits)
	case TagAuthReplyMore:
		payload, err = decodeAuthReplyMore(decoder, limits)
	case TagAuthRequestMore:
		payload, err = decodeAuthRequestMore(decoder, limits)
	case TagAuthDone:
		payload, err = decodeAuthDone(decoder, limits)
	case TagAuthSignature:
		payload, err = decodeAuthSignature(decoder)
	}
	if err == nil {
		err = decoder.Finish()
	}
	if err == nil && decoder.Remaining() != 0 {
		err = fmt.Errorf("%w: %d trailing control bytes", wire.ErrMalformed, decoder.Remaining())
	}
	if err != nil {
		return nil, err
	}
	return payload, nil
}

func controlTag(payload any) (Tag, error) {
	switch value := payload.(type) {
	case Hello:
		return TagHello, nil
	case ClientIdent:
		return TagClientIdent, nil
	case ServerIdent:
		return TagServerIdent, nil
	case IdentMissingFeatures:
		return TagIdentMissingFeatures, nil
	case SessionReconnect:
		return TagSessionReconnect, nil
	case SessionReset:
		return TagSessionReset, nil
	case SessionRetry:
		return TagSessionRetry, nil
	case SessionRetryGlobal:
		return TagSessionRetryGlobal, nil
	case SessionReconnectOK:
		return TagSessionReconnectOK, nil
	case Wait:
		return TagWait, nil
	case Keepalive2:
		return TagKeepalive2, nil
	case Keepalive2Ack:
		return TagKeepalive2Ack, nil
	case Ack:
		return TagAck, nil
	case AuthRequest:
		return TagAuthRequest, nil
	case AuthBadMethod:
		return TagAuthBadMethod, nil
	case AuthReplyMore:
		return TagAuthReplyMore, nil
	case AuthRequestMore:
		return TagAuthRequestMore, nil
	case AuthDone:
		return TagAuthDone, nil
	case AuthSignature:
		return TagAuthSignature, nil
	case AuthPayload:
		if value.Tag < TagAuthRequest || value.Tag > TagAuthSignature {
			return 0, fmt.Errorf("%w: auth tag %d", ErrMalformed, value.Tag)
		}
		return value.Tag, nil
	default:
		return 0, fmt.Errorf("%w: %T", ErrUnsupportedPayload, payload)
	}
}

func encodeTimestamp(encoder *wire.Encoder, value Timestamp) {
	encoder.Uint32(value.Seconds)
	encoder.Uint32(value.Nanoseconds)
}

func decodeKeepalive(decoder *wire.Decoder, ack bool) (any, error) {
	value := Timestamp{Seconds: decoder.Uint32(), Nanoseconds: decoder.Uint32()}
	if err := decoder.Finish(); err != nil {
		return nil, err
	}
	if value.Nanoseconds >= 1_000_000_000 {
		return nil, fmt.Errorf("%w: nanoseconds %d", wire.ErrMalformed, value.Nanoseconds)
	}
	if ack {
		return Keepalive2Ack{Timestamp: value}, nil
	}
	return Keepalive2{Timestamp: value}, nil
}

func checkedUint32Length(length int) (uint32, error) {
	if length < 0 || uint64(length) > math.MaxUint32 {
		return 0, wire.ErrLimitExceeded
	}
	return uint32(length), nil
}

func encodeUint32Slice(encoder *wire.Encoder, values []uint32) error {
	length, err := checkedUint32Length(len(values))
	if err != nil {
		return err
	}
	encoder.Uint32(length)
	for _, value := range values {
		encoder.Uint32(value)
	}
	return nil
}

func decodeUint32Slice(decoder *wire.Decoder, maxBytes uint32) ([]uint32, error) {
	count := decoder.Uint32()
	if uint64(count)*4 > decoder.Remaining() {
		return nil, wire.ErrMalformed
	}
	if uint64(count) > uint64(maxBytes/4) {
		return nil, wire.ErrLimitExceeded
	}
	values := make([]uint32, count)
	for index := range values {
		values[index] = decoder.Uint32()
	}
	return values, nil
}

func decodeAuthBytes(decoder *wire.Decoder, limits Limits) ([]byte, error) {
	length := decoder.Uint32()
	if length > limits.MaxAuthBytes {
		return nil, wire.ErrLimitExceeded
	}
	if uint64(length) > decoder.Remaining() {
		return nil, wire.ErrMalformed
	}
	return decoder.Raw(length), nil
}

func decodeAuthRequest(decoder *wire.Decoder, limits Limits) (AuthRequest, error) {
	payload := AuthRequest{Method: decoder.Uint32()}
	modes, err := decodeUint32Slice(decoder, limits.MaxAuthBytes)
	if err != nil {
		return AuthRequest{}, err
	}
	payload.PreferredModes = modes
	payload.AuthPayload, err = decodeAuthBytes(decoder, limits)
	if err != nil {
		return AuthRequest{}, err
	}
	return payload, nil
}

func decodeAuthBadMethod(decoder *wire.Decoder, limits Limits) (AuthBadMethod, error) {
	payload := AuthBadMethod{Method: decoder.Uint32(), Result: decoder.Int32()}
	methods, err := decodeUint32Slice(decoder, limits.MaxAuthBytes)
	if err != nil {
		return AuthBadMethod{}, err
	}
	modes, err := decodeUint32Slice(decoder, limits.MaxAuthBytes)
	if err != nil {
		return AuthBadMethod{}, err
	}
	payload.AllowedMethods = methods
	payload.AllowedModes = modes
	return payload, nil
}

func decodeAuthReplyMore(decoder *wire.Decoder, limits Limits) (AuthReplyMore, error) {
	payload, err := decodeAuthBytes(decoder, limits)
	if err != nil {
		return AuthReplyMore{}, err
	}
	return AuthReplyMore{AuthPayload: payload}, nil
}

func decodeAuthRequestMore(decoder *wire.Decoder, limits Limits) (AuthRequestMore, error) {
	payload, err := decodeAuthBytes(decoder, limits)
	if err != nil {
		return AuthRequestMore{}, err
	}
	return AuthRequestMore{AuthPayload: payload}, nil
}

func decodeAuthDone(decoder *wire.Decoder, limits Limits) (AuthDone, error) {
	globalID := decoder.Uint64()
	connectionMode := decoder.Uint32()
	payload, err := decodeAuthBytes(decoder, limits)
	if err != nil {
		return AuthDone{}, err
	}
	return AuthDone{GlobalID: globalID, ConnectionMode: connectionMode, AuthPayload: payload}, nil
}

func decodeAuthSignature(decoder *wire.Decoder) (AuthSignature, error) {
	raw := decoder.Raw(32)
	if len(raw) != 32 {
		return AuthSignature{}, wire.ErrMalformed
	}
	var signature [32]byte
	copy(signature[:], raw)
	return AuthSignature{Signature: signature}, nil
}
