package osd

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	wire "github.com/otuschhoff/go-librados/internal/encoding"
	"github.com/otuschhoff/go-librados/internal/maps"
	"github.com/otuschhoff/go-librados/internal/msgr"
	"github.com/otuschhoff/go-librados/internal/protocol"
)

var ErrMalformedCommandReply = errors.New("malformed OSD command reply")

// CommandRequest is the client-side input to a generic OSD/PG command as
// carried by MCommand: a Ceph cluster fsid, a non-empty argv, and an optional
// data blob attached as the message data segment.
type CommandRequest struct {
	FSID          maps.FSID
	TransactionID uint64
	Command       []string
	Input         []byte
}

// CommandReply is the decoded MCommandReply: the server's transaction id, the
// signed Linux errno the server returned, the human-readable status line, and
// the attached output blob as the data segment.
type CommandReply struct {
	TransactionID uint64
	Result        int32
	Status        string
	Output        []byte
}

// EncodeCommandRequest emits the exact MCommand wire form used by
// src/messages/MCommand.h at 7f793731. Payload = uuid_d (16 raw bytes) followed
// by std::vector<std::string> (uint32 count + N * uint32-prefixed strings). The
// caller-supplied Input becomes the message data segment.
func EncodeCommandRequest(request CommandRequest, maxBytes uint32) (msgr.Message, error) {
	if maxBytes == 0 || len(request.Command) == 0 || uint64(len(request.Command)) > uint64(^uint32(0)) || uint64(len(request.Input)) > uint64(maxBytes) {
		return msgr.Message{}, wire.ErrLimitExceeded
	}
	for _, argument := range request.Command {
		if uint64(len(argument)) > uint64(^uint32(0)) {
			return msgr.Message{}, wire.ErrLimitExceeded
		}
	}
	encoder := wire.NewEncoder(maxBytes)
	encoder.Raw(request.FSID[:])
	encoder.Uint32(uint32(len(request.Command)))
	for _, argument := range request.Command {
		encoder.String(argument)
	}
	front, err := encoder.BytesResult()
	if err != nil {
		return msgr.Message{}, err
	}
	if uint64(len(front))+uint64(len(request.Input)) > uint64(maxBytes) {
		return msgr.Message{}, wire.ErrLimitExceeded
	}
	return msgr.Message{
		Header: msgr.MessageHeader{
			TransactionID: request.TransactionID,
			Type:          protocol.MessageCommand,
			Version:       1,
			CompatVersion: 0,
		},
		Front:   front,
		Data:    append([]byte(nil), request.Input...),
		Lengths: msgr.MessageLengths{Front: uint32(len(front)), Data: uint32(len(request.Input))},
	}, nil
}

// DecodeCommandReply parses the exact MCommandReply wire form used by
// src/messages/MCommandReply.h at 7f793731. Payload = errorcode32_t (int32
// Linux errno as encoded on the wire) followed by a uint32-prefixed string.
// The attached data segment carries the command output. Malformed or oversized
// inputs are rejected without retaining caller-owned buffers.
func DecodeCommandReply(message msgr.Message, maxBytes uint32) (CommandReply, error) {
	if maxBytes == 0 {
		return CommandReply{}, wire.ErrLimitExceeded
	}
	if message.Header.Type != protocol.MessageCommandReply {
		return CommandReply{}, fmt.Errorf("%w: header type %d", ErrMalformedCommandReply, message.Header.Type)
	}
	if message.Header.Version < 1 || message.Header.CompatVersion > 1 {
		return CommandReply{}, fmt.Errorf("%w: version %d compat %d", wire.ErrUnsupportedVersion, message.Header.Version, message.Header.CompatVersion)
	}
	if len(message.Middle) != 0 {
		return CommandReply{}, fmt.Errorf("%w: unexpected middle segment", ErrMalformedCommandReply)
	}
	if uint64(len(message.Front))+uint64(len(message.Data)) > uint64(maxBytes) {
		return CommandReply{}, wire.ErrLimitExceeded
	}
	expected := msgr.MessageLengths{Front: uint32(len(message.Front)), Data: uint32(len(message.Data))}
	if message.Lengths != expected {
		return CommandReply{}, fmt.Errorf("%w: segment length mismatch", ErrMalformedCommandReply)
	}
	decoder := wire.NewDecoder(message.Front, wire.Limits{MaxBytes: maxBytes})
	reply := CommandReply{TransactionID: message.Header.TransactionID}
	reply.Result = decoder.Int32()
	reply.Status = decoder.String()
	if err := decoder.Finish(); err != nil {
		return CommandReply{}, fmt.Errorf("%w: %v", ErrMalformedCommandReply, err)
	}
	if decoder.Remaining() != 0 {
		return CommandReply{}, fmt.Errorf("%w: %d trailing front bytes", ErrMalformedCommandReply, decoder.Remaining())
	}
	reply.Output = append([]byte(nil), message.Data...)
	return reply, nil
}

// ParsePG parses the canonical Ceph "poolID.seed" PG string (decimal pool,
// hexadecimal seed) that pg_command clients pass on the wire. Shard/parent
// suffixes are rejected: callers targeting sharded operations must supply the
// shard explicitly.
func ParsePG(text string) (maps.PG, error) {
	dot := strings.IndexByte(text, '.')
	if dot <= 0 || dot >= len(text)-1 {
		return maps.PG{}, fmt.Errorf("%w: malformed PG %q", wire.ErrMalformed, text)
	}
	poolText := text[:dot]
	seedText := text[dot+1:]
	if strings.ContainsAny(seedText, "spSP_") {
		return maps.PG{}, fmt.Errorf("%w: PG %q carries an unsupported suffix", wire.ErrMalformed, text)
	}
	pool, err := strconv.ParseInt(poolText, 10, 64)
	if err != nil || pool < 0 {
		return maps.PG{}, fmt.Errorf("%w: PG pool in %q", wire.ErrMalformed, text)
	}
	seed, err := strconv.ParseUint(seedText, 16, 32)
	if err != nil {
		return maps.PG{}, fmt.Errorf("%w: PG seed in %q", wire.ErrMalformed, text)
	}
	return maps.PG{Pool: uint64(pool), Seed: uint32(seed), Preferred: -1}, nil
}
