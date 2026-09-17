package osd

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	wire "github.com/otuschhoff/rados-go/internal/encoding"
	"github.com/otuschhoff/rados-go/internal/maps"
	"github.com/otuschhoff/rados-go/internal/msgr"
	"github.com/otuschhoff/rados-go/internal/protocol"
)

func TestEncodeCommandRequestExactBytes(t *testing.T) {
	fsid := maps.FSID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	request := CommandRequest{FSID: fsid, TransactionID: 0x0102030405060708, Command: []string{"ping", "osd.5"}, Input: []byte("data")}
	message, err := EncodeCommandRequest(request, 128)
	if err != nil {
		t.Fatal(err)
	}
	if message.Header.Type != protocol.MessageCommand || message.Header.Version != 1 || message.Header.CompatVersion != 0 || message.Header.TransactionID != request.TransactionID {
		t.Fatalf("header=%+v", message.Header)
	}
	expected := bytes.Buffer{}
	expected.Write(fsid[:])
	binary.Write(&expected, binary.LittleEndian, uint32(2))
	binary.Write(&expected, binary.LittleEndian, uint32(4))
	expected.WriteString("ping")
	binary.Write(&expected, binary.LittleEndian, uint32(5))
	expected.WriteString("osd.5")
	if !bytes.Equal(message.Front, expected.Bytes()) {
		t.Fatalf("front=%x want %x", message.Front, expected.Bytes())
	}
	if !bytes.Equal(message.Data, []byte("data")) {
		t.Fatalf("data=%x", message.Data)
	}
	if message.Lengths.Front != uint32(len(message.Front)) || message.Lengths.Data != 4 || message.Lengths.Middle != 0 {
		t.Fatalf("lengths=%+v", message.Lengths)
	}
}

func TestEncodeCommandRequestRejectsEmpty(t *testing.T) {
	if _, err := EncodeCommandRequest(CommandRequest{Command: nil}, 16); !errors.Is(err, wire.ErrLimitExceeded) {
		t.Fatalf("nil command err=%v", err)
	}
	if _, err := EncodeCommandRequest(CommandRequest{Command: []string{"x"}, Input: make([]byte, 32)}, 16); !errors.Is(err, wire.ErrLimitExceeded) {
		t.Fatalf("oversized input err=%v", err)
	}
	if _, err := EncodeCommandRequest(CommandRequest{Command: []string{"x"}}, 0); !errors.Is(err, wire.ErrLimitExceeded) {
		t.Fatalf("zero maxBytes err=%v", err)
	}
	// Front alone that would exceed maxBytes must be refused.
	if _, err := EncodeCommandRequest(CommandRequest{Command: []string{"01234567890123456789"}}, 20); !errors.Is(err, wire.ErrLimitExceeded) {
		t.Fatalf("large argv err=%v", err)
	}
}

func TestDecodeCommandReplyExactBytes(t *testing.T) {
	front := bytes.Buffer{}
	binary.Write(&front, binary.LittleEndian, int32(-13))
	binary.Write(&front, binary.LittleEndian, uint32(6))
	front.WriteString("denied")
	message := msgr.Message{
		Header:  msgr.MessageHeader{Type: protocol.MessageCommandReply, Version: 1, CompatVersion: 1, TransactionID: 42},
		Front:   front.Bytes(),
		Data:    []byte("output"),
		Lengths: msgr.MessageLengths{Front: uint32(front.Len()), Data: 6},
	}
	reply, err := DecodeCommandReply(message, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if reply.TransactionID != 42 || reply.Result != -13 || reply.Status != "denied" || string(reply.Output) != "output" {
		t.Fatalf("reply=%+v", reply)
	}
	// Ownership: mutating original message must not affect returned copies.
	message.Front[0] = 0xff
	message.Data[0] = 'X'
	if reply.Result != -13 || string(reply.Output) != "output" {
		t.Fatalf("post-mutation reply=%+v", reply)
	}
}

func TestDecodeCommandReplyRejectsMalformed(t *testing.T) {
	good := func() msgr.Message {
		front := bytes.Buffer{}
		binary.Write(&front, binary.LittleEndian, int32(0))
		binary.Write(&front, binary.LittleEndian, uint32(2))
		front.WriteString("ok")
		return msgr.Message{
			Header:  msgr.MessageHeader{Type: protocol.MessageCommandReply, Version: 1, CompatVersion: 1},
			Front:   front.Bytes(),
			Lengths: msgr.MessageLengths{Front: uint32(front.Len())},
		}
	}
	cases := []struct {
		name   string
		mutate func(*msgr.Message)
		want   error
	}{
		{"wrong type", func(m *msgr.Message) { m.Header.Type = protocol.MessageCommand }, ErrMalformedCommandReply},
		{"compat too high", func(m *msgr.Message) { m.Header.CompatVersion = 2 }, wire.ErrUnsupportedVersion},
		{"unexpected middle", func(m *msgr.Message) { m.Middle = []byte{1} }, ErrMalformedCommandReply},
		{"length mismatch", func(m *msgr.Message) { m.Lengths.Front++ }, ErrMalformedCommandReply},
		{"trailing bytes", func(m *msgr.Message) { m.Front = append(m.Front, 0); m.Lengths.Front++ }, ErrMalformedCommandReply},
		{"truncated status", func(m *msgr.Message) { m.Front = m.Front[:len(m.Front)-1]; m.Lengths.Front-- }, ErrMalformedCommandReply},
		{"maxBytes zero", func(m *msgr.Message) {}, wire.ErrLimitExceeded},
		{"payload over limit", func(m *msgr.Message) { m.Data = make([]byte, 5000); m.Lengths.Data = 5000 }, wire.ErrLimitExceeded},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			message := good()
			tc.mutate(&message)
			maxBytes := uint32(4096)
			if tc.name == "maxBytes zero" {
				maxBytes = 0
			}
			if _, err := DecodeCommandReply(message, maxBytes); !errors.Is(err, tc.want) {
				t.Fatalf("err=%v want %v", err, tc.want)
			}
		})
	}
}

func TestDecodeCommandReplyPreservesEmptyOutput(t *testing.T) {
	front := bytes.Buffer{}
	binary.Write(&front, binary.LittleEndian, int32(0))
	binary.Write(&front, binary.LittleEndian, uint32(0))
	message := msgr.Message{
		Header:  msgr.MessageHeader{Type: protocol.MessageCommandReply, Version: 1, CompatVersion: 1},
		Front:   front.Bytes(),
		Lengths: msgr.MessageLengths{Front: uint32(front.Len())},
	}
	reply, err := DecodeCommandReply(message, 1024)
	if err != nil || reply.Status != "" || len(reply.Output) != 0 {
		t.Fatalf("reply=%+v err=%v", reply, err)
	}
}

func TestParsePG(t *testing.T) {
	pg, err := ParsePG("7.1a")
	if err != nil || pg.Pool != 7 || pg.Seed != 0x1a || pg.Preferred != -1 {
		t.Fatalf("pg=%+v err=%v", pg, err)
	}
	pg, err = ParsePG("0.0")
	if err != nil || pg.Pool != 0 || pg.Seed != 0 || pg.Preferred != -1 {
		t.Fatalf("pg=%+v err=%v", pg, err)
	}
	for _, bad := range []string{"", "7", "7.", ".1", "-1.1", "7.g", "7.1s0", "7.1p0", "7.1_head", "abc.1"} {
		if _, err := ParsePG(bad); !errors.Is(err, wire.ErrMalformed) {
			t.Fatalf("input=%q err=%v", bad, err)
		}
	}
}
