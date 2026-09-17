package mon

import (
	"bytes"
	"errors"
	"testing"

	wire "github.com/otuschhoff/rados-go/internal/encoding"
	"github.com/otuschhoff/rados-go/internal/maps"
	"github.com/otuschhoff/rados-go/internal/msgr"
	"github.com/otuschhoff/rados-go/internal/protocol"
)

func TestEncodeSubscribeV3ExactBytes(t *testing.T) {
	message, err := EncodeSubscribe(map[string]Subscription{
		"osdmap": {Start: 8},
		"monmap": {Start: 3, Flags: SubscribeOnce},
	}, "client-host", 1024)
	if err != nil {
		t.Fatal(err)
	}
	encoder := wire.NewEncoder(1024)
	encoder.Uint32(2)
	encoder.String("monmap")
	encoder.Uint64(3)
	encoder.Uint8(SubscribeOnce)
	encoder.String("osdmap")
	encoder.Uint64(8)
	encoder.Uint8(0)
	encoder.String("client-host")
	want, _ := encoder.BytesResult()
	if message.Header.Type != protocol.MessageMonSubscribe || message.Header.Version != 3 || message.Header.CompatVersion != 1 || !bytes.Equal(message.Front, want) {
		t.Fatalf("subscription message = %+v front=%x, want=%x", message.Header, message.Front, want)
	}
}

func TestDecodeSubscribeAck(t *testing.T) {
	encoder := wire.NewEncoder(64)
	encoder.Uint32(30)
	encoder.Raw([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})
	front, _ := encoder.BytesResult()
	ack, err := DecodeSubscribeAck(frontMessage(protocol.MessageMonSubscribeAck, 0, 0, front), 64)
	if err != nil {
		t.Fatal(err)
	}
	if ack.IntervalSeconds != 30 || ack.FSID[15] != 15 {
		t.Fatalf("ack = %+v", ack)
	}
}

func TestMonitorCommandCodecs(t *testing.T) {
	fsid := maps.FSID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	command := []string{`{"prefix":"status","format":"json"}`}
	request, err := EncodeCommand(fsid, command, []byte("input"), 1024)
	if err != nil {
		t.Fatal(err)
	}
	decoder := wire.NewDecoder(request.Front, wire.Limits{MaxBytes: 1024})
	if decoder.Uint64() != 0 || decoder.Int16() != -1 || decoder.Uint64() != 0 || !bytes.Equal(decoder.Raw(16), fsid[:]) || decoder.Uint32() != 1 || decoder.String() != command[0] || decoder.Remaining() != 0 {
		t.Fatalf("command front = %x", request.Front)
	}
	if request.Header.Type != protocol.MessageMonCommand || request.Header.Version != 1 || request.Header.CompatVersion != 0 || string(request.Data) != "input" {
		t.Fatalf("command message = %+v", request)
	}

	front := wire.NewEncoder(1024)
	encodePaxosHeader(front, 12)
	front.Int32(-2)
	front.String("not found")
	front.Uint32(1)
	front.String(command[0])
	encoded, _ := front.BytesResult()
	replyMessage := frontMessage(protocol.MessageMonCommandAck, 1, 0, encoded)
	replyMessage.Data = []byte("details")
	replyMessage.Lengths.Data = uint32(len(replyMessage.Data))
	reply, err := DecodeCommandReply(replyMessage, 1024, 4)
	if err != nil {
		t.Fatal(err)
	}
	if reply.Version != 12 || reply.Result != -2 || reply.Status != "not found" || string(reply.Data) != "details" || len(reply.Command) != 1 {
		t.Fatalf("reply = %+v", reply)
	}
	replyMessage.Data[0] = 'X'
	if string(reply.Data) != "details" {
		t.Fatal("reply data aliases message storage")
	}
}

func TestPoolOperationCodecs(t *testing.T) {
	fsid := maps.FSID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	request, err := EncodePoolOperation(fsid, 17, PoolOperationDeleteSelfManaged, 29, "", 23, 1024)
	if err != nil {
		t.Fatal(err)
	}
	decoder := wire.NewDecoder(request.Front, wire.Limits{MaxBytes: 1024})
	if decoder.Uint64() != 23 || decoder.Int16() != -1 || decoder.Uint64() != 0 || !bytes.Equal(decoder.Raw(16), fsid[:]) || decoder.Uint32() != 17 || decoder.Uint32() != uint32(PoolOperationDeleteSelfManaged) || decoder.Uint64() != 0 || decoder.Uint64() != 29 || decoder.String() != "" || decoder.Uint8() != 0 || decoder.Int16() != 0 || decoder.Remaining() != 0 {
		t.Fatalf("pool operation front = %x", request.Front)
	}
	if request.Header.Type != protocol.MessagePoolOp || request.Header.Version != 4 || request.Header.CompatVersion != 2 {
		t.Fatalf("pool operation message = %+v", request.Header)
	}

	replyMessage := encodePoolOperationReply(t, fsid, -13, 31, []byte{1, 2, 3})
	reply, err := DecodePoolOperationReply(replyMessage, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if reply.Version != 31 || reply.FSID != fsid || reply.Result != -13 || reply.Epoch != 31 || !bytes.Equal(reply.ResponseData, []byte{1, 2, 3}) {
		t.Fatalf("pool operation reply = %+v", reply)
	}
	replyMessage.Front[len(replyMessage.Front)-1] = 9
	if !bytes.Equal(reply.ResponseData, []byte{1, 2, 3}) {
		t.Fatal("pool operation reply data aliases message storage")
	}
}

func TestPoolOperationCodecsRejectInvalidInput(t *testing.T) {
	fsid := maps.FSID{}
	requests := []struct {
		operation PoolOperation
		snapID    uint64
		name      string
	}{
		{PoolOperationCreateSnapshot, 0, ""},
		{PoolOperationDeleteSnapshot, 1, "named"},
		{PoolOperationCreateSelfManaged, 0, "named"},
		{PoolOperationDeleteSelfManaged, 0, ""},
		{PoolOperation(0xff), 0, ""},
	}
	for _, test := range requests {
		if _, err := EncodePoolOperation(fsid, 1, test.operation, test.snapID, test.name, 0, 1024); !errors.Is(err, wire.ErrMalformed) {
			t.Fatalf("operation=%d snap=%d name=%q error=%v", test.operation, test.snapID, test.name, err)
		}
	}

	message := encodePoolOperationReply(t, fsid, 0, 1, nil)
	message.Front[len(message.Front)-1] = 2
	if _, err := DecodePoolOperationReply(message, 1024); !errors.Is(err, wire.ErrMalformed) {
		t.Fatalf("noncanonical response flag error = %v", err)
	}
	message = encodePoolOperationReply(t, fsid, 0, 1, []byte{1})
	message.Front = message.Front[:len(message.Front)-1]
	message.Lengths.Front--
	if _, err := DecodePoolOperationReply(message, 1024); !errors.Is(err, wire.ErrMalformed) {
		t.Fatalf("truncated response error = %v", err)
	}
	message = encodePoolOperationReply(t, fsid, 0, 1, nil)
	message.Header.CompatVersion = 2
	if _, err := DecodePoolOperationReply(message, 1024); !errors.Is(err, wire.ErrUnsupportedVersion) {
		t.Fatalf("unsupported response compatibility error = %v", err)
	}
}

func TestDecodeAllocatedSnapshotID(t *testing.T) {
	encoder := wire.NewEncoder(8)
	encoder.Uint64(0x0102030405060708)
	data, _ := encoder.BytesResult()
	snapshotID, err := DecodeAllocatedSnapshotID(data, 8)
	if err != nil || snapshotID != 0x0102030405060708 {
		t.Fatalf("snapshot id=%#x error=%v", snapshotID, err)
	}
	for _, invalid := range [][]byte{nil, make([]byte, 8), append(data, 0)} {
		if _, err := DecodeAllocatedSnapshotID(invalid, 9); !errors.Is(err, wire.ErrMalformed) {
			t.Fatalf("data=%x error=%v", invalid, err)
		}
	}
}

func encodePoolOperationReply(t *testing.T, fsid maps.FSID, result int32, epoch uint32, response []byte) msgr.Message {
	t.Helper()
	encoder := wire.NewEncoder(1024)
	encodePaxosHeader(encoder, uint64(epoch))
	encoder.Raw(fsid[:])
	encoder.Int32(result)
	encoder.Uint32(epoch)
	encoder.Bool(len(response) != 0)
	if len(response) != 0 {
		encoder.Bytes(response)
	}
	front, err := encoder.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	return frontMessage(protocol.MessagePoolOpReply, 1, 1, front)
}

func TestDecodeMonMapMessage(t *testing.T) {
	encoded := encodeMinimalMonMap(t)
	outer := wire.NewEncoder(1024)
	outer.Bytes(encoded)
	front, _ := outer.BytesResult()
	message := frontMessage(protocol.MessageMonMap, 0, 0, front)
	monMap, err := DecodeMonMap(message, maps.Limits{MaxBytes: 1024, MaxMonitors: 4, MaxAddresses: 4, MaxLocations: 4})
	if err != nil {
		t.Fatal(err)
	}
	if monMap.Epoch() != 7 || monMap.MonitorCount() != 0 {
		t.Fatalf("monmap epoch=%d monitors=%d", monMap.Epoch(), monMap.MonitorCount())
	}
}

func TestDecodeOSDMapBatchV4(t *testing.T) {
	message := encodeOSDMapBatchMessage(t, 4, 3, []mapBlob{{2, []byte("inc")}}, []mapBlob{{1, []byte("full")}}, 1, 2, 0)
	batch, err := DecodeOSDMapBatch(message, MessageLimits{MaxBytes: 1024, MaxMaps: 4})
	if err != nil {
		t.Fatal(err)
	}
	if batch.FSID[0] != 1 || string(batch.Incrementals[2]) != "inc" || string(batch.FullMaps[1]) != "full" || batch.TrimLowerBound != 1 || batch.NewestMap != 2 {
		t.Fatalf("batch = %+v", batch)
	}
	message.Front[24] = 'X'
	if string(batch.Incrementals[2]) != "inc" {
		t.Fatal("decoded map blob aliases message storage")
	}
}

func TestDecodeOSDMapBatchRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name    string
		message msgr.Message
		limits  MessageLimits
		want    error
	}{
		{name: "unsupported compat", message: encodeOSDMapBatchMessage(t, 4, 5, nil, nil, 0, 0, 0), limits: MessageLimits{MaxBytes: 1024, MaxMaps: 4}, want: wire.ErrUnsupportedVersion},
		{name: "duplicate epoch", message: encodeOSDMapBatchMessage(t, 4, 3, []mapBlob{{1, []byte("a")}, {1, []byte("b")}}, nil, 0, 1, 0), limits: MessageLimits{MaxBytes: 1024, MaxMaps: 4}, want: maps.ErrMalformedMap},
		{name: "map limit", message: encodeOSDMapBatchMessage(t, 4, 3, []mapBlob{{1, nil}, {2, nil}}, nil, 0, 2, 0), limits: MessageLimits{MaxBytes: 1024, MaxMaps: 1}, want: wire.ErrLimitExceeded},
		{name: "retired field", message: encodeOSDMapBatchMessage(t, 4, 3, nil, nil, 0, 0, 1), limits: MessageLimits{MaxBytes: 1024, MaxMaps: 4}, want: wire.ErrUnsupportedVersion},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := DecodeOSDMapBatch(test.message, test.limits); !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
}

type mapBlob struct {
	epoch uint32
	data  []byte
}

func encodeOSDMapBatchMessage(t *testing.T, version, compat uint16, incrementals, fullMaps []mapBlob, trim, newest, gapCount uint32) msgr.Message {
	t.Helper()
	encoder := wire.NewEncoder(1024)
	encoder.Raw([]byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1})
	encodeMapBlobs(encoder, incrementals)
	encodeMapBlobs(encoder, fullMaps)
	encoder.Uint32(trim)
	encoder.Uint32(newest)
	if version >= 4 {
		encoder.Uint32(gapCount)
	}
	front, err := encoder.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	return frontMessage(protocol.MessageOSDMap, version, compat, front)
}

func encodeMapBlobs(encoder *wire.Encoder, values []mapBlob) {
	encoder.Uint32(uint32(len(values)))
	for _, value := range values {
		encoder.Uint32(value.epoch)
		encoder.Bytes(value.data)
	}
}

func encodeMinimalMonMap(t *testing.T) []byte {
	t.Helper()
	encoder := wire.NewEncoder(1024)
	encoder.Versioned(9, 6, func(payload *wire.Encoder) {
		payload.Raw(make([]byte, 16))
		payload.Uint32(7)
		payload.Raw(make([]byte, 16))
		for range 2 {
			payload.Versioned(1, 1, func(features *wire.Encoder) { features.Uint64(0) })
		}
		payload.Uint32(0)
		payload.Uint32(0)
		payload.Uint8(20)
		payload.Uint32(0)
		payload.Uint8(1)
		payload.Uint32(0)
		payload.Bool(false)
		payload.String("")
		payload.Uint32(0)
	})
	data, err := encoder.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	return data
}
