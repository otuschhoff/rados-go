package osd

import (
	"errors"
	"testing"

	wire "github.com/otuschhoff/go-librados/internal/encoding"
	"github.com/otuschhoff/go-librados/internal/maps"
	"github.com/otuschhoff/go-librados/internal/msgr"
	"github.com/otuschhoff/go-librados/internal/protocol"
)

func TestDecodeBackoffAndEncodeAcknowledgment(t *testing.T) {
	want := Backoff{
		PG: maps.PG{Pool: 7, Seed: 3, Preferred: -1}, Shard: -1, MapEpoch: 9,
		Operation: BackoffBlock, ID: 42,
		Begin: HObject{Object: "a", Snapshot: NoSnap, Hash: 0x80000000, Namespace: "space", Pool: 7},
		End:   HObject{Object: "z", Snapshot: NoSnap, Hash: 0x80000000, Namespace: "space", Pool: 7},
	}
	message := encodeBackoffForTest(t, want)
	got, err := DecodeBackoff(message, testLimits)
	if err != nil || got != want {
		t.Fatalf("backoff=%+v err=%v", got, err)
	}
	ack, err := EncodeBackoffAcknowledgment(got, testLimits)
	if err != nil {
		t.Fatal(err)
	}
	if ack.Header.Type != protocol.MessageOSDBackoff || ack.Header.Version != 1 || ack.Header.CompatVersion != 1 {
		t.Fatalf("header=%+v", ack.Header)
	}
	gotAck, err := decodeBackoffForTest(ack)
	if err != nil || gotAck.Operation != BackoffAckBlock || gotAck.ID != want.ID || gotAck.Begin != want.Begin || gotAck.End != want.End {
		t.Fatalf("ack=%+v err=%v", gotAck, err)
	}
}

func TestBackoffContainsSingleAndRange(t *testing.T) {
	object := HObject{Object: "object", Snapshot: NoSnap, Hash: 1, Pool: 7}
	single := Backoff{Begin: object, End: object}
	if !single.Contains(object) || single.Contains(HObject{Object: "other", Snapshot: NoSnap, Hash: 1, Pool: 7}) {
		t.Fatal("single-object backoff mismatch")
	}
	begin := HObject{Object: "a", Snapshot: NoSnap, Hash: 1, Pool: 7}
	middle := HObject{Object: "m", Snapshot: NoSnap, Hash: 1, Pool: 7}
	end := HObject{Object: "z", Snapshot: NoSnap, Hash: 1, Pool: 7}
	ranged := Backoff{Begin: begin, End: end}
	if !ranged.Contains(begin) || !ranged.Contains(middle) || ranged.Contains(end) {
		t.Fatal("half-open range mismatch")
	}
	if compareHObject(HObject{Hash: 1}, HObject{Hash: 2}) <= 0 {
		t.Fatal("hashes must use bit-reversed ordering")
	}
}

func TestDecodeBackoffRejectsMalformedMessages(t *testing.T) {
	valid := Backoff{PG: maps.PG{Pool: 7, Seed: 3, Preferred: -1}, Shard: -1, Operation: BackoffBlock}
	message := encodeBackoffForTest(t, valid)
	tests := []msgr.Message{
		{Header: msgr.MessageHeader{Type: protocol.MessageOSDOp}, Front: message.Front},
		{Header: message.Header, Front: message.Front[:len(message.Front)-1]},
		{Header: message.Header, Front: message.Front, Data: []byte{1}},
	}
	for _, test := range tests {
		if _, err := DecodeBackoff(test, testLimits); err == nil {
			t.Fatalf("accepted malformed message %+v", test.Header)
		}
	}
	if _, err := DecodeBackoff(message, Limits{MaxBytes: 1}); !errors.Is(err, ErrMalformedReply) {
		t.Fatalf("limit error=%v", err)
	}
}

func FuzzDecodeBackoff(f *testing.F) {
	seed := Backoff{PG: maps.PG{Pool: 7, Seed: 3, Preferred: -1}, Shard: -1, Operation: BackoffBlock}
	f.Add(encodeBackoffForTest(f, seed).Front)
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, front []byte) {
		if len(front) > int(testLimits.MaxBytes) {
			t.Skip()
		}
		_, _ = DecodeBackoff(msgr.Message{Header: msgr.MessageHeader{Type: protocol.MessageOSDBackoff, Version: 1, CompatVersion: 1}, Front: front}, testLimits)
	})
}

func encodeBackoffForTest(t testing.TB, backoff Backoff) msgr.Message {
	t.Helper()
	encoder := wire.NewEncoder(testLimits.MaxBytes)
	encodeSPGWithShard(encoder, backoff.PG, backoff.Shard)
	encoder.Uint32(backoff.MapEpoch)
	encoder.Uint8(backoff.Operation)
	encoder.Uint64(backoff.ID)
	encodeHObject(encoder, backoff.Begin)
	encodeHObject(encoder, backoff.End)
	front, err := encoder.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	return msgr.Message{Header: msgr.MessageHeader{Type: protocol.MessageOSDBackoff, Version: 1, CompatVersion: 1}, Front: front}
}

func decodeBackoffForTest(message msgr.Message) (Backoff, error) {
	decoder := wire.NewDecoder(message.Front, wire.Limits{MaxBytes: testLimits.MaxBytes})
	pg, shard, err := decodeSPG(decoder)
	if err != nil {
		return Backoff{}, err
	}
	backoff := Backoff{PG: pg, Shard: shard, MapEpoch: decoder.Uint32(), Operation: decoder.Uint8(), ID: decoder.Uint64()}
	backoff.Begin, err = decodeHObject(decoder)
	if err != nil {
		return Backoff{}, err
	}
	backoff.End, err = decodeHObject(decoder)
	return backoff, err
}
