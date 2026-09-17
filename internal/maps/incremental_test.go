package maps

import (
	"encoding/binary"
	"errors"
	"reflect"
	"testing"

	wire "github.com/otuschhoff/rados-go/internal/encoding"
	"github.com/otuschhoff/rados-go/internal/protocol"
)

func TestDecodeAndApplyOSDMapIncremental(t *testing.T) {
	base, err := DecodeOSDMap(encodeTestOSDMapNamed(t, 11, "data"), testOSDMapLimits)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := DecodeOSDMap(encodeTestOSDMapNamed(t, 12, "archive"), testOSDMapLimits)
	if err != nil {
		t.Fatal(err)
	}
	encoded := encodeTestIncremental(t, 12, map[int64]string{7: "archive"}, nil, expected.CRC())
	incremental, err := DecodeOSDMapIncremental(encoded, testOSDMapLimits)
	if err != nil {
		t.Fatal(err)
	}
	next, err := ApplyOSDMapIncremental(base, incremental, testOSDMapLimits)
	if err != nil {
		t.Fatal(err)
	}
	pool, ok := next.PoolByName("archive")
	if !ok || pool.ID() != 7 || next.Epoch() != expected.Epoch() || next.CRC() != expected.CRC() {
		t.Fatalf("next epoch=%d crc=%x pool=%+v found=%t", next.Epoch(), next.CRC(), pool, ok)
	}
	if _, ok := next.PoolByName("data"); ok {
		t.Fatal("old pool name survived rename")
	}
	if _, ok := base.PoolByName("data"); !ok {
		t.Fatal("incremental mutated published base map")
	}
	if next.CRCVerified() {
		t.Fatal("applied incremental reported an unverified full-map crc as verified")
	}
}

func TestApplyOSDMapIncrementalConvergesCompleteClientState(t *testing.T) {
	keptPG := PG{Pool: 7, Seed: 1, Preferred: -1}
	deletedPG := PG{Pool: 7, Seed: 2, Preferred: -1}
	address := protocol.EntityAddr{Type: protocol.AddressV2, Nonce: 4, Family: 2, SocketData: []byte{1, 2, 3}}
	base := &OSDMap{
		fsid: FSID{1}, epoch: 4, pools: map[int64]Pool{}, nameToID: map[string]int64{}, maxOSD: 2,
		osdState: []uint32{(1 << 0) | (1 << 2) | (1 << 3), 1 << 0}, osdWeight: []uint32{0, 1},
		clientAddresses: []protocol.EntityAddrVec{{address}, {address}}, primaryAffinity: []uint32{1, 2},
		pgTemp: map[PG][]int32{deletedPG: {1}}, primaryTemp: map[PG]int32{deletedPG: 1},
		crushData: []byte("old"), erasureCodeProfiles: map[string]map[string]string{"old": {"k": "v"}},
		pgUpmap: map[PG][]int32{deletedPG: {1}}, pgUpmapItems: map[PG][]OSDRemap{deletedPG: {{From: 1, To: 0}}},
		newRemovedSnapshots: map[int64][]Interval{1: {{Start: 1, Length: 1}}},
		newPurgedSnapshots:  map[int64][]Interval{1: {{Start: 2, Length: 1}}},
		pgUpmapPrimaries:    map[PG]int32{deletedPG: 1}, crcVerified: true,
	}
	incremental := &OSDMapIncremental{
		fsid: base.fsid, epoch: 5, modified: UTime{Seconds: 10}, newPoolMax: 9, newFlags: 6,
		crushData: []byte("new"), newMaxOSD: 3, newPools: map[int64]Pool{}, newPoolNames: map[int64]string{},
		newUpClient: map[int32]protocol.EntityAddrVec{2: {address}}, newState: map[int32]uint32{1: 1},
		newWeight: map[int32]uint32{0: 10}, newPGTemp: map[PG][]int32{keptPG: {2}, deletedPG: {}},
		newPrimaryTemp: map[PG]int32{keptPG: 2, deletedPG: -1}, newPrimaryAffinity: map[int32]uint32{0: 8},
		newErasureProfiles: map[string]map[string]string{"new": {"m": "2"}}, oldErasureProfiles: []string{"old"},
		newPGUpmap: map[PG][]int32{keptPG: {2}}, oldPGUpmap: []PG{deletedPG},
		newPGUpmapItems: map[PG][]OSDRemap{keptPG: {{From: 0, To: 2}}}, oldPGUpmapItems: []PG{deletedPG},
		newRemovedSnapshots: map[int64][]Interval{2: {{Start: 3, Length: 4}}},
		newPurgedSnapshots:  map[int64][]Interval{2: {{Start: 5, Length: 6}}},
		newLastUpChange:     UTime{Seconds: 11}, newLastInChange: UTime{Seconds: 12},
		newPGUpmapPrimaries: map[PG]int32{keptPG: 2}, oldPGUpmapPrimaries: []PG{deletedPG}, fullCRC: 99,
	}

	next, err := ApplyOSDMapIncremental(base, incremental, testOSDMapLimits)
	if err != nil {
		t.Fatal(err)
	}
	if next.epoch != 5 || next.modified.Seconds != 10 || next.poolMax != 9 || next.flags != 6 || next.maxOSD != 3 || next.crushVersion != 1 || string(next.crushData) != "new" || next.crc != 99 || next.crcVerified {
		t.Fatalf("incorrect scalar state: %+v", next)
	}
	if !reflect.DeepEqual(next.osdState, []uint32{1, 0, 3}) || !reflect.DeepEqual(next.osdWeight, []uint32{10, 1, 0}) || !reflect.DeepEqual(next.primaryAffinity, []uint32{8, defaultPrimaryAffinity, defaultPrimaryAffinity}) {
		t.Fatalf("incorrect osd state: state=%v weight=%v affinity=%v", next.osdState, next.osdWeight, next.primaryAffinity)
	}
	if len(next.clientAddresses[1]) != 0 || !reflect.DeepEqual(next.clientAddresses[2], protocol.EntityAddrVec{address}) {
		t.Fatalf("incorrect osd addresses: %v", next.clientAddresses)
	}
	if !reflect.DeepEqual(next.pgTemp, map[PG][]int32{keptPG: {2}}) || !reflect.DeepEqual(next.primaryTemp, map[PG]int32{keptPG: 2}) || !reflect.DeepEqual(next.pgUpmap, map[PG][]int32{keptPG: {2}}) || !reflect.DeepEqual(next.pgUpmapItems, map[PG][]OSDRemap{keptPG: {{From: 0, To: 2}}}) || !reflect.DeepEqual(next.pgUpmapPrimaries, map[PG]int32{keptPG: 2}) {
		t.Fatal("pg mapping mutations did not converge")
	}
	if !reflect.DeepEqual(next.erasureCodeProfiles, map[string]map[string]string{"new": {"m": "2"}}) || !reflect.DeepEqual(next.newRemovedSnapshots, incremental.newRemovedSnapshots) || !reflect.DeepEqual(next.newPurgedSnapshots, incremental.newPurgedSnapshots) || next.lastUpChange.Seconds != 11 || next.lastInChange.Seconds != 12 {
		t.Fatal("profile, snapshot, or timestamp mutations did not converge")
	}
	next.crushData[0] = 'X'
	next.clientAddresses[2][0].SocketData[0] = 9
	next.erasureCodeProfiles["new"]["m"] = "changed"
	if string(incremental.crushData) != "new" || incremental.newUpClient[2][0].SocketData[0] != 1 || incremental.newErasureProfiles["new"]["m"] != "2" || base.osdState[1] != 1 {
		t.Fatal("published snapshot aliases base or incremental storage")
	}
}

func TestApplyOSDMapIncrementalFullReplacement(t *testing.T) {
	base, _ := DecodeOSDMap(encodeTestOSDMapNamed(t, 11, "data"), testOSDMapLimits)
	replacementBytes := encodeTestOSDMapNamed(t, 12, "archive")
	replacement, _ := DecodeOSDMap(replacementBytes, testOSDMapLimits)
	encoded := encodeTestIncremental(t, 12, nil, replacementBytes, replacement.CRC())
	incremental, err := DecodeOSDMapIncremental(encoded, testOSDMapLimits)
	if err != nil {
		t.Fatal(err)
	}
	next, err := ApplyOSDMapIncremental(base, incremental, testOSDMapLimits)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := next.PoolByName("archive"); !ok || next.CRC() != replacement.CRC() {
		t.Fatalf("replacement = %+v", next)
	}
}

func TestOSDMapIncrementalRejectsCorruptionAndSequenceErrors(t *testing.T) {
	base, _ := DecodeOSDMap(encodeTestOSDMapNamed(t, 11, "data"), testOSDMapLimits)
	valid := encodeTestIncremental(t, 12, nil, nil, 123)
	corrupt := append([]byte(nil), valid...)
	corrupt[20] ^= 1
	if _, err := DecodeOSDMapIncremental(corrupt, testOSDMapLimits); !errors.Is(err, ErrMalformedMap) {
		t.Fatalf("crc error = %v", err)
	}
	incremental, err := DecodeOSDMapIncremental(valid, testOSDMapLimits)
	if err != nil {
		t.Fatal(err)
	}
	incremental.epoch = 13
	if _, err := ApplyOSDMapIncremental(base, incremental, testOSDMapLimits); !errors.Is(err, ErrMalformedMap) {
		t.Fatalf("gap error = %v", err)
	}
	incremental.epoch = 12
	incremental.fsid[0] ^= 1
	if _, err := ApplyOSDMapIncremental(base, incremental, testOSDMapLimits); !errors.Is(err, ErrMalformedMap) {
		t.Fatalf("fsid error = %v", err)
	}
}

func encodeTestIncremental(t *testing.T, epoch uint32, names map[int64]string, fullMap []byte, fullCRC uint32) []byte {
	t.Helper()
	encoder := wire.NewEncoder(testOSDMapLimits.MaxBytes)
	encoder.Versioned(8, 7, func(wrapper *wire.Encoder) {
		wrapper.Versioned(9, 1, func(client *wire.Encoder) {
			client.Raw([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})
			client.Uint32(epoch)
			client.Raw(make([]byte, 8))
			client.Int64(-1)
			client.Int32(-1)
			client.Bytes(fullMap)
			client.Bytes(nil)
			client.Int32(-1)
			client.Uint32(0)
			client.Uint32(uint32(len(names)))
			for id, name := range names {
				client.Int64(id)
				client.String(name)
			}
			client.Uint32(0)
			for range 14 {
				client.Uint32(0)
			}
			client.Raw(make([]byte, 16))
			client.Uint32(0)
			client.Uint32(0)
		})
		wrapper.Versioned(12, 1, func(*wire.Encoder) {})
		wrapper.Uint32(0)
		wrapper.Uint32(fullCRC)
	})
	data, err := encoder.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	crcOffset := len(data) - 8
	crc := wire.CRC32C(^uint32(0), data[:crcOffset])
	crc = wire.CRC32C(crc, data[crcOffset+4:])
	binary.LittleEndian.PutUint32(data[crcOffset:], crc)
	return data
}
