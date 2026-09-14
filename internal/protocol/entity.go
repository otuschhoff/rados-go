package protocol

import wire "github.com/otuschhoff/go-librados/internal/encoding"

type EntityType uint8

const (
	EntityMonitor EntityType = 0x01
	EntityMDS     EntityType = 0x02
	EntityOSD     EntityType = 0x04
	EntityClient  EntityType = 0x08
	EntityManager EntityType = 0x10
	EntityAuth    EntityType = 0x20
	EntityAny     EntityType = 0xff
)

const NewEntity int64 = -1

type EntityName struct {
	Type EntityType
	Num  int64
}

func (name EntityName) Encode(encoder *wire.Encoder) {
	encoder.Uint8(uint8(name.Type))
	encoder.Int64(name.Num)
}

func DecodeEntityName(decoder *wire.Decoder) EntityName {
	return EntityName{Type: EntityType(decoder.Uint8()), Num: decoder.Int64()}
}
