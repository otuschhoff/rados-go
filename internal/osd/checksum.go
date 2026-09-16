package osd

import (
	"encoding/binary"
	"fmt"
)

const (
	ChecksumXXHash32 = uint8(0)
	ChecksumXXHash64 = uint8(1)
	ChecksumCRC32C   = uint8(2)
)

func ChecksumSize(kind uint8) (uint64, bool) {
	switch kind {
	case ChecksumXXHash32, ChecksumCRC32C:
		return 4, true
	case ChecksumXXHash64:
		return 8, true
	default:
		return 0, false
	}
}

func DecodeChecksum(data []byte, kind uint8, maxBytes uint32) ([]byte, error) {
	valueSize, ok := ChecksumSize(kind)
	if !ok || maxBytes == 0 || uint64(len(data)) > uint64(maxBytes) {
		return nil, fmt.Errorf("%w: invalid checksum bounds", ErrMalformedReply)
	}
	if len(data) < 4 {
		return nil, fmt.Errorf("%w: truncated checksum count", ErrMalformedReply)
	}
	count := uint64(binary.LittleEndian.Uint32(data[:4]))
	if count > (uint64(maxBytes)-4)/valueSize || count*valueSize != uint64(len(data)-4) {
		return nil, fmt.Errorf("%w: invalid checksum values", ErrMalformedReply)
	}
	return append([]byte(nil), data...), nil
}
