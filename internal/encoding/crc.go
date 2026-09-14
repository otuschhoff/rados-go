package encoding

import "hash/crc32"

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

// CRC32C extends a Ceph CRC32C value with payload.
func CRC32C(seed uint32, payload []byte) uint32 {
	return ^crc32.Update(^seed, castagnoliTable, payload)
}
