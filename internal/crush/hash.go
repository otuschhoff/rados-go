// Package crush implements Ceph-compatible object placement.
package crush

import "encoding/binary"

const namespaceSeparator = byte(0x1f)

// ObjectHash returns Ceph's raw object hash. A non-empty locator replaces the
// object name as the hash key; namespace bytes are always part of the hash.
func ObjectHash(object, locator, namespace string) uint32 {
	key := object
	if locator != "" {
		key = locator
	}
	if namespace == "" {
		return RJenkins([]byte(key))
	}
	value := make([]byte, 0, len(namespace)+1+len(key))
	value = append(value, namespace...)
	value = append(value, namespaceSeparator)
	value = append(value, key...)
	return RJenkins(value)
}

// StableMod maps a hash into a possibly non-power-of-two placement count
// without remapping entries that already fit when the count grows.
func StableMod(value, count uint32) uint32 {
	if count == 0 {
		return 0
	}
	mask := uint32(1)<<uint(32-leadingZeros32(count-1)) - 1
	if value&mask < count {
		return value & mask
	}
	return value & (mask >> 1)
}

// Hash32Pair implements crush_hash32_2(CRUSH_HASH_RJENKINS1, a, b).
func Hash32Pair(a, b uint32) uint32 {
	hash := uint32(1315423911) ^ a ^ b
	x, y := uint32(231232), uint32(1232)
	a, b, hash = mix(a, b, hash)
	_, _, hash = mix(x, a, hash)
	_, _, hash = mix(b, y, hash)
	return hash
}

// Hash32Triple implements crush_hash32_3(CRUSH_HASH_RJENKINS1, a, b, c).
func Hash32Triple(a, b, c uint32) uint32 {
	hash := uint32(1315423911) ^ a ^ b ^ c
	x, y := uint32(231232), uint32(1232)
	a, b, hash = mix(a, b, hash)
	c, x, hash = mix(c, x, hash)
	y, _, hash = mix(y, a, hash)
	_, _, hash = mix(b, x, hash)
	_, _, hash = mix(y, c, hash)
	return hash
}

// RJenkins implements Ceph's CEPH_STR_HASH_RJENKINS byte hash.
func RJenkins(value []byte) uint32 {
	a, b, c := uint32(0x9e3779b9), uint32(0x9e3779b9), uint32(0)
	remaining := value
	for len(remaining) >= 12 {
		a += binary.LittleEndian.Uint32(remaining[0:4])
		b += binary.LittleEndian.Uint32(remaining[4:8])
		c += binary.LittleEndian.Uint32(remaining[8:12])
		a, b, c = mix(a, b, c)
		remaining = remaining[12:]
	}
	c += uint32(len(value))
	if len(remaining) >= 11 {
		c += uint32(remaining[10]) << 24
	}
	if len(remaining) >= 10 {
		c += uint32(remaining[9]) << 16
	}
	if len(remaining) >= 9 {
		c += uint32(remaining[8]) << 8
	}
	if len(remaining) >= 8 {
		b += uint32(remaining[7]) << 24
	}
	if len(remaining) >= 7 {
		b += uint32(remaining[6]) << 16
	}
	if len(remaining) >= 6 {
		b += uint32(remaining[5]) << 8
	}
	if len(remaining) >= 5 {
		b += uint32(remaining[4])
	}
	if len(remaining) >= 4 {
		a += uint32(remaining[3]) << 24
	}
	if len(remaining) >= 3 {
		a += uint32(remaining[2]) << 16
	}
	if len(remaining) >= 2 {
		a += uint32(remaining[1]) << 8
	}
	if len(remaining) >= 1 {
		a += uint32(remaining[0])
	}
	_, _, c = mix(a, b, c)
	return c
}

func mix(a, b, c uint32) (uint32, uint32, uint32) {
	a -= b
	a -= c
	a ^= c >> 13
	b -= c
	b -= a
	b ^= a << 8
	c -= a
	c -= b
	c ^= b >> 13
	a -= b
	a -= c
	a ^= c >> 12
	b -= c
	b -= a
	b ^= a << 16
	c -= a
	c -= b
	c ^= b >> 5
	a -= b
	a -= c
	a ^= c >> 3
	b -= c
	b -= a
	b ^= a << 10
	c -= a
	c -= b
	c ^= b >> 15
	return a, b, c
}

func leadingZeros32(value uint32) int {
	if value == 0 {
		return 32
	}
	count := 0
	for value&0x80000000 == 0 {
		count++
		value <<= 1
	}
	return count
}
