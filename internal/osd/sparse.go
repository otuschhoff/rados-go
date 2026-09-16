package osd

import (
	"fmt"
	"math"

	wire "github.com/otuschhoff/go-librados/internal/encoding"
)

type SparseExtent struct {
	Offset uint64
	Data   []byte
}

func DecodeSparseRead(data []byte, offset, length uint64, maxBytes, maxExtents uint32) ([]SparseExtent, error) {
	if maxBytes == 0 || maxExtents == 0 || length > math.MaxUint64-offset {
		return nil, fmt.Errorf("%w: invalid sparse-read bounds", ErrMalformedReply)
	}
	decoder := wire.NewDecoder(data, wire.Limits{MaxBytes: maxBytes})
	count := decoder.Uint32()
	if err := decoder.Finish(); err != nil {
		return nil, fmt.Errorf("%w: sparse extent count: %v", ErrMalformedReply, err)
	}
	if count > maxExtents || uint64(count)*16 > decoder.Remaining() {
		return nil, fmt.Errorf("%w: sparse extent count %d", ErrMalformedReply, count)
	}

	type extent struct {
		offset uint64
		length uint64
	}
	extents := make([]extent, count)
	requestEnd := offset + length
	total := uint64(0)
	previousEnd := offset
	for index := range extents {
		extentOffset := decoder.Uint64()
		extentLength := decoder.Uint64()
		if err := decoder.Finish(); err != nil {
			return nil, fmt.Errorf("%w: sparse extent %d: %v", ErrMalformedReply, index, err)
		}
		if extentLength == 0 || extentOffset < offset || extentOffset < previousEnd || extentLength > math.MaxUint64-extentOffset || extentOffset+extentLength > requestEnd || extentLength > uint64(maxBytes)-total {
			return nil, fmt.Errorf("%w: invalid sparse extent %d", ErrMalformedReply, index)
		}
		extents[index] = extent{offset: extentOffset, length: extentLength}
		previousEnd = extentOffset + extentLength
		total += extentLength
	}

	payload := decoder.Bytes()
	if err := decoder.Finish(); err != nil {
		return nil, fmt.Errorf("%w: sparse data: %v", ErrMalformedReply, err)
	}
	if decoder.Remaining() != 0 || uint64(len(payload)) != total {
		return nil, fmt.Errorf("%w: sparse data length", ErrMalformedReply)
	}

	result := make([]SparseExtent, len(extents))
	dataOffset := uint64(0)
	for index, extent := range extents {
		result[index] = SparseExtent{
			Offset: extent.offset,
			Data:   append([]byte(nil), payload[dataOffset:dataOffset+extent.length]...),
		}
		dataOffset += extent.length
	}
	return result, nil
}
