package osd

import (
	"fmt"
	"math"

	wire "github.com/otuschhoff/rados-go/internal/encoding"
)

type ListEntry struct {
	Namespace string
	Object    string
	Locator   string
}

type ListPage struct {
	Next    HObject
	Entries []ListEntry
}

func EncodePGNLSOperation(cursor HObject, count uint64, startEpoch uint32, maxBytes uint32) (Operation, error) {
	if count == 0 {
		return Operation{}, wire.ErrMalformed
	}
	encoder := wire.NewEncoder(maxBytes)
	encodeHObject(encoder, cursor)
	payload, err := encoder.BytesResult()
	if err != nil {
		return Operation{}, err
	}
	return Operation{Code: OpPGNList, ListCount: count, ListStartEpoch: startEpoch, Data: payload}, nil
}

func DecodePGNLSPage(data []byte, maxBytes, maxEntries uint32) (ListPage, error) {
	decoder := wire.NewDecoder(data, wire.Limits{MaxBytes: maxBytes})
	version, payload := decoder.Versioned(1)
	if err := decoder.Finish(); err != nil {
		return ListPage{}, err
	}
	if version != 1 {
		return ListPage{}, fmt.Errorf("%w: PGNLS response version %d", wire.ErrUnsupportedVersion, version)
	}
	next, err := decodeHObject(payload)
	if err != nil {
		return ListPage{}, err
	}
	count := payload.Uint32()
	if err := payload.Finish(); err != nil {
		return ListPage{}, err
	}
	if count > maxEntries || uint64(count) > uint64(math.MaxInt) || uint64(count)*12 > payload.Remaining() {
		return ListPage{}, wire.ErrLimitExceeded
	}
	page := ListPage{Next: next, Entries: make([]ListEntry, count)}
	for index := range page.Entries {
		page.Entries[index] = ListEntry{Namespace: payload.String(), Object: payload.String(), Locator: payload.String()}
		if err := payload.Finish(); err != nil {
			return ListPage{}, err
		}
	}
	if payload.Remaining() != 0 {
		return ListPage{}, fmt.Errorf("%w: trailing PGNLS response bytes", wire.ErrMalformed)
	}
	return page, nil
}
