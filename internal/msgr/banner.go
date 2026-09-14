// Package msgr implements bounded Ceph messenger v2.1 framing and sessions.
package msgr

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	wire "github.com/otuschhoff/go-librados/internal/encoding"
)

const (
	bannerPrefix      = "ceph v2\n"
	bannerPayloadSize = 16
	Revision1Features = uint64(1)
)

var (
	ErrMalformed          = errors.New("malformed messenger data")
	ErrLimitExceeded      = errors.New("messenger limit exceeded")
	ErrUnsupportedFeature = errors.New("unsupported messenger feature")
)

// Banner advertises messenger feature masks. P02 requires revision 1 and does
// not advertise compression.
type Banner struct {
	Supported uint64
	Required  uint64
}

func ClientBanner() Banner {
	return Banner{Supported: Revision1Features, Required: Revision1Features}
}

func (banner Banner) Encode() []byte {
	data := make([]byte, len(bannerPrefix)+2+bannerPayloadSize)
	copy(data, bannerPrefix)
	binary.LittleEndian.PutUint16(data[len(bannerPrefix):], bannerPayloadSize)
	binary.LittleEndian.PutUint64(data[len(bannerPrefix)+2:], banner.Supported)
	binary.LittleEndian.PutUint64(data[len(bannerPrefix)+10:], banner.Required)
	return data
}

func ReadBanner(reader io.Reader, maxPayload uint16) (Banner, error) {
	header := make([]byte, len(bannerPrefix)+2)
	if _, err := io.ReadFull(reader, header); err != nil {
		return Banner{}, fmt.Errorf("%w: banner header: %w", ErrMalformed, err)
	}
	if string(header[:len(bannerPrefix)]) != bannerPrefix {
		return Banner{}, fmt.Errorf("%w: invalid banner prefix", ErrMalformed)
	}
	size := binary.LittleEndian.Uint16(header[len(bannerPrefix):])
	if size < bannerPayloadSize {
		return Banner{}, fmt.Errorf("%w: banner payload length %d", ErrMalformed, size)
	}
	if size > maxPayload {
		return Banner{}, fmt.Errorf("%w: banner payload length %d", ErrLimitExceeded, size)
	}
	payload := make([]byte, size)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return Banner{}, fmt.Errorf("%w: banner payload: %v", ErrMalformed, err)
	}
	return Banner{
		Supported: binary.LittleEndian.Uint64(payload[:8]),
		Required:  binary.LittleEndian.Uint64(payload[8:16]),
	}, nil
}

func NegotiateBanner(local, peer Banner) (uint64, error) {
	if peer.Required&^local.Supported != 0 || local.Required&^peer.Supported != 0 {
		return 0, ErrUnsupportedFeature
	}
	negotiated := local.Supported & peer.Supported
	if negotiated&Revision1Features == 0 {
		return 0, fmt.Errorf("%w: messenger revision 1 is required", ErrUnsupportedFeature)
	}
	return negotiated, nil
}

func cephCRC32C(seed uint32, payload []byte) uint32 {
	return wire.CRC32C(seed, payload)
}
