package msgr

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
)

const (
	secureSecretSize = 64
	secureKeySize    = 16
	secureNonceSize  = 12
	secureTagSize    = 16
	secureBlockSize  = 16
	secureInlineSize = 48
	securePreamble   = PreambleSize + secureInlineSize + secureTagSize
)

var (
	ErrInvalidSecret    = errors.New("invalid messenger secure secret")
	ErrCounterExhausted = errors.New("messenger secure counter exhausted")
)

// SecureCodec encodes and decodes msgr2.1 secure frames. A codec is bound to
// one connection direction pair because every authenticated record consumes a
// nonce, including records that fail authentication.
type SecureCodec struct {
	tx *secureDirection
	rx *secureDirection
}

type secureDirection struct {
	mu        sync.Mutex
	aead      cipher.AEAD
	fixed     uint32
	counter   uint64
	exhausted bool
}

// NewSecureCodec maps a Ceph connection secret to client or server directions.
// The server mapping is crossed so that the client's TX seed is the server's RX
// seed and vice versa.
func NewSecureCodec(secret []byte, server bool) (*SecureCodec, error) {
	if len(secret) != secureSecretSize {
		return nil, fmt.Errorf("%w: got %d bytes, want %d", ErrInvalidSecret, len(secret), secureSecretSize)
	}
	clientRX := secret[16:28]
	clientTX := secret[28:40]
	rxSeed, txSeed := clientRX, clientTX
	if server {
		rxSeed, txSeed = clientTX, clientRX
	}
	tx, err := newSecureDirection(secret[:secureKeySize], txSeed)
	if err != nil {
		return nil, err
	}
	rx, err := newSecureDirection(secret[:secureKeySize], rxSeed)
	if err != nil {
		return nil, err
	}
	return &SecureCodec{tx: tx, rx: rx}, nil
}

func newSecureDirection(key, seed []byte) (*secureDirection, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &secureDirection{
		aead:    aead,
		fixed:   binary.LittleEndian.Uint32(seed[:4]),
		counter: binary.LittleEndian.Uint64(seed[4:]),
	}, nil
}

func (direction *secureDirection) availableLocked(count uint64) bool {
	if count == 0 {
		return true
	}
	return !direction.exhausted && count-1 <= math.MaxUint64-direction.counter
}

func (direction *secureDirection) nonceLocked() ([secureNonceSize]byte, error) {
	var nonce [secureNonceSize]byte
	if !direction.availableLocked(1) {
		return nonce, ErrCounterExhausted
	}
	binary.LittleEndian.PutUint32(nonce[:4], direction.fixed)
	binary.LittleEndian.PutUint64(nonce[4:], direction.counter)
	if direction.counter == math.MaxUint64 {
		direction.exhausted = true
	} else {
		direction.counter++
	}
	return nonce, nil
}

func (direction *secureDirection) sealLocked(plaintext []byte) ([]byte, error) {
	return direction.sealIntoLocked(nil, plaintext)
}

func (direction *secureDirection) sealIntoLocked(destination, plaintext []byte) ([]byte, error) {
	nonce, err := direction.nonceLocked()
	if err != nil {
		return nil, err
	}
	return direction.aead.Seal(destination, nonce[:], plaintext, nil), nil
}

func (direction *secureDirection) openLocked(ciphertext []byte) ([]byte, error) {
	nonce, err := direction.nonceLocked()
	if err != nil {
		return nil, err
	}
	plaintext, err := direction.aead.Open(ciphertext[:0], nonce[:], ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: secure authentication tag", ErrIntegrity)
	}
	return plaintext, nil
}

// Encode returns the Ceph msgr2.1 secure wire representation of frame.
func (codec *SecureCodec) Encode(frame Frame, limits Limits) ([]byte, error) {
	return codec.encodeInto(frame, limits, nil)
}

func (codec *SecureCodec) encodeInto(frame Frame, limits Limits, wire []byte) ([]byte, error) {
	descriptors, segments, wireSize, records, err := prepareSecureFrame(frame, limits)
	if err != nil {
		return nil, err
	}

	codec.tx.mu.Lock()
	defer codec.tx.mu.Unlock()
	if !codec.tx.availableLocked(records) {
		return nil, ErrCounterExhausted
	}

	preamble := encodePreamble(frame.Tag, descriptors)
	if uint64(cap(wire)) < wireSize {
		wire = make([]byte, int(wireSize))
	} else {
		wire = wire[:int(wireSize)]
	}
	first := wire[:PreambleSize+secureInlineSize]
	clear(first)
	copy(first, preamble[:])
	copy(first[PreambleSize:], segments[0].Data)
	result, err := codec.tx.sealIntoLocked(first[:0], first)
	if err != nil {
		return nil, err
	}

	firstPadded := paddedSecureLength(uint64(len(segments[0].Data)))
	if firstPadded > secureInlineSize {
		plaintext := wire[len(result) : len(result)+int(firstPadded-secureInlineSize)]
		copy(plaintext, segments[0].Data[secureInlineSize:])
		clear(plaintext[len(segments[0].Data)-secureInlineSize:])
		sealed, err := codec.tx.sealIntoLocked(plaintext[:0], plaintext)
		if err != nil {
			return nil, err
		}
		result = wire[:len(result)+len(sealed)]
	}
	if len(segments) == 1 {
		return result, nil
	}

	remainingSize := uint64(secureBlockSize)
	for index := 1; index < len(segments); index++ {
		remainingSize += paddedSecureLength(uint64(len(segments[index].Data)))
	}
	remaining := wire[len(result) : len(result)+int(remainingSize)]
	if lease := frame.payloadLease; lease != nil {
		lease.plaintextMu.Lock()
		plaintext := lease.securePlaintext(segments)
		if plaintext != nil && len(plaintext) == len(remaining) {
			sealed, err := codec.tx.sealIntoLocked(remaining[:0], plaintext)
			lease.plaintextMu.Unlock()
			if err != nil {
				return nil, err
			}
			return wire[:len(result)+len(sealed)], nil
		}
		lease.plaintextMu.Unlock()
	}
	offset := 0
	for index := 1; index < len(segments); index++ {
		copy(remaining[offset:], segments[index].Data)
		end := offset + int(paddedSecureLength(uint64(len(segments[index].Data))))
		clear(remaining[offset+len(segments[index].Data) : end])
		offset = end
	}
	clear(remaining[offset:])
	remaining[offset] = LateStatusComplete
	sealed, err := codec.tx.sealIntoLocked(remaining[:0], remaining)
	if err != nil {
		return nil, err
	}
	return wire[:len(result)+len(sealed)], nil
}

func prepareSecureFrame(frame Frame, limits Limits) ([]segmentDescriptor, []Segment, uint64, uint64, error) {
	if len(frame.Segments) < 1 || len(frame.Segments) > MaxSegments {
		return nil, nil, 0, 0, fmt.Errorf("%w: segment count %d", ErrMalformed, len(frame.Segments))
	}
	end := len(frame.Segments)
	for end > 1 && len(frame.Segments[end-1].Data) == 0 {
		end--
	}
	segments := frame.Segments[:end]
	if !validTag(frame.Tag) {
		return nil, nil, 0, 0, fmt.Errorf("%w: tag %d", ErrMalformed, frame.Tag)
	}
	descriptors := make([]segmentDescriptor, len(segments))
	wireSize := uint64(securePreamble)
	records := uint64(1)
	for index, segment := range segments {
		if len(segment.Data) > math.MaxUint32 || uint64(len(segment.Data)) > uint64(limits.MaxSegmentBytes) {
			return nil, nil, 0, 0, ErrLimitExceeded
		}
		if !validAlignment(segment.Alignment) {
			return nil, nil, 0, 0, fmt.Errorf("%w: segment %d alignment %d", ErrMalformed, index, segment.Alignment)
		}
		descriptors[index] = segmentDescriptor{length: uint32(len(segment.Data)), alignment: segment.Alignment}
	}
	firstPadded := paddedSecureLength(uint64(len(segments[0].Data)))
	if firstPadded > secureInlineSize {
		wireSize += firstPadded - secureInlineSize + secureTagSize
		records++
	}
	if len(segments) > 1 {
		wireSize += secureBlockSize + secureTagSize
		for index := 1; index < len(segments); index++ {
			addition := paddedSecureLength(uint64(len(segments[index].Data)))
			if wireSize > math.MaxUint64-addition {
				return nil, nil, 0, 0, ErrLimitExceeded
			}
			wireSize += addition
		}
		records++
	}
	if wireSize > limits.MaxFrameBytes || wireSize > uint64(maxInt()) {
		return nil, nil, 0, 0, ErrLimitExceeded
	}
	return descriptors, segments, wireSize, records, nil
}

// Read reads and authenticates one Ceph msgr2.1 secure frame.
func (codec *SecureCodec) Read(reader io.Reader, limits Limits) (Frame, error) {
	codec.rx.mu.Lock()
	defer codec.rx.mu.Unlock()
	if !codec.rx.availableLocked(1) {
		return Frame{}, ErrCounterExhausted
	}
	if limits.MaxFrameBytes < securePreamble {
		return Frame{}, ErrLimitExceeded
	}

	if err := reserveReceivePrelude(reader); err != nil {
		return Frame{}, err
	}
	var firstCiphertext []byte
	if source, ok := reader.(interface{ receivePreludeBuffer() []byte }); ok {
		firstCiphertext = source.receivePreludeBuffer()
	}
	preludeBackedByLease := firstCiphertext != nil
	if !preludeBackedByLease {
		firstCiphertext = make([]byte, securePreamble)
	}
	if _, err := io.ReadFull(reader, firstCiphertext); err != nil {
		return Frame{}, fmt.Errorf("%w: secure preamble: %v", ErrMalformed, err)
	}
	first, err := codec.rx.openLocked(firstCiphertext)
	if err != nil {
		return Frame{}, err
	}
	var preamble [PreambleSize]byte
	copy(preamble[:], first[:PreambleSize])
	tag, descriptors, err := decodePreamble(preamble)
	if err != nil {
		return Frame{}, err
	}
	wireSize, records, err := validateSecureDescriptors(descriptors, limits)
	if err != nil {
		return Frame{}, err
	}
	if !codec.rx.availableLocked(records - 1) {
		return Frame{}, ErrCounterExhausted
	}

	segments := make([]Segment, len(descriptors))
	firstPadded := paddedSecureLength(uint64(descriptors[0].length))
	retained := uint64(securePreamble)
	peak := retained
	if firstPadded > secureInlineSize {
		retained = firstPadded
		if preludeBackedByLease {
			retained += securePreamble
		}
		peak += firstPadded + firstPadded - secureInlineSize + secureTagSize
	}
	remainingSize := wireSize - uint64(securePreamble)
	if firstPadded > secureInlineSize {
		remainingSize -= firstPadded - secureInlineSize + secureTagSize
	}
	peak += remainingSize
	retained += remainingSize
	inlineControl := tag != TagMessage && firstPadded <= secureInlineSize && len(descriptors) == 1
	if inlineControl {
		peak, retained = 0, 0
	}
	if err := reserveReceiveBytes(reader, peak); err != nil {
		return Frame{}, err
	}
	if !inlineControl {
		releaseReceivePrelude(reader)
	}
	var firstPaddedData []byte
	if firstPadded > secureInlineSize {
		ciphertext := make([]byte, int(firstPadded-secureInlineSize+secureTagSize))
		if _, err := io.ReadFull(reader, ciphertext); err != nil {
			return Frame{}, fmt.Errorf("%w: secure segment 0: %v", ErrMalformed, err)
		}
		remainder, err := codec.rx.openLocked(ciphertext)
		if err != nil {
			return Frame{}, err
		}
		firstPaddedData = make([]byte, int(firstPadded))
		copy(firstPaddedData, first[PreambleSize:])
		copy(firstPaddedData[secureInlineSize:], remainder)
	} else {
		firstPaddedData = first[PreambleSize : PreambleSize+secureInlineSize]
	}
	if !allZero(firstPaddedData[descriptors[0].length:]) {
		return Frame{}, fmt.Errorf("%w: segment 0 padding", ErrMalformed)
	}
	segments[0] = Segment{Alignment: descriptors[0].alignment, Data: firstPaddedData[:descriptors[0].length:descriptors[0].length]}
	if len(descriptors) == 1 {
		if err := reserveReceiveBytes(reader, retained); err != nil {
			return Frame{}, err
		}
		return Frame{Tag: tag, Segments: segments}, nil
	}

	remainingCiphertextSize := wireSize - uint64(securePreamble)
	if firstPadded > secureInlineSize {
		remainingCiphertextSize -= firstPadded - secureInlineSize + secureTagSize
	}
	var remainingCiphertext []byte
	if source, ok := reader.(interface{ receiveBacking(int) ([]byte, error) }); ok {
		remainingCiphertext, err = source.receiveBacking(int(remainingCiphertextSize))
		if err != nil {
			return Frame{}, err
		}
	} else {
		remainingCiphertext = make([]byte, int(remainingCiphertextSize))
	}
	if _, err := io.ReadFull(reader, remainingCiphertext); err != nil {
		return Frame{}, fmt.Errorf("%w: secure remaining segments: %v", ErrMalformed, err)
	}
	remaining, err := codec.rx.openLocked(remainingCiphertext)
	if err != nil {
		return Frame{}, err
	}
	offset := 0
	for index := 1; index < len(descriptors); index++ {
		padded := int(paddedSecureLength(uint64(descriptors[index].length)))
		logical := int(descriptors[index].length)
		if !allZero(remaining[offset+logical : offset+padded]) {
			return Frame{}, fmt.Errorf("%w: segment %d padding", ErrMalformed, index)
		}
		segments[index] = Segment{Alignment: descriptors[index].alignment, Data: remaining[offset : offset+logical : offset+logical]}
		offset += padded
	}
	epilogue := remaining[offset:]
	if len(epilogue) != secureBlockSize || !allZero(epilogue[1:]) {
		return Frame{}, fmt.Errorf("%w: secure epilogue padding", ErrMalformed)
	}
	switch epilogue[0] & 0x0f {
	case LateStatusComplete:
		if err := reserveReceiveBytes(reader, retained); err != nil {
			return Frame{}, err
		}
		return Frame{Tag: tag, Segments: segments}, nil
	case LateStatusAborted:
		return Frame{}, ErrAborted
	default:
		return Frame{}, fmt.Errorf("%w: late status %#x", ErrMalformed, epilogue[0])
	}
}

func validateSecureDescriptors(descriptors []segmentDescriptor, limits Limits) (uint64, uint64, error) {
	wireSize := uint64(securePreamble)
	records := uint64(1)
	for index, descriptor := range descriptors {
		if !validAlignment(descriptor.alignment) {
			return 0, 0, fmt.Errorf("%w: segment %d alignment %d", ErrMalformed, index, descriptor.alignment)
		}
		if descriptor.length > limits.MaxSegmentBytes {
			return 0, 0, ErrLimitExceeded
		}
	}
	firstPadded := paddedSecureLength(uint64(descriptors[0].length))
	if firstPadded > secureInlineSize {
		wireSize += firstPadded - secureInlineSize + secureTagSize
		records++
	}
	if len(descriptors) > 1 {
		wireSize += secureBlockSize + secureTagSize
		for index := 1; index < len(descriptors); index++ {
			addition := paddedSecureLength(uint64(descriptors[index].length))
			if wireSize > math.MaxUint64-addition {
				return 0, 0, ErrLimitExceeded
			}
			wireSize += addition
		}
		records++
	}
	if wireSize > limits.MaxFrameBytes || wireSize > uint64(maxInt()) {
		return 0, 0, ErrLimitExceeded
	}
	return wireSize, records, nil
}

func paddedSecureLength(length uint64) uint64 {
	return (length + secureBlockSize - 1) &^ (secureBlockSize - 1)
}

func allZero(data []byte) bool {
	for _, value := range data {
		if value != 0 {
			return false
		}
	}
	return true
}
