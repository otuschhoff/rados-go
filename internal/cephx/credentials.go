// Package cephx implements CephX authentication protocol mechanics.
package cephx

import (
	"bufio"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	wire "github.com/otuschhoff/go-librados/internal/encoding"
)

const (
	CryptoAES          = 1
	CryptoAES256KRB5   = 2
	AESKeySize         = 16
	AES256KeySize      = 32
	DefaultMaxKeyBytes = 256
	DefaultMaxKeyring  = 1 << 20
)

var (
	ErrCredentialNotFound = errors.New("cephx credential not found")
	ErrInvalidCredential  = errors.New("invalid cephx credential")
	ErrInvalidKeyring     = errors.New("invalid cephx keyring")
)

// Credential is an immutable CephX principal secret.
type Credential struct {
	entity  string
	created time.Time
	secret  CryptoKey
}

// CryptoKey retains the Ceph cipher identifier with value-owned key material.
type CryptoKey struct {
	typeID uint16
	size   uint8
	secret [AES256KeySize]byte
}

func (key CryptoKey) Type() uint16 { return key.typeID }

func (key CryptoKey) String() string {
	return fmt.Sprintf("cephx crypto key type %d (redacted)", key.typeID)
}

func (key CryptoKey) GoString() string { return key.String() }

// Bytes returns a copy of the secret key material.
func (key CryptoKey) Bytes() []byte {
	return append([]byte(nil), key.secret[:key.size]...)
}

func (credential Credential) Entity() string     { return credential.entity }
func (credential Credential) Created() time.Time { return credential.created }

// Secret returns a copy for use by the authentication engine.
func (credential Credential) Secret() CryptoKey { return credential.secret }

func (credential Credential) String() string { return "cephx credential for " + credential.entity }

func (credential Credential) GoString() string { return credential.String() }

// ParseKey decodes canonical 16-byte AES and 32-byte AES256-KRB5 keys produced
// by CryptoKey::encode_base64. Non-canonical key lengths are not supported.
func ParseKey(entity, encoded string, maxBytes uint32) (Credential, error) {
	if !validClientEntity(entity) {
		return Credential{}, fmt.Errorf("%w: invalid client entity", ErrInvalidCredential)
	}
	if maxBytes == 0 {
		maxBytes = DefaultMaxKeyBytes
	}
	encoded = strings.TrimSpace(encoded)
	if encoded == "" || uint64(len(encoded)) > uint64(maxBytes)*2 {
		return Credential{}, fmt.Errorf("%w: encoded key length", ErrInvalidCredential)
	}
	data, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil || uint64(len(data)) > uint64(maxBytes) {
		return Credential{}, fmt.Errorf("%w: malformed base64", ErrInvalidCredential)
	}
	decoder := wire.NewDecoder(data, wire.Limits{MaxBytes: maxBytes})
	keyType := decoder.Uint16()
	seconds := decoder.Uint32()
	nanoseconds := decoder.Uint32()
	secretLength := decoder.Uint16()
	secret := decoder.Raw(uint32(secretLength))
	if err := decoder.Finish(); err != nil || decoder.Remaining() != 0 {
		return Credential{}, fmt.Errorf("%w: malformed key envelope", ErrInvalidCredential)
	}
	if !validKeyParameters(keyType, secretLength) || nanoseconds >= uint32(time.Second) {
		return Credential{}, fmt.Errorf("%w: unsupported key parameters", ErrInvalidCredential)
	}
	credential := Credential{entity: entity, created: time.Unix(int64(seconds), int64(nanoseconds)).UTC()}
	credential.secret.typeID = keyType
	credential.secret.size = uint8(secretLength)
	copy(credential.secret.secret[:], secret)
	return credential, nil
}

func validKeyParameters(keyType, secretLength uint16) bool {
	switch keyType {
	case CryptoAES:
		return secretLength == AESKeySize
	case CryptoAES256KRB5:
		return secretLength == AES256KeySize
	default:
		return false
	}
}

// ParseKeyring extracts one client credential from the supported keyring subset.
// Comments, blank lines, quoted key values, and unrelated client sections are accepted.
func ParseKeyring(data []byte, entity string, maxBytes uint32) (Credential, error) {
	if !validClientEntity(entity) {
		return Credential{}, fmt.Errorf("%w: invalid client entity", ErrInvalidKeyring)
	}
	if maxBytes == 0 {
		maxBytes = DefaultMaxKeyring
	}
	if uint64(len(data)) > uint64(maxBytes) {
		return Credential{}, fmt.Errorf("%w: size limit", ErrInvalidKeyring)
	}

	var section string
	var key string
	foundSection := false
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	scanner.Buffer(make([]byte, 1024), int(maxBytes))
	for scanner.Scan() {
		line := stripComment(strings.TrimSpace(scanner.Text()))
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "[") {
			if !strings.HasSuffix(line, "]") || strings.Count(line, "[") != 1 || strings.Count(line, "]") != 1 {
				return Credential{}, fmt.Errorf("%w: malformed section", ErrInvalidKeyring)
			}
			section = strings.TrimSpace(line[1 : len(line)-1])
			if section == entity {
				if foundSection {
					return Credential{}, fmt.Errorf("%w: duplicate entity", ErrInvalidKeyring)
				}
				foundSection = true
			}
			continue
		}
		name, value, ok := strings.Cut(line, "=")
		if !ok || strings.TrimSpace(name) == "" || section == "" {
			return Credential{}, fmt.Errorf("%w: malformed property", ErrInvalidKeyring)
		}
		if section != entity {
			continue
		}
		property := strings.TrimSpace(name)
		if property == "auid" || strings.HasPrefix(property, "caps ") || strings.HasPrefix(property, "caps_") {
			continue
		}
		if property != "key" || key != "" {
			return Credential{}, fmt.Errorf("%w: unsupported or duplicate property", ErrInvalidKeyring)
		}
		value = strings.TrimSpace(value)
		if len(value) >= 2 && ((value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '\'' && value[len(value)-1] == '\'')) {
			value = value[1 : len(value)-1]
		}
		key = value
	}
	if err := scanner.Err(); err != nil {
		return Credential{}, fmt.Errorf("%w: %v", ErrInvalidKeyring, err)
	}
	if !foundSection || key == "" {
		return Credential{}, fmt.Errorf("%w: %s", ErrCredentialNotFound, entity)
	}
	credential, err := ParseKey(entity, key, DefaultMaxKeyBytes)
	if err != nil {
		return Credential{}, fmt.Errorf("%w: %v", ErrInvalidKeyring, err)
	}
	return credential, nil
}

func validClientEntity(entity string) bool {
	name, id, ok := strings.Cut(entity, ".")
	return ok && name == "client" && id != "" && !strings.ContainsAny(id, "[]\r\n\x00")
}

func stripComment(line string) string {
	var quote byte
	for index := 0; index < len(line); index++ {
		switch line[index] {
		case '\'', '"':
			if quote == 0 {
				quote = line[index]
			} else if quote == line[index] {
				quote = 0
			}
		case '#', ';':
			if quote == 0 {
				return strings.TrimSpace(line[:index])
			}
		}
	}
	return line
}
