package main

import (
	"crypto/aes"
	"crypto/hmac"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
)

const checksumSize = 24

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintf(os.Stderr, "usage: %s KEY_HEX CIPHERTEXT_HEX KEY_USAGE\n", os.Args[0])
		os.Exit(2)
	}
	key, err := hex.DecodeString(os.Args[1])
	if err != nil {
		fatal(err)
	}
	ciphertext, err := hex.DecodeString(os.Args[2])
	if err != nil {
		fatal(err)
	}
	usage, err := strconv.ParseUint(os.Args[3], 10, 32)
	if err != nil {
		fatal(err)
	}
	plaintext, err := decrypt(key, ciphertext, uint32(usage))
	if err != nil {
		fatal(err)
	}
	fmt.Printf("%x\n", plaintext)
}

func decrypt(key, ciphertext []byte, usage uint32) ([]byte, error) {
	if len(key) != 32 || len(ciphertext) < aes.BlockSize+checksumSize {
		return nil, fmt.Errorf("invalid key or ciphertext length")
	}
	encrypted := ciphertext[:len(ciphertext)-checksumSize]
	checksum := ciphertext[len(ciphertext)-checksumSize:]
	plaintext, err := decryptCTS(derive(key, usage, 0xaa, 256), encrypted)
	if err != nil {
		return nil, err
	}
	mac := hmac.New(sha512.New384, derive(key, usage, 0x55, 192))
	_, _ = mac.Write(make([]byte, aes.BlockSize))
	_, _ = mac.Write(encrypted)
	if subtle.ConstantTimeCompare(checksum, mac.Sum(nil)[:checksumSize]) != 1 {
		return nil, fmt.Errorf("checksum mismatch")
	}
	return plaintext[aes.BlockSize:], nil
}

func derive(key []byte, usage uint32, purpose byte, bits uint32) []byte {
	input := make([]byte, 0, 14)
	input = binary.BigEndian.AppendUint32(input, 1)
	input = binary.BigEndian.AppendUint32(input, usage)
	input = append(input, purpose, 0)
	input = binary.BigEndian.AppendUint32(input, bits)
	mac := hmac.New(sha512.New384, key)
	_, _ = mac.Write(input)
	return mac.Sum(nil)[:bits/8]
}

func decryptCTS(key, ciphertext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	if len(ciphertext) < aes.BlockSize {
		return nil, fmt.Errorf("ciphertext is shorter than one block")
	}
	if len(ciphertext)%aes.BlockSize == 0 {
		plaintext := make([]byte, len(ciphertext))
		previous := make([]byte, aes.BlockSize)
		for offset := 0; offset < len(ciphertext); offset += aes.BlockSize {
			block.Decrypt(plaintext[offset:offset+aes.BlockSize], ciphertext[offset:offset+aes.BlockSize])
			xor(plaintext[offset:offset+aes.BlockSize], previous)
			copy(previous, ciphertext[offset:offset+aes.BlockSize])
		}
		return plaintext, nil
	}
	lastLength := len(ciphertext) % aes.BlockSize
	prefixLength := len(ciphertext) - aes.BlockSize - lastLength
	plaintext := make([]byte, len(ciphertext))
	previous := make([]byte, aes.BlockSize)
	for offset := 0; offset < prefixLength; offset += aes.BlockSize {
		block.Decrypt(plaintext[offset:offset+aes.BlockSize], ciphertext[offset:offset+aes.BlockSize])
		xor(plaintext[offset:offset+aes.BlockSize], previous)
		copy(previous, ciphertext[offset:offset+aes.BlockSize])
	}
	finalFull := ciphertext[prefixLength : prefixLength+aes.BlockSize]
	finalPartial := ciphertext[prefixLength+aes.BlockSize:]
	decryptedFinal := make([]byte, aes.BlockSize)
	block.Decrypt(decryptedFinal, finalFull)
	stolenBlock := append(append([]byte(nil), finalPartial...), decryptedFinal[lastLength:]...)
	copy(plaintext[prefixLength+aes.BlockSize:], decryptedFinal[:lastLength])
	xor(plaintext[prefixLength+aes.BlockSize:], finalPartial)
	block.Decrypt(plaintext[prefixLength:prefixLength+aes.BlockSize], stolenBlock)
	xor(plaintext[prefixLength:prefixLength+aes.BlockSize], previous)
	return plaintext, nil
}

func xor(destination, source []byte) {
	for index := range destination {
		destination[index] ^= source[index]
	}
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "rfc8009-oracle: %v\n", err)
	os.Exit(1)
}
