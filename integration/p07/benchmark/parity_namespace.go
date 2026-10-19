package main

import (
	"fmt"
	"strings"
)

func parityNamespace(value string) (string, error) {
	if value == "" {
		return "", nil
	}
	if len(value) > 64 || !strings.HasPrefix(value, "p07-parity-") {
		return "", fmt.Errorf("parity namespace must start with p07-parity- and fit 64 bytes")
	}
	for _, character := range value {
		if character != '-' && (character < 'a' || character > 'z') && (character < '0' || character > '9') {
			return "", fmt.Errorf("parity namespace must contain only lowercase ASCII letters, digits and hyphens")
		}
	}
	return value, nil
}
