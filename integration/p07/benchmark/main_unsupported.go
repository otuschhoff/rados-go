//go:build !linux

package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "P07 benchmark requires Linux")
	os.Exit(2)
}
