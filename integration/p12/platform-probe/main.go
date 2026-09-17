package main

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"

	rados "github.com/otuschhoff/go-librados"
)

const modulePath = "github.com/otuschhoff/go-librados"

var cgoEnabled = "unset"

type observation struct {
	GOOS          string `json:"goos"`
	GOARCH        string `json:"goarch"`
	ModulePath    string `json:"module_path"`
	ModuleVersion string `json:"module_version"`
	CGOEnabled    bool   `json:"cgo_enabled"`
}

func main() {
	enabled, err := strconv.ParseBool(cgoEnabled)
	if err != nil || enabled {
		fmt.Fprintln(os.Stderr, "platform-probe requires an explicit cgo-disabled build")
		os.Exit(1)
	}
	info, ok := debug.ReadBuildInfo()
	if !ok || info.Main.Path != modulePath {
		fmt.Fprintln(os.Stderr, "platform-probe could not establish the package module identity")
		os.Exit(1)
	}
	_ = rados.Config{}
	value := observation{
		GOOS: runtime.GOOS, GOARCH: runtime.GOARCH,
		ModulePath: info.Main.Path, ModuleVersion: info.Main.Version,
		CGOEnabled: enabled,
	}
	if err := json.NewEncoder(os.Stdout).Encode(value); err != nil {
		fmt.Fprintf(os.Stderr, "encode platform observation: %v\n", err)
		os.Exit(1)
	}
}
