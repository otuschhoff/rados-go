package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/otuschhoff/rados-go/internal/perfbaseline"
)

func readEvidence(filename string) (perfbaseline.ModeEvidence, error) {
	var evidence perfbaseline.ModeEvidence
	file, err := os.Open(filename)
	if err != nil {
		return evidence, err
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&evidence); err != nil {
		return evidence, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return evidence, fmt.Errorf("trailing JSON in %s", filename)
	}
	return evidence, nil
}

func run() error {
	goFile := flag.String("go", "", "Go mode evidence sidecar")
	nativeFile := flag.String("native", "", "native mode evidence sidecar")
	nativeLog := flag.String("native-log", "", "fresh native Ceph messenger log instead of native sidecar")
	requested := flag.String("requested", "", "requested native mode when parsing a log: secure|crc")
	output := flag.String("native-out", "", "optional normalized native evidence file")
	flag.Parse()
	if *goFile == "" || (*nativeFile == "") == (*nativeLog == "") || flag.NArg() != 0 {
		return fmt.Errorf("require -go and exactly one of -native or -native-log")
	}
	goEvidence, err := readEvidence(*goFile)
	if err != nil {
		return err
	}
	var nativeEvidence perfbaseline.ModeEvidence
	if *nativeLog != "" {
		file, err := os.Open(*nativeLog)
		if err != nil {
			return err
		}
		nativeEvidence, err = perfbaseline.CaptureNativeModes(file, *requested)
		file.Close()
		if err != nil {
			return err
		}
	} else {
		nativeEvidence, err = readEvidence(*nativeFile)
		if err != nil {
			return err
		}
	}
	if *output != "" {
		data, err := json.MarshalIndent(nativeEvidence, "", "  ")
		if err != nil {
			return err
		}
		file, err := os.OpenFile(*output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return err
		}
		_, writeErr := file.Write(append(data, '\n'))
		closeErr := file.Close()
		if writeErr != nil {
			return writeErr
		}
		if closeErr != nil {
			return closeErr
		}
	}
	if err := perfbaseline.ValidateModePair(goEvidence, nativeEvidence); err != nil {
		return err
	}
	fmt.Println("Phase0 requested/actual Go and native modes match (monitor and OSD)")
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "perf-mode-check:", err)
		os.Exit(1)
	}
}
