package main

import (
	"encoding/json"
	"errors"
	"flag"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/otuschhoff/rados-go/internal/perfbaseline"
)

func TestReadEvidenceStrict(t *testing.T) {
	for _, data := range []string{
		`{"implementation":"go","unexpected":true}`,
		`{"implementation":"go"} {}`,
		`{"implementation":"go"} garbage`,
	} {
		filename := filepath.Join(t.TempDir(), "modes.json")
		if err := os.WriteFile(filename, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := readEvidence(filename); err == nil {
			t.Fatalf("accepted %q", data)
		}
	}
}

func TestNativeSidecarNotCreatedWithoutGoEvidence(t *testing.T) {
	directory := t.TempDir()
	logFilename := filepath.Join(directory, "native.log")
	outputFilename := filepath.Join(directory, "native.json")
	log := "--2- [v2:local] >> [v2:peer] conn(0x123 0x456 secure :0 s=READY pgs=1). ready entity=mon.0\n" +
		"--2- [v2:local] >> [v2:peer] conn(0x789 0xabc secure :0 s=READY pgs=1). ready entity=osd.0\n"
	if err := os.WriteFile(logFilename, []byte(log), 0600); err != nil {
		t.Fatal(err)
	}
	oldArgs, oldFlags := os.Args, flag.CommandLine
	t.Cleanup(func() { os.Args, flag.CommandLine = oldArgs, oldFlags })
	flag.CommandLine = flag.NewFlagSet("test", flag.ContinueOnError)
	os.Args = []string{"perf-mode-check", "-go", filepath.Join(directory, "missing.json"), "-native-log", logFilename, "-requested", "secure", "-native-out", outputFilename}
	if err := run(); err == nil {
		t.Fatal("missing Go evidence accepted")
	}
	if _, err := os.Lstat(outputFilename); !os.IsNotExist(err) {
		t.Fatalf("output created before reading Go evidence: %v", err)
	}
}

func runModeCheck(t *testing.T, args ...string) error {
	t.Helper()
	oldArgs, oldFlags := os.Args, flag.CommandLine
	defer func() { os.Args, flag.CommandLine = oldArgs, oldFlags }()
	flag.CommandLine = flag.NewFlagSet("test", flag.ContinueOnError)
	os.Args = append([]string{"perf-mode-check"}, args...)
	return run()
}

func TestNativeOutputExclusive(t *testing.T) {
	for _, name := range []string{
		"existing", "go", "log", "native", "lexical-go",
		"symlink-go", "symlink-log", "symlink-native", "dangling-symlink",
		"hardlink-go", "hardlink-log", "hardlink-native", "symlink-parent-go",
	} {
		t.Run(name, func(t *testing.T) {
			directory := t.TempDir()
			goFilename := filepath.Join(directory, "go.json")
			logFilename := filepath.Join(directory, "native.log")
			nativeFilename := filepath.Join(directory, "native.json")
			outputFilename := filepath.Join(directory, "output.json")
			goData := `{"implementation":"go","requested":"secure","connections":[{"service":"monitor","connection":"1","actual":"secure","source":"go-auth-metadata"},{"service":"osd","connection":"2","actual":"secure","source":"go-auth-metadata"}]}`
			nativeData := strings.ReplaceAll(strings.Replace(goData, `"implementation":"go"`, `"implementation":"native"`, 1), "go-auth-metadata", "ceph-ready-log")
			log := "--2- [v2:local] >> [v2:peer] conn(0x123 0x456 secure :-1 s=READY pgs=1).ready entity=mon.0\n" +
				"--2- [v2:local] >> [v2:peer] conn(0x789 0xabc secure :-1 s=READY pgs=1).ready entity=osd.1\n"
			originals := map[string]string{goFilename: goData, logFilename: log, nativeFilename: nativeData}
			for filename, data := range originals {
				if err := os.WriteFile(filename, []byte(data), 0600); err != nil {
					t.Fatal(err)
				}
			}
			targets := map[string]string{"go": goFilename, "log": logFilename, "native": nativeFilename}
			switch {
			case name == "existing":
				originals[outputFilename] = "preserved output\n"
				if err := os.WriteFile(outputFilename, []byte(originals[outputFilename]), 0600); err != nil {
					t.Fatal(err)
				}
			case targets[name] != "":
				outputFilename = targets[name]
			case name == "lexical-go":
				outputFilename = directory + "/./go.json"
			case name == "dangling-symlink":
				if err := os.Symlink(filepath.Join(directory, "missing"), outputFilename); err != nil {
					t.Fatal(err)
				}
			case name == "symlink-parent-go":
				alias := filepath.Join(directory, "alias")
				if err := os.Symlink(directory, alias); err != nil {
					t.Fatal(err)
				}
				outputFilename = filepath.Join(alias, "go.json")
			case strings.HasPrefix(name, "symlink-"):
				if err := os.Symlink(targets[strings.TrimPrefix(name, "symlink-")], outputFilename); err != nil {
					t.Fatal(err)
				}
			case strings.HasPrefix(name, "hardlink-"):
				if err := os.Link(targets[strings.TrimPrefix(name, "hardlink-")], outputFilename); err != nil {
					t.Fatal(err)
				}
			}
			nativeArgs := []string{"-native-log", logFilename, "-requested", "secure"}
			if strings.Contains(name, "native") {
				nativeArgs = []string{"-native", nativeFilename}
			}
			args := append([]string{"-go", goFilename, "-native-out", outputFilename}, nativeArgs...)
			if err := runModeCheck(t, args...); !errors.Is(err, os.ErrExist) {
				t.Fatalf("existing output not rejected: %v", err)
			}
			for filename, expected := range originals {
				data, err := os.ReadFile(filename)
				if err != nil || string(data) != expected {
					t.Fatalf("input/output changed at %s: %q, %v", filename, data, err)
				}
			}
			if name == "dangling-symlink" {
				if _, err := os.Lstat(filepath.Join(directory, "missing")); !os.IsNotExist(err) {
					t.Fatalf("dangling symlink target created: %v", err)
				}
			}
		})
	}
}

func TestReadGoEvidenceBeforeNativeOutput(t *testing.T) {
	directory := t.TempDir()
	goFilename := filepath.Join(directory, "go.json")
	outputFilename := filepath.Join(directory, "output.json")
	if err := os.WriteFile(goFilename, []byte("invalid Go evidence"), 0600); err != nil {
		t.Fatal(err)
	}
	err := runModeCheck(t, "-go", goFilename, "-native-log", filepath.Join(directory, "missing.log"), "-requested", "secure", "-native-out", outputFilename)
	if err == nil || !strings.Contains(err.Error(), "invalid character") {
		t.Fatalf("Go evidence not read first: %v", err)
	}
	if _, err := os.Lstat(outputFilename); !os.IsNotExist(err) {
		t.Fatalf("output created before reading Go evidence: %v", err)
	}
}

func TestModeCapturePreflightWithoutDocker(t *testing.T) {
	directory := t.TempDir()
	dockerMarker := filepath.Join(directory, "docker-called")
	if err := os.WriteFile(filepath.Join(directory, "docker"), []byte("#!/bin/sh\nprintf called >\"$P07_TEST_DOCKER_MARKER\"\nexit 99\n"), 0700); err != nil {
		t.Fatal(err)
	}
	existingFile := filepath.Join(directory, "existing-file")
	if err := os.WriteFile(existingFile, nil, 0600); err != nil {
		t.Fatal(err)
	}
	dangling := filepath.Join(directory, "dangling")
	if err := os.Symlink(filepath.Join(directory, "missing"), dangling); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, capture, conflict, message string
	}{
		{"relative", "relative-capture", "", "must be absolute"},
		{"existing-directory", directory, "", "must not exist"},
		{"existing-file", existingFile, "", "must not exist"},
		{"dangling-symlink", dangling, "", "must not exist"},
		{"conflicting-diagnostic", filepath.Join(directory, "fresh"), directory, "cannot be combined"},
	} {
		t.Run(test.name, func(t *testing.T) {
			command := exec.Command("sh", "../../integration/p07/reproduce.sh")
			for _, setting := range os.Environ() {
				if !strings.HasPrefix(setting, "P07_") && !strings.HasPrefix(setting, "PATH=") {
					command.Env = append(command.Env, setting)
				}
			}
			command.Env = append(command.Env, "PATH="+directory+string(os.PathListSeparator)+os.Getenv("PATH"), "P07_TEST_DOCKER_MARKER="+dockerMarker, "P07_MODE_DIAGNOSTIC_DIR="+test.capture, "P07_DIAGNOSTIC_DIR="+test.conflict)
			output, err := command.CombinedOutput()
			exit, ok := err.(*exec.ExitError)
			if !ok || exit.ExitCode() != 2 || !strings.Contains(string(output), test.message) {
				t.Fatalf("preflight: %v, output %s", err, output)
			}
			if _, err := os.Stat(dockerMarker); !os.IsNotExist(err) {
				t.Fatalf("Docker reached before path rejection: %v", err)
			}
		})
	}
}

func TestCLIRejectsModeClaims(t *testing.T) {
	for _, mode := range []string{"secure", "crc", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			directory := t.TempDir()
			goFilename := filepath.Join(directory, "go.json")
			logFilename := filepath.Join(directory, "native.log")
			outputFilename := filepath.Join(directory, "native.json")
			goEvidence := perfbaseline.ModeEvidence{Implementation: "go", Requested: mode, Connections: []perfbaseline.ConnectionMode{
				{Service: "monitor", Connection: "1", Actual: "secure", Source: "go-auth-metadata"},
				{Service: "osd", Connection: "2", Actual: "secure", Source: "go-auth-metadata"},
			}}
			data, err := json.Marshal(goEvidence)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(goFilename, data, 0600); err != nil {
				t.Fatal(err)
			}
			var log strings.Builder
			for _, service := range []string{"mon", "osd"} {
				log.WriteString("--2- [v2:local] >> [v2:peer] conn(0x123 0x456 secure :0 s=READY pgs=1).ready entity=")
				log.WriteString(service)
				log.WriteString(".0 client_cookie=123\n")
			}
			if err := os.WriteFile(logFilename, []byte(log.String()), 0600); err != nil {
				t.Fatal(err)
			}
			oldArgs, oldFlags := os.Args, flag.CommandLine
			t.Cleanup(func() { os.Args, flag.CommandLine = oldArgs, oldFlags })
			flag.CommandLine = flag.NewFlagSet("test", flag.ContinueOnError)
			os.Args = []string{"perf-mode-check", "-go", goFilename, "-native-log", logFilename, "-requested", mode, "-native-out", outputFilename}
			err = run()
			if (err == nil) != (mode == "secure") {
				t.Fatalf("mode %s: error = %v", mode, err)
			}
			if _, err := readEvidence(outputFilename); err != nil {
				t.Fatalf("normalized evidence missing: %v", err)
			}
			info, err := os.Stat(outputFilename)
			if err != nil || info.Mode().Perm() != 0600 {
				t.Fatalf("normalized evidence permissions: %v, %v", info, err)
			}
		})
	}
}
