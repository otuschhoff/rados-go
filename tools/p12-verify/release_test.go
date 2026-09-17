package main

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
)

func TestValidateReleaseChecksumsRequiresExactEntries(t *testing.T) {
	artifacts := map[string][]byte{"a": []byte("a"), "b": []byte("b")}
	var exact strings.Builder
	for _, name := range []string{"a", "b"} {
		digest := sha256Bytes(artifacts[name])
		fmt.Fprintf(&exact, "%s  %s\n", digest, name)
	}
	if err := validateReleaseChecksums([]byte(exact.String()), []string{"a", "b"}, artifacts); err != nil {
		t.Fatalf("exact checksums rejected: %v", err)
	}
	for name, data := range map[string][]byte{
		"reordered":       []byte(sha256Bytes(artifacts["b"]) + "  b\n" + sha256Bytes(artifacts["a"]) + "  a\n"),
		"missing newline": []byte(strings.TrimSuffix(exact.String(), "\n")),
		"wrong separator": []byte(strings.Replace(exact.String(), "  a", " *a", 1)),
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateReleaseChecksums(data, []string{"a", "b"}, artifacts); err == nil {
				t.Fatal("invalid SHA256SUMS accepted")
			}
		})
	}
}

func TestArchiveValidationRejectsUnsafeAndMismatchedEntries(t *testing.T) {
	sources := []releaseSource{{name: "source.go", data: []byte("current")}}
	for name, archive := range map[string][]byte{
		"tar traversal": makeTar(t, []tarFixture{{name: "prefix/../escape", data: []byte("current"), typeflag: tar.TypeReg}}),
		"tar duplicate": makeTar(t, []tarFixture{{name: "prefix/source.go", data: []byte("current"), typeflag: tar.TypeReg}, {name: "prefix/source.go", data: []byte("current"), typeflag: tar.TypeReg}}),
		"tar symlink":   makeTar(t, []tarFixture{{name: "prefix/source.go", typeflag: tar.TypeSymlink}}),
		"tar stale":     makeTar(t, []tarFixture{{name: "prefix/source.go", data: []byte("stale"), typeflag: tar.TypeReg}}),
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateTarGzip(archive, "prefix/", sources); err == nil {
				t.Fatal("unsafe tar archive accepted")
			}
		})
	}
	for name, archive := range map[string][]byte{
		"zip traversal": makeZip(t, []zipFixture{{name: "prefix/../escape", data: []byte("current"), mode: 0o644}}),
		"zip duplicate": makeZip(t, []zipFixture{{name: "prefix/source.go", data: []byte("current"), mode: 0o644}, {name: "prefix/source.go", data: []byte("current"), mode: 0o644}}),
		"zip symlink":   makeZip(t, []zipFixture{{name: "prefix/source.go", mode: os.ModeSymlink | 0o777}}),
		"zip directory": makeZip(t, []zipFixture{{name: "prefix/source.go/", mode: os.ModeDir | 0o755}}),
		"zip stale":     makeZip(t, []zipFixture{{name: "prefix/source.go", data: []byte("stale"), mode: 0o644}}),
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateModuleZip(archive, "prefix/", sources); err == nil {
				t.Fatal("unsafe module zip accepted")
			}
		})
	}
}

func TestTarValidationRejectsNoncanonicalGzipMetadata(t *testing.T) {
	sources := []releaseSource{{name: "source.go", data: []byte("current")}}
	archive := makeTar(t, []tarFixture{{name: "prefix/source.go", data: []byte("current"), typeflag: tar.TypeReg}})
	archive[9] = 3
	if err := validateTarGzip(archive, "prefix/", sources); err == nil {
		t.Fatal("noncanonical gzip operating system accepted")
	}
}

func TestSPDXValidationRejectsUnknownStaleAndExtraAnalysis(t *testing.T) {
	root := seedRetainedRelease(t, "v1.2.3")
	sources, err := collectReleaseSources(root)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(root + "/" + releaseArtifactsPath + "/go-librados-v1.2.3.spdx.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := validateReleaseSPDX(data, "v1.2.3", sources); err != nil {
		t.Fatalf("generated SPDX rejected: %v", err)
	}
	var document releaseSPDXDocument
	if err := json.Unmarshal(data, &document); err != nil {
		t.Fatal(err)
	}
	tests := map[string]func(*releaseSPDXDocument){
		"wrong source checksum": func(value *releaseSPDXDocument) { value.Files[0].Checksums[0].Value = strings.Repeat("0", 64) },
		"extra analyzed file":   func(value *releaseSPDXDocument) { value.Files = append(value.Files, value.Files[0]) },
		"wrong project license": func(value *releaseSPDXDocument) { value.Packages[0].LicenseDeclared = "MIT" },
		"wrong dependency":      func(value *releaseSPDXDocument) { value.Packages[1].VersionInfo = "v9.9.9" },
		"extra relationship": func(value *releaseSPDXDocument) {
			value.Relationships = append(value.Relationships, value.Relationships[0])
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			value := document
			value.Files = slices.Clone(document.Files)
			value.Packages = slices.Clone(document.Packages)
			value.Relationships = slices.Clone(document.Relationships)
			mutate(&value)
			mutated, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			if err := validateReleaseSPDX(mutated, "v1.2.3", sources); err == nil {
				t.Fatal("invalid SPDX accepted")
			}
		})
	}
	unknown := bytes.Replace(data, []byte("{"), []byte(`{"unknown":true,`), 1)
	if err := validateReleaseSPDX(unknown, "v1.2.3", sources); err == nil {
		t.Fatal("unknown SPDX field accepted")
	}
}

type tarFixture struct {
	name     string
	data     []byte
	typeflag byte
}

func makeTar(t *testing.T, entries []tarFixture) []byte {
	t.Helper()
	var buffer bytes.Buffer
	gzipWriter := gzip.NewWriter(&buffer)
	tarWriter := tar.NewWriter(gzipWriter)
	for _, entry := range entries {
		if err := tarWriter.WriteHeader(&tar.Header{Name: entry.name, Mode: 0o644, Size: int64(len(entry.data)), Typeflag: entry.typeflag}); err != nil {
			t.Fatal(err)
		}
		if _, err := tarWriter.Write(entry.data); err != nil {
			t.Fatal(err)
		}
	}
	if err := tarWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gzipWriter.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

type zipFixture struct {
	name string
	data []byte
	mode os.FileMode
}

func makeZip(t *testing.T, entries []zipFixture) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	for _, entry := range entries {
		header := &zip.FileHeader{Name: entry.name, Method: zip.Store}
		header.SetMode(entry.mode)
		file, err := writer.CreateHeader(header)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Write(entry.data); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

func sha256Bytes(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}
