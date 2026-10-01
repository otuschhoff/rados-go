package main

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGenerateIsReproducibleAndModuleIsBounded(t *testing.T) {
	root := seedReleaseTree(t)
	first := filepath.Join(t.TempDir(), "first")
	second := filepath.Join(t.TempDir(), "second")
	for _, output := range []string{first, second} {
		if err := generate(root, output, "v1.2.3-rc.1"); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := os.ReadDir(first)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 4 {
		t.Fatalf("release has %d directory entries, want exactly four", len(entries))
	}
	for _, name := range []string{"rados-go-v1.2.3-rc.1.tar.gz", "rados-go-v1.2.3-rc.1.zip", "rados-go-v1.2.3-rc.1.spdx.json", "SHA256SUMS"} {
		firstData, err := os.ReadFile(filepath.Join(first, name))
		if err != nil {
			t.Fatal(err)
		}
		secondData, err := os.ReadFile(filepath.Join(second, name))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(firstData, secondData) {
			t.Fatalf("%s differs between runs", name)
		}
	}
	var sbom spdxDocument
	sbomData, err := os.ReadFile(filepath.Join(first, "rados-go-v1.2.3-rc.1.spdx.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(sbomData, &sbom); err != nil {
		t.Fatal(err)
	}
	if len(sbom.Packages) != 1+len(productionDependencies) {
		t.Fatalf("SBOM has %d packages, want %d", len(sbom.Packages), 1+len(productionDependencies))
	}

	reader, err := zip.OpenReader(filepath.Join(first, "rados-go-v1.2.3-rc.1.zip"))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	for _, entry := range reader.File {
		if entry.Method != zip.Store {
			t.Fatalf("module entry %q uses toolchain-dependent compression", entry.Name)
		}
		if strings.Contains(entry.Name, "integration/") || strings.Contains(entry.Name, "tools/") || strings.Contains(entry.Name, "native.c") {
			t.Fatalf("module archive contains excluded infrastructure %q", entry.Name)
		}
	}
}

func TestSourceArchiveStoredBlocksRoundTrip(t *testing.T) {
	for _, size := range []int{0, 1, 65534, 65535, 65536, 131070} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			payload := bytes.Repeat([]byte("a"), size)
			output := filepath.Join(t.TempDir(), "source.tar.gz")
			if err := writeSourceArchive(output, "rados-go-v1.2.3", []sourceFile{{path: "payload", data: payload}}); err != nil {
				t.Fatal(err)
			}
			encoded, err := os.ReadFile(output)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(encoded[:10], []byte{0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 0, 255}) || !bytes.Equal(encoded[len(encoded)-13:len(encoded)-8], []byte{1, 0, 0, 255, 255}) {
				t.Fatal("gzip framing is not canonical")
			}
			reader, err := gzip.NewReader(bytes.NewReader(encoded))
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			decoded, err := io.ReadAll(reader)
			if err != nil {
				t.Fatalf("gzip integrity check failed: %v", err)
			}
			archive := tar.NewReader(bytes.NewReader(decoded))
			header, err := archive.Next()
			if err != nil {
				t.Fatal(err)
			}
			if header.Name != "rados-go-v1.2.3/payload" || header.Size != int64(size) || header.Mode != 0o644 {
				t.Fatalf("unexpected tar header: %+v", header)
			}
			contents, err := io.ReadAll(archive)
			if err != nil || !bytes.Equal(contents, payload) {
				t.Fatalf("tar payload mismatch: %v", err)
			}
			if _, err := archive.Next(); err != io.EOF {
				t.Fatalf("unexpected trailing tar entry: %v", err)
			}
		})
	}
}

func TestGenerateRejectsInvalidVersionAndSymlink(t *testing.T) {
	root := seedReleaseTree(t)
	if err := generate(root, t.TempDir(), "1.2.3"); err == nil {
		t.Fatal("version without v prefix was accepted")
	}
	if err := os.Symlink("source.go", filepath.Join(root, "internal", "linked.go")); err != nil {
		t.Fatal(err)
	}
	if err := generate(root, t.TempDir(), "v1.2.3"); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("symlink was accepted: %v", err)
	}
}

func TestValidateRelativePathRejectsTraversal(t *testing.T) {
	for _, name := range []string{"../escape", "a/../../escape", "/absolute", `a\b`, "a/../b"} {
		if err := validateRelativePath(name); err == nil {
			t.Errorf("unsafe path %q was accepted", name)
		}
	}
}

func seedReleaseTree(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	files := map[string]string{
		"LICENSE": "license", "THIRD_PARTY_NOTICES": "notices", "README.md": "readme", "SECURITY.md": "security",
		"go.mod": "module github.com/otuschhoff/rados-go\n", "go.sum": "sum", "client.go": "package rados\n",
		"internal/source.go": "package internal\n", "examples/basic/main.go": "package main\n",
		"integration/live.go": "package integration\n", "integration/native.c": "native", "tools/tool.go": "package main\n",
	}
	for name, content := range files {
		fullPath := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(fullPath, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}
