package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func fixture(t *testing.T) (string, string) {
	t.Helper()
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.MkdirAll(filepath.Join(source, "evidence", "empty"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "evidence", "sample.json"), []byte("{\"value\":42}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	return source, filepath.Join(root, "cache")
}

func TestRoundTripDeterministic(t *testing.T) {
	source, cache := fixture(t)
	first, err := pack(source, cache, []string{"evidence"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(filepath.Join(source, "evidence", "sample.json"), time.Unix(1234, 0), time.Unix(5678, 0)); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(source, "evidence", "sample.json"), 0644); err != nil {
		t.Fatal(err)
	}
	second, err := pack(source, cache+"-second", []string{"evidence"})
	if err != nil {
		t.Fatal(err)
	}
	if first.Entries[0] != second.Entries[0] {
		t.Fatal("nondeterministic archives")
	}
	if err := inspectArchive(cache, first.Entries[0], ""); err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(filepath.Dir(source), "restored")
	if err := restore(first, cache, dest, map[string]bool{"evidence": true}); err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile(filepath.Join(source, "evidence", "sample.json"))
	if err != nil {
		t.Fatal(err)
	}
	restored, err := os.ReadFile(filepath.Join(dest, "evidence", "sample.json"))
	if err != nil || !bytes.Equal(original, restored) {
		t.Fatalf("restored bytes: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dest, "evidence", "empty")); err != nil {
		t.Fatal(err)
	}
	if err := restore(first, cache, dest, map[string]bool{"evidence": true}); err == nil {
		t.Fatal("overwrote destination")
	}
	if _, err := pack(source, cache, []string{"evidence"}); err == nil {
		t.Fatal("overwrote cache")
	}
}

func TestManifestValidation(t *testing.T) {
	base := manifest{Schema: 1, Bundle: "evidence", Files: []record{{Path: "evidence/file", Size: 3, SHA256: digest([]byte("abc"))}}, Directories: []string{"evidence"}}
	item := entry{Bundle: "evidence", Files: 1, UnpackedBytes: 3}
	for _, mutate := range []func(*manifest){
		func(contents *manifest) { contents.Files[0].Path = "../file" },
		func(contents *manifest) { contents.Files[0].Path = "other/file" },
		func(contents *manifest) { contents.Files[0].Path = "evidence/missing/file" },
		func(contents *manifest) { contents.Files[0].Size = -1 },
		func(contents *manifest) { contents.Files[0].Size = maxBytes + 1 },
		func(contents *manifest) { contents.Files[0].SHA256 = "bad" },
		func(contents *manifest) { contents.Directories = append(contents.Directories, "evidence") },
		func(contents *manifest) { contents.Directories = append(contents.Directories, "evidence/file") },
		func(contents *manifest) { contents.Directories = []string{"other"} },
		func(contents *manifest) { contents.Files = append(contents.Files, contents.Files[0]) },
	} {
		contents := base
		contents.Files = append([]record(nil), base.Files...)
		contents.Directories = append([]string(nil), base.Directories...)
		mutate(&contents)
		if err := validateManifest(contents, item); err == nil {
			t.Fatal("accepted invalid manifest")
		}
	}
}

func TestPackStagesBeforePublication(t *testing.T) {
	source, cache := fixture(t)
	if _, err := pack(source, cache, []string{"evidence", "inventory-evidence"}); err == nil {
		t.Fatal("packed missing later bundle")
	}
	files, err := os.ReadDir(cache)
	if err != nil || len(files) != 0 {
		t.Fatalf("failed pack left files: %v, %v", files, err)
	}
	if err := os.Mkdir(filepath.Join(source, "inventory-evidence"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := pack(source, cache, []string{"evidence", "inventory-evidence"}); err != nil {
		t.Fatalf("retry failed: %v", err)
	}
}

func TestPackPublicationRollback(t *testing.T) {
	for _, conflict := range []string{"inventory-evidence.tar.gz", "archive-index.json"} {
		t.Run(conflict, func(t *testing.T) {
			stage, cache := t.TempDir(), t.TempDir()
			names := []string{"evidence.tar.gz", "inventory-evidence.tar.gz", "archive-index.json"}
			for _, name := range names {
				if err := os.WriteFile(filepath.Join(stage, name), []byte("staged"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			occupied := filepath.Join(cache, conflict)
			if err := os.WriteFile(occupied, []byte("keep"), 0600); err != nil {
				t.Fatal(err)
			}
			before, err := os.Lstat(occupied)
			if err != nil {
				t.Fatal(err)
			}
			if err := publishPack(stage, cache, names); err == nil {
				t.Fatal("overwrote publication conflict")
			}
			after, err := os.Lstat(occupied)
			if err != nil || !os.SameFile(before, after) {
				t.Fatal("removed or replaced preexisting file")
			}
			data, err := os.ReadFile(occupied)
			if err != nil || string(data) != "keep" {
				t.Fatal("changed preexisting bytes")
			}
			files, err := os.ReadDir(cache)
			if err != nil || len(files) != 1 {
				t.Fatalf("left early publications: %v, %v", files, err)
			}
			if err := os.Remove(occupied); err != nil {
				t.Fatal(err)
			}
			if err := publishPack(stage, cache, names); err != nil {
				t.Fatalf("retry failed: %v", err)
			}
		})
	}
}

func TestPackRollbackPreservesReplacement(t *testing.T) {
	cache := t.TempDir()
	filename := filepath.Join(cache, "evidence.tar.gz")
	if err := os.WriteFile(filename, []byte("owned"), 0600); err != nil {
		t.Fatal(err)
	}
	owned, err := os.Lstat(filename)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filename, filename+"-owned"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filename, []byte("replacement"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := rollbackPack(map[string]os.FileInfo{filename: owned}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filename)
	if err != nil || string(data) != "replacement" {
		t.Fatal("rollback removed replacement")
	}
}

func TestRestoreAncestorReplacement(t *testing.T) {
	source, cache := fixture(t)
	result, err := pack(source, cache, []string{"evidence"})
	if err != nil {
		t.Fatal(err)
	}
	parent := filepath.Join(filepath.Dir(source), "parent")
	if err := os.Mkdir(parent, 0700); err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(parent, "restored")
	destination, err := reserveRestore(dest)
	if err != nil {
		t.Fatal(err)
	}
	defer destination.root.Close()
	defer destination.parent.Close()
	if err := os.Rename(parent, parent+"-owned"); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dest, 0700); err != nil {
		t.Fatal(err)
	}
	before, err := os.Lstat(dest)
	if err != nil {
		t.Fatal(err)
	}
	if err := materializeRestore(result, cache, map[string]bool{"evidence": true}, destination); err == nil {
		t.Fatal("reported success after ancestor replacement")
	}
	after, err := os.Lstat(dest)
	if err != nil || !os.SameFile(before, after) {
		t.Fatal("cleanup changed replacement under new ancestor")
	}
	files, err := os.ReadDir(dest)
	if err != nil || len(files) != 0 {
		t.Fatal("wrote into replacement ancestor")
	}
}

func TestRestoreMaterializationFailureCleanup(t *testing.T) {
	source, cache := fixture(t)
	result, err := pack(source, cache, []string{"evidence"})
	if err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(filepath.Dir(source), "restored")
	destination, err := reserveRestore(dest)
	if err != nil {
		t.Fatal(err)
	}
	defer destination.root.Close()
	defer destination.parent.Close()
	if err := destination.root.Mkdir("evidence", 0700); err != nil {
		t.Fatal(err)
	}
	if err := destination.root.WriteFile("evidence/sample.json", []byte("conflict"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := materializeRestore(result, cache, map[string]bool{"evidence": true}, destination); err == nil {
		t.Fatal("overwrote file created during extraction")
	}
	if _, err := os.Lstat(dest); !os.IsNotExist(err) {
		t.Fatalf("left owned failed destination: %v", err)
	}
	if err := restore(result, cache, dest, map[string]bool{"evidence": true}); err != nil {
		t.Fatalf("retry failed: %v", err)
	}
}

func TestRestoreDestinationReplacement(t *testing.T) {
	for _, kind := range []string{"empty-directory", "nonempty-directory", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			source, cache := fixture(t)
			result, err := pack(source, cache, []string{"evidence"})
			if err != nil {
				t.Fatal(err)
			}
			dest := filepath.Join(filepath.Dir(source), "restored")
			destination, err := reserveRestore(dest)
			if err != nil {
				t.Fatal(err)
			}
			defer destination.root.Close()
			defer destination.parent.Close()
			moved := dest + "-owned"
			if err := os.Rename(dest, moved); err != nil {
				t.Fatal(err)
			}
			if kind == "symlink" {
				err = os.Symlink(source, dest)
			} else {
				err = os.Mkdir(dest, 0700)
			}
			if err != nil {
				t.Fatal(err)
			}
			if kind == "nonempty-directory" {
				if err := os.WriteFile(filepath.Join(dest, "keep"), []byte("keep"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			before, err := os.Lstat(dest)
			if err != nil {
				t.Fatal(err)
			}
			if err := materializeRestore(result, cache, map[string]bool{"evidence": true}, destination); err == nil {
				t.Fatal("reported success for replaced destination")
			}
			after, err := os.Lstat(dest)
			if err != nil || !os.SameFile(before, after) {
				t.Fatalf("cleanup changed replacement: %v", err)
			}
			data, err := os.ReadFile(filepath.Join(moved, "evidence", "sample.json"))
			if err != nil || string(data) != "{\"value\":42}\n" {
				t.Fatalf("writes did not stay in owned directory: %v", err)
			}
			if kind != "symlink" {
				files, err := os.ReadDir(dest)
				expected := 0
				if kind == "nonempty-directory" {
					expected = 1
				}
				if err != nil || len(files) != expected {
					t.Fatalf("modified replacement contents: %v, %v", files, err)
				}
			}
		})
	}
}

func TestEnvelopeLimits(t *testing.T) {
	for _, header := range []*tar.Header{
		{Name: "not-the-manifest", Typeflag: tar.TypeReg},
		{Name: manifestName, Typeflag: tar.TypeSymlink, Linkname: "elsewhere"},
		{Name: manifestName, Typeflag: tar.TypeReg, Size: maxManifest + 1},
	} {
		var buffer bytes.Buffer
		compressed := gzip.NewWriter(&buffer)
		archive := tar.NewWriter(compressed)
		if err := archive.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		if header.Size == 0 {
			if err := archive.Close(); err != nil {
				t.Fatal(err)
			}
		}
		if err := compressed.Close(); err != nil {
			t.Fatal(err)
		}
		cache := t.TempDir()
		if err := os.WriteFile(filepath.Join(cache, "evidence.tar.gz"), buffer.Bytes(), 0600); err != nil {
			t.Fatal(err)
		}
		item := entry{Bundle: "evidence", Filename: "evidence.tar.gz", SHA256: digest(buffer.Bytes()), ArchiveBytes: int64(buffer.Len())}
		if err := inspectArchive(cache, item, ""); err == nil {
			t.Fatal("accepted invalid envelope")
		}
	}
}

func TestExactCaptureAndSourceChanges(t *testing.T) {
	source, cache := fixture(t)
	if err := os.WriteFile(filepath.Join(source, "secret"), []byte("outside"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "evidence", ".DS_Store"), []byte("inside"), 0600); err != nil {
		t.Fatal(err)
	}
	longName := strings.Repeat("long-name", 20)
	if err := os.WriteFile(filepath.Join(source, "evidence", longName), []byte{0, 1, 255}, 0600); err != nil {
		t.Fatal(err)
	}
	result, err := pack(source, cache, []string{"evidence"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Entries[0].Files != 3 {
		t.Fatal("capture must preserve every bundle file and exclude source siblings")
	}
	if err := os.WriteFile(filepath.Join(source, "evidence", "sample.json"), []byte("changed"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := inspectArchive(cache, result.Entries[0], ""); err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(filepath.Dir(source), "snapshot")
	if err := restore(result, cache, dest, map[string]bool{"evidence": true}); err != nil {
		t.Fatal(err)
	}
	restored, err := os.ReadFile(filepath.Join(dest, "evidence", "sample.json"))
	if err != nil || string(restored) != "{\"value\":42}\n" {
		t.Fatal("archive did not preserve original bytes")
	}
	if _, err := os.Stat(filepath.Join(source, "secret")); err != nil {
		t.Fatal("source was removed")
	}
	if _, err := os.Stat(filepath.Join(dest, "secret")); !os.IsNotExist(err) {
		t.Fatal("captured source sibling")
	}
}

func TestCacheContainment(t *testing.T) {
	source, _ := fixture(t)
	internal := filepath.Join(source, "new-cache")
	if _, err := pack(source, internal, []string{"evidence"}); err == nil {
		t.Fatal("accepted source cache")
	}
	if _, err := os.Stat(internal); !os.IsNotExist(err) {
		t.Fatal("modified source before rejecting cache")
	}
	alias := filepath.Join(filepath.Dir(source), "alias")
	if err := os.Symlink(source, alias); err != nil {
		t.Fatal(err)
	}
	if _, err := pack(source, filepath.Join(alias, "new-cache"), []string{"evidence"}); err == nil {
		t.Fatal("accepted symlink-resolved source cache")
	}
	if err := os.WriteFile(filepath.Join(filepath.Dir(source), "go.mod"), []byte("module fixture\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := pack(source, filepath.Join(filepath.Dir(source), "project-cache"), []string{"evidence"}); err == nil {
		t.Fatal("accepted main-project cache")
	}
}

func TestCLIAndIndex(t *testing.T) {
	source, cache := fixture(t)
	for _, bundle := range defaultBundles {
		if err := os.MkdirAll(filepath.Join(source, bundle), 0700); err != nil {
			t.Fatal(err)
		}
	}
	var output bytes.Buffer
	if err := run([]string{"pack", "--source", source, "--cache-dir", cache}, &output); err != nil {
		t.Fatal(err)
	}
	indexPath := filepath.Join(cache, "archive-index.json")
	result, err := loadIndex(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Entries) != 7 || strings.Contains(output.String(), cache) {
		t.Fatal("incomplete index or absolute storage location")
	}
	for _, command := range []string{"verify", "restore"} {
		args := []string{command, "--index", indexPath, "--cache-dir", cache, "--bundles", "evidence,inventory-evidence"}
		if command == "restore" {
			args = append(args, "--dest", filepath.Join(filepath.Dir(source), "cli-restored"))
		}
		if err := run(args, &output); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(source), "cli-restored", "batching-evidence")); !os.IsNotExist(err) {
		t.Fatal("restored unselected bundle")
	}
	for _, args := range [][]string{{}, {"unknown"}, {"verify"}, {"restore", "--index", indexPath}, {"pack", "--bundles", "../evidence"}, {"verify", "--index", indexPath, "--bundles", "evidence,evidence"}, {"verify", "--index", indexPath, "--bundles", "secret"}} {
		if err := run(args, io.Discard); err == nil {
			t.Fatalf("accepted invalid CLI %v", args)
		}
	}
}

func TestSourceSafety(t *testing.T) {
	for _, kind := range []string{"symlink", "hardlink", "directory-symlink"} {
		t.Run(kind, func(t *testing.T) {
			source, cache := fixture(t)
			original := filepath.Join(source, "evidence", "sample.json")
			link := filepath.Join(source, "evidence", "link")
			var err error
			switch kind {
			case "symlink":
				err = os.Symlink(original, link)
			case "hardlink":
				err = os.Link(original, link)
			case "directory-symlink":
				err = os.Symlink(filepath.Dir(original), link)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := pack(source, cache, []string{"evidence"}); err == nil {
				t.Fatal("accepted linked source")
			}
			if _, err := os.Stat(original); err != nil {
				t.Fatal("source removed")
			}
		})
	}
	source, _ := fixture(t)
	if _, err := pack(source, filepath.Join(source, "cache"), []string{"evidence"}); err == nil {
		t.Fatal("accepted internal cache")
	}
}

func TestPaths(t *testing.T) {
	for _, name := range []string{"", ".", "..", "../file", "/file", "evidence/../file", "evidence//file", "evidence/./file", "evidence\\file", "evidence/", "C:/file", "evidence/\x00", "evidence/\xff"} {
		if err := safePath(name); err == nil {
			t.Fatalf("accepted %q", name)
		}
	}
	if err := safePath("evidence/nested/file.json"); err != nil {
		t.Fatal(err)
	}
}

func craftedArchive(t *testing.T, contents manifest, headers []*tar.Header, payloads [][]byte, mutate func([]byte) []byte) (string, entry) {
	t.Helper()
	data, err := json.Marshal(contents)
	if err != nil {
		t.Fatal(err)
	}
	var buffer bytes.Buffer
	compressed := gzip.NewWriter(&buffer)
	archive := tar.NewWriter(compressed)
	if err := archive.WriteHeader(&tar.Header{Name: manifestName, Typeflag: tar.TypeReg, Mode: 0600, Size: int64(len(data))}); err != nil {
		t.Fatal(err)
	}
	if _, err := archive.Write(data); err != nil {
		t.Fatal(err)
	}
	for position, header := range headers {
		if err := archive.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		if position < len(payloads) && payloads[position] != nil {
			if _, err := archive.Write(payloads[position]); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}
	if err := compressed.Close(); err != nil {
		t.Fatal(err)
	}
	encoded := buffer.Bytes()
	if mutate != nil {
		encoded = mutate(encoded)
	}
	cache := t.TempDir()
	if err := os.WriteFile(filepath.Join(cache, "evidence.tar.gz"), encoded, 0600); err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, record := range contents.Files {
		total += record.Size
	}
	return cache, entry{ID: "evidence", Bundle: "evidence", Filename: "evidence.tar.gz", SHA256: digest(encoded), ArchiveBytes: int64(len(encoded)), Files: len(contents.Files), UnpackedBytes: total, ContentManifestSHA256: digest(data)}
}

func TestMaliciousArchives(t *testing.T) {
	contents := manifest{Schema: 1, Bundle: "evidence", Directories: []string{"evidence"}, Files: []record{{Path: "evidence/file", Size: 3, SHA256: digest([]byte("abc"))}}}
	directory := &tar.Header{Name: "evidence", Typeflag: tar.TypeDir, Mode: 0777}
	file := &tar.Header{Name: "evidence/file", Typeflag: tar.TypeReg, Size: 3, Mode: 0777}
	tests := []struct {
		name     string
		headers  []*tar.Header
		payloads [][]byte
		mutate   func([]byte) []byte
	}{
		{"traversal", []*tar.Header{directory, {Name: "../escape", Typeflag: tar.TypeReg, Size: 3}}, [][]byte{nil, []byte("abc")}, nil},
		{"absolute", []*tar.Header{directory, {Name: "/escape", Typeflag: tar.TypeReg, Size: 3}}, [][]byte{nil, []byte("abc")}, nil},
		{"symlink", []*tar.Header{directory, {Name: "evidence/file", Typeflag: tar.TypeSymlink, Linkname: "../escape"}}, nil, nil},
		{"hardlink", []*tar.Header{directory, {Name: "evidence/file", Typeflag: tar.TypeLink, Linkname: "evidence/other"}}, nil, nil},
		{"fifo", []*tar.Header{directory, {Name: "evidence/file", Typeflag: tar.TypeFifo}}, nil, nil},
		{"duplicate", []*tar.Header{directory, file, file}, [][]byte{nil, []byte("abc"), []byte("abc")}, nil},
		{"changed-byte", []*tar.Header{directory, file}, [][]byte{nil, []byte("abd")}, nil},
		{"missing-file", []*tar.Header{directory}, nil, nil},
		{"missing-directory", []*tar.Header{file}, [][]byte{[]byte("abc")}, nil},
		{"directory-type", []*tar.Header{{Name: "evidence", Typeflag: tar.TypeReg, Size: 0}, file}, [][]byte{nil, []byte("abc")}, nil},
		{"truncated", []*tar.Header{directory, file}, [][]byte{nil, []byte("abc")}, func(data []byte) []byte { return data[:len(data)-5] }},
		{"gzip-checksum", []*tar.Header{directory, file}, [][]byte{nil, []byte("abc")}, func(data []byte) []byte { data[len(data)-8] ^= 1; return data }},
		{"trailing-gzip", []*tar.Header{directory, file}, [][]byte{nil, []byte("abc")}, func(data []byte) []byte { return append(data, 1) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cache, item := craftedArchive(t, contents, test.headers, test.payloads, test.mutate)
			if err := inspectArchive(cache, item, ""); err == nil {
				t.Fatal("accepted malicious archive")
			}
			result := index{Schema: 1, Root: "source", CodeDirs: []string{"tools/experiment-cache"}, Entries: []entry{item}}
			dest := filepath.Join(t.TempDir(), "restored")
			if err := restore(result, cache, dest, map[string]bool{"evidence": true}); err == nil {
				t.Fatal("restored malicious archive")
			}
			if _, err := os.Lstat(dest); !os.IsNotExist(err) {
				t.Fatal("published failed restore")
			}
			partials, err := filepath.Glob(filepath.Join(filepath.Dir(dest), ".experiment-cache-restore-*"))
			if err != nil || len(partials) != 0 {
				t.Fatal("left restore partials")
			}
		})
	}
}

func TestIndexAndManifestMismatch(t *testing.T) {
	source, cache := fixture(t)
	result, err := pack(source, cache, []string{"evidence"})
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*entry){
		func(item *entry) { item.SHA256 = strings.Repeat("0", 64) },
		func(item *entry) { item.ContentManifestSHA256 = strings.Repeat("0", 64) },
		func(item *entry) { item.Files++ },
		func(item *entry) { item.UnpackedBytes++ },
		func(item *entry) { item.ArchiveBytes++ },
	} {
		item := result.Entries[0]
		mutate(&item)
		if err := inspectArchive(cache, item, ""); err == nil {
			t.Fatal("accepted mismatch")
		}
	}
	for _, mutate := range []func(*index){
		func(result *index) { result.Schema = 2 },
		func(result *index) { result.Entries = nil },
		func(result *index) { result.Entries = append(result.Entries, result.Entries[0]) },
		func(result *index) { result.Entries[0].Filename = "../escape.tar.gz" },
		func(result *index) { result.Entries[0].UnpackedBytes = maxBytes + 1 },
		func(result *index) { result.Entries[0].Files = maxFiles + 1 },
		func(result *index) { result.Root = "/absolute" },
		func(result *index) {
			second := result.Entries[0]
			second.ID, second.Bundle, second.Filename = "inventory-evidence", "inventory-evidence", "inventory-evidence.tar.gz"
			result.Entries[0].Files, second.Files = maxFiles, 1
			result.Entries = append(result.Entries, second)
		},
		func(result *index) {
			second := result.Entries[0]
			second.ID, second.Bundle, second.Filename = "inventory-evidence", "inventory-evidence", "inventory-evidence.tar.gz"
			result.Entries[0].UnpackedBytes, second.UnpackedBytes = maxBytes, 1
			result.Entries = append(result.Entries, second)
		},
	} {
		changed := result
		changed.Entries = append([]entry(nil), result.Entries...)
		mutate(&changed)
		if err := validateIndex(changed); err == nil {
			t.Fatal("accepted malformed index")
		}
	}
}

func TestRestorePermissionsAndExistingTargets(t *testing.T) {
	source, cache := fixture(t)
	result, err := pack(source, cache, []string{"evidence"})
	if err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(filepath.Dir(source), "restored")
	if err := restore(result, cache, dest, map[string]bool{"evidence": true}); err != nil {
		t.Fatal(err)
	}
	for name, expected := range map[string]os.FileMode{dest: 0700, filepath.Join(dest, "evidence"): 0700, filepath.Join(dest, "evidence", "sample.json"): 0600} {
		info, err := os.Stat(name)
		if err != nil || info.Mode().Perm() != expected {
			t.Fatalf("permissions %s: %v", name, err)
		}
	}
	for _, kind := range []string{"file", "symlink", "empty-directory"} {
		target := filepath.Join(filepath.Dir(source), kind)
		switch kind {
		case "file":
			err = os.WriteFile(target, []byte("keep"), 0600)
		case "symlink":
			err = os.Symlink(dest, target)
		case "empty-directory":
			err = os.Mkdir(target, 0700)
		}
		if err != nil {
			t.Fatal(err)
		}
		before, err := os.Lstat(target)
		if err != nil {
			t.Fatal(err)
		}
		if err := restore(result, cache, target, map[string]bool{"evidence": true}); err == nil {
			t.Fatal("overwrote target")
		}
		after, err := os.Lstat(target)
		if err != nil || !os.SameFile(before, after) {
			t.Fatal("changed existing target")
		}
	}
}
