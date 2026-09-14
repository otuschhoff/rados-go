// Command p05-verify validates the checked-in exact-placement evidence.
package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const (
	repository = "https://github.com/ceph/ceph.git"
	commit     = "7f793731f1b39eb4f465e960113d2363c311b964"
	image      = "quay.io/ceph/ceph@sha256:09ee90f6f3e0c7b9954f71d214ee05e9bbaaaea3716b1dd619603283b829f8b8"
)

type manifest struct {
	SchemaVersion int    `json:"schema_version"`
	Fixture       string `json:"fixture"`
	SHA256        string `json:"sha256"`
	Kind          string `json:"kind"`
	Source        struct {
		Repository string            `json:"repository"`
		Commit     string            `json:"commit"`
		Paths      []string          `json:"paths"`
		Files      map[string]string `json:"files"`
		References []struct {
			Type, Locator, Revision, Role string
		} `json:"references"`
	} `json:"source"`
	Generator struct {
		Tool, Version, Image string
		Command              []string
	} `json:"generator"`
	Secrets struct {
		ContainsSecrets bool `json:"contains_secrets"`
		SyntheticOnly   bool `json:"synthetic_only"`
	} `json:"secrets"`
	License struct {
		UpstreamExpression string `json:"upstream_expression"`
		Redistribution     string `json:"redistribution"`
		ReviewedBy         string `json:"reviewed_by"`
	} `json:"license"`
}

var sourceHashes = map[string]string{
	"src/crush/CrushWrapper.cc":  "f6d6f3517fd64768070c44ed27cdfc19447660815bf67e9c0c87dfb2fe910e52",
	"src/crush/mapper.c":         "d320fffa1a4b4b1c2d25b1c966f928b3d5ba2636889261054177cc37c41dbef7",
	"src/crush/hash.c":           "65167638f2da5af1b27ff78d1e2b148bf22ff435144c4d8f583db6728fb8a20d",
	"src/crush/crush_ln_table.h": "9d0cdacacbb4a21da36f134368e41e835fca1d5d6058e6abebb85775339e1b1a",
	"src/osd/OSDMap.cc":          "36b74d8c998da9c93b7b0c9ba3ecac5d3e9df26bfb99c2f22095b37da44e0dc8",
	"src/osd/osd_types.cc":       "614710847acaebd82e17787b91b38e07198eec58488cd9f6d7283499e0fa6d3b",
}

func main() {
	paths, err := filepath.Glob("testdata/p05/*.manifest.json")
	must(err)
	if len(paths) != 8 {
		fatalf("found %d P05 manifests, want 8", len(paths))
	}
	for _, path := range paths {
		checkManifest(path)
	}
	checkRows("testdata/p05/mappings.txt", 256)
	checkRows("testdata/p05/mappings-osd1-out.txt", 256)
	checkRows("testdata/p05/object-mappings.txt", 128)
	checkRows("testdata/p05/object-mappings-pg32.txt", 128)
	checkRows("testdata/p05/object-mappings-osd1-out.txt", 128)
	checkRows("testdata/p05/object-mappings-upmap.txt", 128)
	checkRows("testdata/p05/upmap-commands.txt", 20)
	fmt.Println("P05 verification passed: 1024 pinned placement rows")
}

func checkManifest(path string) {
	file, err := os.Open(path)
	must(err)
	defer file.Close()
	var value manifest
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	must(decoder.Decode(&value))
	data, err := os.ReadFile(filepath.Join(filepath.Dir(path), value.Fixture))
	must(err)
	sum := sha256.Sum256(data)
	if value.SchemaVersion != 1 || value.Source.Repository != repository || value.Source.Commit != commit || value.SHA256 != hex.EncodeToString(sum[:]) || value.Generator.Version != "ceph version 20.2.4" || value.Generator.Image != image || len(value.Generator.Command) != 1 || value.Generator.Command[0] != "./integration/p05/reproduce.sh" || value.Secrets.ContainsSecrets || !value.Secrets.SyntheticOnly || value.License.Redistribution != "pending" {
		fatalf("invalid manifest identity: %s", path)
	}
	if len(value.Source.Paths) == 0 || len(value.Source.Paths) != len(value.Source.Files) || len(value.Source.References) != 1 {
		fatalf("incomplete provenance: %s", path)
	}
	for _, sourcePath := range value.Source.Paths {
		if value.Source.Files[sourcePath] != sourceHashes[sourcePath] || sourceHashes[sourcePath] == "" {
			fatalf("missing source hash %s: %s", sourcePath, path)
		}
	}
	reference := value.Source.References[0]
	if reference.Type != "ceph-source" || reference.Locator != repository || reference.Revision != commit || reference.Role == "" {
		fatalf("invalid source reference: %s", path)
	}
}

func checkRows(path string, expected int) {
	file, err := os.Open(path)
	must(err)
	defer file.Close()
	scanner := bufio.NewScanner(file)
	count := 0
	for scanner.Scan() {
		if scanner.Text() == "" {
			fatalf("empty oracle row: %s", path)
		}
		count++
	}
	must(scanner.Err())
	if count != expected {
		fatalf("%s has %d rows, want %d", path, count, expected)
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func fatalf(format string, values ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", values...)
	os.Exit(1)
}
