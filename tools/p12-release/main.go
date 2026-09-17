// Command p12-release creates deterministic source and Go module release artifacts.
package main

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"
)

const modulePath = "github.com/otuschhoff/go-librados"

var semanticVersion = regexp.MustCompile(`^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$`)

type sourceFile struct {
	path string
	data []byte
}

type spdxDocument struct {
	SPDXVersion       string             `json:"spdxVersion"`
	DataLicense       string             `json:"dataLicense"`
	SPDXID            string             `json:"SPDXID"`
	Name              string             `json:"name"`
	DocumentNamespace string             `json:"documentNamespace"`
	CreationInfo      spdxCreationInfo   `json:"creationInfo"`
	Packages          []spdxPackage      `json:"packages"`
	Files             []spdxFile         `json:"files"`
	Relationships     []spdxRelationship `json:"relationships"`
}

type spdxCreationInfo struct {
	Created  string   `json:"created"`
	Creators []string `json:"creators"`
}

type spdxPackage struct {
	Name                    string         `json:"name"`
	SPDXID                  string         `json:"SPDXID"`
	VersionInfo             string         `json:"versionInfo"`
	DownloadLocation        string         `json:"downloadLocation"`
	FilesAnalyzed           bool           `json:"filesAnalyzed"`
	PackageVerificationCode *verification  `json:"packageVerificationCode,omitempty"`
	LicenseConcluded        string         `json:"licenseConcluded"`
	LicenseDeclared         string         `json:"licenseDeclared"`
	CopyrightText           string         `json:"copyrightText"`
	ExternalRefs            []externalRef  `json:"externalRefs"`
	Checksums               []spdxChecksum `json:"checksums,omitempty"`
}

type verification struct {
	Value string `json:"packageVerificationCodeValue"`
}

type externalRef struct {
	Category string `json:"referenceCategory"`
	Type     string `json:"referenceType"`
	Locator  string `json:"referenceLocator"`
}

type spdxFile struct {
	FileName           string         `json:"fileName"`
	SPDXID             string         `json:"SPDXID"`
	Checksums          []spdxChecksum `json:"checksums"`
	LicenseConcluded   string         `json:"licenseConcluded"`
	LicenseInfoInFiles []string       `json:"licenseInfoInFiles"`
	CopyrightText      string         `json:"copyrightText"`
}

type spdxChecksum struct {
	Algorithm string `json:"algorithm"`
	Value     string `json:"checksumValue"`
}

type spdxRelationship struct {
	Element string `json:"spdxElementId"`
	Type    string `json:"relationshipType"`
	Related string `json:"relatedSpdxElement"`
}

type dependency struct {
	name    string
	version string
	license string
}

var productionDependencies = []dependency{
	{name: "github.com/jcmturner/aescts/v2", version: "v2.0.0", license: "Apache-2.0"},
	{name: "github.com/jcmturner/gofork", version: "v1.7.6", license: "BSD-3-Clause"},
	{name: "github.com/otuschhoff/gokrb5/v8", version: "v8.5.3", license: "Apache-2.0"},
	{name: "golang.org/x/crypto", version: "v0.56.0", license: "BSD-3-Clause"},
}

func main() {
	root := flag.String("root", ".", "repository root")
	out := flag.String("out", "dist", "artifact output directory")
	version := flag.String("version", "", "semantic version including leading v")
	flag.Parse()
	if err := generate(*root, *out, *version); err != nil {
		fmt.Fprintf(os.Stderr, "p12-release: %v\n", err)
		os.Exit(1)
	}
}

func generate(root, out, version string) error {
	if !semanticVersion.MatchString(version) {
		return fmt.Errorf("version %q is not a supported semantic version", version)
	}
	files, err := collectFiles(root)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}
	base := "go-librados-" + version
	sourcePath := filepath.Join(out, base+".tar.gz")
	modulePath := filepath.Join(out, base+".zip")
	sbomPath := filepath.Join(out, base+".spdx.json")
	if err := writeSourceArchive(sourcePath, base, files); err != nil {
		return err
	}
	if err := writeModuleArchive(modulePath, version, files); err != nil {
		return err
	}
	if err := validateModuleArchive(modulePath, version); err != nil {
		return err
	}
	if err := writeSBOM(sbomPath, version, files); err != nil {
		return err
	}
	return writeChecksums(filepath.Join(out, "SHA256SUMS"), []string{sourcePath, modulePath, sbomPath})
}

func collectFiles(root string) ([]sourceFile, error) {
	names := append([]string(nil), "LICENSE", "THIRD_PARTY_NOTICES", "README.md", "SECURITY.md", "go.mod", "go.sum")
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, fmt.Errorf("read repository root: %w", err)
	}
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".go") {
			names = append(names, entry.Name())
		}
	}
	for _, directory := range []string{"internal", "examples"} {
		err := filepath.WalkDir(filepath.Join(root, directory), func(filePath string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.Type()&os.ModeSymlink != 0 {
				return fmt.Errorf("release input contains symlink %q", filePath)
			}
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
				return nil
			}
			relative, err := filepath.Rel(root, filePath)
			if err != nil {
				return err
			}
			names = append(names, filepath.ToSlash(relative))
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("walk %s: %w", directory, err)
		}
	}
	slices.Sort(names)
	files := make([]sourceFile, 0, len(names))
	seen := make(map[string]bool, len(names))
	for _, name := range names {
		if err := validateRelativePath(name); err != nil {
			return nil, err
		}
		if seen[strings.ToLower(name)] {
			return nil, fmt.Errorf("case-insensitive duplicate release path %q", name)
		}
		seen[strings.ToLower(name)] = true
		fullPath := filepath.Join(root, filepath.FromSlash(name))
		info, err := os.Lstat(fullPath)
		if err != nil {
			return nil, fmt.Errorf("inspect required release file %q: %w", name, err)
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("release input %q is not a regular file", name)
		}
		data, err := os.ReadFile(fullPath)
		if err != nil {
			return nil, fmt.Errorf("read release file %q: %w", name, err)
		}
		files = append(files, sourceFile{path: name, data: data})
	}
	return files, nil
}

func validateRelativePath(name string) error {
	if name == "" || name != path.Clean(name) || strings.HasPrefix(name, "/") || strings.Contains(name, "\\") || name == ".." || strings.HasPrefix(name, "../") {
		return fmt.Errorf("unsafe release path %q", name)
	}
	return nil
}

func writeSourceArchive(output, prefix string, files []sourceFile) (returnErr error) {
	file, err := os.Create(output)
	if err != nil {
		return fmt.Errorf("create source archive: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, file.Close()) }()
	gzipWriter, err := gzip.NewWriterLevel(file, gzip.BestCompression)
	if err != nil {
		return err
	}
	gzipWriter.Header = gzip.Header{ModTime: time.Unix(0, 0).UTC(), OS: 255}
	tarWriter := tar.NewWriter(gzipWriter)
	for _, item := range files {
		header := &tar.Header{Name: prefix + "/" + item.path, Mode: 0o644, Size: int64(len(item.data)), ModTime: time.Unix(0, 0).UTC(), Typeflag: tar.TypeReg, Format: tar.FormatPAX}
		if err := tarWriter.WriteHeader(header); err != nil {
			return err
		}
		if _, err := tarWriter.Write(item.data); err != nil {
			return err
		}
	}
	return errors.Join(tarWriter.Close(), gzipWriter.Close())
}

func writeModuleArchive(output, version string, files []sourceFile) (returnErr error) {
	file, err := os.Create(output)
	if err != nil {
		return fmt.Errorf("create module archive: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, file.Close()) }()
	writer := zip.NewWriter(file)
	prefix := modulePath + "@" + version + "/"
	for _, item := range files {
		header := &zip.FileHeader{Name: prefix + item.path, Method: zip.Deflate}
		header.SetMode(0o644)
		header.Modified = time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)
		entry, err := writer.CreateHeader(header)
		if err != nil {
			return err
		}
		if _, err := entry.Write(item.data); err != nil {
			return err
		}
	}
	return writer.Close()
}

func validateModuleArchive(archivePath, version string) error {
	reader, err := zip.OpenReader(archivePath)
	if err != nil {
		return fmt.Errorf("open module archive: %w", err)
	}
	defer reader.Close()
	prefix := modulePath + "@" + version + "/"
	seen := make(map[string]bool, len(reader.File))
	for _, entry := range reader.File {
		if entry.Mode()&os.ModeSymlink != 0 || entry.FileInfo().IsDir() {
			return fmt.Errorf("module archive contains non-regular entry %q", entry.Name)
		}
		if !strings.HasPrefix(entry.Name, prefix) {
			return fmt.Errorf("module archive entry %q has invalid prefix", entry.Name)
		}
		relative := strings.TrimPrefix(entry.Name, prefix)
		if err := validateRelativePath(relative); err != nil {
			return err
		}
		folded := strings.ToLower(relative)
		if seen[folded] {
			return fmt.Errorf("module archive contains duplicate path %q", relative)
		}
		seen[folded] = true
	}
	if !seen["go.mod"] || !seen["license"] || !seen["third_party_notices"] {
		return errors.New("module archive is missing required metadata")
	}
	return nil
}

func writeSBOM(output, version string, files []sourceFile) error {
	document := spdxDocument{
		SPDXVersion: "SPDX-2.3", DataLicense: "CC0-1.0", SPDXID: "SPDXRef-DOCUMENT",
		Name:              "go-librados-" + version,
		DocumentNamespace: "https://github.com/otuschhoff/go-librados/releases/" + version + "/spdx",
		CreationInfo:      spdxCreationInfo{Created: "1970-01-01T00:00:00Z", Creators: []string{"Tool: go-librados-p12-release"}},
	}
	verificationHashes := make([]string, 0, len(files))
	for index, item := range files {
		digest := sha256.Sum256(item.data)
		hexDigest := hex.EncodeToString(digest[:])
		verificationHashes = append(verificationHashes, hexDigest)
		id := fmt.Sprintf("SPDXRef-File-%d", index+1)
		document.Files = append(document.Files, spdxFile{FileName: "./" + item.path, SPDXID: id, Checksums: []spdxChecksum{{Algorithm: "SHA256", Value: hexDigest}}, LicenseConcluded: "NOASSERTION", LicenseInfoInFiles: []string{"NOASSERTION"}, CopyrightText: "NOASSERTION"})
		document.Relationships = append(document.Relationships, spdxRelationship{Element: "SPDXRef-Package", Type: "CONTAINS", Related: id})
	}
	slices.Sort(verificationHashes)
	verificationDigest := sha256.Sum256([]byte(strings.Join(verificationHashes, "")))
	document.Packages = []spdxPackage{{
		Name: "go-librados", SPDXID: "SPDXRef-Package", VersionInfo: version,
		DownloadLocation: "https://github.com/otuschhoff/go-librados/releases/tag/" + version,
		FilesAnalyzed:    true, PackageVerificationCode: &verification{Value: hex.EncodeToString(verificationDigest[:])},
		LicenseConcluded: "LGPL-2.1-only", LicenseDeclared: "LGPL-2.1-only", CopyrightText: "NOASSERTION",
		ExternalRefs: []externalRef{{Category: "PACKAGE-MANAGER", Type: "purl", Locator: "pkg:golang/github.com/otuschhoff/go-librados@" + version}},
	}}
	document.Relationships = append([]spdxRelationship{{Element: "SPDXRef-DOCUMENT", Type: "DESCRIBES", Related: "SPDXRef-Package"}}, document.Relationships...)
	for index, item := range productionDependencies {
		id := fmt.Sprintf("SPDXRef-Dependency-%d", index+1)
		document.Packages = append(document.Packages, spdxPackage{
			Name: item.name, SPDXID: id, VersionInfo: item.version, DownloadLocation: "https://" + item.name,
			FilesAnalyzed: false, LicenseConcluded: item.license, LicenseDeclared: item.license, CopyrightText: "NOASSERTION",
			ExternalRefs: []externalRef{{Category: "PACKAGE-MANAGER", Type: "purl", Locator: "pkg:golang/" + item.name + "@" + item.version}},
		})
		document.Relationships = append(document.Relationships, spdxRelationship{Element: "SPDXRef-Package", Type: "DEPENDS_ON", Related: id})
	}
	data, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(output, data, 0o644)
}

func writeChecksums(output string, paths []string) error {
	slices.Sort(paths)
	var builder strings.Builder
	for _, artifactPath := range paths {
		data, err := os.ReadFile(artifactPath)
		if err != nil {
			return err
		}
		digest := sha256.Sum256(data)
		fmt.Fprintf(&builder, "%x  %s\n", digest, filepath.Base(artifactPath))
	}
	return os.WriteFile(output, []byte(builder.String()), 0o644)
}
