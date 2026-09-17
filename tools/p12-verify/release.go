package main

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

const (
	releaseArtifactsPath = "docs/p12/release-artifacts"
	releaseModulePath    = "github.com/otuschhoff/go-librados"
)

type releaseSource struct {
	name string
	data []byte
}

type releaseSPDXDocument struct {
	SPDXVersion       string                    `json:"spdxVersion"`
	DataLicense       string                    `json:"dataLicense"`
	SPDXID            string                    `json:"SPDXID"`
	Name              string                    `json:"name"`
	DocumentNamespace string                    `json:"documentNamespace"`
	CreationInfo      releaseSPDXCreationInfo   `json:"creationInfo"`
	Packages          []releaseSPDXPackage      `json:"packages"`
	Files             []releaseSPDXFile         `json:"files"`
	Relationships     []releaseSPDXRelationship `json:"relationships"`
}

type releaseSPDXCreationInfo struct {
	Created  string   `json:"created"`
	Creators []string `json:"creators"`
}

type releaseSPDXPackage struct {
	Name                    string                `json:"name"`
	SPDXID                  string                `json:"SPDXID"`
	VersionInfo             string                `json:"versionInfo"`
	DownloadLocation        string                `json:"downloadLocation"`
	FilesAnalyzed           bool                  `json:"filesAnalyzed"`
	PackageVerificationCode *releaseVerification  `json:"packageVerificationCode,omitempty"`
	LicenseConcluded        string                `json:"licenseConcluded"`
	LicenseDeclared         string                `json:"licenseDeclared"`
	CopyrightText           string                `json:"copyrightText"`
	ExternalRefs            []releaseExternalRef  `json:"externalRefs"`
	Checksums               []releaseSPDXChecksum `json:"checksums,omitempty"`
}

type releaseVerification struct {
	Value string `json:"packageVerificationCodeValue"`
}

type releaseExternalRef struct {
	Category string `json:"referenceCategory"`
	Type     string `json:"referenceType"`
	Locator  string `json:"referenceLocator"`
}

type releaseSPDXFile struct {
	FileName           string                `json:"fileName"`
	SPDXID             string                `json:"SPDXID"`
	Checksums          []releaseSPDXChecksum `json:"checksums"`
	LicenseConcluded   string                `json:"licenseConcluded"`
	LicenseInfoInFiles []string              `json:"licenseInfoInFiles"`
	CopyrightText      string                `json:"copyrightText"`
}

type releaseSPDXChecksum struct {
	Algorithm string `json:"algorithm"`
	Value     string `json:"checksumValue"`
}

type releaseSPDXRelationship struct {
	Element string `json:"spdxElementId"`
	Type    string `json:"relationshipType"`
	Related string `json:"relatedSpdxElement"`
}

type releaseDependency struct {
	name    string
	version string
	license string
}

var releaseDependencies = []releaseDependency{
	{name: "github.com/jcmturner/aescts/v2", version: "v2.0.0", license: "Apache-2.0"},
	{name: "github.com/jcmturner/gofork", version: "v1.7.6", license: "BSD-3-Clause"},
	{name: "github.com/otuschhoff/gokrb5/v8", version: "v8.5.3", license: "Apache-2.0"},
	{name: "golang.org/x/crypto", version: "v0.56.0", license: "BSD-3-Clause"},
}

func validateReleaseArtifacts(root, version string, reported map[string]string) error {
	base := "go-librados-" + version
	names := []string{base + ".spdx.json", base + ".tar.gz", base + ".zip", "SHA256SUMS"}
	directory := filepath.Join(root, filepath.FromSlash(releaseArtifactsPath))
	entries, err := os.ReadDir(directory)
	if err != nil {
		return fmt.Errorf("read retained release artifacts: %w", err)
	}
	if len(entries) != len(names) || len(reported) != len(names) {
		return fmt.Errorf("retained release has %d directory entries and %d reported entries, want exactly four", len(entries), len(reported))
	}
	wanted := make(map[string]bool, len(names))
	for _, name := range names {
		wanted[name] = true
	}
	for _, entry := range entries {
		if !wanted[entry.Name()] || entry.Type()&os.ModeSymlink != 0 || !entry.Type().IsRegular() {
			return fmt.Errorf("retained release contains unexpected or non-regular file %q", entry.Name())
		}
	}
	bytesByName := make(map[string][]byte, len(names))
	for _, name := range names {
		data, err := os.ReadFile(filepath.Join(directory, name))
		if err != nil {
			return fmt.Errorf("read retained release artifact %q: %w", name, err)
		}
		digest := sha256.Sum256(data)
		if reported[name] != hex.EncodeToString(digest[:]) {
			return fmt.Errorf("retained release artifact hash mismatch for %s", name)
		}
		bytesByName[name] = data
	}
	sources, err := collectReleaseSources(root)
	if err != nil {
		return err
	}
	if err := validateReleaseChecksums(bytesByName["SHA256SUMS"], names[:3], bytesByName); err != nil {
		return err
	}
	if err := validateTarGzip(bytesByName[base+".tar.gz"], base+"/", sources); err != nil {
		return err
	}
	if err := validateModuleZip(bytesByName[base+".zip"], releaseModulePath+"@"+version+"/", sources); err != nil {
		return err
	}
	return validateReleaseSPDX(bytesByName[base+".spdx.json"], version, sources)
}

func collectReleaseSources(root string) ([]releaseSource, error) {
	names := []string{"LICENSE", "THIRD_PARTY_NOTICES", "README.md", "SECURITY.md", "go.mod", "go.sum"}
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, fmt.Errorf("read release source root: %w", err)
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
				return fmt.Errorf("release source contains symlink %q", filePath)
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
			return nil, fmt.Errorf("walk release sources %s: %w", directory, err)
		}
	}
	slices.Sort(names)
	sources := make([]releaseSource, 0, len(names))
	seen := make(map[string]bool, len(names))
	for _, name := range names {
		if err := validateReleasePath(name); err != nil {
			return nil, err
		}
		folded := strings.ToLower(name)
		if seen[folded] {
			return nil, fmt.Errorf("case-insensitive duplicate release source %q", name)
		}
		seen[folded] = true
		fullPath := filepath.Join(root, filepath.FromSlash(name))
		info, err := os.Lstat(fullPath)
		if err != nil || !info.Mode().IsRegular() {
			return nil, fmt.Errorf("release source %q is missing or non-regular", name)
		}
		data, err := os.ReadFile(fullPath)
		if err != nil {
			return nil, fmt.Errorf("read release source %q: %w", name, err)
		}
		sources = append(sources, releaseSource{name: name, data: data})
	}
	return sources, nil
}

func validateReleaseChecksums(data []byte, artifactNames []string, artifacts map[string][]byte) error {
	var expected strings.Builder
	for _, name := range artifactNames {
		digest := sha256.Sum256(artifacts[name])
		fmt.Fprintf(&expected, "%x  %s\n", digest, name)
	}
	if !bytes.Equal(data, []byte(expected.String())) {
		return errors.New("SHA256SUMS entries, digests, order, or framing are invalid")
	}
	return nil
}

func validateTarGzip(data []byte, prefix string, sources []releaseSource) error {
	gzipReader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("open retained source tar.gz: %w", err)
	}
	defer gzipReader.Close()
	if !gzipReader.ModTime.IsZero() || gzipReader.OS != 255 || gzipReader.Name != "" || gzipReader.Comment != "" || len(gzipReader.Extra) != 0 {
		return errors.New("retained source tar.gz has noncanonical gzip metadata")
	}
	tarReader := tar.NewReader(gzipReader)
	return validateArchivedSources(func() (string, []byte, error) {
		header, err := tarReader.Next()
		if err != nil {
			return "", nil, err
		}
		if header.Typeflag != tar.TypeReg || header.Mode != 0o644 || header.Uid != 0 || header.Gid != 0 || header.Uname != "" || header.Gname != "" || !header.ModTime.Equal(time.Unix(0, 0).UTC()) || !header.AccessTime.IsZero() || !header.ChangeTime.IsZero() || header.Format != tar.FormatUSTAR {
			return header.Name, nil, fmt.Errorf("noncanonical tar metadata: type=%d mode=%#o uid=%d gid=%d uname=%q gname=%q modtime=%s accesstime=%s changetime=%s format=%d", header.Typeflag, header.Mode, header.Uid, header.Gid, header.Uname, header.Gname, header.ModTime.Format(time.RFC3339Nano), header.AccessTime.Format(time.RFC3339Nano), header.ChangeTime.Format(time.RFC3339Nano), header.Format)
		}
		entryData, err := io.ReadAll(tarReader)
		return header.Name, entryData, err
	}, prefix, sources, "source tar.gz")
}

func validateModuleZip(data []byte, prefix string, sources []releaseSource) error {
	reader, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return fmt.Errorf("open retained module zip: %w", err)
	}
	index := 0
	canonicalExtra := []byte{0x55, 0x54, 0x05, 0x00, 0x01, 0x00, 0xa6, 0xce, 0x12}
	return validateArchivedSources(func() (string, []byte, error) {
		if index == len(reader.File) {
			return "", nil, io.EOF
		}
		entry := reader.File[index]
		index++
		if entry.Mode() != 0o644 || entry.Method != zip.Deflate || !entry.Modified.Equal(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)) || entry.Comment != "" || !slices.Equal(entry.Extra, canonicalExtra) || entry.NonUTF8 {
			return entry.Name, nil, fmt.Errorf("noncanonical zip metadata: mode=%v method=%d modified=%s comment=%q extra=%x non_utf8=%t", entry.Mode(), entry.Method, entry.Modified.Format(time.RFC3339Nano), entry.Comment, entry.Extra, entry.NonUTF8)
		}
		file, err := entry.Open()
		if err != nil {
			return entry.Name, nil, err
		}
		entryData, readErr := io.ReadAll(file)
		return entry.Name, entryData, errors.Join(readErr, file.Close())
	}, prefix, sources, "module zip")
}

func validateArchivedSources(next func() (string, []byte, error), prefix string, sources []releaseSource, label string) error {
	expected := make(map[string][]byte, len(sources))
	for _, source := range sources {
		expected[source.name] = source.data
	}
	seen := make(map[string]bool, len(sources))
	for {
		name, data, err := next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("%s entry %q: %w", label, name, err)
		}
		if !strings.HasPrefix(name, prefix) {
			return fmt.Errorf("%s entry %q has invalid prefix", label, name)
		}
		relative := strings.TrimPrefix(name, prefix)
		if err := validateReleasePath(relative); err != nil {
			return fmt.Errorf("%s: %w", label, err)
		}
		folded := strings.ToLower(relative)
		expectedData, required := expected[relative]
		if seen[folded] || !required || !bytes.Equal(data, expectedData) {
			return fmt.Errorf("%s has duplicate, unexpected, or mismatched entry %q", label, relative)
		}
		seen[folded] = true
	}
	if len(seen) != len(expected) {
		return fmt.Errorf("%s has %d source entries, want exactly %d", label, len(seen), len(expected))
	}
	return nil
}

func validateReleaseSPDX(data []byte, version string, sources []releaseSource) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var document releaseSPDXDocument
	if err := decoder.Decode(&document); err != nil {
		return fmt.Errorf("decode strict retained SPDX JSON: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("retained SPDX JSON has trailing content")
	}
	if document.SPDXVersion != "SPDX-2.3" || document.DataLicense != "CC0-1.0" || document.SPDXID != "SPDXRef-DOCUMENT" || document.Name != "go-librados-"+version || document.DocumentNamespace != "https://github.com/otuschhoff/go-librados/releases/"+version+"/spdx" || document.CreationInfo.Created != "1970-01-01T00:00:00Z" || !slices.Equal(document.CreationInfo.Creators, []string{"Tool: go-librados-p12-release"}) {
		return errors.New("retained SPDX document has invalid SPDX 2.3 identity")
	}
	if len(document.Files) != len(sources) || len(document.Packages) != 1+len(releaseDependencies) {
		return errors.New("retained SPDX document has missing or extra files or packages")
	}
	verificationHashes := make([]string, 0, len(sources))
	for index, source := range sources {
		file := document.Files[index]
		digest := sha256.Sum256(source.data)
		hexDigest := hex.EncodeToString(digest[:])
		verificationHashes = append(verificationHashes, hexDigest)
		if file.FileName != "./"+source.name || file.SPDXID != fmt.Sprintf("SPDXRef-File-%d", index+1) || !slices.Equal(file.Checksums, []releaseSPDXChecksum{{Algorithm: "SHA256", Value: hexDigest}}) || file.LicenseConcluded != "NOASSERTION" || !slices.Equal(file.LicenseInfoInFiles, []string{"NOASSERTION"}) || file.CopyrightText != "NOASSERTION" {
			return fmt.Errorf("retained SPDX has invalid source entry %q", file.FileName)
		}
	}
	slices.Sort(verificationHashes)
	verificationDigest := sha256.Sum256([]byte(strings.Join(verificationHashes, "")))
	rootPackage := document.Packages[0]
	rootRef := []releaseExternalRef{{Category: "PACKAGE-MANAGER", Type: "purl", Locator: "pkg:golang/github.com/otuschhoff/go-librados@" + version}}
	if rootPackage.Name != "go-librados" || rootPackage.SPDXID != "SPDXRef-Package" || rootPackage.VersionInfo != version || rootPackage.DownloadLocation != "https://github.com/otuschhoff/go-librados/releases/tag/"+version || !rootPackage.FilesAnalyzed || rootPackage.LicenseConcluded != "LGPL-2.1-only" || rootPackage.LicenseDeclared != "LGPL-2.1-only" || rootPackage.CopyrightText != "NOASSERTION" || rootPackage.PackageVerificationCode == nil || rootPackage.PackageVerificationCode.Value != hex.EncodeToString(verificationDigest[:]) || !slices.Equal(rootPackage.ExternalRefs, rootRef) || len(rootPackage.Checksums) != 0 {
		return errors.New("retained SPDX root package has invalid identity, version, or license")
	}
	for index, dependency := range releaseDependencies {
		value := document.Packages[index+1]
		dependencyRef := []releaseExternalRef{{Category: "PACKAGE-MANAGER", Type: "purl", Locator: "pkg:golang/" + dependency.name + "@" + dependency.version}}
		if value.Name != dependency.name || value.VersionInfo != dependency.version || value.SPDXID != fmt.Sprintf("SPDXRef-Dependency-%d", index+1) || value.DownloadLocation != "https://"+dependency.name || value.FilesAnalyzed || value.PackageVerificationCode != nil || value.LicenseConcluded != dependency.license || value.LicenseDeclared != dependency.license || value.CopyrightText != "NOASSERTION" || !slices.Equal(value.ExternalRefs, dependencyRef) || len(value.Checksums) != 0 {
			return fmt.Errorf("retained SPDX has invalid production dependency %q", value.Name)
		}
	}
	wantRelationships := []releaseSPDXRelationship{{Element: "SPDXRef-DOCUMENT", Type: "DESCRIBES", Related: "SPDXRef-Package"}}
	for index := range sources {
		wantRelationships = append(wantRelationships, releaseSPDXRelationship{Element: "SPDXRef-Package", Type: "CONTAINS", Related: fmt.Sprintf("SPDXRef-File-%d", index+1)})
	}
	for index := range releaseDependencies {
		wantRelationships = append(wantRelationships, releaseSPDXRelationship{Element: "SPDXRef-Package", Type: "DEPENDS_ON", Related: fmt.Sprintf("SPDXRef-Dependency-%d", index+1)})
	}
	if !slices.Equal(document.Relationships, wantRelationships) {
		return errors.New("retained SPDX document has missing, extra, or invalid relationships")
	}
	return nil
}

func validateReleasePath(name string) error {
	if name == "" || name != path.Clean(name) || strings.HasPrefix(name, "/") || strings.Contains(name, "\\") || name == ".." || strings.HasPrefix(name, "../") {
		return fmt.Errorf("unsafe release path %q", name)
	}
	return nil
}
