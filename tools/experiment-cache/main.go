package main

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"
)

const (
	manifestName          = "__cache_manifest.json"
	maxManifest           = 8 << 20
	maxFiles              = 100000
	maxBytes        int64 = 5 << 30
	maxArchiveBytes int64 = maxBytes + 256<<20
	defaultCache          = "/Users/oli/proj/rados-go-experiment-cache"
)

var defaultBundles = []string{"evidence", "batching-evidence", "read-into-evidence", "inventory-evidence", "read-scale-evidence", "request-path-evidence", "factorial-stall-evidence"}

type index struct {
	Schema   int      `json:"schema"`
	Root     string   `json:"root"`
	CodeDirs []string `json:"code_dirs"`
	Entries  []entry  `json:"entries"`
}

type entry struct {
	ID                    string `json:"id"`
	Bundle                string `json:"bundle"`
	Filename              string `json:"filename"`
	SHA256                string `json:"sha256"`
	ArchiveBytes          int64  `json:"archive_bytes"`
	Files                 int    `json:"files"`
	UnpackedBytes         int64  `json:"unpacked_bytes"`
	ContentManifestSHA256 string `json:"content_manifest_sha256"`
}

type record struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

type manifest struct {
	Schema      int      `json:"schema"`
	Bundle      string   `json:"bundle"`
	Files       []record `json:"files"`
	Directories []string `json:"directories"`
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "experiment-cache:", err)
		os.Exit(1)
	}
}

func run(args []string, output io.Writer) error {
	if len(args) == 0 {
		return errors.New("expected pack, verify, or restore")
	}
	flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	cache := flags.String("cache-dir", defaultCache, "external archive directory")
	selection := flags.String("bundles", "", "comma-separated bundle names")
	var source, indexPath, dest *string
	switch args[0] {
	case "pack":
		source = flags.String("source", "docs/performance-p99-scheduler", "source root")
	case "verify", "restore":
		indexPath = flags.String("index", "", "archive index JSON")
		if args[0] == "restore" {
			dest = flags.String("dest", "", "fresh destination (reserved before extraction, not atomically published)")
		}
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if source != nil {
		bundles, err := selectBundles(*selection, defaultBundles)
		if err != nil {
			return err
		}
		result, err := pack(*source, *cache, bundles)
		if err != nil {
			return err
		}
		return json.NewEncoder(output).Encode(result)
	}
	if *indexPath == "" {
		return errors.New("--index is required")
	}
	result, err := loadIndex(*indexPath)
	if err != nil {
		return err
	}
	available := make([]string, len(result.Entries))
	for position, item := range result.Entries {
		available[position] = item.Bundle
	}
	bundles, err := selectBundles(*selection, available)
	if err != nil {
		return err
	}
	selected := make(map[string]bool)
	for _, bundle := range bundles {
		selected[bundle] = true
	}
	if dest != nil {
		if *dest == "" {
			return errors.New("--dest is required")
		}
		if err := restore(result, *cache, *dest, selected); err != nil {
			return err
		}
	} else {
		for _, item := range result.Entries {
			if selected[item.Bundle] {
				if err := inspectArchive(*cache, item, ""); err != nil {
					return fmt.Errorf("%s: %w", item.Bundle, err)
				}
			}
		}
	}
	_, err = fmt.Fprintf(output, "%s: validated %d bundles\n", args[0], len(bundles))
	return err
}

func safePath(name string) error {
	if name == "" || !utf8.ValidString(name) || strings.ContainsAny(name, "\\\x00") || path.IsAbs(name) || path.Clean(name) != name {
		return fmt.Errorf("unsafe path %q", name)
	}
	for _, part := range strings.Split(name, "/") {
		if part == "" || part == "." || part == ".." || strings.Contains(part, ":") {
			return fmt.Errorf("unsafe path %q", name)
		}
	}
	return nil
}

func selectBundles(selection string, available []string) ([]string, error) {
	allowed := make(map[string]bool)
	for _, name := range available {
		allowed[name] = true
	}
	result := append([]string(nil), available...)
	if selection != "" {
		result = strings.Split(selection, ",")
	}
	seen := make(map[string]bool)
	for _, name := range result {
		if err := safePath(name); err != nil {
			return nil, err
		}
		if strings.Contains(name, "/") || !allowed[name] || seen[name] {
			return nil, fmt.Errorf("unknown or duplicate bundle %q", name)
		}
		seen[name] = true
	}
	if len(result) == 0 {
		return nil, errors.New("no bundles selected")
	}
	sort.Strings(result)
	return result, nil
}

func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func validDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && value == strings.ToLower(value)
}

func decodeJSON(reader io.Reader, value any) error {
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return errors.New("trailing JSON data")
	}
	return nil
}

func loadIndex(filename string) (index, error) {
	var result index
	file, err := os.Open(filename)
	if err != nil {
		return result, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxManifest+1))
	if err != nil {
		return result, err
	}
	if len(data) > maxManifest {
		return result, errors.New("index exceeds bounds")
	}
	if err := decodeJSON(bytes.NewReader(data), &result); err != nil {
		return result, err
	}
	return result, validateIndex(result)
}

func validateIndex(result index) error {
	if result.Schema != 1 || len(result.Entries) == 0 || len(result.Entries) > len(defaultBundles) {
		return errors.New("invalid index schema or entry count")
	}
	if err := safePath(result.Root); err != nil {
		return err
	}
	if len(result.CodeDirs) != 1 || result.CodeDirs[0] != "tools/experiment-cache" {
		return errors.New("invalid code_dirs")
	}
	seen := make(map[string]bool)
	var totalFiles int
	var totalBytes int64
	for _, item := range result.Entries {
		if _, err := selectBundles(item.Bundle, defaultBundles); err != nil {
			return err
		}
		if item.ID != item.Bundle || item.Filename != item.Bundle+".tar.gz" || seen[item.Bundle] {
			return errors.New("invalid or duplicate index entry")
		}
		seen[item.Bundle] = true
		if !validDigest(item.SHA256) || !validDigest(item.ContentManifestSHA256) || item.ArchiveBytes <= 0 || item.ArchiveBytes > maxArchiveBytes || item.Files < 0 || item.Files > maxFiles || item.UnpackedBytes < 0 || item.UnpackedBytes > maxBytes {
			return errors.New("invalid index digest or bounds")
		}
		if item.Files > maxFiles-totalFiles || item.UnpackedBytes > maxBytes-totalBytes {
			return errors.New("index totals exceed bounds")
		}
		totalFiles += item.Files
		totalBytes += item.UnpackedBytes
	}
	return nil
}

func regularFile(filename string) (*os.File, os.FileInfo, error) {
	info, err := os.Lstat(filename)
	if err != nil {
		return nil, nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, nil, fmt.Errorf("not a regular file: %s", filename)
	}
	if stat, ok := info.Sys().(*syscall.Stat_t); ok && stat.Nlink != 1 {
		return nil, nil, fmt.Errorf("hard-linked file: %s", filename)
	}
	file, err := os.Open(filename)
	if err != nil {
		return nil, nil, err
	}
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		file.Close()
		return nil, nil, errors.New("file changed while opening")
	}
	return file, opened, nil
}

func scanBundle(source, bundle string) (manifest, int64, error) {
	result := manifest{Schema: 1, Bundle: bundle, Files: []record{}, Directories: []string{}}
	var total int64
	root := filepath.Join(source, bundle)
	info, err := os.Lstat(root)
	if err != nil {
		return result, 0, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return result, 0, errors.New("bundle must be a directory")
	}
	err = filepath.WalkDir(root, func(filename string, dirEntry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(source, filename)
		if err != nil {
			return err
		}
		name := filepath.ToSlash(relative)
		if err := safePath(name); err != nil {
			return err
		}
		if dirEntry.IsDir() {
			result.Directories = append(result.Directories, name)
			if len(result.Directories)+len(result.Files) > maxFiles {
				return errors.New("too many paths")
			}
			return nil
		}
		file, info, err := regularFile(filename)
		if err != nil {
			return err
		}
		defer file.Close()
		if len(result.Files)+len(result.Directories) >= maxFiles || info.Size() > maxBytes-total {
			return errors.New("bundle exceeds bounds")
		}
		hasher := sha256.New()
		count, err := io.Copy(hasher, io.LimitReader(file, info.Size()+1))
		if err != nil {
			return err
		}
		if count != info.Size() {
			return errors.New("source size changed")
		}
		result.Files = append(result.Files, record{Path: name, SHA256: hex.EncodeToString(hasher.Sum(nil)), Size: count})
		total += count
		return nil
	})
	sort.Strings(result.Directories)
	sort.Slice(result.Files, func(left, right int) bool { return result.Files[left].Path < result.Files[right].Path })
	return result, total, err
}

func ensureAbsent(filename string) error {
	_, err := os.Lstat(filename)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("destination already exists: %s", filename)
}

func publishFile(temp, destination string) error {
	return os.Link(temp, destination)
}

func physicalPath(filename string) (string, error) {
	absolute, err := filepath.Abs(filename)
	if err != nil {
		return "", err
	}
	ancestor := absolute
	for {
		_, err := os.Lstat(ancestor)
		if err == nil {
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		parent := filepath.Dir(ancestor)
		if parent == ancestor {
			return "", err
		}
		ancestor = parent
	}
	resolved, err := filepath.EvalSymlinks(ancestor)
	if err != nil {
		return "", err
	}
	relative, err := filepath.Rel(ancestor, absolute)
	if err != nil {
		return "", err
	}
	return filepath.Join(resolved, relative), nil
}

func pack(source, cache string, bundles []string) (index, error) {
	root := filepath.ToSlash(filepath.Clean(source))
	if filepath.IsAbs(source) {
		root = filepath.Base(source)
	}
	result := index{Schema: 1, Root: root, CodeDirs: []string{"tools/experiment-cache"}, Entries: []entry{}}
	if err := safePath(root); err != nil {
		return result, err
	}
	selected, err := selectBundles(strings.Join(bundles, ","), defaultBundles)
	if err != nil {
		return result, err
	}
	bundles = selected
	sourceInfo, err := os.Lstat(source)
	if err != nil {
		return result, err
	}
	if !sourceInfo.IsDir() || sourceInfo.Mode()&os.ModeSymlink != 0 {
		return result, errors.New("source must be a directory, not a symlink")
	}
	sourceAbs, err := filepath.EvalSymlinks(source)
	if err != nil {
		return result, err
	}
	sourceAbs, err = filepath.Abs(sourceAbs)
	if err != nil {
		return result, err
	}
	cacheAbs, err := physicalPath(cache)
	if err != nil {
		return result, err
	}
	cacheAbs, err = filepath.Abs(cacheAbs)
	if err != nil {
		return result, err
	}
	boundary := sourceAbs
	for ancestor := sourceAbs; ; ancestor = filepath.Dir(ancestor) {
		if info, err := os.Stat(filepath.Join(ancestor, "go.mod")); err == nil && info.Mode().IsRegular() {
			boundary = ancestor
			break
		}
		if filepath.Dir(ancestor) == ancestor {
			break
		}
	}
	relative, err := filepath.Rel(boundary, cacheAbs)
	if err != nil {
		return result, err
	}
	if relative == "." || (relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))) {
		return result, errors.New("cache must be outside source and its Go project")
	}
	if err := os.MkdirAll(cache, 0700); err != nil {
		return result, err
	}
	stage, err := os.MkdirTemp(cache, ".experiment-cache-pack-*")
	if err != nil {
		return result, err
	}
	defer os.RemoveAll(stage)
	indexName := filepath.Join(cache, "archive-index.json")
	if err := ensureAbsent(indexName); err != nil {
		return result, err
	}
	for _, bundle := range bundles {
		if err := ensureAbsent(filepath.Join(cache, bundle+".tar.gz")); err != nil {
			return result, err
		}
	}
	for _, bundle := range bundles {
		item, err := packBundle(source, stage, bundle)
		if err != nil {
			return result, fmt.Errorf("%s: %w", bundle, err)
		}
		result.Entries = append(result.Entries, item)
		if err := validateIndex(result); err != nil {
			return result, err
		}
	}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return result, err
	}
	temp, err := os.CreateTemp(stage, ".index-*")
	if err != nil {
		return result, err
	}
	defer os.Remove(temp.Name())
	if _, err := temp.Write(append(data, '\n')); err != nil {
		temp.Close()
		return result, err
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return result, err
	}
	if err := temp.Close(); err != nil {
		return result, err
	}
	if err := publishFile(temp.Name(), filepath.Join(stage, "archive-index.json")); err != nil {
		return result, err
	}
	if err := os.Remove(temp.Name()); err != nil {
		return result, err
	}
	names := make([]string, 0, len(result.Entries)+1)
	for _, item := range result.Entries {
		names = append(names, item.Filename)
	}
	names = append(names, "archive-index.json")
	return result, publishPack(stage, cache, names)
}

func publishPack(stage, cache string, names []string) (err error) {
	published := make(map[string]os.FileInfo)
	defer func() {
		if err != nil {
			err = errors.Join(err, rollbackPack(published))
		}
	}()
	for _, name := range names {
		source := filepath.Join(stage, name)
		owned, statErr := os.Lstat(source)
		if statErr != nil {
			return statErr
		}
		destination := filepath.Join(cache, name)
		if err := publishFile(source, destination); err != nil {
			return err
		}
		published[destination] = owned
	}
	return nil
}

func rollbackPack(published map[string]os.FileInfo) error {
	var result error
	for filename, owned := range published {
		current, err := os.Lstat(filename)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			result = errors.Join(result, err)
		} else if os.SameFile(owned, current) {
			result = errors.Join(result, os.Remove(filename))
		}
	}
	return result
}

func packBundle(source, cache, bundle string) (entry, error) {
	item := entry{ID: bundle, Bundle: bundle, Filename: bundle + ".tar.gz"}
	contents, total, err := scanBundle(source, bundle)
	if err != nil {
		return item, err
	}
	data, err := json.Marshal(contents)
	if err != nil {
		return item, err
	}
	if len(data) > maxManifest {
		return item, errors.New("manifest exceeds bounds")
	}
	item.Files, item.UnpackedBytes, item.ContentManifestSHA256 = len(contents.Files), total, digest(data)
	temp, err := os.CreateTemp(cache, ".archive-*")
	if err != nil {
		return item, err
	}
	defer os.Remove(temp.Name())
	defer temp.Close()
	hasher := sha256.New()
	compressed := gzip.NewWriter(io.MultiWriter(temp, hasher))
	compressed.Header.ModTime = time.Unix(0, 0)
	compressed.Header.OS = 255
	archive := tar.NewWriter(compressed)
	writeHeader := func(name string, size int64, kind byte) error {
		mode := int64(0600)
		if kind == tar.TypeDir {
			mode = 0700
		}
		return archive.WriteHeader(&tar.Header{Name: name, Size: size, Mode: mode, Typeflag: kind, ModTime: time.Unix(0, 0), Format: tar.FormatPAX})
	}
	if err := writeHeader(manifestName, int64(len(data)), tar.TypeReg); err != nil {
		return item, err
	}
	if _, err := archive.Write(data); err != nil {
		return item, err
	}
	for _, name := range contents.Directories {
		if err := writeHeader(name, 0, tar.TypeDir); err != nil {
			return item, err
		}
	}
	for _, record := range contents.Files {
		if err := writeHeader(record.Path, record.Size, tar.TypeReg); err != nil {
			return item, err
		}
		file, info, err := regularFile(filepath.Join(source, filepath.FromSlash(record.Path)))
		if err != nil {
			return item, err
		}
		if info.Size() != record.Size {
			file.Close()
			return item, errors.New("source size changed")
		}
		contentHash := sha256.New()
		count, copyErr := io.Copy(io.MultiWriter(archive, contentHash), io.LimitReader(file, record.Size+1))
		closeErr := file.Close()
		if copyErr != nil {
			return item, copyErr
		}
		if closeErr != nil {
			return item, closeErr
		}
		if count != record.Size || hex.EncodeToString(contentHash.Sum(nil)) != record.SHA256 {
			return item, errors.New("source content changed")
		}
	}
	if err := archive.Close(); err != nil {
		return item, err
	}
	if err := compressed.Close(); err != nil {
		return item, err
	}
	if err := temp.Sync(); err != nil {
		return item, err
	}
	info, err := temp.Stat()
	if err != nil {
		return item, err
	}
	item.ArchiveBytes, item.SHA256 = info.Size(), hex.EncodeToString(hasher.Sum(nil))
	if item.ArchiveBytes > maxArchiveBytes {
		return item, errors.New("archive exceeds bounds")
	}
	if err := temp.Close(); err != nil {
		return item, err
	}
	return item, publishFile(temp.Name(), filepath.Join(cache, item.Filename))
}

func validateManifest(contents manifest, item entry) error {
	if contents.Schema != 1 || contents.Bundle != item.Bundle || len(contents.Files) != item.Files || len(contents.Directories) == 0 || len(contents.Directories)+len(contents.Files) > maxFiles {
		return errors.New("manifest disagrees with index")
	}
	seen := make(map[string]bool)
	directories := make(map[string]bool)
	for _, name := range contents.Directories {
		if err := safePath(name); err != nil {
			return err
		}
		if name != item.Bundle && !strings.HasPrefix(name, item.Bundle+"/") {
			return errors.New("directory escapes bundle")
		}
		if seen[name] {
			return errors.New("duplicate manifest path")
		}
		seen[name], directories[name] = true, true
	}
	if !directories[item.Bundle] {
		return errors.New("missing bundle directory")
	}
	var total int64
	for _, record := range contents.Files {
		if err := safePath(record.Path); err != nil {
			return err
		}
		if !strings.HasPrefix(record.Path, item.Bundle+"/") || seen[record.Path] || !validDigest(record.SHA256) || record.Size < 0 || record.Size > maxBytes-total {
			return errors.New("invalid manifest file")
		}
		seen[record.Path] = true
		total += record.Size
	}
	for name := range seen {
		if name != item.Bundle && !directories[path.Dir(name)] {
			return errors.New("missing parent directory")
		}
	}
	if total != item.UnpackedBytes {
		return errors.New("manifest byte count disagrees with index")
	}
	return nil
}

func inspectArchive(cache string, item entry, dest string) error {
	if dest == "" {
		return inspectArchiveRoot(cache, item, nil)
	}
	root, err := os.OpenRoot(dest)
	if err != nil {
		return err
	}
	defer root.Close()
	return inspectArchiveRoot(cache, item, root)
}

func inspectArchiveRoot(cache string, item entry, dest *os.Root) error {
	file, info, err := regularFile(filepath.Join(cache, item.Filename))
	if err != nil {
		return err
	}
	defer file.Close()
	if info.Size() != item.ArchiveBytes {
		return errors.New("archive size mismatch")
	}
	hasher := sha256.New()
	count, err := io.Copy(hasher, io.LimitReader(file, item.ArchiveBytes+1))
	if err != nil {
		return err
	}
	if count != item.ArchiveBytes || hex.EncodeToString(hasher.Sum(nil)) != item.SHA256 {
		return errors.New("archive SHA-256 mismatch")
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	buffered := bufio.NewReader(file)
	compressed, err := gzip.NewReader(buffered)
	if err != nil {
		return err
	}
	defer compressed.Close()
	compressed.Multistream(false)
	limited := &io.LimitedReader{R: compressed, N: item.UnpackedBytes + maxManifest + int64(maxFiles)*2048 + 4096}
	archive := tar.NewReader(limited)
	header, err := archive.Next()
	if err != nil {
		return err
	}
	if header.Name != manifestName || header.Typeflag != tar.TypeReg || header.Size < 0 || header.Size > maxManifest || header.Linkname != "" {
		return errors.New("first header must be bounded regular manifest")
	}
	data, err := io.ReadAll(io.LimitReader(archive, maxManifest+1))
	if err != nil {
		return err
	}
	var contents manifest
	if err := decodeJSON(bytes.NewReader(data), &contents); err != nil {
		return err
	}
	canonical, err := json.Marshal(contents)
	if err != nil {
		return err
	}
	if digest(canonical) != item.ContentManifestSHA256 {
		return errors.New("manifest SHA-256 mismatch")
	}
	if err := validateManifest(contents, item); err != nil {
		return err
	}
	files := make(map[string]record)
	directories := make(map[string]bool)
	for _, record := range contents.Files {
		files[record.Path] = record
	}
	for _, name := range contents.Directories {
		directories[name] = true
	}
	seen := make(map[string]bool)
	for {
		header, err := archive.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if err := safePath(header.Name); err != nil {
			return err
		}
		if seen[header.Name] || header.Linkname != "" {
			return errors.New("duplicate path or link in archive")
		}
		seen[header.Name] = true
		if header.Typeflag == tar.TypeDir {
			if !directories[header.Name] || header.Size != 0 {
				return errors.New("unexpected directory")
			}
			if dest != nil {
				if err := dest.MkdirAll(filepath.FromSlash(header.Name), 0700); err != nil {
					return err
				}
			}
			continue
		}
		record, exists := files[header.Name]
		if header.Typeflag != tar.TypeReg || !exists || header.Size != record.Size {
			return errors.New("unexpected file or archive type")
		}
		contentHash := sha256.New()
		var target *os.File
		var writer io.Writer = contentHash
		if dest != nil {
			filename := filepath.FromSlash(header.Name)
			if err := dest.MkdirAll(filepath.Dir(filename), 0700); err != nil {
				return err
			}
			target, err = dest.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
			if err != nil {
				return err
			}
			writer = io.MultiWriter(contentHash, target)
		}
		count, copyErr := io.Copy(writer, archive)
		var closeErr error
		if target != nil {
			closeErr = target.Close()
		}
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
		if count != record.Size || hex.EncodeToString(contentHash.Sum(nil)) != record.SHA256 {
			return errors.New("file SHA-256 mismatch")
		}
	}
	if len(seen) != len(files)+len(directories) {
		return errors.New("archive missing manifest paths")
	}
	trailer := make([]byte, 32768)
	for {
		count, err := limited.Read(trailer)
		for _, value := range trailer[:count] {
			if value != 0 {
				return errors.New("trailing tar data")
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	if limited.N == 0 {
		return errors.New("decompression exceeds bounds")
	}
	if _, err := buffered.ReadByte(); err != io.EOF {
		return errors.New("trailing gzip data")
	}
	return nil
}

func restore(result index, cache, dest string, selected map[string]bool) error {
	if err := validateIndex(result); err != nil {
		return err
	}
	if err := ensureAbsent(dest); err != nil {
		return err
	}
	for _, item := range result.Entries {
		if selected[item.Bundle] {
			if err := inspectArchive(cache, item, ""); err != nil {
				return fmt.Errorf("%s: %w", item.Bundle, err)
			}
		}
	}
	destination, err := reserveRestore(dest)
	if err != nil {
		return err
	}
	defer destination.root.Close()
	defer destination.parent.Close()
	return materializeRestore(result, cache, selected, destination)
}

type restoreDestination struct {
	root   *os.Root
	parent *os.Root
	name   string
	path   string
	owned  os.FileInfo
}

func reserveRestore(dest string) (*restoreDestination, error) {
	clean := filepath.Clean(dest)
	parent, err := os.OpenRoot(filepath.Dir(clean))
	if err != nil {
		return nil, err
	}
	name := filepath.Base(clean)
	if err := parent.Mkdir(name, 0700); err != nil {
		parent.Close()
		return nil, err
	}
	owned, err := parent.Lstat(name)
	if err != nil {
		parent.Close()
		return nil, err
	}
	root, err := parent.OpenRoot(name)
	if err != nil {
		parent.Close()
		return nil, err
	}
	destination := &restoreDestination{root: root, parent: parent, name: name, path: clean, owned: owned}
	opened, err := root.Stat(".")
	if err != nil || !owned.IsDir() || !os.SameFile(owned, opened) {
		root.Close()
		parent.Close()
		return nil, errors.New("restore destination changed while opening")
	}
	return destination, nil
}

func (destination *restoreDestination) checkIdentity() error {
	current, err := destination.parent.Lstat(destination.name)
	if err != nil {
		return err
	}
	if !os.SameFile(destination.owned, current) {
		return errors.New("restore destination replaced")
	}
	visible, err := os.Lstat(destination.path)
	if err != nil {
		return err
	}
	if !os.SameFile(destination.owned, visible) {
		return errors.New("restore destination ancestor replaced")
	}
	return nil
}

func (destination *restoreDestination) cleanup() error {
	if err := destination.checkIdentity(); err != nil {
		return err
	}
	file, err := destination.root.Open(".")
	if err != nil {
		return err
	}
	names, readErr := file.Readdirnames(-1)
	closeErr := file.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return err
	}
	for _, name := range names {
		if err := destination.root.RemoveAll(name); err != nil {
			return err
		}
	}
	if err := destination.checkIdentity(); err != nil {
		return err
	}
	return destination.parent.Remove(destination.name)
}

func materializeRestore(result index, cache string, selected map[string]bool, destination *restoreDestination) (err error) {
	defer func() {
		if err != nil {
			err = errors.Join(err, destination.cleanup())
		}
	}()
	for _, item := range result.Entries {
		if selected[item.Bundle] {
			if err := inspectArchiveRoot(cache, item, destination.root); err != nil {
				return fmt.Errorf("%s: %w", item.Bundle, err)
			}
		}
	}
	return destination.checkIdentity()
}
