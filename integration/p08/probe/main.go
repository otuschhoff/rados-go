package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	rados "github.com/otuschhoff/rados-go"
)

type report struct {
	NativeMetadata        bool `json:"native_metadata"`
	BinaryMetadata        bool `json:"binary_metadata"`
	OMAPPagination        bool `json:"omap_pagination"`
	CompoundRead          bool `json:"compound_read"`
	CompoundAtomicity     bool `json:"compound_atomicity"`
	CrossClientContention bool `json:"cross_client_contention"`
	Enumeration           bool `json:"enumeration"`
	Namespaces            bool `json:"namespaces"`
	CursorContinuation    bool `json:"cursor_continuation"`
	CursorPartitioning    bool `json:"cursor_partitioning"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "p08 probe:", err)
		os.Exit(1)
	}
}

func run() error {
	monitors := flag.String("monitors", "", "comma-separated v2 monitor endpoints")
	keyFile := flag.String("key", "", "file containing an encoded CephX key")
	fsid := flag.String("fsid", "", "expected cluster FSID")
	coordinationDir := flag.String("coordination-dir", "", "directory for the map-change qualification handshake")
	flag.Parse()
	key, err := os.ReadFile(*keyFile)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	newClient := func() (*rados.Client, error) {
		client, err := rados.New(rados.Config{
			Monitors: strings.Split(*monitors, ","), Entity: "client.p08",
			ClusterFSID: *fsid, Key: bytes.TrimSpace(key), OperationTimeout: 20 * time.Second,
		})
		if err != nil {
			return nil, err
		}
		if err := client.Connect(ctx); err != nil {
			client.Close()
			return nil, err
		}
		return client, nil
	}
	first, err := newClient()
	if err != nil {
		return fmt.Errorf("connect first client: %w", err)
	}
	defer first.Close()
	second, err := newClient()
	if err != nil {
		return fmt.Errorf("connect second client: %w", err)
	}
	defer second.Close()
	firstPool, err := first.OpenPool(ctx, "p08-data")
	if err != nil {
		return err
	}
	secondPool, err := second.OpenPool(ctx, "p08-data")
	if err != nil {
		return err
	}

	native := firstPool.Object("native-metadata")
	xattr, err := native.GetXAttr(ctx, "binary")
	if err != nil || !bytes.Equal(xattr, []byte{0, 1, 0xff, 2}) {
		return fmt.Errorf("native binary xattr=%x: %w", xattr, err)
	}
	attributes, err := native.ListXAttrs(ctx)
	if err != nil || len(attributes) != 2 || attributes[0].Name != "alpha" || attributes[1].Name != "binary" {
		return fmt.Errorf("native xattrs=%+v: %w", attributes, err)
	}
	pageOne, err := native.ListOMAP(ctx, "", 2)
	if err != nil || len(pageOne.Values) != 2 || !pageOne.More {
		return fmt.Errorf("first OMAP page=%+v: %w", pageOne, err)
	}
	pageTwo, err := native.ListOMAP(ctx, string(pageOne.Values[1].Key), 2)
	if err != nil || len(pageTwo.Values) != 1 || pageTwo.More {
		return fmt.Errorf("second OMAP page=%+v: %w", pageTwo, err)
	}
	keys := [][]byte{pageOne.Values[0].Key, pageOne.Values[1].Key, pageTwo.Values[0].Key}
	if !bytes.Equal(keys[0], []byte{0x00, 'a'}) || !bytes.Equal(keys[1], []byte{'b'}) || !bytes.Equal(keys[2], []byte{0xff}) {
		return fmt.Errorf("native OMAP keys=%x", keys)
	}

	read := rados.NewReadOp()
	readIndex := read.Read(0, 16)
	xattrIndex := read.GetXAttr("binary")
	readResult, err := native.ExecuteRead(ctx, read)
	if err != nil || readIndex != 0 || xattrIndex != 1 || len(readResult.Results) != 2 || string(readResult.Results[readIndex].Data) != "native" || !bytes.Equal(readResult.Results[xattrIndex].Data, []byte{0, 1, 0xff, 2}) {
		return fmt.Errorf("compound read indexes=%d,%d result=%+v: %w", readIndex, xattrIndex, readResult, err)
	}
	optional := rados.NewReadOp()
	missingIndex := optional.GetXAttr("missing")
	optional.SetFlags(missingIndex, rados.SubOpFlagFailOK)
	optionalReadIndex := optional.Read(0, 16)
	optionalResult, err := native.ExecuteRead(ctx, optional)
	if err != nil || optionalResult.Results[missingIndex].Err == nil || string(optionalResult.Results[optionalReadIndex].Data) != "native" {
		return fmt.Errorf("FAILOK result=%+v: %w", optionalResult, err)
	}
	versioned := rados.NewWriteOp()
	versioned.AssertVersion(readResult.Version)
	versioned.WriteFull([]byte("native"))
	if _, err := native.ExecuteWrite(ctx, versioned); err != nil {
		return fmt.Errorf("assert current version: %w", err)
	}

	omapLifecycle := firstPool.Object("omap-lifecycle")
	omapSetup := rados.NewWriteOp()
	omapSetup.WriteFull([]byte("stable"))
	omapSetup.SetOMAPHeader([]byte{0, 0xff, 1})
	omapSetup.SetOMAP([]rados.OMAPEntry{{Key: []byte("keep"), Value: []byte("value")}, {Key: []byte("range-a"), Value: []byte("a")}, {Key: []byte("range-b"), Value: []byte("b")}, {Key: []byte("remove"), Value: []byte{2, 0, 3}}})
	if _, err := omapLifecycle.ExecuteWrite(ctx, omapSetup); err != nil {
		return fmt.Errorf("OMAP setup: %w", err)
	}
	omapAssert := rados.NewWriteOp()
	omapAssert.CompareOMAP([]byte("keep"), []byte("value"))
	omapAssert.WriteFull([]byte("stable"))
	if _, err := omapLifecycle.ExecuteWrite(ctx, omapAssert); err != nil {
		return fmt.Errorf("OMAP compare: %w", err)
	}
	header, err := omapLifecycle.GetOMAPHeader(ctx)
	if err != nil || !bytes.Equal(header, []byte{0, 0xff, 1}) {
		return fmt.Errorf("OMAP header=%x: %w", header, err)
	}
	selected, err := omapLifecycle.GetOMAP(ctx, [][]byte{[]byte("missing"), []byte("keep"), []byte("range-b")})
	if err != nil || len(selected) != 2 || string(selected[0].Key) != "keep" || string(selected[1].Key) != "range-b" {
		return fmt.Errorf("keyed OMAP values=%+v: %w", selected, err)
	}
	omapRemove := rados.NewWriteOp()
	omapRemove.RemoveOMAP([][]byte{[]byte("remove")})
	omapRemove.RemoveOMAPRange([]byte("range-a"), []byte("range-z"))
	if _, err := omapLifecycle.ExecuteWrite(ctx, omapRemove); err != nil {
		return fmt.Errorf("remove OMAP key: %w", err)
	}
	omapPage, err := omapLifecycle.ListOMAP(ctx, "", 4)
	if err != nil || len(omapPage.Values) != 1 || string(omapPage.Values[0].Key) != "keep" {
		return fmt.Errorf("OMAP after remove=%+v: %w", omapPage, err)
	}
	omapFailed := rados.NewWriteOp()
	omapFailed.WriteFull([]byte("changed"))
	omapFailed.CompareOMAP([]byte("keep"), []byte("wrong"))
	if _, err := omapLifecycle.ExecuteWrite(ctx, omapFailed); err == nil {
		return fmt.Errorf("failed OMAP compare succeeded")
	}
	omapData, _, err := omapLifecycle.Read(ctx, 0, 16)
	if err != nil || string(omapData) != "stable" {
		return fmt.Errorf("OMAP compare leaked bytes=%q: %w", omapData, err)
	}
	omapClear := rados.NewWriteOp()
	omapClear.ClearOMAP()
	if _, err := omapLifecycle.ExecuteWrite(ctx, omapClear); err != nil {
		return fmt.Errorf("clear OMAP: %w", err)
	}
	omapPage, err = omapLifecycle.ListOMAP(ctx, "", 4)
	if err != nil || len(omapPage.Values) != 0 || omapPage.More {
		return fmt.Errorf("OMAP after clear=%+v: %w", omapPage, err)
	}

	atomic := firstPool.Object("compound-atomic")
	if _, err := atomic.WriteFull(ctx, []byte("before")); err != nil {
		return err
	}
	failed := rados.NewWriteOp()
	failed.WriteFull([]byte("changed"))
	compareIndex := failed.CompareExtent(0, []byte("wrong"))
	failedResult, failedErr := atomic.ExecuteWrite(ctx, failed)
	if failedErr == nil || compareIndex != 1 || len(failedResult.Results) != 2 || failedResult.Results[compareIndex].Err == nil {
		return fmt.Errorf("compound failure index=%d result=%+v error=%v", compareIndex, failedResult, failedErr)
	}
	atomicData, _, err := atomic.Read(ctx, 0, 16)
	if err != nil || string(atomicData) != "before" {
		return fmt.Errorf("compound failure leaked bytes=%q: %w", atomicData, err)
	}

	contended := firstPool.Object("contended")
	if _, err := contended.WriteFull(ctx, []byte("seed")); err != nil {
		return err
	}
	control := rados.NewWriteOp()
	control.CompareExtent(0, []byte("seed"))
	control.WriteFull([]byte("seed"))
	if _, err := contended.ExecuteWrite(ctx, control); err != nil {
		return fmt.Errorf("sequential compare-and-write: %w", err)
	}
	if _, err := secondPool.Object("contended").Stat(ctx); err != nil {
		return fmt.Errorf("warm second contender: %w", err)
	}
	secondControl := rados.NewWriteOp()
	secondControl.CompareExtent(0, []byte("seed"))
	secondControl.WriteFull([]byte("seed"))
	if _, err := secondPool.Object("contended").ExecuteWrite(ctx, secondControl); err != nil {
		return fmt.Errorf("second-client compare-and-write: %w", err)
	}
	type outcome struct {
		index int
		err   error
	}
	outcomes := make(chan outcome, 2)
	start := make(chan struct{})
	var ready sync.WaitGroup
	ready.Add(2)
	for index, object := range []rados.ObjectRef{contended, secondPool.Object("contended")} {
		go func(index int, object rados.ObjectRef) {
			operation := rados.NewWriteOp()
			operation.CompareExtent(0, []byte("seed"))
			operation.WriteFull([]byte(fmt.Sprintf("winner-%d", index)))
			ready.Done()
			<-start
			_, err := object.ExecuteWrite(ctx, operation)
			outcomes <- outcome{index: index, err: err}
		}(index, object)
	}
	ready.Wait()
	close(start)
	successes := 0
	failures := 0
	var contentionErrors []error
	for range 2 {
		if result := <-outcomes; result.err == nil {
			successes++
		} else {
			failures++
			contentionErrors = append(contentionErrors, fmt.Errorf("contender %d: %w", result.index, result.err))
		}
	}
	if successes != 1 || failures != 1 {
		return fmt.Errorf("contention successes=%d failures=%d errors=%v", successes, failures, contentionErrors)
	}

	goMetadata := firstPool.Object("go-metadata")
	metadataWrite := rados.NewWriteOp()
	metadataWrite.WriteFull([]byte("go"))
	metadataWrite.SetXAttr("binary", []byte{0xfe, 0, 0xfd})
	metadataWrite.SetOMAP([]rados.OMAPEntry{{Key: []byte{0, 'g'}, Value: []byte{1, 0, 2}}, {Key: []byte("z"), Value: []byte("last")}})
	if _, err := goMetadata.ExecuteWrite(ctx, metadataWrite); err != nil {
		return fmt.Errorf("go metadata write: %w", err)
	}

	for _, name := range []string{"enum-a", "enum-b", "enum-c", "enum-d", "enum-e"} {
		if _, err := firstPool.Object(name).WriteFull(ctx, nil); err != nil {
			return err
		}
	}
	for _, name := range []string{"ns-a", "ns-b"} {
		if _, err := firstPool.WithNamespace("space").Object(name).WriteFull(ctx, nil); err != nil {
			return err
		}
	}
	defaultNames, pages, err := listAll(ctx, firstPool, 2, func() error {
		return awaitMapChange(ctx, *coordinationDir)
	})
	if err != nil || pages < 2 || !containsAll(defaultNames, "enum-a", "enum-b", "enum-c", "enum-d", "enum-e") || containsAll(defaultNames, "ns-a") {
		return fmt.Errorf("default enumeration pages=%d names=%v: %w", pages, defaultNames, err)
	}
	namespaceNames, namespacePages, err := listAll(ctx, firstPool.WithNamespace("space"), 1, nil)
	if err != nil || namespacePages < 2 || !equalStrings(namespaceNames, []string{"ns-a", "ns-b"}) {
		return fmt.Errorf("namespace enumeration pages=%d names=%v: %w", namespacePages, namespaceNames, err)
	}
	boundaries, err := firstPool.SplitCursor(firstPool.BeginObjectCursor(), firstPool.EndObjectCursor(), 8)
	if err != nil || len(boundaries) != 9 || !boundaries[8].IsEnd() {
		return fmt.Errorf("split cursors=%d: %w", len(boundaries), err)
	}
	for index := 1; index < len(boundaries); index++ {
		comparison, err := rados.CompareObjectCursors(boundaries[index-1], boundaries[index])
		if err != nil || comparison >= 0 {
			return fmt.Errorf("split boundary %d comparison=%d: %w", index, comparison, err)
		}
	}
	partitionNames := make([]string, 0, len(defaultNames))
	seen := make(map[string]bool, len(defaultNames))
	for index := 0; index+1 < len(boundaries); index++ {
		names, err := listRange(ctx, firstPool, boundaries[index], boundaries[index+1], 2)
		if err != nil {
			return fmt.Errorf("scan split partition %d: %w", index, err)
		}
		for _, name := range names {
			if seen[name] {
				return fmt.Errorf("split partition duplicate %q", name)
			}
			seen[name] = true
			partitionNames = append(partitionNames, name)
		}
	}
	if !equalStrings(partitionNames, defaultNames) {
		return fmt.Errorf("split partition union=%v full=%v", partitionNames, defaultNames)
	}
	if err := first.Flush(ctx); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(report{true, true, true, true, true, true, true, true, true, true})
}

func listAll(ctx context.Context, pool rados.Pool, limit uint64, afterFirstPage func() error) ([]string, int, error) {
	cursor := pool.BeginObjectCursor()
	names := []string{}
	pages := 0
	for !cursor.IsEnd() {
		page, err := pool.ListObjects(ctx, cursor, limit)
		if err != nil {
			return nil, pages, err
		}
		pages++
		for _, entry := range page.Values {
			names = append(names, entry.Name)
		}
		if pages == 1 && afterFirstPage != nil {
			if err := afterFirstPage(); err != nil {
				return nil, pages, err
			}
		}
		cursor = page.Next
		if pages > 1000 {
			return nil, pages, fmt.Errorf("enumeration did not terminate")
		}
	}
	return names, pages, nil
}

func awaitMapChange(ctx context.Context, directory string) error {
	if directory == "" {
		return nil
	}
	if err := os.WriteFile(directory+"/enumeration-ready", []byte("ready\n"), 0o600); err != nil {
		return err
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(directory + "/map-changed"); err == nil {
			return nil
		} else if !os.IsNotExist(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func listRange(ctx context.Context, pool rados.Pool, begin, end rados.ObjectCursor, limit uint64) ([]string, error) {
	cursor := begin
	names := []string{}
	for pages := 0; ; pages++ {
		page, err := pool.ListObjectsRange(ctx, cursor, end, limit)
		if err != nil {
			return nil, err
		}
		for _, entry := range page.Values {
			names = append(names, entry.Name)
		}
		if !page.More {
			return names, nil
		}
		cursor = page.Next
		if pages >= 1000 {
			return nil, fmt.Errorf("range enumeration did not terminate")
		}
	}
}

func containsAll(values []string, wanted ...string) bool {
	set := make(map[string]bool, len(values))
	for _, value := range values {
		set[value] = true
	}
	for _, value := range wanted {
		if !set[value] {
			return false
		}
	}
	return true
}

func equalStrings(left, right []string) bool {
	left = append([]string(nil), left...)
	right = append([]string(nil), right...)
	sort.Strings(left)
	sort.Strings(right)
	return strings.Join(left, "\x00") == strings.Join(right, "\x00")
}
