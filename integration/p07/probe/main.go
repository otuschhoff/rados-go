package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	rados "github.com/otuschhoff/go-librados"
)

type report struct {
	Create        bool   `json:"create"`
	Exclusive     bool   `json:"exclusive"`
	Write         bool   `json:"write"`
	WriteFull     bool   `json:"write_full"`
	Append        bool   `json:"append"`
	Truncate      bool   `json:"truncate"`
	Zero          bool   `json:"zero"`
	Remove        bool   `json:"remove"`
	Flush         bool   `json:"flush"`
	PrimaryChange bool   `json:"primary_change"`
	Version       uint64 `json:"version"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "p07 probe:", err)
		os.Exit(1)
	}
}

func run() error {
	monitors := flag.String("monitors", "", "comma-separated v2 monitor endpoints")
	keyFile := flag.String("key", "", "file containing an encoded CephX key")
	fsid := flag.String("fsid", "", "expected cluster FSID")
	control := flag.String("control", "", "primary-change synchronization directory")
	flag.Parse()
	key, err := os.ReadFile(*keyFile)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	client, err := rados.New(rados.Config{Monitors: strings.Split(*monitors, ","), Entity: "client.p07", ClusterFSID: *fsid, Key: bytes.TrimSpace(key), OperationTimeout: 20 * time.Second})
	if err != nil {
		return err
	}
	defer client.Close()
	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	pool, err := client.OpenPool(ctx, "p07-data")
	if err != nil {
		return err
	}
	object := pool.Object("go-crud")
	created, err := object.Create(ctx, true)
	if err != nil || created.Version == 0 {
		return fmt.Errorf("exclusive create version=%d: %w", created.Version, err)
	}
	if _, err := object.Create(ctx, true); !errors.Is(err, rados.ErrExists) {
		return fmt.Errorf("duplicate exclusive create: %v", err)
	}
	full, err := object.WriteFull(ctx, []byte("abcdef"))
	if err != nil || full.Version <= created.Version {
		return fmt.Errorf("write full version=%d: %w", full.Version, err)
	}
	written, err := object.Write(ctx, 1, []byte("Z"))
	if err != nil || written.Version <= full.Version {
		return fmt.Errorf("write version=%d: %w", written.Version, err)
	}
	appended, err := object.Append(ctx, []byte("gh"))
	if err != nil || appended.Version <= written.Version {
		return fmt.Errorf("append version=%d: %w", appended.Version, err)
	}
	zeroed, err := object.Zero(ctx, 2, 2)
	if err != nil || zeroed.Version <= appended.Version {
		return fmt.Errorf("zero version=%d: %w", zeroed.Version, err)
	}
	truncated, err := object.Truncate(ctx, 7)
	if err != nil || truncated.Version <= zeroed.Version {
		return fmt.Errorf("truncate version=%d: %w", truncated.Version, err)
	}
	if err := client.Flush(ctx); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	data, _, err := object.Read(ctx, 0, 32)
	want := []byte{'a', 'Z', 0, 0, 'e', 'f', 'g'}
	if err != nil || !bytes.Equal(data, want) {
		return fmt.Errorf("CRUD bytes=%x: %w", data, err)
	}
	missingZero := pool.Object("missing-zero")
	if _, err := missingZero.Zero(ctx, 0, 4); err != nil {
		return fmt.Errorf("zero missing: %w", err)
	}
	if _, _, err := missingZero.Read(ctx, 0, 8); !errors.Is(err, rados.ErrNotFound) {
		return fmt.Errorf("zero missing read: %v", err)
	}
	missingTruncate := pool.Object("missing-truncate")
	if _, err := missingTruncate.Truncate(ctx, 4); err != nil {
		return fmt.Errorf("truncate missing: %w", err)
	}
	if _, _, err := missingTruncate.Read(ctx, 0, 8); !errors.Is(err, rados.ErrNotFound) {
		return fmt.Errorf("truncate missing read: %v", err)
	}
	if _, err := pool.Object("missing-remove").Remove(ctx); !errors.Is(err, rados.ErrNotFound) {
		return fmt.Errorf("remove missing: %v", err)
	}
	if _, err := object.Remove(ctx); err != nil {
		return fmt.Errorf("remove: %w", err)
	}
	if _, _, err := object.Read(ctx, 0, 1); !errors.Is(err, rados.ErrNotFound) {
		return fmt.Errorf("removed read: %v", err)
	}
	parity := pool.Object("go-parity")
	if _, err := parity.WriteFull(ctx, want); err != nil {
		return fmt.Errorf("parity write: %w", err)
	}
	mixed := pool.Object("mixed-crud")
	if _, err := mixed.Write(ctx, 1, []byte("Z")); err != nil {
		return fmt.Errorf("mixed write: %w", err)
	}
	if _, err := mixed.Append(ctx, []byte("gh")); err != nil {
		return fmt.Errorf("mixed append: %w", err)
	}
	if _, err := mixed.Zero(ctx, 2, 2); err != nil {
		return fmt.Errorf("mixed zero: %w", err)
	}
	if _, err := mixed.Truncate(ctx, 7); err != nil {
		return fmt.Errorf("mixed truncate: %w", err)
	}
	remapObject := pool.Object("remap-append")
	remapInitial, _, err := remapObject.Read(ctx, 0, 16)
	if err != nil || string(remapInitial) != "base" {
		return fmt.Errorf("remap initial bytes=%q: %w", remapInitial, err)
	}
	if err := os.WriteFile(*control+"/ready", nil, 0o600); err != nil {
		return err
	}
	for {
		if _, err := os.Stat(*control + "/submit"); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
	type appendResult struct {
		result rados.OpResult
		err    error
	}
	appendDone := make(chan appendResult, 1)
	go func() {
		result, err := remapObject.Append(ctx, []byte("!"))
		appendDone <- appendResult{result: result, err: err}
	}()
	select {
	case result := <-appendDone:
		return fmt.Errorf("append completed while primary paused: version=%d err=%v", result.result.Version, result.err)
	case <-time.After(time.Second):
		if err := os.WriteFile(*control+"/blocked", nil, 0o600); err != nil {
			return err
		}
	}
	result := <-appendDone
	remapped, err := result.result, result.err
	if err != nil || remapped.Version == 0 {
		return fmt.Errorf("remapped append version=%d: %w", remapped.Version, err)
	}
	remapData, _, err := remapObject.Read(ctx, 0, 16)
	if err != nil || string(remapData) != "base!" {
		return fmt.Errorf("remapped bytes=%q: %w", remapData, err)
	}
	if err := client.Shutdown(ctx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	return json.NewEncoder(os.Stdout).Encode(report{true, true, true, true, true, true, true, true, true, true, remapped.Version})
}
