package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	rados "github.com/otuschhoff/rados-go"
)

type report struct {
	ClassExecution bool `json:"class_execution"`
	LockContention bool `json:"lock_contention"`
	LockRenew      bool `json:"lock_renew"`
	LockBreak      bool `json:"lock_break"`
	LockShared     bool `json:"lock_shared"`
	LockExpiry     bool `json:"lock_expiry"`
	WatchAck       bool `json:"watch_ack"`
	NotifyTimeout  bool `json:"notify_timeout"`
	NativeLocks    bool `json:"native_locks"`
	NativeWatch    bool `json:"native_watch"`
	NativeNotify   bool `json:"native_notify"`
	WatchRemap     bool `json:"watch_remap"`
	OSDRestart     bool `json:"osd_restart"`
	WatchShutdown  bool `json:"watch_shutdown"`
	ClientShutdown bool `json:"client_shutdown"`
}

type nativeSeedReport struct {
	NativeExecResult int    `json:"native_exec_result"`
	NativeExecOutput string `json:"native_exec_output"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "p09 probe:", err)
		os.Exit(1)
	}
}

func run() error {
	monitors := flag.String("monitors", "", "comma-separated v2 monitor endpoints")
	keyFile := flag.String("key", "", "file containing an encoded CephX key")
	fsid := flag.String("fsid", "", "expected cluster FSID")
	coordinationDir := flag.String("coordination-dir", "", "native interoperability handshake directory")
	flag.Parse()
	key, err := os.ReadFile(*keyFile)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	newClient := func(operationTimeout time.Duration) (*rados.Client, error) {
		client, err := rados.New(rados.Config{
			Monitors: strings.Split(*monitors, ","), Entity: "client.p09", ClusterFSID: *fsid,
			Key: bytes.TrimSpace(key), OperationTimeout: operationTimeout,
		})
		if err == nil {
			err = client.Connect(ctx)
		}
		return client, err
	}
	first, err := newClient(4 * time.Second)
	if err != nil {
		return fmt.Errorf("connect first: %w", err)
	}
	defer first.Close()
	second, err := newClient(4 * time.Second)
	if err != nil {
		return fmt.Errorf("connect second: %w", err)
	}
	defer second.Close()
	firstPool, err := first.OpenPool(ctx, "p09-data")
	if err != nil {
		return err
	}
	secondPool, err := second.OpenPool(ctx, "p09-data")
	if err != nil {
		return err
	}
	object := firstPool.Object("coordination")
	other := secondPool.Object("coordination")
	if _, err := object.WriteFull(ctx, []byte("ready")); err != nil {
		return fmt.Errorf("create object: %w", err)
	}
	nativeSeedData, err := os.ReadFile(*coordinationDir + "/native-seed.json")
	if err != nil {
		return err
	}
	var nativeSeed nativeSeedReport
	if err := json.Unmarshal(nativeSeedData, &nativeSeed); err != nil {
		return fmt.Errorf("decode native seed: %w", err)
	}
	nativeClassOutput, err := hex.DecodeString(nativeSeed.NativeExecOutput)
	if err != nil {
		return fmt.Errorf("decode native class output: %w", err)
	}
	classResult, err := firstPool.Object("class-exec").Exec(ctx, "lock", "list_locks", nil)
	if err != nil {
		return fmt.Errorf("generic exec: %w", err)
	}
	if classResult.Code != 0 || nativeSeed.NativeExecResult != len(nativeClassOutput) || !bytes.Equal(classResult.Data, nativeClassOutput) {
		return fmt.Errorf("generic exec mismatch Go=%+v native_result=%d native_output=%x", classResult, nativeSeed.NativeExecResult, nativeClassOutput)
	}
	if err := waitForFile(ctx, *coordinationDir+"/native-watch-ready"); err != nil {
		return fmt.Errorf("wait for native watch: %w", err)
	}
	nativeCookieData, err := os.ReadFile(*coordinationDir + "/native-watch-ready")
	if err != nil {
		return err
	}
	nativeCookie, err := strconv.ParseUint(strings.TrimSpace(string(nativeCookieData)), 10, 64)
	if err != nil || nativeCookie == 0 {
		return fmt.Errorf("native watch cookie %q: %w", nativeCookieData, err)
	}
	nativeLockers, err := object.ListLockers(ctx, "native-lock")
	if err != nil || len(nativeLockers) != 1 || nativeLockers[0].Cookie != "native-cookie" || nativeLockers[0].Description != "native holder renewed" || nativeLockers[0].Expiration.IsZero() {
		return fmt.Errorf("native lockers=%+v: %w", nativeLockers, err)
	}
	if err := other.BreakLock(ctx, "native-lock", nativeLockers[0].Client, nativeLockers[0].Cookie); err != nil {
		return fmt.Errorf("break native lock: %w", err)
	}
	if err := object.Lock(ctx, "native-release", rados.LockExclusive, rados.LockOptions{Cookie: "go-after-native-release"}); err != nil {
		return fmt.Errorf("acquire after native release: %w", err)
	}
	if err := object.Unlock(ctx, "native-release", "go-after-native-release"); err != nil {
		return fmt.Errorf("unlock after native release: %w", err)
	}
	if err := object.Lock(ctx, "native-shared", rados.LockShared, rados.LockOptions{Cookie: "go-native-shared", Tag: "native-shared-tag", Description: "Go shared holder"}); err != nil {
		return fmt.Errorf("join native shared lock: %w", err)
	}
	mixedShared, err := object.ListLockers(ctx, "native-shared")
	if err != nil || len(mixedShared) != 2 || mixedShared[0].Mode != rados.LockShared || mixedShared[0].Tag != "native-shared-tag" {
		return fmt.Errorf("mixed shared lockers=%+v: %w", mixedShared, err)
	}
	if err := object.Unlock(ctx, "native-shared", "go-native-shared"); err != nil {
		return fmt.Errorf("unlock Go mixed shared lock: %w", err)
	}
	if err := waitForLock(ctx, object, "native-expiry", "go-after-native-expiry"); err != nil {
		return err
	}
	if err := object.Unlock(ctx, "native-expiry", "go-after-native-expiry"); err != nil {
		return fmt.Errorf("unlock after native expiry: %w", err)
	}

	options := rados.LockOptions{Cookie: "first", Description: "go holder", Duration: 30 * time.Second}
	if err := object.Lock(ctx, "exclusive", rados.LockExclusive, options); err != nil {
		return fmt.Errorf("acquire lock: %w", err)
	}
	if err := other.Lock(ctx, "exclusive", rados.LockExclusive, rados.LockOptions{Cookie: "second"}); err == nil {
		return errors.New("contending exclusive lock succeeded")
	}
	lockers, err := object.ListLockers(ctx, "exclusive")
	if err != nil || len(lockers) != 1 || lockers[0].Cookie != "first" {
		return fmt.Errorf("list lockers=%+v: %w", lockers, err)
	}
	options.Renew = true
	if err := object.Lock(ctx, "exclusive", rados.LockExclusive, options); err != nil {
		return fmt.Errorf("renew lock: %w", err)
	}
	if err := other.BreakLock(ctx, "exclusive", lockers[0].Client, lockers[0].Cookie); err != nil {
		return fmt.Errorf("break lock: %w", err)
	}
	if err := object.Unlock(ctx, "exclusive", "first"); err == nil {
		return errors.New("unlock after break unexpectedly succeeded")
	}
	if err := object.Lock(ctx, "go-lock", rados.LockExclusive, rados.LockOptions{Cookie: "go-cookie", Duration: 30 * time.Second}); err != nil {
		return fmt.Errorf("seed Go lock: %w", err)
	}
	if err := os.WriteFile(*coordinationDir+"/native-verify-ready", nil, 0o600); err != nil {
		return err
	}
	if err := waitForFile(ctx, *coordinationDir+"/native-verify-complete"); err != nil {
		return fmt.Errorf("wait for native lock verification: %w", err)
	}
	if err := object.Unlock(ctx, "go-lock", "go-cookie"); err == nil {
		return errors.New("go lock remained after native break")
	}
	if err := object.Lock(ctx, "shared", rados.LockShared, rados.LockOptions{Cookie: "shared-first", Tag: "shared-tag", Description: "first shared holder", Duration: 30 * time.Second}); err != nil {
		return fmt.Errorf("first shared lock: %w", err)
	}
	if err := other.Lock(ctx, "shared", rados.LockShared, rados.LockOptions{Cookie: "shared-second", Tag: "shared-tag", Description: "second shared holder", Duration: 30 * time.Second}); err != nil {
		return fmt.Errorf("second shared lock: %w", err)
	}
	sharedLockers, err := object.ListLockers(ctx, "shared")
	if err != nil || len(sharedLockers) != 2 || sharedLockers[0].Mode != rados.LockShared || sharedLockers[0].Tag != "shared-tag" || sharedLockers[0].Description == "" || sharedLockers[0].Expiration.IsZero() {
		return fmt.Errorf("shared lockers=%+v: %w", sharedLockers, err)
	}
	if err := object.Unlock(ctx, "shared", "shared-first"); err != nil {
		return fmt.Errorf("unlock first shared lock: %w", err)
	}
	if err := other.Unlock(ctx, "shared", "shared-second"); err != nil {
		return fmt.Errorf("unlock second shared lock: %w", err)
	}
	if err := object.Lock(ctx, "expiring", rados.LockExclusive, rados.LockOptions{Cookie: "expiring", Duration: time.Second}); err != nil {
		return fmt.Errorf("expiring lock: %w", err)
	}
	time.Sleep(1500 * time.Millisecond)
	if err := other.Lock(ctx, "expiring", rados.LockExclusive, rados.LockOptions{Cookie: "after-expiry"}); err != nil {
		return fmt.Errorf("lock after expiry: %w", err)
	}
	if err := other.Unlock(ctx, "expiring", "after-expiry"); err != nil {
		return fmt.Errorf("unlock after expiry: %w", err)
	}

	nativeWatchers, err := object.ListWatchers(ctx)
	if err != nil || len(nativeWatchers) != 1 || nativeWatchers[0].Cookie != nativeCookie || nativeWatchers[0].Client == "" || nativeWatchers[0].Address == "" || nativeWatchers[0].Timeout <= 0 {
		return fmt.Errorf("native watcher metadata=%+v: %w", nativeWatchers, err)
	}
	nativeReply, err := object.Notify(ctx, []byte("go-native"))
	if err != nil || len(nativeReply.Acknowledged) != 1 {
		return fmt.Errorf("notify native watcher=%+v: %w", nativeReply, err)
	}
	nativeTarget, nativeEvents, err := object.Watch(ctx, 4)
	if err != nil {
		return fmt.Errorf("watch for native notify: %w", err)
	}
	if err := os.WriteFile(*coordinationDir+"/go-watch-ready", nil, 0o600); err != nil {
		return err
	}
	select {
	case event := <-nativeEvents:
		if string(event.Data) != "native-go" {
			return fmt.Errorf("native notify payload=%q", event.Data)
		}
		if err := nativeTarget.Ack(ctx, event.NotifyID, []byte("go-ack")); err != nil {
			return fmt.Errorf("ack native notify: %w", err)
		}
	case <-ctx.Done():
		return fmt.Errorf("wait for native notify: %w", ctx.Err())
	}
	if err := nativeTarget.Close(ctx); err != nil {
		return fmt.Errorf("close native target watch: %w", err)
	}

	remapWatch, remapEvents, err := object.Watch(ctx, 8)
	if err != nil {
		return fmt.Errorf("watch for remap: %w", err)
	}
	remapWatchers, err := object.ListWatchers(ctx)
	if err != nil || len(remapWatchers) != 2 || !containsWatcher(remapWatchers, nativeCookie) || !containsWatcher(remapWatchers, remapWatch.Cookie()) {
		return fmt.Errorf("pre-remap watchers=%+v: %w", remapWatchers, err)
	}
	remapCookie := remapWatch.Cookie()
	if err := os.WriteFile(*coordinationDir+"/remap-watch-ready", nil, 0o600); err != nil {
		return err
	}
	if err := waitForFile(ctx, *coordinationDir+"/remap-complete"); err != nil {
		return fmt.Errorf("wait for remap: %w", err)
	}
	if err := waitForStableWatchers(ctx, object, []uint64{nativeCookie, remapCookie}, 5*time.Second); err != nil {
		return err
	}
	if err := drainWatchInterruptions(remapWatch.Errors()); err != nil {
		return err
	}
	remapNotifyDone := make(chan error, 1)
	go func() {
		reply, err := other.Notify(ctx, []byte("after-remap-native"))
		if err == nil && (len(reply.Acknowledged) != 2 || len(reply.TimedOut) != 0) {
			err = fmt.Errorf("post-remap notify result=%+v", reply)
		}
		remapNotifyDone <- err
	}()
	select {
	case event := <-remapEvents:
		if string(event.Data) != "after-remap-native" {
			return fmt.Errorf("post-remap payload=%q", event.Data)
		}
		if err := remapWatch.Ack(ctx, event.NotifyID, nil); err != nil {
			return fmt.Errorf("ack post-remap notify: %w", err)
		}
	case err := <-remapNotifyDone:
		return fmt.Errorf("post-remap notifier completed before delivery: %w", err)
	case err := <-remapWatch.Errors():
		return fmt.Errorf("post-remap watch lost: %w", err)
	case <-ctx.Done():
		return fmt.Errorf("wait for post-remap notify: %w", ctx.Err())
	}
	if err := <-remapNotifyDone; err != nil {
		return err
	}
	select {
	case duplicate := <-remapEvents:
		return fmt.Errorf("duplicate post-remap event %d", duplicate.NotifyID)
	case <-time.After(500 * time.Millisecond):
	}
	if err := os.WriteFile(*coordinationDir+"/remap-verified", nil, 0o600); err != nil {
		return err
	}
	if err := waitForFile(ctx, *coordinationDir+"/restart-complete"); err != nil {
		return fmt.Errorf("wait for OSD restart: %w", err)
	}
	if err := waitForStableWatchers(ctx, object, []uint64{nativeCookie, remapCookie}, 5*time.Second); err != nil {
		return fmt.Errorf("watch after OSD restart: %w", err)
	}
	if err := drainWatchInterruptions(remapWatch.Errors()); err != nil {
		return err
	}
	restartNotifyDone := make(chan error, 1)
	go func() {
		reply, err := other.Notify(ctx, []byte("after-restart-native"))
		if err == nil && (len(reply.Acknowledged) != 2 || len(reply.TimedOut) != 0) {
			err = fmt.Errorf("post-restart notify result=%+v", reply)
		}
		restartNotifyDone <- err
	}()
	select {
	case event := <-remapEvents:
		if string(event.Data) != "after-restart-native" {
			return fmt.Errorf("post-restart payload=%q", event.Data)
		}
		if err := remapWatch.Ack(ctx, event.NotifyID, nil); err != nil {
			return fmt.Errorf("ack post-restart notify: %w", err)
		}
	case err := <-restartNotifyDone:
		return fmt.Errorf("post-restart notifier completed before delivery: %w", err)
	case err := <-remapWatch.Errors():
		return fmt.Errorf("post-restart watch lost: %w", err)
	case <-ctx.Done():
		return fmt.Errorf("wait for post-restart notify: %w", ctx.Err())
	}
	if err := <-restartNotifyDone; err != nil {
		return err
	}
	select {
	case duplicate := <-remapEvents:
		return fmt.Errorf("duplicate post-restart event %d", duplicate.NotifyID)
	case <-time.After(500 * time.Millisecond):
	}
	if err := remapWatch.Close(ctx); err != nil {
		return fmt.Errorf("close remap watch: %w", err)
	}
	if err := waitForWatcherCount(ctx, object, 0); err != nil {
		return err
	}

	watch, events, err := object.Watch(ctx, 8)
	if err != nil {
		return fmt.Errorf("watch: %w", err)
	}
	notifyDone := make(chan struct {
		reply rados.NotifyReply
		err   error
	}, 1)
	go func() {
		reply, err := other.Notify(ctx, []byte("notify"))
		notifyDone <- struct {
			reply rados.NotifyReply
			err   error
		}{reply, err}
	}()
	select {
	case event := <-events:
		if string(event.Data) != "notify" {
			return fmt.Errorf("watch payload=%q", event.Data)
		}
		if err := watch.Ack(ctx, event.NotifyID, []byte("ack")); err != nil {
			return fmt.Errorf("watch ack: %w", err)
		}
	case <-ctx.Done():
		return fmt.Errorf("wait for watch notify: %w", ctx.Err())
	}
	ackResult := <-notifyDone
	if ackResult.err != nil || len(ackResult.reply.Acknowledged) != 1 || len(ackResult.reply.TimedOut) != 0 {
		return fmt.Errorf("notify ack result=%+v: %w", ackResult.reply, ackResult.err)
	}

	timeoutDone := make(chan struct {
		reply rados.NotifyReply
		err   error
	}, 1)
	go func() {
		reply, err := other.Notify(ctx, []byte("timeout"))
		timeoutDone <- struct {
			reply rados.NotifyReply
			err   error
		}{reply, err}
	}()
	select {
	case event := <-events:
		if string(event.Data) != "timeout" {
			return fmt.Errorf("timeout payload=%q", event.Data)
		}
	case <-ctx.Done():
		return fmt.Errorf("wait for timeout notify: %w", ctx.Err())
	}
	timeoutResult := <-timeoutDone
	if timeoutResult.err == nil || len(timeoutResult.reply.TimedOut) != 1 {
		return fmt.Errorf("notify timeout result=%+v: %w", timeoutResult.reply, timeoutResult.err)
	}
	if err := watch.Close(ctx); err != nil {
		return fmt.Errorf("close watch: %w", err)
	}
	select {
	case <-watch.Done():
	case <-ctx.Done():
		return errors.New("watch did not close")
	}
	shutdownWatch, shutdownEvents, err := object.Watch(ctx, 1)
	if err != nil {
		return fmt.Errorf("watch for client shutdown: %w", err)
	}
	shutdownNotify := make(chan error, 1)
	go func() {
		_, err := object.Notify(ctx, []byte("shutdown"))
		shutdownNotify <- err
	}()
	select {
	case event := <-shutdownEvents:
		if string(event.Data) != "shutdown" {
			return fmt.Errorf("shutdown payload=%q", event.Data)
		}
	case <-ctx.Done():
		return fmt.Errorf("wait for shutdown notify: %w", ctx.Err())
	}
	queueClient, err := newClient(time.Second)
	if err != nil {
		return fmt.Errorf("connect callback queue client: %w", err)
	}
	defer queueClient.Close()
	queuePool, err := queueClient.OpenPool(ctx, "p09-data")
	if err != nil {
		return fmt.Errorf("open callback queue pool: %w", err)
	}
	queuedReply, queuedErr := queuePool.Object("coordination").Notify(ctx, []byte("queued-shutdown"))
	if queuedErr == nil || len(queuedReply.Acknowledged) != 0 || len(queuedReply.TimedOut) != 1 || queuedReply.TimedOut[0].Cookie != shutdownWatch.Cookie() {
		return fmt.Errorf("queued shutdown notify result=%+v: %w", queuedReply, queuedErr)
	}
	shutdownStarted := time.Now()
	if err := first.Shutdown(ctx); !errors.Is(err, rados.ErrOutcomeUnknown) {
		return fmt.Errorf("shutdown client: %w", err)
	}
	if time.Since(shutdownStarted) > time.Second {
		return errors.New("client shutdown exceeded bound")
	}
	select {
	case <-shutdownWatch.Done():
	case <-ctx.Done():
		return errors.New("shutdown watch did not terminate")
	}
	if err := <-shutdownNotify; !errors.Is(err, rados.ErrClosed) || !errors.Is(err, rados.ErrOutcomeUnknown) {
		return fmt.Errorf("shutdown notify error=%v", err)
	}
	if _, ok := <-shutdownEvents; ok {
		return errors.New("callback delivered after client shutdown")
	}
	return json.NewEncoder(os.Stdout).Encode(report{
		ClassExecution: true, LockContention: true, LockRenew: true, LockBreak: true, LockShared: true, LockExpiry: true,
		WatchAck: true, NotifyTimeout: true, NativeLocks: true, NativeWatch: true, NativeNotify: true, WatchRemap: true, OSDRestart: true, WatchShutdown: true, ClientShutdown: true,
	})
}

func waitForStableWatchers(ctx context.Context, object rados.ObjectRef, cookies []uint64, stableFor time.Duration) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	var stableSince time.Time
	for {
		watchers, err := object.ListWatchers(ctx)
		if err == nil {
			found := true
			for _, cookie := range cookies {
				found = found && containsWatcher(watchers, cookie)
			}
			if found && stableSince.IsZero() {
				stableSince = time.Now()
			}
			if found && time.Since(stableSince) >= stableFor {
				return nil
			}
			if !found {
				stableSince = time.Time{}
			}
		} else {
			stableSince = time.Time{}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("watch cookies %v did not remain registered for %s: %w", cookies, stableFor, ctx.Err())
		case <-ticker.C:
		}
	}
}

func containsWatcher(watchers []rados.Watcher, cookie uint64) bool {
	for _, watcher := range watchers {
		if watcher.Cookie == cookie {
			return true
		}
	}
	return false
}

func drainWatchInterruptions(errorsChannel <-chan error) error {
	for {
		select {
		case err := <-errorsChannel:
			if err != nil && !errors.Is(err, rados.ErrWatchInterrupted) {
				return fmt.Errorf("watch error: %w", err)
			}
		default:
			return nil
		}
	}
}

func waitForLock(ctx context.Context, object rados.ObjectRef, name, cookie string) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		err := object.Lock(ctx, name, rados.LockExclusive, rados.LockOptions{Cookie: cookie})
		if err == nil {
			return nil
		}
		if !errors.Is(err, rados.ErrConflict) {
			return fmt.Errorf("acquire %s after native expiry: %w", name, err)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for native lock expiry: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func waitForWatcherCount(ctx context.Context, object rados.ObjectRef, count int) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		watchers, err := object.ListWatchers(ctx)
		if err == nil && len(watchers) == count {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("watcher count did not become %d: %w", count, ctx.Err())
		case <-ticker.C:
		}
	}
}

func waitForFile(ctx context.Context, path string) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(path); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
