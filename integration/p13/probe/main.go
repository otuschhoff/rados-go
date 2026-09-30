package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	rados "github.com/otuschhoff/rados-go"
)

type result struct {
	Implementation     string              `json:"implementation"`
	Transport          string              `json:"transport"`
	Operation          string              `json:"operation"`
	Object             string              `json:"object"`
	StartedAt          string              `json:"started_at"`
	FinishedAt         string              `json:"finished_at"`
	ElapsedNS          int64               `json:"elapsed_ns"`
	Completed          bool                `json:"completed"`
	Errno              int                 `json:"errno"`
	Outcome            string              `json:"outcome"`
	Version            uint64              `json:"version"`
	Data               string              `json:"data"`
	MutationMarker     string              `json:"mutation_marker"`
	WatchCookie        uint64              `json:"watch_cookie"`
	WatchEvents        int                 `json:"watch_events"`
	WatchInterruptions int                 `json:"watch_interruptions"`
	Changes            []changeObservation `json:"changes,omitempty"`
}

type changeObservation struct {
	Sequence    uint64          `json:"sequence"`
	ObservedAt  string          `json:"observed_at"`
	Component   string          `json:"component"`
	Source      string          `json:"source"`
	Kind        string          `json:"kind"`
	Epoch       uint32          `json:"epoch"`
	OSD         *rados.OSDState `json:"osd,omitempty"`
	PreviousOSD *rados.OSDState `json:"previous_osd,omitempty"`
	MON         *rados.MONState `json:"mon,omitempty"`
	PreviousMON *rados.MONState `json:"previous_mon,omitempty"`
	Error       string          `json:"error,omitempty"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "p13 probe:", err)
		os.Exit(1)
	}
}

func run() error {
	monitors := flag.String("monitors", "", "comma-separated v2 monitor endpoints")
	keyFile := flag.String("key-file", "", "file containing an encoded CephX key")
	fsid := flag.String("fsid", "", "expected cluster FSID")
	poolName := flag.String("pool", "", "pool name")
	operation := flag.String("operation", "read", "read, write, append, watch, cluster-changes, or command")
	objectName := flag.String("object", "p13-object", "object name")
	payload := flag.String("payload", "", "write or append payload")
	mutationID := flag.String("mutation-id", "", "stable harness mutation marker")
	timeout := flag.Duration("timeout", 45*time.Second, "operation deadline")
	controlDir := flag.String("control-dir", "", "watch coordination directory")
	flag.Parse()
	if *monitors == "" || *keyFile == "" || *fsid == "" || *poolName == "" || *timeout <= 0 {
		return errors.New("monitors, key-file, fsid, pool, and positive timeout are required")
	}
	key, err := os.ReadFile(*keyFile)
	if err != nil {
		return fmt.Errorf("read key: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	client, err := rados.New(rados.Config{
		Monitors: strings.Split(*monitors, ","), Entity: "client.p13",
		ClusterFSID: *fsid, Key: bytes.TrimSpace(key), SecurityMode: securityMode(), OperationTimeout: *timeout,
	})
	if err != nil {
		return err
	}
	defer client.Close()
	var changes *rados.ClusterSubscription
	if *operation == "cluster-changes" {
		changes, err = client.SubscribeClusterChanges(ctx, rados.ClusterSubscriptionOptions{OSDs: true, MONs: true, Queue: 512})
		if err != nil {
			return fmt.Errorf("subscribe cluster changes: %w", err)
		}
		defer changes.Close()
	}
	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	pool, err := client.OpenPool(ctx, *poolName)
	if err != nil {
		return fmt.Errorf("open pool: %w", err)
	}
	object := pool.Object(*objectName)
	if readyFile := os.Getenv("P13_READY_FILE"); readyFile != "" {
		if err := os.WriteFile(readyFile, []byte("ready"), 0o600); err != nil {
			return fmt.Errorf("write ready file: %w", err)
		}
		releaseFile := os.Getenv("P13_RELEASE_FILE")
		if releaseFile == "" {
			return errors.New("P13_RELEASE_FILE is required with P13_READY_FILE")
		}
		for {
			if _, err := os.Stat(releaseFile); err == nil {
				break
			} else if !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("stat release file: %w", err)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	started := time.Now()
	report := result{Implementation: "go", Transport: transport(), Operation: *operation, Object: *objectName, StartedAt: started.UTC().Format(time.RFC3339Nano), MutationMarker: *mutationID}

	switch *operation {
	case "read":
		data, info, operationErr := object.Read(ctx, 0, 1<<20)
		report.Data, report.Version, err = string(data), info.Version, operationErr
	case "write":
		operationResult, operationErr := object.WriteFull(ctx, []byte(*payload))
		report.Version, err = operationResult.Version, operationErr
	case "append":
		operationResult, operationErr := object.Append(ctx, []byte(*payload))
		report.Version, err = operationResult.Version, operationErr
	case "watch":
		if *controlDir == "" {
			return errors.New("watch requires control-dir")
		}
		watch, events, watchErr := object.Watch(ctx, 8)
		if watchErr != nil {
			err = watchErr
			break
		}
		report.WatchCookie = watch.Cookie()
		if writeErr := os.WriteFile(*controlDir+"/watch-ready", []byte(strconv.FormatUint(watch.Cookie(), 10)), 0o600); writeErr != nil {
			return writeErr
		}
		for {
			if _, statErr := os.Stat(*controlDir + "/watch-release"); statErr == nil {
				break
			}
			select {
			case event := <-events:
				report.WatchEvents++
				if writeErr := os.WriteFile(*controlDir+"/watch-event", []byte("delivered"), 0o600); writeErr != nil {
					err = writeErr
				} else if ackErr := watch.Ack(ctx, event.NotifyID, nil); ackErr != nil {
					err = ackErr
				}
			case watchErr := <-watch.Errors():
				if errors.Is(watchErr, rados.ErrWatchInterrupted) {
					report.WatchInterruptions++
					if writeErr := os.WriteFile(*controlDir+"/watch-interrupted", []byte("interrupted"), 0o600); writeErr != nil {
						err = writeErr
					}
				} else if watchErr != nil {
					err = watchErr
				}
			case <-time.After(100 * time.Millisecond):
			case <-ctx.Done():
				err = ctx.Err()
			}
			if err != nil {
				break
			}
		}
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer closeCancel()
		if closeErr := watch.Close(closeCtx); err == nil {
			err = closeErr
		}
	case "cluster-changes":
		quiet := time.NewTimer(time.Second)
		defer quiet.Stop()
	collectChanges:
		for {
			select {
			case change, ok := <-changes.Events():
				if !ok {
					break collectChanges
				}
				report.Changes = append(report.Changes, normalizeChange(change))
				if !quiet.Stop() {
					select {
					case <-quiet.C:
					default:
					}
				}
				quiet.Reset(time.Second)
			case subscriptionErr, ok := <-changes.Errors():
				if ok && subscriptionErr != nil {
					err = subscriptionErr
				}
				break collectChanges
			case <-quiet.C:
				break collectChanges
			case <-ctx.Done():
				err = ctx.Err()
				break collectChanges
			}
		}
	case "monitor-command":
		commandResult, commandErr := client.MonitorCommand(ctx, []byte(*payload), nil)
		report.Data, err = string(commandResult.Output), commandErr
	case "osd-command":
		osdID, parseErr := strconv.Atoi(*objectName)
		if parseErr != nil {
			return fmt.Errorf("parse OSD id: %w", parseErr)
		}
		commandResult, commandErr := client.OSDCommand(ctx, osdID, []byte(*payload), nil)
		report.Data, err = string(commandResult.Output), commandErr
	case "pg-command":
		commandResult, commandErr := client.PGCommand(ctx, *objectName, []byte(*payload), nil)
		report.Data, err = string(commandResult.Output), commandErr
	default:
		return fmt.Errorf("unsupported operation %q", *operation)
	}

	report.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	report.ElapsedNS = time.Since(started).Nanoseconds()
	report.Completed = err == nil
	report.Errno = errno(err)
	if err == nil {
		report.Outcome = "success"
	} else if errors.Is(err, context.DeadlineExceeded) {
		report.Outcome = "timeout"
	} else if errors.Is(err, rados.ErrOutcomeUnknown) {
		report.Outcome = "unknown"
	} else {
		report.Outcome = "error"
	}
	if encodeErr := json.NewEncoder(os.Stdout).Encode(report); encodeErr != nil {
		return encodeErr
	}
	return err
}

func normalizeChange(change rados.ClusterChange) changeObservation {
	result := changeObservation{
		Sequence: change.Sequence, ObservedAt: change.ObservedAt.Format(time.RFC3339Nano), Epoch: change.Epoch,
		OSD: change.OSD, PreviousOSD: change.PreviousOSD, MON: change.MON, PreviousMON: change.PreviousMON,
	}
	result.Component = map[rados.ClusterComponent]string{rados.ClusterComponentOSD: "osd", rados.ClusterComponentMON: "mon"}[change.Component]
	result.Source = map[rados.ClusterChangeSource]string{rados.ClusterChangeAuthoritative: "authoritative", rados.ClusterChangeObserved: "observed"}[change.Source]
	result.Kind = map[rados.ClusterChangeKind]string{
		rados.ClusterChangeAdded: "added", rados.ClusterChangeRemoved: "removed", rados.ClusterChangeChanged: "changed",
		rados.ClusterChangeUp: "up", rados.ClusterChangeDown: "down", rados.ClusterChangeIn: "in", rados.ClusterChangeOut: "out",
		rados.ClusterChangeAvailable: "available", rados.ClusterChangeUnavailable: "unavailable",
	}[change.Kind]
	if change.Err != nil {
		result.Error = change.Err.Error()
	}
	return result
}

func transport() string {
	if os.Getenv("P13_TRANSPORT") == "crc" {
		return "crc"
	}
	return "secure"
}

func securityMode() rados.SecurityMode {
	if transport() == "crc" {
		return rados.SecurityModeCRC
	}
	return rados.SecurityModeSecure
}

func errno(err error) int {
	if err == nil {
		return 0
	}
	var value syscall.Errno
	if errors.As(err, &value) {
		return int(value)
	}
	var operationError *rados.OpError
	if errors.As(err, &operationError) && operationError.Code != 0 {
		if operationError.Code < 0 {
			return int(-operationError.Code)
		}
		return int(operationError.Code)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return int(syscall.ETIMEDOUT)
	}
	return -1
}
