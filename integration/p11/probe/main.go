package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	rados "github.com/otuschhoff/go-librados"
)

type adminReport struct {
	ClusterStats                bool     `json:"cluster_stats"`
	PoolStats                   bool     `json:"pool_stats"`
	MonitorCommand              bool     `json:"monitor_command"`
	ManagerCommand              bool     `json:"manager_command"`
	OSDCommand                  bool     `json:"osd_command"`
	PGCommand                   bool     `json:"pg_command"`
	PoolCreateDelete            bool     `json:"pool_create_delete"`
	ApplicationMetadata         bool     `json:"application_metadata"`
	SessionAddresses            []string `json:"session_addresses"`
	Blocklist                   bool     `json:"blocklist"`
	InconsistentPGs             bool     `json:"inconsistent_pgs"`
	InconsistentObjects         bool     `json:"inconsistent_objects"`
	CommandErrorOutputPreserved bool     `json:"command_error_output_preserved"`
}

type recoveryReport struct {
	ManagerBeforeFailover bool `json:"manager_before_failover"`
	ManagerAfterFailover  bool `json:"manager_after_failover"`
	IOAfterManagerLoss    bool `json:"io_after_manager_loss"`
}

type leastPrivilegeReport struct {
	WriteReadWithoutManager bool `json:"write_read_without_manager"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "p11 probe:", err)
		os.Exit(1)
	}
}

func run() error {
	mode := flag.String("mode", "admin", "admin, recovery, or least")
	monitors := flag.String("monitors", "", "comma-separated v2 monitor endpoints")
	keyFile := flag.String("key", "", "file containing an encoded CephX key")
	entity := flag.String("entity", "client.p11-admin", "Ceph client entity")
	fsid := flag.String("fsid", "", "expected cluster FSID")
	pg := flag.String("pg", "", "validated PG for PG command tests")
	osdID := flag.Int("osd", 0, "OSD target for command tests")
	coordinationDir := flag.String("coordination-dir", "", "manager failure coordination directory")
	flag.Parse()
	key, err := os.ReadFile(*keyFile)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	client, err := rados.New(rados.Config{Monitors: strings.Split(*monitors, ","), Entity: *entity, ClusterFSID: *fsid, Key: bytes.TrimSpace(key), OperationTimeout: 20 * time.Second})
	if err != nil {
		return err
	}
	if err := client.Connect(ctx); err != nil {
		return err
	}
	defer client.Close()
	switch *mode {
	case "admin":
		return runAdmin(ctx, client, *pg, *osdID)
	case "recovery":
		return runRecovery(ctx, client, *coordinationDir)
	case "least":
		return runLeastPrivilege(ctx, client)
	default:
		return fmt.Errorf("unknown mode %q", *mode)
	}
}

func runAdmin(ctx context.Context, client *rados.Client, pg string, osdID int) error {
	result := adminReport{}
	clusterStats, err := client.ClusterStats(ctx)
	if err != nil || clusterStats.KB == 0 || clusterStats.KBAvailable > clusterStats.KB {
		return fmt.Errorf("cluster stats=%+v: %w", clusterStats, err)
	}
	result.ClusterStats = true
	pool, err := client.OpenPool(ctx, "p11-data")
	if err != nil {
		return err
	}
	if _, err := pool.Object("stats-object").WriteFull(ctx, []byte("p11-stats")); err != nil {
		return err
	}
	poolStats, err := pool.Stats(ctx)
	if err != nil || poolStats.Objects == 0 || poolStats.BytesUsed == 0 {
		return fmt.Errorf("pool stats=%+v: %w", poolStats, err)
	}
	result.PoolStats = true
	if command, err := client.MonitorCommand(ctx, []byte(`{"prefix":"status","format":"json"}`), nil); err != nil || len(command.Output) == 0 {
		return fmt.Errorf("monitor command status=%q bytes=%d: %w", command.Status, len(command.Output), err)
	}
	result.MonitorCommand = true
	if command, err := client.ManagerCommand(ctx, []byte(`{"prefix":"pg dump","format":"json"}`), nil); err != nil || len(command.Output) == 0 {
		return fmt.Errorf("manager command status=%q bytes=%d: %w", command.Status, len(command.Output), err)
	}
	result.ManagerCommand = true
	if command, err := client.OSDCommand(ctx, osdID, []byte(`{"prefix":"version"}`), nil); err != nil || len(command.Output)+len(command.Status) == 0 {
		return fmt.Errorf("OSD command status=%q bytes=%d: %w", command.Status, len(command.Output), err)
	}
	result.OSDCommand = true
	if pg == "" {
		return errors.New("missing PG target")
	}
	pgCommand, err := json.Marshal(map[string]string{"prefix": "pg", "cmd": "query", "pgid": pg})
	if err != nil {
		return err
	}
	if command, err := client.PGCommand(ctx, pg, pgCommand, nil); err != nil || len(command.Output) == 0 {
		return fmt.Errorf("PG command status=%q bytes=%d: %w", command.Status, len(command.Output), err)
	}
	result.PGCommand = true
	if err := client.CreatePool(ctx, "p11-go-created"); err != nil {
		return fmt.Errorf("create pool: %w", err)
	}
	if _, err := client.OpenPool(ctx, "p11-go-created"); err != nil {
		return fmt.Errorf("created pool visibility: %w", err)
	}
	if err := client.DeletePool(ctx, "p11-go-created"); err != nil {
		return fmt.Errorf("delete pool: %w", err)
	}
	if _, err := client.OpenPool(ctx, "p11-go-created"); err == nil {
		return errors.New("deleted pool remained visible")
	}
	result.PoolCreateDelete = true
	if err := pool.EnableApplication(ctx, "p11app", false); err != nil {
		return err
	}
	if err := pool.SetApplicationMetadata(ctx, "p11app", "owner", "go"); err != nil {
		return err
	}
	applications, err := pool.ListApplications(ctx)
	if err != nil || len(applications) != 1 || applications[0] != "p11app" {
		return fmt.Errorf("applications=%v: %w", applications, err)
	}
	metadata, err := pool.ListApplicationMetadata(ctx, "p11app")
	if err != nil || metadata["owner"] != "go" {
		return fmt.Errorf("metadata=%v: %w", metadata, err)
	}
	value, err := pool.GetApplicationMetadata(ctx, "p11app", "owner")
	if err != nil || value != "go" {
		return fmt.Errorf("metadata value=%q: %w", value, err)
	}
	if err := pool.RemoveApplicationMetadata(ctx, "p11app", "owner"); err != nil {
		return err
	}
	if _, err := pool.GetApplicationMetadata(ctx, "p11app", "owner"); err == nil {
		return errors.New("removed application metadata remained visible")
	}
	result.ApplicationMetadata = true
	result.SessionAddresses = client.SessionAddresses()
	if len(result.SessionAddresses) == 0 {
		return errors.New("no session addresses")
	}
	if err := client.Blocklist(ctx, "v2:192.0.2.254:6800/1", time.Minute); err != nil {
		return err
	}
	result.Blocklist = true
	inconsistentPGs, err := client.ListInconsistentPGs(ctx, pool.ID())
	if err != nil || inconsistentPGs == nil {
		return fmt.Errorf("inconsistent PGs=%v: %w", inconsistentPGs, err)
	}
	result.InconsistentPGs = true
	inconsistentObjects, err := client.ListInconsistentObjects(ctx, pg)
	if err != nil || inconsistentObjects == nil {
		return fmt.Errorf("inconsistent objects=%v: %w", inconsistentObjects, err)
	}
	result.InconsistentObjects = true
	failed, err := client.MonitorCommand(ctx, []byte(`{"prefix":"osd pool get","pool":"missing-p11-pool","var":"size","format":"json"}`), nil)
	if err == nil || failed.Status == "" {
		return fmt.Errorf("command error result=%+v error=%v", failed, err)
	}
	result.CommandErrorOutputPreserved = true
	return json.NewEncoder(os.Stdout).Encode(result)
}

func runRecovery(ctx context.Context, client *rados.Client, directory string) error {
	result := recoveryReport{}
	if command, err := client.ManagerCommand(ctx, []byte(`{"prefix":"pg dump","format":"json"}`), nil); err != nil || len(command.Output) == 0 {
		return fmt.Errorf("manager before failover: %w", err)
	}
	result.ManagerBeforeFailover = true
	if err := signalAndWait(ctx, directory, "ready", "continue"); err != nil {
		return err
	}
	if command, err := client.ManagerCommand(ctx, []byte(`{"prefix":"pg dump","format":"json"}`), nil); err != nil || len(command.Output) == 0 {
		return fmt.Errorf("manager after failover: %w", err)
	}
	result.ManagerAfterFailover = true
	if err := signalAndWait(ctx, directory, "failover-done", "managerless"); err != nil {
		return err
	}
	pool, err := client.OpenPool(ctx, "p11-data")
	if err != nil {
		return err
	}
	object := pool.Object("managerless-existing-client")
	if _, err := object.WriteFull(ctx, []byte("managerless")); err != nil {
		return err
	}
	data, _, err := object.Read(ctx, 0, 64)
	if err != nil || string(data) != "managerless" {
		return fmt.Errorf("managerless read=%q: %w", data, err)
	}
	result.IOAfterManagerLoss = true
	return json.NewEncoder(os.Stdout).Encode(result)
}

func runLeastPrivilege(ctx context.Context, client *rados.Client) error {
	pool, err := client.OpenPool(ctx, "p11-data")
	if err != nil {
		return err
	}
	object := pool.Object("least-privilege")
	if _, err := object.WriteFull(ctx, []byte("least")); err != nil {
		return err
	}
	data, _, err := object.Read(ctx, 0, 64)
	if err != nil || string(data) != "least" {
		return fmt.Errorf("least privilege read=%q: %w", data, err)
	}
	return json.NewEncoder(os.Stdout).Encode(leastPrivilegeReport{WriteReadWithoutManager: true})
}

func signalAndWait(ctx context.Context, directory, signal, wait string) error {
	if directory == "" {
		return errors.New("missing coordination directory")
	}
	if err := os.WriteFile(filepath.Join(directory, signal), []byte("ready\n"), 0o600); err != nil {
		return err
	}
	path := filepath.Join(directory, wait)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(path); err == nil {
			return nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
