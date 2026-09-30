//go:build p12diagnostics

package main

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	rados "github.com/otuschhoff/rados-go"
)

type resourceSample struct {
	ElapsedNS  int64  `json:"elapsed_ns"`
	RSSBytes   uint64 `json:"rss_bytes"`
	Goroutines int    `json:"goroutines"`
	HeapBytes  uint64 `json:"heap_bytes"`
	Inflight   *int   `json:"inflight"`
}

type credentialRenewal struct {
	Service             string    `json:"service"`
	ServiceID           int32     `json:"service_id"`
	SessionID           uint64    `json:"session_id"`
	DueGeneration       uint64    `json:"due_generation"`
	CompletedGeneration uint64    `json:"completed_generation"`
	DueAt               time.Time `json:"due_at"`
	CompletedAt         time.Time `json:"completed_at"`
}

type credentialRenewalAbandonment struct {
	Service          string    `json:"service"`
	ServiceID        int32     `json:"service_id"`
	SessionID        uint64    `json:"session_id"`
	DueGeneration    uint64    `json:"due_generation"`
	ClosedGeneration uint64    `json:"closed_generation"`
	DueAt            time.Time `json:"due_at"`
	ClosedAt         time.Time `json:"closed_at"`
}

type report struct {
	Transport                     string                         `json:"transport"`
	RequestedDurationNS           int64                          `json:"requested_duration_ns"`
	ElapsedNS                     int64                          `json:"elapsed_ns"`
	MonotonicDurationSatisfied    bool                           `json:"monotonic_duration_satisfied"`
	Operations                    uint64                         `json:"operations"`
	Writes                        uint64                         `json:"writes"`
	AppendOnceVerifications       uint64                         `json:"append_once_verifications"`
	DuplicateMutationsDetected    uint64                         `json:"duplicate_mutations_detected"`
	Reads                         uint64                         `json:"reads"`
	Stats                         uint64                         `json:"stats"`
	Removes                       uint64                         `json:"removes"`
	Reconnects                    uint64                         `json:"reconnects"`
	LongestConnectionNS           int64                          `json:"longest_connection_ns"`
	CredentialRenewals            []credentialRenewal            `json:"credential_renewals"`
	CredentialRenewalAbandonments []credentialRenewalAbandonment `json:"credential_renewal_abandonments"`
	Samples                       []resourceSample               `json:"samples"`
	InflightMeasurement           string                         `json:"inflight_measurement"`
	MaximumConfiguredSampleCount  int                            `json:"maximum_configured_sample_count"`
}

type configuration struct {
	monitors         []string
	key              []byte
	fsid             string
	pool             string
	entity           string
	transport        string
	duration         time.Duration
	reconnect        time.Duration
	sampleInterval   time.Duration
	workloadInterval time.Duration
	operationTimeout time.Duration
	renewalSettle    time.Duration
	controlDir       string
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	var monitors, keyFile, fsid, pool, entity, transport, controlDir string
	var duration, reconnect, sampleInterval, workloadInterval, operationTimeout, renewalSettle time.Duration
	flag.StringVar(&monitors, "monitors", "", "comma-separated monitor endpoints")
	flag.StringVar(&keyFile, "key-file", "", "CephX key file")
	flag.StringVar(&fsid, "fsid", "", "required cluster FSID")
	flag.StringVar(&pool, "pool", "", "pool name")
	flag.StringVar(&entity, "entity", "client.p12", "CephX entity")
	flag.StringVar(&transport, "transport", "", "secure or crc")
	flag.DurationVar(&duration, "duration", 24*time.Hour, "monotonic soak duration")
	flag.DurationVar(&reconnect, "reconnect-interval", 30*time.Minute, "maximum connection lifetime")
	flag.DurationVar(&sampleInterval, "sample-interval", time.Minute, "runtime sample interval")
	flag.DurationVar(&workloadInterval, "workload-interval", 0, "minimum interval between verified CRUD cycles")
	flag.DurationVar(&operationTimeout, "operation-timeout", 30*time.Second, "per-operation timeout")
	flag.DurationVar(&renewalSettle, "renewal-settle-timeout", 20*time.Minute, "maximum post-workload credential renewal quiescence")
	flag.StringVar(&controlDir, "control-dir", "", "optional harness coordination directory")
	flag.Parse()

	key, err := os.ReadFile(keyFile)
	if err != nil {
		return fmt.Errorf("read key: %w", err)
	}
	cfg := configuration{
		monitors: strings.Split(monitors, ","), key: bytes.TrimSpace(key), fsid: fsid,
		pool: pool, entity: entity, transport: transport, duration: duration,
		reconnect: reconnect, sampleInterval: sampleInterval, workloadInterval: workloadInterval,
		operationTimeout: operationTimeout, renewalSettle: renewalSettle, controlDir: controlDir,
	}
	if err := validate(cfg); err != nil {
		return err
	}

	result, err := soak(cfg)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(result)
}

func validate(cfg configuration) error {
	if len(cfg.monitors) == 0 || len(cfg.monitors) == 1 && cfg.monitors[0] == "" || len(cfg.key) == 0 || cfg.fsid == "" || cfg.pool == "" || cfg.entity == "" {
		return errors.New("monitors, key-file, fsid, pool, and entity are required")
	}
	if cfg.transport != "secure" && cfg.transport != "crc" {
		return errors.New("transport must be secure or crc")
	}
	if cfg.duration <= 0 || cfg.reconnect <= 0 || cfg.sampleInterval <= 0 || cfg.operationTimeout <= 0 || cfg.renewalSettle <= 0 {
		return errors.New("duration values must be positive")
	}
	if cfg.workloadInterval < 0 {
		return errors.New("workload interval must be nonnegative")
	}
	return nil
}

func soak(cfg configuration) (report, error) {
	start := time.Now()
	deadline := start.Add(cfg.duration)
	nextReconnect := start
	nextSample := start
	maxSamples := int(cfg.duration/cfg.sampleInterval) + 2
	result := report{
		Transport: cfg.transport, RequestedDurationNS: cfg.duration.Nanoseconds(),
		InflightMeasurement:           "unavailable through the public API; reported as null",
		MaximumConfiguredSampleCount:  maxSamples,
		CredentialRenewalAbandonments: make([]credentialRenewalAbandonment, 0),
		Samples:                       make([]resourceSample, 0, maxSamples),
	}
	renewals := newRenewalCollector()

	var client *rados.Client
	var pool rados.Pool
	var connectionStarted time.Time
	defer func() {
		if client != nil {
			_ = client.Close()
		}
	}()

	for iteration := uint64(0); ; iteration++ {
		now := time.Now()
		if !now.Before(deadline) {
			result.ElapsedNS = time.Since(start).Nanoseconds()
			result.MonotonicDurationSatisfied = result.ElapsedNS >= cfg.duration.Nanoseconds()
			break
		}
		if client == nil || !now.Before(nextReconnect) {
			if client != nil {
				lifetime := time.Since(connectionStarted)
				if lifetime > time.Duration(result.LongestConnectionNS) {
					result.LongestConnectionNS = lifetime.Nanoseconds()
				}
				if err := client.Close(); err != nil {
					return result, fmt.Errorf("close client: %w", err)
				}
				result.Reconnects++
			}
			var err error
			client, pool, err = connect(cfg, renewals)
			if err != nil {
				return result, err
			}
			connectionStarted = time.Now()
			nextReconnect = connectionStarted.Add(cfg.reconnect)
		}

		if !time.Now().Before(nextSample) {
			if len(result.Samples) >= maxSamples {
				return result, errors.New("runtime sample bound exceeded")
			}
			sample, err := sampleResources(start)
			if err != nil {
				return result, err
			}
			result.Samples = append(result.Samples, sample)
			nextSample = nextSample.Add(cfg.sampleInterval)
			if nextSample.Before(time.Now()) {
				nextSample = time.Now().Add(cfg.sampleInterval)
			}
		}
		if err := awaitChurn(cfg.controlDir, cfg.transport); err != nil {
			return result, err
		}

		operationDeadline := boundedOperationDeadline(now, deadline, cfg.operationTimeout)
		operationCtx, cancel := context.WithDeadline(context.Background(), operationDeadline)
		err := verifiedCRUD(operationCtx, pool, cfg.transport, iteration, &result)
		operationContextErr := operationCtx.Err()
		cancel()
		if err != nil {
			if soakDeadlineReached(time.Now(), deadline, operationContextErr) {
				result.ElapsedNS = time.Since(start).Nanoseconds()
				result.MonotonicDurationSatisfied = true
				break
			}
			return result, err
		}
		waitForWorkloadInterval(deadline, cfg.workloadInterval)
	}

	if lifetime := time.Since(connectionStarted); lifetime > time.Duration(result.LongestConnectionNS) {
		result.LongestConnectionNS = lifetime.Nanoseconds()
	}
	var err error
	if _, err = renewals.waitCompleted(cfg.renewalSettle); err != nil {
		return result, err
	}
	if err = client.Close(); err != nil {
		return result, fmt.Errorf("close client: %w", err)
	}
	client = nil
	result.CredentialRenewals, result.CredentialRenewalAbandonments, err = renewals.finalized()
	if err != nil {
		return result, err
	}
	return result, nil
}

func boundedOperationDeadline(now, soakDeadline time.Time, operationTimeout time.Duration) time.Time {
	operationDeadline := now.Add(operationTimeout)
	if soakDeadline.Before(operationDeadline) {
		return soakDeadline
	}
	return operationDeadline
}

func soakDeadlineReached(now, soakDeadline time.Time, operationErr error) bool {
	return !now.Before(soakDeadline) && errors.Is(operationErr, context.DeadlineExceeded)
}

func waitForWorkloadInterval(deadline time.Time, interval time.Duration) {
	remaining := time.Until(deadline)
	if interval <= 0 || remaining <= 0 {
		return
	}
	timer := time.NewTimer(min(interval, remaining))
	defer timer.Stop()
	<-timer.C
}

type renewalCollector struct {
	nextSessionID atomic.Uint64
	mu            sync.Mutex
	pending       map[uint64][]rados.P12SessionDiagnostic
	completedList []credentialRenewal
	abandonedList []credentialRenewalAbandonment
	invalid       error
}

func newRenewalCollector() *renewalCollector {
	return &renewalCollector{pending: make(map[uint64][]rados.P12SessionDiagnostic)}
}

func (collector *renewalCollector) NextSessionID() uint64 {
	return collector.nextSessionID.Add(1)
}

func (collector *renewalCollector) ObserveP12SessionDiagnostic(event rados.P12SessionDiagnostic) {
	if event.Service != "monitor" && event.Service != "osd" {
		return
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if collector.invalid != nil {
		return
	}
	if event.SessionClosed {
		queue := collector.pending[event.SessionID]
		if len(queue) == 0 {
			collector.invalid = fmt.Errorf("session %d closed without pending renewal evidence", event.SessionID)
			return
		}
		for _, due := range queue {
			if due.Service != event.Service || due.ServiceID != event.ServiceID || event.Generation < due.Generation || event.Timestamp.Before(due.Timestamp) {
				collector.invalid = fmt.Errorf("inconsistent renewal abandonment for session %d", event.SessionID)
				return
			}
			collector.abandonedList = append(collector.abandonedList, credentialRenewalAbandonment{
				Service: due.Service, ServiceID: due.ServiceID, SessionID: event.SessionID,
				DueGeneration: due.Generation, ClosedGeneration: event.Generation,
				DueAt: due.Timestamp, ClosedAt: event.Timestamp,
			})
		}
		delete(collector.pending, event.SessionID)
		return
	}
	if event.RenewalDue {
		queue := collector.pending[event.SessionID]
		if len(queue) > 0 {
			due := queue[len(queue)-1]
			if due.Service != event.Service || due.ServiceID != event.ServiceID || event.Generation < due.Generation || event.Timestamp.Before(due.Timestamp) {
				collector.invalid = fmt.Errorf("inconsistent repeated renewal due for session %d", event.SessionID)
			}
			if event.Generation == due.Generation {
				return
			}
		}
		collector.pending[event.SessionID] = append(queue, event)
		return
	}
	queue := collector.pending[event.SessionID]
	if len(queue) == 0 || queue[0].Service != event.Service || queue[0].ServiceID != event.ServiceID || queue[0].Generation >= event.Generation || queue[0].Timestamp.After(event.Timestamp) {
		prior := make([]string, 0)
		for _, completed := range collector.completedList {
			if completed.SessionID == event.SessionID {
				prior = append(prior, fmt.Sprintf("%d->%d", completed.DueGeneration, completed.CompletedGeneration))
			}
		}
		collector.invalid = fmt.Errorf("renewal completion without ordered due evidence for %s session %d generation %d (prior=%v)", event.Service, event.SessionID, event.Generation, prior)
		return
	}
	due := queue[0]
	if len(queue) == 1 {
		delete(collector.pending, event.SessionID)
	} else {
		collector.pending[event.SessionID] = queue[1:]
	}
	collector.completedList = append(collector.completedList, credentialRenewal{
		Service: event.Service, ServiceID: event.ServiceID, SessionID: event.SessionID,
		DueGeneration: due.Generation, CompletedGeneration: event.Generation,
		DueAt: due.Timestamp, CompletedAt: event.Timestamp,
	})
}

func (collector *renewalCollector) finalized() ([]credentialRenewal, []credentialRenewalAbandonment, error) {
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if collector.invalid != nil {
		return nil, nil, collector.invalid
	}
	if len(collector.pending) != 0 {
		return nil, nil, errors.New("pending credential renewals remain after client close")
	}
	completed := append([]credentialRenewal{}, collector.completedList...)
	abandoned := append([]credentialRenewalAbandonment{}, collector.abandonedList...)
	slices.SortFunc(completed, func(left, right credentialRenewal) int {
		if order := cmp.Compare(left.SessionID, right.SessionID); order != 0 {
			return order
		}
		return cmp.Compare(left.DueGeneration, right.DueGeneration)
	})
	slices.SortFunc(abandoned, func(left, right credentialRenewalAbandonment) int {
		if order := cmp.Compare(left.SessionID, right.SessionID); order != 0 {
			return order
		}
		return cmp.Compare(left.DueGeneration, right.DueGeneration)
	})
	return completed, abandoned, nil
}

func (collector *renewalCollector) completed() ([]credentialRenewal, error) {
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if collector.invalid != nil {
		return nil, collector.invalid
	}
	result := append([]credentialRenewal(nil), collector.completedList...)
	slices.SortFunc(result, func(left, right credentialRenewal) int {
		if order := cmp.Compare(left.SessionID, right.SessionID); order != 0 {
			return order
		}
		return cmp.Compare(left.DueGeneration, right.DueGeneration)
	})
	hasMonitor, hasOSD := false, false
	for _, renewal := range result {
		hasMonitor = hasMonitor || renewal.Service == "monitor"
		hasOSD = hasOSD || renewal.Service == "osd"
	}
	if !hasMonitor || !hasOSD || len(collector.pending) > 0 {
		pending := make([]string, 0, len(collector.pending))
		for sessionID, queue := range collector.pending {
			for _, due := range queue {
				pending = append(pending, fmt.Sprintf("%s/%d/session-%d/generation-%d", due.Service, due.ServiceID, sessionID, due.Generation))
			}
		}
		slices.Sort(pending)
		return nil, fmt.Errorf("completed credential renewals for monitor and OSD sessions with no pending renewals are required (monitor=%t, osd=%t, pending=%v)", hasMonitor, hasOSD, pending)
	}
	return result, nil
}

func (collector *renewalCollector) waitCompleted(timeout time.Duration) ([]credentialRenewal, error) {
	deadline := time.NewTimer(timeout)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer deadline.Stop()
	defer ticker.Stop()
	for {
		collector.mu.Lock()
		invalid := collector.invalid
		pending := len(collector.pending)
		collector.mu.Unlock()
		if invalid != nil || pending == 0 {
			return collector.completed()
		}
		select {
		case <-deadline.C:
			return collector.completed()
		case <-ticker.C:
		}
	}
}

func awaitChurn(directory, transport string) error {
	if directory == "" {
		return nil
	}
	pause := directory + "/pause"
	if _, err := os.Stat(pause); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("check churn pause: %w", err)
	}
	acknowledgement := directory + "/paused-" + transport
	if err := os.WriteFile(acknowledgement, nil, 0o600); err != nil {
		return fmt.Errorf("acknowledge churn pause: %w", err)
	}
	defer os.Remove(acknowledgement)
	for {
		if _, err := os.Stat(pause); errors.Is(err, os.ErrNotExist) {
			return nil
		} else if err != nil {
			return fmt.Errorf("wait for churn resume: %w", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func connect(cfg configuration, observer rados.P12DiagnosticObserver) (*rados.Client, rados.Pool, error) {
	mode := rados.SecurityModeSecure
	if cfg.transport == "crc" {
		mode = rados.SecurityModeCRC
	}
	client, err := rados.NewP12DiagnosticClient(rados.Config{
		Monitors: cfg.monitors, Entity: cfg.entity, ClusterFSID: cfg.fsid, Key: cfg.key,
		SecurityMode: mode, OperationTimeout: cfg.operationTimeout,
	}, observer)
	if err != nil {
		return nil, rados.Pool{}, fmt.Errorf("new client: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.operationTimeout)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		_ = client.Close()
		return nil, rados.Pool{}, fmt.Errorf("connect client: %w", err)
	}
	pool, err := client.OpenPool(ctx, cfg.pool)
	if err != nil {
		_ = client.Close()
		return nil, rados.Pool{}, fmt.Errorf("open pool: %w", err)
	}
	return client, pool, nil
}

func verifiedCRUD(ctx context.Context, pool rados.Pool, transport string, iteration uint64, result *report) error {
	name := fmt.Sprintf("p12-%s-%020d", transport, iteration)
	prefix := []byte("p12:" + transport + ":" + strconv.FormatUint(iteration, 10))
	marker := []byte(":once")
	payload := append(append([]byte(nil), prefix...), marker...)
	object := pool.Object(name)
	if _, err := object.WriteFull(ctx, prefix); err != nil {
		return fmt.Errorf("write %s: %w", name, err)
	}
	if _, err := object.Append(ctx, marker); err != nil {
		return fmt.Errorf("append %s: %w", name, err)
	}
	data, readInfo, err := object.Read(ctx, 0, uint64(len(payload)+1))
	if err != nil || !bytes.Equal(data, payload) || readInfo.Version == 0 {
		if err == nil && bytes.Count(data, marker) != 1 {
			result.DuplicateMutationsDetected++
		}
		return fmt.Errorf("read %s version=%d match=%t: %w", name, readInfo.Version, bytes.Equal(data, payload), err)
	}
	info, err := object.Stat(ctx)
	if err != nil || info.Size != uint64(len(payload)) || info.Version < readInfo.Version {
		return fmt.Errorf("stat %s size=%d version=%d: %w", name, info.Size, info.Version, err)
	}
	if _, err := object.Remove(ctx); err != nil {
		return fmt.Errorf("remove %s: %w", name, err)
	}
	if _, err := object.Stat(ctx); !errors.Is(err, rados.ErrNotFound) {
		return fmt.Errorf("removed object %s still present: %v", name, err)
	}
	result.Writes++
	result.AppendOnceVerifications++
	result.Reads++
	result.Stats++
	result.Removes++
	result.Operations++
	return nil
}

func sampleResources(start time.Time) (resourceSample, error) {
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return resourceSample{}, fmt.Errorf("read /proc/self/statm: %w", err)
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return resourceSample{}, errors.New("invalid /proc/self/statm")
	}
	residentPages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return resourceSample{}, fmt.Errorf("parse resident pages: %w", err)
	}
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	return resourceSample{
		ElapsedNS: time.Since(start).Nanoseconds(), RSSBytes: residentPages * uint64(os.Getpagesize()),
		Goroutines: runtime.NumGoroutine(), HeapBytes: memory.HeapAlloc, Inflight: nil,
	}, nil
}
