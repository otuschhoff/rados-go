//go:build linux

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/bits"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	rados "github.com/otuschhoff/rados-go"
	"github.com/otuschhoff/rados-go/internal/msgr"
	"github.com/otuschhoff/rados-go/internal/perfbaseline"
)

var (
	sizes         = []uint64{4096, 65536, 1048576, 4194304}
	concurrencies = []int{1, 16, 64}
	workloads     = []string{"read", "write", "mixed"}
)

type environment struct {
	GOOS       string `json:"GOOS"`
	GOARCH     string `json:"GOARCH"`
	GoVersion  string `json:"go_version"`
	GOMAXPROCS int    `json:"gomaxprocs,omitempty"`
}

type resources struct {
	CPUUserNS      uint64  `json:"cpu_user_ns"`
	CPUSystemNS    uint64  `json:"cpu_system_ns"`
	Allocations    uint64  `json:"allocations"`
	AllocatedBytes uint64  `json:"allocated_bytes"`
	MaxRSSBytes    uint64  `json:"max_rss_bytes"`
	GCCycles       *uint32 `json:"gc_cycles,omitempty"`
	GCPauseNS      *uint64 `json:"gc_pause_ns,omitempty"`
}

type row struct {
	SizeBytes                uint64         `json:"size_bytes"`
	Concurrency              int            `json:"concurrency"`
	Workload                 string         `json:"workload"`
	Operations               uint64         `json:"operations"`
	Bytes                    uint64         `json:"bytes"`
	ElapsedNS                uint64         `json:"elapsed_ns"`
	ThroughputBytesPerSecond float64        `json:"throughput_bytes_per_second"`
	IOPS                     float64        `json:"iops"`
	P50NS                    uint64         `json:"p50_ns"`
	P95NS                    uint64         `json:"p95_ns"`
	P99NS                    uint64         `json:"p99_ns"`
	Parity                   *parityMetrics `json:"parity,omitempty"`
}

type report struct {
	Implementation string                 `json:"implementation"`
	Transport      string                 `json:"transport"`
	Environment    environment            `json:"environment"`
	Resources      resources              `json:"resources"`
	Rows           []row                  `json:"rows"`
	Diagnostic     *diagnosticMethodology `json:"diagnostic,omitempty"`
	OfferedLoad    *offeredResult         `json:"offered_load,omitempty"`
}

type diagnosticMethodology struct {
	WarmupOperations      int                `json:"warmup_operations"`
	ResourceScope         string             `json:"resource_scope"`
	ObjectSet             string             `json:"object_set"`
	BackgroundCPUWorkers  int                `json:"background_cpu_workers,omitempty"`
	ReadAPI               string             `json:"read_api,omitempty"`
	OperationsPerWorker   int                `json:"operations_per_worker,omitempty"`
	ScratchSlots          int                `json:"scratch_slots,omitempty"`
	AdmissionWindow       int                `json:"admission_window,omitempty"`
	BackgroundAllocations bool               `json:"background_allocations,omitempty"`
	Scratch               *scratchStatistics `json:"scratch,omitempty"`
}

type benchError struct {
	message string
	err     error
}

func (e *benchError) Error() string {
	if e == nil {
		return ""
	}
	if e.err == nil {
		return e.message
	}
	return e.message + ": " + e.err.Error()
}

type resourceSnapshot struct {
	rusage   syscall.Rusage
	memstats runtime.MemStats
}

type rowResources struct {
	SizeBytes    uint64    `json:"size_bytes"`
	Concurrency  int       `json:"concurrency"`
	Workload     string    `json:"workload"`
	HeapAlloc    uint64    `json:"heap_alloc"`
	HeapInuse    uint64    `json:"heap_inuse"`
	HeapIdle     uint64    `json:"heap_idle"`
	HeapReleased uint64    `json:"heap_released"`
	GCCycles     uint32    `json:"gc_cycles"`
	Resources    resources `json:"resources"`
}

func main() {
	var monitorsArg string
	var keyFile string
	var fsid string
	var poolName string
	var transport string
	var entity string

	flag.StringVar(&monitorsArg, "monitors", "", "comma-separated monitor endpoints")
	flag.StringVar(&keyFile, "key-file", "", "path to cephx key or keyring")
	flag.StringVar(&entity, "entity", "client.p07", "cephx client entity")
	flag.StringVar(&fsid, "fsid", "", "cluster fsid")
	flag.StringVar(&poolName, "pool", "", "pool name")
	flag.StringVar(&transport, "transport", "", "secure|crc")
	flag.Parse()

	if err := run(monitorsArg, keyFile, fsid, poolName, transport, entity); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func run(monitorsArg, keyFile, fsid, poolName, transport, entity string) (resultErr error) {
	qualification, err := parseQualificationConfig(os.Getenv)
	if err != nil {
		return err
	}
	namespace, err := parityNamespace(os.Getenv("P07_PARITY_NAMESPACE"))
	if err != nil {
		return err
	}
	retentionWindows, err := parseRetentionWindows(os.Getenv)
	if err != nil {
		return err
	}
	if err := validateOfferedObservationConfig(os.Getenv); err != nil {
		return err
	}
	offered, err := parseOfferedConfig(os.Getenv)
	if err != nil {
		return err
	}
	offeredReported := false
	defer func() {
		if offered != nil && resultErr != nil && !offeredReported {
			resultErr = errors.Join(resultErr, writeOfferedSetupFailure(*offered, transport, resultErr))
		}
	}()
	diagnostic := os.Getenv("P07_READ_DIAGNOSTIC") == "1"
	seedOnly := os.Getenv("P07_SEED_ONLY") == "1"
	matrix, err := parseMatrixExperiment(os.Getenv, diagnostic || seedOnly)
	if err != nil {
		return err
	}
	if namespace != "" && !diagnostic && !seedOnly && (matrix.size == 0 || matrix.concurrency == 0 || matrix.workload == "" || qualification == nil && matrix.operations <= 2) {
		return fmt.Errorf("parity mode requires explicit sustained size, concurrency and workload")
	}
	shape, err := parseReadShape(os.Getenv("P07_READ_SIZE"), os.Getenv("P07_READ_CONCURRENCY"), diagnostic, seedOnly)
	if err != nil {
		return err
	}
	experiment, err := parseReadExperiment(os.Getenv("P07_OPERATIONS_PER_WORKER"), os.Getenv("P07_SCRATCH_SLOTS"), os.Getenv("P07_ADMISSION_WINDOW"), os.Getenv("P07_BACKGROUND_ALLOCATIONS"), os.Getenv("P07_READ_DIAGNOSTIC") == "1")
	if err != nil {
		return err
	}
	monitors := splitMonitors(monitorsArg)
	if len(monitors) == 0 {
		return &benchError{message: "invalid -monitors"}
	}
	if keyFile == "" {
		return &benchError{message: "missing -key-file"}
	}
	key, err := os.ReadFile(keyFile)
	if err != nil {
		return &benchError{message: "read -key-file", err: err}
	}
	key = []byte(strings.TrimSpace(string(key)))
	if len(key) == 0 {
		return &benchError{message: "empty -key-file"}
	}
	if key[0] == '[' {
		key, err = rados.LoadKeyring(keyFile, entity)
		if err != nil {
			return &benchError{message: "load -key-file keyring", err: err}
		}
	}
	if poolName == "" {
		return &benchError{message: "missing -pool"}
	}
	securityMode, err := parseTransport(transport)
	if err != nil {
		return err
	}

	operationTimeout := 10 * time.Minute
	if qualification != nil {
		if transport != "secure" || runtime.GOMAXPROCS(0) != 10 {
			return fmt.Errorf("qualification requires secure transport and ten active Ps")
		}
		operationTimeout = 30 * time.Second
	}
	client, err := rados.New(rados.Config{
		Monitors:         monitors,
		Entity:           entity,
		ClusterFSID:      fsid,
		Key:              key,
		SecurityMode:     securityMode,
		OperationTimeout: operationTimeout,
	})
	if err != nil {
		return &benchError{message: "create client", err: err}
	}
	defer func() {
		_ = client.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	if experiment.slots != 0 {
		if err := configureScratch(client, experiment.slots); err != nil {
			return err
		}
	}
	if diagnostic {
		ctx = readAdmissionContext(ctx, experiment.window, shape.concurrency)
	}
	if evidenceFile := os.Getenv("P07_MODE_EVIDENCE_FILE"); evidenceFile != "" {
		file, err := os.OpenFile(evidenceFile, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return fmt.Errorf("create fresh mode evidence: %w", err)
		}
		collector := perfbaseline.NewModeCollector(transport)
		ctx = msgr.WithModeObserver(ctx, collector.Observe)
		defer func() {
			_ = client.Close()
			data, err := json.MarshalIndent(collector.Evidence(), "", "  ")
			if err == nil {
				_, err = file.Write(append(data, '\n'))
			}
			resultErr = errors.Join(resultErr, err, file.Close())
		}()
	}

	if err := client.Connect(ctx); err != nil {
		return &benchError{message: "connect", err: err}
	}
	pool, err := client.OpenPool(ctx, poolName)
	if err != nil {
		return &benchError{message: "open pool", err: err}
	}
	if namespace != "" {
		pool = pool.WithNamespace(namespace)
	}
	if qualification != nil {
		return runQualification(ctx, pool, *qualification, matrix)
	}
	if retentionWindows != 0 {
		if transport != "secure" {
			return fmt.Errorf("retention requires secure transport")
		}
		return runRetention(ctx, pool, retentionWindows, matrix.size, matrix.concurrency, matrix.workload, os.Stdout)
	}
	if seedOnly {
		for workerID := 0; workerID < shape.concurrency; workerID++ {
			if err := writeSeed(ctx, pool.Object(shape.objectName(workerID)), makePayload(shape.size, 1, uint64(workerID))); err != nil {
				return err
			}
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]bool{"seeded": true})
	}
	if offered != nil {
		if transport != "secure" || runtime.GOMAXPROCS(0) != 10 {
			return fmt.Errorf("offered load requires secure transport and ten active Ps")
		}
		offeredReported = true
		return runOfferedDiagnostic(ctx, client, pool, *offered, transport)
	}
	var timings []*msgr.RequestTiming
	if os.Getenv("P07_TIMING_FILE") != "" {
		if os.Getenv("P07_READ_DIAGNOSTIC") != "1" {
			return &benchError{message: "request timing requires diagnostic mode"}
		}
		timings = make([]*msgr.RequestTiming, shape.operationCount(experiment.operations))
		ctx = context.WithValue(ctx, benchmarkTimingKey{}, timings)
	}
	var stopTrace func() error
	if traceFile := os.Getenv("P07_TRACE_FILE"); traceFile != "" {
		if os.Getenv("P07_READ_DIAGNOSTIC") != "1" {
			return &benchError{message: "execution tracing requires diagnostic mode"}
		}
		file, err := os.Create(traceFile)
		if err != nil {
			return err
		}
		output := &traceOutput{writer: file}
		if err := trace.Start(output); err != nil {
			_ = file.Close()
			return err
		}
		stopTrace = func() error {
			trace.Stop()
			return errors.Join(output.Err(), file.Close())
		}
		defer func() {
			if stopTrace != nil {
				resultErr = errors.Join(resultErr, stopTrace())
			}
		}()
	}
	var stopCPUProfile func() error
	if profileFile := os.Getenv("P07_CPU_PROFILE"); profileFile != "" {
		file, err := os.Create(profileFile)
		if err != nil {
			return err
		}
		if err := pprof.StartCPUProfile(file); err != nil {
			_ = file.Close()
			return err
		}
		stopCPUProfile = func() error {
			pprof.StopCPUProfile()
			return file.Close()
		}
		defer func() {
			if stopCPUProfile != nil {
				_ = stopCPUProfile()
			}
		}()
	}

	if os.Getenv("P07_BACKGROUND_WORKERS") != "" && os.Getenv("P07_READ_DIAGNOSTIC") != "1" {
		return &benchError{message: "background CPU workers require read diagnostic mode"}
	}
	if os.Getenv("P07_READ_INTO") != "" && os.Getenv("P07_READ_DIAGNOSTIC") != "1" && namespace == "" {
		return &benchError{message: "caller-buffer reads require read diagnostic mode"}
	}
	backgroundWorkers, stopBackground, err := startBackgroundWork(os.Getenv("P07_BACKGROUND_WORKERS"), experiment.allocationLoad)
	if err != nil {
		return err
	}
	defer stopBackground()
	scratchBefore := scratchCounters(client)
	before, err := snapshotResources()
	if err != nil {
		return err
	}

	rows := make([]row, 0, len(sizes)*len(concurrencies)*len(workloads))
	var resourceRows []rowResources
	runID := uint64(time.Now().UnixNano())
	if os.Getenv("P07_READ_DIAGNOSTIC") == "1" || namespace != "" {
		runID = 1
	}
	rowSizes, rowConcurrencies := sizes, concurrencies
	if diagnostic {
		rowSizes, rowConcurrencies = []uint64{shape.size}, []int{shape.concurrency}
	} else {
		if matrix.size != 0 {
			rowSizes = []uint64{matrix.size}
		}
		if matrix.concurrency != 0 {
			rowConcurrencies = []int{matrix.concurrency}
		}
	}
	for _, size := range rowSizes {
		for _, concurrency := range rowConcurrencies {
			for _, workload := range workloads {
				if !diagnostic && matrix.workload != "" && workload != matrix.workload {
					continue
				}
				if diagnostic && !shape.matches(size, concurrency, workload) {
					continue
				}
				r, err := runRow(ctx, pool, runID, size, concurrency, workload)
				if err != nil {
					return &benchError{message: fmt.Sprintf("size=%d concurrency=%d workload=%s", size, concurrency, workload), err: err}
				}
				rows = append(rows, r)
				if os.Getenv("P07_RESOURCE_FILE") != "" {
					snapshot, err := snapshotResources()
					if err != nil {
						return err
					}
					cumulative, err := deltaResources(before, snapshot)
					if err != nil {
						return err
					}
					resourceRows = append(resourceRows, rowResources{SizeBytes: size, Concurrency: concurrency, Workload: workload,
						HeapAlloc: snapshot.memstats.HeapAlloc, HeapInuse: snapshot.memstats.HeapInuse,
						HeapIdle: snapshot.memstats.HeapIdle, HeapReleased: snapshot.memstats.HeapReleased,
						GCCycles: snapshot.memstats.NumGC - before.memstats.NumGC, Resources: cumulative})
				}
				runID++
			}
		}
	}

	after, err := snapshotResources()
	scratchAfter := scratchCounters(client)
	stopBackground()
	if err != nil {
		return err
	}
	if stopTrace != nil {
		stopErr := stopTrace()
		stopTrace = nil
		if stopErr != nil {
			return stopErr
		}
	}
	if stopCPUProfile != nil {
		stopErr := stopCPUProfile()
		stopCPUProfile = nil
		if stopErr != nil {
			return stopErr
		}
	}

	res, err := deltaResources(before, after)
	if err != nil {
		return err
	}
	if os.Getenv("P07_READ_DIAGNOSTIC") == "1" {
		gcCycles := after.memstats.NumGC - before.memstats.NumGC
		gcPauseNS := after.memstats.PauseTotalNs - before.memstats.PauseTotalNs
		res.GCCycles = &gcCycles
		res.GCPauseNS = &gcPauseNS
	}
	if profileFile := os.Getenv("P07_MEMORY_PROFILE"); profileFile != "" {
		file, err := os.Create(profileFile)
		if err != nil {
			return err
		}
		writeErr := pprof.Lookup("allocs").WriteTo(file, 0)
		closeErr := file.Close()
		if err := errors.Join(writeErr, closeErr); err != nil {
			return err
		}
	}
	if resourceFile := os.Getenv("P07_RESOURCE_FILE"); resourceFile != "" {
		runtime.GC()
		snapshot, err := snapshotResources()
		if err != nil {
			return err
		}
		cumulative, err := deltaResources(before, snapshot)
		if err != nil {
			return err
		}
		resourceRows = append(resourceRows, rowResources{Workload: "post_gc", HeapAlloc: snapshot.memstats.HeapAlloc,
			HeapInuse: snapshot.memstats.HeapInuse, HeapIdle: snapshot.memstats.HeapIdle,
			HeapReleased: snapshot.memstats.HeapReleased, GCCycles: snapshot.memstats.NumGC - before.memstats.NumGC, Resources: cumulative})
		heapFile, err := os.Create(resourceFile + ".heap.pprof")
		if err != nil {
			return err
		}
		if err := errors.Join(pprof.WriteHeapProfile(heapFile), heapFile.Close()); err != nil {
			return err
		}
		file, err := os.Create(resourceFile)
		if err != nil {
			return err
		}
		encodeErr := json.NewEncoder(file).Encode(resourceRows)
		if err := errors.Join(encodeErr, file.Close()); err != nil {
			return err
		}
	}
	if timings != nil {
		if err := client.Close(); err != nil {
			return err
		}
		events := make([][]msgr.RequestTimingEvent, len(timings))
		for index, timing := range timings {
			if timing == nil {
				return &benchError{message: "missing request timing"}
			}
			events[index] = timing.Events()
		}
		file, err := os.Create(os.Getenv("P07_TIMING_FILE"))
		if err != nil {
			return err
		}
		encodeErr := json.NewEncoder(file).Encode(events)
		closeErr := file.Close()
		if err := errors.Join(encodeErr, closeErr); err != nil {
			return err
		}
	}

	output := report{
		Implementation: "go",
		Transport:      transport,
		Environment: environment{
			GOOS:      runtime.GOOS,
			GOARCH:    runtime.GOARCH,
			GoVersion: runtime.Version(),
		},
		Resources: res,
		Rows:      rows,
	}
	output.Environment.GOMAXPROCS = runtime.GOMAXPROCS(0)
	if os.Getenv("P07_READ_DIAGNOSTIC") == "1" {
		output.Diagnostic = &diagnosticMethodology{WarmupOperations: shape.operationCount(readWarmupPerWorker), ResourceScope: "warmup_and_measured_reads", ObjectSet: shape.objectSet(), BackgroundCPUWorkers: backgroundWorkers}
		output.Diagnostic.ReadAPI = "read"
		output.Diagnostic.OperationsPerWorker = experiment.operations
		output.Diagnostic.ScratchSlots = experiment.slots
		output.Diagnostic.AdmissionWindow = experiment.window
		output.Diagnostic.BackgroundAllocations = experiment.allocationLoad
		if experiment.slots != 0 {
			counters := scratchAfter
			counters.Hits -= scratchBefore.Hits
			counters.Misses -= scratchBefore.Misses
			counters.Bypasses -= scratchBefore.Bypasses
			output.Diagnostic.Scratch = &counters
		}
		if os.Getenv("P07_READ_INTO") == "1" {
			output.Diagnostic.ReadAPI = "read_into"
		}
	}

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(output)
}

func runRow(parent context.Context, pool rados.Pool, runID, size uint64, concurrency int, workload string) (row, error) {
	matched := os.Getenv("P07_PARITY_NAMESPACE") != ""
	if concurrency <= 0 {
		return row{}, &benchError{message: "invalid concurrency"}
	}

	matrix, err := parseMatrixExperiment(os.Getenv, os.Getenv("P07_READ_DIAGNOSTIC") == "1")
	if err != nil {
		return row{}, err
	}
	opPerWorker := matrix.operations
	if os.Getenv("P07_READ_DIAGNOSTIC") == "1" {
		experiment, err := parseReadExperiment(os.Getenv("P07_OPERATIONS_PER_WORKER"), "", "", "", true)
		if err != nil {
			return row{}, err
		}
		opPerWorker = experiment.operations
	}
	totalOps, ok := safeMul(uint64(concurrency), uint64(opPerWorker))
	if !ok {
		return row{}, &benchError{message: "operations overflow"}
	}
	totalBytes, ok := safeMul(totalOps, size)
	if !ok {
		return row{}, &benchError{message: "bytes overflow"}
	}
	objectName := func(workerID int) string {
		if os.Getenv("P07_READ_DIAGNOSTIC") == "1" {
			return (readShape{size: size, concurrency: concurrency}).objectName(workerID)
		}
		if matched {
			return parityObjectName(size, concurrency, workload, workerID)
		}
		return fmt.Sprintf("p07-go-%d-%d-%s-w%d", runID, size, workload, workerID)
	}
	if (workload == "read" || workload == "mixed") && os.Getenv("P07_READ_DIAGNOSTIC") != "1" {
		for workerID := 0; workerID < concurrency; workerID++ {
			obj := pool.Object(objectName(workerID))
			if err := writeSeed(parent, obj, makePayload(size, runID, uint64(workerID))); err != nil {
				for cleanupID := 0; cleanupID <= workerID; cleanupID++ {
					cleanupObject(pool.Object(objectName(cleanupID)))
				}
				return row{}, &benchError{message: "seed object", err: err}
			}
		}
	}

	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	latencies := make([]uint64, totalOps)
	errCh := make(chan error, 1)
	var once sync.Once

	start := make(chan struct{})
	verificationStart := make(chan struct{})
	var readyWG sync.WaitGroup
	var measuredWG sync.WaitGroup
	var allWG sync.WaitGroup
	readyWG.Add(concurrency)
	measuredWG.Add(concurrency)
	allWG.Add(concurrency)

	for workerID := 0; workerID < concurrency; workerID++ {
		workerID := workerID
		go func() {
			defer allWG.Done()

			obj := pool.Object(objectName(workerID))
			payload := makePayload(size, runID, uint64(workerID))
			var destination []byte
			if os.Getenv("P07_READ_INTO") == "1" {
				destination = make([]byte, int(size))
			}
			read := func(readCtx context.Context) ([]byte, error) {
				release, err := acquireRead(readCtx)
				if err != nil {
					return nil, err
				}
				defer release()
				if destination != nil {
					count, _, err := obj.ReadInto(readCtx, 0, destination)
					return destination[:count], err
				}
				data, _, err := obj.Read(readCtx, 0, size)
				return data, err
			}
			if os.Getenv("P07_READ_DIAGNOSTIC") == "1" || matrix.operations > 2 {
				for warmup := 0; warmup < readWarmupPerWorker; warmup++ {
					if workload == "write" || workload == "mixed" && warmup%2 == 1 {
						if _, err := obj.WriteFull(ctx, payload); err != nil {
							once.Do(func() { errCh <- &benchError{message: "warmup write", err: err}; cancel() })
							break
						}
						continue
					}
					data, err := read(ctx)
					if err != nil || !bytes.Equal(data, payload) {
						once.Do(func() { errCh <- &benchError{message: "warmup read", err: err}; cancel() })
						break
					}
				}
			}

			readyWG.Done()
			select {
			case <-start:
			case <-ctx.Done():
				measuredWG.Done()
				cleanupObject(obj)
				return
			}

			index := workerID * opPerWorker
			record := func(pos int, begin time.Time) {
				ns := time.Since(begin).Nanoseconds()
				if ns < 0 {
					ns = 0
				}
				latencies[index+pos] = uint64(ns)
			}

			switch workload {
			case "read":
				for i := 0; i < opPerWorker; i++ {
					if ctx.Err() != nil {
						measuredWG.Done()
						cleanupObject(obj)
						return
					}
					begin := time.Now()
					readCtx := ctx
					var timing *msgr.RequestTiming
					if timings, ok := ctx.Value(benchmarkTimingKey{}).([]*msgr.RequestTiming); ok {
						readCtx, timing = msgr.WithRequestTiming(ctx)
						timings[index+i] = timing
					}
					data, err := read(readCtx)
					if timing != nil {
						timing.Record("read_return", 0, time.Now())
					}
					record(i, begin)
					if err != nil {
						once.Do(func() {
							errCh <- &benchError{message: "read object", err: err}
							cancel()
						})
						measuredWG.Done()
						cleanupObject(obj)
						return
					}
					if uint64(len(data)) != size {
						once.Do(func() {
							errCh <- &benchError{message: "short read"}
							cancel()
						})
						measuredWG.Done()
						cleanupObject(obj)
						return
					}
					if !bytes.Equal(data, payload) {
						once.Do(func() {
							errCh <- &benchError{message: "read payload mismatch"}
							cancel()
						})
						measuredWG.Done()
						cleanupObject(obj)
						return
					}
				}
			case "write":
				for i := 0; i < opPerWorker; i++ {
					if ctx.Err() != nil {
						measuredWG.Done()
						cleanupObject(obj)
						return
					}
					begin := time.Now()
					if _, err := obj.WriteFull(ctx, payload); err != nil {
						once.Do(func() {
							errCh <- &benchError{message: "write object", err: err}
							cancel()
						})
						measuredWG.Done()
						cleanupObject(obj)
						return
					}
					record(i, begin)
				}
			case "mixed":
				for operation := 0; operation < opPerWorker; operation++ {
					if ctx.Err() != nil {
						measuredWG.Done()
						cleanupObject(obj)
						return
					}
					begin := time.Now()
					var err error
					if operation%2 == 0 {
						var data []byte
						data, err = read(ctx)
						record(operation, begin)
						if err == nil && !bytes.Equal(data, payload) {
							err = fmt.Errorf("mixed short read or payload mismatch")
						}
					} else {
						_, err = obj.WriteFull(ctx, payload)
						record(operation, begin)
					}
					if err != nil {
						once.Do(func() { errCh <- &benchError{message: "mixed operation", err: err}; cancel() })
						measuredWG.Done()
						cleanupObject(obj)
						return
					}
				}
			default:
				once.Do(func() {
					errCh <- &benchError{message: "invalid workload"}
					cancel()
				})
				measuredWG.Done()
				cleanupObject(obj)
				return
			}

			measuredWG.Done()
			if matched {
				<-verificationStart
				data, _, verifyErr := obj.Read(ctx, 0, size)
				if verifyErr != nil || !bytes.Equal(data, payload) {
					once.Do(func() { errCh <- fmt.Errorf("final parity payload verification failed: %v", verifyErr) })
				}
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cleanupCancel()
				if _, cleanupErr := obj.Remove(cleanupCtx); cleanupErr != nil {
					once.Do(func() { errCh <- cleanupErr })
				}
				if _, statErr := obj.Stat(cleanupCtx); !errors.Is(statErr, rados.ErrNotFound) {
					once.Do(func() { errCh <- fmt.Errorf("parity fixture not removed: %v", statErr) })
				}
			} else {
				cleanupObject(obj)
			}
		}()
	}

	readyWG.Wait()
	var metrics *parityMetrics
	var measuredBefore resourceSnapshot
	if matched {
		metrics = &parityMetrics{}
		metrics.RSSBefore, err = residentBytes()
		if err != nil {
			cancel()
			close(start)
			close(verificationStart)
			allWG.Wait()
			return row{}, err
		}
		measuredBefore, err = snapshotResources()
		if err != nil {
			cancel()
			close(start)
			close(verificationStart)
			allWG.Wait()
			return row{}, err
		}
	}
	startTime := time.Now()
	close(start)
	measuredWG.Wait()
	elapsed := time.Since(startTime)
	if matched {
		measuredAfter, snapshotErr := snapshotResources()
		if snapshotErr == nil {
			metrics.Resources, snapshotErr = deltaResources(measuredBefore, measuredAfter)
		}
		if snapshotErr == nil {
			metrics.RSSAfter, snapshotErr = residentBytes()
		}
		if snapshotErr != nil {
			once.Do(func() { errCh <- snapshotErr })
		}
	}
	close(verificationStart)
	allWG.Wait()

	select {
	case err := <-errCh:
		if err != nil {
			return row{}, err
		}
	default:
	}
	if matched {
		runtime.GC()
		var memory runtime.MemStats
		runtime.ReadMemStats(&memory)
		metrics.HeapPostGC = memory.HeapAlloc
		metrics.RSSCleanup, err = residentBytes()
		if err != nil {
			return row{}, err
		}
		metrics.PayloadVerified, metrics.CleanupVerified = true, true
	}

	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	elapsedNS := uint64(0)
	if elapsed > 0 {
		elapsedNS = uint64(elapsed.Nanoseconds())
	}

	throughput := 0.0
	iops := 0.0
	if elapsedNS > 0 {
		seconds := float64(elapsedNS) / float64(time.Second)
		throughput = float64(totalBytes) / seconds
		iops = float64(totalOps) / seconds
	}

	return row{
		SizeBytes:                size,
		Concurrency:              concurrency,
		Workload:                 workload,
		Operations:               totalOps,
		Bytes:                    totalBytes,
		ElapsedNS:                elapsedNS,
		ThroughputBytesPerSecond: throughput,
		IOPS:                     iops,
		P50NS:                    percentile(latencies, 0.50),
		P95NS:                    percentile(latencies, 0.95),
		P99NS:                    percentile(latencies, 0.99),
		Parity:                   metrics,
	}, nil
}

func parseTransport(value string) (rados.SecurityMode, error) {
	switch value {
	case "secure":
		return rados.SecurityModeSecure, nil
	case "crc":
		return rados.SecurityModeCRC, nil
	default:
		return 0, &benchError{message: "invalid -transport (expected secure|crc)"}
	}
}

func splitMonitors(monitors string) []string {
	parts := strings.Split(monitors, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func makePayload(size, runID, workerID uint64) []byte {
	payload := make([]byte, size)
	if size == 0 {
		return payload
	}
	state := runID ^ (workerID << 17) ^ 0x9E3779B97F4A7C15
	if os.Getenv("P07_READ_DIAGNOSTIC") == "1" || os.Getenv("P07_SEED_ONLY") == "1" || os.Getenv("P07_PARITY_NAMESPACE") != "" {
		state = runID ^ (workerID << 19) ^ 0x9E3779B97F4A7C15
	}
	for i := range payload {
		state ^= state << 7
		state ^= state >> 9
		state ^= state << 8
		payload[i] = byte(state)
	}
	return payload
}

func writeSeed(ctx context.Context, object rados.ObjectRef, payload []byte) error {
	_, err := object.WriteFull(ctx, payload)
	return err
}

func cleanupObject(object rados.ObjectRef) {
	if os.Getenv("P07_READ_DIAGNOSTIC") == "1" {
		return
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := object.Remove(cleanupCtx)
	if err != nil && !errors.Is(err, rados.ErrNotFound) {
		return
	}
}

type benchmarkTimingKey struct{}

func snapshotResources() (resourceSnapshot, error) {
	var snap resourceSnapshot
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &snap.rusage); err != nil {
		return resourceSnapshot{}, &benchError{message: "getrusage", err: err}
	}
	runtime.ReadMemStats(&snap.memstats)
	return snap, nil
}

func deltaResources(before, after resourceSnapshot) (resources, error) {
	userNS, err := deltaTimevalNS(before.rusage.Utime, after.rusage.Utime)
	if err != nil {
		return resources{}, err
	}
	systemNS, err := deltaTimevalNS(before.rusage.Stime, after.rusage.Stime)
	if err != nil {
		return resources{}, err
	}
	allocations := safeSub(after.memstats.Mallocs, before.memstats.Mallocs)
	allocatedBytes := safeSub(after.memstats.TotalAlloc, before.memstats.TotalAlloc)
	maxRSS := uint64(after.rusage.Maxrss)
	if runtime.GOOS == "linux" {
		product, ok := safeMul(maxRSS, 1024)
		if !ok {
			return resources{}, &benchError{message: "max_rss_bytes overflow"}
		}
		maxRSS = product
	}
	return resources{
		CPUUserNS:      userNS,
		CPUSystemNS:    systemNS,
		Allocations:    allocations,
		AllocatedBytes: allocatedBytes,
		MaxRSSBytes:    maxRSS,
	}, nil
}

func deltaTimevalNS(before, after syscall.Timeval) (uint64, error) {
	beforeNS, ok := timevalToNS(before)
	if !ok {
		return 0, &benchError{message: "time conversion overflow (before)"}
	}
	afterNS, ok := timevalToNS(after)
	if !ok {
		return 0, &benchError{message: "time conversion overflow (after)"}
	}
	return safeSub(afterNS, beforeNS), nil
}

func timevalToNS(value syscall.Timeval) (uint64, bool) {
	if value.Sec < 0 || value.Usec < 0 {
		return 0, false
	}
	sec := uint64(value.Sec)
	usec := uint64(value.Usec)
	secNS, ok := safeMul(sec, uint64(time.Second))
	if !ok {
		return 0, false
	}
	usecNS, ok := safeMul(usec, 1000)
	if !ok {
		return 0, false
	}
	ns, carry := bits.Add64(secNS, usecNS, 0)
	if carry != 0 {
		return 0, false
	}
	return ns, true
}

func safeMul(left, right uint64) (uint64, bool) {
	high, low := bits.Mul64(left, right)
	return low, high == 0
}

func safeSub(left, right uint64) uint64 {
	if left < right {
		return 0
	}
	return left - right
}

func percentile(sorted []uint64, probability float64) uint64 {
	if len(sorted) == 0 {
		return 0
	}
	if probability <= 0 {
		return sorted[0]
	}
	if probability >= 1 {
		return sorted[len(sorted)-1]
	}
	rank := int(math.Ceil(probability*float64(len(sorted)))) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}
