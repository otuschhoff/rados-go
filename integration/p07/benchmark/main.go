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
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	rados "github.com/otuschhoff/rados-go"
)

var (
	sizes         = []uint64{4096, 65536, 1048576, 4194304}
	concurrencies = []int{1, 16, 64}
	workloads     = []string{"read", "write", "mixed"}
)

type environment struct {
	GOOS      string `json:"GOOS"`
	GOARCH    string `json:"GOARCH"`
	GoVersion string `json:"go_version"`
}

type resources struct {
	CPUUserNS      uint64 `json:"cpu_user_ns"`
	CPUSystemNS    uint64 `json:"cpu_system_ns"`
	Allocations    uint64 `json:"allocations"`
	AllocatedBytes uint64 `json:"allocated_bytes"`
	MaxRSSBytes    uint64 `json:"max_rss_bytes"`
}

type row struct {
	SizeBytes                uint64  `json:"size_bytes"`
	Concurrency              int     `json:"concurrency"`
	Workload                 string  `json:"workload"`
	Operations               uint64  `json:"operations"`
	Bytes                    uint64  `json:"bytes"`
	ElapsedNS                uint64  `json:"elapsed_ns"`
	ThroughputBytesPerSecond float64 `json:"throughput_bytes_per_second"`
	IOPS                     float64 `json:"iops"`
	P50NS                    uint64  `json:"p50_ns"`
	P95NS                    uint64  `json:"p95_ns"`
	P99NS                    uint64  `json:"p99_ns"`
}

type report struct {
	Implementation string      `json:"implementation"`
	Transport      string      `json:"transport"`
	Environment    environment `json:"environment"`
	Resources      resources   `json:"resources"`
	Rows           []row       `json:"rows"`
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

func main() {
	var monitorsArg string
	var keyFile string
	var fsid string
	var poolName string
	var transport string

	flag.StringVar(&monitorsArg, "monitors", "", "comma-separated monitor endpoints")
	flag.StringVar(&keyFile, "key-file", "", "path to cephx key for client.p07")
	flag.StringVar(&fsid, "fsid", "", "cluster fsid")
	flag.StringVar(&poolName, "pool", "", "pool name")
	flag.StringVar(&transport, "transport", "", "secure|crc")
	flag.Parse()

	if err := run(monitorsArg, keyFile, fsid, poolName, transport); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func run(monitorsArg, keyFile, fsid, poolName, transport string) error {
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
	if poolName == "" {
		return &benchError{message: "missing -pool"}
	}
	securityMode, err := parseTransport(transport)
	if err != nil {
		return err
	}

	client, err := rados.New(rados.Config{
		Monitors:         monitors,
		Entity:           "client.p07",
		ClusterFSID:      fsid,
		Key:              key,
		SecurityMode:     securityMode,
		OperationTimeout: 10 * time.Minute,
	})
	if err != nil {
		return &benchError{message: "create client", err: err}
	}
	defer func() {
		_ = client.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()

	if err := client.Connect(ctx); err != nil {
		return &benchError{message: "connect", err: err}
	}
	pool, err := client.OpenPool(ctx, poolName)
	if err != nil {
		return &benchError{message: "open pool", err: err}
	}

	before, err := snapshotResources()
	if err != nil {
		return err
	}

	rows := make([]row, 0, len(sizes)*len(concurrencies)*len(workloads))
	runID := uint64(time.Now().UnixNano())
	for _, size := range sizes {
		for _, concurrency := range concurrencies {
			for _, workload := range workloads {
				r, err := runRow(ctx, pool, runID, size, concurrency, workload)
				if err != nil {
					return &benchError{message: fmt.Sprintf("size=%d concurrency=%d workload=%s", size, concurrency, workload), err: err}
				}
				rows = append(rows, r)
				runID++
			}
		}
	}

	after, err := snapshotResources()
	if err != nil {
		return err
	}

	res, err := deltaResources(before, after)
	if err != nil {
		return err
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

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(output)
}

func runRow(parent context.Context, pool rados.Pool, runID, size uint64, concurrency int, workload string) (row, error) {
	if concurrency <= 0 {
		return row{}, &benchError{message: "invalid concurrency"}
	}

	opPerWorker := 2
	totalOps, ok := safeMul(uint64(concurrency), uint64(opPerWorker))
	if !ok {
		return row{}, &benchError{message: "operations overflow"}
	}
	totalBytes, ok := safeMul(totalOps, size)
	if !ok {
		return row{}, &benchError{message: "bytes overflow"}
	}
	objectName := func(workerID int) string {
		return fmt.Sprintf("p07-go-%d-%d-%s-w%d", runID, size, workload, workerID)
	}
	if workload == "read" || workload == "mixed" {
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
					data, _, err := obj.Read(ctx, 0, size)
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
					record(i, begin)
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
				if ctx.Err() != nil {
					measuredWG.Done()
					cleanupObject(obj)
					return
				}
				beginRead := time.Now()
				data, _, err := obj.Read(ctx, 0, size)
				if err != nil {
					once.Do(func() {
						errCh <- &benchError{message: "mixed read", err: err}
						cancel()
					})
					measuredWG.Done()
					cleanupObject(obj)
					return
				}
				if uint64(len(data)) != size {
					once.Do(func() {
						errCh <- &benchError{message: "mixed short read"}
						cancel()
					})
					measuredWG.Done()
					cleanupObject(obj)
					return
				}
				if !bytes.Equal(data, payload) {
					once.Do(func() {
						errCh <- &benchError{message: "mixed read payload mismatch"}
						cancel()
					})
					measuredWG.Done()
					cleanupObject(obj)
					return
				}
				record(0, beginRead)

				if ctx.Err() != nil {
					measuredWG.Done()
					cleanupObject(obj)
					return
				}
				beginWrite := time.Now()
				if _, err := obj.WriteFull(ctx, payload); err != nil {
					once.Do(func() {
						errCh <- &benchError{message: "mixed write", err: err}
						cancel()
					})
					measuredWG.Done()
					cleanupObject(obj)
					return
				}
				record(1, beginWrite)
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
			cleanupObject(obj)
		}()
	}

	readyWG.Wait()
	startTime := time.Now()
	close(start)
	measuredWG.Wait()
	elapsed := time.Since(startTime)
	allWG.Wait()

	select {
	case err := <-errCh:
		if err != nil {
			return row{}, err
		}
	default:
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
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := object.Remove(cleanupCtx)
	if err != nil && !errors.Is(err, rados.ErrNotFound) {
		return
	}
}

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
