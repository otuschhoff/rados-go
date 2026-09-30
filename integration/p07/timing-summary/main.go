package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"time"

	"github.com/otuschhoff/rados-go/internal/msgr"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: timing-summary REQUEST_TIMING_JSON")
		os.Exit(2)
	}
	if err := summarize(os.Args[1]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func summarize(fileName string) error {
	file, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer file.Close()
	var requests [][]msgr.RequestTimingEvent
	if err := json.NewDecoder(file).Decode(&requests); err != nil {
		return err
	}
	pairs := [][2]string{
		{"read_enter", "submit_enter"},
		{"submit_enter", "admitted"},
		{"admitted", "write_begin"},
		{"write_begin", "write_end"},
		{"write_end", "frame_received"},
		{"frame_received", "reply_delivered"},
		{"reply_delivered", "submit_return"},
		{"submit_return", "read_return"},
		{"read_enter", "read_return"},
	}
	fmt.Println("interval\tsamples\tmean_us\tp50_us\tp95_us\tp99_us\tnegative_intervals")
	for _, pair := range pairs {
		var durations []time.Duration
		var total time.Duration
		negative := 0
		for _, events := range requests {
			stages := make(map[string]time.Time)
			for _, event := range events {
				if _, exists := stages[event.Stage]; exists {
					return fmt.Errorf("duplicate stage %s: retries require attempt-specific analysis", event.Stage)
				}
				stages[event.Stage] = event.At
			}
			begin, end := stages[pair[0]], stages[pair[1]]
			if begin.IsZero() || end.IsZero() {
				return fmt.Errorf("missing interval %v", pair)
			}
			duration := end.Sub(begin)
			if duration < 0 {
				negative++
			}
			durations = append(durations, duration)
			total += duration
		}
		if len(durations) == 0 {
			return fmt.Errorf("no measured requests")
		}
		sort.Slice(durations, func(left, right int) bool { return durations[left] < durations[right] })
		percentile := func(fraction float64) float64 {
			return float64(durations[int(math.Ceil(float64(len(durations))*fraction))-1]) / 1000
		}
		fmt.Printf("%s->%s\t%d\t%.3f\t%.3f\t%.3f\t%.3f\t%d\n", pair[0], pair[1], len(durations), float64(total)/float64(len(durations))/1000, percentile(.5), percentile(.95), percentile(.99), negative)
	}
	return nil
}
