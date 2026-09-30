package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/otuschhoff/rados-go/internal/msgr"
)

func TestSummarizeTimingValidation(t *testing.T) {
	stages := []string{"read_enter", "submit_enter", "admitted", "write_begin", "write_end", "frame_received", "reply_delivered", "submit_return", "read_return"}
	var events []msgr.RequestTimingEvent
	for index, stage := range stages {
		events = append(events, msgr.RequestTimingEvent{Stage: stage, TransactionID: 1, At: time.Unix(1, int64(index)*1000)})
	}
	for _, testCase := range []struct {
		name      string
		events    []msgr.RequestTimingEvent
		errorText string
	}{
		{name: "complete", events: events},
		{name: "missing", events: events[:4], errorText: "missing interval"},
		{name: "duplicate", events: append(append([]msgr.RequestTimingEvent(nil), events...), events[0]), errorText: "duplicate stage"},
		{name: "empty", errorText: "no measured requests"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			fileName := filepath.Join(t.TempDir(), "timing.json")
			requests := [][]msgr.RequestTimingEvent{testCase.events}
			if testCase.events == nil {
				requests = nil
			}
			data, err := json.Marshal(requests)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(fileName, data, 0600); err != nil {
				t.Fatal(err)
			}
			err = summarize(fileName)
			if testCase.errorText == "" && err != nil {
				t.Fatal(err)
			}
			if testCase.errorText != "" && (err == nil || !strings.Contains(err.Error(), testCase.errorText)) {
				t.Fatalf("error = %v, want %s", err, testCase.errorText)
			}
		})
	}
}
