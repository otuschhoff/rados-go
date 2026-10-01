package main

import (
	"errors"
	"io"
	"testing"
)

type testTraceWriter func([]byte) (int, error)

func (writer testTraceWriter) Write(data []byte) (int, error) { return writer(data) }

func TestTraceOutput(t *testing.T) {
	failure := errors.New("trace write failed")
	for _, test := range []struct {
		name   string
		writer io.Writer
		want   error
	}{
		{"success", io.Discard, nil},
		{"short", testTraceWriter(func(data []byte) (int, error) { return len(data) - 1, nil }), io.ErrShortWrite},
		{"error", testTraceWriter(func([]byte) (int, error) { return 0, failure }), failure},
	} {
		t.Run(test.name, func(t *testing.T) {
			output := &traceOutput{writer: test.writer}
			_, err := output.Write([]byte("trace data"))
			if !errors.Is(err, test.want) || !errors.Is(output.Err(), test.want) {
				t.Fatalf("Write error = %v, retained error = %v, want %v", err, output.Err(), test.want)
			}
			output.writer = io.Discard
			_, _ = output.Write([]byte("later data"))
			if !errors.Is(output.Err(), test.want) {
				t.Fatalf("later write erased error: %v", output.Err())
			}
		})
	}
}