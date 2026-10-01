package main

import (
	"io"
	"sync"
)

type traceOutput struct {
	writer io.Writer
	mu     sync.Mutex
	err    error
}

func (output *traceOutput) Write(data []byte) (int, error) {
	output.mu.Lock()
	defer output.mu.Unlock()
	written, err := output.writer.Write(data)
	if err == nil && written != len(data) {
		err = io.ErrShortWrite
	}
	if output.err == nil {
		output.err = err
	}
	return written, err
}

func (output *traceOutput) Err() error {
	output.mu.Lock()
	defer output.mu.Unlock()
	return output.err
}