package main

import "testing"

func TestBackgroundAllocationMode(t *testing.T) {
	for _, workers := range []string{"", "0"} {
		if _, _, err := startBackgroundWork(workers, true); err == nil {
			t.Fatal("allocation workload without workers accepted")
		}
	}
	count, stop, err := startBackgroundWork("2", true)
	if err != nil || count != 2 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	stop()
	stop()
}

func TestBackgroundCPU(t *testing.T) {
	for _, value := range []string{"", "0", "2"} {
		count, stop, err := startBackgroundCPU(value)
		if err != nil || count < 0 {
			t.Fatalf("workers %q: count=%d err=%v", value, count, err)
		}
		stop()
		stop()
	}
	for _, value := range []string{"-1", "33", "invalid"} {
		if _, _, err := startBackgroundCPU(value); err == nil {
			t.Fatalf("accepted workers %q", value)
		}
	}
}
