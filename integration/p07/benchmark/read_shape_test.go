package main

import (
	"fmt"
	"testing"
)

func TestReadShape(t *testing.T) {
	shape, err := parseReadShape("", "", false, false)
	if err != nil || shape.size != 65536 || shape.concurrency != 16 || shape.operationCount(256) != 4096 || shape.objectSet() != "p07-shared-read-0..15" {
		t.Fatalf("default=%+v err=%v", shape, err)
	}
	for _, size := range []string{"4096", "65536", "1048576", "4194304"} {
		for _, concurrency := range []string{"16", "32", "64"} {
			for _, seedOnly := range []bool{false, true} {
				shape, err := parseReadShape(size, concurrency, !seedOnly, seedOnly)
				if err != nil || fmt.Sprint(shape.size) != size || fmt.Sprint(shape.concurrency) != concurrency {
					t.Fatalf("shape=%+v err=%v", shape, err)
				}
				prefix := "p07-shared-read-"
				if size != "65536" {
					prefix = fmt.Sprintf("p07-shared-read-size-%s-", size)
				}
				if shape.operationCount(1024) != shape.concurrency*1024 || shape.operationCount(readWarmupPerWorker) != shape.concurrency*8 || shape.objectSet() != fmt.Sprintf("%s0..%d", prefix, shape.concurrency-1) || shape.objectName(0) != prefix+"0" {
					t.Fatal("shape counts or object set mismatch")
				}
				if !shape.matches(shape.size, shape.concurrency, "read") || shape.matches(shape.size, shape.concurrency, "write") || shape.matches(shape.size+1, shape.concurrency, "read") || shape.matches(shape.size, shape.concurrency+1, "read") {
					t.Fatal("shape row filter mismatch")
				}
			}
		}
	}
}

func TestReadShapeRejectsOverrides(t *testing.T) {
	for _, values := range [][2]string{{"65536", ""}, {"", "16"}} {
		if _, err := parseReadShape(values[0], values[1], false, false); err == nil {
			t.Fatal("normal mode accepted shape override")
		}
	}
	for _, values := range [][2]string{{"0", ""}, {"65537", ""}, {"bad", ""}, {"", "1"}, {"", "65"}, {"", "bad"}, {"", "016"}} {
		if _, err := parseReadShape(values[0], values[1], true, false); err == nil {
			t.Fatal("invalid shape accepted")
		}
	}
}
