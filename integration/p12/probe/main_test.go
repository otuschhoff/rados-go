//go:build p12diagnostics

package main

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	rados "github.com/otuschhoff/rados-go"
)

func TestBoundedOperationDeadline(t *testing.T) {
	now := time.Unix(100, 0)
	soakDeadline := now.Add(time.Minute)
	if got := boundedOperationDeadline(now, soakDeadline, 2*time.Minute); !got.Equal(soakDeadline) {
		t.Fatalf("bounded deadline = %v, want %v", got, soakDeadline)
	}
	if got, want := boundedOperationDeadline(now, soakDeadline, 30*time.Second), now.Add(30*time.Second); !got.Equal(want) {
		t.Fatalf("operation deadline = %v, want %v", got, want)
	}
}

func TestSoakDeadlineReachedRequiresDeadlineContextError(t *testing.T) {
	deadline := time.Unix(100, 0)
	if !soakDeadlineReached(deadline, deadline, context.DeadlineExceeded) {
		t.Fatal("deadline context error at soak deadline was rejected")
	}
	if soakDeadlineReached(deadline.Add(-time.Nanosecond), deadline, context.DeadlineExceeded) {
		t.Fatal("deadline context error before soak deadline was accepted")
	}
	if soakDeadlineReached(deadline, deadline, context.Canceled) {
		t.Fatal("canceled context at soak deadline was accepted")
	}
}

func TestValidateRejectsNegativeWorkloadInterval(t *testing.T) {
	cfg := configuration{
		monitors: []string{"127.0.0.1:3300"}, key: []byte("key"), fsid: "fsid", pool: "pool", entity: "client.p12", transport: "secure",
		duration: time.Second, reconnect: time.Second, sampleInterval: time.Second, workloadInterval: -time.Second, operationTimeout: time.Second, renewalSettle: time.Second,
	}
	if err := validate(cfg); err == nil || !strings.Contains(err.Error(), "workload interval") {
		t.Fatalf("validate error = %v, want workload interval error", err)
	}
}

func TestRenewalCollectorRequiresOrderedMonitorAndOSDCompletion(t *testing.T) {
	collector := newRenewalCollector()
	now := time.Unix(100, 0).UTC()
	monitorID := collector.NextSessionID()
	osdID := collector.NextSessionID()
	collector.ObserveP12SessionDiagnostic(rados.P12SessionDiagnostic{RenewalDue: true, Service: "monitor", SessionID: monitorID, Generation: 2, Timestamp: now})
	collector.ObserveP12SessionDiagnostic(rados.P12SessionDiagnostic{RenewalDue: true, Service: "monitor", SessionID: monitorID, Generation: 2, Timestamp: now})
	collector.ObserveP12SessionDiagnostic(rados.P12SessionDiagnostic{Service: "monitor", SessionID: monitorID, Generation: 4, Timestamp: now.Add(time.Second)})
	collector.ObserveP12SessionDiagnostic(rados.P12SessionDiagnostic{RenewalDue: true, Service: "osd", ServiceID: 7, SessionID: osdID, Generation: 6, Timestamp: now})
	collector.ObserveP12SessionDiagnostic(rados.P12SessionDiagnostic{RenewalDue: true, Service: "osd", ServiceID: 7, SessionID: osdID, Generation: 8, Timestamp: now.Add(time.Second)})
	collector.ObserveP12SessionDiagnostic(rados.P12SessionDiagnostic{Service: "osd", ServiceID: 7, SessionID: osdID, Generation: 8, Timestamp: now.Add(2 * time.Second)})
	collector.ObserveP12SessionDiagnostic(rados.P12SessionDiagnostic{Service: "osd", ServiceID: 7, SessionID: osdID, Generation: 10, Timestamp: now.Add(3 * time.Second)})

	completed, err := collector.completed()
	if err != nil || len(completed) != 3 {
		t.Fatalf("completed = %+v, error = %v", completed, err)
	}
	if completed[0].Service != "monitor" || completed[1].Service != "osd" || completed[1].ServiceID != 7 || completed[0].DueGeneration >= completed[0].CompletedGeneration || completed[1].CompletedGeneration != completed[2].DueGeneration || completed[2].DueGeneration >= completed[2].CompletedGeneration {
		t.Fatalf("completion evidence = %+v", completed)
	}
}

func TestRenewalCollectorRejectsInconsistentRepeatedDue(t *testing.T) {
	collector := newRenewalCollector()
	now := time.Unix(100, 0).UTC()
	sessionID := collector.NextSessionID()
	collector.ObserveP12SessionDiagnostic(rados.P12SessionDiagnostic{RenewalDue: true, Service: "monitor", SessionID: sessionID, Generation: 2, Timestamp: now})
	collector.ObserveP12SessionDiagnostic(rados.P12SessionDiagnostic{RenewalDue: true, Service: "osd", ServiceID: 1, SessionID: sessionID, Generation: 2, Timestamp: now})
	if _, err := collector.completed(); err == nil {
		t.Fatal("inconsistent repeated renewal due was accepted")
	}
}

func TestRenewalCollectorRejectsCompletionWithoutDue(t *testing.T) {
	collector := newRenewalCollector()
	collector.ObserveP12SessionDiagnostic(rados.P12SessionDiagnostic{Service: "monitor", SessionID: collector.NextSessionID(), Generation: 4, Timestamp: time.Now()})
	if _, err := collector.completed(); err == nil {
		t.Fatal("completion without renewal due was accepted")
	}
}

func TestRenewalCollectorRejectsPendingAfterCompletedServices(t *testing.T) {
	collector := newRenewalCollector()
	now := time.Unix(100, 0).UTC()
	monitorID := collector.NextSessionID()
	osdID := collector.NextSessionID()
	collector.ObserveP12SessionDiagnostic(rados.P12SessionDiagnostic{RenewalDue: true, Service: "monitor", SessionID: monitorID, Generation: 2, Timestamp: now})
	collector.ObserveP12SessionDiagnostic(rados.P12SessionDiagnostic{Service: "monitor", SessionID: monitorID, Generation: 4, Timestamp: now.Add(time.Second)})
	collector.ObserveP12SessionDiagnostic(rados.P12SessionDiagnostic{RenewalDue: true, Service: "osd", ServiceID: 1, SessionID: osdID, Generation: 2, Timestamp: now})
	collector.ObserveP12SessionDiagnostic(rados.P12SessionDiagnostic{Service: "osd", ServiceID: 1, SessionID: osdID, Generation: 4, Timestamp: now.Add(time.Second)})
	collector.ObserveP12SessionDiagnostic(rados.P12SessionDiagnostic{RenewalDue: true, Service: "osd", ServiceID: 1, SessionID: osdID, Generation: 6, Timestamp: now.Add(2 * time.Second)})
	if _, err := collector.completed(); err == nil {
		t.Fatal("pending renewal after completed services was accepted")
	}
	if _, err := collector.waitCompleted(time.Millisecond); err == nil {
		t.Fatal("pending renewal survived the quiescence deadline")
	}
}

func TestRenewalCollectorDiscardsPendingForClosedSession(t *testing.T) {
	collector := newRenewalCollector()
	now := time.Unix(100, 0).UTC()
	monitorID := collector.NextSessionID()
	osdID := collector.NextSessionID()
	abandonedID := collector.NextSessionID()
	collector.ObserveP12SessionDiagnostic(rados.P12SessionDiagnostic{RenewalDue: true, Service: "monitor", SessionID: monitorID, Generation: 2, Timestamp: now})
	collector.ObserveP12SessionDiagnostic(rados.P12SessionDiagnostic{Service: "monitor", SessionID: monitorID, Generation: 4, Timestamp: now.Add(time.Second)})
	collector.ObserveP12SessionDiagnostic(rados.P12SessionDiagnostic{RenewalDue: true, Service: "osd", ServiceID: 1, SessionID: osdID, Generation: 2, Timestamp: now})
	collector.ObserveP12SessionDiagnostic(rados.P12SessionDiagnostic{Service: "osd", ServiceID: 1, SessionID: osdID, Generation: 4, Timestamp: now.Add(time.Second)})
	collector.ObserveP12SessionDiagnostic(rados.P12SessionDiagnostic{RenewalDue: true, Service: "osd", ServiceID: 1, SessionID: abandonedID, Generation: 2, Timestamp: now})
	collector.ObserveP12SessionDiagnostic(rados.P12SessionDiagnostic{SessionClosed: true, Service: "osd", ServiceID: 1, SessionID: abandonedID, Generation: 3, Timestamp: now.Add(2 * time.Second)})

	completed, err := collector.completed()
	if err != nil || len(completed) != 2 {
		t.Fatalf("completed = %+v, error = %v", completed, err)
	}
	finalCompleted, abandoned, err := collector.finalized()
	if err != nil || len(finalCompleted) != 2 || len(abandoned) != 1 || abandoned[0].SessionID != abandonedID || abandoned[0].DueGeneration != 2 || abandoned[0].ClosedGeneration != 3 {
		t.Fatalf("abandoned = %+v", abandoned)
	}
}

func TestRenewalCollectorFinalizedReturnsEmptyCollections(t *testing.T) {
	completed, abandoned, err := newRenewalCollector().finalized()
	if err != nil || completed == nil || abandoned == nil || len(completed) != 0 || len(abandoned) != 0 {
		t.Fatalf("finalized = (%+v, %+v, %v), want non-nil empty collections", completed, abandoned, err)
	}
}

func TestRenewalCollectorRejectsInvalidSessionClosure(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	for _, event := range []rados.P12SessionDiagnostic{
		{SessionClosed: true, Service: "osd", ServiceID: 1, SessionID: 1, Generation: 3, Timestamp: now.Add(time.Second)},
		{SessionClosed: true, Service: "monitor", SessionID: 1, Generation: 3, Timestamp: now.Add(time.Second)},
		{SessionClosed: true, Service: "osd", ServiceID: 1, SessionID: 1, Generation: 1, Timestamp: now.Add(time.Second)},
		{SessionClosed: true, Service: "osd", ServiceID: 1, SessionID: 1, Generation: 3, Timestamp: now.Add(-time.Second)},
	} {
		collector := newRenewalCollector()
		if event.Service != "osd" || event.Generation <= 1 || event.Timestamp.Before(now) {
			collector.ObserveP12SessionDiagnostic(rados.P12SessionDiagnostic{RenewalDue: true, Service: "osd", ServiceID: 1, SessionID: 1, Generation: 2, Timestamp: now})
		}
		collector.ObserveP12SessionDiagnostic(event)
		if _, err := collector.completed(); err == nil {
			t.Fatalf("invalid close accepted: %+v", event)
		}
	}
}

func TestRenewalCollectorConcurrentSessions(t *testing.T) {
	collector := newRenewalCollector()
	now := time.Unix(100, 0).UTC()
	var workers sync.WaitGroup
	for index := range 32 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			sessionID := collector.NextSessionID()
			service := "osd"
			serviceID := int32(index)
			if index == 0 {
				service = "monitor"
				serviceID = 0
			}
			collector.ObserveP12SessionDiagnostic(rados.P12SessionDiagnostic{RenewalDue: true, Service: service, ServiceID: serviceID, SessionID: sessionID, Generation: 2, Timestamp: now})
			collector.ObserveP12SessionDiagnostic(rados.P12SessionDiagnostic{Service: service, ServiceID: serviceID, SessionID: sessionID, Generation: 4, Timestamp: now.Add(time.Second)})
		}()
	}
	workers.Wait()
	completed, err := collector.completed()
	if err != nil || len(completed) != 32 {
		t.Fatalf("completed count = %d, error = %v", len(completed), err)
	}
	for index := 1; index < len(completed); index++ {
		if completed[index-1].SessionID >= completed[index].SessionID {
			t.Fatalf("session IDs are not monotonic: %+v", completed)
		}
	}
}
