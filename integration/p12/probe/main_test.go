//go:build p12diagnostics

package main

import (
	"sync"
	"testing"
	"time"

	rados "github.com/otuschhoff/rados-go"
)

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
