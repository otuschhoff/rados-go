//go:build p12diagnostics

package rados

import (
	"time"

	"github.com/otuschhoff/rados-go/internal/msgr"
)

type P12SessionDiagnostic struct {
	RenewalDue    bool
	SessionClosed bool
	Service       string
	ServiceID     int32
	SessionID     uint64
	Generation    uint64
	Timestamp     time.Time
}

type P12DiagnosticObserver interface {
	NextSessionID() uint64
	ObserveP12SessionDiagnostic(P12SessionDiagnostic)
}

type P12ScratchDiagnostic struct {
	Hits          uint64 `json:"hits"`
	Misses        uint64 `json:"misses"`
	Bypasses      uint64 `json:"bypasses"`
	RetainedBytes uint64 `json:"shared_receive_retained_bytes"`
}

func (client *Client) ConfigureP12ScratchDiagnostic(slots int) error {
	return client.receiveBudget.ConfigureScratchDiagnostic(slots)
}

func (client *Client) P12ScratchDiagnostic() P12ScratchDiagnostic {
	snapshot := client.receiveBudget.Snapshot()
	return P12ScratchDiagnostic{Hits: snapshot.ScratchHits, Misses: snapshot.ScratchMisses, Bypasses: snapshot.ScratchBypasses, RetainedBytes: snapshot.RetainedBytes}
}

func NewP12DiagnosticClient(config Config, observer P12DiagnosticObserver) (*Client, error) {
	client, err := New(config)
	if err != nil || observer == nil {
		return client, err
	}
	client.diagnosticSessionIDSource = observer.NextSessionID
	client.diagnosticObserver = func(event msgr.SessionDiagnostic) {
		observer.ObserveP12SessionDiagnostic(P12SessionDiagnostic{
			RenewalDue:    event.Kind == msgr.DiagnosticCredentialRenewalDue,
			SessionClosed: event.Kind == msgr.DiagnosticSessionClosed,
			Service:       event.Service,
			ServiceID:     event.ServiceID,
			SessionID:     event.SessionID,
			Generation:    event.Generation,
			Timestamp:     event.Timestamp,
		})
	}
	return client, nil
}
