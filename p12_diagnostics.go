//go:build p12diagnostics

package rados

import (
	"time"

	"github.com/otuschhoff/go-librados/internal/msgr"
)

type P12SessionDiagnostic struct {
	RenewalDue bool
	Service    string
	ServiceID  int32
	SessionID  uint64
	Generation uint64
	Timestamp  time.Time
}

type P12DiagnosticObserver interface {
	NextSessionID() uint64
	ObserveP12SessionDiagnostic(P12SessionDiagnostic)
}

func NewP12DiagnosticClient(config Config, observer P12DiagnosticObserver) (*Client, error) {
	client, err := New(config)
	if err != nil || observer == nil {
		return client, err
	}
	client.diagnosticSessionIDSource = observer.NextSessionID
	client.diagnosticObserver = func(event msgr.SessionDiagnostic) {
		observer.ObserveP12SessionDiagnostic(P12SessionDiagnostic{
			RenewalDue: event.Kind == msgr.DiagnosticCredentialRenewalDue,
			Service:    event.Service,
			ServiceID:  event.ServiceID,
			SessionID:  event.SessionID,
			Generation: event.Generation,
			Timestamp:  event.Timestamp,
		})
	}
	return client, nil
}
