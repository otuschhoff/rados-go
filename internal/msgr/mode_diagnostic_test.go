package msgr

import (
	"context"
	"errors"
	"testing"
	"time"
)

type modeTestTransport struct {
	Transport
	mode uint32
}

func (transport modeTestTransport) NegotiatedMode() uint32 { return transport.mode }

func TestModeDiagnostic(t *testing.T) {
	for _, service := range []string{"monitor", "osd"} {
		for _, mode := range []uint32{0, 1, 2, 99} {
			calls := 0
			observer := ModeObserver(func(actualService string, serviceID int32, actual uint32) {
				calls++
				if actualService != service || serviceID != 7 || actual != mode {
					t.Fatalf("unexpected diagnostic: %s/%d/%d", actualService, serviceID, actual)
				}
			})
			ctx := WithModeObserver(context.Background(), observer)
			observeTransportMode(SessionConfig{DiagnosticService: service, DiagnosticServiceID: 7, ModeObserver: ModeObserverFromContext(ctx)}, modeTestTransport{mode: mode})
			if calls != 1 {
				t.Fatalf("calls = %d", calls)
			}
		}
	}
	if ModeObserverFromContext(context.Background()) != nil {
		t.Fatal("observer enabled by default")
	}
	observeTransportMode(SessionConfig{}, modeTestTransport{mode: 2})
	observeTransportMode(SessionConfig{DiagnosticService: "manager", ModeObserver: func(string, int32, uint32) { t.Fatal("manager should be ignored") }}, modeTestTransport{mode: 2})
	observeTransportMode(SessionConfig{DiagnosticService: "osd", ModeObserver: func(_ string, _ int32, mode uint32) {
		if mode != 0 {
			t.Fatal("missing metadata must be unknown")
		}
	}}, newFakeTransport())
}

func TestModeDiagnosticInitialTransport(t *testing.T) {
	config := testSessionConfig(t)
	config.DiagnosticService = "monitor"
	observed := make(chan uint32, 1)
	config.ModeObserver = func(_ string, _ int32, mode uint32) { observed <- mode }
	session := newTestSession(t, modeTestTransport{Transport: newFakeTransport(), mode: 2}, nil, config)
	defer session.Stop()
	if mode := <-observed; mode != 2 {
		t.Fatalf("mode = %d", mode)
	}
}

func TestModeDiagnosticReconnect(t *testing.T) {
	first := newFakeTransport()
	second := newFakeTransport()
	config := testSessionConfig(t)
	config.ClientCookie = 11
	config.ServerCookie = 22
	config.DiagnosticService = "osd"
	observed := make(chan uint32, 2)
	config.ModeObserver = func(_ string, _ int32, mode uint32) { observed <- mode }
	session := newTestSession(t, modeTestTransport{Transport: first, mode: 2}, oneTransportConnector(modeTestTransport{Transport: second, mode: 1}), config)
	defer session.Stop()
	first.fail(errors.New("reconnect probe"))
	_ = decodeWrittenControl(t, second).(SessionReconnect)
	for _, expected := range []uint32{2, 1} {
		select {
		case mode := <-observed:
			if mode != expected {
				t.Fatalf("mode = %d, want %d", mode, expected)
			}
		case <-time.After(time.Second):
			t.Fatal("missing reconnect evidence")
		}
	}
}
