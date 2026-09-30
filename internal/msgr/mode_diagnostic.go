package msgr

import "context"

type ModeObserver func(service string, serviceID int32, mode uint32)

type modeObserverKey struct{}

func WithModeObserver(ctx context.Context, observer ModeObserver) context.Context {
	return context.WithValue(ctx, modeObserverKey{}, observer)
}

func ModeObserverFromContext(ctx context.Context) ModeObserver {
	if ctx == nil {
		return nil
	}
	observer, _ := ctx.Value(modeObserverKey{}).(ModeObserver)
	return observer
}

func observeTransportMode(config SessionConfig, transport Transport) {
	if config.ModeObserver == nil || (config.DiagnosticService != "monitor" && config.DiagnosticService != "osd") {
		return
	}
	var mode uint32
	if authenticated, ok := transport.(interface{ NegotiatedMode() uint32 }); ok {
		mode = authenticated.NegotiatedMode()
	}
	config.ModeObserver(config.DiagnosticService, config.DiagnosticServiceID, mode)
}
