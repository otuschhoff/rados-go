package msgr

import (
	"testing"
	"testing/synctest"
	"time"
)

func newRenewalTestOwner(t *testing.T) *sessionOwner {
	t.Helper()
	owner := newUnitSessionOwner(t)
	owner.config.MaxReconnectAttempts = 0
	owner.session.resets = make(chan struct{}, 1)
	owner.connectorRequests = make(chan connectRequest, 1)
	owner.connectorCancel = func() {}
	owner.frames = make(chan pumpFrame, 1)
	owner.writes = make(chan pumpWriteResult)
	owner.renewals = make(chan renewalDue)
	t.Cleanup(func() {
		select {
		case <-owner.session.done:
		default:
			close(owner.session.done)
		}
		owner.pumpWG.Wait()
	})
	return owner
}

func waitRenewalPumps(t *testing.T, owner *sessionOwner) {
	t.Helper()
	finished := make(chan struct{})
	go func() {
		owner.pumpWG.Wait()
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("retired transport pumps did not exit before session shutdown")
	}
}

func TestSessionRenewalWatchersExitOnTransportRetirement(t *testing.T) {
	owner := newRenewalTestOwner(t)
	for replacement := 0; replacement < 64; replacement++ {
		transport := &renewableFakeTransport{fakeTransport: newFakeTransport(), renewal: make(chan struct{})}
		owner.startTransport(transport, StateReady)
		owner.handleFault(ErrSessionDisconnected)
		waitRenewalPumps(t, owner)
		<-owner.frames
		<-owner.connectorRequests
		owner.connectPending = false
		select {
		case <-transport.renewal:
			t.Fatal("retirement signaled a false credential renewal")
		default:
		}
		select {
		case <-owner.renewals:
			t.Fatal("retired watcher delivered a credential renewal")
		default:
		}
	}
}

func TestSessionRenewalPumpCancellation(t *testing.T) {
	for _, awaitingDue := range []bool{true, false} {
		for _, shutdown := range []bool{false, true} {
			name := "forwarding"
			if awaitingDue {
				name = "awaiting-due"
			}
			if shutdown {
				name += "/shutdown"
			} else {
				name += "/retirement"
			}
			t.Run(name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					owner := &sessionOwner{session: &Session{done: make(chan struct{})}, renewals: make(chan renewalDue)}
					due := make(chan struct{})
					transportDone := make(chan struct{})
					if !awaitingDue {
						close(due)
					}
					started := make(chan struct{})
					stopped := make(chan struct{})
					owner.pumpWG.Add(1)
					go func() {
						close(started)
						owner.renewalPump(7, due, transportDone)
						close(stopped)
					}()
					<-started
					synctest.Wait()
					select {
					case <-stopped:
						t.Fatal("renewal pump exited before cancellation")
					default:
					}
					if shutdown {
						close(owner.session.done)
					} else {
						close(transportDone)
					}
					synctest.Wait()
					select {
					case <-stopped:
					default:
						t.Fatal("renewal pump remained blocked after cancellation")
					}
					owner.pumpWG.Wait()
				})
			})
		}
	}
}

func TestSessionRenewalWatchersExitOnTerminalAndStop(t *testing.T) {
	for _, boundary := range []string{"terminal", "stop"} {
		t.Run(boundary, func(t *testing.T) {
			owner := newRenewalTestOwner(t)
			owner.stopTransportRenewal()
			transport := &renewableFakeTransport{fakeTransport: newFakeTransport(), renewal: make(chan struct{})}
			owner.startTransport(transport, StateReady)
			transportDone := owner.transportDone
			if boundary == "terminal" {
				owner.failTerminal(ErrReconnectExhausted)
				owner.failTerminal(ErrReconnectExhausted)
			} else {
				owner.stop(make(chan struct{}))
			}
			owner.stopTransportRenewal()
			waitRenewalPumps(t, owner)
			select {
			case <-transportDone:
			default:
				t.Fatal("transport generation was not canceled")
			}
			if owner.transportDone != nil {
				t.Fatal("owner retained retired cancellation channel")
			}
			select {
			case <-transport.renewal:
				t.Fatal("terminal retirement signaled a false credential renewal")
			default:
			}
		})
	}
}
