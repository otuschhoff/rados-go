package objecter

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/otuschhoff/rados-go/internal/msgr"
	"github.com/otuschhoff/rados-go/internal/protocol"
)

type creationTestSession struct {
	stop func()
}

func (*creationTestSession) Submit(context.Context, msgr.Message) (msgr.Message, error) {
	return msgr.Message{}, nil
}

func (active *creationTestSession) Stop() {
	if active.stop != nil {
		active.stop()
	}
}

func TestSessionReplacementAllowsUnrelatedCachedAccess(t *testing.T) {
	for _, gateFactory := range []bool{false, true} {
		name := "Stop"
		if gateFactory {
			name = "Factory"
		}
		t.Run(name, func(t *testing.T) {
			entered := make(chan struct{})
			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			oldAddress := protocol.EntityAddrVec{testAddress(t, "192.0.2.10:6800")}
			newAddress := protocol.EntityAddrVec{testAddress(t, "192.0.2.12:6800")}
			otherAddress := protocol.EntityAddrVec{testAddress(t, "192.0.2.11:6800")}
			old := &creationTestSession{}
			other := &creationTestSession{}
			if !gateFactory {
				old.stop = func() { close(entered); <-release }
			}
			client := newTestClient(t, &fakeMapSource{}, &fakeRouter{}, func(id int32, addresses protocol.EntityAddrVec) (session, error) {
				if id == 1 {
					return other, nil
				}
				selected, _ := selectAddress(addresses)
				original, _ := selectAddress(oldAddress)
				if selected == original {
					return old, nil
				}
				if gateFactory {
					close(entered)
					<-release
				}
				return &creationTestSession{}, nil
			})
			defer func() { unblock(); client.Close() }()
			if _, err := client.getSession(0, oldAddress); err != nil {
				t.Fatal(err)
			}
			if _, err := client.getSession(1, otherAddress); err != nil {
				t.Fatal(err)
			}
			replaced := make(chan error, 1)
			go func() { _, err := client.getSession(0, newAddress); replaced <- err }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("replacement did not enter gate")
			}
			accessed := make(chan error, 1)
			go func() { _, err := client.getSession(1, otherAddress); accessed <- err }()
			select {
			case err := <-accessed:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(200 * time.Millisecond):
				t.Error("unrelated cached session blocked by replacement")
			}
			unblock()
			if err := <-replaced; err != nil {
				t.Fatal(err)
			}
		})
	}
}
