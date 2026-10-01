package objecter

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/otuschhoff/rados-go/internal/maps"
	"github.com/otuschhoff/rados-go/internal/msgr"
	"github.com/otuschhoff/rados-go/internal/osd"
	"github.com/otuschhoff/rados-go/internal/protocol"
)

func TestReadIntoValidatedShortReply(t *testing.T) {
	reply := testReply(t, 10, 42, 0, []byte("data"))
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: testRoute(t, 10, 0, "192.0.2.10:6800")}, func(int32, protocol.EntityAddrVec) (session, error) {
		return &fakeSession{submit: func(context.Context, msgr.Message) (msgr.Message, error) { return reply, nil }}, nil
	})
	defer client.Close()
	destination := []byte("........")
	result, err := client.ReadInto(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, 0, destination)
	if err != nil || string(destination) != "data...." || len(result.Data) != 4 || result.Version != 42 {
		t.Fatalf("destination=%q result=%+v err=%v", destination, result, err)
	}
	reply.Data[0] = 'X'
	if string(destination) != "data...." {
		t.Fatal("caller buffer aliases reply")
	}
}

type borrowedReadSession struct {
	*fakeSession
	reply    msgr.Message
	releases atomic.Int32
	cancel   context.CancelFunc
	submit   func() msgr.Message
}

func (*borrowedReadSession) OwnsReplyMessages() bool { return true }

func (session *borrowedReadSession) SubmitBorrowedTarget(context.Context, maps.PG, osd.HObject, msgr.Message) (msgr.Message, func(), error) {
	reply := session.reply
	if session.submit != nil {
		reply = session.submit()
	}
	if session.cancel != nil {
		session.cancel()
	}
	return reply, func() {
		session.releases.Add(1)
		clear(reply.Data)
	}, nil
}

func TestReadIntoBorrowedRetryReleasesBeforeNextAttempt(t *testing.T) {
	for _, operationError := range []bool{false, true} {
		borrowed := &borrowedReadSession{fakeSession: &fakeSession{}}
		attempts := 0
		borrowed.submit = func() msgr.Message {
			attempts++
			if attempts == 1 {
				if operationError {
					return testReplyOperations(t, 10, 0, 0, []osd.OperationResult{{Operation: osd.OpRead, Code: -11}})
				}
				return testReply(t, 10, 0, -11, nil)
			}
			if borrowed.releases.Load() != 1 {
				t.Fatal("retry retained the previous borrowed reply")
			}
			return testReply(t, 10, 42, 0, []byte("ok"))
		}
		client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: testRoute(t, 10, 0, "192.0.2.10:6800")}, func(int32, protocol.EntityAddrVec) (session, error) { return borrowed, nil })
		destination := []byte("....")
		result, err := client.ReadInto(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, 0, destination)
		if err != nil || string(destination) != "ok.." || result.Version != 42 || attempts != 2 || borrowed.releases.Load() != 2 {
			t.Fatalf("result=%+v err=%v attempts=%d releases=%d", result, err, attempts, borrowed.releases.Load())
		}
		client.Close()
	}
}

func TestReadIntoEmptyReplyAndInvalidLength(t *testing.T) {
	borrowed := &borrowedReadSession{fakeSession: &fakeSession{}, reply: testReply(t, 10, 42, 0, nil)}
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: testRoute(t, 10, 0, "192.0.2.10:6800")}, func(int32, protocol.EntityAddrVec) (session, error) { return borrowed, nil })
	defer client.Close()
	for _, destination := range [][]byte{nil, []byte("canary")} {
		before := string(destination)
		result, err := client.ReadInto(context.Background(), Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, 0, destination)
		if err != nil || len(result.Data) != 0 || string(destination) != before {
			t.Fatalf("destination=%q result=%+v err=%v", destination, result, err)
		}
	}
	if _, err := client.ReadInto(context.Background(), Target{Object: "object"}, ^uint64(0), []byte("x")); err == nil {
		t.Fatal("overflow accepted")
	}
}

func TestReadIntoBorrowedReleaseOnEveryOutcome(t *testing.T) {
	for _, scenario := range []string{"success", "malformed", "server-error", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			borrowed := &borrowedReadSession{fakeSession: &fakeSession{}, reply: testReply(t, 10, 42, 0, []byte("data"))}
			if scenario == "malformed" {
				borrowed.reply.Front = []byte{0}
			}
			if scenario == "server-error" {
				borrowed.reply = testReply(t, 10, 42, -2, []byte("data"))
			}
			if scenario == "canceled" {
				borrowed.cancel = cancel
			}
			client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: testRoute(t, 10, 0, "192.0.2.10:6800")}, func(int32, protocol.EntityAddrVec) (session, error) { return borrowed, nil })
			defer client.Close()
			destination := []byte("........")
			_, err := client.ReadInto(ctx, Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, 0, destination)
			if borrowed.releases.Load() != 1 {
				t.Fatalf("releases=%d", borrowed.releases.Load())
			}
			if scenario == "success" {
				if err != nil || string(destination) != "data...." {
					t.Fatalf("destination=%q err=%v", destination, err)
				}
			} else if err == nil || string(destination) != "........" {
				t.Fatalf("destination=%q err=%v", destination, err)
			}
		})
	}
}

func TestReadIntoFailureLeavesDestinationUnchanged(t *testing.T) {
	for _, scenario := range []string{"oversize", "malformed", "server-error", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			reply := testReply(t, 10, 42, 0, []byte("data"))
			if scenario == "malformed" {
				reply.Front = []byte{0}
			}
			if scenario == "server-error" {
				reply = testReply(t, 10, 42, -2, []byte("data"))
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: testRoute(t, 10, 0, "192.0.2.10:6800")}, func(int32, protocol.EntityAddrVec) (session, error) {
				return &fakeSession{submit: func(context.Context, msgr.Message) (msgr.Message, error) {
					if scenario == "canceled" {
						cancel()
					}
					return reply, nil
				}}, nil
			})
			defer client.Close()
			destination := []byte("........")
			if scenario == "oversize" {
				destination = destination[:2]
			}
			before := string(destination)
			_, err := client.ReadInto(ctx, Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}, 0, destination)
			if err == nil || string(destination) != before {
				t.Fatalf("destination=%q err=%v", destination, err)
			}
		})
	}
}
