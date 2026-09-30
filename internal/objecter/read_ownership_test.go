package objecter

import (
	"context"
	"testing"

	"github.com/otuschhoff/rados-go/internal/msgr"
	"github.com/otuschhoff/rados-go/internal/osd"
	"github.com/otuschhoff/rados-go/internal/protocol"
)

func TestReadIsolatesRetainedSessionReply(t *testing.T) {
	reply := testReply(t, 10, 42, 0, []byte("data"))
	client := newTestClient(t, &fakeMapSource{}, &fakeRouter{route: testRoute(t, 10, 0, "192.0.2.10:6800")}, func(int32, protocol.EntityAddrVec) (session, error) {
		return &fakeSession{submit: func(context.Context, msgr.Message) (msgr.Message, error) { return reply, nil }}, nil
	})
	defer client.Close()
	target := Target{PoolID: 7, Object: "object", Snapshot: osd.NoSnap}
	first, err := client.Read(context.Background(), target, 0, 4)
	if err != nil {
		t.Fatal(err)
	}
	second, err := client.Read(context.Background(), target, 0, 4)
	if err != nil {
		t.Fatal(err)
	}
	reply.Data[0] = 'X'
	second.Data[1] = 'Y'
	if string(first.Data) != "data" || string(reply.Data) != "Xata" {
		t.Fatalf("first=%q retained=%q", first.Data, reply.Data)
	}
}
