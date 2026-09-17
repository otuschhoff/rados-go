package rados

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	wire "github.com/otuschhoff/rados-go/internal/encoding"
)

func TestCommandArgvValidatesAndCopiesJSON(t *testing.T) {
	command := []byte("  {\"prefix\":\"status\"}  ")
	argv, err := commandArgv(command)
	if err != nil {
		t.Fatal(err)
	}
	command[2] = 'X'
	if len(argv) != 1 || argv[0] != `{"prefix":"status"}` {
		t.Fatalf("argv=%q", argv)
	}
}

func TestCommandArgvRejectsEmptyAndMalformedJSON(t *testing.T) {
	for _, command := range [][]byte{nil, []byte("  "), []byte("{"), []byte("status"), []byte("null"), []byte(`[]`), []byte(`"status"`)} {
		if _, err := commandArgv(command); !errors.Is(err, wire.ErrMalformed) {
			t.Fatalf("command=%q err=%v", command, err)
		}
	}
}

func TestCommandsRejectArgumentsBeforeConnection(t *testing.T) {
	client := &Client{}
	valid := []byte(`{"prefix":"status"}`)
	if _, err := client.MonitorCommand(context.Background(), nil, nil); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("monitor malformed err=%v", err)
	}
	if _, err := client.ManagerCommand(context.Background(), valid, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("manager closed err=%v", err)
	}
	if _, err := client.OSDCommand(context.Background(), -1, valid, nil); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("OSD id err=%v", err)
	}
	if _, err := client.PGCommand(context.Background(), "1.0", valid, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("PG closed err=%v", err)
	}
}

func TestBlocklistRejectsMalformedArgumentsBeforeConnection(t *testing.T) {
	client := &Client{}
	for _, test := range []struct {
		address  string
		duration time.Duration
	}{
		{address: "not-an-address", duration: time.Second},
		{address: "v2:192.0.2.1:6800/1", duration: -time.Second},
		{address: "v2:192.0.2.1:6800/1", duration: time.Millisecond},
		{address: "v2:192.0.2.1:6800/1", duration: (time.Duration(math.MaxUint32) + 1) * time.Second},
	} {
		if err := client.Blocklist(context.Background(), test.address, test.duration); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("Blocklist(%q, %s) error=%v", test.address, test.duration, err)
		}
	}
}

func TestBlocklistCommandUsesCephFloatExpiration(t *testing.T) {
	payload, err := blocklistCommand("v2:192.0.2.1:6800/1", 60*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != `{"addr":"v2:192.0.2.1:6800/1","blocklistop":"add","expire":60.0,"prefix":"osd blocklist"}` {
		t.Fatalf("payload=%s", payload)
	}
}

func TestDecodeInconsistentPGsSupportsNativeResponseShapes(t *testing.T) {
	for _, data := range [][]byte{
		[]byte(`[{"pgid":"7.a"},{"pgid":"7.b"}]`),
		[]byte(`{"pg_stats":[{"pgid":"7.a"},{"pgid":"7.b"}]}`),
	} {
		values, err := decodeInconsistentPGs(data)
		if err != nil {
			t.Fatal(err)
		}
		if len(values) != 2 || values[0].PG != "7.a" || values[1].PG != "7.b" || values[0].Errors == nil {
			t.Fatalf("values=%+v", values)
		}
	}
}

func TestDecodeInconsistentPGsAcceptsEmptyObject(t *testing.T) {
	for _, data := range [][]byte{[]byte(`{}`), []byte(`{"pg_stats":null}`)} {
		values, err := decodeInconsistentPGs(data)
		if err != nil || values == nil || len(values) != 0 {
			t.Fatalf("data=%s values=%v err=%v", data, values, err)
		}
	}
}

func TestDecodeInconsistentPGsRejectsMalformedOutput(t *testing.T) {
	for _, data := range [][]byte{[]byte(`[{"pgid":""}]`), []byte(`null`), []byte(`no`)} {
		if _, err := decodeInconsistentPGs(data); !errors.Is(err, wire.ErrMalformed) {
			t.Fatalf("data=%q err=%v", data, err)
		}
	}
}
