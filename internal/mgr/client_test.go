package mgr

import (
	"context"
	"errors"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/otuschhoff/rados-go/internal/cephx"
	wire "github.com/otuschhoff/rados-go/internal/encoding"
	"github.com/otuschhoff/rados-go/internal/maps"
	"github.com/otuschhoff/rados-go/internal/msgr"
	"github.com/otuschhoff/rados-go/internal/protocol"
)

var testMgrMapLimits = maps.Limits{MaxBytes: 16 << 10, MaxAddresses: 8, MaxCollectionEntries: 32}

type fakeMgrMapSource struct {
	mu     sync.Mutex
	mgrMap *maps.MgrMap
}

func (source *fakeMgrMapSource) MgrMap() *maps.MgrMap {
	source.mu.Lock()
	defer source.mu.Unlock()
	return source.mgrMap
}

func (source *fakeMgrMapSource) set(mgrMap *maps.MgrMap) {
	source.mu.Lock()
	source.mgrMap = mgrMap
	source.mu.Unlock()
}

type fakeMgrSession struct {
	mu       sync.Mutex
	submit   func(context.Context, msgr.Message) (msgr.Message, error)
	stopped  chan struct{}
	stopOnce sync.Once
	stops    int
}

func (session *fakeMgrSession) Submit(ctx context.Context, message msgr.Message) (msgr.Message, error) {
	return session.submit(ctx, message)
}

func (session *fakeMgrSession) Stop() {
	session.mu.Lock()
	session.stops++
	session.mu.Unlock()
	if session.stopped != nil {
		session.stopOnce.Do(func() { close(session.stopped) })
	}
}

func (session *fakeMgrSession) stopCount() int {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.stops
}

func TestCommandWaitsForManagerThenUsesAvailableSession(t *testing.T) {
	source := &fakeMgrMapSource{}
	created := 0
	client := newTestManagerClient(t, source, func(_ context.Context, target ActiveTarget) (session, error) {
		created++
		if target.Name != "active-a" {
			t.Fatalf("target=%+v", target)
		}
		return &fakeMgrSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
			return mgrCommandReplyMessage(t, message.Header.TransactionID, 0, "ok", []byte("ready")), nil
		}}, nil
	})
	defer client.Close()

	go func() {
		time.Sleep(12 * time.Millisecond)
		source.set(testMgrMap(t, mgrMapFixture{name: "active-a", gid: 7, endpoint: "192.0.2.50:7000", activeFeatures: uint64(protocol.FeatureServerOctopusMask)}))
	}()

	result, err := client.Command(context.Background(), []string{"status"}, []byte("payload"))
	if err != nil {
		t.Fatal(err)
	}
	if created != 1 || result.Result != 0 || result.Status != "ok" || string(result.Data) != "ready" {
		t.Fatalf("created=%d result=%+v", created, result)
	}
}

func TestCommandCachesSessionAndUsesMonotonicTIDs(t *testing.T) {
	source := &fakeMgrMapSource{mgrMap: testMgrMap(t, mgrMapFixture{name: "active-a", gid: 7, endpoint: "192.0.2.50:7000", activeFeatures: uint64(protocol.FeatureServerOctopusMask)})}
	created := 0
	var tids []uint64
	client := newTestManagerClient(t, source, func(_ context.Context, _ ActiveTarget) (session, error) {
		created++
		return &fakeMgrSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
			tids = append(tids, message.Header.TransactionID)
			return mgrCommandReplyMessage(t, message.Header.TransactionID, 0, "ok", []byte("done")), nil
		}}, nil
	})
	defer client.Close()

	if _, err := client.Command(context.Background(), []string{"one"}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Command(context.Background(), []string{"two"}, nil); err != nil {
		t.Fatal(err)
	}
	if created != 1 || len(tids) != 2 || tids[0] == 0 || tids[1] != tids[0]+1 {
		t.Fatalf("created=%d tids=%v", created, tids)
	}
}

func TestCommandFollowsActiveFailoverWhilePending(t *testing.T) {
	firstMap := testMgrMap(t, mgrMapFixture{name: "active-a", gid: 7, endpoint: "192.0.2.50:7000", activeFeatures: uint64(protocol.FeatureServerOctopusMask)})
	secondMap := testMgrMap(t, mgrMapFixture{epoch: 43, name: "active-b", gid: 8, endpoint: "192.0.2.51:7001", activeFeatures: uint64(protocol.FeatureServerOctopusMask)})
	source := &fakeMgrMapSource{mgrMap: firstMap}
	entered := make(chan struct{}, 1)
	var first *fakeMgrSession
	created := 0
	client := newTestManagerClient(t, source, func(_ context.Context, target ActiveTarget) (session, error) {
		created++
		if target.Name == "active-a" {
			first = &fakeMgrSession{stopped: make(chan struct{})}
			first.submit = func(ctx context.Context, _ msgr.Message) (msgr.Message, error) {
				select {
				case entered <- struct{}{}:
				default:
				}
				select {
				case <-ctx.Done():
					return msgr.Message{}, ctx.Err()
				case <-first.stopped:
					return msgr.Message{}, errors.New("stopped")
				}
			}
			return first, nil
		}
		return &fakeMgrSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
			return mgrCommandReplyMessage(t, message.Header.TransactionID, 0, "ok", []byte("after-failover")), nil
		}}, nil
	})
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resultCh := make(chan struct {
		result CommandReply
		err    error
	}, 1)
	go func() {
		result, err := client.Command(ctx, []string{"status"}, nil)
		resultCh <- struct {
			result CommandReply
			err    error
		}{result: result, err: err}
	}()

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("submit did not start")
	}
	source.set(secondMap)
	outcome := <-resultCh
	if outcome.err != nil {
		t.Fatal(outcome.err)
	}
	if created != 2 || first == nil || first.stopCount() == 0 || string(outcome.result.Data) != "after-failover" {
		t.Fatalf("created=%d stopped=%d result=%+v", created, first.stopCount(), outcome.result)
	}
}

func TestCommandDoesNotRetryUnknownOutcomeAfterFailover(t *testing.T) {
	firstMap := testMgrMap(t, mgrMapFixture{name: "active-a", gid: 7, endpoint: "192.0.2.50:7000", activeFeatures: uint64(protocol.FeatureServerOctopusMask)})
	secondMap := testMgrMap(t, mgrMapFixture{epoch: 43, name: "active-b", gid: 8, endpoint: "192.0.2.51:7001", activeFeatures: uint64(protocol.FeatureServerOctopusMask)})
	source := &fakeMgrMapSource{mgrMap: firstMap}
	entered := make(chan struct{}, 1)
	created := 0
	client := newTestManagerClient(t, source, func(_ context.Context, target ActiveTarget) (session, error) {
		created++
		if target.Name != "active-a" {
			t.Fatal("unknown outcome retried on replacement manager")
		}
		return &fakeMgrSession{submit: func(ctx context.Context, _ msgr.Message) (msgr.Message, error) {
			entered <- struct{}{}
			<-ctx.Done()
			return msgr.Message{}, errors.Join(msgr.ErrOutcomeUnknown, ctx.Err())
		}}, nil
	})
	defer client.Close()

	result := make(chan error, 1)
	go func() {
		_, err := client.Command(context.Background(), []string{"mutating-command"}, nil)
		result <- err
	}()
	<-entered
	source.set(secondMap)
	if err := <-result; !errors.Is(err, msgr.ErrOutcomeUnknown) {
		t.Fatalf("err=%v", err)
	}
	if created != 1 {
		t.Fatalf("created=%d", created)
	}
}

func TestCommandPreservesUnknownOutcomeAfterCancellation(t *testing.T) {
	source := &fakeMgrMapSource{mgrMap: testMgrMap(t, mgrMapFixture{name: "active-a", gid: 7, endpoint: "192.0.2.50:7000", activeFeatures: uint64(protocol.FeatureServerOctopusMask)})}
	attempts := 0
	entered := make(chan struct{})
	client := newTestManagerClient(t, source, func(_ context.Context, _ ActiveTarget) (session, error) {
		return &fakeMgrSession{submit: func(ctx context.Context, _ msgr.Message) (msgr.Message, error) {
			attempts++
			close(entered)
			<-ctx.Done()
			return msgr.Message{}, errors.Join(msgr.ErrOutcomeUnknown, ctx.Err())
		}}, nil
	})
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := client.Command(ctx, []string{"mutating-command"}, nil)
		result <- err
	}()
	<-entered
	cancel()
	if err := <-result; !errors.Is(err, msgr.ErrOutcomeUnknown) {
		t.Fatalf("err=%v", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts=%d", attempts)
	}
}

func TestCommandPreservesErrnoStatusAndOutput(t *testing.T) {
	source := &fakeMgrMapSource{mgrMap: testMgrMap(t, mgrMapFixture{name: "active-a", gid: 7, endpoint: "192.0.2.50:7000", activeFeatures: uint64(protocol.FeatureServerOctopusMask)})}
	client := newTestManagerClient(t, source, func(_ context.Context, _ ActiveTarget) (session, error) {
		return &fakeMgrSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
			return mgrCommandReplyMessage(t, message.Header.TransactionID, -13, "operation not permitted", []byte("audit-log")), nil
		}}, nil
	})
	defer client.Close()

	result, err := client.Command(context.Background(), []string{"status"}, nil)
	if !errors.Is(err, protocol.WireErrno(-13)) {
		t.Fatalf("err=%v", err)
	}
	if result.Result != -13 || result.Status != "operation not permitted" || string(result.Data) != "audit-log" {
		t.Fatalf("result=%+v", result)
	}
}

func TestCommandRejectsMalformedReplyAndTIDMismatch(t *testing.T) {
	for _, test := range []struct {
		name  string
		reply func(*testing.T, uint64) msgr.Message
		want  error
	}{
		{name: "tid mismatch", reply: func(t *testing.T, tid uint64) msgr.Message { return mgrCommandReplyMessage(t, tid+1, 0, "ok", nil) }, want: wire.ErrMalformed},
		{name: "malformed payload", reply: func(t *testing.T, tid uint64) msgr.Message {
			message := mgrCommandReplyMessage(t, tid, 0, "ok", []byte("x"))
			message.Lengths.Data++
			return message
		}, want: wire.ErrMalformed},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := &fakeMgrMapSource{mgrMap: testMgrMap(t, mgrMapFixture{name: "active-a", gid: 7, endpoint: "192.0.2.50:7000", activeFeatures: uint64(protocol.FeatureServerOctopusMask)})}
			var seen uint64
			client := newTestManagerClient(t, source, func(_ context.Context, _ ActiveTarget) (session, error) {
				return &fakeMgrSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
					seen = message.Header.TransactionID
					return test.reply(t, seen), nil
				}}, nil
			})
			defer client.Close()

			if _, err := client.Command(context.Background(), []string{"status"}, nil); !errors.Is(err, test.want) {
				t.Fatalf("err=%v want=%v", err, test.want)
			}
		})
	}
}

func TestCommandCopiesInputBeforeSubmission(t *testing.T) {
	source := &fakeMgrMapSource{mgrMap: testMgrMap(t, mgrMapFixture{name: "active-a", gid: 7, endpoint: "192.0.2.50:7000", activeFeatures: uint64(protocol.FeatureServerOctopusMask)})}
	input := []byte("payload")
	client := newTestManagerClient(t, source, func(_ context.Context, _ ActiveTarget) (session, error) {
		return &fakeMgrSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
			message.Data[0] = 'X'
			return mgrCommandReplyMessage(t, message.Header.TransactionID, 0, "ok", nil), nil
		}}, nil
	})
	defer client.Close()

	if _, err := client.Command(context.Background(), []string{"status"}, input); err != nil {
		t.Fatal(err)
	}
	if string(input) != "payload" {
		t.Fatalf("input mutated: %q", input)
	}
}

func TestCommandRespectsContextCancellationWhileWaitingForManager(t *testing.T) {
	client := newTestManagerClient(t, &fakeMgrMapSource{}, func(context.Context, ActiveTarget) (session, error) {
		t.Fatal("session must not be created")
		return nil, nil
	})
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := client.Command(ctx, []string{"status"}, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v", err)
	}
}

func TestCommandWaitsWhenManagerMapIsExplicitlyUnavailable(t *testing.T) {
	source := &fakeMgrMapSource{mgrMap: testMgrMap(t, mgrMapFixture{unavailable: true})}
	client := newTestManagerClient(t, source, func(context.Context, ActiveTarget) (session, error) {
		t.Fatal("session must not be created")
		return nil, nil
	})
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := client.Command(ctx, []string{"status"}, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v", err)
	}
}

func TestCloseStopsSessionAndRejectsWork(t *testing.T) {
	source := &fakeMgrMapSource{mgrMap: testMgrMap(t, mgrMapFixture{name: "active-a", gid: 7, endpoint: "192.0.2.50:7000", activeFeatures: uint64(protocol.FeatureServerOctopusMask)})}
	active := &fakeMgrSession{submit: func(_ context.Context, message msgr.Message) (msgr.Message, error) {
		return mgrCommandReplyMessage(t, message.Header.TransactionID, 0, "ok", nil), nil
	}}
	client := newTestManagerClient(t, source, func(_ context.Context, _ ActiveTarget) (session, error) {
		return active, nil
	})
	if _, err := client.Command(context.Background(), []string{"status"}, nil); err != nil {
		t.Fatal(err)
	}
	client.Close()
	client.Close()
	if active.stopCount() != 1 {
		t.Fatalf("stop count=%d", active.stopCount())
	}
	if _, err := client.Command(context.Background(), []string{"status"}, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("err=%v", err)
	}
}

func TestCommandRejectsUnsupportedManagerFeatures(t *testing.T) {
	source := &fakeMgrMapSource{mgrMap: testMgrMap(t, mgrMapFixture{name: "active-a", gid: 7, endpoint: "192.0.2.50:7000", activeFeatures: uint64(protocol.FeatureServerNautilusMask)})}
	client := newTestManagerClient(t, source, func(context.Context, ActiveTarget) (session, error) {
		t.Fatal("session must not be created")
		return nil, nil
	})
	defer client.Close()

	if _, err := client.Command(context.Background(), []string{"status"}, nil); !errors.Is(err, ErrUnsupportedManagerFeatures) {
		t.Fatalf("err=%v", err)
	}
}

func TestCommandRejectsEmptyArgs(t *testing.T) {
	client := newTestManagerClient(t, &fakeMgrMapSource{}, func(context.Context, ActiveTarget) (session, error) {
		t.Fatal("session must not be created")
		return nil, nil
	})
	defer client.Close()
	if _, err := client.Command(context.Background(), nil, nil); !errors.Is(err, wire.ErrMalformed) {
		t.Fatalf("err=%v", err)
	}
}

func newTestManagerClient(t *testing.T, source *fakeMgrMapSource, factory SessionFactory) *Client {
	t.Helper()
	address := testMgrClientAddress(t, "192.0.2.99:7000")
	client, err := New(Config{
		Maps:            source,
		FSID:            maps.FSID{0, 1, 2, 3},
		AuthoritySource: func() *cephx.Connector { return nil },
		ClientAddresses: protocol.EntityAddrVec{address},
		MessageLimits:   16 << 10,
		RetryDelay:      5 * time.Millisecond,
		MaxAttempts:     64,
		SessionFactory:  factory,
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func mgrCommandReplyMessage(t *testing.T, tid uint64, result protocol.WireErrno, status string, data []byte) msgr.Message {
	t.Helper()
	encoder := wire.NewEncoder(1024)
	result.Encode(encoder)
	encoder.String(status)
	front, err := encoder.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	message := frontMessage(protocol.MessageMgrCommandReply, 1, 1, front)
	message.Header.TransactionID = tid
	message.Data = append([]byte(nil), data...)
	message.Lengths.Data = uint32(len(message.Data))
	return message
}

type mgrMapFixture struct {
	epoch          uint32
	name           string
	gid            uint64
	endpoint       string
	activeFeatures uint64
	unavailable    bool
	addressType    protocol.AddressType
}

func testMgrMap(t *testing.T, fixture mgrMapFixture) *maps.MgrMap {
	t.Helper()
	if fixture.epoch == 0 {
		fixture.epoch = 42
	}
	if fixture.name == "" {
		fixture.name = "active"
	}
	if fixture.gid == 0 {
		fixture.gid = 7
	}
	if fixture.endpoint == "" {
		fixture.endpoint = "192.0.2.50:7000"
	}
	if fixture.activeFeatures == 0 {
		fixture.activeFeatures = uint64(protocol.FeatureServerOctopusMask)
	}
	if fixture.addressType == 0 {
		fixture.addressType = protocol.AddressV2
	}
	available := !fixture.unavailable
	active, err := protocol.IPv4EntityAddr(fixture.addressType, 3, netip.MustParseAddrPort(fixture.endpoint))
	if err != nil {
		t.Fatal(err)
	}
	standby, err := protocol.IPv4EntityAddr(protocol.AddressV2, 4, netip.MustParseAddrPort("192.0.2.51:7001"))
	if err != nil {
		t.Fatal(err)
	}
	encoder := wire.NewEncoder(testMgrMapLimits.MaxBytes)
	features := protocol.FeatureMessageAddress2 | protocol.FeatureServerNautilusMask
	encoder.Versioned(14, 6, func(payload *wire.Encoder) {
		payload.Uint32(fixture.epoch)
		if err := (protocol.EntityAddrVec{active}).Encode(payload, features); err != nil {
			t.Fatal(err)
		}
		payload.Uint64(fixture.gid)
		payload.Bool(available)
		payload.String(fixture.name)
		payload.Uint32(1)
		payload.Uint64(9)
		payload.Versioned(4, 1, func(standbyPayload *wire.Encoder) {
			standbyPayload.Uint64(9)
			standbyPayload.String("standby")
			payloadStringSet(standbyPayload, []string{"dashboard"})
			payloadModuleInfos(standbyPayload, true)
			standbyPayload.Uint64(0x20)
		})
		payloadStringSet(payload, []string{"dashboard"})
		payload.Uint32(1)
		payload.String("dashboard")
		payload.String("https://192.0.2.50")
		payloadModuleInfos(payload, false)
		payload.Uint32(123)
		payload.Uint32(456)
		payload.Uint32(1)
		payload.Uint32(18)
		payloadStringSet(payload, []string{"status"})
		payload.Uint64(fixture.activeFeatures)
		payload.Uint32(17)
		payload.Uint32(1)
		if err := (protocol.EntityAddrVec{standby}).Encode(payload, features); err != nil {
			t.Fatal(err)
		}
		payload.Uint32(1)
		payload.String("client.admin")
		payload.Uint64(1)
		payloadStringSet(payload, []string{"crash"})
	})
	data, err := encoder.BytesResult()
	if err != nil {
		t.Fatal(err)
	}
	mgrMap, err := maps.DecodeMgrMap(data, testMgrMapLimits)
	if err != nil {
		t.Fatal(err)
	}
	if mgrMap.Available() != available {
		t.Fatalf("decoded availability=%t want=%t", mgrMap.Available(), available)
	}
	return mgrMap
}

func testMgrClientAddress(t *testing.T, endpoint string) protocol.EntityAddr {
	t.Helper()
	address, err := protocol.IPv4EntityAddr(protocol.AddressV2, 1, netip.MustParseAddrPort(endpoint))
	if err != nil {
		t.Fatal(err)
	}
	return address
}
