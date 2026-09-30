package rados

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestClusterSubscriptionFiltersAndOrdersEvents(t *testing.T) {
	client := &Client{clusterChanges: newClusterChangeBroker()}
	subscription, err := client.SubscribeClusterChanges(context.Background(), ClusterSubscriptionOptions{OSDs: true, Queue: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()

	client.clusterChanges.publish(ClusterChange{Component: ClusterComponentMON, Kind: ClusterChangeAdded})
	client.clusterChanges.publish(ClusterChange{Component: ClusterComponentOSD, Kind: ClusterChangeDown, OSD: &OSDState{ID: 2}})
	client.clusterChanges.publish(ClusterChange{Component: ClusterComponentOSD, Kind: ClusterChangeUp, OSD: &OSDState{ID: 2}})

	first, second := <-subscription.Events(), <-subscription.Events()
	if first.Component != ClusterComponentOSD || first.Kind != ClusterChangeDown || first.Sequence == 0 || second.Kind != ClusterChangeUp || second.Sequence <= first.Sequence || first.ObservedAt.IsZero() {
		t.Fatalf("events = %+v, %+v", first, second)
	}
}

func TestClusterSubscriptionOverflowIsObservable(t *testing.T) {
	client := &Client{clusterChanges: newClusterChangeBroker()}
	subscription, err := client.SubscribeClusterChanges(context.Background(), ClusterSubscriptionOptions{MONs: true, Queue: 1})
	if err != nil {
		t.Fatal(err)
	}
	client.clusterChanges.publish(ClusterChange{Component: ClusterComponentMON, Kind: ClusterChangeAdded})
	client.clusterChanges.publish(ClusterChange{Component: ClusterComponentMON, Kind: ClusterChangeRemoved})
	if err := <-subscription.Errors(); !errors.Is(err, ErrSubscriptionOverflow) {
		t.Fatalf("overflow error = %v", err)
	}
	select {
	case <-subscription.Done():
	case <-time.After(time.Second):
		t.Fatal("overflow did not close subscription")
	}
}

func TestClusterSubscriptionEventsAreIsolated(t *testing.T) {
	client := &Client{clusterChanges: newClusterChangeBroker()}
	first, err := client.SubscribeClusterChanges(context.Background(), ClusterSubscriptionOptions{MONs: true, Queue: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := client.SubscribeClusterChanges(context.Background(), ClusterSubscriptionOptions{MONs: true, Queue: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()

	client.clusterChanges.publish(ClusterChange{Component: ClusterComponentMON, Kind: ClusterChangeChanged, MON: &MONState{Addresses: []string{"v2:127.0.0.1:3300/0"}, Location: map[string]string{"host": "a"}}})
	firstEvent := <-first.Events()
	firstEvent.MON.Addresses[0] = "changed"
	firstEvent.MON.Location["host"] = "changed"
	secondEvent := <-second.Events()
	if secondEvent.MON.Addresses[0] != "v2:127.0.0.1:3300/0" || secondEvent.MON.Location["host"] != "a" {
		t.Fatalf("subscriber mutation leaked into another event: %+v", secondEvent.MON)
	}
}

func TestClusterChangeClonesPreserveEmptyJSONCollections(t *testing.T) {
	osdJSON, err := json.Marshal(cloneOSDState(&OSDState{Addresses: []string{}}))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(osdJSON), `"addresses":[]`) {
		t.Fatalf("OSD state JSON = %s", osdJSON)
	}

	monJSON, err := json.Marshal(cloneMONState(&MONState{Addresses: []string{}, Location: map[string]string{}}))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(monJSON), `"addresses":[]`) || !strings.Contains(string(monJSON), `"location":{}`) {
		t.Fatalf("MON state JSON = %s", monJSON)
	}
}

func TestOSDStateChangesIgnoreNonexistentInitialSlots(t *testing.T) {
	if changes := osdStateChanges(OSDState{}, false, OSDState{ID: 7}, true, 12); len(changes) != 0 {
		t.Fatalf("nonexistent initial slot changes = %+v", changes)
	}
	changes := osdStateChanges(OSDState{}, false, OSDState{ID: 7, Exists: true, Up: true, In: true}, true, 12)
	if len(changes) != 1 || changes[0].Kind != ClusterChangeAdded || changes[0].OSD == nil || changes[0].OSD.ID != 7 || changes[0].Epoch != 12 {
		t.Fatalf("initial existing slot changes = %+v", changes)
	}
}

func TestClusterSubscriptionContextAndClientClose(t *testing.T) {
	client := &Client{clusterChanges: newClusterChangeBroker()}
	ctx, cancel := context.WithCancel(context.Background())
	contextSubscription, err := client.SubscribeClusterChanges(ctx, ClusterSubscriptionOptions{OSDs: true, Queue: 1})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-contextSubscription.Done():
	case <-time.After(time.Second):
		t.Fatal("context cancellation did not close subscription")
	}

	closeSubscription, err := client.SubscribeClusterChanges(context.Background(), ClusterSubscriptionOptions{MONs: true, Queue: 1})
	if err != nil {
		t.Fatal(err)
	}
	client.clusterChanges.close()
	if err := <-closeSubscription.Errors(); !errors.Is(err, ErrClosed) {
		t.Fatalf("close error = %v", err)
	}
	if _, err := client.SubscribeClusterChanges(context.Background(), ClusterSubscriptionOptions{MONs: true, Queue: 1}); !errors.Is(err, ErrClosed) {
		t.Fatalf("subscribe after close = %v", err)
	}
}

func TestClusterSubscriptionWithCanceledContextCloses(t *testing.T) {
	client := &Client{clusterChanges: newClusterChangeBroker()}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	subscription, err := client.SubscribeClusterChanges(ctx, ClusterSubscriptionOptions{OSDs: true, Queue: 1})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-subscription.Done():
	case <-time.After(time.Second):
		t.Fatal("already-canceled context did not close subscription")
	}
}

func TestClientTerminationClosesClusterSubscriptions(t *testing.T) {
	for _, terminate := range []struct {
		name string
		run  func(*Client) error
	}{
		{name: "close", run: func(client *Client) error { return client.Close() }},
		{name: "shutdown", run: func(client *Client) error { return client.Shutdown(context.Background()) }},
	} {
		t.Run(terminate.name, func(t *testing.T) {
			client := &Client{clusterChanges: newClusterChangeBroker()}
			subscription, err := client.SubscribeClusterChanges(context.Background(), ClusterSubscriptionOptions{MONs: true, Queue: 1})
			if err != nil {
				t.Fatal(err)
			}
			if err := terminate.run(client); err != nil {
				t.Fatal(err)
			}
			if err := <-subscription.Errors(); !errors.Is(err, ErrClosed) {
				t.Fatalf("subscription close error = %v", err)
			}
			select {
			case <-subscription.Done():
			case <-time.After(time.Second):
				t.Fatal("client termination did not close subscription")
			}
		})
	}
}

func TestSubscribeClusterChangesRejectsInvalidOptions(t *testing.T) {
	client := &Client{clusterChanges: newClusterChangeBroker()}
	for _, options := range []ClusterSubscriptionOptions{{Queue: 1}, {OSDs: true}, {MONs: true, Queue: MaxWatchQueue + 1}} {
		if _, err := client.SubscribeClusterChanges(context.Background(), options); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("options=%+v error=%v", options, err)
		}
	}
	for _, client := range []*Client{nil, {}} {
		if _, err := client.SubscribeClusterChanges(context.Background(), ClusterSubscriptionOptions{OSDs: true, Queue: 1}); !errors.Is(err, ErrClosed) {
			t.Fatalf("client=%p error=%v", client, err)
		}
	}
}
