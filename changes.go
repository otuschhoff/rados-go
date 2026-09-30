package rados

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/otuschhoff/rados-go/internal/maps"
	"github.com/otuschhoff/rados-go/internal/mon"
	"github.com/otuschhoff/rados-go/internal/objecter"
	"github.com/otuschhoff/rados-go/internal/protocol"
)

var ErrSubscriptionOverflow = errors.New("cluster change subscription queue overflow")

type ClusterComponent uint8

const (
	ClusterComponentOSD ClusterComponent = iota + 1
	ClusterComponentMON
)

type ClusterChangeSource uint8

const (
	ClusterChangeAuthoritative ClusterChangeSource = iota + 1
	ClusterChangeObserved
)

type ClusterChangeKind uint8

const (
	ClusterChangeAdded ClusterChangeKind = iota + 1
	ClusterChangeRemoved
	ClusterChangeChanged
	ClusterChangeUp
	ClusterChangeDown
	ClusterChangeIn
	ClusterChangeOut
	ClusterChangeAvailable
	ClusterChangeUnavailable
)

type OSDState struct {
	ID        int32    `json:"id"`
	Exists    bool     `json:"exists"`
	Up        bool     `json:"up"`
	In        bool     `json:"in"`
	Destroyed bool     `json:"destroyed"`
	Addresses []string `json:"addresses"`
}

type MONState struct {
	Name      string            `json:"name"`
	Rank      int               `json:"rank"`
	Addresses []string          `json:"addresses"`
	Priority  uint16            `json:"priority"`
	Weight    uint16            `json:"weight"`
	Location  map[string]string `json:"location"`
}

type ClusterChange struct {
	Sequence    uint64 // Client-global; component filtering can produce gaps.
	ObservedAt  time.Time
	Component   ClusterComponent
	Source      ClusterChangeSource
	Kind        ClusterChangeKind
	Epoch       uint32
	OSD         *OSDState
	PreviousOSD *OSDState
	MON         *MONState
	PreviousMON *MONState
	Err         error
}

type ClusterSubscriptionOptions struct {
	OSDs  bool
	MONs  bool
	Queue uint32
}

type ClusterSubscription struct {
	broker    *clusterChangeBroker
	id        uint64
	options   ClusterSubscriptionOptions
	events    chan ClusterChange
	errors    chan error
	done      chan struct{}
	stop      func() bool
	closeOnce sync.Once
}

func (subscription *ClusterSubscription) Events() <-chan ClusterChange { return subscription.events }
func (subscription *ClusterSubscription) Errors() <-chan error         { return subscription.errors }
func (subscription *ClusterSubscription) Done() <-chan struct{}        { return subscription.done }

func (subscription *ClusterSubscription) Close() {
	if subscription == nil {
		return
	}
	subscription.closeOnce.Do(func() {
		if subscription.stop != nil {
			subscription.stop()
		}
		subscription.broker.remove(subscription.id, nil)
	})
}

type clusterChangeBroker struct {
	mu            sync.Mutex
	nextID        uint64
	nextSequence  uint64
	closed        bool
	subscriptions map[uint64]*ClusterSubscription
}

func newClusterChangeBroker() *clusterChangeBroker {
	return &clusterChangeBroker{subscriptions: make(map[uint64]*ClusterSubscription)}
}

func (broker *clusterChangeBroker) subscribe(ctx context.Context, options ClusterSubscriptionOptions) (*ClusterSubscription, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if (!options.OSDs && !options.MONs) || options.Queue == 0 || options.Queue > MaxWatchQueue {
		return nil, ErrInvalidArgument
	}
	broker.mu.Lock()
	defer broker.mu.Unlock()
	if broker.closed {
		return nil, ErrClosed
	}
	broker.nextID++
	for broker.nextID == 0 || broker.subscriptions[broker.nextID] != nil {
		broker.nextID++
	}
	subscription := &ClusterSubscription{broker: broker, id: broker.nextID, options: options, events: make(chan ClusterChange, options.Queue), errors: make(chan error, 1), done: make(chan struct{})}
	broker.subscriptions[subscription.id] = subscription
	registered := make(chan struct{})
	subscription.stop = context.AfterFunc(ctx, func() {
		<-registered
		subscription.Close()
	})
	close(registered)
	return subscription, nil
}

func (broker *clusterChangeBroker) publish(change ClusterChange) {
	broker.mu.Lock()
	defer broker.mu.Unlock()
	if broker.closed {
		return
	}
	broker.nextSequence++
	change.Sequence = broker.nextSequence
	change.ObservedAt = time.Now().UTC()
	for id, subscription := range broker.subscriptions {
		if change.Component == ClusterComponentOSD && !subscription.options.OSDs || change.Component == ClusterComponentMON && !subscription.options.MONs {
			continue
		}
		select {
		case subscription.events <- cloneClusterChange(change):
		default:
			broker.removeLocked(id, ErrSubscriptionOverflow)
		}
	}
}

func (broker *clusterChangeBroker) remove(id uint64, err error) {
	broker.mu.Lock()
	defer broker.mu.Unlock()
	broker.removeLocked(id, err)
}

func (broker *clusterChangeBroker) removeLocked(id uint64, err error) {
	subscription := broker.subscriptions[id]
	if subscription == nil {
		return
	}
	delete(broker.subscriptions, id)
	if err != nil {
		subscription.errors <- err
	}
	close(subscription.events)
	close(subscription.errors)
	close(subscription.done)
}

func (broker *clusterChangeBroker) close() {
	broker.mu.Lock()
	defer broker.mu.Unlock()
	if broker.closed {
		return
	}
	broker.closed = true
	for id := range broker.subscriptions {
		broker.removeLocked(id, ErrClosed)
	}
}

func (client *Client) SubscribeClusterChanges(ctx context.Context, options ClusterSubscriptionOptions) (*ClusterSubscription, error) {
	if client == nil || client.clusterChanges == nil {
		return nil, ErrClosed
	}
	return client.clusterChanges.subscribe(ctx, options)
}

func (client *Client) observeMONMap(previous, current *maps.MonMap) {
	previousStates := monStates(previous)
	currentStates := monStates(current)
	names := make([]string, 0, len(previousStates)+len(currentStates))
	seen := make(map[string]bool, len(previousStates)+len(currentStates))
	for name := range previousStates {
		names = append(names, name)
		seen[name] = true
	}
	for name := range currentStates {
		if !seen[name] {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		before, hadBefore := previousStates[name]
		after, hasAfter := currentStates[name]
		change := ClusterChange{Component: ClusterComponentMON, Source: ClusterChangeAuthoritative, Epoch: current.Epoch()}
		switch {
		case !hadBefore:
			change.Kind, change.MON = ClusterChangeAdded, cloneMONState(&after)
		case !hasAfter:
			change.Kind, change.PreviousMON = ClusterChangeRemoved, cloneMONState(&before)
		case !reflect.DeepEqual(before, after):
			change.Kind, change.PreviousMON, change.MON = ClusterChangeChanged, cloneMONState(&before), cloneMONState(&after)
		default:
			continue
		}
		client.clusterChanges.publish(change)
	}
}

func (client *Client) observeOSDMap(previous, current *maps.OSDMap) {
	maximum := current.MaxOSD()
	if previous != nil {
		maximum = max(maximum, previous.MaxOSD())
	}
	for id := int32(0); id < maximum; id++ {
		before, hadBefore := publicOSDState(previous, id)
		after, hasAfter := publicOSDState(current, id)
		for _, change := range osdStateChanges(before, hadBefore, after, hasAfter, current.Epoch()) {
			client.clusterChanges.publish(change)
		}
	}
}

func osdStateChanges(before OSDState, hadBefore bool, after OSDState, hasAfter bool, epoch uint32) []ClusterChange {
	beforeExists := hadBefore && before.Exists
	afterExists := hasAfter && after.Exists
	base := ClusterChange{Component: ClusterComponentOSD, Source: ClusterChangeAuthoritative, Epoch: epoch}
	if !beforeExists && afterExists {
		base.Kind, base.OSD = ClusterChangeAdded, cloneOSDState(&after)
		return []ClusterChange{base}
	}
	if beforeExists && !afterExists {
		base.Kind, base.PreviousOSD = ClusterChangeRemoved, cloneOSDState(&before)
		return []ClusterChange{base}
	}
	if !beforeExists || !afterExists {
		return nil
	}
	var changes []ClusterChange
	for _, transition := range []struct {
		changed bool
		kind    ClusterChangeKind
	}{
		{before.Up != after.Up, map[bool]ClusterChangeKind{true: ClusterChangeUp, false: ClusterChangeDown}[after.Up]},
		{before.In != after.In, map[bool]ClusterChangeKind{true: ClusterChangeIn, false: ClusterChangeOut}[after.In]},
		{before.Destroyed != after.Destroyed || !reflect.DeepEqual(before.Addresses, after.Addresses), ClusterChangeChanged},
	} {
		if transition.changed {
			change := base
			change.Kind, change.PreviousOSD, change.OSD = transition.kind, cloneOSDState(&before), cloneOSDState(&after)
			changes = append(changes, change)
		}
	}
	return changes
}

func (client *Client) observeMONConnection(event mon.ConnectionEvent) {
	state := &MONState{Rank: -1, Addresses: []string{fmt.Sprintf("v2:%s", event.Endpoint.Address)}, Location: map[string]string{}}
	client.mu.Lock()
	monitorClient := client.monitor
	client.mu.Unlock()
	if monitorClient != nil {
		if candidate, ok := monStateForEndpoint(monitorClient.MonMap(), event.Endpoint.Address); ok {
			state = cloneMONState(&candidate)
		}
	}
	kind := ClusterChangeUnavailable
	if event.Available {
		kind = ClusterChangeAvailable
	}
	client.clusterChanges.publish(ClusterChange{Component: ClusterComponentMON, Source: ClusterChangeObserved, Kind: kind, MON: state, Err: event.Err})
}

func monStateForEndpoint(monMap *maps.MonMap, endpoint netip.AddrPort) (MONState, bool) {
	for _, candidate := range monStates(monMap) {
		for _, address := range candidate.Addresses {
			parsed, err := protocol.ParseEntityAddr(address)
			if err != nil {
				continue
			}
			if candidateEndpoint, ok := parsed.AddrPort(); ok && candidateEndpoint == endpoint {
				return candidate, true
			}
		}
	}
	return MONState{}, false
}

func (client *Client) observeOSDConnection(event objecter.OSDSessionEvent) {
	var state *OSDState
	client.mu.Lock()
	monitorClient := client.monitor
	client.mu.Unlock()
	if monitorClient != nil {
		if current, ok := publicOSDState(monitorClient.OSDMap(), event.OSDID); ok {
			state = cloneOSDState(&current)
		}
	}
	if state == nil {
		state = &OSDState{ID: event.OSDID, Addresses: []string{}}
	}
	kind := ClusterChangeUnavailable
	if event.Available {
		kind = ClusterChangeAvailable
	}
	client.clusterChanges.publish(ClusterChange{Component: ClusterComponentOSD, Source: ClusterChangeObserved, Kind: kind, OSD: state, Err: event.Err})
}

func monStates(monMap *maps.MonMap) map[string]MONState {
	result := make(map[string]MONState)
	if monMap == nil {
		return result
	}
	for rank, name := range monMap.Ranks() {
		monitor, ok := monMap.Monitor(name)
		if !ok {
			continue
		}
		result[name] = MONState{Name: name, Rank: rank, Addresses: publicAddresses(monitor.Addresses), Priority: monitor.Priority, Weight: monitor.Weight, Location: monitor.Location}
	}
	return result
}

func publicOSDState(osdMap *maps.OSDMap, id int32) (OSDState, bool) {
	if osdMap == nil {
		return OSDState{}, false
	}
	state, ok := osdMap.OSDState(id)
	if !ok {
		return OSDState{}, false
	}
	addresses, _ := osdMap.OSDClientAddresses(id)
	return OSDState{ID: id, Exists: state.Exists, Up: state.Up, In: state.In, Destroyed: state.Destroyed, Addresses: publicAddresses(addresses)}, true
}

func publicAddresses(addresses protocol.EntityAddrVec) []string {
	result := make([]string, 0, len(addresses))
	for _, address := range addresses {
		endpoint, ok := address.AddrPort()
		if !ok {
			continue
		}
		prefix := "v1"
		if address.Type == protocol.AddressV2 {
			prefix = "v2"
		}
		result = append(result, fmt.Sprintf("%s:%s/%d", prefix, endpoint, address.Nonce))
	}
	return result
}

func cloneOSDState(state *OSDState) *OSDState {
	if state == nil {
		return nil
	}
	copy := *state
	copy.Addresses = append(make([]string, 0, len(state.Addresses)), state.Addresses...)
	return &copy
}

func cloneMONState(state *MONState) *MONState {
	if state == nil {
		return nil
	}
	copy := *state
	copy.Addresses = append(make([]string, 0, len(state.Addresses)), state.Addresses...)
	copy.Location = make(map[string]string, len(state.Location))
	for key, value := range state.Location {
		copy.Location[key] = value
	}
	return &copy
}

func cloneClusterChange(change ClusterChange) ClusterChange {
	change.OSD = cloneOSDState(change.OSD)
	change.PreviousOSD = cloneOSDState(change.PreviousOSD)
	change.MON = cloneMONState(change.MON)
	change.PreviousMON = cloneMONState(change.PreviousMON)
	return change
}
