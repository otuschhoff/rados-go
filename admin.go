package rados

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"time"

	wire "github.com/otuschhoff/go-librados/internal/encoding"
	"github.com/otuschhoff/go-librados/internal/mon"
	"github.com/otuschhoff/go-librados/internal/osd"
	"github.com/otuschhoff/go-librados/internal/protocol"
)

type CommandResult struct {
	Output []byte
	Status string
}

type ClusterStats struct {
	KB          uint64
	KBUsed      uint64
	KBAvailable uint64
	Objects     uint64
}

type PoolStats struct {
	BytesUsed  uint64
	Objects    uint64
	ReadBytes  uint64
	WriteBytes uint64
}

type InconsistentObject struct {
	Object string
	Shards []int
	Errors []string
}

type InconsistentPG struct {
	PG     string
	Errors []string
}

func (client *Client) ClusterStats(ctx context.Context) (ClusterStats, error) {
	monitorClient, _, done, err := client.beginOperation()
	if err != nil {
		return ClusterStats{}, err
	}
	defer done()
	operationCtx, cancel := client.operationContext(ctx)
	defer cancel()
	stats, err := monitorClient.StatFS(operationCtx)
	result := ClusterStats{KB: stats.KB, KBUsed: stats.KBUsed, KBAvailable: stats.KBAvailable, Objects: stats.Objects}
	return result, client.wrapError("cluster stats", "cluster", err)
}

func (pool Pool) Stats(ctx context.Context) (PoolStats, error) {
	if pool.client == nil || pool.name == "" {
		return PoolStats{}, &OpError{Op: "pool stats", Err: ErrInvalidArgument}
	}
	monitorClient, _, done, err := pool.client.beginOperation()
	if err != nil {
		return PoolStats{}, err
	}
	defer done()
	operationCtx, cancel := pool.client.operationContext(ctx)
	defer cancel()
	reply, err := monitorClient.PoolStats(operationCtx, []string{pool.name})
	stats := reply.Pools[pool.name]
	if err == nil {
		if _, ok := reply.Pools[pool.name]; !ok {
			err = wire.ErrMalformed
		}
	}
	return PoolStats{BytesUsed: stats.BytesUsed, Objects: stats.Objects, ReadBytes: stats.ReadBytes, WriteBytes: stats.WriteBytes}, pool.client.wrapError("pool stats", pool.safeTarget(), err)
}

func (client *Client) ListInconsistentPGs(ctx context.Context, poolID int64) ([]InconsistentPG, error) {
	if poolID < 0 {
		return nil, client.wrapError("list inconsistent PGs", "pool", wire.ErrMalformed)
	}
	payload, err := json.Marshal(map[string]any{"prefix": "pg ls", "pool": poolID, "states": []string{"inconsistent"}, "format": "json"})
	if err != nil {
		return nil, client.wrapError("list inconsistent PGs", fmt.Sprintf("pool %d", poolID), err)
	}
	result, err := client.ManagerCommand(ctx, payload, nil)
	if err != nil {
		return nil, err
	}
	pgs, err := decodeInconsistentPGs(result.Output)
	if err != nil {
		return nil, client.wrapError("list inconsistent PGs", fmt.Sprintf("pool %d", poolID), err)
	}
	return pgs, nil
}

func decodeInconsistentPGs(data []byte) ([]InconsistentPG, error) {
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return []InconsistentPG{}, nil
	}
	if data[0] != '[' && data[0] != '{' {
		return nil, wire.ErrMalformed
	}
	var envelope json.RawMessage
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, wire.ErrMalformed
	}
	entries := envelope
	if len(envelope) > 0 && envelope[0] == '{' {
		var object struct {
			PGStats json.RawMessage `json:"pg_stats"`
		}
		if err := json.Unmarshal(envelope, &object); err != nil {
			return nil, wire.ErrMalformed
		}
		if len(object.PGStats) == 0 || bytes.Equal(object.PGStats, []byte("null")) {
			return []InconsistentPG{}, nil
		}
		entries = object.PGStats
	}
	var raw []struct {
		PG string `json:"pgid"`
	}
	if err := json.Unmarshal(entries, &raw); err != nil {
		return nil, wire.ErrMalformed
	}
	result := make([]InconsistentPG, len(raw))
	for index, item := range raw {
		if item.PG == "" {
			return nil, wire.ErrMalformed
		}
		result[index] = InconsistentPG{PG: item.PG, Errors: []string{}}
	}
	return result, nil
}

func (client *Client) ListInconsistentObjects(ctx context.Context, pg string) ([]InconsistentObject, error) {
	parsed, err := osd.ParsePG(pg)
	if err != nil {
		return nil, client.wrapError("list inconsistent objects", pg, err)
	}
	_, _, objectClient, done, err := client.beginAdministrativeOperation()
	if err != nil {
		return nil, err
	}
	defer done()
	operationCtx, cancel := client.operationContext(ctx)
	defer cancel()
	objects, err := objectClient.ListInconsistentObjects(operationCtx, parsed)
	if err != nil {
		return nil, client.wrapError("list inconsistent objects", pg, err)
	}
	result := make([]InconsistentObject, len(objects))
	for index, object := range objects {
		result[index] = InconsistentObject{Object: object.Object, Shards: append([]int(nil), object.Shards...), Errors: append([]string(nil), object.Errors...)}
	}
	return result, nil
}

func (client *Client) SessionAddresses() []string {
	client.mu.Lock()
	addresses := append(protocol.EntityAddrVec(nil), client.addresses...)
	client.mu.Unlock()
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

func (client *Client) CreatePool(ctx context.Context, name string) error {
	if name == "" {
		return client.wrapError("create pool", "pool", wire.ErrMalformed)
	}
	monitorClient, _, done, err := client.beginOperation()
	if err != nil {
		return err
	}
	defer done()
	operationCtx, cancel := client.operationContext(ctx)
	defer cancel()
	_, err = monitorClient.ApplyPoolOperation(operationCtx, 0, mon.PoolOperationCreate, 0, name)
	return client.wrapError("create pool", name, err)
}

func (client *Client) DeletePool(ctx context.Context, name string) error {
	monitorClient, _, done, err := client.beginOperation()
	if err != nil {
		return err
	}
	defer done()
	pool, ok := monitorClient.PoolByName(name)
	if !ok {
		return client.wrapError("delete pool", name, protocol.WireErrno(-2))
	}
	operationCtx, cancel := client.operationContext(ctx)
	defer cancel()
	_, err = monitorClient.ApplyPoolOperation(operationCtx, pool.ID(), mon.PoolOperationDelete, 0, "delete")
	return client.wrapError("delete pool", name, err)
}

func (pool Pool) EnableApplication(ctx context.Context, name string, force bool) error {
	command := map[string]any{"prefix": "osd pool application enable", "pool": pool.name, "app": name}
	if force {
		command["yes_i_really_mean_it"] = true
	}
	return pool.mutateApplication(ctx, "enable application", command)
}

func (pool Pool) SetApplicationMetadata(ctx context.Context, application, key, value string) error {
	return pool.mutateApplication(ctx, "set application metadata", map[string]any{"prefix": "osd pool application set", "pool": pool.name, "app": application, "key": key, "value": value})
}

func (pool Pool) RemoveApplicationMetadata(ctx context.Context, application, key string) error {
	return pool.mutateApplication(ctx, "remove application metadata", map[string]any{"prefix": "osd pool application rm", "pool": pool.name, "app": application, "key": key})
}

func (pool Pool) ListApplications(ctx context.Context) ([]string, error) {
	metadata, err := pool.applicationMetadata(ctx, "list applications")
	if err != nil {
		return nil, err
	}
	applications := make([]string, 0, len(metadata))
	for application := range metadata {
		applications = append(applications, application)
	}
	sort.Strings(applications)
	return applications, nil
}

func (pool Pool) GetApplicationMetadata(ctx context.Context, application, key string) (string, error) {
	metadata, err := pool.applicationMetadata(ctx, "get application metadata")
	if err != nil {
		return "", err
	}
	values, ok := metadata[application]
	if !ok {
		return "", pool.client.wrapError("get application metadata", pool.safeTarget(), protocol.WireErrno(-2))
	}
	value, ok := values[key]
	if !ok {
		return "", pool.client.wrapError("get application metadata", pool.safeTarget(), protocol.WireErrno(-2))
	}
	return value, nil
}

func (pool Pool) ListApplicationMetadata(ctx context.Context, application string) (map[string]string, error) {
	metadata, err := pool.applicationMetadata(ctx, "list application metadata")
	if err != nil {
		return nil, err
	}
	values, ok := metadata[application]
	if !ok {
		return nil, pool.client.wrapError("list application metadata", pool.safeTarget(), protocol.WireErrno(-2))
	}
	return values, nil
}

func (pool Pool) applicationMetadata(ctx context.Context, operation string) (map[string]map[string]string, error) {
	if pool.client == nil {
		return nil, &OpError{Op: operation, Err: ErrInvalidArgument}
	}
	monitorClient, _, done, err := pool.client.beginOperation()
	if err != nil {
		return nil, err
	}
	defer done()
	operationCtx, cancel := pool.client.operationContext(ctx)
	defer cancel()
	select {
	case <-operationCtx.Done():
		return nil, pool.client.wrapError(operation, pool.safeTarget(), operationCtx.Err())
	default:
	}
	current, ok := monitorClient.OSDMap().PoolByID(pool.id)
	if !ok {
		return nil, pool.client.wrapError(operation, pool.safeTarget(), protocol.WireErrno(-2))
	}
	return current.ApplicationMetadata(), nil
}

func (pool Pool) mutateApplication(ctx context.Context, operation string, command map[string]any) error {
	if pool.client == nil || pool.id < 0 || pool.name == "" {
		return &OpError{Op: operation, Err: ErrInvalidArgument}
	}
	monitorClient, _, done, err := pool.client.beginOperation()
	if err != nil {
		return err
	}
	defer done()
	payload, err := json.Marshal(command)
	if err != nil {
		return pool.client.wrapError(operation, pool.safeTarget(), err)
	}
	operationCtx, cancel := pool.client.operationContext(ctx)
	defer cancel()
	epoch := monitorClient.OSDMap().Epoch()
	if _, err := monitorClient.Command(operationCtx, []string{string(payload)}, nil); err != nil {
		return pool.client.wrapError(operation, pool.safeTarget(), err)
	}
	if err := monitorClient.RefreshOSDMap(operationCtx, epoch); err != nil {
		return pool.client.wrapError(operation, pool.safeTarget(), err)
	}
	return nil
}

func (client *Client) Blocklist(ctx context.Context, address string, duration time.Duration) error {
	if _, err := protocol.ParseEntityAddr(address); err != nil || duration < 0 || duration > time.Duration(math.MaxUint32)*time.Second || duration%time.Second != 0 {
		return client.wrapError("blocklist", address, wire.ErrMalformed)
	}
	payload, err := blocklistCommand(address, duration)
	if err != nil {
		return client.wrapError("blocklist", address, err)
	}
	monitorClient, _, done, err := client.beginOperation()
	if err != nil {
		return err
	}
	defer done()
	operationCtx, cancel := client.operationContext(ctx)
	defer cancel()
	epoch := monitorClient.OSDMap().Epoch()
	if _, err := monitorClient.Command(operationCtx, []string{string(payload)}, nil); err != nil {
		return client.wrapError("blocklist", address, err)
	}
	return client.wrapError("blocklist", address, monitorClient.RefreshOSDMap(operationCtx, epoch))
}

func blocklistCommand(address string, duration time.Duration) ([]byte, error) {
	command := map[string]any{"prefix": "osd blocklist", "blocklistop": "add", "addr": address}
	if duration > 0 {
		command["expire"] = json.Number(fmt.Sprintf("%d.0", duration/time.Second))
	}
	return json.Marshal(command)
}

func (client *Client) MonitorCommand(ctx context.Context, command []byte, input []byte) (CommandResult, error) {
	argv, err := commandArgv(command)
	if err != nil {
		return CommandResult{}, client.wrapError("monitor command", "monitors", err)
	}
	monitorClient, _, _, done, err := client.beginAdministrativeOperation()
	if err != nil {
		return CommandResult{}, err
	}
	defer done()
	operationCtx, cancel := client.operationContext(ctx)
	defer cancel()
	reply, commandErr := monitorClient.Command(operationCtx, argv, input)
	result := CommandResult{Output: reply.Data, Status: reply.Status}
	return result, client.wrapError("monitor command", "monitors", commandErr)
}

func (client *Client) ManagerCommand(ctx context.Context, command []byte, input []byte) (CommandResult, error) {
	argv, err := commandArgv(command)
	if err != nil {
		return CommandResult{}, client.wrapError("manager command", "manager", err)
	}
	_, managerClient, _, done, err := client.beginAdministrativeOperation()
	if err != nil {
		return CommandResult{}, err
	}
	defer done()
	operationCtx, cancel := client.operationContext(ctx)
	defer cancel()
	reply, commandErr := managerClient.Command(operationCtx, argv, input)
	result := CommandResult{Output: reply.Data, Status: reply.Status}
	return result, client.wrapError("manager command", "manager", commandErr)
}

func (client *Client) OSDCommand(ctx context.Context, id int, command []byte, input []byte) (CommandResult, error) {
	argv, err := commandArgv(command)
	if err != nil || id < 0 || uint64(id) > math.MaxInt32 {
		if err == nil {
			err = wire.ErrMalformed
		}
		return CommandResult{}, client.wrapError("OSD command", "OSD", err)
	}
	_, _, objectClient, done, err := client.beginAdministrativeOperation()
	if err != nil {
		return CommandResult{}, err
	}
	defer done()
	operationCtx, cancel := client.operationContext(ctx)
	defer cancel()
	reply, commandErr := objectClient.OSDCommand(operationCtx, int32(id), argv, input)
	result := CommandResult{Output: reply.Output, Status: reply.Status}
	return result, client.wrapError("OSD command", "OSD", commandErr)
}

func (client *Client) PGCommand(ctx context.Context, pg string, command []byte, input []byte) (CommandResult, error) {
	argv, err := commandArgv(command)
	if err != nil {
		return CommandResult{}, client.wrapError("PG command", pg, err)
	}
	_, _, objectClient, done, err := client.beginAdministrativeOperation()
	if err != nil {
		return CommandResult{}, err
	}
	defer done()
	operationCtx, cancel := client.operationContext(ctx)
	defer cancel()
	reply, commandErr := objectClient.PGCommandString(operationCtx, pg, argv, input)
	result := CommandResult{Output: reply.Output, Status: reply.Status}
	return result, client.wrapError("PG command", pg, commandErr)
}

func commandArgv(command []byte) ([]string, error) {
	trimmed := bytes.TrimSpace(command)
	if len(trimmed) == 0 || trimmed[0] != '{' || !json.Valid(trimmed) {
		return nil, wire.ErrMalformed
	}
	return []string{string(append([]byte(nil), trimmed...))}, nil
}
