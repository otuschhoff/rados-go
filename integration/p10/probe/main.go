package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"os"
	"strings"
	"time"

	rados "github.com/otuschhoff/go-librados"
)

type report struct {
	NamedCreateListLookup  bool   `json:"named_create_list_lookup"`
	NamedReadSnapshot      bool   `json:"named_read_snapshot"`
	NamedRollback          bool   `json:"named_rollback"`
	NamedRemove            bool   `json:"named_remove"`
	SelfManagedCreate      bool   `json:"self_managed_create"`
	SelfManagedWriteCtx    bool   `json:"self_managed_write_context"`
	SelfManagedRead        bool   `json:"self_managed_read"`
	SelfManagedRollback    bool   `json:"self_managed_rollback"`
	SelfManagedRemove      bool   `json:"self_managed_remove"`
	WriteSame              bool   `json:"write_same"`
	Checksum               bool   `json:"checksum"`
	ChecksumHex            string `json:"checksum_hex"`
	AllocationHint         bool   `json:"allocation_hint"`
	SparseRead             bool   `json:"sparse_read"`
	CopyFrom               bool   `json:"copy_from"`
	CopyFrom2              bool   `json:"copy_from2"`
	ReplicatedCapabilities bool   `json:"replicated_capabilities"`
	ECCapabilities         bool   `json:"ec_capabilities"`
	ECWriteRead            bool   `json:"ec_write_read"`
	ECOverwriteRejected    bool   `json:"ec_overwrite_rejected"`
	ECWriteSameRejected    bool   `json:"ec_write_same_rejected"`
	ECChecksum             bool   `json:"ec_checksum"`
	ECAllocationHint       bool   `json:"ec_allocation_hint"`
	ECSparseRead           bool   `json:"ec_sparse_read"`
	ECCopyFrom             bool   `json:"ec_copy_from"`
	ECOMAPRejected         bool   `json:"ec_omap_rejected"`
	ECAlignmentEvidence    bool   `json:"ec_alignment_evidence"`
	RequiredAlignment      uint64 `json:"required_alignment"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "p10 probe:", err)
		os.Exit(1)
	}
}

func run() error {
	monitors := flag.String("monitors", "", "comma-separated v2 monitor endpoints")
	keyFile := flag.String("key", "", "file containing an encoded CephX key")
	fsid := flag.String("fsid", "", "expected cluster FSID")
	coordinationDir := flag.String("coordination-dir", "", "native interoperability directory")
	flag.Parse()
	key, err := os.ReadFile(*keyFile)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	client, err := rados.New(rados.Config{Monitors: strings.Split(*monitors, ","), Entity: "client.p10", ClusterFSID: *fsid, Key: bytes.TrimSpace(key), OperationTimeout: 20 * time.Second})
	if err != nil {
		return err
	}
	if err := client.Connect(ctx); err != nil {
		return err
	}
	defer client.Close()
	named, err := client.OpenPool(ctx, "p10-named")
	if err != nil {
		return err
	}
	self, err := client.OpenPool(ctx, "p10-self")
	if err != nil {
		return err
	}
	ec, err := client.OpenPool(ctx, "p10-ec")
	if err != nil {
		return err
	}

	result := report{}
	if err := exerciseNamed(ctx, named, &result); err != nil {
		return err
	}
	if err := exerciseSelfManaged(ctx, self, &result); err != nil {
		return err
	}
	if err := exerciseSpecialized(ctx, named, ec, &result); err != nil {
		return err
	}
	if err := verifyNativeSeed(ctx, named, self, *coordinationDir, &result); err != nil {
		return err
	}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		return err
	}
	return nil
}

func exerciseNamed(ctx context.Context, pool rados.Pool, result *report) error {
	object := pool.Object("go-named")
	if _, err := object.WriteFull(ctx, []byte("named-before")); err != nil {
		return fmt.Errorf("named seed: %w", err)
	}
	if err := pool.CreateSnapshot(ctx, "go-snapshot"); err != nil {
		return fmt.Errorf("create named snapshot: %w", err)
	}
	snapshot, err := pool.LookupSnapshot(ctx, "go-snapshot")
	if err != nil || snapshot.ID == 0 || snapshot.Name != "go-snapshot" || snapshot.CreatedAt.IsZero() {
		return fmt.Errorf("lookup named snapshot: %+v: %w", snapshot, err)
	}
	snapshots, err := pool.ListSnapshots(ctx)
	if err != nil || !containsSnapshot(snapshots, snapshot) {
		return fmt.Errorf("list named snapshots: %+v: %w", snapshots, err)
	}
	result.NamedCreateListLookup = true
	if _, err := object.WriteFull(ctx, []byte("named-after")); err != nil {
		return err
	}
	data, _, err := pool.WithReadSnapshot(snapshot.ID).Object("go-named").Read(ctx, 0, 64)
	if err != nil || string(data) != "named-before" {
		return fmt.Errorf("named snapshot read=%q: %w", data, err)
	}
	result.NamedReadSnapshot = true
	if _, err := object.RollbackToSnapshot(ctx, "go-snapshot"); err != nil {
		return fmt.Errorf("named rollback: %w", err)
	}
	if err := expectData(ctx, object, "named-before"); err != nil {
		return err
	}
	result.NamedRollback = true
	if err := pool.RemoveSnapshot(ctx, "go-snapshot"); err != nil {
		return fmt.Errorf("remove named snapshot: %w", err)
	}
	if _, err := pool.LookupSnapshot(ctx, "go-snapshot"); err == nil {
		return errors.New("removed named snapshot remained visible")
	}
	result.NamedRemove = true
	if _, err := object.WriteFull(ctx, []byte("go-final")); err != nil {
		return err
	}
	if err := pool.CreateSnapshot(ctx, "go-native-check"); err != nil {
		return fmt.Errorf("create native handoff snapshot: %w", err)
	}
	if _, err := object.WriteFull(ctx, []byte("go-head")); err != nil {
		return err
	}
	return nil
}

func exerciseSelfManaged(ctx context.Context, pool rados.Pool, result *report) error {
	object := pool.Object("go-self")
	if _, err := object.WriteFull(ctx, []byte("self-before")); err != nil {
		return err
	}
	snapshotID, err := pool.CreateSelfManagedSnapshot(ctx)
	if err != nil || snapshotID == 0 {
		return fmt.Errorf("create self-managed snapshot %d: %w", snapshotID, err)
	}
	result.SelfManagedCreate = true
	writePool := pool.WithWriteSnapshot(rados.SnapshotContext{Sequence: snapshotID, Snapshots: []uint64{snapshotID}})
	if _, err := writePool.Object("go-self").WriteFull(ctx, []byte("self-after")); err != nil {
		return fmt.Errorf("self-managed write context: %w", err)
	}
	result.SelfManagedWriteCtx = true
	data, _, err := pool.WithReadSnapshot(snapshotID).Object("go-self").Read(ctx, 0, 64)
	if err != nil || string(data) != "self-before" {
		return fmt.Errorf("self-managed read=%q: %w", data, err)
	}
	result.SelfManagedRead = true
	if _, err := writePool.Object("go-self").RollbackToSelfManagedSnapshot(ctx, snapshotID); err != nil {
		return fmt.Errorf("self-managed rollback: %w", err)
	}
	if err := expectData(ctx, object, "self-before"); err != nil {
		return err
	}
	result.SelfManagedRollback = true
	if err := pool.RemoveSelfManagedSnapshot(ctx, snapshotID); err != nil {
		return fmt.Errorf("remove self-managed snapshot: %w", err)
	}
	result.SelfManagedRemove = true
	return nil
}

func exerciseSpecialized(ctx context.Context, replicated, ec rados.Pool, result *report) error {
	replicatedEC, err := replicated.IsErasureCoded(ctx)
	if err != nil {
		return err
	}
	replicatedRequires, err := replicated.RequiresAlignment(ctx)
	if err != nil {
		return err
	}
	replicatedAlignment, err := replicated.RequiredAlignment(ctx)
	if err != nil || replicatedEC || replicatedRequires || replicatedAlignment != 0 {
		return fmt.Errorf("replicated capabilities ec=%v requires=%v alignment=%d: %w", replicatedEC, replicatedRequires, replicatedAlignment, err)
	}
	result.ReplicatedCapabilities = true
	ecMode, err := ec.IsErasureCoded(ctx)
	if err != nil {
		return err
	}
	requires, err := ec.RequiresAlignment(ctx)
	if err != nil {
		return err
	}
	alignment, err := ec.RequiredAlignment(ctx)
	if err != nil || !ecMode || !requires || alignment == 0 {
		return fmt.Errorf("EC capabilities ec=%v requires=%v alignment=%d: %w", ecMode, requires, alignment, err)
	}
	result.ECCapabilities, result.ECAlignmentEvidence, result.RequiredAlignment = true, true, alignment

	object := replicated.Object("specialized")
	if _, err := object.WriteSame(ctx, 0, 16, []byte("ab")); err != nil {
		return fmt.Errorf("write same: %w", err)
	}
	if err := expectData(ctx, object, "abababababababab"); err != nil {
		return err
	}
	result.WriteSame = true
	checksumObject := replicated.Object("checksum")
	if _, err := checksumObject.WriteFull(ctx, []byte("abababababababab")); err != nil {
		return fmt.Errorf("checksum seed: %w", err)
	}
	checksum, err := checksumObject.Checksum(ctx, rados.ChecksumCRC32C, make([]byte, 4), 0, 16, 8)
	if err != nil {
		return fmt.Errorf("checksum: %w", err)
	}
	wantChecksum := make([]byte, 12)
	binary.LittleEndian.PutUint32(wantChecksum, 2)
	crc := crc32.Update(^uint32(0), crc32.MakeTable(crc32.Castagnoli), []byte("abababab")) ^ uint32(0xffffffff)
	binary.LittleEndian.PutUint32(wantChecksum[4:], crc)
	binary.LittleEndian.PutUint32(wantChecksum[8:], crc)
	if !bytes.Equal(checksum, wantChecksum) {
		return fmt.Errorf("checksum %x, want %x", checksum, wantChecksum)
	}
	result.Checksum, result.ChecksumHex = true, hex.EncodeToString(checksum)
	if _, err := object.SetAllocationHint(ctx, 4096, 1024); err != nil {
		return fmt.Errorf("allocation hint: %w", err)
	}
	result.AllocationHint = true
	if _, err := object.Zero(ctx, 4, 4); err != nil {
		return err
	}
	extents, _, err := object.SparseRead(ctx, 0, 16)
	if err != nil || len(extents) == 0 {
		return fmt.Errorf("sparse read %+v: %w", extents, err)
	}
	result.SparseRead = true
	info, err := object.Stat(ctx)
	if err != nil {
		return err
	}
	copyObject := replicated.Object("go-copy")
	if _, err := copyObject.CopyFrom(ctx, object, info.Version); err != nil {
		return fmt.Errorf("copy from: %w", err)
	}
	if err := expectDataBytes(ctx, copyObject, []byte("abab\x00\x00\x00\x00abababab")); err != nil {
		return err
	}
	result.CopyFrom = true
	copyFrom2 := replicated.Object("go-copy-from2")
	if _, err := copyFrom2.CopyFrom2(ctx, object, info.Version, 1, 8); err != nil {
		return fmt.Errorf("copy from2: %w", err)
	}
	if err := expectDataBytes(ctx, copyFrom2, []byte("abab\x00\x00\x00\x00abababab")); err != nil {
		return fmt.Errorf("copy from2 content: %w", err)
	}
	result.CopyFrom2 = true

	ecObject := ec.Object("ec-specialized")
	aligned := bytes.Repeat([]byte("e"), int(alignment))
	if _, err := ecObject.WriteFull(ctx, aligned); err != nil {
		return fmt.Errorf("EC aligned write: %w", err)
	}
	if err := expectDataBytes(ctx, ecObject, aligned); err != nil {
		return err
	}
	result.ECWriteRead = true
	if _, err := ecObject.Write(ctx, 0, []byte("x")); !errors.Is(err, rados.ErrUnsupported) {
		return fmt.Errorf("EC partial-overwrite rejection: %w", err)
	}
	result.ECOverwriteRejected = true
	if _, err := ecObject.WriteSame(ctx, 0, alignment, []byte("ec")); !errors.Is(err, rados.ErrUnsupported) {
		return fmt.Errorf("EC write-same rejection: %w", err)
	}
	result.ECWriteSameRejected = true
	if value, err := ecObject.Checksum(ctx, rados.ChecksumCRC32C, make([]byte, 4), 0, alignment, alignment); err != nil || len(value) == 0 {
		return fmt.Errorf("EC checksum %x: %w", value, err)
	}
	result.ECChecksum = true
	if _, err := ecObject.SetAllocationHint(ctx, alignment, alignment); err != nil {
		return fmt.Errorf("EC allocation hint: %w", err)
	}
	result.ECAllocationHint = true
	if extents, _, err := ecObject.SparseRead(ctx, 0, alignment); err != nil || len(extents) == 0 {
		return fmt.Errorf("EC sparse read %+v: %w", extents, err)
	}
	result.ECSparseRead = true
	ecInfo, err := ecObject.Stat(ctx)
	if err != nil {
		return err
	}
	if _, err := ec.Object("ec-copy").CopyFrom(ctx, ecObject, ecInfo.Version); err != nil {
		return fmt.Errorf("EC copy from: %w", err)
	}
	result.ECCopyFrom = true
	op := rados.NewWriteOp()
	op.SetOMAP([]rados.OMAPEntry{{Key: []byte("key"), Value: []byte("value")}})
	if _, err := ecObject.ExecuteWrite(ctx, op); !errors.Is(err, rados.ErrUnsupported) {
		return fmt.Errorf("EC OMAP rejection: %w", err)
	}
	result.ECOMAPRejected = true
	return nil
}

func verifyNativeSeed(ctx context.Context, named, self rados.Pool, directory string, result *report) error {
	data, _, err := named.Object("native-named").Read(ctx, 0, 64)
	if err != nil || string(data) != "native-before" {
		return fmt.Errorf("native named seed=%q: %w", data, err)
	}
	selfData, _, err := self.Object("native-self").Read(ctx, 0, 64)
	if err != nil || string(selfData) != "native-before" {
		return fmt.Errorf("native self seed=%q: %w", selfData, err)
	}
	return os.WriteFile(directory+"/go-complete", nil, 0o600)
}

func containsSnapshot(values []rados.Snapshot, target rados.Snapshot) bool {
	for _, value := range values {
		if value.ID == target.ID && value.Name == target.Name && value.CreatedAt.Equal(target.CreatedAt) {
			return true
		}
	}
	return false
}

func expectData(ctx context.Context, object rados.ObjectRef, expected string) error {
	return expectDataBytes(ctx, object, []byte(expected))
}
func expectDataBytes(ctx context.Context, object rados.ObjectRef, expected []byte) error {
	data, _, err := object.Read(ctx, 0, uint64(len(expected))+1)
	if err != nil || !bytes.Equal(data, expected) {
		return fmt.Errorf("read %q, want %q: %w", data, expected, err)
	}
	return nil
}
