package main

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExtractCOnlyExportedDeclarations(t *testing.T) {
	source := `
// rados_from_comment();
CEPH_RADOS_API int rados_one(rados_t cluster);
static int rados_private(void);
CEPH_RADOS_API void rados_two(void);
`
	entries := extractC(source)
	if len(entries) != 2 || entries[0].symbol != "rados_one" || entries[1].symbol != "rados_two" {
		t.Fatalf("unexpected entries: %#v", entries)
	}
}

func TestExtractCPPStructAndOverloads(t *testing.T) {
	source := `
struct CEPH_RADOS_API Completion {
  int wait();
  int wait(int timeout);
private:
  void hidden();
};
class CEPH_RADOS_API Client {
public:
  void connect();
private:
  void secret();
};
`
	entries := extractCPP(source)
	if len(entries) != 3 {
		t.Fatalf("got %d entries: %#v", len(entries), entries)
	}
}

func TestExtractCPPPointerReturnsAndAttributes(t *testing.T) {
	source := `
class CEPH_RADOS_API Rados {
public:
  static PoolAsyncCompletion *pool_async_create_completion();
  static AioCompletion *aio_create_completion();
  static AioCompletion *aio_create_completion(void *cb_arg, callback_t cb_complete,
                                               callback_t cb_safe)
    __attribute__ ((deprecated));
  static AioCompletion *aio_create_completion(void *cb_arg, callback_t cb_complete);
};
class CEPH_RADOS_API NObjectIterator {
public:
  NObjectIterator &operator++();
};
`
	entries := extractCPP(source)
	if len(entries) != 5 {
		t.Fatalf("got %d entries: %#v", len(entries), entries)
	}
	for _, item := range entries {
		if item.symbol == "Rados::__attribute__" {
			t.Fatalf("attribute extracted as method: %#v", item)
		}
	}
}

func TestClassifyAsyncByUnderlyingOperation(t *testing.T) {
	tests := []struct {
		symbol string
		phase  string
	}{
		{"rados_aio_read", "P06"},
		{"rados_aio_write", "P07"},
		{"rados_aio_getxattr", "P08"},
		{"rados_aio_read_op_operate", "P08"},
		{"rados_aio_exec", "P09"},
		{"rados_aio_notify", "P09"},
		{"rados_aio_ioctx_selfmanaged_snap_create", "P10"},
		{"rados_aio_writesame", "P10"},
		{"rados_aio_get_version", "P07"},
		{"rados_read_op_assert_version", "P08"},
	}
	for _, test := range tests {
		t.Run(test.symbol, func(t *testing.T) {
			_, _, phase, _, _ := classify(entry{language: "C", owner: "global", symbol: test.symbol, signature: test.symbol + "();"})
			if phase != test.phase {
				t.Fatalf("phase = %s, want %s", phase, test.phase)
			}
		})
	}
}

func TestPhaseSpecificClassifications(t *testing.T) {
	entries := append(extractC(`
CEPH_RADOS_API void rados_conf_set(void);
CEPH_RADOS_API void rados_create2(void);
CEPH_RADOS_API void rados_cct(void);
CEPH_RADOS_API void rados_version(void);
CEPH_RADOS_API void rados_pool_lookup(void);
CEPH_RADOS_API void rados_ping_monitor(void);
CEPH_RADOS_API void rados_read(void);
CEPH_RADOS_API void rados_aio_stat2(void);
CEPH_RADOS_API void rados_aio_append(void);
CEPH_RADOS_API void rados_get_last_version(void);
`), extractCPP(`
class CEPH_RADOS_API IoCtx {
public:
  void set_namespace();
  void write_full();
};
class CEPH_RADOS_API PlacementGroup {
public:
  bool parse();
};
class CEPH_RADOS_API AioCompletion {
public:
  int get_return_value();
};
`)...)
	bySymbol := make(map[string]entry, len(entries))
	for _, item := range entries {
		bySymbol[item.symbol] = item
	}
	tests := []struct {
		symbol      string
		equivalent  string
		disposition string
		phase       string
	}{
		{"rados_conf_set", "rados.Client / rados.Config lifecycle and configuration methods", "implemented", "P01/P04"},
		{"rados_create2", "rados.New", "go-native", "P01/P04"},
		{"rados_cct", "none", "intentional-omission: native handle access violates pure-Go boundary", "P01"},
		{"rados_version", "none", "intentional-omission: version API absent from frozen v1 contract", "P01"},
		{"rados_pool_lookup", "rados.Client pool/map discovery methods", "implemented", "P04"},
		{"rados_ping_monitor", "none", "intentional-omission: discovery diagnostic absent from frozen v1 contract", "P04"},
		{"IoCtx::set_namespace", "rados.Pool.WithNamespace / rados.Pool.WithLocator", "implemented", "P05"},
		{"PlacementGroup::parse", "none", "intentional-omission: placement diagnostic absent from frozen v1 contract", "P05"},
		{"rados_read", "rados.ObjectRef.Read / rados.ObjectRef.Stat", "implemented", "P06"},
		{"rados_aio_stat2", "rados.ObjectRef.Read / rados.ObjectRef.Stat", "go-native", "P06"},
		{"IoCtx::write_full", "rados.ObjectRef mutation methods", "implemented", "P07"},
		{"rados_aio_append", "rados.ObjectRef mutation methods", "go-native", "P07"},
		{"AioCompletion::get_return_value", "context.Context, operation results, and rados.Client.Flush", "go-native", "P07"},
		{"rados_get_last_version", "context.Context, operation results, and rados.Client.Flush", "go-native", "P07"},
	}
	for _, test := range tests {
		t.Run(test.symbol, func(t *testing.T) {
			item, found := bySymbol[test.symbol]
			if !found {
				t.Fatalf("representative symbol was not extracted")
			}
			equivalent, disposition, phase, _, _ := classify(item)
			if equivalent != test.equivalent || disposition != test.disposition || phase != test.phase {
				t.Fatalf("classification = (%q, %q, %q), want (%q, %q, %q)", equivalent, disposition, phase, test.equivalent, test.disposition, test.phase)
			}
		})
	}
}

func TestGeneratedInventoryInvariant(t *testing.T) {
	file, err := os.Open(filepath.Join("..", "..", "docs", "p00", "api-inventory.csv"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	records, err := csv.NewReader(file).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) < 2 {
		t.Fatal("generated inventory has no API rows")
	}
	for rowIndex, record := range records[1:] {
		if len(record) != len(records[0]) {
			t.Fatalf("row %d has %d fields, want %d", rowIndex+2, len(record), len(records[0]))
		}
		for column := 4; column < len(record); column++ {
			if strings.TrimSpace(record[column]) == "" {
				t.Errorf("row %d column %q is empty", rowIndex+2, records[0][column])
			}
		}
		disposition := record[5]
		if disposition != "implemented" && disposition != "go-native" && !strings.HasPrefix(disposition, "intentional-omission") && !strings.HasPrefix(disposition, "deferred") {
			t.Errorf("row %d has invalid disposition %q", rowIndex+2, disposition)
		}
	}
}

func TestImplementedClassifications(t *testing.T) {
	want := map[string]classification{
		"rados_read_op_set_flags": {
			"rados.ReadOp.SetFlags", "implemented", "P08", "replicated pools on certified Ceph 20.2.4",
			"Exposes the qualified FAILOK sub-operation flag; rejects unknown flags", "P08 unit and live compound FAILOK tests",
		},
		"rados_write_op_set_flags": {
			"rados.WriteOp.SetFlags", "implemented", "P08", "replicated pools on certified Ceph 20.2.4",
			"Exposes the qualified FAILOK sub-operation flag; preserves typed create flags", "P08 unit and live compound FAILOK tests",
		},
		"rados_write_op_omap_rm_range2": {
			"rados.WriteOp.RemoveOMAPRange", "implemented", "P08", "replicated pools on certified Ceph 20.2.4",
			"Binary-safe half-open key range in a server-owned compound", "P08 codec and live metadata tests",
		},
		"IoCtx::omap_get_header": {
			"rados.ObjectRef.GetOMAPHeader", "implemented", "P08", "replicated pools on certified Ceph 20.2.4",
			"Returns caller-owned header bytes", "P08 unit and live metadata tests",
		},
		"IoCtx::omap_get_vals_by_keys": {
			"rados.ObjectRef.GetOMAP", "implemented", "P08", "replicated pools on certified Ceph 20.2.4",
			"Binary-safe key selection returns caller-owned entries", "P08 codec and live metadata tests",
		},
		"ObjectReadOperation::omap_get_header": {
			"rados.ReadOp.GetOMAPHeader", "implemented", "P08", "replicated pools on certified Ceph 20.2.4",
			"Ordered compound result uses Go-owned bytes", "P08 unit and live compound tests",
		},
		"rados_aio_flush": {
			"rados.Client.Flush", "implemented", "P07", "replicated pools on certified Ceph 20.2.4",
			"Watermark-based context-aware drain; no public C completion allocation", "P07 mutation/flush unit and live-cluster tests",
		},
		"rados_aio_flush_async": {
			"rados.Client.Flush", "implemented", "P07", "replicated pools on certified Ceph 20.2.4",
			"Unified context-aware drain; no callback completion allocation", "P07 mutation/flush unit and live-cluster tests",
		},
		"rados_append": {
			"rados.ObjectRef.Append", "implemented", "P07", "replicated pools on certified Ceph 20.2.4",
			"Returns OpResult version; ambiguity is surfaced as outcome unknown", "P07 native/Go CRUD and primary-remap append-once tests",
		},
		"rados_remove": {
			"rados.ObjectRef.Remove", "implemented", "P07", "replicated pools on certified Ceph 20.2.4",
			"Context-aware and returns OpResult version", "P07 native/Go CRUD and missing-object tests",
		},
		"rados_trunc": {
			"rados.ObjectRef.Truncate", "implemented", "P07", "replicated pools on certified Ceph 20.2.4",
			"Context-aware and returns OpResult version", "P07 native/Go CRUD tests",
		},
		"rados_write": {
			"rados.ObjectRef.Write", "implemented", "P07", "replicated pools on certified Ceph 20.2.4",
			"Checked offsets; copied input; returns OpResult version", "P07 native/Go CRUD tests",
		},
		"rados_write_full": {
			"rados.ObjectRef.WriteFull", "implemented", "P07", "replicated pools on certified Ceph 20.2.4",
			"Atomic full replacement; copied input; returns OpResult version", "P07 native/Go CRUD and benchmark tests",
		},
	}
	if len(implementedClassifications) != len(want) {
		t.Fatalf("implemented classifications = %d, want %d", len(implementedClassifications), len(want))
	}
	for symbol, expected := range want {
		if actual := implementedClassifications[symbol]; actual != expected {
			t.Errorf("%s classification = %#v, want %#v", symbol, actual, expected)
		}
	}
}

func TestP09Classifications(t *testing.T) {
	for _, test := range []struct {
		symbol      string
		disposition string
	}{
		{symbol: "rados_exec", disposition: "implemented"},
		{symbol: "rados_lock_exclusive", disposition: "implemented"},
		{symbol: "rados_watch3", disposition: "implemented"},
		{symbol: "rados_watch_flush", disposition: "go-native"},
		{symbol: "rados_decode_notify_response", disposition: "go-native"},
	} {
		_, disposition, phase, _, _ := classify(entry{symbol: test.symbol})
		if disposition != test.disposition || phase != "P09" {
			t.Errorf("%s classification disposition=%q phase=%q", test.symbol, disposition, phase)
		}
	}
}

func TestP10Classifications(t *testing.T) {
	for _, test := range []struct {
		symbol      string
		disposition string
	}{
		{symbol: "rados_ioctx_snap_create", disposition: "implemented"},
		{symbol: "rados_ioctx_snap_remove", disposition: "implemented"},
		{symbol: "rados_ioctx_snap_list", disposition: "implemented"},
		{symbol: "rados_ioctx_snap_lookup", disposition: "implemented"},
		{symbol: "rados_ioctx_snap_get_name", disposition: "implemented"},
		{symbol: "rados_ioctx_snap_get_stamp", disposition: "implemented"},
		{symbol: "rados_ioctx_snap_set_read", disposition: "implemented"},
		{symbol: "rados_ioctx_snap_rollback", disposition: "implemented"},
		{symbol: "rados_ioctx_selfmanaged_snap_create", disposition: "implemented"},
		{symbol: "rados_ioctx_selfmanaged_snap_remove", disposition: "implemented"},
		{symbol: "rados_ioctx_selfmanaged_snap_set_write_ctx", disposition: "implemented"},
		{symbol: "rados_ioctx_selfmanaged_snap_rollback", disposition: "implemented"},
		{symbol: "Rados::pool_is_in_selfmanaged_snaps_mode", disposition: "implemented"},
		{symbol: "rados_writesame", disposition: "implemented"},
		{symbol: "rados_write_op_writesame", disposition: "implemented"},
		{symbol: "rados_checksum", disposition: "implemented"},
		{symbol: "rados_read_op_checksum", disposition: "implemented"},
		{symbol: "IoCtx::sparse_read", disposition: "implemented"},
		{symbol: "ObjectReadOperation::sparse_read", disposition: "implemented"},
		{symbol: "rados_set_alloc_hint", disposition: "implemented"},
		{symbol: "rados_write_op_set_alloc_hint", disposition: "implemented"},
		{symbol: "rados_ioctx_pool_required_alignment2", disposition: "implemented"},
		{symbol: "rados_ioctx_pool_requires_alignment2", disposition: "implemented"},
		{symbol: "ObjectWriteOperation::copy_from", disposition: "implemented"},
		{symbol: "ObjectWriteOperation::copy_from2", disposition: "implemented"},
		{symbol: "rados_aio_ioctx_selfmanaged_snap_create", disposition: "go-native"},
		{symbol: "IoCtx::aio_selfmanaged_snap_remove", disposition: "go-native"},
		{symbol: "rados_aio_writesame", disposition: "go-native"},
		{symbol: "IoCtx::aio_sparse_read", disposition: "go-native"},
		{symbol: "IoCtx::mapext", disposition: "intentional-omission: non-frozen P10 variant"},
		{symbol: "IoCtx::list_snaps", disposition: "intentional-omission: non-frozen P10 variant"},
		{symbol: "Rados::get_inconsistent_snapsets", disposition: "intentional-omission: non-frozen P10 variant"},
		{symbol: "IoCtx::set_alloc_hint2", disposition: "intentional-omission: non-frozen P10 variant"},
		{symbol: "ObjectWriteOperation::set_alloc_hint2", disposition: "intentional-omission: non-frozen P10 variant"},
		{symbol: "IoCtx::pool_required_alignment", disposition: "intentional-omission: non-frozen P10 variant"},
		{symbol: "IoCtx::pool_requires_alignment", disposition: "intentional-omission: non-frozen P10 variant"},
	} {
		_, disposition, phase, _, _ := classify(entry{symbol: test.symbol})
		if disposition != test.disposition || phase != "P10" {
			t.Errorf("%s classification disposition=%q phase=%q", test.symbol, disposition, phase)
		}
	}
}

func TestP11Classifications(t *testing.T) {
	for _, test := range []struct {
		symbol      string
		disposition string
	}{
		{symbol: "rados_cluster_stat", disposition: "implemented"},
		{symbol: "rados_ioctx_pool_stat", disposition: "implemented"},
		{symbol: "rados_mon_command", disposition: "implemented"},
		{symbol: "rados_mgr_command", disposition: "implemented"},
		{symbol: "rados_osd_command", disposition: "implemented"},
		{symbol: "rados_pg_command", disposition: "implemented"},
		{symbol: "rados_pool_create", disposition: "implemented"},
		{symbol: "rados_pool_delete_async", disposition: "go-native"},
		{symbol: "rados_getaddrs", disposition: "implemented"},
		{symbol: "rados_inconsistent_pg_list", disposition: "implemented"},
		{symbol: "rados_mgr_command_target", disposition: "intentional-omission: non-frozen P11 variant"},
		{symbol: "rados_mon_command_target", disposition: "intentional-omission: non-frozen P11 variant"},
		{symbol: "rados_pool_create_with_crush_rule", disposition: "intentional-omission: non-frozen P11 variant"},
		{symbol: "Rados::test_blocklist_self", disposition: "intentional-omission: non-frozen P11 variant"},
	} {
		_, disposition, phase, _, _ := classify(entry{symbol: test.symbol})
		if disposition != test.disposition || phase != "P11" {
			t.Errorf("%s classification disposition=%q phase=%q", test.symbol, disposition, phase)
		}
	}
}
