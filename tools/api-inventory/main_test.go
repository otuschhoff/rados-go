package main

import "testing"

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
