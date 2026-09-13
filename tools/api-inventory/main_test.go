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
