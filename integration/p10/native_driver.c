#define _POSIX_C_SOURCE 200809L
#include <dlfcn.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef void *rados_t;
typedef void *rados_ioctx_t;
typedef uint64_t rados_snap_t;

typedef int (*create2_fn)(rados_t *, const char *, const char *, uint64_t);
typedef int (*conf_read_file_fn)(rados_t, const char *);
typedef int (*conf_set_fn)(rados_t, const char *, const char *);
typedef int (*connect_fn)(rados_t);
typedef int (*ioctx_create_fn)(rados_t, const char *, rados_ioctx_t *);
typedef void (*ioctx_destroy_fn)(rados_ioctx_t);
typedef void (*shutdown_fn)(rados_t);
typedef int (*write_full_fn)(rados_ioctx_t, const char *, const char *, size_t);
typedef int (*read_fn)(rados_ioctx_t, const char *, char *, size_t, uint64_t);
typedef int (*checksum_fn)(rados_ioctx_t, const char *, int, const char *, size_t, size_t, uint64_t, size_t, char *, size_t);
typedef int (*snap_create_fn)(rados_ioctx_t, const char *);
typedef int (*snap_remove_fn)(rados_ioctx_t, const char *);
typedef int (*snap_list_fn)(rados_ioctx_t, rados_snap_t *, int);
typedef int (*snap_lookup_fn)(rados_ioctx_t, const char *, rados_snap_t *);
typedef int (*snap_get_name_fn)(rados_ioctx_t, rados_snap_t, char *, int);
typedef int (*snap_get_stamp_fn)(rados_ioctx_t, rados_snap_t, time_t *);
typedef void (*snap_set_read_fn)(rados_ioctx_t, rados_snap_t);
typedef int (*snap_rollback_fn)(rados_ioctx_t, const char *, const char *);
typedef int (*self_snap_create_fn)(rados_ioctx_t, rados_snap_t *);
typedef int (*self_snap_remove_fn)(rados_ioctx_t, rados_snap_t);
typedef int (*self_snap_set_write_ctx_fn)(rados_ioctx_t, rados_snap_t, rados_snap_t *, int);
typedef int (*self_snap_rollback_fn)(rados_ioctx_t, const char *, rados_snap_t);

struct api {
	void *library;
	create2_fn create2;
	conf_read_file_fn conf_read_file;
	conf_set_fn conf_set;
	connect_fn connect;
	ioctx_create_fn ioctx_create;
	ioctx_destroy_fn ioctx_destroy;
	shutdown_fn shutdown;
	write_full_fn write_full;
	read_fn read;
	checksum_fn checksum;
	snap_create_fn ioctx_snap_create;
	snap_remove_fn ioctx_snap_remove;
	snap_list_fn ioctx_snap_list;
	snap_lookup_fn ioctx_snap_lookup;
	snap_get_name_fn ioctx_snap_get_name;
	snap_get_stamp_fn ioctx_snap_get_stamp;
	snap_set_read_fn ioctx_snap_set_read;
	snap_rollback_fn ioctx_snap_rollback;
	self_snap_create_fn ioctx_selfmanaged_snap_create;
	self_snap_remove_fn ioctx_selfmanaged_snap_remove;
	self_snap_set_write_ctx_fn ioctx_selfmanaged_snap_set_write_ctx;
	self_snap_rollback_fn ioctx_selfmanaged_snap_rollback;
};

static void bind_symbol(void *library, const char *name, void *target, size_t size) {
	void *symbol = dlsym(library, name);
	if (symbol == NULL || size != sizeof(symbol)) {
		fprintf(stderr, "native p10: bind %s: %s\n", name, dlerror());
		exit(2);
	}
	memcpy(target, &symbol, size);
}
#define BIND(api, field, symbol) bind_symbol((api)->library, "rados_" #symbol, &(api)->field, sizeof((api)->field))

static struct api load_api(void) {
	struct api api = {0};
	api.library = dlopen("librados.so.2", RTLD_NOW | RTLD_LOCAL);
	if (api.library == NULL) { fprintf(stderr, "native p10: %s\n", dlerror()); exit(2); }
	BIND(&api, create2, create2);
	BIND(&api, conf_read_file, conf_read_file);
	BIND(&api, conf_set, conf_set);
	BIND(&api, connect, connect);
	BIND(&api, ioctx_create, ioctx_create);
	BIND(&api, ioctx_destroy, ioctx_destroy);
	BIND(&api, shutdown, shutdown);
	BIND(&api, write_full, write_full);
	BIND(&api, read, read);
	BIND(&api, checksum, checksum);
	BIND(&api, ioctx_snap_create, ioctx_snap_create);
	BIND(&api, ioctx_snap_remove, ioctx_snap_remove);
	BIND(&api, ioctx_snap_list, ioctx_snap_list);
	BIND(&api, ioctx_snap_lookup, ioctx_snap_lookup);
	BIND(&api, ioctx_snap_get_name, ioctx_snap_get_name);
	BIND(&api, ioctx_snap_get_stamp, ioctx_snap_get_stamp);
	BIND(&api, ioctx_snap_set_read, ioctx_snap_set_read);
	BIND(&api, ioctx_snap_rollback, ioctx_snap_rollback);
	BIND(&api, ioctx_selfmanaged_snap_create, ioctx_selfmanaged_snap_create);
	BIND(&api, ioctx_selfmanaged_snap_remove, ioctx_selfmanaged_snap_remove);
	BIND(&api, ioctx_selfmanaged_snap_set_write_ctx, ioctx_selfmanaged_snap_set_write_ctx);
	BIND(&api, ioctx_selfmanaged_snap_rollback, ioctx_selfmanaged_snap_rollback);
	return api;
}

static void check(int result, const char *operation) {
	if (result < 0) { fprintf(stderr, "native p10: %s: %s (%d)\n", operation, strerror(-result), result); exit(1); }
}

static void expect_read(struct api *api, rados_ioctx_t io, const char *object, const char *expected) {
	char buffer[4096] = {0};
	int length = api->read(io, object, buffer, sizeof(buffer), 0);
	check(length, "read");
	if ((size_t)length != strlen(expected) || memcmp(buffer, expected, (size_t)length) != 0) {
		fprintf(stderr, "native p10: read %s mismatch length=%d\n", object, length);
		exit(1);
	}
}

static int contains_snap(const rados_snap_t *snapshots, int count, rados_snap_t target) {
	for (int index = 0; index < count; ++index) if (snapshots[index] == target) return 1;
	return 0;
}

static void named_seed(struct api *api, rados_ioctx_t io) {
	check(api->write_full(io, "native-named", "native-before", 13), "named seed write");
	check(api->ioctx_snap_create(io, "native-snapshot"), "named snapshot create");
	rados_snap_t snapshot = 0;
	check(api->ioctx_snap_lookup(io, "native-snapshot", &snapshot), "named snapshot lookup");
	if (snapshot == 0) { fprintf(stderr, "native p10: zero named snapshot id\n"); exit(1); }
	rados_snap_t snapshots[32] = {0};
	int count = api->ioctx_snap_list(io, snapshots, 32);
	check(count, "named snapshot list");
	if (!contains_snap(snapshots, count, snapshot)) { fprintf(stderr, "native p10: named snapshot absent from list\n"); exit(1); }
	char name[128] = {0};
	check(api->ioctx_snap_get_name(io, snapshot, name, sizeof(name)), "named snapshot name");
	time_t stamp = 0;
	check(api->ioctx_snap_get_stamp(io, snapshot, &stamp), "named snapshot stamp");
	if (strcmp(name, "native-snapshot") != 0 || stamp <= 0) { fprintf(stderr, "native p10: named snapshot metadata mismatch\n"); exit(1); }
	check(api->write_full(io, "native-named", "native-after", 12), "named post-snapshot write");
	api->ioctx_snap_set_read(io, snapshot);
	expect_read(api, io, "native-named", "native-before");
	api->ioctx_snap_set_read(io, UINT64_MAX - 1);
	check(api->ioctx_snap_rollback(io, "native-named", "native-snapshot"), "named snapshot rollback");
	expect_read(api, io, "native-named", "native-before");
	check(api->ioctx_snap_remove(io, "native-snapshot"), "named snapshot remove");
	if (api->ioctx_snap_lookup(io, "native-snapshot", &snapshot) >= 0) { fprintf(stderr, "native p10: removed named snapshot found\n"); exit(1); }
}

static void self_seed(struct api *api, rados_ioctx_t io) {
	check(api->write_full(io, "native-self", "native-before", 13), "self seed write");
	rados_snap_t snapshot = 0;
	check(api->ioctx_selfmanaged_snap_create(io, &snapshot), "self snapshot create");
	if (snapshot == 0) { fprintf(stderr, "native p10: zero self-managed snapshot id\n"); exit(1); }
	rados_snap_t snapshots[1] = {snapshot};
	check(api->ioctx_selfmanaged_snap_set_write_ctx(io, snapshot, snapshots, 1), "self write context");
	check(api->write_full(io, "native-self", "native-after", 12), "self post-snapshot write");
	api->ioctx_snap_set_read(io, snapshot);
	expect_read(api, io, "native-self", "native-before");
	api->ioctx_snap_set_read(io, UINT64_MAX - 1);
	check(api->ioctx_selfmanaged_snap_rollback(io, "native-self", snapshot), "self snapshot rollback");
	expect_read(api, io, "native-self", "native-before");
	check(api->ioctx_selfmanaged_snap_remove(io, snapshot), "self snapshot remove");
}

static void seed(struct api *api, rados_t cluster) {
	rados_ioctx_t named = NULL, self = NULL;
	check(api->ioctx_create(cluster, "p10-named", &named), "open named pool");
	check(api->ioctx_create(cluster, "p10-self", &self), "open self-managed pool");
	named_seed(api, named);
	self_seed(api, self);
	api->ioctx_destroy(self);
	api->ioctx_destroy(named);
	printf("{\"named_create_list_lookup_name_stamp\":true,\"named_read_rollback_remove\":true,\"self_managed_create_write_read_rollback_remove\":true}\n");
}

static void verify(struct api *api, rados_t cluster) {
	rados_ioctx_t named = NULL, ec = NULL;
	check(api->ioctx_create(cluster, "p10-named", &named), "open named pool");
	check(api->ioctx_create(cluster, "p10-ec", &ec), "open EC pool");
	rados_snap_t snapshot = 0;
	check(api->ioctx_snap_lookup(named, "go-native-check", &snapshot), "lookup Go snapshot");
	char name[128] = {0}; time_t stamp = 0;
	check(api->ioctx_snap_get_name(named, snapshot, name, sizeof(name)), "get Go snapshot name");
	check(api->ioctx_snap_get_stamp(named, snapshot, &stamp), "get Go snapshot stamp");
	if (strcmp(name, "go-native-check") != 0 || stamp <= 0) { fprintf(stderr, "native p10: Go snapshot metadata mismatch\n"); exit(1); }
	api->ioctx_snap_set_read(named, snapshot);
	expect_read(api, named, "go-named", "go-final");
	api->ioctx_snap_set_read(named, UINT64_MAX - 1);
	expect_read(api, named, "go-named", "go-head");
	char buffer[32] = {0};
	const char expected_copy[16] = {'a', 'b', 'a', 'b', 0, 0, 0, 0, 'a', 'b', 'a', 'b', 'a', 'b', 'a', 'b'};
	int length = api->read(named, "go-copy", buffer, sizeof(buffer), 0);
	check(length, "read Go copy");
	if (length != 16 || memcmp(buffer, expected_copy, sizeof(expected_copy)) != 0) { fprintf(stderr, "native p10: Go copy mismatch length=%d\n", length); exit(1); }
	memset(buffer, 0, sizeof(buffer));
	length = api->read(named, "go-copy-from2", buffer, sizeof(buffer), 0);
	check(length, "read Go copy-from2");
	if (length != 16 || memcmp(buffer, expected_copy, sizeof(expected_copy)) != 0) { fprintf(stderr, "native p10: Go copy-from2 mismatch length=%d\n", length); exit(1); }
	length = api->read(ec, "ec-copy", buffer, sizeof(buffer), 0);
	check(length, "read Go EC copy");
	if (length <= 0) { fprintf(stderr, "native p10: empty Go EC copy\n"); exit(1); }
	const unsigned char expected_checksum[12] = {2, 0, 0, 0, 0xf5, 0xbe, 0x86, 0x2a, 0xf5, 0xbe, 0x86, 0x2a};
	unsigned char checksum[12] = {0};
	uint32_t seed = 0;
	check(api->checksum(named, "checksum", 2, (const char *)&seed, sizeof(seed), 16, 0, 8, (char *)checksum, sizeof(checksum)), "checksum Go object");
	if (memcmp(checksum, expected_checksum, sizeof(checksum)) != 0) { fprintf(stderr, "native p10: Go checksum mismatch\n"); exit(1); }
	check(api->ioctx_snap_remove(named, "go-native-check"), "remove Go snapshot");
	api->ioctx_destroy(ec);
	api->ioctx_destroy(named);
	printf("{\"go_named_snapshot\":true,\"go_named_head\":true,\"go_copy\":true,\"go_copy_from2\":true,\"go_ec_copy\":true,\"go_checksum_hex\":\"02000000f5be862af5be862a\"}\n");
}

int main(int argc, char **argv) {
	if (argc != 4) { fprintf(stderr, "usage: %s seed|verify CONF KEYRING\n", argv[0]); return 2; }
	struct api api = load_api();
	rados_t cluster = NULL;
	check(api.create2(&cluster, "ceph", "client.p10", 0), "create");
	check(api.conf_read_file(cluster, argv[2]), "config");
	check(api.conf_set(cluster, "keyring", argv[3]), "keyring");
	check(api.connect(cluster), "connect");
	if (strcmp(argv[1], "seed") == 0) seed(&api, cluster);
	else if (strcmp(argv[1], "verify") == 0) verify(&api, cluster);
	else { fprintf(stderr, "native p10: invalid mode\n"); return 2; }
	api.shutdown(cluster);
	dlclose(api.library);
	return 0;
}
