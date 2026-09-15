#define _POSIX_C_SOURCE 200809L
#include <dlfcn.h>
#include <errno.h>
#include <stdint.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

typedef void *rados_t;
typedef void *rados_ioctx_t;
typedef void (*watchcb2_t)(void *, uint64_t, uint64_t, uint64_t, void *, size_t);
typedef void (*watcherrcb_t)(void *, uint64_t, int);
struct notify_ack_t { uint64_t notifier_id; uint64_t cookie; char *payload; size_t payload_len; };
struct notify_timeout_t { uint64_t notifier_id; uint64_t cookie; };

typedef int (*create2_fn)(rados_t *, const char *, const char *, uint64_t);
typedef int (*conf_read_file_fn)(rados_t, const char *);
typedef int (*conf_set_fn)(rados_t, const char *, const char *);
typedef int (*connect_fn)(rados_t);
typedef int (*ioctx_create_fn)(rados_t, const char *, rados_ioctx_t *);
typedef void (*ioctx_destroy_fn)(rados_ioctx_t);
typedef void (*shutdown_fn)(rados_t);
typedef int (*write_full_fn)(rados_ioctx_t, const char *, const char *, size_t);
typedef int (*exec_fn)(rados_ioctx_t, const char *, const char *, const char *, const char *, size_t, char *, size_t);
typedef int (*lock_exclusive_fn)(rados_ioctx_t, const char *, const char *, const char *, const char *, struct timeval *, uint8_t);
typedef int (*lock_shared_fn)(rados_ioctx_t, const char *, const char *, const char *, const char *, const char *, struct timeval *, uint8_t);
typedef int (*unlock_fn)(rados_ioctx_t, const char *, const char *, const char *);
typedef int (*break_lock_fn)(rados_ioctx_t, const char *, const char *, const char *, const char *);
typedef ssize_t (*list_lockers_fn)(rados_ioctx_t, const char *, const char *, int *, char *, size_t *, char *, size_t *, char *, size_t *, char *, size_t *);
typedef int (*watch3_fn)(rados_ioctx_t, const char *, uint64_t *, watchcb2_t, watcherrcb_t, uint32_t, void *);
typedef int (*unwatch2_fn)(rados_ioctx_t, uint64_t);
typedef int (*watch_flush_fn)(rados_t);
typedef int (*notify2_fn)(rados_ioctx_t, const char *, const char *, int, uint64_t, char **, size_t *);
typedef int (*decode_notify_response_fn)(char *, size_t, struct notify_ack_t **, size_t *, struct notify_timeout_t **, size_t *);
typedef void (*free_notify_response_fn)(struct notify_ack_t *, size_t, struct notify_timeout_t *);
typedef int (*notify_ack_fn)(rados_ioctx_t, const char *, uint64_t, uint64_t, const char *, int);

struct api {
	void *library;
	create2_fn create2; conf_read_file_fn conf_read_file; conf_set_fn conf_set; connect_fn connect;
	ioctx_create_fn ioctx_create; ioctx_destroy_fn ioctx_destroy; shutdown_fn shutdown;
	write_full_fn write_full; exec_fn exec; lock_exclusive_fn lock_exclusive; lock_shared_fn lock_shared; unlock_fn unlock; break_lock_fn break_lock; list_lockers_fn list_lockers;
	watch3_fn watch3; unwatch2_fn unwatch2; watch_flush_fn watch_flush; notify2_fn notify2;
	decode_notify_response_fn decode_notify_response; free_notify_response_fn free_notify_response; notify_ack_fn notify_ack;
};

static void bind_symbol(void *library, const char *name, void *target, size_t size) {
	void *symbol = dlsym(library, name);
	if (symbol == NULL || size != sizeof(symbol)) { fprintf(stderr, "native p09: bind %s: %s\n", name, dlerror()); exit(2); }
	memcpy(target, &symbol, size);
}
#define BIND(api, name) bind_symbol((api)->library, "rados_" #name, &(api)->name, sizeof((api)->name))

static struct api load_api(void) {
	struct api api = {0};
	api.library = dlopen("librados.so.2", RTLD_NOW | RTLD_LOCAL);
	if (api.library == NULL) { fprintf(stderr, "native p09: %s\n", dlerror()); exit(2); }
	BIND(&api, create2); BIND(&api, conf_read_file); BIND(&api, conf_set); BIND(&api, connect);
	BIND(&api, ioctx_create); BIND(&api, ioctx_destroy); BIND(&api, shutdown); BIND(&api, write_full); BIND(&api, exec);
	BIND(&api, lock_exclusive); BIND(&api, lock_shared); BIND(&api, unlock); BIND(&api, break_lock); BIND(&api, list_lockers); BIND(&api, watch3); BIND(&api, unwatch2);
	BIND(&api, watch_flush); BIND(&api, notify2); BIND(&api, decode_notify_response); BIND(&api, free_notify_response); BIND(&api, notify_ack);
	return api;
}
static void check(int result, const char *operation) {
	if (result < 0) { fprintf(stderr, "native p09: %s: %s (%d)\n", operation, strerror(-result), result); exit(1); }
}

static struct api *callback_api;
static rados_ioctx_t callback_io;
static _Atomic unsigned callback_phases;
static _Atomic int callback_error;
static void watch_callback(void *arg, uint64_t notify_id, uint64_t cookie, uint64_t notifier_id, void *data, size_t length) {
	(void)arg; (void)notifier_id;
	unsigned phase = 0;
	if (length == 9 && memcmp(data, "go-native", 9) == 0) phase = 1;
	else if (length == 18 && memcmp(data, "after-remap-native", 18) == 0) phase = 2;
	else if (length == 20 && memcmp(data, "after-restart-native", 20) == 0) phase = 4;
	else if (length == 9 && memcmp(data, "native-go", 9) == 0) phase = 8;
	else atomic_store(&callback_error, 1);
	unsigned previous = atomic_fetch_or(&callback_phases, phase);
	if (phase == 0 || (previous & phase) != 0) atomic_store(&callback_error, 1);
	if (callback_api->notify_ack(callback_io, "coordination", notify_id, cookie, "native-ack", 10) < 0) atomic_store(&callback_error, 1);
}
static void watch_error(void *arg, uint64_t cookie, int error) { (void)arg; (void)cookie; atomic_store(&callback_error, error == 0 ? 1 : error); }

static void seed(struct api *api, rados_ioctx_t io) {
	check(api->write_full(io, "coordination", "ready", 5), "create coordination object");
	check(api->write_full(io, "class-exec", "ready", 5), "create class execution object");
	char output[4096];
	int output_length = api->exec(io, "class-exec", "lock", "list_locks", NULL, 0, output, sizeof(output));
	check(output_length, "generic exec");
	if (output_length == 0) { fprintf(stderr, "native p09: generic exec returned no output\n"); exit(1); }
	struct timeval duration = {30, 0};
	check(api->lock_exclusive(io, "coordination", "native-lock", "native-cookie", "native holder", &duration, 0), "native lock");
	struct timeval renewed = {60, 0};
	check(api->lock_exclusive(io, "coordination", "native-lock", "native-cookie", "native holder renewed", &renewed, 1), "native lock renew");
	check(api->lock_exclusive(io, "coordination", "native-release", "native-release-cookie", "native release holder", &duration, 0), "native release lock");
	check(api->unlock(io, "coordination", "native-release", "native-release-cookie"), "native lock release");
	struct timeval expiry = {2, 0};
	check(api->lock_exclusive(io, "coordination", "native-expiry", "native-expiry-cookie", "native expiring holder", &expiry, 0), "native expiring lock");
	printf("{\"native_exec\":true,\"native_exec_result\":%d,\"native_exec_output\":\"", output_length);
	for (int index = 0; index < output_length; ++index) printf("%02x", (unsigned char)output[index]);
	printf("\",\"native_lock_seed\":true,\"native_lock_renew\":true,\"native_lock_release\":true,\"native_lock_expiry\":true}\n");
}

static void verify(struct api *api, rados_ioctx_t io) {
	int exclusive = 0; char tag[128], clients[1024], cookies[1024], addresses[2048];
	size_t tag_len = sizeof(tag), clients_len = sizeof(clients), cookies_len = sizeof(cookies), addresses_len = sizeof(addresses);
	ssize_t count = api->list_lockers(io, "coordination", "go-lock", &exclusive, tag, &tag_len, clients, &clients_len, cookies, &cookies_len, addresses, &addresses_len);
	if (count != 1 || !exclusive || strstr(cookies, "go-cookie") == NULL) { fprintf(stderr, "native p09: Go lock mismatch count=%zd exclusive=%d\n", count, exclusive); exit(1); }
	check(api->break_lock(io, "coordination", "go-lock", clients, cookies), "native break Go lock");
	printf("{\"go_lock_native_read\":true,\"native_break_go_lock\":true}\n");
}

static void watch_mode(struct api *api, rados_t cluster, rados_ioctx_t io, const char *directory) {
	callback_api = api; callback_io = io;
	struct timeval shared_duration = {180, 0};
	check(api->lock_shared(io, "coordination", "native-shared", "native-shared-cookie", "native-shared-tag", "native shared holder", &shared_duration, 0), "native shared lock");
	uint64_t cookie = 0;
	check(api->watch3(io, "coordination", &cookie, watch_callback, watch_error, 30, NULL), "native watch");
	char ready[1024]; snprintf(ready, sizeof(ready), "%s/native-watch-ready", directory);
	FILE *file = fopen(ready, "w"); if (file == NULL) { perror("native watch ready"); exit(1); }
	fprintf(file, "%llu\n", (unsigned long long)cookie); fclose(file);
	struct timespec interval = {0, 100000000};
	for (int attempt = 0; attempt < 1200 && (atomic_load(&callback_phases) & 7) != 7 && atomic_load(&callback_error) == 0; ++attempt) nanosleep(&interval, NULL);
	unsigned phases = atomic_load(&callback_phases); int watch_errno = atomic_load(&callback_error);
	if ((phases & 7) != 7 || watch_errno != 0) { fprintf(stderr, "native p09: watch phases=%u error=%d\n", phases, watch_errno); exit(1); }
	struct timespec duplicate_window = {0, 500000000}; nanosleep(&duplicate_window, NULL);
	check(api->unwatch2(io, cookie), "native unwatch"); check(api->watch_flush(cluster), "native watch flush");
	phases = atomic_load(&callback_phases); watch_errno = atomic_load(&callback_error);
	if (phases != 15 || watch_errno != 0) { fprintf(stderr, "native p09: post-flush phases=%u error=%d\n", phases, watch_errno); exit(1); }
	check(api->unlock(io, "coordination", "native-shared", "native-shared-cookie"), "native shared unlock");
	printf("{\"go_notify_native_watch\":true,\"native_watch_remap\":true,\"native_watch_restart\":true,\"native_watch_same_cookie\":true,\"native_watch_exactly_once\":true,\"native_lock_shared\":true,\"native_shared_release\":true}\n");
}

static void notify_mode(struct api *api, rados_ioctx_t io) {
	char *reply = NULL; size_t reply_length = 0;
	check(api->notify2(io, "coordination", "native-go", 9, 5000, &reply, &reply_length), "native notify");
	struct notify_ack_t *acks = NULL; struct notify_timeout_t *timeouts = NULL; size_t ack_count = 0, timeout_count = 0;
	check(api->decode_notify_response(reply, reply_length, &acks, &ack_count, &timeouts, &timeout_count), "decode notify response");
	if (ack_count != 2 || timeout_count != 0) { fprintf(stderr, "native p09: notify acks=%zu timeouts=%zu\n", ack_count, timeout_count); exit(1); }
	api->free_notify_response(acks, ack_count, timeouts); free(reply);
	printf("{\"native_notify_go_watch\":true}\n");
}

int main(int argc, char **argv) {
	if (argc < 5 || argc > 6) { fprintf(stderr, "usage: %s seed|verify|watch|notify CONF KEYRING POOL [DIR]\n", argv[0]); return 2; }
	struct api api = load_api(); rados_t cluster = NULL; rados_ioctx_t io = NULL;
	check(api.create2(&cluster, "ceph", "client.p09", 0), "create"); check(api.conf_read_file(cluster, argv[2]), "config");
	check(api.conf_set(cluster, "keyring", argv[3]), "keyring"); check(api.connect(cluster), "connect"); check(api.ioctx_create(cluster, argv[4], &io), "pool");
	if (strcmp(argv[1], "seed") == 0) seed(&api, io);
	else if (strcmp(argv[1], "verify") == 0) verify(&api, io);
	else if (strcmp(argv[1], "watch") == 0 && argc == 6) watch_mode(&api, cluster, io, argv[5]);
	else if (strcmp(argv[1], "notify") == 0) notify_mode(&api, io);
	else { fprintf(stderr, "native p09: invalid mode\n"); return 2; }
	api.ioctx_destroy(io);
	api.shutdown(cluster);
	dlclose(api.library);
	return 0;
}
