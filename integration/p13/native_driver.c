#define _POSIX_C_SOURCE 200809L
#include <dlfcn.h>
#include <errno.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

typedef void *rados_t;
typedef void *rados_ioctx_t;
typedef void *rados_completion_t;
typedef int (*create2_fn)(rados_t *, const char *, const char *, uint64_t);
typedef int (*conf_read_file_fn)(rados_t, const char *);
typedef int (*conf_set_fn)(rados_t, const char *, const char *);
typedef int (*connect_fn)(rados_t);
typedef int (*ioctx_create_fn)(rados_t, const char *, rados_ioctx_t *);
typedef void (*ioctx_destroy_fn)(rados_ioctx_t);
typedef void (*shutdown_fn)(rados_t);
typedef int (*write_full_fn)(rados_ioctx_t, const char *, const char *, size_t);
typedef int (*append_fn)(rados_ioctx_t, const char *, const char *, size_t);
typedef ssize_t (*read_fn)(rados_ioctx_t, const char *, char *, size_t, uint64_t);
typedef uint64_t (*get_last_version_fn)(rados_ioctx_t);
typedef void (*watch_callback_fn)(void *, uint64_t, uint64_t, uint64_t, void *, size_t);
typedef void (*watch_error_callback_fn)(void *, uint64_t, int);
typedef int (*watch3_fn)(rados_ioctx_t, const char *, uint64_t *, watch_callback_fn, watch_error_callback_fn, uint32_t, void *);
typedef int (*unwatch2_fn)(rados_ioctx_t, uint64_t);
typedef int (*notify_ack_fn)(rados_ioctx_t, const char *, uint64_t, uint64_t, const char *, int);
typedef int (*watch_flush_fn)(rados_t);
typedef int (*mon_command_fn)(rados_t, const char **, size_t, const char *, size_t, char **, size_t *, char **, size_t *);
typedef int (*osd_command_fn)(rados_t, int, const char **, size_t, const char *, size_t, char **, size_t *, char **, size_t *);
typedef int (*pg_command_fn)(rados_t, const char *, const char **, size_t, const char *, size_t, char **, size_t *, char **, size_t *);
typedef void (*buffer_free_fn)(char *);

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
	append_fn append;
	read_fn read;
	get_last_version_fn get_last_version;
	watch3_fn watch3;
	unwatch2_fn unwatch2;
	notify_ack_fn notify_ack;
	watch_flush_fn watch_flush;
	mon_command_fn mon_command;
	osd_command_fn osd_command;
	pg_command_fn pg_command;
	buffer_free_fn buffer_free;
};

struct watch_context {
	struct api *api;
	rados_ioctx_t io;
	const char *object;
	const char *control;
	atomic_uint events;
	atomic_uint interruptions;
	atomic_int callback_error;
};

static void bind_symbol(void *library, const char *name, void *target, size_t size) {
	void *symbol = dlsym(library, name);
	if (symbol == NULL || size != sizeof(symbol)) {
		fprintf(stderr, "native p13: bind %s: %s\n", name, dlerror());
		exit(2);
	}
	memcpy(target, &symbol, size);
}
#define BIND(api, name) bind_symbol((api)->library, "rados_" #name, &(api)->name, sizeof((api)->name))

static struct api load_api(void) {
	struct api api = {0};
	api.library = dlopen("librados.so.2", RTLD_NOW | RTLD_LOCAL);
	if (api.library == NULL) { fprintf(stderr, "native p13: %s\n", dlerror()); exit(2); }
	BIND(&api, create2); BIND(&api, conf_read_file); BIND(&api, conf_set); BIND(&api, connect);
	BIND(&api, ioctx_create); BIND(&api, ioctx_destroy); BIND(&api, shutdown);
	BIND(&api, write_full); BIND(&api, append); BIND(&api, read); BIND(&api, get_last_version);
	BIND(&api, watch3); BIND(&api, unwatch2); BIND(&api, notify_ack); BIND(&api, watch_flush);
	BIND(&api, mon_command); BIND(&api, osd_command); BIND(&api, pg_command); BIND(&api, buffer_free);
	return api;
}

static void write_control(const char *control, const char *suffix, uint64_t value) {
	char path[1024];
	if (snprintf(path, sizeof(path), "%s-%s", control, suffix) >= (int)sizeof(path)) return;
	FILE *file = fopen(path, "w");
	if (file == NULL) return;
	fprintf(file, "%" PRIu64 "\n", value);
	fclose(file);
}

static void watch_callback(void *argument, uint64_t notify_id, uint64_t handle, uint64_t notifier_id, void *data, size_t data_length) {
	(void)notifier_id; (void)data; (void)data_length;
	struct watch_context *context = argument;
	int result = context->api->notify_ack(context->io, context->object, notify_id, handle, NULL, 0);
	if (result < 0) atomic_store(&context->callback_error, result);
	unsigned int events = atomic_fetch_add(&context->events, 1) + 1;
	write_control(context->control, "event", events);
}

static void watch_error_callback(void *argument, uint64_t cookie, int error) {
	(void)cookie; (void)error;
	struct watch_context *context = argument;
	atomic_fetch_add(&context->interruptions, 1);
}

static int wait_for_operation_release(void) {
	const char *ready = getenv("P13_READY_FILE"), *release = getenv("P13_RELEASE_FILE");
	if (ready == NULL || ready[0] == '\0') return 0;
	if (release == NULL || release[0] == '\0') return -EINVAL;
	FILE *file = fopen(ready, "w");
	if (file == NULL) return -errno;
	fputs("ready\n", file);
	if (fclose(file) != 0) return -errno;
	struct timespec pause = {.tv_sec = 0, .tv_nsec = 100000000};
	while (access(release, F_OK) != 0) nanosleep(&pause, NULL);
	return 0;
}

static int64_t nanoseconds(struct timespec value) {
	return (int64_t)value.tv_sec * 1000000000LL + value.tv_nsec;
}

static void timestamp(struct timespec value, char output[31]) {
	struct tm utc;
	if (gmtime_r(&value.tv_sec, &utc) == NULL) { perror("native p13: gmtime_r"); exit(2); }
	if (strftime(output, 20, "%Y-%m-%dT%H:%M:%S", &utc) == 0) { fprintf(stderr, "native p13: strftime failed\n"); exit(2); }
	snprintf(output + 19, 12, ".%09ldZ", value.tv_nsec);
}

static void json_string(const char *value, size_t length) {
	putchar('"');
	for (size_t index = 0; index < length; ++index) {
		unsigned char byte = (unsigned char)value[index];
		if (byte == '"' || byte == '\\') printf("\\%c", byte);
		else if (byte >= 0x20 && byte < 0x7f) putchar(byte);
		else printf("\\u%04x", byte);
	}
	putchar('"');
}

int main(int argc, char **argv) {
	if (argc < 6 || argc > 7) {
		fprintf(stderr, "usage: %s read|write|append|watch|monitor-command CONF KEYRING POOL OBJECT [PAYLOAD_OR_CONTROL]\n", argv[0]);
		return 2;
	}
	const char *operation = argv[1], *payload = argc == 7 ? argv[6] : "";
	struct api api = load_api(); rados_t cluster = NULL; rados_ioctx_t io = NULL;
	int result = api.create2(&cluster, "ceph", "client.p13", 0);
	if (result >= 0) result = api.conf_read_file(cluster, argv[2]);
	if (result >= 0) result = api.conf_set(cluster, "keyring", argv[3]);
	const char *transport = getenv("P13_TRANSPORT");
	if (transport == NULL || transport[0] == '\0') transport = "secure";
	if (result >= 0) result = api.conf_set(cluster, "ms_client_mode", transport);
	if (result >= 0) result = api.connect(cluster);
	if (result >= 0) result = api.ioctx_create(cluster, argv[4], &io);
	if (result >= 0) result = wait_for_operation_release();
	if (result < 0) { fprintf(stderr, "native p13: connect: %s (%d)\n", strerror(-result), result); return 1; }
	char data[1 << 20]; ssize_t length = 0;
	uint64_t watch_cookie = 0;
	struct watch_context watch = {.api = &api, .io = io, .object = argv[5], .control = payload};
	struct timespec started, finished, wall_started, wall_finished;
	clock_gettime(CLOCK_REALTIME, &wall_started); clock_gettime(CLOCK_MONOTONIC, &started);
	if (strcmp(operation, "read") == 0) {
		length = api.read(io, argv[5], data, sizeof(data), 0); result = length < 0 ? (int)length : 0;
	} else if (strcmp(operation, "write") == 0) {
		result = api.write_full(io, argv[5], payload, strlen(payload));
	} else if (strcmp(operation, "append") == 0) {
		result = api.append(io, argv[5], payload, strlen(payload));
	} else if (strcmp(operation, "watch") == 0 && argc == 7) {
		result = api.watch3(io, argv[5], &watch_cookie, watch_callback, watch_error_callback, 60, &watch);
		if (result == 0) {
			write_control(payload, "ready", watch_cookie);
			char release[1024];
			if (snprintf(release, sizeof(release), "%s-release", payload) >= (int)sizeof(release)) result = -ENAMETOOLONG;
			struct timespec pause = {.tv_sec = 0, .tv_nsec = 100000000};
			while (result == 0 && access(release, F_OK) != 0) nanosleep(&pause, NULL);
			int unwatch_result = api.unwatch2(io, watch_cookie);
			int flush_result = api.watch_flush(cluster);
			if (atomic_load(&watch.callback_error) < 0) result = atomic_load(&watch.callback_error);
			else if (unwatch_result < 0) result = unwatch_result;
			else if (flush_result < 0) result = flush_result;
		}
	} else if ((strcmp(operation, "monitor-command") == 0 || strcmp(operation, "osd-command") == 0 || strcmp(operation, "pg-command") == 0) && argc == 7) {
		char *output = NULL, *status = NULL;
		size_t output_length = 0, status_length = 0;
		const char *commands[] = {payload};
		if (strcmp(operation, "monitor-command") == 0) result = api.mon_command(cluster, commands, 1, NULL, 0, &output, &output_length, &status, &status_length);
		else if (strcmp(operation, "osd-command") == 0) result = api.osd_command(cluster, atoi(argv[5]), commands, 1, NULL, 0, &output, &output_length, &status, &status_length);
		else result = api.pg_command(cluster, argv[5], commands, 1, NULL, 0, &output, &output_length, &status, &status_length);
		if (output_length > sizeof(data)) result = -EOVERFLOW;
		else if (output_length > 0) memcpy(data, output, output_length);
		length = (ssize_t)output_length;
		if (output != NULL) api.buffer_free(output);
		if (status != NULL) api.buffer_free(status);
	} else {
		fprintf(stderr, "native p13: invalid operation\n"); return 2;
	}
	clock_gettime(CLOCK_MONOTONIC, &finished); clock_gettime(CLOCK_REALTIME, &wall_finished);
	char started_at[31], finished_at[31]; timestamp(wall_started, started_at); timestamp(wall_finished, finished_at);
	printf("{\"implementation\":\"native\",\"transport\":\"%s\",\"operation\":\"%s\",\"object\":", transport, operation);
	json_string(argv[5], strlen(argv[5]));
	printf(",\"started_at\":\"%s\",\"finished_at\":\"%s\",\"elapsed_ns\":%" PRId64 ",\"completed\":%s,\"errno\":%d,\"outcome\":\"%s\",\"version\":%" PRIu64 ",\"data\":",
		started_at, finished_at,
		nanoseconds(finished) - nanoseconds(started), result == 0 ? "true" : "false", result < 0 ? -result : 0,
		result == 0 ? "success" : "error", api.get_last_version(io));
	json_string(data, result == 0 && (strcmp(operation, "read") == 0 || strcmp(operation, "monitor-command") == 0 || strcmp(operation, "osd-command") == 0 || strcmp(operation, "pg-command") == 0) ? (size_t)length : 0);
	if (strcmp(operation, "append") == 0) { printf(",\"mutation_marker\":"); json_string(payload, strlen(payload)); }
	if (strcmp(operation, "watch") == 0) printf(",\"watch_cookie\":%" PRIu64 ",\"watch_events\":%u,\"watch_interruptions\":%u", watch_cookie, atomic_load(&watch.events), atomic_load(&watch.interruptions));
	printf("}\n");
	api.ioctx_destroy(io); api.shutdown(cluster); dlclose(api.library);
	return result == 0 ? 0 : 1;
}