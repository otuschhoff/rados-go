#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <time.h>
#include <unistd.h>

typedef void *rados_t;
typedef void *rados_ioctx_t;

typedef void (*version_fn)(int *, int *, int *);
typedef int (*create2_fn)(rados_t *, const char *, const char *, uint64_t);
typedef int (*conf_read_file_fn)(rados_t, const char *);
typedef int (*conf_set_fn)(rados_t, const char *, const char *);
typedef int (*connect_fn)(rados_t);
typedef int (*ioctx_create_fn)(rados_t, const char *, rados_ioctx_t *);
typedef void (*ioctx_destroy_fn)(rados_ioctx_t);
typedef void (*shutdown_fn)(rados_t);
typedef int (*write_full_fn)(rados_ioctx_t, const char *, const char *, size_t);
typedef int (*read_fn)(rados_ioctx_t, const char *, char *, size_t, uint64_t);
typedef int (*remove_fn)(rados_ioctx_t, const char *);
typedef void (*ioctx_set_namespace_fn)(rados_ioctx_t, const char *);

struct api {
	void *library;
	version_fn version;
	create2_fn create2;
	conf_read_file_fn conf_read_file;
	conf_set_fn conf_set;
	connect_fn connect;
	ioctx_create_fn ioctx_create;
	ioctx_destroy_fn ioctx_destroy;
	shutdown_fn shutdown;
	write_full_fn write_full;
	read_fn read;
	remove_fn remove;
	ioctx_set_namespace_fn ioctx_set_namespace;
};

struct row_result {
	uint64_t size_bytes;
	int concurrency;
	const char *workload;
	uint64_t operations;
	uint64_t bytes;
	uint64_t elapsed_ns;
	double throughput_bytes_per_second;
	double iops;
	uint64_t p50_ns;
	uint64_t p95_ns;
	uint64_t p99_ns;
	uint64_t cpu_user_ns, cpu_system_ns, rss_before, rss_after, rss_cleanup;
};

enum workload_kind {
	WORKLOAD_READ = 0,
	WORKLOAD_WRITE = 1,
	WORKLOAD_MIXED = 2,
};

struct row_ctx {
	struct api *api;
	rados_ioctx_t ioctx;
	uint64_t size_bytes;
	int op_per_worker;
	enum workload_kind workload;
	uint64_t run_id;
	uint64_t *latencies;

	pthread_mutex_t mu;
	pthread_cond_t ready_cv;
	pthread_cond_t start_cv;
	pthread_cond_t done_cv;
	int ready_count;
	int done_count;
	int start_flag;
	int verification_flag;
	int cancel_flag;
	int error_flag;
	char error_message[256];
};

struct worker_arg {
	struct row_ctx *ctx;
	int worker_id;
};

static const uint64_t k_sizes[] = {4096ULL, 65536ULL, 1048576ULL, 4194304ULL};
static const int k_concurrencies[] = {1, 16, 64};
static const enum workload_kind k_workloads[] = {WORKLOAD_READ, WORKLOAD_WRITE, WORKLOAD_MIXED};

static struct {
	int operations;
	uint64_t size;
	int concurrency;
	int workload;
} matrix = {2, 0, 0, -1};

static int read_diagnostic(void) {
	const char *value = getenv("P07_READ_DIAGNOSTIC");
	return value != NULL && strcmp(value, "1") == 0;
}

static int matched_parity(void) {
	const char *value = getenv("P07_PARITY_NAMESPACE");
	return value != NULL && value[0] != '\0';
}

static uint64_t resident_bytes(void) {
	FILE *file = fopen("/proc/self/status", "r");
	if (file == NULL) return 0;
	char line[256];
	uint64_t result = 0;
	while (fgets(line, sizeof(line), file) != NULL) {
		uint64_t value;
		if (sscanf(line, "VmRSS: %" SCNu64 " kB", &value) == 1) { result = value * 1024; break; }
	}
	fclose(file);
	return result;
}

static int read_matrix_settings(void) {
	const char *names[] = {"P07_MATRIX_OPERATIONS_PER_WORKER", "P07_MATRIX_SIZE", "P07_MATRIX_CONCURRENCY", "P07_MATRIX_WORKLOAD"};
	for (size_t index = 0; index < sizeof(names) / sizeof(names[0]); index++) {
		const char *value = getenv(names[index]);
		if (value == NULL || value[0] == '\0') {
			continue;
		}
		if (read_diagnostic()) {
			fprintf(stderr, "native benchmark: matrix overrides require normal workload mode\n");
			return -1;
		}
		if (index == 3) {
			if (strcmp(value, "read") == 0) matrix.workload = WORKLOAD_READ;
			else if (strcmp(value, "write") == 0) matrix.workload = WORKLOAD_WRITE;
			else if (strcmp(value, "mixed") == 0) matrix.workload = WORKLOAD_MIXED;
			else return -1;
			continue;
		}
		for (const char *character = value; *character != '\0'; character++) {
			if (*character < '0' || *character > '9') return -1;
		}
		char *end = NULL;
		errno = 0;
		long parsed = strtol(value, &end, 10);
		if (errno != 0 || end == value || *end != '\0') return -1;
		if (index == 0) {
			if (parsed < 256 || parsed > 4096) return -1;
			matrix.operations = (int)parsed;
		} else if (index == 1) {
			if (strcmp(value, "4096") != 0 && strcmp(value, "65536") != 0 && strcmp(value, "1048576") != 0 && strcmp(value, "4194304") != 0) return -1;
			matrix.size = (uint64_t)parsed;
		} else {
			if (strcmp(value, "1") != 0 && strcmp(value, "16") != 0 && strcmp(value, "32") != 0 && strcmp(value, "64") != 0 && strcmp(value, "128") != 0 && strcmp(value, "256") != 0) return -1;
			matrix.concurrency = (int)parsed;
		}
	}
	return 0;
}

static int read_operations_per_worker(void) {
	const char *value = getenv("P07_OPERATIONS_PER_WORKER");
	if (value == NULL || value[0] == '\0') {
		return read_diagnostic() ? 256 : matrix.operations;
	}
	char *end = NULL;
	errno = 0;
	long count = strtol(value, &end, 10);
	if (!read_diagnostic() || errno != 0 || end == value || *end != '\0' || count < 256 || count > 4096) {
		fprintf(stderr, "native benchmark: operations per worker requires diagnostic mode and 256..4096\n");
		return -1;
	}
	return (int)count;
}

static int format_object_name(char *buffer, size_t buffer_size, uint64_t run_id, uint64_t size_bytes,
	enum workload_kind workload, int worker_id) {
	if (read_diagnostic()) {
		int result = snprintf(buffer, buffer_size, "p07-shared-read-%d", worker_id);
		return result >= 0 && (size_t)result < buffer_size ? 0 : -1;
	}
	if (matched_parity()) {
		const char *name = workload == WORKLOAD_READ ? "read" : workload == WORKLOAD_WRITE ? "write" : "mixed";
		int result = snprintf(buffer, buffer_size, "p07-parity-%" PRIu64 "-c%d-%s-w%d", size_bytes, matrix.concurrency, name, worker_id);
		return result >= 0 && (size_t)result < buffer_size ? 0 : -1;
	}
	int result = snprintf(buffer, buffer_size, "p07-native-%" PRIu64 "-%" PRIu64 "-%d-%d",
		run_id, size_bytes, (int)workload, worker_id);
	return result >= 0 && (size_t)result < buffer_size ? 0 : -1;
}

static void bind_symbol(void *library, const char *name, void *target, size_t size) {
	void *symbol = dlsym(library, name);
	if (symbol == NULL || size != sizeof(symbol)) {
		fprintf(stderr, "native benchmark: bind %s: %s\n", name, dlerror());
		exit(2);
	}
	memcpy(target, &symbol, size);
}

#define BIND(api_ptr, name) bind_symbol((api_ptr)->library, "rados_" #name, &(api_ptr)->name, sizeof((api_ptr)->name))

static struct api load_api(void) {
	struct api result;
	memset(&result, 0, sizeof(result));
	result.library = dlopen("librados.so.2", RTLD_NOW | RTLD_LOCAL);
	if (result.library == NULL) {
		fprintf(stderr, "native benchmark: load librados.so.2: %s\n", dlerror());
		exit(2);
	}
	BIND(&result, version);
	BIND(&result, create2);
	BIND(&result, conf_read_file);
	BIND(&result, conf_set);
	BIND(&result, connect);
	BIND(&result, ioctx_create);
	BIND(&result, ioctx_destroy);
	BIND(&result, shutdown);
	BIND(&result, write_full);
	BIND(&result, read);
	BIND(&result, remove);
	BIND(&result, ioctx_set_namespace);
	return result;
}

static int check_result(int result, const char *operation) {
	if (result < 0) {
		fprintf(stderr, "native benchmark: %s: %s (%d)\n", operation, strerror(-result), result);
		return -1;
	}
	return 0;
}

static uint64_t timespec_to_ns(struct timespec value) {
	return (uint64_t)value.tv_sec * 1000000000ULL + (uint64_t)value.tv_nsec;
}

static uint64_t timespec_diff_ns(struct timespec start, struct timespec end) {
	uint64_t start_ns = timespec_to_ns(start);
	uint64_t end_ns = timespec_to_ns(end);
	if (end_ns < start_ns) {
		return 0;
	}
	return end_ns - start_ns;
}

static uint64_t timeval_to_ns(struct timeval value) {
	return (uint64_t)value.tv_sec * 1000000000ULL + (uint64_t)value.tv_usec * 1000ULL;
}

static void set_worker_error(struct row_ctx *ctx, const char *message, int code) {
	pthread_mutex_lock(&ctx->mu);
	if (!ctx->error_flag) {
		ctx->error_flag = 1;
		ctx->cancel_flag = 1;
		if (code < 0) {
			snprintf(ctx->error_message, sizeof(ctx->error_message), "%s: %s (%d)", message, strerror(-code), code);
		} else {
			snprintf(ctx->error_message, sizeof(ctx->error_message), "%s", message);
		}
	}
	pthread_cond_broadcast(&ctx->start_cv);
	pthread_cond_broadcast(&ctx->ready_cv);
	pthread_cond_broadcast(&ctx->done_cv);
	pthread_mutex_unlock(&ctx->mu);
}

static int is_canceled(struct row_ctx *ctx) {
	int canceled;
	pthread_mutex_lock(&ctx->mu);
	canceled = ctx->cancel_flag;
	pthread_mutex_unlock(&ctx->mu);
	return canceled;
}

static void fill_payload(unsigned char *buffer, uint64_t size, uint64_t run_id, int worker_id) {
	uint64_t state = run_id ^ ((uint64_t)worker_id << 19) ^ 0x9E3779B97F4A7C15ULL;
	for (uint64_t i = 0; i < size; i++) {
		state ^= state << 7;
		state ^= state >> 9;
		state ^= state << 8;
		buffer[i] = (unsigned char)state;
	}
}

static void wait_for_start(struct row_ctx *ctx) {
	pthread_mutex_lock(&ctx->mu);
	ctx->ready_count++;
	pthread_cond_signal(&ctx->ready_cv);
	while (!ctx->start_flag && !ctx->cancel_flag) {
		pthread_cond_wait(&ctx->start_cv, &ctx->mu);
	}
	pthread_mutex_unlock(&ctx->mu);
}

static void mark_done(struct row_ctx *ctx) {
	pthread_mutex_lock(&ctx->mu);
	ctx->done_count++;
	if (matched_parity()) pthread_cond_broadcast(&ctx->done_cv);
	else pthread_cond_signal(&ctx->done_cv);
	pthread_mutex_unlock(&ctx->mu);
}

static const char *workload_name(enum workload_kind workload) {
	switch (workload) {
	case WORKLOAD_READ:
		return "read";
	case WORKLOAD_WRITE:
		return "write";
	case WORKLOAD_MIXED:
		return "mixed";
	default:
		return "unknown";
	}
}

static void *worker_main(void *arg) {
	struct worker_arg *worker = (struct worker_arg *)arg;
	struct row_ctx *ctx = worker->ctx;
	struct api *api = ctx->api;
	const int op_per_worker = ctx->op_per_worker;
	const int latency_index = worker->worker_id * op_per_worker;
	char object_name[96];
	if (format_object_name(object_name, sizeof(object_name), ctx->run_id, ctx->size_bytes, ctx->workload,
		worker->worker_id) != 0) {
		set_worker_error(ctx, "object name overflow", 0);
		mark_done(ctx);
		return NULL;
	}

	unsigned char *payload = (unsigned char *)malloc((size_t)ctx->size_bytes);
	unsigned char *read_buffer = (unsigned char *)malloc((size_t)ctx->size_bytes);
	if (payload == NULL || read_buffer == NULL) {
		free(payload);
		free(read_buffer);
		set_worker_error(ctx, "allocation failure", 0);
		mark_done(ctx);
		return NULL;
	}
	fill_payload(payload, ctx->size_bytes, ctx->run_id, worker->worker_id);
	if (read_diagnostic() || matrix.operations > 2) {
		for (int warmup = 0; warmup < 8; warmup++) {
			if (ctx->workload == WORKLOAD_WRITE || (ctx->workload == WORKLOAD_MIXED && warmup % 2 == 1)) {
				int write_result = api->write_full(ctx->ioctx, object_name, (const char *)payload, (size_t)ctx->size_bytes);
				if (write_result < 0) {
					set_worker_error(ctx, "warmup write failed", write_result);
					break;
				}
				continue;
			}
			int read_result = api->read(ctx->ioctx, object_name, (char *)read_buffer, (size_t)ctx->size_bytes, 0);
			if (read_result != (int)ctx->size_bytes || memcmp(read_buffer, payload, (size_t)ctx->size_bytes) != 0) {
				set_worker_error(ctx, "warmup read failed", read_result);
				break;
			}
		}
	}

	wait_for_start(ctx);

	if (!is_canceled(ctx)) {
		if (ctx->workload == WORKLOAD_READ) {
			for (int i = 0; i < op_per_worker; i++) {
				struct timespec begin;
				struct timespec end;
				if (clock_gettime(CLOCK_MONOTONIC, &begin) != 0) {
					set_worker_error(ctx, "clock_gettime failed", 0);
					break;
				}
				int read_result = api->read(ctx->ioctx, object_name, (char *)read_buffer, (size_t)ctx->size_bytes, 0);
				if (clock_gettime(CLOCK_MONOTONIC, &end) != 0) {
					set_worker_error(ctx, "clock_gettime failed", 0);
					break;
				}
				if (read_result < 0) {
					set_worker_error(ctx, "read failed", read_result);
					break;
				}
				if ((uint64_t)read_result != ctx->size_bytes) {
					set_worker_error(ctx, "short read", 0);
					break;
				}
				if (memcmp(read_buffer, payload, (size_t)ctx->size_bytes) != 0) {
					set_worker_error(ctx, "read payload mismatch", 0);
					break;
				}
				ctx->latencies[latency_index + i] = timespec_diff_ns(begin, end);
			}
		} else if (ctx->workload == WORKLOAD_WRITE) {
			for (int i = 0; i < op_per_worker; i++) {
				struct timespec begin;
				struct timespec end;
				if (clock_gettime(CLOCK_MONOTONIC, &begin) != 0) {
					set_worker_error(ctx, "clock_gettime failed", 0);
					break;
				}
				int write_result = api->write_full(ctx->ioctx, object_name, (const char *)payload, (size_t)ctx->size_bytes);
				if (clock_gettime(CLOCK_MONOTONIC, &end) != 0) {
					set_worker_error(ctx, "clock_gettime failed", 0);
					break;
				}
				if (write_result < 0) {
					set_worker_error(ctx, "write_full failed", write_result);
					break;
				}
				ctx->latencies[latency_index + i] = timespec_diff_ns(begin, end);
			}
		} else if (ctx->workload == WORKLOAD_MIXED) {
			for (int operation = 0; operation < op_per_worker && !is_canceled(ctx); operation++) {
				struct timespec begin;
				struct timespec end;
				if (clock_gettime(CLOCK_MONOTONIC, &begin) != 0) {
					set_worker_error(ctx, "clock_gettime failed", 0);
					break;
				}
				int result = operation % 2 == 0
					? api->read(ctx->ioctx, object_name, (char *)read_buffer, (size_t)ctx->size_bytes, 0)
					: api->write_full(ctx->ioctx, object_name, (const char *)payload, (size_t)ctx->size_bytes);
				if (clock_gettime(CLOCK_MONOTONIC, &end) != 0) {
					set_worker_error(ctx, "clock_gettime failed", 0);
					break;
				}
				if (result < 0 || (operation % 2 == 0 && (result != (int)ctx->size_bytes || memcmp(read_buffer, payload, (size_t)ctx->size_bytes) != 0))) {
					set_worker_error(ctx, "mixed operation or payload validation failed", result);
					break;
				}
				ctx->latencies[latency_index + operation] = timespec_diff_ns(begin, end);
			}
		} else {
			set_worker_error(ctx, "invalid workload", 0);
		}
	}

	mark_done(ctx);
	if (matched_parity()) {
		pthread_mutex_lock(&ctx->mu);
		while (!ctx->verification_flag) pthread_cond_wait(&ctx->done_cv, &ctx->mu);
		pthread_mutex_unlock(&ctx->mu);
		int verified = api->read(ctx->ioctx, object_name, (char *)read_buffer, (size_t)ctx->size_bytes, 0);
		if (verified != (int)ctx->size_bytes || memcmp(read_buffer, payload, (size_t)ctx->size_bytes) != 0)
			set_worker_error(ctx, "final parity payload mismatch", verified);
	}
	int remove_result = read_diagnostic() && !matched_parity() ? 0 : api->remove(ctx->ioctx, object_name);
	if (remove_result < 0 && remove_result != -ENOENT) {
		set_worker_error(ctx, "cleanup remove failed", remove_result);
	}
	if (matched_parity() && (remove_result != 0 || api->read(ctx->ioctx, object_name, (char *)read_buffer, 1, 0) != -ENOENT))
		set_worker_error(ctx, "parity fixture cleanup not verified", remove_result);

	free(payload);
	free(read_buffer);
	return NULL;
}

static int compare_u64(const void *left, const void *right) {
	const uint64_t a = *(const uint64_t *)left;
	const uint64_t b = *(const uint64_t *)right;
	if (a < b) {
		return -1;
	}
	if (a > b) {
		return 1;
	}
	return 0;
}

static uint64_t percentile(const uint64_t *sorted, size_t count, uint64_t numerator, uint64_t denominator) {
	if (count == 0 || denominator == 0) {
		return 0;
	}
	uint64_t rank = (numerator * (uint64_t)count + denominator - 1ULL) / denominator;
	if (rank == 0) {
		rank = 1;
	}
	if (rank > (uint64_t)count) {
		rank = (uint64_t)count;
	}
	return sorted[rank - 1ULL];
}

static int checked_mul_u64(uint64_t left, uint64_t right, uint64_t *out) {
	if (right != 0 && left > UINT64_MAX / right) {
		return -1;
	}
	*out = left * right;
	return 0;
}

static int run_row(struct api *api, rados_ioctx_t ioctx, uint64_t run_id, uint64_t size_bytes, int concurrency,
	enum workload_kind workload, struct row_result *result) {
	const int op_per_worker = read_operations_per_worker();
	if (op_per_worker < 0) {
		return -1;
	}
	uint64_t operations = 0;
	uint64_t bytes = 0;
	if (checked_mul_u64((uint64_t)concurrency, (uint64_t)op_per_worker, &operations) != 0 ||
		checked_mul_u64(operations, size_bytes, &bytes) != 0) {
		fprintf(stderr, "native benchmark: size/accounting overflow\n");
		return -1;
	}
	uint64_t *latencies = (uint64_t *)calloc((size_t)operations, sizeof(uint64_t));
	pthread_t *threads = (pthread_t *)calloc((size_t)concurrency, sizeof(pthread_t));
	struct worker_arg *worker_args = (struct worker_arg *)calloc((size_t)concurrency, sizeof(struct worker_arg));
	if (latencies == NULL || threads == NULL || worker_args == NULL) {
		free(latencies);
		free(threads);
		free(worker_args);
		fprintf(stderr, "native benchmark: allocation failure\n");
		return -1;
	}
	if ((workload == WORKLOAD_READ || workload == WORKLOAD_MIXED) && !read_diagnostic()) {
		for (int worker_id = 0; worker_id < concurrency; worker_id++) {
			char object_name[96];
			unsigned char *payload = (unsigned char *)malloc((size_t)size_bytes);
			if (payload == NULL || format_object_name(object_name, sizeof(object_name), run_id, size_bytes, workload,
				worker_id) != 0) {
				free(payload);
				fprintf(stderr, "native benchmark: seed setup failed\n");
				free(latencies);
				free(threads);
				free(worker_args);
				return -1;
			}
			fill_payload(payload, size_bytes, run_id, worker_id);
			int seed_result = api->write_full(ioctx, object_name, (const char *)payload, (size_t)size_bytes);
			free(payload);
			if (seed_result < 0) {
				fprintf(stderr, "native benchmark: seed write_full failed: %s (%d)\n", strerror(-seed_result), seed_result);
				free(latencies);
				free(threads);
				free(worker_args);
				return -1;
			}
		}
	}

	struct row_ctx ctx;
	memset(&ctx, 0, sizeof(ctx));
	ctx.api = api;
	ctx.ioctx = ioctx;
	ctx.size_bytes = size_bytes;
	ctx.op_per_worker = op_per_worker;
	ctx.workload = workload;
	ctx.run_id = run_id;
	ctx.latencies = latencies;
	if (pthread_mutex_init(&ctx.mu, NULL) != 0 ||
		pthread_cond_init(&ctx.ready_cv, NULL) != 0 ||
		pthread_cond_init(&ctx.start_cv, NULL) != 0 ||
		pthread_cond_init(&ctx.done_cv, NULL) != 0) {
		fprintf(stderr, "native benchmark: synchronization init failed\n");
		free(latencies);
		free(threads);
		free(worker_args);
		return -1;
	}

	int created = 0;
	for (int i = 0; i < concurrency; i++) {
		worker_args[i].ctx = &ctx;
		worker_args[i].worker_id = i;
		if (pthread_create(&threads[i], NULL, worker_main, &worker_args[i]) != 0) {
			set_worker_error(&ctx, "pthread_create failed", 0);
			break;
		}
		created++;
	}

	pthread_mutex_lock(&ctx.mu);
	while (!ctx.error_flag && ctx.ready_count < created) {
		pthread_cond_wait(&ctx.ready_cv, &ctx.mu);
	}
	struct timespec start_mono;
	struct timespec end_mono;
	struct rusage measured_before, measured_after;
	memset(&measured_before, 0, sizeof(measured_before));
	memset(&measured_after, 0, sizeof(measured_after));
	if (matched_parity()) {
		result->rss_before = resident_bytes();
		if (result->rss_before == 0 || getrusage(RUSAGE_SELF, &measured_before) != 0) ctx.error_flag = ctx.cancel_flag = 1;
	}
	memset(&start_mono, 0, sizeof(start_mono));
	memset(&end_mono, 0, sizeof(end_mono));
	if (!ctx.error_flag) {
		if (clock_gettime(CLOCK_MONOTONIC, &start_mono) != 0) {
			ctx.error_flag = 1;
			ctx.cancel_flag = 1;
			snprintf(ctx.error_message, sizeof(ctx.error_message), "clock_gettime failed");
		}
	}
	ctx.start_flag = 1;
	pthread_cond_broadcast(&ctx.start_cv);
	while (ctx.done_count < created) {
		pthread_cond_wait(&ctx.done_cv, &ctx.mu);
	}
	if (clock_gettime(CLOCK_MONOTONIC, &end_mono) != 0) {
		ctx.error_flag = 1;
		if (ctx.error_message[0] == '\0') {
			snprintf(ctx.error_message, sizeof(ctx.error_message), "clock_gettime failed");
		}
	}
	if (matched_parity()) {
		if (getrusage(RUSAGE_SELF, &measured_after) != 0) ctx.error_flag = 1;
		result->rss_after = resident_bytes();
		if (result->rss_after == 0) ctx.error_flag = 1;
		result->cpu_user_ns = timeval_to_ns(measured_after.ru_utime) - timeval_to_ns(measured_before.ru_utime);
		result->cpu_system_ns = timeval_to_ns(measured_after.ru_stime) - timeval_to_ns(measured_before.ru_stime);
	}
	ctx.verification_flag = 1;
	pthread_cond_broadcast(&ctx.done_cv);
	pthread_mutex_unlock(&ctx.mu);

	for (int i = 0; i < created; i++) {
		pthread_join(threads[i], NULL);
	}
	if (matched_parity()) { result->rss_cleanup = resident_bytes(); if (result->rss_cleanup == 0) ctx.error_flag = 1; }

	if (ctx.error_flag) {
		if (ctx.error_message[0] != '\0') {
			fprintf(stderr, "native benchmark: %s\n", ctx.error_message);
		} else {
			fprintf(stderr, "native benchmark: worker error\n");
		}
		pthread_cond_destroy(&ctx.done_cv);
		pthread_cond_destroy(&ctx.start_cv);
		pthread_cond_destroy(&ctx.ready_cv);
		pthread_mutex_destroy(&ctx.mu);
		free(latencies);
		free(threads);
		free(worker_args);
		return -1;
	}

	qsort(latencies, (size_t)operations, sizeof(uint64_t), compare_u64);
	uint64_t elapsed_ns = timespec_diff_ns(start_mono, end_mono);
	double seconds = elapsed_ns > 0 ? (double)elapsed_ns / 1000000000.0 : 0.0;
	double throughput = seconds > 0 ? (double)bytes / seconds : 0.0;
	double iops = seconds > 0 ? (double)operations / seconds : 0.0;

	result->size_bytes = size_bytes;
	result->concurrency = concurrency;
	result->workload = workload_name(workload);
	result->operations = operations;
	result->bytes = bytes;
	result->elapsed_ns = elapsed_ns;
	result->throughput_bytes_per_second = throughput;
	result->iops = iops;
	result->p50_ns = percentile(latencies, (size_t)operations, 50, 100);
	result->p95_ns = percentile(latencies, (size_t)operations, 95, 100);
	result->p99_ns = percentile(latencies, (size_t)operations, 99, 100);

	pthread_cond_destroy(&ctx.done_cv);
	pthread_cond_destroy(&ctx.start_cv);
	pthread_cond_destroy(&ctx.ready_cv);
	pthread_mutex_destroy(&ctx.mu);
	free(latencies);
	free(threads);
	free(worker_args);
	return 0;
}

int main(int argc, char **argv) {
	if (argc != 5 && argc != 6) {
		fprintf(stderr, "usage: %s CONF KEYRING POOL TRANSPORT [ENTITY]\n", argv[0]);
		return 2;
	}
	const char *conf = argv[1];
	const char *keyring = argv[2];
	const char *pool_name = argv[3];
	const char *transport = argv[4];
	const char *entity = argc == 6 ? argv[5] : "client.p07";
	if (strcmp(transport, "secure") != 0 && strcmp(transport, "crc") != 0) {
		fprintf(stderr, "native benchmark: TRANSPORT must be secure or crc\n");
		return 2;
	}
	if (read_matrix_settings() != 0 || read_operations_per_worker() < 0) {
		return 2;
	}
	if (matched_parity()) {
		const char *name = getenv("P07_PARITY_NAMESPACE");
		if (strlen(name) > 64 || strncmp(name, "p07-parity-", 11) != 0) return 2;
		for (const char *character = name; *character; character++)
			if (*character != '-' && (*character < 'a' || *character > 'z') && (*character < '0' || *character > '9')) return 2;
		if (!read_diagnostic() && (matrix.size == 0 || matrix.concurrency == 0 || matrix.workload < 0 || matrix.operations <= 2)) return 2;
	}

	struct api api = load_api();
	rados_t cluster = NULL;
	rados_ioctx_t ioctx = NULL;

	if (check_result(api.create2(&cluster, "ceph", entity, 0), "create cluster") != 0) {
		dlclose(api.library);
		return 1;
	}
	if (check_result(api.conf_read_file(cluster, conf), "read config") != 0) {
		api.shutdown(cluster);
		dlclose(api.library);
		return 1;
	}
	if (check_result(api.conf_set(cluster, "keyring", keyring), "set keyring") != 0) {
		api.shutdown(cluster);
		dlclose(api.library);
		return 1;
	}
	if (check_result(api.conf_set(cluster, "ms_client_mode", transport), "set ms_client_mode") != 0) {
		api.shutdown(cluster);
		dlclose(api.library);
		return 1;
	}
	const char *mode_log = getenv("P07_NATIVE_MODE_LOG");
	if (mode_log != NULL && mode_log[0] != '\0') {
		int log_fd = open(mode_log, O_WRONLY | O_CREAT | O_EXCL, 0600);
		if (log_fd < 0) {
			fprintf(stderr, "native benchmark: mode log must be a fresh writable path\n");
			api.shutdown(cluster);
			dlclose(api.library);
			return 1;
		}
		close(log_fd);
		if (check_result(api.conf_set(cluster, "log_file", mode_log), "set mode log") != 0 ||
		    check_result(api.conf_set(cluster, "debug_ms", "1/1"), "set mode debug") != 0 ||
		    check_result(api.conf_set(cluster, "debug_auth", "0/0"), "disable auth debug") != 0 ||
		    check_result(api.conf_set(cluster, "log_to_file", "true"), "enable mode log") != 0) {
			api.shutdown(cluster);
			dlclose(api.library);
			return 1;
		}
	}
	if (check_result(api.connect(cluster), "connect") != 0) {
		api.shutdown(cluster);
		dlclose(api.library);
		return 1;
	}
	if (check_result(api.ioctx_create(cluster, pool_name, &ioctx), "open pool") != 0) {
		api.shutdown(cluster);
		dlclose(api.library);
		return 1;
	}
	if (matched_parity()) api.ioctx_set_namespace(ioctx, getenv("P07_PARITY_NAMESPACE"));

	struct rusage before_usage;
	struct rusage after_usage;
	if (getrusage(RUSAGE_SELF, &before_usage) != 0) {
		fprintf(stderr, "native benchmark: getrusage before failed\n");
		api.ioctx_destroy(ioctx);
		api.shutdown(cluster);
		dlclose(api.library);
		return 1;
	}
	struct timespec before_cpu;
	struct timespec after_cpu;
	if (clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &before_cpu) != 0) {
		fprintf(stderr, "native benchmark: process cpu clock before failed\n");
		api.ioctx_destroy(ioctx);
		api.shutdown(cluster);
		dlclose(api.library);
		return 1;
	}

	const size_t total_rows = read_diagnostic() ? 1 : (sizeof(k_sizes) / sizeof(k_sizes[0])) *
		(sizeof(k_concurrencies) / sizeof(k_concurrencies[0])) *
		(sizeof(k_workloads) / sizeof(k_workloads[0]));
	struct row_result *rows = (struct row_result *)calloc(total_rows, sizeof(struct row_result));
	if (rows == NULL) {
		fprintf(stderr, "native benchmark: rows allocation failed\n");
		api.ioctx_destroy(ioctx);
		api.shutdown(cluster);
		dlclose(api.library);
		return 1;
	}

	size_t row_index = 0;
	uint64_t run_id = (uint64_t)time(NULL);
	if (read_diagnostic() || matched_parity()) {
		run_id = 1;
	}
	for (size_t i = 0; i < sizeof(k_sizes) / sizeof(k_sizes[0]); i++) {
		if (matrix.size != 0 && matrix.size != k_sizes[i]) continue;
		for (size_t j = 0; j < sizeof(k_concurrencies) / sizeof(k_concurrencies[0]); j++) {
			if (matrix.concurrency != 0 && j != 0) continue;
			int concurrency = matrix.concurrency != 0 ? matrix.concurrency : k_concurrencies[j];
			for (size_t k = 0; k < sizeof(k_workloads) / sizeof(k_workloads[0]); k++) {
				if (matrix.workload != -1 && matrix.workload != (int)k_workloads[k]) continue;
				if (total_rows == 1 && (k_sizes[i] != 65536 || k_concurrencies[j] != 16 || k_workloads[k] != WORKLOAD_READ)) {
					continue;
				}
				if (run_row(&api, ioctx, run_id, k_sizes[i], concurrency, k_workloads[k], &rows[row_index]) != 0) {
					free(rows);
					api.ioctx_destroy(ioctx);
					api.shutdown(cluster);
					dlclose(api.library);
					return 1;
				}
				run_id++;
				row_index++;
			}
		}
	}

	if (clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &after_cpu) != 0) {
		fprintf(stderr, "native benchmark: process cpu clock after failed\n");
		free(rows);
		api.ioctx_destroy(ioctx);
		api.shutdown(cluster);
		dlclose(api.library);
		return 1;
	}
	if (getrusage(RUSAGE_SELF, &after_usage) != 0) {
		fprintf(stderr, "native benchmark: getrusage after failed\n");
		free(rows);
		api.ioctx_destroy(ioctx);
		api.shutdown(cluster);
		dlclose(api.library);
		return 1;
	}

	uint64_t cpu_user_before = timeval_to_ns(before_usage.ru_utime);
	uint64_t cpu_user_after = timeval_to_ns(after_usage.ru_utime);
	uint64_t cpu_system_before = timeval_to_ns(before_usage.ru_stime);
	uint64_t cpu_system_after = timeval_to_ns(after_usage.ru_stime);
	uint64_t cpu_user_ns = cpu_user_after >= cpu_user_before ? cpu_user_after - cpu_user_before : 0;
	uint64_t cpu_system_ns = cpu_system_after >= cpu_system_before ? cpu_system_after - cpu_system_before : 0;
	uint64_t process_cpu_ns = timespec_diff_ns(before_cpu, after_cpu);
	if (process_cpu_ns < cpu_user_ns + cpu_system_ns) {
		process_cpu_ns = cpu_user_ns + cpu_system_ns;
	}
	(void)process_cpu_ns;

	uint64_t max_rss_bytes = (uint64_t)after_usage.ru_maxrss * 1024ULL;

	printf("{");
	printf("\"implementation\":\"native\",");
	printf("\"transport\":\"%s\",", transport);
	if (read_diagnostic()) {
		printf("\"diagnostic\":{\"warmup_operations\":128,\"operations_per_worker\":%d,\"resource_scope\":\"warmup_and_measured_reads\",\"object_set\":\"p07-shared-read-0..15\"},", read_operations_per_worker());
	}
	int version_major, version_minor, version_patch;
	api.version(&version_major, &version_minor, &version_patch);
	printf("\"environment\":{\"library\":\"librados.so.2\",\"library_api_version\":\"%d.%d.%d\"},", version_major, version_minor, version_patch);
	printf("\"resources\":{");
	printf("\"cpu_user_ns\":%" PRIu64 ",", cpu_user_ns);
	printf("\"cpu_system_ns\":%" PRIu64 ",", cpu_system_ns);
	printf("\"allocations\":null,");
	printf("\"allocated_bytes\":null,");
	printf("\"max_rss_bytes\":%" PRIu64, max_rss_bytes);
	printf("},");
	printf("\"rows\":[");
	for (size_t i = 0; i < row_index; i++) {
		if (i > 0) {
			printf(",");
		}
		printf("{");
		printf("\"size_bytes\":%" PRIu64 ",", rows[i].size_bytes);
		printf("\"concurrency\":%d,", rows[i].concurrency);
		printf("\"workload\":\"%s\",", rows[i].workload);
		printf("\"operations\":%" PRIu64 ",", rows[i].operations);
		printf("\"bytes\":%" PRIu64 ",", rows[i].bytes);
		printf("\"elapsed_ns\":%" PRIu64 ",", rows[i].elapsed_ns);
		printf("\"throughput_bytes_per_second\":%.6f,", rows[i].throughput_bytes_per_second);
		printf("\"iops\":%.6f,", rows[i].iops);
		printf("\"p50_ns\":%" PRIu64 ",", rows[i].p50_ns);
		printf("\"p95_ns\":%" PRIu64 ",", rows[i].p95_ns);
		printf("\"p99_ns\":%" PRIu64, rows[i].p99_ns);
		if (matched_parity()) {
			printf(",\"parity\":{\"measured_resources\":{\"cpu_user_ns\":%" PRIu64 ",\"cpu_system_ns\":%" PRIu64 "},\"rss_before_bytes\":%" PRIu64 ",\"rss_after_bytes\":%" PRIu64 ",\"rss_after_cleanup_bytes\":%" PRIu64 ",\"payload_verified\":true,\"cleanup_verified\":true}", rows[i].cpu_user_ns, rows[i].cpu_system_ns, rows[i].rss_before, rows[i].rss_after, rows[i].rss_cleanup);
		}
		printf("}");
	}
	printf("]}\n");

	free(rows);
	api.ioctx_destroy(ioctx);
	api.shutdown(cluster);
	dlclose(api.library);
	return 0;
}
