#define main p07_qualification_closed_loop_main
#include "native_benchmark.c"
#undef main

#define QUALIFICATION_RECORD_LIMIT 1000000ULL

struct qualification_record {
	uint64_t start_ns, end_ns, deadline_ns;
	int code;
};

struct qualification_worker_records {
	struct qualification_record *records;
	size_t count, capacity;
	uint64_t successful;
};

struct qualification_capture {
	struct qualification_worker_records *workers;
	int concurrency, error;
	uint64_t elapsed_ns, successful, failures, censored;
	uint64_t start_ns;
};

struct qualification_phase {
	struct qualification_capture *capture;
	uint64_t minimum_operations, minimum_ns, maximum_operations, deadline_ns, start_ns;
	uint64_t (*now)(void *);
	int (*operation)(void *, int, uint64_t);
	void *argument;
	pthread_mutex_t mutex;
	pthread_cond_t barrier;
	int ready, started, stop;
};

struct qualification_worker {
	struct qualification_phase *phase;
	int worker;
};

int qualification_complete(uint64_t successful, uint64_t elapsed_ns, uint64_t minimum_operations, uint64_t minimum_ns) {
	return successful >= minimum_operations && elapsed_ns >= minimum_ns;
}

static void qualification_stop(struct qualification_phase *phase, int error) {
	pthread_mutex_lock(&phase->mutex);
	if (!phase->capture->error) phase->capture->error = error;
	phase->stop = 1;
	pthread_mutex_unlock(&phase->mutex);
}

static void *qualification_worker_main(void *argument) {
	struct qualification_worker *worker = argument;
	struct qualification_phase *phase = worker->phase;
	struct qualification_worker_records *records = &phase->capture->workers[worker->worker];
	pthread_mutex_lock(&phase->mutex);
	phase->ready++;
	pthread_cond_broadcast(&phase->barrier);
	while (!phase->started) pthread_cond_wait(&phase->barrier, &phase->mutex);
	pthread_mutex_unlock(&phase->mutex);
	for (;;) {
		uint64_t started = phase->now(phase->argument);
		if (started < phase->start_ns) { qualification_stop(phase, -EIO); break; }
		if (qualification_complete(records->successful, started - phase->start_ns, phase->minimum_operations, phase->minimum_ns)) break;
		pthread_mutex_lock(&phase->mutex);
		int stopped = phase->stop;
		pthread_mutex_unlock(&phase->mutex);
		if (stopped) break;
		if (started >= phase->deadline_ns) { qualification_stop(phase, -ETIMEDOUT); break; }
		if (records->count >= phase->maximum_operations) { qualification_stop(phase, -EOVERFLOW); break; }
		if (records->count == records->capacity) {
			size_t capacity = records->capacity ? records->capacity * 2 : 64;
			if (capacity > phase->maximum_operations) capacity = (size_t)phase->maximum_operations;
			struct qualification_record *resized = realloc(records->records, capacity * sizeof(*resized));
			if (!resized) { qualification_stop(phase, -ENOMEM); break; }
			records->records = resized;
			records->capacity = capacity;
		}
		started = phase->now(phase->argument);
		if (started < phase->start_ns) { qualification_stop(phase, -EIO); break; }
		if (started >= phase->deadline_ns) { qualification_stop(phase, -ETIMEDOUT); break; }
		int code = phase->operation(phase->argument, worker->worker, (uint64_t)records->count);
		uint64_t ended = phase->now(phase->argument);
		uint64_t operation_deadline = started <= UINT64_MAX - 30000000000ULL ? started + 30000000000ULL : UINT64_MAX;
		if (operation_deadline > phase->deadline_ns) operation_deadline = phase->deadline_ns;
		if (ended <= started || ended < phase->start_ns) code = -EIO;
		if (ended > operation_deadline && !code) code = -ETIMEDOUT;
		records->records[records->count++] = (struct qualification_record){started - phase->start_ns, ended >= phase->start_ns ? ended - phase->start_ns : 0, operation_deadline - phase->start_ns, code};
		if (code) { qualification_stop(phase, code); break; }
		records->successful++;
	}
	return NULL;
}

void qualification_capture_destroy(struct qualification_capture *capture) {
	if (capture->workers) {
		for (int worker = 0; worker < capture->concurrency; worker++) free(capture->workers[worker].records);
		free(capture->workers);
	}
	memset(capture, 0, sizeof(*capture));
}

int collect_native_qualification_phase(int concurrency, uint64_t minimum_operations, uint64_t minimum_ns, uint64_t maximum_operations,
	uint64_t deadline_ns, uint64_t (*now)(void *), int (*operation)(void *, int, uint64_t), void *argument, struct qualification_capture *capture) {
	memset(capture, 0, sizeof(*capture));
	if (concurrency < 1 || concurrency > 256 || !minimum_operations || !minimum_ns || maximum_operations < minimum_operations ||
		maximum_operations > QUALIFICATION_RECORD_LIMIT / (uint64_t)concurrency || !deadline_ns || !now || !operation) return -EINVAL;
	capture->concurrency = concurrency;
	capture->workers = calloc((size_t)concurrency, sizeof(*capture->workers));
	pthread_t *threads = calloc((size_t)concurrency, sizeof(*threads));
	struct qualification_worker *workers = calloc((size_t)concurrency, sizeof(*workers));
	if (!capture->workers || !threads || !workers) {
		free(threads); free(workers); qualification_capture_destroy(capture); return -ENOMEM;
	}
	struct qualification_phase phase = {.capture = capture, .minimum_operations = minimum_operations, .minimum_ns = minimum_ns,
		.maximum_operations = maximum_operations, .deadline_ns = deadline_ns, .now = now, .operation = operation, .argument = argument};
	int error = pthread_mutex_init(&phase.mutex, NULL);
	if (error) { free(threads); free(workers); qualification_capture_destroy(capture); return -error; }
	error = pthread_cond_init(&phase.barrier, NULL);
	if (error) { pthread_mutex_destroy(&phase.mutex); free(threads); free(workers); qualification_capture_destroy(capture); return -error; }
	int created = 0;
	for (int worker = 0; worker < concurrency; worker++) {
		workers[worker] = (struct qualification_worker){&phase, worker};
		error = pthread_create(&threads[worker], NULL, qualification_worker_main, &workers[worker]);
		if (error) { qualification_stop(&phase, -error); break; }
		created++;
	}
	pthread_mutex_lock(&phase.mutex);
	while (phase.ready < created) pthread_cond_wait(&phase.barrier, &phase.mutex);
	phase.start_ns = now(argument);
	capture->start_ns = phase.start_ns;
	if (!phase.start_ns || phase.start_ns >= deadline_ns) {
		capture->error = -ETIMEDOUT;
		phase.stop = 1;
	}
	phase.started = 1;
	pthread_cond_broadcast(&phase.barrier);
	pthread_mutex_unlock(&phase.mutex);
	for (int worker = 0; worker < created; worker++) pthread_join(threads[worker], NULL);
	uint64_t ended = now(argument);
	capture->elapsed_ns = ended >= phase.start_ns ? ended - phase.start_ns : 0;
	if (ended < phase.start_ns && !capture->error) capture->error = -EIO;
	for (int worker = 0; worker < concurrency; worker++) {
		struct qualification_worker_records *records = &capture->workers[worker];
		capture->successful += records->successful;
		capture->failures += (uint64_t)records->count - records->successful;
		if (records->count) {
			int code = records->records[records->count - 1].code;
			capture->censored += code == -ETIMEDOUT || code == -ECANCELED;
		}
	}
	pthread_cond_destroy(&phase.barrier);
	pthread_mutex_destroy(&phase.mutex);
	free(threads); free(workers);
	return capture->error;
}

struct qualification_rss_sample {
	uint64_t at_ns, bytes;
};

struct qualification_rss_sampler {
	struct qualification_rss_sample *samples;
	size_t count, capacity;
	struct qualification_rss_sample baseline;
	uint64_t interval_ns;
	uint64_t (*now)(void *), (*read)(void *);
	void *argument;
	pthread_t thread;
	pthread_mutex_t mutex;
	pthread_cond_t stop_cv;
	int stop, error;
};

static int qualification_rss_observe(struct qualification_rss_sampler *sampler) {
	if (sampler->count >= 10000) return -EOVERFLOW;
	if (sampler->count == sampler->capacity) {
		size_t capacity = sampler->capacity ? sampler->capacity * 2 : 64;
		if (capacity > 10000) capacity = 10000;
		struct qualification_rss_sample *samples = realloc(sampler->samples, capacity * sizeof(*samples));
		if (!samples) return -ENOMEM;
		sampler->samples = samples;
		sampler->capacity = capacity;
	}
	uint64_t at = sampler->now(sampler->argument), bytes = sampler->read(sampler->argument);
	if (!at || !bytes || (sampler->count && at <= sampler->samples[sampler->count - 1].at_ns)) return -EIO;
	sampler->samples[sampler->count++] = (struct qualification_rss_sample){at, bytes};
	return 0;
}

static void *qualification_rss_worker(void *argument) {
	struct qualification_rss_sampler *sampler = argument;
	pthread_mutex_lock(&sampler->mutex);
	while (!sampler->stop && !sampler->error) {
		struct timespec wake;
		if (clock_gettime(CLOCK_MONOTONIC, &wake)) { sampler->error = -EIO; break; }
		uint64_t target = timespec_to_ns(wake) + sampler->interval_ns;
		wake.tv_sec = (time_t)(target / 1000000000ULL);
		wake.tv_nsec = (long)(target % 1000000000ULL);
		int code = 0;
		while (!sampler->stop && !code) code = pthread_cond_timedwait(&sampler->stop_cv, &sampler->mutex, &wake);
		if (code && code != ETIMEDOUT) { sampler->error = -code; break; }
		if (!sampler->stop) sampler->error = qualification_rss_observe(sampler);
	}
	if (!sampler->error) sampler->error = qualification_rss_observe(sampler);
	pthread_mutex_unlock(&sampler->mutex);
	return NULL;
}

int qualification_rss_start(struct qualification_rss_sampler *sampler, uint64_t interval_ns,
	uint64_t (*now)(void *), uint64_t (*read)(void *), void *argument) {
	memset(sampler, 0, sizeof(*sampler));
	if (!interval_ns || interval_ns > 1000000000ULL || !now || !read) return -EINVAL;
	sampler->interval_ns = interval_ns; sampler->now = now; sampler->read = read; sampler->argument = argument;
	int code = qualification_rss_observe(sampler);
	if (code) { free(sampler->samples); sampler->samples = NULL; return code; }
	sampler->baseline = sampler->samples[0];
	code = pthread_mutex_init(&sampler->mutex, NULL);
	if (code) goto failed;
	pthread_condattr_t attributes;
	code = pthread_condattr_init(&attributes);
	if (code) { pthread_mutex_destroy(&sampler->mutex); goto failed; }
	code = pthread_condattr_setclock(&attributes, CLOCK_MONOTONIC);
	if (!code) code = pthread_cond_init(&sampler->stop_cv, &attributes);
	pthread_condattr_destroy(&attributes);
	if (code) { pthread_mutex_destroy(&sampler->mutex); goto failed; }
	code = pthread_create(&sampler->thread, NULL, qualification_rss_worker, sampler);
	if (!code) return 0;
	pthread_cond_destroy(&sampler->stop_cv); pthread_mutex_destroy(&sampler->mutex);
failed:
	free(sampler->samples); sampler->samples = NULL; return -code;
}

int qualification_rss_stop(struct qualification_rss_sampler *sampler) {
	pthread_mutex_lock(&sampler->mutex);
	sampler->stop = 1;
	pthread_cond_signal(&sampler->stop_cv);
	pthread_mutex_unlock(&sampler->mutex);
	pthread_join(sampler->thread, NULL);
	pthread_cond_destroy(&sampler->stop_cv); pthread_mutex_destroy(&sampler->mutex);
	return sampler->error;
}

int qualification_rss_validate(const struct qualification_rss_sampler *sampler, uint64_t begin, uint64_t elapsed_ns) {
	if (sampler->error) return sampler->error;
	if (!begin || sampler->count < 2 || sampler->samples[0].at_ns > begin || begin - sampler->samples[0].at_ns > sampler->interval_ns) return -EIO;
	for (size_t index = 1; index < sampler->count; index++) {
		if (sampler->samples[index].at_ns <= sampler->samples[index - 1].at_ns || sampler->samples[index].at_ns - sampler->samples[index - 1].at_ns > 2 * sampler->interval_ns) return -EIO;
	}
	uint64_t ended = sampler->samples[sampler->count - 1].at_ns;
	return ended < begin || ended - begin < elapsed_ns || ended - begin - elapsed_ns > sampler->interval_ns ? -EIO : 0;
}

#ifndef P07_QUALIFICATION_TEST
struct qualification_io {
	struct api *api;
	rados_ioctx_t ioctx;
	uint64_t size;
	int concurrency, workload;
	char (*objects)[96];
	int *owned;
	unsigned char **payloads, **destinations;
};

static uint64_t qualification_now(void *unused) {
	(void)unused;
	struct timespec value;
	return clock_gettime(CLOCK_MONOTONIC, &value) == 0 ? timespec_to_ns(value) : 0;
}

static uint64_t qualification_rss_read(void *unused) {
	(void)unused;
	return resident_bytes();
}

static void qualification_print_memory(FILE *output, const struct qualification_rss_sampler *sampler, uint64_t begin) {
	fprintf(output, ",\"memory\":{\"clock\":\"measurement_relative_ns\",\"interval_ns\":%" PRIu64 ",\"idle\":{\"connected\":true,\"equally_warmed\":true,\"at_ns\":%" PRId64 ",\"rss_bytes\":%" PRIu64 "},\"samples\":[",
		sampler->interval_ns, (int64_t)sampler->baseline.at_ns - (int64_t)begin, sampler->baseline.bytes);
	for (size_t index = 0; index < sampler->count; index++) fprintf(output, "%s{\"at_ns\":%" PRId64 ",\"rss_bytes\":%" PRIu64 "}", index ? "," : "", (int64_t)sampler->samples[index].at_ns - (int64_t)begin, sampler->samples[index].bytes);
	fprintf(output, "]}");
}

static int qualification_io_operation(void *argument, int worker, uint64_t ordinal) {
	struct qualification_io *io = argument;
	if (io->workload == WORKLOAD_WRITE || (io->workload == WORKLOAD_MIXED && ordinal % 2))
		return io->api->write_full(io->ioctx, io->objects[worker], (const char *)io->payloads[worker], (size_t)io->size);
	int count = io->api->read(io->ioctx, io->objects[worker], (char *)io->destinations[worker], (size_t)io->size, 0);
	if (count < 0) return count;
	return count == (int)io->size && memcmp(io->destinations[worker], io->payloads[worker], (size_t)io->size) == 0 ? 0 : -EIO;
}

static int qualification_integer(const char *text, uint64_t *value, int positive) {
	if (!text || !*text) return -EINVAL;
	for (const char *character = text; *character; character++) if (*character < '0' || *character > '9') return -EINVAL;
	char *end;
	errno = 0;
	*value = strtoull(text, &end, 10);
	return errno || *end || *value > 9007199254740991ULL || (positive && !*value) ? -EINVAL : 0;
}

static void qualification_print_phase(FILE *output, const struct qualification_capture *capture, uint64_t round, const char *leg,
	const char *phase_name, const struct qualification_io *io) {
	fprintf(output, "{\"elapsed_ns\":%" PRIu64 ",\"successful_operations\":%" PRIu64 ",\"unexpected_failures\":%" PRIu64 ",\"censored\":%" PRIu64 ",\"operations_per_worker\":[",
		capture->elapsed_ns, capture->successful, capture->failures, capture->censored);
	for (int worker = 0; worker < capture->concurrency; worker++) fprintf(output, "%s%zu", worker ? "," : "", capture->workers[worker].count);
	fprintf(output, "],\"records\":[");
	int preceding = 0;
	for (int worker = 0; worker < capture->concurrency; worker++) {
		const struct qualification_worker_records *records = &capture->workers[worker];
		for (size_t ordinal = 0; ordinal < records->count; ordinal++) {
			const struct qualification_record *record = &records->records[ordinal];
			const char *type = io->workload == WORKLOAD_MIXED ? (ordinal % 2 ? "write" : "read") : workload_name((enum workload_kind)io->workload);
			int censored = record->code == -ETIMEDOUT || record->code == -ECANCELED;
			fprintf(output, "%s{\"round\":%" PRIu64 ",\"leg\":\"%s\",\"operation_id\":\"%s-%s-w%d-op%zu\",\"object\":\"%s\",\"type\":\"%s\",\"worker\":%d,\"ordinal\":%zu,\"start_ns\":%" PRIu64 ",\"end_ns\":%" PRIu64 ",\"success\":%s,\"error\":",
				preceding ? "," : "", round, leg, leg, phase_name, worker, ordinal, io->objects[worker], type, worker, ordinal, record->start_ns, record->end_ns, record->code ? "false" : "true");
			if (record->code) fprintf(output, "\"native_error_%d\"", record->code); else fprintf(output, "null");
			fprintf(output, ",\"timeout\":%s,\"censored\":%s,\"retry_count\":null,\"timeout_deadline_ns\":%" PRIu64 "}", record->code == -ETIMEDOUT ? "true" : "false", censored ? "true" : "false", record->deadline_ns);
			preceding = 1;
		}
	}
	fprintf(output, "]}");
}

int main(int argc, char **argv) {
	const char *file = getenv("P07_QUALIFICATION_FILE"), *mode_log = getenv("P07_NATIVE_MODE_LOG"), *leg = getenv("P07_QUALIFICATION_LEG");
	uint64_t round = 0, seed = 0;
	if ((argc != 5 && argc != 6) || strcmp(argv[4], "secure") || !matched_parity() || read_matrix_settings() || matrix.size == 0 || matrix.concurrency == 0 || matrix.workload < 0 ||
		!file || !*file || !mode_log || !*mode_log || !strcmp(file, mode_log) || qualification_integer(getenv("P07_QUALIFICATION_ROUND"), &round, 1) ||
		qualification_integer(getenv("P07_QUALIFICATION_SEED"), &seed, 0) || !leg || !*leg || strlen(leg) > 80) {
		fprintf(stderr, "native qualification requires secure transport, explicit parity matrix and fresh capture/mode paths with round/leg/seed\n"); return 2;
	}
	const char *namespace = getenv("P07_PARITY_NAMESPACE");
	const char *pgo = getenv("P07_PGO_DIAGNOSTIC");
	if (pgo && *pgo && strcmp(pgo, "1")) return 2;
	if (getenv("P07_PGO_PROFILE_FILE") && *getenv("P07_PGO_PROFILE_FILE")) return 2;
	int pgo_diagnostic = pgo && !strcmp(pgo, "1");
	if (strlen(namespace) > 64 || strncmp(namespace, "p07-parity-", 11)) return 2;
	for (const char *character = namespace; *character; character++) if (*character != '-' && (*character < 'a' || *character > 'z') && (*character < '0' || *character > '9')) return 2;
	for (const char *character = leg; *character; character++) if (*character != '-' && (*character < 'a' || *character > 'z') && (*character < '0' || *character > '9')) return 2;
	const char *conflicts[] = {"P07_READ_DIAGNOSTIC", "P07_SEED_ONLY", "P07_OFFERED_LOAD", "P07_RETENTION_WINDOWS", "P07_BACKGROUND_WORKERS", "P07_CPU_PROFILE", "P07_MEMORY_PROFILE", "P07_TRACE_FILE", "P07_RESOURCE_FILE", "P07_TIMING_FILE", "P07_MATRIX_OPERATIONS_PER_WORKER", "P07_OPERATIONS_PER_WORKER", "P07_SCRATCH_SLOTS", "P07_ADMISSION_WINDOW", "P07_BACKGROUND_ALLOCATIONS"};
	for (size_t index = 0; index < sizeof(conflicts) / sizeof(conflicts[0]); index++) if (getenv(conflicts[index]) && *getenv(conflicts[index])) return 2;
	int descriptor = open(file, O_WRONLY | O_CREAT | O_EXCL, 0600);
	if (descriptor < 0) return 1;
	FILE *output = fdopen(descriptor, "w");
	if (!output) { close(descriptor); return 1; }
	struct api api = load_api();
	rados_t cluster = NULL;
	struct qualification_io io = {.api = &api, .size = matrix.size, .concurrency = matrix.concurrency, .workload = matrix.workload};
	struct qualification_capture warmup = {0}, measured = {0};
	struct qualification_rss_sampler rss_sampler = {0};
	int rss_started = 0;
	int error = 0, payload_verified = 0, cleanup_verified = 0;
	struct rusage before = {0}, after = {0};
	struct timespec window_start = {0}, window_end = {0};
	uint64_t rss_before = 0, rss_after = 0, rss_cleanup = 0, deadline = 0;
	const char *entity = argc == 6 ? argv[5] : "client.p07";
	if ((error = api.create2(&cluster, "ceph", entity, 0)) || (error = api.conf_read_file(cluster, argv[1])) ||
		(error = api.conf_set(cluster, "keyring", argv[2])) || (error = api.conf_set(cluster, "ms_client_mode", "secure")) ||
		(error = api.conf_set(cluster, "ms_mon_client_mode", "secure")) || (error = api.conf_set(cluster, "rados_osd_op_timeout", "30")) ||
		(error = api.conf_set(cluster, "rados_mon_op_timeout", "30"))) goto emit;
	descriptor = open(mode_log, O_WRONLY | O_CREAT | O_EXCL, 0600);
	if (descriptor < 0) { error = -errno; goto emit; }
	close(descriptor);
	if ((error = api.conf_set(cluster, "log_file", mode_log)) || (error = api.conf_set(cluster, "debug_ms", "1/1")) ||
		(error = api.conf_set(cluster, "debug_auth", "0/0")) || (error = api.conf_set(cluster, "log_to_file", "true")) ||
		(error = api.connect(cluster)) || (error = api.ioctx_create(cluster, argv[3], &io.ioctx))) goto emit;
	api.ioctx_set_namespace(io.ioctx, namespace);
	deadline = qualification_now(NULL) + 900000000000ULL;
	io.objects = calloc((size_t)io.concurrency, sizeof(*io.objects));
	io.owned = calloc((size_t)io.concurrency, sizeof(*io.owned));
	io.payloads = calloc((size_t)io.concurrency, sizeof(*io.payloads));
	io.destinations = calloc((size_t)io.concurrency, sizeof(*io.destinations));
	if (!io.objects || !io.owned || !io.payloads || !io.destinations) { error = -ENOMEM; goto cleanup; }
	for (int worker = 0; worker < io.concurrency; worker++) {
		if (format_object_name(io.objects[worker], sizeof(io.objects[worker]), 1, io.size, (enum workload_kind)io.workload, worker)) { error = -EINVAL; goto cleanup; }
		io.payloads[worker] = malloc((size_t)io.size);
		io.destinations[worker] = malloc((size_t)io.size);
		if (!io.payloads[worker] || !io.destinations[worker]) { error = -ENOMEM; goto cleanup; }
		fill_payload(io.payloads[worker], io.size, 1, worker);
		if (qualification_now(NULL) >= deadline) { error = -ETIMEDOUT; goto cleanup; }
		char existing;
		int absent = api.read(io.ioctx, io.objects[worker], &existing, 1, 0);
		if (absent != -ENOENT) { error = absent < 0 ? absent : -EEXIST; goto cleanup; }
		io.owned[worker] = 1;
		error = api.write_full(io.ioctx, io.objects[worker], (const char *)io.payloads[worker], (size_t)io.size);
		if (error) goto cleanup;
	}
	error = collect_native_qualification_phase(io.concurrency, ((pgo_diagnostic ? 1000ULL : 10000ULL) + (uint64_t)io.concurrency - 1) / (uint64_t)io.concurrency,
		pgo_diagnostic ? 1000000000ULL : 10000000000ULL, QUALIFICATION_RECORD_LIMIT / (uint64_t)io.concurrency, deadline, qualification_now, qualification_io_operation, &io, &warmup);
	if (error) goto cleanup;
	error = qualification_rss_start(&rss_sampler, 100000000ULL, qualification_now, qualification_rss_read, NULL);
	if (error) goto cleanup;
	rss_started = 1;
	rss_before = rss_sampler.baseline.bytes;
	if (getrusage(RUSAGE_SELF, &before)) { error = -EIO; goto cleanup; }
	if (clock_gettime(CLOCK_REALTIME, &window_start)) { error = -EIO; goto cleanup; }
	error = collect_native_qualification_phase(io.concurrency, ((pgo_diagnostic ? 10000ULL : 100000ULL) + (uint64_t)io.concurrency - 1) / (uint64_t)io.concurrency,
		pgo_diagnostic ? 8000000000ULL : 60000000000ULL, QUALIFICATION_RECORD_LIMIT / (uint64_t)io.concurrency, deadline, qualification_now, qualification_io_operation, &io, &measured);
	if (clock_gettime(CLOCK_REALTIME, &window_end) && !error) error = -EIO;
	int rss_error = qualification_rss_stop(&rss_sampler);
	rss_started = 0;
	if (!rss_error) rss_error = qualification_rss_validate(&rss_sampler, measured.start_ns, measured.elapsed_ns);
	if (!error) error = rss_error;
	if (getrusage(RUSAGE_SELF, &after) || !(rss_after = resident_bytes())) { if (!error) error = -EIO; }
	if (error) goto cleanup;
	for (int worker = 0; worker < io.concurrency; worker++) {
		if (qualification_now(NULL) >= deadline) { error = -ETIMEDOUT; goto cleanup; }
		int count = api.read(io.ioctx, io.objects[worker], (char *)io.destinations[worker], (size_t)io.size, 0);
		error = count < 0 ? count : count == (int)io.size && memcmp(io.destinations[worker], io.payloads[worker], (size_t)io.size) == 0 ? 0 : -EIO;
		if (!error && qualification_now(NULL) > deadline) error = -ETIMEDOUT;
		if (error) goto cleanup;
	}
	payload_verified = 1;
cleanup:
	if (rss_started) {
		int rss_error = qualification_rss_stop(&rss_sampler);
		rss_started = 0;
		if (!error) error = rss_error;
	}
	if (io.ioctx && io.objects && io.owned) {
		uint64_t cleanup_deadline = qualification_now(NULL) + 15000000000ULL;
		int cleanup_error = api.conf_set(cluster, "rados_osd_op_timeout", "1");
		int cleanup_configured = cleanup_error == 0;
		for (int worker = 0; cleanup_configured && worker < io.concurrency; worker++) {
			if (!io.owned[worker]) continue;
			if (qualification_now(NULL) >= cleanup_deadline) { cleanup_error = -ETIMEDOUT; break; }
			int removed = api.remove(io.ioctx, io.objects[worker]);
			if (removed && removed != -ENOENT) cleanup_error = removed;
			if (qualification_now(NULL) >= cleanup_deadline) { cleanup_error = -ETIMEDOUT; break; }
			char unused;
			if (api.read(io.ioctx, io.objects[worker], &unused, 1, 0) != -ENOENT) cleanup_error = -EIO;
		}
		if (qualification_now(NULL) > cleanup_deadline) cleanup_error = -ETIMEDOUT;
		cleanup_verified = cleanup_error == 0;
		if (!error) error = cleanup_error;
		rss_cleanup = resident_bytes();
		if (!rss_cleanup && !error) error = -EIO;
	}
emit:
	{
		uint64_t operations = 0;
		for (int worker = 0; worker < measured.concurrency; worker++) operations += measured.workers[worker].count;
		uint64_t *latencies = operations ? malloc((size_t)operations * sizeof(*latencies)) : NULL;
		if (operations && !latencies && !error) error = -ENOMEM;
		size_t position = 0;
		if (latencies) {
			for (int worker = 0; worker < measured.concurrency; worker++) for (size_t ordinal = 0; ordinal < measured.workers[worker].count; ordinal++) {
				struct qualification_record *record = &measured.workers[worker].records[ordinal];
				latencies[position++] = record->end_ns >= record->start_ns ? record->end_ns - record->start_ns : 0;
			}
			qsort(latencies, (size_t)operations, sizeof(*latencies), compare_u64);
		}
		double iops = measured.elapsed_ns ? (double)operations * 1000000000.0 / (double)measured.elapsed_ns : 0;
		uint64_t user_before = timeval_to_ns(before.ru_utime), user_after = timeval_to_ns(after.ru_utime);
		uint64_t system_before = timeval_to_ns(before.ru_stime), system_after = timeval_to_ns(after.ru_stime);
		fprintf(output, "{\"status\":\"%s\",\"identity\":{\"round\":%" PRIu64 ",\"leg\":\"%s\",\"seed\":%" PRIu64 "},\"report\":{\"implementation\":\"native\",\"transport\":\"secure\",\"environment\":{\"library\":\"librados.so.2\"},\"rows\":[{\"size_bytes\":%" PRIu64 ",\"concurrency\":%d,\"workload\":\"%s\",\"operations\":%" PRIu64 ",\"bytes\":%" PRIu64 ",\"elapsed_ns\":%" PRIu64 ",\"iops\":%.17g,\"throughput_bytes_per_second\":%.17g,\"p50_ns\":%" PRIu64 ",\"p95_ns\":%" PRIu64 ",\"p99_ns\":%" PRIu64 ",\"parity\":{\"payload_verified\":%s,\"cleanup_verified\":%s,\"rss_before_bytes\":%" PRIu64 ",\"rss_after_bytes\":%" PRIu64 ",\"rss_after_cleanup_bytes\":%" PRIu64 ",\"measured_resources\":{\"cpu_user_ns\":%" PRIu64 ",\"cpu_system_ns\":%" PRIu64 "}}}]},\"attempt\":{\"payload_verified\":%s,\"cleanup_verified\":%s,\"warmup\":",
			error ? "failed" : pgo_diagnostic ? "pgo_diagnostic_native_capture_unqualified" : "sustained_native_capture_unqualified", round, leg, seed, io.size, io.concurrency, workload_name((enum workload_kind)io.workload), operations, operations * io.size, measured.elapsed_ns, iops, iops * (double)io.size,
			latencies ? percentile(latencies, (size_t)operations, 50, 100) : 0, latencies ? percentile(latencies, (size_t)operations, 95, 100) : 0, latencies ? percentile(latencies, (size_t)operations, 99, 100) : 0,
			payload_verified ? "true" : "false", cleanup_verified ? "true" : "false", rss_before, rss_after, rss_cleanup, user_after >= user_before ? user_after - user_before : 0, system_after >= system_before ? system_after - system_before : 0,
			payload_verified ? "true" : "false", cleanup_verified ? "true" : "false");
		qualification_print_phase(output, &warmup, round, leg, "warmup", &io);
		fprintf(output, ",\"measured\":");
		qualification_print_phase(output, &measured, round, leg, "measured", &io);
		if (rss_sampler.samples && rss_sampler.count) qualification_print_memory(output, &rss_sampler, measured.start_ns);
		fprintf(output, "},\"error\":");
		if (error) fprintf(output, "\"native_error_%d\"", error); else fprintf(output, "null");
		fprintf(output, ",\"measurement_window\":{\"clock\":\"realtime\",\"start_ns\":\"%llu\",\"end_ns\":\"%llu\"}",
			(unsigned long long)window_start.tv_sec * 1000000000ULL + (unsigned long long)window_start.tv_nsec,
			(unsigned long long)window_end.tv_sec * 1000000000ULL + (unsigned long long)window_end.tv_nsec);
		fprintf(output, ",\"limitations\":[\"Retry counts are unknown, not zero; no qualification acceptance\",\"RSS interval samples include harness retention; library-only RSS is not established\",\"Measured CPU includes worker start, RSS sampling and record retention\",\"Synchronous RPC cancellation is not immediate: a failed safety deadline may overrun by up to the 30-second RPC timeout\",\"Source/binary, placement/health and Go pairing require an outer evidence driver\"]}\n");
		free(latencies);
	}
	int output_error = ferror(output);
	if (fclose(output)) output_error = 1;
	if (output_error && !error) error = -EIO;
	qualification_capture_destroy(&warmup);
	qualification_capture_destroy(&measured);
	free(rss_sampler.samples);
	for (int worker = 0; worker < io.concurrency; worker++) {
		if (io.payloads) free(io.payloads[worker]);
		if (io.destinations) free(io.destinations[worker]);
	}
	free(io.objects); free(io.owned); free(io.payloads); free(io.destinations);
	if (io.ioctx) api.ioctx_destroy(io.ioctx);
	if (cluster) api.shutdown(cluster);
	dlclose(api.library);
	return error ? 1 : 0;
}
#endif