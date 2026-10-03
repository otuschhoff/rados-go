#define main p07_closed_loop_main
#include "native_benchmark.c"
#undef main

typedef void *native_completion;
typedef void (*native_callback)(native_completion, void *);
struct offered_api {
	int (*create_completion)(void *, native_callback, native_callback, native_completion *);
	int (*read_async)(rados_ioctx_t, const char *, native_completion, char *, size_t, uint64_t);
	int (*cancel)(rados_ioctx_t, native_completion);
	int (*wait)(native_completion);
	int (*result)(native_completion);
	void (*release)(native_completion);
};

struct arrival_outcome {
	int64_t scheduled, enqueued, worker_start, read_start, returned, delivery;
	const char *kind;
	int admitted, attempted;
};

struct offered_state {
	struct api *api;
	struct offered_api *async;
	rados_ioctx_t ioctx;
	int rate, workers, queue_capacity, expected;
	uint64_t window, deadline, start, total;
	struct arrival_outcome *outcomes;
	int *queue, head, count, ready, closed, started, warmup_error, high_water;
	pthread_mutex_t mutex;
	pthread_cond_t available, barrier;
};

struct completion_waiter {
	pthread_mutex_t mutex;
	pthread_cond_t condition;
	int done;
};

struct offered_worker { struct offered_state *state; int worker; };

static uint64_t monotonic_ns(void) {
	struct timespec value;
	if (clock_gettime(CLOCK_MONOTONIC, &value) != 0) abort();
	return timespec_to_ns(value);
}

static struct timespec absolute_time(uint64_t value) {
	struct timespec result = {(time_t)(value / 1000000000ULL), (long)(value % 1000000000ULL)};
	return result;
}

static int wait_until(uint64_t value) {
	struct timespec target = absolute_time(value);
	int result;
	do { result = clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &target, NULL); } while (result == EINTR);
	return result;
}

static void read_completed(native_completion unused, void *argument) {
	(void)unused;
	struct completion_waiter *waiter = argument;
	pthread_mutex_lock(&waiter->mutex);
	waiter->done = 1;
	pthread_cond_signal(&waiter->condition);
	pthread_mutex_unlock(&waiter->mutex);
}

static int deadline_read(struct offered_state *state, const char *object, char *buffer, uint64_t deadline) {
	struct completion_waiter waiter = {0};
	pthread_condattr_t attributes;
	if (pthread_mutex_init(&waiter.mutex, NULL) != 0) return -EIO;
	if (pthread_condattr_init(&attributes) != 0) { pthread_mutex_destroy(&waiter.mutex); return -EIO; }
	if (pthread_condattr_setclock(&attributes, CLOCK_MONOTONIC) != 0 || pthread_cond_init(&waiter.condition, &attributes) != 0) {
		pthread_condattr_destroy(&attributes); pthread_mutex_destroy(&waiter.mutex); return -EIO;
	}
	pthread_condattr_destroy(&attributes);
	native_completion completion = NULL;
	int result = state->async->create_completion(&waiter, read_completed, NULL, &completion);
	int timed_out = 0;
	if (result == 0) result = state->async->read_async(state->ioctx, object, completion, buffer, 65536, 0);
	if (result == 0) {
		struct timespec limit = absolute_time(deadline);
		pthread_mutex_lock(&waiter.mutex);
		while (!waiter.done) {
			int waited = pthread_cond_timedwait(&waiter.condition, &waiter.mutex, &limit);
			if (waited == ETIMEDOUT) { timed_out = 1; break; }
			if (waited != 0) { result = -EIO; break; }
		}
		pthread_mutex_unlock(&waiter.mutex);
		if (timed_out || result != 0) (void)state->async->cancel(state->ioctx, completion);
		(void)state->async->wait(completion);
		pthread_mutex_lock(&waiter.mutex);
		while (!waiter.done) pthread_cond_wait(&waiter.condition, &waiter.mutex);
		pthread_mutex_unlock(&waiter.mutex);
		if (result == 0) result = state->async->result(completion);
	}
	if (completion != NULL) state->async->release(completion);
	pthread_cond_destroy(&waiter.condition); pthread_mutex_destroy(&waiter.mutex);
	return timed_out ? -ETIMEDOUT : result;
}

static void *offered_worker_main(void *argument) {
	struct offered_worker *worker = argument;
	struct offered_state *state = worker->state;
	char object[64];
	snprintf(object, sizeof(object), "p07-shared-read-%d", worker->worker);
	unsigned char *expected = malloc(65536), *buffer = malloc(65536);
	int error = expected == NULL || buffer == NULL;
	if (!error) {
		fill_payload(expected, 65536, 1, worker->worker);
		for (int iteration = 0; iteration < 8; iteration++) {
			int count = state->api->read(state->ioctx, object, (char *)buffer, 65536, 0);
			if (count != 65536 || memcmp(buffer, expected, 65536) != 0) { error = 1; break; }
		}
	}
	pthread_mutex_lock(&state->mutex);
	state->warmup_error |= error;
	state->ready++;
	pthread_cond_broadcast(&state->barrier);
	while (!state->started) pthread_cond_wait(&state->barrier, &state->mutex);
	for (;;) {
		while (state->count == 0 && !state->closed) pthread_cond_wait(&state->available, &state->mutex);
		if (state->count == 0 && state->closed) break;
		int index = state->queue[state->head];
		state->head = (state->head + 1) % state->queue_capacity; state->count--;
		pthread_mutex_unlock(&state->mutex);
		struct arrival_outcome *outcome = &state->outcomes[index];
		outcome->worker_start = (int64_t)(monotonic_ns() - state->start);
		uint64_t deadline = state->start + (uint64_t)outcome->scheduled + state->deadline;
		if (state->warmup_error) outcome->kind = "canceled";
		else if (monotonic_ns() >= deadline) outcome->kind = "timeout";
		else {
			outcome->attempted = 1; outcome->read_start = (int64_t)(monotonic_ns() - state->start);
			int count = deadline_read(state, object, (char *)buffer, deadline);
			int valid = count == 65536 && memcmp(buffer, expected, 65536) == 0;
			outcome->returned = (int64_t)(monotonic_ns() - state->start);
			if (count == -ETIMEDOUT || (uint64_t)outcome->returned >= (uint64_t)outcome->scheduled + state->deadline) outcome->kind = "timeout";
			else if (!valid) outcome->kind = "error";
			else outcome->kind = "success";
		}
		if (outcome->returned < 0) outcome->returned = (int64_t)(monotonic_ns() - state->start);
		pthread_mutex_lock(&state->mutex);
	}
	pthread_mutex_unlock(&state->mutex);
	free(expected); free(buffer);
	return NULL;
}

static int run_native_offered(struct offered_state *state) {
	state->expected = (int)(state->window * (uint64_t)state->rate / 1000000000ULL);
	state->outcomes = calloc((size_t)state->expected, sizeof(*state->outcomes));
	state->queue = calloc((size_t)state->queue_capacity, sizeof(int));
	pthread_t *threads = calloc((size_t)state->workers, sizeof(*threads));
	struct offered_worker *workers = calloc((size_t)state->workers, sizeof(*workers));
	if (!state->outcomes || !state->queue || !threads || !workers) goto allocation_failure;
	if (pthread_mutex_init(&state->mutex, NULL) != 0) goto allocation_failure;
	if (pthread_cond_init(&state->available, NULL) != 0) { pthread_mutex_destroy(&state->mutex); goto allocation_failure; }
	if (pthread_cond_init(&state->barrier, NULL) != 0) { pthread_cond_destroy(&state->available); pthread_mutex_destroy(&state->mutex); goto allocation_failure; }
	for (int index = 0; index < state->expected; index++) {
		struct arrival_outcome *outcome = &state->outcomes[index];
		outcome->scheduled = (int64_t)((uint64_t)index * 1000000000ULL / (uint64_t)state->rate);
		outcome->enqueued = outcome->worker_start = outcome->read_start = outcome->returned = outcome->delivery = -1;
		outcome->kind = "canceled";
	}
	int created = 0;
	for (int worker = 0; worker < state->workers; worker++) {
		workers[worker].state = state; workers[worker].worker = worker;
		if (pthread_create(&threads[worker], NULL, offered_worker_main, &workers[worker]) != 0) break;
		created++;
	}
	pthread_mutex_lock(&state->mutex);
	while (state->ready < created) pthread_cond_wait(&state->barrier, &state->mutex);
	if (created != state->workers) state->warmup_error = 1;
	state->start = monotonic_ns(); state->started = 1;
	pthread_cond_broadcast(&state->barrier); pthread_mutex_unlock(&state->mutex);
	for (int index = 0; index < state->expected; index++) {
		struct arrival_outcome *outcome = &state->outcomes[index];
		int waited = wait_until(state->start + (uint64_t)outcome->scheduled);
		outcome->enqueued = (int64_t)(monotonic_ns() - state->start);
		outcome->delivery = outcome->enqueued - outcome->scheduled;
		pthread_mutex_lock(&state->mutex);
		if (waited != 0 || state->warmup_error) { outcome->kind = "canceled"; outcome->returned = outcome->enqueued; outcome->enqueued = -1; }
		else if (state->count == state->queue_capacity) { outcome->kind = "overload"; outcome->returned = outcome->enqueued; outcome->enqueued = -1; }
		else {
			outcome->admitted = 1;
			state->queue[(state->head + state->count) % state->queue_capacity] = index; state->count++;
			if (state->count > state->high_water) state->high_water = state->count;
			pthread_cond_signal(&state->available);
		}
		pthread_mutex_unlock(&state->mutex);
	}
	(void)wait_until(state->start + state->window);
	pthread_mutex_lock(&state->mutex); state->closed = 1; pthread_cond_broadcast(&state->available); pthread_mutex_unlock(&state->mutex);
	for (int worker = 0; worker < created; worker++) pthread_join(threads[worker], NULL);
	state->total = monotonic_ns() - state->start;
	pthread_cond_destroy(&state->barrier); pthread_cond_destroy(&state->available); pthread_mutex_destroy(&state->mutex);
	free(threads); free(workers); free(state->queue); state->queue = NULL;
	return state->warmup_error ? -1 : 0;

allocation_failure:
	free(state->outcomes); state->outcomes = NULL;
	free(state->queue); state->queue = NULL;
	free(threads); free(workers);
	return -1;
}

static int cleanup_offered_fixtures(struct api *api, rados_ioctx_t ioctx) {
	unsigned char expected[65536], buffer[65536];
	int failed = 0;
	for (int worker = 0; worker < 16; worker++) {
		char object[64]; snprintf(object, sizeof(object), "p07-shared-read-%d", worker);
		fill_payload(expected, sizeof(expected), 1, worker);
		int count = api->read(ioctx, object, (char *)buffer, sizeof(buffer), 0);
		if (count != (int)sizeof(buffer) || memcmp(buffer, expected, sizeof(buffer)) != 0) failed = 1;
		if (api->remove(ioctx, object) != 0 || api->read(ioctx, object, (char *)buffer, 1, 0) != -ENOENT) failed = 1;
	}
	return failed ? -1 : 0;
}

#ifndef P07_NATIVE_OFFERED_TEST
int main(int argc, char **argv) {
	if (argc != 5 || !matched_parity()) { fprintf(stderr, "usage: native-offered CONF KEYRING POOL RATE; exclusive P07_PARITY_NAMESPACE required\n"); return 2; }
	const char *namespace = getenv("P07_PARITY_NAMESPACE");
	if (strlen(namespace) > 64 || strncmp(namespace, "p07-parity-", 11) != 0) return 2;
	for (const char *character = namespace; *character; character++) if (*character != '-' && (*character < 'a' || *character > 'z') && (*character < '0' || *character > '9')) return 2;
	char *end;
	long rate = strtol(argv[4], &end, 10);
	if (*end || (rate != 1000 && rate != 2000 && rate != 4000 && rate != 8000 && rate != 16000 && rate != 32000 && rate != 64000)) return 2;
	struct api api = load_api();
	struct offered_api async = {0};
#define AIO_BIND(field, symbol) bind_symbol(api.library, symbol, &async.field, sizeof(async.field))
	AIO_BIND(create_completion, "rados_aio_create_completion"); AIO_BIND(read_async, "rados_aio_read"); AIO_BIND(cancel, "rados_aio_cancel");
	AIO_BIND(wait, "rados_aio_wait_for_complete_and_cb"); AIO_BIND(result, "rados_aio_get_return_value"); AIO_BIND(release, "rados_aio_release");
	rados_t cluster = NULL; rados_ioctx_t ioctx = NULL;
	if (check_result(api.create2(&cluster, "ceph", "client.amakura", 0), "create") || check_result(api.conf_read_file(cluster, argv[1]), "config") || check_result(api.conf_set(cluster, "keyring", argv[2]), "keyring") || check_result(api.conf_set(cluster, "ms_client_mode", "secure"), "secure")) {
		if (cluster) api.shutdown(cluster);
		dlclose(api.library);
		return 1;
	}
	const char *mode_log = getenv("P07_NATIVE_MODE_LOG");
	if (mode_log && *mode_log) {
		int descriptor = open(mode_log, O_WRONLY | O_CREAT | O_EXCL, 0600);
		if (descriptor < 0) { api.shutdown(cluster); dlclose(api.library); return 1; }
		close(descriptor);
		if (check_result(api.conf_set(cluster, "log_file", mode_log), "mode log") || check_result(api.conf_set(cluster, "debug_ms", "1/1"), "mode debug") || check_result(api.conf_set(cluster, "debug_auth", "0/0"), "auth debug disabled") || check_result(api.conf_set(cluster, "log_to_file", "true"), "mode logging")) {
			api.shutdown(cluster); dlclose(api.library); return 1;
		}
	}
	if (check_result(api.connect(cluster), "connect") || check_result(api.ioctx_create(cluster, argv[3], &ioctx), "pool")) { api.shutdown(cluster); dlclose(api.library); return 1; }
	api.ioctx_set_namespace(ioctx, namespace);
	const char *cleanup = getenv("P07_PARITY_CLEANUP");
	if (cleanup && *cleanup) {
		int checked = strcmp(cleanup, "1") == 0 ? cleanup_offered_fixtures(&api, ioctx) : -1;
		printf("{\"payload_verified\":%s,\"cleanup_verified\":%s,\"owned_objects\":16}\n", checked == 0 ? "true" : "false", checked == 0 ? "true" : "false");
		api.ioctx_destroy(ioctx); api.shutdown(cluster); dlclose(api.library);
		return checked == 0 ? 0 : 1;
	}
	struct offered_state state = {.api = &api, .async = &async, .ioctx = ioctx, .rate = (int)rate, .workers = 16, .queue_capacity = 128, .window = 8000000000ULL, .deadline = 500000000ULL};
	struct rusage before, after; int resource_error = getrusage(RUSAGE_SELF, &before);
	int result = run_native_offered(&state);
	resource_error |= getrusage(RUSAGE_SELF, &after);
	if (!state.outcomes || resource_error) { api.ioctx_destroy(ioctx); api.shutdown(cluster); dlclose(api.library); return 1; }
	uint64_t *latencies = calloc((size_t)state.expected, sizeof(uint64_t));
	if (!latencies) { free(state.outcomes); api.ioctx_destroy(ioctx); api.shutdown(cluster); dlclose(api.library); return 1; }
	int success = 0, errors = 0, timeouts = 0, overload = 0, canceled = 0, attempted = 0, admitted = 0;
	int64_t maximum_lag = 0;
	for (int index = 0; index < state.expected; index++) {
		struct arrival_outcome *outcome = &state.outcomes[index];
		latencies[index] = outcome->returned > outcome->scheduled ? (uint64_t)(outcome->returned - outcome->scheduled) : 0;
		int64_t lag = outcome->delivery; if (lag > maximum_lag) maximum_lag = lag;
		attempted += outcome->attempted; admitted += outcome->admitted;
		if (!strcmp(outcome->kind, "success")) success++;
		else if (!strcmp(outcome->kind, "timeout")) timeouts++;
		else if (!strcmp(outcome->kind, "overload")) overload++;
		else if (!strcmp(outcome->kind, "canceled")) canceled++;
		else errors++;
	}
	qsort(latencies, (size_t)state.expected, sizeof(uint64_t), compare_u64);
	int failed = result != 0 || success != state.expected || maximum_lag > 50000000;
	printf("{\"implementation\":\"native\",\"transport\":\"secure\",\"resources\":{\"cpu_user_ns\":%" PRIu64 ",\"cpu_system_ns\":%" PRIu64 ",\"max_rss_bytes\":%" PRIu64 "},\"offered_load\":{\"expected\":%d,\"admitted\":%d,\"attempted\":%d,\"success\":%d,\"timeouts\":%d,\"overload\":%d,\"errors\":%d,\"canceled\":%d,\"failed\":%s,\"delivery_invalid\":%s,\"rate\":%d,\"window_ns\":%" PRIu64 ",\"deadline_ns\":%" PRIu64 ",\"workers\":16,\"queue_capacity\":128,\"total_window_drain_ns\":%" PRIu64 ",\"all_outcome_p99_ns\":%" PRIu64 ",\"success_ops_per_issuance_second\":%.6f,\"outcomes\":[", timeval_to_ns(after.ru_utime)-timeval_to_ns(before.ru_utime), timeval_to_ns(after.ru_stime)-timeval_to_ns(before.ru_stime), (uint64_t)after.ru_maxrss*1024, state.expected, admitted, attempted, success, timeouts, overload, errors, canceled, failed ? "true" : "false", maximum_lag > 50000000 ? "true" : "false", state.rate, state.window, state.deadline, state.total, percentile(latencies, (size_t)state.expected, 99, 100), (double)success*1e9/(double)state.window);
	for (int index = 0; index < state.expected; index++) {
		struct arrival_outcome *outcome = &state.outcomes[index];
		printf("%s{\"scheduled_ns\":%" PRId64 ",\"enqueued_ns\":%" PRId64 ",\"worker_start_ns\":%" PRId64 ",\"read_start_ns\":%" PRId64 ",\"returned_ns\":%" PRId64 ",\"delivery_delay_ns\":%" PRId64 ",\"kind\":\"%s\",\"admitted\":%s,\"attempted\":%s}", index ? "," : "", outcome->scheduled, outcome->enqueued, outcome->worker_start, outcome->read_start, outcome->returned, outcome->delivery, outcome->kind, outcome->admitted ? "true" : "false", outcome->attempted ? "true" : "false");
	}
	printf("]},\"limitations\":\"Native timeout cancellation drains completion before buffer reuse; resource counters include warmup and outcome bookkeeping, not isolated service cost; no qualification claim\"}\n");
	free(latencies); free(state.outcomes); api.ioctx_destroy(ioctx); api.shutdown(cluster); dlclose(api.library);
	return failed ? 1 : 0;
}
#endif