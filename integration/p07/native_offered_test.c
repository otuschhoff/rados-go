#define P07_NATIVE_OFFERED_TEST
#include "native_offered.c"
#include <assert.h>

struct fake_completion { native_callback callback; void *argument; int result; };
static int force_timeout, force_error, force_warmup_error;
static int cleanup_present[16];

static int fake_cleanup_read(rados_ioctx_t unused, const char *object, char *buffer, size_t size, uint64_t offset) {
	(void)unused; (void)offset;
	int worker = -1; assert(sscanf(object, "p07-shared-read-%d", &worker) == 1 && worker >= 0 && worker < 16);
	if (!cleanup_present[worker]) return -ENOENT;
	fill_payload((unsigned char *)buffer, size, 1, worker);
	return (int)size;
}

static int fake_cleanup_remove(rados_ioctx_t unused, const char *object) {
	(void)unused;
	int worker = -1; assert(sscanf(object, "p07-shared-read-%d", &worker) == 1 && worker >= 0 && worker < 16);
	cleanup_present[worker] = 0;
	return 0;
}

static int fake_completion_create(void *argument, native_callback callback, native_callback unused, native_completion *result) {
	(void)unused;
	struct fake_completion *completion = calloc(1, sizeof(*completion));
	assert(completion != NULL); completion->callback = callback; completion->argument = argument; *result = completion;
	return 0;
}

static int fake_offered_read(rados_ioctx_t unused, const char *object, char *buffer, size_t size, uint64_t offset) {
	(void)unused; (void)offset;
	if (force_warmup_error) return -EIO;
	int worker = -1;
	assert(sscanf(object, "p07-shared-read-%d", &worker) == 1 && worker >= 0);
	fill_payload((unsigned char *)buffer, size, 1, worker);
	return (int)size;
}

static int fake_async_read(rados_ioctx_t ioctx, const char *object, native_completion value, char *buffer, size_t size, uint64_t offset) {
	if (force_error) return -EIO;
	struct fake_completion *completion = value;
	completion->result = fake_offered_read(ioctx, object, buffer, size, offset);
	if (!force_timeout) completion->callback(value, completion->argument);
	return 0;
}

static int fake_async_cancel(rados_ioctx_t unused, native_completion value) {
	(void)unused;
	struct fake_completion *completion = value;
	completion->callback(value, completion->argument);
	return 0;
}

static int fake_async_wait(native_completion unused) { (void)unused; return 0; }
static int fake_async_result(native_completion value) { return ((struct fake_completion *)value)->result; }
static void fake_async_release(native_completion value) { free(value); }

int main(void) {
	struct api api = {0}; api.read = fake_offered_read;
	struct offered_api async = {fake_completion_create, fake_async_read, fake_async_cancel, fake_async_wait, fake_async_result, fake_async_release};
	for (int scenario = 0; scenario < 4; scenario++) {
		force_timeout = scenario == 1; force_error = scenario == 2; force_warmup_error = scenario == 3;
		struct offered_state state = {.api = &api, .async = &async, .rate = scenario == 1 ? 100000 : 1000, .workers = 2, .queue_capacity = 1, .window = 8000000, .deadline = 1000000};
		int result = run_native_offered(&state);
		assert((result == -1) == (scenario == 3));
		int success = 0, timeout = 0, overload = 0, errors = 0, canceled = 0;
		for (int index = 0; index < state.expected; index++) {
			struct arrival_outcome *outcome = &state.outcomes[index];
			assert(outcome->scheduled == (int64_t)((uint64_t)index * 1000000000ULL / (uint64_t)state.rate));
			assert(outcome->returned >= outcome->scheduled);
			if (!outcome->admitted) assert(outcome->enqueued == -1 && !outcome->attempted);
			if (!strcmp(outcome->kind, "success")) success++;
			else if (!strcmp(outcome->kind, "timeout")) timeout++;
			else if (!strcmp(outcome->kind, "overload")) overload++;
			else if (!strcmp(outcome->kind, "error")) errors++;
			else canceled++;
		}
		assert(success + timeout + overload + errors + canceled == state.expected);
		if (scenario == 0) assert(success > 0 && errors == 0);
		if (scenario == 1) assert(timeout > 0 && overload > 0);
		if (scenario == 2) assert(errors > 0);
		if (scenario == 3) assert(canceled == state.expected);
		free(state.outcomes);
	}
	api.read = fake_cleanup_read; api.remove = fake_cleanup_remove;
	for (int worker = 0; worker < 16; worker++) cleanup_present[worker] = 1;
	assert(cleanup_offered_fixtures(&api, NULL) == 0);
	for (int worker = 0; worker < 16; worker++) assert(!cleanup_present[worker]);
	assert(cleanup_offered_fixtures(&api, NULL) == -1);
	puts("native offered arrivals, completion, timeout/drain, overload, error, warmup and exact cleanup tests passed");
	return 0;
}