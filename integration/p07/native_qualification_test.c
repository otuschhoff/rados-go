#define P07_QUALIFICATION_TEST
#include "native_qualification.c"
#include <assert.h>
#include <stdatomic.h>
#include <sched.h>

struct fake_rss {
	_Atomic uint64_t clock, reads;
	int fail;
};

static uint64_t fake_rss_now(void *argument) {
	struct fake_rss *fake = argument;
	return atomic_fetch_add(&fake->clock, 100) + 100;
}

static uint64_t fake_rss_read(void *argument) {
	struct fake_rss *fake = argument;
	return atomic_fetch_add(&fake->reads, 1) >= 1 && fake->fail ? 0 : 4096;
}

static uint64_t test_monotonic_now(void) {
	struct timespec value;
	assert(clock_gettime(CLOCK_MONOTONIC, &value) == 0);
	return timespec_to_ns(value);
}

struct fake_qualification {
	_Atomic uint64_t clock;
	uint64_t step;
	int failure, fail_at;
};

static uint64_t fake_now(void *argument) {
	struct fake_qualification *fake = argument;
	return atomic_load(&fake->clock);
}

static int fake_operation(void *argument, int worker, uint64_t ordinal) {
	(void)worker;
	struct fake_qualification *fake = argument;
	atomic_fetch_add(&fake->clock, fake->step ? fake->step : 10);
	return fake->failure && ordinal == (uint64_t)fake->fail_at ? fake->failure : 0;
}

int main(void) {
	struct qualification_rss_sampler sampler;
	struct fake_rss unavailable = {.fail = 1, .reads = 1};
	assert(qualification_rss_start(&sampler, 0, fake_rss_now, fake_rss_read, &unavailable) == -EINVAL);
	assert(qualification_rss_start(&sampler, 1000000001ULL, fake_rss_now, fake_rss_read, &unavailable) == -EINVAL);
	assert(qualification_rss_start(&sampler, 1000000, fake_rss_now, fake_rss_read, &unavailable) == -EIO);
	for (int fail = 0; fail < 2; fail++) {
		struct fake_rss fake = {.fail = fail};
		assert(qualification_rss_start(&sampler, 1000000, fake_rss_now, fake_rss_read, &fake) == 0);
		uint64_t deadline = test_monotonic_now() + 1000000000ULL;
		while (atomic_load(&fake.reads) < 2 && test_monotonic_now() < deadline) sched_yield();
		assert(atomic_load(&fake.reads) >= 2);
		assert(qualification_rss_stop(&sampler) == (fail ? -EIO : 0));
		assert(qualification_rss_validate(&sampler, 150, 100) == (fail ? -EIO : 0));
		assert(sampler.baseline.at_ns == 100 && sampler.baseline.bytes == 4096);
		free(sampler.samples);
	}
	struct qualification_rss_sample samples[] = {{99, 4096}, {100, 4096}, {200, 8192}};
	sampler = (struct qualification_rss_sampler){.samples = samples, .count = 3, .interval_ns = 100};
	assert(qualification_rss_validate(&sampler, 100, 100) == 0);
	assert(qualification_rss_validate(&sampler, 98, 100) == -EIO);
	assert(qualification_rss_validate(&sampler, 100, 101) == -EIO);
	samples[1].at_ns = 1000;
	assert(qualification_rss_validate(&sampler, 100, 100) == -EIO);
	sampler.count = 10000;
	assert(qualification_rss_observe(&sampler) == -EOVERFLOW);
	struct qualification_capture capture;
	for (int time_first = 0; time_first < 2; time_first++) {
		struct fake_qualification fake = {.clock = 1};
		assert(collect_native_qualification_phase(1, time_first ? 10 : 2, time_first ? 1 : 100, 10, 1000, fake_now, fake_operation, &fake, &capture) == 0);
		assert(capture.successful == 10 && capture.failures == 0 && capture.censored == 0 && capture.workers[0].count == 10);
		qualification_capture_destroy(&capture);
	}
	struct fake_qualification concurrent = {.clock = 1};
	assert(collect_native_qualification_phase(16, 3, 1, 3, 10000, fake_now, fake_operation, &concurrent, &capture) == 0);
	assert(capture.successful == 48 && capture.failures == 0);
	for (int worker = 0; worker < 16; worker++) {
		assert(capture.workers[worker].count == 3);
		uint64_t preceding = 0;
		for (size_t ordinal = 0; ordinal < 3; ordinal++) {
			struct qualification_record *record = &capture.workers[worker].records[ordinal];
			assert(record->end_ns > record->start_ns && record->start_ns >= preceding && record->deadline_ns >= record->end_ns);
			preceding = record->end_ns;
		}
	}
	qualification_capture_destroy(&capture);
	for (int sample = 0; sample < 3; sample++) {
		int code = sample == 0 ? -EIO : sample == 1 ? -ETIMEDOUT : -ECANCELED;
		struct fake_qualification failure = {.clock = 1, .failure = code, .fail_at = 1};
		assert(collect_native_qualification_phase(1, 3, 1, 3, 1000, fake_now, fake_operation, &failure, &capture) == code);
		assert(capture.workers[0].count == 2 && capture.successful == 1 && capture.failures == 1);
		assert(capture.censored == (uint64_t)(code != -EIO));
		qualification_capture_destroy(&capture);
	}
	struct fake_qualification limit = {.clock = 1};
	assert(collect_native_qualification_phase(1, 1, 1000, 3, 10000, fake_now, fake_operation, &limit, &capture) == -EOVERFLOW);
	assert(capture.successful == 3 && capture.workers[0].count == 3);
	qualification_capture_destroy(&capture);
	struct fake_qualification expired = {.clock = 100};
	assert(collect_native_qualification_phase(1, 1, 1, 1, 100, fake_now, fake_operation, &expired, &capture) == -ETIMEDOUT);
	assert(capture.successful == 0 && capture.workers[0].count == 0);
	qualification_capture_destroy(&capture);
	assert(collect_native_qualification_phase(1, 1, 1, UINT64_MAX, 100, fake_now, fake_operation, &expired, &capture) == -EINVAL);
	for (int shorter_parent = 0; shorter_parent < 2; shorter_parent++) {
		uint64_t allowance = shorter_parent ? 5000000000ULL : 30000000000ULL;
		struct fake_qualification late = {.clock = 1, .step = allowance + 1};
		assert(collect_native_qualification_phase(1, 1, 1, 1, shorter_parent ? allowance + 1 : 60000000000ULL, fake_now, fake_operation, &late, &capture) == -ETIMEDOUT);
		assert(capture.successful == 0 && capture.censored == 1 && capture.workers[0].count == 1);
		assert(capture.workers[0].records[0].deadline_ns == allowance);
		qualification_capture_destroy(&capture);
	}
	puts("native qualification core: minimum-order, concurrency, failure, deadline and record-limit tests passed");
	return 0;
}