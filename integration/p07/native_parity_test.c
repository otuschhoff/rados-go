#define main benchmark_main
#include "native_benchmark.c"
#undef main
#include <assert.h>

static unsigned char stored[2][64];
static int present[2];
static int fail_write, corrupt_read, fail_remove;

static int worker_index(const char *object) {
	const char *suffix = strrchr(object, '-');
	assert(suffix != NULL && suffix[1] == 'w');
	int worker = atoi(suffix + 2);
	assert(worker >= 0 && worker < 2);
	return worker;
}

static int fake_write(rados_ioctx_t unused, const char *object, const char *data, size_t size) {
	(void)unused;
	if (fail_write) return -EIO;
	assert(size == 64);
	int worker = worker_index(object);
	memcpy(stored[worker], data, size);
	present[worker] = 1;
	return 0;
}

static int fake_read(rados_ioctx_t unused, const char *object, char *data, size_t size, uint64_t offset) {
	(void)unused; (void)offset;
	int worker = worker_index(object);
	if (!present[worker]) return -ENOENT;
	memcpy(data, stored[worker], size);
	if (corrupt_read) data[0] ^= 1;
	return (int)size;
}

static int fake_remove(rados_ioctx_t unused, const char *object) {
	(void)unused;
	if (fail_remove) return -EIO;
	present[worker_index(object)] = 0;
	return 0;
}

int main(void) {
	assert(setenv("P07_PARITY_NAMESPACE", "p07-parity-test", 1) == 0);
	matrix.operations = 256;
	matrix.concurrency = 2;
	char name[96];
	assert(format_object_name(name, sizeof(name), 1, 1048576, WORKLOAD_MIXED, 1) == 0);
	assert(strcmp(name, "p07-parity-1048576-c2-mixed-w1") == 0);
	struct api api = {0};
	api.write_full = fake_write; api.read = fake_read; api.remove = fake_remove;
	struct row_result result = {0};
	assert(run_row(&api, NULL, 1, 64, 2, WORKLOAD_WRITE, &result) == 0);
	assert(result.operations == 512 && result.bytes == 32768 && result.elapsed_ns > 0);
	assert(!present[0] && !present[1]);
	fail_write = 1;
	assert(run_row(&api, NULL, 1, 64, 2, WORKLOAD_WRITE, &result) == -1);
	fail_write = 0; corrupt_read = 1;
	assert(run_row(&api, NULL, 1, 64, 2, WORKLOAD_WRITE, &result) == -1);
	corrupt_read = 0; fail_remove = 1;
	assert(run_row(&api, NULL, 1, 64, 2, WORKLOAD_WRITE, &result) == -1);
	puts("native parity fixture, warmup failure, payload and cleanup tests passed");
	return 0;
}