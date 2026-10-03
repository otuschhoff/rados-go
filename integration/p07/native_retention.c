#define main p07_retention_closed_loop_main
#include "native_benchmark.c"
#undef main
#include <malloc.h>

int main(int argc, char **argv) {
	if (argc != 5 || !matched_parity() || read_matrix_settings() != 0 || matrix.operations <= 2 || matrix.size == 0 || matrix.concurrency == 0 || matrix.workload < 0) {
		fprintf(stderr, "usage: native-retention CONF KEYRING POOL WINDOWS; explicit parity matrix required\n");
		return 2;
	}
	const char *namespace = getenv("P07_PARITY_NAMESPACE");
	if (strlen(namespace) > 64 || strncmp(namespace, "p07-parity-", 11) != 0) return 2;
	for (const char *character = namespace; *character; character++) if (*character != '-' && (*character < 'a' || *character > 'z') && (*character < '0' || *character > '9')) return 2;
	char *end;
	long windows = strtol(argv[4], &end, 10);
	if (*end || windows < 6 || windows > 64) return 2;
	struct api api = load_api();
	rados_t cluster = NULL; rados_ioctx_t ioctx = NULL;
	if (check_result(api.create2(&cluster, "ceph", "client.amakura", 0), "create") || check_result(api.conf_read_file(cluster, argv[1]), "config") || check_result(api.conf_set(cluster, "keyring", argv[2]), "keyring") || check_result(api.conf_set(cluster, "ms_client_mode", "secure"), "secure")) {
		if (cluster) api.shutdown(cluster);
		dlclose(api.library); return 1;
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
	struct row_result *rows = calloc((size_t)windows, sizeof(*rows));
	uint64_t *allocator = calloc((size_t)windows, sizeof(*allocator));
	if (!rows || !allocator) { free(rows); free(allocator); api.ioctx_destroy(ioctx); api.shutdown(cluster); dlclose(api.library); return 1; }
	int result = 0;
	for (long window = 0; window < windows; window++) {
		if (run_row(&api, ioctx, 1, matrix.size, matrix.concurrency, (enum workload_kind)matrix.workload, &rows[window]) != 0) { result = 1; break; }
		struct mallinfo2 memory = mallinfo2();
		allocator[window] = (uint64_t)memory.uordblks + (uint64_t)memory.hblkhd;
	}
	if (result == 0) {
		printf("{\"implementation\":\"native\",\"transport\":\"secure\",\"windows\":[");
		for (long window = 0; window < windows; window++) {
			printf("%s{\"index\":%ld,\"operations\":%" PRIu64 ",\"size_bytes\":%" PRIu64 ",\"concurrency\":%d,\"workload\":\"%s\",\"rss_after_cleanup_bytes\":%" PRIu64 ",\"glibc_allocated_bytes\":%" PRIu64 ",\"payload_verified\":true,\"cleanup_verified\":true}", window ? "," : "", window, rows[window].operations, rows[window].size_bytes, rows[window].concurrency, rows[window].workload, rows[window].rss_cleanup, allocator[window]);
		}
		printf("],\"limitations\":\"One connected client, repeated fixed fixtures; glibc allocator bytes are not all native live heap or Go post-GC heap; RSS includes runtime/library caches; bounded-window observations are not endurance guarantees\"}\n");
	}
	free(rows); free(allocator); api.ioctx_destroy(ioctx); api.shutdown(cluster); dlclose(api.library);
	return result;
}