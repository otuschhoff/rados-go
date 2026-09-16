#define _POSIX_C_SOURCE 200809L
#include <dlfcn.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef void *rados_t;
typedef void *rados_ioctx_t;

struct rados_cluster_stat_t { uint64_t kb, kb_used, kb_avail, num_objects; };
struct rados_pool_stat_t {
	uint64_t num_bytes, num_kb, num_objects, num_object_clones, num_object_copies;
	uint64_t num_objects_missing_on_primary, num_objects_unfound, num_objects_degraded;
	uint64_t num_rd, num_rd_kb, num_wr, num_wr_kb, num_user_bytes;
	uint64_t compressed_bytes_orig, compressed_bytes, compressed_bytes_alloc;
};

typedef int (*create2_fn)(rados_t *, const char *, const char *, uint64_t);
typedef int (*conf_read_file_fn)(rados_t, const char *);
typedef int (*conf_set_fn)(rados_t, const char *, const char *);
typedef int (*connect_fn)(rados_t);
typedef void (*shutdown_fn)(rados_t);
typedef int (*ioctx_create_fn)(rados_t, const char *, rados_ioctx_t *);
typedef void (*ioctx_destroy_fn)(rados_ioctx_t);
typedef int (*cluster_stat_fn)(rados_t, struct rados_cluster_stat_t *);
typedef int (*pool_stat_fn)(rados_ioctx_t, struct rados_pool_stat_t *);
typedef int (*pool_create_fn)(rados_t, const char *);
typedef int (*pool_delete_fn)(rados_t, const char *);
typedef int (*blocklist_add_fn)(rados_t, char *, uint32_t);
typedef int (*getaddrs_fn)(rados_t, char **);
typedef void (*buffer_free_fn)(char *);
typedef int (*application_enable_fn)(rados_ioctx_t, const char *, int);
typedef int (*application_list_fn)(rados_ioctx_t, char *, size_t *);
typedef int (*application_metadata_get_fn)(rados_ioctx_t, const char *, const char *, char *, size_t *);
typedef int (*application_metadata_set_fn)(rados_ioctx_t, const char *, const char *, const char *);
typedef int (*application_metadata_remove_fn)(rados_ioctx_t, const char *, const char *);
typedef int (*application_metadata_list_fn)(rados_ioctx_t, const char *, char *, size_t *, char *, size_t *);
typedef int (*command_fn)(rados_t, const char **, size_t, const char *, size_t, char **, size_t *, char **, size_t *);
typedef int (*osd_command_fn)(rados_t, int, const char **, size_t, const char *, size_t, char **, size_t *, char **, size_t *);
typedef int (*pg_command_fn)(rados_t, const char *, const char **, size_t, const char *, size_t, char **, size_t *, char **, size_t *);
typedef int (*inconsistent_pg_list_fn)(rados_t, int64_t, char *, size_t);

struct api {
	void *library;
	create2_fn create2; conf_read_file_fn conf_read_file; conf_set_fn conf_set; connect_fn connect; shutdown_fn shutdown;
	ioctx_create_fn ioctx_create; ioctx_destroy_fn ioctx_destroy; cluster_stat_fn cluster_stat; pool_stat_fn ioctx_pool_stat;
	pool_create_fn pool_create; pool_delete_fn pool_delete; blocklist_add_fn blocklist_add; getaddrs_fn getaddrs; buffer_free_fn buffer_free;
	application_enable_fn application_enable; application_list_fn application_list; application_metadata_get_fn application_metadata_get;
	application_metadata_set_fn application_metadata_set; application_metadata_remove_fn application_metadata_remove; application_metadata_list_fn application_metadata_list;
	command_fn mon_command; command_fn mgr_command; osd_command_fn osd_command; pg_command_fn pg_command; inconsistent_pg_list_fn inconsistent_pg_list;
};

static void bind_symbol(void *library, const char *name, void *target, size_t size) {
	void *symbol = dlsym(library, name);
	if (symbol == NULL || size != sizeof(symbol)) { fprintf(stderr, "native p11: bind %s: %s\n", name, dlerror()); exit(2); }
	memcpy(target, &symbol, size);
}
#define BIND(api, field, symbol) bind_symbol((api)->library, "rados_" #symbol, &(api)->field, sizeof((api)->field))

static struct api load_api(void) {
	struct api api = {0};
	api.library = dlopen("librados.so.2", RTLD_NOW | RTLD_LOCAL);
	if (api.library == NULL) { fprintf(stderr, "native p11: %s\n", dlerror()); exit(2); }
	BIND(&api, create2, create2); BIND(&api, conf_read_file, conf_read_file); BIND(&api, conf_set, conf_set); BIND(&api, connect, connect); BIND(&api, shutdown, shutdown);
	BIND(&api, ioctx_create, ioctx_create); BIND(&api, ioctx_destroy, ioctx_destroy); BIND(&api, cluster_stat, cluster_stat); BIND(&api, ioctx_pool_stat, ioctx_pool_stat);
	BIND(&api, pool_create, pool_create); BIND(&api, pool_delete, pool_delete); BIND(&api, blocklist_add, blocklist_add); BIND(&api, getaddrs, getaddrs); BIND(&api, buffer_free, buffer_free);
	BIND(&api, application_enable, application_enable); BIND(&api, application_list, application_list); BIND(&api, application_metadata_get, application_metadata_get);
	BIND(&api, application_metadata_set, application_metadata_set); BIND(&api, application_metadata_remove, application_metadata_remove); BIND(&api, application_metadata_list, application_metadata_list);
	BIND(&api, mon_command, mon_command); BIND(&api, mgr_command, mgr_command); BIND(&api, osd_command, osd_command); BIND(&api, pg_command, pg_command); BIND(&api, inconsistent_pg_list, inconsistent_pg_list);
	return api;
}

static void check(int result, const char *operation) {
	if (result < 0) { fprintf(stderr, "native p11: %s: %s (%d)\n", operation, strerror(-result), result); exit(1); }
}

static void command(struct api *api, command_fn function, rados_t cluster, const char *json, const char *operation) {
	const char *commands[] = {json}; char *output = NULL, *status = NULL; size_t output_len = 0, status_len = 0;
	check(function(cluster, commands, 1, NULL, 0, &output, &output_len, &status, &status_len), operation);
	if (output_len == 0 && status_len == 0) { fprintf(stderr, "native p11: empty %s result\n", operation); exit(1); }
	api->buffer_free(output); api->buffer_free(status);
}

static void target_commands(struct api *api, rados_t cluster, const char *pg) {
	const char *osd_commands[] = {"{\"prefix\":\"version\"}"}; char *output = NULL, *status = NULL; size_t output_len = 0, status_len = 0;
	check(api->osd_command(cluster, 0, osd_commands, 1, NULL, 0, &output, &output_len, &status, &status_len), "OSD command");
	if (output_len == 0 && status_len == 0) { fprintf(stderr, "native p11: empty OSD command result\n"); exit(1); }
	api->buffer_free(output); api->buffer_free(status); output = NULL; status = NULL; output_len = 0; status_len = 0;
	char pg_json[256] = {0};
	if (snprintf(pg_json, sizeof(pg_json), "{\"prefix\":\"pg\",\"cmd\":\"query\",\"pgid\":\"%s\"}", pg) < 0) { fprintf(stderr, "native p11: format PG command\n"); exit(1); }
	const char *pg_commands[] = {pg_json};
	check(api->pg_command(cluster, pg, pg_commands, 1, NULL, 0, &output, &output_len, &status, &status_len), "PG command");
	if (output_len == 0) { fprintf(stderr, "native p11: empty PG command output\n"); exit(1); }
	api->buffer_free(output); api->buffer_free(status);
}

static int string_list_contains(const char *values, size_t length, const char *expected) {
	for (size_t offset = 0; offset < length;) {
		size_t item_len = strnlen(values + offset, length - offset);
		if (item_len == strlen(expected) && memcmp(values + offset, expected, item_len) == 0) return 1;
		offset += item_len + 1;
	}
	return 0;
}

static void application_metadata(struct api *api, rados_ioctx_t io) {
	check(api->application_enable(io, "nativeapp", 0), "application enable");
	check(api->application_metadata_set(io, "nativeapp", "owner", "native"), "application metadata set");
	char applications[256] = {0}; size_t applications_len = sizeof(applications);
	check(api->application_list(io, applications, &applications_len), "application list");
	if (!string_list_contains(applications, applications_len, "nativeapp")) { fprintf(stderr, "native p11: application absent\n"); exit(1); }
	char value[256] = {0}; size_t value_len = sizeof(value);
	check(api->application_metadata_get(io, "nativeapp", "owner", value, &value_len), "application metadata get");
	if (value_len != 7 || memcmp(value, "native", 7) != 0) { fprintf(stderr, "native p11: metadata value mismatch\n"); exit(1); }
	char keys[256] = {0}, values[256] = {0}; size_t keys_len = sizeof(keys), values_len = sizeof(values);
	check(api->application_metadata_list(io, "nativeapp", keys, &keys_len, values, &values_len), "application metadata list");
	if (!string_list_contains(keys, keys_len, "owner") || !string_list_contains(values, values_len, "native")) { fprintf(stderr, "native p11: metadata list mismatch\n"); exit(1); }
	check(api->application_metadata_remove(io, "nativeapp", "owner"), "application metadata remove");
}

int main(int argc, char **argv) {
	if (argc != 5) { fprintf(stderr, "usage: %s CONF KEYRING PG POOL_ID\n", argv[0]); return 2; }
	struct api api = load_api(); rados_t cluster = NULL; rados_ioctx_t data = NULL, app = NULL;
	check(api.create2(&cluster, "ceph", "client.p11-admin", 0), "create");
	check(api.conf_read_file(cluster, argv[1]), "config"); check(api.conf_set(cluster, "keyring", argv[2]), "keyring"); check(api.connect(cluster), "connect");
	struct rados_cluster_stat_t cluster_stats = {0}; check(api.cluster_stat(cluster, &cluster_stats), "cluster stat");
	if (cluster_stats.kb == 0 || cluster_stats.kb_avail > cluster_stats.kb) { fprintf(stderr, "native p11: invalid cluster stats\n"); exit(1); }
	check(api.ioctx_create(cluster, "p11-data", &data), "open data pool"); struct rados_pool_stat_t pool_stats = {0}; check(api.ioctx_pool_stat(data, &pool_stats), "pool stat");
	command(&api, api.mon_command, cluster, "{\"prefix\":\"status\",\"format\":\"json\"}", "monitor command");
	command(&api, api.mgr_command, cluster, "{\"prefix\":\"pg dump\",\"format\":\"json\"}", "manager command"); target_commands(&api, cluster, argv[3]);
	check(api.pool_create(cluster, "p11-native-created"), "pool create"); check(api.pool_delete(cluster, "p11-native-created"), "pool delete");
	check(api.ioctx_create(cluster, "p11-native-app", &app), "open application pool"); application_metadata(&api, app);
	char *addresses = NULL; check(api.getaddrs(cluster, &addresses), "get addresses");
	if (addresses == NULL || addresses[0] == '\0') { fprintf(stderr, "native p11: empty addresses\n"); exit(1); }
	api.buffer_free(addresses); char block_address[] = "v2:192.0.2.253:6800/1"; check(api.blocklist_add(cluster, block_address, 60), "blocklist add");
	char inconsistent[65536] = {0}; int inconsistent_len = api.inconsistent_pg_list(cluster, strtoll(argv[4], NULL, 10), inconsistent, sizeof(inconsistent)); check(inconsistent_len, "inconsistent PG list");
	if (inconsistent_len == 0) { fprintf(stderr, "native p11: empty inconsistent PG response\n"); exit(1); }
	printf("{\"cluster_stats\":true,\"pool_stats\":true,\"monitor_command\":true,\"manager_command\":true,\"osd_command\":true,\"pg_command\":true,\"pool_create_delete\":true,\"application_metadata\":true,\"session_addresses\":true,\"blocklist\":true,\"inconsistent_pgs\":true}\n");
	api.ioctx_destroy(app); api.ioctx_destroy(data); api.shutdown(cluster); dlclose(api.library); return 0;
}
