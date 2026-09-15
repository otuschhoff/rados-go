#include <dlfcn.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef void *rados_t;
typedef void *rados_ioctx_t;
typedef void *rados_write_op_t;
typedef void *rados_read_op_t;
typedef void *rados_omap_iter_t;
typedef void *rados_list_ctx_t;

typedef int (*create2_fn)(rados_t *, const char *, const char *, uint64_t);
typedef int (*conf_read_file_fn)(rados_t, const char *);
typedef int (*conf_set_fn)(rados_t, const char *, const char *);
typedef int (*connect_fn)(rados_t);
typedef int (*ioctx_create_fn)(rados_t, const char *, rados_ioctx_t *);
typedef void (*ioctx_destroy_fn)(rados_ioctx_t);
typedef void (*ioctx_set_namespace_fn)(rados_ioctx_t, const char *);
typedef void (*shutdown_fn)(rados_t);
typedef int (*read_fn)(rados_ioctx_t, const char *, char *, size_t, uint64_t);
typedef int (*getxattr_fn)(rados_ioctx_t, const char *, const char *, char *, size_t);
typedef rados_write_op_t (*create_write_op_fn)(void);
typedef void (*release_write_op_fn)(rados_write_op_t);
typedef void (*write_op_write_full_fn)(rados_write_op_t, const char *, size_t);
typedef void (*write_op_setxattr_fn)(rados_write_op_t, const char *, const char *, size_t);
typedef void (*write_op_omap_set2_fn)(rados_write_op_t, const char *const *, const char *const *, const size_t *, const size_t *, size_t);
typedef int (*write_op_operate_fn)(rados_write_op_t, rados_ioctx_t, const char *, void *, int);
typedef rados_read_op_t (*create_read_op_fn)(void);
typedef void (*release_read_op_fn)(rados_read_op_t);
typedef void (*read_op_omap_get_vals2_fn)(rados_read_op_t, const char *, const char *, uint64_t, rados_omap_iter_t *, unsigned char *, int *);
typedef int (*read_op_operate_fn)(rados_read_op_t, rados_ioctx_t, const char *, int);
typedef int (*omap_get_next2_fn)(rados_omap_iter_t, char **, char **, size_t *, size_t *);
typedef void (*omap_get_end_fn)(rados_omap_iter_t);
typedef int (*nobjects_list_open_fn)(rados_ioctx_t, rados_list_ctx_t *);
typedef int (*nobjects_list_next2_fn)(rados_list_ctx_t, const char **, const char **, const char **, size_t *, size_t *, size_t *);
typedef void (*nobjects_list_close_fn)(rados_list_ctx_t);

struct api {
	void *library;
	create2_fn create2;
	conf_read_file_fn conf_read_file;
	conf_set_fn conf_set;
	connect_fn connect;
	ioctx_create_fn ioctx_create;
	ioctx_destroy_fn ioctx_destroy;
	ioctx_set_namespace_fn ioctx_set_namespace;
	shutdown_fn shutdown;
	read_fn read;
	getxattr_fn getxattr;
	create_write_op_fn create_write_op;
	release_write_op_fn release_write_op;
	write_op_write_full_fn write_op_write_full;
	write_op_setxattr_fn write_op_setxattr;
	write_op_omap_set2_fn write_op_omap_set2;
	write_op_operate_fn write_op_operate;
	create_read_op_fn create_read_op;
	release_read_op_fn release_read_op;
	read_op_omap_get_vals2_fn read_op_omap_get_vals2;
	read_op_operate_fn read_op_operate;
	omap_get_next2_fn omap_get_next2;
	omap_get_end_fn omap_get_end;
	nobjects_list_open_fn nobjects_list_open;
	nobjects_list_next2_fn nobjects_list_next2;
	nobjects_list_close_fn nobjects_list_close;
};

static void bind_symbol(void *library, const char *name, void *target, size_t size) {
	void *symbol = dlsym(library, name);
	if (symbol == NULL || size != sizeof(symbol)) {
		fprintf(stderr, "native driver: bind %s: %s\n", name, dlerror());
		exit(2);
	}
	memcpy(target, &symbol, size);
}

#define BIND(api, name) bind_symbol((api)->library, "rados_" #name, &(api)->name, sizeof((api)->name))

static struct api load_api(void) {
	struct api result = {0};
	result.library = dlopen("librados.so.2", RTLD_NOW | RTLD_LOCAL);
	if (result.library == NULL) {
		fprintf(stderr, "native driver: load librados.so.2: %s\n", dlerror());
		exit(2);
	}
	BIND(&result, create2);
	BIND(&result, conf_read_file);
	BIND(&result, conf_set);
	BIND(&result, connect);
	BIND(&result, ioctx_create);
	BIND(&result, ioctx_destroy);
	BIND(&result, ioctx_set_namespace);
	BIND(&result, shutdown);
	BIND(&result, read);
	BIND(&result, getxattr);
	BIND(&result, create_write_op);
	BIND(&result, release_write_op);
	BIND(&result, write_op_write_full);
	BIND(&result, write_op_setxattr);
	BIND(&result, write_op_omap_set2);
	BIND(&result, write_op_operate);
	BIND(&result, create_read_op);
	BIND(&result, release_read_op);
	BIND(&result, read_op_omap_get_vals2);
	BIND(&result, read_op_operate);
	BIND(&result, omap_get_next2);
	BIND(&result, omap_get_end);
	BIND(&result, nobjects_list_open);
	BIND(&result, nobjects_list_next2);
	BIND(&result, nobjects_list_close);
	return result;
}

static void check(int result, const char *operation) {
	if (result < 0) {
		fprintf(stderr, "native driver: %s: %s (%d)\n", operation, strerror(-result), result);
		exit(1);
	}
}

static void seed(struct api *api, rados_ioctx_t ioctx) {
	static const unsigned char binary[] = {0, 1, 0xff, 2};
	static const char key_zero[] = {0, 'a'};
	static const char key_b[] = {'b'};
	static const unsigned char key_ff[] = {0xff};
	static const char value_zero[] = {'v', 0, '0'};
	static const char value_b[] = {'v', 'b'};
	static const char value_ff[] = {'v', 'f'};
	const char *keys[] = {key_zero, key_b, (const char *)key_ff};
	const char *values[] = {value_zero, value_b, value_ff};
	size_t key_lengths[] = {sizeof(key_zero), sizeof(key_b), sizeof(key_ff)};
	size_t value_lengths[] = {sizeof(value_zero), sizeof(value_b), sizeof(value_ff)};
	rados_write_op_t operation = api->create_write_op();
	if (operation == NULL) {
		fprintf(stderr, "native driver: create write operation failed\n");
		exit(1);
	}
	api->write_op_write_full(operation, "native", 6);
	api->write_op_setxattr(operation, "alpha", "first", 5);
	api->write_op_setxattr(operation, "binary", (const char *)binary, sizeof(binary));
	api->write_op_omap_set2(operation, keys, values, key_lengths, value_lengths, 3);
	check(api->write_op_operate(operation, ioctx, "native-metadata", NULL, 0), "seed metadata compound");
	api->release_write_op(operation);
	printf("{\"native_binary_metadata\":true,\"native_compound_seed\":true}\n");
}

static int contains_object(struct api *api, rados_ioctx_t ioctx, const char *wanted) {
	rados_list_ctx_t list = NULL;
	check(api->nobjects_list_open(ioctx, &list), "open object list");
	int found = 0;
	for (;;) {
		const char *entry = NULL;
		const char *key = NULL;
		const char *namespace_name = NULL;
		size_t entry_length = 0;
		size_t key_length = 0;
		size_t namespace_length = 0;
		int result = api->nobjects_list_next2(list, &entry, &key, &namespace_name, &entry_length, &key_length, &namespace_length);
		if (result == -ENOENT) {
			break;
		}
		check(result, "next object");
		if (entry_length == strlen(wanted) && memcmp(entry, wanted, entry_length) == 0) {
			found = 1;
		}
	}
	api->nobjects_list_close(list);
	return found;
}

static void verify(struct api *api, rados_ioctx_t ioctx) {
	static const unsigned char expected_xattr[] = {0xfe, 0, 0xfd};
	static const unsigned char expected_key[] = {0, 'g'};
	static const unsigned char expected_value[] = {1, 0, 2};
	unsigned char buffer[32] = {0};
	int length = api->getxattr(ioctx, "go-metadata", "binary", (char *)buffer, sizeof(buffer));
	if (length != (int)sizeof(expected_xattr) || memcmp(buffer, expected_xattr, sizeof(expected_xattr)) != 0) {
		fprintf(stderr, "native driver: Go xattr mismatch length=%d\n", length);
		exit(1);
	}
	rados_read_op_t operation = api->create_read_op();
	rados_omap_iter_t iterator = NULL;
	unsigned char more = 0;
	int operation_result = 0;
	api->read_op_omap_get_vals2(operation, "", "", 16, &iterator, &more, &operation_result);
	check(api->read_op_operate(operation, ioctx, "go-metadata", 0), "read Go OMAP");
	check(operation_result, "Go OMAP suboperation");
	int found_binary = 0;
	int found_z = 0;
	for (;;) {
		char *key = NULL;
		char *value = NULL;
		size_t key_length = 0;
		size_t value_length = 0;
		check(api->omap_get_next2(iterator, &key, &value, &key_length, &value_length), "next OMAP value");
		if (key == NULL) {
			break;
		}
		if (key_length == sizeof(expected_key) && value_length == sizeof(expected_value) && memcmp(key, expected_key, key_length) == 0 && memcmp(value, expected_value, value_length) == 0) {
			found_binary = 1;
		}
		if (key_length == 1 && value_length == 4 && key[0] == 'z' && memcmp(value, "last", 4) == 0) {
			found_z = 1;
		}
	}
	api->omap_get_end(iterator);
	api->release_read_op(operation);
	if (!found_binary || !found_z || more) {
		fprintf(stderr, "native driver: Go OMAP mismatch binary=%d z=%d more=%u\n", found_binary, found_z, more);
		exit(1);
	}
	if (!contains_object(api, ioctx, "enum-a") || contains_object(api, ioctx, "ns-a")) {
		fprintf(stderr, "native driver: default namespace listing mismatch\n");
		exit(1);
	}
	api->ioctx_set_namespace(ioctx, "space");
	if (!contains_object(api, ioctx, "ns-a") || contains_object(api, ioctx, "enum-a")) {
		fprintf(stderr, "native driver: named namespace listing mismatch\n");
		exit(1);
	}
	printf("{\"go_binary_metadata\":true,\"go_omap_native_read\":true,\"go_enumeration_native_read\":true,\"namespace_filtering\":true}\n");
}

int main(int argc, char **argv) {
	if (argc != 5 || (strcmp(argv[1], "seed") != 0 && strcmp(argv[1], "verify") != 0)) {
		fprintf(stderr, "usage: %s seed|verify CONF KEYRING POOL\n", argv[0]);
		return 2;
	}
	struct api api = load_api();
	rados_t cluster = NULL;
	rados_ioctx_t ioctx = NULL;
	check(api.create2(&cluster, "ceph", "client.p08", 0), "create cluster");
	check(api.conf_read_file(cluster, argv[2]), "read config");
	check(api.conf_set(cluster, "keyring", argv[3]), "set keyring");
	check(api.connect(cluster), "connect");
	check(api.ioctx_create(cluster, argv[4], &ioctx), "open pool");
	if (strcmp(argv[1], "seed") == 0) {
		seed(&api, ioctx);
	} else {
		verify(&api, ioctx);
	}
	api.ioctx_destroy(ioctx);
	api.shutdown(cluster);
	dlclose(api.library);
	return 0;
}