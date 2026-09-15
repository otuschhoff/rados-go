#include <dlfcn.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef void *rados_t;
typedef void *rados_ioctx_t;

typedef int (*create2_fn)(rados_t *, const char *, const char *, uint64_t);
typedef int (*conf_read_file_fn)(rados_t, const char *);
typedef int (*conf_set_fn)(rados_t, const char *, const char *);
typedef int (*connect_fn)(rados_t);
typedef int (*ioctx_create_fn)(rados_t, const char *, rados_ioctx_t *);
typedef void (*ioctx_destroy_fn)(rados_ioctx_t);
typedef void (*shutdown_fn)(rados_t);
typedef int (*write_full_fn)(rados_ioctx_t, const char *, const char *, size_t);
typedef int (*write_fn)(rados_ioctx_t, const char *, const char *, size_t, uint64_t);
typedef int (*append_fn)(rados_ioctx_t, const char *, const char *, size_t);
typedef int (*trunc_fn)(rados_ioctx_t, const char *, uint64_t);
typedef int (*remove_fn)(rados_ioctx_t, const char *);
typedef int (*read_fn)(rados_ioctx_t, const char *, char *, size_t, uint64_t);

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
	write_fn write;
	append_fn append;
	trunc_fn trunc;
	remove_fn remove;
	read_fn read;
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
	BIND(&result, shutdown);
	BIND(&result, write_full);
	BIND(&result, write);
	BIND(&result, append);
	BIND(&result, trunc);
	BIND(&result, remove);
	BIND(&result, read);
	return result;
}

static void check(int result, const char *operation) {
	if (result < 0) {
		fprintf(stderr, "native driver: %s: %s (%d)\n", operation, strerror(-result), result);
		exit(1);
	}
}

static void expect_bytes(struct api *api, rados_ioctx_t ioctx, const char *object, const unsigned char *expected, size_t length) {
	unsigned char buffer[64] = {0};
	int result = api->read(ioctx, object, (char *)buffer, sizeof(buffer), 0);
	if (result != (int)length || memcmp(buffer, expected, length) != 0) {
		fprintf(stderr, "native driver: %s bytes differ, length=%d want=%zu\n", object, result, length);
		exit(1);
	}
}

static void seed(struct api *api, rados_ioctx_t ioctx) {
	static const unsigned char expected[] = {'a', 'Z', 0, 0, 'e', 'f', 'g'};
	static const unsigned char zeros[] = {0, 0};
	check(api->write_full(ioctx, "native-crud", "abcdef", 6), "write-full");
	check(api->write(ioctx, "native-crud", "Z", 1, 1), "write");
	check(api->append(ioctx, "native-crud", "gh", 2), "append");
	check(api->write(ioctx, "native-crud", (const char *)zeros, sizeof(zeros), 2), "zero bytes");
	check(api->trunc(ioctx, "native-crud", 7), "truncate");
	expect_bytes(api, ioctx, "native-crud", expected, sizeof(expected));
	check(api->remove(ioctx, "native-crud"), "remove");
	if (api->read(ioctx, "native-crud", (char *)zeros, 1, 0) != -ENOENT) {
		fprintf(stderr, "native driver: removed object did not return ENOENT\n");
		exit(1);
	}
	check(api->write_full(ioctx, "mixed-crud", "abcdef", 6), "seed mixed CRUD");
	printf("{\"native_crud\":true,\"mixed_seed\":true}\n");
}

static void verify(struct api *api, rados_ioctx_t ioctx) {
	static const unsigned char expected[] = {'a', 'Z', 0, 0, 'e', 'f', 'g'};
	expect_bytes(api, ioctx, "mixed-crud", expected, sizeof(expected));
	expect_bytes(api, ioctx, "go-parity", expected, sizeof(expected));
	expect_bytes(api, ioctx, "remap-append", (const unsigned char *)"base!", 5);
	printf("{\"mixed_go_native\":true,\"go_write_native_read\":true,\"remap_append_once\":true}\n");
}

int main(int argc, char **argv) {
	if (argc != 5 || (strcmp(argv[1], "seed") != 0 && strcmp(argv[1], "verify") != 0)) {
		fprintf(stderr, "usage: %s seed|verify CONF KEYRING POOL\n", argv[0]);
		return 2;
	}
	struct api api = load_api();
	rados_t cluster = NULL;
	rados_ioctx_t ioctx = NULL;
	check(api.create2(&cluster, "ceph", "client.admin", 0), "create cluster");
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
