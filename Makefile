SHELL := /bin/sh
CEPH_SOURCE ?= /tmp/go-librados-ceph

.PHONY: inventory verify-p00 p00-preflight p00-smoke

inventory:
	GO111MODULE=off go run ./tools/api-inventory \
		-c "$(CEPH_SOURCE)/src/include/rados/librados.h" \
		-cpp "$(CEPH_SOURCE)/src/include/rados/librados.hpp" \
		-output docs/p00/api-inventory.csv

verify-p00:
	GO111MODULE=off go test ./tools/api-inventory ./tools/p00-verify
	GO111MODULE=off go run ./tools/p00-verify
	./integration/p00/preflight.sh

p00-preflight:
	./integration/p00/preflight.sh --require-linux-host

p00-smoke:
	./integration/p00/run.sh