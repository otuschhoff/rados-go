SHELL := /bin/sh
CEPH_SOURCE ?= /tmp/go-librados-ceph

.PHONY: inventory verify-p00 verify-p01 verify-p01-all quality-p01 reproduce-p01 unit-p01 differential-p01 integration-p01 cross-p01 fuzz-p01 fuzz-p01-nightly verify-p02 verify-p02-all quality-p02 reproduce-p02 unit-p02 differential-p02 integration-p02 cross-p02 fuzz-p02 fuzz-p02-nightly p00-preflight p00-smoke

inventory:
	GO111MODULE=off go run ./tools/api-inventory \
		-c "$(CEPH_SOURCE)/src/include/rados/librados.h" \
		-cpp "$(CEPH_SOURCE)/src/include/rados/librados.hpp" \
		-output docs/p00/api-inventory.csv

verify-p00:
	GO111MODULE=off go test ./tools/api-inventory ./tools/p00-verify
	GO111MODULE=off go run ./tools/p00-verify
	./integration/p00/preflight.sh

verify-p01: verify-p00
	test -z "$$(gofmt -l $$(find . -name '*.go' -not -path './.git/*'))"
	$(MAKE) unit-p01
	$(MAKE) differential-p01
	$(MAKE) integration-p01
	CGO_ENABLED=0 go build ./...
	go vet ./...
	go mod verify
	CGO_ENABLED=0 go run ./tools/p01-verify
	$(MAKE) cross-p01

verify-p01-all: verify-p01 quality-p01 reproduce-p01 fuzz-p01

quality-p01:
	test -z "$$(go list -deps -f '{{if .CgoFiles}}{{.ImportPath}}{{end}}' ./...)"
	test "$$(go list -m all | wc -l | tr -d ' ')" -eq 1
	go test -race ./...
	go run "honnef.co/go/tools/cmd/staticcheck@$$(jq -r '.quality_tools.staticcheck' docs/p01/evidence.json)" ./...
	go run "golang.org/x/vuln/cmd/govulncheck@$$(jq -r '.quality_tools.govulncheck' docs/p01/evidence.json)" ./...

reproduce-p01:
	./integration/p01/reproduce.sh

unit-p01:
	CGO_ENABLED=0 go test . ./internal/encoding ./internal/protocol

differential-p01:
	CGO_ENABLED=0 go test ./internal/protocol -run 'FixtureParity$$' -count=1
	CGO_ENABLED=0 go test ./tools/p01-verify
	CGO_ENABLED=0 go run ./tools/p01-verify

integration-p01:
	@printf '%s\n' 'P01 has no cluster integration surface; native fixture parity is differential-p01.'

cross-p01:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build ./...
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build ./...
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build ./...
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build ./...

fuzz-p01:
	CGO_ENABLED=0 go test ./internal/encoding -run '^$$' -fuzz '^FuzzDecoder$$' -fuzztime=60s
	CGO_ENABLED=0 go test ./internal/encoding -run '^$$' -fuzz '^FuzzVersionedEnvelope$$' -fuzztime=60s
	CGO_ENABLED=0 go test ./internal/protocol -run '^$$' -fuzz '^FuzzEntityAddr$$' -fuzztime=60s
	CGO_ENABLED=0 go test ./internal/protocol -run '^$$' -fuzz '^FuzzEntityAddrVec$$' -fuzztime=60s

fuzz-p01-nightly:
	CGO_ENABLED=0 go test ./internal/encoding -run '^$$' -fuzz '^FuzzDecoder$$' -fuzztime=5m
	CGO_ENABLED=0 go test ./internal/encoding -run '^$$' -fuzz '^FuzzVersionedEnvelope$$' -fuzztime=5m
	CGO_ENABLED=0 go test ./internal/protocol -run '^$$' -fuzz '^FuzzEntityAddr$$' -fuzztime=5m
	CGO_ENABLED=0 go test ./internal/protocol -run '^$$' -fuzz '^FuzzEntityAddrVec$$' -fuzztime=5m

verify-p02: verify-p01
	test -z "$$(gofmt -l $$(find . -name '*.go' -not -path './.git/*'))"
	CGO_ENABLED=0 go test ./...
	$(MAKE) unit-p02
	$(MAKE) differential-p02
	$(MAKE) integration-p02
	CGO_ENABLED=0 go build ./...
	go vet ./...
	go mod verify
	CGO_ENABLED=0 go run ./tools/p02-verify
	$(MAKE) cross-p02

verify-p02-all: verify-p02 quality-p02 reproduce-p02 fuzz-p02

quality-p02:
	test -z "$$(go list -deps -f '{{if .CgoFiles}}{{.ImportPath}}{{end}}' ./...)"
	test "$$(go list -m all | wc -l | tr -d ' ')" -eq 1
	go test -race ./...
	go run "honnef.co/go/tools/cmd/staticcheck@$$(jq -r '.quality_tools.staticcheck' docs/p01/evidence.json)" ./...
	go run "golang.org/x/vuln/cmd/govulncheck@$$(jq -r '.quality_tools.govulncheck' docs/p01/evidence.json)" ./...

reproduce-p02:
	./integration/p02/reproduce.sh

unit-p02:
	CGO_ENABLED=0 go test ./internal/msgr ./tools/p02-verify

differential-p02:
	CGO_ENABLED=0 go test ./internal/msgr -run '^TestP02(BannerFixture|CRCFixtures|SecureFixtures)$$' -count=1
	CGO_ENABLED=0 go test ./tools/p02-verify
	CGO_ENABLED=0 go run ./tools/p02-verify

integration-p02:
	@printf '%s\n' 'P02 exercises synthetic protocol peers only; it has no live authenticated Ceph connectivity claim.'

cross-p02:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build ./...
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build ./...
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build ./...
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build ./...

fuzz-p02:
	CGO_ENABLED=0 go test ./internal/msgr -run '^$$' -fuzz '^FuzzBanner$$' -fuzztime=60s
	CGO_ENABLED=0 go test ./internal/msgr -run '^$$' -fuzz '^FuzzCRCFrame$$' -fuzztime=60s
	CGO_ENABLED=0 go test ./internal/msgr -run '^$$' -fuzz '^FuzzSecureFrame$$' -fuzztime=60s
	CGO_ENABLED=0 go test ./internal/msgr -run '^$$' -fuzz '^FuzzControlPayload$$' -fuzztime=60s
	CGO_ENABLED=0 go test ./internal/msgr -run '^$$' -fuzz '^FuzzMessageFrame$$' -fuzztime=60s
	CGO_ENABLED=0 go test ./internal/msgr -run '^$$' -fuzz '^FuzzSessionScript$$' -fuzztime=60s

fuzz-p02-nightly:
	CGO_ENABLED=0 go test ./internal/msgr -run '^$$' -fuzz '^FuzzBanner$$' -fuzztime=5m
	CGO_ENABLED=0 go test ./internal/msgr -run '^$$' -fuzz '^FuzzCRCFrame$$' -fuzztime=5m
	CGO_ENABLED=0 go test ./internal/msgr -run '^$$' -fuzz '^FuzzSecureFrame$$' -fuzztime=5m
	CGO_ENABLED=0 go test ./internal/msgr -run '^$$' -fuzz '^FuzzControlPayload$$' -fuzztime=5m
	CGO_ENABLED=0 go test ./internal/msgr -run '^$$' -fuzz '^FuzzMessageFrame$$' -fuzztime=5m
	CGO_ENABLED=0 go test ./internal/msgr -run '^$$' -fuzz '^FuzzSessionScript$$' -fuzztime=5m

p00-preflight:
	./integration/p00/preflight.sh --require-linux-host

p00-smoke:
	./integration/p00/run.sh