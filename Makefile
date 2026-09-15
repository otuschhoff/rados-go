SHELL := /bin/sh
CEPH_SOURCE ?= /tmp/go-librados-ceph

.PHONY: inventory verify-p00 verify-p01 verify-p01-all quality-p01 reproduce-p01 unit-p01 differential-p01 integration-p01 cross-p01 fuzz-p01 fuzz-p01-nightly verify-p02 verify-p02-all quality-p02 reproduce-p02 reproduce-p02-upstream unit-p02 differential-p02 integration-p02 cross-p02 fuzz-p02 fuzz-p02-nightly verify-p03 verify-p03-all verify-manifests quality-p03 reproduce-p03-fixtures unit-p03 differential-p03 integration-p03 cross-p03 fuzz-p03 fuzz-p03-nightly verify-p04 verify-p04-all quality-p04 reproduce-p04-fixtures unit-p04 differential-p04 integration-p04 cross-p04 fuzz-p04 fuzz-p04-nightly verify-p05 verify-p05-all quality-p05 reproduce-p05 unit-p05 differential-p05 integration-p05 cross-p05 fuzz-p05 fuzz-p05-nightly verify-p06 verify-p06-all quality-p06 unit-p06 integration-p06 cross-p06 fuzz-p06 fuzz-p06-nightly p00-preflight p00-smoke

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
	test -z "$$(CGO_ENABLED=0 go list -deps -f '{{if .CgoFiles}}{{.ImportPath}}{{end}}' ./...)"
	test -z "$$(go list -deps -f '{{with .Module}}{{if ne .Path "github.com/otuschhoff/go-librados"}}{{.Path}}{{end}}{{end}}' . ./internal/encoding ./internal/protocol | sort -u)"
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
	test -z "$$(CGO_ENABLED=0 go list -deps -f '{{if .CgoFiles}}{{.ImportPath}}{{end}}' ./...)"
	test -z "$$(go list -deps -f '{{with .Module}}{{if ne .Path "github.com/otuschhoff/go-librados"}}{{.Path}}{{end}}{{end}}' . ./internal/encoding ./internal/protocol ./internal/msgr | sort -u)"
	go test -race ./...
	go run "honnef.co/go/tools/cmd/staticcheck@$$(jq -r '.quality_tools.staticcheck' docs/p01/evidence.json)" ./...
	go run "golang.org/x/vuln/cmd/govulncheck@$$(jq -r '.quality_tools.govulncheck' docs/p01/evidence.json)" ./...

reproduce-p02:
	./integration/p02/reproduce.sh
	$(MAKE) reproduce-p02-upstream

reproduce-p02-upstream:
	./integration/p02/reproduce-upstream.sh

unit-p02:
	CGO_ENABLED=0 go test ./internal/msgr ./tools/p02-verify

differential-p02:
	CGO_ENABLED=0 go test ./internal/msgr -run '^TestP02(BannerFixture|CRCFixtures|SecureFixtures|UpstreamFrameAssembler.*Fixtures)$$' -count=1
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

verify-p03: verify-p02
	test -z "$$(gofmt -l $$(find . -name '*.go' -not -path './.git/*'))"
	$(MAKE) verify-manifests
	$(MAKE) unit-p03
	$(MAKE) differential-p03
	CGO_ENABLED=0 go build ./...
	go vet ./...
	go mod verify
	CGO_ENABLED=0 go run ./tools/p03-verify
	$(MAKE) cross-p03

verify-manifests:
	npx --yes --package=ajv-cli@5.0.0 --package=ajv-formats@3.0.1 ajv validate --spec=draft2020 -c ajv-formats -s testdata/manifest.schema.json -d 'testdata/p01/*.bin.json' -d 'testdata/p02/*.bin.json' -d 'testdata/p02/upstream/*.bin.json' -d 'testdata/p03/*.manifest.json' -d 'testdata/p04/*.manifest.json' -d 'testdata/p05/*.manifest.json'
	npx --yes --package=ajv-cli@5.0.0 --package=ajv-formats@3.0.1 ajv validate --spec=draft2020 -c ajv-formats -s integration/p03/report.schema.json -d docs/p03/integration-report.json
	npx --yes --package=ajv-cli@5.0.0 --package=ajv-formats@3.0.1 ajv validate --spec=draft2020 -c ajv-formats -s integration/p04/report.schema.json -d docs/p04/integration-report.json
	npx --yes --package=ajv-cli@5.0.0 --package=ajv-formats@3.0.1 ajv validate --spec=draft2020 -c ajv-formats -s integration/p06/report.schema.json -d docs/p06/integration-report.json

verify-p03-all:
	$(MAKE) quality-p03
	$(MAKE) reproduce-p03-fixtures
	$(MAKE) integration-p03
	$(MAKE) verify-p03
	$(MAKE) fuzz-p03

quality-p03:
	test -z "$$(CGO_ENABLED=0 go list -deps -f '{{if .CgoFiles}}{{.ImportPath}}{{end}}' ./...)"
	go mod verify
	go list -m -f '{{.Path}} {{.Version}}' all | diff - docs/p03/modules.txt
	go test -race ./...
	go run "honnef.co/go/tools/cmd/staticcheck@$$(jq -r '.quality_tools.staticcheck' docs/p01/evidence.json)" ./...
	go run "golang.org/x/vuln/cmd/govulncheck@$$(jq -r '.quality_tools.govulncheck' docs/p01/evidence.json)" ./...

reproduce-p03-fixtures:
	./integration/p03/reproduce-fixtures.sh

unit-p03:
	CGO_ENABLED=0 go test ./internal/cephx ./internal/msgr ./tools/p03-verify

differential-p03:
	CGO_ENABLED=0 go test ./internal/cephx -run '^TestP03(CephDencoder|Crypto)FixtureParity$$' -count=1
	CGO_ENABLED=0 go run ./tools/p03-verify

integration-p03:
	./integration/p03/reproduce.sh
	CGO_ENABLED=0 go run ./tools/p03-verify

cross-p03:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build ./...
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build ./...
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build ./...
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build ./...

fuzz-p03:
	CGO_ENABLED=0 go test ./internal/cephx -run '^$$' -fuzz '^FuzzParseKey$$' -fuzztime=60s
	CGO_ENABLED=0 go test ./internal/cephx -run '^$$' -fuzz '^FuzzParseKeyring$$' -fuzztime=60s
	CGO_ENABLED=0 go test ./internal/cephx -run '^$$' -fuzz '^FuzzParseServerChallenge$$' -fuzztime=60s
	CGO_ENABLED=0 go test ./internal/cephx -run '^$$' -fuzz '^FuzzParseAuthSessionReply$$' -fuzztime=60s
	CGO_ENABLED=0 go test ./internal/cephx -run '^$$' -fuzz '^FuzzVerifyAuthorizerReply$$' -fuzztime=60s
	CGO_ENABLED=0 go test ./internal/cephx -run '^$$' -fuzz '^FuzzAddAuthorizerChallenge$$' -fuzztime=60s

fuzz-p03-nightly:
	CGO_ENABLED=0 go test ./internal/cephx -run '^$$' -fuzz '^FuzzParseKey$$' -fuzztime=5m
	CGO_ENABLED=0 go test ./internal/cephx -run '^$$' -fuzz '^FuzzParseKeyring$$' -fuzztime=5m
	CGO_ENABLED=0 go test ./internal/cephx -run '^$$' -fuzz '^FuzzParseServerChallenge$$' -fuzztime=5m
	CGO_ENABLED=0 go test ./internal/cephx -run '^$$' -fuzz '^FuzzParseAuthSessionReply$$' -fuzztime=5m
	CGO_ENABLED=0 go test ./internal/cephx -run '^$$' -fuzz '^FuzzVerifyAuthorizerReply$$' -fuzztime=5m
	CGO_ENABLED=0 go test ./internal/cephx -run '^$$' -fuzz '^FuzzAddAuthorizerChallenge$$' -fuzztime=5m

verify-p04:
	test -z "$$(gofmt -l $$(find . -name '*.go' -not -path './.git/*'))"
	$(MAKE) verify-manifests
	CGO_ENABLED=0 go test ./...
	$(MAKE) unit-p04
	$(MAKE) differential-p04
	CGO_ENABLED=0 go build ./...
	go vet ./...
	go mod verify
	CGO_ENABLED=0 go run ./tools/p04-verify
	$(MAKE) cross-p04

verify-p04-all: quality-p04 reproduce-p04-fixtures integration-p04 verify-p04 fuzz-p04

quality-p04:
	test -z "$$(go list -deps -f '{{if .CgoFiles}}{{.ImportPath}}{{end}}' ./...)"
	go mod verify
	go test -race ./...
	go run "honnef.co/go/tools/cmd/staticcheck@$$(jq -r '.quality_tools.staticcheck' docs/p01/evidence.json)" ./...
	go run "golang.org/x/vuln/cmd/govulncheck@$$(jq -r '.quality_tools.govulncheck' docs/p01/evidence.json)" ./...

reproduce-p04-fixtures:
	./integration/p04/reproduce-fixtures.sh

unit-p04:
	CGO_ENABLED=0 go test ./internal/maps ./internal/mon ./internal/msgr ./tools/p04-verify

differential-p04:
	CGO_ENABLED=0 go test ./internal/maps -run '^TestP04CephDencoderFixtures$$' -count=1
	CGO_ENABLED=0 go run ./tools/p04-verify

integration-p04:
	./integration/p04/reproduce.sh
	CGO_ENABLED=0 go run ./tools/p04-verify

cross-p04:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build ./...
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build ./...
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build ./...
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build ./...

fuzz-p04:
	CGO_ENABLED=0 go test ./internal/maps -run '^$$' -fuzz '^FuzzDecodeMonMap$$' -fuzztime=60s
	CGO_ENABLED=0 go test ./internal/maps -run '^$$' -fuzz '^FuzzDecodeOSDMap$$' -fuzztime=60s
	CGO_ENABLED=0 go test ./internal/maps -run '^$$' -fuzz '^FuzzDecodeOSDMapIncremental$$' -fuzztime=60s

fuzz-p04-nightly:
	CGO_ENABLED=0 go test ./internal/maps -run '^$$' -fuzz '^FuzzDecodeMonMap$$' -fuzztime=5m
	CGO_ENABLED=0 go test ./internal/maps -run '^$$' -fuzz '^FuzzDecodeOSDMap$$' -fuzztime=5m
	CGO_ENABLED=0 go test ./internal/maps -run '^$$' -fuzz '^FuzzDecodeOSDMapIncremental$$' -fuzztime=5m

verify-p05:
	test -z "$$(gofmt -l $$(find . -name '*.go' -not -path './.git/*'))"
	$(MAKE) verify-manifests
	$(MAKE) unit-p05
	$(MAKE) differential-p05
	CGO_ENABLED=0 go test ./...
	CGO_ENABLED=0 go build ./...
	go vet ./...
	go mod verify
	CGO_ENABLED=0 go run ./tools/p05-verify
	$(MAKE) cross-p05

verify-p05-all: quality-p05 reproduce-p05 verify-p05 fuzz-p05

quality-p05:
	test -z "$$(go list -deps -f '{{if .CgoFiles}}{{.ImportPath}}{{end}}' ./...)"
	go mod verify
	go test -race ./...
	go run "honnef.co/go/tools/cmd/staticcheck@$$(jq -r '.quality_tools.staticcheck' docs/p01/evidence.json)" ./...
	go run "golang.org/x/vuln/cmd/govulncheck@$$(jq -r '.quality_tools.govulncheck' docs/p01/evidence.json)" ./...

reproduce-p05:
	./integration/p05/reproduce.sh

unit-p05:
	CGO_ENABLED=0 go test ./internal/crush ./internal/maps ./tools/p05-verify

differential-p05:
	CGO_ENABLED=0 go test ./internal/crush -run '^TestP05CrushtoolCorpus$$' -count=1
	CGO_ENABLED=0 go test ./internal/maps -run '^Test(P05OSDMapToolObjectCorpus|PlaceObjectP00LiveOracleVector)$$' -count=1
	CGO_ENABLED=0 go run ./tools/p05-verify

integration-p05: reproduce-p05 differential-p05

cross-p05:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build ./...
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build ./...
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build ./...
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build ./...

fuzz-p05:
	CGO_ENABLED=0 go test ./internal/crush -run '^$$' -fuzz '^FuzzDecodeMap$$' -fuzztime=60s
	CGO_ENABLED=0 go test ./internal/crush -run '^$$' -fuzz '^FuzzDecodeAndPlace$$' -fuzztime=60s

fuzz-p05-nightly:
	CGO_ENABLED=0 go test ./internal/crush -run '^$$' -fuzz '^FuzzDecodeMap$$' -fuzztime=5m
	CGO_ENABLED=0 go test ./internal/crush -run '^$$' -fuzz '^FuzzDecodeAndPlace$$' -fuzztime=5m

verify-p06: verify-p03 verify-p04 verify-p05
	test -z "$$(gofmt -l $$(find . -name '*.go' -not -path './.git/*'))"
	$(MAKE) verify-manifests
	$(MAKE) unit-p06
	CGO_ENABLED=0 go test ./...
	CGO_ENABLED=0 go build ./...
	go vet ./...
	go mod verify
	CGO_ENABLED=0 go run ./tools/p06-verify
	$(MAKE) cross-p06

verify-p06-all:
	$(MAKE) quality-p06
	$(MAKE) integration-p06
	$(MAKE) verify-p06
	$(MAKE) fuzz-p06

quality-p06:
	test -z "$$(CGO_ENABLED=0 go list -deps -f '{{if .CgoFiles}}{{.ImportPath}}{{end}}' ./...)"
	go mod verify
	go test -race ./...
	go run "honnef.co/go/tools/cmd/staticcheck@$$(jq -r '.quality_tools.staticcheck' docs/p01/evidence.json)" ./...
	go run "golang.org/x/vuln/cmd/govulncheck@$$(jq -r '.quality_tools.govulncheck' docs/p01/evidence.json)" ./...

unit-p06:
	CGO_ENABLED=0 go test . ./internal/cephx ./internal/maps ./internal/mon ./internal/msgr ./internal/objecter ./internal/osd ./tools/p06-verify

integration-p06:
	./integration/p06/reproduce.sh
	CGO_ENABLED=0 go run ./tools/p06-verify

cross-p06:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build ./...
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build ./...
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build ./...
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build ./...

fuzz-p06:
	CGO_ENABLED=0 go test ./internal/osd -run '^$$' -fuzz '^FuzzDecodeReply$$' -fuzztime=60s
	CGO_ENABLED=0 go test ./internal/osd -run '^$$' -fuzz '^FuzzDecodeBackoff$$' -fuzztime=60s

fuzz-p06-nightly:
	CGO_ENABLED=0 go test ./internal/osd -run '^$$' -fuzz '^FuzzDecodeReply$$' -fuzztime=5m
	CGO_ENABLED=0 go test ./internal/osd -run '^$$' -fuzz '^FuzzDecodeBackoff$$' -fuzztime=5m

p00-preflight:
	./integration/p00/preflight.sh --require-linux-host

p00-smoke:
	./integration/p00/run.sh