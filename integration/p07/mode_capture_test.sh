#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd -P)
eval "$(sed '/^root=/,$d' "$root/integration/p07/reproduce.sh")"
temporary=$(mktemp -d)
trap 'rm -rf "$temporary"' EXIT HUP INT TERM

printf '%s\n' 'perf-mode-check: go: requested crc but negotiated secure for monitor connection' >"$temporary/error"
jq -n '{requested:"crc",connections:[{service:"monitor",actual:"secure",source:"go-auth-metadata"},{service:"osd",actual:"secure",source:"go-auth-metadata"},{service:"osd",actual:"secure",source:"go-auth-metadata"}]}' >"$temporary/go.json"
jq '.connections |= map(.source = "ceph-ready-log" | if .service == "osd" then .actual = "crc" else . end)' "$temporary/go.json" >"$temporary/native.json"
mode_capture_expected_crc_rejection 1 "$temporary/error" "$temporary/go.json" "$temporary/native.json"

reject() {
	if mode_capture_expected_crc_rejection "$@" 2>/dev/null; then
		printf '%s\n' 'unexpected CRC rejection classification' >&2
		exit 1
	fi
}
reject 0 "$temporary/error" "$temporary/go.json" "$temporary/native.json"
reject 2 "$temporary/error" "$temporary/go.json" "$temporary/native.json"
printf '%s\n' 'perf-mode-check: native: missing osd evidence' >"$temporary/wrong-error"
reject 1 "$temporary/wrong-error" "$temporary/go.json" "$temporary/native.json"
reject 1 "$temporary/error" "$temporary/missing.json" "$temporary/native.json"
printf '%s\n' '{invalid' >"$temporary/invalid.json"
reject 1 "$temporary/error" "$temporary/go.json" "$temporary/invalid.json"
for implementation in go native; do
	for mutation in \
		'.connections |= map(select(.service != "monitor"))' \
		'.connections |= map(select(.service != "osd"))' \
		'.connections = []' \
		'.connections[2].actual = "unknown"' \
		'.connections[2].actual = (if .connections[2].actual == "crc" then "secure" else "crc" end)' \
		'.connections[0].actual = "crc"' \
		'.connections[2].source = "unsupported"' \
		'.connections[2].service = "mgr"' \
		'.requested = "secure"'; do
		jq "$mutation" "$temporary/$implementation.json" >"$temporary/mutated.json"
		if test "$implementation" = go; then
			reject 1 "$temporary/error" "$temporary/mutated.json" "$temporary/native.json"
		else
			reject 1 "$temporary/error" "$temporary/go.json" "$temporary/mutated.json"
		fi
	done
done

mkdir "$temporary/accepted" "$temporary/rejected"
(
	export GOFLAGS='-race -cover -tags=custom' GOENV=custom.env GOEXPERIMENT='' GOWORK=custom.work GOTOOLCHAIN=local
	mode_capture_build_environment "$temporary/accepted"
	test "$GOFLAGS" = -mod=readonly
	test "$GOENV" = off
	test "$GOWORK" = off
	test "$GOEXPERIMENT" = ''
	test "$GOTOOLCHAIN" = local
	jq -e '. == {GOFLAGS:"-race -cover -tags=custom",GOENV:"custom.env",GOEXPERIMENT:"",GOWORK:"custom.work",GOTOOLCHAIN:"local"}' "$temporary/accepted/inherited-go.env.json" >/dev/null
)
(
	export GOEXPERIMENT=loopvar
	exit_code=0
	mode_capture_build_environment "$temporary/rejected" 2>"$temporary/setup.stderr" || exit_code=$?
	test "$exit_code" -eq 2
	test "$GOEXPERIMENT" = loopvar
	jq -e '.GOEXPERIMENT == "loopvar"' "$temporary/rejected/inherited-go.env.json" >/dev/null
	grep -qx 'mode capture requires empty GOEXPERIMENT' "$temporary/setup.stderr"
)
printf '%s\n' 'mode capture shell regressions passed (no Docker or live capture)'