#!/bin/sh
set -eu

mode_capture_expected_crc_rejection() {
	test "$1" -eq 1 &&
		grep -q '^perf-mode-check: go: requested crc but negotiated secure for ' "$2" &&
		jq -e '.requested == "crc" and ([.connections[].service] | index("monitor") != null and index("osd") != null) and all(.connections[]; (.service == "monitor" or .service == "osd") and .actual == "secure" and .source == "go-auth-metadata")' "$3" >/dev/null &&
		jq -e '.requested == "crc" and ([.connections[].service] | index("monitor") != null and index("osd") != null) and all(.connections[]; .source == "ceph-ready-log" and ((.service == "monitor" and .actual == "secure") or (.service == "osd" and .actual == "crc")))' "$4" >/dev/null
}

mode_capture_build_environment() {
	perl -MJSON::PP -e 'my %settings; for my $name (qw(GOFLAGS GOENV GOEXPERIMENT GOWORK GOTOOLCHAIN)) { $settings{$name} = exists $ENV{$name} ? $ENV{$name} : undef; } print JSON::PP->new->canonical->encode(\%settings), "\n";' >"$1/inherited-go.env.json"
	if test -n "${GOEXPERIMENT:-}"; then
		printf '%s\n' 'mode capture requires empty GOEXPERIMENT' >&2
		return 2
	fi
	export GOWORK=off GOENV=off GOFLAGS=-mod=readonly GOEXPERIMENT=''
}

root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd -P)
cd "$root"
original_root=$root
capture=
temporary=
cluster_started=false
network="rados-go-p07-$$"
stage=capture-preflight
mode_failed=0
started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
offered_failed=0
if test -n "${P07_OFFERED_OBSERVATION_DIR:-}${P07_OFFERED_OBSERVATION_LABEL:-}"; then
	printf '%s\n' 'observation directory and label are harness-owned; use P07_OFFERED_OBSERVE=1' >&2; exit 2
fi
if test -n "${P07_OFFERED_OBSERVE:-}"; then
	test "$P07_OFFERED_OBSERVE" = 1 && test "${P07_OFFERED_FACTORIAL:-}" = 1 && test "${P07_OFFERED_LOAD:-}" = 1 && test "${P07_SWEEP_FIXED_PROCS:-}" = 10 || {
		printf '%s\n' 'observation requires P07_OFFERED_OBSERVE=1, factorial offered load and fixed ten-P runtime' >&2; exit 2;
	}
fi
if test -n "${P07_OFFERED_RATE:-}${P07_FIXED_ALLOC_RATE:-}${P07_OFFERED_CASE:-}"; then
	printf '%s\n' 'offered rate controls are benchmark-only; harness runs the fixed matrix' >&2; exit 2
fi
if test -n "${P07_OFFERED_FACTORIAL:-}"; then
	test "$P07_OFFERED_FACTORIAL" = 1 && test "${P07_OFFERED_LOAD:-}" = 1 || {
		printf '%s\n' 'factorial load requires P07_OFFERED_FACTORIAL=1 and P07_OFFERED_LOAD=1' >&2; exit 2;
	}
fi
if test -n "${P07_OFFERED_LOAD:-}"; then
	test "$P07_OFFERED_LOAD" = 1 && test "${P07_SCHEDULER_SWEEP:-}" = 1 && test "${P07_SWEEP_FIXED_PROCS:-}" = 10 && test -n "${P07_DIAGNOSTIC_DIR:-}" &&
		test -z "${P07_READ_PROFILE:-}${P07_READ_SCALE:-}${P07_INVENTORY_SWEEP:-}${P07_DEFAULT_SCRATCH_CONFIRM:-}${P07_READ_INTO_COMPARE:-}${P07_RESOURCE_DIAGNOSTIC_DIR:-}${P07_MODE_DIAGNOSTIC_DIR:-}${P07_BACKGROUND_WORKERS:-}${P07_BACKGROUND_ALLOCATIONS:-}${P07_READ_SIZE:-}${P07_READ_CONCURRENCY:-}${P07_TIMING_FILE:-}${P07_TRACE_FILE:-}${P07_CPU_PROFILE:-}${P07_MEMORY_PROFILE:-}${P07_RESOURCE_FILE:-}${P07_MODE_EVIDENCE_FILE:-}${P07_PROFILE_KIND:-}${P07_SCRATCH_SLOTS:-}${P07_ADMISSION_WINDOW:-}${P07_OPERATIONS_PER_WORKER:-}${P07_SEED_ONLY:-}${P07_READ_INTO:-}${P07_READ_DIAGNOSTIC:-}${P07_REPORT:-}" || {
		printf '%s\n' 'offered load requires an isolated fixed ten-P scheduler sweep with harness-owned shape and load' >&2; exit 2;
	}
fi
if test -n "${P07_INVENTORY_SWEEP:-}${P07_SWEEP_FIXED_PROCS:-}${P07_DEFAULT_SCRATCH_CONFIRM:-}${P07_READ_PROFILE:-}${P07_READ_SCALE:-}" && test "${P07_SCHEDULER_SWEEP:-}" != 1; then
	printf '%s\n' 'inventory/fixed parallelism options require scheduler sweep' >&2; exit 2
fi
read_size=${P07_READ_SIZE:-65536}
read_concurrency=${P07_READ_CONCURRENCY:-16}
if test -n "${P07_READ_SIZE:-}${P07_READ_CONCURRENCY:-}"; then
	test -n "${P07_DIAGNOSTIC_DIR:-}" && test -z "${P07_RESOURCE_DIAGNOSTIC_DIR:-}${P07_MODE_DIAGNOSTIC_DIR:-}" || {
		printf '%s\n' 'read shape overrides require isolated read diagnostics' >&2; exit 2;
	}
fi
case "$read_size" in 4096|65536|1048576|4194304) ;; *) printf '%s\n' 'invalid diagnostic read size' >&2; exit 2 ;; esac
case "$read_concurrency" in 16|32|64) ;; *) printf '%s\n' 'invalid diagnostic read concurrency' >&2; exit 2 ;; esac
if test -n "${P07_BACKGROUND_WORKERS:-}"; then
	case "$P07_BACKGROUND_WORKERS" in *[!0-9]*|'') printf '%s\n' 'invalid background workers' >&2; exit 2 ;; esac
	test "$P07_BACKGROUND_WORKERS" -le 32 || { printf '%s\n' 'too many background workers' >&2; exit 2; }
fi
case "${P07_BACKGROUND_ALLOCATIONS:-0}" in 0|1) ;; *) printf '%s\n' 'invalid allocation workload' >&2; exit 2 ;; esac
if test "${P07_BACKGROUND_ALLOCATIONS:-0}" = 1; then
	test "${P07_BACKGROUND_WORKERS:-0}" -gt 0 && test "${P07_SCHEDULER_SWEEP:-}" = 1 || {
		printf '%s\n' 'allocation workload requires positive workers and scheduler sweep' >&2; exit 2;
	}
fi
if test -n "${P07_SCHEDULER_SWEEP:-}"; then
	test -z "${P07_READ_SCALE:-}" || {
		test "$P07_READ_SCALE" = 1 && test "${P07_SWEEP_FIXED_PROCS:-}" = 10 && test -z "${P07_READ_PROFILE:-}${P07_INVENTORY_SWEEP:-}${P07_DEFAULT_SCRATCH_CONFIRM:-}${P07_READ_INTO_COMPARE:-}";
	} || { printf '%s\n' 'read scale requires fixed ten-P sweep without competing modes' >&2; exit 2; }
	test -z "${P07_READ_PROFILE:-}" || {
		test "$P07_READ_PROFILE" = 1 && test "${P07_SWEEP_FIXED_PROCS:-}" = 10 && test -z "${P07_INVENTORY_SWEEP:-}${P07_DEFAULT_SCRATCH_CONFIRM:-}${P07_READ_INTO_COMPARE:-}";
	} || { printf '%s\n' 'read profiling requires fixed ten-P sweep without competing modes' >&2; exit 2; }
	test -z "${P07_DEFAULT_SCRATCH_CONFIRM:-}" || {
		test "$P07_DEFAULT_SCRATCH_CONFIRM" = 1 && test "${P07_SWEEP_FIXED_PROCS:-}" = 10 && test -z "${P07_INVENTORY_SWEEP:-}${P07_READ_INTO_COMPARE:-}";
	} || { printf '%s\n' 'default confirmation requires fixed ten-P sweep without competing modes' >&2; exit 2; }
	test -z "${P07_INVENTORY_SWEEP:-}" || {
		test "$P07_INVENTORY_SWEEP" = 1 && test "${P07_SWEEP_FIXED_PROCS:-}" = 10 && test -z "${P07_READ_INTO_COMPARE:-}";
	} || { printf '%s\n' 'inventory sweep requires fixed ten-P sweep without API comparison' >&2; exit 2; }
	test -z "${P07_READ_INTO_COMPARE:-}" || {
		test "$P07_READ_INTO_COMPARE" = 1 && test "${P07_SWEEP_FIXED_PROCS:-}" = 10;
	} || { printf '%s\n' 'caller-buffer comparison requires fixed ten-P sweep' >&2; exit 2; }
	test -z "${P07_SWEEP_FIXED_PROCS:-}" || test "$P07_SWEEP_FIXED_PROCS" = 10 || {
		printf '%s\n' 'fixed parallelism sweep currently supports only 10' >&2; exit 2;
	}
	test "$P07_SCHEDULER_SWEEP" = 1 && test -n "${P07_DIAGNOSTIC_DIR:-}" || {
		printf '%s\n' 'scheduler sweep requires P07_SCHEDULER_SWEEP=1 and P07_DIAGNOSTIC_DIR' >&2; exit 2;
	}
	test -z "${P07_RESOURCE_DIAGNOSTIC_DIR:-}${P07_MODE_DIAGNOSTIC_DIR:-}" || {
		printf '%s\n' 'scheduler sweep cannot be combined with resource or mode diagnostics' >&2; exit 2;
	}
	case "$P07_DIAGNOSTIC_DIR" in
		/*) ;;
		*) printf '%s\n' 'scheduler sweep output must be absolute' >&2; exit 2 ;;
	esac
	test ! -e "$P07_DIAGNOSTIC_DIR" && test ! -L "$P07_DIAGNOSTIC_DIR" || {
		printf '%s\n' 'scheduler sweep output must not exist' >&2; exit 2;
	}
	umask 077
	mkdir -m 700 "$P07_DIAGNOSTIC_DIR"
	printf '%s\n' 'running' >"$P07_DIAGNOSTIC_DIR/scheduler-status"
	git rev-parse HEAD >"$P07_DIAGNOSTIC_DIR/source-head"
	git ls-files -co --exclude-standard -z -- '*.go' '*.mjs' go.mod go.sum docs/p00/evidence.json integration/p07/reproduce.sh integration/p07/native_driver.c integration/p07/native_benchmark.c |
		xargs -0 shasum -a 256 >"$P07_DIAGNOSTIC_DIR/source-before.sha256"
fi
cleanup() {
	if test -n "$temporary"; then
		if test "$cluster_started" = true; then
			docker rm -f "p07-probe-$$" "p07-mon-$$" "p07-osd-0-$$" "p07-osd-1-$$" "p07-osd-2-$$" >/dev/null 2>&1 || true
			docker volume rm "rados-go-p07-osd-0-$$" "rados-go-p07-osd-1-$$" "rados-go-p07-osd-2-$$" >/dev/null 2>&1 || true
			docker network rm "$network" >/dev/null 2>&1 || true
		fi
		rm -rf "$temporary"
	fi
}
offered_finalize() {
	run_exit=$?
	trap - EXIT HUP INT TERM
	set +e
	source_check=failed
	if (cd "$original_root" && git ls-files -co --exclude-standard -z -- '*.go' '*.mjs' go.mod go.sum docs/p00/evidence.json integration/p07/reproduce.sh integration/p07/native_driver.c integration/p07/native_benchmark.c |
		xargs -0 shasum -a 256) >"$P07_DIAGNOSTIC_DIR/source-after.sha256" &&
		cmp -s "$P07_DIAGNOSTIC_DIR/source-before.sha256" "$P07_DIAGNOSTIC_DIR/source-after.sha256"; then
		source_check=unchanged
	else
		run_exit=1
	fi
	if test "${P07_OFFERED_FACTORIAL:-}" = 1 && test "$source_check" = unchanged; then
		if ! (cd "$root" && shasum -a 256 -c "$P07_DIAGNOSTIC_DIR/source-before.sha256") >"$P07_DIAGNOSTIC_DIR/source-snapshot-check.txt" 2>&1; then
			source_check=failed
			run_exit=1
		fi
	fi
	if test "$offered_failed" -ne 0; then run_exit=1; fi
	status=diagnostic-completed
	if test "$run_exit" -ne 0; then status=diagnostic-failed; fi
	measurement=primary
	if test "${P07_OFFERED_OBSERVE:-}" = 1; then measurement=instrumented; fi
	printf '%s\n' "$status" >"$P07_DIAGNOSTIC_DIR/scheduler-status"
	printf '{"status":"%s","measurement":"%s","benchmark_claim":false,"exit_code":%s,"failed_legs":%s,"source_check":"%s","started_at":"%s","finished_at":"%s"}\n' \
		"$status" "$measurement" "$run_exit" "$offered_failed" "$source_check" "$started_at" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >"$P07_DIAGNOSTIC_DIR/final-status.json"
	if ! (cd "$P07_DIAGNOSTIC_DIR" && find . -type f ! -name artifacts.sha256 -exec shasum -a 256 {} +) >"$P07_DIAGNOSTIC_DIR/artifacts.sha256"; then
		run_exit=1
		printf '%s\n' 'diagnostic-failed' >"$P07_DIAGNOSTIC_DIR/scheduler-status"
		printf '{"status":"diagnostic-failed","measurement":"%s","benchmark_claim":false,"exit_code":1,"reason":"artifact hashing failed"}\n' "$measurement" >"$P07_DIAGNOSTIC_DIR/final-status.json"
	fi
	cleanup
	exit "$run_exit"
}
if test "${P07_OFFERED_LOAD:-}" = 1; then
	trap offered_finalize EXIT
	trap 'exit 129' HUP
	trap 'exit 130' INT
	trap 'exit 143' TERM
	if test "${P07_OFFERED_FACTORIAL:-}" = 1; then
		stage=factorial-source-freeze
		mode_capture_build_environment "$P07_DIAGNOSTIC_DIR"
		git ls-files -co --exclude-standard -z -- '*.go' '*.mjs' go.mod go.sum docs/p00/evidence.json integration/p07/reproduce.sh integration/p07/native_driver.c integration/p07/native_benchmark.c >"$P07_DIAGNOSTIC_DIR/source-files.nul"
		tar -czf "$P07_DIAGNOSTIC_DIR/workload-sources.tar.gz" --null -T "$P07_DIAGNOSTIC_DIR/source-files.nul"
		temporary=$(mktemp -d)
		mkdir "$temporary/source"
		tar -xzf "$P07_DIAGNOSTIC_DIR/workload-sources.tar.gz" -C "$temporary/source"
		root=$temporary/source
		cd "$root"
		shasum -a 256 -c "$P07_DIAGNOSTIC_DIR/source-before.sha256" >"$P07_DIAGNOSTIC_DIR/source-snapshot-before.txt"
		printf '%s\n' 'factorial; 60 primary legs; independent CPU and allocation workers; fixed ten-P runtime is diagnostic, not library tuning; no runtime observation' >"$P07_DIAGNOSTIC_DIR/offered-methodology.txt"
		if test "${P07_OFFERED_OBSERVE:-}" = 1; then
			printf '%s\n' 'instrumented; 12 legs; three rotated repetitions of none cpu alloc both at rate 2000; runtime trace and per-attempt timing; excluded from primary comparisons; frozen sources and normal untagged binary; fixed ten-P runtime is diagnostic, not library tuning' >"$P07_DIAGNOSTIC_DIR/offered-methodology.txt"
		fi
	fi
fi
finalize() {
	run_exit=$?
	trap - EXIT HUP INT TERM
	set +e
	if test -n "$capture"; then
		if test "$run_exit" -ne 0 && test "$mode_failed" -eq 0; then mode_failed=1; fi
		source_check=not_frozen
		if test -f "$capture/source-manifest.json"; then
			source_check=failed
			if perl "$capture/source-audit.pl" select "$original_root" "$capture" >"$capture/source-files-after.nul" &&
				perl "$capture/source-audit.pl" manifest "$original_root" <"$capture/source-files-after.nul" >"$capture/source-manifest-after.json" &&
				cmp -s "$capture/source-files.nul" "$capture/source-files-after.nul" &&
				cmp -s "$capture/source-manifest.json" "$capture/source-manifest-after.json" &&
				perl "$capture/source-audit.pl" manifest "$root" <"$capture/source-files.nul" >"$capture/source-manifest-snapshot-after.json" &&
				cmp -s "$capture/source-manifest.json" "$capture/source-manifest-snapshot-after.json" &&
				(cd "$capture" && shasum -a 256 -c source-binding.sha256); then
				source_check=unchanged
			else
				mode_failed=$((mode_failed + 1))
				test "$run_exit" -ne 0 || run_exit=1
			fi
		fi
		for artifact in probe benchmark native-driver native-benchmark perf-mode-check native-seed.json \
			ceph.version librados.path librados.package librados.sha256 seed-modes.json \
			go-secure-modes.json go-crc-modes.json native-secure.log native-crc.log \
			native-secure-modes.json native-crc-modes.json; do
			if test -n "$temporary" && test -f "$temporary/$artifact"; then
				if ! cp "$temporary/$artifact" "$capture/"; then
					mode_failed=$((mode_failed + 1))
					test "$run_exit" -ne 0 || run_exit=1
				fi
			fi
		done
		(cd "$capture" && for artifact in probe benchmark native-driver native-benchmark perf-mode-check workload-sources.tar.gz source-manifest.json; do
			test ! -f "$artifact" || shasum -a 256 "$artifact" || exit 1
		done) >"$capture/artifacts.sha256"
		if test "$?" -ne 0; then mode_failed=$((mode_failed + 1)); test "$run_exit" -ne 0 || run_exit=1; fi
		printf '{"started_at":"%s","finished_at":"%s","stage":"%s","exit_code":%s,"failed_count":%s,"source_check":"%s"}\n' \
			"$started_at" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$stage" "$run_exit" "$mode_failed" "$source_check" >"$capture/final-status.json"
		if test -f "$capture/status.json" && command -v jq >/dev/null 2>&1; then
			jq --slurpfile final "$capture/final-status.json" '. + $final[0]' "$capture/status.json" >"$capture/status.final.json" && mv "$capture/status.final.json" "$capture/status.json" || cp "$capture/final-status.json" "$capture/status.json"
		else
			cp "$capture/final-status.json" "$capture/status.json"
		fi
		printf 'P07 LIVE mode capture: %s (stage %s, exit %s)\n' "$capture" "$stage" "$run_exit" >&3
	fi
	cleanup
	exit "$run_exit"
}
if test -n "${P07_MODE_DIAGNOSTIC_DIR:-}"; then
	case "$P07_MODE_DIAGNOSTIC_DIR" in
		/*) ;;
		*) printf '%s\n' 'P07_MODE_DIAGNOSTIC_DIR must be absolute' >&2; exit 2 ;;
	esac
	if test -e "$P07_MODE_DIAGNOSTIC_DIR" || test -L "$P07_MODE_DIAGNOSTIC_DIR"; then
		printf '%s\n' 'P07_MODE_DIAGNOSTIC_DIR must not exist' >&2
		exit 2
	fi
	test -z "${P07_RESOURCE_DIAGNOSTIC_DIR:-}${P07_DIAGNOSTIC_DIR:-}" || {
		printf '%s\n' 'mode capture cannot be combined with other diagnostics' >&2; exit 2;
	}
	umask 077
	mkdir -m 700 "$P07_MODE_DIAGNOSTIC_DIR"
	exec 3>&1 4>&2
	capture=$P07_MODE_DIAGNOSTIC_DIR
	trap finalize EXIT
	trap 'exit 129' HUP
	trap 'exit 130' INT
	trap 'exit 143' TERM
	stage=capture-setup
	exec >"$capture/setup.stdout" 2>"$capture/setup.stderr"
	capture=$(CDPATH= cd -- "$capture" && pwd -P)
	stage=toolchain-preflight
	mode_capture_build_environment "$capture"
	stage=source-freeze
	cat >"$capture/source-audit.pl" <<'PERL'
use strict;
use warnings;
use JSON::PP;
my ($action, $root, $capture) = @ARGV;
chdir $root or die "source root: $!\n";
if ($action eq 'select') {
	open my $tracked_pipe, '-|', 'git', 'ls-files', '-z' or die "git tracked: $!\n";
	my $tracked_data = do { local $/; <$tracked_pipe> };
	close $tracked_pipe or die "git tracked failed\n";
	my %tracked = map { $_ => 1 } split /\0/, $tracked_data;
	open my $all_pipe, '-|', 'git', 'ls-files', '-co', '--exclude-standard', '-z' or die "git selected: $!\n";
	my $all_data = do { local $/; <$all_pipe> };
	close $all_pipe or die "git selected failed\n";
	my %selected;
	for my $file (split /\0/, $all_data) {
		next if "$root/$file" eq $capture || index("$root/$file", "$capture/") == 0;
		next unless $tracked{$file} || $file =~ /\.go\z/ || $file eq 'go.sum' || $file eq 'integration/p07/MODE_EVIDENCE.md';
		die "non-regular selected source (including symlink): $file\n" if -l $file || !-f $file;
		$selected{$file} = 1;
	}
	print join('', map { "$_\0" } sort keys %selected);
} elsif ($action eq 'manifest') {
	my $data = do { local $/; <STDIN> };
	my @manifest;
	for my $file (sort split /\0/, $data) {
		die "non-regular source: $file\n" if -l $file || !-f $file;
		open my $hash_pipe, '-|', 'shasum', '-a', '256', '--', $file or die "shasum: $!\n";
		my $hash_line = do { local $/; <$hash_pipe> };
		close $hash_pipe or die "shasum failed\n";
		$hash_line =~ /\A\\?([a-f0-9]{64}) / or die "invalid shasum output\n";
		push @manifest, {path_hex => unpack('H*', $file), sha256 => $1};
	}
	print JSON::PP->new->canonical->encode(\@manifest), "\n";
} else { die "unknown source audit action\n"; }
PERL
	git rev-parse HEAD >"$capture/git-head"
	git status --short >"$capture/git-status"
	perl "$capture/source-audit.pl" select "$original_root" "$capture" >"$capture/source-files.nul"
	perl "$capture/source-audit.pl" manifest "$original_root" <"$capture/source-files.nul" >"$capture/source-manifest.before.json"
	tar -czf "$capture/workload-sources.tar.gz" --null -T "$capture/source-files.nul"
	temporary=$(mktemp -d)
	mkdir "$temporary/source"
	tar -xzf "$capture/workload-sources.tar.gz" -C "$temporary/source"
	root=$temporary/source
	perl "$capture/source-audit.pl" manifest "$root" <"$capture/source-files.nul" >"$capture/source-manifest.json"
	cmp "$capture/source-manifest.before.json" "$capture/source-manifest.json"
	(cd "$capture" && shasum -a 256 workload-sources.tar.gz source-manifest.json) >"$capture/source-binding.sha256"
	cd "$root"
	stage=toolchain
	go version >"$capture/go.version"
	go env -json GOVERSION GOOS GOARCH GOTOOLCHAIN GOWORK GOFLAGS GOENV GOEXPERIMENT CGO_ENABLED >"$capture/go.env.json"
fi
report=${P07_REPORT:-docs/p07/integration-report.json}
stage=docker-discovery
case "$(docker info --format '{{.Architecture}}')" in
	x86_64|amd64) platform=linux/amd64; goarch=amd64 ;;
	aarch64|arm64) platform=linux/arm64; goarch=arm64 ;;
	*) printf '%s\n' 'unsupported Docker architecture' >&2; exit 2 ;;
esac
image_index=$(jq -r '.images.qualification.reference' docs/p00/evidence.json)
image_digest=$(jq -r --arg architecture "$goarch" '.images.qualification[$architecture]' docs/p00/evidence.json)
image="${image_index%@*}@$image_digest"
if test -n "$capture"; then
	printf '%s\n' "$image" >"$capture/image.reference"
	printf '%s\n' "$image_index" >"$capture/image.index"
	printf '%s\n' "$platform" >"$capture/platform"
	docker version --format '{{.Server.Version}} {{.Server.GitCommit}}' >"$capture/docker.version"
fi
test -n "$temporary" || temporary=$(mktemp -d)
fsid=11111111-2222-4333-8444-777777777777
if test -z "$capture" && test "${P07_OFFERED_LOAD:-}" != 1; then trap cleanup EXIT HUP INT TERM; fi
compile() {
	if test -n "$capture"; then
		perl -MJSON::PP -e 'print JSON::PP->new->canonical->encode({cwd=>shift @ARGV,argv=>\@ARGV}), "\n"' "$PWD" "$@" >>"$capture/compile-commands.jsonl"
	fi
	"$@"
}

stage=go-build
compile env CGO_ENABLED=0 GOOS=linux GOARCH="$goarch" go build -trimpath -o "$temporary/probe" ./integration/p07/probe
if test "${P07_INVENTORY_SWEEP:-}" = 1 || test "${P07_READ_SCALE:-}" = 1; then
	compile env CGO_ENABLED=0 GOOS=linux GOARCH="$goarch" go build -tags p12diagnostics -trimpath -o "$temporary/benchmark" ./integration/p07/benchmark
elif test "${P07_OFFERED_LOAD:-}" = 1; then
	compile env CGO_ENABLED=0 GOOS=linux GOARCH="$goarch" GOFLAGS=-mod=readonly GOENV=off GOEXPERIMENT='' GOWORK=off go build -trimpath -o "$temporary/benchmark" ./integration/p07/benchmark
else
	compile env CGO_ENABLED=0 GOOS=linux GOARCH="$goarch" go build -trimpath -o "$temporary/benchmark" ./integration/p07/benchmark
fi
if test -n "$capture"; then
	compile env CGO_ENABLED=0 GOOS=linux GOARCH="$goarch" go build -trimpath -o "$temporary/perf-mode-check" ./tools/perf-mode-check
fi
cp integration/p07/native_driver.c integration/p07/native_benchmark.c "$temporary/"
stage=native-build
compile docker run --rm --user 0 --platform "$platform" -v "$temporary:/cluster" "$image" sh -c '
	set -eu
	cc -std=c11 -Wall -Wextra -Werror -O2 /cluster/native_driver.c -ldl -o /cluster/native-driver
	cc -std=c11 -Wall -Wextra -Werror -O2 -pthread /cluster/native_benchmark.c -ldl -o /cluster/native-benchmark
'
printf 'aZ\000\000efg' >"$temporary/parity.expected"
printf base >"$temporary/base"
stage=cluster-setup
cluster_started=true
docker network create --subnet 172.30.97.0/24 "$network" >/dev/null
docker run --rm --user 0 --platform "$platform" -v "$temporary:/cluster" "$image" sh -c '
	set -eu
	cat >/cluster/ceph.conf <<EOF
[global]
fsid = 11111111-2222-4333-8444-777777777777
mon host = v2:172.30.97.10:3300
auth cluster required = cephx
auth service required = cephx
auth client required = cephx
ms bind msgr1 = false
ms bind msgr2 = true
osd pool default size = 2
osd pool default min size = 1
EOF
	ceph-authtool /cluster/mon.keyring --create-keyring --gen-key -n mon. --cap mon "allow *"
	ceph-authtool /cluster/admin.keyring --create-keyring --gen-key -n client.admin --cap mon "allow *" --cap osd "allow *" --cap mgr "allow *"
	ceph-authtool /cluster/mon.keyring --import-keyring /cluster/admin.keyring
	monmaptool --create --fsid 11111111-2222-4333-8444-777777777777 --addv a "[v2:172.30.97.10:3300/0]" /cluster/monmap
	mkdir -p /cluster/mondata
	ceph-mon --mkfs -i a --fsid 11111111-2222-4333-8444-777777777777 --monmap /cluster/monmap --keyring /cluster/mon.keyring --mon-data /cluster/mondata
	chown -R ceph:ceph /cluster/mondata
' >/dev/null 2>&1
docker run -d --name "p07-mon-$$" --platform "$platform" --network "$network" --ip 172.30.97.10 -v "$temporary:/cluster" "$image" \
	ceph-mon -f -i a --mon-data /cluster/mondata --public-addr v2:172.30.97.10:3300 --setuser ceph --setgroup ceph --mon-data-avail-crit 0 --no-mon-cluster-log-to-stderr >/dev/null

ceph_cli() {
	docker run --rm --platform "$platform" --network "$network" -v "$temporary:/cluster" "$image" \
		timeout 15 ceph --conf /cluster/ceph.conf --name client.admin --keyring /cluster/admin.keyring "$@"
}
for attempt in $(seq 1 30); do
	if ceph_cli status --format json 2>/dev/null | jq -e '.health.status != null' >/dev/null; then break; fi
	mon_running=$(docker inspect -f '{{.State.Running}}' "p07-mon-$$" 2>/dev/null || printf false)
	test "$mon_running" = true || { docker logs "p07-mon-$$" >&2; exit 1; }
	test "$attempt" -lt 30 || { docker logs "p07-mon-$$" >&2; exit 1; }
	sleep 1
done

for id in 0 1 2; do
	uuid="00000000-0000-4000-8000-00000000001$id"
	volume="rados-go-p07-osd-$id-$$"
	docker volume create "$volume" >/dev/null
	ceph_cli osd create "$uuid" "$id" >/dev/null
	ceph_cli auth get-or-create "osd.$id" mon 'allow profile osd' mgr 'allow profile osd' osd 'allow *' -o "/cluster/osd-$id.keyring" >/dev/null 2>&1
	ceph_cli mon getmap -o "/cluster/osd-$id.monmap" >/dev/null
	docker run --rm --user 0 --privileged --platform "$platform" -v "$temporary:/cluster" -v "$volume:/osd" "$image" sh -c '
		set -eu
		id='"$id"'; uuid='"$uuid"'
		mkdir -p /osd/data
		truncate -s 8G /osd/block
		cp /cluster/osd-$id.keyring /osd/data/keyring
		cp /cluster/osd-$id.monmap /osd/data/activate.monmap
		chown -R ceph:ceph /osd
		ceph-osd --mkfs -i "$id" --osd-data /osd/data --osd-uuid "$uuid" --osd-objectstore bluestore --bluestore-block-path /osd/block --monmap /osd/data/activate.monmap --keyring /osd/data/keyring --setuser ceph --setgroup ceph
	' >/dev/null 2>&1
	ip="172.30.97.$((20 + id))"
	docker run -d --privileged --name "p07-osd-$id-$$" --platform "$platform" --network "$network" --ip "$ip" -v "$temporary:/cluster" -v "$volume:/osd" "$image" \
		ceph-osd -f --conf /cluster/ceph.conf -i "$id" --osd-data /osd/data --osd-objectstore bluestore --public-addr "v2:$ip:6800" --cluster-addr "v2:$ip:6802" --setuser ceph --setgroup ceph >/dev/null
done
for attempt in $(seq 1 60); do
	if ceph_cli osd stat --format json 2>/dev/null | jq -e '.num_osds == 3 and .num_up_osds == 3 and .num_in_osds == 3' >/dev/null; then break; fi
	test "$attempt" -lt 60 || { ceph_cli osd tree >&2; exit 1; }
	sleep 1
done

ceph_cli osd crush rule create-replicated p07-rule default osd >/dev/null
ceph_cli osd pool create p07-data 16 16 replicated p07-rule >/dev/null
ceph_cli osd pool set p07-data size 2 >/dev/null
ceph_cli osd pool set p07-data min_size 1 >/dev/null
ceph_cli auth get-or-create client.p07 mon 'allow r' osd 'allow rw pool=p07-data' >/dev/null 2>&1
ceph_cli auth get-key client.p07 >"$temporary/client.key" 2>/dev/null
ceph_cli auth get client.p07 -o /cluster/client.keyring >/dev/null 2>&1

rados_cli() {
	docker run --rm --platform "$platform" --network "$network" -v "$temporary:/cluster" "$image" \
		rados --conf /cluster/ceph.conf --name client.admin --keyring /cluster/admin.keyring -p p07-data "$@"
}
benchmark_failed() {
	for daemon in "p07-mon-$$" "p07-osd-0-$$" "p07-osd-1-$$" "p07-osd-2-$$"; do
		printf '\n%s\n' "last logs for $daemon" >&2
		docker logs --tail 120 "$daemon" >&2 || true
	done
	exit 1
}
rados_cli put remap-append /cluster/base

stage=native-seed
docker run --rm --platform "$platform" --network "$network" -v "$temporary:/cluster" "$image" \
	timeout 60 /cluster/native-driver seed /cluster/ceph.conf /cluster/admin.keyring p07-data >"$temporary/native-seed.json"
jq -e '.native_crud and .mixed_seed' "$temporary/native-seed.json" >/dev/null

if test -n "${P07_MODE_DIAGNOSTIC_DIR:-}"; then
	stage=native-runtime
	docker run --rm --platform "$platform" -v "$temporary:/cluster" "$image" sh -c '
		set -eu
		ceph --version > /cluster/ceph.version
		library=$(ldconfig -p | awk "/librados.so.2/{print \$NF; exit}")
		test -n "$library"
		readlink -f "$library" > /cluster/librados.path
		rpm -qf "$(readlink -f "$library")" > /cluster/librados.package
		sha256sum "$(readlink -f "$library")" > /cluster/librados.sha256
	'
	cp "$temporary/ceph.version" "$temporary/librados.path" "$temporary/librados.package" "$temporary/librados.sha256" "$capture/"
	stage=go-seed
	seed_exit=0
	docker run --rm --platform "$platform" --network "$network" -v "$temporary:/work" \
		-e P07_SEED_ONLY=1 -e P07_MODE_EVIDENCE_FILE=/work/seed-modes.json "$image" \
		timeout 60 /work/benchmark -monitors 172.30.97.10:3300 -key-file /work/client.key -fsid "$fsid" -pool p07-data -transport secure \
		>"$capture/seed.json" 2>"$capture/seed.stderr" || seed_exit=$?
	test ! -f "$temporary/seed-modes.json" || cp "$temporary/seed-modes.json" "$capture/"
	jq -n --argjson exit_code "$seed_exit" '{exit_code:$exit_code}' >"$capture/seed-status.json"
	test "$seed_exit" -eq 0 && jq -e '.seeded' "$capture/seed.json" >/dev/null || {
		printf 'P07 mode seed failed; see %s\n' "$capture" >&4; exit 1;
	}
	mode_failed=0
	for mode in secure crc; do
		stage=mode-$mode
		go_exit=0; native_exit=0; validation_exit=0; workload_exit=0
		mode_started=$(date -u +%Y-%m-%dT%H:%M:%SZ)
		docker run --rm --platform "$platform" --network "$network" -v "$temporary:/work" \
			-e P07_READ_DIAGNOSTIC=1 -e "P07_MODE_EVIDENCE_FILE=/work/go-$mode-modes.json" "$image" \
			timeout 3600 /work/benchmark -monitors 172.30.97.10:3300 -key-file /work/client.key -fsid "$fsid" -pool p07-data -transport "$mode" \
			>"$capture/go-$mode.json" 2>"$capture/go-$mode.stderr" || go_exit=$?
		docker run --rm --platform "$platform" --network "$network" -v "$temporary:/cluster" \
			-e P07_READ_DIAGNOSTIC=1 -e "P07_NATIVE_MODE_LOG=/cluster/native-$mode.log" "$image" \
			timeout 3600 /cluster/native-benchmark /cluster/ceph.conf /cluster/client.keyring p07-data "$mode" \
			>"$capture/native-$mode.json" 2>"$capture/native-$mode.stderr" || native_exit=$?
		docker run --rm --platform "$platform" -v "$temporary:/work" "$image" \
			/work/perf-mode-check -go "/work/go-$mode-modes.json" -native-log "/work/native-$mode.log" -requested "$mode" -native-out "/work/native-$mode-modes.json" \
			>"$capture/validation-$mode.stdout" 2>"$capture/validation-$mode.stderr" || validation_exit=$?
		for artifact in "go-$mode-modes.json" "native-$mode.log" "native-$mode-modes.json"; do
			test ! -f "$temporary/$artifact" || cp "$temporary/$artifact" "$capture/"
		done
		for implementation in go native; do
			jq -e --arg implementation "$implementation" --arg mode "$mode" \
				'.implementation == $implementation and .transport == $mode and (.rows | length) == 1 and .rows[0].operations == 4096 and .rows[0].workload == "read"' \
				"$capture/$implementation-$mode.json" >"$capture/$implementation-$mode-workload.stdout" 2>"$capture/$implementation-$mode-workload.stderr" || workload_exit=1
		done
		observed=failed; expected=pass
		if test "$mode" = crc; then expected=requested_actual_mismatch; fi
		if test "$validation_exit" -eq 0; then
			observed=pass
		elif test "$mode" = crc && mode_capture_expected_crc_rejection "$validation_exit" \
			"$capture/validation-$mode.stderr" "$capture/go-$mode-modes.json" "$capture/native-$mode-modes.json"; then
			observed=requested_actual_mismatch
		fi
		matched=false
		if test "$go_exit" -eq 0 && test "$native_exit" -eq 0 && test "$workload_exit" -eq 0 && test "$observed" = "$expected"; then
			matched=true
		else
			mode_failed=$((mode_failed + 1))
		fi
		jq -n --arg mode "$mode" --arg started_at "$mode_started" --arg finished_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
			--arg expected "$expected" --arg observed "$observed" --argjson matched "$matched" \
			--argjson go_exit "$go_exit" --argjson native_exit "$native_exit" --argjson validation_exit "$validation_exit" --argjson workload_exit "$workload_exit" \
			'{requested:$mode,started_at:$started_at,finished_at:$finished_at,expected:$expected,observed:$observed,expectation_met:$matched,exit_codes:{go:$go_exit,native:$native_exit,validator:$validation_exit,workload_check:$workload_exit}}' >"$capture/status-$mode.json"
	done
	stage=mode-summary
	jq -n --arg started_at "$started_at" --arg finished_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
		--arg image "$image" --arg image_index "$image_index" --arg platform "$platform" --arg version "$(cat "$capture/ceph.version")" \
		--argjson exit_code "$mode_failed" --slurpfile secure "$capture/status-secure.json" --slurpfile crc "$capture/status-crc.json" \
		'{schema_version:1,kind:"phase0-live-mode-diagnostic",benchmark_claim:false,started_at:$started_at,finished_at:$finished_at,exit_code:$exit_code,server:{image:$image,image_index:$image_index,image_digest_source:"docs/p00/evidence.json:images.qualification",platform:$platform,version:$version,source_anchor_commit:"7f793731f1b39eb4f465e960113d2363c311b964",native_parser_source:"https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/msg/async/ProtocolV2.cc"},modes:[$secure[0],$crc[0]]}' >"$capture/status.json"
	printf 'P07 LIVE mode diagnostics written to %s (exit %s)\n' "$capture" "$mode_failed" >&3
	stage=complete
	exit "$mode_failed"
fi

if test -n "${P07_RESOURCE_DIAGNOSTIC_DIR:-}"; then
	case "$P07_RESOURCE_DIAGNOSTIC_DIR" in
		/*) ;;
		*) printf '%s\n' 'P07_RESOURCE_DIAGNOSTIC_DIR must be absolute' >&2; exit 2 ;;
	esac
	mkdir -p "$P07_RESOURCE_DIAGNOSTIC_DIR"
	docker run --rm --platform "$platform" --network "$network" -v "$temporary:/cluster" "$image" \
		timeout 3600 /cluster/native-benchmark /cluster/ceph.conf /cluster/client.keyring p07-data secure >"$P07_RESOURCE_DIAGNOSTIC_DIR/native.json"
	for mode in baseline profile; do
		if test "$mode" = profile; then
			profile_env=P07_CPU_PROFILE=/work/cpu.pprof
		else
			profile_env=P07_CPU_PROFILE=
		fi
		docker run --rm --platform "$platform" --network "$network" -v "$temporary:/work" \
			-e "$profile_env" -e "P07_MEMORY_PROFILE=/work/$mode-allocs.pprof" -e "P07_RESOURCE_FILE=/work/$mode-resources.json" "$image" \
			timeout 3600 /work/benchmark -monitors 172.30.97.10:3300 -key-file /work/client.key -fsid "$fsid" -pool p07-data -transport secure >"$P07_RESOURCE_DIAGNOSTIC_DIR/go-$mode.json"
		cp "$temporary/$mode-allocs.pprof" "$temporary/$mode-resources.json" "$temporary/$mode-resources.json.heap.pprof" "$P07_RESOURCE_DIAGNOSTIC_DIR/"
	done
	cp "$temporary/cpu.pprof" "$temporary/benchmark" "$P07_RESOURCE_DIAGNOSTIC_DIR/"
	printf 'P07 full-workload resource diagnostics written to %s\n' "$P07_RESOURCE_DIAGNOSTIC_DIR"
	exit 0
fi

if test -n "${P07_DIAGNOSTIC_DIR:-}"; then
	case "$P07_DIAGNOSTIC_DIR" in
		/*) ;;
		*) printf '%s\n' 'P07_DIAGNOSTIC_DIR must be absolute' >&2; exit 2 ;;
	esac
	mkdir -p "$P07_DIAGNOSTIC_DIR"
	if test "$read_size" != 65536; then
		docker run --rm --platform "$platform" --network "$network" -v "$temporary:/work" -e P07_SEED_ONLY=1 "$image" \
			timeout 60 /work/benchmark -monitors 172.30.97.10:3300 -key-file /work/client.key -fsid "$fsid" -pool p07-data -transport secure >"$P07_DIAGNOSTIC_DIR/native-context-seed.json"
		jq -e '.seeded' "$P07_DIAGNOSTIC_DIR/native-context-seed.json" >/dev/null
	fi
	seed_concurrency=$read_concurrency
	if test "${P07_READ_SCALE:-}" = 1; then seed_concurrency=64; fi
	docker run --rm --platform "$platform" --network "$network" -v "$temporary:/work" -e P07_SEED_ONLY=1 \
		-e P07_READ_SIZE="$read_size" -e P07_READ_CONCURRENCY="$seed_concurrency" "$image" \
		timeout 60 /work/benchmark -monitors 172.30.97.10:3300 -key-file /work/client.key -fsid "$fsid" -pool p07-data -transport secure >"$P07_DIAGNOSTIC_DIR/seed.json"
	jq -e '.seeded' "$P07_DIAGNOSTIC_DIR/seed.json" >/dev/null
	initial_legs='native-1 go-1 go-2 native-2'
	if test "${P07_READ_SCALE:-}" = 1; then initial_legs=''; fi
	if test "${P07_OFFERED_LOAD:-}" = 1; then initial_legs=''; fi
	for leg in $initial_legs; do
		for id in 0 1 2; do
			ceph_cli tell "osd.$id" perf dump >"$P07_DIAGNOSTIC_DIR/$leg-osd-$id-before.json"
		done
		date -u +%Y-%m-%dT%H:%M:%SZ >"$P07_DIAGNOSTIC_DIR/$leg-started-at"
		case "$leg" in
			native-*)
				leg_count=4096; leg_size=65536; leg_concurrency=16
				docker run --rm --platform "$platform" --network "$network" -v "$temporary:/cluster" -e P07_READ_DIAGNOSTIC=1 "$image" \
					timeout 3600 /cluster/native-benchmark /cluster/ceph.conf /cluster/client.keyring p07-data secure >"$P07_DIAGNOSTIC_DIR/$leg.json" ;;
			go-*)
				leg_count=$((read_concurrency * 256)); leg_size=$read_size; leg_concurrency=$read_concurrency
				docker run --rm --platform "$platform" --network "$network" -v "$temporary:/work" -e P07_READ_DIAGNOSTIC=1 \
					-e P07_READ_SIZE="$read_size" -e P07_READ_CONCURRENCY="$read_concurrency" "$image" \
					timeout 3600 /work/benchmark -monitors 172.30.97.10:3300 -key-file /work/client.key -fsid "$fsid" -pool p07-data -transport secure >"$P07_DIAGNOSTIC_DIR/$leg.json" ;;
		esac
		date -u +%Y-%m-%dT%H:%M:%SZ >"$P07_DIAGNOSTIC_DIR/$leg-finished-at"
		jq -e --argjson count "$leg_count" --argjson size "$leg_size" --argjson concurrency "$leg_concurrency" '.transport == "secure" and (.rows | length) == 1 and .rows[0].operations == $count and .rows[0].size_bytes == $size and .rows[0].concurrency == $concurrency and .rows[0].workload == "read"' "$P07_DIAGNOSTIC_DIR/$leg.json" >/dev/null
		for id in 0 1 2; do
			ceph_cli tell "osd.$id" perf dump >"$P07_DIAGNOSTIC_DIR/$leg-osd-$id-after.json"
		done
	done
	if test "${P07_SCHEDULER_SWEEP:-}" = 1; then
		cp "$temporary/benchmark" "$temporary/native-benchmark" "$P07_DIAGNOSTIC_DIR/"
		go version -m "$temporary/benchmark" >"$P07_DIAGNOSTIC_DIR/go-buildinfo.txt"
		docker info --format '{{json .}}' >"$P07_DIAGNOSTIC_DIR/docker-info.json"
		printf '%s\n' "$image" >"$P07_DIAGNOSTIC_DIR/image.reference"
		if test "${P07_OFFERED_LOAD:-}" = 1; then
			offered_run() {
				label=$1; rate=$2
				load_case=${3:-both}
				cpu_workers=8; allocation_workers=8; allocation_expected=6400
				factorial_case=
				if test "${P07_OFFERED_FACTORIAL:-}" = 1; then
					factorial_case=$load_case
					case "$load_case" in
						none) cpu_workers=0; allocation_workers=0 ;;
						cpu) allocation_workers=0 ;;
						alloc) cpu_workers=0 ;;
						both) ;;
						*) printf '%s\n' 'invalid factorial case' >&2; return 2 ;;
					 esac
					allocation_expected=$((allocation_workers * 800))
				fi
				leg_exit=0
				for id in 0 1 2; do
					ceph_cli tell "osd.$id" perf dump >"$P07_DIAGNOSTIC_DIR/$label-osd-$id-before.json" || leg_exit=1
				 done
				date -u +%Y-%m-%dT%H:%M:%SZ >"$P07_DIAGNOSTIC_DIR/$label-started-at"
				docker_exit=0
				instrumented=false; observation_dir=; observation_label=
				if test "${P07_OFFERED_OBSERVE:-}" = 1; then
					instrumented=true; observation_dir="/work/$label-observation"; observation_label=instrumented
				fi
				docker run --rm --platform "$platform" --network "$network" -v "$temporary:/work" \
					-e P07_OFFERED_LOAD=1 -e P07_READ_DIAGNOSTIC=1 -e P07_READ_INTO=1 \
					-e GOMAXPROCS=10 -e GOGC=100 -e GOMEMLIMIT=off \
					-e P07_OFFERED_RATE="$rate" -e P07_FIXED_ALLOC_RATE=100 -e P07_BACKGROUND_WORKERS=8 \
					-e P07_OFFERED_FACTORIAL="${P07_OFFERED_FACTORIAL:-}" -e P07_OFFERED_CASE="$factorial_case" \
					-e P07_OFFERED_OBSERVATION_DIR="$observation_dir" -e P07_OFFERED_OBSERVATION_LABEL="$observation_label" \
					"$image" sh -c '
						set -u
						label=$1
						capture_failed=0
						for entry in cpu.max cpu.stat cpuset.cpus.effective memory.max; do
							cat "/sys/fs/cgroup/$entry" >"/work/$label-$entry-before" || capture_failed=1
						 done
						benchmark_exit=0
						timeout 120 /work/benchmark -monitors 172.30.97.10:3300 -key-file /work/client.key -fsid 11111111-2222-4333-8444-777777777777 -pool p07-data -transport secure || benchmark_exit=$?
						printf "%s\n" "$benchmark_exit" >"/work/$label-benchmark-exit-code"
						for entry in cpu.max cpu.stat cpuset.cpus.effective memory.max; do
							cat "/sys/fs/cgroup/$entry" >"/work/$label-$entry-after" || capture_failed=1
						 done
						test "$benchmark_exit" -eq 0 || exit "$benchmark_exit"
						exit "$capture_failed"
					' sh "$label" >"$P07_DIAGNOSTIC_DIR/$label.json" 2>"$P07_DIAGNOSTIC_DIR/$label.stderr" || docker_exit=$?
				printf '%s\n' "$docker_exit" >"$P07_DIAGNOSTIC_DIR/$label-exit-code"
				test "$docker_exit" -eq 0 || leg_exit=1
				date -u +%Y-%m-%dT%H:%M:%SZ >"$P07_DIAGNOSTIC_DIR/$label-finished-at"
				for id in 0 1 2; do
					ceph_cli tell "osd.$id" perf dump >"$P07_DIAGNOSTIC_DIR/$label-osd-$id-after.json" || leg_exit=1
				 done
				jq -e --argjson instrumented "$instrumented" '.offered_load.config.instrumented == $instrumented' "$P07_DIAGNOSTIC_DIR/$label.json" >/dev/null 2>>"$P07_DIAGNOSTIC_DIR/$label.stderr" || leg_exit=1
				if test "$instrumented" = true; then
					test -s "$temporary/$label-observation/trace.out" && test -s "$temporary/$label-observation/timing.json" || leg_exit=1
					jq -e --slurpfile report "$P07_DIAGNOSTIC_DIR/$label.json" '.label == "instrumented" and (.calls | length) == $report[0].offered_load.attempted' "$temporary/$label-observation/timing.json" >/dev/null 2>>"$P07_DIAGNOSTIC_DIR/$label.stderr" || leg_exit=1
					if test -d "$temporary/$label-observation"; then
						cp -R "$temporary/$label-observation" "$P07_DIAGNOSTIC_DIR/" || leg_exit=1
					fi
				fi
				jq -e --argjson rate "$rate" --argjson cpu "$cpu_workers" --argjson allocations "$allocation_expected" '.implementation == "go" and .transport == "secure" and .environment.gomaxprocs == 10 and .diagnostic.read_api == "read_into" and .diagnostic.scratch_slots == 4 and .diagnostic.warmup_operations == 128 and (.diagnostic.background_cpu_workers // 0) == $cpu and (.diagnostic.scratch == null) and .rows[0].operations == ($rate * 8) and .rows[0].size_bytes == 65536 and .rows[0].concurrency == 16 and .offered_load.config.rate_ops_per_second == $rate and .offered_load.config.issuance_window_ns == 8000000000 and .offered_load.config.deadline_ns == 500000000 and .offered_load.config.queue_capacity == 128 and .offered_load.config.workers == 16 and .offered_load.expected == ($rate * 8) and ((.offered_load.success + .offered_load.timeouts + .offered_load.overload + .offered_load.errors + .offered_load.canceled) == .offered_load.expected) and (.offered_load.outcomes | length) == ($rate * 8) and .offered_load.background.expected == $allocations and (.offered_load.background.completed + .offered_load.background.missed == $allocations)' "$P07_DIAGNOSTIC_DIR/$label.json" >/dev/null 2>>"$P07_DIAGNOSTIC_DIR/$label.stderr" || leg_exit=1
				if test "${P07_OFFERED_FACTORIAL:-}" = 1; then
					jq -e --arg case "$load_case" --argjson cpu "$cpu_workers" --argjson alloc "$allocation_workers" '.offered_load.config.factorial == true and .offered_load.config.load_case == $case and .offered_load.config.cpu_workers == $cpu and .offered_load.config.allocation_workers == $alloc and .offered_load.background.cpu_workers == $cpu and .offered_load.background.allocation_workers == $alloc and (.offered_load.background.workers | length) == ($cpu + $alloc) and (.diagnostic.background_allocations // false) == ($alloc > 0) and (.offered_load.background.hash_iterations == (.offered_load.background.warmup_hash_iterations + .offered_load.background.window_hash_iterations + .offered_load.background.drain_hash_iterations)) and (if $cpu == 0 then .offered_load.background.hash_iterations == 0 else true end)' "$P07_DIAGNOSTIC_DIR/$label.json" >/dev/null 2>>"$P07_DIAGNOSTIC_DIR/$label.stderr" || leg_exit=1
				fi
				if test "$docker_exit" -eq 0; then
					jq -e '.offered_load.failed == false and .offered_load.delivery_invalid == false and .offered_load.slo_failures == 0 and .offered_load.background.delivery_invalid == false' "$P07_DIAGNOSTIC_DIR/$label.json" >/dev/null 2>>"$P07_DIAGNOSTIC_DIR/$label.stderr" || leg_exit=1
				fi
				for artifact in "$temporary/$label-"*; do
					test ! -f "$artifact" || cp "$artifact" "$P07_DIAGNOSTIC_DIR/" || leg_exit=1
				 done
				printf '%s\n' "$leg_exit" >"$P07_DIAGNOSTIC_DIR/$label-capture-exit-code"
				if test "$leg_exit" -ne 0; then offered_failed=$((offered_failed + 1)); fi
				return 0
			}
			if test "${P07_OFFERED_OBSERVE:-}" = 1; then
				for repeat in 1 2 3; do
					case "$repeat" in
						1) case_order='none cpu alloc both' ;;
						2) case_order='cpu alloc both none' ;;
						3) case_order='alloc both none cpu' ;;
					 esac
					for load_case in $case_order; do offered_run "observed-$repeat-case-$load_case-rate-2000" 2000 "$load_case"; done
				 done
			else
			if test "${P07_OFFERED_FACTORIAL:-}" != 1; then
			for repeat in 1 2 3 4 5; do
				case "$repeat" in
					1|4) rate_order='1000 2000 4000' ;;
					2|5) rate_order='2000 4000 1000' ;;
					3) rate_order='4000 1000 2000' ;;
				 esac
				for rate in $rate_order; do offered_run "offered-$repeat-rate-$rate" "$rate"; done
			 done
			else
				for repeat in 1 2 3 4 5; do
					case "$repeat" in
						1|4) rate_order='1000 2000 4000' ;;
						2|5) rate_order='2000 4000 1000' ;;
						3) rate_order='4000 1000 2000' ;;
					 esac
					case "$repeat" in
						1|5) case_order='none cpu alloc both' ;;
						2) case_order='cpu alloc both none' ;;
						3) case_order='alloc both none cpu' ;;
						4) case_order='both none cpu alloc' ;;
					 esac
					for rate in $rate_order; do
						for load_case in $case_order; do offered_run "factorial-$repeat-case-$load_case-rate-$rate" "$rate" "$load_case"; done
					 done
				 done
			fi
			fi
			test "$offered_failed" -eq 0 || exit 1
			exit 0
		fi
		scheduler_run() {
			label=$1; implementation=$2; parallelism=$3; traced=$4
			operations=${8:-256}
			shape_concurrency=${10:-$read_concurrency}; shape_size=${11:-$read_size}
			count=4096; expected_concurrency=16; expected_size=65536
			if test "$implementation" = go; then
				count=$((operations * shape_concurrency)); expected_concurrency=$shape_concurrency; expected_size=$shape_size
			fi
			date -u +%Y-%m-%dT%H:%M:%SZ >"$P07_DIAGNOSTIC_DIR/$label-started-at"
			docker run --rm --platform "$platform" --network "$network" -v "$temporary:/work" \
				-e P07_READ_DIAGNOSTIC=1 -e GOGC=100 -e GOMEMLIMIT=off -e GOMAXPROCS="$parallelism" \
				-e P07_BACKGROUND_WORKERS="${P07_BACKGROUND_WORKERS:-0}" \
				-e P07_READ_INTO="${5:-0}" \
				-e P07_PROFILE_KIND="${9:-}" \
				-e P07_SCRATCH_SLOTS="${6:-}" -e P07_ADMISSION_WINDOW="${7:-}" \
				-e P07_OPERATIONS_PER_WORKER="$operations" -e P07_BACKGROUND_ALLOCATIONS="${P07_BACKGROUND_ALLOCATIONS:-0}" \
				"$image" sh -c '
					set -eu
					label=$1; implementation=$2; traced=$3
					if test "$implementation" = go; then
						export P07_READ_CONCURRENCY="$4" P07_READ_SIZE="$5"
					fi
						test -r /sys/fs/cgroup/cpu.max && test -r /sys/fs/cgroup/cpu.stat && test -r /sys/fs/cgroup/cpuset.cpus.effective
					for entry in cpu.max cpu.stat cpuset.cpus.effective memory.max; do
						if test -r "/sys/fs/cgroup/$entry"; then
							cat "/sys/fs/cgroup/$entry" >"/work/$label-$entry-before"
						fi
					 done
					if test "$traced" = yes; then
						export P07_TRACE_FILE="/work/$label.trace" P07_TIMING_FILE="/work/$label-timing.json"
					fi
					case "$P07_PROFILE_KIND" in
						cpu) export P07_CPU_PROFILE="/work/$label-cpu.pprof" ;;
						allocs) export P07_MEMORY_PROFILE="/work/$label-allocs.pprof" ;;
						"") ;;
						*) exit 2 ;;
					esac
					if test "$implementation" = native; then
						timeout 3600 /work/native-benchmark /work/ceph.conf /work/client.keyring p07-data secure
					else
						timeout 3600 /work/benchmark -monitors 172.30.97.10:3300 -key-file /work/client.key -fsid 11111111-2222-4333-8444-777777777777 -pool p07-data -transport secure
					fi
					if test -r /sys/fs/cgroup/cpu.stat; then
						cat /sys/fs/cgroup/cpu.stat >"/work/$label-cpu.stat-after"
					fi
				' sh "$label" "$implementation" "$traced" "$shape_concurrency" "$shape_size" >"$P07_DIAGNOSTIC_DIR/$label.json"
			date -u +%Y-%m-%dT%H:%M:%SZ >"$P07_DIAGNOSTIC_DIR/$label-finished-at"
			jq -e --argjson count "$count" --argjson size "$expected_size" --argjson concurrency "$expected_concurrency" --arg implementation "$implementation" '.implementation == $implementation and .transport == "secure" and (.rows | length) == 1 and .rows[0].operations == $count and .rows[0].size_bytes == $size and .rows[0].concurrency == $concurrency and .rows[0].workload == "read"' "$P07_DIAGNOSTIC_DIR/$label.json" >/dev/null
			if test "$implementation" = go; then
				jq -e --argjson parallelism "$parallelism" --argjson workers "${P07_BACKGROUND_WORKERS:-0}" '.implementation == "go" and .environment.gomaxprocs == $parallelism and (.diagnostic.background_cpu_workers // 0) == $workers' "$P07_DIAGNOSTIC_DIR/$label.json" >/dev/null
				jq -e --argjson warmup "$((shape_concurrency * 8))" --argjson operations "$operations" '.diagnostic.warmup_operations == $warmup and .diagnostic.operations_per_worker == $operations' "$P07_DIAGNOSTIC_DIR/$label.json" >/dev/null
				if test -n "${6:-}"; then
					jq -e --argjson slots "$6" --argjson window "$7" --argjson allocation "${P07_BACKGROUND_ALLOCATIONS:-0}" '.diagnostic.read_api == "read_into" and .diagnostic.scratch_slots == $slots and .diagnostic.admission_window == $window and (.diagnostic.background_allocations // false) == ($allocation == 1)' "$P07_DIAGNOSTIC_DIR/$label.json" >/dev/null
					if test "$shape_size" = 65536; then
						jq -e --argjson total "$((count + shape_concurrency * 8))" '(.diagnostic.scratch.hits + .diagnostic.scratch.misses) >= $total' "$P07_DIAGNOSTIC_DIR/$label.json" >/dev/null
					fi
				fi
			fi
			if test "$traced" = yes; then
				test -s "$temporary/$label.trace" && test -s "$temporary/$label-timing.json"
				jq -e --argjson count "$count" 'length == $count' "$temporary/$label-timing.json" >/dev/null
			fi
			for artifact in "$temporary/$label-"* "$temporary/$label.trace"; do
				test ! -f "$artifact" || cp "$artifact" "$P07_DIAGNOSTIC_DIR/"
			 done
		}
		for repeat in 1 2 3 4 5; do
			scheduler_run "sweep-$repeat-native-before" native 10 no
			if test "${P07_READ_SCALE:-}" = 1; then
				case "$repeat" in
					1|4) concurrency_order='16 32 64' ;;
					2|5) concurrency_order='32 64 16' ;;
					3) concurrency_order='64 16 32' ;;
				 esac
				case "$repeat" in 1|3|5) slot_order='4 8' ;; 2|4) slot_order='8 4' ;; esac
				for shape_workers in $concurrency_order; do
					for slots in $slot_order; do
						scheduler_run "scale-$repeat-concurrency-$shape_workers-slots-$slots" go 10 no 1 "$slots" "$shape_workers" 1024 '' "$shape_workers" "$read_size"
					 done
				 done
				scheduler_run "sweep-$repeat-native-after" native 10 no
				continue
			fi
			if test "${P07_READ_PROFILE:-}" = 1; then
				scheduler_run "profile-baseline-$repeat" go 10 no 1 '' 16 1024
				scheduler_run "sweep-$repeat-native-after" native 10 no
				continue
			fi
			if test "${P07_DEFAULT_SCRATCH_CONFIRM:-}" = 1; then
				scheduler_run "default-$repeat" go 10 no 1 '' 16 1024
				scheduler_run "sweep-$repeat-native-after" native 10 no
				continue
			fi
			if test "${P07_INVENTORY_SWEEP:-}" = 1; then
				case "$repeat" in
					1) variants='1:16 2:16 4:16 1:8 1:4' ;;
					2) variants='2:16 4:16 1:8 1:4 1:16' ;;
					3) variants='4:16 1:8 1:4 1:16 2:16' ;;
					4) variants='1:8 1:4 1:16 2:16 4:16' ;;
					5) variants='1:4 1:16 2:16 4:16 1:8' ;;
				esac
				for variant in $variants; do
					slots=${variant%:*}; window=${variant#*:}
					scheduler_run "inventory-$repeat-slots-$slots-window-$window" go 10 no 1 "$slots" "$window" 1024
				 done
				scheduler_run "sweep-$repeat-native-after" native 10 no
				continue
			fi
			case "$repeat" in
				1|3|5) order='2 4 6 10' ;;
				2|4) order='10 6 4 2' ;;
			esac
			if test "${P07_SWEEP_FIXED_PROCS:-}" = 10; then order=10; fi
			for parallelism in $order; do
				if test "${P07_READ_INTO_COMPARE:-}" = 1; then
					case "$repeat" in 2|4) scheduler_run "sweep-$repeat-procs-$parallelism-into" go "$parallelism" no 1 ;; esac
				fi
				scheduler_run "sweep-$repeat-procs-$parallelism" go "$parallelism" no
				if test "${P07_READ_INTO_COMPARE:-}" = 1; then
					case "$repeat" in 1|3|5) scheduler_run "sweep-$repeat-procs-$parallelism-into" go "$parallelism" no 1 ;; esac
				fi
			 done
			scheduler_run "sweep-$repeat-native-after" native 10 no
		 done
		if test "${P07_READ_PROFILE:-}" = 1; then
			scheduler_run profile-cpu go 10 no 1 '' 16 1024 cpu
			scheduler_run profile-allocs go 10 no 1 '' 16 1024 allocs
			scheduler_run profile-trace go 10 yes 1 '' 16 1024
			trace_order=''
		elif test "${P07_READ_SCALE:-}" = 1; then
			trace_order=''
			jq -n --argjson size "$read_size" '{kind:"read-scale-diagnostic",benchmark_claim:false,matched_native:false,go:{size_bytes:$size,concurrencies:[16,32,64],scratch_slots:[4,8],operations_per_worker:1024,measured_legs:30},native_context:{size_bytes:65536,concurrency:16,operations:4096,scope:"shorter unmatched context"}}' >"$P07_DIAGNOSTIC_DIR/scale-methodology.json"
		else
			trace_order='2 10'
			if test "${P07_SWEEP_FIXED_PROCS:-}" = 10; then trace_order=10; fi
		fi
		for parallelism in $trace_order; do
			scheduler_run "trace-procs-$parallelism" go "$parallelism" yes
		 done
		git ls-files -co --exclude-standard -z -- '*.go' '*.mjs' go.mod go.sum integration/p07/reproduce.sh integration/p07/native_driver.c integration/p07/native_benchmark.c |
			xargs -0 shasum -a 256 >"$P07_DIAGNOSTIC_DIR/source-after.sha256"
		cmp "$P07_DIAGNOSTIC_DIR/source-before.sha256" "$P07_DIAGNOSTIC_DIR/source-after.sha256"
		(cd "$P07_DIAGNOSTIC_DIR" && shasum -a 256 benchmark native-benchmark *.json source-before.sha256 && for artifact in *.trace; do
			test ! -f "$artifact" || shasum -a 256 "$artifact" || exit 1
		done) >"$P07_DIAGNOSTIC_DIR/artifacts.sha256"
		if test "${P07_READ_PROFILE:-}" = 1; then
			test -s "$P07_DIAGNOSTIC_DIR/profile-cpu-cpu.pprof" && test -s "$P07_DIAGNOSTIC_DIR/profile-allocs-allocs.pprof"
			(cd "$P07_DIAGNOSTIC_DIR" && shasum -a 256 *.pprof) >>"$P07_DIAGNOSTIC_DIR/artifacts.sha256"
		fi
		printf '%s\n' 'passed' >"$P07_DIAGNOSTIC_DIR/scheduler-status"
		printf 'P07 serial scheduler sweep written to %s\n' "$P07_DIAGNOSTIC_DIR"
		exit 0
	fi
	for mode in timing cpu memory nogc procs2; do
		case "$mode" in
			timing) diagnostic_env=P07_TIMING_FILE=/work/request-timing.json ;;
			cpu) diagnostic_env=P07_CPU_PROFILE=/work/cpu.pprof ;;
			memory) diagnostic_env=P07_MEMORY_PROFILE=/work/allocs.pprof ;;
			nogc) diagnostic_env=GOGC=off ;;
			procs2) diagnostic_env=GOMAXPROCS=2 ;;
		esac
		docker run --rm --platform "$platform" --network "$network" -v "$temporary:/work" -e P07_READ_DIAGNOSTIC=1 -e "$diagnostic_env" \
			-e P07_READ_SIZE="$read_size" -e P07_READ_CONCURRENCY="$read_concurrency" "$image" \
			timeout 3600 /work/benchmark -monitors 172.30.97.10:3300 -key-file /work/client.key -fsid "$fsid" -pool p07-data -transport secure >"$P07_DIAGNOSTIC_DIR/go-$mode.json"
		jq -e --argjson count "$((read_concurrency * 256))" --argjson size "$read_size" --argjson concurrency "$read_concurrency" '.rows[0].operations == $count and .rows[0].size_bytes == $size and .rows[0].concurrency == $concurrency' "$P07_DIAGNOSTIC_DIR/go-$mode.json" >/dev/null
	done
	cp "$temporary/request-timing.json" "$temporary/cpu.pprof" "$temporary/allocs.pprof" "$temporary/benchmark" "$P07_DIAGNOSTIC_DIR/"
	printf '%s\n' "P07 isolated read diagnostics written to $P07_DIAGNOSTIC_DIR"
	exit 0
fi

for transport in secure crc; do
	docker run --rm --platform "$platform" --network "$network" -v "$temporary:/cluster" "$image" \
		timeout 3600 /cluster/native-benchmark /cluster/ceph.conf /cluster/client.keyring p07-data "$transport" >"$temporary/benchmark-native-$transport.json" || benchmark_failed
	jq -e --arg transport "$transport" '.implementation == "native" and .transport == $transport and (.rows | length) == 36' "$temporary/benchmark-native-$transport.json" >/dev/null
	docker run --rm --platform "$platform" --network "$network" -v "$temporary:/work" "$image" \
		timeout 3600 /work/benchmark -monitors 172.30.97.10:3300 -key-file /work/client.key -fsid "$fsid" -pool p07-data -transport "$transport" >"$temporary/benchmark-go-$transport.json" || benchmark_failed
	jq -e --arg transport "$transport" '.implementation == "go" and .transport == $transport and (.rows | length) == 36' "$temporary/benchmark-go-$transport.json" >/dev/null
done

probe_id=$(docker run -d --name "p07-probe-$$" --platform "$platform" --network "$network" --ip 172.30.97.40 -v "$temporary:/work" "$image" \
	/work/probe -monitors 172.30.97.10:3300 -key /work/client.key -fsid "$fsid" -control /work)
for attempt in $(seq 1 120); do
	if test -e "$temporary/ready"; then break; fi
	probe_running=$(docker inspect -f '{{.State.Running}}' "p07-probe-$$" 2>/dev/null || printf false)
	test "$probe_running" = true || { docker logs "p07-probe-$$" >&2; for id in 0 1 2; do docker logs "p07-osd-$id-$$" >&2; done; exit 1; }
	test "$attempt" -lt 120 || { docker logs "p07-probe-$$" >&2; for id in 0 1 2; do docker logs "p07-osd-$id-$$" >&2; done; exit 1; }
	sleep 1
done
rados_cli get go-parity /cluster/parity.actual
cmp "$temporary/parity.expected" "$temporary/parity.actual"
primary=$(ceph_cli osd map p07-data remap-append --format json | jq -r '.acting_primary')
docker pause "p07-osd-$primary-$$" >/dev/null
touch "$temporary/submit"
for attempt in $(seq 1 120); do
	if test -e "$temporary/blocked"; then break; fi
	probe_running=$(docker inspect -f '{{.State.Running}}' "p07-probe-$$" 2>/dev/null || printf false)
	test "$probe_running" = true || { docker logs "p07-probe-$$" >&2; exit 1; }
	test "$attempt" -lt 120 || exit 1
	sleep 0.1
done
docker rm -f "p07-osd-$primary-$$" >/dev/null
ceph_cli osd down "$primary" >/dev/null
for attempt in $(seq 1 60); do
	remapped_mapping=$(ceph_cli osd map p07-data remap-append --format json)
	new_primary=$(printf '%s' "$remapped_mapping" | jq -r '.acting_primary')
	if test "$new_primary" != "$primary"; then break; fi
	test "$attempt" -lt 60 || exit 1
	sleep 1
done
printf 'native remap: %s\n' "$remapped_mapping"
exit_code=$(docker wait "p07-probe-$$")
test "$exit_code" = 0 || { docker logs "p07-probe-$$" >&2; for id in 0 1 2; do docker logs "p07-osd-$id-$$" >&2 2>/dev/null || true; done; exit 1; }
docker logs "p07-probe-$$" >"$temporary/probe.json"
jq -e '.create and .exclusive and .write and .write_full and .append and .truncate and .zero and .remove and .flush and .primary_change and .version > 0' "$temporary/probe.json" >/dev/null
rados_cli get remap-append /cluster/remap.actual
test "$(cat "$temporary/remap.actual")" = 'base!'
docker run --rm --platform "$platform" --network "$network" -v "$temporary:/cluster" "$image" \
	timeout 60 /cluster/native-driver verify /cluster/ceph.conf /cluster/admin.keyring p07-data >"$temporary/native-verify.json"
jq -e '.mixed_go_native and .go_write_native_read and .remap_append_once' "$temporary/native-verify.json" >/dev/null

mkdir -p docs/p07
docker run --rm --platform "$platform" -v "$temporary:/cluster" "$image" sh -c '
	set -eu
	ceph --version > /cluster/ceph.version
	sha256sum "$(command -v ceph-mon)" | awk "{print \$1}" > /cluster/ceph-mon.sha256
	sha256sum "$(command -v ceph-osd)" | awk "{print \$1}" > /cluster/ceph-osd.sha256
	library=$(ldconfig -p | awk "/librados.so.2/{print \$NF; exit}")
	test -n "$library"
	readlink -f "$library" > /cluster/librados.path
	sha256sum "$(readlink -f "$library")" | awk "{print \$1}" > /cluster/librados.sha256
	rpm -qf "$(readlink -f "$library")" > /cluster/librados.package
	uname -srmo > /cluster/kernel
	cpu_model=$(awk -F: "/^(model name|Model|Hardware)/ {sub(/^[[:space:]]+/, \"\", \$2); print \$2; exit}" /proc/cpuinfo)
	test -n "$cpu_model" || cpu_model=$(uname -m)
	printf "%s\n" "$cpu_model" > /cluster/cpu.model
	getconf _NPROCESSORS_ONLN > /cluster/logical-cpus
	cat /sys/fs/cgroup/cpu.max > /cluster/cpu.max
	cat /sys/fs/cgroup/memory.max > /cluster/memory.max
'
artifacts="$temporary/artifacts.json"
printf '{}\n' >"$artifacts"
artifact_paths=$(find internal/cephx internal/crush internal/encoding internal/maps internal/mon internal/msgr internal/objecter internal/osd internal/protocol -type f -name '*.go' -print; printf '%s\n' Makefile README.md SPEC.md client.go client_test.go doc.go errors.go errors_test.go object.go docs/p00/api-inventory.csv integration/README.md integration/p07/benchmark/main.go integration/p07/benchmark/main_unsupported.go integration/p07/native_benchmark.c integration/p07/native_driver.c integration/p07/probe/main.go integration/p07/report.schema.json integration/p07/reproduce.sh tools/p07-verify/main.go tools/p07-verify/main_test.go docs/p07/tasks.md docs/p07/provenance.md go.mod go.sum)
for artifact in $artifact_paths; do
	test -f "$artifact"
	hash=$(shasum -a 256 "$artifact" | awk '{print $1}')
	jq --arg path "$artifact" --arg hash "$hash" '. + {($path):$hash}' "$artifacts" >"$artifacts.next"
	mv "$artifacts.next" "$artifacts"
done
jq -n \
	--arg started_at "$started_at" \
	--arg finished_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
	--arg image "$image" \
	--arg platform "$platform" \
	--arg ceph_version "$(cat "$temporary/ceph.version")" \
	--arg ceph_mon_sha256 "$(cat "$temporary/ceph-mon.sha256")" \
	--arg ceph_osd_sha256 "$(cat "$temporary/ceph-osd.sha256")" \
	--arg librados_path "$(cat "$temporary/librados.path")" \
	--arg librados_sha256 "$(cat "$temporary/librados.sha256")" \
	--arg librados_package "$(cat "$temporary/librados.package")" \
	--arg kernel "$(cat "$temporary/kernel")" \
	--arg cpu_model "$(cat "$temporary/cpu.model")" \
	--argjson logical_cpus "$(cat "$temporary/logical-cpus")" \
	--arg cpu_max "$(cat "$temporary/cpu.max")" \
	--arg memory_max "$(cat "$temporary/memory.max")" \
	--arg docker_server_version "$(docker version --format '{{.Server.Version}}')" \
	--argjson artifacts "$(cat "$artifacts")" \
	--argjson probe "$(cat "$temporary/probe.json")" \
	--argjson native_seed "$(cat "$temporary/native-seed.json")" \
	--argjson native_verify "$(cat "$temporary/native-verify.json")" \
	--argjson go_secure "$(cat "$temporary/benchmark-go-secure.json")" \
	--argjson go_crc "$(cat "$temporary/benchmark-go-crc.json")" \
	--argjson native_secure "$(cat "$temporary/benchmark-native-secure.json")" \
	--argjson native_crc "$(cat "$temporary/benchmark-native-crc.json")" \
	'{schema_version:1,status:"passed",command:"make integration-p07",started_at:$started_at,finished_at:$finished_at,source:{repository:"https://github.com/otuschhoff/rados-go.git",identity:"content-addressed-artifacts",artifacts:$artifacts},server:{repository:"https://github.com/ceph/ceph.git",source_anchor_commit:"7f793731f1b39eb4f465e960113d2363c311b964",version:$ceph_version,image:$image,platform:$platform,binaries:{mon_sha256:$ceph_mon_sha256,osd_sha256:$ceph_osd_sha256}},native_runtime:{soname:"librados.so.2",path:$librados_path,package:$librados_package,sha256:$librados_sha256},cluster:{fsid:"11111111-2222-4333-8444-777777777777",osds:3,pool:"p07-data",replicas:2,osd_device_bytes:8589934592},scenarios:{go_crud:"passed",native_crud:"passed",native_seed_go_mutate_native_verify:"passed",go_write_native_read:"passed",exclusive_create:"passed",missing_semantics:"passed",flush:"passed",primary_change_append_once:"passed"},probe:$probe,native:{seed:$native_seed,verify:$native_verify},benchmark:{execution_environment:{kernel:$kernel,cpu_model:$cpu_model,logical_cpus:$logical_cpus,cpu_max:$cpu_max,memory_max:$memory_max,docker_server_version:$docker_server_version},methodology:{sizes_bytes:[4096,65536,1048576,4194304],concurrency:[1,16,64],workloads:["read","write","mixed"],operations_per_worker:2,transports:["secure","crc"],allocation_measurement:{go:"runtime.MemStats deltas",native:"unavailable from the dynamically loaded librados ABI"},results_are_baseline_not_parity_claim:true},runs:[$go_secure,$go_crc,$native_secure,$native_crc]}}' >"$report"
printf '%s\n' 'P07 real mutation interoperability passed'