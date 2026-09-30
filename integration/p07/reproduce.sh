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
if test -z "$capture"; then trap cleanup EXIT HUP INT TERM; fi
compile() {
	if test -n "$capture"; then
		perl -MJSON::PP -e 'print JSON::PP->new->canonical->encode({cwd=>shift @ARGV,argv=>\@ARGV}), "\n"' "$PWD" "$@" >>"$capture/compile-commands.jsonl"
	fi
	"$@"
}

stage=go-build
compile env CGO_ENABLED=0 GOOS=linux GOARCH="$goarch" go build -trimpath -o "$temporary/probe" ./integration/p07/probe
compile env CGO_ENABLED=0 GOOS=linux GOARCH="$goarch" go build -trimpath -o "$temporary/benchmark" ./integration/p07/benchmark
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
	docker run --rm --platform "$platform" --network "$network" -v "$temporary:/work" -e P07_SEED_ONLY=1 "$image" \
		timeout 60 /work/benchmark -monitors 172.30.97.10:3300 -key-file /work/client.key -fsid "$fsid" -pool p07-data -transport secure >"$P07_DIAGNOSTIC_DIR/seed.json"
	jq -e '.seeded' "$P07_DIAGNOSTIC_DIR/seed.json" >/dev/null
	for leg in native-1 go-1 go-2 native-2; do
		for id in 0 1 2; do
			ceph_cli tell "osd.$id" perf dump >"$P07_DIAGNOSTIC_DIR/$leg-osd-$id-before.json"
		done
		date -u +%Y-%m-%dT%H:%M:%SZ >"$P07_DIAGNOSTIC_DIR/$leg-started-at"
		case "$leg" in
			native-*)
				docker run --rm --platform "$platform" --network "$network" -v "$temporary:/cluster" -e P07_READ_DIAGNOSTIC=1 "$image" \
					timeout 3600 /cluster/native-benchmark /cluster/ceph.conf /cluster/client.keyring p07-data secure >"$P07_DIAGNOSTIC_DIR/$leg.json" ;;
			go-*)
				docker run --rm --platform "$platform" --network "$network" -v "$temporary:/work" -e P07_READ_DIAGNOSTIC=1 "$image" \
					timeout 3600 /work/benchmark -monitors 172.30.97.10:3300 -key-file /work/client.key -fsid "$fsid" -pool p07-data -transport secure >"$P07_DIAGNOSTIC_DIR/$leg.json" ;;
		esac
		date -u +%Y-%m-%dT%H:%M:%SZ >"$P07_DIAGNOSTIC_DIR/$leg-finished-at"
		jq -e '.transport == "secure" and (.rows | length) == 1 and .rows[0].operations == 4096 and .rows[0].workload == "read"' "$P07_DIAGNOSTIC_DIR/$leg.json" >/dev/null
		for id in 0 1 2; do
			ceph_cli tell "osd.$id" perf dump >"$P07_DIAGNOSTIC_DIR/$leg-osd-$id-after.json"
		done
	done
	for mode in timing cpu memory nogc procs2; do
		case "$mode" in
			timing) diagnostic_env=P07_TIMING_FILE=/work/request-timing.json ;;
			cpu) diagnostic_env=P07_CPU_PROFILE=/work/cpu.pprof ;;
			memory) diagnostic_env=P07_MEMORY_PROFILE=/work/allocs.pprof ;;
			nogc) diagnostic_env=GOGC=off ;;
			procs2) diagnostic_env=GOMAXPROCS=2 ;;
		esac
		docker run --rm --platform "$platform" --network "$network" -v "$temporary:/work" -e P07_READ_DIAGNOSTIC=1 -e "$diagnostic_env" "$image" \
			timeout 3600 /work/benchmark -monitors 172.30.97.10:3300 -key-file /work/client.key -fsid "$fsid" -pool p07-data -transport secure >"$P07_DIAGNOSTIC_DIR/go-$mode.json"
		jq -e '.rows[0].operations == 4096' "$P07_DIAGNOSTIC_DIR/go-$mode.json" >/dev/null
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