#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
cd "$root"
started_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
repository_commit=$(git rev-parse HEAD)
repository_dirty=false
if test -n "$(git status --porcelain --untracked-files=all)"; then
	repository_dirty=true
fi
case "$(docker info --format '{{.Architecture}}')" in
	x86_64|amd64) host_platform=linux/amd64 ;;
	aarch64|arm64) host_platform=linux/arm64 ;;
	*) printf 'unsupported native Docker architecture\n' >&2; exit 2 ;;
esac
platform=${P03_PLATFORM:-$host_platform}
if test "${CI:-}" = true && test "$platform" != "$host_platform"; then
	printf 'CI platform %s is not native on Docker host %s\n' "$platform" "$host_platform" >&2
	exit 2
fi
goarch=${platform#linux/}
case "$platform" in
	linux/amd64|linux/arm64) ;;
	*) printf 'unsupported P03 platform: %s\n' "$platform" >&2; exit 2 ;;
esac
image_index=$(jq -r '.images.qualification.reference' docs/p00/evidence.json)
image_digest=$(jq -r --arg architecture "$goarch" '.images.qualification[$architecture]' docs/p00/evidence.json)
image="${image_index%@*}@$image_digest"
temporary=$(mktemp -d)
source_snapshot="$temporary/source"
network="go-librados-p03-$$"
monitor="go-librados-p03-mon-$$"
subnet="172.30.93.0/24"
monitor_ip="172.30.93.10"
client_ip="172.30.93.20"
fsid="11111111-2222-4333-8444-555555555555"
report=${P03_REPORT:-"$root/docs/p03/integration-report.json"}

cleanup() {
	docker rm -f "$monitor" >/dev/null 2>&1 || true
	docker network rm "$network" >/dev/null 2>&1 || true
	docker run --rm --user 0 --platform "$platform" \
		-v "$temporary:/cluster" "$image" find /cluster -mindepth 1 -delete >/dev/null 2>&1 || true
	rm -rf "$temporary"
}
trap cleanup EXIT HUP INT TERM

implementation_files=$(
	{
		find internal/cephx internal/encoding internal/msgr internal/protocol -type f -name '*.go'
		printf '%s\n' go.mod go.sum integration/p03/legacy-crush.txt integration/p03/probe/main.go integration/p03/report.schema.json integration/p03/reproduce.sh
	} | LC_ALL=C sort
)
hash_implementation() {
	implementation_root=$1
	for artifact in $implementation_files; do
		printf '%s\t%s\n' "$(shasum -a 256 "$implementation_root/$artifact" | awk '{print $1}')" "$artifact"
	done | jq -Rn '[inputs | split("\t") | {(.[1]): .[0]}] | add'
}
mkdir -p "$source_snapshot/integration/p03/probe"
cp go.mod go.sum "$source_snapshot/"
cp -R internal "$source_snapshot/"
cp integration/p03/legacy-crush.txt integration/p03/report.schema.json integration/p03/reproduce.sh "$source_snapshot/integration/p03/"
cp integration/p03/probe/main.go "$source_snapshot/integration/p03/probe/"
artifacts=$(hash_implementation "$source_snapshot")

CGO_ENABLED=0 GOOS=linux GOARCH="$goarch" go -C "$source_snapshot" build -trimpath -o "$temporary/probe" ./integration/p03/probe
docker network create --subnet "$subnet" "$network" >/dev/null

docker run --rm --user 0 --platform "$platform" \
	-v "$temporary:/cluster" "$image" sh -c '
		set -eu
		ceph-authtool /cluster/mon.keyring --create-keyring --gen-key -n mon. --cap mon "allow *"
		ceph-authtool /cluster/admin.keyring --create-keyring --gen-key -n client.admin --cap mon "allow *" --cap osd "allow *" --cap mgr "allow *"
		ceph-authtool /cluster/client.keyring --create-keyring --gen-key -n client.p03 --cap mon "allow r"
		ceph-authtool /cluster/bad.keyring --create-keyring --gen-key -n client.p03
		ceph-authtool /cluster/mon.keyring --import-keyring /cluster/admin.keyring
		ceph-authtool /cluster/mon.keyring --import-keyring /cluster/client.keyring
		monmaptool --create --fsid '"$fsid"' --addv a "[v2:'"$monitor_ip"':3300/0]" /cluster/monmap
		mkdir -p /cluster/mondata
		ceph-mon --mkfs -i a --fsid '"$fsid"' --monmap /cluster/monmap --keyring /cluster/mon.keyring --mon-data /cluster/mondata
		chown -R ceph:ceph /cluster/mondata /cluster/*.keyring /cluster/monmap
		chmod 755 /cluster
	'

start_monitor() {
	mode=$1
	docker run -d --name "$monitor" --platform "$platform" \
		--network "$network" --ip "$monitor_ip" \
		-v "$temporary:/cluster" "$image" \
		ceph-mon -f -i a --mon-data /cluster/mondata \
		--public-addr "v2:$monitor_ip:3300" --setuser ceph --setgroup ceph \
		--mon-data-avail-crit 1 --ms-mon-service-mode "$mode" \
		--auth-mon-ticket-ttl 4 --auth-service-ticket-ttl 4 \
		--no-mon-cluster-log-to-stderr >/dev/null
	for attempt in 1 2 3 4 5 6 7 8 9 10; do
		if docker run --rm --platform "$platform" --network "$network" "$image" \
			bash -c "</dev/tcp/$monitor_ip/3300"; then
			return 0
		fi
		sleep 1
	done
	docker logs "$monitor" >&2 || true
	return 1
}

run_probe() {
	address=$1
	keyring=$2
	shift 2
	docker run --rm --platform "$platform" --network "$network" --ip "$address" \
		-v "$temporary/probe:/probe:ro" -v "$temporary/$keyring:/client.keyring:ro" \
		"$image" /probe -monitor "$monitor_ip:3300" \
		-client-address "$address:0" -keyring /client.keyring -timeout 10s "$@"
}

start_monitor "secure crc"
docker run --rm --platform "$platform" --network "$network" \
	-v "$temporary/admin.keyring:/admin.keyring:ro" \
	-v "$temporary:/cluster" -v "$source_snapshot/integration/p03:/input:ro" "$image" sh -c '
		set -eu
		crushtool -c /input/legacy-crush.txt -o /cluster/legacy-crush.bin
		ceph --conf /dev/null --mon-host v2:'"$monitor_ip"':3300 --name client.admin \
			--keyring /admin.keyring osd setcrushmap -i /cluster/legacy-crush.bin >/dev/null
	'

run_probe "$client_ip" client.keyring -exercise-lifecycle >"$temporary/success.json"
jq -e '
	.authenticated_mode == "secure" and
	(.global_id > 0) and
	.reconnected and .ticket_renewed and .expiry_rejected and .expired_reconnect and
	(.server_addresses | index("'"$monitor_ip"':3300") != null)
' "$temporary/success.json" >/dev/null

set +e
wrong_output=$(run_probe "172.30.93.21" bad.keyring 2>&1)
wrong_status=$?
set -e
test "$wrong_status" -ne 0
case "$wrong_output" in
	*"cephx authentication rejected"*) ;;
	*) printf '%s\n' "$wrong_output" >&2; exit 1 ;;
esac

docker rm -f "$monitor" >/dev/null
start_monitor "crc"
set +e
downgrade_output=$(run_probe "$client_ip" client.keyring 2>&1)
downgrade_status=$?
set -e
test "$downgrade_status" -ne 0
case "$downgrade_output" in
	*"cephx auth downgrade rejected"*) ;;
	*) printf '%s\n' "$downgrade_output" >&2; exit 1 ;;
esac

runtime_package=$(docker exec "$monitor" rpm -q --qf '%{NAME}-%{VERSION}-%{RELEASE}' ceph-mon)
monitor_binary_sha256=$(docker exec "$monitor" sh -c 'sha256sum "$(command -v ceph-mon)"' | awk '{print $1}')

test "$(hash_implementation "$root")" = "$artifacts"
jq -n \
	--arg repository "https://github.com/otuschhoff/go-librados.git" \
	--arg repository_commit "$repository_commit" \
	--argjson repository_dirty "$repository_dirty" \
	--arg ceph_repository "https://github.com/ceph/ceph.git" \
	--arg source_anchor_commit "7f793731f1b39eb4f465e960113d2363c311b964" \
	--arg image "$image" \
	--arg platform "$platform" \
	--arg host_platform "$host_platform" \
	--arg runtime_package "$runtime_package" \
	--arg monitor_binary_sha256 "$monitor_binary_sha256" \
	--arg started_at "$started_at" \
	--arg finished_at "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
	--argjson artifacts "$artifacts" \
	--slurpfile probe "$temporary/success.json" \
	'{
		schema_version: 1,
		status: "passed",
		command: "make integration-p03",
		started_at: $started_at,
		finished_at: $finished_at,
		source: {repository: $repository, repository_commit: $repository_commit, dirty: $repository_dirty, identity: "content-addressed-artifacts", artifacts: $artifacts},
		server: {repository: $ceph_repository, source_anchor_commit: $source_anchor_commit, image: $image, platform: $platform, host_platform: $host_platform, runtime_package: $runtime_package, monitor_binary_sha256: $monitor_binary_sha256},
		scenarios: {
			secure_authentication: "passed",
			client_server_ident: "passed",
			same_session_renewal: "passed",
			global_id_reuse: "passed",
			ticket_renewal: "passed",
			expired_ticket_rejection: "passed",
			post_expiry_reconnect: "passed",
			valid_wrong_key_rejection: "passed",
			secure_to_crc_downgrade_rejection: "passed"
		},
		probe: $probe[0]
	}' >"$temporary/integration-report.json"
mkdir -p "$(dirname "$report")"
cp "$temporary/integration-report.json" "$report"

jq -c . "$temporary/success.json"
printf 'P03 integration report: %s\n' "$report"
printf '%s\n' 'P03 real monitor authentication passed'