#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
cd "$root"
started_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
case "$(docker info --format '{{.Architecture}}')" in
	x86_64|amd64) platform=linux/amd64; goarch=amd64 ;;
	aarch64|arm64) platform=linux/arm64; goarch=arm64 ;;
	*) printf '%s\n' 'unsupported Docker architecture' >&2; exit 2 ;;
esac
image_index=$(jq -r '.images.qualification.reference' docs/p00/evidence.json)
image_digest=$(jq -r --arg architecture "$goarch" '.images.qualification[$architecture]' docs/p00/evidence.json)
image="${image_index%@*}@$image_digest"
temporary=$(mktemp -d)
network="go-librados-p04-$$"
client_container="go-librados-p04-client-$$"
fsid="11111111-2222-4333-8444-555555555555"
foreign_fsid="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
report=${P04_REPORT:-"$root/docs/p04/integration-report.json"}
implementation_files=$(
	{
		find internal/cephx internal/encoding internal/maps internal/mon internal/msgr internal/protocol -type f -name '*.go'
		printf '%s\n' Makefile go.mod go.sum integration/p04/probe/main.go integration/p04/report.schema.json integration/p04/reproduce-fixtures.sh integration/p04/reproduce.sh
	} | LC_ALL=C sort
)
hash_implementation() {
	for artifact in $implementation_files; do
		printf '%s\t%s\n' "$(shasum -a 256 "$artifact" | awk '{print $1}')" "$artifact"
	done | jq -Rn '[inputs | split("\t") | {(.[1]): .[0]}] | add'
}
artifacts=$(hash_implementation)
cleanup() {
	docker rm -f "$client_container" "p04-mon-a-$$" "p04-mon-b-$$" "p04-mon-c-$$" "p04-mon-d-$$" >/dev/null 2>&1 || true
	docker network rm "$network" >/dev/null 2>&1 || true
	rm -rf "$temporary"
}
trap cleanup EXIT HUP INT TERM

CGO_ENABLED=0 GOOS=linux GOARCH="$goarch" go build -trimpath -o "$temporary/probe" ./integration/p04/probe
docker network create --subnet 172.30.94.0/24 "$network" >/dev/null
docker run --rm --user 0 --platform "$platform" -v "$temporary:/cluster" "$image" sh -c '
	set -eu
	ceph-authtool /cluster/mon.keyring --create-keyring --gen-key -n mon. --cap mon "allow *"
	ceph-authtool /cluster/admin.keyring --create-keyring --gen-key -n client.admin --cap mon "allow *" --cap osd "allow *" --cap mgr "allow *"
	ceph-authtool /cluster/client.keyring --create-keyring --gen-key -n client.p04 --cap mon "allow r"
	ceph-authtool /cluster/mon.keyring --import-keyring /cluster/admin.keyring
	ceph-authtool /cluster/mon.keyring --import-keyring /cluster/client.keyring
	monmaptool --create --fsid '"$fsid"' --addv a "[v2:172.30.94.10:3300/0]" --addv b "[v2:172.30.94.11:3300/0]" --addv c "[v2:172.30.94.12:3300/0]" /cluster/monmap
	for id in a b c; do mkdir -p /cluster/mondata-$id; ceph-mon --mkfs -i $id --fsid '"$fsid"' --monmap /cluster/monmap --keyring /cluster/mon.keyring --mon-data /cluster/mondata-$id; done
	ceph-authtool /cluster/foreign-mon.keyring --create-keyring --gen-key -n mon. --cap mon "allow *"
	ceph-authtool /cluster/foreign-mon.keyring --import-keyring /cluster/client.keyring
	monmaptool --create --fsid '"$foreign_fsid"' --addv d "[v2:172.30.94.13:3300/0]" /cluster/foreign-monmap
	mkdir -p /cluster/mondata-d
	ceph-mon --mkfs -i d --fsid '"$foreign_fsid"' --monmap /cluster/foreign-monmap --keyring /cluster/foreign-mon.keyring --mon-data /cluster/mondata-d
	chown -R ceph:ceph /cluster
'
start_mon() {
	id=$1; ip=$2
	docker run -d --rm --name "p04-mon-$id-$$" --platform "$platform" --network "$network" --ip "$ip" -v "$temporary:/cluster" "$image" \
		ceph-mon -f -i "$id" --mon-data "/cluster/mondata-$id" --public-addr "v2:$ip:3300" --setuser ceph --setgroup ceph --mon-data-avail-crit 1 --no-mon-cluster-log-to-stderr >/dev/null
}
ceph_cli() {
	docker run --rm --platform "$platform" --network "$network" -v "$temporary/admin.keyring:/admin.keyring:ro" "$image" \
		ceph --conf /dev/null --mon-host "v2:172.30.94.11:3300" --name client.admin --keyring /admin.keyring "$@"
}
wait_file() {
	path=$1
	for attempt in $(seq 1 60); do test -e "$path" && return 0; sleep 1; done
	docker logs "$client_container" >&2 || true
	return 1
}
start_mon a 172.30.94.10
start_mon b 172.30.94.11
start_mon c 172.30.94.12
start_mon d 172.30.94.13
for attempt in $(seq 1 30); do
	if ceph_cli quorum_status --format json 2>/dev/null | jq -e '.quorum | length == 3' >/dev/null; then break; fi
	test "$attempt" -lt 30 || exit 1
	sleep 1
done
ceph_cli osd pool create p04-initial 8 8 replicated >/dev/null

docker run -d --name "$client_container" --platform "$platform" --network "$network" --ip 172.30.94.20 \
	-v "$temporary:/work" "$image" /work/probe \
	-monitors 172.30.94.10:3300,172.30.94.11:3300,172.30.94.12:3300 -foreign-monitor 172.30.94.13:3300 \
	-client-address 172.30.94.20:0 -keyring /work/client.keyring -fsid "$fsid" -control /work -timeout 60s >/dev/null
wait_file "$temporary/ready"
ceph_cli osd pool create p04-mutated 8 8 replicated >/dev/null
wait_file "$temporary/mutated"
docker rm -f "p04-mon-a-$$" >/dev/null
for attempt in $(seq 1 30); do
	if ceph_cli quorum_status --format json 2>/dev/null | jq -e '.quorum | length == 2' >/dev/null; then break; fi
	test "$attempt" -lt 30 || exit 1
	sleep 1
done
ceph_cli osd pool create p04-failover 8 8 replicated >/dev/null
exit_code=$(docker wait "$client_container")
test "$exit_code" = 0 || { docker logs "$client_container" >&2; exit 1; }
docker logs "$client_container" >"$temporary/probe.json"
jq -e '.incremental_observed and .full_incremental_equivalent and .monitor_loss_recovered and .post_failover_command and .foreign_fsid_rejected and (.pools | index("p04-initial") != null) and (.pools | index("p04-mutated") != null) and (.pools | index("p04-failover") != null)' "$temporary/probe.json" >/dev/null
mkdir -p "$(dirname "$report")"
test "$(hash_implementation)" = "$artifacts"
jq -n --arg started_at "$started_at" --arg finished_at "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" --arg image "$image" --arg platform "$platform" --argjson artifacts "$artifacts" --slurpfile probe "$temporary/probe.json" '{schema_version:1,status:"passed",command:"make integration-p04",started_at:$started_at,finished_at:$finished_at,source:{repository:"https://github.com/otuschhoff/go-librados.git",identity:"content-addressed-artifacts",artifacts:$artifacts},server:{repository:"https://github.com/ceph/ceph.git",source_anchor_commit:"7f793731f1b39eb4f465e960113d2363c311b964",image:$image,platform:$platform},scenarios:{m0:"passed",pool_report:"passed",map_change:"passed",monitor_loss:"passed",foreign_fsid:"passed",convergence:"passed",read_only_command:"passed"},probe:$probe[0]}' >"$report"
printf 'P04 integration report: %s\n' "$report"
