#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
report=$root/docs/p13/integration-report.json
temporary=
ajv_root=
report_stage_dir=
preflight_cleanup() {
	exit_code=$?
	if test "$exit_code" -ne 0; then
		rm -f "$report"
		test -z "$report_stage_dir" || rm -rf "$report_stage_dir"
		test -z "$ajv_root" || rm -rf "$ajv_root"
		if test -z "${P13_ARTIFACT_DIR:-}" && test -n "$temporary"; then rm -rf "$temporary"; fi
	fi
	exit "$exit_code"
}
trap preflight_cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
cd "$root"
quick=false
case "${1:-}" in
	"") ;;
	--quick) quick=true ;;
	*) printf 'usage: %s [--quick]\n' "$0" >&2; exit 2 ;;
esac

started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
mode=full
command='./integration/p13/reproduce.sh'
device_bytes=8589934592
poll_limit=120
minimum_pending=5
minimum_watch_outage=12
flaky_cycles=4
if test "$quick" = true; then
	mode=quick
	command='./integration/p13/reproduce.sh --quick'
	device_bytes=2147483648
	poll_limit=60
	minimum_pending=2
	minimum_watch_outage=10
	flaky_cycles=2
fi

run_id=$$
temporary=${P13_ARTIFACT_DIR:-$(mktemp -d)}
mkdir -p "$temporary"
ajv_root=$(mktemp -d)
network="rados-go-p13-$run_id"
fsid=13131313-2222-4333-8444-131313131313
report_stage_dir=$(mktemp -d docs/p13/.integration-report.XXXXXX)
report_stage=$report_stage_dir/report.json
scenarios="$temporary/scenarios.json"
printf '[]\n' >"$scenarios"
completed=false
publication_started=false
failed_stage=
phase=setup

publish_failed() {
	exit_code=$1
	test "$completed" = false || return 0
	test "$publication_started" = false || return 0
	command -v jq >/dev/null 2>&1 || return 1
	failed_stage=$(mktemp docs/p13/.integration-report.failed.XXXXXX)
	jq -n --arg command "$command" --arg mode "$mode" --arg started "$started_at" \
		--arg finished "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --arg failure "P13 harness failed in $phase with exit status $exit_code; artifacts: $temporary" \
		'{schema_version:1,status:"failed",command:$command,mode:$mode,started_at:$started,finished_at:$finished,failure:$failure,source:{repository:"https://github.com/otuschhoff/rados-go.git",artifacts:{}},server:null,native_runtime:null,cluster:null,scenarios:[],change_observations:null,unqualified:{multi_host_maintenance:"unqualified: disposable harness uses one Docker host",probabilistic_packet_loss:"unqualified: no host-global or probabilistic network fault injection is used"}}' >"$failed_stage"
	jq -e '.status == "failed" and (.failure | length > 0)' "$failed_stage" >/dev/null
	mv "$failed_stage" "$report"
}

fail_if_requested() {
	if test "${P13_FAIL_PHASE:-}" = "$phase"; then
		printf 'p13: forced failure after %s\n' "$phase" >&2
		return 97
	fi
}

cleanup() {
	exit_code=$?
	if test "$exit_code" -ne 0; then
		for daemon in "p13-mon-a-$run_id" "p13-mon-b-$run_id" "p13-mon-c-$run_id" "p13-mgr-$run_id" "p13-osd-0-$run_id" "p13-osd-1-$run_id" "p13-osd-2-$run_id" "p13-osd-3-$run_id" "p13-osd-3b-$run_id"; do
			docker logs --tail 50 "$daemon" >&2 2>/dev/null || true
		done
		if ! publish_failed "$exit_code"; then rm -f "$report"; fi
	fi
	docker rm -f "p13-mon-a-$run_id" "p13-mon-b-$run_id" "p13-mon-c-$run_id" "p13-mgr-$run_id" "p13-osd-0-$run_id" "p13-osd-1-$run_id" "p13-osd-2-$run_id" "p13-osd-3-$run_id" "p13-osd-3b-$run_id" >/dev/null 2>&1 || true
	attached=$(docker network inspect "$network" --format '{{range $id, $value := .Containers}}{{$id}} {{end}}' 2>/dev/null || true)
	if test -n "$attached"; then docker rm -f $attached >/dev/null 2>&1 || true; fi
	docker volume rm "rados-go-p13-osd-0-$run_id" "rados-go-p13-osd-1-$run_id" "rados-go-p13-osd-2-$run_id" "rados-go-p13-osd-3-$run_id" "rados-go-p13-osd-3b-$run_id" >/dev/null 2>&1 || true
	docker network rm "$network" >/dev/null 2>&1 || true
	rm -rf "$report_stage_dir"
	rm -rf "$ajv_root"
	test -z "$failed_stage" || rm -f "$failed_stage"
	if test "$exit_code" -eq 0 && test -z "${P13_ARTIFACT_DIR:-}"; then rm -rf "$temporary"; fi
	exit "$exit_code"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

phase=preflight
fail_if_requested
command -v jq >/dev/null 2>&1 || { printf '%s\n' 'P13 requires jq' >&2; exit 2; }
command -v npm >/dev/null 2>&1 || { printf '%s\n' 'P13 requires npm' >&2; exit 2; }
case "$(docker info --format '{{.Architecture}}')" in
	x86_64|amd64) platform=linux/amd64; goarch=amd64 ;;
	aarch64|arm64) platform=linux/arm64; goarch=arm64 ;;
	*) printf '%s\n' 'unsupported Docker architecture' >&2; exit 2 ;;
esac
image_index=$(jq -r '.images.qualification.reference' docs/p00/evidence.json)
image_digest=$(jq -r --arg architecture "$goarch" '.images.qualification[$architecture]' docs/p00/evidence.json)
image="${image_index%@*}@$image_digest"

ceph_cli() (
	docker run --rm --platform "$platform" --network "$network" -v "$temporary:/cluster" "$image" \
		timeout 20 ceph --conf /cluster/ceph.conf --name client.admin --keyring /cluster/admin.keyring "$@"
)
rados_cli() (
	pool=$1; shift
	docker run --rm --platform "$platform" --network "$network" -v "$temporary:/cluster" "$image" \
		timeout 20 rados --conf /cluster/ceph.conf --name client.admin --keyring /cluster/admin.keyring -p "$pool" "$@"
)
wait_osds() (
	total=$1; up=$2; in_count=$3
	for attempt in $(seq 1 "$poll_limit"); do
		if ceph_cli osd stat --format json 2>/dev/null | jq -e --argjson total "$total" --argjson up "$up" --argjson inside "$in_count" '.num_osds == $total and .num_up_osds == $up and .num_in_osds == $inside' >/dev/null; then return 0; fi
		test "$attempt" -lt "$poll_limit" || { ceph_cli osd tree >&2; return 1; }
		sleep 1
	done
)
wait_clean() (
	for attempt in $(seq 1 "$poll_limit"); do
		if ceph_cli pg dump pgs_brief --format json 2>/dev/null | jq -e '.pg_stats | length > 0 and all(.[]; .state == "active+clean")' >/dev/null; then return 0; fi
		test "$attempt" -lt "$poll_limit" || { ceph_cli pg dump pgs_brief --format json >&2; ceph_cli health detail >&2; return 1; }
		sleep 1
	done
)
wait_quorum() (
	expected=$1
	for attempt in $(seq 1 "$poll_limit"); do
		if ceph_cli quorum_status --format json 2>/dev/null | jq -e --argjson expected "$expected" '.quorum_names | length == $expected' >/dev/null; then return 0; fi
		test "$attempt" -lt "$poll_limit" || { ceph_cli quorum_status --format json >&2; return 1; }
		sleep 1
	done
)
wait_osd_drained() (
	id=$1
	for attempt in $(seq 1 "$poll_limit"); do
		if ceph_cli pg dump pgs_brief --format json 2>/dev/null | jq -e --argjson id "$id" '.pg_stats | length > 0 and all(.[]; .state == "active+clean" and (.up | index($id) | not) and (.acting | index($id) | not))' >/dev/null; then return 0; fi
		test "$attempt" -lt "$poll_limit" || { ceph_cli pg dump pgs_brief --format json >&2; ceph_cli health detail >&2; return 1; }
		sleep 1
	done
)
snapshot() (
	pool=$1; object=$2; output=$3
	mapping=$(ceph_cli osd map "$pool" "$object" --format json)
	pgid=$(printf '%s' "$mapping" | jq -r '.pgid')
	pg_state=$(ceph_cli pg "$pgid" query --format json | jq -r '.state')
	epoch=$(ceph_cli osd dump --format json | jq '.epoch')
	printf '%s' "$mapping" | jq --argjson epoch "$epoch" --arg state "$pg_state" --arg observed "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
		'{epoch:$epoch,pgid:.pgid,acting:.acting,primary:.acting_primary,pg_state:$state,observed_at:$observed}' >"$output"
)
snapshot_osd() (
	id=$1; output=$2
	ceph_cli osd dump --format json | jq --argjson id "$id" '
		([.osds[] | select(.osd == $id)][0] // null) as $osd |
		if $osd == null then {id:$id,exists:false,up:false,in:false,destroyed:false,uuid:"",address:""}
		else {id:$id,exists:true,up:($osd.up == 1 or $osd.up == true),in:($osd.in == 1 or $osd.in == true),destroyed:(($osd.state // []) | index("destroyed") != null),uuid:($osd.uuid // ""),address:($osd.public_addrs.addrvec[0].addr // $osd.public_addr // "")}
		end' >"$output"
)
snapshot_monmap() (
	output=$1
	ceph_cli mon dump --format json | jq '
		{epoch:.epoch,monitors:[.mons[] | {name:.name,rank:.rank,address:(.public_addrs.addrvec[0].addr // .addr // ""),priority:(.priority // 0),weight:(.weight // 0),location:(((.crush_location // {}) | if type == "string" then fromjson? elif type == "array" then map({key:.key,value:.val}) | from_entries else . end) // {})}]}' >"$output"
)
wait_primary_change() (
	pool=$1; object=$2; old_primary=$3
	for attempt in $(seq 1 "$poll_limit"); do
		mapping=$(ceph_cli osd map "$pool" "$object" --format json 2>/dev/null || true)
		new_primary=$(printf '%s' "$mapping" | jq -r '.acting_primary // -1')
		if test "$new_primary" -ge 0 && test "$new_primary" != "$old_primary"; then return 0; fi
		test "$attempt" -lt "$poll_limit" || return 1
		sleep 1
	done
)
wait_watcher() (
	pool=$1; object=$2; cookie=$3
	for attempt in $(seq 1 "$poll_limit"); do
		if rados_cli "$pool" listwatchers "$object" 2>/dev/null | grep -F "cookie=$cookie" >/dev/null; then return 0; fi
		test "$attempt" -lt "$poll_limit" || { rados_cli "$pool" listwatchers "$object" >&2; return 1; }
		sleep 1
	done
)
wait_event_count() (
	path=$1; expected=$2; process=$3
	for attempt in $(seq 1 "$poll_limit"); do
		observed=$(cat "$path" 2>/dev/null || printf 0)
		if test "$observed" -ge "$expected" 2>/dev/null; then return 0; fi
		kill -0 "$process" 2>/dev/null || return 1
		test "$attempt" -lt "$poll_limit" || return 1
		sleep 1
	done
)
run_go() (
	pool=$1; operation=$2; object=$3; payload=${4:-}; mutation=${5:-}; output=$6
	docker run --rm --platform "$platform" --network "$network" -v "$temporary:/work" -e P13_READY_FILE -e P13_RELEASE_FILE -e P13_TRANSPORT "$image" \
		timeout 75 /work/p13-probe -monitors 172.30.113.10:3300,172.30.113.12:3300,172.30.113.13:3300 -key-file /work/client.key -fsid "$fsid" -pool "$pool" -operation "$operation" -object "$object" -payload "$payload" -mutation-id "$mutation" -timeout 60s >"$output"
)
run_native() (
	pool=$1; operation=$2; object=$3; payload=${4:-}; output=$5
	docker run --rm --platform "$platform" --network "$network" -v "$temporary:/work" -e P13_READY_FILE -e P13_RELEASE_FILE -e P13_TRANSPORT "$image" \
		timeout 75 /work/native-driver "$operation" /work/ceph.conf /work/client.keyring "$pool" "$object" "$payload" >"$output"
)
add_scenario() (
	id=$1; pool=$2; operation=$3; fault=$4; injected=$5; released=$6; before=$7; after=$8; go_result=$9
	shift 9
	native_result=$1; final_data=$2; markers=$3; notes=$4
	lifecycle=${5:-null}
	monitor_lifecycle=${6:-null}
	fault_intervals=${7:-[]}
	jq --arg id "$id" --arg pool "$pool" --arg operation "$operation" --arg fault "$fault" \
		--arg injected "$injected" --arg released "$released" --arg final "$final_data" --arg notes "$notes" \
		--argjson before "$(cat "$before")" --argjson after "$(cat "$after")" \
		--argjson go "$(cat "$go_result")" --argjson native "$(cat "$native_result")" --argjson markers "$markers" --argjson lifecycle "$lifecycle" --argjson monitor_lifecycle "$monitor_lifecycle" --argjson fault_intervals "$fault_intervals" \
		'. + [{id:$id,status:"passed",pool:$pool,operation:$operation,fault:$fault,injected_at:$injected,released_at:$released,before:$before,after:$after,go:$go,native:$native,final_data:$final,mutation_markers:$markers,notes:$notes,lifecycle:$lifecycle,monitor_lifecycle:$monitor_lifecycle,fault_intervals:$fault_intervals}]' "$scenarios" >"$scenarios.next"
	mv "$scenarios.next" "$scenarios"
)
create_osd() (
	id=$1; uuid=$2; ip=$3; suffix=${4:-$id}; volume="rados-go-p13-osd-$suffix-$run_id"
	docker volume create "$volume" >/dev/null
	ceph_cli osd new "$uuid" "$id" >/dev/null
	ceph_cli auth get-or-create "osd.$id" mon 'allow profile osd' mgr 'allow profile osd' osd 'allow *' -o "/cluster/osd-$suffix.keyring"
	ceph_cli mon getmap -o "/cluster/osd-$suffix.monmap" >/dev/null
	docker run --rm --user 0 --privileged --platform "$platform" -v "$temporary:/cluster" -v "$volume:/osd" "$image" sh -c '
		set -eu
		id='"$id"'; uuid='"$uuid"'; bytes='"$device_bytes"'; suffix='"$suffix"'
		mkdir -p /osd/data; truncate -s "$bytes" /osd/block
		cp "/cluster/osd-$suffix.keyring" /osd/data/keyring; cp "/cluster/osd-$suffix.monmap" /osd/data/activate.monmap
		chown -R ceph:ceph /osd
		ceph-osd --mkfs -i "$id" --osd-data /osd/data --osd-uuid "$uuid" --osd-objectstore bluestore --bluestore-block-path /osd/block --monmap /osd/data/activate.monmap --keyring /osd/data/keyring --setuser ceph --setgroup ceph
	'
	name="p13-osd-$suffix-$run_id"
	docker run -d --privileged --name "$name" --platform "$platform" --network "$network" --ip "$ip" -v "$temporary:/cluster" -v "$volume:/osd" "$image" \
		ceph-osd -f --conf /cluster/ceph.conf -i "$id" --osd-data /osd/data --osd-objectstore bluestore --public-addr "v2:$ip:6800" --cluster-addr "v2:$ip:6802" --log-file '' --setuser ceph --setgroup ceph >/dev/null
)
start_mon() (
	id=$1; ip=$2
	docker run -d --name "p13-mon-$id-$run_id" --platform "$platform" --network "$network" --ip "$ip" -v "$temporary:/cluster" "$image" \
		ceph-mon -f -i "$id" --mon-data "/cluster/mondata-$id" --public-addr "v2:$ip:3300" --setuser ceph --setgroup ceph --mon-data-avail-crit 0 --no-mon-cluster-log-to-stderr >/dev/null
)

CGO_ENABLED=0 GOOS=linux GOARCH="$goarch" GOTOOLCHAIN=go1.27.1 go build -trimpath -o "$temporary/p13-probe" ./integration/p13/probe
cp integration/p13/native_driver.c "$temporary/"
docker run --rm --user 0 --platform "$platform" -v "$temporary:/cluster" "$image" sh -c '
	set -eu
	cc -std=c11 -Wall -Wextra -Werror -O2 /cluster/native_driver.c -ldl -o /cluster/native-driver
'
docker network create --subnet 172.30.113.0/24 "$network" >/dev/null
docker run --rm --user 0 --platform "$platform" -v "$temporary:/cluster" "$image" sh -c '
	set -eu
	cat >/cluster/ceph.conf <<EOF
[global]
fsid = 13131313-2222-4333-8444-131313131313
mon host = v2:172.30.113.10:3300,v2:172.30.113.12:3300,v2:172.30.113.13:3300
auth cluster required = cephx
auth service required = cephx
auth client required = cephx
ms bind msgr1 = false
ms bind msgr2 = true
mon osd down out interval = 30
EOF
	ceph-authtool /cluster/mon.keyring --create-keyring --gen-key -n mon. --cap mon "allow *"
	ceph-authtool /cluster/admin.keyring --create-keyring --gen-key -n client.admin --cap mon "allow *" --cap osd "allow *" --cap mgr "allow *"
	ceph-authtool /cluster/mgr.keyring --create-keyring --gen-key -n mgr.x --cap mon "allow profile mgr" --cap osd "allow *" --cap mgr "allow *"
	ceph-authtool /cluster/mon.keyring --import-keyring /cluster/admin.keyring
	ceph-authtool /cluster/mon.keyring --import-keyring /cluster/mgr.keyring
	monmaptool --create --fsid 13131313-2222-4333-8444-131313131313 --addv a "[v2:172.30.113.10:3300/0]" --addv b "[v2:172.30.113.12:3300/0]" --addv c "[v2:172.30.113.13:3300/0]" /cluster/monmap
	mkdir -p /cluster/mondata-a /cluster/mondata-b /cluster/mondata-c /cluster/mgrdata
	cp /cluster/mgr.keyring /cluster/mgrdata/keyring
	for id in a b c; do ceph-mon --mkfs -i "$id" --fsid 13131313-2222-4333-8444-131313131313 --monmap /cluster/monmap --keyring /cluster/mon.keyring --mon-data "/cluster/mondata-$id"; done
	chown -R ceph:ceph /cluster/mondata-a /cluster/mondata-b /cluster/mondata-c /cluster/mgrdata
'
start_mon a 172.30.113.10
start_mon b 172.30.113.12
start_mon c 172.30.113.13
for attempt in $(seq 1 30); do
	if ceph_cli status --format json 2>/dev/null | jq -e '.health.status != null' >/dev/null; then break; fi
	test "$attempt" -lt 30 || exit 1
	sleep 1
done
wait_quorum 3
docker run -d --name "p13-mgr-$run_id" --platform "$platform" --network "$network" --ip 172.30.113.11 -v "$temporary:/cluster" "$image" \
	ceph-mgr -f --conf /cluster/ceph.conf -i x --mgr-data /cluster/mgrdata --setuser ceph --setgroup ceph >/dev/null
for id in 0 1 2; do create_osd "$id" "13000000-0000-4000-8000-00000000000$id" "172.30.113.$((20 + id))"; done
wait_osds 3 3 3
ceph_cli osd crush rule create-replicated p13-rule default osd >/dev/null
ceph_cli osd pool create p13-size3 8 8 replicated p13-rule >/dev/null
ceph_cli osd pool set p13-size3 pg_autoscale_mode off >/dev/null
ceph_cli osd pool set p13-size3 size 3 >/dev/null
ceph_cli osd pool set p13-size3 min_size 2 >/dev/null
ceph_cli osd pool create p13-size1 8 8 replicated p13-rule >/dev/null
ceph_cli osd pool set p13-size1 pg_autoscale_mode off >/dev/null
ceph_cli config set mon mon_allow_pool_size_one true >/dev/null
ceph_cli osd pool set p13-size1 size 1 --yes-i-really-mean-it >/dev/null
ceph_cli osd pool set p13-size1 min_size 1 >/dev/null
printf '%s\n' 'p13: creating client identity'
ceph_cli auth get-or-create client.p13 mon 'allow r' osd 'allow *' -o /cluster/client.keyring
ceph_cli auth get-key client.p13 >"$temporary/client.key"
printf '%s\n' 'p13: waiting for initial active+clean PGs'
wait_clean
printf '%s\n' 'p13: starting cluster-change subscription'
export P13_READY_FILE=/work/changes-ready P13_RELEASE_FILE=/work/changes-release
docker run --rm --platform "$platform" --network "$network" -v "$temporary:/work" -e P13_READY_FILE -e P13_RELEASE_FILE "$image" \
	timeout 7300 /work/p13-probe -monitors 172.30.113.10:3300,172.30.113.12:3300,172.30.113.13:3300 -key-file /work/client.key -fsid "$fsid" -pool p13-size3 -operation cluster-changes -object cluster -timeout 2h >"$temporary/changes-go.json" &
changes_pid=$!
unset P13_READY_FILE P13_RELEASE_FILE
for attempt in $(seq 1 "$poll_limit"); do test -f "$temporary/changes-ready" && break; kill -0 "$changes_pid" 2>/dev/null || exit 1; test "$attempt" -lt "$poll_limit" || exit 1; sleep 1; done
printf '%s\n' 'p13: running baseline scenarios'

for pool in p13-size3 p13-size1; do
	phase="p13-baseline-${pool#p13-}"
	object="baseline-$pool"; printf '%s' "$pool-baseline" >"$temporary/input"
	rados_cli "$pool" put "$object" /cluster/input
	snapshot "$pool" "$object" "$temporary/$pool-before.json"
	injected=$(date -u +%Y-%m-%dT%H:%M:%SZ)
	run_go "$pool" read "$object" '' '' "$temporary/$pool-go.json"
	run_native "$pool" read "$object" '' "$temporary/$pool-native.json"
	jq -e --arg data "$pool-baseline" '.completed and .data == $data' "$temporary/$pool-go.json" >/dev/null
	jq -e --arg data "$pool-baseline" '.completed and .data == $data' "$temporary/$pool-native.json" >/dev/null
	snapshot "$pool" "$object" "$temporary/$pool-after.json"
	released=$(date -u +%Y-%m-%dT%H:%M:%SZ)
	add_scenario "p13-baseline-${pool#p13-}" "$pool" read none "$injected" "$released" "$temporary/$pool-before.json" "$temporary/$pool-after.json" "$temporary/$pool-go.json" "$temporary/$pool-native.json" "$pool-baseline" '[]' 'Go and native baseline agree'
	fail_if_requested
done

object=one-down; printf stable >"$temporary/input"; rados_cli p13-size3 put "$object" /cluster/input
phase=p13-size3-one-down
printf '%s\n' 'p13: size3 one-down availability'
snapshot p13-size3 "$object" "$temporary/one-down-before.json"; failed_osd=$(jq '.acting[-1]' "$temporary/one-down-before.json"); injected=$(date -u +%Y-%m-%dT%H:%M:%SZ)
docker stop "p13-osd-$failed_osd-$run_id" >/dev/null; ceph_cli osd down "$failed_osd" >/dev/null; wait_osds 3 2 3
run_go p13-size3 read "$object" '' '' "$temporary/one-down-go.json"; run_native p13-size3 read "$object" '' "$temporary/one-down-native.json"
docker start "p13-osd-$failed_osd-$run_id" >/dev/null; wait_osds 3 3 3; wait_clean; released=$(date -u +%Y-%m-%dT%H:%M:%SZ); snapshot p13-size3 "$object" "$temporary/one-down-after.json"
add_scenario p13-size3-one-down p13-size3 read osd-down "$injected" "$released" "$temporary/one-down-before.json" "$temporary/one-down-after.json" "$temporary/one-down-go.json" "$temporary/one-down-native.json" stable '[]' 'size 3 min 2 remained available with one replica down'

object=below-min; printf parked >"$temporary/input"; rados_cli p13-size3 put "$object" /cluster/input; snapshot p13-size3 "$object" "$temporary/below-before.json"
phase=p13-size3-below-min-recovery
printf '%s\n' 'p13: size3 below-min parking and recovery'
first=$(jq '.acting[1]' "$temporary/below-before.json"); second=$(jq '.acting[2]' "$temporary/below-before.json"); export P13_RELEASE_FILE=/work/below-release
export P13_READY_FILE=/work/below-go-ready; run_go p13-size3 read "$object" '' '' "$temporary/below-go.json" & go_pid=$!
export P13_READY_FILE=/work/below-native-ready; run_native p13-size3 read "$object" '' "$temporary/below-native.json" & native_pid=$!; unset P13_READY_FILE P13_RELEASE_FILE
for ready in below-go-ready below-native-ready; do for attempt in $(seq 1 "$poll_limit"); do test -f "$temporary/$ready" && break; test "$attempt" -lt "$poll_limit" || exit 1; sleep 1; done; done
injected=$(date -u +%Y-%m-%dT%H:%M:%SZ); docker stop "p13-osd-$first-$run_id" "p13-osd-$second-$run_id" >/dev/null; ceph_cli osd down "$first" "$second" >/dev/null; wait_osds 3 1 3; : >"$temporary/below-release"
sleep "$minimum_pending"; kill -0 "$go_pid" 2>/dev/null && kill -0 "$native_pid" 2>/dev/null || { printf '%s\n' 'size3 operations did not park below min_size' >&2; exit 1; }
docker start "p13-osd-$first-$run_id" "p13-osd-$second-$run_id" >/dev/null; wait_osds 3 3 3; wait_clean; wait "$go_pid"; wait "$native_pid"
released=$(date -u +%Y-%m-%dT%H:%M:%SZ); snapshot p13-size3 "$object" "$temporary/below-after.json"
add_scenario p13-size3-below-min-recovery p13-size3 read two-osds-down "$injected" "$released" "$temporary/below-before.json" "$temporary/below-after.json" "$temporary/below-go.json" "$temporary/below-native.json" parked '[]' 'operation remained pending for the configured minimum interval and completed after recovery'

phase=p13-crc-below-min-recovery
printf '%s\n' 'p13: CRC transport below-min parking and recovery'
snapshot p13-size3 "$object" "$temporary/crc-below-before.json"; export P13_TRANSPORT=crc P13_RELEASE_FILE=/work/crc-below-release
export P13_READY_FILE=/work/crc-below-go-ready; run_go p13-size3 read "$object" '' '' "$temporary/crc-below-go.json" & go_pid=$!
export P13_READY_FILE=/work/crc-below-native-ready; run_native p13-size3 read "$object" '' "$temporary/crc-below-native.json" & native_pid=$!; unset P13_READY_FILE P13_RELEASE_FILE P13_TRANSPORT
for ready in crc-below-go-ready crc-below-native-ready; do for attempt in $(seq 1 "$poll_limit"); do test -f "$temporary/$ready" && break; test "$attempt" -lt "$poll_limit" || exit 1; sleep 1; done; done
injected=$(date -u +%Y-%m-%dT%H:%M:%SZ); docker stop "p13-osd-$first-$run_id" "p13-osd-$second-$run_id" >/dev/null; ceph_cli osd down "$first" "$second" >/dev/null; wait_osds 3 1 3; : >"$temporary/crc-below-release"
sleep "$minimum_pending"; kill -0 "$go_pid" 2>/dev/null && kill -0 "$native_pid" 2>/dev/null || { printf '%s\n' 'CRC operations did not park below min_size' >&2; exit 1; }
docker start "p13-osd-$first-$run_id" "p13-osd-$second-$run_id" >/dev/null; wait_osds 3 3 3; wait_clean; wait "$go_pid"; wait "$native_pid"; released=$(date -u +%Y-%m-%dT%H:%M:%SZ); snapshot p13-size3 "$object" "$temporary/crc-below-after.json"
add_scenario p13-crc-below-min-recovery p13-size3 read two-osds-down "$injected" "$released" "$temporary/crc-below-before.json" "$temporary/crc-below-after.json" "$temporary/crc-below-go.json" "$temporary/crc-below-native.json" parked '[]' 'forced CRC clients remained pending below min_size and completed after recovery'

object=sole-loss; printf sole >"$temporary/input"; rados_cli p13-size1 put "$object" /cluster/input; snapshot p13-size1 "$object" "$temporary/sole-before.json"; sole=$(jq '.primary' "$temporary/sole-before.json"); injected=$(date -u +%Y-%m-%dT%H:%M:%SZ)
phase=p13-size1-loss-recovery
printf '%s\n' 'p13: size1 sole-OSD parking and recovery'
export P13_RELEASE_FILE=/work/sole-release; export P13_READY_FILE=/work/sole-go-ready; run_go p13-size1 read "$object" '' '' "$temporary/sole-go.json" & go_pid=$!
export P13_READY_FILE=/work/sole-native-ready; run_native p13-size1 read "$object" '' "$temporary/sole-native.json" & native_pid=$!; unset P13_READY_FILE P13_RELEASE_FILE
for ready in sole-go-ready sole-native-ready; do for attempt in $(seq 1 "$poll_limit"); do test -f "$temporary/$ready" && break; test "$attempt" -lt "$poll_limit" || exit 1; sleep 1; done; done
injected=$(date -u +%Y-%m-%dT%H:%M:%SZ); docker stop "p13-osd-$sole-$run_id" >/dev/null; ceph_cli osd down "$sole" >/dev/null; wait_osds 3 2 3; : >"$temporary/sole-release"; sleep "$minimum_pending"; kill -0 "$go_pid" 2>/dev/null && kill -0 "$native_pid" 2>/dev/null || exit 1
docker start "p13-osd-$sole-$run_id" >/dev/null; wait_osds 3 3 3; wait_clean; wait "$go_pid"; wait "$native_pid"
released=$(date -u +%Y-%m-%dT%H:%M:%SZ); snapshot p13-size1 "$object" "$temporary/sole-after.json"
add_scenario p13-size1-loss-recovery p13-size1 read sole-osd-down "$injected" "$released" "$temporary/sole-before.json" "$temporary/sole-after.json" "$temporary/sole-go.json" "$temporary/sole-native.json" sole '[]' 'size 1 operation parked and completed after the sole OSD restarted'

object=silent-primary; printf silent >"$temporary/input"; rados_cli p13-size3 put "$object" /cluster/input; snapshot p13-size3 "$object" "$temporary/silent-before.json"; primary=$(jq '.primary' "$temporary/silent-before.json")
phase=p13-silent-primary-map-wakeup
printf '%s\n' 'p13: silent primary then map-driven completion'
export P13_RELEASE_FILE=/work/silent-release; export P13_READY_FILE=/work/silent-go-ready; run_go p13-size3 read "$object" '' '' "$temporary/silent-go.json" & go_pid=$!
export P13_READY_FILE=/work/silent-native-ready; run_native p13-size3 read "$object" '' "$temporary/silent-native.json" & native_pid=$!; unset P13_READY_FILE P13_RELEASE_FILE
for ready in silent-go-ready silent-native-ready; do for attempt in $(seq 1 "$poll_limit"); do test -f "$temporary/$ready" && break; test "$attempt" -lt "$poll_limit" || exit 1; sleep 1; done; done
injected=$(date -u +%Y-%m-%dT%H:%M:%SZ); docker pause "p13-osd-$primary-$run_id" >/dev/null; : >"$temporary/silent-release"; sleep "$minimum_pending"; kill -0 "$go_pid" 2>/dev/null && kill -0 "$native_pid" 2>/dev/null || { printf '%s\n' 'silent-primary operations completed without a map change' >&2; exit 1; }
docker rm -f "p13-osd-$primary-$run_id" >/dev/null; ceph_cli osd down "$primary" >/dev/null; ceph_cli osd out "$primary" >/dev/null; wait_primary_change p13-size3 "$object" "$primary"; wait "$go_pid"; wait "$native_pid"
docker start "p13-osd-$primary-$run_id" >/dev/null 2>&1 || true
# The removed container is recreated from its retained P13 volume.
docker run -d --privileged --name "p13-osd-$primary-$run_id" --platform "$platform" --network "$network" --ip "172.30.113.$((20 + primary))" -v "$temporary:/cluster" -v "rados-go-p13-osd-$primary-$run_id:/osd" "$image" \
	ceph-osd -f --conf /cluster/ceph.conf -i "$primary" --osd-data /osd/data --osd-objectstore bluestore --public-addr "v2:172.30.113.$((20 + primary)):6800" --cluster-addr "v2:172.30.113.$((20 + primary)):6802" --log-file '' --setuser ceph --setgroup ceph >/dev/null
ceph_cli osd in "$primary" >/dev/null; wait_osds 3 3 3; wait_clean; released=$(date -u +%Y-%m-%dT%H:%M:%SZ); snapshot p13-size3 "$object" "$temporary/silent-after.json"
add_scenario p13-silent-primary-map-wakeup p13-size3 read paused-primary-then-down-out "$injected" "$released" "$temporary/silent-before.json" "$temporary/silent-after.json" "$temporary/silent-go.json" "$temporary/silent-native.json" silent '[]' 'no completion during the minimum silent interval; newer map drove completion'

object=flaky-mutation; : >"$temporary/empty"; rados_cli p13-size3 put "$object" /cluster/empty; snapshot p13-size3 "$object" "$temporary/flaky-before.json"; injected=$(date -u +%Y-%m-%dT%H:%M:%SZ); markers='[]'; fault_intervals='[]'
phase=p13-flaky-stop-start
printf '%s\n' 'p13: bounded stop-start mutation cycles'
for cycle in $(seq 1 "$flaky_cycles"); do
	primary=$(ceph_cli osd map p13-size3 "$object" --format json | jq '.acting_primary'); marker="go-marker-$cycle;"; native_marker="native-marker-$cycle;"
	rm -f "$temporary/flaky-go-ready" "$temporary/flaky-native-ready" "$temporary/flaky-release"
	export P13_RELEASE_FILE=/work/flaky-release; export P13_READY_FILE=/work/flaky-go-ready; run_go p13-size3 append "$object" "$marker" "$marker" "$temporary/flaky-go-$cycle.json" & go_pid=$!
	export P13_READY_FILE=/work/flaky-native-ready; run_native p13-size3 append "$object" "$native_marker" "$temporary/flaky-native-$cycle.json" & native_pid=$!; unset P13_READY_FILE P13_RELEASE_FILE
	for attempt in $(seq 1 "$poll_limit"); do test -f "$temporary/flaky-go-ready" && test -f "$temporary/flaky-native-ready" && break; kill -0 "$go_pid" 2>/dev/null && kill -0 "$native_pid" 2>/dev/null || exit 1; test "$attempt" -lt "$poll_limit" || exit 1; sleep 1; done
	cycle_injected=$(date -u +%Y-%m-%dT%H:%M:%SZ); docker pause "p13-osd-$primary-$run_id" >/dev/null; : >"$temporary/flaky-release"; sleep "$minimum_pending"; kill -0 "$go_pid"; kill -0 "$native_pid"; cycle_released=$(date -u +%Y-%m-%dT%H:%M:%SZ)
	docker unpause "p13-osd-$primary-$run_id" >/dev/null; docker stop "p13-osd-$primary-$run_id" >/dev/null; ceph_cli osd down "$primary" >/dev/null; wait_primary_change p13-size3 "$object" "$primary"
	wait "$go_pid"; wait "$native_pid"; docker start "p13-osd-$primary-$run_id" >/dev/null; wait_osds 3 3 3
	markers=$(printf '%s' "$markers" | jq --arg go "$marker" --arg native "$native_marker" '. + [$go, $native]')
	fault_intervals=$(printf '%s' "$fault_intervals" | jq --arg injected "$cycle_injected" --arg released "$cycle_released" '. + [{injected_at:$injected,released_at:$released}]')
done
wait_clean
rados_cli p13-size3 get "$object" /cluster/flaky-final; final=$(cat "$temporary/flaky-final")
expected_markers=$markers
markers=$(jq -Rn --arg final "$final" '$final | [scan("(?:go|native)-marker-[0-9]+;")]')
jq -n -e --arg final "$final" --argjson expected "$expected_markers" --argjson observed "$markers" '($observed | join("")) == $final and ($observed | sort) == ($expected | sort)' >/dev/null
cp "$temporary/flaky-go-$flaky_cycles.json" "$temporary/flaky-go.json"; cp "$temporary/flaky-native-$flaky_cycles.json" "$temporary/flaky-native.json"; released=$(date -u +%Y-%m-%dT%H:%M:%SZ); snapshot p13-size3 "$object" "$temporary/flaky-after.json"
add_scenario p13-flaky-stop-start p13-size3 append bounded-stop-start "$injected" "$released" "$temporary/flaky-before.json" "$temporary/flaky-after.json" "$temporary/flaky-go.json" "$temporary/flaky-native.json" "$final" "$markers" 'paired appends remained pending on each paused primary, completed after map-driven failover, and each marker appears exactly once; probabilistic packet loss is not claimed' null null "$fault_intervals"

object=watch-object; printf watch >"$temporary/input"; rados_cli p13-size3 put "$object" /cluster/input; snapshot p13-size3 "$object" "$temporary/watch-before.json"
maintenance_write=maintenance-write; : >"$temporary/empty"; rados_cli p13-size3 put "$maintenance_write" /cluster/empty; snapshot p13-size3 "$maintenance_write" "$temporary/maintenance-write-before.json"
rm -f "$temporary/watch-ready" "$temporary/watch-interrupted" "$temporary/watch-event" "$temporary/watch-release" "$temporary/native-watch-ready" "$temporary/native-watch-event" "$temporary/native-watch-release"
phase=p13-noout-long-watch
printf '%s\n' 'p13: global noout long-watch restart'
docker run --rm --platform "$platform" --network "$network" -v "$temporary:/work" "$image" timeout 90 /work/p13-probe -monitors 172.30.113.10:3300 -key-file /work/client.key -fsid "$fsid" -pool p13-size3 -operation watch -object "$object" -control-dir /work -timeout 75s >"$temporary/watch-go.json" & watch_pid=$!
run_native p13-size3 watch "$object" /work/native-watch "$temporary/watch-native.json" & native_watch_pid=$!
for attempt in $(seq 1 "$poll_limit"); do test -f "$temporary/watch-ready" && break; kill -0 "$watch_pid" 2>/dev/null || exit 1; test "$attempt" -lt "$poll_limit" || exit 1; sleep 1; done
for attempt in $(seq 1 "$poll_limit"); do test -f "$temporary/native-watch-ready" && break; kill -0 "$native_watch_pid" 2>/dev/null || exit 1; test "$attempt" -lt "$poll_limit" || exit 1; sleep 1; done
rados_cli p13-size3 notify "$object" baseline >/dev/null
for attempt in $(seq 1 "$poll_limit"); do test -f "$temporary/watch-event" && break; kill -0 "$watch_pid" 2>/dev/null || exit 1; test "$attempt" -lt "$poll_limit" || exit 1; sleep 1; done
wait_event_count "$temporary/native-watch-event" 1 "$native_watch_pid"
rm -f "$temporary/watch-event"
ceph_cli osd set noout >/dev/null; primary=$(jq '.primary' "$temporary/watch-before.json"); injected=$(date -u +%Y-%m-%dT%H:%M:%SZ); docker stop "p13-osd-$primary-$run_id" >/dev/null; ceph_cli osd down "$primary" >/dev/null; sleep "$minimum_watch_outage"
ceph_cli osd dump --format json | jq -e --argjson id "$primary" '.osds[] | select(.osd == $id) | .in == 1' >/dev/null
for attempt in $(seq 1 "$poll_limit"); do test -f "$temporary/watch-interrupted" && break; kill -0 "$watch_pid" 2>/dev/null || exit 1; test "$attempt" -lt "$poll_limit" || exit 1; sleep 1; done
wait_primary_change p13-size3 "$object" "$primary"; snapshot p13-size3 "$object" "$temporary/maintenance-read-after.json"; snapshot p13-size3 "$maintenance_write" "$temporary/maintenance-write-after.json"
run_go p13-size3 read "$object" '' '' "$temporary/maintenance-read-go.json"; run_native p13-size3 read "$object" '' "$temporary/maintenance-read-native.json"
run_go p13-size3 write "$maintenance_write" maintenance '' "$temporary/maintenance-write-go.json"; run_native p13-size3 write "$maintenance_write" maintenance "$temporary/maintenance-write-native.json"
rados_cli p13-size3 get "$maintenance_write" /cluster/maintenance-write-final; test "$(cat "$temporary/maintenance-write-final")" = maintenance
pgid=$(jq -r '.pgid' "$temporary/maintenance-read-after.json"); pg_command=$(printf '{"prefix":"pg","cmd":"query","pgid":"%s"}' "$pgid")
run_go p13-size3 pg-command "$pgid" "$pg_command" '' "$temporary/maintenance-command-go.json"; run_native p13-size3 pg-command "$pgid" "$pg_command" "$temporary/maintenance-command-native.json"
docker start "p13-osd-$primary-$run_id" >/dev/null; wait_osds 3 3 3; wait_clean; ceph_cli osd unset noout >/dev/null
wait_watcher p13-size3 "$object" "$(cat "$temporary/watch-ready")"
rados_cli p13-size3 notify "$object" recovered >/dev/null
for attempt in $(seq 1 "$poll_limit"); do test -f "$temporary/watch-event" && break; kill -0 "$watch_pid" 2>/dev/null || exit 1; test "$attempt" -lt "$poll_limit" || exit 1; sleep 1; done
: >"$temporary/watch-release"; wait_event_count "$temporary/native-watch-event" 2 "$native_watch_pid"; : >"$temporary/native-watch-release"; wait "$watch_pid"; wait "$native_watch_pid"
jq -e '.completed and .watch_interruptions > 0 and .watch_events > 0' "$temporary/watch-go.json" >/dev/null
jq -e '.completed and .operation == "watch" and .watch_cookie > 0 and .watch_events >= 2' "$temporary/watch-native.json" >/dev/null
released=$(date -u +%Y-%m-%dT%H:%M:%SZ); snapshot p13-size3 "$object" "$temporary/watch-after.json"
add_scenario p13-noout-read p13-size3 read global-noout-and-primary-restart "$injected" "$released" "$temporary/watch-before.json" "$temporary/maintenance-read-after.json" "$temporary/maintenance-read-go.json" "$temporary/maintenance-read-native.json" watch '[]' 'read completed while noout retained the down OSD'
add_scenario p13-noout-write p13-size3 write global-noout-and-primary-restart "$injected" "$released" "$temporary/maintenance-write-before.json" "$temporary/maintenance-write-after.json" "$temporary/maintenance-write-go.json" "$temporary/maintenance-write-native.json" maintenance '[]' 'matching writes completed while noout retained the down OSD'
add_scenario p13-noout-pg-command cluster pg-command global-noout-and-primary-restart "$injected" "$released" "$temporary/watch-before.json" "$temporary/maintenance-read-after.json" "$temporary/maintenance-command-go.json" "$temporary/maintenance-command-native.json" '' '[]' 'PG query followed the acting primary while noout retained the down OSD'
add_scenario p13-noout-long-watch p13-size3 watch global-noout-and-primary-restart "$injected" "$released" "$temporary/watch-before.json" "$temporary/watch-after.json" "$temporary/watch-go.json" "$temporary/watch-native.json" watch '[]' 'single-host global noout only; multi-host maintenance remains unqualified'

object=remap-candidate-0; printf remap >"$temporary/input"
docker run --rm --platform "$platform" --network "$network" -v "$temporary:/cluster" "$image" sh -c '
	set -eu
	for candidate in $(seq 0 127); do
		timeout 20 rados --conf /cluster/ceph.conf --name client.admin --keyring /cluster/admin.keyring -p p13-size3 put "remap-candidate-$candidate" /cluster/input
	done
'
wait_clean; before_epoch=$(ceph_cli osd dump --format json | jq '.epoch'); before_observed=$(date -u +%Y-%m-%dT%H:%M:%SZ); for candidate in $(seq 0 127); do ceph_cli osd map p13-size3 "remap-candidate-$candidate" --format json >"$temporary/add-candidate-$candidate.json"; done; injected=$(date -u +%Y-%m-%dT%H:%M:%SZ)
phase=p13-add-osd-remap
printf '%s\n' 'p13: add osd.3 and observe remap'
create_osd 3 13000000-0000-4000-8000-000000000003 172.30.113.23 3; wait_osds 4 4 4; wait_clean
for candidate in $(seq 0 127); do
	name="remap-candidate-$candidate"; before_acting=$(jq -c '.acting' "$temporary/add-candidate-$candidate.json"); after_mapping=$(ceph_cli osd map p13-size3 "$name" --format json)
	if printf '%s' "$after_mapping" | jq -e --argjson before "$before_acting" '.acting | index(3) != null and . != $before' >/dev/null; then object=$name; printf '%s' "$(cat "$temporary/add-candidate-$candidate.json")" | jq --argjson epoch "$before_epoch" --arg observed "$before_observed" '{epoch:$epoch,pgid:.pgid,acting:.acting,primary:.acting_primary,pg_state:"active+clean",observed_at:$observed}' >"$temporary/add-before.json"; break; fi
done
test -f "$temporary/add-before.json"
snapshot p13-size3 "$object" "$temporary/add-after.json"; jq -e '.acting | index(3) != null' "$temporary/add-after.json" >/dev/null
run_go p13-size3 read "$object" '' '' "$temporary/add-go.json"; run_native p13-size3 read "$object" '' "$temporary/add-native.json"; released=$(date -u +%Y-%m-%dT%H:%M:%SZ)
add_scenario p13-add-osd-remap p13-size3 read add-osd-3 "$injected" "$released" "$temporary/add-before.json" "$temporary/add-after.json" "$temporary/add-go.json" "$temporary/add-native.json" remap '[]' 'selected post-add PG acts on osd.3'

phase=p13-out-down-remove
printf '%s\n' 'p13: out down remove osd.3'
snapshot p13-size3 "$object" "$temporary/remove-before.json"; snapshot_osd 3 "$temporary/remove-osd-before.json"; injected=$(date -u +%Y-%m-%dT%H:%M:%SZ); ceph_cli osd out 3 >/dev/null; wait_osd_drained 3; docker stop "p13-osd-3-$run_id" >/dev/null; ceph_cli osd down 3 >/dev/null; snapshot p13-size3 "$object" "$temporary/command-down-map.json"; snapshot_osd 3 "$temporary/command-down-osd.json"
osd_command_json='{"prefix":"version"}'
if run_go p13-size3 osd-command 3 "$osd_command_json" '' "$temporary/command-down-go.json"; then printf '%s\n' 'Go command unexpectedly reached down osd.3' >&2; exit 1; fi
if run_native p13-size3 osd-command 3 "$osd_command_json" "$temporary/command-down-native.json"; then printf '%s\n' 'native command unexpectedly reached down osd.3' >&2; exit 1; fi
jq -e '.errno == 6 and (.completed | not)' "$temporary/command-down-go.json" "$temporary/command-down-native.json" >/dev/null; command_released=$(date -u +%Y-%m-%dT%H:%M:%SZ); lifecycle=$(jq -n --argjson before "$(cat "$temporary/remove-osd-before.json")" --argjson after "$(cat "$temporary/command-down-osd.json")" '{before:$before,after:$after}')
add_scenario p13-explicit-command-down cluster osd-command osd-3-down "$injected" "$command_released" "$temporary/remove-before.json" "$temporary/command-down-map.json" "$temporary/command-down-go.json" "$temporary/command-down-native.json" '' '[]' 'both clients report ENXIO for an existing down explicit target' "$lifecycle"
ceph_cli osd purge 3 --yes-i-really-mean-it >/dev/null; wait_osds 3 3 3; wait_clean
run_go p13-size3 read "$object" '' '' "$temporary/remove-go.json"; run_native p13-size3 read "$object" '' "$temporary/remove-native.json"; released=$(date -u +%Y-%m-%dT%H:%M:%SZ); snapshot p13-size3 "$object" "$temporary/remove-after.json"
snapshot_osd 3 "$temporary/remove-osd-after.json"; lifecycle=$(jq -n --argjson before "$(cat "$temporary/remove-osd-before.json")" --argjson after "$(cat "$temporary/remove-osd-after.json")" '{before:$before,after:$after}')
add_scenario p13-out-down-remove p13-size3 read osd-3-out-down-purge "$injected" "$released" "$temporary/remove-before.json" "$temporary/remove-after.json" "$temporary/remove-go.json" "$temporary/remove-native.json" remap '[]' 'removed OSD is absent and both clients read through the remapped PG' "$lifecycle"
command_injected=$(date -u +%Y-%m-%dT%H:%M:%SZ)
if run_go p13-size3 osd-command 3 "$osd_command_json" '' "$temporary/command-absent-go.json"; then printf '%s\n' 'Go command unexpectedly reached absent osd.3' >&2; exit 1; fi
if run_native p13-size3 osd-command 3 "$osd_command_json" "$temporary/command-absent-native.json"; then printf '%s\n' 'native command unexpectedly reached absent osd.3' >&2; exit 1; fi
jq -e '.errno == 2 and (.completed | not)' "$temporary/command-absent-go.json" "$temporary/command-absent-native.json" >/dev/null; command_released=$(date -u +%Y-%m-%dT%H:%M:%SZ)
add_scenario p13-explicit-command-absent cluster osd-command osd-3-absent "$command_injected" "$command_released" "$temporary/remove-after.json" "$temporary/remove-after.json" "$temporary/command-absent-go.json" "$temporary/command-absent-native.json" '' '[]' 'both clients report ENOENT for an absent explicit target' "$lifecycle"

phase=p13-destroy-recreate
printf '%s\n' 'p13: destroy and recreate osd.3 with new identity'
docker rm "p13-osd-3-$run_id" >/dev/null; docker volume rm "rados-go-p13-osd-3-$run_id" >/dev/null
create_osd 3 13000000-0000-4000-8000-000000000013 172.30.113.23 3; wait_osds 4 4 4; wait_clean; snapshot p13-size3 "$object" "$temporary/recreate-before.json"; snapshot_osd 3 "$temporary/recreate-osd-before.json"; injected=$(date -u +%Y-%m-%dT%H:%M:%SZ)
ceph_cli osd out 3 >/dev/null; wait_osd_drained 3; docker stop "p13-osd-3-$run_id" >/dev/null; ceph_cli osd down 3 >/dev/null; ceph_cli osd destroy 3 --yes-i-really-mean-it >/dev/null
snapshot_osd 3 "$temporary/recreate-osd-destroyed.json"
docker rm "p13-osd-3-$run_id" >/dev/null; docker volume rm "rados-go-p13-osd-3-$run_id" >/dev/null; create_osd 3 13000000-0000-4000-8000-000000000023 172.30.113.33 3b; wait_osds 4 4 4; wait_clean
run_go p13-size3 read "$object" '' '' "$temporary/recreate-go.json"; run_native p13-size3 read "$object" '' "$temporary/recreate-native.json"; released=$(date -u +%Y-%m-%dT%H:%M:%SZ); snapshot p13-size3 "$object" "$temporary/recreate-after.json"; snapshot_osd 3 "$temporary/recreate-osd-after.json"
lifecycle=$(jq -n --argjson before "$(cat "$temporary/recreate-osd-before.json")" --argjson destroyed "$(cat "$temporary/recreate-osd-destroyed.json")" --argjson after "$(cat "$temporary/recreate-osd-after.json")" '{before:$before,destroyed:$destroyed,after:$after}')
add_scenario p13-destroy-recreate p13-size3 read destroy-recreate-osd-3-new-uuid-address "$injected" "$released" "$temporary/recreate-before.json" "$temporary/recreate-after.json" "$temporary/recreate-go.json" "$temporary/recreate-native.json" remap '[]' 'osd.3 reused only after destroy with a new UUID, volume, and client address' "$lifecycle"

monitor_command='{"prefix":"status","format":"json"}'
phase=p13-mon-offline-failover
printf '%s\n' 'p13: active monitor offline failover'
snapshot p13-size3 "$object" "$temporary/mon-offline-before.json"; snapshot_monmap "$temporary/mon-offline-map-before.json"
rm -f "$temporary/mon-offline-go-ready" "$temporary/mon-offline-native-ready" "$temporary/mon-offline-release"
export P13_RELEASE_FILE=/work/mon-offline-release P13_READY_FILE=/work/mon-offline-go-ready
run_go p13-size3 monitor-command status "$monitor_command" '' "$temporary/mon-offline-go.json" & go_pid=$!
export P13_READY_FILE=/work/mon-offline-native-ready
run_native p13-size3 monitor-command status "$monitor_command" "$temporary/mon-offline-native.json" & native_pid=$!
unset P13_READY_FILE P13_RELEASE_FILE
for attempt in $(seq 1 "$poll_limit"); do test -f "$temporary/mon-offline-go-ready" && test -f "$temporary/mon-offline-native-ready" && break; kill -0 "$go_pid" 2>/dev/null && kill -0 "$native_pid" 2>/dev/null || exit 1; test "$attempt" -lt "$poll_limit" || exit 1; sleep 1; done
injected=$(date -u +%Y-%m-%dT%H:%M:%SZ); docker stop "p13-mon-a-$run_id" >/dev/null; wait_quorum 2; : >"$temporary/mon-offline-release"; wait "$go_pid"; wait "$native_pid"
docker start "p13-mon-a-$run_id" >/dev/null; wait_quorum 3; released=$(date -u +%Y-%m-%dT%H:%M:%SZ)
snapshot p13-size3 "$object" "$temporary/mon-offline-after.json"; snapshot_monmap "$temporary/mon-offline-map-after.json"
monitor_lifecycle=$(jq -n --argjson before "$(cat "$temporary/mon-offline-map-before.json")" --argjson after "$(cat "$temporary/mon-offline-map-after.json")" '{before:$before,after:$after}')
add_scenario p13-mon-offline-failover cluster monitor-command mon-a-offline "$injected" "$released" "$temporary/mon-offline-before.json" "$temporary/mon-offline-after.json" "$temporary/mon-offline-go.json" "$temporary/mon-offline-native.json" '' '[]' 'both clients failed over and completed a monitor command while mon.a was offline' null "$monitor_lifecycle"

phase=p13-mon-unreliable
printf '%s\n' 'p13: bounded multi-monitor instability'
snapshot p13-size3 "$object" "$temporary/mon-unreliable-before.json"; snapshot_monmap "$temporary/mon-unreliable-map-before.json"
rm -f "$temporary/mon-unreliable-go-ready" "$temporary/mon-unreliable-native-ready" "$temporary/mon-unreliable-release"
export P13_RELEASE_FILE=/work/mon-unreliable-release P13_READY_FILE=/work/mon-unreliable-go-ready
run_go p13-size3 monitor-command status "$monitor_command" '' "$temporary/mon-unreliable-go.json" & go_pid=$!
export P13_READY_FILE=/work/mon-unreliable-native-ready
run_native p13-size3 monitor-command status "$monitor_command" "$temporary/mon-unreliable-native.json" & native_pid=$!
unset P13_READY_FILE P13_RELEASE_FILE
for attempt in $(seq 1 "$poll_limit"); do test -f "$temporary/mon-unreliable-go-ready" && test -f "$temporary/mon-unreliable-native-ready" && break; kill -0 "$go_pid" 2>/dev/null && kill -0 "$native_pid" 2>/dev/null || exit 1; test "$attempt" -lt "$poll_limit" || exit 1; sleep 1; done
injected=$(date -u +%Y-%m-%dT%H:%M:%SZ)
docker stop "p13-mon-b-$run_id" >/dev/null; wait_quorum 2; docker start "p13-mon-b-$run_id" >/dev/null; wait_quorum 3
docker stop "p13-mon-a-$run_id" >/dev/null; wait_quorum 2; : >"$temporary/mon-unreliable-release"; wait "$go_pid"; wait "$native_pid"
docker start "p13-mon-a-$run_id" >/dev/null; wait_quorum 3; released=$(date -u +%Y-%m-%dT%H:%M:%SZ)
snapshot p13-size3 "$object" "$temporary/mon-unreliable-after.json"; snapshot_monmap "$temporary/mon-unreliable-map-after.json"
monitor_lifecycle=$(jq -n --argjson before "$(cat "$temporary/mon-unreliable-map-before.json")" --argjson after "$(cat "$temporary/mon-unreliable-map-after.json")" '{before:$before,after:$after}')
add_scenario p13-mon-unreliable cluster monitor-command bounded-mon-stop-start "$injected" "$released" "$temporary/mon-unreliable-before.json" "$temporary/mon-unreliable-after.json" "$temporary/mon-unreliable-go.json" "$temporary/mon-unreliable-native.json" '' '[]' 'bounded mon.b and mon.a outages preserved quorum and monitor-command completion' null "$monitor_lifecycle"

phase=p13-mon-changed
printf '%s\n' 'p13: monitor location change'
snapshot p13-size3 "$object" "$temporary/mon-changed-before.json"; snapshot_monmap "$temporary/mon-changed-map-before.json"; injected=$(date -u +%Y-%m-%dT%H:%M:%SZ)
ceph_cli mon set_location c rack=r1 >/dev/null
for attempt in $(seq 1 "$poll_limit"); do ceph_cli mon dump --format json 2>/dev/null | jq -e '.mons[] | select(.name == "c") | (.crush_location | tostring) | contains("r1")' >/dev/null && break; test "$attempt" -lt "$poll_limit" || exit 1; sleep 1; done
run_go p13-size3 monitor-command status "$monitor_command" '' "$temporary/mon-changed-go.json"; run_native p13-size3 monitor-command status "$monitor_command" "$temporary/mon-changed-native.json"
released=$(date -u +%Y-%m-%dT%H:%M:%SZ); snapshot p13-size3 "$object" "$temporary/mon-changed-after.json"; snapshot_monmap "$temporary/mon-changed-map-after.json"
monitor_lifecycle=$(jq -n --argjson before "$(cat "$temporary/mon-changed-map-before.json")" --argjson after "$(cat "$temporary/mon-changed-map-after.json")" '{before:$before,after:$after}')
add_scenario p13-mon-changed cluster monitor-command mon-c-location-change "$injected" "$released" "$temporary/mon-changed-before.json" "$temporary/mon-changed-after.json" "$temporary/mon-changed-go.json" "$temporary/mon-changed-native.json" '' '[]' 'authoritative monmap changed mon.c location while commands remained available' null "$monitor_lifecycle"

phase=p13-mon-removed
printf '%s\n' 'p13: monitor removal'
snapshot p13-size3 "$object" "$temporary/mon-removed-before.json"; snapshot_monmap "$temporary/mon-removed-map-before.json"; injected=$(date -u +%Y-%m-%dT%H:%M:%SZ)
docker stop "p13-mon-c-$run_id" >/dev/null; ceph_cli mon remove c >/dev/null
for attempt in $(seq 1 "$poll_limit"); do ceph_cli mon dump --format json 2>/dev/null | jq -e '([.mons[].name] | index("c")) == null' >/dev/null && break; test "$attempt" -lt "$poll_limit" || exit 1; sleep 1; done
wait_quorum 2; run_go p13-size3 monitor-command status "$monitor_command" '' "$temporary/mon-removed-go.json"; run_native p13-size3 monitor-command status "$monitor_command" "$temporary/mon-removed-native.json"
released=$(date -u +%Y-%m-%dT%H:%M:%SZ); snapshot p13-size3 "$object" "$temporary/mon-removed-after.json"; snapshot_monmap "$temporary/mon-removed-map-after.json"
monitor_lifecycle=$(jq -n --argjson before "$(cat "$temporary/mon-removed-map-before.json")" --argjson after "$(cat "$temporary/mon-removed-map-after.json")" '{before:$before,after:$after}')
add_scenario p13-mon-removed cluster monitor-command mon-c-removed "$injected" "$released" "$temporary/mon-removed-before.json" "$temporary/mon-removed-after.json" "$temporary/mon-removed-go.json" "$temporary/mon-removed-native.json" '' '[]' 'authoritative monmap removed mon.c while the remaining quorum served both clients' null "$monitor_lifecycle"

phase=p13-mon-added
printf '%s\n' 'p13: monitor re-addition'
snapshot p13-size3 "$object" "$temporary/mon-added-before.json"; snapshot_monmap "$temporary/mon-added-map-before.json"; injected=$(date -u +%Y-%m-%dT%H:%M:%SZ)
docker rm "p13-mon-c-$run_id" >/dev/null
ceph_cli mon getmap -o /cluster/monmap-c >/dev/null
docker run --rm --user 0 --platform "$platform" -v "$temporary:/cluster" "$image" sh -c '
	set -eu
	rm -rf /cluster/mondata-c; mkdir -p /cluster/mondata-c
	monmaptool --addv c "[v2:172.30.113.13:3300/0]" /cluster/monmap-c
	ceph-mon --mkfs -i c --fsid 13131313-2222-4333-8444-131313131313 --monmap /cluster/monmap-c --keyring /cluster/mon.keyring --mon-data /cluster/mondata-c
	chown -R ceph:ceph /cluster/mondata-c
'
ceph_cli mon add c 172.30.113.13:3300 >/dev/null; start_mon c 172.30.113.13; wait_quorum 3
run_go p13-size3 monitor-command status "$monitor_command" '' "$temporary/mon-added-go.json"; run_native p13-size3 monitor-command status "$monitor_command" "$temporary/mon-added-native.json"
released=$(date -u +%Y-%m-%dT%H:%M:%SZ); snapshot p13-size3 "$object" "$temporary/mon-added-after.json"; snapshot_monmap "$temporary/mon-added-map-after.json"
monitor_lifecycle=$(jq -n --argjson before "$(cat "$temporary/mon-added-map-before.json")" --argjson after "$(cat "$temporary/mon-added-map-after.json")" '{before:$before,after:$after}')
add_scenario p13-mon-added cluster monitor-command mon-c-added "$injected" "$released" "$temporary/mon-added-before.json" "$temporary/mon-added-after.json" "$temporary/mon-added-go.json" "$temporary/mon-added-native.json" '' '[]' 'mon.c rejoined the authoritative monmap and quorum; both clients completed commands' null "$monitor_lifecycle"

: >"$temporary/changes-release"; wait "$changes_pid"
jq -e '.completed and .operation == "cluster-changes" and (.changes | length) > 0' "$temporary/changes-go.json" >/dev/null

phase=p13-evidence-finalization
docker run --rm --platform "$platform" -v "$temporary:/cluster" "$image" sh -c '
	set -eu
	ceph --version > /cluster/ceph.version
	library=$(ldconfig -p | awk "/librados.so.2/{print \$NF; exit}"); test -n "$library"
	readlink -f "$library" > /cluster/librados.path; sha256sum "$(readlink -f "$library")" | cut -d " " -f 1 > /cluster/librados.sha256
'
artifacts="$temporary/artifacts.json"; printf '{}\n' >"$artifacts"
find . -type f ! -path './.git/*' ! -path './docs/p13/integration-report.json' ! -path './docs/p13/.integration-report.*' -print | LC_ALL=C sort | while IFS= read -r artifact_path; do
	artifact=${artifact_path#./}
	hash=$(shasum -a 256 "$artifact" | awk '{print $1}'); jq --arg path "$artifact" --arg hash "$hash" '. + {($path):$hash}' "$artifacts" >"$artifacts.next"; mv "$artifacts.next" "$artifacts"
done
jq -n --arg command "$command" --arg mode "$mode" --arg started "$started_at" --arg finished "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
	--arg image "$image" --arg platform "$platform" --arg version "$(cat "$temporary/ceph.version")" --arg path "$(cat "$temporary/librados.path")" --arg hash "$(cat "$temporary/librados.sha256")" \
	--argjson bytes "$device_bytes" --argjson artifacts "$(cat "$artifacts")" --argjson scenarios "$(cat "$scenarios")" --argjson changes "$(cat "$temporary/changes-go.json")" \
	'{schema_version:1,status:"passed",command:$command,mode:$mode,started_at:$started,finished_at:$finished,failure:null,source:{repository:"https://github.com/otuschhoff/rados-go.git",artifacts:$artifacts},server:{image:$image,platform:$platform,version:$version},native_runtime:{soname:"librados.so.2",path:$path,sha256:$hash},cluster:{fsid:"13131313-2222-4333-8444-131313131313",subnet:"172.30.113.0/24",monitors:["p13-mon-a","p13-mon-b","p13-mon-c"],manager:"p13-mgr-x",initial_osds:3,device_bytes:$bytes,pools:[{name:"p13-size3",size:3,min_size:2},{name:"p13-size1",size:1,min_size:1}],user:"client.p13"},scenarios:$scenarios,change_observations:$changes,unqualified:{multi_host_maintenance:"unqualified: disposable harness uses one Docker host",probabilistic_packet_loss:"unqualified: no host-global or probabilistic network fault injection is used"}}' >"$report_stage"

npm install --silent --ignore-scripts --no-audit --no-fund --prefix "$ajv_root" ajv@8.17.1 ajv-formats@3.0.1
node - "$ajv_root" integration/p13/report.schema.json "$report_stage" <<'NODE'
const fs = require("fs");
const root = process.argv[2];
const Ajv2020 = require(`${root}/node_modules/ajv/dist/2020`).default;
const addFormats = require(`${root}/node_modules/ajv-formats`);
const schema = JSON.parse(fs.readFileSync(process.argv[3], "utf8"));
const data = JSON.parse(fs.readFileSync(process.argv[4], "utf8"));
const ajv = new Ajv2020({allErrors: true, strict: true});
addFormats(ajv);
const validate = ajv.compile(schema);
if (!validate(data)) {
	console.error(JSON.stringify(validate.errors, null, 2));
	process.exit(1);
}
NODE
jq -e '
	(keys | sort) == (["change_observations","cluster","command","failure","finished_at","mode","native_runtime","scenarios","schema_version","server","source","started_at","status","unqualified"] | sort) and
	.schema_version == 1 and .status == "passed" and .failure == null and
	(.scenarios | length) == 22 and ([.scenarios[].id] | length == (unique | length)) and
	([.scenarios[].mutation_markers[]] | length == (unique | length)) and
	all(.scenarios[];
		(keys | sort) == (["after","before","fault","fault_intervals","final_data","go","id","injected_at","lifecycle","monitor_lifecycle","mutation_markers","native","notes","operation","pool","released_at","status"] | sort) and
		.status == "passed" and .before.epoch > 0 and .after.epoch > 0 and
		(.before.acting | type) == "array" and (.after.acting | type) == "array" and
		(.injected_at | test("Z$")) and (.released_at | test("Z$"))) and
	.change_observations.completed and .change_observations.operation == "cluster-changes" and (.change_observations.changes | length) > 0 and
	.unqualified.multi_host_maintenance == "unqualified: disposable harness uses one Docker host" and
	.unqualified.probabilistic_packet_loss == "unqualified: no host-global or probabilistic network fault injection is used"
' "$report_stage" >/dev/null
if test "$quick" = true; then
	CGO_ENABLED=0 GOTOOLCHAIN=go1.27.1 go run ./tools/p13-verify -report "$report_stage" -allow-quick
else
	CGO_ENABLED=0 GOTOOLCHAIN=go1.27.1 go run ./tools/p13-verify -report "$report_stage"
fi
trap '' HUP INT TERM
mv "$report_stage" "$report"
publication_started=true
completed=true
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
printf '%s\n' "P13 lifecycle quick=$quick passed; report: $report"