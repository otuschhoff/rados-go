#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
cd "$root"
started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
case "$(docker info --format '{{.Architecture}}')" in
	x86_64|amd64) platform=linux/amd64; goarch=amd64 ;;
	aarch64|arm64) platform=linux/arm64; goarch=arm64 ;;
	*) printf '%s\n' 'unsupported Docker architecture' >&2; exit 2 ;;
esac
image_index=$(jq -r '.images.qualification.reference' docs/p00/evidence.json)
image_digest=$(jq -r --arg architecture "$goarch" '.images.qualification[$architecture]' docs/p00/evidence.json)
image="${image_index%@*}@$image_digest"
temporary=${P11_ARTIFACT_DIR:-$(mktemp -d)}
mkdir -p "$temporary"
network="rados-go-p11-$$"
fsid=21111111-2222-4333-8444-111111111111
report="$root/docs/p11/.integration-report.json.$$"
cleanup() {
	exit_code=$?
	if test "$exit_code" -ne 0; then
		for daemon in "p11-mon-$$" "p11-mgr-a-$$" "p11-mgr-b-$$" "p11-osd-0-$$" "p11-recovery-$$"; do
			docker logs "$daemon" 2>&1 | tail -n 40 >&2 || true
		done
	fi
	docker rm -f "p11-mon-$$" "p11-mgr-a-$$" "p11-mgr-b-$$" "p11-osd-0-$$" "p11-recovery-$$" >/dev/null 2>&1 || true
	docker volume rm "rados-go-p11-osd-0-$$" >/dev/null 2>&1 || true
	docker network rm "$network" >/dev/null 2>&1 || true
	rm -f "$report"
	if test -z "${P11_ARTIFACT_DIR:-}"; then rm -rf "$temporary"; fi
	exit "$exit_code"
}
trap cleanup EXIT HUP INT TERM

CGO_ENABLED=0 GOOS=linux GOARCH="$goarch" go build -trimpath -o "$temporary/probe" ./integration/p11/probe
cp integration/p11/native_driver.c "$temporary/"
docker run --rm --user 0 --platform "$platform" -v "$temporary:/cluster" "$image" sh -c '
	set -eu
	cc -std=c11 -Wall -Wextra -Werror -O2 /cluster/native_driver.c -ldl -o /cluster/native-driver
'
docker network create --subnet 172.30.111.0/24 "$network" >/dev/null
docker run --rm --user 0 --platform "$platform" -v "$temporary:/cluster" "$image" sh -c '
	set -eu
	cat >/cluster/ceph.conf <<EOF
[global]
fsid = 21111111-2222-4333-8444-111111111111
mon host = v2:172.30.111.10:3300
auth cluster required = cephx
auth service required = cephx
auth client required = cephx
ms bind msgr1 = false
ms bind msgr2 = true
osd pool default size = 1
osd pool default min size = 1
mon_allow_pool_size_one = true
mon_allow_pool_delete = true
EOF
	ceph-authtool /cluster/mon.keyring --create-keyring --gen-key -n mon. --cap mon "allow *"
	ceph-authtool /cluster/admin.keyring --create-keyring --gen-key -n client.admin --cap mon "allow *" --cap osd "allow *" --cap mgr "allow *"
	ceph-authtool /cluster/mon.keyring --import-keyring /cluster/admin.keyring
	monmaptool --create --fsid 21111111-2222-4333-8444-111111111111 --addv a "[v2:172.30.111.10:3300/0]" /cluster/monmap
	mkdir -p /cluster/mondata
	ceph-mon --mkfs -i a --fsid 21111111-2222-4333-8444-111111111111 --monmap /cluster/monmap --keyring /cluster/mon.keyring --mon-data /cluster/mondata
	chown -R ceph:ceph /cluster/mondata
'
docker run -d --name "p11-mon-$$" --platform "$platform" --network "$network" --ip 172.30.111.10 -v "$temporary:/cluster" "$image" \
	ceph-mon -f -i a --conf /cluster/ceph.conf --mon-data /cluster/mondata --public-addr v2:172.30.111.10:3300 --setuser ceph --setgroup ceph --mon-data-avail-crit 0 --no-mon-cluster-log-to-stderr >/dev/null
ceph_cli() {
	docker run --rm --platform "$platform" --network "$network" -v "$temporary:/cluster" "$image" \
		timeout 20 ceph --conf /cluster/ceph.conf --name client.admin --keyring /cluster/admin.keyring "$@"
}
for attempt in $(seq 1 30); do
	if ceph_cli status --format json 2>/dev/null | jq -e '.health.status != null' >/dev/null; then break; fi
	test "$attempt" -lt 30 || exit 1
	sleep 1
done

volume="rados-go-p11-osd-0-$$"
uuid=11000000-0000-4000-8000-000000000010
docker volume create "$volume" >/dev/null
ceph_cli osd create "$uuid" 0 >/dev/null
ceph_cli auth get-or-create osd.0 mon 'allow profile osd' mgr 'allow profile osd' osd 'allow *' -o /cluster/osd-0.keyring
ceph_cli mon getmap -o /cluster/osd-0.monmap >/dev/null
docker run --rm --user 0 --privileged --platform "$platform" -v "$temporary:/cluster" -v "$volume:/osd" "$image" sh -c '
	set -eu
	mkdir -p /osd/data
	truncate -s 4G /osd/block
	cp /cluster/osd-0.keyring /osd/data/keyring
	cp /cluster/osd-0.monmap /osd/data/activate.monmap
	chown -R ceph:ceph /osd
	ceph-osd --mkfs -i 0 --osd-data /osd/data --osd-uuid 11000000-0000-4000-8000-000000000010 --osd-objectstore bluestore --bluestore-block-path /osd/block --monmap /osd/data/activate.monmap --keyring /osd/data/keyring --setuser ceph --setgroup ceph
'
docker run -d --privileged --name "p11-osd-0-$$" --platform "$platform" --network "$network" --ip 172.30.111.20 -v "$temporary:/cluster" -v "$volume:/osd" "$image" \
	ceph-osd -f --conf /cluster/ceph.conf -i 0 --osd-data /osd/data --osd-objectstore bluestore --public-addr v2:172.30.111.20:6800 --cluster-addr v2:172.30.111.20:6802 --log-to-stderr true --err-to-stderr true --log-file '' --setuser ceph --setgroup ceph >/dev/null
for attempt in $(seq 1 60); do
	if ceph_cli osd stat --format json 2>/dev/null | jq -e '.num_osds == 1 and .num_up_osds == 1 and .num_in_osds == 1' >/dev/null; then break; fi
	test "$attempt" -lt 60 || exit 1
	sleep 1
done

for id in a b; do
	ceph_cli auth get-or-create "mgr.$id" mon 'allow profile mgr' osd 'allow *' mds 'allow *' -o "/cluster/mgr-$id.keyring"
	mkdir -p "$temporary/mgr-$id"
	cp "$temporary/mgr-$id.keyring" "$temporary/mgr-$id/keyring"
	chmod 600 "$temporary/mgr-$id/keyring"
	docker run -d --name "p11-mgr-$id-$$" --platform "$platform" --network "$network" --ip "172.30.111.$(test "$id" = a && printf 30 || printf 31)" -v "$temporary:/cluster" "$image" \
		ceph-mgr -f -i "$id" --conf /cluster/ceph.conf --mgr-data "/cluster/mgr-$id" --keyring "/cluster/mgr-$id/keyring" --setuser ceph --setgroup ceph --log-to-stderr true --err-to-stderr true --log-file '' >/dev/null
done
for attempt in $(seq 1 60); do
	if ceph_cli mgr dump --format json 2>/dev/null | jq -e '.active_name != "" and (.standbys | length) == 1' >/dev/null; then break; fi
	test "$attempt" -lt 60 || exit 1
	sleep 1
done

ceph_cli osd pool create p11-data 8 >/dev/null
ceph_cli osd pool create p11-native-app 8 >/dev/null
for pool in p11-data p11-native-app; do ceph_cli osd pool set "$pool" size 1 --yes-i-really-mean-it >/dev/null; done
printf 'p11-seed\n' >"$temporary/seed"
for attempt in $(seq 1 120); do
	if docker run --rm --platform "$platform" --network "$network" -v "$temporary:/cluster" "$image" timeout 10 rados --conf /cluster/ceph.conf --name client.admin --keyring /cluster/admin.keyring --pool p11-data put command-object /cluster/seed >/dev/null 2>&1; then break; fi
	test "$attempt" -lt 120 || exit 1
	sleep 1
done
pg=$(ceph_cli osd map p11-data command-object --format json | jq -r '.pgid')
pool_id=$(ceph_cli osd pool ls detail --format json | jq -r '.[] | select(.pool_name == "p11-data") | .pool_id')
test -n "$pg" && test -n "$pool_id"
for attempt in $(seq 1 120); do
	if ceph_cli pg dump pgs --format json 2>/dev/null | jq -e --arg pg "$pg" '.pg_stats[] | select(.pgid == $pg) | .state | contains("active+clean")' >/dev/null; then break; fi
	test "$attempt" -lt 120 || exit 1
	sleep 1
done
last_deep_scrub=$(ceph_cli pg dump pgs --format json | jq -r --arg pg "$pg" '.pg_stats[] | select(.pgid == $pg) | .last_deep_scrub_stamp')
ceph_cli pg deep-scrub "$pg" >/dev/null
for attempt in $(seq 1 120); do
	current_deep_scrub=$(ceph_cli pg dump pgs --format json 2>/dev/null | jq -r --arg pg "$pg" '.pg_stats[] | select(.pgid == $pg) | .last_deep_scrub_stamp')
	if test -n "$current_deep_scrub" && test "$current_deep_scrub" != "$last_deep_scrub"; then break; fi
	test "$attempt" -lt 120 || exit 1
	sleep 1
done

ceph_cli auth get-or-create client.p11-admin mon 'allow *' mgr 'allow *' osd 'allow *' >/dev/null
ceph_cli auth get-key client.p11-admin >"$temporary/admin.key"
ceph_cli auth get client.p11-admin -o /cluster/admin-client.keyring >/dev/null
ceph_cli auth get-or-create client.p11-io mon 'allow r' osd 'allow rw pool=p11-data' >/dev/null
ceph_cli auth get-key client.p11-io >"$temporary/io.key"
ceph_cli auth get client.p11-io --format json | jq '.[0] | {entity,mon_caps:.caps.mon,mgr_caps:(.caps.mgr // ""),osd_caps:.caps.osd}' >"$temporary/io-auth.json"
ceph_cli auth get client.p11-admin --format json | jq '.[0] | {entity,mon_caps:.caps.mon,mgr_caps:.caps.mgr,osd_caps:.caps.osd}' >"$temporary/admin-auth.json"

docker run --rm --platform "$platform" --network "$network" -v "$temporary:/cluster" "$image" \
	timeout 60 /cluster/native-driver /cluster/ceph.conf /cluster/admin-client.keyring "$pg" "$pool_id" >"$temporary/native.json"
docker run --rm --platform "$platform" --network "$network" -v "$temporary:/work" "$image" \
	timeout 90 /work/probe -mode admin -monitors 172.30.111.10:3300 -key /work/admin.key -fsid "$fsid" -pg "$pg" -osd 0 >"$temporary/admin.json"

rm -f "$temporary/ready" "$temporary/continue" "$temporary/failover-done" "$temporary/managerless"
docker run -d --name "p11-recovery-$$" --platform "$platform" --network "$network" -v "$temporary:/work" "$image" sh -c \
	'timeout 140 /work/probe -mode recovery -monitors 172.30.111.10:3300 -key /work/admin.key -fsid '"$fsid"' -coordination-dir /work >/work/recovery.json' >/dev/null
for attempt in $(seq 1 100); do test -f "$temporary/ready" && break; test "$attempt" -lt 100 || exit 1; sleep 1; done
active=$(ceph_cli mgr dump --format json | jq -r '.active_name')
test "$active" = a || test "$active" = b
docker rm -f "p11-mgr-$active-$$" >/dev/null
for attempt in $(seq 1 60); do
	new_active=$(ceph_cli mgr dump --format json 2>/dev/null | jq -r '.active_name // empty')
	if test -n "$new_active" && test "$new_active" != "$active"; then break; fi
	test "$attempt" -lt 60 || exit 1
	sleep 1
done
: >"$temporary/continue"
for attempt in $(seq 1 100); do test -f "$temporary/failover-done" && break; test "$attempt" -lt 100 || exit 1; sleep 1; done
docker rm -f "p11-mgr-$new_active-$$" >/dev/null
: >"$temporary/managerless"
test "$(docker wait "p11-recovery-$$")" -eq 0

docker run --rm --platform "$platform" --network "$network" -v "$temporary:/work" "$image" \
	timeout 60 /work/probe -mode least -entity client.p11-io -monitors 172.30.111.10:3300 -key /work/io.key -fsid "$fsid" >"$temporary/least.json"

jq -e '([del(.session_addresses)[]] | all(. == true)) and (.session_addresses | length > 0)' "$temporary/admin.json" >/dev/null
jq -e 'all(.[]; . == true)' "$temporary/native.json" >/dev/null
jq -e 'all(.[]; . == true)' "$temporary/recovery.json" >/dev/null
jq -e '.write_read_without_manager' "$temporary/least.json" >/dev/null
ceph_cli fsid --format json >"$temporary/fsid.json"
ceph_cli osd stat --format json >"$temporary/osd-stat.json"
ceph_cli osd metadata --format json >"$temporary/osd-metadata.json"
ceph_cli osd pool get p11-data all --format json >"$temporary/data-pool.json"
docker run --rm --platform "$platform" -v "$temporary:/cluster" "$image" sh -c '
	set -eu
	ceph --version > /cluster/ceph.version
	sha256sum "$(command -v ceph-mon)" | awk "{print \$1}" > /cluster/ceph-mon.sha256
	sha256sum "$(command -v ceph-mgr)" | awk "{print \$1}" > /cluster/ceph-mgr.sha256
	sha256sum "$(command -v ceph-osd)" | awk "{print \$1}" > /cluster/ceph-osd.sha256
	library=$(ldconfig -p | awk "/librados.so.2/{print \$NF; exit}")
	readlink -f "$library" > /cluster/librados.path
	sha256sum "$(readlink -f "$library")" | awk "{print \$1}" > /cluster/librados.sha256
	rpm -qf "$(readlink -f "$library")" > /cluster/librados.package
'

artifacts="$temporary/artifacts.json"
printf '{}\n' >"$artifacts"
artifact_paths=$(find internal -type f -name '*.go' -print; find . -maxdepth 1 -type f -name '*.go' -print | sed 's#^./##'; printf '%s\n' Makefile SPEC.md go.mod go.sum tools/api-inventory/main.go tools/api-inventory/main_test.go integration/p11/native_driver.c integration/p11/probe/main.go integration/p11/report.schema.json integration/p11/reproduce.sh tools/p11-verify/main.go tools/p11-verify/main_test.go docs/p00/api-inventory.csv docs/p01/public-api.md docs/p11/tasks.md docs/p11/provenance.md)
for artifact in $artifact_paths; do
	test -f "$artifact"
	hash=$(shasum -a 256 "$artifact" | awk '{print $1}')
	jq --arg path "$artifact" --arg hash "$hash" '. + {($path):$hash}' "$artifacts" >"$artifacts.next"
	mv "$artifacts.next" "$artifacts"
done
mkdir -p docs/p11
jq -n \
	--arg started_at "$started_at" --arg finished_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --arg image "$image" --arg platform "$platform" \
	--arg ceph_version "$(cat "$temporary/ceph.version")" --arg ceph_mon_sha256 "$(cat "$temporary/ceph-mon.sha256")" --arg ceph_mgr_sha256 "$(cat "$temporary/ceph-mgr.sha256")" --arg ceph_osd_sha256 "$(cat "$temporary/ceph-osd.sha256")" \
	--arg librados_path "$(cat "$temporary/librados.path")" --arg librados_sha256 "$(cat "$temporary/librados.sha256")" --arg librados_package "$(cat "$temporary/librados.package")" --argjson fsid "$(cat "$temporary/fsid.json")" \
	--argjson osd_stat "$(cat "$temporary/osd-stat.json")" --argjson osd_metadata "$(cat "$temporary/osd-metadata.json")" --argjson pool "$(cat "$temporary/data-pool.json")" \
	--argjson admin_client "$(cat "$temporary/admin-auth.json")" --argjson io_client "$(cat "$temporary/io-auth.json")" --argjson artifacts "$(cat "$artifacts")" \
	--argjson admin "$(cat "$temporary/admin.json")" --argjson native "$(cat "$temporary/native.json")" --argjson recovery "$(cat "$temporary/recovery.json")" --argjson least "$(cat "$temporary/least.json")" \
	'{schema_version:1,status:"passed",command:"make integration-p11",started_at:$started_at,finished_at:$finished_at,source:{repository:"https://github.com/otuschhoff/rados-go.git",identity:"content-addressed-artifacts",artifacts:$artifacts},server:{repository:"https://github.com/ceph/ceph.git",source_anchor_commit:"7f793731f1b39eb4f465e960113d2363c311b964",version:$ceph_version,image:$image,platform:$platform,binaries:{mon_sha256:$ceph_mon_sha256,mgr_sha256:$ceph_mgr_sha256,osd_sha256:$ceph_osd_sha256}},native_runtime:{soname:"librados.so.2",path:$librados_path,package:$librados_package,sha256:$librados_sha256},cluster:{fsid:$fsid.fsid,network:"172.30.111.0/24",osds:$osd_stat.num_osds,objectstore:(if ($osd_metadata|length)==$osd_stat.num_osds and all($osd_metadata[];.osd_objectstore=="bluestore") then "bluestore" else "mixed" end),osd_device_bytes:4294967296,pool:{name:$pool.pool,size:$pool.size,min_size:$pool.min_size,pg_num:$pool.pg_num},manager_daemons:2,admin_client:$admin_client,io_client:$io_client},scenarios:{administration:"passed",native_conformance:"passed",manager_failover:"passed",manager_loss_io:"passed",least_privilege:"passed",destructive_resource_validation:"passed"},probe:{admin:$admin,recovery:$recovery,least_privilege:$least},native:$native}' >"$report"
mv "$report" docs/p11/integration-report.json
printf '%s\n' 'P11 live administrative and manager qualification passed'
