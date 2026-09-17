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
temporary=$(mktemp -d)
network="rados-go-p08-$$"
fsid=11111111-2222-4333-8444-888888888888
cleanup() {
	status=$?
	if test "$status" -ne 0; then
		for daemon in "p08-osd-0-$$" "p08-osd-1-$$" "p08-osd-2-$$"; do
			printf '%s\n' "===== $daemon logs =====" >&2
			docker logs "$daemon" 2>&1 | tail -n 40 >&2 || true
		done
	fi
	docker rm -f "p08-probe-$$" "p08-mon-$$" "p08-osd-0-$$" "p08-osd-1-$$" "p08-osd-2-$$" >/dev/null 2>&1 || true
	docker volume rm "rados-go-p08-osd-0-$$" "rados-go-p08-osd-1-$$" "rados-go-p08-osd-2-$$" >/dev/null 2>&1 || true
	docker network rm "$network" >/dev/null 2>&1 || true
	rm -rf "$temporary"
	exit "$status"
}
trap cleanup EXIT HUP INT TERM

CGO_ENABLED=0 GOOS=linux GOARCH="$goarch" go build -trimpath -o "$temporary/probe" ./integration/p08/probe
cp integration/p08/native_driver.c "$temporary/"
docker run --rm --user 0 --platform "$platform" -v "$temporary:/cluster" "$image" sh -c '
	set -eu
	cc -std=c11 -Wall -Wextra -Werror -O2 /cluster/native_driver.c -ldl -o /cluster/native-driver
'
docker network create --subnet 172.30.98.0/24 "$network" >/dev/null
docker run --rm --user 0 --platform "$platform" -v "$temporary:/cluster" "$image" sh -c '
	set -eu
	cat >/cluster/ceph.conf <<EOF
[global]
fsid = 11111111-2222-4333-8444-888888888888
mon host = v2:172.30.98.10:3300
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
	monmaptool --create --fsid 11111111-2222-4333-8444-888888888888 --addv a "[v2:172.30.98.10:3300/0]" /cluster/monmap
	mkdir -p /cluster/mondata
	ceph-mon --mkfs -i a --fsid 11111111-2222-4333-8444-888888888888 --monmap /cluster/monmap --keyring /cluster/mon.keyring --mon-data /cluster/mondata
	chown -R ceph:ceph /cluster/mondata
'
docker run -d --name "p08-mon-$$" --platform "$platform" --network "$network" --ip 172.30.98.10 -v "$temporary:/cluster" "$image" \
	ceph-mon -f -i a --mon-data /cluster/mondata --public-addr v2:172.30.98.10:3300 --setuser ceph --setgroup ceph --mon-data-avail-crit 0 --no-mon-cluster-log-to-stderr >/dev/null

ceph_cli() {
	docker run --rm --platform "$platform" --network "$network" -v "$temporary:/cluster" "$image" \
		timeout 15 ceph --conf /cluster/ceph.conf --name client.admin --keyring /cluster/admin.keyring "$@"
}
for attempt in $(seq 1 30); do
	if ceph_cli status --format json 2>/dev/null | jq -e '.health.status != null' >/dev/null; then break; fi
	mon_running=$(docker inspect -f '{{.State.Running}}' "p08-mon-$$" 2>/dev/null || printf false)
	test "$mon_running" = true || { docker logs "p08-mon-$$" >&2; exit 1; }
	test "$attempt" -lt 30 || { docker logs "p08-mon-$$" >&2; exit 1; }
	sleep 1
done

for id in 0 1 2; do
	uuid="00000000-0000-4000-8000-00000000002$id"
	volume="rados-go-p08-osd-$id-$$"
	docker volume create "$volume" >/dev/null
	ceph_cli osd create "$uuid" "$id" >/dev/null
	ceph_cli auth get-or-create "osd.$id" mon 'allow profile osd' mgr 'allow profile osd' osd 'allow *' -o "/cluster/osd-$id.keyring"
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
	'
	ip="172.30.98.$((20 + id))"
	docker run -d --privileged --name "p08-osd-$id-$$" --platform "$platform" --network "$network" --ip "$ip" -v "$temporary:/cluster" -v "$volume:/osd" "$image" \
		ceph-osd -f --conf /cluster/ceph.conf -i "$id" --osd-data /osd/data --osd-objectstore bluestore --public-addr "v2:$ip:6800" --cluster-addr "v2:$ip:6802" --log-to-stderr true --err-to-stderr true --log-file '' --setuser ceph --setgroup ceph >/dev/null
done
for attempt in $(seq 1 60); do
	if ceph_cli osd stat --format json 2>/dev/null | jq -e '.num_osds == 3 and .num_up_osds == 3 and .num_in_osds == 3' >/dev/null; then break; fi
	test "$attempt" -lt 60 || { ceph_cli osd tree >&2; exit 1; }
	sleep 1
done

ceph_cli osd crush rule create-replicated p08-rule default osd >/dev/null
ceph_cli osd pool create p08-data 16 16 replicated p08-rule >/dev/null
ceph_cli osd pool set p08-data size 2 >/dev/null
ceph_cli osd pool set p08-data min_size 1 >/dev/null
ceph_cli auth get-or-create client.p08 mon 'allow r' osd 'allow rw pool=p08-data' >/dev/null
ceph_cli auth get-key client.p08 >"$temporary/client.key"
ceph_cli auth get client.p08 -o /cluster/client.keyring >/dev/null

docker run --rm --platform "$platform" --network "$network" -v "$temporary:/cluster" "$image" \
	timeout 60 /cluster/native-driver seed /cluster/ceph.conf /cluster/client.keyring p08-data >"$temporary/native-seed.json"
jq -e '.native_binary_metadata and .native_compound_seed' "$temporary/native-seed.json" >/dev/null
docker run --rm --name "p08-probe-$$" --platform "$platform" --network "$network" -v "$temporary:/work" "$image" \
	timeout 180 /work/probe -monitors 172.30.98.10:3300 -key /work/client.key -fsid "$fsid" -coordination-dir /work >"$temporary/probe.json" &
probe_pid=$!
for attempt in $(seq 1 60); do
	if test -f "$temporary/enumeration-ready"; then break; fi
	kill -0 "$probe_pid" 2>/dev/null || { wait "$probe_pid"; exit 1; }
	test "$attempt" -lt 60 || { printf '%s\n' 'probe did not reach enumeration map-change point' >&2; exit 1; }
	sleep 1
done
ceph_cli osd pool set p08-data pg_num 32 >/dev/null
: >"$temporary/map-changed"
wait "$probe_pid"
jq -e '.native_metadata and .binary_metadata and .omap_pagination and .compound_read and .compound_atomicity and .cross_client_contention and .enumeration and .namespaces and .cursor_continuation and .cursor_partitioning' "$temporary/probe.json" >/dev/null
docker run --rm --platform "$platform" --network "$network" -v "$temporary:/cluster" "$image" \
	timeout 60 /cluster/native-driver verify /cluster/ceph.conf /cluster/client.keyring p08-data >"$temporary/native-verify.json"
jq -e '.go_binary_metadata and .go_omap_native_read and .go_enumeration_native_read and .namespace_filtering' "$temporary/native-verify.json" >/dev/null

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
'
artifacts="$temporary/artifacts.json"
printf '{}\n' >"$artifacts"
artifact_paths=$(find internal/cephx internal/crush internal/encoding internal/maps internal/mon internal/msgr internal/objecter internal/osd internal/protocol -type f -name '*.go' -print; printf '%s\n' Makefile README.md SPEC.md client.go client_test.go doc.go errors.go errors_test.go object.go metadata.go enumeration_test.go metadata_test.go docs/p00/api-inventory.csv integration/README.md integration/p08/native_driver.c integration/p08/probe/main.go integration/p08/report.schema.json integration/p08/reproduce.sh tools/p08-verify/main.go tools/p08-verify/main_test.go docs/p08/tasks.md docs/p08/provenance.md go.mod go.sum)
for artifact in $artifact_paths; do
	test -f "$artifact"
	hash=$(shasum -a 256 "$artifact" | awk '{print $1}')
	jq --arg path "$artifact" --arg hash "$hash" '. + {($path):$hash}' "$artifacts" >"$artifacts.next"
	mv "$artifacts.next" "$artifacts"
done
mkdir -p docs/p08
jq -n \
	--arg started_at "$started_at" \
	--arg finished_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
	--arg image "$image" --arg platform "$platform" \
	--arg ceph_version "$(cat "$temporary/ceph.version")" \
	--arg ceph_mon_sha256 "$(cat "$temporary/ceph-mon.sha256")" \
	--arg ceph_osd_sha256 "$(cat "$temporary/ceph-osd.sha256")" \
	--arg librados_path "$(cat "$temporary/librados.path")" \
	--arg librados_sha256 "$(cat "$temporary/librados.sha256")" \
	--arg librados_package "$(cat "$temporary/librados.package")" \
	--argjson artifacts "$(cat "$artifacts")" \
	--argjson probe "$(cat "$temporary/probe.json")" \
	--argjson native_seed "$(cat "$temporary/native-seed.json")" \
	--argjson native_verify "$(cat "$temporary/native-verify.json")" \
	'{schema_version:1,status:"passed",command:"make integration-p08",started_at:$started_at,finished_at:$finished_at,source:{repository:"https://github.com/otuschhoff/rados-go.git",identity:"content-addressed-artifacts",artifacts:$artifacts},server:{repository:"https://github.com/ceph/ceph.git",source_anchor_commit:"7f793731f1b39eb4f465e960113d2363c311b964",version:$ceph_version,image:$image,platform:$platform,binaries:{mon_sha256:$ceph_mon_sha256,osd_sha256:$ceph_osd_sha256}},native_runtime:{soname:"librados.so.2",path:$librados_path,package:$librados_package,sha256:$librados_sha256},cluster:{fsid:"11111111-2222-4333-8444-888888888888",osds:3,pool:"p08-data",replicas:2,osd_device_bytes:8589934592},scenarios:{metadata_interoperability:"passed",compound_atomicity:"passed",cross_client_contention:"passed",enumeration_conformance:"passed",namespace_isolation:"passed",cursor_pagination_and_partitioning:"passed"},probe:$probe,native:{seed:$native_seed,verify:$native_verify}}' >docs/p08/integration-report.json
printf '%s\n' 'P08 metadata, compound, and enumeration qualification passed'