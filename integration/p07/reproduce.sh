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
network="rados-go-p07-$$"
fsid=11111111-2222-4333-8444-777777777777
cleanup() {
	docker rm -f "p07-probe-$$" "p07-mon-$$" "p07-osd-0-$$" "p07-osd-1-$$" "p07-osd-2-$$" >/dev/null 2>&1 || true
	docker volume rm "rados-go-p07-osd-0-$$" "rados-go-p07-osd-1-$$" "rados-go-p07-osd-2-$$" >/dev/null 2>&1 || true
	docker network rm "$network" >/dev/null 2>&1 || true
	rm -rf "$temporary"
}
trap cleanup EXIT HUP INT TERM

CGO_ENABLED=0 GOOS=linux GOARCH="$goarch" go build -trimpath -o "$temporary/probe" ./integration/p07/probe
CGO_ENABLED=0 GOOS=linux GOARCH="$goarch" go build -trimpath -o "$temporary/benchmark" ./integration/p07/benchmark
cp integration/p07/native_driver.c integration/p07/native_benchmark.c "$temporary/"
docker run --rm --user 0 --platform "$platform" -v "$temporary:/cluster" "$image" sh -c '
	set -eu
	cc -std=c11 -Wall -Wextra -Werror -O2 /cluster/native_driver.c -ldl -o /cluster/native-driver
	cc -std=c11 -Wall -Wextra -Werror -O2 -pthread /cluster/native_benchmark.c -ldl -o /cluster/native-benchmark
'
printf 'aZ\000\000efg' >"$temporary/parity.expected"
printf base >"$temporary/base"
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
'
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
ceph_cli auth get-or-create client.p07 mon 'allow r' osd 'allow rw pool=p07-data' >/dev/null
ceph_cli auth get-key client.p07 >"$temporary/client.key"
ceph_cli auth get client.p07 -o /cluster/client.keyring >/dev/null

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

docker run --rm --platform "$platform" --network "$network" -v "$temporary:/cluster" "$image" \
	timeout 60 /cluster/native-driver seed /cluster/ceph.conf /cluster/admin.keyring p07-data >"$temporary/native-seed.json"
jq -e '.native_crud and .mixed_seed' "$temporary/native-seed.json" >/dev/null

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
	'{schema_version:1,status:"passed",command:"make integration-p07",started_at:$started_at,finished_at:$finished_at,source:{repository:"https://github.com/otuschhoff/rados-go.git",identity:"content-addressed-artifacts",artifacts:$artifacts},server:{repository:"https://github.com/ceph/ceph.git",source_anchor_commit:"7f793731f1b39eb4f465e960113d2363c311b964",version:$ceph_version,image:$image,platform:$platform,binaries:{mon_sha256:$ceph_mon_sha256,osd_sha256:$ceph_osd_sha256}},native_runtime:{soname:"librados.so.2",path:$librados_path,package:$librados_package,sha256:$librados_sha256},cluster:{fsid:"11111111-2222-4333-8444-777777777777",osds:3,pool:"p07-data",replicas:2,osd_device_bytes:8589934592},scenarios:{go_crud:"passed",native_crud:"passed",native_seed_go_mutate_native_verify:"passed",go_write_native_read:"passed",exclusive_create:"passed",missing_semantics:"passed",flush:"passed",primary_change_append_once:"passed"},probe:$probe,native:{seed:$native_seed,verify:$native_verify},benchmark:{execution_environment:{kernel:$kernel,cpu_model:$cpu_model,logical_cpus:$logical_cpus,cpu_max:$cpu_max,memory_max:$memory_max,docker_server_version:$docker_server_version},methodology:{sizes_bytes:[4096,65536,1048576,4194304],concurrency:[1,16,64],workloads:["read","write","mixed"],operations_per_worker:2,transports:["secure","crc"],allocation_measurement:{go:"runtime.MemStats deltas",native:"unavailable from the dynamically loaded librados ABI"},results_are_baseline_not_parity_claim:true},runs:[$go_secure,$go_crc,$native_secure,$native_crc]}}' >docs/p07/integration-report.json
printf '%s\n' 'P07 real mutation interoperability passed'