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
temporary=${P10_ARTIFACT_DIR:-$(mktemp -d)}
mkdir -p "$temporary"
network="go-librados-p10-$$"
fsid=21000000-2222-4333-8444-101010101010
report="$root/docs/p10/.integration-report.json.$$"
cleanup() {
	exit_code=$?
	if test "$exit_code" -ne 0; then
		for daemon in "p10-osd-0-$$" "p10-osd-1-$$" "p10-osd-2-$$"; do
			docker logs "$daemon" 2>&1 | tail -n 30 >&2 || true
		done
	fi
	docker rm -f "p10-mon-$$" "p10-osd-0-$$" "p10-osd-1-$$" "p10-osd-2-$$" >/dev/null 2>&1 || true
	docker volume rm "go-librados-p10-osd-0-$$" "go-librados-p10-osd-1-$$" "go-librados-p10-osd-2-$$" >/dev/null 2>&1 || true
	docker network rm "$network" >/dev/null 2>&1 || true
	rm -f "$report"
	if test -z "${P10_ARTIFACT_DIR:-}"; then rm -rf "$temporary"; fi
	exit "$exit_code"
}
trap cleanup EXIT HUP INT TERM

CGO_ENABLED=0 GOOS=linux GOARCH="$goarch" go build -trimpath -o "$temporary/probe" ./integration/p10/probe
cp integration/p10/native_driver.c "$temporary/"
docker run --rm --user 0 --platform "$platform" -v "$temporary:/cluster" "$image" sh -c '
	set -eu
	cc -std=c11 -Wall -Wextra -Werror -O2 /cluster/native_driver.c -ldl -o /cluster/native-driver
'
docker network create --subnet 172.30.110.0/24 "$network" >/dev/null
docker run --rm --user 0 --platform "$platform" -v "$temporary:/cluster" "$image" sh -c '
	set -eu
	cat >/cluster/ceph.conf <<EOF
[global]
fsid = 21000000-2222-4333-8444-101010101010
mon host = v2:172.30.110.10:3300
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
	monmaptool --create --fsid 21000000-2222-4333-8444-101010101010 --addv a "[v2:172.30.110.10:3300/0]" /cluster/monmap
	mkdir -p /cluster/mondata
	ceph-mon --mkfs -i a --fsid 21000000-2222-4333-8444-101010101010 --monmap /cluster/monmap --keyring /cluster/mon.keyring --mon-data /cluster/mondata
	chown -R ceph:ceph /cluster/mondata
'
docker run -d --name "p10-mon-$$" --platform "$platform" --network "$network" --ip 172.30.110.10 -v "$temporary:/cluster" "$image" \
	ceph-mon -f -i a --mon-data /cluster/mondata --public-addr v2:172.30.110.10:3300 --setuser ceph --setgroup ceph --mon-data-avail-crit 0 --no-mon-cluster-log-to-stderr >/dev/null
ceph_cli() {
	docker run --rm --platform "$platform" --network "$network" -v "$temporary:/cluster" "$image" \
		timeout 15 ceph --conf /cluster/ceph.conf --name client.admin --keyring /cluster/admin.keyring "$@"
}
for attempt in $(seq 1 30); do
	if ceph_cli status --format json 2>/dev/null | jq -e '.health.status != null' >/dev/null; then break; fi
	test "$attempt" -lt 30 || exit 1
	sleep 1
done
for id in 0 1 2; do
	uuid="10000000-0000-4000-8000-00000000001$id"
	volume="go-librados-p10-osd-$id-$$"
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
	ip="172.30.110.$((20 + id))"
	docker run -d --privileged --name "p10-osd-$id-$$" --platform "$platform" --network "$network" --ip "$ip" -v "$temporary:/cluster" -v "$volume:/osd" "$image" \
		ceph-osd -f --conf /cluster/ceph.conf -i "$id" --osd-data /osd/data --osd-objectstore bluestore --public-addr "v2:$ip:6800" --cluster-addr "v2:$ip:6802" --log-to-stderr true --err-to-stderr true --log-file '' --setuser ceph --setgroup ceph >/dev/null
done
for attempt in $(seq 1 60); do
	if ceph_cli osd stat --format json 2>/dev/null | jq -e '.num_osds == 3 and .num_up_osds == 3 and .num_in_osds == 3' >/dev/null; then break; fi
	test "$attempt" -lt 60 || exit 1
	sleep 1
done
ceph_cli osd crush rule create-replicated p10-replicated-rule default osd >/dev/null
ceph_cli osd erasure-code-profile set p10-ec-profile plugin=jerasure k=2 m=1 crush-failure-domain=osd stripe_unit=4096 >/dev/null
ceph_cli osd crush rule create-erasure p10-ec-rule p10-ec-profile >/dev/null
ceph_cli osd pool create p10-named 16 16 replicated p10-replicated-rule >/dev/null
ceph_cli osd pool create p10-self 16 16 replicated p10-replicated-rule >/dev/null
ceph_cli osd pool create p10-ec 16 16 erasure p10-ec-profile p10-ec-rule >/dev/null
for pool in p10-named p10-self; do
	ceph_cli osd pool set "$pool" size 2 >/dev/null
	ceph_cli osd pool set "$pool" min_size 1 >/dev/null
done
test "$(ceph_cli osd pool get p10-ec size --format json | jq -r '.size')" -eq 3
test "$(ceph_cli osd pool get p10-ec min_size --format json | jq -r '.min_size')" -eq 2

printf 'p10-ready\n' >"$temporary/readiness"
for attempt in $(seq 1 180); do
	ready=true
	for pool_object in p10-named:native-named p10-self:native-self p10-ec:ec-specialized; do
		pool=${pool_object%%:*}
		object=${pool_object#*:}
		if ! docker run --rm --platform "$platform" --network "$network" -v "$temporary:/cluster" "$image" \
			timeout 10 rados --conf /cluster/ceph.conf --name client.admin --keyring /cluster/admin.keyring --pool "$pool" put "$object" /cluster/readiness >/dev/null 2>&1; then
			ready=false
			break
		fi
	done
	if "$ready"; then break; fi
	test "$attempt" -lt 180 || exit 1
	sleep 1
done
sleep 3
for pool_object in p10-named:native-named p10-self:native-self p10-ec:ec-specialized; do
	pool=${pool_object%%:*}
	object=${pool_object#*:}
	docker run --rm --platform "$platform" --network "$network" -v "$temporary:/cluster" "$image" \
		timeout 10 rados --conf /cluster/ceph.conf --name client.admin --keyring /cluster/admin.keyring --pool "$pool" rm "$object"
done
ceph_cli auth get-or-create client.p10 mon 'allow rw' osd 'allow rwx pool=p10-named, allow rwx pool=p10-self, allow rwx pool=p10-ec' >/dev/null
ceph_cli auth get-key client.p10 >"$temporary/client.key"
ceph_cli auth get client.p10 -o /cluster/client.keyring >/dev/null
ceph_cli osd getcrushmap -o /cluster/crush.bin >/dev/null
ceph_cli osd map p10-ec ec-specialized --format json >"$temporary/ec-map.json"
ceph_cli fsid --format json >"$temporary/fsid.json"
ceph_cli osd stat --format json >"$temporary/osd-stat.json"
ceph_cli osd metadata --format json >"$temporary/osd-metadata.json"
ceph_cli osd pool get p10-named all --format json >"$temporary/named-pool.json"
ceph_cli osd pool get p10-self all --format json >"$temporary/self-pool.json"
ceph_cli osd pool get p10-ec all --format json >"$temporary/ec-pool.json"
ceph_cli osd erasure-code-profile get p10-ec-profile --format json >"$temporary/ec-profile.json"
ceph_cli osd crush rule dump p10-replicated-rule --format json >"$temporary/replicated-rule.json"
ceph_cli osd crush rule dump p10-ec-rule --format json >"$temporary/ec-rule.json"
ceph_cli auth get client.p10 --format json | jq '.[0] | {entity, mon_caps:.caps.mon, osd_caps:.caps.osd}' >"$temporary/client-auth.json"
jq -n \
	--slurpfile fsid "$temporary/fsid.json" --slurpfile osd_stat "$temporary/osd-stat.json" --slurpfile osd_metadata "$temporary/osd-metadata.json" \
	--slurpfile named "$temporary/named-pool.json" --slurpfile self "$temporary/self-pool.json" --slurpfile ec "$temporary/ec-pool.json" \
	--slurpfile profile "$temporary/ec-profile.json" --slurpfile replicated_rule "$temporary/replicated-rule.json" --slurpfile ec_rule "$temporary/ec-rule.json" \
	--slurpfile client "$temporary/client-auth.json" \
	'{fsid:$fsid[0].fsid,osds:$osd_stat[0].num_osds,objectstore:(if ($osd_metadata[0] | length) == $osd_stat[0].num_osds and all($osd_metadata[0][]; .osd_objectstore == "bluestore") then "bluestore" else "mixed" end),replicated_rule:$replicated_rule[0].rule_name,named_pool:{name:$named[0].pool,size:$named[0].size,min_size:$named[0].min_size,pg_num:$named[0].pg_num},self_managed_pool:{name:$self[0].pool,size:$self[0].size,min_size:$self[0].min_size,pg_num:$self[0].pg_num},ec_pool:{name:$ec[0].pool,size:$ec[0].size,min_size:$ec[0].min_size,pg_num:$ec[0].pg_num,profile:$ec[0].erasure_code_profile,rule:$ec_rule[0].rule_name,plugin:$profile[0].plugin,k:($profile[0].k|tonumber),m:($profile[0].m|tonumber),failure_domain:$profile[0]."crush-failure-domain",allow_ec_overwrites:$ec[0].allow_ec_overwrites,stripe_unit:($profile[0].stripe_unit|tonumber)},client:$client[0]}' >"$temporary/observed-cluster.json"

docker run --rm --platform "$platform" --network "$network" -v "$temporary:/cluster" "$image" \
	timeout 45 /cluster/native-driver seed /cluster/ceph.conf /cluster/client.keyring >"$temporary/native-seed.json"
docker run --rm --platform "$platform" --network "$network" -v "$temporary:/work" "$image" \
	timeout 150 /work/probe -monitors 172.30.110.10:3300 -key /work/client.key -fsid "$fsid" -coordination-dir /work >"$temporary/probe.json"
docker run --rm --platform "$platform" --network "$network" -v "$temporary:/cluster" "$image" \
	timeout 45 /cluster/native-driver verify /cluster/ceph.conf /cluster/client.keyring >"$temporary/native-verify.json"

jq -e '.named_create_list_lookup and .named_read_snapshot and .named_rollback and .named_remove and .self_managed_create and .self_managed_write_context and .self_managed_read and .self_managed_rollback and .self_managed_remove and .write_same and .checksum and (.checksum_hex | test("^[0-9a-f]+$")) and .allocation_hint and .sparse_read and .copy_from and .copy_from2 and .replicated_capabilities and .ec_capabilities and .ec_write_read and .ec_overwrite_rejected and .ec_write_same_rejected and .ec_checksum and .ec_allocation_hint and .ec_sparse_read and .ec_copy_from and .ec_omap_rejected and .ec_alignment_evidence and (.required_alignment > 0)' "$temporary/probe.json" >/dev/null
jq -e '.named_create_list_lookup_name_stamp and .named_read_rollback_remove and .self_managed_create_write_read_rollback_remove' "$temporary/native-seed.json" >/dev/null
jq -e '.go_named_snapshot and .go_named_head and .go_copy and .go_copy_from2 and .go_ec_copy and .go_checksum_hex == "02000000f5be862af5be862a"' "$temporary/native-verify.json" >/dev/null

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
artifact_paths=$(find internal -type f -name '*.go' -print; find testdata/p10 -type f -print; find . -maxdepth 1 -type f -name '*.go' -print | sed 's#^./##'; printf '%s\n' Makefile SPEC.md go.mod go.sum tools/api-inventory/main.go tools/api-inventory/main_test.go integration/p10/native_driver.c integration/p10/probe/main.go integration/p10/report.schema.json integration/p10/reproduce.sh tools/p10-verify/main.go tools/p10-verify/main_test.go docs/p00/api-inventory.csv docs/p01/public-api.md docs/p10/tasks.md docs/p10/provenance.md)
for artifact in $artifact_paths; do
	test -f "$artifact"
	hash=$(shasum -a 256 "$artifact" | awk '{print $1}')
	jq --arg path "$artifact" --arg hash "$hash" '. + {($path):$hash}' "$artifacts" >"$artifacts.next"
	mv "$artifacts.next" "$artifacts"
done
mkdir -p docs/p10
jq -n \
	--arg started_at "$started_at" --arg finished_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
	--arg image "$image" --arg platform "$platform" --arg ceph_version "$(cat "$temporary/ceph.version")" \
	--arg ceph_mon_sha256 "$(cat "$temporary/ceph-mon.sha256")" --arg ceph_osd_sha256 "$(cat "$temporary/ceph-osd.sha256")" \
	--arg librados_path "$(cat "$temporary/librados.path")" --arg librados_sha256 "$(cat "$temporary/librados.sha256")" --arg librados_package "$(cat "$temporary/librados.package")" \
	--argjson observed_cluster "$(cat "$temporary/observed-cluster.json")" --argjson artifacts "$(cat "$artifacts")" \
	--argjson probe "$(cat "$temporary/probe.json")" --argjson native_seed "$(cat "$temporary/native-seed.json")" --argjson native_verify "$(cat "$temporary/native-verify.json")" \
	'{schema_version:1,status:"passed",command:"make integration-p10",started_at:$started_at,finished_at:$finished_at,source:{repository:"https://github.com/otuschhoff/go-librados.git",identity:"content-addressed-artifacts",artifacts:$artifacts},server:{repository:"https://github.com/ceph/ceph.git",source_anchor_commit:"7f793731f1b39eb4f465e960113d2363c311b964",version:$ceph_version,image:$image,platform:$platform,binaries:{mon_sha256:$ceph_mon_sha256,osd_sha256:$ceph_osd_sha256}},native_runtime:{soname:"librados.so.2",path:$librados_path,package:$librados_package,sha256:$librados_sha256},cluster:($observed_cluster + {network:"172.30.110.0/24",osd_device_bytes:8589934592,ec_pool:($observed_cluster.ec_pool + {stripe_width:$probe.required_alignment})}),scenarios:{named_snapshots:"passed",self_managed_snapshots:"passed",specialized_io:"passed",erasure_coded_io:"passed",native_interoperability:"passed"},probe:$probe,native:{seed:$native_seed,verify:$native_verify}}' >"$report"
mv "$report" docs/p10/integration-report.json
printf '%s\n' 'P10 live snapshot and specialized-I/O qualification passed'
