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
implementation_files=$(
	{
		find internal/cephx internal/crush internal/encoding internal/maps internal/mon internal/msgr internal/objecter internal/osd internal/protocol -type f -name '*.go'
		printf '%s\n' Makefile README.md SPEC.md client.go client_test.go doc.go errors.go errors_test.go object.go integration/README.md integration/p06/probe/main.go integration/p06/report.schema.json integration/p06/reproduce.sh tools/p06-verify/main.go docs/p06/tasks.md docs/p06/provenance.md go.mod go.sum
	} | LC_ALL=C sort
)
hash_implementation() {
	for artifact in $implementation_files; do
		printf '%s\t%s\n' "$(shasum -a 256 "$artifact" | awk '{print $1}')" "$artifact"
	done | jq -Rn '[inputs | split("\t") | {(.[1]): .[0]}] | add'
}
artifacts=$(hash_implementation)
server_version=$(docker run --rm --platform "$platform" "$image" ceph --version)
server_binary_sha256=$(docker run --rm --platform "$platform" "$image" sh -c 'sha256sum "$(command -v ceph-osd)"' | awk '{print $1}')
temporary=$(mktemp -d)
network="rados-go-p06-$$"
fsid=11111111-2222-4333-8444-666666666666
cleanup() {
	docker rm -f "p06-probe-$$" "p06-mon-$$" "p06-osd-0-$$" "p06-osd-1-$$" "p06-osd-2-$$" >/dev/null 2>&1 || true
	docker volume rm "rados-go-p06-osd-0-$$" "rados-go-p06-osd-1-$$" "rados-go-p06-osd-2-$$" >/dev/null 2>&1 || true
	docker network rm "$network" >/dev/null 2>&1 || true
	rm -rf "$temporary"
}
trap cleanup EXIT HUP INT TERM

CGO_ENABLED=0 GOOS=linux GOARCH="$goarch" go build -trimpath -o "$temporary/probe" ./integration/p06/probe
python3 -c 'import sys; sys.stdout.buffer.write(bytes(range(256))*16)' >"$temporary/data.bin"
: >"$temporary/empty"
printf namespace-value >"$temporary/namespace"
printf locator-value >"$temporary/locator"
docker network create --subnet 172.30.96.0/24 "$network" >/dev/null
docker run --rm --user 0 --platform "$platform" -v "$temporary:/cluster" "$image" sh -c '
	set -eu
	cat >/cluster/ceph.conf <<EOF
[global]
fsid = '"$fsid"'
mon host = v2:172.30.96.10:3300
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
	monmaptool --create --fsid '"$fsid"' --addv a "[v2:172.30.96.10:3300/0]" /cluster/monmap
	mkdir -p /cluster/mondata
	ceph-mon --mkfs -i a --fsid '"$fsid"' --monmap /cluster/monmap --keyring /cluster/mon.keyring --mon-data /cluster/mondata
	chown -R ceph:ceph /cluster/mondata
'
docker run -d --rm --name "p06-mon-$$" --platform "$platform" --network "$network" --ip 172.30.96.10 -v "$temporary:/cluster" "$image" \
	ceph-mon -f -i a --mon-data /cluster/mondata --public-addr v2:172.30.96.10:3300 --setuser ceph --setgroup ceph --mon-data-avail-crit 1 --no-mon-cluster-log-to-stderr >/dev/null

ceph_cli() {
	docker run --rm --platform "$platform" --network "$network" -v "$temporary:/cluster" "$image" \
		ceph --conf /cluster/ceph.conf --name client.admin --keyring /cluster/admin.keyring "$@"
}
for attempt in $(seq 1 30); do
	if ceph_cli status --format json 2>/dev/null | jq -e '.health.status != null' >/dev/null; then break; fi
	test "$attempt" -lt 30 || { docker logs "p06-mon-$$" >&2; exit 1; }
	sleep 1
done

for id in 0 1 2; do
	uuid="00000000-0000-4000-8000-00000000000$id"
	volume="rados-go-p06-osd-$id-$$"
	docker volume create "$volume" >/dev/null
	ceph_cli osd create "$uuid" "$id" >/dev/null
	ceph_cli auth get-or-create "osd.$id" mon 'allow profile osd' mgr 'allow profile osd' osd 'allow *' -o "/cluster/osd-$id.keyring"
	ceph_cli mon getmap -o "/cluster/osd-$id.monmap" >/dev/null
	docker run --rm --user 0 --privileged --platform "$platform" -v "$temporary:/cluster" -v "$volume:/osd" "$image" sh -c '
		set -eu
		id='"$id"'; uuid='"$uuid"'
		mkdir -p /osd/data
		truncate -s 1G /osd/block
		cp /cluster/osd-$id.keyring /osd/data/keyring
		cp /cluster/osd-$id.monmap /osd/data/activate.monmap
		chown -R ceph:ceph /osd
		ceph-osd --mkfs -i "$id" --osd-data /osd/data --osd-uuid "$uuid" --osd-objectstore bluestore --bluestore-block-path /osd/block --monmap /osd/data/activate.monmap --keyring /osd/data/keyring --setuser ceph --setgroup ceph
	'
	ip="172.30.96.$((20 + id))"
	docker run -d --privileged --name "p06-osd-$id-$$" --platform "$platform" --network "$network" --ip "$ip" -v "$temporary:/cluster" -v "$volume:/osd" "$image" \
		ceph-osd -f --conf /cluster/ceph.conf -i "$id" --osd-data /osd/data --osd-objectstore bluestore --public-addr "v2:$ip:6800" --cluster-addr "v2:$ip:6802" --setuser ceph --setgroup ceph >/dev/null
done
for attempt in $(seq 1 60); do
	if ceph_cli osd stat --format json 2>/dev/null | jq -e '.num_osds == 3 and .num_up_osds == 3 and .num_in_osds == 3' >/dev/null; then break; fi
	test "$attempt" -lt 60 || { ceph_cli osd tree >&2; docker logs "p06-osd-0-$$" >&2 || true; exit 1; }
	sleep 1
done

ceph_cli osd crush rule create-replicated p06-rule default osd >/dev/null
ceph_cli osd pool create p06-data 16 16 replicated p06-rule >/dev/null
ceph_cli osd pool set p06-data size 2 >/dev/null
ceph_cli osd pool set p06-data min_size 1 >/dev/null
ceph_cli auth get-or-create client.p06 mon 'allow r' osd 'allow r pool=p06-data' >/dev/null
ceph_cli auth get-key client.p06 >"$temporary/client.key"

rados_cli() {
	docker run --rm --platform "$platform" --network "$network" -v "$temporary:/cluster" "$image" \
		rados --conf /cluster/ceph.conf --name client.admin --keyring /cluster/admin.keyring -p p06-data "$@"
}
rados_cli put binary /cluster/data.bin
rados_cli put empty /cluster/empty
rados_cli -N space put namespaced /cluster/namespace
rados_cli --object-locator routing-key put located /cluster/locator
printf '%s\n' 'P06 native fixtures written'

probe_id=$(docker run -d --name "p06-probe-$$" --platform "$platform" --network "$network" --ip 172.30.96.40 -v "$temporary:/work" "$image" \
	/work/probe -monitors 172.30.96.10:3300 -key /work/client.key -fsid "$fsid" -data /work/data.bin -control /work)
printf 'P06 Go probe launched: %s\n' "$probe_id"
for attempt in $(seq 1 120); do
	if test -e "$temporary/ready"; then break; fi
	probe_running=$(docker inspect -f '{{.State.Running}}' "p06-probe-$$" 2>/dev/null || printf false)
	test "$probe_running" = true || { docker inspect -f 'probe exit={{.State.ExitCode}} error={{.State.Error}}' "p06-probe-$$" >&2; docker logs "p06-probe-$$" >&2; for id in 0 1 2; do docker logs "p06-osd-$id-$$" >&2; done; exit 1; }
	test "$attempt" -lt 120 || { docker logs "p06-probe-$$" >&2; for id in 0 1 2; do docker logs "p06-osd-$id-$$" >&2; done; exit 1; }
	sleep 1
done
printf '%s\n' 'P06 Go probe reached initial read gate'
primary=$(ceph_cli osd map p06-data binary --format json | jq -r '.acting_primary')
docker rm -f "p06-osd-$primary-$$" >/dev/null
ceph_cli osd down "$primary" >/dev/null
for attempt in $(seq 1 60); do
	new_primary=$(ceph_cli osd map p06-data binary --format json | jq -r '.acting_primary')
	if test "$new_primary" != "$primary"; then break; fi
	test "$attempt" -lt 60 || exit 1
	sleep 1
done
touch "$temporary/remapped"
exit_code=$(docker wait "p06-probe-$$")
test "$exit_code" = 0 || { docker logs "p06-probe-$$" >&2; exit 1; }
docker logs "p06-probe-$$" >"$temporary/probe.json"
jq -e '.ranged_read and .full_read and .empty_read and .namespace_read and .locator_read and .stat and .missing and .primary_change and .version > 0' "$temporary/probe.json" >/dev/null
finished_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
jq -n \
	--arg started_at "$started_at" \
	--arg finished_at "$finished_at" \
	--arg image "$image" \
	--arg platform "$platform" \
	--arg server_version "$server_version" \
	--arg server_binary_sha256 "$server_binary_sha256" \
	--argjson artifacts "$artifacts" \
	--argjson probe "$(cat "$temporary/probe.json")" \
	'{schema_version:1,status:"passed",command:"make integration-p06",started_at:$started_at,finished_at:$finished_at,source:{repository:"https://github.com/otuschhoff/rados-go.git",identity:"content-addressed-artifacts",artifacts:$artifacts},server:{repository:"https://github.com/ceph/ceph.git",source_anchor_commit:"7f793731f1b39eb4f465e960113d2363c311b964",version:$server_version,image:$image,platform:$platform,binary_sha256:$server_binary_sha256},cluster:{fsid:"11111111-2222-4333-8444-666666666666",osds:3,pool:"p06-data",replicas:2},scenarios:{native_contents:"passed",ranged_read:"passed",empty_read:"passed",namespace_read:"passed",locator_read:"passed",stat_metadata:"passed",missing_object:"passed",operation_version:"passed",primary_change:"passed"},probe:$probe}' >"$temporary/report.json"
mkdir -p docs/p06
cp "$temporary/report.json" docs/p06/integration-report.json
printf 'P06 integration report: %s\n' "$root/docs/p06/integration-report.json"