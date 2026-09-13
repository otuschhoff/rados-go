#!/bin/sh
set -eu

GUARD_VALUE=I_UNDERSTAND_THIS_DESTROYS_DATA
STATE_DIR=/var/lib/go-librados-p00
REPORT_DIR=${P00_REPORT_DIR:-integration/reports}
POOL=p00-replicated
OBJECT=p00-smoke-object
STARTED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)
FSID=""
LOOPS=""
ORACLE_IMAGE=go-librados-p00-oracle
ORACLE_IMAGE_BUILT=false

if [ "${P00_DISPOSABLE_CLUSTER:-}" != "$GUARD_VALUE" ]; then
  echo "refusing destructive run: set P00_DISPOSABLE_CLUSTER=$GUARD_VALUE" >&2
  exit 2
fi

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
cd "$REPO_ROOT"
"$SCRIPT_DIR/preflight.sh" --require-linux-host
if [ -n "$(git status --porcelain --untracked-files=all)" ]; then
  echo "refusing to run: commit all changes so repository_commit identifies the tested tree" >&2
  exit 2
fi
EVIDENCE=docs/p00/evidence.json
CEPH_IMAGE=$(jq -r '.images.qualification.reference' "$EVIDENCE")
BASELINE_COMMIT=$(jq -r '.ceph.source_baseline.commit' "$EVIDENCE")
QUALIFICATION_COMMIT=$(jq -r '.ceph.qualification_release.commit' "$EVIDENCE")
ORACLE_COMPILER=$(jq -r '.oracle.compiler' "$EVIDENCE")
ORACLE_CEPH_DEVEL=$(jq -r '.oracle.ceph_devel' "$EVIDENCE")
ORACLE_CEPHPP_DEVEL=$(jq -r '.oracle.cephpp_devel' "$EVIDENCE")
ORACLE_DEVEL_REPOSITORY=$(jq -r '.oracle.devel_repository' "$EVIDENCE")
ORACLE_CEPH_DEVEL_AMD64_SHA256=$(jq -r '.oracle.ceph_devel_amd64_sha256' "$EVIDENCE")
ORACLE_CEPH_DEVEL_ARM64_SHA256=$(jq -r '.oracle.ceph_devel_arm64_sha256' "$EVIDENCE")
ORACLE_CEPHPP_DEVEL_AMD64_SHA256=$(jq -r '.oracle.cephpp_devel_amd64_sha256' "$EVIDENCE")
ORACLE_CEPHPP_DEVEL_ARM64_SHA256=$(jq -r '.oracle.cephpp_devel_arm64_sha256' "$EVIDENCE")
ORACLE_LIBRADOS=$(jq -r '.oracle.librados' "$EVIDENCE")
GO_MINIMUM=$(jq -r '.go.minimum.version' "$EVIDENCE")
GO_LATEST=$(jq -r '.go.latest.version' "$EVIDENCE")

if [ -e /var/lib/ceph/ceph ] || find /var/lib/ceph -mindepth 1 -maxdepth 1 -type d 2>/dev/null | grep -q .; then
  echo "refusing to run: /var/lib/ceph is not empty; use a fresh disposable VM" >&2
  exit 2
fi

cleanup() {
  exit_code=$?
  set +e
  if [ -n "$FSID" ] && [ -x "$STATE_DIR/cephadm" ]; then
    if ! "$STATE_DIR/cephadm" --image "$CEPH_IMAGE" rm-cluster --force --zap-osds --fsid "$FSID" >/dev/null 2>&1; then
      "$STATE_DIR/cephadm" --image "$CEPH_IMAGE" rm-cluster --force --fsid "$FSID" >/dev/null 2>&1
    fi
  fi
  for loop_device in $LOOPS; do
    losetup -d "$loop_device" >/dev/null 2>&1
  done
  if [ "$ORACLE_IMAGE_BUILT" = true ]; then
    docker image rm "$ORACLE_IMAGE" >/dev/null 2>&1
  fi
  rm -rf "$STATE_DIR"
  exit "$exit_code"
}
trap cleanup EXIT INT TERM

rm -rf "$STATE_DIR"
mkdir -p "$STATE_DIR/client" "$STATE_DIR/osds" "$REPORT_DIR"
FSID=$(cat /proc/sys/kernel/random/uuid)
MON_IP=${P00_MON_IP:-$(ip -4 route get 1.1.1.1 | awk '{for (i=1;i<=NF;i++) if ($i=="src") {print $(i+1); exit}}')}
if [ -z "$MON_IP" ]; then
  echo "unable to select monitor IPv4 address; set P00_MON_IP" >&2
  exit 2
fi

container_id=$(docker create "$CEPH_IMAGE")
docker cp "$container_id:/usr/sbin/cephadm" "$STATE_DIR/cephadm"
docker rm "$container_id" >/dev/null
chmod 0755 "$STATE_DIR/cephadm"

"$STATE_DIR/cephadm" --image "$CEPH_IMAGE" bootstrap \
  --fsid "$FSID" --mon-ip "$MON_IP" --skip-dashboard --skip-monitoring-stack \
  --single-host-defaults --output-dir "$STATE_DIR/client"

for index in 0 1 2; do
  image_file="$STATE_DIR/osds/osd-$index.img"
  truncate -s 6G "$image_file"
  loop_device=$(losetup --find --show "$image_file")
  LOOPS="$LOOPS $loop_device"
done

loop_paths=$(printf '%s\n' $LOOPS | jq -R . | jq -s .)
jq -n --arg host "$(hostname -s)" --argjson paths "$loop_paths" \
  '{service_type:"osd",service_id:"p00-raw",placement:{host_pattern:$host},method:"raw",data_devices:{paths:($paths | map({path:.}))}}' \
  > "$STATE_DIR/osd-spec.json"
"$STATE_DIR/cephadm" --image "$CEPH_IMAGE" shell --fsid "$FSID" -- ceph orch apply -i - < "$STATE_DIR/osd-spec.json"

ceph_shell() {
  "$STATE_DIR/cephadm" --image "$CEPH_IMAGE" shell --fsid "$FSID" -- ceph "$@"
}

osds_ready=false
for attempt in $(seq 1 60); do
  if ceph_shell osd stat --format json | jq -e '.num_osds == 3 and .num_up_osds == 3 and .num_in_osds == 3' >/dev/null; then
    osds_ready=true
    break
  fi
  sleep 5
done
if [ "$osds_ready" != true ]; then
  echo "timed out waiting for three OSDs to become up and in" >&2
  exit 1
fi

ceph_shell osd crush rule create-replicated p00-replicated-rule default osd
ceph_shell osd pool create "$POOL" 32 32 replicated p00-replicated-rule
ceph_shell osd pool set "$POOL" size 3
ceph_shell osd pool set "$POOL" min_size 2
ceph_shell osd erasure-code-profile set p00-ec k=2 m=1 plugin=jerasure technique=reed_sol_van crush-failure-domain=osd
ceph_shell osd pool create p00-ec 32 32 erasure p00-ec
ceph_shell osd pool set p00-ec allow_ec_overwrites true
ceph_shell auth get-or-create client.p00-rw mon 'profile rbd' osd "allow rw pool=$POOL" > "$STATE_DIR/client/ceph.client.p00-rw.keyring"

docker build --build-arg "CEPH_IMAGE=$CEPH_IMAGE" \
  --build-arg "COMPILER_PACKAGE=$ORACLE_COMPILER" \
  --build-arg "CEPH_DEVEL_PACKAGE=$ORACLE_CEPH_DEVEL" \
  --build-arg "CEPHPP_DEVEL_PACKAGE=$ORACLE_CEPHPP_DEVEL" \
  --build-arg "CEPH_DEVEL_REPOSITORY=$ORACLE_DEVEL_REPOSITORY" \
  --build-arg "CEPH_DEVEL_AMD64_SHA256=$ORACLE_CEPH_DEVEL_AMD64_SHA256" \
  --build-arg "CEPH_DEVEL_ARM64_SHA256=$ORACLE_CEPH_DEVEL_ARM64_SHA256" \
  --build-arg "CEPHPP_DEVEL_AMD64_SHA256=$ORACLE_CEPHPP_DEVEL_AMD64_SHA256" \
  --build-arg "CEPHPP_DEVEL_ARM64_SHA256=$ORACLE_CEPHPP_DEVEL_ARM64_SHA256" \
  --build-arg "LIBRADOS_PACKAGE=$ORACLE_LIBRADOS" \
  -t "$ORACLE_IMAGE" "$SCRIPT_DIR/oracle"
ORACLE_IMAGE_BUILT=true
ORACLE_IMAGE_ID=$(docker image inspect "$ORACLE_IMAGE" --format '{{.Id}}')
ORACLE_SOURCE_SHA256=$(sha256sum "$SCRIPT_DIR/oracle/main.cc" | awk '{print $1}')
ORACLE_DOCKERFILE_SHA256=$(sha256sum "$SCRIPT_DIR/oracle/Dockerfile" | awk '{print $1}')
if ! CRUD_JSON=$(docker run --rm --network host \
  -v "$STATE_DIR/client:/etc/ceph:ro" "$ORACLE_IMAGE" \
  smoke client.p00-rw /etc/ceph/ceph.conf "$POOL" "$OBJECT" 2>"$STATE_DIR/oracle.stderr"); then
  cat "$STATE_DIR/oracle.stderr" >&2
  printf '%s\n' "$CRUD_JSON" >&2
  exit 1
fi
printf '%s\n' "$CRUD_JSON" | jq -e '.status == "passed"' >/dev/null

ceph_shell osd map "$POOL" "$OBJECT" --format json > "$STATE_DIR/object-map.json"
if ! jq -e --arg pool "$POOL" --arg object "$OBJECT" \
  '.pool == $pool and .object == $object and .pgid and (.up | length > 0) and (.acting | length > 0) and (.acting_primary >= 0)' \
  "$STATE_DIR/object-map.json" >/dev/null; then
  echo "invalid object mapping evidence:" >&2
  cat "$STATE_DIR/object-map.json" >&2
  exit 1
fi
SERVER_VERSION=$("$STATE_DIR/cephadm" --image "$CEPH_IMAGE" shell --fsid "$FSID" -- ceph --version)
FINISHED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)
REPORT="$REPORT_DIR/p00-$FSID.json"

jq -n \
  --arg started "$STARTED_AT" --arg finished "$FINISHED_AT" \
  --arg baseline "$BASELINE_COMMIT" --arg qualification "$QUALIFICATION_COMMIT" \
  --arg repository "$(git rev-parse HEAD)" --arg image "$CEPH_IMAGE" \
  --arg version "$SERVER_VERSION" --arg os "$(uname -s)" --arg arch "$(uname -m)" \
  --arg oracle_image "$ORACLE_IMAGE_ID" --arg oracle_source "$ORACLE_SOURCE_SHA256" \
  --arg oracle_dockerfile "$ORACLE_DOCKERFILE_SHA256" --arg oracle_compiler "$ORACLE_COMPILER" \
  --arg oracle_ceph_devel "$ORACLE_CEPH_DEVEL" --arg oracle_cephpp_devel "$ORACLE_CEPHPP_DEVEL" \
  --arg oracle_devel_repository "$ORACLE_DEVEL_REPOSITORY" \
  --arg oracle_ceph_devel_amd64_sha256 "$ORACLE_CEPH_DEVEL_AMD64_SHA256" \
  --arg oracle_ceph_devel_arm64_sha256 "$ORACLE_CEPH_DEVEL_ARM64_SHA256" \
  --arg oracle_cephpp_devel_amd64_sha256 "$ORACLE_CEPHPP_DEVEL_AMD64_SHA256" \
  --arg oracle_cephpp_devel_arm64_sha256 "$ORACLE_CEPHPP_DEVEL_ARM64_SHA256" \
  --arg oracle_librados "$ORACLE_LIBRADOS" \
  --arg go_minimum "$GO_MINIMUM" --arg go_latest "$GO_LATEST" \
  --arg fsid "$FSID" --argjson crud "$CRUD_JSON" \
  --slurpfile mapping "$STATE_DIR/object-map.json" \
  '{schema_version:1,status:"passed",started_at:$started,finished_at:$finished,
    source:{baseline_commit:$baseline,qualification_commit:$qualification,repository_commit:$repository},
    server:{image:$image,version:$version},
    oracle:{image_id:$oracle_image,source_sha256:$oracle_source,dockerfile_sha256:$oracle_dockerfile,
      compiler:$oracle_compiler,ceph_devel:$oracle_ceph_devel,cephpp_devel:$oracle_cephpp_devel,
      devel_repository:$oracle_devel_repository,ceph_devel_amd64_sha256:$oracle_ceph_devel_amd64_sha256,
      ceph_devel_arm64_sha256:$oracle_ceph_devel_arm64_sha256,
      cephpp_devel_amd64_sha256:$oracle_cephpp_devel_amd64_sha256,
      cephpp_devel_arm64_sha256:$oracle_cephpp_devel_arm64_sha256,librados:$oracle_librados},
    client:{os:$os,architecture:$arch,go_versions:[$go_minimum,$go_latest]},
    cluster:{fsid:$fsid,replicated_profile:"size=3,min_size=2,pg=32,rule=p00-replicated-rule,failure_domain=osd",ec_profile:"k=2,m=1,jerasure,reed_sol_van,pg=32,failure_domain=osd,overwrite=true"},
    tests:{native_crud:{status:"passed",evidence:$crud},object_mapping:{status:"passed",evidence:$mapping[0]}}}' > "$REPORT"

GO111MODULE=off go run ./tools/p00-verify -report "$REPORT"
echo "P00 smoke passed: $REPORT"