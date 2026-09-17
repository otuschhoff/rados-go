#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
cd "$root"
repository=$(jq -r '.ceph.repository' docs/p00/evidence.json)
baseline_commit=$(jq -r '.ceph.source_baseline.commit' docs/p00/evidence.json)
qualification_commit=$(jq -r '.ceph.qualification_release.commit' docs/p00/evidence.json)
case "$(docker info --format '{{.Architecture}}')" in
  x86_64|amd64) host_platform=linux/amd64 ;;
  aarch64|arm64) host_platform=linux/arm64 ;;
  *) printf 'unsupported native Docker architecture\n' >&2; exit 2 ;;
esac
platform=${P02_PLATFORM:-$host_platform}
architecture=${platform#linux/}
case "$platform" in
  linux/amd64|linux/arm64) ;;
  *) printf 'unsupported P02 platform: %s\n' "$platform" >&2; exit 2 ;;
esac
if test "${CI:-}" = true && test "$platform" != "$host_platform"; then
  printf 'CI platform %s is not native on Docker host %s\n' "$platform" "$host_platform" >&2
  exit 2
fi
image_index=$(jq -r '.images.qualification.reference' docs/p00/evidence.json)
image_digest=$(jq -r --arg architecture "$architecture" '.images.qualification[$architecture]' docs/p00/evidence.json)
image="${image_index%@*}@$image_digest"
expected_library=$(jq -r --arg platform "$platform" '.upstream_oracle.libraries[$platform]' docs/p02/evidence.json)
temporary=$(mktemp -d)
trap 'rm -rf "$temporary"' EXIT HUP INT TERM

mkdir "$temporary/ceph-repository"
git -C "$temporary/ceph-repository" init
git -C "$temporary/ceph-repository" remote add origin "$repository"
git -C "$temporary/ceph-repository" fetch --depth=1 origin "$baseline_commit"
git -C "$temporary/ceph-repository" fetch --depth=1 origin "$qualification_commit"
git -C "$temporary/ceph-repository" worktree add --detach "$temporary/ceph-deps" "$baseline_commit"
git -C "$temporary/ceph-repository" worktree add --detach "$temporary/ceph-source" "$qualification_commit"

submodules='src/BLAKE3 src/crypto/isa-l/isa-l_crypto src/fmt src/erasure-code/jerasure/gf-complete src/erasure-code/jerasure/jerasure src/isa-l src/rocksdb src/xxHash src/zstd'
git -C "$temporary/ceph-deps" submodule update --init --depth 1 -- $submodules
for submodule in $submodules; do
  baseline_tree=$(git -C "$temporary/ceph-deps" ls-tree "$baseline_commit" "$submodule")
  qualification_tree=$(git -C "$temporary/ceph-source" ls-tree "$qualification_commit" "$submodule")
  test "$baseline_tree" = "$qualification_tree"
done

actual_library=$(docker run --rm --platform "$platform" "$image" sh -c "sha256sum \$(readlink -f /usr/lib64/ceph/libceph-common.so.2)" | awk '{print $1}')
test "$actual_library" = "$expected_library"
test "$(docker run --rm --platform "$platform" "$image" rpm -q --qf '%{NAME}-%{VERSION}-%{RELEASE}' ceph-common)" = "ceph-common-20.2.4-0.el9"

docker buildx build --load --platform "$platform" \
  --build-context "ceph-deps-source=$temporary/ceph-deps" \
  --build-context "ceph-source=$temporary/ceph-source" \
  --build-arg "CEPH_IMAGE=$image" \
  -f integration/p02/Dockerfile.upstream \
  -t rados-go-p02-upstream integration/p02
mkdir "$temporary/vectors"
docker run --rm --platform "$platform" -v "$temporary/vectors:/vectors" \
  rados-go-p02-upstream /vectors

for fixture in \
  upstream-ack-control.bin \
  upstream-crc-disabled-one-segment.bin \
  upstream-crc-four-segment.bin \
  upstream-crc-one-segment.bin \
  upstream-message-frame.bin \
  upstream-secure-multi-record.bin \
  upstream-secure-one-segment.bin
do
  cmp "testdata/p02/upstream/$fixture" "$temporary/vectors/$fixture"
done
test "$(find "$temporary/vectors" -type f | wc -l | tr -d ' ')" -eq 7

cmp testdata/p02/crc-one-segment.bin "$temporary/vectors/upstream-crc-one-segment.bin"
cmp testdata/p02/crc-four-segment.bin "$temporary/vectors/upstream-crc-four-segment.bin"
cmp testdata/p02/secure-one-segment.bin "$temporary/vectors/upstream-secure-one-segment.bin"
cmp testdata/p02/secure-multi-record.bin "$temporary/vectors/upstream-secure-multi-record.bin"
printf '%s\n' 'P02 upstream FrameAssembler reproduction passed'