#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
cd "$root"
image=$(jq -r '.images.qualification.reference' docs/p00/evidence.json)
compiler=$(jq -r '.oracle.compiler' docs/p00/evidence.json)
ceph_devel=$(jq -r '.oracle.ceph_devel' docs/p00/evidence.json)
cephpp_devel=$(jq -r '.oracle.cephpp_devel' docs/p00/evidence.json)
repository=$(jq -r '.oracle.devel_repository' docs/p00/evidence.json)
temporary=$(mktemp -d)
trap 'rm -rf "$temporary"' EXIT HUP INT TERM

docker build \
  --build-arg "CEPH_IMAGE=$image" \
  --build-arg "COMPILER_PACKAGE=$compiler" \
  --build-arg "CEPH_DEVEL_PACKAGE=$ceph_devel" \
  --build-arg "CEPHPP_DEVEL_PACKAGE=$cephpp_devel" \
  --build-arg "CEPH_DEVEL_REPOSITORY=$repository" \
  --build-arg "CEPH_DEVEL_AMD64_SHA256=$(jq -r '.oracle.ceph_devel_amd64_sha256' docs/p00/evidence.json)" \
  --build-arg "CEPH_DEVEL_ARM64_SHA256=$(jq -r '.oracle.ceph_devel_arm64_sha256' docs/p00/evidence.json)" \
  --build-arg "CEPHPP_DEVEL_AMD64_SHA256=$(jq -r '.oracle.cephpp_devel_amd64_sha256' docs/p00/evidence.json)" \
  --build-arg "CEPHPP_DEVEL_ARM64_SHA256=$(jq -r '.oracle.cephpp_devel_arm64_sha256' docs/p00/evidence.json)" \
  -t rados-go-p02-oracle integration/p02
docker run --rm -v "$temporary:/vectors" rados-go-p02-oracle /vectors

for fixture in banner-rev1.bin crc-one-segment.bin crc-four-segment.bin secure-one-segment.bin secure-multi-record.bin; do
  cmp "testdata/p02/$fixture" "$temporary/$fixture"
done
test "$(find "$temporary" -type f | wc -l | tr -d ' ')" -eq 5
printf '%s\n' 'P02 fixture reproduction passed'