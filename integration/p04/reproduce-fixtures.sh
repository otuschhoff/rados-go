#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
cd "$root"
case "$(docker info --format '{{.Architecture}}')" in
	x86_64|amd64) platform=linux/amd64; architecture=amd64 ;;
	aarch64|arm64) platform=linux/arm64; architecture=arm64 ;;
	*) printf '%s\n' 'unsupported Docker architecture' >&2; exit 2 ;;
esac
image_index=$(jq -r '.images.qualification.reference' docs/p00/evidence.json)
image_digest=$(jq -r --arg architecture "$architecture" '.images.qualification[$architecture]' docs/p00/evidence.json)
image="${image_index%@*}@$image_digest"
temporary=$(mktemp -d)
trap 'rm -rf "$temporary"' EXIT HUP INT TERM

docker run --rm --platform "$platform" -v "$temporary:/out" "$image" sh -c '
	set -eu
	test "$(ceph --version | awk '\''{print $3}'\'')" = 20.2.4
	ceph-dencoder type MonMap select_test 0 encode export /out/monmap-v9.bin
	ceph-dencoder type OSDMap select_test 0 encode export /out/osdmap-v8.bin
	ceph-dencoder type "OSDMap::Incremental" select_test 0 encode export /out/osdmap-incremental-v8.bin
'
for fixture in monmap-v9.bin osdmap-v8.bin osdmap-incremental-v8.bin; do
	cmp "testdata/p04/$fixture" "$temporary/$fixture"
	expected=$(jq -r .sha256 "testdata/p04/$fixture.manifest.json")
	actual=$(shasum -a 256 "$temporary/$fixture" | awk '{print $1}')
	test "$actual" = "$expected"
done
for source_path in src/mon/MonMap.h src/mon/MonMap.cc src/osd/OSDMap.h src/osd/OSDMap.cc; do
	expected=$(jq -r --arg path "$source_path" '.source.files[$path] // empty' testdata/p04/*.manifest.json | sort -u)
	test -n "$expected"
	actual=$(curl -fsSL "https://raw.githubusercontent.com/ceph/ceph/7f793731f1b39eb4f465e960113d2363c311b964/$source_path" | shasum -a 256 | awk '{print $1}')
	test "$actual" = "$expected"
done
printf '%s\n' 'P04 fixture reproduction passed'
