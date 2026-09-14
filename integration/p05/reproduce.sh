#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
cd "$root"
platform=linux/amd64
image_repository=$(jq -r '.images.qualification.reference' docs/p00/evidence.json)
image="${image_repository%@*}@$(jq -r '.images.qualification.amd64' docs/p00/evidence.json)"
temporary=$(mktemp -d)
trap 'rm -rf "$temporary"' EXIT HUP INT TERM

docker run --rm --platform "$platform" -v "$root/integration/p05:/input:ro" -v "$temporary:/output" "$image" sh -c '
	set -eu
	test "$(ceph --version | awk '\''{print $3}'\'')" = 20.2.4
	crushtool -c /input/crushmap.txt -o /output/crushmap.bin \
		--set-choose-local-tries 0 --set-choose-local-fallback-tries 0 \
		--set-choose-total-tries 50 --set-chooseleaf-descend-once 1 \
		--set-chooseleaf-vary-r 1 --set-chooseleaf-stable 1
	crushtool -i /output/crushmap.bin --test --show-mappings --rule 0 \
		--num-rep 3 --min-x 0 --max-x 255 > /output/mappings.txt 2>&1
	crushtool -i /output/crushmap.bin --test --show-mappings --rule 0 \
		--num-rep 3 --min-x 0 --max-x 255 --weight 1 0 > /output/mappings-osd1-out.txt 2>&1
	osdmaptool --createsimple 4 --with-default-pool --clobber /output/osdmap.bin >/dev/null
	osdmaptool --import-crush /output/crushmap.bin --save /output/osdmap.bin >/dev/null
	cp /output/osdmap.bin /output/osdmap-upmap.bin
	osdmaptool --mark-up-in --upmap /output/upmap-commands.txt --upmap-max 20 \
		--upmap-deviation 1 --upmap-seed 1 --save /output/osdmap-upmap.bin >/dev/null
	osdmaptool --createsimple 4 --with-default-pool --pg-bits 3 --pgp-bits 3 --clobber /output/osdmap32.bin >/dev/null
	osdmaptool --import-crush /output/crushmap.bin --save /output/osdmap32.bin >/dev/null
	: > /output/object-mappings.txt
	: > /output/object-mappings-pg32.txt
	: > /output/object-mappings-osd1-out.txt
	: > /output/object-mappings-upmap.txt
	i=0
	while test "$i" -lt 128; do
		osdmaptool --mark-up-in --test-map-object "p05-object-$i" --pool 1 /output/osdmap.bin 2>/dev/null |
			sed -n "s/^ object /object /p" >> /output/object-mappings.txt
		osdmaptool --mark-up-in --test-map-object "p05-object-$i" --pool 1 /output/osdmap32.bin 2>/dev/null |
			sed -n "s/^ object /object /p" >> /output/object-mappings-pg32.txt
		osdmaptool --mark-up-in --mark-out 1 --test-map-object "p05-object-$i" --pool 1 /output/osdmap32.bin 2>/dev/null |
			sed -n "s/^ object /object /p" >> /output/object-mappings-osd1-out.txt
		osdmaptool --mark-up-in --test-map-object "p05-object-$i" --pool 1 /output/osdmap-upmap.bin 2>/dev/null |
			sed -n "s/^ object /object /p" >> /output/object-mappings-upmap.txt
		i=$((i+1))
	done
'
for fixture in crushmap.bin mappings.txt mappings-osd1-out.txt object-mappings.txt object-mappings-pg32.txt object-mappings-osd1-out.txt object-mappings-upmap.txt upmap-commands.txt; do
	cmp "testdata/p05/$fixture" "$temporary/$fixture"
done
for source_path in src/crush/CrushWrapper.cc src/crush/mapper.c src/crush/hash.c src/crush/crush_ln_table.h src/osd/OSDMap.cc src/osd/osd_types.cc; do
	expected=$(jq -r --arg path "$source_path" '.source.files[$path] // empty' testdata/p05/*.manifest.json | sort -u)
	test -n "$expected"
	test "$(printf '%s\n' "$expected" | wc -l | tr -d ' ')" = 1
	actual=$(curl -fsSL "https://raw.githubusercontent.com/ceph/ceph/7f793731f1b39eb4f465e960113d2363c311b964/$source_path" | shasum -a 256 | awk '{print $1}')
	test "$actual" = "$expected"
done
printf '%s\n' 'P05 pinned placement corpus reproduction passed'
