#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
cd "$root"
platform=${P03_PLATFORM:-}
if test -z "$platform"; then
	case "$(docker info --format '{{.Architecture}}')" in
		x86_64|amd64) platform=linux/amd64 ;;
		aarch64|arm64) platform=linux/arm64 ;;
		*) printf 'unsupported native Docker architecture\n' >&2; exit 2 ;;
	esac
fi
architecture=${platform#linux/}
case "$platform" in
	linux/amd64|linux/arm64) ;;
	*) printf 'unsupported P03 platform: %s\n' "$platform" >&2; exit 2 ;;
esac
image_index=$(jq -r '.images.qualification.reference' docs/p00/evidence.json)
image_digest=$(jq -r --arg architecture "$architecture" '.images.qualification[$architecture]' docs/p00/evidence.json)
image="${image_index%@*}@$image_digest"
temporary=$(mktemp -d)
trap 'rm -rf "$temporary"' EXIT HUP INT TERM

test "$(docker run --rm --platform "$platform" "$image" ceph --version | awk '{print $3}')" = "20.2.4"
test "$(docker run --rm --platform "$platform" "$image" openssl version | awk '{print $2}')" = "3.5.7"

encoding_vectors='[]'
for type in CephXServerChallenge CephXResponseHeader CephXTicketBlob CryptoKey; do
	docker run --rm --platform "$platform" -v "$temporary:/out" "$image" \
		ceph-dencoder type "$type" select_test 0 encode export "/out/$type.bin"
	actual=$(xxd -p "$temporary/$type.bin" | tr -d '\n')
	encoding_vectors=$(printf '%s' "$encoding_vectors" | jq --arg type "$type" --arg hex "$actual" '. + [{type: $type, test_instance: 0, hex: $hex}]')
done
jq -n --argjson vectors "$encoding_vectors" '{schema_version: 1, vectors: $vectors}' | jq -S . >"$temporary/cephx-encoding-vectors.json"
jq -S . testdata/p03/cephx-encoding-vectors.json >"$temporary/expected-cephx-encoding-vectors.json"
cmp "$temporary/expected-cephx-encoding-vectors.json" "$temporary/cephx-encoding-vectors.json"

oracle=$(docker run --rm -i --platform "$platform" "$image" python3 - <<'PY'
import hashlib
import hmac
import struct
import subprocess

padded = bytes.fromhex("6162630d0d0d0d0d0d0d0d0d0d0d0d0d")
cbc = subprocess.run(
	["openssl", "enc", "-aes-128-cbc", "-K", "31323334353637383930313233343536", "-iv", "63657068736167657975646167726567", "-nopad"],
	input=padded,
	check=True,
	capture_output=True,
).stdout
digest = hmac.new(
	bytes(range(32)),
	bytes.fromhex("88776655443322110807060504030201"),
	hashlib.sha256,
).digest()
words = struct.unpack("<QQQQ", digest)
print(cbc.hex())
print(struct.pack("<Q", words[0] ^ words[1] ^ words[2] ^ words[3]).hex())
PY
)
cbc=$(printf '%s\n' "$oracle" | sed -n '1p')
test "$cbc" = "$(jq -r '.vectors.ceph_aes_cbc.ciphertext_hex' testdata/p03/crypto-vectors.json)"

challenge=$(printf '%s\n' "$oracle" | sed -n '2p')

rfc_key=$(jq -r '.vectors.rfc8009_aes256.key_hex' testdata/p03/crypto-vectors.json)
rfc_ciphertext=$(jq -r '.vectors.rfc8009_aes256.ciphertext_hex' testdata/p03/crypto-vectors.json)
rfc_usage=$(jq -r '.vectors.rfc8009_aes256.key_usage' testdata/p03/crypto-vectors.json)
rfc_plaintext=$(go run ./integration/p03/rfc8009-oracle "$rfc_key" "$rfc_ciphertext" "$rfc_usage")

jq -n \
	--arg cbc "$cbc" \
	--arg challenge "$challenge" \
	--arg rfc_plaintext "$rfc_plaintext" \
	'{
		schema_version: 1,
		vectors: {
			ceph_aes_cbc: {
				key_hex: "31323334353637383930313233343536",
				plaintext_hex: "616263",
				ciphertext_hex: $cbc
			},
			rfc8009_aes256: {
				key_usage: 2,
				key_hex: "6d404d37faf79f9df0d33568d320669800eb4836472ea8a026d16b7182460c52",
				ciphertext_hex: "4ed7b37c2bcac8f74f23c1cf07e62bc7b75fb3f637b9f559c7f664f69eab7b6092237526ea0d1f61cb20d69d10f2",
				plaintext_hex: $rfc_plaintext
			},
			aes256_challenge: {
				key_hex: "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
				server_challenge: 1234605616436508552,
				client_challenge: 72623859790382856,
				result_hex_le: $challenge
			}
		}
	}' | jq -S . >"$temporary/crypto-vectors.json"
jq -S . testdata/p03/crypto-vectors.json >"$temporary/expected-crypto-vectors.json"
cmp "$temporary/expected-crypto-vectors.json" "$temporary/crypto-vectors.json"

for source_path in src/auth/cephx/CephxProtocol.h src/auth/Crypto.h src/auth/Crypto.cc src/auth/cephx/CephxProtocol.cc; do
	expected=$(jq -r --arg path "$source_path" '.source.files[$path] // empty' testdata/p03/*.manifest.json | sort -u)
	test -n "$expected"
	actual=$(curl -fsSL "https://raw.githubusercontent.com/ceph/ceph/7f793731f1b39eb4f465e960113d2363c311b964/$source_path" | shasum -a 256 | awk '{print $1}')
	test "$actual" = "$expected"
done

printf '%s\n' 'P03 fixture reproduction passed'
