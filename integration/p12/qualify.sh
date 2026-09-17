#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
cd "$root"

exec env CGO_ENABLED=0 GOTOOLCHAIN=go1.27.1 go run ./tools/p12-qualify -root . -out docs/p12/qualification-report.json "$@"