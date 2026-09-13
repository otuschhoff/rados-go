#!/bin/sh
set -eu

require_linux=false
if [ "${1:-}" = "--require-linux-host" ]; then
  require_linux=true
fi

missing=""
for command_name in docker jq git go; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    missing="$missing $command_name"
  fi
done
if [ -n "$missing" ]; then
  echo "P00 preflight: missing required commands:$missing" >&2
  exit 1
fi

if [ "$(uname -s)" != "Linux" ]; then
  echo "P00 preflight: local evidence checks available; live cluster blocked on non-Linux host"
  if [ "$require_linux" = true ]; then
    exit 2
  fi
  exit 0
fi

if [ "$(id -u)" -ne 0 ]; then
  echo "P00 preflight: live runner requires root for loop devices and cephadm" >&2
  if [ "$require_linux" = true ]; then
    exit 2
  fi
  exit 0
fi

for command_name in awk df getconf losetup python3 ss systemctl sha256sum ip truncate; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "P00 preflight: missing Linux host command: $command_name" >&2
    exit 2
  fi
done

if [ "$(docker info --format '{{.OSType}}' 2>/dev/null)" != "linux" ]; then
  echo "P00 preflight: a reachable Linux Docker daemon is required" >&2
  exit 2
fi

if ! ss -ltnH | awk '$4 ~ /:22$/ { found = 1 } END { exit !found }'; then
  echo "P00 preflight: an SSH daemon must listen on port 22 for cephadm host management" >&2
  exit 2
fi

if ! systemctl is-system-running >/dev/null 2>&1 && ! systemctl is-system-running 2>/dev/null | grep -q degraded; then
  echo "P00 preflight: systemd is not running" >&2
  exit 2
fi

cpu_count=$(getconf _NPROCESSORS_ONLN)
memory_kib=$(awk '/^MemTotal:/ {print $2}' /proc/meminfo)
disk_kib=$(df -Pk /var/lib | awk 'NR == 2 {print $4}')
if [ "$cpu_count" -lt 4 ]; then
  echo "P00 preflight: at least 4 CPUs required; found $cpu_count" >&2
  exit 2
fi
if [ "$memory_kib" -lt 12582912 ]; then
  echo "P00 preflight: at least 12 GiB RAM required; found $((memory_kib / 1024 / 1024)) GiB" >&2
  exit 2
fi
if [ "$disk_kib" -lt 41943040 ]; then
  echo "P00 preflight: at least 40 GiB free under /var/lib required; found $((disk_kib / 1024 / 1024)) GiB" >&2
  exit 2
fi

echo "P00 preflight: Linux host requirements satisfied"