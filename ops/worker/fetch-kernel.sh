#!/usr/bin/env bash
# Downloads the guest kernel pinned in ops/worker/kernel.lock from Azure Blob
# (<container>/kernel-release/<release>/vmlinux, mirrored there by spawn-kernel)
# and verifies it against the pinned sha256.
#
# Usage: fetch-kernel.sh <output-dir>      -> <output-dir>/vmlinux
# Env:   AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY, BLOB_CONTAINER_NAME
set -euo pipefail

OUT="${1:?usage: fetch-kernel.sh <output-dir>}"
LOCK="$(dirname "$0")/kernel.lock"
: "${AZURE_STORAGE_ACCOUNT:?AZURE_STORAGE_ACCOUNT required}"
: "${AZURE_STORAGE_KEY:?AZURE_STORAGE_KEY required}"
: "${BLOB_CONTAINER_NAME:?BLOB_CONTAINER_NAME required}"

lock_value() { sed -n "s/^$1=//p" "$LOCK"; }
release="$(lock_value KERNEL_RELEASE)"
expected="$(lock_value KERNEL_SHA256)"
[[ "$release" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] || { echo "invalid KERNEL_RELEASE: $release" >&2; exit 1; }
[[ "$expected" =~ ^[0-9a-f]{64}$ ]] || { echo "KERNEL_SHA256 in kernel.lock is not a sha256 digest" >&2; exit 1; }

mkdir -p "$OUT"
az storage blob download --only-show-errors \
	--container-name "$BLOB_CONTAINER_NAME" \
	--name "kernel-release/$release/vmlinux" \
	--file "$OUT/vmlinux" >/dev/null
actual="$(sha256sum "$OUT/vmlinux" | awk '{ print $1 }')"
if [ "$actual" != "$expected" ]; then
	echo "kernel checksum mismatch: expected $expected, got $actual" >&2
	exit 1
fi
echo "fetched $release ($actual)"
