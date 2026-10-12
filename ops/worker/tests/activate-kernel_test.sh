#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

ROOT="$TMP/worker"
ASSETS="$ROOT/assets"
mkdir -p "$ASSETS/kernel" "$ASSETS/rootfs"
printf 'old-kernel' > "$ASSETS/kernel/vmlinux"
printf 'rootfs' > "$ASSETS/rootfs/rootfs-alpine.ext4"
OLD_SUM="$(sha256sum "$ASSETS/kernel/vmlinux" | awk '{ print $1 }')"
ROOTFS_SUM="$(sha256sum "$ASSETS/rootfs/rootfs-alpine.ext4" | awk '{ print $1 }')"
printf '%s  kernel/vmlinux\n%s  rootfs/rootfs-alpine.ext4\n' "$OLD_SUM" "$ROOTFS_SUM" > "$ASSETS/manifest.sha256"
printf 'stale-bundle' > "$ROOT/renderops-assets.tar.gz"
printf 'stale-marker' > "$ASSETS/.installed-bundle"

printf 'new-kernel' > "$TMP/vmlinux"
NEW_SUM="$(sha256sum "$TMP/vmlinux" | awk '{ print $1 }')"

ROOT_DIRECTORY="$ROOT" "$SCRIPT_DIR/activate-kernel.sh" "$TMP/vmlinux" "$NEW_SUM" vmlinux-test.1

test -L "$ASSETS/kernel/vmlinux"
test "$(cat "$ASSETS/kernel/vmlinux")" = 'new-kernel'
test -f "$ASSETS/kernel/versions/vmlinux-test.1"
test -f "$ASSETS/kernel/versions/vmlinux-legacy-${OLD_SUM:0:12}"
grep -q "^${NEW_SUM}  kernel/vmlinux$" "$ASSETS/manifest.sha256"
grep -q "^${ROOTFS_SUM}  rootfs/rootfs-alpine.ext4$" "$ASSETS/manifest.sha256"
(cd "$ASSETS" && sha256sum -c --quiet manifest.sha256)
test ! -e "$ROOT/renderops-assets.tar.gz"
test ! -e "$ASSETS/.installed-bundle"

# Reinstalling the same immutable release is idempotent.
ROOT_DIRECTORY="$ROOT" "$SCRIPT_DIR/activate-kernel.sh" "$TMP/vmlinux" "$NEW_SUM" vmlinux-test.1

# Rollback re-activates the preserved legacy kernel.
LEGACY="$ASSETS/kernel/versions/vmlinux-legacy-${OLD_SUM:0:12}"
ROOT_DIRECTORY="$ROOT" "$SCRIPT_DIR/activate-kernel.sh" "$LEGACY" "$OLD_SUM" "vmlinux-legacy-${OLD_SUM:0:12}"
test "$(cat "$ASSETS/kernel/vmlinux")" = 'old-kernel'
grep -q "^${OLD_SUM}  kernel/vmlinux$" "$ASSETS/manifest.sha256"

if ROOT_DIRECTORY="$ROOT" "$SCRIPT_DIR/activate-kernel.sh" \
	"$TMP/vmlinux" "$(printf '%064d' 0)" vmlinux-bad.1; then
	echo 'activation accepted a bad checksum' >&2
	exit 1
fi
if ROOT_DIRECTORY="$ROOT" "$SCRIPT_DIR/activate-kernel.sh" \
	"$TMP/vmlinux" "$NEW_SUM" '../escape'; then
	echo 'activation accepted a path in the release name' >&2
	exit 1
fi

echo 'activate-kernel tests passed'
