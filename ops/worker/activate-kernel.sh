#!/usr/bin/env bash
# Installs one immutable guest kernel version and atomically points the worker's
# stable kernel path at it. The worker must be restarted after this script succeeds.
#
# Rollback: run it again with an already-installed version, e.g.
#   activate-kernel.sh assets/kernel/versions/vmlinux-legacy-<sum> <sha256> vmlinux-legacy-<sum>
set -euo pipefail

KERNEL_FILE="${1:?usage: activate-kernel.sh <vmlinux> <sha256> <release>}"
EXPECTED="${2:?usage: activate-kernel.sh <vmlinux> <sha256> <release>}"
RELEASE="${3:?usage: activate-kernel.sh <vmlinux> <sha256> <release>}"
ROOT="${ROOT_DIRECTORY:-/opt/renderops/worker}"
ASSETS="$ROOT/assets"
KERNEL_DIR="$ASSETS/kernel"
VERSIONS_DIR="$KERNEL_DIR/versions"
ACTIVE_PATH="$KERNEL_DIR/vmlinux"
MANIFEST="$ASSETS/manifest.sha256"

case "$RELEASE" in
	*[!A-Za-z0-9._-]* | '' | .*)
		echo "invalid kernel release: $RELEASE" >&2
		exit 1
		;;
esac

case "$EXPECTED" in
	*[!0-9a-f]* | '')
		echo "invalid kernel checksum" >&2
		exit 1
		;;
esac
if [ "${#EXPECTED}" -ne 64 ]; then
	echo "invalid kernel checksum" >&2
	exit 1
fi

test -f "$MANIFEST"
test -f "$KERNEL_FILE"
ACTUAL="$(sha256sum "$KERNEL_FILE" | awk '{ print $1 }')"
if [ "$ACTUAL" != "$EXPECTED" ]; then
	echo "kernel checksum mismatch: expected $EXPECTED, got $ACTUAL" >&2
	exit 1
fi

install -d -m 0755 "$VERSIONS_DIR"
TARGET="$VERSIONS_DIR/$RELEASE"
if [ -e "$TARGET" ]; then
	INSTALLED="$(sha256sum "$TARGET" | awk '{ print $1 }')"
	if [ "$INSTALLED" != "$EXPECTED" ]; then
		echo "existing kernel release $RELEASE has a different checksum" >&2
		exit 1
	fi
else
	STAGE="$(mktemp "$VERSIONS_DIR/.kernel-stage.XXXXXX")"
	trap 'rm -f "$STAGE"' EXIT
	cp "$KERNEL_FILE" "$STAGE"
	chmod 0644 "$STAGE"
	mv "$STAGE" "$TARGET"
fi

# Preserve the pre-versioning kernel as an immutable rollback target. Renaming
# is safe for running Firecracker processes because their open inode remains valid.
if [ -e "$ACTIVE_PATH" ] && [ ! -L "$ACTIVE_PATH" ]; then
	LEGACY_SUM="$(sha256sum "$ACTIVE_PATH" | awk '{ print $1 }')"
	LEGACY="$VERSIONS_DIR/vmlinux-legacy-${LEGACY_SUM:0:12}"
	if [ ! -e "$LEGACY" ]; then
		mv "$ACTIVE_PATH" "$LEGACY"
	else
		rm -f "$ACTIVE_PATH"
	fi
fi

LINK="$KERNEL_DIR/.vmlinux.next"
ln -sfn "versions/$RELEASE" "$LINK"
mv -f "$LINK" "$ACTIVE_PATH"

# Worker startup verifies every asset against the manifest, so the kernel line
# must change together with the file.
MANIFEST_NEXT="$ASSETS/.manifest.sha256.next"
awk -v sum="$EXPECTED" '
	$2 == "kernel/vmlinux" { printf "%s  %s\n", sum, $2; found = 1; next }
	{ print }
	END { if (!found) exit 2 }
' "$MANIFEST" > "$MANIFEST_NEXT"
chmod 0644 "$MANIFEST_NEXT"
mv -f "$MANIFEST_NEXT" "$MANIFEST"

# The original all-assets bundle contains the old kernel. Keeping it would allow
# a deleted marker to silently reinstall it on a later restart.
rm -f "$ROOT/renderops-assets.tar.gz" "$ASSETS/.installed-bundle"
sync
echo "activated kernel $RELEASE ($EXPECTED)"
