#!/usr/bin/env bash
# Ships the worker binary + systemd unit to the bare-metal host and restarts it.
# Idempotent. Runs inside GitHub Actions — the SSH key never leaves the runner.
#
# Assumes the ONE-TIME host setup is already done (worker.env with WORKER_TOKEN,
# and Firecracker assets under /opt/renderops/worker/assets). This installer
# verifies those exist and fails clearly if not — it does NOT create secrets or
# provision assets.
#
# Usage: install-worker.sh <local-binary-path> [rootfs-archive checksum-file]
# Env:   SSH_KEY (private key contents), SSH_HOST, SSH_USER
#        WORKER_MAX_SESSIONS, WORKER_*_OVERCOMMIT_RATIO,
#        WORKER_MAX_TERMINALS_PER_SANDBOX
#        CONTROL_PLANE_INTERNAL_URL (internal raw-usage ingestion endpoint)
#        BLOB_STORAGE_ACCOUNT, BLOB_CONTAINER_NAME, BLOB_SECRET_KEY
#        KERNEL_FILE, KERNEL_SHA256, KERNEL_RELEASE (optional, all or none:
#        activates a guest kernel from ops/worker/kernel.lock)
#        WORKER_IMAGES (extra images from ops/worker/images.lock; empty removes them)
set -euo pipefail

BINARY="${1:?usage: install-worker.sh <binary>}"
ROOTFS_ARCHIVE="${2:-}"
ROOTFS_CHECKSUM="${3:-}"
: "${SSH_KEY:?SSH_KEY required}" "${SSH_HOST:?SSH_HOST required}" "${SSH_USER:?SSH_USER required}"
UNIT="$(dirname "$0")/worker.service"
ROOTFS_INSTALLER="$(dirname "$0")/activate-rootfs.sh"
KERNEL_INSTALLER="$(dirname "$0")/activate-kernel.sh"
DEPLOY_ROOTFS_VERSION=""
DEPLOY_KERNEL_RELEASE=""

if [ -n "$ROOTFS_ARCHIVE" ] || [ -n "$ROOTFS_CHECKSUM" ]; then
	: "${ROOTFS_ARCHIVE:?rootfs archive required}" "${ROOTFS_CHECKSUM:?rootfs checksum required}"
	: "${ROOTFS_VERSION:?ROOTFS_VERSION required when deploying a rootfs}"
	test -f "$ROOTFS_ARCHIVE"
	test -f "$ROOTFS_CHECKSUM"
	DEPLOY_ROOTFS_VERSION="$ROOTFS_VERSION"
fi

if [ -n "${KERNEL_FILE:-}" ] || [ -n "${KERNEL_SHA256:-}" ] || [ -n "${KERNEL_RELEASE:-}" ]; then
	: "${KERNEL_FILE:?KERNEL_FILE required when deploying a kernel}"
	: "${KERNEL_SHA256:?KERNEL_SHA256 required when deploying a kernel}"
	: "${KERNEL_RELEASE:?KERNEL_RELEASE required when deploying a kernel}"
	test -f "$KERNEL_FILE"
	# Both values are embedded in the remote command line below.
	case "$KERNEL_RELEASE" in
	*[!A-Za-z0-9._-]* | .*)
		echo "invalid KERNEL_RELEASE: $KERNEL_RELEASE" >&2
		exit 1
		;;
	esac
	case "$KERNEL_SHA256" in
	*[!0-9a-f]*)
		echo "invalid KERNEL_SHA256" >&2
		exit 1
		;;
	esac
	DEPLOY_KERNEL_RELEASE="$KERNEL_RELEASE"
fi

# Embedded in the remote command line below.
case "${WORKER_IMAGES:-}" in
*[!a-z0-9A-Z._@=,-]*)
	echo "invalid WORKER_IMAGES" >&2
	exit 1
	;;
esac

for value in "${WORKER_MAX_SESSIONS:-}" "${WORKER_MAX_TERMINALS_PER_SANDBOX:-}"; do
	[ -z "$value" ] && continue
	case "$value" in
	*[!0-9]* | 0)
		echo "worker capacity values must be positive integers" >&2
		exit 1
		;;
	esac
done

SERVICE=renderops-worker
HEALTH_URL=http://127.0.0.1:9876/worker/health

KEYFILE="$(mktemp)"
trap 'rm -f "$KEYFILE"' EXIT
printf '%s\n' "$SSH_KEY" > "$KEYFILE"
chmod 600 "$KEYFILE"

SCP="scp -i $KEYFILE -o StrictHostKeyChecking=accept-new"
SSH="ssh -i $KEYFILE -o StrictHostKeyChecking=accept-new -o ConnectTimeout=20 ${SSH_USER}@${SSH_HOST}"

echo "==> uploading worker binary + service unit"
$SCP "$BINARY" "${SSH_USER}@${SSH_HOST}:/tmp/renderops-worker.new"
$SCP "$UNIT"   "${SSH_USER}@${SSH_HOST}:/tmp/renderops-worker.service.new"
if [ -n "$ROOTFS_ARCHIVE" ]; then
	echo "==> uploading rootfs version ${ROOTFS_VERSION}"
	$SCP "$ROOTFS_ARCHIVE" "${SSH_USER}@${SSH_HOST}:/tmp/renderops-rootfs.tar.gz.new"
	$SCP "$ROOTFS_CHECKSUM" "${SSH_USER}@${SSH_HOST}:/tmp/renderops-rootfs.sha256.new"
	$SCP "$ROOTFS_INSTALLER" "${SSH_USER}@${SSH_HOST}:/tmp/activate-rootfs.sh.new"
fi
if [ -n "$DEPLOY_KERNEL_RELEASE" ]; then
	echo "==> uploading kernel ${KERNEL_RELEASE}"
	$SCP "$KERNEL_FILE" "${SSH_USER}@${SSH_HOST}:/tmp/renderops-vmlinux.new"
	$SCP "$KERNEL_INSTALLER" "${SSH_USER}@${SSH_HOST}:/tmp/activate-kernel.sh.new"
fi

echo "==> installing (idempotent) and restarting"
# shellcheck disable=SC2087
$SSH "sudo WORKER_MAX_SESSIONS='${WORKER_MAX_SESSIONS:-}' WORKER_CPU_OVERCOMMIT_RATIO='${WORKER_CPU_OVERCOMMIT_RATIO:-}' WORKER_MEMORY_OVERCOMMIT_RATIO='${WORKER_MEMORY_OVERCOMMIT_RATIO:-}' WORKER_MAX_TERMINALS_PER_SANDBOX='${WORKER_MAX_TERMINALS_PER_SANDBOX:-}' CONTROL_PLANE_INTERNAL_URL='${CONTROL_PLANE_INTERNAL_URL:-}' BLOB_STORAGE_ACCOUNT='${BLOB_STORAGE_ACCOUNT:-}' BLOB_CONTAINER_NAME='${BLOB_CONTAINER_NAME:-}' BLOB_SECRET_KEY='${BLOB_SECRET_KEY:-}' ROOTFS_VERSION='${DEPLOY_ROOTFS_VERSION}' KERNEL_RELEASE='${DEPLOY_KERNEL_RELEASE}' KERNEL_SHA256='${KERNEL_SHA256:-}' WORKER_IMAGES='${WORKER_IMAGES:-}' bash -s" <<'REMOTE'
set -euo pipefail
# Preconditions from the one-time host setup — fail clearly if missing.
if [ ! -f /etc/renderops/worker.env ]; then
  echo "ERROR: /etc/renderops/worker.env missing — run the one-time worker host setup first." >&2
  exit 1
fi
if [ ! -f /opt/renderops/worker/assets/manifest.sha256 ] &&
   [ ! -f /opt/renderops/worker/renderops-assets.tar.gz ]; then
  echo "ERROR: Firecracker assets or renderops-assets.tar.gz are missing — run worker setup first." >&2
  exit 1
fi

set_env_value() {
  local key="$1"
  local value="$2"
  [ -n "$value" ] || return 0
  if grep -q "^${key}=" /etc/renderops/worker.env; then
    sed -i "s|^${key}=.*|${key}=${value}|" /etc/renderops/worker.env
  else
    printf '%s=%s\n' "$key" "$value" >> /etc/renderops/worker.env
  fi
}

# Remove the legacy fixed slot count. The worker now derives network capacity
# from detected vCPUs and the configured CPU overcommit ratio.
sed -i '/^SLOT_COUNT=/d' /etc/renderops/worker.env
set_env_value WORKER_MAX_SESSIONS "$WORKER_MAX_SESSIONS"
set_env_value WORKER_CPU_OVERCOMMIT_RATIO "$WORKER_CPU_OVERCOMMIT_RATIO"
set_env_value WORKER_MEMORY_OVERCOMMIT_RATIO "$WORKER_MEMORY_OVERCOMMIT_RATIO"
set_env_value WORKER_MAX_TERMINALS_PER_SANDBOX "$WORKER_MAX_TERMINALS_PER_SANDBOX"
set_env_value CONTROL_PLANE_INTERNAL_URL "${CONTROL_PLANE_INTERNAL_URL:-}"
set_env_value BLOB_STORAGE_ACCOUNT "${BLOB_STORAGE_ACCOUNT:-}"
set_env_value BLOB_CONTAINER_NAME "${BLOB_CONTAINER_NAME:-}"
set_env_value BLOB_SECRET_KEY "${BLOB_SECRET_KEY:-}"
set_env_value SANDBOX_CHECKPOINTS_ENABLED true
set_env_value SANDBOX_CHECKPOINT_CONTAINER_NAME "${BLOB_CONTAINER_NAME:-}"
set_env_value SANDBOX_CHECKPOINT_PREFIX sandbox-checkpoints
# Extra images follow ops/worker/images.lock exactly, including removal.
if [ -n "${WORKER_IMAGES:-}" ]; then
  set_env_value WORKER_IMAGES "$WORKER_IMAGES"
else
  sed -i '/^WORKER_IMAGES=/d' /etc/renderops/worker.env
fi

install -D -m 0755 /tmp/renderops-worker.new /opt/renderops/renderops-worker
install -D -m 0644 /tmp/renderops-worker.service.new /etc/systemd/system/renderops-worker.service
if [ -n "${ROOTFS_VERSION:-}" ]; then
  chmod 0755 /tmp/activate-rootfs.sh.new
  ROOT_DIRECTORY=/opt/renderops/worker /tmp/activate-rootfs.sh.new \
    /tmp/renderops-rootfs.tar.gz.new /tmp/renderops-rootfs.sha256.new "$ROOTFS_VERSION"
fi
if [ -n "${KERNEL_RELEASE:-}" ]; then
  chmod 0755 /tmp/activate-kernel.sh.new
  ROOT_DIRECTORY=/opt/renderops/worker /tmp/activate-kernel.sh.new \
    /tmp/renderops-vmlinux.new "$KERNEL_SHA256" "$KERNEL_RELEASE"
fi
rm -f /tmp/renderops-worker.new /tmp/renderops-worker.service.new \
  /tmp/activate-rootfs.sh.new /tmp/renderops-rootfs.tar.gz.new /tmp/renderops-rootfs.sha256.new \
  /tmp/activate-kernel.sh.new /tmp/renderops-vmlinux.new
systemctl daemon-reload
systemctl enable renderops-worker >/dev/null 2>&1 || true
systemctl restart renderops-worker
REMOTE

echo "==> health check (allow time for snapshot template warmup)"
for i in $(seq 1 "${WORKER_HEALTH_ATTEMPTS:-150}"); do
  if $SSH "curl -fsS -m 4 ${HEALTH_URL} >/dev/null 2>&1"; then
    echo "    worker healthy after $((i*2))s"
    exit 0
  fi
  sleep 2
done

echo "    worker did not become healthy — status:"
$SSH "sudo systemctl status ${SERVICE} --no-pager -l | tail -25 || true"
exit 1
