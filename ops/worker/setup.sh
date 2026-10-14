#!/usr/bin/env bash
# One-time setup for a bare-metal Firecracker worker.
set -euo pipefail

if [ "$(id -u)" -ne 0 ]; then
	echo "run as root: sudo WORKER_TOKEN=<shared-token> bash $0" >&2
	exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR=/opt/renderops
WORKER_ROOT="$APP_DIR/worker"
ENV_FILE=/etc/renderops/worker.env

if [ ! -e /dev/kvm ]; then
	echo "/dev/kvm is missing; this host cannot run Firecracker" >&2
	exit 1
fi
if [ ! -e /sys/fs/cgroup/cgroup.controllers ]; then
	echo "cgroup v2 is required" >&2
	exit 1
fi

if command -v apt-get >/dev/null 2>&1; then
	apt-get update -qq
	DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
		acl ca-certificates curl iproute2 nftables tar util-linux
fi

install -d -m 0750 /etc/renderops
install -d -m 0750 "$APP_DIR" "$WORKER_ROOT"

if [ ! -f "$ENV_FILE" ]; then
	: "${WORKER_TOKEN:?WORKER_TOKEN is required for the first setup}"
	umask 077
	{
		printf 'ROOT_DIRECTORY=%s\n' "$WORKER_ROOT"
		printf 'ACTIVE_DISK_BACKEND=%s\n' "${ACTIVE_DISK_BACKEND:-filesystem}"
		printf 'ACTIVE_DISK_DIR=%s\n' "${ACTIVE_DISK_DIR:-$WORKER_ROOT/sockets}"
		printf 'ACTIVE_DISK_CLONE_MODE=%s\n' "${ACTIVE_DISK_CLONE_MODE:-auto}"
		printf 'WORKER_BIND=%s\n' "${WORKER_BIND:-127.0.0.1:9876}"
		printf 'WORKER_TOKEN=%s\n' "$WORKER_TOKEN"
		printf 'WORKER_ID=%s\n' "${WORKER_ID:-}"
		printf 'WORKER_ADVERTISE_URL=%s\n' "${WORKER_ADVERTISE_URL:-}"
		printf 'WORKER_POOL=%s\n' "${WORKER_POOL:-default}"
		printf 'WORKER_ALLOCATABLE_VCPUS=%s\n' "${WORKER_ALLOCATABLE_VCPUS:-}"
		printf 'WORKER_ALLOCATABLE_MEMORY_MB=%s\n' "${WORKER_ALLOCATABLE_MEMORY_MB:-}"
		printf 'WORKER_DISK_CAP_GB=%s\n' "${WORKER_DISK_CAP_GB:-}"
		printf 'WORKER_DISK_RESERVE_GB=%s\n' "${WORKER_DISK_RESERVE_GB:-}"
		printf 'WORKER_CPU_OVERCOMMIT_RATIO=%s\n' "${WORKER_CPU_OVERCOMMIT_RATIO:-2}"
		printf 'WORKER_MEMORY_OVERCOMMIT_RATIO=%s\n' "${WORKER_MEMORY_OVERCOMMIT_RATIO:-1}"
		printf 'WORKER_MAX_SESSIONS=%s\n' "${WORKER_MAX_SESSIONS:-200}"
		printf 'WORKER_MAX_TERMINALS_PER_SANDBOX=%s\n' "${WORKER_MAX_TERMINALS_PER_SANDBOX:-8}"
		printf 'ORCHESTRATOR_URL=%s\n' "${ORCHESTRATOR_URL:-}"
		printf 'CONTROL_PLANE_INTERNAL_URL=%s\n' "${CONTROL_PLANE_INTERNAL_URL:-}"
		printf 'WORKER_LOG_INGEST_URL=%s\n' "${WORKER_LOG_INGEST_URL:-}"
		printf 'WORKER_LOG_SPOOL_DIR=%s\n' "${WORKER_LOG_SPOOL_DIR:-$WORKER_ROOT/logs/pending}"
		printf 'HOST_VALIDATION_MODE=strict\n'
		# SLOT_COUNT is intentionally omitted. The worker derives active network
		# slots from physical vCPUs x CPU overcommit, capped by max sessions.
		printf 'MAX_CONCURRENT_PROVISIONS=%s\n' "${MAX_CONCURRENT_PROVISIONS:-8}"
		printf 'TEMPLATE_SOURCE=%s\n' "${TEMPLATE_SOURCE:-build}"
		printf 'TEMPLATE_CACHE_DIR=%s\n' "${TEMPLATE_CACHE_DIR:-$WORKER_ROOT/template-cache}"
		printf 'DEFAULT_TEMPLATE_RELEASE=%s\n' "${DEFAULT_TEMPLATE_RELEASE:-}"
		printf 'BLOB_STORAGE_ACCOUNT=%s\n' "${BLOB_STORAGE_ACCOUNT:-}"
		printf 'BLOB_CONTAINER_NAME=%s\n' "${BLOB_CONTAINER_NAME:-}"
		printf 'BLOB_SECRET_KEY=%s\n' "${BLOB_SECRET_KEY:-}"
		printf 'SANDBOX_CHECKPOINTS_ENABLED=%s\n' "${SANDBOX_CHECKPOINTS_ENABLED:-true}"
		printf 'SANDBOX_CHECKPOINT_CONTAINER_NAME=%s\n' "${SANDBOX_CHECKPOINT_CONTAINER_NAME:-${BLOB_CONTAINER_NAME:-}}"
		printf 'SANDBOX_CHECKPOINT_PREFIX=%s\n' "${SANDBOX_CHECKPOINT_PREFIX:-sandbox-checkpoints}"
		printf 'SHUTDOWN_GRACE_PERIOD_SECONDS=%s\n' "${SHUTDOWN_GRACE_PERIOD_SECONDS:-300}"
		printf 'SHUTDOWN_PAUSE_CONCURRENCY=%s\n' "${SHUTDOWN_PAUSE_CONCURRENCY:-4}"
	} > "$ENV_FILE"
else
	echo "preserved existing $ENV_FILE"
fi
chown root:root "$ENV_FILE"
chmod 0600 "$ENV_FILE"

if [ -n "${ASSET_BUNDLE:-}" ]; then
	if [ ! -f "$ASSET_BUNDLE" ]; then
		echo "ASSET_BUNDLE does not exist: $ASSET_BUNDLE" >&2
		exit 1
	fi
	install -m 0600 "$ASSET_BUNDLE" "$WORKER_ROOT/renderops-assets.tar.gz"
fi

install -m 0644 "$SCRIPT_DIR/worker.service" /etc/systemd/system/renderops-worker.service
systemctl daemon-reload
systemctl enable renderops-worker >/dev/null

if [ ! -f "$WORKER_ROOT/assets/manifest.sha256" ] &&
	[ ! -f "$WORKER_ROOT/renderops-assets.tar.gz" ]; then
	echo "worker host prepared, but Firecracker assets are still required" >&2
	echo "copy renderops-assets.tar.gz to $WORKER_ROOT before deployment" >&2
	exit 2
fi

echo "Worker host prepared. Run the Deploy workflow with target: worker."
