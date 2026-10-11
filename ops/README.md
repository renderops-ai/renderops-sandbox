# Operations Guide

Runtime operations are grouped by deployment target. The canonical model is:

| Target | Runtime | Public ingress |
| --- | --- | --- |
| Control-plane VPS | Docker Compose | Caddy on `80/443` |
| Orchestrator server | Docker Compose | none; private `8090` only |
| Worker host | systemd binary | none; private `9876` only |

Server environment files are deployment inputs and must not be committed.

## Control-Plane VPS

`ops/control-plane/` owns the Compose stack:

- PostgreSQL 18 for the current development topology;
- Go control plane on internal port `8081`;
- preview gateway on internal port `8082`;
- SvelteKit platform on internal port `3000`;
- Caddy on public ports `80`, `443/tcp`, and `443/udp`.

Caddy routes:

```text
dev.renderops.com                  -> platform:3000
dev-api.renderops.com              -> control-plane:8081
*.dev-sandbox.renderops.com        -> preview-gateway:8082
```

The wildcard preview site uses Caddy's Cloudflare DNS challenge. The
Cloudflare token needs zone DNS edit and zone read permission scoped to the
RenderOps zone; it does not need account-wide access.

### One-time setup

From a repository checkout on the VPS:

```bash
sudo RUNNER_USER=renderops-admin bash ops/control-plane/setup.sh
```

Then:

1. Populate `/opt/renderops/.env` from the server-specific secret source.
2. Set `ORCHESTRATOR_URL` to the orchestrator's private URL.
3. Register a GitHub Actions runner with the `control-plane` label.
4. Authenticate the host to pull private GHCR images.
5. Re-login so Docker/deployment-group membership applies.
6. Run the `Deploy` workflow with target `control-plane`.

The setup script preserves an existing `/opt/renderops/.env`. The deploy job
updates image references and runs `docker compose pull` plus
`docker compose up -d`; it does not copy source code or install Go on the VPS.

Required deployment values include:

```text
POSTGRES_PASSWORD
POSTGRES_BIND_IP
ORCHESTRATOR_URL
ORCHESTRATOR_TOKEN
WORKER_TOKEN
BETTER_AUTH_SECRET
BETTER_AUTH_URL
BETTER_AUTH_API_KEY
CLOUDFLARE_API_TOKEN
```

OAuth provider credentials and `TERMINAL_ALLOWED_ORIGINS`, `PREVIEW_DOMAIN`,
and `BACKEND_PUBLIC_URL` are environment-specific.

### Database migrations

`platform-migrate` is a one-shot Compose profile using the migration image
published by the platform repository:

```bash
cd /opt/renderops
docker compose --profile tools run --rm platform-migrate
```

Run reviewed Drizzle migrations before deploying application code that
requires them. Do not run `drizzle-kit push` against a shared database. The Go
services never run schema DDL.

### Backups

`ops/control-plane/backup/` installs a systemd timer that runs a custom-format
`pg_dump` daily at approximately 02:30 UTC and retains seven days under:

```text
/opt/renderops/backups/daily/
```

The dump is a full logical backup at that point in time, not an incremental
append. Local-only backups do not protect against loss of the VPS; production
must copy encrypted backups to independent durable storage and regularly test
restore.

Useful checks:

```bash
cd /opt/renderops
docker compose ps
docker compose logs --tail=200 control-plane preview-gateway platform caddy
docker compose exec -T control-plane wget -qO- http://127.0.0.1:8081/health
systemctl status renderops-backup.timer
```

## Orchestrator Server

`ops/orchestrator/` owns the private service used for worker registration,
heartbeat, placement, and lifecycle coordination. It has no Caddy route.

### One-time setup

```bash
sudo RUNNER_TOKEN='<temporary-repository-runner-token>' \
  bash ops/orchestrator/setup-github-runner.sh
sudo RUNNER_USER=renderops-runner bash ops/orchestrator/setup.sh
```

Populate `/opt/renderops-orchestrator/.env` with:

```text
DATABASE_URL
ORCHESTRATOR_BIND_IP
ORCHESTRATOR_TOKEN
WORKER_TOKEN
ORCHESTRATOR_HEARTBEAT_TTL_SECONDS=30
ORCHESTRATOR_CPU_OVERCOMMIT_RATIO=2
ORCHESTRATOR_MEMORY_OVERCOMMIT_RATIO=1
```

Register the runner with the `orchestrator` label. Permit private TCP `8090`
only from control-plane and worker addresses. PostgreSQL must permit the
orchestrator's private source address on `5432`; PostgreSQL itself must never
bind publicly.

After the runner, network route, database, and secrets are verified, set the
GitHub repository variable:

```text
ORCHESTRATOR_DEPLOY_ENABLED=true
```

Until then, CI publishes the image but deliberately skips server deployment.
The deployment records the immutable image tag in the protected server `.env`
and restarts only the orchestrator service.

Useful checks:

```bash
cd /opt/renderops-orchestrator
docker compose ps
docker compose logs --tail=200 orchestrator
docker compose exec -T orchestrator wget -qO- http://127.0.0.1:8090/health
```

## Worker Hosts

`ops/worker/` owns one-time KVM host setup, the systemd unit, rootfs activation,
and the rolling CI installer. Workers are not containers because they require
direct `/dev/kvm`, cgroup v2, nftables, network namespaces, and mounted-disk
access.

### Host prerequisites

- Linux x86_64 compatible with the published Firecracker assets;
- `/dev/kvm`;
- cgroup v2;
- `iproute2`, `nftables`, `util-linux`, `curl`, and `tar`;
- private reachability to orchestrator `8090` and control-plane metering;
- an active-disk filesystem with adequate headroom;
- Firecracker/kernel/rootfs asset bundle.

### One-time setup

Example:

```bash
sudo \
  WORKER_TOKEN='<shared-worker-token>' \
  ORCHESTRATOR_URL='http://10.0.0.7:8090' \
  CONTROL_PLANE_INTERNAL_URL='http://10.0.0.5:8081' \
  WORKER_ID='worker-1' \
  WORKER_BIND='0.0.0.0:9876' \
  WORKER_ADVERTISE_URL='http://10.0.0.4:9876' \
  WORKER_ALLOCATABLE_VCPUS='8' \
  WORKER_ALLOCATABLE_MEMORY_MB='28000' \
  WORKER_DISK_RESERVE_GB='100' \
  ACTIVE_DISK_DIR='/mnt/renderops-active' \
  ACTIVE_DISK_CLONE_MODE='required' \
  ASSET_BUNDLE='/path/to/renderops-assets.tar.gz' \
  bash ops/worker/setup.sh
```

Setup creates `/etc/renderops/worker.env` with mode `0600`, installs
`renderops-worker.service`, and preserves an existing environment file.

Every worker requires a unique:

```text
WORKER_ID
WORKER_ADVERTISE_URL
deployment SSH key
```

The advertise URL is a private address reachable by the orchestrator, control
plane, and preview gateway. Do not expose `9876` to the internet.

### Active writable disks

The configured filesystem backend writes sandbox images under
`ACTIVE_DISK_DIR`. Point it at a per-worker Azure Managed Disk, EBS volume, or
equivalent block volume mounted on that host. A volume cannot be mounted
read-write by unrelated workers just because each uses a separate directory;
normal managed disks are single-writer unless a clustered filesystem and
multi-attach design explicitly supports otherwise.

XFS with reflink enabled is preferred:

```bash
sudo parted /dev/nvme0n2 --script mklabel gpt mkpart primary xfs 0% 100%
sudo mkfs.xfs -f -m reflink=1 /dev/nvme0n2p1
sudo mkdir -p /mnt/renderops-active
sudo mount /dev/nvme0n2p1 /mnt/renderops-active
xfs_info /mnt/renderops-active | grep reflink
```

Use a stable `/etc/fstab` entry by filesystem UUID before relying on the mount
across reboots. Confirm the device name and preserve data before formatting;
`mkfs.xfs` is destructive.

Storage controls:

| Variable | Purpose |
| --- | --- |
| `ACTIVE_DISK_BACKEND` | `filesystem` today; abstraction point for another implementation |
| `ACTIVE_DISK_DIR` | Mounted directory containing active writable images |
| `ACTIVE_DISK_CLONE_MODE` | `required`, `auto`, or `copy` |
| `WORKER_DISK_RESERVE_GB` | Keeps operator headroom unavailable to admission |
| `WORKER_DISK_CAP_GB` | Optional ceiling below detected filesystem capacity |
| `TEMPLATE_CACHE_DIR` | Immutable template cache; use the active filesystem when reflinks are required |

Disk capacity is detected from the filesystem containing `ACTIVE_DISK_DIR`.
Logical sandbox quota is not physical allocation: sparse images grow as users
write data, so free-space metrics and reserve alarms remain mandatory.

### Checkpoints and templates

With these values present, pause checkpoints are stored in Azure Blob:

```text
BLOB_STORAGE_ACCOUNT
BLOB_SECRET_KEY
BLOB_CONTAINER_NAME
SANDBOX_CHECKPOINTS_ENABLED=true
SANDBOX_CHECKPOINT_CONTAINER_NAME
SANDBOX_CHECKPOINT_PREFIX=sandbox-checkpoints
```

Writable disks use 4 MiB content-addressed chunks. Later generations reuse
unchanged references and omit sparse holes. VM state and memory are immutable
per generation. Blob credentials should move to workload identity before
production.

Template controls:

```text
TEMPLATE_SOURCE=build|prebuilt
TEMPLATE_CACHE_DIR=/mnt/renderops-active/template-cache
DEFAULT_TEMPLATE_RELEASE=<release>
```

Production should fail closed when a required compatible template is missing;
runtime cold-build fallback is for development.

### Capacity and networking

The worker derives network-slot capacity from physical vCPUs multiplied by
`WORKER_CPU_OVERCOMMIT_RATIO`, capped by `WORKER_MAX_SESSIONS`. Do not manually
maintain a separate static slot count.

Common controls:

```text
WORKER_CPU_OVERCOMMIT_RATIO=2
WORKER_MEMORY_OVERCOMMIT_RATIO=1
WORKER_MAX_SESSIONS=200
MAX_CONCURRENT_PROVISIONS=8
WORKER_MAX_TERMINALS_PER_SANDBOX=8
```

The orchestrator and worker ratios must agree. Memory should not be
overcommitted without an explicit workload policy and measured OOM behavior.

### Deployment

The `Deploy` workflow builds one Linux worker binary and rolls it across the
configured hosts, waiting for worker 1 to become healthy before worker 2. Rootfs
rebuild/activation is opt-in because it changes the guest agent and template
compatibility.

GitHub `production` environment secrets:

```text
WORKER_SSH_KEY
WORKER_HOST
WORKER_USER
WORKER_2_SSH_KEY
WORKER_2_HOST
WORKER_2_USER
```

Workflow targets `worker-1` and `worker-2` deploy one host; `worker` performs
the rolling deployment. Adding autoscaled workers should use an image/bootstrap
process and instance identity rather than adding one set of CI secrets per VM.

Useful checks:

```bash
sudo systemctl status renderops-worker
sudo journalctl -u renderops-worker -n 200 --no-pager
curl -fsS http://127.0.0.1:9876/worker/health
df -Th /mnt/renderops-active
```

## CI/CD Behavior

`.github/workflows/ci.yml` formats, vets, builds, and tests all Go services and
the guest agent. `.github/workflows/deploy.yml` uses path filters so only
affected deployment targets run:

- control-plane changes publish control-plane/Caddy images and run the
  `control-plane` self-hosted job;
- orchestrator changes publish its image and run the `orchestrator` self-hosted
  job when enabled;
- worker changes build a binary and deploy over SSH;
- guest-agent/rootfs changes build and activate a versioned rootfs only when
  the workflow requests it.

A job displaying `Waiting for a runner` means no online self-hosted runner has
all requested labels; it is not an application-build failure.

## Production Topology Changes

Before production, replace the colocated development database and long-lived
static secrets with:

- managed PostgreSQL with private networking, backups, PITR, and monitoring;
- managed secret storage/workload identity;
- independent object-storage backup/checkpoint retention;
- image/bootstrap-based worker autoscaling;
- central metrics, logs, traces, and alerting.

See [../PLAN.md](../PLAN.md) for rollout gates and [../API.md](../API.md) for
public/private contracts.
