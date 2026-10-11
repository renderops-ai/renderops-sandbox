# RenderOps Architecture

This is the current architecture implemented in the repository. It replaces
the earlier single-process API/queue/pool design.

## Planes and Services

RenderOps has two logical planes. A plane is an ownership boundary, not
necessarily one executable.

### Management plane

- **Platform/dashboard** owns user interaction, Better Auth, API-key creation,
  and the Drizzle schema/migration history.
- **Control plane** authenticates requests, applies execution and billing
  policy, owns the public API, and records sandbox/usage/audit data.
- **Orchestrator** owns worker discovery, health, placement, and lifecycle
  coordination. It is private and is not an execution proxy.
- **PostgreSQL** is the durable source of truth for identities, policies,
  sandbox lifecycle, placements, usage, and audit history.
- **Preview gateway** authenticates signed preview URLs and proxies web traffic
  to the sandbox's current worker.

### Execution plane

- **Worker service** is one process per KVM host. It atomically admits work and
  owns Firecracker processes, cgroups, networking, disk paths, snapshots, and
  local session state.
- **Firecracker microVM** is one isolated VM per active sandbox.
- **Guest agent** is PID 1 inside the microVM. It serves code execution,
  commands, runtime reset, and PTYs over Firecracker vsock.

Workers do not receive PostgreSQL credentials. They publish heartbeats and raw
usage through authenticated private APIs.

## Create Flow

```text
1. Client -> POST /session -> control plane
2. Auth middleware resolves API key or Better Auth session.
3. Control plane validates canonical size, network, and timeout policy.
4. Control plane inserts sandboxes(state=scheduling).
5. Control plane -> POST /internal/sandboxes -> orchestrator.
6. Orchestrator transaction samples healthy compatible worker candidates,
   selects the best score, and writes host_id + state=provisioning.
7. Orchestrator -> POST /worker/sandbox -> selected worker.
8. Worker atomically reserves CPU, RAM, disk, and a network slot in memory.
9. Worker restores a template (or builds in development), creates cgroup and
   network state, starts Firecracker, and waits for the guest agent.
10. Worker returns active; orchestrator changes provisioning -> active.
11. Control plane enriches the row with API key, name, and metadata, records
    audit/usage events, and returns 201.
```

The durable sandbox row exists before capacity is reserved. A failed boot
releases the worker reservation and marks/releases the placement. A timeout is
treated as ambiguous: cleanup is attempted by sandbox ID before capacity is
made available again.

## Placement and Capacity

`worker_hosts` stores the latest registered endpoint, pool, allocatable
resources, heartbeat time, draining flag, and reported reservations.
`sandboxes.host_id` stores the durable placement.

Placement currently:

1. filters to the requested pool, recent heartbeat, non-draining state, static
   capacity, free slots, and sufficient projected CPU/RAM/disk;
2. randomly samples up to three eligible workers;
3. scores each candidate by its worst projected CPU or memory utilization;
4. atomically writes one placement using PostgreSQL row locking;
5. retries short database contention with bounded jitter.

This is a best-of-K scheduler. Random sampling prevents every concurrent
scheduler request from targeting one globally "best" worker. The database
choice is advisory: the worker's mutex-protected admission map is the final
authority and may reject stale placement with `no_capacity`. The orchestrator
then excludes that worker and tries another.

Each worker reservation is created before VM boot. It simultaneously increases
reserved resources and decreases free resources. There is not currently a
second worker-side `pending` counter: failed boot removes the reservation;
successful boot keeps it. The lifecycle state `provisioning` is durable status,
not another capacity counter.

Configured defaults in deployment are CPU overcommit 2x and memory overcommit
1x. Policy belongs to the orchestrator/worker environment, never the client.

## Execution Flow

```text
Client
  -> control plane authentication and ownership check
  -> orchestrator GET placement
  -> selected worker /run or /exec
  -> persistent worker-to-guest vsock connection
  -> guest runtime
  -> worker
  -> control plane records run/log/usage and billing data
  -> client response
```

Execution bypasses the orchestrator after placement resolution. This keeps the
orchestrator out of stdout/stderr latency and isolates scheduling load from
runtime traffic.

Python and Node executions use persistent guest runtime bridges while active.
Bash commands are direct processes. Pause/resume resets in-memory language
runtimes so cross-language behavior is consistent; filesystem changes remain
on the writable disk.

## Pause, Resume, and Destroy

### Pause

The worker locks the session, rechecks auto-pause idleness when applicable,
closes PTYs and the persistent vsock connection, and snapshots Firecracker VM
state and memory. It keeps the sandbox writable disk, tears down Firecracker,
the cgroup, and active network namespace, and reports `paused`.

Capacity accounting for a paused sandbox retains its sandbox/disk reservation
and therefore still consumes one `max_sandboxes` slot, but releases active CPU
and RAM. The durable row remains placed on the worker.

When checkpoint storage is enabled, pause writes:

- VM/device state;
- guest memory;
- a generation manifest for the writable disk;
- only changed 4 MiB content-addressed writable chunks.

The active generation pointer is published last. Partial uploads therefore do
not replace the last valid checkpoint.

### Resume

The orchestrator verifies the existing placement is healthy and atomically
reserves active CPU/RAM again. The worker restores local artifacts or downloads
the active checkpoint generation, recreates the expected network/vsock device
paths, resumes Firecracker, resets guest runtimes, and reports `active`.

Current resume is placement-affine. A portable cross-worker resume is planned,
but requires identical Firecracker/kernel/CPU-template compatibility and a
durable writable-disk/cache design.

### Destroy

Destroy is idempotent by sandbox ID. It closes terminals, destroys the VM,
removes local writable/checkpoint state, releases the worker admission record,
releases placement, and changes the durable row to `destroyed`. The row remains
for audit and billing history, but list APIs exclude it.

## Writable Storage

The worker accesses writable images through the `writabledisk.Store`
abstraction. The current implementation is filesystem-backed and the directory
is configured with `ACTIVE_DISK_DIR`. Production workers point it at a mounted
per-worker Azure Managed Disk or equivalent block volume rather than the OS
disk.

Each sandbox gets a sparse ext4 image with a logical quota matching `disk_gb`.
Logical size is not physical allocation: an empty image consumes filesystem
metadata and grows as blocks are written. XFS with reflink enabled is preferred
for the host volume because golden writable seeds can be copy-on-write cloned.

`ACTIVE_DISK_CLONE_MODE` controls behavior:

- `required`: fail if a reflink clone cannot be created;
- `auto`: request a reflink and fall back to sparse copy;
- `copy`: always use a sparse copy.

Active reads and writes still use the worker's mounted block volume. Blob
storage is the durable pause/template store, not a directly mounted POSIX disk.
The chunked checkpoint layer avoids uploading an unchanged logical disk on
every pause, but it is not yet a lazy NBD/page cache for active VMs.

## Templates

Templates are immutable Firecracker artifacts identified by a manifest. A
template consists of VM state, memory, a golden writable seed, and compatibility
metadata/digests for the Firecracker, kernel, rootfs, CPU/architecture, and
resource shape.

`backend/cmd/template-builder` builds each canonical size and publishes
artifacts through `internal/template.ArtifactStore`. Blob and local directory
stores implement the interface. The manifest repository publishes immutable
versions and flips a small active pointer only after validation succeeds.

Workers support build/prebuilt template sources. A prebuilt worker downloads,
checks compatibility and checksums, caches immutable artifacts locally, and
constructs Firecracker snapshot templates from those paths. Workers should not
advertise a template as ready until verification completes.

## Terminal Flow

```text
Browser -> authenticated POST create terminal -> control plane
Control plane -> worker gRPC OpenTerminal -> guest-agent PTY
Control plane <- worker confirms PTY
Browser <- terminal ID + 60-second single-use attachment token
Browser -> WebSocket attach with token -> control plane
Control plane <-> worker bidirectional gRPC stream <-> guest PTY
```

Binary WebSocket frames are terminal bytes. Text frames are resize/close and
ready/exit/error controls. Multiple terminal IDs may target one sandbox; they
share its filesystem and processes but each owns a separate PTY. Closing the
WebSocket closes that PTY.

Terminal authorization metadata is currently process-local to one control
plane instance. Horizontal control-plane scaling needs a shared short-lived
token/session store or a fully self-contained single-use-token mechanism.

## Preview Flow

The control plane creates an HMAC-signed token scoped to user, sandbox, port,
and expiry. Its URL format is:

```text
https://<port>-<sandbox-uuid>.<PREVIEW_DOMAIN>/?_renderops_token=<token>
```

The wildcard hostname resolves to the preview gateway. The gateway validates
the token, moves it to a secure HttpOnly cookie, resolves the current worker,
and reverse-proxies HTTP/WebSocket traffic over the private network. Invalid
requests return `404`. The preview gateway cannot infer open ports and does not
make previews public by default.

## Database Ownership

The platform repository's Drizzle schema and migrations are authoritative.
Go services must not run DDL.

| Table | Primary writer/use |
| --- | --- |
| `profiles` | Platform account/balance; control plane reads and debits |
| `api_keys` | Platform creates/revokes; control plane authenticates |
| `session` | Better Auth; control plane resolves dashboard sessions |
| `execution_policies` | Singleton server-side execution limits |
| `pricing_rates` | Singleton server-side PAYG rates |
| `sandboxes` | Control plane lifecycle row; orchestrator placement transitions |
| `worker_hosts` | Orchestrator registration, heartbeat, drain, capacity |
| `sandbox_runs` | Control-plane run/exec history |
| `sandbox_logs` | Bounded stdout/stderr records for dashboard history |
| `usage_logs` | Per-operation audit-style usage/cost record |
| `usage_meters` | Idempotent minute-bucketed CPU/RAM/disk accumulation |
| `audit_events` | User/API-key lifecycle audit trail |

`usage_logs` records operations such as `session_run`. `usage_meters` aggregates
resource-seconds for billing. `sandbox_logs` is output history; it is not the
live PTY stream.

## Deployment Topology

```text
Public control-plane VPS
  Caddy :80/:443
  platform
  control plane :8081 (internal)
  preview gateway :8082 (internal)
  PostgreSQL (development topology)

Private orchestrator server
  orchestrator :8090

KVM worker hosts
  renderops-worker systemd service :9876 private
  Firecracker + guest microVMs
  mounted active-disk filesystem
```

The control-plane and orchestrator are container images published to GHCR.
Workers are Linux binaries installed by the deploy workflow because they need
direct KVM, cgroup, network, and filesystem access.

## Known Constraints

- Public error bodies are not yet uniform JSON.
- `active_sessions` in API limit responses is not yet calculated.
- Placement sampling uses PostgreSQL `ORDER BY random()` and will need a more
  scalable candidate index/cache for a very large worker fleet.
- Control-plane API-key cache is in-process and caches balance for 60 seconds.
- Terminal attachment state is in-process and not horizontally shared.
- Lifecycle transport is private HTTP; terminal streaming is gRPC. Versioned
  lifecycle gRPC can be introduced behind the existing service interfaces.
- Cross-host resume and lazy remote block reads are not implemented.
- The development Compose stack colocates PostgreSQL with public services; a
  production rollout should use managed PostgreSQL and managed secrets.
