# RenderOps API Reference

This document describes the API implemented by
`backend/cmd/control-plane`. The development base URL is:

```text
https://dev-api.renderops.com
```

Examples use `$RENDEROPS_API_URL`, `$RENDEROPS_API_KEY`, and `$SANDBOX_ID`.

## Conventions

### Authentication

Every public management REST route except `GET /health` requires a bearer
credential:

```http
Authorization: Bearer ro_live_<api-key>
```

The dashboard may instead send an opaque Better Auth session token. API keys
are stored only as SHA-256 hashes and resolved with their owning profile. The
server, not the request, selects execution policy and billing configuration.

An API key may be rejected when it is missing, inactive, expired, unknown, or
its profile has no available balance. Successful key lookups are cached by the
control-plane process for 60 seconds.

The terminal WebSocket upgrade uses its short-lived single-use attachment
token instead. Preview traffic uses a signed preview token/cookie at the
preview gateway. Private service routes use the credentials documented below.

### Headers

```http
Content-Type: application/json
Authorization: Bearer <credential>
X-Request-ID: <optional caller-provided id>
```

The logging middleware returns an `X-Request-ID`. Structured responses also
include `request_id` where the handler supports it.

### Error bodies

Newer handlers return:

```json
{
  "status": "error",
  "code": "no_capacity",
  "message": "no healthy worker has sufficient capacity",
  "request_id": "4e245aae-1694-4dd5-b944-7180bf480fe8"
}
```

Some legacy session branches still return plain-text `http.Error` bodies.
Clients must use the HTTP status as the primary error signal and should not
depend on every error being JSON until the response envelope is unified.

### Sandbox states

The durable lifecycle may pass through:

```text
scheduling -> provisioning -> active -> paused -> resuming -> active
                                             \-> destroying -> destroyed
```

Failed lifecycle operations may set `error`. Public operations only succeed
when the current state permits the requested transition.

## Health

### `GET /health`

Authentication is not required.

```bash
curl "$RENDEROPS_API_URL/health"
```

```json
{
  "status": "ok",
  "message": "control plane is healthy",
  "role": "control-plane"
}
```

This proves that the HTTP process is responding. It is not a transitive check
of PostgreSQL, the orchestrator, every worker, or template readiness.

## Sandboxes

### `POST /session`

Creates a durable sandbox row, asks the orchestrator for a healthy compatible
worker, reserves worker capacity, boots a Firecracker microVM, and returns only
after the guest agent is ready.

```bash
curl -sS -X POST "$RENDEROPS_API_URL/session" \
  -H "Authorization: Bearer $RENDEROPS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "dependency-check",
    "image": "alpine",
    "size": "nano",
    "metadata": {"project": "docs"},
    "env": {"APP_ENV": "dev"},
    "network": {"internet": true},
    "idle_timeout_s": 120,
    "max_lifetime_s": 3600
  }'
```

Request fields:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `name` | string | no | Dashboard label; defaults to `sandbox` |
| `os` | string | no | Base OS; defaults to `alpine`. Choices: `GET /v1/catalog` |
| `template` | string | no | Software on top of the OS: `base` (default) or `agents` (Claude Code + Codex) |
| `image` | string | no | Legacy image id (`alpine`, `agents`), used only when `os` and `template` are unset; a value that disagrees with them is rejected |
| `metadata` | object | no | Caller-owned labels stored with the sandbox |
| `env` | object of strings | no | Environment injected into guest executions |
| `size` | string | no | `nano`, `small`, `medium` or `large`; defaults to `nano`. Each template runs at the sizes `GET /v1/catalog` lists for it |
| `resources` | object | no | Exact canonical shape; takes precedence over `size` |
| `network.internet` | boolean | no | Enables guest egress; defaults to `true` |
| `idle_timeout_s` | integer | no | Auto-pause interval; capped by policy/lifetime |
| `max_lifetime_s` | integer | no | Maximum lifetime; capped by server policy |

Image availability is worker capability-aware. A sandbox is scheduled only to
a healthy worker that advertises the requested image. The current published
platform image is `alpine`; additional image identifiers become usable after
their rootfs/template releases are published and loaded by workers.

Canonical shapes:

| Size | `vcpus` | `memory_mb` | `disk_gb` |
| --- | ---: | ---: | ---: |
| `nano` | 1 | 256 | 1 |
| `small` | 2 | 512 | 10 |
| `medium` | 2 | 1024 | 20 |

An explicit request must match one row exactly:

```json
{
  "resources": {
    "vcpus": 2,
    "memory_mb": 512,
    "disk_gb": 10
  }
}
```

Success: `201 Created`

```json
{
  "status": "success",
  "request_id": "4e245aae-1694-4dd5-b944-7180bf480fe8",
  "session": {
    "session_id": "3ecb0221-faf1-4976-a8ae-6fd6262de59a",
    "state": "active",
    "billing_model": "payg",
    "created_at": "2026-08-17T11:00:00Z",
    "last_used": "2026-08-17T11:00:00Z",
    "expires_at": "2026-08-17T11:02:00Z"
  },
  "limits": {
    "max_sessions": 100,
    "active_sessions": 0,
    "max_execution_ms": 30000,
    "idle_timeout_ms": 120000
  },
  "tenant": {
    "tenant_id": "<user-id>",
    "billing_model": "payg"
  }
}
```

`limits.active_sessions` is currently a placeholder and is always `0`; do not
use it for quota decisions.

Important failures:

| Status | Code | Meaning |
| ---: | --- | --- |
| `400` | `invalid_resources` | The size or resource tuple is unsupported |
| `403` | `sessions_not_allowed` | Current execution policy disables sessions |
| `402` | n/a | Profile has insufficient balance |
| `503` | `no_capacity` | No healthy worker can admit the sandbox |
| `503` | `scheduler_busy` | Durable placement remained contended after retries |
| `500` | `provision_failed` | Worker boot or lifecycle coordination failed |

### `GET /session`

Lists non-destroyed sandboxes owned by the authenticated user.

```bash
curl -sS "$RENDEROPS_API_URL/session" \
  -H "Authorization: Bearer $RENDEROPS_API_KEY"
```

```json
{
  "sandboxes": [
    {
      "id": "3ecb0221-faf1-4976-a8ae-6fd6262de59a",
      "name": "dependency-check",
      "state": "active",
      "billing_model": "payg",
      "vcpus": 1,
      "memory_mb": 256,
      "disk_gb": 1,
      "idle_timeout_ms": 120000,
      "metadata": {"project": "docs"},
      "created_at": "2026-08-17T11:00:00Z"
    }
  ]
}
```

### `GET /session/{sandboxID}`

Returns lifecycle, limits, and aggregate execution statistics for an owned
sandbox. Missing and foreign IDs both return `404`.

```bash
curl -sS "$RENDEROPS_API_URL/session/$SANDBOX_ID" \
  -H "Authorization: Bearer $RENDEROPS_API_KEY"
```

```json
{
  "status": "success",
  "request_id": "f4095325-ae50-43ae-a4ca-ff6431d5785a",
  "session": {
    "session_id": "3ecb0221-faf1-4976-a8ae-6fd6262de59a",
    "state": "active",
    "billing_model": "payg",
    "created_at": "2026-08-17T11:00:00Z",
    "last_used": "2026-08-17T11:01:00Z",
    "expires_at": "2026-08-17T11:03:00Z"
  },
  "limits": {
    "max_sessions": 100,
    "active_sessions": 0,
    "max_execution_ms": 30000,
    "idle_timeout_ms": 120000
  },
  "stats": {
    "run_count": 2,
    "total_execution_ms": 138.4,
    "last_exit_code": 0
  },
  "tenant": {
    "tenant_id": "<user-id>",
    "billing_model": "payg"
  }
}
```

### `POST /session/{sandboxID}/run`

Executes source code through the guest agent. Supported language identifiers
are `python`, `node`, and `bash`. Python and Node use persistent runtime bridges
while the sandbox remains active; runtime process state is reset across a
pause/resume boundary, but writable-disk changes persist.

```bash
curl -sS -X POST "$RENDEROPS_API_URL/session/$SANDBOX_ID/run" \
  -H "Authorization: Bearer $RENDEROPS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "language": "python",
    "code": "print(6 * 7)",
    "timeout": 10
  }'
```

```json
{
  "status": "success",
  "request_id": "eb3d0130-2de1-4f86-8651-038b9a747dab",
  "session_id": "3ecb0221-faf1-4976-a8ae-6fd6262de59a",
  "result": {
    "stdout": "42\n",
    "stderr": "",
    "exit_code": 0,
    "termination_reason": "success",
    "duration_ms": 54.1,
    "guest_duration_ms": 17.8
  },
  "usage": {
    "execution_time_ms": 17.8,
    "queue_wait_ms": 0,
    "timeout_limit_ms": 30000
  },
  "tenant": {
    "tenant_id": "<user-id>",
    "billing_model": "payg"
  },
  "session": {
    "state": "active",
    "last_used": "2026-08-17T11:01:00Z",
    "expires_at": "2026-08-17T11:03:00Z",
    "run_count": 1
  }
}
```

A completed process with a non-zero exit code still returns HTTP `200`, but
the body has `status: "error"` and an `execution_failed` detail.

### `POST /session/{sandboxID}/exec`

Runs a shell command inside the persistent sandbox filesystem.

```bash
curl -sS -X POST "$RENDEROPS_API_URL/session/$SANDBOX_ID/exec" \
  -H "Authorization: Bearer $RENDEROPS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"command":"cd /workspace && node --version","timeout":10}'
```

The response uses the same envelope as `/run`. A non-zero command exit uses
`error.code: "exec_failed"`.

`timeout` is in seconds, as for `/run`: unset uses the policy default and
larger values are capped at the policy max (`limits.max_execution_ms`). A
paused sandbox is resumed first, exactly like `POST /session/{id}/resume`, so
the request also waits for the resume.

### `POST /session/{sandboxID}/pause`

Snapshots VM/device state, memory, and the writable disk, releases active CPU
and memory admission, and keeps durable disk allocation for resume.

```bash
curl -sS -X POST "$RENDEROPS_API_URL/session/$SANDBOX_ID/pause" \
  -H "Authorization: Bearer $RENDEROPS_API_KEY"
```

```json
{
  "status": "success",
  "session_id": "3ecb0221-faf1-4976-a8ae-6fd6262de59a",
  "state": "paused"
}
```

When Blob checkpointing is configured, pause uploads immutable VM state and
memory plus a chunked writable-disk generation before local artifacts may be
removed. Unchanged writable chunks are reused across generations.

### `POST /session/{sandboxID}/resume`

Reserves capacity on the sandbox's assigned worker, restores the most recent
checkpoint, reconnects guest communication, and returns the sandbox to
`active`.

```bash
curl -sS -X POST "$RENDEROPS_API_URL/session/$SANDBOX_ID/resume" \
  -H "Authorization: Bearer $RENDEROPS_API_KEY"
```

```json
{
  "status": "success",
  "session_id": "3ecb0221-faf1-4976-a8ae-6fd6262de59a",
  "state": "active"
}
```

The current architecture resumes on the existing worker placement. Cross-host
resume requires portable template/checkpoint compatibility and placement
changes described in [PLAN.md](PLAN.md).

### `DELETE /session/{sandboxID}`

Destroys the microVM and writable disk, releases the placement and worker
reservation, bills any final active runtime, and leaves the durable row in the
`destroyed` state for history/audit purposes.

```bash
curl -i -X DELETE "$RENDEROPS_API_URL/session/$SANDBOX_ID" \
  -H "Authorization: Bearer $RENDEROPS_API_KEY"
```

Success: `204 No Content`.

## Interactive Terminals

Terminal creation is a two-step protocol. First authenticate with the REST API
and create a guest PTY. Then consume the returned single-use token when opening
the WebSocket.

### `POST /v1/sandboxes/{sandboxID}/terminals`

```bash
curl -sS -X POST \
  "$RENDEROPS_API_URL/v1/sandboxes/$SANDBOX_ID/terminals" \
  -H "Authorization: Bearer $RENDEROPS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"shell":"/bin/bash","columns":120,"rows":32}'
```

Only `/bin/bash` is currently supported. Columns must be `20..500`; rows must
be `5..200`. Defaults are 120 x 32.

Success: `201 Created`, `Cache-Control: no-store`

```json
{
  "status": "success",
  "request_id": "a782888f-d609-482c-9910-271f80f650aa",
  "terminal": {
    "terminal_id": "term_PF9lwt4Vsn2C7DCBtqu3kNQn",
    "sandbox_id": "3ecb0221-faf1-4976-a8ae-6fd6262de59a",
    "state": "ready",
    "created_at": "2026-08-17T11:10:00Z",
    "token_expires_at": "2026-08-17T11:11:00Z"
  },
  "websocket_path": "/v1/terminals/term_PF9lwt4Vsn2C7DCBtqu3kNQn",
  "attachment_token": "<single-use-token>",
  "expires_in": 59
}
```

### `GET /v1/terminals/{terminalID}?token={attachmentToken}`

Upgrade this URL to WebSocket. The attachment token is valid for 60 seconds
and is consumed exactly once. It replaces normal bearer auth for this upgrade.
The request origin must match `TERMINAL_ALLOWED_ORIGINS` when that allowlist is
configured.

Browser to server frames:

| WebSocket frame | Payload | Meaning |
| --- | --- | --- |
| binary | raw bytes | PTY stdin |
| text | `{"type":"resize","columns":120,"rows":32}` | Resize PTY |
| text | `{"type":"close"}` | Close terminal |

Server to browser frames:

| WebSocket frame | Payload | Meaning |
| --- | --- | --- |
| text | `{"type":"ready"}` | Worker stream is attached |
| binary | raw bytes | PTY stdout/stderr stream |
| text | `{"type":"exit","exit_code":0}` | Shell exited |
| text | `{"type":"error","message":"..."}` | Terminal failed |

The control plane bridges this WebSocket to the assigned worker's internal
gRPC terminal stream. Closing either side closes the guest PTY.

## Browser IDE

### `POST /v1/sandboxes/{sandboxID}/ide/sessions`

Starts OpenVSCode Server inside an active sandbox and creates a browser IDE
authorization handoff. The sandbox must belong to the authenticated user.

```bash
curl -sS -X POST \
  "$RENDEROPS_API_URL/v1/sandboxes/$SANDBOX_ID/ide/sessions" \
  -H "Authorization: Bearer $RENDEROPS_API_KEY"
```

Success: `201 Created`, `Cache-Control: no-store`

```json
{
  "url": "https://3001-3ecb0221-faf1-4976-a8ae-6fd6262de59a.dev-sandbox.renderops.com/?ro_auth=<single-use-token>",
  "expires_at": "2026-08-17T11:11:00Z"
}
```

Open the returned URL within 60 seconds. The preview gateway consumes the
single-use token, sets a host-only `__Host-renderops_ide` Secure, HttpOnly
cookie, and redirects to the same URL without the token. The IDE cookie lasts
one hour and is valid only for that sandbox hostname and port. It is stripped
before proxying the request to the worker and OpenVSCode Server.

## Catalog

### `GET /v1/catalog`

Lists what `POST /session` accepts: each OS, the templates available on it with
the sizes each template runs at, and the size specs.

```json
{
  "os": [{
    "id": "alpine", "name": "Alpine Linux 3.22",
    "templates": [
      {"id": "base", "name": "Base", "description": "Python 3.12, Node 22, git", "sizes": ["nano", "small", "medium"], "image": "alpine"},
      {"id": "agents", "name": "Agents", "description": "Base plus Claude Code and Codex", "sizes": ["large"], "image": "agents"}
    ]
  }],
  "sizes": [{"name": "nano", "vcpus": 1, "memory_mb": 256, "disk_gb": 1}, "..."]
}
```

`image` is the image id a sandbox of that template records (the legacy `image`
create field), so a client can show an existing sandbox's OS and template.

A create request for a template at a size it does not list fails with `400
size_not_available`; an unknown OS or template fails with `400 invalid_template`.

`agents` lists the coding agents a session can run in the `agents` template and
the model provider each uses. `available: false` (Codex, GPT models) is shown but
cannot be granted yet:

```json
"agents": [
  {"id": "claude-code", "name": "Claude Code", "provider": "anthropic", "template": "agents", "available": true},
  {"id": "codex", "name": "Codex", "provider": "openai", "template": "agents", "available": false, "note": "GPT models are coming soon"}
]
```

## Model Access

An agent in a sandbox calls its model through the platform; provider keys never
enter the sandbox. The sandbox owner saves their provider credentials on the
dashboard's Secrets page (stored encrypted). Granting access opens a model proxy
on the sandbox's own gateway (`http://172.16.0.1:7081`), reachable only from
inside that sandbox. The guest presents a random proxy token; the worker forwards
to the control plane, which adds the saved key and allows only the saved model.
Pause and destroy revoke the grant.

### `POST /v1/sandboxes/{sandboxID}/model-access`

The sandbox must be active and owned by the caller.

```json
{"provider": "anthropic"}
```

`201 Created`:

```json
{
  "status": "success",
  "provider": "anthropic",
  "model": "claude-opus-5-5",
  "base_url": "http://172.16.0.1:7081",
  "token": "<proxy token>",
  "expires_at": "2026-10-16T16:00:00Z",
  "env": {
    "ANTHROPIC_BASE_URL": "http://172.16.0.1:7081",
    "ANTHROPIC_AUTH_TOKEN": "<proxy token>",
    "ANTHROPIC_MODEL": "claude-opus-5-5",
    "ANTHROPIC_DEFAULT_OPUS_MODEL": "claude-opus-5-5",
    "ANTHROPIC_DEFAULT_SONNET_MODEL": "claude-opus-5-5",
    "ANTHROPIC_DEFAULT_HAIKU_MODEL": "claude-opus-5-5",
    "ANTHROPIC_SMALL_FAST_MODEL": "claude-opus-5-5",
    "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1"
  }
}
```

Pass `env` to the agent process (for example in an exec command). Grants last
4 hours; grant again to extend. Errors: `409 credentials_missing` (nothing saved
on the Secrets page), `409 sandbox_not_active`, `400 provider_unavailable`
(`openai`), `503 model_access_unavailable` (server has no
`SECRETS_ENCRYPTION_KEY`).

### `DELETE /v1/sandboxes/{sandboxID}/model-access`

Revokes the grant. `204 No Content`; idempotent.

## Port Previews

### `POST /v1/sandboxes/{sandboxID}/ports/{port}/preview`

Creates a signed private link to an HTTP service listening inside an active
sandbox. The port must be `1..65535`.

```bash
curl -sS -X POST \
  "$RENDEROPS_API_URL/v1/sandboxes/$SANDBOX_ID/ports/3000/preview" \
  -H "Authorization: Bearer $RENDEROPS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"expires_in_seconds":3600}'
```

TTL must be between one second and 24 hours and defaults to one hour.

```json
{
  "url": "https://3000-3ecb0221-faf1-4976-a8ae-6fd6262de59a.dev-sandbox.renderops.com/?_renderops_token=<signed-token>",
  "expires_at": "2026-08-17T12:10:00Z"
}
```

On first use, the preview gateway verifies the HMAC token, stores it in a
secure HttpOnly cookie, and redirects to remove the query token. It resolves
the current placement and proxies to the sandbox port through the private
worker API. Missing, invalid, expired, or mismatched credentials return `404`
to avoid revealing sandbox existence. Public/no-auth previews are not
implemented.

## Private Service API

These routes are deployment contracts, not SDK endpoints. They must remain on
the private network.

### Orchestrator (`:8090`)

Worker calls send `X-Orchestrator-Token: $WORKER_TOKEN`:

| Method | Route | Purpose |
| --- | --- | --- |
| `PUT` | `/internal/workers/{workerID}` | Register/update endpoint and allocatable capacity |
| `POST` | `/internal/workers/{workerID}/heartbeat` | Publish authoritative free/reserved capacity |
| `POST` | `/internal/workers/{workerID}/draining` | Include/exclude worker from new placement |
| `POST` | `/internal/workers/{workerID}/sandboxes/{sandboxID}/state` | Report lifecycle state |

Control-plane calls send
`X-Orchestrator-Token: $ORCHESTRATOR_TOKEN`:

| Method | Route | Purpose |
| --- | --- | --- |
| `POST` | `/internal/placements` | Reserve a healthy worker placement |
| `GET` | `/internal/placements/{sandboxID}` | Resolve worker ID and endpoint |
| `DELETE` | `/internal/placements/{sandboxID}` | Release durable placement |
| `POST` | `/internal/sandboxes` | Place and provision a sandbox |
| `POST` | `/internal/sandboxes/{sandboxID}/pause` | Pause on assigned worker and update placement |
| `POST` | `/internal/sandboxes/{sandboxID}/resume` | Reserve and resume on assigned worker |
| `DELETE` | `/internal/sandboxes/{sandboxID}` | Destroy and release placement |

### Worker (`:9876`)

`GET /worker/health` is unauthenticated for infrastructure health probes. All
other worker routes require `X-Worker-Token: $WORKER_TOKEN`.

| Method | Route | Purpose |
| --- | --- | --- |
| `GET` | `/worker/capacity` | Current worker-authoritative capacity snapshot |
| `POST` | `/worker/sandbox` | Atomically reserve capacity and boot a sandbox |
| `POST` | `/worker/sandbox/{id}/run` | Execute source code |
| `POST` | `/worker/sandbox/{id}/exec` | Execute a shell command |
| `POST` | `/worker/sandbox/{id}/pause` | Snapshot and release active compute |
| `POST` | `/worker/sandbox/{id}/resume` | Reserve compute and restore |
| `DELETE` | `/worker/sandbox/{id}` | Destroy local state and release capacity |

Terminal create/attach/close uses the internal worker gRPC service on the same
private listener. Preview traffic uses the worker's sandbox-port proxy path.

### Raw usage ingestion

Workers send minute-bucketed, idempotent samples to the control plane:

```http
POST /internal/usage/meters
Authorization: Bearer <WORKER_TOKEN>
Content-Type: application/json
```

```json
{
  "samples": [
    {
      "worker_id": "worker-1",
      "sandbox_id": "3ecb0221-faf1-4976-a8ae-6fd6262de59a",
      "bucket": "2026-08-17T11:10:00Z",
      "vcpu_seconds": 60,
      "ram_gb_seconds": 15,
      "disk_gb_seconds": 60
    }
  ]
}
```

Batches contain 1 to 1000 samples. The bucket must be an exact UTC minute
boundary and values cannot be negative. Success is `204 No Content`. This
endpoint records raw usage; pricing and balance deduction are separate control
plane responsibilities.
