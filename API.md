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

Optional `"quiet": true` returns the command's output in the response only:
it is not copied into the sandbox's log stream (the dashboard's Logs view,
kept 48 hours) or into usage records. Use it for reads you do not want logged,
such as file contents. The run itself (command, exit code, duration) is still
recorded. A sandbox started from a guest image that predates `quiet` still
logs the output.

```bash
curl -sS -X POST "$RENDEROPS_API_URL/session/$SANDBOX_ID/exec" \
  -H "Authorization: Bearer $RENDEROPS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"command":"git -C /workspace/repo diff --stat","timeout":10,"quiet":true}'
```

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

Optionally the terminal can start in a directory, load an env file and run a
command instead of a prompt (the spawn CLI opens `claude` this way):

```json
{
  "columns": 120, "rows": 32,
  "command": ["claude", "--continue"],
  "cwd": "/workspace/repo",
  "env_file": "/var/lib/spawn/cli/env"
}
```

| Field | Rules |
| --- | --- |
| `command` | argv array, run with `exec` (no shell parsing). 1 to 256 arguments, 16 KiB in total, UTF-8 without NUL; `command[0]` must not be empty or start with `-`. Omit it for a shell. |
| `cwd` | Absolute, clean path: `/workspace` or below. |
| `env_file` | Absolute, clean path below `/var/lib/spawn/`. Sourced with every assignment exported (`set -a`), so both `KEY=value` and `export KEY=value` lines work. |

All three are optional and independent; with only `cwd`/`env_file` the user
gets a prompt there with the env loaded. The terminal ends when the command
exits, with its exit code (`exit` frame). Nothing is typed into the PTY, so
neither the env file nor the command is echoed: the worker hands the startup to
the login shell as a one-shot `PROMPT_COMMAND` (user values only as base64
literals), which bash runs after the login profile and unsets before the
command starts. Exit code `125` means the directory or env file could not be
used (a `spawn: ...` line says which); `127` means the command could not be
started. The shell's `HOME` is the guest's (`/` unless the sandbox env sets
it); set `HOME=/root` in the env file for tools that keep state in `~`.

Errors: `400 invalid_terminal_options` (with the reason), and `502
terminal_command_unsupported` when the sandbox's worker predates terminal
commands (the terminal is not left open).

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

## Files

Read and write files in a sandbox's `/workspace` (the dashboard's Code tab).
All four routes need the sandbox's owner (`404 sandbox_not_found` otherwise)
and an active sandbox: a paused one is never resumed by these routes and
answers `409 sandbox_not_active`, so the caller can offer Resume. Responses
carry `Cache-Control: no-store`.

Paths (`root`, `path`) must be absolute, already clean (no `.` or `..`
segments, no `//`, no trailing `/`), free of NUL bytes, at most 4096 bytes,
and inside `/workspace` after the sandbox resolves symlinks; anything else is
`400 invalid_path`. Writes are also refused under any `.git` directory.
`rev` and `base` are full 40-character lower-case commit shas
(`400 invalid_rev`).

Reads (tree, content, changes) do not count as sandbox activity: they do not
reset the idle timer, so a sandbox nobody uses pauses on time even while the
Code tab polls. Saves do count, like any command. Every request runs a short
script through the sandbox's shell as a quiet exec, so file contents read or
written here do not appear in the sandbox's output log. (A sandbox started
from a guest image that predates quiet execs still logs them, as base64.)

### `GET /v1/sandboxes/{sandboxID}/files/tree?root={dir}`

Lists every file under `root`, relative to it. In a git work tree: tracked
files plus untracked files that are not ignored (`git ls-files -c`, then
`git ls-files -o --exclude-standard`; tracked files deleted from the work tree
are left out). Elsewhere: every non-directory entry except inside `.git`,
`node_modules`, `.venv`, `__pycache__`, `build` and `dist` directories. At most
20 000 entries, sorted; `truncated` says more exist.

```bash
curl -sS "$RENDEROPS_API_URL/v1/sandboxes/$SANDBOX_ID/files/tree?root=/workspace/repo" \
  -H "Authorization: Bearer $RENDEROPS_API_KEY"
```

```json
{
  "root": "/workspace/repo",
  "git": true,
  "files": [".gitignore", "app/src/Main.kt", "README.md"],
  "truncated": false
}
```

Errors: `404 not_found` (root missing), `400 not_a_directory`.

### `GET /v1/sandboxes/{sandboxID}/files/content?path={file}[&rev={sha}]`

Returns a file of at most 2 MiB. With `rev`, the file as it is in that commit
of the git repository containing `path` (the file may since have been deleted
from the work tree).

```bash
curl -sS "$RENDEROPS_API_URL/v1/sandboxes/$SANDBOX_ID/files/content?path=/workspace/repo/app/src/Main.kt" \
  -H "Authorization: Bearer $RENDEROPS_API_KEY"
```

```json
{
  "path": "/workspace/repo/app/src/Main.kt",
  "size": 1234,
  "sha256": "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
  "binary": false,
  "content": "package app\n...",
  "mode": "100644"
}
```

`sha256` is the hex sha256 of the file's bytes; send it back as
`expected_sha256` when saving. `mode` is `100644` or `100755` (with `rev`, the
mode git recorded). A file with a NUL byte in its first 8 KiB, or that is not
valid UTF-8, has `"binary": true` and no `content`. A symlink in the work tree
is followed to its target, which must also be inside `/workspace`; with `rev`
the target's path is read from that commit.

Errors: `413 file_too_large` with the file's `size`, `404 file_not_found`,
`400 not_a_file` (directory, FIFO, device), and with `rev`:
`400 not_a_git_repo`, `404 rev_not_found` (commit not in the repository),
`404 not_in_revision` (no such file in that commit).

```json
{"status":"error","code":"file_too_large","message":"file is larger than 2 MiB","request_id":"...","size":3145728}
```

### `PUT /v1/sandboxes/{sandboxID}/files/content`

Saves a file of at most 2 MiB.

```bash
curl -sS -X PUT "$RENDEROPS_API_URL/v1/sandboxes/$SANDBOX_ID/files/content" \
  -H "Authorization: Bearer $RENDEROPS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"path":"/workspace/repo/app/src/Main.kt","content":"package app\n","expected_sha256":"9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"}'
```

```json
{
  "path": "/workspace/repo/app/src/Main.kt",
  "size": 12,
  "sha256": "75d99e22087438b67ab1768073505b6ad05fa235b57f02efe129400534b6053c"
}
```

`expected_sha256` is the `sha256` the editor loaded (any case). If the file
changed since, the save is refused with `409 conflict` and the file's current
`sha256` (`null` when it was deleted). `null` (or omitting it) creates a new
file and is refused with `409 exists` if the file is there.

The write is atomic: the content goes to a temporary file in the same
directory, which gets the existing file's permissions (new files `0644`) and
is renamed over the file, or hard-linked into place for a new file. The sha is
compared just before the write and again right before the rename; a change in
the microseconds between that last check and the rename is not detected
(nothing in the sandbox locks files against the agent). Content over 192 KiB
(256 KiB as base64) is uploaded in chunks to `/var/tmp/spawn-files` first and
moved in by the final step; a 2 MiB save takes a few seconds.

Errors: `413 file_too_large` (with `size` when known), `404 parent_not_found`
(the directory does not exist), `400 not_a_file`, `400 invalid_path` (also
under `.git`), `400 invalid_sha256`, `400 invalid_request` (body is not
`{path, content, expected_sha256}`; `content` is required).

```json
{"status":"error","code":"conflict","message":"file changed since it was loaded","request_id":"...","sha256":"2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae"}
```

### `GET /v1/sandboxes/{sandboxID}/files/changes?root={dir}&base={sha}`

What changed under `root` since commit `base`: the work tree (uncommitted
edits included) against `base`, `git diff --find-renames <base>`, plus
untracked files that are not ignored. Paths are relative to `root`.

```bash
curl -sS "$RENDEROPS_API_URL/v1/sandboxes/$SANDBOX_ID/files/changes?root=/workspace/repo&base=4b6299a1c0e2d3f4a5b6c7d8e9f0a1b2c3d4e5f6" \
  -H "Authorization: Bearer $RENDEROPS_API_KEY"
```

```json
{
  "base": "4b6299a1c0e2d3f4a5b6c7d8e9f0a1b2c3d4e5f6",
  "head": "f0ccf0f2b1a0c9d8e7f6a5b4c3d2e1f0a9b8c7d6",
  "files": [
    {"path": "app/src/Main.kt", "status": "M", "additions": 12, "deletions": 3},
    {"path": "app/src/New.kt", "from": "app/src/Old.kt", "status": "R", "additions": 1, "deletions": 1},
    {"path": "docs/notes.md", "status": "?", "additions": 0, "deletions": 0}
  ],
  "truncated": false
}
```

`status` is `A` (added), `M` (modified, including type changes), `D`
(deleted), `R` (renamed; `from` is the old path) or `?` (untracked).
`additions`/`deletions` are line counts; they are 0 for binary and untracked
files. `head` is the current `HEAD` (empty before the first commit). At most
2 000 files; `truncated` says more exist.

Errors: `400 not_a_git_repo`, `404 rev_not_found` (`base` is not in the
repository), `404 not_found`, `400 not_a_directory`.

### `GET /v1/sandboxes/{sandboxID}/files/patch?root={dir}&base={sha}`

Everything that changed under `root` since commit `base`, as one patch to `git
apply`: `git diff --binary --find-renames <base>` of the work tree, so commits
since `base`, staged and unstaged edits, deletions, and untracked files that
are not ignored (as new files). Untracked nested repositories are left out.
The repository's own index is never written: untracked files are added
intent-to-add to a copy of it. Paths are relative to `root`.

```bash
curl -sS -D headers.txt -o agent.patch \
  "$RENDEROPS_API_URL/v1/sandboxes/$SANDBOX_ID/files/patch?root=/workspace/repo&base=4b6299a1c0e2d3f4a5b6c7d8e9f0a1b2c3d4e5f6" \
  -H "Authorization: Bearer $RENDEROPS_API_KEY"
git apply --binary agent.patch
```

`200 OK` with the raw patch as the body (`Content-Type: text/x-diff`; empty
when nothing changed) and:

| Header | Value |
| --- | --- |
| `Content-Length` | The patch's size. |
| `X-Patch-Sha256` | The patch's sha256. |
| `X-Patch-Base` | `base`. |
| `X-Patch-Head` | The current `HEAD` (empty before the first commit). |

The patch is at most 50 MiB: larger is `413 patch_too_large` with `size` (at
least that many bytes). The body streams in 2 MiB pieces; if the sandbox fails
part way, the body ends short of `Content-Length`, so check the length and
`X-Patch-Sha256` before applying. Other errors are those of `changes`. Like
the other reads it does not count as sandbox activity.

Any unexpected failure inside the sandbox is `502 files_failed`.

## Uploads

Send a folder to a sandbox without streaming it through the API: create an
upload, PUT a `.tar.gz` straight to blob storage, then have the sandbox
download and unpack it. Both routes need the sandbox's owner (`404
sandbox_not_found`) and an active sandbox (`409 sandbox_not_active`; never
resumed here). A server without upload storage answers `503
uploads_unavailable`.

### `POST /v1/sandboxes/{sandboxID}/uploads`

```bash
curl -sS -X POST "$RENDEROPS_API_URL/v1/sandboxes/$SANDBOX_ID/uploads" \
  -H "Authorization: Bearer $RENDEROPS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"size": 18734211, "sha256": "9f2c...64 hex..."}'
```

`size` is the archive's size in bytes (1 to `max_bytes`, 500 MiB by default)
and `sha256` its lower-case hex sha256. `201 Created`:

```json
{
  "status": "success",
  "upload_id": "0d3c6a52-5f7e-4f0a-9d1e-2b8f3c4a5e6d",
  "put_url": "https://<account>.blob.core.windows.net/<container>/uploads/0d3c6a52-....tar.gz?sv=...&sp=cw&sr=b&spr=https&se=...&sig=...",
  "put_headers": {"x-ms-blob-type": "BlockBlob"},
  "expires_at": "2026-10-17T12:15:00Z",
  "max_bytes": 524288000
}
```

PUT the archive to `put_url` in one request with `put_headers`:

```bash
curl -sS --fail -X PUT -H "x-ms-blob-type: BlockBlob" --data-binary @repo.tar.gz "$PUT_URL"
```

The URL can only create or overwrite that one blob, over HTTPS, until
`expires_at` (15 minutes). Errors: `413 upload_too_large` (with `max_bytes`),
`400 invalid_size`, `400 invalid_sha256`, `429 too_many_uploads` (8
unextracted uploads per sandbox).

### `POST /v1/sandboxes/{sandboxID}/uploads/{uploadID}/extract`

```bash
curl -sS -X POST "$RENDEROPS_API_URL/v1/sandboxes/$SANDBOX_ID/uploads/$UPLOAD_ID/extract" \
  -H "Authorization: Bearer $RENDEROPS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"dest": "/workspace/repo"}'
```

`dest` must be an absolute, clean path below `/workspace` (not `/workspace`
itself), outside any `.git` directory; it must not exist yet, or be an empty
directory. Missing parents are created. The sandbox downloads the archive
through a 10-minute read-only URL (the sandbox needs internet access), checks
its size and sha256, and unpacks it into a temporary directory next to `dest`,
which then replaces `dest` in one rename: `dest` gets the whole archive or
nothing. The archive is validated in full before anything is written:

- entries are regular files, directories, symlinks or hard links (no devices,
  fifos or sparse files), each name at most once;
- no absolute names and no `..` components;
- no entry below a symlink, and every symlink resolves inside `dest`, also when
  followed through the archive's other symlinks;
- hard links only to a regular file earlier in the archive;
- at most 200 000 entries, 2 GiB per file and 4 GiB in total.

Python's `tarfile` `data` filter runs while unpacking, and every symlink is
checked with `realpath` afterwards. Setuid/setgid bits and group/other write
bits are dropped; files are owned by root (the sandbox user).

`200 OK`:

```json
{"status": "success", "upload_id": "0d3c6a52-...", "dest": "/workspace/repo", "entries": 1834, "bytes": 52011873}
```

The upload is single-use: a successful extract deletes it and its blob. An
archive the sandbox refuses is deleted too; any other failure (`dest` in use,
the download, the sandbox) keeps the upload so the caller can retry until
`expires_at`. Expired uploads and their blobs are deleted. It counts as sandbox
activity.

| Status | Code | Meaning | Upload kept |
| --- | --- | --- | --- |
| 404 | `upload_not_found` | Unknown, already extracted, or another sandbox's. | – |
| 410 | `upload_expired` | Create a new upload. | no |
| 409 | `upload_incomplete` | The archive has not been PUT yet. | yes |
| 409 | `upload_busy` | Another extract of this upload is running. | yes |
| 400 | `invalid_dest` | See the `dest` rules. | yes |
| 409 | `dest_exists` / `dest_not_empty` | `dest` is a file / a non-empty directory. | yes |
| 502 | `download_failed` | The sandbox could not download the archive. | yes |
| 422 | `size_mismatch` / `checksum_mismatch` | The archive is not what was declared. | no |
| 422 | `invalid_archive` | Not a readable `.tar.gz`. | no |
| 422 | `unsafe_path` / `unsafe_link` / `unsupported_entry` / `duplicate_entry` | A refused entry; `entry` names it. | no |
| 413 | `entry_too_large` / `archive_too_large` / `too_many_entries` | Over a cap. | no |
| 507 | `no_space` | The sandbox's disk is full. | yes |
| 409 | `extract_unsupported` | The sandbox has no Python 3.12+ (every current image does). | yes |

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

Pass `env` to the agent process. Grants last 4 hours; grant again to extend
(each grant has a new token and replaces the sandbox's previous one). Errors:
`409 credentials_missing` (nothing saved on the Secrets page), `409
sandbox_not_active`, `400 provider_unavailable` (`openai`), `503
model_access_unavailable` (server has no `SECRETS_ENCRYPTION_KEY`).

Note that a command sent through `POST /session/{sandboxID}/exec` is kept in
the sandbox's run history, so don't put the token in one. To hand the env to
a terminal, have the server write it instead:

#### Writing the env into the sandbox (`env_file`)

```json
{"provider": "anthropic", "env_file": "/var/lib/spawn/cli/env"}
```

After the grant, the control plane writes `env` plus `HOME=/root` to
`env_file` in the sandbox, one `export KEY='value'` line per variable (sorted,
every value single-quoted). The file is mode `0600` and its directory `0700`
(created if missing); the file is replaced atomically. The write is an internal
exec: it is not recorded in the run history, usage logs or the sandbox's log
stream. `env_file` must be an absolute, clean path (no `..`) below
`/var/lib/spawn/`, the same rule as a terminal's `env_file`, so the result can
be passed straight to `POST /v1/sandboxes/{sandboxID}/terminals`.

The token is then left out of the response: `token` and `env` are omitted,
and `env_file_written` is `true`:

```json
{
  "status": "success",
  "provider": "anthropic",
  "model": "claude-opus-5-5",
  "base_url": "http://172.16.0.1:7081",
  "expires_at": "2026-10-16T16:00:00Z",
  "env_file": "/var/lib/spawn/cli/env",
  "env_file_written": true
}
```

Extra errors: `400 invalid_env_file` (checked before anything is granted),
`502 env_file_failed` (the sandbox could not write the file; grant again to
retry) and `409 sandbox_not_active` if the sandbox paused before the write.

### `DELETE /v1/sandboxes/{sandboxID}/model-access`

Revokes the grant. `204 No Content`; idempotent.

## Agent State

The spawn CLI runs the agent (Claude) in a `tmux` session inside the sandbox,
so the user can close their terminal while it keeps working and reattach
later. Hooks it installs in the agent keep `/var/lib/spawn/agent-state.json`
up to date:

```json
{"state": "working", "updated_at": "2026-10-17T12:00:00Z"}
```

`state` is `working`, `waiting` (for the user), `done` or `idle`;
`updated_at` is RFC3339.

Idle auto-pause: a sandbox past its idle timeout with no open terminal is not
paused while its agent is `working` and `updated_at` is within the worker's
`AGENT_BUSY_GRACE` (default `15m`; the hooks rewrite it on every tool call).
The worker reads the file again every minute and pauses once the agent stops
working or its state goes stale. A missing or invalid file never keeps a
sandbox awake, and nothing extends `max_lifetime_s`. Reading the file does not
count as activity.

### `GET /v1/sandboxes/{sandboxID}/agent-state`

What the sandbox's agent is doing, for the dashboard to poll. The sandbox must
be owned by the caller (`404 sandbox_not_found`) and active (`409
sandbox_not_active`; it is never resumed). The read is passive and quiet: it
does not reset the idle timer, and it is not recorded in the run history or the
sandbox's log stream.

```bash
curl -sS "$RENDEROPS_API_URL/v1/sandboxes/$SANDBOX_ID/agent-state" \
  -H "Authorization: Bearer $RENDEROPS_API_KEY"
```

`200 OK`:

```json
{"state": "working", "updated_at": "2026-10-17T12:00:00Z", "age_seconds": 42}
```

`age_seconds` is how long ago the state was written, measured on the
sandbox's own clock. Use it rather than comparing `updated_at` with your
clock: a resumed sandbox's clock can run behind. With no state file, or one
that is not valid, the response is only:

```json
{"state": "unknown"}
```

Other errors: `409 sandbox_not_placed`, `502 agent_state_failed` (the sandbox
could not run the read).

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

An exec body may set `"active_only": true` (refuse a sandbox that is not
running with `409 sandbox_not_active` instead of resuming it),
`"passive": true` (do not reset the idle timer, run count or last exit code)
and `"quiet": true` (the guest keeps the output out of the sandbox's log
stream; the output is still returned). The files API sets `active_only` and
`quiet` for every command and `passive` for reads. Agent-session commands, the
workspace checkout and the IDE's commands are quiet too. A guest agent built
before `quiet` ignores it and logs the output as before.

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
