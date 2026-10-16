# Agent sessions v1: execution contract

A user assigns a GitHub issue (or a PR, or a prompt) to an agent on the
dashboard. A Claude Code **coding agent** works on it inside a Spawn sandbox and
commits on branch `spawn/<sid8>`. Progress streams into the session page; the
Preview tab shows servers the agent starts. Nothing runs on the host: every
turn runs detached inside the session's Firecracker VM.

```
platform (queue, Postgres)  <-- /api/internal/agent-sessions/* -->  runner (agentsession.Pool in the control plane)
      | session_events -> SSE -> browser                                  | in-process: CreateWithID, Exec, Resume,
                                                                          | Pause, model-access grant, gitworkspace.Prepare
                                                                          v
                                                         guest: /var/lib/spawn/agent/<sid>/, /workspace/repo
```

## Turning it on

| Where | Variable | Value |
|---|---|---|
| control plane | `AGENT_RUNNER_ENABLED` | `true` (default off) |
| control plane | `AGENT_WORKER_TOKEN` | >= 32 chars, same as the platform's (startup fails without it) |
| control plane | `SECRETS_ENCRYPTION_KEY` | required when the runner is on (model access) |
| control plane | `PLATFORM_INTERNAL_URL`, `AGENT_RUNNER_CONCURRENCY`, `AGENT_TURN_TIMEOUT` | defaults `http://platform:3000`, `4`, `45m` |
| platform | `AGENT_SESSIONS_ENABLED`, `AGENT_WORKER_TOKEN` | `true`, the same token |

## Platform API (Bearer `AGENT_WORKER_TOKEN`, JSON bodies)

`claim {workerId, leaseSeconds}` -> 204 or `{session}` with a UUID `leaseToken`;
every other call carries it and gets 409 once the lease is lost or expired.
`[id]/heartbeat {leaseToken, leaseSeconds}`, `[id]/events {leaseToken, cursorFrom?, cursorTo?, events}`
(cursor CAS on `runtime_cursor`; 409 `{runtimeCursor}` on mismatch), `[id]/inbox`, `[id]/inbox/ack {seq}`
(also clears the interrupt flag), `[id]/release {idle}` (409 `{pending}` while messages wait),
`[id]/workspace-grant` -> `{repository, headSha, baseSha, token, expiresAt}`.

## Runner (`backend/internal/agentsession`)

Per claimed session (one goroutine; heartbeat every 15 s, lease 60 s):

1. **Ensure** (`runtime.go`): append `status provisioning` (if queued) and `sandbox {sandboxId}` *before*
   creating it. The sandbox ID is the platform's, or derived from the session ID (UUIDv5), so a retry
   never creates a second VM. `CreateWithID` (idempotent per owner): image `agents`, size large
   (2 vCPU / 4 GB / 20 GB), internet on, idle 20 min, lifetime 24 h, owner = `ownerUserId`. Resume if
   paused. Grant model access with a **stable per-session token** (HMAC of the session and sandbox
   IDs), so it can be granted again after a resume or before the 4 h TTL while a turn keeps running.
   Write the guest files. Once (no `workspace.ok`): workspace-grant -> `gitworkspace.Prepare` (the repo
   token goes to the worker's git proxy only, revoked after the checkout) -> `git switch -c spawn/<sid8>`,
   commit identity, `workspace.ok`.
2. **Loop**: poll the guest; decode new journal lines; append events with `cursorFrom/cursorTo`.
   While no turn runs: inbox -> deliver (messages joined into one turn, id = last seq) -> ack; or
   release idle (status waiting). Turn end -> inbox -> deliver or release.
3. **Stop** (`interruptRequested`): kill the turn (turn_end "interrupted"). Messages sent before the
   Stop are held until a new message arrives (or 15 min). **End session** (`cancelRequested`, also from
   waiting): stop the turn, pause the sandbox (revokes grants), close its previews, append
   `status cancelled`. **Lease lost** (409): stop at once, never touch the guest. **SIGTERM**: stop
   claiming and heartbeating; guest turns keep running and the next runner resumes from the cursor.

Errors that end a session (`error` event, final): no capacity, missing Anthropic credentials, repository
access revoked, pinned commit gone, sandbox expired, journal lost, sandbox unreachable for ~5 min.
A failed or timed-out *turn* is not final: it ends with a system message and the session waits.

## Guest

Every script runs as `printf %s <base64> | base64 -d | sh` (body in a function with stdin from
`/dev/null`). `/var/lib/spawn/agent/<sid>/` (0700): `env` (0600: model proxy env, `HOME=/root`,
`TMPDIR=GOTMPDIR=/var/tmp/spawn`, `GOTOOLCHAIN=auto`), `system.txt`, `turn.sh`, `prompts/<seq>.txt`,
`turns/<seq>/` (mkdir = start lock), `events.jsonl` (journal), `stderr.log`, `turn.pgid`,
`workspace.ok`, `ports.published`.

Turn wrapper (`setsid`, detached): `turn.pgid`, `turn_start` marker, then in `/workspace/repo`
`timeout <45m> claude -p "<prompt>" --output-format stream-json --verbose (--session-id|--resume) <sid>
--permission-mode auto --max-turns 200 --append-system-prompt "<system.txt>" </dev/null >>events.jsonl`,
then a `turn_end {exit, reason}` marker (`interrupted`, `timeout`). `--resume` once
`$HOME/.claude/projects/*/<sid>.jsonl` exists.

Poll (args: cursor, ports signature): exit 10 when nothing changed; else size, wrapper alive
(`/proc/<pid>/stat` not zombie, start time matches), last started turn, listening ports
(`/proc/net/tcp{,6}` state 0A), and up to 256 KB of journal as 76-column base64. A dead wrapper without
`turn_end` gets a synthesized `turn_end {reason: lost}`. Stop: TERM to the wrapper's process group and
every process in its session, 3 s, KILL; servers started with `setsid` survive.

## Decoder (`claude.go`, stream-json -> events)

| Journal | Event |
|---|---|
| `turn_start` | `status running` |
| `system/init` | step "Claude Code started (<model>)" |
| assistant text | `message` (agent) |
| Bash tool_use + tool_result | `command {cmd, exitCode, output}` (last 4 KB, ANSI stripped) |
| Edit/MultiEdit/Write | step "Edited/Created <path>" `{additions, deletions}` |
| Read, Grep, Glob, TodoWrite, Task, other tools | step (tool name and path/pattern only) |
| result with error | system `message`, sanitized (credentials, rate limit, provider down, step limit) |
| `turn_end` with a reason or a crash | system `message` |
| port change | `preview {port, open, bind}` (runner-origin; 3001 and loopback ephemeral ports hidden) |

Thinking, subagent internals, `system/thinking_tokens`, `task_*` and unknown types are never published.
A Bash/Edit event waits for its result, and nothing after it is committed first, so the committed cursor
is always a line boundary a restarted runner can decode from with no loss and no duplicates.
Previews: after any resume every known port is closed and the listening ones are opened again; the
published set is kept in `ports.published` for the next claim.

## Limits and known gaps

- 2 active sessions per user (platform), 45 min per turn, 200 agent steps per turn, idle 20 min, 24 h.
- No push and no PR yet: changes stay committed in the sandbox on `spawn/<sid8>`.
- The checkout is a blob-less partial clone; after the git grant is revoked, history blobs other than
  the checkout cannot be fetched (`git log -p` of older commits fails).
- Poll output passes through the guest's exec log capture, so journal chunks also appear in the
  sandbox's log stream.
- Ports shown while a session waits can be stale after its idle pause until the next turn resets them.

Tests: `cd backend && go test -race ./internal/agentsession ./internal/controlplane/... ./internal/modelaccess`.
`AGENTSESSION_DOCKER_IMAGE=alpine:3.20 go test ./internal/agentsession -run Busybox` runs the guest
scripts under busybox in a container.
