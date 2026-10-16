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

`claim {workerId, leaseSeconds}` -> 204 or `{session}` with a UUID `leaseToken` and the agent's
`instructions` (see below); every other call carries the lease token and gets 409 once the lease is
lost or expired.
`[id]/heartbeat {leaseToken, leaseSeconds}`, `[id]/events {leaseToken, cursorFrom?, cursorTo?, events}`
(cursor CAS on `runtime_cursor`; 409 `{runtimeCursor}` on mismatch), `[id]/inbox`, `[id]/inbox/ack {seq}`
(also clears the interrupt flag), `[id]/release {idle}` (409 `{pending}` while messages wait),
`[id]/workspace-grant` -> `{repository, headSha, baseSha, token, expiresAt}`.

## Agent instructions (rendered by the platform)

The platform renders the agent's instructions; the runner only validates them and writes them into
the sandbox. The claim's `session.instructions`:

```json
{
  "version": "2026-10-17.1",
  "system": "appended to Claude Code's system prompt, <= 16 KB",
  "notices": { "resumed": "<= 2 KB", "interrupted": "<= 2 KB", "turn_failed": "<= 2 KB" }
}
```

- **Validation** (`instructions.go`): an object; `version` (<= 100 bytes) and `system` are non-empty
  strings; `notices` and each notice are optional (absent or `null`), otherwise strings; no NUL. Unknown
  keys are ignored. Absent or `null` `instructions` (an older platform) and invalid ones both use the
  runner's fallback: a few generic sentences (isolated sandbox, repo at `/workspace/repo`, commit on
  `spawn/<sid8>`, no credentials) and short generic notices. Invalid ones are logged; the claim log's
  `instructions_version` names the version in use (`fallback` for the runner's own).
- **System prompt**: `system` is written to `system.txt` on every claim and passed with
  `--append-system-prompt-file`. Claude Code records the system prompt on a conversation's first request
  and reuses it on `--resume` until compaction, so a changed text reaches new sessions only. GitHub-authored
  text (issue and PR titles, bodies, comments) never goes in it.
- **Turn notices**: before a follow-up turn, the runner puts one block at the top of that turn's prompt
  file (still written base64):

  ```
  <spawn_notice>
  <resumed text>
  <interrupted or turn_failed text>
  </spawn_notice>

  <the user's messages>
  ```

  `resumed`: the sandbox was resumed since the previous turn was delivered. `interrupted`: the previous
  turn's `turn_end` reason is `interrupted` (the user's Stop). `turn_failed`: it ended any other way than a
  clean exit (`timeout`, `lost`, `start_failed`, or a non-zero exit: Claude Code exits 1 on an API error and
  at its step limit). Each event is noticed once, on the next delivered turn; the first turn gets none; an
  empty notice text is left out, and with nothing to notice there is no block. Each notice is written as
  sent, one after another (the platform decides any bullets). The previous turn's end comes from the decoded
  journal or, when it is behind the committed cursor, from the setup script, which reports the journal's
  last `turn_end` marker.
- **Review file**: setup creates `/var/tmp/spawn/review/` (root, 0755); the agent writes `review.json`
  there and the platform reads it with exec. The runner does nothing else with it.

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
`TMPDIR=GOTMPDIR=/var/tmp/spawn`, `GOTOOLCHAIN=auto`, `CLAUDE_CODE_PROVIDER_MANAGED_BY_HOST=1`),
`system.txt` (0600), `settings.json` (0600), `turn.sh`, `prompts/<seq>.txt`, `turns/<seq>/` (mkdir =
start lock), `events.jsonl` (journal), `stderr.log`, `turn.pgid`, `workspace.ok`, `ports.published`.
`/var/tmp/spawn/review/` (0755) is the agent's review directory.

Turn wrapper (`setsid`, detached): `turn.pgid`, `turn_start` marker, then in `/workspace/repo`
`timeout <45m> claude -p "<prompt>" --output-format stream-json --verbose (--session-id|--resume) <sid>
--permission-mode auto --permission-prompts none --max-turns 200 --append-system-prompt-file <system.txt>
--settings <settings.json> </dev/null >>events.jsonl`, then a `turn_end {exit, reason}` marker
(`interrupted`, `timeout`). `--resume` once `$HOME/.claude/projects/*/<sid>.jsonl` exists.

Claude Code settings, each checked against Claude Code 2.1.296 (the agents image's version):

- `CLAUDE_CODE_PROVIDER_MANAGED_BY_HOST=1`: Claude Code ignores provider, auth and proxy keys
  (`ANTHROPIC_BASE_URL`, tokens, `HTTPS_PROXY`, CA settings) in settings files. Without it, a repository's
  `.claude/settings.json` `env` block redirects the model traffic, and `-p` applies project settings
  without a trust prompt.
- `--settings settings.json` sets `pluginConfigs["cc-plugin-agents-md@builtin"].options.instructionFiles`
  to `claude-md-and-agents-md`: AGENTS.md loads beside CLAUDE.md (by default only where there is no
  CLAUDE.md). Claude Code reads plugin options from user and `--settings` (flag) settings only, so a
  repository cannot turn it off.
- `--permission-prompts none`: anything that would prompt (an MCP elicitation included) is denied
  locally instead of waiting for a host. `AskUserQuestion` is not offered in this `-p` invocation either way.

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
- Runtime commands (setup, polls, deliver, stop, workspace) and the checkout are quiet execs, so
  journal chunks stay out of the sandbox's log stream. A sandbox started from a guest image that
  predates quiet execs still logs them.
- Ports shown while a session waits can be stale after its idle pause until the next turn resets them.

Tests: `cd backend && go test -race ./internal/agentsession ./internal/controlplane/... ./internal/modelaccess`.
`AGENTSESSION_DOCKER_IMAGE=alpine:3.20 go test ./internal/agentsession -run Busybox` runs the guest
scripts under busybox in a container.
