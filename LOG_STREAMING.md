# Sandbox output streaming

Implemented locally; not deployed. Output capture is a component of the existing worker, not another server.

| Layer | Editable source / endpoint |
| --- | --- |
| Guest capture | `guest-agent/logs.go`, hooks in `main.go` and `terminal.go` |
| Worker collector | `backend/internal/logagent/agent.go` |
| Lifecycle wiring | `backend/cmd/worker/main.go`, `backend/internal/session/logs.go` |
| Ingest | `POST /internal/logs/batches`, `backend/internal/logstream/ingest.go` |
| Storage/retention | `backend/internal/platform/logs.go` |
| Dashboard SSE | adjacent `sandbox_platform/src/routes/api/logs/stream/+server.js` |
| Replay query | adjacent `sandbox_platform/src/lib/server/log-stream.js` |
| Dashboard UI | adjacent `sandbox_platform/src/lib/components/dashboard/logs/LogsOutput.svelte` |
| Migration | adjacent `sandbox_platform/drizzle/0015_sandbox_log_stream.sql` |

Flow: guest output → dedicated vsock feed → disk-spooled worker batches → authenticated Go control plane → PostgreSQL → dashboard SSE.

## Editable worker settings

Edit `/etc/renderops/worker.env` on each host:

```dotenv
WORKER_LOG_INGEST_URL=http://127.0.0.1:18081/internal/logs/batches
WORKER_LOG_SPOOL_DIR=/opt/renderops/worker/logs/pending
```

The example assumes an SSH-forwarded control-plane port; use your actual private URL or HTTPS endpoint. The collector uses the existing host WORKER_TOKEN and WORKER_ID, never guest credentials. No token goes in the URL or guest. Without WORKER_LOG_INGEST_URL capture stays disabled for staged rollout.

Files are 0600 in a 0700 spool directory and are inspectable JSON batches (sensitive output). Flush at 250ms, 100 events or ~64 KiB; fsync + rename before advancing the guest cursor. Disk spool cap: 64 MiB per worker. Guest replay cap: 1 MiB per VM. Replay overflow emits an output-gap marker, not a false completeness guarantee.

ACK follows database commit. Stable event UUIDs deduplicate retries and lost ACKs. The server resolves ownership from the sandbox row and inserts only events matching the current assigned worker. Late output from released/moved/deleted sandboxes is intentionally not retained. The existing shared trusted-worker token is the boundary; per-worker credentials/mTLS are future hardening.

## Retention and coverage

- Output expires after **48 hours from capture**, even if it arrives late. All dashboard reads enforce this cutoff.
- Control-plane cleanup runs at startup and every minute, bounded to 1,000 rows per delete/update. Pending batches expire too.
- Billing metadata and audit events remain. New billing rows no longer duplicate stdout/stderr; old payloads are nulled after 48h.
- PTY output is captured without a dashboard attachment. PTYs merge stdout/stderr, labelled stdout. Input is not explicitly captured, but echoed commands may appear in output.
- Shell stdout and direct-process stdout/stderr stream while running. Persistent-shell stderr and Python/Node cells still arrive on completion under the existing execution protocols. File-redirected/background processes outside captured pipes are not intercepted.
- Quiet execs are not captured: the guest skips the feed for an exec request with `quiet: true`. Platform-internal commands use it (files API, agent-session runtime, workspace checkout, IDE), as does a public `POST /session/{id}/exec` with `"quiet": true`. Guests built before the flag ignore it and capture the output.
- Capture doesn't update idle TTL. Pause/resume reconnects and replays retained output. This policy does not scrub guest RAM checkpoints, customer files, or backups.
- Ordinary output logs aren't a lossless compliance recorder or a secret-redaction system. Outages, full buffers or worker disk loss can cause explicit/possible gaps.

## SSE

`GET /api/logs/stream?since=<ISO>&after=<decimal ID>` requires the dashboard session, scopes every read to its owner, and reconnects using Last-Event-ID. Bigint/string cursors avoid timestamp collisions. Heartbeats prevent proxy idling. Connections recycle after 60s to re-check auth; slow consumers are disconnected and replay on reconnect. The first version tails committed PostgreSQL rows once/second internally; no Redis/pubsub service is added.

The UI loads history first, then SSE; buffer limits are 5,000 records and 2 MiB content. The output range is capped at 48h. Run summaries remain, but streamed output is not yet associated with run_id. All output database writers use one short advisory transaction lock so identity allocation order matches commit order for replay. Measure this serial batch-insert boundary before high-volume rollout.

## Rollout

1. The standalone logging SQL is included, but its Drizzle journal/snapshot registration is deliberately pending alongside the untouched draft agent-session migration. Register and apply logging SQL through the proper chain before enabling worker capture; do not assume deployment applies it automatically. Dashboard SSE reads existing columns and can deploy before registration. Leave the agent-session migration for the separate task.
2. Deploy the control plane (ingest, writer ordering, retention).
3. Rebuild rootfs/initramfs and templates with the updated guest agent.
4. Deploy workers; enable WORKER_LOG_INGEST_URL only on upgraded hosts/templates. It also disables duplicate legacy execution-output writes. Old templates cannot support logs_follow.
5. Deploy dashboard SSE. Test on a fresh sandbox; old paused snapshots still contain their old guest binary.
6. Run `printf 'first\n'; sleep 3; printf 'second\n'` in a terminal. Confirm first appears before second, capture continues with the UI detached, reconnect doesn't duplicate events, and ingest outage recovers from disk.

Unit/regression tests and a disposable PostgreSQL integration test passed (migration, deduplication, assigned-worker filtering, NUL sanitization, output retention and preservation of audit/billing metadata). These don't replace real Firecracker rollout and end-to-end checks. No production records were removed. After deploying cleanup, expired output cannot be recovered from the live database; backups have their own retention.
