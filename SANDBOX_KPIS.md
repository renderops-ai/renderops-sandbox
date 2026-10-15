# Sandbox dashboard KPI slice (local changes, not deployed)

## Storage decision

No new KPI table or schema migration is needed for this increment. The source
records already exist in `sandboxes`, `usage_logs` and `audit_events`. Server
queries return aggregate totals and at most 169 hourly startup points; they
do not ship entire histories to the frontend.

If traffic later warrants materialized hourly rollups, introduce a separate
rollup table with a defined refresh/reconciliation policy. Do not put windowed
totals/percentiles on every sandbox row or duplicate billing amounts there.

## Definitions

- Running: `sandboxes.state = active`. Creating/resuming are shown separately
  as starting; pausing and paused are separate too. No made-up per-user limit.
- Creation requests: sandbox rows whose creation timestamp is in the selected
  24-hour/7-day window, including pending/failed provisions. This is deliberately
  different from the existing successful-create activity chart.
- Recorded execution cost: SUM of stored `usage_logs.cost_usd` in the window,
  including historical/deleted sandboxes, not visible-row runtime estimates.
  It is not a quote, a recomputation from current prices, or total resource cost.
- Median lifetime: SQL percentile_cont over individually observed, successfully
  audited deletions of this user's terminal sandboxes, using the sandbox's
  creation timestamp and first successful destroy event. Duplicate destroy
  records do not add samples. Missing auto-deletion audit events are excluded
  and the UI explicitly labels sample coverage. `updated_at` is NOT treated as
  an exact death timestamp. Empty samples yield null, not zero.
- Time to start: monotonic elapsed duration from entry into the authenticated
  API create handler to successful VM-running acknowledgement, captured BEFORE
  response enrichment/audit logging. Stored as `startup_duration_ms` in the
  existing `sandbox.created` audit metadata. It excludes auth middleware/network
  latency, failed provisions, resumes and application readiness. Historical
  events without valid metadata have no measurement; they are not backfilled
  from created_at/updated_at.

All queries are owner-scoped and parameterized. Windows are [start,end), and
startup buckets explicitly use UTC regardless of the DB session timezone.
The dashboard obtains a fresh snapshot on page load/refresh and after lifecycle
invalidations; this increment does not add live telemetry, SSE or background
refresh infrastructure. DB errors are not silently turned into zero counts.

## Code

Platform (`../sandbox_platform`):
- `src/lib/server/actions/sandbox-kpis.js` and `.spec.js`
- `src/routes/dashboard/sandboxes/+page.server.js` and `+page.svelte`
- `src/lib/components/dashboard/sandboxes/SandboxesScreen.svelte`
- `src/lib/components/dashboard/Chart.svelte`: opt-in markers and zero-value
  display for sparse startup samples; other charts keep existing defaults.

Backend:
- `backend/internal/handler/session.go`: capture measured successful create
  latency in the existing audit event (no migration or new writer service).
- `backend/internal/handler/tests/integration_test.go`: assert non-negative
  measured duration on the dashboard lifecycle contract.

## Verification / rollout

- Focused platform action tests and backend handler tests run locally.
- Platform production build compiles the real loader and chart changes.
- SQL generation/aggregation mapping tests use mocked DB results; a real
  PostgreSQL fixture test and a live-worker create remain rollout checks.
- Deploy the backend addition to start collecting durations, then the platform.
  Either order remains schema-compatible; old backend events simply have no
  duration and the UI shows the explicit empty state.
- Nothing in this change applies migrations, publishes assets, restarts servers
  or changes the paused Azure migration. Other agents' edits remain untouched.

CPU utilization, guest RAM usage, network rates, readiness probes, classified
exit reasons and filesystem-used bytes remain separate telemetry work.
