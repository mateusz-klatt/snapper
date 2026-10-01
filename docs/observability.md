# Observability

Snapper exposes permission-gated health metrics so a dashboard can see
the gradient toward exhaustion before a crash. The observability surface
includes process-level counters sampled into an in-memory ring buffer,
per-table SCD2 database statistics at `/api/metrics/db/tables`, and retention
policy state at `/api/metrics/retention`. The retention definition of
"archivable" drives the database statistics `archivable` counter.

## Endpoints

Metrics reads require effective `Permission.READ_SYSTEM_STATUS`; the current
`ai_reviewer`, `ai_delegate`, `viewer`, `operator`, and `admin` named sets
contain that permission. The two tracemalloc mutations instead require
effective `Permission.MANAGE_RUNTIME_DIAGNOSTICS`, which is present in the
current `ai_reviewer`, `ai_delegate`, `operator`, and `admin` sets but not
`viewer` or `ai_researcher`. Those `POST` routes also require CSRF on the
cookie-authenticated path; `Authorization: Bearer` requests bypass CSRF per
the project-wide auth contract. On this surface, `viewer` is a read-only
operator: it can call every `GET` route below but cannot start or stop
tracemalloc.

| Method | Path                                                | Purpose |
|--------|-----------------------------------------------------|---------|
| GET    | `/api/metrics/system`                               | Latest snapshot |
| GET    | `/api/metrics/system/history?since&until&limit`     | Windowed slice of the ring buffer |
| POST   | `/api/metrics/system/tracemalloc/start?duration_s`  | Arm Python tracemalloc with auto-stop deadline |
| POST   | `/api/metrics/system/tracemalloc/stop`              | Disarm tracemalloc + cancel pending deadline |
| GET    | `/api/metrics/notifications`                        | Notify-sidecar outbox delivery counters |
| GET    | `/api/metrics/rest-rate`                            | Rolling REST call rates + rate-limit utilization per exchange |
| GET    | `/api/metrics/retention`                            | Retention scheduler status and policy counters |
| GET    | `/api/metrics/db/tables`                            | Per-table row-count and SCD2 lifecycle counters |

### Sibling health endpoint

`GET /api/health/egress` is a sibling health endpoint outside the
`/api/metrics/*` surface. It shares the same
`Permission.READ_SYSTEM_STATUS` gate as the metrics routes and returns a
per-container egress pool status snapshot, aggregated cross-process over
the `system.egress.snapshot` ZMQ topic, with sidecar
`system.egress.transfer` samples joined to matching SOCKS5 route rows. See
[api.md](api.md) (`GET /api/health/egress`) for the full wire schema.

### Data-liveness watchdog

Beyond the REST surface, each exchange feed runs a per-subscription
data-liveness watchdog (`SubscriptionHealthTracker`,
`infrastructure/exchanges/_subscription_health.py`): a confirmed
subscription that receives no data for `data_stale_threshold_s` (default
`300.0`s; `ack_timeout_s` default `15.0`s guards the initial subscribe
ACK) is marked stale and emits a one-shot log line. Beyond the one-shot
log line, each publisher periodically flushes the tracker's current
state into the `instrument_feed_health` table. A token retaining
`read:system_status` can query it through `GET /api/market/feed-health`
(optional `exchange` and `fresh_within_seconds` filters; same
`Permission.READ_SYSTEM_STATUS` gate as the metrics routes).

### Market-data watchdog (whole-exchange silence)

The per-subscription tracker cannot alert on a whole exchange going
dark: feed heartbeats report DB flush errors and venue-specific health
contributions, but a healthy socket alone does not prove fresh candles;
in-process freshness clocks reset on every publisher respawn, and a
fully hung publisher emits nothing at all. The API lifespan therefore
runs `MarketDataWatchdog`
(`application/market_data_watchdog/watchdog.py`), which every
`MARKET_DATA_WATCHDOG_INTERVAL_SECONDS` (default 60) queries the
database for the newest candle per live feed exchange (`kraken`,
`kraken_futures`, `kraken_equities`, `walutomat`). When an exchange's
silence — measured from its newest candle minute END — exceeds
`MARKET_DATA_WATCHDOG_THRESHOLD_SECONDS` (default 600, floor 120), the
watchdog publishes a synthetic 3-frame WARNING heartbeat burst on
`system.heartbeats.marketdata.{exchange}` (frames spaced 2 s to clear
the bridge's 1 s heartbeat throttle). The existing
`critical_system_error` rule then supplies the 3-consecutive gate,
rolling hourly cooldown, hour-bucket dedup, and fan-out to every user
with `read:system_status` — about one page per hour per silent
exchange, with `meta` carrying `silent_seconds`, `threshold_seconds`,
and `latest_candle_open_at` for forensics.

Scheduled venue closures are suppressed via the shared CME calendar
(`core/market_hours.py`), whose schedule is defined in Chicago wall
time (CT) and converted per call: `kraken_equities` never alerts
inside the daily 16:00-17:00 CT maintenance break, the Friday
16:00 CT - Sunday 17:00 CT weekend closure, or a curated US-holiday
closure, and after a closure ends its silence clock restarts at the
reopen boundary instead of the last pre-closure candle.
Per-exchange thresholds can be overridden (or one exchange
disabled with `0`) via `MARKET_DATA_WATCHDOG_EXCHANGE_THRESHOLDS`
(`exchange=seconds` CSV), and `MARKET_DATA_WATCHDOG_DISABLED=true`
parks the watchdog entirely. Detection runs level-triggered — an
exchange that stays silent keeps re-bursting each tick, so a notify
sidecar restart cannot permanently miss an ongoing outage.

### Trade-integrity monitors

Coordinator instance 0 runs two incremental database monitors every
`TRADE_INTEGRITY_MONITOR_INTERVAL_SECONDS` (default 60, clamped to
30–60 seconds). M1 detects one non-null
`(instrument_public_id, trade_id)` at differing
`executed_at` values; M2 detects more than one active `trades` row for
one `public_id`. The current U2 constraint and active-identity partial
unique index make those conditions impossible, establishing the
required zero baseline before U2 is widened.
First startup does not pre-credit the window: completed coverage starts
at the overlap boundary and becomes current only after the full initial
pass completes.

Each monitor returns at most 25,000 recent sweep rows and 2,000 durable
restore/import worklog rows per run, then performs only targeted
index-backed identity probes for at most 27,000 distinct candidates;
each probe reads at most two matching index entries. The repository
rejects larger row limits. Each repository call has a 30-second timeout;
one timeout does not prevent the other monitor from running.
Across both monitors the scheduler therefore requests at most 50,000
sweep rows and 4,000 worklog rows per tick. Under normal production
load each monitor is expected to finish well inside its 30-second
ceiling.

Findings or excessive completed-coverage lag publish three WARNING
heartbeats, two seconds apart, on
`system.heartbeats.trade_integrity.{m1|m2}`. Clean passes publish one
HEALTHY frame. The existing `critical_system_error` rule handles the
three-frame gate, hourly deduplication, and operator fan-out. With
successful calls and an available publisher, committed worklog
identities with no backlog are normally detected in about 65 seconds
for M1 and 99 seconds for M2 at the default cadence; worst tick ordering
with successful calls consuming their full timeout is about 94 seconds
for M1 and 128 seconds for M2. The raw six-hour
overlap contains about 1.68 million rows at the current ingestion rate;
arrivals during pagination expand a steady pass to about 2.06 million
rows, or roughly 82 minutes at 25,000 rows per minute. Including the
five-minute settlement grace, cadence, and warning spacing, a newly
settled cursor-only violation normally takes at most about 89 minutes
to alert. If every successful database call consumes nearly all of its
30-second budget, the same throughput calculation grows to roughly four
hours. These are operational bounds, not mathematical guarantees: the
live publisher retries retained batches without a time limit, so a
commit stalled for more than the six-hour overlap is outside enforced
live-row coverage. Archive restore, historical paper replay, and the
local-UAT trade import do not share that exposure because their trade
write and exact monitor obligations commit in one transaction.

An M1 or M2 finding blocks the U2-to-U3 widening: inspect the identities
in heartbeat metadata and database logs, stop the responsible
ingestion/restore/import/replay writer, preserve the pending worklog evidence,
and repair or quarantine the conflicting rows.
A lag-only warning means the zero observation is not current; restore
monitor coverage before treating the baseline as proven or proceeding
with schema work.

### AI-delegate watchdog

The API lifespan also runs `AiDelegateWatchdog`
(`application/ai_review/watchdog.py`) for the single-operator AI review
plane. It waits one 60-second poll interval at startup so a healthy
delegate can reconnect before strict liveness is evaluated. Every 60
seconds thereafter it performs two database reads: delegate
liveness across active AI-delegate users whose operator membership has
an active scope grant, using the same strict 15-second heartbeat boundary
as `AiReviewService.create_review`, and the count of reviews resolved as
`timeout_no_response` during the preceding hour. No live delegate, or one
or more recent unanswered reviews, publishes a synthetic three-frame
WARNING burst on `system.heartbeats.ai_delegate.global`. A healthy tick
publishes one HEALTHY frame to reset the alert rule's rolling state.

The existing `critical_system_error` rule owns the three-frame gate,
rolling one-hour cooldown, hour-bucket deduplication, permission-based
fan-out, localization, and APNs delivery. The one-timeout threshold is
intentional: a single unanswered LIVE consult already vetoes a trading
window, while the existing cooldown prevents repeated pages. The
one-hour lookback keeps that failure visible across a notify-sidecar or
API restart.

### Failure contract

If the snapshotter singleton failed to start at lifespan time, the
`app.state.system_metrics_snapshotter` attribute stays `None` and every
`/api/metrics/system*` route returns HTTP `503` with body
`{"detail": "system metrics snapshotter not available"}`. The rest of
the application continues to serve other endpoints.

### Cold-start contract

`SystemMetricsSnapshotter.start()` takes one eager synchronous sample
BEFORE returning, so the first request after lifespan startup completes
hits a populated buffer. When the API process has its ZMQ publisher
wired, that same eager sample also publishes the first
`system.heartbeats.host.disk` heartbeat.

### Notification delivery metrics

`GET /api/metrics/notifications` does not touch the snapshotter or
the ring buffer (the failure contract above does not apply to it).
Each request aggregates active `alert_deliveries` rows (SCD2
predecessor versions excluded) into per-status delivery totals —
`sent`, `failed`, APNs-410 `unregistered`, `cancelled_scope` — plus
the `queued` backlog depth of the notify sidecar's retry loop.
Statuses absent from the database report `0`. See [api.md](api.md)
for the `NotificationMetricsResponse` wire schema.

Use container state, restart history and sidecar logs to investigate retry-worker
failure. The sidecar propagates unexpected worker failure through its CLI and
awaits owned-task cleanup. A secondary cleanup failure is logged by stage and
exception class while the original failure remains primary. Delivery totals
describe persisted outbox state; they do not certify worker liveness or APNs
receipt.

## Snapshot fields

Each sample captures nine nested groups, a top-level `bus_time`
timestamp, and two top-level flags. All
field names are stable; downstream consumers (frontend, iOS) regenerate
from the backend OpenAPI / JSON-Schema export and pin the wire shape.

### `process`

| Field              | Type   | Description |
|--------------------|--------|-------------|
| `pid`              | int    | Process ID. |
| `uptime_seconds`   | float  | Monotonic-clock seconds since the snapshotter was constructed. |
| `status`           | str    | psutil process status (`running`, `sleeping`, ...). |
| `num_threads`      | int    | Live thread count. |
| `num_fds`          | int    | Open file descriptor count. |
| `num_connections`  | int    | Open TCP/UDP socket count. |

### `cpu`

| Field                          | Type        | Description |
|--------------------------------|-------------|-------------|
| `process_percent`              | float       | `psutil.Process.cpu_percent` reading at sample time. |
| `user_time_seconds`            | float       | Cumulative user-mode CPU. |
| `system_time_seconds`          | float       | Cumulative system-mode CPU. |
| `cgroup_quota_microseconds`    | int \| null | cgroup CPU quota microseconds per period; `null` when absent or unlimited. |
| `cgroup_throttled_count`       | int \| null | Cumulative throttle event count from `cpu.stat` `nr_throttled`; `null` when cgroup absent. |

### `memory`

| Field                  | Type          | Description |
|------------------------|---------------|-------------|
| `rss_bytes`            | int           | Resident set size (RSS). |
| `rss_peak_bytes`       | int           | Peak RSS from `/proc/self/status` `VmHWM`; falls back to `rss_bytes` on non-Linux. |
| `vms_bytes`            | int           | Virtual memory size. |
| `python_traced_bytes`  | int \| null   | Bytes tracked by `tracemalloc`; `null` when tracemalloc is OFF. |
| `native_bytes`         | int \| null   | `max(0, rss - python_traced)` when tracemalloc is active; `null` otherwise — exposes "native dark matter" (numpy / pandas / zmq / aiosqlite native cache / pydantic-core Rust). |
| `cgroup_limit_bytes`   | int \| null   | cgroup memory cap; `null` when absent or unlimited. |
| `cgroup_current_bytes` | int \| null   | Current cgroup memory consumption. |
| `saturation_pct`       | float \| null | `cgroup_current / cgroup_limit` when both available; `null` otherwise. |

### `asyncio`

| Field           | Type | Description |
|-----------------|------|-------------|
| `active_tasks`  | int  | `len(asyncio.all_tasks())`. |
| `pending_tasks` | int  | Subset of `all_tasks` whose `done()` is `False`. |

### `gc`

The fields below describe the API/wire shape returned by
`/api/metrics/system`. Internally, the snapshotter stores the
generation counters as a tuple (`collections_per_gen:
tuple[int, int, int]` on the `GcMetrics` group reached via
`SystemMetricsSnapshot.gc`); the route
mapper flattens that tuple into the three `collections_gen{N}`
fields below before serializing.

| Field                | Type | Description |
|----------------------|------|-------------|
| `collections_gen0`   | int  | Cumulative GC collection count for generation 0. |
| `collections_gen1`   | int  | Generation 1. |
| `collections_gen2`   | int  | Generation 2. |
| `uncollectable`      | int  | Cumulative count of objects GC could not free. |
| `current_objects`    | int  | Sum of `gc.get_count()` across all generations at sample time. |

### `limits`

| Field              | Type | Description |
|--------------------|------|-------------|
| `rlimit_nproc`     | int  | Soft `RLIMIT_NPROC`. |
| `rlimit_nofile`    | int  | Soft `RLIMIT_NOFILE`. |
| `rlimit_as_bytes`  | int  | Soft `RLIMIT_AS` (address space). |

### `saturation`

Percentage toward exhaustion (0.0 - 1.0). The gradient operators
actually need to see (vs raw counts) — the rate of increase here is
the leading indicator of a thread leak or fd leak.

| Field          | Type          | Description |
|----------------|---------------|-------------|
| `threads_pct`  | float \| null | `num_threads / rlimit_nproc`; `null` when `rlimit_nproc` is `RLIM_INFINITY` or zero. |
| `fds_pct`      | float \| null | `num_fds / rlimit_nofile`; `null` under the same conditions. |

### `db_internal`

| Field                       | Type        | Description |
|-----------------------------|-------------|-------------|
| `aiosqlite_live_connections`| int         | `len(_live_aiosqlite_connections)` — atomic read; each live aiosqlite connection runs its own worker OS thread. SQLite-only tracker; stays `0` on Postgres. |
| `pool_size`                 | int \| null | The primary async engine's queue-pool base size (`QueuePool.size()`), for asyncpg/Postgres and file-backed SQLite. `null` for an in-memory `StaticPool`/`NullPool` or when no engine is injected. |
| `pool_checked_out`          | int \| null | Connections currently checked out of that pool (`QueuePool.checkedout()`, includes overflow). `null` under the same conditions as `pool_size`. |

### `disk`

The snapshotter samples the configured data-partition mount on every
tick and exposes the counters through the REST snapshot. The API
process also publishes the same status as a `system.heartbeats.host.disk`
`HeartbeatData` frame when its shared ZMQ publisher is available. The
existing `critical_system_error` rule consumes that heartbeat, gates on
three consecutive non-HEALTHY frames in its rolling window, dedups by
host/disk/hour, and fans out one alert row per current user SCD2 row whose
named permission set contains `read:system_status`. The current matching sets
are `ai_reviewer`, `ai_delegate`, `viewer`, `operator`, and `admin`; the fan-out
helper excludes deactivated users (`users.is_active = FALSE`).

| Field             | Type                          | Description |
|-------------------|-------------------------------|-------------|
| `mount_path`      | str                           | Mount path passed to `shutil.disk_usage`. |
| `total_bytes`     | int \| null                   | Total bytes on the mount, or `null` if the mount could not be sampled. |
| `used_bytes`      | int \| null                   | Used bytes on the mount, or `null` on sampling failure. |
| `free_bytes`      | int \| null                   | Free bytes on the mount, or `null` on sampling failure. |
| `percent_used`    | float \| null                 | Used percentage, or `null` on sampling failure. |
| `disk_low`        | bool                          | `true` when free bytes are below the warning threshold or the critical threshold. |
| `disk_critical`   | bool                          | `true` when free bytes are below the critical threshold. |
| `status`          | `healthy` \| `warning` \| `error` | `warning` below the warning threshold, `error` below the critical threshold, and `warning` when sampling the mount raises `OSError`. |

### Top-level flags

| Field                 | Type                       | Description |
|-----------------------|----------------------------|-------------|
| `bus_time`            | datetime (UTC)             | Sample timestamp; window queries match against this field. |
| `tracemalloc_active`  | bool                       | `True` iff `tracemalloc.is_tracing()` was `True` at sample time. |
| `cgroup_version`      | `"v1"` \| `"v2"` \| `null` | cgroup detection result; `null` on hosts without cgroup. |

## Configuration

The snapshotter reads its environment variables directly (no
`AppSettings` extension). Defaults match the in-code
constants in
[`snapper.application.system_metrics.snapshotter`](../src/snapper/application/system_metrics/snapshotter.py).

| Variable                               | Default       | Effect |
|----------------------------------------|---------------|--------|
| `SYSTEM_METRICS_INTERVAL_SECONDS`      | `5`           | Seconds between sampler ticks. Empty / unparseable / non-positive falls back to the default. |
| `SYSTEM_METRICS_HISTORY_CAP`           | `17280`       | Ring buffer cap (snapshot count). 17280 ≈ 24h at the default 5s interval. Empty / unparseable / non-positive falls back to the default. |
| `SYSTEM_METRICS_DISK_FREE_WARN_BYTES`  | `21474836480` | Free-byte threshold below which disk status becomes `warning`. |
| `SYSTEM_METRICS_DISK_FREE_CRIT_BYTES`  | `10737418240` | Free-byte threshold below which disk status becomes `error`. |
| `SYSTEM_METRICS_DISK_MOUNT_PATH`       | `/`           | Mount path sampled by `shutil.disk_usage`. Empty falls back to `/`. |

Operators can drop the ring buffer cap to reduce retained history
(720 snapshots is approximately one hour at the default interval). Actual
resident memory depends on Python object overhead and snapshot contents.

Publisher hot-path probes are separate, opt-in diagnostics:

| Variable | Default | Effect |
| -------- | ------- | ------ |
| `SNAPPER_TICK_PROBE` | unset | Logs per-stage tick publisher timing histograms every ~10 seconds when truthy. |
| `SNAPPER_TRADE_PROBE` | unset | Logs per-stage trade publisher timing histograms every ~10 seconds when truthy. |

Use these only during targeted throughput investigations; when unset
the hot path pays only a branch and return.

## Sampling cadence + ring buffer

- Sample interval: configurable, default **5 seconds**.
- Default cap: 17,280 snapshots; no fixed resident-memory byte budget is enforced.
- Eviction: oldest-first via `collections.deque(maxlen=N)` semantics.
- All buffer access goes through an `asyncio.Lock` so concurrent route
  reads see a consistent view (single-writer / multi-reader).

## cgroup detection

Runs at every sample and degrades silently when paths are absent:

- v2 default in modern Docker (kernel 5.0+, Docker Engine 20.10+):
  `/sys/fs/cgroup/memory.max`, `cpu.max`, `cpu.stat`, `memory.current`.
- v1 fallback: `memory/memory.limit_in_bytes`,
  `memory/memory.usage_in_bytes`, `cpu,cpuacct/cpu.cfs_quota_us`
  (or the older `cpu/cpu.cfs_quota_us` mount), and the matching
  `cpu.stat` file for `nr_throttled`.
- `cgroup_version` reports the detected layout; `null` on dev hosts
  without cgroup (macOS, non-containerised Linux without unified
  hierarchy).

## Tracemalloc

Off by default — enabling tracemalloc adds workload-dependent CPU overhead
and holds extra metadata in process memory. A caller with
`manage:runtime_diagnostics` arms it briefly to capture the
`python_traced_bytes` byte counter for the `native_bytes` diagnostic, then
auto-stop fires after a bounded window.

| Knob                       | Default | Cap |
|----------------------------|---------|-----|
| `duration_s` (POST query)  | 600s    | 3600s (clamped) |

Calling `start` while already armed REPLACES the deadline (cancels the
previous timer, starts a fresh one with the new duration). The route
response carries the clamped `requested_duration_seconds`, so the caller sees
what was actually applied.

## Example invocations

Latest snapshot:

```bash
curl -fsS \
  -H "Authorization: Bearer ${SNAPPER_ACCESS_TOKEN}" \
  https://snapper.example.com/api/metrics/system \
  | jq '.payload | {threads_pct: .saturation.threads_pct, rss: .memory.rss_bytes, aiosqlite: .db_internal.aiosqlite_live_connections}'
```

Last hour, capped at 60 samples:

```bash
SINCE=$(date -u -d '-1 hour' +%FT%TZ)
curl -fsS \
  -H "Authorization: Bearer ${SNAPPER_ACCESS_TOKEN}" \
  "https://snapper.example.com/api/metrics/system/history?since=${SINCE}&limit=60" \
  | jq '.count, [.payload[].memory.rss_bytes]'
```

Arm tracemalloc for 5 minutes to inspect the native gap. The token used here
must retain `manage:runtime_diagnostics`:

```bash
curl -fsS -X POST \
  -H "Authorization: Bearer ${SNAPPER_ACCESS_TOKEN}" \
  "https://snapper.example.com/api/metrics/system/tracemalloc/start?duration_s=300" \
  | jq
sleep 30
curl -fsS \
  -H "Authorization: Bearer ${SNAPPER_ACCESS_TOKEN}" \
  https://snapper.example.com/api/metrics/system \
  | jq '.payload.memory | {rss: .rss_bytes, traced: .python_traced_bytes, native: .native_bytes}'
```

## Multi-instance note

The ring buffer is per-process. With `N>1` instances each hosts its own
buffer; aggregation across instances is outside the process-metrics scope.
Operators who need cross-instance views should poll each instance
directly; no cross-instance collector exists.

# Retention

Snapper runs a **policy-driven retention loop** that periodically
archives + purges old rows from event tables, complementing the
existing manual `snapper archive` CLI. Policies are declarative
(`RetentionPolicy(table, retain_days, backlog_lookback_days)`) and
evaluated by `RetentionService` on every scheduler tick.

## Endpoint

| Method | Path                       | Purpose |
|--------|----------------------------|---------|
| GET    | `/api/metrics/retention`   | Most recent scheduler tick's per-policy summary |

The route is gated by `Permission.READ_SYSTEM_STATUS` (same gate as
the system metrics surface).

### Failure / cold-start contract

The route returns HTTP `503` with one of three details:

- `"retention scheduler not available"` — singleton failed to start
  at lifespan time (the lifespan hook leaves
  `app.state.retention_scheduler` as `None` so the route falls
  through to 503).
- `"retention scheduler disabled"` — the operator set
  `RETENTION_DISABLED=true`; the scheduler is parked.
- `"retention scheduler not yet run"` — eager run did not populate
  the summary before the first request (defensive; should not happen
  in production because `start()` awaits the eager `run_once`).

## Policy semantics

For each scheduler tick, with `today_utc = datetime.now(UTC).date()`:

```
oldest_kept_day      = today_utc - retain_days       # rows on this day stay in DB
last_eligible_day    = oldest_kept_day - 1d          # day_end (inclusive)
earliest_scanned_day = last_eligible_day - backlog_lookback_days  # day_start (inclusive)
```

The archiver call covers the inclusive day-range `[day_start, day_end]`,
matching `EventArchiver.export(...)` semantics. Steady state after the
backlog drains processes one new day per tick.

Concrete example with `today_utc = 2026-05-01`, `retain_days = 1`,
`backlog_lookback_days = 30`:

- `oldest_kept_day = 2026-04-30` (rows with `timestamp` on
  2026-04-30 stay in DB).
- `last_eligible_day = 2026-04-29` (last day archived + purged this
  tick).
- `earliest_scanned_day = 2026-03-30` (oldest day touched this tick).
- The archiver query covers
  `[2026-03-30 00:00 UTC, 2026-04-30 00:00 UTC)` per the repository
  semantics.

The shipped default policy:

| Table       | retain_days | backlog_lookback_days |
|-------------|-------------|-----------------------|
| `telemetry` | 1           | 30                    |

## Configuration

| Variable | Default | Effect |
|---|---|---|
| `RETENTION_INTERVAL_SECONDS` | `3600` | Scheduler loop period. Parsed as a positive `float`; empty / unparseable / non-positive values silently fall back to the default. |
| `RETENTION_DISABLED`         | `false` | Truthy (`"1"/"true"/"yes"`, case-insensitive) parks the scheduler entirely. |
| `RETENTION_DRY_RUN`          | `false` | Truthy forces `purge=False` on every archiver call regardless of policy. Operators flip to `true` for one cycle when adding a NEW high-volume policy to verify the window before any DB row is deleted. |
| `RETENTION_OUTPUT_DIR`       | `data`  | Filesystem root passed to `EventArchiver`; matches the `snapper archive` CLI default. |

## Roll-out checklist for a new high-volume policy

1. Add the new `RetentionPolicy` entry to `RETENTION_POLICIES` in
   `src/snapper/application/retention/policies.py`. The `table` must be
   a key in `EVENT_TABLES` except `executions` (append-only — the
   archiver refuses its purge); an invalid policy fails at module import.
2. Set `RETENTION_DRY_RUN=true` for one full interval and inspect
   `GET /api/metrics/retention` — check that the `day_start` /
   `day_end` window matches expectations and `archived_rows` is
   non-zero.
3. If the new policy targets a deeply backlogged table, run a
   one-time `snapper archive --table=<...> --from=<old> --to=<recent>
   --purge` to drain backlog before flipping `RETENTION_DRY_RUN=false`.
4. Flip `RETENTION_DRY_RUN=false`. Subsequent ticks will both archive
   AND purge.

## Multi-instance note

The retention scheduler is single-instance only for v1. With `N>1`
instances, `RETENTION_DISABLED=true` on `N-1` instances avoids
double-archive / double-purge races. A coordinator-based extension is
a future iteration.

## Per-table SCD2 stats (DB metrics)

`GET /api/metrics/db/tables` returns the latest sampled per-table row
counters for every registered event + state SCD2 table. The endpoint
is operator telemetry: at a glance it answers "is `telemetry` growing
as expected", "are there orders accumulating closed versions", "how
many rows are eligible for the next retention cycle".

### Database metrics endpoint

`GET /api/metrics/db/tables` — `READ_SYSTEM_STATUS` permission gate.
Returns a `DbStatsResponse` envelope wrapping a `DbStatsData` payload
with `tables: list[TableStatsItem]` in the sampler's deterministic
order (STATE-table block first alphabetical, EVENT-table block second
alphabetical).

### Database metrics failure / cold-start contract

| State | Status | Detail | `Retry-After` |
|---|---|---|---|
| Snapshotter helper failed before assigning | 503 | `DB metrics snapshotter not initialized` | (none) |
| `DB_METRICS_DISABLED=true` (operator opt-out) | 503 | `DB metrics snapshotter disabled via DB_METRICS_DISABLED` | (none) |
| Started but no sample yet (cold-start) | 503 | `DB metrics snapshotter has not completed a sample yet` | `<interval_seconds>` |
| Healthy | 200 | — | — |

The cold-start 503 window opens at lifespan start and closes only after
the first full sampling pass completes: the sampler sleeps one
`DB_METRICS_INTERVAL_SECONDS` (default 60s) and THEN samples every table
sequentially (30s per-table timeout), so a degraded first pass can run
well past a single interval. By design the sampler does NOT block
lifespan startup on that first sample. `Retry-After` is a polling hint
for frontend dashboards, not an upper-bound SLA — poll-with-backoff.

## Snapshot fields

### `TableStatsItem`

| Field | Type | Notes |
|---|---|---|
| `table` | `str` | Table name. Usually a key in `EVENT_TABLES` or `STATE_TABLES`; `candles` is also sampled (as a state-kind table) despite living outside both archiver registries. |
| `table_kind` | `"event"` \| `"state"` | Discriminates wire semantics. |
| `total` | `int \| null` | Total row count. On PostgreSQL this is a `pg_class.reltuples` planner estimate (`GREATEST(reltuples, 0)` — fast, accurate within `ANALYZE`/autovacuum drift, intended for growth-trend monitoring); on the SQLite dev fixture it is an exact `COUNT(*)`. For state tables the published value is additionally clamped no lower than `current`. `null` only on per-table query failure with no prior sample to clone. |
| `current` | `int \| null` | Active SCD2 versions (rows whose `known_to` equals the SCD2 sentinel) for state tables; `null` for event tables (no SCD2 lifecycle — reporting `0` would imply the dimension exists). |
| `closed` | `int \| null` | Superseded SCD2 versions for state tables; `null` for event tables. |
| `archivable` | `int \| null` | Row count in the policy retention window when a `RETENTION_POLICIES` entry applies; `null` when no policy applies (semantically distinct from `0`). |
| `is_stale` | `bool` | `True` when the row was reused from a prior sample after a per-table query timeout or exception. |
| `last_sampled_at` | `datetime` | UTC timestamp of the row's source sample. On a stale clone this is the original timestamp, NOT the current cycle's clock. |

### `DbStatsData`

| Field | Type | Notes |
|---|---|---|
| `snapshot_started_at` | `datetime` | When the sampler began the cycle. |
| `snapshot_completed_at` | `datetime` | When the sampler finished the cycle. |
| `interval_seconds` | `int` | Echo of the configured cadence. |
| `tables` | `list[TableStatsItem]` | One entry per registered table. |

## Per-kind semantics

- **EVENT tables** (append-only — `ticks`, `trades`, `signals`,
  `executions`, `telemetry`, `control`): `total` comes from the
  dialect-aware row-count primitive — a `pg_class.reltuples` planner
  estimate on PostgreSQL, an exact `COUNT(*)` on the SQLite fixture.
  The `current` / `closed` axes are `null` because event tables have
  no SCD2 lifecycle. `archivable` is non-null only for tables with a
  registered policy (currently only `telemetry`).
- **STATE tables** (SCD2-versioned — `orders`, `positions`,
  `instruments`, etc.): `current = COUNT(known_to == sentinel)`
  counts active SCD2 versions — exact, except on PostgreSQL for
  tables with a registered active-index estimate (currently
  `candles`, via the `uq_candle_itf_open` partial index), where
  `current` is a `pg_class.reltuples` planner estimate; on the
  SQLite fixture it is always exact; `total` comes from the
  dialect-aware row-count primitive (PG planner estimate / SQLite
  exact); `total` is clamped no lower than `current`
  (`total = max(total, current)`) to absorb stale PostgreSQL planner
  estimates where the exact `current` temporarily exceeds the
  estimated `total`; `closed = max(0, total - current)` is then
  derived from the clamped `total` (so it can never actually go
  negative), and panels always see `Total >= Current >= 0` while
  PostgreSQL `total` and `closed` remain estimates. `archivable`
  is `null` until a policy is registered for the table.

## Database metrics and retention alignment

The `archivable` counter computes the same window as the retention service's
`compute_retention_window(today_utc, policy)`: the half-open
`timestamp >= day_start midnight UTC AND timestamp < day_end + 1d
midnight UTC` predicate, with the policy's
`(retain_days, backlog_lookback_days)` knobs. This means dashboard
`archivable` and retention export use the same eligibility boundaries. Counts
can differ between sampling and export if rows arrive or are purged, or the
UTC day changes; equality requires the same data and evaluation day. The
`tests/application/db_stats/test_cluster_alignment.py` integration
test pins the equality contract.

## Database metrics configuration

| Variable | Default | Effect |
|---|---|---|
| `DB_METRICS_INTERVAL_SECONDS` | `60` | Sampler loop period. Empty / unset → default; a non-integer or out-of-range value raises `ValueError` at startup. |
| `DB_METRICS_DISABLED` | `false` | Truthy (`"1"/"true"/"yes"`, case-insensitive) parks the snapshotter entirely. PG operators with deployments lacking `psycopg2` in deps boot cleanly with this flag (the underlying repo factory is never called). |

`PER_TABLE_TIMEOUT_SECONDS` is a module constant (30s, not env-
configurable for v1). On per-table timeout, the snapshotter clones
the prior `TableStats` with `is_stale=True`; if no prior sample
exists the row carries all-null counters. Per-table failures NEVER
abort the sampler tick.

## Sampling order + cold-start

STATE tables sample first (alphabetical by name), EVENT tables second.
Atomic swap of `_latest_snapshot` happens at the END of the cycle —
readers wait one full `interval_seconds` before the first 200,
regardless of order. STATE-first ordering is for resilience to EVENT
timeouts: if a heavy EVENT table (e.g. `ticks` at 50M rows) times
out, STATE counters were already computed and the snapshot still
publishes with EVENT entries marked `is_stale=True` from a prior run.

## `telemetry.timestamp` index

The database metrics `archivable` query on `telemetry` filters on
`Telemetry.timestamp` (the bus-time column inherited from
`TemporalMixin`). Without an index on that column, the query is a
full table scan over a high-volume audit table. The
`ix_telemetry_timestamp` index over `Telemetry.timestamp` is part
of the consolidated `0001_init` migration
(`src/snapper/data/migrations/versions/0001_init.py`), so both
the database metrics counter and the retention scan run
in `O(log n + k)` (an index-assisted seek plus a scan of the `k`
matching entries) from the first deployed schema.
