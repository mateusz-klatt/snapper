# Observability

Snapper exposes operator-facing health metrics so a dashboard can see
the gradient toward exhaustion before a crash. The current observability
surface is **Cluster A** of the three-cluster monitoring initiative:
process-level counters sampled into an in-memory ring buffer with a
REST query API.

> Cluster B (per-table SCD2 DB stats) and Cluster C (retention policy
> framework) are sequenced after this. C lands before B because C
> defines what "archivable" means.

## Endpoints

All four routes are gated by `Permission.READ_SYSTEM_STATUS` (held by
`AI_DELEGATE`, `VIEWER`, `OPERATOR`, `ADMIN`). The two `POST` routes
also require CSRF (cookie-auth path); `Authorization: Bearer` requests
bypass CSRF per the project-wide auth contract.

| Method | Path                                                | Purpose |
|--------|-----------------------------------------------------|---------|
| GET    | `/api/metrics/system`                               | Latest snapshot |
| GET    | `/api/metrics/system/history?since&until&limit`     | Windowed slice of the ring buffer |
| POST   | `/api/metrics/system/tracemalloc/start?duration_s`  | Arm Python tracemalloc with auto-stop deadline |
| POST   | `/api/metrics/system/tracemalloc/stop`              | Disarm tracemalloc + cancel pending deadline |

### Failure contract

If the snapshotter singleton failed to start at lifespan time, the
`app.state.system_metrics_snapshotter` attribute stays `None` and every
metrics route returns HTTP `503` with body
`{"detail": "system metrics snapshotter not available"}`. The rest of
the application continues to serve other endpoints.

### Cold-start contract

`SystemMetricsSnapshotter.start()` takes one eager synchronous sample
BEFORE returning, so the first request after lifespan startup completes
hits a populated buffer.

## Snapshot fields

Each sample captures eight nested groups plus two top-level flags. All
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
| `aiosqlite_live_connections`| int         | `len(_live_aiosqlite_connections)` — atomic read; each entry corresponds to one OS thread under NullPool. |
| `pool_size`                 | int \| null | SQLAlchemy pool size when using a queue pool (PG / `DB_POOL_MODE=queue`); `null` under NullPool. |
| `pool_checked_out`          | int \| null | Pool entries currently checked out; `null` under NullPool. |

### Top-level flags

| Field                 | Type                       | Description |
|-----------------------|----------------------------|-------------|
| `bus_time`            | datetime (UTC)             | Sample timestamp; window queries match against this field. |
| `tracemalloc_active`  | bool                       | `True` iff `tracemalloc.is_tracing()` was `True` at sample time. |
| `cgroup_version`      | `"v1"` \| `"v2"` \| `null` | cgroup detection result; `null` on hosts without cgroup. |

## Configuration

The snapshotter reads two environment variables directly (no
`AppSettings` extension this iteration). Defaults match the in-code
constants in
[`snapper.application.system_metrics.snapshotter`](../src/snapper/application/system_metrics/snapshotter.py).

| Variable                          | Default | Effect |
|-----------------------------------|---------|--------|
| `SYSTEM_METRICS_INTERVAL_SECONDS` | `5`     | Seconds between sampler ticks. Empty / unparseable / non-positive falls back to the default. |
| `SYSTEM_METRICS_HISTORY_CAP`      | `17280` | Ring buffer cap (snapshot count). 17280 ≈ 24h at the default 5s interval. Empty / unparseable / non-positive falls back to the default. |

Operators can drop the ring buffer cap to reduce resident memory
(720 ≈ 1h ≈ 360 KB).

## Sampling cadence + ring buffer

- Sample interval: configurable, default **5 seconds**.
- Per-snapshot byte budget: ~500 bytes (≈30 metrics, ~15 bytes each).
- Default cap 17280 snapshots ≈ **8.6 MB resident**.
- Eviction: oldest-first via `collections.deque(maxlen=N)` semantics.
- All buffer access goes through an `asyncio.Lock` so concurrent route
  reads see a consistent view (single-writer / multi-reader).

## cgroup detection

Runs at every sample and degrades silently when paths are absent:

- v2 default in modern Docker (kernel 5.0+, Docker Engine 20.10+):
  `/sys/fs/cgroup/memory.max`, `cpu.max`, `cpu.stat`, `memory.current`.
- v1 fallback: `memory/memory.max_usage_in_bytes`,
  `cpu/cpu.cfs_quota_us`, `cpu/cpu.cfs_period_us`, `cpu/cpu.stat`,
  `memory/memory.usage_in_bytes`.
- `cgroup_version` reports the detected layout; `null` on dev hosts
  without cgroup (macOS, non-containerised Linux without unified
  hierarchy).

## Tracemalloc

Off by default — enabling tracemalloc costs **5-10% CPU** while active
and holds extra metadata in process memory. Operators arm it briefly to
capture the `python_traced_bytes` byte counter for the `native_bytes`
diagnostic, then auto-stop fires after a bounded window.

| Knob                       | Default | Cap |
|----------------------------|---------|-----|
| `duration_s` (POST query)  | 600s    | 3600s (clamped) |

Calling `start` while already armed REPLACES the deadline (cancels the
previous timer, starts a fresh one with the new duration). The route
response carries the clamped `requested_duration_seconds`, so the
operator sees what was actually applied.

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

Arm tracemalloc for 5 minutes to inspect the native gap:

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
buffer; aggregation across instances is out of scope for Cluster A.
Operators who need cross-instance views should poll each instance
directly until a downstream collector lands (Cluster B/C territory).
