# CLI

Snapper provides a command-line interface built on the Typer framework.
All commands are available through the `snapper` command.

## Usage

```bash
snapper [COMMAND] [OPTIONS]
```

Help for a specific command:

```bash
snapper [COMMAND] --help
```

## Server and Infrastructure

### `server`

Starts the FastAPI server with the web dashboard.

```bash
snapper server [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `--host` | string | from env | Listen address |
| `--port` | int | from env | Server port |
| `--reload` / `--no-reload` | bool | from env (`SERVER_RELOAD`) | Auto-reload for development; explicit flag overrides the env-var setting |

**Environment:**

Set `SERVER_API_ONLY=true` to skip process autostart while keeping
the ZMQ-WebSocket bridge alive (useful when the engine runs as a
separate process). Multi-worker uvicorn (multiple FastAPI processes
sharing a broker) is supported — see the AI-review fanout dedup
contract in `docs/architecture.md` (Deployment Modes); set
`SNAPPER_COORDINATOR_INSTANCE_ID` + `SNAPPER_COORDINATOR_INSTANCE_COUNT`
per worker (env vars only — `snapper server` does not expose
per-worker CLI flags; the `--instance-id` / `--instance-count` flags
in the table below are on `snapper trade-zmq`) to enable the shared
partitioning that prevents duplicate caps-violation WS frames under
N>1.

**Examples:**

```bash
# Start with default settings (all-in-one dev mode)
snapper server

# Start on all interfaces
snapper server --host 0.0.0.0 --port 8000

# Development mode with auto-reload
snapper server --reload

# API-only mode (engine runs separately)
SERVER_API_ONLY=true snapper server
```

### `broker`

Starts the ZeroMQ XPUB/XSUB broker for message routing.

```bash
snapper broker [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `--xsub` | string | from env | XSUB endpoint (publishers) |
| `--xpub` | string | from env | XPUB endpoint (subscribers) |

**Example:**

```bash
snapper broker --xsub tcp://127.0.0.1:7500 --xpub tcp://127.0.0.1:7501
```

### `trade-zmq`

Starts the central trade runtime coordinator.

The process still runs as the `trade-zmq` command, but after the trade
runtime redesign it also hosts `TradeService`, `BalanceService`, the
outbox dispatcher, and the reconciliation loops. It writes
`TradeCommand` rows, consumes `orders.events.*` to keep projections in
sync, and persists checkpoints for recovery.

```bash
snapper trade-zmq [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `--signal-topics` | string | `signals.` | Signal topics to subscribe |
| `--instance-id` | int | `0` (or `$SNAPPER_COORDINATOR_INSTANCE_ID`) | Multi-instance partitioning: zero-based id for this coordinator in a multi-instance deployment. CLI flag > env var > default. |
| `--instance-count` | int | `1` (or `$SNAPPER_COORDINATOR_INSTANCE_COUNT`) | Multi-instance partitioning: total coordinator instance count across the deployment. Must match across all coordinators. |

**Example:**

```bash
snapper trade-zmq --signal-topics "signals.paper.BTC-USD.rsi_btc_1h,signals.kraken.BTC-USD.live"
```

**Multi-instance deployment:**

```bash
# Coordinator 0 of 2
snapper trade-zmq --instance-id 0 --instance-count 2

# Coordinator 1 of 2 (separate process, same DB + broker)
snapper trade-zmq --instance-id 1 --instance-count 2
```

Each instance owns `~1/N` of the shard_keys via deterministic
SHA-256 hashing. Default `--instance-count 1` is byte-identical to
single-coordinator behavior (every shard owned by the single
coordinator).

Multi-instance deployment requires PostgreSQL. With
`--instance-count` greater than 1 on a SQLite backend the coordinator
fails fast at startup with a `ValueError`: SQLite compiles
`SELECT ... FOR UPDATE` to a plain SELECT, so cross-instance row
claims cannot serialize. A single-instance SQLite coordinator is
allowed but logs a startup warning about the same locking caveat.

See [docs/operations.md](operations.md) for systemd template unit +
scale-up/down/crash-recovery runbooks.

### `executor`

Starts the standalone order execution service for a selected exchange.
Process-managed deployments use per-wallet executor instances instead:
bare `executor_<exchange>` process configs are templates, and the
launcher expands active `wallet_credentials` rows into runnable
`executor_<exchange>_w<wallet_short>` instances. Start/stop the generated
per-wallet instance names in the process UI/API; starting a bare executor
template is rejected.

```bash
snapper executor [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `-e, --exchange` | string | `kraken` | Exchange (`kraken`, `walutomat`) |

`wallet_short` is the last 12 lowercase hex characters of the wallet
UUID7. Each per-wallet instance subscribes to the same exchange command
prefix as the template, then filters by `wallet_public_id` before loading
credentials or placing orders.

**Examples:**

```bash
# Kraken executor
snapper executor -e kraken

# Walutomat executor
snapper executor --exchange walutomat
```

### `feed`

Starts the direct Kraken market data publisher helper via WebSocket.
For the dedicated feed-container topology, use `snapper feed-engine`
instead; it starts every enabled market-data publisher from the process
registry as a supervised subprocess.

```bash
snapper feed [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `--symbols` | string | `BTC-USD` | Comma-separated native symbols passed to the Kraken publisher. Symbols must be in native dash format (e.g. `BTC-USD`); slash-format WebSocket symbols are skipped as unknown. |

**Example:**

```bash
snapper feed --symbols "BTC-USD,ETH-USD"
```

### `feed-engine`

Runs the dedicated feed-container entrypoint. It syncs the process
registry, starts enabled market-data publishers as OS subprocesses,
and exits non-zero if any supervised publisher crashes so the
orchestrator restarts the feed container. It does not start a broker;
publisher subprocesses connect to the backend's configured
`ZMQ_BROKER_*` endpoints. Per-process RSS/CPU summary events are emitted
best-effort when the backend broker is reachable, but metrics-publisher
setup failure does not abort feed startup.

```bash
snapper feed-engine
```

### `zmq-logger`

Monitors and logs ZMQ message traffic.

```bash
snapper zmq-logger [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `--payload/--no-payload` | bool | `false` | Log full payload |
| `--file/--no-file` | bool | `true` | Write to audit file |
| `--audit-file` | string | `data/zmq_audit.jsonl` | Audit file path |
| `--max-length` | int | `200` | Max payload preview length |

**Example:**

```bash
snapper zmq-logger --payload --audit-file logs/zmq.jsonl
```

### `egress`

Runs the snapper-egress sidecar from the unified Snapper image.
Additional arguments after `snapper egress` are forwarded verbatim to
the egress entrypoint's argparse parser. This is the compose dispatch
path for `ENTRYPOINT ["snapper"]` plus `command: ["egress"]`; the module
entrypoint `python -m snapper.egress` remains supported for bare-shell
invocations. The Typer wrapper propagates the egress entrypoint's
integer return code as the process exit code.

```bash
snapper egress [--instance-id snapper-egress-prod]
```

See [snapper-egress.md](snapper-egress.md) for tunnel settings and
verification steps.

## Database

### `db-init`

Initializes the database schema.

```bash
snapper db-init
```

### `db-upgrade`

Runs Alembic migrations to a specified revision.

```bash
snapper db-upgrade [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `--revision` | string | `head` | Target revision |

**Examples:**

```bash
# Migrate to latest version
snapper db-upgrade

# Migrate to specific revision
snapper db-upgrade --revision abc123
```

### `db-downgrade`

Rolls back Alembic migrations.

```bash
snapper db-downgrade [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `--revision` | string | `-1` | Steps to roll back |

**Examples:**

```bash
# Roll back one migration
snapper db-downgrade

# Roll back to specific revision
snapper db-downgrade --revision base
```

### `db-seed`

Seeds the database with environment-specific data from TOML profiles.
Automatically run by `make migrate-dev` and `make migrate-prod`.

```bash
snapper db-seed [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `--profile` | string | `dev` | Seed profile name (e.g. dev, prod) |

## Users

### `init-admin`

Creates the initial administrator user.

```bash
snapper init-admin [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `--username` | string | `admin` | Username |
| `--password` | string | _(prompted, hidden, confirmed)_ | Password — no default; the command prompts with hidden input and confirmation when the flag is omitted, so the value never lands in shell history. |

**Example:**

```bash
# Prompts for password with hidden input + confirmation; no inline value
# means nothing lands in shell history
snapper init-admin --username admin
```

### `list-users`

Displays a list of all system users.

```bash
snapper list-users
```

### `reset-password`

Resets a user's password.

```bash
snapper reset-password USERNAME [OPTIONS]
```

**Arguments:**

- `USERNAME` — Username (required)

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `--new-password` | string | (prompt) | New password |

**Example:**

```bash
# Prompts for the new password with hidden input + confirmation; the secret
# never lands in shell history when --new-password is omitted
snapper reset-password john
```

## Symbol Synchronization

### `update-kraken-symbols`

Synchronizes symbol mappings from the Kraken API.

```bash
snapper update-kraken-symbols [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `-f, --force` | bool | `false` | Force update |

### `update-kraken-futures-symbols`

Syncs Kraken Futures crypto symbol mappings from the exchange API.

```bash
snapper update-kraken-futures-symbols [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `-f, --force` | bool | `false` | Force update |

### `update-kraken-equities-symbols`

Syncs Kraken Equities (FCM Futures) symbol mappings from the exchange
API.

```bash
snapper update-kraken-equities-symbols [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `-f, --force` | bool | `false` | Force update |

### `update-walutomat-symbols`

Synchronizes symbol mappings from the Walutomat API.

```bash
snapper update-walutomat-symbols [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `-f, --force` | bool | `false` | Force update |

### `update-polygon-symbols`

Synchronizes symbol mappings from the Polygon.io API. By default it
updates existing rows only; pass `--insert-new` to insert symbols that
are not yet in the database.

```bash
snapper update-polygon-symbols [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `-f, --force` | bool | `false` | Force update |
| `--insert-new` | bool | `false` | Insert new symbols |

**Note:** This operation may take 10-15 minutes (44k+ symbols).

### `update-underlyings`

Syncs underlying asset definitions from YAML to the database. It matches
active instruments to underlyings via pattern rules, upserts mappings,
and applies YAML-fallback `instrument_type` / `expiry_override` values to
`InstrumentSpec` rows when API-sourced values are NULL.

```bash
snapper update-underlyings [OPTIONS]
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `-f, --force` | bool | `false` | Bypass safety guard for stale cleanup |

### `reconcile-symbol-aliases`

Closes active `symbol_aliases` rows whose owning capability row is no
longer tradeable and no longer market-data-enabled. The command is
idempotent and can be scoped to one exchange.

```bash
snapper reconcile-symbol-aliases [OPTIONS]
```

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `-e, --exchange` | string | `all` | Exchange to reconcile (`kraken`, `kraken_futures`, `kraken_equities`, `walutomat`, `polygon`, or `all`) |

## Market Snapshots

### `update-kraken-market-snapshot`

Updates Kraken market snapshots with current prices.

```bash
snapper update-kraken-market-snapshot
```

### `update-kraken-futures-market-snapshot`

Updates Kraken Futures market snapshots with current prices.

```bash
snapper update-kraken-futures-market-snapshot
```

### `update-kraken-equities-market-snapshot`

Updates Kraken Equities market snapshots with current prices.

```bash
snapper update-kraken-equities-market-snapshot
```

### `update-kraken-futures-funding-rates`

Backfills historical funding rates for Kraken Futures perpetuals
into the `funding_rates` table.  Duplicates are silently skipped via
the partial unique index.

```bash
snapper update-kraken-futures-funding-rates [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `--symbol` / `-s` | list[str] | None | One or more native perpetual symbols (`-s BTC-USD-PERP -s ETH-USD-PERP`) |
| `--all` | bool | `false` | Backfill every mapped Kraken Futures perpetual |

### `update-walutomat-market-snapshot`

Updates Walutomat market snapshots with current prices.

```bash
snapper update-walutomat-market-snapshot
```

## Data Backfill

### `polygon-backfill-aggregates`

Downloads historical aggregated data from the Polygon.io API to the
on-disk CSV cache. This command is download-only: it writes CSV files
and never touches the database. Populating the `candles` table is the
separate second step, handled by [`polygon-load-csv`](#polygon-load-csv).

The full workflow is two steps:

1. `polygon-backfill-aggregates` downloads candles into the CSV cache.
2. `polygon-load-csv` reads that cache and upserts the candles into the
    database (no Polygon API calls).

```bash
snapper polygon-backfill-aggregates [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `-s, --symbol` | string[] | from settings | Symbols to download |
| `--all` | bool | `false` | All mapped symbols |
| `-m, --multiplier` | int | `1` | Timeframe multiplier |
| `-t, --timespan` | string | `minute` | Timespan (`minute`, `hour`, `day`) |
| `-d, --days` | int | `None` (effective default `30`) | Days back |
| `--resume/--no-resume` | bool | `true` | Skip days already cached on disk |
| `--csv/--no-csv` | bool | `true` | Save to CSV files |

**Examples:**

```bash
# Download specific symbols to the CSV cache
snapper polygon-backfill-aggregates -s AAPL -s MSFT --days 365

# Download all mapped symbols
snapper polygon-backfill-aggregates --all --timespan day

# Hourly download, skipping already-cached days
snapper polygon-backfill-aggregates -s SPY -t hour -m 1 --resume

# Then load the downloaded cache into the database
snapper polygon-load-csv --all --timespan day
```

### `polygon-load-csv`

Loads candles from the on-disk Polygon CSV cache into the database. This
is the cache-only counterpart to `polygon-backfill-aggregates`: it reads
the CSV files produced by that command and upserts the candles into the
`candles` table. It never contacts the Polygon API.

Run this after `polygon-backfill-aggregates` has populated the cache.
Archive-symbol cache directories with no current `Symbol` identity are
skipped with a warning, so a stale cache directory never aborts the load.

When neither `--symbol` nor `--all` is given, the symbols default to the
settings-configured Polygon instruments (the `instruments` setting). The
wildcard sentinel `["*"]` in that setting is treated like `--all` and
loads every cached archive symbol; an explicit settings list loads only
those symbols.

```bash
snapper polygon-load-csv [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `-s, --symbol` | string[] | from settings | Symbols to load (native or Polygon format) |
| `--all` | bool | `false` | Load every archive symbol present in the cache |
| `-t, --timespan` | string | `day` | Timespan subtree to read (`minute`, `hour`, `day`) |
| `--since` | string | `None` | Earliest day to load, inclusive (`YYYY-MM-DD`) |
| `--until` | string | `None` | Latest day to load, inclusive (`YYYY-MM-DD`) |

**Examples:**

```bash
# Load every cached minute symbol into the database
snapper polygon-load-csv --all --timespan minute

# Load a single symbol within a date window
snapper polygon-load-csv -s X:BTCUSD --since 2024-01-01 --until 2024-01-31
```

### `polygon-repair-splits`

Detects and repairs stale split-price bases in polygon equities. The
incremental CSV cache freezes each day's file at the price-adjustment
basis of its fetch time, so a stock split executed while the cache is
being collected leaves an unadjusted discontinuity in both the cache
and the database. This command automates the repair chain proven in
the 2026-07-02 audit:

1. Pulls split events from the Polygon reference endpoint for the
    trailing `--lookback-days` window and maps them onto active
    polygon instruments (crypto/FX never split and are skipped by
    construction).
2. Confirms via the current `1d` close history which symbols actually
    carry a consecutive-close break matching the split's ratio — the
    break sits at the fetch boundary between cache waves, not
    necessarily at the execution date. Symbols whose whole history was
    fetched after the split are already uniform and are skipped, so
    the command is idempotent and safe to run on every fetch cycle.
3. For confirmed symbols only: re-fetches the full `--window-days`
    range (adjusted as of today), prunes cache files older than the
    window (the plan's minute-data lookback limit means they can never
    be refreshed and would reintroduce the stale basis on a future
    full-range load), SCD2-supersedes every current candle row,
    reloads the refreshed cache, re-synthesizes the higher timeframes,
    and re-runs the detector.

Two safety behaviors to know about:

- Micro-splits whose ratio is closer to 1.0 than 1.4 (e.g. `20:21`,
    `1000:1061`) are reported as `undetectable` and skipped — an
    ordinary flat close would match them forever, so a price detector
    cannot repair them without endless churn; verify those manually.
- An EMPTY current `1d` history for a candidate confirms the repair
    (it is the signature of a run that crashed between supersede and
    reload), so simply rerunning the command self-heals a partial
    repair.

Exits non-zero when a repaired symbol still shows the matching break
afterwards. MUST run on the host, not in a container — the fetch and
prune steps write the `data/polygon` CSV cache, which containers mount
read-only (for the same reason this service is deliberately not
registered with the process manager).

```bash
snapper polygon-repair-splits [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `-s, --symbol` | string[] | all equities | Restrict the check to these native symbols |
| `--lookback-days` | int | `45` | Trailing window of split executions to inspect |
| `--window-days` | int | `730` | Re-fetch window in days (plan minute-data lookback limit) |
| `--dry-run` | bool | `false` | Detect and report stale split bases without repairing |

**Examples:**

```bash
# After each fetch cycle: check recent splits, repair what is broken
snapper polygon-repair-splits

# Preview without mutating anything
snapper polygon-repair-splits --dry-run

# Targeted re-check of one symbol
snapper polygon-repair-splits -s NFLX
```

### `polygon-load-grouped-candles`

Loads the on-disk Polygon **grouped-daily** cache into the database as
`1d` candles tagged `source='native', complete=True`. This is the
cache-only counterpart to `polygon-backfill-grouped` (which only writes
CSV): it reads the grouped-daily cache — the same corpus the strategy
warmup reads — and upserts each leg's days into the `candles` table so the
persisted plane holds the daily history the single-source candle read path
and the DB-first warmup depend on. It never contacts the Polygon API.

`--exchange` is **required**: it is the venue the persisted `1d` bars live
under, which **must** be the same venue the leg's live read and warmup
resolve (e.g. `kraken` for FET/RENDER) — *not* `polygon`. Instrument
identity is per `(symbol, exchange)`; live synthesized higher-TF bars
persist under the live publisher's venue, so a Polygon-keyed backfill would
be an orphaned plane that no live read or warmup ever sees. `polygon` here
is only the CSV cache segment that supplies the OHLCV.

`--cut-date` is **required**: it is the first UTC day that synthesized live
persistence may own. The backfill writes only days strictly *before* it, so
under one venue native backfilled history and synthesized live bars never
share a bar key (the candle unique key excludes `source`). Before writing,
an ownership preflight — reading back under the same venue — fails closed if
the persisted plane already holds a synthesized `1d` row before the cut date
or a native `1d` row at/after it, so the operator resolves any overlap
rather than silently version-thrashing.

When neither `--symbol` nor `--all` is given, the symbols default to the
settings-configured Polygon instruments; the wildcard sentinel `["*"]` is
treated like `--all` and enumerates every Polygon-mapped native symbol.

```bash
snapper polygon-load-grouped-candles --exchange VENUE --cut-date YYYY-MM-DD [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `-e, --exchange` | string | *required* | Venue the persisted bars live under (e.g. `kraken`); the leg's live-read/warmup venue, never `polygon` |
| `--cut-date` | string | *required* | First UTC day synthesis may own (`YYYY-MM-DD`); writes only days before it |
| `-s, --symbol` | string[] | from settings | Native symbols to load |
| `--all` | bool | `false` | Load every Polygon-mapped native symbol |
| `--lookback-days` | int | `800` | Calendar-day cap on the backward cache walk per symbol |

**Examples:**

```bash
# Load FET/RENDER daily history (under the kraken venue) before the cutover boundary
snapper polygon-load-grouped-candles --exchange kraken --cut-date 2026-06-16 -s FET-USD -s RENDER-USD

# Or via Make (EXCHANGE and CUT_DATE are required; unset fails fast)
make run-polygon-grouped-candles EXCHANGE=kraken CUT_DATE=2026-06-16
```

### `verify-candle-coverage`

Read-only gate that verifies the persisted `candles` plane is ready to be the
single source for `>1m` reads before the read-path cutover. For each
`(symbol, timeframe)` under `--exchange` it RANGE-reads an explicit canonical
window and checks the FULL expected slot grid (not just the newest rows) is
fresh, gap-free, complete and correctly-tagged — `1d` is `native` strictly
before `--cut-date` and `synthesized` at/after, every other higher timeframe is
`synthesized`. Exits `1` (and prints the failing `(symbol, timeframe)` reasons)
when any pair is incomplete, so it can gate the cutover and the DB-first warmup;
exits `0` when every pair passes.

It reads under the live venue — the same `--exchange` the read path and warmup
resolve; passing `-e polygon` is rejected (that plane is orphaned). For `1d` the
window is extended back across the `--cut-date` seam, so the §4e invariant is
enforced directly: a `[cut_date, publisher_start)` gap (synthesis not yet
persisting from the cut) surfaces as a missing-bar failure rather than silently
staling the warmup. `--min-bars` is the window DEPTH verified — set it to the
consumer's required lookback (e.g. the warmup depth) to verify that far back.

The `derive_snaps` OHLCV-parity rung (the "no derive regression" check) needs a
genuinely live, SUB-socket-warm cache as an independent witness, so the
standalone CLI runs **structural checks only**; parity is deferred to an
in-process slice-4 cutover check.

```bash
snapper verify-candle-coverage --exchange VENUE --cut-date YYYY-MM-DD [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `-e, --exchange` | string | *required* | Live venue the persisted bars + read path resolve under |
| `--cut-date` | string | *required* | Synthesis ownership boundary (`YYYY-MM-DD`); decides `1d` provenance |
| `-s, --symbol` | string[] | from settings | Native symbols to verify |
| `-t, --timeframe` | string[] | `5m 15m 30m 1h 4h 1d` | Timeframes to verify |
| `--min-bars` | int | `30` | Window depth (canonical slots) verified per pair |
| `--writer-lag-seconds` | int | `120` | Grace seconds for the writer to flush the just-closed bar |

**Examples:**

```bash
# Gate the FET/RENDER plane before the single-source cutover
snapper verify-candle-coverage --exchange kraken --cut-date 2026-06-16 -s FET-USD -s RENDER-USD

# Verify only the daily plane
snapper verify-candle-coverage -e kraken --cut-date 2026-06-16 -s FET-USD -t 1d
```

### `kraken-futures-backfill-candles`

Backfills historical OHLCV candles for Kraken Futures perpetuals.

```bash
snapper kraken-futures-backfill-candles [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `-s, --symbol` | str[] | None | Native symbols to backfill (e.g. `BTC-USD-PERP`) |
| `--all` | bool | `false` | Backfill all mapped Kraken Futures symbols |
| `-t, --timeframe` | str | `1h` | Candle interval (`1m`, `1h`, `4h`, `1d`) |
| `-d, --days` | int | `90` | Days back to fetch |
| `--resume` / `--no-resume` | bool | `--resume` | Resume from latest stored candle |

### `kraken-equities-backfill-candles`

Backfills historical OHLCV candles for Kraken Equities (TradFi FCM)
contracts via the internal `iapi.kraken.com` ticker-history endpoint.
Response is ~10-minute delayed per FCM policy. Per-chunk upstream
failures raise `RuntimeError` so outages are distinguishable from
legitimately-empty candle windows.

```bash
snapper kraken-equities-backfill-candles [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `-s, --symbol` | str[] | None | Native symbols to backfill (e.g. `MNQM6-CME`) |
| `--all` | bool | `false` | Backfill all mapped Kraken Equities symbols |
| `-t, --timeframe` | str | `1h` | Candle interval (`1m`, `5m`, `15m`, `30m`, `1h`, `1d`) |
| `-d, --days` | int | `30` | Days back to fetch |
| `--resume` / `--no-resume` | bool | `--resume` | Resume from latest stored candle |

### `backfill-candles-from-trades`

Backfills calculated 1-minute candles from persisted trades.

```bash
snapper backfill-candles-from-trades --exchange EXCHANGE --start START --end END [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `--exchange` | str | *required* | Exchange whose trades should be aggregated |
| `--start` | str | *required* | Inclusive UTC start date or datetime |
| `--end` | str | *required* | Inclusive UTC end date or datetime |
| `-s, --symbol` | str[] | None | Native symbols to backfill |
| `--all` | bool | `false` | Backfill all active symbols on the exchange |

### `backfill-synthesized-candles`

Backfills synthesized higher-timeframe candles from persisted 1m candles.

```bash
snapper backfill-synthesized-candles --exchange EXCHANGE --start START --end END [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `--exchange` | str | *required* | Exchange whose 1m candles should be aggregated |
| `--start` | str | *required* | Inclusive UTC 1m candle lower bound (date or datetime) |
| `--end` | str | *required* | UTC upper bound used to seal higher-timeframe windows |
| `-s, --symbol` | str[] | None | Native symbols to backfill |
| `--all` | bool | `false` | Backfill all active symbols on the exchange |
| `--timeframes` | str | `5m,15m,30m,1h,4h,1d` | Comma-separated higher timeframes to synthesize |
| `--cut-date` | str | None | UTC date (`YYYY-MM-DD`) where synthesized `1d` ownership begins |

### `build-continuous`

Builds and displays a continuous-contract series for an underlying.

```bash
snapper build-continuous TICKER EXCHANGE CONTRACT_FAMILY [OPTIONS]
```

**Arguments:**

| Argument | Description |
| -------- | ----------- |
| `TICKER` | Underlying asset ticker (e.g. `SPX`, `GOLD`) |
| `EXCHANGE` | Exchange to source contracts from |
| `CONTRACT_FAMILY` | Product root (e.g. `ES`, `GC`) |

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `-t, --timeframe` | str | `1d` | Candle timeframe |
| `-m, --method` | str | `panama` | Adjustment method (`unadjusted`, `ratio`, `panama`) |
| `--start` | str | 365 days ago | Series start time (ISO) |
| `--end` | str | now | Series end time (ISO) |
| `--rollover-days` | int | `0` | Days before expiry to roll (0..365) |

### `polygon-backfill-grouped`

Downloads grouped daily data from the Polygon.io API to the CSV cache.
Like `polygon-backfill-aggregates`, this is download-only and writes CSV
files without touching the database.

```bash
snapper polygon-backfill-grouped [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `--market` / `-m` | str | `crypto` | Market type (`crypto`, `stocks`, `fx`) |
| `--days` / `-d` | int | `3` | Number of recent days to fetch |
| `--locale` / `-l` | str | `global` | Market locale (`global`, `us`) |
| `--csv` / `--no-csv` | flag | `--csv` | Save data to CSV files |
| `--adjusted` / `--unadjusted` | flag | `--adjusted` | Use adjusted prices |

## Data Archive

### `archive`

Export data to CSV archive files.  Supports candle cache projection,
append-only event tables (`ticks`, `trades`, `signals`, `executions`,
`telemetry`, `control`), and SCD2 state tables (`orders`, `positions`,
`instruments`, `instrument_specs`, `settings`, `symbols`,
`symbol_aliases`, `symbol_exchange_capabilities`, `users`,
`user_login_events`, `market_snapshots`, `process_runs`,
`underlying_assets`, `instrument_underlying_mappings`,
`continuous_contract_configs`).  Merges with existing files and
deduplicates rows.

```bash
snapper archive [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--table` | TEXT | `candles` | Table to archive. Accepts `candles`, `candles-audit`, any event table (`ticks`, `trades`, `signals`, `executions`, `telemetry`, `control`), or any state table (see the section intro for the full list). |
| `--exchange` | TEXT | None | Exchange filter (e.g. `polygon`, `kraken`) |
| `--symbol` | TEXT | None | Native symbol filter (e.g. `BTC-USD`), resolved to stable archive_symbol |
| `--timeframe` | TEXT | `1m` | Candle timeframe (`1m`, `5m`, `1h`, `1d`) — `candles` and `candles-audit` only |
| `--day` | str | None | Single day to archive (`YYYY-MM-DD`; parsed in the handler) |
| `--from` | str | None | Start of date range (`YYYY-MM-DD`; parsed in the handler) |
| `--to` | str | None | End of date range (`YYYY-MM-DD`; parsed in the handler) |
| `--dry-run` | FLAG | False | Report counts without writing files |
| `--purge` | FLAG | False | Delete exported rows from DB after writing (event tables, plus `candles-audit`/state tables when combined with `--closed-only`) |
| `--closed-only` | FLAG | False | Only closed SCD2 versions (`candles-audit` and state tables) |
| `--output-dir` | str | `data` | Base output directory |

**Output structure:**

```text
Candles:  {output_dir}/{exchange}/cache/{timespan}/{archive_symbol}/{year}/{date}.csv
Events:   {output_dir}/archive/{table}/{exchange}/{archive_symbol}/{year}/{date}.csv
Flat:     {output_dir}/archive/{table}/{year}/{date}.csv  (telemetry, control)
```

**Examples:**

```bash
snapper archive --day 2024-03-15 --exchange polygon --symbol BTC-USD
snapper archive --from 2024-01-01 --to 2024-03-31 --dry-run
snapper archive --day 2024-03-15 --timeframe 1d
snapper archive --table ticks --day 2024-03-15 --exchange polygon
snapper archive --table trades --from 2024-01-01 --to 2024-03-31 --purge
snapper archive --table control --day 2024-03-15
snapper archive --table candles-audit --day 2024-03-15 --timeframe 1m
snapper archive --table candles-audit --day 2024-03-15 --closed-only --purge
snapper archive --table orders --day 2024-03-15
snapper archive --table symbols --day 2024-03-15 --closed-only --purge
snapper archive --table instruments --day 2024-03-15
```

**Notes:**

- `--symbol` resolves the current native_symbol to the stable archive_symbol
  via the Symbol anchor row.  After a symbol rename, the CLI argument and
  the output directory may differ.
- `--day` or `--from`/`--to` is required.
- Daily timespan (`1d`) produces monthly CSV files to match Polygon layout.
- `--purge` is supported for event tables, plus `candles-audit` and
  state tables when combined with `--closed-only` (which protects
  active rows); it is **not** supported for the candle cache export.
- Event archive CSV files include full temporal metadata (`public_id`,
  `timestamp`, `known_to`, `session_id`, `sequence_id`).
- Merge/dedup for events uses `(public_id, timestamp, known_to)` as key.
- Executions are partitioned by exchange/archive_symbol via order -> instrument.
- ``candles-audit`` exports all SCD2 versions (closed + active) grouped by
  ``open_at`` date.  ``--purge`` requires ``--closed-only`` to protect active rows.

### `restore`

Restore archived CSV data back into the database.  Reads CSV files
exported by ``snapper archive`` and inserts rows, deduplicating against
existing data by ``(public_id, timestamp, known_to)``.

```bash
snapper restore [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--table` | TEXT | (required) | Table name to restore into |
| `--file` | str | None | Single CSV file to restore |
| `--dir` | str | None | Directory to scan recursively for CSV files |
| `--source` | TEXT | `audit` | Restore mode (`audit` for full history) |

**Examples:**

```bash
snapper restore --table ticks --file data/archive/ticks/polygon/BTC-USD/2024/2024-01-01.csv
snapper restore --table settings --dir data/archive/settings/
snapper restore --table candles --dir data/archive/candles/polygon/BTC-USD/
```

**Notes:**

- At least one of `--file` or `--dir` is required.
- Rows already present in DB (matching `public_id + timestamp + known_to`) are skipped.
- Audit restore inserts all temporal columns exactly as exported.

## Encryption Management

### `settings-rotate-encryption`

Rotates encryption keys for settings in the database.

```bash
snapper settings-rotate-encryption [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `--new-password` | string | _(prompted, hidden, confirmed)_ | New master password — no default; the command prompts with hidden input and confirmation when the flag is omitted, so the value never lands in shell history. Inline `--new-password X` is still accepted for non-interactive automation but the same shell-history caveats apply. |
| `--old-password` | string | from env | Current master password (optional; reads from env/bootstrap when omitted). Avoid inline use — passing a secret on the command line leaks it to shell history. |
| `--dry-run` | bool | `false` | Show changes without applying |

**Example:**

```bash
# Simulate rotation — prompts hidden for the new master password
snapper settings-rotate-encryption --dry-run

# Actual rotation — same prompt flow, then re-encrypts every encrypted setting
snapper settings-rotate-encryption
```

The CLI prompts for `--new-password` with hidden input and confirmation when
the flag is omitted (preferred), so the master password never lands in shell
history, scrollback, or CI logs. After a successful rotation the command
prints only a reminder to update `MASTER_PASSWORD` in `.env` — it does NOT
echo the value you entered. Inline `--new-password X` is still accepted for
non-interactive automation but the same shell-history caveats apply.

## Example Workflows

### New Installation Initialization

```bash
# 1. Initialize database
snapper db-init

# 2. Create administrator (prompts for password — never put it on the cmdline)
snapper init-admin --username admin

# 3. Synchronize symbols
snapper update-kraken-symbols
snapper update-polygon-symbols

# 4. Start server
snapper server
```

### Full Trading System Startup

```bash
# Terminal 1: ZMQ Broker
snapper broker

# Terminal 2: Data feed
snapper feed --symbols "BTC-USD,ETH-USD"

# Terminal 3: Executor
snapper executor -e kraken

# Terminal 4: Trade runtime
snapper trade-zmq

# Terminal 5: Server with dashboard
SERVER_API_ONLY=true snapper server
```

### Historical Data Backfill

Backfilling Polygon candles is a two-step workflow: download to the CSV
cache, then load the cache into the database.

```bash
# Step 1 - download last 30 days for SPY to the CSV cache (no DB write)
snapper polygon-backfill-aggregates -s SPY --days 30 --timespan minute

# Step 1 - download daily data for all symbols to the CSV cache
snapper polygon-backfill-aggregates --all --timespan day --days 365

# Step 2 - load the downloaded CSV cache into the database (no API calls)
snapper polygon-load-csv --all --timespan day
```

## Notifications

### `notify`

Long-running iOS Push Foundation sidecar (ZMQ `alerts.*` → APNs HTTP/2).
Subscribes to the `alerts.` ZMQ prefix, fans out each received
`AlertEventData` to the target user's active devices via the
outbox-backed `NotifySidecar`, and retries server/throttled failures
on its own 30-second scheduler. Configuration is read from the
`apns_*` settings (provisioned through the seed profile).

```bash
snapper notify
```

Run under systemd / K8s with `Restart=always` — the sidecar exits
only on unhandled errors or SIGINT; the outbox drain on the next
start recovers any queued deliveries left behind.

## AI Integration

### `dev-mint-pat`

Mints a long-lived AI delegate JWT into `data/dev-pat.json`
(mode `0600`) for the local Snapper MCP bridge. See
[ai-integration.md](ai-integration.md) for the wider Claude Code /
Cursor / Windsurf wire-up.

```bash
snapper dev-mint-pat [OPTIONS]
```

**Options:**

| Option | Env var | Default | Description |
| ------ | ------- | ------- | ----------- |
| `--base-url URL` | `SNAPPER_DEV_BASE_URL` | `http://localhost:8000` | Base URL of the running backend |
| `--admin-username USER` | `SNAPPER_DEV_ADMIN_USERNAME` | `None` (resolved from seed profiles, `mcp` then `dev`) | Admin login |
| `--admin-password PASS` | `SNAPPER_DEV_ADMIN_PASSWORD` | `None` (resolved from seed profiles, `mcp` then `dev`) | Admin password |
| `--output PATH` | `SNAPPER_DEV_PAT_OUTPUT` | `data/dev-pat.json` | Output file path |
| `--label LABEL` | — | `Local Dev MCP` | Delegate label |

The script drives the production REST endpoints (`POST /api/auth/login` +
`POST /api/ai-delegates`) so the seeded delegate exercises the same
code path the AI Integration UI uses. Tokens never appear in
stderr — HTTP error bodies are passed through a JWT-shape redaction
helper before printing.

## Backtesting

### `backtest-run`

Run a backtest synchronously. Creates a DB record, executes the engine
in-process, and persists results (signals, trades, equity, metrics).

```bash
snapper backtest-run \
    --strategy MACDCrossover \
    --instrument BTC-USD \
    --exchange kraken \
    --start 2026-01-01 \
    --end 2026-06-01 \
    --timeframe 1h \
    --initial-cash 10000 \
    --params '{"fast": 12, "slow": 26, "signal_period": 9}'
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `--strategy` | str | required | Registered strategy class name |
| `--instrument` | str | required | Native instrument symbol |
| `--exchange` | str | required | Exchange name |
| `--start` | str | required | Start date (ISO format) |
| `--end` | str | required | End date (ISO format) |
| `--timeframe` | str | `1h` | Candle timeframe |
| `--initial-cash` | float | `10000` | Starting cash balance |
| `--wallet` | str | `cli` | Wallet public ID |
| `--params` | str | `{}` | Strategy params as JSON |
| `--execution-mode` | str | `direct_db` | Engine type (`direct_db` or `zmq_replay`) |
| `--fill-model` | str | `market` | Fill simulation model |
| `--slippage-bps` | float | `0.0` | Per-fill slippage in basis points |
| `--commission-bps` | float | `0.0` | Per-fill commission in basis points |

`--execution-mode` is persisted on the run, but the CLI computes the
pairing config hash with execution mode excluded and currently
instantiates `DirectDbEngine` directly. Use `POST /api/backtests` for
a run that actually executes through `zmq_replay`.

### `backtest-list`

List backtest runs with optional filters.

```bash
snapper backtest-list --status completed --limit 10
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `--strategy` | str | None | Filter by strategy name |
| `--status` | str | None | Filter by status (`pending`, `running`, `completed`, `failed`, `cancel_requested`, `cancelled`) |
| `--wallet` | str | None | Filter by wallet public ID |
| `--limit` | int | `20` | Maximum number of rows to return |

### `backtest-cancel`

Cancel a pending or running backtest run.

```bash
snapper backtest-cancel <run-public-id>
```

### `backtest-rerun`

Re-run a CLI-compatible backtest with the fields exposed by
`backtest-run`, including strategy params. The CLI rerun path does not
preserve `target_execution_exchange`; use the API/process runner for
cross-asset reruns.

```bash
snapper backtest-rerun <run-public-id>
```
