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
| `--reload` | bool | from env | Auto-reload for development |

**Environment:**

Set `SERVER_API_ONLY=true` to skip process autostart while keeping
the ZMQ-WebSocket bridge alive (useful for multi-worker deployments or when
the engine runs as a separate process).

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

Starts the central trading coordinator.

```bash
snapper trade-zmq [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `--signal-topics` | string | `signals.` | Signal topics to subscribe |

**Example:**

```bash
snapper trade-zmq --signal-topics "signals.paper.BTC-USD.rsi_btc_1h,signals.kraken.BTC-USD.live"
```

### `executor`

Starts the order execution service for a selected exchange.

```bash
snapper executor [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `-e, --exchange` | string | `kraken` | Exchange (`kraken`, `zonda`, `walutomat`) |

**Examples:**

```bash
# Kraken executor
snapper executor -e kraken

# Zonda executor
snapper executor --exchange zonda
```

### `feed`

Starts the market data publisher via WebSocket.

```bash
snapper feed [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `--symbols` | string | `BTC-USD` | Comma-separated symbols |

**Example:**

```bash
snapper feed --symbols "BTC-USD,ETH-USD,SOL-USD"
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
| `--password` | string | `AdminSnapper2026!` | Password |

**Example:**

```bash
snapper init-admin --username admin --password SuperSecurePass123!
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
snapper reset-password john --new-password NewSecurePass123!
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

### `update-zonda-symbols`

Synchronizes symbol mappings from the Zonda API.

```bash
snapper update-zonda-symbols [OPTIONS]
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

Synchronizes symbol mappings from the Polygon.io API.

```bash
snapper update-polygon-symbols [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `-f, --force` | bool | `false` | Force update |
| `--insert-new` | bool | `false` | Insert new symbols |

**Note:** This operation may take 10-15 minutes (44k+ symbols).

## Market Snapshots

### `update-kraken-market-snapshot`

Updates Kraken market snapshots with current prices.

```bash
snapper update-kraken-market-snapshot
```

### `update-zonda-market-snapshot`

Updates Zonda market snapshots with current prices.

```bash
snapper update-zonda-market-snapshot
```

### `update-walutomat-market-snapshot`

Updates Walutomat market snapshots with current prices.

```bash
snapper update-walutomat-market-snapshot
```

## Data Backfill

### `polygon-backfill-aggregates`

Fetches historical aggregated data from the Polygon.io API.

```bash
snapper polygon-backfill-aggregates [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `-s, --symbol` | string[] | from settings | Symbols to backfill |
| `--all` | bool | `false` | All mapped symbols |
| `-m, --multiplier` | int | `1` | Timeframe multiplier |
| `-t, --timespan` | string | `minute` | Timespan (`minute`, `hour`, `day`) |
| `-d, --days` | int | from settings | Days back |
| `--resume/--no-resume` | bool | `true` | Resume from last timestamp |
| `--csv/--no-csv` | bool | `true` | Save to CSV.gz files |

**Examples:**

```bash
# Backfill for specific symbols
snapper polygon-backfill-aggregates -s AAPL -s MSFT --days 365

# Backfill all mapped symbols
snapper polygon-backfill-aggregates --all --timespan day

# Hourly backfill with resume
snapper polygon-backfill-aggregates -s SPY -t hour -m 1 --resume
```

### `polygon-backfill-grouped`

Fetches grouped daily data from the Polygon.io API.

```bash
snapper polygon-backfill-grouped [OPTIONS]
```

## Data Archive

### `archive`

Export data to CSV archive files.  Supports candle cache projection
and append-only event tables (ticks, trades, signals, executions,
telemetry, control).  Merges with existing files and deduplicates rows.

```bash
snapper archive [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--table` | TEXT | `candles` | Table to archive (`candles`, `candles-audit`, `ticks`, `trades`, `signals`, `executions`, `telemetry`, `control`) |
| `--exchange` | TEXT | None | Exchange filter (e.g. `polygon`, `kraken`) |
| `--symbol` | TEXT | None | Native symbol filter (e.g. `BTC-USD`), resolved to stable archive_symbol |
| `--timeframe` | TEXT | `1m` | Candle timeframe (`1m`, `5m`, `1h`, `1d`) — candles only |
| `--day` | DATE | None | Single day to archive (`YYYY-MM-DD`) |
| `--from` | DATE | None | Start of date range (`YYYY-MM-DD`) |
| `--to` | DATE | None | End of date range (`YYYY-MM-DD`) |
| `--dry-run` | FLAG | False | Report counts without writing files |
| `--purge` | FLAG | False | Delete exported rows from DB after writing (event tables and candles-audit with --closed-only) |
| `--closed-only` | FLAG | False | Only closed SCD2 versions (candles-audit only) |
| `--output-dir` | PATH | `data` | Base output directory |

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
```

**Notes:**

- `--symbol` resolves the current native_symbol to the stable archive_symbol
  via the Symbol anchor row.  After a symbol rename, the CLI argument and
  the output directory may differ.
- `--day` or `--from`/`--to` is required.
- Daily timespan (`1d`) produces monthly CSV files to match Polygon layout.
- `--purge` is only supported for event tables, not candle cache export.
- Event archive CSV files include full temporal metadata (`public_id`,
  `timestamp`, `known_to`, `session_id`, `sequence_id`).
- Merge/dedup for events uses `(public_id, timestamp, known_to)` as key.
- Executions are partitioned by exchange/archive_symbol via order -> instrument.
- ``candles-audit`` exports all SCD2 versions (closed + active) grouped by
  ``open_at`` date.  ``--purge`` requires ``--closed-only`` to protect active rows.

### One-time cache directory migration

Renames legacy Polygon cache directories from API ticker format
(`X_BTCUSD`) to archive_symbol format (`BTC-USD`).

```bash
python -m scripts.migrate_polygon_cache_dirs [--dry-run] [--cache-root PATH]
```

## Encryption Management

### `settings-rotate-encryption`

Rotates encryption keys for settings in the database.

```bash
snapper settings-rotate-encryption [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `--new-password` | string | (required) | New master password |
| `--old-password` | string | from env | Current master password (optional, reads from env/bootstrap) |
| `--dry-run` | bool | `false` | Show changes without applying |

**Example:**

```bash
# Simulate rotation
snapper settings-rotate-encryption --new-password NewSecurePass --dry-run

# Actual rotation
snapper settings-rotate-encryption --new-password NewSecurePass
```

## Example Workflows

### New Installation Initialization

```bash
# 1. Initialize database
snapper db-init

# 2. Create administrator
snapper init-admin --username admin --password SecretPassword123!

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

# Terminal 4: Coordinator
snapper trade-zmq

# Terminal 5: Server with dashboard
SERVER_API_ONLY=true snapper server
```

### Historical Data Backfill

```bash
# Backfill last 30 days for SPY
snapper polygon-backfill-aggregates -s SPY --days 30 --timespan minute

# Backfill daily data for all symbols
snapper polygon-backfill-aggregates --all --timespan day --days 365
```
