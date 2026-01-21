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

**Examples:**

```bash
# Start with default settings
snapper server

# Start on all interfaces
snapper server --host 0.0.0.0 --port 8000

# Development mode with auto-reload
snapper server --reload
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
snapper trade-zmq --signal-topics "signals.rsi,signals.macd"
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
| `--symbols` | string | `BTC/USD` | Comma-separated symbols |

**Example:**

```bash
snapper feed --symbols "BTC/USD,ETH/USD,SOL/USD"
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
| `--new-salt` | string | (unchanged) | New salt |
| `--old-password` | string | from env | Current password |
| `--old-salt` | string | from env | Current salt |
| `--dry-run` | bool | `false` | Show changes without applying |

**Example:**

```bash
# Simulate rotation
snapper settings-rotate-encryption --new-password NewSecurePass --dry-run

# Actual rotation
snapper settings-rotate-encryption --new-password NewSecurePass --new-salt NewSalt123
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
snapper feed --symbols "BTC/USD,ETH/USD"

# Terminal 3: Executor
snapper executor -e kraken

# Terminal 4: Coordinator
snapper trade-zmq

# Terminal 5: Server with dashboard
snapper server
```

### Historical Data Backfill

```bash
# Backfill last 30 days for SPY
snapper polygon-backfill-aggregates -s SPY --days 30 --timespan minute

# Backfill daily data for all symbols
snapper polygon-backfill-aggregates --all --timespan day --days 365
```
