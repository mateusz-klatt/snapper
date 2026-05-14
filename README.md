# Snapper

Trading platform with market data collection, trade runtime, and backtester.
Supports Kraken, Zonda, Walutomat exchanges and Polygon.io data.

## Quick Steps

```bash
# Initialize database with seed data and build static assets
make migrate-dev run-static

# Start the server
make run-server
```

Open <http://localhost:8000/> and log in with the dev seed credentials —
the default values are defined in `proprietary/data/seed/dev.toml` (the
proprietary submodule). Override per-environment by editing the seed file
before running `make migrate-dev`, or rotate after first login via
`POST /api/auth/users/<username>/password`.

## Features

- **Market data collection** — WebSocket and REST API from Kraken, Zonda,
  Walutomat, Polygon.io
- **Symbol correlation** — Underlying asset model linking instruments across
    exchanges (e.g., SPY, ESM6-CME, SPYX-USD-PERP all map to S&P 500).
    YAML-driven pattern matching, front-month rollover, contract ladder API
- **Trade runtime** — Facts-canonical live and paper trading with canonical
    Order and Execution facts, rebuildable Position and Balance projections,
    dispatched via a durable outbox
- **Strategies** — Framework for creating strategies based on RSI, MACD,
  cointegration, and TA-Lib indicators
- **ZeroMQ messaging** — Pub/sub architecture for market data and signals
- **Provenance and audit** — Every payload item carries `session_id` and
  `sequence_id` for gap detection. Control table (always-on) and telemetry
  table (toggleable) record all commands and operational events
- **Web dashboard** — FastAPI + React with real-time WebSocket
- **CLI** — Full system management via Typer CLI
- **Backtesting** — Strategy testing on historical data

## Quick Start

### Requirements

- Python 3.14+
- Poetry
- Node.js 25+ and pnpm (for frontend)
- TA-Lib (C library)

### Installation

```bash
# Clone repository
git clone https://github.com/mateusz-klatt/snapper.git
cd snapper

# Install system dependencies (macOS)
brew install ta-lib

# Install Python dependencies
make setup

# Install frontend dependencies
make ui-setup

# Copy and configure environment variables
cp .env.example .env
```

### Configuration

Edit `.env` file:

```bash
# Database (SQLite dev, PostgreSQL prod)
DB_URL=sqlite+aiosqlite:///./data/snapper.db

# Settings encryption in database
MASTER_PASSWORD=your_master_password

# HTTP Server
SERVER_HOST=127.0.0.1
SERVER_PORT=8000
SERVER_API_ONLY=false
SERVER_PROXY_HEADERS=true
SERVER_FORWARDED_ALLOW_IPS=127.0.0.1

# Telemetry recording (pings, heartbeats, GET reads)
TELEMETRY_RECORDING_ENABLED=false

# ZeroMQ broker
ZMQ_BROKER_XSUB=tcp://127.0.0.1:7500
ZMQ_BROKER_XPUB=tcp://127.0.0.1:7501
```

### Running

```bash
# Initialize database and seed data
make migrate-dev

# Start server
snapper server
```

Dashboard available at `http://localhost:8000/`.

### Trade Runtime Dispatch

The trade runtime always uses durable, outbox-driven dispatch: commands
are persisted as `TradeCommand` rows and published by the outbox
dispatcher. On the accepted/fill paths, executor `VenueEvent`
persistence is fail-closed — a failed write raises before the
corresponding `orders.events.*` publish.

## System Overview

```mermaid
flowchart TB
    subgraph Presentation["Presentation Layer"]
        Dashboard["Dashboard React<br/>WebSocket + REST API Client"]
    end

    subgraph Server["Server"]
        FastAPI["FastAPI Server<br/>REST API + WebSocket + Static Files"]
        Bridge["ZMQ Bridge<br/>ZMQ ↔ WebSocket Translation"]
    end

    subgraph Messaging["ZeroMQ Messaging"]
        Broker["ZMQ Broker XPUB/XSUB<br/>Central Message Hub"]
    end

    subgraph Components["Components"]
        Feed["Feed Publisher"]
        Strategies["ZMQ Strategies"]
        Runtime["Trade Runtime<br/>Coordinator + Per-Symbol Engines + Trade/Balance Services"]
        Executor["Order Executor<br/>Venue Adapter"]
    end

    Dashboard --> FastAPI
    FastAPI --> Bridge
    Feed --> Broker
    Strategies --> Broker
    Broker --> Strategies
    Runtime --> Broker
    Broker --> Bridge
    Broker --> Runtime
    Executor --> Broker
    Broker --> Executor
```

## CLI Commands

### Server and Infrastructure

```bash
snapper server              # Start FastAPI server
snapper broker              # Start ZMQ broker
snapper trade-zmq           # Trade runtime / coordinator (pass --instance-id + --instance-count for N>=2, see docs/operations.md)
snapper executor            # Order executor
snapper feed                # Market data publisher
```

### Database

```bash
snapper db-init             # Initialize schema
snapper db-upgrade          # Alembic migrations
snapper db-downgrade        # Rollback migrations
```

### Users

```bash
snapper init-admin          # Create admin user
snapper list-users          # List users
snapper reset-password      # Reset password
```

### Local AI delegate PAT (for `@mateusz-klatt/snapper-mcp` bridge)

After `make dev-backend` is running and the seed has provisioned the `admin`
user (per `dev.toml`), mint a long-lived AI delegate PAT into a JSON file
the bridge consumes via `--config=PATH`:

```bash
snapper dev-mint-pat        # writes data/dev-pat.json (mode 0600)
```

Wire your `~/.claude.json` mcpServers entry once with the file path:

```json
"snapper-local": {
  "command": "node",
  "args": [
    "/path/to/snapper-mcp/dist/index.js",
    "--config=/path/to/snapper/data/dev-pat.json"
  ]
}
```

After `rm data/snapper.db && make dev-backend && snapper dev-mint-pat` the
bridge picks up the refreshed token automatically — no manual edits to
`~/.claude.json` per DB wipe. CLI flags + environment overrides:

```text
--base-url URL        SNAPPER_DEV_BASE_URL          (default http://localhost:8000)
--admin-username USER SNAPPER_DEV_ADMIN_USERNAME    (default admin)
--admin-password PASS SNAPPER_DEV_ADMIN_PASSWORD    (default matches dev.toml)
--output PATH         SNAPPER_DEV_PAT_OUTPUT        (default data/dev-pat.json)
--label LABEL                                         (default "Local Dev MCP")
```

The script drives production REST endpoints (`POST /api/auth/login` +
`POST /api/ai-delegates`) so the seeded delegate exercises the same code
path the AI Integration UI uses. Tokens never appear in stderr — HTTP
error bodies are passed through a JWT-shape redaction helper before
printing.

### Market Data

```bash
snapper update-kraken-symbols        # Sync Kraken symbols
snapper update-polygon-symbols       # Sync Polygon symbols
snapper update-underlyings           # Sync underlying asset mappings from YAML
snapper polygon-backfill-aggregates  # Backfill historical data
snapper archive --day 2024-01-15     # Export candle cache to CSV
snapper archive --from 2024-01-01 --to 2024-01-31 --exchange polygon
snapper archive --table ticks --day 2024-01-15 --exchange polygon
snapper archive --table candles-audit --day 2024-01-15 --closed-only --purge
```

## Strategies

Creating your own strategy:

```python
from snapper.strategies.base import BaseStrategy, StrategySignal, StrategyConfig
from snapper.strategies.decorators import register_strategy, create_strategy_process
from snapper.messaging.schemas.data import CandleData

@register_strategy("MyStrategy")
@create_strategy_process(
    process_name="my_strategy",
    default_config={
        "name": "my_strategy",
        "inputs": ["market.kraken.BTC-USD.candles.1h"],
        "outputs": ["BTC-USD"],
        "exchange": "paper",
        "params": {"threshold": 0.5},
    },
)
class MyStrategy(BaseStrategy):
    def __init__(self, config: StrategyConfig) -> None:
        super().__init__(config)
        self.threshold = self.params.get("threshold", 0.5)

    async def on_candle(self, instrument: str, candle: CandleData) -> StrategySignal | None:
        if some_condition:
            return StrategySignal(
                instrument=instrument,
                side="buy",
                strength=1.0,
                price=candle.close,
                reason="Condition met",
            )
        return None
```

## Development

### Quality Gates

```bash
make check-all   # Full quality gate (backend + frontend + tests + 100% coverage)
make fix-all     # Automatic formatting and linting fixes
```

### Individual Steps

```bash
make fmt         # Check formatting
make lint        # Linting (ruff)
make typecheck   # Type checking (mypy)
make test        # Unit tests
make cov         # Tests with coverage (100% required)
```

### Frontend

```bash
make ui-dev      # Vite development server
make ui-build    # Production build
make ui-lint     # ESLint
make ui-format   # Prettier
```

## Docker

```bash
make docker-build-dev    # Build dev image
make docker-build-prod   # Build production image
make docker-run          # Run container
make docker-stop         # Stop container
```

Or with docker compose:

```bash
docker compose up -d
```

## Documentation

Detailed documentation in [docs/](docs/) directory:

- [Architecture](docs/architecture.md) — System structure and layers
- [Configuration](docs/configuration.md) — Environment variables and settings
- [CLI](docs/cli.md) — Full command documentation
- [Strategies](docs/strategies.md) — Creating trading strategies
- [API](docs/api.md) — REST API and WebSocket
- [Messaging](docs/messaging.md) — ZeroMQ architecture
- [Development](docs/development.md) — Developer guidelines

## License

MIT License — see [LICENSE](LICENSE) file.
