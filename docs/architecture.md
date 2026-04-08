# Architecture

Snapper is a trading platform built with a layered architecture
using asynchronous processing and ZeroMQ messaging.

The trading path is facts-canonical: `Order` and `Execution` are the
canonical business facts, while `Position`, `Balance`, and equity are
derived projections that can be rebuilt from facts plus checkpoints.

## System Layers

```mermaid
flowchart TB
    subgraph Presentation["Presentation Layer"]
        P1["Dashboard React + FastAPI REST/WebSocket"]
    end

    subgraph Application["Application Layer"]
        A1["Strategies, Trade Runtime, Trade Services, Process Manager"]
    end

    subgraph Infrastructure["Infrastructure Layer"]
        I1["ZMQ Messaging, Exchange Clients, Data Providers"]
    end

    subgraph Data["Data Layer"]
        D1["SQLAlchemy ORM, Alembic Migrations, Repository"]
    end

    Presentation --> Application
    Application --> Infrastructure
    Infrastructure --> Data
```

## Components

### Core (`src/snapper/core/`)

Fundamental types and aliases used throughout the application:

- `TradeSide` — Trade direction (`buy`, `sell`)
- `OrderType` — Order type (`market`, `limit`, `stop`, `stop_limit`)
- `OrderStatus` — Order status in lifecycle
- `ExecutionMode` — Execution mode (`live`, `paper`)
- `OrderExchange` — Order-capable exchanges (`paper`, `kraken`, `zonda`, `walutomat`)

### Config (`src/snapper/config/`)

Two-layer configuration:

1.  **Bootstrap Settings** (`bootstrap.py`)

    Settings from environment variables required before database connection:

    - Database URL
    - Master password for encryption
    - HTTP and ZMQ server endpoints

2.  **App Settings** (`app.py`)

    Settings from database (encrypted):

    - Exchange API keys
    - Trading parameters
    - Authentication configuration

### Data (`src/snapper/data/`)

Persistence layer with SQLAlchemy:

- **ORM Models** (`models.py`):

    - `Instrument` — Financial instruments (natural key: symbol_public_id + exchange).
        `Instrument.public_id` is the stable identity used by fact tables (orders,
        executions, positions, signals, candles). The versioned business attributes
        `symbol_public_id` and `exchange` are resolved via `ensure_instrument()`
    - `Candle` — OHLCV candles
    - `Trade` — Transactions
    - `Order` — Canonical order lifecycle facts (includes `mode`: `"live"` or `"paper"`)
    - `Execution` — Canonical confirmed fills
    - `Position` — Derived position projection (includes `mode`: `"live"` or `"paper"`)
    - `TradeCommand` — Durable trade intent written by the engine before execution
    - `VenueEvent` — Durable venue observations and acknowledgements persisted by executors
    - `TradeProjectionCheckpoint` — Materialized position/balance snapshot for fast recovery
    - `Signal` — Signal events
    - `User` — System users
    - `Setting` — Settings (encrypted)
    - `Symbol` — Symbol identity with versioned attributes (native_symbol, base, quote, asset_type) via SCD Type 2.
      Archive symbols for filesystem paths are derived from the anchor row (first version per public_id)
    - `SymbolAlias` — Exchange-specific symbol aliases (one row per native/exchange/channel)
    - `SymbolExchangeCapability` — Exchange-specific symbol capabilities
    - `ProcessRun` — Background process execution records
    - `InstrumentSpec` — Instrument trading specifications (tick_size, margins, expiry_at, instrument_kind)
    - `UnderlyingAsset` — Canonical asset identity (e.g., S&P 500, Gold) linking instruments across exchanges
    - `InstrumentUnderlyingMapping` — Temporal link from instrument to underlying (exact/derivative/proxy)
    - `MarketSnapshot` — Real-time market data snapshots
    - `UserLoginEvent` — Authentication event log
    - `Control` — Always-on audit for commands, auth events, subscribe/unsubscribe,
      REST mutations. Includes redacted payload, outcome, and client causation linkage
      (`client_session_id`, `client_public_id`)
    - `Telemetry` — Toggleable high-volume table for pings, heartbeats, pongs, and
      GET read requests. Recording gated by `TELEMETRY_RECORDING_ENABLED` setting

    All ORM models carry provenance via `TemporalMixin`:

    - `session_id` (str, required) — producer session identity
    - `sequence_id` (int, required) — per-table monotonic counter for gap detection

    `StrictDataSchema` (Pydantic base) requires these fields at construction.
    Producers must obtain values from a `SequenceTracker` before creating
    the event, ensuring every payload is complete and identifiable from birth.

    All ORM models use a dual-key identity pattern:

    - `id` (INTEGER, internal PK) — row/version identifier for SCD2 close
        operations: SELECT → lock → close by id → INSERT new version.
        Never exposed outside the repository layer.
    - `public_id` (UUID7 string) — logical entity identifier for domain
        joins and external references. Stable across SCD2 versions.
    - `timestamp` (DateTime, system time / known_from)
    - `known_to` (DateTime NOT NULL, default KNOWN_TO_MAX = 9999-12-31T23:59:59 UTC, active rows have known_to == KNOWN_TO_MAX; query pattern: `WHERE timestamp <= :t AND known_to > :t`)

    No hard foreign keys between temporal entities. All domain joins use
    `child.instrument_public_id == Instrument.public_id` (or
    `Execution.order_public_id == Order.public_id`) with temporal filters.
    This enables future archival of closed SCD2 rows without breaking
    referential integrity.

- **SQLAlchemyRepository** (`repository.py`) — Async CRUD for SQLite/PostgreSQL
- **DatabaseRepository** (`repository.py`) — Sync access for scripts, archiver, and background updaters
- **CandleCacheArchiver** (`archiver.py`) — Exports active candle data to polygon-compatible CSV cache files per exchange/archive_symbol
- **EventArchiver** (`archiver.py`) — Exports append-only event rows (Tick, Trade, Signal, Execution, Telemetry, Control) to per-day CSV archive files with full temporal metadata, merge/dedup, and optional purge
- **CandleAuditArchiver** (`archiver.py`) — Exports all candle SCD2 versions (closed + active) with full temporal metadata, grouped by open_at date
- **StateArchiver** (`archiver.py`) — Generic archiver for state-SCD2 tables (Order, Position, Instrument, Setting, Symbol, etc.) with closed_only filtering and Symbol anchor row protection
- **Archive symbol resolution** (`archive_symbols.py`) — Stable filesystem-safe symbol naming derived from Symbol anchor rows, with seniority-based collision handling

### Strategies (`src/snapper/strategies/`)

Trading strategies framework:

- **BaseStrategy** (`base.py`) — Base class for all strategies
- **StrategySignal** — Trading signal structure
- **StrategyConfig** — Strategy configuration

Built-in strategies:

- `RSIReversion` (`rsi.py`) — Mean reversion on RSI
- `MACDCrossover` (`macd.py`) — MACD crossover
- `CointegrationPairs` (`cointegration.py`) — Pairs trading

### Indicators (`src/snapper/indicators/`)

Technical indicators:

- `rsi` — Relative Strength Index (Python implementation)
- `macd` — Moving Average Convergence Divergence
- `ta_lib_adapter` — TA-Lib library adapter

### Messaging (`src/snapper/messaging/`)

ZeroMQ pub/sub architecture:

```mermaid
flowchart LR
    Publishers -->|connect| XSUB
    XSUB <-->|proxy| XPUB
    XPUB -->|connect| Subscribers
```

Components:

- **Broker** (`infrastructure/broker.py`) — XPUB/XSUB proxy
- **Publishers** (`publishers/`) — Market data publication
- **Executors** (`executors/`) — Per-wallet order execution on
  exchanges. One executor process per `(exchange, wallet)` pair is
  spawned at boot from active `wallet_credentials` rows; each
  instance filters incoming commands by `wallet_public_id` and loads
  its credentials via `CredentialResolver` during startup.
- **Schemas** (`schemas/`) — Pydantic message models
- **Topics** (`topics/`) — ZMQ topic definitions

Message types (in `messaging.schemas.data`):

- `TickData` — Price tick
- `CandleData` — OHLCV candle
- `SignalData` — Trading signal
- `TradeData` — Trade execution
- `OrderData` — Order status
- `ExecutionData` — Order execution
- `OrderRequestData` — Order request
- `HeartbeatData` — Component heartbeat

Every Data class carries `public_id: str` (UUID7), `type: Literal[...]`, and `timestamp: datetime`.

### Application (`src/snapper/application/`)

Business logic:

- **Engine** (`engine/`) — `TraderCoordinator` is the multi-symbol trade runtime coordinator; `TradingEngineService` is the per-instrument engine that turns signals into `TradeCommand` rows and order requests
- **Trade** (`trade/`) — Trade-domain services: `trade_service.py` (in-memory command and position read model), `balance_service.py` (cash/equity/exposure projection), `outbox.py` (outbox-driven publishing), `reconciler.py` (stale-command scan and reconciliation failure feedback)
- **Process Manager** (`process_manager/`) — Process management
- **Services** (`services/`) — Application services
- **Updaters** (`updaters/`) — Data updates (symbols, historical)
- **Risk** (`risk/`) — Risk management
- **Portfolio** (`portfolio/`) — Portfolio management

### Infrastructure (`src/snapper/infrastructure/`)

External integrations:

- **Exchanges** (`exchanges/`) — Exchange clients (Kraken, Zonda, Walutomat)
- **Market Data** (`market_data/`) — WebSocket feeds
- **Symbols** (`symbols/`) — Symbol mapping
- **Security** (`security/`) — Settings encryption
- **Logging** (`logging/`) — Logging configuration

### Server (`src/snapper/server/`)

FastAPI application:

- **App Factory** (`app.py`) — `create_app()` with lifespan management
- **REST API** — HTTP endpoints
- **WebSocket** — Real-time streaming via ZMQ bridge
- **Static Files** — Frontend dashboard
- **ClientProvenanceMiddleware** (`provenance_middleware.py`) — Extracts client
  provenance from mutation requests, runs per-session gap detection, records
  mutations to `control` table and GET reads to `telemetry` table

### API (`src/snapper/api/`)

Shared API schemas and WebSocket auth helpers (not route definitions):

- **Schemas** (`schemas/`) — Pydantic request/response models (health, process, settings).

    Schema class hierarchy:

    ```text
    BaseModel
    ├── PartialBody           — extra="ignore", strict=True (GapEnvelope, WsTokenPayload)
    ├── StrictBody            — extra="forbid", strict=True (request bodies, nested DTOs)
    │   └── StrictDataSchema  — + provenance (all event payloads, REST/WS/ZMQ)
    │       ├── PayloadRequest[T]
    │       ├── PayloadResponse[T]
    │       └── PayloadListResponse[T]
    ├── ExchangeResponse      — extra="allow", strict=True (exchange API parsing)
    ├── ExchangeRequest       — extra="forbid", strict=True (outgoing exchange requests)
    └── BaseSettings          — BootstrapSettingsLoader (env var config)
    ```

- **Auth** (`auth/`) — WebSocket token service and schemas

Route modules live closer to their domains: `server/app.py` (assembly and
data endpoints), `server/process_routes.py`, `server/strategy_routes.py`,
`config/settings_routes.py`, and `auth/routes.py`.

REST endpoints return the same Data schemas used by WebSocket messaging
(`messaging.schemas.data`): `OrderData`, `SignalData`, `ExecutionData`,
`PositionData`, `CandleData`. Old REST-specific schema modules were removed.

### Auth (`src/snapper/auth/`)

Authentication system:

- JWT tokens with access/refresh via HTTP-only cookies
- CSRF protection
- Role-based access (viewer, operator, admin)
- WebSocket authentication

### CLI (`src/snapper/cli/`)

Command-line interface (Typer):

- Server and infrastructure management
- Database operations
- User management
- Market data updates

## Data Flow

### Market Data and Signal Flow

```mermaid
flowchart TB
    Exchange["Exchange WebSocket / REST"] --> Publisher["Market Data Publisher"]
    Publisher -->|market.*| Broker["ZMQ Broker"]
    Broker -->|market.*| Strategy
    Strategy -->|signals.*| Broker
    Broker -->|signals.*| Runtime["Trade Runtime"]
    Broker -->|market.*| Bridge
    Bridge --> WebSocket["WebSocket<br/>Dashboard"]
    Runtime -->|orders.commands.*| Broker
    Broker -->|orders.commands.*| Executor
    Executor --> API["Exchange API / private WS"]
```

### Order Flow

```mermaid
flowchart TB
    Signal["Strategy Signal"] -->|ZMQ| Runtime["Trade Runtime"]
    Runtime --> Engine["TradingEngineService\n(per instrument / shard)"]
    Engine --> Command["TradeCommand\n(durable DB command log)"]
    Command --> Dispatch["Command dispatch\n(direct ZMQ or OutboxDispatcher)"]
    Dispatch -->|orders.commands.*| Executor["Order Executor"]
    Executor --> Exchange["Exchange API / private WS"]
    Exchange --> VenueEvent["VenueEvent\n(persisted by executor)"]
    VenueEvent --> Facts["Order + Execution\nfacts in DB"]
    Executor -->|orders.events.*| Runtime
    Runtime --> Trade["TradeService\nshadow read model + checkpoint state"]
    Trade --> Balance["BalanceService\nprojection"]
```

## Database

SQLite for development, PostgreSQL for production.

### Schema

```sql
-- Market data (joined to instruments via instrument_public_id)
instruments         -- Financial instruments (logical key: symbol_public_id + exchange)
candles             -- OHLCV data
ticks               -- Real-time price snapshots
trades              -- Transaction history
market_snapshots    -- Real-time market data (SCD2 per instrument, one active row each)

-- Trading (joined to instruments via instrument_public_id)
orders              -- Order history (mode: live/paper)
executions          -- Order executions (joined to orders via order_public_id)
positions           -- Portfolio positions (mode: live/paper)
signals             -- Strategy signals

-- Trade runtime (durable command path)
trade_commands              -- Durable trade intent (engine writes before execution)
venue_events                -- Durable venue observations and acknowledgements from executors
trade_projection_checkpoints -- Materialized position/balance snapshots

-- Symbol management
symbols             -- Symbol identity + versioned attributes (SCD2)
symbol_aliases      -- Exchange-specific symbol mappings
symbol_exchange_capabilities -- Exchange-specific capabilities

-- Instrument specifications
instrument_specs    -- Trading specifications (tick_size, lot_size, limits)

-- System
users               -- User accounts
settings            -- Settings (encrypted)
user_login_events   -- Authentication event log
process_runs        -- Background process execution records
control             -- Audit log (commands, auth events, mutations)
telemetry           -- High-volume operational events (toggleable)
```

### Migrations

Alembic manages migrations:

```bash
snapper db-upgrade    # Apply migrations
snapper db-downgrade  # Rollback
```

## Trade Runtime

The trade runtime supports two deployment modes controlled by the
`use_durable_commands` database setting (default: `false`):

-   **Dual-write** (default) — `TradingEngineService` writes `TradeCommand` rows and also
    publishes directly to ZMQ. Durable tables are populated in the background,
    and `direct_dispatched` prevents the outbox from replaying already sent
    commands.
-   **Durable** (outbox-driven) — `TradingEngineService` writes `TradeCommand` rows only.
    `OutboxDispatcher` publishes from the database, and executor `VenueEvent`
    writes become fail-closed on the accepted/fill paths.

The `TraderCoordinator` class acts as the trade runtime coordinator and integrates
`TradeService` (command lifecycle) and `BalanceService` (balance tracking) in both
modes. When `use_durable_commands=true`, it also starts the outbox dispatcher and
the reconciliation/circuit-breaker task. In dual-write mode, the runtime still
writes `TradeCommand` rows, consumes `orders.events.*` to shadow-update trade
projections, and persists checkpoints for recovery. Canonical `Order` and
`Execution` rows are persisted on the exchange/executor path. Restart the trade
runtime and executors after changing `use_durable_commands`.

## Bitemporal Model

All entity tables inherit `TemporalMixin` which provides `id`, `public_id`,
`timestamp` (bus-time / known_from), and `known_to` columns.

Key concepts:

- **SCD Type 2** — No in-place UPDATE or DELETE. Mutations use a close+insert
    pattern: the current row is closed (known_to set to now) and a new row is
    inserted with the updated values and known_to = KNOWN_TO_MAX.
- **KNOWN_TO_MAX** — Sentinel value (9999-12-31T23:59:59 UTC) marking the
    active version of a row.
- **Point-in-time queries** — REST endpoints accept an `as_of` query parameter
    to retrieve the state of data at a specific moment.
- **Helpers**:
    - `where_active(model, at)` — SQLAlchemy filter for temporal queries.
        `at: datetime` is required — no silent default-to-now
    - `where_active_now(model)` — convenience for operations that genuinely
        mean "current state" (auth login, heartbeat). Not a shortcut for
        skipping `as_of` threading
    - `close_and_insert()` / `close_and_insert_sync()` — Repository helpers
        that atomically close the current row and insert a new version

## Processes

The system manages processes through Process Manager:

- **Core** — Broker, Feed, Bridge (required — startup is aborted if any
  enabled long-running CORE process fails to start)
- **Strategy** — Trading strategies
- **Task** — One-time tasks
- **Backtest** — Backtesting processes

Processes can be:

- `long_running` — Run continuously (services)
- `one_shot` — Execute once (tasks)

### Deployment Modes

By default, the FastAPI lifespan starts all enabled processes (all-in-one
dev mode).  Set `SERVER_API_ONLY=true` to skip process autostart -- the
ZMQ-WebSocket bridge still starts, so the frontend receives live data from
a separately-running engine.  Useful for multi-worker uvicorn or when
broker/strategies/executors run on different hosts.

## Security

### Settings Encryption

Sensitive data (API keys) are encrypted in the database:

```mermaid
flowchart TB
    Input["Master Password + Salt"] --> KDF["Key Derivation PBKDF2"]
    KDF --> Encryption["Fernet (AES-128-CBC + HMAC-SHA256)"]
    Encryption --> DB["Encrypted Setting in DB"]
```

### Authentication

- JWT tokens via HTTP-only cookies (access: 15min, refresh: 7 days)
- CSRF tokens for mutating requests
- WebSocket tokens (one-time)
- Bcrypt password hashing
