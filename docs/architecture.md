# Architecture

Snapper is a trading platform built with a layered architecture
using asynchronous processing and ZeroMQ messaging.

## System Layers

```mermaid
flowchart TB
    subgraph Presentation["Presentation Layer"]
        P1["Dashboard React + FastAPI REST/WebSocket"]
    end

    subgraph Application["Application Layer"]
        A1["Strategies, Trader Coordinator, Process Manager"]
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

    - `Instrument` — Financial instruments
    - `Candle` — OHLCV candles
    - `Trade` — Transactions
    - `Order` — Order history
    - `Execution` — Order executions
    - `Position` — Portfolio positions
    - `Signal` — Signal events
    - `User` — System users
    - `Setting` — Settings (encrypted)
    - `Symbol` — Symbol identity with versioned attributes (native_symbol, base, quote, asset_type) via SCD Type 2
    - `SymbolAlias` — Exchange-specific symbol aliases (one row per native/exchange/channel)
    - `SymbolExchangeCapability` — Exchange-specific symbol capabilities
    - `ProcessRun` — Background process execution records
    - `InstrumentSpec` — Instrument trading specifications
    - `MarketSnapshot` — Real-time market data snapshots
    - `UserLoginEvent` — Authentication event log

    All ORM models use a dual-key pattern:

    - `id` (INTEGER, internal PK, never exposed outside repository)
    - `public_id` (UUID7 string, external identifier)
    - `timestamp` (DateTime, system time / known_from)
    - `known_to` (DateTime NOT NULL, default KNOWN_TO_MAX = 9999-12-31T23:59:59 UTC, active rows have known_to == KNOWN_TO_MAX; query pattern: `WHERE timestamp <= :t AND known_to > :t`)

- **Repository** (`repository.py`) — Async CRUD operations

### Strategies (`src/snapper/strategies/`)

Trading strategies framework:

- **BaseStrategy** (`base.py`) — Base class for all strategies
- **StrategySignal** — Trading signal structure
- **StrategyConfig** — Strategy configuration

Built-in strategies:

- `RSIReversion` (`rsi.py`) — Mean reversion on RSI
- `MACDCrossover` (`macd.py`) — MACD crossover
- `Cointegration` (`cointegration.py`) — Pairs trading

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
- **Executors** (`executors/`) — Order execution on exchanges
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

- **Engine** (`engine/`) — Trader Coordinator
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

### API (`src/snapper/api/`)

API layer:

- **Schemas** (`schemas/`) — Pydantic request/response models (health, process, settings)
- **Auth** (`auth/`) — Authentication services

REST endpoints return the same Data schemas used by WebSocket messaging
(`messaging.schemas.data`): `OrderData`, `SignalData`, `ExecutionData`,
`PositionData`, `CandleData`. Old REST-specific schema modules were removed.

### Auth (`src/snapper/auth/`)

Authentication system:

- JWT tokens with access/refresh via HTTP-only cookies
- CSRF protection
- Role-based access (admin, user, viewer)
- WebSocket authentication

### CLI (`src/snapper/cli/`)

Command-line interface (Typer):

- Server and infrastructure management
- Database operations
- User management
- Market data updates

## Data Flow

### Market Data Flow

```mermaid
flowchart TB
    Exchange["Exchange WebSocket"] --> Publisher["Market Data Publisher"]
    Publisher -->|ZMQ XSUB| Broker["ZMQ Broker"]
    Broker -->|ZMQ XPUB| Strategy
    Broker -->|ZMQ XPUB| Bridge
    Broker -->|ZMQ XPUB| Logger
    Strategy --> Signal
    Bridge --> WebSocket["WebSocket<br/>Dashboard"]
    Signal --> Executor
    Executor --> API["Exchange API"]
```

### Order Flow

```mermaid
flowchart TB
    Signal["Strategy Signal"] -->|ZMQ| Coordinator["Trader Coordinator"]
    Coordinator -->|ZMQ| Executor["Order Executor"]
    Executor --> API["Exchange API"]
    API --> Confirmation["Execution Confirmation"]
    Confirmation -->|ZMQ| Position["Position Update"]
```

## Database

SQLite by default, with PostgreSQL and Azure SQL support.

### Schema

```sql
-- Main tables
instruments     -- Financial instruments
candles         -- OHLCV data
trades          -- Transaction history
orders          -- Orders
executions      -- Executions
positions       -- Positions

-- Strategies
signals         -- Signals

-- System
users           -- Users
settings        -- Settings (encrypted)
symbols         -- Symbol identity + versioned attributes (SCD2)
symbol_aliases  -- Symbol aliases (exchange-specific symbol mappings)
user_login_events -- Authentication event log
process_runs    -- Process history
```

### Migrations

Alembic manages migrations:

```bash
snapper db-upgrade    # Apply migrations
snapper db-downgrade  # Rollback
```

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
    - `where_active(model, at)` — SQLAlchemy filter for temporal queries
    - `close_and_insert()` / `close_and_insert_sync()` — Repository helpers
        that atomically close the current row and insert a new version

## Processes

The system manages processes through Process Manager:

- **Core** — Broker, Feed, Bridge (required)
- **Strategy** — Trading strategies
- **Task** — One-time tasks
- **Backtest** — Backtesting processes

Processes can be:

- `long_running` — Run continuously (services)
- `one_shot` — Execute once (tasks)

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
