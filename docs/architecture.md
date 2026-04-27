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

### Venue reconciliation (executors)

Executors can periodically poll exchange REST APIs to reconcile local
pending-order state against the exchange's authoritative view. This
detects two failure modes that WebSocket streams alone miss: **fill
gaps** (exchange has more filled quantity than the executor observed)
and **disappeared orders** (pending orders absent from the open-orders
snapshot).

Always-on — runs alongside the executor's order handler + heartbeat
tasks (no feature flag; the defensive polling is cheap enough that
gating it provided no value).

**`_reconcile_fill_gap`** (`messaging/executors/base.py:1139-1211`)
emits a corrective `ExecutionUpdate` covering the observed cum-qty
delta, stamping the synthetic execution with
`exec_id=recon-<exchange_oid>-<monotonic_ns>` so downstream de-dup
stays correct.

**`_reconcile_disappeared_order`** (`messaging/executors/base.py:1074-1137`)
calls `get_order()` on orders missing from the open-orders snapshot to
determine their actual terminal status (CLOSED / CANCELED / EXPIRED),
emits any residual fill gap first, then publishes the terminal
`ExecutionUpdate`.

#### Known limitation — market orders without price

If a market order's exchange snapshot reports `price=None`, the
fill-gap path logs an ERROR (`"no price on market order, skipping
corrective fill"`) and skips the corrective emission. The executor
does not subscribe to ticks and cannot approximate the fill price
without degrading VWAP accuracy silently. Recovery is
lifecycle-dependent: if the order stays open and a later snapshot
populates `price`, the next reconciliation iteration emits the
corrective fill; if the order reaches a terminal state first, the
skipped gap persists until reconciled manually against the exchange's
fill history. Operators seeing this ERROR log should not patch Snapper
to approximate; the skip is an intentional trade-off (predictable
behaviour + observable gap) over silent approximation drift.

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

The trade runtime always uses the durable (outbox-driven) dispatch
path. `TradingEngineService` writes `TradeCommand` rows to the
database; `OutboxDispatcher` polls the table and publishes undispatched
commands to ZMQ. Executor `VenueEvent` writes are fail-closed — a
failed persist raises before the executor acknowledges the venue event.

The `TraderCoordinator` class acts as the trade runtime coordinator
and integrates `TradeService` (command lifecycle) and `BalanceService`
(balance tracking). It spawns the outbox dispatcher (when a
`SQLAlchemyRepository` is wired) plus per-exchange reconciliation
loops that feed the circuit breaker. Canonical `Order` and `Execution`
rows are persisted on the exchange/executor path.

Since Phase 4 (2026-04-18), multiple `TraderCoordinator` processes can
run against the same DB + broker with deterministic SHA-256 shard
partitioning via `snapper.core.partitioning.ShardOwnership`. Each
coordinator owns `~1/N` of the `shard_key` set; signals, venue events,
recovery, outbox dispatch, and reconciliation all filter by ownership
so no two instances process the same shard. Default `--instance-count
1` is byte-identical to pre-Phase-4 behavior. See
[`docs/operations.md`](operations.md) for the systemd template recipe
and scale-up / scale-down / crash-recovery procedures.

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

### Batch atomicity guarantees

True SCD2 batch writes (`upsert_candles` at `repository.py:2552-2599`
+ `upsert_market_snapshots` at `:3019-3061`) and single-row close+insert
methods (`revise_*`, `close_and_insert` at `:292-342`) commit as
all-or-nothing transactions. The pattern is:

```python
async with self.session() as s:          # opens a transaction
    for row in rows:
        # SELECT ... FOR UPDATE the active version
        # UPDATE close (set known_to = bus_time)
        # s.add(new row)
    await s.commit()                     # single commit
```

If any row in the loop raises (IntegrityError, serialization failure,
connection drop, `await` cancellation), the transaction aborts and
**no** close/insert pair from this batch lands in the DB. The batch
must be retried in full; there is no partial-progress mode.

Append-only batch writes do NOT use the SCD2 close-insert pattern and
split into two shapes:

-   `upsert_trades` (`repository.py:2601-2605`) delegates to the
    dialect-aware `_upsert_batch` (`:2505-2550`). On native dialects
    (SQLite / Postgres) it uses `INSERT ... ON CONFLICT DO NOTHING` in a
    single `s.execute` — atomic per-batch, and duplicates are silently
    skipped. On the fallback row-by-row path it wraps each row in a
    `begin_nested()` SAVEPOINT; successful rows stay in the outer
    transaction and are committed at the final `await s.commit()`,
    while rows that hit `IntegrityError` are skipped. This trades
    all-or-nothing for conflict tolerance.
-   `upsert_ticks` (`repository.py:2607-2617`) is plain
    `session.add_all(rows)` + single `await s.commit()`. Atomic
    per-batch on every dialect — it is not routed through
    `_upsert_batch` and has no duplicate-skipping behaviour.

Serialization on the same natural key: on **PostgreSQL**, row-level
`SELECT ... FOR UPDATE` serializes concurrent transactions via MVCC
row locks — a second session waiting on a locked active row observes
the first session's close+insert only after the first commits; both
batches complete without error and produce a clean SCD2 chain.

On **SQLite** (aiosqlite), `SELECT ... FOR UPDATE` is a no-op and the
default `BEGIN DEFERRED` isolation does not acquire a write lock at
SELECT time — two concurrent tasks can both read the same active
row before either writes. The first writer to COMMIT wins; the second
writer's INSERT then collides on the `public_id` UNIQUE index and
raises `sqlalchemy.exc.IntegrityError`. The final DB state is still a
valid SCD2 chain (exactly one active row, carried-forward `public_id`
preserved, no orphan inserts) — one writer is simply reported the
conflict via exception rather than implicitly serialized. Callers on
SQLite that race on the same natural key must either retry the failed
batch or rely on the single-publisher-per-exchange invariant to avoid
the contention entirely.

**At-scale implication:** under heavy multi-publisher contention on
the same natural key, batches serialize strictly. If a long-running
batch holds the lock for many milliseconds per row, throughput falls
linearly in contention. The current single-publisher-per-exchange
shape makes this a non-issue; revisit when Trade Runtime Phase 4
(multi-instance) lands.

**Non-atomic fallback (`_upsert_batch` row-by-row path):** the
dialect-agnostic fallback at `repository.py:2540-2550` wraps each row
in a `begin_nested()` SAVEPOINT and continues on `IntegrityError`.
Successful rows are NOT committed immediately — they are held in the
outer transaction and finalised only by the final `await s.commit()`.
This trades all-or-nothing for conflict tolerance — the caller sees
`inserted = N_successful`, not all-or-nothing. Used only for
append-only paths where a duplicate key is a genuine no-op. Never
used for SCD2 close-and-insert.

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
a separately-running engine. Useful when broker/strategies/executors run
on different hosts.

**Multi-instance deployment (Plan D Phase 2 #8 — AI-review fanout dedup):**
multi-worker uvicorn is now a supported production topology. The
shared ZMQ XPUB-XSUB broker delivers every internal bus event to
every FastAPI worker's ``AiReviewService.start_bus_listener``;
each subscribed topic has its own dedup story so connected
clients see exactly one external WS frame per source event
regardless of worker count:

- ``bus.delegate_offline`` is idempotent at the DB layer — the
  handler runs a per-row ``atomic_dispatch_fanout_with_audit`` CAS
  UPDATE, so only one worker wins per affected review row even when
  every worker receives the bus event.
- ``bus.ai_review_decision`` is process-local — the handler resolves
  an ``asyncio.Future`` from the per-worker registry populated by
  the worker that ran ``create_review`` for the strategy await loop.
  Other workers stash the event in a bounded pending-resolution
  cache (1024-entry hard cap, 30s TTL); cache memory across the
  cluster is bounded by ``N × 1024`` entries even at peak.
- ``bus.caps_violation_after_ai_approve`` is gated by deterministic
  partitioning. ``handle_caps_violation_bus_message`` short-circuits
  on ``ShardOwnership.owns(msg.review_public_id)``; exactly one
  worker (the SHA-256 owner of the review row) re-publishes the
  external ``ai_reviews.*.caps_violation`` frame. Other workers
  observe the bus event, log a debug skip, and return False.
  ``instance_count == 1`` (default deployment) makes
  ``ShardOwnership.owns`` return True unconditionally so behaviour
  is byte-identical to single-worker.

The same partitioning primitive backs the Trade Runtime Phase 4
shard ownership; AI-review reuses it via the
``set_shard_ownership(ShardOwnership(coordinator_instance_id,
coordinator_instance_count))`` injection seam wired by the FastAPI
lifespan after the publisher seam and before the bus listener
starts.

**Operational scaling:** raise
``SNAPPER_COORDINATOR_INSTANCE_COUNT`` to N and start N FastAPI
workers each with a distinct ``SNAPPER_COORDINATOR_INSTANCE_ID`` in
``[0, N)`` (or use the ``--instance-id`` / ``--instance-count`` CLI
flags). Each worker subscribes to every bus topic; only the SHA-256
owner runs the caps-violation external fanout.

## Execution Plans

The Execution Plans framework provides a unified control plane above the
existing TradeCommand spine. All manual and algorithmic trading actions
become `ExecutionPlan` instances evaluated by pluggable `PlanEvaluator`
classes running inside `PlanExecutorService`.

**Tables:** `execution_plans` (plan state + lifecycle), `execution_plan_checkpoints`
(high-churn evaluator state), `execution_plan_decisions` (tiered decision log),
`instrument_order_capabilities` (capability matrix), `venue_fee_schedules` (fee tiers),
`position_cycles` (one open→close lifetime per shard, brackets attach by `position_cycle_public_id`).

**Plan types:** `manual_once` (Phase 1 + Phase 1.5 hardening, shipped),
`bracket` (Phase 2 step 2, shipped 2026-04-12 — attaches to `position_cycles` row from step 1),
`trailing_stop` (Phase 3, shipped 2026-04-13 — stateful ratcheting stop with
checkpoint persistence, separate `/api/trailing-stops` route module),
`peg` (Phase 4), `scheduler` (Phase 5).

**Manual order create flow:** `POST /api/orders` creates a `manual_once`
plan (pending), stamps `child_client_order_id`, `native_instrument`, and
`venue_order_type` into `plan.params`, then inserts a `create`
`TradeCommand` for the outbox dispatcher and transitions the plan to
`active`. The `OutboxDispatcher` inside `TraderCoordinator` publishes the
command as `OrderRequestData` on the `orders.commands.{ex}.{instr}.submit`
topic.

**Fill propagation:** `PlanExecutorService` subscribes to
`orders.events.` and `market.` on the broker XPUB and consumes every
fill. Cumulative `ExecutionData.size` is compared against
`plan.filled_quantity` with a small epsilon so duplicate or stale frames
are dropped. Partial fills update `filled_quantity` and keep the plan
`active`; full fills transition the plan to `completed` with
`completed_at` stamped via SCD2.

**Cancel flow:** `POST /api/orders/{plan_public_id}/cancel` (plan id path)
or `POST /api/orders/by-client-order-id/{client_order_id}/cancel` (UI
convenience route) resolves the plan, verifies wallet scope, transitions
the plan to `cancel_requested`, hydrates `exchange_order_id` from the
active `orders` row, and inserts a `cancel` `TradeCommand`. On insert
failure the plan is marked `failed` and the route returns HTTP 500;
`PlanExecutorService._recover_plans` then re-emits the cancel on the
next service restart (deduplicated via `has_pending_cancel_command`).
The outbox dispatcher branches on `command_type` and re-hydrates
`exchange_order_id` one more time at publish time so a late venue ACK
is picked up, then publishes `OrderCancelData` on the
`orders.commands.{ex}.{instr}.cancel` topic.

**Listen loop resilience:** per-message exceptions (parse failures,
handler errors) are caught inside the loop so a single bad frame
cannot silently stop the service. Transient `recv_multipart` errors
retry with a 100 ms backoff. `asyncio.CancelledError` still unwinds
the loop for graceful shutdown. After `_setup_subscriber()` the service
sleeps 500 ms for ZMQ XPUB/XSUB slow-joiner stabilization before
recovery to avoid losing terminal events triggered by stranded-cancel
re-emits.

**Position cycles (Phase 2 step 1, shipped 2026-04-11):** the
`position_cycles` table represents one flat→non-flat→flat lifetime per
shard via the standard `TemporalMixin` SCD2 envelope. A partial unique
index `uq_pc_shard_open_active` enforces at most one open cycle per
`shard_key` at any instant. Five repository methods cover the lifecycle:
`insert_position_cycle`, `close_position_cycle` (SCD2 close-and-insert),
`get_open_position_cycle` (lookup by `shard_key` because paper shards
embed `strategy_tag`), `flip_position_cycle` (TOCTOU-hardened atomic
close+open with `shard_key` match assertion), and
`update_position_cycle_max_qty` (monotonic — silent no-op when the new
peak does not strictly exceed the existing one). The repository methods
are called from the trader (not from `TradeService`, which stays sync
and pure); `ShardState` carries trader-owned cache fields
`active_cycle_public_id` and `active_cycle_max_qty`.

The trader's `_sync_fill_to_trade_service` samples `old_qty` before
`apply_venue_event` and `new_qty` after, then delegates to
`_sync_position_cycle_on_fill`. That helper classifies the transition
via `TradeService._detect_cycle_transition` (returns
`Literal["open", "close", "flip", "scale_up"] | None` with a 1e-12
epsilon to match the existing zero-snap) and issues the matching
repository call. Four guards apply: degraded-identity fail-closed when
`engine.wallet_public_id` is falsy, idempotency on open via a
`get_open_position_cycle` pre-check, async `instrument_public_id`
resolution via `get_instrument_public_id_by_symbol`, and a symmetric DB
fallback on close/flip/scale_up paths so a degraded restart cannot
reuse a stale cycle row from a prior life of the shard. When a flip's
instrument resolution fails, the trader degrades to close-only — the
old cycle is closed cleanly, the new leg is left uncovered with an
audit-gap warning, and the cache is cleared so subsequent fail-soft
paths handle themselves correctly.

`_recover_engine_state` runs `_reconcile_position_cycles` as a fourth
recovery phase, after engine state is rebuilt and before the trading
loop starts. It iterates `self.engines` (engines, not `TradeService`,
because full replay rebuilds `engine.position_qty` but not the
projection) and handles four cases: recovered flat with a stale open
row → close the row; recovered non-flat with a matching-direction open
row → hydrate the cache and bump `max_qty` if downtime scaled-up beyond
the stored peak; recovered non-flat with an opposite-direction row →
atomic flip via `flip_position_cycle` (or degrade to close-only on
unresolved instrument); recovered non-flat with no row → bootstrap a
synthetic cycle. Brackets (shipped Phase 2 step 2, 2026-04-12) attach to
`position_cycle_public_id`, not to an order, so a flat reopen does not
inherit stale stop levels. Orphan cycles (open rows without a matching
engine) can be detected and closed via admin endpoints at
`/api/position-cycles/` (shipped 2026-04-13).

**`max_qty` is per-cycle, not lifetime.** Each `position_cycles` row
tracks the peak absolute quantity reached within its own
`opened_at → closed_at` window. A new cycle on the same shard starts
a fresh accumulator at `abs(opening_qty)`; it does not inherit or
max against prior cycles' peaks. This matches the bracket-evaluator
contract: a bracket attaching to cycle C1 is sized against C1's
peak, not against all-time peak. A future UI surfacing `max_qty`
must scope to the individual cycle row; summing or max-ing across
cycles would conflate independent trading episodes.

## Security

### Settings Encryption

Sensitive data (API keys) are encrypted in the database:

```mermaid
flowchart TB
    Input["Master Password + Salt"] --> KDF["Key Derivation PBKDF2"]
    KDF --> Encryption["Fernet (AES-128-CBC + HMAC-SHA256)"]
    Encryption --> DB["Encrypted Setting in DB"]
```

### Backtesting

The backtesting subsystem runs strategy simulations against historical candle
data. Phase 1 uses DirectDbEngine (synchronous candle reads from DB).

**Architecture:**

- **BacktestConfig** (Pydantic): Strategy, instruments, date range, fill model
- **DirectDbEngine**: Candle-driven simulation loop with time-batched processing
- **ResultCollector**: In-memory artifact buffer (signals, trades, equity)
- **BacktestMetrics**: Pure-math metrics computation (Sharpe, Sortino, CAGR, etc.)
- **BacktestRunnerProcess**: ONE_SHOT RegisterableProcess orchestrating lifecycle
- **BacktestRepository**: Separate repo sharing session_factory, SCD2 status transitions

**Lifecycle:** pending -> running -> completed | failed | cancelled

**Boot reconciliation:** Orphaned runs (pending/running/cancel_requested) are
swept to failed at server startup via `reconcile_stale_runs()`. This runs
only on the normal startup path (skipped in `SERVER_API_ONLY` mode).

**Storage:** 6 bitemporal tables (BacktestRun, BacktestEvent, BacktestSignal,
BacktestTrade, BacktestEquityPoint, BacktestResult) all with TemporalMixin.

### Authentication

- JWT tokens via HTTP-only cookies (access: 15min, refresh: 7 days)
- CSRF tokens for mutating requests
- WebSocket tokens (one-time)
- Bcrypt password hashing

## Kraken FCM market data (TradFi)

Kraken's FCM platform hosts index-futures, energy, metals, and yield
contracts (collective name: "TradFi" in this project). Unlike crypto
spot + perpetuals, FCM has **no public order API** — Snapper treats
these instruments as observation-only.

### Data sources

- REST instrument metadata:
  `iapi.kraken.com/api/internal/markets/all/futures-contracts`.
  Reverse-engineered from `pro.kraken.com` (requires
  `Origin: https://pro.kraken.com` + `Referer: https://pro.kraken.com/`
  headers). Powers the `kraken_equities_symbol_updater` process.
- REST historical candles:
  `iapi.kraken.com/api/internal/markets/{ws_symbol}/ticker/history`.
  Same Origin/Referer requirement. Drives
  `KrakenEquitiesExchangeClient.get_ohlcv()` and the
  `kraken_equities_aggregates_backfill` process. Intervals
  `{1, 5, 15, 30, 60, 1440}` minutes accepted.
- WebSocket ticks + trades: `wss://ws-equities.kraken.com` (Kraken
  Spot WS v2 protocol with `asset_class: futures_contract`). Feed is
  delayed ~10 minutes per FCM policy. The outer envelope's
  `delayed: true` flag propagates onto every
  `TickData.is_delayed=True` so ZMQ subscribers can gate accordingly.

### Capability model

Each TradFi symbol is seeded with a
`SymbolExchangeCapability(exchange="kraken_equities", can_trade=False,
can_market_data=True)` row. Every order-entry REST route
(`/api/orders`, `/api/execution-plans`, `/api/trailing-stops`) calls
`snapper.server._capability_guard.require_tradable` before emitting a
TradeCommand — market-data-only submits fail with HTTP 422
`error_code=instrument_market_data_only`. The AST meta-test at
`tests/meta/test_order_entry_capability_guard.py` enforces that every
enumerated submit handler imports and calls the guard.

### Frontend UX

`GET /api/exchanges/{exchange}/instruments/detail` exposes
capability-aware rows (`InstrumentDetailData` — symbol +
`can_trade` + `can_market_data` + `instrument_kind` + `expiry_at`).
`MarketData.tsx` renders a "Market-data only" badge next to the page
title when the currently-selected instrument is observation-only and
marks every `can_trade=false` row in the instrument dropdown with
the same pill. `NewOrderModal.tsx` consumes the same endpoint, shows
an amber inline notice, suffixes option labels with `— market-data
only`, and disables the submit button. Submit failures branch on
`APIError.details.error_code` via `errorMessages.ts`.

### Cross-asset strategy pattern (informational)

Strategies may subscribe TradFi candles via the normal
`market.kraken_equities.{symbol}.candles.{timeframe}` topic and emit
signals whose `instrument` targets a `can_trade=True` execution
instrument (crypto spot, xStocks). See
`src/snapper/strategies/examples/tradfi_observe_crypto_execute.py`
for an illustrative EMA-crossover implementation + activation
instructions in the module docstring. Cross-asset execution at the
backtest-engine level is out of scope for Phase A — see
`plan_tradfi_market_data_p3.md` §5 item 18 for the deferred follow-up
(engine changes required: `batch_processor` needs to use
`signal.instrument`/`signal.exchange` for target attribution, plus
multi-feed `domain_time` alignment).

### Operational runbook

See `docs/operations.md` "Kraken Equities (TradFi) market data" for
enable-the-feed steps (SQL DB `Setting` toggles), backfill invocation,
quarterly rotation cadence, and troubleshooting (iapi reachability
checks, WS disconnect loops).
