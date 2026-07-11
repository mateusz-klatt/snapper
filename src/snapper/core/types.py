"""Core type aliases for the Snapper trading platform.

This module defines fundamental type aliases used throughout the application
for type-safe trading operations. All types are Literal-based for strict
compile-time and runtime validation.

Type Aliases:
    TradeSide: Direction of a trade (buy/sell).
    OrderType: Type of order to place.
    OrderStatus: Current state of an order in its lifecycle.
    FillStatus: Result of an order fill attempt.
    ExecutionMode: Trading mode (live/paper).
    OrderExchange: Order-capable exchanges.
    MarketSubscribeExchange: Live market feed exchanges.
    MarketDataExchange: Exchanges that produce market data (live or historical).
    AllExchange: All known exchange identifiers.
    HealthStatus: Component health state.
    ComponentStatus: Infrastructure component status.
    ProcessMode: Process execution mode (thread/process).
    StartProcessStatus: Start operation outcome.
    StopProcessStatus: Stop operation outcome.
    ProcessLifecycleType: Process duration type.
    ProcessRoleType: Process role in the system.
    ProcessRunStatusType: Current process execution state.
    IndicatorBackend: Technical indicator computation backend.
    UpsertResult: Outcome of an upsert operation.
    AssetType: Financial asset class category.
    MarketDataType: Market data type for ZMQ topic routing.
    SubscriptionAction: WebSocket subscription action.
    SubscriptionStatus: WebSocket subscription result status.
    SpawnerProcessStatus: Subprocess-level process status.
    AliasChannel: Symbol alias channel type.
    OrderCommand: Order command types for ZMQ topics.
    OrderEvent: Order event types for ZMQ topics.
"""

from enum import StrEnum
from typing import Literal


class ExchangeEnum(StrEnum):
    """All known exchange identifiers across all domains."""

    PAPER = "paper"
    KRAKEN = "kraken"
    KRAKEN_FUTURES = "kraken_futures"
    KRAKEN_EQUITIES = "kraken_equities"
    WALUTOMAT = "walutomat"
    POLYGON = "polygon"


class AssetTypeEnum(StrEnum):
    """Financial asset class category for symbol catalog entries."""

    CRYPTO = "crypto"
    FOREX = "forex"
    EQUITY = "equity"
    INDEX = "index"
    COMMODITY = "commodity"
    YIELD = "yield"


class ExecutionModeEnum(StrEnum):
    """Trading mode: live for real money, paper for simulation."""

    LIVE = "live"
    PAPER = "paper"


class MarketDataTypeEnum(StrEnum):
    """Market data type for ZMQ topic routing."""

    TICKS = "ticks"
    TRADES = "trades"
    CANDLES = "candles"


class RelationshipTypeEnum(StrEnum):
    """Relationship between an instrument and its underlying asset."""

    EXACT = "exact"
    DERIVATIVE = "derivative"
    PROXY = "proxy"


class InstrumentKindEnum(StrEnum):
    """Instrument product type for front-month rollover and continuous contracts."""

    SPOT = "spot"
    PERPETUAL = "perpetual"
    FUTURE = "future"
    ETF = "etf"
    OPTION = "option"


class AliasChannelEnum(StrEnum):
    """Symbol alias channel type: ws for WebSocket, rest for REST API, ccxt for CCXT library."""

    WS = "ws"
    REST = "rest"
    CCXT = "ccxt"


class TradeSideEnum(StrEnum):
    """Direction of a trade: buy for long entry, sell for short/exit."""

    BUY = "buy"
    SELL = "sell"


class OrderCommandEnum(StrEnum):
    """Order command types for orders.commands.* ZMQ topics."""

    SUBMIT = "submit"
    CANCEL = "cancel"
    REPLACE = "replace"


class HealthStatusEnum(StrEnum):
    """Component health state for monitoring and alerting."""

    HEALTHY = "healthy"
    WARNING = "warning"
    ERROR = "error"


class ProcessModeEnum(StrEnum):
    """Process execution mode: thread for in-process, process for subprocess."""

    THREAD = "thread"
    PROCESS = "process"


class ProcessAutostartProfileEnum(StrEnum):
    """Container autostart profile selecting which processes a node starts.

    Splits the market-data ingest tier from the API/trading tier so each
    runs in its own container without sharing one asyncio event loop:

    - ``ALL``: start every enabled process (single-container / dev default).
    - ``API``: start everything EXCEPT market-data publishers — the
      backend container keeps the broker, executors, strategies, and API
      while the publishers move out so the FastAPI loop stops sharing CPU
      with them under NYSE burst.
    - ``FEED``: start ONLY market-data publishers — the dedicated feed
      container. Used by the publishers-only feed CLI entrypoint, which
      runs each publisher as its own OS process; the FastAPI server is
      never launched with this profile.

    A market-data publisher is any registered process whose tags carry
    both ``"market-data"`` and ``"publisher"``.

    ``STRATEGY`` selects ONLY role-STRATEGY processes — the dedicated
    strategies container (``snapper strategies-engine``) runs them as
    THREAD-mode tasks off the backend loop; the backend then sets
    ``STRATEGIES_EMBEDDED=false`` so it stops owning them.
    """

    ALL = "all"
    API = "api"
    FEED = "feed"
    STRATEGY = "strategy"


class ProcessLifecycleEnum(StrEnum):
    """Process lifecycle type.

    Defines whether a process is designed to run continuously
    or execute once and terminate.
    """

    LONG_RUNNING = "long_running"
    ONE_SHOT = "one_shot"


class ProcessRoleEnum(StrEnum):
    """Process role in the system.

    Categorizes processes by their function: core services,
    maintenance tasks, trading strategies, or backtests.
    """

    CORE = "core"
    TASK = "task"
    STRATEGY = "strategy"
    BACKTEST = "backtest"


class ProcessRestartPolicyEnum(StrEnum):
    """Auto-restart policy for a managed process.

    Drives the launcher watchdog: ``ALWAYS`` respawns on any exit (even a
    clean one — for live feed publishers that must never stop on their own),
    ``ON_FAILURE`` respawns only when the run ends in the FAILED status (the
    safe default; a clean idle exit-0 maps to SUCCEEDED and is left alone),
    and ``NEVER`` disables auto-restart entirely.
    """

    ALWAYS = "always"
    ON_FAILURE = "on_failure"
    NEVER = "never"


class ProcessRunStatusEnum(StrEnum):
    """Current execution state of a managed process."""

    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class BacktestRunStatusEnum(StrEnum):
    """Lifecycle state of a backtest run."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCEL_REQUESTED = "cancel_requested"
    CANCELLED = "cancelled"


class TradeCommandStatusEnum(StrEnum):
    """Unified FSM status for TradeCommand DB rows + in-memory CommandState.

    Covers both the durable-command outbox lifecycle (``created`` →
    ``dispatched`` / ``direct_dispatched``) and the venue-side
    transitions observed by ``TradeService`` in the in-memory
    ``CommandState`` projection (``accepted``, ``filled``,
    ``partially_filled``, ``rejected``, ``cancelled``, ``expired``,
    ``failed``). A single enum is justified because the same command
    object's status field carries values from both domains across its
    lifetime: the engine inserts a ``TradeCommand`` row with
    ``status=created``, the outbox transitions it to ``dispatched``,
    and the venue events subsequently update the in-memory projection
    to the terminal status.
    """

    CREATED = "created"
    DISPATCHED = "dispatched"
    DIRECT_DISPATCHED = "direct_dispatched"
    ACCEPTED = "accepted"
    FILLED = "filled"
    PARTIALLY_FILLED = "partially_filled"
    REJECTED = "rejected"
    CANCELLED = "cancelled"
    EXPIRED = "expired"
    FAILED = "failed"


class ExecutionPlanStatusEnum(StrEnum):
    """Lifecycle state of an ExecutionPlan row.

    Covers every state the PlanExecutorService transitions through
    ``pending`` (created, not yet armed — written by the REST/MCP submit
    routes before the executor picks it up), ``armed`` (waiting for first
    tick / command), ``active`` (has in-flight child commands),
    ``paused`` (operator intervention), ``cancel_requested`` (cancel
    issued but children still racing), terminal states ``completed`` /
    ``cancelled`` / ``failed`` / ``expired``.
    """

    PENDING = "pending"
    ARMED = "armed"
    ACTIVE = "active"
    PAUSED = "paused"
    CANCEL_REQUESTED = "cancel_requested"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    FAILED = "failed"
    EXPIRED = "expired"


class PairedExecutionGroupStatusEnum(StrEnum):
    """Lifecycle state of a ``paired_execution_groups`` SCD2 row.

    Drives the multi-leg execution guard's bounded-compensation FSM:
    ``assembling`` (legs registering, no venue order dispatched yet);
    ``armed`` (all expected legs durably registered, owned outboxes may
    dispatch); ``broken`` (a leg rejected / expired / missed a deadline,
    no further dispatch, compensation pending); ``compensating``
    (reduce-only flatten of filled legs in progress); ``completed``
    (all legs reached a consistent terminal state); and
    ``manual_intervention`` (compensation could not complete
    automatically, operator action required). Persisted as the column
    string value; not enforced by a DB CHECK, so the FSM can gain new
    states without a constraint-widening migration.
    """

    ASSEMBLING = "assembling"
    ARMED = "armed"
    BROKEN = "broken"
    COMPENSATING = "compensating"
    COMPLETED = "completed"
    MANUAL_INTERVENTION = "manual_intervention"


class PairedExecutionLegStatusEnum(StrEnum):
    """Lifecycle state of a ``paired_execution_legs`` SCD2 row.

    A leg is the authoritative per-instrument record inside a paired
    group: ``pending`` (registered, command inserted, not yet armed);
    ``armed`` (group armed, command may dispatch); ``working`` (venue
    order acked / live, no fill yet); ``partially_filled`` / ``filled``
    (fill progress); ``rejected`` / ``cancelled`` / ``expired`` (venue
    terminal without full fill); ``broken`` (flagged for compensation
    because the group broke); ``compensating`` (reduce-only flatten in
    flight); ``flattened`` (net group exposure for this leg returned to
    zero); ``manual_intervention`` (compensation needs an operator).
    Persisted as the column string value; not enforced by a DB CHECK.
    """

    PENDING = "pending"
    ARMED = "armed"
    WORKING = "working"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    REJECTED = "rejected"
    CANCELLED = "cancelled"
    EXPIRED = "expired"
    BROKEN = "broken"
    COMPENSATING = "compensating"
    FLATTENED = "flattened"
    MANUAL_INTERVENTION = "manual_intervention"


class PairedExecutionPolicyEnum(StrEnum):
    """Execution-coordination policy for a paired-execution group.

    ``simultaneous`` arms all legs together behind one barrier
    (``CointegrationPairs`` and other same-instant spreads).
    ``sequential_handoff`` arms leg N+1 only once leg N is terminal
    (``ParlayCascade``-style exit-then-enter transitions), where
    flattening a successful prior leg would be wrong. Stored as the
    column string value and pinned by a DB CHECK because the policy set
    is immutable.
    """

    SIMULTANEOUS = "simultaneous"
    SEQUENTIAL_HANDOFF = "sequential_handoff"


class PairedGroupTerminalizeOutcome(StrEnum):
    """Outcome of an operator terminalize attempt on a paired-execution group.

    ``TERMINALIZED`` — the attestation was recorded and the group is now
    ``completed`` (the guard scanner's quiet-halt sweep then clears the
    scope's durable halt and every coordinator's in-memory mirror within one
    cycle). ``NOT_FOUND`` — no current active group carries the public id
    (maps to HTTP 404). ``NOT_TERMINALIZABLE`` — the group exists but is not
    operator-attestable right now: it is neither ``manual_intervention`` nor a
    ``compensating`` group re-opened onto a manual leg, or some leg is still
    in flight (neither settled at zero open exposure nor manual), so attesting
    would paper over live automation (maps to HTTP 409).
    """

    TERMINALIZED = "terminalized"
    NOT_FOUND = "not_found"
    NOT_TERMINALIZABLE = "not_terminalizable"


class PairedFillProjection(StrEnum):
    """Outcome of projecting a venue fill onto the paired-execution tables.

    Distinguishes WHICH leg-accounting field a fill landed on, and whether
    it changed anything, so the live fill hook can route a single venue
    event correctly. A fill on a leg's ORIGINAL order updates
    ``filled_signed_qty`` (``ORIGINAL_*``); a fill on a reduce-only FLATTEN
    order (whose command ``supersedes`` the leg's original) updates
    ``compensated_signed_qty`` (``COMPENSATION_*``). ``*_APPLIED`` wrote an
    SCD2 successor; ``*_NOOP`` matched a leg but the monotonic accounting
    left it unchanged (a duplicate / out-of-order / post-terminal replay);
    ``NO_MATCH`` resolved no leg at all (an ungrouped order). The original
    projection returns one of ``ORIGINAL_APPLIED`` / ``ORIGINAL_NOOP`` /
    ``NO_MATCH``; only on ``NO_MATCH`` does the caller try the compensation
    projection, so a non-paired fill never pays the second lookup twice.
    """

    ORIGINAL_APPLIED = "original_applied"
    ORIGINAL_NOOP = "original_noop"
    COMPENSATION_APPLIED = "compensation_applied"
    COMPENSATION_NOOP = "compensation_noop"
    NO_MATCH = "no_match"


class AiReviewStatusEnum(StrEnum):
    """Lifecycle state of an AI delegate review (CONSULT pattern).

    Covers the full state machine for an :class:`AiReview` row: created
    with ``PENDING``; transitions to ``FANOUT_DISPATCHED`` when the
    selected delegate goes offline (reconnect hysteresis) and the
    review is broadcast to fanout-eligible delegates; terminal at
    ``RESOLVED_APPROVED``/``RESOLVED_REJECTED`` on first decision,
    ``TIMEOUT`` if deadline passes, or ``SUPERSEDED`` if the strategy
    abandons (e.g. signal expired before deadline).

    All terminal states are FINAL — no further transitions. Audit row
    persists indefinitely in :class:`AiReview`; transition events
    captured in append-only :class:`AiReviewEvent` table.
    """

    PENDING = "pending"
    FANOUT_DISPATCHED = "fanout_dispatched"
    RESOLVED_APPROVED = "resolved_approved"
    RESOLVED_REJECTED = "resolved_rejected"
    TIMEOUT = "timeout"
    SUPERSEDED = "superseded"


class AiReviewDecisionEnum(StrEnum):
    """AI delegate decision outcome.

    APPROVE: agent approved the trade; strategy proceeds.
    REJECT: agent vetoed the trade — strategy ABORTS, no fall-through
    to baseline.
    """

    APPROVE = "approve"
    REJECT = "reject"


class AiReviewResolutionModeEnum(StrEnum):
    """How an :class:`AiReview` reached its terminal state.

    PICK_ONE_PRIMARY: selected delegate (immutable from review
    creation) responded directly. Most common case.
    SECONDARY_AFTER_FANOUT: selected delegate went offline; fanout
    fired; a different delegate (with scope grant) responded.
    FANOUT_FIRST_RESPONDER: review was created in fanout-dispatched
    state at INSERT time (selected delegate already offline >
    heartbeat window); first eligible responder wins.
    TIMEOUT_NO_RESPONSE: deadline passed; reaper transitioned to
    TIMEOUT status.
    SUPERSEDED_BY_STRATEGY: strategy abandoned the review (e.g.
    signal expired before deadline); status SUPERSEDED.
    """

    PICK_ONE_PRIMARY = "pick_one_primary"
    SECONDARY_AFTER_FANOUT = "secondary_after_fanout"
    FANOUT_FIRST_RESPONDER = "fanout_first_responder"
    TIMEOUT_NO_RESPONSE = "timeout_no_response"
    SUPERSEDED_BY_STRATEGY = "superseded_by_strategy"


class AiReviewEventTypeEnum(StrEnum):
    """Event type for append-only :class:`AiReviewEvent` audit log.

    Each transition of an :class:`AiReview` row appends ONE event
    row. Event types fully discriminate the audit trail without
    needing previous_status: rows are immutable; queries filter by
    event_type.
    """

    CREATED = "created"
    FANOUT_DISPATCHED = "fanout_dispatched"
    DECISION_RECORDED = "decision_recorded"
    TIMEOUT_MARKED = "timeout_marked"
    SUPERSEDED = "superseded"
    COUNTER_DECREMENTED = "counter_decremented"
    COUNTER_ADJUSTED = "counter_adjusted"


class ComponentStatusEnum(StrEnum):
    """Infrastructure component status for ZMQ and WebSocket health checks."""

    OK = "ok"
    ERROR = "error"


class StartProcessStatusEnum(StrEnum):
    """Start operation outcome."""

    SUCCESS = "success"
    ALREADY_RUNNING = "already_running"
    ERROR = "error"


class StopProcessStatusEnum(StrEnum):
    """Stop operation outcome."""

    SUCCESS = "success"
    NOT_RUNNING = "not_running"
    ERROR = "error"


class SpawnerProcessStatusEnum(StrEnum):
    """Subprocess-level process status for spawner-managed processes."""

    NOT_RUNNING = "not_running"
    RUNNING = "running"
    STOPPED = "stopped"
    COMPLETED = "completed"
    ERROR = "error"


class SubscriptionActionEnum(StrEnum):
    """WebSocket subscription action."""

    SUBSCRIBE = "subscribe"
    UNSUBSCRIBE = "unsubscribe"


class SubscriptionStatusEnum(StrEnum):
    """WebSocket subscription result status."""

    SUBSCRIBED = "subscribed"
    UNSUBSCRIBED = "unsubscribed"
    PARTIAL = "partial"
    DENIED = "denied"
    NO_TOPICS = "no_topics"


class FillStatusEnum(StrEnum):
    """Result of an order execution."""

    FILLED = "filled"
    PARTIAL = "partial"


class OrderTypeEnum(StrEnum):
    """Domain order execution type for the trading core.

    Covers the 4 order types the Snapper engine reasons about. Distinct
    from ``infrastructure.exchanges.contracts.ExchangeOrderTypeEnum``
    (wire-format) by design — exchange-side values include ICEBERG,
    STOP_LOSS_LIMIT, TAKE_PROFIT_LIMIT, TRAILING_STOP, SETTLE_POSITION
    etc. that the trading core never instantiates.
    """

    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"


class OrderStatusEnum(StrEnum):
    """Domain order lifecycle status.

    Covers the 7-state lifecycle the Snapper engine reasons about.
    Distinct from ``infrastructure.exchanges.contracts.ExchangeOrderStatusEnum``
    (wire-format) by design — exchange-side values use American
    spelling (``CANCELED``) and include ``PENDING``, ``CLOSED``,
    ``PENDING_NEW``, ``EXPIRED`` that the trading core reduces to this
    7-state set. The cross-spelling mapping is already handled in
    ``infrastructure/exchanges/implementations/kraken.py`` and
    ``infrastructure/exchanges/adapters/kraken_futures.py``.
    """

    NEW = "new"
    SUBMITTED = "submitted"
    OPEN = "open"
    FILLED = "filled"
    PARTIALLY_FILLED = "partially_filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


class OrderEventEnum(StrEnum):
    """Order lifecycle event emitted on orders.events.* ZMQ topics.

    Covers both ``OrderData`` (submit flow) and ``OrderEventData``
    (cancel/replace confirmations). The ``EXECUTED`` member is routed
    on a separate ``orders.events.*.*.executed`` topic via
    ``ExecutionData`` (with ``FillStatus``) — the ``OrderEventType``
    Literal alias at the bottom of this module intentionally excludes
    it, keeping non-execution events on a single type.

    ``UNKNOWN`` marks an ambiguous submit outcome: the venue call
    failed in a way where the order MAY exist on the venue (request
    possibly executed, response lost). It is non-terminal — the engine
    must keep the in-flight guard held until the executor resolves the
    order to accepted or rejected via venue verification.
    """

    SUBMITTED = "submitted"
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    EXECUTED = "executed"
    CANCELLED = "cancelled"
    EXPIRED = "expired"
    REPLACED = "replaced"
    UNKNOWN = "unknown"


TradeSide = Literal[TradeSideEnum.BUY, TradeSideEnum.SELL]
"""Direction of a trade: 'buy' for long entry, 'sell' for short/exit."""

OrderType = Literal[
    OrderTypeEnum.MARKET,
    OrderTypeEnum.LIMIT,
    OrderTypeEnum.STOP,
    OrderTypeEnum.STOP_LIMIT,
]
"""Order execution type determining how the order is matched."""

OrderStatus = Literal[
    OrderStatusEnum.NEW,
    OrderStatusEnum.SUBMITTED,
    OrderStatusEnum.OPEN,
    OrderStatusEnum.FILLED,
    OrderStatusEnum.PARTIALLY_FILLED,
    OrderStatusEnum.CANCELLED,
    OrderStatusEnum.REJECTED,
]
"""Order lifecycle state from creation through completion or cancellation."""

FillStatus = Literal[FillStatusEnum.FILLED, FillStatusEnum.PARTIAL]
"""Result of an order execution: 'filled' for complete, 'partial' for ongoing."""

CancelEventType = Literal[OrderEventEnum.CANCELLED, OrderEventEnum.REJECTED]
"""Event types for cancel command responses."""

StreamTerminalEventType = Literal[OrderEventEnum.CANCELLED, OrderEventEnum.EXPIRED]
"""Event types for venue-stream-sourced order terminals (PnL Phase 1).

A cancel/expiry arriving on the venue EXECUTION STREAM (as opposed to a
cancel-command response) must publish a bus terminal so the engine's
in-flight guard releases promptly instead of waiting for the lazy 60s
valve. Distinct from :data:`CancelEventType` because stream terminals
can be EXPIRED but never REJECTED."""

ReplaceEventType = Literal[OrderEventEnum.REPLACED, OrderEventEnum.REJECTED]
"""Event types for replace command responses."""

ORDER_STATUS_REASON_ADOPTED = "adopted"
"""``OrderData.reason`` marker on adoption-shaped ACCEPTED publishes (#155).

The executor sets it when adopting a venue-live order (ghost adoption,
ambiguous-submit verification, false-reject heal, startup recovery of a
venue-verified-open row); the trader coordinator re-arms a RELEASED
engine's in-flight guard ONLY on frames carrying this reason — ordinary
acceptances stay reason-less so normal submit semantics are untouched.
Shared constant (not a per-module literal) because the value is a wire
contract between two processes."""

OrderEventType = Literal[
    OrderEventEnum.SUBMITTED,
    OrderEventEnum.ACCEPTED,
    OrderEventEnum.REJECTED,
    OrderEventEnum.CANCELLED,
    OrderEventEnum.EXPIRED,
    OrderEventEnum.REPLACED,
    OrderEventEnum.UNKNOWN,
]
"""Order event type for ZMQ topic suffix (non-execution events).

This type MUST match the suffix of the orders.events.{exchange}.{instrument}.{event}
topic for non-execution events. Used by:
- OrderData.status: Full order lifecycle events (submit flow).
- OrderEventData.event: Lightweight cancel/replace confirmations.

Note: 'executed' is NOT in this type. Execution events use ExecutionData (with FillStatus),
not OrderData. This separation ensures clear payload types:
- orders.events.*.*.executed -> ExecutionData
- orders.events.*.*.{submitted|accepted|rejected|expired|unknown} -> OrderData
- orders.events.*.*.{cancelled|replaced|rejected} -> OrderEventData

The 'rejected' event may come from either data type:
- OrderData: Submit rejected by executor validation or exchange.
- OrderEventData: Cancel/replace rejected.

Events:
    submitted: Executor accepted command, order sent to exchange (local event).
    accepted: Exchange confirmed order receipt (exchange ACK).
    rejected: Order/cancel/replace was rejected by executor or exchange.
    cancelled: Order was successfully cancelled.
    expired: Order expired due to time-in-force constraint.
    replaced: Order was modified/replaced (cancel + new order).
"""

ExecutionMode = Literal[ExecutionModeEnum.LIVE, ExecutionModeEnum.PAPER]
"""Trading mode: 'live' for real money, 'paper' for simulation."""

OrderExchange = Literal[
    ExchangeEnum.PAPER,
    ExchangeEnum.KRAKEN,
    ExchangeEnum.KRAKEN_FUTURES,
    ExchangeEnum.WALUTOMAT,
]
"""Exchanges capable of order execution (paper simulator + live venues)."""

MarketSubscribeExchange = Literal[
    ExchangeEnum.KRAKEN,
    ExchangeEnum.KRAKEN_FUTURES,
    ExchangeEnum.KRAKEN_EQUITIES,
    ExchangeEnum.WALUTOMAT,
]
"""Live market feed exchanges (no paper — paper replays from these)."""

MarketDataExchange = Literal[
    ExchangeEnum.KRAKEN,
    ExchangeEnum.KRAKEN_FUTURES,
    ExchangeEnum.KRAKEN_EQUITIES,
    ExchangeEnum.WALUTOMAT,
    ExchangeEnum.POLYGON,
]
"""Exchanges that produce market data, live or historical."""

AllExchange = Literal[
    ExchangeEnum.PAPER,
    ExchangeEnum.KRAKEN,
    ExchangeEnum.KRAKEN_FUTURES,
    ExchangeEnum.KRAKEN_EQUITIES,
    ExchangeEnum.WALUTOMAT,
    ExchangeEnum.POLYGON,
]
"""All known exchange identifiers across all domains."""

HealthStatus = Literal[HealthStatusEnum.HEALTHY, HealthStatusEnum.WARNING, HealthStatusEnum.ERROR]
"""Component health state for monitoring and alerting."""

ComponentStatus = Literal[ComponentStatusEnum.OK, ComponentStatusEnum.ERROR]
"""Infrastructure component status for ZMQ and WebSocket health checks."""

ProcessMode = Literal[ProcessModeEnum.THREAD, ProcessModeEnum.PROCESS]
"""Process execution mode: 'thread' for in-process, 'process' for subprocess."""

StartProcessStatus = Literal[
    StartProcessStatusEnum.SUCCESS,
    StartProcessStatusEnum.ALREADY_RUNNING,
    StartProcessStatusEnum.ERROR,
]
"""Start operation outcome: success, already running, or error."""

StopProcessStatus = Literal[
    StopProcessStatusEnum.SUCCESS, StopProcessStatusEnum.NOT_RUNNING, StopProcessStatusEnum.ERROR
]
"""Stop operation outcome: success, not running, or error."""

ProcessLifecycleType = Literal[ProcessLifecycleEnum.LONG_RUNNING, ProcessLifecycleEnum.ONE_SHOT]
"""Process duration: 'long_running' for services, 'one_shot' for tasks."""

ProcessRoleType = Literal[
    ProcessRoleEnum.CORE, ProcessRoleEnum.TASK, ProcessRoleEnum.STRATEGY, ProcessRoleEnum.BACKTEST
]
"""Process role determining its function in the trading system."""

ProcessRunStatusType = Literal[
    ProcessRunStatusEnum.RUNNING,
    ProcessRunStatusEnum.SUCCEEDED,
    ProcessRunStatusEnum.FAILED,
    ProcessRunStatusEnum.CANCELLED,
]
"""Current execution state of a managed process."""

IndicatorBackend = Literal["talib", "python"]
"""Technical indicator computation backend: 'talib' for TA-Lib C library, 'python' for pure Python."""

UpsertResult = Literal["created", "updated", "unchanged"]
"""Outcome of an upsert operation: row was created, updated, or left unchanged."""

AssetType = Literal[
    AssetTypeEnum.CRYPTO,
    AssetTypeEnum.FOREX,
    AssetTypeEnum.EQUITY,
    AssetTypeEnum.INDEX,
    AssetTypeEnum.COMMODITY,
    AssetTypeEnum.YIELD,
]
"""Financial asset class category for symbol catalog entries."""

MarketDataType = Literal[
    MarketDataTypeEnum.TICKS, MarketDataTypeEnum.TRADES, MarketDataTypeEnum.CANDLES
]
"""Market data type for ZMQ topic routing (market.{exchange}.{instrument}.{type})."""

SubscriptionAction = Literal[SubscriptionActionEnum.SUBSCRIBE, SubscriptionActionEnum.UNSUBSCRIBE]
"""WebSocket subscription action: subscribe to or unsubscribe from topics."""

SubscriptionStatus = Literal[
    SubscriptionStatusEnum.SUBSCRIBED,
    SubscriptionStatusEnum.UNSUBSCRIBED,
    SubscriptionStatusEnum.PARTIAL,
    SubscriptionStatusEnum.DENIED,
    SubscriptionStatusEnum.NO_TOPICS,
]
"""WebSocket subscription result status indicating the outcome of a subscription request."""

SpawnerProcessStatus = Literal[
    SpawnerProcessStatusEnum.NOT_RUNNING,
    SpawnerProcessStatusEnum.RUNNING,
    SpawnerProcessStatusEnum.STOPPED,
    SpawnerProcessStatusEnum.COMPLETED,
    SpawnerProcessStatusEnum.ERROR,
]
"""Subprocess-level process status for spawner-managed processes."""

AliasChannel = Literal[AliasChannelEnum.WS, AliasChannelEnum.REST, AliasChannelEnum.CCXT]
"""Symbol alias channel type: ws for WebSocket, rest for REST API, ccxt for CCXT library."""

InstrumentKind = Literal[
    InstrumentKindEnum.SPOT,
    InstrumentKindEnum.PERPETUAL,
    InstrumentKindEnum.FUTURE,
    InstrumentKindEnum.ETF,
    InstrumentKindEnum.OPTION,
]
"""Instrument product type: spot, perpetual, future, etf, or option."""

OrderCommand = Literal[OrderCommandEnum.SUBMIT, OrderCommandEnum.CANCEL, OrderCommandEnum.REPLACE]
"""Order command types for orders.commands.* ZMQ topics."""

OrderEvent = Literal[
    OrderEventEnum.SUBMITTED,
    OrderEventEnum.ACCEPTED,
    OrderEventEnum.REJECTED,
    OrderEventEnum.EXECUTED,
    OrderEventEnum.CANCELLED,
    OrderEventEnum.EXPIRED,
    OrderEventEnum.REPLACED,
    OrderEventEnum.UNKNOWN,
]
"""Order event types for orders.events.* ZMQ topics."""

PairedExecutionPolicy = Literal[
    PairedExecutionPolicyEnum.SIMULTANEOUS,
    PairedExecutionPolicyEnum.SEQUENTIAL_HANDOFF,
]
"""Coordination policy carried on grouped signals: simultaneous or sequential."""

__all__ = [
    "AliasChannel",
    "AliasChannelEnum",
    "AllExchange",
    "AssetType",
    "AssetTypeEnum",
    "ComponentStatus",
    "ComponentStatusEnum",
    "ExchangeEnum",
    "ExecutionMode",
    "ExecutionModeEnum",
    "ExecutionPlanStatusEnum",
    "FillStatus",
    "FillStatusEnum",
    "HealthStatus",
    "HealthStatusEnum",
    "IndicatorBackend",
    "InstrumentKind",
    "InstrumentKindEnum",
    "MarketDataExchange",
    "MarketDataType",
    "MarketDataTypeEnum",
    "MarketSubscribeExchange",
    "OrderCommand",
    "OrderCommandEnum",
    "OrderEvent",
    "OrderEventEnum",
    "OrderEventType",
    "OrderExchange",
    "OrderStatus",
    "OrderStatusEnum",
    "OrderType",
    "OrderTypeEnum",
    "PairedExecutionGroupStatusEnum",
    "PairedExecutionLegStatusEnum",
    "PairedExecutionPolicy",
    "PairedExecutionPolicyEnum",
    "PairedFillProjection",
    "PairedGroupTerminalizeOutcome",
    "ProcessLifecycleEnum",
    "ProcessLifecycleType",
    "ProcessMode",
    "ProcessModeEnum",
    "ProcessRoleEnum",
    "ProcessRoleType",
    "ProcessRunStatusEnum",
    "ProcessRunStatusType",
    "RelationshipTypeEnum",
    "SpawnerProcessStatus",
    "SpawnerProcessStatusEnum",
    "StartProcessStatus",
    "StartProcessStatusEnum",
    "StopProcessStatus",
    "StopProcessStatusEnum",
    "SubscriptionAction",
    "SubscriptionActionEnum",
    "SubscriptionStatus",
    "SubscriptionStatusEnum",
    "TradeCommandStatusEnum",
    "TradeSide",
    "TradeSideEnum",
    "UpsertResult",
]
