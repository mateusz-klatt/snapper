"""Market data, trading event, and system message schemas for ZMQ messaging.

This module defines Pydantic models representing all entities that flow through
the ZMQ messaging bus. Each entity inherits StrictDataSchema which provides id
(UUID7), type (Literal discriminator), and timestamp (bus creation time).

These schemas serve as the single source of truth for both ZMQ transport and
REST API responses. Domain-specific timestamps (open_at, fired_at, executed_at,
created_at) are separate from the bus timestamp.

Market data classes:
    TickData: Real-time bid/ask/last price snapshot.
    CandleData: OHLCV candle data for a specific timeframe.
    TradeData: Individual trade execution data.

Trading event classes:
    SignalData: Trading signal with direction and strength.
    ExecutionData: Order fill/execution details.
    OrderData: Current order state and fill progress.
    PositionData: Portfolio position snapshot.

Order command classes:
    OrderRequestData: Order submission request.
    OrderCancelData: Order cancellation request.
    OrderReplaceData: Order modification request.
    OrderEventData: Lightweight order lifecycle event.

System message classes:
    HeartbeatData: Component health heartbeat.
    SettingChangedData: Configuration change notification.
    SymbolAliasUpdateData: Symbol alias cache invalidation.
    ReplayStartData: Historical data replay start marker.
    ReplayEndData: Historical data replay end marker.
"""

from datetime import datetime
from typing import Literal
from typing import Self

from pydantic import Field
from pydantic import model_validator

from snapper.api.schemas.base import StrictBody
from snapper.api.schemas.base import StrictDataSchema
from snapper.core.json_types import JsonObject
from snapper.core.types import ExchangeEnum
from snapper.core.types import ExecutionModeEnum
from snapper.core.types import MarketDataExchange
from snapper.core.types import OrderEventType
from snapper.core.types import OrderExchange
from snapper.interface.websocket.schemas import ExecutionMode
from snapper.interface.websocket.schemas import FillStatus
from snapper.interface.websocket.schemas import HealthStatus
from snapper.interface.websocket.schemas import OrderType
from snapper.interface.websocket.schemas import TradeSide


class TickData(StrictDataSchema[Literal["tick"]]):
    """Real-time price tick snapshot from an exchange.

    Represents a point-in-time snapshot of bid/ask prices and last trade.
    Used for real-time price monitoring and spread calculations.

    Attributes:
        instrument: Trading pair symbol (e.g., 'BTC-USD').
        exchange: Source exchange producing this tick data.
        volume: Trading volume for the current period.
        bid: Best bid price (highest buy order).
        ask: Best ask price (lowest sell order).
        last: Last traded price.
        is_delayed: Whether the feed delivers ticks with an exchange-mandated
            delay (Kraken FCM / TradFi index futures = ~10 min). Strategies
            must gate on this flag before treating the price as current.
        is_extended_hours: Whether the tick occurred during extended trading
            hours (TradFi overnight session). ``None`` means the feed does
            not distinguish extended-hours ticks.
    """

    type: Literal["tick"] = "tick"
    instrument: str
    exchange: MarketDataExchange
    volume: float
    bid: float | None = None
    ask: float | None = None
    last: float | None = None
    is_delayed: bool = False
    is_extended_hours: bool | None = None


class CandleData(StrictDataSchema[Literal["candle"]]):
    """OHLCV candlestick data for technical analysis.

    Represents aggregated price action over a specific timeframe.
    Used by strategies for pattern recognition and indicator calculation.

    Attributes:
        instrument: Trading pair symbol (e.g., 'BTC-USD').
        exchange: Source exchange producing this candle data.
        timeframe: Candle duration (e.g., '1m', '1h', '1d').
        open_at: Exchange-provided candle interval start time.
        open: Opening price of the candle.
        high: Highest price during the candle.
        low: Lowest price during the candle.
        close: Closing price of the candle.
        volume: Total traded volume during the candle.
        vwap: Volume-weighted average price (optional).
        trades: Number of trades in the candle (optional).
    """

    type: Literal["candle"] = "candle"
    instrument: str
    exchange: MarketDataExchange
    timeframe: str
    open_at: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: float | None = None
    trades: int | None = None


class TradeData(StrictDataSchema[Literal["trade"]]):
    """Individual trade execution from the market.

    Represents a single trade that occurred on the exchange.
    Used for trade tape analysis and market activity monitoring.

    Attributes:
        instrument: Trading pair symbol (e.g., 'BTC-USD').
        exchange: Source exchange where the trade occurred.
        executed_at: Exchange-provided trade execution timestamp.
        price: Execution price of the trade.
        volume: Size of the trade.
        side: Trade direction ('buy'/'sell') if available.
        trade_id: Exchange-provided trade identifier for deduplication.
    """

    type: Literal["trade"] = "trade"
    instrument: str
    exchange: MarketDataExchange
    executed_at: datetime | None = None
    price: float
    volume: float
    side: str | None = None
    trade_id: str | None = None


class SignalData(StrictDataSchema[Literal["signal"]]):
    """Trading signal generated by a strategy.

    Represents a recommendation to enter or exit a position.
    Signals are published to the messaging bus for execution.

    Attributes:
        instrument: Target trading pair symbol.
        exchange: Target exchange for execution.
        side: Recommended direction ('buy' or 'sell').
        strength: Signal confidence from 0.0 (weak) to 1.0 (strong).
        reason: Human-readable explanation for the signal.
        price: Suggested entry/exit price (optional).
        strategy_name: Name of the generating strategy (optional).
        fired_at: Domain timestamp when the signal was generated.
    """

    type: Literal["signal"] = "signal"
    instrument: str
    exchange: OrderExchange
    side: TradeSide
    strength: float = Field(ge=0.0, le=1.0)
    reason: str
    price: float | None = None
    strategy_name: str | None = None
    fired_at: datetime
    wallet_public_id: str = ""
    operator_public_id: str | None = None
    user_public_id: str | None = None

    @model_validator(mode="after")
    def _paper_requires_strategy_name(self) -> Self:
        """Paper signals require strategy_name for topic derivation."""
        if self.exchange == ExchangeEnum.PAPER and not self.strategy_name:
            raise ValueError(
                "Paper signals require strategy_name to be set "
                "for topic derivation (signals.paper.{instrument}.{strategy_name})"
            )
        return self


class ExecutionData(StrictDataSchema[Literal["execution"]]):
    """Order fill/execution details from an exchange.

    Represents a completed or partial fill of an order.
    Contains all information needed for trade tracking and P&L calculation.

    Attributes:
        trade_id: Unique fill/trade ID from exchange (e.g., Kraken exec_id).
            May be None for exchanges that don't provide it.
        exchange_order_id: Exchange-assigned order ID (e.g., Kraken txid).
            May be None if exchange hasn't assigned an ID yet.
        client_order_id: Our generated order ID (e.g., 'signal-a1b2c3d4').
        instrument: Trading pair symbol.
        exchange: Exchange where the fill occurred.
        side: Trade direction ('buy' or 'sell').
        size: Cumulative filled quantity across all fills for the order.
        price: Cumulative average execution price across all fills.
        last_size: Incremental quantity filled by this execution event (delta).
        last_price: Price of the incremental fill (delta).
        fee: Transaction fee charged.
        fee_asset: Currency of the fee (e.g., 'USD', 'BTC').
        status: Fill status ('filled', 'partial', etc.).
        executed_at: Timestamp of the fill.
    """

    type: Literal["execution"] = "execution"
    trade_id: str | None = None
    exchange_order_id: str | None = None
    client_order_id: str
    instrument: str
    exchange: OrderExchange
    side: TradeSide
    size: float
    price: float
    last_size: float
    last_price: float
    fee: float
    fee_asset: str
    status: FillStatus
    executed_at: datetime
    wallet_public_id: str = ""
    operator_public_id: str | None = None
    user_public_id: str | None = None
    liquidity_role: str = "unknown"


class OrderData(StrictDataSchema[Literal["order"]]):
    """Current state of an order.

    Used for both ZMQ event publishing and REST API responses.
    Published on orders.events.{exchange}.{instrument}.{status} topics.

    INVARIANT: The 'status' field MUST match the topic suffix.

    Attributes:
        exchange_order_id: Exchange-assigned order ID (e.g., Kraken txid).
            May be None before exchange ACK (e.g., for 'submitted' event).
        client_order_id: Our generated order ID (e.g., 'signal-a1b2c3d4').
        instrument: Trading pair symbol.
        exchange: Exchange where the order is placed.
        side: Order direction ('buy' or 'sell').
        status: Event type matching topic suffix (OrderEventType, excludes 'execution').
        order_type: Type of order ('market', 'limit', etc.).
        size: Total order size.
        filled_size: Amount filled so far.
        price: Limit price (for limit orders).
        average_price: Average fill price (for partial fills).
        reason: Optional rejection/failure reason (for 'rejected' status).
        time_in_force: Order time-in-force setting.
        error: Error message if order failed.
        created_at: Order creation timestamp.
        updated_at: Last status update timestamp.
        leverage: Margin leverage (None for spot, integer for margin).
        reduce_only: True when the order may only reduce an existing position.
    """

    type: Literal["order"] = "order"
    exchange_order_id: str | None = None
    client_order_id: str
    instrument: str
    exchange: OrderExchange
    mode: ExecutionMode = ExecutionModeEnum.LIVE
    side: TradeSide
    status: str
    order_type: OrderType
    size: float
    filled_size: float
    price: float | None = None
    average_price: float | None = None
    reason: str | None = None
    time_in_force: str | None = None
    error: str | None = None
    created_at: datetime
    updated_at: datetime | None = None
    leverage: int | None = None
    reduce_only: bool = False
    wallet_public_id: str = ""
    operator_public_id: str | None = None
    user_public_id: str | None = None
    plan_public_id: str | None = None


class PositionData(StrictDataSchema[Literal["position"]]):
    """Portfolio position snapshot.

    Represents a single position in the portfolio.
    Used for both ZMQ event publishing and REST API responses.
    The inherited ``timestamp`` field carries the last-update time.

    Attributes:
        instrument: Trading pair native symbol (e.g. "BTC-USD").
        instrument_public_id: UUID7 of the underlying ``instruments``
            row. Surfaces here so iOS reduce/close mutations can
            target the position with a single
            ``CreateOrderCommand`` payload (which requires
            ``instrument_public_id`` per
            ``CreateOrderBody``) without an extra symbol -> id
            lookup round-trip.
        exchange: Exchange where the position is held.
        quantity: Position size (positive for long, negative for short).
        average_price: Average entry price.
        unrealized_pnl: Unrealized profit/loss.
        realized_pnl: Realized profit/loss.
        position_cycle_public_id: Public ID of the open position cycle, if any.
        wallet_public_id: Owning wallet UUID7 — non-null on the ORM
            so always present in projection. Surfaces here so iOS /
            UI clients can wallet-scope the position list without an
            extra position-cycle join (Plan iOS-NP backlog item 4).
    """

    type: Literal["position"] = "position"
    instrument: str
    instrument_public_id: str = ""
    exchange: OrderExchange
    mode: ExecutionMode = ExecutionModeEnum.LIVE
    quantity: float
    average_price: float
    unrealized_pnl: float
    realized_pnl: float
    position_cycle_public_id: str | None = None
    wallet_public_id: str = ""


class OrderRequestData(StrictDataSchema[Literal["order_request"]]):
    """Order request from strategy to executor.

    Sent by strategies to request order placement on an exchange.
    Contains all information needed for order creation.
    Published on: orders.commands.{exchange}.{instrument}.submit

    Attributes:
        strategy_id: Identifier of the requesting strategy.
        exchange: Target exchange for the order.
        instrument: Trading pair symbol.
        mode: Execution mode ('live' or 'paper').
        side: Order direction ('buy' or 'sell').
        order_type: Type of order ('market', 'limit', etc.).
        quantity: Order size (must be positive).
        price: Limit price (required for limit orders).
        client_order_id: Client-side order identifier.
        signaled_at: Original signal timestamp (optional).
        leverage: Margin leverage (None for spot, integer for margin).
        reduce_only: True when closing an existing position.
    """

    type: Literal["order_request"] = "order_request"
    strategy_id: str
    exchange: OrderExchange
    instrument: str
    mode: ExecutionMode
    side: TradeSide
    order_type: OrderType
    quantity: float = Field(gt=0)
    price: float | None = None
    client_order_id: str
    signaled_at: datetime | None = None
    strategy_tag: str | None = None
    leverage: int | None = None
    reduce_only: bool = False
    wallet_public_id: str = ""
    operator_public_id: str | None = None
    user_public_id: str | None = None


class OrderCancelData(StrictDataSchema[Literal["order_cancel"]]):
    """Order cancel request from strategy to executor.

    Sent to request cancellation of an existing order.
    Published on: orders.commands.{exchange}.{instrument}.cancel

    Attributes:
        exchange: Target exchange for the cancel.
        instrument: Trading pair symbol.
        exchange_order_id: Exchange-assigned order ID.
        client_order_id: Our generated order ID.
    """

    type: Literal["order_cancel"] = "order_cancel"
    exchange: OrderExchange
    instrument: str
    exchange_order_id: str
    client_order_id: str
    wallet_public_id: str = ""
    operator_public_id: str | None = None
    user_public_id: str | None = None


class OrderReplaceData(StrictDataSchema[Literal["order_replace"]]):
    """Order replace/modify request from strategy to executor.

    Sent to request modification of an existing order (price/quantity).
    Published on: orders.commands.{exchange}.{instrument}.replace

    Attributes:
        exchange: Target exchange for the replace.
        instrument: Trading pair symbol.
        exchange_order_id: Exchange-assigned order ID.
        client_order_id: Our generated order ID.
        new_quantity: New order quantity (optional).
        new_price: New limit price (optional).
    """

    type: Literal["order_replace"] = "order_replace"
    exchange: OrderExchange
    instrument: str
    exchange_order_id: str
    client_order_id: str
    new_quantity: float | None = None
    new_price: float | None = None
    wallet_public_id: str = ""
    operator_public_id: str | None = None
    user_public_id: str | None = None


class OrderEventData(StrictDataSchema[Literal["order_event"]]):
    """Lightweight order event for cancel/replace confirmations.

    Used for publishing order lifecycle events that don't require full order
    details. Preferred for cancel/replace results because those commands
    don't carry side/order_type information.

    Published on: orders.events.{exchange}.{instrument}.{event}

    INVARIANT: The 'event' field MUST match the topic suffix.

    Attributes:
        exchange_order_id: Exchange-assigned order ID.
        client_order_id: Our generated order ID.
        exchange: Exchange where the order exists.
        instrument: Trading pair symbol.
        event: Event type matching topic suffix (OrderEventType).
        reason: Optional rejection/cancellation reason.
    """

    type: Literal["order_event"] = "order_event"
    exchange_order_id: str
    client_order_id: str
    exchange: OrderExchange
    instrument: str
    event: OrderEventType
    reason: str | None = None
    wallet_public_id: str = ""
    operator_public_id: str | None = None
    user_public_id: str | None = None


class HeartbeatData(StrictDataSchema[Literal["heartbeat"]]):
    """Component health heartbeat message.

    Published periodically by components to indicate they are alive.
    Used for health monitoring and dead component detection.

    Attributes:
        component: Name of the sending component.
        sequence: Domain-level heartbeat generation count (not transport sequence_id).
        status: Current health status.
        lag_ms: Processing lag in milliseconds.
        meta: Optional metadata dictionary for extensions.
    """

    type: Literal["heartbeat"] = "heartbeat"
    component: str
    sequence: int
    status: HealthStatus
    lag_ms: int
    meta: JsonObject = Field(default={})


class SettingChangedData(StrictDataSchema[Literal["setting_changed"]]):
    """Configuration setting change notification.

    Published when a setting is modified in the database.
    Subscribers use this to invalidate caches or reload config.

    Attributes:
        key: Setting key that changed.
        value: New setting value.
        category: Setting category for grouping.
        updated_by: User who made the change (optional).
    """

    type: Literal["setting_changed"] = "setting_changed"
    key: str
    value: str
    category: str
    updated_by: str | None = None


class UserDeactivatedData(StrictDataSchema[Literal["user_deactivated"]]):
    """Admin kill-switch event for a deactivated user.

    Published by `UserService.deactivate_user` as the SOLE publisher of
    the `admin.user_deactivated` bus topic (single-publisher
    rule resolves). Subscribers
    `AuthenticatedWebSocketManager` — closes active WS connections
      whose principal matches `user_public_id` with code 4003.
    `TokenManager` (cross-instance) — evicts every LRU cache entry
      where the cached `user_public_id` matches, eliminating the 30s
      multi-instance cache-staleness gap.
    Audit consumers — preserves `reason` for the kill-switch trail.
    `TokenManager.revoke_user_sessions` is invoked synchronously by
    the publisher BEFORE this event so the local instance is already
    guaranteed-rejecting; the bus event handles cross-instance fanout
    only.

    Attributes:
        user_public_id: UUID7 of the deactivated user row.
        deactivated_at: UTC timestamp of the kill-switch effective
            moment (matches the bus_time of the SCD2 close+insert).
        reason: Optional admin-supplied rationale, forwarded verbatim
            from `DeactivateUserBody.reason`.
    """

    type: Literal["user_deactivated"] = "user_deactivated"
    user_public_id: str
    deactivated_at: datetime
    reason: str | None = None


class CapsViolationAfterAiApproveData(StrictDataSchema[Literal["caps_violation_after_ai_approve"]]):
    """Plan D §3.6 — caps-violation event for an already-AI-approved CONSULT.

    Published by ``TradingCapsEnforcer`` (deferred wiring chunk —
    publisher side ships when the enforcer learns to thread the
    ``ai_review_public_id`` through ``TradeCommandSubmission``) on the
    internal ``bus.caps_violation_after_ai_approve`` topic when a
    trade that previously passed the AI delegate's review later
    fails the caps gate. ``AiReviewService.handle_caps_violation_bus_message``
    subscribes and re-publishes the event onto the external WS
    topic ``ai_reviews.{user_public_id}.{strategy_public_id}.caps_violation``
    so the bridge (Plan C) can surface the rejection back to the
    delegate's UI.

    Attributes:
        review_public_id: UUID7 of the ``ai_reviews`` row whose
            decision authorised the trade.
        user_public_id: Owner of the strategy that originally
            consulted — drives the WS topic suffix.
        strategy_public_id: Origin strategy — drives the WS topic
            suffix so the operator dashboard can correlate back to
            the strategy that made the call.
        wallet_public_id: Wallet the trade would have settled on.
        instrument_public_id: Instrument the trade was on.
        cap_type: Plan A / Plan D cap classifier (e.g.
            ``"max_open_orders"``, ``"max_daily_notional_usd"``,
            ``"max_position_quantity"``).
        attempted: Numeric value the trade tried to push the cap to.
        limit: Configured cap value for the user that the attempt
            exceeded.
    """

    type: Literal["caps_violation_after_ai_approve"] = "caps_violation_after_ai_approve"
    review_public_id: str
    user_public_id: str
    strategy_public_id: str
    wallet_public_id: str
    instrument_public_id: str
    cap_type: str
    attempted: float
    limit: float
    dispatch_version: int
    """Plan A Q18 — bridge dedup key. Mirrors the ``ai_reviews``
    row's ``dispatch_version`` at publish time so the JS bridge
    dispatcher can dedupe replays of the same caps violation
    against the same review row (e.g. if the listener re-dispatches
    a frame the bridge already forwarded)."""


class AiReviewRequestFrameData(StrictDataSchema[Literal["ai_review.request"]]):
    """Plan A §4.2 / Q16 — external WS frame for new AI review consults.

    Published by :meth:`AiReviewService.create_review` (sole publisher)
    AFTER the atomic claim+insert commits. Targets the
    ``ai_reviews.{user_public_id}.{strategy_public_id}.request`` topic
    so the bridge per-frame scope filter (Plan A Q15) routes it to
    AI delegates that have a scope grant on the wallet+instrument.
    Routing fields stay at envelope level (top-level
    ``wallet_public_id`` + ``instrument_public_id``) so the bridge
    scope filter reads them directly off the parsed JSON without
    descending into the nested payload.

    Q18 dedup: ``dispatch_version`` is the original review row's
    counter (always 0 on the request frame since the row was just
    inserted; supersede / fanout retries that increment the counter
    are surfaced as separate frames). The JS bridge dispatcher
    dedupes by ``(public_id, dispatch_version)`` so duplicate
    receives from a flapping subscriber don't replay the consult
    request.

    Attributes:
        review_public_id: UUID7 of the just-created ``ai_reviews`` row.
        user_public_id: Owner of the consulting strategy.
        strategy_public_id: Origin strategy.
        wallet_public_id: Wallet the strategy would trade on.
        instrument_public_id: Instrument the strategy is consulting on.
        selected_delegate_public_id: AI delegate selected by Q10
            admission control. Threaded onto the frame so the
            delegate UI can show a "this is for you" affordance vs a
            broadcast frame received via fanout.
        deadline: ISO8601 wall-clock deadline by which the delegate
            must submit a decision before the strategy primitive
            transitions the row to ``timeout``. The delegate UI
            renders this as a countdown.
        signal_envelope: Plan A §3 strategy signal payload.
            Bounded by 16KB at the create_review enforcement layer.
        instrument_metadata: Strategy-supplied instrument context
            (e.g. order book snapshot, recent volatility metrics)
            used by the delegate to inform their decision.
        dispatch_version: Plan A Q18 — bridge dedup key.
    """

    type: Literal["ai_review.request"] = "ai_review.request"
    review_public_id: str
    user_public_id: str
    strategy_public_id: str
    wallet_public_id: str
    instrument_public_id: str
    selected_delegate_public_id: str
    deadline: datetime
    signal_envelope: JsonObject
    instrument_metadata: JsonObject
    dispatch_version: int


class AiReviewDecisionAckFrameData(StrictDataSchema[Literal["ai_review.decision_ack"]]):
    """Plan A §4.2 / Q16 — external WS frame acknowledging a decision.

    Published by :meth:`AiReviewService.submit_decision` (sole
    publisher) AFTER the atomic resolve commits + the audit event +
    the counter decrement + the internal
    ``bus.ai_review_decision`` fast-path event. Targets the
    ``ai_reviews.{user_public_id}.{strategy_public_id}.decision_ack``
    topic so the operator dashboard + the originating delegate can
    surface the decision in real-time without polling REST.

    Distinct from the internal ``bus.ai_review_decision`` event
    (Phase 2 #3): that event drives the strategy primitive's
    in-process Future fast-path; this frame drives the bridge fanout
    to WS subscribers (delegate UIs + operator dashboards). Both
    fire from the same submit_decision post-commit chain but go to
    different subscribers via different topic families.

    Routing fields stay at envelope top-level so the bridge
    per-frame scope filter (Plan A Q15) reads
    ``wallet_public_id`` + ``instrument_public_id`` directly without
    descending into the nested payload.

    Q18 dedup: ``dispatch_version`` is the resolved review row's
    current counter (atomic_resolve preserves it; supersede / fanout
    flows are the operations that bump it). A re-fanout of the same
    decision after the row's version bumps surfaces as a fresh frame
    to the delegate UI.

    Attributes:
        review_public_id: UUID7 of the resolved ``ai_reviews`` row.
        user_public_id: Owner of the consulting strategy.
        strategy_public_id: Origin strategy.
        wallet_public_id: Wallet the strategy would trade on.
        instrument_public_id: Instrument the strategy consulted on.
        responding_delegate_public_id: ``ai_delegates.public_id`` of
            the delegate whose decision resolved the row.
        decision: Plan A Q9 enum value — ``"approve"`` or ``"reject"``.
        new_status: Resolved row's terminal status
            (``resolved_approved`` or ``resolved_rejected``).
        resolution_mode: Plan A Q4 enum — ``pick_one_primary`` /
            ``secondary_after_fanout`` / ``fanout_first_responder``.
        rationale: Optional free-form rationale supplied by the
            delegate, threaded onto the audit event payload + this
            frame so the operator dashboard can show it next to the
            decision.
        dispatch_version: Plan A Q18 — bridge dedup key.
    """

    type: Literal["ai_review.decision_ack"] = "ai_review.decision_ack"
    review_public_id: str
    user_public_id: str
    strategy_public_id: str
    wallet_public_id: str
    instrument_public_id: str
    responding_delegate_public_id: str
    decision: str
    new_status: str
    resolution_mode: str
    rationale: str | None
    dispatch_version: int


class AiReviewCapsViolationFrameData(StrictDataSchema[Literal["ai_review.caps_violation"]]):
    """Plan A §4.2 / Q16 — external WS frame for caps violations.

    Public-facing counterpart to :class:`CapsViolationAfterAiApproveData`.
    The internal bus topic ``bus.caps_violation_after_ai_approve`` carries
    the snake-case ``caps_violation_after_ai_approve`` discriminator;
    Plan A §4.2 line 467 fixes the OUTBOUND WS frame discriminator at
    ``ai_review.caps_violation`` so the JS bridge dispatcher's
    ``switch (frame.type)`` (Plan A §4.2 line 503) routes the frame to
    the delegate's ``handleCapsViolation`` handler.
    :meth:`AiReviewService.handle_caps_violation_bus_message` translates
    from the internal schema to this external schema before publishing
    to the ``ai_reviews.{user}.{strategy}.caps_violation`` topic.
    Routing fields stay at envelope level so the bridge per-frame scope
    filter (Plan A Q15) reads ``wallet_public_id`` +
    ``instrument_public_id`` directly off the parsed envelope without
    descending into a nested payload.

    Attributes:
        review_public_id: UUID7 of the ``ai_reviews`` row whose AI
            decision authorised the trade that later violated a cap.
        user_public_id: Owner of the consulting strategy — drives the
            WS topic suffix.
        strategy_public_id: Origin strategy — drives the WS topic
            suffix so the operator dashboard can correlate the rejection
            back to the strategy that made the call.
        wallet_public_id: Wallet the trade would have settled on.
        instrument_public_id: Instrument the trade was on.
        cap_type: Plan A / Plan D cap classifier (e.g.
            ``"max_open_orders"``, ``"max_daily_notional_usd"``,
            ``"max_position_quantity"``).
        attempted: Numeric value the trade tried to push the cap to.
        limit: Configured cap value for the user that the attempt
            exceeded.
    """

    type: Literal["ai_review.caps_violation"] = "ai_review.caps_violation"
    review_public_id: str
    user_public_id: str
    strategy_public_id: str
    wallet_public_id: str
    instrument_public_id: str
    cap_type: str
    attempted: float
    limit: float
    dispatch_version: int
    """Plan A Q18 — bridge dedup key forwarded verbatim from the
    internal :class:`CapsViolationAfterAiApproveData` (which the
    publisher populates from the ``ai_reviews`` row). Listed here so
    the external WS frame matches the Plan A §4.2 line 481 envelope
    contract that fixes ``dispatch_version`` as a required field on
    every ``ai_review.*`` frame type."""


class AiReviewDecisionData(StrictDataSchema[Literal["ai_review_decision"]]):
    """Plan A §7.1 — internal bus event published when an AI review row resolves.

    Published by :meth:`AiReviewService.submit_decision` (sole publisher)
    on the internal ``bus.ai_review_decision`` topic AFTER the atomic
    state-machine transition + audit event + counter decrement have
    committed. Sole subscriber is the same :class:`AiReviewService`
    bus listener (Phase 2 #1) which resolves the registered
    :class:`asyncio.Future` so the strategy primitive's await loop
    (Plan A §7.1) wakes up immediately instead of spinning the DB-poll
    loop until the next jitter interval. Cross-instance: a remote
    instance's listener that finds no matching future in its registry
    treats the event as a no-op (the strategy's future was registered
    on the originating instance).

    Attributes:
        review_public_id: UUID7 of the resolved ``ai_reviews`` row.
            The strategy primitive uses this to look up the matching
            future in :class:`AiReviewService`'s in-memory registry.
        responding_delegate_public_id: ``ai_delegates.public_id`` of
            the delegate whose decision resolved the row. Threaded onto
            the outcome surfaced to the strategy.
        decision: Plan A Q9 enum value — ``"approve"`` or ``"reject"``.
        new_status: Resolved row's terminal status (``resolved_approved``
            or ``resolved_rejected``).
        resolution_mode: Plan A Q4 enum — ``pick_one_primary`` /
            ``secondary_after_fanout`` / ``fanout_first_responder``.
        dispatch_version: Plan A Q18 — bridge dedup key forwarded onto
            the optional external WS frame so a re-fanout of the same
            decision after a row's version bumped surfaces as a fresh
            frame to the delegate UI.
    """

    type: Literal["ai_review_decision"] = "ai_review_decision"
    review_public_id: str
    responding_delegate_public_id: str
    decision: str
    new_status: str
    resolution_mode: str
    dispatch_version: int


class DelegateOfflineData(StrictDataSchema[Literal["delegate_offline"]]):
    """Q17 Layer 1 fast-path event for an AI delegate that dropped its WS.

    Published by :class:`WebSocketAuthManager` on the
    ``bus.delegate_offline`` topic ONLY after a delay equal to
    ``_delegate_offline_grace_seconds`` so a flapping reconnect within
    the grace window cancels the publish before any subscriber observes
    a phantom-offline transition. Sole subscriber per Plan D §3.5 is
    ``AiReviewService``, which atomically CAS-fans-out matching pending
    reviews (same UPDATE shape as the §3.4 Layer 2 scanner).

    Attributes:
        user_public_id: Owner of the AI_DELEGATE user row.
        delegate_public_id: ``ai_delegates.public_id`` whose WS dropped.
            Stable across reconnects for the same delegate user; used
            by subscribers to look up affected ``ai_reviews`` rows by
            ``selected_delegate_public_id``.
        last_seen_at: Wall-clock at which the delayed publish fires
            (= disconnect time + grace). Subscribers compare this to
            ``ai_reviews.fanout_after`` to decide whether to dispatch
            the fanout immediately or wait for the natural fanout
            timer.
    """

    type: Literal["delegate_offline"] = "delegate_offline"
    user_public_id: str
    delegate_public_id: str
    last_seen_at: datetime


class ScopeRevokedData(StrictDataSchema[Literal["scope_revoked"]]):
    """Admin scope-revocation event for a wallet-operator scope grant.

    Published by ``ScopeGrantService.revoke_grant`` as the SOLE publisher
    of the ``admin.scope_revoked`` bus topic (single-publisher rule).
    Subscriber: ``WebSocketAuthManager`` closes or narrows affected
    AI_DELEGATE subscriptions via mid-session revalidation without
    dropping the WS connection itself.

    The payload carries the full scope identity (grant, operator,
    wallet, scope_kind, resource ids) so log readers and audit
    consumers have enough context without a follow-up DB round-trip.
    The subscriber, however, ignores the resource ids for revalidation
    logic and re-runs ``list_scope_grant_instrument_pairs`` against
    the post-revocation DB snapshot — the event is a wake-up signal,
    not the authoritative new state.

    Attributes:
        grant_public_id: Closed grant identity (SCD2 row at the moment
            of revocation).
        operator_public_id: Operator that lost the scope.
        wallet_public_id: Wallet on which the scope was revoked.
        scope_kind: ``"underlying"`` or ``"instrument"``.
        underlying_public_id: Populated when ``scope_kind='underlying'``.
        instrument_public_id: Populated when ``scope_kind='instrument'``.
        revoked_at: UTC timestamp of the SCD2 close bus-time.
        revoked_by_user_public_id: ADMIN user who initiated the revoke.
        reason: Optional admin-supplied rationale.
    """

    type: Literal["scope_revoked"] = "scope_revoked"
    grant_public_id: str
    operator_public_id: str
    wallet_public_id: str
    scope_kind: Literal["underlying", "instrument"]
    underlying_public_id: str | None = None
    instrument_public_id: str | None = None
    revoked_at: datetime
    revoked_by_user_public_id: str | None = None
    reason: str | None = None


class SymbolAliasUpdateData(StrictDataSchema[Literal["symbol_alias_update"]]):
    """Symbol alias cache invalidation message.

    Published when symbol aliases are updated in the database.
    Subscribers should clear their symbol mapper caches.

    Attributes:
        event: Event type (always 'symbol_aliases_updated').
        action: Required action (always 'clear_cache').
    """

    type: Literal["symbol_alias_update"] = "symbol_alias_update"
    event: Literal["symbol_aliases_updated"] = "symbol_aliases_updated"
    action: Literal["clear_cache"] = "clear_cache"


class ReplayStartData(StrictDataSchema[Literal["replay_start"]]):
    """Historical data replay start marker.

    Sent at the beginning of a historical data replay session.
    Strategies use this to reset state before receiving replayed data.

    Attributes:
        started_at: Replay start timestamp (optional).
    """

    type: Literal["replay_start"] = "replay_start"
    started_at: datetime | None = None


class ReplayEndData(StrictDataSchema[Literal["replay_end"]]):
    """Historical data replay end marker.

    Sent at the end of a historical data replay session.
    Strategies use this to finalize analysis and generate reports.
    """

    type: Literal["replay_end"] = "replay_end"


class UnderlyingAssetData(StrictDataSchema[Literal["underlying_asset"]]):
    """Underlying asset with instrument count.

    Provenance fields (public_id, session_id, sequence_id, timestamp)
    come from the DB row via UnderlyingAssetRow.

    Attributes:
        ticker: Short code (e.g. 'SPX', 'GOLD').
        name: Canonical name (e.g. 'S&P 500').
        asset_class: Asset type category.
        sector: Optional sector classification.
        instrument_count: Number of instruments mapped to this underlying.
    """

    type: Literal["underlying_asset"] = "underlying_asset"
    ticker: str
    name: str
    asset_class: str
    sector: str | None
    instrument_count: int


class UnderlyingInstrumentData(StrictDataSchema[Literal["underlying_instrument"]]):
    """Instrument mapped to an underlying asset.

    Provenance comes from the InstrumentUnderlyingMapping DB row.

    Attributes:
        instrument_public_id: Public ID of the instrument.
        native_symbol: Symbol as known on the exchange.
        exchange: Exchange identifier.
        asset_type: Asset type of the symbol.
        relationship_type: How instrument relates to underlying.
        contract_family: Futures product root (nullable).
    """

    type: Literal["underlying_instrument"] = "underlying_instrument"
    instrument_public_id: str
    native_symbol: str
    exchange: str
    asset_type: str
    relationship_type: str
    contract_family: str | None


class InstrumentDetailData(StrictDataSchema[Literal["instrument_detail"]]):
    """Capability-aware instrument projection for REST responses.

    Joins Symbol + SymbolExchangeCapability + Instrument + InstrumentSpec
    so the frontend can render the market-data-only badge + disable
    submit buttons without a separate round-trip for capability lookup.

    Attributes:
        instrument_public_id: Public ID of the Instrument row when an
            active Instrument exists at the query snapshot. When the
            Symbol + capability rows are present but no Instrument row
            has been synced yet (transient state during symbol-updater
            runs), this field falls back to ``symbol_public_id`` so the
            frontend still has a stable identifier for dropdown keys.
            ``instrument_resolved=True`` marks the former case; callers
            that need to persist an order-entry reference MUST gate on
            that flag and re-resolve via
            ``Repository.get_instrument_public_id_by_symbol`` at submit
            time (the REST order-entry path already does this).
        symbol_public_id: Public ID of the Symbol row
            (same symbol can map to many instruments across exchanges).
        symbol: Native symbol string (e.g. ``MNQM6-CME``).
        exchange: Exchange identifier.
        can_trade: Value of ``SymbolExchangeCapability.can_trade`` — False
            means market-data only (frontend renders a "Market-data only"
            badge and disables order submit).
        can_market_data: Value of ``SymbolExchangeCapability.can_market_data``.
        instrument_resolved: ``True`` when ``instrument_public_id`` is a
            real ``Instrument.public_id``; ``False`` when it is the
            fallback ``Symbol.public_id``. Consumers that persist the
            instrument reference (order submission, cap tracking,
            pricing) MUST reject rows where ``instrument_resolved=False``
            and re-resolve via the authoritative symbol → instrument
            resolver, since these identifiers cross different namespaces.
        instrument_kind: InstrumentSpec kind label (``future``, ``spot``,
            etc.); ``None`` when no spec row exists.
        expiry_at: InstrumentSpec expiry timestamp; ``None`` for perpetuals
            + assets without a scheduled expiry.
    """

    type: Literal["instrument_detail"] = "instrument_detail"
    instrument_public_id: str
    symbol_public_id: str
    symbol: str
    exchange: str
    can_trade: bool
    can_market_data: bool
    instrument_resolved: bool
    instrument_kind: str | None
    expiry_at: datetime | None


class FrontMonthData(StrictDataSchema[Literal["front_month"]]):
    """Front-month futures contract for an underlying.

    Provenance is minted by the API handler (projection across
    multiple temporal tables, not a single DB row).

    Attributes:
        instrument_public_id: Public ID of the front-month instrument.
        native_symbol: Symbol as known on the exchange.
        exchange: Exchange identifier.
        expiry_at: Contract expiry timestamp (UTC).
        relationship_type: How instrument relates to underlying.
        contract_family: Futures product root (nullable).
    """

    type: Literal["front_month"] = "front_month"
    instrument_public_id: str
    native_symbol: str
    exchange: str
    expiry_at: datetime
    relationship_type: str
    contract_family: str | None


class ContractData(StrictDataSchema[Literal["contract"]]):
    """Futures contract in a contract ladder listing.

    Provenance is minted per item (same pattern as FrontMonthData).

    Attributes:
        instrument_public_id: Public ID of the instrument.
        native_symbol: Symbol as known on the exchange.
        exchange: Exchange identifier.
        expiry_at: Contract expiry timestamp (nullable for perpetuals).
        instrument_kind: Product type (future, perpetual, etc.).
        relationship_type: How instrument relates to underlying.
        contract_family: Futures product root (nullable).
        is_front_month: True if this is the nearest non-expired contract.
    """

    type: Literal["contract"] = "contract"
    instrument_public_id: str
    native_symbol: str
    exchange: str
    expiry_at: datetime | None
    instrument_kind: str | None
    relationship_type: str
    contract_family: str | None
    is_front_month: bool


class ContinuousCandleData(StrictDataSchema[Literal["continuous_candle"]]):
    """Candle from a stitched continuous contract series.

    Provenance is minted by the API handler (on-demand computation,
    not a single DB row). open_at is domain time (interval start).

    Attributes:
        open_at: Candle interval start time.
        timeframe: Candle timeframe (e.g., "1h", "1d").
        open: Adjusted open price.
        high: Adjusted high price.
        low: Adjusted low price.
        close: Adjusted close price.
        volume: Raw volume (not adjusted).
        vwap: Adjusted VWAP (nullable).
        trades: Raw trade count (not adjusted, nullable).
        source_contract: Native symbol of the contract this bar came from.
        adjustment_factor: Cumulative adjustment applied (None for anchor).
    """

    type: Literal["continuous_candle"] = "continuous_candle"
    open_at: datetime
    timeframe: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: float | None
    trades: int | None
    source_contract: str
    adjustment_factor: float | None


class RollPointDetail(StrictBody):
    """Roll point detail for partial failure response."""

    from_contract: str
    to_contract: str
    roll_at: str


class ContinuousSeriesPartialResponse(StrictDataSchema[Literal["continuous_partial"]]):
    """Partial continuous series response when a roll gap is too large.

    Returned with HTTP 200 so consumers receive usable data up to the
    failure point. The failed_roll field signals truncation.
    """

    type: Literal["continuous_partial"] = "continuous_partial"
    payload: list[ContinuousCandleData]
    count: int
    failed_roll: RollPointDetail
    message: str


class FundingAccrualData(StrictDataSchema[Literal["funding_accrual"]]):
    """Funding/rollover accrual event published to ZMQ for UI observability.

    Emitted by the trader's funding accrual loop after a charge is
    persisted to the AccrualLedger and applied to the in-memory
    TradeService and PortfolioTracker state.

    Attributes:
        instrument: Native symbol of the charged instrument.
        exchange: Exchange where the position is held.
        mode: Execution mode (live, paper).
        accrual_type: Kind of periodic charge applied.
        accrued_at: Boundary timestamp the charge covers.
        amount: Signed charge in the notional asset.
        amount_asset: Currency of the charge (e.g., USD).
        rate: Per-boundary rate used for the charge.
        notional: Absolute notional value of the position at charge time.
        position_quantity: Signed position size at charge time.
    """

    type: Literal["funding_accrual"] = "funding_accrual"
    instrument: str
    exchange: OrderExchange
    mode: ExecutionMode
    accrual_type: Literal["funding", "rollover", "borrow"]
    accrued_at: datetime
    amount: float
    amount_asset: str
    rate: float
    notional: float
    position_quantity: float


class InstrumentCapabilityData(StrictDataSchema[Literal["instrument_capability"]]):
    """Instrument order capability matrix row for REST API responses.

    Describes which order types, features, and limits are available
    for a given instrument on a given exchange.

    Attributes:
        instrument_public_id: UUID of the instrument.
        exchange: Exchange identifier.
        supported_order_types: List of supported order type strings.
        supports_post_only: Whether post-only orders are supported.
        supports_reduce_only: Whether reduce-only orders are supported.
        supports_amend_in_place: Whether in-place order amendment is supported.
        supports_native_stop_loss: Whether the exchange supports native SL.
        supports_native_take_profit: Whether the exchange supports native TP.
        supports_trailing_stop_client_side: Whether client-side trailing stop is viable.
        supports_market_making: Whether MM strategy is viable.
        supports_short_selling: Whether short selling is supported.
        supports_leverage: Whether leverage is supported.
        max_leverage_long: Maximum leverage for long positions.
        max_leverage_short: Maximum leverage for short positions.
        min_notional: Minimum notional order value.
        max_order_size: Maximum order size.
        top_of_book_quality: Book data quality hint for pegs/MM.
    """

    type: Literal["instrument_capability"] = "instrument_capability"
    instrument_public_id: str
    exchange: str
    supported_order_types: list[str]
    supports_post_only: bool
    supports_reduce_only: bool
    supports_amend_in_place: bool
    supports_native_stop_loss: bool
    supports_native_take_profit: bool
    supports_trailing_stop_client_side: bool
    supports_market_making: bool
    supports_short_selling: bool
    supports_leverage: bool
    max_leverage_long: float
    max_leverage_short: float
    min_notional: float | None
    max_order_size: float | None
    top_of_book_quality: str


class VenueFeeScheduleData(StrictDataSchema[Literal["venue_fee_schedule"]]):
    """Venue fee schedule row for REST API responses.

    Used by market-making evaluators to estimate profitability and
    by the UI to display fee tiers.

    Attributes:
        exchange: Exchange identifier.
        instrument_public_id: Instrument UUID (null = exchange-wide default).
        fee_tier: Fee tier name.
        maker_bps: Maker fee in basis points (negative = rebate).
        taker_bps: Taker fee in basis points.
        min_volume_30d: Minimum 30-day volume for this tier.
        currency: Fee denomination currency.
    """

    type: Literal["venue_fee_schedule"] = "venue_fee_schedule"
    exchange: str
    instrument_public_id: str | None
    fee_tier: str
    maker_bps: float
    taker_bps: float
    min_volume_30d: float | None
    currency: str


class ExecutionPlanData(StrictDataSchema[Literal["execution_plan"]]):
    """Execution plan state for REST API responses.

    Attributes:
        plan_type: Plan type discriminator.
        status: Current plan lifecycle status.
        instrument_public_id: Target instrument UUID.
        exchange: Target exchange.
        mode: Execution mode (live/paper).
        side: Order side (buy/sell).
        total_quantity: Total intended quantity.
        filled_quantity: Quantity filled so far.
        created_at: Plan creation timestamp.
        created_via: Creation channel (ui/api/cli/strategy).
        wallet_public_id: Owning wallet UUID.
        operator_public_id: Operator identity (nullable).
        params: Plan-type-specific parameters.
        last_error: Most recent error message (nullable).
        idempotency_key: Client-provided dedup key (nullable).
    """

    type: Literal["execution_plan"] = "execution_plan"
    plan_type: str
    status: str
    instrument_public_id: str
    exchange: str
    mode: str
    side: str
    total_quantity: float
    filled_quantity: float
    created_at: datetime
    created_via: str
    wallet_public_id: str
    operator_public_id: str | None
    params: dict[str, object]
    position_cycle_public_id: str | None
    parent_plan_public_id: str | None
    last_error: str | None
    idempotency_key: str | None


BacktestProgressEvent = Literal[
    "started", "progress", "milestone", "completed", "failed", "cancelled"
]
"""Canonical single source of truth for backtest progress event names.

Consumed by the topic validator
(``snapper.messaging.topics.validation._validate_backtest_topic`` —
which derives ``BACKTEST_EVENTS = frozenset(typing.get_args(...))``
from this Literal so the enum and the topic layer cannot drift), the
``BacktestProgressData.event`` payload field (below), the runner
emitter, and the frontend WS handlers. Milestone is NOT an event
value but a separate field on the payload with a ``@model_validator``
enforcing ``milestone is not None iff event == 'milestone'``.
"""


BacktestProgressMilestone = Literal["25pct", "50pct", "75pct"]
"""25/50/75 percent milestone buckets for backtest progress."""


class BacktestProgressData(StrictDataSchema[Literal["backtest_progress"]]):
    """Live backtest progress payload published to ZMQ + forwarded to WS.

    Published on topic ``backtest.{wallet_public_id}.{run_public_id}.{event}``
    where ``event`` matches ``BacktestProgressEvent``. The bridge
    forwards the envelope verbatim to every WS client subscribed to the
    wallet + run prefix the topic falls under.
    Cross-field invariant: ``milestone`` is
    non-null if-and-only-if ``event == "milestone"``. Without this
    guard, a malformed payload ``(event="progress", milestone="25pct")``
    would parse cleanly and the frontend milestone chip would fire on
    every throttled progress tick. The validator closes both
    directions.

    Attributes:
        type: Payload discriminator (``backtest_progress``).
        run_public_id: UUID7 of the backtest run.
        wallet_public_id: UUID7 of the owning wallet (for topic scope).
        event: Progress event enum.
        milestone: 25pct/50pct/75pct bucket (only on milestone events).
        candles_done: Candles processed so far.
        total_candles: Expected total (None when count query failed).
        signals_count: Cumulative signals generated.
        trades_count: Cumulative trades simulated.
        equity: Current portfolio equity.
        progress_pct: candles_done / total_candles (0.0 if total is None).
    """

    type: Literal["backtest_progress"] = "backtest_progress"
    run_public_id: str
    wallet_public_id: str
    event: BacktestProgressEvent
    milestone: BacktestProgressMilestone | None = None
    candles_done: int
    total_candles: int | None
    signals_count: int
    trades_count: int
    equity: float
    progress_pct: float

    @model_validator(mode="after")
    def _milestone_requires_event(self) -> Self:
        """Cross-field invariant: ``milestone`` non-null iff event == 'milestone'.

        Closes both directions so neither a milestone event without a
        bucket nor a non-milestone event carrying a bucket slips past
        Pydantic into the bridge.
        """
        if self.event == "milestone" and self.milestone is None:
            raise ValueError("milestone event requires a milestone bucket value")
        if self.event != "milestone" and self.milestone is not None:
            raise ValueError("milestone field must be None when event != 'milestone'")
        return self


AlertType = Literal[
    "order_fill_full",
    "order_rejected",
    "position_stop_loss_fired",
    "margin_warning",
    "critical_system_error",
]
"""Canonical alert type enumeration for iOS Push Foundation Plan 2 §D4.

Mirrored by:
- ``DeviceAlertPrefBody.alert_type`` (wire schema Literal)
- ``_ALERT_TYPES`` in ``snapper.messaging.topics.validation`` (topic validator)
- the iOS-side ``AlertType`` enum in generated Swift types

Any change to this list MUST update all three sites together — the
topic validator will reject any alerts.*.<unknown_type> at publish
time and ``make check-all`` will fail.
"""


AlertPriority = Literal["low", "medium", "high"]
"""Delivery priority tier. Maps to APNs ``apns-priority`` headers:

- ``low``  -> 5 (throttleable)
- ``medium`` -> 5 (throttleable, default)
- ``high`` -> 10 (immediate)

``is_safety_critical=True`` alerts bypass user-pref gating regardless
of priority (§D4).
"""


class AlertEventData(StrictDataSchema[Literal["alert_event"]]):
    """ZMQ bus payload published on ``alerts.{user_public_id}.{alert_type}``.

    The sidecar (``snapper notify``, BE-3a) subscribes to the
    ``alerts.`` prefix via ``ValidatedSubscriber.subscribe("alerts.")``
    and dispatches into the APNs outbox per Plan 2 §D5. Producers are
    domain services (trader / portfolio / system health) that call
    ``build_alert_event()`` to mint a valid payload with topic string
    + provenance stamped from a ``SequenceTracker``.

    Scope fields (``operator_public_id`` / ``wallet_public_id``) are
    denormalised at emit time from the source event so the delivery
    outbox row gets a race-free view of the scope (closes Copilot R2
    MAJOR-2 on SCD2-join scope races).

    Attributes:
        type: Payload discriminator (always ``alert_event``).
        user_public_id: Recipient user UUID7 — must equal the topic's
            segment-2 value (cross-checked by the sidecar).
        operator_public_id: Optional operator scope of the event.
        wallet_public_id: Optional wallet scope of the event.
        alert_type: One of the enumerated ``AlertType`` values.
        priority: Delivery priority (``low`` / ``medium`` / ``high``).
        is_safety_critical: When True, delivery bypasses user prefs
            (critical system / margin warnings).
        title: Short notification title (APNs ``aps.alert.title``).
        body: Localised body (APNs ``aps.alert.body``).
        payload: Optional structured context for the iOS client's
            notification-expand renderer.
        dedup_key: Optional idempotency key — sidecar collapses
            duplicates sharing the same key within a window.
        thread_key: Optional APNs ``aps.thread-id`` for iOS
            notification grouping.
        source_topic: Informational — the ZMQ topic the alert was
            published on; mirrored into the ``alert_events`` row for
            downstream diagnostics.
    """

    type: Literal["alert_event"] = "alert_event"
    user_public_id: str
    operator_public_id: str | None = None
    wallet_public_id: str | None = None
    alert_type: AlertType
    priority: AlertPriority = "medium"
    is_safety_critical: bool = False
    title: str = Field(min_length=1, max_length=160)
    body: str = Field(min_length=1, max_length=512)
    payload: JsonObject | None = None
    dedup_key: str | None = Field(default=None, max_length=128)
    thread_key: str | None = Field(default=None, max_length=64)
    source_topic: str | None = None


class ExecutionPlanDecisionEventData(StrictDataSchema[Literal["execution_plan_decision_event"]]):
    """Published on ``plans.decisions.{plan_public_id}`` right after insert (§D6.2).

    Emitted by ``PlanExecutorService`` immediately after every
    ``ExecutionPlanDecision`` row commits. Minimal shape — carries
    only fields available on ``ExecutionPlanDecisionInsertRow`` + the
    returned ``decision_public_id``. Consumers (notify sidecar's
    stop-loss rule) enrich via repository reads.

    ``reason`` is free-form ``str`` (not a Literal) because
    ``_log_decision`` writes varied strings like
    ``"evaluator emitted command"`` or
    ``"Cycle <x> closed before command dispatch"`` alongside the
    well-known ``"sl_hit"`` / ``"tp_hit"`` / ``"trailing_stop_hit"``
    values. The stop-loss rule filters on known-loss reasons at
    evaluation time rather than at the schema layer (Plan v1.12
    R10.B-3 closure).

    Attributes:
        type: Payload discriminator (always
            ``execution_plan_decision_event``).
        decision_public_id: The row's stable UUID7 returned by
            ``insert_execution_plan_decision``.
        plan_public_id: Parent ``ExecutionPlan`` UUID7 — mirrored
            into the topic segment so subscribers can filter at the
            ZMQ layer without parsing the payload (Plan v1.12 R10.B-2
            closure: real field name is ``plan_public_id``, not
            ``execution_plan_public_id``).
        decision_type: Free-form discriminator —
            ``"evaluator"`` / ``"lifecycle"`` / similar. Preserved
            verbatim from ``ExecutionPlanDecisionInsertRow``.
        trigger_type: Free-form trigger discriminator —
            ``"tick"`` / ``"clock"`` / ``"execution"`` / etc.
        reason: Free-form diagnostic string. Well-known values
            (``"sl_hit"`` / ``"trailing_stop_hit"``) are the trigger
            set for the stop-loss rule.
        triggered_at: Bus-time the decision was logged.
    """

    type: Literal["execution_plan_decision_event"] = "execution_plan_decision_event"
    decision_public_id: str
    plan_public_id: str
    decision_type: str
    trigger_type: str
    reason: str
    triggered_at: datetime
