"""Exchange contracts and data transfer objects.

Defines common data structures for orders, executions, and market data
shared across all exchange implementations.
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from enum import StrEnum
from typing import Literal

from snapper.core.types import FillStatus
from snapper.core.types import FillStatusEnum


class OrderSideEnum(Enum):
    """Enumeration of order sides (buy/sell)."""

    BUY = "buy"
    SELL = "sell"


class ExchangeOrderTypeEnum(StrEnum):
    """Exchange-wire-format order type.

    Covers SDK-reported order types across all supported exchanges
    (Kraken, Kraken Futures, Walutomat, Polygon). Distinct
    from ``snapper.core.types.OrderTypeEnum`` (domain trading-core
    type) by design — exchange values include ICEBERG, STOP_LOSS_LIMIT,
    TAKE_PROFIT_LIMIT, TRAILING_STOP_LIMIT, SETTLE_POSITION that the
    trading core never reasons about.
    """

    LIMIT = "limit"
    MARKET = "market"
    ICEBERG = "iceberg"
    STOP_LOSS = "stop-loss"
    STOP_LOSS_LIMIT = "stop-loss-limit"
    TAKE_PROFIT = "take-profit"
    TAKE_PROFIT_LIMIT = "take-profit-limit"
    TRAILING_STOP = "trailing-stop"
    TRAILING_STOP_LIMIT = "trailing-stop-limit"
    SETTLE_POSITION = "settle-position"


CORE_TO_EXCHANGE_ORDER_TYPE: dict[str, ExchangeOrderTypeEnum] = {
    "market": ExchangeOrderTypeEnum.MARKET,
    "limit": ExchangeOrderTypeEnum.LIMIT,
    "stop": ExchangeOrderTypeEnum.STOP_LOSS,
    "stop_limit": ExchangeOrderTypeEnum.STOP_LOSS_LIMIT,
}
"""Core domain order types -> exchange wire types (#156).

The two vocabularies coincide ONLY for market/limit;
``ExchangeOrderTypeEnum('stop')`` raises, so every venue-boundary
translation must go through this map. An order type absent here is a
DEFINITIVE executor-side rejection, never a bare ValueError.
"""

EXCHANGE_TO_CORE_ORDER_TYPE: dict[str, str] = {
    ExchangeOrderTypeEnum.MARKET.value: "market",
    ExchangeOrderTypeEnum.LIMIT.value: "limit",
    ExchangeOrderTypeEnum.STOP_LOSS.value: "stop",
    ExchangeOrderTypeEnum.STOP_LOSS_LIMIT.value: "stop_limit",
}
"""Exchange wire order types -> core domain types (#156).

Used at the durable boundaries that historically leaked wire vocabulary:
the ``orders`` table persistence (venue snapshots carry wire types) and
the legacy ``venue_order_type`` plan-param fallback. Unmapped wire types
(iceberg, take-profit, ...) pass through unchanged — they never enter
core flows.
"""


class ExchangeOrderStatusEnum(StrEnum):
    """Exchange-wire-format order status.

    Covers SDK-reported order statuses across all supported exchanges.
    Distinct from ``snapper.core.types.OrderStatusEnum`` (domain
    trading-core status) by design — exchange values use American
    spelling (``CANCELED``) and include ``PENDING``, ``CLOSED``,
    ``PENDING_NEW``, ``EXPIRED`` that the trading core reduces to its
    7-state lifecycle. The cross-spelling mapping between domain
    ``CANCELLED`` and exchange ``CANCELED`` is handled in
    ``implementations/kraken.py`` and ``adapters/kraken_futures.py``.
    """

    PENDING = "pending"
    OPEN = "open"
    CLOSED = "closed"
    PENDING_NEW = "pending_new"
    NEW = "new"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELED = "canceled"
    EXPIRED = "expired"


class TimeInForceEnum(Enum):
    """Enumeration of time-in-force policies."""

    GTC = "GTC"
    GTD = "GTD"
    IOC = "IOC"


def to_fill_status(execution: ExecutionUpdate) -> FillStatus:
    """Derive FillStatus from an ExecutionUpdate.

    Single source of truth for mapping exchange-level order status to
    the domain FillStatus used in ExecutionData and the DB executions table.

    Args:
        execution: Execution update from exchange WebSocket or REST.

    Returns:
        'partial' when order is still open with partial fills,
        'filled' otherwise (closed, explicitly filled, or default).
    """
    if execution.order_status == ExchangeOrderStatusEnum.OPEN and (execution.cum_qty or 0) > 0:
        return FillStatusEnum.PARTIAL
    return FillStatusEnum.FILLED


def normalize_order_status(status: ExchangeOrderStatusEnum) -> ExchangeOrderStatusEnum:
    normalization_map = {
        ExchangeOrderStatusEnum.PENDING: ExchangeOrderStatusEnum.OPEN,
        ExchangeOrderStatusEnum.PENDING_NEW: ExchangeOrderStatusEnum.OPEN,
        ExchangeOrderStatusEnum.NEW: ExchangeOrderStatusEnum.OPEN,
        ExchangeOrderStatusEnum.PARTIALLY_FILLED: ExchangeOrderStatusEnum.OPEN,
        ExchangeOrderStatusEnum.FILLED: ExchangeOrderStatusEnum.CLOSED,
        ExchangeOrderStatusEnum.OPEN: ExchangeOrderStatusEnum.OPEN,
        ExchangeOrderStatusEnum.CLOSED: ExchangeOrderStatusEnum.CLOSED,
        ExchangeOrderStatusEnum.CANCELED: ExchangeOrderStatusEnum.CANCELED,
        ExchangeOrderStatusEnum.EXPIRED: ExchangeOrderStatusEnum.EXPIRED,
    }
    return normalization_map.get(status, status)


_TERMINAL_ORDER_STATUSES: frozenset[ExchangeOrderStatusEnum] = frozenset(
    {
        ExchangeOrderStatusEnum.CLOSED,
        ExchangeOrderStatusEnum.CANCELED,
        ExchangeOrderStatusEnum.EXPIRED,
    }
)


def order_status_is_terminal(status: ExchangeOrderStatusEnum) -> bool:
    """Return whether a normalized order status is terminal.

    Args:
        status: A normalized :class:`ExchangeOrderStatusEnum`.

    Returns:
        Whether the status is closed, canceled, or expired.
    """
    return status in _TERMINAL_ORDER_STATUSES


def is_lifecycle_only(execution: ExecutionUpdate) -> bool:
    """Return whether a frame is a quantity-less, non-terminal lifecycle ack.

    A venue ``new``/``pending_new``/``status`` frame for a freshly placed
    RESTING order carries no traded quantity — its zero cumulative is coerced
    to ``None`` by the adapter's ``_optional_float`` — and a non-terminal
    normalized status (``NEW``/``PENDING_NEW`` map to ``OPEN`` via
    :func:`normalize_order_status`). Such a frame confirms the order is resting
    on the venue; it is NOT a fill and must not be booked or PUBLISHED as one:
    :func:`to_fill_status` would default it to ``FILLED``, and even a
    zero-delta publish carries a lifecycle-terminal ``FILLED`` status that
    releases the engine's in-flight guard and retires the command. A frame that
    carries any cumulative or last quantity, or whose normalized status is
    terminal (a legitimate zero-delta terminal publish), is not lifecycle-only.

    Args:
        execution: Execution update from an exchange WebSocket or REST read.

    Returns:
        Whether the frame is a resting-order acknowledgement rather than a fill.
    """
    if execution.cum_qty is not None or execution.last_qty is not None:
        return False
    return not order_status_is_terminal(execution.order_status)


@dataclass
class TickerSnapshot:
    """Snapshot of ticker data for a trading symbol.

    ``last`` carries the venue's best available estimate of the executable
    price at that instant, from the evidence that venue actually delivers:
    the venue's own trade print where the feed delivers prints; the midpoint
    of the venue's two-sided top-of-book quote where the feed delivers a book
    and no usable print. It is never an externally-sourced reference rate, an
    index, a mark price, or a previous-session close. Where a venue publishes
    a reference or index alongside the book it belongs in its own named field
    and never in ``last`` — the established shape is
    ``KrakenFuturesTickerSchema.mark_price`` / ``index_price``, which the
    adapter deliberately does not route into ``last``.
    """

    symbol: str
    bid: float
    ask: float
    last: float
    timestamp: float


@dataclass
class OhlcvSnapshot:
    """Snapshot of OHLCV (Open-High-Low-Close-Volume) candle data."""

    timestamp: float
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass
class AccountBalance:
    """Account balance for a single currency."""

    currency: str
    free: float
    used: float
    total: float


class CapabilityStatus(StrEnum):
    """Structural account-reading capability of an exchange client (PnL Phase 3).

    Declares — WITHOUT connecting — whether a client can faithfully report a
    given account component. It is the fail-closed gate the account observer
    consults before ever calling a reader, so a venue that cannot be
    account-tracked is never mistaken for one that returned empty data.

    - ``SUPPORTED``: the client reads faithful native data for this component.
    - ``SIMULATED``: the client returns a modeled fiction (paper) — recorded as
      ``simulated``, never ``observed``.
    - ``NOT_APPLICABLE``: the component does not exist for this venue (e.g. spot
      or FX has no derivatives positions) — a benign structural absence.
    - ``UNSUPPORTED``: the venue cannot be account-tracked for this component
      (market-data-only) — the fail-closed default.
    """

    SUPPORTED = "supported"
    SIMULATED = "simulated"
    NOT_APPLICABLE = "not_applicable"
    UNSUPPORTED = "unsupported"


@dataclass
class NativeBalanceEntry:
    """One faithful native per-currency balance reading (PnL Phase 3).

    Distinct from ``AccountBalance``: ``free`` and ``used`` are NULLABLE because
    some venues (Kraken Futures coin-margin) expose only a per-currency total
    and an account-level aggregate margin, never a faithful per-currency
    free/used split. The account observer stores exactly what the venue
    reported — a null free/used is honest "unknown", never a fabricated split.
    """

    currency: str
    total: float
    free: float | None
    used: float | None
    total_decimal: str | None = None
    free_decimal: str | None = None
    used_decimal: str | None = None
    numeric_provenance: str = "legacy_float"


@dataclass
class ExchangeOrderRequest:
    """Request parameters for placing an order on an exchange.

    ``client_order_id`` is REQUIRED and non-empty, enforced here rather
    than per-venue, because it is the correlation identity every venue
    adapter must put on the wire byte-identically. It is the only
    identity that survives a process death, and the sole key both
    recovery paths query with: ``_verify_ambiguous_submit`` looks the
    order up by it after an ambiguous submit, and the cross-restart
    dispatched sweep looks it up by the same value read off
    ``trade_commands.client_order_id``. An adapter that substitutes a
    fabricated id, or omits it, makes the venue hold the order under an
    identity nothing can query — and a venue lookup that answers "not
    found" is contractually an authoritative statement of ABSENCE, which
    converts a live, possibly-filling order into a false REJECTED.

    Requiredness and the ``__post_init__`` guard are complementary, not
    redundant: the field forbids OMITTING the id (caught statically by
    mypy across ``src``, ``tests`` and ``scripts``), the guard forbids
    the empty string (a runtime value the type system admits).
    ``""`` is not harmless — ``ccxt.safe_string`` drops an empty string
    from the wire params exactly as it drops ``None``, so without the
    guard an empty id would reproduce the omission defect in full.

    Note the asymmetry with ``ExchangeOrderSnapshot.client_order_id``,
    which stays optional: that field holds the venue's ECHO, which can
    legitimately be absent. This one is what we SEND, and it never can.

    Attributes:
        symbol: Native instrument symbol of the order.
        side: Buy or sell.
        type: Wire order type.
        amount: Order quantity in base units.
        client_order_id: Correlation identity of the order; must equal
            ``trade_commands.client_order_id`` byte-for-byte.
        price: Limit price, or None for market orders.
        stop_price: Trigger price for stop-typed orders.
        signaled_at: Strategy signal timestamp, for latency accounting.
        leverage: Requested leverage, when the venue supports it.
        reduce_only: Whether the order may only reduce a position.
        post_only: Whether the order must not take liquidity.
        wallet_public_id: Owning wallet, for attribution.
        operator_public_id: Operator who authored the order, if manual.
    """

    symbol: str
    side: OrderSideEnum
    type: ExchangeOrderTypeEnum
    amount: float
    client_order_id: str
    price: float | None = None
    stop_price: float | None = None
    signaled_at: datetime | None = None
    leverage: int | None = None
    reduce_only: bool = False
    post_only: bool = False
    wallet_public_id: str = ""
    operator_public_id: str | None = None

    def __post_init__(self) -> None:
        """Refuse an order that cannot be correlated back to its command.

        Tests falsiness, not ``is None``: mypy already forbids ``None``,
        so the value this guard exists to reject is ``""``.

        Raises:
            ValueError: If ``client_order_id`` is empty. Raised at
                construction, which on the executor path is strictly
                BEFORE any network send, so the failure is
                provably-not-placed.
        """
        if not self.client_order_id:
            raise ValueError(
                f"ExchangeOrderRequest for {self.symbol} has an empty client_order_id: "
                f"this id is the only identity that survives a restart and the sole key "
                f"ambiguous-submit verification and dispatched-order recovery query with. "
                f"Refusing to submit an order that could never be correlated back."
            )


type ExecType = Literal[
    "pending_new",
    "new",
    "trade",
    "filled",
    "iceberg_refill",
    "canceled",
    "expired",
    "amended",
    "restated",
    "status",
]
type LiquidityIndicator = Literal["m", "t"]
__all__ = [
    "to_fill_status",
    "order_status_is_terminal",
    "is_lifecycle_only",
    "OrderSideEnum",
    "ExchangeOrderTypeEnum",
    "ExchangeOrderStatusEnum",
    "TimeInForceEnum",
    "TickerSnapshot",
    "OhlcvSnapshot",
    "AccountBalance",
    "ExchangeOrderRequest",
    "ExecType",
    "LiquidityIndicator",
    "ExchangeOrderSnapshot",
    "FundingRateSnapshot",
    "OpenPositionSnapshot",
    "TickerUpdate",
    "CandleUpdate",
    "TradeUpdate",
    "ExecutionFeeBreakdown",
    "ExecutionUpdate",
    "InstrumentPairDescriptor",
]


@dataclass
class ExchangeOrderSnapshot:
    """Snapshot of an order's current state on an exchange.

    ``counter_filled`` is the opposite-currency GROSS cumulative of the filled
    side (a walutomat BUY's ``soldAmount``, a SELL's ``boughtAmount``) — the
    counter-amount truth effective execution prices derive from under price
    improvement. ``None`` on venues that do not report it; ``filled`` semantics
    are untouched (the witness identity depends on them).

    ``amount_is_order_size`` distinguishes venue snapshots whose ``amount`` is
    the authoritative requested order quantity from synthesized observations
    that can know only a cumulative fill. Durable repair must preserve the
    original request size for the latter.
    """

    id: str
    client_order_id: str | None
    symbol: str
    side: OrderSideEnum
    type: ExchangeOrderTypeEnum
    amount: float
    price: float | None
    status: ExchangeOrderStatusEnum
    filled: float
    remaining: float
    timestamp: float
    fee: float | None = None
    fee_currency: str | None = None
    db_order_id: int | None = None
    db_order_public_id: str | None = None
    amount_decimal: str | None = None
    price_decimal: str | None = None
    filled_decimal: str | None = None
    fee_decimal: str | None = None
    counter_filled: float | None = None
    counter_filled_decimal: str | None = None
    amount_is_order_size: bool = True


@dataclass
class TickerUpdate:
    """Real-time ticker update with full market data.

    ``last`` carries the venue's best available estimate of the executable
    price at that instant, from the evidence that venue actually delivers:
    the venue's own trade print where the feed delivers prints; the midpoint
    of the venue's two-sided top-of-book quote where the feed delivers a book
    and no usable print. It is never an externally-sourced reference rate, an
    index, a mark price, or a previous-session close. Where a venue publishes
    a reference or index alongside the book it belongs in its own named field
    and never in ``last`` — the established shape is
    ``KrakenFuturesTickerSchema.mark_price`` / ``index_price``, which the
    adapter deliberately does not route into ``last``.

    ``vwap``, ``low`` and ``high`` are 24-hour aggregates. A venue that does
    not report them sets them to ``0.0``; an instantaneous price in a
    24h-extreme column is a fabrication under any convention.

    ``is_delayed`` marks feeds that deliver ticks with an exchange-mandated
    delay (Kraken FCM / TradFi index futures publishes ~10-minute-delayed
    prices to non-subscribed users). Strategies subscribing via ZMQ must
    gate on this flag before treating the price as current.

    ``is_extended_hours`` indicates the tick occurred during extended trading
    hours where applicable (TradFi equities overnight session); ``None``
    means the feed does not distinguish extended-hours ticks.
    """

    symbol: str
    bid: float
    bid_qty: float
    ask: float
    ask_qty: float
    last: float
    volume: float
    vwap: float
    low: float
    high: float
    change: float
    change_pct: float
    is_delayed: bool = False
    is_extended_hours: bool | None = None


@dataclass
class CandleUpdate:
    """Real-time candle update with OHLCV data.

    ``close`` (and the ``open``/``high``/``low`` of the same bar) carries the
    venue's best available estimate of the executable price, from the evidence
    that venue actually delivers: the venue's own trade prints where the feed
    delivers prints; the midpoint of the venue's two-sided top-of-book quote
    where the feed delivers a book and no usable print. It is never an
    externally-sourced reference rate, an index, a mark price, or a
    previous-session close, and a bar never mixes conventions across its four
    price columns — a high taken from asks and a low taken from bids is a
    spread envelope, not a price series.

    ``complete`` is the trustworthy-boundary flag for higher-TF synthesis: True
    for native frames and for synthesized bars whose window was seeded from the
    durable plane or opened at/after the aggregator live epoch. It marks
    boundary trust so the persistence layer can record provenance; it is not,
    by itself, a coverage claim about the plane the bar belongs to.

    Whether the 1m plane is DENSE is a separate, per-venue question, and the
    answer differs across this codebase. On Kraken spot in ``trade_built`` mode
    and on Kraken futures, with ``candle_minute_completion`` switched on, the
    publisher additionally emits a flat carried-close bar for every minute it
    witnessed the venue live and in which the instrument did not trade — so on
    those two venues, and only over the intersection of (minutes witnessed live
    edge to edge, symbols with a confirmed trade subscription, symbols with an
    in-session close), the plane is complete at every minute boundary. Outside
    that intersection — and on every other venue and every historical or replay
    path, where the 1m corpus stays inherently gappy — an absent minute means
    "not observed", never "no trades".

    The positive assertion of the opposite is a bar carrying
    ``source == "synthesized"`` with ``trades == 0`` and ``volume == 0.0``: the
    venue was live, the subscription was confirmed, and nobody traded. **All
    three conjuncts are required, and the ``source`` one is what makes the rule
    sound corpus-wide.** Walutomat emits every REAL 1m bar with ``volume=0.0``
    and ``trades=0`` — it reports a genuine tick-mean price and no size at all —
    so a reader testing only the volume and trade count would read the entire
    walutomat corpus as flat no-trade bars.
    """

    symbol: str
    open: float
    high: float
    low: float
    close: float
    vwap: float
    trades: int
    volume: float
    interval_begin: datetime
    interval: int
    complete: bool = True


@dataclass
class TradeUpdate:
    """Real-time trade update from the exchange."""

    symbol: str
    side: str
    quantity: float
    price: float
    ord_type: str
    timestamp: datetime
    trade_id: str | None = None


@dataclass
class ExecutionFeeBreakdown:
    """Fee breakdown for a single asset in an execution."""

    asset: str
    quantity: float
    quantity_decimal: str | None = None


@dataclass
class OrderFillSummary:
    """Venue-true aggregate over an order's own fills.

    Fill-gap reconciliation's price AND fee source for venues whose
    order snapshots lack them: ``vwap`` over the fills the venue
    returned, the ``covered_qty`` those fills span (a partial fills
    page must never price or fee the whole gap), and the summed
    ``fee_total`` in ``fee_currency`` when every fill reported a
    parseable single-currency fee (both None otherwise — an honest
    absence beats a partial sum).
    """

    vwap: float
    covered_qty: float
    fee_total: float | None = None
    fee_currency: str | None = None


@dataclass(frozen=True)
class VenueAccountHistoryItem:
    """One faithful entry from a venue's account-history ledger (S4c-3 anchor).

    Carries the raw-but-typed fields the spot-anchor bootstrap needs from a
    single ``account/history`` row, before any normalization. ``operation_type``
    is the venue's own event class (``MARKET_FX`` a balance-affecting FX leg,
    ``COMMISSION`` a fee leg); ``operation_amount`` is the signed exact-decimal
    delta for ``currency`` and ``balance_after`` the venue's own post-event
    balance. ``transaction_id`` is shared across the two currency legs of one
    fill (present on FX legs, absent on non-order rows), so the witness join
    groups a fill's legs by it; ``order_id`` correlates every leg of one order
    (from ``operationDetails``) and is absent on non-order rows. ``submit_id``
    is the caller-supplied placement identity echoed by Walutomat history.
    ``ordered_by`` attributes the event (an ``API/`` prefix marks our own key).
    """

    item_id: int
    operation_type: str
    operation_amount: Decimal
    balance_after: Decimal
    currency: str
    transaction_id: str | None
    ordered_by: str
    order_id: str | None
    submit_id: str | None = None
    correcting_entry: bool = False


@dataclass(frozen=True)
class VenueAccountHistoryTip:
    """A venue account-history tip: the newest item id and its first page.

    ``item_id`` is the highest (newest) history id at read time — the ``H0``
    cursor the anchor seals. ``items`` is the first page in DESCENDING id order
    (newest first), the window the bootstrap folds against the local ledger; it
    is empty only when the account has no history. ``reached_genesis`` is True
    when the page holds the account's ENTIRE history (fewer rows than the
    requested limit): the anchor records which claim strength it certifies —
    genesis-scoped (the whole ledger observed) or window-scoped (activity older
    than the page is absorbed into the sealed balances, unobserved).
    """

    item_id: int
    items: tuple[VenueAccountHistoryItem, ...]
    reached_genesis: bool


@dataclass(frozen=True)
class VenueOrderFillLegs:
    """The cumulative per-leg totals a venue reports for one order (S4c-3).

    The independent composition anchor for the witness join: ``bought_amount``
    and ``sold_amount`` are the order's exact cumulative filled legs (gross,
    denominated in ``bought_currency`` / ``sold_currency``), ``commission_amount``
    the summed fee in ``commission_currency``. The witness builder proves the
    per-fill ``account/history`` MARKET_FX legs sum exactly to these totals, so a
    dropped or extra history leg is caught before any witness map is produced.
    """

    order_id: str
    bought_amount: Decimal
    sold_amount: Decimal
    commission_amount: Decimal
    bought_currency: str
    sold_currency: str
    commission_currency: str
    buy_sell: str


@dataclass
class ExecutionUpdate:
    """Real-time execution update for an order."""

    order_id: str
    exec_type: ExecType | None
    symbol: str
    side: OrderSideEnum
    order_type: ExchangeOrderTypeEnum
    order_status: ExchangeOrderStatusEnum
    timestamp: datetime
    cum_qty: float | None = None
    cum_cost: float | None = None
    order_userref: int | None = None
    exec_id: str | None = None
    trade_id: int | None = None
    last_qty: float | None = None
    last_price: float | None = None
    liquidity_ind: LiquidityIndicator | None = None
    cost: float | None = None
    average_price: float | None = None
    fee_usd_equiv: float | None = None
    fees: list[ExecutionFeeBreakdown] | None = None
    cum_fee: float | None = None
    cum_fee_currency: str | None = None
    order_qty: float | None = None
    limit_price: float | None = None
    cash_order_qty: float | None = None
    cl_ord_id: str | None = None
    margin: bool | None = None
    margin_borrow: bool | None = None
    post_only: bool | None = None
    reduce_only: bool | None = None
    time_in_force: TimeInForceEnum | None = None
    reason: str | None = None
    last_qty_decimal: str | None = None
    last_price_decimal: str | None = None
    fee_usd_equiv_decimal: str | None = None
    cum_qty_decimal: str | None = None
    average_price_decimal: str | None = None
    cum_fee_decimal: str | None = None
    counter_amount_decimal: str | None = None

    def __post_init__(self) -> None:
        """Normalize the instance after initialization."""
        object.__setattr__(self, "order_status", normalize_order_status(self.order_status))


@dataclass
class InstrumentPairDescriptor:
    """Descriptor for a trading instrument pair with precision and limits."""

    symbol: str
    base: str
    quote: str
    status: str
    qty_precision: int
    qty_increment: float
    qty_min: float
    price_precision: int
    price_increment: float
    cost_precision: int
    cost_min: float
    marginable: bool
    has_index: bool
    margin_initial: float | None = None
    position_limit_long: int | None = None
    position_limit_short: int | None = None
    tick_size: float | None = None


@dataclass(frozen=True)
class FundingRateSnapshot:
    """Funding or rollover rate fetched from an exchange.

    Attributes:
        symbol: Native symbol (e.g., ``BTC-USD-PERP``).
        exchange: Exchange name (lowercase).
        rate_type: Funding model identifier (``perpetual_funding``
            or ``spot_margin_rollover``).
        direction: Position direction the rate applies to
            (``long``, ``short``, or ``both``).
        rate: Per-boundary rate as a decimal fraction.
        effective_from: Exchange-side timestamp when this rate
            became effective.
        notional_asset: Quote/settlement currency for the rate.
        source: Provenance tag (``exchange_api``, ``exchange_ws``,
            ``exchange_docs``).
    """

    symbol: str
    exchange: str
    rate_type: str
    direction: str
    rate: float
    effective_from: datetime
    notional_asset: str
    source: str


@dataclass
class OpenPositionSnapshot:
    """Snapshot of an open leveraged or margin position."""

    symbol: str
    side: OrderSideEnum
    size: float
    entry_price: float
    mark_price: float
    unrealized_pnl: float
    unrealized_funding: float
    timestamp: datetime
