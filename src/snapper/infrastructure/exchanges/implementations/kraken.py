"""Kraken cryptocurrency exchange client implementation.

This module provides KrakenExchangeClient, a full-featured client for
interacting with the Kraken exchange. It supports:

REST API Operations:
    - Market data: tickers, OHLCV candles
    - Order management: create, cancel, get orders
    - Account: balance inquiries

WebSocket Subscriptions:
    - Public: tickers, candles, trades, instruments
    - Private: executions (requires API credentials)

Features:
    - CCXT integration for standard operations
    - Native Kraken SDK for advanced features and fallbacks
    - Circuit breaker pattern for resilience
    - Automatic rate limit handling with exponential backoff
    - Symbol conversion between native, CCXT, and Kraken formats

The client maintains internal queues for each WebSocket channel to
decouple message reception from consumption, enabling async iteration
patterns for real-time data streaming.
"""

import asyncio
import contextlib
import inspect
import json
import threading
import time
from collections.abc import AsyncIterator
from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from time import monotonic
from typing import Any
from typing import Final
from typing import Literal
from typing import cast

import ccxt
import requests
from kraken.spot import SpotWSClient
from kraken.spot import Trade
from kraken.spot import User
from loguru import logger
from pydantic import ValidationError

from snapper.config.settings import get_settings
from snapper.core.json_types import JsonValue
from snapper.core.types import ExchangeEnum
from snapper.core.types import TradeSideEnum
from snapper.data.repository import Repository
from snapper.infrastructure.exchanges._subscription_health import SubscriptionHealthTracker
from snapper.infrastructure.exchanges._subscription_health import interval_to_label
from snapper.infrastructure.exchanges._subscription_health import label_to_interval
from snapper.infrastructure.exchanges._subscription_request import SubscriptionRequest
from snapper.infrastructure.exchanges._subscription_request import canonicalise_parameters
from snapper.infrastructure.exchanges._trade_candle_builder import TradeCandleBuilder
from snapper.infrastructure.exchanges._trade_candle_builder import enqueue_or_drop_oldest_candle
from snapper.infrastructure.exchanges.adapters.kraken import parse_kraken_candle_list
from snapper.infrastructure.exchanges.adapters.kraken import parse_kraken_execution_list
from snapper.infrastructure.exchanges.adapters.kraken import parse_kraken_instrument
from snapper.infrastructure.exchanges.adapters.kraken import parse_kraken_ticker_list
from snapper.infrastructure.exchanges.adapters.kraken import parse_kraken_trade_list
from snapper.infrastructure.exchanges.base import ExchangeClientBase
from snapper.infrastructure.exchanges.contracts import AccountBalance
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import ExchangeOrderRequest
from snapper.infrastructure.exchanges.contracts import ExchangeOrderSnapshot
from snapper.infrastructure.exchanges.contracts import ExchangeOrderStatusEnum
from snapper.infrastructure.exchanges.contracts import ExchangeOrderTypeEnum
from snapper.infrastructure.exchanges.contracts import ExecutionUpdate
from snapper.infrastructure.exchanges.contracts import InstrumentPairDescriptor
from snapper.infrastructure.exchanges.contracts import OhlcvSnapshot
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.contracts import TickerSnapshot
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate
from snapper.infrastructure.exchanges.errors import AmbiguousOrderSubmitError
from snapper.infrastructure.exchanges.errors import CircuitBreakerOpenError
from snapper.infrastructure.exchanges.errors import RestPoolDispatchError
from snapper.infrastructure.exchanges.kraken_rest_egress import KrakenRestOperationKind
from snapper.infrastructure.exchanges.kraken_rest_egress import RestProxyTarget
from snapper.infrastructure.exchanges.kraken_rest_egress import ccxt_proxy_target
from snapper.infrastructure.exchanges.kraken_rest_egress import route_kraken_rest_sync_call
from snapper.infrastructure.exchanges.kraken_rest_egress import spot_sdk_proxy_target
from snapper.infrastructure.exchanges.kraken_sdk_patches import apply_kraken_ws_teardown_hardening
from snapper.infrastructure.exchanges.kraken_sdk_patches import force_close_ws_client
from snapper.infrastructure.exchanges.schemas.kraken import KrakenExecutionSubscribeParamsSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenExecutionSubscriptionAckSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenInstrumentPairSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenInstrumentSubscribeParamsSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenOhlcSubscribeParamsSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenOhlcSubscriptionAckSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenTickerSubscribeParamsSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenTickerSubscriptionAckSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenTradeSubscribeParamsSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenTradeSubscriptionAckSchema
from snapper.infrastructure.symbols.functions import ccxt_to_native
from snapper.infrastructure.symbols.functions import get_available_kraken_symbols
from snapper.infrastructure.symbols.functions import native_to_ccxt
from snapper.infrastructure.symbols.functions import native_to_kraken_rest
from snapper.infrastructure.symbols.functions import native_to_kraken_websocket

apply_kraken_ws_teardown_hardening()

_CREDENTIALS_REQUIRED_MSG = "API credentials required for trading"
_STOP_ORDER_TYPES = frozenset(
    {ExchangeOrderTypeEnum.STOP_LOSS, ExchangeOrderTypeEnum.STOP_LOSS_LIMIT}
)
_retry_sleep = asyncio.sleep


def _require_stop_price(request: ExchangeOrderRequest) -> float:
    """Return the trigger price of a stop-typed request, or raise pre-send.

    Raising BEFORE any network call keeps the failure provably-not-placed,
    so the executor's definitive-reject branch is the correct disposition
    (#156). The executor's own gate makes this unreachable for outbox
    traffic; it defends direct client callers. A stop-loss-limit request
    must also carry its limit leg — silently submitting without it would
    either crash inside ccxt's price formatting or degrade the order on
    the native path, mirroring the futures client's pre-send gate.

    Raises:
        ValueError: When ``stop_price`` is missing, or a stop-loss-limit
            request carries no ``price``.
    """
    if request.stop_price is None:
        raise ValueError(f"{request.type.value} order requires stop_price")
    if request.type is ExchangeOrderTypeEnum.STOP_LOSS_LIMIT and request.price is None:
        raise ValueError("stop-loss-limit order requires price (the limit leg)")
    return float(request.stop_price)


_WS_CLIENT_CONNECTED_MSG = "WebSocket client should be connected"
_REPLAY_CLIENT_REPLACED_MSG = "WebSocket client replaced during subscription replay"
_CONNECT_OWNERSHIP_LOST_MSG = "WebSocket client replaced during connect"
_WS_CONNECT_TIMEOUT_S = 20.0
"""Upper bound on a WebSocket connect (``SpotWSClient.start``) so a connect that
never completes cannot wedge liveness recovery.

The python-kraken-sdk's ``start`` polls for the socket with a connect timeout
that never fires (``while (timeout := 0.0) < 10`` resets the counter every
iteration), so on a prolonged blackout — where the connector hits its reconnect
ceiling and exits without ever setting the socket — ``start`` loops forever.
Bounding it here turns that permanent hang into a timeout that tears the partial
client down and lets the recovery loop retry with a fresh client, which
reconnects once the network returns instead of requiring a process restart."""
_SDK_SEND_TIMEOUT_S = 5.0
"""Upper bound on a single private-channel SDK subscribe send.

A hung executions subscribe would otherwise wedge the generator BEFORE its
death-detection loop starts, making the death invisible to the executor-side
stream supervisor (the exact #144 wedge class, on the fills money path). On
timeout the subscribed client is closed (compare-and-clear) so the next
attempt rebuilds fresh instead of re-sending into the same dead socket.
Mirrors the Kraken Futures constant of the same name."""
_QUEUE_MAX_SIZE = 10_000
_TICK_QUEUE_MAX_SIZE = 50_000
"""Boot-time absorption budget for ticker and candle producer queues.

The Kraken Spot broker emits a wildcard replay/update burst in the
first 60 s after WS handshake — every subscribed pair on every
``ticker`` feed delivers its current snapshot, and every per-interval
``ohlc`` feed dumps its current bar plus seed history. The 2026-05-25
post-restart log review counted 34 drop-oldest WARN records on the
tick path; the 2026-05-26 follow-up (after the tick fix landed)
counted a 3 177-message candle drop burst in a single second once the
tick queue stopped being the bottleneck. The behaviour is correct
(bounded queue + drop-oldest is the right contract for a publisher
feeding live consumers) but the boot burst is a known transient that
the queue depth should absorb without dropping. ``trade``/
``instrument``/``execution`` queues remain at ``_QUEUE_MAX_SIZE``
(10 k) because no drops have been observed on those paths."""
_TRADE_SUBSCRIBE_CHUNK_SIZE = 100
_TRADE_SUBSCRIBE_CHUNK_DELAY_S = 0.1
_CANDLE_SUBSCRIBE_CHUNK_SIZE = 100
_CANDLE_SUBSCRIBE_CHUNK_DELAY_S = 0.1
_RESUBSCRIBE_CHUNK_DELAY_S = 0.5
"""Delay between cached subscription chunks when the app-level recovery
path rebuilds a fresh ``SpotWSClient`` and replays the subscription cache.

A freshly built client has an empty SDK subscription set, so the SDK's own
paced ``_recover_subscriptions`` cannot cover this rebuild path — the app
must replay. At the previous 5.0 s/chunk, replaying the Spot universe
(~39 cached chunks) took ~195 s, which the liveness-recovery path could
never complete inside its bounding window, leaving the feed permanently
dark after a multi-minute outage. 0.5 s/chunk replays the whole universe
in ~20 s while staying well under Kraken's per-connection subscribe
message-rate limit (the per-symbol chunk sends are themselves paced
``_TRADE_SUBSCRIBE_CHUNK_DELAY_S``/``_CANDLE_SUBSCRIBE_CHUNK_DELAY_S``)."""
_ALREADY_SUBSCRIBED_ERROR = "Already subscribed"
_UNKNOWN_SUBSCRIPTION_ERROR = "unknown subscription error"

_DROP_LOG_INTERVAL_S = 1.0
_drop_counters: dict[str, list[float]] = {}

_OHLC_DEPRECATED_TIMESTAMP_NOTICE: Final[str] = "timestamp is deprecated, use interval_begin"
"""Known Kraken WS deprecation notice for OHLC subscription ACKs.

Kraken v2 emits this in ``result.warnings`` on every OHLC subscribe — we
already parse ``interval_begin`` exclusively, so the notice is
informational and does not require operator action. Logged once per
process at INFO; subsequent occurrences are emitted at DEBUG. See
``snapper.infrastructure.exchanges.implementations.kraken
._handle_ohlc_subscription_ack``."""

_OHLC_NOTICES_LOGGED: set[str] = set()
"""Module-level set of OHLC ack warning strings already INFO-logged once.

Used by ``_log_ohlc_subscription_warning`` to deduplicate known
informational notices that Kraken emits on every subscribe. Mutating a
set in place does not require the ``global`` statement and keeps the
clean-signal-log behaviour without tripping Ruff's ``PLW0603`` lint
rule."""


def _subscription_ack_confirms(success: bool, error: str | None) -> bool:
    """Return whether a subscribe ACK confirms active server-side state.

    Args:
        success: ACK success flag.
        error: Optional ACK error string.

    Returns:
        True when the ACK is successful or reports an idempotent
        already-subscribed condition.
    """
    return success or error == _ALREADY_SUBSCRIBED_ERROR


def _enqueue_or_drop_oldest(queue: asyncio.Queue[Any], item: Any, label: str) -> None:
    """Put item on queue, dropping the oldest if full.

    Matches the bounded-queue pattern already used by
    :py:mod:`~snapper.infrastructure.exchanges.implementations.kraken_futures`
    and
    :py:mod:`~snapper.infrastructure.exchanges.implementations.kraken_equities`.
    Drop-oldest is the safer default for market-data feeds: a slow
    consumer must never grow RSS without bound.

    Logs are rate-limited to one summary line per ``_DROP_LOG_INTERVAL_S``
    seconds per ``label`` — the previous per-drop ``logger.warning`` cost
    ~29 us each, which at sustained drop rates of hundreds per second
    became its own non-trivial fraction of the publisher hot path.

    Args:
        queue: Bounded asyncio queue.
        item: Item to enqueue.
        label: Human-readable label for the warning log.
    """
    try:
        queue.put_nowait(item)
    except asyncio.QueueFull:
        counters = _drop_counters.setdefault(label, [0.0, 0.0])
        counters[0] += 1
        now = monotonic()
        if now - counters[1] >= _DROP_LOG_INTERVAL_S:
            logger.warning(
                f"{label} queue full, dropped {int(counters[0])} messages "
                f"in last {now - counters[1]:.1f}s (drop-oldest backpressure)"
            )
            counters[0] = 0.0
            counters[1] = now
        queue.get_nowait()
        queue.put_nowait(item)


_CCXT_STATUS_MAP: Final[dict[str, ExchangeOrderStatusEnum]] = {
    "open": ExchangeOrderStatusEnum.OPEN,
    "closed": ExchangeOrderStatusEnum.CLOSED,
    "canceled": ExchangeOrderStatusEnum.CANCELED,
    "cancelled": ExchangeOrderStatusEnum.CANCELED,
    "expired": ExchangeOrderStatusEnum.EXPIRED,
    "pending": ExchangeOrderStatusEnum.PENDING,
}

_CCXT_SIDE_MAP: Final[dict[str, OrderSideEnum]] = {
    TradeSideEnum.BUY: OrderSideEnum.BUY,
    TradeSideEnum.SELL: OrderSideEnum.SELL,
}

_CCXT_TYPE_MAP: Final[dict[str, ExchangeOrderTypeEnum]] = {
    "market": ExchangeOrderTypeEnum.MARKET,
    "limit": ExchangeOrderTypeEnum.LIMIT,
    "stop": ExchangeOrderTypeEnum.STOP_LOSS,
    "stop-loss": ExchangeOrderTypeEnum.STOP_LOSS,
    "take-profit": ExchangeOrderTypeEnum.TAKE_PROFIT,
}

_TRIGGER_ORDER_TYPES: Final[frozenset[ExchangeOrderTypeEnum]] = frozenset(
    {
        ExchangeOrderTypeEnum.STOP_LOSS,
        ExchangeOrderTypeEnum.STOP_LOSS_LIMIT,
        ExchangeOrderTypeEnum.TAKE_PROFIT,
        ExchangeOrderTypeEnum.TAKE_PROFIT_LIMIT,
        ExchangeOrderTypeEnum.TRAILING_STOP,
        ExchangeOrderTypeEnum.TRAILING_STOP_LIMIT,
    }
)
"""Order types whose native ``descr.price`` is a TRIGGER, not a price.

For these, the executable limit leg lives in ``descr.price2`` (absent on
pure market-leg stops) — reading ``descr.price`` as a limit/fill price
would leak the trigger into snapshots.
"""


def _native_fee_currency(native_symbol: str, oflags: str) -> str | None:
    """Infer the fee currency of a native spot order from its oflags.

    Mirrors the vendored ccxt ``parse_order`` exactly: the currency is
    resolved ONLY when ``oflags`` explicitly carries ``fciq`` (quote) or
    ``fcib`` (base); otherwise it stays None and downstream fee
    accounting treats the snapshot as fee-less — the same behavior the
    CCXT-mapped path already has. The native symbol is ``BASE-QUOTE``.

    Args:
        native_symbol: Native symbol the order was placed on.
        oflags: Raw comma-delimited order flags from the payload.

    Returns:
        The fee currency code, or None when not explicitly flagged or
        the symbol shape is not BASE-QUOTE.
    """
    base, separator, quote = native_symbol.partition("-")
    if not separator or not base or not quote:
        return None
    if "fciq" in oflags:
        return quote
    if "fcib" in oflags:
        return base
    return None


def _resolve_ccxt_order_type(ccxt_order: dict[str, Any]) -> ExchangeOrderTypeEnum:
    """Resolve the wire order type of a fetched ccxt order.

    ccxt's unified ``type`` collapses Kraken stop orders to their BASE
    type (``parse_order_type`` maps raw ``stop-loss`` -> ``market`` and
    ``stop-loss-limit`` -> ``limit``), which would make an adopted or
    fetched protective stop indistinguishable from a plain order (#156).
    The raw Kraken ``ordertype`` survives under ``info.descr`` and its
    values coincide exactly with ``ExchangeOrderTypeEnum``, so it is
    preferred; the unified ``type`` via ``_CCXT_TYPE_MAP`` remains the
    fallback for payloads without a raw description.
    """
    info = ccxt_order.get("info")
    descr = info.get("descr") if isinstance(info, dict) else None
    raw = descr.get("ordertype") if isinstance(descr, dict) else None
    if isinstance(raw, str):
        try:
            return ExchangeOrderTypeEnum(raw)
        except ValueError:
            logger.warning(f"Unknown raw Kraken ordertype {raw!r}; falling back to unified type")
    return _CCXT_TYPE_MAP.get(ccxt_order.get("type", ""), ExchangeOrderTypeEnum.LIMIT)


def _resolve_ccxt_fill_price(ccxt_order: dict[str, Any]) -> float | None:
    """Resolve the snapshot price for a CCXT order.

    Prefers the explicit ``price`` (the limit price). When ``price`` is
    absent — the common case for market orders — falls back to the
    executed average (``average``, the venue's VWAP across fills), but
    only when the order has actually filled (``filled > 0``); an unfilled
    order with no price stays ``None``. This lets fill-gap reconciliation
    emit a corrective fill for filled market orders instead of skipping on
    a missing price. The venue's own VWAP is used directly — the price is
    never approximated from local ticks.

    Args:
        ccxt_order: Raw CCXT order dict (external SDK boundary).

    Returns:
        The resolved fill price, or ``None`` when neither a limit price
        nor an executed average is available for a filled order.
    """
    price = ccxt_order.get("price")
    if price:
        return float(price)
    average = ccxt_order.get("average")
    if not average:
        return None
    if float(ccxt_order.get("filled") or 0) <= 0:
        return None
    return float(average)


def _kraken_user_order_descr(payload: dict[str, Any]) -> dict[str, Any]:
    """Return the native Kraken order description mapping."""
    descr_raw = payload.get("descr")
    if isinstance(descr_raw, dict):
        return descr_raw
    return {}


def _mapping_float_value(mapping: dict[str, Any], key: str) -> float:
    """Return a float field value, treating missing or blank values as zero."""
    value = mapping.get(key)
    if value:
        return float(value)
    return 0.0


def _mapping_optional_float_value(mapping: dict[str, Any], key: str) -> float | None:
    """Return an optional float field value."""
    value = mapping.get(key)
    if value:
        return float(value)
    return None


def _mapping_string_value(mapping: dict[str, Any], key: str) -> str:
    """Return a string field value, treating missing values as blank."""
    return str(mapping.get(key) or "")


def _resolve_kraken_user_order_type(raw_type: str) -> ExchangeOrderTypeEnum:
    """Resolve a native Kraken user-order type."""
    try:
        return ExchangeOrderTypeEnum(raw_type)
    except ValueError:
        logger.warning(f"Unknown raw Kraken ordertype {raw_type!r}; falling back to limit")
        return ExchangeOrderTypeEnum.LIMIT


def _resolve_kraken_user_limit_price(
    descr: dict[str, Any], order_type: ExchangeOrderTypeEnum
) -> float:
    """Resolve the executable limit price field for a native Kraken order."""
    price_key = "price2" if order_type in _TRIGGER_ORDER_TYPES else "price"
    return _mapping_float_value(descr, price_key)


def _resolve_native_order_price(
    limit_price: float, filled: float, average_price: float
) -> float | None:
    """Resolve the snapshot price for a native Kraken order."""
    if limit_price > 0:
        return limit_price
    if filled > 0 and average_price > 0:
        return average_price
    return None


def _resolve_kraken_user_order_timestamp(opentm: Any) -> float:
    """Resolve the snapshot timestamp for a native Kraken order."""
    if opentm:
        return float(opentm)
    return time.time()


def _resolve_kraken_user_fee_currency(
    native_symbol: str, oflags: str, fee: float | None
) -> str | None:
    """Resolve native Kraken fee currency when fee data is present."""
    if fee is None:
        return None
    return _native_fee_currency(native_symbol, oflags)


class KrakenExchangeClient(ExchangeClientBase):
    """Kraken exchange client with REST and WebSocket support.

    This client provides comprehensive access to the Kraken cryptocurrency
    exchange, combining CCXT for standard operations with the native Kraken
    SDK for advanced features like WebSocket trading.

    The client implements circuit breaker pattern to handle API failures
    gracefully and automatic retry with exponential backoff for rate limits.

    Attributes:
        settings: Application settings instance.
        api_key: Kraken API key for authenticated requests.
        api_secret: Kraken API secret for request signing.
        sandbox: Whether to use sandbox/demo environment.
    """

    def __init__(
        self,
        api_key: str | None = None,
        api_secret: str | None = None,
        sandbox: bool = False,
        enable_rate_limit: bool = True,
        repository: Repository | None = None,
    ) -> None:
        """Initialize Kraken exchange client.

        Args:
            api_key: Kraken API key. Required for trading and private data.
            api_secret: Kraken API secret. Required for trading and private data.
            sandbox: Use sandbox environment for testing (default: False).
            enable_rate_limit: Enable automatic rate limiting (default: True).
            repository: Database repository for order/execution logging.
        """
        super().__init__(repository=repository, exchange_name=ExchangeEnum.KRAKEN)
        self._health_tracker: SubscriptionHealthTracker = SubscriptionHealthTracker(
            dark_recovery_channels=frozenset({"ticker", "trade"})
        )
        self.settings = get_settings()
        self.api_key = api_key
        self.api_secret = api_secret
        self.sandbox = sandbox
        ccxt_kwargs: dict[str, Any] = {
            "apiKey": api_key,
            "secret": api_secret,
            "sandbox": sandbox,
            "enableRateLimit": enable_rate_limit,
            "timeout": 30000,
            "rateLimit": 1000,
        }
        self._ccxt_client = cast(Any, ccxt.kraken)(ccxt_kwargs)
        self._ws_client: SpotWSClient | None = None
        self._ws_connect_lock: asyncio.Lock = asyncio.Lock()
        self._ws_connected = False
        self._trade_client: Trade | None = None
        self._user_client: User | None = None
        self._tick_queue: asyncio.Queue[TickerUpdate] = asyncio.Queue(maxsize=_TICK_QUEUE_MAX_SIZE)
        self._candle_queues: dict[int, asyncio.Queue[CandleUpdate]] = {}
        self._trade_queue: asyncio.Queue[TradeUpdate] = asyncio.Queue(maxsize=_QUEUE_MAX_SIZE)
        self._trade_built_candle_builder: TradeCandleBuilder = TradeCandleBuilder(
            interval_seconds=60
        )
        self._trade_built_candle_queue: asyncio.Queue[CandleUpdate] = asyncio.Queue(
            maxsize=_TICK_QUEUE_MAX_SIZE
        )
        self._trade_built_candle_aggregator_task: asyncio.Task[None] | None = None
        self._trade_built_candles_enabled: bool = False
        self._execution_queue: asyncio.Queue[ExecutionUpdate] = asyncio.Queue(
            maxsize=_QUEUE_MAX_SIZE
        )
        self._raw_instrument_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(
            maxsize=_QUEUE_MAX_SIZE
        )
        self._instrument_queue: asyncio.Queue[InstrumentPairDescriptor] = asyncio.Queue(
            maxsize=_QUEUE_MAX_SIZE
        )
        self._raw_ticker_capture: dict[str, dict[str, Any]] | None = None
        self._subscription_cache: dict[tuple[str, frozenset[str], str], SubscriptionRequest] = {}
        self._circuit_failures = 0
        self._circuit_open_until = 0.0
        self._max_failures = 5
        self._circuit_timeout = 60.0
        self._rest_egress_proxy_lock = threading.RLock()

    def get_ccxt_client(self) -> Any:
        """Get the underlying CCXT client instance.

        Returns:
            The CCXT Kraken client for direct access to CCXT methods.
        """
        return self._ccxt_client

    async def _dispatch_routed_rest[ResultT](
        self,
        *,
        operation: str,
        kind: KrakenRestOperationKind,
        target: RestProxyTarget,
        sync_call: Callable[[], ResultT],
    ) -> ResultT:
        """Dispatch a blocking Kraken REST call through the egress router.

        The route helper runs inside the REST worker thread so mutable
        CCXT/SDK proxy state is scoped around the actual blocking HTTP
        send, not around an ``await`` in the event loop.

        Args:
            operation: Stable operation name for egress identity.
            kind: Public/private/idempotency classification.
            target: REST client whose proxy state should be scoped.
            sync_call: Blocking SDK call to execute.

        Returns:
            Result returned by ``sync_call``.
        """
        return await self._dispatch_blocking(
            lambda: route_kraken_rest_sync_call(
                exchange=str(self.exchange_name),
                operation=operation,
                kind=kind,
                target=target,
                proxy_lock=self._rest_egress_proxy_lock,
                sync_call=sync_call,
            )
        )

    async def connect(self) -> None:
        """Establish REST connection to Kraken exchange.

        Loads markets and configures nonce generation for authenticated
        requests. Resets circuit breaker state on successful connection.
        Reopens the bounded REST pool first (same-instance reconnects)
        and on ANY failure — including cancellation — shuts the pool
        back down AND closes the ccxt REST session, because manual
        ``connect()`` callers (publisher start, symbol updaters,
        snapshot utilities) have no ``__aexit__``/stop path that would
        release either resource after a failed connect. A later
        ``connect()`` on the same instance still works: requests
        recreates a closed session's connection pools lazily.

        Raises:
            Exception: If connection or market loading fails.
        """
        self._reopen_rest_pool()
        try:
            self._ccxt_client.nonce = lambda: int(time.time() * 100_000_000)
            await self._with_retry(
                self._ccxt_client.load_markets,
                egress_kind="public_read",
                egress_operation="load_markets",
                egress_target=ccxt_proxy_target(self._ccxt_client),
            )
            logger.info("Kraken REST connection established")
            self._circuit_failures = 0
            self._circuit_open_until = 0.0
        except BaseException as e:
            self._shutdown_rest_pool()
            with contextlib.suppress(Exception):
                if self._ccxt_client and hasattr(self._ccxt_client, "session"):
                    self._ccxt_client.session.close()
            logger.error(f"Failed to connect to Kraken: {e}")
            raise

    _WS_CLOSE_TIMEOUT_SECONDS = 10.0

    async def _close_ws_client(self) -> None:
        """Close WebSocket client with timeout protection.

        Closes whenever a client object exists, regardless of the
        ``_ws_connected`` flag. The flag is left ``False`` when a recovery
        attempt is cancelled mid subscription-replay (the success line that
        sets it is never reached); gating ``close()`` on the flag therefore
        leaked the SDK client, its background run task, and its aiohttp
        session on every cancelled recovery, accumulating orphaned
        reconnecting clients that competed for the same egress route and
        provoked rate-limit cascades. Closing by object existence tears the
        prior client down cleanly before a new one is built.

        The slot clear is compare-and-clear: this close may race a concurrent
        ``_ensure_ws_connected`` that already installed a NEWER client, and
        unconditionally nulling the slot would detach that live client
        (callback still attached, no owner). The ``_ws_connected`` flag write
        is owner-gated for the same reason: an unconditional ``False`` from a
        loser of that race would stamp the freshly built connection as
        disconnected after the winner already set it ``True``.

        Ensures the process does not hang indefinitely when the Kraken
        SDK fails to close the connection or its underlying aiohttp
        session in a timely manner. When the bounded ``close()`` times out
        or raises, :func:`force_close_ws_client` finishes the teardown
        explicitly — cancelling the connector run tasks and closing the
        aiohttp session the SDK's interrupted ``close()`` left open (#143).
        """
        client = self._ws_client
        if not client:
            return
        was_connected = self._ws_connected
        try:
            async with asyncio.timeout(self._WS_CLOSE_TIMEOUT_SECONDS):
                await client.close()
        except TimeoutError:
            logger.warning("WebSocket close timed out - forcing cleanup")
            await force_close_ws_client(client)
        except Exception as exc:
            logger.warning(f"WebSocket close failed - forcing cleanup: {exc!r}")
            await force_close_ws_client(client)
        finally:
            if self._ws_client is client:
                self._ws_client = None
                self._ws_connected = False
                if was_connected:
                    logger.info("Kraken WebSocket disconnected")

    async def disconnect_websocket(self) -> None:
        """Close WebSocket connection without disconnecting REST client."""
        await self._close_ws_client()

    async def disconnect(self) -> None:
        """Disconnect from Kraken exchange.

        Closes both WebSocket and REST client sessions, then shuts down
        the bounded REST thread pool (idempotent; a later ``connect()``
        on the same instance rebuilds it).
        """
        try:
            await self._close_ws_client()
            if self._ccxt_client and hasattr(self._ccxt_client, "session"):
                self._ccxt_client.session.close()
                logger.info("Kraken REST client session closed")
        except Exception as e:
            logger.warning(f"Error during disconnect: {e}")
        finally:
            self._shutdown_rest_pool()

    async def get_ticker(self, symbol: str) -> TickerSnapshot:
        """Fetch current ticker data for a symbol.

        Args:
            symbol: Trading pair in native format (e.g., 'BTC-USD').

        Returns:
            Current ticker with bid, ask, last price and timestamp.

        Raises:
            Exception: If ticker fetch fails.
        """
        try:
            ccxt_symbol = native_to_ccxt(symbol)
            ticker_data = await self._with_retry(
                self._ccxt_client.fetch_ticker,
                ccxt_symbol,
                egress_kind="public_read",
                egress_operation="fetch_ticker",
                egress_target=ccxt_proxy_target(self._ccxt_client),
            )
            native_symbol = ccxt_to_native(ticker_data["symbol"])
            return TickerSnapshot(
                symbol=native_symbol,
                bid=float(ticker_data["bid"]),
                ask=float(ticker_data["ask"]),
                last=float(ticker_data["last"]),
                timestamp=ticker_data["timestamp"] / 1000.0,
            )
        except Exception as e:
            logger.error(f"Failed to get ticker for {symbol}: {e}")
            raise

    async def get_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1m",
        since: int | None = None,
        limit: int | None = None,
    ) -> list[OhlcvSnapshot]:
        """Fetch OHLCV candle data for a symbol.

        Args:
            symbol: Trading pair in native format.
            timeframe: Candle interval (e.g., '1m', '1h', '1d').
            since: Start timestamp in milliseconds.
            limit: Maximum number of candles to return.

        Returns:
            List of OHLCV snapshots.

        Raises:
            Exception: If OHLCV fetch fails.
        """
        try:
            ccxt_symbol = native_to_ccxt(symbol)
            ohlcv_data = await self._with_retry(
                self._ccxt_client.fetch_ohlcv,
                ccxt_symbol,
                timeframe,
                since,
                limit,
                egress_kind="public_read",
                egress_operation="fetch_ohlcv",
                egress_target=ccxt_proxy_target(self._ccxt_client),
            )
            return [
                OhlcvSnapshot(
                    timestamp=float(candle[0]) / 1000.0,
                    open=float(candle[1]),
                    high=float(candle[2]),
                    low=float(candle[3]),
                    close=float(candle[4]),
                    volume=float(candle[5]),
                )
                for candle in ohlcv_data
            ]
        except Exception as e:
            logger.error(f"Failed to get OhlcvSnapshot for {symbol}: {e}")
            raise

    async def create_order(self, request: ExchangeOrderRequest) -> ExchangeOrderSnapshot:
        """Create a new order on the exchange.

        Uses CCXT for standard symbols, falls back to native Kraken API
        for unsupported symbols (e.g., tokenized assets).

        Args:
            request: Order parameters including symbol, side, type, amount.

        Returns:
            Created order snapshot with status.

        Raises:
            RuntimeError: If API credentials are missing.
            ValueError: If a stop-typed request carries no
                ``stop_price`` — raised BEFORE any network send so the
                failure is provably-not-placed (definitive reject).
            Exception: If order creation fails.
        """
        if not self.api_key or not self.api_secret:
            raise RuntimeError(_CREDENTIALS_REQUIRED_MSG)
        try:
            return await self._create_order_via_ccxt(request)
        except ValueError as e:
            if "Unknown native symbol" not in str(e):
                raise
            logger.info(
                f"Symbol {request.symbol} not supported by CCXT, using native Kraken API fallback"
            )
            return await self._create_order_via_native(request)
        except Exception as e:
            logger.error(f"Failed to create order: {e}")
            raise

    async def _create_order_via_ccxt(self, request: ExchangeOrderRequest) -> ExchangeOrderSnapshot:
        """Create order using CCXT client.

        Order creation is sent with ``retry_network_errors=False``: it is
        the one non-idempotent venue mutation on this client, and a blind
        re-send after an ambiguous network failure (request possibly
        executed, response lost) can double-place a market order because
        Kraken's ``cl_ord_id`` dedupe covers only open orders. The
        ambiguous failure is re-raised as ``AmbiguousOrderSubmitError``
        so the executor routes it to the UNKNOWN/verify path instead of
        fabricating a rejection.
        ``RateLimitExceeded`` is re-raised plain FIRST because it
        subclasses ``NetworkError`` while being a definitive venue-side
        rejection (the exhausted 429 cannot have placed the order).

        Stop types translate through ccxt's ``stopLossPrice`` param
        (verified against the pinned ccxt 4.5.57 kraken source,
        ``order_request``): the ccxt type is the BASE type (``market``
        for stop-loss, ``limit`` for stop-loss-limit) and ccxt itself
        derives ``ordertype`` from the param, putting the trigger in
        AddOrder ``price`` and the limit leg in ``price2``. Passing the
        wire value (``stop-loss``) as the ccxt type instead would make
        ccxt treat it as an already-triggered exotic type and skip the
        trigger wiring.

        Args:
            request: Order parameters.

        Returns:
            Created order snapshot.

        Raises:
            AmbiguousOrderSubmitError: If the venue call failed in a way
                where the order MAY exist on the venue.
        """
        ccxt_symbol, ccxt_type, ccxt_params = self._build_ccxt_order_submit(request)
        order_data = await self._submit_ccxt_order(request, ccxt_symbol, ccxt_type, ccxt_params)
        order = self._snapshot_submitted_ccxt_order(request, order_data)
        await self._attach_submitted_order_db_ids(request, order)
        return order

    def _build_ccxt_order_submit(
        self, request: ExchangeOrderRequest
    ) -> tuple[str, str, dict[str, Any]]:
        """Given an order request, when using CCXT, then build submit arguments.

        Args:
            request: Order request from the exchange contract.

        Returns:
            Native symbol mapped to CCXT, CCXT order type, and CCXT params.

        Raises:
            ValueError: Propagates symbol mapping or stop-price validation failures.
        """
        ccxt_symbol = native_to_ccxt(request.symbol)
        ccxt_params: dict[str, Any] = {}
        if request.client_order_id:
            ccxt_params["clientOrderId"] = request.client_order_id
        if request.leverage is not None:
            ccxt_params["leverage"] = request.leverage
        if request.post_only:
            ccxt_params["postOnly"] = True
        ccxt_type = request.type.value
        if request.type in _STOP_ORDER_TYPES:
            ccxt_params["stopLossPrice"] = _require_stop_price(request)
            ccxt_type = (
                "limit" if request.type is ExchangeOrderTypeEnum.STOP_LOSS_LIMIT else "market"
            )
        return ccxt_symbol, ccxt_type, ccxt_params

    async def _submit_ccxt_order(
        self,
        request: ExchangeOrderRequest,
        ccxt_symbol: str,
        ccxt_type: str,
        ccxt_params: dict[str, Any],
    ) -> dict[str, Any]:
        """Given prepared CCXT arguments, when submitting, then map ambiguous failures.

        Args:
            request: Original order request used for error identity.
            ccxt_symbol: CCXT trading pair.
            ccxt_type: CCXT order type.
            ccxt_params: CCXT-specific order parameters.

        Returns:
            Raw CCXT order payload.

        Raises:
            AmbiguousOrderSubmitError: If the send failed ambiguously.
            ccxt.RateLimitExceeded: Propagated unchanged for definitive venue rejection.
        """
        try:
            order_data = await self._with_retry(
                self._ccxt_client.create_order,
                ccxt_symbol,
                ccxt_type,
                request.side.value,
                float(request.amount),
                float(request.price) if request.price else None,
                ccxt_params,
                retry_network_errors=False,
                egress_kind="private_mutation",
                egress_operation="create_order",
                egress_target=ccxt_proxy_target(self._ccxt_client),
            )
        except ccxt.RateLimitExceeded:
            raise
        except RestPoolDispatchError as e:
            raise AmbiguousOrderSubmitError(
                client_order_id=request.client_order_id or "",
                instrument=request.symbol,
                message=f"Kraken Spot create_order pool dispatch failure (order may exist): {e}",
            ) from e
        except (ccxt.NetworkError, ccxt.ExchangeNotAvailable) as e:
            raise AmbiguousOrderSubmitError(
                client_order_id=request.client_order_id or "",
                instrument=request.symbol,
                message=f"Kraken Spot create_order network failure (order may exist): {e}",
            ) from e
        return cast(dict[str, Any], order_data)

    def _snapshot_submitted_ccxt_order(
        self, request: ExchangeOrderRequest, order_data: dict[str, Any]
    ) -> ExchangeOrderSnapshot:
        """Given a raw CCXT submit answer, when accepted, then build a snapshot.

        Args:
            request: Original order request.
            order_data: Raw CCXT order payload.

        Returns:
            Pending order snapshot matching the previous inline construction.

        Raises:
            This helper does not raise directly.
        """
        return ExchangeOrderSnapshot(
            id=str(order_data.get("id") or ""),
            client_order_id=request.client_order_id,
            symbol=request.symbol,
            side=request.side,
            type=request.type,
            amount=float(request.amount),
            price=float(request.price) if request.price else None,
            status=ExchangeOrderStatusEnum.PENDING,
            filled=0.0,
            remaining=float(request.amount),
            timestamp=time.time(),
        )

    async def _attach_submitted_order_db_ids(
        self, request: ExchangeOrderRequest, order: ExchangeOrderSnapshot
    ) -> None:
        """Given a submitted order, when it has a venue id, then attach DB ids.

        Args:
            request: Original order request.
            order: Submitted order snapshot to mutate with DB identifiers.

        Returns:
            None.

        Raises:
            Exception: Propagates database logging failures unchanged.
        """
        if order.id:
            db_result = await self._log_order_to_db(request, order)
            if db_result is not None:
                order.db_order_id = db_result[0]
                order.db_order_public_id = db_result[1]

    async def _create_order_via_native(
        self, request: ExchangeOrderRequest
    ) -> ExchangeOrderSnapshot:
        """Create order using native Kraken Trade API (fallback for unsupported CCXT symbols).

        For stop types the AddOrder mapping is the documented Kraken
        one: ``price`` carries the STOP TRIGGER and ``price2`` the
        limit leg of ``stop-loss-limit`` (#156).

        Args:
            request: Order parameters.

        Returns:
            Created order snapshot.

        Raises:
            ValueError: If a stop-typed request carries no ``stop_price``
                (pre-send, provably-not-placed).
            Exception: If native API order creation fails.
        """
        try:
            kraken_rest_symbol = native_to_kraken_rest(request.symbol)
            trade_client = self._get_trade_client()
            kraken_params: dict[str, Any] = {
                "ordertype": request.type.value,
                "side": request.side.value,
                "pair": kraken_rest_symbol,
                "volume": str(request.amount),
            }
            if request.type in _STOP_ORDER_TYPES:
                kraken_params["price"] = str(_require_stop_price(request))
                if request.price:
                    kraken_params["price2"] = str(request.price)
            elif request.price:
                kraken_params["price"] = str(request.price)
            if request.leverage is not None:
                kraken_params["leverage"] = str(request.leverage)
            if request.post_only:
                kraken_params["oflags"] = "post"
            extra_params: dict[str, Any] = {}
            if request.client_order_id:
                extra_params["cl_ord_id"] = str(request.client_order_id)
            if kraken_rest_symbol.endswith(("x/USD", "x/EUR")):
                extra_params["asset_class"] = "tokenized_asset"
            try:
                result = await self._dispatch_routed_rest(
                    operation="native_create_order",
                    kind="private_mutation",
                    target=spot_sdk_proxy_target(trade_client),
                    sync_call=lambda: trade_client.create_order(
                        **kraken_params,
                        extra_params=extra_params or None,
                    ),
                )
            except (requests.exceptions.RequestException, RestPoolDispatchError) as e:
                raise AmbiguousOrderSubmitError(
                    client_order_id=request.client_order_id or "",
                    instrument=request.symbol,
                    message=(
                        f"Kraken native create_order transport failure (order may exist): {e}"
                    ),
                ) from e
            order = self._convert_kraken_native_order(result, request)
            db_result = await self._log_order_to_db(request, order)
            if db_result is not None:
                order.db_order_id = db_result[0]
                order.db_order_public_id = db_result[1]
            return order
        except Exception as fallback_error:
            logger.error(f"Failed to create order with native Kraken API: {fallback_error}")
            raise

    async def cancel_order(self, order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        """Cancel an existing order.

        Args:
            order_id: Exchange order ID to cancel.
            symbol: Trading pair (optional, improves performance).

        Returns:
            Cancelled order snapshot.

        Raises:
            RuntimeError: If API credentials are missing.
            Exception: If cancellation fails.
        """
        if not self.api_key or not self.api_secret:
            raise RuntimeError(_CREDENTIALS_REQUIRED_MSG)
        try:
            ccxt_symbol = native_to_ccxt(symbol) if symbol else None
            await self._with_retry(
                self._ccxt_client.cancel_order,
                order_id,
                ccxt_symbol,
                retry_network_errors=False,
                egress_kind="private_mutation",
                egress_operation="cancel_order",
                egress_target=ccxt_proxy_target(self._ccxt_client),
            )
            order_data = await self._with_retry(
                self._ccxt_client.fetch_order,
                order_id,
                ccxt_symbol,
                retry_network_errors=False,
                egress_kind="private_idempotent_read",
                egress_operation="fetch_order_after_cancel",
                egress_target=ccxt_proxy_target(self._ccxt_client),
            )
            canceled_order = self._convert_ccxt_order(order_data)
            return canceled_order
        except ValueError as e:
            if not symbol or "Unknown native symbol" not in str(e):
                raise
            logger.info(f"Symbol {symbol} not supported by CCXT, using native Kraken API fallback")
            try:
                trade_client = self._get_trade_client()
                await self._dispatch_routed_rest(
                    operation="native_cancel_order",
                    kind="private_mutation",
                    target=spot_sdk_proxy_target(trade_client),
                    sync_call=lambda: trade_client.cancel_order(txid=order_id),
                )
                return ExchangeOrderSnapshot(
                    id=order_id,
                    client_order_id=None,
                    symbol=symbol,
                    side=OrderSideEnum.BUY,
                    type=ExchangeOrderTypeEnum.LIMIT,
                    amount=float("0"),
                    price=None,
                    status=ExchangeOrderStatusEnum.CANCELED,
                    filled=float("0"),
                    remaining=float("0"),
                    timestamp=time.time(),
                    fee=None,
                )
            except Exception as fallback_error:
                logger.error(f"Failed to cancel order with native Kraken API: {fallback_error}")
                raise
        except Exception as e:
            logger.error(f"Failed to cancel order {order_id}: {e}")
            raise

    async def get_order(self, order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        """Fetch details of a specific order.

        Args:
            order_id: Exchange order ID.
            symbol: Trading pair (optional).

        Returns:
            Order snapshot with current status.

        Raises:
            RuntimeError: If API credentials are missing.
            Exception: If order fetch fails.
        """
        if not self.api_key or not self.api_secret:
            raise RuntimeError(_CREDENTIALS_REQUIRED_MSG)
        try:
            ccxt_symbol: str | None = None
            if symbol is not None:
                try:
                    ccxt_symbol = native_to_ccxt(symbol)
                except ValueError:
                    return await self._get_order_native(order_id, symbol)
            order_data = await self._with_retry(
                self._ccxt_client.fetch_order,
                order_id,
                ccxt_symbol,
                retry_network_errors=False,
                egress_kind="private_idempotent_read",
                egress_operation="fetch_order",
                egress_target=ccxt_proxy_target(self._ccxt_client),
            )
            return self._convert_ccxt_order(order_data)
        except Exception as e:
            logger.error(f"Failed to get order {order_id}: {e}")
            raise

    async def _get_order_native(self, order_id: str, symbol: str) -> ExchangeOrderSnapshot:
        """Fetch one order via the native User REST API.

        Fallback for CCXT-unmapped (native-only) symbols so an order
        adopted through the native client-id verification can still be
        re-fetched by post-adoption reconciliation — the CCXT
        ``fetch_order`` path would raise out of the symbol mapping
        forever.

        Args:
            order_id: Venue order id (txid).
            symbol: Native symbol to stamp on the snapshot.

        Returns:
            The converted order snapshot.

        Raises:
            RuntimeError: If the answer shape is not interpretable.
            Exception: If the venue query fails.
        """
        user_client = self._get_user_client()
        response = await self._with_retry(
            user_client.get_orders_info,
            order_id,
            retry_network_errors=False,
            egress_kind="private_idempotent_read",
            egress_operation="native_get_orders_info",
            egress_target=spot_sdk_proxy_target(user_client),
        )
        entry = response.get(order_id) if isinstance(response, dict) else None
        if not isinstance(entry, dict):
            raise RuntimeError(
                f"Kraken returned a malformed orders-info answer for {order_id}: "
                f"cannot interpret the order state"
            )
        return self._convert_kraken_user_order(order_id, entry, symbol)

    async def find_order_by_client_id(
        self, client_order_id: str, symbol: str | None = None
    ) -> ExchangeOrderSnapshot | None:
        """Verify whether an order with the given cl_ord_id exists on Kraken.

        Queries open AND closed orders with the ``clientOrderId``
        param, which ccxt maps to Kraken's ``cl_ord_id`` filter on both
        endpoints. ``fetch_order`` is deliberately NOT used: its
        ``clientOrderId`` param maps to the integer ``userref`` field —
        a silent wrong-identifier query for our uuid7 client ids. The
        echoed client id on each result is double-checked so a venue
        ignoring the filter cannot produce a false match. Symbols
        without a CCXT mapping (native-only instruments) verify through
        the native ``kraken.spot.User`` endpoints instead, under the
        same echo and shape strictness.

        Args:
            client_order_id: Client order id the submit was sent with.
            symbol: Optional native symbol to narrow the query.

        Returns:
            The order snapshot when found; None only after BOTH queries
            succeeded and neither contains the client id, so None is an
            authoritative statement of absence.

        Raises:
            RuntimeError: If API credentials are missing.
            Exception: If either venue query fails — the caller must
                treat this as could-not-verify, never as absence.
        """
        if not self.api_key or not self.api_secret:
            raise RuntimeError(_CREDENTIALS_REQUIRED_MSG)
        ccxt_symbol: str | None = None
        if symbol is not None:
            try:
                ccxt_symbol = native_to_ccxt(symbol)
            except ValueError:
                return await self._find_order_by_client_id_native(client_order_id, symbol)
        params = {"clientOrderId": client_order_id}
        for fetch in (self._ccxt_client.fetch_open_orders, self._ccxt_client.fetch_closed_orders):
            orders = await self._with_retry(
                fetch,
                ccxt_symbol,
                None,
                None,
                params,
                retry_network_errors=False,
                egress_kind="private_idempotent_read",
                egress_operation="find_order_by_client_id",
                egress_target=ccxt_proxy_target(self._ccxt_client),
            )
            if not isinstance(orders, list):
                raise RuntimeError(
                    f"Kraken returned a non-list order set for {client_order_id}: "
                    f"cannot answer authoritatively"
                )
            for order_data in orders:
                snapshot = self._convert_ccxt_order(order_data)
                if snapshot.client_order_id == client_order_id:
                    return snapshot
        return None

    async def _find_order_by_client_id_native(
        self, client_order_id: str, symbol: str
    ) -> ExchangeOrderSnapshot | None:
        """Client-id lookup via the native User REST API.

        Fallback for CCXT-unmapped (native-only) symbols, which the
        CCXT path cannot verify. Mirrors its authority contract:
        queries open AND closed orders through ``kraken.spot.User``
        with the ``cl_ord_id`` filter (``extra_params`` merges into the
        signed request body), demands well-formed dict shapes, and
        double-checks the echoed ``cl_ord_id`` on every order — so a
        venue ignoring the filter or a misbehaving transport can never
        read as venue-confirmed absence.

        Args:
            client_order_id: Client order id the submit was sent with.
            symbol: Native symbol of the original request, stamped on
                the returned snapshot — identity is established by the
                echoed client id, and CCXT-unmapped pairs by definition
                have no reverse mapping through the CCXT tables.

        Returns:
            The order snapshot when found; None only after BOTH queries
            succeeded with well-formed shapes and neither contains the
            client id (authoritative absence).

        Raises:
            RuntimeError: If a response shape is not interpretable.
            Exception: If either venue query fails — could-not-verify,
                never absence.
        """
        user_client = self._get_user_client()
        extra_params = {"cl_ord_id": client_order_id}
        for fetch, key in self._native_client_id_queries(user_client):
            response = await self._with_retry(
                fetch,
                extra_params=extra_params,
                retry_network_errors=False,
                egress_kind="private_idempotent_read",
                egress_operation=f"native_find_order_by_client_id_{key}",
                egress_target=spot_sdk_proxy_target(user_client),
            )
            response_dict = cast(dict[str, Any], response)
            orders = self._native_order_set(response_dict, key, client_order_id)
            match = self._matching_native_client_order(orders, key, client_order_id, symbol)
            if match is not None:
                return match
            if key == "closed":
                self._ensure_closed_orders_exhaustive(response_dict, orders, client_order_id)
        return None

    def _native_client_id_queries(
        self, user_client: User
    ) -> tuple[tuple[Callable[..., dict[str, Any]], str], ...]:
        """Given a native user client, when verifying id, then return query order.

        Args:
            user_client: Native Kraken User REST client.

        Returns:
            Open-order query followed by closed-order query.

        Raises:
            This helper does not raise directly.
        """
        return (
            (user_client.get_open_orders, "open"),
            (user_client.get_closed_orders, "closed"),
        )

    @staticmethod
    def _native_order_set(
        response: dict[str, Any], key: str, client_order_id: str
    ) -> dict[str, Any]:
        """Given a native response, when reading one set, then validate its shape.

        Args:
            response: Native Kraken response payload.
            key: Response key being read, either ``open`` or ``closed``.
            client_order_id: Client id used in the query, for diagnostics.

        Returns:
            Mapping of order txid to native order payload.

        Raises:
            RuntimeError: If the response does not contain an order mapping.
        """
        orders = response.get(key) if isinstance(response, dict) else None
        if not isinstance(orders, dict):
            raise RuntimeError(
                f"Kraken returned a malformed {key}-orders set for "
                f"{client_order_id}: cannot answer authoritatively"
            )
        return orders

    def _matching_native_client_order(
        self,
        orders: dict[str, Any],
        key: str,
        client_order_id: str,
        symbol: str,
    ) -> ExchangeOrderSnapshot | None:
        """Given native orders, when scanning, then return the matching client id.

        Args:
            orders: Native order mapping keyed by txid.
            key: Response set name used for diagnostics.
            client_order_id: Client id being verified.
            symbol: Native symbol stamped on the returned snapshot.

        Returns:
            Converted order snapshot when the echoed client id matches, else ``None``.

        Raises:
            RuntimeError: If any native order payload is malformed.
        """
        for txid, payload in orders.items():
            if not isinstance(payload, dict):
                raise RuntimeError(
                    f"Kraken returned a malformed {key} order entry for "
                    f"{client_order_id}: cannot answer authoritatively"
                )
            if payload.get("cl_ord_id") == client_order_id:
                return self._convert_kraken_user_order(str(txid), payload, symbol)
        return None

    @staticmethod
    def _ensure_closed_orders_exhaustive(
        response: dict[str, Any], orders: dict[str, Any], client_order_id: str
    ) -> None:
        """Given closed-order response, when proving absence, then require exhaustiveness.

        Args:
            response: Native closed-orders response payload.
            orders: Closed orders returned in the response.
            client_order_id: Client id being verified.

        Returns:
            None.

        Raises:
            RuntimeError: If the closed-order response may be paged or count-less.
        """
        count = response.get("count")
        if not isinstance(count, int) or count > len(orders):
            raise RuntimeError(
                f"Kraken closed-orders answer for {client_order_id} is not "
                f"provably exhaustive (count={count!r}, rows={len(orders)}): "
                f"a paged or count-less answer that may hide the order "
                f"cannot prove absence"
            )

    async def _fetch_orders_from_exchange(
        self,
        symbol: str | None,
        status: ExchangeOrderStatusEnum | None,
        limit: int | None,
    ) -> list[dict[str, Any]]:
        """Fetch raw order data from exchange via CCXT.

        Args:
            symbol: Filter by trading pair (native format).
            status: If OPEN, fetches only open orders.
            limit: Maximum number of orders to return.

        Returns:
            List of raw CCXT order dictionaries.
        """
        ccxt_symbol = native_to_ccxt(symbol) if symbol else None
        fetch_func = (
            self._ccxt_client.fetch_open_orders
            if status == ExchangeOrderStatusEnum.OPEN
            else self._ccxt_client.fetch_orders
        )
        result: list[dict[str, Any]] = await self._with_retry(
            fetch_func,
            ccxt_symbol,
            None,
            limit,
            retry_network_errors=False,
            egress_kind="private_idempotent_read",
            egress_operation="fetch_orders",
            egress_target=ccxt_proxy_target(self._ccxt_client),
        )
        return result

    async def get_orders(
        self,
        symbol: str | None = None,
        status: ExchangeOrderStatusEnum | None = None,
        limit: int | None = None,
    ) -> list[ExchangeOrderSnapshot]:
        """Fetch multiple orders with optional filtering.

        Args:
            symbol: Filter by trading pair.
            status: Filter by order status (OPEN, CLOSED, etc.).
            limit: Maximum number of orders to return.

        Returns:
            List of order snapshots.

        Raises:
            RuntimeError: If API credentials are missing.
            Exception: If orders fetch fails.
        """
        if not self.api_key or not self.api_secret:
            raise RuntimeError(_CREDENTIALS_REQUIRED_MSG)
        try:
            orders_data = await self._fetch_orders_from_exchange(symbol, status, limit)
            orders = [self._convert_ccxt_order(order) for order in orders_data]
            if status:
                orders = [order for order in orders if order.status == status]
            return orders
        except Exception as e:
            logger.error(f"Failed to get orders: {e}")
            raise

    async def get_balance(self, currency: str | None = None) -> dict[str, AccountBalance]:
        """Fetch account balances.

        Args:
            currency: Filter by specific currency (e.g., 'BTC', 'USD').

        Returns:
            Dictionary of currency to balance info.

        Raises:
            RuntimeError: If API credentials are missing.
            Exception: If balance fetch fails.
        """
        if not self.api_key or not self.api_secret:
            raise RuntimeError("API credentials required for balance")
        try:
            balance_data = await self._with_retry(
                self._ccxt_client.fetch_balance,
                retry_network_errors=False,
                egress_kind="private_idempotent_read",
                egress_operation="fetch_balance",
                egress_target=ccxt_proxy_target(self._ccxt_client),
            )
            balances = {}
            for curr, data in balance_data.items():
                if curr in ["free", "used", "total", "info", "timestamp", "datetime"]:
                    continue
                if not isinstance(data, dict):
                    continue
                balances[curr] = AccountBalance(
                    currency=curr,
                    free=float(data.get("free", 0)),
                    used=float(data.get("used", 0)),
                    total=float(data.get("total", 0)),
                )
            if currency:
                return {
                    currency: balances.get(
                        currency, AccountBalance(currency, float("0"), float("0"), float("0"))
                    )
                }
            return balances
        except Exception as e:
            logger.error(f"Failed to get balance: {e}")
            raise

    def subscribe_ticks(
        self, symbols: list[str], *, req_id: int | None = None
    ) -> AsyncIterator[TickerUpdate]:
        """Subscribe to real-time ticker updates via WebSocket.

        Args:
            symbols: List of symbols or ['*'] for all.
            req_id: Optional request ID for tracking.

        Returns:
            AsyncIterator yielding TickerUpdate for each price change.

        Raises:
            Exception: If subscription fails.
        """
        return self._subscribe_ticks_impl(symbols, req_id=req_id)

    async def _subscribe_ticks_impl(
        self, symbols: list[str], *, req_id: int | None
    ) -> AsyncIterator[TickerUpdate]:
        """Implement WebSocket ticker subscription.

        Args:
            symbols: List of symbols or ['*'] for all.
            req_id: Optional request ID for tracking.

        Yields:
            TickerUpdate for each price change.

        Raises:
            Exception: If subscription fails.
        """
        try:
            await self._ensure_ws_connected()
            assert self._ws_client is not None, _WS_CLIENT_CONNECTED_MSG
            if symbols == ["*"]:
                ws_symbols = ["*"]
            else:
                ws_symbols = [native_to_kraken_websocket(symbol) for symbol in symbols]
            logger.info(f"Subscribing to ticks: {symbols} -> {ws_symbols}")
            async with self._ws_client as ws:
                params = KrakenTickerSubscribeParamsSchema(symbol=ws_symbols).as_params()
                req = SubscriptionRequest(
                    channel="ticker",
                    symbols=tuple(ws_symbols),
                    parameters_json=canonicalise_parameters(cast(dict[str, JsonValue], params)),
                )
                self._subscription_cache[req.key()] = req
                is_wildcard = ws_symbols == ["*"]
                seeded = self._seed_ticker_health(ws_symbols)
                await ws.subscribe(params=params, req_id=req_id)
                logger.info(
                    "Subscribed to {} ticks on kraken/ticker: tracking {} symbol(s) ({})",
                    "wildcard" if is_wildcard else "explicit",
                    seeded,
                    "confirmed; data pending" if is_wildcard else "data/ACK pending",
                )
                while not hasattr(ws, "exception_occur") or not ws.exception_occur:
                    try:
                        message = await asyncio.wait_for(self._tick_queue.get(), timeout=0.1)
                        yield message
                    except TimeoutError:
                        await asyncio.sleep(0.01)
                    except Exception as e:
                        logger.error(f"Error receiving WebSocket message: {e}")
                        break
        except Exception as e:
            logger.error(f"WebSocket tick subscription error: {e}")
            raise

    def _seed_ticker_health(self, ws_symbols: list[str]) -> int:
        """Seed ticker health entries for the subscribed universe.

        The ``ticker`` channel is the only Kraken Spot channel that accepts
        the server-side ``["*"]`` wildcard, so the wildcard path never lists
        the individual symbols it covers. Before this seeding a symbol inside
        the wildcard universe that never streamed a frame had NO tracker entry
        and NO log line, leaving it invisible to both the background health
        loop and the persisted ``instrument_feed_health`` table.

        The wildcard and explicit paths seed DIFFERENT statuses because the
        subscription mechanics differ:

        - Wildcard ``["*"]`` has NO per-symbol ACK; the single ``["*"]``
          subscribe IS the subscription, so each resolved symbol is already
          "confirmed/subscribed" at seed time. Seeding them ``confirmed``
          means a symbol that never streams data is surfaced by
          :meth:`SubscriptionHealthTracker.list_stale_data` (dark detection,
          log-once) and persisted via the feed-health snapshot, but is NEVER
          pending-retried — so no false per-symbol re-subscribe traffic and
          no false ``failed`` state. These entries are seeded
          ``dark_recovery_enabled=False`` so dark auto-recovery also skips
          them: there is no per-symbol ticker subscription to re-issue under
          the wildcard, and the wildcard itself is replayed on reconnect.
          The literal ``"*"`` sentinel is never
          inserted; only the resolved wire symbols are. The universe is
          resolved from :func:`get_available_kraken_symbols` (the same
          market-data-capable set the trade and ohlc channels expand ``["*"]``
          to) and converted to wire format, matching the keys used by
          ``mark_data_seen`` so a later frame keeps the entry fresh.
        - Explicit (non-wildcard) subscribes are real per-symbol subscribes
          confirmed by per-symbol ACKs, so they seed ``pending`` and are
          confirmed (or failed) by the ACK handler as before.

        Args:
            ws_symbols: Wire-format symbols passed to the subscribe call, or
                the literal ``["*"]`` wildcard sentinel.

        Returns:
            Number of ticker entries seeded.
        """
        if ws_symbols == ["*"]:
            universe = [
                native_to_kraken_websocket(symbol) for symbol in get_available_kraken_symbols()
            ]
            for ws_symbol in universe:
                self._health_tracker.mark_confirmed(
                    "ticker", ws_symbol, dark_recovery_enabled=False
                )
            return len(universe)
        for ws_symbol in ws_symbols:
            self._health_tracker.mark_pending("ticker", ws_symbol)
        return len(ws_symbols)

    async def collect_raw_ticker_symbols(self, window_seconds: float) -> dict[str, dict[str, Any]]:
        """Capture raw ticker frames for symbol-updater discovery.

        Subscribes to the wildcard ticker channel ``["*"]`` and records
        the first raw frame seen for each distinct exchange symbol over
        a fixed wall-clock window. The capture runs alongside the
        normal parse pipeline; ``_handle_ticker_data`` records the raw
        symbol BEFORE ``parse_kraken_ticker_list`` filters out frames
        whose ``symbol`` is not mapped to a native form. This is the
        only way to discover product-type listings such as
        ``BTC/USD:BTNL`` (Kraken Bitnomial perpetuals) that are absent
        from REST ``get_asset_pairs`` and CCXT markets.

        Args:
            window_seconds: Wall-clock duration to keep the subscription
                open. Sized 60-120s in the symbol updater because BTNL
                ticker updates are 5-100x less frequent than spot
                updates, so shorter windows miss several pairs.

        Returns:
            Dict mapping the raw exchange symbol string to the first
            ticker frame dict observed for that symbol.

        Raises:
            Exception: If the WebSocket subscription itself fails.
        """
        capture: dict[str, dict[str, Any]] = {}
        previous_capture = self._raw_ticker_capture
        self._raw_ticker_capture = capture
        progress_interval = 15.0
        try:
            await self._ensure_ws_connected()
            assert self._ws_client is not None, _WS_CLIENT_CONNECTED_MSG
            async with self._ws_client as ws:
                params = KrakenTickerSubscribeParamsSchema(symbol=["*"]).as_params()
                await ws.subscribe(params=params)
                logger.info(
                    "Raw ticker capture: subscribed to wildcard, holding {:.0f}s "
                    "to observe low-frequency symbols",
                    window_seconds,
                )
                elapsed = 0.0
                while elapsed < window_seconds:
                    step = min(progress_interval, window_seconds - elapsed)
                    await asyncio.sleep(step)
                    elapsed += step
                    if elapsed < window_seconds:
                        logger.info(
                            "Raw ticker capture: {} symbols seen so far ({:.0f}/{:.0f}s)",
                            len(capture),
                            elapsed,
                            window_seconds,
                        )
        finally:
            self._raw_ticker_capture = previous_capture
        logger.info(
            "Raw ticker capture complete: {} distinct symbols in {:.0f}s",
            len(capture),
            window_seconds,
        )
        return capture

    def subscribe_candles(
        self,
        symbols: list[str],
        timeframe: str = "1m",
        *,
        req_id: int | None = None,
    ) -> AsyncIterator[CandleUpdate]:
        """Subscribe to real-time OHLC candle updates.

        Args:
            symbols: List of symbols or ['*'] for all.
            timeframe: Candle interval (e.g., '1m', '1h').
            req_id: Optional request ID.

        Returns:
            AsyncIterator yielding CandleUpdate for each candle change.

        Raises:
            Exception: If subscription fails.
        """
        return self._subscribe_candles_impl(symbols, timeframe=timeframe, req_id=req_id)

    async def _subscribe_candles_impl(
        self,
        symbols: list[str],
        *,
        timeframe: str,
        req_id: int | None,
    ) -> AsyncIterator[CandleUpdate]:
        """Implement WebSocket candle subscription.

        Args:
            symbols: List of symbols or ['*'] for all.
            timeframe: Candle interval (e.g., '1m', '1h').
            req_id: Optional request ID.

        Yields:
            CandleUpdate for each candle change.

        Raises:
            Exception: If subscription fails.
        """
        try:
            await self._ensure_ws_connected()
            ws_client = self._connected_ws_client()
            native_syms = self._expand_spot_symbols(
                symbols,
                channel_label=f"{timeframe} candles",
                wildcard_note="Kraken Spot ohlc channel does not accept '*'",
            )
            ws_symbols = [native_to_kraken_websocket(symbol) for symbol in native_syms]
            interval = self._candle_interval(timeframe)
            channel_key = f"ohlc:{interval_to_label(interval)}"
            self._ensure_candle_queue(interval)
            logger.info(
                f"Subscribing to {timeframe} candles: {len(ws_symbols)} symbols in "
                f"{self._chunk_count(len(ws_symbols), _CANDLE_SUBSCRIBE_CHUNK_SIZE)} "
                f"chunks of <={_CANDLE_SUBSCRIBE_CHUNK_SIZE}"
            )
            async with ws_client as ws:
                await self._subscribe_candle_chunks(ws, ws_symbols, interval, channel_key, req_id)
                logger.info(
                    "Subscribed to {} candles on kraken/{}: "
                    "tracking {} symbol(s) (data/ACK pending)",
                    timeframe,
                    channel_key,
                    len(ws_symbols),
                )
                async for message in self._iter_candle_messages(ws, interval):
                    yield message
        except Exception as e:
            logger.error(f"WebSocket candle subscription error: {e}")
            raise

    def subscribe_trade_built_candles(
        self,
        symbols: list[str],
        timeframe: str = "1m",
    ) -> AsyncIterator[CandleUpdate]:
        """Stream trade-built 1m candles synthesized from the spot trade feed.

        Phase-4 SHADOW path: spot 1m candles built locally from the
        already-consumed trade stream (mirroring Kraken futures/equities),
        for a live A/B against the venue-native ``ohlc:1m``. This NEVER
        subscribes to Kraken again and NEVER touches the native candle
        queues (:attr:`_candle_queues`); it only starts the local aggregator
        and yields completed buckets from the dedicated trade-built queue.

        Args:
            symbols: Native symbols (advisory; the builder emits a candle for
                every symbol whose trades it has actually folded).
            timeframe: Candle interval. Only ``"1m"`` is supported.

        Returns:
            AsyncIterator yielding ``CandleUpdate`` for each completed
            1-minute trade-built bucket.

        Raises:
            ValueError: When ``timeframe`` is not ``"1m"``.
        """
        return self._subscribe_trade_built_candles_impl(symbols, timeframe)

    async def _subscribe_trade_built_candles_impl(
        self,
        symbols: list[str],
        timeframe: str,
    ) -> AsyncIterator[CandleUpdate]:
        """Yield trade-built 1m candles from the dedicated shadow queue.

        Starts the background :meth:`_trade_built_candle_aggregator` task on
        first call and cancels it cleanly on iterator close so a publisher
        shutdown does not leak the task.

        Args:
            symbols: Native symbols (advisory; see
                :meth:`subscribe_trade_built_candles`).
            timeframe: Candle interval. Must be ``"1m"``.

        Yields:
            ``CandleUpdate`` for each completed 1-minute trade-built bucket.

        Raises:
            ValueError: When ``timeframe`` is not ``"1m"``.
        """
        if timeframe != "1m":
            raise ValueError(
                f"Kraken spot trade-built candles only support 1m "
                f"(synthesized from trades). Got {timeframe!r}."
            )
        self._trade_built_candles_enabled = True
        if (
            self._trade_built_candle_aggregator_task is None
            or self._trade_built_candle_aggregator_task.done()
        ):
            self._trade_built_candle_aggregator_task = asyncio.create_task(
                self._trade_built_candle_aggregator()
            )
        try:
            while True:
                try:
                    candle = await asyncio.wait_for(
                        self._trade_built_candle_queue.get(), timeout=0.1
                    )
                    yield candle
                except TimeoutError:
                    await asyncio.sleep(0.01)
        finally:
            self._trade_built_candles_enabled = False
            task = self._trade_built_candle_aggregator_task
            if task and not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    async def _trade_built_candle_aggregator(self) -> None:
        """Emit completed trade-built 1m candles roughly once per second.

        Wakes every second, asks :attr:`_trade_built_candle_builder` for any
        bucket whose minute has finished beyond the configured late-trade
        grace, and routes each into :attr:`_trade_built_candle_queue`.

        A failing tick (e.g. a transient settings read) is logged with its
        traceback and swallowed rather than propagated: the aggregator runs as
        an unsupervised task, so an unhandled exception would silently kill it
        and stall Spot candle production while trades keep flowing (the
        2026-06-23 trade-built stall). ``asyncio.CancelledError`` is a
        ``BaseException`` and is therefore NOT caught here, so a publisher
        shutdown can still cancel the task cleanly.
        """
        while True:
            await asyncio.sleep(1.0)
            try:
                cutoff = datetime.now(UTC) - timedelta(
                    seconds=self.settings.trade_built_finalize_grace_seconds
                )
                for candle in self._trade_built_candle_builder.pop_completed(cutoff):
                    enqueue_or_drop_oldest_candle(self._trade_built_candle_queue, candle, "Candle")
            except Exception:
                logger.exception(
                    "trade-built candle aggregator tick failed; continuing so a "
                    "transient fault does not silently stall Spot candle production"
                )

    def _expand_spot_symbols(
        self, symbols: list[str], *, channel_label: str, wildcard_note: str
    ) -> list[str]:
        """Expand Kraken Spot wildcard subscriptions when the channel needs it.

        Args:
            symbols: Native symbol list, possibly ``["*"]``.
            channel_label: Human-readable channel label for the log line.
            wildcard_note: Operational note explaining why expansion is required.

        Returns:
            Native symbols to convert and subscribe.
        """
        if symbols != ["*"]:
            return symbols
        native_syms = get_available_kraken_symbols()
        logger.info(
            "Subscribing to {}: wildcard expansion -> {} native symbols ({})",
            channel_label,
            len(native_syms),
            wildcard_note,
        )
        return native_syms

    @staticmethod
    def _candle_interval(timeframe: str) -> int:
        """Return Kraken OHLC interval minutes for a Snapper timeframe.

        Args:
            timeframe: Snapper candle timeframe.

        Returns:
            Kraken OHLC interval minutes.
        """
        interval_map = {
            "1m": 1,
            "5m": 5,
            "15m": 15,
            "30m": 30,
            "1h": 60,
            "4h": 240,
            "1d": 1440,
        }
        return interval_map.get(timeframe, 1)

    @staticmethod
    def _chunk_count(total: int, chunk_size: int) -> int:
        """Return the number of chunks needed for ``total`` items.

        Args:
            total: Number of items to chunk.
            chunk_size: Maximum items per chunk.

        Returns:
            Number of chunks.
        """
        return (total + chunk_size - 1) // chunk_size

    def _ensure_candle_queue(self, interval: int) -> None:
        """Create the interval candle queue if it does not exist.

        Args:
            interval: Kraken OHLC interval minutes.

        Returns:
            None.
        """
        if interval not in self._candle_queues:
            self._candle_queues[interval] = asyncio.Queue(maxsize=_TICK_QUEUE_MAX_SIZE)

    def _connected_ws_client(self) -> SpotWSClient:
        """Return the active WebSocket client or raise.

        Returns:
            Active Kraken Spot WebSocket client.

        Raises:
            RuntimeError: If the WebSocket client is not connected.
        """
        if self._ws_client is None:
            raise RuntimeError(_WS_CLIENT_CONNECTED_MSG)
        return self._ws_client

    def _cache_subscription(
        self,
        channel: Literal["ticker", "trade", "ohlc", "book"],
        symbols: list[str],
        params: dict[str, JsonValue],
    ) -> None:
        """Cache one subscription request for reconnect replay.

        Args:
            channel: Kraken channel name.
            symbols: Wire-format symbols for this request.
            params: Canonical subscription parameters.

        Returns:
            None.
        """
        req = SubscriptionRequest(
            channel=channel,
            symbols=tuple(symbols),
            parameters_json=canonicalise_parameters(params),
        )
        self._subscription_cache[req.key()] = req

    async def _subscribe_candle_chunks(
        self,
        ws: SpotWSClient,
        ws_symbols: list[str],
        interval: int,
        channel_key: str,
        req_id: int | None,
    ) -> None:
        """Subscribe to OHLC symbols in bounded chunks.

        Args:
            ws: Active Kraken Spot WebSocket client.
            ws_symbols: Kraken wire-format symbols.
            interval: Kraken OHLC interval minutes.
            channel_key: Health tracker channel key for this interval.
            req_id: Optional request id for the first chunk.

        Returns:
            None.
        """
        for index in range(0, len(ws_symbols), _CANDLE_SUBSCRIBE_CHUNK_SIZE):
            chunk = ws_symbols[index : index + _CANDLE_SUBSCRIBE_CHUNK_SIZE]
            params = cast(
                dict[str, JsonValue],
                KrakenOhlcSubscribeParamsSchema(symbol=chunk, interval=interval).as_params(),
            )
            self._cache_subscription("ohlc", chunk, params)
            for ws_symbol in chunk:
                self._health_tracker.mark_pending(channel_key, ws_symbol)
            chunk_req_id = req_id if index == 0 else None
            await ws.subscribe(params=params, req_id=chunk_req_id)
            if index + _CANDLE_SUBSCRIBE_CHUNK_SIZE < len(ws_symbols):
                await asyncio.sleep(_CANDLE_SUBSCRIBE_CHUNK_DELAY_S)

    async def _iter_candle_messages(
        self, ws: SpotWSClient, interval: int
    ) -> AsyncIterator[CandleUpdate]:
        """Yield queued candle messages while the WebSocket is healthy.

        Args:
            ws: Active Kraken Spot WebSocket client.
            interval: Kraken OHLC interval minutes.

        Yields:
            Candle updates from the interval queue.
        """
        while not self._ws_exception_occurred(ws):
            try:
                message = await asyncio.wait_for(self._candle_queues[interval].get(), timeout=0.1)
                yield message
            except TimeoutError:
                await asyncio.sleep(0.01)
            except Exception as e:
                logger.error(f"Error receiving WebSocket message: {e}")
                break

    @staticmethod
    def _ws_exception_occurred(ws: SpotWSClient) -> bool:
        """Return whether the Kraken SDK WebSocket reports an exception.

        Args:
            ws: Kraken Spot WebSocket client.

        Returns:
            True when ``exception_occur`` exists and is truthy.
        """
        return bool(getattr(ws, "exception_occur", False))

    def subscribe_trades(
        self, symbols: list[str], *, req_id: int | None = None
    ) -> AsyncIterator[TradeUpdate]:
        """Subscribe to real-time trade updates.

        Args:
            symbols: List of symbols or ['*'] for all.
            req_id: Optional request ID.

        Returns:
            AsyncIterator yielding TradeUpdate for each trade execution.

        Raises:
            Exception: If subscription fails.
        """
        return self._subscribe_trades_impl(symbols, req_id=req_id)

    async def _subscribe_trades_impl(
        self, symbols: list[str], *, req_id: int | None
    ) -> AsyncIterator[TradeUpdate]:
        """Implement WebSocket trades subscription.

        Kraken Spot WS v2 caveat (live-validated 2026-05-12): the
        ``trade`` channel does NOT accept the ``["*"]`` wildcard
        accepted by the ``ticker`` channel. Sending ``["*"]`` results
        in a synchronous subscribe-ack with
        ``success: false`` and
        ``error: "Currency pair not in ISO 4217-A3 format *"``, and no
        trade frames ever arrive. The ticker wildcard path is left
        untouched because it works server-side; the trade path expands
        ``["*"]`` to the full Kraken-spot native-symbol catalog via
        :func:`get_available_kraken_symbols` and chunks the subscribe
        at :data:`_TRADE_SUBSCRIBE_CHUNK_SIZE` to keep individual
        frames within the WS server's accepted size envelope.

        Args:
            symbols: List of native symbols or ``["*"]`` for all
                Kraken-spot symbols loaded by the mapper.
            req_id: Optional request ID (passed to the first chunk
                only; subsequent chunks use the same WS connection).

        Yields:
            TradeUpdate for each trade execution.

        Raises:
            Exception: If subscription fails.
        """
        try:
            await self._ensure_ws_connected()
            ws_client = self._connected_ws_client()
            native_syms = self._expand_spot_symbols(
                symbols,
                channel_label="trades",
                wildcard_note="Kraken Spot trade channel does not accept '*'",
            )
            ws_symbols = [native_to_kraken_websocket(symbol) for symbol in native_syms]
            logger.info(
                f"Subscribing to trades: {len(ws_symbols)} symbols in "
                f"{self._chunk_count(len(ws_symbols), _TRADE_SUBSCRIBE_CHUNK_SIZE)} "
                f"chunks of <={_TRADE_SUBSCRIBE_CHUNK_SIZE}"
            )
            async with ws_client as ws:
                await self._subscribe_trade_chunks(ws, ws_symbols, req_id)
                logger.info(
                    "Subscribed to trades on kraken/trade: "
                    "tracking {} symbol(s) (data/ACK pending)",
                    len(ws_symbols),
                )
                async for message in self._iter_trade_messages(ws):
                    yield message
        except Exception as e:
            logger.error(f"WebSocket trade subscription error: {e}")
            raise

    async def _subscribe_trade_chunks(
        self, ws: SpotWSClient, ws_symbols: list[str], req_id: int | None
    ) -> None:
        """Subscribe to trade symbols in bounded chunks.

        Args:
            ws: Active Kraken Spot WebSocket client.
            ws_symbols: Kraken wire-format symbols.
            req_id: Optional request id for the first chunk.

        Returns:
            None.
        """
        for index in range(0, len(ws_symbols), _TRADE_SUBSCRIBE_CHUNK_SIZE):
            chunk = ws_symbols[index : index + _TRADE_SUBSCRIBE_CHUNK_SIZE]
            params = cast(
                dict[str, JsonValue],
                KrakenTradeSubscribeParamsSchema(symbol=chunk).as_params(),
            )
            self._cache_subscription("trade", chunk, params)
            for ws_symbol in chunk:
                self._health_tracker.mark_pending("trade", ws_symbol)
            chunk_req_id = req_id if index == 0 else None
            await ws.subscribe(params=params, req_id=chunk_req_id)
            if index + _TRADE_SUBSCRIBE_CHUNK_SIZE < len(ws_symbols):
                await asyncio.sleep(_TRADE_SUBSCRIBE_CHUNK_DELAY_S)

    async def _iter_trade_messages(self, ws: SpotWSClient) -> AsyncIterator[TradeUpdate]:
        """Yield queued trade messages while the WebSocket is healthy.

        Args:
            ws: Active Kraken Spot WebSocket client.

        Yields:
            Trade updates from the producer queue.
        """
        while not self._ws_exception_occurred(ws):
            try:
                message = await asyncio.wait_for(self._trade_queue.get(), timeout=0.1)
                yield message
            except TimeoutError:
                await asyncio.sleep(0.01)
            except Exception as e:
                logger.error(f"Error receiving WebSocket trade message: {e}")
                break

    def subscribe_executions(
        self,
        *,
        snap_orders: bool | None = False,
        snap_trades: bool | None = False,
        order_status: bool | None = True,
        ratecounter: bool | None = None,
        users: Literal["all"] | None = None,
        req_id: int | None = None,
    ) -> AsyncIterator[ExecutionUpdate]:
        """Subscribe to user execution reports (private channel).

        Snapshots default to ``False`` (sent as explicit ``false`` — the
        params schema excludes only ``None``): startup state is owned by
        the executor's recovery (watermark-anchored seeding plus recovery
        correctives in ``_recover_pending_orders``) and the steady-state
        recon loop. A snapshot replayed on subscribe — or on every SDK
        in-budget reconnect, which re-sends the cached subscription
        verbatim — would race those correctives and re-deliver
        already-accounted executions into the fill pipeline.

        Args:
            snap_orders: Include order snapshot on subscribe.
            snap_trades: Include trade snapshot on subscribe.
            order_status: Include order status updates.
            ratecounter: Enable rate counter.
            users: Subscribe to all users (for sub-accounts).
            req_id: Optional request ID.

        Returns:
            AsyncIterator yielding ExecutionUpdate for each order fill or status change.

        Raises:
            RuntimeError: If API credentials are missing.
            Exception: If subscription fails.
        """
        return self._subscribe_executions_impl(
            snap_orders=snap_orders,
            snap_trades=snap_trades,
            order_status=order_status,
            ratecounter=ratecounter,
            users=users,
            req_id=req_id,
        )

    async def _subscribe_executions_impl(
        self,
        *,
        snap_orders: bool | None,
        snap_trades: bool | None,
        order_status: bool | None,
        ratecounter: bool | None,
        users: Literal["all"] | None,
        req_id: int | None,
    ) -> AsyncIterator[ExecutionUpdate]:
        """Implement private execution reports subscription.

        Args:
            snap_orders: Include order snapshot on subscribe.
            snap_trades: Include trade snapshot on subscribe.
            order_status: Include order status updates.
            ratecounter: Enable rate counter.
            users: Subscribe to all users (for sub-accounts).
            req_id: Optional request ID.

        Yields:
            ExecutionUpdate for each order fill or status change.

        Raises:
            RuntimeError: If API credentials are missing.
            ConnectionError: If the private WebSocket connection is lost
                (the SDK's in-client reconnect budget is exhausted and
                ``exception_occur`` is terminal). The poisoned client is
                closed and its slot compare-and-cleared before the raise
                propagates, so the next ``_ensure_ws_connected`` builds a
                fresh client instead of trusting a dead one.
            Exception: If subscription fails.
        """
        if not self.api_key or not self.api_secret:
            raise RuntimeError("API credentials required for executions subscription")
        try:
            await self._ensure_ws_connected()
            subscribed_client = self._ws_client
            assert subscribed_client is not None, "WebSocket client should be started"
            logger.info("Subscribing to executions (private channel)")
            subscribe_params = KrakenExecutionSubscribeParamsSchema(
                snap_orders=snap_orders,
                snap_trades=snap_trades,
                order_status=order_status,
                ratecounter=ratecounter,
                users=users,
                reqid=req_id,
            ).as_params()
            try:
                async with asyncio.timeout(_SDK_SEND_TIMEOUT_S):
                    await subscribed_client.subscribe(params=subscribe_params, req_id=req_id)
            except TimeoutError:
                if self._ws_client is subscribed_client:
                    await self._close_ws_client()
                raise
            try:
                while not self._ws_exception_occurred(subscribed_client):
                    try:
                        message = await asyncio.wait_for(self._execution_queue.get(), timeout=0.1)
                        yield message
                    except TimeoutError:
                        await asyncio.sleep(0.01)
                    except Exception as e:
                        logger.error(f"Error receiving WebSocket execution message: {e}")
                        break
                if self._ws_exception_occurred(subscribed_client):
                    raise ConnectionError("Kraken spot private WS connection lost")
            finally:
                if self._ws_client is subscribed_client and self._ws_exception_occurred(
                    subscribed_client
                ):
                    await self._close_ws_client()
        except Exception as e:
            logger.error(f"WebSocket executions subscription error: {e}")
            raise

    def subscribe_instruments(self, **kwargs: Any) -> AsyncIterator[dict[str, Any]]:
        """Subscribe to instrument/pair updates.

        Args:
            **kwargs: Options including req_id and raw (bool).

        Returns:
            AsyncIterator yielding instrument dictionaries.
        """
        req_id = kwargs.get("req_id")
        raw = kwargs.get("raw", False)
        return self._subscribe_instruments_impl(req_id=req_id, raw=raw)

    async def _subscribe_instruments_impl(
        self, *, req_id: int | None = None, raw: bool = False
    ) -> AsyncIterator[dict[str, Any]]:
        try:
            await self._ensure_ws_connected()
            assert self._ws_client is not None, _WS_CLIENT_CONNECTED_MSG
            logger.info("Subscribing to instruments (public channel, raw={})", raw)
            async with self._ws_client as ws:
                params = KrakenInstrumentSubscribeParamsSchema(snapshot=True).as_params()
                await ws.subscribe(params=params, req_id=req_id)
                if raw:
                    queue: asyncio.Queue[Any] = self._raw_instrument_queue
                else:
                    queue = self._instrument_queue
                while not hasattr(ws, "exception_occur") or not ws.exception_occur:
                    try:
                        message = await asyncio.wait_for(queue.get(), timeout=0.1)
                        if raw:
                            yield message
                        else:
                            from dataclasses import asdict

                            yield asdict(message)
                    except TimeoutError:
                        await asyncio.sleep(0.01)
                    except Exception as e:
                        logger.error(f"Error receiving WebSocket instrument message: {e}")
                        break
        except Exception as e:
            logger.error(f"WebSocket instrument subscription error: {e}")
            raise

    async def _on_message(self, message: dict[str, Any] | list[Any]) -> None:
        """Route incoming WebSocket messages to appropriate handlers.

        Dispatches based on message structure:
        - Order request responses (req_id present)
        - Subscription acknowledgements (method=subscribe)
        - Channel data messages (channel + data present)

        Args:
            message: Raw WebSocket message (dict or list).
        """
        try:
            if not isinstance(message, dict):
                return
            if message.get("method") == "subscribe" and isinstance(message.get("result"), dict):
                self._handle_subscription_ack(message)
                return
            if "channel" in message and "data" in message:
                await self._handle_channel_data(message)
        except Exception as e:
            logger.error(f"Error processing WebSocket message: {e}")

    def _handle_subscription_ack(self, message: dict[str, Any]) -> None:
        """Process subscription acknowledgement messages by channel type.

        Args:
            message: WebSocket subscription ack message.
        """
        result_dict = cast(dict[str, Any], message["result"])
        channel_name = result_dict.get("channel")
        ack_handlers: dict[str, Callable[..., None]] = {
            "ticker": lambda: self._handle_ticker_subscription_ack(message, result_dict),
            "trade": lambda: self._handle_trade_subscription_ack(message, result_dict),
            "executions": lambda: self._handle_execution_subscription_ack(message),
            "ohlc": lambda: self._handle_ohlc_subscription_ack(message),
        }
        handler = ack_handlers.get(channel_name or "")
        if handler:
            handler()

    def _handle_ticker_subscription_ack(
        self, message: dict[str, Any], result_dict: dict[str, Any]
    ) -> None:
        """Track ticker subscription acknowledgement result.

        Args:
            message: Full subscription ack message.
            result_dict: The result sub-dict from the ack.
        """
        try:
            ticker_ack = KrakenTickerSubscriptionAckSchema.model_validate(message)
            symbol = result_dict.get("symbol")
            if not isinstance(symbol, str):
                logger.debug("Received ticker control message without symbol: {}", message)
                return
            if symbol == "*":
                if _subscription_ack_confirms(ticker_ack.success, ticker_ack.error):
                    logger.debug(
                        "Ignoring confirming wildcard ticker ack; universe health is owned "
                        "by _seed_ticker_health and the literal '*' is never tracked: {}",
                        message,
                    )
                else:
                    logger.warning(
                        "Wildcard ticker subscribe failed (error={}); universe health is owned "
                        "by _seed_ticker_health, the literal '*' is not tracked",
                        ticker_ack.error or _UNKNOWN_SUBSCRIPTION_ERROR,
                    )
                return
            if _subscription_ack_confirms(ticker_ack.success, ticker_ack.error):
                self._health_tracker.mark_confirmed("ticker", symbol)
                return
            error = ticker_ack.error or _UNKNOWN_SUBSCRIPTION_ERROR
            self._health_tracker.mark_failed("ticker", symbol, error)
            logger.warning(
                "Ticker subscription failed symbol={} error={}",
                symbol,
                ticker_ack.error,
            )
        except ValidationError:
            logger.debug(
                "Received non-standard ticker control message: {}",
                message,
            )

    def _handle_trade_subscription_ack(
        self, message: dict[str, Any], result_dict: dict[str, Any]
    ) -> None:
        """Log trade subscription acknowledgement result.

        Args:
            message: Full subscription ack message.
            result_dict: The result sub-dict from the ack.
        """
        try:
            trade_ack = KrakenTradeSubscriptionAckSchema.model_validate(message)
            symbol = result_dict.get("symbol")
            if not isinstance(symbol, str):
                logger.debug("Received trade control message without symbol: {}", message)
                return
            if _subscription_ack_confirms(trade_ack.success, trade_ack.error):
                self._health_tracker.mark_confirmed("trade", symbol)
                return
            error = trade_ack.error or _UNKNOWN_SUBSCRIPTION_ERROR
            self._health_tracker.mark_failed("trade", symbol, error)
            logger.warning(
                "Trade subscription failed symbol={} error={}",
                symbol,
                trade_ack.error,
            )
        except ValidationError:
            logger.debug(
                "Received non-standard trade control message: {}",
                message,
            )

    def _handle_execution_subscription_ack(self, message: dict[str, Any]) -> None:
        """Log execution subscription acknowledgement result.

        Args:
            message: Full subscription ack message.
        """
        try:
            execution_ack = KrakenExecutionSubscriptionAckSchema.model_validate(message)
            if not execution_ack.success:
                logger.warning(
                    "Executions subscription failed (orders={} trades={}) error={}",
                    execution_ack.result.snap_orders,
                    execution_ack.result.snap_trades,
                    execution_ack.error,
                )
            if execution_ack.warnings:
                for warning in execution_ack.warnings:
                    logger.warning("Execution subscription warning: {}", warning)
        except ValidationError:
            logger.debug(
                "Received non-standard executions control message: {}",
                message,
            )

    def _handle_ohlc_subscription_ack(self, message: dict[str, Any]) -> None:
        """Log OHLC subscription acknowledgement result.

        Args:
            message: Full subscription ack message.
        """
        try:
            ohlc_ack = KrakenOhlcSubscriptionAckSchema.model_validate(message)
            interval = ohlc_ack.result.interval
            if interval is None:
                logger.debug("Received OHLC control message without interval: {}", message)
                return
            channel_key = f"ohlc:{interval_to_label(interval)}"
            if _subscription_ack_confirms(ohlc_ack.success, ohlc_ack.error):
                self._health_tracker.mark_confirmed(channel_key, ohlc_ack.result.symbol)
            else:
                error = ohlc_ack.error or _UNKNOWN_SUBSCRIPTION_ERROR
                self._health_tracker.mark_failed(channel_key, ohlc_ack.result.symbol, error)
                logger.warning(
                    "OHLC subscription failed symbol={} interval={} error={}",
                    ohlc_ack.result.symbol,
                    ohlc_ack.result.interval,
                    ohlc_ack.error,
                )
            if ohlc_ack.result.warnings:
                for warning in ohlc_ack.result.warnings:
                    self._log_ohlc_subscription_warning(warning)
        except ValidationError:
            logger.debug(
                "Received non-standard OHLC control message: {}",
                message,
            )

    def _log_ohlc_subscription_warning(self, warning: str) -> None:
        """Log an OHLC subscription warning with deduplication of known notices.

        Kraken emits a fixed informational notice
        ``timestamp is deprecated, use interval_begin`` on every OHLC
        subscribe. We already use ``interval_begin`` exclusively, so the
        notice is not operator-actionable. Per the clean-signal-log rule
        we log the first occurrence at INFO with an explanation, then
        downgrade subsequent occurrences to DEBUG so the warning stream
        stays meaningful. Unknown warnings remain at WARNING.

        Args:
            warning: Warning string from ``result.warnings`` in the
                OHLC subscription ACK.
        """
        if warning == _OHLC_DEPRECATED_TIMESTAMP_NOTICE:
            if warning not in _OHLC_NOTICES_LOGGED:
                logger.info(
                    "Kraken OHLC ack carries informational notice "
                    "'timestamp is deprecated, use interval_begin' — we "
                    "already use interval_begin; subsequent occurrences "
                    "suppressed to DEBUG"
                )
                _OHLC_NOTICES_LOGGED.add(warning)
            else:
                logger.debug("OHLC subscription notice: {}", warning)
            return
        logger.warning(f"OHLC subscription warning: {warning}")

    async def _handle_channel_data(self, message_dict: dict[str, Any]) -> None:
        """Dispatch channel data messages to channel-specific handlers.

        Args:
            message_dict: WebSocket message with channel and data fields.
        """
        channel = message_dict["channel"]
        data = message_dict["data"]
        msg_type = message_dict.get("type")
        if channel in {"ticker", "trade"} and msg_type == "snapshot":
            logger.debug(
                "Skipping {} snapshot frame because it is a replay artifact",
                channel,
            )
            return
        data_handlers: dict[str, Callable[[Any], None]] = {
            "ohlc": self._handle_ohlc_data,
            "ticker": self._handle_ticker_data,
            "trade": self._handle_trade_data,
            "executions": self._handle_executions_data,
        }
        handler = data_handlers.get(channel)
        if handler:
            handler(data)
        elif channel == "instrument":
            await self._handle_instrument_data(message_dict)

    def _handle_ohlc_data(self, data: Any) -> None:
        """Parse and enqueue OHLC candle data.

        Args:
            data: Raw OHLC data from WebSocket.
        """
        try:
            self._mark_ohlc_health_seen(data)
            candle_list = parse_kraken_candle_list(data) if isinstance(data, list) else []
            for candle_data in candle_list:
                self._enqueue_candle_data(candle_data)
        except ValueError as e:
            logger.warning(f"Failed to parse candle data: {e}")

    def _mark_ohlc_health_seen(self, data: Any) -> None:
        """Mark OHLC subscriptions as live from raw frames.

        Args:
            data: Raw OHLC data from WebSocket.

        Returns:
            None.
        """
        if not isinstance(data, list):
            return
        for item in data:
            self._mark_ohlc_frame_health(item)

    def _mark_ohlc_frame_health(self, item: object) -> None:
        """Mark one OHLC frame as seen when it carries symbol and interval.

        Args:
            item: Raw frame item from the WebSocket payload.

        Returns:
            None.
        """
        if not isinstance(item, dict):
            return
        wire_symbol = item.get("symbol")
        raw_interval = item.get("interval")
        if not isinstance(wire_symbol, str) or not isinstance(raw_interval, int):
            return
        try:
            channel_key = f"ohlc:{interval_to_label(raw_interval)}"
            self._health_tracker.mark_data_seen(channel_key, wire_symbol)
        except ValueError:
            logger.debug("Skipping OHLC health mark for interval {}", raw_interval)

    def _enqueue_candle_data(self, candle_data: CandleUpdate) -> None:
        """Enqueue one parsed candle to the matching queues.

        Args:
            candle_data: Parsed candle update.

        Returns:
            None.
        """
        queue = self._candle_queues.get(candle_data.interval)
        if queue is not None:
            _enqueue_or_drop_oldest(queue, candle_data, "candle")
            return
        for fallback_queue in self._candle_queues.values():
            _enqueue_or_drop_oldest(fallback_queue, candle_data, "candle")

    def _handle_ticker_data(self, data: Any) -> None:
        """Parse and enqueue ticker data.

        When ``_raw_ticker_capture`` is set (by
        :meth:`collect_raw_ticker_symbols`), every wire symbol in the
        batch is recorded into the capture dict and the parse pipeline
        is SKIPPED. Skipping parse during capture avoids two side
        effects that are inappropriate when the client is being driven
        by the symbol updater (the only intended caller of
        ``collect_raw_ticker_symbols``):

        * The mapper is intentionally stale during the discovery
          window — ``_update_database`` has not yet committed the
          freshly discovered aliases — so ``parse_kraken_ticker_list``
          would warn ``Unknown Kraken WebSocket v2 symbol`` for every
          new pair Kraken added since the last refresh, even though
          those pairs are about to be registered in the same updater
          run. Skipping parse keeps those harmless first-sight
          warnings out of the logs.
        * No publisher consumes ``_tick_queue`` during a
          symbol-updater process, so enqueueing parsed frames is
          wasted work that competes with the discovery throughput.

        Outside discovery (the production publisher path), capture is
        ``None`` and the parse pipeline runs as before.

        Args:
            data: Raw ticker data from WebSocket.
        """
        if not isinstance(data, list):
            return
        self._mark_ticker_health_seen(data)
        capture = self._raw_ticker_capture
        if capture is not None:
            self._capture_raw_ticker_frames(data, capture)
            return
        ticker_list = parse_kraken_ticker_list(data)
        for ticker_data in ticker_list:
            _enqueue_or_drop_oldest(self._tick_queue, ticker_data, "tick")

    def _mark_ticker_health_seen(self, data: list[Any]) -> None:
        """Mark ticker subscriptions as live from raw frames.

        Args:
            data: Raw ticker frames from WebSocket.

        Returns:
            None.
        """
        for frame in data:
            if not isinstance(frame, dict):
                continue
            wire_symbol = frame.get("symbol")
            if isinstance(wire_symbol, str):
                self._health_tracker.mark_data_seen("ticker", wire_symbol)

    @staticmethod
    def _capture_raw_ticker_frames(data: list[Any], capture: dict[str, dict[str, Any]]) -> None:
        """Capture first-seen raw ticker frames by wire symbol.

        Args:
            data: Raw ticker frames from WebSocket.
            capture: Mutable capture dictionary keyed by wire symbol.

        Returns:
            None.
        """
        for frame in data:
            if not isinstance(frame, dict):
                continue
            wire_symbol = frame.get("symbol")
            if isinstance(wire_symbol, str) and wire_symbol not in capture:
                capture[wire_symbol] = frame

    def _handle_trade_data(self, data: Any) -> None:
        """Parse and enqueue trade data.

        Args:
            data: Raw trade data from WebSocket.
        """
        try:
            if isinstance(data, list):
                for item in data:
                    if not isinstance(item, dict):
                        continue
                    wire_symbol = item.get("symbol")
                    if isinstance(wire_symbol, str):
                        self._health_tracker.mark_data_seen("trade", wire_symbol)
            trade_list = parse_kraken_trade_list(data)
            for trade_data in trade_list:
                _enqueue_or_drop_oldest(self._trade_queue, trade_data, "trade")
                if self._trade_built_candles_enabled:
                    self._trade_built_candle_builder.update(trade_data)
        except ValueError as e:
            logger.warning(f"Failed to parse trade data: {e}")

    def _handle_executions_data(self, data: Any) -> None:
        """Parse and enqueue execution data.

        Args:
            data: Raw execution data from WebSocket.
        """
        try:
            execution_list = parse_kraken_execution_list(data)
            for execution_data in execution_list:
                _enqueue_or_drop_oldest(self._execution_queue, execution_data, "execution")
        except ValueError as e:
            logger.warning(f"Failed to parse execution data: {e}")

    async def _handle_instrument_data(self, message_dict: dict[str, Any]) -> None:
        """Parse and enqueue instrument pair data.

        Args:
            message_dict: Full WebSocket message with instrument data.
        """
        try:
            data = message_dict.get("data")
            if data is None:
                return
            if not isinstance(data, dict):
                logger.warning(f"Unexpected instrument data format: {type(data)}")
                return
            pairs_data: list[dict[str, Any]] = data.get("pairs", [])
            for pair_dict in pairs_data:
                await self._process_instrument_pair(pair_dict)
        except Exception as e:
            logger.warning(f"Failed to process instrument data: {e}")

    async def _process_instrument_pair(self, pair_dict: dict[str, Any]) -> None:
        """Validate and enqueue a single instrument pair.

        Args:
            pair_dict: Raw instrument pair dictionary from WebSocket.
        """
        try:
            validated = await asyncio.to_thread(
                KrakenInstrumentPairSchema.model_validate, pair_dict
            )
            _enqueue_or_drop_oldest(
                self._raw_instrument_queue,
                validated.model_dump(by_alias=True),
                "raw_instrument",
            )
        except ValidationError as e:
            logger.warning(f"Invalid instrument data, skipping: {e}")
            return
        try:
            instrument_pair = await asyncio.to_thread(parse_kraken_instrument, pair_dict)
            _enqueue_or_drop_oldest(self._instrument_queue, instrument_pair, "instrument")
        except ValueError as e:
            logger.debug(f"Skipping instrument parse (raw available): {e}")

    async def _get_or_create_ws_client(self) -> Any:
        if not self.api_key or not self.api_secret:
            raise RuntimeError("API credentials required for WebSocket trading")
        await self._ensure_ws_connected()
        return self._ws_client

    async def _ensure_ws_connected(self) -> None:
        """Connect the SpotWSClient if not already connected.

        Serialized on ``_ws_connect_lock``: the publisher's recovery path and
        supervised consumer restarts call this concurrently, and unserialized
        callers can build DUPLICATE clients whose teardown writes clobber each
        other's ``_ws_client`` slot (the duplicate-client churn observed on
        Futures in the 2026-06-09 blackout fault test). Under the lock,
        concurrent callers coalesce onto a single client and the slot
        re-check runs inside the lock.

        A slot holding a client whose ``exception_occur`` flag is terminal is
        closed (bounded, compare-and-clear) and rebuilt instead of being
        trusted: the SDK never recovers such a client, and the residual
        window where no generator finalizer ran (e.g. a consumer death path
        that bypassed cleanup) would otherwise poison every later caller —
        the existence guard treated the dead client as connected and even
        re-asserted ``_ws_connected``.
        """
        async with self._ws_connect_lock:
            poisoned = self._ws_client
            if poisoned is not None and self._ws_exception_occurred(poisoned):
                logger.warning("Kraken WebSocket client slot holds a dead client - rebuilding")
                await self._close_ws_client()
            if not self._ws_client:
                await self._start_new_ws_client()
            self._ws_connected = True

    async def _start_new_ws_client(self) -> None:
        """Build, install, and start a fresh SpotWSClient in the slot.

        Must be called with ``_ws_connect_lock`` held and an empty
        ``_ws_client`` slot. The new client is installed in the slot BEFORE
        ``start()`` so the teardown paths can compare against it, then any
        cached subscriptions are replayed. If the slot stops pointing at the
        new client after the replay (a concurrent path replaced it), this
        connect no longer owns the connection and must fail rather than
        report success for a client nobody holds.

        On ANY failure (including cancellation) the client is torn down
        before the exception propagates: through the compare-and-clear
        :meth:`_close_ws_client` while the slot is still owned, or through
        :meth:`_close_disowned_ws_client` when a newer client already took
        the slot (closing via the slot would tear down the wrong client).

        Returns:
            None.

        Raises:
            RuntimeError: If the ``_ws_client`` slot was replaced during
                connect (``_CONNECT_OWNERSHIP_LOST_MSG``).
            BaseException: Whatever ``start()`` or the subscription replay
                raised, re-raised after teardown.
        """
        client = SpotWSClient(
            key=self.api_key or "",
            secret=self.api_secret or "",
            callback=self._on_message,
        )
        self._ws_client = client
        logger.info("Kraken WebSocket client initialized")
        try:
            async with asyncio.timeout(_WS_CONNECT_TIMEOUT_S):
                await client.start()
            logger.info("Kraken WebSocket client started")
            if self._subscription_cache:
                await self._replay_subscriptions()
            if self._ws_client is not client:
                raise RuntimeError(_CONNECT_OWNERSHIP_LOST_MSG)
        except BaseException:
            if self._ws_client is client:
                await self._close_ws_client()
            else:
                await self._close_disowned_ws_client(client)
            raise

    async def _close_disowned_ws_client(self, client: SpotWSClient) -> None:
        """Close a client that lost the ``_ws_client`` slot mid-connect.

        The slot already points at a NEWER client, so the regular
        :meth:`_close_ws_client` path (which closes through the slot) would
        tear down the wrong connection. The disowned client is closed
        directly with the same bounded timeout; when that close times out or
        raises, :func:`force_close_ws_client` finishes the teardown
        explicitly so no background run task or aiohttp session leaks (#143).

        Args:
            client: The replaced WebSocket client to tear down.

        Returns:
            None.
        """
        try:
            async with asyncio.timeout(self._WS_CLOSE_TIMEOUT_SECONDS):
                await client.close()
        except Exception as exc:
            logger.warning(f"Disowned WebSocket close failed - forcing cleanup: {exc!r}")
            await force_close_ws_client(client)

    async def _replay_subscriptions(self) -> None:
        """Replay cached public market-data subscriptions after reconnect.

        Per-symbol subscriptions re-arm their tracker entries as pending
        (preserving the retry budget) before the subscribe is re-issued. The
        wildcard ticker request carries the literal ``"*"`` sentinel, which
        must never enter the tracker (``_retry_subscribe`` rejects it). Rather
        than skip it, the wildcard ticker universe is re-seeded as confirmed
        via :meth:`_seed_ticker_health` so the stale clock is re-armed for the
        whole wildcard universe after a reconnect.

        Returns:
            None.

        Raises:
            RuntimeError: If the WebSocket client is not connected at entry,
                or the ``_ws_client`` slot stops pointing at the entry client
                between chunk sends (a concurrent path replaced it — keeping
                on marching would aim the replay at the wrong connection).
        """
        client = self._ws_client
        if client is None:
            raise RuntimeError(_WS_CLIENT_CONNECTED_MSG)
        requests = list(self._subscription_cache.values())
        for index, req in enumerate(requests):
            if self._ws_client is not client:
                raise RuntimeError(_REPLAY_CLIENT_REPLACED_MSG)
            await self._replay_one_subscription(client, req)
            if index < len(requests) - 1:
                await asyncio.sleep(_RESUBSCRIBE_CHUNK_DELAY_S)

    async def _replay_one_subscription(
        self, client: SpotWSClient, req: SubscriptionRequest
    ) -> None:
        """Re-arm health tracking and re-issue one cached subscription.

        Per-symbol entries are marked pending (preserving the retry budget)
        before the subscribe is re-issued. The wildcard ticker sentinel
        ``"*"`` must never enter the tracker (``_retry_subscribe`` rejects
        it); its universe is re-seeded as confirmed via
        :meth:`_seed_ticker_health` instead, so the stale clock is re-armed
        for the whole wildcard universe.

        Args:
            client: The WebSocket client the replay is aimed at.
            req: Cached subscription request to replay.

        Returns:
            None.
        """
        params = {
            "channel": req.channel,
            "symbol": list(req.symbols),
            **cast(dict[str, JsonValue], json.loads(req.parameters_json)),
        }
        health_channel: str = req.channel
        if req.channel == "ohlc":
            raw_interval = params.get("interval", 1)
            if isinstance(raw_interval, int):
                health_channel = f"ohlc:{interval_to_label(raw_interval)}"
        for symbol in req.symbols:
            if health_channel == "ticker" and symbol == "*":
                self._seed_ticker_health(["*"])
                continue
            self._health_tracker.mark_pending(
                health_channel,
                symbol,
                preserve_retry_count=True,
            )
        await client.subscribe(params=params)

    async def _retry_subscribe(self, channel: str, symbol: str) -> None:
        """Retry a single Spot subscription without updating replay cache.

        Args:
            channel: Tracker channel key to retry.
            symbol: Kraken Spot wire-format symbol to retry.

        Returns:
            None.

        Raises:
            RuntimeError: If the WebSocket client is not connected.
            ValueError: If the channel key is unsupported.
        """
        if self._ws_client is None:
            raise RuntimeError(_WS_CLIENT_CONNECTED_MSG)
        if channel == "ticker":
            if symbol == "*":
                raise ValueError("Cannot retry wildcard ticker subscription as a single symbol")
            params = KrakenTickerSubscribeParamsSchema(symbol=[symbol]).as_params()
        elif channel == "trade":
            params = KrakenTradeSubscribeParamsSchema(symbol=[symbol]).as_params()
        elif channel.startswith("ohlc:"):
            interval = label_to_interval(channel.removeprefix("ohlc:"))
            params = KrakenOhlcSubscribeParamsSchema(symbol=[symbol], interval=interval).as_params()
        else:
            raise ValueError(f"Unsupported subscription health channel: {channel}")
        await self._ws_client.subscribe(params=params)

    def _get_trade_client(self) -> Trade:
        if not self.api_key or not self.api_secret:
            raise RuntimeError("API credentials required for native Kraken Trade API")
        if self._trade_client is None:
            self._trade_client = Trade(key=self.api_key, secret=self.api_secret)
            logger.debug("Initialized native Kraken Trade REST API client")
        return self._trade_client

    def _get_user_client(self) -> User:
        """Return the lazily-initialized native Kraken User REST client.

        Mirrors :meth:`_get_trade_client`: the ``kraken.spot.User``
        endpoints (open/closed orders) are needed for client-id
        verification of CCXT-unmapped native-only symbols.

        Returns:
            The cached ``User`` client instance.

        Raises:
            RuntimeError: If API credentials are missing.
        """
        if not self.api_key or not self.api_secret:
            raise RuntimeError("API credentials required for native Kraken User API")
        if self._user_client is None:
            self._user_client = User(key=self.api_key, secret=self.api_secret)
            logger.debug("Initialized native Kraken User REST API client")
        return self._user_client

    async def _invoke_with_retry_slot(
        self,
        func: Callable[..., Any],
        *args: Any,
        egress_kind: KrakenRestOperationKind | None = None,
        egress_operation: str | None = None,
        egress_target: RestProxyTarget | None = None,
        **kwargs: Any,
    ) -> Any:
        """Given a REST callable, when one retry attempt runs, then return its result.

        Args:
            func: Function to call (sync or async).
            *args: Positional arguments for func.
            egress_kind: Optional REST routing classification for sync Kraken calls.
            egress_operation: Stable operation name for the egress identity.
            egress_target: External client/session proxy target.
            **kwargs: Keyword arguments for func.

        Returns:
            Result of the function call.

        Raises:
            Exception: Propagates acquisition or invocation failures unchanged.
        """
        await self._acquire_rest_slot()
        result = await self._invoke_func(
            func,
            *args,
            egress_kind=egress_kind,
            egress_operation=egress_operation,
            egress_target=egress_target,
            **kwargs,
        )
        self._circuit_failures = 0
        return result

    async def _with_retry(
        self,
        func: Callable[..., Any],
        *args: Any,
        retry_network_errors: bool = True,
        egress_kind: KrakenRestOperationKind | None = None,
        egress_operation: str | None = None,
        egress_target: RestProxyTarget | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute function with retry and circuit breaker logic.

        Non-idempotent venue mutations (order creation) MUST pass
        ``retry_network_errors=False``: a network-class failure such as
        ``ccxt.RequestTimeout`` is AMBIGUOUS — the request may have
        reached Kraken and executed even though no response came back.
        Kraken deduplicates ``cl_ord_id`` only among OPEN orders and all
        strategy orders are MARKET (filled instantly, never open), so a
        blind re-send after a send-then-timeout can double-place a real
        position. With the flag off, network failures still feed the
        circuit breaker but raise immediately; rate-limit retries remain
        enabled because a 429 is a definitive venue-side rejection and
        re-sending cannot duplicate.

        Args:
            func: Function to call (sync or async).
            *args: Positional arguments for func.
            retry_network_errors: When False, raise network-class errors
                immediately after circuit-breaker accounting instead of
                retrying. Required for non-idempotent venue mutations.
            egress_kind: Optional REST routing classification for sync
                Kraken calls.
            egress_operation: Stable operation name for the egress identity.
            egress_target: External client/session proxy target.
            **kwargs: Keyword arguments for func.

        Returns:
            Result of the function call.

        Raises:
            RuntimeError: If circuit breaker is open.
            Exception: If all retries exhausted or unexpected error.
        """
        if time.time() < self._circuit_open_until:
            raise CircuitBreakerOpenError("Circuit breaker open")
        max_retries = 3
        base_delay = 1.0
        attempt = 0
        while True:
            try:
                return await self._invoke_with_retry_slot(
                    func,
                    *args,
                    egress_kind=egress_kind,
                    egress_operation=egress_operation,
                    egress_target=egress_target,
                    **kwargs,
                )
            except ccxt.RateLimitExceeded:
                attempt = await self._handle_rate_limit(attempt, max_retries, base_delay)
            except (ccxt.NetworkError, ccxt.ExchangeNotAvailable) as e:
                if not retry_network_errors:
                    self._record_unretried_network_failure(e)
                    raise
                attempt = await self._handle_network_error(e, attempt, max_retries, base_delay)
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                raise

    async def _invoke_func(
        self,
        func: Callable[..., Any],
        *args: Any,
        egress_kind: KrakenRestOperationKind | None = None,
        egress_operation: str | None = None,
        egress_target: RestProxyTarget | None = None,
        **kwargs: Any,
    ) -> Any:
        """Invoke a function, awaiting if it is a coroutine.

        Synchronous callables (CCXT REST helpers, native Kraken Trade
        REST SDK) are dispatched via the client-owned bounded REST pool
        (``_dispatch_blocking``) so a slow Kraken REST round-trip (2–5s
        under network jitter has been observed) neither blocks the
        executor event loop nor competes for the loop's shared default
        executor (audit P1-5). Async callables are awaited directly.

        Args:
            func: Function to call.
            *args: Positional arguments.
            egress_kind: Optional REST routing classification for sync
                Kraken calls.
            egress_operation: Stable operation name for the egress identity.
            egress_target: External client/session proxy target.
            **kwargs: Keyword arguments.

        Returns:
            Result of the function call.
        """
        if inspect.iscoroutinefunction(func):
            return await func(*args, **kwargs)
        if egress_kind is not None:
            if egress_operation is None or egress_target is None:
                raise RuntimeError("Kraken REST egress routing requires operation and target")
            return await self._dispatch_routed_rest(
                operation=egress_operation,
                kind=egress_kind,
                target=egress_target,
                sync_call=lambda: func(*args, **kwargs),
            )
        return await self._dispatch_blocking(func, *args, **kwargs)

    @staticmethod
    async def _handle_rate_limit(attempt: int, max_retries: int, base_delay: float) -> int:
        """Handle rate limit exceeded by sleeping with exponential backoff.

        Args:
            attempt: Current attempt number.
            max_retries: Maximum number of retries.
            base_delay: Base delay in seconds.

        Returns:
            Incremented attempt number.

        Raises:
            ccxt.RateLimitExceeded: If max retries exhausted.
        """
        logger.warning(f"Rate limit exceeded, retrying in {base_delay * (2**attempt)}s")
        if attempt < max_retries - 1:
            attempt += 1
            await _retry_sleep(base_delay * (2 ** (attempt - 1)))
            return attempt
        raise ccxt.RateLimitExceeded("Rate limit exceeded after max retries")

    def _record_unretried_network_failure(self, error: Exception) -> None:
        """Feed the circuit breaker for a network failure that will not be retried.

        Mirrors the exhaustion path of ``_handle_network_error`` (failure
        increment plus breaker-open check at raise time) so that
        non-retried order submissions still trip the breaker under a
        sustained outage instead of hammering the venue once per command.

        Args:
            error: The network error that occurred.

        Returns:
            None.
        """
        logger.warning(f"Network error (no retry, non-idempotent call): {error}")
        self._circuit_failures += 1
        if self._circuit_failures >= self._max_failures:
            self._circuit_open_until = time.time() + self._circuit_timeout
            logger.error("Circuit breaker opened due to repeated failures")

    async def _handle_network_error(
        self, error: Exception, attempt: int, max_retries: int, base_delay: float
    ) -> int:
        """Handle network errors with retry and circuit breaker.

        Args:
            error: The network error that occurred.
            attempt: Current attempt number.
            max_retries: Maximum number of retries.
            base_delay: Base delay in seconds.

        Returns:
            Incremented attempt number.

        Raises:
            Exception: If max retries exhausted (re-raises original).
        """
        logger.warning(f"Network error: {error}, retrying in {base_delay * (2**attempt)}s")
        self._circuit_failures += 1
        if attempt < max_retries - 1:
            attempt += 1
            await _retry_sleep(base_delay * (2 ** (attempt - 1)))
            return attempt
        if self._circuit_failures >= self._max_failures:
            self._circuit_open_until = time.time() + self._circuit_timeout
            logger.error("Circuit breaker opened due to repeated failures")
        raise

    def _convert_ccxt_order(self, ccxt_order: dict[str, Any]) -> ExchangeOrderSnapshot:
        """Convert a CCXT order dict to an ExchangeOrderSnapshot.

        Uses module-level _CCXT_STATUS_MAP, _CCXT_SIDE_MAP, and _CCXT_TYPE_MAP
        to translate CCXT string values to internal enum types.
        """
        ccxt_symbol = ccxt_order.get("symbol")
        native_symbol = ccxt_to_native(ccxt_symbol) if ccxt_symbol else ""
        return ExchangeOrderSnapshot(
            id=ccxt_order["id"],
            client_order_id=ccxt_order.get("clientOrderId"),
            symbol=native_symbol,
            side=_CCXT_SIDE_MAP.get(ccxt_order.get("side", ""), OrderSideEnum.BUY),
            type=_resolve_ccxt_order_type(ccxt_order),
            amount=float(ccxt_order.get("amount") or 0),
            price=_resolve_ccxt_fill_price(ccxt_order),
            status=_CCXT_STATUS_MAP[ccxt_order["status"]],
            filled=float(ccxt_order.get("filled") or 0),
            remaining=float(ccxt_order.get("remaining") or 0),
            timestamp=(
                ccxt_order["timestamp"] / 1000.0 if ccxt_order.get("timestamp") else time.time()
            ),
            fee=float(ccxt_order["fee"]["cost"]) if ccxt_order.get("fee") else None,
            fee_currency=(
                str(ccxt_order["fee"]["currency"])
                if ccxt_order.get("fee") and ccxt_order["fee"].get("currency")
                else None
            ),
        )

    def _convert_kraken_user_order(
        self, txid: str, payload: dict[str, Any], native_symbol: str
    ) -> ExchangeOrderSnapshot:
        """Convert a native open/closed-orders entry to a snapshot.

        Field semantics follow :meth:`_convert_ccxt_order`: the raw
        ``descr.ordertype`` is authoritative for the order type (the
        unified ccxt type would collapse stop orders to their base
        type, #156) and an unknown ``status`` raises so the caller
        treats the answer as could-not-verify instead of guessing.
        Price: for trigger-bearing types (stop/take-profit/trailing)
        ``descr.price`` is the TRIGGER, so the executable limit leg
        ``descr.price2`` is used instead; plain orders use
        ``descr.price``; the executed average (``price``) is the
        fallback once something filled, never a fabricated zero. Fee
        currency resolves from ``oflags`` via
        :func:`_native_fee_currency` (ccxt parity).

        Args:
            txid: Venue order id (the key of the orders dict entry).
            payload: Raw order dict from ``get_open_orders`` /
                ``get_closed_orders``.
            native_symbol: Native symbol to stamp on the snapshot.

        Returns:
            The converted snapshot.

        Raises:
            KeyError: If the order ``status`` is missing or unknown.
        """
        descr = _kraken_user_order_descr(payload)
        amount = _mapping_float_value(payload, "vol")
        filled = _mapping_float_value(payload, "vol_exec")
        order_type = _resolve_kraken_user_order_type(_mapping_string_value(descr, "ordertype"))
        limit_price = _resolve_kraken_user_limit_price(descr, order_type)
        average_price = _mapping_float_value(payload, "price")
        price = _resolve_native_order_price(limit_price, filled, average_price)
        fee = _mapping_optional_float_value(payload, "fee")
        return ExchangeOrderSnapshot(
            id=txid,
            client_order_id=payload.get("cl_ord_id"),
            symbol=native_symbol,
            side=_CCXT_SIDE_MAP.get(_mapping_string_value(descr, "type"), OrderSideEnum.BUY),
            type=order_type,
            amount=amount,
            price=price,
            status=_CCXT_STATUS_MAP[str(payload["status"])],
            filled=filled,
            remaining=max(0.0, amount - filled),
            timestamp=_resolve_kraken_user_order_timestamp(payload.get("opentm")),
            fee=fee,
            fee_currency=_resolve_kraken_user_fee_currency(
                native_symbol, _mapping_string_value(payload, "oflags"), fee
            ),
        )

    def _convert_kraken_native_order(
        self, kraken_response: dict[str, Any], request: ExchangeOrderRequest
    ) -> ExchangeOrderSnapshot:
        txid = (
            kraken_response.get("txid", [""])[0]
            if isinstance(kraken_response.get("txid"), list)
            else kraken_response.get("txid", "")
        )
        return ExchangeOrderSnapshot(
            id=str(txid),
            client_order_id=request.client_order_id,
            symbol=request.symbol,
            side=request.side,
            type=request.type,
            amount=float(request.amount),
            price=float(request.price) if request.price else None,
            status=ExchangeOrderStatusEnum.PENDING,
            filled=float("0"),
            remaining=float(request.amount),
            timestamp=time.time(),
            fee=None,
        )
