"""Kraken Futures exchange client implementation.

This module provides KrakenFuturesExchangeClient for the Kraken Futures
exchange. It supports:

REST API Operations:
    - Market data: tickers, OHLCV candles (via CCXT krakenfutures)
    - Instrument metadata (via kraken.futures.Market)
    - Order CRUD: create, cancel, query (via kraken.futures.Trade)
    - Account: wallet balances (via kraken.futures.User)

WebSocket Subscriptions (via callback-to-queue bridge):
    - Public: tickers, trades
    - Private: fills, open_orders (authenticated)

The Kraken Futures SDK uses a callback-driven WebSocket client. This
implementation bridges callbacks to asyncio.Queue objects so that the
publisher can consume data via async iterators (matching Snapper's
established pattern).
"""

import asyncio
import contextlib
import math
import time
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from time import monotonic
from typing import Any
from typing import Literal
from typing import cast

import ccxt
from kraken.futures import FuturesWSClient
from kraken.futures import Market
from kraken.futures import Trade
from kraken.futures import User
from loguru import logger

from snapper.core.types import ExchangeEnum
from snapper.data.repository import Repository
from snapper.infrastructure.exchanges._subscription_health import SubscriptionHealthTracker
from snapper.infrastructure.exchanges._subscription_request import SubscriptionRequest
from snapper.infrastructure.exchanges._trade_candle_builder import TradeCandleBuilder
from snapper.infrastructure.exchanges._trade_candle_builder import enqueue_or_drop_oldest_candle
from snapper.infrastructure.exchanges.adapters.kraken_futures import _ORDER_TYPE_MAP
from snapper.infrastructure.exchanges.adapters.kraken_futures import _SIDE_MAP
from snapper.infrastructure.exchanges.adapters.kraken_futures import _STATUS_MAP
from snapper.infrastructure.exchanges.adapters.kraken_futures import parse_kraken_futures_fill
from snapper.infrastructure.exchanges.adapters.kraken_futures import parse_kraken_futures_instrument
from snapper.infrastructure.exchanges.adapters.kraken_futures import parse_kraken_futures_ticker
from snapper.infrastructure.exchanges.adapters.kraken_futures import parse_kraken_futures_trade
from snapper.infrastructure.exchanges.base import ExchangeClientBase
from snapper.infrastructure.exchanges.contracts import AccountBalance
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import ExchangeOrderRequest
from snapper.infrastructure.exchanges.contracts import ExchangeOrderSnapshot
from snapper.infrastructure.exchanges.contracts import ExchangeOrderStatusEnum
from snapper.infrastructure.exchanges.contracts import ExchangeOrderTypeEnum
from snapper.infrastructure.exchanges.contracts import ExecutionUpdate
from snapper.infrastructure.exchanges.contracts import FundingRateSnapshot
from snapper.infrastructure.exchanges.contracts import InstrumentPairDescriptor
from snapper.infrastructure.exchanges.contracts import OhlcvSnapshot
from snapper.infrastructure.exchanges.contracts import OpenPositionSnapshot
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.contracts import TickerSnapshot
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate
from snapper.infrastructure.symbols.functions import kraken_futures_ws_to_native
from snapper.infrastructure.symbols.functions import native_to_kraken_futures_ws

_CREDENTIALS_REQUIRED_MSG = "API credentials required for authenticated operations"
_PUBLIC_WS_NOT_CONNECTED_MSG = "WebSocket client not connected"
_QUEUE_DRAIN_TIMEOUT = 0.1
_WS_CLOSE_TIMEOUT_S = 10.0
"""Upper bound on a WebSocket close so a blackholed socket cannot hang the
liveness-recovery teardown or process shutdown indefinitely."""

_WS_CONNECT_TIMEOUT_S = 20.0
"""Upper bound on a WebSocket connect (``FuturesWSClient.start``) so a connect
that never completes cannot wedge liveness recovery.

The python-kraken-sdk's ``start`` polls for the socket with a connect timeout
that never fires (``while (timeout := 0.0) < 10`` resets the counter every
iteration), so on a prolonged blackout — where the connector hits
``MAX_RECONNECT_NUM`` and exits without ever setting the socket — ``start``
loops forever. Bounding it here turns that permanent hang into a timeout that
tears the partial client down and lets the recovery loop retry with a fresh
client, which reconnects once the network returns instead of requiring a
process restart."""

_SDK_SEND_TIMEOUT_S = 5.0
"""Upper bound on a single SDK subscribe/unsubscribe send (public AND private).

The Futures SDK's ``ConnectFuturesWebsocket.send_message`` polls
``while not self.socket: await asyncio.sleep(0.4)`` with NO exit condition, so
a send issued against a client whose connection task died before the socket
was ever assigned blocks forever. In the 2026-06-09 blackout fault test one
such send wedged while holding ``_public_subscribe_lock``, queueing every
other subscribe path (replay, health retries, consumer chunk subscribes)
behind it permanently — the feed stayed CONNECTED-BUT-DARK until a process
restart. Bounding each send makes a socketless client raise ``TimeoutError``
out of the lock instead; callers already treat a raising send as a retryable
failure. A healthy send completes in well under 100 ms, so 5 s cannot
false-trip."""

_REPLAY_CLIENT_REPLACED_MSG = "WebSocket client replaced during subscription replay"
_CONNECT_OWNERSHIP_LOST_MSG = "WebSocket client replaced during connect"
_QUEUE_MAX_SIZE = 10000
_TICK_QUEUE_MAX_SIZE = 50000
"""Boot-time absorption budget for ticker and candle producer queues.

The Kraken Futures broker, like Spot, replays a wildcard snapshot of
every subscribed instrument's ticker on initial subscribe + the
``TradeCandleBuilder`` aggregator emits a synthetic candle per trade
during the boot replay window. The 2026-05-25 post-restart 5-minute
slice counted 5 drop-oldest WARN records on the Futures tick queue
(smaller burst than Spot — fewer instruments). The 2026-05-26 Spot
analysis showed the candle queue is the next bottleneck once the
tick queue stops being one; the Futures candle queue follows the
same producer/consumer ratio, so we pre-emptively widen both here
to match. ``trade``/``execution`` queues stay at ``_QUEUE_MAX_SIZE``
(10 k) — no drops observed."""
_PUBLIC_SUBSCRIBE_MIN_INTERVAL_S = 0.075
_RATE_LIMITED_COOLDOWN_S = 5.0
_ALREADY_SUBSCRIBED_FUTURES_ALERT = "Already subscribed to feed, re-requesting"
"""Kraken Futures WS server alert text emitted when the broker detects
that a feed/product subscription request races against an existing
in-server subscription. Benign: fires during our publisher health-loop
replays and on broker-side reconnect echoes. The Spot equivalent
``'Already subscribed'`` is filtered in the SDK-level monkeypatch
(see :mod:`snapper.infrastructure.exchanges.kraken_sdk_patches`), but
Futures uses a separate ``ConnectFuturesWebsocket`` client that does
not flow through that patch; the gate lives at the publisher
``_handle_alert_event`` boundary instead."""

_TIMEFRAME_SECONDS: dict[str, int] = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
}


def _tracker_feed_key(feed: str) -> str:
    """Normalize Futures feed names to tracker channel keys.

    Args:
        feed: Kraken Futures feed name from a subscription or data frame.

    Returns:
        Tracker channel key.
    """
    if feed in {"ticker", "ticker_lite"}:
        return "ticker"
    if feed in {"trade", "trade_snapshot"}:
        return "trade"
    return feed


def _timeframe_to_seconds(timeframe: str) -> int:
    """Convert CCXT timeframe string to duration in seconds.

    Args:
        timeframe: Candle interval (e.g., ``1m``, ``1h``, ``1d``).

    Returns:
        Duration in seconds.

    Raises:
        ValueError: If timeframe is not supported.
    """
    try:
        return _TIMEFRAME_SECONDS[timeframe]
    except KeyError as exc:
        supported = ", ".join(sorted(_TIMEFRAME_SECONDS))
        raise ValueError(f"Unsupported timeframe: {timeframe}. Supported: {supported}") from exc


_DROP_LOG_INTERVAL_S = 1.0
_drop_counters: dict[str, list[float]] = {}


def _enqueue_or_drop_oldest(queue: asyncio.Queue[Any], item: Any, label: str) -> None:
    """Put item on queue, dropping the oldest if full.

    Logs are rate-limited to one summary line per
    ``_DROP_LOG_INTERVAL_S`` seconds per ``label`` so a sustained
    drop-oldest burst does not amplify log I/O on the publisher hot
    path.

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


def _map_kraken_order_type(raw: str) -> ExchangeOrderTypeEnum:
    """Map Kraken Futures order type string to ExchangeOrderTypeEnum.

    Args:
        raw: Kraken order type (e.g., ``lmt``, ``mkt``).

    Returns:
        Corresponding ExchangeOrderTypeEnum value.
    """
    return _ORDER_TYPE_MAP.get(raw, ExchangeOrderTypeEnum.LIMIT)


def _map_kraken_status(raw: str) -> ExchangeOrderStatusEnum:
    """Map Kraken Futures order status string to ExchangeOrderStatusEnum.

    Args:
        raw: Kraken order status (e.g., ``placed``, ``filled``).

    Returns:
        Corresponding ExchangeOrderStatusEnum value.
    """
    return _STATUS_MAP.get(raw, ExchangeOrderStatusEnum.OPEN)


def _map_kraken_side(raw: str) -> OrderSideEnum:
    """Map Kraken Futures side string to OrderSideEnum.

    Args:
        raw: Kraken side string (``buy`` or ``sell``).

    Returns:
        Corresponding OrderSideEnum value.
    """
    return _SIDE_MAP.get(raw, OrderSideEnum.BUY)


class KrakenFuturesExchangeClient(ExchangeClientBase):
    """Kraken Futures exchange client.

    Bridges the callback-driven FuturesWSClient to asyncio.Queue-based
    async iterators matching Snapper's publisher consumption pattern.
    Supports both market data (public) and order execution (authenticated).

    Attributes:
        sandbox: Whether to use the demo/sandbox environment.
        supports_websocket_executions: True when API credentials are provided.
    """

    supports_websocket_executions: bool = False

    def __init__(
        self,
        sandbox: bool = False,
        repository: Repository | None = None,
        api_key: str | None = None,
        api_secret: str | None = None,
    ) -> None:
        """Initialize Kraken Futures exchange client.

        The health tracker enables dark auto-recovery for BOTH ``ticker`` and
        ``trade`` (not the ``ticker``-only default): Futures candles are
        synthesized from the live trade stream, so without ``trade`` in the
        dark-recovery set the candle pipeline would have no slow backstop at
        all when every trade subscription goes dark simultaneously.

        Args:
            sandbox: Use sandbox environment for testing (default: False).
            repository: Database repository for order/execution logging.
            api_key: Kraken Futures API key (required for order operations).
            api_secret: Kraken Futures API secret (required for order operations).
        """
        super().__init__(repository=repository, exchange_name=ExchangeEnum.KRAKEN_FUTURES)
        self._health_tracker: SubscriptionHealthTracker = SubscriptionHealthTracker(
            dark_recovery_channels=frozenset({"ticker", "trade"})
        )
        self.sandbox = sandbox
        self._api_key = api_key
        self._api_secret = api_secret
        self._ccxt_client = cast(Any, ccxt.krakenfutures)({"sandbox": sandbox, "timeout": 30000})
        self._market_client: Market | None = None
        self._trade_client: Trade | None = None
        self._user_client: User | None = None
        self._ws_client: FuturesWSClient | None = None
        self._private_ws_client: FuturesWSClient | None = None
        self._tick_queue: asyncio.Queue[TickerUpdate] = asyncio.Queue(maxsize=_TICK_QUEUE_MAX_SIZE)
        self._trade_queue: asyncio.Queue[TradeUpdate] = asyncio.Queue(maxsize=_QUEUE_MAX_SIZE)
        self._candle_queue: asyncio.Queue[CandleUpdate] = asyncio.Queue(
            maxsize=_TICK_QUEUE_MAX_SIZE
        )
        self._candle_builder = TradeCandleBuilder(interval_seconds=60)
        self._candle_aggregator_task: asyncio.Task[None] | None = None
        self._subscription_cache: dict[tuple[str, frozenset[str], str], SubscriptionRequest] = {}
        self._next_public_subscribe_at: float = 0.0
        self._last_rate_limited_log_at: float = -math.inf
        self._public_subscribe_lock: asyncio.Lock = asyncio.Lock()
        self._ws_connect_lock: asyncio.Lock = asyncio.Lock()
        self._private_ws_connect_lock: asyncio.Lock = asyncio.Lock()
        self._execution_queue: asyncio.Queue[ExecutionUpdate] = asyncio.Queue(
            maxsize=_QUEUE_MAX_SIZE
        )
        if api_key and api_secret:
            self.supports_websocket_executions = True
            self._trade_client = Trade(key=api_key, secret=api_secret, sandbox=sandbox)
            self._user_client = User(key=api_key, secret=api_secret, sandbox=sandbox)

    def _require_authenticated(self) -> None:
        """Raise RuntimeError if API credentials are missing.

        Raises:
            RuntimeError: If api_key or api_secret were not provided.
        """
        if not self._api_key or not self._api_secret:
            raise RuntimeError(_CREDENTIALS_REQUIRED_MSG)

    async def connect(self) -> None:
        """Establish REST connection to Kraken Futures.

        Loads markets via CCXT and creates the Market REST client.

        Raises:
            RuntimeError: If connection fails.
        """
        try:
            self._record_rest_call()
            await asyncio.to_thread(self._ccxt_client.load_markets)
            self._market_client = Market(sandbox=self.sandbox)
            logger.info("Kraken Futures REST connection established")
        except Exception as e:
            logger.error(f"Failed to connect to Kraken Futures: {e}")
            raise

    async def disconnect(self) -> None:
        """Close all Kraken Futures connections.

        Each close is bounded by ``_WS_CLOSE_TIMEOUT_S`` so a blackholed
        socket cannot hang liveness recovery or shutdown; on timeout or
        close error the client reference is dropped and rebuilt on the next
        connect. Slot clears are compare-and-clear: a concurrent
        ``_ensure_ws_connected`` may have installed a NEWER client while this
        close was in flight, and unconditionally nulling the slot here would
        detach that live client (callback still attached, no owner — the
        orphan pattern from the 2026-06-09 incident).
        """
        client = self._ws_client
        if client:
            try:
                async with asyncio.timeout(_WS_CLOSE_TIMEOUT_S):
                    await client.close()
            except TimeoutError:
                logger.warning("Kraken Futures WS close timed out - forcing cleanup")
            except Exception as e:
                logger.warning(f"Error closing Kraken Futures WS: {e}")
            if self._ws_client is client:
                self._ws_client = None
        private_client = self._private_ws_client
        if private_client:
            try:
                async with asyncio.timeout(_WS_CLOSE_TIMEOUT_S):
                    await private_client.close()
            except TimeoutError:
                logger.warning("Kraken Futures private WS close timed out - forcing cleanup")
            except Exception as e:
                logger.warning(f"Error closing Kraken Futures private WS: {e}")
            if self._private_ws_client is private_client:
                self._private_ws_client = None
        logger.info("Kraken Futures connections closed")

    async def _on_ws_message(self, message: dict[str, Any]) -> None:
        """Route incoming WS messages to the appropriate queue.

        This is the callback passed to FuturesWSClient. It parses the
        feed type and dispatches to tick_queue or trade_queue.

        Kraken Futures emits two different shapes on the trade channel:

        - ``feed == "trade"``: a SINGLE trade per message. The trade
          fields (uid, side, type, seq, time, qty, price) live directly
          on the top-level message alongside ``product_id``.
        - ``feed == "trade_snapshot"``: an initial-state batch carrying
          ``trades: [...]`` with up to ~100 historical fills wrapped in
          a single envelope keyed by ``product_id``.

        The legacy handler only iterated ``message.get("trades", [])``
        which silently dropped every live ``feed=trade`` update (the
        single-trade envelope has no ``trades`` array). This branch
        normalizes both shapes through the same parse path.

        Args:
            message: Raw WebSocket message dictionary.
        """
        await asyncio.sleep(0)
        if await self._handle_ws_event(message):
            return
        feed = message.get("feed", "")
        if feed in ("ticker", "ticker_lite"):
            self._handle_ticker_feed(message)
        elif feed in ("trade", "trade_snapshot"):
            self._handle_trade_feed(feed, message)

    async def _handle_ws_event(self, message: dict[str, Any]) -> bool:
        """Handle event-shaped Futures WebSocket messages.

        Args:
            message: Raw WebSocket message dictionary.

        Returns:
            True when the message was an event and no feed handling is needed.
        """
        event = message.get("event")
        if event == "subscribed":
            self._handle_subscribed_event(message)
            return True
        if event == "alert":
            await self._handle_alert_event(message)
            return True
        return event is not None

    def _handle_ticker_feed(self, message: dict[str, Any]) -> None:
        """Parse and enqueue one ticker feed message.

        Args:
            message: Raw ticker or ticker_lite feed message.

        Returns:
            None.
        """
        try:
            product_id = self._product_id_from_message(message)
            if product_id:
                self._health_tracker.mark_data_seen("ticker", product_id)
            tick = parse_kraken_futures_ticker(self._ticker_parse_payload(message, product_id))
            _enqueue_or_drop_oldest(self._tick_queue, tick, "Tick")
        except (ValueError, KeyError) as exc:
            logger.debug(f"Skipping unparseable ticker WS message: {exc}")

    @staticmethod
    def _product_id_from_message(message: dict[str, Any]) -> str:
        """Return the product id or symbol from a Futures message.

        Args:
            message: Raw WebSocket message.

        Returns:
            Product id string, or an empty string when missing.
        """
        product_id = message.get("product_id")
        if isinstance(product_id, str):
            return product_id
        raw_symbol = message.get("symbol")
        return raw_symbol if isinstance(raw_symbol, str) else ""

    @staticmethod
    def _ticker_parse_payload(message: dict[str, Any], product_id: str) -> dict[str, Any]:
        """Return ticker payload with the parser's expected symbol field.

        Args:
            message: Raw ticker or ticker_lite feed message.
            product_id: Product id already resolved from the message.

        Returns:
            Parser payload containing ``symbol``.
        """
        if "symbol" in message:
            return message
        return {**message, "symbol": product_id}

    def _handle_trade_feed(self, feed: object, message: dict[str, Any]) -> None:
        """Parse and enqueue one live trade feed message.

        Args:
            feed: Raw feed discriminator.
            message: Raw trade or trade_snapshot feed message.

        Returns:
            None.
        """
        if feed == "trade_snapshot":
            logger.debug("Skipping Futures trade_snapshot batch because it is a replay artifact")
            return
        product_id = message.get("product_id", "")
        if isinstance(product_id, str) and product_id:
            self._health_tracker.mark_data_seen("trade", product_id)
        self._enqueue_trade_message(message, product_id)

    def _enqueue_trade_message(self, message: dict[str, Any], product_id: object) -> None:
        """Parse and enqueue one normalized trade message.

        Args:
            message: Raw trade feed message.
            product_id: Product id value to inject into the parser payload.

        Returns:
            None.
        """
        try:
            trade = parse_kraken_futures_trade({**message, "product_id": product_id})
        except (ValueError, KeyError) as exc:
            logger.debug(f"Skipping unparseable trade WS message: {exc}")
            return
        _enqueue_or_drop_oldest(self._trade_queue, trade, "Trade")
        self._candle_builder.update(trade)

    def _handle_subscribed_event(self, message: dict[str, Any]) -> None:
        """Track a Kraken Futures subscribed event.

        Args:
            message: Raw event frame.
        """
        raw_feed = message.get("feed")
        if not isinstance(raw_feed, str):
            logger.debug("Received Futures subscribed event without feed: {}", message)
            return
        feed = _tracker_feed_key(raw_feed)
        raw_product_ids = message.get("product_ids", [])
        if not isinstance(raw_product_ids, list):
            logger.debug("Received Futures subscribed event without product_ids list: {}", message)
            return
        for product_id in raw_product_ids:
            if isinstance(product_id, str):
                self._health_tracker.mark_confirmed(feed, product_id)

    async def _handle_alert_event(self, message: dict[str, Any]) -> None:
        """Track an attributable Kraken Futures alert event as failed.

        Args:
            message: Raw event frame.
        """
        if message.get("message") == "rate_limited":
            should_log: bool = False
            async with self._public_subscribe_lock:
                now = time.monotonic()
                cooldown_until = now + _RATE_LIMITED_COOLDOWN_S
                self._next_public_subscribe_at = max(self._next_public_subscribe_at, cooldown_until)
                if now - self._last_rate_limited_log_at >= _RATE_LIMITED_COOLDOWN_S:
                    self._last_rate_limited_log_at = now
                    should_log = True
            if should_log:
                logger.warning(
                    "Kraken Futures rate_limited: pausing public subscribes for {}s",
                    _RATE_LIMITED_COOLDOWN_S,
                )
            return
        if message.get("message") == _ALREADY_SUBSCRIBED_FUTURES_ALERT:
            logger.debug("Kraken Futures subscribe race (already subscribed): {}", message)
            return
        message_text = message.get("message")
        error = message_text if isinstance(message_text, str) else "subscription alert"
        raw_feed = message.get("feed")
        product_id = message.get("product_id")
        if isinstance(raw_feed, str) and isinstance(product_id, str):
            feed = _tracker_feed_key(raw_feed)
            self._health_tracker.mark_failed(feed, product_id, error)
            logger.warning(
                "Kraken Futures subscription alert feed={} product={} error={}",
                feed,
                product_id,
                error,
            )
            return
        logger.warning("Kraken Futures unattributed subscription alert: {}", message)

    async def _on_execution_message(self, message: dict[str, Any]) -> None:
        """Route private WS fill messages to the execution queue.

        Only processes ``fills`` and ``fills_snapshot`` feeds. The
        ``open_orders`` feed is intentionally excluded: it carries
        cumulative order-state snapshots (not incremental fills) and
        feeding it through the executor's fill pipeline would emit
        bogus zero-size executions or double-count real fills.

        Args:
            message: Raw WebSocket message dictionary.
        """
        await asyncio.sleep(0)
        if "event" in message:
            return
        feed = message.get("feed", "")
        if feed in ("fills", "fills_snapshot"):
            for fill in message.get("fills", []):
                try:
                    update = parse_kraken_futures_fill(fill, kraken_futures_ws_to_native)
                    _enqueue_or_drop_oldest(self._execution_queue, update, "Fill")
                except (ValueError, KeyError) as exc:
                    logger.debug(f"Skipping unparseable fill WS message: {exc}")

    async def _ensure_ws_connected(self) -> None:
        """Connect the FuturesWSClient if not already connected.

        Serialized on ``_ws_connect_lock``: the recovery loop and supervised
        consumer restarts call this concurrently, and unserialized callers
        previously built DUPLICATE clients (two live connections observed in
        the 2026-06-09 blackout fault test) while their unconditional teardown
        writes clobbered each other's ``_ws_client`` slot. Under the lock,
        concurrent callers coalesce onto a single client; the slot re-check
        runs inside the lock. Teardown is compare-and-clear so a failed
        caller can never null out a different caller's client.

        On a build/replay failure or cancellation the partial public client
        is torn down (bounded by ``_WS_CLOSE_TIMEOUT_S``) before the error
        propagates, so a cancelled or failed recovery cannot leak the SDK
        client, its background run task, or its aiohttp session.
        """
        async with self._ws_connect_lock:
            if self._ws_client is not None:
                return
            client = FuturesWSClient(callback=self._on_ws_message, sandbox=self.sandbox)
            self._ws_client = client
            try:
                async with asyncio.timeout(_WS_CONNECT_TIMEOUT_S):
                    await client.start()
                logger.info("Kraken Futures WebSocket connected")
                if self._subscription_cache:
                    await self._replay_subscriptions()
                if self._ws_client is not client:
                    raise RuntimeError(_CONNECT_OWNERSHIP_LOST_MSG)
            except BaseException:
                try:
                    async with asyncio.timeout(_WS_CLOSE_TIMEOUT_S):
                        await client.close()
                except Exception as exc:
                    logger.warning(f"Error closing partial Kraken Futures WS: {exc!r}")
                if self._ws_client is client:
                    self._ws_client = None
                raise

    async def _replay_subscriptions(self) -> None:
        """Replay cached public subscriptions after reconnect.

        Captures the client at entry and aborts (raises) if the
        ``_ws_client`` slot is swapped mid-replay, so a replay aimed at a
        client that a concurrent path already replaced cannot keep marching
        through hundreds of sends against the wrong connection.

        Raises:
            RuntimeError: If no client is connected at entry, or the slot
                stops pointing at the entry client between sends.
        """
        client = self._ws_client
        if client is None:
            raise RuntimeError(_PUBLIC_WS_NOT_CONNECTED_MSG)
        requests = list(self._subscription_cache.values())
        for req in requests:
            for product in req.symbols:
                if self._ws_client is not client:
                    raise RuntimeError(_REPLAY_CLIENT_REPLACED_MSG)
                await self._send_public_subscribe(
                    feed=req.channel, product=product, preserve_retry_count=True
                )

    async def _retry_subscribe(self, channel: str, symbol: str) -> None:
        """Retry a single Futures subscription through the shared limiter.

        Args:
            channel: Tracker channel key to retry.
            symbol: Kraken Futures product id to retry.

        Returns:
            None.

        Raises:
            RuntimeError: If the WebSocket client is not connected.
            ValueError: If the channel key is unsupported.
        """
        if self._ws_client is None:
            raise RuntimeError(_PUBLIC_WS_NOT_CONNECTED_MSG)
        if channel not in {"ticker", "trade"}:
            raise ValueError(f"Unsupported subscription health channel: {channel}")
        await self._send_public_subscribe(feed=channel, product=symbol, preserve_retry_count=True)

    async def _ensure_private_ws_connected(self) -> None:
        """Connect the authenticated FuturesWSClient if not already connected.

        Serialized on ``_private_ws_connect_lock`` with compare-and-clear
        teardown and a post-start ownership check, mirroring the public
        ``_ensure_ws_connected`` hardening: an unserialized private connect
        raced by ``disconnect()`` could otherwise clobber a newer private
        client or leak a started-but-unowned one (callback attached, no
        owner).

        Raises:
            RuntimeError: If API credentials are missing, or the private
                slot was replaced while the connect was in flight.
        """
        self._require_authenticated()
        async with self._private_ws_connect_lock:
            if self._private_ws_client is not None:
                return
            client = FuturesWSClient(
                key=self._api_key or "",
                secret=self._api_secret or "",
                callback=self._on_execution_message,
                sandbox=self.sandbox,
            )
            self._private_ws_client = client
            try:
                async with asyncio.timeout(_WS_CONNECT_TIMEOUT_S):
                    await client.start()
                logger.info("Kraken Futures private WebSocket connected")
                if self._private_ws_client is not client:
                    raise RuntimeError(_CONNECT_OWNERSHIP_LOST_MSG)
            except BaseException:
                try:
                    async with asyncio.timeout(_WS_CLOSE_TIMEOUT_S):
                        await client.close()
                except Exception as exc:
                    logger.warning(f"Error closing partial Kraken Futures private WS: {exc!r}")
                if self._private_ws_client is client:
                    self._private_ws_client = None
                raise

    async def get_ticker(self, symbol: str) -> TickerSnapshot:
        """Fetch current ticker via CCXT.

        Args:
            symbol: CCXT-format symbol (e.g., ``BTC/USD:USD``).

        Returns:
            TickerSnapshot with current price data.
        """
        self._record_rest_call()
        data = await asyncio.to_thread(self._ccxt_client.fetch_ticker, symbol)
        return TickerSnapshot(
            symbol=symbol,
            bid=float(data.get("bid") or 0),
            ask=float(data.get("ask") or 0),
            last=float(data.get("last") or 0),
            timestamp=float(data.get("timestamp") or 0) / 1000,
        )

    async def get_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1m",
        since: int | None = None,
        limit: int | None = None,
    ) -> list[OhlcvSnapshot]:
        """Fetch OHLCV candles via CCXT.

        Args:
            symbol: CCXT-format symbol.
            timeframe: Candle interval (e.g., ``1m``, ``1h``).
            since: Start timestamp in milliseconds.
            limit: Maximum number of candles.

        Returns:
            List of OhlcvSnapshot objects.
        """
        self._record_rest_call()
        raw = await asyncio.to_thread(
            self._ccxt_client.fetch_ohlcv, symbol, timeframe, since, limit
        )
        return [
            OhlcvSnapshot(
                timestamp=candle[0] / 1000,
                open=float(candle[1]),
                high=float(candle[2]),
                low=float(candle[3]),
                close=float(candle[4]),
                volume=float(candle[5]),
            )
            for candle in raw
        ]

    async def create_order(self, request: ExchangeOrderRequest) -> ExchangeOrderSnapshot:
        """Submit a new order on Kraken Futures.

        Args:
            request: Order parameters.

        Returns:
            ExchangeOrderSnapshot with the created order state.

        Raises:
            RuntimeError: If API credentials are missing.
            Exception: If order creation fails.
        """
        self._require_authenticated()
        kraken_symbol = native_to_kraken_futures_ws(request.symbol)
        supported_order_types: dict[ExchangeOrderTypeEnum, str] = {
            ExchangeOrderTypeEnum.LIMIT: "lmt",
            ExchangeOrderTypeEnum.MARKET: "mkt",
            ExchangeOrderTypeEnum.STOP_LOSS: "stp",
            ExchangeOrderTypeEnum.TAKE_PROFIT: "take_profit",
            ExchangeOrderTypeEnum.TRAILING_STOP: "trailing_stop",
        }
        if request.type not in supported_order_types:
            raise ValueError(
                f"Unsupported order type for Kraken Futures: {request.type.value}. "
                f"Supported: {', '.join(t.value for t in supported_order_types)}"
            )
        kraken_order_type = supported_order_types[request.type]
        if request.post_only and kraken_order_type == "lmt":
            kraken_order_type = "post"
        kwargs: dict[str, Any] = {
            "orderType": kraken_order_type,
            "size": request.amount,
            "symbol": kraken_symbol,
            "side": request.side.value,
        }
        if request.price is not None:
            kwargs["limitPrice"] = request.price
        if request.client_order_id:
            kwargs["cliOrdId"] = request.client_order_id
        if request.stop_price is not None:
            kwargs["stopPrice"] = request.stop_price
        if request.reduce_only:
            kwargs["reduceOnly"] = True
        self._record_rest_call()
        result = await asyncio.to_thread(cast(Trade, self._trade_client).create_order, **kwargs)
        send_status = result.get("sendStatus", {})
        order_id = send_status.get("order_id", "")
        status_str = send_status.get("status", "placed")
        order = ExchangeOrderSnapshot(
            id=order_id,
            client_order_id=request.client_order_id,
            symbol=request.symbol,
            side=request.side,
            type=request.type,
            amount=request.amount,
            price=request.price,
            status=_map_kraken_status(status_str),
            filled=0.0,
            remaining=request.amount,
            timestamp=datetime.now(UTC).timestamp(),
        )
        db_result = await self._log_order_to_db(request, order)
        if db_result is not None:
            order.db_order_id = db_result[0]
            order.db_order_public_id = db_result[1]
        return order

    async def cancel_order(self, order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        """Cancel an existing order on Kraken Futures.

        Args:
            order_id: Exchange order ID to cancel.
            symbol: Native symbol (optional, used for response).

        Returns:
            ExchangeOrderSnapshot with cancelled status.

        Raises:
            RuntimeError: If API credentials are missing.
            Exception: If cancellation fails.
        """
        self._require_authenticated()
        self._record_rest_call()
        result = await asyncio.to_thread(
            cast(Trade, self._trade_client).cancel_order, order_id=order_id
        )
        cancel_status = result.get("cancelStatus", {})
        status_str = cancel_status.get("status", "cancelled")
        events = cancel_status.get("orderEvents", [])
        order_data: dict[str, Any] = {}
        if events:
            order_data = events[0].get("order", {})
        if order_data:
            order_data["status"] = status_str
            return self._convert_sdk_order(order_data)
        if status_str == "notFound":
            try:
                return await self.get_order(order_id, symbol)
            except Exception:
                logger.warning("cancel_order: notFound and get_order failed for {}", order_id)
        return ExchangeOrderSnapshot(
            id=order_id,
            client_order_id=None,
            symbol=symbol or "",
            side=OrderSideEnum.BUY,
            type=ExchangeOrderTypeEnum.LIMIT,
            amount=0.0,
            price=None,
            status=_map_kraken_status(status_str),
            filled=0.0,
            remaining=0.0,
            timestamp=datetime.now(UTC).timestamp(),
        )

    async def get_order(self, order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        """Fetch order details from Kraken Futures.

        Args:
            order_id: Exchange order ID.
            symbol: Native symbol (unused, kept for interface compatibility).

        Returns:
            ExchangeOrderSnapshot with current order state.

        Raises:
            RuntimeError: If API credentials are missing.
            Exception: If order fetch fails.
        """
        self._require_authenticated()
        self._record_rest_call()
        result = await asyncio.to_thread(
            cast(Trade, self._trade_client).get_orders_status, orderIds=[order_id]
        )
        orders = result.get("orders", [])
        if not orders:
            raise ValueError(f"Order {order_id} not found")
        entry = orders[0]
        inner = entry.get("order", {})
        if inner:
            inner["status"] = entry.get("status", inner.get("status", "placed"))
        else:
            inner = entry
        return self._convert_sdk_order(inner)

    async def get_orders(
        self,
        symbol: str | None = None,
        status: ExchangeOrderStatusEnum | None = None,
        limit: int | None = None,
    ) -> list[ExchangeOrderSnapshot]:
        """Fetch open orders from Kraken Futures.

        Args:
            symbol: Filter by native symbol (optional).
            status: Filter by order status (optional).
            limit: Maximum number of orders to return (optional).

        Returns:
            List of ExchangeOrderSnapshot objects.

        Raises:
            RuntimeError: If API credentials are missing.
            Exception: If orders fetch fails.
        """
        self._require_authenticated()
        self._record_rest_call()
        result = await asyncio.to_thread(cast(User, self._user_client).get_open_orders)
        raw_orders: list[dict[str, Any]] = result.get("openOrders", [])
        snapshots = [self._convert_sdk_order(o) for o in raw_orders]
        if symbol:
            snapshots = [s for s in snapshots if s.symbol == symbol]
        if status:
            snapshots = [s for s in snapshots if s.status == status]
        if limit:
            snapshots = snapshots[:limit]
        return snapshots

    @staticmethod
    def _parse_coin_margin_account(
        acct_data: dict[str, Any],
    ) -> list[AccountBalance]:
        """Parse a coin-margined account (has a ``balances`` dict).

        Args:
            acct_data: Raw account dictionary from get_wallets.

        Returns:
            List of AccountBalance entries for non-zero currencies.
        """
        acct_balances: dict[str, Any] = acct_data.get("balances", {})
        margin_req = acct_data.get("marginRequirements", {})
        total_margin = float(margin_req.get("im", 0)) if isinstance(margin_req, dict) else 0.0
        non_zero: dict[str, float] = {}
        grand_total = 0.0
        for curr, amount in acct_balances.items():
            val = float(amount)
            if val == 0:
                continue
            non_zero[curr] = val
            grand_total += val
        entries: list[AccountBalance] = []
        for curr, total in non_zero.items():
            share = (total / grand_total) if grand_total > 0 else 0.0
            used = total_margin * share
            entries.append(AccountBalance(currency=curr, free=total - used, used=used, total=total))
        return entries

    @staticmethod
    def _parse_flex_account(
        acct_name: str,
        acct_data: dict[str, Any],
    ) -> AccountBalance | None:
        """Parse a flex/cash multi-collateral account.

        Args:
            acct_name: Account name (``flex`` or ``cash``).
            acct_data: Raw account dictionary from get_wallets.

        Returns:
            AccountBalance or None if balance is zero.
        """
        balance_value = float(acct_data.get("balanceValue", 0))
        if balance_value == 0:
            return None
        available = min(float(acct_data.get("availableMargin", balance_value)), balance_value)
        used = balance_value - available
        return AccountBalance(
            currency=f"{acct_name}_usd",
            free=available,
            used=max(used, 0.0),
            total=balance_value,
        )

    async def get_balance(self, currency: str | None = None) -> dict[str, AccountBalance]:
        """Fetch wallet balances from Kraken Futures.

        Args:
            currency: Filter by specific currency (optional).

        Returns:
            Dictionary of currency to AccountBalance.

        Raises:
            RuntimeError: If API credentials are missing.
            Exception: If balance fetch fails.
        """
        self._require_authenticated()
        self._record_rest_call()
        result = await asyncio.to_thread(cast(User, self._user_client).get_wallets)
        accounts: dict[str, Any] = result.get("accounts", {})
        balances: dict[str, AccountBalance] = {}
        for acct_name, acct_data in accounts.items():
            if not isinstance(acct_data, dict):
                continue
            if acct_data.get("balances"):
                for coin_bal in self._parse_coin_margin_account(acct_data):
                    balances[coin_bal.currency] = coin_bal
            elif acct_name in ("flex", "cash"):
                flex_bal = self._parse_flex_account(acct_name, acct_data)
                if flex_bal is not None:
                    balances[flex_bal.currency] = flex_bal
        if currency:
            return {k: v for k, v in balances.items() if k == currency}
        return balances

    async def get_open_positions(self) -> list[OpenPositionSnapshot]:
        """Fetch open positions from Kraken Futures.

        Returns:
            List of open position snapshots.

        Raises:
            RuntimeError: If API credentials are missing.
        """
        self._require_authenticated()
        self._record_rest_call()
        result = await asyncio.to_thread(cast(User, self._user_client).get_open_positions)
        raw_positions: list[dict[str, Any]] = result.get("openPositions", [])
        positions: list[OpenPositionSnapshot] = []
        for pos in raw_positions:
            kraken_symbol = (pos.get("symbol") or "").upper()
            try:
                native_symbol = kraken_futures_ws_to_native(kraken_symbol)
            except ValueError:
                native_symbol = kraken_symbol
            side_str = pos.get("side", "long")
            side = OrderSideEnum.BUY if side_str == "long" else OrderSideEnum.SELL
            positions.append(
                OpenPositionSnapshot(
                    symbol=native_symbol,
                    side=side,
                    size=float(pos.get("size", 0)),
                    entry_price=float(pos.get("price", 0)),
                    mark_price=float(pos.get("markPrice", 0)),
                    unrealized_pnl=float(pos.get("unrealizedPnl", 0)),
                    unrealized_funding=float(pos.get("unrealizedFunding", 0)),
                    timestamp=datetime.now(UTC),
                ),
            )
        return positions

    async def get_historical_funding_rates(
        self,
        symbol: str,
    ) -> list[FundingRateSnapshot]:
        """Fetch all historical funding rates for a perpetual contract.

        Calls ``Market.get_historical_funding_rates(symbol)`` on the
        Kraken Futures SDK. The SDK returns **absolute** ``fundingRate``
        (price units per contract per hour) and **relative**
        ``relativeFundingRate`` (fractional, e.g. 7e-05 ≈ 0.007%).
        We store the relative rate since it matches the
        ``max_funding_rate`` cap on InstrumentSpec.

        Args:
            symbol: Kraken WS symbol (e.g., ``PF_XBTUSD``).

        Returns:
            List of FundingRateSnapshot sorted by effective_from ascending.
        """
        if not self._market_client:
            self._market_client = Market(sandbox=self.sandbox)
        self._record_rest_call()
        result = await asyncio.to_thread(
            self._market_client.get_historical_funding_rates,
            symbol,
        )
        if not isinstance(result, dict):
            logger.warning(f"Unexpected SDK response type for historical funding: {type(result)}")
            return []
        raw_rates = result.get("rates", [])
        if not isinstance(raw_rates, list):
            logger.warning(f"Expected list for 'rates', got {type(raw_rates)}")
            return []
        try:
            native_symbol = kraken_futures_ws_to_native(symbol.upper())
        except ValueError:
            native_symbol = symbol
        snapshots: list[FundingRateSnapshot] = []
        for entry in raw_rates:
            if not isinstance(entry, dict):
                continue
            ts_str = entry.get("timestamp", "")
            try:
                effective = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                continue
            raw_rate = entry.get("relativeFundingRate")
            if raw_rate is None:
                continue
            try:
                rate = float(raw_rate)
            except (ValueError, TypeError):
                continue
            snapshots.append(
                FundingRateSnapshot(
                    symbol=native_symbol,
                    exchange=ExchangeEnum.KRAKEN_FUTURES,
                    rate_type="perpetual_funding",
                    direction="both",
                    rate=rate,
                    effective_from=effective,
                    notional_asset="USD",
                    source="exchange_api",
                ),
            )
        snapshots.sort(key=lambda s: s.effective_from)
        return snapshots

    async def get_current_funding_rate(self, symbol: str) -> FundingRateSnapshot | None:
        """Extract the live funding rate from the ticker for a perpetual.

        Calls ``Market.get_tickers()`` and finds the ticker matching
        ``symbol``. Returns None if no ticker found or funding rate is
        absent.

        The REST ticker's ``fundingRate`` is already the **relative**
        (fractional) rate, despite sharing its name with the absolute
        rate field in the historical endpoint. Verified against live
        Kraken API: values are ~1e-05 order of magnitude, matching
        ``relativeFundingRate`` from the WS ticker feed. The REST
        ticker does NOT carry a separate ``relativeFundingRate`` field.

        Args:
            symbol: Kraken WS symbol (e.g., ``PF_XBTUSD``).

        Returns:
            FundingRateSnapshot with the current rate, or None.
        """
        if not self._market_client:
            self._market_client = Market(sandbox=self.sandbox)
        self._record_rest_call()
        result = await asyncio.to_thread(self._market_client.get_tickers)
        if not isinstance(result, dict):
            logger.warning(f"Unexpected SDK response type for tickers: {type(result)}")
            return None
        tickers = result.get("tickers", [])
        if not isinstance(tickers, list):
            logger.warning(f"Expected list for 'tickers', got {type(tickers)}")
            return None
        ticker = self._find_ticker(tickers, symbol.upper())
        if ticker is None:
            return None
        return self._parse_funding_ticker(ticker, symbol)

    def _find_ticker(
        self,
        tickers: list[Any],
        symbol_upper: str,
    ) -> dict[str, Any] | None:
        """Find the ticker dict matching the given symbol.

        Args:
            tickers: List of ticker dicts from SDK.
            symbol_upper: Uppercase Kraken WS symbol to match.

        Returns:
            Matching ticker dict, or None.
        """
        for t in tickers:
            if not isinstance(t, dict):
                continue
            if (t.get("symbol") or "").upper() == symbol_upper:
                return t
        return None

    def _parse_funding_ticker(
        self,
        ticker: dict[str, Any],
        symbol: str,
    ) -> FundingRateSnapshot | None:
        """Parse a funding rate snapshot from a ticker dict.

        Args:
            ticker: Ticker dict from SDK containing fundingRate.
            symbol: Original Kraken WS symbol for fallback.

        Returns:
            FundingRateSnapshot, or None if rate is absent/invalid.
        """
        raw_rate = ticker.get("fundingRate")
        if raw_rate is None:
            return None
        try:
            rate = float(raw_rate)
        except (ValueError, TypeError):
            return None
        symbol_upper = symbol.upper()
        try:
            native_symbol = kraken_futures_ws_to_native(symbol_upper)
        except ValueError:
            native_symbol = symbol
        now = datetime.now(UTC)
        effective = now.replace(minute=0, second=0, microsecond=0)
        return FundingRateSnapshot(
            symbol=native_symbol,
            exchange=ExchangeEnum.KRAKEN_FUTURES,
            rate_type="perpetual_funding",
            direction="both",
            rate=rate,
            effective_from=effective,
            notional_asset="USD",
            source="exchange_api",
        )

    def _convert_sdk_order(self, data: dict[str, Any]) -> ExchangeOrderSnapshot:
        """Convert a Kraken Futures SDK order dict to ExchangeOrderSnapshot.

        Handles both documented field variants from the SDK:
        ``order_id`` / ``orderId``, ``filledSize`` / ``filled``,
        ``unfilledSize`` / ``qty`` / ``quantity``, ``orderType`` / ``type``,
        and lowercase symbols (``pf_xbtusd`` → ``PF_XBTUSD``).

        Args:
            data: Raw order dictionary from SDK.

        Returns:
            ExchangeOrderSnapshot with mapped fields.
        """
        order_id = data.get("order_id") or data.get("orderId", "")
        kraken_symbol = (data.get("symbol") or "").upper()
        try:
            native_symbol = kraken_futures_ws_to_native(kraken_symbol)
        except ValueError:
            native_symbol = kraken_symbol
        filled = float(data.get("filledSize", data.get("filled", 0)))
        unfilled = data.get("unfilledSize")
        if unfilled is not None:
            qty = filled + float(unfilled)
        elif "quantity" in data:
            qty = filled + float(data["quantity"])
        elif "size" in data:
            qty = float(data["size"])
        else:
            qty_raw = data.get("qty")
            qty = float(qty_raw) if qty_raw is not None else filled
        order_type_raw = data.get("orderType") or data.get("type", "lmt")
        return ExchangeOrderSnapshot(
            id=order_id,
            client_order_id=data.get("cliOrdId"),
            symbol=native_symbol,
            side=_map_kraken_side(data.get("side", "buy")),
            type=_map_kraken_order_type(order_type_raw),
            amount=qty,
            price=data.get("limitPrice"),
            status=_map_kraken_status(data.get("status", "placed")),
            filled=filled,
            remaining=qty - filled,
            timestamp=datetime.now(UTC).timestamp(),
        )

    def subscribe_ticks(self, symbols: list[str]) -> AsyncIterator[TickerUpdate]:
        """Subscribe to real-time ticker updates via WebSocket.

        Args:
            symbols: Native symbols (e.g., ``BTC-USD-PERP``) — converted
                to Kraken Futures product IDs internally.

        Returns:
            AsyncIterator yielding TickerUpdate for each price change.
        """
        return self._subscribe_ticks_impl(symbols)

    async def _subscribe_in_chunks(
        self, feed: Literal["ticker", "trade"], ws_symbols: list[str]
    ) -> None:
        """Subscribe to a Kraken Futures feed one product at a time.

        Kraken Futures accepts a first multi-product ``subscribe`` call
        for a feed but live observation on 2026-05-24 showed subsequent
        chunks on the same feed are answered with ``event: alert`` and
        ``Already subscribed to feed, re-requesting`` while the new
        products remain unsubscribed. Use one product per call so every
        configured product receives its own ``event: subscribed`` ACK and
        the health tracker keeps a natural 1:1 pending-to-confirmed
        mapping. Per-product calls also attribute invalid-product alerts
        such as ``Couldn't subscribe to invalid product`` to the product
        that caused them.

        Args:
            feed: Kraken Futures feed name (``ticker`` or ``trade``).
            ws_symbols: WS-format product IDs (e.g. ``PF_XBTUSD``).
        """
        if self._ws_client is None:
            raise RuntimeError(_PUBLIC_WS_NOT_CONNECTED_MSG)
        for product in ws_symbols:
            await self._send_public_subscribe(feed=feed, product=product)

    async def _send_public_subscribe(
        self,
        feed: str,
        product: str,
        *,
        preserve_retry_count: bool = False,
    ) -> None:
        """Send one public WS subscribe with global throttle + mark_pending.

        The lock serializes ticker, trade, replay, and retry paths so they
        cannot collectively exceed the rate-limit. mark_pending is INSIDE
        the lock and AFTER the wait so health-tracker requested_at reflects
        actual send time, preventing false-overdue retries while requests
        queue behind the throttle.

        ``preserve_retry_count`` MUST be ``True`` when called from replay or
        retry paths so the health-tracker retry budget is not wiped. Initial
        subscribes leave it ``False`` so each fresh product starts with a
        full budget.

        The client is snapshotted INSIDE the lock and the SDK send is bounded
        by ``_SDK_SEND_TIMEOUT_S``: the SDK's ``send_message``
        spins forever on a client whose socket was never assigned, and an
        unbounded send here once wedged this lock permanently, starving every
        other subscribe path (the 2026-06-09 connected-but-dark incident). A
        timed-out or failed send raises out of the lock so callers retry and
        the lock is released.

        Raises:
            RuntimeError: If no WebSocket client is connected.
            ValueError: If ``feed`` is not a supported public feed.
            TimeoutError: If the SDK send exceeds the per-send bound.
        """
        if self._ws_client is None:
            raise RuntimeError(_PUBLIC_WS_NOT_CONNECTED_MSG)
        if feed == "ticker":
            channel: Literal["ticker", "trade"] = "ticker"
        elif feed == "trade":
            channel = "trade"
        else:
            raise ValueError(f"Unsupported public subscription feed: {feed}")
        async with self._public_subscribe_lock:
            client = self._ws_client
            if client is None:
                raise RuntimeError(_PUBLIC_WS_NOT_CONNECTED_MSG)
            now = time.monotonic()
            wait_s = self._next_public_subscribe_at - now
            if wait_s > 0:
                await asyncio.sleep(wait_s)
            req = SubscriptionRequest(channel=channel, symbols=(product,), parameters_json="{}")
            self._subscription_cache[req.key()] = req
            self._health_tracker.mark_pending(
                channel, product, preserve_retry_count=preserve_retry_count
            )
            async with asyncio.timeout(_SDK_SEND_TIMEOUT_S):
                await client.subscribe(feed=channel, products=[product])
            self._next_public_subscribe_at = time.monotonic() + _PUBLIC_SUBSCRIBE_MIN_INTERVAL_S

    async def _subscribe_ticks_impl(self, symbols: list[str]) -> AsyncIterator[TickerUpdate]:
        """Implement ticker subscription via callback-to-queue bridge.

        Converts native symbols (e.g., ``BTC-USD-PERP``) to Kraken Futures
        product IDs (e.g., ``PF_XBTUSD``) before subscribing. Kraken Futures
        subscriptions are issued one product per call so each product gets
        a server ACK and a dedicated health-tracker pending entry.

        Cleanup unsubscribes ONLY when ``_ws_client`` still holds the client
        this generator subscribed on — a dying generator must never bulk
        unsubscribe the whole universe from a fresh client installed by a
        concurrent recovery — and the unsubscribe send is bounded so cleanup
        cannot wedge on a socketless client.

        Args:
            symbols: Native symbols to subscribe.

        Yields:
            TickerUpdate for each price change.

        Raises:
            ConnectionError: If the WebSocket connection is lost.
        """
        await self._ensure_ws_connected()
        subscribed_client = self._ws_client
        if subscribed_client is None:
            raise RuntimeError(_PUBLIC_WS_NOT_CONNECTED_MSG)
        ws_symbols = [native_to_kraken_futures_ws(s) for s in symbols]
        await self._subscribe_in_chunks("ticker", ws_symbols)
        logger.info(
            f"Subscribed to Kraken Futures tickers: {len(symbols)} symbols (per-product mode)"
        )
        try:
            while True:
                if self._ws_client and getattr(self._ws_client, "exception_occur", False):
                    raise ConnectionError("Kraken Futures WS connection lost (tickers)")
                try:
                    message = await asyncio.wait_for(
                        self._tick_queue.get(), timeout=_QUEUE_DRAIN_TIMEOUT
                    )
                    yield message
                except TimeoutError:
                    await asyncio.sleep(0.01)
        finally:
            current = self._ws_client
            if current is not None and current is subscribed_client:
                try:
                    async with asyncio.timeout(_SDK_SEND_TIMEOUT_S):
                        await current.unsubscribe(feed="ticker", products=ws_symbols)
                except Exception:
                    logger.debug("Failed to unsubscribe from tickers on cleanup")
                if getattr(current, "exception_occur", False) and self._ws_client is current:
                    self._ws_client = None

    def subscribe_candles(
        self,
        symbols: list[str],
        timeframe: str = "1m",
    ) -> AsyncIterator[CandleUpdate]:
        """Subscribe to 1-minute candles synthesized from the live trade stream.

        Kraken Futures has no WebSocket candle channel. The previous
        implementation polled the REST ``/derivatives/api/v4/charts/`` /
        CCXT ``fetch_ohlcv`` endpoint per symbol per minute, which on
        the 330-product live wildcard meant hundreds of REST calls per
        minute against the venue and was a real IP-ban risk over a
        multi-hour session. This implementation folds every trade
        delivered via the WS ``trade`` / ``trade_snapshot`` channels
        into a per-symbol-per-minute accumulator (see
        :class:`snapper.infrastructure.exchanges._trade_candle_builder.TradeCandleBuilder`)
        and emits the candle once the minute closes. Symbols with no
        trades in a minute simply have no candle row for that minute.

        Args:
            symbols: Native symbols (e.g. ``BTC-USD-PERP``). Currently
                informational only — the builder emits a candle for
                every symbol whose trades it has actually seen.
            timeframe: Candle interval. Only ``"1m"`` is supported;
                anything else raises ``ValueError`` (use
                :meth:`get_ohlcv` for historical / multi-interval
                queries via the existing backfill paths).

        Returns:
            AsyncIterator yielding ``CandleUpdate`` for each completed
            1-minute bucket.

        Raises:
            ValueError: When ``timeframe`` is not ``"1m"``.
        """
        return self._subscribe_candles_impl(symbols, timeframe)

    async def _subscribe_candles_impl(
        self,
        symbols: list[str],
        timeframe: str,
    ) -> AsyncIterator[CandleUpdate]:
        """Implement candle streaming by yielding from :attr:`_candle_queue`.

        Starts the background :meth:`_candle_aggregator` task on first
        call. Cancels it cleanly on iterator close so a publisher
        shutdown does not leak the task.

        Args:
            symbols: Native symbols (informational — see ``subscribe_candles``).
            timeframe: Candle interval. Must be ``"1m"``.

        Yields:
            ``CandleUpdate`` for each completed 1-minute bucket.

        Raises:
            ValueError: When ``timeframe`` is not ``"1m"``.
        """
        if timeframe != "1m":
            raise ValueError(
                f"Kraken Futures only supports 1m candles (synthesized from trades). "
                f"Got {timeframe!r}; use get_ohlcv() for other intervals."
            )
        if self._candle_aggregator_task is None or self._candle_aggregator_task.done():
            self._candle_aggregator_task = asyncio.create_task(self._candle_aggregator())
        try:
            while True:
                try:
                    candle = await asyncio.wait_for(
                        self._candle_queue.get(), timeout=_QUEUE_DRAIN_TIMEOUT
                    )
                    yield candle
                except TimeoutError:
                    await asyncio.sleep(0.01)
        finally:
            task = self._candle_aggregator_task
            if task and not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    async def _candle_aggregator(self) -> None:
        """Emit completed 1-minute candles roughly once per second.

        Wakes every second, asks :attr:`_candle_builder` for any
        bucket whose minute has finished, and routes each into
        :attr:`_candle_queue`.
        """
        while True:
            await asyncio.sleep(1.0)
            for candle in self._candle_builder.pop_completed(datetime.now(UTC)):
                enqueue_or_drop_oldest_candle(self._candle_queue, candle, "Candle")

    def subscribe_trades(self, symbols: list[str]) -> AsyncIterator[TradeUpdate]:
        """Subscribe to real-time trade updates via WebSocket.

        Args:
            symbols: Native symbols (e.g., ``BTC-USD-PERP``) — converted
                to Kraken Futures product IDs internally.

        Returns:
            AsyncIterator yielding TradeUpdate for each trade.
        """
        return self._subscribe_trades_impl(symbols)

    async def _subscribe_trades_impl(self, symbols: list[str]) -> AsyncIterator[TradeUpdate]:
        """Implement trade subscription via callback-to-queue bridge.

        Converts native symbols to Kraken Futures product IDs before subscribing.

        Cleanup unsubscribes ONLY when ``_ws_client`` still holds the client
        this generator subscribed on (see the ticker twin for rationale), and
        the unsubscribe send is bounded so cleanup cannot wedge on a
        socketless client.

        Args:
            symbols: Native symbols to subscribe.

        Yields:
            TradeUpdate for each execution.

        Raises:
            ConnectionError: If the WebSocket connection is lost.
        """
        await self._ensure_ws_connected()
        subscribed_client = self._ws_client
        if subscribed_client is None:
            raise RuntimeError(_PUBLIC_WS_NOT_CONNECTED_MSG)
        ws_symbols = [native_to_kraken_futures_ws(s) for s in symbols]
        await self._subscribe_in_chunks("trade", ws_symbols)
        logger.info(
            f"Subscribed to Kraken Futures trades: {len(symbols)} symbols (per-product mode)"
        )
        try:
            while True:
                if self._ws_client and getattr(self._ws_client, "exception_occur", False):
                    raise ConnectionError("Kraken Futures WS connection lost (trades)")
                try:
                    message = await asyncio.wait_for(
                        self._trade_queue.get(), timeout=_QUEUE_DRAIN_TIMEOUT
                    )
                    yield message
                except TimeoutError:
                    await asyncio.sleep(0.01)
        finally:
            current = self._ws_client
            if current is not None and current is subscribed_client:
                try:
                    async with asyncio.timeout(_SDK_SEND_TIMEOUT_S):
                        await current.unsubscribe(feed="trade", products=ws_symbols)
                except Exception:
                    logger.debug("Failed to unsubscribe from trades on cleanup")
                if getattr(current, "exception_occur", False) and self._ws_client is current:
                    self._ws_client = None

    def subscribe_executions(self) -> AsyncIterator[ExecutionUpdate]:
        """Subscribe to real-time execution updates via authenticated WebSocket.

        Yields ExecutionUpdate from the ``fills`` private channel only.
        The ``open_orders`` channel is excluded because it carries
        cumulative order-state (not incremental fills).

        Returns:
            AsyncIterator yielding ExecutionUpdate for each fill event.

        Raises:
            RuntimeError: If API credentials are missing.
        """
        return self._subscribe_executions_impl()

    async def _subscribe_executions_impl(self) -> AsyncIterator[ExecutionUpdate]:
        """Implement execution subscription via authenticated WS.

        Subscribes to the ``fills`` private channel and yields parsed
        ExecutionUpdate objects for each individual fill.

        Yields:
            ExecutionUpdate for each fill event.

        Raises:
            RuntimeError: If credentials are missing or WS connection fails.
            ConnectionError: If the private WebSocket connection is lost.
        """
        await self._ensure_private_ws_connected()
        subscribed_client = self._private_ws_client
        if subscribed_client is None:
            raise RuntimeError("Private WebSocket client not connected")
        async with asyncio.timeout(_SDK_SEND_TIMEOUT_S):
            await subscribed_client.subscribe(feed="fills")
        logger.info("Subscribed to Kraken Futures private feed: fills")
        try:
            while True:
                if self._private_ws_client and getattr(
                    self._private_ws_client, "exception_occur", False
                ):
                    raise ConnectionError("Kraken Futures private WS connection lost")
                try:
                    update = await asyncio.wait_for(
                        self._execution_queue.get(), timeout=_QUEUE_DRAIN_TIMEOUT
                    )
                    yield update
                except TimeoutError:
                    await asyncio.sleep(0.01)
        finally:
            current = self._private_ws_client
            if current is not None and current is subscribed_client:
                try:
                    async with asyncio.timeout(_SDK_SEND_TIMEOUT_S):
                        await current.unsubscribe(feed="fills")
                except Exception:
                    logger.debug("Failed to unsubscribe from private feeds on cleanup")
                if getattr(current, "exception_occur", False) and (
                    self._private_ws_client is current
                ):
                    self._private_ws_client = None

    def subscribe_instruments(self, **kwargs: Any) -> AsyncIterator[dict[str, Any]]:
        """Subscribe to instrument data via REST.

        Unlike Kraken Spot, Futures has no WS instrument feed.
        This fetches instruments once via REST and yields each.

        Returns:
            AsyncIterator yielding raw instrument dicts.
        """
        return self._subscribe_instruments_impl()

    async def _subscribe_instruments_impl(self) -> AsyncIterator[dict[str, Any]]:
        """Fetch instruments from REST and yield each as a dict.

        Yields:
            Raw instrument dict from Market.get_instruments().
        """
        if not self._market_client:
            self._market_client = Market(sandbox=self.sandbox)
        self._record_rest_call()
        result = await asyncio.to_thread(self._market_client.get_instruments)
        instruments: list[dict[str, Any]] = result.get("instruments", [])
        for inst in instruments:
            yield inst

    def get_instruments_sync(self) -> list[dict[str, Any]]:
        """Fetch all instruments synchronously via REST.

        Returns:
            List of raw instrument dicts from Kraken Futures API.
        """
        if not self._market_client:
            self._market_client = Market(sandbox=self.sandbox)
        result = self._market_client.get_instruments()
        return list(result.get("instruments", []))

    def get_parsed_instrument(self, data: dict[str, Any]) -> InstrumentPairDescriptor:
        """Parse a raw instrument dict into InstrumentPairDescriptor.

        Args:
            data: Raw instrument dict from Kraken Futures API.

        Returns:
            Parsed InstrumentPairDescriptor.
        """
        return parse_kraken_futures_instrument(data)
