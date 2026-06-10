"""Kraken Equities (FCM Futures) exchange client implementation.

This module provides KrakenEquitiesExchangeClient, a market-data-only client
for traditional commodity/index futures on Kraken's equities platform. It supports:

REST API Operations:
    - Instrument metadata (via internal ``iapi.kraken.com`` API).
    - OHLCV history via ``ticker/history`` (delayed=true, per-interval
      {1, 5, 15, 30, 60, 1440} minutes). Used by the historical aggregates
      backfill service — see ``get_ohlcv``.

WebSocket Subscriptions (via SpotWSClient with overridden URL):
    - Public: tickers, trades, and 1-minute candles synthesized
      from trades.

The Kraken Equities WebSocket uses the same v2 protocol as Kraken Spot,
with an additional ``asset_class`` field. This implementation reuses the
``SpotWSClient`` from the Kraken SDK by pointing it at
``wss://ws-equities.kraken.com``.

Limitations:
    - No order execution — ``create_order`` / ``cancel_order`` raise
      ``NotImplementedError`` until the authenticated FCM order API is
      reverse-engineered. Market-data only.
    - No execution subscriptions (``subscribe_executions`` raises
      ``NotImplementedError``).
    - Live candle subscriptions are synthesized from trades and
      support only ``1m``; use REST ``get_ohlcv`` for historical and
      non-1m intervals.
    - ``supports_websocket_executions = False``.
    - Feed is delayed (~10 minutes). TickerUpdate carries the
      envelope-level delayed flag routed through ``_on_ws_message``.
"""

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator
from collections.abc import Callable
from time import monotonic
from typing import Any
from typing import cast

import httpx
from kraken.spot import SpotWSClient
from loguru import logger
from pydantic import ValidationError

from snapper.core.json_types import JsonValue
from snapper.core.types import ExchangeEnum
from snapper.data.repository import Repository
from snapper.infrastructure.exchanges._subscription_health import SubscriptionHealthTracker
from snapper.infrastructure.exchanges._subscription_request import SubscriptionRequest
from snapper.infrastructure.exchanges._subscription_request import canonicalise_parameters
from snapper.infrastructure.exchanges._trade_candle_builder import TradeCandleBuilder
from snapper.infrastructure.exchanges._trade_candle_builder import enqueue_or_drop_oldest_candle
from snapper.infrastructure.exchanges.adapters.kraken_equities import (
    parse_kraken_equities_instrument,
)
from snapper.infrastructure.exchanges.adapters.kraken_equities import parse_kraken_equities_ticker
from snapper.infrastructure.exchanges.adapters.kraken_equities import parse_kraken_equities_trade
from snapper.infrastructure.exchanges.base import ExchangeClientBase
from snapper.infrastructure.exchanges.contracts import AccountBalance
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import ExchangeOrderRequest
from snapper.infrastructure.exchanges.contracts import ExchangeOrderSnapshot
from snapper.infrastructure.exchanges.contracts import ExchangeOrderStatusEnum
from snapper.infrastructure.exchanges.contracts import ExecutionUpdate
from snapper.infrastructure.exchanges.contracts import InstrumentPairDescriptor
from snapper.infrastructure.exchanges.contracts import OhlcvSnapshot
from snapper.infrastructure.exchanges.contracts import TickerSnapshot
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate
from snapper.infrastructure.exchanges.schemas.kraken import KrakenTickerSubscriptionAckSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenTradeSubscriptionAckSchema
from snapper.infrastructure.symbols.functions import native_to_kraken_equities_ws

_NOT_IMPLEMENTED_MSG = "Order execution not available for Kraken Equities (market data only)"
_QUEUE_DRAIN_TIMEOUT = 0.1
_TICK_QUEUE_MAX_SIZE = 30000
"""Tick (ticker) producer queue cap.

Tuned 2026-05-22 from the shared 10000 → 30000. Tickers benefit
materially from the server-side ``_WS_THROTTLE_MS`` parameter (one
update per symbol per throttle-window), so 30k is plenty for the
snapshot=True burst at subscribe time (178 instruments × 1 frame in
<1s) plus reconnect storms. Post-deploy verified zero ticker drops.
"""

_TRADE_QUEUE_MAX_SIZE = 100000
"""Trade producer queue cap.

Tuned 2026-05-22 from the shared 10000 → 100000 — much bigger than
the tick queue. Empirically the Kraken WS ``throttle`` parameter
does NOT effectively limit trade-channel flow (verified post 5000ms
throttle deploy: tickers stopped dropping but trades continued at
49/s sustained drop rate over 30min). Reasonable hypothesis: Kraken
batches trades into time windows but each batch may still carry
many trade events per symbol — so the per-symbol rate cap doesn't
translate into a global frame rate cap the same way it does for
tickers. Pending deeper investigation, the
100k cap buys ~30min of head-room before steady-state overflow,
which covers a full Cloudflare WS-proxy restart cycle. Trades feed
``TradeCandleBuilder`` (candle aggregation); some drop is tolerable
for candle accuracy without affecting real-time ticker pricing.
"""

_CANDLE_QUEUE_MAX_SIZE = 30000
"""Candle producer queue cap (kept at the tick-queue size).

Candles are derived/aggregated at lower frequency than raw trades;
the same 30k headroom that works for tickers works for candles.
"""

_CANDLE_WATERMARK_GRACE_S = 60.0
"""Event-time slack beyond a minute's end before its candle is finalized.

Kraken Equities is a ~10-min DELAYED feed (see module docstring): a single
event-minute's trades arrive across many WS batches. Closing buckets by the
feed's own event watermark (max folded ``trade.timestamp``) plus this grace
lets every batch accumulate into ONE candle instead of fragmenting the minute
into partial, mutually-superseding rows where the last fragment wrongly
becomes the current SCD2 version.
"""

_CANDLE_IDLE_FLUSH_S = 120.0
"""Wall-clock silence after which stranded buckets are flushed.

When the whole delayed feed goes quiet (e.g. session close) no trades arrive
and the event watermark stalls, so the final minute's bucket would never
close. After this much wall-clock time with no trade activity at all (keyed
on the builder's activity counter, not watermark movement) the aggregator
flushes whatever remains so the last bar is not stranded until the next
session reopens.
"""

_WS_THROTTLE_MS = 5000
_WS_CLOSE_TIMEOUT_S = 10.0
"""Upper bound on the WebSocket close so a blackholed socket cannot hang the
liveness-recovery teardown or process shutdown indefinitely."""

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
"""Kraken WS server-side throttle for ticker / trade subscriptions.

Tuned 2026-05-22 from 1000ms → 5000ms. The server then batches updates
per symbol into a 5-second window before forwarding, capping arrival
at ~187 frames / 5s ≈ 37 frames/s for the full Equities universe.
Below the consumer's ~56/s capacity → producer queue stays drained.
Trade-off: tick freshness drops to <=5s — acceptable for Equities
(slow-moving prices, no HFT clients), unacceptable for Kraken Spot
(no throttle there).
"""
_WS_URL = "wss://ws-equities.kraken.com"
_IAPI_BASE_URL = "https://iapi.kraken.com/api/internal/markets"
_INSTRUMENTS_URL = f"{_IAPI_BASE_URL}/all/futures-contracts"
_TICKER_HISTORY_URL_TEMPLATE = f"{_IAPI_BASE_URL}/{{ws_symbol}}/ticker/history"
_INSTRUMENTS_HEADERS = {
    "accept": "application/json",
    "origin": "https://pro.kraken.com",
    "referer": "https://pro.kraken.com/",
}
_TIMEFRAME_TO_INTERVAL: dict[str, int] = {
    "1m": 1,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "1h": 60,
    "1d": 1440,
}
_RESUBSCRIBE_CHUNK_DELAY_S = 5.0
_ALREADY_SUBSCRIBED_ERROR = "Already subscribed"
_WS_CLIENT_NOT_CONNECTED_MSG = "WebSocket client not connected"
_REPLAY_CLIENT_REPLACED_MSG = "WebSocket client replaced during subscription replay"
_CONNECT_OWNERSHIP_LOST_MSG = "WebSocket client replaced during connect"


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


def _timeframe_to_interval(timeframe: str) -> int:
    """Map a Snapper-style timeframe to the iapi ticker/history ``interval`` minutes.

    The Kraken FCM ``iapi.kraken.com`` ``ticker/history`` endpoint accepts
    ``1, 5, 15, 30, 60, 1440`` minute intervals.

    Args:
        timeframe: Snapper timeframe string (e.g. ``"1m"``, ``"1h"``, ``"1d"``).

    Returns:
        The corresponding integer-minute interval accepted by the endpoint.

    Raises:
        ValueError: When ``timeframe`` is not in the supported set. The
            message lists the accepted values so operators can correct the
            call site.
    """
    try:
        return _TIMEFRAME_TO_INTERVAL[timeframe]
    except KeyError as exc:
        allowed = ", ".join(sorted(_TIMEFRAME_TO_INTERVAL))
        raise ValueError(
            f"Unsupported Kraken Equities timeframe {timeframe!r} (allowed: {allowed})"
        ) from exc


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


class KrakenEquitiesExchangeClient(ExchangeClientBase):
    """Kraken Equities exchange client for FCM commodity/index futures.

    Reuses SpotWSClient with ``ws_url`` pointed at ``ws-equities.kraken.com``.
    The WS v2 protocol is identical to Kraken Spot, with an additional
    ``asset_class: "futures_contract"`` param in subscriptions.

    Attributes:
        supports_websocket_executions: Always False (market data only).
    """

    supports_websocket_executions: bool = False

    def __init__(
        self,
        repository: Repository | None = None,
    ) -> None:
        """Initialize Kraken Equities exchange client.

        Args:
            repository: Database repository for logging (optional).
        """
        super().__init__(repository=repository, exchange_name=ExchangeEnum.KRAKEN_EQUITIES)
        self._health_tracker: SubscriptionHealthTracker = SubscriptionHealthTracker()
        self._ws_client: SpotWSClient | None = None
        self._ws_connect_lock: asyncio.Lock = asyncio.Lock()
        self._tick_queue: asyncio.Queue[TickerUpdate] = asyncio.Queue(maxsize=_TICK_QUEUE_MAX_SIZE)
        self._trade_queue: asyncio.Queue[TradeUpdate] = asyncio.Queue(maxsize=_TRADE_QUEUE_MAX_SIZE)
        self._candle_queue: asyncio.Queue[CandleUpdate] = asyncio.Queue(
            maxsize=_CANDLE_QUEUE_MAX_SIZE
        )
        self._candle_builder = TradeCandleBuilder(interval_seconds=60)
        self._candle_aggregator_task: asyncio.Task[None] | None = None
        self._subscription_cache: dict[tuple[str, frozenset[str], str], SubscriptionRequest] = {}

    async def connect(self) -> None:
        """Establish connection (no-op until WS subscription).

        The SpotWSClient is created lazily on first subscription.
        """
        logger.info("Kraken Equities client ready")

    async def disconnect(self) -> None:
        """Close all Kraken Equities connections.

        The close is bounded by ``_WS_CLOSE_TIMEOUT_S`` so a blackholed
        socket cannot hang liveness recovery or shutdown; on timeout or
        close error the client reference is dropped and a fresh one is
        rebuilt on the next connect. The slot clear is compare-and-clear: a
        concurrent ``_ensure_ws_connected`` may have installed a NEWER client
        while this close was in flight, and unconditionally nulling the slot
        would detach that live client (callback still attached, no owner).
        """
        client = self._ws_client
        if client:
            try:
                async with asyncio.timeout(_WS_CLOSE_TIMEOUT_S):
                    await client.close()
            except TimeoutError:
                logger.warning("Kraken Equities WS close timed out - forcing cleanup")
            except Exception as e:
                logger.warning(f"Error closing Kraken Equities WS: {e}")
            if self._ws_client is client:
                self._ws_client = None
        logger.info("Kraken Equities connections closed")

    async def _on_ws_message(self, message: dict[str, Any] | list[Any]) -> None:
        """Route incoming WS messages to the appropriate queue.

        This is the callback passed to SpotWSClient. It dispatches
        ticker and trade updates to their respective queues. The outer
        WS envelope carries the ``delayed`` flag (Kraken FCM publishes
        this once per frame rather than per item); ticker dispatch reads
        it here and passes it into the adapter.

        Args:
            message: Parsed WebSocket message.
        """
        await asyncio.sleep(0)
        if isinstance(message, list) or not isinstance(message, dict):
            return
        channel = message.get("channel", "")
        msg_type = message.get("type", "")
        if channel in {"ticker", "trade"} and msg_type == "snapshot":
            return
        if message.get("method") == "subscribe" and isinstance(message.get("result"), dict):
            self._handle_subscription_ack(message)
            return
        if channel == "ticker" and msg_type == "update":
            raw_delayed = message.get("delayed", False)
            if isinstance(raw_delayed, bool):
                envelope_delayed = raw_delayed
            else:
                logger.warning(
                    f"Kraken Equities envelope 'delayed' not a bool "
                    f"(got {type(raw_delayed).__name__}={raw_delayed!r}); "
                    "defaulting to False"
                )
                envelope_delayed = False
            self._handle_ticker_message(message, envelope_delayed=envelope_delayed)
            return
        if channel == "trade" and msg_type == "update":
            self._handle_trade_message(message)

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
        }
        handler = ack_handlers.get(channel_name or "")
        if handler:
            handler()

    def _handle_ticker_subscription_ack(
        self,
        message: dict[str, Any],
        result_dict: dict[str, Any],
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
                logger.debug("Received equities ticker control message without symbol: {}", message)
                return
            if _subscription_ack_confirms(ticker_ack.success, ticker_ack.error):
                self._health_tracker.mark_confirmed("ticker", symbol)
                return
            error = ticker_ack.error or "unknown subscription error"
            self._health_tracker.mark_failed("ticker", symbol, error)
            logger.warning(
                "Kraken Equities ticker subscription failed symbol={} error={}",
                symbol,
                ticker_ack.error,
            )
        except ValidationError:
            logger.debug("Received non-standard equities ticker control message: {}", message)

    def _handle_trade_subscription_ack(
        self,
        message: dict[str, Any],
        result_dict: dict[str, Any],
    ) -> None:
        """Track trade subscription acknowledgement result.

        Args:
            message: Full subscription ack message.
            result_dict: The result sub-dict from the ack.
        """
        try:
            trade_ack = KrakenTradeSubscriptionAckSchema.model_validate(message)
            symbol = result_dict.get("symbol")
            if not isinstance(symbol, str):
                logger.debug("Received equities trade control message without symbol: {}", message)
                return
            if _subscription_ack_confirms(trade_ack.success, trade_ack.error):
                self._health_tracker.mark_confirmed("trade", symbol)
                return
            error = trade_ack.error or "unknown subscription error"
            self._health_tracker.mark_failed("trade", symbol, error)
            logger.warning(
                "Kraken Equities trade subscription failed symbol={} error={}",
                symbol,
                trade_ack.error,
            )
        except ValidationError:
            logger.debug("Received non-standard equities trade control message: {}", message)

    def _handle_ticker_message(
        self,
        message: dict[str, Any],
        *,
        envelope_delayed: bool,
    ) -> None:
        """Parse and enqueue equities ticker updates.

        Args:
            message: Raw WS frame with ``data`` list of per-symbol items.
            envelope_delayed: Value of the outer envelope's ``delayed`` flag,
                propagated into every resulting TickerUpdate.
        """
        for item in message.get("data", []):
            if isinstance(item, dict):
                wire_symbol = item.get("symbol")
                if isinstance(wire_symbol, str):
                    self._health_tracker.mark_data_seen("ticker", wire_symbol)
            try:
                tick = parse_kraken_equities_ticker(item, envelope_delayed=envelope_delayed)
                _enqueue_or_drop_oldest(self._tick_queue, tick, "Tick")
            except (ValueError, KeyError) as exc:
                logger.debug(f"Skipping unparseable equities ticker: {exc}")

    def _handle_trade_message(self, message: dict[str, Any]) -> None:
        """Parse, enqueue, and fold equities trade updates into the candle builder.

        Each parsed :class:`TradeUpdate` is both placed on the trade
        queue (for downstream consumers that want raw fills) AND fed
        into :attr:`_candle_builder` so the once-per-second
        :meth:`_candle_aggregator` can emit completed 1-minute candles
        synthesized from the trade stream. Kraken Equities has no WS
        OHLC channel, and REST polling 100+ FCM contracts every minute
        risks rate-limit / IP-ban on the iapi endpoint.
        """
        for item in message.get("data", []):
            if isinstance(item, dict):
                wire_symbol = item.get("symbol")
                if isinstance(wire_symbol, str):
                    self._health_tracker.mark_data_seen("trade", wire_symbol)
            try:
                trade = parse_kraken_equities_trade(item)
            except (ValueError, KeyError) as exc:
                logger.debug(f"Skipping unparseable equities trade: {exc}")
                continue
            _enqueue_or_drop_oldest(self._trade_queue, trade, "Trade")
            self._candle_builder.update(trade)

    async def _ensure_ws_connected(self) -> None:
        """Connect the SpotWSClient if not already connected.

        Serialized on ``_ws_connect_lock``: the recovery loop and supervised
        consumer restarts call this concurrently, and unserialized callers
        can build DUPLICATE clients whose teardown writes clobber each
        other's ``_ws_client`` slot (the duplicate-client churn observed on
        Futures in the 2026-06-09 blackout fault test). Under the lock,
        concurrent callers coalesce onto a single client.

        On a build/replay failure or cancellation the partial client is
        torn down via :meth:`disconnect` before the error propagates, so a
        cancelled or failed recovery cannot leak the SDK client, its
        background run task, or its aiohttp session.
        """
        async with self._ws_connect_lock:
            if self._ws_client is not None:
                return
            client = SpotWSClient(
                ws_url=_WS_URL,
                callback=self._on_ws_message,
                no_public=False,
            )
            self._ws_client = client
            try:
                async with asyncio.timeout(_WS_CONNECT_TIMEOUT_S):
                    await client.start()
                logger.info("Kraken Equities WebSocket connected")
                if self._subscription_cache:
                    await self._replay_subscriptions()
                if self._ws_client is not client:
                    raise RuntimeError(_CONNECT_OWNERSHIP_LOST_MSG)
            except BaseException:
                if self._ws_client is client:
                    await self.disconnect()
                else:
                    try:
                        async with asyncio.timeout(_WS_CLOSE_TIMEOUT_S):
                            await client.close()
                    except Exception as exc:
                        logger.warning(f"Error closing disowned Kraken Equities WS: {exc!r}")
                raise

    async def _replay_subscriptions(self) -> None:
        """Replay cached public subscriptions after reconnect.

        Captures the client at entry and aborts (raises) if the
        ``_ws_client`` slot is swapped between chunk sends, so a replay
        aimed at a client that a concurrent path already replaced cannot
        keep sending against the wrong connection.

        Raises:
            RuntimeError: If no client is connected at entry, or the slot
                stops pointing at the entry client between sends.
        """
        client = self._ws_client
        if client is None:
            raise RuntimeError(_WS_CLIENT_NOT_CONNECTED_MSG)
        requests = list(self._subscription_cache.values())
        for index, req in enumerate(requests):
            if self._ws_client is not client:
                raise RuntimeError(_REPLAY_CLIENT_REPLACED_MSG)
            params = {
                "channel": req.channel,
                "symbol": list(req.symbols),
                **cast(dict[str, JsonValue], json.loads(req.parameters_json)),
            }
            for symbol in req.symbols:
                self._health_tracker.mark_pending(
                    req.channel,
                    symbol,
                    preserve_retry_count=True,
                )
            await client.subscribe(params=params)
            if index < len(requests) - 1:
                await asyncio.sleep(_RESUBSCRIBE_CHUNK_DELAY_S)

    async def _retry_subscribe(self, channel: str, symbol: str) -> None:
        """Retry a single Equities subscription without updating replay cache.

        Args:
            channel: Tracker channel key to retry.
            symbol: Kraken Equities wire-format symbol to retry.

        Returns:
            None.

        Raises:
            RuntimeError: If the WebSocket client is not connected.
            ValueError: If the channel key is unsupported.
        """
        if self._ws_client is None:
            raise RuntimeError(_WS_CLIENT_NOT_CONNECTED_MSG)
        if channel not in {"ticker", "trade"}:
            raise ValueError(f"Unsupported subscription health channel: {channel}")
        params: dict[str, JsonValue] = {
            "channel": channel,
            "symbol": cast(list[JsonValue], [symbol]),
            "snapshot": True,
            "throttle": _WS_THROTTLE_MS,
            "asset_class": "futures_contract",
        }
        await self._ws_client.subscribe(params=params)

    async def get_ticker(self, symbol: str) -> TickerSnapshot:
        """Fetch current ticker (not implemented for equities REST).

        Args:
            symbol: Symbol (unused).

        Returns:
            Never returns; always raises.

        Raises:
            NotImplementedError: Always. Use WS subscription instead.
        """
        raise NotImplementedError("Use WebSocket ticker subscription for Kraken Equities")

    async def get_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1m",
        since: int | None = None,
        limit: int | None = None,
    ) -> list[OhlcvSnapshot]:
        """Fetch OHLCV candles for an FCM contract from the internal iapi endpoint.

        Hits ``iapi.kraken.com/api/internal/markets/{ws_symbol}/ticker/history``
        with ``delayed=true`` + ``asset_class=futures_contract``. The endpoint
        returns a bounded default window; ``since`` / ``limit`` are applied
        client-side to match the base-class contract (see
        ``ExchangeClientBase.get_ohlcv`` — ``since`` is Unix milliseconds).

        Each raw row has shape
        ``{time: int (unix seconds), open: str, high: str, low: str, close: str,
        volume_wap: str, volume: str, count: int}``; we coerce the price/volume
        strings to floats and keep ``timestamp`` in Unix seconds to match
        ``OhlcvSnapshot``.

        Args:
            symbol: Contract symbol in native (dash-separated) form, e.g.
                ``"MNQM6-CME"``. Converted to exchange-format
                (``"MNQM6.CME"``) before the request.
            timeframe: Snapper-style timeframe. Allowed values:
                ``"1m", "5m", "15m", "30m", "1h", "1d"``. Others raise
                ``ValueError`` via ``_timeframe_to_interval``.
            since: Start timestamp in Unix **milliseconds**. When provided,
                candles with ``timestamp * 1000 < since`` are filtered out.
            limit: Maximum number of candles to return from the tail of the
                response; ``None`` returns the full server window.

        Returns:
            Ordered list of ``OhlcvSnapshot`` entries (oldest first). Empty
            list when the response contains no data rows for the requested
            window (distinguished from upstream failures — see Raises).

        Raises:
            ValueError: When ``timeframe`` is not supported.
            httpx.HTTPStatusError: Propagated for non-2xx responses so the
                caller can distinguish transient from permanent failures.
            RuntimeError: When the endpoint returns HTTP 200 with an
                application-layer failure envelope (``result=null`` or
                non-empty ``errors``). Raising here prevents a broken
                upstream from being silently observed as an empty candle
                window, which would otherwise cause historical backfill
                to skip rows it should have retried.
        """
        interval = _timeframe_to_interval(timeframe)
        ws_symbol = native_to_kraken_equities_ws(symbol)
        url = _TICKER_HISTORY_URL_TEMPLATE.format(ws_symbol=ws_symbol)
        params = {
            "interval": str(interval),
            "delayed": "true",
            "asset_class": "futures_contract",
        }
        self._record_rest_call()
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url, params=params, headers=_INSTRUMENTS_HEADERS)
            response.raise_for_status()
            payload = response.json()
        result = payload.get("result")
        errors = payload.get("errors") or []
        if result is None or errors:
            raise RuntimeError(
                f"Kraken Equities ticker/history failure for {symbol} "
                f"(interval={interval}m): errors={errors!r}, result={result!r}"
            )
        rows: list[dict[str, Any]] = result.get("data") or []
        snapshots: list[OhlcvSnapshot] = []
        for row in rows:
            try:
                ts_seconds = float(row["time"])
                snapshots.append(
                    OhlcvSnapshot(
                        timestamp=ts_seconds,
                        open=float(row["open"]),
                        high=float(row["high"]),
                        low=float(row["low"]),
                        close=float(row["close"]),
                        volume=float(row["volume"]),
                    )
                )
            except (KeyError, TypeError, ValueError) as exc:
                logger.warning(f"Skipping unparseable equities candle for {symbol}: {exc}")
        if since is not None:
            since_seconds = since / 1000.0
            snapshots = [s for s in snapshots if s.timestamp >= since_seconds]
        if limit is not None and len(snapshots) > limit:
            snapshots = snapshots[-limit:]
        return snapshots

    async def create_order(self, request: ExchangeOrderRequest) -> ExchangeOrderSnapshot:
        """Submit a new order (not available).

        Args:
            request: Order parameters (unused).

        Returns:
            Never returns; always raises.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(_NOT_IMPLEMENTED_MSG)

    async def cancel_order(self, order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        """Cancel an existing order (not available).

        Args:
            order_id: Exchange order ID (unused).
            symbol: Symbol filter (unused).

        Returns:
            Never returns; always raises.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(_NOT_IMPLEMENTED_MSG)

    async def get_order(self, order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        """Fetch order details (not available).

        Args:
            order_id: Exchange order ID (unused).
            symbol: Symbol filter (unused).

        Returns:
            Never returns; always raises.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(_NOT_IMPLEMENTED_MSG)

    async def get_orders(
        self,
        symbol: str | None = None,
        status: ExchangeOrderStatusEnum | None = None,
        limit: int | None = None,
    ) -> list[ExchangeOrderSnapshot]:
        """Fetch orders (not available).

        Args:
            symbol: Symbol filter (unused).
            status: Status filter (unused).
            limit: Maximum results (unused).

        Returns:
            Never returns; always raises.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(_NOT_IMPLEMENTED_MSG)

    async def get_balance(self, currency: str | None = None) -> dict[str, AccountBalance]:
        """Fetch account balance (not available).

        Args:
            currency: Currency filter (unused).

        Returns:
            Never returns; always raises.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(_NOT_IMPLEMENTED_MSG)

    def subscribe_ticks(self, symbols: list[str]) -> AsyncIterator[TickerUpdate]:
        """Subscribe to real-time ticker updates via WebSocket.

        Args:
            symbols: Native symbols (e.g., ``CLM6-NYMEX``) -- converted
                to Kraken Equities format internally.

        Returns:
            AsyncIterator yielding TickerUpdate for each price change.
        """
        return self._subscribe_ticks_impl(symbols)

    async def _subscribe_ticks_impl(self, symbols: list[str]) -> AsyncIterator[TickerUpdate]:
        """Implement ticker subscription via SpotWSClient.

        Args:
            symbols: Native symbols to subscribe.

        Yields:
            TickerUpdate for each price change.
        """
        await self._ensure_ws_connected()
        if self._ws_client is None:
            raise RuntimeError(_WS_CLIENT_NOT_CONNECTED_MSG)
        ws_symbols = [native_to_kraken_equities_ws(s) for s in symbols]
        symbols_json = cast(list[JsonValue], list(ws_symbols))
        params: dict[str, JsonValue] = {
            "channel": "ticker",
            "symbol": symbols_json,
            "snapshot": True,
            "throttle": _WS_THROTTLE_MS,
            "asset_class": "futures_contract",
        }
        req = SubscriptionRequest(
            channel="ticker",
            symbols=tuple(ws_symbols),
            parameters_json=canonicalise_parameters(params),
        )
        self._subscription_cache[req.key()] = req
        for ws_symbol in ws_symbols:
            self._health_tracker.mark_pending("ticker", ws_symbol)
        await self._ws_client.subscribe(params=params)
        logger.info(f"Subscribed to Kraken Equities tickers: {symbols} -> {ws_symbols}")
        try:
            while True:
                try:
                    message = await asyncio.wait_for(
                        self._tick_queue.get(), timeout=_QUEUE_DRAIN_TIMEOUT
                    )
                    yield message
                except TimeoutError:
                    await asyncio.sleep(0.01)
        finally:
            if self._ws_client:
                try:
                    await self._ws_client.subscribe(
                        params={
                            "channel": "ticker",
                            "symbol": ws_symbols,
                            "snapshot": True,
                            "throttle": _WS_THROTTLE_MS,
                            "asset_class": "futures_contract",
                        }
                    )
                except Exception:
                    logger.debug("Failed to unsubscribe from equities tickers on cleanup")

    def subscribe_candles(
        self,
        symbols: list[str],
        timeframe: str = "1m",
    ) -> AsyncIterator[CandleUpdate]:
        """Subscribe to 1-minute candles synthesized from the live trade stream.

        Kraken Equities has no WebSocket OHLC channel. Rather than
        polling the iapi ``ticker/history`` REST endpoint per symbol
        per minute (which would mean hundreds of REST calls per minute
        across the FCM contract universe and risks rate-limit / IP-ban),
        this client folds every parsed trade into a per-symbol-per-
        minute accumulator and emits the candle once the minute is
        complete. Symbols with no trades in a minute simply have no
        candle row for that minute.

        Args:
            symbols: Native dash-separated symbols (e.g. ``MNQM6-CME``).
                Advisory for the public interface; the builder emits
                a candle for every symbol that actually saw trades.
            timeframe: Candle interval. Only ``"1m"`` is supported;
                anything else raises ``ValueError``.

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
            symbols: Native dash-separated symbols (advisory; the
                builder emits whatever trades it has actually seen).
            timeframe: Candle interval. Must be ``"1m"``.

        Yields:
            ``CandleUpdate`` for each completed 1-minute bucket.

        Raises:
            ValueError: When ``timeframe`` is not ``"1m"``.
        """
        if timeframe != "1m":
            raise ValueError(
                f"Kraken Equities only supports 1m candles (synthesized from trades). "
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
        """Emit completed 1-minute candles using the feed's EVENT clock.

        Kraken Equities has no WS OHLC channel and its trade feed is
        delayed ~10 min, so candles are synthesized from trades. Closing
        buckets by wall-clock (the kraken_futures path) would fragment each
        delayed minute into many partial, mutually-superseding rows — the
        last fragment wrongly becoming the current SCD2 version. Instead
        this closes a bucket once the builder's event watermark (the highest
        folded ``trade.timestamp``) has passed the bucket's minute by
        :data:`_CANDLE_WATERMARK_GRACE_S`, so every delayed batch for a
        minute accumulates into ONE correct candle. If the feed goes quiet
        (NO trades at all) the remaining buckets are flushed after
        :data:`_CANDLE_IDLE_FLUSH_S` of wall-clock silence so the final bar
        is not stranded until the next session. The idle timer is keyed on
        the builder's trade-activity counter, NOT on watermark movement, so
        a stream of out-of-order / duplicate / same-minute delayed trades
        (which do not advance the watermark) still counts as activity and
        correctly DEFERS the flush — otherwise the flush could fire
        mid-activity and re-fragment a minute.
        """
        last_update_count = self._candle_builder.update_count
        last_advance = monotonic()
        while True:
            await asyncio.sleep(1.0)
            now_mono = monotonic()
            update_count = self._candle_builder.update_count
            if update_count != last_update_count:
                last_update_count = update_count
                last_advance = now_mono
            candles = self._candle_builder.pop_completed_by_event_watermark(
                _CANDLE_WATERMARK_GRACE_S
            )
            if not candles and now_mono - last_advance >= _CANDLE_IDLE_FLUSH_S:
                candles = self._candle_builder.pop_all()
                last_advance = now_mono
            for candle in candles:
                enqueue_or_drop_oldest_candle(self._candle_queue, candle, "Candle")

    def subscribe_trades(self, symbols: list[str]) -> AsyncIterator[TradeUpdate]:
        """Subscribe to real-time trade updates via WebSocket.

        Args:
            symbols: Native symbols (e.g., ``CLM6-NYMEX``).

        Returns:
            AsyncIterator yielding TradeUpdate for each trade.
        """
        return self._subscribe_trades_impl(symbols)

    async def _subscribe_trades_impl(self, symbols: list[str]) -> AsyncIterator[TradeUpdate]:
        """Implement trade subscription via SpotWSClient.

        Args:
            symbols: Native symbols to subscribe.

        Yields:
            TradeUpdate for each execution.
        """
        await self._ensure_ws_connected()
        if self._ws_client is None:
            raise RuntimeError(_WS_CLIENT_NOT_CONNECTED_MSG)
        ws_symbols = [native_to_kraken_equities_ws(s) for s in symbols]
        symbols_json = cast(list[JsonValue], list(ws_symbols))
        params: dict[str, JsonValue] = {
            "channel": "trade",
            "symbol": symbols_json,
            "snapshot": True,
            "throttle": _WS_THROTTLE_MS,
            "asset_class": "futures_contract",
        }
        req = SubscriptionRequest(
            channel="trade",
            symbols=tuple(ws_symbols),
            parameters_json=canonicalise_parameters(params),
        )
        self._subscription_cache[req.key()] = req
        for ws_symbol in ws_symbols:
            self._health_tracker.mark_pending("trade", ws_symbol)
        await self._ws_client.subscribe(params=params)
        logger.info(f"Subscribed to Kraken Equities trades: {symbols} -> {ws_symbols}")
        try:
            while True:
                try:
                    message = await asyncio.wait_for(
                        self._trade_queue.get(), timeout=_QUEUE_DRAIN_TIMEOUT
                    )
                    yield message
                except TimeoutError:
                    await asyncio.sleep(0.01)
        finally:
            if self._ws_client:
                try:
                    await self._ws_client.subscribe(
                        params={
                            "channel": "trade",
                            "symbol": ws_symbols,
                            "snapshot": True,
                            "throttle": _WS_THROTTLE_MS,
                            "asset_class": "futures_contract",
                        }
                    )
                except Exception:
                    logger.debug("Failed to unsubscribe from equities trades on cleanup")

    def subscribe_executions(self) -> AsyncIterator[ExecutionUpdate]:
        """Subscribe to execution updates (not available).

        Returns:
            Never returns; always raises.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(_NOT_IMPLEMENTED_MSG)

    def subscribe_instruments(self, **kwargs: Any) -> AsyncIterator[dict[str, Any]]:
        """Fetch instruments from REST and yield each as a dict.

        Returns:
            AsyncIterator yielding raw instrument dicts.
        """
        return self._subscribe_instruments_impl()

    async def _subscribe_instruments_impl(self) -> AsyncIterator[dict[str, Any]]:
        """Fetch FCM instruments from internal REST API.

        Yields:
            Raw instrument dict for each tradable contract.
        """
        instruments = await self._fetch_instruments_rest()
        for inst in instruments:
            yield inst

    async def _fetch_instruments_rest(self) -> list[dict[str, Any]]:
        """Fetch all FCM futures contracts from Kraken internal API.

        Returns:
            List of raw instrument dicts (tradable + active only).
        """
        self._record_rest_call()
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(
                _INSTRUMENTS_URL,
                params={"delayed": "true"},
                headers=_INSTRUMENTS_HEADERS,
            )
            response.raise_for_status()
            data = response.json()
        result = data.get("result", {})
        contracts: list[dict[str, Any]] = result.get("data", [])
        active = [c for c in contracts if c.get("tradable") and c.get("status") == "active"]
        logger.info(f"Fetched {len(active)} active FCM contracts (of {len(contracts)} total)")
        return active

    def get_instruments_sync(self) -> list[dict[str, Any]]:
        """Fetch all instruments synchronously via REST.

        Returns:
            List of raw instrument dicts from Kraken internal API.
        """
        with httpx.Client(timeout=30.0) as client:
            response = client.get(
                _INSTRUMENTS_URL,
                params={"delayed": "true"},
                headers=_INSTRUMENTS_HEADERS,
            )
            response.raise_for_status()
            data = response.json()
        result = data.get("result", {})
        contracts: list[dict[str, Any]] = result.get("data", [])
        return [c for c in contracts if c.get("tradable") and c.get("status") == "active"]

    def get_parsed_instrument(self, data: dict[str, Any]) -> InstrumentPairDescriptor:
        """Parse a raw instrument dict into InstrumentPairDescriptor.

        Args:
            data: Raw instrument dict from REST API.

        Returns:
            Parsed InstrumentPairDescriptor.
        """
        return parse_kraken_equities_instrument(data)
