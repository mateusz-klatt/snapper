"""Kraken exchange market data publisher.

This module provides a market data feed publisher for the Kraken exchange.
It streams real-time ticks, trades, and candles via Kraken's WebSocket API
and publishes normalized data to the ZMQ messaging bus.

The publisher handles Kraken-specific symbol conversion and respects the
exchange's WebSocket connection limit of 20 symbols per connection.

Classes
-------
KrakenMarketDataPublisher
    RegisterableProcess for Kraken market data streaming.

Configuration
-------------
Symbols are configured via settings.instruments["kraken"].
The publisher uses public (anonymous) WebSocket connections.

Example:
-------
Register and run via process manager::

    # Configured automatically via @register_process decorator
    # or manually:
    publisher = KrakenMarketDataPublisher(symbols=["BTC-USD", "ETH-USD"])
    await publisher.start()
"""

import asyncio
import time
from collections import deque
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from functools import partial
from typing import Any
from typing import Final
from typing import cast
from uuid import uuid7

from loguru import logger

from snapper.application.process_manager.process_parameters import PublisherSymbolsParameters
from snapper.application.process_manager.registry import register_process
from snapper.config.settings import AppSettings
from snapper.core.types import ExchangeEnum
from snapper.core.types import MarketDataExchange
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRestartPolicyEnum
from snapper.core.types import ProcessRoleEnum
from snapper.data.repository_types import CandleUpsertRow
from snapper.data.repository_types import ShadowCandleUpsertRow
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.implementations.kraken import KrakenExchangeClient
from snapper.infrastructure.exchanges.kraken_sdk_patches import _RECONNECT_LIMIT
from snapper.infrastructure.exchanges.kraken_sdk_patches import _RECONNECT_WINDOW_S
from snapper.infrastructure.exchanges.kraken_sdk_patches import (
    apply_kraken_already_subscribed_filter,
)
from snapper.infrastructure.exchanges.kraken_sdk_patches import apply_kraken_resubscribe_pacing
from snapper.infrastructure.exchanges.kraken_sdk_patches import apply_kraken_retry_after_honoring
from snapper.infrastructure.network.egress_context import _CURRENT_PUBLISHER
from snapper.infrastructure.symbols.functions import get_available_kraken_symbols
from snapper.infrastructure.symbols.functions import kraken_websocket_to_native
from snapper.infrastructure.symbols.functions import native_to_kraken_websocket
from snapper.messaging.publishers.base import MarketDataPublisherService
from snapper.messaging.publishers.base import _CandleRepairKey
from snapper.messaging.publishers.candle_aggregator import SUPPORTED_SYNTHESIS_TIMEFRAMES
from snapper.messaging.publishers.kraken_candle_catchup import CatchupCallbacks
from snapper.messaging.publishers.kraken_candle_catchup import KrakenCandleCatchup
from snapper.messaging.publishers.kraken_candle_catchup import complete_repair_windows
from snapper.messaging.publishers.kraken_candle_catchup import repair_read_start
from snapper.messaging.publishers.minute_completion import MINUTE_SWEEP_MARGIN_S
from snapper.messaging.publishers.minute_completion import confirmed_trade_symbols

apply_kraken_retry_after_honoring()
apply_kraken_already_subscribed_filter()
apply_kraken_resubscribe_pacing()
"""Install kraken-sdk patches at module import.

Idempotent — calling multiple times is a no-op. Importing this module from
the process_manager startup path is the documented installation point for
the kraken-sdk patches.

* :func:`apply_kraken_retry_after_honoring` — 429 Retry-After honoring +
  reconnect watchdog.
* :func:`apply_kraken_already_subscribed_filter` — downgrades benign
  ``Already subscribed`` race-condition warnings from the SDK to DEBUG
  so real subscription failures stay visible.
* :func:`apply_kraken_resubscribe_pacing` — paces the SDK's post-reconnect
  per-symbol subscription replay so a large universe does not burst past
  Kraken's subscribe message-rate limit and dark the stream on reconnect.
"""

_FORCE_WS_RESTART_BACKOFF_S: Final[float] = 5.0
_CATCHUP_REPAIR_SOURCE_ROW_LIMIT: Final[int] = 2880
"""Bound a 120-minute reconciliation expanded to complete enclosing UTC days."""
_LIVENESS_RECOVERY_THRESHOLD_S: Final[int] = 60
"""Message-silence threshold (seconds) before Spot liveness recovery fires.

Lowered from the 300 s base default: the wildcard ticker covers the whole
~1900-symbol Spot universe, so ``_last_message_at`` only goes stale when the
feed is genuinely dark (some symbol ticks every few seconds otherwise), and a
multi-minute outage is detected and recovered in ~1 minute rather than five."""
"""Sleep between disconnect and reconnect during in-process WS restart."""

_NATIVE_CANDLE_LIVENESS_THRESHOLD_S: Final[int] = 300
"""Native-candle silence threshold (seconds) before Spot candle recovery fires.

Spot receives 1m on a dedicated native ``ohlc:1m`` channel that can stall
silently while ticks and trades keep the shared message watchdog satisfied (the
2026-06-18 incident: trades alive ~21h, candles dark, no error). Across the
~1900-symbol Spot universe a liquid pair prints a 1m candle every minute, so
venue-wide candle silence past 300 s (>> the ~60 s cadence, <<< the 21 h outage)
is an unambiguous channel stall and triggers the WS-restart recovery that
re-subscribes the dead channel."""


@register_process(
    "kraken_feed_publisher",
    description="Kraken market data feed publisher",
    priority=20,
    role=ProcessRoleEnum.CORE,
    restart_policy=ProcessRestartPolicyEnum.ALWAYS,
    tags=("market-data", "publisher", "kraken"),
    parameters_model=PublisherSymbolsParameters,
    enabled=True,
    mode=ProcessModeEnum.THREAD,
)
class KrakenMarketDataPublisher(MarketDataPublisherService[KrakenExchangeClient]):
    """Kraken exchange market data publisher.

    Streams real-time market data from Kraken's WebSocket API and publishes
    normalized messages to ZMQ. Handles symbol conversion between native
    format (BTC-USD) and Kraken WebSocket format (XBT/USD).

    Respects Kraken's limit of 20 symbols per WebSocket connection.

    Topics Published:
        - market.kraken.{instrument}.ticks
        - market.kraken.{instrument}.trades
        - market.kraken.{instrument}.candles.{timeframe}
        - system.heartbeats.feed.kraken

    Attributes:
        Inherits all attributes from MarketDataPublisherService.

    Example:
        ::

            publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
            await publisher.start()  # Streams until stopped
    """

    @staticmethod
    def get_default_parameters(settings: AppSettings) -> dict[str, Any]:
        """Get default parameters from settings.

        Args:
            settings: Application settings with instruments config.

        Returns:
            Dictionary with symbols list for Kraken.
        """
        instruments = settings.instruments
        kraken_symbols = instruments.get(ExchangeEnum.KRAKEN, [])
        return {
            "symbols": kraken_symbols,
        }

    def _create_exchange_client(self) -> KrakenExchangeClient:
        """Create anonymous Kraken WebSocket client wired to DB-backed settings.

        The trade-built candle aggregator reads
        ``trade_built_finalize_grace_seconds`` from ``settings`` on every tick.
        The client's own ``__init__`` seeds a bootstrap ``get_settings()`` that
        carries no ``SettingsService``, so that DB-backed read raises
        ``RuntimeError`` and — because the aggregator runs as an unsupervised
        ``create_task`` — silently kills the task, stalling Spot candle
        production while trades keep flowing. Injecting the publisher's
        DB-backed settings keeps the live grace readable so trade-built candles
        are emitted.

        Returns:
            Configured KrakenExchangeClient for public data, wired to the
            publisher's DB-backed settings.
        """
        client = KrakenExchangeClient()
        client.settings = self.settings
        return client

    def _get_exchange_name(self) -> MarketDataExchange:
        """Get exchange identifier.

        Returns:
            "kraken" exchange name.
        """
        return ExchangeEnum.KRAKEN

    def _native_candle_timeframes(self) -> frozenset[str]:
        """Kraken spot's WebSocket OHLC channel serves every supported timeframe.

        Spot is the one venue with native higher-TF OHLC; the synthesize-from-1m
        layer rolls those up from 1m instead, so this set drives the one-time
        warning that synthesis REPLACES the native feed (a documented fidelity
        tradeoff — the user's synthesize-everything decision).

        Returns:
            ``{"1m"}`` plus all synthesizable higher timeframes in native
            mode; empty in trade-built mode because no venue OHLC channel is
            the live 1m source.
        """
        if self.settings.spot_candle_source == "trade_built":
            return frozenset()
        return frozenset({"1m"}) | SUPPORTED_SYNTHESIS_TIMEFRAMES

    def _candle_stream_timeframes(self) -> frozenset[str]:
        """Return timeframes consumed by the live Spot candle loop.

        Returns:
            ``{"1m"}`` in trade-built mode, otherwise the native OHLC set.
        """
        if self.settings.spot_candle_source == "trade_built":
            return frozenset({"1m"})
        return super()._candle_stream_timeframes()

    def _subscribe_candle_stream(
        self, symbols: list[str], timeframe: str
    ) -> AsyncIterator[CandleUpdate]:
        """Subscribe to the selected live Spot candle source.

        Args:
            symbols: Native Spot symbols to subscribe.
            timeframe: Candle timeframe requested by the base candle loop.

        Returns:
            Native OHLC candles by default, or trade-built 1m candles when the
            operator explicitly selects ``spot_candle_source=trade_built``.
        """
        if self.settings.spot_candle_source == "trade_built":
            return self._subscribe_trade_built_live_candles(symbols)
        return super()._subscribe_candle_stream(symbols, timeframe)

    async def _subscribe_trade_built_live_candles(
        self, symbols: list[str]
    ) -> AsyncIterator[CandleUpdate]:
        """Subscribe to calculated Spot candles and mark candle liveness at receipt.

        Args:
            symbols: Native Spot symbols to subscribe.

        Yields:
            Trade-built 1m candle updates from the exchange client.
        """
        client = cast(KrakenExchangeClient, self._exchange_client)
        async for candle in client.subscribe_trade_built_candles(symbols, "1m"):
            self._mark_candle_liveness_progress()
            yield candle

    def _candle_source_for(self, timeframe: str) -> str:
        """Return the provenance tag for Spot live 1m candle rows.

        Args:
            timeframe: Candle timeframe requested by the base candle loop.

        Returns:
            ``"calculated"`` in trade-built mode, otherwise ``"native"``.
        """
        if self.settings.spot_candle_source == "trade_built":
            return "calculated"
        return super()._candle_source_for(timeframe)

    def _supports_forward_fill(self) -> bool:
        """Kraken spot is a continuous 24/7 crypto feed — forward-fill is sound.

        Returns:
            ``True``.
        """
        return True

    def _supports_minute_completion(self) -> bool:
        """Spot may assert a tradeless minute only when its 1m source is trades.

        Kraken spot is a continuous 24/7 crypto venue, so "no bar" can never
        mean "no session" here — it means no fill. That makes a flat bar for an
        observed tradeless minute a true statement, but only in ``trade_built``
        mode: with the native ``ohlc:1m`` channel selected the venue owns the
        1m plane and the sweep's settle arithmetic (built around the trade-built
        finalize grace) does not describe when a bar can still arrive.

        Returns:
            ``True`` in trade-built mode, otherwise ``False``.
        """
        return self.settings.spot_candle_source == "trade_built"

    def _minute_sweep_settle_seconds(self) -> float:
        """Return the settle slack before a Spot minute may be swept.

        The client's trade-built builder holds a bucket until
        ``trade_built_finalize_grace_seconds`` past the minute close and pops it
        on a 1 Hz loop, so a real bar for minute ``M`` lands around
        ``M_end + grace + 1``. Sweeping before that would race a genuine bar.

        Returns:
            The live finalize grace plus the shared scheduling margin.
        """
        return float(self.settings.trade_built_finalize_grace_seconds) + MINUTE_SWEEP_MARGIN_S

    def _confirmed_trade_roster(self) -> set[str]:
        """Return Spot symbols whose ``trade`` subscription is confirmed now.

        Read from the exchange client's subscription-health tracker rather than
        from the symbol mapper: the mapper would name pairs this connection
        never subscribed, and a flat bar for one of those would assert silence
        on a channel nobody was listening to.

        Returns:
            Confirmed native Spot symbols; empty before the client exists.
        """
        client = self._exchange_client
        if client is None:
            return set()
        return confirmed_trade_symbols(
            client.subscription_health_snapshot(), kraken_websocket_to_native
        )

    def _candle_liveness_threshold_s(self) -> int:
        """Enable the candle liveness guard for Spot's live 1m stream.

        Spot is the one venue whose 1m bars arrive on a dedicated native
        WebSocket channel by default. When the live source is trade-built,
        candles are derived from the public trade feed instead of a separate
        OHLC channel, so the candle-only watchdog is disabled and the existing
        message/trade-subscription health paths own recovery.

        Returns:
            Native OHLC silence threshold in native mode, otherwise ``0``.
        """
        if self.settings.spot_candle_source == "trade_built":
            return 0
        return _NATIVE_CANDLE_LIVENESS_THRESHOLD_S

    def _validate_symbols(self, symbols: list[str]) -> list[str]:
        """Validate and filter symbols for Kraken.

        Converts symbols to Kraken WebSocket format to validate them.
        Invalid symbols are logged and skipped. The wildcard ``["*"]``
        passes through untouched — Kraken's WebSocket accepts ``"*"``
        as a subscribe-all sentinel (already used by
        :class:`KrakenSnapshotUpdaterService`) so the publisher
        forwards it verbatim and bypasses both per-symbol mapping and
        the 20-symbol-per-connection limit.

        Args:
            symbols: Input symbols in native format, or ``["*"]``.

        Returns:
            Valid symbols that can be streamed from Kraken, or
            ``["*"]`` when the caller requested subscribe-all.
        """
        if symbols == ["*"]:
            return ["*"]
        native_symbols: list[str] = []
        seen_symbols: set[str] = set()
        for symbol in symbols:
            try:
                native_to_kraken_websocket(symbol)
            except ValueError:
                logger.warning(
                    f"KrakenMarketDataPublisher: Skipping unknown native symbol {symbol}"
                )
                continue
            if symbol in seen_symbols:
                continue
            seen_symbols.add(symbol)
            native_symbols.append(symbol)
        return native_symbols

    def _get_max_symbols_per_connection(self) -> int:
        """Get Kraken's WebSocket symbol limit.

        Returns:
            ``0`` when ``self.symbols == ["*"]`` (wildcard subscribe-all
            uses a single connection unconstrained by the per-symbol
            limit) or ``20`` for the explicit-symbol path.
        """
        if self.symbols == ["*"]:
            return 0
        return 20

    def __init__(self, symbols: list[str]) -> None:
        """Initialise the publisher with reconnect-storm watchdog state.

        Args:
            symbols: Native symbols to subscribe to, or ``["*"]`` for
                wildcard subscribe-all.
        """
        super().__init__(symbols)
        self._reconnect_timestamps: deque[float] = deque(maxlen=_RECONNECT_LIMIT * 2)
        self._restart_lock = asyncio.Lock()
        self._force_ws_restart_task: asyncio.Task[None] | None = None
        self._candle_catchup: KrakenCandleCatchup | None = None
        self._candle_identity_gate = asyncio.Lock()
        self._pending_candle_identities: dict[tuple[str, str, datetime], str] = {}

    async def _resolve_live_candle_public_id(
        self, instrument_public_id: str, timeframe: str, open_at: datetime, native_symbol: str
    ) -> str:
        """Adopt committed identities or reserve one bounded UUID before publication.

        Args:
            instrument_public_id: Canonical instrument identity.
            timeframe: Explicit candle timeframe.
            open_at: Candle business opening boundary.
            native_symbol: Native symbol whose persistence policy owns the row.

        Returns:
            Proven stored identity or the sole unresolved local UUID for this key.

        Raises:
            RuntimeError: When unresolved reservations exhaust the writer capacity.
        """
        if not self._should_persist_row("candles", self._get_data_exchange(), native_symbol):
            return self._resolve_candle_public_id(instrument_public_id, timeframe, open_at)
        async with self._candle_identity_gate:
            key = (instrument_public_id, timeframe, open_at)
            pending = self._pending_candle_identities.get(key)
            if pending is not None:
                return pending
            cached = self._candle_id_cache.get((instrument_public_id, timeframe))
            if cached is not None and cached[0] == open_at:
                return cached[1]
            public_id = await self._require_repository().get_canonical_candle_public_id(
                instrument_public_id, timeframe, open_at
            )
            if public_id is None:
                public_id = self._reserve_live_candle_identity(key)
            if cached is None or cached[0] <= open_at:
                self._candle_id_cache[(instrument_public_id, timeframe)] = (open_at, public_id)
            return public_id

    def _reserve_live_candle_identity(self, key: tuple[str, str, datetime]) -> str:
        """Retain unresolved publication identities within queue and in-flight capacity.

        Queue eviction, failed sends, cancellation and unknown commit acknowledgements
        cannot discard an unresolved identity. Exhaustion refuses a new publication
        rather than guessing or evicting a prior event's UUID.

        Args:
            key: Full natural key whose first event is about to be emitted.

        Returns:
            New UUID7 retained until a durable matching commit acknowledgement.

        Raises:
            RuntimeError: When the finite writer-derived reservation bound is full.
        """
        capacity = self._candle_write_queue.maxsize + self._candle_batch_max_rows
        if capacity <= 0 or len(self._pending_candle_identities) >= capacity:
            raise RuntimeError("unresolved candle identities exhaust bounded writer capacity")
        public_id = str(uuid7())
        self._pending_candle_identities[key] = public_id
        return public_id

    def _bind_pending_candle_identities(self, rows: list[CandleUpsertRow]) -> list[CandleUpsertRow]:
        """Reuse outstanding event UUIDs without mutating the latest-window cache.

        Args:
            rows: Quiet native or synthesized candidate rows.

        Returns:
            Independent candidate copies carrying matching unresolved event identities.
        """
        prepared = []
        for original in rows:
            row = original.copy()
            key = (row["instrument_public_id"], row["timeframe"], row["open_at"])
            public_id = self._pending_candle_identities.get(key)
            if public_id is None:
                cached = self._candle_id_cache.get((row["instrument_public_id"], row["timeframe"]))
                if cached is not None and cached[0] == row["open_at"]:
                    public_id = cached[1]
            if public_id is not None:
                row["public_id"] = public_id
            prepared.append(row)
        return prepared

    def _release_committed_candle_identities(self, rows: list[CandleUpsertRow]) -> None:
        """Release only reservations acknowledged under their exact durable identity.

        Args:
            rows: Actual committed rows or acknowledged native inserts.
        """
        for row in rows:
            key = (row["instrument_public_id"], row["timeframe"], row["open_at"])
            if key in self._pending_candle_identities and self._pending_candle_identities[
                key
            ] == row.get("public_id"):
                self._pending_candle_identities.pop(key, None)

    def _mark_candle_writes_committed(self, rows: list[CandleUpsertRow]) -> None:
        """Preserve writer bookkeeping and retire only acknowledged event identities.

        Args:
            rows: Writer rows whose commit acknowledgement completed.
        """
        super()._mark_candle_writes_committed(rows)
        self._release_committed_candle_identities(rows)

    async def _insert_catchup_candle_rows(self, rows: list[CandleUpsertRow]) -> None:
        """Serialize quiet native commits with live identity choice after REST acquisition.

        Args:
            rows: Full closed native observations acquired outside the local gate.
        """
        async with self._candle_identity_gate:
            prepared = self._bind_pending_candle_identities(rows)
            inserted = await self._require_repository().insert_missing_candles(prepared)
            self._release_committed_candle_identities(inserted)
            await self._retire_acknowledged_candle_identities(prepared)

    async def _persist_historical_candle_rows(self, rows: list[CandleUpsertRow]) -> None:
        """Serialize quiet synthesized commits without touching current-window identity.

        Args:
            rows: Full historically derived synthesized candidate rows.
        """
        async with self._candle_identity_gate:
            prepared = self._bind_pending_candle_identities(rows)
            await self._require_repository().upsert_historical_synthesized_candles(prepared)
            await self._retire_acknowledged_candle_identities(prepared)

    async def _retire_acknowledged_candle_identities(self, rows: list[CandleUpsertRow]) -> None:
        """Retire only affected reservations whose acknowledged stored UUID is proven.

        The owning local gate remains held. A successful replay can report no new
        rows even after an earlier unknown commit succeeded, so a writer-fenced
        canonical lookup proves each remaining affected identity independently.

        Args:
            rows: Bounded quiet candidates whose write or replay was acknowledged.
        """
        keys: set[tuple[str, str, datetime]] = set()
        for row in rows:
            key = (row["instrument_public_id"], row["timeframe"], row["open_at"])
            if key in self._pending_candle_identities:
                keys.add(key)
        for key in keys:
            expected = self._pending_candle_identities.get(key)
            if expected is None:
                continue
            stored = await self._require_repository().get_canonical_candle_public_id(*key)
            if stored == expected and self._pending_candle_identities.get(key) == expected:
                self._pending_candle_identities.pop(key)

    def _suspend_candle_synthesis_across_break(self) -> None:
        """Reset live trade trust and request bounded native history reconciliation."""
        super()._suspend_candle_synthesis_across_break()
        client = self._exchange_client
        if client is not None:
            client.reset_trade_candle_epoch()
        if self._candle_catchup is not None:
            self._candle_catchup.request()

    def _catchup_symbols(self, subscribed: list[str]) -> list[str]:
        """Resolve only subscribed instruments allowed by candle persistence policy.

        Args:
            subscribed: Actual explicit roster or wildcard of this live publisher.

        Returns:
            Canonical persisted Spot symbols, empty without a resolved policy.
        """
        policy = self._persist_policy
        if policy is None:
            return []
        selected = {
            symbol
            for exchange, symbol in policy.iter_persisted_instruments("candles")
            if exchange == ExchangeEnum.KRAKEN
        }
        if "*" in selected:
            selected = set(get_available_kraken_symbols())
        return sorted(
            symbol
            for symbol in selected
            if ("*" in subscribed or symbol in subscribed)
            and policy.should_persist(ExchangeEnum.KRAKEN, "candles", symbol)
        )

    async def _reconcile_catchup_repairs(self, symbol: str, start: datetime, end: datetime) -> None:
        """Queue historical-only repairs proved by every committed source minute.

        Args:
            symbol: Canonical symbol whose bounded acquisition completed.
            start: Inclusive business-time bound of the history reconciliation.
            end: Exclusive closed-minute horizon of the history reconciliation.
        """
        if self._candle_aggregator is None:
            return
        timeframes = [tf for tf in self.settings.timeframes if tf in SUPPORTED_SYNTHESIS_TIMEFRAMES]
        if not timeframes:
            return
        rows = await self._require_repository().get_candles(
            symbol,
            "1m",
            repair_read_start(start, timeframes),
            end - timedelta(microseconds=1),
            ExchangeEnum.KRAKEN,
            datetime.now(UTC),
            limit=_CATCHUP_REPAIR_SOURCE_ROW_LIMIT,
            order="asc",
        )
        for window in complete_repair_windows(rows, start, end, timeframes):
            self._schedule_seeded_incomplete_candle_repair_key(
                _CandleRepairKey(symbol, window.timeframe, window.begin),
                window.minutes,
                historical_only=True,
            )

    async def _start_extra_background_tasks(
        self, symbols_to_subscribe: list[str]
    ) -> list[asyncio.Task[None]]:
        """Start bounded trade-built catchup or the optional native-mode shadow writer.

        Args:
            symbols_to_subscribe: Native Spot symbols selected for this
                publisher connection.

        Returns:
            Supervised bounded catchup in trade-built mode, or the optional
            shadow task in native mode; empty without an applicable client.
        """
        if self.settings.spot_candle_source == "trade_built":
            client = self._exchange_client
            if client is None:
                return []
            self._candle_catchup = KrakenCandleCatchup(
                self._require_repository(),
                client,
                CatchupCallbacks(
                    symbols=partial(self._catchup_symbols, symbols_to_subscribe),
                    resolve=self._ensure_instrument,
                    committed=self._reconcile_catchup_repairs,
                    active=lambda: self.running,
                    tracker=self._tracker,
                    insert_missing=self._insert_catchup_candle_rows,
                ),
            )
            return [asyncio.create_task(self._candle_catchup.run())]
        if not self.settings.spot_trade_built_shadow_enabled:
            return []
        return [
            asyncio.create_task(
                self._supervise_consumer(
                    "shadow_candle:1m",
                    partial(self._shadow_candle_loop, symbols_to_subscribe),
                )
            )
        ]

    async def _shadow_candle_loop(self, symbols: list[str]) -> None:
        """Persist trade-built Spot 1m candles into the shadow table only.

        Args:
            symbols: Native Spot symbols to pass to the trade-built candle
                subscription.

        Returns:
            None.
        """
        client = self._exchange_client
        if client is None:
            logger.error("Exchange client not initialized")
            return
        repository = self._require_repository()
        exchange = self._get_data_exchange()
        async for candle in client.subscribe_trade_built_candles(symbols, "1m"):
            if not self.running:
                break
            instrument_public_id = await self._ensure_instrument(candle.symbol)
            if instrument_public_id is None:
                continue
            row = self._build_shadow_candle_row(candle, instrument_public_id)
            if not self._should_persist_row("candles", exchange, candle.symbol):
                continue
            await repository.upsert_shadow_candles([row])

    def _build_shadow_candle_row(
        self, candle: CandleUpdate, instrument_public_id: str
    ) -> ShadowCandleUpsertRow:
        """Build a shadow candle row from a trade-built candle update.

        Args:
            candle: Completed trade-built Spot 1m candle.
            instrument_public_id: Resolved instrument identity.

        Returns:
            Shadow candle row tagged ``calculated`` and ready for repository
            upsert.
        """
        return {
            "instrument_public_id": instrument_public_id,
            "open_at": candle.interval_begin,
            "timestamp": datetime.now(UTC),
            "timeframe": "1m",
            "open": candle.open,
            "high": candle.high,
            "low": candle.low,
            "close": candle.close,
            "volume": candle.volume,
            "vwap": candle.vwap,
            "trades": candle.trades,
            "source": "calculated",
            "complete": True,
            "session_id": self._tracker.session_id,
            "sequence_id": self._tracker.next_sequence("shadow_candles"),
        }

    async def start(self) -> None:
        """Start the publisher within a connector-registration context.

        Stamps ``_CURRENT_PUBLISHER`` so each ``ConnectSpotWebsocketBase``
        instance constructed during startup is registered against this
        publisher via the patched ``__init__``. The token is reset in
        the ``finally`` clause so the ContextVar does not leak across
        publisher restarts.
        """
        token = _CURRENT_PUBLISHER.set(self)
        try:
            await super().start()
        finally:
            _CURRENT_PUBLISHER.reset(token)

    def _on_sdk_reconnect_attempt(self) -> None:
        """Hook called by the patched SDK reconnect path on every attempt.

        Re-arms the candle aggregator's live epoch FIRST. An SDK-internal
        reconnect is invisible to the liveness watchdog — the socket comes
        back on its own and messages resume, so no stall is ever detected —
        yet the gap is exactly as real, and higher-TF forward-fill would
        carry the pre-gap close straight across it.

        Records the reconnect timestamp and schedules an in-process WS
        restart when more than ``_RECONNECT_LIMIT`` attempts fall inside
        the rolling ``_RECONNECT_WINDOW_S`` window. The publisher process
        itself stays alive; only the WebSocket client is torn down and
        rebuilt via the existing
        ``KrakenExchangeClient.disconnect_websocket`` +
        ``_ensure_ws_connected`` cycle. Per the clean-signal-log rule the
        timestamp deque is cleared after a restart trigger so back-to-back
        storms do not double-fire. A forced restart already in flight is
        not re-scheduled, so a storm cannot spawn overlapping restart
        tasks that race on the same WebSocket client.

        Every attempt — not only the ones that escalate to a restart — opens a
        minute-completion feed break, because the SDK reconnects internally and
        this hook is the only place the publisher learns of it. A minute
        overlapping that window is never asserted and is never retro-filled once
        the socket returns: the resulting hole in the 1m plane is the correct
        record that we were not listening.
        """
        self._suspend_candle_synthesis_across_break()
        self._mark_minute_feed_break()
        now = time.monotonic()
        self._reconnect_timestamps.append(now)
        cutoff = now - _RECONNECT_WINDOW_S
        recent = [t for t in self._reconnect_timestamps if t >= cutoff]
        if len(recent) >= _RECONNECT_LIMIT:
            if self._force_ws_restart_task is not None and not self._force_ws_restart_task.done():
                return
            logger.error(
                "kraken publisher: {} reconnects in {}s — forcing WS restart",
                len(recent),
                _RECONNECT_WINDOW_S,
            )
            self._reconnect_timestamps.clear()
            self._force_ws_restart_task = asyncio.create_task(self._force_ws_restart())

    async def _force_ws_restart(self) -> None:
        """Tear down the WS client and re-establish via existing lifecycle.

        Uses ``KrakenExchangeClient.disconnect_websocket`` for teardown
        and ``_ensure_ws_connected`` for the rebuild — both are pre-existing
        methods on the exchange client, so no new public surface is
        introduced. A 5-second back-off between disconnect and reconnect
        gives the SDK a moment to settle internal state and gives
        Cloudflare time to release any in-flight 429 tracking against
        the same source IP. The rebuild is skipped when the publisher is
        no longer running so a storm restart that overlaps :meth:`stop`
        cannot re-establish a WebSocket after shutdown has begun.

        Opens a minute-completion feed break before the teardown so no minute
        spanning the restart is asserted as observed.
        """
        async with self._restart_lock:
            client = self._exchange_client
            if client is None:
                return
            self._suspend_candle_synthesis_across_break()
            self._mark_minute_feed_break()
            try:
                await client.disconnect_websocket()
            except Exception:
                logger.exception("kraken publisher: disconnect_websocket failed during restart")
            await asyncio.sleep(_FORCE_WS_RESTART_BACKOFF_S)
            if not self.running:
                return
            await client._ensure_ws_connected()

    async def stop(self) -> None:
        """Cancel an in-flight forced WS restart, then stop normally.

        The reconnect-storm watchdog schedules ``_force_ws_restart`` as a
        standalone task outside the base recovery-task set; cancel and join
        it here so a restart in progress cannot outlive shutdown or race the
        base teardown.
        """
        task = self._force_ws_restart_task
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await super().stop()

    async def _attempt_liveness_recovery(self, reason: str) -> None:
        """Recover stale market data by forcing a WS restart.

        Args:
            reason: Liveness trigger reason.

        Returns:
            None.
        """
        logger.error("kraken publisher: liveness recovery triggered ({})", reason)
        if self.settings.spot_candle_source == "trade_built" and reason.startswith(
            "no_candles_for_"
        ):
            logger.warning(
                "kraken publisher: skipping candle-only WS restart in trade_built mode; "
                "calculated candles are recovered by trade feed liveness"
            )
            self._mark_candle_liveness_progress()
            return
        await self._force_ws_restart()

    def _get_liveness_recovery_threshold_s(self) -> int:
        """Return the Spot message-silence threshold before recovery fires.

        Returns:
            ``_LIVENESS_RECOVERY_THRESHOLD_S`` seconds.
        """
        return _LIVENESS_RECOVERY_THRESHOLD_S
