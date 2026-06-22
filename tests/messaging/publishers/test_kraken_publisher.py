"""Unit tests for KrakenMarketDataPublisher."""

import asyncio
from collections import deque
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.process_manager.registry import get_registered_processes
from snapper.config.app import AppSettings
from snapper.core.types import ProcessRestartPolicyEnum
from snapper.data.repository import Repository
from snapper.data.repository_types import CandleUpsertRow
from snapper.data.repository_types import ShadowCandleUpsertRow
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.implementations.kraken import KrakenExchangeClient
from snapper.infrastructure.exchanges.kraken_sdk_patches import _RECONNECT_LIMIT
from snapper.infrastructure.network.egress_context import _CURRENT_PUBLISHER
from snapper.messaging.publishers import kraken as kraken_module
from snapper.messaging.publishers.base import MarketDataPublisherService
from snapper.messaging.publishers.candle_aggregator import SUPPORTED_SYNTHESIS_TIMEFRAMES
from snapper.messaging.publishers.kraken import KrakenMarketDataPublisher


class _ShadowSettings:
    """Minimal settings object for the shadow hook tests."""

    def __init__(self, enabled: bool, source: str = "native") -> None:
        """Store the shadow setting value.

        Args:
            enabled: Value returned by spot_trade_built_shadow_enabled.
            source: Value returned by spot_candle_source.
        """
        self.spot_trade_built_shadow_enabled = enabled
        self.spot_candle_source = source


class _ShadowRepository:
    """Repository fake that records live and shadow candle writes separately."""

    def __init__(self) -> None:
        """Initialize recorded write batches."""
        self.shadow_rows: list[list[ShadowCandleUpsertRow]] = []
        self.live_rows: list[list[CandleUpsertRow]] = []

    async def upsert_shadow_candles(
        self, rows: list[ShadowCandleUpsertRow], session: AsyncSession | None = None
    ) -> int:
        """Record shadow candle rows.

        Args:
            rows: Shadow rows written by the publisher.
            session: Optional repository session.

        Returns:
            Number of rows recorded.
        """
        del session
        self.shadow_rows.append(rows)
        return len(rows)

    async def upsert_candles(
        self, rows: list[CandleUpsertRow], session: AsyncSession | None = None
    ) -> int:
        """Record live candle rows.

        Args:
            rows: Live rows written by the publisher.
            session: Optional repository session.

        Returns:
            Number of rows recorded.
        """
        del session
        self.live_rows.append(rows)
        return len(rows)


class _ShadowClient:
    """Exchange client fake yielding trade-built candles."""

    def __init__(self, candles: list[CandleUpdate]) -> None:
        """Store candles for the subscription iterator.

        Args:
            candles: Candle updates to yield.
        """
        self.candles = candles
        self.calls: list[tuple[list[str], str]] = []
        self.native_calls: list[tuple[list[str], str]] = []
        self.native_stream = self._iter_candles()

    def subscribe_candles(self, symbols: list[str], timeframe: str) -> AsyncIterator[CandleUpdate]:
        """Return an async iterator of native candles.

        Args:
            symbols: Symbols passed by the publisher.
            timeframe: Requested timeframe.

        Returns:
            Async iterator over configured candles.
        """
        self.native_calls.append((symbols, timeframe))
        return self.native_stream

    def subscribe_trade_built_candles(
        self, symbols: list[str], timeframe: str
    ) -> AsyncIterator[CandleUpdate]:
        """Return an async iterator of trade-built candles.

        Args:
            symbols: Symbols passed by the publisher.
            timeframe: Requested timeframe.

        Returns:
            Async iterator over configured candles.
        """
        self.calls.append((symbols, timeframe))
        return self._iter_candles()

    async def _iter_candles(self) -> AsyncIterator[CandleUpdate]:
        """Yield configured candles.

        Yields:
            Candle updates in configured order.
        """
        for candle in self.candles:
            yield candle


def _shadow_candle(symbol: str = "BTC-USD") -> CandleUpdate:
    """Build one completed trade-built candle update.

    Args:
        symbol: Native symbol for the candle.

    Returns:
        Candle update suitable for the shadow publisher path.
    """
    return CandleUpdate(
        symbol=symbol,
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.5,
        vwap=100.2,
        trades=8,
        volume=12.0,
        interval_begin=datetime(2026, 6, 20, 12, 0, tzinfo=UTC),
        interval=60,
        complete=True,
    )


class TestKrakenMarketDataPublisher:
    """Tests for KrakenMarketDataPublisher functionality."""

    def test_registered_restart_policy_is_always(self) -> None:
        """The kraken publisher restarts on any exit (live venue must never stop).

        Given: The kraken feed publisher registered via @register_process,
        When: Its registry entry is inspected,
        Then: restart_policy is ALWAYS.
        """
        entry = get_registered_processes()["kraken_feed_publisher"]
        assert entry.restart_policy == ProcessRestartPolicyEnum.ALWAYS

    def test_create_exchange_client_returns_kraken_client(self) -> None:
        """Verify factory method creates KrakenExchangeClient.

        Given a KrakenMarketDataPublisher instance,
        When _create_exchange_client is called,
        Then it returns a KrakenExchangeClient instance.
        """
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        client = publisher._create_exchange_client()
        assert isinstance(client, KrakenExchangeClient)

    def test_get_exchange_name_returns_kraken(self) -> None:
        """Verify exchange name returns 'kraken'.

        Given a KrakenMarketDataPublisher instance,
        When _get_exchange_name is called,
        Then it returns the literal string 'kraken'.
        """
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        assert publisher._get_exchange_name() == "kraken"

    def test_candle_synthesis_hooks_for_spot(self) -> None:
        """Kraken spot exposes its full native OHLC set and permits forward-fill.

        Given a KrakenMarketDataPublisher,
        When the candle-synthesis hooks are inspected,
        Then it reports every native OHLC timeframe (so synthesis-replaces-native
        warns) and permits forward-fill (continuous 24/7 crypto corpus).
        """
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        assert publisher._supports_forward_fill() is True
        assert publisher._native_candle_timeframes() == frozenset({"1m"}) | (
            SUPPORTED_SYNTHESIS_TIMEFRAMES
        )
        assert publisher._candle_stream_timeframes() == frozenset({"1m"}) | (
            SUPPORTED_SYNTHESIS_TIMEFRAMES
        )

    def test_trade_built_mode_keeps_1m_stream_without_native_claim(self) -> None:
        """Trade-built mode starts the 1m loop without claiming venue OHLC.

        Given a Kraken Spot publisher in trade-built mode,
        When its candle timeframe hooks are inspected,
        Then 1m remains stream-consumed but no timeframe is reported native.

        Returns:
            None.
        """
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        publisher.settings = cast(AppSettings, _ShadowSettings(False, "trade_built"))
        assert publisher._native_candle_timeframes() == frozenset()
        assert publisher._candle_stream_timeframes() == frozenset({"1m"})
        assert publisher._candle_liveness_threshold_s() == 300

    def test_subscribe_candle_stream_native_mode_uses_native_ohlc(self) -> None:
        """Native mode delegates the live candle stream to subscribe_candles.

        Given a Kraken Spot publisher in default native mode,
        When the candle stream hook is called,
        Then the exchange client's native candle subscription is used.

        Returns:
            None.
        """
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        client = _ShadowClient([])
        publisher.settings = cast(AppSettings, _ShadowSettings(False, "native"))
        publisher._exchange_client = cast(KrakenExchangeClient, client)
        stream = publisher._subscribe_candle_stream(["BTC-USD"], "1m")
        assert stream is client.native_stream
        assert client.native_calls == [(["BTC-USD"], "1m")]
        assert client.calls == []

    def test_subscribe_candle_stream_trade_built_mode_uses_trade_built(self) -> None:
        """Trade-built mode delegates the live candle stream to trades.

        Given a Kraken Spot publisher in trade-built mode,
        When the candle stream hook is called,
        Then the exchange client's trade-built candle subscription is used.

        Returns:
            None.
        """
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        client = _ShadowClient([])
        publisher.settings = cast(AppSettings, _ShadowSettings(False, "trade_built"))
        publisher._exchange_client = cast(KrakenExchangeClient, client)
        stream = publisher._subscribe_candle_stream(["BTC-USD"], "1m")
        assert stream is not client.native_stream
        assert client.calls == [(["BTC-USD"], "1m")]
        assert client.native_calls == []

    def test_candle_source_for_tracks_spot_source_setting(self) -> None:
        """The live Spot candle provenance follows the selected source.

        Given Kraken Spot publishers in native and trade-built modes,
        When the candle source hook is read,
        Then native mode stays native and trade-built mode is calculated.

        Returns:
            None.
        """
        native = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        native.settings = cast(AppSettings, _ShadowSettings(False, "native"))
        trade_built = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        trade_built.settings = cast(AppSettings, _ShadowSettings(False, "trade_built"))
        assert native._candle_source_for("1m") == "native"
        assert trade_built._candle_source_for("1m") == "calculated"

    @pytest.mark.asyncio
    async def test_trade_built_live_candle_updates_liveness_watermark(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Trade-built live candles still refresh the candle watchdog.

        Given a Kraken Spot publisher in trade-built mode,
        When a live 1m candle flows through the normal candle processor,
        Then the row is calculated and the candle liveness watermark advances.

        Args:
            monkeypatch: Pytest monkeypatch fixture.

        Returns:
            None.
        """
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        publisher.settings = cast(AppSettings, _ShadowSettings(False, "trade_built"))
        publisher._ensure_instrument = AsyncMock(
            return_value="00000000-0000-7000-8000-000000000501"
        )
        publisher._publish_message = AsyncMock()
        publisher._last_candle_msg_at = 0.0
        monkeypatch.setattr("snapper.messaging.publishers.base.monotonic", lambda: 700.0)
        row = await publisher._process_candle(_shadow_candle(), "kraken", "1m")
        assert row is not None
        assert row["source"] == "calculated"
        assert publisher._last_candle_msg_at == 700.0
        assert bool(publisher._candle_stream_timeframes()) is True
        assert publisher._candle_liveness_threshold_s() == 300

    def test_validate_symbols_filters_invalid(self) -> None:
        """Verify invalid symbols are filtered out during validation.

        Given a publisher and symbols including one that raises ValueError,
        When _validate_symbols is called,
        Then invalid symbols are excluded from result.
        """
        publisher = KrakenMarketDataPublisher(symbols=[])
        with patch(
            "snapper.messaging.publishers.kraken.native_to_kraken_websocket",
            side_effect=lambda s: s if s != "INVALID" else (_ for _ in ()).throw(ValueError()),
        ):
            valid_symbols = publisher._validate_symbols(["BTC-USD", "INVALID", "ETH-USD"])
        assert valid_symbols == ["BTC-USD", "ETH-USD"]

    def test_validate_symbols_removes_duplicates(self) -> None:
        """Verify duplicate symbols are removed during validation.

        Given a publisher and symbols list with duplicates,
        When _validate_symbols is called,
        Then duplicates are removed from result.
        """
        publisher = KrakenMarketDataPublisher(symbols=[])
        with patch(
            "snapper.messaging.publishers.kraken.native_to_kraken_websocket", return_value="BTC/USD"
        ):
            valid_symbols = publisher._validate_symbols(["BTC-USD", "BTC-USD", "ETH-USD"])
        assert len(valid_symbols) == 2
        assert valid_symbols == ["BTC-USD", "ETH-USD"]

    def test_get_max_symbols_per_connection_returns_20(self) -> None:
        """Verify Kraken WebSocket limit is 20 symbols per connection.

        Given a KrakenMarketDataPublisher instance,
        When _get_max_symbols_per_connection is called,
        Then it returns 20.
        """
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        assert publisher._get_max_symbols_per_connection() == 20

    def test_validate_symbols_passes_through_wildcard(self) -> None:
        """Verify ``["*"]`` survives validation as a subscribe-all sentinel.

        Given: A KrakenMarketDataPublisher constructed with ``["*"]``,
        When: ``_validate_symbols`` is invoked on the wildcard list,
        Then: It returns ``["*"]`` verbatim — bypassing per-symbol
            ``native_to_kraken_websocket`` mapping. Kraken's WebSocket
            accepts ``"*"`` as subscribe-all (already used by
            ``KrakenSnapshotUpdaterService``).
        """
        publisher = KrakenMarketDataPublisher(symbols=["*"])
        assert publisher._validate_symbols(["*"]) == ["*"]
        assert publisher.symbols == ["*"]

    def test_get_max_symbols_per_connection_unlimited_for_wildcard(self) -> None:
        """Wildcard subscribers run a single connection without per-symbol cap.

        Given: A KrakenMarketDataPublisher constructed with ``["*"]``,
        When: ``_get_max_symbols_per_connection`` is invoked,
        Then: It returns ``0`` (unlimited) so the base class does not
            truncate the wildcard sentinel to the first 20 entries.
        """
        publisher = KrakenMarketDataPublisher(symbols=["*"])
        assert publisher._get_max_symbols_per_connection() == 0

    def test_get_default_parameters_from_settings(self) -> None:
        """Verify default kwargs extracts kraken symbols from settings.

        Given settings with instruments.kraken containing symbols,
        When get_default_parameters is called,
        Then returns dict with those symbols.
        """
        mock_settings = MagicMock(spec=AppSettings)
        mock_settings.instruments = {"kraken": ["BTC-USD", "ETH-USD"], "walutomat": ["EUR-PLN"]}
        kwargs = KrakenMarketDataPublisher.get_default_parameters(mock_settings)
        assert kwargs == {"symbols": ["BTC-USD", "ETH-USD"]}

    def test_get_default_parameters_with_empty_kraken_list(self) -> None:
        """Verify default kwargs handles empty kraken instrument list.

        Given settings with instruments.kraken as empty list,
        When get_default_parameters is called,
        Then returns dict with empty symbols list.
        """
        mock_settings = MagicMock(spec=AppSettings)
        mock_settings.instruments = {"kraken": [], "walutomat": ["EUR-PLN"]}
        kwargs = KrakenMarketDataPublisher.get_default_parameters(mock_settings)
        assert kwargs == {"symbols": []}

    def test_get_default_parameters_with_missing_kraken_key(self) -> None:
        """Verify default kwargs handles missing kraken key in instruments.

        Given settings without 'kraken' key in instruments dict,
        When get_default_parameters is called,
        Then returns dict with empty symbols list.
        """
        mock_settings = MagicMock(spec=AppSettings)
        mock_settings.instruments = {"walutomat": ["EUR-PLN"]}
        kwargs = KrakenMarketDataPublisher.get_default_parameters(mock_settings)
        assert kwargs == {"symbols": []}

    @pytest.mark.asyncio
    async def test_shadow_background_hook_disabled_returns_empty(self) -> None:
        """The Spot shadow task is disabled by default.

        Given: A Kraken publisher with the shadow setting disabled,
        When: the extra background task hook runs,
        Then: no task is started.

        Returns:
            None.
        """
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        publisher.settings = cast(AppSettings, _ShadowSettings(False))
        assert await publisher._start_extra_background_tasks(["BTC-USD"]) == []

    @pytest.mark.asyncio
    async def test_shadow_background_hook_enabled_starts_consumer(self) -> None:
        """The Spot shadow setting starts one supervised consumer task.

        Given: A Kraken publisher with the shadow setting enabled,
        When: the extra background task hook runs,
        Then: one task starts the shadow candle loop with the selected symbols.

        Returns:
            None.
        """
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        publisher.settings = cast(AppSettings, _ShadowSettings(True))
        started = asyncio.Event()
        captured: list[list[str]] = []

        async def _fake_shadow_loop(symbols: list[str]) -> None:
            """Record symbols and wait until cancelled.

            Args:
                symbols: Symbols passed into the shadow loop.

            Returns:
                None.
            """
            captured.append(symbols)
            started.set()
            await asyncio.Event().wait()

        publisher.running = True
        publisher._shadow_candle_loop = _fake_shadow_loop
        tasks = await publisher._start_extra_background_tasks(["BTC-USD"])
        try:
            await asyncio.wait_for(started.wait(), timeout=1.0)
            assert len(tasks) == 1
            assert captured == [["BTC-USD"]]
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    @pytest.mark.asyncio
    async def test_shadow_background_hook_trade_built_mode_returns_empty(self) -> None:
        """Trade-built live mode suppresses the shadow A/B writer.

        Given: A Kraken publisher with shadow enabled but live source trade-built,
        When: the extra background task hook runs,
        Then: no shadow task is started because the trade-built stream is live.

        Returns:
            None.
        """
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        publisher.settings = cast(AppSettings, _ShadowSettings(True, "trade_built"))
        assert await publisher._start_extra_background_tasks(["BTC-USD"]) == []

    @pytest.mark.asyncio
    async def test_shadow_candle_loop_returns_when_client_missing(self) -> None:
        """The shadow loop exits cleanly before startup attaches a client.

        Given: A Kraken publisher without an exchange client,
        When: the shadow loop is called,
        Then: it returns without writing rows.

        Returns:
            None.
        """
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        publisher.running = True
        repository = _ShadowRepository()
        publisher.repository = cast(Repository, repository)
        await publisher._shadow_candle_loop(["BTC-USD"])
        assert repository.shadow_rows == []

    @pytest.mark.asyncio
    async def test_shadow_candle_loop_breaks_when_stopped(self) -> None:
        """The shadow loop honors the running flag before row processing.

        Given: A shadow candle arrives after running has been cleared,
        When: the loop receives it,
        Then: it breaks before resolving instruments or writing rows.

        Returns:
            None.
        """
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        repository = _ShadowRepository()
        client = _ShadowClient([_shadow_candle()])
        publisher.repository = cast(Repository, repository)
        publisher._exchange_client = cast(KrakenExchangeClient, client)
        publisher.running = False
        publisher._ensure_instrument = AsyncMock(return_value="unused")
        await publisher._shadow_candle_loop(["BTC-USD"])
        assert client.calls == [(["BTC-USD"], "1m")]
        publisher._ensure_instrument.assert_not_awaited()
        assert repository.shadow_rows == []

    @pytest.mark.asyncio
    async def test_shadow_candle_loop_skips_unknown_instrument(self) -> None:
        """Unknown symbols do not reach the shadow repository.

        Given: A trade-built candle whose instrument cannot be resolved,
        When: the shadow loop processes it,
        Then: no shadow write occurs.

        Returns:
            None.
        """
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        repository = _ShadowRepository()
        publisher.repository = cast(Repository, repository)
        publisher._exchange_client = cast(KrakenExchangeClient, _ShadowClient([_shadow_candle()]))
        publisher.running = True
        publisher._ensure_instrument = AsyncMock(return_value=None)
        publisher._should_persist_row = MagicMock(return_value=True)
        await publisher._shadow_candle_loop(["BTC-USD"])
        publisher._should_persist_row.assert_not_called()
        assert repository.shadow_rows == []

    @pytest.mark.asyncio
    async def test_shadow_candle_loop_applies_persist_policy_skip(self) -> None:
        """The existing candle persist policy gates shadow writes.

        Given: A resolved trade-built candle filtered by the persist policy,
        When: the shadow loop processes it,
        Then: the repository is not called.

        Returns:
            None.
        """
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        repository = _ShadowRepository()
        publisher.repository = cast(Repository, repository)
        publisher._exchange_client = cast(KrakenExchangeClient, _ShadowClient([_shadow_candle()]))
        publisher.running = True
        publisher._ensure_instrument = AsyncMock(
            return_value="00000000-0000-7000-8000-000000000501"
        )
        publisher._should_persist_row = MagicMock(return_value=False)
        await publisher._shadow_candle_loop(["BTC-USD"])
        publisher._should_persist_row.assert_called_once_with("candles", "kraken", "BTC-USD")
        assert repository.shadow_rows == []

    @pytest.mark.asyncio
    async def test_shadow_candle_loop_writes_only_shadow_rows(self) -> None:
        """The shadow path never touches the live candle pipeline.

        Given: A resolved trade-built Spot candle that passes the persist policy,
        When: the shadow loop processes it,
        Then: only upsert_shadow_candles receives a calculated complete 1m row.

        Returns:
            None.
        """
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        repository = _ShadowRepository()
        publisher.repository = cast(Repository, repository)
        publisher._exchange_client = cast(KrakenExchangeClient, _ShadowClient([_shadow_candle()]))
        publisher.running = True
        instrument_public_id = "00000000-0000-7000-8000-000000000501"
        publisher._ensure_instrument = AsyncMock(return_value=instrument_public_id)
        publisher._should_persist_row = MagicMock(return_value=True)
        publisher._process_candle = AsyncMock(side_effect=AssertionError("live candle path"))
        publisher._observe_native_candle = MagicMock(side_effect=AssertionError("native observer"))
        publisher._enqueue_finalized_candles = MagicMock(side_effect=AssertionError("live queue"))
        publisher._publish_synthesized_candle = AsyncMock(
            side_effect=AssertionError("synth publisher")
        )
        publisher._resolve_candle_public_id = MagicMock(
            side_effect=AssertionError("live candle id")
        )
        publisher._candle_source_for = MagicMock(side_effect=AssertionError("live source"))
        await publisher._shadow_candle_loop(["BTC-USD"])
        assert repository.live_rows == []
        assert len(repository.shadow_rows) == 1
        row = repository.shadow_rows[0][0]
        assert row["instrument_public_id"] == instrument_public_id
        assert row["timeframe"] == "1m"
        assert row["source"] == "calculated"
        assert row["complete"] is True
        assert row["open"] == pytest.approx(100.0)
        assert publisher._candle_write_queue.empty()
        publisher._process_candle.assert_not_awaited()
        publisher._resolve_candle_public_id.assert_not_called()


class TestKrakenReconnectWatchdog:
    """Tests for the publisher-side reconnect-storm watchdog.

    The watchdog adds an in-process WS-restart mechanism so the publisher
    recovers from reconnect cascades (e.g. HTTP 429 rate-limit storms)
    without leaving the SDK to silently exhaust ``MAX_RECONNECT_NUM`` and
    die.
    """

    def test_init_creates_reconnect_state(self) -> None:
        """Spec — full Given/When/Then below.

        Given a fresh publisher,
        When constructed with explicit symbols,
        Then the reconnect-storm deque and lock are initialised empty.
        """
        pub = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        assert list(pub._reconnect_timestamps) == []
        assert pub._restart_lock is not None

    def test_liveness_threshold_is_60s(self) -> None:
        """Spec — full Given/When/Then below.

        Given a Spot publisher,
        When the liveness-recovery threshold is read,
        Then it is the lowered 60 second realtime-venue threshold.
        """
        pub = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        assert pub._get_liveness_recovery_threshold_s() == 60

    def test_candle_liveness_threshold_is_300s(self) -> None:
        """Spec — full Given/When/Then below.

        Given a Spot publisher whose 1m bars arrive on a dedicated native
            ohlc channel that can stall independently of ticks and trades,
        When the native-candle liveness threshold is read,
        Then it is the 300 second venue-wide candle-silence threshold.
        """
        pub = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        assert pub._candle_liveness_threshold_s() == 300

    def test_storm_under_limit_does_not_schedule_restart(self) -> None:
        """Spec — full Given/When/Then below.

        Given fewer than ``_RECONNECT_LIMIT`` attempts in the window,
        When ``_on_sdk_reconnect_attempt`` is called,
        Then no restart task is scheduled.
        """
        pub = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        for _ in range(_RECONNECT_LIMIT - 1):
            pub._on_sdk_reconnect_attempt()
        assert len(pub._reconnect_timestamps) == _RECONNECT_LIMIT - 1

    @pytest.mark.asyncio
    async def test_storm_over_limit_schedules_restart(self) -> None:
        """Spec — full Given/When/Then below.

        Given ``_RECONNECT_LIMIT`` attempts in the window,
        When ``_on_sdk_reconnect_attempt`` is called for the Nth time,
        Then a restart task is scheduled and the deque is cleared so
        back-to-back storms do not double-fire.
        """
        pub = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        pub._force_ws_restart = AsyncMock()
        for _ in range(_RECONNECT_LIMIT):
            pub._on_sdk_reconnect_attempt()
        await asyncio.sleep(0)
        assert pub._reconnect_timestamps == deque(maxlen=_RECONNECT_LIMIT * 2)
        pub._force_ws_restart.assert_called_once()

    @pytest.mark.asyncio
    async def test_storm_does_not_double_schedule_while_restart_in_flight(self) -> None:
        """Spec — full Given/When/Then below.

        Given a forced WS restart already in flight,
        When another storm crosses the limit,
        Then no second restart task is scheduled and the in-flight task is
        left untouched.
        """
        pub = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        in_flight = asyncio.Event()

        async def _hang() -> None:
            await in_flight.wait()

        pub._force_ws_restart = AsyncMock(side_effect=_hang)
        for _ in range(_RECONNECT_LIMIT):
            pub._on_sdk_reconnect_attempt()
        await asyncio.sleep(0)
        first_task = pub._force_ws_restart_task
        for _ in range(_RECONNECT_LIMIT):
            pub._on_sdk_reconnect_attempt()
        await asyncio.sleep(0)
        assert pub._force_ws_restart_task is first_task
        pub._force_ws_restart.assert_called_once()
        in_flight.set()
        assert first_task is not None
        await first_task

    @pytest.mark.asyncio
    async def test_force_ws_restart_calls_disconnect_then_ensure(self) -> None:
        """Spec — full Given/When/Then below.

        Given a publisher with an attached exchange client,
        When ``_force_ws_restart`` is awaited,
        Then ``disconnect_websocket`` is called, the back-off sleep runs,
        and ``_ensure_ws_connected`` is called afterwards.
        """
        pub = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        pub.running = True
        client = MagicMock()
        client.disconnect_websocket = AsyncMock()
        client._ensure_ws_connected = AsyncMock()
        pub._exchange_client = client

        with patch.object(kraken_module.asyncio, "sleep", new=AsyncMock()) as sleep_mock:
            await pub._force_ws_restart()
        client.disconnect_websocket.assert_called_once()
        sleep_mock.assert_called_once()
        client._ensure_ws_connected.assert_called_once()

    @pytest.mark.asyncio
    async def test_force_ws_restart_skips_reconnect_when_stopped(self) -> None:
        """Spec — full Given/When/Then below.

        Given a storm restart that runs while the publisher is stopping,
        When ``_force_ws_restart`` reaches the rebuild step with
        ``running`` False,
        Then it tears the socket down but does NOT re-establish a new one,
        so a restart overlapping shutdown cannot revive the WebSocket.
        """
        pub = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        pub.running = False
        client = MagicMock()
        client.disconnect_websocket = AsyncMock()
        client._ensure_ws_connected = AsyncMock()
        pub._exchange_client = client
        with patch.object(kraken_module.asyncio, "sleep", new=AsyncMock()):
            await pub._force_ws_restart()
        client.disconnect_websocket.assert_called_once()
        client._ensure_ws_connected.assert_not_called()

    @pytest.mark.asyncio
    async def test_stop_cancels_force_ws_restart_task(self) -> None:
        """Spec — full Given/When/Then below.

        Given a publisher with an in-flight forced-restart task,
        When ``stop`` is called,
        Then the task is cancelled before the base shutdown runs.
        """
        pub = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        pub.running = False
        task = asyncio.create_task(asyncio.sleep(60))
        pub._force_ws_restart_task = task
        await pub.stop()
        assert task.cancelled()

    @pytest.mark.asyncio
    async def test_stop_without_force_ws_restart_task(self) -> None:
        """Spec — full Given/When/Then below.

        Given a publisher with no in-flight forced-restart task,
        When ``stop`` is called,
        Then it proceeds to the base shutdown without error.
        """
        pub = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        pub.running = False
        await pub.stop()
        assert pub._force_ws_restart_task is None

    @pytest.mark.asyncio
    async def test_force_ws_restart_skips_when_no_client(self) -> None:
        """Spec — full Given/When/Then below.

        Given a publisher whose exchange client has not been created,
        When ``_force_ws_restart`` is awaited,
        Then the method returns immediately without raising.
        """
        pub = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        pub._exchange_client = None
        await pub._force_ws_restart()

    @pytest.mark.asyncio
    async def test_force_ws_restart_swallows_disconnect_exception(self) -> None:
        """Spec — full Given/When/Then below.

        Given a disconnect that raises an exception,
        When ``_force_ws_restart`` runs,
        Then the exception is logged and the rebuild path still proceeds.
        """
        pub = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        pub.running = True
        client = MagicMock()
        client.disconnect_websocket = AsyncMock(side_effect=RuntimeError("boom"))
        client._ensure_ws_connected = AsyncMock()
        pub._exchange_client = client
        with patch.object(kraken_module.asyncio, "sleep", new=AsyncMock()):
            await pub._force_ws_restart()
        client._ensure_ws_connected.assert_called_once()

    @pytest.mark.asyncio
    async def test_attempt_liveness_recovery_calls_force_ws_restart(self) -> None:
        """Liveness recovery delegates to the existing restart helper.

        Given: A Kraken publisher with a patched force restart helper,
        When: Liveness recovery is attempted,
        Then: The force restart helper is awaited once.
        """
        pub = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        pub._force_ws_restart = AsyncMock()
        await pub._attempt_liveness_recovery("stale")
        pub._force_ws_restart.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_start_sets_current_publisher_context_var(self) -> None:
        """Spec — full Given/When/Then below.

        Given ``KrakenMarketDataPublisher.start``,
        When invoked,
        Then ``_CURRENT_PUBLISHER`` carries ``self`` during the super call
        and is reset to ``None`` afterwards.
        """
        pub = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        observed: dict[str, object] = {}

        async def fake_super_start(self_obj: object) -> None:
            observed["during"] = _CURRENT_PUBLISHER.get()

        with patch.object(MarketDataPublisherService, "start", fake_super_start):
            await pub.start()
        assert observed["during"] is pub
        assert _CURRENT_PUBLISHER.get() is None
