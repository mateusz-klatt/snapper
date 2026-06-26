"""Unit tests for KrakenFuturesMarketDataPublisher."""

import asyncio
from datetime import UTC
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.config.app import AppSettings
from snapper.data.models import SymbolMarketDataChannelCapability
from snapper.infrastructure.exchanges._subscription_health import _SymbolEntry
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)
from snapper.infrastructure.network.egress_context import _CURRENT_PUBLISHER
from snapper.messaging.publishers.kraken_futures import KrakenFuturesMarketDataPublisher


class _AsyncSessionContext:
    """Async context manager returning a mocked session."""

    def __init__(self, session: MagicMock) -> None:
        """Store the session returned on enter."""
        self.session = session

    async def __aenter__(self) -> MagicMock:
        """Return the configured session."""
        return self.session

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        """Leave the context without suppressing exceptions."""
        return None


def _health_entry(
    *,
    channel: str,
    symbol: str = "PF_XBTUSD",
    status: str = "confirmed",
    last_error: str | None = None,
    slow_retry_count: int = 0,
    last_seen_data_at: float | None = None,
    ever_seen_data: bool = False,
    ever_confirmed: bool = False,
) -> _SymbolEntry:
    """Build a subscription-health entry for publisher runtime-learning tests."""
    return _SymbolEntry(
        channel=channel,
        symbol=symbol,
        status=status,
        requested_at=100.0,
        last_error=last_error,
        slow_retry_count=slow_retry_count,
        last_seen_data_at=last_seen_data_at,
        ever_seen_data=ever_seen_data,
        ever_confirmed=ever_confirmed,
    )


class TestKrakenFuturesMarketDataPublisher:
    """Tests for KrakenFuturesMarketDataPublisher functionality."""

    def test_create_exchange_client_returns_futures_client(self) -> None:
        """Verify factory method creates KrakenFuturesExchangeClient.

        Given: A KrakenFuturesMarketDataPublisher instance,
        When: _create_exchange_client is called,
        Then: Returns a KrakenFuturesExchangeClient instance.
        """
        publisher = KrakenFuturesMarketDataPublisher(symbols=["BTC-USD-PERP"])
        with patch(
            "snapper.messaging.publishers.kraken_futures.KrakenFuturesExchangeClient"
        ) as mock_cls:
            mock_cls.return_value = MagicMock(spec=KrakenFuturesExchangeClient)
            client = publisher._create_exchange_client()
            assert isinstance(client, KrakenFuturesExchangeClient)

    def test_get_exchange_name_returns_kraken_futures(self) -> None:
        """Verify exchange name returns 'kraken_futures'.

        Given: A KrakenFuturesMarketDataPublisher instance,
        When: _get_exchange_name is called,
        Then: Returns 'kraken_futures'.
        """
        publisher = KrakenFuturesMarketDataPublisher(symbols=["BTC-USD-PERP"])
        assert publisher._get_exchange_name() == "kraken_futures"

    def test_candle_source_for_returns_calculated(self) -> None:
        """Verify futures 1m bars are tagged calculated (trade-built).

        Given: A KrakenFuturesMarketDataPublisher instance,
        When: _candle_source_for is called for 1m,
        Then: Returns 'calculated' (Snapper-computed, not venue OHLC).
        """
        publisher = KrakenFuturesMarketDataPublisher(symbols=["BTC-USD-PERP"])
        assert publisher._candle_source_for("1m") == "calculated"

    def test_liveness_threshold_is_60s(self) -> None:
        """Verify the Futures liveness-recovery threshold is lowered to 60s.

        Given: A KrakenFuturesMarketDataPublisher instance,
        When: _get_liveness_recovery_threshold_s is called,
        Then: Returns the 60 second realtime-venue threshold.
        """
        publisher = KrakenFuturesMarketDataPublisher(symbols=["BTC-USD-PERP"])
        assert publisher._get_liveness_recovery_threshold_s() == 60

    def test_validate_symbols_filters_invalid(self) -> None:
        """Verify invalid symbols are filtered out.

        Given: Publisher and symbols including one that raises ValueError,
        When: _validate_symbols is called,
        Then: Invalid symbols are excluded.
        """
        publisher = KrakenFuturesMarketDataPublisher(symbols=[])

        def _lookup(s: str) -> str:
            if s == "INVALID":
                raise ValueError("unknown")
            return s

        with patch(
            "snapper.messaging.publishers.kraken_futures.native_to_kraken_futures_ws",
            side_effect=_lookup,
        ):
            valid = publisher._validate_symbols(["BTC-USD-PERP", "INVALID", "ETH-USD-PERP"])
        assert valid == ["BTC-USD-PERP", "ETH-USD-PERP"]

    def test_validate_symbols_removes_duplicates(self) -> None:
        """Verify duplicate symbols are removed.

        Given: Symbols list with duplicates,
        When: _validate_symbols is called,
        Then: Duplicates removed.
        """
        publisher = KrakenFuturesMarketDataPublisher(symbols=[])
        with patch(
            "snapper.messaging.publishers.kraken_futures.native_to_kraken_futures_ws",
            return_value="PF_XBTUSD",
        ):
            valid = publisher._validate_symbols(["BTC-USD-PERP", "BTC-USD-PERP", "ETH-USD-PERP"])
        assert len(valid) == 2
        assert valid == ["BTC-USD-PERP", "ETH-USD-PERP"]

    def test_validate_symbols_wildcard_expands_to_catalog(self) -> None:
        """Verify ``["*"]`` expands to the full catalog of futures symbols.

        Given: Publisher and ``symbols=["*"]`` request,
        When: _validate_symbols is called,
        Then: The full list returned by
            ``get_available_kraken_futures_symbols`` is returned verbatim,
            without per-symbol validation (Kraken Futures has no
            server-side wildcard, so expansion happens here).
        """
        publisher = KrakenFuturesMarketDataPublisher(symbols=[])
        catalog = ["BTC-USD-PERP", "ETH-USD-PERP", "SOL-USD-PERP"]
        with patch(
            "snapper.messaging.publishers.kraken_futures.get_available_kraken_futures_symbols",
            return_value=catalog,
        ):
            valid = publisher._validate_symbols(["*"])
        assert valid == catalog

    def test_symbols_for_trade_loop_uses_trade_channel_filter(self) -> None:
        """Trade-loop symbols are filtered through the trade channel capability gate.

        Given: A publisher with two symbol-level futures symbols,
        When: The trade-loop hook runs,
        Then: Only symbols returned by channel-aware availability remain.
        """
        publisher = KrakenFuturesMarketDataPublisher(symbols=["BTC-USD-PERP", "ETH-USD-PERP"])
        with patch(
            "snapper.messaging.publishers.kraken_futures.get_available_kraken_futures_symbols",
            return_value=["BTC-USD-PERP"],
        ) as available_mock:
            symbols = publisher._symbols_for_trade_loop(["BTC-USD-PERP", "ETH-USD-PERP"])
        available_mock.assert_called_once_with(channel="trade")
        assert symbols == ["BTC-USD-PERP"]

    def test_record_trade_confirmations_sticks_ack_data_and_lifetime_flags(self) -> None:
        """Trade confirmations are recorded from ACKs and data evidence.

        Given: A feed-health snapshot with confirmed, data-seen, and lifetime flags,
        When: The publisher records trade confirmations,
        Then: Only trade products with ACK or data evidence become sticky.
        """
        publisher = KrakenFuturesMarketDataPublisher(symbols=["BTC-USD-PERP"])
        snapshot = {
            ("ticker", "PF_XBTUSD"): _health_entry(
                channel="ticker",
                symbol="PF_XBTUSD",
                status="confirmed",
                last_seen_data_at=101.0,
            ),
            ("trade", "PF_XBTUSD"): _health_entry(
                channel="trade",
                symbol="PF_XBTUSD",
                status="confirmed",
            ),
            ("trade", "PF_ETHUSD"): _health_entry(
                channel="trade",
                symbol="PF_ETHUSD",
                status="pending",
                last_seen_data_at=102.0,
            ),
            ("trade", "PF_SOLUSD"): _health_entry(
                channel="trade",
                symbol="PF_SOLUSD",
                status="pending",
                ever_seen_data=True,
            ),
            ("trade", "PF_ADAUSD"): _health_entry(
                channel="trade",
                symbol="PF_ADAUSD",
                status="pending",
                ever_confirmed=True,
            ),
            ("trade", "PF_LDOUSD"): _health_entry(
                channel="trade",
                symbol="PF_LDOUSD",
                status="pending",
            ),
        }

        publisher._record_trade_confirmations(snapshot)

        assert publisher._trade_confirmed_products == {
            "PF_XBTUSD",
            "PF_ETHUSD",
            "PF_SOLUSD",
            "PF_ADAUSD",
        }

    def test_should_learn_trade_channel_false_requires_stable_failure(self) -> None:
        """Runtime learning requires stable failure and no prior trade confirmation.

        Given: A trade subscription that exhausted after slow retry and ticker has data,
        When: The trade product has never confirmed in this publisher process,
        Then: Runtime learning accepts the failure as safe to persist.
        """
        trade = _health_entry(
            channel="trade",
            status="failed",
            last_error="retry budget exhausted",
            slow_retry_count=1,
        )
        ticker = _health_entry(channel="ticker", status="confirmed", last_seen_data_at=101.0)
        snapshot = {("trade", "PF_XBTUSD"): trade, ("ticker", "PF_XBTUSD"): ticker}
        assert (
            KrakenFuturesMarketDataPublisher._should_learn_trade_channel_false(
                trade,
                snapshot,
                False,
            )
            is True
        )

    @pytest.mark.parametrize(
        ("entry", "ticker", "trade_confirmed_since_start", "expected"),
        [
            (
                _health_entry(
                    channel="ticker",
                    status="failed",
                    last_error="retry budget exhausted",
                    slow_retry_count=1,
                ),
                _health_entry(channel="ticker", status="confirmed", last_seen_data_at=101.0),
                False,
                False,
            ),
            (
                _health_entry(
                    channel="trade",
                    status="pending",
                    last_error="retry budget exhausted",
                    slow_retry_count=1,
                ),
                _health_entry(channel="ticker", status="confirmed", last_seen_data_at=101.0),
                False,
                False,
            ),
            (
                _health_entry(channel="trade", status="failed", last_error="rejected"),
                _health_entry(channel="ticker", status="confirmed", last_seen_data_at=101.0),
                False,
                False,
            ),
            (
                _health_entry(
                    channel="trade",
                    status="failed",
                    last_error="retry budget exhausted",
                    slow_retry_count=0,
                ),
                _health_entry(channel="ticker", status="confirmed", last_seen_data_at=101.0),
                False,
                False,
            ),
            (
                _health_entry(
                    channel="trade",
                    status="failed",
                    last_error="retry budget exhausted",
                    slow_retry_count=1,
                ),
                None,
                False,
                False,
            ),
            (
                _health_entry(
                    channel="trade",
                    status="failed",
                    last_error="retry budget exhausted",
                    slow_retry_count=1,
                ),
                _health_entry(channel="ticker", status="pending", last_seen_data_at=101.0),
                False,
                False,
            ),
            (
                _health_entry(
                    channel="trade",
                    status="failed",
                    last_error="retry budget exhausted",
                    slow_retry_count=1,
                ),
                _health_entry(channel="ticker", status="confirmed", last_seen_data_at=None),
                False,
                False,
            ),
            (
                _health_entry(
                    channel="trade",
                    status="failed",
                    last_error="retry budget exhausted",
                    slow_retry_count=1,
                    ever_confirmed=True,
                ),
                _health_entry(channel="ticker", status="confirmed", last_seen_data_at=101.0),
                False,
                False,
            ),
            (
                _health_entry(
                    channel="trade",
                    status="failed",
                    last_error="retry budget exhausted",
                    slow_retry_count=1,
                ),
                _health_entry(channel="ticker", status="confirmed", last_seen_data_at=101.0),
                True,
                False,
            ),
        ],
    )
    def test_should_learn_trade_channel_false_rejects_unsafe_cases(
        self,
        entry: _SymbolEntry,
        ticker: _SymbolEntry | None,
        trade_confirmed_since_start: bool,
        expected: bool,
    ) -> None:
        """Runtime learning rejects incomplete, transient, or unsafe evidence.

        Given: A trade failure candidate with missing evidence or prior trade confirmation,
        When: The runtime-learning predicate evaluates it,
        Then: The failure is rejected.
        """
        snapshot = {("trade", entry.symbol): entry}
        if ticker is not None:
            snapshot[("ticker", entry.symbol)] = ticker
        assert (
            KrakenFuturesMarketDataPublisher._should_learn_trade_channel_false(
                entry,
                snapshot,
                trade_confirmed_since_start,
            )
            is expected
        )

    def test_get_max_symbols_per_connection_returns_zero(self) -> None:
        """Verify Kraken Futures WS has no documented symbol limit.

        Given: A KrakenFuturesMarketDataPublisher instance,
        When: _get_max_symbols_per_connection is called,
        Then: Returns 0 (unlimited).
        """
        publisher = KrakenFuturesMarketDataPublisher(symbols=["BTC-USD-PERP"])
        assert publisher._get_max_symbols_per_connection() == 0

    def test_get_default_parameters_from_settings(self) -> None:
        """Verify default parameters extracts kraken_futures symbols.

        Given: Settings with instruments.kraken_futures containing symbols,
        When: get_default_parameters is called,
        Then: Returns dict with those symbols.
        """
        mock_settings = MagicMock(spec=AppSettings)
        mock_settings.instruments = {"kraken_futures": ["BTC-USD-PERP"], "kraken": ["BTC-USD"]}
        params = KrakenFuturesMarketDataPublisher.get_default_parameters(mock_settings)
        assert params == {"symbols": ["BTC-USD-PERP"]}

    def test_get_default_parameters_with_empty_list(self) -> None:
        """Verify default parameters handles empty instrument list.

        Given: Settings with empty kraken_futures instruments,
        When: get_default_parameters is called,
        Then: Returns dict with empty symbols list.
        """
        mock_settings = MagicMock(spec=AppSettings)
        mock_settings.instruments = {"kraken_futures": [], "kraken": ["BTC-USD"]}
        params = KrakenFuturesMarketDataPublisher.get_default_parameters(mock_settings)
        assert params == {"symbols": []}

    def test_get_default_parameters_with_missing_key(self) -> None:
        """Verify default parameters handles missing kraken_futures key.

        Given: Settings without kraken_futures key in instruments,
        When: get_default_parameters is called,
        Then: Returns dict with empty symbols list.
        """
        mock_settings = MagicMock(spec=AppSettings)
        mock_settings.instruments = {"kraken": ["BTC-USD"]}
        params = KrakenFuturesMarketDataPublisher.get_default_parameters(mock_settings)
        assert params == {"symbols": []}

    def test_candle_loop_uses_base_class(self) -> None:
        """Candle loop is inherited from base class (no override).

        Given: A KrakenFuturesMarketDataPublisher instance,
        When: Checking _candle_loop,
        Then: It is not overridden (uses base class polling via subscribe_candles).
        """
        assert "_candle_loop" not in KrakenFuturesMarketDataPublisher.__dict__

    @pytest.mark.asyncio
    async def test_attempt_liveness_recovery_disconnects_and_ensures(self) -> None:
        """Futures liveness recovery rebuilds the websocket client.

        Given: A Futures publisher with an attached exchange client,
        When: Liveness recovery is attempted,
        Then: The client disconnects and reconnects its websocket.
        """
        publisher = KrakenFuturesMarketDataPublisher(symbols=["BTC-USD-PERP"])
        client = MagicMock()
        client.disconnect = AsyncMock()
        client._ensure_ws_connected = AsyncMock()
        publisher._exchange_client = client
        await publisher._attempt_liveness_recovery("stale")
        client.disconnect.assert_awaited_once()
        client._ensure_ws_connected.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_attempt_liveness_recovery_preserves_trade_confirmation_sticky_flag(
        self,
    ) -> None:
        """Futures liveness recovery does not reset trade confirmation memory.

        Given: A publisher process whose trade feed confirmed for one product,
        When: Liveness recovery rebuilds the websocket client,
        Then: The process-local trade confirmation flag remains set.
        """
        publisher = KrakenFuturesMarketDataPublisher(symbols=["BTC-USD-PERP"])
        publisher._trade_confirmed_products.add("PF_XBTUSD")
        client = MagicMock()
        client.disconnect = AsyncMock()
        client._ensure_ws_connected = AsyncMock()
        publisher._exchange_client = client

        await publisher._attempt_liveness_recovery("stale")

        assert publisher._trade_confirmed_products == {"PF_XBTUSD"}

    @pytest.mark.asyncio
    async def test_attempt_liveness_recovery_skips_without_client(self) -> None:
        """Futures liveness recovery tolerates missing client.

        Given: A Futures publisher without an exchange client,
        When: Liveness recovery is attempted,
        Then: It completes without raising.
        """
        publisher = KrakenFuturesMarketDataPublisher(symbols=["BTC-USD-PERP"])
        publisher._exchange_client = None
        await publisher._attempt_liveness_recovery("stale")

    def test_invalidate_symbol_cache_skips_reprobe_when_not_running(self) -> None:
        """Cache invalidation does not schedule re-probe while stopped."""
        publisher = KrakenFuturesMarketDataPublisher(symbols=["BTC-USD-PERP"])
        mapper = MagicMock()
        with (
            patch(
                "snapper.messaging.publishers.base.SymbolMapperService.get_instance",
                return_value=mapper,
            ),
            patch("snapper.messaging.publishers.kraken_futures.asyncio.create_task") as task_mock,
        ):
            publisher.running = False
            publisher._invalidate_symbol_cache()
        mapper.trigger_cache_invalidation.assert_called_once_with(fail_fast=False)
        task_mock.assert_not_called()

    @pytest.mark.asyncio
    async def test_invalidate_symbol_cache_schedules_reprobe_when_running(self) -> None:
        """Cache invalidation schedules a trade re-probe while running."""
        publisher = KrakenFuturesMarketDataPublisher(symbols=["BTC-USD-PERP"])
        mapper = MagicMock()
        with (
            patch(
                "snapper.messaging.publishers.base.SymbolMapperService.get_instance",
                return_value=mapper,
            ),
            patch.object(
                publisher,
                "_reprobe_trade_symbols",
                new=AsyncMock(return_value=None),
            ) as reprobe_mock,
        ):
            publisher.running = True
            publisher._invalidate_symbol_cache()
            await asyncio.sleep(0)
        mapper.trigger_cache_invalidation.assert_called_once_with(fail_fast=False)
        reprobe_mock.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_reprobe_trade_symbols_skips_without_client(self) -> None:
        """Reprobe exits when no exchange client is attached."""
        publisher = KrakenFuturesMarketDataPublisher(symbols=["BTC-USD-PERP"])
        publisher._exchange_client = None
        await publisher._reprobe_trade_symbols()

    @pytest.mark.asyncio
    async def test_reprobe_trade_symbols_maps_and_sends_allowed_products(self) -> None:
        """Reprobe maps allowed native symbols and asks the client to re-subscribe."""
        publisher = KrakenFuturesMarketDataPublisher(
            symbols=["BTC-USD-PERP", "ETH-USD-PERP", "BAD-SYMBOL"]
        )
        client = MagicMock()
        client.reprobe_public_subscription = AsyncMock(side_effect=[True, False])
        publisher._exchange_client = client

        def map_symbol(symbol: str) -> str:
            if symbol == "BAD-SYMBOL":
                raise ValueError("unknown")
            return {"BTC-USD-PERP": "PF_XBTUSD", "ETH-USD-PERP": "PF_ETHUSD"}[symbol]

        with (
            patch.object(
                publisher,
                "_symbols_for_trade_loop",
                return_value=["BTC-USD-PERP", "BAD-SYMBOL", "ETH-USD-PERP"],
            ),
            patch(
                "snapper.messaging.publishers.kraken_futures.native_to_kraken_futures_ws",
                side_effect=map_symbol,
            ),
        ):
            await publisher._reprobe_trade_symbols()
        assert client.reprobe_public_subscription.await_args_list[0].args == (
            "trade",
            "PF_XBTUSD",
        )
        assert client.reprobe_public_subscription.await_args_list[1].args == (
            "trade",
            "PF_ETHUSD",
        )

    @pytest.mark.asyncio
    async def test_reprobe_trade_symbols_handles_no_sent_subscriptions(self) -> None:
        """Reprobe completes quietly when every allowed product is already cached."""
        publisher = KrakenFuturesMarketDataPublisher(symbols=["BTC-USD-PERP"])
        client = MagicMock()
        client.reprobe_public_subscription = AsyncMock(return_value=False)
        publisher._exchange_client = client
        with (
            patch.object(publisher, "_symbols_for_trade_loop", return_value=["BTC-USD-PERP"]),
            patch(
                "snapper.messaging.publishers.kraken_futures.native_to_kraken_futures_ws",
                return_value="PF_XBTUSD",
            ),
            patch("snapper.messaging.publishers.kraken_futures.logger.info") as info_mock,
        ):
            await publisher._reprobe_trade_symbols()
        client.reprobe_public_subscription.assert_awaited_once_with("trade", "PF_XBTUSD")
        info_mock.assert_not_called()

    def test_log_reprobe_task_result_handles_success_cancel_and_error(self) -> None:
        """Done-callback logging handles every task result path."""
        success_task = MagicMock()
        success_task.result.return_value = None
        KrakenFuturesMarketDataPublisher._log_reprobe_task_result(success_task)
        success_task.result.assert_called_once()

        cancelled_task = MagicMock()
        cancelled_task.result.side_effect = asyncio.CancelledError()
        KrakenFuturesMarketDataPublisher._log_reprobe_task_result(cancelled_task)

        failing_task = MagicMock()
        failing_task.result.side_effect = RuntimeError("boom")
        with patch("snapper.messaging.publishers.kraken_futures.logger.warning") as warning:
            KrakenFuturesMarketDataPublisher._log_reprobe_task_result(failing_task)
        warning.assert_called_once()

    @pytest.mark.asyncio
    async def test_after_feed_health_snapshot_returns_without_client_or_repository(self) -> None:
        """Runtime learning is a no-op until client and repository are both present."""
        publisher = KrakenFuturesMarketDataPublisher(symbols=["BTC-USD-PERP"])
        publisher._exchange_client = None
        publisher.repository = MagicMock()
        snapshot = {
            ("trade", "PF_XBTUSD"): _health_entry(
                channel="trade",
                status="failed",
                last_error="retry budget exhausted",
                slow_retry_count=1,
            )
        }
        await publisher._after_feed_health_snapshot(snapshot)
        publisher._exchange_client = MagicMock()
        publisher.repository = None
        await publisher._after_feed_health_snapshot(snapshot)

    @pytest.mark.asyncio
    async def test_after_feed_health_snapshot_persists_suppresses_and_broadcasts(
        self,
    ) -> None:
        """Stable trade failure persists channel false, suppresses churn, and broadcasts."""
        publisher = KrakenFuturesMarketDataPublisher(symbols=["BTC-USD-PERP"])
        client = MagicMock()
        client.suppress_public_subscription = MagicMock()
        publisher._exchange_client = client
        publisher.repository = MagicMock()
        publisher.msg_publisher = MagicMock()
        publisher.msg_publisher.tracker.session_id = "pub-session"
        publisher.msg_publisher.tracker.next_sequence.return_value = 4
        publisher.msg_publisher.send = AsyncMock()
        trade = _health_entry(
            channel="trade",
            status="failed",
            last_error="retry budget exhausted",
            slow_retry_count=1,
        )
        ticker = _health_entry(channel="ticker", status="confirmed", last_seen_data_at=101.0)
        snapshot = {("trade", "PF_XBTUSD"): trade, ("ticker", "PF_XBTUSD"): ticker}
        with (
            patch(
                "snapper.messaging.publishers.kraken_futures.kraken_futures_ws_to_native",
                return_value="BTC-USD-PERP",
            ),
            patch.object(
                publisher,
                "_persist_runtime_trade_channel_false",
                new=AsyncMock(return_value=True),
            ) as persist_mock,
            patch.object(publisher, "_invalidate_symbol_cache") as invalidate_mock,
        ):
            await publisher._after_feed_health_snapshot(snapshot)
        persist_mock.assert_awaited_once_with(publisher.repository, "BTC-USD-PERP")
        client.suppress_public_subscription.assert_called_once_with(
            "trade",
            "PF_XBTUSD",
            "runtime learned trade channel unavailable",
        )
        invalidate_mock.assert_called_once()
        publisher.msg_publisher.send.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_after_feed_health_snapshot_skips_confirmed_then_exhausted_trade(
        self,
    ) -> None:
        """Previously confirmed trade products are left to self-heal.

        Given: A trade product that confirmed once in this publisher process,
        When: The same product later exhausts retry budget while ticker has data,
        Then: Runtime learning does not persist or suppress a channel denial.
        """
        publisher = KrakenFuturesMarketDataPublisher(symbols=["BTC-USD-PERP"])
        client = MagicMock()
        client.suppress_public_subscription = MagicMock()
        publisher._exchange_client = client
        publisher.repository = MagicMock()
        confirmed_snapshot = {
            ("trade", "PF_XBTUSD"): _health_entry(
                channel="trade",
                status="confirmed",
            )
        }
        failed_snapshot = {
            ("trade", "PF_XBTUSD"): _health_entry(
                channel="trade",
                status="failed",
                last_error="retry budget exhausted",
                slow_retry_count=1,
            ),
            ("ticker", "PF_XBTUSD"): _health_entry(
                channel="ticker",
                status="confirmed",
                last_seen_data_at=101.0,
            ),
        }
        with patch.object(
            publisher,
            "_persist_runtime_trade_channel_false",
            new=AsyncMock(return_value=True),
        ) as persist_mock:
            await publisher._after_feed_health_snapshot(confirmed_snapshot)
            await publisher._after_feed_health_snapshot(failed_snapshot)

        assert publisher._trade_confirmed_products == {"PF_XBTUSD"}
        persist_mock.assert_not_awaited()
        client.suppress_public_subscription.assert_not_called()

    @pytest.mark.asyncio
    async def test_after_feed_health_snapshot_skips_tracker_ever_confirmed_trade(
        self,
    ) -> None:
        """Tracker lifetime confirmation blocks runtime learning.

        Given: A trade product ACK-confirmed before the publisher observed a
            health snapshot and later exhausted after reconnect,
        When: The failed snapshot carries ever_confirmed from the tracker,
        Then: Runtime learning leaves the product to self-heal.
        """
        publisher = KrakenFuturesMarketDataPublisher(symbols=["BTC-USD-PERP"])
        client = MagicMock()
        client.suppress_public_subscription = MagicMock()
        publisher._exchange_client = client
        publisher.repository = MagicMock()
        failed_snapshot = {
            ("trade", "PF_XBTUSD"): _health_entry(
                channel="trade",
                status="failed",
                last_error="retry budget exhausted",
                slow_retry_count=1,
                ever_confirmed=True,
            ),
            ("ticker", "PF_XBTUSD"): _health_entry(
                channel="ticker",
                status="confirmed",
                last_seen_data_at=101.0,
            ),
        }
        with patch.object(
            publisher,
            "_persist_runtime_trade_channel_false",
            new=AsyncMock(return_value=True),
        ) as persist_mock:
            await publisher._after_feed_health_snapshot(failed_snapshot)

        assert publisher._trade_confirmed_products == {"PF_XBTUSD"}
        persist_mock.assert_not_awaited()
        client.suppress_public_subscription.assert_not_called()

    @pytest.mark.asyncio
    async def test_after_feed_health_snapshot_learns_never_confirmed_trade(
        self,
    ) -> None:
        """Never-confirmed trade products are still learned as unavailable.

        Given: A trade product that never confirmed while ticker has data,
        When: The trade subscription exhausts retry budget after slow retry,
        Then: Runtime learning persists and suppresses the channel denial.
        """
        publisher = KrakenFuturesMarketDataPublisher(symbols=["LDO-USD-PERP"])
        client = MagicMock()
        client.suppress_public_subscription = MagicMock()
        publisher._exchange_client = client
        publisher.repository = MagicMock()
        snapshot = {
            ("trade", "PF_LDOUSD"): _health_entry(
                channel="trade",
                symbol="PF_LDOUSD",
                status="failed",
                last_error="retry budget exhausted",
                slow_retry_count=1,
            ),
            ("ticker", "PF_LDOUSD"): _health_entry(
                channel="ticker",
                symbol="PF_LDOUSD",
                status="confirmed",
                last_seen_data_at=101.0,
            ),
        }
        with (
            patch(
                "snapper.messaging.publishers.kraken_futures.kraken_futures_ws_to_native",
                return_value="LDO-USD-PERP",
            ),
            patch.object(
                publisher,
                "_persist_runtime_trade_channel_false",
                new=AsyncMock(return_value=True),
            ) as persist_mock,
            patch.object(publisher, "_broadcast_runtime_capability_invalidation") as broadcast,
        ):
            await publisher._after_feed_health_snapshot(snapshot)

        assert publisher._trade_confirmed_products == set()
        persist_mock.assert_awaited_once_with(publisher.repository, "LDO-USD-PERP")
        client.suppress_public_subscription.assert_called_once_with(
            "trade",
            "PF_LDOUSD",
            "runtime learned trade channel unavailable",
        )
        broadcast.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_after_feed_health_snapshot_keeps_trade_sticky_per_symbol(self) -> None:
        """Trade confirmation memory is scoped per product.

        Given: One product confirmed trade and another product never confirmed,
        When: The never-confirmed product exhausts while ticker has data,
        Then: Runtime learning suppresses only the never-confirmed product.
        """
        publisher = KrakenFuturesMarketDataPublisher(symbols=["BTC-USD-PERP", "LDO-USD-PERP"])
        client = MagicMock()
        client.suppress_public_subscription = MagicMock()
        publisher._exchange_client = client
        publisher.repository = MagicMock()
        snapshot = {
            ("trade", "PF_XBTUSD"): _health_entry(
                channel="trade",
                symbol="PF_XBTUSD",
                status="confirmed",
            ),
            ("trade", "PF_LDOUSD"): _health_entry(
                channel="trade",
                symbol="PF_LDOUSD",
                status="failed",
                last_error="retry budget exhausted",
                slow_retry_count=1,
            ),
            ("ticker", "PF_LDOUSD"): _health_entry(
                channel="ticker",
                symbol="PF_LDOUSD",
                status="confirmed",
                last_seen_data_at=101.0,
            ),
        }
        with (
            patch(
                "snapper.messaging.publishers.kraken_futures.kraken_futures_ws_to_native",
                return_value="LDO-USD-PERP",
            ),
            patch.object(
                publisher,
                "_persist_runtime_trade_channel_false",
                new=AsyncMock(return_value=True),
            ) as persist_mock,
            patch.object(publisher, "_broadcast_runtime_capability_invalidation"),
        ):
            await publisher._after_feed_health_snapshot(snapshot)

        assert publisher._trade_confirmed_products == {"PF_XBTUSD"}
        persist_mock.assert_awaited_once_with(publisher.repository, "LDO-USD-PERP")
        client.suppress_public_subscription.assert_called_once_with(
            "trade",
            "PF_LDOUSD",
            "runtime learned trade channel unavailable",
        )

    @pytest.mark.asyncio
    async def test_after_feed_health_snapshot_skips_when_persist_returns_false(
        self,
    ) -> None:
        """Unresolved symbols do not suppress subscriptions or broadcast."""
        publisher = KrakenFuturesMarketDataPublisher(symbols=["BTC-USD-PERP"])
        client = MagicMock()
        client.suppress_public_subscription = MagicMock()
        publisher._exchange_client = client
        publisher.repository = MagicMock()
        trade = _health_entry(
            channel="trade",
            status="failed",
            last_error="retry budget exhausted",
            slow_retry_count=1,
        )
        ticker = _health_entry(channel="ticker", status="confirmed", last_seen_data_at=101.0)
        snapshot = {("trade", "PF_XBTUSD"): trade, ("ticker", "PF_XBTUSD"): ticker}
        with (
            patch(
                "snapper.messaging.publishers.kraken_futures.kraken_futures_ws_to_native",
                return_value="BTC-USD-PERP",
            ),
            patch.object(
                publisher,
                "_persist_runtime_trade_channel_false",
                new=AsyncMock(return_value=False),
            ),
            patch.object(publisher, "_broadcast_runtime_capability_invalidation") as broadcast,
        ):
            await publisher._after_feed_health_snapshot(snapshot)
        client.suppress_public_subscription.assert_not_called()
        broadcast.assert_not_called()

    @pytest.mark.asyncio
    async def test_after_feed_health_snapshot_logs_and_continues_on_persist_error(
        self,
    ) -> None:
        """Persistence errors are logged and do not escape the feed-health hook."""
        publisher = KrakenFuturesMarketDataPublisher(symbols=["BTC-USD-PERP"])
        client = MagicMock()
        client.suppress_public_subscription = MagicMock()
        publisher._exchange_client = client
        publisher.repository = MagicMock()
        trade = _health_entry(
            channel="trade",
            status="failed",
            last_error="retry budget exhausted",
            slow_retry_count=1,
        )
        ticker = _health_entry(channel="ticker", status="confirmed", last_seen_data_at=101.0)
        snapshot = {("trade", "PF_XBTUSD"): trade, ("ticker", "PF_XBTUSD"): ticker}
        with (
            patch(
                "snapper.messaging.publishers.kraken_futures.kraken_futures_ws_to_native",
                return_value="BTC-USD-PERP",
            ),
            patch.object(
                publisher,
                "_persist_runtime_trade_channel_false",
                new=AsyncMock(side_effect=RuntimeError("db down")),
            ),
            patch("snapper.messaging.publishers.kraken_futures.logger.warning") as warning,
        ):
            await publisher._after_feed_health_snapshot(snapshot)
        client.suppress_public_subscription.assert_not_called()
        warning.assert_called_once()

    @pytest.mark.asyncio
    async def test_broadcast_runtime_capability_invalidation_without_publisher(self) -> None:
        """Local cache invalidation still runs without a message publisher."""
        publisher = KrakenFuturesMarketDataPublisher(symbols=["BTC-USD-PERP"])
        publisher.msg_publisher = None
        with patch.object(publisher, "_invalidate_symbol_cache") as invalidate_mock:
            await publisher._broadcast_runtime_capability_invalidation()
        invalidate_mock.assert_called_once()

    @pytest.mark.asyncio
    async def test_persist_runtime_trade_channel_false_writes_and_reuses_scd2_row(self) -> None:
        """Runtime learning writes through the SCD2 helper and is idempotent."""
        now = datetime.now(UTC)
        symbol_public_id = "11111111-1111-7111-8111-111111111111"
        native_symbol = "TST-USD-PERP"
        repository = MagicMock()
        session = MagicMock()
        session.commit = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = None
        session.execute = AsyncMock(return_value=result)
        repository.session.return_value = _AsyncSessionContext(session)
        existing = SimpleNamespace(
            can_market_data=False,
            source="kraken_futures_publisher_runtime",
            reason="Learned: trade not confirmed while ticker confirmed",
            created_at=now,
        )
        repeated_result = MagicMock()
        repeated_result.scalar_one_or_none.return_value = existing
        unresolved_repository = MagicMock()
        publisher = KrakenFuturesMarketDataPublisher(symbols=[native_symbol])
        with (
            patch(
                "snapper.messaging.publishers.kraken_futures.resolve_symbol_public_id",
                new=AsyncMock(side_effect=[symbol_public_id, symbol_public_id, None]),
            ),
            patch(
                "snapper.messaging.publishers.kraken_futures.close_and_insert",
                new=AsyncMock(return_value=SimpleNamespace()),
            ) as close_mock,
        ):
            inserted = await publisher._persist_runtime_trade_channel_false(
                repository,
                native_symbol,
            )
            session.execute = AsyncMock(return_value=repeated_result)
            repeated = await publisher._persist_runtime_trade_channel_false(
                repository,
                native_symbol,
            )
            unresolved = await publisher._persist_runtime_trade_channel_false(
                unresolved_repository,
                "MISSING-USD-PERP",
            )
        assert inserted is True
        assert repeated is True
        assert unresolved is False
        close_mock.assert_awaited_once()
        call = close_mock.await_args
        assert call is not None
        assert call.kwargs["model"] is SymbolMarketDataChannelCapability
        assert call.kwargs["new_values"]["symbol_public_id"] == symbol_public_id
        assert call.kwargs["new_values"]["exchange"] == "kraken_futures"
        assert call.kwargs["new_values"]["channel"] == "trade"
        assert call.kwargs["new_values"]["can_market_data"] is False
        assert session.commit.await_count == 1
        unresolved_repository.session.assert_not_called()

    @pytest.mark.asyncio
    async def test_persist_runtime_trade_channel_false_closes_existing_channel_row(self) -> None:
        """Runtime learning preserves created_at when closing an existing row."""
        repository = MagicMock()
        session = MagicMock()
        session.commit = AsyncMock()
        now = datetime.now(UTC)
        symbol_public_id = "44444444-4444-7444-8444-444444444444"
        existing = SimpleNamespace(
            can_market_data=True,
            source="manual",
            reason=None,
            created_at=now,
        )
        result = MagicMock()
        result.scalar_one_or_none.return_value = existing
        session.execute = AsyncMock(return_value=result)
        repository.session.return_value = _AsyncSessionContext(session)
        native_symbol = "TST2-USD-PERP"
        publisher = KrakenFuturesMarketDataPublisher(symbols=[native_symbol])
        with (
            patch(
                "snapper.messaging.publishers.kraken_futures.resolve_symbol_public_id",
                new=AsyncMock(return_value=symbol_public_id),
            ),
            patch(
                "snapper.messaging.publishers.kraken_futures.close_and_insert",
                new=AsyncMock(return_value=SimpleNamespace()),
            ) as close_mock,
        ):
            persisted = await publisher._persist_runtime_trade_channel_false(
                repository,
                native_symbol,
            )
        assert persisted is True
        close_mock.assert_awaited_once()
        call = close_mock.await_args
        assert call is not None
        assert call.kwargs["new_values"]["created_at"] == now
        session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_persist_runtime_trade_channel_false_preserves_non_runtime_false_row(
        self,
    ) -> None:
        """Runtime learning leaves operator-owned false channel rows unchanged."""
        repository = MagicMock()
        session = MagicMock()
        session.commit = AsyncMock()
        now = datetime.now(UTC)
        symbol_public_id = "77777777-7777-7777-8777-777777777777"
        existing = SimpleNamespace(
            can_market_data=False,
            source="operator",
            reason="disabled by operator",
            created_at=now,
        )
        result = MagicMock()
        result.scalar_one_or_none.return_value = existing
        session.execute = AsyncMock(return_value=result)
        repository.session.return_value = _AsyncSessionContext(session)
        native_symbol = "TST3-USD-PERP"
        publisher = KrakenFuturesMarketDataPublisher(symbols=[native_symbol])
        with (
            patch(
                "snapper.messaging.publishers.kraken_futures.resolve_symbol_public_id",
                new=AsyncMock(return_value=symbol_public_id),
            ),
            patch(
                "snapper.messaging.publishers.kraken_futures.close_and_insert",
                new=AsyncMock(return_value=SimpleNamespace()),
            ) as close_mock,
        ):
            persisted = await publisher._persist_runtime_trade_channel_false(
                repository,
                native_symbol,
            )
        assert persisted is True
        close_mock.assert_not_awaited()
        session.commit.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_start_sets_and_resets_current_publisher_context_var(self) -> None:
        """Start override registers this publisher in the SDK-patch ContextVar.

        Given: A KrakenFuturesMarketDataPublisher instance with a
            patched base ``start`` that observes the ContextVar.
        When: ``start()`` is awaited,
        Then: ``_CURRENT_PUBLISHER`` resolves to the publisher
            during ``super().start()`` execution, then resets to
            ``None`` after ``start()`` returns.

        Required so the patched ``kraken.futures.websocket.connect``
        shim reads ``_get_exchange_name()`` ("kraken_futures") and
        any egress_pool route pinned to ``["kraken_futures"]`` (or
        ``["kraken_equities", "kraken_futures"]``) becomes
        selectable for this publisher's WS reservation.
        """
        publisher = KrakenFuturesMarketDataPublisher(symbols=["BTC-USD-PERP"])
        observed: list[KrakenFuturesMarketDataPublisher | None] = []

        async def fake_super_start(self: KrakenFuturesMarketDataPublisher) -> None:
            observed.append(_CURRENT_PUBLISHER.get())

        with patch(
            "snapper.messaging.publishers.base.MarketDataPublisherService.start",
            new=fake_super_start,
        ):
            await publisher.start()

        assert observed == [publisher]
        assert _CURRENT_PUBLISHER.get() is None

    @pytest.mark.asyncio
    async def test_start_resets_context_var_even_if_super_raises(self) -> None:
        """Token reset must occur in ``finally`` so failures do not leak the ContextVar.

        Given: ``super().start()`` raises an arbitrary RuntimeError,
        When: ``start()`` is awaited,
        Then: The exception propagates AND ``_CURRENT_PUBLISHER``
            is restored to its prior value (``None``).
        """
        publisher = KrakenFuturesMarketDataPublisher(symbols=["BTC-USD-PERP"])
        fake_super = AsyncMock(side_effect=RuntimeError("boom"))

        with (
            patch(
                "snapper.messaging.publishers.base.MarketDataPublisherService.start",
                new=fake_super,
            ),
            pytest.raises(RuntimeError, match="boom"),
        ):
            await publisher.start()

        assert _CURRENT_PUBLISHER.get() is None
