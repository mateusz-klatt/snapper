"""Unit tests for WalutomatMarketDataPublisher."""

import asyncio
import time
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from loguru import logger

from snapper.infrastructure.exchanges.implementations.walutomat import (
    WALUTOMAT_REFUSAL_WARNING_SECONDS,
)
from snapper.infrastructure.exchanges.implementations.walutomat import WalutomatExchangeClient
from snapper.infrastructure.exchanges.schemas.walutomat import WalutomatBestOffer
from snapper.infrastructure.exchanges.schemas.walutomat import WalutomatMarketPair
from snapper.infrastructure.network.egress_context import _CURRENT_PUBLISHER
from snapper.messaging.publishers.walutomat import WalutomatMarketDataPublisher


class TestWalutomatPublisherUnknownSymbols:
    """Tests for Walutomat publisher unknown symbol handling."""

    def test_validate_symbols_logs_unknown_native_symbol(self) -> None:
        """Verify unknown symbols are logged and filtered out.

        Given a publisher and an invalid symbol,
        When _validate_symbols is called,
        Then symbol is excluded and warning is logged.
        """
        publisher = WalutomatMarketDataPublisher(symbols=[])
        with patch("snapper.messaging.publishers.walutomat.logger") as mock_logger:
            valid_symbols = publisher._validate_symbols(["INVALID-XXX"])
            assert valid_symbols == []
            mock_logger.warning.assert_called_once()
            assert "Skipping unknown native symbol INVALID-XXX" in str(
                mock_logger.warning.call_args
            )

    def test_validate_symbols_filters_duplicates(self) -> None:
        """Verify duplicate symbols are removed.

        Given a publisher and symbols list with duplicates,
        When _validate_symbols is called,
        Then duplicates are removed from result.
        """
        publisher = WalutomatMarketDataPublisher(symbols=[])
        with patch(
            "snapper.messaging.publishers.walutomat.native_to_walutomat_ws",
            return_value="BTCPLN",
        ):
            valid_symbols = publisher._validate_symbols(["BTC-PLN", "BTC-PLN", "BTC-PLN"])
        assert valid_symbols == ["BTC-PLN"]

    def test_validate_symbols_passes_through_wildcard(self) -> None:
        """Verify ``["*"]`` survives validation as a subscribe-all sentinel.

        Given: A WalutomatMarketDataPublisher constructed with ``["*"]``,
        When: ``_validate_symbols`` is invoked on the wildcard list,
        Then: It returns ``["*"]`` verbatim — bypassing per-symbol
            ``native_to_walutomat_ws`` mapping. Walutomat's WebSocket
            accepts ``"*"`` as subscribe-all (already used by
            ``WalutomatSnapshotUpdaterService``).
        """
        publisher = WalutomatMarketDataPublisher(symbols=["*"])
        assert publisher._validate_symbols(["*"]) == ["*"]
        assert publisher.symbols == ["*"]


class TestWalutomatPublisher:
    """Tests for WalutomatMarketDataPublisher functionality."""

    @patch("snapper.config.settings.get_settings")
    def test_initialization(self, mock_get_settings: MagicMock) -> None:
        """Verify publisher initializes with correct default state.

        Given mocked settings with ZMQ endpoint,
        When WalutomatMarketDataPublisher is created,
        Then symbols are stored and running=False, heartbeat=0.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = WalutomatMarketDataPublisher(symbols=["EUR-PLN", "USD-PLN"])
        assert publisher.symbols == ["EUR-PLN", "USD-PLN"]
        assert publisher.running is False
        assert publisher.heartbeat_seq == 0
        assert publisher.repository is None

    @patch("snapper.config.settings.get_settings")
    def test_get_status(self, mock_get_settings: MagicMock) -> None:
        """Verify status returns current publisher state.

        Given a publisher instance,
        When get_status is called,
        Then returns dict with running, symbols, broker_endpoint, heartbeat.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_settings.zmq_heartbeat_interval_ms = 1000
        mock_get_settings.return_value = mock_settings
        publisher = WalutomatMarketDataPublisher(symbols=["EUR-PLN"])
        status = publisher.get_status()
        assert status["running"] is False
        assert status["symbols"] == ["EUR-PLN"]
        assert "broker_endpoint" in status
        assert status["heartbeat_seq"] == 0

    @patch("snapper.config.settings.get_settings")
    def test_get_exchange_name(self, mock_get_settings: MagicMock) -> None:
        """Verify exchange name returns 'walutomat'.

        Given a WalutomatMarketDataPublisher instance,
        When _get_exchange_name is called,
        Then it returns 'walutomat'.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = WalutomatMarketDataPublisher(symbols=[])
        assert publisher._get_exchange_name() == "walutomat"

    @patch("snapper.config.settings.get_settings")
    def test_candle_source_for_returns_calculated(self, mock_get_settings: MagicMock) -> None:
        """Verify Walutomat 1m bars are tagged calculated (quote-poll built).

        Given a WalutomatMarketDataPublisher instance,
        When _candle_source_for is called for 1m,
        Then it returns 'calculated'.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = WalutomatMarketDataPublisher(symbols=[])
        assert publisher._candle_source_for("1m") == "calculated"

    @patch("snapper.config.settings.get_settings")
    def test_candle_price_basis_for_returns_quote_mid(self, mock_get_settings: MagicMock) -> None:
        """Verify Walutomat bars declare the top-of-book mid as their basis.

        Given a WalutomatMarketDataPublisher instance,
        When _candle_price_basis_for is called for 1m and for a rollup frame,
        Then both return 'quote_mid' — a rollup of these bars inherits the
        basis of the bars it rolls up.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = WalutomatMarketDataPublisher(symbols=[])
        assert publisher._candle_price_basis_for("1m") == "quote_mid"
        assert publisher._candle_price_basis_for("1h") == "quote_mid"

    @patch("snapper.config.settings.get_settings")
    def test_supports_public_trades_false(self, mock_get_settings: MagicMock) -> None:
        """Verify Walutomat reports no public trade feed.

        Given a WalutomatMarketDataPublisher instance,
        When _supports_public_trades is called,
        Then it returns False.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = WalutomatMarketDataPublisher(symbols=[])
        assert publisher._supports_public_trades() is False

    @patch("snapper.config.settings.get_settings")
    def test_create_exchange_client(self, mock_get_settings: MagicMock) -> None:
        """Verify factory creates WalutomatExchangeClient.

        Given a WalutomatMarketDataPublisher instance,
        When _create_exchange_client is called,
        Then it returns a WalutomatExchangeClient.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = WalutomatMarketDataPublisher(symbols=[])
        client = publisher._create_exchange_client()
        assert client is not None
        assert client.__class__.__name__ == "WalutomatExchangeClient"

    @patch("snapper.config.settings.get_settings")
    def test_get_default_parameters(self, mock_get_settings: MagicMock) -> None:
        """Verify default kwargs extracts walutomat symbols from settings.

        Given settings with instruments.walutomat containing symbols,
        When get_default_parameters is called,
        Then returns dict with those symbols.
        """
        mock_settings = MagicMock()
        mock_settings.instruments = {"walutomat": ["EUR-PLN", "USD-PLN"]}
        mock_get_settings.return_value = mock_settings
        kwargs = WalutomatMarketDataPublisher.get_default_parameters(mock_settings)
        assert kwargs == {"symbols": ["EUR-PLN", "USD-PLN"]}

    @pytest.mark.asyncio
    async def test_start_sets_and_resets_current_publisher_context_var(self) -> None:
        """Start override registers this publisher in the SDK-patch ContextVar.

        Given: A WalutomatMarketDataPublisher instance with a patched
            base ``start`` that observes the ContextVar.
        When: ``start()`` is awaited,
        Then: ``_CURRENT_PUBLISHER`` resolves to the publisher during
            ``super().start()`` execution, then resets to ``None`` after
            ``start()`` returns.

        Required so the
        :class:`snapper.infrastructure.network.pooled_httpx_transport.PooledAsyncTransport`
        wired into ``WalutomatExchangeClient._http_client`` reads
        ``"walutomat"`` as the reservation tag. The pool's matching
        route with ``allowed_exchanges=["walutomat"]`` then accepts the
        reservation and pins HTTP polling to the configured egress
        tunnel.
        """
        publisher = WalutomatMarketDataPublisher(symbols=["EUR-PLN"])
        observed: list[WalutomatMarketDataPublisher | None] = []

        async def fake_super_start(self: WalutomatMarketDataPublisher) -> None:
            observed.append(_CURRENT_PUBLISHER.get())

        with patch(
            "snapper.messaging.publishers.base.MarketDataPublisherService.start",
            new=fake_super_start,
        ):
            await publisher.start()

        assert observed == [publisher]
        assert _CURRENT_PUBLISHER.get() is None

    @pytest.mark.asyncio
    async def test_attempt_liveness_recovery_sets_wakeup_and_resets_counters(self) -> None:
        """Walutomat liveness recovery wakes polling backoff.

        Given: A Walutomat publisher with a client in backoff,
        When: Liveness recovery is attempted,
        Then: Error counters reset and the wakeup event is set.
        """
        publisher = WalutomatMarketDataPublisher(symbols=["EUR-PLN"])
        client = MagicMock()
        client._consecutive_error_count = 5
        client._backoff_attempts = 2
        client._backoff_wakeup_event = asyncio.Event()
        publisher._exchange_client = client
        await publisher._attempt_liveness_recovery("stale")
        assert client._consecutive_error_count == 0
        assert client._backoff_attempts == 0
        assert client._backoff_wakeup_event.is_set()

    @pytest.mark.asyncio
    async def test_attempt_liveness_recovery_resets_counters_without_wakeup(self) -> None:
        """Walutomat liveness recovery tolerates absent wakeup event.

        Given: A Walutomat client whose polling loop has not created a wakeup event,
        When: Liveness recovery is attempted,
        Then: Error counters reset without raising.
        """
        publisher = WalutomatMarketDataPublisher(symbols=["EUR-PLN"])
        client = MagicMock()
        client._consecutive_error_count = 5
        client._backoff_attempts = 2
        client._backoff_wakeup_event = None
        publisher._exchange_client = client
        await publisher._attempt_liveness_recovery("stale")
        assert client._consecutive_error_count == 0
        assert client._backoff_attempts == 0

    @pytest.mark.asyncio
    async def test_attempt_liveness_recovery_skips_without_client(self) -> None:
        """Walutomat liveness recovery tolerates missing client.

        Given: A Walutomat publisher without an exchange client,
        When: Liveness recovery is attempted,
        Then: It completes without raising.
        """
        publisher = WalutomatMarketDataPublisher(symbols=["EUR-PLN"])
        publisher._exchange_client = None
        await publisher._attempt_liveness_recovery("stale")

    @pytest.mark.asyncio
    async def test_start_resets_context_var_even_if_super_raises(self) -> None:
        """Token reset must occur in ``finally`` so failures do not leak the ContextVar.

        Given: ``super().start()`` raises an arbitrary RuntimeError,
        When: ``start()`` is awaited,
        Then: The exception propagates AND ``_CURRENT_PUBLISHER`` is
            restored to its prior value (``None``).
        """
        publisher = WalutomatMarketDataPublisher(symbols=["EUR-PLN"])
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

    def test_venue_feed_health_is_empty_before_the_client_exists(self) -> None:
        """Verify the heartbeat hook tolerates a publisher that has not started.

        Given: A Walutomat publisher whose exchange client has not been created,
        When: The venue health hook is read,
        Then: It reports no refusals and no degradation rather than raising
            inside the heartbeat loop.
        """
        publisher = WalutomatMarketDataPublisher(symbols=["EUR-PLN"])
        publisher._exchange_client = None
        health = publisher._venue_feed_health()
        assert health.meta == {}
        assert health.degraded is False

    def test_venue_feed_health_publishes_refusal_counts_and_durations(self) -> None:
        """Verify refused marks reach the heartbeat with counts and durations.

        Given: A client that has refused one symbol several times without
            reaching the chronic-refusal duration,
        When: The venue health hook is read,
        Then: The refused symbol, its cumulative refusal count and its
            continuous refusal duration ride the heartbeat meta, and the feed
            is not yet degraded. Fail-closed without visibility is fail-stale:
            the ticker plane has no age gate, so a refused pair keeps serving
            a frozen last-good mark under an otherwise healthy heartbeat.
        """
        publisher = WalutomatMarketDataPublisher(symbols=["TRY-PLN"])
        client = WalutomatExchangeClient()
        offer = WalutomatBestOffer(bid_now=0.01, ask_now=4.31, forex_now=4.3166)
        for _ in range(3):
            client._note_refused_mark("TRY-PLN", offer)
        publisher._exchange_client = client
        health = publisher._venue_feed_health()
        assert health.meta["refused_marks"] == ["TRY-PLN"]
        assert health.meta["refused_mark_counts"] == {"TRY-PLN": 3}
        assert health.meta["refused_mark_seconds"] == {"TRY-PLN": 0}
        assert health.meta["refused_mark_fractions"] == {}
        assert health.degraded is False

    def test_venue_feed_health_degrades_after_the_chronic_refusal_duration(self) -> None:
        """Verify a chronically refused symbol degrades the feed to WARNING.

        Given: A client whose symbol has been refused continuously for longer
            than the venue's chronic-refusal duration,
        When: The venue health hook is read,
        Then: It reports degraded, which the base heartbeat turns into a
            WARNING status — the signal that survives after the entry log line
            has scrolled away.
        """
        publisher = WalutomatMarketDataPublisher(symbols=["TRY-PLN"])
        client = WalutomatExchangeClient()
        offer = WalutomatBestOffer(bid_now=0.01, ask_now=4.31, forex_now=4.3166)
        client._note_refused_mark("TRY-PLN", offer)
        client._refused_since["TRY-PLN"] = (
            time.monotonic() - WALUTOMAT_REFUSAL_WARNING_SECONDS - 1.0
        )
        publisher._exchange_client = client
        health = publisher._venue_feed_health()
        assert health.degraded is True

    def test_wildcard_universe_is_empty_before_the_client_exists(self) -> None:
        """Verify wildcard resolution tolerates a publisher that has not started.

        Given: A wildcard publisher whose exchange client has not been created,
        When: The wildcard universe is resolved for lag seeding,
        Then: It is empty rather than raising, so the seed degrades to the
            previous behaviour instead of breaking the start path.
        """
        publisher = WalutomatMarketDataPublisher(symbols=["*"])
        publisher._exchange_client = None
        assert publisher._wildcard_symbol_universe() == []

    def test_wildcard_universe_resolves_the_venue_pair_list(self) -> None:
        """Verify ``["*"]`` resolves to concrete pairs for the lag baseline.

        Given: A wildcard publisher whose connected client knows its pairs,
        When: The lag baseline is seeded,
        Then: Every pair is seeded under its concrete native key and the
            sentinel is not. The publisher forwards ``"*"`` verbatim and the
            client expands it privately, so without this the per-symbol lag
            path was dead on the configuration that actually ships — every
            exchange defaults to ``["*"]``.
        """
        publisher = WalutomatMarketDataPublisher(symbols=["*"])
        client = WalutomatExchangeClient()
        client._last_data = {
            "EUR_PLN": WalutomatMarketPair(
                pair="EUR_PLN",
                bestOffers=WalutomatBestOffer(bid_now=4.3107, ask_now=4.3157, forex_now=4.3166),
            ),
            "USD_PLN": WalutomatMarketPair(
                pair="USD_PLN",
                bestOffers=WalutomatBestOffer(bid_now=3.9512, ask_now=3.9556, forex_now=3.9571),
            ),
        }
        publisher._exchange_client = client
        assert publisher._wildcard_symbol_universe() == ["EUR-PLN", "USD-PLN"]
        publisher._seed_symbol_lag_baseline(publisher.symbols)
        assert sorted(publisher._last_data_timestamps) == ["EUR-PLN", "USD-PLN"]
        assert "*" not in publisher._last_data_timestamps

    def test_wildcard_universe_is_fail_soft_when_the_venue_cannot_name_it(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Verify an unresolvable wildcard degrades the seed, not the publisher.

        Given: A wildcard publisher whose client has fetched no payload yet,
        When: The wildcard universe is resolved,
        Then: It is empty and the failure is logged. Seeding is observability;
            it must never take down the start path of a live market-data feed.
        """
        publisher = WalutomatMarketDataPublisher(symbols=["*"])
        publisher._exchange_client = WalutomatExchangeClient()
        sink_id = logger.add(caplog.handler, format="{message}", level="DEBUG")
        try:
            with caplog.at_level("DEBUG"):
                assert publisher._wildcard_symbol_universe() == []
        finally:
            logger.remove(sink_id)
        assert any("cannot resolve the wildcard" in record.message for record in caplog.records)
