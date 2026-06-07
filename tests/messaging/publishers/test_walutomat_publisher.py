"""Unit tests for WalutomatMarketDataPublisher."""

import asyncio
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.infrastructure.exchanges.kraken_sdk_patches import _CURRENT_PUBLISHER
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
