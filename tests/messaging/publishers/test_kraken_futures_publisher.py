"""Unit tests for KrakenFuturesMarketDataPublisher."""

from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.config.app import AppSettings
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)
from snapper.infrastructure.exchanges.kraken_sdk_patches import _CURRENT_PUBLISHER
from snapper.messaging.publishers.kraken_futures import KrakenFuturesMarketDataPublisher


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
    async def test_attempt_liveness_recovery_skips_without_client(self) -> None:
        """Futures liveness recovery tolerates missing client.

        Given: A Futures publisher without an exchange client,
        When: Liveness recovery is attempted,
        Then: It completes without raising.
        """
        publisher = KrakenFuturesMarketDataPublisher(symbols=["BTC-USD-PERP"])
        publisher._exchange_client = None
        await publisher._attempt_liveness_recovery("stale")

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
