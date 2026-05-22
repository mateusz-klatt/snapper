"""Unit tests for KrakenEquitiesMarketDataPublisher."""

from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.config.settings import AppSettings
from snapper.infrastructure.exchanges.implementations.kraken_equities import (
    KrakenEquitiesExchangeClient,
)
from snapper.infrastructure.exchanges.kraken_sdk_patches import _CURRENT_PUBLISHER
from snapper.messaging.publishers.kraken_equities import KrakenEquitiesMarketDataPublisher


class TestKrakenEquitiesMarketDataPublisher:
    """Tests for KrakenEquitiesMarketDataPublisher functionality."""

    def test_create_exchange_client_returns_equities_client(self) -> None:
        """Verify factory method creates KrakenEquitiesExchangeClient.

        Given: A KrakenEquitiesMarketDataPublisher instance,
        When: _create_exchange_client is called,
        Then: Returns a KrakenEquitiesExchangeClient instance.
        """
        publisher = KrakenEquitiesMarketDataPublisher(symbols=["CLM6-NYMEX"])
        with patch(
            "snapper.messaging.publishers.kraken_equities.KrakenEquitiesExchangeClient"
        ) as mock_cls:
            mock_cls.return_value = MagicMock(spec=KrakenEquitiesExchangeClient)
            client = publisher._create_exchange_client()
            assert isinstance(client, KrakenEquitiesExchangeClient)

    def test_get_exchange_name_returns_kraken_equities(self) -> None:
        """Verify exchange name returns 'kraken_equities'.

        Given: A KrakenEquitiesMarketDataPublisher instance,
        When: _get_exchange_name is called,
        Then: Returns 'kraken_equities'.
        """
        publisher = KrakenEquitiesMarketDataPublisher(symbols=["CLM6-NYMEX"])
        assert publisher._get_exchange_name() == "kraken_equities"

    def test_validate_symbols_filters_invalid(self) -> None:
        """Verify invalid symbols are filtered out.

        Given: Publisher and symbols including one that raises ValueError,
        When: _validate_symbols is called,
        Then: Invalid symbols are excluded.
        """
        publisher = KrakenEquitiesMarketDataPublisher(symbols=[])

        def _lookup(s: str) -> str:
            if s == "INVALID":
                raise ValueError("unknown")
            return s

        with patch(
            "snapper.messaging.publishers.kraken_equities.native_to_kraken_equities_ws",
            side_effect=_lookup,
        ):
            valid = publisher._validate_symbols(["CLM6-NYMEX", "INVALID", "GCQ6-COMEX"])
        assert valid == ["CLM6-NYMEX", "GCQ6-COMEX"]

    def test_validate_symbols_removes_duplicates(self) -> None:
        """Verify duplicate symbols are removed.

        Given: Symbols list with duplicates,
        When: _validate_symbols is called,
        Then: Duplicates removed.
        """
        publisher = KrakenEquitiesMarketDataPublisher(symbols=[])
        with patch(
            "snapper.messaging.publishers.kraken_equities.native_to_kraken_equities_ws",
            return_value="CLM6.NYMEX",
        ):
            valid = publisher._validate_symbols(["CLM6-NYMEX", "CLM6-NYMEX", "GCQ6-COMEX"])
        assert len(valid) == 2
        assert valid == ["CLM6-NYMEX", "GCQ6-COMEX"]

    def test_validate_symbols_wildcard_expands_to_catalog(self) -> None:
        """Verify ``["*"]`` expands to the full catalog of equities symbols.

        Given: Publisher and ``symbols=["*"]`` request,
        When: _validate_symbols is called,
        Then: The full list returned by
            ``get_available_kraken_equities_symbols`` is returned verbatim.
            Mirrors the spot/futures wildcard pattern — Kraken
            Equities WS has no server-side wildcard, so expansion
            happens client-side.
        """
        publisher = KrakenEquitiesMarketDataPublisher(symbols=[])
        catalog = ["CLM6-NYMEX", "GCQ6-COMEX", "ESZ6-CME"]
        with patch(
            "snapper.messaging.publishers.kraken_equities.get_available_kraken_equities_symbols",
            return_value=catalog,
        ):
            valid = publisher._validate_symbols(["*"])
        assert valid == catalog

    def test_get_max_symbols_per_connection_returns_zero(self) -> None:
        """Verify Kraken Equities WS has no documented symbol limit.

        Given: A KrakenEquitiesMarketDataPublisher instance,
        When: _get_max_symbols_per_connection is called,
        Then: Returns 0 (unlimited).
        """
        publisher = KrakenEquitiesMarketDataPublisher(symbols=["CLM6-NYMEX"])
        assert publisher._get_max_symbols_per_connection() == 0

    def test_get_default_parameters_from_settings(self) -> None:
        """Verify default parameters extracts kraken_equities symbols.

        Given: Settings with instruments.kraken_equities containing symbols,
        When: get_default_parameters is called,
        Then: Returns dict with those symbols.
        """
        mock_settings = MagicMock(spec=AppSettings)
        mock_settings.instruments = {
            "kraken_equities": ["CLM6-NYMEX"],
            "kraken": ["BTC-USD"],
        }
        params = KrakenEquitiesMarketDataPublisher.get_default_parameters(mock_settings)
        assert params == {"symbols": ["CLM6-NYMEX"]}

    def test_get_default_parameters_with_empty_list(self) -> None:
        """Verify default parameters handles empty instrument list.

        Given: Settings with empty kraken_equities instruments,
        When: get_default_parameters is called,
        Then: Returns dict with empty symbols list.
        """
        mock_settings = MagicMock(spec=AppSettings)
        mock_settings.instruments = {"kraken_equities": [], "kraken": ["BTC-USD"]}
        params = KrakenEquitiesMarketDataPublisher.get_default_parameters(mock_settings)
        assert params == {"symbols": []}

    def test_get_default_parameters_with_missing_key(self) -> None:
        """Verify default parameters handles missing kraken_equities key.

        Given: Settings without kraken_equities key in instruments,
        When: get_default_parameters is called,
        Then: Returns dict with empty symbols list.
        """
        mock_settings = MagicMock(spec=AppSettings)
        mock_settings.instruments = {"kraken": ["BTC-USD"]}
        params = KrakenEquitiesMarketDataPublisher.get_default_parameters(mock_settings)
        assert params == {"symbols": []}

    @pytest.mark.asyncio
    async def test_candle_loop_is_noop(self) -> None:
        """Candle loop does nothing since Kraken Equities has no WS candle feed.

        Given: A KrakenEquitiesMarketDataPublisher instance,
        When: _candle_loop is called,
        Then: Returns immediately without error.
        """
        publisher = KrakenEquitiesMarketDataPublisher(symbols=["CLM6-NYMEX"])
        await publisher._candle_loop(["CLM6-NYMEX"], "1m")

    @pytest.mark.asyncio
    async def test_start_sets_and_resets_current_publisher_context_var(self) -> None:
        """Start override registers this publisher in the SDK-patch ContextVar.

        Given: A KrakenEquitiesMarketDataPublisher instance with a
            patched base ``start`` that observes the ContextVar.
        When: ``start()`` is awaited,
        Then: ``_CURRENT_PUBLISHER`` resolves to the publisher
            during ``super().start()`` execution, then resets to
            ``None`` after ``start()`` returns.

        This proves the SDK connect shim will read this publisher's
        ``_get_exchange_name()`` ("kraken_equities") instead of the
        legacy hardcoded ``"kraken"`` tag when reserving an
        egress-pool route — required so the Phase B'.5
        ``allowed_exchanges=["kraken_equities"]`` filter pins this
        publisher to the dedicated NYC tunnel.
        """
        publisher = KrakenEquitiesMarketDataPublisher(symbols=["CLM6-NYMEX"])
        observed: list[KrakenEquitiesMarketDataPublisher | None] = []

        async def fake_super_start(self: KrakenEquitiesMarketDataPublisher) -> None:
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
            is restored to its prior value (``None``) — no leak
            across publisher restarts.
        """
        publisher = KrakenEquitiesMarketDataPublisher(symbols=["CLM6-NYMEX"])
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
