"""Unit tests for KrakenEquitiesMarketDataPublisher."""

import asyncio
from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import PropertyMock
from unittest.mock import patch

import pytest

from snapper.config.settings import AppSettings
from snapper.infrastructure.exchanges.implementations import kraken_equities as ke
from snapper.infrastructure.exchanges.implementations.kraken_equities import (
    KrakenEquitiesExchangeClient,
)
from snapper.infrastructure.network.egress_context import _CURRENT_PUBLISHER
from snapper.messaging.publishers import kraken_equities as equities_module
from snapper.messaging.publishers.kraken_equities import KrakenEquitiesMarketDataPublisher
from snapper.messaging.publishers.kraken_equities import _is_cme_closed


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
        mock_cls.assert_called_once_with(
            repository=None,
            realtime_ws_enabled=False,
            realtime_wallet_public_id="",
        )

    def test_create_exchange_client_passes_realtime_settings(self) -> None:
        """Verify factory method wires DB-backed realtime settings.

        Given: A started publisher with repository and settings installed,
        When: _create_exchange_client is called,
        Then: The client receives repository plus realtime auth settings.
        """
        publisher = KrakenEquitiesMarketDataPublisher(symbols=["CLM6-NYMEX"])
        repository = MagicMock()
        settings = MagicMock(spec=AppSettings)
        settings.kraken_equities_realtime_ws_enabled = True
        settings.kraken_equities_realtime_wallet_public_id = "wallet-1"
        publisher.repository = repository
        publisher.settings = settings

        with patch(
            "snapper.messaging.publishers.kraken_equities.KrakenEquitiesExchangeClient"
        ) as mock_cls:
            mock_cls.return_value = MagicMock(spec=KrakenEquitiesExchangeClient)
            client = publisher._create_exchange_client()
            assert isinstance(client, KrakenEquitiesExchangeClient)
        mock_cls.assert_called_once_with(
            repository=repository,
            realtime_ws_enabled=True,
            realtime_wallet_public_id="wallet-1",
        )

    def test_realtime_settings_default_when_db_unavailable(self) -> None:
        """Bootstrap-only settings keep realtime disabled."""
        publisher = KrakenEquitiesMarketDataPublisher(symbols=["CLM6-NYMEX"])
        settings = MagicMock(spec=AppSettings)
        publisher.settings = settings
        with (
            patch.object(
                type(settings),
                "kraken_equities_realtime_ws_enabled",
                new_callable=PropertyMock,
                create=True,
            ) as enabled_prop,
            patch.object(
                type(settings),
                "kraken_equities_realtime_wallet_public_id",
                new_callable=PropertyMock,
                create=True,
            ) as wallet_prop,
        ):
            enabled_prop.side_effect = RuntimeError("no db")
            wallet_prop.side_effect = RuntimeError("no db")
            assert publisher._kraken_equities_realtime_ws_enabled() is False
            assert publisher._kraken_equities_realtime_wallet_public_id() == ""

    def test_get_exchange_name_returns_kraken_equities(self) -> None:
        """Verify exchange name returns 'kraken_equities'.

        Given: A KrakenEquitiesMarketDataPublisher instance,
        When: _get_exchange_name is called,
        Then: Returns 'kraken_equities'.
        """
        publisher = KrakenEquitiesMarketDataPublisher(symbols=["CLM6-NYMEX"])
        assert publisher._get_exchange_name() == "kraken_equities"

    def test_candle_source_for_returns_calculated(self) -> None:
        """Verify equities 1m bars are tagged calculated (trade-built).

        Given: A KrakenEquitiesMarketDataPublisher instance,
        When: _candle_source_for is called for 1m,
        Then: Returns 'calculated' (Snapper-computed, not venue OHLC).
        """
        publisher = KrakenEquitiesMarketDataPublisher(symbols=["CLM6-NYMEX"])
        assert publisher._candle_source_for("1m") == "calculated"

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

    def test_threshold_zero_during_cme_closure(self) -> None:
        """Equities liveness recovery is disabled during CME closure.

        Given: The CME schedule helper reports a closure,
        When: The liveness threshold is read,
        Then: The publisher returns zero to disable recovery.
        """
        publisher = KrakenEquitiesMarketDataPublisher(symbols=["CLM6-NYMEX"])
        with patch.object(equities_module, "_is_cme_closed", return_value=True):
            assert publisher._get_liveness_recovery_threshold_s() == 0

    def test_threshold_during_cme_open(self) -> None:
        """Equities liveness recovery uses the venue threshold while open.

        Given: The CME schedule helper reports an open market,
        When: The liveness threshold is read,
        Then: The publisher returns the Equities 120 second threshold.
        """
        publisher = KrakenEquitiesMarketDataPublisher(symbols=["CLM6-NYMEX"])
        with patch.object(equities_module, "_is_cme_closed", return_value=False):
            assert publisher._get_liveness_recovery_threshold_s() == 120

    def test_market_schedule_state_reports_closure_and_reopen(self) -> None:
        """During a CME closure the heartbeat carries closed + next reopen.

        Given: A Saturday instant inside the weekend closure
            (2026-05-23 12:00 UTC),
        When: The heartbeat market-schedule hook is read,
        Then: It reports market-closed with the Sunday 22:00 UTC reopen so
            the heartbeat surfaces expected silence instead of a fault.
        """
        publisher = KrakenEquitiesMarketDataPublisher(symbols=["CLM6-NYMEX"])
        assert publisher._market_schedule_state(datetime(2026, 5, 23, 12, 0, tzinfo=UTC)) == (
            True,
            datetime(2026, 5, 24, 22, 0, tzinfo=UTC),
        )

    def test_market_schedule_state_open_reports_no_closure(self) -> None:
        """While the venue is open the hook reports no closure and no reopen.

        Given: A Friday afternoon instant before the 16:00 CT break
            (2026-05-22 20:59 UTC),
        When: The heartbeat market-schedule hook is read,
        Then: It reports ``(False, None)`` so the heartbeat behaves as a
            normal live feed.
        """
        publisher = KrakenEquitiesMarketDataPublisher(symbols=["CLM6-NYMEX"])
        assert publisher._market_schedule_state(datetime(2026, 5, 22, 20, 59, tzinfo=UTC)) == (
            False,
            None,
        )

    @pytest.mark.parametrize(
        ("now_utc", "expected"),
        [
            (datetime(2026, 5, 23, 12, 0, tzinfo=UTC), True),
            (datetime(2026, 5, 24, 21, 59, tzinfo=UTC), True),
            (datetime(2026, 5, 24, 22, 1, tzinfo=UTC), False),
            (datetime(2026, 5, 18, 20, 59, tzinfo=UTC), False),
            (datetime(2026, 5, 18, 21, 1, tzinfo=UTC), True),
            (datetime(2026, 5, 18, 21, 59, tzinfo=UTC), True),
            (datetime(2026, 5, 18, 22, 1, tzinfo=UTC), False),
            (datetime(2026, 5, 25, 18, 0, tzinfo=UTC), True),
            (datetime(2026, 5, 25, 22, 1, tzinfo=UTC), False),
            (datetime(2026, 5, 22, 20, 59, tzinfo=UTC), False),
            (datetime(2026, 5, 22, 21, 59, tzinfo=UTC), True),
            (datetime(2026, 5, 22, 22, 1, tzinfo=UTC), True),
        ],
    )
    def test_is_cme_closed_for_each_window_boundary(
        self,
        now_utc: datetime,
        expected: bool,
    ) -> None:
        """CME schedule helper matches weekend and daily-break boundaries.

        Given: Representative UTC datetimes around the CME closure
            windows on a plain Monday (2026-05-18), the weekend, and
            Memorial Day (2026-05-25 — the shared calendar's holiday
            table halts it at 12:00 CT and reopens 17:00 CT same day),
        When: _is_cme_closed is evaluated,
        Then: It reports closed only inside the approved windows — the
            shared calendar treats Friday 21:00-22:00 UTC as closed (the
            daily break rolls into the weekend), unlike the old local
            helper whose Friday branch only tested the weekend rule.
        """
        assert _is_cme_closed(now_utc) is expected

    @pytest.mark.asyncio
    async def test_attempt_liveness_recovery_disconnects_and_ensures(self) -> None:
        """Equities liveness recovery rebuilds the websocket client.

        Given: An Equities publisher with an attached exchange client,
        When: Liveness recovery is attempted,
        Then: The client disconnects and reconnects its websocket.
        """
        publisher = KrakenEquitiesMarketDataPublisher(symbols=["CLM6-NYMEX"])
        client = MagicMock()
        client.disconnect = AsyncMock()
        client.connect = AsyncMock()
        client._ensure_ws_connected = AsyncMock()
        publisher._exchange_client = client
        await publisher._attempt_liveness_recovery("stale")
        client.disconnect.assert_awaited_once()
        client.connect.assert_awaited_once()
        client._ensure_ws_connected.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_liveness_recovery_reopens_lifecycle_for_auth_demotion(self) -> None:
        """Realtime liveness rebuild allows later auth rejection demotion."""
        publisher = KrakenEquitiesMarketDataPublisher(symbols=["CLM6-NYMEX"])
        client = KrakenEquitiesExchangeClient(realtime_ws_enabled=True)
        publisher._exchange_client = client
        auth_ws = AsyncMock()
        public_ws = AsyncMock()

        async def _prepare_auth() -> bool:
            client._ws_auth_active = True
            client._ws_token = "token-1"
            client._ws_token_refresh_at = ke.monotonic() + 100.0
            client._ws_token_expires_at = ke.monotonic() + 200.0
            return True

        ack = {
            "method": "subscribe",
            "result": {
                "channel": "ticker",
                "symbol": "CLM6.NYMEX",
                "snapshot": True,
            },
            "success": False,
            "error": "invalid token",
        }
        try:
            with (
                patch.object(client, "_prepare_realtime_ws_auth", side_effect=_prepare_auth),
                patch.object(client, "_force_sdk_public_endpoint", return_value=True),
                patch.object(client, "_disable_sdk_reconnect_replay_for_auth", return_value=True),
                patch(
                    "snapper.infrastructure.exchanges.implementations.kraken_equities."
                    "SpotWSClient",
                    side_effect=[auth_ws, public_ws],
                ) as ws_cls,
            ):
                await publisher._attempt_liveness_recovery("stale")
                assert client._ws_closing is False
                assert client._ws_client is auth_ws
                await client._on_ws_message(ack)
                task = client._ws_public_demotion_task
                assert task is not None
                await asyncio.wait_for(task, timeout=1.0)

            assert [call.kwargs["ws_url"] for call in ws_cls.call_args_list] == [
                ke._WS_AUTH_URL,
                ke._WS_URL,
            ]
            auth_ws.close.assert_awaited_once()
            public_ws.start.assert_awaited_once()
            assert client._ws_client is public_ws
            assert client._ws_auth_active is False
        finally:
            await client.disconnect()

    @pytest.mark.asyncio
    async def test_attempt_liveness_recovery_skips_without_client(self) -> None:
        """Equities liveness recovery tolerates missing client.

        Given: An Equities publisher without an exchange client,
        When: Liveness recovery is attempted,
        Then: It completes without raising.
        """
        publisher = KrakenEquitiesMarketDataPublisher(symbols=["CLM6-NYMEX"])
        publisher._exchange_client = None
        await publisher._attempt_liveness_recovery("stale")

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
        egress-pool route — required so the
        ``allowed_exchanges=["kraken_equities"]`` filter pins this
        publisher to its dedicated egress tunnel.
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
