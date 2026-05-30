"""Unit tests for KrakenMarketDataPublisher."""

import asyncio
from collections import deque
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.application.process_manager.registry import get_registered_processes
from snapper.config.app import AppSettings
from snapper.core.types import ProcessRestartPolicyEnum
from snapper.infrastructure.exchanges.implementations.kraken import KrakenExchangeClient
from snapper.infrastructure.exchanges.kraken_sdk_patches import _CURRENT_PUBLISHER
from snapper.infrastructure.exchanges.kraken_sdk_patches import _RECONNECT_LIMIT
from snapper.messaging.publishers import kraken as kraken_module
from snapper.messaging.publishers.base import MarketDataPublisherService
from snapper.messaging.publishers.kraken import KrakenMarketDataPublisher


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


class TestKrakenReconnectWatchdog:
    """Tests for the publisher-side reconnect-storm watchdog.

    Phase A.2 of the Kraken 429 plan adds an in-process WS-restart
    mechanism so the publisher recovers from reconnect cascades without
    leaving the SDK to silently exhaust ``MAX_RECONNECT_NUM`` and die.
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
    async def test_force_ws_restart_calls_disconnect_then_ensure(self) -> None:
        """Spec — full Given/When/Then below.

        Given a publisher with an attached exchange client,
        When ``_force_ws_restart`` is awaited,
        Then ``disconnect_websocket`` is called, the back-off sleep runs,
        and ``_ensure_ws_connected`` is called afterwards.
        """
        pub = KrakenMarketDataPublisher(symbols=["BTC-USD"])
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
