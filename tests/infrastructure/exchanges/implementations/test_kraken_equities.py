"""Tests for Kraken Equities exchange client."""

import asyncio
import contextlib
from collections.abc import Generator
from datetime import UTC as _UTC
from datetime import datetime as _dt
from datetime import timedelta as _td
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import httpx
import pytest
import requests
from loguru import logger

from snapper.config.credentials import CredentialNotFoundError
from snapper.core.json_types import JsonValue
from snapper.data.repository_types import WalletCredentialRow
from snapper.data.repository_types import WalletRow
from snapper.infrastructure.exchanges._subscription_request import SubscriptionRequest
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import InstrumentPairDescriptor
from snapper.infrastructure.exchanges.contracts import OhlcvSnapshot
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate
from snapper.infrastructure.exchanges.implementations import kraken_equities as ke
from snapper.infrastructure.exchanges.implementations.kraken_equities import (
    KrakenEquitiesExchangeClient,
)
from snapper.infrastructure.exchanges.implementations.kraken_equities import _enqueue_or_drop_oldest
from snapper.infrastructure.exchanges.implementations.kraken_equities import _timeframe_to_interval
from snapper.infrastructure.exchanges.kraken_sdk_patches import _WS_CLOSE_TIMEOUT_S
from snapper.infrastructure.exchanges.kraken_sdk_patches import _WS_PING_INTERVAL_S
from snapper.infrastructure.exchanges.kraken_sdk_patches import _WS_PING_TIMEOUT_S
from snapper.infrastructure.exchanges.kraken_sdk_patches import _wrap_connect_factory
from snapper.infrastructure.network.egress_context import _CURRENT_PUBLISHER
from snapper.infrastructure.network.egress_models import EgressPoolConfig
from snapper.infrastructure.network.egress_models import RouteConfig
from snapper.infrastructure.network.egress_pool import configure_egress_pool
from snapper.infrastructure.network.egress_pool import reset_egress_pool


@pytest.fixture()
def client() -> KrakenEquitiesExchangeClient:
    """Create a KrakenEquitiesExchangeClient instance for testing."""
    return KrakenEquitiesExchangeClient()


class _PublisherDouble:
    """Publisher double exposing the exchange tag used by WS egress routing."""

    def _get_exchange_name(self) -> str:
        """Return the Kraken Equities venue tag."""
        return "kraken_equities"


class _FakeSpotTokenClient:
    """Spot SDK token client double with mutable private proxy state."""

    def __init__(self) -> None:
        """Create SDK-shaped session and proxy attributes."""
        self._SpotClient__session = requests.Session()
        self._SpotClient__proxy = None
        self.URL = "https://api.kraken.com"
        self.seen_sdk_proxy: list[object] = []
        self.seen_session_proxies: list[dict[str, str]] = []
        self.seen_requests: list[tuple[str, str, int]] = []

    def request(self, method: str, path: str, timeout: int) -> dict[str, object]:
        """Capture proxy state during the token request and return a token."""
        session = getattr(self, "_SpotClient__session")
        assert isinstance(session, requests.Session)
        self.seen_sdk_proxy.append(getattr(self, "_SpotClient__proxy"))
        self.seen_session_proxies.append(_proxy_snapshot(session))
        self.seen_requests.append((method, path, timeout))
        return {"token": "token-1", "expires": 900}


class _SdkShapedWsClient:
    """WebSocket double whose close awaits the SDK-shaped parent run task."""

    def __init__(self) -> None:
        """Create close-observation state."""
        self.parent_task: asyncio.Task[None] | None = None
        self.close_started = asyncio.Event()
        self.close_finished = asyncio.Event()
        self.closed = False

    async def close(self) -> None:
        """Mirror SDK close waiting for the connector parent task."""
        self.close_started.set()
        parent_task = self.parent_task
        if parent_task is not None:
            await parent_task
        self.closed = True
        self.close_finished.set()


def _proxy_snapshot(session: requests.Session) -> dict[str, str]:
    """Return a plain proxy mapping snapshot for assertions."""
    return {str(key): str(value) for key, value in session.proxies.items()}


def _wallet_credential_row(
    wallet_public_id: str,
    *,
    exchange: str = "kraken",
    credential_type: str = "api_key_secret",
) -> WalletCredentialRow:
    """Build an active wallet credential row for repository doubles."""
    return {
        "public_id": f"credential-{wallet_public_id}",
        "wallet_public_id": wallet_public_id,
        "exchange": exchange,
        "credential_type": credential_type,
        "encrypted_payload": "encrypted-payload",
        "label": None,
        "timestamp": _dt(2026, 6, 30, tzinfo=_UTC),
        "session_id": "session-1",
        "sequence_id": 1,
    }


def _wallet_row(public_id: str, *, label: str, is_paper: bool = False) -> WalletRow:
    """Build an active wallet catalogue row for repository doubles."""
    return {
        "public_id": public_id,
        "label": label,
        "description": None,
        "is_paper": is_paper,
        "timestamp": _dt(2026, 6, 30, tzinfo=_UTC),
        "session_id": "session-1",
        "sequence_id": 1,
    }


def _equities_public_pool_config() -> EgressPoolConfig:
    """Build a pool whose public Equities route is a NY SOCKS tunnel."""
    return EgressPoolConfig(
        enabled=True,
        routes=[
            RouteConfig(id="default", kind="direct", priority=100),
            RouteConfig(
                id="ny",
                kind="socks5",
                proxy_url="socks5h://snapper-egress-ny:1080",
                priority=1,
                allowed_exchanges=("kraken_equities",),
            ),
        ],
    )


async def _await_public_demotion(client: KrakenEquitiesExchangeClient) -> None:
    """Wait for a scheduled public demotion task to finish in tests."""
    task = client._ws_public_demotion_task
    if task is None:
        return
    await asyncio.wait_for(task, timeout=1.0)
    await asyncio.sleep(0)


class TestClientInit:
    """Tests for client initialization."""

    def test_init_defaults(self) -> None:
        """Initialize with default parameters.

        Given: No arguments,
        When: KrakenEquitiesExchangeClient is created,
        Then: Queues are empty and supports_websocket_executions is False.
        """
        c = KrakenEquitiesExchangeClient()
        assert c.exchange_name == "kraken_equities"
        assert c.supports_websocket_executions is False
        assert c._tick_queue.empty()
        assert c._trade_queue.empty()
        assert c._ws_client is None


class TestRealtimeAuthWebSocket:
    """Tests for the optional Kraken Equities realtime auth feed."""

    @pytest.fixture(autouse=True)
    def _reset_egress_state(self) -> Generator[None]:
        """Reset egress pool and publisher context around auth WS tests."""
        reset_egress_pool()
        token = _CURRENT_PUBLISHER.set(None)
        try:
            yield
        finally:
            reset_egress_pool()
            _CURRENT_PUBLISHER.reset(token)

    def test_subscription_ack_confirms_success_and_idempotent_error(self) -> None:
        """ACK confirmation helper accepts success and already-subscribed errors."""
        assert ke._subscription_ack_confirms(success=True, error=None) is True
        assert ke._subscription_ack_confirms(success=False, error="Already subscribed") is True
        assert ke._subscription_ack_confirms(success=False, error="invalid symbol") is False
        assert ke._is_realtime_auth_subscribe_error(None) is False
        assert ke._redact_sensitive_payload(({"token": "secret"},)) == (
            {"token": "***REDACTED***"},
        )

    @pytest.mark.asyncio
    async def test_flag_off_uses_public_url_without_token(self) -> None:
        """Default-off keeps today's public delayed WS behavior.

        Given: A default client with no realtime config,
        When: The WS client is built,
        Then: SpotWSClient receives the public URL and no token is minted.
        """
        c = KrakenEquitiesExchangeClient()
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.SpotWSClient"
            ) as ws_cls,
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.SpotClient"
            ) as spot_cls,
        ):
            ws_cls.return_value.start = AsyncMock()
            await c._ensure_ws_connected()
        assert ws_cls.call_args.kwargs["ws_url"] == ke._WS_URL
        assert ws_cls.call_args.kwargs["no_public"] is False
        assert c._ws_auth_active is False
        assert c._ws_token is None
        spot_cls.assert_not_called()

    @pytest.mark.asyncio
    async def test_flag_on_with_credentials_uses_auth_url_and_stores_token(self) -> None:
        """Realtime mode mints a token before connecting to the auth URL.

        Given: Realtime is enabled and a Kraken Spot wallet credential resolves,
        When: The WS client is built,
        Then: The auth endpoint is used and token state remains in memory only.
        """
        repository = MagicMock()
        repository.list_active_wallet_credentials = AsyncMock(
            return_value=[_wallet_credential_row("wallet-auto")]
        )
        c = KrakenEquitiesExchangeClient(
            repository=repository,
            realtime_ws_enabled=True,
            realtime_wallet_public_id="wallet-1",
        )
        resolver = MagicMock()
        resolver.get_credentials = AsyncMock(
            return_value={"api_key": "api-key", "api_secret": "api-secret"}
        )

        try:
            with (
                patch(
                    "snapper.infrastructure.exchanges.implementations.kraken_equities."
                    "CredentialResolver",
                    return_value=resolver,
                ) as resolver_cls,
                patch.object(
                    c,
                    "_dispatch_blocking",
                    new_callable=AsyncMock,
                    return_value={"token": "token-1", "expires": 900},
                ) as dispatch,
                patch(
                    "snapper.infrastructure.exchanges.implementations.kraken_equities."
                    "SpotWSClient"
                ) as ws_cls,
            ):
                ws_cls.return_value.start = AsyncMock()
                ws_cls.return_value.close = AsyncMock()
                await c._ensure_ws_connected()

            resolver_cls.assert_called_once_with(repository)
            repository.list_active_wallet_credentials.assert_not_awaited()
            resolver.get_credentials.assert_awaited_once_with(
                exchange="kraken",
                wallet_public_id="wallet-1",
            )
            dispatch.assert_awaited_once()
            dispatch_await_args = dispatch.await_args
            assert dispatch_await_args is not None
            dispatch_args = dispatch_await_args.args
            assert dispatch_args[1:] == ("api-key", "api-secret")
            assert ws_cls.call_args.kwargs["ws_url"] == ke._WS_AUTH_URL
            assert c._ws_auth_active is True
            assert c._ws_token == "token-1"
        finally:
            await c.disconnect()

    @pytest.mark.asyncio
    async def test_realtime_autolookup_single_wallet_mints_token(self) -> None:
        """An empty realtime wallet setting auto-selects one Kraken Spot key."""
        repository = MagicMock()
        repository.list_active_wallet_credentials = AsyncMock(
            return_value=[_wallet_credential_row("wallet-auto")]
        )
        c = KrakenEquitiesExchangeClient(
            repository=repository,
            realtime_ws_enabled=True,
            realtime_wallet_public_id="",
        )
        resolver = MagicMock()
        resolver.get_credentials = AsyncMock(
            return_value={"api_key": "api-key", "api_secret": "api-secret"}
        )
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities."
                "CredentialResolver",
                return_value=resolver,
            ) as resolver_cls,
            patch.object(
                c,
                "_dispatch_blocking",
                new_callable=AsyncMock,
                return_value={"token": "token-1", "expires": 900},
            ),
        ):
            assert await c._refresh_realtime_ws_token() is True

        resolver_cls.assert_called_once_with(repository)
        repository.list_active_wallet_credentials.assert_awaited_once()
        as_of = repository.list_active_wallet_credentials.await_args.kwargs["as_of"]
        assert isinstance(as_of, _dt)
        assert as_of.tzinfo is _UTC
        resolver.get_credentials.assert_awaited_once_with(
            exchange="kraken",
            wallet_public_id="wallet-auto",
        )
        assert c._resolved_realtime_wallet_public_id == "wallet-auto"
        assert c._ws_auth_active is True
        assert c._ws_token == "token-1"

    @pytest.mark.asyncio
    async def test_realtime_autolookup_multiple_wallets_picks_first_and_warns(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Multiple auto candidates use deterministic order and warn to pin."""
        repository = MagicMock()
        repository.list_active_wallet_credentials = AsyncMock(
            return_value=[
                _wallet_credential_row(
                    "wallet-paper",
                    credential_type="paper",
                ),
                _wallet_credential_row("wallet-a"),
                _wallet_credential_row("wallet-b"),
            ]
        )
        c = KrakenEquitiesExchangeClient(
            repository=repository,
            realtime_ws_enabled=True,
            realtime_wallet_public_id="",
        )
        resolver = MagicMock()
        resolver.get_credentials = AsyncMock(
            return_value={"api_key": "api-key", "api_secret": "api-secret"}
        )
        sink_id = logger.add(caplog.handler, format="{message}", level="WARNING")
        try:
            with (
                patch(
                    "snapper.infrastructure.exchanges.implementations.kraken_equities."
                    "CredentialResolver",
                    return_value=resolver,
                ),
                patch.object(
                    c,
                    "_dispatch_blocking",
                    new_callable=AsyncMock,
                    return_value={"token": "token-1", "expires": 900},
                ),
            ):
                assert await c._refresh_realtime_ws_token() is True
        finally:
            logger.remove(sink_id)

        resolver.get_credentials.assert_awaited_once_with(
            exchange="kraken",
            wallet_public_id="wallet-a",
        )
        logged = "\n".join(record.message for record in caplog.records)
        assert "multiple Kraken Spot api_key_secret wallet credentials" in logged
        assert "wallet-a" in logged
        assert "kraken_equities_realtime_wallet_public_id" in logged

    @pytest.mark.asyncio
    async def test_realtime_autolookup_cache_invalidates_after_failures(self) -> None:
        """Cached auto wallet lookup survives success and refreshes after failures."""
        repository = MagicMock()
        repository.list_active_wallet_credentials = AsyncMock(
            side_effect=[
                [_wallet_credential_row("wallet-a")],
                [_wallet_credential_row("wallet-b")],
                [_wallet_credential_row("wallet-c")],
            ]
        )
        c = KrakenEquitiesExchangeClient(
            repository=repository,
            realtime_ws_enabled=True,
            realtime_wallet_public_id="",
        )
        resolver = MagicMock()
        resolver.get_credentials = AsyncMock(
            side_effect=[
                {"api_key": "api-key-a", "api_secret": "api-secret-a"},
                {"api_key": "api-key-a", "api_secret": "api-secret-a"},
                {"api_key": "api-key-a", "api_secret": "api-secret-a"},
                CredentialNotFoundError(exchange="kraken", wallet_public_id="wallet-b"),
                {"api_key": "api-key-c", "api_secret": "api-secret-c"},
            ]
        )
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities."
                "CredentialResolver",
                return_value=resolver,
            ),
            patch.object(
                c,
                "_dispatch_blocking",
                new_callable=AsyncMock,
                side_effect=[
                    {"token": "token-1", "expires": 900},
                    {"token": "token-2", "expires": 900},
                    RuntimeError("mint failed"),
                    {"token": "token-3", "expires": 900},
                ],
            ),
        ):
            assert await c._refresh_realtime_ws_token() is True
            assert await c._refresh_realtime_ws_token() is True
            assert await c._refresh_realtime_ws_token(clear_on_failure=False) is False
            assert await c._refresh_realtime_ws_token(clear_on_failure=False) is False
            assert await c._refresh_realtime_ws_token() is True

        assert repository.list_active_wallet_credentials.await_count == 3
        wallet_public_ids = [
            call.kwargs["wallet_public_id"] for call in resolver.get_credentials.await_args_list
        ]
        assert wallet_public_ids == [
            "wallet-a",
            "wallet-a",
            "wallet-a",
            "wallet-b",
            "wallet-c",
        ]
        assert c._resolved_realtime_wallet_public_id == "wallet-c"
        assert c._ws_token == "token-3"

    @pytest.mark.asyncio
    async def test_realtime_autolookup_exception_clears_auth_state(self) -> None:
        """Realtime token refresh falls back when wallet autolookup raises."""
        c = KrakenEquitiesExchangeClient(repository=MagicMock(), realtime_ws_enabled=True)
        c._ws_token = "stale-token"
        c._ws_token_refresh_at = 10.0
        c._ws_token_expires_at = 20.0
        c._ws_auth_active = True
        with patch.object(
            c,
            "_resolve_realtime_wallet_public_id",
            new_callable=AsyncMock,
            side_effect=RuntimeError("repository unavailable"),
        ):
            assert await c._refresh_realtime_ws_token() is False

        assert c._ws_token is None
        assert c._ws_token_refresh_at == 0.0
        assert c._ws_token_expires_at == 0.0
        assert c._ws_auth_active is False

    @pytest.mark.asyncio
    async def test_realtime_autolookup_exception_preserves_auth_state(self) -> None:
        """Lazy realtime refresh preserves auth state when wallet autolookup raises."""
        c = KrakenEquitiesExchangeClient(repository=MagicMock(), realtime_ws_enabled=True)
        c._ws_token = "stale-token"
        c._ws_token_refresh_at = 10.0
        c._ws_token_expires_at = 20.0
        c._ws_auth_active = True
        with patch.object(
            c,
            "_resolve_realtime_wallet_public_id",
            new_callable=AsyncMock,
            side_effect=RuntimeError("repository unavailable"),
        ):
            assert await c._refresh_realtime_ws_token(clear_on_failure=False) is False

        assert c._ws_token == "stale-token"
        assert c._ws_token_refresh_at == 10.0
        assert c._ws_token_expires_at == 20.0
        assert c._ws_auth_active is True

    @pytest.mark.asyncio
    async def test_realtime_label_pin_resolves_single_live_wallet(self) -> None:
        """A ``label:`` pin resolves to the one live wallet and is cached.

        Given: Two wallets share the pinned label but only one is live,
        When: The realtime token is refreshed twice,
        Then: The live wallet's credentials mint both tokens and the wallet
            catalogue is queried only once (cached resolution).
        """
        repository = MagicMock()
        repository.list_active_wallets = AsyncMock(
            return_value=[
                _wallet_row("wallet-paper", label="market-data", is_paper=True),
                _wallet_row("wallet-live", label="market-data"),
                _wallet_row("wallet-other", label="trading"),
            ]
        )
        repository.list_active_wallet_credentials = AsyncMock(return_value=[])
        c = KrakenEquitiesExchangeClient(
            repository=repository,
            realtime_ws_enabled=True,
            realtime_wallet_public_id="label:market-data",
        )
        resolver = MagicMock()
        resolver.get_credentials = AsyncMock(
            return_value={"api_key": "api-key", "api_secret": "api-secret"}
        )
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities."
                "CredentialResolver",
                return_value=resolver,
            ),
            patch.object(
                c,
                "_dispatch_blocking",
                new_callable=AsyncMock,
                return_value={"token": "token-1", "expires": 900},
            ),
        ):
            assert await c._refresh_realtime_ws_token() is True
            assert await c._refresh_realtime_ws_token() is True

        repository.list_active_wallets.assert_awaited_once()
        as_of = repository.list_active_wallets.await_args.args[0]
        assert isinstance(as_of, _dt)
        assert as_of.tzinfo is _UTC
        repository.list_active_wallet_credentials.assert_not_awaited()
        wallet_public_ids = [
            call.kwargs["wallet_public_id"] for call in resolver.get_credentials.await_args_list
        ]
        assert wallet_public_ids == ["wallet-live", "wallet-live"]
        assert c._resolved_realtime_wallet_public_id == "wallet-live"
        assert c._ws_auth_active is True

    @pytest.mark.asyncio
    async def test_realtime_label_pin_ambiguous_fails_closed(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Two live wallets sharing the pinned label refuse to mint.

        Given: The pinned label matches two live wallets,
        When: The realtime token is refreshed,
        Then: No credential lookup happens and the client stays on the
            public delayed feed with an explicit ambiguity warning.
        """
        repository = MagicMock()
        repository.list_active_wallets = AsyncMock(
            return_value=[
                _wallet_row("wallet-a", label="market-data"),
                _wallet_row("wallet-b", label="market-data"),
            ]
        )
        repository.list_active_wallet_credentials = AsyncMock(return_value=[])
        c = KrakenEquitiesExchangeClient(
            repository=repository,
            realtime_ws_enabled=True,
            realtime_wallet_public_id="label:market-data",
        )
        sink_id = logger.add(caplog.handler, format="{message}", level="WARNING")
        try:
            assert await c._refresh_realtime_ws_token() is False
        finally:
            logger.remove(sink_id)

        assert "matched 2 live wallets" in caplog.text
        assert c._ws_auth_active is False
        assert c._resolved_realtime_wallet_public_id is None
        repository.list_active_wallet_credentials.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_realtime_label_pin_missing_fails_closed(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A pinned label matching only paper wallets refuses to mint.

        Given: The pinned label exists only on a paper wallet,
        When: The realtime token is refreshed,
        Then: The client stays on the public delayed feed and warns that
            zero live wallets matched.
        """
        repository = MagicMock()
        repository.list_active_wallets = AsyncMock(
            return_value=[_wallet_row("wallet-paper", label="market-data", is_paper=True)]
        )
        repository.list_active_wallet_credentials = AsyncMock(return_value=[])
        c = KrakenEquitiesExchangeClient(
            repository=repository,
            realtime_ws_enabled=True,
            realtime_wallet_public_id="label:market-data",
        )
        sink_id = logger.add(caplog.handler, format="{message}", level="WARNING")
        try:
            assert await c._refresh_realtime_ws_token() is False
        finally:
            logger.remove(sink_id)

        assert "matched 0 live wallets" in caplog.text
        assert c._ws_auth_active is False
        assert c._resolved_realtime_wallet_public_id is None

    @pytest.mark.asyncio
    async def test_realtime_label_pin_without_repository_fails_closed(self) -> None:
        """A ``label:`` pin cannot resolve without a repository handle.

        Given: The client has no repository,
        When: The realtime token is refreshed with a label pin,
        Then: The refresh fails closed onto the public delayed feed.
        """
        c = KrakenEquitiesExchangeClient(
            repository=None,
            realtime_ws_enabled=True,
            realtime_wallet_public_id="label:market-data",
        )
        assert await c._refresh_realtime_ws_token() is False
        assert c._ws_auth_active is False

    @pytest.mark.asyncio
    async def test_realtime_label_pin_mint_failure_clears_cached_resolution(self) -> None:
        """A failed mint drops the cached label resolution for a re-lookup.

        Given: A label pin resolved and cached a live wallet,
        When: The token mint raises and a later refresh succeeds,
        Then: The cached resolution is cleared on failure and the wallet
            catalogue is queried again on the retry.
        """
        repository = MagicMock()
        repository.list_active_wallets = AsyncMock(
            return_value=[_wallet_row("wallet-live", label="market-data")]
        )
        c = KrakenEquitiesExchangeClient(
            repository=repository,
            realtime_ws_enabled=True,
            realtime_wallet_public_id="label:market-data",
        )
        resolver = MagicMock()
        resolver.get_credentials = AsyncMock(
            return_value={"api_key": "api-key", "api_secret": "api-secret"}
        )
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities."
                "CredentialResolver",
                return_value=resolver,
            ),
            patch.object(
                c,
                "_dispatch_blocking",
                new_callable=AsyncMock,
                side_effect=RuntimeError("mint down"),
            ),
        ):
            assert await c._refresh_realtime_ws_token() is False

        assert c._resolved_realtime_wallet_public_id is None

        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities."
                "CredentialResolver",
                return_value=resolver,
            ),
            patch.object(
                c,
                "_dispatch_blocking",
                new_callable=AsyncMock,
                return_value={"token": "token-2", "expires": 900},
            ),
        ):
            assert await c._refresh_realtime_ws_token() is True

        assert repository.list_active_wallets.await_count == 2
        assert c._resolved_realtime_wallet_public_id == "wallet-live"
        assert c._ws_token == "token-2"

    @pytest.mark.asyncio
    async def test_realtime_label_pin_blank_label_fails_closed(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A blank ``label:`` pin refuses to mint even if a blank-label wallet exists.

        Given: The pin is ``label:`` with only whitespace and a live wallet
            with an empty label exists in the catalogue,
        When: The realtime token is refreshed,
        Then: The catalogue is never queried, auth state is cleared, and
            the client stays on the public delayed feed.
        """
        repository = MagicMock()
        repository.list_active_wallets = AsyncMock(
            return_value=[_wallet_row("wallet-blank", label="")]
        )
        c = KrakenEquitiesExchangeClient(
            repository=repository,
            realtime_ws_enabled=True,
            realtime_wallet_public_id="label:   ",
        )
        c._ws_token = "stale-token"
        c._ws_auth_active = True
        sink_id = logger.add(caplog.handler, format="{message}", level="WARNING")
        try:
            assert await c._refresh_realtime_ws_token() is False
        finally:
            logger.remove(sink_id)

        assert "blank label" in caplog.text
        repository.list_active_wallets.assert_not_awaited()
        assert c._ws_token is None
        assert c._ws_auth_active is False

    @pytest.mark.asyncio
    async def test_realtime_label_pin_hard_failure_clears_unexpired_token(self) -> None:
        """A hard label failure clears an unexpired token even on lazy refresh.

        Given: An active auth session with an unexpired token and a pin
            whose label now matches two live wallets,
        When: A lazy refresh runs with ``clear_on_failure=False``,
        Then: The stale token is cleared instead of being reused until TTL
            (hard pin failures fail closed immediately, unlike transient
            mint failures which preserve auth state).
        """
        repository = MagicMock()
        repository.list_active_wallets = AsyncMock(
            return_value=[
                _wallet_row("wallet-a", label="market-data"),
                _wallet_row("wallet-b", label="market-data"),
            ]
        )
        c = KrakenEquitiesExchangeClient(
            repository=repository,
            realtime_ws_enabled=True,
            realtime_wallet_public_id="label:market-data",
        )
        c._ws_token = "stale-token"
        c._ws_token_refresh_at = 10.0
        c._ws_token_expires_at = 10_000.0
        c._ws_auth_active = True

        assert await c._refresh_realtime_ws_token(clear_on_failure=False) is False

        assert c._ws_token is None
        assert c._ws_token_expires_at == 0.0
        assert c._ws_auth_active is False

    @pytest.mark.asyncio
    async def test_realtime_label_hard_failure_blocks_previous_token_reuse(self) -> None:
        """Subscribe decoration refuses the old token after a hard label failure.

        Given: An active auth socket holds an unexpired token whose refresh
            window has lapsed, and the pinned label now matches two live
            wallets,
        When: Subscribe params are decorated (lazy refresh path),
        Then: The refresh hard-fails, the captured previous token is NOT
            reused, and the auth-unavailable error is raised instead.
        """
        repository = MagicMock()
        repository.list_active_wallets = AsyncMock(
            return_value=[
                _wallet_row("wallet-a", label="market-data"),
                _wallet_row("wallet-b", label="market-data"),
            ]
        )
        c = KrakenEquitiesExchangeClient(
            repository=repository,
            realtime_ws_enabled=True,
            realtime_wallet_public_id="label:market-data",
        )
        c._ws_auth_active = True
        c._ws_token = "stale-token"
        c._ws_token_refresh_at = 0.0
        c._ws_token_expires_at = ke.monotonic() + 1000.0

        with pytest.raises(ke._RealtimeWsAuthUnavailableError):
            await c._decorate_ws_subscribe_params({"channel": "trade"})

        assert c._ws_token is None
        assert c._ws_auth_active is False

    def test_token_mint_routes_through_equities_public_egress_pool(self) -> None:
        """Realtime token mint uses the same public Equities egress route as WS.

        Given: The feed egress pool has a NY route pinned to ``kraken_equities``,
        When: A Kraken WebSockets token is minted,
        Then: The Spot SDK request runs with the NY SOCKS proxy scoped onto the
            SDK client and requests session, then restores both afterward.
        """
        configure_egress_pool(_equities_public_pool_config())
        c = KrakenEquitiesExchangeClient(realtime_ws_enabled=True)
        fake_spot_client = _FakeSpotTokenClient()
        session = getattr(fake_spot_client, "_SpotClient__session")
        assert isinstance(session, requests.Session)

        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.SpotClient",
            return_value=fake_spot_client,
        ) as spot_cls:
            payload = c._request_realtime_ws_token("api-key", "api-secret")

        assert payload == {"token": "token-1", "expires": 900}
        spot_cls.assert_called_once_with(key="api-key", secret="api-secret")
        assert fake_spot_client.seen_requests == [("POST", "/0/private/GetWebSocketsToken", 10)]
        assert fake_spot_client.seen_sdk_proxy == ["socks5h://snapper-egress-ny:1080"]
        assert fake_spot_client.seen_session_proxies == [
            {
                "http": "socks5h://snapper-egress-ny:1080",
                "https": "socks5h://snapper-egress-ny:1080",
            }
        ]
        assert getattr(fake_spot_client, "_SpotClient__proxy") is None
        assert session.proxies == {}

    def test_token_mint_remains_direct_without_egress_pool(self) -> None:
        """Realtime token mint remains direct when feed egress is absent."""
        c = KrakenEquitiesExchangeClient(realtime_ws_enabled=True)
        fake_spot_client = _FakeSpotTokenClient()
        session = getattr(fake_spot_client, "_SpotClient__session")
        assert isinstance(session, requests.Session)

        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.SpotClient",
            return_value=fake_spot_client,
        ):
            payload = c._request_realtime_ws_token("api-key", "api-secret")

        assert payload == {"token": "token-1", "expires": 900}
        assert fake_spot_client.seen_sdk_proxy == [None]
        assert fake_spot_client.seen_session_proxies == [{}]
        assert getattr(fake_spot_client, "_SpotClient__proxy") is None
        assert session.proxies == {}

    @pytest.mark.asyncio
    async def test_disconnect_shuts_down_rest_pool_after_token_mint(self) -> None:
        """Token mint uses the blocking REST pool that disconnect must close."""
        repository = MagicMock()
        c = KrakenEquitiesExchangeClient(
            repository=repository,
            realtime_ws_enabled=True,
            realtime_wallet_public_id="wallet-1",
        )
        resolver = MagicMock()
        resolver.get_credentials = AsyncMock(
            return_value={"api_key": "api-key", "api_secret": "api-secret"}
        )
        fake_spot_client = _FakeSpotTokenClient()
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities."
                "CredentialResolver",
                return_value=resolver,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.SpotClient",
                return_value=fake_spot_client,
            ),
        ):
            assert await c._refresh_realtime_ws_token() is True
        assert c._rest_pool is not None
        assert c._rest_pool_closed is False
        await c.disconnect()
        assert c._rest_pool is None
        assert c._rest_pool_closed is True
        await c.connect()
        assert c._rest_pool_closed is False

    def test_auth_ws_endpoint_patch_preserves_egress_proxy_injection(self) -> None:
        """Auth URL patching still leaves WS handshakes on the egress shim path.

        Given: Auth mode corrects the SDK connector endpoint to the exact
            ``?f`` URL,
        And: The publisher context tags traffic as ``kraken_equities``,
        When: The patched connect shim receives that endpoint,
        Then: It reserves the public Equities route and injects the NY SOCKS
            proxy exactly as the public feed path does.
        """
        configure_egress_pool(_equities_public_pool_config())
        c = KrakenEquitiesExchangeClient(realtime_ws_enabled=True)
        sdk_client = MagicMock()
        connector = MagicMock()
        sdk_client._pub_conn = connector
        c._force_sdk_public_endpoint(sdk_client, ke._WS_AUTH_URL)
        endpoint = getattr(connector, "_ConnectSpotWebsocketBase__ws_endpoint")
        seen_kwargs: dict[str, object] = {}

        def fake_connect(*args: object, **kwargs: object) -> MagicMock:
            seen_kwargs.update(kwargs)
            return MagicMock()

        publisher = _PublisherDouble()
        token = _CURRENT_PUBLISHER.set(publisher)
        try:
            shim_cls = _wrap_connect_factory(fake_connect)
            shim_cls(endpoint)
        finally:
            _CURRENT_PUBLISHER.reset(token)

        assert endpoint == ke._WS_AUTH_URL
        assert seen_kwargs == {
            "proxy": "socks5h://snapper-egress-ny:1080",
            "ping_interval": _WS_PING_INTERVAL_S,
            "ping_timeout": _WS_PING_TIMEOUT_S,
            "close_timeout": _WS_CLOSE_TIMEOUT_S,
        }

    @pytest.mark.asyncio
    async def test_realtime_missing_wallet_setting_falls_back_to_public(self) -> None:
        """An enabled feed without an auto credential fails open to public WS."""
        repository = MagicMock()
        repository.list_active_wallet_credentials = AsyncMock(
            return_value=[
                _wallet_credential_row(
                    "wallet-paper",
                    credential_type="paper",
                ),
                _wallet_credential_row(
                    "wallet-futures",
                    exchange="kraken_futures",
                ),
            ]
        )
        c = KrakenEquitiesExchangeClient(
            repository=repository,
            realtime_ws_enabled=True,
            realtime_wallet_public_id="",
        )
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities."
                "CredentialResolver"
            ) as resolver_cls,
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.SpotWSClient"
            ) as ws_cls,
        ):
            ws_cls.return_value.start = AsyncMock()
            await c._ensure_ws_connected()
        repository.list_active_wallet_credentials.assert_awaited_once()
        resolver_cls.assert_not_called()
        assert ws_cls.call_args.kwargs["ws_url"] == ke._WS_URL
        assert c._ws_auth_active is False

    @pytest.mark.asyncio
    async def test_realtime_missing_repository_falls_back_to_public(self) -> None:
        """An enabled feed without repository cannot resolve credentials."""
        c = KrakenEquitiesExchangeClient(
            repository=None,
            realtime_ws_enabled=True,
            realtime_wallet_public_id="wallet-1",
        )
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities."
                "CredentialResolver"
            ) as resolver_cls,
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.SpotWSClient"
            ) as ws_cls,
        ):
            ws_cls.return_value.start = AsyncMock()
            await c._ensure_ws_connected()
        resolver_cls.assert_not_called()
        assert ws_cls.call_args.kwargs["ws_url"] == ke._WS_URL
        assert c._ws_auth_active is False

    @pytest.mark.asyncio
    async def test_realtime_missing_credential_row_falls_back_to_public(self) -> None:
        """A missing wallet credential row fails open to public WS."""
        repository = MagicMock()
        c = KrakenEquitiesExchangeClient(
            repository=repository,
            realtime_ws_enabled=True,
            realtime_wallet_public_id="wallet-1",
        )
        resolver = MagicMock()
        resolver.get_credentials = AsyncMock(
            side_effect=CredentialNotFoundError(exchange="kraken", wallet_public_id="wallet-1")
        )
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities."
                "CredentialResolver",
                return_value=resolver,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.SpotWSClient"
            ) as ws_cls,
        ):
            ws_cls.return_value.start = AsyncMock()
            await c._ensure_ws_connected()
        assert ws_cls.call_args.kwargs["ws_url"] == ke._WS_URL
        assert c._ws_auth_active is False

    @pytest.mark.asyncio
    async def test_realtime_mint_permission_failure_falls_back_to_public(self) -> None:
        """A token mint failure fails open to public delayed WS."""
        repository = MagicMock()
        c = KrakenEquitiesExchangeClient(
            repository=repository,
            realtime_ws_enabled=True,
            realtime_wallet_public_id="wallet-1",
        )
        resolver = MagicMock()
        resolver.get_credentials = AsyncMock(
            return_value={"api_key": "api-key", "api_secret": "api-secret"}
        )
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities."
                "CredentialResolver",
                return_value=resolver,
            ),
            patch.object(
                c,
                "_dispatch_blocking",
                new_callable=AsyncMock,
                side_effect=RuntimeError("permission denied"),
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.SpotWSClient"
            ) as ws_cls,
        ):
            ws_cls.return_value.start = AsyncMock()
            await c._ensure_ws_connected()
        assert ws_cls.call_args.kwargs["ws_url"] == ke._WS_URL
        assert c._ws_auth_active is False

    @pytest.mark.asyncio
    async def test_refresh_failure_without_clear_preserves_auth_state(self) -> None:
        """Lazy refresh failures do not clear auth before callers demote."""
        missing_wallet = KrakenEquitiesExchangeClient(realtime_ws_enabled=True)
        missing_wallet._ws_auth_active = True
        assert await missing_wallet._refresh_realtime_ws_token(clear_on_failure=False) is False
        assert missing_wallet._ws_auth_active is True

        missing_repository = KrakenEquitiesExchangeClient(
            realtime_ws_enabled=True,
            realtime_wallet_public_id="wallet-1",
        )
        missing_repository._ws_auth_active = True
        assert await missing_repository._refresh_realtime_ws_token(clear_on_failure=False) is False
        assert missing_repository._ws_auth_active is True

        repository = MagicMock()
        mint_failure = KrakenEquitiesExchangeClient(
            repository=repository,
            realtime_ws_enabled=True,
            realtime_wallet_public_id="wallet-1",
        )
        mint_failure._ws_auth_active = True
        resolver = MagicMock()
        resolver.get_credentials = AsyncMock(
            return_value={"api_key": "api-key", "api_secret": "api-secret"}
        )
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities."
                "CredentialResolver",
                return_value=resolver,
            ),
            patch.object(
                mint_failure,
                "_dispatch_blocking",
                new_callable=AsyncMock,
                side_effect=RuntimeError("mint failed"),
            ),
        ):
            assert await mint_failure._refresh_realtime_ws_token(clear_on_failure=False) is False
        assert mint_failure._ws_auth_active is True

    @pytest.mark.asyncio
    async def test_auth_start_failure_falls_back_to_public_feed(self) -> None:
        """Auth startup failure degrades to the public delayed feed.

        Given: Token minting succeeded but the auth WebSocket cannot start,
        When: _ensure_ws_connected runs,
        Then: The auth client is closed and a fresh public client is started.
        """
        c = KrakenEquitiesExchangeClient(realtime_ws_enabled=True)
        c._ws_auth_active = True
        auth_ws = AsyncMock()
        auth_ws.start.side_effect = RuntimeError("auth down")
        public_ws = AsyncMock()
        c._prepare_realtime_ws_auth = AsyncMock(return_value=True)
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.SpotWSClient",
            side_effect=[auth_ws, public_ws],
        ) as ws_cls:
            await c._ensure_ws_connected()
        assert [call.kwargs["ws_url"] for call in ws_cls.call_args_list] == [
            ke._WS_AUTH_URL,
            ke._WS_URL,
        ]
        auth_ws.close.assert_awaited_once()
        public_ws.start.assert_awaited_once()
        assert c._ws_client is public_ws
        assert c._ws_auth_active is False

    @pytest.mark.asyncio
    async def test_auth_replay_failure_falls_back_to_public_feed(self) -> None:
        """Auth replay failure degrades to the public delayed feed.

        Given: Auth connect succeeds but tokenized subscription replay fails,
        When: _ensure_ws_connected runs,
        Then: The auth client is closed and public replay is attempted.
        """
        c = KrakenEquitiesExchangeClient(realtime_ws_enabled=True)
        c._ws_auth_active = True
        req = SubscriptionRequest(channel="ticker", symbols=("CLM6.NYMEX",), parameters_json="{}")
        c._subscription_cache[req.key()] = req
        auth_ws = AsyncMock()
        public_ws = AsyncMock()
        c._prepare_realtime_ws_auth = AsyncMock(return_value=True)
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.SpotWSClient",
                side_effect=[auth_ws, public_ws],
            ) as ws_cls,
            patch.object(
                c,
                "_replay_subscriptions",
                new_callable=AsyncMock,
                side_effect=[RuntimeError("auth replay failed"), None],
            ) as replay,
        ):
            await c._ensure_ws_connected()
        assert [call.kwargs["ws_url"] for call in ws_cls.call_args_list] == [
            ke._WS_AUTH_URL,
            ke._WS_URL,
        ]
        auth_ws.close.assert_awaited_once()
        public_ws.start.assert_awaited_once()
        assert replay.await_count == 2
        assert c._ws_client is public_ws
        assert c._ws_auth_active is False

    @pytest.mark.asyncio
    async def test_auth_sdk_patch_failure_falls_back_before_auth_start(self) -> None:
        """SDK private patch failure prevents connecting to a malformed auth URL.

        Given: Token minting succeeded but the SDK endpoint patch fails,
        When: _ensure_ws_connected runs,
        Then: The unstarted auth client is closed and public WS is used.
        """
        c = KrakenEquitiesExchangeClient(realtime_ws_enabled=True)
        c._ws_auth_active = True
        auth_ws = AsyncMock()
        public_ws = AsyncMock()
        c._prepare_realtime_ws_auth = AsyncMock(return_value=True)
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.SpotWSClient",
                side_effect=[auth_ws, public_ws],
            ) as ws_cls,
            patch.object(c, "_force_sdk_public_endpoint", return_value=False),
        ):
            await c._ensure_ws_connected()
        assert [call.kwargs["ws_url"] for call in ws_cls.call_args_list] == [
            ke._WS_AUTH_URL,
            ke._WS_URL,
        ]
        auth_ws.start.assert_not_awaited()
        auth_ws.close.assert_awaited_once()
        public_ws.start.assert_awaited_once()
        assert c._ws_client is public_ws
        assert c._ws_auth_active is False

    @pytest.mark.parametrize(
        "payload",
        [
            [],
            {"token": "", "expires": 900},
            {"token": "token-1", "expires": "900"},
            {"token": "token-1", "expires": True},
            {"token": "token-1", "expires": 0},
            {"expires": 900},
            {"token": "token-1"},
        ],
    )
    def test_parse_ws_token_response_rejects_invalid_payload(self, payload: object) -> None:
        """Token payload validation rejects malformed SDK responses."""
        c = KrakenEquitiesExchangeClient()
        with pytest.raises(RuntimeError):
            c._parse_realtime_ws_token_response(payload)

    @pytest.mark.asyncio
    async def test_concurrent_decorate_params_single_flights_token_refresh(self) -> None:
        """Concurrent subscribe decoration refreshes an expired token once."""
        c = KrakenEquitiesExchangeClient()
        c._ws_auth_active = True
        c._ws_token = None
        c._ws_token_refresh_at = 0.0
        c._ws_token_expires_at = 0.0
        refresh_calls = 0

        async def _refresh(*, clear_on_failure: bool = True) -> bool:
            nonlocal refresh_calls
            assert clear_on_failure is False
            refresh_calls += 1
            await asyncio.sleep(0)
            c._ws_token = "fresh-token"
            c._ws_token_refresh_at = ke.monotonic() + 100.0
            c._ws_token_expires_at = ke.monotonic() + 200.0
            return True

        with patch.object(c, "_refresh_realtime_ws_token", side_effect=_refresh):
            first, second = await asyncio.gather(
                c._decorate_ws_subscribe_params({"channel": "ticker"}),
                c._decorate_ws_subscribe_params({"channel": "trade"}),
            )

        assert refresh_calls == 1
        assert first["token"] == "fresh-token"
        assert second["token"] == "fresh-token"

    @pytest.mark.asyncio
    async def test_decorate_params_keeps_valid_token_after_refresh_failure(self) -> None:
        """A transient refresh failure keeps using an unexpired token."""
        c = KrakenEquitiesExchangeClient()
        c._ws_auth_active = True
        c._ws_token = "old-token"
        c._ws_token_refresh_at = 0.0
        c._ws_token_expires_at = ke.monotonic() + 100.0
        params: dict[str, JsonValue] = {"channel": "ticker"}
        with patch.object(
            c,
            "_refresh_realtime_ws_token",
            new_callable=AsyncMock,
            return_value=False,
        ) as refresh:
            decorated = await c._decorate_ws_subscribe_params(params)
        refresh.assert_awaited_once_with(clear_on_failure=False)
        assert decorated["token"] == "old-token"
        assert c._ws_auth_active is True

    @pytest.mark.asyncio
    async def test_expired_token_refresh_failure_rebuilds_public_before_subscribe(
        self,
    ) -> None:
        """Expired auth token never sends an untokened subscribe to auth WS.

        Given: The auth socket is active but no valid token remains,
        When: A retry subscribe needs a token and refresh fails,
        Then: The auth client is closed, public WS is started, and the
            outbound subscribe is sent without token on the public client.
        """
        c = KrakenEquitiesExchangeClient()
        c._ws_auth_active = True
        c._ws_token = "expired-token"
        c._ws_token_refresh_at = 0.0
        c._ws_token_expires_at = 0.0
        auth_ws = AsyncMock()
        public_ws = AsyncMock()
        c._ws_client = auth_ws
        with (
            patch.object(
                c,
                "_refresh_realtime_ws_token",
                new_callable=AsyncMock,
                return_value=False,
            ) as refresh,
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.SpotWSClient",
                return_value=public_ws,
            ) as ws_cls,
        ):
            await c._retry_subscribe("ticker", "CLM6.NYMEX")

        refresh.assert_awaited_once_with(clear_on_failure=False)
        auth_ws.subscribe.assert_not_awaited()
        auth_ws.close.assert_awaited_once()
        public_ws.start.assert_awaited_once()
        public_ws.subscribe.assert_awaited_once()
        assert ws_cls.call_args.kwargs["ws_url"] == ke._WS_URL
        assert "token" not in public_ws.subscribe.await_args.kwargs["params"]
        assert c._ws_client is public_ws
        assert c._ws_auth_active is False

    @pytest.mark.asyncio
    async def test_successful_refresh_without_token_raises_auth_unavailable(self) -> None:
        """A malformed refresh success without token does not send a subscribe."""
        c = KrakenEquitiesExchangeClient()
        c._ws_auth_active = True
        c._ws_token = None
        c._ws_token_refresh_at = 0.0
        c._ws_token_expires_at = 0.0
        with (
            patch.object(
                c,
                "_refresh_realtime_ws_token",
                new_callable=AsyncMock,
                return_value=True,
            ),
            pytest.raises(ke._RealtimeWsAuthUnavailableError),
        ):
            await c._decorate_ws_subscribe_params({"channel": "ticker"})

    @pytest.mark.asyncio
    async def test_send_ws_subscribe_requires_connected_client(self) -> None:
        """Subscribe helper still fails loudly without any WS client."""
        c = KrakenEquitiesExchangeClient()
        with pytest.raises(RuntimeError, match="WebSocket client not connected"):
            await c._send_ws_subscribe({"channel": "ticker"}, allow_auth_demote=True)

    @pytest.mark.asyncio
    async def test_send_ws_subscribe_hard_label_failure_demotes_and_retries_public(self) -> None:
        """A hard label failure during subscribe force-demotes the auth socket.

        Given: An auth client with an unexpired stale token, a lapsed
            refresh window, and a pinned label matching two live wallets
            (the resolver clears auth state during decoration),
        When: A subscribe runs with auth demotion allowed,
        Then: The auth client is force-closed despite the cleared auth
            state, a public client is installed at the delayed URL, and the
            retry goes out untokened on the public client.
        """
        repository = MagicMock()
        repository.list_active_wallets = AsyncMock(
            return_value=[
                _wallet_row("wallet-a", label="market-data"),
                _wallet_row("wallet-b", label="market-data"),
            ]
        )
        c = KrakenEquitiesExchangeClient(
            repository=repository,
            realtime_ws_enabled=True,
            realtime_wallet_public_id="label:market-data",
        )
        auth_client = AsyncMock()
        c._ws_client = auth_client
        c._ws_auth_active = True
        c._ws_token = "stale-token"
        c._ws_token_refresh_at = 0.0
        c._ws_token_expires_at = ke.monotonic() + 1000.0
        public_ws = AsyncMock()
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.SpotWSClient",
            return_value=public_ws,
        ) as ws_cls:
            await c._send_ws_subscribe(
                {"channel": "ticker", "symbol": ["CLM6.NYMEX"]},
                allow_auth_demote=True,
            )

        public_ws.start.assert_awaited_once()
        assert ws_cls.call_args.kwargs["ws_url"] == ke._WS_URL
        assert c._ws_client is public_ws
        assert c._ws_auth_active is False
        public_ws.subscribe.assert_awaited_once()
        sent_params = public_ws.subscribe.await_args.kwargs["params"]
        assert "token" not in sent_params

    @pytest.mark.asyncio
    async def test_send_ws_subscribe_propagates_auth_unavailable_when_demote_disallowed(
        self,
    ) -> None:
        """Replay send path propagates auth exhaustion to the caller."""
        c = KrakenEquitiesExchangeClient()
        c._ws_client = AsyncMock()
        with (
            patch.object(
                c,
                "_decorate_ws_subscribe_params",
                new_callable=AsyncMock,
                side_effect=ke._RealtimeWsAuthUnavailableError("expired"),
            ),
            pytest.raises(ke._RealtimeWsAuthUnavailableError),
        ):
            await c._send_ws_subscribe({"channel": "ticker"}, allow_auth_demote=False)

    @pytest.mark.asyncio
    async def test_send_ws_subscribe_requires_public_client_after_demote(self) -> None:
        """Subscribe helper fails if demotion cannot install a public client."""
        c = KrakenEquitiesExchangeClient()
        c._ws_client = AsyncMock()

        async def _demote(_: str, *, force: bool = False) -> None:
            assert force is True
            c._ws_client = None

        with (
            patch.object(
                c,
                "_decorate_ws_subscribe_params",
                new_callable=AsyncMock,
                side_effect=ke._RealtimeWsAuthUnavailableError("expired"),
            ),
            patch.object(c, "_demote_realtime_ws_to_public", side_effect=_demote),
            pytest.raises(RuntimeError, match="WebSocket client not connected"),
        ):
            await c._send_ws_subscribe({"channel": "ticker"}, allow_auth_demote=True)

    @pytest.mark.asyncio
    async def test_send_ws_subscribe_aborts_when_client_changes_during_decoration(self) -> None:
        """Subscribe helper never sends on a client replaced during token decoration."""
        c = KrakenEquitiesExchangeClient()
        stale_ws = AsyncMock()
        replacement_ws = AsyncMock()
        c._ws_client = stale_ws

        async def _decorate(params: dict[str, JsonValue]) -> dict[str, JsonValue]:
            c._ws_client = replacement_ws
            await asyncio.sleep(0)
            return dict(params)

        with (
            patch.object(c, "_decorate_ws_subscribe_params", side_effect=_decorate),
            pytest.raises(RuntimeError, match=ke._REPLAY_CLIENT_REPLACED_MSG),
        ):
            await c._send_ws_subscribe({"channel": "ticker"}, allow_auth_demote=True)

        stale_ws.subscribe.assert_not_awaited()
        replacement_ws.subscribe.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_send_ws_subscribe_does_not_demote_replacement_after_refresh_failure(
        self,
    ) -> None:
        """Stale auth-refresh failure cannot close a replacement client."""
        c = KrakenEquitiesExchangeClient()
        stale_ws = AsyncMock()
        replacement_ws = AsyncMock()
        c._ws_client = stale_ws

        async def _decorate(_: dict[str, JsonValue]) -> dict[str, JsonValue]:
            c._ws_client = replacement_ws
            raise ke._RealtimeWsAuthUnavailableError("expired")

        with (
            patch.object(c, "_decorate_ws_subscribe_params", side_effect=_decorate),
            patch.object(c, "_demote_realtime_ws_to_public", new_callable=AsyncMock) as demote,
            pytest.raises(RuntimeError, match=ke._REPLAY_CLIENT_REPLACED_MSG),
        ):
            await c._send_ws_subscribe({"channel": "ticker"}, allow_auth_demote=True)

        demote.assert_not_awaited()
        stale_ws.subscribe.assert_not_awaited()
        replacement_ws.subscribe.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_pending_public_demotion_never_sends_token_free_on_auth_client(self) -> None:
        """Scheduled demotion keeps auth endpoint sends token-consistent."""
        c = KrakenEquitiesExchangeClient()
        c._ws_auth_active = True
        c._ws_token = "auth-token"
        c._ws_token_refresh_at = ke.monotonic() + 100.0
        c._ws_token_expires_at = ke.monotonic() + 200.0
        auth_ws = AsyncMock()
        public_ws = AsyncMock()
        c._ws_client = auth_ws
        close_started = asyncio.Event()
        release_close = asyncio.Event()
        send_task: asyncio.Task[None] | None = None

        async def _close(client: object) -> None:
            assert client is auth_ws
            close_started.set()
            await release_close.wait()
            if c._ws_client is client:
                c._ws_client = None

        with (
            patch.object(c, "_close_ws_client", side_effect=_close),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.SpotWSClient",
                return_value=public_ws,
            ),
        ):
            c._schedule_realtime_ws_public_demotion("auth rejected")
            try:
                await asyncio.wait_for(close_started.wait(), timeout=1.0)
                assert c._ws_client is auth_ws
                assert c._ws_auth_active is True

                send_task = asyncio.create_task(
                    c._send_ws_subscribe(
                        {"channel": "ticker", "symbol": ["CLM6.NYMEX"]},
                        allow_auth_demote=True,
                    )
                )
                await asyncio.sleep(0)

                if auth_ws.subscribe.await_count:
                    auth_params = auth_ws.subscribe.await_args.kwargs["params"]
                    assert auth_params["token"] == "auth-token"
                else:
                    assert send_task.done() is False
            finally:
                release_close.set()
                if send_task is not None:
                    with contextlib.suppress(Exception):
                        await asyncio.wait_for(send_task, timeout=1.0)
                task = c._ws_public_demotion_task
                if task is not None:
                    with contextlib.suppress(Exception):
                        await asyncio.wait_for(task, timeout=1.0)

        if auth_ws.subscribe.await_count:
            auth_params = auth_ws.subscribe.await_args.kwargs["params"]
            assert auth_params["token"] == "auth-token"
        else:
            public_ws.subscribe.assert_awaited_once()
            assert "token" not in public_ws.subscribe.await_args.kwargs["params"]
        assert c._ws_client is public_ws
        assert c._ws_auth_active is False

    @pytest.mark.asyncio
    async def test_ticker_subscribe_and_cleanup_inject_token_without_caching_it(self) -> None:
        """Ticker subscribe sends tokenized params but caches stable params.

        Given: The authenticated feed is active,
        When: A ticker subscription starts and closes,
        Then: Both outbound SDK calls carry a token and the replay cache does not.
        """
        c = KrakenEquitiesExchangeClient()
        c._ws_auth_active = True
        c._ws_token = "token-1"
        c._ws_token_refresh_at = ke.monotonic() + 100.0
        c._ws_token_expires_at = ke.monotonic() + 200.0
        ws = AsyncMock()
        c._ws_client = ws
        c._ensure_ws_connected = AsyncMock()
        await c._tick_queue.put(
            TickerUpdate(
                symbol="CLM6-NYMEX",
                bid=90.0,
                bid_qty=1.0,
                ask=90.1,
                ask_qty=1.0,
                last=90.05,
                volume=10.0,
                vwap=90.0,
                low=89.0,
                high=91.0,
                change=0.1,
                change_pct=0.1,
            )
        )
        gen = c.subscribe_ticks(["CLM6-NYMEX"])
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities."
            "native_to_kraken_equities_ws",
            return_value="CLM6.NYMEX",
        ):
            await anext(gen)
            await gen.aclose()
        assert ws.subscribe.await_count == 2
        for call_args in ws.subscribe.await_args_list:
            assert call_args.kwargs["params"]["token"] == "token-1"
        req = next(iter(c._subscription_cache.values()))
        assert "token" not in req.parameters_json

    @pytest.mark.asyncio
    async def test_trade_subscribe_and_cleanup_inject_token_without_caching_it(self) -> None:
        """Trade subscribe sends tokenized params but caches stable params."""
        c = KrakenEquitiesExchangeClient()
        c._ws_auth_active = True
        c._ws_token = "token-1"
        c._ws_token_refresh_at = ke.monotonic() + 100.0
        c._ws_token_expires_at = ke.monotonic() + 200.0
        ws = AsyncMock()
        c._ws_client = ws
        c._ensure_ws_connected = AsyncMock()
        await c._trade_queue.put(
            TradeUpdate(
                symbol="CLM6-NYMEX",
                price=90.0,
                quantity=1.0,
                side="buy",
                ord_type="fill",
                trade_id="trade-1",
                timestamp=_dt.now(_UTC),
            )
        )
        gen = c.subscribe_trades(["CLM6-NYMEX"])
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities."
            "native_to_kraken_equities_ws",
            return_value="CLM6.NYMEX",
        ):
            await anext(gen)
            await gen.aclose()
        assert ws.subscribe.await_count == 2
        for call_args in ws.subscribe.await_args_list:
            assert call_args.kwargs["params"]["token"] == "token-1"
        req = next(iter(c._subscription_cache.values()))
        assert "token" not in req.parameters_json

    @pytest.mark.asyncio
    async def test_ticker_initial_replaced_client_error_is_supervisor_signal(self) -> None:
        """Ticker initial send intentionally propagates replaced-client restart signal."""
        c = KrakenEquitiesExchangeClient()
        c._ws_client = AsyncMock()
        c._ensure_ws_connected = AsyncMock()
        gen = c.subscribe_ticks(["CLM6-NYMEX"])

        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities."
                "native_to_kraken_equities_ws",
                return_value="CLM6.NYMEX",
            ),
            patch.object(
                c,
                "_send_ws_subscribe",
                new_callable=AsyncMock,
                side_effect=RuntimeError(ke._REPLAY_CLIENT_REPLACED_MSG),
            ),
            pytest.raises(RuntimeError, match=ke._REPLAY_CLIENT_REPLACED_MSG),
        ):
            await anext(gen)

    @pytest.mark.asyncio
    async def test_trade_initial_replaced_client_error_is_supervisor_signal(self) -> None:
        """Trade initial send intentionally propagates replaced-client restart signal."""
        c = KrakenEquitiesExchangeClient()
        c._ws_client = AsyncMock()
        c._ensure_ws_connected = AsyncMock()
        gen = c.subscribe_trades(["CLM6-NYMEX"])

        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities."
                "native_to_kraken_equities_ws",
                return_value="CLM6.NYMEX",
            ),
            patch.object(
                c,
                "_send_ws_subscribe",
                new_callable=AsyncMock,
                side_effect=RuntimeError(ke._REPLAY_CLIENT_REPLACED_MSG),
            ),
            pytest.raises(RuntimeError, match=ke._REPLAY_CLIENT_REPLACED_MSG),
        ):
            await anext(gen)

    @pytest.mark.asyncio
    async def test_retry_subscribe_injects_token(self) -> None:
        """Health-loop single-symbol retries use the token decorator."""
        c = KrakenEquitiesExchangeClient()
        c._ws_auth_active = True
        c._ws_token = "token-1"
        c._ws_token_refresh_at = ke.monotonic() + 100.0
        c._ws_token_expires_at = ke.monotonic() + 200.0
        ws = AsyncMock()
        c._ws_client = ws
        await c._retry_subscribe("ticker", "CLM6.NYMEX")
        assert ws.subscribe.await_args.kwargs["params"]["token"] == "token-1"

    @pytest.mark.asyncio
    async def test_retry_subscribe_requires_connected_client(self) -> None:
        """Health-loop retry fails loudly without a connected SDK client."""
        c = KrakenEquitiesExchangeClient()
        with pytest.raises(RuntimeError, match="WebSocket client not connected"):
            await c._retry_subscribe("ticker", "CLM6.NYMEX")

    @pytest.mark.asyncio
    async def test_retry_subscribe_rejects_unknown_channel(self) -> None:
        """Health-loop retry rejects unsupported tracker channels."""
        c = KrakenEquitiesExchangeClient()
        c._ws_client = AsyncMock()
        with pytest.raises(ValueError, match="Unsupported subscription health channel"):
            await c._retry_subscribe("book", "CLM6.NYMEX")

    @pytest.mark.asyncio
    async def test_replay_refreshes_near_expiry_token(self) -> None:
        """Reconnect replay decorates cached params with a freshly minted token.

        Given: A cached subscription and an auth token inside its refresh window,
        When: Snapper-owned replay runs,
        Then: The outgoing params carry the refreshed token while the cache stays clean.
        """
        c = KrakenEquitiesExchangeClient()
        c._ws_auth_active = True
        c._ws_token = "old-token"
        c._ws_token_refresh_at = 0.0
        c._ws_token_expires_at = ke.monotonic() + 10.0
        ws = AsyncMock()
        c._ws_client = ws
        req = SubscriptionRequest(
            channel="ticker",
            symbols=("CLM6.NYMEX",),
            parameters_json='{"asset_class": "futures_contract", "snapshot": true, "throttle": 5000}',
        )
        c._subscription_cache[req.key()] = req

        async def _refresh(*, clear_on_failure: bool = True) -> bool:
            assert clear_on_failure is False
            c._ws_token = "fresh-token"
            c._ws_token_refresh_at = ke.monotonic() + 100.0
            c._ws_token_expires_at = ke.monotonic() + 200.0
            return True

        with patch.object(c, "_refresh_realtime_ws_token", side_effect=_refresh) as refresh:
            await c._replay_subscriptions()
        refresh.assert_awaited_once()
        assert ws.subscribe.await_args.kwargs["params"]["token"] == "fresh-token"
        assert "token" not in req.parameters_json

    @pytest.mark.asyncio
    async def test_sdk_reconnect_replay_refreshes_token_and_resubscribes_once(self) -> None:
        """Auth SDK reconnect replay re-mints and restores Snapper subscriptions.

        Given: The SDK internally reconnects an auth Equities socket while
            Snapper's token-free cache has one ticker subscription,
        When: The patched SDK recover hook runs,
        Then: It mints a fresh token immediately and sends exactly one
            tokenized subscribe on the current SDK client.
        """
        c = KrakenEquitiesExchangeClient()
        sdk_client = MagicMock()
        connector = MagicMock()
        sdk_client._pub_conn = connector
        sdk_client.subscribe = AsyncMock()
        c._ws_client = sdk_client
        c._ws_auth_active = True
        c._ws_token = "stale-token"
        c._ws_token_refresh_at = ke.monotonic() + 300.0
        c._ws_token_expires_at = ke.monotonic() + 600.0
        req = SubscriptionRequest(
            channel="ticker",
            symbols=("CLM6.NYMEX",),
            parameters_json='{"asset_class": "futures_contract", "snapshot": true, "throttle": 5000}',
        )
        c._subscription_cache[req.key()] = req

        async def _refresh(*, clear_on_failure: bool = True) -> bool:
            assert clear_on_failure is False
            c._ws_token = "fresh-token"
            c._ws_token_refresh_at = ke.monotonic() + 300.0
            c._ws_token_expires_at = ke.monotonic() + 600.0
            return True

        assert c._disable_sdk_reconnect_replay_for_auth(sdk_client) is True
        event = asyncio.Event()
        event.set()
        with patch.object(c, "_refresh_realtime_ws_token", side_effect=_refresh) as refresh:
            await connector._recover_subscriptions(event)
        refresh.assert_awaited_once_with(clear_on_failure=False)
        sdk_client.subscribe.assert_awaited_once()
        subscribe_args = sdk_client.subscribe.await_args
        assert subscribe_args is not None
        params = subscribe_args.kwargs["params"]
        assert params["token"] == "fresh-token"
        assert params["symbol"] == ["CLM6.NYMEX"]
        assert "stale-token" not in str(params)
        assert "token" not in req.parameters_json

    @pytest.mark.asyncio
    async def test_sdk_reconnect_replay_skips_when_client_slot_was_swapped(self) -> None:
        """Auth SDK reconnect replay never sends through a disowned client."""
        c = KrakenEquitiesExchangeClient()
        sdk_client = MagicMock()
        connector = MagicMock()
        sdk_client._pub_conn = connector
        sdk_client.subscribe = AsyncMock()
        c._ws_client = AsyncMock()
        c._ws_auth_active = True
        req = SubscriptionRequest(channel="ticker", symbols=("CLM6.NYMEX",), parameters_json="{}")
        c._subscription_cache[req.key()] = req
        assert c._disable_sdk_reconnect_replay_for_auth(sdk_client) is True
        event = asyncio.Event()
        event.set()
        with patch.object(
            c,
            "_refresh_realtime_ws_token",
            new_callable=AsyncMock,
            return_value=True,
        ) as refresh:
            await connector._recover_subscriptions(event)
        refresh.assert_not_awaited()
        sdk_client.subscribe.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_sdk_reconnect_replay_demotes_when_fresh_token_unavailable(self) -> None:
        """Auth SDK reconnect replay falls back to public when token refresh fails."""
        c = KrakenEquitiesExchangeClient()
        sdk_client = MagicMock()
        connector = MagicMock()
        sdk_client._pub_conn = connector
        sdk_client.subscribe = AsyncMock()
        sdk_client.close = AsyncMock()
        c._ws_client = sdk_client
        c._ws_auth_active = True
        req = SubscriptionRequest(channel="ticker", symbols=("CLM6.NYMEX",), parameters_json="{}")
        c._subscription_cache[req.key()] = req
        public_ws = AsyncMock()
        assert c._disable_sdk_reconnect_replay_for_auth(sdk_client) is True
        event = asyncio.Event()
        event.set()
        with (
            patch.object(
                c,
                "_refresh_realtime_ws_token",
                new_callable=AsyncMock,
                return_value=False,
            ) as refresh,
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.SpotWSClient",
                return_value=public_ws,
            ) as ws_cls,
        ):
            await connector._recover_subscriptions(event)
            await _await_public_demotion(c)
        refresh.assert_awaited_once_with(clear_on_failure=False)
        sdk_client.subscribe.assert_not_awaited()
        sdk_client.close.assert_awaited_once()
        public_ws.start.assert_awaited_once()
        assert ws_cls.call_args.kwargs["ws_url"] == ke._WS_URL
        assert c._ws_client is public_ws
        assert c._ws_auth_active is False

    @pytest.mark.asyncio
    async def test_sdk_reconnect_replay_skips_demote_when_refresh_failure_swaps_client(
        self,
    ) -> None:
        """SDK reconnect refresh failure does not demote a replaced client."""
        c = KrakenEquitiesExchangeClient()
        sdk_client = MagicMock()
        connector = MagicMock()
        sdk_client._pub_conn = connector
        sdk_client.subscribe = AsyncMock()
        c._ws_client = sdk_client
        c._ws_auth_active = True
        replacement_ws = AsyncMock()
        assert c._disable_sdk_reconnect_replay_for_auth(sdk_client) is True
        event = asyncio.Event()
        event.set()

        async def _refresh(*, clear_on_failure: bool = True) -> bool:
            assert clear_on_failure is False
            c._ws_client = replacement_ws
            return False

        with patch.object(c, "_refresh_realtime_ws_token", side_effect=_refresh) as refresh:
            await connector._recover_subscriptions(event)

        refresh.assert_awaited_once_with(clear_on_failure=False)
        sdk_client.subscribe.assert_not_awaited()
        assert c._ws_public_demotion_task is None
        assert c._ws_client is replacement_ws

    @pytest.mark.asyncio
    async def test_sdk_reconnect_hard_label_failure_still_demotes_to_public(self) -> None:
        """A hard label-pin failure during SDK reconnect still demotes the socket.

        Given: An active auth client whose pinned label now matches two live
            wallets (the resolver clears auth state during the refresh),
        When: The patched SDK recover hook runs the real token refresh,
        Then: Public demotion is still scheduled — the auth socket must not
            stay installed with cleared auth state, or later subscribes
            would send undecorated params on the auth endpoint.
        """
        repository = MagicMock()
        repository.list_active_wallets = AsyncMock(
            return_value=[
                _wallet_row("wallet-a", label="market-data"),
                _wallet_row("wallet-b", label="market-data"),
            ]
        )
        c = KrakenEquitiesExchangeClient(
            repository=repository,
            realtime_ws_enabled=True,
            realtime_wallet_public_id="label:market-data",
        )
        sdk_client = MagicMock()
        connector = MagicMock()
        sdk_client._pub_conn = connector
        sdk_client.subscribe = AsyncMock()
        sdk_client.close = AsyncMock()
        c._ws_client = sdk_client
        c._ws_auth_active = True
        c._ws_token = "stale-token"
        public_ws = AsyncMock()
        assert c._disable_sdk_reconnect_replay_for_auth(sdk_client) is True
        event = asyncio.Event()
        event.set()
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.SpotWSClient",
            return_value=public_ws,
        ) as ws_cls:
            await connector._recover_subscriptions(event)
            await _await_public_demotion(c)

        sdk_client.subscribe.assert_not_awaited()
        sdk_client.close.assert_awaited_once()
        public_ws.start.assert_awaited_once()
        assert ws_cls.call_args.kwargs["ws_url"] == ke._WS_URL
        assert c._ws_client is public_ws
        assert c._ws_auth_active is False
        assert c._ws_token is None

    @pytest.mark.asyncio
    async def test_sdk_reconnect_replay_demotes_when_replay_fails(self) -> None:
        """Auth SDK reconnect replay falls back to public when replay raises."""
        c = KrakenEquitiesExchangeClient()
        sdk_client = MagicMock()
        connector = MagicMock()
        sdk_client._pub_conn = connector
        sdk_client.subscribe = AsyncMock()
        sdk_client.close = AsyncMock()
        c._ws_client = sdk_client
        c._ws_auth_active = True
        public_ws = AsyncMock()
        assert c._disable_sdk_reconnect_replay_for_auth(sdk_client) is True
        event = asyncio.Event()
        event.set()
        with (
            patch.object(
                c,
                "_refresh_realtime_ws_token",
                new_callable=AsyncMock,
                return_value=True,
            ) as refresh,
            patch.object(
                c,
                "_replay_subscriptions",
                new_callable=AsyncMock,
                side_effect=RuntimeError("replay failed"),
            ) as replay,
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.SpotWSClient",
                return_value=public_ws,
            ),
        ):
            await connector._recover_subscriptions(event)
            await _await_public_demotion(c)
        refresh.assert_awaited_once_with(clear_on_failure=False)
        replay.assert_awaited_once()
        sdk_client.close.assert_awaited_once()
        public_ws.start.assert_awaited_once()
        assert c._ws_client is public_ws
        assert c._ws_auth_active is False

    @pytest.mark.asyncio
    async def test_sdk_reconnect_replay_skips_when_replay_lost_auth_client(
        self,
    ) -> None:
        """Auth SDK reconnect replay treats replaced-client replay as stale."""
        c = KrakenEquitiesExchangeClient()
        sdk_client = MagicMock()
        connector = MagicMock()
        sdk_client._pub_conn = connector
        sdk_client.subscribe = AsyncMock()
        sdk_client.close = AsyncMock()
        public_ws = AsyncMock()
        replacement_ws = AsyncMock()
        c._ws_client = sdk_client
        c._ws_auth_active = True

        async def _replay() -> None:
            c._ws_client = public_ws
            c._ws_auth_active = False
            raise RuntimeError(ke._REPLAY_CLIENT_REPLACED_MSG)

        assert c._disable_sdk_reconnect_replay_for_auth(sdk_client) is True
        event = asyncio.Event()
        event.set()
        with (
            patch.object(
                c,
                "_refresh_realtime_ws_token",
                new_callable=AsyncMock,
                return_value=True,
            ) as refresh,
            patch.object(c, "_replay_subscriptions", side_effect=_replay) as replay,
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.SpotWSClient",
                return_value=replacement_ws,
            ) as ws_cls,
        ):
            await connector._recover_subscriptions(event)
            await _await_public_demotion(c)

        refresh.assert_awaited_once_with(clear_on_failure=False)
        replay.assert_awaited_once()
        sdk_client.close.assert_not_awaited()
        public_ws.close.assert_not_awaited()
        replacement_ws.start.assert_not_awaited()
        ws_cls.assert_not_called()
        assert c._ws_public_demotion_task is None
        assert c._ws_client is public_ws
        assert c._ws_auth_active is False

    @pytest.mark.asyncio
    async def test_sdk_reconnect_replay_skips_when_refresh_swaps_client(self) -> None:
        """Auth SDK reconnect replay rechecks ownership after token refresh."""
        c = KrakenEquitiesExchangeClient()
        sdk_client = MagicMock()
        connector = MagicMock()
        sdk_client._pub_conn = connector
        sdk_client.subscribe = AsyncMock()
        c._ws_client = sdk_client
        c._ws_auth_active = True
        req = SubscriptionRequest(channel="ticker", symbols=("CLM6.NYMEX",), parameters_json="{}")
        c._subscription_cache[req.key()] = req

        async def _refresh(*, clear_on_failure: bool = True) -> bool:
            assert clear_on_failure is False
            c._ws_client = AsyncMock()
            c._ws_token = "fresh-token"
            c._ws_token_refresh_at = ke.monotonic() + 100.0
            c._ws_token_expires_at = ke.monotonic() + 200.0
            return True

        assert c._disable_sdk_reconnect_replay_for_auth(sdk_client) is True
        event = asyncio.Event()
        event.set()
        with patch.object(c, "_refresh_realtime_ws_token", side_effect=_refresh):
            await connector._recover_subscriptions(event)
        sdk_client.subscribe.assert_not_awaited()

    def test_disable_sdk_reconnect_replay_tolerates_missing_public_connector(self) -> None:
        """The SDK replay guard is a no-op if the public connector is absent."""
        c = KrakenEquitiesExchangeClient()
        sdk_client = MagicMock()
        sdk_client._pub_conn = None
        assert c._disable_sdk_reconnect_replay_for_auth(sdk_client) is False

    def test_disable_sdk_reconnect_replay_fails_without_recover_attr(self) -> None:
        """SDK replay guard fails closed if the private recover hook is absent."""
        c = KrakenEquitiesExchangeClient()
        sdk_client = MagicMock()
        sdk_client._pub_conn = object()
        assert c._disable_sdk_reconnect_replay_for_auth(sdk_client) is False

    def test_force_sdk_public_endpoint_preserves_exact_auth_url(self) -> None:
        """Auth mode corrects the SDK endpoint after its automatic /v2 append.

        Given: An SDK client whose public connector exists,
        When: the auth endpoint override runs,
        Then: Both the client URL and connector endpoint use the exact auth URL.
        """
        c = KrakenEquitiesExchangeClient()
        sdk_client = MagicMock()
        connector = MagicMock()
        sdk_client._pub_conn = connector
        connector._ConnectSpotWebsocketBase__ws_endpoint = "bad"
        assert c._force_sdk_public_endpoint(sdk_client, ke._WS_AUTH_URL) is True
        assert sdk_client.WS_URL == ke._WS_AUTH_URL
        assert getattr(connector, "_ConnectSpotWebsocketBase__ws_endpoint") == ke._WS_AUTH_URL

    def test_force_sdk_public_endpoint_tolerates_missing_public_connector(self) -> None:
        """Endpoint correction is a no-op if the SDK public connector is absent.

        Given: An SDK client without a public connector,
        When: the auth endpoint override runs,
        Then: No exception is raised.
        """
        c = KrakenEquitiesExchangeClient()
        sdk_client = MagicMock()
        sdk_client._pub_conn = None
        assert c._force_sdk_public_endpoint(sdk_client, ke._WS_AUTH_URL) is False

    def test_force_sdk_public_endpoint_fails_when_private_endpoint_attr_is_missing(
        self,
    ) -> None:
        """Endpoint correction fails closed when the SDK private attr is absent."""
        c = KrakenEquitiesExchangeClient()
        sdk_client = MagicMock()
        sdk_client._pub_conn = object()
        assert c._force_sdk_public_endpoint(sdk_client, ke._WS_AUTH_URL) is False

    @pytest.mark.asyncio
    async def test_demote_noops_when_already_public(self) -> None:
        """Public clients are not rebuilt when demotion has already happened."""
        c = KrakenEquitiesExchangeClient()
        c._ws_auth_active = False
        c._ws_client = AsyncMock()
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.SpotWSClient"
        ) as ws_cls:
            await c._demote_realtime_ws_to_public("already public")
        ws_cls.assert_not_called()

    @pytest.mark.asyncio
    async def test_demote_installs_public_when_auth_state_has_no_client(self) -> None:
        """Auth demotion can install public WS even after the auth slot is empty."""
        c = KrakenEquitiesExchangeClient()
        c._ws_auth_active = True
        c._ws_client = None
        public_ws = AsyncMock()
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.SpotWSClient",
            return_value=public_ws,
        ) as ws_cls:
            await c._demote_realtime_ws_to_public("missing auth client")
        assert ws_cls.call_args.kwargs["ws_url"] == ke._WS_URL
        public_ws.start.assert_awaited_once()
        assert c._ws_client is public_ws
        assert c._ws_auth_active is False

    @pytest.mark.asyncio
    async def test_schedule_public_demotion_is_single_flight(self) -> None:
        """Duplicate auth failures share one pending public demotion task."""
        c = KrakenEquitiesExchangeClient()
        c._ws_auth_active = True
        started = asyncio.Event()
        release = asyncio.Event()

        async def _demote(
            reason: str,
            *,
            force: bool = False,
            expected_generation: int | None = None,
        ) -> None:
            assert reason == "first"
            assert force is True
            assert expected_generation == c._ws_connection_generation
            started.set()
            await release.wait()
            c._ws_auth_active = False

        with patch.object(c, "_demote_realtime_ws_to_public", side_effect=_demote) as demote:
            c._schedule_realtime_ws_public_demotion("first")
            await asyncio.wait_for(started.wait(), timeout=1.0)
            c._schedule_realtime_ws_public_demotion("second")
            assert demote.await_count == 1
            release.set()
            await _await_public_demotion(c)
        assert c._ws_auth_active is False

    @pytest.mark.asyncio
    async def test_late_demotion_scheduler_is_ignored_during_disconnect(self) -> None:
        """A late auth ACK cannot install public WS after disconnect starts."""
        c = KrakenEquitiesExchangeClient()
        c._ws_auth_active = True
        auth_ws = AsyncMock()
        c._ws_client = auth_ws
        close_started = asyncio.Event()
        release_close = asyncio.Event()

        async def _close(client: object) -> None:
            assert client is auth_ws
            close_started.set()
            await release_close.wait()
            if c._ws_client is client:
                c._ws_client = None

        public_ws = AsyncMock()
        with (
            patch.object(c, "_close_ws_client", side_effect=_close),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.SpotWSClient",
                return_value=public_ws,
            ) as ws_cls,
        ):
            disconnect_task = asyncio.create_task(c.disconnect())
            await asyncio.wait_for(close_started.wait(), timeout=1.0)
            c._schedule_realtime_ws_public_demotion("late auth ACK")
            release_close.set()
            await asyncio.wait_for(disconnect_task, timeout=1.0)
            await asyncio.sleep(0)

        ws_cls.assert_not_called()
        public_ws.start.assert_not_awaited()
        assert c._ws_public_demotion_task is None
        assert c._ws_client is None
        assert c._ws_closing is True

    @pytest.mark.asyncio
    async def test_stale_generation_demotion_does_not_install_public_client(self) -> None:
        """A superseded demotion task exits before building a public client."""
        c = KrakenEquitiesExchangeClient()
        c._ws_auth_active = True
        c._ws_client = AsyncMock()
        c._ws_connection_generation = 2

        with patch.object(c, "_install_ws_client", new_callable=AsyncMock) as install:
            await c._demote_realtime_ws_to_public(
                "stale",
                force=True,
                expected_generation=1,
            )

        install.assert_not_awaited()
        assert c._ws_client is not None

    @pytest.mark.asyncio
    async def test_locked_demotion_exits_when_disconnect_already_started(self) -> None:
        """Locked demotion does not install public when shutdown already won."""
        c = KrakenEquitiesExchangeClient()
        c._ws_auth_active = True
        c._ws_client = AsyncMock()
        c._ws_connection_generation = 3
        c._ws_closing = True

        with patch.object(c, "_install_ws_client", new_callable=AsyncMock) as install:
            await c._demote_realtime_ws_to_public_locked(
                "closing",
                force=True,
                expected_generation=3,
            )

        install.assert_not_awaited()
        assert c._ws_client is not None

    @pytest.mark.asyncio
    async def test_demotion_does_not_install_public_if_disconnect_starts_after_close(
        self,
    ) -> None:
        """Demotion rechecks generation after closing the old auth client."""
        c = KrakenEquitiesExchangeClient()
        auth_ws = AsyncMock()
        c._ws_auth_active = True
        c._ws_client = auth_ws
        c._ws_connection_generation = 5

        async def _close(client: object) -> None:
            assert client is auth_ws
            c._ws_connection_generation += 1
            c._ws_closing = True
            c._ws_client = None

        with (
            patch.object(c, "_close_ws_client", side_effect=_close) as close,
            patch.object(c, "_install_ws_client", new_callable=AsyncMock) as install,
        ):
            await c._demote_realtime_ws_to_public(
                "disconnect raced",
                force=True,
                expected_generation=5,
            )

        close.assert_awaited_once_with(auth_ws)
        install.assert_not_awaited()
        assert c._ws_client is None

    def test_recent_token_cache_prunes_expired_and_oldest_values(self) -> None:
        """Recent-token redaction cache stays bounded and expiry-aware."""
        c = KrakenEquitiesExchangeClient()
        now = ke.monotonic()
        c._ws_recent_tokens = {
            "expired-token": now - 1.0,
            "oldest-token": now + 1.0,
            "older-token": now + 2.0,
            "keep-a": now + 3.0,
            "keep-b": now + 4.0,
            "keep-c": now + 5.0,
        }

        c._prune_recent_ws_tokens()

        assert "expired-token" not in c._ws_recent_tokens
        assert "oldest-token" not in c._ws_recent_tokens
        assert set(c._ws_recent_tokens) == {"older-token", "keep-a", "keep-b", "keep-c"}

    @pytest.mark.asyncio
    async def test_scheduled_public_demotion_logs_failure_and_clears_slot(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Scheduled demotion task failures are observed and redacted."""
        c = KrakenEquitiesExchangeClient()
        secret_value = "demotion-secret-value"

        async def _demote(
            reason: str,
            *,
            force: bool = False,
            expected_generation: int | None = None,
        ) -> None:
            assert reason == "auth rejected"
            assert force is True
            assert expected_generation == c._ws_connection_generation
            raise RuntimeError(f"fallback failed token={secret_value}")

        sink_id = logger.add(caplog.handler, format="{message}", level="WARNING")
        try:
            with patch.object(c, "_demote_realtime_ws_to_public", side_effect=_demote):
                c._schedule_realtime_ws_public_demotion("auth rejected")
                task = c._ws_public_demotion_task
                assert task is not None
                with contextlib.suppress(RuntimeError):
                    await asyncio.wait_for(task, timeout=1.0)
                await asyncio.sleep(0)
        finally:
            logger.remove(sink_id)

        logged = "\n".join(record.message for record in caplog.records)
        assert c._ws_public_demotion_task is None
        assert "scheduled public demotion failed" in logged
        assert secret_value not in logged
        assert "***REDACTED***" in logged

    @pytest.mark.asyncio
    async def test_public_demotion_done_callback_preserves_newer_task_on_cancel(self) -> None:
        """Done callback ignores cancelled tasks that no longer own the slot."""
        c = KrakenEquitiesExchangeClient()
        task = asyncio.create_task(asyncio.sleep(30.0))
        newer_task = asyncio.create_task(asyncio.sleep(30.0))
        c._ws_public_demotion_task = newer_task

        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        c._handle_realtime_ws_public_demotion_done(task)

        assert c._ws_public_demotion_task is newer_task
        newer_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await newer_task

    @pytest.mark.asyncio
    async def test_disconnect_cancels_pending_public_demotion_task(self) -> None:
        """Disconnect cancels an unfinished callback-scheduled demotion task."""
        c = KrakenEquitiesExchangeClient()
        task = asyncio.create_task(asyncio.sleep(30.0))
        c._ws_public_demotion_task = task

        await c.disconnect()

        assert task.cancelled()
        assert c._ws_public_demotion_task is None

    @pytest.mark.asyncio
    async def test_installed_sdk_auth_patches_take_effect(self) -> None:
        """Installed SpotWSClient private patches are verified under an event loop.

        Given: The real installed python-kraken-sdk SpotWSClient,
        When: Auth endpoint and reconnect replay patches are applied,
        Then: The public connector dials the exact ``?f`` endpoint and
            Snapper-owned reconnect replay sends a freshly tokenized subscribe.
        """
        c = KrakenEquitiesExchangeClient()
        sdk_client = ke.SpotWSClient(
            ws_url=ke._WS_AUTH_URL,
            callback=AsyncMock(),
            no_public=False,
        )
        try:
            c._ws_client = sdk_client
            c._ws_auth_active = True
            req = SubscriptionRequest(
                channel="trade",
                symbols=("CLM6.NYMEX",),
                parameters_json='{"asset_class": "futures_contract", "snapshot": true, "throttle": 5000}',
            )
            c._subscription_cache[req.key()] = req
            connector = getattr(sdk_client, "_pub_conn")
            assert getattr(connector, "_ConnectSpotWebsocketBase__ws_endpoint").endswith("?f/v2")
            assert c._force_sdk_public_endpoint(sdk_client, ke._WS_AUTH_URL) is True
            assert getattr(connector, "_ConnectSpotWebsocketBase__ws_endpoint") == ke._WS_AUTH_URL
            subscribe_mock = AsyncMock()
            sdk_client.subscribe = subscribe_mock
            assert c._disable_sdk_reconnect_replay_for_auth(sdk_client) is True
            event = asyncio.Event()
            event.set()
            with patch.object(
                c,
                "_refresh_realtime_ws_token",
                new_callable=AsyncMock,
                return_value=True,
            ) as refresh:
                c._ws_token = "fresh-token"
                c._ws_token_refresh_at = ke.monotonic() + 100.0
                c._ws_token_expires_at = ke.monotonic() + 200.0
                await connector._recover_subscriptions(event)
            refresh.assert_awaited_once_with(clear_on_failure=False)
            subscribe_mock.assert_awaited_once()
            subscribe_args = subscribe_mock.await_args
            assert subscribe_args is not None
            assert subscribe_args.kwargs["params"]["token"] == "fresh-token"
        finally:
            await sdk_client.close()


@pytest.mark.asyncio
async def test_subscribe_ticks_caches_asset_class_and_throttle(
    client: KrakenEquitiesExchangeClient,
) -> None:
    """Ticker subscription cache preserves Equities-specific parameters.

    Given: An Equities client with an active websocket,
    When: A ticker subscription starts,
    Then: The cache records asset_class and throttle in parameters_json.
    """
    client._ws_client = AsyncMock()
    await client._tick_queue.put(
        TickerUpdate(
            symbol="CLM6-NYMEX",
            bid=90.0,
            bid_qty=1.0,
            ask=90.1,
            ask_qty=1.0,
            last=90.05,
            volume=10.0,
            vwap=90.0,
            low=89.0,
            high=91.0,
            change=0.1,
            change_pct=0.1,
        )
    )
    gen = client.subscribe_ticks(["CLM6-NYMEX"])
    with patch(
        "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
        return_value="CLM6.NYMEX",
    ):
        await anext(gen)
    await gen.aclose()
    req = next(iter(client._subscription_cache.values()))
    assert req.channel == "ticker"
    assert req.symbols == ("CLM6.NYMEX",)
    assert '"asset_class": "futures_contract"' in req.parameters_json
    assert '"throttle": 5000' in req.parameters_json


@pytest.mark.asyncio
async def test_subscribe_trades_caches_trade_channel(
    client: KrakenEquitiesExchangeClient,
) -> None:
    """Trade subscription cache records the trade channel.

    Given: An Equities client with an active websocket,
    When: A trade subscription starts,
    Then: The cache records a trade subscription for the WS symbol.
    """
    client._ws_client = AsyncMock()
    await client._trade_queue.put(
        TradeUpdate(
            symbol="CLM6-NYMEX",
            price=90.0,
            quantity=1.0,
            side="buy",
            ord_type="fill",
            trade_id="trade-1",
            timestamp=_dt.now(_UTC),
        )
    )
    gen = client.subscribe_trades(["CLM6-NYMEX"])
    with patch(
        "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
        return_value="CLM6.NYMEX",
    ):
        await anext(gen)
    await gen.aclose()
    req = next(iter(client._subscription_cache.values()))
    assert req.channel == "trade"
    assert req.symbols == ("CLM6.NYMEX",)


@pytest.mark.asyncio
async def test_replay_iterates_cache_in_insertion_order_with_5s_delay(
    client: KrakenEquitiesExchangeClient,
) -> None:
    """Equities replay reconstructs Spot-style params payloads.

    Given: An Equities client with cached subscriptions,
    When: Subscriptions are replayed,
    Then: Subscribe is called with reconstructed params and a 5s inter-request delay.
    """
    ws = AsyncMock()
    client._ws_client = ws
    first = SubscriptionRequest(
        channel="ticker",
        symbols=("CLM6.NYMEX",),
        parameters_json='{"asset_class": "futures_contract", "snapshot": true, "throttle": 5000}',
    )
    second = SubscriptionRequest(
        channel="trade",
        symbols=("GCQ6.COMEX",),
        parameters_json='{"asset_class": "futures_contract", "snapshot": true, "throttle": 5000}',
    )
    client._subscription_cache[first.key()] = first
    client._subscription_cache[second.key()] = second
    with patch(
        "snapper.infrastructure.exchanges.implementations.kraken_equities.asyncio.sleep",
        new_callable=AsyncMock,
    ) as sleep_mock:
        await client._replay_subscriptions()
    assert ws.subscribe.await_args_list[0].kwargs["params"]["channel"] == "ticker"
    assert ws.subscribe.await_args_list[0].kwargs["params"]["symbol"] == ["CLM6.NYMEX"]
    assert ws.subscribe.await_args_list[1].kwargs["params"]["channel"] == "trade"
    assert ws.subscribe.await_args_list[1].kwargs["params"]["symbol"] == ["GCQ6.COMEX"]
    sleep_mock.assert_awaited_once_with(5.0)


@pytest.mark.asyncio
async def test_replay_requires_ws_client(client: KrakenEquitiesExchangeClient) -> None:
    """Equities replay fails without a websocket client.

    Given: An Equities client without a websocket,
    When: Subscriptions are replayed,
    Then: RuntimeError is raised.
    """
    client._ws_client = None
    with pytest.raises(RuntimeError):
        await client._replay_subscriptions()


@pytest.mark.asyncio
async def test_ensure_ws_connected_auto_replays_after_reconnect(
    client: KrakenEquitiesExchangeClient,
) -> None:
    """Equities reconnect automatically replays cached subscriptions.

    Given: An Equities client with a cached subscription,
    When: _ensure_ws_connected creates a websocket,
    Then: The replay hook is awaited.
    """
    req = SubscriptionRequest(channel="ticker", symbols=("CLM6.NYMEX",), parameters_json="{}")
    client._subscription_cache[req.key()] = req
    with (
        patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.SpotWSClient"
        ) as ws_cls,
        patch.object(client, "_replay_subscriptions", new_callable=AsyncMock) as replay_mock,
    ):
        ws_cls.return_value.start = AsyncMock()
        await client._ensure_ws_connected()
    replay_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_ensure_ws_connected_closes_partial_client_on_failure(
    client: KrakenEquitiesExchangeClient,
) -> None:
    """A failed build closes the partial client and re-raises.

    Given: An Equities client whose WS start raises,
    When: _ensure_ws_connected runs,
    Then: The partial client is torn down (client reference cleared) and the
        error propagates so a failed recovery cannot leak the client.
    """
    with patch(
        "snapper.infrastructure.exchanges.implementations.kraken_equities.SpotWSClient"
    ) as ws_cls:
        ws_cls.return_value.start = AsyncMock(side_effect=RuntimeError("boom"))
        ws_cls.return_value.close = AsyncMock()
        with pytest.raises(RuntimeError, match="boom"):
            await client._ensure_ws_connected()
    assert client._ws_client is None


@pytest.mark.asyncio
async def test_ensure_ws_connected_times_out_on_hung_start(
    client: KrakenEquitiesExchangeClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A connect that never completes is bounded so recovery can retry.

    Given: A SpotWSClient whose start() never returns (the SDK's connect timeout
        never fires — its walrus-reset loop polls the socket forever).
    When: _ensure_ws_connected runs,
    Then: It times out within _WS_CONNECT_TIMEOUT_S, tears down the partial
        client, and raises — so recovery retries with a fresh client instead of
        wedging until a process restart.
    """
    monkeypatch.setattr(ke, "_WS_CONNECT_TIMEOUT_S", 0.05)

    async def _hang() -> None:
        await asyncio.Event().wait()

    with patch(
        "snapper.infrastructure.exchanges.implementations.kraken_equities.SpotWSClient"
    ) as ws_cls:
        ws_cls.return_value.start = _hang
        ws_cls.return_value.close = AsyncMock()
        with pytest.raises(TimeoutError):
            await client._ensure_ws_connected()
    assert client._ws_client is None


class TestEnqueueOrDropOldest:
    """Tests for _enqueue_or_drop_oldest module-level helper."""

    def test_enqueue_when_space_available(self) -> None:
        """Enqueue item normally when queue has capacity.

        Given: Queue with maxsize=2 and one existing item,
        When: _enqueue_or_drop_oldest is called with a new item,
        Then: New item is added, queue has two items total.
        """
        queue: asyncio.Queue[str] = asyncio.Queue(maxsize=2)
        queue.put_nowait("first")
        _enqueue_or_drop_oldest(queue, "second", "test")
        assert queue.qsize() == 2

    def test_drops_oldest_when_full(self) -> None:
        """Drop oldest item and enqueue newest when queue is full.

        Given: Queue with maxsize=1 already containing 'old',
        When: _enqueue_or_drop_oldest is called with 'new',
        Then: Queue contains only 'new'.
        """
        queue: asyncio.Queue[str] = asyncio.Queue(maxsize=1)
        queue.put_nowait("old")
        _enqueue_or_drop_oldest(queue, "new", "test")
        assert queue.qsize() == 1
        assert queue.get_nowait() == "new"

    def test_drop_log_is_rate_limited(self, caplog: pytest.LogCaptureFixture) -> None:
        """Per-drop log spam is collapsed to one summary per interval.

        Given: A bounded queue at capacity 1 and a freshly-reset counter,
        When: 50 drop-oldest events fire within the same interval,
        Then: At most one warning summary line is emitted.
        """
        ke._drop_counters.clear()
        queue: asyncio.Queue[str] = asyncio.Queue(maxsize=1)
        queue.put_nowait("seed")
        sink_id = logger.add(caplog.handler, format="{message}", level="WARNING")
        try:
            for i in range(50):
                _enqueue_or_drop_oldest(queue, f"item-{i}", "equities-tick")
        finally:
            logger.remove(sink_id)
        ke._drop_counters.clear()

        summaries = [rec for rec in caplog.records if "equities-tick queue full" in rec.message]
        assert len(summaries) <= 1


class TestConnect:
    """Tests for connect/disconnect lifecycle."""

    @pytest.mark.asyncio
    async def test_connect_is_noop(self, client: KrakenEquitiesExchangeClient) -> None:
        """Connect completes without error (lazy WS creation).

        Given: Fresh client,
        When: connect() is called,
        Then: Completes without error, no WS client created.
        """
        await client.connect()
        assert client._ws_client is None

    @pytest.mark.asyncio
    async def test_disconnect_without_ws(self, client: KrakenEquitiesExchangeClient) -> None:
        """Disconnect when no WS client is active.

        Given: Client with no WS connection,
        When: disconnect() is called,
        Then: Completes without error.
        """
        await client.disconnect()
        assert client._ws_client is None

    @pytest.mark.asyncio
    async def test_disconnect_with_ws(self, client: KrakenEquitiesExchangeClient) -> None:
        """Disconnect closes WS client.

        Given: Client with active WS connection,
        When: disconnect() is called,
        Then: WS client is closed and set to None.
        """
        mock_ws = AsyncMock()
        client._ws_client = mock_ws
        await client.disconnect()
        mock_ws.close.assert_awaited_once()
        assert client._ws_client is None

    @pytest.mark.asyncio
    async def test_disconnect_ws_error_handled(self, client: KrakenEquitiesExchangeClient) -> None:
        """Disconnect handles WS close error gracefully.

        Given: WS client raises on close(),
        When: disconnect() is called,
        Then: No exception propagated, WS client set to None.
        """
        mock_ws = AsyncMock()
        mock_ws.close.side_effect = RuntimeError("close failed")
        client._ws_client = mock_ws
        await client.disconnect()
        assert client._ws_client is None

    @pytest.mark.asyncio
    async def test_disconnect_ws_close_timeout(self, client: KrakenEquitiesExchangeClient) -> None:
        """Disconnect drops the client when the close times out.

        Given: WS close exceeds the close timeout,
        When: disconnect() is called,
        Then: The timeout is handled and the WS client is set to None so a
            blackholed socket cannot hang recovery or shutdown.
        """
        mock_ws = AsyncMock()
        mock_ws.close.side_effect = TimeoutError
        client._ws_client = mock_ws
        await client.disconnect()
        assert client._ws_client is None

    @pytest.mark.asyncio
    async def test_disconnect_timeout_invokes_force_close(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """A close timeout hands the client to the force teardown (#143).

        Given: WS close exceeds the close timeout (connector stuck in its
            reconnect backoff),
        When: disconnect() is called,
        Then: ``force_close_ws_client`` is awaited with the abandoned client
            so its run task is cancelled and its aiohttp session closed —
            the pre-fix behaviour leaked one session per rebuild cycle.
        """
        mock_ws = AsyncMock()
        mock_ws.close.side_effect = TimeoutError
        client._ws_client = mock_ws
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities."
            "force_close_ws_client",
            new_callable=AsyncMock,
        ) as force_close:
            await client.disconnect()
        force_close.assert_awaited_once_with(mock_ws)
        assert client._ws_client is None


class TestOnWsMessage:
    """Tests for the WS callback-to-queue bridge."""

    @pytest.fixture(autouse=True)
    def _patch_adapters(self) -> Generator[None]:
        """Patch adapter functions for WS message routing tests."""
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.parse_kraken_equities_ticker",
                return_value=TickerUpdate(
                    symbol="CLM6-NYMEX",
                    bid=90.10,
                    bid_qty=3.0,
                    ask=90.13,
                    ask_qty=4.0,
                    last=90.11,
                    volume=148427.0,
                    vwap=91.2,
                    low=88.7,
                    high=94.81,
                    change=-3.05,
                    change_pct=-3.27,
                ),
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.parse_kraken_equities_trade",
                return_value=TradeUpdate(
                    symbol="CLM6-NYMEX",
                    side="buy",
                    quantity=1.0,
                    price=90.12,
                    ord_type="fill",
                    timestamp=MagicMock(),
                    trade_id="7623847138430121307",
                ),
            ),
        ):
            yield

    @pytest.mark.asyncio
    async def test_on_ws_message_drops_ticker_snapshot(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Drop ticker snapshot WS message before enqueue.

        Given: WS message with channel=ticker and type=snapshot,
        When: _on_ws_message is called,
        Then: No TickerUpdate is placed in _tick_queue.
        """
        msg = {
            "channel": "ticker",
            "type": "snapshot",
            "data": [{"symbol": "CLM6.NYMEX", "bid": 90.10}],
        }
        await client._on_ws_message(msg)
        assert client._tick_queue.empty()

    @pytest.mark.asyncio
    async def test_on_ws_message_passes_ticker_update(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Route ticker update WS message to tick_queue.

        Given: WS message with channel=ticker and type=update,
        When: _on_ws_message is called,
        Then: Parsed TickerUpdate is placed in _tick_queue.
        """
        msg = {
            "channel": "ticker",
            "type": "update",
            "data": [{"symbol": "CLM6.NYMEX", "last": 90.15}],
        }
        await client._on_ws_message(msg)
        assert not client._tick_queue.empty()

    @pytest.mark.asyncio
    async def test_ticker_message_handles_non_symbol_items(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Ticker updates tolerate non-dict items and dicts without symbols.

        Given: A ticker update includes payload items without a usable symbol,
        When: _on_ws_message is called,
        Then: Parsing still runs and enqueueing succeeds without health-key errors.
        """
        msg = {
            "channel": "ticker",
            "type": "update",
            "data": ["not-a-dict", {"last": 90.15}],
        }
        await client._on_ws_message(msg)
        assert client._tick_queue.qsize() == 2

    @pytest.mark.asyncio
    async def test_on_ws_message_drops_trade_snapshot(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Drop trade snapshot WS message before enqueue.

        Given: WS message with channel=trade and type=snapshot,
        When: _on_ws_message is called,
        Then: No TradeUpdate is placed in _trade_queue.
        """
        msg = {
            "channel": "trade",
            "type": "snapshot",
            "data": [
                {
                    "symbol": "CLM6.NYMEX",
                    "side": "buy",
                    "price": 90.12,
                    "qty": 1,
                    "timestamp": "2026-04-01T17:40:36.368Z",
                    "sequence": 95579,
                    "index": 7623847138430121307,
                }
            ],
        }
        await client._on_ws_message(msg)
        assert client._trade_queue.empty()
        assert client._candle_builder.active_buckets() == 0

    @pytest.mark.asyncio
    async def test_trade_message_also_folds_into_candle_builder(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Every parsed trade is also routed into ``_candle_builder``.

        Given: A trade update WS message arrives via the equities channel,
        When: ``_on_ws_message`` is called,
        Then: ``_candle_builder.active_buckets()`` becomes 1 —
            confirming the trade-handler path wires the builder, not
            just the trade queue. Regression guard analogous to the
            Kraken Futures test for the same property; without it,
            the live candle stream would silently stay empty.
        """
        assert client._candle_builder.active_buckets() == 0
        msg = {
            "channel": "trade",
            "type": "update",
            "data": [
                {
                    "symbol": "CLM6.NYMEX",
                    "side": "buy",
                    "price": 90.12,
                    "qty": 1,
                    "timestamp": "2026-04-01T17:40:36.368Z",
                    "sequence": 95579,
                    "index": 7623847138430121307,
                }
            ],
        }
        await client._on_ws_message(msg)
        assert client._candle_builder.active_buckets() == 1

    @pytest.mark.asyncio
    async def test_trade_message_handles_non_symbol_items(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Trade updates tolerate non-dict items and dicts without symbols.

        Given: A trade update includes payload items without a usable symbol,
        When: _on_ws_message is called,
        Then: Parsing still runs and the trade queue receives both parsed items.
        """
        msg = {
            "channel": "trade",
            "type": "update",
            "data": ["not-a-dict", {"price": 90.12}],
        }
        trades = [
            TradeUpdate(
                symbol="CLM6-NYMEX",
                side="buy",
                quantity=1.0,
                price=90.12,
                ord_type="fill",
                timestamp=_dt(2026, 6, 29, 12, 0, tzinfo=_UTC),
                trade_id="trade-1",
            ),
            TradeUpdate(
                symbol="CLM6-NYMEX",
                side="buy",
                quantity=1.0,
                price=90.13,
                ord_type="fill",
                timestamp=_dt(2026, 6, 29, 12, 1, tzinfo=_UTC),
                trade_id="trade-2",
            ),
        ]
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities."
            "parse_kraken_equities_trade",
            side_effect=trades,
        ):
            await client._on_ws_message(msg)
        assert client._trade_queue.qsize() == 2

    @pytest.mark.asyncio
    async def test_heartbeat_ignored(self, client: KrakenEquitiesExchangeClient) -> None:
        """Ignore heartbeat messages.

        Given: WS heartbeat message,
        When: _on_ws_message is called,
        Then: No items in any queue.
        """
        await client._on_ws_message({"channel": "heartbeat", "type": "heartbeat"})
        assert client._tick_queue.empty()
        assert client._trade_queue.empty()

    @pytest.mark.asyncio
    async def test_list_message_ignored(self, client: KrakenEquitiesExchangeClient) -> None:
        """Ignore list-type messages.

        Given: WS message that is a list,
        When: _on_ws_message is called,
        Then: No items in any queue.
        """
        await client._on_ws_message([1, 2, 3])
        assert client._tick_queue.empty()
        assert client._trade_queue.empty()

    @pytest.mark.asyncio
    async def test_ticker_subscription_ack_marks_confirmed(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Ticker subscribe ACKs mark the symbol confirmed.

        Given: A successful ticker subscribe ACK,
        When: _on_ws_message handles it,
        Then: The health tracker records a confirmed ticker subscription.
        """
        msg = {
            "method": "subscribe",
            "result": {
                "channel": "ticker",
                "symbol": "CLM6.NYMEX",
                "snapshot": True,
            },
            "success": True,
        }
        await client._on_ws_message(msg)
        entry = client._health_tracker.snapshot()[("ticker", "CLM6.NYMEX")]
        assert entry.status == "confirmed"

    @pytest.mark.asyncio
    async def test_ticker_subscription_ack_marks_failed(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Ticker subscribe rejection marks the symbol failed.

        Given: A failed ticker subscribe ACK,
        When: _on_ws_message handles it,
        Then: The health tracker records a terminal failed ticker subscription.
        """
        msg = {
            "method": "subscribe",
            "result": {
                "channel": "ticker",
                "symbol": "CLM6.NYMEX",
                "snapshot": True,
            },
            "success": False,
            "error": "invalid symbol",
        }
        await client._on_ws_message(msg)
        entry = client._health_tracker.snapshot()[("ticker", "CLM6.NYMEX")]
        assert entry.status == "failed"
        assert entry.last_error == "invalid symbol"

    @pytest.mark.asyncio
    async def test_ticker_subscription_ack_failure_log_redacts_token(
        self,
        client: KrakenEquitiesExchangeClient,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Subscribe failure logs redact current and token-shaped values."""
        known_token = "short-active-token"
        token_like_value = "T" * 43
        client._ws_auth_active = True
        client._ws_token = known_token
        client._ws_token_refresh_at = ke.monotonic() + 100.0
        client._ws_token_expires_at = ke.monotonic() + 200.0
        msg = {
            "method": "subscribe",
            "result": {
                "channel": "ticker",
                "symbol": "CLM6.NYMEX",
                "snapshot": True,
            },
            "success": False,
            "error": f"invalid token {known_token} echoed {token_like_value}",
        }
        sink_id = logger.add(caplog.handler, format="{message}", level="WARNING")
        try:
            with patch.object(client, "_schedule_realtime_ws_public_demotion"):
                await client._on_ws_message(msg)
        finally:
            logger.remove(sink_id)
        logged = "\n".join(record.message for record in caplog.records)
        assert known_token not in logged
        assert token_like_value not in logged
        assert "***REDACTED***" in logged

    @pytest.mark.asyncio
    async def test_ticker_subscription_ack_health_error_redacts_active_token(
        self,
        client: KrakenEquitiesExchangeClient,
    ) -> None:
        """Ticker ACK health storage redacts token while demotion still fires."""
        known_token = "short-active-token"
        client._ws_auth_active = True
        client._ws_token = known_token
        client._ws_token_refresh_at = ke.monotonic() + 100.0
        client._ws_token_expires_at = ke.monotonic() + 200.0
        msg = {
            "method": "subscribe",
            "result": {
                "channel": "ticker",
                "symbol": "CLM6.NYMEX",
                "snapshot": True,
            },
            "success": False,
            "error": f"invalid token {known_token}",
        }
        with patch.object(client, "_schedule_realtime_ws_public_demotion") as demote:
            await client._on_ws_message(msg)

        entry = client._health_tracker.snapshot()[("ticker", "CLM6.NYMEX")]
        assert entry.status == "failed"
        assert known_token not in (entry.last_error or "")
        assert "***REDACTED***" in (entry.last_error or "")
        demote.assert_called_once_with("auth subscribe ACK rejected")

    @pytest.mark.asyncio
    async def test_subscription_ack_failure_log_redacts_previous_token(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Delayed subscribe failure logs redact rotated-out auth tokens."""
        repository = MagicMock()
        client = KrakenEquitiesExchangeClient(
            repository=repository,
            realtime_ws_enabled=True,
            realtime_wallet_public_id="wallet-1",
        )
        previous_token = "previous-ws-token"
        current_token = "current-ws-token"
        resolver = MagicMock()
        resolver.get_credentials = AsyncMock(
            return_value={"api_key": "api-key", "api_secret": "api-secret"}
        )
        payloads: list[object] = [
            {"token": previous_token, "expires": 900},
            {"token": current_token, "expires": 900},
        ]

        async def _dispatch(func: object, api_key: str, api_secret: str) -> object:
            assert callable(func)
            assert api_key == "api-key"
            assert api_secret == "api-secret"
            return payloads.pop(0)

        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities."
                "CredentialResolver",
                return_value=resolver,
            ),
            patch.object(client, "_dispatch_blocking", side_effect=_dispatch),
        ):
            assert await client._refresh_realtime_ws_token() is True
            assert await client._refresh_realtime_ws_token() is True

        msg = {
            "method": "subscribe",
            "result": {
                "channel": "ticker",
                "symbol": "CLM6.NYMEX",
                "snapshot": True,
            },
            "success": False,
            "error": f"invalid token {previous_token}",
        }
        sink_id = logger.add(caplog.handler, format="{message}", level="WARNING")
        try:
            with patch.object(client, "_schedule_realtime_ws_public_demotion"):
                await client._on_ws_message(msg)
        finally:
            logger.remove(sink_id)
        logged = "\n".join(record.message for record in caplog.records)
        assert previous_token not in logged
        assert current_token not in logged
        assert "***REDACTED***" in logged

    def test_redact_sensitive_payload_masks_token_echoes_and_query_params(self) -> None:
        """Payload redaction masks token fields, echoes, and query strings."""
        field_token = "field-token"
        query_token = "query-token"
        payload = {
            "params": {"token": field_token},
            "error": (
                f"invalid {field_token} at "
                f"wss://ws-equities-auth.kraken.com/?token={query_token}&channel=ticker"
            ),
            "nested": [f"retry echoed {field_token}"],
        }

        redacted = ke._redact_sensitive_payload(payload)

        rendered = str(redacted)
        assert field_token not in rendered
        assert query_token not in rendered
        assert "token=***REDACTED***" in rendered
        assert "***REDACTED***" in rendered

    @pytest.mark.asyncio
    async def test_auth_token_ack_error_demotes_to_public_feed(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Auth/token subscribe rejection schedules callback-safe demotion.

        Given: The authenticated feed is active,
        When: Kraken rejects a subscribe from inside an SDK callback child task,
        Then: The callback returns without deadlock and a public socket is started.
        """
        client._ws_auth_active = True
        auth_ws = _SdkShapedWsClient()
        public_ws = AsyncMock()
        client._ws_client = auth_ws
        msg = {
            "method": "subscribe",
            "result": {
                "channel": "ticker",
                "symbol": "CLM6.NYMEX",
                "snapshot": True,
            },
            "success": False,
            "error": "invalid token",
        }

        async def _child_callback() -> None:
            await client._on_ws_message(msg)

        async def _parent_connector() -> None:
            child_task = asyncio.create_task(_child_callback())
            await child_task

        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.SpotWSClient",
            return_value=public_ws,
        ) as ws_cls:
            parent_task = asyncio.create_task(_parent_connector())
            auth_ws.parent_task = parent_task
            await asyncio.wait_for(parent_task, timeout=0.5)
            assert client._ws_auth_active is True
            await _await_public_demotion(client)
        assert auth_ws.closed is True
        assert auth_ws.close_started.is_set()
        assert auth_ws.close_finished.is_set()
        public_ws.start.assert_awaited_once()
        assert ws_cls.call_args.kwargs["ws_url"] == ke._WS_URL
        assert client._ws_client is public_ws
        assert client._ws_auth_active is False

    @pytest.mark.asyncio
    async def test_ticker_subscription_ack_without_symbol_is_ignored(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Ticker ACKs without a string symbol do not create health entries."""
        ack = MagicMock(success=True, error=None)
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities."
            "KrakenTickerSubscriptionAckSchema.model_validate",
            return_value=ack,
        ):
            await client._on_ws_message(
                {
                    "method": "subscribe",
                    "result": {"channel": "ticker"},
                    "success": True,
                }
            )
        assert client._health_tracker.snapshot() == {}

    @pytest.mark.asyncio
    async def test_logged_ws_control_message_redacts_token(
        self,
        client: KrakenEquitiesExchangeClient,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Whole-message control logs never expose token-like secrets."""
        ack = MagicMock(success=True, error=None)
        sink_id = logger.add(caplog.handler, format="{message}", level="DEBUG")
        try:
            with patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities."
                "KrakenTickerSubscriptionAckSchema.model_validate",
                return_value=ack,
            ):
                await client._on_ws_message(
                    {
                        "method": "subscribe",
                        "result": {"channel": "ticker"},
                        "params": {
                            "token": "secret-token",
                            "api_key": "secret-key",
                            "nested": {"api_secret": "secret-value"},
                        },
                        "success": True,
                    }
                )
        finally:
            logger.remove(sink_id)
        logged = "\n".join(record.message for record in caplog.records)
        assert "secret-token" not in logged
        assert "secret-key" not in logged
        assert "secret-value" not in logged
        assert "***REDACTED***" in logged

    @pytest.mark.asyncio
    async def test_ticker_subscription_ack_validation_error_is_ignored(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Non-standard ticker control messages do not raise."""
        await client._on_ws_message(
            {
                "method": "subscribe",
                "result": {"channel": "ticker"},
                "success": True,
            }
        )
        assert client._health_tracker.snapshot() == {}

    @pytest.mark.asyncio
    async def test_trade_subscription_ack_marks_confirmed(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Trade subscribe ACKs mark the symbol confirmed.

        Given: A successful trade subscribe ACK,
        When: _on_ws_message handles it,
        Then: The health tracker records a confirmed trade subscription.
        """
        msg = {
            "method": "subscribe",
            "result": {"channel": "trade", "symbol": "CLM6.NYMEX"},
            "success": True,
        }
        await client._on_ws_message(msg)
        entry = client._health_tracker.snapshot()[("trade", "CLM6.NYMEX")]
        assert entry.status == "confirmed"

    @pytest.mark.asyncio
    async def test_trade_subscription_ack_marks_failed_with_default_error(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Trade subscribe rejection without error uses the default message.

        Given: A failed trade subscribe ACK without an error string,
        When: _on_ws_message handles it,
        Then: The health tracker records a failed trade subscription.
        """
        msg = {
            "method": "subscribe",
            "result": {"channel": "trade", "symbol": "CLM6.NYMEX"},
            "success": False,
        }
        await client._on_ws_message(msg)
        entry = client._health_tracker.snapshot()[("trade", "CLM6.NYMEX")]
        assert entry.status == "failed"
        assert entry.last_error == "unknown subscription error"

    @pytest.mark.asyncio
    async def test_trade_subscription_ack_health_error_redacts_recent_token(
        self,
        client: KrakenEquitiesExchangeClient,
    ) -> None:
        """Trade ACK health storage redacts recently issued token values."""
        previous_token = "previous-ws-token"
        client._ws_auth_active = True
        client._ws_token = "current-ws-token"
        client._ws_token_refresh_at = ke.monotonic() + 100.0
        client._ws_token_expires_at = ke.monotonic() + 200.0
        client._remember_realtime_ws_token(previous_token, ke.monotonic() + 200.0)
        msg = {
            "method": "subscribe",
            "result": {"channel": "trade", "symbol": "CLM6.NYMEX"},
            "success": False,
            "error": f"invalid token {previous_token}",
        }
        with patch.object(client, "_schedule_realtime_ws_public_demotion") as demote:
            await client._on_ws_message(msg)

        entry = client._health_tracker.snapshot()[("trade", "CLM6.NYMEX")]
        assert entry.status == "failed"
        assert previous_token not in (entry.last_error or "")
        assert "***REDACTED***" in (entry.last_error or "")
        demote.assert_called_once_with("auth subscribe ACK rejected")

    @pytest.mark.asyncio
    async def test_trade_subscription_ack_without_symbol_is_ignored(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Trade ACKs without a string symbol do not create health entries."""
        await client._on_ws_message(
            {
                "method": "subscribe",
                "result": {"channel": "trade"},
                "success": True,
            }
        )
        assert client._health_tracker.snapshot() == {}

    @pytest.mark.asyncio
    async def test_trade_subscription_ack_validation_error_is_ignored(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Non-standard trade control messages do not raise."""
        await client._on_ws_message(
            {
                "method": "subscribe",
                "result": {"channel": "trade", "symbol": "CLM6.NYMEX"},
                "success": [],
            }
        )
        assert client._health_tracker.snapshot() == {}

    @pytest.mark.asyncio
    async def test_unknown_subscription_ack_channel_is_ignored(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Subscribe ACKs for unsupported channels do not mutate health state."""
        await client._on_ws_message(
            {
                "method": "subscribe",
                "result": {"channel": "book", "symbol": "CLM6.NYMEX"},
                "success": True,
            }
        )
        assert client._health_tracker.snapshot() == {}

    @pytest.mark.asyncio
    async def test_unparseable_ticker_skipped(self, client: KrakenEquitiesExchangeClient) -> None:
        """Skip ticker messages that fail parsing.

        Given: WS ticker message that causes ValueError in parser,
        When: _on_ws_message is called,
        Then: No items in tick_queue and no exception raised.
        """
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.parse_kraken_equities_ticker",
            side_effect=ValueError("parse error"),
        ):
            msg = {
                "channel": "ticker",
                "type": "update",
                "data": [{"symbol": "INVALID"}],
            }
            await client._on_ws_message(msg)
        assert client._tick_queue.empty()

    @pytest.mark.asyncio
    async def test_unparseable_trade_skipped(self, client: KrakenEquitiesExchangeClient) -> None:
        """Skip trade messages that fail parsing.

        Given: WS trade message that causes ValueError in parser,
        When: _on_ws_message is called,
        Then: No items in trade_queue and no exception raised.
        """
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.parse_kraken_equities_trade",
            side_effect=ValueError("parse error"),
        ):
            msg = {
                "channel": "trade",
                "type": "update",
                "data": [{"symbol": "INVALID"}],
            }
            await client._on_ws_message(msg)
        assert client._trade_queue.empty()


class TestFetchInstrumentsRest:
    """Tests for REST instrument fetching."""

    @pytest.mark.asyncio
    async def test_fetch_instruments_rest_filters_active(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Fetch and filter only active tradable contracts.

        Given: REST response with 3 contracts (2 active+tradable, 1 inactive),
        When: _fetch_instruments_rest is called,
        Then: Returns only the 2 active tradable contracts.
        """
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "result": {
                "data": [
                    {"symbol": "CLM6.NYMEX", "tradable": True, "status": "active"},
                    {"symbol": "GCQ6.COMEX", "tradable": True, "status": "active"},
                    {"symbol": "CLK5.NYMEX", "tradable": False, "status": "inactive"},
                ]
            }
        }
        mock_response.raise_for_status = MagicMock()

        mock_http_client = AsyncMock()
        mock_http_client.get.return_value = mock_response
        mock_http_client.__aenter__ = AsyncMock(return_value=mock_http_client)
        mock_http_client.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.httpx.AsyncClient",
            return_value=mock_http_client,
        ):
            result = await client._fetch_instruments_rest()
        assert len(result) == 2
        assert result[0]["symbol"] == "CLM6.NYMEX"
        assert result[1]["symbol"] == "GCQ6.COMEX"

    @pytest.mark.asyncio
    async def test_fetch_instruments_rest_filters_tradable_but_not_active(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Filter out contracts that are tradable but not active.

        Given: REST response with tradable=True but status=undefined,
        When: _fetch_instruments_rest is called,
        Then: Contract is excluded.
        """
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "result": {
                "data": [
                    {"symbol": "CLM6.NYMEX", "tradable": True, "status": "undefined"},
                ]
            }
        }
        mock_response.raise_for_status = MagicMock()

        mock_http_client = AsyncMock()
        mock_http_client.get.return_value = mock_response
        mock_http_client.__aenter__ = AsyncMock(return_value=mock_http_client)
        mock_http_client.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.httpx.AsyncClient",
            return_value=mock_http_client,
        ):
            result = await client._fetch_instruments_rest()
        assert len(result) == 0


class TestGetParsedInstrument:
    """Tests for get_parsed_instrument."""

    def test_get_parsed_instrument(self, client: KrakenEquitiesExchangeClient) -> None:
        """Parse raw instrument dict into descriptor.

        Given: Raw instrument dict,
        When: get_parsed_instrument is called,
        Then: Returns InstrumentPairDescriptor.
        """
        raw = {
            "symbol": "CLM6.NYMEX",
            "tradable": True,
            "status": "active",
            "tick_size": "0.01",
            "contract_size": "1000",
            "base": "USD",
            "quote": "USD",
        }
        result = client.get_parsed_instrument(raw)
        assert isinstance(result, InstrumentPairDescriptor)
        assert result.symbol == "CLM6.NYMEX"
        assert result.base == "USD"


class TestNotImplementedMethods:
    """Tests for market-data-only stub methods."""

    @pytest.mark.asyncio
    async def test_create_order_raises(self, client: KrakenEquitiesExchangeClient) -> None:
        """create_order raises NotImplementedError.

        Given: Market-data-only client,
        When: create_order is called,
        Then: Raises NotImplementedError.
        """
        with pytest.raises(NotImplementedError, match="market data only"):
            await client.create_order(MagicMock())

    @pytest.mark.asyncio
    async def test_cancel_order_raises(self, client: KrakenEquitiesExchangeClient) -> None:
        """cancel_order raises NotImplementedError.

        Given: Market-data-only client,
        When: cancel_order is called,
        Then: Raises NotImplementedError.
        """
        with pytest.raises(NotImplementedError, match="market data only"):
            await client.cancel_order("123")

    @pytest.mark.asyncio
    async def test_get_order_raises(self, client: KrakenEquitiesExchangeClient) -> None:
        """get_order raises NotImplementedError.

        Given: Market-data-only client,
        When: get_order is called,
        Then: Raises NotImplementedError.
        """
        with pytest.raises(NotImplementedError, match="market data only"):
            await client.get_order("123")

    @pytest.mark.asyncio
    async def test_get_orders_raises(self, client: KrakenEquitiesExchangeClient) -> None:
        """get_orders raises NotImplementedError.

        Given: Market-data-only client,
        When: get_orders is called,
        Then: Raises NotImplementedError.
        """
        with pytest.raises(NotImplementedError, match="market data only"):
            await client.get_orders()

    @pytest.mark.asyncio
    async def test_get_balance_raises(self, client: KrakenEquitiesExchangeClient) -> None:
        """get_balance raises NotImplementedError.

        Given: Market-data-only client,
        When: get_balance is called,
        Then: Raises NotImplementedError.
        """
        with pytest.raises(NotImplementedError, match="market data only"):
            await client.get_balance()

    @pytest.mark.asyncio
    async def test_get_ticker_raises(self, client: KrakenEquitiesExchangeClient) -> None:
        """get_ticker raises NotImplementedError.

        Given: Market-data-only client,
        When: get_ticker is called,
        Then: Raises NotImplementedError.
        """
        with pytest.raises(NotImplementedError, match="WebSocket ticker"):
            await client.get_ticker("CLM6-NYMEX")

    @pytest.mark.asyncio
    async def test_subscribe_candles_reuses_running_aggregator_task(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """A second subscribe_candles call does not spawn a duplicate aggregator.

        Given: A pre-existing aggregator task that is still running,
        When: subscribe_candles is iterated and immediately closed,
        Then: The same task object remains the client's aggregator
            handle (the ``is None or .done()`` guard short-circuits).
        """

        async def never_returns() -> None:
            while True:
                await asyncio.sleep(60)

        client._candle_aggregator_task = asyncio.create_task(never_returns())
        original_task = client._candle_aggregator_task
        try:
            iterator = client.subscribe_candles(["MNQM6-CME"], "1m")
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(iterator.__anext__(), timeout=0.05)
            assert client._candle_aggregator_task is original_task
        finally:
            original_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await original_task

    @pytest.mark.asyncio
    async def test_subscribe_candles_timeout_loops_until_candle_arrives(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """The yield loop survives queue-empty timeouts without spinning out.

        Given: The candle queue is empty and the aggregator never
            emits anything,
        When: subscribe_candles is awaited with a tight outer timeout,
        Then: The inner ``asyncio.wait_for`` raises ``TimeoutError``,
            the ``await asyncio.sleep(0.01)`` branch executes, and
            the outer ``wait_for`` is the one that finally bails — proving
            the empty-queue branch is reachable.
        """
        real_sleep = asyncio.sleep

        async def fast_sleep(_: float) -> None:
            await real_sleep(0)

        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.asyncio.sleep",
            new=fast_sleep,
        ):
            iterator = client.subscribe_candles(["MNQM6-CME"], "1m")
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(iterator.__anext__(), timeout=0.2)

    @pytest.mark.asyncio
    async def test_subscribe_candles_rejects_non_1m(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """subscribe_candles only accepts the 1m timeframe.

        Given: A ``"5m"`` request,
        When: subscribe_candles is iterated,
        Then: A ``ValueError`` surfaces with a hint to ``get_ohlcv``.
        """
        iterator = client.subscribe_candles(["MNQM6-CME"], "5m")
        with pytest.raises(ValueError, match="only supports 1m"):
            await iterator.__anext__()

    @pytest.mark.asyncio
    async def test_subscribe_candles_emits_built_from_trades(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """subscribe_candles emits a candle once the EVENT watermark passes it.

        Given: A trade in past minute M, plus a later (still past) trade in
            M+2 that advances the builder's event watermark beyond M's end +
            grace. Kraken Equities closes candles by the feed's event-clock,
            not wall-clock (the delayed-feed fix), so a lone trade does NOT
            finalize — a later trade must move the watermark,
        When: subscribe_candles is iterated with ``asyncio.sleep`` collapsed,
        Then: A ``CandleUpdate`` carrying M's OHLCV is yielded; the M+2
            bucket stays open.
        """
        past_minute = (_dt.now(_UTC) - _td(minutes=5)).replace(second=0, microsecond=0)
        client._candle_builder.update(
            TradeUpdate(
                symbol="MNQM6-CME",
                side="buy",
                quantity=2.0,
                price=22500.0,
                ord_type="fill",
                timestamp=past_minute,
                trade_id="t1",
            )
        )
        client._candle_builder.update(
            TradeUpdate(
                symbol="MNQM6-CME",
                side="buy",
                quantity=1.0,
                price=22510.0,
                ord_type="fill",
                timestamp=past_minute + _td(minutes=2),
                trade_id="t2",
            )
        )

        real_sleep = asyncio.sleep

        async def fast_sleep(_: float) -> None:
            await real_sleep(0)

        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.asyncio.sleep",
            new=fast_sleep,
        ):
            iterator = client.subscribe_candles(["MNQM6-CME"], "1m")
            result = await asyncio.wait_for(iterator.__anext__(), timeout=2.0)
        assert isinstance(result, CandleUpdate)
        assert result.symbol == "MNQM6-CME"
        assert result.open == pytest.approx(22500.0)
        assert result.volume == pytest.approx(2.0)
        assert result.trades == 1
        assert result.interval == 60

    @pytest.mark.asyncio
    async def test_candle_aggregator_idle_flush_emits_stranded_bucket(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """A quiet feed triggers the idle flush so the final bar is not stranded.

        Given: One trade whose minute the event watermark never passes (no
            later trade arrives), so the watermark-close path leaves it open,
        When: wall-clock silence exceeds ``_CANDLE_IDLE_FLUSH_S`` — simulated
            by a ``monotonic`` stub that jumps past the threshold,
        Then: ``pop_all`` flushes the stranded bucket and subscribe_candles
            yields it.
        """
        minute = _dt(2026, 6, 8, 14, 39, tzinfo=_UTC)
        client._candle_builder.update(
            TradeUpdate(
                symbol="MNQM6-CME",
                side="buy",
                quantity=3.0,
                price=22500.0,
                ord_type="fill",
                timestamp=minute,
                trade_id="t1",
            )
        )

        real_sleep = asyncio.sleep

        async def fast_sleep(_: float) -> None:
            await real_sleep(0)

        mono_values = iter([0.0, 0.0, 1000.0])

        def fake_monotonic() -> float:
            try:
                return next(mono_values)
            except StopIteration:
                return 1000.0

        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.asyncio.sleep",
                new=fast_sleep,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.monotonic",
                new=fake_monotonic,
            ),
        ):
            iterator = client.subscribe_candles(["MNQM6-CME"], "1m")
            result = await asyncio.wait_for(iterator.__anext__(), timeout=2.0)
        assert isinstance(result, CandleUpdate)
        assert result.symbol == "MNQM6-CME"
        assert result.volume == pytest.approx(3.0)
        assert result.trades == 1

    @pytest.mark.asyncio
    async def test_candle_aggregator_idle_flush_deferred_while_trades_arrive(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Ongoing trade activity defers the idle flush even with a stuck watermark.

        Regression for the review finding: keying the idle timer on watermark
        movement would flush mid-activity when out-of-order / same-minute
        delayed trades keep arriving (they do NOT advance the watermark).

        Given: A bucket open within grace, with a trade arriving every tick at
            a timestamp that does NOT advance the watermark (so the activity
            counter changes but the watermark is stuck),
        When: wall-clock jumps far past ``_CANDLE_IDLE_FLUSH_S``,
        Then: the idle flush does NOT fire (activity resets the timer) and no
            premature/fragmented candle is emitted — the iterator times out.
        """
        minute = _dt(2026, 6, 8, 14, 39, tzinfo=_UTC)
        builder = client._candle_builder
        builder.update(
            TradeUpdate(
                symbol="MNQM6-CME",
                side="buy",
                quantity=1.0,
                price=22500.0,
                ord_type="fill",
                timestamp=minute,
                trade_id="t0",
            )
        )

        real_sleep = asyncio.sleep
        state = {"n": 0}

        async def fast_sleep_with_trade(_: float) -> None:
            state["n"] += 1
            builder.update(
                TradeUpdate(
                    symbol="MNQM6-CME",
                    side="buy",
                    quantity=1.0,
                    price=22500.0,
                    ord_type="fill",
                    timestamp=minute + _td(seconds=5),
                    trade_id=f"t{state['n']}",
                )
            )
            await real_sleep(0)

        def fake_monotonic() -> float:
            return state["n"] * 1000.0

        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.asyncio.sleep",
                new=fast_sleep_with_trade,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.monotonic",
                new=fake_monotonic,
            ),
        ):
            iterator = client.subscribe_candles(["MNQM6-CME"], "1m")
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(iterator.__anext__(), timeout=0.3)

    def test_subscribe_executions_raises(self, client: KrakenEquitiesExchangeClient) -> None:
        """subscribe_executions raises NotImplementedError.

        Given: Market-data-only client,
        When: subscribe_executions is called,
        Then: Raises NotImplementedError.
        """
        with pytest.raises(NotImplementedError, match="market data only"):
            client.subscribe_executions()


class TestEnsureWsConnected:
    """Tests for WebSocket connection management."""

    @pytest.mark.asyncio
    async def test_creates_ws_on_first_call(self, client: KrakenEquitiesExchangeClient) -> None:
        """Create WS client on first call.

        Given: No WS client,
        When: _ensure_ws_connected is called,
        Then: SpotWSClient is created and started.
        """
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.SpotWSClient"
        ) as mock_cls:
            mock_ws = AsyncMock()
            mock_cls.return_value = mock_ws
            await client._ensure_ws_connected()
            mock_cls.assert_called_once()
            mock_ws.start.assert_awaited_once()
            assert client._ws_client is mock_ws

    @pytest.mark.asyncio
    async def test_noop_when_already_connected(self, client: KrakenEquitiesExchangeClient) -> None:
        """Skip creation when already connected.

        Given: WS client already exists,
        When: _ensure_ws_connected is called,
        Then: No new client is created.
        """
        mock_ws = AsyncMock()
        client._ws_client = mock_ws
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.SpotWSClient"
        ) as mock_cls:
            await client._ensure_ws_connected()
            mock_cls.assert_not_called()


class TestSubscribeTicks:
    """Tests for subscribe_ticks async iterator."""

    @pytest.mark.asyncio
    async def test_subscribe_ticks_yields_from_queue(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Yield ticker updates from internal queue.

        Given: WS client connected and tick_queue has an item,
        When: subscribe_ticks iterator is consumed,
        Then: Yields the queued TickerUpdate.
        """
        mock_ws = AsyncMock()
        client._ws_client = mock_ws
        client._ensure_ws_connected = AsyncMock()
        ticker = TickerUpdate(
            symbol="CLM6-NYMEX",
            bid=90.10,
            bid_qty=3.0,
            ask=90.13,
            ask_qty=4.0,
            last=90.11,
            volume=148427.0,
            vwap=91.2,
            low=88.7,
            high=94.81,
            change=-3.05,
            change_pct=-3.27,
        )
        client._tick_queue.put_nowait(ticker)

        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
            return_value="CLM6.NYMEX",
        ):
            items = []
            async for item in client.subscribe_ticks(["CLM6-NYMEX"]):
                items.append(item)
                break
        assert len(items) == 1
        assert items[0].symbol == "CLM6-NYMEX"
        mock_ws.subscribe.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_subscribe_ticks_handles_timeout(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Handle queue timeout when no tickers available initially.

        Given: WS client connected but tick_queue is empty initially,
        When: subscribe_ticks iterator is consumed,
        Then: Loops through timeout, then yields when item arrives.
        """
        mock_ws = AsyncMock()
        client._ws_client = mock_ws
        client._ensure_ws_connected = AsyncMock()
        ticker = TickerUpdate(
            symbol="CLM6-NYMEX",
            bid=90.10,
            bid_qty=3.0,
            ask=90.13,
            ask_qty=4.0,
            last=90.11,
            volume=148427.0,
            vwap=91.2,
            low=88.7,
            high=94.81,
            change=-3.05,
            change_pct=-3.27,
        )

        async def delayed_put() -> None:
            await asyncio.sleep(0.15)
            client._tick_queue.put_nowait(ticker)

        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
            return_value="CLM6.NYMEX",
        ):
            asyncio.create_task(delayed_put())
            items = []
            async for item in client.subscribe_ticks(["CLM6-NYMEX"]):
                items.append(item)
                break
        assert len(items) == 1

    @pytest.mark.asyncio
    async def test_subscribe_ticks_unsubscribes_on_break(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Cleanup calls subscribe (unsubscribe) when consumer breaks out.

        Given: WS client connected and tick_queue has an item,
        When: Consumer breaks out of iterator and aclose is called,
        Then: Cleanup subscribe is invoked.
        """
        mock_ws = AsyncMock()
        client._ws_client = mock_ws
        client._ensure_ws_connected = AsyncMock()
        ticker = TickerUpdate(
            symbol="CLM6-NYMEX",
            bid=90.10,
            bid_qty=3.0,
            ask=90.13,
            ask_qty=4.0,
            last=90.11,
            volume=148427.0,
            vwap=91.2,
            low=88.7,
            high=94.81,
            change=-3.05,
            change_pct=-3.27,
        )
        client._tick_queue.put_nowait(ticker)

        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
            return_value="CLM6.NYMEX",
        ):
            gen = client.subscribe_ticks(["CLM6-NYMEX"])
            async for _ in gen:
                break
            await gen.aclose()
        assert mock_ws.subscribe.await_count == 2

    @pytest.mark.asyncio
    async def test_subscribe_ticks_cleanup_failure_handled(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Handle cleanup subscribe failure gracefully.

        Given: WS client raises on second subscribe (cleanup),
        When: Consumer breaks out of iterator,
        Then: No exception propagated.
        """
        call_count = 0

        def subscribe_side_effect(**kwargs: dict[str, object]) -> None:
            nonlocal call_count
            call_count += 1
            if call_count > 1:
                raise RuntimeError("unsubscribe failed")

        mock_ws = AsyncMock()
        mock_ws.subscribe = AsyncMock(side_effect=subscribe_side_effect)
        client._ws_client = mock_ws
        client._ensure_ws_connected = AsyncMock()
        ticker = TickerUpdate(
            symbol="CLM6-NYMEX",
            bid=90.10,
            bid_qty=3.0,
            ask=90.13,
            ask_qty=4.0,
            last=90.11,
            volume=148427.0,
            vwap=91.2,
            low=88.7,
            high=94.81,
            change=-3.05,
            change_pct=-3.27,
        )
        client._tick_queue.put_nowait(ticker)

        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
            return_value="CLM6.NYMEX",
        ):
            gen = client.subscribe_ticks(["CLM6-NYMEX"])
            async for _ in gen:
                break
            await gen.aclose()

    @pytest.mark.asyncio
    async def test_subscribe_ticks_raises_when_ws_none_after_connect(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Raise RuntimeError if _ws_client is None after _ensure_ws_connected.

        Given: _ensure_ws_connected is a no-op that does not set _ws_client,
        When: subscribe_ticks is called,
        Then: RuntimeError is raised.
        """
        client._ensure_ws_connected = AsyncMock()
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
                return_value="CLM6.NYMEX",
            ),
            pytest.raises(RuntimeError, match="WebSocket client not connected"),
        ):
            await anext(aiter(client.subscribe_ticks(["CLM6-NYMEX"])))


class TestSubscribeTrades:
    """Tests for subscribe_trades async iterator."""

    @pytest.mark.asyncio
    async def test_subscribe_trades_yields_from_queue(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Yield trade updates from internal queue.

        Given: WS client connected and trade_queue has an item,
        When: subscribe_trades iterator is consumed,
        Then: Yields the queued TradeUpdate.
        """
        mock_ws = AsyncMock()
        client._ws_client = mock_ws
        client._ensure_ws_connected = AsyncMock()
        trade = TradeUpdate(
            symbol="CLM6-NYMEX",
            side="buy",
            quantity=1.0,
            price=90.12,
            ord_type="fill",
            timestamp=MagicMock(),
            trade_id="7623847138430121307",
        )
        client._trade_queue.put_nowait(trade)

        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
            return_value="CLM6.NYMEX",
        ):
            items = []
            async for item in client.subscribe_trades(["CLM6-NYMEX"]):
                items.append(item)
                break
        assert len(items) == 1
        assert items[0].symbol == "CLM6-NYMEX"
        mock_ws.subscribe.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_subscribe_trades_handles_timeout(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Handle queue timeout when no trades available initially.

        Given: WS client connected but trade_queue is empty initially,
        When: subscribe_trades iterator is consumed,
        Then: Loops through timeout, then yields when item arrives.
        """
        mock_ws = AsyncMock()
        client._ws_client = mock_ws
        client._ensure_ws_connected = AsyncMock()
        trade = TradeUpdate(
            symbol="CLM6-NYMEX",
            side="buy",
            quantity=1.0,
            price=90.12,
            ord_type="fill",
            timestamp=MagicMock(),
            trade_id="7623847138430121307",
        )

        async def delayed_put() -> None:
            await asyncio.sleep(0.15)
            client._trade_queue.put_nowait(trade)

        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
            return_value="CLM6.NYMEX",
        ):
            asyncio.create_task(delayed_put())
            items = []
            async for item in client.subscribe_trades(["CLM6-NYMEX"]):
                items.append(item)
                break
        assert len(items) == 1

    @pytest.mark.asyncio
    async def test_subscribe_trades_unsubscribes_on_break(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Cleanup calls subscribe (unsubscribe) when consumer breaks out.

        Given: WS client connected and trade_queue has an item,
        When: Consumer breaks out of iterator and aclose is called,
        Then: Cleanup subscribe is invoked.
        """
        mock_ws = AsyncMock()
        client._ws_client = mock_ws
        client._ensure_ws_connected = AsyncMock()
        trade = TradeUpdate(
            symbol="CLM6-NYMEX",
            side="buy",
            quantity=1.0,
            price=90.12,
            ord_type="fill",
            timestamp=MagicMock(),
            trade_id="7623847138430121307",
        )
        client._trade_queue.put_nowait(trade)

        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
            return_value="CLM6.NYMEX",
        ):
            gen = client.subscribe_trades(["CLM6-NYMEX"])
            async for _ in gen:
                break
            await gen.aclose()
        assert mock_ws.subscribe.await_count == 2

    @pytest.mark.asyncio
    async def test_subscribe_trades_cleanup_failure_handled(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Handle cleanup subscribe failure gracefully.

        Given: WS client raises on second subscribe (cleanup),
        When: Consumer breaks out of trade iterator,
        Then: No exception propagated.
        """
        call_count = 0

        def subscribe_side_effect(**kwargs: dict[str, object]) -> None:
            nonlocal call_count
            call_count += 1
            if call_count > 1:
                raise RuntimeError("unsubscribe failed")

        mock_ws = AsyncMock()
        mock_ws.subscribe = AsyncMock(side_effect=subscribe_side_effect)
        client._ws_client = mock_ws
        client._ensure_ws_connected = AsyncMock()
        trade = TradeUpdate(
            symbol="CLM6-NYMEX",
            side="buy",
            quantity=1.0,
            price=90.12,
            ord_type="fill",
            timestamp=MagicMock(),
            trade_id="7623847138430121307",
        )
        client._trade_queue.put_nowait(trade)

        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
            return_value="CLM6.NYMEX",
        ):
            gen = client.subscribe_trades(["CLM6-NYMEX"])
            async for _ in gen:
                break
            await gen.aclose()

    @pytest.mark.asyncio
    async def test_subscribe_trades_raises_when_ws_none_after_connect(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Raise RuntimeError if _ws_client is None after _ensure_ws_connected.

        Given: _ensure_ws_connected is a no-op that does not set _ws_client,
        When: subscribe_trades is called,
        Then: RuntimeError is raised.
        """
        client._ensure_ws_connected = AsyncMock()
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
                return_value="CLM6.NYMEX",
            ),
            pytest.raises(RuntimeError, match="WebSocket client not connected"),
        ):
            await anext(aiter(client.subscribe_trades(["CLM6-NYMEX"])))


class TestSubscribeInstruments:
    """Tests for subscribe_instruments async iterator."""

    @pytest.mark.asyncio
    async def test_subscribe_instruments_yields_all(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Yield all instruments from REST API.

        Given: _fetch_instruments_rest returns 2 instruments,
        When: subscribe_instruments is iterated,
        Then: Yields 2 raw instrument dicts.
        """
        instruments = [
            {"symbol": "CLM6.NYMEX", "tradable": True, "status": "active"},
            {"symbol": "GCQ6.COMEX", "tradable": True, "status": "active"},
        ]
        client._fetch_instruments_rest = AsyncMock(return_value=instruments)
        results = []
        async for inst in client.subscribe_instruments():
            results.append(inst)
        assert len(results) == 2
        assert results[0]["symbol"] == "CLM6.NYMEX"
        assert results[1]["symbol"] == "GCQ6.COMEX"

    @pytest.mark.asyncio
    async def test_subscribe_instruments_empty(self, client: KrakenEquitiesExchangeClient) -> None:
        """Yield nothing when no instruments available.

        Given: _fetch_instruments_rest returns empty list,
        When: subscribe_instruments is iterated,
        Then: No items yielded.
        """
        client._fetch_instruments_rest = AsyncMock(return_value=[])
        results = []
        async for inst in client.subscribe_instruments():
            results.append(inst)
        assert len(results) == 0


class TestGetInstrumentsSync:
    """Tests for get_instruments_sync synchronous REST fetch."""

    def test_get_instruments_sync_filters_active(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Fetch and filter only active tradable contracts synchronously.

        Given: REST response with 3 contracts (2 active+tradable, 1 inactive),
        When: get_instruments_sync is called,
        Then: Returns only the 2 active tradable contracts.
        """
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "result": {
                "data": [
                    {"symbol": "CLM6.NYMEX", "tradable": True, "status": "active"},
                    {"symbol": "GCQ6.COMEX", "tradable": True, "status": "active"},
                    {"symbol": "CLK5.NYMEX", "tradable": False, "status": "inactive"},
                ]
            }
        }
        mock_response.raise_for_status = MagicMock()

        mock_http_client = MagicMock()
        mock_http_client.get.return_value = mock_response
        mock_http_client.__enter__ = MagicMock(return_value=mock_http_client)
        mock_http_client.__exit__ = MagicMock(return_value=False)

        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.httpx.Client",
            return_value=mock_http_client,
        ):
            result = client.get_instruments_sync()
        assert len(result) == 2
        assert result[0]["symbol"] == "CLM6.NYMEX"
        assert result[1]["symbol"] == "GCQ6.COMEX"

    def test_get_instruments_sync_empty_result(self, client: KrakenEquitiesExchangeClient) -> None:
        """Return empty list when no active contracts exist.

        Given: REST response with no active tradable contracts,
        When: get_instruments_sync is called,
        Then: Returns empty list.
        """
        mock_response = MagicMock()
        mock_response.json.return_value = {"result": {"data": []}}
        mock_response.raise_for_status = MagicMock()

        mock_http_client = MagicMock()
        mock_http_client.get.return_value = mock_response
        mock_http_client.__enter__ = MagicMock(return_value=mock_http_client)
        mock_http_client.__exit__ = MagicMock(return_value=False)

        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.httpx.Client",
            return_value=mock_http_client,
        ):
            result = client.get_instruments_sync()
        assert len(result) == 0


class TestTimeframeToInterval:
    """Tests for the ``_timeframe_to_interval`` pure helper."""

    @pytest.mark.parametrize(
        ("timeframe", "expected"),
        [
            ("1m", 1),
            ("5m", 5),
            ("15m", 15),
            ("30m", 30),
            ("1h", 60),
            ("1d", 1440),
        ],
    )
    def test_supported_timeframe_maps_to_minutes(self, timeframe: str, expected: int) -> None:
        """Accept every documented timeframe and return its minute-interval.

        Given: a timeframe string from the iapi-probed support set,
        When: ``_timeframe_to_interval`` is called,
        Then: the corresponding integer minute value is returned.
        """
        assert _timeframe_to_interval(timeframe) == expected

    def test_unknown_timeframe_raises_valueerror_with_allowed_list(self) -> None:
        """Reject unsupported timeframes with a message listing allowed values.

        Given: a timeframe string not in the support set,
        When: ``_timeframe_to_interval`` is called,
        Then: ``ValueError`` is raised and the message enumerates the accepted values.
        """
        with pytest.raises(ValueError, match="Unsupported Kraken Equities timeframe '2h'"):
            _timeframe_to_interval("2h")
        with pytest.raises(ValueError, match=r"allowed: .*1m.*"):
            _timeframe_to_interval("bogus")


class TestGetOhlcv:
    """Tests for ``KrakenEquitiesExchangeClient.get_ohlcv``.

    All tests patch ``native_to_kraken_equities_ws`` to bypass the DB-backed
    symbol mapper, and stub ``httpx.AsyncClient`` to return a canned payload
    mirroring the live iapi shape probed 2026-04-21.
    """

    @staticmethod
    def _mock_httpx(payload: dict[str, object]) -> MagicMock:
        """Build a MagicMock satisfying the async-context-manager + get protocol."""
        mock_response = MagicMock()
        mock_response.json.return_value = payload
        mock_response.raise_for_status = MagicMock()
        mock_http_client = AsyncMock()
        mock_http_client.get.return_value = mock_response
        mock_http_client.__aenter__ = AsyncMock(return_value=mock_http_client)
        mock_http_client.__aexit__ = AsyncMock(return_value=False)
        return mock_http_client

    @pytest.mark.asyncio
    async def test_returns_ordered_snapshots_from_happy_path_payload(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Map live iapi rows into OhlcvSnapshot entries, preserving order.

        Given: a canned iapi payload with two rows,
        When: ``get_ohlcv`` is called,
        Then: two ``OhlcvSnapshot`` entries are returned with coerced floats.
        """
        payload = {
            "result": {
                "data": [
                    {
                        "time": 1776556800,
                        "open": "26600.5",
                        "high": "26650.0",
                        "low": "26580.25",
                        "close": "26620.0",
                        "volume_wap": "26610",
                        "volume": "1234",
                        "count": 42,
                    },
                    {
                        "time": 1776643200,
                        "open": "26658.25",
                        "high": "26826.0",
                        "low": "26569.0",
                        "close": "26797.0",
                        "volume_wap": "26706.44",
                        "volume": "1638087",
                        "count": 1403613,
                    },
                ]
            }
        }
        mock_http = self._mock_httpx(payload)
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.httpx.AsyncClient",
                return_value=mock_http,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
                return_value="MNQM6.CME",
            ),
        ):
            snapshots = await client.get_ohlcv("MNQM6-CME", timeframe="1d")
        assert len(snapshots) == 2
        assert snapshots[0] == OhlcvSnapshot(
            timestamp=1776556800.0,
            open=26600.5,
            high=26650.0,
            low=26580.25,
            close=26620.0,
            volume=1234.0,
        )
        assert snapshots[-1].close == 26797.0
        call = mock_http.get.call_args
        assert call.args[0].endswith("/markets/MNQM6.CME/ticker/history")
        assert call.kwargs["params"] == {
            "interval": "1440",
            "delayed": "true",
            "asset_class": "futures_contract",
        }

    @pytest.mark.asyncio
    async def test_empty_payload_returns_empty_list(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Return an empty list when the payload has no data rows.

        Given: an iapi payload with ``result.data`` empty,
        When: ``get_ohlcv`` is called,
        Then: an empty list is returned.
        """
        mock_http = self._mock_httpx({"result": {"data": []}})
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.httpx.AsyncClient",
                return_value=mock_http,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
                return_value="MNQM6.CME",
            ),
        ):
            snapshots = await client.get_ohlcv("MNQM6-CME", timeframe="1h")
        assert snapshots == []

    @pytest.mark.asyncio
    async def test_application_error_envelope_raises(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Raise when the 200-response body signals application-layer failure.

        Given: an iapi payload with ``result=null`` + non-empty ``errors`` —
            the shape the endpoint returns for "Unknown method" or other
            upstream failures that still produce HTTP 200 (observed live
            2026-04-21 when the endpoint is called without the required
            Origin/Referer headers),
        When: ``get_ohlcv`` is called,
        Then: ``RuntimeError`` is raised so callers do not silently observe
            the failure as an empty candle window. Prevents historical
            backfill from skipping rows it should have retried.
        """
        mock_http = self._mock_httpx({"result": None, "errors": [{"msg": "transient"}]})
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.httpx.AsyncClient",
                return_value=mock_http,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
                return_value="MNQM6.CME",
            ),
            pytest.raises(RuntimeError, match="Kraken Equities ticker/history failure"),
        ):
            await client.get_ohlcv("MNQM6-CME")

    @pytest.mark.asyncio
    async def test_errors_present_with_result_still_raises(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Raise even when ``result`` is non-null if ``errors`` is non-empty.

        Given: an iapi payload with both ``result.data`` populated and a
            non-empty ``errors`` array (partial-failure shape),
        When: ``get_ohlcv`` is called,
        Then: ``RuntimeError`` is raised. Partial-failure responses are
            treated as outright failures rather than silently returning
            the partial data.
        """
        payload = {
            "result": {
                "data": [
                    {
                        "time": 1,
                        "open": "1",
                        "high": "1",
                        "low": "1",
                        "close": "1",
                        "volume_wap": "1",
                        "volume": "0",
                        "count": 0,
                    }
                ]
            },
            "errors": [{"msg": "one feed failed"}],
        }
        mock_http = self._mock_httpx(payload)
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.httpx.AsyncClient",
                return_value=mock_http,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
                return_value="MNQM6.CME",
            ),
            pytest.raises(RuntimeError, match="Kraken Equities ticker/history failure"),
        ):
            await client.get_ohlcv("MNQM6-CME")

    @pytest.mark.asyncio
    async def test_http_error_propagates(self, client: KrakenEquitiesExchangeClient) -> None:
        """Propagate ``httpx.HTTPStatusError`` so callers can distinguish failures.

        Given: an iapi response whose ``raise_for_status`` raises,
        When: ``get_ohlcv`` is called,
        Then: the exception escapes the method.
        """
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock(
            side_effect=httpx.HTTPStatusError(
                "503", request=MagicMock(), response=MagicMock(status_code=503)
            )
        )
        mock_http_client = AsyncMock()
        mock_http_client.get.return_value = mock_response
        mock_http_client.__aenter__ = AsyncMock(return_value=mock_http_client)
        mock_http_client.__aexit__ = AsyncMock(return_value=False)
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.httpx.AsyncClient",
                return_value=mock_http_client,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
                return_value="MNQM6.CME",
            ),
            pytest.raises(httpx.HTTPStatusError),
        ):
            await client.get_ohlcv("MNQM6-CME")

    @pytest.mark.asyncio
    async def test_since_filter_drops_older_candles(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Apply the ``since`` millisecond filter client-side.

        Given: a 3-row payload spanning 3 daily candles,
        When: ``get_ohlcv`` is called with ``since`` after the first row,
        Then: only rows at or after ``since`` remain.
        """
        payload = {
            "result": {
                "data": [
                    {
                        "time": 1000,
                        "open": "1",
                        "high": "1",
                        "low": "1",
                        "close": "1",
                        "volume_wap": "1",
                        "volume": "0",
                        "count": 0,
                    },
                    {
                        "time": 2000,
                        "open": "2",
                        "high": "2",
                        "low": "2",
                        "close": "2",
                        "volume_wap": "2",
                        "volume": "0",
                        "count": 0,
                    },
                    {
                        "time": 3000,
                        "open": "3",
                        "high": "3",
                        "low": "3",
                        "close": "3",
                        "volume_wap": "3",
                        "volume": "0",
                        "count": 0,
                    },
                ]
            }
        }
        mock_http = self._mock_httpx(payload)
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.httpx.AsyncClient",
                return_value=mock_http,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
                return_value="MNQM6.CME",
            ),
        ):
            snapshots = await client.get_ohlcv("MNQM6-CME", since=2_000_000)
        assert [s.timestamp for s in snapshots] == [2000.0, 3000.0]

    @pytest.mark.asyncio
    async def test_limit_truncates_from_tail(self, client: KrakenEquitiesExchangeClient) -> None:
        """Keep the newest ``limit`` rows when the server returns more.

        Given: a 3-row payload,
        When: ``get_ohlcv`` is called with ``limit=2``,
        Then: only the two most-recent (tail) rows are returned.
        """
        payload = {
            "result": {
                "data": [
                    {
                        "time": ts,
                        "open": "1",
                        "high": "1",
                        "low": "1",
                        "close": "1",
                        "volume_wap": "1",
                        "volume": "0",
                        "count": 0,
                    }
                    for ts in (1000, 2000, 3000)
                ]
            }
        }
        mock_http = self._mock_httpx(payload)
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.httpx.AsyncClient",
                return_value=mock_http,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
                return_value="MNQM6.CME",
            ),
        ):
            snapshots = await client.get_ohlcv("MNQM6-CME", limit=2)
        assert [s.timestamp for s in snapshots] == [2000.0, 3000.0]

    @pytest.mark.asyncio
    async def test_unparseable_row_skipped_not_raised(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Log-and-skip rows with missing or invalid fields.

        Given: a 2-row payload where the first row is missing ``open``,
        When: ``get_ohlcv`` is called,
        Then: only the second row is returned; no exception escapes.
        """
        payload = {
            "result": {
                "data": [
                    {"time": 1000, "close": "1"},
                    {
                        "time": 2000,
                        "open": "2",
                        "high": "2",
                        "low": "2",
                        "close": "2",
                        "volume_wap": "2",
                        "volume": "0",
                        "count": 0,
                    },
                ]
            }
        }
        mock_http = self._mock_httpx(payload)
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.httpx.AsyncClient",
                return_value=mock_http,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
                return_value="MNQM6.CME",
            ),
        ):
            snapshots = await client.get_ohlcv("MNQM6-CME")
        assert [s.timestamp for s in snapshots] == [2000.0]

    @pytest.mark.asyncio
    async def test_unknown_timeframe_raises_valueerror(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Reject unsupported timeframes before any HTTP call is made.

        Given: an unsupported timeframe string,
        When: ``get_ohlcv`` is called,
        Then: ``ValueError`` is raised and no request is issued.
        """
        with pytest.raises(ValueError, match="Unsupported Kraken Equities timeframe"):
            await client.get_ohlcv("MNQM6-CME", timeframe="bogus")

    @pytest.mark.asyncio
    async def test_unknown_symbol_raises_without_http_call(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Symbol-mapper errors are surfaced before any HTTP call is made.

        Given: the symbol mapper raises ValueError for an unmapped symbol,
        When: ``get_ohlcv`` is called,
        Then: the exception escapes and ``httpx.AsyncClient`` is never touched.
        """
        mock_http_client = AsyncMock()
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.httpx.AsyncClient",
                return_value=mock_http_client,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
                side_effect=ValueError("Unknown native symbol: BOGUS-XX"),
            ),
            pytest.raises(ValueError, match="Unknown native symbol"),
        ):
            await client.get_ohlcv("BOGUS-XX")
        mock_http_client.get.assert_not_called()


class TestWsEnvelopeDelayedPropagation:
    """Tests that the outer WS envelope's ``delayed`` flag reaches the adapter."""

    @pytest.mark.asyncio
    async def test_envelope_delayed_true_flows_into_adapter(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Forward ``message['delayed']=True`` as ``envelope_delayed=True``.

        Given: a WS frame with ``delayed=True`` at the envelope level,
        When: ``_on_ws_message`` dispatches the ticker item,
        Then: ``parse_kraken_equities_ticker`` is invoked with
              ``envelope_delayed=True``.
        """
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.parse_kraken_equities_ticker",
            return_value=TickerUpdate(
                symbol="MNQM6-CME",
                bid=0.0,
                bid_qty=0.0,
                ask=0.0,
                ask_qty=0.0,
                last=0.0,
                volume=0.0,
                vwap=0.0,
                low=0.0,
                high=0.0,
                change=0.0,
                change_pct=0.0,
                is_delayed=True,
            ),
        ) as parse_mock:
            msg = {
                "channel": "ticker",
                "type": "update",
                "delayed": True,
                "data": [{"symbol": "MNQM6.CME"}],
            }
            await client._on_ws_message(msg)
        parse_mock.assert_called_once()
        assert parse_mock.call_args.kwargs == {"envelope_delayed": True}

    @pytest.mark.asyncio
    async def test_missing_envelope_delayed_defaults_to_false(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Default envelope-level delayed to False when the key is absent.

        Given: a WS frame without the ``delayed`` key,
        When: ``_on_ws_message`` dispatches the ticker item,
        Then: the adapter receives ``envelope_delayed=False``.
        """
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.parse_kraken_equities_ticker",
            return_value=TickerUpdate(
                symbol="MNQM6-CME",
                bid=0.0,
                bid_qty=0.0,
                ask=0.0,
                ask_qty=0.0,
                last=0.0,
                volume=0.0,
                vwap=0.0,
                low=0.0,
                high=0.0,
                change=0.0,
                change_pct=0.0,
            ),
        ) as parse_mock:
            msg = {
                "channel": "ticker",
                "type": "update",
                "data": [{"symbol": "MNQM6.CME"}],
            }
            await client._on_ws_message(msg)
        assert parse_mock.call_args.kwargs == {"envelope_delayed": False}

    @pytest.mark.asyncio
    async def test_non_bool_envelope_delayed_coerces_to_false(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Reject non-bool ``delayed`` payloads and default to False.

        Given: a WS frame whose ``delayed`` key is a truthy string (``"false"``)
            under a hypothetical schema drift,
        When: ``_on_ws_message`` dispatches the ticker item,
        Then: the adapter receives ``envelope_delayed=False`` rather than
            ``bool("false") == True``. Prevents a permissive ``bool(...)``
            coercion from mis-flagging live ticks as delayed under an
            unexpected wire shape.
        """
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.parse_kraken_equities_ticker",
            return_value=TickerUpdate(
                symbol="MNQM6-CME",
                bid=0.0,
                bid_qty=0.0,
                ask=0.0,
                ask_qty=0.0,
                last=0.0,
                volume=0.0,
                vwap=0.0,
                low=0.0,
                high=0.0,
                change=0.0,
                change_pct=0.0,
            ),
        ) as parse_mock:
            msg = {
                "channel": "ticker",
                "type": "update",
                "delayed": "false",
                "data": [{"symbol": "MNQM6.CME"}],
            }
            await client._on_ws_message(msg)
        assert parse_mock.call_args.kwargs == {"envelope_delayed": False}


class TestConnectRaceHardening:
    """Equities parity tests for the shared Kraken connect-race hardening."""

    @pytest.mark.asyncio
    async def test_ensure_ws_connected_coalesces_concurrent_callers(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Concurrent connect callers build exactly one SpotWSClient.

        Given: Two concurrent _ensure_ws_connected callers and a slow start,
        When: Both run,
        Then: Only one client is constructed — the second caller parks on
            the connect lock and adopts the first one's client.
        """
        started = asyncio.Event()
        release = asyncio.Event()

        async def _slow_start() -> None:
            started.set()
            await release.wait()

        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.SpotWSClient"
        ) as ws_cls:
            ws_cls.return_value.start = _slow_start
            first = asyncio.create_task(client._ensure_ws_connected())
            await started.wait()
            second = asyncio.create_task(client._ensure_ws_connected())
            await asyncio.sleep(0.01)
            release.set()
            await asyncio.gather(first, second)
        assert ws_cls.call_count == 1

    @pytest.mark.asyncio
    async def test_disconnect_does_not_clobber_newer_client(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Disconnect leaves a newer concurrently-installed client alone.

        Given: A close() during which a concurrent path installs a NEW client,
        When: disconnect finishes,
        Then: The new client remains in the slot (compare-and-clear).
        """
        newer = AsyncMock()
        old = AsyncMock()

        async def _close_and_install() -> None:
            client._ws_client = newer

        old.close = AsyncMock(side_effect=_close_and_install)
        client._ws_client = old
        await client.disconnect()
        assert client._ws_client is newer

    @pytest.mark.asyncio
    async def test_replay_aborts_when_client_swapped_mid_replay(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Equities replay raises when the client slot is swapped mid-replay.

        Given: Two cached bulk requests and a subscribe that swaps the slot,
        When: _replay_subscriptions runs,
        Then: It raises after the first send instead of continuing against
            the wrong connection.
        """
        ws = AsyncMock()

        async def _swap(**_kwargs: object) -> None:
            client._ws_client = AsyncMock()

        ws.subscribe = AsyncMock(side_effect=_swap)
        client._ws_client = ws
        first = SubscriptionRequest(channel="ticker", symbols=("AAPL.US",), parameters_json="{}")
        second = SubscriptionRequest(channel="trade", symbols=("AAPL.US",), parameters_json="{}")
        client._subscription_cache[first.key()] = first
        client._subscription_cache[second.key()] = second
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.asyncio.sleep",
                new_callable=AsyncMock,
            ),
            pytest.raises(RuntimeError, match="replaced during subscription replay"),
        ):
            await client._replay_subscriptions()
        ws.subscribe.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_ensure_ws_connected_detects_ownership_loss(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """An equities connect raced by a disconnect closes its own client.

        Given: An empty replay cache and a start() during which a concurrent
            path nulls the slot,
        When: _ensure_ws_connected finishes starting,
        Then: It raises the ownership-lost error and closes the DISOWNED
            client directly (slot-scoped disconnect would no-op and leak).
        """

        async def _start_while_disconnected() -> None:
            client._ws_client = None

        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.SpotWSClient"
            ) as ws_cls,
            patch.object(client, "disconnect", new_callable=AsyncMock) as slot_disconnect,
        ):
            ws_cls.return_value.start = _start_while_disconnected
            ws_cls.return_value.close = AsyncMock()
            with pytest.raises(RuntimeError, match="replaced during connect"):
                await client._ensure_ws_connected()
            ws_cls.return_value.close.assert_awaited_once()
            slot_disconnect.assert_not_awaited()
        assert client._ws_client is None

    @pytest.mark.asyncio
    async def test_disowned_close_error_is_swallowed(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """A failing disowned close never masks the ownership error.

        Given: An ownership-lost connect whose direct close also raises,
        When: _ensure_ws_connected tears down,
        Then: The close error is swallowed and the ownership RuntimeError
            propagates.
        """

        async def _start_while_disconnected() -> None:
            client._ws_client = None

        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.SpotWSClient"
        ) as ws_cls:
            ws_cls.return_value.start = _start_while_disconnected
            ws_cls.return_value.close = AsyncMock(side_effect=RuntimeError("close fail"))
            with pytest.raises(RuntimeError, match="replaced during connect"):
                await client._ensure_ws_connected()
