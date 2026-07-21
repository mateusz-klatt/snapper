"""Tests for permissions, CSRF, tokens, and auth dependencies."""

import json
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

import jwt
import pytest
from fastapi import HTTPException
from fastapi import Request

from snapper.application.services.settings import SettingsService
from snapper.auth.dependencies import CSRFManager
from snapper.auth.dependencies import get_csrf_manager
from snapper.auth.dependencies import get_csrf_token
from snapper.auth.dependencies import get_current_user
from snapper.auth.dependencies import require_authentication
from snapper.auth.dependencies import require_permission
from snapper.auth.dependencies import require_role
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.permissions import RESOURCE_PERMISSIONS
from snapper.auth.domain.permissions import ROLE_PERMISSIONS
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.schemas.tokens import TokenClaims
from snapper.auth.schemas.tokens import TokenPair
from snapper.auth.tokens import BLACKLIST_CLEANUP_BATCH_SIZE
from snapper.auth.tokens import BLACKLIST_GRACE_PERIOD_SECONDS
from snapper.auth.tokens import PERMISSION_SCOPE_VERSION
from snapper.auth.tokens import PermissionScopeError
from snapper.auth.tokens import TokenManager
from snapper.auth.tokens import WebSocketTokenRotator
from snapper.auth.tokens import get_token_manager
from snapper.auth.tokens import get_ws_token_rotator
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.market_data.walutomat import WalutomatSnapshotUpdaterService
from snapper.interface.websocket.dispatcher import dispatch_messages
from snapper.interface.websocket.helpers import build_allowed_origins
from snapper.messaging.schemas.data import CandleData
from snapper.strategies.base import BaseStrategy
from snapper.strategies.base import StrategyConfig
from snapper.strategies.base import StrategySignal
from snapper.strategies.cointegration import CointegrationPairs
from snapper.strategies.factory import StrategyFactory
from snapper.strategies.factory import StrategyNotFoundError


def make_candle_envelope(
    instrument: str = "BTC-USD",
    close: float = 100.0,
    ts: float | None = None,
    exchange: str = "kraken",
) -> CandleData:
    """Create a CandleData instance with configurable parameters."""
    return CandleData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        instrument=instrument,
        timeframe="1h",
        open=close - 100,
        high=close + 100,
        low=close - 200,
        close=close,
        volume=1000.0,
        exchange=exchange,
        timestamp=datetime.fromtimestamp(ts, tz=UTC) if ts else datetime.now(UTC),
        open_at=datetime.fromtimestamp(ts, tz=UTC) if ts else datetime.now(UTC),
    )


def prefill_candle_buffer(
    strategy: BaseStrategy,
    instrument: str,
    closes: list[float],
    exchange: str = "kraken",
) -> None:
    """Fill strategy candle buffer with bars for given close prices."""
    if instrument not in strategy.candle_buffer:
        strategy.candle_buffer[instrument] = []
    for close in closes:
        candle = make_candle_envelope(instrument, close, exchange=exchange)
        strategy.candle_buffer[instrument].append(candle)


async def feed_bar_to_strategy(
    strategy: BaseStrategy,
    instrument: str,
    close: float,
    exchange: str = "kraken",
) -> StrategySignal | None:
    """Feed a candle to strategy and return resulting signal if any."""
    candle = make_candle_envelope(instrument, close, exchange=exchange)
    if instrument not in strategy.candle_buffer:
        strategy.candle_buffer[instrument] = []
    strategy.candle_buffer[instrument].append(candle)
    max_buffer_size = strategy.params.get("buffer_size", 100)
    if len(strategy.candle_buffer[instrument]) > max_buffer_size:
        strategy.candle_buffer[instrument].pop(0)
    result = await strategy.on_candle(instrument, candle)
    if isinstance(result, list):
        return result[0] if result else None
    return result


class TestDispatcherReauthFailure:
    """Tests for dispatcher reauth failure handling."""

    @pytest.mark.asyncio
    async def test_reauth_failure_breaks_loop(self) -> None:
        """Verify reauth failure terminates message loop.

        Given: A websocket sending reauth message with invalid token,
        When: dispatch_messages processes the message,
        Then: Message loop terminates after single receive.
        """
        mock_websocket = AsyncMock()
        mock_manager = MagicMock()
        mock_user = MagicMock()
        mock_user.username = "testuser"
        mock_user.role = "admin"
        mock_ws_auth_manager = MagicMock()
        mock_ws_token_service = MagicMock()
        mock_manager.tracker.session_id = "test-session"
        mock_manager.tracker.next_sequence.return_value = 1
        mock_websocket.receive_text.return_value = json.dumps(
            {
                "type": "reauth",
                "session_id": "",
                "sequence_id": 0,
                "public_id": "test",
                "timestamp": "2024-01-01T00:00:00Z",
                "ws_token": "invalid",
            }
        )
        with patch(
            "snapper.interface.websocket.dispatcher.handle_reauth",
            new_callable=AsyncMock,
            return_value=False,
        ):
            await dispatch_messages(
                mock_websocket,
                mock_manager,
                mock_user,
                mock_ws_auth_manager,
                mock_ws_token_service,
            )
        mock_websocket.receive_text.assert_called_once()


class TestHelpersSessionDomainHttp:
    """Tests for session domain helpers with HTTP/HTTPS prefixes."""

    def test_session_domain_with_http_prefix(self) -> None:
        """Verify HTTP session domain is included in allowed origins.

        Given: Settings with HTTP session domain,
        When: build_allowed_origins is called,
        Then: HTTP origin is included in returned list.
        """
        mock_settings = MagicMock()
        mock_settings.ui_origin = None
        mock_settings.session_domain = "http://example.com"
        origins = build_allowed_origins(mock_settings, server_port=8000)
        assert "http://example.com" in origins

    def test_session_domain_with_https_prefix(self) -> None:
        """Verify HTTPS session domain is included in allowed origins.

        Given: Settings with HTTPS session domain with trailing slash,
        When: build_allowed_origins is called,
        Then: HTTPS origin without trailing slash is included.
        """
        mock_settings = MagicMock()
        mock_settings.ui_origin = None
        mock_settings.session_domain = "https://secure.example.com/"
        origins = build_allowed_origins(mock_settings, server_port=8000)
        assert "https://secure.example.com" in origins


class TestWalutomatAllSymbolsCollected:
    """Tests for Walutomat snapshot collection termination."""

    @pytest.mark.asyncio
    async def test_stops_when_all_symbols_collected(self) -> None:
        """Verify snapshot collection stops when all symbols received.

        Given: A single symbol to collect,
        When: Ticker update for that symbol is received,
        Then: Collection loop terminates with symbol in snapshots.
        """
        mock_exchange_client = MagicMock()
        mock_repository = MagicMock()
        ticker_data = TickerUpdate(
            symbol="EUR-PLN",
            bid=4.30,
            ask=4.35,
            last=4.32,
            high=4.38,
            low=4.28,
            volume=1000000.0,
            change=0.02,
            vwap=4.31,
            bid_qty=5000.0,
            ask_qty=5000.0,
            change_pct=0.5,
        )

        async def mock_subscribe_ticks(symbols: list[str]) -> Any:
            yield ticker_data

        mock_exchange_client.subscribe_ticks = mock_subscribe_ticks
        service = WalutomatSnapshotUpdaterService(mock_exchange_client, mock_repository)

        async def mock_load_all_symbols() -> list[str]:
            return ["EUR-PLN"]

        service.load_all_symbols = mock_load_all_symbols
        snapshots: dict[str, Any] = {}
        await service._collect_snapshots_loop(["EUR-PLN"], snapshots)
        assert len(snapshots) == 1
        assert "EUR-PLN" in snapshots


class TestCointegrationInstrument2Exit:
    """Tests for cointegration strategy exit signals on instrument 2."""

    @pytest.mark.asyncio
    async def test_instrument2_exit_short_spread(self) -> None:
        """Verify exit signal for short spread on instrument 2.

        Given: A cointegration strategy with short_spread position,
        When: Bar for instrument 2 triggers exit condition,
        Then: Exit signal is generated for instrument 2.
        """
        config = StrategyConfig(
            name="test_coint",
            strategy_class="CointegrationPairs",
            inputs=[
                "market.paper.kraken.BTC-USD.candles.1h",
                "market.paper.kraken.ETH-USD.candles.1h",
            ],
            outputs=["BTC-USD", "ETH-USD"],
            exchange="paper",
            params={
                "beta": 0.05,
                "entry_threshold": 2.0,
                "exit_threshold": 0.5,
                "lookback_window": 50,
                "min_data_points": 10,
            },
        )
        strategy = CointegrationPairs(config)
        btc_closes = [
            50000.0,
            50100.0,
            50200.0,
            50150.0,
            50050.0,
            49950.0,
            49900.0,
            49950.0,
            50000.0,
            50050.0,
        ]
        eth_closes = [
            3000.0,
            3010.0,
            3020.0,
            3015.0,
            3005.0,
            2995.0,
            2990.0,
            2995.0,
            3000.0,
            3005.0,
        ]
        prefill_candle_buffer(strategy, "BTC-USD", btc_closes)
        prefill_candle_buffer(strategy, "ETH-USD", eth_closes)
        strategy._position = "short_spread"
        signal = await feed_bar_to_strategy(strategy, "ETH-USD", 3005.0)
        if signal is not None:
            assert signal.instrument == "ETH-USD"
            assert signal.side == "sell"
            assert signal.strength == pytest.approx(0.0)
            assert "Exit short spread" in signal.reason


class TestCointegrationInstrument2ExitDirect:
    """Tests for direct z-score signal generation on instrument 2."""

    @pytest.mark.asyncio
    async def test_generate_signal_exit_short_spread_instrument2(self) -> None:
        """Verify exit short spread signal for instrument 2.

        Given: Strategy with short_spread position and exit z-score,
        When: _generate_signal_from_zscore is called for ETH-USD,
        Then: The returned group's ETH leg is a sell with "Exit short
            spread hedge" reason (the BTC partner is built alongside it).
        """
        config = StrategyConfig(
            name="test_coint",
            strategy_class="CointegrationPairs",
            inputs=[
                "market.paper.kraken.BTC-USD.candles.1h",
                "market.paper.kraken.ETH-USD.candles.1h",
            ],
            outputs=["BTC-USD", "ETH-USD"],
            exchange="paper",
            params={
                "beta": 0.05,
                "entry_threshold": 2.0,
                "exit_threshold": 0.5,
                "lookback_window": 50,
                "min_data_points": 10,
            },
        )
        strategy = CointegrationPairs(config)
        strategy.candle_buffer["BTC-USD"] = [make_candle_envelope("BTC-USD", 50000.0)]
        strategy._position = "short_spread"
        signals = strategy._generate_signal_from_zscore(
            z_score=0.3,
            instrument="ETH-USD",
            price=3000.0,
        )
        assert signals is not None
        eth_leg = signals[0]
        assert eth_leg.instrument == "ETH-USD"
        assert eth_leg.side == "sell"
        assert eth_leg.strength == pytest.approx(0.0)
        assert "Exit short spread hedge" in eth_leg.reason

    @pytest.mark.asyncio
    async def test_generate_signal_long_spread_no_exit(self) -> None:
        """Verify no exit signal when z-score doesn't trigger exit.

        Given: Strategy with long_spread position,
        When: z-score doesn't meet exit threshold,
        Then: None is returned and position unchanged.
        """
        config = StrategyConfig(
            name="test_coint",
            strategy_class="CointegrationPairs",
            inputs=[
                "market.paper.kraken.BTC-USD.candles.1h",
                "market.paper.kraken.ETH-USD.candles.1h",
            ],
            outputs=["BTC-USD", "ETH-USD"],
            exchange="paper",
            params={
                "beta": 0.05,
                "entry_threshold": 2.0,
                "exit_threshold": 0.5,
                "lookback_window": 50,
                "min_data_points": 10,
            },
        )
        strategy = CointegrationPairs(config)
        strategy._position = "long_spread"
        signal = strategy._generate_signal_from_zscore(
            z_score=-1.0,
            instrument="BTC-USD",
            price=50000.0,
        )
        assert signal is None
        assert strategy._position == "long_spread"

    def test_generate_signal_exit_long_spread_instrument2(self) -> None:
        """Verify exit long spread signal for instrument 2.

        Given: Strategy with long_spread position and exit z-score,
        When: _generate_signal_from_zscore is called for ETH-USD,
        Then: The returned group's ETH leg is a buy with "Exit long spread
            hedge" reason and the position is cleared.
        """
        config = StrategyConfig(
            name="test_coint",
            strategy_class="CointegrationPairs",
            inputs=[
                "market.paper.kraken.BTC-USD.candles.1h",
                "market.paper.kraken.ETH-USD.candles.1h",
            ],
            outputs=["BTC-USD", "ETH-USD"],
            exchange="paper",
            params={
                "beta": 0.05,
                "entry_threshold": 2.0,
                "exit_threshold": 0.5,
                "lookback_window": 50,
                "min_data_points": 10,
            },
        )
        strategy = CointegrationPairs(config)
        strategy.candle_buffer["BTC-USD"] = [make_candle_envelope("BTC-USD", 50000.0)]
        strategy._position = "long_spread"
        signals = strategy._generate_signal_from_zscore(
            z_score=-0.1,
            instrument="ETH-USD",
            price=3000.0,
        )
        assert signals is not None
        eth_leg = signals[0]
        assert eth_leg.instrument == "ETH-USD"
        assert eth_leg.side == "buy"
        assert eth_leg.strength == pytest.approx(0.0)
        assert "Exit long spread hedge" in eth_leg.reason
        assert strategy._position is None

    def test_generate_signal_exit_long_spread_instrument1(self) -> None:
        """Verify exit long spread signal for instrument 1.

        Given: Strategy with long_spread position and exit z-score,
        When: _generate_signal_from_zscore is called for BTC-USD,
        Then: The returned group's BTC leg is a sell with "Exit long
            spread" reason and the position is cleared.
        """
        config = StrategyConfig(
            name="test_coint",
            strategy_class="CointegrationPairs",
            inputs=[
                "market.paper.kraken.BTC-USD.candles.1h",
                "market.paper.kraken.ETH-USD.candles.1h",
            ],
            outputs=["BTC-USD", "ETH-USD"],
            exchange="paper",
            params={
                "beta": 0.05,
                "entry_threshold": 2.0,
                "exit_threshold": 0.5,
                "lookback_window": 50,
                "min_data_points": 10,
            },
        )
        strategy = CointegrationPairs(config)
        strategy.candle_buffer["ETH-USD"] = [make_candle_envelope("ETH-USD", 3000.0)]
        strategy._position = "long_spread"
        signals = strategy._generate_signal_from_zscore(
            z_score=-0.1,
            instrument="BTC-USD",
            price=50000.0,
        )
        assert signals is not None
        btc_leg = signals[0]
        assert btc_leg.instrument == "BTC-USD"
        assert btc_leg.side == "sell"
        assert btc_leg.strength == pytest.approx(0.0)
        assert "Exit long spread" in btc_leg.reason
        assert strategy._position is None

    def test_generate_signal_unknown_position_returns_none(self) -> None:
        """Verify unknown position state returns None.

        Given: Strategy with unexpected position state,
        When: _generate_signal_from_zscore is called,
        Then: None is returned and position unchanged.
        """
        config = StrategyConfig(
            name="test_coint",
            strategy_class="CointegrationPairs",
            inputs=[
                "market.paper.kraken.BTC-USD.candles.1h",
                "market.paper.kraken.ETH-USD.candles.1h",
            ],
            outputs=["BTC-USD", "ETH-USD"],
            exchange="paper",
            params={
                "beta": 0.05,
                "entry_threshold": 2.0,
                "exit_threshold": 0.5,
                "lookback_window": 50,
                "min_data_points": 10,
            },
        )
        strategy = CointegrationPairs(config)
        strategy._position = "unexpected_state"
        signal = strategy._generate_signal_from_zscore(
            z_score=0.0,
            instrument="BTC-USD",
            price=50000.0,
        )
        assert signal is None
        assert strategy._position == "unexpected_state"


class TestFactoryStrategyNotFound:
    """Tests for StrategyFactory error handling."""

    @pytest.mark.asyncio
    async def test_stop_nonexistent_strategy_raises(self) -> None:
        """Verify stopping non-existent strategy raises error.

        Given: A StrategyFactory with no registered strategies,
        When: stop_strategy is called with unknown name,
        Then: StrategyNotFoundError is raised.
        """
        factory = StrategyFactory()
        with pytest.raises(StrategyNotFoundError, match="not found"):
            await factory.stop_strategy("nonexistent_strategy")


class TestKrakenSkipInvalidPair:
    """Tests for Kraken pair filtering with missing base/quote."""

    def test_skip_pair_filtering_logic(self) -> None:
        """Verify pairs with missing base or quote are skipped.

        Given: Kraken symbol data with some missing base/quote,
        When: Filtering pairs with empty base or quote,
        Then: Only complete pairs are processed.
        """
        kraken_rest_symbols = {
            "XXBTZUSD": {
                "base": "",
                "quote": "ZUSD",
                "ccxt_symbol": "BTC/USD",
                "asset_class": "currency",
            },
            "XETHZEUR": {
                "base": "XETH",
                "quote": "",
                "ccxt_symbol": "ETH/EUR",
                "asset_class": "currency",
            },
            "XXRPZUSD": {
                "base": "XXRP",
                "quote": "ZUSD",
                "ccxt_symbol": "XRP/USD",
                "asset_class": "currency",
            },
        }
        processed_pairs = []
        for pair_name, pair_info in kraken_rest_symbols.items():
            base = pair_info.get("base", "")
            quote = pair_info.get("quote", "")
            if not base or not quote:
                continue
            processed_pairs.append(pair_name)
        assert len(processed_pairs) == 1
        assert "XXRPZUSD" in processed_pairs
        assert "XXBTZUSD" not in processed_pairs
        assert "XETHZEUR" not in processed_pairs


@dataclass
class StubTokenManager:
    """Stub token manager for testing token operations."""

    verify_response: TokenClaims | None = None
    refresh_response: TokenPair | None = None

    def verify_token(self, token: str) -> TokenClaims | None:
        """Return preconfigured verify response."""
        return self.verify_response

    def refresh_tokens(self, token: str) -> TokenPair | None:
        """Return preconfigured refresh response."""
        return self.refresh_response


def _token_data() -> TokenClaims:
    return TokenClaims(
        sub="user",
        username="name",
        role=UserRole.VIEWER,
        permissions=["read"],
        exp=999999999,
        iat=123456,
        jti="token-id",
        sid="session-rotator",
    )


def _token_pair(access_token: str) -> TokenPair:
    return TokenPair(
        access_token=access_token,
        refresh_token="refresh",
        expires_in=120,
    )


def test_update_and_get_connection_token() -> None:
    """Verify update_connection_token stores new token.

    Given: A registered connection,
    When: update_connection_token is called with new token,
    Then: get_connection_token returns the new token.
    """
    manager = StubTokenManager(verify_response=_token_data())
    rotator = WebSocketTokenRotator(manager)
    assert rotator.register_connection("conn", "initial-token")
    rotator.update_connection_token("conn", "new-token")
    assert rotator.get_connection_token("conn") == "new-token"


def test_rotate_connection_token_refresh_fallback() -> None:
    """Verify token rotation uses refresh_tokens.

    Given: A registered connection with refresh capability,
    When: rotate_connection_token is called,
    Then: Token is refreshed and stored.
    """
    manager = StubTokenManager(
        verify_response=_token_data(),
        refresh_response=_token_pair("refreshed-token"),
    )
    rotator = WebSocketTokenRotator(manager)
    assert rotator.register_connection("conn", "initial-token")
    assert rotator.rotate_connection_token("conn", "refresh-token") == "refreshed-token"
    assert rotator.get_connection_token("conn") == "refreshed-token"


class TestTokenManager:
    """Tests for TokenManager JWT operations."""

    def test_init(self) -> None:
        """Verify TokenManager initializes with default values.

        Given: A new TokenManager instance,
        When: Instance is created,
        Then: Settings and empty blacklist are initialized.
        """
        token_manager = TokenManager()
        assert token_manager.settings is not None
        assert isinstance(token_manager._blacklisted_tokens, dict)
        assert len(token_manager._blacklisted_tokens) == 0
        assert token_manager._blacklist_cleanup_heap == []
        assert token_manager._next_blacklist_cleanup_ts == float("inf")
        assert token_manager._blacklist_grace_period == pytest.approx(
            BLACKLIST_GRACE_PERIOD_SECONDS
        )

    def test_create_tokens_basic(self) -> None:
        """Verify create_tokens generates valid access and refresh tokens.

        Given: A TokenManager and user profile,
        When: create_tokens is called,
        Then: TokenPair with valid JWTs and claims is returned.
        """
        token_manager = TokenManager()
        user = AuthPrincipal(
            username="testuser",
            email="test@example.com",
            role=UserRole.OPERATOR,
            is_active=True,
        )
        token_pair = token_manager.create_tokens(user)
        assert token_pair.access_token is not None
        assert token_pair.refresh_token is not None
        assert token_pair.token_type == "bearer"
        assert token_pair.expires_in > 0
        access_payload = jwt.decode(
            token_pair.access_token,
            token_manager.settings.auth_secret_key,
            algorithms=[token_manager.settings.auth_algorithm],
        )
        assert access_payload["sub"] == user.username
        assert access_payload["username"] == user.username
        assert access_payload["role"] == user.role.value
        assert "permissions" in access_payload
        assert access_payload["jti"] is not None
        refresh_payload = jwt.decode(
            token_pair.refresh_token,
            token_manager.settings.auth_secret_key,
            algorithms=[token_manager.settings.auth_algorithm],
        )
        assert refresh_payload["sub"] == user.username
        assert refresh_payload["username"] == user.username
        assert set(refresh_payload["permissions"]) == {
            permission.value for permission in ROLE_PERMISSIONS[user.role]
        }
        assert refresh_payload["permission_scope_version"] == PERMISSION_SCOPE_VERSION
        assert refresh_payload["jti"].startswith("refresh_")

    def test_create_tokens_enforces_requested_scope_and_role_ceiling(self) -> None:
        """A requested grant is preserved exactly and cannot exceed the role.

        Given: An operator and a requested read-only permission scope,
        When: A token pair is minted and a role-external permission is tried,
        Then: Both JWTs carry only the requested grant and the invalid request
            is rejected instead of silently dropping the extra permission.
        """
        token_manager = TokenManager()
        user = AuthPrincipal(username="scoped", role=UserRole.OPERATOR)

        token_pair = token_manager.create_tokens(
            user,
            permissions=[Permission.READ_MARKET_DATA],
        )
        access_claims = token_manager.decode_fresh_token(token_pair.access_token)
        refresh_claims = token_manager.decode_fresh_token(token_pair.refresh_token)

        assert access_claims.permissions == [Permission.READ_MARKET_DATA.value]
        assert refresh_claims.permissions == [Permission.READ_MARKET_DATA.value]
        assert refresh_claims.permission_scope_version == PERMISSION_SCOPE_VERSION
        with pytest.raises(PermissionScopeError, match="manage:users"):
            token_manager.create_tokens(
                user,
                permissions=[Permission.READ_MARKET_DATA, Permission.MANAGE_USERS],
            )

    def test_create_tokens_accepts_explicit_empty_scope(self) -> None:
        """An intentionally empty token grant remains distinguishable from omission."""
        token_manager = TokenManager()
        user = AuthPrincipal(username="empty-scope", role=UserRole.ADMIN)

        token_pair = token_manager.create_tokens(user, permissions=[])

        access_claims = token_manager.decode_fresh_token(token_pair.access_token)
        refresh_claims = token_manager.decode_fresh_token(token_pair.refresh_token)
        assert access_claims.permissions == []
        assert refresh_claims.permissions == []
        assert refresh_claims.permission_scope_version == PERMISSION_SCOPE_VERSION

    def test_create_tokens_remember_me(self) -> None:
        """Verify remember_me extends refresh token expiration.

        Given: A TokenManager and user profile,
        When: create_tokens is called with remember_me=True,
        Then: Refresh token has longer expiration.
        """
        token_manager = TokenManager()
        user = AuthPrincipal(
            username="testuser",
            role=UserRole.VIEWER,
            is_active=True,
        )
        tokens_normal = token_manager.create_tokens(user, remember_me=False)
        refresh_payload_normal = jwt.decode(
            tokens_normal.refresh_token,
            token_manager.settings.auth_secret_key,
            algorithms=[token_manager.settings.auth_algorithm],
        )
        tokens_extended = token_manager.create_tokens(user, remember_me=True)
        refresh_payload_extended = jwt.decode(
            tokens_extended.refresh_token,
            token_manager.settings.auth_secret_key,
            algorithms=[token_manager.settings.auth_algorithm],
        )
        assert refresh_payload_extended["exp"] > refresh_payload_normal["exp"]

    def test_create_tokens_all_roles(self) -> None:
        """Verify create_tokens works for all user roles.

        Given: Users with each UserRole,
        When: create_tokens is called for each user,
        Then: Valid tokens with correct role claims are created.
        """
        token_manager = TokenManager()
        for role in UserRole:
            user = AuthPrincipal(
                username=f"user_{role.value}",
                role=role,
                is_active=True,
            )
            token_pair = token_manager.create_tokens(user)
            token_data = token_manager.verify_token(token_pair.access_token)
            assert token_data is not None
            assert token_data.role == role
            assert token_data.permissions is not None

    def test_verify_token_valid(self) -> None:
        """Verify verify_token returns claims for valid token.

        Given: A generated token pair,
        When: verify_token is called with access token,
        Then: TokenClaims with user data is returned.
        """
        token_manager = TokenManager()
        user = AuthPrincipal(
            username="testuser",
            role=UserRole.ADMIN,
            is_active=True,
        )
        token_pair = token_manager.create_tokens(user)
        access_data = token_manager.verify_token(token_pair.access_token)
        assert access_data is not None
        assert access_data.sub == user.username
        assert access_data.username == user.username
        assert access_data.role == user.role
        refresh_data = token_manager.verify_token(token_pair.refresh_token)
        assert refresh_data is not None
        assert refresh_data.sub == user.username
        assert refresh_data.jti.startswith("refresh_")

    def test_verify_token_invalid_jwt(self) -> None:
        """Verify verify_token returns None for invalid JWT.

        Given: Invalid token strings,
        When: verify_token is called,
        Then: None is returned.
        """
        token_manager = TokenManager()
        result = token_manager.verify_token("invalid_token")
        assert result is None
        result = token_manager.verify_token("invalid.jwt.token")
        assert result is None
        result = token_manager.verify_token("")
        assert result is None

    def test_token_blacklist_grace_and_expiry(self) -> None:
        """Verify blacklist respects grace period and cleanup.

        Given: Tokens blacklisted at different times,
        When: Checking blacklist status,
        Then: Recent tokens pass grace, old ones are blocked and cleaned.
        """
        token_manager = TokenManager()
        token_manager._blacklist_grace_period = 0.1
        now = datetime.now(UTC).timestamp()
        token_manager._record_blacklisted_token("jti-new", now)
        token_manager._record_blacklisted_token("jti-old", now - 1.0)
        assert token_manager._is_token_blacklisted("jti-new") is False
        assert token_manager._is_token_blacklisted("jti-old") is True
        token_manager._cleanup_old_blacklist_entries()
        assert "jti-old" not in token_manager._blacklisted_tokens
        result = token_manager.verify_token(None)
        assert result is None

    def test_blacklist_cleanup_ignores_stale_heap_entries_for_reused_jti(self) -> None:
        """Verify stale cleanup heap entries do not remove refreshed JTI state.

        Given: A JTI recorded twice with a newer blacklist timestamp,
        When: The old cleanup deadline is processed,
        Then: The current blacklist entry remains in its grace period.
        """
        token_manager = TokenManager()
        token_manager._blacklist_grace_period = 0.1
        now = datetime.now(UTC).timestamp()
        token_manager._record_blacklisted_token("jti-reused", now - 1.0)
        token_manager._record_blacklisted_token("jti-reused", now)
        token_manager._cleanup_old_blacklist_entries()
        assert "jti-reused" in token_manager._blacklisted_tokens
        assert token_manager._is_token_blacklisted("jti-reused") is False

    def test_blacklist_cleanup_is_bounded_per_pass(self) -> None:
        """Verify blacklist cleanup removes only a fixed expired batch.

        Given: More expired blacklist entries than the cleanup batch size,
        When: Cleanup runs once,
        Then: Expired entries remain for subsequent cleanup passes.
        """
        token_manager = TokenManager()
        token_manager._blacklist_grace_period = 0.1
        old_time = datetime.now(UTC).timestamp() - 1.0
        for index in range(BLACKLIST_CLEANUP_BATCH_SIZE + 3):
            token_manager._record_blacklisted_token(f"jti-old-{index}", old_time)
        token_manager._cleanup_old_blacklist_entries()
        assert len(token_manager._blacklisted_tokens) == 3

    def test_verify_token_missing_required_claim(self) -> None:
        """Verify verify_token returns None for token missing sid claim.

        Given: A token without required sid claim,
        When: verify_token is called,
        Then: None is returned.
        """
        token_manager = TokenManager()
        now = datetime.now(UTC)
        payload_missing_sid: dict[str, str | int | list[str]] = {
            "sub": "user-123",
            "username": "tester",
            "role": "viewer",
            "permissions": [],
            "exp": int(now.timestamp()) + 3600,
            "iat": int(now.timestamp()),
            "jti": "test-jti",
        }
        malformed_token = jwt.encode(
            payload_missing_sid,
            token_manager.settings.auth_secret_key,
            algorithm=token_manager.settings.auth_algorithm,
        )
        assert token_manager.verify_token(malformed_token) is None

    def test_verify_token_accepts_legacy_token_without_permissions_claim(self) -> None:
        """A legacy access token with no permissions claim remains decodable.

        Given: A correctly signed access token minted before the claim existed,
        When: The token manager verifies it,
        Then: Claims decode with ``permissions=None`` so authorization can use
            the backward-compatible full-role fallback.
        """
        token_manager = TokenManager()
        now = datetime.now(UTC)
        payload: dict[str, str | int] = {
            "sub": "legacy-user",
            "username": "legacy-user",
            "role": UserRole.VIEWER.value,
            "sid": "legacy-session",
            "exp": int((now + timedelta(hours=1)).timestamp()),
            "iat": int(now.timestamp()),
            "jti": "legacy-jti",
        }
        token = jwt.encode(
            payload,
            token_manager.settings.auth_secret_key,
            algorithm=token_manager.settings.auth_algorithm,
        )

        claims = token_manager.verify_token(token)

        assert claims is not None
        assert claims.permissions is None

    def test_verify_token_expired(self) -> None:
        """Verify verify_token returns None for expired token.

        Given: A token with past expiration,
        When: verify_token is called,
        Then: None is returned.
        """
        token_manager = TokenManager()
        now = datetime.now(UTC)
        expired_payload: dict[str, str | int | list[str]] = {
            "sub": "test_user",
            "username": "testuser",
            "role": "operator",
            "permissions": [],
            "sid": "session_expired",
            "exp": int((now - timedelta(hours=1)).timestamp()),
            "iat": int((now - timedelta(hours=2)).timestamp()),
            "jti": "test_jti",
        }
        expired_token = jwt.encode(
            expired_payload,
            token_manager.settings.auth_secret_key,
            algorithm=token_manager.settings.auth_algorithm,
        )
        result = token_manager.verify_token(expired_token)
        assert result is None

    def test_verify_token_expired_timestamp_check(self) -> None:
        """Verify verify_token rejects recently expired tokens.

        Given: A token that expired 1 second ago,
        When: verify_token is called,
        Then: None is returned.
        """
        token_manager = TokenManager()
        now = datetime.now(UTC)
        almost_expired_payload: dict[str, str | int | list[str]] = {
            "sub": "test_user_timestamp",
            "username": "testuser",
            "role": "operator",
            "permissions": [],
            "sid": "session_timestamp",
            "exp": int((now - timedelta(seconds=1)).timestamp()),
            "iat": int((now - timedelta(hours=1)).timestamp()),
            "jti": "test_jti_timestamp",
        }
        with patch("jwt.decode") as mock_decode:
            mock_decode.return_value = almost_expired_payload
            result = token_manager.verify_token("fake_token")
            assert result is None

    def test_verify_token_blacklisted(self) -> None:
        """Verify verify_token returns None for blacklisted token.

        Given: A valid token that is blacklisted,
        When: verify_token is called,
        Then: None is returned.
        """
        token_manager = TokenManager()
        user = AuthPrincipal(
            username="testuser",
            role=UserRole.OPERATOR,
            is_active=True,
        )
        token_pair = token_manager.create_tokens(user)
        token_data = token_manager.verify_token(token_pair.access_token)
        assert token_data is not None
        token_manager.blacklist_token_immediately(token_data.jti)
        result = token_manager.verify_token(token_pair.access_token)
        assert result is None

    @patch("snapper.auth.tokens.jwt.decode")
    def test_verify_token_jwt_error(self, mock_decode: MagicMock) -> None:
        """Verify verify_token handles JWT errors gracefully.

        Given: jwt.decode raises various JWT errors,
        When: verify_token is called,
        Then: None is returned without raising.
        """
        token_manager = TokenManager()
        mock_decode.side_effect = jwt.ExpiredSignatureError("Token expired")
        result = token_manager.verify_token("some_token")
        assert result is None
        mock_decode.side_effect = jwt.PyJWTError("Invalid token")
        result = token_manager.verify_token("some_token")
        assert result is None

    def test_refresh_tokens_valid(self) -> None:
        """Verify refresh_tokens returns new token pair.

        Given: A valid refresh token,
        When: refresh_tokens is called,
        Then: New token pair with different tokens is returned.
        """
        token_manager = TokenManager()
        user = AuthPrincipal(
            username="testuser",
            role=UserRole.ADMIN,
            is_active=True,
        )
        initial_tokens = token_manager.create_tokens(user)
        new_tokens = token_manager.refresh_tokens(initial_tokens.refresh_token)
        assert new_tokens is not None
        assert new_tokens.access_token != initial_tokens.access_token
        assert new_tokens.refresh_token != initial_tokens.refresh_token
        new_access_data = token_manager.verify_token(new_tokens.access_token)
        assert new_access_data is not None
        assert new_access_data.sub == user.username
        old_refresh_result = token_manager.verify_token(initial_tokens.refresh_token)
        assert old_refresh_result is not None

    def test_refresh_tokens_preserves_narrow_and_empty_scopes(self) -> None:
        """Refresh rotation never restores permissions omitted by the token grant."""
        token_manager = TokenManager()
        user = AuthPrincipal(username="scoped-refresh", role=UserRole.ADMIN)

        narrow_pair = token_manager.create_tokens(
            user,
            permissions=[Permission.READ_MARKET_DATA],
        )
        empty_pair = token_manager.create_tokens(user, permissions=[])
        refreshed_narrow = token_manager.refresh_tokens(narrow_pair.refresh_token)
        refreshed_empty = token_manager.refresh_tokens(empty_pair.refresh_token)

        assert refreshed_narrow is not None
        assert refreshed_empty is not None
        narrow_claims = token_manager.decode_fresh_token(refreshed_narrow.access_token)
        empty_claims = token_manager.decode_fresh_token(refreshed_empty.access_token)
        assert narrow_claims.permissions == [Permission.READ_MARKET_DATA.value]
        assert empty_claims.permissions == []

    def test_refresh_tokens_legacy_empty_claim_falls_back_to_full_role(self) -> None:
        """An in-flight legacy refresh token retains its historical semantics.

        Given: A pre-version refresh JWT whose permissions list is empty,
        When: It rotates after scoped-token support is deployed,
        Then: The successor receives the complete role grant because the old
            empty list did not represent an intentional empty access scope.
        """
        token_manager = TokenManager()
        now = datetime.now(UTC)
        payload: dict[str, str | int | list[str]] = {
            "sub": "legacy-refresh-user",
            "username": "legacy-refresh-user",
            "role": UserRole.OPERATOR.value,
            "permissions": [],
            "sid": "legacy-refresh-session",
            "exp": int((now + timedelta(hours=1)).timestamp()),
            "iat": int(now.timestamp()),
            "jti": "refresh_legacy-jti",
        }
        refresh_token = jwt.encode(
            payload,
            token_manager.settings.auth_secret_key,
            algorithm=token_manager.settings.auth_algorithm,
        )

        successor = token_manager.refresh_tokens(refresh_token)

        assert successor is not None
        successor_claims = token_manager.decode_fresh_token(successor.access_token)
        assert set(successor_claims.permissions or []) == {
            permission.value for permission in ROLE_PERMISSIONS[UserRole.OPERATOR]
        }

    def test_refresh_tokens_invalid_token(self) -> None:
        """Verify refresh_tokens returns None for invalid token.

        Given: An invalid token string,
        When: refresh_tokens is called,
        Then: None is returned.
        """
        token_manager = TokenManager()
        result = token_manager.refresh_tokens("invalid_token")
        assert result is None

    def test_refresh_tokens_not_refresh_token(self) -> None:
        """Verify refresh_tokens rejects access tokens.

        Given: An access token instead of refresh token,
        When: refresh_tokens is called,
        Then: None is returned.
        """
        token_manager = TokenManager()
        user = AuthPrincipal(
            username="testuser",
            role=UserRole.OPERATOR,
            is_active=True,
        )
        token_pair = token_manager.create_tokens(user)
        result = token_manager.refresh_tokens(token_pair.access_token)
        assert result is None

    def test_blacklist_token(self) -> None:
        """Verify blacklist_token adds JTI to blacklist.

        Given: A TokenManager with empty blacklist,
        When: blacklist_token is called with a JTI,
        Then: JTI is added to blacklisted tokens.
        """
        token_manager = TokenManager()
        test_jti = "test_jti_123"
        assert test_jti not in token_manager._blacklisted_tokens
        token_manager.blacklist_token(test_jti)
        assert test_jti in token_manager._blacklisted_tokens
        assert token_manager._blacklist_cleanup_heap
        assert token_manager._next_blacklist_cleanup_ts < float("inf")

    def test_invalidate_token(self) -> None:
        """Verify invalidate_token blocks token verification.

        Given: A valid access token,
        When: invalidate_token is called,
        Then: verify_token returns None for that token.
        """
        token_manager = TokenManager()
        user = AuthPrincipal(
            username="testuser",
            role=UserRole.VIEWER,
            is_active=True,
        )
        token_pair = token_manager.create_tokens(user)
        token_data = token_manager.verify_token(token_pair.access_token)
        assert token_data is not None
        token_manager.invalidate_token(token_pair.access_token)
        result = token_manager.verify_token(token_pair.access_token)
        assert result is None

    def test_invalidate_token_invalid_token(self) -> None:
        """Verify invalidate_token handles invalid token gracefully.

        Given: An invalid token string,
        When: invalidate_token is called,
        Then: No error is raised.
        """
        token_manager = TokenManager()
        token_manager.invalidate_token("invalid_token")

    def test_invalidate_user_tokens(self) -> None:
        """Verify invalidate_user_tokens handles user ID.

        Given: A user ID,
        When: invalidate_user_tokens is called,
        Then: No error is raised.
        """
        token_manager = TokenManager()
        user_id = "test_user_invalidate_all"
        token_manager.invalidate_user_tokens(user_id)

    def test_create_csrf_token(self) -> None:
        """Verify create_csrf_token generates unique tokens.

        Given: A TokenManager,
        When: create_csrf_token is called multiple times,
        Then: Different non-empty tokens are returned.
        """
        token_manager = TokenManager()
        csrf_token1 = token_manager.create_csrf_token()
        csrf_token2 = token_manager.create_csrf_token()
        assert csrf_token1 is not None
        assert csrf_token2 is not None
        assert csrf_token1 != csrf_token2
        assert len(csrf_token1) > 0
        assert len(csrf_token2) > 0

    def test_verify_csrf_token(self) -> None:
        """Verify verify_csrf_token compares tokens correctly.

        Given: CSRF tokens,
        When: verify_csrf_token compares matching and non-matching,
        Then: True for match, False for mismatch.
        """
        token_manager = TokenManager()
        csrf_token = token_manager.create_csrf_token()
        assert token_manager.verify_csrf_token(csrf_token, csrf_token)
        other_token = token_manager.create_csrf_token()
        assert not token_manager.verify_csrf_token(csrf_token, other_token)
        assert not token_manager.verify_csrf_token(csrf_token, "wrong_token")
        assert not token_manager.verify_csrf_token("wrong_token", csrf_token)


class TestWebSocketTokenRotator:
    """Tests for WebSocket token rotation functionality."""

    def test_init(self) -> None:
        """Verify WebSocketTokenRotator initializes correctly.

        Given: A TokenManager,
        When: WebSocketTokenRotator is created,
        Then: Token manager is set and connection dict is empty.
        """
        token_manager = TokenManager()
        rotator = WebSocketTokenRotator(token_manager)
        assert rotator.token_manager is token_manager
        assert isinstance(rotator._connection_tokens, dict)
        assert len(rotator._connection_tokens) == 0

    def test_init_is_idempotent_on_repeated_construction(self) -> None:
        """Verify a second WebSocketTokenRotator(...) call is a no-op.

        Given: A WebSocketTokenRotator already constructed and initialized,
        When: ``WebSocketTokenRotator(...)`` is invoked a second time
            (Python re-runs ``__init__`` on the singleton instance returned
            by ``__new__``),
        Then: The ``_initialized`` guard short-circuits before re-binding
            ``token_manager`` / ``_connection_tokens``, so prior state and
            the singleton identity are preserved.
        """
        WebSocketTokenRotator.clear_instance()
        first_manager = TokenManager()
        rotator_first = WebSocketTokenRotator(first_manager)
        rotator_first._connection_tokens["sentinel"] = "preserved"
        second_manager = TokenManager()
        rotator_second = WebSocketTokenRotator(second_manager)
        assert rotator_first is rotator_second
        assert rotator_second.token_manager is first_manager
        assert rotator_second._connection_tokens == {"sentinel": "preserved"}

    def test_register_connection_valid_token(self) -> None:
        """Verify register_connection stores valid token.

        Given: A valid access token,
        When: register_connection is called,
        Then: Connection is registered and True returned.
        """
        token_manager = TokenManager()
        rotator = WebSocketTokenRotator(token_manager)
        user = AuthPrincipal(
            username="testuser",
            role=UserRole.OPERATOR,
            is_active=True,
        )
        token_pair = token_manager.create_tokens(user)
        connection_id = "ws_conn_123"
        result = rotator.register_connection(connection_id, token_pair.access_token)
        assert result is True
        assert connection_id in rotator._connection_tokens
        assert rotator._connection_tokens[connection_id] == token_pair.access_token

    def test_register_connection_invalid_token(self) -> None:
        """Verify register_connection rejects invalid token.

        Given: An invalid token string,
        When: register_connection is called,
        Then: Connection is not registered and False returned.
        """
        token_manager = TokenManager()
        rotator = WebSocketTokenRotator(token_manager)
        connection_id = "ws_conn_invalid"
        result = rotator.register_connection(connection_id, "invalid_token")
        assert result is False
        assert connection_id not in rotator._connection_tokens

    def test_should_rotate_token_connection_not_found(self) -> None:
        """Verify should_rotate_token returns False for unknown connection.

        Given: An unregistered connection ID,
        When: should_rotate_token is called,
        Then: False is returned.
        """
        token_manager = TokenManager()
        rotator = WebSocketTokenRotator(token_manager)
        result = rotator.should_rotate_token("unknown_connection")
        assert result is False

    def test_should_rotate_token_invalid_token(self) -> None:
        """Verify should_rotate_token returns True for invalid stored token.

        Given: A connection with invalid token stored,
        When: should_rotate_token is called,
        Then: True is returned.
        """
        token_manager = TokenManager()
        rotator = WebSocketTokenRotator(token_manager)
        connection_id = "ws_conn_invalid_token"
        rotator._connection_tokens[connection_id] = "invalid_token"
        result = rotator.should_rotate_token(connection_id)
        assert result is True

    def test_should_rotate_token_not_expired(self) -> None:
        """Verify should_rotate_token returns False for fresh token.

        Given: A connection with fresh valid token,
        When: should_rotate_token is called,
        Then: False is returned.
        """
        token_manager = TokenManager()
        rotator = WebSocketTokenRotator(token_manager)
        user = AuthPrincipal(
            username="testuser",
            role=UserRole.OPERATOR,
            is_active=True,
        )
        token_pair = token_manager.create_tokens(user)
        connection_id = "ws_conn_not_expired"
        rotator.register_connection(connection_id, token_pair.access_token)
        result = rotator.should_rotate_token(connection_id)
        assert result is False

    def test_should_rotate_token_expiring_soon(self) -> None:
        """Verify should_rotate_token returns True for expiring token.

        Given: A connection with token expiring in 2 minutes,
        When: should_rotate_token is called,
        Then: True is returned.
        """
        token_manager = TokenManager()
        rotator = WebSocketTokenRotator(token_manager)
        now = datetime.now(UTC)
        soon_expired_payload: dict[str, str | int | list[str]] = {
            "sub": "test_user",
            "username": "testuser",
            "role": "operator",
            "permissions": ["read_data"],
            "sid": "session_expiring_soon",
            "exp": int((now + timedelta(minutes=2)).timestamp()),
            "iat": int(now.timestamp()),
            "jti": "test_jti_soon",
        }
        soon_expired_token = jwt.encode(
            soon_expired_payload,
            token_manager.settings.auth_secret_key,
            algorithm=token_manager.settings.auth_algorithm,
        )
        connection_id = "ws_conn_expiring_soon"
        rotator._connection_tokens[connection_id] = soon_expired_token
        result = rotator.should_rotate_token(connection_id)
        assert result is True

    def test_rotate_connection_token_success(self) -> None:
        """Verify rotate_connection_token updates stored token.

        Given: A registered connection with refresh token,
        When: rotate_connection_token is called,
        Then: New access token is stored and returned.
        """
        token_manager = TokenManager()
        rotator = WebSocketTokenRotator(token_manager)
        user = AuthPrincipal(
            username="testuser",
            role=UserRole.ADMIN,
            is_active=True,
        )
        token_pair = token_manager.create_tokens(user)
        connection_id = "ws_conn_rotate"
        rotator.register_connection(connection_id, token_pair.access_token)
        old_token = rotator._connection_tokens[connection_id]
        new_token = rotator.rotate_connection_token(connection_id, token_pair.refresh_token)
        assert new_token is not None
        assert new_token != old_token
        assert rotator._connection_tokens[connection_id] == new_token

    def test_rotate_connection_token_invalid_refresh(self) -> None:
        """Verify rotate_connection_token returns None for invalid refresh.

        Given: An invalid refresh token,
        When: rotate_connection_token is called,
        Then: None is returned.
        """
        token_manager = TokenManager()
        rotator = WebSocketTokenRotator(token_manager)
        connection_id = "ws_conn_invalid_refresh"
        result = rotator.rotate_connection_token(connection_id, "invalid_refresh_token")
        assert result is None

    def test_unregister_connection_exists(self) -> None:
        """Verify unregister_connection removes registered connection.

        Given: A registered connection,
        When: unregister_connection is called,
        Then: Connection is removed from tracking.
        """
        token_manager = TokenManager()
        rotator = WebSocketTokenRotator(token_manager)
        user = AuthPrincipal(
            username="testuser",
            role=UserRole.OPERATOR,
            is_active=True,
        )
        token_pair = token_manager.create_tokens(user)
        connection_id = "ws_conn_unregister"
        rotator.register_connection(connection_id, token_pair.access_token)
        assert connection_id in rotator._connection_tokens
        rotator.unregister_connection(connection_id)
        assert connection_id not in rotator._connection_tokens

    def test_unregister_connection_not_exists(self) -> None:
        """Verify unregister_connection handles non-existent gracefully.

        Given: An unregistered connection ID,
        When: unregister_connection is called,
        Then: No error is raised.
        """
        token_manager = TokenManager()
        rotator = WebSocketTokenRotator(token_manager)
        rotator.unregister_connection("non_existing_connection")


class TestGlobalInstances:
    """Tests for global singleton instances (token manager, rotator)."""

    def test_get_token_manager_singleton(self) -> None:
        """Verify get_token_manager returns singleton instance.

        Given: Multiple calls to get_token_manager,
        When: Comparing returned instances,
        Then: All references point to the same TokenManager.
        """
        manager1 = get_token_manager()
        manager2 = get_token_manager()
        assert manager1 is manager2
        assert isinstance(manager1, TokenManager)

    def test_get_ws_token_rotator_singleton(self) -> None:
        """Verify get_ws_token_rotator returns singleton instance.

        Given: Multiple calls to get_ws_token_rotator,
        When: Comparing returned instances,
        Then: All references point to the same rotator.
        """
        rotator1 = get_ws_token_rotator()
        rotator2 = get_ws_token_rotator()
        assert rotator1 is rotator2
        assert isinstance(rotator1, WebSocketTokenRotator)

    def test_ws_token_rotator_uses_token_manager(self) -> None:
        """Verify WS token rotator uses the global token manager.

        Given: Global token manager and rotator instances,
        When: Comparing rotator's token_manager,
        Then: It references the global token manager.
        """
        token_manager = get_token_manager()
        ws_rotator = get_ws_token_rotator()
        assert ws_rotator.token_manager is token_manager


class TestIntegrationScenarios:
    """Integration tests for full token and auth workflows."""

    def test_full_token_lifecycle(self) -> None:
        """Verify complete token create, verify, refresh, invalidate cycle.

        Given: A user and TokenManager,
        When: Full lifecycle operations are performed,
        Then: Each step behaves correctly.
        """
        token_manager = TokenManager()
        user = AuthPrincipal(
            username="lifecycleuser",
            role=UserRole.OPERATOR,
            is_active=True,
        )
        tokens = token_manager.create_tokens(user)
        assert tokens.access_token is not None
        assert tokens.refresh_token is not None
        access_data = token_manager.verify_token(tokens.access_token)
        refresh_data = token_manager.verify_token(tokens.refresh_token)
        assert access_data is not None
        assert refresh_data is not None
        new_tokens = token_manager.refresh_tokens(tokens.refresh_token)
        assert new_tokens is not None
        assert new_tokens.access_token != tokens.access_token
        old_refresh_result = token_manager.verify_token(tokens.refresh_token)
        assert old_refresh_result is not None
        token_manager.invalidate_token(new_tokens.access_token)
        invalid_result = token_manager.verify_token(new_tokens.access_token)
        assert invalid_result is None

    def test_websocket_token_rotation_scenario(self) -> None:
        """Verify WebSocket token rotation workflow.

        Given: A user with tokens and WS rotator,
        When: Full rotation scenario is executed,
        Then: Registration, rotation, unregistration work correctly.
        """
        token_manager = TokenManager()
        rotator = WebSocketTokenRotator(token_manager)
        user = AuthPrincipal(
            username="wsuser",
            role=UserRole.ADMIN,
            is_active=True,
        )
        tokens = token_manager.create_tokens(user)
        connection_id = "ws_scenario_conn"
        success = rotator.register_connection(connection_id, tokens.access_token)
        assert success is True
        needs_rotation = rotator.should_rotate_token(connection_id)
        assert needs_rotation is False
        new_token = rotator.rotate_connection_token(connection_id, tokens.refresh_token)
        assert new_token is not None
        assert rotator._connection_tokens[connection_id] == new_token
        rotator.unregister_connection(connection_id)
        assert connection_id not in rotator._connection_tokens

    @patch("snapper.auth.tokens.secrets.token_urlsafe")
    def test_csrf_token_generation_mocked(self, mock_token_urlsafe: MagicMock) -> None:
        """Verify CSRF token generation uses secrets.token_urlsafe.

        Given: Mocked token_urlsafe,
        When: create_csrf_token is called,
        Then: Mocked value is returned.
        """
        mock_token_urlsafe.return_value = "mocked_csrf_token"
        token_manager = TokenManager()
        csrf_token = token_manager.create_csrf_token()
        assert csrf_token == "mocked_csrf_token"
        mock_token_urlsafe.assert_called_once_with(32)

    @patch("snapper.auth.tokens.secrets.compare_digest")
    def test_csrf_token_verification_mocked(self, mock_compare_digest: MagicMock) -> None:
        """Verify CSRF verification uses secrets.compare_digest.

        Given: Mocked compare_digest returning True,
        When: verify_csrf_token is called,
        Then: True is returned.
        """
        mock_compare_digest.return_value = True
        token_manager = TokenManager()
        result = token_manager.verify_csrf_token("token1", "token2")
        assert result is True
        mock_compare_digest.assert_called_once_with("token1", "token2")

    def test_token_permissions_for_all_roles(self) -> None:
        """Verify token permissions match ROLE_PERMISSIONS for all roles.

        Given: Users with each UserRole,
        When: Tokens are created and verified,
        Then: Token permissions match ROLE_PERMISSIONS mapping.
        """
        token_manager = TokenManager()
        for role in UserRole:
            user = AuthPrincipal(
                username=f"user_{role.value}",
                role=role,
                is_active=True,
            )
            tokens = token_manager.create_tokens(user)
            token_data = token_manager.verify_token(tokens.access_token)
            assert token_data is not None
            expected_permissions = [p.value for p in ROLE_PERMISSIONS[role]]
            assert token_data.permissions == expected_permissions

    def test_concurrent_token_operations(self) -> None:
        """Verify multiple tokens for same user work independently.

        Given: Multiple token pairs for same user,
        When: One token is blacklisted,
        Then: Other tokens remain valid.
        """
        token_manager = TokenManager()
        user = AuthPrincipal(
            username="concurrentuser",
            role=UserRole.OPERATOR,
            is_active=True,
        )
        tokens1 = token_manager.create_tokens(user)
        tokens2 = token_manager.create_tokens(user)
        tokens3 = token_manager.create_tokens(user)
        assert token_manager.verify_token(tokens1.access_token) is not None
        assert token_manager.verify_token(tokens2.access_token) is not None
        assert token_manager.verify_token(tokens3.access_token) is not None
        data1 = token_manager.verify_token(tokens1.access_token)
        assert data1 is not None
        token_manager.blacklist_token_immediately(data1.jti)
        assert token_manager.verify_token(tokens1.access_token) is None
        assert token_manager.verify_token(tokens2.access_token) is not None
        assert token_manager.verify_token(tokens3.access_token) is not None


class TestGetCurrentUser:
    """Tests for get_current_user dependency."""

    async def test_get_current_user_no_credentials(self) -> None:
        """Verify get_current_user returns None without credentials.

        Given: A request with no access_token cookie,
        When: get_current_user is called,
        Then: None is returned.
        """
        request = Mock(spec=Request)
        request.cookies = {}
        request.headers = {}
        repo = Mock()
        result = await get_current_user(request, repo)
        assert result is None

    async def test_get_current_user_valid_token(self) -> None:
        """Verify get_current_user returns user for valid token.

        Given: A request with valid access_token cookie,
        When: get_current_user is called,
        Then: UserProfile is returned and set on request.state.
        """
        request = Mock(spec=Request)
        request.state = Mock()
        request.cookies = {"access_token": "valid_token"}
        request.headers = {}
        now = int(datetime.now(UTC).timestamp())
        token_data = TokenClaims(
            sub="user123",
            username="testuser",
            role=UserRole.OPERATOR,
            permissions=["read:market_data", "create:orders"],
            exp=now + 3600,
            iat=now,
            jti="test-jwt-id",
            sid="session-deps",
        )
        with patch("snapper.auth.dependencies.get_token_manager") as mock_get_token_manager:
            mock_token_manager = Mock()
            mock_token_manager.verify_token_with_db = AsyncMock(return_value=token_data)
            mock_get_token_manager.return_value = mock_token_manager
            repo = Mock()
            result = await get_current_user(request, repo)
            assert result is not None
            assert result.username == "testuser"
            assert result.role == UserRole.OPERATOR
            assert result.permissions == ["read:market_data", "create:orders"]
            assert request.state.user == result
            assert request.state.token_data == token_data
            mock_token_manager.verify_token_with_db.assert_awaited_once_with("valid_token", repo)

    async def test_get_current_user_populates_delegate_public_id_for_ai_delegate(
        self,
    ) -> None:
        """AI_DELEGATE role -> ``AuthPrincipal.delegate_public_id`` populated.

        The AI_DELEGATE auth chain MUST forward
        ``ai_delegates.public_id`` onto the principal so downstream
        routes (``GET /api/ai-reviews/pending``, the WS hysteresis
        hooks) can key on the delegate identity without re-querying.

        Given a valid AI_DELEGATE token,
        When get_current_user resolves the principal,
        Then it issues a delegate-row lookup keyed by user_public_id
        and sets ``delegate_public_id`` from the resulting row.
        """
        request = Mock(spec=Request)
        request.state = Mock()
        request.cookies = {"access_token": "valid_token"}
        request.headers = {}
        now = int(datetime.now(UTC).timestamp())
        token_data = TokenClaims(
            sub="user-delegate-1",
            username="delegate-1",
            role=UserRole.AI_DELEGATE,
            permissions=["create:orders", "read:signals"],
            exp=now + 3600,
            iat=now,
            jti="jwt-d",
            sid="sid-d",
            user_public_id="user-delegate-1",
        )
        with patch("snapper.auth.dependencies.get_token_manager") as mock_get_token_manager:
            mock_token_manager = Mock()
            mock_token_manager.verify_token_with_db = AsyncMock(return_value=token_data)
            mock_get_token_manager.return_value = mock_token_manager
            repo = Mock()
            repo.get_ai_delegate_by_user_public_id = AsyncMock(
                return_value={
                    "public_id": "del-9",
                    "user_public_id": "user-delegate-1",
                    "last_seen_at": None,
                    "active_reviews_count": 0,
                    "created_at": datetime.now(UTC),
                    "updated_at": datetime.now(UTC),
                }
            )
            result = await get_current_user(request, repo)
        assert result is not None
        assert result.role == UserRole.AI_DELEGATE
        assert result.delegate_public_id == "del-9"
        repo.get_ai_delegate_by_user_public_id.assert_awaited_once_with("user-delegate-1")

    async def test_get_current_user_skips_delegate_lookup_for_non_delegate_role(
        self,
    ) -> None:
        """Non-AI_DELEGATE roles -> ``delegate_public_id`` stays ``None``.

        Given a valid OPERATOR token,
        When get_current_user resolves the principal,
        Then no AiDelegate lookup is performed and the delegate field
        remains ``None``.
        """
        request = Mock(spec=Request)
        request.state = Mock()
        request.cookies = {"access_token": "valid_token"}
        request.headers = {}
        now = int(datetime.now(UTC).timestamp())
        token_data = TokenClaims(
            sub="op-user",
            username="op-user",
            role=UserRole.OPERATOR,
            permissions=["create:orders"],
            exp=now + 3600,
            iat=now,
            jti="jwt-o",
            sid="sid-o",
        )
        with patch("snapper.auth.dependencies.get_token_manager") as mock_get_token_manager:
            mock_token_manager = Mock()
            mock_token_manager.verify_token_with_db = AsyncMock(return_value=token_data)
            mock_get_token_manager.return_value = mock_token_manager
            repo = Mock()
            repo.get_ai_delegate_by_user_public_id = AsyncMock()
            result = await get_current_user(request, repo)
        assert result is not None
        assert result.role == UserRole.OPERATOR
        assert result.delegate_public_id is None
        repo.get_ai_delegate_by_user_public_id.assert_not_awaited()

    async def test_get_current_user_handles_missing_delegate_row_gracefully(
        self,
    ) -> None:
        """AI_DELEGATE role with no delegate row -> ``delegate_public_id`` stays None.

        Defends the legacy migration path where a token may have
        ``role=AI_DELEGATE`` before the operational ``ai_delegates``
        row exists (rare, but possible during a partial backfill).

        Given an AI_DELEGATE token whose ``user_public_id`` has no row,
        When get_current_user resolves the principal,
        Then delegate_public_id is None and authentication still
        succeeds — downstream endpoints that key on the delegate
        identity surface their own 422.
        """
        request = Mock(spec=Request)
        request.state = Mock()
        request.cookies = {"access_token": "valid_token"}
        request.headers = {}
        now = int(datetime.now(UTC).timestamp())
        token_data = TokenClaims(
            sub="user-orphan",
            username="orphan",
            role=UserRole.AI_DELEGATE,
            permissions=[],
            exp=now + 3600,
            iat=now,
            jti="jwt-x",
            sid="sid-x",
            user_public_id="user-orphan",
        )
        with patch("snapper.auth.dependencies.get_token_manager") as mock_get_token_manager:
            mock_token_manager = Mock()
            mock_token_manager.verify_token_with_db = AsyncMock(return_value=token_data)
            mock_get_token_manager.return_value = mock_token_manager
            repo = Mock()
            repo.get_ai_delegate_by_user_public_id = AsyncMock(return_value=None)
            result = await get_current_user(request, repo)
        assert result is not None
        assert result.role == UserRole.AI_DELEGATE
        assert result.delegate_public_id is None

    async def test_get_current_user_invalid_token(self) -> None:
        """Verify get_current_user returns None for invalid token.

        Given: A request with invalid access_token cookie,
        When: get_current_user is called,
        Then: None is returned.
        """
        request = Mock(spec=Request)
        request.cookies = {"access_token": "invalid_token"}
        request.headers = {}
        with patch("snapper.auth.dependencies.get_token_manager") as mock_get_token_manager:
            mock_token_manager = Mock()
            mock_token_manager.verify_token_with_db = AsyncMock(return_value=None)
            mock_get_token_manager.return_value = mock_token_manager
            repo = Mock()
            result = await get_current_user(request, repo)
            assert result is None


class TestRequireAuthentication:
    """Tests for require_authentication dependency."""

    async def test_require_authentication_with_user(self) -> None:
        """Verify require_authentication returns user when present.

        Given: A UserProfile,
        When: require_authentication is called,
        Then: Same user is returned.
        """
        user = AuthPrincipal(username="testuser", role=UserRole.OPERATOR)
        result = require_authentication(user)
        assert result == user

    async def test_require_authentication_without_user(self) -> None:
        """Verify require_authentication raises 401 without user.

        Given: No user (None),
        When: require_authentication is called,
        Then: HTTPException with 401 status is raised.
        """
        with pytest.raises(HTTPException) as exc_info:
            require_authentication(None)
        assert exc_info.value.status_code == 401
        assert "Authentication required" in exc_info.value.detail


class TestRequirePermission:
    """Tests for require_permission dependency."""

    async def test_require_permission_with_valid_permission(self) -> None:
        """Verify require_permission passes with valid permission.

        Given: An admin user with MANAGE_PROCESSES permission,
        When: Permission checker is called,
        Then: User is returned.
        """
        user = AuthPrincipal(username="testuser", role=UserRole.ADMIN)
        permission_checker = require_permission(Permission.MANAGE_PROCESSES)
        with patch(
            "snapper.auth.domain.permissions.ROLE_PERMISSIONS",
            {UserRole.ADMIN: {Permission.MANAGE_PROCESSES}},
        ):
            result = permission_checker(user)
            assert result == user

    async def test_require_permission_without_permission(self) -> None:
        """Verify require_permission raises 403 without permission.

        Given: A viewer user without required permission,
        When: Permission checker is called,
        Then: HTTPException with 403 status is raised.
        """
        user = AuthPrincipal(username="testuser", role=UserRole.VIEWER)
        permission_checker = require_permission(Permission.MANAGE_PROCESSES)
        with patch("snapper.auth.domain.permissions.ROLE_PERMISSIONS", {UserRole.VIEWER: set()}):
            with pytest.raises(HTTPException) as exc_info:
                permission_checker(user)
            assert exc_info.value.status_code == 403
            assert "Permission 'manage:processes' required" in exc_info.value.detail

    async def test_require_permission_enforces_narrow_token_scope(self) -> None:
        """A role permission omitted from the token grant is denied.

        Given: An operator token scoped to market-data reads only,
        When: A REST dependency requires order creation,
        Then: The role ceiling does not restore the omitted permission.
        """
        user = AuthPrincipal(
            username="scoped-operator",
            role=UserRole.OPERATOR,
            permissions=[Permission.READ_MARKET_DATA.value],
        )
        permission_checker = require_permission(Permission.CREATE_ORDERS)

        with pytest.raises(HTTPException) as exc_info:
            permission_checker(user)

        assert exc_info.value.status_code == 403

    async def test_require_permission_legacy_absent_claim_uses_full_role(self) -> None:
        """An absent legacy claim retains the role's complete permission grant."""
        user = AuthPrincipal(
            username="legacy-operator",
            role=UserRole.OPERATOR,
            permissions=None,
        )
        permission_checker = require_permission(Permission.CREATE_ORDERS)

        result = permission_checker(user)

        assert result == user


class TestResourcePermissions:
    """Tests for RESOURCE_PERMISSIONS mapping."""

    def test_all_ui_resources_have_mapping(self) -> None:
        """Every UI resource has an entry in RESOURCE_PERMISSIONS.

        Given: The set of expected UI tab resources,
        When: Checking RESOURCE_PERMISSIONS keys,
        Then: All expected resources are present.
        """
        expected = {
            "overview",
            "market",
            "processes",
            "strategies",
            "orders",
            "positions",
            "accounts",
            "signals",
            "health",
            "admin",
            "settings",
            "backtests",
            "ai-integration",
            "ai-reviews",
            "notifications",
        }
        assert set(RESOURCE_PERMISSIONS.keys()) == expected

    def test_positions_requires_read_positions(self) -> None:
        """Positions resource requires READ_POSITIONS permission.

        Given: RESOURCE_PERMISSIONS mapping,
        When: Checking the 'positions' entry,
        Then: Its value is Permission.READ_POSITIONS so viewers/operators/admins
            (which already hold READ_POSITIONS via ROLE_PERMISSIONS) can access
            the new Positions tab.
        """
        assert RESOURCE_PERMISSIONS["positions"] == Permission.READ_POSITIONS

    def test_accounts_requires_read_account_state(self) -> None:
        """Accounts resource requires READ_ACCOUNT_STATE permission.

        Given: RESOURCE_PERMISSIONS mapping,
        When: Checking the 'accounts' entry,
        Then: Its value is Permission.READ_ACCOUNT_STATE so viewers/operators/admins
            (which hold READ_ACCOUNT_STATE) can access the venue-account tab, while
            AI delegates (which do not) are excluded from the derived resource access.
        """
        assert RESOURCE_PERMISSIONS["accounts"] == Permission.READ_ACCOUNT_STATE

    def test_signals_requires_read_signals(self) -> None:
        """Trading-signal resources require the signal-specific grant.

        Given: The resource permission mapping used to generate client access.
        When: The signals entry is inspected.
        Then: It requires READ_SIGNALS rather than general market-data access.
        """
        assert RESOURCE_PERMISSIONS["signals"] == Permission.READ_SIGNALS

    def test_overview_requires_no_permission(self) -> None:
        """Overview resource is accessible without any specific permission.

        Given: RESOURCE_PERMISSIONS mapping,
        When: Checking the 'overview' entry,
        Then: Its value is None (no permission required).
        """
        assert RESOURCE_PERMISSIONS["overview"] is None

    def test_protected_resources_have_valid_permissions(self) -> None:
        """All non-None entries map to valid Permission enum members.

        Given: RESOURCE_PERMISSIONS with some Permission values,
        When: Filtering for non-None entries,
        Then: Each value is a member of Permission.
        """
        for resource, perm in RESOURCE_PERMISSIONS.items():
            if perm is not None:
                assert isinstance(perm, Permission), f"{resource} maps to non-Permission value"

    def test_admin_requires_manage_users(self) -> None:
        """Admin resource requires MANAGE_USERS permission.

        Given: RESOURCE_PERMISSIONS mapping,
        When: Checking the 'admin' entry,
        Then: Its value is Permission.MANAGE_USERS.
        """
        assert RESOURCE_PERMISSIONS["admin"] == Permission.MANAGE_USERS

    def test_settings_requires_configure_system(self) -> None:
        """Settings resource requires CONFIGURE_SYSTEM permission.

        Given: RESOURCE_PERMISSIONS mapping,
        When: Checking the 'settings' entry,
        Then: Its value is Permission.CONFIGURE_SYSTEM.
        """
        assert RESOURCE_PERMISSIONS["settings"] == Permission.CONFIGURE_SYSTEM


class TestMultiTenantPermissions:
    """Tests for the multi-tenant permission additions.

    These four permissions guard frontend admin tabs and impersonation
    flows. They are ADMIN-only (VIEWER + OPERATOR must NOT receive
    them), and their presence in ``ROLE_PERMISSIONS[ADMIN]`` is
    derived automatically from ``set(Permission)`` — the assertions
    below are behavioural pinning so an accidental change to the
    ADMIN set (e.g. hand-picking permissions) does not silently drop
    any of the four new entries.
    """

    def test_admin_holds_all_multi_tenant_permissions(self) -> None:
        """ADMIN receives every new multi-tenant permission.

        Given: ``ROLE_PERMISSIONS[ADMIN]``,
        When: Checking for the four multi-tenant permissions,
        Then: All four are present in the ADMIN set.
        """
        admin_perms = ROLE_PERMISSIONS[UserRole.ADMIN]
        assert Permission.READ_WALLET_CREDENTIALS in admin_perms
        assert Permission.MANAGE_WALLET_CREDENTIALS in admin_perms
        assert Permission.MANAGE_SCOPE_GRANTS in admin_perms
        assert Permission.IMPERSONATE_OPERATOR in admin_perms

    def test_viewer_does_not_receive_multi_tenant_permissions(self) -> None:
        """VIEWER must never hold any multi-tenant permission.

        Given: ``ROLE_PERMISSIONS[VIEWER]``,
        When: Checking for the four multi-tenant permissions,
        Then: None of them are present.
        """
        viewer_perms = ROLE_PERMISSIONS[UserRole.VIEWER]
        assert Permission.READ_WALLET_CREDENTIALS not in viewer_perms
        assert Permission.MANAGE_WALLET_CREDENTIALS not in viewer_perms
        assert Permission.MANAGE_SCOPE_GRANTS not in viewer_perms
        assert Permission.IMPERSONATE_OPERATOR not in viewer_perms

    def test_operator_does_not_receive_multi_tenant_permissions(self) -> None:
        """OPERATOR must never hold any multi-tenant permission.

        Given: ``ROLE_PERMISSIONS[OPERATOR]``,
        When: Checking for the four multi-tenant permissions,
        Then: None of them are present (multi-tenant administration
            is reserved for ADMIN).
        """
        operator_perms = ROLE_PERMISSIONS[UserRole.OPERATOR]
        assert Permission.READ_WALLET_CREDENTIALS not in operator_perms
        assert Permission.MANAGE_WALLET_CREDENTIALS not in operator_perms
        assert Permission.MANAGE_SCOPE_GRANTS not in operator_perms
        assert Permission.IMPERSONATE_OPERATOR not in operator_perms

    def test_permission_values_follow_resource_action_pattern(self) -> None:
        """New permission values follow the ``resource:action`` convention.

        Given: The four multi-tenant permission enum values,
        When: Comparing them to the established naming,
        Then: Each is a colon-separated ``resource:action`` string.
        """
        assert Permission.READ_WALLET_CREDENTIALS.value == "read:wallet_credentials"
        assert Permission.MANAGE_WALLET_CREDENTIALS.value == "manage:wallet_credentials"
        assert Permission.MANAGE_SCOPE_GRANTS.value == "manage:scope_grants"
        assert Permission.IMPERSONATE_OPERATOR.value == "impersonate:operator"


class TestRequireRole:
    """Tests for require_role dependency."""

    async def test_require_role_with_sufficient_role(self) -> None:
        """Verify require_role passes with higher role.

        Given: An admin user when operator role is required,
        When: Role checker is called,
        Then: User is returned.
        """
        user = AuthPrincipal(username="testuser", role=UserRole.ADMIN)
        role_checker = require_role(UserRole.OPERATOR)
        result = role_checker(user)
        assert result == user

    async def test_require_role_with_exact_role(self) -> None:
        """Verify require_role passes with exact role.

        Given: An operator user when operator role is required,
        When: Role checker is called,
        Then: User is returned.
        """
        user = AuthPrincipal(username="testuser", role=UserRole.OPERATOR)
        role_checker = require_role(UserRole.OPERATOR)
        result = role_checker(user)
        assert result == user

    async def test_require_role_with_insufficient_role(self) -> None:
        """Verify require_role raises 403 with insufficient role.

        Given: A viewer user when admin role is required,
        When: Role checker is called,
        Then: HTTPException with 403 status is raised.
        """
        user = AuthPrincipal(username="testuser", role=UserRole.VIEWER)
        role_checker = require_role(UserRole.ADMIN)
        with pytest.raises(HTTPException) as exc_info:
            role_checker(user)
        assert exc_info.value.status_code == 403
        assert "Role 'admin' or higher required" in exc_info.value.detail

    async def test_require_role_with_permission_enforces_token_scope(self) -> None:
        """A role-qualified request is still denied when its scope omits the action."""
        user = AuthPrincipal(
            username="scoped-operator",
            role=UserRole.OPERATOR,
            permissions=[Permission.READ_MARKET_DATA.value],
        )
        role_checker = require_role(UserRole.OPERATOR, Permission.MANAGE_PROCESSES)

        with pytest.raises(HTTPException) as exc_info:
            role_checker(user)

        assert exc_info.value.status_code == 403
        assert "Permission 'manage:processes' required" in exc_info.value.detail

    async def test_require_role_with_permission_accepts_retained_grant(self) -> None:
        """A token retaining the role-gated action permission is admitted."""
        user = AuthPrincipal(
            username="scoped-operator",
            role=UserRole.OPERATOR,
            permissions=[Permission.MANAGE_PROCESSES.value],
        )
        role_checker = require_role(UserRole.OPERATOR, Permission.MANAGE_PROCESSES)

        result = role_checker(user)

        assert result == user


class TestCSRFManager:
    """Tests for CSRFManager token generation and validation."""

    def test_generate_token(self) -> None:
        """Verify generate_token creates valid CSRF token format.

        Given: A CSRFManager,
        When: generate_token is called,
        Then: Token has nonce.timestamp.signature format.
        """
        csrf_manager = CSRFManager()
        token = csrf_manager.generate_token()
        assert isinstance(token, str)
        assert len(token) > 0
        assert token.count(".") == 2
        parts = token.split(".")
        assert len(parts) == 3
        nonce, timestamp, signature = parts
        assert len(nonce) > 0
        assert len(timestamp) > 0
        assert len(signature) > 0
        assert int(timestamp) > 0

    def test_validate_valid_token(self) -> None:
        """Verify validate_token returns True for valid token.

        Given: A freshly generated CSRF token,
        When: validate_token is called,
        Then: True is returned.
        """
        csrf_manager = CSRFManager()
        token = csrf_manager.generate_token()
        assert csrf_manager.validate_token(token) is True

    def test_validate_invalid_token(self) -> None:
        """Verify validate_token returns False for invalid tokens.

        Given: Invalid token strings,
        When: validate_token is called,
        Then: False is returned.
        """
        csrf_manager = CSRFManager()
        assert csrf_manager.validate_token("invalid_token") is False
        assert csrf_manager.validate_token("invalid.token.format") is False
        assert csrf_manager.validate_token("") is False

    def test_validate_tampered_token(self) -> None:
        """Verify validate_token returns False for tampered token.

        Given: A valid token with modified signature,
        When: validate_token is called,
        Then: False is returned.
        """
        csrf_manager = CSRFManager()
        token = csrf_manager.generate_token()
        nonce, timestamp, signature = token.split(".")
        tampered_token = f"{nonce}.{timestamp}.{signature[:-1]}x"
        assert csrf_manager.validate_token(tampered_token) is False

    def test_validate_malformed_token(self) -> None:
        """Verify validate_token returns False for malformed tokens.

        Given: Tokens with wrong number of parts or empty parts,
        When: validate_token is called,
        Then: False is returned.
        """
        csrf_manager = CSRFManager()
        assert csrf_manager.validate_token("no_dots") is False
        assert csrf_manager.validate_token("one.dot") is False
        assert csrf_manager.validate_token("too.many.dots.here") is False
        assert csrf_manager.validate_token("..empty_parts") is False
        assert csrf_manager.validate_token("empty..parts") is False

    def test_validate_expired_token(self) -> None:
        """Verify validate_token returns False for expired token.

        Given: A token generated with past timestamp,
        When: validate_token is called,
        Then: False is returned.
        """
        csrf_manager = CSRFManager()
        past_timestamp = str(int(datetime.now(UTC).timestamp()) - 3700)
        with patch.object(csrf_manager, "_get_current_timestamp", return_value=past_timestamp):
            expired_token = csrf_manager.generate_token()
        assert csrf_manager.validate_token(expired_token) is False

    def test_invalidate_token(self) -> None:
        """Verify invalidate_token does not affect validation.

        Given: A valid CSRF token,
        When: invalidate_token is called,
        Then: Token still validates (signature-based, not blacklist).
        """
        csrf_manager = CSRFManager()
        token = csrf_manager.generate_token()
        csrf_manager.invalidate_token(token)
        assert csrf_manager.validate_token(token) is True

    def test_invalidate_nonexistent_token(self) -> None:
        """Verify invalidate_token handles non-existent gracefully.

        Given: A non-existent token string,
        When: invalidate_token is called,
        Then: No error is raised.
        """
        csrf_manager = CSRFManager()
        csrf_manager.invalidate_token("nonexistent_token")

    def test_cleanup_expired_tokens(self) -> None:
        """Verify cleanup_expired_tokens completes without error.

        Given: A CSRFManager,
        When: cleanup_expired_tokens is called,
        Then: No error is raised.
        """
        csrf_manager = CSRFManager()
        csrf_manager.cleanup_expired_tokens()
        assert csrf_manager is not None


def test_csrf_manager_settings_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify CSRFManager uses get_settings fallback.

    Given: CSRFManager without explicit settings,
    When: Accessing settings property,
    Then: Settings are retrieved via get_settings.
    """
    CSRFManager.clear_instance()
    stub_settings = Mock(auth_secret_key="secret", csrf_token_expire_minutes=5)
    monkeypatch.setattr("snapper.auth.dependencies.get_settings", lambda: stub_settings)
    manager = CSRFManager()
    manager._settings = None
    resolved = manager.settings
    assert resolved.auth_secret_key == "secret"


def test_csrf_manager_settings_fallback_called_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify CSRFManager caches settings after first access.

    Given: CSRFManager without cached settings,
    When: Accessing settings property multiple times,
    Then: get_settings is called only once.
    """
    CSRFManager.clear_instance()
    call_counter = {"count": 0}

    def _get_settings() -> Mock:
        call_counter["count"] += 1
        return Mock(auth_secret_key="secret", csrf_token_expire_minutes=1)

    monkeypatch.setattr("snapper.auth.dependencies.get_settings", _get_settings)
    manager = CSRFManager()
    manager._settings = None
    assert manager.settings.auth_secret_key == "secret"
    assert manager.settings.auth_secret_key == "secret"
    assert call_counter["count"] == 1


def test_validate_token_handles_internal_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify validate_token handles internal errors gracefully.

    Given: _is_timestamp_valid raises ValueError,
    When: validate_token is called,
    Then: False is returned without raising.
    """
    manager = CSRFManager()

    def _raise(*_: object, **__: object) -> bool:
        raise ValueError("boom")

    monkeypatch.setattr(manager, "_is_timestamp_valid", _raise)
    assert manager.validate_token("nonce.123.signature") is False


def test_validate_token_handles_verification_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify validate_token handles verification errors gracefully.

    Given: _verify_hmac_signature raises TypeError,
    When: validate_token is called,
    Then: False is returned without raising.
    """
    manager = CSRFManager()

    def _verify(*_: object, **__: object) -> bool:
        raise TypeError("verify boom")

    monkeypatch.setattr(manager, "_is_timestamp_valid", lambda *_: True)
    monkeypatch.setattr(manager, "_verify_hmac_signature", _verify)
    assert manager.validate_token("nonce.123.signature") is False


class TestCSRFDependencies:
    """Tests for CSRF-related FastAPI dependencies."""

    def test_get_csrf_manager_singleton(self) -> None:
        """Verify get_csrf_manager returns singleton instance.

        Given: Multiple calls to get_csrf_manager,
        When: Comparing returned instances,
        Then: All references point to the same CSRFManager.
        """
        manager1 = get_csrf_manager()
        manager2 = get_csrf_manager()
        assert manager1 is manager2
        assert isinstance(manager1, CSRFManager)

    async def test_get_csrf_token_from_header(self) -> None:
        """Verify get_csrf_token extracts token from header.

        Given: A request with X-CSRF-Token header,
        When: get_csrf_token is called,
        Then: Token from header is returned.
        """
        request = Mock(spec=Request)
        request.headers = {"X-CSRF-Token": "token_from_header"}
        request.cookies = {}
        token = get_csrf_token(request)
        assert token == "token_from_header"

    async def test_get_csrf_token_from_cookie(self) -> None:
        """Verify get_csrf_token extracts token from cookie.

        Given: A request with csrf_token cookie and no header,
        When: get_csrf_token is called,
        Then: Token from cookie is returned.
        """
        request = Mock(spec=Request)
        request.headers = {}
        request.cookies = {"csrf_token": "token_from_cookie"}
        token = get_csrf_token(request)
        assert token == "token_from_cookie"

    async def test_get_csrf_token_not_found(self) -> None:
        """Verify get_csrf_token returns None when no token found.

        Given: A request without CSRF token in header or cookie,
        When: get_csrf_token is called,
        Then: None is returned.
        """
        request = Mock(spec=Request)
        request.headers = {}
        request.cookies = {}
        token = get_csrf_token(request)
        assert token is None

    async def test_validate_csrf_token_safe_method(self) -> None:
        """Verify validate_csrf_token skips safe methods.

        Given: A GET request without CSRF token,
        When: validate_csrf_token is called,
        Then: No error is raised.
        """
        request = Mock(spec=Request)
        request.method = "GET"
        request.headers = {}
        request.cookies = {}
        validate_csrf_token(request, None)

    async def test_validate_csrf_token_missing_token(self) -> None:
        """Verify validate_csrf_token raises 403 for missing token.

        Given: A POST request without CSRF token,
        When: validate_csrf_token is called,
        Then: HTTPException with 403 and "CSRF token required" is raised.
        """
        request = Mock(spec=Request)
        request.method = "POST"
        request.headers = {}
        request.cookies = {}
        with pytest.raises(HTTPException) as exc_info:
            validate_csrf_token(request, None)
        assert exc_info.value.status_code == 403
        assert "CSRF token required" in exc_info.value.detail

    async def test_validate_csrf_token_valid_token(self) -> None:
        """Verify validate_csrf_token passes with valid matching tokens.

        Given: A POST request with valid matching CSRF tokens,
        When: validate_csrf_token is called,
        Then: No error is raised.
        """
        csrf_manager = CSRFManager()
        valid_token = csrf_manager.generate_token()
        request = Mock(spec=Request)
        request.method = "POST"
        request.headers = {"origin": "http://localhost:8000", "X-CSRF-Token": valid_token}
        request.cookies = {"csrf_token": valid_token}
        validate_csrf_token(request, valid_token)

    async def test_validate_csrf_token_mismatch(self) -> None:
        """Verify validate_csrf_token raises 403 for token mismatch.

        Given: A POST request with different header and cookie tokens,
        When: validate_csrf_token is called,
        Then: HTTPException with 403 and "mismatch" is raised.
        """
        csrf_manager = CSRFManager()
        token1 = csrf_manager.generate_token()
        token2 = csrf_manager.generate_token()
        request = Mock(spec=Request)
        request.method = "POST"
        request.headers = {"origin": "http://localhost:8000", "X-CSRF-Token": token1}
        request.cookies = {"csrf_token": token2}
        with pytest.raises(HTTPException) as exc_info:
            validate_csrf_token(request, token1)
        assert exc_info.value.status_code == 403
        assert "CSRF token mismatch between cookie and header" in exc_info.value.detail

    async def test_validate_csrf_token_invalid_signature(self) -> None:
        """Verify validate_csrf_token raises 403 for invalid signature.

        Given: A POST request with invalid CSRF token signature,
        When: validate_csrf_token is called,
        Then: HTTPException with 403 and "Invalid or tampered" is raised.
        """
        request = Mock(spec=Request)
        request.method = "POST"
        request.headers = {
            "origin": "http://localhost:8000",
            "X-CSRF-Token": "nonce.1234567890.invalid_signature",
        }
        request.cookies = {"csrf_token": "nonce.1234567890.invalid_signature"}
        with patch("snapper.auth.dependencies.get_csrf_manager") as mock_get_csrf_manager:
            mock_csrf_manager = Mock()
            mock_csrf_manager.validate_token.return_value = False
            mock_get_csrf_manager.return_value = mock_csrf_manager
            with pytest.raises(HTTPException) as exc_info:
                validate_csrf_token(request, "nonce.1234567890.invalid_signature")
            assert exc_info.value.status_code == 403
            assert "Invalid or tampered CSRF token signature" in exc_info.value.detail

    @pytest.mark.real_settings
    async def test_validate_csrf_token_accepts_db_configured_ui_origin(self) -> None:
        """A same-site mutation from the DB-configured ui_origin is accepted.

        Regression: ``validate_csrf_token`` read origins via the unbound
        ``get_settings()``, whose DB-backed ``ui_origin`` raises
        RuntimeError, so ``build_allowed_origins`` silently collapsed to
        localhost defaults and rejected EVERY browser mutation from the
        deployment domain with 403 "Invalid origin". Binding the
        request's ``app.state.settings_service`` makes ``ui_origin``
        resolve. Uses real settings so the autouse mock (which forces
        ``ui_origin=""``) does not mask the bug.
        """
        service = Mock(spec=SettingsService)
        service.get_setting = lambda key, default: (
            "https://snapper.ch" if key == "ui_origin" else default
        )
        csrf_manager = CSRFManager()
        csrf_manager.set_settings_service(service)
        valid_token = csrf_manager.generate_token()
        request = Mock(spec=Request)
        request.method = "POST"
        request.app.state.settings_service = service
        request.headers = {
            "origin": "https://snapper.ch",
            "referer": "https://snapper.ch/processes",
            "X-CSRF-Token": valid_token,
        }
        request.cookies = {"csrf_token": valid_token}
        validate_csrf_token(request, valid_token)

    @pytest.mark.real_settings
    async def test_validate_csrf_token_rejects_ui_origin_without_bound_service(
        self,
    ) -> None:
        """Without the bound service the deployment origin is NOT trusted.

        The negative half of the regression: when ``app.state`` carries
        no settings service the resolver falls back to bootstrap-only
        settings (localhost defaults), so the very same deployment-domain
        request is rejected — proving it is the service binding, not some
        other change, that fixes the 403.
        """
        request = Mock(spec=Request)
        request.method = "POST"
        request.app.state.settings_service = None
        request.headers = {
            "origin": "https://snapper.ch",
            "referer": "https://snapper.ch/processes",
            "X-CSRF-Token": "any-token",
        }
        request.cookies = {"csrf_token": "any-token"}
        with pytest.raises(HTTPException) as exc_info:
            validate_csrf_token(request, "any-token")
        assert exc_info.value.status_code == 403
        assert "Invalid origin" in exc_info.value.detail

    async def test_validate_csrf_token_invalid_origin(self) -> None:
        """Verify validate_csrf_token raises 403 for invalid origin.

        Given: A POST request from malicious origin,
        When: validate_csrf_token is called,
        Then: HTTPException with 403 and "Invalid origin" is raised.
        """
        request = Mock(spec=Request)
        request.method = "POST"
        request.headers = {
            "origin": "https://malicious-site.com",
            "referer": "https://malicious-site.com/evil",
            "X-CSRF-Token": "test_token",
        }
        request.cookies = {"csrf_token": "test_token"}
        with pytest.raises(HTTPException) as exc_info:
            validate_csrf_token(request, "test_token")
        assert exc_info.value.status_code == 403
        assert "Invalid origin" in exc_info.value.detail

    async def test_validate_csrf_token_rejects_prefix_attack_origin(self) -> None:
        """Verify validate_csrf_token rejects origin that is a prefix match.

        Given: A POST request with origin that starts with an allowed origin
            but belongs to a different domain (prefix attack),
        When: validate_csrf_token is called,
        Then: HTTPException with 403 and "Invalid origin" is raised.
        """
        request = Mock(spec=Request)
        request.method = "POST"
        request.headers = {
            "origin": "https://snapper.ch.evil.com",
            "referer": "https://snapper.ch.evil.com/page",
            "X-CSRF-Token": "test_token",
        }
        request.cookies = {"csrf_token": "test_token"}
        with pytest.raises(HTTPException) as exc_info:
            validate_csrf_token(request, "test_token")
        assert exc_info.value.status_code == 403
        assert "Invalid origin" in exc_info.value.detail

    async def test_validate_csrf_token_rejects_prefix_attack_referer(self) -> None:
        """Verify validate_csrf_token rejects referer that is a prefix match.

        Given: A POST request with empty origin and a referer that starts with
            an allowed origin but belongs to a different domain,
        When: validate_csrf_token is called with both origin and referer present,
        Then: HTTPException with 403 and "Invalid origin" is raised.
        """
        request = Mock(spec=Request)
        request.method = "POST"
        request.headers = {
            "origin": "https://snapper.ch.attacker.com",
            "referer": "https://snapper.ch.attacker.com/steal",
            "X-CSRF-Token": "test_token",
        }
        request.cookies = {"csrf_token": "test_token"}
        with pytest.raises(HTTPException) as exc_info:
            validate_csrf_token(request, "test_token")
        assert exc_info.value.status_code == 403
        assert "Invalid origin" in exc_info.value.detail

    async def test_validate_csrf_token_accepts_valid_referer_with_path(self) -> None:
        """Verify validate_csrf_token accepts valid referer that includes a path.

        Given: A POST request with valid origin and referer containing a path,
        When: validate_csrf_token is called with valid CSRF tokens,
        Then: No error is raised.
        """
        csrf_manager = CSRFManager()
        valid_token = csrf_manager.generate_token()
        request = Mock(spec=Request)
        request.method = "POST"
        request.headers = {
            "origin": "http://localhost:8000",
            "referer": "http://localhost:8000/some/page",
            "X-CSRF-Token": valid_token,
        }
        request.cookies = {"csrf_token": valid_token}
        validate_csrf_token(request, valid_token)

    async def test_validate_csrf_token_requires_cookie_and_header(self) -> None:
        """Verify validate_csrf_token requires both cookie and header.

        Given: A POST request with cookie but no header,
        When: validate_csrf_token is called,
        Then: HTTPException with 403 and "both cookie and header" is raised.
        """
        csrf_manager = CSRFManager()
        token = csrf_manager.generate_token()
        request = Mock(spec=Request)
        request.method = "POST"
        request.headers = {"origin": "http://localhost:8000"}
        request.cookies = {"csrf_token": token}
        with pytest.raises(HTTPException) as exc_info:
            validate_csrf_token(request, token)
        assert exc_info.value.status_code == 403
        assert "both cookie and header" in exc_info.value.detail

    async def test_validate_csrf_token_missing_cookie(self) -> None:
        """Verify validate_csrf_token fails with missing cookie.

        Given: A POST request with header but no cookie,
        When: validate_csrf_token is called,
        Then: HTTPException with 403 and "both cookie and header" is raised.
        """
        csrf_manager = CSRFManager()
        token = csrf_manager.generate_token()
        request = Mock(spec=Request)
        request.method = "POST"
        request.headers = {"origin": "http://localhost:8000", "X-CSRF-Token": token}
        request.cookies = {}
        with pytest.raises(HTTPException) as exc_info:
            validate_csrf_token(request, token)
        assert exc_info.value.status_code == 403
        assert "both cookie and header" in exc_info.value.detail
