"""Tests for WebSocket helper functions."""

from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.auth.domain.roles import UserRole
from snapper.auth.tokens import TokenManager
from snapper.auth.user_service import UserService
from snapper.interface.websocket.helpers import build_allowed_origins
from snapper.interface.websocket.helpers import determine_topic_category
from snapper.interface.websocket.helpers import get_allowed_topics_for_role
from snapper.interface.websocket.helpers import parse_wallet_scoped_topic
from snapper.interface.websocket.helpers import role_allowed_categories
from snapper.messaging.infrastructure.logger import ZmqMessageLogger
from snapper.utils.logging import _get_context_bg_color


class TestTokenManagerSettingsFallback:
    """Tests for TokenManager settings fallback behavior."""

    def test_settings_fallback_when_not_initialized(self) -> None:
        """TokenManager settings fallback when not initialized.

        Given: A TokenManager with _settings set to None,
        When: Accessing settings property,
        Then: Settings are fetched via get_settings() and have auth_secret_key.
        """
        manager = TokenManager()
        manager._settings = None
        settings = manager.settings
        assert settings is not None
        assert hasattr(settings, "auth_secret_key")


class TestTokenManagerVerifyTokenInvalid:
    """Tests for TokenManager.verify_token with invalid tokens."""

    def test_verify_token_returns_false_for_invalid_token(self) -> None:
        """Verify token returns None for invalid token.

        Given: A TokenManager,
        When: Verifying an invalid token string,
        Then: Returns None.
        """
        manager = TokenManager()
        result = manager.verify_token("invalid.token.here")
        assert result is None


class TestUserServiceReInitialization:
    """Tests for UserService re-initialization behavior."""

    def test_user_service_skips_reinitialization(self) -> None:
        """UserService skips repository re-initialization.

        Given: An initialized UserService,
        When: __init__ is called again,
        Then: The original repository is preserved.
        """
        with patch("snapper.auth.user_service.get_repository") as mock_get_repo:
            mock_repo = MagicMock()
            mock_get_repo.return_value = mock_repo
            service = UserService()
            original_repo = service.repository
            mock_get_repo.reset_mock()
            UserService.__init__(service)
            assert service.repository is original_repo
            mock_get_repo.assert_not_called()


class TestWebSocketHelpersRuntimeError:
    """Tests for WebSocket helper error handling."""

    def test_build_allowed_origins_handles_runtime_error(self) -> None:
        """Build allowed origins handles RuntimeError from settings.

        Given: Settings that raise RuntimeError on property access,
        When: Building allowed origins,
        Then: Falls back to localhost origins.
        """
        mock_settings = MagicMock()
        type(mock_settings).ui_origin = property(
            lambda self: (_ for _ in ()).throw(RuntimeError("Not initialized"))
        )
        type(mock_settings).session_domain = property(
            lambda self: (_ for _ in ()).throw(RuntimeError("Not initialized"))
        )
        origins = build_allowed_origins(mock_settings, 8000)
        assert isinstance(origins, set)
        assert "http://localhost:8000" in origins

    def test_build_allowed_origins_parses_comma_separated_ui_origin(self) -> None:
        """Build allowed origins parses comma-separated ui_origin values.

        Given: Settings with a comma-separated ui_origin string containing empty entries,
        When: Building allowed origins,
        Then: Each non-empty origin is added individually to the set.
        """
        mock_settings = MagicMock()
        mock_settings.ui_origin = "http://example.com:3000, , http://example.com:8000,"
        mock_settings.session_domain = ""
        origins = build_allowed_origins(mock_settings, 8000)
        assert "http://example.com:3000" in origins
        assert "http://example.com:8000" in origins

    def test_role_allowed_categories_returns_empty_for_unknown_role(self) -> None:
        """Role allowed categories returns empty set for unknown role.

        Given: A mock role with unknown name,
        When: Getting allowed categories,
        Then: Returns empty set (no permissions match).
        """
        mock_role = MagicMock(spec=[])
        mock_role.name = "UNKNOWN"
        result = role_allowed_categories(mock_role)
        assert result == set()

    def test_determine_topic_category_covers_direct_prefixes(self) -> None:
        """Determine topic category handles direct category-only names.

        Given: Direct category topic names without dot separators,
        When: Determining category,
        Then: Matching category names are returned.
        """
        assert determine_topic_category("market") == "market"
        assert determine_topic_category("signals") == "signals"
        assert determine_topic_category("system") == "system"
        assert determine_topic_category("admin") == "admin"

    def test_determine_topic_category_handles_partial_market_prefix(self) -> None:
        """Determine topic category maps partial market prefixes.

        Given: A market prefix-like topic that is not a full market message,
        When: Determining category,
        Then: Category resolves to market via prefix fallback.
        """
        assert determine_topic_category("market.kraken.") == "market"

    def test_determine_topic_category_backtest(self) -> None:
        """Backtest topics resolve to ``backtest`` at every depth."""
        assert determine_topic_category("backtest") == "backtest"
        assert determine_topic_category("backtest.") == "backtest"
        assert determine_topic_category("backtest.wallet-uuid.") == "backtest"
        assert determine_topic_category("backtest.wallet-uuid.run-uuid.") == "backtest"
        assert determine_topic_category("backtest.wallet.run.started") == "backtest"


class TestRoleCategorySecurityMatrix:
    """Role x category x topic security boundary tests.

    Verifies that each role gets exactly the expected WS categories
    and topic patterns, with no over- or under-provisioning.
    """

    def test_viewer_gets_market_system_and_backtest(self) -> None:
        """VIEWER role receives read-only categories (market, system, backtest).

        Given: A VIEWER role,
        When: Getting allowed categories,
        Then: market, system, backtest are allowed (no trade, strategy, admin).
        """
        categories = role_allowed_categories(UserRole.VIEWER)
        assert categories == {"market", "system", "backtest"}

    def test_operator_gets_market_trade_signals_strategy_system_backtest(self) -> None:
        """OPERATOR receives market, trade, signals, strategy, system, backtest, ai_reviews.

        Given: An OPERATOR role,
        When: Getting allowed categories,
        Then: market, trade, signals, strategy, system, backtest, ai_reviews
            allowed but NOT admin. The ``signals`` category is separate from
            ``strategy`` so AI_DELEGATE (READ_SIGNALS only, no
            START_STRATEGIES) can subscribe to ``signals.*`` without the
            operator-level strategy-management surface. ``ai_reviews`` is
            the CONSULT-pattern category — AI_DELEGATE delivers
            review requests + receives decisions over it.
        """
        categories = role_allowed_categories(UserRole.OPERATOR)
        assert categories == {
            "market",
            "trade",
            "signals",
            "strategy",
            "system",
            "backtest",
            "ai_reviews",
        }
        assert "admin" not in categories

    def test_admin_gets_all_categories_including_admin(self) -> None:
        """ADMIN role receives all categories including admin + backtest + ai_reviews.

        Given: An ADMIN role,
        When: Getting allowed categories,
        Then: All eight categories are allowed
            (market, trade, signals, strategy, system, admin, backtest,
            ai_reviews — CONSULT-pattern surface).
        """
        categories = role_allowed_categories(UserRole.ADMIN)
        assert categories == {
            "market",
            "trade",
            "signals",
            "strategy",
            "system",
            "admin",
            "backtest",
            "ai_reviews",
        }

    def test_admin_available_topics_include_admin_prefix(self) -> None:
        """ADMIN available topics include the admin. registry root.

        Given: An ADMIN role,
        When: Getting allowed topics,
        Then: admin. prefix pattern is present in the topic list.
        """
        topics = get_allowed_topics_for_role(UserRole.ADMIN)
        assert "admin." in topics

    def test_operator_available_topics_exclude_admin_prefix(self) -> None:
        """OPERATOR available topics do not include admin. registry root.

        Given: An OPERATOR role,
        When: Getting allowed topics,
        Then: admin. prefix pattern is absent.
        """
        topics = get_allowed_topics_for_role(UserRole.OPERATOR)
        assert "admin." not in topics

    def test_viewer_available_topics_exclude_trade_and_strategy(self) -> None:
        """VIEWER available topics exclude trade and strategy patterns.

        Given: A VIEWER role,
        When: Getting allowed topics,
        Then: No trade or strategy patterns are present.
        """
        topics = get_allowed_topics_for_role(UserRole.VIEWER)
        assert "orders.commands." not in topics
        assert "orders.events." not in topics
        assert "signals." not in topics

    def test_viewer_available_topics_include_market(self) -> None:
        """VIEWER available topics include market pattern.

        Given: A VIEWER role,
        When: Getting allowed topics,
        Then: market. pattern is present.
        """
        topics = get_allowed_topics_for_role(UserRole.VIEWER)
        assert "market." in topics


class TestLoggingColorConversion:
    """Tests for logging color conversion utilities."""

    def test_context_bg_color_hue_60_to_120(self) -> None:
        """Context background color generates valid ANSI codes.

        Given: Various context strings,
        When: Getting background color,
        Then: Returns valid ANSI escape code starting with expected prefix.
        """
        for i in range(100):
            context = f"test_context_{i}"
            result = _get_context_bg_color(context)
            assert result.startswith("\033[48;2;")

    def test_context_bg_color_hue_120_to_180(self) -> None:
        """Context background color handles different context values.

        Given: Various context strings with different hashes,
        When: Getting background color,
        Then: Returns valid ANSI escape code.
        """
        for i in range(100):
            context = f"another_context_{i}"
            result = _get_context_bg_color(context)
            assert result.startswith("\033[48;2;")


class TestZmqLoggerErrorBranches:
    """Tests for ZmqMessageLogger error handling branches."""

    @pytest.mark.asyncio
    async def test_log_message_metadata_only_mode(self) -> None:
        """ZMQ logger logs metadata only when payload logging disabled.

        Given: A ZmqMessageLogger with log_payload=False,
        When: Logging a message,
        Then: Only metadata is logged without payload content.
        """
        logger_instance = ZmqMessageLogger(
            log_to_file=False,
            log_payload=False,
        )
        await logger_instance._log_message("test.topic", b'{"data": "test"}')


class TestParseWalletScopedTopic:
    """Tests for  :func:`parse_wallet_scoped_topic` helper."""

    @pytest.mark.parametrize(
        ("topic", "expected"),
        [
            ("signals.kraken.BTC-USD.live", ("kraken", "BTC-USD")),
            ("orders.commands.kraken.BTC-USD.submit", ("kraken", "BTC-USD")),
            ("orders.commands.kraken.BTC-USD.cancel", ("kraken", "BTC-USD")),
            ("orders.events.kraken.BTC-USD.executed", ("kraken", "BTC-USD")),
            ("orders.events.kraken.BTC-USD.submitted", ("kraken", "BTC-USD")),
            ("orders.events.kraken.BTC-USD.accepted", ("kraken", "BTC-USD")),
            ("orders.events.kraken.BTC-USD.rejected", ("kraken", "BTC-USD")),
            ("orders.events.kraken.BTC-USD.cancelled", ("kraken", "BTC-USD")),
            ("orders.events.kraken.BTC-USD.expired", ("kraken", "BTC-USD")),
            ("orders.events.kraken.BTC-USD.replaced", ("kraken", "BTC-USD")),
        ],
    )
    def test_wallet_scoped_topics_decompose(self, topic: str, expected: tuple[str, str]) -> None:
        """Positive: every wallet-scoped family yields its ``(exchange, symbol)``."""
        assert parse_wallet_scoped_topic(topic) == expected

    @pytest.mark.parametrize(
        "topic",
        [
            "signals.paper.BTC-USD.live",
            "signals.paper.BTC-USD.my_strategy",
            "signals.paper.MNQU6-CME.ema_cross",
            "market.kraken.BTC-USD.ticks",
            "system.heartbeats.strategy.my_strategy",
            "backtest.00000000-0000-7000-8000-000000000001.run-7.progress",
            "admin.user_deactivated",
            "admin.scope_revoked",
            "signals.",
            "signals.kraken",
            "orders.unknownkind.kraken.BTC-USD.submit",
            "orders",
            "signals.kraken.BTC-USD.historical",
        ],
    )
    def test_non_wallet_scoped_topics_return_none(self, topic: str) -> None:
        """Negative: every non-wallet-scoped topic passes through as ``None``.

        Includes the ``signals.paper.BTC-USD.live`` edge case that hits
        the paper-exchange guard AFTER the ``live`` suffix check.
        """
        assert parse_wallet_scoped_topic(topic) is None
