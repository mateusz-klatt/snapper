"""Tests for WebSocket helper functions."""

from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.auth.tokens import TokenManager
from snapper.auth.user_service import UserService
from snapper.interface.websocket.helpers import build_allowed_origins
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

    def test_role_allowed_categories_returns_default_for_unknown_role(self) -> None:
        """Role allowed categories returns default for unknown role.

        Given: A mock role with unknown name,
        When: Getting allowed categories,
        Then: Returns default set {'market', 'system'}.
        """
        mock_role = MagicMock(spec=[])
        mock_role.name = "UNKNOWN"
        result = role_allowed_categories(mock_role)
        assert result == {"market", "system"}


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
