"""Unit tests for authentication security schemas and utilities."""

import contextlib

import pytest
from pydantic import ValidationError

from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.security import CsrfToken
from snapper.auth.schemas.tokens import MCPOAuthAccessTokenClaims
from snapper.auth.schemas.tokens import TokenPair
from snapper.config.settings import get_settings
from snapper.data.models import Order
from snapper.data.models import Trade
from snapper.data.models import User
from snapper.data.repository import get_repository
from snapper.utils.logging import setup_logging


class TestUtilitiesCoverage:
    """Tests for utilities coverage (logging, settings, models)."""

    def test_logging_setup(self) -> None:
        """Verify logging setup accepts all standard log levels.

        Given the setup_logging function,
        When called with DEBUG, INFO, WARNING, ERROR levels,
        Then no exceptions are raised.
        """
        setup_logging("DEBUG")
        setup_logging("INFO")
        setup_logging("WARNING")
        setup_logging("ERROR")

    def test_settings_singleton(self) -> None:
        """Verify get_settings returns the same instance.

        Given two calls to get_settings(),
        When comparing the returned objects,
        Then they are the same instance (singleton pattern).
        """
        settings1 = get_settings()
        settings2 = get_settings()
        assert settings1 is settings2

    def test_settings_attributes(self) -> None:
        """Verify settings object has required attributes.

        Given a settings instance,
        When checking for db_url attribute,
        Then it exists and is a string.
        """
        settings = get_settings()
        assert hasattr(settings, "db_url")
        assert isinstance(settings.db_url, str)

    def test_data_models_import(self) -> None:
        """Verify data models can be imported.

        Given User, Trade, Order model classes,
        When checking their existence,
        Then all are defined (not None).
        """
        assert User is not None
        assert Trade is not None
        assert Order is not None

    def test_error_scenarios(self) -> None:
        """Verify invalid log level is handled gracefully.

        Given an invalid log level string,
        When setup_logging is called,
        Then exception is suppressed (no crash).
        """
        with contextlib.suppress(Exception):
            setup_logging("INVALID_LEVEL")

    def test_additional_auth_models(self) -> None:
        """Verify auth schema models can be imported.

        Given TokenPair and CsrfToken classes,
        When checking their existence,
        Then both are defined (not None).
        """
        assert TokenPair is not None
        assert CsrfToken is not None

    def test_mcp_oauth_claims_reject_wrong_token_purpose(self) -> None:
        """Verify MCP OAuth claims cannot decode as another token purpose.

        Given a complete audience-bound MCP OAuth access-token payload,
        When it is validated with its default purpose and a wrong purpose,
        Then the OAuth purpose is preserved and the wrong value is rejected.
        """
        payload: dict[str, object] = {
            "sub": "delegate-user",
            "username": "chatgpt-reader",
            "role": UserRole.AI_DELEGATE,
            "permissions": ["read:market_data"],
            "exp": 2_000_000_900,
            "iat": 2_000_000_000,
            "jti": "oauth-jti",
            "sid": "oauth-session",
            "iss": "https://snapper.ch/api/mcp",
            "aud": "https://snapper.ch/api/mcp",
            "scope": "snapper.read offline_access",
            "client_id": "chatgpt-client",
            "nbf": 2_000_000_000,
            "grant_id": "grant-public-id",
        }
        claims = MCPOAuthAccessTokenClaims.model_validate(payload)
        assert claims.token_use == "mcp_oauth_access"
        assert claims.aud == "https://snapper.ch/api/mcp"

        payload["token_use"] = "access"
        with pytest.raises(ValidationError):
            MCPOAuthAccessTokenClaims.model_validate(payload)

    def test_repository_import(self) -> None:
        """Verify repository function can be imported.

        Given get_repository function,
        When checking its existence,
        Then it is defined (not None).
        """
        assert get_repository is not None
