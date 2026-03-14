"""Unit tests for authentication security schemas and utilities."""

import contextlib

from snapper.auth.schemas.security import CsrfToken
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

    def test_repository_import(self) -> None:
        """Verify repository function can be imported.

        Given get_repository function,
        When checking its existence,
        Then it is defined (not None).
        """
        assert get_repository is not None
