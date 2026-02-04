"""Tests for rate limiting configuration and middleware integration."""

from types import SimpleNamespace
from unittest.mock import MagicMock

from slowapi import Limiter

from snapper.server.app import handle_rate_limit_exceeded
from snapper.server.rate_limiting import LOGIN_RATE_LIMIT
from snapper.server.rate_limiting import PASSWORD_CHANGE_RATE_LIMIT
from snapper.server.rate_limiting import PASSWORD_RESET_RATE_LIMIT
from snapper.server.rate_limiting import limiter


class TestRateLimitingConfig:
    """Tests for rate limiting module configuration."""

    def test_limiter_is_slowapi_instance(self) -> None:
        """Verify limiter is a Limiter instance.

        Given: The rate limiting module,
        When: Accessing the limiter,
        Then: It is a slowapi Limiter instance.
        """
        assert isinstance(limiter, Limiter)

    def test_login_rate_limit_format(self) -> None:
        """Verify login rate limit has correct format.

        Given: The LOGIN_RATE_LIMIT constant,
        When: Checking its value,
        Then: It follows the slowapi rate format.
        """
        assert LOGIN_RATE_LIMIT == "5/15minutes"

    def test_password_change_rate_limit_format(self) -> None:
        """Verify password change rate limit has correct format.

        Given: The PASSWORD_CHANGE_RATE_LIMIT constant,
        When: Checking its value,
        Then: It follows the slowapi rate format.
        """
        assert PASSWORD_CHANGE_RATE_LIMIT == "5/hour"

    def test_password_reset_rate_limit_format(self) -> None:
        """Verify password reset rate limit has correct format.

        Given: The PASSWORD_RESET_RATE_LIMIT constant,
        When: Checking its value,
        Then: It follows the slowapi rate format.
        """
        assert PASSWORD_RESET_RATE_LIMIT == "10/hour"


class TestHandleRateLimitExceeded:
    """Tests for the rate limit exceeded handler."""

    def test_returns_429_with_detail(self) -> None:
        """Handler returns 429 response with exception detail.

        Given: A request and rate limit exception with detail attribute,
        When: handle_rate_limit_exceeded is called,
        Then: Response has 429 status and includes the detail message.
        """
        mock_request = MagicMock()
        mock_request.state = SimpleNamespace()
        exc = MagicMock()
        exc.detail = "Rate Limit Exceeded: 5 per 15 minutes"
        response = handle_rate_limit_exceeded(mock_request, exc)
        assert response.status_code == 429
        assert b"Rate limit exceeded: Rate Limit Exceeded: 5 per 15 minutes" in response.body

    def test_includes_retry_after_header(self) -> None:
        """Handler includes Retry-After header when view_rate_limit is set.

        Given: A request with view_rate_limit state,
        When: handle_rate_limit_exceeded is called,
        Then: Response includes Retry-After header.
        """
        mock_request = MagicMock()
        mock_request.state = SimpleNamespace(view_rate_limit="900")
        exc = MagicMock()
        exc.detail = "Too many requests"
        response = handle_rate_limit_exceeded(mock_request, exc)
        assert response.status_code == 429
        assert response.headers["Retry-After"] == "900"

    def test_no_retry_after_when_not_set(self) -> None:
        """Handler omits Retry-After header when view_rate_limit is absent.

        Given: A request without view_rate_limit state,
        When: handle_rate_limit_exceeded is called,
        Then: Response does not include Retry-After header.
        """
        mock_request = MagicMock()
        mock_request.state = SimpleNamespace()
        exc = Exception("generic error")
        response = handle_rate_limit_exceeded(mock_request, exc)
        assert response.status_code == 429
        assert "Retry-After" not in response.headers
        assert b"Rate limit exceeded: generic error" in response.body
