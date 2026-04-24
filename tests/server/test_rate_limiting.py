"""Tests for rate limiting configuration and middleware integration."""

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException
from fastapi import Request
from limits import parse
from slowapi import Limiter
from starlette.exceptions import HTTPException as StarletteHTTPException

from snapper.server.app import handle_rate_limit_exceeded
from snapper.server.rate_limiting import ACCOUNT_CHANGE_RATE_LIMIT
from snapper.server.rate_limiting import ACCOUNT_RESET_RATE_LIMIT
from snapper.server.rate_limiting import LOGIN_RATE_LIMIT
from snapper.server.rate_limiting import clear_failed_login_attempts
from snapper.server.rate_limiting import enforce_failed_login_rate_limit
from snapper.server.rate_limiting import limiter
from snapper.server.rate_limiting import register_failed_login_attempt


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

        Given: The ACCOUNT_CHANGE_RATE_LIMIT constant,
        When: Checking its value,
        Then: It follows the slowapi rate format.
        """
        assert ACCOUNT_CHANGE_RATE_LIMIT == "5/hour"

    def test_password_reset_rate_limit_format(self) -> None:
        """Verify password reset rate limit has correct format.

        Given: The ACCOUNT_RESET_RATE_LIMIT constant,
        When: Checking its value,
        Then: It follows the slowapi rate format.
        """
        assert ACCOUNT_RESET_RATE_LIMIT == "10/hour"


class TestHandleRateLimitExceeded:
    """Tests for the rate limit exceeded handler."""

    def test_returns_429_with_detail(self) -> None:
        """Handler returns 429 response with exception detail.

        Given: A request and a RateLimitExceeded exception,
        When: handle_rate_limit_exceeded is called,
        Then: Response has 429 status and includes the detail message.
        """
        mock_request = MagicMock()
        mock_request.state = SimpleNamespace()
        exc = HTTPException(status_code=429, detail="Rate Limit Exceeded: 5 per 15 minutes")
        response = handle_rate_limit_exceeded(mock_request, exc)
        assert response.status_code == 429
        assert b"Rate limit exceeded: Rate Limit Exceeded: 5 per 15 minutes" in response.body

    def test_includes_retry_after_header(self) -> None:
        """Handler includes Retry-After header when view_rate_limit is set.

        Given: A request with view_rate_limit state and a RateLimitExceeded exception,
        When: handle_rate_limit_exceeded is called,
        Then: Response includes Retry-After header.
        """
        mock_request = MagicMock()
        mock_request.state = SimpleNamespace(view_rate_limit="900")
        exc = HTTPException(status_code=429, detail="Too many requests")
        response = handle_rate_limit_exceeded(mock_request, exc)
        assert response.status_code == 429
        assert response.headers["Retry-After"] == "900"

    def test_returns_429_with_starlette_http_exception(self) -> None:
        """Handler extracts detail from Starlette HTTPException subclasses.

        Given: A Starlette HTTPException (same base as RateLimitExceeded),
        When: handle_rate_limit_exceeded is called,
        Then: Response uses .detail attribute, not str(exc).
        """
        mock_request = MagicMock()
        mock_request.state = SimpleNamespace()
        exc = StarletteHTTPException(status_code=429, detail="5 per 15 minutes")
        response = handle_rate_limit_exceeded(mock_request, exc)
        assert response.status_code == 429
        assert b"Rate limit exceeded: 5 per 15 minutes" in response.body

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


class TestFailedLoginRateLimiting:
    """Tests for failed-only login rate limiting behavior."""

    def setup_method(self) -> None:
        """Reset limiter storage before each test."""
        limiter.enabled = True
        limiter.reset()

    def teardown_method(self) -> None:
        """Reset limiter storage after each test."""
        limiter.reset()

    def _build_request(self, host: str = "127.0.0.1") -> Request:
        """Build request with a specific client host.

        Args:
            host: Client IP address.

        Returns:
            Starlette request instance.
        """
        scope: dict[str, Any] = {
            "type": "http",
            "method": "POST",
            "path": "/api/auth/login",
            "headers": [],
            "client": (host, 12345),
        }
        return Request(scope)

    def test_blocks_after_configured_number_of_failed_attempts(self) -> None:
        """Failed attempts are blocked once configured threshold is reached.

        Given: Repeated failed login attempts for same username and IP,
        When: Checking limiter after exhausting quota,
        Then: HTTP 429 is raised with retry hint.
        """
        request = self._build_request()
        username = "admin"
        allowed_attempts = parse(LOGIN_RATE_LIMIT).amount
        for _ in range(allowed_attempts):
            enforce_failed_login_rate_limit(request, username)
            register_failed_login_attempt(request, username)
        with pytest.raises(HTTPException) as exc:
            enforce_failed_login_rate_limit(request, username)
        assert exc.value.status_code == 429
        assert exc.value.detail == "Too many failed login attempts"
        assert exc.value.headers is not None
        assert "Retry-After" in exc.value.headers

    def test_successful_login_reset_allows_new_failed_attempt_budget(self) -> None:
        """Successful login clears failed-attempt counter.

        Given: Failed attempts reaching the configured threshold,
        When: Counter is cleared after successful authentication,
        Then: Full failed-attempt budget is available again.
        """
        request = self._build_request()
        username = "admin"
        allowed_attempts = parse(LOGIN_RATE_LIMIT).amount
        for _ in range(allowed_attempts):
            enforce_failed_login_rate_limit(request, username)
            register_failed_login_attempt(request, username)
        with pytest.raises(HTTPException):
            enforce_failed_login_rate_limit(request, username)
        clear_failed_login_attempts(request, username)
        for _ in range(allowed_attempts):
            enforce_failed_login_rate_limit(request, username)
            register_failed_login_attempt(request, username)
        with pytest.raises(HTTPException):
            enforce_failed_login_rate_limit(request, username)

    def test_counter_is_scoped_per_username_on_same_ip(self) -> None:
        """Failed-attempt quota is isolated per username on same source IP.

        Given: One username already blocked for a shared IP,
        When: Another username logs in from same IP,
        Then: Second username retains its own failed-attempt budget.
        """
        request = self._build_request()
        blocked_username = "admin"
        second_username = "operator"
        allowed_attempts = parse(LOGIN_RATE_LIMIT).amount
        for _ in range(allowed_attempts):
            enforce_failed_login_rate_limit(request, blocked_username)
            register_failed_login_attempt(request, blocked_username)
        with pytest.raises(HTTPException):
            enforce_failed_login_rate_limit(request, blocked_username)
        enforce_failed_login_rate_limit(request, second_username)
