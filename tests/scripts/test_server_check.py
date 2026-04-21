"""Tests for server health check utility."""

import urllib.error
from http.client import HTTPResponse
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from scripts.server_check import DEFAULT_DELAY
from scripts.server_check import DEFAULT_MAX_RETRIES
from scripts.server_check import DEFAULT_URL
from scripts.server_check import check_health
from scripts.server_check import main


class TestCheckHealth:
    """Test suite for CheckHealth functionality."""

    def test_success_on_first_attempt(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Verify health check succeeds on first attempt.

        Given: Server responds with HTTP 200 status,
        When: check_health is called with max_retries=3,
        Then: Returns True and prints success message with attempt 1.
        """
        mock_response = MagicMock(spec=HTTPResponse)
        mock_response.status = 200
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch("urllib.request.urlopen", return_value=mock_response):
            result = check_health(max_retries=3, delay=0)

        assert result is True
        captured = capsys.readouterr()
        assert "Health check passed (attempt 1)" in captured.out

    def test_success_on_second_attempt(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Verify health check succeeds after initial failure.

        Given: First request fails with URLError then second succeeds with HTTP 200,
        When: check_health is called with max_retries=3,
        Then: Returns True and prints retry attempt 1/3 followed by success on attempt 2.
        """
        mock_response = MagicMock(spec=HTTPResponse)
        mock_response.status = 200
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)

        call_count = 0

        def side_effect(*args: object, **kwargs: object) -> HTTPResponse:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise urllib.error.URLError("Connection refused")
            return mock_response

        with patch("urllib.request.urlopen", side_effect=side_effect):
            result = check_health(max_retries=3, delay=0)

        assert result is True
        captured = capsys.readouterr()
        assert "Attempt 1/3" in captured.out
        assert "Health check passed (attempt 2)" in captured.out

    def test_failure_after_all_retries(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Verify health check fails after exhausting all retry attempts.

        Given: Server always responds with URLError (connection refused),
        When: check_health is called with max_retries=2,
        Then: Returns False and prints all attempt messages plus final failure message.
        """
        with patch(
            "urllib.request.urlopen", side_effect=urllib.error.URLError("Connection refused")
        ):
            result = check_health(max_retries=2, delay=0)

        assert result is False
        captured = capsys.readouterr()
        assert "Attempt 1/2" in captured.out
        assert "Attempt 2/2" in captured.out
        assert "Health check failed after all retries" in captured.out

    def test_handles_connection_reset_error(self) -> None:
        """Verify health check gracefully handles ConnectionResetError.

        Given: Server raises ConnectionResetError on connection attempt,
        When: check_health is called with max_retries=1,
        Then: Returns False without raising an exception.
        """
        with patch("urllib.request.urlopen", side_effect=ConnectionResetError()):
            result = check_health(max_retries=1, delay=0)

        assert result is False

    def test_handles_timeout_error(self) -> None:
        """Verify health check gracefully handles TimeoutError.

        Given: Server raises TimeoutError on connection attempt,
        When: check_health is called with max_retries=1,
        Then: Returns False without raising an exception.
        """
        with patch("urllib.request.urlopen", side_effect=TimeoutError()):
            result = check_health(max_retries=1, delay=0)

        assert result is False

    def test_uses_default_url(self) -> None:
        """Verify check_health uses DEFAULT_URL when no URL is specified.

        Given: No custom URL is provided,
        When: check_health is called without url parameter,
        Then: urlopen is called with DEFAULT_URL and timeout=5.
        """
        with patch("urllib.request.urlopen", side_effect=urllib.error.URLError("test")) as mock:
            check_health(max_retries=1, delay=0)

        mock.assert_called_with(DEFAULT_URL, timeout=5)

    def test_uses_custom_url(self) -> None:
        """Verify check_health uses custom URL when provided.

        Given: Custom URL 'http://example.com/health' is specified,
        When: check_health is called with url parameter,
        Then: urlopen is called with the custom URL instead of default.
        """
        custom_url = "http://example.com/health"

        with patch("urllib.request.urlopen", side_effect=urllib.error.URLError("test")) as mock:
            check_health(url=custom_url, max_retries=1, delay=0)

        mock.assert_called_with(custom_url, timeout=5)

    def test_uses_custom_timeout(self) -> None:
        """Verify check_health uses custom timeout when provided.

        Given: Custom timeout of 10 seconds is specified,
        When: check_health is called with timeout=10,
        Then: urlopen is called with timeout=10 instead of default 5.
        """
        with patch("urllib.request.urlopen", side_effect=urllib.error.URLError("test")) as mock:
            check_health(max_retries=1, delay=0, timeout=10)

        mock.assert_called_with(DEFAULT_URL, timeout=10)

    def test_non_200_status_continues_retry(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Test that non-200 status does not return True and continues retrying."""
        mock_response = MagicMock(spec=HTTPResponse)
        mock_response.status = 503
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch("urllib.request.urlopen", return_value=mock_response):
            result = check_health(max_retries=2, delay=0)

        assert result is False
        captured = capsys.readouterr()
        assert "Health check failed after all retries" in captured.out


class TestMain:
    """Test suite for Main functionality."""

    def test_returns_zero_on_success(self) -> None:
        """Verify main returns exit code 0 on successful health check.

        Given: check_health function returns True,
        When: main() is called,
        Then: Returns 0 indicating success.
        """
        with patch("scripts.server_check.check_health", return_value=True):
            result = main()

        assert result == 0

    def test_returns_one_on_failure(self) -> None:
        """Verify main returns exit code 1 on failed health check.

        Given: check_health function returns False,
        When: main() is called,
        Then: Returns 1 indicating failure.
        """
        with patch("scripts.server_check.check_health", return_value=False):
            result = main()

        assert result == 1


class TestConstants:
    """Test suite for Constants functionality."""

    def test_default_url(self) -> None:
        """Verify DEFAULT_URL constant has correct value.

        Given: DEFAULT_URL constant is imported from server_check module,
        When: Value is checked,
        Then: Equals 'http://localhost:8000/api/health'.
        """
        assert DEFAULT_URL == "http://localhost:8000/api/health"

    def test_default_max_retries(self) -> None:
        """Verify DEFAULT_MAX_RETRIES constant has correct value."""
        assert DEFAULT_MAX_RETRIES == 45

    def test_default_delay(self) -> None:
        """Verify DEFAULT_DELAY constant has correct value.

        Given: DEFAULT_DELAY constant is imported from server_check module,
        When: Value is checked,
        Then: Equals 2 seconds.
        """
        assert DEFAULT_DELAY == 2
