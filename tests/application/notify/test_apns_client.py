"""Tests for ``snapper.application.notify.apns_client``.

Exercises the ``ApnsClientPool`` routing layer + the
``_map_result`` / ``_status_code_from`` / ``_coarse_status``
helpers that translate ``aioapns`` responses into the outbox-
friendly ``ApnsSendResult`` shape. The aioapns client itself is
always mocked — the tests assert our routing semantics, not
Apple's HTTP/2 stack.
"""

import errno
import socket
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import httpx
import pytest
from aioapns.common import NotificationResult
from aioapns.exceptions import ConnectionClosed as ApnsConnectionClosed
from aioapns.exceptions import ConnectionError as ApnsConnectionError
from aioapns.exceptions import MaxAttemptsExceeded as ApnsMaxAttemptsExceeded

from snapper.application.notify.apns_client import ApnsClientPool
from snapper.application.notify.apns_client import _coarse_status
from snapper.application.notify.apns_client import _map_result
from snapper.application.notify.apns_client import _status_code_from
from snapper.application.notify.apns_client import build_apns_client_pool
from snapper.application.notify.apns_client import is_network_delivery_error
from snapper.application.notify.apns_config import ApnsConfig


def _config(environment: str = "sandbox_and_production") -> ApnsConfig:
    """Return an ``ApnsConfig`` with minimum valid fields for builder tests."""
    return ApnsConfig(
        team_id="TEAMID",
        key_id="KEYID",
        bundle_id="ie.klatt.snapper",
        topic="ie.klatt.snapper",
        environment=environment,
        private_key_pem="-----BEGIN PRIVATE KEY-----\nfake\n-----END PRIVATE KEY-----\n",
    )


class TestStatusCodeMapping:
    """``_status_code_from`` converts aioapns' string ``status`` to int."""

    def test_numeric_string_parses(self) -> None:
        """A well-formed numeric string is parsed as int."""
        assert _status_code_from("200") == 200
        assert _status_code_from("410") == 410
        assert _status_code_from("503") == 503

    def test_empty_or_malformed_collapses_to_zero(self) -> None:
        """Empty or non-numeric strings collapse to 0 (handled as ``other``)."""
        assert _status_code_from("") == 0
        assert _status_code_from("not-a-number") == 0


class TestCoarseStatus:
    """``_coarse_status`` assigns one of 7 outbox-friendly status strings."""

    def test_success(self) -> None:
        """200 maps to ``success``."""
        assert _coarse_status(200, "") == "success"

    def test_unregistered(self) -> None:
        """410 maps to ``unregistered`` (triggers device de-activation)."""
        assert _coarse_status(410, "Unregistered") == "unregistered"
        assert _coarse_status(410, "BadDeviceToken") == "unregistered"

    def test_400_bad_device_token_maps_to_unregistered(self) -> None:
        """400 BadDeviceToken maps to ``unregistered``."""
        assert _coarse_status(400, "BadDeviceToken") == "unregistered"

    def test_400_device_token_not_for_topic_maps_to_config_error(self) -> None:
        """400 DeviceTokenNotForTopic maps to non-deactivating ``config_error``."""
        assert _coarse_status(400, "DeviceTokenNotForTopic") == "config_error"

    def test_throttled(self) -> None:
        """429 maps to ``throttled``."""
        assert _coarse_status(429, "TooManyRequests") == "throttled"

    def test_server_error_5xx(self) -> None:
        """Any 5xx response maps to ``server_error`` for retry."""
        assert _coarse_status(500, "") == "server_error"
        assert _coarse_status(503, "") == "server_error"

    @pytest.mark.parametrize(
        "description",
        [
            "BadRequest",
            "BadTopic",
            "MissingDeviceToken",
            "",
            "<html>not-json</html>",
        ],
    )
    def test_400_other_reasons_map_to_other(self, description: str) -> None:
        """Non-unregistered 400 reasons (including empty/unparseable) stay ``other``."""
        assert _coarse_status(400, description) == "other"

    def test_zero_status_with_description_maps_to_other(self) -> None:
        """0 status with a non-empty description still maps to ``other``."""
        assert _coarse_status(0, "TransportFailure") == "other"

    def test_unknown_without_description(self) -> None:
        """0 status + empty description maps to ``unknown``."""
        assert _coarse_status(0, "") == "unknown"


class TestMapResult:
    """``_map_result`` assembles ``ApnsSendResult`` from ``NotificationResult``."""

    def test_success_result_populates_apns_id(self) -> None:
        """Success echoes notification_id as apns_id with empty description."""
        raw = NotificationResult("apns-id-42", "200", "", None)

        mapped = _map_result(raw)

        assert mapped.status_code == 200
        assert mapped.status == "success"
        assert mapped.apns_id == "apns-id-42"
        assert mapped.description == ""

    def test_410_result_populates_description(self) -> None:
        """410 response carries the APNs error string in description."""
        raw = NotificationResult("", "410", "Unregistered", None)

        mapped = _map_result(raw)

        assert mapped.status_code == 410
        assert mapped.status == "unregistered"
        assert mapped.description == "Unregistered"

    def test_400_bad_device_token_result_maps_to_unregistered(self) -> None:
        """400 BadDeviceToken maps to ``unregistered`` while keeping status_code 400."""
        raw = NotificationResult("", "400", "BadDeviceToken", None)

        mapped = _map_result(raw)

        assert mapped.status_code == 400
        assert mapped.status == "unregistered"
        assert mapped.description == "BadDeviceToken"

    def test_400_device_token_not_for_topic_maps_to_config_error(self) -> None:
        """400 DeviceTokenNotForTopic maps to ``config_error`` while keeping status_code 400."""
        raw = NotificationResult("", "400", "DeviceTokenNotForTopic", None)

        mapped = _map_result(raw)

        assert mapped.status_code == 400
        assert mapped.status == "config_error"
        assert mapped.description == "DeviceTokenNotForTopic"

    @pytest.mark.parametrize(
        ("raw_description", "expected_description"),
        [
            ("BadTopic", "BadTopic"),
            ("MissingDeviceToken", "MissingDeviceToken"),
            ("", ""),
            (None, ""),
            ("<html>not-json</html>", "<html>not-json</html>"),
        ],
    )
    def test_400_non_unregistered_reasons_map_to_other(
        self, raw_description: str | None, expected_description: str
    ) -> None:
        """400 BadTopic, MissingDeviceToken, empty reason, and unparseable body stay ``other``."""
        raw = NotificationResult("", "400", raw_description, None)

        mapped = _map_result(raw)

        assert mapped.status_code == 400
        assert mapped.status == "other"
        assert mapped.description == expected_description

    def test_missing_notification_id_becomes_empty_string(self) -> None:
        """None ``notification_id`` is normalised to empty string."""
        raw = NotificationResult(None, "500", "", None)

        mapped = _map_result(raw)

        assert mapped.apns_id == ""


class TestApnsClientPoolRouting:
    """``ApnsClientPool.send`` routes to the matching env-scoped client."""

    @pytest.mark.asyncio
    async def test_sandbox_device_routes_to_sandbox_client(self) -> None:
        """``env='sandbox'`` dispatches to the sandbox client only."""
        sandbox = MagicMock()
        sandbox.send_notification = AsyncMock(
            return_value=NotificationResult("apns-1", "200", "", None)
        )
        production = MagicMock()
        production.send_notification = AsyncMock()
        pool = ApnsClientPool(sandbox_client=sandbox, production_client=production)

        result = await pool.send(
            env="sandbox",
            device_token="a" * 64,
            payload={"aps": {"alert": {"title": "t", "body": "b"}}},
            apns_topic="ie.klatt.snapper",
        )

        assert result.status == "success"
        sandbox.send_notification.assert_awaited_once()
        production.send_notification.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_prod_device_routes_to_production_client(self) -> None:
        """``env='prod'`` dispatches to the production client only."""
        sandbox = MagicMock()
        sandbox.send_notification = AsyncMock()
        production = MagicMock()
        production.send_notification = AsyncMock(
            return_value=NotificationResult("apns-9", "200", "", None)
        )
        pool = ApnsClientPool(sandbox_client=sandbox, production_client=production)

        await pool.send(
            env="prod",
            device_token="a" * 64,
            payload={"aps": {"alert": {"title": "t", "body": "b"}}},
            apns_topic="ie.klatt.snapper",
        )

        production.send_notification.assert_awaited_once()
        sandbox.send_notification.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unknown_env_raises(self) -> None:
        """Any non-``sandbox``/``prod`` value is a programmer error."""
        pool = ApnsClientPool(sandbox_client=MagicMock(), production_client=None)

        with pytest.raises(ValueError, match="Unknown APNs env"):
            await pool.send(
                env="staging",
                device_token="a" * 64,
                payload={},
                apns_topic="t",
            )

    @pytest.mark.asyncio
    async def test_send_to_missing_env_raises(self) -> None:
        """Device registered for prod but pool has no prod client -> loud failure."""
        pool = ApnsClientPool(sandbox_client=MagicMock(), production_client=None)

        with pytest.raises(ValueError, match="no production client"):
            await pool.send(
                env="prod",
                device_token="a" * 64,
                payload={},
                apns_topic="t",
            )

    @pytest.mark.asyncio
    async def test_send_sandbox_when_pool_has_no_sandbox_raises(self) -> None:
        """Sandbox device on a production-only pool -> loud failure.

        The mirror of ``test_send_to_missing_env_raises`` — guards the
        other ``_select_client`` branch so both "wrong env" paths have
        test coverage.
        """
        pool = ApnsClientPool(sandbox_client=None, production_client=MagicMock())

        with pytest.raises(ValueError, match="no sandbox client"):
            await pool.send(
                env="sandbox",
                device_token="a" * 64,
                payload={},
                apns_topic="t",
            )

    def test_both_clients_none_rejected_at_construction(self) -> None:
        """Construction guards against the degenerate both-None case."""
        with pytest.raises(ValueError, match="at least one"):
            ApnsClientPool(sandbox_client=None, production_client=None)


class TestBuildApnsClientPool:
    """``build_apns_client_pool`` spins up the correct env-scoped clients."""

    def test_sandbox_only_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """``environment='sandbox'`` builds only the sandbox client."""
        fake_apns = MagicMock()
        monkeypatch.setattr("snapper.application.notify.apns_client._AioApns", fake_apns)

        pool = build_apns_client_pool(_config(environment="sandbox"))

        assert pool._sandbox is not None
        assert pool._production is None
        fake_apns.assert_called_once()
        _, kwargs = fake_apns.call_args
        assert kwargs["use_sandbox"] is True

    def test_production_only_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """``environment='production'`` builds only the production client."""
        fake_apns = MagicMock()
        monkeypatch.setattr("snapper.application.notify.apns_client._AioApns", fake_apns)

        pool = build_apns_client_pool(_config(environment="production"))

        assert pool._sandbox is None
        assert pool._production is not None
        _, kwargs = fake_apns.call_args
        assert kwargs["use_sandbox"] is False

    def test_both_environment_builds_two_clients(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """``sandbox_and_production`` builds both, sharing the PEM key."""
        fake_apns = MagicMock()
        monkeypatch.setattr("snapper.application.notify.apns_client._AioApns", fake_apns)

        pool = build_apns_client_pool(_config(environment="sandbox_and_production"))

        assert pool._sandbox is not None
        assert pool._production is not None
        assert fake_apns.call_count == 2
        sandbox_kwargs = fake_apns.call_args_list[0].kwargs
        prod_kwargs = fake_apns.call_args_list[1].kwargs
        assert sandbox_kwargs["use_sandbox"] is True
        assert prod_kwargs["use_sandbox"] is False
        assert sandbox_kwargs["key"] == prod_kwargs["key"]


class TestNetworkDeliveryErrorClassification:
    """``is_network_delivery_error`` separates network outages from other errors."""

    @pytest.mark.parametrize(
        "exc",
        [
            ApnsMaxAttemptsExceeded(),
            ApnsConnectionError(),
            ApnsConnectionClosed("eof"),
            TimeoutError("timed out"),
            socket.gaierror(-2, "Name or service not known"),
            ConnectionRefusedError("refused"),
            ConnectionResetError("reset"),
            OSError(errno.ENETUNREACH, "network unreachable"),
            OSError(errno.EHOSTUNREACH, "no route to host"),
            OSError(errno.ENETDOWN, "network down"),
            OSError(errno.ETIMEDOUT, "connection timed out"),
            OSError(errno.ECONNREFUSED, "connection refused"),
            OSError(errno.ECONNRESET, "connection reset"),
            httpx.ConnectError("connect failed"),
            httpx.ProxyError("proxy down"),
            httpx.ReadTimeout("read timed out"),
        ],
    )
    def test_network_exceptions_classified_as_network(self, exc: BaseException) -> None:
        """Transient network/transport exception types return True.

        Given: An exception instance representing DNS, connect, proxy, timeout,
            network-errno ``OSError``, or aioapns ``MaxAttemptsExceeded``.
        When: ``is_network_delivery_error`` inspects the exception.
        Then: It returns ``True`` so the sidecar retries until TTL expiry.
        """
        assert is_network_delivery_error(exc) is True

    @pytest.mark.parametrize(
        "exc",
        [
            RuntimeError("unexpected"),
            ValueError("Unknown APNs env"),
            FileNotFoundError(errno.ENOENT, "No such file or directory"),
            PermissionError(errno.EACCES, "Permission denied"),
            OSError(errno.EINVAL, "Invalid argument"),
            OSError("bare error without errno"),
        ],
    )
    def test_non_network_exceptions_classified_false(self, exc: BaseException) -> None:
        """Non-network and filesystem/config ``OSError`` exceptions return False.

        Given: A non-network exception, a ``FileNotFoundError``/``PermissionError``
            (such as a missing/unreadable APNs key), or an ``OSError`` without
            a network errno.
        When: ``is_network_delivery_error`` inspects the exception.
        Then: It returns ``False`` so the 3-attempt path applies.
        """
        assert is_network_delivery_error(exc) is False

    def test_wrapped_cause_and_context_follow_chain(self) -> None:
        """Wrapped network causes and contexts are recognized across the chain.

        Given: Non-network wrapper exceptions linked via ``__cause__`` and
            ``__context__`` to a network error, plus a non-network cycle.
        When: ``is_network_delivery_error`` walks the exception chain.
        Then: Wrapped network chains return ``True`` and cyclic non-network
            chains terminate safely with ``False``.
        """
        caused = RuntimeError("wrapper")
        caused.__cause__ = ApnsMaxAttemptsExceeded()
        assert is_network_delivery_error(caused) is True

        contextual = RuntimeError("secondary")
        contextual.__context__ = socket.gaierror(-3, "temporary failure")
        assert is_network_delivery_error(contextual) is True

        cycle_a = RuntimeError("a")
        cycle_b = RuntimeError("b")
        cycle_a.__cause__ = cycle_b
        cycle_b.__cause__ = cycle_a
        assert is_network_delivery_error(cycle_a) is False
