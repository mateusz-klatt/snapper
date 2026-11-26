"""Tests for ``snapper.application.notify.apns_client``.

Exercises the ``ApnsClientPool`` routing layer + the
``_map_result`` / ``_status_code_from`` / ``_coarse_status``
helpers that translate ``aioapns`` responses into the outbox-
friendly ``ApnsSendResult`` shape. The aioapns client itself is
always mocked — the tests assert our routing semantics, not
Apple's HTTP/2 stack.
"""

import asyncio
import errno
import inspect
import socket
import ssl
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import httpx
import pytest
from aioapns import APNs as _AioApns
from aioapns.common import NotificationRequest
from aioapns.common import NotificationResult
from aioapns.common import PushType
from aioapns.connection import APNsDevelopmentClientProtocol
from aioapns.connection import APNsKeyConnectionPool
from aioapns.connection import APNsProductionClientProtocol
from aioapns.exceptions import ConnectionClosed as ApnsConnectionClosed
from aioapns.exceptions import ConnectionError as ApnsConnectionError
from aioapns.exceptions import MaxAttemptsExceeded as ApnsMaxAttemptsExceeded
from h2.config import H2Configuration
from h2.connection import H2Connection
from h2.events import RequestReceived
from h2.settings import SettingCodes

from snapper.application.notify.apns_client import ApnsClientPool
from snapper.application.notify.apns_client import _coarse_status
from snapper.application.notify.apns_client import _HardenedKeyConnectionPool
from snapper.application.notify.apns_client import _map_result
from snapper.application.notify.apns_client import _status_code_from
from snapper.application.notify.apns_client import _TimeoutSafeDevelopmentClientProtocol
from snapper.application.notify.apns_client import _TimeoutSafeProductionClientProtocol
from snapper.application.notify.apns_client import _TimeoutSafeProtocolMixin
from snapper.application.notify.apns_client import build_apns_client_pool
from snapper.application.notify.apns_client import is_network_delivery_error
from snapper.application.notify.apns_config import ApnsConfig
from snapper.core.json_types import JsonObject


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

    def test_ssl_errors_and_causes_classified_non_network(self) -> None:
        """TLS/certificate errors and wrapped non-network causes return False.

        Given: Direct ``ssl.SSLError`` / ``ssl.SSLCertVerificationError``,
            ``MaxAttemptsExceeded`` or ``ConnectionError`` wrapping SSL or
            ``ValueError`` causes, and ``httpx.ConnectError`` holding an SSL
            verification error in nested exception arguments.
        When: ``is_network_delivery_error`` evaluates each exception chain.
        Then: All return ``False`` so permanent TLS/config errors stop at 3 attempts,
            while wrapped network causes still return ``True``.
        """
        cert_err = ssl.SSLCertVerificationError(1, "certificate verify failed")
        assert is_network_delivery_error(cert_err) is False
        assert is_network_delivery_error(ssl.SSLError(errno.ECONNRESET, "ssl failure")) is False

        wrapped_max = ApnsMaxAttemptsExceeded()
        wrapped_max.__cause__ = cert_err
        assert is_network_delivery_error(wrapped_max) is False

        wrapped_conn = ApnsConnectionError()
        wrapped_conn.__context__ = cert_err
        assert is_network_delivery_error(wrapped_conn) is False

        wrapped_value = ApnsMaxAttemptsExceeded()
        wrapped_value.__cause__ = ValueError("bad payload")
        assert is_network_delivery_error(wrapped_value) is False

        wrapped_net = ApnsMaxAttemptsExceeded()
        inner_conn = ApnsConnectionError()
        inner_conn.__context__ = socket.gaierror(-2, "dns fail")
        wrapped_net.__cause__ = inner_conn
        assert is_network_delivery_error(wrapped_net) is True

        inner_http = Exception(cert_err)
        httpx_ssl = httpx.ConnectError("tls verify failed")
        httpx_ssl.__cause__ = inner_http
        assert is_network_delivery_error(httpx_ssl) is False

    def test_transient_ssl_drop_errors_classified_network_and_permanent_ssl_false(self) -> None:
        """SSL connection drops retry while certificate/config SSL faults stay non-network.

        Given: Transient SSL drop subclasses (``SSLEOFError``,
            ``SSLZeroReturnError``, ``SSLSyscallError``, ``SSLWantReadError``,
            ``SSLWantWriteError``) and permanent SSL errors
            (``SSLCertVerificationError``, bare config ``SSLError``), both
            directly and wrapped in ``httpx.ConnectError`` or
            ``MaxAttemptsExceeded``.
        When: ``is_network_delivery_error`` classifies each exception chain.
        Then: Transient drop subclasses return ``True`` and permanent TLS/cert
            faults return ``False``.
        """
        for transient in (
            ssl.SSLEOFError("EOF occurred in violation of protocol"),
            ssl.SSLZeroReturnError("TLS/SSL connection has been closed"),
            ssl.SSLSyscallError(errno.ECONNRESET, "syscall failure"),
            ssl.SSLWantReadError("want read"),
            ssl.SSLWantWriteError("want write"),
        ):
            assert is_network_delivery_error(transient) is True
            wrapped_http = httpx.ConnectError("tls eof")
            wrapped_http.__cause__ = transient
            assert is_network_delivery_error(wrapped_http) is True
            wrapped_apns = ApnsMaxAttemptsExceeded()
            wrapped_apns.__cause__ = transient
            assert is_network_delivery_error(wrapped_apns) is True

        for permanent in (
            ssl.SSLCertVerificationError(1, "certificate verify failed"),
            ssl.SSLError(1, "WRONG_VERSION_NUMBER"),
        ):
            assert is_network_delivery_error(permanent) is False
            wrapped_http = httpx.ConnectError("tls config failed")
            wrapped_http.__cause__ = permanent
            assert is_network_delivery_error(wrapped_http) is False
            wrapped_args_http = httpx.ConnectError("tls config args")
            wrapped_args_http.__cause__ = RuntimeError("tls config args", permanent)
            assert is_network_delivery_error(wrapped_args_http) is False
            wrapped_apns = ApnsMaxAttemptsExceeded()
            wrapped_apns.__cause__ = permanent
            assert is_network_delivery_error(wrapped_apns) is False

    def test_suppress_context_stops_chain_walk(self) -> None:
        """``raise ... from None`` stops context inspection at the outer exception.

        Given: A ``RuntimeError`` raised ``from None`` inside an ``except TimeoutError``
            block so ``__suppress_context__`` is ``True``.
        When: ``is_network_delivery_error`` walks the chain.
        Then: It respects ``__suppress_context__`` and returns ``False``.
        """
        try:
            try:
                raise TimeoutError("timed out")
            except TimeoutError:
                raise RuntimeError("suppressed wrapper") from None
        except RuntimeError as suppressed:
            assert suppressed.__context__ is not None
            assert suppressed.__suppress_context__ is True
            assert is_network_delivery_error(suppressed) is False


class _FakeH2Transport(asyncio.Transport):
    """Minimal in-memory asyncio transport recording outbound HTTP/2 frames."""

    def __init__(self) -> None:
        """Initialize an empty outbound frame buffer and open closing flag."""
        super().__init__()
        self.written = bytearray()
        self.closing = False

    def write(self, data: bytes | bytearray | memoryview) -> None:
        """Append outbound bytes written by ``H2Protocol.flush``."""
        self.written.extend(data)

    def is_closing(self) -> bool:
        """Return whether the transport has started closing."""
        return self.closing

    def close(self) -> None:
        """Mark the transport closing."""
        self.closing = True


def _connected_h2_pair(
    protocol_cls: type[_TimeoutSafeProtocolMixin],
    loop: asyncio.AbstractEventLoop,
    *,
    max_concurrent_streams: int = 1,
) -> tuple[_TimeoutSafeProtocolMixin, _FakeH2Transport, H2Connection]:
    """Build a connected real aioapns protocol paired with an H2 server state machine."""
    server = H2Connection(config=H2Configuration(client_side=False))
    server.initiate_connection()
    server.update_settings({SettingCodes.MAX_CONCURRENT_STREAMS: max_concurrent_streams})
    transport = _FakeH2Transport()
    protocol = protocol_cls("ie.klatt.snapper", loop=loop)
    protocol.connection_made(transport)
    server.receive_data(bytes(transport.written))
    transport.written.clear()
    protocol.data_received(server.data_to_send())
    server.receive_data(bytes(transport.written))
    transport.written.clear()
    return protocol, transport, server


def _request(notification_id: str) -> NotificationRequest:
    """Create a minimal ``NotificationRequest`` with a deterministic id."""
    return NotificationRequest(
        device_token="a" * 64,
        message={"aps": {"alert": "ping"}},
        notification_id=notification_id,
        push_type=PushType.ALERT,
        apns_topic="ie.klatt.snapper",
    )


async def _receive_h2_requests(
    transport: _FakeH2Transport, server: H2Connection
) -> list[RequestReceived]:
    """Yield one event-loop turn and parse outbound HTTP/2 requests."""
    await asyncio.sleep(0)
    events = server.receive_data(bytes(transport.written))
    transport.written.clear()
    return [e for e in events if isinstance(e, RequestReceived)]


class TestProtocolTimeoutSlotSafety:
    """Timeout-safe protocol prevents late-frame InvalidStateError and stream leaks."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "protocol_cls",
        [_TimeoutSafeDevelopmentClientProtocol, _TimeoutSafeProductionClientProtocol],
        ids=["sandbox-protocol", "production-protocol"],
    )
    async def test_late_200_after_timeout_does_not_raise_or_leak_stream_slot(
        self,
        protocol_cls: type[_TimeoutSafeProtocolMixin],
    ) -> None:
        """Late HTTP/2 200 after timeout leaves stream capacity intact for subsequent sends.

        Given: A real timeout-safe APNs protocol with ``MAX_CONCURRENT_STREAMS=1``.
        When: Request 1 times out under ``asyncio.wait_for``, then APNs delivers a
            late 200 + ``END_STREAM`` frame, followed by request 2 on the same protocol.
        Then: ``data_received`` raises no ``InvalidStateError``, the stream slot is
            restored to capacity ``1``, and request 2 completes with status ``200``.
        """
        loop = asyncio.get_running_loop()
        protocol, transport, server = _connected_h2_pair(
            protocol_cls, loop, max_concurrent_streams=1
        )
        assert protocol.free_channels._value == 1

        req1 = _request("notif-timeout-1")
        task1 = asyncio.create_task(protocol.send_notification(req1))
        await asyncio.sleep(0)
        events = server.receive_data(bytes(transport.written))
        transport.written.clear()
        stream_id = next(e.stream_id for e in events if isinstance(e, RequestReceived))
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(task1, timeout=0.01)

        assert protocol.free_channels._value == 1
        server.send_headers(
            stream_id, [(":status", "200"), ("apns-id", "notif-timeout-1")], end_stream=True
        )
        protocol.data_received(server.data_to_send())
        assert protocol.free_channels._value == 1
        server.receive_data(bytes(transport.written))
        transport.written.clear()

        req2 = _request("notif-ok-2")
        task2 = asyncio.create_task(protocol.send_notification(req2))
        await asyncio.sleep(0)
        events2 = server.receive_data(bytes(transport.written))
        transport.written.clear()
        stream_id2 = next(e.stream_id for e in events2 if isinstance(e, RequestReceived))
        server.send_headers(
            stream_id2, [(":status", "200"), ("apns-id", "notif-ok-2")], end_stream=True
        )
        protocol.data_received(server.data_to_send())
        result2 = await asyncio.wait_for(task2, timeout=1.0)
        assert result2.status == "200"
        assert protocol.free_channels._value == 1
        protocol.connection_lost(None)

    @pytest.mark.asyncio
    async def test_same_tick_cancel_race_with_late_200_and_400(self) -> None:
        """Same-tick cancellations handle late 200 and 400 frames without double-release.

        Given: A real protocol with an active request alongside requests whose
            response futures are cancelled in-place before their tasks unwind.
        When: Late 200 and late 400 + DATA frames arrive before task cancellation
            unwinds, followed by a normal 400 response.
        Then: No ``InvalidStateError`` is raised, stream capacity stays intact,
            and the normal 400 response completes cleanly.
        """
        loop = asyncio.get_running_loop()
        protocol, transport, server = _connected_h2_pair(
            _TimeoutSafeDevelopmentClientProtocol, loop, max_concurrent_streams=2
        )

        live_task = asyncio.create_task(protocol.send_notification(_request("notif-live")))
        race_task = asyncio.create_task(protocol.send_notification(_request("notif-race-200")))
        await asyncio.sleep(0)
        events = server.receive_data(bytes(transport.written))
        transport.written.clear()
        stream_ids = [e.stream_id for e in events if isinstance(e, RequestReceived)]
        assert len(stream_ids) == 2

        protocol.requests["notif-race-200"].cancel()
        server.send_headers(
            stream_ids[1], [(":status", "200"), ("apns-id", "notif-race-200")], end_stream=True
        )
        protocol.data_received(server.data_to_send())
        race_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await race_task
        assert protocol.free_channels._value == 1

        race_400_task = asyncio.create_task(protocol.send_notification(_request("notif-race-400")))
        await asyncio.sleep(0)
        events_400 = server.receive_data(bytes(transport.written))
        transport.written.clear()
        stream_400 = next(e.stream_id for e in events_400 if isinstance(e, RequestReceived))
        protocol.requests["notif-race-400"].cancel()
        server.send_headers(stream_400, [(":status", "400"), ("apns-id", "notif-race-400")])
        server.send_data(stream_400, b'{"reason":"BadDeviceToken"}', end_stream=True)
        protocol.data_received(server.data_to_send())
        race_400_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await race_400_task
        assert protocol.free_channels._value == 1

        err_task = asyncio.create_task(protocol.send_notification(_request("notif-real-400")))
        await asyncio.sleep(0)
        events_err = server.receive_data(bytes(transport.written))
        transport.written.clear()
        stream_err = next(e.stream_id for e in events_err if isinstance(e, RequestReceived))
        server.send_headers(stream_err, [(":status", "400"), ("apns-id", "notif-real-400")])
        server.send_data(stream_err, b'{"reason":"BadDeviceToken"}', end_stream=True)
        protocol.data_received(server.data_to_send())
        err_result = await asyncio.wait_for(err_task, timeout=1.0)
        assert err_result.status == "400"
        assert protocol.free_channels._value == 1

        protocol.requests["notif-live"].cancel()
        protocol.connection_lost(None)
        live_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await live_task

    @pytest.mark.asyncio
    async def test_acquire_cancel_closing_transport_and_disconnect(self) -> None:
        """Acquire cancellation, closing-transport cancellation, and disconnect stay safe.

        Given: A saturated protocol with an in-flight stream and a blocked waiter.
        When: The acquire-blocked task cancels, a new stream cancels while the
            transport is already closing, and ``connection_lost`` runs.
        Then: No ``InvalidStateError`` occurs and slots are not leaked or double-released.
        """
        loop = asyncio.get_running_loop()
        protocol, transport, server = _connected_h2_pair(
            _TimeoutSafeDevelopmentClientProtocol, loop, max_concurrent_streams=1
        )
        live_task = asyncio.create_task(protocol.send_notification(_request("notif-busy")))
        await asyncio.sleep(0)
        server.receive_data(bytes(transport.written))
        transport.written.clear()
        assert protocol.free_channels._value == 0

        blocked_task = asyncio.create_task(protocol.send_notification(_request("notif-blocked")))
        await asyncio.sleep(0)
        blocked_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await blocked_task

        protocol.requests["notif-busy"].cancel()
        transport.close()
        live_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await live_task
        assert protocol.free_channels._value == 1
        protocol.connection_lost(None)

    @pytest.mark.asyncio
    async def test_build_apns_client_pool_wires_hardened_protocol_and_surfaces_ssl_error(
        self,
    ) -> None:
        """Real pool wiring uses timeout-safe protocols and preserves SSL errors.

        Given: A real ``ApnsClientPool`` constructed inside a running event loop via
            ``build_apns_client_pool``.
        When: ``sandbox`` hits ``ssl.SSLCertVerificationError`` and ``production`` hits
            ``ConnectionRefusedError`` during connection setup.
        Then: ``sandbox`` propagates ``SSLCertVerificationError`` directly without
            converting it to ``MaxAttemptsExceeded``, while ``production`` exhausts
            connection attempts into ``MaxAttemptsExceeded``.
        """
        pool = build_apns_client_pool(_config(environment="sandbox_and_production"))
        sandbox_client = pool._select_client("sandbox")
        prod_client = pool._select_client("prod")
        assert isinstance(sandbox_client.pool, _HardenedKeyConnectionPool)
        assert isinstance(prod_client.pool, _HardenedKeyConnectionPool)
        assert sandbox_client.pool.protocol_class is _TimeoutSafeDevelopmentClientProtocol
        assert prod_client.pool.protocol_class is _TimeoutSafeProductionClientProtocol

        sandbox_client.pool.create_connection = AsyncMock(
            side_effect=ssl.SSLCertVerificationError(1, "certificate verify failed")
        )
        with pytest.raises(ssl.SSLCertVerificationError) as ssl_exc_info:
            await pool.send(
                env="sandbox",
                device_token="a" * 64,
                payload={"aps": {"alert": {"title": "t", "body": "b"}}},
                apns_topic="ie.klatt.snapper",
            )
        assert is_network_delivery_error(ssl_exc_info.value) is False

        prod_client.pool.max_connection_attempts = 1
        prod_client.pool.create_connection = AsyncMock(
            side_effect=ConnectionRefusedError("connection refused")
        )
        with pytest.raises(ApnsMaxAttemptsExceeded):
            await pool.send(
                env="prod",
                device_token="a" * 64,
                payload={"aps": {"alert": {"title": "t", "body": "b"}}},
                apns_topic="ie.klatt.snapper",
            )
        sandbox_client.pool.close()
        prod_client.pool.close()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "protocol_cls",
        [_TimeoutSafeDevelopmentClientProtocol, _TimeoutSafeProductionClientProtocol],
        ids=["sandbox-protocol", "production-protocol"],
    )
    async def test_empty_body_non_200_stream_ended_resolves_single_post_and_classifies(
        self,
        protocol_cls: type[_TimeoutSafeProtocolMixin],
    ) -> None:
        """Bodyless non-200 ``StreamEnded`` resolves on a single POST with status classification.

        Given: A real ``ApnsClientPool`` wired to a real timeout-safe HTTP/2
            protocol with ``MAX_CONCURRENT_STREAMS=1`` and ``max_connection_attempts=3``.
        When: The server completes a ``503`` stream and a ``403`` stream with
            no ``DATA`` frame, then completes a normal ``200`` send.
        Then: Bodyless ``503`` and ``403`` execute a single POST without pool
            retries and map to ``server_error`` (attempt-limited 5xx) and
            ``other`` (4xx) with description ``"empty response body"``, stream
            capacity stays ``1``, and the subsequent ``200`` succeeds.
        """
        loop = asyncio.get_running_loop()
        protocol, transport, server = _connected_h2_pair(
            protocol_cls, loop, max_concurrent_streams=1
        )
        env = "sandbox" if protocol_cls is _TimeoutSafeDevelopmentClientProtocol else "prod"
        pool = build_apns_client_pool(
            _config(environment="sandbox" if env == "sandbox" else "production")
        )
        raw_client = pool._select_client(env)
        raw_client.pool.max_connection_attempts = 3
        raw_client.pool.create_connection = AsyncMock(return_value=protocol)
        payload: JsonObject = {"aps": {"alert": {"title": "t", "body": "b"}}}

        task_503 = asyncio.create_task(
            pool.send(
                env=env, device_token="a" * 64, payload=payload, apns_topic="ie.klatt.snapper"
            )
        )
        requests_503 = await _receive_h2_requests(transport, server)
        assert len(requests_503) == 1
        apns_id_503 = dict(requests_503[0].headers)[b"apns-id"].decode("utf8")
        server.send_headers(
            requests_503[0].stream_id,
            [(":status", "503"), ("apns-id", apns_id_503)],
            end_stream=True,
        )
        protocol.data_received(server.data_to_send())
        res_503 = await asyncio.wait_for(task_503, timeout=0.5)
        assert await _receive_h2_requests(transport, server) == []
        assert (res_503.status_code, res_503.status, res_503.apns_id, res_503.description) == (
            503,
            "server_error",
            apns_id_503,
            "empty response body",
        )
        assert raw_client.pool.create_connection.await_count == 1
        assert protocol.free_channels._value == 1
        assert apns_id_503 not in protocol.requests
        assert requests_503[0].stream_id not in protocol.request_streams
        assert apns_id_503 not in protocol.request_statuses

        task_403 = asyncio.create_task(
            pool.send(
                env=env, device_token="a" * 64, payload=payload, apns_topic="ie.klatt.snapper"
            )
        )
        requests_403 = await _receive_h2_requests(transport, server)
        assert len(requests_403) == 1
        apns_id_403 = dict(requests_403[0].headers)[b"apns-id"].decode("utf8")
        server.send_headers(
            requests_403[0].stream_id,
            [(":status", "403"), ("apns-id", apns_id_403)],
            end_stream=True,
        )
        protocol.data_received(server.data_to_send())
        res_403 = await asyncio.wait_for(task_403, timeout=0.5)
        assert (res_403.status_code, res_403.status, res_403.apns_id, res_403.description) == (
            403,
            "other",
            apns_id_403,
            "empty response body",
        )
        assert protocol.free_channels._value == 1

        res_ok_task = asyncio.create_task(
            pool.send(
                env=env, device_token="a" * 64, payload=payload, apns_topic="ie.klatt.snapper"
            )
        )
        requests_ok = await _receive_h2_requests(transport, server)
        assert len(requests_ok) == 1
        apns_id_ok = dict(requests_ok[0].headers)[b"apns-id"].decode("utf8")
        server.send_headers(
            requests_ok[0].stream_id, [(":status", "200"), ("apns-id", apns_id_ok)], end_stream=True
        )
        protocol.data_received(server.data_to_send())
        res_ok = await asyncio.wait_for(res_ok_task, timeout=0.5)
        assert (res_ok.status_code, res_ok.status, protocol.free_channels._value) == (
            200,
            "success",
            1,
        )
        raw_client.pool.close()
        protocol.connection_lost(None)

    @pytest.mark.asyncio
    async def test_blank_json_503_resolves_and_statusless_stream_end_fails(self) -> None:
        """Blank JSON non-200 bodies resolve cleanly; statusless stream ends fail as network.

        Given: A real ``_TimeoutSafeDevelopmentClientProtocol`` with ``MAX_CONCURRENT_STREAMS=1``.
        When: One stream returns ``503`` with JSON ``{}`` (empty reason) and a
            second stream ends via ``on_stream_ended`` without response headers.
        Then: The blank-JSON ``503`` maps to ``server_error`` with description
            ``"empty response body"``, the statusless stream end fails with
            ``ApnsConnectionClosed``, and stream capacity returns to ``1``.
        """
        loop = asyncio.get_running_loop()
        protocol, transport, server = _connected_h2_pair(
            _TimeoutSafeDevelopmentClientProtocol, loop, max_concurrent_streams=1
        )
        task_blank = asyncio.create_task(protocol.send_notification(_request("notif-503-blank")))
        stream_blank = (await _receive_h2_requests(transport, server))[0].stream_id
        server.send_headers(stream_blank, [(":status", "503"), ("apns-id", "notif-503-blank")])
        server.send_data(stream_blank, b"{}", end_stream=True)
        protocol.data_received(server.data_to_send())
        mapped_blank = _map_result(await asyncio.wait_for(task_blank, timeout=0.5))
        assert (mapped_blank.status_code, mapped_blank.status, mapped_blank.description) == (
            503,
            "server_error",
            "empty response body",
        )
        assert protocol.free_channels._value == 1

        task_statusless = asyncio.create_task(
            protocol.send_notification(_request("notif-statusless"))
        )
        stream_statusless = (await _receive_h2_requests(transport, server))[0].stream_id
        protocol.on_stream_ended(stream_statusless)
        with pytest.raises(ApnsConnectionClosed) as statusless_exc:
            await asyncio.wait_for(task_statusless, timeout=0.5)
        assert is_network_delivery_error(statusless_exc.value) is True
        assert protocol.free_channels._value == 1
        protocol.connection_lost(None)

    @pytest.mark.asyncio
    async def test_hardened_pool_surfaces_non_transient_connect_errors_and_retries_ssl_drops(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Non-transient connect errors never chain ``ApnsConnectionError``; SSL drops retry.

        Given: A real ``ApnsClientPool`` with ``max_connection_attempts=3``
            and a bare ``ApnsConnectionError`` case on ``acquire``.
        When: ``sandbox`` raises a ``ValueError`` (with an original
            ``KeyError`` cause and without one), ``prod`` raises transient
            ``ssl.SSLEOFError``, and bare ``APNsKeyConnectionPool.acquire``
            raises an uncaused ``ApnsConnectionError``.
        Then: ``sandbox`` propagates ``ValueError`` after one attempt with
            ``is_network_delivery_error(raised) is False`` (preserving its
            original cause without chaining ``ApnsConnectionError``), ``prod``
            exhausts attempts into ``MaxAttemptsExceeded`` with
            ``is_network_delivery_error(raised) is True`` (as well as
            ``is_network_delivery_error(SSLEOFError) is True``), and bare
            ``ApnsConnectionError`` re-raises unchanged.
        """
        pool = build_apns_client_pool(_config(environment="sandbox_and_production"))
        sandbox_client = pool._select_client("sandbox")
        prod_client = pool._select_client("prod")
        sandbox_client.pool.max_connection_attempts = 3
        prod_client.pool.max_connection_attempts = 2
        monkeypatch.setattr("aioapns.connection.asyncio.sleep", AsyncMock())

        original_cause = KeyError("bad key")
        setup_err = ValueError("invalid APNs signing key")
        setup_err.__cause__ = original_cause
        sandbox_connect = AsyncMock(side_effect=setup_err)
        sandbox_client.pool.create_connection = sandbox_connect
        with pytest.raises(ValueError, match="invalid APNs signing key") as val_exc_info:
            await pool.send(
                env="sandbox",
                device_token="a" * 64,
                payload={"aps": {"alert": {"title": "t", "body": "b"}}},
                apns_topic="ie.klatt.snapper",
            )
        assert sandbox_connect.await_count == 1
        assert is_network_delivery_error(val_exc_info.value) is False
        assert val_exc_info.value.__cause__ is original_cause
        assert val_exc_info.value.__suppress_context__ is True

        bare_connect = AsyncMock(side_effect=ValueError("missing PEM header"))
        sandbox_client.pool.create_connection = bare_connect
        with pytest.raises(ValueError, match="missing PEM header") as bare_exc_info:
            await pool.send(
                env="sandbox",
                device_token="a" * 64,
                payload={"aps": {"alert": {"title": "t", "body": "b"}}},
                apns_topic="ie.klatt.snapper",
            )
        assert is_network_delivery_error(bare_exc_info.value) is False
        assert bare_exc_info.value.__cause__ is None
        assert bare_exc_info.value.__suppress_context__ is True

        eof_err = ssl.SSLEOFError("EOF occurred in violation")
        assert is_network_delivery_error(eof_err) is True
        prod_connect = AsyncMock(side_effect=eof_err)
        prod_client.pool.create_connection = prod_connect
        with pytest.raises(ApnsMaxAttemptsExceeded) as eof_exc_info:
            await pool.send(
                env="prod",
                device_token="a" * 64,
                payload={"aps": {"alert": {"title": "t", "body": "b"}}},
                apns_topic="ie.klatt.snapper",
            )
        assert prod_connect.await_count == 2
        assert is_network_delivery_error(eof_exc_info.value) is True

        monkeypatch.setattr(
            APNsKeyConnectionPool,
            "acquire",
            AsyncMock(side_effect=ApnsConnectionError()),
        )
        with pytest.raises(ApnsConnectionError):
            await sandbox_client.pool.acquire()

        sandbox_client.pool.close()
        prod_client.pool.close()


class TestAioApnsInternalContract:
    """Guard private ``aioapns`` attributes and methods relied on by our wrappers."""

    @pytest.mark.asyncio
    async def test_installed_aioapns_protocol_and_pool_expose_wrapped_internals(self) -> None:
        """Fail loudly if an ``aioapns`` upgrade renames or removes wrapped internals.

        Given: Real installed ``APNsDevelopmentClientProtocol``,
            ``APNsProductionClientProtocol``, and ``aioapns.APNs`` instances
            constructed without mocks on the running event loop.
        When: Inspecting every private attribute and overridden/super-called
            method used by ``_TimeoutSafeProtocolMixin`` and
            ``_HardenedKeyConnectionPool``.
        Then: All required attributes, nested channel/conn methods, and
            protocol/pool methods exist with their expected callability.
        """
        loop = asyncio.get_running_loop()
        for protocol_cls in (APNsDevelopmentClientProtocol, APNsProductionClientProtocol):
            protocol = protocol_cls("ie.klatt.snapper", loop=loop)
            for attr_name in ("requests", "request_streams", "request_statuses"):
                assert isinstance(getattr(protocol, attr_name), dict)
            assert hasattr(protocol, "transport")
            assert callable(protocol.free_channels.acquire)
            assert callable(protocol.free_channels.release)
            assert callable(protocol.conn.reset_stream)
            assert callable(protocol.conn.data_to_send)
            assert inspect.iscoroutinefunction(protocol.send_notification)
            for method_name in (
                "on_response_received",
                "on_data_received",
                "on_stream_ended",
                "connection_lost",
                "close",
                "flush",
            ):
                assert callable(getattr(protocol_cls, method_name))

        client = _AioApns(
            key="fake-key",
            key_id="KEYID",
            team_id="TEAMID",
            topic="ie.klatt.snapper",
            use_sandbox=True,
        )
        assert isinstance(client.pool, APNsKeyConnectionPool)
        for pool_attr in (
            "protocol_class",
            "key",
            "key_id",
            "team_id",
            "apns_topic",
            "max_connections",
            "max_connection_attempts",
            "ssl_context",
            "proxy_host",
            "proxy_port",
        ):
            assert hasattr(client.pool, pool_attr)
        assert inspect.iscoroutinefunction(client.pool.acquire)
        assert inspect.iscoroutinefunction(client.pool.create_connection)
        client.pool.close()
