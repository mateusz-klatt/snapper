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
        with pytest.raises(ssl.SSLCertVerificationError):
            await pool.send(
                env="sandbox",
                device_token="a" * 64,
                payload={"aps": {"alert": {"title": "t", "body": "b"}}},
                apns_topic="ie.klatt.snapper",
            )

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
