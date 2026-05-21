"""Unit tests for ``Socks5Server`` — asyncio SOCKS5 v5 listener.

Tests drive the server against in-process loopback clients so the wire
protocol exchange is exercised end-to-end without external dependencies.
``asyncio.open_connection`` (the outbound bind point) is mocked where
needed to simulate upstream failures and to assert source-IP propagation.
"""

import asyncio
import socket
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from loguru import logger as loguru_logger

from snapper.infrastructure.network.socks5_server import ATYP_DOMAIN
from snapper.infrastructure.network.socks5_server import ATYP_IPV4
from snapper.infrastructure.network.socks5_server import ATYP_IPV6
from snapper.infrastructure.network.socks5_server import CMD_CONNECT
from snapper.infrastructure.network.socks5_server import METHOD_NO_ACCEPTABLE
from snapper.infrastructure.network.socks5_server import METHOD_NO_AUTH
from snapper.infrastructure.network.socks5_server import REP_ADDRTYPE_NOT_SUPPORTED
from snapper.infrastructure.network.socks5_server import REP_COMMAND_NOT_SUPPORTED
from snapper.infrastructure.network.socks5_server import REP_CONNECTION_REFUSED
from snapper.infrastructure.network.socks5_server import REP_GENERAL_FAILURE
from snapper.infrastructure.network.socks5_server import REP_HOST_UNREACHABLE
from snapper.infrastructure.network.socks5_server import REP_NETWORK_UNREACHABLE
from snapper.infrastructure.network.socks5_server import REP_SUCCEEDED
from snapper.infrastructure.network.socks5_server import RSV_REQUIRED
from snapper.infrastructure.network.socks5_server import SOCKS_VERSION
from snapper.infrastructure.network.socks5_server import Socks5Server


def _free_port() -> int:
    """Helper — get an OS-assigned free TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class TestLifecycle:
    """``start`` / ``stop`` / ``is_running``."""

    @pytest.mark.asyncio
    async def test_start_starts_listener(self) -> None:
        """Spec — start() begins accepting on the configured port.

        Given a fresh server,
        When start() is awaited,
        Then is_running() returns True.
        """
        port = _free_port()
        server = Socks5Server(bind_addr="127.0.0.1", listen_port=port, listen_host="127.0.0.1")
        try:
            await server.start()
            assert server.is_running() is True
        finally:
            await server.stop()

    @pytest.mark.asyncio
    async def test_start_is_idempotent(self) -> None:
        """Spec — second start() while running is a no-op.

        Given a running server,
        When start() is called again,
        Then no exception fires and the listener is still running.
        """
        port = _free_port()
        server = Socks5Server(bind_addr="127.0.0.1", listen_port=port, listen_host="127.0.0.1")
        try:
            await server.start()
            await server.start()
            assert server.is_running() is True
        finally:
            await server.stop()

    @pytest.mark.asyncio
    async def test_stop_stops_listener(self) -> None:
        """Spec — stop() releases the port and clears is_running.

        Given a running server,
        When stop() is awaited,
        Then is_running() returns False AND the port is free.
        """
        port = _free_port()
        server = Socks5Server(bind_addr="127.0.0.1", listen_port=port, listen_host="127.0.0.1")
        await server.start()
        await server.stop()
        assert server.is_running() is False
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", port))

    @pytest.mark.asyncio
    async def test_stop_is_idempotent(self) -> None:
        """Spec — stop() called twice does not raise.

        Given a server that has already stopped,
        When stop() is called again,
        Then no exception fires.
        """
        port = _free_port()
        server = Socks5Server(bind_addr="127.0.0.1", listen_port=port, listen_host="127.0.0.1")
        await server.start()
        await server.stop()
        await server.stop()
        assert server.is_running() is False


class TestEndToEnd:
    """Full handshake + CONNECT + relay against in-process upstreams."""

    @pytest.mark.asyncio
    async def test_ipv4_connect_and_relay(self) -> None:
        """Spec — IPv4 CONNECT round-trips bytes through the server.

        Given an upstream echo server bound to 127.0.0.1,
        When a SOCKS5 client opens CONNECT to the upstream and sends a payload,
        Then the upstream receives the payload AND the client gets the echo back.
        """
        upstream_payload = bytearray()

        async def upstream_handler(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            data = await reader.read(1024)
            upstream_payload.extend(data)
            writer.write(b"echo:" + data)
            await writer.drain()
            writer.close()
            await writer.wait_closed()

        upstream_port = _free_port()
        upstream = await asyncio.start_server(
            upstream_handler, host="127.0.0.1", port=upstream_port
        )
        socks_port = _free_port()
        server = Socks5Server(
            bind_addr="127.0.0.1",
            listen_port=socks_port,
            listen_host="127.0.0.1",
        )
        try:
            await server.start()
            client_reader, client_writer = await asyncio.open_connection("127.0.0.1", socks_port)
            client_writer.write(bytes((SOCKS_VERSION, 1, METHOD_NO_AUTH)))
            await client_writer.drain()
            method_reply = await client_reader.readexactly(2)
            assert method_reply == bytes((SOCKS_VERSION, METHOD_NO_AUTH))
            client_writer.write(
                bytes((SOCKS_VERSION, CMD_CONNECT, RSV_REQUIRED, ATYP_IPV4))
                + socket.inet_aton("127.0.0.1")
                + upstream_port.to_bytes(2, "big")
            )
            await client_writer.drain()
            connect_reply = await client_reader.readexactly(10)
            assert connect_reply[0] == SOCKS_VERSION
            assert connect_reply[1] == REP_SUCCEEDED
            assert connect_reply[3] == ATYP_IPV4
            client_writer.write(b"hello")
            await client_writer.drain()
            response = await client_reader.read(1024)
            assert response == b"echo:hello"
            assert bytes(upstream_payload) == b"hello"
            client_writer.close()
            await client_writer.wait_closed()
        finally:
            await server.stop()
            upstream.close()
            await upstream.wait_closed()

    @pytest.mark.asyncio
    async def test_relay_drains_upstream_after_client_half_close(self) -> None:
        """Spec — client EOF does NOT drop in-flight upstream response.

        Given an upstream that writes a final response AFTER client EOF
            (the typical WebSocket close-frame pattern),
        When the client half-closes its send direction,
        Then the upstream's final bytes still reach the client before
            both halves close.

        This is the critical bug Codex Plan/Code Reviewer pinned: a
        naive FIRST_COMPLETED + cancel-other relay would drop the
        upstream's close frame.
        """

        async def upstream_handler(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            data = await reader.read(1024)
            await asyncio.sleep(0.05)
            writer.write(b"bye-after-eof:" + data)
            await writer.drain()
            writer.close()
            await writer.wait_closed()

        upstream_port = _free_port()
        upstream = await asyncio.start_server(
            upstream_handler, host="127.0.0.1", port=upstream_port
        )
        socks_port = _free_port()
        server = Socks5Server(
            bind_addr="127.0.0.1",
            listen_port=socks_port,
            listen_host="127.0.0.1",
        )
        try:
            await server.start()
            client_reader, client_writer = await asyncio.open_connection("127.0.0.1", socks_port)
            client_writer.write(bytes((SOCKS_VERSION, 1, METHOD_NO_AUTH)))
            await client_writer.drain()
            await client_reader.readexactly(2)
            client_writer.write(
                bytes((SOCKS_VERSION, CMD_CONNECT, RSV_REQUIRED, ATYP_IPV4))
                + socket.inet_aton("127.0.0.1")
                + upstream_port.to_bytes(2, "big")
            )
            await client_writer.drain()
            await client_reader.readexactly(10)
            client_writer.write(b"please-close")
            await client_writer.drain()
            client_writer.write_eof()
            response = await client_reader.read(1024)
            assert response == b"bye-after-eof:please-close"
            client_writer.close()
            await client_writer.wait_closed()
        finally:
            await server.stop()
            upstream.close()
            await upstream.wait_closed()


class TestNegotiation:
    """Direct unit tests against the negotiation helpers."""

    @staticmethod
    def _stream_pair() -> tuple[asyncio.StreamReader, Any]:
        """Helper — a feedable reader + a writer mock with drain awaitable."""
        reader = asyncio.StreamReader()
        writer = MagicMock()
        writer.write = MagicMock()
        writer.drain = AsyncMock()
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()
        return reader, writer

    @pytest.mark.asyncio
    async def test_negotiate_rejects_wrong_socks_version(self) -> None:
        """Spec — non-SOCKS5 version raises _Socks5Error.

        Given a client greeting with version 0x04 instead of 0x05,
        When _negotiate_method runs,
        Then it raises _Socks5Error.
        """
        reader, writer = self._stream_pair()
        reader.feed_data(bytes((0x04, 0x01, METHOD_NO_AUTH)))
        server = Socks5Server(bind_addr="127.0.0.1", listen_port=1)
        with pytest.raises(Exception, match="unsupported SOCKS version"):
            await server._negotiate_method(reader, writer)

    @pytest.mark.asyncio
    async def test_negotiate_rejects_no_supported_methods(self) -> None:
        """Spec — client without NO_AUTH gets METHOD_NO_ACCEPTABLE reply.

        Given a greeting offering only username/password auth (0x02),
        When _negotiate_method runs,
        Then the server writes (0x05, 0xFF) and raises _Socks5Error.
        """
        reader, writer = self._stream_pair()
        reader.feed_data(bytes((SOCKS_VERSION, 0x01, 0x02)))
        server = Socks5Server(bind_addr="127.0.0.1", listen_port=1)
        with pytest.raises(Exception, match="no acceptable auth"):
            await server._negotiate_method(reader, writer)
        writer.write.assert_called_with(bytes((SOCKS_VERSION, METHOD_NO_ACCEPTABLE)))

    @pytest.mark.asyncio
    async def test_negotiate_short_greeting_raises(self) -> None:
        """Spec — incomplete greeting bytes raise _Socks5Error.

        Given only the version byte arrives before EOF,
        When _negotiate_method runs,
        Then _Socks5Error fires (caller closes the socket).
        """
        reader, writer = self._stream_pair()
        reader.feed_data(bytes((SOCKS_VERSION,)))
        reader.feed_eof()
        server = Socks5Server(bind_addr="127.0.0.1", listen_port=1)
        with pytest.raises(Exception, match="short read"):
            await server._negotiate_method(reader, writer)


class TestRequestParsing:
    """Tests for ``_read_connect_request`` + address-type variants."""

    @staticmethod
    def _stream_pair() -> tuple[asyncio.StreamReader, Any]:
        reader = asyncio.StreamReader()
        writer = MagicMock()
        writer.write = MagicMock()
        writer.drain = AsyncMock()
        return reader, writer

    @pytest.mark.asyncio
    async def test_request_rejects_wrong_version(self) -> None:
        """Spec — request version != 0x05 raises _Socks5Error.

        Given a CONNECT request with header[0] = 0x04,
        When _read_connect_request runs,
        Then _Socks5Error fires.
        """
        reader, writer = self._stream_pair()
        reader.feed_data(bytes((0x04, CMD_CONNECT, RSV_REQUIRED, ATYP_IPV4)))
        server = Socks5Server(bind_addr="127.0.0.1", listen_port=1)
        with pytest.raises(Exception, match="unexpected SOCKS version"):
            await server._read_connect_request(reader, writer)

    @pytest.mark.asyncio
    async def test_request_rejects_nonzero_reserved_byte(self) -> None:
        """Spec — RFC 1928 reserved byte must be 0x00.

        Given a CONNECT request with reserved=0x42,
        When _read_connect_request runs,
        Then _Socks5Error fires (no failure reply, just close).
        """
        reader, writer = self._stream_pair()
        reader.feed_data(bytes((SOCKS_VERSION, CMD_CONNECT, 0x42, ATYP_IPV4)))
        server = Socks5Server(bind_addr="127.0.0.1", listen_port=1)
        with pytest.raises(Exception, match="reserved byte"):
            await server._read_connect_request(reader, writer)

    @pytest.mark.asyncio
    async def test_request_rejects_non_connect_cmd_with_rep_07(self) -> None:
        """Spec — BIND (0x02) or UDP ASSOCIATE (0x03) → REP_COMMAND_NOT_SUPPORTED.

        Given a request with cmd=0x02,
        When _read_connect_request runs,
        Then the server writes a reply with REP=0x07 (RFC 1928 §6) and
        raises _Socks5Error.
        """
        reader, writer = self._stream_pair()
        reader.feed_data(bytes((SOCKS_VERSION, 0x02, RSV_REQUIRED, ATYP_IPV4)))
        server = Socks5Server(bind_addr="127.0.0.1", listen_port=1)
        with pytest.raises(Exception, match="unsupported SOCKS5 command"):
            await server._read_connect_request(reader, writer)
        first_call = writer.write.call_args_list[0]
        assert first_call.args[0][1] == REP_COMMAND_NOT_SUPPORTED

    @pytest.mark.asyncio
    async def test_request_reads_ipv4_address(self) -> None:
        """Spec — ATYP=IPv4 yields the dotted-quad host.

        Given a CONNECT to 192.0.2.1:443,
        When _read_connect_request returns,
        Then the tuple is ("192.0.2.1", 443).
        """
        reader, writer = self._stream_pair()
        reader.feed_data(bytes((SOCKS_VERSION, CMD_CONNECT, RSV_REQUIRED, ATYP_IPV4)))
        reader.feed_data(socket.inet_aton("192.0.2.1") + (443).to_bytes(2, "big"))
        server = Socks5Server(bind_addr="127.0.0.1", listen_port=1)
        host, port = await server._read_connect_request(reader, writer)
        assert host == "192.0.2.1"
        assert port == 443

    @pytest.mark.asyncio
    async def test_request_reads_ipv6_address(self) -> None:
        """Spec — ATYP=IPv6 yields the canonical IPv6 form.

        Given a CONNECT to [2001:db8::1]:443,
        When _read_connect_request returns,
        Then the tuple is ("2001:db8::1", 443).
        """
        reader, writer = self._stream_pair()
        reader.feed_data(bytes((SOCKS_VERSION, CMD_CONNECT, RSV_REQUIRED, ATYP_IPV6)))
        reader.feed_data(
            socket.inet_pton(socket.AF_INET6, "2001:db8::1") + (443).to_bytes(2, "big")
        )
        server = Socks5Server(bind_addr="127.0.0.1", listen_port=1)
        host, port = await server._read_connect_request(reader, writer)
        assert host == "2001:db8::1"
        assert port == 443

    @pytest.mark.asyncio
    async def test_request_reads_domain_address(self) -> None:
        """Spec — ATYP=domain yields the ASCII hostname.

        Given a CONNECT to "ws.kraken.com:443",
        When _read_connect_request returns,
        Then the tuple is ("ws.kraken.com", 443). Domain resolution
        happens later in _open_upstream (socks5h semantics).
        """
        domain = b"ws.kraken.com"
        reader, writer = self._stream_pair()
        reader.feed_data(bytes((SOCKS_VERSION, CMD_CONNECT, RSV_REQUIRED, ATYP_DOMAIN)))
        reader.feed_data(bytes((len(domain),)) + domain + (443).to_bytes(2, "big"))
        server = Socks5Server(bind_addr="127.0.0.1", listen_port=1)
        host, port = await server._read_connect_request(reader, writer)
        assert host == "ws.kraken.com"
        assert port == 443

    @pytest.mark.asyncio
    async def test_request_rejects_non_ascii_domain(self) -> None:
        """Spec — domain bytes outside ASCII raise _Socks5Error.

        Given a CONNECT whose domain payload contains 0xFF,
        When _read_connect_request runs,
        Then _Socks5Error fires (wrapped from UnicodeDecodeError).
        """
        reader, writer = self._stream_pair()
        reader.feed_data(bytes((SOCKS_VERSION, CMD_CONNECT, RSV_REQUIRED, ATYP_DOMAIN)))
        reader.feed_data(bytes((1, 0xFF)) + (443).to_bytes(2, "big"))
        server = Socks5Server(bind_addr="127.0.0.1", listen_port=1)
        with pytest.raises(Exception, match="domain bytes not ASCII"):
            await server._read_connect_request(reader, writer)

    @pytest.mark.asyncio
    async def test_request_rejects_unknown_atyp(self) -> None:
        """Spec — unknown address type byte returns REP_ADDRTYPE_NOT_SUPPORTED.

        Given a request with atyp=0x99,
        When _read_connect_request runs,
        Then the server writes REP_ADDRTYPE_NOT_SUPPORTED and raises.
        """
        reader, writer = self._stream_pair()
        reader.feed_data(bytes((SOCKS_VERSION, CMD_CONNECT, RSV_REQUIRED, 0x99)))
        server = Socks5Server(bind_addr="127.0.0.1", listen_port=1)
        with pytest.raises(Exception, match="unsupported address type"):
            await server._read_connect_request(reader, writer)
        first_call = writer.write.call_args_list[0]
        assert first_call.args[0][1] == REP_ADDRTYPE_NOT_SUPPORTED


class TestFailureReplyShape:
    """Tests that failure replies are well-formed regardless of request ATYP.

    Pinned by Codex Code Reviewer: previously the failure reply echoed
    the request's ATYP, producing malformed packets for the domain
    case (4 zero bytes are not a valid length-prefixed domain).
    """

    @pytest.mark.asyncio
    async def test_failure_reply_uses_ipv4_zero_placeholder_for_domain_request(
        self,
    ) -> None:
        """Spec — failure reply on a DOMAIN-typed CONNECT still uses IPv4 placeholder.

        Given a domain CONNECT that fails with REP_HOST_UNREACHABLE,
        When _reply_failure runs,
        Then the reply is the 10-byte IPv4 form
            (SOCKS_VERSION, REP, RSV, ATYP_IPV4, 4 zero address bytes,
             2 zero port bytes) — NOT the domain-typed form.
        """
        writer = MagicMock()
        writer.write = MagicMock()
        writer.drain = AsyncMock()
        server = Socks5Server(bind_addr="127.0.0.1", listen_port=1)
        await server._reply_failure(writer, REP_HOST_UNREACHABLE)
        sent = writer.write.call_args[0][0]
        assert sent == bytes(
            (SOCKS_VERSION, REP_HOST_UNREACHABLE, RSV_REQUIRED, ATYP_IPV4, 0, 0, 0, 0, 0, 0)
        )

    @pytest.mark.asyncio
    async def test_failure_reply_uses_ipv4_zero_placeholder_for_ipv6_request(
        self,
    ) -> None:
        """Spec — failure reply on an IPv6 CONNECT still uses IPv4 placeholder.

        Given an IPv6 CONNECT that fails,
        When _reply_failure runs,
        Then the reply is the 10-byte IPv4 form (not 22-byte IPv6).
        """
        writer = MagicMock()
        writer.write = MagicMock()
        writer.drain = AsyncMock()
        server = Socks5Server(bind_addr="127.0.0.1", listen_port=1)
        await server._reply_failure(writer, REP_GENERAL_FAILURE)
        sent = writer.write.call_args[0][0]
        assert len(sent) == 10
        assert sent[3] == ATYP_IPV4


class TestOpenUpstream:
    """Tests for ``_open_upstream`` error-to-reply mapping + source-IP binding."""

    @staticmethod
    def _writer_mock() -> Any:
        writer = MagicMock()
        writer.write = MagicMock()
        writer.drain = AsyncMock()
        return writer

    @pytest.mark.asyncio
    async def test_open_upstream_passes_bind_addr_as_local_addr(self) -> None:
        """Spec — outbound socket binds to ``self.bind_addr``.

        Given a server with bind_addr="10.1.2.3",
        When _open_upstream runs successfully (open_connection mocked),
        Then asyncio.open_connection is called with local_addr=("10.1.2.3", 0).
        """
        writer = self._writer_mock()
        server = Socks5Server(bind_addr="10.1.2.3", listen_port=1)
        fake_reader = MagicMock()
        fake_writer = MagicMock()
        with patch(
            "snapper.infrastructure.network.socks5_server.asyncio.open_connection",
            new=AsyncMock(return_value=(fake_reader, fake_writer)),
        ) as mock_open:
            result = await server._open_upstream("ws.kraken.com", 443, writer)
        mock_open.assert_awaited_once()
        kwargs = mock_open.call_args.kwargs
        assert kwargs["local_addr"] == ("10.1.2.3", 0)
        assert kwargs["host"] == "ws.kraken.com"
        assert kwargs["port"] == 443
        assert result == (fake_reader, fake_writer)

    @pytest.mark.asyncio
    async def test_open_upstream_translates_timeout(self) -> None:
        """Spec — TimeoutError maps to REP_HOST_UNREACHABLE.

        Given asyncio.open_connection times out,
        When _open_upstream runs,
        Then the writer receives a reply with REP_HOST_UNREACHABLE and
        _Socks5Error fires.
        """
        writer = self._writer_mock()
        server = Socks5Server(bind_addr="127.0.0.1", listen_port=1)
        with patch(
            "snapper.infrastructure.network.socks5_server.asyncio.open_connection",
            new=AsyncMock(side_effect=TimeoutError()),
        ), pytest.raises(Exception, match="timed out"):
            await server._open_upstream("ws.kraken.com", 443, writer)
        first_call = writer.write.call_args_list[0]
        assert first_call.args[0][1] == REP_HOST_UNREACHABLE

    @pytest.mark.asyncio
    async def test_open_upstream_translates_connection_refused(self) -> None:
        """Spec — ConnectionRefusedError maps to REP_CONNECTION_REFUSED.

        Given the upstream actively refuses the TCP SYN,
        When _open_upstream runs,
        Then the reply carries REP_CONNECTION_REFUSED.
        """
        writer = self._writer_mock()
        server = Socks5Server(bind_addr="127.0.0.1", listen_port=1)
        with patch(
            "snapper.infrastructure.network.socks5_server.asyncio.open_connection",
            new=AsyncMock(side_effect=ConnectionRefusedError()),
        ), pytest.raises(Exception, match="refused"):
            await server._open_upstream("ws.kraken.com", 443, writer)
        first_call = writer.write.call_args_list[0]
        assert first_call.args[0][1] == REP_CONNECTION_REFUSED

    @pytest.mark.asyncio
    async def test_open_upstream_translates_network_unreachable(self) -> None:
        """Spec — OSError with errno 101 (ENETUNREACH) → REP_NETWORK_UNREACHABLE.

        Given a kernel ENETUNREACH (typical when the WG interface is
        down or the source IP has no route),
        When _open_upstream runs,
        Then the reply carries REP_NETWORK_UNREACHABLE.
        """
        writer = self._writer_mock()
        server = Socks5Server(bind_addr="127.0.0.1", listen_port=1)
        err = OSError(101, "Network is unreachable")
        with patch(
            "snapper.infrastructure.network.socks5_server.asyncio.open_connection",
            new=AsyncMock(side_effect=err),
        ), pytest.raises(Exception, match="upstream connect failed"):
            await server._open_upstream("ws.kraken.com", 443, writer)
        first_call = writer.write.call_args_list[0]
        assert first_call.args[0][1] == REP_NETWORK_UNREACHABLE

    @pytest.mark.asyncio
    async def test_open_upstream_translates_generic_oserror(self) -> None:
        """Spec — generic OSError → REP_GENERAL_FAILURE.

        Given an unexpected OSError errno,
        When _open_upstream runs,
        Then the reply carries REP_GENERAL_FAILURE so the client sees
        a meaningful (if non-specific) failure.
        """
        writer = self._writer_mock()
        server = Socks5Server(bind_addr="127.0.0.1", listen_port=1)
        err = OSError(22, "Invalid argument")
        with patch(
            "snapper.infrastructure.network.socks5_server.asyncio.open_connection",
            new=AsyncMock(side_effect=err),
        ), pytest.raises(Exception, match="upstream connect failed"):
            await server._open_upstream("ws.kraken.com", 443, writer)
        first_call = writer.write.call_args_list[0]
        assert first_call.args[0][1] == REP_GENERAL_FAILURE


class TestRelay:
    """Tests for the bidirectional pump + half-close drain."""

    @pytest.mark.asyncio
    async def test_pump_copies_until_eof(self) -> None:
        """Spec — _pump copies bytes until reader hits EOF.

        Given a stream that feeds three chunks then EOF,
        When _pump runs,
        Then the writer sees exactly those bytes in order and the
        coroutine returns cleanly.
        """
        reader = asyncio.StreamReader()
        for chunk in (b"abc", b"def", b"ghi"):
            reader.feed_data(chunk)
        reader.feed_eof()
        writer = MagicMock()
        writer.write = MagicMock()
        writer.drain = AsyncMock()
        writer.can_write_eof = MagicMock(return_value=False)
        server = Socks5Server(bind_addr="127.0.0.1", listen_port=1)
        await server._pump(reader, writer, half_close_target=writer)
        written = b"".join(call.args[0] for call in writer.write.call_args_list)
        assert written == b"abcdefghi"

    @pytest.mark.asyncio
    async def test_pump_calls_write_eof_on_clean_eof_when_supported(self) -> None:
        """Spec — _pump half-closes the target writer after EOF.

        Given a target writer that supports write_eof,
        When _pump finishes normally (reader EOFs),
        Then write_eof is called so the OTHER pump's reader sees EOF.
        """
        reader = asyncio.StreamReader()
        reader.feed_eof()
        writer = MagicMock()
        writer.write = MagicMock()
        writer.drain = AsyncMock()
        target = MagicMock()
        target.can_write_eof = MagicMock(return_value=True)
        target.write_eof = MagicMock()
        server = Socks5Server(bind_addr="127.0.0.1", listen_port=1)
        await server._pump(reader, writer, half_close_target=target)
        target.write_eof.assert_called_once()

    @pytest.mark.asyncio
    async def test_pump_skips_write_eof_when_unsupported(self) -> None:
        """Spec — _pump tolerates targets that cannot half-close.

        Given a target whose can_write_eof returns False (e.g. SSL),
        When _pump finishes,
        Then write_eof is NOT called and no exception fires.
        """
        reader = asyncio.StreamReader()
        reader.feed_eof()
        writer = MagicMock()
        writer.write = MagicMock()
        writer.drain = AsyncMock()
        target = MagicMock()
        target.can_write_eof = MagicMock(return_value=False)
        target.write_eof = MagicMock()
        server = Socks5Server(bind_addr="127.0.0.1", listen_port=1)
        await server._pump(reader, writer, half_close_target=target)
        target.write_eof.assert_not_called()

    @pytest.mark.asyncio
    async def test_pump_swallows_write_eof_attribute_error(self) -> None:
        """Spec — _pump tolerates targets whose write_eof raises AttributeError.

        Given a target whose can_write_eof returns True but write_eof
            raises AttributeError (rare transport implementations),
        When _pump finishes,
        Then no exception escapes (suppressed by contextlib.suppress).
        """
        reader = asyncio.StreamReader()
        reader.feed_eof()
        writer = MagicMock()
        writer.write = MagicMock()
        writer.drain = AsyncMock()
        target = MagicMock()
        target.can_write_eof = MagicMock(return_value=True)
        target.write_eof = MagicMock(side_effect=AttributeError("noop"))
        server = Socks5Server(bind_addr="127.0.0.1", listen_port=1)
        await server._pump(reader, writer, half_close_target=target)

    @pytest.mark.asyncio
    async def test_pump_returns_on_connection_error(self) -> None:
        """Spec — _pump swallows ConnectionError + returns.

        Given a writer whose drain raises ConnectionResetError,
        When _pump runs,
        Then the coroutine returns cleanly (no propagation).
        """
        reader = asyncio.StreamReader()
        reader.feed_data(b"x")
        writer = MagicMock()
        writer.write = MagicMock()
        writer.drain = AsyncMock(side_effect=ConnectionResetError())
        target = MagicMock()
        target.can_write_eof = MagicMock(return_value=False)
        server = Socks5Server(bind_addr="127.0.0.1", listen_port=1)
        await server._pump(reader, writer, half_close_target=target)

    @pytest.mark.asyncio
    async def test_relay_logs_pump_exceptions(self, caplog: Any) -> None:
        """Spec — _relay surfaces pump exceptions via debug log.

        Given a pump that raises an unexpected exception,
        When _relay completes,
        Then the exception is logged (proves we are not silently
        swallowing programming errors in pump coroutines).

        The loguru sink is removed in a finally block so this test
        does not leak handlers into the global logger and pollute
        other tests' caplog assertions.
        """
        handler_id = loguru_logger.add(caplog.handler, format="{message}", level="DEBUG")
        try:
            server = Socks5Server(bind_addr="127.0.0.1", listen_port=1)
            with patch.object(server, "_pump", new=AsyncMock(side_effect=RuntimeError("boom"))):
                await server._relay(MagicMock(), MagicMock(), MagicMock(), MagicMock())
        finally:
            loguru_logger.remove(handler_id)
        assert "pump finished with exception" in caplog.text

    @pytest.mark.asyncio
    async def test_relay_drain_timeout_cancels_pending_pumps(self) -> None:
        """Spec — half-close drain has a bounded timeout.

        Given one pump finished and the other still running after
            _HALF_CLOSE_DRAIN_S,
        When the drain timeout fires,
        Then the remaining pump is cancelled cleanly (no leak).

        Patch _HALF_CLOSE_DRAIN_S to a small value to keep the test fast.
        """
        server = Socks5Server(bind_addr="127.0.0.1", listen_port=1)

        async def quick_pump(*_a: object, **_kw: object) -> None:
            await asyncio.sleep(0.005)

        async def slow_pump(*_a: object, **_kw: object) -> None:
            await asyncio.sleep(10.0)

        call_count = {"n": 0}

        async def alternating(*args: object, **kwargs: object) -> None:
            call_count["n"] += 1
            if call_count["n"] == 1:
                await quick_pump(*args, **kwargs)
            else:
                await slow_pump(*args, **kwargs)

        with (
            patch.object(server, "_pump", new=AsyncMock(side_effect=alternating)),
            patch(
                "snapper.infrastructure.network.socks5_server._HALF_CLOSE_DRAIN_S",
                0.05,
            ),
        ):
            await server._relay(MagicMock(), MagicMock(), MagicMock(), MagicMock())


class TestHandleClient:
    """End-to-end ``_handle_client`` paths via real loopback connections."""

    @pytest.mark.asyncio
    async def test_handle_client_swallows_relay_oserror(self) -> None:
        """Spec — ConnectionError during relay does not crash the handler.

        Given a successful negotiate + open,
        When _reply_success raises ConnectionError (client disappeared
        between CONNECT request and the success reply),
        Then _handle_client logs + cleans up; no exception escapes.
        """
        server = Socks5Server(bind_addr="127.0.0.1", listen_port=1)
        fake_reader = MagicMock()
        fake_writer = MagicMock()
        fake_writer.close = MagicMock()
        fake_writer.wait_closed = AsyncMock()
        fake_upstream_reader = MagicMock()
        fake_upstream_writer = MagicMock()
        fake_upstream_writer.close = MagicMock()
        fake_upstream_writer.wait_closed = AsyncMock()
        with (
            patch.object(
                server,
                "_negotiate_and_open",
                new=AsyncMock(return_value=(fake_upstream_reader, fake_upstream_writer)),
            ),
            patch.object(
                server,
                "_reply_success",
                new=AsyncMock(side_effect=ConnectionResetError("client gone")),
            ),
        ):
            await server._handle_client(fake_reader, fake_writer)

    @pytest.mark.asyncio
    async def test_relay_force_cancels_unresponsive_pump(self) -> None:
        """Spec — pump that ignores drain timeout still gets cancelled.

        Given a pump that swallows CancelledError and keeps running,
        When _relay's drain timeout fires and the cancel loop runs,
        Then the cancel + await path completes without leaking the task.
        """
        server = Socks5Server(bind_addr="127.0.0.1", listen_port=1)
        cancel_observed = {"flag": False}

        async def quick_pump(*_a: object, **_kw: object) -> None:
            await asyncio.sleep(0.005)

        async def stubborn_pump(*_a: object, **_kw: object) -> None:
            try:
                await asyncio.sleep(10.0)
            except asyncio.CancelledError:
                cancel_observed["flag"] = True
                raise

        call_count = {"n": 0}

        async def alternating(*args: object, **kwargs: object) -> None:
            call_count["n"] += 1
            if call_count["n"] == 1:
                await quick_pump(*args, **kwargs)
            else:
                await stubborn_pump(*args, **kwargs)

        with (
            patch.object(server, "_pump", new=AsyncMock(side_effect=alternating)),
            patch(
                "snapper.infrastructure.network.socks5_server._HALF_CLOSE_DRAIN_S",
                0.01,
            ),
        ):
            await server._relay(MagicMock(), MagicMock(), MagicMock(), MagicMock())
        assert cancel_observed["flag"] is True

    @pytest.mark.asyncio
    async def test_handle_client_swallows_protocol_errors(self) -> None:
        """Spec — malformed peer does not crash the listener.

        Given a SOCKS5 server,
        When a peer sends only the version byte then closes,
        Then _handle_client logs + cleans up; no exception escapes.
        """
        port = _free_port()
        server = Socks5Server(bind_addr="127.0.0.1", listen_port=port, listen_host="127.0.0.1")
        try:
            await server.start()
            client_reader, client_writer = await asyncio.open_connection("127.0.0.1", port)
            client_writer.write(bytes((SOCKS_VERSION,)))
            await client_writer.drain()
            client_writer.close()
            await client_writer.wait_closed()
            await asyncio.sleep(0.05)
            assert server.is_running() is True
        finally:
            await server.stop()
