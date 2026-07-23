"""Minimal asyncio SOCKS5 v5 server with TCP CONNECT and source-IP binding.

This module powers the snapper-egress sidecar's per-tunnel listener. Each
tunnel exposes one ``Socks5Server`` instance bound to the tunnel's
WireGuard interface IP; the kernel's source-based routing (``ip rule from
<tunnel_addr> table N``) then sends every outbound TCP packet through
``wg-<tunnel_id>``.

Wire-protocol scope (intentionally minimal):

* SOCKS5 only (no SOCKS4 / SOCKS4a / HTTP CONNECT fallback).
* Authentication method NO_AUTHENTICATION (``0x00``) only. The listener
  is bound to a private Docker network; auth would add noise without
  meaningful protection.
* CONNECT command only (no BIND, no UDP ASSOCIATE).
* Address types: IPv4 (``0x01``), domain (``0x03``, resolved remotely
  per ``socks5h`` semantics), IPv6 (``0x04``).

The server's source-address binding is the mechanism that makes the
tunnel switching actually work: ``asyncio.open_connection(host, port,
local_addr=(bind_addr, 0))`` opens an outbound socket whose source IP is
``bind_addr``, which then matches the per-tunnel ``ip rule`` and routes
the packet through the correct WG interface.

RFC notes (RFC 1928):

* The handshake and CONNECT request steps are bounded by short timeouts.
* The relay loop is UNBOUNDED — Kraken WebSocket sessions live for
  hours, so wrapping the relay in a global ``wait_for`` would kill them
  silently.
* Failure replies always carry the IPv4 placeholder ``0.0.0.0:0`` as
  BND.ADDR/PORT regardless of the request's ATYP — emitting a different
  ATYP in the failure reply produces malformed packets.
* Unsupported commands get reply code ``0x07`` (Command not supported),
  not ``0x02`` (Not allowed by ruleset).
"""

import asyncio
import contextlib
import socket
import struct
from typing import Final

from loguru import logger

SOCKS_VERSION: Final[int] = 0x05
"""SOCKS protocol version byte."""

METHOD_NO_AUTH: Final[int] = 0x00
"""Authentication method: no authentication required."""

METHOD_NO_ACCEPTABLE: Final[int] = 0xFF
"""Reply code: client offered no acceptable authentication method."""

CMD_CONNECT: Final[int] = 0x01
"""Command: TCP CONNECT (the only command this server supports)."""

ATYP_IPV4: Final[int] = 0x01
"""Address type: IPv4 (4 raw bytes)."""

ATYP_DOMAIN: Final[int] = 0x03
"""Address type: domain name (1-byte length prefix + ASCII bytes)."""

ATYP_IPV6: Final[int] = 0x04
"""Address type: IPv6 (16 raw bytes)."""

REP_SUCCEEDED: Final[int] = 0x00
"""Reply code: connection succeeded."""

REP_GENERAL_FAILURE: Final[int] = 0x01
"""Reply code: general SOCKS server failure."""

REP_NETWORK_UNREACHABLE: Final[int] = 0x03
"""Reply code: network unreachable."""

REP_HOST_UNREACHABLE: Final[int] = 0x04
"""Reply code: host unreachable."""

REP_CONNECTION_REFUSED: Final[int] = 0x05
"""Reply code: connection refused by destination."""

REP_COMMAND_NOT_SUPPORTED: Final[int] = 0x07
"""Reply code: command not supported (RFC 1928 section 6)."""

REP_ADDRTYPE_NOT_SUPPORTED: Final[int] = 0x08
"""Reply code: address type not supported."""

RSV_REQUIRED: Final[int] = 0x00
"""RFC 1928 section 6: the reserved byte MUST be ``0x00`` in every request/reply."""

_NEGOTIATION_TIMEOUT_S: Final[float] = 5.0
"""Maximum time to wait for the SOCKS5 greeting + CONNECT request."""

_CONNECT_TIMEOUT_S: Final[float] = 10.0
"""Maximum time to wait for the outbound TCP CONNECT to complete."""

_HALF_CLOSE_DRAIN_S: Final[float] = 5.0
"""After one direction half-closes, how long the other direction may
keep draining before being cancelled. Bounds the worst case where the
remote peer never finishes sending."""

_RELAY_BUFFER_SIZE: Final[int] = 65536
"""Per-direction relay chunk size for the bidirectional copy."""


class Socks5Server:
    """Per-tunnel SOCKS5 v5 listener bound to a fixed source IP.

    Each tunnel in the snapper-egress sidecar owns one ``Socks5Server``
    instance. The server accepts SOCKS5 clients on
    ``(listen_host, listen_port)`` and proxies each accepted connection's
    CONNECT request out through ``asyncio.open_connection`` with
    ``local_addr=(bind_addr, 0)``. The kernel's source-routing rules
    then carry the packets through the matching WG interface.

    Attributes:
        bind_addr: Source IP the server binds outbound TCP sockets to.
            Equals the tunnel's WireGuard interface address.
        listen_host: Host the listener accepts on. Defaults to ``0.0.0.0``
            (the Docker private network).
        listen_port: TCP port the listener accepts on.
    """

    def __init__(
        self,
        *,
        bind_addr: str,
        listen_port: int,
        listen_host: str = "0.0.0.0",
    ) -> None:
        """Create the server without starting it.

        Args:
            bind_addr: Source IP for outbound sockets. The kernel matches
                this against ``ip rule from <bind_addr>`` to pick the
                correct tunnel.
            listen_port: TCP port the SOCKS5 listener binds to.
            listen_host: Listen address. Defaults to ``0.0.0.0`` so the
                listener is reachable from any peer on the Docker
                private network. NEVER bind to a host-network address
                (the listener has NO authentication).
        """
        self.bind_addr = bind_addr
        self.listen_host = listen_host
        self.listen_port = listen_port
        self._server: asyncio.Server | None = None

    async def start(self) -> None:
        """Begin accepting SOCKS5 connections.

        Idempotent: a second call while the server is already running
        is a no-op.
        """
        if self._server is not None:
            return
        self._server = await asyncio.start_server(
            self._handle_client,
            host=self.listen_host,
            port=self.listen_port,
        )
        logger.info(
            "socks5: listening on {}:{} (bind_addr={})",
            self.listen_host,
            self.listen_port,
            self.bind_addr,
        )

    async def stop(self) -> None:
        """Stop accepting new connections and wait for the listener to drain.

        Idempotent: a second call after the server has already stopped
        is a no-op. Currently-in-flight connections are NOT forcibly
        closed — they finish their relay naturally.
        """
        if self._server is None:
            return
        self._server.close()
        await self._server.wait_closed()
        self._server = None
        logger.info(
            "socks5: stopped listener on {}:{}",
            self.listen_host,
            self.listen_port,
        )

    def is_running(self) -> bool:
        """Return whether the listener is currently accepting connections.

        Returns:
            ``True`` if ``start()`` has been called and ``stop()`` has
            not yet completed; ``False`` otherwise.
        """
        return self._server is not None

    async def _handle_client(
        self,
        client_reader: asyncio.StreamReader,
        client_writer: asyncio.StreamWriter,
    ) -> None:
        """Drive one SOCKS5 conversation end-to-end.

        Negotiation + CONNECT establishment is bounded by
        ``_NEGOTIATION_TIMEOUT_S + _CONNECT_TIMEOUT_S``; the relay
        loop is UNBOUNDED (long-lived WebSocket sessions). Catches
        protocol-level and IO errors so a misbehaving peer cannot crash
        the listener; unexpected exceptions still bubble to asyncio's
        default handler so they are not silently lost.

        Args:
            client_reader: Stream the SOCKS5 client writes to.
            client_writer: Stream the SOCKS5 client reads from.
        """
        try:
            upstream_reader, upstream_writer = await asyncio.wait_for(
                self._negotiate_and_open(client_reader, client_writer),
                timeout=_NEGOTIATION_TIMEOUT_S + _CONNECT_TIMEOUT_S,
            )
        except (OSError, _Socks5Error) as exc:
            logger.debug("socks5: client negotiation ended: {}", exc)
            client_writer.close()
            with contextlib.suppress(OSError, ConnectionError):
                await client_writer.wait_closed()
            return
        try:
            await self._reply_success(client_writer)
            await self._relay(client_reader, client_writer, upstream_reader, upstream_writer)
        except OSError as exc:
            logger.debug("socks5: relay ended: {}", exc)
        finally:
            upstream_writer.close()
            with contextlib.suppress(OSError, ConnectionError):
                await upstream_writer.wait_closed()
            client_writer.close()
            with contextlib.suppress(OSError, ConnectionError):
                await client_writer.wait_closed()

    async def _negotiate_and_open(
        self,
        client_reader: asyncio.StreamReader,
        client_writer: asyncio.StreamWriter,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """Run greeting + CONNECT request + open upstream.

        Returns the upstream streams on success; raises ``_Socks5Error``
        (or lets the underlying IO error propagate) on failure so the
        caller can clean up.
        """
        await self._negotiate_method(client_reader, client_writer)
        host, port = await self._read_connect_request(client_reader, client_writer)
        return await self._open_upstream(host, port, client_writer)

    async def _negotiate_method(
        self,
        client_reader: asyncio.StreamReader,
        client_writer: asyncio.StreamWriter,
    ) -> None:
        """Read SOCKS5 greeting and reply with NO_AUTH (or reject).

        Raises:
            _Socks5Error: if the client does not advertise NO_AUTH.
        """
        header = await self._readexactly_or_raise(client_reader, 2, "greeting header")
        version, nmethods = header[0], header[1]
        if version != SOCKS_VERSION:
            raise _Socks5Error(f"unsupported SOCKS version {version}")
        methods = await self._readexactly_or_raise(client_reader, nmethods, "greeting methods")
        if METHOD_NO_AUTH not in methods:
            client_writer.write(bytes((SOCKS_VERSION, METHOD_NO_ACCEPTABLE)))
            await client_writer.drain()
            raise _Socks5Error("client offered no acceptable auth method")
        client_writer.write(bytes((SOCKS_VERSION, METHOD_NO_AUTH)))
        await client_writer.drain()

    async def _read_connect_request(
        self,
        client_reader: asyncio.StreamReader,
        client_writer: asyncio.StreamWriter,
    ) -> tuple[str, int]:
        """Parse the CONNECT request and return ``(host, port)``.

        Replies with the matching error code and raises ``_Socks5Error``
        on any unsupported field. Per RFC 1928 section 6 the reserved byte
        MUST be ``0x00``; non-zero is treated as a malformed request.
        """
        header = await self._readexactly_or_raise(client_reader, 4, "request header")
        version, cmd, reserved, atyp = header
        if version != SOCKS_VERSION:
            raise _Socks5Error(f"request used unexpected SOCKS version {version}")
        if reserved != RSV_REQUIRED:
            raise _Socks5Error(f"request reserved byte must be 0x00 (got {reserved:#04x})")
        if cmd != CMD_CONNECT:
            await self._reply_failure(client_writer, REP_COMMAND_NOT_SUPPORTED)
            raise _Socks5Error(f"unsupported SOCKS5 command {cmd}")
        try:
            host = await self._read_address(client_reader, client_writer, atyp)
        except UnicodeDecodeError as exc:
            raise _Socks5Error(f"domain bytes not ASCII: {exc!r}") from exc
        port_bytes = await self._readexactly_or_raise(client_reader, 2, "request port")
        port = struct.unpack("!H", port_bytes)[0]
        return host, port

    async def _read_address(
        self,
        client_reader: asyncio.StreamReader,
        client_writer: asyncio.StreamWriter,
        atyp: int,
    ) -> str:
        """Read the destination address according to ``atyp``."""
        if atyp == ATYP_IPV4:
            raw = await self._readexactly_or_raise(client_reader, 4, "IPv4 address")
            return socket.inet_ntop(socket.AF_INET, raw)
        if atyp == ATYP_IPV6:
            raw = await self._readexactly_or_raise(client_reader, 16, "IPv6 address")
            return socket.inet_ntop(socket.AF_INET6, raw)
        if atyp == ATYP_DOMAIN:
            length_bytes = await self._readexactly_or_raise(client_reader, 1, "domain length")
            length = length_bytes[0]
            domain_bytes = await self._readexactly_or_raise(client_reader, length, "domain bytes")
            return domain_bytes.decode("ascii", errors="strict")
        await self._reply_failure(client_writer, REP_ADDRTYPE_NOT_SUPPORTED)
        raise _Socks5Error(f"unsupported address type {atyp}")

    async def _open_upstream(
        self,
        host: str,
        port: int,
        client_writer: asyncio.StreamWriter,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """Open the outbound TCP socket bound to ``self.bind_addr``.

        Maps common errno values to SOCKS5 reply codes so the client
        sees a meaningful failure instead of a closed socket.
        """
        try:
            return await asyncio.wait_for(
                asyncio.open_connection(
                    host=host,
                    port=port,
                    local_addr=(self.bind_addr, 0),
                ),
                timeout=_CONNECT_TIMEOUT_S,
            )
        except TimeoutError as exc:
            await self._reply_failure(client_writer, REP_HOST_UNREACHABLE)
            raise _Socks5Error(f"upstream connect timed out: {host}:{port}") from exc
        except ConnectionRefusedError as exc:
            await self._reply_failure(client_writer, REP_CONNECTION_REFUSED)
            raise _Socks5Error(f"upstream refused: {host}:{port}") from exc
        except OSError as exc:
            reply_code = REP_GENERAL_FAILURE
            if exc.errno in {socket.EAI_NONAME, 101, 113}:
                reply_code = REP_NETWORK_UNREACHABLE
            await self._reply_failure(client_writer, reply_code)
            raise _Socks5Error(f"upstream connect failed for {host}:{port}: {exc}") from exc

    async def _reply_success(self, client_writer: asyncio.StreamWriter) -> None:
        """Send the SOCKS5 success reply with IPv4 ``0.0.0.0:0`` BIND placeholder.

        Per RFC 1928 section 6 the success reply MUST be well-formed; we use the
        canonical IPv4 zero placeholder for BND.ADDR/PORT since we never
        advertise a useful bind address back to the client.
        """
        client_writer.write(
            bytes(
                (
                    SOCKS_VERSION,
                    REP_SUCCEEDED,
                    RSV_REQUIRED,
                    ATYP_IPV4,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                )
            )
        )
        await client_writer.drain()

    async def _reply_failure(
        self,
        client_writer: asyncio.StreamWriter,
        reply_code: int,
    ) -> None:
        """Send a SOCKS5 failure reply with the IPv4 ``0.0.0.0:0`` placeholder.

        Per RFC 1928 section 6 every reply (success or failure) must be a
        well-formed BND.ADDR + BND.PORT tuple. Failure replies always
        use ATYP=IPv4 with zero address regardless of the request's
        ATYP — emitting the request ATYP would produce malformed
        packets for the domain case (4 zero bytes are not a valid
        length-prefixed domain).
        """
        client_writer.write(
            bytes(
                (
                    SOCKS_VERSION,
                    reply_code,
                    RSV_REQUIRED,
                    ATYP_IPV4,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                )
            )
        )
        with contextlib.suppress(OSError, ConnectionError):
            await client_writer.drain()

    async def _relay(
        self,
        client_reader: asyncio.StreamReader,
        client_writer: asyncio.StreamWriter,
        upstream_reader: asyncio.StreamReader,
        upstream_writer: asyncio.StreamWriter,
    ) -> None:
        """Run two pump coroutines until both halves close.

        Half-close semantics: when one direction finishes (e.g. client
        EOFs), the remote peer may still send a final response/close
        frame. We give the OTHER pump ``_HALF_CLOSE_DRAIN_S`` to drain
        before cancelling — preventing the relay from dropping
        in-flight Kraken WebSocket close frames.
        """
        client_to_upstream = asyncio.create_task(
            self._pump(client_reader, upstream_writer, half_close_target=upstream_writer)
        )
        upstream_to_client = asyncio.create_task(
            self._pump(upstream_reader, client_writer, half_close_target=client_writer)
        )
        done, pending = await asyncio.wait(
            {client_to_upstream, upstream_to_client},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for finished in done:
            exc = finished.exception()
            if exc is not None:
                logger.debug("socks5: pump finished with exception: {!r}", exc)
        if not pending:
            return
        still_pending = (await asyncio.wait(pending, timeout=_HALF_CLOSE_DRAIN_S))[1]
        if still_pending:
            logger.debug(
                "socks5: half-close drain timeout — cancelling {} pump(s)",
                len(still_pending),
            )
            for task in still_pending:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    async def _pump(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        half_close_target: asyncio.StreamWriter,
    ) -> None:
        """One-way copy until EOF; then half-close the target writer.

        Half-closing the target via ``write_eof`` lets the OTHER pump
        see the EOF on its reader and finish draining cleanly. If the
        transport doesn't support ``write_eof`` (e.g. SSL streams), the
        method falls back silently.
        """
        try:
            while True:
                data = await reader.read(_RELAY_BUFFER_SIZE)
                if not data:
                    break
                writer.write(data)
                await writer.drain()
        except OSError:
            return
        with contextlib.suppress(OSError, AttributeError, NotImplementedError):
            if half_close_target.can_write_eof():
                half_close_target.write_eof()

    @staticmethod
    async def _readexactly_or_raise(
        reader: asyncio.StreamReader,
        nbytes: int,
        what: str,
    ) -> bytes:
        """``reader.readexactly`` with friendlier error wrapping."""
        try:
            return await reader.readexactly(nbytes)
        except asyncio.IncompleteReadError as exc:
            raise _Socks5Error(f"short read on {what}: {exc!r}") from exc


class _Socks5Error(Exception):
    """Internal protocol-level error — caught at the conversation boundary."""
