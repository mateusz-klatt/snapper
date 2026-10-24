"""Unapplied pinned-SDK prepared-key, actual socket and one-shot receive integration.

This adapter composes the existing owned privacy/epoch decoder and egress connect
wrapper. Only an actual registered private SDK's physical context can authorize
an issuance handle. Public context or ticket construction is not source proof.
"""

import asyncio
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Mapping
from contextlib import AbstractAsyncContextManager
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from functools import wraps
from typing import Any
from typing import cast
from uuid import uuid7
from weakref import ReferenceType
from weakref import WeakKeyDictionary
from weakref import WeakSet
from weakref import ref

import kraken.futures.websocket as futures_websocket
from kraken.futures import FuturesWSClient
from kraken.futures.websocket import ConnectFuturesWebsocket
from kraken.utils.utils import WSState

import snapper.infrastructure.exchanges.kraken_futures_private_receive as receive
from snapper.core.private_frame_ticket import AuthenticatedPrivateFrameTicket
from snapper.core.private_frame_ticket import _open_actual_private_frame_issuer
from snapper.core.private_frame_ticket import require_authenticated_private_frame
from snapper.infrastructure.exchanges.authenticated_private_shutdown import OwnedPrivateShutdown
from snapper.infrastructure.exchanges.authenticated_private_shutdown import (
    PrivateShutdownUnresolvedError,
)
from snapper.infrastructure.exchanges.execution_wire_receipts import PrivateWireMessage
from snapper.infrastructure.exchanges.kraken_futures_balance_epoch import _CURRENT_EPOCH
from snapper.infrastructure.exchanges.kraken_futures_balance_epoch import (
    active_futures_connection_epoch,
)
from snapper.infrastructure.exchanges.kraken_sdk_patches import force_close_ws_client
from snapper.infrastructure.exchanges.owned_private_socket import ActualReceiveCell
from snapper.infrastructure.exchanges.owned_private_socket import OriginalReceive
from snapper.infrastructure.exchanges.owned_private_socket import OwnedPrivateSocket
from snapper.infrastructure.exchanges.owned_private_socket import PrivatePhysicalSocket
from snapper.infrastructure.exchanges.prepared_private_key import PreparedKeyToken
from snapper.infrastructure.exchanges.private_control_decoder import TransientPrivateControl
from snapper.infrastructure.exchanges.private_socket_authentication import (
    PreparedPrivateRuntimeBinding,
)
from snapper.infrastructure.exchanges.private_socket_authentication import (
    PrivateAuthenticationError,
)
from snapper.infrastructure.exchanges.private_socket_authentication import PrivateRuntimeSelection
from snapper.infrastructure.exchanges.private_socket_authentication import (
    PrivateSocketAuthentication,
)
from snapper.infrastructure.exchanges.private_socket_authentication import _OpaqueState

_BINDING_ATTRIBUTE = "_snapper_prepared_private_transport_binding"
_TICKET_ATTRIBUTE = "_snapper_authenticated_private_frame_ticket"
_CONNECT_MARKER = "_snapper_authenticated_private_connect"
_ACTUAL_CONNECT_SEAL = object()
_SDK_CLOSE_METHOD = "close"
_WS_ORIGINS = {
    "https://futures.kraken.com": "wss://futures.kraken.com/ws/v1",
    "https://demo-futures.kraken.com": "wss://demo-futures.kraken.com/ws/v1",
}


class _AuthenticatedClientBinding(_OpaqueState):
    """One original actual private SDK, token and current-parent ownership provider."""

    __slots__ = (
        "client_capture_public_id",
        "runtime",
        "_sockets",
        "_token",
        "_provider",
        "_receive_tasks",
        "_closing",
        "_shutdown",
        "__weakref__",
    )

    def __init__(
        self,
        client: FuturesWSClient,
        token: PreparedKeyToken,
        provider: Callable[[], PrivateRuntimeSelection],
    ) -> None:
        """Bind only after the actual transport-issued token references an awaited committed proof."""
        self.client_capture_public_id = str(uuid7())
        self.runtime = PreparedPrivateRuntimeBinding(client, token, provider)
        self._sockets: WeakSet[PrivateSocketAuthentication] = WeakSet()
        self._token = token
        self._provider = provider
        self._receive_tasks: set[asyncio.Task[None]] = set()
        self._closing = False
        self._shutdown: OwnedPrivateShutdown | None = None
        self.runtime.current_proof(datetime.now(UTC))

    def __repr__(self) -> str:
        """Reveal no SDK, key token or callback closure."""
        return "<AuthenticatedPrivateClientBinding:opaque>"

    def matches(
        self, token: PreparedKeyToken, provider: Callable[[], PrivateRuntimeSelection]
    ) -> bool:
        """Allow only an exact same-object idempotent registration before startup."""
        return self._token is token and self._provider == provider

    def _open_socket(
        self, epoch: str, cell: ActualReceiveCell, actual_connect_seal: object
    ) -> PrivateSocketAuthentication:
        """Create one new empty authentication state after actual selected socket proof checks."""
        if actual_connect_seal is not _ACTUAL_CONNECT_SEAL or self._closing:
            raise PrivateAuthenticationError("private_socket_actual_connect_not_observed")
        task = asyncio.current_task()
        if task is None:
            raise PrivateAuthenticationError("private_socket_actual_receive_task_missing")
        actual_task = cast(asyncio.Task[None], task)
        self._receive_tasks.add(actual_task)
        actual_task.add_done_callback(self._receive_finished)
        state = PrivateSocketAuthentication(self.runtime, epoch, self.client_capture_public_id)
        state._bind_actual_socket_issuer(_open_actual_private_frame_issuer(), cell)
        self._sockets.add(state)
        return state

    def _receive_finished(self, task: asyncio.Task[None]) -> None:
        """Retrieve a completed actual receive exception without retaining or logging provider traceback."""
        self._receive_tasks.discard(task)
        if not task.cancelled():
            task.exception()

    def _receive_tasks_snapshot(self) -> tuple[asyncio.Task[None], ...]:
        """Keep actual still-running receives distinct from a successfully joined SDK supervisor."""
        return tuple(self._receive_tasks)

    def _quiesce(self) -> None:
        """Revoke future physical auth/frame issuance without closing the still-owned original token."""
        self._closing = True
        for state in tuple(self._sockets):
            state.close()

    def _release_joined_owner(self) -> None:
        """Drop transient ownership only after all actual original receive tasks have terminated."""
        self._sockets.clear()
        self._token.close()

    def install_shutdown(
        self, client: FuturesWSClient, original_close: Callable[[], Awaitable[None]]
    ) -> None:
        """Retain one actual SDK cleanup and a strongly supervised final receive join."""
        self._shutdown = OwnedPrivateShutdown(
            original_close,
            lambda: force_close_ws_client(client),
            self._receive_tasks_snapshot,
            self._quiesce,
            self._release_joined_owner,
        )

    async def close(self) -> None:
        """Await actual supervised close while cancellation preserves explicit unresolved ownership."""
        origin = receive._CURRENT_PRIVATE_RUN.get()
        if origin is not None and any(
            id(task) == origin.task_identity for task in self._receive_tasks
        ):
            raise PrivateShutdownUnresolvedError("private_socket_close_from_receive_task")
        if self._shutdown is None:
            raise PrivateAuthenticationError("private_socket_shutdown_not_bound")
        await self._shutdown.close()

    @property
    def close_resolved(self) -> bool:
        """Expose real receiver/token release rather than successful entry into a close helper.

        Returns:
            Whether the original shutdown joined its actual receives and released its owner.
        """
        return self._shutdown is not None and self._shutdown.resolved


_AUTHENTICATED_CLIENTS: WeakKeyDictionary[
    ConnectFuturesWebsocket, ReferenceType[_AuthenticatedClientBinding]
] = WeakKeyDictionary()


@dataclass(slots=True)
class _InstalledBridge:
    """Only module-local protocol delegates, without credential or private SDK material."""

    decoder: receive._OwnedPrivateJsonCodec | None = None
    connect: Callable[..., AbstractAsyncContextManager[PrivatePhysicalSocket]] | None = None


_INSTALLED = _InstalledBridge()


class _AuthenticatedSocket(OwnedPrivateSocket):
    """One actual original physical socket and its actual one-shot/authentication state."""

    __slots__ = ("_authentication", "_receive_cell", "_original_scope", "_epoch")

    def __init__(
        self,
        socket: PrivatePhysicalSocket,
        cell: ActualReceiveCell,
        authentication: PrivateSocketAuthentication,
        scope: receive._PrivateRun,
    ) -> None:
        """Retain original pointers only in an opaque actual-socket object."""
        self._authentication = authentication
        self._receive_cell = cell
        self._original_scope = scope
        boundary = _CURRENT_EPOCH.get()
        if boundary is None or boundary[1] != scope.task_identity:
            raise PrivateAuthenticationError("private_socket_actual_epoch_missing")
        self._epoch = boundary[0]
        super().__init__(socket, cell, lambda: str(uuid7()), self._guard_authentication_send)

    def _is_active_original(self) -> bool:
        """Compare actual socket/connector/client and active epoch, not a constructor flag."""
        scope = self._original_scope
        return (
            scope.connector.socket is self
            and scope.client._conn is scope.connector
            and getattr(scope.connector, "_ConnectFuturesWebsocket__client", None) is scope.client
            and active_futures_connection_epoch(scope.client) == self._epoch
        )

    def _guard_authentication_send(self, source: str, encoded_at: datetime) -> None:
        """Guard actual encoded auth after challenge waits and before the real physical send."""
        if not self._is_active_original():
            raise PrivateAuthenticationError("private_socket_actual_dispatch_scope_changed")
        self._authentication.before_send(source, encoded_at)

    def take_original(self, source: object) -> OriginalReceive | None:
        """Consume only in the exact actual SDK run's decoder after the receive child completed."""
        scope = receive._current_scope()
        if (
            scope is None
            or scope.client is not self._original_scope.client
            or scope.connector is not self._original_scope.connector
            or scope.task_identity != self._original_scope.task_identity
            or not self._is_active_original()
        ):
            return None
        return self._receive_cell.consume(source)

    def observe_decoded(self, result: object, received: OriginalReceive) -> object:
        """Project ACKs privately or attach only an actual registered financial receive ticket."""
        if isinstance(result, TransientPrivateControl):
            self._authentication.observe_control(result, received)
        elif isinstance(result, PrivateWireMessage):
            ticket = self._authentication.financial_ticket(result, received)
            if ticket is not None:
                result.__dict__[_TICKET_ATTRIBUTE] = ticket
        return result

    async def close(self) -> None:
        """Invalidate current authentication immediately before awaiting the actual socket close."""
        self._authentication.close()
        await super().close()

    def end_context(self) -> None:
        """Invalidate current state when the real context exits, including SDK run cancellation."""
        self._authentication.close()
        self._receive_cell.close()


class _AuthenticatedJsonCodec(receive._OwnedPrivateJsonCodec):
    """Compose exact public/private privacy decoding with an actual consumed receive capability."""

    @staticmethod
    def loads(data: str | bytes | bytearray, **kwargs: Any) -> object:
        """Delegate unchanged source/options, then use only this actual socket's one-shot receipt."""
        delegate = _INSTALLED.decoder
        if delegate is None:
            raise PrivateAuthenticationError("private_socket_decoder_delegate_missing")
        scope = receive._current_scope()
        socket = getattr(scope.connector, "socket", None) if scope is not None else None
        original = socket.take_original(data) if isinstance(socket, _AuthenticatedSocket) else None
        result = delegate.loads(data, **kwargs)
        if original is not None and isinstance(socket, _AuthenticatedSocket):
            return socket.observe_decoded(result, original)
        return result

    @staticmethod
    def dumps(value: object, **kwargs: Any) -> str:
        """Preserve the original SDK encoding; actual socket.send inspects the final encoded source."""
        delegate = _INSTALLED.decoder
        if delegate is None:
            raise PrivateAuthenticationError("private_socket_encoder_delegate_missing")
        return delegate.dumps(value, **kwargs)


def _actual_binding(scope: receive._PrivateRun | None) -> _AuthenticatedClientBinding | None:
    """Use only the original actual owned SDK; a registered lost binding cannot silently downgrade."""
    if scope is None:
        return None
    registered = _AUTHENTICATED_CLIENTS.get(scope.connector)
    if registered is None:
        return None
    binding = registered()
    if binding is None:
        raise PrivateAuthenticationError("private_socket_original_binding_unavailable")
    return binding


def _verify_handshake(
    socket: PrivatePhysicalSocket, requested: tuple[object, ...], expected_url: str
) -> None:
    """Require actual requested route plus actual handshake path and Host, with no inferred aliases."""
    try:
        request = getattr(socket, "request", None)
        headers = getattr(request, "headers", None)
        if (
            not requested
            or requested[0] != expected_url
            or getattr(request, "path", None) != "/ws/v1"
            or not isinstance(headers, Mapping)
            or headers.get("Host") != expected_url.removeprefix("wss://").removesuffix("/ws/v1")
        ):
            raise PrivateAuthenticationError("private_socket_actual_handshake_route_unproven")
    except Exception:
        raise PrivateAuthenticationError("private_socket_actual_handshake_route_unproven") from None


@asynccontextmanager
async def _owned_connect(*args: object, **kwargs: object) -> AsyncIterator[PrivatePhysicalSocket]:
    """Wrap only actual registered private connects, retaining the existing egress/TLS implementation."""
    delegate = _INSTALLED.connect
    if delegate is None:
        raise PrivateAuthenticationError("private_socket_connect_delegate_missing")
    scope = receive._current_scope()
    binding = _actual_binding(scope)
    async with delegate(*args, **kwargs) as socket:
        if binding is None or scope is None:
            yield socket
            return
        proof = binding.runtime.current_proof(datetime.now(UTC))
        expected = _WS_ORIGINS.get(proof.client_selection.public_api_origin or "")
        boundary = _CURRENT_EPOCH.get()
        if expected is None or boundary is None or boundary[1] != scope.task_identity:
            raise PrivateAuthenticationError("private_socket_actual_connect_scope_unproven")
        _verify_handshake(socket, args, expected)
        cell = ActualReceiveCell(scope.task_identity)
        state = binding._open_socket(boundary[0], cell, _ACTUAL_CONNECT_SEAL)
        proxy = _AuthenticatedSocket(socket, cell, state, scope)
        try:
            yield proxy
        finally:
            proxy.end_context()


def install_authenticated_private_socket_bridge() -> None:
    """Compose only the pinned owned-private JSON/connect boundaries; public decoding remains original."""
    receive.install_owned_futures_private_receive_capture()
    decoder = futures_websocket.__dict__.get("json")
    connect = futures_websocket.__dict__.get("connect")
    if getattr(connect, _CONNECT_MARKER, False):
        if not isinstance(decoder, _AuthenticatedJsonCodec) or _INSTALLED.decoder is None:
            raise PrivateAuthenticationError("private_socket_installed_boundary_changed")
        return
    if (
        isinstance(decoder, _AuthenticatedJsonCodec)
        or not isinstance(decoder, receive._OwnedPrivateJsonCodec)
        or not callable(connect)
    ):
        raise PrivateAuthenticationError("private_socket_install_boundary_unsupported")
    _INSTALLED.decoder = decoder
    _INSTALLED.connect = cast(
        Callable[..., AbstractAsyncContextManager[PrivatePhysicalSocket]], connect
    )
    installed = wraps(connect)(_owned_connect)
    setattr(installed, _CONNECT_MARKER, True)
    futures_websocket.__dict__["connect"] = installed
    futures_websocket.__dict__["json"] = _AuthenticatedJsonCodec()


def bind_authenticated_private_client(
    client: FuturesWSClient,
    token: PreparedKeyToken,
    current: Callable[[], PrivateRuntimeSelection],
) -> _AuthenticatedClientBinding:
    """Bind the actual original SDK before start, requiring an actual transport-issued committed token.

    Args:
        client: Actual original fresh private SDK instance before any start or subscription.
        token: Actual transport-issued key token bound after its original safe proof committed.
        current: Provider reading actual current parent User/private SDK/public selected revision.

    Returns:
        Opaque original binding whose socket/ACK checks still precede any frame ownership claim.
    """
    connector = client._conn
    if connector.state != WSState.INIT or connector.socket is not None:
        raise PrivateAuthenticationError("private_socket_binding_requires_fresh_client")
    previous = getattr(client, _BINDING_ATTRIBUTE, None)
    if previous is not None:
        if not isinstance(previous, _AuthenticatedClientBinding) or not previous.matches(
            token, current
        ):
            raise PrivateAuthenticationError("private_socket_existing_binding_changed")
        return previous
    install_authenticated_private_socket_bridge()
    binding = _AuthenticatedClientBinding(client, token, current)
    binding.install_shutdown(client, client.close)

    async def owned_close() -> None:
        """Keep the SDK instance hook explicit while preserving the supervised binding's await contract."""
        await binding.close()

    setattr(client, _SDK_CLOSE_METHOD, owned_close)
    setattr(client, _BINDING_ATTRIBUTE, binding)
    _AUTHENTICATED_CLIENTS[connector] = ref(binding)
    return binding


def authenticated_private_frame_ticket(
    message: Mapping[str, object],
) -> AuthenticatedPrivateFrameTicket | None:
    """Read only an actually issued matching original ticket, not a caller/provider-shaped object.

    Args:
        message: Actual financial mapping returned by the owned decoder, not reconstructed provider JSON.

    Returns:
        The actually issued matching original receive ticket, or none for unproven material.
    """
    if not isinstance(message, PrivateWireMessage):
        return None
    candidate = message.__dict__.get(_TICKET_ATTRIBUTE)
    try:
        require_authenticated_private_frame(candidate, message.wire_bytes)
    except ValueError:
        return None
    return candidate if isinstance(candidate, AuthenticatedPrivateFrameTicket) else None


def authenticated_private_close_resolved(client: FuturesWSClient) -> bool:
    """Distinguish a generic closed client from an owned close whose actual receives remain unresolved.

    Args:
        client: Actual SDK object whose parent wants to compare and clear its occupied private slot.

    Returns:
        True for generic clients or actual joined owned cleanup; false while its owner remains unresolved.
    """
    binding = getattr(client, _BINDING_ATTRIBUTE, None)
    return not isinstance(binding, _AuthenticatedClientBinding) or binding.close_resolved
