"""Owned-private receive privacy and immutable physical source context.

This adapter grants no authenticated ownership or financial permission. It
composes the existing physical-epoch wrapper and delegates public traffic
unchanged. Only a registered actual private SDK object selects privacy filtering.
"""

import asyncio
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Mapping
from contextvars import ContextVar
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from functools import wraps
from typing import Any
from typing import Literal
from typing import Never
from typing import SupportsIndex
from typing import cast
from uuid import UUID
from weakref import ReferenceType
from weakref import WeakKeyDictionary
from weakref import ref

import kraken.futures.websocket as futures_websocket
from kraken.futures import FuturesWSClient
from kraken.futures.websocket import ConnectFuturesWebsocket
from kraken.utils.utils import WSState

from snapper.infrastructure.exchanges.execution_wire_receipts import PrivateWireMessage
from snapper.infrastructure.exchanges.execution_wire_receipts import (
    install_private_execution_wire_capture,
)
from snapper.infrastructure.exchanges.kraken_futures_balance_epoch import _CURRENT_EPOCH
from snapper.infrastructure.exchanges.kraken_futures_balance_epoch import _EpochJsonCodec
from snapper.infrastructure.exchanges.kraken_futures_balance_epoch import _running_task_identity
from snapper.infrastructure.exchanges.kraken_futures_balance_epoch import (
    install_futures_balance_epoch_capture,
)
from snapper.infrastructure.exchanges.private_control_decoder import TransientPrivateControl
from snapper.infrastructure.exchanges.private_control_decoder import decode_owned_private_message

_RUN = "_ConnectFuturesWebsocket__run"
_INSTALL_ATTRIBUTE = "_snapper_owned_private_receive_capture"
_METADATA_ATTRIBUTE = "_snapper_owned_private_receive_metadata"
_CALLBACK_BINDING_ATTRIBUTE = "_snapper_owned_private_callback_binding"
_SDK_CALLBACK_ATTRIBUTE = "_FuturesWSClient__callback"
_OWNED: WeakKeyDictionary[ConnectFuturesWebsocket, ReferenceType[FuturesWSClient]] = (
    WeakKeyDictionary()
)


@dataclass(slots=True)
class _InstalledCodec:
    """One actual composable namespace delegate, containing no client or credential material."""

    delegate: _EpochJsonCodec | None = None


_INSTALLED = _InstalledCodec()


class _ReceiverObjects:
    """Opaque immutable runtime pointers that generic serializers must not traverse into SDK secrets."""

    __slots__ = ("client", "connector", "socket")
    client: FuturesWSClient
    connector: ConnectFuturesWebsocket
    socket: object

    def __init__(
        self, client: FuturesWSClient, connector: ConnectFuturesWebsocket, socket: object
    ) -> None:
        """Retain actual runtime pointers without copying any SDK credential value."""
        object.__setattr__(self, "client", client)
        object.__setattr__(self, "connector", connector)
        object.__setattr__(self, "socket", socket)

    def __setattr__(self, name: str, value: object) -> Never:
        """Refuse replacement of the actual original SDK pointers."""
        raise AttributeError("Private receiver object binding is immutable")

    def __reduce_ex__(self, protocol: SupportsIndex) -> Never:
        """Refuse serialization before a generic pickler can traverse an SDK object."""
        raise TypeError("Private receiver object binding cannot be serialized")

    def __deepcopy__(self, memo: dict[int, object]) -> Never:
        """Refuse generic dataclass conversion before copying credential-bearing SDK fields."""
        raise TypeError("Private receiver object binding cannot be copied")


@dataclass(frozen=True, slots=True)
class PrivateReceiveMetadata:
    """Original non-authoritative physical context, without any key or authentication-success claim."""

    _objects: _ReceiverObjects = field(repr=False)
    physical_epoch_public_id: str
    observed_at: datetime
    source_policy: Literal["kraken_futures_owned_private_decode_v1"] = (
        "kraken_futures_owned_private_decode_v1"
    )

    @property
    def client(self) -> FuturesWSClient:
        """Read the original actual SDK object for an internal closed-boundary comparison.

        Returns:
            Original runtime SDK object, never a serializable account witness.
        """
        return self._objects.client

    @property
    def connector(self) -> ConnectFuturesWebsocket:
        """Read the original actual physical connector for an internal identity comparison.

        Returns:
            Original runtime connector, never persisted as account evidence.
        """
        return self._objects.connector

    @property
    def socket(self) -> object:
        """Read the original actual socket for an internal source-ownership comparison.

        Returns:
            Original runtime socket, without claiming authenticated ownership.
        """
        return self._objects.socket

    def __reduce_ex__(self, protocol: SupportsIndex) -> Never:
        """Refuse generic serialization of actual runtime object pointers."""
        raise TypeError("Private receive metadata cannot be serialized")

    def __deepcopy__(self, memo: dict[int, object]) -> Never:
        """Refuse deep copying actual runtime object pointers."""
        raise TypeError("Private receive metadata cannot be copied")


@dataclass(frozen=True, slots=True)
class _PrivateRun:
    """An actual registered connector task, including its originally registered SDK owner."""

    client: FuturesWSClient
    connector: ConnectFuturesWebsocket
    task_identity: int


_CURRENT_PRIVATE_RUN: ContextVar[_PrivateRun | None] = ContextVar(
    "snapper_owned_futures_private_receive", default=None
)
_CALLBACK_OWNER: ContextVar[tuple[FuturesWSClient, int] | None] = ContextVar(
    "snapper_owned_futures_private_callback", default=None
)


def _delegate() -> _EpochJsonCodec:
    """Require the actual captured epoch decoder instead of silently selecting another boundary."""
    if _INSTALLED.delegate is None:
        raise RuntimeError("Owned Futures private decoder was not installed")
    return _INSTALLED.delegate


def _current_scope() -> _PrivateRun | None:
    """Reject inherited receive context in unrelated tasks or calls outside a running event loop."""
    scope = _CURRENT_PRIVATE_RUN.get()
    return scope if scope is not None and scope.task_identity == _running_task_identity() else None


def _metadata(scope: _PrivateRun, observed_at: datetime) -> PrivateReceiveMetadata | None:
    """Copy original physical context only when its actual SDK owner and epoch remain bound."""
    boundary = _CURRENT_EPOCH.get()
    socket = getattr(scope.connector, "socket", None)
    if (
        boundary is None
        or boundary[1] != scope.task_identity
        or socket is None
        or getattr(scope.connector, "_ConnectFuturesWebsocket__client", None) is not scope.client
    ):
        return None
    return PrivateReceiveMetadata(
        _ReceiverObjects(scope.client, scope.connector, socket), boundary[0], observed_at
    )


def _attach_metadata(result: object, metadata: PrivateReceiveMetadata | None) -> object:
    """Keep local immutable context separate from provider JSON and exact original receipt bytes."""
    if metadata is None:
        return result
    if isinstance(result, TransientPrivateControl):
        return TransientPrivateControl(dict(result), result.failure_reason, metadata)
    if isinstance(result, PrivateWireMessage):
        if result.__dict__.get("_snapper_numeric_decode_unavailable") is not True:
            result.__dict__[_METADATA_ATTRIBUTE] = metadata
    return result


class _OwnedPrivateJsonCodec(_EpochJsonCodec):
    """Compose the validated epoch codec while selecting privacy only in an owned private run."""

    @staticmethod
    def loads(data: str | bytes | bytearray, **kwargs: Any) -> object:
        """Preserve public JSON behavior and sanitize source only in the actual owned private task.

        Args:
            data: Original SDK socket payload.
            **kwargs: External JSON decoder options, unsupported inside the pinned private call path.

        Returns:
            Existing public decode result or private financial/control result with local context.
        """
        delegate = _delegate()
        if _CURRENT_PRIVATE_RUN.get() is None:
            return delegate.loads(data, **kwargs)
        observed_at = datetime.now(UTC)
        result = (
            TransientPrivateControl(
                {"event": "error", "message": "private_wire_decode_refused"},
                "private_decoder_options_unsupported",
            )
            if kwargs
            else decode_owned_private_message(data, delegate)
        )
        scope = _current_scope()
        return _attach_metadata(
            result, _metadata(scope, observed_at) if scope is not None else None
        )

    @staticmethod
    def dumps(value: object, **kwargs: Any) -> str:
        """Preserve actual outbound SDK encoding without authentication capture or permission.

        Args:
            value: Actual SDK outbound value.
            **kwargs: Original external JSON encoder options.

        Returns:
            The existing delegate's unchanged encoded payload.
        """
        return _delegate().dumps(value, **kwargs)


def register_futures_private_receiver(client: FuturesWSClient) -> None:
    """Register only a real SDK connector actually constructed with this private client object.

    Args:
        client: Actual private SDK instance whose receive path needs authentication privacy.
    """
    connector = getattr(client, "_conn", None)
    if (
        not isinstance(connector, ConnectFuturesWebsocket)
        or getattr(connector, "_ConnectFuturesWebsocket__client", None) is not client
    ):
        raise RuntimeError("Owned Futures private connector identity is unproven")
    previous = _OWNED.get(connector)
    if previous is not None and previous() is not client:
        raise RuntimeError("Owned Futures private connector identity changed")
    _OWNED[connector] = ref(client)


def install_owned_futures_private_receive_capture() -> None:
    """Compose actual pinned receive and JSON boundaries without changing public decode semantics.

    Raises:
        RuntimeError: If an unsupported SDK or independently replaced capture boundary is present.
    """
    current = getattr(ConnectFuturesWebsocket, _RUN, None)
    namespace = futures_websocket.__dict__.get("json")
    if getattr(current, _INSTALL_ATTRIBUTE, False):
        if not isinstance(namespace, _OwnedPrivateJsonCodec) or _INSTALLED.delegate is None:
            raise RuntimeError("Owned Futures private decoder was replaced")
        return
    if isinstance(namespace, _OwnedPrivateJsonCodec):
        raise RuntimeError("Owned Futures private receive wrapper was replaced")
    install_private_execution_wire_capture()
    install_futures_balance_epoch_capture()
    original = getattr(ConnectFuturesWebsocket, _RUN, None)
    codec = futures_websocket.__dict__.get("json")
    if not callable(original) or not isinstance(codec, _EpochJsonCodec):
        raise RuntimeError("Owned Futures private receive boundary is unsupported")
    run = cast(Callable[[ConnectFuturesWebsocket, asyncio.Event], Awaitable[None]], original)
    _INSTALLED.delegate = codec

    @wraps(run)
    async def owned_run(connector: ConnectFuturesWebsocket, event: asyncio.Event) -> None:
        """Reset private context for every actual run, including public nested runs and failures."""
        registered = _OWNED.get(connector)
        client = registered() if registered is not None else None
        if registered is not None and client is None:
            raise RuntimeError("Owned Futures private original receiver is unavailable")
        task_identity = _running_task_identity()
        if client is not None and task_identity is None:
            raise RuntimeError("Owned Futures private receive task is unavailable")
        scope = (
            _PrivateRun(client, connector, task_identity)
            if client is not None and task_identity is not None
            else None
        )
        token = _CURRENT_PRIVATE_RUN.set(scope)
        try:
            await run(connector, event)
        finally:
            _CURRENT_PRIVATE_RUN.reset(token)

    setattr(owned_run, _INSTALL_ATTRIBUTE, True)
    setattr(ConnectFuturesWebsocket, _RUN, owned_run)
    futures_websocket.__dict__["json"] = _OwnedPrivateJsonCodec()


def private_receive_metadata(message: Mapping[str, object]) -> PrivateReceiveMetadata | None:
    """Read original local context without accepting similarly named provider JSON fields.

    Args:
        message: Actual financial boundary or repr-safe transient control mapping.

    Returns:
        Immutable physical context only; it proves neither authentication nor current ownership.
    """
    value = (
        message.receive_metadata
        if isinstance(message, TransientPrivateControl)
        else (
            message.__dict__.get(_METADATA_ATTRIBUTE)
            if isinstance(message, PrivateWireMessage)
            else None
        )
    )
    if not isinstance(value, PrivateReceiveMetadata) or value.observed_at.utcoffset() is None:
        return None
    try:
        return (
            value
            if str(UUID(value.physical_epoch_public_id)) == value.physical_epoch_public_id
            else None
        )
    except ValueError:
        return None


def private_callback_owner() -> FuturesWSClient | None:
    """Read only the actual current owned callback task rather than an inherited task context.

    Returns:
        The original actual SDK object, without proving selected key or account ownership.
    """
    owner = _CALLBACK_OWNER.get()
    return owner[0] if owner is not None and owner[1] == _running_task_identity() else None


@dataclass(frozen=True, slots=True)
class _CallbackBinding:
    """One immutable private callback replacement whose lifecycle remains owned by its SDK."""

    original: Callable[[Mapping[str, Any]], Awaitable[None]] = field(repr=False)
    wrapper: Callable[[Mapping[str, Any]], Awaitable[None]] = field(repr=False)


def bind_owned_futures_private_callback(
    client: FuturesWSClient, callback: Callable[[Mapping[str, Any]], Awaitable[None]]
) -> None:
    """Register an existing actual private SDK before start and bind its original object through awaits.

    Args:
        client: Newly constructed actual private SDK whose caller owns failure cleanup.
        callback: Original private source handler, accepting the external SDK mapping boundary.
    """
    install_owned_futures_private_receive_capture()
    register_futures_private_receiver(client)
    previous = getattr(client, _CALLBACK_BINDING_ATTRIBUTE, None)
    actual = getattr(client, _SDK_CALLBACK_ATTRIBUTE, None)
    if previous is not None:
        if (
            not isinstance(previous, _CallbackBinding)
            or previous.original != callback
            or actual is not previous.wrapper
        ):
            raise RuntimeError("Owned Futures private callback identity changed")
        return
    connector = client._conn
    if connector.state != WSState.INIT or connector.socket is not None:
        raise RuntimeError("Owned Futures private callback must bind before start")
    if actual != callback:
        raise RuntimeError("Owned Futures private original callback is unproven")

    async def owned_callback(message: Mapping[str, Any]) -> None:
        """Bind this original SDK object through awaits and clear it on failure or cancellation."""
        task_identity = _running_task_identity()
        if task_identity is None:
            raise RuntimeError("Owned Futures private callback context is unavailable")
        token = _CALLBACK_OWNER.set((client, task_identity))
        try:
            await callback(message)
        finally:
            _CALLBACK_OWNER.reset(token)

    binding = _CallbackBinding(callback, owned_callback)
    setattr(client, _SDK_CALLBACK_ATTRIBUTE, owned_callback)
    setattr(client, _CALLBACK_BINDING_ATTRIBUTE, binding)
