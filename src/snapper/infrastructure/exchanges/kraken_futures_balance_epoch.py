"""Bind genuine Futures payloads to one physical SDK receive-loop invocation.

The pinned SDK replaces physical sockets inside a long-lived client object.
Construction identity therefore cannot prove one source sequence epoch. This
optional module is installed only by the native-balance integration; the local
context belongs to each actual SDK run task and is copied onto genuine boundary
messages before the callback can block or another socket can reconnect.
"""

import asyncio
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Mapping
from contextvars import ContextVar
from typing import Any
from typing import cast
from uuid import UUID
from uuid import uuid7
from weakref import WeakKeyDictionary

import kraken.futures.websocket as futures_websocket
from kraken.futures.websocket import ConnectFuturesWebsocket
from kraken.utils.utils import WSState
from websockets.protocol import State

from snapper.infrastructure.exchanges.execution_wire_receipts import PrivateExecutionJsonCodec
from snapper.infrastructure.exchanges.execution_wire_receipts import PrivateWireMessage

_CURRENT_EPOCH: ContextVar[tuple[str, int] | None] = ContextVar(
    "kraken_futures_physical_epoch", default=None
)
_EPOCH_ATTRIBUTE = "_snapper_futures_physical_epoch"
_RUN_ATTRIBUTE = "_ConnectFuturesWebsocket__run"
_INSTALL_ATTRIBUTE = "_snapper_native_balance_epoch_capture"
_ACTIVE_EPOCHS: WeakKeyDictionary[ConnectFuturesWebsocket, str] = WeakKeyDictionary()


def _running_task_identity() -> int | None:
    """Refuse inherited receive context in worker threads or plain event-loop callbacks."""
    try:
        task = asyncio.current_task()
    except RuntimeError:
        return None
    return id(task) if task is not None else None


class _EpochJsonCodec(PrivateExecutionJsonCodec):
    """Copy the active physical receive-loop identity beside genuine retained bytes."""

    @staticmethod
    def loads(data: str | bytes | bytearray, **kwargs: Any) -> object:
        """Decode through the existing exact wire boundary and retain only a real task epoch.

        Args:
            data: The pinned SDK's received JSON payload.
            **kwargs: Original SDK JSON wrapper options at this external boundary.

        Returns:
            The original codec result, optionally carrying a local transport epoch.
        """
        result = PrivateExecutionJsonCodec.loads(data, **kwargs)
        boundary = _CURRENT_EPOCH.get()
        if (
            isinstance(result, PrivateWireMessage)
            and boundary is not None
            and _running_task_identity() == boundary[1]
        ):
            result.__dict__[_EPOCH_ATTRIBUTE] = boundary[0]
        return result


def private_futures_message_epoch(message: Mapping[str, object]) -> str | None:
    """Read a genuine receive-loop epoch without trusting similarly named provider fields.

    Args:
        message: Actual SDK callback object, possibly a weaker reconstructed mapping.

    Returns:
        Canonical physical source epoch, absent when its boundary provenance is missing.
    """
    if not isinstance(message, PrivateWireMessage):
        return None
    value = message.__dict__.get(_EPOCH_ATTRIBUTE)
    if not isinstance(value, str):
        return None
    try:
        canonical = str(UUID(value))
    except ValueError:
        return None
    return canonical if canonical == value else None


def active_futures_connection_epoch(client: object) -> str | None:
    """Read only a currently open physical receive loop belonging to the actual SDK client.

    Args:
        client: Actual selected private Futures SDK object, possibly absent or already closed.

    Returns:
        Its active physical epoch, absent before connection or after socket/lifecycle closure.
    """
    connector = getattr(client, "_conn", None)
    if not isinstance(connector, ConnectFuturesWebsocket):
        return None
    if (
        not connector.keep_alive
        or connector.state != WSState.CONNECTED
        or getattr(getattr(connector, "socket", None), "state", None) != State.OPEN
    ):
        return None
    return _ACTIVE_EPOCHS.get(connector)


def install_futures_balance_epoch_capture() -> None:
    """Instrument each physical run while preserving the existing SDK reconnect implementation.

    Raises:
        RuntimeError: If another installed JSON boundary or run method is unsupported.
    """
    codec = futures_websocket.__dict__.get("json")
    original = getattr(ConnectFuturesWebsocket, _RUN_ATTRIBUTE, None)
    if not isinstance(codec, PrivateExecutionJsonCodec) or not callable(original):
        raise RuntimeError("Futures native balance epoch boundary is unsupported")
    if getattr(original, _INSTALL_ATTRIBUTE, False):
        if not isinstance(codec, _EpochJsonCodec):
            raise RuntimeError("Futures native balance epoch codec was replaced")
        return
    run = cast(Callable[[ConnectFuturesWebsocket, asyncio.Event], Awaitable[None]], original)

    async def _run_in_physical_epoch(
        connector: ConnectFuturesWebsocket, event: asyncio.Event
    ) -> None:
        """Scope one actual receive task and remove its local proof even on failure or cancellation."""
        epoch = str(uuid7())
        token = _CURRENT_EPOCH.set((epoch, id(asyncio.current_task())))
        _ACTIVE_EPOCHS[connector] = epoch
        try:
            await run(connector, event)
        finally:
            if _ACTIVE_EPOCHS.get(connector) == epoch:
                _ACTIVE_EPOCHS.pop(connector, None)
            _CURRENT_EPOCH.reset(token)

    setattr(_run_in_physical_epoch, _INSTALL_ATTRIBUTE, True)
    setattr(ConnectFuturesWebsocket, _RUN_ATTRIBUTE, _run_in_physical_epoch)
    futures_websocket.__dict__["json"] = _EpochJsonCodec()
