"""Unapplied exact physical receive/send adapter, without authentication authority.

A shared one-shot cell survives asyncio.wait_for's receive child task. Only the
original actual SDK run task can consume the exact returned source object. This
module does not mint an authenticated frame ticket or persist a sidecar.
"""

import asyncio
from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from typing import Never
from typing import Protocol
from typing import SupportsIndex
from weakref import WeakKeyDictionary

from websockets.protocol import State


class PrivatePhysicalSocket(Protocol):
    """The actual pinned connector's complete socket surface used by the SDK."""

    @property
    def state(self) -> State:
        """Expose the real socket lifecycle to the existing active-epoch check.

        Returns:
            Actual physical WebSocket lifecycle state, without a guessed connection flag.
        """
        ...

    async def recv(self) -> str | bytes:
        """Receive an original complete WebSocket entity.

        Returns:
            The same original complete text or binary entity returned by the physical socket.
        """
        ...

    async def send(self, message: str) -> None:
        """Send the exact already encoded SDK request.

        Args:
            message: Exact already encoded SDK request to guard and send without reserialization.
        """
        ...

    async def close(self) -> None:
        """Close the same actual physical socket."""
        ...


class PrivateSocketBoundaryError(ValueError):
    """The original private socket boundary is unavailable without exposing source text."""


def _task_identity() -> int | None:
    """Read only the actual current task, without accepting an inherited context label."""
    task = asyncio.current_task()
    return id(task) if task is not None else None


class OriginalReceive:
    """Opaque original source object, without authentication, raw-storage or serialization rights."""

    __slots__ = ("_entity", "observed_at", "receive_public_id", "__weakref__")
    _entity: str | bytes
    observed_at: datetime
    receive_public_id: str

    def __init__(self, entity: str | bytes, observed_at: datetime, receive_public_id: str) -> None:
        """Retain the exact actual returned source only until the owned decoder classifies it."""
        object.__setattr__(self, "_entity", entity)
        object.__setattr__(self, "observed_at", observed_at)
        object.__setattr__(self, "receive_public_id", receive_public_id)

    def __setattr__(self, name: str, value: object) -> Never:
        """Refuse replacement of the exact original source or its original observation clock."""
        raise AttributeError("Original private receive is immutable")

    def __repr__(self) -> str:
        """Keep control bodies out of generic source/debug output."""
        return "<OriginalReceive:opaque>"

    def __reduce_ex__(self, protocol: SupportsIndex) -> Never:
        """Refuse a generic serialization path into an unclassified control body."""
        raise TypeError("Original private receive cannot be serialized")

    def __deepcopy__(self, memo: dict[int, object]) -> Never:
        """Refuse dataclass conversion/copy traversal into the unclassified source."""
        raise TypeError("Original private receive cannot be copied")

    def is_source(self, entity: object) -> bool:
        """Compare source object identity rather than accepting reconstructed equivalent JSON.

        Args:
            entity: Actual decoder argument to compare with the original returned source object.

        Returns:
            Whether this is the identical original source object, not equivalent reconstructed JSON.
        """
        return self._entity is entity

    def matches_financial_wire(self, financial_wire: bytes) -> bool:
        """Compare the already safely classified financial wire without persisting another digest.

        Args:
            financial_wire: Already safely classified original financial bytes to compare in memory.

        Returns:
            Whether those bytes match the original receive without retaining another body digest.
        """
        try:
            original = (
                self._entity.encode("utf-8") if isinstance(self._entity, str) else self._entity
            )
        except UnicodeError:
            return False
        return original == financial_wire


_SOCKET_RECEIVES: WeakKeyDictionary[OriginalReceive, object] = WeakKeyDictionary()
_CONSUMED_RECEIVES: WeakKeyDictionary[OriginalReceive, object] = WeakKeyDictionary()


def _mint_original_receive(
    entity: str | bytes, observed_at: datetime, public_id: str, cell_issuer: object
) -> OriginalReceive:
    """Issue only immediately after the owned proxy's actual physical recv completed."""
    received = OriginalReceive(entity, observed_at, public_id)
    _SOCKET_RECEIVES[received] = cell_issuer
    return received


class ActualReceiveCell:
    """One socket's single receive handoff, consumed once only by its original SDK run task."""

    __slots__ = ("_owner_task", "_pending", "_closed", "_issuer")
    _owner_task: int
    _pending: OriginalReceive | None
    _closed: bool

    def __init__(self, owner_task: int) -> None:
        """Bind the actual run task before the wait_for-created receive child can start."""
        self._owner_task = owner_task
        self._issuer = object()
        self._pending = None
        self._closed = False

    def record(self, received: OriginalReceive) -> None:
        """Refuse ambiguity instead of assigning one of two concurrent physical receives.

        Args:
            received: Actual receive issued by this owned physical socket handoff.
        """
        if self._closed:
            return
        if _SOCKET_RECEIVES.get(received) is not self._issuer:
            self.close()
            return
        if self._pending is not None:
            self.close()
            return
        self._pending = received

    def consume(self, original_entity: object) -> OriginalReceive | None:
        """Hand the exact source to its actual SDK decoder task once, before callback suspension.

        Args:
            original_entity: Exact source argument passed to the actual original SDK decoder task.

        Returns:
            The single original receive, or none when the source/task/lifecycle is unproven.
        """
        if self._closed or _task_identity() != self._owner_task:
            return None
        pending = self._pending
        self._pending = None
        if pending is None or not pending.is_source(original_entity):
            return None
        _CONSUMED_RECEIVES[pending] = self._issuer
        return pending

    def was_consumed(self, received: OriginalReceive) -> bool:
        """Require actual issuance and original-parent consumption by this exact physical cell.

        Args:
            received: Actual source candidate whose issuance and parent-task consumption must be verified.

        Returns:
            Whether this same physical cell issued and consumed the original receive.
        """
        return _CONSUMED_RECEIVES.get(received) is self._issuer

    def close(self) -> None:
        """Invalidate current/handoff source without changing already issued historical tickets."""
        self._closed = True
        self._pending = None


class OwnedPrivateSocket:
    """Opaque proxy around the actual socket; it establishes no ACK or account ownership by itself."""

    __slots__ = ("_socket", "_cell", "_new_receive_id", "_before_send")

    def __init__(
        self,
        socket: PrivatePhysicalSocket,
        cell: ActualReceiveCell,
        new_receive_id: Callable[[], str],
        before_send: Callable[[str, datetime], None],
    ) -> None:
        """Keep exact owned physical transport plus closed local ID/send guards."""
        self._socket = socket
        self._cell = cell
        self._new_receive_id = new_receive_id
        self._before_send = before_send

    def __repr__(self) -> str:
        """Prevent a socket's implementation repr from leaking connection/control material."""
        return "<OwnedPrivateSocket:opaque>"

    def __reduce_ex__(self, protocol: SupportsIndex) -> Never:
        """Refuse generic persistence of an actual private transport and its pending receive."""
        raise TypeError("Owned private socket cannot be serialized")

    def __deepcopy__(self, memo: dict[int, object]) -> Never:
        """Refuse generic copy traversal into the actual transport or pending source."""
        raise TypeError("Owned private socket cannot be copied")

    @property
    def state(self) -> State:
        """Delegate the exact real lifecycle required by active physical epoch checks.

        Returns:
            Actual physical WebSocket lifecycle state, without a guessed connection flag.
        """
        return self._socket.state

    @property
    def request(self) -> object | None:
        """Expose the original physical handshake to the existing endpoint verifier.

        Returns:
            The same actual socket request, or explicit absence when that source is unavailable.
            This proxy creates no handshake fields and establishes no route by itself.
        """
        request: object | None = getattr(self._socket, "request", None)
        return request

    async def recv(self) -> str | bytes:
        """Record the exact actual return object even when wait_for runs this method in a child task.

        Returns:
            The same original complete text or binary entity returned by the physical socket.
        """
        try:
            entity = await self._socket.recv()
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            raise TimeoutError("private_socket_receive_timeout") from None
        except Exception:
            raise PrivateSocketBoundaryError("private_socket_receive_failed") from None
        if not isinstance(entity, str | bytes):
            raise PrivateSocketBoundaryError("private_socket_receive_type_invalid")
        self._cell.record(
            _mint_original_receive(
                entity, datetime.now(UTC), self._new_receive_id(), self._cell._issuer
            )
        )
        return entity

    async def send(self, message: str) -> None:
        """Guard actual encoded source after SDK challenge waits and before physical send begins.

        Args:
            message: Exact already encoded SDK request to guard and send without reserialization.
        """
        try:
            self._before_send(message, datetime.now(UTC))
            await self._socket.send(message)
        except asyncio.CancelledError:
            raise
        except Exception:
            self._cell.close()
            raise PrivateSocketBoundaryError("private_socket_send_refused") from None

    async def close(self) -> None:
        """Invalidate current handoff immediately, then close the same actual socket."""
        self._cell.close()
        try:
            await self._socket.close()
        except asyncio.CancelledError:
            raise
        except Exception:
            raise PrivateSocketBoundaryError("private_socket_close_failed") from None
