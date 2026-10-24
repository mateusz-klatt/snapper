"""Unapplied exact prepared-key/authentication state for one actual private socket epoch.

This state machine is fed only by the closed socket proxy and one-shot actual
receive cell. Public models and nominal decoder metadata cannot mint a frame.
No control body, challenge, signature or API-key hash is persisted.
"""

from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from hmac import compare_digest
from typing import Literal
from typing import Never
from typing import SupportsIndex
from typing import cast
from uuid import uuid7

from kraken.futures import FuturesWSClient
from kraken.futures import User

from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.private_frame_ticket import AuthenticatedPrivateFrameTicket
from snapper.core.private_frame_ticket import PrivateSourceFrameContext
from snapper.core.private_frame_ticket import _ActualPrivateFrameIssuer
from snapper.core.private_frame_ticket import _mint_authenticated_private_frame
from snapper.infrastructure.exchanges.execution_wire_receipts import PrivateWireMessage
from snapper.infrastructure.exchanges.execution_wire_receipts import (
    decode_private_execution_receipt,
)
from snapper.infrastructure.exchanges.execution_wire_receipts import is_execution_control_message
from snapper.infrastructure.exchanges.owned_private_socket import ActualReceiveCell
from snapper.infrastructure.exchanges.owned_private_socket import OriginalReceive
from snapper.infrastructure.exchanges.prepared_private_key import PreparedKeyProofReference
from snapper.infrastructure.exchanges.prepared_private_key import PreparedKeyToken
from snapper.infrastructure.exchanges.private_control_decoder import TransientPrivateControl

type PrivateFeed = Literal["fills", "account_log"]
_FEEDS = frozenset({"fills", "account_log"})
_RAW_TO_SUBSCRIPTION: dict[str, PrivateFeed] = {
    "fills": "fills",
    "fills_snapshot": "fills",
    "account_log": "account_log",
    "account_log_snapshot": "account_log",
}


class PrivateAuthenticationError(ValueError):
    """The actual private source cannot establish ownership without retaining authentication material."""


class _OpaqueState:
    """Prevent accidental repr or generic serialization of live protocol state."""

    __slots__ = ()

    def __repr__(self) -> str:
        """Expose no source, SDK objects or transient authentication fields."""
        return "<PrivateAuthenticationState:opaque>"

    def __reduce_ex__(self, protocol: SupportsIndex) -> Never:
        """Refuse default persistence of live key/socket state."""
        raise TypeError("Private authentication state cannot be serialized")

    def __deepcopy__(self, memo: dict[int, object]) -> Never:
        """Refuse generic dataclass conversion into live SDK/key fields."""
        raise TypeError("Private authentication state cannot be copied")


class PrivateRuntimeSelection(_OpaqueState):
    """Actual current SDK objects in a non-dataclass box that generic asdict cannot traverse."""

    __slots__ = ("user", "private_client", "selection")
    user: User
    private_client: FuturesWSClient
    selection: ExecutionClientSelection

    def __init__(
        self, user: User, private_client: FuturesWSClient, selection: ExecutionClientSelection
    ) -> None:
        """Retain exact actual pointers without exposing them to default copying or serialization."""
        object.__setattr__(self, "user", user)
        object.__setattr__(self, "private_client", private_client)
        object.__setattr__(self, "selection", selection)

    def __setattr__(self, name: str, value: object) -> Never:
        """Refuse relabeling the exact actual SDK objects or selection after capture."""
        raise AttributeError("Actual private runtime selection is immutable")


class PreparedPrivateRuntimeBinding(_OpaqueState):
    """Freeze the original private SDK while rechecking actual parent ownership at every boundary."""

    __slots__ = ("_client", "_connector", "_token", "_current")

    def __init__(
        self,
        client: FuturesWSClient,
        token: PreparedKeyToken,
        current: Callable[[], PrivateRuntimeSelection],
    ) -> None:
        """Retain the actual selected objects without copying or exposing their secret fields."""
        self._client = client
        self._connector = client._conn
        self._token = token
        self._current = current

    def prove_key(self, encoded_key: object, observed_at: datetime) -> PreparedKeyProofReference:
        """Check the actual current User, original private SDK, selected route and actual encoded key.

        Args:
            encoded_key: Actual key field observed at this outbound/authentication boundary.
            observed_at: Actual aware local clock at that boundary after any SDK waits.

        Returns:
            The original safe committed proof reference only when actual SDK/key/selection still match.
        """
        try:
            current = self._current()
        except Exception:
            raise PrivateAuthenticationError("private_auth_runtime_provider_unavailable") from None
        if current.private_client is not self._client or self._client._conn is not self._connector:
            raise PrivateAuthenticationError("private_auth_actual_client_changed")
        try:
            self._token.assert_prepared_key(
                current.user, current.selection, self._client.key, observed_at
            )
            return self._token.assert_prepared_key(
                current.user, current.selection, encoded_key, observed_at
            )
        except Exception:
            raise PrivateAuthenticationError("private_auth_actual_key_or_route_changed") from None

    def current_proof(self, observed_at: datetime) -> PreparedKeyProofReference:
        """Recheck the original actual private SDK key rather than accepting a later nominal proof.

        Args:
            observed_at: Actual aware local clock at the current physical boundary.

        Returns:
            The same source-scoped safe proof after checking the actual original private SDK key.
        """
        return self.prove_key(self._client.key, observed_at)


def _same_text(left: object, right: str) -> bool:
    """Compare exact control text only in memory, refusing Unicode failures without source output."""
    if not isinstance(left, str):
        return False
    try:
        return compare_digest(left.encode("utf-8"), right.encode("utf-8"))
    except UnicodeError:
        return False


class _AuthEcho(_OpaqueState):
    """Bounded transient challenge/signature pair, discarded after its actual ACK or refusal."""

    __slots__ = ("_challenge", "_signature")

    def __init__(self, challenge: str, signature: str) -> None:
        """Keep exact fields privately without generating any persistent body or hash."""
        self._challenge = challenge
        self._signature = signature

    def matches(self, control: TransientPrivateControl) -> bool:
        """Match the exact sent echo values instead of matching feed or arrival order alone."""
        return _same_text(control.get("original_challenge"), self._challenge) and _same_text(
            control.get("signed_challenge"), self._signature
        )

    def close(self) -> None:
        """Drop transient secret references; Python memory zeroization is not claimed."""
        self._challenge = ""
        self._signature = ""


@dataclass(frozen=True, slots=True, repr=False)
class _PendingSubscription(_OpaqueState):
    """An actual encoded dispatch and transient echoes waiting for its matching physical ACK."""

    public_id: str
    feed: PrivateFeed
    encoded_at: datetime
    proof: PreparedKeyProofReference
    echo: _AuthEcho = field(repr=False)


@dataclass(frozen=True, slots=True)
class _AcknowledgedSubscription:
    """Only safe original facts survive a successful exact subscription acknowledgement."""

    dispatch_public_id: str
    feed: PrivateFeed
    encoded_at: datetime
    ack_observed_at: datetime
    proof: PreparedKeyProofReference


class PrivateSocketAuthentication(_OpaqueState):
    """One physical socket's exact key/ACK state, independent from current financial eligibility."""

    __slots__ = (
        "_binding",
        "_epoch",
        "_client_capture",
        "_challenge_requested_at",
        "_challenge",
        "_pending",
        "_acknowledged",
        "_blocked",
        "_closed",
        "_actual_socket_issuer",
        "_actual_receive_cell",
        "__weakref__",
    )

    def __init__(
        self,
        binding: PreparedPrivateRuntimeBinding,
        connection_epoch_public_id: str,
        client_capture_public_id: str,
    ) -> None:
        """Start without an acknowledged feed; decoder epoch alone grants no ownership."""
        self._binding = binding
        self._epoch = connection_epoch_public_id
        self._client_capture = client_capture_public_id
        self._challenge_requested_at: datetime | None = None
        self._challenge: str | None = None
        self._pending: dict[PrivateFeed, _PendingSubscription] = {}
        self._acknowledged: dict[PrivateFeed, _AcknowledgedSubscription] = {}
        self._blocked: set[PrivateFeed] = set()
        self._closed = False
        self._actual_socket_issuer: _ActualPrivateFrameIssuer | None = None
        self._actual_receive_cell: ActualReceiveCell | None = None

    def _bind_actual_socket_issuer(
        self, issuer: _ActualPrivateFrameIssuer, actual_receive_cell: ActualReceiveCell
    ) -> None:
        """Bind only from the actual verified connect adapter, never from public context construction."""
        if self._closed or self._actual_socket_issuer is not None:
            raise PrivateAuthenticationError("private_auth_socket_issuer_changed")
        self._actual_socket_issuer = issuer
        self._actual_receive_cell = actual_receive_cell

    def _block(self, feed: PrivateFeed | None = None) -> None:
        """Invalidate the ambiguous feed or whole socket until a new physical epoch."""
        selected = _FEEDS if feed is None else {feed}
        for value in selected:
            actual = cast(PrivateFeed, value)
            pending = self._pending.pop(actual, None)
            if pending is not None:
                pending.echo.close()
            self._acknowledged.pop(actual, None)
            self._blocked.add(actual)

    def _dispatch_subscription(
        self, document: dict[str, object], encoded_at: datetime, proof: PreparedKeyProofReference
    ) -> None:
        """Freeze one actual same-feed dispatch after the SDK has finished its challenge waits."""
        value = document.get("feed")
        if not isinstance(value, str) or value not in _FEEDS:
            self._block()
            return
        feed = cast(PrivateFeed, value)
        if feed in self._blocked or feed in self._pending or feed in self._acknowledged:
            self._block(feed)
            return
        original = document.get("original_challenge")
        signature = document.get("signed_challenge")
        if (
            self._challenge is None
            or not _same_text(original, self._challenge)
            or not isinstance(signature, str)
            or not signature.strip()
        ):
            raise PrivateAuthenticationError("private_auth_actual_challenge_unproven")
        self._pending[feed] = _PendingSubscription(
            str(uuid7()), feed, encoded_at, proof, _AuthEcho(self._challenge, signature)
        )

    def before_send(self, source: str, encoded_at: datetime) -> None:
        """Observe actual encoded source immediately before the real physical send starts.

        Args:
            source: Exact SDK-encoded outbound JSON before actual physical send.
            encoded_at: Actual aware local dispatch clock after challenge/backpressure waits.
        """
        if self._closed:
            raise PrivateAuthenticationError("private_auth_socket_closed")
        try:
            document = decode_private_execution_receipt(source.encode("utf-8"))
        except (ValueError, UnicodeError, RecursionError):
            raise PrivateAuthenticationError("private_auth_outbound_json_invalid") from None
        event = document.get("event")
        if event not in {"challenge", "subscribe", "unsubscribe"}:
            return
        proof = self._binding.prove_key(document.get("api_key"), encoded_at)
        if event == "challenge":
            if self._challenge_requested_at is not None:
                self._block()
            else:
                self._challenge_requested_at = encoded_at
        elif event == "unsubscribe":
            value = document.get("feed")
            self._block(
                cast(PrivateFeed, value) if isinstance(value, str) and value in _FEEDS else None
            )
        else:
            self._dispatch_subscription(document, encoded_at, proof)

    def _observe_challenge(self, control: TransientPrivateControl, observed_at: datetime) -> None:
        """Bind only the actual first returned challenge after the actual outbound request."""
        value = control.get("message")
        requested = self._challenge_requested_at
        if (
            requested is None
            or observed_at < requested
            or self._challenge is not None
            or not isinstance(value, str)
        ):
            self._block()
            return
        self._challenge = value

    def _observe_ack(self, control: TransientPrivateControl, observed_at: datetime) -> None:
        """Accept a successful ACK even while the actual physical send awaits backpressure."""
        value = control.get("feed")
        if not isinstance(value, str) or value not in _FEEDS:
            self._block()
            return
        feed = cast(PrivateFeed, value)
        pending = self._pending.pop(feed, None)
        if pending is None or control.get("event") != "subscribed":
            self._block(feed)
            if pending is not None:
                pending.echo.close()
            return
        try:
            proof = self._binding.prove_key(control.get("api_key"), observed_at)
            if (
                proof != pending.proof
                or observed_at < pending.encoded_at
                or not pending.echo.matches(control)
            ):
                self._block(feed)
                return
            self._acknowledged[feed] = _AcknowledgedSubscription(
                pending.public_id, feed, pending.encoded_at, observed_at, pending.proof
            )
        except ValueError:
            self._block(feed)
        finally:
            pending.echo.close()

    def observe_control(self, control: TransientPrivateControl, received: OriginalReceive) -> None:
        """Consume only an actual one-shot physical receive, never a decoder context label.

        Args:
            control: Safely decoded transient control view, never persisted as financial source.
            received: Actual one-shot original receive consumed by the same owned SDK task.
        """
        actual_cell = self._actual_receive_cell
        if self._closed or actual_cell is None or not actual_cell.was_consumed(received):
            return
        if control.failure_reason is not None:
            self._block()
        elif control.get("event") == "challenge":
            self._observe_challenge(control, received.observed_at)
        elif control.get("event") in {
            "subscribed",
            "subscribed_failed",
            "unsubscribed",
            "unsubscribed_failed",
        }:
            self._observe_ack(control, received.observed_at)
        elif control.get("event") == "error":
            self._block()

    def financial_ticket(
        self, message: PrivateWireMessage, received: OriginalReceive
    ) -> AuthenticatedPrivateFrameTicket | None:
        """Mint only before callback suspension from an actual original financial receive.

        Args:
            message: Safely classified private financial mapping retaining its original complete wire.
            received: Actual original physical receive already consumed by this socket task.

        Returns:
            An actually issued immutable frame ticket, or none when original ownership is unproven.
        """
        actual_cell = self._actual_receive_cell
        if (
            self._closed
            or actual_cell is None
            or not actual_cell.was_consumed(received)
            or not received.matches_financial_wire(message.wire_bytes)
        ):
            return None
        try:
            document = decode_private_execution_receipt(message.wire_bytes)
            raw_feed = document.get("feed")
            feed = _RAW_TO_SUBSCRIPTION.get(raw_feed) if isinstance(raw_feed, str) else None
            acknowledged = self._acknowledged.get(feed) if feed is not None else None
            if (
                acknowledged is None
                or is_execution_control_message(document)
                or received.observed_at < acknowledged.ack_observed_at
                or self._binding.current_proof(received.observed_at) != acknowledged.proof
            ):
                return None
            return self._frame_ticket(message, received, acknowledged)
        except (ValueError, UnicodeError, RecursionError):
            return None

    def _frame_ticket(
        self,
        message: PrivateWireMessage,
        received: OriginalReceive,
        acknowledged: _AcknowledgedSubscription,
    ) -> AuthenticatedPrivateFrameTicket:
        """Freeze only safe original proof/source facts; no UID is injected into source bytes."""
        proof = acknowledged.proof
        context = PrivateSourceFrameContext.model_validate(
            {
                "client_selection": proof.client_selection,
                "key_proof_public_id": proof.public_id,
                "key_proof_content_digest": proof.content_digest,
                "key_proof_recorded_at": proof.recorded_at,
                "key_owner_root_public_id": proof.key_owner_root_public_id,
                "key_owner_root_content_digest": proof.key_owner_root_content_digest,
                "client_capture_public_id": self._client_capture,
                "connection_epoch_public_id": self._epoch,
                "subscription_dispatch_public_id": acknowledged.dispatch_public_id,
                "frame_receive_public_id": received.receive_public_id,
                "subscription_feed": acknowledged.feed,
                "raw_feed": message.get("feed"),
                "auth_encoded_at": acknowledged.encoded_at,
                "ack_observed_at": acknowledged.ack_observed_at,
                "frame_observed_at": received.observed_at,
            }
        )
        issuer = self._actual_socket_issuer
        if issuer is None:
            raise PrivateAuthenticationError("private_auth_socket_was_not_actually_bound")
        return _mint_authenticated_private_frame(issuer, context, message.wire_bytes)

    def close(self) -> None:
        """Immediately revoke current state while preserving already minted historical frame tickets."""
        self._closed = True
        self._block()
        self._challenge = None
        if self._actual_socket_issuer is not None:
            self._actual_socket_issuer.close()
