"""Unapplied immutable public frame context and opaque actual authenticated receive ticket.

Only the closed pinned-SDK bridge may construct a ticket after actual exact-key
proof, physical subscribe dispatch, matching ACK and original receive. The safe
public context alone grants no permission. A ticket contains financial bytes
privately; it never contains authentication bodies, keys or their hashes.
"""

from typing import Literal
from typing import Never
from typing import Self
from typing import SupportsIndex
from weakref import WeakKeyDictionary
from weakref import WeakSet

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import field_validator
from pydantic import model_validator

from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.execution_unit_snapshot import EvidenceDigest
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.execution_unit_snapshot import EvidenceUUID


class PrivateSourceFrameContext(BaseModel):
    """Safe original physical-frame facts, requiring independent proof/raw rebinding at persistence."""

    model_config = ConfigDict(
        frozen=True,
        strict=True,
        extra="forbid",
        revalidate_instances="always",
        hide_input_in_errors=True,
    )

    schema_version: Literal[1] = 1
    source_policy: Literal["kraken_futures_closed_private_transport_v1"] = (
        "kraken_futures_closed_private_transport_v1"
    )
    client_selection: ExecutionClientSelection
    key_proof_public_id: EvidenceUUID
    key_proof_content_digest: EvidenceDigest
    key_proof_recorded_at: EvidenceInstant
    key_owner_root_public_id: EvidenceUUID
    key_owner_root_content_digest: EvidenceDigest
    client_capture_public_id: EvidenceUUID
    connection_epoch_public_id: EvidenceUUID
    subscription_dispatch_public_id: EvidenceUUID
    frame_receive_public_id: EvidenceUUID
    subscription_feed: Literal["fills", "account_log"]
    raw_feed: Literal["fills", "fills_snapshot", "account_log", "account_log_snapshot"]
    auth_encoded_at: EvidenceInstant
    ack_observed_at: EvidenceInstant
    frame_observed_at: EvidenceInstant

    @field_validator("schema_version", mode="before")
    @classmethod
    def _strict_version(cls, value: object) -> object:
        """Reject bool/float aliases before a literal version could coerce them."""
        if type(value) is not int:
            raise ValueError("Private source version must be an integer")
        return value

    @model_validator(mode="after")
    def validate_source(self) -> Self:
        """Require one exact documented feed pair, selected native route and honest original clocks.

        Returns:
            The unchanged safe public context after static consistency validation.
        """
        selection = self.client_selection
        origins = {
            "production": "https://futures.kraken.com",
            "demo": "https://demo-futures.kraken.com",
        }
        if (
            selection.exchange != "kraken_futures"
            or selection.mode != "live"
            or selection.environment not in origins
            or selection.public_api_origin != origins[selection.environment]
        ):
            raise ValueError("Private source selected route is unproven")
        feeds = {
            "fills": {"fills", "fills_snapshot"},
            "account_log": {"account_log", "account_log_snapshot"},
        }
        if self.raw_feed not in feeds[self.subscription_feed]:
            raise ValueError("Private source feed does not match its authenticated subscription")
        revision = selection.credential_timestamp
        if (
            selection.credential_public_id is None
            or revision is None
            or not revision
            <= self.key_proof_recorded_at
            <= self.auth_encoded_at
            <= self.ack_observed_at
            <= self.frame_observed_at
        ):
            raise ValueError("Private source original chronology is inconsistent")
        return self


class AuthenticatedPrivateFrameTicket:
    """Opaque original authenticated source capability, minted only by the closed runtime bridge.

    Delayed capture may preserve this original historical receive after disconnect;
    it cannot reactivate current socket state. All N children share this ticket's
    original frame identity. The financial writer still separately enforces raw
    interpretation, actual account authority and durable financial-owner policy.
    """

    __slots__ = ("_context", "_financial_wire", "__weakref__")
    _context: PrivateSourceFrameContext
    _financial_wire: bytes

    def __init__(self, context: PrivateSourceFrameContext, financial_wire: bytes) -> None:
        """Retain one actual safely classified original financial frame and its original safe context.

        Args:
            context: Bridge-created immutable facts from the exact actual socket/key/ACK path.
            financial_wire: Exact original financial bytes, containing no authentication control body.
        """
        if not isinstance(financial_wire, bytes) or not financial_wire:
            raise ValueError("Authenticated private frame requires original financial bytes")
        object.__setattr__(self, "_context", PrivateSourceFrameContext.model_validate(context))
        object.__setattr__(self, "_financial_wire", financial_wire)

    def __setattr__(self, name: str, value: object) -> Never:
        """Refuse replacement of original source bytes or public evidence through a delayed callback."""
        raise AttributeError("Authenticated private frame ticket is immutable")

    def __repr__(self) -> str:
        """Reveal no financial bytes or secret-bearing runtime pointers in generic log formatting."""
        return "<AuthenticatedPrivateFrameTicket:opaque>"

    def __reduce_ex__(self, protocol: SupportsIndex) -> Never:
        """Refuse persistence of the runtime capability itself instead of its verified safe projection."""
        raise TypeError("Authenticated private frame ticket cannot be serialized")

    def __deepcopy__(self, memo: dict[int, object]) -> Never:
        """Refuse generic dataclass copy traversal into original source bytes."""
        raise TypeError("Authenticated private frame ticket cannot be copied")

    @property
    def context(self) -> PrivateSourceFrameContext:
        """Expose only safe immutable facts, never authentication or financial source bodies.

        Returns:
            Original public evidence; it still requires the actual bound proof/raw loaders.
        """
        return self._context.model_copy(deep=True)

    def matches_financial_wire(self, original_financial_wire: bytes) -> bool:
        """Verify actual observation bytes before constructing its durable raw envelope.

        Args:
            original_financial_wire: Genuine exact-wire field from the source capture boundary.

        Returns:
            Whether this is the same original received frame, without modifying either source.
        """
        return self._financial_wire == original_financial_wire


class _ActualPrivateFrameIssuer:
    """Private closed-socket issuance handle, separate from a publicly constructed ticket shape."""

    __slots__ = ("closed", "__weakref__")

    def __init__(self) -> None:
        """Start unregistered; only the private actual-connect factory registers an issuer."""
        self.closed = False

    def __repr__(self) -> str:
        """Expose no runtime source objects or protocol state."""
        return "<ActualPrivateFrameIssuer:opaque>"

    def __reduce_ex__(self, protocol: SupportsIndex) -> Never:
        """Refuse transfer of an in-process actual-socket issuance handle."""
        raise TypeError("Actual private frame issuer cannot be serialized")

    def __deepcopy__(self, memo: dict[int, object]) -> Never:
        """Refuse generic copying of actual socket issuance authority."""
        raise TypeError("Actual private frame issuer cannot be copied")

    def close(self) -> None:
        """Revoke future issuance while leaving original already issued historical tickets intact."""
        self.closed = True
        _ACTUAL_SOCKET_ISSUERS.discard(self)


_ACTUAL_SOCKET_ISSUERS: WeakSet[object] = WeakSet()
_ISSUED_FRAMES: WeakKeyDictionary[
    AuthenticatedPrivateFrameTicket, tuple[PrivateSourceFrameContext, bytes]
] = WeakKeyDictionary()


def _open_actual_private_frame_issuer() -> _ActualPrivateFrameIssuer:
    """Open only from the actual selected-key/socket/epoch connect adapter after its proof checks.

    This internal entry point is not exposed as a source-capture parameter or API.
    Publicly constructed issuer/ticket objects remain unregistered and grant none.
    """
    issuer = _ActualPrivateFrameIssuer()
    _ACTUAL_SOCKET_ISSUERS.add(issuer)
    return issuer


def _mint_authenticated_private_frame(
    issuer: _ActualPrivateFrameIssuer,
    context: PrivateSourceFrameContext,
    financial_wire: bytes,
) -> AuthenticatedPrivateFrameTicket:
    """Issue only through an actual bridge-owned open socket after its one-shot authenticated receive."""
    if issuer not in _ACTUAL_SOCKET_ISSUERS or issuer.closed:
        raise ValueError("Actual private socket issuer is unavailable")
    ticket = AuthenticatedPrivateFrameTicket(context, financial_wire)
    _ISSUED_FRAMES[ticket] = (ticket.context.model_copy(deep=True), financial_wire)
    return ticket


def require_authenticated_private_frame(
    ticket: object, financial_wire: bytes
) -> PrivateSourceFrameContext:
    """Require actual runtime issuance before the in-session raw/sidecar producer can use a ticket.

    Args:
        ticket: Candidate runtime object, possibly an unissued publicly constructed lookalike.
        financial_wire: Actual observation's original safely classified financial wire bytes.

    Returns:
        The original privately registered immutable safe context for this exact source.

    Raises:
        ValueError: If the candidate was not actually issued or its source/context was changed.
    """
    if not isinstance(ticket, AuthenticatedPrivateFrameTicket):
        raise ValueError("Authenticated private frame was not actually issued")
    issued = _ISSUED_FRAMES.get(ticket)
    if issued is None:
        raise ValueError("Authenticated private frame was not actually issued")
    context, original_wire = issued
    if (
        original_wire != financial_wire
        or ticket.context != context
        or not ticket.matches_financial_wire(original_wire)
    ):
        raise ValueError("Authenticated private frame original source changed")
    return context.model_copy(deep=True)
