"""Server-authoritative consult-duty control state for one delegate runner.

The runner never decides whether it may work; it obeys a persisted state the
server owns, exactly like the process desired-state plane. This module is the
pure decision core of that obedience: it parses what the server said, decides
whether the runner may accept consults, and refuses to guess.

Three properties carry the whole design.

**Held is the boot state.** A runner that has not yet applied a valid revision
is held, so a restart can never run a single consult before it learns what it
is allowed to do. Ambiguity — a missing field, a malformed frame, a revision
that moves backwards — resolves to held, never to active.

**Hold is a pause, not containment.** A held runner keeps its socket and its
heartbeat: it stays observable and instantly resumable. Hold stops NEW consult
work; it is not a security boundary and must never be described as one. A
compromised agent still holds its token, so containment is delegate
deactivation, which revokes that token.

**The applied revision is echoed back.** The server cannot infer from silence
whether a control frame arrived, and a lost resume would otherwise leave the
server believing a runner is working while it sits held — every consult routed
to it would then die at its deadline. So the runner reports the revision it has
actually applied, and the server gates eligibility on that echo rather than on
its own last write.
"""

from dataclasses import dataclass
from enum import StrEnum
from typing import Final

from snapper_delegate.json_types import JsonObject
from snapper_delegate.json_types import JsonValue

CONTROL_PROTOCOL_VERSION: Final[str] = "delegate-control-v1"
"""Wire contract this runner implements, echoed so a peer can detect drift."""

_CONTROL_FRAME_TYPE: Final[str] = "delegate.control"
"""Server push carrying a live control-state change."""

_APPLIED_FRAME_TYPE: Final[str] = "delegate.control_applied"
"""Client echo reporting the revision this runner has actually applied."""


class ControlState(StrEnum):
    """Consult-duty state a delegate runner may occupy."""

    ACTIVE = "active"
    ON_HOLD = "on_hold"


@dataclass(frozen=True, slots=True)
class ControlDirective:
    """One server-authoritative state paired with its monotonic revision."""

    state: ControlState
    revision: int


def control_topic(delegate_public_id: str) -> str:
    """Return the delegate-scoped topic control frames are published on.

    Push routing is topic-based and carries no per-connection registry, so the
    delegate's own identity is what addresses it.

    Args:
        delegate_public_id: Identity this runner authenticated as.

    Returns:
        The topic to subscribe for control frames.

    Raises:
        ValueError: When the identity is blank and could not address anything.
    """
    identity = delegate_public_id.strip()
    if not identity:
        raise ValueError("a control topic requires a non-empty delegate identity")
    return f"delegates.{identity}.control"


def parse_control_directive(payload: JsonValue) -> ControlDirective | None:
    """Read a directive from a frame or an auth payload, or refuse it.

    Refusing returns ``None`` rather than raising: an unreadable directive is
    an ordinary protocol event that resolves to held, not an error the caller
    should have to catch on the receive path.

    Args:
        payload: Decoded frame body or the control block of an auth payload.

    Returns:
        The directive, or ``None`` when the payload cannot be trusted.
    """
    if not isinstance(payload, dict):
        return None
    raw_state = payload.get("state")
    raw_revision = payload.get("revision")
    if not isinstance(raw_state, str):
        return None
    if not isinstance(raw_revision, int) or isinstance(raw_revision, bool):
        return None
    if raw_revision < 0:
        return None
    try:
        state = ControlState(raw_state)
    except ValueError:
        return None
    return ControlDirective(state=state, revision=raw_revision)


def is_control_frame(frame_type: str) -> bool:
    """Return whether this frame type carries a control directive.

    Args:
        frame_type: Discriminator read from one decoded server frame.

    Returns:
        Whether the frame should be routed to the control gate.
    """
    return frame_type == _CONTROL_FRAME_TYPE


def applied_echo(directive: ControlDirective) -> JsonObject:
    """Build the echo announcing which revision this runner has applied.

    Args:
        directive: The directive that is now in force locally.

    Returns:
        The client frame body to send.
    """
    return {
        "type": _APPLIED_FRAME_TYPE,
        "protocol": CONTROL_PROTOCOL_VERSION,
        "state": directive.state.value,
        "applied_revision": directive.revision,
    }


class ControlGate:
    """Track the applied directive and answer whether consults may be accepted.

    The gate starts held with no applied revision, which is what makes a cold
    boot safe. It only ever moves forward: a directive whose revision is not
    newer than the applied one is ignored, so a replayed or reordered frame
    cannot resurrect a superseded state.
    """

    def __init__(self) -> None:
        """Start held, before any server directive has been seen."""
        self._directive: ControlDirective | None = None

    @property
    def applied(self) -> ControlDirective | None:
        """Return the directive in force, or ``None`` while never configured.

        Returns:
            The applied directive, or ``None`` before any was adopted.
        """
        return self._directive

    @property
    def accepts_consults(self) -> bool:
        """Return whether new consult work may start right now.

        A runner that has applied nothing is held, so this is ``False`` until a
        valid directive arrives — the property that makes a restart cold.

        Returns:
            Whether an active directive is currently in force.
        """
        return self._directive is not None and self._directive.state is ControlState.ACTIVE

    def apply(self, directive: ControlDirective | None) -> bool:
        """Adopt a directive when it is newer, reporting whether state changed.

        A ``None`` directive is an unreadable one. It never clears an applied
        state — losing a good state because one frame was malformed would swap a
        working runner into a hold nobody ordered — but it also never grants
        one, so a runner that has yet to hear anything valid stays held.

        Args:
            directive: Parsed directive, or ``None`` when unreadable.

        Returns:
            Whether the applied state or revision changed.
        """
        if directive is None:
            return False
        current = self._directive
        if current is not None and directive.revision <= current.revision:
            return False
        self._directive = directive
        return current is None or current.state is not directive.state

    def reset_for_reconnect(self) -> None:
        """Forget the applied directive because a new session must re-learn it.

        A reconnect is a fresh negotiation: the state may have changed while the
        socket was down, and the server re-states it on connect. Holding across
        that gap is the safe direction — the runner pauses for the length of one
        handshake rather than working on a state it can no longer vouch for.
        """
        self._directive = None
