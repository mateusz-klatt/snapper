"""Cover the delegate consult-duty control gate and its wire parsing."""

import pytest

from snapper_delegate.delegate_control import CONTROL_PROTOCOL_VERSION
from snapper_delegate.delegate_control import ControlDirective
from snapper_delegate.delegate_control import ControlGate
from snapper_delegate.delegate_control import ControlState
from snapper_delegate.delegate_control import applied_echo
from snapper_delegate.delegate_control import control_topic
from snapper_delegate.delegate_control import is_control_frame
from snapper_delegate.delegate_control import parse_control_directive
from snapper_delegate.json_types import JsonValue


class TestControlTopic:
    """Cover delegate-scoped control addressing."""

    def test_the_topic_carries_the_delegate_identity(self) -> None:
        """Push routing is topic-based, so identity is what addresses a runner."""
        assert control_topic("019f3e12") == "delegates.019f3e12.control"

    def test_a_blank_identity_cannot_address_anything(self) -> None:
        """A runner without identity must not subscribe to a guessed topic."""
        with pytest.raises(ValueError):
            control_topic("   ")


class TestParseControlDirective:
    """Cover the refusal boundary between a trustworthy directive and noise."""

    def test_a_well_formed_directive_parses(self) -> None:
        """Both fields present and typed yields the directive."""
        parsed = parse_control_directive({"state": "on_hold", "revision": 7})
        assert parsed == ControlDirective(state=ControlState.ON_HOLD, revision=7)

    @pytest.mark.parametrize(
        "payload",
        [
            None,
            "on_hold",
            {"revision": 1},
            {"state": "on_hold"},
            {"state": "paused", "revision": 1},
            {"state": "active", "revision": "1"},
            {"state": "active", "revision": -1},
            {"state": "active", "revision": True},
        ],
        ids=[
            "null",
            "not-an-object",
            "missing-state",
            "missing-revision",
            "unknown-state",
            "revision-as-string",
            "negative-revision",
            "bool-masquerading-as-int",
        ],
    )
    def test_an_untrustworthy_payload_is_refused(self, payload: JsonValue) -> None:
        """Anything the runner cannot vouch for parses to nothing, never to active."""
        assert parse_control_directive(payload) is None


class TestControlGate:
    """Cover the gate that decides whether consult work may start."""

    def test_a_fresh_gate_is_held(self) -> None:
        """A gate that has applied nothing refuses consults.

        Given a runner that has applied no directive
        When it asks whether it may work
        Then it is held, which is what makes a restart cold.
        """
        gate = ControlGate()
        assert gate.accepts_consults is False
        assert gate.applied is None

    def test_an_active_directive_opens_the_gate(self) -> None:
        """A valid active directive is what grants consult duty."""
        gate = ControlGate()
        assert gate.apply(ControlDirective(ControlState.ACTIVE, 1)) is True
        assert gate.accepts_consults is True

    def test_a_hold_closes_the_gate_and_reports_the_change(self) -> None:
        """A live hold transitions immediately and tells the caller to act."""
        gate = ControlGate()
        gate.apply(ControlDirective(ControlState.ACTIVE, 1))
        assert gate.apply(ControlDirective(ControlState.ON_HOLD, 2)) is True
        assert gate.accepts_consults is False

    def test_an_unreadable_directive_never_grants_duty(self) -> None:
        """Noise never opens the gate.

        Given a runner that has heard nothing valid
        When a malformed frame arrives
        Then it stays held rather than defaulting to working.
        """
        gate = ControlGate()
        assert gate.apply(None) is False
        assert gate.accepts_consults is False

    def test_an_unreadable_directive_never_revokes_a_good_state(self) -> None:
        """Noise never closes the gate either.

        Given a working runner
        When one malformed frame arrives
        Then the state it already proved is kept, so noise cannot pause it.
        """
        gate = ControlGate()
        gate.apply(ControlDirective(ControlState.ACTIVE, 3))
        assert gate.apply(None) is False
        assert gate.accepts_consults is True

    def test_a_stale_revision_cannot_resurrect_a_superseded_state(self) -> None:
        """Control only ever moves forward.

        Given a hold applied at revision 5
        When a delayed active frame from revision 4 arrives
        Then the hold stands, because control only moves forward.
        """
        gate = ControlGate()
        gate.apply(ControlDirective(ControlState.ON_HOLD, 5))
        assert gate.apply(ControlDirective(ControlState.ACTIVE, 4)) is False
        assert gate.accepts_consults is False

    def test_a_repeated_revision_is_not_reapplied(self) -> None:
        """A redelivered frame is idempotent and reports no change."""
        gate = ControlGate()
        gate.apply(ControlDirective(ControlState.ACTIVE, 2))
        assert gate.apply(ControlDirective(ControlState.ACTIVE, 2)) is False
        assert gate.accepts_consults is True

    def test_a_newer_revision_of_the_same_state_advances_quietly(self) -> None:
        """The revision advances for the echo, but no transition is announced."""
        gate = ControlGate()
        gate.apply(ControlDirective(ControlState.ACTIVE, 2))
        assert gate.apply(ControlDirective(ControlState.ACTIVE, 3)) is False
        applied = gate.applied
        assert applied is not None
        assert applied.revision == 3

    def test_a_reconnect_returns_the_runner_to_held(self) -> None:
        """A new session must re-learn the state before working.

        Given a working runner whose socket drops
        When a new session begins
        Then it holds until the server re-states control, because the state may
        have changed while it was not listening.
        """
        gate = ControlGate()
        gate.apply(ControlDirective(ControlState.ACTIVE, 9))
        gate.reset_for_reconnect()
        assert gate.accepts_consults is False
        assert gate.applied is None

    def test_a_revision_that_regressed_across_a_reconnect_is_accepted(self) -> None:
        """After a reset the server's current revision governs, whatever it is."""
        gate = ControlGate()
        gate.apply(ControlDirective(ControlState.ON_HOLD, 9))
        gate.reset_for_reconnect()
        assert gate.apply(ControlDirective(ControlState.ACTIVE, 1)) is True
        assert gate.accepts_consults is True


class TestWireHelpers:
    """Cover frame recognition and the applied-revision echo."""

    def test_only_the_control_type_is_recognised(self) -> None:
        """Recognition is exact so an unrelated frame never reaches the gate."""
        assert is_control_frame("delegate.control") is True
        assert is_control_frame("ai_review.request") is False

    def test_the_echo_reports_the_applied_revision_and_protocol(self) -> None:
        """The echo names both the applied revision and the protocol.

        Given a directive in force
        When the runner reports it
        Then the echo names the revision and the protocol, so the server can
        gate eligibility on what was applied rather than on what it last sent.
        """
        echo = applied_echo(ControlDirective(ControlState.ON_HOLD, 4))
        assert echo == {
            "type": "delegate.control_applied",
            "protocol": CONTROL_PROTOCOL_VERSION,
            "state": "on_hold",
            "applied_revision": 4,
        }
