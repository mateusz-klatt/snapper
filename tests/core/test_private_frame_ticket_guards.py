"""Reject invalid public frame shapes and tampered actual tickets without minting ownership."""

import copy
import pickle
from datetime import timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

import snapper.core.private_frame_ticket as tickets
import tests.core.test_private_source_ownership as public_shapes
import tests.data.test_private_source_ownership_store as actual_frames
from tests.helpers.native_account_authority import native_authority_scenario


@pytest.mark.parametrize("version", (True, 1.0, "1"))
def test_frame_context_rejects_coerced_version_aliases(version: object) -> None:
    """Keep public context discriminators exact before any runtime capability is considered.

    Given: A complete public context with a noninteger version alias.
    When: The public shape is revalidated.
    Then: The alias is refused without issuing a frame ticket.
    """
    material = public_shapes._payload().frame_context.model_dump(mode="python")
    material["schema_version"] = version
    with pytest.raises(ValidationError, match="must be an integer"):
        tickets.PrivateSourceFrameContext.model_validate(material)


@pytest.mark.parametrize("change", ("route", "feed", "chronology"))
def test_frame_context_refuses_inconsistent_original_source(change: str) -> None:
    """A valid field shape cannot conceal another route, subscription or source chronology.

    Given: Original public frame facts changed at one source boundary.
    When: Complete model validation checks the copied context.
    Then: The inconsistent original source is refused.
    """
    context = public_shapes._payload().frame_context
    if change == "route":
        context = context.model_copy(
            update={
                "client_selection": context.client_selection.model_copy(
                    update={"exchange": "kraken"}
                )
            }
        )
    elif change == "feed":
        context = context.model_copy(update={"raw_feed": "account_log"})
    else:
        context = context.model_copy(
            update={"ack_observed_at": context.auth_encoded_at - timedelta(seconds=1)}
        )
    with pytest.raises(ValidationError, match="Private source"):
        tickets.PrivateSourceFrameContext.model_validate(context)


def test_public_ticket_requires_nonempty_original_financial_bytes() -> None:
    """Empty source material cannot become even an unissued ticket shape.

    Given: A valid public context and empty financial bytes.
    When: A caller invokes the public ticket constructor.
    Then: It refuses the absent original frame.
    """
    with pytest.raises(ValueError, match="requires original financial bytes"):
        tickets.AuthenticatedPrivateFrameTicket(public_shapes._payload().frame_context, b"")


def test_public_ticket_refuses_attribute_replacement() -> None:
    """Public construction still cannot allow a delayed consumer to replace original bytes.

    Given: An unissued immutable ticket shape.
    When: Its private financial field is assigned another value.
    Then: Assignment is refused and the original bytes remain unchanged.
    """
    ticket = tickets.AuthenticatedPrivateFrameTicket(
        public_shapes._payload().frame_context, b"original financial bytes"
    )
    with pytest.raises(AttributeError, match="immutable"):
        ticket._financial_wire = b"changed financial bytes"
    assert ticket.matches_financial_wire(b"original financial bytes")


def test_unregistered_issuer_is_opaque_nontransferable_and_cannot_mint() -> None:
    """A publicly reachable constructor cannot fabricate actual socket registration.

    Given: An unregistered issuer lookalike and a valid public context.
    When: Generic formatting, persistence, copying and issuance are attempted.
    Then: Formatting is opaque and every authority-transfer attempt is refused.
    """
    issuer = tickets._ActualPrivateFrameIssuer()
    assert repr(issuer) == "<ActualPrivateFrameIssuer:opaque>"
    with pytest.raises(TypeError, match="cannot be serialized"):
        pickle.dumps(issuer)
    with pytest.raises(TypeError, match="cannot be copied"):
        copy.deepcopy(issuer)
    with pytest.raises(ValueError, match="issuer is unavailable"):
        tickets._mint_authenticated_private_frame(
            issuer, public_shapes._payload().frame_context, b"financial bytes"
        )


@pytest.mark.parametrize("change", ("selected-wire", "ticket-wire", "ticket-context"))
async def test_actual_issued_ticket_refuses_changed_original_material(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    """An actually issued capability cannot authenticate modified bytes or public facts.

    Given: A ticket from real local signed HTTP and the actual SDK subscribe/ACK/receive path.
    When: The requested wire or an externally tampered ticket field differs from its registration.
    Then: The consumer refuses the changed original instead of creating ownership.
    """
    async with (
        native_authority_scenario(tmp_path, monkeypatch) as source,
        actual_frames._received_frame(source, monkeypatch) as received,
    ):
        ticket = received.ticket
        wire = actual_frames._WIRE
        if change == "selected-wire":
            wire += b" "
        elif change == "ticket-wire":
            object.__setattr__(ticket, "_financial_wire", wire + b" ")
        else:
            object.__setattr__(
                ticket,
                "_context",
                ticket.context.model_copy(
                    update={"frame_receive_public_id": public_shapes._uuid(999)}
                ),
            )
        with pytest.raises(ValueError, match="original source changed"):
            tickets.require_authenticated_private_frame(ticket, wire)
