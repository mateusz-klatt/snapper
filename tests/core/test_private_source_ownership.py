"""Safe-shape and canonical projection checks never fabricate authenticated source ownership."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest
from pydantic import ValidationError

from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.private_frame_ticket import PrivateSourceFrameContext
from snapper.core.private_source_ownership import PrivateSourceOwnershipPayload
from snapper.core.private_source_ownership import PrivateSourceOwnershipReference
from snapper.core.private_source_ownership import canonical_private_source_ownership
from snapper.core.private_source_ownership import private_source_ownership_digest
from snapper.core.private_source_ownership import private_source_ownership_projection

_TIME = datetime(2026, 10, 23, 18, tzinfo=UTC)


def _uuid(number: int) -> str:
    """Provide explicitly synthetic canonical identities for shape tests only."""
    return f"00000000-0000-7000-8000-{number:012d}"


def _payload() -> PrivateSourceOwnershipPayload:
    """Construct a public shape without issuing a runtime ticket or certifying any actual source."""
    selection = ExecutionClientSelection(
        wallet_public_id=_uuid(1),
        exchange="kraken_futures",
        mode="live",
        environment="production",
        credential_public_id=_uuid(2),
        credential_timestamp=_TIME,
        public_api_origin="https://futures.kraken.com",
    )
    frame = PrivateSourceFrameContext(
        client_selection=selection,
        key_proof_public_id=_uuid(3),
        key_proof_content_digest="a" * 64,
        key_proof_recorded_at=_TIME + timedelta(seconds=1),
        key_owner_root_public_id=_uuid(3),
        key_owner_root_content_digest="a" * 64,
        client_capture_public_id=_uuid(4),
        connection_epoch_public_id=_uuid(5),
        subscription_dispatch_public_id=_uuid(6),
        frame_receive_public_id=_uuid(7),
        subscription_feed="fills",
        raw_feed="fills",
        auth_encoded_at=_TIME + timedelta(seconds=2),
        ack_observed_at=_TIME + timedelta(seconds=3),
        frame_observed_at=_TIME + timedelta(seconds=4),
    )
    return PrivateSourceOwnershipPayload(
        binding_kind="actual_receive",
        frame_context=frame,
        raw_receipt_public_id=_uuid(8),
        raw_content_digest="b" * 64,
        target_json_pointer="/fills/0",
        frame_basis_digest="c" * 64,
        raw_observed_at=_TIME + timedelta(seconds=5),
        raw_recorded_at=_TIME + timedelta(seconds=6),
        recorded_at=_TIME + timedelta(seconds=7),
    )


def test_safe_shape_roundtrip_does_not_contain_wire_or_auth_material() -> None:
    """Keep only explicitly safe public evidence in the prospective canonical sidecar.

    Given: A public frame context and canonical raw/proof references with no issued runtime ticket.
    When: The safe payload is serialized and rebound.
    Then: Its values roundtrip without financial bytes, account UID or authentication material.
    """
    payload = _payload()
    canonical = canonical_private_source_ownership(payload)
    assert PrivateSourceOwnershipPayload.model_validate_json(canonical) == payload
    assert len(private_source_ownership_digest(payload)) == 64
    for forbidden in ("financial_wire", "account_uid", "apiKey", "challenge", "signature"):
        assert f'"{forbidden}"' not in canonical


@pytest.mark.parametrize("version", [True, False, 1.0, "1", 0, 2])
def test_payload_requires_the_actual_supported_integer_version(version: object) -> None:
    """Reject discriminator aliases without applying a discriminated union to strict before validators.

    Given: A complete prospective safe payload and a different or coerced version value.
    When: Canonicalization revalidates an unchecked model copy.
    Then: The unsupported shape is refused.
    """
    with pytest.raises(ValidationError):
        canonical_private_source_ownership(
            _payload().model_copy(update={"schema_version": version})
        )


@pytest.mark.parametrize("field", ["raw_observed_at", "raw_recorded_at", "recorded_at"])
def test_retained_source_clocks_cannot_precede_their_actual_predecessor(field: str) -> None:
    """Keep the selected raw and sidecar recording chronology separate from the source receive clock.

    Given: Original frame knowledge and later raw/sidecar recording clocks.
    When: One retained clock is changed to precede the original frame.
    Then: Full canonical validation refuses the inconsistent chronology.
    """
    with pytest.raises(ValidationError):
        canonical_private_source_ownership(_payload().model_copy(update={field: _TIME}))


@pytest.mark.parametrize(
    "pointer", ["", "/fills/00", "/fills/-1", "/fills/+1", "/fills/١", "/logs/0", "/fills/1/extra"]
)
def test_selector_must_be_canonical_for_the_original_raw_feed(pointer: str) -> None:
    """Require ASCII canonical child syntax without interpreting financial values or inventing bounds.

    Given: A fills frame and a malformed or different-feed selector.
    When: The payload is revalidated.
    Then: The selector is refused before any actual source ownership claim could be persisted.
    """
    with pytest.raises(ValidationError):
        canonical_private_source_ownership(
            _payload().model_copy(update={"target_json_pointer": pointer})
        )


def test_actual_receive_cannot_claim_a_recovery_origin() -> None:
    """Keep original physical receives and derived recovery projections distinct.

    Given: A shaped actual_receive payload.
    When: An explicit origin tuple is added through an unchecked copy.
    Then: Revalidation refuses the mixed source kind.
    """
    with pytest.raises(ValidationError):
        canonical_private_source_ownership(
            _payload().model_copy(
                update={
                    "origin_sidecar_public_id": _uuid(20),
                    "origin_sidecar_content_digest": "d" * 64,
                }
            )
        )


@pytest.mark.parametrize(
    "changes",
    [{}, {"origin_sidecar_public_id": _uuid(20)}, {"origin_sidecar_content_digest": "d" * 64}],
)
def test_projection_requires_the_complete_direct_original_reference(
    changes: dict[str, object],
) -> None:
    """Require an explicit original reference instead of a digest search or current identity fallback.

    Given: A safe shape changed to recovery_projection.
    When: Its direct origin tuple is absent or partial.
    Then: Canonical validation refuses the incomplete source relation.
    """
    with pytest.raises(ValidationError):
        canonical_private_source_ownership(
            _payload().model_copy(update={"binding_kind": "recovery_projection", **changes})
        )


def test_recovery_keeps_original_source_clocks_and_later_selected_raw_recording() -> None:
    """Preserve original receive evidence while recording the actual newly selected recovered raw row.

    Given: Original auth/ACK/frame/raw-observed clocks and a later raw retention transaction.
    When: A projection names its explicit original sidecar and distinct recovered raw identity.
    Then: Only selected-row and sidecar recording clocks advance and the source context stays unchanged.
    """
    original = _payload()
    projection = PrivateSourceOwnershipPayload.model_validate(
        original.model_copy(
            update={
                "binding_kind": "recovery_projection",
                "raw_receipt_public_id": _uuid(9),
                "target_json_pointer": "/fills/1",
                "origin_sidecar_public_id": _uuid(20),
                "origin_sidecar_content_digest": "d" * 64,
                "raw_recorded_at": _TIME + timedelta(seconds=8),
                "recorded_at": _TIME + timedelta(seconds=9),
            }
        )
    )
    assert projection.frame_context == original.frame_context
    assert projection.raw_observed_at == original.raw_observed_at
    assert projection.raw_recorded_at > original.raw_recorded_at
    assert private_source_ownership_digest(projection) != private_source_ownership_digest(original)


def test_flat_projection_matches_all_payload_columns_without_inventing_provenance() -> None:
    """Map the exact payload-derived portion of the prospective 44-column immutable row.

    Given: A coherent safe public shape awaiting actual raw/key/root rebinding.
    When: Its flat physical projection is built.
    Then: It has 39 values, canonical/digest agreement and no invented insert identity or account UID.
    """
    payload = _payload()
    values = private_source_ownership_projection(payload)
    assert len(values) == 39
    assert values["timestamp"] == payload.recorded_at
    assert values["margin_account"] is None
    assert values["canonical_payload"] == canonical_private_source_ownership(payload)
    assert values["content_digest"] == private_source_ownership_digest(payload)
    assert (
        not {"id", "public_id", "session_id", "sequence_id", "known_to", "account_uid"}
        & values.keys()
    )


def test_reference_retains_actual_recorded_clock_and_exact_raw_selector() -> None:
    """Bind immutable sidecar references by exact actual row/source copies without a source-family fallback.

    Given: A shaped reference naming one actual sidecar and selected raw receipt.
    When: Its recording clock is changed while all other copies remain equal.
    Then: The references differ and the bound reader must compare the actual stored clock independently.
    """
    reference = PrivateSourceOwnershipReference(
        public_id=_uuid(20),
        content_digest="d" * 64,
        raw_receipt_public_id=_uuid(8),
        raw_content_digest="b" * 64,
        target_json_pointer="/fills/0",
        recorded_at=_TIME + timedelta(seconds=7),
    )
    assert reference.model_copy(update={"recorded_at": _TIME + timedelta(seconds=8)}) != reference


@pytest.mark.parametrize(
    "field", ["apiKey", "challenge", "signed_challenge", "financial_wire", "account_uid"]
)
def test_safe_payload_refuses_unsupported_auth_wire_or_invented_uid_fields(field: str) -> None:
    """Keep extra private material outside the safe public payload and its hash input.

    Given: A public shape with an unsupported field containing a synthetic private marker.
    When: The complete payload is validated before canonicalization.
    Then: The shape refuses it and ordinary validation text does not expose the marker.
    """
    material = _payload().model_dump(mode="python")
    material[field] = "synthetic-private-marker-only"
    with pytest.raises(ValidationError) as raised:
        PrivateSourceOwnershipPayload.model_validate(material)
    assert "synthetic-private-marker-only" not in str(raised.value)


@pytest.mark.parametrize("pointer", [None, "/new_entry", "/logs/0", "/fills/10"])
def test_reference_preserves_each_supported_selector_shape(pointer: str | None) -> None:
    """References retain a whole frame or canonical child without guessing its actual feed.

    Given: Exact copied source identifiers and one supported canonical selector shape.
    When: The strict safe reference is validated.
    Then: Its selector is preserved for later actual bound-row equality.
    """
    reference = PrivateSourceOwnershipReference(
        public_id=_uuid(20),
        content_digest="d" * 64,
        raw_receipt_public_id=_uuid(8),
        raw_content_digest="b" * 64,
        target_json_pointer=pointer,
        recorded_at=_TIME + timedelta(seconds=7),
    )
    assert reference.target_json_pointer == pointer


@pytest.mark.parametrize("pointer", ["", "/fills/00", "/logs/+1", "/new_entry/0"])
def test_reference_refuses_noncanonical_child_aliases(pointer: str) -> None:
    """A reference cannot preserve an ambiguous path that an actual bound raw row must never use.

    Given: Exact public source copies paired with a malformed child pointer.
    When: The safe reference is validated before financial use.
    Then: Canonical selector validation refuses the alias.
    """
    with pytest.raises(ValidationError):
        PrivateSourceOwnershipReference(
            public_id=_uuid(20),
            content_digest="d" * 64,
            raw_receipt_public_id=_uuid(8),
            raw_content_digest="b" * 64,
            target_json_pointer=pointer,
            recorded_at=_TIME + timedelta(seconds=7),
        )


def test_actual_receive_may_bind_the_complete_original_frame() -> None:
    """Original receive shapes may retain a full frame before selecting any economic child.

    Given: A coherent safe original receive context and its actual raw reference shape.
    When: Its selector denotes the whole retained original frame.
    Then: Canonical validation preserves None without inventing a child identity.
    """
    payload = PrivateSourceOwnershipPayload.model_validate(
        _payload().model_copy(update={"target_json_pointer": None})
    )
    assert payload.target_json_pointer is None
    assert (
        PrivateSourceOwnershipPayload.model_validate_json(
            canonical_private_source_ownership(payload)
        )
        == payload
    )


def test_recovery_cannot_omit_the_selected_child() -> None:
    """A recovery projection requires an explicit child even when its original frame is retained.

    Given: A complete direct-origin tuple and coherent original source clocks.
    When: A projection omits its child selector.
    Then: The safe shape refuses that incomplete recovery relation.
    """
    with pytest.raises(ValidationError, match="explicit selected child"):
        PrivateSourceOwnershipPayload.model_validate(
            _payload().model_copy(
                update={
                    "binding_kind": "recovery_projection",
                    "target_json_pointer": None,
                    "origin_sidecar_public_id": _uuid(20),
                    "origin_sidecar_content_digest": "d" * 64,
                }
            )
        )


@pytest.mark.parametrize(
    ("raw_feed", "pointer"), [("account_log", "/new_entry"), ("account_log_snapshot", "/logs/0")]
)
def test_account_log_shapes_keep_the_original_feed_specific_selector(
    raw_feed: str, pointer: str
) -> None:
    """The account-log stream and snapshot use different original child namespaces.

    Given: A safe account-log subscription shape with its exact raw feed classification.
    When: A matching live-entry or snapshot selector is validated.
    Then: Its original namespace is preserved without relabeling source material.
    """
    original = _payload()
    frame = original.frame_context.model_copy(
        update={"subscription_feed": "account_log", "raw_feed": raw_feed}
    )
    payload = PrivateSourceOwnershipPayload.model_validate(
        original.model_copy(update={"frame_context": frame, "target_json_pointer": pointer})
    )
    assert payload.frame_context.raw_feed == raw_feed
    assert payload.target_json_pointer == pointer


@pytest.mark.parametrize("pointer", ["/logs/0", "/fills/0"])
def test_live_account_log_cannot_borrow_a_snapshot_or_fill_selector(pointer: str) -> None:
    """A familiar canonical pointer is still invalid for another original feed namespace.

    Given: A safe live account-log shape using the exact new-entry source policy.
    When: Its pointer names a snapshot log or a fills child.
    Then: The live-source selector validator refuses the mismatched namespace.
    """
    original = _payload()
    frame = original.frame_context.model_copy(
        update={"subscription_feed": "account_log", "raw_feed": "account_log"}
    )
    with pytest.raises(ValidationError, match="original feed"):
        PrivateSourceOwnershipPayload.model_validate(
            original.model_copy(update={"frame_context": frame, "target_json_pointer": pointer})
        )


def test_canonical_boundary_refuses_unchecked_context_policy_mutation() -> None:
    """Canonical validation rejects an unchecked unsupported policy at its actual public boundary.

    Given: An unchecked public model copy changes only the nested context's source policy.
    When: Canonical serialization revalidates the complete original payload.
    Then: It refuses the nested policy literal without certifying any source ownership.
    """
    original = _payload()
    frame = original.frame_context.model_copy(
        update={"source_policy": "unsupported_private_policy"}
    )
    unchecked = original.model_copy(update={"frame_context": frame})
    with pytest.raises(ValidationError, match="source_policy") as caught:
        canonical_private_source_ownership(unchecked)
    detail = caught.value.errors(include_input=False)[0]
    assert detail["loc"] == ("frame_context", "source_policy")
    assert detail["type"] == "literal_error"
