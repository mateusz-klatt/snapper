"""Exact correction evidence must not lose scope, float bits or version identity."""

import json
from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest
from pydantic import ValidationError

from snapper.core.accrual_correction import AccrualCorrectionDraft
from snapper.core.accrual_correction import AccrualCorrectionScope
from snapper.core.accrual_correction import AccrualVersionEvidence
from snapper.core.accrual_correction import accrual_correction_digest
from snapper.core.accrual_correction import accrual_scope_digest
from snapper.core.accrual_correction import accrual_version_digest
from snapper.core.accrual_correction import canonical_accrual_correction
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance

_AT = datetime(2026, 1, 10, 12, tzinfo=UTC)
_MAX = datetime(9999, 12, 31, 23, 59, 59, tzinfo=UTC)
_WALLET = "00000000-0000-4000-8000-000000000101"
_INSTRUMENT = "00000000-0000-4000-8000-000000000102"
_ACTOR = "00000000-0000-4000-8000-000000000103"
_ROW = "00000000-0000-4000-8000-000000000104"
_SECOND = "00000000-0000-4000-8000-000000000105"


def _scope() -> AccrualCorrectionScope:
    """Return one synthetic instrument scope, not an account-wide authority domain."""
    return AccrualCorrectionScope(
        wallet_public_id=_WALLET, instrument_public_id=_INSTRUMENT, exchange="kraken", mode="live"
    )


def _version() -> AccrualVersionEvidence:
    """Capture one wholly synthetic posted version with exact floating-point operands."""
    return AccrualVersionEvidence(
        physical_id=11,
        public_id=_ROW,
        scope=_scope(),
        timestamp=_AT,
        known_to=_MAX,
        session_id=_ACTOR,
        sequence_id=1,
        operator_public_id=None,
        accrual_type="rollover",
        accrued_at=_AT - timedelta(minutes=1),
        amount=0.1,
        amount_asset="EUR",
        rate=0.01,
        notional=10.0,
        position_quantity_at_accrual=2.0,
    )


def _draft(*targets: AccrualVersionEvidence) -> AccrualCorrectionDraft:
    """Build an intent that grants no production correction authority."""
    return AccrualCorrectionDraft(
        scope=_scope(),
        expected_revision=0,
        actor_public_id=_ACTOR,
        request_key="synthetic-one",
        reason="synthetic erroneous internal estimate",
        evidence_digest="a" * 64,
        provenance=SnapshotWriteProvenance(
            public_id=_SECOND,
            session_id=_ACTOR,
            sequence_id=2,
            timestamp=_AT + timedelta(seconds=1),
        ),
        targets=targets or (_version(),),
    )


def test_canonical_correction_preserves_original_float_bits_and_scope() -> None:
    """The audit freezes exact stored bits without inventing native decimal precision."""
    draft = _draft()
    material = json.loads(canonical_accrual_correction(draft))
    assert material["targets"][0]["amount"] == (0.1).hex()
    assert material["targets"][0]["rate"] == (0.01).hex()
    assert material["targets"][0]["scope"]["instrument_public_id"] == _INSTRUMENT
    assert len(accrual_correction_digest(draft)) == 64


def test_signed_zero_and_every_physical_version_have_distinct_digests() -> None:
    """Numerical equality cannot merge distinct stored binary operands or physical rows."""
    zero = _version().model_copy(update={"amount": 0.0})
    negative_zero = zero.model_copy(update={"amount": -0.0})
    other_physical = zero.model_copy(update={"physical_id": 12})
    assert len({accrual_version_digest(row) for row in (zero, negative_zero, other_physical)}) == 3


def test_target_order_does_not_change_complete_intent_or_its_digest() -> None:
    """The same exact target set remains idempotent when preview ordering changes."""
    first = _version()
    second = first.model_copy(
        update={
            "physical_id": 12,
            "public_id": _SECOND,
            "accrued_at": _AT - timedelta(minutes=2),
        }
    )
    assert canonical_accrual_correction(_draft(first, second)) == canonical_accrual_correction(
        _draft(second, first)
    )
    assert accrual_correction_digest(_draft(first, second)) == accrual_correction_digest(
        _draft(second, first)
    )


def test_duplicate_physical_identity_cannot_choose_two_logical_winners() -> None:
    """One physical source row cannot be represented as two otherwise distinct targets."""
    first = _version()
    contradictory = first.model_copy(
        update={
            "public_id": _SECOND,
            "accrued_at": _AT - timedelta(minutes=2),
        }
    )
    with pytest.raises(ValidationError):
        _draft(first, contradictory)


@pytest.mark.parametrize("value", [float("inf"), float("-inf"), float("nan")])
def test_digest_revalidates_unchecked_nonfinite_source_evidence(value: float) -> None:
    """An unchecked model copy must not make nonfinite stored evidence look certified."""
    unchecked = _version().model_copy(update={"amount": value})
    with pytest.raises(ValidationError):
        accrual_version_digest(unchecked)


def test_canonical_intent_revalidates_unchecked_foreign_target() -> None:
    """A model-copy shortcut cannot bypass the full target-set ownership proof."""
    foreign = _version().model_copy(
        update={"scope": _scope().model_copy(update={"exchange": "walutomat"})}
    )
    unchecked = _draft().model_copy(update={"targets": (foreign,)})
    with pytest.raises(ValidationError):
        canonical_accrual_correction(unchecked)


@pytest.mark.parametrize(
    "field,value",
    [
        ("mode", "paper"),
        ("exchange", "walutomat"),
        ("wallet_public_id", _SECOND),
        ("instrument_public_id", _SECOND),
    ],
)
def test_sibling_scopes_never_share_a_revision_digest(field: str, value: str) -> None:
    """Unrelated instruments and account domains retain independent revision barriers."""
    assert accrual_scope_digest(_scope()) != accrual_scope_digest(
        _scope().model_copy(update={field: value})
    )


@pytest.mark.parametrize(
    "change", ["same_clock", "foreign_scope", "already_voided", "same_logical", "same_boundary"]
)
def test_invalid_target_sets_refuse_without_selecting_a_winner(change: str) -> None:
    """Clock skew, foreign rows and duplicate historical identities cannot be corrected blindly."""
    first = _version()
    second = first.model_copy(
        update={"physical_id": 12, "public_id": _SECOND, "accrued_at": _AT - timedelta(minutes=2)}
    )
    if change == "same_clock":
        first = first.model_copy(update={"timestamp": _AT + timedelta(seconds=1)})
    elif change == "foreign_scope":
        first = first.model_copy(
            update={"scope": _scope().model_copy(update={"exchange": "walutomat"})}
        )
    elif change == "already_voided":
        first = first.model_copy(
            update={"record_status": "voided", "correction_public_id": _SECOND, "amount": 0.0}
        )
    elif change == "same_logical":
        second = second.model_copy(update={"public_id": first.public_id})
    else:
        second = second.model_copy(update={"accrued_at": first.accrued_at})
    with pytest.raises(ValidationError):
        _draft(first, second)
