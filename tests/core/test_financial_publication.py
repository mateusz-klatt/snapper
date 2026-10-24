"""Immutable accounting message identity and honest local-versus-venue chronology."""

import hashlib
import json
from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest
from pydantic import ValidationError

from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.core.financial_publication import ContractPositionPublication
from snapper.core.financial_publication import canonical_financial_publication
from snapper.core.financial_publication import financial_publication_digest
from tests.helpers.unit_admission import admission_id

_NOW = datetime(2026, 10, 23, 10, tzinfo=UTC)


@pytest.fixture
def payload() -> ContractPositionPublication:
    """Build a structural message example without claiming these identities exist in storage."""
    return ContractPositionPublication(
        scope=ExecutionAccountScope(
            wallet_public_id=admission_id(1),
            exchange="kraken_futures",
            mode="live",
            environment="production",
            margin_account="actual-profile",
        ),
        source_execution_id="venue-fill-1",
        execution_public_id=admission_id(2),
        venue_event_public_id=admission_id(3),
        unit_snapshot_public_id=admission_id(4),
        position_event_public_id=admission_id(5),
        instrument_public_id=admission_id(6),
        raw_receipt_public_id=admission_id(7),
        route_authority_public_id=admission_id(8),
        ledger_sequence=1,
        executed_at=_NOW - timedelta(seconds=1),
        execution_known_at=_NOW,
        recorded_at=_NOW,
    )


def test_notification_roundtrips_all_references_under_a_dedicated_hash_domain(
    payload: ContractPositionPublication,
) -> None:
    """All reference identity survives.

    Given: an immutable message,
    When: encoded and replayed,
    Then: all reference identity survives.
    """
    encoded = canonical_financial_publication(payload)
    assert ContractPositionPublication.model_validate_json(encoded) == payload
    assert list(json.loads(encoded)) == sorted(json.loads(encoded))
    assert (
        financial_publication_digest(payload)
        == hashlib.sha256(b"snapper:financial-publication:v1\x00" + encoded.encode()).hexdigest()
    )
    changed = ContractPositionPublication.model_validate(
        {**payload.model_dump(mode="python"), "raw_receipt_public_id": admission_id(9)}
    )
    assert financial_publication_digest(changed) != financial_publication_digest(payload)


@pytest.mark.parametrize("field", ["schema_version", "ledger_sequence"])
@pytest.mark.parametrize("value", [True, 1.0, "1", None])
def test_integer_identity_cannot_be_replaced_by_coerced_aliases(
    payload: ContractPositionPublication, field: str, value: object
) -> None:
    """No coercion changes the canonical fact.

    Given: an identity integer alias,
    When: validating,
    Then: no coercion changes the canonical fact.
    """
    with pytest.raises(ValidationError, match="require integers"):
        ContractPositionPublication.model_validate(
            {**payload.model_dump(mode="python"), field: value}
        )


@pytest.mark.parametrize("value", [0, -1])
def test_nonpositive_ledger_sequence_cannot_become_a_recording_notification(
    payload: ContractPositionPublication, value: int
) -> None:
    """No apparent financial progress is emitted.

    Given: an absent ledger ordinal,
    When: validating,
    Then: no apparent financial progress is emitted.
    """
    with pytest.raises(ValidationError):
        ContractPositionPublication.model_validate(
            {**payload.model_dump(mode="python"), "ledger_sequence": value}
        )


def test_publication_cannot_predate_the_actual_local_execution_knowledge(
    payload: ContractPositionPublication,
) -> None:
    """The event cannot misstate knowledge.

    Given: a backdated local record,
    When: validating,
    Then: the event cannot misstate knowledge.
    """
    with pytest.raises(ValidationError, match="predates"):
        ContractPositionPublication.model_validate(
            {**payload.model_dump(mode="python"), "recorded_at": _NOW - timedelta(microseconds=1)}
        )


def test_venue_clock_skew_remains_separate_from_local_record_chronology(
    payload: ContractPositionPublication,
) -> None:
    """Its original instant is retained.

    Given: a venue clock ahead of local time,
    When: recording,
    Then: its original instant is retained.
    """
    skewed = ContractPositionPublication.model_validate(
        {**payload.model_dump(mode="python"), "executed_at": _NOW + timedelta(minutes=1)}
    )
    assert skewed.executed_at > skewed.recorded_at
    assert skewed.execution_known_at == payload.execution_known_at


def test_canonicalizer_revalidates_constructed_values_and_refuses_money_instructions(
    payload: ContractPositionPublication,
) -> None:
    """Neither passes as metadata.

    Given: a forged instance or executable amount,
    When: canonicalized,
    Then: neither passes as metadata.
    """
    forged = payload.model_copy(update={"ledger_sequence": True})
    with pytest.raises(ValidationError, match="require integers"):
        canonical_financial_publication(forged)
    with pytest.raises(ValidationError, match="Extra inputs"):
        ContractPositionPublication.model_validate(
            {**payload.model_dump(mode="python"), "cash_delta": 1000}
        )
