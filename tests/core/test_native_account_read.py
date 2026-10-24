"""Native account request ordering and outcome chronology remain distinct from account authority."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest

from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.native_account_read import NativeAccountReadAttemptPayload
from snapper.core.native_account_read import NativeAccountReadCapturedSource
from snapper.core.native_account_read import NativeAccountReadFailure
from snapper.core.native_account_read import NativeAccountReadOutcomePayload
from snapper.core.native_account_read import canonical_native_account_read_attempt
from snapper.core.native_account_read import canonical_native_account_read_outcome
from snapper.core.native_account_read import native_account_read_attempt_digest
from snapper.core.native_account_read import native_account_read_family_digest
from snapper.core.native_account_read import native_account_read_outcome_digest

_NOW = datetime(2026, 10, 23, 12, tzinfo=UTC)


def _id(number: int) -> str:
    """Keep request and source fixture UUIDs distinct and canonical."""
    return f"00000000-0000-7000-8000-{number:012d}"


def _selection(**updates: object) -> ExecutionClientSelection:
    """Select one actual public credential revision without exposing authentication material."""
    return ExecutionClientSelection.model_validate(
        {
            "wallet_public_id": _id(1),
            "exchange": "kraken_futures",
            "mode": "live",
            "environment": "production",
            "credential_public_id": _id(2),
            "credential_timestamp": _NOW - timedelta(days=1),
            "public_api_origin": "https://futures.kraken.com",
            **updates,
        }
    )


def _attempt(**updates: object) -> NativeAccountReadAttemptPayload:
    """Provide a committed-request-shaped value, without claiming a database row exists."""
    return NativeAccountReadAttemptPayload.model_validate(
        {"client_selection": _selection(), "family_sequence": 1, "started_at": _NOW, **updates}
    )


def _source(**updates: object) -> NativeAccountReadCapturedSource:
    """Reference exact retained source identity and separately timed local storage."""
    return NativeAccountReadCapturedSource.model_validate(
        {
            "raw_receipt_public_id": _id(3),
            "raw_content_digest": "a" * 64,
            "request_started_at": _NOW + timedelta(milliseconds=500),
            "source_observed_at": _NOW + timedelta(seconds=1),
            "source_recorded_at": _NOW + timedelta(seconds=2),
            **updates,
        }
    )


def _outcome(**updates: object) -> NativeAccountReadOutcomePayload:
    """Bind the exact original attempt while leaving current/latest selection to the store."""
    attempt = _attempt()
    return NativeAccountReadOutcomePayload.model_validate(
        {
            "attempt_public_id": _id(4),
            "attempt_content_digest": native_account_read_attempt_digest(attempt),
            "attempt": attempt,
            "completed_at": _NOW + timedelta(seconds=3),
            "result": _source(),
            **updates,
        }
    )


def test_request_and_outcome_roundtrip_under_distinct_hash_domains() -> None:
    """Original request and retained outcome remain independent immutable facts.

    Given: An ordered request and its separately recorded original source,
    When: Both payloads are canonicalized and replayed,
    Then: Exact identities and clocks survive while their dedicated digests remain distinct.
    """
    attempt, outcome = _attempt(), _outcome()
    assert (
        NativeAccountReadAttemptPayload.model_validate_json(
            canonical_native_account_read_attempt(attempt)
        )
        == attempt
    )
    assert (
        NativeAccountReadOutcomePayload.model_validate_json(
            canonical_native_account_read_outcome(outcome)
        )
        == outcome
    )
    assert native_account_read_attempt_digest(attempt) != native_account_read_outcome_digest(
        outcome
    )


def test_credential_rotation_does_not_fork_read_ordering_but_environment_does() -> None:
    """A newer selected credential shares the original account family's latest-read barrier.

    Given: Rotated credentials, another wallet and the real demo endpoint,
    When: Their coarse request-ordering families are compared,
    Then: Only credential rotation retains the same family digest.
    """
    original = _selection()
    rotated = _selection(credential_public_id=_id(8), credential_timestamp=_NOW)
    demo = _selection(environment="demo", public_api_origin="https://demo-futures.kraken.com")
    other_wallet = _selection(wallet_public_id=_id(9))
    assert native_account_read_family_digest(original) == native_account_read_family_digest(rotated)
    assert native_account_read_family_digest(original) != native_account_read_family_digest(demo)
    assert native_account_read_family_digest(original) != native_account_read_family_digest(
        other_wallet
    )
    assert native_account_read_attempt_digest(_attempt()) != native_account_read_attempt_digest(
        _attempt(client_selection=rotated)
    )
    assert _attempt(client_selection=demo).client_selection.environment == "demo"


@pytest.mark.parametrize("value", (True, False, 1.0, "1", 0, 2))
def test_schema_version_aliases_cannot_change_request_or_outcome_identity(value: object) -> None:
    """Schema discriminators cannot enter the immutable hash through coercion.

    Given: A boolean, float, text or unsupported integer version,
    When: Either immutable request or outcome is constructed,
    Then: The unsupported identity is refused.
    """
    with pytest.raises(ValueError):
        _attempt(schema_version=value)
    with pytest.raises(ValueError):
        _outcome(schema_version=value)


@pytest.mark.parametrize("value", (True, 1.0, "1", 0, -1))
def test_request_family_ordinal_requires_a_real_positive_integer(value: object) -> None:
    """A malformed ordinal cannot masquerade as durable request ordering.

    Given: A coerced or nonpositive family sequence,
    When: A native account request is validated,
    Then: It is refused without a synthetic default ordinal.
    """
    with pytest.raises(ValueError):
        _attempt(family_sequence=value)


@pytest.mark.parametrize(
    "updates",
    (
        {"exchange": "kraken"},
        {"public_api_origin": "https://different.invalid"},
        {"credential_timestamp": _NOW + timedelta(seconds=1)},
        {
            "exchange": "paper",
            "mode": "paper",
            "environment": "paper",
            "credential_public_id": None,
            "credential_timestamp": None,
            "public_api_origin": None,
        },
    ),
)
def test_unsupported_or_future_client_cannot_create_native_request(
    updates: dict[str, object],
) -> None:
    """The request proves the actual supported selected client before any source can be attached.

    Given: Another venue, unsupported endpoint, Paper or a future credential revision,
    When: A native account request is constructed,
    Then: No supported authenticated read is inferred.
    """
    with pytest.raises(ValueError):
        _attempt(client_selection=_selection(**updates))


@pytest.mark.parametrize(
    "reason", ("transport_error", "cancelled", "capture_failed", "source_invalid", "http_error")
)
def test_failed_outcome_needs_no_fabricated_source(reason: str) -> None:
    """A failed attempt remains explicit even when no provider response was captured.

    Given: A supported transport, cancellation, capture or source refusal,
    When: Its original attempt is completed,
    Then: The outcome retains the reason without requiring a false raw receipt UUID.
    """
    failure = NativeAccountReadFailure.model_validate({"reason": reason})
    outcome = _outcome(result=failure)
    assert outcome.result == failure
    assert "raw_receipt_public_id" not in canonical_native_account_read_outcome(outcome)


def test_failed_interpretation_retains_its_original_source_reference() -> None:
    """A captured error body remains traceable without becoming a successful account read.

    Given: A real source reference whose financial interpretation failed,
    When: The attempt's failure outcome is retained,
    Then: Its exact raw UUID survives while the result remains explicitly failed.
    """
    source = _source()
    outcome = _outcome(result=NativeAccountReadFailure(reason="source_invalid", source=source))
    assert outcome.result.kind == "failed"
    assert outcome.source_reference == source
    assert _id(3) in canonical_native_account_read_outcome(outcome)


@pytest.mark.parametrize("change", ("attempt", "completion", "request", "recording"))
def test_outcome_cannot_rewrite_attempt_or_knowledge_interval(change: str) -> None:
    """Result arrival cannot repair contradictory request identity or source chronology.

    Given: A changed request, early completion, premature network start or late recording,
    When: The original outcome is validated,
    Then: The contradictory result remains unavailable as account evidence.
    """
    updates: dict[str, object]
    if change == "attempt":
        updates = {"attempt": _attempt(family_sequence=2)}
    elif change == "completion":
        updates = {"completed_at": _NOW - timedelta(seconds=1)}
    elif change == "request":
        updates = {"result": _source(request_started_at=_NOW - timedelta(seconds=1))}
    else:
        updates = {"result": _source(source_recorded_at=_NOW + timedelta(seconds=4))}
    with pytest.raises(ValueError):
        _outcome(**updates)


def test_source_recording_cannot_precede_original_capture() -> None:
    """Source storage cannot invent earlier knowledge by reversing its clocks.

    Given: A source observed after its claimed durable recording time,
    When: The captured source reference is validated,
    Then: The original chronology is refused.
    """
    with pytest.raises(ValueError):
        _source(source_recorded_at=_NOW)


def test_canonicalizers_revalidate_unchecked_instances_and_reject_money() -> None:
    """An unchecked object or monetary instruction cannot enter a public read-evidence hash.

    Given: Mutated request/outcome instances and an unexpected spendable amount,
    When: The immutable payload boundaries are used,
    Then: Each unsupported object is rejected before canonical storage.
    """
    with pytest.raises(ValueError):
        canonical_native_account_read_attempt(
            _attempt().model_copy(update={"family_sequence": True})
        )
    with pytest.raises(ValueError):
        canonical_native_account_read_outcome(
            _outcome().model_copy(update={"attempt_content_digest": "b" * 64})
        )
    with pytest.raises(ValueError):
        _outcome(available_collateral="1000")
