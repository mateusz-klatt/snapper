"""Pure product request/result contracts remain distinct from actual transport and account authority."""

import base64
import hashlib
from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest
from pydantic import ValidationError

from snapper.core.futures_product_read import FuturesProductReadAttempt
from snapper.core.futures_product_read import FuturesProductReadFailure
from snapper.core.futures_product_read import FuturesProductReadOutcome
from snapper.core.futures_product_read import FuturesProductReadSource
from snapper.core.futures_product_read import ProductReadKeyReference
from snapper.core.futures_product_read import canonical_futures_product_attempt
from snapper.core.futures_product_read import canonical_futures_product_outcome
from snapper.core.futures_product_read import futures_product_attempt_digest
from snapper.core.futures_product_read import futures_product_outcome_digest
from snapper.core.futures_product_read import futures_product_read_family
from snapper.core.native_account_read import native_account_read_family_digest
from tests.core.test_native_account_read import _id
from tests.core.test_native_account_read import _selection

_NOW = datetime(2026, 10, 23, 19, tzinfo=UTC)
_ENTITY = b'{"result":"success","serverTime":"2026-10-23T19:00:01Z","instruments":[]}'


def _key(**updates: object) -> ProductReadKeyReference:
    """Provide public shape-only proof coordinates without pretending an actual key check exists."""
    return ProductReadKeyReference.model_validate(
        {
            "public_id": _id(10),
            "content_digest": "a" * 64,
            "read_attempt_public_id": _id(11),
            "account_uid": _id(12),
            "client_selection": _selection(),
            "key_owner_root_public_id": _id(10),
            "key_owner_root_content_digest": "a" * 64,
            "observed_at": _NOW - timedelta(seconds=2),
            "recorded_at": _NOW - timedelta(seconds=1),
            **updates,
        }
    )


def _attempt(**updates: object) -> FuturesProductReadAttempt:
    """Build an internally coherent request without claiming actual ordering or commit."""
    return FuturesProductReadAttempt.model_validate(
        {
            "client_selection": _selection(),
            "key_proof": _key(),
            "family_sequence": 1,
            "started_at": _NOW,
            **updates,
        }
    )


def _source(**updates: object) -> FuturesProductReadSource:
    """Retain exact illustrative bytes and their original request/capture clocks."""
    return FuturesProductReadSource.model_validate(
        {
            "body": base64.b64encode(_ENTITY).decode("ascii"),
            "request_started_at": _NOW,
            "observed_at": _NOW + timedelta(seconds=1),
            **updates,
        }
    )


def _outcome(**updates: object) -> FuturesProductReadOutcome:
    """Bind one original request while leaving current selection and source authenticity unproven."""
    attempt = _attempt()
    return FuturesProductReadOutcome.model_validate(
        {
            "attempt_public_id": _id(13),
            "attempt_content_digest": futures_product_attempt_digest(attempt),
            "attempt": attempt,
            "completed_at": _NOW + timedelta(seconds=2),
            "result": _source(),
            **updates,
        }
    )


def test_original_product_attempt_and_result_roundtrip_in_distinct_versioned_domains() -> None:
    """Preserve exact product bytes without making the record a financial receipt or accounts read.

    Given: A shape-only product attempt and a separately timed successful original entity.
    When: Both records are serialized, rebound and hashed under their documented domains.
    Then: Original clocks and bytes survive, and their digests cannot use the accounts-read domain.
    """
    attempt, outcome = _attempt(), _outcome()
    request_json = canonical_futures_product_attempt(attempt)
    result_json = canonical_futures_product_outcome(outcome)
    assert FuturesProductReadAttempt.model_validate_json(request_json) == attempt
    assert FuturesProductReadOutcome.model_validate_json(result_json) == outcome
    assert (
        futures_product_attempt_digest(attempt)
        == hashlib.sha256(
            b"snapper:futures-product-read-attempt:v1\x00" + request_json.encode()
        ).hexdigest()
    )
    assert (
        futures_product_outcome_digest(outcome)
        == hashlib.sha256(
            b"snapper:futures-product-read-outcome:v1\x00" + result_json.encode()
        ).hexdigest()
    )
    assert (
        futures_product_attempt_digest(attempt)
        != hashlib.sha256(
            b"snapper:native-account-read-attempt:v1\x00" + request_json.encode()
        ).hexdigest()
    )
    assert isinstance(outcome.result, FuturesProductReadSource)
    assert base64.b64decode(outcome.result.body) == _ENTITY
    assert not {"available_funds", "order_permission", "raw_receipt_public_id"} & set(
        outcome.model_dump(mode="json")
    )


def test_product_ordering_family_spans_credential_rotation_without_becoming_accounts_ordering() -> (
    None
):
    """Keep product ordinals in their own table/purpose while sharing the coarse account identity.

    Given: Two selected credentials name the same wallet, venue, mode and environment.
    When: Their product ordering families are computed.
    Then: They share the coarse family while each product attempt retains its explicit separate purpose.
    """
    selected = _selection()
    rotated = _selection(credential_public_id=_id(99))
    assert futures_product_read_family(selected) == futures_product_read_family(rotated)
    assert futures_product_read_family(selected) == native_account_read_family_digest(selected)
    assert _attempt().purpose == "kraken_futures.trading_instruments"


@pytest.mark.parametrize("value", [True, 1.0, "1", 2])
def test_version_aliases_cannot_relabel_a_product_request_or_result(value: object) -> None:
    """Require exact supported integer versions at both immutable boundaries.

    Given: A caller supplies a Boolean, float, text or unsupported version.
    When: A product attempt and result are constructed.
    Then: Both independently refuse the version before canonical evidence can be formed.
    """
    with pytest.raises(ValidationError):
        _attempt(schema_version=value)
    with pytest.raises(ValidationError):
        _outcome(schema_version=value)


@pytest.mark.parametrize(
    "updates",
    [
        {"exchange": "kraken", "public_api_origin": "https://api.kraken.com"},
        {"mode": "paper"},
        {"environment": "unproven"},
        {"public_api_origin": "https://demo-futures.kraken.com"},
    ],
)
def test_request_cannot_borrow_a_different_route_or_unsupported_source(
    updates: dict[str, object],
) -> None:
    """Refuse unsupported or inconsistent public selection before any product attempt exists.

    Given: A copied selected route differs from its supported source or original safe proof.
    When: The product request boundary validates that material.
    Then: It refuses without silently translating environments or assuming account equivalence.
    """
    with pytest.raises(ValidationError):
        _attempt(client_selection=_selection(**updates))


@pytest.mark.parametrize("field", ["observed_at", "recorded_at"])
def test_key_proof_must_be_known_before_the_actual_product_attempt(field: str) -> None:
    """Keep future identity evidence from appearing as already available at request start.

    Given: A copied proof moves observation or recording beyond the original attempt clock.
    When: The complete request is validated.
    Then: Its proof chronology refuses the future reference.
    """
    with pytest.raises(ValidationError):
        _attempt(key_proof=_key(**{field: _NOW + timedelta(seconds=1)}))


@pytest.mark.parametrize("body", ["!", "Zg=", "Zh==", "", "===="])
def test_invalid_or_noncanonical_original_entity_encoding_is_rejected(body: str) -> None:
    """Avoid multiple text identities for the same source bytes or empty capture claims.

    Given: An original entity uses invalid, noncanonical or empty base64.
    When: The product source shape is validated.
    Then: It refuses instead of changing the retained entity representation.
    """
    with pytest.raises(ValidationError):
        _source(body=body)


@pytest.mark.parametrize("status", [199, 201, 403, True, 200.0, "200"])
def test_captured_product_entity_requires_a_strict_successful_http_status(status: object) -> None:
    """Preserve successful source semantics without coercing a failed or aliased status code.

    Given: A response status differs from strict integer200.
    When: A captured original product source is constructed.
    Then: It refuses that status; failure metadata belongs to a separate result variant.
    """
    with pytest.raises(ValidationError):
        _source(status_code=status)


def test_original_request_observation_and_completion_clocks_cannot_be_reversed() -> None:
    """Keep each local knowledge edge ordered independently of later source freshness decisions.

    Given: A source or outcome places an observation before its request or after completion.
    When: The strict source and result boundaries validate those copies.
    Then: They refuse reversed chronology without replacing any clock with now.
    """
    with pytest.raises(ValidationError):
        _source(observed_at=_NOW - timedelta(seconds=1))
    with pytest.raises(ValidationError):
        _outcome(completed_at=_NOW - timedelta(seconds=1))
    with pytest.raises(ValidationError):
        _outcome(completed_at=_NOW)
    with pytest.raises(ValidationError):
        _outcome(result=_source(request_started_at=_NOW - timedelta(seconds=1)))


@pytest.mark.parametrize("extra_bytes", [0, 1, 2])
def test_decoded_entity_size_is_bounded_independently_of_base64_padding(extra_bytes: int) -> None:
    """Keep the exact decoded-byte bound even when a longer payload has the same base64 length.

    Given: A valid JSON entity is padded to the exact eight-MiB limit or one or two bytes beyond it.
    When: Its canonical base64 is validated as an original product source.
    Then: The exact boundary survives and both larger entities are refused.
    """
    size = 8 * 1024 * 1024 + extra_bytes
    body = base64.b64encode(_ENTITY + b" " * (size - len(_ENTITY))).decode("ascii")
    if extra_bytes:
        with pytest.raises(ValidationError):
            _source(body=body)
    else:
        assert _source(body=body).body == body


@pytest.mark.parametrize(
    "updates",
    [
        {"credential_public_id": None, "credential_timestamp": None},
        {"credential_timestamp": _NOW + timedelta(seconds=1)},
    ],
)
def test_missing_or_not_yet_known_selected_credential_cannot_support_a_product_attempt(
    updates: dict[str, object],
) -> None:
    """Require original selected credential knowledge before using its safe-key reference.

    Given: A self-consistent selection/proof pair omits credentials or puts their revision in the future.
    When: A product attempt is constructed from those public shapes.
    Then: It refuses the unknown credential chronology independently of selection equality.
    """
    with pytest.raises(ValidationError):
        selected = _selection(**updates)
        _attempt(client_selection=selected, key_proof=_key(client_selection=selected))


def test_changed_attempt_digest_or_unchecked_nested_copy_cannot_preserve_a_result() -> None:
    """Rebind exact request identity and nested source values before canonicalization.

    Given: A result names a false request digest or copies an invalid successful source status.
    When: Construction or canonical serialization revalidates the complete graph.
    Then: It refuses the changed claim rather than accepting already-constructed model shape.
    """
    with pytest.raises(ValidationError):
        _outcome(attempt_content_digest="f" * 64)
    source = _source().model_copy(update={"status_code": 200.0})
    with pytest.raises(ValidationError):
        canonical_futures_product_outcome(_outcome().model_copy(update={"result": source}))


def test_failed_product_read_retains_only_fixed_metadata_and_original_request() -> None:
    """Keep an unsafe or missing response from becoming a fabricated source or secret-bearing error.

    Given: A real producer would need to record a failed original product attempt.
    When: The public failed shape is represented with a fixed source-unsafe category.
    Then: Its original attempt survives while no response body, credentials or permission appears.
    """
    failed = _outcome(result=FuturesProductReadFailure(reason="source_unsafe"))
    assert failed.result.model_dump(mode="json") == {"kind": "failed", "reason": "source_unsafe"}
    with pytest.raises(ValidationError):
        FuturesProductReadFailure.model_validate(
            {"reason": "source_unsafe", "body": "synthetic-sensitive-source"}
        )
