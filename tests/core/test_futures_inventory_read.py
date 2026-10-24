"""Pure inventory request/result contracts remain distinct from actual transport and account authority."""

import base64
import hashlib
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import cast

import pytest
from pydantic import ValidationError

from snapper.core.futures_inventory_read import FuturesInventoryReadAttempt
from snapper.core.futures_inventory_read import FuturesInventoryReadFailure
from snapper.core.futures_inventory_read import FuturesInventoryReadOutcome
from snapper.core.futures_inventory_read import FuturesInventoryReadSource
from snapper.core.futures_inventory_read import InventoryReadKeyReference
from snapper.core.futures_inventory_read import InventoryReadPurpose
from snapper.core.futures_inventory_read import canonical_futures_inventory_attempt
from snapper.core.futures_inventory_read import canonical_futures_inventory_outcome
from snapper.core.futures_inventory_read import futures_inventory_attempt_digest
from snapper.core.futures_inventory_read import futures_inventory_outcome_digest
from snapper.core.futures_inventory_read import futures_inventory_read_family
from snapper.core.native_account_read import native_account_read_family_digest
from tests.core.test_native_account_read import _id
from tests.core.test_native_account_read import _selection

_NOW = datetime(2026, 10, 23, 19, tzinfo=UTC)
_ENTITY = b'{"result":"success","serverTime":"2026-10-23T19:00:01Z","openOrders":[]}'


def _key(**updates: object) -> InventoryReadKeyReference:
    """Provide public shape-only proof coordinates without pretending an actual key check exists."""
    return InventoryReadKeyReference.model_validate(
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


def _attempt(**updates: object) -> FuturesInventoryReadAttempt:
    """Build an internally coherent request without claiming actual ordering or commit."""
    return FuturesInventoryReadAttempt.model_validate(
        {
            "purpose": "kraken_futures.open_orders",
            "client_selection": _selection(),
            "key_proof": _key(),
            "family_sequence": 1,
            "started_at": _NOW,
            **updates,
        }
    )


def _source(**updates: object) -> FuturesInventoryReadSource:
    """Retain exact illustrative bytes and their original request/capture clocks."""
    return FuturesInventoryReadSource.model_validate(
        {
            "parser_policy": "kraken_futures_open_orders_v1",
            "body": base64.b64encode(_ENTITY).decode("ascii"),
            "request_started_at": _NOW,
            "observed_at": _NOW + timedelta(seconds=1),
            **updates,
        }
    )


def _outcome(**updates: object) -> FuturesInventoryReadOutcome:
    """Bind one original request while leaving current selection and source authenticity unproven."""
    attempt = _attempt()
    return FuturesInventoryReadOutcome.model_validate(
        {
            "attempt_public_id": _id(13),
            "attempt_content_digest": futures_inventory_attempt_digest(attempt),
            "attempt": attempt,
            "completed_at": _NOW + timedelta(seconds=2),
            "result": _source(),
            **updates,
        }
    )


def test_original_inventory_attempt_and_result_roundtrip_in_distinct_versioned_domains() -> None:
    """Preserve exact inventory bytes without making the record a financial receipt or accounts read.

    Given: A shape-only inventory attempt and a separately timed successful original entity.
    When: Both records are serialized, rebound and hashed under their documented domains.
    Then: Original clocks and bytes survive, and their digests cannot use the accounts-read domain.
    """
    attempt, outcome = _attempt(), _outcome()
    request_json = canonical_futures_inventory_attempt(attempt)
    result_json = canonical_futures_inventory_outcome(outcome)
    assert FuturesInventoryReadAttempt.model_validate_json(request_json) == attempt
    assert FuturesInventoryReadOutcome.model_validate_json(result_json) == outcome
    assert (
        futures_inventory_attempt_digest(attempt)
        == hashlib.sha256(
            b"snapper:futures-inventory-read-attempt:v1\x00" + request_json.encode()
        ).hexdigest()
    )
    assert (
        futures_inventory_outcome_digest(outcome)
        == hashlib.sha256(
            b"snapper:futures-inventory-read-outcome:v1\x00" + result_json.encode()
        ).hexdigest()
    )
    assert (
        futures_inventory_attempt_digest(attempt)
        != hashlib.sha256(
            b"snapper:native-account-read-attempt:v1\x00" + request_json.encode()
        ).hexdigest()
    )
    assert isinstance(outcome.result, FuturesInventoryReadSource)
    assert base64.b64decode(outcome.result.body) == _ENTITY
    assert not {"available_funds", "order_permission", "raw_receipt_public_id"} & set(
        outcome.model_dump(mode="json")
    )


def test_inventory_ordering_family_spans_rotation_but_separates_endpoint_purposes() -> None:
    """Keep each endpoint's latest-request barrier independent without evading rotation ordering.

    Given: Two selected credentials name the same coarse account and both inventory endpoints.
    When: Their original purpose-scoped ordering families are computed.
    Then: Rotation keeps one family, but another endpoint and accounts reads have distinct families.
    """
    selected = _selection()
    rotated = _selection(credential_public_id=_id(99))
    orders = futures_inventory_read_family(selected, "kraken_futures.open_orders")
    positions = futures_inventory_read_family(selected, "kraken_futures.open_positions")
    assert orders == futures_inventory_read_family(rotated, "kraken_futures.open_orders")
    assert positions == futures_inventory_read_family(rotated, "kraken_futures.open_positions")
    assert orders != positions
    assert orders != native_account_read_family_digest(selected)
    assert positions != native_account_read_family_digest(selected)
    with pytest.raises(ValueError, match="Inventory read purpose is unsupported"):
        futures_inventory_read_family(
            selected, cast(InventoryReadPurpose, "kraken_futures.trading_instruments")
        )


@pytest.mark.parametrize("value", [True, 1.0, "1", 2])
def test_version_aliases_cannot_relabel_a_inventory_request_or_result(value: object) -> None:
    """Require exact supported integer versions at both immutable boundaries.

    Given: A caller supplies a Boolean, float, text or unsupported version.
    When: A inventory attempt and result are constructed.
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
    """Refuse unsupported or inconsistent public selection before any inventory attempt exists.

    Given: A copied selected route differs from its supported source or original safe proof.
    When: The inventory request boundary validates that material.
    Then: It refuses without silently translating environments or assuming account equivalence.
    """
    with pytest.raises(ValidationError):
        _attempt(client_selection=_selection(**updates))


@pytest.mark.parametrize("field", ["observed_at", "recorded_at"])
def test_key_proof_must_be_known_before_the_actual_inventory_attempt(field: str) -> None:
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
    When: The inventory source shape is validated.
    Then: It refuses instead of changing the retained entity representation.
    """
    with pytest.raises(ValidationError):
        _source(body=body)


@pytest.mark.parametrize("status", [199, 201, 403, True, 200.0, "200"])
def test_captured_inventory_entity_requires_a_strict_successful_http_status(status: object) -> None:
    """Preserve successful source semantics without coercing a failed or aliased status code.

    Given: A response status differs from strict integer200.
    When: A captured original inventory source is constructed.
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
    When: Its canonical base64 is validated as an original inventory source.
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
def test_missing_or_not_yet_known_selected_credential_cannot_support_a_inventory_attempt(
    updates: dict[str, object],
) -> None:
    """Require original selected credential knowledge before using its safe-key reference.

    Given: A self-consistent selection/proof pair omits credentials or puts their revision in the future.
    When: A inventory attempt is constructed from those public shapes.
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
        canonical_futures_inventory_outcome(_outcome().model_copy(update={"result": source}))


def test_failed_inventory_read_retains_only_fixed_metadata_and_original_request() -> None:
    """Keep an unsafe or missing response from becoming a fabricated source or secret-bearing error.

    Given: A real producer would need to record a failed original inventory attempt.
    When: The public failed shape is represented with a fixed source-unsafe category.
    Then: Its original attempt survives while no response body, credentials or permission appears.
    """
    failed = _outcome(result=FuturesInventoryReadFailure(reason="source_unsafe"))
    assert failed.result.model_dump(mode="json") == {"kind": "failed", "reason": "source_unsafe"}
    with pytest.raises(ValidationError):
        FuturesInventoryReadFailure.model_validate(
            {"reason": "source_unsafe", "body": "synthetic-sensitive-source"}
        )


@pytest.mark.parametrize("environment", ["production", "demo"])
@pytest.mark.parametrize("purpose", ["kraken_futures.open_orders", "kraken_futures.open_positions"])
def test_both_real_environments_and_endpoint_policies_preserve_distinct_requests(
    environment: str, purpose: InventoryReadPurpose
) -> None:
    """Bind each actual supported endpoint and environment without silently dropping production.

    Given: Consistent public proof shapes for production or demo and either fixed inventory purpose.
    When: An original request and matching endpoint result are validated and canonically rebound.
    Then: The literal selection and purpose survive without becoming an atomic account snapshot.
    """
    origin = (
        "https://futures.kraken.com"
        if environment == "production"
        else "https://demo-futures.kraken.com"
    )
    selection = _selection(environment=environment, public_api_origin=origin)
    attempt = _attempt(
        purpose=purpose, client_selection=selection, key_proof=_key(client_selection=selection)
    )
    policy = (
        "kraken_futures_open_orders_v1"
        if purpose == "kraken_futures.open_orders"
        else "kraken_futures_open_positions_v1"
    )
    entity = (
        _ENTITY
        if purpose == "kraken_futures.open_orders"
        else _ENTITY.replace(b'"openOrders"', b'"openPositions"')
    )
    source = _source(parser_policy=policy, body=base64.b64encode(entity).decode("ascii"))
    outcome = _outcome(
        attempt=attempt,
        attempt_content_digest=futures_inventory_attempt_digest(attempt),
        result=source,
    )
    restored = FuturesInventoryReadOutcome.model_validate_json(
        canonical_futures_inventory_outcome(outcome)
    )
    assert restored.attempt.client_selection.environment == environment
    assert restored.attempt.purpose == purpose
    assert isinstance(restored.result, FuturesInventoryReadSource)
    assert restored.result.parser_policy == policy
    assert base64.b64decode(restored.result.body) == entity


def test_outcome_cannot_substitute_another_inventory_endpoint_or_relabel_its_digest() -> None:
    """Keep individually valid endpoint observations from crossing request and parser domains.

    Given: Valid orders and positions purpose shapes using the same source key and original ordinal.
    When: A positions source is attached to an orders request or the purpose is copied under its old hash.
    Then: Independent parser-purpose and original-attempt digest checks both refuse the substitution.
    """
    with pytest.raises(ValidationError, match="parser does not match"):
        _outcome(result=_source(parser_policy="kraken_futures_open_positions_v1"))
    original = _attempt()
    positions = _attempt(purpose="kraken_futures.open_positions")
    assert futures_inventory_attempt_digest(original) != futures_inventory_attempt_digest(positions)
    with pytest.raises(ValidationError, match="exact attempt digest"):
        _outcome(attempt=positions)
    with pytest.raises(ValidationError):
        canonical_futures_inventory_attempt(
            original.model_copy(update={"purpose": "kraken_futures.trading_instruments"})
        )
