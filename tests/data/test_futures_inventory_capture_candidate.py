"""Unexecuted helper tests for actual HTTP capture and exact inventory persistence projections.

The genuine guarded HTTP fixture is supplied by the separate unpromoted wire
candidate after WSL composition. Its proof binding is synthetic. Loader-boundary
unit doubles below prove comparison behavior, not real SQL/root persistence.
No runtime, migration or integrated acceptance is claimed by this source.
"""

import base64
import json
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import cast

import pytest
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

import snapper.data.futures_inventory_capture as capture
import snapper.data.futures_inventory_key_binding as key_binding
from snapper.core.execution_route_authority import client_selection_digest
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
from snapper.core.native_account_identity import NativeAccountIdentityProofPayload
from snapper.core.native_account_identity import NativeAccountIdentityRootReference
from snapper.core.native_account_identity import NativeAccountPublicIdentity
from snapper.core.native_account_identity import native_account_identity_digest
from snapper.core.native_account_read import NativeAccountReadAttemptPayload
from snapper.core.native_account_read import native_account_read_attempt_digest
from snapper.data.futures_inventory_capture import inventory_result_for_storage
from snapper.data.futures_inventory_key_binding import FuturesInventoryBindingError
from snapper.data.futures_inventory_key_binding import get_bound_inventory_read_key
from snapper.data.futures_inventory_key_binding import inventory_read_key_reference
from snapper.data.futures_inventory_read_projection import inventory_read_attempt_projection
from snapper.data.futures_inventory_read_projection import inventory_read_outcome_projection
from snapper.data.futures_inventory_read_projection import inventory_read_scope_projection
from snapper.data.native_account_identity_store import BoundNativeAccountIdentityProof
from snapper.infrastructure.exchanges.futures_inventory_wire import GuardedFuturesInventoryReceipt
from snapper.infrastructure.exchanges.futures_inventory_wire import (
    require_guarded_inventory_receipt,
)
from tests.infrastructure.exchanges.test_futures_inventory_wire import _Scenario
from tests.infrastructure.exchanges.test_futures_inventory_wire import _body
from tests.infrastructure.exchanges.test_futures_inventory_wire import _scenario
from tests.infrastructure.exchanges.test_futures_inventory_wire import _selection
from tests.infrastructure.exchanges.test_futures_inventory_wire import _uuid

_PURPOSES: tuple[InventoryReadPurpose, ...] = (
    "kraken_futures.open_orders",
    "kraken_futures.open_positions",
)
_POLICIES = {
    "kraken_futures.open_orders": "kraken_futures_open_orders_v1",
    "kraken_futures.open_positions": "kraken_futures_open_positions_v1",
}
_ARRAYS = {
    "kraken_futures.open_orders": "openOrders",
    "kraken_futures.open_positions": "openPositions",
}


def _attempt(scenario: _Scenario) -> FuturesInventoryReadAttempt:
    """Freeze the independent request coordinates before genuine inventory HTTP is invoked."""
    reference = InventoryReadKeyReference.model_validate(
        scenario.token.require_proof_reference().model_dump(mode="python")
    )
    return FuturesInventoryReadAttempt(
        purpose=scenario.source.purpose,
        client_selection=scenario.selection,
        key_proof=reference,
        family_sequence=1,
        started_at=datetime.now(UTC),
    )


def _outcome(
    attempt: FuturesInventoryReadAttempt, result: FuturesInventoryReadSource
) -> FuturesInventoryReadOutcome:
    """Construct result shape after actual capture without claiming a SQL outcome commit."""
    return FuturesInventoryReadOutcome(
        attempt_public_id=_uuid(30),
        attempt_content_digest=futures_inventory_attempt_digest(attempt),
        attempt=attempt,
        completed_at=datetime.now(UTC),
        result=result,
    )


@pytest.mark.parametrize("purpose", _PURPOSES)
def test_new_source_requires_actual_http_and_retains_exact_entity_and_indexed_coordinates(
    purpose: InventoryReadPurpose,
) -> None:
    """Bridge actual issued HTTP to full parsing and the exact purpose-specific storage projection.

    Given: Original READ_ONLY key-token HTTP and request coordinates frozen before inventory I/O.
    When: The real guarded leaf returns complete exact source and capture validates its whole array.
    Then: Original bytes/clocks survive and all indexed copies match the canonical request/result.
    """
    with _scenario(purpose) as scenario:
        attempt = _attempt(scenario)
        actual = scenario.read()
        original = require_guarded_inventory_receipt(actual)
        result = inventory_result_for_storage(actual, _uuid(30), attempt)
        assert isinstance(result, FuturesInventoryReadSource)
        assert base64.b64decode(result.body, validate=True) == _body(purpose)
        assert result.parser_policy == _POLICIES[purpose]
        assert (result.request_started_at, result.observed_at) == (
            original.request_started_at, original.observed_at
        )
        outcome = _outcome(attempt, result)
        scope = inventory_read_scope_projection(attempt)
        assert scope == {
            "wallet_public_id": scenario.selection.wallet_public_id,
            "exchange": "kraken_futures",
            "mode": "live",
            "environment": "production",
            "margin_account": None,
            "scope_digest": futures_inventory_read_family(scenario.selection, purpose),
            "purpose": purpose,
            "family_sequence": 1,
        }
        request_projection = inventory_read_attempt_projection(attempt)
        result_projection = inventory_read_outcome_projection(outcome)
        assert request_projection == scope | {
            "client_selection_digest": client_selection_digest(scenario.selection),
            "key_proof_public_id": attempt.key_proof.public_id,
            "key_proof_content_digest": attempt.key_proof.content_digest,
            "started_at": attempt.started_at,
            "timestamp": attempt.started_at,
            "content_digest": futures_inventory_attempt_digest(attempt),
            "canonical_payload": canonical_futures_inventory_attempt(attempt),
        }
        assert result_projection == scope | {
            "attempt_public_id": _uuid(30),
            "attempt_content_digest": futures_inventory_attempt_digest(attempt),
            "completed_at": outcome.completed_at,
            "timestamp": outcome.completed_at,
            "result_kind": "captured",
            "content_digest": futures_inventory_outcome_digest(outcome),
            "canonical_payload": canonical_futures_inventory_outcome(outcome),
        }
        assert len(request_projection) == len(result_projection) == 15
        assert len(scenario.adapter.inventory_calls) == 1


@pytest.mark.parametrize("purpose", _PURPOSES)
@pytest.mark.parametrize("fault", ("missing_row_fields", "invalid_time", "duplicate_identity"))
def test_actual_semantically_invalid_inventory_never_encodes_a_retained_source(
    monkeypatch: pytest.MonkeyPatch, purpose: InventoryReadPurpose, fault: str
) -> None:
    """Full row semantics must run before retained entity encoding, even after privacy-safe HTTP.

    Given: Genuine HTTP returns an envelope with invalid required rows, time or identities.
    When: Capture invokes the actual complete purpose parser before encoding any captured source.
    Then: Only fixed source_invalid metadata is returned and no body is retained.
    """
    with _scenario(purpose) as scenario:
        document: dict[str, object] = json.loads(_body(purpose))
        array_name = _ARRAYS[purpose]
        entries = document[array_name]
        assert isinstance(entries, list)
        if fault == "missing_row_fields":
            document[array_name] = [{}]
        elif fault == "invalid_time":
            document["serverTime"] = "not-a-provider-clock"
        else:
            entries.append(entries[0])
        scenario.source.body = json.dumps(document).encode()
        attempt = _attempt(scenario)
        actual = scenario.read()

        def refuse_encoding(value: bytes) -> bytes:
            """Fail if semantic refusal attempts retained original-entity base64 encoding."""
            raise AssertionError("invalid_inventory_encoded_before_semantic_refusal")

        monkeypatch.setattr(capture.base64, "b64encode", refuse_encoding)
        result = inventory_result_for_storage(actual, _uuid(30), attempt)
        assert result == FuturesInventoryReadFailure(reason="source_invalid")
        assert result.model_dump(mode="json") == {"kind": "failed", "reason": "source_invalid"}
        assert len(scenario.adapter.inventory_calls) == 1


@pytest.mark.parametrize(
    "field_name",
    (
        "public_id", "content_digest", "read_attempt_public_id", "account_uid",
        "key_owner_root_public_id", "key_owner_root_content_digest", "observed_at",
    ),
)
def test_actual_source_cannot_use_another_proof_or_owner_root(field_name: str) -> None:
    """All public actual-key coordinates participate in binding, including the original owner root.

    Given: Genuine guarded inventory source belongs to its actual original public key proof.
    When: An otherwise coherent request changes one retained proof/root field.
    Then: Source cannot complete that different request and no public success is produced.
    """
    with _scenario("kraken_futures.open_orders") as scenario:
        attempt = _attempt(scenario)
        actual = scenario.read()
        changes: dict[str, object] = {
            "public_id": _uuid(101),
            "content_digest": "b" * 64,
            "read_attempt_public_id": _uuid(102),
            "account_uid": _uuid(103),
            "key_owner_root_public_id": _uuid(104),
            "key_owner_root_content_digest": "c" * 64,
            "observed_at": attempt.key_proof.observed_at - timedelta(microseconds=1),
        }
        key = InventoryReadKeyReference.model_validate(
            attempt.key_proof.model_dump(mode="python") | {field_name: changes[field_name]}
        )
        changed = FuturesInventoryReadAttempt.model_validate(
            attempt.model_dump(mode="python") | {"key_proof": key}
        )
        with pytest.raises(FuturesInventoryBindingError, match="actual_transport_attempt_mismatch"):
            inventory_result_for_storage(actual, _uuid(30), changed)
        assert len(scenario.adapter.inventory_calls) == 1


@pytest.mark.parametrize("fault", ("attempt_uuid", "purpose", "started_after_http"))
def test_genuine_response_is_bound_to_its_original_independent_request(fault: str) -> None:
    """An equal source body is not permission to complete another attempt or endpoint request.

    Given: Actual orders HTTP completed with its own original purpose and request identity.
    When: A different attempt UUID, endpoint purpose or later request start is offered.
    Then: Capture refuses before any new captured-source encoding.
    """
    with _scenario("kraken_futures.open_orders") as scenario:
        attempt = _attempt(scenario)
        actual = scenario.read()
        changed_id = _uuid(31) if fault == "attempt_uuid" else _uuid(30)
        if fault == "purpose":
            attempt = FuturesInventoryReadAttempt.model_validate(
                attempt.model_dump(mode="python") | {"purpose": "kraken_futures.open_positions"}
            )
        elif fault == "started_after_http":
            original = require_guarded_inventory_receipt(actual)
            attempt = FuturesInventoryReadAttempt.model_validate(
                attempt.model_dump(mode="python")
                | {"started_at": original.observed_at + timedelta(seconds=1)}
            )
        with pytest.raises(FuturesInventoryBindingError, match="actual_transport_attempt_mismatch"):
            inventory_result_for_storage(actual, changed_id, attempt)


def test_public_success_or_reconstructed_receipt_cannot_create_a_new_outcome() -> None:
    """A public successful DTO or newly constructed opaque shape is not an issued observation.

    Given: Genuine inventory HTTP produced one correctly classified original source.
    When: Its public result or a reconstructed matching wrapper is offered for a new outcome write.
    Then: Only the original issued receipt satisfies new-source capture.
    """
    with _scenario("kraken_futures.open_orders") as scenario:
        attempt = _attempt(scenario)
        actual = scenario.read()
        result = inventory_result_for_storage(actual, _uuid(30), attempt)
        assert isinstance(result, FuturesInventoryReadSource)
        with pytest.raises(
            FuturesInventoryBindingError, match="new_success_requires_actual_transport"
        ):
            inventory_result_for_storage(result, _uuid(30), attempt)
        forged = GuardedFuturesInventoryReceipt(require_guarded_inventory_receipt(actual))
        with pytest.raises(ValueError):
            inventory_result_for_storage(forged, _uuid(30), attempt)


@pytest.mark.parametrize("fault", ("unchanged", "parser_policy", "clock", "invalid_rows"))
def test_stored_source_retry_requires_original_purpose_clocks_and_full_semantics(
    fault: str,
) -> None:
    """Reparse existing-source retries without claiming the helper loaded a SQL outcome itself.

    Given: An actual original captured source exists in this fixture's caller memory.
    When: The caller selects stored-source parsing, optionally changing retained evidence.
    Then: Only unchanged valid source passes; the future store must still compare its original row.
    """
    with _scenario("kraken_futures.open_orders") as scenario:
        attempt = _attempt(scenario)
        original = inventory_result_for_storage(scenario.read(), _uuid(30), attempt)
        assert isinstance(original, FuturesInventoryReadSource)
        changes: dict[str, object] = {}
        if fault == "parser_policy":
            changes["parser_policy"] = "kraken_futures_open_positions_v1"
        elif fault == "clock":
            changes["request_started_at"] = attempt.started_at - timedelta(seconds=1)
        elif fault == "invalid_rows":
            changes["body"] = base64.b64encode(
                b'{"result":"success","serverTime":"2026-10-25T00:00:00Z","openOrders":[{}]}'
            ).decode("ascii")
        candidate = FuturesInventoryReadSource.model_validate(
            original.model_dump(mode="python") | changes
        )
        if fault == "unchanged":
            assert inventory_result_for_storage(
                candidate, _uuid(30), attempt, allow_stored_source=True
            ) == original
        else:
            with pytest.raises(FuturesInventoryBindingError, match="stored_source_invalid"):
                inventory_result_for_storage(
                    candidate, _uuid(30), attempt, allow_stored_source=True
                )


def test_failure_metadata_is_revalidated_without_accepting_unchecked_model_updates() -> None:
    """Strict immutable failures cannot carry an arbitrary provider-controlled reason string.

    Given: An original request and a supported fixed failure DTO.
    When: Its caller attempts an unchecked model-copy replacement of the reason.
    Then: The original failure remains valid while the changed copy is refused.
    """
    with _scenario("kraken_futures.open_orders") as scenario:
        attempt = _attempt(scenario)
        original = FuturesInventoryReadFailure(reason="transport_error")
        assert inventory_result_for_storage(original, _uuid(30), attempt) == original
        changed = original.model_copy(update={"reason": "provider-secret-text"})
        with pytest.raises(ValidationError):
            inventory_result_for_storage(changed, _uuid(30), attempt)


def _proof(*, explicit_root: bool = False) -> BoundNativeAccountIdentityProof:
    """Build pure unit proof shape without pretending this constructor verifies a database root."""
    selected = _selection()
    now = datetime.now(UTC)
    attempt = NativeAccountReadAttemptPayload(
        client_selection=selected, family_sequence=1, started_at=now
    )
    observation = NativeAccountPublicIdentity(
        client_selection=selected,
        account_uid=_uuid(90),
        general_access="READ_ONLY",
        transfer_access="NO_ACCESS",
        request_started_at=now,
        observed_at=now,
    )
    root = (
        NativeAccountIdentityRootReference(
            public_id=_uuid(91), content_digest="d" * 64, account_uid=_uuid(90)
        )
        if explicit_root else None
    )
    return BoundNativeAccountIdentityProof(
        _uuid(92),
        NativeAccountIdentityProofPayload(
            attempt_public_id=_uuid(93),
            attempt_content_digest=native_account_read_attempt_digest(attempt),
            attempt=attempt,
            observation=observation,
            recorded_at=now,
            owner_root=root,
        ),
    )


@pytest.mark.parametrize("explicit_root", (False, True))
def test_public_key_projection_preserves_first_or_explicit_owner_root(explicit_root: bool) -> None:
    """Project all coordinates without imposing FULL_ACCESS or inventing a margin profile.

    Given: A pure READ_ONLY native proof shape with its first owner or explicit same-account root.
    When: Its inventory public reference is projected.
    Then: Original proof/root identity, selection and knowledge remain unchanged.
    """
    proof = _proof(explicit_root=explicit_root)
    reference = inventory_read_key_reference(proof)
    assert reference.public_id == proof.public_id
    assert reference.content_digest == native_account_identity_digest(proof.payload)
    assert reference.client_selection == proof.payload.observation.client_selection
    assert reference.account_uid == proof.payload.observation.account_uid
    root = proof.payload.owner_root
    assert reference.key_owner_root_public_id == (
        proof.public_id if root is None else root.public_id
    )
    assert reference.key_owner_root_content_digest == (
        reference.content_digest if root is None else root.content_digest
    )
    assert reference.observed_at == proof.payload.observation.observed_at
    assert reference.recorded_at == proof.payload.recorded_at


async def test_key_loader_rebinds_native_history_before_comparing_retained_reference(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify native loader delegation without claiming that this unit double supplies SQL evidence.

    Given: The exact imported native loader boundary returns one independently supplied proof shape.
    When: The inventory reference matches or changes an original owner-root coordinate.
    Then: Every call uses the native loader and only the complete matching reference is accepted.
    """
    proof = _proof(explicit_root=True)
    reference = inventory_read_key_reference(proof)
    session = cast(AsyncSession, object())
    calls: list[str] = []

    async def native_loader(
        actual_session: AsyncSession, public_id: str
    ) -> BoundNativeAccountIdentityProof:
        """Observe exact delegation arguments without performing or emulating a SQL operation."""
        assert actual_session is session and public_id == proof.public_id
        calls.append(public_id)
        return proof

    monkeypatch.setattr(key_binding, "get_bound_native_account_identity", native_loader)
    assert await get_bound_inventory_read_key(session, reference) is proof
    changed = InventoryReadKeyReference.model_validate(
        reference.model_dump(mode="python") | {"key_owner_root_content_digest": "e" * 64}
    )
    with pytest.raises(FuturesInventoryBindingError, match="actual_key_proof_mismatch"):
        await get_bound_inventory_read_key(session, changed)
    assert calls == [proof.public_id, proof.public_id]
