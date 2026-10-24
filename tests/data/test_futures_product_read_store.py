"""Actual product storage retries preserve original provenance, ordering and source knowledge."""

import base64
from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import select

from snapper.application.trade.futures_product_read import read_native_futures_products
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.core.futures_product_read import FuturesProductReadFailure
from snapper.core.futures_product_read import FuturesProductReadSource
from snapper.data.futures_product_key_binding import FuturesProductBindingError
from snapper.data.futures_product_read_store import BoundFuturesProductAttempt
from snapper.data.futures_product_read_store import BoundFuturesProductOutcome
from snapper.data.futures_product_read_store import _append_futures_product_outcome
from snapper.data.futures_product_read_store import begin_futures_product_read
from snapper.data.futures_product_read_store import get_bound_futures_product_attempt
from snapper.data.futures_product_read_store import get_bound_futures_product_outcome
from snapper.data.futures_product_read_store import get_latest_futures_product_read
from snapper.data.models import FuturesProductReadAttempt as ProductAttemptRow
from snapper.data.models import FuturesProductReadOutcome as ProductOutcomeRow
from tests.helpers.futures_product_read import FuturesProductScenario
from tests.helpers.futures_product_read import futures_product_scenario
from tests.helpers.native_account_authority import native_authority_uuid


async def _original_identity(
    scenario: FuturesProductScenario, outcome: BoundFuturesProductOutcome, kind: str
) -> ExecutionEvidenceIdentity:
    """Reload actual immutable insertion provenance without inventing source or request evidence."""
    async with scenario.native.repository.session() as session:
        row: ProductAttemptRow | ProductOutcomeRow | None
        if kind == "attempt":
            row = await session.scalar(
                select(ProductAttemptRow).where(
                    ProductAttemptRow.public_id == outcome.payload.attempt_public_id
                )
            )
        else:
            row = await session.scalar(
                select(ProductOutcomeRow).where(ProductOutcomeRow.public_id == outcome.public_id)
            )
        assert row is not None
        return ExecutionEvidenceIdentity(
            public_id=row.public_id, session_id=row.session_id, sequence_id=row.sequence_id
        )


async def _retry(
    scenario: FuturesProductScenario,
    outcome: BoundFuturesProductOutcome,
    kind: str,
    identity: ExecutionEvidenceIdentity,
) -> BoundFuturesProductAttempt | BoundFuturesProductOutcome:
    """Repeat actual persistence through the same repository transaction and wallet fence."""
    native = scenario.native
    if kind == "attempt":
        return await begin_futures_product_read(
            native.repository, outcome.payload.attempt.key_proof, identity
        )
    async with native.repository.session() as session:
        await native.repository._begin_execution_insert_transaction(session)
        await native.repository._acquire_execution_fence_lock(
            session, native.selection.wallet_public_id
        )
        retried = await _append_futures_product_outcome(
            session, outcome.payload.attempt_public_id, outcome.payload.result, identity
        )
        await session.commit()
        return retried


@pytest.mark.parametrize("kind", ["attempt", "outcome"])
async def test_exact_actual_retry_preserves_original_identity_sequence_and_knowledge(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    """Keep a committed product observation unchanged after a later acknowledgement retry.

    Given: Actual signed key, accounts and product HTTP have committed genuine source evidence.
    When: The original insertion identity is retried after the local clock advances.
    Then: Original request ordering, completion time and HTTP source ages remain unchanged.
    """
    async with futures_product_scenario(tmp_path, monkeypatch) as scenario:
        native = scenario.native
        outcome = await read_native_futures_products(
            native.repository, native.client, native.tracker
        )
        identity = await _original_identity(scenario, outcome, kind)
        native.clock.current += timedelta(seconds=2)
        retried = await _retry(scenario, outcome, kind, identity)
        expected = outcome.payload.attempt if kind == "attempt" else outcome.payload
        assert retried.payload == expected
        assert retried.public_id == identity.public_id
        assert len(scenario.adapter.product_calls) == 1
        assert await _original_identity(scenario, outcome, kind) == identity
        async with native.repository.session() as session:
            latest = await get_latest_futures_product_read(
                session, native.selection, native.clock.current
            )
        assert latest is not None and latest.outcome == outcome


@pytest.mark.parametrize("kind", ["attempt", "outcome"])
@pytest.mark.parametrize("field", ["session_id", "sequence_id"])
async def test_actual_retry_refuses_different_insertion_provenance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str, field: str
) -> None:
    """Refuse a changed producer identity even when the request UUID and source bytes match.

    Given: An actual product operation has immutable request and outcome insertion provenance.
    When: Its retry changes only the original session or sequence while reusing its public UUID.
    Then: The writer refuses that different identity and the original committed evidence remains.
    """
    async with futures_product_scenario(tmp_path, monkeypatch) as scenario:
        native = scenario.native
        outcome = await read_native_futures_products(
            native.repository, native.client, native.tracker
        )
        identity = await _original_identity(scenario, outcome, kind)
        changed = identity.model_dump(mode="python")
        changed[field] = (
            native_authority_uuid(987) if field == "session_id" else identity.sequence_id + 1
        )
        with pytest.raises(FuturesProductBindingError, match="insertion_identity_changed"):
            await _retry(scenario, outcome, kind, ExecutionEvidenceIdentity.model_validate(changed))
        assert await _original_identity(scenario, outcome, kind) == identity
        async with native.repository.session() as session:
            assert (
                await get_bound_futures_product_outcome(session, outcome.payload.attempt_public_id)
                == outcome
            )


@pytest.mark.parametrize("kind", ["attempt", "outcome"])
async def test_actual_retry_refuses_clock_rollback_without_rewriting_original_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    """Do not relabel committed knowledge with an earlier local clock after a retry.

    Given: A genuine product request and its outcome were committed at the original local time.
    When: An exact retry runs with a local clock earlier than those stored knowledge boundaries.
    Then: Persistence refuses the regression and keeps the original successful evidence intact.
    """
    async with futures_product_scenario(tmp_path, monkeypatch) as scenario:
        native = scenario.native
        outcome = await read_native_futures_products(
            native.repository, native.client, native.tracker
        )
        identity = await _original_identity(scenario, outcome, kind)
        native.clock.current -= timedelta(microseconds=1)
        with pytest.raises(FuturesProductBindingError, match="knowledge_clock_regressed"):
            await _retry(scenario, outcome, kind, identity)
        async with native.repository.session() as session:
            assert (
                await get_bound_futures_product_outcome(session, outcome.payload.attempt_public_id)
                == outcome
            )


async def test_actual_outcome_retry_cannot_replace_success_with_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep an actual captured result immutable after a retry reports a different terminal state.

    Given: Real signed product HTTP produced a committed successful outcome.
    When: The same outcome identity is used to append a transport failure instead.
    Then: The writer refuses the conflicting result and retains the original successful source.
    """
    async with futures_product_scenario(tmp_path, monkeypatch) as scenario:
        native = scenario.native
        outcome = await read_native_futures_products(
            native.repository, native.client, native.tracker
        )
        identity = await _original_identity(scenario, outcome, "outcome")
        async with native.repository.session() as session:
            await native.repository._begin_execution_insert_transaction(session)
            await native.repository._acquire_execution_fence_lock(
                session, native.selection.wallet_public_id
            )
            with pytest.raises(FuturesProductBindingError, match="outcome_identity_changed"):
                await _append_futures_product_outcome(
                    session,
                    outcome.payload.attempt_public_id,
                    FuturesProductReadFailure(reason="transport_error"),
                    identity,
                )
        async with native.repository.session() as session:
            assert (
                await get_bound_futures_product_outcome(session, outcome.payload.attempt_public_id)
                == outcome
            )


async def test_actual_outcome_identity_cannot_be_reused_for_another_pending_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep immutable outcome identities unique across separately committed product requests.

    Given: A completed genuine product read and a later pending request using its actual safe proof.
    When: The later request tries to reuse the earlier outcome's exact insertion identity.
    Then: The writer refuses the collision and the newer request remains honestly pending.
    """
    async with futures_product_scenario(tmp_path, monkeypatch) as scenario:
        native = scenario.native
        outcome = await read_native_futures_products(
            native.repository, native.client, native.tracker
        )
        identity = await _original_identity(scenario, outcome, "outcome")
        pending = await begin_futures_product_read(
            native.repository,
            outcome.payload.attempt.key_proof,
            ExecutionEvidenceIdentity(
                public_id=native_authority_uuid(980),
                session_id=native_authority_uuid(981),
                sequence_id=980,
            ),
        )
        async with native.repository.session() as session:
            await native.repository._begin_execution_insert_transaction(session)
            await native.repository._acquire_execution_fence_lock(
                session, native.selection.wallet_public_id
            )
            with pytest.raises(FuturesProductBindingError, match="outcome_identity_reused"):
                await _append_futures_product_outcome(
                    session, pending.public_id, outcome.payload.result, identity
                )
        async with native.repository.session() as session:
            latest = await get_latest_futures_product_read(
                session, native.selection, native.clock.current
            )
        assert latest is not None and latest.attempt == pending and latest.outcome is None


async def test_unread_family_and_missing_request_remain_explicit_without_fabricated_outcome(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep absence distinct from a failed or successful product observation.

    Given: The actual wallet and credential exist without a committed product request.
    When: Current family and direct historical readers inspect that empty product lifecycle.
    Then: Latest is absent and direct reads refuse an invented request identity.
    """
    async with futures_product_scenario(tmp_path, monkeypatch) as scenario:
        native = scenario.native
        async with native.repository.session() as session:
            assert (
                await get_latest_futures_product_read(
                    session, native.selection, native.clock.current
                )
                is None
            )
            with pytest.raises(FuturesProductBindingError, match="attempt_missing"):
                await get_bound_futures_product_attempt(session, native_authority_uuid(980))
            with pytest.raises(FuturesProductBindingError, match="attempt_missing"):
                await get_bound_futures_product_outcome(session, native_authority_uuid(980))


async def test_public_success_shape_cannot_create_new_product_source_even_with_actual_key_proof(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Require the issued original HTTP edge instead of trusting a fresh-looking constructed success.

    Given: A real product read supplied an actual bound key proof and a later independent request commits.
    When: A caller constructs otherwise valid success bytes and clocks without that request's actual HTTP.
    Then: New persistence refuses the source shape and keeps the later request pending with no fallback.
    """
    async with futures_product_scenario(tmp_path, monkeypatch) as scenario:
        native = scenario.native
        outcome = await read_native_futures_products(
            native.repository, native.client, native.tracker
        )
        native.clock.current += timedelta(seconds=1)
        pending = await begin_futures_product_read(
            native.repository,
            outcome.payload.attempt.key_proof,
            ExecutionEvidenceIdentity(
                public_id=native_authority_uuid(970),
                session_id=native_authority_uuid(971),
                sequence_id=970,
            ),
        )
        invented = FuturesProductReadSource(
            status_code=200,
            body=base64.b64encode(scenario.adapter._original_body()).decode("ascii"),
            request_started_at=native.clock.current,
            observed_at=native.clock.current,
        )
        native.clock.current += timedelta(seconds=1)
        async with native.repository.session() as session:
            await native.repository._begin_execution_insert_transaction(session)
            await native.repository._acquire_execution_fence_lock(
                session, native.selection.wallet_public_id
            )
            with pytest.raises(FuturesProductBindingError, match="requires_actual_transport"):
                await _append_futures_product_outcome(
                    session,
                    pending.public_id,
                    invented,
                    ExecutionEvidenceIdentity(
                        public_id=native_authority_uuid(972),
                        session_id=native_authority_uuid(971),
                        sequence_id=972,
                    ),
                )
        async with native.repository.session() as session:
            latest = await get_latest_futures_product_read(
                session, native.selection, native.clock.current
            )
        assert latest is not None and latest.attempt == pending and latest.outcome is None
        assert len(scenario.adapter.product_calls) == 1
