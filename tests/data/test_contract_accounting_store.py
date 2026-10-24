"""Real SQLite capture, retry, conflict, crash and durable account-hold witnesses."""

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock
from unittest.mock import patch
from uuid import NAMESPACE_URL
from uuid import uuid5

import pytest
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.core.contract_accounting_observation import AccountingObservationScope
from snapper.core.contract_accounting_observation import AccountingWriteProvenance
from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.core.contract_accounting_observation import accounting_hold_scope_digest
from snapper.core.contract_accounting_observation import accounting_observation_digest
from snapper.core.contract_accounting_observation import canonical_accounting_observation
from snapper.data.contract_accounting_store import AccountingEvidenceConflict
from snapper.data.contract_accounting_store import AccountingScopeUnavailable
from snapper.data.contract_accounting_store import capture_accounting_observation
from snapper.data.contract_accounting_store import load_unresolved_accounting_holds
from snapper.data.contract_accounting_store import require_accounting_scope_available
from snapper.data.models import AccountingRawReceipt
from snapper.data.models import AccountingScopeHold
from snapper.data.models import Base
from snapper.data.models import Execution
from snapper.data.repository import SQLAlchemyRepository

_NOW = datetime(2026, 10, 23, 6, tzinfo=UTC)


def _id(number: int) -> str:
    """Provide independent stable observation, wallet and instrument identities."""
    return f"00000000-0000-7000-8000-{number:012d}"


def _observation() -> RawAccountingObservation:
    """Build a malformed financial source with no fresh account or unit authority.

    Returns:
        A capturable receipt whose source identity is not financial deduplication.
    """
    return RawAccountingObservation.model_validate(
        {
            "scope": {
                "wallet_public_id": _id(100),
                "exchange": "kraken_futures",
                "mode": "live",
                "environment": "production",
                "margin_account": "flex",
            },
            "route_revision": "r" * 64,
            "kind": "execution",
            "channel": "futures.private.fills",
            "representation": "decoded_json_text",
            "material": '{ "size": NaN, "fee": "unknown" }',
            "observed_at": _NOW,
            "source_identity": "venue-CaseSensitive-1",
            "source_identity_kind": "venue_execution_id",
            "source_instrument_public_id": _id(200),
        }
    )


def _provenance(number: int = 1) -> AccountingWriteProvenance:
    """Provide one frozen local observation identity retained on transaction retry."""
    return AccountingWriteProvenance(
        public_id=_id(number), session_id=_id(999), sequence_id=number, timestamp=_NOW
    )


def _changed_scope(
    scope: AccountingObservationScope, updates: dict[str, object]
) -> AccountingObservationScope:
    """Change only explicitly supplied actual-account source fields."""
    return AccountingObservationScope.model_validate({**scope.model_dump(), **updates})


@pytest.fixture
async def accounting_repo(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Provide a private real SQLite database with create-all physical guards."""
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'accounting.db'}")
    await repository.create_all()
    try:
        yield repository
    finally:
        await repository.engine.dispose()


async def _count(repository: SQLAlchemyRepository, model: type[Base]) -> int:
    """Read an independently committed row count from a fresh database session."""
    async with repository.session() as session:
        return int((await session.scalar(select(func.count()).select_from(model))) or 0)


async def test_raw_capture_commits_evidence_and_hold_without_interpreting_finances(
    accounting_repo: SQLAlchemyRepository,
) -> None:
    """Retain malformed evidence before authority, units or currency can be established.

    Given: A financially invalid raw frame and no catalog/account authority.
    When: Stage A captures it in a real transaction.
    Then: Receipt and hold commit together without creating an economic Execution.
    """
    observation = _observation()
    result = await capture_accounting_observation(accounting_repo, observation, _provenance())
    async with accounting_repo.session() as session:
        row = (await session.execute(select(AccountingRawReceipt))).scalar_one()
        holds = await load_unresolved_accounting_holds(session, observation.scope)
    assert row.canonical_payload == canonical_accounting_observation(observation)
    assert row.route_authority_public_id is None
    assert result.created is True
    assert result.conflicted is False
    assert len(holds) == 1
    assert holds[0].public_id == result.hold_public_id
    assert holds[0].raw_receipt_public_id == result.public_id
    assert holds[0].reason == "uninterpreted_observation"
    assert await _count(accounting_repo, Execution) == 0


async def test_exact_transaction_retry_retains_original_receipt_and_hold_identity(
    accounting_repo: SQLAlchemyRepository,
) -> None:
    """Deduplicate a local observation retry without deduplicating real observations.

    Given: One frozen receipt UUID and identical provider content.
    When: Its transaction is retried with later insertion metadata.
    Then: One original receipt and one original hold remain committed.
    """
    observation = _observation()
    first = await capture_accounting_observation(accounting_repo, observation, _provenance())
    retry = _provenance().model_copy(update={"timestamp": _NOW + timedelta(seconds=10)})
    again = await capture_accounting_observation(accounting_repo, observation, retry)
    assert again.public_id == first.public_id
    assert again.hold_public_id == first.hold_public_id
    assert again.created is False
    assert again.conflicted is False
    assert await _count(accounting_repo, AccountingRawReceipt) == 1
    assert await _count(accounting_repo, AccountingScopeHold) == 1


@pytest.mark.parametrize("same_material", (True, False))
async def test_new_observations_are_retained_even_for_the_same_venue_execution_id(
    accounting_repo: SQLAlchemyRepository, same_material: bool
) -> None:
    """Keep redelivery and changed execution evidence available for later reconciliation.

    Given: Two separately observed frames carrying the same opaque venue execution ID.
    When: Each observation receives its own local UUID.
    Then: Both receipts and both pending holds survive, even with identical content.
    """
    first = _observation()
    second = first if same_material else first.model_copy(update={"material": '{"fee":"0.02"}'})
    one = await capture_accounting_observation(accounting_repo, first, _provenance(1))
    two = await capture_accounting_observation(accounting_repo, second, _provenance(2))
    assert one.public_id != two.public_id
    assert two.conflicted is False
    assert await _count(accounting_repo, AccountingRawReceipt) == 2
    assert await _count(accounting_repo, AccountingScopeHold) == 2


async def test_changed_retry_content_is_retained_with_a_durable_conflict_hold(
    accounting_repo: SQLAlchemyRepository,
) -> None:
    """Never replace the first observed source or merely log an idempotency conflict.

    Given: One local observation UUID already bound to different provider material.
    When: The changed material is captured and retried.
    Then: A separate stable conflict receipt and persistent refusal are committed once.
    """
    original = _observation()
    first = await capture_accounting_observation(accounting_repo, original, _provenance())
    changed = original.model_copy(update={"material": '{"size":"2","fee":"0.1"}'})
    conflict = await capture_accounting_observation(accounting_repo, changed, _provenance())
    retried = await capture_accounting_observation(accounting_repo, changed, _provenance())
    assert conflict.public_id != first.public_id
    assert conflict.claimed_public_id == first.public_id
    assert conflict.conflicted is True
    assert conflict.created is True
    assert retried.created is False
    assert retried.public_id == conflict.public_id
    assert retried.hold_public_id == conflict.hold_public_id
    assert await _count(accounting_repo, AccountingRawReceipt) == 2
    async with accounting_repo.session() as session:
        holds = await load_unresolved_accounting_holds(session, original.scope)
        with pytest.raises(AccountingScopeUnavailable) as refused:
            await require_accounting_scope_available(session, original.scope)
    assert {hold.reason for hold in holds} == {
        "uninterpreted_observation",
        "observation_identity_conflict",
    }
    assert refused.value.holds == holds


async def test_same_frame_siblings_keep_separate_receipts_and_holds(
    accounting_repo: SQLAlchemyRepository,
) -> None:
    """Never clear a whole-frame hold after interpreting only its first child.

    Given: Contradictory sibling fills with one source ID and unchanged original frame bytes.
    When: Each persisted child pointer is captured under its own local observation UUID.
    Then: Both separate receipts and both unresolved holds remain durable.
    """
    original = _observation().model_copy(
        update={
            "material": '{"fills":[{"id":"same","qty":1},{"id":"same","qty":2}]}',
            "target_json_pointer": "/fills/0",
        }
    )
    sibling = original.model_copy(update={"target_json_pointer": "/fills/1"})
    first = await capture_accounting_observation(accounting_repo, original, _provenance(1))
    second = await capture_accounting_observation(accounting_repo, sibling, _provenance(2))
    assert first.content_digest != second.content_digest
    assert first.hold_public_id != second.hold_public_id
    async with accounting_repo.session() as session:
        assert len(await load_unresolved_accounting_holds(session, original.scope)) == 2


async def test_existing_hold_identity_cannot_bind_another_receipt(
    accounting_repo: SQLAlchemyRepository,
) -> None:
    """Refuse a contradictory retained hold rather than pretending capture is protected.

    Given: Corrupt retained storage reserves this observation's hold UUID for another receipt.
    When: Stage A attempts its atomic raw receipt and mandatory hold commit.
    Then: The contradiction refuses and the new receipt is rolled back, never falsely published.
    """
    observation = _observation()
    provenance = _provenance()
    reason = "uninterpreted_observation"
    hold_id = str(
        uuid5(NAMESPACE_URL, f"snapper:accounting-hold:v1:{provenance.public_id}:{reason}")
    )
    scope = observation.scope
    async with accounting_repo.session() as session:
        session.add(
            AccountingScopeHold(
                **{**provenance.model_dump(mode="python"), "public_id": hold_id},
                scope_digest=accounting_hold_scope_digest(scope),
                wallet_public_id=scope.wallet_public_id,
                exchange=scope.exchange,
                mode=scope.mode,
                environment=scope.environment,
                margin_account=scope.margin_account,
                raw_receipt_public_id=_id(444),
                reason=reason,
            )
        )
        await session.commit()
    with pytest.raises(AccountingEvidenceConflict, match="hold identity contradicts"):
        await capture_accounting_observation(accounting_repo, observation, provenance)
    assert await _count(accounting_repo, AccountingRawReceipt) == 0
    assert await _count(accounting_repo, AccountingScopeHold) == 1


async def test_reserved_conflict_uuid_never_adopts_unrelated_receipt_content(
    accounting_repo: SQLAlchemyRepository,
) -> None:
    """Refuse a retained identity collision without accepting another financial observation.

    Given: A primary receipt and an unrelated observation already occupy the retry conflict key.
    When: Changed content for the original UUID is observed.
    Then: Neither retained observation is overwritten or adopted as the changed source.
    """
    original = _observation()
    provenance = _provenance()
    await capture_accounting_observation(accounting_repo, original, provenance)
    changed = original.model_copy(update={"material": '{"fee":"changed"}'})
    digest = accounting_observation_digest(changed)
    reserved_id = str(
        uuid5(NAMESPACE_URL, f"snapper:accounting-conflict:v1:{provenance.public_id}:{digest}")
    )
    unrelated = original.model_copy(update={"material": '{"execution":"unrelated"}'})
    await capture_accounting_observation(
        accounting_repo, unrelated, _provenance(2).model_copy(update={"public_id": reserved_id})
    )
    with pytest.raises(AccountingEvidenceConflict, match="conflict identity already"):
        await capture_accounting_observation(accounting_repo, changed, provenance)
    assert await _count(accounting_repo, AccountingRawReceipt) == 2
    assert await _count(accounting_repo, AccountingScopeHold) == 2


@pytest.mark.parametrize(
    "updates",
    (
        {"wallet_public_id": _id(101)},
        {"exchange": "kraken"},
        {"environment": "demo"},
        {"exchange": "paper", "mode": "paper", "environment": "paper"},
        {"margin_account": "F-XBTUSD"},
    ),
)
async def test_known_hold_scope_is_isolated_by_actual_account_and_margin_profile(
    accounting_repo: SQLAlchemyRepository, updates: dict[str, object]
) -> None:
    """Do not hide a refusal by instrument changes or leak it into another actual account.

    Given: One held production-flex source account.
    When: Another wallet, venue, environment or known independent margin profile is read.
    Then: That different account has no inherited hold from this receipt.
    """
    observation = _observation()
    await capture_accounting_observation(accounting_repo, observation, _provenance())
    async with accounting_repo.session() as session:
        other = _changed_scope(observation.scope, updates)
        assert await load_unresolved_accounting_holds(session, other) == ()
        await require_accounting_scope_available(session, other)


async def test_unknown_margin_hold_affects_every_profile_in_its_account_family(
    accounting_repo: SQLAlchemyRepository,
) -> None:
    """Keep an uninterpretable incoming source from escaping through another instrument.

    Given: A malformed observation whose actual margin account cannot yet be resolved.
    When: Either a known profile or the whole account family is queried.
    Then: The same durable refusal remains visible across every candidate profile.
    """
    original = _observation()
    unknown = original.model_copy(
        update={"scope": _changed_scope(original.scope, {"margin_account": None})}
    )
    await capture_accounting_observation(accounting_repo, unknown, _provenance())
    async with accounting_repo.session() as session:
        for label in ("flex", "F-XBTUSD", None):
            scope = _changed_scope(original.scope, {"margin_account": label})
            assert len(await load_unresolved_accounting_holds(session, scope)) == 1
            with pytest.raises(AccountingScopeUnavailable):
                await require_accounting_scope_available(session, scope)


async def test_credential_rotation_and_source_instrument_changes_do_not_clear_holds(
    accounting_repo: SQLAlchemyRepository,
) -> None:
    """Keep a cash-account refusal independent of credential revision and instrument UUID.

    Given: A pending receipt from an old selected credential and one instrument.
    When: A new authority/credential version and another instrument are observed.
    Then: Both unresolved receipts remain in the same financial account family.
    """
    original = _observation()
    await capture_accounting_observation(accounting_repo, original, _provenance(1))
    rotated = original.model_copy(
        update={
            "credential_public_id": _id(300),
            "credential_timestamp": _NOW + timedelta(seconds=1),
            "route_authority_public_id": _id(301),
            "source_instrument_public_id": _id(201),
            "route_revision": "new-actual-public-route",
        }
    )
    await capture_accounting_observation(accounting_repo, rotated, _provenance(2))
    async with accounting_repo.session() as session:
        holds = await load_unresolved_accounting_holds(session, rotated.scope)
    assert len(holds) == 2


async def test_receipt_and_hold_rollback_together_when_hold_storage_fails(
    accounting_repo: SQLAlchemyRepository,
) -> None:
    """Do not publish a half-committed receipt or advance financial interpretation.

    Given: Raw receipt flush succeeds but the mandatory hold writer fails.
    When: Capture attempts to complete the transaction.
    Then: Neither row commits and the same frozen observation can be retried intact.
    """
    observation = _observation()
    failure = AsyncMock(side_effect=RuntimeError("hold storage unavailable"))
    with (
        patch("snapper.data.contract_accounting_store._ensure_hold", new=failure),
        pytest.raises(RuntimeError, match="hold storage unavailable"),
    ):
        await capture_accounting_observation(accounting_repo, observation, _provenance())
    assert await _count(accounting_repo, AccountingRawReceipt) == 0
    assert await _count(accounting_repo, AccountingScopeHold) == 0
    result = await capture_accounting_observation(accounting_repo, observation, _provenance())
    assert result.created is True


async def test_commit_failure_preserves_no_partial_capture_and_retry_can_commit(
    accounting_repo: SQLAlchemyRepository,
) -> None:
    """Keep durable retry authority at transaction completion rather than successful flush.

    Given: Both inserts flush but the transaction commit fails.
    When: The session is discarded and the frozen observation is retried.
    Then: No partial rows survive and one complete receipt/hold pair eventually commits.
    """
    observation = _observation()
    with patch.object(
        AsyncSession, "commit", new=AsyncMock(side_effect=RuntimeError("commit failed"))
    ), pytest.raises(RuntimeError, match="commit failed"):
        await capture_accounting_observation(accounting_repo, observation, _provenance())
    assert await _count(accounting_repo, AccountingRawReceipt) == 0
    assert await _count(accounting_repo, AccountingScopeHold) == 0
    await capture_accounting_observation(accounting_repo, observation, _provenance())
    assert await _count(accounting_repo, AccountingRawReceipt) == 1
    assert await _count(accounting_repo, AccountingScopeHold) == 1


async def test_concurrent_exact_retry_across_real_sessions_commits_one_pair(
    accounting_repo: SQLAlchemyRepository,
) -> None:
    """Serialize the actual read-then-write retry decision under the wallet write fence.

    Given: Two simultaneous sessions capturing the same frozen observation UUID.
    When: They race through the real SQLite transaction opener.
    Then: One creates and the other reuses exactly one receipt/hold pair.
    """
    observation = _observation()
    results = await asyncio.gather(
        capture_accounting_observation(accounting_repo, observation, _provenance()),
        capture_accounting_observation(accounting_repo, observation, _provenance()),
    )
    assert sorted(result.created for result in results) == [False, True]
    assert len({result.public_id for result in results}) == 1
    assert len({result.hold_public_id for result in results}) == 1
    assert await _count(accounting_repo, AccountingRawReceipt) == 1


@pytest.mark.parametrize("table", ("accounting_raw_receipts", "accounting_scope_holds"))
@pytest.mark.parametrize("operation", ("UPDATE", "DELETE"))
async def test_create_all_evidence_and_holds_are_physically_immutable(
    accounting_repo: SQLAlchemyRepository, table: str, operation: str
) -> None:
    """Prevent bypassing the application helper from rewriting financial refusals.

    Given: A real committed source receipt and its account hold.
    When: Direct SQL attempts a row update or deletion.
    Then: The database refuses the mutation and retains both original rows.
    """
    await capture_accounting_observation(accounting_repo, _observation(), _provenance())
    statement = (
        f"UPDATE {table} SET sequence_id = 99" if operation == "UPDATE" else f"DELETE FROM {table}"
    )
    async with accounting_repo.session() as session:
        with pytest.raises(IntegrityError, match="append-only"):
            await session.execute(text(statement))
        await session.rollback()
    assert await _count(accounting_repo, AccountingRawReceipt) == 1
    assert await _count(accounting_repo, AccountingScopeHold) == 1


async def test_restart_independent_session_reads_retain_account_unavailability(
    accounting_repo: SQLAlchemyRepository,
) -> None:
    """Keep hold truth in the durable database after every in-memory projection is gone.

    Given: A committed raw source and no engine or checkpoint state.
    When: A separate repository instance reads the same SQLite file.
    Then: The exact durable refusal remains visible without a replay watermark.
    """
    observation = _observation()
    result = await capture_accounting_observation(accounting_repo, observation, _provenance())
    other = SQLAlchemyRepository(str(accounting_repo.engine.url))
    try:
        async with other.session() as session:
            holds = await load_unresolved_accounting_holds(session, observation.scope)
            assert holds[0].public_id == result.hold_public_id
            with pytest.raises(AccountingScopeUnavailable):
                await require_accounting_scope_available(session, observation.scope)
    finally:
        await other.engine.dispose()
