"""Tests for the reviewed derived-plane retirement writer.

A repudiated execution leaves the projections built from it wrong. The adopted
design's answer is NOT a hand-written UPDATE against live money state: it is one
reviewed, scoped, idempotent writer that SCD2-retires the affected
``trade_projection_checkpoints`` and ``positions`` versions at the correction's
knowledge instant and lets the trader's normal recovery path rebuild them. These
tests pin that act, every refusal that keeps it safe, its idempotence, and the
one table it must never touch — ``execution_plan_checkpoints``, which the design
keeps as control state rather than economic projection.
"""

from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import PropertyMock
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy import select

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import ExecutionAnnulment
from snapper.data.models import ExecutionAnnulmentVisibility
from snapper.data.models import ExecutionPlan
from snapper.data.models import ExecutionPlanCheckpoint
from snapper.data.models import Position
from snapper.data.models import TradeProjectionCheckpoint
from snapper.data.repository import DerivedProjectionRetirementError
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import _DerivedProjectionVersion
from snapper.data.repository_types import DerivedProjectionRetirementRequest

_SESSION = "00000000-0000-7000-8000-000000000901"
_USER = "0000face-0000-7000-8000-0000000000d1"
_MAIN_WALLET = "0000face-0000-7000-8000-0000000000a1"
_INSTRUMENT = "00000000-0000-7000-8000-000000000b01"
_EXECUTION = "00000000-0000-7000-8000-000000000e01"
_ANNULMENT = "00000000-0000-7000-8000-0000000009a1"
_OBSERVATION = "00000000-0000-7000-8000-0000000009b1"
_CHECKPOINT = "00000000-0000-7000-8000-0000000009c1"
_PAPER_CHECKPOINT = "00000000-0000-7000-8000-0000000009c3"
_LEGACY_CHECKPOINT = "00000000-0000-7000-8000-0000000009c4"
_POSITION = "00000000-0000-7000-8000-0000000009c2"
_PLAN = "00000000-0000-7000-8000-0000000009d1"
_PLAN_CHECKPOINT = "00000000-0000-7000-8000-0000000009d2"
_ABSENT = "00000000-0000-7000-8000-0000000009ff"

_PROJECTION_AT = datetime(2026, 7, 19, 22, 0, tzinfo=UTC)
_KNOWN_AT = datetime(2026, 7, 26, 10, 0, tzinfo=UTC)
_OBSERVED_AT = datetime(2026, 7, 26, 10, 0, 1, tzinfo=UTC)
_CORRECTION_AT = datetime(2026, 7, 26, 9, 0, tzinfo=UTC)
_LIVE_SHARD = "kraken.EUR-PLN.live.wa10000000001"
_PAPER_SHARD = "paper.EUR-PLN.paper.wa10000000001.trend"


def _annulment() -> ExecutionAnnulment:
    """Build one committed manifest row for the live Kraken scope."""
    return ExecutionAnnulment(
        public_id=_ANNULMENT,
        target_execution_public_id=_EXECUTION,
        target_execution_digest="a" * 64,
        wallet_public_id=_MAIN_WALLET,
        exchange="kraken",
        mode="live",
        scope_sequence=1,
        annulled_by_user_public_id=_USER,
        correction_time=_CORRECTION_AT,
        reason="unwitnessed_phantom",
        evidence_json='{"diagnosis":"phantom"}',
        timestamp=_KNOWN_AT,
        known_to=KNOWN_TO_MAX,
        session_id=_SESSION,
        sequence_id=1,
    )


def _observation() -> ExecutionAnnulmentVisibility:
    """Build the durability observation that makes the correction APPLIED."""
    return ExecutionAnnulmentVisibility(
        public_id=_OBSERVATION,
        annulment_public_id=_ANNULMENT,
        annulment_id=1,
        observed_at=_OBSERVED_AT,
        wallet_public_id=_MAIN_WALLET,
        exchange="kraken",
        mode="live",
        timestamp=_OBSERVED_AT,
        known_to=KNOWN_TO_MAX,
        session_id=_SESSION,
        sequence_id=1,
    )


def _checkpoint(
    public_id: str,
    shard_key: str,
    timestamp: datetime = _PROJECTION_AT,
) -> TradeProjectionCheckpoint:
    """Build one active trade projection checkpoint version."""
    return TradeProjectionCheckpoint(
        public_id=public_id,
        shard_key=shard_key,
        wallet_public_id=_MAIN_WALLET,
        operator_public_id=None,
        position_qty=20.04,
        entry_price=4.3836,
        position_opened_at=_PROJECTION_AT,
        cash=100.0,
        peak_equity=100.0,
        realized_pnl=0.0,
        turnover=0.0,
        last_venue_event_id=7,
        last_venue_event_at=_PROJECTION_AT,
        open_command_ids=None,
        seen_exec_ids="[]",
        checkpoint_at=_PROJECTION_AT,
        timestamp=timestamp,
        known_to=KNOWN_TO_MAX,
        session_id=_SESSION,
        sequence_id=1,
    )


def _position(timestamp: datetime = _PROJECTION_AT) -> Position:
    """Build one active position projection version."""
    return Position(
        public_id=_POSITION,
        instrument_public_id=_INSTRUMENT,
        mode="live",
        wallet_public_id=_MAIN_WALLET,
        quantity=20.04,
        average_price=4.3836,
        unrealized_pnl=None,
        realized_pnl=0.0,
        mark_price=None,
        marked_at=None,
        source_venue_event_id=7,
        timestamp=timestamp,
        known_to=KNOWN_TO_MAX,
        session_id=_SESSION,
        sequence_id=1,
    )


def _plan(status: str = "completed") -> ExecutionPlan:
    """Build one execution plan owning the checkpoint that must stay untouched."""
    return ExecutionPlan(
        public_id=_PLAN,
        plan_type="manual_once",
        created_by_user_id=_USER,
        created_by_strategy=None,
        created_via="api",
        instrument_public_id=_INSTRUMENT,
        exchange="kraken",
        mode="live",
        shard_key=_LIVE_SHARD,
        wallet_public_id=_MAIN_WALLET,
        operator_public_id=None,
        total_quantity=20.04,
        filled_quantity=20.04,
        side="buy",
        parent_plan_public_id=None,
        position_cycle_public_id=None,
        params={},
        status=status,
        created_at=_PROJECTION_AT,
        timestamp=_PROJECTION_AT,
        known_to=KNOWN_TO_MAX,
        session_id=_SESSION,
        sequence_id=1,
    )


def _plan_checkpoint() -> ExecutionPlanCheckpoint:
    """Build the control-state checkpoint the retirement must never rebuild."""
    return ExecutionPlanCheckpoint(
        public_id=_PLAN_CHECKPOINT,
        plan_public_id=_PLAN,
        state={},
        last_venue_event_id=7,
        last_tick_timestamp=_PROJECTION_AT,
        checkpoint_at=_PROJECTION_AT,
        timestamp=_PROJECTION_AT,
        known_to=KNOWN_TO_MAX,
        session_id=_SESSION,
        sequence_id=1,
    )


@pytest.fixture()
async def unobserved_repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Create a repository whose correction is durable but NOT yet observed.

    Both planes of the knowledge protocol are append-only, so an unobserved
    correction cannot be produced by deleting an observation. It has to be the
    starting state, which is also the truthful order of events: the manifest row
    commits first and the observation follows in a second transaction.
    """
    db_path = tmp_path / "derived-projections.db"
    schema_engine = create_engine(f"sqlite:///{db_path}")
    ExecutionAnnulment.__table__.create(schema_engine)
    ExecutionAnnulmentVisibility.__table__.create(schema_engine)
    TradeProjectionCheckpoint.__table__.create(schema_engine)
    Position.__table__.create(schema_engine)
    ExecutionPlan.__table__.create(schema_engine)
    ExecutionPlanCheckpoint.__table__.create(schema_engine)
    schema_engine.dispose()
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    try:
        async with repo.session() as s:
            s.add_all(
                [
                    _annulment(),
                    _checkpoint(_CHECKPOINT, _LIVE_SHARD),
                    _checkpoint(_PAPER_CHECKPOINT, _PAPER_SHARD),
                    _position(),
                    _plan(),
                    _plan_checkpoint(),
                ]
            )
            await s.commit()
        yield repo
    finally:
        await repo.engine.dispose()


@pytest.fixture()
async def repository(
    unobserved_repository: SQLAlchemyRepository,
) -> SQLAlchemyRepository:
    """Complete the knowledge protocol so the scope carries an APPLIED correction."""
    async with unobserved_repository.session() as s:
        s.add(_observation())
        await s.commit()
    return unobserved_repository


def _request(**overrides: object) -> DerivedProjectionRetirementRequest:
    """Build one retirement request asserting the live scope's active rows."""
    base: dict[str, object] = {
        "wallet_public_id": _MAIN_WALLET,
        "mode": "live",
        "expected_trade_projection_checkpoint_public_ids": [_CHECKPOINT],
        "expected_position_public_ids": [_POSITION],
    }
    base.update(overrides)
    return cast(DerivedProjectionRetirementRequest, base)


async def _known_to(repository: SQLAlchemyRepository, public_id: str) -> datetime:
    """Read one checkpoint version's ``known_to`` straight from storage."""
    async with repository.session() as s:
        return (
            (
                await s.execute(
                    select(TradeProjectionCheckpoint.known_to).where(
                        TradeProjectionCheckpoint.public_id == public_id
                    )
                )
            )
            .scalars()
            .one()
        )


async def test_the_retirement_closes_exactly_the_asserted_rows_at_the_correction_instant(
    repository: SQLAlchemyRepository,
) -> None:
    """The derived half of a correction is one reviewed, scoped act.

    Given: A scope with one applied correction, one active live checkpoint, one
        active position, an unrelated paper checkpoint, and one plan checkpoint.
    When: The retirement runs asserting exactly the live checkpoint and the
        position.
    Then: Both are closed at the correction's KNOWLEDGE instant — not deleted,
        not backdated, and with no successor invented — the paper shard is
        untouched because it is not in this scope, and the result reports the
        act together with the scope it left behind.
    """
    result = await repository.retire_execution_annulment_derived_projections(_request())

    assert result["retired_at"] == _KNOWN_AT
    assert result["applied_annulment_public_ids"] == [_ANNULMENT]
    assert [(row["plane"], row["public_id"]) for row in result["retired"]] == [
        ("trade_projection_checkpoints", _CHECKPOINT),
        ("positions", _POSITION),
    ]
    assert await _known_to(repository, _CHECKPOINT) == _KNOWN_AT
    assert await _known_to(repository, _PAPER_CHECKPOINT) == KNOWN_TO_MAX
    assert result["scope_after"]["trade_projection_checkpoints"] == []
    assert result["scope_after"]["positions"] == []


async def test_the_retirement_never_touches_the_plan_checkpoints_it_reports(
    repository: SQLAlchemyRepository,
) -> None:
    """Control state is reported so its untouchedness is checked, not trusted.

    Given: One active ``execution_plan_checkpoints`` row whose plan is
        completed.
    When: The retirement runs.
    Then: The row is still active afterwards and is reported with its plan's
        terminality, which is the postcondition the adopted design asks for —
        plan checkpoints are NOT rebuilt, and the plans holding them can no
        longer act on the state they hold.
    """
    result = await repository.retire_execution_annulment_derived_projections(_request())

    witnesses = result["scope_after"]["execution_plan_checkpoints"]
    assert witnesses == [
        {
            "public_id": _PLAN_CHECKPOINT,
            "plan_public_id": _PLAN,
            "plan_status": "completed",
            "plan_is_terminal": True,
        }
    ]
    async with repository.session() as s:
        surviving = (
            (
                await s.execute(
                    select(ExecutionPlanCheckpoint.known_to).where(
                        ExecutionPlanCheckpoint.public_id == _PLAN_CHECKPOINT
                    )
                )
            )
            .scalars()
            .one()
        )
    assert surviving == KNOWN_TO_MAX


async def test_a_non_terminal_plan_is_reported_as_such_rather_than_assumed_safe(
    repository: SQLAlchemyRepository,
) -> None:
    """A plan that can still act is a finding, not a formatting detail.

    Given: A plan checkpoint whose plan is still ``active``.
    When: The scope is read.
    Then: The witness reports ``plan_is_terminal`` false, so the operator's
        postcondition fails loudly instead of being satisfied by the row merely
        existing.
    """
    async with repository.session() as s:
        plan = (await s.execute(select(ExecutionPlan))).scalars().one()
        plan.status = "active"
        await s.commit()

    scope = await repository.get_derived_projection_scope(_MAIN_WALLET, "live")

    assert scope["execution_plan_checkpoints"][0]["plan_is_terminal"] is False


async def test_rerunning_a_completed_retirement_refuses_instead_of_closing_twice(
    repository: SQLAlchemyRepository,
) -> None:
    """Idempotence here is a refusal that names the row, never a silent no-op.

    Given: A retirement that already ran.
    When: The identical request runs again.
    Then: It is refused with ``already_retired_derived_projection`` naming the
        checkpoint, because a second close would move an interval that has
        already been proven, and the honest answer to "did this run?" is the
        refusal rather than a second success.
    """
    await repository.retire_execution_annulment_derived_projections(_request())

    with pytest.raises(
        DerivedProjectionRetirementError, match="already_retired_derived_projection"
    ):
        await repository.retire_execution_annulment_derived_projections(_request())

    assert await _known_to(repository, _CHECKPOINT) == _KNOWN_AT


async def test_a_scope_with_no_applied_correction_has_nothing_to_retire(
    unobserved_repository: SQLAlchemyRepository,
) -> None:
    """A durable-but-unobserved correction is not yet a reason to rebuild.

    Given: A correction that is durable while its visibility observation is
        missing, so every historical horizon still refuses it.
    When: The retirement is attempted.
    Then: It is refused with ``no_applied_execution_annulment`` and nothing is
        closed. Retiring here would bake into the projections an effect history
        still denies.
    """
    with pytest.raises(DerivedProjectionRetirementError, match="no_applied_execution_annulment"):
        await unobserved_repository.retire_execution_annulment_derived_projections(_request())

    assert await _known_to(unobserved_repository, _CHECKPOINT) == KNOWN_TO_MAX


async def test_an_asserted_row_that_does_not_exist_here_is_refused(
    repository: SQLAlchemyRepository,
) -> None:
    """An id from the wrong scope or a mistyped one must stop the act.

    Given: An asserted checkpoint id no row in this scope carries.
    When: The retirement is attempted.
    Then: It is refused with ``unknown_derived_projection``, distinguished from
        the already-retired case because the two mistakes call for opposite
        actions.
    """
    with pytest.raises(DerivedProjectionRetirementError, match="unknown_derived_projection"):
        await repository.retire_execution_annulment_derived_projections(
            _request(expected_trade_projection_checkpoint_public_ids=[_ABSENT])
        )

    assert await _known_to(repository, _CHECKPOINT) == KNOWN_TO_MAX


async def test_an_active_row_the_operator_did_not_assert_refuses_the_whole_act(
    repository: SQLAlchemyRepository,
) -> None:
    """A row nobody listed is a row nobody looked at.

    Given: A request naming the position but not the active live checkpoint.
    When: The retirement is attempted.
    Then: It is refused with ``unexpected_derived_projection_set`` naming the
        unasserted row, so the improvised blast radius this writer exists to
        remove cannot reappear as an omission.
    """
    with pytest.raises(
        DerivedProjectionRetirementError,
        match=f"unexpected_derived_projection_set: plane=trade_projection_checkpoints "
        f"unasserted_active={_CHECKPOINT}",
    ):
        await repository.retire_execution_annulment_derived_projections(
            _request(expected_trade_projection_checkpoint_public_ids=[])
        )

    assert await _known_to(repository, _CHECKPOINT) == KNOWN_TO_MAX


async def test_a_version_that_started_after_the_correction_is_refused(
    repository: SQLAlchemyRepository,
) -> None:
    """A live writer is still producing state from the uncorrected ledger.

    Given: A checkpoint version whose own ``timestamp`` postdates the
        correction's knowledge instant — exactly what a trader left running
        produces.
    When: The retirement is attempted.
    Then: It is refused with ``derived_projection_postdates_correction``.
        Closing it at the correction instant would invert its interval, and the
        operator's real problem is that something must be stopped first.
    """
    async with repository.session() as s:
        checkpoint = (
            (
                await s.execute(
                    select(TradeProjectionCheckpoint).where(
                        TradeProjectionCheckpoint.public_id == _CHECKPOINT
                    )
                )
            )
            .scalars()
            .one()
        )
        checkpoint.timestamp = _OBSERVED_AT
        await s.commit()

    with pytest.raises(
        DerivedProjectionRetirementError, match="derived_projection_postdates_correction"
    ):
        await repository.retire_execution_annulment_derived_projections(_request())

    assert await _known_to(repository, _CHECKPOINT) == KNOWN_TO_MAX


async def test_a_shard_key_too_short_to_carry_a_mode_stays_in_scope(
    repository: SQLAlchemyRepository,
) -> None:
    """An unrecognized legacy key must be seen, never silently skipped.

    Given: An active checkpoint whose shard key has no mode segment at all.
    When: The live scope is read.
    Then: The row is listed, so it lands in the set the operator must assert and
        decide about, instead of being filtered away by a maintenance writer.
    """
    async with repository.session() as s:
        s.add(_checkpoint(_LEGACY_CHECKPOINT, "legacy"))
        await s.commit()

    scope = await repository.get_derived_projection_scope(_MAIN_WALLET, "live")

    assert [row["public_id"] for row in scope["trade_projection_checkpoints"]] == [
        _CHECKPOINT,
        _LEGACY_CHECKPOINT,
    ]


async def test_the_scope_read_reports_no_retirement_instant_without_a_correction(
    unobserved_repository: SQLAlchemyRepository,
) -> None:
    """No applied correction means no instant a retirement could honestly use.

    Given: A scope whose only correction carries no observation.
    When: The scope is read.
    Then: ``retired_at`` is ``None`` and the applied list is empty, so nothing
        publishes a plausible-looking timestamp no correction justifies.
    """
    scope = await unobserved_repository.get_derived_projection_scope(_MAIN_WALLET, "live")

    assert scope["applied_annulment_public_ids"] == []
    assert scope["retired_at"] is None


async def test_the_scope_read_refuses_a_malformed_wallet_identity(
    repository: SQLAlchemyRepository,
) -> None:
    """An unresolvable scope has no derived plane, empty or otherwise.

    Given: A wallet spelling that is not a UUID.
    When: The scope is read.
    Then: It refuses rather than reporting an empty plane a reader would take
        for "already clean".
    """
    with pytest.raises(ValueError, match="wallet_public_id is not a valid uuid"):
        await repository.get_derived_projection_scope("not-a-uuid", "live")


async def test_the_retirement_refuses_a_malformed_wallet_identity(
    repository: SQLAlchemyRepository,
) -> None:
    """The writer canonicalizes the scope before it opens a transaction.

    Given: A wallet spelling that is not a UUID.
    When: The retirement is attempted.
    Then: It refuses before any database work.
    """
    with pytest.raises(ValueError, match="wallet_public_id is not a valid uuid"):
        await repository.retire_execution_annulment_derived_projections(
            _request(wallet_public_id="not-a-uuid")
        )


async def test_a_version_closed_between_the_proof_and_the_close_is_refused(
    repository: SQLAlchemyRepository,
) -> None:
    """The close re-asks "is this still active?" against committed state.

    Given: A proven version whose row is no longer active by the time the
        UPDATE runs — the shape a concurrent writer produces.
    When: The close is issued for it.
    Then: The predicate matches nothing, the rowcount is zero, and the act is
        refused with ``already_retired_derived_projection`` rather than moving
        an interval somebody else already fixed.
    """
    async with repository.session() as s:
        stale = _DerivedProjectionVersion(
            plane="positions",
            row_id=9999,
            public_id=_POSITION,
            identity=_INSTRUMENT,
            version_started_at=_PROJECTION_AT,
            known_to=KNOWN_TO_MAX,
        )
        with pytest.raises(
            DerivedProjectionRetirementError, match="already_retired_derived_projection"
        ):
            await SQLAlchemyRepository._close_derived_projection_versions(
                s, Position, [stale], _KNOWN_AT
            )


async def test_the_postgresql_retirement_takes_its_own_scope_advisory_key(
    repository: SQLAlchemyRepository,
) -> None:
    """Two operators must not each see the other's rows as still active.

    Given: A repository reporting the ``postgresql`` dialect.
    When: The derived-plane fence is acquired.
    Then: It takes a scope advisory lock in a keyspace of its OWN, disjoint from
        the manifest writer's — the two acts contend for nothing, since the
        retirement never touches the ledger and the annulment never touches a
        projection.
    """
    session = AsyncMock()
    with patch.object(
        SQLAlchemyRepository,
        "dialect_name",
        new_callable=PropertyMock,
        return_value="postgresql",
    ):
        await repository._acquire_derived_projection_fence(session, _MAIN_WALLET, "live")

    statement = " ".join(str(session.execute.await_args.args[0]).split())
    assert "hashtext('execution_annulment_derived')" in statement
    assert session.execute.await_args.args[1] == {"scope": f"{_MAIN_WALLET}|live"}


async def test_an_unknown_dialect_refuses_the_derived_fence(
    repository: SQLAlchemyRepository,
) -> None:
    """A dialect with no stated guarantee never gets an implicit one.

    Given: A repository reporting a dialect this writer has no protocol for.
    When: The derived-plane fence is acquired.
    Then: It raises rather than proceeding unfenced, because a silent no-op
        would let two retirements interleave with no serialization at all.
    """
    session = AsyncMock()
    with patch.object(
        SQLAlchemyRepository,
        "dialect_name",
        new_callable=PropertyMock,
        return_value="oracle",
    ), pytest.raises(NotImplementedError, match="derived projection retirement fence"):
        await repository._acquire_derived_projection_fence(session, _MAIN_WALLET, "live")

    session.execute.assert_not_awaited()
