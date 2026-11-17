"""Real SQLite atomicity and race witnesses for dormant P&L correction control."""

from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from uuid import uuid7

import pytest
from sqlalchemy import create_engine
from sqlalchemy import delete
from sqlalchemy import select
from sqlalchemy import text

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import PortfolioPnlCorrectionFrontier
from snapper.data.models import PortfolioPnlPoint
from snapper.data.pnl_correction_state import PnlCorrectionChunk
from snapper.data.pnl_correction_state import PnlCorrectionEdit
from snapper.data.pnl_correction_state import PnlCorrectionFrontier
from snapper.data.pnl_correction_state import PnlCorrectionRequest
from snapper.data.pnl_correction_state import PnlCorrectionSampleVersion
from snapper.data.pnl_correction_state import PortfolioPnlCorrectionConflictError
from snapper.data.repository import PortfolioPnlSampleScopeError
from snapper.data.repository import PortfolioPnlSampleSupersedeError
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import PortfolioPnlSampleRow
from tests.data import test_pnl_timeline_sample_repository as samples


@pytest.fixture
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Create only the sample/control tables and one real activation anchor.

    Args:
        tmp_path: Owned disposable SQLite directory.

    Yields:
        A repository with no network or production database binding.
    """
    db_path = tmp_path / "correction.db"
    engine = create_engine(f"sqlite:///{db_path}")
    PortfolioPnlPoint.__table__.create(engine)
    PortfolioPnlCorrectionFrontier.__table__.create(engine)
    engine.dispose()
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    async with repo.session() as session:
        session.add(samples._anchor_orm())
        await session.commit()
    try:
        yield repo
    finally:
        await repo.engine.dispose()


async def _start(repository: SQLAlchemyRepository) -> PnlCorrectionFrontier:
    """Begin three minutes with an unadvanced execution-detection baseline.

    Args:
        repository: Isolated real sample/control repository.

    Returns:
        Committed generation one.
    """
    request = PnlCorrectionRequest(
        samples._M1, samples._M3, samples._M5, {"kraken": 1}, {"kraken": 5}, None
    )
    return await repository.invalidate_portfolio_pnl_correction(samples._scope(), request, None)


async def _plan(
    repository: SQLAlchemyRepository,
    frontier: PnlCorrectionFrontier,
    end: datetime,
    rows: tuple[PortfolioPnlSampleRow, ...] = (),
) -> PnlCorrectionChunk:
    """Plan inserts against the original physical presence/absence witness.

    Args:
        repository: The isolated database to observe before a simulated race.
        frontier: Durable next-minute boundary.
        end: Inclusive admitted end.
        rows: Financial inserts, absent when this is a true zero-write chunk.

    Returns:
        An explicit complete-set witness and correctly predicted peak.
    """
    assert frontier.next_minute is not None
    evidence = await repository.get_portfolio_pnl_correction_evidence(
        samples._scope(), frontier.next_minute, end
    )
    versions = {version.row["point_time"]: version for version in evidence.versions}
    edits = tuple(
        PnlCorrectionEdit(row["point_time"], versions.get(row["point_time"]), row) for row in rows
    )
    peak = 1250.0 if rows or evidence.versions else evidence.prior_peak
    return PnlCorrectionChunk(end, edits, peak, evidence)


async def test_empty_chunk_commits_explicit_progress_and_restart_keeps_it(
    repository: SQLAlchemyRepository,
) -> None:
    """No financial rows is valid progress, and no watermark is consumed early.

    Args:
        repository: Isolated real sample/control repository.
    """
    frontier = await _start(repository)
    plan = await _plan(repository, frontier, samples._M1)
    advanced = await repository.commit_portfolio_pnl_correction_chunk(
        samples._scope(), frontier, plan
    )
    assert advanced.next_minute == samples._M2
    assert advanced.baseline_watermarks == {"kraken": 1}
    assert (
        await repository.get_portfolio_pnl_samples(samples._query(), samples._M1, samples._M3) == []
    )
    resumed = SQLAlchemyRepository(str(repository.engine.url))
    try:
        assert await resumed.get_portfolio_pnl_correction_frontier(samples._scope()) == advanced
    finally:
        await resumed.engine.dispose()


async def test_financial_rows_frontier_peak_and_final_baseline_commit_together(
    repository: SQLAlchemyRepository,
) -> None:
    """Successful continuation preserves every persisted financial field.

    Args:
        repository: Isolated real sample/control repository.
    """
    frontier = await _start(repository)
    rows = (samples._complete_sample(samples._M1), samples._complete_sample(samples._M3))
    plan = await _plan(repository, frontier, samples._M3, rows)
    done = await repository.commit_portfolio_pnl_correction_chunk(samples._scope(), frontier, plan)
    assert done.next_minute is None and done.repair_from is None
    assert done.baseline_watermarks == {"kraken": 5} and done.prior_peak == 1250.0
    assert await repository.get_portfolio_pnl_samples(
        samples._query(), samples._M1, samples._M3
    ) == list(rows)
    assert await repository.get_portfolio_pnl_correction_frontier(samples._scope()) == done


async def test_generation_rewind_blocks_an_inflight_old_chunk(
    repository: SQLAlchemyRepository,
) -> None:
    """An old in-flight plan cannot overwrite a newer earlier invalidation.

    Args:
        repository: Isolated real sample/control repository.
    """
    frontier = await _start(repository)
    plan = await _plan(repository, frontier, samples._M3, (samples._complete_sample(),))
    request = PnlCorrectionRequest(samples._M1, samples._M3, samples._M5, {}, {"kraken": 6}, None)
    newer = await repository.invalidate_portfolio_pnl_correction(
        samples._scope(), request, frontier
    )
    with pytest.raises(PortfolioPnlCorrectionConflictError):
        await repository.commit_portfolio_pnl_correction_chunk(samples._scope(), frontier, plan)
    assert await repository.get_portfolio_pnl_correction_frontier(samples._scope()) == newer
    assert (
        await repository.get_portfolio_pnl_samples(samples._query(), samples._M1, samples._M3) == []
    )


async def test_completion_boundary_invalidation_retries_against_completed_generation(
    repository: SQLAlchemyRepository,
) -> None:
    """The completion row is retained, so a racing invalidation cannot disappear.

    Args:
        repository: Isolated real sample/control repository.
    """
    frontier = await _start(repository)
    plan = await _plan(repository, frontier, samples._M3)
    done = await repository.commit_portfolio_pnl_correction_chunk(samples._scope(), frontier, plan)
    request = PnlCorrectionRequest(samples._M1, samples._M3, samples._M5, {}, {"kraken": 6}, None)
    with pytest.raises(PortfolioPnlCorrectionConflictError):
        await repository.invalidate_portfolio_pnl_correction(samples._scope(), request, frontier)
    fresh = await repository.invalidate_portfolio_pnl_correction(samples._scope(), request, done)
    assert fresh.generation == done.generation + 1
    assert fresh.next_minute == samples._M1
    assert fresh.baseline_watermarks == {"kraken": 5}


@pytest.mark.parametrize("round_trip", [False, True])
async def test_identical_or_aba_scd2_replacement_is_a_physical_conflict(
    repository: SQLAlchemyRepository, round_trip: bool
) -> None:
    """The same public id, timestamp and final contents do not hide a new version.

    Args:
        repository: Isolated real sample/control repository.
        round_trip: Whether a changed financial version returns to its original value.
    """
    row = samples._complete_sample(samples._M2)
    await repository.record_portfolio_pnl_samples([row], samples._scope())
    frontier = await _start(repository)
    plan = await _plan(repository, frontier, samples._M3)
    intermediate = samples._mutate(row, realized_pnl=99.0) if round_trip else row
    await repository.supersede_portfolio_pnl_sample(
        samples._scope(),
        intermediate,
        derived_suffix_reconciliation=True,
        expected_public_id=row["public_id"],
    )
    if round_trip:
        await repository.supersede_portfolio_pnl_sample(
            samples._scope(),
            row,
            derived_suffix_reconciliation=True,
            expected_public_id=row["public_id"],
        )
    assert (await repository.get_portfolio_pnl_samples(samples._query(), samples._M2, samples._M2))[
        0
    ] == row
    with pytest.raises(PortfolioPnlCorrectionConflictError, match="evidence changed"):
        await repository.commit_portfolio_pnl_correction_chunk(samples._scope(), frontier, plan)
    assert await repository.get_portfolio_pnl_correction_frontier(samples._scope()) == frontier


async def test_new_presence_conflicts_even_for_zero_edit_chunk(
    repository: SQLAlchemyRepository,
) -> None:
    """A minute planned as absent cannot silently acquire an unexamined sample.

    Args:
        repository: Isolated real sample/control repository.
    """
    frontier = await _start(repository)
    plan = await _plan(repository, frontier, samples._M3)
    await repository.record_portfolio_pnl_samples([samples._complete_sample()], samples._scope())
    with pytest.raises(PortfolioPnlCorrectionConflictError, match="evidence changed"):
        await repository.commit_portfolio_pnl_correction_chunk(samples._scope(), frontier, plan)
    assert await repository.get_portfolio_pnl_correction_frontier(samples._scope()) == frontier


async def test_failure_after_earlier_insert_rolls_back_rows_and_control(
    repository: SQLAlchemyRepository,
) -> None:
    """An edit inconsistent with the original set cannot leave a partial financial commit.

    Args:
        repository: Isolated real sample/control repository.
    """
    frontier = await _start(repository)
    first, second = samples._complete_sample(samples._M1), samples._complete_sample(samples._M2)
    plan = await _plan(repository, frontier, samples._M3, (first, second))
    forged = replace(plan.edits[1], expected_row=PnlCorrectionSampleVersion(999999, second))
    invalid = replace(plan, edits=(plan.edits[0], forged))
    with pytest.raises(PortfolioPnlCorrectionConflictError, match="stale version"):
        await repository.commit_portfolio_pnl_correction_chunk(samples._scope(), frontier, invalid)
    assert (
        await repository.get_portfolio_pnl_samples(samples._query(), samples._M1, samples._M3) == []
    )
    assert await repository.get_portfolio_pnl_correction_frontier(samples._scope()) == frontier


async def test_withheld_zero_edit_completion_keeps_baseline_and_pending_repair(
    repository: SQLAlchemyRepository,
) -> None:
    """Scanning a withheld minute must not consume its execution-detection baseline.

    Args:
        repository: Isolated real sample/control repository.
    """
    frontier = await _start(repository)
    plan = replace(await _plan(repository, frontier, samples._M3), withheld_minutes=(samples._M2,))
    done = await repository.commit_portfolio_pnl_correction_chunk(samples._scope(), frontier, plan)
    assert done.next_minute is None and done.repair_from == samples._M2
    assert done.baseline_watermarks == {"kraken": 1}
    request = PnlCorrectionRequest(samples._M3, samples._M3, samples._M5, {}, {"kraken": 6}, None)
    with pytest.raises(ValueError, match="repair boundary"):
        await repository.invalidate_portfolio_pnl_correction(samples._scope(), request, done)
    renewed = await repository.invalidate_portfolio_pnl_correction(
        samples._scope(), replace(request, start=samples._M2), done
    )
    assert renewed.next_minute == samples._M2
    assert renewed.baseline_watermarks == {"kraken": 1}
    assert renewed.repair_from == samples._M2
    repair_plan = await _plan(repository, renewed, samples._M3)
    still_pending = await repository.commit_portfolio_pnl_correction_chunk(
        samples._scope(), renewed, repair_plan
    )
    assert still_pending.repair_from == samples._M2
    assert still_pending.baseline_watermarks == {"kraken": 1}
    retry = await repository.invalidate_portfolio_pnl_correction(
        samples._scope(), replace(request, start=samples._M2), still_pending
    )
    reconciled = replace(await _plan(repository, retry, samples._M3), reconciles_repair=True)
    done = await repository.commit_portfolio_pnl_correction_chunk(
        samples._scope(), retry, reconciled
    )
    assert done.repair_from is None
    assert done.baseline_watermarks == {"kraken": 6}


async def test_earlier_peak_change_invalidates_plan_without_current_chunk_changes(
    repository: SQLAlchemyRepository,
) -> None:
    """The sample fence alone does not certify the causal input used before it was taken.

    Args:
        repository: Isolated real sample/control repository.
    """
    row = samples._complete_sample(samples._M1)
    await repository.record_portfolio_pnl_samples([row], samples._scope())
    request = PnlCorrectionRequest(samples._M2, samples._M3, samples._M5, {}, {"kraken": 5}, 1250.0)
    frontier = await repository.invalidate_portfolio_pnl_correction(samples._scope(), request, None)
    plan = await _plan(repository, frontier, samples._M3)
    await repository.supersede_portfolio_pnl_sample(
        samples._scope(),
        samples._mutate(row, cash_usd=2000.0),
        derived_suffix_reconciliation=True,
        expected_public_id=row["public_id"],
    )
    with pytest.raises(PortfolioPnlCorrectionConflictError, match="evidence changed"):
        await repository.commit_portfolio_pnl_correction_chunk(samples._scope(), frontier, plan)
    assert await repository.get_portfolio_pnl_correction_frontier(samples._scope()) == frontier


async def test_new_evidence_cannot_silently_adopt_a_changed_checkpoint_peak(
    repository: SQLAlchemyRepository,
) -> None:
    """A refreshed chunk witness still needs a rewind if the saved causal prefix changed.

    Args:
        repository: Isolated real sample/control repository.
    """
    row = samples._complete_sample(samples._M1)
    await repository.record_portfolio_pnl_samples([row], samples._scope())
    request = PnlCorrectionRequest(samples._M2, samples._M3, samples._M5, {}, {"kraken": 5}, 1250.0)
    frontier = await repository.invalidate_portfolio_pnl_correction(samples._scope(), request, None)
    await repository.supersede_portfolio_pnl_sample(
        samples._scope(),
        samples._mutate(row, cash_usd=2000.0),
        derived_suffix_reconciliation=True,
        expected_public_id=row["public_id"],
    )
    refreshed = await _plan(repository, frontier, samples._M3)
    with pytest.raises(PortfolioPnlCorrectionConflictError, match="checkpoint causal peak"):
        await repository.commit_portfolio_pnl_correction_chunk(
            samples._scope(), frontier, refreshed
        )
    assert await repository.get_portfolio_pnl_correction_frontier(samples._scope()) == frontier


async def test_real_replacement_and_retraction_are_one_atomic_chunk(
    repository: SQLAlchemyRepository,
) -> None:
    """A successful correction retains history while replacing and retracting exactly once.

    Args:
        repository: Isolated real sample/control repository.
    """
    first, second = samples._complete_sample(samples._M1), samples._complete_sample(samples._M2)
    await repository.record_portfolio_pnl_samples([first, second], samples._scope())
    frontier = await _start(repository)
    plan = await _plan(repository, frontier, samples._M3)
    versions = {version.row["point_time"]: version for version in plan.evidence.versions}
    replacement = samples._mutate(first, timestamp=samples._M5, realized_pnl=99.0)
    edits = (
        PnlCorrectionEdit(samples._M1, versions[samples._M1], replacement),
        PnlCorrectionEdit(samples._M2, versions[samples._M2], None),
    )
    done = await repository.commit_portfolio_pnl_correction_chunk(
        samples._scope(), frontier, replace(plan, edits=edits)
    )
    active = await repository.get_portfolio_pnl_samples(samples._query(), samples._M1, samples._M3)
    assert len(active) == 1 and active[0]["realized_pnl"] == 99.0
    assert active[0]["public_id"] == first["public_id"]
    assert done.next_minute is None
    async with repository.session() as session:
        all_rows = list((await session.execute(select(PortfolioPnlPoint))).scalars())
    assert len(all_rows) == 4
    assert sum(row.known_to != KNOWN_TO_MAX for row in all_rows) == 2


async def test_old_calculation_row_requires_explicit_withheld_repair(
    repository: SQLAlchemyRepository,
) -> None:
    """An old active row is visible to the whole-set witness and cannot be silently skipped.

    Args:
        repository: Isolated real sample/control repository.
    """
    row = samples._complete_sample(samples._M2)
    await repository.record_portfolio_pnl_samples([row], samples._scope())
    async with repository.session() as session:
        stored = (
            await session.execute(
                select(PortfolioPnlPoint).where(PortfolioPnlPoint.point_kind == "sample")
            )
        ).scalar_one()
        stored.calc_version = "5B.1"
        await session.commit()
    frontier = await _start(repository)
    plan = await _plan(repository, frontier, samples._M3)
    assert len(plan.evidence.versions) == 1
    assert plan.evidence.versions[0].row["calc_version"] == "5B.1"
    with pytest.raises(ValueError, match="old-version evidence"):
        await repository.commit_portfolio_pnl_correction_chunk(samples._scope(), frontier, plan)
    retained = replace(plan, prior_peak=None, withheld_minutes=(samples._M2,))
    done = await repository.commit_portfolio_pnl_correction_chunk(
        samples._scope(), frontier, retained
    )
    assert done.baseline_watermarks == {"kraken": 1}
    assert done.repair_from == samples._M2
    assert (
        await repository.get_portfolio_pnl_samples(samples._query(), samples._M1, samples._M3) == []
    )


async def test_checkpoint_claim_cannot_cross_scope_or_algorithm(
    repository: SQLAlchemyRepository,
) -> None:
    """Equal revision numbers do not turn another scope's token into this scope's token.

    Args:
        repository: Isolated real sample/control repository.
    """
    frontier = await _start(repository)
    plan = await _plan(repository, frontier, samples._M3)
    wrong = replace(frontier, key=replace(frontier.key, calc_version="old"))
    with pytest.raises(PortfolioPnlCorrectionConflictError, match="checkpoint CAS"):
        await repository.commit_portfolio_pnl_correction_chunk(samples._scope(), wrong, plan)
    assert (
        await repository.get_portfolio_pnl_correction_frontier(
            samples._scope(epoch=samples._OTHER_EPOCH)
        )
        is None
    )
    request = PnlCorrectionRequest(samples._M1, samples._M3, samples._M5, {}, {}, None)
    with pytest.raises(PortfolioPnlSampleScopeError):
        await repository.invalidate_portfolio_pnl_correction(
            samples._scope(epoch=samples._OTHER_EPOCH), request, None
        )


async def test_failed_completion_peak_verification_rolls_back_successful_edits(
    repository: SQLAlchemyRepository,
) -> None:
    """A wrong planned peak cannot leave either new money rows or progress committed.

    Args:
        repository: Isolated real sample/control repository.
    """
    frontier = await _start(repository)
    plan = await _plan(repository, frontier, samples._M3, (samples._complete_sample(),))
    with pytest.raises(PortfolioPnlCorrectionConflictError, match="resulting causal peak"):
        await repository.commit_portfolio_pnl_correction_chunk(
            samples._scope(), frontier, replace(plan, prior_peak=9999.0)
        )
    assert (
        await repository.get_portfolio_pnl_samples(samples._query(), samples._M1, samples._M3) == []
    )
    assert await repository.get_portfolio_pnl_correction_frontier(samples._scope()) == frontier


async def test_initial_causal_peak_is_verified_inside_the_creation_fence(
    repository: SQLAlchemyRepository,
) -> None:
    """Even an initially absent checkpoint cannot certify a stale planned prefix.

    Args:
        repository: Isolated real sample/control repository.
    """
    request = PnlCorrectionRequest(samples._M1, samples._M3, samples._M5, {}, {}, 42.0)
    with pytest.raises(PortfolioPnlCorrectionConflictError, match="initial causal peak"):
        await repository.invalidate_portfolio_pnl_correction(samples._scope(), request, None)
    assert await repository.get_portfolio_pnl_correction_frontier(samples._scope()) is None


@pytest.mark.parametrize(
    "payload", ["[]", '{"kraken": "1"}', '{"kraken": true}', '{"kraken": -1}', "{"]
)
async def test_corrupt_persisted_watermarks_are_refused_without_reset(
    repository: SQLAlchemyRepository, payload: str
) -> None:
    """Bad durable control raises instead of inventing an empty baseline.

    Args:
        repository: Isolated real sample/control repository.
        payload: A deliberately corrupted control map in the throwaway database.
    """
    await _start(repository)
    async with repository.session() as session:
        stored = (await session.execute(select(PortfolioPnlCorrectionFrontier))).scalar_one()
        stored.baseline_watermarks_json = payload
        await session.commit()
    with pytest.raises(ValueError):
        await repository.get_portfolio_pnl_correction_frontier(samples._scope())


async def test_database_refusal_of_final_cas_rolls_back_preceding_financial_insert(
    repository: SQLAlchemyRepository,
) -> None:
    """A database-side rejected progress update cannot leave a committed partial chunk.

    Args:
        repository: Isolated real sample/control repository.
    """
    frontier = await _start(repository)
    plan = await _plan(repository, frontier, samples._M3, (samples._complete_sample(),))
    async with repository.session() as session:
        await session.execute(
            text(
                "CREATE TRIGGER reject_correction_progress BEFORE UPDATE ON "
                "portfolio_pnl_correction_frontiers BEGIN SELECT RAISE(IGNORE); END"
            )
        )
        await session.commit()
    with pytest.raises(PortfolioPnlCorrectionConflictError, match="final checkpoint CAS"):
        await repository.commit_portfolio_pnl_correction_chunk(samples._scope(), frontier, plan)
    assert (
        await repository.get_portfolio_pnl_samples(samples._query(), samples._M1, samples._M3) == []
    )
    assert await repository.get_portfolio_pnl_correction_frontier(samples._scope()) == frontier


async def test_database_refusal_of_physical_close_preserves_original_and_progress(
    repository: SQLAlchemyRepository,
) -> None:
    """A failed physical-version close must not be treated as a successful retraction.

    Args:
        repository: Isolated real sample/control repository.
    """
    original = samples._complete_sample(samples._M2)
    await repository.record_portfolio_pnl_samples([original], samples._scope())
    frontier = await _start(repository)
    plan = await _plan(repository, frontier, samples._M3)
    observed = plan.evidence.versions[0]
    edits = (PnlCorrectionEdit(samples._M2, observed, None),)
    async with repository.session() as session:
        await session.execute(
            text(
                "CREATE TRIGGER reject_sample_close BEFORE UPDATE OF known_to ON "
                "portfolio_pnl_points BEGIN SELECT RAISE(IGNORE); END"
            )
        )
        await session.commit()
    with pytest.raises(PortfolioPnlCorrectionConflictError, match="financial close CAS"):
        await repository.commit_portfolio_pnl_correction_chunk(
            samples._scope(), frontier, replace(plan, edits=edits, prior_peak=None)
        )
    assert await repository.get_portfolio_pnl_samples(
        samples._query(), samples._M1, samples._M3
    ) == [original]
    assert await repository.get_portfolio_pnl_correction_frontier(samples._scope()) == frontier


@pytest.mark.parametrize("offset_minutes", [0, 1441])
async def test_evidence_read_cannot_become_an_anchor_or_unbounded_range(
    repository: SQLAlchemyRepository, offset_minutes: int
) -> None:
    """The public witness reader rejects a whole-history scan at its input boundary.

    Args:
        repository: Isolated real sample/control repository.
        offset_minutes: An anchor-start or excessive-window request.
    """
    start = samples._T0 if offset_minutes == 0 else samples._M1
    end = start + timedelta(minutes=offset_minutes)
    with pytest.raises(ValueError, match="bounded post-anchor"):
        await repository.get_portfolio_pnl_correction_evidence(samples._scope(), start, end)


async def test_empty_checkpoint_claim_is_rechecked_under_the_fence(
    repository: SQLAlchemyRepository,
) -> None:
    """A second creator cannot replace a generation just because its earlier read was empty.

    Args:
        repository: Isolated real sample/control repository.
    """
    assert await repository.get_portfolio_pnl_correction_frontier(samples._scope()) is None
    frontier = await _start(repository)
    with pytest.raises(PortfolioPnlCorrectionConflictError):
        await _start(repository)
    assert await repository.get_portfolio_pnl_correction_frontier(samples._scope()) == frontier


async def test_replacement_identity_must_match_its_admitted_minute(
    repository: SQLAlchemyRepository,
) -> None:
    """A valid financial row for another minute cannot masquerade as this edit.

    Args:
        repository: Isolated real sample/control repository.
    """
    frontier = await _start(repository)
    plan = await _plan(repository, frontier, samples._M3)
    wrong = PnlCorrectionEdit(samples._M1, None, samples._complete_sample(samples._M2))
    with pytest.raises(ValueError, match="minute must agree"):
        await repository.commit_portfolio_pnl_correction_chunk(
            samples._scope(), frontier, replace(plan, edits=(wrong,))
        )
    assert await repository.get_portfolio_pnl_correction_frontier(samples._scope()) == frontier


async def test_lost_control_row_is_a_conflict_not_an_implicit_sample_cursor(
    repository: SQLAlchemyRepository,
) -> None:
    """A lost checkpoint cannot be reconstructed and advanced from financial rows.

    Args:
        repository: Isolated real sample/control repository.
    """
    frontier = await _start(repository)
    plan = await _plan(repository, frontier, samples._M3)
    async with repository.session() as session:
        await session.execute(delete(PortfolioPnlCorrectionFrontier))
        await session.commit()
    with pytest.raises(PortfolioPnlCorrectionConflictError, match="checkpoint CAS"):
        await repository.commit_portfolio_pnl_correction_chunk(samples._scope(), frontier, plan)
    assert await repository.get_portfolio_pnl_correction_frontier(samples._scope()) is None


async def test_anchor_minute_cannot_become_correction_progress(
    repository: SQLAlchemyRepository,
) -> None:
    """Even a zero-write request cannot claim the activation anchor as a sample.

    Args:
        repository: Isolated real sample/control repository.
    """
    request = PnlCorrectionRequest(samples._T0, samples._M3, samples._M5, {}, {}, None)
    with pytest.raises(PortfolioPnlSampleScopeError, match="after its anchor"):
        await repository.invalidate_portfolio_pnl_correction(samples._scope(), request, None)
    assert await repository.get_portfolio_pnl_correction_frontier(samples._scope()) is None


async def test_another_epochs_active_sample_cannot_be_overwritten(
    repository: SQLAlchemyRepository,
) -> None:
    """The wider active uniqueness key never permits crossing the verified anchor epoch.

    Args:
        repository: Isolated real sample/control repository.
    """
    row = samples._complete_sample(samples._M2)
    await repository.record_portfolio_pnl_samples([row], samples._scope())
    async with repository.session() as session:
        stored = (
            await session.execute(
                select(PortfolioPnlPoint).where(PortfolioPnlPoint.point_kind == "sample")
            )
        ).scalar_one()
        stored.epoch_public_id = samples._OTHER_EPOCH
        await session.commit()
    frontier = await _start(repository)
    plan = await _plan(repository, frontier, samples._M3, (samples._complete_sample(samples._M2),))
    with pytest.raises(PortfolioPnlSampleSupersedeError, match="another epoch"):
        await repository.commit_portfolio_pnl_correction_chunk(samples._scope(), frontier, plan)
    assert await repository.get_portfolio_pnl_correction_frontier(samples._scope()) == frontier


async def test_evidence_overflow_is_refused_instead_of_silently_truncated(
    repository: SQLAlchemyRepository,
) -> None:
    """Malformed legacy subminute rows cannot make a bounded read allocate an entire history.

    Args:
        repository: Isolated real sample/control repository.
    """
    async with repository.session() as session:
        for index in range(1441):
            point = samples._anchor_orm()
            point.public_id = str(uuid7())
            point.point_kind = "sample"
            point.point_time = samples._M1 + timedelta(microseconds=index)
            session.add(point)
        await session.commit()
    with pytest.raises(ValueError, match="fixed row bound"):
        await repository.get_portfolio_pnl_correction_evidence(
            samples._scope(), samples._M1, samples._M3
        )
