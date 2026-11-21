"""Real SQLite atomicity and race witnesses for dormant P&L correction control."""

from collections.abc import AsyncIterator
from collections.abc import Sequence
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

from snapper.application.portfolio.pnl_correction_planner import PnlCorrectionPlanInputs
from snapper.application.portfolio.pnl_correction_planner import plan_correction_chunk
from snapper.application.portfolio.pnl_snapshot_planner import MinuteInputs
from snapper.application.portfolio.pnl_snapshot_planner import plan_chunk_samples
from snapper.application.portfolio.pnl_timeline import PnlIncompletenessReasonEntry
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
from tests.application.portfolio import test_pnl_snapshot_planner as planner
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
    rows = (samples._complete_sample(samples._M2), samples._complete_sample(samples._M3))
    reconciled = replace(await _plan(repository, retry, samples._M3, rows), reconciles_repair=True)
    done = await repository.commit_portfolio_pnl_correction_chunk(
        samples._scope(), retry, reconciled
    )
    assert done.repair_from is None
    assert done.baseline_watermarks == {"kraken": 6}


@pytest.mark.parametrize("has_insert", [False, True])
async def test_acknowledged_repair_with_missing_result_refuses_all_progress(
    repository: SQLAlchemyRepository, has_insert: bool
) -> None:
    """A repair certificate cannot consume its baseline while a non-withheld minute is absent.

    Args:
        repository: Isolated real sample/control repository.
        has_insert: Whether an earlier minute has a valid proposed financial insert.
    """
    initial = await _start(repository)
    withheld = replace(
        await _plan(repository, initial, samples._M3), withheld_minutes=(samples._M2,)
    )
    scanned = await repository.commit_portfolio_pnl_correction_chunk(
        samples._scope(), initial, withheld
    )
    request = PnlCorrectionRequest(samples._M2, samples._M3, samples._M5, {}, {"kraken": 6}, None)
    retry = await repository.invalidate_portfolio_pnl_correction(samples._scope(), request, scanned)
    rows = (samples._complete_sample(samples._M2),) if has_insert else ()
    invalid = replace(await _plan(repository, retry, samples._M3, rows), reconciles_repair=True)
    async with repository.session() as session:
        before = (await session.execute(select(PortfolioPnlPoint.__table__))).all()
    with pytest.raises(ValueError, match="repair acknowledgement"):
        await repository.commit_portfolio_pnl_correction_chunk(samples._scope(), retry, invalid)
    async with repository.session() as session:
        assert (await session.execute(select(PortfolioPnlPoint.__table__))).all() == before
    assert await repository.get_portfolio_pnl_correction_frontier(samples._scope()) == retry


async def test_real_planner_unchanged_rows_allow_zero_edit_repair_acknowledgement(
    repository: SQLAlchemyRepository,
) -> None:
    """A successful current planner output may certify unchanged rows without rewriting money.

    Args:
        repository: Isolated real sample/control repository.
    """
    inputs = tuple(
        MinuteInputs(
            planner._complete_point(minute),
            {"kraken": planner._attempt(minute=minute)},
            planner._crypto_evidence(),
        )
        for minute in (samples._M2, samples._M3)
    )
    planned = plan_chunk_samples(inputs, frozenset({"kraken"}), (), None)
    assert len(planned.samples) == 2 and planned.withheld == ()
    rows = tuple(
        samples._mutate(
            planner._to_row(row),
            wallet_public_id=samples._WALLET,
            epoch_public_id=samples._EPOCH,
            timestamp=samples._M5,
        )
        for row in planned.samples
    )
    await repository.record_portfolio_pnl_samples(rows, samples._scope())
    initial = await _start(repository)
    withheld = replace(
        await _plan(repository, initial, samples._M3),
        prior_peak=planned.peak,
        withheld_minutes=(samples._M2,),
    )
    scanned = await repository.commit_portfolio_pnl_correction_chunk(
        samples._scope(), initial, withheld
    )
    request = PnlCorrectionRequest(samples._M2, samples._M3, samples._M5, {}, {"kraken": 6}, None)
    retry = await repository.invalidate_portfolio_pnl_correction(samples._scope(), request, scanned)
    acknowledged = replace(
        await _plan(repository, retry, samples._M3),
        prior_peak=planned.peak,
        reconciles_repair=True,
    )
    async with repository.session() as session:
        before = (await session.execute(select(PortfolioPnlPoint.__table__))).all()
    done = await repository.commit_portfolio_pnl_correction_chunk(
        samples._scope(), retry, acknowledged
    )
    assert done.repair_from is None and done.baseline_watermarks == {"kraken": 6}
    async with repository.session() as session:
        assert (await session.execute(select(PortfolioPnlPoint.__table__))).all() == before


async def test_real_planner_withheld_retraction_keeps_repair_and_baseline(
    repository: SQLAlchemyRepository,
) -> None:
    """A newly untrusted minute closes its old row and retains its explicit repair obligation.

    Args:
        repository: Isolated real sample/control repository.
    """
    row = samples._complete_sample(samples._M2)
    await repository.record_portfolio_pnl_samples([row], samples._scope())
    inputs = tuple(
        MinuteInputs(planner._untrusted_point(minute), {}, planner._crypto_evidence())
        for minute in (samples._M2, samples._M3)
    )
    planned = plan_chunk_samples(inputs, frozenset({"kraken"}), (), None)
    assert planned.samples == () and planned.withheld[0].reason_codes == ("fill_evidence_gap",)
    initial = await _start(repository)
    first = replace(await _plan(repository, initial, samples._M3), withheld_minutes=(samples._M2,))
    scanned = await repository.commit_portfolio_pnl_correction_chunk(
        samples._scope(), initial, first
    )
    request = PnlCorrectionRequest(samples._M2, samples._M3, samples._M5, {}, {"kraken": 6}, None)
    retry = await repository.invalidate_portfolio_pnl_correction(samples._scope(), request, scanned)
    plan = await _plan(repository, retry, samples._M3)
    retraction = PnlCorrectionEdit(samples._M2, plan.evidence.versions[0], None)
    withheld = replace(
        plan,
        edits=(retraction,),
        prior_peak=planned.peak,
        withheld_minutes=tuple(minute.point_time for minute in planned.withheld),
        reconciles_repair=True,
    )
    done = await repository.commit_portfolio_pnl_correction_chunk(samples._scope(), retry, withheld)
    assert done.repair_from == samples._M2 and done.baseline_watermarks == {"kraken": 1}
    assert (
        await repository.get_portfolio_pnl_samples(samples._query(), samples._M1, samples._M3) == []
    )
    async with repository.session() as session:
        stored = (
            await session.execute(
                select(PortfolioPnlPoint).where(PortfolioPnlPoint.point_kind == "sample")
            )
        ).scalar_one()
        assert repository._portfolio_pnl_sample_to_row(stored) == row
        assert stored.known_to == samples._M5


@pytest.mark.parametrize("had_row", [False, True])
async def test_withheld_minute_still_refuses_inserts_and_replacements(
    repository: SQLAlchemyRepository, had_row: bool
) -> None:
    """The retraction exception cannot stamp a fresh value onto a withheld minute.

    Args:
        repository: Isolated real sample/control repository.
        had_row: Whether the invalid edit replaces an existing row instead of inserting.
    """
    row = samples._complete_sample(samples._M2)
    if had_row:
        await repository.record_portfolio_pnl_samples([row], samples._scope())
    initial = await _start(repository)
    invalid = replace(
        await _plan(repository, initial, samples._M3, (row,)),
        withheld_minutes=(samples._M2,),
    )
    async with repository.session() as session:
        before = (await session.execute(select(PortfolioPnlPoint.__table__))).all()
    with pytest.raises(ValueError, match="only observed-row retractions"):
        await repository.commit_portfolio_pnl_correction_chunk(samples._scope(), initial, invalid)
    async with repository.session() as session:
        assert (await session.execute(select(PortfolioPnlPoint.__table__))).all() == before
    assert await repository.get_portfolio_pnl_correction_frontier(samples._scope()) == initial


async def test_acknowledgement_without_an_outstanding_repair_preserves_sparse_progress(
    repository: SQLAlchemyRepository,
) -> None:
    """Acknowledgement does not impose an unrelated occupancy rule on initial sparse chunks.

    Args:
        repository: Isolated real sample/control repository.
    """
    initial = await _start(repository)
    plan = replace(await _plan(repository, initial, samples._M3), reconciles_repair=True)
    done = await repository.commit_portfolio_pnl_correction_chunk(samples._scope(), initial, plan)
    assert done.next_minute is None and done.repair_from is None
    assert done.baseline_watermarks == {"kraken": 5}


@pytest.mark.parametrize("repair_after_chunk", [False, True])
async def test_acknowledgement_outside_the_outstanding_boundary_cannot_clear_it(
    repository: SQLAlchemyRepository, repair_after_chunk: bool
) -> None:
    """An acknowledgement is scoped to its admitted outstanding repair, not arbitrary progress.

    Args:
        repository: Isolated real sample/control repository.
        repair_after_chunk: Whether the repair is ahead of this chunk rather than behind it.
    """
    initial = await _start(repository)
    end = samples._M3 if repair_after_chunk else samples._M1
    first = replace(await _plan(repository, initial, end), withheld_minutes=(end,))
    pending = await repository.commit_portfolio_pnl_correction_chunk(
        samples._scope(), initial, first
    )
    if repair_after_chunk:
        request = PnlCorrectionRequest(
            samples._M1, samples._M3, samples._M5, {}, {"kraken": 6}, None
        )
        pending = await repository.invalidate_portfolio_pnl_correction(
            samples._scope(), request, pending
        )
    stop = samples._M1 if repair_after_chunk else samples._M3
    plan = replace(await _plan(repository, pending, stop), reconciles_repair=True)
    done = await repository.commit_portfolio_pnl_correction_chunk(samples._scope(), pending, plan)
    assert done.repair_from == end and done.baseline_watermarks == {"kraken": 1}


@pytest.mark.parametrize("foreign_epoch", [False, True])
async def test_acknowledgement_with_foreign_rows_keeps_the_explicit_withheld_obligation(
    repository: SQLAlchemyRepository, foreign_epoch: bool
) -> None:
    """Only this key's post-edit rows cover repair; foreign evidence remains retained for repair.

    Args:
        repository: Isolated real sample/control repository.
        foreign_epoch: Whether the observed row belongs to another epoch or calculation version.
    """
    await repository.record_portfolio_pnl_samples(
        [samples._complete_sample(samples._M2)], samples._scope()
    )
    async with repository.session() as session:
        old = (
            await session.execute(
                select(PortfolioPnlPoint).where(PortfolioPnlPoint.point_kind == "sample")
            )
        ).scalar_one()
        if foreign_epoch:
            old.epoch_public_id = samples._OTHER_EPOCH
        else:
            old.calc_version = "legacy"
        await session.commit()
    initial = await _start(repository)
    first = replace(
        await _plan(repository, initial, samples._M3),
        prior_peak=None,
        withheld_minutes=(samples._M2,),
    )
    scanned = await repository.commit_portfolio_pnl_correction_chunk(
        samples._scope(), initial, first
    )
    request = PnlCorrectionRequest(samples._M2, samples._M3, samples._M5, {}, {"kraken": 6}, None)
    retry = await repository.invalidate_portfolio_pnl_correction(samples._scope(), request, scanned)
    plan = replace(
        await _plan(repository, retry, samples._M3, (samples._complete_sample(samples._M3),)),
        withheld_minutes=(samples._M2,),
        reconciles_repair=True,
    )
    before = plan.evidence.versions[0]
    done = await repository.commit_portfolio_pnl_correction_chunk(samples._scope(), retry, plan)
    after = await repository.get_portfolio_pnl_correction_evidence(
        samples._scope(), samples._M2, samples._M3
    )
    assert after.versions[0] == before
    assert done.repair_from == samples._M2 and done.baseline_watermarks == {"kraken": 1}


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


@pytest.mark.parametrize(
    ("cut", "manifest", "has_fill_gap", "venues"),
    [
        (None, None, None, None),
        ({}, "a" * 64, False, frozenset({"kraken"})),
        ({"kraken": 8, "walutomat": 4}, "f" * 64, True, frozenset({"walutomat", "kraken"})),
    ],
)
async def test_execution_evidence_round_trips_with_each_financial_revision(
    repository: SQLAlchemyRepository,
    cut: dict[str, int] | None,
    manifest: str | None,
    has_fill_gap: bool | None,
    venues: frozenset[str] | None,
) -> None:
    """Read back legacy nulls and certified replay evidence.

    Given a checkpoint with legacy nulls or captured execution evidence and spot venues.
    When the repository starts a generation and commits its first financial chunk.
    Then both revisions round-trip the exact evidence and venue set.
    """
    scope = samples._scope()
    witness = {"kraken:spot": 215} if cut is not None else None
    witness_digest = "c" * 64 if cut is not None else None
    request = PnlCorrectionRequest(
        samples._M1,
        samples._M3,
        samples._M5,
        {},
        {},
        None,
        raw_execution_watermarks=cut,
        execution_manifest_digest=manifest,
        has_fill_gap=has_fill_gap,
        expected_venues=venues,
        venue_event_watermarks=witness,
        fill_witness_digest=witness_digest,
    )
    started = await repository.invalidate_portfolio_pnl_correction(scope, request, None)
    assert started.raw_execution_watermarks == cut
    assert started.execution_manifest_digest == manifest
    assert started.has_fill_gap is has_fill_gap
    loaded = await repository.get_portfolio_pnl_correction_frontier(scope)
    assert loaded == started and loaded is not None
    assert loaded.has_fill_gap is has_fill_gap
    assert loaded.expected_venues == venues
    assert loaded.venue_event_watermarks == witness
    assert loaded.fill_witness_digest == witness_digest

    plan = await _plan(repository, started, samples._M2, (samples._complete_sample(),))
    advanced = await repository.commit_portfolio_pnl_correction_chunk(scope, started, plan)
    assert advanced.revision == started.revision + 1
    assert advanced.raw_execution_watermarks == cut
    assert advanced.execution_manifest_digest == manifest
    assert advanced.has_fill_gap is has_fill_gap
    loaded = await repository.get_portfolio_pnl_correction_frontier(scope)
    assert loaded == advanced and loaded is not None
    assert loaded.has_fill_gap is has_fill_gap
    assert loaded.expected_venues == venues
    assert loaded.venue_event_watermarks == witness
    assert loaded.fill_witness_digest == witness_digest


async def test_reconciled_sample_query_exposes_only_the_settled_prefix(
    repository: SQLAlchemyRepository,
) -> None:
    """Expose equity through the settled correction prefix.

    Given three stored samples and a correction spanning the last two minutes.
    When a read follows correction advancement, a withheld minute, and its repair.
    Then serving reads expose only the settled prefix and ordinary reads retain all three rows.
    """
    scope = samples._scope()
    query = samples._query()
    published = replace(query, require_reconciled=True)
    rows = tuple(
        samples._complete_sample(minute) for minute in (samples._M1, samples._M2, samples._M3)
    )
    await repository.record_portfolio_pnl_samples(rows, scope)
    assert await repository.get_portfolio_pnl_samples(published, samples._M1, samples._M3) == list(
        rows
    )

    request = PnlCorrectionRequest(samples._M2, samples._M3, samples._M5, {}, {}, 1250.0)
    started = await repository.invalidate_portfolio_pnl_correction(scope, request, None)
    assert await repository.get_portfolio_pnl_samples(published, samples._M1, samples._M3) == list(
        rows[:1]
    )
    plan = await _plan(repository, started, samples._M2)
    advanced = await repository.commit_portfolio_pnl_correction_chunk(scope, started, plan)
    assert await repository.get_portfolio_pnl_samples(published, samples._M1, samples._M3) == list(
        rows[:2]
    )

    withheld = replace(
        await _plan(repository, advanced, samples._M3), withheld_minutes=(samples._M3,)
    )
    blocked = await repository.commit_portfolio_pnl_correction_chunk(scope, advanced, withheld)
    assert blocked.next_minute is None and blocked.repair_from == samples._M3
    assert await repository.get_portfolio_pnl_samples(published, samples._M1, samples._M3) == list(
        rows[:2]
    )
    assert await repository.get_portfolio_pnl_samples(query, samples._M1, samples._M3) == list(rows)

    repaired = await repository.invalidate_portfolio_pnl_correction(
        scope, replace(request, start=samples._M3), blocked
    )
    acknowledged = replace(await _plan(repository, repaired, samples._M3), reconciles_repair=True)
    settled = await repository.commit_portfolio_pnl_correction_chunk(scope, repaired, acknowledged)
    assert settled.next_minute is None and settled.repair_from is None
    assert await repository.get_portfolio_pnl_samples(published, samples._M1, samples._M3) == list(
        rows
    )


async def test_reconciled_sample_query_ignores_unrelated_correction_keys(
    repository: SQLAlchemyRepository,
) -> None:
    """Scope serving visibility to the exact correction key.

    Given a stored sample and five active frontiers that each differ in one key field.
    When the repository queries the sample with reconciliation required.
    Then the sample remains visible for its own wallet, mode, currency, epoch and algorithm.
    """
    scope = samples._scope()
    query = replace(samples._query(), require_reconciled=True)
    row = samples._complete_sample()
    await repository.record_portfolio_pnl_samples([row], scope)
    matching: dict[str, object] = {
        "wallet_public_id": query.wallet_public_id,
        "mode": query.mode,
        "valuation_ccy": query.valuation_ccy,
        "epoch_public_id": query.epoch_public_id,
        "calc_version": query.calc_version,
    }
    unrelated: tuple[dict[str, str], ...] = (
        {"wallet_public_id": samples._OTHER_WALLET},
        {"mode": "paper"},
        {"valuation_ccy": "EUR"},
        {"epoch_public_id": samples._OTHER_EPOCH},
        {"calc_version": "5C.3"},
    )
    async with repository.session() as session:
        for difference in unrelated:
            session.add(
                PortfolioPnlCorrectionFrontier(
                    **{**matching, **difference},
                    generation=1,
                    revision=1,
                    window_start=samples._M1,
                    next_minute=samples._M1,
                    tip=samples._M3,
                    as_of=samples._M5,
                    baseline_watermarks_json="{}",
                    pending_watermarks_json="{}",
                    raw_execution_watermarks_json="{}",
                )
            )
        await session.commit()
    assert await repository.get_portfolio_pnl_samples(query, samples._M1, samples._M3) == [row]


async def test_corrupt_execution_manifest_is_refused_on_read(
    repository: SQLAlchemyRepository,
) -> None:
    """Refuse a malformed execution manifest when reading a checkpoint.

    Given a stored checkpoint whose manifest has been corrupted.
    When the repository reads the checkpoint at its exact scope.
    Then it raises a digest validation error.
    """
    await _start(repository)
    async with repository.session() as session:
        await session.execute(
            text("UPDATE portfolio_pnl_correction_frontiers SET execution_manifest_digest = 'bad'")
        )
        await session.commit()
    with pytest.raises(ValueError, match="lowercase SHA-256"):
        await repository.get_portfolio_pnl_correction_frontier(samples._scope())


@pytest.mark.parametrize("payload", ["{}", "[1]", '["kraken", "kraken"]', "[]", '["Kraken"]'])
async def test_corrupt_spot_venue_set_is_refused_on_read(
    repository: SQLAlchemyRepository, payload: str
) -> None:
    """Refuse an invalid spot venue set instead of guessing the basket denominator.

    Given a stored checkpoint with corrupt venue evidence.
    When the repository loads its exact scope.
    Then it raises a venue validation error.
    """
    await _start(repository)
    async with repository.session() as session:
        await session.execute(
            text("UPDATE portfolio_pnl_correction_frontiers SET expected_venues_json = :venues"),
            {"venues": payload},
        )
        await session.commit()
    with pytest.raises(ValueError, match="spot venue"):
        await repository.get_portfolio_pnl_correction_frontier(samples._scope())


@pytest.mark.parametrize("row_kind", ["current", "legacy", "foreign"])
async def test_forged_unrepairable_withheld_is_refused_at_repository_fence(
    repository: SQLAlchemyRepository,
    row_kind: str,
) -> None:
    """Refuse a forged unrepairable list at commit_portfolio_pnl_correction_chunk.

    Given: An active current, legacy calc_version, or foreign-epoch sample at M2 in SQLite.
    When: A caller commits a chunk listing M2 in unrepairable_withheld_minutes without
        retracting that physical version.
    Then: The repository fence raises ValueError and leaves frontier and samples unchanged.
    """
    stored_row = samples._complete_sample(samples._M2)
    if row_kind == "legacy":
        stored_row["calc_version"] = "5C.3"
    elif row_kind == "foreign":
        stored_row["epoch_public_id"] = samples._OTHER_EPOCH
    async with repository.session() as session:
        session.add(
            PortfolioPnlPoint(
                **repository._portfolio_pnl_sample_orm_kwargs(stored_row), known_to=KNOWN_TO_MAX
            )
        )
        await session.commit()
    frontier = await _start(repository)
    plan = await _plan(repository, frontier, samples._M3)
    forged = replace(
        plan,
        prior_peak=None,
        withheld_minutes=(samples._M2,),
        unrepairable_withheld_minutes=(samples._M2,),
    )
    with pytest.raises(ValueError, match="cannot retain an active sample row"):
        await repository.commit_portfolio_pnl_correction_chunk(samples._scope(), frontier, forged)
    assert await repository.get_portfolio_pnl_correction_frontier(samples._scope()) == frontier


def _repo_minute(minute: datetime, *, sum_unrepresentable: bool = False) -> MinuteInputs:
    """Build planner MinuteInputs producing either a complete or sum-unrepresentable point."""
    if sum_unrepresentable:
        point = replace(
            planner._untrusted_point(minute),
            incompleteness_reasons=(
                PnlIncompletenessReasonEntry(
                    reason="instrument_sum_unrepresentable",
                    withholding_tier="untrusted",
                    withholding_scope="global",
                    trigger_instrument_public_id=None,
                ),
            ),
        )
    else:
        point = planner._complete_point(minute)
    return MinuteInputs(
        point,
        {"kraken": planner._attempt(minute=minute)},
        planner._crypto_evidence(),
    )


def _assert_settled_rows_match_plan(
    settled_rows: Sequence[PortfolioPnlSampleRow],
    initial_rows: Sequence[PortfolioPnlSampleRow],
    committed_chunks: Sequence[PnlCorrectionChunk],
) -> None:
    """Assert the settled M1..M3 rows are exactly the committed plans applied in order.

    Args:
        settled_rows: Rows read back with ``require_reconciled=True`` after settlement.
        initial_rows: The originally committed M1..M3 rows.
        committed_chunks: Every later committed chunk, in commit order.
    """
    expected: dict[datetime, PortfolioPnlSampleRow | None] = {
        row["point_time"]: row for row in initial_rows
    }
    for chunk in committed_chunks:
        for edit in chunk.edits:
            expected[edit.point_time] = edit.replacement
    settled_m2 = expected[samples._M2]
    assert settled_m2 is not None
    assert settled_m2["cash_usd"] != initial_rows[1]["cash_usd"]
    provenance = {"public_id", "session_id", "sequence_id", "timestamp"}
    expected_rows = [expected[samples._M1], settled_m2, expected[samples._M3]]
    assert all(row is not None for row in expected_rows)
    assert [
        {key: value for key, value in row.items() if key not in provenance} for row in settled_rows
    ] == [
        {key: value for key, value in row.items() if key not in provenance}
        for row in expected_rows
        if row is not None
    ]


async def test_r1_retained_sum_unrepresentable_keeps_repair_from_and_gates_reconciled_reads(
    repository: SQLAlchemyRepository,
) -> None:
    """Enforce R1 end-to-end across SQLite persistence and require_reconciled reads.

    Given: Settled COMPLETE same-epoch 5C.5 rows at M1..M3 with M2 carrying equity 2500.0.
    When: Changed evidence triggers pure instrument_sum_unrepresentable at M2, retaining M2
        and latching repair_from=M2 across repeated withholding, followed by later settlement.
    Then: M2 stays repairable, repair_from remains pinned at M2, require_reconciled=True
        hides [M2..M3] before settlement, and after settlement exposes exactly the rows
        of the committed plans applied in order, values and drawdown included and
        write provenance aside, with the stale 2500.0 equity at M2 replaced.
    """
    scope = samples._scope()
    query = samples._query()
    published = replace(query, require_reconciled=True)
    initial_rows = (
        samples._mutate(
            samples._complete_sample(samples._M1),
            cash_usd=1000.0,
            position_value_usd=0.0,
            drawdown=0.0,
        ),
        samples._mutate(
            samples._complete_sample(samples._M2),
            cash_usd=2500.0,
            position_value_usd=0.0,
            drawdown=0.0,
        ),
        samples._mutate(
            samples._complete_sample(samples._M3),
            cash_usd=1000.0,
            position_value_usd=0.0,
            drawdown=0.6,
        ),
    )
    req_g1 = PnlCorrectionRequest(samples._M1, samples._M3, samples._M5, {}, {"kraken": 5}, None)
    g1 = await repository.invalidate_portfolio_pnl_correction(scope, req_g1, None)
    g1_plan = await _plan(repository, g1, samples._M3, initial_rows)
    settled_g1 = await repository.commit_portfolio_pnl_correction_chunk(
        scope, g1, replace(g1_plan, prior_peak=2500.0)
    )
    assert settled_g1.repair_from is None
    assert await repository.get_portfolio_pnl_samples(published, samples._M1, samples._M3) == list(
        initial_rows
    )

    venues = frozenset({"kraken"})
    req_g2 = replace(req_g1, pending_watermarks={"kraken": 6})
    g2 = await repository.invalidate_portfolio_pnl_correction(scope, req_g2, settled_g1)
    ev_g2 = await repository.get_portfolio_pnl_correction_evidence(scope, samples._M1, samples._M3)
    planned_g2 = plan_correction_chunk(
        PnlCorrectionPlanInputs(
            evidence=ev_g2,
            minute_inputs=(
                _repo_minute(samples._M1),
                _repo_minute(samples._M2, sum_unrepresentable=True),
                _repo_minute(samples._M3),
            ),
            expected_venues=venues,
            position_versions=(),
            make_row=planner._to_row,
        )
    )
    assert planned_g2.chunk.withheld_minutes == (samples._M2,)
    assert planned_g2.chunk.unrepairable_withheld_minutes == ()
    blocked = await repository.commit_portfolio_pnl_correction_chunk(scope, g2, planned_g2.chunk)
    assert blocked.next_minute is None
    assert blocked.repair_from == samples._M2
    assert blocked.baseline_watermarks == {"kraken": 5}
    reconciled_g2 = await repository.get_portfolio_pnl_samples(published, samples._M1, samples._M3)
    assert [row["point_time"] for row in reconciled_g2] == [samples._M1]
    all_g2 = await repository.get_portfolio_pnl_samples(query, samples._M1, samples._M3)
    assert len(all_g2) == 3
    assert all_g2[1] == initial_rows[1]

    g3 = await repository.invalidate_portfolio_pnl_correction(
        scope, replace(req_g2, start=samples._M2, prior_peak=1000.0), blocked
    )
    ev_g3 = await repository.get_portfolio_pnl_correction_evidence(scope, samples._M2, samples._M3)
    planned_g3 = plan_correction_chunk(
        PnlCorrectionPlanInputs(
            evidence=ev_g3,
            minute_inputs=(
                _repo_minute(samples._M2, sum_unrepresentable=True),
                _repo_minute(samples._M3),
            ),
            expected_venues=venues,
            position_versions=(),
            make_row=planner._to_row,
        )
    )
    still_blocked = await repository.commit_portfolio_pnl_correction_chunk(
        scope, g3, planned_g3.chunk
    )
    assert still_blocked.repair_from == samples._M2
    reconciled_g3 = await repository.get_portfolio_pnl_samples(published, samples._M1, samples._M3)
    assert [row["point_time"] for row in reconciled_g3] == [samples._M1]

    req_g4 = replace(req_g2, start=samples._M2, pending_watermarks={"kraken": 7})
    g4 = await repository.invalidate_portfolio_pnl_correction(
        scope, replace(req_g4, prior_peak=1000.0), still_blocked
    )
    ev_g4 = await repository.get_portfolio_pnl_correction_evidence(scope, samples._M2, samples._M3)
    planned_g4 = plan_correction_chunk(
        PnlCorrectionPlanInputs(
            evidence=ev_g4,
            minute_inputs=(_repo_minute(samples._M2), _repo_minute(samples._M3)),
            expected_venues=venues,
            position_versions=(),
            make_row=planner._to_row,
        )
    )
    settled_g4 = await repository.commit_portfolio_pnl_correction_chunk(scope, g4, planned_g4.chunk)
    assert settled_g4.next_minute is None
    assert settled_g4.repair_from is None
    assert settled_g4.baseline_watermarks == {"kraken": 7}
    settled_rows = await repository.get_portfolio_pnl_samples(published, samples._M1, samples._M3)
    assert len(settled_rows) == 3
    assert [row["point_time"] for row in settled_rows] == [samples._M1, samples._M2, samples._M3]
    _assert_settled_rows_match_plan(
        settled_rows, initial_rows, (planned_g2.chunk, planned_g3.chunk, planned_g4.chunk)
    )
