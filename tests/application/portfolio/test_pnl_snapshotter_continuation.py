"""Run snapshotter replay and restart through the migrated correction repository."""

import json
from collections.abc import AsyncIterator
from collections.abc import Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from dataclasses import field
from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from importlib import import_module
from pathlib import Path
from typing import Literal

import pytest
from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext
from sqlalchemy import create_engine
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.portfolio import pnl_snapshotter
from snapper.application.portfolio.pnl_snapshotter import PortfolioPnlSnapshotter
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReplayOptions
from snapper.application.portfolio.pnl_timeline_service import PnlWalletSeriesResult
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Instrument
from snapper.data.models import InstrumentSpec
from snapper.data.models import PortfolioPnlPoint
from snapper.data.models import Position
from snapper.data.models import Symbol
from snapper.data.models import VenueAccountObservation
from snapper.data.models import WalletCredential
from snapper.data.pnl_correction_state import PnlCorrectionChunk
from snapper.data.pnl_correction_state import PnlCorrectionFrontier
from snapper.data.repository import PortfolioPnlSampleScope
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import PortfolioPnlSampleRow
from tests.application.portfolio import test_pnl_snapshotter as series_fixtures
from tests.data import test_pnl_timeline_sample_repository as sample_fixtures

_MINUTE = timedelta(minutes=1)
_MINUTES = tuple(sample_fixtures._T0 + offset * _MINUTE for offset in range(1, 6))
_FIRST, _SECOND, _THIRD, _FOURTH, _TIP = _MINUTES
_AS_OF = _TIP + 2 * _MINUTE
_BALANCES = (1_000, 1_200, 900, 1_500, 750)
_ROW_PROVENANCE = frozenset({"public_id", "session_id", "sequence_id", "timestamp"})
_MANIFEST_DIGEST = "a" * 64

type _ReplayRead = tuple[
    datetime, datetime, datetime, dict[str, int], dict[str, int] | None, str | None
]


@dataclass
class _ReplayTape:
    """Synthetic valuation and execution evidence served at the requested horizon."""

    sequence: int = 1
    affected: datetime | None = None
    withheld: datetime | None = None
    reads: list[_ReplayRead] = field(default_factory=list)

    async def series(self, *args: object, **kwargs: object) -> PnlWalletSeriesResult:
        """Return deterministic financial input, with a late fill or untrusted point on demand."""
        start, end, as_of = args[3], args[4], args[6]
        assert isinstance(start, datetime) and isinstance(end, datetime)
        assert isinstance(as_of, datetime)
        options = kwargs["options"]
        assert isinstance(options, PnlSeriesReplayOptions) and options.offload_valuation
        baseline = dict(options.baseline_watermarks or {})
        frozen = options.frozen_execution_watermarks
        cut = dict(frozen) if frozen is not None else None
        raw = {"kraken": self.sequence + 100}
        assert cut is None or cut == raw
        manifest_digest = options.frozen_execution_manifest_digest
        assert manifest_digest == (_MANIFEST_DIGEST if cut is not None else None)
        assert options.frozen_has_fill_gap is (False if cut is not None else None)
        assert options.pointwise_exact_fx
        if cut is None:
            assert options.frozen_fill_witness_cut is None
        else:
            assert options.replay_generation_tip == _TIP
            assert options.frozen_fill_witness_cut == {
                "watermarks": {"kraken:spot": 10},
                "digest": "c" * 64,
            }
        self.reads.append((start, end, as_of, baseline, cut, manifest_digest))
        affected = self.affected if baseline.get("kraken", 0) < self.sequence else None
        points = tuple(
            (
                series_fixtures._untrusted_point(minute)
                if minute == self.withheld
                else series_fixtures._complete_point(minute)
            )
            for minute in _MINUTES
            if start <= minute <= end
        )
        return PnlWalletSeriesResult(
            points=points,
            granularity="1m",
            valuation_ccy="USD",
            rate_sources=(),
            replay_metadata=replace(
                series_fixtures._meta(seq=self.sequence, affected=affected),
                detection_watermarks={"kraken": self.sequence},
                raw_execution_watermarks=raw,
                execution_manifest_digest=_MANIFEST_DIGEST,
                has_fill_gap=False,
            ),
        )


@pytest.fixture
def tape(monkeypatch: pytest.MonkeyPatch) -> _ReplayTape:
    """Force three short production chunks and replace only the valuation data source."""
    tape = _ReplayTape()
    monkeypatch.setattr(pnl_snapshotter, "MAX_CORRECTION_CHUNK_MINUTES", 2)
    monkeypatch.setattr(pnl_snapshotter, "build_wallet_pnl_series", tape.series)
    monkeypatch.delenv("PNL_FX_SHADOW_PINNING_ENABLED", raising=False)
    return tape


@asynccontextmanager
async def _repository(path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Give each snapshotter instance its own pool over the same on-disk database."""
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{path.as_posix()}")
    try:
        yield repo
    finally:
        await repo.engine.dispose()


def _create_schema(path: Path) -> None:
    """Create the production tables used by replay and apply the correction migrations."""
    engine = create_engine(f"sqlite:///{path.as_posix()}")
    frontier = import_module("snapper.data.migrations.versions.0069_pnl_correction_frontiers")
    raw_cut = import_module("snapper.data.migrations.versions.0070_pnl_correction_raw_watermarks")
    try:
        with engine.begin() as connection:
            for table in (
                WalletCredential.__table__,
                PortfolioPnlPoint.__table__,
                VenueAccountObservation.__table__,
                Symbol.__table__,
                Instrument.__table__,
                InstrumentSpec.__table__,
                Position.__table__,
            ):
                table.create(connection)
            with Operations.context(MigrationContext.configure(connection)):
                frontier.upgrade()
                raw_cut.upgrade()
    finally:
        engine.dispose()


async def _seed(path: Path, *, legacy_samples: bool = False) -> None:
    """Write deterministic spot evidence and, optionally, a previous-version history."""
    _create_schema(path)
    async with _repository(path) as repo, repo.session() as session:
        anchor = sample_fixtures._anchor_orm()
        anchor.watermarks_json = "{}"
        anchor.contributions_json = "{}"
        session.add(anchor)
        session.add(
            WalletCredential(
                public_id="00000000-0000-7000-8000-000000000071",
                wallet_public_id=sample_fixtures._WALLET,
                exchange="kraken",
                credential_type="api_key_secret",
                encrypted_payload="synthetic-no-exchange-connection",
                timestamp=sample_fixtures._T0,
                session_id=sample_fixtures._SESSION,
                sequence_id=1,
                known_to=KNOWN_TO_MAX,
            )
        )
        for offset, (minute, balance) in enumerate(zip(_MINUTES, _BALANCES, strict=True), 1):
            observation = sample_fixtures._observation(
                timestamp=minute,
                balances_json=json.dumps([{"currency": "USD", "total": balance}]),
            )
            observation.public_id = f"00000000-0000-7000-8000-{offset:012x}"
            observation.sequence_id = offset
            session.add(observation)
            if legacy_samples:
                legacy = sample_fixtures._complete_sample(minute)
                legacy["calc_version"] = "5C.3"
                session.add(
                    PortfolioPnlPoint(
                        **repo._portfolio_pnl_sample_orm_kwargs(legacy), known_to=KNOWN_TO_MAX
                    )
                )
        await session.commit()


def _snapshotter(repo: SQLAlchemyRepository, as_of: datetime = _AS_OF) -> PortfolioPnlSnapshotter:
    """Run the production tick on a fixed evidence clock without starting background polling."""
    return PortfolioPnlSnapshotter(repo=repo, disabled=False, clock=lambda: as_of)


async def _frontier(repo: SQLAlchemyRepository) -> PnlCorrectionFrontier:
    """Read the persisted checkpoint after a writer tick."""
    frontier = await repo.get_portfolio_pnl_correction_frontier(sample_fixtures._scope())
    assert frontier is not None
    return frontier


async def _samples(repo: SQLAlchemyRepository) -> list[PortfolioPnlSampleRow]:
    """Return the active recalculated series in minute order."""
    return await repo.get_portfolio_pnl_samples(sample_fixtures._query(), _FIRST, _TIP)


def _economics(rows: Sequence[PortfolioPnlSampleRow]) -> tuple[dict[str, object], ...]:
    """Compare financial values, marks and audit while allowing distinct session identities."""
    return tuple(
        {key: value for key, value in row.items() if key not in _ROW_PROVENANCE} for row in rows
    )


async def _interrupt_tick(
    repo: SQLAlchemyRepository,
    snapshotter: PortfolioPnlSnapshotter,
    monkeypatch: pytest.MonkeyPatch,
    stage: Literal["before_commit", "after_commit"],
) -> None:
    """Lose a chunk before commit or lose its acknowledged result after a durable commit."""
    failures: list[str] = []
    commit = repo.commit_portfolio_pnl_correction_chunk

    async def before_commit(
        session: AsyncSession,
        current: PnlCorrectionFrontier | None,
        advanced: PnlCorrectionFrontier,
    ) -> None:
        """Raise after the actual sample edits are flushed and before the commit."""
        assert current is not None and current.next_minute == _THIRD
        assert advanced.next_minute == _TIP
        samples = await session.scalar(
            select(func.count())
            .select_from(PortfolioPnlPoint)
            .where(
                PortfolioPnlPoint.point_kind == "sample",
                PortfolioPnlPoint.known_to == KNOWN_TO_MAX,
                PortfolioPnlPoint.calc_version == current.key.calc_version,
            )
        )
        assert samples == 4
        failures.append("before_commit")
        raise RuntimeError("injected failure before P&L chunk commit")

    async def after_commit(
        scope: PortfolioPnlSampleScope,
        expected: PnlCorrectionFrontier,
        chunk: PnlCorrectionChunk,
    ) -> PnlCorrectionFrontier:
        """Raise after both the financial writes and checkpoint have committed."""
        advanced = await commit(scope, expected, chunk)
        assert advanced.next_minute == _TIP
        failures.append("after_commit")
        raise RuntimeError("injected loss of durable P&L chunk acknowledgement")

    with monkeypatch.context() as patch:
        if stage == "before_commit":
            patch.setattr(repo, "_write_pnl_correction", before_commit)
        else:
            patch.setattr(repo, "commit_portfolio_pnl_correction_chunk", after_commit)
        await snapshotter._tick_once()
    assert failures == [stage]


async def _uninterrupted(path: Path) -> tuple[PnlCorrectionFrontier, list[PortfolioPnlSampleRow]]:
    """Recompute the entire older financial snapshot without an interruption."""
    await _seed(path, legacy_samples=True)
    async with _repository(path) as control:
        snapshotter = _snapshotter(control)
        for _ in range(3):
            await snapshotter._tick_once()
        return await _frontier(control), await _samples(control)


@pytest.mark.parametrize("stage", ["before_commit", "after_commit"])
async def test_restart_matches_uninterrupted_history_after_chunk_interruption(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    tape: _ReplayTape,
    stage: Literal["before_commit", "after_commit"],
) -> None:
    """Resume financial replay from its last committed checkpoint.

    Given a history recomputation interrupted just before or after a chunk commits.
    When a fresh writer opens the same repository and resumes the unfinished chunks.
    Then its evidence cut, financial rows, peak and drawdown match uninterrupted replay.
    """
    expected, expected_samples = await _uninterrupted(tmp_path / "uninterrupted.db")
    path = tmp_path / "interrupted.db"
    await _seed(path, legacy_samples=True)
    before_commit = stage == "before_commit"
    async with _repository(path) as original:
        snapshotter = _snapshotter(original)
        await snapshotter._tick_once()
        first = await _frontier(original)
        assert first.next_minute == _THIRD
        assert first.baseline_watermarks == {}
        await _interrupt_tick(original, snapshotter, monkeypatch, stage)
        saved = await _frontier(original)
        assert saved.next_minute == (_THIRD if before_commit else _TIP)
        assert saved.baseline_watermarks == {}
        assert saved.prior_peak == (1_200 if before_commit else 1_500)
        assert len(await _samples(original)) == (2 if before_commit else 4)
        if before_commit:
            assert saved == first

    tape.reads.clear()
    async with _repository(path) as reopened:
        assert await _frontier(reopened) == saved
        resumed = _snapshotter(reopened, _AS_OF + 10 * _MINUTE)
        for _ in range(2 if before_commit else 1):
            await resumed._tick_once()
        rows = await _samples(reopened)
        assert await _frontier(reopened) == expected
        assert _economics(rows) == _economics(expected_samples)
        async with reopened.session() as session:
            versions = await session.scalar(
                select(func.count())
                .select_from(PortfolioPnlPoint)
                .where(PortfolioPnlPoint.point_kind == "sample")
            )
        assert versions == 2 * len(_MINUTES)

    assert [row["point_time"] for row in rows] == list(_MINUTES)
    assert [row["drawdown"] for row in rows] == pytest.approx([0, 0, 0.25, 0, 0.5])
    assert expected.prior_peak == 1_500
    assert expected.baseline_watermarks == {"kraken": 1}
    assert expected.raw_execution_watermarks == {"kraken": 101}
    assert expected.execution_manifest_digest == _MANIFEST_DIGEST
    assert expected.venue_event_watermarks == {"kraken:spot": 10}
    assert expected.fill_witness_digest == "c" * 64
    assert expected.has_fill_gap is False
    assert expected.expected_venues == frozenset({"kraken"})
    assert expected.next_minute is None
    expected_reads: list[_ReplayRead] = [
        (_TIP, _TIP, _AS_OF, {}, {"kraken": 101}, _MANIFEST_DIGEST)
    ]
    if before_commit:
        expected_reads.insert(0, (_THIRD, _FOURTH, _AS_OF, {}, {"kraken": 101}, _MANIFEST_DIGEST))
    assert tape.reads == expected_reads


async def test_restart_rewinds_after_an_earlier_complete_sample_changes_the_peak(
    tmp_path: Path, tape: _ReplayTape
) -> None:
    """Rewind before publishing a chunk whose persisted opening peak has changed.

    Given a writer that committed two minutes and a second writer that raises the first balance.
    When a fresh snapshotter attempts the next chunk at the original evidence horizon.
    Then it resets the cursor to the first minute and leaves the active financial rows intact.
    """
    path = tmp_path / "revised-peak.db"
    await _seed(path)
    async with _repository(path) as original:
        await _snapshotter(original)._tick_once()
        saved = await _frontier(original)
        assert saved.next_minute == _THIRD and saved.prior_peak == 1_200

    async with _repository(path) as concurrent:
        first, second = await _samples(concurrent)
        assert (first["point_time"], second["point_time"]) == (_FIRST, _SECOND)
        replacement = first.copy()
        replacement["cash_usd"] = 2_000.0
        replacement["timestamp"] = first["timestamp"] + _MINUTE
        amended = await concurrent.supersede_portfolio_pnl_sample(
            sample_fixtures._scope(),
            replacement,
            derived_suffix_reconciliation=True,
            expected_public_id=first["public_id"],
        )

    tape.reads.clear()
    async with _repository(path) as reopened:
        await _snapshotter(reopened, _AS_OF + 10 * _MINUTE)._tick_once()
        assert await _frontier(reopened) == replace(
            saved,
            generation=saved.generation + 1,
            revision=saved.revision + 1,
            window_start=_FIRST,
            next_minute=_FIRST,
            prior_peak=None,
        )
        assert await _samples(reopened) == [amended, second]
    assert tape.reads == [(_THIRD, _FOURTH, _AS_OF, {}, {"kraken": 101}, _MANIFEST_DIGEST)]


async def test_restart_repairs_an_interior_withheld_minute_without_consuming_its_baseline(
    tmp_path: Path, tape: _ReplayTape
) -> None:
    """Repair a withheld interior minute after restarting the snapshotter.

    Given a replay that withholds an interior minute and commits later samples.
    When the evidence is restored and a fresh writer resumes the outstanding repair.
    Then it restores the complete series and causal drawdown before publishing the new baseline.
    """
    path = tmp_path / "withheld.db"
    await _seed(path)
    async with _repository(path) as original:
        writer = _snapshotter(original)
        for _ in range(3):
            await writer._tick_once()
        assert (await _frontier(original)).baseline_watermarks == {"kraken": 1}

        tape.sequence, tape.affected, tape.withheld = 9, _SECOND, _SECOND
        correction = _snapshotter(original, _AS_OF + timedelta(seconds=15))
        await correction._tick_once()
        partial = await _frontier(original)
        assert partial.next_minute == _FOURTH
        assert partial.repair_from == _SECOND
        await correction._tick_once()
        blocked = await _frontier(original)
        assert blocked.next_minute is None and blocked.repair_from == _SECOND
        assert blocked.baseline_watermarks == {"kraken": 1}
        assert blocked.pending_watermarks == {"kraken": 9}
        assert blocked.raw_execution_watermarks == {"kraken": 109}
        assert blocked.expected_venues == frozenset({"kraken"})
        rows = await _samples(original)
        assert [row["point_time"] for row in rows] == [_FIRST, _THIRD, _FOURTH, _TIP]
        assert rows[1]["drawdown"] == pytest.approx(0.1)
        assert json.loads(rows[-1]["watermarks_json"]) == {"kraken": 9}

    tape.affected = tape.withheld = None
    tape.reads.clear()
    async with _repository(path) as reopened:
        assert await _frontier(reopened) == blocked
        recovery = _snapshotter(reopened, _AS_OF + timedelta(seconds=30))
        await recovery._tick_once()
        repairing = await _frontier(reopened)
        assert repairing.next_minute == _FOURTH and repairing.repair_from == _FOURTH
        assert repairing.baseline_watermarks == {"kraken": 1}
        await recovery._tick_once()
        done = await _frontier(reopened)
        assert done.next_minute is None and done.repair_from is None
        assert done.baseline_watermarks == {"kraken": 9}
        rows = await _samples(reopened)

    assert [row["point_time"] for row in rows] == list(_MINUTES)
    assert [row["drawdown"] for row in rows] == pytest.approx([0, 0, 0.25, 0, 0.5])
    assert done.prior_peak == 1_500
    assert [(start, end, cut, digest) for start, end, _, _, cut, digest in tape.reads] == [
        (_TIP, _TIP, None, None),
        (_SECOND, _THIRD, {"kraken": 109}, _MANIFEST_DIGEST),
        (_FOURTH, _TIP, {"kraken": 109}, _MANIFEST_DIGEST),
    ]
