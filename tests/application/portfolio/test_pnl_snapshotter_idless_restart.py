"""Exercise delayed ID-less partial-fill admission through the real SQLite snapshotter."""

from datetime import datetime
from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import select

from snapper.application.portfolio import pnl_snapshotter
from snapper.application.portfolio.execution_chain import ExecutionChainError
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import PortfolioPnlPoint
from snapper.data.models import WalletCredential
from snapper.data.pnl_correction_state import PnlCorrectionFrontier
from snapper.data.repository import PortfolioPnlSampleScope
from snapper.data.repository import SQLAlchemyRepository
from tests.application.portfolio import test_pnl_snapshotter_continuation as restart
from tests.application.portfolio import test_pnl_timeline_service as timeline
from tests.data import test_pnl_timeline_execution_prefix_repository as prefix
from tests.data import test_pnl_timeline_sample_repository as samples


async def _append_fill(repo: SQLAlchemyRepository, number: int, *, matched: bool) -> None:
    """Append a distinct backdated venue fill and, when paired, its immutable execution."""
    time = samples._T0 + number * timedelta(minutes=1)
    fill = prefix._fill_event(prefix._CLIENT_ORDER, prefix._KRAKEN_SHARD, sequence_id=number)
    fill.exec_id = fill.trade_id = None
    fill.timestamp = fill.received_at = fill.venue_timestamp = time
    fill.cum_fill_size = float(number)
    fill.status = "partially_filled"
    async with repo.session() as session:
        session.add(fill)
        if matched:
            execution = prefix._execution("kraken", number, time)
            execution.status = "partial"
            execution.exec_id = execution.trade_id = None
            session.add(execution)
        await session.commit()


@pytest.fixture
async def idless_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[Path, PortfolioPnlSampleScope]:
    """Seed a canonical flat opening, oracle observations, one order and its first matched fill."""
    path = tmp_path / "idless-history.db"
    monkeypatch.setattr(pnl_snapshotter, "MAX_CORRECTION_CHUNK_MINUTES", 2)
    monkeypatch.delenv("PNL_FX_SHADOW_PINNING_ENABLED", raising=False)
    async with restart._repository(path) as repo:
        await repo.create_all()
        anchor = timeline._empty_anchor(prefix._WALLET, "live", "USD", samples._T0)
        async with repo.session() as session:
            session.add(PortfolioPnlPoint(**anchor, known_to=KNOWN_TO_MAX))
            session.add(
                WalletCredential(
                    public_id="00000000-0000-7000-8000-000000000071",
                    wallet_public_id=prefix._WALLET,
                    exchange="kraken",
                    credential_type="api_key_secret",
                    encrypted_payload="isolated-replay-fixture",
                    timestamp=samples._T0,
                    session_id=samples._SESSION,
                    sequence_id=1,
                    known_to=KNOWN_TO_MAX,
                )
            )
            session.add_all([prefix._symbol(), prefix._instrument(), prefix._order()])
            for minute in restart._MINUTES:
                session.add(
                    samples._observation(
                        timestamp=minute, balances_json='[{"currency":"USD","total":1000}]'
                    )
                )
            await session.commit()
        await _append_fill(repo, 1, matched=True)
    return path, samples._scope(epoch=anchor["epoch_public_id"])


async def _frontier(
    repo: SQLAlchemyRepository, scope: PortfolioPnlSampleScope
) -> PnlCorrectionFrontier:
    """Read the checkpoint from storage after the writer has returned."""
    state = await repo.get_portfolio_pnl_correction_frontier(scope)
    assert state is not None
    return state


async def _versions(repo: SQLAlchemyRepository) -> tuple[tuple[int, datetime], ...]:
    """Identify every historical and active sample and whether its validity end changed."""
    async with repo.session() as session:
        rows = await session.execute(
            select(PortfolioPnlPoint.id, PortfolioPnlPoint.known_to)
            .where(PortfolioPnlPoint.point_kind == "sample")
            .order_by(PortfolioPnlPoint.id)
        )
        return tuple((row_id, known_to) for row_id, known_to in rows)


async def test_matched_idless_append_resumes_frozen_replay(
    idless_history: tuple[Path, PortfolioPnlSampleScope],
) -> None:
    """Resume durable replay after a late, fully matched partial fill.

    Given a pending checkpoint whose frozen cut contains only the first partial fill.
    When a later matched ID-less partial fill commits and a fresh writer opens.
    Then the pending writer progresses and ultimately settles the same target.
    """
    path, scope = idless_history
    async with restart._repository(path) as first:
        await restart._snapshotter(first)._tick_once()
        old = await _frontier(first, scope)
        assert old.raw_execution_watermarks == {"kraken": 1}
        assert old.next_minute == restart._THIRD and old.tip == restart._TIP
        versions = await _versions(first)

    async with restart._repository(path) as appender:
        await _append_fill(appender, 2, matched=True)
        fresh = await appender.get_pnl_timeline_execution_prefix(prefix._WALLET, "live", old.as_of)
        assert fresh["watermarks"] == {"kraken": 2}
        assert [row["scope_sequence"] for row in fresh["executions"]] == [1, 2]

    async with restart._repository(path) as reopened:
        await restart._snapshotter(reopened)._tick_once()
        resumed = await _frontier(reopened, scope)
        assert resumed.revision == old.revision + 1
        pending_starts: list[datetime | None]
        if resumed.generation == old.generation:
            assert resumed.raw_execution_watermarks == old.raw_execution_watermarks
            assert resumed.next_minute == restart._TIP
            pending_starts = [None]
        else:
            assert resumed.generation == old.generation + 1
            assert resumed.raw_execution_watermarks == fresh["watermarks"]
            assert resumed.window_start == resumed.next_minute == restart._FIRST
            assert await _versions(reopened) == versions
            pending_starts = [restart._THIRD, restart._TIP, None]
        assert resumed.tip == old.tip and resumed.as_of == old.as_of
        assert resumed.baseline_watermarks == old.baseline_watermarks

    for next_start in pending_starts:
        async with restart._repository(path) as later:
            await restart._snapshotter(later)._tick_once()
            advanced = await _frontier(later, scope)
            assert advanced.generation == resumed.generation
            assert advanced.revision == resumed.revision + 1
            assert advanced.raw_execution_watermarks == resumed.raw_execution_watermarks
            assert advanced.next_minute == next_start
            resumed = advanced


async def test_unpaired_idless_append_preserves_settled_frontier(
    idless_history: tuple[Path, PortfolioPnlSampleScope],
) -> None:
    """Preserve previously committed rows when a fresh fill cannot be certified.

    Given a settled checkpoint and an extra venue fill without a ledger match.
    When a fresh writer attempts admission with that fill in its horizon.
    Then the settled frontier and physical sample versions remain unchanged.
    """
    path, scope = idless_history
    async with restart._repository(path) as first:
        writer = restart._snapshotter(first)
        for _ in range(3):
            await writer._tick_once()
        settled = await _frontier(first, scope)
        assert settled.next_minute is None and settled.tip == restart._TIP
        assert settled.raw_execution_watermarks == {"kraken": 1}
        versions = await _versions(first)

    later = restart._AS_OF + timedelta(minutes=1)
    async with restart._repository(path) as appender:
        await _append_fill(appender, 2, matched=False)
        with pytest.raises(ExecutionChainError, match="unconsumed_execution_fill_lineage"):
            await appender.get_pnl_timeline_execution_prefix(prefix._WALLET, "live", later)

    async with restart._repository(path) as reopened:
        await restart._snapshotter(reopened, later)._tick_once()
    async with restart._repository(path) as observer:
        assert await _frontier(observer, scope) == settled
        assert await _versions(observer) == versions
