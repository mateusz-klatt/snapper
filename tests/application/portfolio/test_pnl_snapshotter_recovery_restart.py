"""Exercise real valuation across different snapshotter chunk widths and a restart."""

import math
from datetime import datetime
from pathlib import Path

import pytest

from snapper.application.portfolio import pnl_snapshotter
from snapper.application.portfolio import pnl_timeline
from snapper.application.portfolio.pnl_faithful_recovery import MAX_RECOVERY_KEY_VISITS
from snapper.application.portfolio.pnl_snapshot_planner import ChunkWindow
from snapper.application.portfolio.pnl_timeline import PnlTimelineWorkBudgetError
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReplayOptions
from snapper.application.portfolio.pnl_timeline_service import PnlWalletSeriesResult
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Execution
from snapper.data.models import Order
from snapper.data.models import PortfolioPnlPoint
from snapper.data.models import TradeCommand
from snapper.data.models import VenueEvent
from snapper.data.models import WalletCredential
from snapper.data.repository import PortfolioPnlSampleScope
from snapper.data.repository import Repository
from snapper.data.repository_types import PortfolioPnlSampleRow
from tests.application.portfolio import test_pnl_snapshotter_continuation as restart
from tests.application.portfolio import test_pnl_timeline_service as timeline
from tests.data import test_pnl_timeline_execution_prefix_repository as prefix
from tests.data import test_pnl_timeline_lineage_repository as lineage
from tests.data import test_pnl_timeline_sample_repository as samples

_FIRST = restart._FIRST
_SECOND = restart._SECOND
_AS_OF = _SECOND + pnl_snapshotter.FINALIZATION_LAG
_LEADING_FEE_DELTAS = ((32.0, 0.875), (120.0, 1.875), (-132.0, 2.875))
_FEE_DELTAS = (
    tuple((base - 0.0625, 0.0625 + scale * math.ulp(20.0)) for base, scale in _LEADING_FEE_DELTAS)
    + ((0.9375, math.nextafter(0.0625, math.inf)),) * 9
    + ((0.0, 0.0),) * 20
)


def _public_id(group: int, offset: int) -> str:
    """Keep database UUIDs distinct and instrument order reproducible."""
    return f"00000000-0000-7000-8000-{group:04x}{offset:08x}"


def _execution_rows(
    number: int, sequence: int, minute: datetime, fee: float, side: str
) -> tuple[Order, Execution, VenueEvent, TradeCommand]:
    """Create one matched fill and its durable order and execution records."""
    native_symbol = f"COIN{number:02d}-USD"
    order_id = _public_id(3, sequence)
    client_id = f"recovery-{sequence}"
    order = prefix._order(
        order_id, instrument_public_id=_public_id(1, number), client_order_id=client_id
    )
    order.side = side
    order.plan_public_id = _public_id(5, number)
    command = lineage._command(
        _public_id(6, sequence),
        client_id,
        sequence,
        "strategy",
        plan_public_id=order.plan_public_id,
        signal_public_id=_public_id(7, number),
    )
    command.wallet_public_id = order.wallet_public_id
    command.shard_key = f"kraken.{native_symbol}.live"
    command.instrument = native_symbol
    command.side = side
    command.venue_client_id = client_id
    execution = prefix._execution("kraken", sequence, minute, order_id)
    execution.side = side
    execution.fee = fee
    event = prefix._fill_event(
        client_id, f"kraken.{native_symbol}.live", sequence_id=sequence, instrument=native_symbol
    )
    event.side = side
    event.fee = fee
    event.command_public_id = command.public_id
    event.timestamp = event.received_at = event.venue_timestamp = minute
    return order, execution, event, command


async def _seed(path: Path) -> PortfolioPnlSampleScope:
    """Seed a flat anchor, wallet equity observations, and matched spot fills."""
    async with restart._repository(path) as repo:
        await repo.create_all()
        anchor = timeline._empty_anchor(samples._WALLET, "live", "USD", samples._T0)
        async with repo.session() as session:
            session.add(PortfolioPnlPoint(**anchor, known_to=KNOWN_TO_MAX))
            session.add(
                WalletCredential(
                    public_id=_public_id(4, 1),
                    wallet_public_id=samples._WALLET,
                    exchange="kraken",
                    credential_type="api_key_secret",
                    encrypted_payload="isolated-replay-fixture",
                    timestamp=samples._T0,
                    session_id=samples._SESSION,
                    sequence_id=1,
                    known_to=KNOWN_TO_MAX,
                )
            )
            for number, minute in enumerate((_FIRST, _SECOND), start=1):
                observation = samples._observation(
                    timestamp=minute, balances_json='[{"currency":"USD","total":1000}]'
                )
                observation.public_id = _public_id(8, number)
                observation.sequence_id = number
                session.add(observation)
            for number in range(len(_FEE_DELTAS)):
                instrument_id = _public_id(1, number)
                symbol_id = _public_id(2, number)
                symbol = prefix._symbol(f"COIN{number:02d}-USD", public_id=symbol_id)
                symbol.base = f"COIN{number:02d}"
                signal = lineage._signal(
                    f"strategy-{number:02d}",
                    samples._T0,
                    KNOWN_TO_MAX,
                    public_id=_public_id(7, number),
                )
                signal.instrument_public_id = instrument_id
                signal.wallet_public_id = samples._WALLET
                session.add_all(
                    [
                        symbol,
                        prefix._instrument(public_id=instrument_id, symbol_public_id=symbol_id),
                        signal,
                    ]
                )
            sequence = 0
            for minute in (_FIRST, _SECOND):
                for number, legs in enumerate(_FEE_DELTAS):
                    for side, delta in zip(("buy", "sell"), legs, strict=True):
                        sequence += 1
                        session.add_all(_execution_rows(number, sequence, minute, -delta, side))
            await session.commit()
    return samples._scope(epoch=anchor["epoch_public_id"])


async def _run_chunk(
    path: Path, scope: PortfolioPnlSampleScope, *, next_minute: datetime | None
) -> list[PortfolioPnlSampleRow]:
    """Run one actual writer tick with a newly opened repository and snapshotter."""
    async with restart._repository(path) as repo:
        await restart._snapshotter(repo, _AS_OF)._tick_once()
        frontier = await repo.get_portfolio_pnl_correction_frontier(scope)
        assert frontier is not None
        assert frontier.generation == 1
        assert frontier.next_minute == next_minute
        return await repo.get_portfolio_pnl_samples(
            samples._query(scope.epoch_public_id), _FIRST, _SECOND
        )


async def test_faithful_recovery_survives_restart_with_a_different_chunk_width(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Persist identical financial samples for uninterrupted and resumed valuation.

    Given 32 instruments receive USD fees and rebates while remaining flat at
        the minute boundaries, with both minutes requiring a large faithful
        rounding search across instrument and strategy attribution totals.
    When an uninterrupted writer uses width two and a separate writer
        restarts after a width-one chunk and resumes at width two.
    Then the actual valuation engine publishes identical financial rows,
        audit and watermarks through the same two-minute target.
    """
    monkeypatch.delenv("PNL_FX_SHADOW_PINNING_ENABLED", raising=False)
    wide_path = tmp_path / "wide.db"
    split_path = tmp_path / "split.db"
    wide_scope = await _seed(wide_path)
    split_scope = await _seed(split_path)

    monkeypatch.setattr(
        pnl_timeline, "MAX_REPLAY_RECOVERY_KEY_VISITS", 2 * MAX_RECOVERY_KEY_VISITS, raising=False
    )
    monkeypatch.setattr(pnl_snapshotter, "MAX_CORRECTION_CHUNK_MINUTES", 2)
    wide = await _run_chunk(wide_path, wide_scope, next_minute=None)

    monkeypatch.setattr(pnl_snapshotter, "MAX_CORRECTION_CHUNK_MINUTES", 1)
    initial = await _run_chunk(split_path, split_scope, next_minute=_SECOND)
    assert [row["point_time"] for row in initial] == [_FIRST]
    monkeypatch.setattr(pnl_snapshotter, "MAX_CORRECTION_CHUNK_MINUTES", 2)
    resumed = await _run_chunk(split_path, split_scope, next_minute=None)

    assert [row["point_time"] for row in resumed] == [_FIRST, _SECOND]
    assert [row["valuation_status"] for row in resumed] == ["complete", "complete"]
    assert [row["fee_pnl"] for row in resumed] == [29.00000000000002, 58.00000000000004]
    assert [row["realized_pnl"] for row in resumed] == [0.0, 0.0]
    assert initial == resumed[:1]
    assert [row["point_time"] for row in wide] == [_FIRST, _SECOND]
    assert restart._economics(resumed) == restart._economics(wide)


async def test_snapshotter_splits_a_chunk_over_the_faithful_recovery_work_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Split an over-budget valuation and finish the same generation.

    Given each minute can finish its search within the aggregate replay limit.
    When the writer requests both minutes in one chunk and hits that limit.
    Then it retries the first minute, advances on the next tick, and persists
        the same values and evidence as a writer configured for one minute.
    """
    monkeypatch.delenv("PNL_FX_SHADOW_PINNING_ENABLED", raising=False)
    monkeypatch.setattr(
        pnl_timeline, "MAX_REPLAY_RECOVERY_KEY_VISITS", MAX_RECOVERY_KEY_VISITS, raising=False
    )
    reference_path = tmp_path / "minute.db"
    split_path = tmp_path / "over-budget.db"
    reference_scope = await _seed(reference_path)
    split_scope = await _seed(split_path)

    monkeypatch.setattr(pnl_snapshotter, "MAX_CORRECTION_CHUNK_MINUTES", 1)
    await _run_chunk(reference_path, reference_scope, next_minute=_SECOND)
    reference = await _run_chunk(reference_path, reference_scope, next_minute=None)

    attempts: list[tuple[datetime, datetime]] = []
    overflows: list[tuple[datetime, datetime]] = []
    run_series = pnl_snapshotter.PortfolioPnlSnapshotter._series

    async def track_replay(
        writer: pnl_snapshotter.PortfolioPnlSnapshotter,
        repo: Repository,
        ctx: pnl_snapshotter._ScopeContext,
        chunk: ChunkWindow,
        options: PnlSeriesReplayOptions,
    ) -> PnlWalletSeriesResult:
        """Record the actual valuation windows and propagate any budget error."""
        window = (chunk.start, chunk.end)
        if options.replay_generation_tip is not None:
            attempts.append(window)
        try:
            return await run_series(writer, repo, ctx, chunk, options)
        except PnlTimelineWorkBudgetError:
            overflows.append(window)
            raise

    monkeypatch.setattr(pnl_snapshotter.PortfolioPnlSnapshotter, "_series", track_replay)
    monkeypatch.setattr(pnl_snapshotter, "MAX_CORRECTION_CHUNK_MINUTES", 2)
    first = await _run_chunk(split_path, split_scope, next_minute=_SECOND)
    assert [row["point_time"] for row in first] == [_FIRST]
    result = await _run_chunk(split_path, split_scope, next_minute=None)

    assert overflows == [(_FIRST, _SECOND)]
    assert attempts == [(_FIRST, _SECOND), (_FIRST, _FIRST), (_SECOND, _SECOND)]
    assert [row["point_time"] for row in result] == [_FIRST, _SECOND]
    assert [row["valuation_status"] for row in result] == ["complete", "complete"]
    assert [row["fee_pnl"] for row in result] == [29.00000000000002, 58.00000000000004]
    assert first == result[:1]
    assert restart._economics(result) == restart._economics(reference)
