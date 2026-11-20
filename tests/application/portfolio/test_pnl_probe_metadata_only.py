"""Verify the metadata-only admission probe option and chunk-width SQLite identity."""

import json
from dataclasses import dataclass
from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import cast

import pytest

from snapper.application.portfolio import pnl_snapshotter
from snapper.application.portfolio.execution_chain import ExecutionChainError
from snapper.application.portfolio.fx_conversion_shadow import FxShadowPinContext
from snapper.application.portfolio.fx_conversion_shadow import activate_fx_shadow_context
from snapper.application.portfolio.pnl_snapshot_planner import ChunkWindow
from snapper.application.portfolio.pnl_timeline_service import PnlAnchorEvidenceError
from snapper.application.portfolio.pnl_timeline_service import PnlFrozenReplayEvidenceChangedError
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReadPolicy
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReplayOptions
from snapper.application.portfolio.pnl_timeline_service import PnlWalletSeriesResult
from snapper.application.portfolio.pnl_timeline_service import build_wallet_pnl_series
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Candle
from snapper.data.models import PortfolioPnlPoint
from snapper.data.models import WalletCredential
from snapper.data.pnl_correction_state import PnlCorrectionFrontier
from snapper.data.repository import PortfolioPnlSampleScope
from snapper.data.repository import Repository
from snapper.data.repository_types import PnlTimelineExecutionPrefix
from snapper.data.repository_types import PortfolioPnlSampleRow
from tests.application.portfolio import test_pnl_replay_annulments as annulments
from tests.application.portfolio import test_pnl_snapshotter_continuation as restart
from tests.application.portfolio import test_pnl_timeline_service as timeline
from tests.data import test_pnl_timeline_execution_prefix_repository as prefix
from tests.data import test_pnl_timeline_sample_repository as samples

_MINUTE = timedelta(minutes=1)
_FIRST = samples._T0 + _MINUTE
_SECOND = samples._T0 + 2 * _MINUTE
_THIRD = samples._T0 + 3 * _MINUTE
_FOURTH = samples._T0 + 4 * _MINUTE
_AS_OF = _FOURTH + pnl_snapshotter.FINALIZATION_LAG
_READ_ONLY = PnlSeriesReadPolicy(allow_anchor_creation=False)


@dataclass(frozen=True, slots=True)
class _SeriesCallRecord:
    """Recorded invocation attributes from the snapshotter's series adapter."""

    start: datetime
    end: datetime
    metadata_only: bool
    verify_replay_probe: bool
    frozen: bool
    point_count: int


def _probe_repo(
    *,
    has_fill_gap: bool = False,
    known_minute: int = 5,
    refuse_previous_cut: bool = False,
) -> annulments._ReplayRepo:
    """Build a repository fixture with FX, candles, accruals, lineage, and annulments."""
    repo = annulments._ReplayRepo(known_minute, refuse_previous_cut=refuse_previous_cut)
    repo._has_fill_gap = has_fill_gap
    repo._accruals = [timeline._accrual_row(timeline._I1, 2, 1.5, "EUR")]
    repo._refs = [timeline._ref(timeline._I1, "BTC-EUR", "EUR")]
    repo._candles = [timeline._candle(timeline._m(minute), 50.0) for minute in (5, 7)]
    repo._fx_rows = [timeline._fx_row("EUR", "USD", minute, 2.0) for minute in (2, 3, 5, 6, 7, 8)]
    repo._lineage = [
        {
            "order_public_id": row["order_public_id"],
            "source_surface": "rest",
            "plan_public_id": "plan-1",
            "signal_public_id": None,
            "strategy_name": None,
            "origin": "live",
        }
        for row in repo._executions
    ]
    return repo


def _install_series_spy(monkeypatch: pytest.MonkeyPatch, records: list[_SeriesCallRecord]) -> None:
    """Wrap ``PortfolioPnlSnapshotter._series`` without altering real engine execution."""
    run_series = pnl_snapshotter.PortfolioPnlSnapshotter._series

    async def tracked(
        writer: pnl_snapshotter.PortfolioPnlSnapshotter,
        repo: Repository,
        ctx: pnl_snapshotter._ScopeContext,
        chunk: ChunkWindow,
        options: PnlSeriesReplayOptions,
    ) -> PnlWalletSeriesResult:
        """Forward to the real series adapter and capture call metadata."""
        result = await run_series(writer, repo, ctx, chunk, options)
        records.append(
            _SeriesCallRecord(
                start=chunk.start,
                end=chunk.end,
                metadata_only=options.metadata_only,
                verify_replay_probe=options.verify_replay_probe,
                frozen=options.frozen_execution_watermarks is not None,
                point_count=len(result.points),
            )
        )
        return result

    monkeypatch.setattr(pnl_snapshotter.PortfolioPnlSnapshotter, "_series", tracked)


def _sqlite_candle(open_at: datetime, close: float, sequence_id: int) -> Candle:
    """Create a 1m finalized mark candle known across the test horizon."""
    return Candle(
        public_id=f"00000000-0000-7000-8000-000000000f{sequence_id:02x}",
        instrument_public_id=prefix._INSTRUMENT,
        open_at=open_at,
        timeframe="1m",
        open=close,
        high=close,
        low=close,
        close=close,
        volume=10.0,
        vwap=close,
        trades=1,
        source="native",
        complete=True,
        timestamp=open_at,
        known_to=KNOWN_TO_MAX,
        session_id=samples._SESSION,
        sequence_id=sequence_id,
    )


def _sqlite_trade(
    sequence: int, minute: datetime, side: str, price: float, fee: float
) -> list[object]:
    """Create one matched order, execution, and venue fill event at a minute."""
    order_id = f"00000000-0000-7000-8000-000000000c{sequence:02x}"
    client_id = f"probe-client-{sequence}"
    order = prefix._order(order_id, client_order_id=client_id)
    order.wallet_public_id = samples._WALLET
    order.side = side
    order.price = price
    execution = prefix._execution(
        "kraken", sequence, minute, order_id, wallet_public_id=samples._WALLET
    )
    execution.public_id = f"00000000-0000-7000-8000-000000000d{sequence:02x}"
    execution.side = side
    execution.price = price
    execution.fee = fee
    event = prefix._fill_event(
        client_id, prefix._KRAKEN_SHARD, wallet_public_id=samples._WALLET, sequence_id=sequence
    )
    event.public_id = f"00000000-0000-7000-8000-000000000e{sequence:02x}"
    event.side = side
    event.fill_price = price
    event.fee = fee
    event.timestamp = event.received_at = event.venue_timestamp = minute
    return [order, execution, event]


async def _seed_sqlite_history(path: Path) -> PortfolioPnlSampleScope:
    """Seed one anchored wallet with candles, trades, fills, and observations in SQLite."""
    async with restart._repository(path) as repo:
        await repo.create_all()
        anchor = timeline._empty_anchor(samples._WALLET, "live", "USD", samples._T0)
        symbol = prefix._symbol()
        instrument = prefix._instrument()
        balances = (1000.0, 899.5, 959.5, 959.5)
        mark_closes = (100.0, 110.0, 120.0, 130.0)
        async with repo.session() as session:
            session.add(PortfolioPnlPoint(**anchor, known_to=KNOWN_TO_MAX))
            session.add(
                WalletCredential(
                    public_id="00000000-0000-7000-8000-000000000071",
                    wallet_public_id=samples._WALLET,
                    exchange="kraken",
                    credential_type="api_key_secret",
                    encrypted_payload="isolated-probe-fixture",
                    timestamp=samples._T0,
                    session_id=samples._SESSION,
                    sequence_id=1,
                    known_to=KNOWN_TO_MAX,
                )
            )
            session.add_all([symbol, instrument])
            for idx, (minute, balance, close) in enumerate(
                zip((_FIRST, _SECOND, _THIRD, _FOURTH), balances, mark_closes, strict=True), 1
            ):
                obs = samples._observation(
                    timestamp=minute,
                    balances_json=json.dumps([{"currency": "USD", "total": balance}]),
                )
                obs.public_id = f"00000000-0000-7000-8000-000000000a{idx:02x}"
                obs.sequence_id = idx
                session.add(obs)
                session.add(_sqlite_candle(minute - _MINUTE, close, idx))
            session.add_all(_sqlite_trade(1, _FIRST, "buy", 100.0, 0.5))
            session.add_all(_sqlite_trade(2, _SECOND, "sell", 120.0, 0.5))
            await session.commit()
    return samples._scope(epoch=anchor["epoch_public_id"])


async def _fetch_state(
    path: Path, scope: PortfolioPnlSampleScope
) -> tuple[PnlCorrectionFrontier, list[PortfolioPnlSampleRow]]:
    """Read durable frontier and persisted active samples from SQLite."""
    async with restart._repository(path) as repo:
        frontier = await repo.get_portfolio_pnl_correction_frontier(scope)
        assert frontier is not None
        rows = await repo.get_portfolio_pnl_samples(
            samples._query(scope.epoch_public_id), _FIRST, _FOURTH
        )
        return frontier, rows


@pytest.mark.parametrize(
    ("options", "policy", "message"),
    [
        (
            PnlSeriesReplayOptions(
                baseline_watermarks={},
                frozen_execution_watermarks={"kraken": 1},
                metadata_only=True,
            ),
            _READ_ONLY,
            "without a frozen cut",
        ),
        (
            PnlSeriesReplayOptions(
                baseline_watermarks={},
                replay_generation_tip=timeline._m(6),
                metadata_only=True,
            ),
            _READ_ONLY,
            "without a frozen cut",
        ),
        (
            PnlSeriesReplayOptions(baseline_watermarks=None, metadata_only=True),
            _READ_ONLY,
            "without a frozen cut",
        ),
        (
            PnlSeriesReplayOptions(baseline_watermarks={}, metadata_only=True),
            PnlSeriesReadPolicy(allow_anchor_creation=True),
            "read-only historical policy",
        ),
        (
            PnlSeriesReplayOptions(baseline_watermarks={}, metadata_only=True),
            PnlSeriesReadPolicy(allow_anchor_creation=False, current_truth=True),
            "read-only historical policy",
        ),
        (
            PnlSeriesReplayOptions(baseline_watermarks={}, metadata_only=True),
            PnlSeriesReadPolicy(allow_anchor_creation=False, current_truth_horizon=True),
            "read-only historical policy",
        ),
    ],
)
async def test_metadata_only_rejects_incompatible_options_and_policies(
    options: PnlSeriesReplayOptions,
    policy: PnlSeriesReadPolicy,
    message: str,
) -> None:
    """Spec — invalid metadata_only combinations fail before any anchor lookup.

    Given: metadata_only=True paired with a frozen cut, missing baseline, or non-historical policy.
    When: build_wallet_pnl_series validates its policy before opening repository state.
    Then: It raises ValueError and issues zero anchor lookups.
    """
    repo = _probe_repo()
    with pytest.raises(ValueError, match=message):
        await build_wallet_pnl_series(
            cast(Repository, repo),
            timeline._W1,
            "live",
            timeline._m(6),
            timeline._m(6),
            "1m",
            timeline._m(9),
            policy=policy,
            options=options,
        )
    assert repo.anchor_calls == []


async def test_metadata_only_preserves_missing_anchor_and_preactivation_early_returns() -> None:
    """Spec — missing anchors and pre-activation probes still return early without replay reads.

    Given: Either a scope lacking a durable anchor or a target ending before anchor activation.
    When: build_wallet_pnl_series runs with metadata_only=True and valid historical read policy.
    Then: Both cases return early without loading executions or market data.
    """
    no_anchor = _probe_repo()
    no_anchor._has_anchor = False
    empty_result = await build_wallet_pnl_series(
        cast(Repository, no_anchor),
        timeline._W1,
        "live",
        timeline._m(6),
        timeline._m(6),
        "1m",
        timeline._m(9),
        policy=_READ_ONLY,
        options=PnlSeriesReplayOptions(baseline_watermarks={}, metadata_only=True),
    )
    assert empty_result.points == ()
    assert empty_result.replay_metadata is None
    assert no_anchor.execution_calls == []

    future_anchor_repo = timeline.FakeRepo(
        anchor=timeline._empty_anchor(timeline._W1, "live", "USD", point_time=timeline._m(5)),
        candles=[timeline._candle(timeline._m(1), 100.0)],
    )
    preactivation = await build_wallet_pnl_series(
        cast(Repository, future_anchor_repo),
        timeline._W1,
        "live",
        timeline._m(2),
        timeline._m(2),
        "1m",
        timeline._m(9),
        policy=_READ_ONLY,
        options=PnlSeriesReplayOptions(baseline_watermarks={}, metadata_only=True),
    )
    assert preactivation.replay_metadata is None
    assert preactivation.applied_annulments == ()
    assert future_anchor_repo.execution_calls == []
    assert future_anchor_repo.candle_calls == []


@pytest.mark.parametrize("has_fill_gap", [False, True])
async def test_metadata_only_matches_full_probe_metadata_and_skips_valuation_reads(
    has_fill_gap: bool,
) -> None:
    """Spec — metadata_only=True produces identical probe metadata with zero market or FX reads.

    Given: Two identical repositories with executions, annulments, accruals, candles, FX, and shadow enabled.
    When: One computes full probe valuation and the other runs with metadata_only=True.
    Then: Both emit equal replay_metadata and applied_annulments while metadata_only skips skipped reads and FX shadow work.
    """
    full_repo = _probe_repo(has_fill_gap=has_fill_gap)
    probe_repo = _probe_repo(has_fill_gap=has_fill_gap)
    full_shadow = FxShadowPinContext(calculation_version="5C.5", evaluations=[])
    probe_shadow = FxShadowPinContext(calculation_version="5C.5", evaluations=[])
    shared_options = PnlSeriesReplayOptions(
        baseline_watermarks={"kraken": 4},
        offload_valuation=True,
        previous_replay_as_of=timeline._m(7),
        verify_replay_probe=True,
        pointwise_exact_fx=True,
    )

    with activate_fx_shadow_context(full_shadow):
        full_result = await build_wallet_pnl_series(
            cast(Repository, full_repo),
            timeline._W1,
            "live",
            timeline._m(6),
            timeline._m(6),
            "1m",
            timeline._m(9),
            policy=_READ_ONLY,
            options=shared_options,
        )

    with activate_fx_shadow_context(probe_shadow):
        probe_result = await build_wallet_pnl_series(
            cast(Repository, probe_repo),
            timeline._W1,
            "live",
            timeline._m(6),
            timeline._m(6),
            "1m",
            timeline._m(9),
            policy=_READ_ONLY,
            options=replace(shared_options, metadata_only=True),
        )

    assert probe_result.points == ()
    assert probe_result.rate_sources == ()
    assert probe_result.equity_overlay == {}
    assert probe_result.replay_metadata == full_result.replay_metadata
    assert probe_result.applied_annulments == full_result.applied_annulments
    assert probe_repo.execution_prefix_intents == [timeline._m(9), timeline._m(9)]
    assert len(probe_repo.fill_gap_calls) == 1
    assert len(probe_repo.previous_cuts) == 1
    assert probe_repo.lineage_calls == []
    assert probe_repo.accrual_calls == []
    assert probe_repo.symbol_ref_calls == []
    assert probe_repo.candle_calls == []
    assert probe_repo.fx_candidate_pair_calls == []
    assert probe_repo.fx_pair_calls == []
    assert probe_repo.fx_exact_minute_calls == []
    assert probe_shadow.evaluations == []
    assert len(full_repo.lineage_calls) == 1
    assert len(full_repo.accrual_calls) == 1
    assert len(full_repo.symbol_ref_calls) == 1
    assert len(full_repo.candle_calls) == 1
    assert full_repo.fx_candidate_pair_calls
    assert full_repo.fx_pair_calls
    assert full_shadow.evaluations


@pytest.mark.parametrize("failure", ["probe_race", "watermark_regression", "anchor_manifest_drift"])
async def test_metadata_only_retains_certification_and_anchor_consistency_guards(
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    """Spec — metadata_only keeps recertification, anchor watermark, and manifest checks intact.

    Given: A metadata_only probe encountering a mid-probe witness race, watermark regression, or anchor drift.
    When: build_wallet_pnl_series validates prefix evidence for the probe.
    Then: The corresponding certification or anchor integrity error still raises without reading market data.
    """
    repo = _probe_repo()
    options = PnlSeriesReplayOptions(
        baseline_watermarks={},
        verify_replay_probe=True,
        metadata_only=True,
    )
    expected: type[Exception]
    if failure == "probe_race":
        original_gap = repo.pnl_timeline_scope_has_fill_gap

        async def shift_witness(
            wallet_public_id: str,
            mode: str,
            as_of: datetime | None,
            prefix_arg: object,
        ) -> bool:
            """Mutate fill witness digest during gap analysis to trigger recertification failure."""
            repo._fill_witness_digest = "d" * 64
            return await original_gap(
                wallet_public_id, mode, as_of, cast(PnlTimelineExecutionPrefix, prefix_arg)
            )

        monkeypatch.setattr(repo, "pnl_timeline_scope_has_fill_gap", shift_witness)
        expected = PnlFrozenReplayEvidenceChangedError
    elif failure == "watermark_regression":
        repo._anchor = timeline._anchor_recording({"kraken": 99}, [])
        expected = PnlAnchorEvidenceError
    else:
        repo._anchor = timeline._anchor_recording({"kraken": 4}, [])
        repo._annulments_known_from = timeline._m(1)
        expected = ExecutionChainError

    with pytest.raises(expected):
        await build_wallet_pnl_series(
            cast(Repository, repo),
            timeline._W1,
            "live",
            timeline._m(6),
            timeline._m(6),
            "1m",
            timeline._m(9),
            policy=_READ_ONLY,
            options=options,
        )
    assert repo.symbol_ref_calls == []
    assert repo.candle_calls == []


async def test_snapshotter_metadata_only_probe_preserves_restarted_mixed_width_sqlite_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Spec — metadata-only probe matches uninterrupted vs restarted mixed-width SQLite persistence.

    Given: Real SQLite scopes with candles, matched executions, fills, and balance observations.
    When: An uninterrupted width-4 replay and a restarted 1 -> 2 mixed-width replay run with metadata_only probes.
    Then: Persisted sample rows, audit envelopes, watermarks, and frontier state match identically.
    """
    monkeypatch.delenv("PNL_FX_SHADOW_PINNING_ENABLED", raising=False)
    wide_path = tmp_path / "wide-probe.db"
    split_path = tmp_path / "split-probe.db"
    wide_scope = await _seed_sqlite_history(wide_path)
    split_scope = await _seed_sqlite_history(split_path)

    wide_calls: list[_SeriesCallRecord] = []
    with monkeypatch.context() as wide_patch:
        _install_series_spy(wide_patch, wide_calls)
        monkeypatch.setattr(pnl_snapshotter, "MAX_CORRECTION_CHUNK_MINUTES", 4)
        async with restart._repository(wide_path) as repo:
            snap = restart._snapshotter(repo, _AS_OF)
            await snap._tick_once()
            await restart._snapshotter(repo, _AS_OF + timedelta(seconds=10))._tick_once()

    split_calls: list[_SeriesCallRecord] = []
    with monkeypatch.context() as split_patch:
        _install_series_spy(split_patch, split_calls)
        monkeypatch.setattr(pnl_snapshotter, "MAX_CORRECTION_CHUNK_MINUTES", 1)
        async with restart._repository(split_path) as repo:
            await restart._snapshotter(repo, _AS_OF)._tick_once()
        partial_frontier, _ = await _fetch_state(split_path, split_scope)
        assert partial_frontier.next_minute == _SECOND

        monkeypatch.setattr(pnl_snapshotter, "MAX_CORRECTION_CHUNK_MINUTES", 2)
        async with restart._repository(split_path) as repo:
            await restart._snapshotter(repo, _AS_OF)._tick_once()
        mid_frontier, _ = await _fetch_state(split_path, split_scope)
        assert mid_frontier.next_minute == _FOURTH

        async with restart._repository(split_path) as repo:
            await restart._snapshotter(repo, _AS_OF)._tick_once()
            await restart._snapshotter(repo, _AS_OF + timedelta(seconds=10))._tick_once()

    wide_frontier, wide_rows = await _fetch_state(wide_path, wide_scope)
    split_frontier, split_rows = await _fetch_state(split_path, split_scope)

    wide_probes = [call for call in wide_calls if call.verify_replay_probe]
    wide_chunks = [call for call in wide_calls if not call.verify_replay_probe]
    split_probes = [call for call in split_calls if call.verify_replay_probe]
    split_chunks = [call for call in split_calls if not call.verify_replay_probe]

    assert len(wide_probes) == 2 and all(
        c.metadata_only and c.point_count == 0 for c in wide_probes
    )
    assert [(c.start, c.end, c.metadata_only, c.frozen) for c in wide_chunks] == [
        (_FIRST, _FOURTH, False, True)
    ]
    assert len(split_probes) == 2 and all(
        c.metadata_only and c.point_count == 0 for c in split_probes
    )
    assert [(c.start, c.end, c.metadata_only, c.frozen) for c in split_chunks] == [
        (_FIRST, _FIRST, False, True),
        (_SECOND, _THIRD, False, True),
        (_FOURTH, _FOURTH, False, True),
    ]
    assert [row["point_time"] for row in split_rows] == [_FIRST, _SECOND, _THIRD, _FOURTH]
    assert [row["valuation_status"] for row in split_rows] == ["complete"] * 4
    assert restart._economics(split_rows) == restart._economics(wide_rows)
    assert wide_frontier.revision == 2
    assert split_frontier.revision == 4
    assert replace(split_frontier, revision=wide_frontier.revision) == wide_frontier
