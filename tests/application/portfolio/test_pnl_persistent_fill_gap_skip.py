"""Verify skipping settled persistently gapped P&L scopes with liveness telemetry."""

import json
import logging
from collections.abc import Callable
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import cast

import pytest

from snapper.application.portfolio import pnl_snapshotter
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReplayMetadata
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReplayOptions
from snapper.application.portfolio.pnl_timeline_service import PnlWalletSeriesResult
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Execution
from snapper.data.models import Order
from snapper.data.models import PortfolioPnlPoint
from snapper.data.models import VenueEvent
from snapper.data.models import WalletCredential
from snapper.data.pnl_correction_state import PnlCorrectionChunk
from snapper.data.pnl_correction_state import PnlCorrectionEvidence
from snapper.data.pnl_correction_state import PnlCorrectionFrontier
from snapper.data.pnl_correction_state import PnlCorrectionRequest
from snapper.data.pnl_correction_state import PnlCorrectionSampleVersion
from snapper.data.pnl_correction_state import advance_correction_frontier
from snapper.data.pnl_correction_state import start_correction_frontier
from snapper.data.repository import PortfolioPnlSampleQuery
from snapper.data.repository import PortfolioPnlSampleScope
from snapper.data.repository import Repository
from snapper.data.repository_types import PNL_SAMPLE_CALC_VERSION
from snapper.data.repository_types import PnlTimelineFillWitnessCut
from snapper.data.repository_types import PortfolioPnlSampleRow
from tests.application.portfolio import test_pnl_probe_metadata_only as probe_helpers
from tests.application.portfolio import test_pnl_snapshotter as snap_fixtures
from tests.application.portfolio import test_pnl_snapshotter_continuation as restart
from tests.application.portfolio import test_pnl_timeline_service as timeline
from tests.data import test_pnl_timeline_execution_prefix_repository as prefix
from tests.data import test_pnl_timeline_sample_repository as samples

_MINUTE = timedelta(minutes=1)
_T0 = samples._T0
_M1 = _T0 + _MINUTE
_M2 = _T0 + 2 * _MINUTE
_M3 = _T0 + 3 * _MINUTE
_M4 = _T0 + 4 * _MINUTE
_M5 = _T0 + 5 * _MINUTE
_M6 = _T0 + 6 * _MINUTE
_MANIFEST_A = "a" * 64
_MANIFEST_B = "b" * 64
_WITNESS_DIGEST_A = "c" * 64
_WITNESS_DIGEST_B = "d" * 64
_DEFAULT_WITNESS = PnlTimelineFillWitnessCut(
    watermarks={"kraken:spot": 10}, digest=_WITNESS_DIGEST_A
)


@dataclass(frozen=True, slots=True)
class _SeriesTrace:
    """Capture _series invocations during persistent fill-gap tests."""

    start: datetime
    end: datetime
    as_of: datetime
    verify_replay_probe: bool
    metadata_only: bool


class _GapFakeRepo(snap_fixtures._FakeRepo):
    """Fake repository adding active occupancy lookup and call counters for gap-skip tests."""

    def __init__(self) -> None:
        """Initialize an anchored kraken live scope with query tracking."""
        super().__init__(
            credentials=[snap_fixtures._cred("kraken")],
            anchor=snap_fixtures._anchor(),
        )
        self.occupancy_calls: list[tuple[PortfolioPnlSampleScope, datetime, datetime]] = []
        self.evidence_reads: list[tuple[datetime, datetime]] = []
        self.invalidation_requests: list[PnlCorrectionRequest] = []

    async def get_portfolio_pnl_correction_evidence(
        self, scope: PortfolioPnlSampleScope, start: datetime, end: datetime
    ) -> PnlCorrectionEvidence:
        """Record correction evidence reads alongside the base fake evidence snapshot.

        Args:
            scope: Target sample scope.
            start: Inclusive chunk start minute.
            end: Inclusive chunk end minute.

        Returns:
            Physical active versions and causal prior peak.
        """
        self.evidence_reads.append((start, end))
        return await super().get_portfolio_pnl_correction_evidence(scope, start, end)

    async def invalidate_portfolio_pnl_correction(
        self,
        scope: PortfolioPnlSampleScope,
        request: PnlCorrectionRequest,
        expected: PnlCorrectionFrontier | None,
    ) -> PnlCorrectionFrontier:
        """Record started generations in order.

        Args:
            scope: Target sample scope.
            request: Requested replay bounds and evidence cuts.
            expected: Previously observed checkpoint or ``None``.

        Returns:
            Newly committed correction frontier.
        """
        self.invalidation_requests.append(request)
        return await super().invalidate_portfolio_pnl_correction(scope, request, expected)

    async def has_active_portfolio_pnl_samples(
        self,
        scope: PortfolioPnlSampleScope,
        start: datetime,
        end: datetime,
    ) -> bool:
        """Check occupancy across all active samples sharing wallet, mode, and currency.

        Args:
            scope: Target sample scope.
            start: Inclusive range start minute.
            end: Inclusive range end minute.

        Returns:
            ``True`` when any active row occupies ``[start .. end]``.
        """
        self.occupancy_calls.append((scope, start, end))
        return any(
            start <= version.row["point_time"] <= end
            for version in self._scope_rows(list(self._samples.values()), scope)
        )


def _gap_metadata(
    *,
    seq: int = 4,
    has_fill_gap: bool = True,
    affected: datetime | None = None,
    manifest: str = _MANIFEST_A,
    witness: PnlTimelineFillWitnessCut = _DEFAULT_WITNESS,
) -> PnlSeriesReplayMetadata:
    """Build deterministic replay metadata matching the gapped frontier defaults.

    Args:
        seq: Raw and scope execution watermark sequence.
        has_fill_gap: Global fill-gap verdict returned by probe.
        affected: Earliest affected late-fill minute, or ``None``.
        manifest: Execution annulment manifest digest.
        witness: Fill-witness bounds and digest cut.

    Returns:
        Constructed :class:`PnlSeriesReplayMetadata`.
    """
    raw = {"kraken": seq}
    return PnlSeriesReplayMetadata(
        max_scope_sequence_by_exchange=raw,
        earliest_affected_minute=affected,
        raw_execution_watermarks=dict(raw),
        execution_manifest_digest=manifest,
        has_fill_gap=has_fill_gap,
        fill_witness_cut=PnlTimelineFillWitnessCut(
            watermarks=dict(witness["watermarks"]),
            digest=witness["digest"],
        ),
    )


def _seed_settled_gap_frontier(
    repo: _GapFakeRepo,
    tip: datetime = _M3,
    *,
    window_start: datetime = _M1,
    repair_from: datetime | None = _M1,
    has_fill_gap: bool = True,
) -> PnlCorrectionFrontier:
    """Install a settled correction frontier directly on the fake repository.

    Args:
        repo: In-memory test repository.
        tip: Settled frontier tip minute.
        window_start: First minute admitted in the settled generation.
        repair_from: Saved repair obligation minute, or ``None``.
        has_fill_gap: Frozen fill-gap verdict on the frontier.

    Returns:
        Settled :class:`PnlCorrectionFrontier` persisted on ``repo``.
    """
    scope = snap_fixtures._sample_scope()
    evidence = repo._evidence(scope, window_start, tip)
    watermark = {"kraken": 4}
    initial = start_correction_frontier(
        evidence.key,
        PnlCorrectionRequest(
            start=window_start,
            tip=tip,
            as_of=tip + timedelta(minutes=2),
            baseline_watermarks={},
            pending_watermarks=watermark,
            prior_peak=None,
            raw_execution_watermarks=watermark,
            execution_manifest_digest=_MANIFEST_A,
            has_fill_gap=has_fill_gap,
            expected_venues=frozenset({"kraken"}),
            venue_event_watermarks=dict(_DEFAULT_WITNESS["watermarks"]),
            fill_witness_digest=_DEFAULT_WITNESS["digest"],
        ),
        None,
    )
    withheld = (repair_from,) if repair_from is not None else ()
    settled = advance_correction_frontier(
        initial,
        PnlCorrectionChunk(
            end=tip,
            edits=(),
            prior_peak=None,
            evidence=evidence,
            withheld_minutes=withheld,
            reconciles_repair=False,
        ),
    )
    repo._frontiers[evidence.key] = settled
    return settled


def _install_metadata_series(
    monkeypatch: pytest.MonkeyPatch,
    metadata_factory: Callable[[], PnlSeriesReplayMetadata],
    traces: list[_SeriesTrace],
) -> None:
    """Install a fake build_wallet_pnl_series recording every invocation.

    Args:
        monkeypatch: Pytest monkeypatch helper.
        metadata_factory: Zero-arg factory supplying probe and chunk metadata.
        traces: List receiving recorded call descriptors.
    """

    async def fake_series(*args: object, **kwargs: object) -> PnlWalletSeriesResult:
        """Record one series call and return deterministic synthetic series output."""
        start = args[3]
        end = args[4]
        as_of = args[6]
        options = kwargs["options"]
        assert isinstance(start, datetime) and isinstance(end, datetime)
        assert isinstance(as_of, datetime) and isinstance(options, PnlSeriesReplayOptions)
        traces.append(
            _SeriesTrace(
                start=start,
                end=end,
                as_of=as_of,
                verify_replay_probe=options.verify_replay_probe,
                metadata_only=options.metadata_only,
            )
        )
        metadata = metadata_factory()
        return snap_fixtures._series(
            start, end, metadata=metadata, untrusted=bool(metadata.has_fill_gap)
        )

    monkeypatch.setattr(pnl_snapshotter, "build_wallet_pnl_series", fake_series)


def _skip_logs(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    """Return captured INFO skip liveness records."""
    return [
        record
        for record in caplog.records
        if getattr(record, "scope_skip_reason", None) == "persistent_fill_gap_skip"
    ]


async def test_probe_replay_preserves_true_earliest_affected_minute(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Preserve genuine earliest_affected_minute from metadata probes when fill gap holds.

    Given: A probe metadata payload with has_fill_gap=True and earliest_affected_minute=None.
    When: PortfolioPnlSnapshotter._probe_replay invokes the metadata-only series probe.
    Then: The returned metadata preserves earliest_affected_minute=None rather than forcing t0+1m.
    """
    repo = _GapFakeRepo()
    snap = snap_fixtures._snapshotter(cast(snap_fixtures._FakeRepo, repo), _M5)
    ctx = snap._build_scope_context(
        snap_fixtures._WALLET, snap_fixtures._anchor(), frozenset({"kraken"}), _M5
    )
    traces: list[_SeriesTrace] = []
    _install_metadata_series(
        monkeypatch,
        lambda: _gap_metadata(has_fill_gap=True, affected=None),
        traces,
    )
    metadata = await snap._probe_replay(cast(Repository, repo), ctx, None, _M3)
    assert metadata.has_fill_gap is True
    assert metadata.earliest_affected_minute is None
    assert len(traces) == 1 and traces[0].metadata_only and traces[0].verify_replay_probe


async def test_settled_gapped_scope_skips_stationary_and_advancing_ticks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Skip re-replay on stationary and tip-advancing ticks with unchanged gap evidence.

    Given: A settled gapped frontier spanning from M1 to M3 with zero active samples.
    When: Stationary and tip-advancing ticks run with identical execution, manifest,
        venue, and witness evidence.
    Then: Only metadata_only probes and bounded occupancy checks execute; generation,
        frontier fields, chunk commits, and persisted samples remain untouched.
    """
    repo = _GapFakeRepo()
    settled = _seed_settled_gap_frontier(repo, tip=_M3)
    traces: list[_SeriesTrace] = []
    _install_metadata_series(monkeypatch, lambda: _gap_metadata(), traces)

    now = settled.as_of
    snap = pnl_snapshotter.PortfolioPnlSnapshotter(
        repo=cast(Repository, repo),
        interval_seconds=60,
        disabled=False,
        clock=lambda: now,
    )
    await snap._tick_once()
    now = settled.as_of + _MINUTE
    await snap._tick_once()
    now = settled.as_of + 2 * _MINUTE
    await snap._tick_once()

    assert [trace for trace in traces if not trace.verify_replay_probe] == []
    assert [(trace.start, trace.end, trace.metadata_only) for trace in traces] == [
        (_M3, _M3, True),
        (_M4, _M4, True),
        (_M5, _M5, True),
    ]
    assert [(start, end) for _scope, start, end in repo.occupancy_calls] == [
        (_M1, _M3),
        (_M1, _M4),
        (_M1, _M5),
    ]
    assert repo.invalidation_requests == []
    assert repo.evidence_reads == []
    assert repo.committed_chunks == []
    assert repo.persisted == []
    assert repo.frontier() == settled


@pytest.mark.parametrize(
    "condition",
    [
        "raw_watermarks_changed",
        "manifest_digest_changed",
        "fill_witness_digest_changed",
        "venue_event_watermarks_changed",
        "expected_venues_changed",
        "late_fill_at_or_before_tip",
        "window_start_not_first",
        "repair_from_not_first",
        "repair_from_none",
        "active_same_epoch_sample",
        "active_foreign_epoch_sample",
        "active_legacy_calc_version_sample",
        "gap_true_to_false",
        "gap_false_to_true",
    ],
)
async def test_break_conditions_force_fresh_anchor_generation(
    monkeypatch: pytest.MonkeyPatch,
    condition: str,
) -> None:
    """Break persistent-gap skip whenever evidence, occupancy, frontier invariants, or gap change.

    Given: A settled frontier and one specific break condition violating the skip proof.
    When: The next tick processes the scope at an advancing tip M4.
    Then: The snapshotter does not skip and starts a fresh generation from first post-anchor M1.
    """
    repo = _GapFakeRepo()
    if condition == "window_start_not_first":
        settled = _seed_settled_gap_frontier(repo, tip=_M3, window_start=_M2, repair_from=_M2)
    elif condition == "repair_from_not_first":
        settled = _seed_settled_gap_frontier(repo, tip=_M3, repair_from=_M2)
    elif condition == "repair_from_none":
        settled = _seed_settled_gap_frontier(repo, tip=_M3, repair_from=None)
    elif condition == "gap_false_to_true":
        settled = _seed_settled_gap_frontier(repo, tip=_M3, repair_from=None, has_fill_gap=False)
    else:
        settled = _seed_settled_gap_frontier(repo, tip=_M3)

    if condition == "expected_venues_changed":
        repo._credentials.append(snap_fixtures._cred("walutomat"))
    if condition == "active_same_epoch_sample":
        row = snap_fixtures._persisted_sample(_M4, status="complete")
        repo._samples[repo._sample_key(row)] = PnlCorrectionSampleVersion(10, row)
    elif condition == "active_foreign_epoch_sample":
        row = snap_fixtures._persisted_sample(_M2, status="complete")
        row["epoch_public_id"] = samples._OTHER_EPOCH
        repo._samples[repo._sample_key(row)] = PnlCorrectionSampleVersion(11, row)
    elif condition == "active_legacy_calc_version_sample":
        row = snap_fixtures._persisted_sample(_M3, status="complete")
        row["calc_version"] = "5C.3"
        repo._samples[repo._sample_key(row)] = PnlCorrectionSampleVersion(12, row)

    witness = PnlTimelineFillWitnessCut(
        watermarks=(
            {"kraken:spot": 11}
            if condition == "venue_event_watermarks_changed"
            else dict(_DEFAULT_WITNESS["watermarks"])
        ),
        digest=(
            _WITNESS_DIGEST_B if condition == "fill_witness_digest_changed" else _WITNESS_DIGEST_A
        ),
    )
    probe_meta = _gap_metadata(
        seq=5 if condition == "raw_watermarks_changed" else 4,
        has_fill_gap=condition != "gap_true_to_false",
        affected=_M2 if condition == "late_fill_at_or_before_tip" else None,
        manifest=_MANIFEST_B if condition == "manifest_digest_changed" else _MANIFEST_A,
        witness=witness,
    )

    traces: list[_SeriesTrace] = []
    _install_metadata_series(monkeypatch, lambda: probe_meta, traces)

    snap = snap_fixtures._snapshotter(cast(snap_fixtures._FakeRepo, repo), _M6)
    await snap._tick_once()

    assert len(repo.invalidation_requests) == 1
    assert repo.invalidation_requests[0].start == _M1
    assert repo.frontier().generation == settled.generation + 1
    assert repo.frontier().window_start == _M1
    assert any(not trace.verify_replay_probe and trace.start == _M1 for trace in traces)
    if condition == "gap_true_to_false":
        assert repo.frontier().has_fill_gap is False
        assert repo.frontier().repair_from is None
    else:
        assert repo.frontier().has_fill_gap is True
        assert repo.frontier().repair_from == _M1


async def test_replay_start_does_not_skip_when_frontier_next_minute_is_pending() -> None:
    """Refuse skipping when an unsettled frontier reaches _replay_start directly.

    Given: An unsettled gapped frontier whose next_minute is still non-None.
    When: _replay_start evaluates skip eligibility.
    Then: It returns first (M1) instead of skipping.
    """
    repo = _GapFakeRepo()
    settled = _seed_settled_gap_frontier(repo, tip=_M3)
    pending_frontier = replace(settled, next_minute=_M2)
    snap = snap_fixtures._snapshotter(cast(snap_fixtures._FakeRepo, repo), _M5)
    ctx = snap._build_scope_context(
        snap_fixtures._WALLET, snap_fixtures._anchor(), frozenset({"kraken"}), _M5
    )
    start = await snap._replay_start(
        cast(Repository, repo), ctx, pending_frontier, _M3, _gap_metadata()
    )
    assert start == (_M1, None)


def _assert_skip_record_fields(
    record: logging.LogRecord,
    frontier: PnlCorrectionFrontier,
    as_of: datetime,
) -> None:
    """Assert structured liveness fields attached to persistent_fill_gap_skip records."""
    assert getattr(record, "wallet_public_id") == snap_fixtures._WALLET
    assert getattr(record, "mode") == "live"
    assert getattr(record, "valuation_ccy") == "USD"
    assert getattr(record, "epoch_public_id") == snap_fixtures._EPOCH
    assert getattr(record, "frontier_tip") == frontier.tip
    assert getattr(record, "frontier_as_of") == frontier.as_of
    assert getattr(record, "as_of") == as_of
    assert getattr(record, "gap_age") == as_of - frontier.as_of


async def test_liveness_emits_once_per_streak_ignores_reset_and_rearms_on_resume(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Emit one persistent_fill_gap_skip record per pause streak with full scope telemetry.

    Given: A settled gapped scope followed by repeated skip ticks, a resumed replay
        with new gap evidence, a second pause streak, gap clearing, and a later pause.
    When: Each phase ticks through the snapshotter and absent scopes are pruned.
    Then: Each pause streak emits exactly one structured log record carrying scope,
        frozen tip/as_of, current as_of, and gap_age, unaffected by _reset_scope_logs.
    """
    repo = _GapFakeRepo()
    initial = _seed_settled_gap_frontier(repo, tip=_M3)
    state: dict[str, PnlSeriesReplayMetadata] = {"meta": _gap_metadata()}
    traces: list[_SeriesTrace] = []
    _install_metadata_series(monkeypatch, lambda: state["meta"], traces)

    now = initial.as_of
    snap = pnl_snapshotter.PortfolioPnlSnapshotter(
        repo=cast(Repository, repo), interval_seconds=60, disabled=False, clock=lambda: now
    )

    with caplog.at_level("INFO", logger=pnl_snapshotter.__name__):
        now = initial.as_of - timedelta(seconds=10)
        await snap._tick_once()
        assert _skip_logs(caplog) == []

        first_skip_at = initial.as_of + _MINUTE
        for tick_offset in (1, 2):
            now = initial.as_of + tick_offset * _MINUTE
            await snap._tick_once()
        snap._reset_scope_logs(snap_fixtures._WALLET)
        now = initial.as_of + 3 * _MINUTE
        await snap._tick_once()

        first_logs = _skip_logs(caplog)
        assert len(first_logs) == 1
        _assert_skip_record_fields(first_logs[0], initial, first_skip_at)

        state["meta"] = _gap_metadata(seq=5, has_fill_gap=True)
        for tick_offset in (4, 5):
            now = initial.as_of + tick_offset * _MINUTE
            await snap._tick_once()
        assert len(_skip_logs(caplog)) == 2

        state["meta"] = _gap_metadata(seq=5, has_fill_gap=False)
        now = initial.as_of + 6 * _MINUTE
        await snap._tick_once()
        assert repo.frontier().has_fill_gap is False and repo.frontier().repair_from is None

        state["meta"] = _gap_metadata(seq=6, has_fill_gap=True)
        for tick_offset in (7, 8):
            now = initial.as_of + tick_offset * _MINUTE
            await snap._tick_once()
        assert len(_skip_logs(caplog)) == 3

        assert len(snap._persistent_fill_gap_skip_logged) == 1
        snap._prune_absent_scopes({snap_fixtures._WALLET})
        assert len(snap._persistent_fill_gap_skip_logged) == 1
        snap._prune_absent_scopes(set())
        assert snap._persistent_fill_gap_skip_logged == set()


def _sqlite_candle_at(minute: datetime, close: float, sequence_id: int) -> object:
    """Create a 1m finalized mark candle with a unique identifier."""
    candle = probe_helpers._sqlite_candle(minute - _MINUTE, close, sequence_id)
    candle.public_id = f"00000000-0000-7000-8000-0000000f{sequence_id:04x}"
    return candle


def _sqlite_order(order_num: int, side: str, price: float) -> Order:
    """Create one deterministic Order row matching test wallet and shard defaults."""
    order_id = f"00000000-0000-7000-8000-000000000c{order_num:02x}"
    order = prefix._order(order_id, client_order_id=f"gap-client-{order_num}")
    order.wallet_public_id = samples._WALLET
    order.side = side
    order.price = price
    return order


def _sqlite_execution(order_num: int, scope_sequence: int, minute: datetime) -> Execution:
    """Create one contiguous Execution matching order ``order_num`` by exec and trade ids."""
    order_id = f"00000000-0000-7000-8000-000000000c{order_num:02x}"
    execution = prefix._execution(
        "kraken",
        scope_sequence,
        minute,
        order_id,
        wallet_public_id=samples._WALLET,
        exec_id=f"exec-kraken-{order_num}",
        trade_id=f"trade-kraken-{order_num}",
    )
    execution.public_id = f"00000000-0000-7000-8000-000000000d{order_num:02x}"
    execution.side = "sell" if order_num == 3 else "buy"
    execution.price = {1: 100.0, 2: 110.0, 3: 120.0}[order_num]
    execution.fee = 0.5
    return execution


def _sqlite_fill(order_num: int, sequence_id: int, minute: datetime) -> VenueEvent:
    """Create one VenueEvent fill matching order ``order_num`` by exec and trade ids."""
    event = prefix._fill_event(
        f"gap-client-{order_num}",
        prefix._KRAKEN_SHARD,
        wallet_public_id=samples._WALLET,
        sequence_id=sequence_id,
        execution_scope_sequence=None,
    )
    event.public_id = f"00000000-0000-7000-8000-000000000e{order_num:02x}"
    event.side = "sell" if order_num == 3 else "buy"
    event.fill_price = {1: 100.0, 2: 110.0, 3: 120.0}[order_num]
    event.fee = 0.5
    event.exec_id = f"exec-kraken-{order_num}"
    event.trade_id = f"trade-kraken-{order_num}"
    event.timestamp = event.received_at = event.venue_timestamp = minute
    return event


async def _seed_sqlite_gapped_history(path: Path) -> PortfolioPnlSampleScope:
    """Seed six observation minutes with one matched trade and one unconsumed fill event at M5."""
    async with restart._repository(path) as repo:
        await repo.create_all()
        anchor = timeline._empty_anchor(samples._WALLET, "live", "USD", _T0)
        symbol = prefix._symbol()
        instrument = prefix._instrument()
        async with repo.session() as session:
            session.add(PortfolioPnlPoint(**anchor, known_to=KNOWN_TO_MAX))
            session.add(
                WalletCredential(
                    public_id="00000000-0000-7000-8000-000000000071",
                    wallet_public_id=samples._WALLET,
                    exchange="kraken",
                    credential_type="api_key_secret",
                    encrypted_payload="isolated-gap-skip-fixture",
                    timestamp=_T0,
                    session_id=samples._SESSION,
                    sequence_id=1,
                    known_to=KNOWN_TO_MAX,
                )
            )
            session.add_all([symbol, instrument])
            for idx, minute in enumerate((_M1, _M2, _M3, _M4, _M5, _M6), start=1):
                obs = samples._observation(
                    timestamp=minute,
                    balances_json=json.dumps([{"currency": "USD", "total": 1000.0}]),
                )
                obs.public_id = f"00000000-0000-7000-8000-0000000a{idx:04x}"
                obs.sequence_id = idx
                session.add(obs)
                session.add(_sqlite_candle_at(minute, 100.0 + idx * 5.0, idx))
            session.add_all(
                [
                    _sqlite_order(1, "buy", 100.0),
                    _sqlite_execution(1, 1, _M1),
                    _sqlite_fill(1, 1, _M1),
                    _sqlite_order(2, "buy", 110.0),
                    _sqlite_fill(2, 2, _M5),
                ]
            )
            await session.commit()
    return samples._scope(epoch=anchor["epoch_public_id"])


async def _append_matched_fill_three(path: Path) -> None:
    """Append a second matched execution at M4 while trade 2 fill remains unconsumed."""
    async with restart._repository(path) as repo, repo.session() as session:
        session.add_all(
            [
                _sqlite_order(3, "sell", 120.0),
                _sqlite_execution(3, 2, _M4),
                _sqlite_fill(3, 3, _M4),
            ]
        )
        await session.commit()


async def _match_missing_execution_two(path: Path) -> None:
    """Commit the missing execution for trade 2 so the global fill gap clears."""
    async with restart._repository(path) as repo, repo.session() as session:
        session.add(_sqlite_execution(2, 3, _M5))
        await session.commit()


async def _drive_sqlite_replay(
    path: Path,
    scope: PortfolioPnlSampleScope,
    as_of: datetime,
    widths: Sequence[int],
    monkeypatch: pytest.MonkeyPatch,
) -> PnlCorrectionFrontier:
    """Drive ticks across reopened connections using configured chunk widths until settled."""
    step = 0
    while True:
        width = widths[min(step, len(widths) - 1)]
        monkeypatch.setattr(pnl_snapshotter, "MAX_CORRECTION_CHUNK_MINUTES", width)
        async with restart._repository(path) as repo:
            await restart._snapshotter(repo, as_of)._tick_once()
            frontier = await repo.get_portfolio_pnl_correction_frontier(scope)
            assert frontier is not None
            if frontier.next_minute is None:
                return frontier
        step += 1


async def _run_paused_ticks(
    path: Path,
    start_clock: datetime,
    second_clock: datetime,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Run two stationary/advancing pause ticks on one live snapshotter instance."""
    current = {"now": start_clock}
    async with restart._repository(path) as repo:
        snap = pnl_snapshotter.PortfolioPnlSnapshotter(
            repo=repo, interval_seconds=60, disabled=False, clock=lambda: current["now"]
        )
        with caplog.at_level("INFO", logger=pnl_snapshotter.__name__):
            await snap._tick_once()
            current["now"] = second_clock
            await snap._tick_once()


async def _read_persisted_and_reconciled(
    path: Path, scope: PortfolioPnlSampleScope
) -> tuple[PnlCorrectionFrontier, list[PortfolioPnlSampleRow], list[PortfolioPnlSampleRow]]:
    """Read frontier, all persisted active rows, and require_reconciled=True rows from SQLite."""
    query = PortfolioPnlSampleQuery(
        wallet_public_id=scope.wallet_public_id,
        mode=scope.mode,
        valuation_ccy=scope.valuation_ccy,
        epoch_public_id=scope.epoch_public_id,
        calc_version=PNL_SAMPLE_CALC_VERSION,
    )
    published = replace(query, require_reconciled=True)
    async with restart._repository(path) as repo:
        frontier = await repo.get_portfolio_pnl_correction_frontier(scope)
        assert frontier is not None
        all_rows = await repo.get_portfolio_pnl_samples(query, _M1, _M6)
        reconciled_rows = await repo.get_portfolio_pnl_samples(published, _M1, _M6)
        return frontier, all_rows, reconciled_rows


async def test_sqlite_mixed_width_restart_lifecycle_matches_uninterrupted_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Preserve exact persisted rows and settled frontier across skip, resume, and clear.

    Given: Two identical real SQLite histories with one unconsumed fill event at M5.
    When: Run A advances uninterrupted with wide chunks while Run B restarts with mixed
        narrow chunk widths through: gapped Gen1 -> pause ticks -> matched fill at M4 ->
        gap resolution at M5 -> final settlement at M6.
    Then: Both produce identical require_reconciled=True reads during and after the gap,
        identical final sample economics (values, audit_json, watermarks_json), and identical
        settled PnlCorrectionFrontier fields except ``revision`` (excluded solely because
        Run B commits more intermediate narrower chunks than uninterrupted Run A).
    """
    monkeypatch.delenv("PNL_FX_SHADOW_PINNING_ENABLED", raising=False)
    wide_path = tmp_path / "wide-gap-lifecycle.db"
    split_path = tmp_path / "split-gap-lifecycle.db"
    wide_scope = await _seed_sqlite_gapped_history(wide_path)
    split_scope = await _seed_sqlite_gapped_history(split_path)

    as_of_g1 = _M3 + pnl_snapshotter.FINALIZATION_LAG
    wide_g1 = await _drive_sqlite_replay(wide_path, wide_scope, as_of_g1, (10,), monkeypatch)
    split_g1 = await _drive_sqlite_replay(split_path, split_scope, as_of_g1, (2, 1), monkeypatch)
    assert wide_g1.has_fill_gap is True and split_g1.has_fill_gap is True
    assert wide_g1.repair_from == _M1 and split_g1.repair_from == _M1
    assert replace(split_g1, revision=wide_g1.revision) == wide_g1

    pause_1, pause_2 = as_of_g1 + _MINUTE, as_of_g1 + 2 * _MINUTE
    await _run_paused_ticks(wide_path, as_of_g1, pause_1, caplog)
    await _run_paused_ticks(split_path, pause_1, pause_2, caplog)
    assert len(_skip_logs(caplog)) == 2

    for path, scope in ((wide_path, wide_scope), (split_path, split_scope)):
        frontier, persisted_rows, reconciled_rows = await _read_persisted_and_reconciled(
            path, scope
        )
        assert frontier.tip == _M3 and persisted_rows == [] and reconciled_rows == []

    await _append_matched_fill_three(wide_path)
    await _append_matched_fill_three(split_path)
    as_of_g2 = _M5 + pnl_snapshotter.FINALIZATION_LAG
    wide_g2 = await _drive_sqlite_replay(wide_path, wide_scope, as_of_g2, (10,), monkeypatch)
    split_g2 = await _drive_sqlite_replay(split_path, split_scope, as_of_g2, (2, 1, 3), monkeypatch)
    assert wide_g2.generation == wide_g1.generation + 1
    assert split_g2.generation == split_g1.generation + 1
    assert wide_g2.raw_execution_watermarks == {"kraken": 2}
    assert replace(split_g2, revision=wide_g2.revision) == wide_g2

    await _match_missing_execution_two(wide_path)
    await _match_missing_execution_two(split_path)
    as_of_final = _M6 + pnl_snapshotter.FINALIZATION_LAG
    await _drive_sqlite_replay(wide_path, wide_scope, as_of_final, (10,), monkeypatch)
    await _drive_sqlite_replay(split_path, split_scope, as_of_final, (2, 1, 4), monkeypatch)

    wide_done, wide_rows, wide_published = await _read_persisted_and_reconciled(
        wide_path, wide_scope
    )
    split_done, split_rows, split_published = await _read_persisted_and_reconciled(
        split_path, split_scope
    )

    assert wide_done.next_minute is None and wide_done.repair_from is None
    assert wide_done.has_fill_gap is False and wide_done.tip == _M6
    assert replace(split_done, revision=wide_done.revision) == wide_done
    assert wide_done.revision != split_done.revision
    assert [row["point_time"] for row in wide_rows] == [_M1, _M2, _M3, _M4, _M5, _M6]
    assert [row["valuation_status"] for row in wide_rows] == ["complete"] * 6
    assert restart._economics(split_rows) == restart._economics(wide_rows)
    assert restart._economics(split_published) == restart._economics(wide_published)
    assert restart._economics(split_published) == restart._economics(split_rows)
