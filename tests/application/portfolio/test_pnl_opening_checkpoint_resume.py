"""End-to-end checkpoint suffix resume, bit identity, and late-fill fallback."""

from collections.abc import Iterator
from collections.abc import Sequence
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from typing import cast
from unittest.mock import AsyncMock

import pytest

from snapper.application.portfolio import pnl_engine_options
from snapper.application.portfolio import pnl_opening_checkpoint_verifier as verifier_service
from snapper.application.portfolio import pnl_timeline_service as service
from snapper.application.portfolio.execution_chain import ExecutionChainError
from snapper.application.portfolio.fx_conversion_shadow import FxShadowPinContext
from snapper.application.portfolio.fx_conversion_shadow import activate_fx_shadow_context
from snapper.application.portfolio.pnl_engine_options import PnlEngineOptions
from snapper.application.portfolio.pnl_fx_replay_state import _FxInstrumentRequirements
from snapper.application.portfolio.pnl_opening_checkpoint_verifier import PnlCheckpointResume
from snapper.application.portfolio.pnl_opening_checkpoint_verifier import (
    verify_checkpoint_for_resume,
)
from snapper.application.portfolio.pnl_timeline import PnlTimelineWorkBudgetError
from snapper.application.portfolio.pnl_timeline import TimelineOpening
from snapper.application.portfolio.pnl_timeline_service import PnlFrozenReplayEvidenceChangedError
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReadPolicy
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReplayOptions
from snapper.application.portfolio.pnl_timeline_service import PnlWalletSeriesResult
from snapper.application.portfolio.pnl_timeline_service import _execution_manifest_digest
from snapper.application.portfolio.pnl_timeline_service import build_wallet_pnl_series
from snapper.data.pnl_correction_state import PnlOpeningCheckpointRecord
from snapper.data.repository import Repository
from snapper.data.repository_types import PnlFxRatePlane
from snapper.data.repository_types import PnlFxRateRow
from snapper.data.repository_types import PnlTimelineAccrualRow
from snapper.data.repository_types import PnlTimelineAppliedAnnulment
from snapper.data.repository_types import PnlTimelineExecutionSuffix
from snapper.data.repository_types import PnlTimelineExecutionSuffixRequest
from snapper.data.repository_types import PnlTimelineOpeningExecutionRow
from tests.application.portfolio import test_pnl_opening_checkpoint as checkpoint_fixtures
from tests.application.portfolio import test_pnl_opening_checkpoint_capture as capture
from tests.application.portfolio import test_pnl_opening_checkpoint_verifier as verifier
from tests.application.portfolio import test_pnl_timeline_service as fixtures

_C = fixtures._m(4)
_T = fixtures._m(8)


class _ResumeRepo(capture._CaptureRepo):
    """Capture fake with suffix-only resume reads and accrual-bound spies."""

    def __init__(self, *, late: str | None = None) -> None:
        """Install fills, EUR fees, an accrual, and deferred fills crossing C.

        Args:
            late: ``"clamped"`` for a pre-C row or ``"stamped"`` for a t0 row.
        """
        execution_rows = [
            fixtures._exec_row(fixtures._I1, 1, 1, "buy", 2.0, 100.0, 0.0, "USD"),
            fixtures._exec_row(fixtures._I1, 2, 5, "sell", 1.0, 105.0, 1.0, "EUR"),
            fixtures._exec_row(fixtures._I1, 3, 4, "buy", 1.0, 104.0, 0.0, "USD"),
            fixtures._exec_row(
                fixtures._I2,
                4,
                3 if late == "clamped" else 0 if late == "stamped" else 7,
                "buy",
                1.0,
                50.0,
                0.0,
                "USD",
            ),
        ]
        if late == "stamped":
            execution_rows[-1]["timestamp"] = fixtures._EMPTY_ANCHOR_T0
        super().__init__()
        self._executions = execution_rows
        self._accruals = [fixtures._accrual_row(fixtures._I1, 6, 2.0, "EUR")]
        self._refs = [
            fixtures._ref(fixtures._I1, "BTC-USD", "USD"),
            fixtures._ref(fixtures._I2, "ETH-USD", "USD"),
        ]
        self._candles = [
            fixtures._candle(fixtures._m(minute), 101.0, instrument)
            for minute in range(9)
            for instrument in (fixtures._I1, fixtures._I2)
        ]
        self._lineage = [
            fixtures._lineage_row(row["order_public_id"], "rest") for row in execution_rows
        ]
        self._fx_rows = [fixtures._fx_row("EUR", "USD", minute, 2.0) for minute in (5, 6, 7)]
        self._execution_watermarks = {"kraken": 3}
        self.suffix_calls = 0
        self.suffix_requests: list[PnlTimelineExecutionSuffixRequest] = []
        self.resume_accrual_bounds: list[tuple[datetime | None, datetime | None]] = []
        self.forbid_prefix = False

    async def get_pnl_timeline_execution_suffix_at_watermarks(
        self,
        request: PnlTimelineExecutionSuffixRequest,
    ) -> PnlTimelineExecutionSuffix:
        """Return only requested suffix and deferred rows without a prefix read.

        Args:
            request: Exact cutoff, generation, deferred coordinates, and horizon.

        Returns:
            Filtered canned effective execution rows and suffix annulments.
        """
        self.suffix_calls += 1
        self.suffix_requests.append(request)
        deferred = {
            (exchange, sequence)
            for exchange, sequences in request.deferred_execution_sequences.items()
            for sequence in sequences
        }
        executions = [
            row
            for row in self._executions
            if (
                request.cutoff_watermarks.get(row["exchange"], 0)
                < row["scope_sequence"]
                <= request.generation_watermarks.get(row["exchange"], 0)
            )
            or (row["exchange"], row["scope_sequence"]) in deferred
        ]
        annulments = [
            row
            for row in self._applied_annulments
            if (
                request.cutoff_watermarks.get(row["exchange"], 0)
                < row["scope_sequence"]
                <= request.generation_watermarks.get(row["exchange"], 0)
            )
            or (row["exchange"], row["scope_sequence"]) in deferred
        ]
        return PnlTimelineExecutionSuffix(executions=executions, annulments=annulments)

    async def get_accruals_for_pnl(
        self,
        wallet_public_id: str,
        mode: str,
        as_of: datetime,
        *,
        after_exclusive: datetime | None = None,
        through: datetime | None = None,
    ) -> list[PnlTimelineAccrualRow]:
        """Record exact accrual bounds before applying the canned filter.

        Args:
            wallet_public_id: Wallet scope.
            mode: Trading mode.
            as_of: Knowledge horizon.
            after_exclusive: Exclusive lower accrual boundary.
            through: Inclusive upper accrual boundary.

        Returns:
            Bounded canned accrual rows.
        """
        if self.forbid_prefix:
            self.resume_accrual_bounds.append((after_exclusive, through))
        return await super().get_accruals_for_pnl(
            wallet_public_id,
            mode,
            as_of,
            after_exclusive=after_exclusive,
            through=through,
        )

    def _assert_no_prefix_read(self) -> None:
        """Assert that resume never invoked either full-prefix repository surface."""
        assert self.calls.count("execution_prefix") == 0


def _resume_options(
    resume: PnlCheckpointResume | None,
) -> PnlSeriesReplayOptions:
    """Build exact generation replay options with optional verified resume state.

    Args:
        resume: Verified checkpoint resume state or ``None`` for anchor replay.

    Returns:
        Replay options using the generation watermark at T.
    """
    return PnlSeriesReplayOptions(
        frozen_execution_watermarks={"kraken": 4},
        frozen_execution_manifest_digest=capture._MANIFEST,
        frozen_has_fill_gap=False,
        frozen_fill_witness_cut=capture._WITNESS,
        replay_generation_tip=_T,
        pointwise_exact_fx=True,
        checkpoint_resume=resume,
    )


async def _read_window(
    repo: _ResumeRepo,
    resume: PnlCheckpointResume | None,
) -> PnlWalletSeriesResult:
    """Read the suffix window under anchor or resumed replay.

    Args:
        repo: Spy repository containing the complete constellation.
        resume: Verified resume state or ``None``.

    Returns:
        The completed wallet series result.
    """
    if resume is None:
        repo._execution_watermarks = {"kraken": 4}
    return await build_wallet_pnl_series(
        repo,
        fixtures._W1,
        "live",
        fixtures._m(5),
        _T,
        "1m",
        fixtures._m(10),
        "USD",
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=_resume_options(resume),
    )


async def _verified_resume(
    monkeypatch: pytest.MonkeyPatch,
    repo: _ResumeRepo,
) -> PnlCheckpointResume:
    """Capture at C and verify the persisted-shaped record at the resume horizon.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        repo: Populated capture and verification fake.

    Returns:
        Verified decoded resume state.
    """
    capture._permit_capture(monkeypatch)
    request = replace(capture._request(), raw_execution_watermarks={"kraken": 3})
    capture_options = replace(capture._options(request), replay_generation_tip=_C)
    capture_result = await build_wallet_pnl_series(
        repo,
        fixtures._W1,
        "live",
        fixtures._m(0),
        _C,
        "1m",
        capture._AS_OF,
        "USD",
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=capture_options,
    )
    assert capture_result.checkpoint_write is not None
    write = capture_result.checkpoint_write
    record = PnlOpeningCheckpointRecord(write.certificate, write.state_json, fixtures._m(10))
    eligibility = replace(
        verifier._eligibility(request),
        from_time=fixtures._m(5),
        frontier_tip=_T,
        replay_generation_tip=_T,
    )
    outcome = await verify_checkpoint_for_resume(
        repo,
        record,
        verifier._scope(request),
        fixtures._m(10),
        eligibility,
    )
    assert isinstance(outcome, PnlCheckpointResume)
    repo._execution_watermarks = {"kraken": 4}
    repo.calls.clear()
    repo.forbid_prefix = True
    return outcome


async def test_resume_suffix_is_bit_identical_to_anchor_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Resume fills, EUR fees, accruals, and deferred rows without prefix reads.

    Given: A capture at C with deferred fills and a verified generation at T.
    When: The suffix resumes and the anchor replays the same window.
    Then: Points and reasons are bit-identical, only suffix execution reads occur,
        and accrual SQL excludes every row at or before C.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    repo = _ResumeRepo()
    resume = await _verified_resume(monkeypatch, repo)
    resumed = await _read_window(repo, resume)
    repo._assert_no_prefix_read()
    assert repo.suffix_calls == 1
    assert repo.resume_accrual_bounds == [(_C, _T)]
    anchor = await _read_window(_ResumeRepo(), None)
    assert resumed.checkpoint_resume_fallback is None
    checkpoint_fixtures._assert_points_bit_identical(anchor.points, resumed.points)


@contextmanager
def _conversion_only_event_fx(
    monkeypatch: pytest.MonkeyPatch,
    instrument_public_id: str,
    minute: datetime,
) -> Iterator[None]:
    """Temporarily inject one stateless conversion-only minute into event FX requirements.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        instrument_public_id: Instrument credited with the synthetic conversion minute.
        minute: Minute to add for the ``("EUR", "USD")`` pair.

    Yields:
        Control while ``service._event_fx_minutes`` includes the synthetic minute.
    """
    original_event_fx = service._event_fx_minutes

    def event_fx_with_conversion_only(
        execution_rows: Sequence[PnlTimelineOpeningExecutionRow],
        accrual_rows: Sequence[PnlTimelineAccrualRow],
        context: service._EventFxContext,
        opening: TimelineOpening | None = None,
    ) -> _FxInstrumentRequirements:
        """Augment causal event requirements with one stateless conversion minute.

        Args:
            execution_rows: Trusted replayed executions.
            accrual_rows: Windowed accruals.
            context: Proven denominations, valuation currency, and conversion flag.
            opening: Optional opening state.

        Returns:
            Event requirements including the synthetic conversion minute.
        """
        reqs = original_event_fx(
            execution_rows,
            accrual_rows,
            context,
            opening,
        )
        reqs.setdefault(instrument_public_id, {}).setdefault(("EUR", "USD"), set()).add(minute)
        return reqs

    monkeypatch.setattr(service, "_event_fx_minutes", event_fx_with_conversion_only)
    try:
        yield
    finally:
        monkeypatch.setattr(service, "_event_fx_minutes", original_event_fx)


@pytest.mark.parametrize("late_kind", ["clamped", "stamped"])
async def test_suffix_row_clamped_to_checkpoint_falls_back_softly(
    monkeypatch: pytest.MonkeyPatch,
    late_kind: str,
) -> None:
    """Return the late-execution fallback signal instead of raising.

    Given: A verified checkpoint and a new-pool suffix row stamped before C.
    When: Resume detects the row's clamped effective time is not after C.
    Then: The result carries ``checkpoint_late_execution`` and anchor-replay points.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        late_kind: Whether the row clamps to C or is stamped at t0.
    """
    repo = _ResumeRepo(late=late_kind)
    resume = await _verified_resume(monkeypatch, repo)
    resumed = await _read_window(repo, resume)
    assert resumed.checkpoint_resume_fallback == "checkpoint_late_execution"
    anchor = await _read_window(_ResumeRepo(late=late_kind), None)
    checkpoint_fixtures._assert_points_bit_identical(anchor.points, resumed.points)


def _eur_prefix_only_repo() -> _ResumeRepo:
    """Build a repo with one EUR-quoted fill in (t0, C] and no suffix events.

    Returns:
        A fake repository holding one EUR-quoted instrument active through T.
    """
    repo = _ResumeRepo()
    repo._executions = [
        fixtures._exec_row(fixtures._I1, 1, 1, "buy", 2.0, 100.0, 0.0, "USD"),
    ]
    repo._accruals = []
    repo._refs = [fixtures._ref(fixtures._I1, "BTC-EUR", "EUR")]
    repo._candles = [
        fixtures._candle(fixtures._m(minute), 100.0 + minute, fixtures._I1) for minute in range(9)
    ]
    repo._lineage = [fixtures._lineage_row(f"order-{fixtures._I1}-1", "rest")]
    repo._fx_rows = [fixtures._fx_row("EUR", "USD", minute, 1.5) for minute in range(1, 10)]
    repo._execution_watermarks = {"kraken": 1}
    return repo


async def _capture_and_verify_custom(
    monkeypatch: pytest.MonkeyPatch,
    repo: _ResumeRepo,
    cutoff_watermarks: dict[str, int],
    *,
    manifest_digest: str = capture._MANIFEST,
    anchor_t0: datetime = fixtures._EMPTY_ANCHOR_T0,
) -> PnlCheckpointResume:
    """Capture at C and verify at the resuming horizon for custom constellations.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        repo: Configured fake repository.
        cutoff_watermarks: Raw execution cut captured at C.
        manifest_digest: Expected execution manifest digest at capture.
        anchor_t0: Activation anchor minute for the verification scope.

    Returns:
        Verified checkpoint resume state with prefix reads reset.
    """
    capture._permit_capture(monkeypatch)
    request = replace(
        capture._request(),
        raw_execution_watermarks=cutoff_watermarks,
        execution_manifest_digest=manifest_digest,
    )
    capture_options = replace(
        capture._options(request),
        frozen_execution_manifest_digest=manifest_digest,
        replay_generation_tip=_C,
    )
    capture_result = await build_wallet_pnl_series(
        repo,
        fixtures._W1,
        "live",
        fixtures._m(0),
        _C,
        "1m",
        capture._AS_OF,
        "USD",
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=capture_options,
    )
    assert capture_result.checkpoint_write is not None
    write = capture_result.checkpoint_write
    record = PnlOpeningCheckpointRecord(write.certificate, write.state_json, fixtures._m(10))
    scope = replace(verifier._scope(request), anchor_point_time=anchor_t0)
    eligibility = replace(
        verifier._eligibility(request),
        from_time=fixtures._m(5),
        frontier_tip=_T,
        replay_generation_tip=_T,
    )
    outcome = await verify_checkpoint_for_resume(
        repo,
        record,
        scope,
        fixtures._m(10),
        eligibility,
    )
    assert isinstance(outcome, PnlCheckpointResume)
    repo.calls.clear()
    repo.symbol_ref_calls.clear()
    repo.forbid_prefix = True
    return outcome


async def test_resume_preserves_eur_instrument_acquired_in_prefix_with_no_suffix_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retain marks, FX, display identity, and budget for prefix-only instruments.

    Given: An EUR-quoted instrument acquired in (t0, C] with zero executions or
        accruals in (C, T].
    When: The suffix window is built via checkpoint resume and via anchor replay.
    Then: Points, display identities, and rate sources match anchor replay bit
        for bit, and the instrument is included in symbol-ref loading.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    repo = _eur_prefix_only_repo()
    resume = await _capture_and_verify_custom(monkeypatch, repo, {"kraken": 1})
    options = replace(
        _resume_options(resume),
        frozen_execution_watermarks={"kraken": 1},
    )
    resumed = await build_wallet_pnl_series(
        repo,
        fixtures._W1,
        "live",
        fixtures._m(5),
        _T,
        "1m",
        fixtures._m(10),
        "USD",
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=options,
    )
    anchor_repo = _eur_prefix_only_repo()
    anchor = await build_wallet_pnl_series(
        anchor_repo,
        fixtures._W1,
        "live",
        fixtures._m(5),
        _T,
        "1m",
        fixtures._m(10),
        "USD",
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=replace(options, checkpoint_resume=None),
    )
    repo._assert_no_prefix_read()
    assert fixtures._I1 in repo.symbol_ref_calls[-1]
    assert resumed.rate_sources == anchor.rate_sources
    checkpoint_fixtures._assert_points_bit_identical(anchor.points, resumed.points)


async def test_resume_work_budget_counts_carried_pool_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Count checkpoint-carried pools in the series work budget on resume.

    Given: A verified checkpoint carrying two active pools with zero suffix events.
    When: The maximum work-unit budget is 4 (enough for 1 pool over 4 minutes,
        too small for 2 pools over 4 minutes).
    Then: Checkpoint resume raises PnlTimelineWorkBudgetError just like anchor replay.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    repo = _eur_prefix_only_repo()
    repo._executions.append(
        fixtures._exec_row(fixtures._I2, 2, 2, "buy", 1.0, 50.0, 0.0, "USD"),
    )
    repo._refs.append(fixtures._ref(fixtures._I2, "ETH-USD", "USD"))
    repo._candles.extend(
        fixtures._candle(fixtures._m(minute), 50.0, fixtures._I2) for minute in range(9)
    )
    repo._lineage.append(fixtures._lineage_row(f"order-{fixtures._I2}-2", "rest"))
    repo._execution_watermarks = {"kraken": 2}
    resume = await _capture_and_verify_custom(monkeypatch, repo, {"kraken": 2})
    monkeypatch.setattr(service, "PNL_TIMELINE_MAX_WORK_UNITS", 4)
    options = replace(
        _resume_options(resume),
        frozen_execution_watermarks={"kraken": 2},
    )
    with pytest.raises(PnlTimelineWorkBudgetError):
        await build_wallet_pnl_series(
            repo,
            fixtures._W1,
            "live",
            fixtures._m(5),
            _T,
            "1m",
            fixtures._m(10),
            "USD",
            policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
            options=options,
        )


def _expiring_opening_ref_repo() -> _ResumeRepo:
    """Build a repo whose opening pool's symbol ref expires between C and T.

    Returns:
        A fake repository with one anchor opening pool and a ref ending at C + 1m.
    """
    repo = _ResumeRepo()
    repo._anchor = fixtures._one_pool_anchor()
    repo._executions = []
    repo._accruals = []
    repo._refs = [
        fixtures._ref(
            fixtures._I1,
            "BTC-USD",
            "USD",
            valid_from=fixtures._m(0),
            valid_to=_C + timedelta(minutes=1),
        )
    ]
    repo._candles = [
        fixtures._candle(fixtures._m(minute), 120.0, fixtures._I1) for minute in range(9)
    ]
    repo._lineage = []
    repo._fx_rows = []
    repo._execution_watermarks = {}
    return repo


async def test_resume_opening_symbol_ref_continuity_matches_anchor_over_reference_tip(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Evaluate opening symbol-ref continuity through reference_tip on resume.

    Given: An anchor opening pool whose symbol ref covers [t0, C] but expires
        before reference_tip T.
    When: The suffix window is built via checkpoint resume and via anchor replay.
    Then: Both produce bit-identical points with identical ref-continuity rulings.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    repo = _expiring_opening_ref_repo()
    resume = await _capture_and_verify_custom(
        monkeypatch,
        repo,
        {},
        anchor_t0=fixtures._m(1),
    )
    options = replace(
        _resume_options(resume),
        frozen_execution_watermarks={},
    )
    resumed = await build_wallet_pnl_series(
        repo,
        fixtures._W1,
        "live",
        fixtures._m(5),
        _T,
        "1m",
        fixtures._m(10),
        "USD",
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=options,
    )
    anchor_repo = _expiring_opening_ref_repo()
    anchor = await build_wallet_pnl_series(
        anchor_repo,
        fixtures._W1,
        "live",
        fixtures._m(5),
        _T,
        "1m",
        fixtures._m(10),
        "USD",
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=replace(options, checkpoint_resume=None),
    )
    checkpoint_fixtures._assert_points_bit_identical(anchor.points, resumed.points)


def _two_exchange_annulment_repo() -> _ResumeRepo:
    """Build a repo where kraken has a prefix fill and annulment but no suffix fills.

    Returns:
        A fake repository with two exchanges and one prefix annulment on kraken.
    """
    repo = _ResumeRepo()
    repo._executions = [
        fixtures._exec_row(fixtures._I1, 1, 1, "buy", 1.0, 100.0, 0.0, "USD", exchange="kraken"),
        fixtures._exec_row(fixtures._I2, 1, 2, "buy", 1.0, 50.0, 0.0, "USD", exchange="coinbase"),
        fixtures._exec_row(fixtures._I2, 2, 6, "buy", 1.0, 55.0, 0.0, "USD", exchange="coinbase"),
    ]
    repo._accruals = []
    repo._refs = [
        fixtures._ref(fixtures._I1, "BTC-USD", "USD", exchange="kraken"),
        fixtures._ref(fixtures._I2, "ETH-USD", "USD", exchange="coinbase"),
    ]
    repo._candles = [
        fixtures._candle(fixtures._m(minute), 105.0, instrument)
        for minute in range(9)
        for instrument in (fixtures._I1, fixtures._I2)
    ]
    repo._lineage = [
        fixtures._lineage_row(row["order_public_id"], "rest") for row in repo._executions
    ]
    repo._fx_rows = []
    repo._execution_watermarks = {"kraken": 2, "coinbase": 1}
    prefix_annulment = fixtures._applied_annulment(scope_sequence=2)
    suffix_annulment: PnlTimelineAppliedAnnulment = {
        "public_id": "00000000-0000-7000-8000-0000000000da",
        "target_execution_public_id": "00000000-0000-7000-8000-0000000000e2",
        "target_execution_digest": prefix_annulment["target_execution_digest"],
        "exchange": "coinbase",
        "scope_sequence": 3,
        "reason": "unwitnessed_phantom",
        "correction_time": prefix_annulment["correction_time"],
    }
    repo.apply_annulments([suffix_annulment, prefix_annulment])
    return repo


async def test_resumed_replay_metadata_and_applied_annulments_match_anchor_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Carry prefix watermarks and annulments on resume when an exchange is quiet.

    Given: Two exchanges where kraken has prefix activity and a prefix annulment
        but zero suffix executions, while coinbase has a suffix execution and a
        suffix annulment.
    When: Both anchor replay and checkpoint resume run with baseline watermarks.
    Then: sample_watermarks_by_minute, max_scope_sequence_by_exchange,
        execution_manifest_digest, and applied_annulments match anchor replay.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    repo = _two_exchange_annulment_repo()
    suffix_annulment, prefix_annulment = repo._applied_annulments
    repo.apply_annulments([prefix_annulment])
    capture_manifest = _execution_manifest_digest([prefix_annulment])
    resume = await _capture_and_verify_custom(
        monkeypatch,
        repo,
        {"kraken": 2, "coinbase": 1},
        manifest_digest=capture_manifest,
    )
    repo.apply_annulments([suffix_annulment, prefix_annulment])
    full_manifest = _execution_manifest_digest([suffix_annulment, prefix_annulment])
    repo._execution_watermarks = {"kraken": 2, "coinbase": 3}
    options = PnlSeriesReplayOptions(
        baseline_watermarks={"kraken": 1, "coinbase": 1},
        frozen_execution_watermarks={"kraken": 2, "coinbase": 3},
        frozen_execution_manifest_digest=full_manifest,
        frozen_has_fill_gap=False,
        frozen_fill_witness_cut=capture._WITNESS,
        replay_generation_tip=_T,
        pointwise_exact_fx=True,
        checkpoint_resume=resume,
    )
    resumed = await build_wallet_pnl_series(
        repo,
        fixtures._W1,
        "live",
        fixtures._m(5),
        _T,
        "1m",
        fixtures._m(10),
        "USD",
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=options,
    )
    anchor_repo = _two_exchange_annulment_repo()
    anchor_repo._execution_watermarks = {"kraken": 2, "coinbase": 3}
    anchor = await build_wallet_pnl_series(
        anchor_repo,
        fixtures._W1,
        "live",
        fixtures._m(5),
        _T,
        "1m",
        fixtures._m(10),
        "USD",
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=replace(options, checkpoint_resume=None),
    )
    assert resumed.replay_metadata is not None
    assert anchor.replay_metadata is not None
    assert (
        resumed.replay_metadata.sample_watermarks_by_minute
        == anchor.replay_metadata.sample_watermarks_by_minute
    )
    assert (
        resumed.replay_metadata.max_scope_sequence_by_exchange
        == anchor.replay_metadata.max_scope_sequence_by_exchange
    )
    assert (
        resumed.replay_metadata.execution_manifest_digest
        == anchor.replay_metadata.execution_manifest_digest
    )
    assert resumed.applied_annulments == anchor.applied_annulments
    assert len(resumed.applied_annulments) == 2
    checkpoint_fixtures._assert_points_bit_identical(anchor.points, resumed.points)


async def test_resume_raises_when_frozen_execution_manifest_digest_changed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fail closed on resume when the generation annulment manifest drifts.

    Given: A verified checkpoint resume and a frozen manifest digest that does
        not match the merged prefix-plus-suffix applied annulments.
    When: build_wallet_pnl_series loads suffix resume inputs.
    Then: It raises PnlFrozenReplayEvidenceChangedError.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    repo = _ResumeRepo()
    resume = await _verified_resume(monkeypatch, repo)
    options = replace(
        _resume_options(resume),
        frozen_execution_manifest_digest="0" * 64,
    )
    with pytest.raises(PnlFrozenReplayEvidenceChangedError, match="manifest changed"):
        await build_wallet_pnl_series(
            repo,
            fixtures._W1,
            "live",
            fixtures._m(5),
            _T,
            "1m",
            fixtures._m(10),
            "USD",
            policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
            options=options,
        )


async def test_checkpoint_resume_without_frozen_watermarks_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject checkpoint resume when frozen_execution_watermarks is omitted.

    Given: A verified checkpoint resume state.
    When: build_wallet_pnl_series is called without frozen_execution_watermarks.
    Then: Policy validation raises ValueError before reading suffix evidence.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    repo = _ResumeRepo()
    resume = await _verified_resume(monkeypatch, repo)
    with pytest.raises(ValueError, match="checkpoint resume"):
        await build_wallet_pnl_series(
            repo,
            fixtures._W1,
            "live",
            fixtures._m(5),
            _T,
            "1m",
            fixtures._m(10),
            "USD",
            policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
            options=PnlSeriesReplayOptions(
                pointwise_exact_fx=True,
                checkpoint_resume=resume,
            ),
        )


@pytest.mark.parametrize(
    ("pointwise_exact_fx", "carry_valuation"),
    [(False, False), (True, True), (False, True)],
)
async def test_checkpoint_resume_with_ineligible_read_profile_falls_back_to_anchor(
    monkeypatch: pytest.MonkeyPatch,
    pointwise_exact_fx: bool,
    carry_valuation: bool,
) -> None:
    """Refuse direct resume whenever read profile is not pointwise exact without carry.

    Given: A verified checkpoint resume state and frozen generation watermarks.
    When: ``build_wallet_pnl_series`` runs with ``pointwise_exact_fx=False`` or
        ``carry_valuation=True``.
    Then: Suffix reads are skipped, ``checkpoint_resume_fallback`` is
        ``"checkpoint_ineligible"``, and points equal the anchor replay under that profile.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        pointwise_exact_fx: Pointwise exact FX flag on replay options.
        carry_valuation: Carry valuation flag on the series read policy.
    """
    repo = _ResumeRepo()
    resume = await _verified_resume(monkeypatch, repo)
    policy = PnlSeriesReadPolicy(allow_anchor_creation=False, carry_valuation=carry_valuation)
    options = replace(_resume_options(resume), pointwise_exact_fx=pointwise_exact_fx)
    resumed = await build_wallet_pnl_series(
        repo,
        fixtures._W1,
        "live",
        fixtures._m(5),
        _T,
        "1m",
        fixtures._m(10),
        "USD",
        policy=policy,
        options=options,
    )
    assert repo.suffix_calls == 0
    assert resumed.checkpoint_resume_fallback == "checkpoint_ineligible"
    anchor_repo = _ResumeRepo()
    anchor_repo._execution_watermarks = {"kraken": 4}
    anchor = await build_wallet_pnl_series(
        anchor_repo,
        fixtures._W1,
        "live",
        fixtures._m(5),
        _T,
        "1m",
        fixtures._m(10),
        "USD",
        policy=policy,
        options=replace(options, checkpoint_resume=None),
    )
    checkpoint_fixtures._assert_points_bit_identical(anchor.points, resumed.points)


async def test_checkpoint_resume_with_previous_replay_as_of_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject checkpoint resume when previous_replay_as_of is set.

    Given: A verified checkpoint resume state and frozen generation watermarks.
    When: ``build_wallet_pnl_series`` is called with ``previous_replay_as_of`` set.
    Then: ``_validate_checkpoint_resume_policy`` raises ``ValueError`` before
        any suffix read runs.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    repo = _ResumeRepo()
    resume = await _verified_resume(monkeypatch, repo)
    options = replace(_resume_options(resume), previous_replay_as_of=fixtures._m(9))
    with pytest.raises(ValueError, match="checkpoint resume does not permit previous_replay_as_of"):
        await build_wallet_pnl_series(
            repo,
            fixtures._W1,
            "live",
            fixtures._m(5),
            _T,
            "1m",
            fixtures._m(10),
            "USD",
            policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
            options=options,
        )


async def test_checkpoint_resume_with_metadata_only_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject checkpoint resume when metadata_only is true.

    Given: A verified checkpoint resume state and frozen generation watermarks.
    When: ``build_wallet_pnl_series`` is called with ``metadata_only=True``.
    Then: ``_validate_checkpoint_resume_policy`` raises ``ValueError`` before
        any suffix read runs.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    repo = _ResumeRepo()
    resume = await _verified_resume(monkeypatch, repo)
    options = replace(_resume_options(resume), metadata_only=True)
    with pytest.raises(ValueError, match="checkpoint resume does not permit metadata_only"):
        await build_wallet_pnl_series(
            repo,
            fixtures._W1,
            "live",
            fixtures._m(5),
            _T,
            "1m",
            fixtures._m(10),
            "USD",
            policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
            options=options,
        )


async def test_resume_forwards_prefix_has_idless_executions_to_suffix_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Forward certificate.prefix_has_idless_executions into the suffix request.

    Given: A checkpoint captured over a prefix containing an ID-less execution.
    When: The suffix window resumes from that verified checkpoint.
    Then: The suffix request carries prefix_has_idless_executions=True.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    repo = _ResumeRepo()
    repo._executions[0]["exec_id"] = None
    repo._executions[0]["trade_id"] = None
    resume = await _verified_resume(monkeypatch, repo)
    assert resume.record.certificate.prefix_has_idless_executions is True
    await _read_window(repo, resume)
    assert len(repo.suffix_requests) == 1
    assert getattr(repo.suffix_requests[0], "prefix_has_idless_executions", False) is True


async def test_resume_threads_frozen_fill_witness_cut_into_suffix_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Thread options.frozen_fill_witness_cut into the suffix repository request.

    Given: A verified checkpoint resume with a frozen generation fill-witness cut.
    When: ``build_wallet_pnl_series`` loads the suffix window.
    Then: The suffix request receives that exact ``fill_witness_cut``.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    repo = _ResumeRepo()
    resume = await _verified_resume(monkeypatch, repo)
    await _read_window(repo, resume)
    assert len(repo.suffix_requests) == 1
    assert getattr(repo.suffix_requests[0], "fill_witness_cut", None) == capture._WITNESS


async def test_resume_wraps_suffix_execution_chain_error_as_frozen_evidence_changed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Wrap ExecutionChainError from suffix loading as PnlFrozenReplayEvidenceChangedError.

    Given: A verified checkpoint resume whose suffix execution read raises
        ``ExecutionChainError``.
    When: ``build_wallet_pnl_series`` resumes the suffix window.
    Then: It raises ``PnlFrozenReplayEvidenceChangedError`` chaining the
        underlying ``ExecutionChainError``.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    repo = _ResumeRepo()
    resume = await _verified_resume(monkeypatch, repo)
    monkeypatch.setattr(
        repo,
        "get_pnl_timeline_execution_suffix_at_watermarks",
        AsyncMock(side_effect=ExecutionChainError("missing_execution_suffix_row")),
    )
    with pytest.raises(
        PnlFrozenReplayEvidenceChangedError, match="could not be recertified"
    ) as err:
        await _read_window(repo, resume)
    assert isinstance(err.value.__cause__, ExecutionChainError)


def _permit_profile(
    monkeypatch: pytest.MonkeyPatch,
    engine_options: PnlEngineOptions,
) -> None:
    """Permit capture under a specific ``PnlEngineOptions`` seam profile.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        engine_options: Candidate calculation-version flags to install.
    """
    monkeypatch.setattr(
        pnl_engine_options,
        "_engine_options_table",
        lambda: {
            capture._CAPTURE_VERSION: engine_options,
            fixtures.PNL_SAMPLE_CALC_VERSION: engine_options,
        },
    )


async def _capture_and_verify_profile(
    monkeypatch: pytest.MonkeyPatch,
    repo: _ResumeRepo,
    cutoff_watermarks: dict[str, int],
    engine_options: PnlEngineOptions,
) -> PnlCheckpointResume:
    """Capture at C and verify at T under the supplied engine-options seam.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        repo: Configured fake repository.
        cutoff_watermarks: Raw execution cut captured at C.
        engine_options: Candidate calculation-version flags.

    Returns:
        Verified checkpoint resume state with prefix reads reset.
    """
    _permit_profile(monkeypatch, engine_options)
    request = replace(capture._request(), raw_execution_watermarks=cutoff_watermarks)
    capture_options = replace(capture._options(request), replay_generation_tip=_C)
    capture_result = await build_wallet_pnl_series(
        cast(Repository, repo),
        fixtures._W1,
        "live",
        fixtures._m(0),
        _C,
        "1m",
        capture._AS_OF,
        "USD",
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=capture_options,
    )
    assert capture_result.checkpoint_write is not None
    write = capture_result.checkpoint_write
    record = PnlOpeningCheckpointRecord(write.certificate, write.state_json, fixtures._m(10))
    eligibility = replace(
        verifier._eligibility(request),
        from_time=fixtures._m(5),
        frontier_tip=_T,
        replay_generation_tip=_T,
    )
    outcome = await verify_checkpoint_for_resume(
        cast(Repository, repo),
        record,
        verifier._scope(request),
        fixtures._m(10),
        eligibility,
    )
    assert isinstance(outcome, PnlCheckpointResume)
    repo.calls.clear()
    repo.symbol_ref_calls.clear()
    repo.forbid_prefix = True
    return outcome


def _span_extension_repo(*, include_suffix: bool) -> _ResumeRepo:
    """Build a prefix fill at C-1 whose unchanged ref expires before suffix fill C+3.

    Args:
        include_suffix: Whether the C+3 suffix fill is present in the ledger cut.

    Returns:
        Configured fake repository for Item 1 capture, verification, and replay.
    """
    repo = _ResumeRepo()
    executions = [
        fixtures._exec_row(fixtures._I1, 1, 3, "buy", 1.0, 100.0, 1.0, "USD"),
    ]
    if include_suffix:
        executions.append(
            fixtures._exec_row(fixtures._I1, 2, 7, "buy", 1.0, 105.0, 0.0, "USD"),
        )
    repo._executions = executions
    repo._accruals = []
    repo._refs = [
        fixtures._ref(
            fixtures._I1,
            "BTC-USD",
            "USD",
            valid_from=fixtures._m(0),
            valid_to=_C + timedelta(minutes=1),
        )
    ]
    repo._candles = [
        fixtures._candle(fixtures._m(minute), 110.0, fixtures._I1) for minute in range(9)
    ]
    repo._lineage = [fixtures._lineage_row(row["order_public_id"], "rest") for row in executions]
    repo._fx_rows = []
    repo._execution_watermarks = {"kraken": len(executions)}
    return repo


@pytest.mark.parametrize(
    "engine_options",
    [
        PnlEngineOptions(True, False, False),
        PnlEngineOptions(True, True, True),
    ],
)
async def test_resume_falls_back_when_suffix_extends_price_ref_span_past_validity(
    monkeypatch: pytest.MonkeyPatch,
    engine_options: PnlEngineOptions,
) -> None:
    """Fall back to anchor replay when suffix rows extend a prefix price span past validity.

    Given: A prefix fill at C-1 covered by an unchanged symbol ref expiring at C+1,
        and a suffix fill at C+3 that extends the required price span past C+1.
    When: Checkpoint capture runs at C, verification passes with unchanged refs,
        and suffix resume runs over (C, T] alongside uninterrupted anchor replay.
    Then: Resume falls back with ``checkpoint_price_provenance_changed`` and emits
        points, reasons, attribution, and rate sources bit-identical to anchor.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        engine_options: Seam profile under test.
    """
    repo = _span_extension_repo(include_suffix=False)
    resume = await _capture_and_verify_profile(monkeypatch, repo, {"kraken": 1}, engine_options)
    full_repo = _span_extension_repo(include_suffix=True)
    repo._executions = list(full_repo._executions)
    repo._lineage = list(full_repo._lineage)
    repo._execution_watermarks = {"kraken": 2}
    options = replace(_resume_options(resume), frozen_execution_watermarks={"kraken": 2})
    resumed = await build_wallet_pnl_series(
        cast(Repository, repo),
        fixtures._W1,
        "live",
        fixtures._m(5),
        _T,
        "1m",
        fixtures._m(10),
        "USD",
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=options,
    )
    anchor = await build_wallet_pnl_series(
        cast(Repository, _span_extension_repo(include_suffix=True)),
        fixtures._W1,
        "live",
        fixtures._m(5),
        _T,
        "1m",
        fixtures._m(10),
        "USD",
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=replace(options, checkpoint_resume=None),
    )
    assert resumed.checkpoint_resume_fallback == "checkpoint_price_provenance_changed"
    assert resumed.rate_sources == anchor.rate_sources
    checkpoint_fixtures._assert_points_bit_identical(anchor.points, resumed.points)


@pytest.mark.parametrize(
    "engine_options",
    [
        PnlEngineOptions(True, False, False),
        PnlEngineOptions(True, True, True),
    ],
)
async def test_resume_merges_carried_prefix_conversion_only_minutes(
    monkeypatch: pytest.MonkeyPatch,
    engine_options: PnlEngineOptions,
) -> None:
    """Carry stateless prefix conversion-only minutes into resumed FX and shadow inputs.

    Given: A capture at C where event requirements include a non-causal prefix
        conversion-only minute (simulating a zero-size, zero-fee foreign fill).
    When: The bundle is verified and suffix resume runs under an FX shadow context.
    Then: The bundle carries the conversion-only minute, resume passes it to FX
        loading and FX shadow inputs, and resumed points match anchor replay.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        engine_options: Seam profile under test.
    """
    repo = _eur_prefix_only_repo()
    repo._executions = [
        fixtures._exec_row(fixtures._I1, 1, 2, "buy", 0.0, 100.0, 0.0, "USD"),
        fixtures._exec_row(fixtures._I1, 2, 3, "buy", 1.0, 100.0, 0.0, "USD"),
    ]
    repo._lineage = [
        fixtures._lineage_row(row["order_public_id"], "rest") for row in repo._executions
    ]
    repo._execution_watermarks = {"kraken": 2}
    with _conversion_only_event_fx(monkeypatch, fixtures._I1, fixtures._m(2)):
        resume = await _capture_and_verify_profile(monkeypatch, repo, {"kraken": 2}, engine_options)
    observed_event_reqs: list[_FxInstrumentRequirements] = []
    original_load_fx = service._load_wallet_series_fx

    async def spy_load_fx(
        repo_arg: Repository,
        request: service._FxRequest,
        options_arg: PnlSeriesReplayOptions,
        policy_arg: PnlSeriesReadPolicy,
    ) -> service._SeriesFxEvidence:
        """Record event requirements passed to FX loading on resume.

        Args:
            repo_arg: Repository instance.
            request: FX requirement request.
            options_arg: Replay options.
            policy_arg: Read policy.

        Returns:
            Loaded FX evidence from the real loader.
        """
        observed_event_reqs.append(
            {
                inst: {pair: set(mins) for pair, mins in pairs.items()}
                for inst, pairs in request.event_requirements.items()
            }
        )
        return await original_load_fx(repo_arg, request, options_arg, policy_arg)

    monkeypatch.setattr(service, "_load_wallet_series_fx", spy_load_fx)
    options = replace(_resume_options(resume), frozen_execution_watermarks={"kraken": 2})
    shadow_ctx = FxShadowPinContext(calculation_version="5C.5", evaluations=[])
    with activate_fx_shadow_context(shadow_ctx):
        resumed = await build_wallet_pnl_series(
            cast(Repository, repo),
            fixtures._W1,
            "live",
            fixtures._m(5),
            _T,
            "1m",
            fixtures._m(10),
            "USD",
            policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
            options=options,
        )
    assert fixtures._m(2) in observed_event_reqs[0].get(fixtures._I1, {}).get(("EUR", "USD"), set())
    assert any(
        ev.scope_kind == "shared_pair"
        and fixtures._m(2) in ev.event_minutes
        and fixtures._m(2) in ev.required_minutes
        for ev in shadow_ctx.evaluations
    )
    assert resumed.checkpoint_resume_fallback is None


def _accrual_before_deferred_first_fill_repo() -> _ResumeRepo:
    """Build a scope with a prefix accrual on EUR-USD before its deferred first fill.

    Returns:
        Fake repository where kraken and walutomat publish differing EUR/USD rates.
    """
    repo = _ResumeRepo()
    executions = [
        fixtures._exec_row(fixtures._I1, 1, 2, "buy", 1.0, 100.0, 1.0, "EUR"),
        fixtures._exec_row(
            fixtures._I2,
            1,
            6,
            "buy",
            1.0,
            1.1,
            0.0,
            "USD",
            exchange="walutomat",
        ),
    ]
    repo._executions = executions
    repo._accruals = [fixtures._accrual_row(fixtures._I2, 3, 2.0, "EUR")]
    repo._refs = [
        fixtures._ref(fixtures._I1, "BTC-USD", "USD", exchange="kraken"),
        fixtures._ref(fixtures._I2, "EUR-USD", "USD", exchange="walutomat"),
    ]
    repo._candles = [
        fixtures._candle(fixtures._m(minute), 100.0, fixtures._I1) for minute in range(9)
    ] + [fixtures._candle(fixtures._m(minute), 1.2, fixtures._I2) for minute in range(9)]
    repo._lineage = [fixtures._lineage_row(row["order_public_id"], "rest") for row in executions]
    repo._fx_rows = [
        fixtures._fx_row("EUR", "USD", 2, 2.0, exchange="kraken"),
        fixtures._fx_row("EUR", "USD", 3, 2.0, exchange="kraken"),
        fixtures._fx_row("EUR", "USD", 3, 3.0, exchange="walutomat"),
        fixtures._fx_row("EUR", "USD", 6, 3.0, exchange="walutomat"),
    ]
    repo._execution_watermarks = {"kraken": 1, "walutomat": 1}
    return repo


@pytest.mark.parametrize(
    "engine_options",
    [
        PnlEngineOptions(True, False, False),
        PnlEngineOptions(True, True, True),
    ],
)
async def test_resume_falls_back_when_prefix_accrual_instrument_gains_identity_plane(
    monkeypatch: pytest.MonkeyPatch,
    engine_options: PnlEngineOptions,
) -> None:
    """Match anchor rate sources and accrual valuation when an instrument gains a canonical plane.

    Given: An EUR-USD instrument on walutomat with a foreign prefix accrual at m(3)
        and a deferred first fill at m(6), while kraken also quotes EUR/USD.
    When: Checkpoint capture runs at C=m(4) using shared kraken for the accrual,
        then resume runs over (C, T] where the deferred fill enters trusted_refs.
    Then: Resume falls back with ``checkpoint_fx_election_changed``, matching anchor's
        canonical rate sources and exact accrual points.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        engine_options: Seam profile under test.
    """
    repo = _accrual_before_deferred_first_fill_repo()
    resume = await _capture_and_verify_profile(
        monkeypatch,
        repo,
        {"kraken": 1, "walutomat": 1},
        engine_options,
    )
    options = replace(
        _resume_options(resume),
        frozen_execution_watermarks={"kraken": 1, "walutomat": 1},
    )
    resumed = await build_wallet_pnl_series(
        cast(Repository, repo),
        fixtures._W1,
        "live",
        fixtures._m(5),
        _T,
        "1m",
        fixtures._m(10),
        "USD",
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=options,
    )
    anchor = await build_wallet_pnl_series(
        cast(Repository, _accrual_before_deferred_first_fill_repo()),
        fixtures._W1,
        "live",
        fixtures._m(5),
        _T,
        "1m",
        fixtures._m(10),
        "USD",
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=replace(options, checkpoint_resume=None),
    )
    assert resumed.checkpoint_resume_fallback == "checkpoint_fx_election_changed"
    assert resumed.rate_sources == anchor.rate_sources
    checkpoint_fixtures._assert_points_bit_identical(anchor.points, resumed.points)


async def test_deferred_first_fill_fee_does_not_leak_shared_venue_on_master(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify 5C.6 capture refuses uncertifiable post-C conversion FX while matching cold read.

    Given: An EUR-USD instrument on walutomat whose only prefix row is a deferred
        first fill at m(6) with a foreign EUR fee, while kraken also quotes EUR/USD.
    When: A cold 5C.6 read and a permitted capture attempt both run at C=m(4).
    Then: The cold 5C.6 read discloses shared kraken from the loaded m(6) row and
        capture returns that exact cold read while refusing with ``capture_value_error``,
        leaving the uninterrupted horizon read on canonical walutomat.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    repo = _accrual_before_deferred_first_fill_repo()
    repo._accruals = []
    repo._executions = [
        fixtures._exec_row(
            fixtures._I2,
            1,
            6,
            "buy",
            1.0,
            1.1,
            1.0,
            "EUR",
            exchange="walutomat",
        ),
    ]
    repo._refs = [fixtures._ref(fixtures._I2, "EUR-USD", "USD", exchange="walutomat")]
    repo._candles = [
        fixtures._candle(fixtures._m(minute), 1.2, fixtures._I2) for minute in range(9)
    ]
    repo._lineage = [
        fixtures._lineage_row(row["order_public_id"], "rest") for row in repo._executions
    ]
    repo._fx_rows = [
        fixtures._fx_row("EUR", "USD", 6, 2.0, exchange="kraken"),
        fixtures._fx_row("EUR", "USD", 6, 3.0, exchange="walutomat"),
    ]
    repo._execution_watermarks = {"walutomat": 1}
    _permit_profile(monkeypatch, PnlEngineOptions(True, True, True))
    request = replace(capture._request(), raw_execution_watermarks={"walutomat": 1})
    capture_options = replace(capture._options(request), replay_generation_tip=_C)
    captured = await build_wallet_pnl_series(
        cast(Repository, repo),
        fixtures._W1,
        "live",
        fixtures._m(0),
        _C,
        "1m",
        capture._AS_OF,
        "USD",
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=capture_options,
    )
    cold_at_c = await build_wallet_pnl_series(
        cast(Repository, repo),
        fixtures._W1,
        "live",
        fixtures._m(0),
        _C,
        "1m",
        capture._AS_OF,
        "USD",
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=PnlSeriesReplayOptions(pointwise_exact_fx=True),
    )
    assert captured.checkpoint_write is None
    assert captured.checkpoint_capture_refusal == "capture_value_error"
    assert [source.exchange for source in cold_at_c.rate_sources] == ["kraken"]
    assert captured.rate_sources == cold_at_c.rate_sources
    checkpoint_fixtures._assert_points_bit_identical(cold_at_c.points, captured.points)

    anchor = await build_wallet_pnl_series(
        cast(Repository, repo),
        fixtures._W1,
        "live",
        fixtures._m(5),
        _T,
        "1m",
        fixtures._m(10),
        "USD",
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=PnlSeriesReplayOptions(pointwise_exact_fx=True),
    )
    assert [source.exchange for source in anchor.rate_sources] == ["walutomat"]


async def test_deferred_clamped_first_fill_fee_matches_5c6_cold_capture_and_anchor_resume(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Match 5C.6 cold capture and anchor resume for an open_at <= C deferred fee.

    Given: An EUR-USD instrument on walutomat with no applied rows before C=m(4)
        and two deferred prefix rows in W_C (seq 1 at m(5) USD-free, seq 2 at
        timestamp m(2) with a 1.0 EUR fee clamped past C to effective_time m(5)).
    When: Cold read and capture run at C=m(4), verification runs at T=m(8), and
        suffix resume runs over (C, T].
    Then: Capture matches cold read at C on kraken with m(2) carried, and suffix
        resume falls back with ``checkpoint_fx_election_changed`` to walutomat matching
        the cold anchor read bit for bit.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """

    def _build_repo() -> _ResumeRepo:
        """Build repo with clamped pre-C deferred foreign fee and dual-venue FX."""
        built = _accrual_before_deferred_first_fill_repo()
        built._accruals = []
        built._executions = [
            fixtures._exec_row(
                fixtures._I2,
                1,
                5,
                "buy",
                1.0,
                1.1,
                0.0,
                "USD",
                exchange="walutomat",
                shard_key="s-def",
            ),
            fixtures._exec_row(
                fixtures._I2,
                2,
                2,
                "buy",
                1.0,
                1.1,
                1.0,
                "EUR",
                exchange="walutomat",
                shard_key="s-def",
            ),
        ]
        built._refs = [fixtures._ref(fixtures._I2, "EUR-USD", "USD", exchange="walutomat")]
        built._candles = [
            fixtures._candle(fixtures._m(minute), 1.2, fixtures._I2) for minute in range(9)
        ]
        built._lineage = [
            fixtures._lineage_row(row["order_public_id"], "rest") for row in built._executions
        ]
        built._fx_rows = [
            fixtures._fx_row("EUR", "USD", 2, 2.0, exchange="kraken"),
            fixtures._fx_row("EUR", "USD", 2, 3.0, exchange="walutomat"),
            fixtures._fx_row("EUR", "USD", 5, 3.0, exchange="walutomat"),
        ]
        built._execution_watermarks = {"walutomat": 2}
        return built

    repo = _build_repo()
    resume = await _capture_and_verify_profile(
        monkeypatch,
        repo,
        {"walutomat": 2},
        PnlEngineOptions(True, True, True),
    )
    assert [source.exchange for source in resume.bundle.rate_sources_prefix] == ["kraken"]
    options = replace(_resume_options(resume), frozen_execution_watermarks={"walutomat": 2})
    resumed = await build_wallet_pnl_series(
        cast(Repository, repo),
        fixtures._W1,
        "live",
        fixtures._m(5),
        _T,
        "1m",
        fixtures._m(10),
        "USD",
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=options,
    )
    anchor = await build_wallet_pnl_series(
        cast(Repository, _build_repo()),
        fixtures._W1,
        "live",
        fixtures._m(5),
        _T,
        "1m",
        fixtures._m(10),
        "USD",
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=replace(options, checkpoint_resume=None),
    )
    assert resumed.checkpoint_resume_fallback == "checkpoint_fx_election_changed"
    assert [source.exchange for source in anchor.rate_sources] == ["walutomat"]
    assert resumed.rate_sources == anchor.rate_sources
    checkpoint_fixtures._assert_points_bit_identical(anchor.points, resumed.points)


async def test_resume_allows_prefix_proven_and_fresh_suffix_identity_plane_instruments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Allow suffix identity planes for already-proven or prefix-unused instruments.

    Given: A prefix-proven EUR-USD instrument continuing into the suffix alongside
        a fresh suffix-only EUR-USD instrument with no prior prefix FX requirements.
    When: Checkpoint capture runs at C and suffix resume runs over (C, T].
    Then: Neither triggers ``checkpoint_fx_election_changed`` and resume matches anchor.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    third_instrument = "00000000-0000-7000-8000-000000000b06"

    def _build_repo(*, include_suffix: bool) -> _ResumeRepo:
        """Construct the prefix or full ledger cut for provenance and suffix checks.

        Args:
            include_suffix: Whether suffix EUR-USD executions are visible yet.

        Returns:
            Configured fake repository.
        """
        built = _accrual_before_deferred_first_fill_repo()
        built._accruals = []
        built._executions = [
            fixtures._exec_row(
                fixtures._I2,
                1,
                2,
                "buy",
                1.0,
                1.1,
                1.0,
                "EUR",
                exchange="walutomat",
            ),
        ]
        if include_suffix:
            built._executions.extend(
                [
                    fixtures._exec_row(
                        fixtures._I2,
                        2,
                        6,
                        "buy",
                        1.0,
                        1.2,
                        1.0,
                        "EUR",
                        exchange="walutomat",
                    ),
                    fixtures._exec_row(
                        third_instrument,
                        3,
                        7,
                        "buy",
                        1.0,
                        1.3,
                        1.0,
                        "EUR",
                        exchange="walutomat",
                    ),
                ]
            )
        built._refs = [
            fixtures._ref(fixtures._I2, "EUR-USD", "USD", exchange="walutomat"),
            fixtures._ref(third_instrument, "EUR-USD", "USD", exchange="walutomat"),
        ]
        built._candles = [
            fixtures._candle(fixtures._m(minute), 1.2, instrument)
            for minute in range(9)
            for instrument in (fixtures._I2, third_instrument)
        ]
        built._lineage = [
            fixtures._lineage_row(row["order_public_id"], "rest") for row in built._executions
        ]
        built._fx_rows = [
            fixtures._fx_row("EUR", "USD", minute, 3.0, exchange="walutomat")
            for minute in (2, 6, 7)
        ]
        built._execution_watermarks = {"walutomat": len(built._executions)}
        return built

    repo = _build_repo(include_suffix=False)
    resume = await _capture_and_verify_profile(
        monkeypatch,
        repo,
        {"walutomat": 1},
        PnlEngineOptions(True, True, True),
    )
    full_repo = _build_repo(include_suffix=True)
    repo._executions = list(full_repo._executions)
    repo._lineage = list(full_repo._lineage)
    repo._execution_watermarks = {"walutomat": 3}
    options = replace(_resume_options(resume), frozen_execution_watermarks={"walutomat": 3})
    resumed = await build_wallet_pnl_series(
        cast(Repository, repo),
        fixtures._W1,
        "live",
        fixtures._m(5),
        _T,
        "1m",
        fixtures._m(10),
        "USD",
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=options,
    )
    anchor = await build_wallet_pnl_series(
        cast(Repository, _build_repo(include_suffix=True)),
        fixtures._W1,
        "live",
        fixtures._m(5),
        _T,
        "1m",
        fixtures._m(10),
        "USD",
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=replace(options, checkpoint_resume=None),
    )
    assert resumed.checkpoint_resume_fallback is None
    assert resumed.rate_sources == anchor.rate_sources
    checkpoint_fixtures._assert_points_bit_identical(anchor.points, resumed.points)


async def test_resume_falls_back_when_conversion_only_instrument_gains_identity_plane(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Trigger venue fallback when conversion_requirements carried an unproven instrument.

    Given: An unproven EUR-USD instrument whose prefix carried only a non-causal
        ``conversion_requirements`` entry before its deferred first fill in suffix.
    When: Suffix resume gives that instrument its canonical identity plane.
    Then: ``_validate_resume_fx_election`` falls back with ``checkpoint_fx_election_changed``.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    repo = _accrual_before_deferred_first_fill_repo()
    repo._accruals = []
    with _conversion_only_event_fx(monkeypatch, fixtures._I2, fixtures._m(3)):
        resume = await _capture_and_verify_profile(
            monkeypatch,
            repo,
            {"kraken": 1, "walutomat": 1},
            PnlEngineOptions(True, True, True),
        )
    options = replace(
        _resume_options(resume),
        frozen_execution_watermarks={"kraken": 1, "walutomat": 1},
    )
    resumed = await build_wallet_pnl_series(
        cast(Repository, repo),
        fixtures._W1,
        "live",
        fixtures._m(5),
        _T,
        "1m",
        fixtures._m(10),
        "USD",
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=options,
    )
    assert resumed.checkpoint_resume_fallback == "checkpoint_fx_election_changed"


def _build_5c6_prefix_conversion_repo(*, include_suffix: bool) -> _ResumeRepo:
    """Build a ledger cut with a pre-C zero-size foreign-priced fill and optional suffix sell.

    Args:
        include_suffix: Whether the suffix sell at m(6) is present in the cut.

    Returns:
        Fake repository exercising real 5C.6 capture, verification, and suffix resume.
    """
    repo = _eur_prefix_only_repo()
    executions = [
        fixtures._exec_row(fixtures._I1, 1, 2, "buy", 0.0, 100.0, 0.0, "USD"),
        fixtures._exec_row(fixtures._I1, 2, 3, "buy", 1.0, 100.0, 1.0, "EUR"),
    ]
    if include_suffix:
        executions.append(fixtures._exec_row(fixtures._I1, 3, 6, "sell", 0.5, 110.0, 0.5, "EUR"))
    repo._executions = executions
    repo._lineage = [
        fixtures._lineage_row(row["order_public_id"], "rest") for row in repo._executions
    ]
    repo._execution_watermarks = {"kraken": len(executions)}
    return repo


async def test_e2e_5c6_capture_verify_resume_preserves_prefix_conversion_only_minute(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Preserve real prefix conversion-only minutes end-to-end under the 5C.6 profile.

    Given: Calculation version 5C.6 with ``capture_permitted=True`` enabled via
        ``_engine_options_table`` and a pre-C zero-size, zero-fee BTC-EUR fill.
    When: Real checkpoint capture runs at C=m(4), verification runs at T=m(8),
        and suffix resume runs over (C, T] alongside an uninterrupted anchor read.
    Then: No capture mock is used, m(2) appears in the resumed FX requirement set
        and shadow evaluation without prefix reads or fallback, and resumed points
        and rate sources equal the anchor bit for bit.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    assert fixtures.PNL_SAMPLE_CALC_VERSION == "5C.6"
    repo = _build_5c6_prefix_conversion_repo(include_suffix=False)
    resume = await _capture_and_verify_profile(
        monkeypatch,
        repo,
        {"kraken": 2},
        PnlEngineOptions(True, True, True),
    )
    carried_pairs = {
        entry.instrument_public_id: {req.pair: set(req.minutes) for req in entry.requirements}
        for entry in resume.bundle.event_walk_state.conversion_requirements
    }
    assert fixtures._m(2) in carried_pairs.get(fixtures._I1, {}).get(("EUR", "USD"), set())
    full_repo = _build_5c6_prefix_conversion_repo(include_suffix=True)
    repo._executions = list(full_repo._executions)
    repo._lineage = list(full_repo._lineage)
    repo._execution_watermarks = {"kraken": 3}
    observed_event_reqs: list[_FxInstrumentRequirements] = []
    original_load_fx = service._load_wallet_series_fx

    async def spy_load_fx(
        repo_arg: Repository,
        request: service._FxRequest,
        options_arg: PnlSeriesReplayOptions,
        policy_arg: PnlSeriesReadPolicy,
    ) -> service._SeriesFxEvidence:
        """Capture exact event FX requirements passed into FX loading.

        Args:
            repo_arg: Repository instance.
            request: FX requirement request.
            options_arg: Replay options.
            policy_arg: Read policy.

        Returns:
            Loaded FX evidence from the real loader.
        """
        observed_event_reqs.append(
            {
                inst: {pair: set(mins) for pair, mins in pairs.items()}
                for inst, pairs in request.event_requirements.items()
            }
        )
        return await original_load_fx(repo_arg, request, options_arg, policy_arg)

    monkeypatch.setattr(service, "_load_wallet_series_fx", spy_load_fx)
    options = replace(_resume_options(resume), frozen_execution_watermarks={"kraken": 3})
    shadow_ctx = FxShadowPinContext(calculation_version="5C.6", evaluations=[])
    with activate_fx_shadow_context(shadow_ctx):
        resumed = await build_wallet_pnl_series(
            cast(Repository, repo),
            fixtures._W1,
            "live",
            fixtures._m(5),
            _T,
            "1m",
            fixtures._m(10),
            "USD",
            policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
            options=options,
        )
    anchor = await build_wallet_pnl_series(
        cast(Repository, _build_5c6_prefix_conversion_repo(include_suffix=True)),
        fixtures._W1,
        "live",
        fixtures._m(5),
        _T,
        "1m",
        fixtures._m(10),
        "USD",
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=replace(options, checkpoint_resume=None),
    )
    assert fixtures._m(2) in observed_event_reqs[0].get(fixtures._I1, {}).get(("EUR", "USD"), set())
    assert any(
        ev.scope_kind == "shared_pair"
        and fixtures._m(2) in ev.event_minutes
        and fixtures._m(2) in ev.required_minutes
        for ev in shadow_ctx.evaluations
    )
    assert resumed.checkpoint_resume_fallback is None
    assert resumed.rate_sources == anchor.rate_sources
    checkpoint_fixtures._assert_points_bit_identical(anchor.points, resumed.points)


def _unproven_opening_pool_repo(*, include_suffix: bool) -> _ResumeRepo:
    """Hold EUR-USD at t0 with no opening-span proof until a covered suffix fill.

    Args:
        include_suffix: Whether the suffix execution pinning walutomat is present.

    Returns:
        Configured fake repository for unproven opening-pool identity testing.
    """
    repo = _ResumeRepo()
    shard_key = f"walutomat:live:{fixtures._I2}:untagged"
    anchor = fixtures._empty_anchor(
        fixtures._W1,
        "live",
        "USD",
        watermarks={"walutomat": 1},
    )
    anchor["unrealized_pnl"] = 0.5
    anchor["opening_basket_json"] = fixtures._canonical_test_json(
        {
            "schema_version": 3,
            "pools": [
                fixtures._test_anchor_pool(
                    instrument_public_id=fixtures._I2,
                    shard_key=shard_key,
                    exchange="walutomat",
                    position_qty=1.0,
                    historical_entry_price=1.0,
                    t0_mark=1.5,
                    opening_unrealized_value=0.5,
                )
            ],
            "annulments": [],
            "native_basket": {fixtures._I2: 1.0},
        }
    )
    anchor["contributions_json"] = fixtures._canonical_test_json(
        {
            "schema_version": 3,
            "pools": [
                fixtures._test_anchor_contribution(
                    instrument_public_id=fixtures._I2,
                    shard_key=shard_key,
                    exchange="walutomat",
                    quantity=1.0,
                )
            ],
        }
    )
    repo._anchor = anchor
    repo._executions = [
        fixtures._exec_row(
            fixtures._I2,
            1,
            -1,
            "buy",
            1.0,
            1.0,
            0.0,
            "USD",
            exchange="walutomat",
        ),
        *(
            [
                fixtures._exec_row(
                    fixtures._I2,
                    2,
                    6,
                    "buy",
                    1.0,
                    1.5,
                    0.0,
                    "USD",
                    exchange="walutomat",
                )
            ]
            if include_suffix
            else []
        ),
    ]
    repo._accruals = [fixtures._accrual_row(fixtures._I2, 3, 1.0, "EUR")]
    repo._refs = [
        fixtures._ref(
            fixtures._I2,
            "EUR-USD",
            "USD",
            exchange="walutomat",
            valid_from=fixtures._m(2),
        )
    ]
    repo._candles = [
        fixtures._candle(fixtures._m(minute), 1.5, fixtures._I2) for minute in range(9)
    ]
    repo._lineage = [
        fixtures._lineage_row(row["order_public_id"], "rest") for row in repo._executions
    ]
    repo._fx_rows = [
        fixtures._fx_row("EUR", "USD", 3, 2.0, exchange="kraken"),
        *[
            fixtures._fx_row("EUR", "USD", minute, 3.0, exchange="walutomat")
            for minute in range(1, 9)
        ],
    ]
    repo._execution_watermarks = {"walutomat": len(repo._executions)}
    return repo


async def test_resume_refuses_identity_plane_for_unproven_opening_pool_span(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Refuse when a suffix fill proves a plane that an unproven opening pool could not pin.

    Given: An opening EUR-USD pool on walutomat whose symbol reference starts only at
        ``m(2)`` (leaving ``[t0, C]`` unproven), a prefix accrual at ``m(3)`` converted on
        the shared kraken plane (``-2.0``), and a suffix fill at ``m(6)`` covered by
        the reference that proves walutomat on a cold replay (``-3.0``).
    When: Checkpoint capture runs without the suffix fill and resume runs after the suffix
        fill lands.
    Then: Resume falls back with ``checkpoint_fx_election_changed`` and returns bit-identical
        points and rate sources to the cold anchor replay.

    Args:
        monkeypatch: Pytest monkeypatch fixture for profile flags.
    """
    repo = _unproven_opening_pool_repo(include_suffix=False)
    resume = await _capture_and_verify_profile(
        monkeypatch,
        repo,
        {"walutomat": 1},
        PnlEngineOptions(True, True, True),
    )
    full_repo = _unproven_opening_pool_repo(include_suffix=True)
    repo._executions = list(full_repo._executions)
    repo._lineage = list(full_repo._lineage)
    repo._execution_watermarks = {"walutomat": 2}
    options = replace(_resume_options(resume), frozen_execution_watermarks={"walutomat": 2})
    resumed = await build_wallet_pnl_series(
        cast(Repository, repo),
        fixtures._W1,
        "live",
        fixtures._m(5),
        _T,
        "1m",
        fixtures._m(10),
        "USD",
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=options,
    )
    anchor = await build_wallet_pnl_series(
        cast(Repository, full_repo),
        fixtures._W1,
        "live",
        fixtures._m(5),
        _T,
        "1m",
        fixtures._m(10),
        "USD",
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=replace(options, checkpoint_resume=None),
    )
    assert anchor.points[-1].accrual_pnl == pytest.approx(-3.0)
    assert resumed.checkpoint_resume_fallback == "checkpoint_fx_election_changed"
    assert resumed.points[-1].accrual_pnl == pytest.approx(-3.0)
    assert resumed.rate_sources == anchor.rate_sources
    checkpoint_fixtures._assert_points_bit_identical(anchor.points, resumed.points)


def _reclassified_span_repo(*, include_suffix: bool) -> _ResumeRepo:
    """Build Case A: opening span proven by walutomat at C, reclassified to kraken at T.

    Args:
        include_suffix: Whether the suffix fill inside the kraken ref window is visible.

    Returns:
        Configured fake repository exercising span reclassification between C and T.
    """
    repo = _ResumeRepo()
    shard_key = f"walutomat:live:{fixtures._I2}:untagged"
    anchor = fixtures._empty_anchor(
        fixtures._W1,
        "live",
        "USD",
        watermarks={"walutomat": 1},
    )
    anchor["unrealized_pnl"] = 0.5
    anchor["opening_basket_json"] = fixtures._canonical_test_json(
        {
            "schema_version": 3,
            "pools": [
                fixtures._test_anchor_pool(
                    instrument_public_id=fixtures._I2,
                    shard_key=shard_key,
                    exchange="walutomat",
                    position_qty=1.0,
                    historical_entry_price=1.0,
                    t0_mark=1.5,
                    opening_unrealized_value=0.5,
                )
            ],
            "annulments": [],
            "native_basket": {fixtures._I2: 1.0},
        }
    )
    anchor["contributions_json"] = fixtures._canonical_test_json(
        {
            "schema_version": 3,
            "pools": [
                fixtures._test_anchor_contribution(
                    instrument_public_id=fixtures._I2,
                    shard_key=shard_key,
                    exchange="walutomat",
                    quantity=1.0,
                )
            ],
        }
    )
    repo._anchor = anchor
    repo._executions = [
        fixtures._exec_row(
            fixtures._I2,
            1,
            -1,
            "buy",
            1.0,
            1.0,
            0.0,
            "USD",
            exchange="walutomat",
        ),
        *(
            [
                fixtures._exec_row(
                    fixtures._I2,
                    2,
                    7,
                    "buy",
                    1.0,
                    1.5,
                    0.0,
                    "USD",
                    exchange="walutomat",
                )
            ]
            if include_suffix
            else []
        ),
    ]
    repo._accruals = [fixtures._accrual_row(fixtures._I2, 3, 1.0, "EUR")]
    repo._refs = [
        fixtures._ref(
            fixtures._I2,
            "EUR-USD",
            "USD",
            exchange="walutomat",
            instrument_exchange="walutomat",
            valid_from=fixtures._EMPTY_ANCHOR_T0,
            valid_to=fixtures._m(6),
        ),
        fixtures._ref(
            fixtures._I2,
            "EUR-USD",
            "USD",
            exchange="kraken",
            instrument_exchange="walutomat",
            valid_from=fixtures._m(6),
        ),
    ]
    repo._candles = [
        fixtures._candle(fixtures._m(minute), 1.5, fixtures._I2) for minute in range(9)
    ]
    repo._lineage = [
        fixtures._lineage_row(row["order_public_id"], "rest") for row in repo._executions
    ]
    repo._fx_rows = [
        *[fixtures._fx_row("EUR", "USD", minute, 2.0, exchange="kraken") for minute in range(1, 9)],
        *[
            fixtures._fx_row("EUR", "USD", minute, 3.0, exchange="walutomat")
            for minute in range(1, 9)
        ],
    ]
    repo._execution_watermarks = {"walutomat": len(repo._executions)}
    return repo


def _shared_plane_move_repo(*, include_suffix: bool) -> _ResumeRepo:
    """Build Case B: shared EUR/USD tie-break at C moves to sole complete venue at T.

    Args:
        include_suffix: Whether the suffix fill at m(6) with walutomat-only FX is present.

    Returns:
        Configured fake repository with primary fee instrument and peer accrual instrument.
    """
    peer_instrument = "00000000-0000-7000-8000-000000000b06"
    repo = _ResumeRepo()
    repo._executions = [
        fixtures._exec_row(fixtures._I1, 1, 2, "buy", 1.0, 100.0, 1.0, "EUR"),
        *(
            [fixtures._exec_row(fixtures._I1, 2, 6, "buy", 1.0, 100.0, 1.0, "EUR")]
            if include_suffix
            else []
        ),
    ]
    repo._accruals = [fixtures._accrual_row(peer_instrument, 3, 1.0, "EUR")]
    repo._refs = [
        fixtures._ref(fixtures._I1, "BTC-USD", "USD"),
        fixtures._ref(peer_instrument, "ETH-USD", "USD"),
    ]
    repo._candles = [
        fixtures._candle(fixtures._m(minute), 100.0, instrument)
        for minute in range(9)
        for instrument in (fixtures._I1, peer_instrument)
    ]
    repo._lineage = [
        fixtures._lineage_row(row["order_public_id"], "rest") for row in repo._executions
    ]
    repo._fx_rows = [
        fixtures._fx_row("EUR", "USD", 2, 2.0, exchange="kraken"),
        fixtures._fx_row("EUR", "USD", 3, 2.0, exchange="kraken"),
        fixtures._fx_row("EUR", "USD", 2, 3.0, exchange="walutomat"),
        fixtures._fx_row("EUR", "USD", 3, 3.0, exchange="walutomat"),
        fixtures._fx_row("EUR", "USD", 6, 3.0, exchange="walutomat"),
    ]
    repo._execution_watermarks = {"kraken": len(repo._executions)}
    return repo


async def test_resume_refuses_span_proven_at_c_reclassified_to_suffix_ref(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Refuse when an opening span proven at C by walutomat reclassifies to kraken at T.

    Given: An opening EUR-USD pool whose walutomat ref covers ``[t0, C]`` while a second
        known ref on kraken starts after ``C`` and covers a suffix fill at ``m(7)``.
    When: Capture runs at ``C`` electing walutomat (``-3.0`` accrual) and suffix resume runs
        at ``T`` alongside the cold anchor replay (which installs kraken, ``-2.0`` accrual).
    Then: Election stability refuses resume and returns the cold-equal anchor result.

    Args:
        monkeypatch: Pytest monkeypatch fixture for profile flags.
    """
    repo = _reclassified_span_repo(include_suffix=False)
    resume = await _capture_and_verify_profile(
        monkeypatch,
        repo,
        {"walutomat": 1},
        PnlEngineOptions(True, True, True),
    )
    full_repo = _reclassified_span_repo(include_suffix=True)
    repo._executions = list(full_repo._executions)
    repo._lineage = list(full_repo._lineage)
    repo._execution_watermarks = {"walutomat": 2}
    options = replace(_resume_options(resume), frozen_execution_watermarks={"walutomat": 2})
    resumed = await build_wallet_pnl_series(
        cast(Repository, repo),
        fixtures._W1,
        "live",
        fixtures._m(5),
        _T,
        "1m",
        fixtures._m(10),
        "USD",
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=options,
    )
    anchor = await build_wallet_pnl_series(
        cast(Repository, full_repo),
        fixtures._W1,
        "live",
        fixtures._m(5),
        _T,
        "1m",
        fixtures._m(10),
        "USD",
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=replace(options, checkpoint_resume=None),
    )
    assert anchor.points[-1].accrual_pnl == pytest.approx(-2.0)
    assert resumed.checkpoint_resume_fallback == "checkpoint_fx_election_changed"
    assert resumed.points[-1].accrual_pnl == pytest.approx(-2.0)
    assert resumed.rate_sources == anchor.rate_sources
    checkpoint_fixtures._assert_points_bit_identical(anchor.points, resumed.points)


async def test_resume_refuses_shared_plane_move_including_peer_via_election_stability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Refuse when window-wide shared FX election moves prefix fee and peer to walutomat.

    Given: A prefix EUR fee and peer EUR accrual tie-broken to kraken at capture, plus a
        suffix EUR fee at ``m(6)`` printed only by walutomat under window-wide election.
    When: The window-wide election policy runs capture at ``C`` and resume vs cold at ``T``.
    Then: Election stability detects that both prefix consumer minutes moved to walutomat,
        refuses with ``checkpoint_fx_election_changed``, and matches the cold anchor.

    Args:
        monkeypatch: Pytest monkeypatch fixture for profile flags and window-wide election seam.
    """
    monkeypatch.setattr(
        service,
        "_checkpoint_read_profile_supported",
        lambda options, policy: not policy.carry_valuation,
    )
    monkeypatch.setattr(
        verifier_service,
        "_checkpoint_ineligible",
        lambda record, elig: False,
    )
    _permit_profile(monkeypatch, PnlEngineOptions(True, True, True))
    monkeypatch.setattr(
        service,
        "_series_engine_options",
        lambda options: PnlEngineOptions(True, True, True),
    )
    repo = _shared_plane_move_repo(include_suffix=False)
    request = replace(capture._request(), raw_execution_watermarks={"kraken": 1})
    capture_options = replace(
        capture._options(request, pointwise_exact_fx=False),
        replay_generation_tip=_C,
    )
    capture_result = await build_wallet_pnl_series(
        cast(Repository, repo),
        fixtures._W1,
        "live",
        fixtures._m(0),
        _C,
        "1m",
        capture._AS_OF,
        "USD",
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=capture_options,
    )
    assert capture_result.checkpoint_write is not None
    write = capture_result.checkpoint_write
    record = PnlOpeningCheckpointRecord(write.certificate, write.state_json, fixtures._m(10))
    eligibility = replace(
        verifier._eligibility(request),
        pointwise_exact_fx=False,
        from_time=fixtures._m(5),
        frontier_tip=_T,
        replay_generation_tip=_T,
    )
    outcome = await verify_checkpoint_for_resume(
        cast(Repository, repo),
        record,
        verifier._scope(request),
        fixtures._m(10),
        eligibility,
    )
    assert isinstance(outcome, PnlCheckpointResume)
    repo.forbid_prefix = True
    full_repo = _shared_plane_move_repo(include_suffix=True)
    repo._executions = list(full_repo._executions)
    repo._lineage = list(full_repo._lineage)
    repo._execution_watermarks = {"kraken": 2}
    options = replace(
        _resume_options(outcome),
        pointwise_exact_fx=False,
        frozen_execution_watermarks={"kraken": 2},
    )
    resumed = await build_wallet_pnl_series(
        cast(Repository, repo),
        fixtures._W1,
        "live",
        fixtures._m(5),
        _T,
        "1m",
        fixtures._m(10),
        "USD",
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=options,
    )
    anchor = await build_wallet_pnl_series(
        cast(Repository, full_repo),
        fixtures._W1,
        "live",
        fixtures._m(5),
        _T,
        "1m",
        fixtures._m(10),
        "USD",
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=replace(options, checkpoint_resume=None),
    )
    assert anchor.points[-1].fee_pnl == pytest.approx(-6.0)
    assert anchor.points[-1].accrual_pnl == pytest.approx(-3.0)
    assert resumed.checkpoint_resume_fallback == "checkpoint_fx_election_changed"
    assert resumed.points[-1].fee_pnl == pytest.approx(-6.0)
    assert resumed.points[-1].accrual_pnl == pytest.approx(-3.0)
    assert resumed.rate_sources == anchor.rate_sources
    checkpoint_fixtures._assert_points_bit_identical(anchor.points, resumed.points)


async def test_resume_accepts_unchanged_shared_and_identity_elections(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Accept direct resume without fallback when shared and identity elections stay stable.

    Given: A prefix EUR fee tie-broken to kraken plus a peer EUR accrual, followed by a
        suffix EUR fee covered only by walutomat under the active pointwise profile.
    When: Capture runs at ``C`` and pointwise resume runs at ``T``.
    Then: Elections for every prefix conversion remain unchanged, resume succeeds without
        reloading the prefix or falling back, and points match anchor bit for bit.

    Args:
        monkeypatch: Pytest monkeypatch fixture for profile flags.
    """
    repo = _shared_plane_move_repo(include_suffix=False)
    resume = await _capture_and_verify_profile(
        monkeypatch,
        repo,
        {"kraken": 1},
        PnlEngineOptions(True, True, True),
    )
    full_repo = _shared_plane_move_repo(include_suffix=True)
    repo._executions = list(full_repo._executions)
    repo._lineage = list(full_repo._lineage)
    repo._execution_watermarks = {"kraken": 2}
    options = replace(_resume_options(resume), frozen_execution_watermarks={"kraken": 2})
    resumed = await build_wallet_pnl_series(
        cast(Repository, repo),
        fixtures._W1,
        "live",
        fixtures._m(5),
        _T,
        "1m",
        fixtures._m(10),
        "USD",
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=options,
    )
    anchor = await build_wallet_pnl_series(
        cast(Repository, full_repo),
        fixtures._W1,
        "live",
        fixtures._m(5),
        _T,
        "1m",
        fixtures._m(10),
        "USD",
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=replace(options, checkpoint_resume=None),
    )
    assert resumed.checkpoint_resume_fallback is None
    assert resumed.points[-1].fee_pnl == pytest.approx(-5.0)
    assert resumed.points[-1].accrual_pnl == pytest.approx(-2.0)
    assert resumed.rate_sources == anchor.rate_sources
    checkpoint_fixtures._assert_points_bit_identical(anchor.points, resumed.points)


async def test_resume_excludes_prefix_mark_only_plane_from_rate_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exclude prefix mark-only FX planes from ``rate_sources_prefix`` so resume equals cold.

    Given: I1 BTC-EUR on kraken opening at m(1) and closing at m(3), I2 EUR-USD on
        walutomat buying at m(4) with a 1 EUR fee, kraken EUR-USD candles at m(1) and
        m(3), and walutomat EUR-USD only at mark-only m(2).
    When: Checkpoint capture runs at C=m(4) under pointwise exact FX and resumes to m(8).
    Then: Resumed rate_sources matches cold rate_sources ``(kraken,)`` without leaking
        walutomat from prefix mark minute m(2), and points remain bit-identical.

    Args:
        monkeypatch: Pytest monkeypatch fixture for profile flags.
    """
    repo = _ResumeRepo()
    repo._accruals = []
    repo._executions = [
        fixtures._exec_row(fixtures._I1, 1, 1, "buy", 1.0, 100.0, 0.0, "EUR"),
        fixtures._exec_row(fixtures._I1, 2, 3, "sell", 1.0, 100.0, 0.0, "EUR"),
        fixtures._exec_row(
            fixtures._I2,
            1,
            4,
            "buy",
            1.0,
            1.1,
            1.0,
            "EUR",
            exchange="walutomat",
        ),
    ]
    repo._refs = [
        fixtures._ref(fixtures._I1, "BTC-EUR", "EUR", exchange="kraken"),
        fixtures._ref(fixtures._I2, "EUR-USD", "USD", exchange="walutomat"),
    ]
    repo._candles = [
        fixtures._candle(fixtures._m(minute), 100.0, fixtures._I1) for minute in range(9)
    ] + [fixtures._candle(fixtures._m(minute), 1.1, fixtures._I2) for minute in range(9)]
    repo._lineage = [
        fixtures._lineage_row(row["order_public_id"], "rest") for row in repo._executions
    ]
    repo._fx_rows = [
        fixtures._fx_row("EUR", "USD", 1, 2.0, exchange="kraken"),
        fixtures._fx_row("EUR", "USD", 2, 2.5, exchange="walutomat"),
        fixtures._fx_row("EUR", "USD", 3, 2.0, exchange="kraken"),
    ]
    repo._execution_watermarks = {"kraken": 2, "walutomat": 1}
    resume = await _capture_and_verify_profile(
        monkeypatch,
        repo,
        {"kraken": 2, "walutomat": 1},
        PnlEngineOptions(True, True, True),
    )
    assert [source.exchange for source in resume.bundle.rate_sources_prefix] == ["kraken"]
    options = replace(
        _resume_options(resume),
        frozen_execution_watermarks={"kraken": 2, "walutomat": 1},
    )
    resumed = await build_wallet_pnl_series(
        cast(Repository, repo),
        fixtures._W1,
        "live",
        fixtures._m(5),
        _T,
        "1m",
        fixtures._m(10),
        "USD",
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=options,
    )
    cold = await build_wallet_pnl_series(
        cast(Repository, repo),
        fixtures._W1,
        "live",
        fixtures._m(5),
        _T,
        "1m",
        fixtures._m(10),
        "USD",
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=replace(options, checkpoint_resume=None),
    )
    assert [source.exchange for source in cold.rate_sources] == ["kraken"]
    assert resumed.checkpoint_resume_fallback is None
    assert resumed.rate_sources == cold.rate_sources
    checkpoint_fixtures._assert_points_bit_identical(cold.points, resumed.points)


async def test_resume_matches_cold_rate_sources_with_post_c_mark_only_plane(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Include post-C mark-only FX planes on resume identically to cold read.

    Given: The prefix mark-only walutomat setup at m(2) plus a held BTC-EUR position
        with an isolated post-C mark-only bybit EUR-USD candle at m(6).
    When: Checkpoint capture runs at C=m(4) and resumes across [m(5), m(8)].
    Then: Both cold and resumed rate_sources include ``bybit`` and ``kraken`` while
        excluding prefix mark-only ``walutomat``, and all points stay bit-identical.

    Args:
        monkeypatch: Pytest monkeypatch fixture for profile flags.
    """
    held_btc_eur = "00000000-0000-7000-8000-000000000b09"
    repo = _ResumeRepo()
    repo._accruals = []
    repo._executions = [
        fixtures._exec_row(fixtures._I1, 1, 1, "buy", 1.0, 100.0, 0.0, "EUR"),
        fixtures._exec_row(fixtures._I1, 2, 3, "sell", 1.0, 100.0, 0.0, "EUR"),
        fixtures._exec_row(held_btc_eur, 3, 1, "buy", 1.0, 100.0, 0.0, "EUR"),
        fixtures._exec_row(
            fixtures._I2,
            1,
            4,
            "buy",
            1.0,
            1.1,
            1.0,
            "EUR",
            exchange="walutomat",
        ),
    ]
    repo._refs = [
        fixtures._ref(fixtures._I1, "BTC-EUR", "EUR", exchange="kraken"),
        fixtures._ref(held_btc_eur, "BTC-EUR", "EUR", exchange="kraken"),
        fixtures._ref(fixtures._I2, "EUR-USD", "USD", exchange="walutomat"),
    ]
    repo._candles = (
        [fixtures._candle(fixtures._m(minute), 100.0, fixtures._I1) for minute in range(9)]
        + [fixtures._candle(fixtures._m(minute), 100.0, held_btc_eur) for minute in range(9)]
        + [fixtures._candle(fixtures._m(minute), 1.1, fixtures._I2) for minute in range(9)]
    )
    repo._lineage = [
        fixtures._lineage_row(row["order_public_id"], "rest") for row in repo._executions
    ]
    repo._fx_rows = [
        fixtures._fx_row("EUR", "USD", 1, 2.0, exchange="kraken"),
        fixtures._fx_row("EUR", "USD", 2, 2.5, exchange="walutomat"),
        fixtures._fx_row("EUR", "USD", 3, 2.0, exchange="kraken"),
        fixtures._fx_row("EUR", "USD", 6, 2.2, exchange="bybit"),
    ]
    repo._execution_watermarks = {"kraken": 3, "walutomat": 1}
    resume = await _capture_and_verify_profile(
        monkeypatch,
        repo,
        {"kraken": 3, "walutomat": 1},
        PnlEngineOptions(True, True, True),
    )
    options = replace(
        _resume_options(resume),
        frozen_execution_watermarks={"kraken": 3, "walutomat": 1},
    )
    resumed = await build_wallet_pnl_series(
        cast(Repository, repo),
        fixtures._W1,
        "live",
        fixtures._m(5),
        _T,
        "1m",
        fixtures._m(10),
        "USD",
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=options,
    )
    cold = await build_wallet_pnl_series(
        cast(Repository, repo),
        fixtures._W1,
        "live",
        fixtures._m(5),
        _T,
        "1m",
        fixtures._m(10),
        "USD",
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=replace(options, checkpoint_resume=None),
    )
    assert sorted(source.exchange for source in cold.rate_sources) == ["bybit", "kraken"]
    assert resumed.checkpoint_resume_fallback is None
    assert resumed.rate_sources == cold.rate_sources
    checkpoint_fixtures._assert_points_bit_identical(cold.points, resumed.points)


@pytest.mark.parametrize("offset_seconds", [-30, 30])
async def test_subminute_from_time_on_direct_resume_falls_back_equal_to_cold(
    monkeypatch: pytest.MonkeyPatch,
    offset_seconds: int,
) -> None:
    """Fall back cleanly instead of raising when direct resume receives sub-minute ``from_time``.

    Given: A valid checkpoint captured at C and a read starting at ``C - 30s`` or ``C + 30s``.
    When: ``build_wallet_pnl_series`` is called directly with that resume state.
    Then: It never raises ``ValueError``, falls back with ``checkpoint_ineligible``,
        and returns points bit-identical to the cold anchor read.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        offset_seconds: Sub-minute delta relative to ``C``.
    """
    repo = _ResumeRepo()
    resume = await _verified_resume(monkeypatch, repo)
    from_time = _C + timedelta(seconds=offset_seconds)
    options = replace(_resume_options(resume), checkpoint_resume=resume)
    resumed = await build_wallet_pnl_series(
        repo,
        fixtures._W1,
        "live",
        from_time,
        _T,
        "1m",
        fixtures._m(10),
        "USD",
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=options,
    )
    anchor_repo = _ResumeRepo()
    anchor_repo._execution_watermarks = {"kraken": 4}
    cold = await build_wallet_pnl_series(
        anchor_repo,
        fixtures._W1,
        "live",
        from_time,
        _T,
        "1m",
        fixtures._m(10),
        "USD",
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=replace(options, checkpoint_resume=None),
    )
    assert resumed.checkpoint_resume_fallback == "checkpoint_ineligible"
    checkpoint_fixtures._assert_points_bit_identical(cold.points, resumed.points)


async def test_resume_falls_back_when_prefix_fx_candle_commits_between_verify_and_fx_load(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fall back cold when same-plane prefix FX candles commit after verify before FX load.

    Given: A verified checkpoint whose prefix EUR fee consumed kraken EUR/USD at m(1),
        plus a suffix EUR fee at m(6).
    When: A same-plane FX correction updates m(1) and m(6) inside ``frontier_as_of``
        after ``verify_checkpoint_for_resume`` passes and before FX loading runs.
    Then: The post-FX-load fingerprint guard detects the TOCTOU change, falls back with
        ``checkpoint_event_fx_candles_changed``, and matches the cold read bit for bit
        instead of mixing stale prefix totals with corrected suffix FX.

    Args:
        monkeypatch: Pytest monkeypatch fixture for profile flags and TOCTOU hook.
    """
    repo = _ResumeRepo()
    repo._accruals = []
    repo._executions = [
        fixtures._exec_row(fixtures._I1, 1, 1, "buy", 1.0, 100.0, 1.0, "EUR"),
        fixtures._exec_row(fixtures._I1, 2, 6, "buy", 1.0, 100.0, 1.0, "EUR"),
    ]
    repo._refs = [fixtures._ref(fixtures._I1, "BTC-USD", "USD", exchange="kraken")]
    repo._candles = [
        fixtures._candle(fixtures._m(minute), 100.0, fixtures._I1) for minute in range(9)
    ]
    repo._lineage = [
        fixtures._lineage_row(row["order_public_id"], "rest") for row in repo._executions
    ]
    repo._fx_rows = [
        fixtures._fx_row("EUR", "USD", 1, 2.0, exchange="kraken"),
        fixtures._fx_row("EUR", "USD", 6, 2.0, exchange="kraken"),
    ]
    repo._execution_watermarks = {"kraken": 1}
    resume = await _capture_and_verify_profile(
        monkeypatch,
        repo,
        {"kraken": 1},
        PnlEngineOptions(True, True, True),
    )
    repo._executions = list(repo._executions)
    repo._execution_watermarks = {"kraken": 2}
    corrected_prefix = fixtures._fx_row("EUR", "USD", 1, 5.0, exchange="kraken")
    corrected_prefix["candle_id"] = 101
    corrected_prefix["candle_timestamp"] = fixtures._m(9)
    corrected_suffix = fixtures._fx_row("EUR", "USD", 6, 3.0, exchange="kraken")
    corrected_suffix["candle_id"] = 106
    corrected_suffix["candle_timestamp"] = fixtures._m(9)
    committed_after_verify = False
    original_load_candles = repo.get_pnl_fx_rate_candles_at

    async def commit_fx_before_load(
        planes: Sequence[PnlFxRatePlane],
        open_ats: Sequence[datetime],
        as_of: datetime,
    ) -> list[PnlFxRateRow]:
        """Commit corrected prefix and suffix FX candles before resume loads FX rows."""
        nonlocal committed_after_verify
        if not committed_after_verify:
            committed_after_verify = True
            repo._fx_rows = [corrected_prefix, corrected_suffix]
        return await original_load_candles(planes, open_ats, as_of)

    monkeypatch.setattr(repo, "get_pnl_fx_rate_candles_at", commit_fx_before_load)
    options = replace(_resume_options(resume), frozen_execution_watermarks={"kraken": 2})
    resumed = await build_wallet_pnl_series(
        cast(Repository, repo),
        fixtures._W1,
        "live",
        fixtures._m(5),
        _T,
        "1m",
        fixtures._m(10),
        "USD",
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=options,
    )
    assert committed_after_verify
    cold = await build_wallet_pnl_series(
        cast(Repository, repo),
        fixtures._W1,
        "live",
        fixtures._m(5),
        _T,
        "1m",
        fixtures._m(10),
        "USD",
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=replace(options, checkpoint_resume=None),
    )
    assert cold.points[-1].fee_pnl == pytest.approx(-8.0)
    assert resumed.checkpoint_resume_fallback == "checkpoint_event_fx_candles_changed"
    checkpoint_fixtures._assert_points_bit_identical(cold.points, resumed.points)
