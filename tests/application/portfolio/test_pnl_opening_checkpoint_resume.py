"""End-to-end checkpoint suffix resume, bit identity, and late-fill fallback."""

from dataclasses import replace
from datetime import datetime
from datetime import timedelta

import pytest

from snapper.application.portfolio import pnl_timeline_service as service
from snapper.application.portfolio.pnl_opening_checkpoint_verifier import PnlCheckpointResume
from snapper.application.portfolio.pnl_opening_checkpoint_verifier import (
    verify_checkpoint_for_resume,
)
from snapper.application.portfolio.pnl_timeline import PnlTimelineWorkBudgetError
from snapper.application.portfolio.pnl_timeline_service import PnlFrozenReplayEvidenceChangedError
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReadPolicy
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReplayOptions
from snapper.application.portfolio.pnl_timeline_service import PnlWalletSeriesResult
from snapper.application.portfolio.pnl_timeline_service import _execution_manifest_digest
from snapper.application.portfolio.pnl_timeline_service import build_wallet_pnl_series
from snapper.data.pnl_correction_state import PnlOpeningCheckpointRecord
from snapper.data.repository_types import PnlTimelineAccrualRow
from snapper.data.repository_types import PnlTimelineAppliedAnnulment
from snapper.data.repository_types import PnlTimelineExecutionSuffix
from snapper.data.repository_types import PnlTimelineExecutionSuffixRequest
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
    anchor_t0: datetime = fixtures._T0,
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


async def test_checkpoint_resume_with_carry_valuation_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject checkpoint resume when carry_valuation is enabled.

    Given: A verified checkpoint resume state and frozen generation watermarks.
    When: build_wallet_pnl_series is called with policy.carry_valuation=True.
    Then: Policy validation raises ValueError.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    repo = _ResumeRepo()
    resume = await _verified_resume(monkeypatch, repo)
    with pytest.raises(ValueError, match="carry valuation"):
        await build_wallet_pnl_series(
            repo,
            fixtures._W1,
            "live",
            fixtures._m(5),
            _T,
            "1m",
            fixtures._m(10),
            "USD",
            policy=PnlSeriesReadPolicy(allow_anchor_creation=False, carry_valuation=True),
            options=_resume_options(resume),
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
