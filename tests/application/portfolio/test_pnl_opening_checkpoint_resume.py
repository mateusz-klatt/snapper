"""End-to-end checkpoint suffix resume, bit identity, and late-fill fallback."""

from dataclasses import replace
from datetime import datetime

import pytest

from snapper.application.portfolio.pnl_opening_checkpoint_verifier import PnlCheckpointResume
from snapper.application.portfolio.pnl_opening_checkpoint_verifier import (
    verify_checkpoint_for_resume,
)
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReadPolicy
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReplayOptions
from snapper.application.portfolio.pnl_timeline_service import PnlWalletSeriesResult
from snapper.application.portfolio.pnl_timeline_service import build_wallet_pnl_series
from snapper.data.pnl_correction_state import PnlOpeningCheckpointRecord
from snapper.data.repository_types import PnlTimelineAccrualRow
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
        self.resume_accrual_bounds: list[tuple[datetime | None, datetime | None]] = []
        self.forbid_prefix = False

    async def get_pnl_timeline_execution_suffix_at_watermarks(
        self,
        request: PnlTimelineExecutionSuffixRequest,
    ) -> list[PnlTimelineOpeningExecutionRow]:
        """Return only requested suffix and deferred rows without a prefix read.

        Args:
            request: Exact cutoff, generation, deferred coordinates, and horizon.

        Returns:
            Filtered canned effective execution rows.
        """
        self.suffix_calls += 1
        deferred = {
            (exchange, sequence)
            for exchange, sequences in request.deferred_execution_sequences.items()
            for sequence in sequences
        }
        return [
            row
            for row in self._executions
            if (
                request.cutoff_watermarks.get(row["exchange"], 0)
                < row["scope_sequence"]
                <= request.generation_watermarks.get(row["exchange"], 0)
            )
            or (row["exchange"], row["scope_sequence"]) in deferred
        ]

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
