"""Expectations for snapshotter detection across certified annulment folds."""

import hashlib
from collections.abc import Mapping
from dataclasses import replace
from datetime import datetime
from typing import Literal

import pytest

from snapper.application.portfolio.execution_chain import ExecutionChainError
from snapper.application.portfolio.pnl_timeline_service import PnlAnchorEvidenceError
from snapper.application.portfolio.pnl_timeline_service import PnlFrozenReplayEvidenceChangedError
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReadPolicy
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReplayMetadata
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReplayOptions
from snapper.application.portfolio.pnl_timeline_service import PnlWalletSeriesResult
from snapper.application.portfolio.pnl_timeline_service import _execution_manifest_digest
from snapper.application.portfolio.pnl_timeline_service import build_wallet_pnl_series
from snapper.data.repository_types import PnlTimelineAppliedAnnulment
from snapper.data.repository_types import PnlTimelineExecutionPrefix
from tests.application.portfolio import test_pnl_timeline_service as fixtures

_WALLET = fixtures._W1
_minute = fixtures._m


class _ReplayRepo(fixtures.FakeRepo):
    """Current certified fold plus a proof double for the exact previous cut."""

    def __init__(self, known_minute: int, *, refuse_previous_cut: bool = False) -> None:
        """Keep later effective executions outside the requested finalized tip."""
        super().__init__(
            executions=[
                fixtures._exec_row(fixtures._I1, 1, 3, "buy", 1.0, 100.0, 0.0, "USD"),
                fixtures._exec_row(fixtures._I1, 2, 7, "buy", 1.0, 100.0, 0.0, "USD"),
                fixtures._exec_row(fixtures._I1, 4, 8, "buy", 1.0, 100.0, 0.0, "USD"),
            ],
            execution_watermarks={"kraken": 4},
            anchor=fixtures._anchor_recording({}, []),
        )
        self.apply_annulments(
            [fixtures._applied_annulment(correction_time=_minute(2), scope_sequence=3)],
            known_from=_minute(known_minute),
        )
        self.previous_cuts: list[tuple[str, str, dict[str, int], datetime]] = []
        self._refuse_previous_cut = refuse_previous_cut
        self.reported_cut: dict[str, int] | None = None

    async def get_pnl_timeline_execution_prefix_at_watermarks(
        self,
        wallet_public_id: str,
        mode: str,
        watermarks: Mapping[str, int],
        as_of: datetime,
    ) -> PnlTimelineExecutionPrefix:
        """Record the cut and return only annulments proved within its coordinates."""
        self.previous_cuts.append((wallet_public_id, mode, dict(watermarks), as_of))
        if self._refuse_previous_cut:
            raise ExecutionChainError("historical certification unavailable")
        prefix = self._execution_prefix_at(as_of)
        return {
            "watermarks": dict(watermarks) if self.reported_cut is None else self.reported_cut,
            "executions": [
                row
                for row in prefix["executions"]
                if row["scope_sequence"] <= watermarks.get(row["exchange"], 0)
            ],
            "annulments": [
                row
                for row in prefix["annulments"]
                if row["scope_sequence"] <= watermarks.get(row["exchange"], 0)
            ],
        }


async def _read_series(
    repo: fixtures.FakeRepo,
    options: PnlSeriesReplayOptions,
    *,
    policy: PnlSeriesReadPolicy = PnlSeriesReadPolicy(allow_anchor_creation=False),
    valuation_ccy: str = "USD",
) -> PnlWalletSeriesResult:
    """Read a finalized minute under the requested replay cut and policy."""
    return await build_wallet_pnl_series(
        repo,
        _WALLET,
        "live",
        _minute(6),
        _minute(6),
        "1m",
        _minute(9),
        valuation_ccy=valuation_ccy,
        policy=policy,
        options=options,
    )


def _sealed_replay_options(
    watermarks: Mapping[str, int], digest: str | None = None, has_fill_gap: bool | None = None
) -> PnlSeriesReplayOptions:
    """Use a saved generation's raw coordinates and manifest in a chunk read."""
    return PnlSeriesReplayOptions(
        baseline_watermarks={},
        frozen_execution_watermarks=watermarks,
        frozen_execution_manifest_digest=digest,
        frozen_has_fill_gap=has_fill_gap,
    )


async def _probe(
    repo: _ReplayRepo, baseline: Mapping[str, int], previous_minute: int | None = 7
) -> PnlSeriesReplayMetadata:
    """Return detector metadata without changing the certified series."""
    result = await _read_series(
        repo,
        PnlSeriesReplayOptions(
            baseline_watermarks=baseline,
            previous_replay_as_of=None if previous_minute is None else _minute(previous_minute),
        ),
    )
    assert result.replay_metadata is not None
    return result.replay_metadata


def _add_backdated_execution(repo: _ReplayRepo) -> None:
    """Add a later raw coordinate with an event time before the finalized tip."""
    repo._executions.append(fixtures._exec_row(fixtures._I1, 5, 4, "buy", 1.0, 100.0, 0.0, "USD"))
    repo._execution_watermarks = {"kraken": 5}


def test_manifest_digest_is_stable_for_order_and_seals_every_identity_field() -> None:
    """Hash the certified ledger identities rather than iteration order.

    Given: A correction and alternatives differing in one identity component.
    When: Each singleton and both orders of the combined manifest are hashed.
    Then: Every identity change alters the digest and reordering preserves it.
    """
    correction = fixtures._applied_annulment()
    changed: tuple[PnlTimelineAppliedAnnulment, ...] = (
        {**correction, "public_id": "different-annulment"},
        {**correction, "target_execution_public_id": "different-execution"},
        {**correction, "target_execution_digest": "cd" * 32},
        {**correction, "exchange": "walutomat"},
        {**correction, "scope_sequence": 5},
    )
    digest = _execution_manifest_digest((correction,))
    assert all(_execution_manifest_digest((other,)) != digest for other in changed)
    assert _execution_manifest_digest((correction, *changed)) == _execution_manifest_digest(
        (*reversed(changed), correction)
    )


@pytest.mark.parametrize(
    ("known_minute", "refuse_previous_cut", "affected_minute"),
    [(8, False, 2), (5, False, 8), (8, True, 2)],
)
async def test_retired_tail_keeps_the_detector_cut_and_only_new_annulment_rewinds(
    known_minute: int, refuse_previous_cut: bool, affected_minute: int
) -> None:
    """Preserve processed raw coordinates independently of the effective sample prefix.

    Given: A certified raw tail is annulled and a later fill is beyond the tip.
    When: The detector compares the manifest with the prior admitted cut.
    Then: The raw watermark survives and only new or unprovable corrections rewind.
    """
    repo = _ReplayRepo(known_minute, refuse_previous_cut=refuse_previous_cut)
    metadata = await _probe(repo, {"kraken": 3})
    assert metadata.max_scope_sequence_by_exchange == {"kraken": 1}
    assert metadata.detection_watermarks == {"kraken": 3}
    assert metadata.earliest_affected_minute == _minute(affected_minute)
    assert repo.previous_cuts == [(_WALLET, "live", {"kraken": 3}, _minute(7))]


async def test_no_previous_manifest_read_for_correction_outside_previous_cut() -> None:
    """Unprocessed corrections require no rewind or historical proof query.

    Given: The annulled sequence is above the previous detector watermark.
    When: The snapshotter probes its next evidence horizon.
    Then: It keeps the old cut and skips the historical manifest comparison.
    """
    repo = _ReplayRepo(8)
    metadata = await _probe(repo, {"kraken": 1})
    assert metadata.detection_watermarks == {"kraken": 1}
    assert metadata.earliest_affected_minute == _minute(7)
    assert repo.previous_cuts == []


async def test_annulment_already_recorded_by_the_anchor_does_not_rewind() -> None:
    """An unchanged opening audit can certify without a suffix manifest comparison.

    Given: The activation anchor already records the correction in its audit.
    When: A durable replay probe certifies the same manifest.
    Then: The detector leaves the opening alone and skips the prior suffix read.
    """
    repo = _ReplayRepo(8)
    repo._anchor = fixtures._anchor_recording(
        {"kraken": 3}, [fixtures._test_anchor_annulment(scope_sequence=3)]
    )
    metadata = await _probe(repo, {"kraken": 3})
    assert metadata.detection_watermarks == {"kraken": 3}
    assert metadata.earliest_affected_minute == _minute(8)
    assert repo.previous_cuts == []


async def test_annulment_without_any_new_execution_still_rewinds() -> None:
    """An appended correction can invalidate history by itself.

    Given: A newly visible annulment and no execution above the prior cut.
    When: The snapshotter probes the current certified manifest.
    Then: History rewinds to the first post-activation minute.
    """
    repo = _ReplayRepo(8)
    repo._executions = [row for row in repo._executions if row["scope_sequence"] < 4]
    repo._execution_watermarks = {"kraken": 3}
    metadata = await _probe(repo, {"kraken": 3})
    assert metadata.earliest_affected_minute == _minute(2)


async def test_probed_detector_can_move_to_a_new_finalized_sequence() -> None:
    """The probe consumes advances backed by effective, certified executions.

    Given: An empty previous cut and a certified prefix with future-effective rows.
    When: The snapshotter probes a finalized minute.
    Then: The detector advances through the eligible fill and reports its boundary.
    """
    repo = _ReplayRepo(8)
    metadata = await _probe(repo, {})
    assert metadata.max_scope_sequence_by_exchange == {"kraken": 1}
    assert metadata.detection_watermarks == {"kraken": 1}
    assert metadata.earliest_affected_minute == _minute(3)
    assert repo.previous_cuts == []


async def test_detector_cut_beyond_raw_certificate_refuses_replay() -> None:
    """A genuinely regressing certificate cannot be hidden by retaining old metadata.

    Given: The prior detector includes a coordinate above the certified ledger.
    When: The snapshotter tries to compute the next detector watermark.
    Then: It refuses the new generation without reading a historical manifest.
    """
    repo = _ReplayRepo(8)
    with pytest.raises(PnlAnchorEvidenceError, match="exceeds its certified physical prefix"):
        await _probe(repo, {"kraken": 5})
    assert repo.previous_cuts == []


async def test_previous_horizon_cannot_be_after_the_probe_horizon() -> None:
    """Durable comparison always looks backwards in knowledge time.

    Given: The supplied previous horizon is later than the current probe horizon.
    When: The snapshotter requests durable replay metadata.
    Then: It rejects the horizon order before reading the previous manifest.
    """
    repo = _ReplayRepo(8)
    with pytest.raises(ValueError, match="previous replay horizon"):
        await _probe(repo, {"kraken": 3}, previous_minute=10)
    assert repo.previous_cuts == []


async def test_unadorned_series_replay_still_reports_the_old_effective_cut() -> None:
    """Ordinary series callers need no durable comparison or additional read.

    Given: An ordinary series call with a baseline and no prior replay horizon.
    When: The series reports replay metadata for an annulled effective prefix.
    Then: It returns the effective watermark and makes no prior-cut query.
    """
    repo = _ReplayRepo(8)
    metadata = await _probe(repo, {"kraken": 3}, previous_minute=None)
    assert metadata.max_scope_sequence_by_exchange == {"kraken": 1}
    assert metadata.earliest_affected_minute == _minute(8)
    assert metadata.detection_watermarks is None
    assert repo.previous_cuts == []


async def test_resumed_series_reuses_full_raw_cut_despite_a_backdated_execution() -> None:
    """A later execution cannot enter a sealed generation under its previous clock.

    Given: A sealed raw cut and a later execution with an earlier event timestamp.
    When: A chunk resumes at the saved horizon and coordinates.
    Then: The identical raw cut is recertified and the later coordinate is absent.
    """
    repo = _ReplayRepo(5)
    initial = await _probe(repo, {}, previous_minute=None)
    watermarks = initial.raw_execution_watermarks
    assert watermarks is not None and watermarks == {"kraken": 4}
    assert initial.max_scope_sequence_by_exchange == {"kraken": 1}
    assert initial.execution_manifest_digest is not None
    _add_backdated_execution(repo)

    resumed = await _read_series(
        repo, _sealed_replay_options(watermarks, initial.execution_manifest_digest)
    )

    assert resumed.replay_metadata == initial
    assert repo.previous_cuts == [(_WALLET, "live", {"kraken": 4}, _minute(9))]
    assert repo.execution_prefix_intents == [_minute(9)]
    assert [row["scope_sequence"] for row in repo.fill_gap_prefixes[-1]["executions"]] == [
        1,
        2,
        4,
    ]


async def test_empty_frozen_cut_does_not_capture_the_current_ledger() -> None:
    """A covered empty prefix has the same replay semantics as any other cut.

    Given: A captured empty raw prefix and its canonical manifest digest.
    When: The next chunk replays against the empty cut.
    Then: It certifies exactly that cut without discovering the current ledger.
    """
    repo = _ReplayRepo(5)
    empty_manifest = hashlib.sha256(b"[]").hexdigest()
    resumed = await _read_series(repo, _sealed_replay_options({}, empty_manifest))
    assert resumed.replay_metadata is not None
    assert resumed.replay_metadata.raw_execution_watermarks == {}
    assert resumed.replay_metadata.execution_manifest_digest == empty_manifest
    assert repo.previous_cuts == [(_WALLET, "live", {}, _minute(9))]
    assert repo.execution_prefix_intents == []


async def test_frozen_cut_refuses_mismatched_repository_certificate() -> None:
    """A replay cannot proceed with a certificate for other raw coordinates.

    Given: The repository returns raw coordinates different from the saved cut.
    When: A resumed chunk checks its certificate.
    Then: The raw mismatch is rejected before manifest or fill-gap evaluation.
    """
    repo = _ReplayRepo(5)
    repo.reported_cut = {"kraken": 5}
    with pytest.raises(PnlAnchorEvidenceError, match="did not match the captured cut"):
        await _read_series(repo, _sealed_replay_options({"kraken": 4}, "0" * 64))
    assert repo.execution_prefix_intents == []
    assert repo.fill_gap_prefixes == []


async def test_newly_visible_manifest_invalidates_the_same_raw_cut() -> None:
    """A changed effective fold is a distinct signal to start a new generation.

    Given: An annulment becomes historically visible for an execution in the cut.
    When: The same physical coordinates are recertified with the saved digest.
    Then: A dedicated error signals the changed fold before the chunk is valued.
    """
    repo = _ReplayRepo(10)
    repo._executions.insert(
        2, fixtures._exec_row(fixtures._I1, 3, 7, "buy", 1.0, 100.0, 0.0, "USD")
    )
    initial = await _probe(repo, {}, previous_minute=None)
    watermarks = initial.raw_execution_watermarks
    assert watermarks is not None and watermarks == {"kraken": 4}
    assert initial.execution_manifest_digest == hashlib.sha256(b"[]").hexdigest()
    repo._executions.pop(2)
    repo.apply_annulments([fixtures._applied_annulment(scope_sequence=3)], known_from=_minute(8))

    with pytest.raises(
        PnlFrozenReplayEvidenceChangedError, match="frozen execution manifest changed"
    ):
        await _read_series(
            repo, _sealed_replay_options(watermarks, initial.execution_manifest_digest)
        )

    assert len(repo.fill_gap_prefixes) == 1
    assert repo.previous_cuts == [(_WALLET, "live", {"kraken": 4}, _minute(9))]


async def test_frozen_manifest_requires_a_saved_raw_cut() -> None:
    """A partial certificate cannot be used to infer which physical rows to reopen.

    Given: A saved manifest digest without matching captured raw coordinates.
    When: A chunk requests replay with that partial certificate.
    Then: The series rejects the options before loading the anchor.
    """
    repo = _ReplayRepo(5)
    with pytest.raises(ValueError, match="manifest requires its captured raw cut"):
        await _read_series(
            repo,
            PnlSeriesReplayOptions(frozen_execution_manifest_digest="0" * 64),
        )
    assert repo.anchor_calls == []


async def test_frozen_fill_gap_requires_a_saved_raw_cut_even_when_false() -> None:
    """An explicitly gap-free witness is also specific to its physical ledger cut.

    Given: A saved false gap assessment with no raw execution cut.
    When: The series validates the frozen replay options.
    Then: It rejects the assessment before loading the anchor.
    """
    repo = _ReplayRepo(5)
    with pytest.raises(ValueError, match="fill-gap assessment requires its captured raw cut"):
        await _read_series(repo, PnlSeriesReplayOptions(frozen_has_fill_gap=False))
    assert repo.anchor_calls == []


@pytest.mark.parametrize("saved_gap", [False, True])
async def test_resume_does_not_recapture_the_venue_fill_gap(saved_gap: bool) -> None:
    """Newly observable fills cannot change the trust classification of a sealed generation.

    Given: A saved gap assessment and an opposite later repository assessment.
    When: A chunk resumes with the original raw cut and certified gap flag.
    Then: Coverage uses the saved flag and the repository sees one gap query.
    """
    repo = _ReplayRepo(5)
    repo._has_fill_gap = saved_gap
    initial = await _probe(repo, {}, previous_minute=None)
    watermarks = initial.raw_execution_watermarks
    assert watermarks is not None
    assert initial.has_fill_gap is saved_gap
    repo._has_fill_gap = not saved_gap

    resumed = await _read_series(
        repo,
        replace(
            _sealed_replay_options(watermarks, initial.execution_manifest_digest, saved_gap),
            verify_replay_probe=True,
        ),
    )

    assert resumed.replay_metadata is not None
    assert resumed.replay_metadata.has_fill_gap is saved_gap
    (point,) = resumed.points
    assert (
        "fill_evidence_gap" in {entry.reason for entry in point.incompleteness_reasons}
    ) is saved_gap
    assert len(repo.fill_gap_calls) == 1
    assert repo.execution_prefix_intents == [_minute(9)]


@pytest.mark.parametrize("change", ["coordinates", "annulment"])
async def test_probe_rejects_ledger_change_between_capture_and_fill_analysis(
    monkeypatch: pytest.MonkeyPatch, change: Literal["coordinates", "annulment"]
) -> None:
    """Do not persist a fill-gap verdict from a mismatched execution snapshot.

    Given: A matching execution and fill commit during the scope gap await, or
        a newly visible annulment changes the effective fold at the same raw cut.
    When: The admission probe observes a gap against its earlier execution cut.
    Then: The probe rejects the mixed evidence and a stable retry sees no gap.
    """
    repo = _ReplayRepo(10)
    repo._executions.insert(
        2, fixtures._exec_row(fixtures._I1, 3, 7, "buy", 1.0, 100.0, 0.0, "USD")
    )
    original_gap = repo.pnl_timeline_scope_has_fill_gap

    async def gap_during_commit(
        wallet_public_id: str,
        mode: str,
        as_of: datetime | None,
        prefix: PnlTimelineExecutionPrefix,
    ) -> bool:
        """Expose a simulated committed ledger change while observing venue witnesses."""
        if change == "coordinates":
            _add_backdated_execution(repo)
        else:
            repo._executions.pop(2)
            repo.apply_annulments(
                [fixtures._applied_annulment(scope_sequence=3)], known_from=_minute(8)
            )
        repo._has_fill_gap = True
        return await original_gap(wallet_public_id, mode, as_of, prefix)

    options = PnlSeriesReplayOptions(baseline_watermarks={}, verify_replay_probe=True)
    with monkeypatch.context() as patch:
        patch.setattr(repo, "pnl_timeline_scope_has_fill_gap", gap_during_commit)
        with pytest.raises(PnlFrozenReplayEvidenceChangedError, match="during replay admission"):
            await _read_series(repo, options)

    assert len(repo.fill_gap_calls) == 1
    assert repo.execution_prefix_intents == [_minute(9), _minute(9)]
    repo._has_fill_gap = False

    retry = await _read_series(repo, options)

    assert retry.replay_metadata is not None
    assert retry.replay_metadata.has_fill_gap is False
    assert len(repo.fill_gap_calls) == 2


async def test_probe_rejects_anchor_creation_before_recertifying_the_cut() -> None:
    """An unconfirmed admission probe cannot create an activation anchor.

    Given: No existing anchor and a probe with anchor creation enabled.
    When: The series validates the probe's read policy.
    Then: It rejects the options before reading or creating an anchor.
    """
    repo = fixtures.FakeRepo(has_anchor=False)
    with pytest.raises(ValueError, match="requires a read-only historical policy"):
        await _read_series(
            repo,
            PnlSeriesReplayOptions(verify_replay_probe=True),
            policy=PnlSeriesReadPolicy(),
        )
    assert repo.anchor_calls == []
    assert repo.anchor_record_calls == []


@pytest.mark.parametrize(
    "policy",
    [
        PnlSeriesReadPolicy(),
        PnlSeriesReadPolicy(allow_anchor_creation=False, current_truth=True),
        PnlSeriesReadPolicy(allow_anchor_creation=False, current_truth_horizon=True),
    ],
)
async def test_frozen_cut_requires_historical_read_only_policy(policy: PnlSeriesReadPolicy) -> None:
    """Reject a mutable or current-truth replay before loading the anchor.

    Given: Frozen coordinates paired with anchor creation or current-truth access.
    When: The series validates the replay policy.
    Then: It rejects the incompatible policy before reading the anchor.
    """
    repo = _ReplayRepo(5)
    with pytest.raises(ValueError, match="requires a read-only historical policy"):
        await _read_series(repo, _sealed_replay_options({"kraken": 4}), policy=policy)
    assert repo.anchor_calls == []


async def test_durable_detector_rewinds_to_regression_shadow_before_its_clamped_time() -> None:
    """The old point is untrusted even if its regression ends beyond this chunk.

    Given: A new execution at minute four that clamps to minute eight.
    When: A durable probe values minute six and derives the rewind boundary.
    Then: Minute six is withheld and the correction rewinds to minute four.
    """
    repo = _ReplayRepo(5)
    _add_backdated_execution(repo)
    series = await _read_series(
        repo,
        PnlSeriesReplayOptions(baseline_watermarks={"kraken": 4}, previous_replay_as_of=_minute(7)),
    )
    metadata = series.replay_metadata
    assert metadata is not None
    assert metadata.earliest_affected_minute == _minute(4)
    assert metadata.max_scope_sequence_by_exchange == {"kraken": 1}
    assert metadata.detection_watermarks == {"kraken": 4}
    assert metadata.raw_execution_watermarks == {"kraken": 5}
    (point,) = series.points
    assert point.point_time == _minute(6)
    assert point.realized_pnl is None
    assert "scope_order_regression" in {reason.reason for reason in point.incompleteness_reasons}


async def test_current_equity_reads_require_reconciled_rows_from_the_repository() -> None:
    """The equity overlay asks the same sample query to enforce durable repair boundaries.

    Given: A current-truth series request with an existing activation anchor.
    When: The service loads persisted equity samples for that scope.
    Then: The sample query requires complete and reconciled rows.
    """
    repo = _ReplayRepo(5)
    await _read_series(
        repo,
        PnlSeriesReplayOptions(baseline_watermarks={}),
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False, current_truth=True),
    )
    ((query, _, _, status),) = repo.sample_calls
    assert query.require_reconciled
    assert status == "complete"


@pytest.mark.parametrize("currency", ["USD", "PLN"])
@pytest.mark.parametrize("reason", ["fill_evidence_gap", "late_pre_activation_execution"])
async def test_current_global_uncertainty_withholds_previously_complete_equity(
    currency: str, reason: str
) -> None:
    """Withhold stale equity whenever current accounting evidence is globally incomplete.

    Given: Stored complete USD equity and a new gap or pre-activation execution.
    When: The viewer requests a current USD or converted PLN timeline.
    Then: The accounting reason is reported and no stored equity is served.
    """
    executions = (
        [fixtures._exec_row(fixtures._I1, 1, 0, "buy", 1.0, 100.0, 0.0, "USD")]
        if reason == "late_pre_activation_execution"
        else []
    )
    repo = fixtures.FakeRepo(
        anchor=fixtures._empty_anchor(_WALLET, "live", currency, point_time=_minute(1)),
        executions=executions,
        has_fill_gap=reason == "fill_evidence_gap",
        fx_rows=[fixtures._fx_row("PLN", "USD", 6, 0.25)],
    )
    repo.load_samples([fixtures._sample_row(_minute(6))])

    result = await _read_series(
        repo,
        PnlSeriesReplayOptions(),
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False, current_truth=True),
        valuation_ccy=currency,
    )

    (point,) = result.points
    assert reason in {entry.reason for entry in point.incompleteness_reasons}
    assert result.equity_overlay_at(_minute(6)).equity is None
    assert not result.equity_coverage.sampled
    assert not repo.sample_calls
