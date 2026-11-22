"""Bit-exact shadow comparison tests for durable opening checkpoints."""

from dataclasses import replace
from datetime import datetime

import pytest

from snapper.application.portfolio import pnl_snapshotter_checkpoint_shadow as shadow_module
from snapper.application.portfolio.pnl_correction_planner import PlannedPnlCorrection
from snapper.application.portfolio.pnl_snapshotter_checkpoint_shadow import (
    PnlCheckpointShadowOutcome,
)
from snapper.application.portfolio.pnl_snapshotter_checkpoint_shadow import PnlCheckpointShadowSide
from snapper.application.portfolio.pnl_snapshotter_checkpoint_shadow import (
    PnlCheckpointShadowWindow,
)
from snapper.application.portfolio.pnl_snapshotter_checkpoint_shadow import (
    compare_checkpoint_shadow,
)
from snapper.application.portfolio.pnl_snapshotter_checkpoint_shadow import (
    compare_checkpoint_shadow_sides,
)
from snapper.data.pnl_correction_state import PnlCorrectionChunk
from snapper.data.pnl_correction_state import PnlCorrectionEvidence
from snapper.data.pnl_correction_state import PnlCorrectionFrontier
from tests.application.portfolio import test_pnl_snapshotter as fixtures


def _plan(frontier: PnlCorrectionFrontier) -> PlannedPnlCorrection:
    """Build one empty but fully bound correction plan."""
    start = frontier.next_minute
    assert isinstance(start, datetime)
    end = frontier.tip
    return PlannedPnlCorrection(
        chunk=PnlCorrectionChunk(
            end=end,
            edits=(),
            prior_peak=frontier.prior_peak,
            evidence=PnlCorrectionEvidence(
                key=frontier.key,
                start=start,
                end=end,
                versions=(),
                prior_peak=frontier.prior_peak,
            ),
        ),
        withheld=(),
    )


def _frontier() -> PnlCorrectionFrontier:
    """Return one certified pending generation."""
    repo = fixtures._FakeRepo()
    repo.seed_completed_replay(fixtures._minute(2))
    return replace(
        repo.frontier(),
        next_minute=fixtures._minute(3),
        tip=fixtures._minute(3),
    )


def test_shadow_match_is_nan_aware_and_bit_exact() -> None:
    """Compare equal NaNs and distinguish equal-looking float values.

    Given: Anchor and resumed results with identical NaN payloads and plans.
    When: The shadow comparator runs and a signed zero is then introduced.
    Then: NaNs compare equal while negative zero differs at its exact point field.
    """
    frontier = _frontier()
    anchor = fixtures._series(fixtures._minute(3), fixtures._minute(3))
    point = replace(anchor.points[0], realized_pnl=float("nan"))
    anchor = replace(anchor, points=(point,))
    resumed = replace(anchor, points=(replace(point, realized_pnl=float("nan")),))
    plan = _plan(frontier)

    assert compare_checkpoint_shadow(anchor, plan, resumed, plan) is None
    divergent = replace(anchor, points=(replace(point, unrealized_pnl=-0.0),))
    mismatch = compare_checkpoint_shadow(anchor, plan, divergent, plan)
    assert mismatch is not None
    assert mismatch.point_time == fixtures._minute(3)
    assert mismatch.field == "points.unrealized_pnl"


def test_shadow_mismatch_names_first_result_and_plan_field() -> None:
    """Report replay metadata before a later planned-chunk divergence.

    Given: A resumed result with a different earliest affected minute and plan peak.
    When: The comparator scans fields in stable order.
    Then: Replay metadata is the first mismatch and the plan differs independently.
    """
    frontier = _frontier()
    anchor = fixtures._series(
        fixtures._minute(3), fixtures._minute(3), metadata=fixtures._meta(seq=4)
    )
    metadata = anchor.replay_metadata
    assert metadata is not None
    resumed = replace(
        anchor,
        replay_metadata=replace(metadata, earliest_affected_minute=fixtures._minute(2)),
    )
    plan = _plan(frontier)
    other_plan = replace(plan, chunk=replace(plan.chunk, prior_peak=12.0))

    mismatch = compare_checkpoint_shadow(anchor, plan, resumed, other_plan)
    assert mismatch is not None
    assert mismatch.field == "replay_metadata"
    plan_mismatch = compare_checkpoint_shadow(anchor, plan, anchor, other_plan)
    assert plan_mismatch is not None
    assert plan_mismatch.field == "chunk.prior_peak"


def test_shadow_reports_length_time_and_withheld_differences() -> None:
    """Locate non-value divergences across points and withheld diagnostics.

    Given: Results with different lengths and minute identities, and distinct plans.
    When: Each comparison runs independently.
    Then: The mismatch names length, point identity, or withheld diagnostics.
    """
    frontier = _frontier()
    plan = _plan(frontier)
    result = fixtures._series(fixtures._minute(3), fixtures._minute(3))
    empty = replace(result, points=())
    length = compare_checkpoint_shadow(result, plan, empty, plan)
    assert length is not None
    assert length.field == "points.length"
    assert shadow_module._first_available_point((), (), 0) is None
    moved = replace(result, points=(replace(result.points[0], point_time=fixtures._minute(4)),))
    instant = compare_checkpoint_shadow(result, plan, moved, plan)
    assert instant is not None
    assert instant.field == "points.point_time"
    withheld = fixtures.WithheldMinute(fixtures._minute(3), ("missing_mark",), ("missing_mark",))
    other = replace(plan, withheld=(withheld,))
    mismatch = compare_checkpoint_shadow(result, plan, result, other)
    assert mismatch is not None
    assert mismatch.field == "withheld"


def test_shadow_canonicalizes_retractions_and_rejects_unknown_values() -> None:
    """Handle a retraction edit and fail loudly on an unsupported comparison value.

    Given: A retraction-only plan and an opaque object.
    When: Plans compare and the canonicalizer receives the object.
    Then: Retractions compare cleanly and unsupported values raise TypeError.
    """
    frontier = _frontier()
    plan = _plan(frontier)
    retraction = fixtures.PnlCorrectionEdit(fixtures._minute(3), None, None)
    changed = replace(plan, chunk=replace(plan.chunk, edits=(retraction,)))
    assert (
        compare_checkpoint_shadow(
            fixtures._series(fixtures._minute(3), fixtures._minute(3)),
            changed,
            fixtures._series(fixtures._minute(3), fixtures._minute(3)),
            changed,
        )
        is None
    )
    with pytest.raises(TypeError, match="Unsupported checkpoint shadow value"):
        shadow_module._canonical(object())


@pytest.mark.parametrize(
    "override",
    [
        {"from_time": fixtures._minute(4)},
        {"to_time": fixtures._minute(4)},
        {"scope": ("other-wallet", "live", "USD", fixtures._anchor()["epoch_public_id"])},
        {"calc_version": "5C.5"},
    ],
)
def test_shadow_sides_mismatching_window_scope_or_calc_version_record_skip(
    override: dict[str, object],
) -> None:
    """Skip shadow comparison with ``window_mismatch`` when bounds, scope, or version differ.

    Given: Divergent anchor and resumed series paired with differing window identities.
    When: ``compare_checkpoint_shadow_sides`` checks window equality first.
    Then: It returns ``skipped_reason="window_mismatch"`` and ``mismatch=None``.

    Args:
        override: Single field override making the resumed window differ from anchor.
    """
    frontier = _frontier()
    plan = _plan(frontier)
    anchor = fixtures._series(fixtures._minute(3), fixtures._minute(3))
    divergent = replace(anchor, points=(replace(anchor.points[0], realized_pnl=999.0),))
    base_window = PnlCheckpointShadowWindow(
        from_time=fixtures._minute(3),
        to_time=fixtures._minute(3),
        scope=(
            frontier.key.wallet_public_id,
            frontier.key.mode,
            frontier.key.valuation_ccy,
            frontier.key.epoch_public_id,
        ),
        calc_version=frontier.key.calc_version,
    )
    resumed_window = replace(base_window, **override)
    outcome = compare_checkpoint_shadow_sides(
        PnlCheckpointShadowSide(base_window, anchor, plan),
        PnlCheckpointShadowSide(resumed_window, divergent, plan),
    )
    assert outcome == PnlCheckpointShadowOutcome(skipped_reason="window_mismatch", mismatch=None)
