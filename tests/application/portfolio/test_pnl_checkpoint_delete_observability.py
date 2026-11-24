"""Attribution helpers for P&L opening-checkpoint deletes and full recomputes."""

from dataclasses import replace
from datetime import datetime
from types import SimpleNamespace
from typing import cast

import pytest

from snapper.application.portfolio.pnl_opening_checkpoint_certificate import (
    opening_checkpoint_fence_source,
)
from snapper.application.portfolio.pnl_snapshotter import full_recompute_reason
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReplayMetadata
from snapper.data.pnl_correction_state import PnlCorrectionFrontier
from tests.application.portfolio import test_pnl_opening_checkpoint_certificate as cert


def _pending(frontier: PnlCorrectionFrontier) -> PnlSeriesReplayMetadata:
    """Build replay evidence that matches ``frontier`` on every compared field."""
    return cast(
        PnlSeriesReplayMetadata,
        SimpleNamespace(
            execution_manifest_digest=frontier.execution_manifest_digest,
            has_fill_gap=frontier.has_fill_gap,
        ),
    )


@pytest.mark.parametrize(
    ("window_start", "next_minute", "repair_from", "expected"),
    [
        (cert._M2, None, None, "window_start"),
        (cert._M2, cert._M2, cert._M2, "window_start"),
        (cert._M3, cert._M2, None, "next_minute"),
        (cert._M3, cert._M2, cert._M2, "next_minute"),
        (cert._M3, cert._M4, cert._M1, "repair_from"),
    ],
)
def test_fence_source_names_the_field_that_sets_the_fence(
    window_start: datetime,
    next_minute: datetime | None,
    repair_from: datetime | None,
    expected: str,
) -> None:
    """Attribute the opening-checkpoint fence to one frontier field.

    Given: A frontier whose window, pending and repair boundaries vary.
    When: The fence source is resolved.
    Then: The field holding the earliest minute is named, with ties going to
        ``window_start`` and then ``next_minute``.

    Args:
        window_start: Frontier window start.
        next_minute: Frontier pending minute, or ``None``.
        repair_from: Frontier repair boundary, or ``None``.
        expected: Field name expected to set the fence.
    """
    frontier = replace(
        cert._settled_frontier(),
        window_start=window_start,
        next_minute=next_minute,
        repair_from=repair_from,
    )

    assert opening_checkpoint_fence_source(frontier) == expected


@pytest.mark.parametrize(
    ("change", "venues", "expected"),
    [
        ({}, frozenset({"kraken"}), None),
        (
            {"raw_execution_watermarks": None},
            frozenset({"kraken"}),
            "raw_execution_watermarks_absent",
        ),
        ({"venue_event_watermarks": None}, frozenset({"kraken"}), "venue_event_watermarks_absent"),
        (
            {"execution_manifest_digest": "f" * 64},
            frozenset({"kraken"}),
            "execution_manifest_changed",
        ),
        ({"has_fill_gap": True}, frozenset({"kraken"}), "fill_gap_changed"),
        ({}, frozenset({"kraken", "walutomat"}), "expected_venues_changed"),
    ],
)
def test_full_recompute_reason_names_the_first_differing_evidence(
    change: dict[str, object],
    venues: frozenset[str],
    expected: str | None,
) -> None:
    """Name the stored evidence that forces a replay from the epoch anchor.

    Given: A stored frontier and this tick's evidence, equal except for one field.
    When: The full-recompute reason is resolved.
    Then: The differing field is named, and fully matching evidence allows an
        incremental replay.

    Args:
        change: Frontier fields that differ from this tick's evidence.
        venues: Venues the scope expects this tick.
        expected: Reason expected, or ``None`` for an incremental replay.
    """
    stored = cert._settled_frontier()
    pending = _pending(stored)
    frontier = replace(stored, **change)

    assert full_recompute_reason(frontier, pending, venues) == expected
