"""Financial continuation invariants independent of sample presence or a process lifetime."""

from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import cast

import pytest

from snapper.data.pnl_correction_state import PnlCorrectionChunk
from snapper.data.pnl_correction_state import PnlCorrectionEdit
from snapper.data.pnl_correction_state import PnlCorrectionEvidence
from snapper.data.pnl_correction_state import PnlCorrectionFrontier
from snapper.data.pnl_correction_state import PnlCorrectionKey
from snapper.data.pnl_correction_state import PnlCorrectionRequest
from snapper.data.pnl_correction_state import advance_correction_frontier
from snapper.data.pnl_correction_state import correction_watermarks
from snapper.data.pnl_correction_state import start_correction_frontier
from snapper.data.repository_types import PNL_SAMPLE_CALC_VERSION
from tests.data import test_pnl_timeline_sample_repository as samples

KEY = PnlCorrectionKey(samples._WALLET, "live", "USD", samples._EPOCH, PNL_SAMPLE_CALC_VERSION)
REQUEST = PnlCorrectionRequest(
    samples._M1, samples._M3, samples._M5, {"kraken": 1}, {"kraken": 5}, 1250.0
)


def _chunk(
    end: datetime,
    edits: tuple[PnlCorrectionEdit, ...],
    peak: float | None,
    *,
    start: datetime = samples._M1,
) -> PnlCorrectionChunk:
    """Build a bounded pure-state witness with an explicit complete active set."""
    evidence = PnlCorrectionEvidence(KEY, start, end, (), 1250.0)
    return PnlCorrectionChunk(end, edits, peak, evidence)


def _frontier() -> PnlCorrectionFrontier:
    """Start a three-minute correction with an unadvanced durable baseline."""
    return start_correction_frontier(KEY, REQUEST, None)


def test_zero_write_progress_is_explicit_and_baseline_waits_for_completion() -> None:
    """Skipped sample minutes advance durable control without publishing early truth."""
    initial = _frontier()
    partial = advance_correction_frontier(initial, _chunk(samples._M1, (), None))
    assert partial.next_minute == samples._M2
    assert partial.baseline_watermarks == {"kraken": 1}
    assert partial.pending_watermarks == {"kraken": 5}
    assert partial.prior_peak is None
    done = advance_correction_frontier(partial, _chunk(samples._M3, (), 1300.0, start=samples._M2))
    assert done.next_minute is None
    assert done.baseline_watermarks == {"kraken": 5}
    assert done.revision == initial.revision + 2
    assert done.generation == initial.generation
    assert initial.next_minute == samples._M1


def test_new_invalidation_rewinds_earlier_and_preserves_unfinished_suffix() -> None:
    """An earlier late fill starts a new generation without dropping pending work."""
    partial = advance_correction_frontier(_frontier(), _chunk(samples._M2, (), 1300.0))
    invalidation = replace(REQUEST, tip=samples._M2, prior_peak=1200.0)
    rewound = start_correction_frontier(KEY, invalidation, partial)
    assert rewound.next_minute == samples._M1
    assert rewound.tip == samples._M3
    assert rewound.baseline_watermarks == {"kraken": 1}
    assert rewound.prior_peak == 1200.0
    assert rewound.generation == partial.generation + 1
    assert rewound.revision == partial.revision + 1


def test_later_invalidation_keeps_earlier_pending_boundary_and_its_peak() -> None:
    """A later event cannot skip unfinished minutes or substitute their causal peak."""
    partial = advance_correction_frontier(_frontier(), _chunk(samples._M1, (), 1300.0))
    request = replace(REQUEST, start=samples._M3, tip=samples._M4, prior_peak=9999.0)
    continued = start_correction_frontier(KEY, request, partial)
    assert continued.next_minute == samples._M2
    assert continued.prior_peak == 1300.0
    assert continued.tip == samples._M4


def test_completed_frontier_can_be_invalidated_without_losing_published_baseline() -> None:
    """The completed row persists as a CAS target when a late fill arrives at the tip."""
    done = advance_correction_frontier(_frontier(), _chunk(samples._M3, (), None))
    request = replace(REQUEST, pending_watermarks={"kraken": 6})
    renewed = start_correction_frontier(KEY, request, done)
    assert renewed.baseline_watermarks == {"kraken": 5}
    assert renewed.pending_watermarks == {"kraken": 6}
    assert renewed.next_minute == samples._M1


def test_input_watermark_mutation_cannot_change_a_created_checkpoint() -> None:
    """A caller retaining a mutable input dictionary cannot rewrite committed control."""
    baseline, pending = {"kraken": 1}, {"kraken": 5}
    frontier = start_correction_frontier(
        KEY, replace(REQUEST, baseline_watermarks=baseline, pending_watermarks=pending), None
    )
    baseline["kraken"] = 100
    pending["kraken"] = 200
    assert frontier.baseline_watermarks == {"kraken": 1}
    assert frontier.pending_watermarks == {"kraken": 5}


@pytest.mark.parametrize(
    "candidate",
    [
        replace(REQUEST, start=samples._M1.replace(tzinfo=None)),
        replace(REQUEST, tip=samples._M3.replace(second=1)),
        replace(REQUEST, start=samples._M1.replace(microsecond=1)),
        replace(REQUEST, start=samples._M1.replace(tzinfo=timezone(timedelta(seconds=30)))),
        replace(REQUEST, start=samples._M4),
        replace(REQUEST, as_of=samples._M5.replace(tzinfo=None)),
        replace(REQUEST, as_of=samples._M2),
        replace(REQUEST, prior_peak=float("nan")),
        replace(REQUEST, prior_peak=float("inf")),
        replace(REQUEST, pending_watermarks={}),
        replace(REQUEST, baseline_watermarks={"kraken": 6}),
    ],
)
def test_invalid_generation_evidence_is_refused(candidate: PnlCorrectionRequest) -> None:
    """Malformed clocks, ranges, peaks and regressing prefixes never create control."""
    with pytest.raises(ValueError):
        start_correction_frontier(KEY, candidate, None)


@pytest.mark.parametrize(
    "values", [{"": 1}, {" kraken": 1}, {"kraken": -1}, {"kraken": True}, {"kraken": 1.5}]
)
def test_invalid_watermarks_are_not_coerced(values: dict[str, object]) -> None:
    """A malformed sequence cannot silently become a valid progress watermark."""
    with pytest.raises(ValueError):
        correction_watermarks(cast(dict[str, int], values))


@pytest.mark.parametrize("wrong_scope", [True, False])
def test_rewind_cannot_cross_scope_or_knowledge_horizon(wrong_scope: bool) -> None:
    """Rewind is a new generation of the same scope using nonregressing evidence."""
    key = replace(KEY, epoch_public_id=samples._OTHER_EPOCH) if wrong_scope else KEY
    request = REQUEST if wrong_scope else replace(REQUEST, as_of=samples._M4)
    with pytest.raises(ValueError):
        start_correction_frontier(key, request, _frontier())


def test_new_prefix_cannot_forget_a_pending_generation() -> None:
    """An incomplete generation's observed execution prefix must remain represented."""
    with pytest.raises(ValueError):
        start_correction_frontier(
            KEY, replace(REQUEST, pending_watermarks={"kraken": 4}), _frontier()
        )


def test_chunk_minute_limit_is_inclusive() -> None:
    """One day is accepted while a 1,441-minute transaction is rejected."""
    tip = samples._M1 + timedelta(days=2)
    frontier = start_correction_frontier(
        KEY, replace(REQUEST, tip=tip, as_of=tip + timedelta(minutes=1)), None
    )
    end = samples._M1 + timedelta(minutes=1439)
    assert advance_correction_frontier(frontier, _chunk(end, (), None)).next_minute
    with pytest.raises(ValueError, match="minute bound"):
        advance_correction_frontier(frontier, _chunk(end + timedelta(minutes=1), (), None))


@pytest.mark.parametrize(
    "chunk",
    [
        _chunk(samples._T0, (), None),
        _chunk(samples._M4, (), None),
        _chunk(samples._M1, (), float("inf")),
        _chunk(
            samples._M1, (PnlCorrectionEdit(samples._M2, None, samples._complete_sample()),), None
        ),
        _chunk(samples._M1, (PnlCorrectionEdit(samples._M1, None, None),), None),
        _chunk(
            samples._M1,
            (PnlCorrectionEdit(samples._M1, None, samples._complete_sample()),) * 2,
            None,
        ),
    ],
)
def test_invalid_chunk_cannot_advance_any_control(chunk: PnlCorrectionChunk) -> None:
    """Out-of-range, duplicate and ambiguous edits never manufacture progress."""
    with pytest.raises(ValueError):
        advance_correction_frontier(_frontier(), chunk)


def test_completed_generation_cannot_be_advanced_again() -> None:
    """Completion is durable state rather than permission to replay the same chunk."""
    done = advance_correction_frontier(_frontier(), _chunk(samples._M3, (), None))
    with pytest.raises(ValueError):
        advance_correction_frontier(done, _chunk(samples._M3, (), None))


@pytest.mark.parametrize("field", ["key", "start", "end"])
def test_evidence_must_cover_the_exact_scope_and_admitted_window(field: str) -> None:
    """A subset, wider window or different scope is not a complete presence witness.

    Args:
        field: One independently mismatched evidence identity.
    """
    chunk = _chunk(samples._M3, (), None)
    evidence = chunk.evidence
    if field == "key":
        evidence = replace(evidence, key=replace(KEY, epoch_public_id=samples._OTHER_EPOCH))
    elif field == "start":
        evidence = replace(evidence, start=samples._M2)
    else:
        evidence = replace(evidence, end=samples._M2)
    with pytest.raises(ValueError, match="complete admitted chunk"):
        advance_correction_frontier(_frontier(), replace(chunk, evidence=evidence))


@pytest.mark.parametrize("has_edit", [False, True])
def test_withheld_evidence_cannot_be_outside_the_chunk_or_also_mutated(has_edit: bool) -> None:
    """A preserved withheld minute has exactly one honest disposition.

    Args:
        has_edit: Whether a contradictory financial edit is present.
    """
    edits = (PnlCorrectionEdit(samples._M1, None, samples._complete_sample()),) if has_edit else ()
    chunk = _chunk(samples._M1, edits, None)
    minute = samples._M1 if has_edit else samples._M2
    with pytest.raises(ValueError, match="have no edit"):
        advance_correction_frontier(_frontier(), replace(chunk, withheld_minutes=(minute,)))


def test_earliest_withheld_repair_survives_later_chunks_and_scan_completion() -> None:
    """Later progress cannot replace the first unresolved minute or consume its baseline."""
    first = replace(_chunk(samples._M1, (), None), withheld_minutes=(samples._M1,))
    partial = advance_correction_frontier(_frontier(), first)
    last = replace(
        _chunk(samples._M3, (), None, start=samples._M2), withheld_minutes=(samples._M3,)
    )
    done = advance_correction_frontier(partial, last)
    assert done.repair_from == samples._M1
    assert done.next_minute is None
    assert done.baseline_watermarks == {"kraken": 1}


def test_rewind_retains_repair_until_explicitly_reconciled_through_the_tip() -> None:
    """Starting another generation cannot itself declare old withheld work repaired."""
    withheld = replace(_chunk(samples._M3, (), None), withheld_minutes=(samples._M1,))
    scanned = advance_correction_frontier(_frontier(), withheld)
    renewed = start_correction_frontier(KEY, REQUEST, scanned)
    assert renewed.repair_from == samples._M1
    first = replace(_chunk(samples._M1, (), None), reconciles_repair=True)
    partial = advance_correction_frontier(renewed, first)
    assert partial.repair_from == samples._M2
    assert partial.baseline_watermarks == {"kraken": 1}
    last = replace(_chunk(samples._M3, (), None, start=samples._M2), reconciles_repair=True)
    repaired = advance_correction_frontier(partial, last)
    assert repaired.repair_from is None
    assert repaired.baseline_watermarks == {"kraken": 5}
