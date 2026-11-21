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
from snapper.data.pnl_correction_state import PnlCorrectionSampleVersion
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
    baseline, pending, raw = {"kraken": 1}, {"kraken": 5}, {"kraken": 7}
    witness = {"kraken:spot": 12}
    frontier = start_correction_frontier(
        KEY,
        replace(
            REQUEST,
            baseline_watermarks=baseline,
            pending_watermarks=pending,
            raw_execution_watermarks=raw,
            venue_event_watermarks=witness,
            fill_witness_digest="a" * 64,
        ),
        None,
    )
    baseline["kraken"] = 100
    pending["kraken"] = 200
    raw["kraken"] = 300
    witness["kraken:spot"] = 400
    assert frontier.baseline_watermarks == {"kraken": 1}
    assert frontier.pending_watermarks == {"kraken": 5}
    assert frontier.raw_execution_watermarks == {"kraken": 7}
    advanced = advance_correction_frontier(frontier, _chunk(samples._M1, (), None))
    assert advanced.raw_execution_watermarks == {"kraken": 7}
    assert advanced.raw_execution_watermarks is not frontier.raw_execution_watermarks
    assert advanced.venue_event_watermarks == {"kraken:spot": 12}
    assert advanced.venue_event_watermarks is not frontier.venue_event_watermarks
    assert advanced.fill_witness_digest == "a" * 64


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
        replace(REQUEST, raw_execution_watermarks={"kraken": True}),
        replace(REQUEST, execution_manifest_digest="a" * 63),
        replace(REQUEST, execution_manifest_digest="A" * 64),
        replace(REQUEST, execution_manifest_digest="z" * 64),
        replace(REQUEST, execution_manifest_digest=cast(str, 123)),
        replace(REQUEST, venue_event_watermarks={}),
        replace(REQUEST, fill_witness_digest="b" * 64),
        replace(REQUEST, venue_event_watermarks={}, fill_witness_digest="broken"),
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


def test_new_generation_cannot_forget_its_fill_witnesses() -> None:
    """A replacement generation must certify both parts of a previously sealed cut."""
    request = replace(REQUEST, venue_event_watermarks={}, fill_witness_digest="b" * 64)
    frontier = start_correction_frontier(KEY, request, None)
    with pytest.raises(ValueError, match="retain its certified fill-witness cut"):
        start_correction_frontier(KEY, REQUEST, frontier)


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
    with pytest.raises(ValueError, match="permit only observed-row retractions"):
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


@pytest.mark.parametrize("cut", [None, {}, {"kraken": 6}, {"walutomat": 8}])
def test_an_established_raw_execution_cut_cannot_be_forgotten(cut: dict[str, int] | None) -> None:
    """Reject a generation that discards certified execution evidence.

    Given a checkpoint with a nonempty physical execution prefix.
    When a successor omits the prefix, removes its venue or lowers its sequence.
    Then the transition raises a watermark validation error.
    """
    current = start_correction_frontier(
        KEY, replace(REQUEST, raw_execution_watermarks={"kraken": 7}), None
    )
    with pytest.raises(ValueError, match="raw execution watermarks|watermarks must not move"):
        start_correction_frontier(KEY, replace(REQUEST, raw_execution_watermarks=cut), current)


def test_a_new_generation_can_certify_a_longer_raw_execution_prefix() -> None:
    """Certify an extended execution prefix in a new generation.

    Given a checkpoint that records one venue's raw sequence and a legacy null manifest.
    When an explicit invalidation supplies a longer prefix and certifies its manifest.
    Then the generation advances with the new evidence and the original checkpoint is preserved.
    """
    current = start_correction_frontier(
        KEY, replace(REQUEST, raw_execution_watermarks={"kraken": 7}), None
    )
    request = replace(
        REQUEST,
        raw_execution_watermarks={"kraken": 10, "walutomat": 2},
        execution_manifest_digest="a" * 64,
    )
    renewed = start_correction_frontier(KEY, request, current)
    assert renewed.raw_execution_watermarks == {"kraken": 10, "walutomat": 2}
    assert renewed.generation == current.generation + 1
    assert current.raw_execution_watermarks == {"kraken": 7}
    assert renewed.execution_manifest_digest == "a" * 64
    assert current.execution_manifest_digest is None


def test_new_generation_requires_a_manifest_after_an_attested_cut() -> None:
    """Preserve the pinned manifest across chunks.

    Given a generation with a certified execution manifest.
    When it advances a chunk and a successor is proposed with or without a new manifest.
    Then advancement retains the digest and a successor requires a certified replacement.
    """
    request = replace(REQUEST, execution_manifest_digest="a" * 64)
    started = start_correction_frontier(KEY, request, None)
    partial = advance_correction_frontier(started, _chunk(samples._M1, (), None))
    assert partial.execution_manifest_digest == "a" * 64
    with pytest.raises(ValueError, match="retain its execution manifest digest"):
        start_correction_frontier(KEY, REQUEST, partial)
    renewed = start_correction_frontier(
        KEY, replace(request, execution_manifest_digest="b" * 64), partial
    )
    assert renewed.execution_manifest_digest == "b" * 64


@pytest.mark.parametrize("has_fill_gap", [False, True])
def test_certified_fill_gap_verdict_survives_chunk_advancement(has_fill_gap: bool) -> None:
    """Preserve the pinned fill-gap verdict across chunks.

    Given a generation with a verdict of true or false.
    When a chunk advances and a successor is proposed with an absent or updated verdict.
    Then the chunk retains its verdict and the successor requires an explicit verdict.
    """
    request = replace(REQUEST, has_fill_gap=has_fill_gap)
    started = start_correction_frontier(KEY, request, None)
    partial = advance_correction_frontier(started, _chunk(samples._M1, (), None))
    assert partial.has_fill_gap is has_fill_gap
    with pytest.raises(ValueError, match="retain its fill-gap verdict"):
        start_correction_frontier(KEY, REQUEST, partial)
    changed = replace(request, has_fill_gap=not has_fill_gap)
    renewed = start_correction_frontier(KEY, changed, partial)
    assert renewed.has_fill_gap is changed.has_fill_gap


@pytest.mark.parametrize(
    "venues",
    [
        cast(frozenset[str], ["kraken"]),
        frozenset(),
        frozenset({cast(str, 3)}),
        frozenset({""}),
        frozenset({"Kraken"}),
        frozenset({"krak en"}),
    ],
)
def test_certified_spot_venue_set_must_be_nonempty_and_canonical(venues: frozenset[str]) -> None:
    """Refuse a malformed spot basket denominator at generation start.

    Given a request with a malformed or empty claimed spot venue set.
    When the state transition attempts to start a generation.
    Then it rejects the invalid set before admitting any replay work.
    """
    with pytest.raises(ValueError, match="nonempty canonical spot venue set"):
        start_correction_frontier(KEY, replace(REQUEST, expected_venues=venues), None)


def test_certified_spot_venue_set_survives_chunk_advancement() -> None:
    """Keep the original spot venue denominator through every chunk.

    Given a generation whose basket has two certified spot venues.
    When a chunk advances and a successor attempts to omit or replace its spot venue set.
    Then the chunk retains both venues and the successor requires an explicit certified set.
    """
    venues = frozenset({"kraken", "walutomat"})
    request = replace(REQUEST, expected_venues=venues)
    started = start_correction_frontier(KEY, request, None)
    partial = advance_correction_frontier(started, _chunk(samples._M1, (), None))
    assert partial.expected_venues == venues
    with pytest.raises(ValueError, match="retain its certified spot venues"):
        start_correction_frontier(KEY, REQUEST, partial)
    replacement = frozenset({"kraken"})
    renewed = start_correction_frontier(KEY, replace(request, expected_venues=replacement), partial)
    assert renewed.expected_venues == replacement
    assert partial.expected_venues == venues


@pytest.mark.parametrize(
    ("withheld", "unrepairable"),
    [
        ((), (samples._M1,)),
        ((samples._M1,), (samples._M2,)),
        ((samples._M1,), (samples._M1, samples._M1)),
        ((samples._M1,), (samples._M1.replace(tzinfo=None),)),
    ],
)
def test_unrepairable_withheld_rejects_subset_window_duplicate_and_boundary_violations(
    withheld: tuple[datetime, ...],
    unrepairable: tuple[datetime, ...],
) -> None:
    """Refuse malformed, non-subset, out-of-window, or duplicate unrepairable minutes.

    Given: A one-minute frontier chunk ending at M1.
    When: unrepairable_withheld_minutes omits withheld_minutes, steps outside [M1..M1],
        duplicates a minute, or supplies a naive boundary.
    Then: advance_correction_frontier rejects the chunk with ValueError.
    """
    chunk = replace(
        _chunk(samples._M1, (), None),
        withheld_minutes=withheld,
        unrepairable_withheld_minutes=unrepairable,
    )
    with pytest.raises(ValueError):
        advance_correction_frontier(_frontier(), chunk)


@pytest.mark.parametrize(
    "case",
    [
        "retained_current",
        "retained_legacy",
        "retained_foreign",
        "wrong_expected_row",
        "duplicate_evidence_versions",
    ],
)
def test_unrepairable_withheld_refuses_forged_retained_active_versions(case: str) -> None:
    """Enforce R1 at the pure fence when active versions remain unretracted.

    Given: Chunk evidence holding an active current, legacy, foreign, or mismatched row at M1.
    When: unrepairable_withheld_minutes lists M1 without an exact retraction edit.
    Then: advance_correction_frontier raises ValueError before accepting the chunk.
    """
    row = samples._complete_sample(samples._M1)
    if case == "retained_legacy":
        row["calc_version"] = "5C.3"
    elif case == "retained_foreign":
        row["epoch_public_id"] = samples._OTHER_EPOCH
    version = PnlCorrectionSampleVersion(101, row)
    versions: tuple[PnlCorrectionSampleVersion, ...] = (version,)
    edits: tuple[PnlCorrectionEdit, ...] = ()
    if case == "wrong_expected_row":
        edits = (PnlCorrectionEdit(samples._M1, PnlCorrectionSampleVersion(999, row), None),)
    elif case == "duplicate_evidence_versions":
        versions = (version, PnlCorrectionSampleVersion(102, row))
        edits = (PnlCorrectionEdit(samples._M1, version, None),)
    evidence = PnlCorrectionEvidence(KEY, samples._M1, samples._M2, versions, None)
    chunk = PnlCorrectionChunk(
        end=samples._M2,
        edits=edits,
        prior_peak=None,
        evidence=evidence,
        withheld_minutes=(samples._M1,),
        unrepairable_withheld_minutes=(samples._M1,),
    )
    with pytest.raises(ValueError, match="cannot retain an active sample row"):
        advance_correction_frontier(_frontier(), chunk)


def test_unrepairable_withheld_accepts_absent_and_exact_retraction_and_settles() -> None:
    """Accept exempt minutes when absent or explicitly retracted and clear repair_from.

    Given: M1 absent and M2 carrying an active version explicitly retracted in chunk.edits.
    When: Both M1 and M2 are classified in unrepairable_withheld_minutes through tip M3.
    Then: advance_correction_frontier accepts the chunk, sets repair_from to None, and
        advances baseline_watermarks to pending_watermarks.
    """
    observed = PnlCorrectionSampleVersion(55, samples._complete_sample(samples._M2))
    evidence = PnlCorrectionEvidence(KEY, samples._M1, samples._M3, (observed,), None)
    chunk = PnlCorrectionChunk(
        end=samples._M3,
        edits=(PnlCorrectionEdit(samples._M2, observed, None),),
        prior_peak=None,
        evidence=evidence,
        withheld_minutes=(samples._M1, samples._M2),
        reconciles_repair=True,
        unrepairable_withheld_minutes=(samples._M1, samples._M2),
    )
    done = advance_correction_frontier(_frontier(), chunk)
    assert done.next_minute is None
    assert done.repair_from is None
    assert done.baseline_watermarks == {"kraken": 5}


def test_remaining_repair_ignores_exempt_minutes_while_keeping_other_and_pending() -> None:
    """Exclude unrepairable minutes without altering repairable or latched pending logic.

    Given: Chunks combining unrepairable minutes, repairable withheld minutes, and an
        already-latched repair_from across unacknowledged and acknowledged transitions.
    When: advance_correction_frontier computes _remaining_correction_repair.
    Then: Unrepairable minutes are ignored, repairable minutes latch or advance repair_from,
        and an already-latched pending boundary survives unacknowledged chunks.
    """
    first = PnlCorrectionChunk(
        end=samples._M3,
        edits=(),
        prior_peak=None,
        evidence=PnlCorrectionEvidence(KEY, samples._M1, samples._M3, (), None),
        withheld_minutes=(samples._M1, samples._M2, samples._M3),
        unrepairable_withheld_minutes=(samples._M1, samples._M3),
    )
    latched = advance_correction_frontier(_frontier(), first)
    assert latched.repair_from == samples._M2
    assert latched.baseline_watermarks == {"kraken": 1}

    renewed = start_correction_frontier(KEY, REQUEST, latched)
    unacknowledged = PnlCorrectionChunk(
        end=samples._M1,
        edits=(),
        prior_peak=None,
        evidence=PnlCorrectionEvidence(KEY, samples._M1, samples._M1, (), None),
        withheld_minutes=(samples._M1,),
        reconciles_repair=False,
        unrepairable_withheld_minutes=(samples._M1,),
    )
    kept = advance_correction_frontier(renewed, unacknowledged)
    assert kept.repair_from == samples._M2

    acknowledged = PnlCorrectionChunk(
        end=samples._M3,
        edits=(),
        prior_peak=None,
        evidence=PnlCorrectionEvidence(KEY, samples._M2, samples._M3, (), None),
        withheld_minutes=(samples._M3,),
        reconciles_repair=True,
        unrepairable_withheld_minutes=(samples._M3,),
    )
    settled = advance_correction_frontier(kept, acknowledged)
    assert settled.repair_from is None
    assert settled.baseline_watermarks == {"kraken": 5}
