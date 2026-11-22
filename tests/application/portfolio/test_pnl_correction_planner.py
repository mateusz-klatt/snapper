"""Contracts for planning bounded P&L corrections against an active sample set."""

from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from typing import Literal

import pytest

from snapper.application.portfolio.pnl_correction_planner import REPAIR_EXEMPT_WITHHOLD_REASONS
from snapper.application.portfolio.pnl_correction_planner import PnlCorrectionPlanInputs
from snapper.application.portfolio.pnl_correction_planner import _is_unrepairable_withhold
from snapper.application.portfolio.pnl_correction_planner import plan_correction_chunk
from snapper.application.portfolio.pnl_snapshot_planner import MinuteInputs
from snapper.application.portfolio.pnl_snapshot_planner import WithheldMinute
from snapper.application.portfolio.pnl_snapshot_planner import assemble_minute_sample
from snapper.application.portfolio.pnl_timeline import PnlIncompletenessReason
from snapper.application.portfolio.pnl_timeline import PnlIncompletenessReasonEntry
from snapper.data.pnl_correction_state import MAX_CORRECTION_CHUNK_MINUTES
from snapper.data.pnl_correction_state import PnlCorrectionEdit
from snapper.data.pnl_correction_state import PnlCorrectionEvidence
from snapper.data.pnl_correction_state import PnlCorrectionKey
from snapper.data.pnl_correction_state import PnlCorrectionSampleVersion
from snapper.data.repository_types import PNL_SAMPLE_CALC_VERSION
from snapper.data.repository_types import PortfolioPnlSampleRow
from tests.application.portfolio import test_pnl_snapshot_planner as fixtures

_M1 = fixtures._M1
_M2 = fixtures._M2
_M3 = fixtures._M3
_M4 = _M3 + timedelta(minutes=1)
_M5 = _M4 + timedelta(minutes=1)
_KEY = PnlCorrectionKey(fixtures._WALLET, "live", "USD", fixtures._EPOCH, PNL_SAMPLE_CALC_VERSION)
_VENUES = frozenset({"kraken"})


def _minute(point_time: datetime, *reasons: PnlIncompletenessReason) -> MinuteInputs:
    """Use real planner fixtures for either an authoritative basket or a withheld point."""
    if reasons:
        entries = tuple(
            PnlIncompletenessReasonEntry(
                reason=reason,
                withholding_tier="untrusted",
                withholding_scope="global",
                trigger_instrument_public_id=None,
            )
            for reason in sorted(reasons)
        )
        point = replace(fixtures._untrusted_point(point_time), incompleteness_reasons=entries)
    else:
        point = fixtures._complete_point(point_time)
    return MinuteInputs(
        point, {"kraken": fixtures._attempt(minute=point_time)}, fixtures._crypto_evidence()
    )


def _complete_row(point_time: datetime) -> PortfolioPnlSampleRow:
    """Stamp a real complete sample before introducing scenario-specific changes."""
    sample = assemble_minute_sample(_minute(point_time), _VENUES, (), None).sample
    assert sample is not None and sample.valuation_status == "complete"
    return fixtures._to_row(sample)


def _inputs(
    valuations: tuple[MinuteInputs, ...],
    versions: tuple[PnlCorrectionSampleVersion, ...] = (),
    *,
    start: datetime = _M1,
    end: datetime = _M3,
    peak: float | None = None,
) -> PnlCorrectionPlanInputs:
    """Make the exact bounded read and the separately derived valuation inputs."""
    evidence = PnlCorrectionEvidence(_KEY, start, end, versions, peak)
    return PnlCorrectionPlanInputs(evidence, valuations, _VENUES, (), fixtures._to_row)


def test_peak_matches_exact_sum_of_the_persisted_equity_components() -> None:
    """A valid partition must satisfy the database's exact causal peak fence.

    Given: A basket whose rounded total differs from the sum of its saved partition.
    When: The real correction planner plans that minute as a complete sample.
    Then: Its causal peak equals the same cash-plus-position sum SQL will read.
    """
    attempt = fixtures._attempt(balances_json='[{"currency":"BTC","total":0.11}]')
    valuation = MinuteInputs(
        fixtures._complete_point(_M1),
        {"kraken": attempt},
        fixtures._crypto_evidence(fixtures._crypto_candle("BTC", _M1, 1.0)),
    )
    position = fixtures._version("kraken", "BTC", 0.04, valid_from=_M1)
    inputs = replace(_inputs((valuation,), end=_M1), position_versions=(position,))
    result = plan_correction_chunk(inputs)
    (edit,) = result.chunk.edits
    row = edit.replacement
    assert row is not None
    cash, position_value = row["cash_usd"], row["position_value_usd"]
    assert cash is not None and position_value is not None
    assert result.chunk.prior_peak == cash + position_value
    assert result.chunk.prior_peak == 0.11000000000000001


def test_exact_versions_are_used_for_updates_inserts_and_retractions() -> None:
    """Write only economic changes, old-version recomputations, or new samples.

    Given: Active and absent minutes, including a sample from a previous calculation.
    When: A bounded replay reproduces some evidence and changes or withholds other minutes.
    Then: The atomic plan identifies the exact physical edits and preserves equal samples.
    """
    stable = _complete_row(_M1)
    stable["session_id"], stable["sequence_id"], stable["timestamp"] = "earlier-writer", 77, _M5
    legacy = _complete_row(_M2)
    legacy["calc_version"] = "5C.3"
    original = PnlCorrectionSampleVersion(72, legacy)
    untrusted = PnlCorrectionSampleVersion(73, _complete_row(_M3))
    inputs = _inputs(
        (_minute(_M5), _minute(_M3, "fill_evidence_gap"), _minute(_M2), _minute(_M1)),
        (PnlCorrectionSampleVersion(71, stable), original, untrusted),
        end=_M5,
    )
    result = plan_correction_chunk(inputs)

    existing_edit, retract, insert = result.chunk.edits
    assert existing_edit.point_time == _M2 and existing_edit.expected_row is original
    assert existing_edit.replacement is not None
    assert existing_edit.replacement["calc_version"] == PNL_SAMPLE_CALC_VERSION
    assert retract == PnlCorrectionEdit(_M3, untrusted, None)
    assert insert.point_time == _M5 and insert.expected_row is None
    assert insert.replacement is not None
    assert result.chunk.evidence is inputs.evidence
    assert result.chunk.withheld_minutes == (_M3, _M4)
    assert result.chunk.reconciles_repair
    assert [item.reasons for item in result.withheld] == [
        ("fill_evidence_gap[global]",),
        ("missing_series_minute",),
    ]


@pytest.mark.parametrize("field", ["audit_json", "watermarks_json", "realized_pnl"])
def test_changed_evidence_or_amount_is_not_settled(
    field: Literal["audit_json", "watermarks_json", "realized_pnl"],
) -> None:
    """Persist changed money or provenance integral to the result.

    Given: A complete active sample differs in one evidence or value field.
    When: The correction planner compares it with a new valuation.
    Then: The result includes a replacement bound to the observed physical record.
    """
    row = _complete_row(_M1)
    if field == "audit_json":
        row[field] = '{"original_proof":true}'
    elif field == "watermarks_json":
        row[field] = '{"kraken":4}'
    else:
        row[field] = -2.0
    existing = PnlCorrectionSampleVersion(31, row)
    result = plan_correction_chunk(_inputs((_minute(_M1),), (existing,), end=_M1))
    (edit,) = result.chunk.edits
    assert edit.expected_row is existing and edit.replacement is not None
    assert edit.replacement[field] != row[field]


def test_foreign_epoch_is_retained_and_its_equity_never_affects_the_running_peak() -> None:
    """Preserve blocked ownership and retract a different unsupported same-epoch row.

    Given: The admitted range has a foreign-epoch sample and a same-epoch sample.
    When: Replay returns a valuation for the first and withholds the second.
    Then: The first remains unresolved and the second is retracted without borrowing its peak.
    """
    missing = PnlCorrectionSampleVersion(98, _complete_row(_M1))
    foreign = _complete_row(_M2)
    foreign["epoch_public_id"] = "foreign-epoch"
    foreign["cash_usd"] = 9000.0
    result = plan_correction_chunk(
        _inputs(
            (_minute(_M2), _minute(_M3)),
            (missing, PnlCorrectionSampleVersion(99, foreign)),
        )
    )
    retract, insert = result.chunk.edits
    assert retract == PnlCorrectionEdit(_M1, missing, None)
    assert insert.point_time == _M3 and insert.replacement is not None
    assert insert.replacement["drawdown"] == 0.0
    assert result.chunk.prior_peak == 1000.0
    assert result.withheld == (
        WithheldMinute(_M1, ("missing_series_minute",), ()),
        WithheldMinute(_M2, ("foreign_epoch_sample",), ()),
    )


@pytest.mark.parametrize(
    ("version", "status", "seed_peak", "expected_peak", "expected_drawdown"),
    [
        (PNL_SAMPLE_CALC_VERSION, "complete", None, 2500.0, 0.6),
        (PNL_SAMPLE_CALC_VERSION, "complete", 5000.0, 5000.0, 0.8),
        (PNL_SAMPLE_CALC_VERSION, "incomplete", None, 1000.0, 0.0),
    ],
)
def test_only_retained_current_complete_rows_raise_the_peak(
    version: str,
    status: Literal["complete", "incomplete"],
    seed_peak: float | None,
    expected_peak: float,
    expected_drawdown: float,
) -> None:
    """Fold a retained, qualifying sample before computing the next drawdown.

    Given: A row-preserving arithmetic refusal leaves an earlier physical sample in place.
    When: The planner calculates the next minute's drawdown.
    Then: Only complete retained equity from the current calculation can raise its peak.
    """
    row = _complete_row(_M1)
    row["calc_version"], row["valuation_status"], row["cash_usd"] = version, status, 2500.0
    inputs = _inputs(
        (_minute(_M1, "instrument_sum_unrepresentable"), _minute(_M2)),
        (PnlCorrectionSampleVersion(61, row),),
        end=_M2,
        peak=seed_peak,
    )
    result = plan_correction_chunk(inputs)
    (edit,) = result.chunk.edits
    assert edit.point_time == _M2 and edit.replacement is not None
    assert edit.replacement["drawdown"] == pytest.approx(expected_drawdown)
    assert result.chunk.prior_peak == expected_peak
    assert result.chunk.withheld_minutes == (_M1,)
    assert result.withheld[0].reason_codes == ("instrument_sum_unrepresentable",)


@pytest.mark.parametrize(
    "reason",
    ["instrument_sum_unrepresentable", "attribution_sum_unrepresentable"],
)
def test_row_preserving_withhold_retracts_a_foreign_calc_version_row(
    reason: PnlIncompletenessReason,
) -> None:
    """Retract a foreign-version row on a sum-unrepresentable withhold while keeping same-version.

    Given: Active complete M1 samples on 5C.3 and current calc_version with cash 2500.0.
    When: Replay withholds M1 for a pure sum-unrepresentable reason and values M2 as complete.
    Then: The foreign M1 row is retracted and marked unrepairable without raising the peak,
        whereas the same-version control retains M1 with no M1 edit and stays repairable.
    """
    valuations = (_minute(_M1, reason), _minute(_M2))
    foreign_row = _complete_row(_M1)
    foreign_row["calc_version"], foreign_row["cash_usd"] = "5C.3", 2500.0
    foreign_version = PnlCorrectionSampleVersion(61, foreign_row)
    result = plan_correction_chunk(_inputs(valuations, (foreign_version,), end=_M2))

    retract, insert = result.chunk.edits
    assert retract == PnlCorrectionEdit(_M1, foreign_version, None)
    assert insert.point_time == _M2 and insert.expected_row is None
    assert insert.replacement is not None
    assert insert.replacement["drawdown"] == 0.0
    assert result.chunk.prior_peak == 1000.0
    assert result.chunk.withheld_minutes == (_M1,)
    assert result.chunk.unrepairable_withheld_minutes == (_M1,)

    same_row = _complete_row(_M1)
    same_row["cash_usd"] = 2500.0
    same_version = PnlCorrectionSampleVersion(62, same_row)
    control = plan_correction_chunk(_inputs(valuations, (same_version,), end=_M2))

    (control_edit,) = control.chunk.edits
    assert control_edit.point_time == _M2 and control_edit.expected_row is None
    assert control_edit.replacement is not None
    assert control_edit.replacement["drawdown"] == pytest.approx(0.6)
    assert control.chunk.prior_peak == 2500.0
    assert control.chunk.withheld_minutes == (_M1,)
    assert control.chunk.unrepairable_withheld_minutes == ()


@pytest.mark.parametrize("field", ["cash_usd", "position_value_usd"])
def test_malformed_retained_complete_sample_refuses_the_chunk(
    field: Literal["cash_usd", "position_value_usd"],
) -> None:
    """Never give a later drawdown a peak from an invalid complete row.

    Given: A retained complete sample is missing a component of equity.
    When: Replay would otherwise preserve the sample for a representation limit.
    Then: Planning fails before accepting a fabricated causal peak.
    """
    row = _complete_row(_M1)
    row[field] = None
    inputs = _inputs(
        (_minute(_M1, "attribution_sum_unrepresentable"),),
        (PnlCorrectionSampleVersion(61, row),),
        end=_M1,
    )
    with pytest.raises(ValueError, match="both equity components"):
        plan_correction_chunk(inputs)


def test_representation_limit_does_not_mask_untrusted_money() -> None:
    """A mixed-cause refusal cannot preserve the previous monetary values.

    Given: A minute has both an arithmetic refusal and a gap in trusted records.
    When: Replay plans against its previously complete financial sample.
    Then: The plan retracts the stale sample and records the minute for repair.
    """
    existing = PnlCorrectionSampleVersion(33, _complete_row(_M1))
    inputs = _inputs(
        (_minute(_M1, "fill_evidence_gap", "instrument_sum_unrepresentable"),),
        (existing,),
        end=_M1,
    )
    assert plan_correction_chunk(inputs).chunk.edits == (PnlCorrectionEdit(_M1, existing, None),)


@pytest.mark.parametrize("grids", [(_M1, _M1), (fixtures._T0,), (_M2,)])
def test_duplicate_and_out_of_window_series_inputs_are_refused(
    grids: tuple[datetime, ...],
) -> None:
    """Only one valuation input can be bound to each admitted minute.

    Given: Series input includes a duplicate minute or a minute outside the chunk.
    When: The planner indexes the requested valuations.
    Then: It rejects the evidence before the write plan is formed.
    """
    inputs = _inputs(tuple(_minute(minute) for minute in grids), end=_M1)
    with pytest.raises(ValueError, match="unique series inputs in the admitted window"):
        plan_correction_chunk(inputs)


@pytest.mark.parametrize("minutes", [0, MAX_CORRECTION_CHUNK_MINUTES + 1])
def test_invalid_window_is_refused_before_planning(minutes: int) -> None:
    """A reversed or oversized caller interval cannot allocate a plan.

    Given: The caller proposes an inverted interval or more minutes than the chunk limit.
    When: The planner validates the bounded evidence window.
    Then: It rejects the interval without planning any sample edits.
    """
    end = _M1 + timedelta(minutes=minutes - 1)
    with pytest.raises(ValueError, match="bounded minute window"):
        plan_correction_chunk(_inputs((), end=end))


def test_each_minute_in_a_full_zero_write_window_is_withheld() -> None:
    """A bounded run with no usable series does not disguise missing evidence.

    Given: A complete admitted day has no usable series inputs or active samples.
    When: Replay plans the entire interval without producing a financial row.
    Then: Every minute is explicitly withheld so the repair boundary stays visible.
    """
    end = _M1 + timedelta(minutes=MAX_CORRECTION_CHUNK_MINUTES - 1)
    result = plan_correction_chunk(_inputs((), end=end, peak=3000.0))

    assert result.chunk.edits == ()
    assert result.chunk.prior_peak == 3000.0
    assert len(result.chunk.withheld_minutes) == MAX_CORRECTION_CHUNK_MINUTES
    assert result.chunk.withheld_minutes[0] == _M1
    assert result.chunk.withheld_minutes[-1] == end


def test_repair_exempt_reason_set_matches_closed_deterministic_taxonomy() -> None:
    """Pin the closed set of reasons eligible for repair exemption under PR-1.

    Given: The planner module constant REPAIR_EXEMPT_WITHHOLD_REASONS.
    When: Its members are inspected against the reconciliation and sum-unrepresentable taxonomy.
    Then: It contains exactly the four deterministic post-input reasons and nothing else.
    """
    assert (
        frozenset(
            {
                "instrument_reconciliation_failed",
                "attribution_reconciliation_failed",
                "instrument_sum_unrepresentable",
                "attribution_sum_unrepresentable",
            }
        )
        == REPAIR_EXEMPT_WITHHOLD_REASONS
    )


@pytest.mark.parametrize(
    "reason",
    [
        "instrument_reconciliation_failed",
        "attribution_reconciliation_failed",
        "instrument_sum_unrepresentable",
        "attribution_sum_unrepresentable",
    ],
)
def test_exempt_absent_withheld_minutes_become_unrepairable(
    reason: PnlIncompletenessReason,
) -> None:
    """Exempt a pure deterministic withhold when no active row exists at that minute.

    Given: An absent sample minute whose valuation withholds solely for one exempt reason.
    When: plan_correction_chunk plans the minute against empty active evidence.
    Then: The minute appears in both withheld_minutes and unrepairable_withheld_minutes.
    """
    result = plan_correction_chunk(_inputs((_minute(_M1, reason),), end=_M1))
    assert result.chunk.edits == ()
    assert result.chunk.withheld_minutes == (_M1,)
    assert result.chunk.unrepairable_withheld_minutes == (_M1,)


@pytest.mark.parametrize(
    "reasons",
    [
        ("instrument_reconciliation_failed",),
        ("attribution_reconciliation_failed",),
        ("instrument_reconciliation_failed", "instrument_sum_unrepresentable"),
    ],
)
def test_exempt_retracted_withheld_minutes_become_unrepairable(
    reasons: tuple[PnlIncompletenessReason, ...],
) -> None:
    """Exempt an active-row minute when non-preserving exempt reasons retract it (R1).

    Given: An active sample row at M1 and pure exempt reasons that include a retraction cause.
    When: plan_correction_chunk plans the minute against that observed version.
    Then: It emits the explicit retraction edit and classifies M1 as unrepairable.
    """
    existing = PnlCorrectionSampleVersion(88, _complete_row(_M1))
    result = plan_correction_chunk(_inputs((_minute(_M1, *reasons),), (existing,), end=_M1))
    assert result.chunk.edits == (PnlCorrectionEdit(_M1, existing, None),)
    assert result.chunk.withheld_minutes == (_M1,)
    assert result.chunk.unrepairable_withheld_minutes == (_M1,)


@pytest.mark.parametrize(
    "reason",
    ["instrument_sum_unrepresentable", "attribution_sum_unrepresentable"],
)
def test_sum_unrepresentable_with_retained_row_stays_repairable(
    reason: PnlIncompletenessReason,
) -> None:
    """Retain the repair obligation whenever _withhold preserves an active row (R1).

    Given: An existing active sample at M1 and a pure sum-unrepresentable refusal.
    When: plan_correction_chunk retains the existing row via preserves_persisted_row.
    Then: M1 stays in withheld_minutes and is excluded from unrepairable_withheld_minutes.
    """
    existing = PnlCorrectionSampleVersion(89, _complete_row(_M1))
    result = plan_correction_chunk(_inputs((_minute(_M1, reason),), (existing,), end=_M1))
    assert result.chunk.edits == ()
    assert result.chunk.withheld_minutes == (_M1,)
    assert result.chunk.unrepairable_withheld_minutes == ()


def test_mixed_non_exempt_and_empty_reason_withholds_stay_repairable() -> None:
    """Keep mixed, market-data, and empty-code withholds repairable.

    Given: A four-minute chunk mixing an exempt+gap minute, missing_series_minute,
        foreign_epoch_sample, and mark_unavailable, plus an empty-code WithheldMinute.
    When: plan_correction_chunk and _is_unrepairable_withhold classify all minutes.
    Then: All four appear in withheld_minutes and unrepairable_withheld_minutes stays empty.
    """
    foreign = _complete_row(_M3)
    foreign["epoch_public_id"] = "foreign-epoch"
    existing_m1 = PnlCorrectionSampleVersion(91, _complete_row(_M1))
    inputs = _inputs(
        (
            _minute(_M1, "fill_evidence_gap", "instrument_sum_unrepresentable"),
            _minute(_M3),
            _minute(_M4, "mark_unavailable"),
        ),
        (existing_m1, PnlCorrectionSampleVersion(92, foreign)),
        end=_M4,
    )
    result = plan_correction_chunk(inputs)
    assert result.chunk.withheld_minutes == (_M1, _M2, _M3, _M4)
    assert result.chunk.unrepairable_withheld_minutes == ()
    assert PnlCorrectionEdit(_M1, existing_m1, None) in result.chunk.edits
    assert not _is_unrepairable_withhold(WithheldMinute(_M1, ("custom",), ()), None, None)


def test_reconciled_m1_propagates_causal_peak_into_m2_drawdown() -> None:
    """Propagate newly recovered M1 peak into M2 drawdown across 5C.5 -> 5C.6 (R3).

    Given: A two-minute window where M1 is either withheld as
        instrument_reconciliation_failed (5C.5) or complete with cash 2500 (5C.6),
        followed by complete M2 with cash 1000 and an active 5C.5 M2 row at zero
        drawdown.
    When: plan_correction_chunk plans [M1, M2] under both valuations.
    Then: Withheld M1 leaves M2 at drawdown 0.0 and peak 1000, whereas complete M1
        at cash 2500 updates M2 to drawdown 0.6 and peak 2500.
    """
    withheld_plan = plan_correction_chunk(
        _inputs((_minute(_M1, "instrument_reconciliation_failed"), _minute(_M2)), end=_M2)
    )
    (withheld_m2_edit,) = withheld_plan.chunk.edits
    assert withheld_m2_edit.point_time == _M2
    assert withheld_m2_edit.replacement is not None
    assert withheld_m2_edit.replacement["drawdown"] == 0.0
    assert withheld_plan.chunk.prior_peak == 1000.0

    legacy_m2 = _complete_row(_M2)
    legacy_m2["calc_version"] = "5C.5"
    existing_m2 = PnlCorrectionSampleVersion(95, legacy_m2)
    recovered_m1 = MinuteInputs(
        fixtures._complete_point(_M1),
        {
            "kraken": fixtures._attempt(
                minute=_M1,
                balances_json='[{"currency":"USD","total":2500.0}]',
            )
        },
        fixtures._crypto_evidence(),
    )
    recovered_plan = plan_correction_chunk(
        _inputs((recovered_m1, _minute(_M2)), (existing_m2,), end=_M2)
    )
    edits_by_minute = {edit.point_time: edit for edit in recovered_plan.chunk.edits}
    assert sorted(edits_by_minute) == [_M1, _M2]
    m2_replacement = edits_by_minute[_M2].replacement
    assert m2_replacement is not None
    assert m2_replacement["drawdown"] == pytest.approx(0.6)
    assert recovered_plan.chunk.prior_peak == 2500.0
