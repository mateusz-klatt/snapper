"""Contracts for planning bounded P&L corrections against an active sample set."""

from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from typing import Literal

import pytest

from snapper.application.portfolio.pnl_correction_planner import PnlCorrectionPlanInputs
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
        ("5C.3", "complete", None, 1000.0, 0.0),
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
