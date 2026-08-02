"""Tests for the strict P&L timeline series response schemas (Phase 5A).

Pins the strict-body contract: every monetary field accepts ``None`` (honest
incompleteness), unknown fields are rejected (``extra='forbid'``), and the
valuation status is equivalent to a required list of closed causal reasons.
"""

from datetime import UTC
from datetime import datetime

import pytest
from pydantic import ValidationError

from snapper.api.schemas.pnl_timeline import PnlAiDecisionMarkerData
from snapper.api.schemas.pnl_timeline import PnlAttributionContributionData
from snapper.api.schemas.pnl_timeline import PnlEquityCoverageData
from snapper.api.schemas.pnl_timeline import PnlExecutionCorrectionData
from snapper.api.schemas.pnl_timeline import PnlExecutionHistoryData
from snapper.api.schemas.pnl_timeline import PnlFillMarkerData
from snapper.api.schemas.pnl_timeline import PnlFxRateSourceData
from snapper.api.schemas.pnl_timeline import PnlIncompletenessReasonData
from snapper.api.schemas.pnl_timeline import PnlInstrumentContributionData
from snapper.api.schemas.pnl_timeline import PnlSeriesData
from snapper.api.schemas.pnl_timeline import PnlSeriesResponse
from snapper.api.schemas.pnl_timeline import PnlSignalMarkerData
from snapper.api.schemas.pnl_timeline import PnlTimelineData
from snapper.api.schemas.pnl_timeline import PnlTimelinePointData
from snapper.api.schemas.pnl_timeline import PnlTimelineResponse
from snapper.api.schemas.pnl_timeline import PnlValuationStatus

_NOW = datetime(2026, 7, 20, 12, 0, tzinfo=UTC)


def _contribution() -> PnlInstrumentContributionData:
    """Build a fully-null contribution (an untrusted point's shape)."""
    return PnlInstrumentContributionData(
        instrument_public_id="i1",
        native_symbol=None,
        exchange=None,
        realized_pnl=None,
        fee_pnl=None,
        accrual_pnl=None,
        unrealized_pnl=None,
    )


def _attribution() -> PnlAttributionContributionData:
    """Build a fully-null attribution bucket for an untrusted point."""
    return PnlAttributionContributionData(
        origin="unattributed",
        strategy_name=None,
        realized_pnl=None,
        fee_pnl=None,
        accrual_pnl=None,
        unrealized_pnl=None,
    )


def _multi_cause_reasons() -> list[PnlIncompletenessReasonData]:
    """Build one MARK and one UNTRUSTED instrument-scoped cause."""
    return [
        PnlIncompletenessReasonData(
            reason="mark_unavailable",
            withholding_tier="mark_incomplete",
            withholding_scope="instrument",
            trigger_instrument_public_id="i1",
        ),
        PnlIncompletenessReasonData(
            reason="execution_price_invalid",
            withholding_tier="untrusted",
            withholding_scope="instrument",
            trigger_instrument_public_id="i2",
        ),
    ]


def _point(status: PnlValuationStatus = "complete") -> PnlTimelinePointData:
    """Build one valued point with the given valuation status."""
    return PnlTimelinePointData(
        point_time=_NOW,
        realized_pnl=1.0,
        fee_pnl=-0.5,
        accrual_pnl=0.0,
        unrealized_pnl=2.0,
        net_pnl=2.5,
        equity=1000.0,
        cash=400.0,
        position_value=600.0,
        drawdown=0.1,
        valuation_status=status,
        incompleteness_reasons=[] if status == "complete" else _multi_cause_reasons(),
        per_instrument=[
            PnlInstrumentContributionData(
                instrument_public_id="i1",
                native_symbol="BTC-USD",
                exchange="kraken",
                realized_pnl=1.0,
                fee_pnl=-0.5,
                accrual_pnl=0.0,
                unrealized_pnl=2.0,
            )
        ],
        attribution=[
            PnlAttributionContributionData(
                origin="system",
                strategy_name="momentum",
                realized_pnl=1.0,
                fee_pnl=-0.5,
                accrual_pnl=0.0,
                unrealized_pnl=2.0,
            )
        ],
    )


def _unsampled_coverage() -> PnlEquityCoverageData:
    """Build the unsampled equity-coverage disclosure used by the envelopes."""
    return PnlEquityCoverageData(
        sampled=False,
        venue_scope=None,
        external_flows_adjusted=None,
        complete_minutes=0,
        first_minute=None,
        last_minute=None,
        sample_calc_version=None,
    )


def _sampled_coverage() -> PnlEquityCoverageData:
    """Build the sampled spot-only equity-coverage disclosure."""
    return PnlEquityCoverageData(
        sampled=True,
        venue_scope="spot_only",
        external_flows_adjusted=False,
        complete_minutes=3,
        first_minute=_NOW,
        last_minute=_NOW,
        sample_calc_version="5B.2",
    )


def _correction() -> PnlExecutionCorrectionData:
    """Build one applied operator correction as the fold reports it."""
    return PnlExecutionCorrectionData(
        correction_public_id="00000000-0000-7000-8000-0000000000d9",
        target_execution_public_id="00000000-0000-7000-8000-0000000000e1",
        exchange="kraken",
        scope_sequence=1,
        reason="unwitnessed_phantom",
        correction_time=_NOW,
    )


def _corrected_history() -> PnlExecutionHistoryData:
    """Build the disclosure of a scope whose history an operator corrected."""
    return PnlExecutionHistoryData(status="operator_corrected", corrections=[_correction()])


def _uncorrected_history() -> PnlExecutionHistoryData:
    """Build the disclosure of a scope whose history is the raw ledger."""
    return PnlExecutionHistoryData(status="as_recorded", corrections=[])


def _series_data() -> PnlSeriesData:
    """Build a valid series payload envelope."""
    return PnlSeriesData(
        public_id="p1",
        timestamp=_NOW,
        session_id="s1",
        sequence_id=1,
        wallet_public_id="w1",
        mode="live",
        granularity="1m",
        valuation_ccy="USD",
        from_time=_NOW,
        to_time=_NOW,
        as_of=_NOW,
        mark_source="finalized_1m_candle_close",
        rate_sources=[
            PnlFxRateSourceData(
                source_currency="EUR",
                valuation_currency="USD",
                base_currency="EUR",
                quote_currency="USD",
                exchange="kraken",
            )
        ],
        calc_version="5A.1",
        equity_coverage=_sampled_coverage(),
        execution_history=_corrected_history(),
        points=[_point()],
    )


def _timeline_data() -> PnlTimelineData:
    """Build a valid marker-bearing timeline payload."""
    return PnlTimelineData(
        public_id="timeline-1",
        timestamp=_NOW,
        session_id="s1",
        sequence_id=3,
        wallet_public_id="w1",
        mode="live",
        granularity="1m",
        valuation_ccy="USD",
        from_time=_NOW,
        to_time=_NOW,
        as_of=_NOW,
        mark_source="finalized_1m_candle_close",
        rate_sources=[],
        calc_version="5A.2",
        equity_coverage=_unsampled_coverage(),
        execution_history=_uncorrected_history(),
        points=[_point()],
        marker_limit=2_000,
        markers_truncated=False,
        markers=[
            PnlFillMarkerData(
                marker_time=_NOW,
                instrument_public_id="i1",
                side="buy",
                size=1.0,
                price=100.0,
                execution_public_id="execution-1",
                order_public_id="order-1",
                status="filled",
            ),
            PnlSignalMarkerData(
                marker_time=_NOW,
                instrument_public_id="i1",
                side="buy",
                strategy_name=None,
                strength=0.8,
                reason="breakout",
                price=None,
                signal_public_id="signal-1",
                outcome="no_fill",
                status="no_fill",
            ),
            PnlAiDecisionMarkerData(
                marker_time=_NOW,
                instrument_public_id="i1",
                strategy_public_id="strategy-1",
                review_public_id="review-1",
                event_public_id="event-1",
                decision="reject",
                rationale=None,
                outcome="rejected",
                status="resolved_rejected",
            ),
        ],
    )


class TestStrictContract:
    """Cover the strict-body validation contract."""

    def test_valid_series_response_round_trips(self) -> None:
        """A complete response validates and preserves its payload points."""
        response = PnlSeriesResponse(
            public_id="env",
            timestamp=_NOW,
            session_id="s1",
            sequence_id=2,
            payload=_series_data(),
        )
        assert response.type == "pnl_series"
        assert response.payload.calc_version == "5A.1"
        assert (
            response.payload.rate_sources[0].source_currency,
            response.payload.rate_sources[0].valuation_currency,
            response.payload.rate_sources[0].base_currency,
            response.payload.rate_sources[0].quote_currency,
            response.payload.rate_sources[0].exchange,
        ) == ("EUR", "USD", "EUR", "USD", "kraken")
        assert response.payload.points[0].valuation_status == "complete"
        assert response.payload.points[0].attribution[0].origin == "system"
        assert response.payload.points[0].attribution[0].strategy_name == "momentum"

    def test_null_pnl_fields_are_allowed(self) -> None:
        """An incomplete point carries null monetary fields honestly."""
        point = PnlTimelinePointData(
            point_time=_NOW,
            realized_pnl=None,
            fee_pnl=None,
            accrual_pnl=None,
            unrealized_pnl=None,
            net_pnl=None,
            equity=None,
            cash=None,
            position_value=None,
            drawdown=None,
            valuation_status="incomplete",
            incompleteness_reasons=_multi_cause_reasons(),
            per_instrument=[_contribution()],
            attribution=[_attribution()],
        )
        assert point.net_pnl is None
        assert point.equity is None
        assert point.drawdown is None
        assert [reason.reason for reason in point.incompleteness_reasons] == [
            "mark_unavailable",
            "execution_price_invalid",
        ]
        assert point.incompleteness_reasons[0].withholding_tier == "mark_incomplete"
        assert point.incompleteness_reasons[1].trigger_instrument_public_id == "i2"
        assert point.per_instrument[0].realized_pnl is None
        assert point.attribution[0].realized_pnl is None

    def test_valuation_status_and_reason_presence_are_equivalent(self) -> None:
        """Both directions of the status-to-reason invariant are enforced."""
        complete_payload = _point().model_dump()
        complete_payload["incompleteness_reasons"] = [
            PnlIncompletenessReasonData(
                reason="fill_evidence_gap",
                withholding_tier="untrusted",
                withholding_scope="global",
                trigger_instrument_public_id=None,
            ).model_dump()
        ]
        with pytest.raises(ValidationError):
            PnlTimelinePointData.model_validate(complete_payload)
        incomplete_payload = _point().model_dump()
        incomplete_payload["valuation_status"] = "incomplete"
        with pytest.raises(ValidationError):
            PnlTimelinePointData.model_validate(incomplete_payload)

    def test_reason_contract_is_required_closed_and_strict(self) -> None:
        """Reason records are required and reject unknown literals and fields."""
        missing = _point().model_dump()
        missing.pop("incompleteness_reasons")
        with pytest.raises(ValidationError):
            PnlTimelinePointData.model_validate(missing)
        entry = _multi_cause_reasons()[0].model_dump()
        entry["reason"] = "unknown"
        with pytest.raises(ValidationError):
            PnlIncompletenessReasonData.model_validate(entry)
        entry = _multi_cause_reasons()[0].model_dump()
        entry["withholding_tier"] = "partial"
        with pytest.raises(ValidationError):
            PnlIncompletenessReasonData.model_validate(entry)
        entry = _multi_cause_reasons()[0].model_dump()
        entry["withholding_scope"] = "wallet"
        with pytest.raises(ValidationError):
            PnlIncompletenessReasonData.model_validate(entry)
        entry = _multi_cause_reasons()[0].model_dump()
        entry["surprise"] = True
        with pytest.raises(ValidationError):
            PnlIncompletenessReasonData.model_validate(entry)

    def test_contribution_identity_is_required_but_nullable(self) -> None:
        """Display identity accepts honest nulls but cannot disappear from the contract."""
        contribution = _contribution()
        assert contribution.native_symbol is None
        assert contribution.exchange is None
        missing_symbol = contribution.model_dump()
        missing_symbol.pop("native_symbol")
        with pytest.raises(ValidationError):
            PnlInstrumentContributionData.model_validate(missing_symbol)
        missing_exchange = contribution.model_dump()
        missing_exchange.pop("exchange")
        with pytest.raises(ValidationError):
            PnlInstrumentContributionData.model_validate(missing_exchange)

    def test_instrument_scope_requires_a_triggering_instrument(self) -> None:
        """Instrument-scoped reason records cannot erase causal identity."""
        payload = _multi_cause_reasons()[0].model_dump()
        payload["trigger_instrument_public_id"] = None
        with pytest.raises(ValidationError):
            PnlIncompletenessReasonData.model_validate(payload)

    def test_fill_marker_price_can_be_withheld(self) -> None:
        """A fill marker retains its identity when its native price is unproved."""
        marker = PnlFillMarkerData(
            marker_time=_NOW,
            instrument_public_id="i1",
            side="buy",
            size=1.0,
            price=None,
            execution_public_id="execution-1",
            order_public_id="order-1",
            status="filled",
        )
        assert marker.price is None

    def test_unknown_field_is_rejected(self) -> None:
        """Extra fields are forbidden on the strict point schema."""
        payload: dict[str, object] = {
            "point_time": _NOW,
            "realized_pnl": 1.0,
            "fee_pnl": 0.0,
            "accrual_pnl": 0.0,
            "unrealized_pnl": 0.0,
            "net_pnl": 1.0,
            "equity": None,
            "cash": None,
            "position_value": None,
            "drawdown": None,
            "valuation_status": "complete",
            "incompleteness_reasons": [],
            "per_instrument": [],
            "attribution": [],
            "surprise": 1,
        }
        with pytest.raises(ValidationError):
            PnlTimelinePointData.model_validate(payload)

    def test_invalid_valuation_status_is_rejected(self) -> None:
        """The valuation-status discriminator is a closed literal set."""
        payload: dict[str, object] = {
            "point_time": _NOW,
            "realized_pnl": 1.0,
            "fee_pnl": 0.0,
            "accrual_pnl": 0.0,
            "unrealized_pnl": 0.0,
            "net_pnl": 1.0,
            "equity": None,
            "cash": None,
            "position_value": None,
            "drawdown": None,
            "valuation_status": "partial",
            "incompleteness_reasons": [],
            "per_instrument": [],
            "attribution": [],
        }
        with pytest.raises(ValidationError):
            PnlTimelinePointData.model_validate(payload)

    def test_attribution_rejects_unknown_fields_and_origins(self) -> None:
        """Attribution bodies are strict and their origin is a closed literal."""
        attribution = _point().attribution[0].model_dump()
        attribution["surprise"] = True
        with pytest.raises(ValidationError):
            PnlAttributionContributionData.model_validate(attribution)
        attribution.pop("surprise")
        attribution["origin"] = "replay"
        with pytest.raises(ValidationError):
            PnlAttributionContributionData.model_validate(attribution)

    def test_marker_timeline_response_round_trips_all_kinds(self) -> None:
        """The timeline envelope preserves each typed marker discriminator."""
        response = PnlTimelineResponse(
            public_id="envelope-1",
            timestamp=_NOW,
            session_id="s1",
            sequence_id=4,
            payload=_timeline_data(),
        )
        assert response.type == "pnl_timeline"
        assert response.payload.marker_limit == 2_000
        assert response.payload.markers_truncated is False
        assert [marker.kind for marker in response.payload.markers] == [
            "fill",
            "signal",
            "ai_decision",
        ]
        assert response.payload.markers[1].outcome == "no_fill"
        assert response.payload.markers[2].outcome == "rejected"

    def test_unknown_marker_discriminator_is_rejected(self) -> None:
        """Only fill, signal, and AI-decision marker kinds are accepted."""
        payload = _timeline_data().model_dump()
        payload["markers"] = [
            {
                "kind": "order",
                "marker_time": _NOW,
                "instrument_public_id": "i1",
            }
        ]
        with pytest.raises(ValidationError):
            PnlTimelineData.model_validate(payload)

    def test_marker_models_reject_unknown_fields_and_outcomes(self) -> None:
        """Marker bodies remain strict and their outcomes are closed literals."""
        fill = _timeline_data().markers[0].model_dump()
        fill["surprise"] = True
        with pytest.raises(ValidationError):
            PnlFillMarkerData.model_validate(fill)
        signal = _timeline_data().markers[1].model_dump()
        signal["outcome"] = "rejected"
        with pytest.raises(ValidationError):
            PnlSignalMarkerData.model_validate(signal)


class TestEquityCoverageContract:
    """Cover the equity-coverage disclosure and its honesty invariant."""

    def test_point_exposes_nullable_equity_overlay_fields(self) -> None:
        """A point transports the four nullable observed-equity overlay fields."""
        point = _point()
        assert (point.equity, point.cash, point.position_value, point.drawdown) == (
            1000.0,
            400.0,
            600.0,
            0.1,
        )

    def test_equity_field_is_required_but_nullable(self) -> None:
        """The overlay fields are required in the contract but accept honest nulls."""
        missing = _point().model_dump()
        missing.pop("equity")
        with pytest.raises(ValidationError):
            PnlTimelinePointData.model_validate(missing)

    def test_sampled_coverage_round_trips(self) -> None:
        """A fully provisioned sampled disclosure validates."""
        coverage = _sampled_coverage()
        assert coverage.sampled is True
        assert coverage.venue_scope == "spot_only"
        assert coverage.sample_calc_version == "5B.2"

    def test_unsampled_coverage_round_trips(self) -> None:
        """A wholly null unsampled disclosure validates."""
        coverage = _unsampled_coverage()
        assert coverage.sampled is False
        assert coverage.complete_minutes == 0

    def test_sampled_coverage_requires_full_provenance(self) -> None:
        """A sampled disclosure cannot drop its scope, span, or minute count."""
        missing_scope = _sampled_coverage().model_dump()
        missing_scope["venue_scope"] = None
        with pytest.raises(ValidationError):
            PnlEquityCoverageData.model_validate(missing_scope)
        empty_minutes = _sampled_coverage().model_dump()
        empty_minutes["complete_minutes"] = 0
        with pytest.raises(ValidationError):
            PnlEquityCoverageData.model_validate(empty_minutes)

    def test_unsampled_coverage_rejects_stray_provenance(self) -> None:
        """An unsampled disclosure must null every provenance field."""
        stray_scope = _unsampled_coverage().model_dump()
        stray_scope["venue_scope"] = "spot_only"
        with pytest.raises(ValidationError):
            PnlEquityCoverageData.model_validate(stray_scope)
        stray_minutes = _unsampled_coverage().model_dump()
        stray_minutes["complete_minutes"] = 2
        with pytest.raises(ValidationError):
            PnlEquityCoverageData.model_validate(stray_minutes)

    def test_envelope_requires_equity_coverage(self) -> None:
        """The series envelope cannot drop the equity-coverage disclosure."""
        payload = _series_data().model_dump()
        payload.pop("equity_coverage")
        with pytest.raises(ValidationError):
            PnlSeriesData.model_validate(payload)


class TestExecutionHistoryContract:
    """Cover the operator-correction disclosure and its honesty invariant (A3)."""

    def test_corrected_history_names_every_applied_correction(self) -> None:
        """A corrected scope transports the identity and scope of each correction."""
        history = _corrected_history()
        assert history.status == "operator_corrected"
        correction = history.corrections[0]
        assert (
            correction.correction_public_id,
            correction.target_execution_public_id,
            correction.exchange,
            correction.scope_sequence,
            correction.reason,
            correction.correction_time,
        ) == (
            "00000000-0000-7000-8000-0000000000d9",
            "00000000-0000-7000-8000-0000000000e1",
            "kraken",
            1,
            "unwitnessed_phantom",
            _NOW,
        )

    def test_uncorrected_history_round_trips(self) -> None:
        """An uncorrected scope transports the empty as-recorded disclosure."""
        history = _uncorrected_history()
        assert history.status == "as_recorded"
        assert history.corrections == []

    def test_corrected_status_requires_a_named_correction(self) -> None:
        """A correction claim with nothing named is refused, never served empty."""
        empty = _corrected_history().model_dump()
        empty["corrections"] = []
        with pytest.raises(ValidationError, match="operator_corrected requires"):
            PnlExecutionHistoryData.model_validate(empty)

    def test_as_recorded_status_cannot_hide_a_correction(self) -> None:
        """A correction can never ride out under an as-recorded status."""
        hidden = _uncorrected_history().model_dump()
        hidden["corrections"] = [_correction().model_dump()]
        with pytest.raises(ValidationError, match="as_recorded requires none"):
            PnlExecutionHistoryData.model_validate(hidden)

    def test_history_status_and_reason_taxonomies_are_closed(self) -> None:
        """Neither the status nor the correction reason accepts an open string."""
        unknown_status = _uncorrected_history().model_dump()
        unknown_status["status"] = "partially_corrected"
        with pytest.raises(ValidationError):
            PnlExecutionHistoryData.model_validate(unknown_status)
        unknown_reason = _correction().model_dump()
        unknown_reason["reason"] = "operator_felt_like_it"
        with pytest.raises(ValidationError):
            PnlExecutionCorrectionData.model_validate(unknown_reason)

    def test_correction_rejects_unknown_fields_and_missing_scope(self) -> None:
        """A correction is strict: no extra keys and no dropped scope coordinate."""
        extra = _correction().model_dump()
        extra["operator_note"] = "trust me"
        with pytest.raises(ValidationError):
            PnlExecutionCorrectionData.model_validate(extra)
        missing = _correction().model_dump()
        missing.pop("scope_sequence")
        with pytest.raises(ValidationError):
            PnlExecutionCorrectionData.model_validate(missing)

    def test_both_envelopes_require_the_execution_history(self) -> None:
        """Neither P&L envelope may omit the correction disclosure."""
        series_payload = _series_data().model_dump()
        series_payload.pop("execution_history")
        with pytest.raises(ValidationError):
            PnlSeriesData.model_validate(series_payload)
        timeline_payload = _timeline_data().model_dump()
        timeline_payload.pop("execution_history")
        with pytest.raises(ValidationError):
            PnlTimelineData.model_validate(timeline_payload)
