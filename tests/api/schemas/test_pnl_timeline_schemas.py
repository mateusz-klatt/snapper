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
            valuation_status="incomplete",
            incompleteness_reasons=_multi_cause_reasons(),
            per_instrument=[_contribution()],
            attribution=[_attribution()],
        )
        assert point.net_pnl is None
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
