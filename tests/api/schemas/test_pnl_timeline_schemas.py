"""Tests for the strict P&L timeline series response schemas (Phase 5A).

Pins the strict-body contract: every monetary field accepts ``None`` (honest
incompleteness), unknown fields are rejected (``extra='forbid'``), and the
valuation-status discriminator is a closed literal.
"""

from datetime import UTC
from datetime import datetime

import pytest
from pydantic import ValidationError

from snapper.api.schemas.pnl_timeline import PnlInstrumentContributionData
from snapper.api.schemas.pnl_timeline import PnlSeriesData
from snapper.api.schemas.pnl_timeline import PnlSeriesResponse
from snapper.api.schemas.pnl_timeline import PnlTimelinePointData
from snapper.api.schemas.pnl_timeline import PnlValuationStatus

_NOW = datetime(2026, 7, 20, 12, 0, tzinfo=UTC)


def _contribution() -> PnlInstrumentContributionData:
    """Build a fully-null contribution (an untrusted point's shape)."""
    return PnlInstrumentContributionData(
        instrument_public_id="i1",
        realized_pnl=None,
        fee_pnl=None,
        accrual_pnl=None,
        unrealized_pnl=None,
    )


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
        per_instrument=[
            PnlInstrumentContributionData(
                instrument_public_id="i1",
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
        calc_version="5A.1",
        points=[_point()],
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
        assert response.payload.points[0].valuation_status == "complete"

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
            per_instrument=[_contribution()],
        )
        assert point.net_pnl is None
        assert point.per_instrument[0].realized_pnl is None

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
            "per_instrument": [],
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
            "per_instrument": [],
        }
        with pytest.raises(ValidationError):
            PnlTimelinePointData.model_validate(payload)
