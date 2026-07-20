"""Strict response schemas for the P&L timeline series API (Phase 5A).

Models the ``GET /api/portfolio/pnl/series`` response: a per-point
Net-P&L-since-activation series with a realized / fee / accrual / unrealized /
net decomposition, plus each point's per-instrument contributions and the
series-level provenance (granularity, valuation currency, mark source, and the
reconstruction ``calc_version``). Every monetary field is ``float | None`` so an
incomplete point (a missing mark, or untrusted cumulatives) is transported
honestly as ``null`` rather than a fabricated zero (checklist #7 / #10).
"""

from datetime import datetime
from typing import Literal

from snapper.api.schemas.base import PayloadResponse
from snapper.api.schemas.base import StrictBody
from snapper.api.schemas.base import StrictDataSchema

type PnlValuationStatus = Literal["complete", "incomplete"]
"""Whether a point's mark-to-market valuation is trustworthy or withheld."""


class PnlInstrumentContributionData(StrictBody):
    """One instrument's contribution to a series point.

    Every field is ``None`` together when the point's cumulatives are untrusted;
    ``unrealized_pnl`` alone is ``None`` when the instrument is held but has no
    mark or seeded entry for that minute.
    """

    instrument_public_id: str
    realized_pnl: float | None
    fee_pnl: float | None
    accrual_pnl: float | None
    unrealized_pnl: float | None


class PnlTimelinePointData(StrictBody):
    """One point on the P&L series.

    ``realized_pnl`` / ``fee_pnl`` / ``accrual_pnl`` are cumulative since
    activation; ``unrealized_pnl`` and ``net_pnl`` are the mark-dependent stocks
    that go ``None`` on an incomplete point. ``valuation_status`` names why.
    """

    point_time: datetime
    realized_pnl: float | None
    fee_pnl: float | None
    accrual_pnl: float | None
    unrealized_pnl: float | None
    net_pnl: float | None
    valuation_status: PnlValuationStatus
    per_instrument: list[PnlInstrumentContributionData]


class PnlSeriesData(StrictDataSchema[Literal["pnl_series"]]):
    """The P&L timeline series payload with its provenance envelope.

    Carries the requested scope and window echoed back for the client, the
    valuation currency and mark source that value the series (checklist #10 —
    currency is explicit, never assumed), the reconstruction ``calc_version``,
    and the ordered points at the requested granularity.
    """

    type: Literal["pnl_series"] = "pnl_series"
    wallet_public_id: str
    mode: str
    granularity: str
    valuation_ccy: str
    from_time: datetime
    to_time: datetime
    as_of: datetime
    mark_source: str
    calc_version: str
    points: list[PnlTimelinePointData]


class PnlSeriesResponse(PayloadResponse[Literal["pnl_series"], PnlSeriesData]):
    """Singleton REST response wrapping one P&L timeline series."""

    type: Literal["pnl_series"] = "pnl_series"
