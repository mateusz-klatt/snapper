"""Strict response schemas for the P&L timeline series and marker APIs.

Models the ``GET /api/portfolio/pnl/series`` response: a per-point
Net-P&L-since-activation series with a realized / fee / accrual / unrealized /
net decomposition, plus each point's per-instrument and origin/strategy
attribution contributions and the series-level provenance (granularity,
valuation currency, mark source, pinned FX rate sources, and the reconstruction
``calc_version``).
Every monetary field is ``float | None`` so an incomplete point (a missing mark,
or untrusted cumulatives) is transported honestly as ``null`` rather than a
fabricated zero (checklist #7 / #10).
"""

from datetime import datetime
from typing import Literal

from snapper.api.schemas.base import PayloadResponse
from snapper.api.schemas.base import StrictBody
from snapper.api.schemas.base import StrictDataSchema

type PnlValuationStatus = Literal["complete", "incomplete"]
"""Whether a point's mark-to-market valuation is trustworthy or withheld."""

type PnlAttributionOrigin = Literal["manual", "plan", "system", "unattributed"]
"""Proven initiating origin for one composite attribution bucket."""

type PnlMarkerOutcome = Literal["executed", "rejected", "no_fill"]
"""Observable execution outcome carried by a timeline decision marker."""


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


class PnlAttributionContributionData(StrictBody):
    """One composite origin/strategy contribution to a series point.

    ``strategy_name`` is ``None`` when no stable signal strategy identity was
    proven. Every monetary field is withheld when the point's cumulatives are
    untrusted, matching both the aggregate and per-instrument projections.
    """

    origin: PnlAttributionOrigin
    strategy_name: str | None
    realized_pnl: float | None
    fee_pnl: float | None
    accrual_pnl: float | None
    unrealized_pnl: float | None


class PnlTimelinePointData(StrictBody):
    """One point on the P&L series.

    ``realized_pnl`` / ``fee_pnl`` / ``accrual_pnl`` are cumulative since
    activation; ``unrealized_pnl`` and ``net_pnl`` are the mark-dependent stocks
    that go ``None`` on an incomplete point. ``valuation_status`` names why.
    ``attribution`` carries composite origin/strategy buckets whose components
    reconcile with the point totals.
    """

    point_time: datetime
    realized_pnl: float | None
    fee_pnl: float | None
    accrual_pnl: float | None
    unrealized_pnl: float | None
    net_pnl: float | None
    valuation_status: PnlValuationStatus
    per_instrument: list[PnlInstrumentContributionData]
    attribution: list[PnlAttributionContributionData]


class PnlFxRateSourceData(StrictBody):
    """One used FX plane with conversion and symbol directions.

    ``source_currency`` is converted into ``valuation_currency`` while
    ``base_currency`` and ``quote_currency`` identify the exact oriented candle
    series pinned on ``exchange`` for at least one contributing instrument.
    """

    source_currency: str
    valuation_currency: str
    base_currency: str
    quote_currency: str
    exchange: str


class PnlFillMarkerData(StrictBody):
    """One execution marker projected from the immutable fill ledger."""

    kind: Literal["fill"] = "fill"
    marker_time: datetime
    instrument_public_id: str
    side: str
    size: float
    price: float | None
    execution_public_id: str
    order_public_id: str
    outcome: Literal["executed"] = "executed"
    status: str


class PnlSignalMarkerData(StrictBody):
    """One source signal, including signals that never produced an execution."""

    kind: Literal["signal"] = "signal"
    marker_time: datetime
    instrument_public_id: str
    side: str
    strategy_name: str | None
    strength: float
    reason: str
    price: float | None
    signal_public_id: str
    outcome: Literal["executed", "no_fill"]
    status: Literal["executed", "no_fill"]


class PnlAiDecisionMarkerData(StrictBody):
    """One append-only AI decision event, whether executed or declined."""

    kind: Literal["ai_decision"] = "ai_decision"
    marker_time: datetime
    instrument_public_id: str
    strategy_public_id: str
    review_public_id: str
    event_public_id: str
    decision: str | None
    rationale: str | None
    outcome: PnlMarkerOutcome
    status: str


type PnlTimelineMarkerData = PnlFillMarkerData | PnlSignalMarkerData | PnlAiDecisionMarkerData
"""Strict marker union distinguished by each model's literal ``kind`` field."""


class _PnlSeriesFields[TypeT: str](StrictDataSchema[TypeT]):
    """Fields shared by series-only and marker-bearing timeline payloads."""

    wallet_public_id: str
    mode: str
    granularity: str
    valuation_ccy: str
    from_time: datetime
    to_time: datetime
    as_of: datetime
    mark_source: str
    rate_sources: list[PnlFxRateSourceData]
    calc_version: str
    points: list[PnlTimelinePointData]


class PnlSeriesData(_PnlSeriesFields[Literal["pnl_series"]]):
    """The P&L timeline series payload with its provenance envelope.

    Carries the requested scope and window echoed back for the client, the
    valuation currency and mark source that value the series (checklist #10 —
    currency is explicit, never assumed), the reconstruction ``calc_version``,
    and the ordered points at the requested granularity.
    """

    type: Literal["pnl_series"] = "pnl_series"


class PnlSeriesResponse(PayloadResponse[Literal["pnl_series"], PnlSeriesData]):
    """Singleton REST response wrapping one P&L timeline series."""

    type: Literal["pnl_series"] = "pnl_series"


class PnlTimelineData(_PnlSeriesFields[Literal["pnl_timeline"]]):
    """Series payload augmented with bounded, explicitly disclosed markers."""

    type: Literal["pnl_timeline"] = "pnl_timeline"
    marker_limit: int
    markers_truncated: bool
    markers: list[PnlTimelineMarkerData]


class PnlTimelineResponse(PayloadResponse[Literal["pnl_timeline"], PnlTimelineData]):
    """Singleton REST response wrapping a series and its decision markers."""

    type: Literal["pnl_timeline"] = "pnl_timeline"
