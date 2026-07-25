"""Strict response schemas for the P&L timeline series and marker APIs.

Models the ``GET /api/portfolio/pnl/series`` response: a per-point
Net-P&L-since-activation series with a realized / fee / accrual / unrealized /
net decomposition, plus each point's per-instrument and origin/strategy
attribution contributions and the series-level provenance (granularity,
valuation currency, mark source, pinned FX rate sources, and the reconstruction
``calc_version``).
Every monetary field is ``float | None`` so an incomplete point (a missing mark,
or untrusted cumulatives) is transported honestly as ``null`` rather than a
fabricated zero (checklist #7 / #10). Every incomplete point also carries the
closed causal records stamped by its withholding sites.

The envelope also carries ``execution_history``, the permanent series-level
disclosure of every operator correction folded into the reconstruction. Point
level cannot carry it — a ``complete`` point is forbidden from carrying reasons —
and a correction is not incompleteness: the numbers are trustworthy precisely
because the repudiated booking was removed. So it rides the envelope instead,
where it can never be silently dropped.
"""

from datetime import datetime
from typing import Literal
from typing import Self

from pydantic import model_validator

from snapper.api.schemas.base import PayloadResponse
from snapper.api.schemas.base import StrictBody
from snapper.api.schemas.base import StrictDataSchema
from snapper.application.portfolio.pnl_timeline import (
    PnlIncompletenessReason as DomainPnlIncompletenessReason,
)
from snapper.application.portfolio.pnl_timeline import (
    PnlWithholdingScope as DomainPnlWithholdingScope,
)
from snapper.application.portfolio.pnl_timeline import (
    PnlWithholdingTier as DomainPnlWithholdingTier,
)
from snapper.data.repository_types import ExecutionAnnulmentReason as DomainAnnulmentReason

type PnlValuationStatus = Literal["complete", "incomplete"]
"""Whether a point's mark-to-market valuation is trustworthy or withheld."""

type PnlIncompletenessReason = DomainPnlIncompletenessReason
"""Closed transport taxonomy for why a P&L point was withheld."""

type PnlWithholdingTier = DomainPnlWithholdingTier
"""Closed transport tier for a withholding cause."""

type PnlWithholdingScope = DomainPnlWithholdingScope
"""Closed transport scope for a withholding cause."""

type PnlAttributionOrigin = Literal["manual", "plan", "system", "unattributed"]
"""Proven initiating origin for one composite attribution bucket."""

type PnlMarkerOutcome = Literal["executed", "rejected", "no_fill"]
"""Observable execution outcome carried by a timeline decision marker."""

type PnlExecutionCorrectionReason = DomainAnnulmentReason
"""Closed transport taxonomy for why an operator repudiated one booking."""

type PnlExecutionHistoryStatus = Literal["as_recorded", "operator_corrected"]
"""Whether this scope's effective history is the raw ledger or a corrected fold."""


class PnlIncompletenessReasonData(StrictBody):
    """One causal reason stamped where a point value was withheld.

    Instrument-scoped causes always identify the triggering instrument. Global
    causes retain an instrument only when the detection site proves one, such as
    the instrument whose scope-order regression shadows a minute.
    """

    reason: PnlIncompletenessReason
    withholding_tier: PnlWithholdingTier
    withholding_scope: PnlWithholdingScope
    trigger_instrument_public_id: str | None

    @model_validator(mode="after")
    def _require_instrument_identity(self) -> Self:
        """Reject instrument-scoped causes that omit their proven identity."""
        if self.withholding_scope == "instrument" and self.trigger_instrument_public_id is None:
            raise ValueError("instrument-scoped incompleteness requires a triggering instrument")
        return self


class PnlInstrumentContributionData(StrictBody):
    """One instrument's contribution to a series point.

    ``native_symbol`` and ``exchange`` are nullable display identity fields
    proven from the reconstruction's symbol reference at ``as_of``.
    Every monetary field is ``None`` together when the point's cumulatives are
    untrusted; ``unrealized_pnl`` alone is ``None`` when the instrument is held
    but has no mark or seeded entry for that minute.
    """

    instrument_public_id: str
    native_symbol: str | None
    exchange: str | None
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
    that go ``None`` on an incomplete point. ``incompleteness_reasons`` carries
    every cause established at the withholding sites without deriving causes
    from the null fields. ``attribution`` carries composite origin/strategy
    buckets whose components reconcile with the point totals.

    ``equity`` / ``cash`` / ``position_value`` / ``drawdown`` are the Phase-5B
    observed-equity overlay (decision D13/R4). They are populated ONLY for a
    current-truth USD request from a persisted ``complete`` sample of the current
    anchor epoch whose ``point_time`` equals this point's minute — ``equity`` is
    ``cash_usd + position_value_usd`` taken from the SAME persisted floats, never
    an independent recompute. They are all ``None`` together whenever the minute
    has no qualifying sample, the request is historical or non-USD, or the overlay
    was withheld fail-closed. For a downsampled series the value is the one at the
    bucket's endpoint minute (endpoint-selection); these stocks are never summed
    or averaged across a bucket. The 5A P&L fields above are unaffected by the
    overlay and always come from the live recompute.
    """

    point_time: datetime
    realized_pnl: float | None
    fee_pnl: float | None
    accrual_pnl: float | None
    unrealized_pnl: float | None
    net_pnl: float | None
    equity: float | None
    cash: float | None
    position_value: float | None
    drawdown: float | None
    valuation_status: PnlValuationStatus
    incompleteness_reasons: list[PnlIncompletenessReasonData]
    per_instrument: list[PnlInstrumentContributionData]
    attribution: list[PnlAttributionContributionData]

    @model_validator(mode="after")
    def _require_consistent_incompleteness(self) -> Self:
        """Keep valuation status equivalent to presence of causal reasons."""
        has_reasons = bool(self.incompleteness_reasons)
        if (self.valuation_status == "complete") == has_reasons:
            raise ValueError(
                "complete points require no reasons and incomplete points require reasons"
            )
        return self


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


class PnlEquityCoverageData(StrictBody):
    """Envelope disclosure of the Phase-5B observed-equity overlay (D13/R10/R11).

    ``sampled`` is ``True`` only when the request was current-truth and USD and at
    least one ``complete`` sample of the current anchor epoch backs the requested
    window; every other scope (historical ``as_of``, non-USD, no anchor, no
    sample, or a fail-closed withholding) reports ``sampled=False`` with the
    remaining fields null or zero. ``venue_scope`` is the v1 spot-only equity
    denominator (futures venues are excluded from the basket and the completeness
    denominator, R11). ``external_flows_adjusted`` is ``False`` in v1: deposits and
    withdrawals are NOT rebased out of the observed-equity and drawdown curves
    (R10/D14), so the served metric is honestly labelled observed equity rather
    than a flow-adjusted return. ``complete_minutes`` counts the ``complete``
    sample minutes in the requested window and ``first_minute`` / ``last_minute``
    span them. ``sample_calc_version`` is the exact sample-algorithm version that
    produced the backing samples, distinct from the recompute ``calc_version`` on
    the envelope.
    """

    sampled: bool
    venue_scope: Literal["spot_only"] | None
    external_flows_adjusted: bool | None
    complete_minutes: int
    first_minute: datetime | None
    last_minute: datetime | None
    sample_calc_version: str | None

    @model_validator(mode="after")
    def _require_consistent_coverage(self) -> Self:
        """Keep the disclosure internally honest for both sampled states."""
        provenance = (
            self.venue_scope,
            self.external_flows_adjusted,
            self.first_minute,
            self.last_minute,
            self.sample_calc_version,
        )
        if self.sampled:
            if None in provenance or self.complete_minutes < 1:
                raise ValueError(
                    "a sampled coverage requires full provenance and a complete minute"
                )
        elif any(field is not None for field in provenance) or self.complete_minutes != 0:
            raise ValueError("an unsampled coverage must null every provenance field")
        return self


class PnlExecutionCorrectionData(StrictBody):
    """One operator correction the certified fold applied to this scope (A3).

    A row here is not a request or a listing: the certification fold bound it to
    an in-prefix execution by immutable id, canonical row digest, and scope
    coordinate, and proved no contradicting fill witness exists, before that
    booking was excluded from the numbers this response carries.
    ``scope_sequence`` locates the repudiated booking in its exchange's
    contiguous sequence, so an auditor can name the exact position that was
    corrected without re-reading the ledger. ``correction_time`` is the
    operator's knowledge timestamp: a read whose horizon precedes it neither
    folds nor discloses this correction.
    """

    correction_public_id: str
    target_execution_public_id: str
    exchange: str
    scope_sequence: int
    reason: PnlExecutionCorrectionReason
    correction_time: datetime


class PnlExecutionHistoryData(StrictBody):
    """Envelope disclosure of every operator correction behind this series (A3).

    ``as_recorded`` states that the effective history equals the raw immutable
    ledger; ``operator_corrected`` states that an append-only annulment manifest
    removed at least one booking from it and names each one. The two are kept
    equivalent to the presence of ``corrections`` by a validator, so the status
    can never be a claim the payload does not itself carry — a correction can be
    disclosed silently in neither direction.

    The list is exactly the fold applied to THESE numbers, never a wider manifest
    read: a historical ``as_of`` request whose horizon precedes a correction's
    ``correction_time`` reports the history it actually replayed, which is the
    same reason such a read keeps failing rather than pretending the correction
    was already known.
    """

    status: PnlExecutionHistoryStatus
    corrections: list[PnlExecutionCorrectionData]

    @model_validator(mode="after")
    def _require_consistent_status(self) -> Self:
        """Keep the disclosed status equivalent to the disclosed corrections."""
        if (self.status == "operator_corrected") != bool(self.corrections):
            raise ValueError(
                "operator_corrected requires at least one correction and "
                "as_recorded requires none"
            )
        return self


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
    equity_coverage: PnlEquityCoverageData
    execution_history: PnlExecutionHistoryData
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
