"""Strict response schemas for portfolio reconciliation truth."""

from datetime import datetime
from typing import Literal

from snapper.api.schemas.base import StrictBody
from snapper.application.portfolio.reconciliation_methods import PortfolioReconciliationMethod
from snapper.core.json_types import JsonObject

type PortfolioReconciliationEvaluationStatus = Literal[
    "matched",
    "mismatched",
    "incomplete",
    "unsupported",
    "error",
]
type PortfolioReconciliationEffectiveStatus = Literal[
    "matched",
    "mismatched",
    "incomplete",
    "unsupported",
    "error",
    "stale",
    "clock_error",
    "corrupt",
]


class PortfolioReconciliationDriftEpisode(StrictBody):
    """One open durable portfolio-drift episode."""

    public_id: str
    status: Literal["open"]
    opened_at: datetime
    trigger_observation_id: int
    last_observation_id: int
    details_source_observation_id: int
    latest_full_mismatch_count: int


class PortfolioReconciliationView(StrictBody):
    """Fail-closed read-time portfolio reconciliation truth."""

    method: PortfolioReconciliationMethod | None
    evaluation_status: PortfolioReconciliationEvaluationStatus | None
    effective_status: PortfolioReconciliationEffectiveStatus
    is_authoritative: bool
    evaluated_at: datetime | None
    current_observation_id: int | None
    last_full_observation_id: int | None
    detail_source_observation_id: int | None
    last_full_outcome: Literal["matched", "mismatched"] | None
    consecutive_full_mismatches: int
    anchor_public_id: str | None
    venue_account_state_public_id: str | None
    venue_account_observation_id: int | None
    source_watermark_kind: str | None
    source_watermark: int | None
    expected: JsonObject | None
    actual: JsonObject | None
    difference: JsonObject | None
    tolerance: JsonObject | None
    reconciled_at: datetime | None
    authoritative_until: datetime | None
    error: str | None
    open_drift_episode: PortfolioReconciliationDriftEpisode | None
