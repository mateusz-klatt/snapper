"""Shared scope limits and refusal vocabulary for market-snapshot freshness observations."""

from typing import Literal

MAX_REFRESH_TARGETS = 1000
STALENESS_THRESHOLD_SECONDS = 300
MAX_FUTURE_SKEW_SECONDS = 5

type FreshnessFailureReason = Literal[
    "missing_snapshot",
    "invalid_timestamp",
    "future_timestamp",
    "stale_snapshot",
    "invalid_price",
]

type FxExclusionReason = Literal["fx_venue_not_selected"]
