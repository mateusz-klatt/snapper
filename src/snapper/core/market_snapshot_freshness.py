"""Shared refusal vocabulary for market-snapshot freshness observations."""

from typing import Literal

type FreshnessFailureReason = Literal[
    "missing_snapshot",
    "invalid_timestamp",
    "future_timestamp",
    "stale_snapshot",
    "invalid_price",
    "unsupported_exchange",
]
