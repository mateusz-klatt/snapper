"""Bounded, read-only freshness checks for explicit market-snapshot scopes.

Readiness requires usable prices across the complete resolved refresh set.
It does not certify USD conversion: the converter independently rejects
conflicting FX evidence and validates contract semantics at admission time.
"""

import math
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from typing import Protocol

from snapper.application.pricing.usd_converter import MAX_FUTURE_SKEW_SECONDS
from snapper.application.pricing.usd_converter import STALENESS_THRESHOLD_SECONDS
from snapper.core.market_snapshot_freshness import FreshnessFailureReason
from snapper.data.repository_types import MarketSnapshotFreshnessRow
from snapper.data.repository_types import MarketSnapshotRefreshTargetRow

MAX_REQUESTED_INSTRUMENTS = 100
MAX_REFRESH_TARGETS = 1000
SNAPSHOT_REFRESH_EXCHANGES = frozenset({"kraken", "kraken_futures", "kraken_equities", "walutomat"})


class SnapshotFreshnessRepository(Protocol):
    """The two bounded reads required for refresh-scope observation."""

    async def resolve_market_snapshot_refresh_targets(
        self, instrument_public_ids: list[str]
    ) -> list[MarketSnapshotRefreshTargetRow]:
        """Resolve requested instruments and their metadata-only FX dependencies.

        Args:
            instrument_public_ids: Explicit, nonempty requested identities.

        Returns:
            Current requested instruments and every eligible FX candidate.
        """
        ...

    async def get_active_market_snapshot_freshness(
        self, instrument_public_ids: list[str]
    ) -> list[MarketSnapshotFreshnessRow]:
        """Read current price evidence only for the given resolved identities.

        Args:
            instrument_public_ids: Bounded, explicit resolved identities.

        Returns:
            Current snapshot evidence, without synthesizing missing prices.
        """
        ...


@dataclass(frozen=True)
class SnapshotFreshnessItem:
    """Readiness evidence for one requested instrument or FX dependency.

    Attributes:
        instrument_public_id: Exact venue-specific instrument identity.
        exchange: Source venue of the resolved instrument.
        base: Instrument base currency or asset.
        quote: Instrument quote currency.
        snapshot_timestamp: Original snapshot time, null when absent.
        age_seconds: Signed age at the final observation clock, null if invalid.
        reason: Refusal reason, null only for fresh, usable price evidence.
    """

    instrument_public_id: str
    exchange: str
    base: str
    quote: str
    snapshot_timestamp: datetime | None
    age_seconds: float | None
    reason: FreshnessFailureReason | None


@dataclass(frozen=True)
class SnapshotFreshnessReport:
    """Conservative freshness result for one explicit expanded scope.

    Attributes:
        requested_instrument_public_ids: Deduplicated caller-specified scope.
        checked_at: UTC clock read after every awaited repository operation.
        freshness_ready: Whether every resolved dependency is fresh and usable.
        unresolved_fx_currencies: Non-USD quotes lacking any FX candidate.
        instruments: Full resolved set, including missing or unsupported evidence.
    """

    requested_instrument_public_ids: list[str]
    checked_at: datetime
    freshness_ready: bool
    unresolved_fx_currencies: list[str]
    instruments: list[SnapshotFreshnessItem]


def _requested_scope(instrument_public_ids: list[str]) -> list[str]:
    """Validate and deduplicate the explicit scope before any repository read.

    Args:
        instrument_public_ids: Caller-supplied instrument identities.

    Returns:
        Nonempty identities in their first-occurrence order.

    Raises:
        ValueError: When the request is empty, oversized or contains blank ids.
    """
    if not instrument_public_ids or len(instrument_public_ids) > MAX_REQUESTED_INSTRUMENTS:
        raise ValueError(f"Request between 1 and {MAX_REQUESTED_INSTRUMENTS} instruments")
    if any(not value or value.strip() != value for value in instrument_public_ids):
        raise ValueError(
            "Instrument identities must be nonempty and have no surrounding whitespace"
        )
    return list(dict.fromkeys(instrument_public_ids))


def _validate_targets(
    requested: list[str], targets: list[MarketSnapshotRefreshTargetRow]
) -> dict[str, MarketSnapshotRefreshTargetRow]:
    """Require complete requested identity resolution and bound expansion.

    Args:
        requested: Validated requested identities.
        targets: Resolver output, including metadata-only FX dependencies.

    Returns:
        Resolved rows indexed by their exact instrument identity.

    Raises:
        ValueError: When resolution is incomplete, duplicated or excessive.
    """
    if len(targets) > MAX_REFRESH_TARGETS:
        raise ValueError(f"Expanded refresh scope exceeds {MAX_REFRESH_TARGETS} instruments")
    by_id = {target["instrument_public_id"]: target for target in targets}
    if len(by_id) != len(targets):
        raise ValueError("Refresh resolution returned duplicate instrument identities")
    if any(
        not value.strip()
        for target in targets
        for value in (target["base"], target["quote"], target["native_symbol"])
    ):
        raise ValueError("Refresh resolution returned incomplete instrument metadata")
    missing = sorted(set(requested).difference(by_id))
    if missing:
        raise ValueError(f"Unknown or unsupported requested instruments: {', '.join(missing)}")
    return by_id


def _unresolved_fx_currencies(
    requested: list[str], targets: dict[str, MarketSnapshotRefreshTargetRow]
) -> list[str]:
    """Identify quote currencies with no resolved direct or inverse FX leg.

    Args:
        requested: Explicit submitted-instrument scope.
        targets: Current requested instruments and eligible FX candidates.

    Returns:
        Sorted non-USD quote currencies whose candidate set is empty.
    """
    needed = {targets[identity]["quote"] for identity in requested} - {"USD"}
    available: set[str] = set()
    for target in targets.values():
        if target["asset_type"] != "forex" or target["exchange"] == "paper":
            continue
        if target["quote"] == "USD":
            available.add(target["base"])
        if target["base"] == "USD":
            available.add(target["quote"])
    return sorted(needed - available)


def _timestamp_state(
    timestamp: datetime | None, now: datetime
) -> tuple[float | None, FreshnessFailureReason | None]:
    """Evaluate original evidence time against the shared inclusive boundaries.

    Args:
        timestamp: Original snapshot timestamp, without repair or restamping.
        now: Final UTC observation time.

    Returns:
        Signed age and a refusal reason, or null reason for an admissible time.
    """
    if timestamp is None:
        return None, "missing_snapshot"
    if timestamp.utcoffset() is None:
        return None, "invalid_timestamp"
    age = (now - timestamp).total_seconds()
    if age < -MAX_FUTURE_SKEW_SECONDS:
        return age, "future_timestamp"
    if age > STALENESS_THRESHOLD_SECONDS:
        return age, "stale_snapshot"
    return age, None


def _freshness_item(
    target: MarketSnapshotRefreshTargetRow,
    snapshot: MarketSnapshotFreshnessRow | None,
    now: datetime,
) -> SnapshotFreshnessItem:
    """Combine venue support, original timestamp and price usability.

    Args:
        target: Resolved metadata for one exact instrument.
        snapshot: Its current snapshot evidence, or null when absent.
        now: Final UTC observation time.

    Returns:
        Per-instrument evidence without publishing price values.
    """
    timestamp = snapshot["timestamp"] if snapshot is not None else None
    age, reason = _timestamp_state(timestamp, now)
    price = snapshot["last_price"] if snapshot is not None else None
    if target["exchange"] not in SNAPSHOT_REFRESH_EXCHANGES:
        reason = "unsupported_exchange"
    elif reason is None and (price is None or not math.isfinite(price) or price <= 0):
        reason = "invalid_price"
    return SnapshotFreshnessItem(
        instrument_public_id=target["instrument_public_id"],
        exchange=target["exchange"],
        base=target["base"],
        quote=target["quote"],
        snapshot_timestamp=timestamp,
        age_seconds=age,
        reason=reason,
    )


async def load_market_snapshot_freshness(
    repository: SnapshotFreshnessRepository,
    instrument_public_ids: list[str],
    *,
    now: Callable[[], datetime] | None = None,
) -> SnapshotFreshnessReport:
    """Observe a bounded explicit scope and all resolved FX dependencies.

    Every candidate must be fresh, including alternative FX venues. This is
    deliberately conservative and does not replace the converter's conflict
    checks or authorize an order. No instrument list ever means all instruments.

    Args:
        repository: Current metadata and snapshot read boundary.
        instrument_public_ids: Explicit operator-selected identities.
        now: Optional UTC clock used after all awaited reads.

    Returns:
        Full expanded freshness evidence and unresolved FX dependencies.

    Raises:
        ValueError: When the explicit scope cannot be safely resolved or bounded.
    """
    requested = _requested_scope(instrument_public_ids)
    targets = await repository.resolve_market_snapshot_refresh_targets(requested)
    by_id = _validate_targets(requested, targets)
    snapshots = await repository.get_active_market_snapshot_freshness(sorted(by_id))
    snapshot_by_id = {row["instrument_public_id"]: row for row in snapshots}
    checked_at = now() if now is not None else datetime.now(UTC)
    items = [
        _freshness_item(by_id[identity], snapshot_by_id.get(identity), checked_at)
        for identity in sorted(by_id)
    ]
    unresolved = _unresolved_fx_currencies(requested, by_id)
    return SnapshotFreshnessReport(
        requested_instrument_public_ids=requested,
        checked_at=checked_at,
        freshness_ready=not unresolved and all(item.reason is None for item in items),
        unresolved_fx_currencies=unresolved,
        instruments=items,
    )
