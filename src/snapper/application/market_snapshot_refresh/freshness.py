"""Bounded, read-only freshness checks for explicit market-snapshot scopes.

Readiness requires usable prices across the explicitly configured refresh set.
The response retains metadata for every excluded FX candidate.
It does not certify USD conversion: the converter independently rejects
conflicting FX evidence and validates contract semantics at admission time.
"""

import hashlib
import json
import math
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from typing import Protocol
from typing import TypedDict

from snapper.application.pricing.usd_converter import MAX_FUTURE_SKEW_SECONDS
from snapper.application.pricing.usd_converter import STALENESS_THRESHOLD_SECONDS
from snapper.core.market_snapshot_freshness import FreshnessFailureReason
from snapper.core.market_snapshot_freshness import FxExclusionReason
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
        native_symbol: Native exchange symbol from the resolved metadata.
        exchange: Source venue of the resolved instrument.
        base: Instrument base currency or asset.
        quote: Instrument quote currency.
        asset_type: Current asset classification.
        snapshot_timestamp: Original snapshot time, null when absent.
        age_seconds: Signed age at the final observation clock, null if invalid.
        reason: Refusal reason, null only for fresh, usable price evidence.
    """

    instrument_public_id: str
    native_symbol: str
    exchange: str
    base: str
    quote: str
    asset_type: str
    snapshot_timestamp: datetime | None
    age_seconds: float | None
    reason: FreshnessFailureReason | None


@dataclass(frozen=True)
class ExcludedFxInstrument:
    """Metadata for a resolved candidate outside the configured FX refresh venues.

    Attributes:
        instrument_public_id: Excluded instrument identity.
        native_symbol: Its native exchange symbol.
        exchange: Source venue omitted from the explicit FX refresh configuration.
        base: Base asset or currency.
        quote: Quote currency.
        asset_type: Current asset classification.
        reason: Stable explanation for exclusion from this refresh scope.
    """

    instrument_public_id: str
    native_symbol: str
    exchange: str
    base: str
    quote: str
    asset_type: str
    reason: FxExclusionReason = "fx_venue_not_selected"


@dataclass(frozen=True)
class SnapshotFreshnessReport:
    """Freshness result for explicitly requested instruments and configured FX venues.

    Attributes:
        requested_instrument_public_ids: Deduplicated caller-specified scope.
        configured_fx_exchanges: Explicit FX refresh venues, not trading permissions.
        scope_digest: Digest of normalized configuration and resolved metadata.
        checked_at: UTC clock read after every awaited repository operation.
        freshness_ready: Whether every required dependency is fresh and usable.
        unresolved_fx_currencies: Non-USD quotes lacking a configured FX candidate.
        instruments: Requested instruments and configured FX dependencies.
        excluded_fx_instruments: Auditable metadata excluded by FX venue selection.
    """

    requested_instrument_public_ids: list[str]
    configured_fx_exchanges: list[str]
    scope_digest: str
    checked_at: datetime
    freshness_ready: bool
    unresolved_fx_currencies: list[str]
    instruments: list[SnapshotFreshnessItem]
    excluded_fx_instruments: list[ExcludedFxInstrument]


class _ScopeDigestPayload(TypedDict):
    """Stable configuration and metadata payload, excluding all prices and clocks."""

    requested_instrument_public_ids: list[str]
    configured_fx_exchanges: list[str]
    required_instruments: list[MarketSnapshotRefreshTargetRow]
    excluded_fx_instruments: list[MarketSnapshotRefreshTargetRow]


def market_snapshot_refresh_scope_digest(
    requested_instrument_public_ids: list[str],
    configured_fx_exchanges: list[str],
    required_targets: dict[str, MarketSnapshotRefreshTargetRow],
    excluded_targets: dict[str, MarketSnapshotRefreshTargetRow],
) -> str:
    """Hash a validated explicit configuration and its classified metadata universe.

    Args:
        requested_instrument_public_ids: Explicit requested identities.
        configured_fx_exchanges: Explicit configured FX refresh venues.
        required_targets: Required main and FX rows indexed by instrument identity.
        excluded_targets: Excluded FX rows indexed by instrument identity.

    Returns:
        SHA256 of canonical JSON, independent of list or dictionary ordering.
    """
    payload = _ScopeDigestPayload(
        requested_instrument_public_ids=sorted(set(requested_instrument_public_ids)),
        configured_fx_exchanges=sorted(set(configured_fx_exchanges)),
        required_instruments=[required_targets[key] for key in sorted(required_targets)],
        excluded_fx_instruments=[excluded_targets[key] for key in sorted(excluded_targets)],
    )
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


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


def _target_identity_is_explicit(target: MarketSnapshotRefreshTargetRow) -> bool:
    """Apply the collector's exact identity requirements without normalizing metadata.

    Args:
        target: One resolved main instrument or FX dependency.

    Returns:
        Whether identity fields are nonblank, unpadded and contain no native wildcard.
    """
    values = (
        target["instrument_public_id"],
        target["native_symbol"],
        target["base"],
        target["quote"],
    )
    return (
        all(value and value == value.strip() for value in values)
        and "*" not in target["native_symbol"]
    )


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
    if any(not _target_identity_is_explicit(target) for target in targets):
        raise ValueError("Refresh resolution returned incomplete instrument metadata")
    missing = sorted(set(requested).difference(by_id))
    if missing:
        raise ValueError(f"Unknown or unsupported requested instruments: {', '.join(missing)}")
    unsupported = sorted(
        identity
        for identity in requested
        if by_id[identity]["exchange"] not in SNAPSHOT_REFRESH_EXCHANGES
    )
    if unsupported:
        raise ValueError(f"Unsupported requested instrument venues: {', '.join(unsupported)}")
    return by_id


def _configured_fx_scope(fx_exchanges: list[str]) -> list[str]:
    """Validate the explicit FX refresh configuration before querying metadata.

    Args:
        fx_exchanges: Caller-specified FX refresh venues, without an implicit default.

    Returns:
        Supported venue names deduplicated in first-occurrence order.

    Raises:
        ValueError: If the list is empty, oversized or contains unsupported names.
    """
    if not fx_exchanges or len(fx_exchanges) > MAX_REQUESTED_INSTRUMENTS:
        raise ValueError("An explicit bounded nonempty FX exchange list is required")
    if any(exchange not in SNAPSHOT_REFRESH_EXCHANGES for exchange in fx_exchanges):
        raise ValueError("FX exchanges must name supported snapshot refresh venues")
    return list(dict.fromkeys(fx_exchanges))


def _partition_targets(
    requested: list[str],
    configured_fx_exchanges: list[str],
    targets: dict[str, MarketSnapshotRefreshTargetRow],
) -> tuple[dict[str, MarketSnapshotRefreshTargetRow], dict[str, MarketSnapshotRefreshTargetRow]]:
    """Classify every resolved target without dropping excluded FX metadata.

    Args:
        requested: Explicit main instruments, always required regardless of FX venue.
        configured_fx_exchanges: Supported venues selected for FX refreshing.
        targets: Complete bounded resolver output.

    Returns:
        Required and excluded metadata indexed by exact instrument identity.
    """
    required: dict[str, MarketSnapshotRefreshTargetRow] = {}
    excluded: dict[str, MarketSnapshotRefreshTargetRow] = {}
    for identity, target in targets.items():
        if identity in requested or target["exchange"] in configured_fx_exchanges:
            required[identity] = target
        else:
            excluded[identity] = target
    return required, excluded


def _unresolved_fx_currencies(
    requested: list[str],
    configured_fx_exchanges: list[str],
    targets: dict[str, MarketSnapshotRefreshTargetRow],
) -> list[str]:
    """Identify quote currencies with no resolved direct or inverse FX leg.

    Args:
        requested: Explicit submitted-instrument scope.
        configured_fx_exchanges: Explicit FX refresh venues eligible for readiness.
        targets: Current requested instruments and eligible FX candidates.

    Returns:
        Sorted non-USD quote currencies whose candidate set is empty.
    """
    needed = {targets[identity]["quote"] for identity in requested} - {"USD"}
    available: set[str] = set()
    for target in targets.values():
        if target["asset_type"] != "forex" or target["exchange"] not in configured_fx_exchanges:
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
    if reason is None and (price is None or not math.isfinite(price) or price <= 0):
        reason = "invalid_price"
    return SnapshotFreshnessItem(
        instrument_public_id=target["instrument_public_id"],
        native_symbol=target["native_symbol"],
        exchange=target["exchange"],
        base=target["base"],
        quote=target["quote"],
        asset_type=target["asset_type"],
        snapshot_timestamp=timestamp,
        age_seconds=age,
        reason=reason,
    )


async def load_market_snapshot_freshness(
    repository: SnapshotFreshnessRepository,
    instrument_public_ids: list[str],
    *,
    fx_exchanges: list[str],
    now: Callable[[], datetime] | None = None,
) -> SnapshotFreshnessReport:
    """Observe explicitly requested instruments and configured FX dependencies.

    Every candidate on the configured FX venues must be fresh. Other candidates
    remain visible in excluded metadata. This is refresh configuration, not a
    restriction on the converter's evidence selection or a trading permission.
    No absent instrument or FX venue list ever means all instruments or venues.

    Args:
        repository: Current metadata and snapshot read boundary.
        instrument_public_ids: Explicit operator-selected identities.
        fx_exchanges: Explicit nonempty FX refresh venue selection, without a default.
        now: Optional UTC clock used after all awaited reads.

    Returns:
        Full expanded freshness evidence and unresolved FX dependencies.

    Raises:
        ValueError: When the explicit scope cannot be safely resolved or bounded.
    """
    requested = _requested_scope(instrument_public_ids)
    configured = _configured_fx_scope(fx_exchanges)
    targets = await repository.resolve_market_snapshot_refresh_targets(requested)
    by_id = _validate_targets(requested, targets)
    required, excluded = _partition_targets(requested, configured, by_id)
    snapshots = await repository.get_active_market_snapshot_freshness(sorted(required))
    snapshot_by_id = {row["instrument_public_id"]: row for row in snapshots}
    checked_at = now() if now is not None else datetime.now(UTC)
    items = [
        _freshness_item(required[identity], snapshot_by_id.get(identity), checked_at)
        for identity in sorted(required)
    ]
    unresolved = _unresolved_fx_currencies(requested, configured, required)
    return SnapshotFreshnessReport(
        requested_instrument_public_ids=requested,
        configured_fx_exchanges=configured,
        scope_digest=market_snapshot_refresh_scope_digest(
            requested, configured, required, excluded
        ),
        checked_at=checked_at,
        freshness_ready=not unresolved and all(item.reason is None for item in items),
        unresolved_fx_currencies=unresolved,
        instruments=items,
        excluded_fx_instruments=[ExcludedFxInstrument(**excluded[key]) for key in sorted(excluded)],
    )
