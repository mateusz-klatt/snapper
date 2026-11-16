"""Wire schemas for bounded market-snapshot freshness observations."""

from datetime import datetime
from typing import Literal

from snapper.api.schemas.base import PayloadResponse
from snapper.api.schemas.base import StrictBody
from snapper.api.schemas.base import StrictDataSchema
from snapper.core.market_snapshot_freshness import FreshnessFailureReason


class MarketSnapshotFreshnessItem(StrictBody):
    """Original snapshot evidence for one resolved refresh dependency.

    Attributes:
        instrument_public_id: Exact venue-specific identity.
        exchange: Source exchange.
        base: Base currency or asset.
        quote: Quote currency.
        snapshot_timestamp: Original snapshot timestamp, null if missing.
        age_seconds: Signed age at checked_at, null if time evidence is unusable.
        reason: Refusal reason, null only for fresh, positive finite price evidence.
    """

    instrument_public_id: str
    exchange: str
    base: str
    quote: str
    snapshot_timestamp: datetime | None
    age_seconds: float | None
    reason: FreshnessFailureReason | None


class MarketSnapshotFreshnessData(StrictDataSchema[Literal["market_snapshot_freshness"]]):
    """Freshness-only readiness for an explicitly requested expanded scope.

    This response does not certify FX consistency, contract valuation or order
    admission. Every alternative FX candidate is required to be fresh here;
    the USD converter independently selects and validates conversion evidence.

    Attributes:
        type: Payload discriminator.
        requested_instrument_public_ids: Explicit deduplicated requested scope.
        checked_at: Observation time captured after all database reads.
        freshness_ready: Whether the complete scope has fresh, usable prices.
        unresolved_fx_currencies: Non-USD quotes with no metadata FX candidates.
        instruments: All resolved requested instruments and FX dependencies.
    """

    type: Literal["market_snapshot_freshness"] = "market_snapshot_freshness"
    requested_instrument_public_ids: list[str]
    checked_at: datetime
    freshness_ready: bool
    unresolved_fx_currencies: list[str]
    instruments: list[MarketSnapshotFreshnessItem]


class MarketSnapshotFreshnessResponse(
    PayloadResponse[Literal["market_snapshot_freshness_response"], MarketSnapshotFreshnessData]
):
    """Envelope for the explicit market-snapshot freshness GET route."""

    type: Literal["market_snapshot_freshness_response"] = "market_snapshot_freshness_response"
