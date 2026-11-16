"""Wire schemas for bounded market-snapshot freshness observations."""

from datetime import datetime
from typing import Literal

from snapper.api.schemas.base import PayloadResponse
from snapper.api.schemas.base import StrictBody
from snapper.api.schemas.base import StrictDataSchema
from snapper.core.market_snapshot_freshness import FreshnessFailureReason
from snapper.core.market_snapshot_freshness import FxExclusionReason


class MarketSnapshotFreshnessItem(StrictBody):
    """Original snapshot evidence for one resolved refresh dependency.

    Attributes:
        instrument_public_id: Exact venue-specific identity.
        native_symbol: Native exchange symbol from resolved metadata.
        exchange: Source exchange.
        base: Base currency or asset.
        quote: Quote currency.
        asset_type: Current asset classification.
        snapshot_timestamp: Original snapshot timestamp, null if missing.
        age_seconds: Signed age at checked_at, null if time evidence is unusable.
        reason: Refusal reason, null only for fresh, positive finite price evidence.
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


class ExcludedMarketSnapshotFxInstrument(StrictBody):
    """Metadata for a candidate excluded only from this configured FX refresh scope.

    Attributes:
        instrument_public_id: Exact excluded instrument identity.
        native_symbol: Native exchange symbol.
        exchange: Unselected FX refresh venue.
        base: Base currency or asset.
        quote: Quote currency.
        asset_type: Current asset classification.
        reason: Explicit refresh-configuration exclusion reason.
    """

    instrument_public_id: str
    native_symbol: str
    exchange: str
    base: str
    quote: str
    asset_type: str
    reason: FxExclusionReason = "fx_venue_not_selected"


class MarketSnapshotFreshnessData(StrictDataSchema[Literal["market_snapshot_freshness"]]):
    """Freshness-only readiness for an explicitly requested expanded scope.

    This response does not certify FX consistency, contract valuation or order
    admission. Every FX candidate on the explicitly configured refresh venues
    must be fresh here. The converter independently selects and validates any
    eligible evidence, including fresh planes outside this refresh selection.

    Attributes:
        type: Payload discriminator.
        requested_instrument_public_ids: Explicit deduplicated requested scope.
        configured_fx_exchanges: Explicit deduplicated FX refresh venue selection.
        scope_digest: SHA256 of normalized scope and classified resolved metadata.
        checked_at: Observation time captured after all database reads.
        freshness_ready: Whether the complete scope has fresh, usable prices.
        unresolved_fx_currencies: Non-USD quotes without configured FX candidates.
        instruments: Required requested instruments and configured FX dependencies.
        excluded_fx_instruments: Explicit metadata audit of unselected FX candidates.
    """

    type: Literal["market_snapshot_freshness"] = "market_snapshot_freshness"
    requested_instrument_public_ids: list[str]
    configured_fx_exchanges: list[str]
    scope_digest: str
    checked_at: datetime
    freshness_ready: bool
    unresolved_fx_currencies: list[str]
    instruments: list[MarketSnapshotFreshnessItem]
    excluded_fx_instruments: list[ExcludedMarketSnapshotFxInstrument]


class MarketSnapshotFreshnessResponse(
    PayloadResponse[Literal["market_snapshot_freshness_response"], MarketSnapshotFreshnessData]
):
    """Envelope for the explicit market-snapshot freshness GET route."""

    type: Literal["market_snapshot_freshness_response"] = "market_snapshot_freshness_response"
