"""Keep the writer's frozen scope digest compatible with the established read-only API."""

from uuid import UUID

import pytest

from snapper.application.market_snapshot_refresh.freshness import (
    market_snapshot_refresh_scope_digest,
)
from snapper.core.market_snapshot_refresh_scope import RefreshTargetMetadata
from snapper.core.market_snapshot_refresh_scope import freeze_refresh_scope
from snapper.data.repository_types import MarketSnapshotRefreshTargetRow


def _target(identity: int, exchange: str, quote: str) -> RefreshTargetMetadata:
    """Construct explicit synthetic current instrument metadata.

    Args:
        identity: Stable UUID suffix for this test.
        exchange: Exact source venue.
        quote: Currency needed by the main instrument or provided by an FX pair.

    Returns:
        The six existing digest fields without wire-alias or price evidence.
    """
    return {
        "instrument_public_id": str(UUID(int=identity)),
        "native_symbol": f"SYNTHETIC-{identity}",
        "exchange": exchange,
        "base": "BTC" if identity == 1 else "EUR",
        "quote": quote,
        "asset_type": "crypto" if identity == 1 else "forex",
    }


@pytest.mark.parametrize("fx_venues", [("kraken",), ("walutomat",), ("kraken", "walutomat")])
@pytest.mark.parametrize("reverse", [False, True])
def test_writer_scope_digest_matches_api_bytes(fx_venues: tuple[str, ...], reverse: bool) -> None:
    """Use the same complete required and excluded metadata payload on both paths.

    Given: Multiple direct FX candidates, including a venue outside refresh support.
    When: The writer freezes an explicit selection in either metadata order.
    Then: Its digest exactly equals the existing API's digest for that same classification.
    """
    rows = [
        _target(1, "kraken", "EUR"),
        _target(2, "kraken", "USD"),
        _target(3, "walutomat", "USD"),
        _target(4, "polygon", "USD"),
    ]
    if reverse:
        rows.reverse()
    scope = freeze_refresh_scope([str(UUID(int=1))], fx_venues, rows)
    required = {
        target.instrument_public_id: MarketSnapshotRefreshTargetRow(**target.metadata())
        for target in scope.required
    }
    excluded = {
        target.instrument_public_id: MarketSnapshotRefreshTargetRow(**target.metadata())
        for target in scope.excluded
    }
    assert scope.scope_digest == market_snapshot_refresh_scope_digest(
        list(scope.requested_instrument_public_ids),
        list(scope.configured_fx_exchanges),
        required,
        excluded,
    )
    assert str(UUID(int=4)) in excluded
    assert scope.unresolved_fx_currencies == ()
