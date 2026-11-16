"""Freshness policy witnesses using bounded in-memory repository responses."""

from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest

from snapper.application.market_snapshot_refresh.freshness import MAX_REFRESH_TARGETS
from snapper.application.market_snapshot_refresh.freshness import MAX_REQUESTED_INSTRUMENTS
from snapper.application.market_snapshot_refresh.freshness import load_market_snapshot_freshness
from snapper.core.market_snapshot_freshness import FreshnessFailureReason
from snapper.data.repository_types import MarketSnapshotFreshnessRow
from snapper.data.repository_types import MarketSnapshotRefreshTargetRow

_NOW = datetime(2026, 11, 16, 15, 0, tzinfo=UTC)


def _target(
    identity: str = "requested",
    *,
    exchange: str = "kraken",
    base: str = "BTC",
    quote: str = "USD",
    asset_type: str = "crypto",
) -> MarketSnapshotRefreshTargetRow:
    """Build one metadata target with explicit venue and currency identity.

    Args:
        identity: Instrument public identity.
        exchange: Source venue name.
        base: Base asset or currency.
        quote: Quote currency.
        asset_type: Asset classification used for FX discovery.

    Returns:
        One typed resolver target.
    """
    return {
        "instrument_public_id": identity,
        "native_symbol": f"{base}/{quote}",
        "exchange": exchange,
        "base": base,
        "quote": quote,
        "asset_type": asset_type,
    }


def _snapshot(
    identity: str = "requested", timestamp: datetime | None = _NOW, price: float | None = 100.0
) -> MarketSnapshotFreshnessRow:
    """Build snapshot evidence without replacing missing or invalid values.

    Args:
        identity: Instrument public identity.
        timestamp: Original observed timestamp.
        price: Last price, including deliberately unusable test values.

    Returns:
        One typed snapshot response row.
    """
    return {"instrument_public_id": identity, "timestamp": timestamp, "last_price": price}


@dataclass
class _Repository:
    """Small typed repository witness recording both bounded read arguments."""

    targets: list[MarketSnapshotRefreshTargetRow]
    snapshots: list[MarketSnapshotFreshnessRow]
    before_snapshots: Callable[[], None] | None = None
    target_queries: list[list[str]] = field(default_factory=list)
    snapshot_queries: list[list[str]] = field(default_factory=list)

    async def resolve_market_snapshot_refresh_targets(
        self, instrument_public_ids: list[str]
    ) -> list[MarketSnapshotRefreshTargetRow]:
        """Return controlled metadata and capture the exact requested scope.

        Args:
            instrument_public_ids: Explicit requested identities.

        Returns:
            Configured metadata rows.
        """
        self.target_queries.append(instrument_public_ids)
        return self.targets

    async def get_active_market_snapshot_freshness(
        self, instrument_public_ids: list[str]
    ) -> list[MarketSnapshotFreshnessRow]:
        """Return controlled snapshots after the optional asynchronous-boundary hook.

        Args:
            instrument_public_ids: Expanded bounded identities.

        Returns:
            Configured snapshot rows.
        """
        self.snapshot_queries.append(instrument_public_ids)
        if self.before_snapshots is not None:
            self.before_snapshots()
        return self.snapshots


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("age", "expected_reason"),
    [
        (-6.0, "future_timestamp"),
        (-5.000001, "future_timestamp"),
        (-5.0, None),
        (0.0, None),
        (300.0, None),
        (300.000001, "stale_snapshot"),
        (301.0, "stale_snapshot"),
    ],
)
async def test_inclusive_shared_time_boundaries(
    age: float, expected_reason: FreshnessFailureReason | None
) -> None:
    """Apply the shared inclusive timestamp boundaries.

    Given: An original snapshot at one of the exact age or skew boundaries.
    When: The bounded observation evaluates that timestamp against its final clock.
    Then: Exactly 300 seconds old and five seconds future remain admissible.
    """
    repository = _Repository([_target()], [_snapshot(timestamp=_NOW - timedelta(seconds=age))])

    report = await load_market_snapshot_freshness(repository, ["requested"], now=lambda: _NOW)

    assert report.instruments[0].reason == expected_reason
    assert report.instruments[0].age_seconds == age
    assert report.freshness_ready is (expected_reason is None)
    assert report.checked_at == _NOW


@pytest.mark.asyncio
@pytest.mark.parametrize("price", [None, 0.0, -1.0, float("nan"), float("inf"), float("-inf")])
async def test_fresh_timestamp_with_unusable_price_refuses(price: float | None) -> None:
    """Refuse fresh timestamps carrying unusable price evidence.

    Given: A fresh snapshot whose price is missing, nonpositive or nonfinite.
    When: Freshness policy evaluates the exact requested instrument.
    Then: The report refuses readiness with an explicit invalid-price reason.
    """
    repository = _Repository([_target()], [_snapshot(price=price)])

    report = await load_market_snapshot_freshness(repository, ["requested"], now=lambda: _NOW)

    assert not report.freshness_ready
    assert report.instruments[0].reason == "invalid_price"


@pytest.mark.asyncio
@pytest.mark.parametrize("include_row", [False, True])
async def test_missing_snapshot_or_timestamp_refuses(include_row: bool) -> None:
    """Preserve missing evidence without manufacturing snapshot timestamps.

    Given: A requested instrument has no snapshot row or a null timestamp.
    When: The policy reports the resolved explicit instrument scope.
    Then: Timestamp and age remain null and readiness is refused.
    """
    repository = _Repository([_target()], [_snapshot(timestamp=None)] if include_row else [])

    report = await load_market_snapshot_freshness(repository, ["requested"], now=lambda: _NOW)

    assert not report.freshness_ready
    assert report.instruments[0].reason == "missing_snapshot"
    assert report.instruments[0].snapshot_timestamp is None
    assert report.instruments[0].age_seconds is None


@pytest.mark.asyncio
async def test_naive_timestamp_refuses_without_datetime_exception() -> None:
    """Report naive timestamps as unusable evidence without arithmetic errors.

    Given: A repository row carries a datetime without an offset.
    When: Freshness policy evaluates the row against a UTC observation clock.
    Then: The response retains the original time and refuses readiness.
    """
    timestamp = _NOW.replace(tzinfo=None)
    repository = _Repository([_target()], [_snapshot(timestamp=timestamp)])

    report = await load_market_snapshot_freshness(repository, ["requested"], now=lambda: _NOW)

    assert not report.freshness_ready
    assert report.instruments[0].reason == "invalid_timestamp"
    assert report.instruments[0].snapshot_timestamp == timestamp
    assert report.instruments[0].age_seconds is None


@pytest.mark.asyncio
@pytest.mark.parametrize("ids", [[], [""], [" "], [" requested"], ["requested "]])
async def test_invalid_scope_never_reads_repository(ids: list[str]) -> None:
    """Reject invalid explicit scopes before any repository access.

    Given: An empty list or a blank or whitespace-padded instrument identity.
    When: The caller requests a bounded freshness observation.
    Then: Input validation raises without either repository query running.
    """
    repository = _Repository([_target()], [_snapshot()])

    with pytest.raises(ValueError):
        await load_market_snapshot_freshness(repository, ids)

    assert repository.target_queries == []
    assert repository.snapshot_queries == []


@pytest.mark.asyncio
async def test_oversized_requested_scope_never_resolves() -> None:
    """Bound the raw requested scope before resolving metadata.

    Given: A request contains more entries than the configured request maximum.
    When: The service validates the explicit instrument list.
    Then: It rejects the request before querying target metadata.
    """
    repository = _Repository([], [])

    with pytest.raises(ValueError, match="Request between"):
        await load_market_snapshot_freshness(
            repository, ["requested"] * (MAX_REQUESTED_INSTRUMENTS + 1)
        )

    assert repository.target_queries == []


@pytest.mark.asyncio
async def test_unknown_requested_identity_never_reads_snapshots() -> None:
    """Require every requested identity to resolve before reading snapshots.

    Given: One caller-requested instrument is absent from the resolver output.
    When: The bounded service checks whether resolution covers the request.
    Then: It refuses the request without silently dropping the missing identity.
    """
    repository = _Repository([_target()], [_snapshot()])

    with pytest.raises(ValueError, match="Unknown or unsupported requested instruments: unknown"):
        await load_market_snapshot_freshness(repository, ["requested", "unknown"])

    assert repository.snapshot_queries == []


@pytest.mark.asyncio
async def test_duplicate_resolver_identity_refuses() -> None:
    """Reject contradictory target identity resolution before selecting evidence.

    Given: Two metadata rows have the same instrument identity and different venues.
    When: The service indexes the resolved explicit scope.
    Then: It refuses duplicates before any snapshot row can mask another.
    """
    repository = _Repository([_target(), _target(exchange="walutomat")], [])

    with pytest.raises(ValueError, match="duplicate instrument identities"):
        await load_market_snapshot_freshness(repository, ["requested"])

    assert repository.snapshot_queries == []


@pytest.mark.asyncio
@pytest.mark.parametrize("blank_field", ["base", "quote", "native_symbol"])
async def test_blank_metadata_refuses_before_snapshot_read(blank_field: str) -> None:
    """Refuse incomplete metadata without claiming a usable resolved scope.

    Given: A target has a blank base, quote or native exchange symbol.
    When: Freshness service validates the metadata resolver output.
    Then: Validation refuses before starting the snapshot evidence query.
    """
    target = _target()
    if blank_field == "base":
        target["base"] = " "
    elif blank_field == "quote":
        target["quote"] = " "
    else:
        target["native_symbol"] = " "
    repository = _Repository([target], [])

    with pytest.raises(ValueError, match="incomplete instrument metadata"):
        await load_market_snapshot_freshness(repository, ["requested"])

    assert repository.snapshot_queries == []


@pytest.mark.asyncio
async def test_expansion_limit_applies_before_snapshot_read() -> None:
    """Bound dependency expansion independently of the original request size.

    Given: Metadata resolution produces more than the allowed expanded identities.
    When: The service prepares its explicit snapshot evidence query.
    Then: It rejects the expansion before executing that query.
    """
    targets = [_target(str(index)) for index in range(MAX_REFRESH_TARGETS + 1)]
    repository = _Repository(targets, [])

    with pytest.raises(ValueError, match="Expanded refresh scope exceeds"):
        await load_market_snapshot_freshness(repository, ["0"])

    assert repository.snapshot_queries == []


@pytest.mark.asyncio
async def test_missing_fx_metadata_cannot_report_fresh_main_as_ready() -> None:
    """Distinguish a fresh main mark from a complete conversion dependency scope.

    Given: A fresh EUR-quoted instrument has no metadata FX candidate.
    When: The service evaluates the requested instrument and resolved dependencies.
    Then: It reports EUR unresolved and freshness readiness remains false.
    """
    repository = _Repository([_target(quote="EUR")], [_snapshot()])

    report = await load_market_snapshot_freshness(repository, ["requested"], now=lambda: _NOW)

    assert report.instruments[0].reason is None
    assert report.unresolved_fx_currencies == ["EUR"]
    assert not report.freshness_ready


@pytest.mark.asyncio
async def test_both_fx_orientations_are_observed_including_absent_snapshot() -> None:
    """Retain both FX orientations even when a candidate lacks a snapshot.

    Given: A main instrument has direct and inverse FX metadata candidates.
    When: The snapshot read returns no evidence for the inverse candidate.
    Then: The expanded report retains that missing dependency and refuses readiness.
    """
    targets = [
        _target(quote="EUR"),
        _target("direct", base="EUR", asset_type="forex"),
        _target("inverse", base="USD", quote="EUR", asset_type="forex"),
    ]
    repository = _Repository(targets, [_snapshot(), _snapshot("direct", price=1.2)])

    report = await load_market_snapshot_freshness(repository, ["requested"], now=lambda: _NOW)

    assert report.unresolved_fx_currencies == []
    assert repository.snapshot_queries == [["direct", "inverse", "requested"]]
    assert [item.instrument_public_id for item in report.instruments] == [
        "direct",
        "inverse",
        "requested",
    ]
    assert report.instruments[1].reason == "missing_snapshot"
    assert not report.freshness_ready


@pytest.mark.asyncio
@pytest.mark.parametrize("inverse", [False, True])
async def test_fresh_fx_candidate_satisfies_dependency_resolution(inverse: bool) -> None:
    """Accept a fresh resolved dependency in either direct or inverse orientation.

    Given: A main instrument has one eligible metadata FX candidate.
    When: Fresh snapshots exist for both the main instrument and that candidate.
    Then: The complete scope is fresh without claiming numeric FX consistency.
    """
    fx = _target(
        "fx",
        base="USD" if inverse else "EUR",
        quote="EUR" if inverse else "USD",
        asset_type="forex",
    )
    repository = _Repository([_target(quote="EUR"), fx], [_snapshot(), _snapshot("fx")])

    report = await load_market_snapshot_freshness(repository, ["requested"], now=lambda: _NOW)

    assert report.freshness_ready
    assert report.unresolved_fx_currencies == []


@pytest.mark.asyncio
@pytest.mark.parametrize("exchange", ["paper", "unsupported_venue"])
async def test_unsupported_expanded_target_never_silently_disappears(exchange: str) -> None:
    """Expose unsupported expanded venues instead of silently dropping them.

    Given: A metadata FX candidate uses a venue unsupported by refresh collectors.
    When: The diagnostic report examines all resolved dependency rows.
    Then: The candidate remains visible with an unsupported-exchange refusal.
    """
    targets = [
        _target(quote="EUR"),
        _target("fx", exchange=exchange, base="EUR", asset_type="forex"),
    ]
    repository = _Repository(targets, [_snapshot(), _snapshot("fx")])

    report = await load_market_snapshot_freshness(repository, ["requested"], now=lambda: _NOW)

    assert not report.freshness_ready
    assert report.instruments[0].instrument_public_id == "fx"
    assert report.instruments[0].reason == "unsupported_exchange"


@pytest.mark.asyncio
async def test_non_forex_candidate_does_not_satisfy_currency_dependency() -> None:
    """Require forex metadata rather than inferring FX from currency spelling.

    Given: A candidate has matching currency names but a non-forex asset type.
    When: The policy checks whether the main quote has FX metadata evidence.
    Then: The quote remains unresolved and the scope cannot become ready.
    """
    targets = [_target(quote="EUR"), _target("not-fx", base="EUR", asset_type="crypto")]
    repository = _Repository(targets, [_snapshot(), _snapshot("not-fx")])

    report = await load_market_snapshot_freshness(repository, ["requested"], now=lambda: _NOW)

    assert report.unresolved_fx_currencies == ["EUR"]
    assert not report.freshness_ready


@pytest.mark.asyncio
async def test_final_clock_is_read_after_snapshot_await() -> None:
    """Read the observation clock only after the awaited snapshot operation.

    Given: A mark is 299 seconds old before a repository read advances time.
    When: The read finishes two seconds later and freshness is evaluated.
    Then: The 301-second-old mark is refused using the later clock.
    """
    clock = [_NOW]

    def advance_clock() -> None:
        """Move the synthetic clock during the repository await boundary."""
        clock[0] += timedelta(seconds=2)

    repository = _Repository(
        [_target()],
        [_snapshot(timestamp=_NOW - timedelta(seconds=299))],
        before_snapshots=advance_clock,
    )

    report = await load_market_snapshot_freshness(repository, ["requested"], now=lambda: clock[0])

    assert report.checked_at == _NOW + timedelta(seconds=2)
    assert report.instruments[0].age_seconds == 301.0
    assert report.instruments[0].reason == "stale_snapshot"
    assert not report.freshness_ready


@pytest.mark.asyncio
async def test_duplicate_requested_ids_are_deduplicated_without_broadening_scope() -> None:
    """Deduplicate requested identities while preserving an exact bounded scope.

    Given: The caller supplies the same explicit requested identity twice.
    When: The service observes its fresh row with the default UTC clock.
    Then: Each repository read sees one identity and the report is ready.
    """
    timestamp = datetime.now(UTC)
    repository = _Repository([_target()], [_snapshot(timestamp=timestamp)])

    report = await load_market_snapshot_freshness(repository, ["requested", "requested"])

    assert report.requested_instrument_public_ids == ["requested"]
    assert repository.target_queries == [["requested"]]
    assert repository.snapshot_queries == [["requested"]]
    assert report.checked_at >= timestamp
    assert report.freshness_ready
