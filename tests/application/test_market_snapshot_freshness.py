"""Freshness policy witnesses using bounded in-memory repository responses."""

from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Literal

import pytest

from snapper.application.market_snapshot_refresh.freshness import MAX_REQUESTED_INSTRUMENTS
from snapper.application.market_snapshot_refresh.freshness import load_market_snapshot_freshness
from snapper.application.market_snapshot_refresh.freshness import (
    market_snapshot_refresh_scope_digest,
)
from snapper.core.market_snapshot_freshness import MAX_REFRESH_TARGETS
from snapper.core.market_snapshot_freshness import FreshnessFailureReason
from snapper.data.repository_types import MarketSnapshotFreshnessRow
from snapper.data.repository_types import MarketSnapshotRefreshTargetRow

_NOW = datetime(2026, 11, 16, 15, 0, tzinfo=UTC)
_REQUESTED_ID = "feed0000-1234-4567-89ab-0123456789ab"
_UNKNOWN_ID = "feed0000-1234-4567-89ab-0123456789ac"


def _target(
    identity: str = _REQUESTED_ID,
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
    identity: str = _REQUESTED_ID, timestamp: datetime | None = _NOW, price: float | None = 100.0
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

    report = await load_market_snapshot_freshness(
        repository, [_REQUESTED_ID], now=lambda: _NOW, fx_exchanges=["kraken"]
    )

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

    report = await load_market_snapshot_freshness(
        repository, [_REQUESTED_ID], now=lambda: _NOW, fx_exchanges=["kraken"]
    )

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

    report = await load_market_snapshot_freshness(
        repository, [_REQUESTED_ID], now=lambda: _NOW, fx_exchanges=["kraken"]
    )

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

    report = await load_market_snapshot_freshness(
        repository, [_REQUESTED_ID], now=lambda: _NOW, fx_exchanges=["kraken"]
    )

    assert not report.freshness_ready
    assert report.instruments[0].reason == "invalid_timestamp"
    assert report.instruments[0].snapshot_timestamp == timestamp
    assert report.instruments[0].age_seconds is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "ids",
    [
        [],
        [""],
        [" "],
        [" requested"],
        ["requested "],
        [f" {_REQUESTED_ID}"],
        [f"{_REQUESTED_ID} "],
        ["not-uuid"],
        ["z" * 32],
        [_REQUESTED_ID[:-1]],
        [_REQUESTED_ID, "not-uuid"],
    ],
)
async def test_invalid_scope_never_reads_repository(ids: list[str]) -> None:
    """Reject invalid explicit scopes before any repository access.

    Given: An empty list, malformed UUID, blank or whitespace-padded identity.
    When: The caller requests a bounded freshness observation.
    Then: Input validation raises without either repository query running.
    """
    repository = _Repository([_target()], [_snapshot()])

    with pytest.raises(ValueError):
        await load_market_snapshot_freshness(repository, ids, fx_exchanges=["kraken"])

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
            repository, [_REQUESTED_ID] * (MAX_REQUESTED_INSTRUMENTS + 1), fx_exchanges=["kraken"]
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

    with pytest.raises(
        ValueError, match=f"Unknown or unsupported requested instruments: {_UNKNOWN_ID}"
    ):
        await load_market_snapshot_freshness(
            repository, [_REQUESTED_ID, _UNKNOWN_ID], fx_exchanges=["kraken"]
        )

    assert repository.target_queries == [[_REQUESTED_ID, _UNKNOWN_ID]]
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
        await load_market_snapshot_freshness(repository, [_REQUESTED_ID], fx_exchanges=["kraken"])

    assert repository.snapshot_queries == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("malformed_field", "invalid_value"),
    [
        ("instrument_public_id", ""),
        ("instrument_public_id", " requested"),
        ("instrument_public_id", "requested "),
        ("base", ""),
        ("base", " "),
        ("base", " BTC"),
        ("quote", ""),
        ("quote", " "),
        ("quote", "USD "),
        ("native_symbol", ""),
        ("native_symbol", " "),
        ("native_symbol", " BTC/USD "),
        ("native_symbol", "*"),
        ("native_symbol", "BTC/*"),
    ],
)
async def test_malformed_metadata_refuses_before_snapshot_read(
    malformed_field: Literal["instrument_public_id", "base", "quote", "native_symbol"],
    invalid_value: str,
) -> None:
    """Refuse metadata that cannot identify an explicit collector subscription.

    Given: A target has blank or padded identity metadata, or a native wildcard.
    When: Freshness service validates the metadata resolver output.
    Then: Validation refuses before reading snapshots and retains the original metadata.
    """
    target = _target()
    target[malformed_field] = invalid_value
    repository = _Repository([target], [_snapshot()])

    with pytest.raises(ValueError, match="incomplete instrument metadata"):
        await load_market_snapshot_freshness(repository, [_REQUESTED_ID], fx_exchanges=["kraken"])

    assert repository.snapshot_queries == []
    assert target[malformed_field] == invalid_value


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
        await load_market_snapshot_freshness(repository, [_REQUESTED_ID], fx_exchanges=["kraken"])

    assert repository.snapshot_queries == []


@pytest.mark.asyncio
async def test_missing_fx_metadata_cannot_report_fresh_main_as_ready() -> None:
    """Distinguish a fresh main mark from a complete conversion dependency scope.

    Given: A fresh EUR-quoted instrument has no metadata FX candidate.
    When: The service evaluates the requested instrument and resolved dependencies.
    Then: It reports EUR unresolved and freshness readiness remains false.
    """
    repository = _Repository([_target(quote="EUR")], [_snapshot()])

    report = await load_market_snapshot_freshness(
        repository, [_REQUESTED_ID], now=lambda: _NOW, fx_exchanges=["kraken"]
    )

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

    report = await load_market_snapshot_freshness(
        repository, [_REQUESTED_ID], now=lambda: _NOW, fx_exchanges=["kraken"]
    )

    assert report.unresolved_fx_currencies == []
    assert repository.snapshot_queries == [sorted(["direct", "inverse", _REQUESTED_ID])]
    assert [item.instrument_public_id for item in report.instruments] == sorted(
        ["direct", "inverse", _REQUESTED_ID]
    )
    assert (
        next(item for item in report.instruments if item.instrument_public_id == "inverse").reason
        == "missing_snapshot"
    )
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

    report = await load_market_snapshot_freshness(
        repository, [_REQUESTED_ID], now=lambda: _NOW, fx_exchanges=["kraken"]
    )

    assert report.freshness_ready
    assert report.unresolved_fx_currencies == []


@pytest.mark.asyncio
@pytest.mark.parametrize("exchange", ["paper", "unsupported_venue"])
async def test_unselected_expanded_target_never_silently_disappears(exchange: str) -> None:
    """Expose unselected expanded venues instead of silently dropping them.

    Given: A metadata FX candidate uses a venue outside explicit FX configuration.
    When: The diagnostic report examines all resolved dependency rows.
    Then: The candidate remains visible in excluded metadata without a price read.
    """
    targets = [
        _target(quote="EUR"),
        _target("fx", exchange=exchange, base="EUR", asset_type="forex"),
    ]
    repository = _Repository(targets, [_snapshot(), _snapshot("fx")])

    report = await load_market_snapshot_freshness(
        repository, [_REQUESTED_ID], now=lambda: _NOW, fx_exchanges=["kraken"]
    )

    assert not report.freshness_ready
    assert report.instruments[0].instrument_public_id == _REQUESTED_ID
    assert report.excluded_fx_instruments[0].instrument_public_id == "fx"
    assert report.excluded_fx_instruments[0].reason == "fx_venue_not_selected"
    assert report.unresolved_fx_currencies == ["EUR"]
    assert repository.snapshot_queries == [[_REQUESTED_ID]]


@pytest.mark.asyncio
async def test_non_forex_candidate_does_not_satisfy_currency_dependency() -> None:
    """Require forex metadata rather than inferring FX from currency spelling.

    Given: A candidate has matching currency names but a non-forex asset type.
    When: The policy checks whether the main quote has FX metadata evidence.
    Then: The quote remains unresolved and the scope cannot become ready.
    """
    targets = [_target(quote="EUR"), _target("not-fx", base="EUR", asset_type="crypto")]
    repository = _Repository(targets, [_snapshot(), _snapshot("not-fx")])

    report = await load_market_snapshot_freshness(
        repository, [_REQUESTED_ID], now=lambda: _NOW, fx_exchanges=["kraken"]
    )

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

    report = await load_market_snapshot_freshness(
        repository, [_REQUESTED_ID], now=lambda: clock[0], fx_exchanges=["kraken"]
    )

    assert report.checked_at == _NOW + timedelta(seconds=2)
    assert report.instruments[0].age_seconds == 301.0
    assert report.instruments[0].reason == "stale_snapshot"
    assert not report.freshness_ready


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "ids",
    [
        [_REQUESTED_ID, _REQUESTED_ID],
        [_REQUESTED_ID.upper(), f"{{{_REQUESTED_ID}}}", _REQUESTED_ID],
        [_REQUESTED_ID.replace("-", ""), f"urn:uuid:{_REQUESTED_ID}"],
    ],
)
async def test_requested_uuid_spellings_are_canonicalized_and_deduplicated(
    ids: list[str],
) -> None:
    """Canonicalize requested UUIDs before either bounded repository read.

    Given: The caller repeats one version-four UUID using equivalent accepted spellings.
    When: The service observes its fresh row with the default UTC clock.
    Then: Each repository read and the ready report contain only the canonical identity.
    """
    timestamp = datetime.now(UTC)
    repository = _Repository([_target()], [_snapshot(timestamp=timestamp)])

    report = await load_market_snapshot_freshness(repository, ids, fx_exchanges=["kraken"])

    assert report.requested_instrument_public_ids == [_REQUESTED_ID]
    assert repository.target_queries == [[_REQUESTED_ID]]
    assert repository.snapshot_queries == [[_REQUESTED_ID]]
    assert report.checked_at >= timestamp
    assert report.freshness_ready


@pytest.mark.asyncio
@pytest.mark.parametrize("fx_exchanges", [[], [""], [" "], ["kraken "], ["Kraken"], ["polygon"]])
async def test_invalid_fx_configuration_never_reads_repository(fx_exchanges: list[str]) -> None:
    """Require explicit supported FX refresh venues before any repository access.

    Given: The main scope is valid but FX refresh configuration is empty or invalid.
    When: Freshness service validates both explicit caller inputs.
    Then: It raises without metadata or snapshot queries and supplies no default.
    """
    repository = _Repository([_target()], [_snapshot()])

    with pytest.raises(ValueError, match="FX exchange"):
        await load_market_snapshot_freshness(repository, [_REQUESTED_ID], fx_exchanges=fx_exchanges)

    assert repository.target_queries == []
    assert repository.snapshot_queries == []


@pytest.mark.asyncio
async def test_oversized_fx_configuration_refuses_before_queries() -> None:
    """Bound repeated FX configuration values before deduplicating them.

    Given: An explicit FX parameter list exceeds the input ceiling.
    When: Freshness is requested for a valid main instrument.
    Then: The oversized input is rejected without either repository read.
    """
    repository = _Repository([_target()], [_snapshot()])

    with pytest.raises(ValueError, match="bounded nonempty FX exchange"):
        await load_market_snapshot_freshness(
            repository, [_REQUESTED_ID], fx_exchanges=["kraken"] * (MAX_REQUESTED_INSTRUMENTS + 1)
        )

    assert repository.target_queries == []
    assert repository.snapshot_queries == []


@pytest.mark.asyncio
async def test_polygon_is_explicitly_excluded_without_a_snapshot_read() -> None:
    """Observe configured Kraken FX while preserving excluded Polygon metadata.

    Given: Fresh main and Kraken FX rows plus a Polygon FX metadata candidate.
    When: Only Kraken is explicitly configured for FX refresh.
    Then: Readiness succeeds without querying Polygon and its exclusion is auditable.
    """
    targets = [
        _target(quote="EUR"),
        _target("fx", base="EUR", asset_type="forex"),
        _target("polygon-fx", exchange="polygon", base="EUR", asset_type="forex"),
    ]
    repository = _Repository(targets, [_snapshot(), _snapshot("fx")])

    report = await load_market_snapshot_freshness(
        repository, [_REQUESTED_ID], fx_exchanges=["kraken", "kraken"], now=lambda: _NOW
    )

    assert report.freshness_ready
    assert report.configured_fx_exchanges == ["kraken"]
    assert repository.snapshot_queries == [sorted(["fx", _REQUESTED_ID])]
    excluded = report.excluded_fx_instruments[0]
    assert excluded.instrument_public_id == "polygon-fx"
    assert excluded.native_symbol == "EUR/USD"
    assert excluded.exchange == "polygon"
    assert (excluded.base, excluded.quote, excluded.asset_type) == ("EUR", "USD", "forex")
    assert excluded.reason == "fx_venue_not_selected"


@pytest.mark.asyncio
async def test_excluded_only_fresh_fx_does_not_fulfil_configured_dependency() -> None:
    """Do not fall back to a fresh FX plane outside the configured refresh venues.

    Given: Only Polygon metadata can supply the requested EUR conversion.
    When: Kraken is the explicit FX refresh selection and both marks are fresh.
    Then: EUR remains unresolved and excluded FX evidence cannot produce readiness.
    """
    targets = [
        _target(quote="EUR"),
        _target("polygon-fx", exchange="polygon", base="EUR", asset_type="forex"),
    ]
    repository = _Repository(targets, [_snapshot(), _snapshot("polygon-fx")])

    report = await load_market_snapshot_freshness(
        repository, [_REQUESTED_ID], fx_exchanges=["kraken"], now=lambda: _NOW
    )

    assert not report.freshness_ready
    assert report.unresolved_fx_currencies == ["EUR"]
    assert repository.snapshot_queries == [[_REQUESTED_ID]]
    assert report.excluded_fx_instruments[0].instrument_public_id == "polygon-fx"


@pytest.mark.asyncio
async def test_inverse_orientation_on_unselected_venue_is_explicitly_excluded() -> None:
    """Apply configured venue selection independently to each FX orientation.

    Given: Kraken offers direct FX and Walutomat offers the inverse orientation.
    When: The caller explicitly chooses Kraken as its FX refresh venue.
    Then: Direct FX is required and inverse metadata is reported as excluded.
    """
    targets = [
        _target(quote="EUR"),
        _target("direct", base="EUR", asset_type="forex"),
        _target("inverse", exchange="walutomat", base="USD", quote="EUR", asset_type="forex"),
    ]
    repository = _Repository(targets, [_snapshot(), _snapshot("direct")])

    report = await load_market_snapshot_freshness(
        repository, [_REQUESTED_ID], fx_exchanges=["kraken"], now=lambda: _NOW
    )

    assert report.freshness_ready
    assert repository.snapshot_queries == [["direct", _REQUESTED_ID]]
    assert report.excluded_fx_instruments[0].instrument_public_id == "inverse"
    assert report.excluded_fx_instruments[0].native_symbol == "USD/EUR"


@pytest.mark.asyncio
async def test_requested_main_on_other_supported_venue_remains_required() -> None:
    """Do not use FX venue configuration to filter explicitly requested main instruments.

    Given: The requested main belongs to Walutomat while FX refresh selects Kraken.
    When: Its main snapshot is absent from the bounded evidence query.
    Then: The main remains required and missing evidence refuses readiness.
    """
    repository = _Repository([_target(exchange="walutomat", base="EUR", asset_type="forex")], [])

    report = await load_market_snapshot_freshness(
        repository, [_REQUESTED_ID], fx_exchanges=["kraken"], now=lambda: _NOW
    )

    assert not report.freshness_ready
    assert repository.snapshot_queries == [[_REQUESTED_ID]]
    assert report.instruments[0].reason == "missing_snapshot"
    assert report.excluded_fx_instruments == []


@pytest.mark.asyncio
async def test_requested_forex_main_cannot_bypass_configured_fx_venue() -> None:
    """Keep required main inclusion distinct from eligible FX dependency satisfaction.

    Given: A requested USD-EUR main is on Walutomat but only Kraken FX is configured.
    When: The main has fresh evidence and no configured FX candidate exists.
    Then: Its main status is fresh while EUR remains unresolved for readiness.
    """
    repository = _Repository(
        [_target(exchange="walutomat", base="USD", quote="EUR", asset_type="forex")], [_snapshot()]
    )

    report = await load_market_snapshot_freshness(
        repository, [_REQUESTED_ID], fx_exchanges=["kraken"], now=lambda: _NOW
    )

    assert report.instruments[0].reason is None
    assert report.unresolved_fx_currencies == ["EUR"]
    assert not report.freshness_ready


@pytest.mark.asyncio
async def test_unsupported_requested_main_refuses_before_snapshot_query() -> None:
    """Require main instruments themselves to be supported by refresh collectors.

    Given: Resolver output unexpectedly includes a requested main on Polygon.
    When: The service validates the caller's main identities.
    Then: It rejects that request before querying snapshot evidence.
    """
    repository = _Repository([_target(exchange="polygon")], [_snapshot()])

    with pytest.raises(ValueError, match="Unsupported requested instrument venues"):
        await load_market_snapshot_freshness(repository, [_REQUESTED_ID], fx_exchanges=["kraken"])

    assert repository.snapshot_queries == []


@pytest.mark.asyncio
async def test_expansion_ceiling_includes_excluded_fx_candidates() -> None:
    """Apply the metadata bound before filtering unselected FX venues.

    Given: The complete resolver output exceeds the ceiling because of Polygon rows.
    When: The caller selects only Kraken for FX refresh.
    Then: Exclusions cannot hide excessive expansion from the input bound.
    """
    targets = [_target()] + [
        _target(str(index), exchange="polygon", base="EUR", asset_type="forex")
        for index in range(MAX_REFRESH_TARGETS)
    ]
    repository = _Repository(targets, [])

    with pytest.raises(ValueError, match="Expanded refresh scope exceeds"):
        await load_market_snapshot_freshness(repository, [_REQUESTED_ID], fx_exchanges=["kraken"])

    assert repository.snapshot_queries == []


def test_scope_digest_normalizes_order_and_duplicate_configuration() -> None:
    """Hash effective metadata scope independently of ordering and duplicate input values.

    Given: Equivalent required/excluded metadata and reordered repeated configuration.
    When: Each version is encoded through the shared scope-digest helper.
    Then: The canonical SHA256 is identical and excludes incidental ordering.
    """
    required = {"first": _target("first"), "second": _target("second")}
    excluded = {"polygon": _target("polygon", exchange="polygon", base="EUR", asset_type="forex")}
    expected = market_snapshot_refresh_scope_digest(
        ["first", "second"], ["kraken", "walutomat"], required, excluded
    )

    actual = market_snapshot_refresh_scope_digest(
        ["second", "first", "second"],
        ["walutomat", "kraken", "kraken"],
        dict(reversed(list(required.items()))),
        excluded,
    )

    assert actual == expected
    assert len(actual) == 64


@pytest.mark.parametrize(
    "changed",
    [
        _target("different"),
        _target(exchange="walutomat"),
        _target(base="ETH"),
        _target(quote="EUR"),
        _target(asset_type="forex"),
    ],
)
def test_scope_digest_binds_resolved_identity_metadata(
    changed: MarketSnapshotRefreshTargetRow,
) -> None:
    """Bind metadata changes rather than hashing only the requested instrument list.

    Given: One resolved identity, venue, currency or classification changes.
    When: The scope-digest helper hashes the original and changed resolution.
    Then: The digests differ even without any snapshot price or time inputs.
    """
    original = market_snapshot_refresh_scope_digest(
        [_REQUESTED_ID], ["kraken"], {_REQUESTED_ID: _target()}, {}
    )

    changed_digest = market_snapshot_refresh_scope_digest(
        [changed["instrument_public_id"]],
        ["kraken"],
        {changed["instrument_public_id"]: changed},
        {},
    )

    assert changed_digest != original


def test_scope_digest_binds_configuration_and_exclusion_classification() -> None:
    """Bind configured FX venues and the required/excluded classification explicitly.

    Given: The same metadata rows can be classified differently or use another FX list.
    When: The helper hashes each exact configuration and classification.
    Then: Neither change can preserve the original frozen-scope digest.
    """
    main = _target()
    fx = _target("fx", base="EUR", asset_type="forex")
    original = market_snapshot_refresh_scope_digest(
        [_REQUESTED_ID], ["kraken"], {_REQUESTED_ID: main, "fx": fx}, {}
    )
    different_config = market_snapshot_refresh_scope_digest(
        [_REQUESTED_ID], ["kraken", "walutomat"], {_REQUESTED_ID: main, "fx": fx}, {}
    )
    different_classification = market_snapshot_refresh_scope_digest(
        [_REQUESTED_ID], ["kraken"], {_REQUESTED_ID: main}, {"fx": fx}
    )

    assert different_config != original
    assert different_classification != original


def test_scope_digest_binds_excluded_native_metadata() -> None:
    """Retain excluded native identity changes in the frozen metadata digest.

    Given: Required metadata is unchanged but an excluded native symbol changes.
    When: The shared helper hashes both complete classified universes.
    Then: The excluded candidate change produces a different scope digest.
    """
    required = {_REQUESTED_ID: _target()}
    excluded = _target("polygon", exchange="polygon", base="EUR", asset_type="forex")
    original = market_snapshot_refresh_scope_digest(
        [_REQUESTED_ID], ["kraken"], required, {"polygon": excluded}
    )
    changed = excluded.copy()
    changed["native_symbol"] = "C:EURUSD"

    revised = market_snapshot_refresh_scope_digest(
        [_REQUESTED_ID], ["kraken"], required, {"polygon": changed}
    )

    assert revised != original


@pytest.mark.asyncio
async def test_report_scope_digest_excludes_prices_and_observation_times() -> None:
    """Keep runtime price and clock changes outside the frozen metadata scope digest.

    Given: A resolved main instrument has a later snapshot with a different price.
    When: The same explicit scope is observed before and after that update.
    Then: The scope digest remains equal while observation timestamps differ.
    """
    repository = _Repository([_target()], [_snapshot()])
    first = await load_market_snapshot_freshness(
        repository, [_REQUESTED_ID], fx_exchanges=["kraken"], now=lambda: _NOW
    )
    later = _NOW + timedelta(seconds=1)
    repository.snapshots = [_snapshot(timestamp=later, price=200.0)]

    second = await load_market_snapshot_freshness(
        repository, [_REQUESTED_ID], fx_exchanges=["kraken"], now=lambda: later
    )

    assert first.scope_digest == second.scope_digest
    assert first.checked_at != second.checked_at
