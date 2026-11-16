"""Exercise bounded snapshot refresh identities and missing evidence on real SQLite."""

from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from uuid import NAMESPACE_URL
from uuid import uuid5

import pytest
from sqlalchemy.dialects.postgresql import dialect as postgresql_dialect

from snapper.data import repository as repository_module
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Instrument
from snapper.data.models import MarketSnapshot
from snapper.data.models import Symbol
from snapper.data.models import SymbolExchangeCapability
from snapper.data.repository import DatabaseRepository
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import MarketSnapshotFreshnessRow
from snapper.data.repository_types import MarketSnapshotRefreshTargetRow

_OBSERVED = datetime(2090, 1, 2, tzinfo=UTC)
_CLOSED = _OBSERVED + timedelta(hours=1)


@dataclass(frozen=True)
class _Target:
    """Typed metadata seed with independent temporal and capability controls."""

    name: str
    base: str = "EUR"
    quote: str | None = "USD"
    asset_type: str = "forex"
    exchange: str = "kraken"
    native_symbol: str = "EUR-USD"
    can_market_data: bool = True
    symbol_active: bool = True
    instrument_active: bool = True
    capability_active: bool = True
    include_capability: bool = True
    capability_exchange: str | None = None


@dataclass(frozen=True)
class _Repositories:
    """Async and synchronous readers of one fixture-owned SQLite database."""

    sync: DatabaseRepository
    asynchronous: SQLAlchemyRepository


@pytest.fixture
async def repositories(tmp_path: Path) -> AsyncIterator[_Repositories]:
    """Create only the four required tables in an isolated temporary database.

    Args:
        tmp_path: Test-owned directory, unrelated to any configured runtime database.

    Yields:
        Both repository implementations connected to the same isolated fixture.
    """
    database = tmp_path / "refresh.db"
    synchronous = DatabaseRepository(f"sqlite:///{database}")
    asynchronous = SQLAlchemyRepository(f"sqlite+aiosqlite:///{database}")
    for model in (Symbol, Instrument, SymbolExchangeCapability, MarketSnapshot):
        model.__table__.create(synchronous.engine)
    try:
        yield _Repositories(synchronous, asynchronous)
    finally:
        await asynchronous.engine.dispose()
        synchronous.engine.dispose()


@pytest.fixture(params=["sync", "async"])
def read_mode(request: pytest.FixtureRequest) -> str:
    """Select each concrete repository reader for the same behavioral witnesses.

    Args:
        request: Parameterized pytest request.

    Returns:
        The concrete reader name.
    """
    return str(request.param)


def _identity(name: str) -> str:
    """Build deterministic valid public identities for fixture-only objects.

    Args:
        name: Stable seed identity.

    Returns:
        A UUID string derived only from the synthetic seed name.
    """
    return str(uuid5(NAMESPACE_URL, f"snapshot-refresh-test:{name}"))


def _seed_target(repository: DatabaseRepository, target: _Target) -> str:
    """Persist one independent symbol, instrument and optional capability.

    Args:
        repository: Fixture-owned synchronous writer.
        target: Typed identity and temporal controls.

    Returns:
        The instrument identity inserted by this seed.
    """
    symbol_id = _identity(f"symbol-{target.name}")
    instrument_id = _identity(target.name)
    with repository.get_session() as session:
        session.add(
            Symbol(
                public_id=symbol_id,
                timestamp=_OBSERVED,
                known_to=KNOWN_TO_MAX if target.symbol_active else _CLOSED,
                session_id="test",
                sequence_id=1,
                native_symbol=target.native_symbol,
                base=target.base,
                quote=target.quote,
                asset_type=target.asset_type,
                created_at=_OBSERVED,
            )
        )
        session.add(
            Instrument(
                public_id=instrument_id,
                timestamp=_OBSERVED,
                known_to=KNOWN_TO_MAX if target.instrument_active else _CLOSED,
                session_id="test",
                sequence_id=1,
                symbol_public_id=symbol_id,
                exchange=target.exchange,
            )
        )
        if target.include_capability:
            session.add(
                SymbolExchangeCapability(
                    public_id=_identity(f"capability-{target.name}"),
                    timestamp=_OBSERVED,
                    known_to=KNOWN_TO_MAX if target.capability_active else _CLOSED,
                    session_id="test",
                    sequence_id=1,
                    symbol_public_id=symbol_id,
                    exchange=target.capability_exchange or target.exchange,
                    can_market_data=target.can_market_data,
                    can_trade=False,
                    created_at=_OBSERVED,
                )
            )
        session.commit()
    return instrument_id


def _seed_snapshot(
    repository: DatabaseRepository, instrument_id: str, last_price: float | None, active: bool
) -> None:
    """Persist active or closed snapshot evidence under the synthetic future clock.

    Args:
        repository: Fixture-owned synchronous writer.
        instrument_id: Exact logical instrument identity.
        last_price: Price evidence, including a valid missing-price state.
        active: Whether the row retains the current-version sentinel.
    """
    with repository.get_session() as session:
        session.add(
            MarketSnapshot(
                timestamp=_OBSERVED,
                known_to=KNOWN_TO_MAX if active else _CLOSED,
                session_id="test",
                sequence_id=1,
                instrument_public_id=instrument_id,
                last_price=last_price,
            )
        )
        session.commit()


async def _resolve(
    repositories: _Repositories, read_mode: str, instrument_ids: list[str]
) -> list[MarketSnapshotRefreshTargetRow]:
    """Invoke the selected concrete resolver without substituting the SQL layer.

    Args:
        repositories: Fixture-owned readers.
        read_mode: Concrete reader name.
        instrument_ids: Explicit requested identities.

    Returns:
        The resolver's actual dependency closure.
    """
    if read_mode == "async":
        return await repositories.asynchronous.resolve_market_snapshot_refresh_targets(
            instrument_ids
        )
    return repositories.sync.resolve_market_snapshot_refresh_targets(instrument_ids)


async def _freshness(
    repositories: _Repositories, read_mode: str, instrument_ids: list[str]
) -> list[MarketSnapshotFreshnessRow]:
    """Invoke the selected concrete evidence reader against the isolated database.

    Args:
        repositories: Fixture-owned readers.
        read_mode: Concrete reader name.
        instrument_ids: Explicit requested identities.

    Returns:
        Actual snapshot evidence including missing requested identities.
    """
    if read_mode == "async":
        return await repositories.asynchronous.get_active_market_snapshot_freshness(instrument_ids)
    return repositories.sync.get_active_market_snapshot_freshness(instrument_ids)


@pytest.mark.asyncio
async def test_refresh_resolves_both_fx_orientations_without_snapshots(
    repositories: _Repositories, read_mode: str
) -> None:
    """Given future active metadata without snapshots, when resolved, then both FX legs remain.

    Unsupported FX venues remain visible so callers can reject an unrefreshable closure.
    Duplicate and unresolved requested identities cannot broaden it to unrelated currencies.
    """
    main = _seed_target(
        repositories.sync,
        _Target("main", base="BTC", quote="EUR", asset_type="crypto", native_symbol="BTC-EUR"),
    )
    direct = _seed_target(repositories.sync, _Target("direct"))
    inverse = _seed_target(
        repositories.sync,
        _Target("inverse", base="USD", quote="EUR", exchange="walutomat", native_symbol="USD-EUR"),
    )
    other_venue = _seed_target(
        repositories.sync, _Target("other-venue", exchange="polygon", native_symbol="EURUSD")
    )
    _seed_target(repositories.sync, _Target("unrelated", base="JPY", native_symbol="JPY-USD"))
    rows = await _resolve(repositories, read_mode, [main, _identity("missing"), main])
    assert [row["instrument_public_id"] for row in rows] == sorted(
        [main, direct, inverse, other_venue]
    )
    assert next(row for row in rows if row["instrument_public_id"] == main) == {
        "instrument_public_id": main,
        "native_symbol": "BTC-EUR",
        "exchange": "kraken",
        "base": "BTC",
        "quote": "EUR",
        "asset_type": "crypto",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("fx_count", [2, 3, 8])
async def test_refresh_scope_limit_preserves_complete_boundary_or_refuses(
    repositories: _Repositories, read_mode: str, monkeypatch: pytest.MonkeyPatch, fx_count: int
) -> None:
    """Bound returned metadata without silently dropping FX candidates from any venue.

    Given: A small scope limit and complete metadata at or beyond that boundary.
    When: Either concrete repository resolves the requested main and its FX candidates.
    Then: The exact boundary remains complete and every overflowing scope raises.
    """
    monkeypatch.setattr(repository_module, "MAX_REFRESH_TARGETS", 3)
    main = _seed_target(
        repositories.sync,
        _Target("main", base="BTC", quote="EUR", asset_type="crypto", native_symbol="BTC-EUR"),
    )
    expected = [main]
    for index in range(fx_count):
        expected.append(
            _seed_target(
                repositories.sync,
                _Target(
                    f"fx-{index}",
                    native_symbol=f"EUR-USD-{index}",
                    exchange="polygon" if index % 2 else "kraken",
                ),
            )
        )
    if fx_count > 2:
        with pytest.raises(ValueError, match="Expanded refresh scope exceeds 3 instruments"):
            await _resolve(repositories, read_mode, [main])
    else:
        rows = await _resolve(repositories, read_mode, [main])
        assert [row["instrument_public_id"] for row in rows] == sorted(expected)


def test_refresh_query_limits_database_result_to_one_overflow_witness(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Enforce the result ceiling in PostgreSQL before buffered results reach Python.

    Given: A refresh target cap shared by SQL construction and row projection.
    When: The actual metadata query is compiled for the production database dialect.
    Then: SQL requests at most the cap plus one row needed to prove overflow.
    """
    monkeypatch.setattr(repository_module, "MAX_REFRESH_TARGETS", 3)
    query = repository_module._market_snapshot_refresh_target_query([_identity("main")])
    compiled = str(
        query.compile(dialect=postgresql_dialect(), compile_kwargs={"literal_binds": True})
    )
    assert compiled.rstrip().endswith("LIMIT 4")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "candidate",
    [
        _Target("candidate", asset_type="crypto"),
        _Target("candidate", exchange="paper"),
        _Target("candidate", can_market_data=False),
        _Target("candidate", symbol_active=False),
        _Target("candidate", instrument_active=False),
        _Target("candidate", capability_active=False),
        _Target("candidate", include_capability=False),
        _Target("candidate", capability_exchange="walutomat"),
        _Target("candidate", base=""),
        _Target("candidate", quote=""),
        _Target("candidate", native_symbol=""),
        _Target("candidate", native_symbol="   "),
    ],
)
async def test_refresh_excludes_ineligible_fx_candidates(
    repositories: _Repositories, read_mode: str, candidate: _Target
) -> None:
    """Given ineligible FX metadata, when the main target resolves, then no unsafe leg appears."""
    main = _seed_target(
        repositories.sync,
        _Target("main", base="BTC", quote="EUR", asset_type="crypto", native_symbol="BTC-EUR"),
    )
    _seed_target(repositories.sync, candidate)
    rows = await _resolve(repositories, read_mode, [main])
    assert [row["instrument_public_id"] for row in rows] == [main]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "requested",
    [
        _Target("requested", exchange="paper"),
        _Target("requested", exchange="polygon"),
        _Target("requested", can_market_data=False),
        _Target("requested", symbol_active=False),
        _Target("requested", instrument_active=False),
        _Target("requested", capability_active=False),
        _Target("requested", include_capability=False),
        _Target("requested", capability_exchange="walutomat"),
        _Target("requested", base=""),
        _Target("requested", base="   "),
        _Target("requested", quote=""),
        _Target("requested", quote="   "),
        _Target("requested", quote=None, asset_type="equity"),
        _Target("requested", native_symbol=""),
    ],
)
async def test_refresh_omits_invalid_requested_identities(
    repositories: _Repositories, read_mode: str, requested: _Target
) -> None:
    """Given an unusable requested identity, when resolved, then it cannot acquire FX dependencies."""
    requested_id = _seed_target(repositories.sync, requested)
    assert await _resolve(repositories, read_mode, [requested_id, _identity("missing")]) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("exchange", ["kraken", "kraken_futures", "kraken_equities", "walutomat"])
async def test_refresh_supports_exact_main_venues_without_usd_self_leg(
    repositories: _Repositories, read_mode: str, exchange: str
) -> None:
    """Given a supported USD target, when resolved, then it has no synthetic USD self-conversion."""
    main = _seed_target(
        repositories.sync,
        _Target(
            "main", base="BTC", asset_type="crypto", exchange=exchange, native_symbol="BTC-USD"
        ),
    )
    _seed_target(repositories.sync, _Target("self", base="USD", native_symbol="USD-USD"))
    assert [
        row["instrument_public_id"] for row in await _resolve(repositories, read_mode, [main])
    ] == [main]


@pytest.mark.asyncio
async def test_refresh_empty_never_selects_all_rows(
    repositories: _Repositories, read_mode: str
) -> None:
    """Given available metadata and evidence, when identities are empty, then both readers return nothing."""
    identity = _seed_target(repositories.sync, _Target("available"))
    _seed_snapshot(repositories.sync, identity, 1.25, active=True)
    assert await _resolve(repositories, read_mode, []) == []
    assert await _freshness(repositories, read_mode, []) == []


@pytest.mark.asyncio
async def test_freshness_preserves_missing_and_null_evidence(
    repositories: _Repositories, read_mode: str
) -> None:
    """Given active, closed and missing snapshots, when read, then missing evidence never disappears."""
    present = _seed_target(repositories.sync, _Target("present"))
    null_price = _seed_target(
        repositories.sync, _Target("null", native_symbol="USD-EUR", base="USD", quote="EUR")
    )
    closed = _seed_target(repositories.sync, _Target("closed", native_symbol="GBP-USD", base="GBP"))
    missing = _identity("missing")
    _seed_snapshot(repositories.sync, present, 9.0, active=False)
    _seed_snapshot(repositories.sync, present, 1.25, active=True)
    _seed_snapshot(repositories.sync, null_price, None, active=True)
    _seed_snapshot(repositories.sync, closed, 2.0, active=False)
    rows = await _freshness(
        repositories, read_mode, [present, missing, closed, null_price, present]
    )
    expected: list[MarketSnapshotFreshnessRow] = [
        {"instrument_public_id": present, "timestamp": _OBSERVED, "last_price": 1.25},
        {"instrument_public_id": null_price, "timestamp": _OBSERVED, "last_price": None},
        {"instrument_public_id": closed, "timestamp": None, "last_price": None},
        {"instrument_public_id": missing, "timestamp": None, "last_price": None},
    ]
    assert rows == sorted(expected, key=lambda row: row["instrument_public_id"])
