"""Repository side of the guarded candle void (catalog item C5) and the fenced candle restore."""

from collections.abc import AsyncIterator
from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import PropertyMock
from unittest.mock import patch
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session as SyncSession

from snapper.application.portfolio.pnl_timeline_service import _PlaneCoverage
from snapper.application.portfolio.pnl_timeline_service import _preferred_fx_plane
from snapper.data.candle_void_types import CandleVersionKey
from snapper.data.candle_void_types import CandleVersionReading
from snapper.data.candle_void_types import CandleVoidRefusedError
from snapper.data.candle_void_types import CandleVoidRequest
from snapper.data.candle_void_types import CatalogExpectation
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Base
from snapper.data.models import Candle
from snapper.data.models import FxConversionElection
from snapper.data.models import FxConversionProof
from snapper.data.models import Instrument
from snapper.data.models import ShadowCandle
from snapper.data.models import Symbol
from snapper.data.models import SymbolAlias
from snapper.data.models import SymbolExchangeCapability
from snapper.data.repository import DatabaseRepository
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import _candle_version_key_match

_SEED_AT = datetime(2026, 10, 11, 0, 0, tzinfo=UTC)
_FIRST_MINUTE = datetime(2026, 11, 7, 19, 30, tzinfo=UTC)
_WRITTEN_AT = datetime(2026, 11, 11, 11, 18, tzinfo=UTC)
_CLOSED_AT = datetime(2026, 11, 9, 0, 0, tzinfo=UTC)
_KRAKEN_SYMBOL = "sym-chf-usd"
_KRAKEN = "inst-kraken-chf-usd"
_POLYGON = "inst-polygon-chf-usd"
_OTHER = "inst-kraken-eth-usd"
_CAPABILITY = "cap-kraken-chf-usd"


@pytest.fixture
async def repo() -> AsyncIterator[SQLAlchemyRepository]:
    """Provide a fresh in-memory repository with the full schema.

    Yields:
        The repository.
    """
    repository = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    await repository.create_all()
    yield repository
    await repository.engine.dispose()


async def _add(repo: SQLAlchemyRepository, *rows: Base) -> None:
    """Insert ORM rows in one transaction.

    Args:
        repo: Repository.
        *rows: Rows to insert.
    """
    async with repo.session() as session:
        session.add_all(list(rows))
        await session.commit()


def _symbol(public_id: str, native: str, base: str, quote: str) -> Symbol:
    """Build a forex symbol version created before every candle.

    Args:
        public_id: Symbol public id.
        native: Native symbol.
        base: Base currency.
        quote: Quote currency.

    Returns:
        The symbol row.
    """
    return Symbol(
        public_id=public_id,
        native_symbol=native,
        base=base,
        quote=quote,
        asset_type="forex",
        created_at=_SEED_AT,
        session_id="seed",
        sequence_id=1,
        timestamp=_SEED_AT,
    )


def _instrument(public_id: str, symbol_public_id: str, exchange: str) -> Instrument:
    """Build an instrument version created before every candle.

    Args:
        public_id: Instrument public id.
        symbol_public_id: Owning symbol.
        exchange: Exchange.

    Returns:
        The instrument row.
    """
    return Instrument(
        public_id=public_id,
        symbol_public_id=symbol_public_id,
        exchange=exchange,
        session_id="seed",
        sequence_id=1,
        timestamp=_SEED_AT,
    )


def _capability(*, market: bool = False, trade: bool = False) -> SymbolExchangeCapability:
    """Build the Kraken capability of the franc symbol.

    Args:
        market: Market data flag.
        trade: Trading flag.

    Returns:
        The capability row.
    """
    return SymbolExchangeCapability(
        public_id=_CAPABILITY,
        symbol_public_id=_KRAKEN_SYMBOL,
        exchange="kraken",
        can_market_data=market,
        can_trade=trade,
        created_at=_SEED_AT,
        session_id="seed",
        sequence_id=1,
        timestamp=_SEED_AT,
    )


def _alias(known_to: datetime = KNOWN_TO_MAX) -> SymbolAlias:
    """Build a Kraken REST alias of the franc symbol.

    Args:
        known_to: End of the alias version.

    Returns:
        The alias row.
    """
    return SymbolAlias(
        symbol_public_id=_KRAKEN_SYMBOL,
        exchange="kraken",
        channel="rest",
        exchange_symbol="ZCHFUSD",
        created_at=_SEED_AT,
        session_id="seed",
        sequence_id=1,
        timestamp=_SEED_AT,
        known_to=known_to,
    )


def _candle(
    instrument: str,
    open_at: datetime,
    *,
    timeframe: str = "1m",
    timestamp: datetime = _WRITTEN_AT,
    known_to: datetime = KNOWN_TO_MAX,
) -> Candle:
    """Build one complete candle version.

    Args:
        instrument: Instrument public id.
        open_at: Open minute.
        timeframe: Timeframe label; 1m bars are native, others synthesized.
        timestamp: Bus time it was written at.
        known_to: End of the version.

    Returns:
        The candle row.
    """
    close = 1.1
    source = "native" if timeframe == "1m" else "synthesized"
    return Candle(
        public_id=str(uuid4()),
        instrument_public_id=instrument,
        open_at=open_at,
        timeframe=timeframe,
        open=close,
        high=close,
        low=close,
        close=close,
        volume=10.0,
        vwap=close,
        trades=3,
        source=source,
        complete=True,
        session_id=str(uuid4()),
        sequence_id=1,
        timestamp=timestamp,
        known_to=known_to,
    )


async def _seed_catalog(repo: SQLAlchemyRepository) -> None:
    """Seed the franc symbol on Kraken and polygon, and an unrelated Kraken instrument.

    Args:
        repo: Repository.
    """
    await _add(
        repo,
        _symbol(_KRAKEN_SYMBOL, "CHF-USD", "CHF", "USD"),
        _symbol("sym-eth-usd", "ETH-USD", "ETH", "USD"),
        _instrument(_KRAKEN, _KRAKEN_SYMBOL, "kraken"),
        _instrument(_POLYGON, _KRAKEN_SYMBOL, "polygon"),
        _instrument(_OTHER, "sym-eth-usd", "kraken"),
        _capability(),
        _alias(known_to=_CLOSED_AT),
    )


async def _seed_cohort(repo: SQLAlchemyRepository) -> None:
    """Seed the Kraken franc cohort: 1m and 5m bars, one closed version, and controls.

    Args:
        repo: Repository.
    """
    await _seed_catalog(repo)
    await _add(
        repo,
        _candle(_KRAKEN, _FIRST_MINUTE),
        _candle(_KRAKEN, _FIRST_MINUTE + timedelta(minutes=1)),
        _candle(_KRAKEN, _FIRST_MINUTE, timeframe="5m"),
        _candle(_KRAKEN, _FIRST_MINUTE + timedelta(minutes=2), known_to=_CLOSED_AT),
        _candle(_POLYGON, _FIRST_MINUTE),
        _candle(_OTHER, _FIRST_MINUTE),
    )


async def _keys(repo: SQLAlchemyRepository) -> tuple[CandleVersionKey, ...]:
    """Read the active cohort as version keys.

    Args:
        repo: Repository.

    Returns:
        The keys of every active Kraken franc candle.
    """
    rows = await repo.get_active_candle_versions([_KRAKEN])
    return tuple(
        CandleVersionKey(
            instrument_public_id=row["instrument_public_id"],
            timeframe=row["timeframe"],
            open_at=row["open_at"],
            id=row["id"],
            public_id=row["public_id"],
            session_id=row["session_id"],
            sequence_id=row["sequence_id"],
            timestamp=row["timestamp"],
        )
        for row in rows
    )


async def _catalog(repo: SQLAlchemyRepository) -> tuple[CatalogExpectation, ...]:
    """Read the approved catalog expectation of the Kraken franc instrument.

    Args:
        repo: Repository.

    Returns:
        Its expectation.
    """
    targets = await repo.get_candle_void_targets([_KRAKEN])
    return tuple(target.catalog for target in targets)


def _request(
    keys: tuple[CandleVersionKey, ...],
    catalog: tuple[CatalogExpectation, ...],
    prepared: list[datetime],
) -> CandleVoidRequest:
    """Build a void request whose hook records the prepared instant.

    Args:
        keys: Expected versions.
        catalog: Approved catalog state.
        prepared: List that receives the prepared instant.

    Returns:
        The request.
    """
    return CandleVoidRequest(
        instrument_public_ids=(_KRAKEN,),
        expected=keys,
        catalog=catalog,
        on_prepared=prepared.append,
    )


async def _known_to(repo: SQLAlchemyRepository, instrument: str) -> list[datetime]:
    """Read every version's ``known_to`` of one instrument, ordered by id.

    Args:
        repo: Repository.
        instrument: Instrument public id.

    Returns:
        The ``known_to`` values.
    """
    async with repo.session() as session:
        result = await session.execute(
            select(Candle.known_to)
            .where(Candle.instrument_public_id == instrument)
            .order_by(Candle.id)
        )
        return list(result.scalars())


async def test_targets_resolve_the_catalog_state_of_each_instrument(
    repo: SQLAlchemyRepository,
) -> None:
    """Targets carry exchange, symbol, capability flags and the active alias count.

    Given: The franc symbol with a delisted Kraken capability and only a closed alias,
        and an unrelated Kraken instrument without a capability.
    When: Targets are resolved for both and for an unknown id.
    Then: Each known instrument resolves with its catalog state, in request order;
        the unknown id is left out.
    """
    await _seed_catalog(repo)

    targets = await repo.get_candle_void_targets([_KRAKEN, "inst-unknown", _OTHER, _KRAKEN])

    assert [target.instrument_public_id for target in targets] == [_KRAKEN, _OTHER]
    franc, ether = targets
    assert (franc.exchange, franc.native_symbol) == ("kraken", "CHF-USD")
    assert franc.catalog == CatalogExpectation(
        instrument_public_id=_KRAKEN,
        capability_public_id=_CAPABILITY,
        capability_timestamp=_SEED_AT,
        can_market_data=False,
        can_trade=False,
        active_alias_count=0,
    )
    assert ether.catalog.capability_public_id is None
    assert ether.catalog.can_market_data is None


async def test_active_candle_versions_cover_every_timeframe_of_the_cohort_only(
    repo: SQLAlchemyRepository,
) -> None:
    """The cohort read returns every active candle of the instruments, sorted.

    Given: Active 1m and 5m bars, a closed 1m version, and other instruments' bars.
    When: The active versions of the Kraken franc instrument are read.
    Then: Exactly its three active bars come back, ordered by timeframe and open.
    """
    await _seed_cohort(repo)

    rows = await repo.get_active_candle_versions([_KRAKEN])

    assert [(row["timeframe"], row["open_at"]) for row in rows] == [
        ("1m", _FIRST_MINUTE),
        ("1m", _FIRST_MINUTE + timedelta(minutes=1)),
        ("5m", _FIRST_MINUTE),
    ]
    assert {row["known_to"] for row in rows} == {KNOWN_TO_MAX}


async def test_references_count_fx_artifacts_and_shadow_candles(
    repo: SQLAlchemyRepository,
) -> None:
    """References split FX artifacts into active and historical, and count shadow bars.

    Given: An active and a closed election selecting the Kraken plane, two proofs
        from it, one closed, an election on another instrument, and shadow bars.
    When: The references of the Kraken franc instrument are counted.
    Then: The counts reflect only that instrument's artifacts and active shadows.
    """
    await _seed_catalog(repo)
    elections = [_election(_KRAKEN), _election(_KRAKEN, known_to=_CLOSED_AT), _election(_POLYGON)]
    proofs = [_proof(_KRAKEN), _proof(_KRAKEN, known_to=_CLOSED_AT), _proof(_POLYGON)]
    shadows = [_shadow(_KRAKEN, "calculated"), _shadow(_KRAKEN, "calculated", minutes=1)]
    shadows.append(_shadow(_KRAKEN, "calculated", minutes=2, known_to=_CLOSED_AT))
    await _add(repo, *elections, *proofs, *shadows)

    references = await repo.get_candle_void_references([_KRAKEN])

    assert (references.elections_active, references.elections_historical) == (1, 1)
    assert (references.proofs_active, references.proofs_historical) == (1, 1)
    assert references.shadow_candles == {f"{_KRAKEN}|1m|calculated": 2}


def _election(instrument: str, *, known_to: datetime = KNOWN_TO_MAX) -> FxConversionElection:
    """Build a complete shared-pair election selecting one instrument.

    Args:
        instrument: Selected source instrument.
        known_to: End of the version.

    Returns:
        The election row.
    """
    return FxConversionElection(
        scope_kind="shared_pair",
        source_currency="CHF",
        target_currency="USD",
        unordered_pair="CHF-USD",
        requirement_manifest_digest=uuid4().hex * 2,
        requested_knowledge_at=_WRITTEN_AT,
        resolved_knowledge_at=_WRITTEN_AT,
        election_policy_version="v1",
        calculation_version="v1",
        selected_source_exchange="kraken",
        selected_source_instrument_public_id=instrument,
        selected_native_symbol="CHF-USD",
        selected_base="CHF",
        selected_quote="USD",
        selected_orientation="direct",
        decision_inputs_digest=uuid4().hex * 2,
        completeness_state="complete",
        session_id="seed",
        sequence_id=1,
        timestamp=_WRITTEN_AT,
        known_to=known_to,
    )


def _proof(instrument: str, *, known_to: datetime = KNOWN_TO_MAX) -> FxConversionProof:
    """Build an inverse exact-minute proof sourced from one instrument.

    Args:
        instrument: Source instrument.
        known_to: End of the version.

    Returns:
        The proof row.
    """
    return FxConversionProof(
        election_public_id=str(uuid4()),
        conversion_minute=_FIRST_MINUTE + timedelta(minutes=1),
        candle_open_minute=_FIRST_MINUTE,
        carried_minutes=0,
        candle_id=1,
        candle_public_id=str(uuid4()),
        candle_session_id=str(uuid4()),
        candle_sequence_id=1,
        candle_timestamp=_WRITTEN_AT,
        candle_known_to=KNOWN_TO_MAX,
        raw_close_decimal="1.1",
        operation="inverse",
        source_instrument_public_id=instrument,
        proof_digest=uuid4().hex * 2,
        session_id="seed",
        sequence_id=1,
        timestamp=_WRITTEN_AT,
        known_to=known_to,
    )


def _shadow(
    instrument: str, source: str, *, minutes: int = 0, known_to: datetime = KNOWN_TO_MAX
) -> ShadowCandle:
    """Build one shadow candle version.

    Args:
        instrument: Instrument public id.
        source: Shadow source label.
        minutes: Minutes after the first minute.
        known_to: End of the version.

    Returns:
        The shadow candle row.
    """
    return ShadowCandle(
        instrument_public_id=instrument,
        open_at=_FIRST_MINUTE + timedelta(minutes=minutes),
        timeframe="1m",
        open=1.1,
        high=1.1,
        low=1.1,
        close=1.1,
        volume=1.0,
        source=source,
        session_id="seed",
        sequence_id=1,
        timestamp=_WRITTEN_AT,
        known_to=known_to,
    )


async def test_void_closes_exactly_the_voted_set_at_one_instant(
    repo: SQLAlchemyRepository,
) -> None:
    """V1 closes every expected version at one database instant and nothing else.

    Given: The cohort, an earlier closed version and other instruments' bars.
    When: The exact cohort is voided.
    Then: The three versions end at the prepared instant, the closed version keeps
        its own end, the controls stay active, and the result lists the keys sorted.
    """
    await _seed_cohort(repo)
    keys = await _keys(repo)
    prepared: list[datetime] = []

    result = await repo.void_candle_versions(_request(keys, await _catalog(repo), prepared))

    assert prepared == [result.void_at]
    assert result.closed == tuple(sorted(keys))
    assert result.void_at > _WRITTEN_AT
    assert await _known_to(repo, _KRAKEN) == [
        result.void_at,
        result.void_at,
        result.void_at,
        _CLOSED_AT,
    ]
    assert await _known_to(repo, _POLYGON) == [KNOWN_TO_MAX]
    assert await _known_to(repo, _OTHER) == [KNOWN_TO_MAX]
    assert await repo.get_active_candle_versions([_KRAKEN]) == []


@pytest.mark.parametrize(
    "change",
    ["extra", "new_timeframe", "missing", "timestamp", "public_id"],
)
async def test_void_refuses_a_changed_cohort_and_closes_nothing(
    repo: SQLAlchemyRepository, change: str
) -> None:
    """Any difference between the fenced cohort and the vote refuses.

    Given: The cohort and its keys, then one kind of change.
    When: The void runs with the original expectation, or a mutated one.
    Then: It raises ``cohort_changed`` and every version is still active.
    """
    await _seed_cohort(repo)
    keys = await _keys(repo)
    if change == "extra":
        await _add(repo, _candle(_KRAKEN, _FIRST_MINUTE + timedelta(minutes=5)))
    elif change == "new_timeframe":
        await _add(repo, _candle(_KRAKEN, _FIRST_MINUTE, timeframe="1h"))
    elif change == "missing":
        keys = keys[1:]
    elif change == "timestamp":
        keys = (replace(keys[0], timestamp=keys[0].timestamp + timedelta(seconds=1)), *keys[1:])
    else:
        keys = (replace(keys[0], public_id=str(uuid4())), *keys[1:])

    with pytest.raises(CandleVoidRefusedError) as refusal:
        await repo.void_candle_versions(_request(keys, await _catalog(repo), []))

    assert refusal.value.code == "cohort_changed"
    assert KNOWN_TO_MAX in await _known_to(repo, _KRAKEN)
    assert len(await repo.get_active_candle_versions([_KRAKEN])) >= 2


@pytest.mark.parametrize(
    ("keys_kind", "code"), [("empty", "empty_cohort"), ("duplicate", "duplicate_keys")]
)
async def test_void_refuses_a_malformed_request(
    repo: SQLAlchemyRepository, keys_kind: str, code: str
) -> None:
    """An empty or duplicated expectation is refused before the fence.

    Given: The cohort.
    When: The void runs with no keys, or with a key given twice.
    Then: It raises the matching code.
    """
    await _seed_cohort(repo)
    keys = await _keys(repo)
    expected = () if keys_kind == "empty" else (keys[0], keys[0])

    with pytest.raises(CandleVoidRefusedError) as refusal:
        await repo.void_candle_versions(_request(expected, await _catalog(repo), []))

    assert refusal.value.code == code


async def test_void_refuses_a_changed_catalog(repo: SQLAlchemyRepository) -> None:
    """A catalog state different from the approved one refuses under the fence.

    Given: The cohort, its keys and an approved expectation, then a new active alias.
    When: The void runs with the old expectation.
    Then: It raises ``catalog`` and closes nothing.
    """
    await _seed_cohort(repo)
    keys = await _keys(repo)
    catalog = await _catalog(repo)
    await _add(repo, _alias())

    with pytest.raises(CandleVoidRefusedError) as refusal:
        await repo.void_candle_versions(_request(keys, catalog, []))

    assert refusal.value.code == "catalog"
    assert len(await repo.get_active_candle_versions([_KRAKEN])) == 3


async def test_void_refuses_a_candle_written_at_or_after_the_clock(
    repo: SQLAlchemyRepository,
) -> None:
    """A version whose bus time is not before the database clock refuses.

    Given: A cohort candle written in the future.
    When: The void runs.
    Then: It raises ``clock`` and closes nothing.
    """
    await _seed_catalog(repo)
    await _add(
        repo, _candle(_KRAKEN, _FIRST_MINUTE, timestamp=datetime.now(UTC) + timedelta(hours=1))
    )
    keys = await _keys(repo)

    with pytest.raises(CandleVoidRefusedError) as refusal:
        await repo.void_candle_versions(_request(keys, await _catalog(repo), []))

    assert refusal.value.code == "clock"
    assert await _known_to(repo, _KRAKEN) == [KNOWN_TO_MAX]


async def test_void_rolls_back_when_the_preparation_hook_raises(
    repo: SQLAlchemyRepository,
) -> None:
    """A hook failure aborts before any row is closed.

    Given: The cohort.
    When: The void runs with a hook that raises.
    Then: The hook's error propagates and every version is still active.
    """
    await _seed_cohort(repo)
    keys = await _keys(repo)

    def refuse_preparation(void_at: datetime) -> None:
        raise CandleVoidRefusedError("reconciled")

    request = replace(_request(keys, await _catalog(repo), []), on_prepared=refuse_preparation)
    with pytest.raises(CandleVoidRefusedError):
        await repo.void_candle_versions(request)

    assert len(await repo.get_active_candle_versions([_KRAKEN])) == 3


async def test_void_refuses_when_the_closure_returns_another_set(
    repo: SQLAlchemyRepository,
) -> None:
    """A closure that does not return exactly the expected keys rolls back.

    Given: The cohort and a closure that returns no keys.
    When: The void runs.
    Then: It raises ``closed_set`` and every version is still active.
    """
    await _seed_cohort(repo)
    keys = await _keys(repo)

    with (
        patch.object(
            SQLAlchemyRepository, "_close_candle_versions", AsyncMock(return_value=frozenset())
        ),
        pytest.raises(CandleVoidRefusedError) as refusal,
    ):
        await repo.void_candle_versions(_request(keys, await _catalog(repo), []))

    assert refusal.value.code == "closed_set"
    assert len(await repo.get_active_candle_versions([_KRAKEN])) == 3


async def test_void_closes_more_than_one_statement_batch(repo: SQLAlchemyRepository) -> None:
    """A cohort larger than one key batch is closed completely.

    Given: 1,001 active 1m bars of the Kraken franc instrument.
    When: The exact cohort is voided.
    Then: All 1,001 versions are closed.
    """
    await _seed_catalog(repo)
    await _add(repo, *(_candle(_KRAKEN, _FIRST_MINUTE + timedelta(minutes=i)) for i in range(1001)))
    keys = await _keys(repo)

    result = await repo.void_candle_versions(_request(keys, await _catalog(repo), []))

    assert len(result.closed) == 1001
    assert await repo.get_active_candle_versions([_KRAKEN]) == []


def test_key_match_binds_every_key_column() -> None:
    """The closure's predicate compares every key column with the bound keys.

    Given: The key CTE of one version.
    When: A closure statement using the key match is compiled for PostgreSQL.
    Then: Its WHERE clause names every key column and the active sentinel.
    """
    repository = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    key = CandleVersionKey(_KRAKEN, "1m", _FIRST_MINUTE, 7, "pub", "sess", 1, _WRITTEN_AT)
    with patch.object(
        SQLAlchemyRepository, "dialect_name", new_callable=PropertyMock, return_value="postgresql"
    ):
        keys = repository._candle_key_cte([key])
    statement = update(Candle).where(
        *_candle_version_key_match(keys), Candle.known_to == KNOWN_TO_MAX
    )

    compiled = str(statement.compile(dialect=postgresql.dialect()))

    for column in (
        "instrument_public_id",
        "timeframe",
        "open_at",
        "id",
        "public_id",
        "session_id",
        "sequence_id",
        "timestamp",
    ):
        assert f"candles.{column} = candle_void_keys.{column}" in compiled
    assert "unnest" in compiled
    assert "candles.known_to =" in compiled


async def test_postgresql_void_clock_reads_clock_timestamp() -> None:
    """On PostgreSQL the void instant is the database's ``clock_timestamp()``.

    Given: A PostgreSQL-flavoured repository and a session returning an instant.
    When: The void clock is read.
    Then: The session's value is returned and the query names clock_timestamp.
    """
    repository = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    instant = datetime(2026, 11, 12, 1, 2, 3, 456789, tzinfo=UTC)
    session = AsyncMock(spec=AsyncSession)
    session.execute.return_value = MagicMock(scalar_one=MagicMock(return_value=instant))
    with patch.object(
        SQLAlchemyRepository, "dialect_name", new_callable=PropertyMock, return_value="postgresql"
    ):
        value = await repository._candle_void_clock(cast(AsyncSession, session))

    assert value == instant
    assert "clock_timestamp" in str(session.execute.await_args.args[0])
    await repository.engine.dispose()


async def test_observe_reads_each_key_under_the_fence_and_writes_nothing(
    repo: SQLAlchemyRepository,
) -> None:
    """The fenced read reports active, closed and absent versions to the decider.

    Given: The cohort, one version closed earlier, and a key that does not exist.
    When: The versions are observed.
    Then: The decider sees active, closed and absent readings, and nothing changes.
    """
    await _seed_cohort(repo)
    keys = await _keys(repo)
    async with repo.session() as session:
        closed_row = (
            await session.execute(
                select(Candle).where(
                    Candle.instrument_public_id == _KRAKEN, Candle.known_to == _CLOSED_AT
                )
            )
        ).scalar_one()
    closed = CandleVersionKey(
        _KRAKEN,
        "1m",
        closed_row.open_at,
        closed_row.id,
        closed_row.public_id,
        closed_row.session_id,
        closed_row.sequence_id,
        closed_row.timestamp,
    )
    absent = replace(keys[0], public_id=str(uuid4()))
    seen: list[dict[CandleVersionKey, CandleVersionReading]] = []

    await repo.observe_candle_void([keys[0], closed, absent], seen.append)

    assert seen == [
        {
            keys[0]: CandleVersionReading(known_to=KNOWN_TO_MAX, active=True),
            closed: CandleVersionReading(known_to=_CLOSED_AT, active=False),
            absent: CandleVersionReading(known_to=None, active=False),
        }
    ]
    assert len(await repo.get_active_candle_versions([_KRAKEN])) == 3


async def test_fx_discovery_and_election_drop_the_kraken_plane_from_void_at(
    repo: SQLAlchemyRepository,
) -> None:
    """After the void, discovery at or after ``void_at`` names no Kraken franc plane.

    Given: Kraken and polygon CHF-USD planes with equal complete 1m coverage and
        metadata older than every candle.
    When: Discovery and election run before the void, and at horizons before, at
        and after ``void_at``.
    Then: Before the void the Kraken plane is discovered and elected on the tie; at
        and after ``void_at`` only polygon is discovered and elected; a horizon
        before ``void_at`` still discovers Kraken.
    """
    await _seed_cohort(repo)
    window = (_FIRST_MINUTE, _FIRST_MINUTE + timedelta(minutes=1))
    pair = ("CHF", "USD")
    later = datetime.now(UTC)

    before = set(await repo.get_pnl_fx_rate_exchanges([pair], *window, later))
    keys = await _keys(repo)
    result = await repo.void_candle_versions(_request(keys, await _catalog(repo), []))
    earlier = result.void_at - timedelta(microseconds=1)
    at_void = set(await repo.get_pnl_fx_rate_exchanges([pair], *window, result.void_at))
    after = set(
        await repo.get_pnl_fx_rate_exchanges([pair], *window, result.void_at + timedelta(seconds=1))
    )
    history = set(await repo.get_pnl_fx_rate_exchanges([pair], *window, earlier))

    assert before == {("CHF", "USD", "kraken"), ("CHF", "USD", "polygon")}
    assert at_void == after == {("CHF", "USD", "polygon")}
    assert history == before
    minutes = {_FIRST_MINUTE}
    coverage = {
        plane: _PlaneCoverage(exact=set(minutes), carried=set(), carry_cost=0) for plane in before
    }
    assert _preferred_fx_plane(pair, minutes, before, coverage, "USD") == ("CHF", "USD", "kraken")
    assert _preferred_fx_plane(pair, minutes, after, coverage, "USD") == ("CHF", "USD", "polygon")


@pytest.fixture
def sync_repo(tmp_path: Path) -> Iterator[DatabaseRepository]:
    """Provide a synchronous repository on a fresh SQLite file.

    Args:
        tmp_path: Test directory.

    Yields:
        The repository.
    """
    repository = DatabaseRepository(f"sqlite:///{tmp_path / 'restore.db'}")
    Base.metadata.create_all(repository.engine)
    yield repository
    repository.dispose()


def _archive_row(candle: Candle) -> dict[str, object]:
    """Copy a candle's non-id columns as an archive restore row.

    Args:
        candle: Candle version.

    Returns:
        Column values keyed by name.
    """
    return {
        column.name: getattr(candle, column.name)
        for column in Candle.__table__.columns
        if column.name != "id"
    }


def test_restore_batch_classifies_by_version_identity(sync_repo: DatabaseRepository) -> None:
    """The fenced restore inserts absent versions and never re-activates a closed one.

    Given: A stored closed version, a stored active version, and an absent one.
    When: A batch holds an active copy of the closed version, an exact copy of the
        active one, a closed copy of the active one, and the absent version twice.
    Then: Only the absent version is inserted, once; the others are counted as
        exact or known_to-differs skips, and the closed version stays closed.
    """
    voided = _candle(_KRAKEN, _FIRST_MINUTE, known_to=_CLOSED_AT)
    active = _candle(_KRAKEN, _FIRST_MINUTE + timedelta(minutes=1))
    absent = _candle(_KRAKEN, _FIRST_MINUTE + timedelta(minutes=2))
    with sync_repo.get_session() as session:
        session.add_all([voided, active])
        session.commit()
        stored = [_archive_row(voided), _archive_row(active)]
    rows = [
        {**stored[0], "known_to": KNOWN_TO_MAX},
        stored[1],
        {**stored[1], "known_to": _CLOSED_AT},
        _archive_row(absent),
        _archive_row(absent),
    ]

    counts = sync_repo.restore_candle_archive_batch(rows)

    assert (counts.inserted, counts.skipped_exact, counts.skipped_known_to_differs) == (1, 2, 2)
    with sync_repo.get_session() as session:
        found = session.execute(select(Candle.open_at, Candle.known_to).order_by(Candle.id)).all()
    assert [row[1] for row in found] == [_CLOSED_AT, KNOWN_TO_MAX, KNOWN_TO_MAX]


def test_restore_batch_of_nothing_writes_nothing(sync_repo: DatabaseRepository) -> None:
    """An empty batch returns zero counts without opening a transaction.

    Given: An empty database.
    When: An empty batch is restored.
    Then: Every count is zero.
    """
    counts = sync_repo.restore_candle_archive_batch([])

    assert (counts.inserted, counts.skipped_exact, counts.skipped_known_to_differs) == (0, 0, 0)


def _fenced_repo(dialect: str) -> DatabaseRepository:
    """Build a repository double whose engine reports one dialect.

    Args:
        dialect: Dialect name.

    Returns:
        The repository, without a real engine.
    """
    repository = DatabaseRepository.__new__(DatabaseRepository)
    repository.engine = MagicMock()
    repository.engine.dialect.name = dialect
    return repository


def test_postgresql_restore_fence_bounds_the_wait_and_locks_candles() -> None:
    """On PostgreSQL the restore fence checks READ COMMITTED and takes SHARE ROW EXCLUSIVE.

    Given: A PostgreSQL-flavoured repository and a READ COMMITTED session.
    When: The fence is taken.
    Then: It reads the isolation, bounds lock_timeout and locks public.candles.
    """
    session = MagicMock(spec=SyncSession)
    session.execute.return_value = MagicMock(scalar_one=MagicMock(return_value="read committed"))

    _fenced_repo("postgresql")._begin_sync_candle_fence(session)

    statements = [str(call.args[0]) for call in session.execute.call_args_list]
    assert statements[0] == "SHOW transaction_isolation"
    assert "lock_timeout" in statements[1]
    assert statements[2] == "LOCK TABLE public.candles IN SHARE ROW EXCLUSIVE MODE"


def test_postgresql_restore_fence_refuses_another_isolation() -> None:
    """The restore fence refuses a PostgreSQL session that is not READ COMMITTED.

    Given: A PostgreSQL-flavoured repository and a REPEATABLE READ session.
    When: The fence is taken.
    Then: It raises ValueError before locking.
    """
    session = MagicMock(spec=SyncSession)
    session.execute.return_value = MagicMock(scalar_one=MagicMock(return_value="repeatable read"))

    with pytest.raises(ValueError, match="READ COMMITTED"):
        _fenced_repo("postgresql")._begin_sync_candle_fence(session)

    assert session.execute.call_count == 1


def test_restore_fence_refuses_an_unknown_dialect() -> None:
    """The restore fence supports only SQLite and PostgreSQL.

    Given: A repository whose engine reports MySQL.
    When: The fence is taken.
    Then: It raises NotImplementedError without executing anything.
    """
    session = MagicMock(spec=SyncSession)

    with pytest.raises(NotImplementedError, match="SQLite and PostgreSQL"):
        _fenced_repo("mysql")._begin_sync_candle_fence(session)

    session.execute.assert_not_called()
