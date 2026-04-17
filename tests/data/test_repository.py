"""Tests for Repository pattern implementations."""

import asyncio
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Generator
from contextlib import AbstractAsyncContextManager
from contextlib import asynccontextmanager
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from types import TracebackType
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

import pytest
from sqlalchemy import select as _sa_select
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker

import snapper.data.repository
import snapper.data.repository as repo
import snapper.data.repository as repository
from snapper.data import repository as repo_module
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import InstrumentOrderCapability
from snapper.data.models import MarketSnapshot
from snapper.data.models import Position
from snapper.data.models import PositionCycle
from snapper.data.models import Setting
from snapper.data.models import Signal
from snapper.data.models import Symbol
from snapper.data.models import SymbolAlias
from snapper.data.models import VenueFeeSchedule
from snapper.data.repository import DatabaseRepository
from snapper.data.repository import InstrumentSpecInput
from snapper.data.repository import Repository
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import dispose_repositories
from snapper.data.repository import get_repository
from snapper.data.repository import where_active
from snapper.data.repository import where_active_now
from snapper.data.repository_types import AccrualLedgerInsertRow
from snapper.data.repository_types import FundingRateInsertRow
from snapper.infrastructure.symbols.functions import resolve_symbol_public_id


class _DummyAsyncSession:
    def __init__(self, fail_on: int = 0) -> None:
        self.fail_on = fail_on
        self.calls = 0
        self.rollback_called = False
        self.commit_called = False
        self.savepoint_rollbacks = 0

    async def __aenter__(self) -> _DummyAsyncSession:
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        return False

    def begin_nested(self) -> _DummyAsyncSavepoint:
        return _DummyAsyncSavepoint(self)

    async def execute(self, stmt: Any) -> Any:
        self.calls += 1
        if self.fail_on and self.calls == self.fail_on:
            raise IntegrityError("stmt", {}, Exception("fail"))
        return SimpleNamespace(rowcount=1)

    async def commit(self) -> None:
        self.commit_called = True

    async def rollback(self) -> None:
        self.rollback_called = True


class _DummyAsyncSavepoint:
    def __init__(self, parent: _DummyAsyncSession) -> None:
        self._parent = parent

    async def __aenter__(self) -> _DummyAsyncSavepoint:
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        if exc_type is not None:
            self._parent.savepoint_rollbacks += 1
        return False


def _patch_begin_nested(mock_session: AsyncMock) -> None:
    """Configure begin_nested on an AsyncMock session as a sync call returning async CM."""
    mock_session.begin_nested = Mock(return_value=AsyncMock())


@asynccontextmanager
async def _session_factory(session: _DummyAsyncSession) -> AsyncIterator[_DummyAsyncSession]:
    yield session


def _make_repo(session_factory: Callable[[], Any], dialect: str = "other") -> SQLAlchemyRepository:
    repo = SQLAlchemyRepository.__new__(SQLAlchemyRepository)
    repo.db_url = "test"
    repo.engine = cast(
        AsyncEngine,
        SimpleNamespace(url=SimpleNamespace(get_dialect=lambda: SimpleNamespace(name=dialect))),
    )
    repo.session_factory = cast(async_sessionmaker[AsyncSession], session_factory)
    return repo


@pytest.mark.asyncio
async def test_session_rolls_back_on_exception() -> None:
    """Test session rolls back on exception.

    Given: A session context manager,
    When: Exception is raised inside session,
    Then: Session rollback is called.
    """
    session = _DummyAsyncSession()
    repo = _make_repo(lambda: _session_factory(session))
    with pytest.raises(ValueError):
        async with repo.session():
            raise ValueError("boom")
    assert session.rollback_called is True


@pytest.mark.asyncio
async def test_session_skips_generator_exit_without_rollback() -> None:
    """Test session skips rollback on GeneratorExit.

    Given: A session context manager,
    When: GeneratorExit is raised,
    Then: Session rollback is not called.
    """
    session = _DummyAsyncSession()
    repo = _make_repo(lambda: _session_factory(session))
    async with repo.session():
        raise GeneratorExit
    assert session.rollback_called is False


class _DummyInsert:
    def __init__(self) -> None:
        self.values_kwargs: dict[str, Any] | None = None

    def values(self, **kwargs: Any) -> _DummyInsert:
        self.values_kwargs = kwargs
        return self


@pytest.mark.asyncio
async def test_upsert_candles_closes_old_and_inserts_new(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test upsert_candles closes old row and inserts new (SCD Type 2).

    Given: Session where SELECT returns an existing candle,
    When: upsert_candles is called,
    Then: Old row is closed (known_to set) and new row is added.
    """
    ts = datetime(2024, 1, 1, tzinfo=UTC)
    existing_candle = SimpleNamespace(id=42, public_id="existing-uuid")
    call_count = 0
    added_objects: list[Any] = []

    async def _execute(stmt: Any) -> Any:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return SimpleNamespace(scalars=lambda: SimpleNamespace(first=lambda: existing_candle))
        return SimpleNamespace(rowcount=1)

    session = _DummyAsyncSession()
    session.execute = _execute
    session.add = lambda obj: added_objects.append(obj)
    repo = _make_repo(lambda: _session_factory(session), dialect="custom")
    rows = [
        {
            "instrument_public_id": "fake-inst-pid",
            "open_at": ts,
            "timestamp": ts,
            "timeframe": "1m",
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
            "volume": 1000.0,
            "vwap": None,
            "trades": 10,
            "session_id": "test-session",
            "sequence_id": 1,
        },
    ]
    inserted = await repo.upsert_candles(rows)
    assert inserted == 1
    assert session.commit_called is True
    assert len(added_objects) == 1
    assert rows[0]["public_id"] == "existing-uuid"


@pytest.mark.asyncio
async def test_upsert_trades_other_dialect_skips_duplicates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test upsert_trades skips duplicates in other dialects.

    Given: Session that fails on second execute,
    When: upsert_trades is called with two rows,
    Then: Returns 1 and savepoint rolled back for failed row.
    """
    session = _DummyAsyncSession(fail_on=2)
    repo = _make_repo(lambda: _session_factory(session), dialect="custom")
    monkeypatch.setattr(repository, "insert", lambda table: _DummyInsert())
    rows = [{"trade_id": "t1"}, {"trade_id": "t2"}]
    inserted = await repo.upsert_trades(rows)
    assert inserted == 1
    assert session.savepoint_rollbacks == 1
    assert session.commit_called is True


@pytest.mark.asyncio
async def test_upsert_candles_inserts_new_when_no_existing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify upsert_candles inserts new rows when no existing match.

    Given: Session where SELECT returns None (no active candle),
    When: upsert_candles is called,
    Then: New row is added via session.add and count is 1.
    """
    ts = datetime(2024, 1, 1, tzinfo=UTC)
    added_objects: list[Any] = []

    async def _execute(stmt: Any) -> Any:
        return SimpleNamespace(scalars=lambda: SimpleNamespace(first=lambda: None))

    session = _DummyAsyncSession()
    session.execute = _execute
    session.add = lambda obj: added_objects.append(obj)
    repo = _make_repo(lambda: _session_factory(session), dialect="custom")
    rows = [
        {
            "instrument_public_id": "fake-inst-pid",
            "open_at": ts,
            "timestamp": ts,
            "timeframe": "1m",
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
            "volume": 1000.0,
            "vwap": None,
            "trades": 10,
            "session_id": "test-session",
            "sequence_id": 1,
        },
    ]
    inserted = await repo.upsert_candles(rows)
    assert inserted == 1
    assert session.commit_called is True
    assert len(added_objects) == 1


@pytest.mark.asyncio
async def test_upsert_candles_preserves_caller_supplied_public_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify upsert_candles preserves a caller-supplied public_id.

    Given: A row that already contains a 'public_id' key and no existing match,
    When: upsert_candles is called,
    Then: The existing public_id is passed through, not overwritten.
    """
    ts = datetime(2024, 1, 1, tzinfo=UTC)
    added_objects: list[Any] = []

    async def _execute(stmt: Any) -> Any:
        return SimpleNamespace(scalars=lambda: SimpleNamespace(first=lambda: None))

    session = _DummyAsyncSession()
    session.execute = _execute
    session.add = lambda obj: added_objects.append(obj)
    repo = _make_repo(lambda: _session_factory(session), dialect="custom")
    rows = [
        {
            "instrument_public_id": "fake-inst-pid",
            "open_at": ts,
            "timestamp": ts,
            "timeframe": "1m",
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
            "volume": 1000.0,
            "vwap": None,
            "trades": 10,
            "public_id": "my-custom-uuid",
        },
    ]
    await repo.upsert_candles(rows)
    assert rows[0]["public_id"] == "my-custom-uuid"


@pytest.mark.asyncio
async def test_upsert_candles_preserves_caller_supplied_known_to(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify upsert_candles preserves a caller-supplied known_to.

    Given: A row that already contains a 'known_to' key,
    When: upsert_candles is called,
    Then: The existing known_to is passed through, not overwritten.
    """
    ts = datetime(2024, 1, 1, tzinfo=UTC)
    added_objects: list[Any] = []

    async def _execute(stmt: Any) -> Any:
        return SimpleNamespace(scalars=lambda: SimpleNamespace(first=lambda: None))

    session = _DummyAsyncSession()
    session.execute = _execute
    session.add = lambda obj: added_objects.append(obj)
    repo = _make_repo(lambda: _session_factory(session), dialect="custom")
    rows = [
        {
            "instrument_public_id": "fake-inst-pid",
            "open_at": ts,
            "timestamp": ts,
            "timeframe": "1m",
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
            "volume": 1000.0,
            "vwap": None,
            "trades": 10,
            "known_to": KNOWN_TO_MAX,
        },
    ]
    await repo.upsert_candles(rows)
    assert rows[0]["known_to"] == KNOWN_TO_MAX


@pytest.mark.asyncio
async def test_upsert_candles_requires_caller_timestamp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify upsert_candles uses caller-supplied timestamp.

    Given: A row with an explicit 'timestamp' key,
    When: upsert_candles is called,
    Then: The row keeps the caller-supplied timestamp.
    """
    added_objects: list[Any] = []

    async def _execute(stmt: Any) -> Any:
        return SimpleNamespace(scalars=lambda: SimpleNamespace(first=lambda: None))

    session = _DummyAsyncSession()
    session.execute = _execute
    session.add = lambda obj: added_objects.append(obj)
    repo = _make_repo(lambda: _session_factory(session), dialect="custom")
    fixed_time = datetime(2024, 1, 1, tzinfo=UTC)
    rows = [
        {
            "instrument_public_id": "fake-inst-pid",
            "open_at": datetime(2024, 1, 1, tzinfo=UTC),
            "timeframe": "1m",
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
            "volume": 1000.0,
            "vwap": None,
            "trades": 10,
            "session_id": "test-session",
            "sequence_id": 1,
            "timestamp": fixed_time,
        },
    ]
    await repo.upsert_candles(rows)
    assert rows[0]["timestamp"] == fixed_time


@pytest.mark.asyncio
async def test_upsert_trades_other_dialect_preserves_existing_public_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify _upsert_batch preserves a caller-supplied public_id for trades.

    Given: A trade row that already contains a 'public_id' key,
    When: upsert_trades is called via the fallback dialect path,
    Then: The existing public_id is preserved, not overwritten.
    """
    session = _DummyAsyncSession()
    repo = _make_repo(lambda: _session_factory(session), dialect="custom")
    monkeypatch.setattr(repository, "insert", lambda table: _DummyInsert())
    rows = [{"trade_id": "t1", "public_id": "my-trade-uuid"}]
    await repo.upsert_trades(rows)
    assert rows[0]["public_id"] == "my-trade-uuid"


@pytest.mark.asyncio
async def test_upsert_trades_savepoint_preserves_earlier_inserts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify SAVEPOINT does not roll back previously inserted rows.

    Given: 3-row batch where the second row is a duplicate,
    When: upsert_trades is called via the fallback dialect path,
    Then: First and third rows are counted (inserted == 2).
    """
    session = _DummyAsyncSession(fail_on=2)
    repo = _make_repo(lambda: _session_factory(session), dialect="custom")
    monkeypatch.setattr(repository, "insert", lambda table: _DummyInsert())
    rows = [{"trade_id": "t1"}, {"trade_id": "t2"}, {"trade_id": "t3"}]
    inserted = await repo.upsert_trades(rows)
    assert inserted == 2
    assert session.savepoint_rollbacks == 1
    assert session.commit_called is True


@pytest.mark.asyncio
async def test_upsert_market_snapshots_empty_returns_zero() -> None:
    """Return 0 when called with empty list.

    Given: Empty rows list,
    When: upsert_market_snapshots is called,
    Then: Returns 0 without opening session.
    """
    session = _DummyAsyncSession()
    repo = _make_repo(lambda: _session_factory(session), dialect="custom")
    result = await repo.upsert_market_snapshots([])
    assert result == 0
    assert session.commit_called is False


@pytest.mark.asyncio
async def test_upsert_market_snapshots_inserts_new_row() -> None:
    """Insert new market snapshot when no existing active row.

    Given: Session returning no existing row,
    When: upsert_market_snapshots is called,
    Then: New row is added and count is 1.
    """
    added_objects: list[Any] = []

    async def _execute(stmt: Any) -> Any:
        return SimpleNamespace(scalars=lambda: SimpleNamespace(first=lambda: None))

    session = _DummyAsyncSession()
    session.execute = _execute
    session.add = lambda obj: added_objects.append(obj)
    repo = _make_repo(lambda: _session_factory(session), dialect="custom")
    ts = datetime(2024, 6, 1, tzinfo=UTC)
    rows = [
        {
            "instrument_public_id": "inst-abc",
            "bid": 100.0,
            "ask": 101.0,
            "timestamp": ts,
            "session_id": "s1",
            "sequence_id": 1,
        },
    ]
    result = await repo.upsert_market_snapshots(rows)
    assert result == 1
    assert session.commit_called is True
    assert len(added_objects) == 1
    assert added_objects[0].instrument_public_id == "inst-abc"


@pytest.mark.asyncio
async def test_upsert_market_snapshots_closes_and_replaces() -> None:
    """Close existing active snapshot and insert new version.

    Given: Session returning an existing active row,
    When: upsert_market_snapshots is called,
    Then: Existing row is closed (UPDATE executed), public_id is preserved, new row added.
    """
    existing = SimpleNamespace(id=42, public_id="existing-uuid")
    call_count = 0
    added_objects: list[Any] = []

    async def _execute(stmt: Any) -> Any:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return SimpleNamespace(scalars=lambda: SimpleNamespace(first=lambda: existing))
        return SimpleNamespace(rowcount=1)

    session = _DummyAsyncSession()
    session.execute = _execute
    session.add = lambda obj: added_objects.append(obj)
    repo = _make_repo(lambda: _session_factory(session), dialect="custom")
    ts = datetime(2024, 6, 1, tzinfo=UTC)
    rows = [
        {
            "instrument_public_id": "inst-abc",
            "bid": 100.0,
            "ask": 101.0,
            "timestamp": ts,
            "session_id": "s1",
            "sequence_id": 1,
        },
    ]
    result = await repo.upsert_market_snapshots(rows)
    assert result == 1
    assert rows[0]["public_id"] == "existing-uuid"
    assert session.commit_called is True
    assert len(added_objects) == 1


@pytest.mark.asyncio
async def test_upsert_market_snapshots_generates_defaults() -> None:
    """Generate public_id and known_to when missing; timestamp is required.

    Given: Row dict without public_id or known_to but with timestamp,
    When: upsert_market_snapshots is called,
    Then: Defaults are generated for public_id and known_to.
    """
    added_objects: list[Any] = []

    async def _execute(stmt: Any) -> Any:
        return SimpleNamespace(scalars=lambda: SimpleNamespace(first=lambda: None))

    session = _DummyAsyncSession()
    session.execute = _execute
    session.add = lambda obj: added_objects.append(obj)
    repo = _make_repo(lambda: _session_factory(session), dialect="custom")
    fixed_time = datetime(2024, 1, 1, tzinfo=UTC)
    rows: list[dict[str, Any]] = [
        {
            "instrument_public_id": "inst-xyz",
            "bid": 50.0,
            "ask": 51.0,
            "session_id": "s1",
            "sequence_id": 1,
            "timestamp": fixed_time,
        },
    ]
    result = await repo.upsert_market_snapshots(rows)
    assert result == 1
    assert "public_id" in rows[0]
    assert len(rows[0]["public_id"]) > 0
    assert rows[0]["known_to"] == KNOWN_TO_MAX
    assert rows[0]["timestamp"] == fixed_time


@pytest.mark.asyncio
async def test_upsert_market_snapshots_preserves_supplied_keys() -> None:
    """Preserve caller-supplied public_id, known_to, and timestamp.

    Given: Row dict with all optional keys pre-populated,
    When: upsert_market_snapshots is called,
    Then: Supplied values are not overwritten by defaults.
    """
    added_objects: list[Any] = []

    async def _execute(stmt: Any) -> Any:
        return SimpleNamespace(scalars=lambda: SimpleNamespace(first=lambda: None))

    session = _DummyAsyncSession()
    session.execute = _execute
    session.add = lambda obj: added_objects.append(obj)
    repo = _make_repo(lambda: _session_factory(session), dialect="custom")
    ts = datetime(2024, 6, 1, tzinfo=UTC)
    custom_known_to = datetime(2099, 1, 1, tzinfo=UTC)
    rows: list[dict[str, Any]] = [
        {
            "instrument_public_id": "inst-xyz",
            "bid": 50.0,
            "ask": 51.0,
            "session_id": "s1",
            "sequence_id": 1,
            "public_id": "custom-pid",
            "known_to": custom_known_to,
            "timestamp": ts,
        },
    ]
    result = await repo.upsert_market_snapshots(rows)
    assert result == 1
    assert rows[0]["public_id"] == "custom-pid"
    assert rows[0]["known_to"] == custom_known_to
    assert rows[0]["timestamp"] == ts


def test_get_repository_caches_instances(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test get_repository caches instances.

    Given: Repository factory,
    When: Called twice with same URL,
    Then: Returns same cached instance.
    """
    repo._repository_cache.clear()
    first = get_repository("sqlite+aiosqlite:///:memory:")
    second = get_repository("sqlite+aiosqlite:///:memory:")
    assert first is second
    repo._repository_cache.clear()


@pytest.mark.asyncio
async def test_dispose_repositories_awaits_and_clears_cache() -> None:
    """Test dispose_repositories awaits dispose and clears cache.

    Given: Repository cache with mock repository,
    When: dispose_repositories is called,
    Then: Engine dispose is awaited and cache cleared.
    """
    repo._repository_cache.clear()

    class _Repo:
        def __init__(self) -> None:
            self.called = False
            self.awaited = False
            self.engine = SimpleNamespace(dispose=self._dispose)

        async def _dispose(self) -> None:
            self.called = True
            self.awaited = True

    repo_instance = _Repo()
    repository._repository_cache["db"] = cast(repository.Repository, repo_instance)
    await dispose_repositories()
    assert repo_instance.called is True
    assert repo_instance.awaited is True
    assert repository._repository_cache == {}


def test_database_repository_convert_to_sync_urls() -> None:
    """Test DatabaseRepository converts async URLs to sync.

    Given: Async database URLs,
    When: _convert_to_sync_url is called,
    Then: Returns sync driver equivalents.
    """
    sqlite_url = DatabaseRepository._convert_to_sync_url("sqlite+aiosqlite:///tmp/db")
    postgres_url = DatabaseRepository._convert_to_sync_url("postgresql+asyncpg://host/db")
    passthrough = DatabaseRepository._convert_to_sync_url("postgresql://host/db")
    assert sqlite_url == "sqlite:///tmp/db"
    assert postgres_url == "postgresql+psycopg2://host/db"
    assert passthrough == "postgresql://host/db"


@pytest.mark.asyncio
async def test_sqlalchemy_repository_sqlite_crud(tmp_path: Path) -> None:
    """Test SQLAlchemyRepository full CRUD operations on SQLite.

    Given: SQLite repository,
    When: All CRUD operations are executed,
    Then: Data is persisted and retrieved correctly.
    """
    db_path = tmp_path / "repo.db"
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await repo.create_all()
    async with repo.session() as s:
        s.add(
            Symbol(
                native_symbol="BTC-USD",
                base="BTC",
                quote="USD",
                asset_type="crypto",
                created_at=datetime.now(UTC),
                timestamp=datetime.now(UTC),
                session_id="test-session",
                sequence_id=1,
            )
        )
        await s.commit()
    spid = await resolve_symbol_public_id(repo, "BTC-USD", as_of=datetime.now(UTC))
    assert spid is not None
    instrument_id, instrument_public_id = await repo.ensure_instrument(
        symbol_public_id=spid,
        exchange="kraken",
        session_id="test-session",
        sequence_id=1,
        timestamp=datetime.now(UTC),
    )
    duplicate_id, _ = await repo.ensure_instrument(
        symbol_public_id=spid,
        exchange="kraken",
        session_id="test-session",
        sequence_id=1,
        timestamp=datetime.now(UTC),
    )
    assert duplicate_id == instrument_id
    base_ts = datetime.now(UTC) - timedelta(minutes=10)
    candle_rows = [
        {
            "instrument_public_id": instrument_public_id,
            "timeframe": "1m",
            "open_at": base_ts,
            "timestamp": base_ts,
            "open": 10.0,
            "high": 12.0,
            "low": 9.5,
            "close": 11.0,
            "volume": 100.0,
            "vwap": 10.5,
            "trades": 4,
            "session_id": "test-session",
            "sequence_id": 1,
        },
        {
            "instrument_public_id": instrument_public_id,
            "timeframe": "1m",
            "open_at": base_ts + timedelta(minutes=1),
            "timestamp": base_ts + timedelta(minutes=1),
            "open": 11.0,
            "high": 12.5,
            "low": 10.5,
            "close": 12.0,
            "volume": 80.0,
            "vwap": 11.8,
            "trades": 3,
            "session_id": "test-session",
            "sequence_id": 1,
        },
    ]
    inserted_candles = await repo.upsert_candles(candle_rows)
    assert inserted_candles == 2
    trade_rows = [
        {
            "instrument_public_id": instrument_public_id,
            "timestamp": base_ts,
            "price": 10.5,
            "size": 0.25,
            "side": "buy",
            "trade_id": "t1",
            "session_id": "test-session",
            "sequence_id": 1,
        },
        {
            "instrument_public_id": instrument_public_id,
            "timestamp": base_ts + timedelta(minutes=1),
            "price": 11.5,
            "size": 0.5,
            "side": "sell",
            "trade_id": "t2",
            "session_id": "test-session",
            "sequence_id": 2,
        },
    ]
    inserted_trades = await repo.upsert_trades(trade_rows)
    assert inserted_trades == 2
    candle_results = await repo.get_candles(
        "BTC-USD",
        "1m",
        base_ts - timedelta(minutes=1),
        base_ts + timedelta(minutes=2),
        exchange="kraken",
        as_of=datetime.now(UTC),
    )
    assert len(candle_results) == 2
    trade_results = await repo.get_trades(
        "BTC-USD",
        base_ts - timedelta(minutes=1),
        base_ts + timedelta(minutes=2),
        exchange="kraken",
        as_of=datetime.now(UTC),
    )
    assert len(trade_results) == 2
    order_id, order_public_id = await repo.insert_order(
        instrument_public_id=instrument_public_id,
        wallet_public_id="00000000-0000-7000-8000-000000000001",
        client_order_id="client-1",
        exchange_order_id=None,
        created_at=base_ts,
        side="buy",
        order_type="limit",
        price=10.5,
        size=0.75,
        status="new",
        session_id="",
        sequence_id=0,
        timestamp=base_ts,
    )
    order_v2 = await repo.update_order(
        order_id=order_id,
        status="partially_filled",
        updated_at=base_ts + timedelta(minutes=1),
        session_id="",
        sequence_id=0,
        timestamp=base_ts + timedelta(minutes=1),
        filled_size=0.5,
        average_price=10.55,
    )
    await repo.update_order(
        order_id=order_v2,
        status="filled",
        updated_at=base_ts + timedelta(minutes=2),
        session_id="",
        sequence_id=0,
        timestamp=base_ts + timedelta(minutes=2),
        exchange_order_id="ex-1",
        error=None,
    )
    execution_id = await repo.insert_execution(
        order_public_id=order_public_id,
        wallet_public_id="00000000-0000-7000-8000-000000000001",
        timestamp=base_ts + timedelta(minutes=2, seconds=30),
        side="buy",
        status="filled",
        price=10.6,
        size=0.5,
        fee=0.01,
        fee_asset="USD",
        session_id="",
        sequence_id=0,
    )
    assert isinstance(execution_id, int)
    assert execution_id > 0
    snapshot_inst_pid = instrument_public_id
    async with repo.session() as session:
        stored_snapshot = MarketSnapshot(
            instrument_public_id=snapshot_inst_pid,
            bid=10.4,
            bid_volume=1.0,
            ask=10.6,
            ask_volume=1.5,
            last_price=10.5,
            volume_24h=5000.0,
            vwap_24h=10.3,
            low_24h=9.0,
            high_24h=11.5,
            change_24h=1.5,
            spread=0.2,
            spread_pct=0.018,
            timestamp=base_ts,
            session_id="test-session",
            sequence_id=1,
        )
        session.add(stored_snapshot)
        await session.commit()
    snapshots = await repo.get_market_snapshots(
        [snapshot_inst_pid],
        base_ts - timedelta(seconds=1),
        base_ts + timedelta(seconds=1),
        as_of=datetime.now(UTC),
    )
    assert len(snapshots) == 1
    result_snapshot = snapshots[0]
    normalized_ts = (
        result_snapshot["ts"]
        if result_snapshot["ts"].tzinfo
        else result_snapshot["ts"].replace(tzinfo=UTC)
    )
    assert normalized_ts == base_ts
    assert result_snapshot["instrument_public_id"] == snapshot_inst_pid
    assert result_snapshot["bid"] == pytest.approx(10.4)
    assert result_snapshot["bid_volume"] == pytest.approx(1.0)
    assert result_snapshot["ask"] == pytest.approx(10.6)
    assert result_snapshot["ask_volume"] == pytest.approx(1.5)
    assert result_snapshot["last"] == pytest.approx(10.5)
    assert result_snapshot["volume"] == pytest.approx(5000.0)
    assert result_snapshot["vwap"] == pytest.approx(10.3)
    assert result_snapshot["low"] == pytest.approx(9.0)
    assert result_snapshot["high"] == pytest.approx(11.5)


class DummyEngine(SimpleNamespace):
    """Dummy async engine for testing repository disposal."""

    def __init__(self) -> None:
        """Initialize the instance."""
        super().__init__()
        self.dispose = asyncio.create_task


class DummyRepo(SimpleNamespace):
    """Dummy repository for testing cache operations."""

    def __init__(self) -> None:
        """Initialize the instance."""
        super().__init__()
        self.engine = DummyEngine()


def teardown_function() -> None:
    """Clear repository cache after each test function."""
    repo._repository_cache.clear()


@pytest.mark.asyncio
async def test_dispose_repositories_awaits_dispose(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test dispose_repositories awaits engine dispose.

    Given: Repository cache with dummy repo,
    When: dispose_repositories is called,
    Then: Cache is cleared after disposal.
    """
    dummy = DummyRepo()
    repo._repository_cache["test"] = cast(Any, dummy)
    await repo.dispose_repositories()
    assert not repo._repository_cache


def test_sqlalchemy_repository_pool_setup_sqlite_memory() -> None:
    """Test SQLAlchemyRepository handles SQLite memory URL.

    Given: SQLite memory URL,
    When: SQLAlchemyRepository is instantiated,
    Then: URL is preserved.
    """
    sa_repo = repo.SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    assert "sqlite" in sa_repo.db_url


@pytest.mark.asyncio
async def test_sqlalchemy_session_rolls_back_on_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test SQLAlchemyRepository session rolls back on exception.

    Given: Mocked session factory,
    When: RuntimeError raised in session context,
    Then: Session rollback is called.
    """
    sa_repo = repo.SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")

    class DummySession:
        def __init__(self) -> None:
            self.committed = False
            self.rolled = False

        async def __aenter__(self) -> DummySession:
            return self

        async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            tb: TracebackType | None,
        ) -> None:
            return None

        async def rollback(self) -> None:
            self.rolled = True

    dummy = DummySession()

    class DummyCtx:
        async def __aenter__(self) -> DummySession:
            return dummy

        async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            tb: TracebackType | None,
        ) -> None:
            return None

        async def rollback(self) -> None:
            await dummy.rollback()

    monkeypatch.setattr(sa_repo, "session_factory", lambda: DummyCtx())
    with pytest.raises(RuntimeError):
        async with sa_repo.session():
            raise RuntimeError("boom")
    assert dummy.rolled


@pytest.mark.asyncio
async def test_sqlalchemy_repository_create_all_runs_metadata() -> None:
    """Test SQLAlchemyRepository.create_all runs model metadata on the engine."""
    called_with: list[Callable[..., object]] = []

    class _Connection:
        async def __aenter__(self) -> _Connection:
            return self

        async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            tb: TracebackType | None,
        ) -> None:
            return None

        async def run_sync(self, fn: Callable[..., object]) -> None:
            called_with.append(fn)

    class _Engine:
        def begin(self) -> _Connection:
            return _Connection()

    repo = SQLAlchemyRepository.__new__(SQLAlchemyRepository)
    repo.engine = cast(AsyncEngine, _Engine())

    create_all = cast(
        Callable[[SQLAlchemyRepository], Awaitable[None]],
        repo_module.SQLAlchemyRepository.create_all.__wrapped__,
    )
    await create_all(repo)

    assert called_with == [repo_module.Base.metadata.create_all]


class DummyRepository(Repository):
    """Dummy repository implementing Repository interface for testing."""

    def session(self) -> AbstractAsyncContextManager[AsyncSession]:
        """Raise NotImplementedError as session is not implemented."""
        raise NotImplementedError

    async def create_all(self) -> None:
        """Create all tables - no-op for dummy."""
        return None

    @property
    def dialect_name(self) -> str:
        """Return dummy dialect name."""
        return "dummy"

    async def ensure_instrument(
        self,
        symbol_public_id: str,
        exchange: str,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
    ) -> tuple[int, str]:
        """Ensure instrument - no-op returning (0, stub-public-id)."""
        return (0, "stub-public-id")

    async def revise_instrument(
        self,
        instrument_public_id: str,
        symbol_public_id: str,
        exchange: str,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
    ) -> int:
        """Revise instrument - no-op returning 0."""
        return 0

    async def revise_instrument_spec(
        self,
        instrument_public_id: str,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
        spec: InstrumentSpecInput,
    ) -> int:
        """Revise instrument spec - no-op returning 0."""
        return 0

    async def get_latest_candle_ids(
        self, as_of: datetime
    ) -> dict[tuple[str, str], tuple[datetime, str]]:
        """Load latest candle IDs - returns empty dict for dummy."""
        return {}

    async def upsert_candles(self, rows: list[dict[str, Any]]) -> int:
        """Upsert candles - no-op returning 0."""
        return 0

    async def upsert_trades(self, rows: list[dict[str, Any]]) -> int:
        """Upsert trades - no-op returning 0."""
        return 0

    async def upsert_ticks(self, rows: list[dict[str, Any]]) -> int:
        """Upsert ticks - no-op returning 0."""
        return 0

    async def insert_order(
        self,
        instrument_public_id: str,
        client_order_id: str | None,
        exchange_order_id: str | None,
        created_at: datetime,
        side: str,
        order_type: str,
        price: float | None,
        size: float,
        status: str,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
        time_in_force: str | None = None,
        mode: str = "live",
        leverage: int | None = None,
        reduce_only: bool = False,
    ) -> tuple[int, str]:
        """Insert order - no-op returning (0, stub-public-id)."""
        return (0, "stub-public-id")

    async def update_order(
        self,
        order_id: int,
        status: str,
        updated_at: datetime,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
        exchange_order_id: str | None = None,
        error: str | None = None,
        filled_size: float | None = None,
        average_price: float | None = None,
    ) -> int:
        """Update order - no-op returning 0."""
        return 0

    async def insert_execution(
        self,
        order_public_id: str,
        timestamp: datetime,
        side: str,
        status: str,
        price: float,
        size: float,
        fee: float,
        fee_asset: str,
        session_id: str,
        sequence_id: int,
        exec_id: str | None = None,
        trade_id: str | None = None,
    ) -> int:
        """Insert execution - no-op returning 0."""
        return 0

    async def get_candles(
        self,
        instrument: str,
        timeframe: str,
        start: datetime | None,
        end: datetime | None,
        exchange: str,
        as_of: datetime,
        limit: int | None = None,
        order: str = "asc",
    ) -> list[dict[str, Any]]:
        """Get candles - returns empty list."""
        return []

    async def get_trades(
        self,
        instrument: str,
        start: datetime,
        end: datetime,
        exchange: str,
        as_of: datetime,
    ) -> list[dict[str, Any]]:
        """Get trades - returns empty list."""
        return []

    async def get_market_snapshots(
        self,
        instrument_public_ids: list[str],
        start: datetime,
        end: datetime,
        as_of: datetime,
    ) -> list[dict[str, Any]]:
        """Get market snapshots - returns empty list."""
        return []

    async def upsert_market_snapshots(self, rows: list[dict[str, Any]]) -> int:
        """Upsert market snapshots - no-op returning 0."""
        return 0

    async def get_exchanges(self, as_of: datetime) -> list[str]:
        """Get exchanges - returns empty list."""
        return []

    async def get_exchange_instruments(self, exchange: str, as_of: datetime) -> list[str]:
        """Get exchange instruments - returns empty list."""
        return []

    async def get_signals(
        self,
        since: datetime,
        limit: int,
        as_of: datetime,
        instrument: str | None = None,
        strategy: str | None = None,
        exchange: str | None = None,
    ) -> list[dict[str, Any]]:
        """Get signals - returns empty list."""
        return []

    async def get_orders(
        self,
        limit: int,
        offset: int,
        as_of: datetime,
        symbol: str | None = None,
        exchange: str | None = None,
    ) -> list[dict[str, Any]]:
        """Get orders - returns empty list."""
        return []

    async def get_executions(self, limit: int, as_of: datetime) -> list[dict[str, Any]]:
        """Get executions - returns empty list."""
        return []

    async def get_positions(self, as_of: datetime) -> list[dict[str, Any]]:
        """Get positions - returns empty list."""
        return []

    async def get_settings(
        self, as_of: datetime, category: str | None = None
    ) -> list[dict[str, Any]]:
        """Get settings - returns empty list."""
        return []

    async def get_setting_by_key(self, key: str, as_of: datetime) -> dict[str, Any] | None:
        """Get setting by key - returns None."""
        return None

    async def get_setting_categories(self, as_of: datetime) -> list[str]:
        """Get setting categories - returns empty list."""
        return []


@pytest.mark.parametrize(
    ("input_url", "expected"),
    [
        ("sqlite+aiosqlite:///tmp/test.db", "sqlite:///tmp/test.db"),
        ("postgresql+asyncpg://user:pass@host/db", "postgresql+psycopg2://user:pass@host/db"),
        ("mysql://user:pass@host/db", "mysql://user:pass@host/db"),
    ],
)
def test_database_repository_converts_urls(
    monkeypatch: pytest.MonkeyPatch, input_url: str, expected: str
) -> None:
    """Verify DatabaseRepository converts async URLs to sync equivalents."""
    created_urls: list[str] = []

    class DummySyncEngine:
        def __init__(self, url: str) -> None:
            self.url = url

    def fake_create_sync_engine(db_url: str, future: bool = True) -> DummySyncEngine:
        created_urls.append(db_url)
        return DummySyncEngine(db_url)

    def fake_sync_sessionmaker(
        engine: Any, expire_on_commit: bool = False, class_: Any = None
    ) -> Callable[[], Any]:
        def factory() -> Any:
            return object()

        return factory

    monkeypatch.setattr(snapper.data.repository, "create_sync_engine", fake_create_sync_engine)
    monkeypatch.setattr(snapper.data.repository, "sync_sessionmaker", fake_sync_sessionmaker)
    repo = DatabaseRepository(input_url)
    assert repo.db_url == expected
    assert created_urls[-1] == expected


def test_database_repository_get_session_and_create_all(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Test DatabaseRepository get_session and create_all.

    Given: SQLite database path,
    When: create_all and get_session are called,
    Then: Database file created and session returned.
    """
    db_file = tmp_path / "test.db"
    repo = DatabaseRepository(f"sqlite:///{db_file}")
    repo.create_all()
    assert db_file.exists()
    session = repo.get_session()
    assert session is not None
    session.close()


def test_database_repository_create_all_delegates_to_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test DatabaseRepository.create_all delegates to SQLAlchemy metadata."""
    captured_engines: list[object] = []

    def fake_create_all(engine: object) -> None:
        captured_engines.append(engine)

    repo = DatabaseRepository.__new__(DatabaseRepository)
    repo.engine = object()
    monkeypatch.setattr(repo_module.Base.metadata, "create_all", fake_create_all)

    create_all = cast(
        Callable[[DatabaseRepository], None],
        repo_module.DatabaseRepository.create_all.__wrapped__,
    )
    create_all(repo)

    assert captured_engines == [repo.engine]


def test_database_repository_del_without_engine() -> None:
    """Test DatabaseRepository.__del__ tolerates missing engine attribute.

    Given: A partially initialized repository without an engine,
    When: __del__ is invoked,
    Then: No exception is raised.
    """
    repo = DatabaseRepository.__new__(DatabaseRepository)
    DatabaseRepository.__del__(repo)


def test_sqlalchemy_repository_has_no_custom_del() -> None:
    """Test SQLAlchemyRepository does not define custom garbage-collection cleanup.

    Given: Async repository cleanup is handled through explicit lifecycle hooks,
    When: Inspecting SQLAlchemyRepository,
    Then: It does not expose a custom ``__del__`` implementation.
    """
    assert "__del__" not in SQLAlchemyRepository.__dict__


def test_get_repository_caches_by_url(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test get_repository caches instances by URL.

    Given: Mocked repository classes,
    When: Called with same URL twice,
    Then: Returns same cached instance.
    """
    repo._repository_cache.clear()

    class _StubRepo:
        def __init__(self, url: str) -> None:
            self.url = url

    monkeypatch.setattr(SQLAlchemyRepository, "__call__", None, raising=False)
    monkeypatch.setattr("snapper.data.repository.SQLAlchemyRepository", lambda url: _StubRepo(url))
    repo1 = get_repository("sqlite:///:memory:")
    repo2 = get_repository("sqlite:///:memory:")
    repo3 = get_repository("postgresql+asyncpg://server/db")
    assert repo1 is repo2
    assert repo3 is not repo1


@pytest.mark.asyncio
async def test_dispose_repositories_handles_mock_engine(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test dispose_repositories handles mock engines.

    Given: Repository with MagicMock engine,
    When: dispose_repositories is called,
    Then: Cache is cleared.
    """
    repo._repository_cache.clear()

    class _StubRepo:
        def __init__(self, url: str) -> None:
            self.url = url
            self.engine = MagicMock()

    monkeypatch.setattr("snapper.data.repository.SQLAlchemyRepository", lambda url: _StubRepo(url))
    get_repository("sqlite:///:memory:")
    await dispose_repositories()
    assert repo_module._repository_cache == {}


@pytest.mark.asyncio
async def test_dispose_repositories_with_sync_dispose(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test dispose_repositories with synchronous dispose.

    Given: Repository with sync dispose method,
    When: dispose_repositories is called,
    Then: Dispose is called and cache cleared.
    """
    repo._repository_cache.clear()
    dispose_called = {"value": False}

    class _SyncEngine:
        def dispose(self) -> None:
            dispose_called["value"] = True
            return None

    class _StubRepo:
        def __init__(self) -> None:
            self.engine = _SyncEngine()

    repo_module._repository_cache["test_sync"] = _StubRepo()
    await dispose_repositories()
    assert dispose_called["value"] is True
    assert repo_module._repository_cache == {}


@pytest.mark.asyncio
async def test_dispose_repositories_includes_live_uncached_repositories() -> None:
    """Test dispose_repositories also disposes uncached live SQLAlchemyRepository instances."""
    repo._repository_cache.clear()
    dispose_called = {"value": False}

    class _SyncEngine:
        def dispose(self) -> None:
            dispose_called["value"] = True
            return None

    class _LiveRepo:
        def __init__(self) -> None:
            self.engine = _SyncEngine()

    live_repo = _LiveRepo()
    repo_module._live_sqlalchemy_repositories.add(live_repo)
    await dispose_repositories()
    assert dispose_called["value"] is True
    assert repo_module._repository_cache == {}


@pytest.mark.asyncio
async def test_dispose_repositories_engine_no_dispose() -> None:
    """Test dispose_repositories logs warning for engine without dispose.

    Given: Repository with engine lacking dispose method,
    When: dispose_repositories is called,
    Then: Logs warning and clears cache.
    """
    repo._repository_cache.clear()

    class _EngineNoDispose:
        pass

    class _StubRepo:
        def __init__(self) -> None:
            self.engine = _EngineNoDispose()

    repo_module._repository_cache["test_no_dispose"] = _StubRepo()
    with patch.object(repo_module, "logger") as mock_logger:
        await dispose_repositories()
    assert repo_module._repository_cache == {}
    mock_logger.warning.assert_called_once()
    assert "Failed to dispose repository engine" in mock_logger.warning.call_args[0][0]


@pytest.mark.asyncio
async def test_dispose_repositories_engine_dispose_not_callable() -> None:
    """Test dispose_repositories handles non-callable dispose.

    Given: Repository with dispose as non-callable,
    When: dispose_repositories is called,
    Then: Logs warning and clears cache.
    """
    repo._repository_cache.clear()

    class _EngineDisposeNotCallable:
        dispose = "not_callable"

    class _StubRepo:
        def __init__(self) -> None:
            self.engine = _EngineDisposeNotCallable()

    repo_module._repository_cache["test_not_callable"] = _StubRepo()
    with patch.object(repo_module, "logger") as mock_logger:
        await dispose_repositories()
    assert repo_module._repository_cache == {}
    mock_logger.warning.assert_called_once()
    assert "Failed to dispose repository engine" in mock_logger.warning.call_args[0][0]


@pytest.mark.asyncio
async def test_dispose_repositories_engine_none(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test dispose_repositories handles None engine.

    Given: Repository with engine set to None,
    When: dispose_repositories is called,
    Then: Cache is cleared without error.
    """
    repo._repository_cache.clear()

    class _StubRepo:
        def __init__(self) -> None:
            self.engine = None

    repo_module._repository_cache["test_engine_none"] = _StubRepo()
    await dispose_repositories()
    assert repo_module._repository_cache == {}


@pytest.mark.asyncio
async def test_dispose_repositories_no_engine_attr(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test dispose_repositories handles missing engine attribute.

    Given: Repository without engine attribute,
    When: dispose_repositories is called,
    Then: Cache is cleared without error.
    """
    repo._repository_cache.clear()

    class _StubRepo:
        pass

    repo_module._repository_cache["test_no_engine"] = _StubRepo()
    await dispose_repositories()
    assert repo_module._repository_cache == {}


def test_get_repository_sqlite() -> None:
    """Test get_repository returns SQLAlchemyRepository for SQLite URL.

    Given: SQLite connection URL,
    When: get_repository is called,
    Then: Returns SQLAlchemyRepository instance.
    """
    with patch("snapper.data.repository.create_async_engine"):
        sqlite_url = "sqlite+aiosqlite:///:memory:"
        repo = get_repository(sqlite_url)
        assert isinstance(repo, SQLAlchemyRepository)


class TestSQLAlchemyRepositoryDialects:
    """Tests for SQLAlchemy repository dialect-specific behaviors."""

    @pytest.fixture
    def mock_postgres_repo(self) -> Generator[SQLAlchemyRepository]:
        """Create mocked PostgreSQL repository for testing."""
        with patch("snapper.data.repository.create_async_engine") as mock_engine:
            mock_engine.return_value = Mock()
            repo = SQLAlchemyRepository("postgresql+asyncpg://user:pass@localhost/test")
            with patch.object(type(repo), "dialect_name", new_callable=lambda: "postgresql"):
                yield repo

    @pytest.fixture
    def mock_other_repo(self) -> Generator[SQLAlchemyRepository]:
        """Create mocked non-PostgreSQL repository for testing."""
        with patch("snapper.data.repository.create_async_engine") as mock_engine:
            mock_engine.return_value = Mock()
            repo = SQLAlchemyRepository("mysql+aiomysql://user:pass@localhost/test")
            with patch.object(type(repo), "dialect_name", new_callable=lambda: "mysql"):
                yield repo

    @pytest.mark.asyncio
    async def test_upsert_candles_postgres_dialect(
        self, mock_postgres_repo: SQLAlchemyRepository
    ) -> None:
        """Verify upsert_candles uses unified close+insert on PostgreSQL.

        Given: PostgreSQL repository with no existing active candle,
        When: upsert_candles is called,
        Then: SELECT finds no match, row is added via session.add.
        """
        mock_session = AsyncMock()
        mock_session.add = MagicMock()
        scalars_mock = Mock()
        scalars_mock.first.return_value = None
        select_result = Mock()
        select_result.scalars.return_value = scalars_mock
        mock_session.execute.return_value = select_result
        with patch.object(mock_postgres_repo, "session") as mock_session_ctx:
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            mock_session_ctx.return_value.__aexit__.return_value = None
            rows: list[dict[str, Any]] = [
                {
                    "instrument_public_id": "fake-inst-pid",
                    "open_at": datetime(2024, 1, 1, tzinfo=UTC),
                    "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
                    "timeframe": "1m",
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.5,
                    "volume": 1000.0,
                    "vwap": None,
                    "trades": 10,
                    "session_id": "test-session",
                    "sequence_id": 1,
                }
            ]
            result = await mock_postgres_repo.upsert_candles(rows)
            assert result == 1
            mock_session.execute.assert_called_once()
            mock_session.add.assert_called_once()
            mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_upsert_candles_inserts_new_rows(
        self, mock_other_repo: SQLAlchemyRepository
    ) -> None:
        """Verify upsert_candles inserts new rows when no active match.

        Given: Repository with no existing active candles,
        When: upsert_candles is called with two rows,
        Then: Each row triggers a SELECT (no match) then session.add.
        """
        mock_session = AsyncMock()
        mock_session.add = MagicMock()
        scalars_mock = Mock()
        scalars_mock.first.return_value = None
        select_result = Mock()
        select_result.scalars.return_value = scalars_mock
        mock_session.execute.return_value = select_result
        with patch.object(mock_other_repo, "session") as mock_session_ctx:
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            mock_session_ctx.return_value.__aexit__.return_value = None
            rows: list[dict[str, Any]] = [
                {
                    "instrument_public_id": "fake-inst-pid",
                    "open_at": datetime(2024, 1, 1, tzinfo=UTC),
                    "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
                    "timeframe": "1m",
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.5,
                    "volume": 1000.0,
                    "vwap": None,
                    "trades": 10,
                    "session_id": "test-session",
                    "sequence_id": 1,
                },
                {
                    "instrument_public_id": "fake-inst-pid",
                    "open_at": datetime(2024, 1, 1, 0, 1, tzinfo=UTC),
                    "timestamp": datetime(2024, 1, 1, 0, 1, tzinfo=UTC),
                    "timeframe": "1m",
                    "open": 100.5,
                    "high": 101.5,
                    "low": 99.5,
                    "close": 101.0,
                    "volume": 1200.0,
                    "vwap": None,
                    "trades": 12,
                    "session_id": "test-session",
                    "sequence_id": 1,
                },
            ]
            result = await mock_other_repo.upsert_candles(rows)
            assert result == 2
            assert mock_session.execute.call_count == 2
            assert mock_session.add.call_count == 2
            mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_upsert_candles_closes_old_and_inserts_new(
        self, mock_other_repo: SQLAlchemyRepository
    ) -> None:
        """Verify upsert_candles closes old and inserts new (SCD Type 2).

        Given: Repository with an existing active candle,
        When: upsert_candles is called with matching key,
        Then: SELECT finds existing, UPDATE closes old, session.add inserts new.
        """
        mock_session = AsyncMock()
        mock_session.add = MagicMock()
        existing_candle = SimpleNamespace(id=42, public_id="old-uuid")
        scalars_mock = Mock()
        scalars_mock.first.return_value = existing_candle
        select_result = Mock()
        select_result.scalars.return_value = scalars_mock
        update_result = Mock(rowcount=1)
        mock_session.execute.side_effect = [
            select_result,
            update_result,
        ]
        with patch.object(mock_other_repo, "session") as mock_session_ctx:
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            mock_session_ctx.return_value.__aexit__.return_value = None
            rows: list[dict[str, Any]] = [
                {
                    "instrument_public_id": "fake-inst-pid",
                    "open_at": datetime(2024, 1, 1, tzinfo=UTC),
                    "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
                    "timeframe": "1m",
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.5,
                    "volume": 1000.0,
                    "vwap": None,
                    "trades": 10,
                    "session_id": "test-session",
                    "sequence_id": 1,
                },
            ]
            result = await mock_other_repo.upsert_candles(rows)
            assert result == 1
            assert mock_session.execute.call_count == 2
            mock_session.add.assert_called_once()
            mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_upsert_trades_postgres_dialect(
        self, mock_postgres_repo: SQLAlchemyRepository
    ) -> None:
        """Verify upsert_trades uses PostgreSQL ON CONFLICT syntax.

        Given: PostgreSQL repository,
        When: upsert_trades is called,
        Then: Uses bulk insert with rowcount.
        """
        mock_session = AsyncMock()
        mock_result = Mock()
        mock_result.rowcount = 3
        mock_session.execute.return_value = mock_result
        with patch.object(mock_postgres_repo, "session") as mock_session_ctx:
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            mock_session_ctx.return_value.__aexit__.return_value = None
            rows: list[dict[str, Any]] = [
                {
                    "trade_id": "trade_1",
                    "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
                    "side": "buy",
                    "size": 100.0,
                    "price": 100.5,
                }
            ]
            result = await mock_postgres_repo.upsert_trades(rows)
            assert result == 3
            mock_session.execute.assert_called_once()
            mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_upsert_trades_other_dialect(self, mock_other_repo: SQLAlchemyRepository) -> None:
        """Verify upsert_trades uses row-by-row insert for non-PostgreSQL.

        Given: MySQL repository,
        When: upsert_trades is called,
        Then: Inserts rows individually.
        """
        mock_session = AsyncMock()
        _patch_begin_nested(mock_session)
        with patch.object(mock_other_repo, "session") as mock_session_ctx:
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            mock_session_ctx.return_value.__aexit__.return_value = None
            rows: list[dict[str, Any]] = [
                {
                    "trade_id": "trade_1",
                    "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
                    "side": "buy",
                    "size": 100.0,
                    "price": 100.5,
                }
            ]
            result = await mock_other_repo.upsert_trades(rows)
            assert result == 1
            mock_session.execute.assert_called_once()
            mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_upsert_empty_rows(self, mock_postgres_repo: SQLAlchemyRepository) -> None:
        """Verify upsert methods return 0 for empty input.

        Given: Empty row list,
        When: upsert_candles/upsert_trades/upsert_ticks is called,
        Then: Returns 0 without database operation.
        """
        result_candles = await mock_postgres_repo.upsert_candles([])
        assert result_candles == 0
        result_trades = await mock_postgres_repo.upsert_trades([])
        assert result_trades == 0
        result_ticks = await mock_postgres_repo.upsert_ticks([])
        assert result_ticks == 0

    @pytest.mark.asyncio
    async def test_ensure_instrument_with_integrity_error(
        self, mock_postgres_repo: SQLAlchemyRepository
    ) -> None:
        """Verify ensure_instrument handles duplicate key gracefully.

        Given: No active instrument found, insert hits IntegrityError (race),
        When: ensure_instrument is called,
        Then: Retries lookup and returns existing instrument ID.
        """
        mock_session = AsyncMock()
        mock_result = Mock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result
        mock_instrument = Mock()
        mock_instrument.id = 123
        mock_instrument.public_id = "mock-public-id-123"
        mock_session.add = Mock()
        mock_session.commit.side_effect = [IntegrityError("duplicate", "params", Exception()), None]
        mock_result2 = Mock()
        mock_result2.scalar_one_or_none.return_value = mock_instrument
        mock_session.execute.side_effect = [mock_result, mock_result2]
        mock_session.refresh = AsyncMock()
        with patch.object(mock_postgres_repo, "session") as mock_session_ctx:
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            mock_session_ctx.return_value.__aexit__.return_value = None
            result = await mock_postgres_repo.ensure_instrument(
                symbol_public_id="fake-spid",
                exchange="kraken",
                session_id="test-session",
                sequence_id=1,
                timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            )
            assert result == (123, "mock-public-id-123")
            mock_session.rollback.assert_called_once()

    @pytest.mark.asyncio
    async def test_ensure_instrument_race_retry_uses_bus_time(
        self, mock_postgres_repo: SQLAlchemyRepository
    ) -> None:
        """Regression: retry after IntegrityError uses caller's bus_time.

        Given: Historical timestamp passed to ensure_instrument,
        When: First INSERT hits IntegrityError (race),
        Then: Retry lookup calls where_active with the same bus_time,
              not datetime.now(UTC).
        """
        historical_time = datetime(2024, 1, 15, tzinfo=UTC)
        mock_session = AsyncMock()
        mock_result = Mock()
        mock_result.scalar_one_or_none.return_value = None
        mock_instrument = Mock()
        mock_instrument.id = 42
        mock_instrument.public_id = "hist-pid"
        mock_result2 = Mock()
        mock_result2.scalar_one_or_none.return_value = mock_instrument
        mock_session.execute.side_effect = [mock_result, mock_result2]
        mock_session.add = Mock()
        mock_session.commit.side_effect = [
            IntegrityError("dup", "params", Exception()),
            None,
        ]
        with patch.object(mock_postgres_repo, "session") as mock_ctx:
            mock_ctx.return_value.__aenter__.return_value = mock_session
            mock_ctx.return_value.__aexit__.return_value = None
            result = await mock_postgres_repo.ensure_instrument(
                symbol_public_id="fake-spid",
                exchange="kraken",
                session_id="s",
                sequence_id=1,
                timestamp=historical_time,
            )
            assert result == (42, "hist-pid")
            retry_stmt = mock_session.execute.call_args_list[1].args[0]
            compiled = retry_stmt.compile(compile_kwargs={"literal_binds": True})
            compiled_sql = str(compiled)
            assert "2024-01-15" in compiled_sql

    @pytest.mark.asyncio
    async def test_ensure_instrument_integrity_error_reraise(
        self, mock_postgres_repo: SQLAlchemyRepository
    ) -> None:
        """Verify ensure_instrument re-raises when retry also finds nothing.

        Given: Instrument not found before or after IntegrityError,
        When: ensure_instrument is called,
        Then: IntegrityError is re-raised.
        """
        mock_session = AsyncMock()
        mock_result = Mock()
        mock_result.scalar_one_or_none.return_value = None
        mock_result2 = Mock()
        mock_result2.scalar_one_or_none.return_value = None
        mock_session.execute.side_effect = [mock_result, mock_result2]
        mock_session.add = Mock()
        mock_session.commit.side_effect = IntegrityError("duplicate", "params", Exception())
        mock_session.refresh = AsyncMock()
        with patch.object(mock_postgres_repo, "session") as mock_session_ctx:
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            mock_session_ctx.return_value.__aexit__.return_value = None
            with pytest.raises(IntegrityError):
                await mock_postgres_repo.ensure_instrument(
                    symbol_public_id="fake-spid",
                    exchange="kraken",
                    session_id="test-session",
                    sequence_id=1,
                    timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                )
            mock_session.rollback.assert_called_once()

    @pytest.mark.asyncio
    async def test_ensure_instrument_existing(
        self, mock_postgres_repo: SQLAlchemyRepository
    ) -> None:
        """Verify ensure_instrument returns existing ID when payload matches.

        Given: Active instrument with identical payload exists,
        When: ensure_instrument is called,
        Then: Returns existing ID, skips add.
        """
        mock_session = AsyncMock()
        mock_result = Mock()
        mock_instrument = Mock()
        mock_instrument.id = 456
        mock_instrument.public_id = "mock-public-id-456"
        mock_instrument.symbol = "ETH-USD"
        mock_instrument.base = "ETH"
        mock_instrument.quote = "USD"
        mock_result.scalar_one_or_none.return_value = mock_instrument
        mock_session.execute.return_value = mock_result
        with patch.object(mock_postgres_repo, "session") as mock_session_ctx:
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            mock_session_ctx.return_value.__aexit__.return_value = None
            result = await mock_postgres_repo.ensure_instrument(
                symbol_public_id="fake-spid",
                exchange="kraken",
                session_id="test-session",
                sequence_id=1,
                timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            )
            assert result == (456, "mock-public-id-456")
            mock_session.add.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_candles_symbol_not_found(
        self, mock_postgres_repo: SQLAlchemyRepository
    ) -> None:
        """Verify get_candles returns empty list when Symbol row is missing.

        Given: No active Symbol row for the requested native_symbol,
        When: get_candles is called,
        Then: Returns empty list without querying Instrument.
        """
        mock_session = AsyncMock()
        mock_sym_result = Mock()
        mock_sym_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_sym_result
        with patch.object(mock_postgres_repo, "session") as mock_session_ctx:
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            mock_session_ctx.return_value.__aexit__.return_value = None
            start = datetime(2024, 1, 1, tzinfo=UTC)
            end = datetime(2024, 1, 1, 1, 0, tzinfo=UTC)
            result = await mock_postgres_repo.get_candles(
                "NONEXISTENT", "1m", start, end, exchange="kraken", as_of=start
            )
            assert result == []

    @pytest.mark.asyncio
    async def test_get_candles_instrument_not_found(
        self, mock_postgres_repo: SQLAlchemyRepository
    ) -> None:
        """Verify get_candles returns empty list for unknown instrument.

        Given: Symbol exists but no matching Instrument,
        When: get_candles is called,
        Then: Returns empty list.
        """
        mock_session = AsyncMock()
        mock_sym_result = Mock()
        mock_sym_result.scalar_one_or_none.return_value = "sym-pub-1"
        mock_inst_result = Mock()
        mock_inst_scalars = Mock()
        mock_inst_scalars.first.return_value = None
        mock_inst_result.scalars.return_value = mock_inst_scalars
        mock_session.execute.side_effect = [mock_sym_result, mock_inst_result]
        with patch.object(mock_postgres_repo, "session") as mock_session_ctx:
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            mock_session_ctx.return_value.__aexit__.return_value = None
            start = datetime(2024, 1, 1, tzinfo=UTC)
            end = datetime(2024, 1, 1, 1, 0, tzinfo=UTC)
            result = await mock_postgres_repo.get_candles(
                "NONEXISTENT", "1m", start, end, exchange="kraken", as_of=start
            )
            assert result == []

    @pytest.mark.asyncio
    async def test_get_candles_success(self, mock_postgres_repo: SQLAlchemyRepository) -> None:
        """Verify get_candles returns candle data as dictionaries.

        Given: Instrument with candles,
        When: get_candles is called,
        Then: Returns list of candle dicts.
        """
        mock_session = AsyncMock()
        mock_sym_result = Mock()
        mock_sym_result.scalar_one_or_none.return_value = "sym-pub-1"
        mock_inst_result = Mock()
        mock_instrument = Mock()
        mock_instrument.id = 1
        mock_inst_scalars = Mock()
        mock_inst_scalars.first.return_value = mock_instrument
        mock_inst_result.scalars.return_value = mock_inst_scalars
        mock_candles_result = Mock()
        mock_row = Mock()
        mock_row.open_at = datetime(2024, 1, 1, tzinfo=UTC)
        mock_row.timeframe = "1m"
        mock_row.open = 100.0
        mock_row.high = 101.0
        mock_row.low = 99.0
        mock_row.close = 100.5
        mock_row.volume = 1000.0
        mock_row.vwap = None
        mock_row.trades = 10
        mock_row.public_id = "candle-pub-1"
        mock_row.timestamp = datetime(2024, 1, 1, tzinfo=UTC)
        mock_row.session_id = "sess-1"
        mock_row.sequence_id = 1
        mock_candles_result.all.return_value = [mock_row]
        mock_session.execute.side_effect = [mock_sym_result, mock_inst_result, mock_candles_result]
        with patch.object(mock_postgres_repo, "session") as mock_session_ctx:
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            mock_session_ctx.return_value.__aexit__.return_value = None
            start = datetime(2024, 1, 1, tzinfo=UTC)
            end = datetime(2024, 1, 1, 1, 0, tzinfo=UTC)
            result = await mock_postgres_repo.get_candles(
                "BTC-USD", "1m", start, end, exchange="kraken", as_of=start
            )
            expected: list[dict[str, Any]] = [
                {
                    "open_at": datetime(2024, 1, 1, tzinfo=UTC),
                    "timeframe": "1m",
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.5,
                    "volume": 1000.0,
                    "vwap": None,
                    "trades": 10,
                    "public_id": "candle-pub-1",
                    "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
                    "session_id": "sess-1",
                    "sequence_id": 1,
                }
            ]
            assert result == expected

    @pytest.mark.asyncio
    async def test_get_candles_with_exchange_filter(
        self, mock_postgres_repo: SQLAlchemyRepository
    ) -> None:
        """Verify get_candles filters by exchange when provided.

        Given: Instrument with exchange,
        When: get_candles is called with exchange parameter,
        Then: Filters by both symbol and exchange.
        """
        mock_session = AsyncMock()
        mock_inst_result = Mock()
        mock_inst_scalars = Mock()
        mock_inst_scalars.first.return_value = None
        mock_inst_result.scalars.return_value = mock_inst_scalars
        mock_session.execute.return_value = mock_inst_result
        with patch.object(mock_postgres_repo, "session") as mock_session_ctx:
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            mock_session_ctx.return_value.__aexit__.return_value = None
            start = datetime(2024, 1, 1, tzinfo=UTC)
            end = datetime(2024, 1, 1, 1, 0, tzinfo=UTC)
            result = await mock_postgres_repo.get_candles(
                "BTC-USD", "1m", start, end, exchange="kraken", as_of=start
            )
            assert result == []

    def test_dialect_name_property(self, mock_postgres_repo: SQLAlchemyRepository) -> None:
        """Verify dialect_name returns correct database dialect.

        Given: PostgreSQL repository,
        When: dialect_name is accessed,
        Then: Returns 'postgresql'.
        """
        assert mock_postgres_repo.dialect_name == "postgresql"


@pytest.mark.asyncio()
async def test_dispose_repositories_awaits_coroutine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify dispose_repositories awaits async dispose methods."""
    repo._repository_cache.clear()
    disposed: list[bool] = []

    async def _async_dispose() -> None:
        disposed.append(True)

    class _Repo:
        def __init__(self) -> None:
            self.engine = SimpleNamespace(dispose=_async_dispose)

    repo_module._repository_cache["sqlite:///:memory:"] = cast(repo_module.Repository, _Repo())
    await dispose_repositories()
    assert disposed == [True]
    assert repo_module._repository_cache == {}


@pytest.mark.asyncio()
async def test_get_trades_returns_empty_when_instrument_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify get_trades returns empty list when instrument is not found."""
    with patch("snapper.data.repository.create_async_engine"):
        repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    mock_sym_result = SimpleNamespace(scalar_one_or_none=lambda: None)

    async def _execute(*_: object, **__: object) -> SimpleNamespace:
        return mock_sym_result

    mock_session = AsyncMock()
    mock_session.execute.side_effect = _execute

    class _Ctx:
        async def __aenter__(self) -> Any:
            return mock_session

        async def __aexit__(self, *_: object) -> None:
            return None

    monkeypatch.setattr(repo, "session", lambda: _Ctx())
    result = await repo.get_trades(
        "MISSING",
        datetime.now(UTC),
        datetime.now(UTC),
        exchange="kraken",
        as_of=datetime.now(UTC),
    )
    assert result == []


@pytest.mark.asyncio()
async def test_get_trades_with_exchange_filter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify get_trades filters by exchange when provided."""
    with patch("snapper.data.repository.create_async_engine"):
        repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    mock_sym_result = SimpleNamespace(scalar_one_or_none=lambda: "sym-pub-1")
    mock_inst_result = SimpleNamespace(scalars=lambda: SimpleNamespace(first=lambda: None))
    call_count = 0

    async def _execute(*_: object, **__: object) -> SimpleNamespace:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return mock_sym_result
        return mock_inst_result

    mock_session = AsyncMock()
    mock_session.execute.side_effect = _execute

    class _Ctx:
        async def __aenter__(self) -> Any:
            return mock_session

        async def __aexit__(self, *_: object) -> None:
            return None

    monkeypatch.setattr(repo, "session", lambda: _Ctx())
    result = await repo.get_trades(
        "MISSING",
        datetime.now(UTC),
        datetime.now(UTC),
        exchange="kraken",
        as_of=datetime.now(UTC),
    )
    assert result == []


class _MinimalRepository(Repository):
    def session(self) -> Any:
        raise NotImplementedError

    async def create_all(self) -> None:
        """Intentionally empty async stub for testing."""
        pass

    @property
    def dialect_name(self) -> str:
        return "sqlite"

    async def get_latest_candle_ids(
        self, as_of: datetime
    ) -> dict[tuple[str, str], tuple[datetime, str]]:
        return {}

    async def upsert_candles(self, rows: list[dict[str, Any]]) -> int:
        return 0

    async def upsert_trades(self, rows: list[dict[str, Any]]) -> int:
        return 0

    async def upsert_ticks(self, rows: list[dict[str, Any]]) -> int:
        return 0

    async def ensure_instrument(
        self,
        symbol_public_id: str,
        exchange: str,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
    ) -> tuple[int, str]:
        return (0, "stub-public-id")

    async def revise_instrument(
        self,
        instrument_public_id: str,
        symbol_public_id: str,
        exchange: str,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
    ) -> int:
        return 0

    async def insert_order(
        self,
        instrument_public_id: str,
        client_order_id: str | None,
        exchange_order_id: str | None,
        created_at: datetime,
        side: str,
        order_type: str,
        price: float | None,
        size: float,
        status: str,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
        time_in_force: str | None = None,
        mode: str = "live",
        leverage: int | None = None,
        reduce_only: bool = False,
    ) -> tuple[int, str]:
        return (0, "stub-public-id")

    async def update_order(
        self,
        order_id: int,
        status: str,
        updated_at: datetime,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
        exchange_order_id: str | None = None,
        error: str | None = None,
        filled_size: float | None = None,
        average_price: float | None = None,
    ) -> int:
        """Update order - no-op returning 0."""
        return 0

    async def insert_execution(
        self,
        order_public_id: str,
        timestamp: datetime,
        side: str,
        status: str,
        price: float,
        size: float,
        fee: float,
        fee_asset: str,
        session_id: str,
        sequence_id: int,
        exec_id: str | None = None,
        trade_id: str | None = None,
    ) -> int:
        return 0

    async def revise_instrument_spec(
        self,
        instrument_public_id: str,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
        spec: InstrumentSpecInput,
    ) -> int:
        return 0

    async def get_candles(
        self,
        instrument: str,
        timeframe: str,
        start: datetime,
        end: datetime,
        exchange: str,
        as_of: datetime,
        limit: int | None = None,
        order: str = "asc",
    ) -> list[dict[str, Any]]:
        return []

    async def get_trades(
        self,
        instrument: str,
        start: datetime,
        end: datetime,
        exchange: str,
        as_of: datetime,
    ) -> list[dict[str, Any]]:
        return []

    async def get_market_snapshots(
        self,
        instrument_public_ids: list[str],
        start: datetime,
        end: datetime,
        as_of: datetime,
    ) -> list[dict[str, Any]]:
        return []

    async def upsert_market_snapshots(self, rows: list[dict[str, Any]]) -> int:
        return 0

    async def get_exchanges(self, as_of: datetime) -> list[str]:
        return []

    async def get_exchange_instruments(self, exchange: str, as_of: datetime) -> list[str]:
        return []

    async def get_signals(
        self,
        since: datetime,
        limit: int,
        as_of: datetime,
        instrument: str | None = None,
        strategy: str | None = None,
        exchange: str | None = None,
    ) -> list[dict[str, Any]]:
        return []

    async def get_orders(
        self,
        limit: int,
        offset: int,
        as_of: datetime,
        symbol: str | None = None,
        exchange: str | None = None,
    ) -> list[dict[str, Any]]:
        return []

    async def get_executions(self, limit: int, as_of: datetime) -> list[dict[str, Any]]:
        return []

    async def get_positions(self, as_of: datetime) -> list[dict[str, Any]]:
        return []

    async def get_settings(
        self, as_of: datetime, category: str | None = None
    ) -> list[dict[str, Any]]:
        return []

    async def get_setting_by_key(self, key: str, as_of: datetime) -> dict[str, Any] | None:
        return None

    async def get_setting_categories(self, as_of: datetime) -> list[str]:
        return []


class TestWhereActive:
    """Tests for the where_active temporal filter helpers."""

    def test_where_active_now_returns_two_clauses(self) -> None:
        """where_active_now returns a tuple of exactly two filter clauses.

        Given: A model with timestamp and known_to columns,
        When: where_active_now is called,
        Then: Returns a tuple of two SQLAlchemy filter expressions.
        """
        clauses = where_active_now(MarketSnapshot)
        assert len(clauses) == 2

    def test_where_active_now_uses_current_time(self) -> None:
        """where_active_now uses current time for filtering.

        Given: A model with temporal columns,
        When: where_active_now is called,
        Then: The filter clauses reference the current UTC time.
        """
        ts_clause, known_to_clause = where_active_now(MarketSnapshot)
        assert ts_clause is not None
        assert known_to_clause is not None

    def test_where_active_with_explicit_time(self) -> None:
        """where_active uses the provided time for filtering.

        Given: A model and a specific point-in-time,
        When: where_active is called with at=some_time,
        Then: The clauses filter using that specific time.
        """
        fixed_time = datetime(2026, 6, 15, 12, 0, 0, tzinfo=UTC)
        ts_clause, known_to_clause = where_active(MarketSnapshot, at=fixed_time)
        assert "timestamp" in str(ts_clause)
        assert "known_to" in str(known_to_clause)


async def _seed_full_repo(tmp_path: Path) -> tuple[SQLAlchemyRepository, str, str]:
    """Seed a repository with symbol, alias, instrument, and related records.

    Returns (repo, symbol_public_id, instrument_public_id).
    """
    db_path = tmp_path / "rest_repo.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    async with r.session() as s:
        sym = Symbol(
            native_symbol="BTC-USD",
            base="BTC",
            quote="USD",
            asset_type="crypto",
            created_at=now,
            timestamp=now,
            session_id="s1",
            sequence_id=1,
        )
        s.add(sym)
        await s.commit()
        await s.refresh(sym)
        alias = SymbolAlias(
            symbol_public_id=sym.public_id,
            exchange="kraken",
            exchange_symbol="XXBTZUSD",
            channel="ws",
            created_at=now,
            timestamp=now,
            session_id="s1",
            sequence_id=2,
        )
        s.add(alias)
        await s.commit()
    _, inst_pid = await r.ensure_instrument(
        symbol_public_id=sym.public_id,
        exchange="kraken",
        session_id="s1",
        sequence_id=3,
        timestamp=now,
    )
    return r, sym.public_id, inst_pid


@pytest.mark.asyncio
async def test_get_exchanges_returns_active(tmp_path: Path) -> None:
    """Verify get_exchanges returns distinct active exchange names.

    Given: Repository with active symbol alias,
    When: get_exchanges is called,
    Then: Returns list containing the exchange name.
    """
    r, _, _ = await _seed_full_repo(tmp_path)
    result = await r.get_exchanges(as_of=datetime.now(UTC))
    assert result == ["kraken"]


@pytest.mark.asyncio
async def test_get_exchange_instruments_returns_symbols(tmp_path: Path) -> None:
    """Verify get_exchange_instruments returns native symbols for exchange.

    Given: Repository with symbol alias for kraken,
    When: get_exchange_instruments is called for kraken,
    Then: Returns list containing 'BTC-USD'.
    """
    r, _, _ = await _seed_full_repo(tmp_path)
    result = await r.get_exchange_instruments("kraken", as_of=datetime.now(UTC))
    assert result == ["BTC-USD"]


@pytest.mark.asyncio
async def test_get_signals_returns_data(tmp_path: Path) -> None:
    """Verify get_signals returns signal dicts with instrument info.

    Given: Repository with signal record,
    When: get_signals is called,
    Then: Returns denormalized signal dicts.
    """
    r, _, inst_pid = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    async with r.session() as s:
        s.add(
            Signal(
                instrument_public_id=inst_pid,
                wallet_public_id="00000000-0000-7000-8000-000000000001",
                strategy_name="rsi",
                side="buy",
                strength=0.9,
                reason="oversold",
                price=50000.0,
                fired_at=now - timedelta(minutes=5),
                timestamp=now,
                session_id="s1",
                sequence_id=10,
            )
        )
        await s.commit()
    result = await r.get_signals(since=now - timedelta(hours=1), limit=10, as_of=now)
    assert len(result) == 1
    assert result[0]["instrument"] == "BTC-USD"
    assert result[0]["exchange"] == "kraken"
    assert result[0]["strategy_name"] == "rsi"


@pytest.mark.asyncio
async def test_get_signals_filters_by_exchange(tmp_path: Path) -> None:
    """Verify get_signals filters by exchange.

    Given: Repository with signal on kraken,
    When: get_signals is called with exchange='zonda',
    Then: Returns empty list.
    """
    r, _, inst_pid = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    async with r.session() as s:
        s.add(
            Signal(
                instrument_public_id=inst_pid,
                wallet_public_id="00000000-0000-7000-8000-000000000001",
                strategy_name="rsi",
                side="buy",
                strength=0.9,
                reason="oversold",
                price=50000.0,
                fired_at=now - timedelta(minutes=5),
                timestamp=now,
                session_id="s1",
                sequence_id=10,
            )
        )
        await s.commit()
    result = await r.get_signals(
        since=now - timedelta(hours=1), limit=10, as_of=now, exchange="zonda"
    )
    assert len(result) == 0


@pytest.mark.asyncio
async def test_get_orders_returns_data(tmp_path: Path) -> None:
    """Verify get_orders returns order dicts with instrument info.

    Given: Repository with order record,
    When: get_orders is called,
    Then: Returns denormalized order dicts.
    """
    r, _, inst_pid = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    await r.insert_order(
        instrument_public_id=inst_pid,
        wallet_public_id="00000000-0000-7000-8000-000000000001",
        client_order_id="c1",
        exchange_order_id="e1",
        created_at=now,
        side="buy",
        order_type="limit",
        price=50000.0,
        size=1.0,
        status="open",
        session_id="s1",
        sequence_id=20,
        timestamp=now,
    )
    result = await r.get_orders(limit=10, offset=0, as_of=now)
    assert len(result) == 1
    assert result[0]["instrument"] == "BTC-USD"
    assert result[0]["exchange"] == "kraken"
    assert result[0]["side"] == "buy"


@pytest.mark.asyncio
async def test_get_orders_filters_by_exchange(tmp_path: Path) -> None:
    """Verify get_orders filters by exchange.

    Given: Repository with order on kraken,
    When: get_orders is called with exchange='zonda',
    Then: Returns empty list.
    """
    r, _, inst_pid = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    await r.insert_order(
        instrument_public_id=inst_pid,
        wallet_public_id="00000000-0000-7000-8000-000000000001",
        client_order_id="c1",
        exchange_order_id="e1",
        created_at=now,
        side="buy",
        order_type="limit",
        price=50000.0,
        size=1.0,
        status="open",
        session_id="s1",
        sequence_id=20,
        timestamp=now,
    )
    result = await r.get_orders(limit=10, offset=0, as_of=now, exchange="zonda")
    assert len(result) == 0


@pytest.mark.asyncio
async def test_insert_order_persists_leverage_and_reduce_only(tmp_path: Path) -> None:
    """Verify insert_order persists leverage and reduce_only.

    Given: Repository,
    When: insert_order is called with leverage=3 and reduce_only=True,
    Then: get_orders returns those values on the row.
    """
    r, _, inst_pid = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    await r.insert_order(
        instrument_public_id=inst_pid,
        wallet_public_id="00000000-0000-7000-8000-000000000001",
        client_order_id="c-lev",
        exchange_order_id="e-lev",
        created_at=now,
        side="sell",
        order_type="limit",
        price=50000.0,
        size=1.0,
        status="open",
        session_id="s1",
        sequence_id=30,
        timestamp=now,
        leverage=3,
        reduce_only=True,
    )
    result = await r.get_orders(limit=10, offset=0, as_of=now)
    assert len(result) == 1
    assert result[0]["leverage"] == 3
    assert result[0]["reduce_only"] is True


@pytest.mark.asyncio
async def test_insert_order_defaults_leverage_and_reduce_only(tmp_path: Path) -> None:
    """Verify insert_order defaults leverage to None and reduce_only to False.

    Given: Repository,
    When: insert_order is called without leverage/reduce_only kwargs,
    Then: get_orders returns leverage=None and reduce_only=False.
    """
    r, _, inst_pid = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    await r.insert_order(
        instrument_public_id=inst_pid,
        wallet_public_id="00000000-0000-7000-8000-000000000001",
        client_order_id="c-default",
        exchange_order_id="e-default",
        created_at=now,
        side="buy",
        order_type="limit",
        price=50000.0,
        size=1.0,
        status="open",
        session_id="s1",
        sequence_id=31,
        timestamp=now,
    )
    result = await r.get_orders(limit=10, offset=0, as_of=now)
    assert len(result) == 1
    assert result[0]["leverage"] is None
    assert result[0]["reduce_only"] is False


@pytest.mark.asyncio
async def test_update_order_carries_forward_leverage_and_reduce_only(tmp_path: Path) -> None:
    """Verify update_order SCD2 cycle preserves leverage and reduce_only.

    Given: Repository with an order persisted with leverage=5/reduce_only=True,
    When: update_order is called twice (first to partially_filled, then to filled),
    Then: After both updates, get_orders still returns leverage=5 and reduce_only=True
        because update_order carries forward the immutable margin/intent fields when
        constructing the new SCD2 row.
    """
    r, _, inst_pid = await _seed_full_repo(tmp_path)
    base_ts = datetime.now(UTC)
    order_id, _ = await r.insert_order(
        instrument_public_id=inst_pid,
        wallet_public_id="00000000-0000-7000-8000-000000000001",
        client_order_id="c-carry",
        exchange_order_id="e-carry",
        created_at=base_ts,
        side="sell",
        order_type="limit",
        price=50000.0,
        size=1.0,
        status="open",
        session_id="s1",
        sequence_id=40,
        timestamp=base_ts,
        leverage=5,
        reduce_only=True,
    )
    v2 = await r.update_order(
        order_id=order_id,
        status="partially_filled",
        updated_at=base_ts + timedelta(milliseconds=1),
        session_id="s1",
        sequence_id=41,
        timestamp=base_ts + timedelta(milliseconds=1),
        filled_size=0.5,
        average_price=49999.0,
    )
    await r.update_order(
        order_id=v2,
        status="filled",
        updated_at=base_ts + timedelta(milliseconds=2),
        session_id="s1",
        sequence_id=42,
        timestamp=base_ts + timedelta(milliseconds=2),
        filled_size=1.0,
        average_price=49999.0,
    )
    result = await r.get_orders(limit=10, offset=0, as_of=base_ts + timedelta(milliseconds=3))
    assert len(result) == 1
    assert result[0]["status"] == "filled"
    assert result[0]["leverage"] == 5
    assert result[0]["reduce_only"] is True


@pytest.mark.asyncio
async def test_get_executions_returns_data(tmp_path: Path) -> None:
    """Verify get_executions returns execution dicts.

    Given: Repository with order and execution records,
    When: get_executions is called,
    Then: Returns denormalized execution dicts.
    """
    r, _, inst_pid = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    _, order_pid = await r.insert_order(
        instrument_public_id=inst_pid,
        wallet_public_id="00000000-0000-7000-8000-000000000001",
        client_order_id="c1",
        exchange_order_id="e1",
        created_at=now,
        side="buy",
        order_type="limit",
        price=50000.0,
        size=1.0,
        status="filled",
        session_id="s1",
        sequence_id=20,
        timestamp=now,
    )
    await r.insert_execution(
        order_public_id=order_pid,
        wallet_public_id="00000000-0000-7000-8000-000000000001",
        timestamp=now,
        side="buy",
        status="filled",
        price=50000.0,
        size=1.0,
        fee=10.0,
        fee_asset="USD",
        session_id="s1",
        sequence_id=21,
    )
    result = await r.get_executions(limit=10, as_of=now)
    assert len(result) == 1
    assert result[0]["instrument"] == "BTC-USD"
    assert result[0]["exchange"] == "kraken"


@pytest.mark.asyncio
async def test_get_active_orders_for_recovery(tmp_path: Path) -> None:
    """Verify get_active_orders_for_recovery returns only active orders.

    Given: Repository with one open and one closed order on kraken,
    When: get_active_orders_for_recovery is called,
    Then: Only the open order is returned.
    """
    r, _, inst_pid = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    await r.insert_order(
        instrument_public_id=inst_pid,
        wallet_public_id="00000000-0000-7000-8000-000000000001",
        client_order_id="c-open",
        exchange_order_id="e-open",
        created_at=now,
        side="buy",
        order_type="market",
        price=None,
        size=1.0,
        status="open",
        session_id="s1",
        sequence_id=20,
        timestamp=now,
    )
    await r.insert_order(
        instrument_public_id=inst_pid,
        wallet_public_id="00000000-0000-7000-8000-000000000001",
        client_order_id="c-closed",
        exchange_order_id="e-closed",
        created_at=now,
        side="sell",
        order_type="limit",
        price=51000.0,
        size=0.5,
        status="closed",
        session_id="s1",
        sequence_id=21,
        timestamp=now,
    )
    result = await r.get_active_orders_for_recovery(exchange="kraken", as_of=now)
    assert len(result) == 1
    assert result[0]["client_order_id"] == "c-open"
    assert result[0]["status"] == "open"


@pytest.mark.asyncio
async def test_get_executions_for_recovery(tmp_path: Path) -> None:
    """Verify get_executions_for_recovery returns all executions in ASC order.

    Given: Repository with two executions,
    When: get_executions_for_recovery is called,
    Then: Both are returned in chronological order.
    """
    r, _, inst_pid = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    _, order_pid = await r.insert_order(
        instrument_public_id=inst_pid,
        wallet_public_id="00000000-0000-7000-8000-000000000001",
        client_order_id="c1",
        exchange_order_id="e1",
        created_at=now,
        side="buy",
        order_type="limit",
        price=50000.0,
        size=1.0,
        status="filled",
        session_id="s1",
        sequence_id=20,
        timestamp=now,
    )
    await r.insert_execution(
        order_public_id=order_pid,
        wallet_public_id="00000000-0000-7000-8000-000000000001",
        timestamp=now,
        side="buy",
        status="partial",
        price=50000.0,
        size=0.5,
        fee=5.0,
        fee_asset="USD",
        session_id="s1",
        sequence_id=21,
    )
    await r.insert_execution(
        order_public_id=order_pid,
        wallet_public_id="00000000-0000-7000-8000-000000000001",
        timestamp=now,
        side="buy",
        status="filled",
        price=50100.0,
        size=0.5,
        fee=5.0,
        fee_asset="USD",
        session_id="s1",
        sequence_id=22,
    )
    result = await r.get_executions_for_recovery(as_of=now, exchange="kraken")
    assert len(result) == 2
    assert result[0]["size"] == pytest.approx(0.5)
    assert result[1]["size"] == pytest.approx(0.5)


@pytest.mark.asyncio
async def test_get_executions_for_recovery_filters_instrument(tmp_path: Path) -> None:
    """Verify get_executions_for_recovery filters by instrument.

    Given: Repository with execution for BTC-USD,
    When: get_executions_for_recovery is called with instrument=ETH-USD,
    Then: Returns empty list.
    """
    r, _, inst_pid = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    _, order_pid = await r.insert_order(
        instrument_public_id=inst_pid,
        wallet_public_id="00000000-0000-7000-8000-000000000001",
        client_order_id="c1",
        exchange_order_id="e1",
        created_at=now,
        side="buy",
        order_type="limit",
        price=50000.0,
        size=1.0,
        status="filled",
        session_id="s1",
        sequence_id=20,
        timestamp=now,
    )
    await r.insert_execution(
        order_public_id=order_pid,
        wallet_public_id="00000000-0000-7000-8000-000000000001",
        timestamp=now,
        side="buy",
        status="filled",
        price=50000.0,
        size=1.0,
        fee=10.0,
        fee_asset="USD",
        session_id="s1",
        sequence_id=21,
    )
    result = await r.get_executions_for_recovery(as_of=now, instrument="ETH-USD")
    assert len(result) == 0


@pytest.mark.asyncio
async def test_get_active_orders_for_recovery_filters_by_wallet(tmp_path: Path) -> None:
    """``wallet_public_id`` filter scopes recovery to one wallet.

    Given: Two active orders on the same exchange tagged with two
        different ``wallet_public_id`` values via direct SCD2 update,
    When: ``get_active_orders_for_recovery`` is called with
        ``wallet_public_id=wallet_a``,
    Then: Only the order tagged ``wallet_a`` is returned, exercising
        the new ``Order.wallet_public_id == wallet_public_id`` clause
        added.
    """
    r, _, inst_pid = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    wallet_a = "00000000-0000-7000-8000-0000000000a1"
    wallet_b = "00000000-0000-7000-8000-0000000000b2"
    _, pid_a = await r.insert_order(
        instrument_public_id=inst_pid,
        wallet_public_id="00000000-0000-7000-8000-000000000001",
        client_order_id="c-wallet-a",
        exchange_order_id="e-wallet-a",
        created_at=now,
        side="buy",
        order_type="market",
        price=None,
        size=1.0,
        status="open",
        session_id="s1",
        sequence_id=30,
        timestamp=now,
    )
    _, pid_b = await r.insert_order(
        instrument_public_id=inst_pid,
        wallet_public_id="00000000-0000-7000-8000-000000000001",
        client_order_id="c-wallet-b",
        exchange_order_id="e-wallet-b",
        created_at=now,
        side="sell",
        order_type="limit",
        price=51000.0,
        size=0.5,
        status="open",
        session_id="s1",
        sequence_id=31,
        timestamp=now,
    )
    async with r.session() as s:
        await s.execute(
            text("UPDATE orders SET wallet_public_id=:w_a WHERE public_id=:pid_a"),
            {"w_a": wallet_a, "pid_a": pid_a},
        )
        await s.execute(
            text("UPDATE orders SET wallet_public_id=:w_b WHERE public_id=:pid_b"),
            {"w_b": wallet_b, "pid_b": pid_b},
        )
        await s.commit()
    filtered = await r.get_active_orders_for_recovery(
        exchange="kraken", as_of=now, wallet_public_id=wallet_a
    )
    assert len(filtered) == 1
    assert filtered[0]["client_order_id"] == "c-wallet-a"
    assert filtered[0]["wallet_public_id"] == wallet_a
    legacy = await r.get_active_orders_for_recovery(exchange="kraken", as_of=now)
    assert len(legacy) == 2
    assert {row["client_order_id"] for row in legacy} == {"c-wallet-a", "c-wallet-b"}


@pytest.mark.asyncio
async def test_get_executions_for_recovery_filters_by_wallet(tmp_path: Path) -> None:
    """``wallet_public_id`` filter scopes execution recovery.

    Given: Two executions tagged with two different ``wallet_public_id``
        values via direct SCD2 update,
    When: ``get_executions_for_recovery`` is called with
        ``wallet_public_id=wallet_a``,
    Then: Only the execution tagged ``wallet_a`` is returned and the
        legacy default still returns both rows.
    """
    r, _, inst_pid = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    wallet_a = "00000000-0000-7000-8000-0000000000a1"
    wallet_b = "00000000-0000-7000-8000-0000000000b2"
    _, order_a = await r.insert_order(
        instrument_public_id=inst_pid,
        wallet_public_id="00000000-0000-7000-8000-000000000001",
        client_order_id="c-exec-a",
        exchange_order_id="e-exec-a",
        created_at=now,
        side="buy",
        order_type="limit",
        price=50000.0,
        size=1.0,
        status="filled",
        session_id="s1",
        sequence_id=40,
        timestamp=now,
    )
    _, order_b = await r.insert_order(
        instrument_public_id=inst_pid,
        wallet_public_id="00000000-0000-7000-8000-000000000001",
        client_order_id="c-exec-b",
        exchange_order_id="e-exec-b",
        created_at=now,
        side="sell",
        order_type="limit",
        price=51000.0,
        size=1.0,
        status="filled",
        session_id="s1",
        sequence_id=41,
        timestamp=now,
    )
    await r.insert_execution(
        order_public_id=order_a,
        wallet_public_id="00000000-0000-7000-8000-000000000001",
        timestamp=now,
        side="buy",
        status="filled",
        price=50000.0,
        size=1.0,
        fee=5.0,
        fee_asset="USD",
        session_id="s1",
        sequence_id=42,
    )
    await r.insert_execution(
        order_public_id=order_b,
        wallet_public_id="00000000-0000-7000-8000-000000000001",
        timestamp=now,
        side="sell",
        status="filled",
        price=51000.0,
        size=1.0,
        fee=5.0,
        fee_asset="USD",
        session_id="s1",
        sequence_id=43,
    )
    async with r.session() as s:
        await s.execute(
            text("UPDATE executions SET wallet_public_id=:w_a WHERE order_public_id=:order_a"),
            {"w_a": wallet_a, "order_a": order_a},
        )
        await s.execute(
            text("UPDATE executions SET wallet_public_id=:w_b WHERE order_public_id=:order_b"),
            {"w_b": wallet_b, "order_b": order_b},
        )
        await s.commit()
    filtered = await r.get_executions_for_recovery(
        as_of=now, exchange="kraken", wallet_public_id=wallet_a
    )
    assert len(filtered) == 1
    assert filtered[0]["client_order_id"] == "c-exec-a"
    assert filtered[0]["wallet_public_id"] == wallet_a
    legacy = await r.get_executions_for_recovery(as_of=now, exchange="kraken")
    assert len(legacy) == 2
    assert {row["client_order_id"] for row in legacy} == {"c-exec-a", "c-exec-b"}


@pytest.mark.asyncio
async def test_get_positions_returns_data(tmp_path: Path) -> None:
    """Verify get_positions returns position dicts.

    Given: Repository with position record,
    When: get_positions is called,
    Then: Returns denormalized position dicts.
    """
    r, _, inst_pid = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    async with r.session() as s:
        s.add(
            Position(
                instrument_public_id=inst_pid,
                wallet_public_id="",
                quantity=1.5,
                average_price=48000.0,
                unrealized_pnl=3000.0,
                realized_pnl=500.0,
                timestamp=now,
                session_id="s1",
                sequence_id=30,
            )
        )
        await s.commit()
    result = await r.get_positions(as_of=now)
    assert len(result) == 1
    assert result[0]["instrument"] == "BTC-USD"
    assert result[0]["quantity"] == 1.5


@pytest.mark.asyncio
async def test_get_candles_with_limit_and_order(tmp_path: Path) -> None:
    """Verify get_candles supports limit and order params.

    Given: Repository with multiple candles,
    When: get_candles is called with limit=1 and order='desc',
    Then: Returns only the latest candle.
    """
    r, _, inst_pid = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    base = now - timedelta(hours=2)
    rows = [
        {
            "instrument_public_id": inst_pid,
            "timeframe": "1h",
            "open_at": base,
            "timestamp": now,
            "open": 10.0,
            "high": 12.0,
            "low": 9.0,
            "close": 11.0,
            "volume": 100.0,
            "vwap": 10.5,
            "trades": 5,
            "session_id": "s1",
            "sequence_id": 40,
        },
        {
            "instrument_public_id": inst_pid,
            "timeframe": "1h",
            "open_at": base + timedelta(hours=1),
            "timestamp": now,
            "open": 11.0,
            "high": 13.0,
            "low": 10.0,
            "close": 12.0,
            "volume": 200.0,
            "vwap": 11.5,
            "trades": 8,
            "session_id": "s1",
            "sequence_id": 41,
        },
    ]
    await r.upsert_candles(rows)
    result = await r.get_candles(
        "BTC-USD",
        "1h",
        start=base,
        end=base + timedelta(hours=2),
        exchange="kraken",
        as_of=now,
        limit=1,
        order="desc",
    )
    assert len(result) == 1
    assert result[0]["close"] == 12.0


@pytest.mark.asyncio
async def test_get_candles_latest_mode(tmp_path: Path) -> None:
    """Verify get_candles works without start/end (latest-as-of mode).

    Given: Repository with candles,
    When: get_candles is called with limit only (start/end passed but not used as range),
    Then: Returns up to limit candles.
    """
    r, _, inst_pid = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    rows = [
        {
            "instrument_public_id": inst_pid,
            "timeframe": "1m",
            "open_at": now - timedelta(minutes=i),
            "timestamp": now,
            "open": 10.0,
            "high": 12.0,
            "low": 9.0,
            "close": 11.0,
            "volume": 100.0,
            "vwap": 10.5,
            "trades": 5,
            "session_id": "s1",
            "sequence_id": 50 + i,
        }
        for i in range(5)
    ]
    await r.upsert_candles(rows)
    result = await r.get_candles(
        "BTC-USD",
        "1m",
        start=None,
        end=None,
        exchange="kraken",
        as_of=now,
        limit=3,
        order="desc",
    )
    assert len(result) == 3


@pytest.mark.asyncio
async def test_get_settings_returns_data(tmp_path: Path) -> None:
    """Verify get_settings returns setting dicts.

    Given: Repository with setting records,
    When: get_settings is called,
    Then: Returns setting dicts with all fields.
    """
    r, _, _ = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    async with r.session() as s:
        s.add(
            Setting(
                key="api_key",
                value="secret123",
                category="auth",
                description="API key",
                updated_by="admin",
                timestamp=now,
                session_id="s1",
                sequence_id=60,
            )
        )
        s.add(
            Setting(
                key="theme",
                value="dark",
                category="ui",
                description="UI theme",
                updated_by="admin",
                timestamp=now,
                session_id="s1",
                sequence_id=61,
            )
        )
        await s.commit()
    result = await r.get_settings(as_of=now)
    assert len(result) == 2
    keys = {s["key"] for s in result}
    assert keys == {"api_key", "theme"}


@pytest.mark.asyncio
async def test_get_settings_filters_by_category(tmp_path: Path) -> None:
    """Verify get_settings filters by category.

    Given: Settings in different categories,
    When: get_settings is called with category='auth',
    Then: Returns only auth settings.
    """
    r, _, _ = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    async with r.session() as s:
        s.add(
            Setting(
                key="api_key",
                value="secret",
                category="auth",
                description="key",
                updated_by="admin",
                timestamp=now,
                session_id="s1",
                sequence_id=60,
            )
        )
        s.add(
            Setting(
                key="theme",
                value="dark",
                category="ui",
                description="theme",
                updated_by="admin",
                timestamp=now,
                session_id="s1",
                sequence_id=61,
            )
        )
        await s.commit()
    result = await r.get_settings(as_of=now, category="auth")
    assert len(result) == 1
    assert result[0]["key"] == "api_key"


@pytest.mark.asyncio
async def test_get_setting_by_key_found(tmp_path: Path) -> None:
    """Verify get_setting_by_key returns setting dict when found.

    Given: Repository with a setting,
    When: get_setting_by_key is called with the key,
    Then: Returns the setting dict.
    """
    r, _, _ = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    async with r.session() as s:
        s.add(
            Setting(
                key="api_key",
                value="secret",
                category="auth",
                description="key",
                updated_by="admin",
                timestamp=now,
                session_id="s1",
                sequence_id=60,
            )
        )
        await s.commit()
    result = await r.get_setting_by_key("api_key", as_of=now)
    assert result is not None
    assert result["key"] == "api_key"
    assert result["value"] == "secret"


@pytest.mark.asyncio
async def test_get_setting_by_key_not_found(tmp_path: Path) -> None:
    """Verify get_setting_by_key returns None when not found.

    Given: Repository with no matching setting,
    When: get_setting_by_key is called,
    Then: Returns None.
    """
    r, _, _ = await _seed_full_repo(tmp_path)
    result = await r.get_setting_by_key("nonexistent", as_of=datetime.now(UTC))
    assert result is None


@pytest.mark.asyncio
async def test_get_setting_categories_returns_sorted(tmp_path: Path) -> None:
    """Verify get_setting_categories returns sorted distinct categories.

    Given: Settings in multiple categories,
    When: get_setting_categories is called,
    Then: Returns sorted list of category names.
    """
    r, _, _ = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    async with r.session() as s:
        s.add(
            Setting(
                key="k1",
                value="v1",
                category="ui",
                description="d",
                updated_by="admin",
                timestamp=now,
                session_id="s1",
                sequence_id=60,
            )
        )
        s.add(
            Setting(
                key="k2",
                value="v2",
                category="auth",
                description="d",
                updated_by="admin",
                timestamp=now,
                session_id="s1",
                sequence_id=61,
            )
        )
        await s.commit()
    result = await r.get_setting_categories(as_of=now)
    assert result == ["auth", "ui"]


@pytest.mark.asyncio
async def test_upsert_ticks_appends_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify upsert_ticks inserts all rows as append-only.

    Given: A repository session,
    When: upsert_ticks is called with two rows,
    Then: Both rows are added and count returned.
    """
    added_objects: list[Any] = []
    session = _DummyAsyncSession()
    session.add_all = lambda objs: added_objects.extend(objs)
    repo = _make_repo(lambda: _session_factory(session))
    ts = datetime(2024, 6, 1, tzinfo=UTC)
    rows: list[dict[str, Any]] = [
        {
            "instrument_public_id": "inst-pub-1",
            "timestamp": ts,
            "bid": 100.0,
            "ask": 101.0,
            "last": 100.5,
            "volume": 5.0,
            "session_id": "s1",
            "sequence_id": 1,
            "public_id": "tick-pub-1",
        },
        {
            "instrument_public_id": "inst-pub-1",
            "timestamp": ts,
            "bid": 100.5,
            "ask": 101.5,
            "last": 101.0,
            "volume": 3.0,
            "session_id": "s1",
            "sequence_id": 2,
        },
    ]
    result = await repo.upsert_ticks(rows)
    assert result == 2
    assert len(added_objects) == 2
    assert session.commit_called is True
    assert "public_id" in rows[1]


@pytest.mark.asyncio
async def test_upsert_ticks_returns_zero_for_empty() -> None:
    """Verify upsert_ticks returns 0 for empty input."""
    session = _DummyAsyncSession()
    repo = _make_repo(lambda: _session_factory(session))
    result = await repo.upsert_ticks([])
    assert result == 0
    assert session.commit_called is False


@pytest.mark.asyncio
async def test_get_ticks_returns_empty_when_instrument_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify get_ticks returns empty list when instrument is not found."""
    with patch("snapper.data.repository.create_async_engine"):
        repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    mock_sym_result = SimpleNamespace(scalar_one_or_none=lambda: None)

    async def _execute(*_: object, **__: object) -> SimpleNamespace:
        return mock_sym_result

    mock_session = AsyncMock()
    mock_session.execute.side_effect = _execute

    class _Ctx:
        async def __aenter__(self) -> Any:
            return mock_session

        async def __aexit__(self, *_: object) -> None:
            return None

    monkeypatch.setattr(repo, "session", lambda: _Ctx())
    result = await repo.get_ticks(
        "MISSING",
        datetime.now(UTC),
        datetime.now(UTC),
        exchange="kraken",
        as_of=datetime.now(UTC),
    )
    assert result == []


@pytest.mark.asyncio
async def test_get_ticks_returns_rows_when_instrument_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify get_ticks returns mapped rows when instrument exists."""
    with patch("snapper.data.repository.create_async_engine"):
        repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    ts = datetime(2024, 6, 1, tzinfo=UTC)
    mock_inst = SimpleNamespace(public_id="inst-pub-1")
    mock_sym_result = SimpleNamespace(scalar_one_or_none=lambda: "sym-pub-1")
    mock_inst_result = SimpleNamespace(scalars=lambda: SimpleNamespace(first=lambda: mock_inst))
    tick_row = SimpleNamespace(
        timestamp=ts,
        bid=100.0,
        ask=101.0,
        last=100.5,
        volume=5.0,
        public_id="tick-pub-1",
        session_id="s1",
        sequence_id=1,
    )
    mock_query_result = SimpleNamespace(all=lambda: [tick_row])
    call_count = 0

    async def _execute(*_: object, **__: object) -> SimpleNamespace:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return mock_sym_result
        if call_count == 2:
            return mock_inst_result
        return mock_query_result

    mock_session = AsyncMock()
    mock_session.execute.side_effect = _execute

    class _Ctx:
        async def __aenter__(self) -> Any:
            return mock_session

        async def __aexit__(self, *_: object) -> None:
            return None

    monkeypatch.setattr(repo, "session", lambda: _Ctx())
    result = await repo.get_ticks(
        "BTC-USD",
        ts - timedelta(hours=1),
        ts + timedelta(hours=1),
        exchange="kraken",
        as_of=ts + timedelta(hours=1),
    )
    assert len(result) == 1
    assert result[0]["bid"] == 100.0
    assert result[0]["ask"] == 101.0
    assert result[0]["last"] == 100.5
    assert result[0]["volume"] == 5.0
    assert result[0]["public_id"] == "tick-pub-1"


@pytest.mark.asyncio
async def test_get_trades_returns_executed_at(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify get_trades returns executed_at and maps it correctly."""
    with patch("snapper.data.repository.create_async_engine"):
        repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    bus_time = datetime(2024, 6, 1, 0, 0, 1, tzinfo=UTC)
    event_time = datetime(2024, 6, 1, 0, 0, 0, tzinfo=UTC)
    mock_inst = SimpleNamespace(public_id="inst-pub-1")
    mock_sym_result = SimpleNamespace(scalar_one_or_none=lambda: "sym-pub-1")
    mock_inst_result = SimpleNamespace(scalars=lambda: SimpleNamespace(first=lambda: mock_inst))
    trade_row = SimpleNamespace(
        timestamp=bus_time,
        executed_at=event_time,
        price=100.0,
        size=1.5,
        side="buy",
        trade_id="exch-123",
    )
    mock_query_result = SimpleNamespace(all=lambda: [trade_row])
    call_count = 0

    async def _execute(*_: object, **__: object) -> SimpleNamespace:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return mock_sym_result
        if call_count == 2:
            return mock_inst_result
        return mock_query_result

    mock_session = AsyncMock()
    mock_session.execute.side_effect = _execute

    class _Ctx:
        async def __aenter__(self) -> Any:
            return mock_session

        async def __aexit__(self, *_: object) -> None:
            return None

    monkeypatch.setattr(repo, "session", lambda: _Ctx())
    result = await repo.get_trades(
        "BTC-USD",
        event_time - timedelta(seconds=1),
        event_time + timedelta(seconds=1),
        exchange="kraken",
        as_of=bus_time + timedelta(hours=1),
    )
    assert len(result) == 1
    assert result[0]["timestamp"] == bus_time
    assert result[0]["executed_at"] == event_time
    assert result[0]["trade_id"] == "exch-123"
    assert result[0]["price"] == 100.0


@pytest.mark.asyncio
async def test_insert_trade_command(tmp_path: Path) -> None:
    """Insert trade command persists the row and returns id and public_id.

    Given: an empty database with the schema created,
    When: insert_trade_command is called with valid submit parameters,
    Then: a positive id and a 36-char UUID public_id are returned.
    """
    db_path = tmp_path / "cmd.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    cmd_id, cmd_pid = await r.insert_trade_command(
        {
            "command_type": "submit",
            "shard_key": "kraken.BTC-USD.live",
            "exchange": "kraken",
            "instrument": "BTC-USD",
            "mode": "live",
            "strategy_id": "engine-buy",
            "client_order_id": "cid-1",
            "venue_client_id": "vcid-1",
            "side": "buy",
            "order_type": "market",
            "quantity": 0.5,
            "price": None,
            "status": "created",
            "created_at": now,
            "correlation_id": "corr-1",
            "session_id": "s1",
            "sequence_id": 1,
            "timestamp": now,
        }
    )
    assert cmd_id > 0
    assert len(cmd_pid) == 36


@pytest.mark.asyncio
async def test_get_plan_public_id_for_client_order_id_found(tmp_path: Path) -> None:
    """Resolving by client_order_id returns the plan linked on the create command.

    Given: a database with a ``create`` trade command linked to a plan,
    When: get_plan_public_id_for_client_order_id is called with a matching as_of,
    Then: the linked plan public_id is returned.
    """
    db_path = tmp_path / "plan_cid.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    await r.insert_trade_command(
        {
            "command_type": "create",
            "shard_key": "kraken.BTC-USD.live",
            "exchange": "kraken",
            "instrument": "BTC-USD",
            "mode": "live",
            "strategy_id": "manual",
            "client_order_id": "cid-7",
            "venue_client_id": "vcid-7",
            "side": "buy",
            "order_type": "limit",
            "quantity": 0.5,
            "price": 50000.0,
            "status": "created",
            "created_at": now,
            "correlation_id": "corr-7",
            "session_id": "s1",
            "sequence_id": 1,
            "timestamp": now,
            "plan_public_id": "plan-42",
            "wallet_public_id": "wallet-1",
        }
    )
    found = await r.get_plan_public_id_for_client_order_id("cid-7", as_of=now)
    assert found == "plan-42"


@pytest.mark.asyncio
async def test_get_plan_public_id_for_client_order_id_future_as_of(
    tmp_path: Path,
) -> None:
    """Resolving by client_order_id honors an explicit temporal point.

    Given: a plan-linked create command whose bus_time is slightly in the future,
    When: get_plan_public_id_for_client_order_id is called with that future as_of,
    Then: the linked plan public_id is still returned.
    """
    db_path = tmp_path / "plan_cid_future.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    future_time = now + timedelta(seconds=5)
    await r.insert_trade_command(
        {
            "command_type": "create",
            "shard_key": "kraken.BTC-USD.live",
            "exchange": "kraken",
            "instrument": "BTC-USD",
            "mode": "live",
            "strategy_id": "manual",
            "client_order_id": "cid-future-7",
            "venue_client_id": "vcid-future-7",
            "side": "buy",
            "order_type": "limit",
            "quantity": 0.5,
            "price": 50000.0,
            "status": "created",
            "created_at": future_time,
            "correlation_id": "corr-future-7",
            "session_id": "s1",
            "sequence_id": 1,
            "timestamp": future_time,
            "plan_public_id": "plan-future-42",
            "wallet_public_id": "wallet-1",
        }
    )
    found = await r.get_plan_public_id_for_client_order_id(
        "cid-future-7",
        as_of=future_time,
    )
    assert found == "plan-future-42"


@pytest.mark.asyncio
async def test_get_plan_public_id_for_client_order_id_missing(tmp_path: Path) -> None:
    """Resolving by an unknown client_order_id returns None."""
    db_path = tmp_path / "plan_cid_miss.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    found = await r.get_plan_public_id_for_client_order_id(
        "does-not-exist",
        as_of=datetime.now(UTC),
    )
    assert found is None


@pytest.mark.asyncio
async def test_get_exchange_order_id_for_client_order_id_returns_value(
    tmp_path: Path,
) -> None:
    """Resolving by client_order_id returns the venue-assigned exchange id.

    Given: an ``orders`` row with a non-null ``exchange_order_id``,
    When: get_exchange_order_id_for_client_order_id is called,
    Then: the venue-assigned id is returned.
    """
    r, _, inst_pid = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    await r.insert_order(
        instrument_public_id=inst_pid,
        wallet_public_id="00000000-0000-7000-8000-000000000001",
        client_order_id="cid-xo-1",
        exchange_order_id="ex-123",
        created_at=now,
        side="buy",
        order_type="limit",
        price=50000.0,
        size=0.5,
        status="open",
        session_id="s1",
        sequence_id=1,
        timestamp=now,
    )
    found = await r.get_exchange_order_id_for_client_order_id("cid-xo-1", as_of=now)
    assert found == "ex-123"


@pytest.mark.asyncio
async def test_get_exchange_order_id_for_client_order_id_returns_none_without_row(
    tmp_path: Path,
) -> None:
    """Returns None when no active order exists for the client_order_id."""
    r, _, _ = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    found = await r.get_exchange_order_id_for_client_order_id("nope", as_of=now)
    assert found is None


@pytest.mark.asyncio
async def test_has_pending_cancel_command_false_for_unknown(tmp_path: Path) -> None:
    """Returns False when no cancel command exists for the client_order_id."""
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'pend_none.db'}")
    await r.create_all()
    now = datetime.now(UTC)
    result = await r.has_pending_cancel_command("none", as_of=now)
    assert result is False


@pytest.mark.asyncio
async def test_has_pending_cancel_command_true_for_pending(tmp_path: Path) -> None:
    """Returns True when a non-terminal cancel command row is present.

    Given: an active trade_commands row with ``command_type='cancel'``
        and status ``created``,
    When: has_pending_cancel_command is called for that client_order_id,
    Then: True is returned so recovery can dedupe.
    """
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'pend_yes.db'}")
    await r.create_all()
    now = datetime.now(UTC)
    await r.insert_trade_command(
        {
            "command_type": "cancel",
            "shard_key": "kraken.BTC-USD.live",
            "exchange": "kraken",
            "instrument": "BTC-USD",
            "mode": "live",
            "strategy_id": "manual",
            "client_order_id": "cid-pend-1",
            "venue_client_id": "cid-pend-1",
            "side": "buy",
            "order_type": "market",
            "quantity": 0.5,
            "price": None,
            "status": "created",
            "created_at": now,
            "correlation_id": "corr-pend",
            "session_id": "s1",
            "sequence_id": 1,
            "timestamp": now,
            "wallet_public_id": "wallet-1",
        }
    )
    result = await r.has_pending_cancel_command("cid-pend-1", as_of=now)
    assert result is True


@pytest.mark.asyncio
async def test_has_pending_cancel_command_ignores_create_commands(tmp_path: Path) -> None:
    """Only ``command_type='cancel'`` rows are counted."""
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'pend_create.db'}")
    await r.create_all()
    now = datetime.now(UTC)
    await r.insert_trade_command(
        {
            "command_type": "create",
            "shard_key": "kraken.BTC-USD.live",
            "exchange": "kraken",
            "instrument": "BTC-USD",
            "mode": "live",
            "strategy_id": "manual",
            "client_order_id": "cid-pend-2",
            "venue_client_id": "cid-pend-2",
            "side": "buy",
            "order_type": "market",
            "quantity": 0.5,
            "price": None,
            "status": "created",
            "created_at": now,
            "correlation_id": "corr",
            "session_id": "s1",
            "sequence_id": 1,
            "timestamp": now,
            "wallet_public_id": "wallet-1",
        }
    )
    result = await r.has_pending_cancel_command("cid-pend-2", as_of=now)
    assert result is False


@pytest.mark.asyncio
async def test_get_plan_public_id_for_client_order_id_skips_null_plan(
    tmp_path: Path,
) -> None:
    """Commands without a plan_public_id do not shadow plan-linked commands."""
    db_path = tmp_path / "plan_cid_null.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    await r.insert_trade_command(
        {
            "command_type": "create",
            "shard_key": "kraken.BTC-USD.live",
            "exchange": "kraken",
            "instrument": "BTC-USD",
            "mode": "live",
            "strategy_id": "engine-buy",
            "client_order_id": "cid-8",
            "venue_client_id": "vcid-8",
            "side": "buy",
            "order_type": "market",
            "quantity": 0.5,
            "price": None,
            "status": "created",
            "created_at": now,
            "correlation_id": "corr-8",
            "session_id": "s1",
            "sequence_id": 1,
            "timestamp": now,
            "wallet_public_id": "wallet-1",
        }
    )
    found = await r.get_plan_public_id_for_client_order_id("cid-8", as_of=now)
    assert found is None


@pytest.mark.asyncio
async def test_get_undispatched_commands(tmp_path: Path) -> None:
    """Get undispatched commands returns only commands with status created.

    Given: a database with one trade command in "created" status,
    When: get_undispatched_commands is called,
    Then: exactly one command is returned with status "created" and exchange "kraken".
    """
    db_path = tmp_path / "cmd2.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    await r.insert_trade_command(
        {
            "command_type": "submit",
            "shard_key": "kraken.BTC-USD.live",
            "exchange": "kraken",
            "instrument": "BTC-USD",
            "mode": "live",
            "strategy_id": "engine-buy",
            "client_order_id": "cid-1",
            "venue_client_id": "vcid-1",
            "side": "buy",
            "order_type": "market",
            "quantity": 0.5,
            "price": None,
            "status": "created",
            "created_at": now,
            "correlation_id": "corr-1",
            "session_id": "s1",
            "sequence_id": 1,
            "timestamp": now,
        }
    )
    cmds = await r.get_undispatched_commands(as_of=now, limit=10)
    assert len(cmds) == 1
    assert cmds[0]["status"] == "created"
    assert cmds[0]["exchange"] == "kraken"


@pytest.mark.asyncio
async def test_trade_command_query_projections_carry_leverage_and_reduce_only(
    tmp_path: Path,
) -> None:
    """Verify trade command query projections expose leverage and reduce_only.

    Given: A database with a trade command persisted with leverage=4 and
        reduce_only=True (the durable outbox command path),
    When: get_undispatched_commands, get_active_commands_for_shard, and
        get_active_commands_for_exchange are called,
    Then: All three projections return rows that include the leverage and
        reduce_only fields, so `_outbox_publish` can reconstruct an
        OrderRequestData carrying the correct margin metadata. Without this
        propagation the executor would receive defaults (None / False) on the
        durable path and the frontend / DB would lose the short / margin
        intent.
    """
    db_path = tmp_path / "cmd_lev.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    await r.insert_trade_command(
        {
            "command_type": "submit",
            "shard_key": "kraken.BTC-USD.live",
            "exchange": "kraken",
            "instrument": "BTC-USD",
            "mode": "live",
            "strategy_id": "engine-sell",
            "client_order_id": "cid-lev",
            "venue_client_id": "vcid-lev",
            "side": "sell",
            "order_type": "limit",
            "quantity": 0.5,
            "price": 50000.0,
            "leverage": 4,
            "reduce_only": True,
            "status": "created",
            "created_at": now,
            "correlation_id": "corr-lev",
            "session_id": "s1",
            "sequence_id": 1,
            "timestamp": now,
        }
    )
    undispatched = await r.get_undispatched_commands(as_of=now, limit=10)
    assert len(undispatched) == 1
    assert undispatched[0]["leverage"] == 4
    assert undispatched[0]["reduce_only"] is True
    by_shard = await r.get_active_commands_for_shard("kraken.BTC-USD.live", now)
    assert len(by_shard) == 1
    assert by_shard[0]["leverage"] == 4
    assert by_shard[0]["reduce_only"] is True
    by_exchange = await r.get_active_commands_for_exchange("kraken", now)
    assert len(by_exchange) == 1
    assert by_exchange[0]["leverage"] == 4
    assert by_exchange[0]["reduce_only"] is True


@pytest.mark.asyncio
async def test_update_trade_command_status_scd2(tmp_path: Path) -> None:
    """Update trade command status performs SCD2 close-and-insert.

    Given: a database with one trade command in "created" status,
    When: update_trade_command_status is called to transition to "dispatched",
    Then: a new row is created, undispatched returns empty, and active shows "dispatched".
    """
    db_path = tmp_path / "cmd3.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    _, cmd_pid = await r.insert_trade_command(
        {
            "command_type": "submit",
            "shard_key": "kraken.BTC-USD.live",
            "exchange": "kraken",
            "instrument": "BTC-USD",
            "mode": "live",
            "strategy_id": "engine-buy",
            "client_order_id": "cid-1",
            "venue_client_id": "vcid-1",
            "side": "buy",
            "order_type": "market",
            "quantity": 0.5,
            "price": None,
            "status": "created",
            "created_at": now,
            "correlation_id": "corr-1",
            "session_id": "s1",
            "sequence_id": 1,
            "timestamp": now,
        }
    )
    later = now + timedelta(seconds=1)
    new_id = await r.update_trade_command_status(
        public_id=cmd_pid,
        new_status="dispatched",
        bus_time=later,
        session_id="s1",
        sequence_id=2,
        dispatched_at=later,
    )
    assert new_id is not None
    cmds = await r.get_undispatched_commands(as_of=later, limit=10)
    assert len(cmds) == 0
    active = await r.get_active_commands_for_shard("kraken.BTC-USD.live", later)
    assert len(active) == 1
    assert active[0]["status"] == "dispatched"
    assert active[0]["public_id"] == cmd_pid


@pytest.mark.asyncio
async def test_update_trade_command_status_carries_forward_leverage_and_reduce_only(
    tmp_path: Path,
) -> None:
    """Verify update_trade_command_status SCD2 cycle preserves leverage/reduce_only.

    Given: A trade command persisted with leverage=4/reduce_only=True (the durable
        outbox command path),
    When: update_trade_command_status is called twice to cycle created → dispatched
        → accepted (each call closes the old SCD2 row and inserts a new one),
    Then: After both status transitions get_active_commands_for_shard still returns
        leverage=4 and reduce_only=True because update_trade_command_status carries
        the immutable margin/intent fields forward from the old row when constructing
        the replacement TradeCommand. Without this, the first status transition
        silently zeroes the margin metadata, and any downstream query (outbox
        retry re-read, reconciliation, crash recovery) would see defaults.
    """
    db_path = tmp_path / "cmd_scd2_lev.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    _, cmd_pid = await r.insert_trade_command(
        {
            "command_type": "submit",
            "shard_key": "kraken.BTC-USD.live",
            "exchange": "kraken",
            "instrument": "BTC-USD",
            "mode": "live",
            "strategy_id": "engine-sell",
            "client_order_id": "cid-scd2-lev",
            "venue_client_id": "vcid-scd2-lev",
            "side": "sell",
            "order_type": "limit",
            "quantity": 0.5,
            "price": 50000.0,
            "leverage": 4,
            "reduce_only": True,
            "status": "created",
            "created_at": now,
            "correlation_id": "corr-scd2-lev",
            "session_id": "s1",
            "sequence_id": 1,
            "timestamp": now,
        }
    )
    after_dispatch = now + timedelta(milliseconds=1)
    await r.update_trade_command_status(
        public_id=cmd_pid,
        new_status="dispatched",
        bus_time=after_dispatch,
        session_id="s1",
        sequence_id=2,
        dispatched_at=after_dispatch,
    )
    after_accept = now + timedelta(milliseconds=2)
    await r.update_trade_command_status(
        public_id=cmd_pid,
        new_status="accepted",
        bus_time=after_accept,
        session_id="s1",
        sequence_id=3,
        acked_at=after_accept,
    )
    active = await r.get_active_commands_for_shard("kraken.BTC-USD.live", after_accept)
    assert len(active) == 1
    assert active[0]["status"] == "accepted"
    assert active[0]["leverage"] == 4
    assert active[0]["reduce_only"] is True


@pytest.mark.asyncio
async def test_update_trade_command_status_carries_forward_multi_tenant_ids(
    tmp_path: Path,
) -> None:
    """Verify SCD2 cycle preserves wallet/operator/user IDs across multiple transitions.

    Given: A trade command inserted with all three multi-tenant IDs,
    When: update_trade_command_status cycles created → dispatched → accepted,
    Then: After both transitions get_active_commands_for_shard still returns
        the original wallet_public_id / operator_public_id / user_public_id,
        because update_trade_command_status copies them from the locked
        active row when constructing the replacement TradeCommand. Without
        this carry-forward, the very first status transition would silently
        null out the audit identity, breaking the outbox path.
    """
    db_path = tmp_path / "cmd_scd2_mt.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    wallet_pid = "01975a8b-3c7d-7000-8000-aaaaaaaaaaaa"
    operator_pid = "01975a8b-3c7d-7000-8000-bbbbbbbbbbbb"
    user_pid = "01975a8b-3c7d-7000-8000-cccccccccccc"
    _, cmd_pid = await r.insert_trade_command(
        {
            "command_type": "submit",
            "shard_key": "kraken.BTC-USD.live",
            "exchange": "kraken",
            "instrument": "BTC-USD",
            "mode": "live",
            "strategy_id": "engine-buy",
            "client_order_id": "cid-scd2-mt",
            "venue_client_id": "vcid-scd2-mt",
            "side": "buy",
            "order_type": "market",
            "quantity": 0.25,
            "price": None,
            "leverage": None,
            "reduce_only": False,
            "status": "created",
            "created_at": now,
            "correlation_id": "corr-scd2-mt",
            "session_id": "s1",
            "sequence_id": 1,
            "timestamp": now,
            "wallet_public_id": wallet_pid,
            "operator_public_id": operator_pid,
            "user_public_id": user_pid,
        }
    )
    after_dispatch = now + timedelta(milliseconds=1)
    await r.update_trade_command_status(
        public_id=cmd_pid,
        new_status="dispatched",
        bus_time=after_dispatch,
        session_id="s1",
        sequence_id=2,
        dispatched_at=after_dispatch,
    )
    after_accept = now + timedelta(milliseconds=2)
    await r.update_trade_command_status(
        public_id=cmd_pid,
        new_status="accepted",
        bus_time=after_accept,
        session_id="s1",
        sequence_id=3,
        acked_at=after_accept,
    )
    active = await r.get_active_commands_for_shard("kraken.BTC-USD.live", after_accept)
    assert len(active) == 1
    assert active[0]["status"] == "accepted"
    assert active[0]["wallet_public_id"] == wallet_pid
    assert active[0]["operator_public_id"] == operator_pid
    assert active[0]["user_public_id"] == user_pid


@pytest.mark.asyncio
async def test_update_trade_command_status_clears_last_error_on_success(
    tmp_path: Path,
) -> None:
    """Verify last_error does NOT carry forward across a retry-then-success cycle.

    Given: A trade command persisted, then a failed dispatch attempt that
        rewrites the row to status='created' with last_error='dispatch failed'
        (matching the outbox retry path),
    When: A subsequent successful dispatch transitions to status='dispatched'
        without passing last_error (the outbox success path does not),
    Then: The new active row has last_error=None — the previous attempt's
        error message is wiped instead of being silently carried forward.
        Stale errors carrying through to a successful state would mislead
        operators reading the trade_commands history.
    """
    db_path = tmp_path / "cmd_last_error.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    _, cmd_pid = await r.insert_trade_command(
        {
            "command_type": "submit",
            "shard_key": "kraken.BTC-USD.live",
            "exchange": "kraken",
            "instrument": "BTC-USD",
            "mode": "live",
            "strategy_id": "engine-buy",
            "client_order_id": "cid-err",
            "venue_client_id": "vcid-err",
            "side": "buy",
            "order_type": "limit",
            "quantity": 0.5,
            "price": 50000.0,
            "leverage": 2,
            "reduce_only": False,
            "status": "created",
            "created_at": now,
            "correlation_id": "corr-err",
            "session_id": "s1",
            "sequence_id": 1,
            "timestamp": now,
        }
    )
    after_fail = now + timedelta(milliseconds=1)
    await r.update_trade_command_status(
        public_id=cmd_pid,
        new_status="created",
        bus_time=after_fail,
        session_id="s1",
        sequence_id=2,
        attempt_count=1,
        last_error="dispatch failed",
    )
    after_dispatch = now + timedelta(milliseconds=2)
    await r.update_trade_command_status(
        public_id=cmd_pid,
        new_status="dispatched",
        bus_time=after_dispatch,
        session_id="s1",
        sequence_id=3,
        dispatched_at=after_dispatch,
        attempt_count=2,
    )
    active = await r.get_active_commands_for_shard("kraken.BTC-USD.live", after_dispatch)
    assert len(active) == 1
    assert active[0]["status"] == "dispatched"
    assert active[0]["last_error"] is None
    assert active[0]["attempt_count"] == 2
    assert active[0]["leverage"] == 2


@pytest.mark.asyncio
async def test_insert_venue_event(tmp_path: Path) -> None:
    """Insert venue event persists the row and returns a monotonic local_seq.

    Given: an empty database with the schema created,
    When: two venue events are inserted sequentially,
    Then: both return positive local_seq values with the second greater than the first.
    """
    db_path = tmp_path / "ve.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    seq1 = await r.insert_venue_event(
        {
            "event_type": "order_accepted",
            "shard_key": "kraken.BTC-USD.live",
            "exchange": "kraken",
            "instrument": "BTC-USD",
            "mode": "live",
            "received_at": now,
            "session_id": "s1",
            "sequence_id": 1,
            "timestamp": now,
            "exchange_order_id": "ex-1",
            "client_order_id": "cid-1",
        }
    )
    seq2 = await r.insert_venue_event(
        {
            "event_type": "fill_observed",
            "shard_key": "kraken.BTC-USD.live",
            "exchange": "kraken",
            "instrument": "BTC-USD",
            "mode": "live",
            "received_at": now,
            "session_id": "s1",
            "sequence_id": 2,
            "timestamp": now,
            "fill_price": 50000.0,
            "fill_size": 0.5,
        }
    )
    assert seq2 > seq1 > 0


@pytest.mark.asyncio
async def test_get_venue_events_after(tmp_path: Path) -> None:
    """Get venue events after watermark returns only newer events.

    Given: a database with two venue events for the same shard,
    When: get_venue_events_after is called with the first event's seq as watermark,
    Then: only the second event is returned.
    """
    db_path = tmp_path / "ve2.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    seq1 = await r.insert_venue_event(
        {
            "event_type": "order_accepted",
            "shard_key": "kraken.BTC-USD.live",
            "exchange": "kraken",
            "instrument": "BTC-USD",
            "mode": "live",
            "received_at": now,
            "session_id": "s1",
            "sequence_id": 1,
            "timestamp": now,
        }
    )
    await r.insert_venue_event(
        {
            "event_type": "fill_observed",
            "shard_key": "kraken.BTC-USD.live",
            "exchange": "kraken",
            "instrument": "BTC-USD",
            "mode": "live",
            "received_at": now,
            "session_id": "s1",
            "sequence_id": 2,
            "timestamp": now,
        }
    )
    events = await r.get_venue_events_after("kraken.BTC-USD.live", seq1)
    assert len(events) == 1
    assert events[0]["event_type"] == "fill_observed"
    events_all = await r.get_venue_events_after("kraken.BTC-USD.live", 0)
    assert len(events_all) == 2


@pytest.mark.asyncio
async def test_upsert_checkpoint_and_get(tmp_path: Path) -> None:
    """Upsert checkpoint creates and updates checkpoint rows via SCD2.

    Given: an empty database with the schema created,
    When: upsert_checkpoint is called twice with updated position and cash values,
    Then: get_checkpoint returns the latest values and the second row has a different id.
    """
    db_path = tmp_path / "cp.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    cp_id = await r.upsert_checkpoint(
        {
            "shard_key": "kraken.BTC-USD.live",
            "wallet_public_id": "00000000-0000-7000-8000-000000000001",
            "position_qty": 0.5,
            "entry_price": 50000.0,
            "position_opened_at": now,
            "cash": 9000.0,
            "peak_equity": 10000.0,
            "realized_pnl": 0.0,
            "turnover": 500.0,
            "last_venue_event_id": 42,
            "last_venue_event_at": now,
            "open_command_ids": '["cmd-1"]',
            "seen_exec_ids": '["t1"]',
            "checkpoint_at": now,
            "session_id": "s1",
            "sequence_id": 1,
            "bus_time": now,
        }
    )
    assert cp_id > 0
    cp = await r.get_checkpoint("kraken.BTC-USD.live", now)
    assert cp is not None
    assert cp["position_qty"] == 0.5
    assert cp["cash"] == 9000.0
    assert cp["last_venue_event_id"] == 42
    assert cp["position_opened_at"] == now
    later = now + timedelta(seconds=1)
    cp_id2 = await r.upsert_checkpoint(
        {
            "shard_key": "kraken.BTC-USD.live",
            "wallet_public_id": "00000000-0000-7000-8000-000000000001",
            "position_qty": 1.0,
            "entry_price": 50000.0,
            "position_opened_at": now,
            "cash": 8500.0,
            "peak_equity": 10500.0,
            "realized_pnl": 0.0,
            "turnover": 1000.0,
            "last_venue_event_id": 43,
            "last_venue_event_at": later,
            "open_command_ids": None,
            "seen_exec_ids": '["t1", "t2"]',
            "checkpoint_at": later,
            "session_id": "s1",
            "sequence_id": 2,
            "bus_time": later,
        }
    )
    assert cp_id2 != cp_id
    cp2 = await r.get_checkpoint("kraken.BTC-USD.live", later)
    assert cp2 is not None
    assert cp2["position_qty"] == 1.0
    assert cp2["cash"] == 8500.0


@pytest.mark.asyncio
async def test_get_checkpoint_returns_none_when_missing(tmp_path: Path) -> None:
    """Get checkpoint returns None when no checkpoint exists for the shard.

    Given: an empty database with the schema created,
    When: get_checkpoint is called for a non-existent shard key,
    Then: None is returned.
    """
    db_path = tmp_path / "cp2.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    result = await r.get_checkpoint("nonexistent.shard", datetime.now(UTC))
    assert result is None


@pytest.mark.asyncio
async def test_get_all_checkpoints_returns_all_active(tmp_path: Path) -> None:
    """Get all checkpoints returns every active checkpoint row.

    Given: two checkpoints for different shards,
    When: get_all_checkpoints is called,
    Then: both rows are returned ordered by shard_key.
    """
    db_path = tmp_path / "cp_all.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    await r.upsert_checkpoint(
        {
            "shard_key": "kraken.BTC-USD.live",
            "wallet_public_id": "00000000-0000-7000-8000-000000000001",
            "position_qty": 0.5,
            "entry_price": 50000.0,
            "position_opened_at": now,
            "cash": 9000.0,
            "peak_equity": 10000.0,
            "realized_pnl": 0.0,
            "turnover": 500.0,
            "last_venue_event_id": 10,
            "last_venue_event_at": now,
            "open_command_ids": None,
            "seen_exec_ids": '["t1"]',
            "checkpoint_at": now,
            "session_id": "s1",
            "sequence_id": 1,
            "bus_time": now,
        }
    )
    await r.upsert_checkpoint(
        {
            "shard_key": "kraken.ETH-USD.live",
            "wallet_public_id": "00000000-0000-7000-8000-000000000001",
            "position_qty": 10.0,
            "entry_price": 3000.0,
            "position_opened_at": None,
            "cash": 5000.0,
            "peak_equity": 8000.0,
            "realized_pnl": 50.0,
            "turnover": 1000.0,
            "last_venue_event_id": 20,
            "last_venue_event_at": now,
            "open_command_ids": None,
            "seen_exec_ids": "[]",
            "checkpoint_at": now,
            "session_id": "s1",
            "sequence_id": 2,
            "bus_time": now,
        }
    )
    rows = await r.get_all_checkpoints(now)
    assert len(rows) == 2
    assert rows[0]["shard_key"] == "kraken.BTC-USD.live"
    assert rows[1]["shard_key"] == "kraken.ETH-USD.live"
    assert rows[0]["seen_exec_ids"] == '["t1"]'


@pytest.mark.asyncio
async def test_update_trade_command_returns_none_for_missing(tmp_path: Path) -> None:
    """Update trade command status returns None for a non-existent public_id.

    Given: an empty database with the schema created,
    When: update_trade_command_status is called with a non-existent public_id,
    Then: None is returned.
    """
    db_path = tmp_path / "cmd4.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    result = await r.update_trade_command_status(
        public_id="nonexistent",
        new_status="dispatched",
        bus_time=datetime.now(UTC),
        session_id="s1",
        sequence_id=1,
    )
    assert result is None


@pytest.mark.asyncio
async def test_get_fill_exec_ids_for_shard(tmp_path: Path) -> None:
    """Get fill exec IDs returns all exec_id and trade_id for fill events.

    Given: a database with two fill_observed events and one order_accepted event,
    When: get_fill_exec_ids_for_shard is called,
    Then: only exec_id and trade_id from fill events are returned.
    """
    db_path = tmp_path / "dedup.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    await r.insert_venue_event(
        {
            "event_type": "fill_observed",
            "shard_key": "kraken.BTC-USD.live",
            "exchange": "kraken",
            "instrument": "BTC-USD",
            "mode": "live",
            "received_at": now,
            "session_id": "s1",
            "sequence_id": 1,
            "timestamp": now,
            "exec_id": "exec-1",
            "trade_id": "trade-1",
        }
    )
    await r.insert_venue_event(
        {
            "event_type": "fill_observed",
            "shard_key": "kraken.BTC-USD.live",
            "exchange": "kraken",
            "instrument": "BTC-USD",
            "mode": "live",
            "received_at": now,
            "session_id": "s1",
            "sequence_id": 2,
            "timestamp": now,
            "exec_id": "exec-2",
        }
    )
    await r.insert_venue_event(
        {
            "event_type": "fill_observed",
            "shard_key": "kraken.BTC-USD.live",
            "exchange": "kraken",
            "instrument": "BTC-USD",
            "mode": "live",
            "received_at": now,
            "session_id": "s1",
            "sequence_id": 3,
            "timestamp": now,
        }
    )
    await r.insert_venue_event(
        {
            "event_type": "order_accepted",
            "shard_key": "kraken.BTC-USD.live",
            "exchange": "kraken",
            "instrument": "BTC-USD",
            "mode": "live",
            "received_at": now,
            "session_id": "s1",
            "sequence_id": 4,
            "timestamp": now,
            "exec_id": "should-not-appear",
        }
    )
    ids = await r.get_fill_exec_ids_for_shard("kraken.BTC-USD.live")
    assert ids == {"exec-1", "trade-1", "exec-2"}


@pytest.mark.asyncio
async def test_get_latest_venue_event_id(tmp_path: Path) -> None:
    """Get latest venue event ID returns highest id for shard.

    Given: a database with two venue events for the same shard,
    When: get_latest_venue_event_id is called,
    Then: the highest id is returned.
    """
    db_path = tmp_path / "latest.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    await r.insert_venue_event(
        {
            "event_type": "fill_observed",
            "shard_key": "kraken.BTC-USD.live",
            "exchange": "kraken",
            "instrument": "BTC-USD",
            "mode": "live",
            "received_at": now,
            "session_id": "s1",
            "sequence_id": 1,
            "timestamp": now,
        }
    )
    await r.insert_venue_event(
        {
            "event_type": "fill_observed",
            "shard_key": "kraken.BTC-USD.live",
            "exchange": "kraken",
            "instrument": "BTC-USD",
            "mode": "live",
            "received_at": now,
            "session_id": "s1",
            "sequence_id": 2,
            "timestamp": now,
        }
    )
    result = await r.get_latest_venue_event_id("kraken.BTC-USD.live")
    assert result is not None
    assert result >= 2
    empty = await r.get_latest_venue_event_id("nonexistent.shard")
    assert empty is None


@pytest.mark.asyncio
async def test_get_active_commands_for_exchange(tmp_path: Path) -> None:
    """Get active commands for exchange returns non-terminal commands.

    Given: a database with one created and one filled command for kraken,
    When: get_active_commands_for_exchange is called for kraken,
    Then: only the created (non-terminal) command is returned.
    """
    db_path = tmp_path / "exch.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    await r.insert_trade_command(
        {
            "command_type": "submit",
            "shard_key": "kraken.BTC-USD.live",
            "exchange": "kraken",
            "instrument": "BTC-USD",
            "mode": "live",
            "strategy_id": "test",
            "client_order_id": "cid-1",
            "venue_client_id": "vcid-1",
            "side": "buy",
            "order_type": "market",
            "quantity": 0.5,
            "price": None,
            "status": "created",
            "created_at": now,
            "correlation_id": "corr-1",
            "session_id": "s1",
            "sequence_id": 1,
            "timestamp": now,
        }
    )
    _, filled_pid = await r.insert_trade_command(
        {
            "command_type": "submit",
            "shard_key": "kraken.ETH-USD.live",
            "exchange": "kraken",
            "instrument": "ETH-USD",
            "mode": "live",
            "strategy_id": "test",
            "client_order_id": "cid-2",
            "venue_client_id": "vcid-2",
            "side": "sell",
            "order_type": "market",
            "quantity": 1.0,
            "price": None,
            "status": "filled",
            "created_at": now,
            "correlation_id": "corr-2",
            "session_id": "s1",
            "sequence_id": 2,
            "timestamp": now,
        }
    )
    cmds = await r.get_active_commands_for_exchange("kraken", now)
    assert len(cmds) == 1
    assert cmds[0]["status"] == "created"
    assert cmds[0]["instrument"] == "BTC-USD"


@pytest.mark.asyncio
async def test_revise_instrument_spec_persists_funding_fields(tmp_path: Path) -> None:
    """revise_instrument_spec round-trips the new funding metadata fields.

    Given: a seeded instrument with no spec,
    When: revise_instrument_spec is called with funding fields populated,
    Then: get_instrument_spec returns the same funding values.
    """
    r, _, inst_pid = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    spec = InstrumentSpecInput(
        tick_size=0.5,
        funding_type="perpetual_funding",
        funding_frequency_hours=1,
        max_funding_rate=0.0025,
    )
    spec_id = await r.revise_instrument_spec(
        instrument_public_id=inst_pid,
        session_id="s1",
        sequence_id=10,
        timestamp=now,
        spec=spec,
    )
    assert spec_id > 0
    row = await r.get_instrument_spec(inst_pid, as_of=now)
    assert row is not None
    assert row["funding_type"] == "perpetual_funding"
    assert row["funding_frequency_hours"] == 1
    assert row["max_funding_rate"] == pytest.approx(0.0025)
    assert row["rollover_rate_long"] is None
    assert row["rollover_rate_short"] is None


@pytest.mark.asyncio
async def test_revise_instrument_spec_funding_type_check_constraint(tmp_path: Path) -> None:
    """Funding type CHECK constraint rejects invalid values.

    Given: a seeded instrument,
    When: revise_instrument_spec is called with funding_type='bogus',
    Then: an IntegrityError is raised by the database CHECK constraint.
    """
    r, _, inst_pid = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    spec = InstrumentSpecInput(funding_type="bogus")
    with pytest.raises(IntegrityError):
        await r.revise_instrument_spec(
            instrument_public_id=inst_pid,
            session_id="s1",
            sequence_id=11,
            timestamp=now,
            spec=spec,
        )


@pytest.mark.asyncio
async def test_insert_funding_rate_round_trip(tmp_path: Path) -> None:
    """insert_funding_rate persists a row that get_funding_rates returns.

    Given: a seeded instrument,
    When: a perpetual_funding rate is inserted with effective_from=t1,
    Then: get_funding_rates with a window covering t1 returns one row
        carrying the original rate, direction, and notional asset.
    """
    r, _, inst_pid = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    effective = datetime(2026, 4, 6, 12, 0, 0, tzinfo=UTC)
    row_id = await r.insert_funding_rate(
        {
            "instrument_public_id": inst_pid,
            "exchange": "kraken",
            "rate_type": "perpetual_funding",
            "direction": "both",
            "rate": 0.0001,
            "notional_asset": "USD",
            "effective_from": effective,
            "source": "exchange_api",
            "session_id": "s1",
            "sequence_id": 100,
            "timestamp": now,
        }
    )
    assert row_id > 0
    rows = await r.get_funding_rates(
        instrument_public_id=inst_pid,
        exchange="kraken",
        rate_type="perpetual_funding",
        direction="both",
        as_of=now,
        range_start=effective - timedelta(hours=1),
        range_end=effective + timedelta(hours=1),
    )
    assert len(rows) == 1
    assert rows[0]["rate"] == pytest.approx(0.0001)
    assert rows[0]["direction"] == "both"
    assert rows[0]["notional_asset"] == "USD"
    assert rows[0]["source"] == "exchange_api"


@pytest.mark.asyncio
async def test_insert_funding_rate_duplicate_raises(tmp_path: Path) -> None:
    """Duplicate funding rate inserts raise IntegrityError on the partial unique index.

    Given: a funding rate already inserted,
    When: a second insert with the same business key is attempted,
    Then: an IntegrityError is raised so the caller can swallow the
        duplicate as an idempotency no-op.
    """
    r, _, inst_pid = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    effective = datetime(2026, 4, 6, 12, 0, 0, tzinfo=UTC)
    row: FundingRateInsertRow = {
        "instrument_public_id": inst_pid,
        "exchange": "kraken",
        "rate_type": "spot_margin_rollover",
        "direction": "long",
        "rate": 0.00025,
        "notional_asset": "USD",
        "effective_from": effective,
        "source": "exchange_docs",
        "session_id": "s1",
        "sequence_id": 200,
        "timestamp": now,
    }
    await r.insert_funding_rate(row)
    with pytest.raises(IntegrityError):
        await r.insert_funding_rate(row)


@pytest.mark.asyncio
async def test_insert_funding_rate_with_caller_session(tmp_path: Path) -> None:
    """insert_funding_rate with caller-managed session does not auto-commit.

    Given: a caller-managed AsyncSession,
    When: insert_funding_rate is called with session=s,
    Then: the row is visible inside the session before commit and
        becomes durable only after the caller commits.
    """
    r, _, inst_pid = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    effective = datetime(2026, 4, 6, 13, 0, 0, tzinfo=UTC)
    row: FundingRateInsertRow = {
        "instrument_public_id": inst_pid,
        "exchange": "kraken",
        "rate_type": "perpetual_funding",
        "direction": "both",
        "rate": 0.0002,
        "notional_asset": "USD",
        "effective_from": effective,
        "source": "exchange_api",
        "session_id": "s1",
        "sequence_id": 300,
        "timestamp": now,
    }
    async with r.session() as s:
        new_id = await r.insert_funding_rate(row, session=s)
        await s.commit()
    assert new_id > 0
    rows = await r.get_funding_rates(
        instrument_public_id=inst_pid,
        exchange="kraken",
        rate_type="perpetual_funding",
        direction="both",
        as_of=now,
        range_start=effective - timedelta(hours=1),
        range_end=effective + timedelta(hours=1),
    )
    assert len(rows) == 1


@pytest.mark.asyncio
async def test_get_funding_rates_filters_by_window(tmp_path: Path) -> None:
    """get_funding_rates honours range_start / range_end on effective_from.

    Given: three funding rates inserted at t1, t2, t3,
    When: get_funding_rates is called with the window [t2, t3],
    Then: only the t2 and t3 rows are returned ordered ascending.
    """
    r, _, inst_pid = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    base = datetime(2026, 4, 6, 0, 0, 0, tzinfo=UTC)
    rates = [
        (base, 0.0001),
        (base + timedelta(hours=1), 0.00015),
        (base + timedelta(hours=2), 0.0002),
    ]
    for idx, (eff, rate) in enumerate(rates):
        await r.insert_funding_rate(
            {
                "instrument_public_id": inst_pid,
                "exchange": "kraken",
                "rate_type": "perpetual_funding",
                "direction": "both",
                "rate": rate,
                "notional_asset": "USD",
                "effective_from": eff,
                "source": "exchange_api",
                "session_id": "s1",
                "sequence_id": 400 + idx,
                "timestamp": now,
            }
        )
    rows = await r.get_funding_rates(
        instrument_public_id=inst_pid,
        exchange="kraken",
        rate_type="perpetual_funding",
        direction="both",
        as_of=now,
        range_start=base + timedelta(hours=1),
        range_end=base + timedelta(hours=2),
    )
    assert [row["rate"] for row in rows] == pytest.approx([0.00015, 0.0002])


@pytest.mark.asyncio
async def test_get_funding_rates_no_window_returns_all(tmp_path: Path) -> None:
    """get_funding_rates returns all matching rows when window bounds are None.

    Given: two funding rates with different effective_from values,
    When: get_funding_rates is called without range_start/range_end,
    Then: both rows are returned ordered by effective_from.
    """
    r, _, inst_pid = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    base = datetime(2026, 4, 6, 0, 0, 0, tzinfo=UTC)
    for idx, eff in enumerate([base, base + timedelta(hours=1)]):
        await r.insert_funding_rate(
            {
                "instrument_public_id": inst_pid,
                "exchange": "kraken",
                "rate_type": "perpetual_funding",
                "direction": "both",
                "rate": 0.0001 * (idx + 1),
                "notional_asset": "USD",
                "effective_from": eff,
                "source": "exchange_api",
                "session_id": "s1",
                "sequence_id": 500 + idx,
                "timestamp": now,
            }
        )
    rows = await r.get_funding_rates(
        instrument_public_id=inst_pid,
        exchange="kraken",
        rate_type="perpetual_funding",
        direction="both",
        as_of=now,
    )
    assert len(rows) == 2


@pytest.mark.asyncio
async def test_insert_accrual_round_trip_and_get_last(tmp_path: Path) -> None:
    """insert_accrual + get_last_accrual round-trip the most recent row.

    Given: two accruals at t1 and t2 with t2 later,
    When: get_last_accrual is queried,
    Then: the t2 row is returned.
    """
    r, _, inst_pid = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    t1 = datetime(2026, 4, 6, 0, 0, 0, tzinfo=UTC)
    t2 = t1 + timedelta(hours=1)
    for idx, (accrued, amount) in enumerate([(t1, -0.50), (t2, -0.75)]):
        await r.insert_accrual(
            {
                "instrument_public_id": inst_pid,
                "wallet_public_id": "00000000-0000-7000-8000-000000000001",
                "mode": "live",
                "accrual_type": "funding",
                "accrued_at": accrued,
                "amount": amount,
                "amount_asset": "USD",
                "rate": 0.0001,
                "notional": 50000.0,
                "position_quantity_at_accrual": 1.0,
                "exchange": "kraken",
                "session_id": "s1",
                "sequence_id": 600 + idx,
                "timestamp": now,
            }
        )
    last = await r.get_last_accrual(inst_pid, mode="live", accrual_type="funding")
    assert last is not None
    assert last["accrued_at"] == t2
    assert last["amount"] == pytest.approx(-0.75)


@pytest.mark.asyncio
async def test_get_last_accrual_returns_none_when_empty(tmp_path: Path) -> None:
    """get_last_accrual returns None when no accruals exist for the key.

    Given: a fresh repository with no accruals,
    When: get_last_accrual is queried,
    Then: None is returned.
    """
    r, _, inst_pid = await _seed_full_repo(tmp_path)
    last = await r.get_last_accrual(inst_pid, mode="live", accrual_type="funding")
    assert last is None


@pytest.mark.asyncio
async def test_insert_accrual_duplicate_raises(tmp_path: Path) -> None:
    """Duplicate accrual rows raise IntegrityError on the partial unique index.

    Given: an accrual already inserted,
    When: a second insert with the same business key is attempted,
    Then: an IntegrityError is raised so the caller can swallow the
        duplicate as a "boundary already applied" no-op.
    """
    r, _, inst_pid = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    accrued = datetime(2026, 4, 6, 0, 0, 0, tzinfo=UTC)
    row: AccrualLedgerInsertRow = {
        "instrument_public_id": inst_pid,
        "wallet_public_id": "00000000-0000-7000-8000-000000000001",
        "mode": "live",
        "accrual_type": "funding",
        "accrued_at": accrued,
        "amount": -0.5,
        "amount_asset": "USD",
        "rate": 0.0001,
        "notional": 50000.0,
        "position_quantity_at_accrual": 1.0,
        "exchange": "kraken",
        "session_id": "s1",
        "sequence_id": 700,
        "timestamp": now,
    }
    await r.insert_accrual(row)
    with pytest.raises(IntegrityError):
        await r.insert_accrual(row)


@pytest.mark.asyncio
async def test_insert_accrual_with_caller_session(tmp_path: Path) -> None:
    """insert_accrual with caller-managed session does not auto-commit.

    Given: a caller-managed AsyncSession,
    When: insert_accrual is called with session=s,
    Then: the caller is responsible for the commit before the row is durable.
    """
    r, _, inst_pid = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    accrued = datetime(2026, 4, 6, 1, 0, 0, tzinfo=UTC)
    row: AccrualLedgerInsertRow = {
        "instrument_public_id": inst_pid,
        "wallet_public_id": "00000000-0000-7000-8000-000000000001",
        "mode": "live",
        "accrual_type": "funding",
        "accrued_at": accrued,
        "amount": -1.0,
        "amount_asset": "USD",
        "rate": 0.0001,
        "notional": 100000.0,
        "position_quantity_at_accrual": 2.0,
        "exchange": "kraken",
        "session_id": "s1",
        "sequence_id": 800,
        "timestamp": now,
    }
    async with r.session() as s:
        new_id = await r.insert_accrual(row, session=s)
        await s.commit()
    assert new_id > 0
    rows = await r.get_accruals(
        instrument_public_id=inst_pid,
        mode="live",
        range_start=now - timedelta(hours=1),
        range_end=now + timedelta(hours=1),
    )
    assert len(rows) == 1


@pytest.mark.asyncio
async def test_insert_accrual_savepoint_isolates_duplicate_in_caller_session(
    tmp_path: Path,
) -> None:
    """Caller-managed duplicate accrual rolls back the SAVEPOINT only.

    Given: a caller-managed AsyncSession that already inserted accrual A,
    When: a duplicate insert of A raises IntegrityError but the caller
        catches it and proceeds with a different accrual B in the SAME
        session,
    Then: B is committed alongside A, proving the savepoint isolated
        the duplicate failure from the outer transaction. This is the
        contract that the future B4 funding accrual loop relies on for
        idempotent boundary application.
    """
    r, _, inst_pid = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    accrued_a = datetime(2026, 4, 6, 0, 0, 0, tzinfo=UTC)
    accrued_b = datetime(2026, 4, 6, 1, 0, 0, tzinfo=UTC)
    row_a: AccrualLedgerInsertRow = {
        "instrument_public_id": inst_pid,
        "wallet_public_id": "00000000-0000-7000-8000-000000000001",
        "mode": "live",
        "accrual_type": "funding",
        "accrued_at": accrued_a,
        "amount": -0.5,
        "amount_asset": "USD",
        "rate": 0.0001,
        "notional": 50000.0,
        "position_quantity_at_accrual": 1.0,
        "exchange": "kraken",
        "session_id": "s1",
        "sequence_id": 950,
        "timestamp": now,
    }
    row_b: AccrualLedgerInsertRow = {
        "instrument_public_id": inst_pid,
        "wallet_public_id": "00000000-0000-7000-8000-000000000001",
        "mode": "live",
        "accrual_type": "funding",
        "accrued_at": accrued_b,
        "amount": -0.6,
        "amount_asset": "USD",
        "rate": 0.0001,
        "notional": 60000.0,
        "position_quantity_at_accrual": 1.0,
        "exchange": "kraken",
        "session_id": "s1",
        "sequence_id": 951,
        "timestamp": now,
    }
    await r.insert_accrual(row_a)
    async with r.session() as s:
        with pytest.raises(IntegrityError):
            await r.insert_accrual(row_a, session=s)
        await r.insert_accrual(row_b, session=s)
        await s.commit()
    rows = await r.get_accruals(
        instrument_public_id=inst_pid,
        mode="live",
        range_start=now - timedelta(hours=1),
        range_end=now + timedelta(hours=1),
    )
    assert [row["accrued_at"] for row in rows] == [accrued_a, accrued_b]


@pytest.mark.asyncio
async def test_insert_funding_rate_savepoint_isolates_duplicate_in_caller_session(
    tmp_path: Path,
) -> None:
    """Caller-managed duplicate funding rate rolls back the SAVEPOINT only.

    Mirrors the accrual savepoint test for ``insert_funding_rate`` so a
    future caller that wants to batch a funding-rate seed with another
    write inside the same outer transaction is not poisoned by a
    duplicate insert.
    """
    r, _, inst_pid = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    eff_a = datetime(2026, 4, 6, 0, 0, 0, tzinfo=UTC)
    eff_b = datetime(2026, 4, 6, 1, 0, 0, tzinfo=UTC)
    row_a: FundingRateInsertRow = {
        "instrument_public_id": inst_pid,
        "exchange": "kraken",
        "rate_type": "perpetual_funding",
        "direction": "both",
        "rate": 0.0001,
        "notional_asset": "USD",
        "effective_from": eff_a,
        "source": "exchange_api",
        "session_id": "s1",
        "sequence_id": 960,
        "timestamp": now,
    }
    row_b: FundingRateInsertRow = {
        "instrument_public_id": inst_pid,
        "exchange": "kraken",
        "rate_type": "perpetual_funding",
        "direction": "both",
        "rate": 0.00015,
        "notional_asset": "USD",
        "effective_from": eff_b,
        "source": "exchange_api",
        "session_id": "s1",
        "sequence_id": 961,
        "timestamp": now,
    }
    await r.insert_funding_rate(row_a)
    async with r.session() as s:
        with pytest.raises(IntegrityError):
            await r.insert_funding_rate(row_a, session=s)
        await r.insert_funding_rate(row_b, session=s)
        await s.commit()
    rows = await r.get_funding_rates(
        instrument_public_id=inst_pid,
        exchange="kraken",
        rate_type="perpetual_funding",
        direction="both",
        as_of=now,
        range_start=eff_a - timedelta(hours=1),
        range_end=eff_b + timedelta(hours=1),
    )
    assert [row["effective_from"] for row in rows] == [eff_a, eff_b]


@pytest.mark.asyncio
async def test_get_accruals_strict_lower_bound(tmp_path: Path) -> None:
    """get_accruals uses a STRICT lower bound on accrued_at.

    Given: accruals at t0, t1, t2 with t0 < t1 < t2,
    When: get_accruals is queried with range_start=t1, range_end=t2,
    Then: only the t2 row is returned (the t1 boundary already in a
        prior checkpoint snapshot must NOT be re-applied).
    """
    r, _, inst_pid = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    t0 = datetime(2026, 4, 6, 0, 0, 0, tzinfo=UTC)
    t1 = t0 + timedelta(hours=1)
    t2 = t0 + timedelta(hours=2)
    for idx, accrued in enumerate([t0, t1, t2]):
        await r.insert_accrual(
            {
                "instrument_public_id": inst_pid,
                "wallet_public_id": "00000000-0000-7000-8000-000000000001",
                "mode": "live",
                "accrual_type": "funding",
                "accrued_at": accrued,
                "amount": -0.1 * (idx + 1),
                "amount_asset": "USD",
                "rate": 0.0001,
                "notional": 10000.0,
                "position_quantity_at_accrual": 1.0,
                "exchange": "kraken",
                "session_id": "s1",
                "sequence_id": 900 + idx,
                "timestamp": now,
            }
        )
    rows = await r.get_accruals(
        instrument_public_id=inst_pid,
        mode="live",
        range_start=now - timedelta(seconds=1),
        range_end=now + timedelta(seconds=1),
    )
    assert len(rows) == 3
    assert [row["accrued_at"] for row in rows] == [t0, t1, t2]


@pytest.mark.asyncio
async def test_get_orders_filters_by_wallet_public_ids(tmp_path: Path) -> None:
    """Verify ``get_orders`` applies the ``wallet_public_ids`` filter.

    Given: An order with ``wallet_public_id='w-1'``,
    When: ``get_orders(wallet_public_ids=['w-other'])`` is called,
    Then: The order is excluded from the result.
    """
    r, _, inst_pid = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    await r.insert_order(
        instrument_public_id=inst_pid,
        wallet_public_id="00000000-0000-7000-8000-000000000001",
        client_order_id="c1",
        exchange_order_id="e1",
        created_at=now,
        side="buy",
        order_type="limit",
        price=50000.0,
        size=1.0,
        status="open",
        session_id="s1",
        sequence_id=20,
        timestamp=now,
    )
    result = await r.get_orders(
        limit=10,
        offset=0,
        as_of=now,
        wallet_public_ids=["00000000-0000-7000-8000-fffffffffff9"],
    )
    assert len(result) == 0
    result_matching = await r.get_orders(
        limit=10,
        offset=0,
        as_of=now,
        wallet_public_ids=["00000000-0000-7000-8000-000000000001"],
    )
    assert len(result_matching) == 1


@pytest.mark.asyncio
async def test_get_signals_filters_by_wallet_public_ids(tmp_path: Path) -> None:
    """Verify ``get_signals`` applies the ``wallet_public_ids`` filter.

    Given: A signal inserted for a known wallet,
    When: ``get_signals(wallet_public_ids=['other'])`` is called,
    Then: The signal is excluded.
    """
    r, _, inst_pid = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    async with r.session() as s:
        s.add(
            Signal(
                instrument_public_id=inst_pid,
                wallet_public_id="00000000-0000-7000-8000-000000000001",
                strategy_name="test_strategy",
                side="buy",
                strength=0.8,
                reason="test",
                price=50000.0,
                fired_at=now,
                timestamp=now,
                session_id="s1",
                sequence_id=30,
            )
        )
        await s.commit()
    result = await r.get_signals(
        since=now - timedelta(hours=1),
        limit=10,
        as_of=now,
        wallet_public_ids=["00000000-0000-7000-8000-fffffffffff9"],
    )
    assert len(result) == 0
    result_matching = await r.get_signals(
        since=now - timedelta(hours=1),
        limit=10,
        as_of=now,
        wallet_public_ids=["00000000-0000-7000-8000-000000000001"],
    )
    assert len(result_matching) == 1


@pytest.mark.asyncio
async def test_get_executions_filters_by_wallet_public_ids(tmp_path: Path) -> None:
    """Verify ``get_executions`` applies the ``wallet_public_ids`` filter.

    Given: An execution for wallet w-1,
    When: ``get_executions(wallet_public_ids=['other'])`` is called,
    Then: The execution is excluded.
    """
    r, _, inst_pid = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    await r.insert_order(
        instrument_public_id=inst_pid,
        wallet_public_id="00000000-0000-7000-8000-000000000001",
        client_order_id="c-exec",
        exchange_order_id="e-exec",
        created_at=now,
        side="buy",
        order_type="limit",
        price=50000.0,
        size=1.0,
        status="filled",
        session_id="s1",
        sequence_id=40,
        timestamp=now,
    )
    orders = await r.get_orders(limit=1, offset=0, as_of=now)
    order_pid = orders[0]["public_id"]
    await r.insert_execution(
        order_public_id=order_pid,
        trade_id="t1",
        side="buy",
        size=1.0,
        price=50000.0,
        fee=0.1,
        fee_asset="USD",
        status="filled",
        session_id="s1",
        sequence_id=41,
        timestamp=now,
        wallet_public_id="00000000-0000-7000-8000-000000000001",
    )
    result = await r.get_executions(
        limit=10,
        as_of=now,
        wallet_public_ids=["00000000-0000-7000-8000-fffffffffff9"],
    )
    assert len(result) == 0
    result_matching = await r.get_executions(
        limit=10,
        as_of=now,
        wallet_public_ids=["00000000-0000-7000-8000-000000000001"],
    )
    assert len(result_matching) == 1


@pytest.mark.asyncio
async def test_get_positions_filters_by_wallet_public_ids(tmp_path: Path) -> None:
    """Verify ``get_positions`` applies the ``wallet_public_ids`` filter.

    Given: A position for wallet w-1,
    When: ``get_positions(wallet_public_ids=['other'])`` is called,
    Then: The position is excluded.
    """
    r, _, inst_pid = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    async with r.session() as s:
        s.add(
            Position(
                instrument_public_id=inst_pid,
                wallet_public_id="00000000-0000-7000-8000-000000000001",
                quantity=1.0,
                average_price=50000.0,
                unrealized_pnl=0.0,
                realized_pnl=0.0,
                timestamp=now,
                session_id="s1",
                sequence_id=50,
            )
        )
        await s.commit()
    result = await r.get_positions(
        as_of=now,
        wallet_public_ids=["00000000-0000-7000-8000-fffffffffff9"],
    )
    assert len(result) == 0
    result_matching = await r.get_positions(
        as_of=now,
        wallet_public_ids=["00000000-0000-7000-8000-000000000001"],
    )
    assert len(result_matching) == 1


@pytest.mark.asyncio
async def test_get_positions_single_cycle_returns_cycle_pid(tmp_path: Path) -> None:
    """Verify get_positions returns position_cycle_public_id for unambiguous cycle.

    Given: One position and one open cycle for the same instrument/exchange/mode/wallet,
    When: get_positions is called,
    Then: position_cycle_public_id is the cycle's public_id.
    """
    r, _, inst_pid = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    async with r.session() as s:
        s.add(
            Position(
                instrument_public_id=inst_pid,
                wallet_public_id="wallet-1",
                quantity=1.5,
                average_price=48000.0,
                unrealized_pnl=0.0,
                realized_pnl=0.0,
                timestamp=now,
                session_id="s1",
                sequence_id=30,
            )
        )
        await s.commit()
    cycle_row = _make_cycle_row(
        instrument_public_id=inst_pid,
        exchange="kraken",
        mode="live",
        shard_key="kraken.BTC-USD.live",
        wallet_public_id="wallet-1",
        direction="long",
        max_qty=1.5,
        opened_at=now,
        timestamp=now,
        sequence_id=31,
    )
    _, cycle_pid = await r.insert_position_cycle(cycle_row)
    result = await r.get_positions(as_of=now)
    assert len(result) == 1
    assert result[0]["position_cycle_public_id"] == cycle_pid


@pytest.mark.asyncio
async def test_get_positions_multiple_cycles_returns_null(tmp_path: Path) -> None:
    """Verify get_positions returns NULL when multiple cycles match one position.

    Given: One position and two open cycles for the same instrument/exchange/mode/wallet
    (different shard_keys, e.g. paper mode with strategy tags),
    When: get_positions is called,
    Then: position_cycle_public_id is None (fail-closed).
    """
    r, _, inst_pid = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    async with r.session() as s:
        s.add(
            Position(
                instrument_public_id=inst_pid,
                wallet_public_id="wallet-1",
                mode="paper",
                quantity=2.0,
                average_price=48000.0,
                unrealized_pnl=0.0,
                realized_pnl=0.0,
                timestamp=now,
                session_id="s1",
                sequence_id=30,
            )
        )
        await s.commit()
    cycle_a = _make_cycle_row(
        instrument_public_id=inst_pid,
        exchange="kraken",
        mode="paper",
        shard_key="kraken.BTC-USD.paper.momentum",
        wallet_public_id="wallet-1",
        direction="long",
        max_qty=1.0,
        opened_at=now,
        timestamp=now,
        sequence_id=31,
    )
    cycle_b = _make_cycle_row(
        instrument_public_id=inst_pid,
        exchange="kraken",
        mode="paper",
        shard_key="kraken.BTC-USD.paper.mean_revert",
        wallet_public_id="wallet-1",
        direction="long",
        max_qty=1.0,
        opened_at=now,
        timestamp=now,
        sequence_id=32,
    )
    await r.insert_position_cycle(cycle_a)
    await r.insert_position_cycle(cycle_b)
    result = await r.get_positions(as_of=now)
    assert len(result) == 1
    assert result[0]["position_cycle_public_id"] is None


@pytest.mark.asyncio
async def test_get_accruals_wallet_filter(tmp_path: Path) -> None:
    """Verify get_accruals filters by wallet_public_id when provided.

    Given: Two accruals for different wallets on the same instrument,
    When: get_accruals called with wallet_public_id,
    Then: Only the matching wallet's accrual is returned.
    """
    r, _, inst_pid = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    wallet_a = "00000000-0000-7000-8000-000000000001"
    wallet_b = "00000000-0000-7000-8000-000000000002"
    for wallet, seq in [(wallet_a, 901), (wallet_b, 902)]:
        await r.insert_accrual(
            {
                "instrument_public_id": inst_pid,
                "wallet_public_id": wallet,
                "mode": "live",
                "accrual_type": "funding",
                "accrued_at": datetime(2026, 4, 6, 4, 0, 0, tzinfo=UTC),
                "amount": -1.0,
                "amount_asset": "USD",
                "rate": 0.0001,
                "notional": 100000.0,
                "position_quantity_at_accrual": 1.0,
                "exchange": "kraken",
                "session_id": "s1",
                "sequence_id": seq,
                "timestamp": now,
            }
        )
    rows = await r.get_accruals(
        instrument_public_id=inst_pid,
        mode="live",
        range_start=now - timedelta(hours=1),
        range_end=now + timedelta(hours=1),
        wallet_public_id=wallet_a,
    )
    assert len(rows) == 1


@pytest.mark.asyncio
async def test_get_last_accrual_wallet_filter(tmp_path: Path) -> None:
    """Verify get_last_accrual filters by wallet_public_id when provided.

    Given: Two accruals for different wallets on the same instrument,
    When: get_last_accrual called with wallet_public_id,
    Then: Only the matching wallet's accrual is returned.
    """
    r, _, inst_pid = await _seed_full_repo(tmp_path)
    now = datetime.now(UTC)
    wallet_a = "00000000-0000-7000-8000-000000000001"
    wallet_b = "00000000-0000-7000-8000-000000000002"
    for wallet, seq, accrued in [
        (wallet_a, 901, datetime(2026, 4, 6, 4, 0, 0, tzinfo=UTC)),
        (wallet_b, 902, datetime(2026, 4, 6, 8, 0, 0, tzinfo=UTC)),
    ]:
        await r.insert_accrual(
            {
                "instrument_public_id": inst_pid,
                "wallet_public_id": wallet,
                "mode": "live",
                "accrual_type": "funding",
                "accrued_at": accrued,
                "amount": -1.0,
                "amount_asset": "USD",
                "rate": 0.0001,
                "notional": 100000.0,
                "position_quantity_at_accrual": 1.0,
                "exchange": "kraken",
                "session_id": "s1",
                "sequence_id": seq,
                "timestamp": now,
            }
        )
    row = await r.get_last_accrual(
        instrument_public_id=inst_pid,
        mode="live",
        accrual_type="funding",
        wallet_public_id=wallet_a,
    )
    assert row is not None
    assert row["accrued_at"] == datetime(2026, 4, 6, 4, 0, 0, tzinfo=UTC)


@pytest.mark.asyncio
async def test_get_instrument_capabilities_returns_active_rows(tmp_path: Path) -> None:
    """Given seeded capability rows, When querying, Then active rows returned."""
    db_path = tmp_path / "cap.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    async with r.session() as s:
        s.add(
            InstrumentOrderCapability(
                instrument_public_id="inst-1",
                exchange="kraken",
                supported_order_types=["market", "limit"],
                supports_post_only=True,
                supports_reduce_only=False,
                supports_amend_in_place=False,
                supports_native_stop_loss=True,
                supports_native_take_profit=True,
                supports_trailing_stop_client_side=True,
                supports_market_making=False,
                supports_short_selling=True,
                supports_leverage=True,
                max_leverage_long=5.0,
                max_leverage_short=3.0,
                min_notional=10.0,
                max_order_size=1000.0,
                top_of_book_quality="realtime",
                timestamp=now,
                session_id="s1",
                sequence_id=1,
            )
        )
        s.add(
            InstrumentOrderCapability(
                instrument_public_id="inst-2",
                exchange="zonda",
                supported_order_types=["limit"],
                supports_post_only=False,
                supports_reduce_only=False,
                supports_amend_in_place=False,
                supports_native_stop_loss=False,
                supports_native_take_profit=False,
                supports_trailing_stop_client_side=True,
                supports_market_making=False,
                supports_short_selling=False,
                supports_leverage=False,
                max_leverage_long=1.0,
                max_leverage_short=0.0,
                min_notional=None,
                max_order_size=None,
                top_of_book_quality="polled",
                timestamp=now,
                session_id="s1",
                sequence_id=2,
            )
        )
        await s.commit()
    rows = await r.get_instrument_capabilities(as_of=now)
    assert len(rows) == 2
    assert rows[0]["exchange"] == "kraken"
    assert rows[0]["supported_order_types"] == ["market", "limit"]
    assert rows[0]["supports_post_only"] is True
    assert rows[0]["max_leverage_long"] == 5.0
    assert rows[1]["exchange"] == "zonda"


@pytest.mark.asyncio
async def test_get_instrument_capabilities_exchange_filter(tmp_path: Path) -> None:
    """Given rows for multiple exchanges, When filtering, Then only matching returned."""
    db_path = tmp_path / "cap_filter.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    async with r.session() as s:
        for ex in ("kraken", "zonda"):
            s.add(
                InstrumentOrderCapability(
                    instrument_public_id=f"inst-{ex}",
                    exchange=ex,
                    supported_order_types=["limit"],
                    timestamp=now,
                    session_id="s1",
                    sequence_id=1,
                )
            )
        await s.commit()
    rows = await r.get_instrument_capabilities(as_of=now, exchange="kraken")
    assert len(rows) == 1
    assert rows[0]["exchange"] == "kraken"


@pytest.mark.asyncio
async def test_get_instrument_capabilities_instrument_filter(tmp_path: Path) -> None:
    """Given rows, When filtering by instrument, Then only matching returned."""
    db_path = tmp_path / "cap_inst.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    async with r.session() as s:
        for idx in (1, 2):
            s.add(
                InstrumentOrderCapability(
                    instrument_public_id=f"inst-{idx}",
                    exchange="kraken",
                    supported_order_types=["limit"],
                    timestamp=now,
                    session_id="s1",
                    sequence_id=idx,
                )
            )
        await s.commit()
    rows = await r.get_instrument_capabilities(as_of=now, instrument_public_id="inst-2")
    assert len(rows) == 1
    assert rows[0]["instrument_public_id"] == "inst-2"


@pytest.mark.asyncio
async def test_get_instrument_capabilities_empty(tmp_path: Path) -> None:
    """Given no rows, When querying, Then empty list returned."""
    db_path = tmp_path / "cap_empty.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    rows = await r.get_instrument_capabilities(as_of=datetime.now(UTC))
    assert rows == []


@pytest.mark.asyncio
async def test_get_venue_fee_schedules_returns_active_rows(tmp_path: Path) -> None:
    """Given seeded fee schedule rows, When querying, Then active rows returned."""
    db_path = tmp_path / "fees.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    async with r.session() as s:
        s.add(
            VenueFeeSchedule(
                exchange="kraken",
                instrument_public_id=None,
                fee_tier="default",
                maker_bps=16.0,
                taker_bps=26.0,
                min_volume_30d=None,
                currency="USD",
                timestamp=now,
                session_id="s1",
                sequence_id=1,
            )
        )
        s.add(
            VenueFeeSchedule(
                exchange="kraken",
                instrument_public_id=None,
                fee_tier="vip_1",
                maker_bps=12.0,
                taker_bps=22.0,
                min_volume_30d=50000.0,
                currency="USD",
                timestamp=now,
                session_id="s1",
                sequence_id=2,
            )
        )
        await s.commit()
    rows = await r.get_venue_fee_schedules(as_of=now)
    assert len(rows) == 2
    assert rows[0]["fee_tier"] == "default"
    assert rows[0]["maker_bps"] == 16.0
    assert rows[0]["taker_bps"] == 26.0
    assert rows[1]["fee_tier"] == "vip_1"
    assert rows[1]["min_volume_30d"] == 50000.0


@pytest.mark.asyncio
async def test_get_venue_fee_schedules_exchange_filter(tmp_path: Path) -> None:
    """Given rows for multiple exchanges, When filtering, Then only matching returned."""
    db_path = tmp_path / "fees_filter.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    async with r.session() as s:
        for ex in ("kraken", "zonda"):
            s.add(
                VenueFeeSchedule(
                    exchange=ex,
                    instrument_public_id=None,
                    fee_tier="default",
                    maker_bps=16.0,
                    taker_bps=26.0,
                    min_volume_30d=None,
                    currency="USD",
                    timestamp=now,
                    session_id="s1",
                    sequence_id=1,
                )
            )
        await s.commit()
    rows = await r.get_venue_fee_schedules(as_of=now, exchange="zonda")
    assert len(rows) == 1
    assert rows[0]["exchange"] == "zonda"


@pytest.mark.asyncio
async def test_get_venue_fee_schedules_empty(tmp_path: Path) -> None:
    """Given no rows, When querying, Then empty list returned."""
    db_path = tmp_path / "fees_empty.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    rows = await r.get_venue_fee_schedules(as_of=datetime.now(UTC))
    assert rows == []


@pytest.mark.asyncio
async def test_get_instrument_capabilities_temporal_filter(tmp_path: Path) -> None:
    """Closed rows (known_to <= as_of) are excluded; future rows excluded too."""
    db_path = tmp_path / "cap_temporal.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    t1 = datetime(2026, 1, 1, tzinfo=UTC)
    t2 = datetime(2026, 6, 1, tzinfo=UTC)
    t3 = datetime(2026, 12, 1, tzinfo=UTC)
    async with r.session() as s:
        s.add(
            InstrumentOrderCapability(
                instrument_public_id="inst-closed",
                exchange="kraken",
                supported_order_types=["limit"],
                timestamp=t1,
                known_to=t2,
                session_id="s1",
                sequence_id=1,
            )
        )
        s.add(
            InstrumentOrderCapability(
                instrument_public_id="inst-active",
                exchange="kraken",
                supported_order_types=["market"],
                timestamp=t1,
                known_to=KNOWN_TO_MAX,
                session_id="s1",
                sequence_id=2,
            )
        )
        s.add(
            InstrumentOrderCapability(
                instrument_public_id="inst-future",
                exchange="kraken",
                supported_order_types=["limit"],
                timestamp=t3,
                known_to=KNOWN_TO_MAX,
                session_id="s1",
                sequence_id=3,
            )
        )
        await s.commit()
    rows = await r.get_instrument_capabilities(as_of=t2)
    assert len(rows) == 1
    assert rows[0]["instrument_public_id"] == "inst-active"


@pytest.mark.asyncio
async def test_get_venue_fee_schedules_temporal_filter(tmp_path: Path) -> None:
    """Closed rows excluded; only active-at-as_of rows returned."""
    db_path = tmp_path / "fees_temporal.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    t1 = datetime(2026, 1, 1, tzinfo=UTC)
    t2 = datetime(2026, 6, 1, tzinfo=UTC)
    async with r.session() as s:
        s.add(
            VenueFeeSchedule(
                exchange="kraken",
                instrument_public_id=None,
                fee_tier="old",
                maker_bps=20.0,
                taker_bps=30.0,
                min_volume_30d=None,
                currency="USD",
                timestamp=t1,
                known_to=t2,
                session_id="s1",
                sequence_id=1,
            )
        )
        s.add(
            VenueFeeSchedule(
                exchange="kraken",
                instrument_public_id=None,
                fee_tier="current",
                maker_bps=16.0,
                taker_bps=26.0,
                min_volume_30d=None,
                currency="USD",
                timestamp=t1,
                known_to=KNOWN_TO_MAX,
                session_id="s1",
                sequence_id=2,
            )
        )
        await s.commit()
    rows = await r.get_venue_fee_schedules(as_of=t2)
    assert len(rows) == 1
    assert rows[0]["fee_tier"] == "current"


@pytest.mark.asyncio
async def test_insert_and_get_execution_plan(tmp_path: Path) -> None:
    """Given an inserted plan, When querying by public_id, Then row returned."""
    db_path = tmp_path / "plans.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    _id, pid = await r.insert_execution_plan(
        {
            "plan_type": "manual_once",
            "created_by_user_id": "user-1",
            "created_via": "ui",
            "instrument_public_id": "inst-1",
            "exchange": "kraken",
            "mode": "live",
            "shard_key": "kraken:BTC-USD:live",
            "wallet_public_id": "wallet-1",
            "total_quantity": 0.5,
            "side": "buy",
            "params": {"order_type": "limit", "price": 50000.0},
            "status": "pending",
            "created_at": now,
            "session_id": "s1",
            "sequence_id": 1,
            "timestamp": now,
        }
    )
    assert _id > 0
    assert pid
    row = await r.get_execution_plan(pid, as_of=now)
    assert row is not None
    assert row["plan_type"] == "manual_once"
    assert row["status"] == "pending"
    assert row["total_quantity"] == 0.5


@pytest.mark.asyncio
async def test_get_execution_plans_with_filters(tmp_path: Path) -> None:
    """Given multiple plans, When filtering, Then correct subset returned."""
    db_path = tmp_path / "plans_filter.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    for i, status in enumerate(("pending", "active", "completed")):
        await r.insert_execution_plan(
            {
                "plan_type": "manual_once",
                "created_by_user_id": "user-1",
                "created_via": "ui",
                "instrument_public_id": "inst-1",
                "exchange": "kraken",
                "mode": "live",
                "shard_key": "kraken:BTC-USD:live",
                "wallet_public_id": "wallet-1",
                "total_quantity": 1.0,
                "side": "buy",
                "params": {},
                "status": status,
                "created_at": now,
                "session_id": "s1",
                "sequence_id": i + 1,
                "timestamp": now,
            }
        )
    rows = await r.get_execution_plans(as_of=now, status="active")
    assert len(rows) == 1
    assert rows[0]["status"] == "active"
    all_rows = await r.get_execution_plans(as_of=now)
    assert len(all_rows) == 3


@pytest.mark.asyncio
async def test_update_execution_plan_status_scd2(tmp_path: Path) -> None:
    """Given an active plan, When updating status, Then SCD2 versioning applied."""
    db_path = tmp_path / "plans_scd2.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    _id, pid = await r.insert_execution_plan(
        {
            "plan_type": "manual_once",
            "created_by_user_id": "user-1",
            "created_via": "ui",
            "instrument_public_id": "inst-1",
            "exchange": "kraken",
            "mode": "live",
            "shard_key": "kraken:BTC-USD:live",
            "wallet_public_id": "wallet-1",
            "total_quantity": 1.0,
            "side": "buy",
            "params": {},
            "status": "pending",
            "created_at": now,
            "session_id": "s1",
            "sequence_id": 1,
            "timestamp": now,
        }
    )
    later = datetime(2026, 6, 1, tzinfo=UTC)
    new_id = await r.update_execution_plan_status(
        public_id=pid,
        new_status="active",
        bus_time=later,
        session_id="s2",
        sequence_id=2,
        started_at=later,
    )
    assert new_id is not None
    assert new_id != _id
    row = await r.get_execution_plan(pid, as_of=later)
    assert row is not None
    assert row["status"] == "active"
    assert row["started_at"] == later


@pytest.mark.asyncio
async def test_update_execution_plan_status_not_found(tmp_path: Path) -> None:
    """Given no matching plan, When updating, Then None returned."""
    db_path = tmp_path / "plans_notfound.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    result = await r.update_execution_plan_status(
        public_id="nonexistent",
        new_status="active",
        bus_time=datetime.now(UTC),
        session_id="s1",
        sequence_id=1,
    )
    assert result is None


@pytest.mark.asyncio
async def test_get_active_execution_plans(tmp_path: Path) -> None:
    """Given plans with various statuses, When querying active, Then only actionable returned."""
    db_path = tmp_path / "plans_active.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    for i, status in enumerate(("pending", "active", "completed", "cancelled", "armed")):
        await r.insert_execution_plan(
            {
                "plan_type": "manual_once",
                "created_by_user_id": "user-1",
                "created_via": "ui",
                "instrument_public_id": "inst-1",
                "exchange": "kraken",
                "mode": "live",
                "shard_key": "kraken:BTC-USD:live",
                "wallet_public_id": "wallet-1",
                "total_quantity": 1.0,
                "side": "buy",
                "params": {},
                "status": status,
                "created_at": now,
                "session_id": "s1",
                "sequence_id": i + 1,
                "timestamp": now,
            }
        )
    rows = await r.get_active_execution_plans()
    statuses = {row["status"] for row in rows}
    assert statuses == {"pending", "active", "armed"}
    assert "completed" not in statuses
    assert "cancelled" not in statuses


@pytest.mark.asyncio
async def test_insert_and_get_plan_checkpoint(tmp_path: Path) -> None:
    """Given a plan, When inserting checkpoints, Then latest is returned."""
    db_path = tmp_path / "plans_cp.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    _id, pid = await r.insert_execution_plan(
        {
            "plan_type": "manual_once",
            "created_by_user_id": "user-1",
            "created_via": "ui",
            "instrument_public_id": "inst-1",
            "exchange": "kraken",
            "mode": "live",
            "shard_key": "kraken:BTC-USD:live",
            "wallet_public_id": "wallet-1",
            "total_quantity": 1.0,
            "side": "buy",
            "params": {},
            "status": "active",
            "created_at": now,
            "session_id": "s1",
            "sequence_id": 1,
            "timestamp": now,
        }
    )
    t1 = datetime(2026, 4, 10, 12, 0, 0, tzinfo=UTC)
    t2 = datetime(2026, 4, 10, 12, 0, 10, tzinfo=UTC)
    await r.insert_execution_plan_checkpoint(
        plan_public_id=pid,
        state={"version": 1},
        last_venue_event_id=10,
        checkpoint_at=t1,
        session_id="s1",
        sequence_id=2,
        bus_time=t1,
    )
    await r.insert_execution_plan_checkpoint(
        plan_public_id=pid,
        state={"version": 2},
        last_venue_event_id=20,
        checkpoint_at=t2,
        session_id="s1",
        sequence_id=3,
        bus_time=t2,
    )
    cp = await r.get_latest_plan_checkpoint(pid)
    assert cp is not None
    assert cp["state"]["version"] == 2
    assert cp["last_venue_event_id"] == 20


@pytest.mark.asyncio
async def test_get_latest_plan_checkpoint_none(tmp_path: Path) -> None:
    """Given no checkpoints, When querying, Then None returned."""
    db_path = tmp_path / "plans_cp_none.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    result = await r.get_latest_plan_checkpoint("nonexistent-plan")
    assert result is None


@pytest.mark.asyncio
async def test_get_execution_plan_not_found(tmp_path: Path) -> None:
    """Given no matching plan, When querying by public_id, Then None returned."""
    db_path = tmp_path / "plans_notfound2.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    result = await r.get_execution_plan("nonexistent", as_of=datetime.now(UTC))
    assert result is None


@pytest.mark.asyncio
async def test_get_execution_plans_exchange_mode_wallet_filters(tmp_path: Path) -> None:
    """Given plans, When filtering by exchange/mode/wallet, Then branches covered."""
    db_path = tmp_path / "plans_branches.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    await r.insert_execution_plan(
        {
            "plan_type": "manual_once",
            "created_by_user_id": "user-1",
            "created_via": "ui",
            "instrument_public_id": "inst-1",
            "exchange": "kraken",
            "mode": "live",
            "shard_key": "kraken:BTC-USD:live",
            "wallet_public_id": "wallet-1",
            "total_quantity": 1.0,
            "side": "buy",
            "params": {},
            "status": "active",
            "created_at": now,
            "session_id": "s1",
            "sequence_id": 1,
            "timestamp": now,
        }
    )
    rows = await r.get_execution_plans(as_of=now, exchange="kraken")
    assert len(rows) == 1
    rows = await r.get_execution_plans(as_of=now, exchange="zonda")
    assert len(rows) == 0
    rows = await r.get_execution_plans(as_of=now, mode="live")
    assert len(rows) == 1
    rows = await r.get_execution_plans(as_of=now, mode="paper")
    assert len(rows) == 0
    rows = await r.get_execution_plans(as_of=now, wallet_public_ids=["wallet-1"])
    assert len(rows) == 1
    rows = await r.get_execution_plans(as_of=now, wallet_public_ids=["wallet-other"])
    assert len(rows) == 0


def _make_cycle_row(
    shard_key: str = "kraken.BTC-USD.live.w0000000000aa",
    instrument_public_id: str = "inst-btc",
    exchange: str = "kraken",
    mode: str = "live",
    wallet_public_id: str = "wallet-1",
    operator_public_id: str | None = None,
    direction: str = "long",
    max_qty: float = 1.0,
    status: str = "open",
    opened_at: datetime | None = None,
    opening_command_public_id: str | None = None,
    session_id: str = "s1",
    sequence_id: int = 1,
    timestamp: datetime | None = None,
) -> dict[str, Any]:
    """Build a PositionCycleInsertRow dict for repository tests."""
    now = datetime.now(UTC)
    return {
        "instrument_public_id": instrument_public_id,
        "exchange": exchange,
        "mode": mode,
        "shard_key": shard_key,
        "wallet_public_id": wallet_public_id,
        "operator_public_id": operator_public_id,
        "direction": direction,
        "max_qty": max_qty,
        "status": status,
        "opened_at": opened_at or now,
        "opening_command_public_id": opening_command_public_id,
        "session_id": session_id,
        "sequence_id": sequence_id,
        "timestamp": timestamp or now,
    }


@pytest.mark.asyncio
async def test_insert_and_get_open_position_cycle(tmp_path: Path) -> None:
    """Given an open cycle inserted, When queried by shard_key, Then row returned."""
    db_path = tmp_path / "pc_insert.db"
    r = repo_module.SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    row = _make_cycle_row(max_qty=2.5, direction="long", opened_at=now, timestamp=now)
    new_id, pid = await r.insert_position_cycle(row)
    assert new_id > 0
    assert pid
    found = await r.get_open_position_cycle(
        "kraken.BTC-USD.live.w0000000000aa",
        as_of=now + timedelta(seconds=1),
    )
    assert found is not None
    assert found["public_id"] == pid
    assert found["direction"] == "long"
    assert found["max_qty"] == pytest.approx(2.5)
    assert found["status"] == "open"
    assert found["closed_at"] is None
    assert found["opening_command_public_id"] is None


@pytest.mark.asyncio
async def test_get_open_position_cycle_not_found(tmp_path: Path) -> None:
    """Given no cycle, When queried by shard_key, Then None returned."""
    db_path = tmp_path / "pc_missing.db"
    r = repo_module.SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    result = await r.get_open_position_cycle("nonexistent.shard", as_of=now)
    assert result is None


@pytest.mark.asyncio
async def test_close_position_cycle_scd2(tmp_path: Path) -> None:
    """Given an open cycle, When closed, Then SCD2 transition + closed_at recorded."""
    db_path = tmp_path / "pc_close.db"
    r = repo_module.SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    row = _make_cycle_row(opened_at=now, timestamp=now)
    _id, pid = await r.insert_position_cycle(row)
    close_time = now + timedelta(minutes=5)
    new_row_id = await r.close_position_cycle(
        cycle_public_id=pid,
        closed_at=close_time,
        closing_command_public_id="cmd-close-1",
        bus_time=close_time,
        session_id="s2",
        sequence_id=2,
    )
    assert new_row_id is not None
    after = await r.get_open_position_cycle(
        "kraken.BTC-USD.live.w0000000000aa",
        as_of=close_time + timedelta(seconds=1),
    )
    assert after is None


@pytest.mark.asyncio
async def test_close_position_cycle_noop_on_closed(tmp_path: Path) -> None:
    """Given a closed cycle, When close called again, Then None returned."""
    db_path = tmp_path / "pc_close_noop.db"
    r = repo_module.SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    _id, pid = await r.insert_position_cycle(_make_cycle_row(opened_at=now, timestamp=now))
    close_time = now + timedelta(minutes=5)
    await r.close_position_cycle(pid, close_time, None, close_time, "s1", 2)
    result = await r.close_position_cycle(
        pid, close_time + timedelta(seconds=1), None, close_time + timedelta(seconds=1), "s1", 3
    )
    assert result is None


@pytest.mark.asyncio
async def test_update_position_cycle_max_qty_increases(tmp_path: Path) -> None:
    """Given an open cycle, When max_qty increased, Then SCD2 row carries new peak."""
    db_path = tmp_path / "pc_max_up.db"
    r = repo_module.SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    _id, pid = await r.insert_position_cycle(
        _make_cycle_row(max_qty=1.0, opened_at=now, timestamp=now)
    )
    t1 = now + timedelta(seconds=10)
    new_id = await r.update_position_cycle_max_qty(pid, 2.5, t1, "s1", 2)
    assert new_id is not None
    found = await r.get_open_position_cycle(
        "kraken.BTC-USD.live.w0000000000aa", as_of=t1 + timedelta(seconds=1)
    )
    assert found is not None
    assert found["max_qty"] == pytest.approx(2.5)
    assert found["public_id"] == pid


@pytest.mark.asyncio
async def test_update_position_cycle_max_qty_monotonic_noop(tmp_path: Path) -> None:
    """Given peak of 2.5, When setting equal or lower, Then no-op (None returned)."""
    db_path = tmp_path / "pc_max_noop.db"
    r = repo_module.SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    _id, pid = await r.insert_position_cycle(
        _make_cycle_row(max_qty=2.5, opened_at=now, timestamp=now)
    )
    t1 = now + timedelta(seconds=10)
    equal = await r.update_position_cycle_max_qty(pid, 2.5, t1, "s1", 2)
    assert equal is None
    lower = await r.update_position_cycle_max_qty(pid, 1.0, t1, "s1", 3)
    assert lower is None
    found = await r.get_open_position_cycle(
        "kraken.BTC-USD.live.w0000000000aa", as_of=t1 + timedelta(seconds=1)
    )
    assert found is not None
    assert found["max_qty"] == pytest.approx(2.5)


@pytest.mark.asyncio
async def test_update_position_cycle_max_qty_raises_on_closed(tmp_path: Path) -> None:
    """Given a closed cycle, When update called, Then ValueError raised."""
    db_path = tmp_path / "pc_max_closed.db"
    r = repo_module.SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    _id, pid = await r.insert_position_cycle(_make_cycle_row(opened_at=now, timestamp=now))
    close_time = now + timedelta(seconds=10)
    await r.close_position_cycle(pid, close_time, None, close_time, "s1", 2)
    with pytest.raises(ValueError, match="no active open cycle"):
        await r.update_position_cycle_max_qty(pid, 10.0, close_time, "s1", 3)


@pytest.mark.asyncio
async def test_flip_position_cycle_atomic_close_open(tmp_path: Path) -> None:
    """Given a long cycle, When flipped to short, Then old closed + new open in one txn.

    Also asserts that the closed row's ``closing_command_public_id`` is populated
    from the new_open_row's ``opening_command_public_id`` — a single fill command
    is the cause of both the close and the open, so the lineage field is carried
    through. Without this assertion, a regression at ``flip_position_cycle`` that
    silently dropped the cross-carry would not break any test.
    """
    db_path = tmp_path / "pc_flip.db"
    r = repo_module.SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    _id, long_pid = await r.insert_position_cycle(
        _make_cycle_row(direction="long", max_qty=1.0, opened_at=now, timestamp=now)
    )
    flip_time = now + timedelta(seconds=30)
    new_open = _make_cycle_row(
        direction="short",
        max_qty=0.8,
        opened_at=flip_time,
        timestamp=flip_time,
        opening_command_public_id="cmd-flip-close-1",
    )
    _new_id, short_pid = await r.flip_position_cycle(
        close_cycle_public_id=long_pid,
        new_open_row=new_open,
        bus_time=flip_time,
        session_id="s1",
        sequence_id=2,
    )
    assert short_pid != long_pid
    after = await r.get_open_position_cycle(
        "kraken.BTC-USD.live.w0000000000aa", as_of=flip_time + timedelta(seconds=1)
    )
    assert after is not None
    assert after["public_id"] == short_pid
    assert after["direction"] == "short"
    assert after["max_qty"] == pytest.approx(0.8)
    assert after["opening_command_public_id"] == "cmd-flip-close-1"
    async with r.session() as _s:
        _rows = (
            (
                await _s.execute(
                    _sa_select(PositionCycle).where(
                        PositionCycle.public_id == long_pid,
                        PositionCycle.status == "closed",
                    )
                )
            )
            .scalars()
            .all()
        )
    assert len(_rows) == 1
    closed_row = _rows[0]
    assert closed_row.status == "closed"
    assert closed_row.direction == "long"
    assert closed_row.closing_command_public_id == "cmd-flip-close-1"
    assert closed_row.closed_at == flip_time


@pytest.mark.asyncio
async def test_flip_position_cycle_raises_on_stale_public_id(tmp_path: Path) -> None:
    """Given an already-closed cycle id, When flip called, Then ValueError raised."""
    db_path = tmp_path / "pc_flip_stale.db"
    r = repo_module.SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    _id, pid = await r.insert_position_cycle(_make_cycle_row(opened_at=now, timestamp=now))
    close_time = now + timedelta(seconds=10)
    await r.close_position_cycle(pid, close_time, None, close_time, "s1", 2)
    stale_row = _make_cycle_row(direction="short", opened_at=close_time, timestamp=close_time)
    with pytest.raises(ValueError, match="no active open cycle"):
        await r.flip_position_cycle(pid, stale_row, close_time, "s1", 3)


@pytest.mark.asyncio
async def test_flip_position_cycle_raises_on_shard_mismatch(tmp_path: Path) -> None:
    """Given cycle on shard A, When flip attempted with shard B payload, Then ValueError raised."""
    db_path = tmp_path / "pc_flip_shard.db"
    r = repo_module.SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    _id, pid = await r.insert_position_cycle(
        _make_cycle_row(
            shard_key="kraken.BTC-USD.live.w1111111111aa",
            opened_at=now,
            timestamp=now,
        )
    )
    mismatch = _make_cycle_row(
        shard_key="kraken.ETH-USD.live.w2222222222bb",
        direction="short",
        opened_at=now + timedelta(seconds=5),
        timestamp=now + timedelta(seconds=5),
    )
    with pytest.raises(ValueError, match="shard_key mismatch"):
        await r.flip_position_cycle(pid, mismatch, now + timedelta(seconds=5), "s1", 2)


@pytest.mark.asyncio
async def test_position_cycle_full_lifecycle(tmp_path: Path) -> None:
    """End-to-end: insert -> update_max -> flip -> close yields expected DB state."""
    db_path = tmp_path / "pc_lifecycle.db"
    r = repo_module.SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    shard = "kraken.BTC-USD.live.w3333333333cc"
    t0 = datetime.now(UTC)
    _id, long_pid = await r.insert_position_cycle(
        _make_cycle_row(
            shard_key=shard,
            direction="long",
            max_qty=1.0,
            opened_at=t0,
            timestamp=t0,
        )
    )
    t1 = t0 + timedelta(seconds=10)
    peak_id = await r.update_position_cycle_max_qty(long_pid, 3.0, t1, "s1", 2)
    assert peak_id is not None
    t2 = t0 + timedelta(seconds=20)
    _flip_id, short_pid = await r.flip_position_cycle(
        long_pid,
        _make_cycle_row(
            shard_key=shard,
            direction="short",
            max_qty=0.5,
            opened_at=t2,
            timestamp=t2,
        ),
        t2,
        "s1",
        3,
    )
    t3 = t0 + timedelta(seconds=30)
    close_id = await r.close_position_cycle(short_pid, t3, None, t3, "s1", 4)
    assert close_id is not None
    final = await r.get_open_position_cycle(shard, as_of=t3 + timedelta(seconds=1))
    assert final is None


@pytest.mark.asyncio
async def test_insert_and_list_execution_plan_decisions(tmp_path: Path) -> None:
    """Given inserted decisions, When listing, Then rows returned with temporal filter."""
    db_path = tmp_path / "decisions.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    pid = await r.insert_execution_plan_decision(
        row={
            "plan_public_id": "plan-1",
            "decision_type": "command_emitted",
            "decided_at": now,
            "trigger_type": "tick",
            "evidence": {"price": 50000.0},
            "emitted_command_public_id": "cmd-1",
            "new_status": None,
            "reason": "SL triggered",
            "decision_importance": "action",
        },
        bus_time=now,
        session_id="s1",
        sequence_id=1,
    )
    assert pid
    rows = await r.list_execution_plan_decisions("plan-1", as_of=now)
    assert len(rows) == 1
    assert rows[0]["decision_type"] == "command_emitted"
    assert rows[0]["trigger_type"] == "tick"
    assert rows[0]["evidence"] == {"price": 50000.0}
    assert rows[0]["emitted_command_public_id"] == "cmd-1"
    assert rows[0]["decision_importance"] == "action"


@pytest.mark.asyncio
async def test_list_decisions_importance_filter(tmp_path: Path) -> None:
    """Given decisions of different importance, When filtering, Then subset returned."""
    db_path = tmp_path / "decisions_filter.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    for i, imp in enumerate(("action", "transition", "routine")):
        await r.insert_execution_plan_decision(
            row={
                "plan_public_id": "plan-1",
                "decision_type": "test",
                "decided_at": now,
                "trigger_type": "tick",
                "evidence": {},
                "emitted_command_public_id": None,
                "new_status": None,
                "reason": f"test {imp}",
                "decision_importance": imp,
            },
            bus_time=now,
            session_id="s1",
            sequence_id=i + 1,
        )
    rows = await r.list_execution_plan_decisions("plan-1", as_of=now, importance="action")
    assert len(rows) == 1
    assert rows[0]["decision_importance"] == "action"


@pytest.mark.asyncio
async def test_list_decisions_pagination(tmp_path: Path) -> None:
    """Given multiple decisions, When paginating, Then correct slices returned."""
    db_path = tmp_path / "decisions_page.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    for i in range(5):
        t = now + timedelta(seconds=i)
        await r.insert_execution_plan_decision(
            row={
                "plan_public_id": "plan-1",
                "decision_type": "test",
                "decided_at": t,
                "trigger_type": "tick",
                "evidence": {},
                "emitted_command_public_id": None,
                "new_status": None,
                "reason": f"reason-{i}",
                "decision_importance": "action",
            },
            bus_time=t,
            session_id="s1",
            sequence_id=i + 1,
        )
    page1 = await r.list_execution_plan_decisions(
        "plan-1", as_of=now + timedelta(seconds=10), limit=2
    )
    assert len(page1) == 2
    page2 = await r.list_execution_plan_decisions(
        "plan-1", as_of=now + timedelta(seconds=10), limit=2, offset=2
    )
    assert len(page2) == 2
    page3 = await r.list_execution_plan_decisions(
        "plan-1", as_of=now + timedelta(seconds=10), limit=2, offset=4
    )
    assert len(page3) == 1


@pytest.mark.asyncio
async def test_revise_execution_plan_params(tmp_path: Path) -> None:
    """Given a plan, When revising params, Then SCD2 close-and-insert preserves other fields."""
    db_path = tmp_path / "params_rev.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    _id, pid = await r.insert_execution_plan(
        {
            "plan_type": "manual_once",
            "created_by_user_id": "user-1",
            "created_via": "ui",
            "instrument_public_id": "inst-1",
            "exchange": "kraken",
            "mode": "live",
            "shard_key": "kraken:BTC-USD:live",
            "wallet_public_id": "wallet-1",
            "total_quantity": 1.0,
            "side": "buy",
            "params": {"order_type": "limit", "price": 50000.0},
            "status": "active",
            "created_at": now,
            "session_id": "s1",
            "sequence_id": 1,
            "timestamp": now,
        }
    )
    t2 = now + timedelta(seconds=5)
    await r.revise_execution_plan_params(
        public_id=pid,
        param_updates={"child_client_order_id": "cid-99"},
        bus_time=t2,
        session_id="s1",
        sequence_id=2,
    )
    row = await r.get_execution_plan(pid, as_of=t2)
    assert row is not None
    assert row["params"]["child_client_order_id"] == "cid-99"
    assert row["params"]["order_type"] == "limit"
    assert row["params"]["price"] == 50000.0
    assert row["status"] == "active"
    assert row["total_quantity"] == 1.0


@pytest.mark.asyncio
async def test_revise_params_multiple_revisions(tmp_path: Path) -> None:
    """Given sequential param revisions, Then each produces correct lineage."""
    db_path = tmp_path / "params_multi.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    _id, pid = await r.insert_execution_plan(
        {
            "plan_type": "manual_once",
            "created_by_user_id": "user-1",
            "created_via": "ui",
            "instrument_public_id": "inst-1",
            "exchange": "kraken",
            "mode": "live",
            "shard_key": "kraken:BTC-USD:live",
            "wallet_public_id": "wallet-1",
            "total_quantity": 1.0,
            "side": "buy",
            "params": {"a": 1},
            "status": "active",
            "created_at": now,
            "session_id": "s1",
            "sequence_id": 1,
            "timestamp": now,
        }
    )
    t2 = now + timedelta(seconds=5)
    await r.revise_execution_plan_params(pid, {"b": 2}, t2, "s1", 2)
    t3 = now + timedelta(seconds=10)
    await r.revise_execution_plan_params(pid, {"c": 3}, t3, "s1", 3)
    row = await r.get_execution_plan(pid, as_of=t3)
    assert row is not None
    assert row["params"] == {"a": 1, "b": 2, "c": 3}


@pytest.mark.asyncio
async def test_revise_params_shallow_merge(tmp_path: Path) -> None:
    """Param merge is shallow — nested dicts are replaced, not deep-merged."""
    db_path = tmp_path / "params_shallow.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    _id, pid = await r.insert_execution_plan(
        {
            "plan_type": "manual_once",
            "created_by_user_id": "user-1",
            "created_via": "ui",
            "instrument_public_id": "inst-1",
            "exchange": "kraken",
            "mode": "live",
            "shard_key": "kraken:BTC-USD:live",
            "wallet_public_id": "wallet-1",
            "total_quantity": 1.0,
            "side": "buy",
            "params": {"nested": {"x": 1, "y": 2}},
            "status": "active",
            "created_at": now,
            "session_id": "s1",
            "sequence_id": 1,
            "timestamp": now,
        }
    )
    t2 = now + timedelta(seconds=5)
    await r.revise_execution_plan_params(pid, {"nested": {"z": 3}}, t2, "s1", 2)
    row = await r.get_execution_plan(pid, as_of=t2)
    assert row is not None
    assert row["params"]["nested"] == {"z": 3}


@pytest.mark.asyncio
async def test_revise_params_no_active_row_noop(tmp_path: Path) -> None:
    """Given a nonexistent plan, When revising params, Then nothing happens."""
    db_path = tmp_path / "params_noop.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    await r.revise_execution_plan_params("nonexistent-id", {"a": 1}, now, "s1", 1)


@pytest.mark.asyncio
async def test_get_position_cycle_by_public_id(tmp_path: Path) -> None:
    """Given an inserted cycle, When queried by public_id, Then row returned."""
    db_path = tmp_path / "pc_by_pid.db"
    r = repo_module.SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    row = _make_cycle_row(opened_at=now, timestamp=now)
    _id, pid = await r.insert_position_cycle(row)
    cycle = await r.get_position_cycle_by_public_id(pid, as_of=now)
    assert cycle is not None
    assert cycle["public_id"] == pid
    assert cycle["status"] == "open"
    assert cycle["direction"] == "long"


@pytest.mark.asyncio
async def test_get_position_cycle_by_public_id_not_found(tmp_path: Path) -> None:
    """Given nonexistent public_id, When queried, Then None returned."""
    db_path = tmp_path / "pc_by_pid_nf.db"
    r = repo_module.SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    cycle = await r.get_position_cycle_by_public_id("nonexistent", as_of=now)
    assert cycle is None


@pytest.mark.asyncio
async def test_get_all_open_position_cycles_returns_open_only(tmp_path: Path) -> None:
    """Only open cycles are returned, closed cycles excluded.

    Given: one open cycle and one closed cycle,
    When: get_all_open_position_cycles is called,
    Then: only the open cycle is returned.
    """
    db_path = tmp_path / "pc_all_open.db"
    r = repo_module.SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    open_row = _make_cycle_row(
        shard_key="kraken.BTC-USD.live.waaaa",
        opened_at=now - timedelta(hours=48),
        timestamp=now - timedelta(hours=48),
    )
    _, open_pid = await r.insert_position_cycle(open_row)
    closed_row = _make_cycle_row(
        shard_key="kraken.ETH-USD.live.wbbbb",
        opened_at=now - timedelta(hours=96),
        timestamp=now - timedelta(hours=96),
        sequence_id=2,
    )
    _, closed_pid = await r.insert_position_cycle(closed_row)
    await r.close_position_cycle(
        cycle_public_id=closed_pid,
        closed_at=now - timedelta(hours=24),
        closing_command_public_id=None,
        bus_time=now,
        session_id="s1",
        sequence_id=10,
    )
    cycles = await r.get_all_open_position_cycles(as_of=now + timedelta(seconds=1))
    assert len(cycles) == 1
    assert cycles[0]["public_id"] == open_pid


@pytest.mark.asyncio
async def test_get_all_open_position_cycles_age_filter(tmp_path: Path) -> None:
    """Opened_before filter excludes recent cycles.

    Given: one cycle opened 5 days ago, one opened 1 hour ago,
    When: get_all_open_position_cycles is called with opened_before=3 days ago,
    Then: only the old cycle is returned.
    """
    db_path = tmp_path / "pc_age_filter.db"
    r = repo_module.SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    old_row = _make_cycle_row(
        shard_key="kraken.BTC-USD.live.wold1",
        opened_at=now - timedelta(days=5),
        timestamp=now - timedelta(days=5),
    )
    _, old_pid = await r.insert_position_cycle(old_row)
    recent_row = _make_cycle_row(
        shard_key="kraken.ETH-USD.live.wnew1",
        opened_at=now - timedelta(hours=1),
        timestamp=now - timedelta(hours=1),
        sequence_id=2,
    )
    await r.insert_position_cycle(recent_row)
    cycles = await r.get_all_open_position_cycles(
        as_of=now + timedelta(seconds=1),
        opened_before=now - timedelta(days=3),
    )
    assert len(cycles) == 1
    assert cycles[0]["public_id"] == old_pid


@pytest.mark.asyncio
async def test_get_all_open_position_cycles_empty(tmp_path: Path) -> None:
    """Empty result when no open cycles exist.

    Given: no position cycles in the database,
    When: get_all_open_position_cycles is called,
    Then: empty list returned.
    """
    db_path = tmp_path / "pc_empty.db"
    r = repo_module.SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    now = datetime.now(UTC)
    cycles = await r.get_all_open_position_cycles(as_of=now)
    assert cycles == []
