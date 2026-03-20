"""Tests for Repository pattern implementations."""

import asyncio
from collections.abc import AsyncIterator
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
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker

import snapper.data.repository
import snapper.data.repository as repo
import snapper.data.repository as repository
from snapper.data import repository as repo_module
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import MarketSnapshot
from snapper.data.models import Symbol
from snapper.data.repository import DatabaseRepository
from snapper.data.repository import Repository
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import dispose_repositories
from snapper.data.repository import get_repository
from snapper.data.repository import where_active
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
            "instrument_id": 1,
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
            "instrument_id": 1,
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
            "instrument_id": 1,
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
            "instrument_id": 1,
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
async def test_upsert_candles_generates_timestamp_when_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify upsert_candles generates timestamp when not provided.

    Given: A row without a 'timestamp' key,
    When: upsert_candles is called,
    Then: The row gets a generated UTC timestamp.
    """
    added_objects: list[Any] = []

    async def _execute(stmt: Any) -> Any:
        return SimpleNamespace(scalars=lambda: SimpleNamespace(first=lambda: None))

    session = _DummyAsyncSession()
    session.execute = _execute
    session.add = lambda obj: added_objects.append(obj)
    repo = _make_repo(lambda: _session_factory(session), dialect="custom")
    before = datetime.now(UTC)
    rows = [
        {
            "instrument_id": 1,
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
        },
    ]
    await repo.upsert_candles(rows)
    assert isinstance(rows[0]["timestamp"], datetime)
    assert rows[0]["timestamp"] >= before


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
    spid = await resolve_symbol_public_id(repo, "BTC-USD")
    assert spid is not None
    instrument_payload = {
        "symbol_public_id": spid,
        "symbol": "BTC-USD",
        "base": "BTC",
        "quote": "USD",
        "exchange": "kraken",
        "tick_size": 0.01,
        "lot_size": 0.001,
    }
    instrument_id = await repo.upsert_instrument(
        **instrument_payload, session_id="test-session", sequence_id=1
    )
    duplicate_id = await repo.upsert_instrument(
        **instrument_payload, session_id="test-session", sequence_id=1
    )
    assert duplicate_id == instrument_id
    base_ts = datetime.now(UTC) - timedelta(minutes=10)
    candle_rows = [
        {
            "instrument_id": instrument_id,
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
            "instrument_id": instrument_id,
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
            "instrument_id": instrument_id,
            "timestamp": base_ts,
            "price": 10.5,
            "size": 0.25,
            "side": "buy",
            "trade_id": "t1",
            "session_id": "test-session",
            "sequence_id": 1,
        },
        {
            "instrument_id": instrument_id,
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
    )
    assert len(candle_results) == 2
    trade_results = await repo.get_trades(
        "BTC-USD",
        base_ts - timedelta(minutes=1),
        base_ts + timedelta(minutes=2),
        exchange="kraken",
    )
    assert len(trade_results) == 2
    order_id, order_public_id = await repo.insert_order(
        instrument_id=instrument_id,
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
    )
    order_v2 = await repo.update_order(
        order_id=order_id,
        status="partially_filled",
        updated_at=base_ts + timedelta(minutes=1),
        session_id="",
        sequence_id=0,
        filled_size=0.5,
        average_price=10.55,
    )
    order_v3 = await repo.update_order(
        order_id=order_v2,
        status="filled",
        updated_at=base_ts + timedelta(minutes=2),
        session_id="",
        sequence_id=0,
        exchange_order_id="ex-1",
        error=None,
    )
    execution_id = await repo.insert_execution(
        order_id=order_v3,
        order_public_id=order_public_id,
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
    async with repo.session() as session:
        stored_snapshot = MarketSnapshot(
            exchange="kraken",
            symbol="BTC-USD",
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
        "kraken",
        ["BTC-USD"],
        base_ts - timedelta(seconds=1),
        base_ts + timedelta(seconds=1),
    )
    assert len(snapshots) == 1
    result_snapshot = snapshots[0]
    normalized_ts = (
        result_snapshot["ts"]
        if result_snapshot["ts"].tzinfo
        else result_snapshot["ts"].replace(tzinfo=UTC)
    )
    assert normalized_ts == base_ts
    assert result_snapshot["symbol"] == "BTC-USD"
    assert result_snapshot["exchange"] == "kraken"
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

    async def upsert_instrument(self, **kwargs: Any) -> int:
        """Upsert instrument - no-op returning 0."""
        return 0

    async def get_latest_candle_ids(self) -> dict[tuple[int, str], tuple[datetime, str]]:
        """Load latest candle IDs - returns empty dict for dummy."""
        return {}

    async def upsert_candles(self, rows: list[dict[str, Any]]) -> int:
        """Upsert candles - no-op returning 0."""
        return 0

    async def upsert_trades(self, rows: list[dict[str, Any]]) -> int:
        """Upsert trades - no-op returning 0."""
        return 0

    async def insert_order(
        self,
        instrument_id: int,
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
        time_in_force: str | None = None,
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
        exchange_order_id: str | None = None,
        err: str | None = None,
    ) -> int:
        """Update order - no-op returning 0."""
        return 0

    async def insert_execution(
        self,
        order_id: int,
        order_public_id: str,
        ts: datetime,
        price: float,
        size: float,
        fee: float,
        fee_asset: str,
        exec_id: str | None = None,
        trade_id: str | None = None,
    ) -> int:
        """Insert execution - no-op returning 0."""
        return 0

    async def get_candles(
        self,
        instrument: str,
        timeframe: str,
        start: datetime,
        end: datetime,
        exchange: str,
    ) -> list[dict[str, Any]]:
        """Get candles - returns empty list."""
        return []

    async def get_trades(
        self,
        instrument: str,
        start: datetime,
        end: datetime,
        exchange: str,
    ) -> list[dict[str, Any]]:
        """Get trades - returns empty list."""
        return []

    async def get_market_snapshots(
        self, exchange: str, symbols: list[str], start: datetime, end: datetime
    ) -> list[dict[str, Any]]:
        """Get market snapshots - returns empty list."""
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


def test_database_repository_del_without_engine() -> None:
    """Test DatabaseRepository.__del__ tolerates missing engine attribute.

    Given: A partially initialized repository without an engine,
    When: __del__ is invoked,
    Then: No exception is raised.
    """
    repo = DatabaseRepository.__new__(DatabaseRepository)
    DatabaseRepository.__del__(repo)


def test_sqlalchemy_repository_del_without_engine() -> None:
    """Test SQLAlchemyRepository.__del__ tolerates missing engine attribute.

    Given: A partially initialized async repository without an engine,
    When: __del__ is invoked,
    Then: No exception is raised.
    """
    repo = SQLAlchemyRepository.__new__(SQLAlchemyRepository)
    SQLAlchemyRepository.__del__(repo)


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
                    "instrument_id": 1,
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
                    "instrument_id": 1,
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
                    "instrument_id": 1,
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
                    "instrument_id": 1,
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
                    "instrument_id": 1,
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
                    "instrument_id": 1,
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
        When: upsert_candles/upsert_trades is called,
        Then: Returns 0 without database operation.
        """
        result_candles = await mock_postgres_repo.upsert_candles([])
        assert result_candles == 0
        result_trades = await mock_postgres_repo.upsert_trades([])
        assert result_trades == 0

    @pytest.mark.asyncio
    async def test_upsert_instrument_with_integrity_error(
        self, mock_postgres_repo: SQLAlchemyRepository
    ) -> None:
        """Verify upsert_instrument handles duplicate key gracefully.

        Given: No active instrument found, insert hits IntegrityError (race),
        When: upsert_instrument is called,
        Then: Retries lookup and returns existing instrument ID.
        """
        mock_session = AsyncMock()
        mock_result = Mock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result
        mock_instrument = Mock()
        mock_instrument.id = 123
        mock_session.add = Mock()
        mock_session.commit.side_effect = [IntegrityError("duplicate", "params", Exception()), None]
        mock_result2 = Mock()
        mock_result2.scalar_one_or_none.return_value = mock_instrument
        mock_session.execute.side_effect = [mock_result, mock_result2]
        mock_session.refresh = AsyncMock()
        with patch.object(mock_postgres_repo, "session") as mock_session_ctx:
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            mock_session_ctx.return_value.__aexit__.return_value = None
            result = await mock_postgres_repo.upsert_instrument(
                symbol_public_id="fake-spid",
                symbol="BTC-USD",
                base="BTC",
                quote="USD",
                exchange="kraken",
                tick_size=0.01,
                lot_size=0.001,
                session_id="test-session",
                sequence_id=1,
            )
            assert result == 123
            mock_session.rollback.assert_called_once()

    @pytest.mark.asyncio
    async def test_upsert_instrument_integrity_error_reraise(
        self, mock_postgres_repo: SQLAlchemyRepository
    ) -> None:
        """Verify upsert_instrument re-raises when retry also finds nothing.

        Given: Instrument not found before or after IntegrityError,
        When: upsert_instrument is called,
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
                await mock_postgres_repo.upsert_instrument(
                    symbol_public_id="fake-spid",
                    symbol="BTC-USD",
                    base="BTC",
                    quote="USD",
                    exchange="kraken",
                    tick_size=0.01,
                    lot_size=0.001,
                    session_id="test-session",
                    sequence_id=1,
                )
            mock_session.rollback.assert_called_once()

    @pytest.mark.asyncio
    async def test_upsert_instrument_existing(
        self, mock_postgres_repo: SQLAlchemyRepository
    ) -> None:
        """Verify upsert_instrument returns existing ID when payload matches.

        Given: Active instrument with identical payload exists,
        When: upsert_instrument is called,
        Then: Returns existing ID, skips add.
        """
        mock_session = AsyncMock()
        mock_result = Mock()
        mock_instrument = Mock()
        mock_instrument.id = 456
        mock_instrument.symbol = "ETH-USD"
        mock_instrument.base = "ETH"
        mock_instrument.quote = "USD"
        mock_result.scalar_one_or_none.return_value = mock_instrument
        mock_session.execute.return_value = mock_result
        with patch.object(mock_postgres_repo, "session") as mock_session_ctx:
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            mock_session_ctx.return_value.__aexit__.return_value = None
            result = await mock_postgres_repo.upsert_instrument(
                symbol_public_id="fake-spid",
                symbol="ETH-USD",
                base="ETH",
                quote="USD",
                exchange="kraken",
                tick_size=0.01,
                lot_size=0.001,
                session_id="test-session",
                sequence_id=1,
            )
            assert result == 456
            mock_session.add.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_candles_instrument_not_found(
        self, mock_postgres_repo: SQLAlchemyRepository
    ) -> None:
        """Verify get_candles returns empty list for unknown instrument.

        Given: Non-existent instrument,
        When: get_candles is called,
        Then: Returns empty list.
        """
        mock_session = AsyncMock()
        mock_result = Mock()
        mock_scalars = Mock()
        mock_scalars.first.return_value = None
        mock_result.scalars.return_value = mock_scalars
        mock_session.execute.return_value = mock_result
        with patch.object(mock_postgres_repo, "session") as mock_session_ctx:
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            mock_session_ctx.return_value.__aexit__.return_value = None
            start = datetime(2024, 1, 1, tzinfo=UTC)
            end = datetime(2024, 1, 1, 1, 0, tzinfo=UTC)
            result = await mock_postgres_repo.get_candles(
                "NONEXISTENT", "1m", start, end, exchange="kraken"
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
        mock_candles_result.all.return_value = [mock_row]
        mock_session.execute.side_effect = [mock_inst_result, mock_candles_result]
        with patch.object(mock_postgres_repo, "session") as mock_session_ctx:
            mock_session_ctx.return_value.__aenter__.return_value = mock_session
            mock_session_ctx.return_value.__aexit__.return_value = None
            start = datetime(2024, 1, 1, tzinfo=UTC)
            end = datetime(2024, 1, 1, 1, 0, tzinfo=UTC)
            result = await mock_postgres_repo.get_candles(
                "BTC-USD", "1m", start, end, exchange="kraken"
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
                "BTC-USD", "1m", start, end, exchange="kraken"
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
    mock_execute_result = SimpleNamespace(scalars=lambda: SimpleNamespace(first=lambda: None))

    async def _execute(*_: object, **__: object) -> SimpleNamespace:
        return mock_execute_result

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
    )
    assert result == []


@pytest.mark.asyncio()
async def test_get_trades_with_exchange_filter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify get_trades filters by exchange when provided."""
    with patch("snapper.data.repository.create_async_engine"):
        repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    mock_execute_result = SimpleNamespace(scalars=lambda: SimpleNamespace(first=lambda: None))

    async def _execute(*_: object, **__: object) -> SimpleNamespace:
        return mock_execute_result

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

    async def get_latest_candle_ids(self) -> dict[tuple[int, str], tuple[datetime, str]]:
        return {}

    async def upsert_candles(self, rows: list[dict[str, Any]]) -> int:
        return 0

    async def upsert_trades(self, rows: list[dict[str, Any]]) -> int:
        return 0

    async def upsert_instrument(self, **kwargs: Any) -> int:
        return 0

    async def insert_order(
        self,
        instrument_id: int,
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
        time_in_force: str | None = None,
    ) -> tuple[int, str]:
        return (0, "stub-public-id")

    async def update_order(
        self,
        order_id: int,
        status: str,
        updated_at: datetime,
        session_id: str,
        sequence_id: int,
        exchange_order_id: str | None = None,
        error: str | None = None,
    ) -> int:
        """Update order - no-op returning 0."""
        return 0

    async def insert_execution(
        self,
        order_id: int,
        order_public_id: str,
        timestamp: datetime,
        price: float,
        size: float,
        fee: float,
        fee_asset: str,
        exec_id: str | None = None,
        trade_id: str | None = None,
    ) -> int:
        return 0

    async def get_candles(
        self,
        instrument: str,
        timeframe: str,
        start: datetime,
        end: datetime,
        exchange: str,
    ) -> list[dict[str, Any]]:
        return []

    async def get_trades(
        self,
        instrument: str,
        start: datetime,
        end: datetime,
        exchange: str,
    ) -> list[dict[str, Any]]:
        return []

    async def get_market_snapshots(
        self,
        exchange: str,
        symbols: list[str],
        start: datetime,
        end: datetime,
    ) -> list[dict[str, Any]]:
        return []


class TestWhereActive:
    """Tests for the where_active temporal filter helper."""

    def test_where_active_returns_two_clauses(self) -> None:
        """where_active returns a tuple of exactly two filter clauses.

        Given: A model with timestamp and known_to columns,
        When: where_active is called,
        Then: Returns a tuple of two SQLAlchemy filter expressions.
        """
        clauses = where_active(MarketSnapshot)
        assert len(clauses) == 2

    def test_where_active_uses_now_by_default(self) -> None:
        """where_active uses current time when no explicit time is given.

        Given: A model with temporal columns,
        When: where_active is called without an 'at' argument,
        Then: The filter clauses reference the current UTC time.
        """
        clauses = where_active(MarketSnapshot)
        ts_clause, known_to_clause = clauses
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
