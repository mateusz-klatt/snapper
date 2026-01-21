"""Tests for Walutomat FX market data snapshot service."""

import asyncio
import math
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Coroutine
from datetime import UTC
from datetime import datetime
from types import SimpleNamespace
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.data.models import MarketSnapshot
from snapper.data.repository import DatabaseRepository
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.implementations.walutomat import WalutomatExchangeClient
from snapper.infrastructure.market_data import walutomat as module
from snapper.infrastructure.market_data.walutomat import WalutomatSnapshotUpdaterService
from snapper.infrastructure.market_data.walutomat import _async_update_snapshots
from snapper.infrastructure.market_data.walutomat import run_walutomat_snapshot_update


class DummyTicker(SimpleNamespace):
    """Dummy ticker for testing market data processing."""

    pass


class DummyClient(WalutomatExchangeClient):
    """Dummy Walutomat client for testing."""

    def __init__(self, ticks: list[Any]) -> None:
        """Initialize the instance."""
        super().__init__()
        self._ticks = ticks

    def get_supported_pairs(self) -> list[str]:
        """Return hardcoded supported pairs."""
        return ["EUR-PLN", "USD-PLN"]

    async def subscribe_ticks(self, symbols: list[str]) -> AsyncIterator[DummyTicker]:
        """Yield configured tickers."""
        for t in self._ticks:
            yield t


class DummyRepo(SimpleNamespace):
    """Dummy repository for testing database operations."""

    def __init__(self) -> None:
        """Initialize the instance."""
        super().__init__()
        self.saved: list[Any] = []
        self.committed = False

    def session_factory(self) -> "DummyRepo":
        """Return self as session."""
        return self

    def __enter__(self) -> "DummyRepo":
        """Enter the context manager."""
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """Exit the context manager."""
        pass

    def bulk_save_objects(self, objs: list[Any]) -> None:
        """Store objects in saved list."""
        self.saved.extend(objs)

    def commit(self) -> None:
        """Mark as committed."""
        self.committed = True


@pytest.mark.asyncio
async def test_collect_snapshots_loop_stops_after_all(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stop loop after collecting all symbols.

    Given: Client yielding ticker for requested symbol,
    When: _collect_snapshots_loop is called,
    Then: Snapshot with correct exchange is stored.
    """
    ticker = DummyTicker(
        symbol="EUR-PLN",
        bid=4.0,
        ask=4.1,
        last=4.05,
        high=4.2,
        low=3.9,
        volume=100.0,
        change=0.01,
        vwap=4.0,
        bid_qty=10.0,
        ask_qty=12.0,
    )
    client = DummyClient([ticker])
    repo: Any = DummyRepo()
    svc = WalutomatSnapshotUpdaterService(client, repo)
    snapshots: dict[str, Any] = {}
    await svc._collect_snapshots_loop(["EUR-PLN"], snapshots)
    assert "EUR-PLN" in snapshots
    snap = snapshots["EUR-PLN"]
    assert snap.exchange == "walutomat"


@pytest.mark.asyncio
async def test_update_market_snapshots_handles_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    """Return zero count on timeout.

    Given: Service with slow collection loop,
    When: update_market_snapshots times out,
    Then: Returns count of 0.
    """
    client = DummyClient([])
    repo: Any = DummyRepo()
    svc = WalutomatSnapshotUpdaterService(client, repo)

    async def slow_loop(*args: Any, **kwargs: Any) -> None:
        await asyncio.sleep(1)

    monkeypatch.setattr(svc, "_collect_snapshots_loop", slow_loop)
    count = await svc.update_market_snapshots(timeout_seconds=0.01)
    assert count == 0


@pytest.mark.asyncio
async def test_update_market_snapshots_saves(monkeypatch: pytest.MonkeyPatch) -> None:
    """Persist snapshots to repository.

    Given: Service with mocked collection returning snapshots,
    When: update_market_snapshots is called,
    Then: Snapshots are saved and committed.
    """
    client = DummyClient([])
    repo: Any = DummyRepo()
    svc = WalutomatSnapshotUpdaterService(client, repo)
    snapshots = [SimpleNamespace(), SimpleNamespace()]
    monkeypatch.setattr(svc, "_collect_snapshots_with_timeout", AsyncMock(return_value=snapshots))
    count = await svc.update_market_snapshots(timeout_seconds=1)
    assert count == 2
    assert len(repo.saved) == 2
    assert repo.committed


class BadTicker(SimpleNamespace):
    """Ticker that raises AttributeError on any attribute access."""

    def __getattr__(self, name: str) -> Any:
        """Magic method."""
        raise AttributeError("missing")


@pytest.mark.asyncio
async def test_collect_snapshots_loop_recovers_from_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Continue processing after ticker error.

    Given: Client yielding bad then good ticker,
    When: _collect_snapshots_loop is called,
    Then: Good ticker is processed after error.
    """
    good = DummyTicker(
        symbol="EUR-PLN",
        bid=4.0,
        ask=4.1,
        last=4.05,
        high=4.2,
        low=3.9,
        volume=100.0,
        change=0.01,
        vwap=4.0,
        bid_qty=10.0,
        ask_qty=12.0,
    )
    client = DummyClient([BadTicker(), good])
    repo: Any = DummyRepo()
    svc = WalutomatSnapshotUpdaterService(client, repo)
    snapshots: dict[str, Any] = {}
    await svc._collect_snapshots_loop(["EUR-PLN"], snapshots)
    assert snapshots["EUR-PLN"].symbol == "EUR-PLN"


@pytest.mark.asyncio
async def test_async_update_snapshots_uses_service(monkeypatch: pytest.MonkeyPatch) -> None:
    """Orchestrate service lifecycle correctly.

    Given: Mocked client and service,
    When: _async_update_snapshots is called,
    Then: Connect, start, disconnect are all invoked.
    """
    called = {"start": False, "connect": False, "disconnect": False}

    class DummyClient:
        async def connect(self) -> None:
            called["connect"] = True

        async def disconnect(self) -> None:
            called["disconnect"] = True

    class DummyService:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.called_with = args

        async def start(self) -> None:
            called["start"] = True

    monkeypatch.setattr(module, "get_settings", lambda: SimpleNamespace(db_url="sqlite:///tmp.db"))
    monkeypatch.setattr(module, "DatabaseRepository", lambda _url: SimpleNamespace())
    monkeypatch.setattr(module, "WalutomatExchangeClient", DummyClient)
    monkeypatch.setattr(module, "WalutomatSnapshotUpdaterService", DummyService)
    await module._async_update_snapshots()
    assert all(called.values())


class DummySession:
    """Dummy database session for testing."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.saved: list[MarketSnapshot] = []
        self.committed: bool = False

    def bulk_save_objects(self, objects: list[MarketSnapshot]) -> None:
        """Store objects in saved list."""
        self.saved.extend(objects)

    def commit(self) -> None:
        """Mark as committed."""
        self.committed = True

    def __enter__(self) -> "DummySession":
        """Enter the context manager."""
        return self

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        """Exit the context manager."""
        return None


class DummyRepository:
    """Dummy repository providing session factory."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.session = DummySession()

    def session_factory(self) -> DummySession:
        """Return the dummy session."""
        return self.session


class StubWalutomatClient:
    """Stub Walutomat client for integration testing."""

    def __init__(self, supported_pairs: list[str], tickers: list[TickerUpdate]) -> None:
        """Initialize the instance."""
        self._supported_pairs = supported_pairs
        self._tickers = tickers
        self.subscribe_requests: list[list[str]] = []

    def get_supported_pairs(self) -> list[str]:
        """Return configured supported pairs."""
        return list(self._supported_pairs)

    async def subscribe_ticks(self, symbols: list[str]) -> AsyncIterator[TickerUpdate]:
        """Record request and yield configured tickers."""
        self.subscribe_requests.append(list(symbols))
        for ticker in self._tickers:
            yield ticker


@pytest.mark.asyncio()
async def test_load_all_symbols_returns_sorted_pairs() -> None:
    """Return supported pairs sorted alphabetically.

    Given: Client with unsorted supported pairs,
    When: load_all_symbols is called,
    Then: Returns sorted list of symbols.
    """
    client = StubWalutomatClient(
        [
            "USD-PLN",
            "EUR-PLN",
            "CHF-PLN",
        ],
        tickers=[],
    )
    service = WalutomatSnapshotUpdaterService(
        cast(WalutomatExchangeClient, client),
        cast(DatabaseRepository, DummyRepository()),
    )
    symbols = await service.load_all_symbols()
    assert symbols == ["CHF-PLN", "EUR-PLN", "USD-PLN"]


@pytest.mark.asyncio()
async def test_collect_snapshots_loop_creates_market_snapshots() -> None:
    """Create MarketSnapshot with calculated spread.

    Given: Client yielding ticker with bid/ask,
    When: _collect_snapshots_loop is called,
    Then: Snapshot with spread and spread_pct is stored.
    """
    ticker = TickerUpdate(
        symbol="EUR-PLN",
        bid=4.3,
        bid_qty=1.0,
        ask=4.5,
        ask_qty=1.0,
        last=4.4,
        volume=2500.0,
        vwap=4.4,
        low=4.2,
        high=4.6,
        change=0.1,
        change_pct=2.3,
    )
    client = StubWalutomatClient(["EUR-PLN"], [ticker])
    service = WalutomatSnapshotUpdaterService(
        cast(WalutomatExchangeClient, client),
        cast(DatabaseRepository, DummyRepository()),
    )
    snapshots: dict[str, MarketSnapshot] = {}
    await service._collect_snapshots_loop(["EUR-PLN"], snapshots)
    assert "EUR-PLN" in snapshots
    snapshot = snapshots["EUR-PLN"]
    assert snapshot.exchange == "walutomat"
    assert snapshot.symbol == "EUR-PLN"
    assert math.isclose(snapshot.spread or 0.0, 0.2, rel_tol=1e-9)
    assert math.isclose(snapshot.spread_pct or 0.0, (0.2 / 4.4) * 100, rel_tol=1e-9)


@pytest.mark.asyncio()
async def test_collect_snapshots_with_timeout_handles_asyncio_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return empty list on asyncio timeout.

    Given: Mocked wait_for raising TimeoutError,
    When: _collect_snapshots_with_timeout is called,
    Then: Returns empty list.
    """
    client = StubWalutomatClient(["EUR-PLN"], [])
    service = WalutomatSnapshotUpdaterService(
        cast(WalutomatExchangeClient, client),
        cast(DatabaseRepository, DummyRepository()),
    )

    async def fake_load_all_symbols() -> list[str]:
        return ["EUR-PLN", "USD-PLN"]

    async def fake_wait_for(
        _awaitable: Awaitable[object], *, timeout: int | float | None = None
    ) -> object:
        if hasattr(_awaitable, "close"):
            cast(Any, _awaitable).close()
        raise TimeoutError

    monkeypatch.setattr(service, "load_all_symbols", fake_load_all_symbols)
    monkeypatch.setattr(asyncio, "wait_for", fake_wait_for)
    snapshots = await service._collect_snapshots_with_timeout(timeout_seconds=1)
    assert snapshots == []


@pytest.mark.asyncio()
async def test_update_market_snapshots_persists_bulk_insert(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bulk save and commit snapshots.

    Given: Service with mocked collection returning snapshot,
    When: update_market_snapshots is called,
    Then: Snapshot is saved and session committed.
    """
    client = StubWalutomatClient(["EUR-PLN"], [])
    repository = DummyRepository()
    service = WalutomatSnapshotUpdaterService(
        cast(WalutomatExchangeClient, client),
        cast(DatabaseRepository, repository),
    )
    snapshot = MarketSnapshot(
        exchange="walutomat",
        symbol="EUR-PLN",
        bid=4.3,
        bid_volume=None,
        ask=4.5,
        ask_volume=None,
        last_price=4.4,
        volume_24h=1000.0,
        vwap_24h=4.4,
        low_24h=4.2,
        high_24h=4.6,
        change_24h=0.1,
        spread=0.2,
        spread_pct=0.2 / 4.4 * 100,
        updated_at=datetime.now(UTC),
    )

    async def fake_collect(_timeout: int) -> list[MarketSnapshot]:
        return [snapshot]

    monkeypatch.setattr(
        service,
        "_collect_snapshots_with_timeout",
        fake_collect,
    )
    result = await service.update_market_snapshots(timeout_seconds=9)
    assert result == 1
    assert repository.session.saved == [snapshot]
    assert repository.session.committed is True


@pytest.mark.asyncio()
async def test_update_market_snapshots_propagates_exceptions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Propagate collection errors.

    Given: Service with collection raising RuntimeError,
    When: update_market_snapshots is called,
    Then: RuntimeError is propagated.
    """
    client = StubWalutomatClient(["EUR-PLN"], [])
    service = WalutomatSnapshotUpdaterService(
        cast(WalutomatExchangeClient, client),
        cast(DatabaseRepository, DummyRepository()),
    )

    async def fake_collect(_timeout: int) -> list[MarketSnapshot]:
        raise RuntimeError("database failure")

    monkeypatch.setattr(service, "_collect_snapshots_with_timeout", fake_collect)
    with pytest.raises(RuntimeError, match="database failure"):
        await service.update_market_snapshots(timeout_seconds=5)


@pytest.mark.asyncio()
async def test_async_update_snapshots_manages_lifecycle() -> None:
    """Manage client connection lifecycle.

    Given: Mocked client, service, and repository,
    When: _async_update_snapshots is called,
    Then: Connect, start, disconnect are awaited.
    """
    with (
        patch("snapper.infrastructure.market_data.walutomat.get_settings") as mock_settings,
        patch("snapper.infrastructure.market_data.walutomat.DatabaseRepository") as mock_repo,
        patch(
            "snapper.infrastructure.market_data.walutomat.WalutomatExchangeClient"
        ) as mock_client,
        patch(
            "snapper.infrastructure.market_data.walutomat.WalutomatSnapshotUpdaterService"
        ) as mock_service,
    ):
        mock_settings.return_value = SimpleNamespace(db_url="sqlite:///:memory:")
        mock_repo.return_value = MagicMock()
        client_instance = AsyncMock()
        mock_client.return_value = client_instance
        service_instance = AsyncMock()
        mock_service.return_value = service_instance
        await _async_update_snapshots()
        client_instance.connect.assert_awaited_once()
        service_instance.start.assert_awaited_once()
        client_instance.disconnect.assert_awaited_once()


def test_run_walutomat_snapshot_update_invokes_asyncio_run(monkeypatch: pytest.MonkeyPatch) -> None:
    """Invoke asyncio.run with coroutine.

    Given: Mocked asyncio.run,
    When: run_walutomat_snapshot_update is called,
    Then: Coroutine is passed to asyncio.run.
    """
    captured: dict[str, Coroutine[Any, Any, Any]] = {}

    def fake_run(coro: Coroutine[Any, Any, Any]) -> None:
        captured["coro"] = coro
        coro.close()

    monkeypatch.setattr(asyncio, "run", fake_run)
    run_walutomat_snapshot_update()
    assert "coro" in captured


class _DummyClient:
    """Test dummy for Walutomat client."""

    def __init__(self) -> None:
        self.pairs = ["EUR-PLN"]

    def get_supported_pairs(self) -> list[str]:
        return list(self.pairs)


class _TimeoutUpdater(WalutomatSnapshotUpdaterService):
    """Test updater that simulates timeout during snapshot collection."""

    def __init__(self) -> None:
        self.exchange_client = cast(Any, _DummyClient())
        self.repository = cast(Any, SimpleNamespace())

    async def _collect_snapshots_loop(
        self, all_symbols: list[str], snapshots: dict[str, Any]
    ) -> None:
        await asyncio.sleep(10)

    async def load_all_symbols(self) -> list[str]:
        return self.exchange_client.get_supported_pairs()


@pytest.mark.asyncio()
async def test_collect_snapshots_handles_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    """Return empty list on timeout.

    Given: Mocked wait_for raising TimeoutError,
    When: _collect_snapshots_with_timeout is called,
    Then: Returns empty list.
    """
    updater = _TimeoutUpdater()

    async def fake_wait_for(coro: Any, timeout: int) -> Any:
        task = asyncio.create_task(coro)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        raise TimeoutError

    monkeypatch.setattr(
        "snapper.infrastructure.market_data.walutomat.asyncio.wait_for",
        fake_wait_for,
    )
    snapshots = await updater._collect_snapshots_with_timeout(timeout_seconds=1)
    assert snapshots == []


class _FaultyTicker:
    """Test ticker that raises error on bid access."""

    def __init__(self) -> None:
        self.symbol = "EUR-PLN"

    @property
    def bid(self) -> float:
        raise ValueError("bad bid")


class _FaultyClient:
    """Test client that yields faulty tickers."""

    def get_supported_pairs(self) -> list[str]:
        return ["EUR-PLN"]

    async def subscribe_ticks(self, symbols: list[str]) -> AsyncIterator[_FaultyTicker]:
        del symbols
        yield _FaultyTicker()


class _FaultyUpdater(WalutomatSnapshotUpdaterService):
    """Test updater with faulty client for error handling tests."""

    def __init__(self) -> None:
        self.exchange_client = cast(Any, _FaultyClient())
        self.repository = cast(Any, SimpleNamespace())

    async def load_all_symbols(self) -> list[str]:
        return self.exchange_client.get_supported_pairs()


@pytest.mark.asyncio()
async def test_collect_snapshots_loop_handles_processing_errors() -> None:
    """Skip tickers with processing errors.

    Given: Client yielding faulty ticker,
    When: _collect_snapshots_loop is called,
    Then: Snapshots dict remains empty.
    """
    updater = _FaultyUpdater()
    snapshots: dict[str, Any] = {}
    await updater._collect_snapshots_loop(["EUR-PLN"], snapshots)
    assert snapshots == {}


class _HappyTicker:
    """Test ticker with valid market data."""

    def __init__(self) -> None:
        self.symbol = "EUR-PLN"
        self.bid = 4.0
        self.ask = 4.2
        self.last = 4.1
        self.high = 4.3
        self.low = 3.9
        self.volume = 120.0
        self.change = 0.05
        self.vwap = 4.05
        self.bid_qty = 10.0
        self.ask_qty = 11.0


class _HappyClient:
    """Test client that yields valid tickers."""

    def get_supported_pairs(self) -> list[str]:
        return ["EUR-PLN"]

    async def subscribe_ticks(self, symbols: list[str]) -> AsyncIterator[_HappyTicker]:
        del symbols
        yield _HappyTicker()


class _HappyUpdater(WalutomatSnapshotUpdaterService):
    """Test updater with valid client for success path tests."""

    def __init__(self) -> None:
        self.exchange_client = cast(Any, _HappyClient())
        self.repository = cast(Any, SimpleNamespace())

    async def load_all_symbols(self) -> list[str]:
        return self.exchange_client.get_supported_pairs()


@pytest.mark.asyncio()
async def test_collect_snapshots_loop_stops_after_all_symbols() -> None:
    """Stop loop after all symbols received.

    Given: Client yielding ticker for requested symbol,
    When: _collect_snapshots_loop is called,
    Then: Snapshot with correct bid/ask is stored.
    """
    updater = _HappyUpdater()
    snapshots: dict[str, Any] = {}
    await updater._collect_snapshots_loop(["EUR-PLN"], snapshots)
    assert set(snapshots) == {"EUR-PLN"}
    snapshot = snapshots["EUR-PLN"]
    assert snapshot.bid == pytest.approx(4.0)
    assert snapshot.ask == pytest.approx(4.2)


class _DynamicTicker:
    """Test ticker with configurable symbol."""

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self.bid = 5.0
        self.ask = 5.2
        self.last = 5.1
        self.high = 5.3
        self.low = 4.9
        self.volume = 50.0
        self.change = 0.02
        self.vwap = 5.05
        self.bid_qty = 2.0
        self.ask_qty = 3.0


class _MultiClient:
    """Test client that yields multiple symbol tickers."""

    def __init__(self, symbols: list[str]) -> None:
        self._symbols = symbols

    def get_supported_pairs(self) -> list[str]:
        return list(self._symbols)

    async def subscribe_ticks(self, symbols: list[str]) -> AsyncIterator[_DynamicTicker]:
        del symbols
        for symbol in self._symbols:
            yield _DynamicTicker(symbol)


class _MultiUpdater(WalutomatSnapshotUpdaterService):
    """Test updater for multi-symbol tests."""

    def __init__(self, symbols: list[str]) -> None:
        self.exchange_client = cast(Any, _MultiClient(symbols))
        self.repository = cast(Any, SimpleNamespace())

    async def load_all_symbols(self) -> list[str]:
        return self.exchange_client.get_supported_pairs()


@pytest.mark.asyncio()
async def test_collect_snapshots_loop_logs_progress_and_continues() -> None:
    """Process all symbols in large batch.

    Given: Client yielding many tickers,
    When: _collect_snapshots_loop is called,
    Then: All symbols are collected.
    """
    symbols = [f"SYM-{i}" for i in range(12)]
    updater = _MultiUpdater(symbols)
    snapshots: dict[str, Any] = {}
    await updater._collect_snapshots_loop(symbols, snapshots)
    assert set(snapshots) == set(symbols)
