"""Tests for Zonda market data snapshot service."""

import asyncio
import logging
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
from snapper.infrastructure.exchanges.implementations.zonda import ZondaExchangeClient
from snapper.infrastructure.market_data.zonda import ZondaSnapshotUpdaterService
from snapper.infrastructure.market_data.zonda import _async_update_snapshots
from snapper.infrastructure.market_data.zonda import run_zonda_snapshot_update


class DummySession:
    """Dummy database session for testing."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.saved: list[MarketSnapshot] = []
        self.committed = False

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


class StubZondaClient:
    """Stub Zonda client for integration testing."""

    def __init__(self, tickers: list[TickerUpdate]) -> None:
        """Initialize the instance."""
        self._tickers = tickers
        self.requests: list[dict[str, Any]] = []

    async def subscribe_ticks(
        self, symbols: list[str], snapshot: bool = False
    ) -> AsyncIterator[TickerUpdate]:
        """Record request and yield configured tickers."""
        self.requests.append({"symbols": list(symbols), "snapshot": snapshot})
        for ticker in self._tickers:
            yield ticker


@pytest.mark.asyncio()
async def test_load_all_symbols_returns_mapper_data(monkeypatch: pytest.MonkeyPatch) -> None:
    """Load symbols from symbol mapper function.

    Given: Mocked get_available_zonda_symbols returning symbols,
    When: load_all_symbols is called,
    Then: Returns same symbols list.
    """
    symbols = ["BTC-PLN", "ADA-PLN"]

    def fake_get_symbols() -> list[str]:
        return symbols

    monkeypatch.setattr(
        "snapper.infrastructure.market_data.zonda.get_available_zonda_symbols",
        fake_get_symbols,
    )
    service = ZondaSnapshotUpdaterService(
        cast(ZondaExchangeClient, StubZondaClient([])),
        cast(DatabaseRepository, DummyRepository()),
    )
    result = await service.load_all_symbols()
    assert result == symbols


@pytest.mark.asyncio()
async def test_collect_snapshots_loop_creates_entries(monkeypatch: pytest.MonkeyPatch) -> None:
    """Collect snapshots from ticker stream.

    Given: Stub client yielding valid and invalid tickers,
    When: _collect_snapshots_loop is called,
    Then: Only valid symbols create snapshots with spread.
    """
    ticker_valid = TickerUpdate(
        symbol="BTC-PLN",
        bid=100.0,
        ask=101.0,
        last=100.5,
        volume=1200.0,
        vwap=100.7,
        low=98.0,
        high=105.0,
        change=0.02,
        change_pct=0.02,
        bid_qty=5.0,
        ask_qty=4.0,
    )
    ticker_invalid = TickerUpdate(
        symbol="UNKNOWN",
        bid=0.0,
        bid_qty=0.0,
        ask=0.0,
        ask_qty=0.0,
        last=0.0,
        volume=0.0,
        vwap=0.0,
        low=0.0,
        high=0.0,
        change=0.0,
        change_pct=0.0,
    )
    client = StubZondaClient([ticker_valid, ticker_invalid])

    def fake_zonda_to_native(symbol: str) -> str:
        if symbol == "BTC-PLN":
            return "BTC-PLN"
        raise ValueError("unknown symbol")

    monkeypatch.setattr(
        "snapper.infrastructure.market_data.zonda.zonda_to_native",
        fake_zonda_to_native,
    )
    service = ZondaSnapshotUpdaterService(
        cast(ZondaExchangeClient, client),
        cast(DatabaseRepository, DummyRepository()),
    )
    snapshots: dict[str, MarketSnapshot] = {}
    await service._collect_snapshots_loop(["BTC-PLN", "OTHER"], snapshots)
    assert "BTC-PLN" in snapshots
    snapshot = snapshots["BTC-PLN"]
    assert snapshot.exchange == "zonda"
    assert math.isclose(snapshot.spread or 0.0, 1.0, rel_tol=1e-9)
    assert math.isclose(snapshot.spread_pct or 0.0, (1.0 / 100.5) * 100, rel_tol=1e-9)


@pytest.mark.asyncio()
async def test_collect_snapshots_with_timeout_returns_partial_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return empty list on timeout.

    Given: Service with mocked timeout behavior,
    When: _collect_snapshots_with_timeout times out,
    Then: Returns empty list.
    """
    service = ZondaSnapshotUpdaterService(
        cast(ZondaExchangeClient, StubZondaClient([])),
        cast(DatabaseRepository, DummyRepository()),
    )

    async def fake_load_all_symbols() -> list[str]:
        return ["BTC-PLN", "ETH-PLN"]

    async def fake_wait_for(
        awaitable: Awaitable[object], *, timeout: int | float | None = None
    ) -> object:
        if hasattr(awaitable, "close"):
            cast(Any, awaitable).close()
        raise TimeoutError

    monkeypatch.setattr(service, "load_all_symbols", fake_load_all_symbols)
    monkeypatch.setattr(
        "snapper.infrastructure.market_data.zonda.asyncio.wait_for",
        fake_wait_for,
    )
    result = await service._collect_snapshots_with_timeout(timeout_seconds=1)
    assert result == []


@pytest.mark.asyncio()
async def test_update_market_snapshots_persists_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Persist collected snapshots to database.

    Given: Service with mocked collection returning snapshot,
    When: update_market_snapshots is called,
    Then: Snapshot is saved and committed.
    """
    repository = DummyRepository()
    service = ZondaSnapshotUpdaterService(
        cast(ZondaExchangeClient, StubZondaClient([])),
        cast(DatabaseRepository, repository),
    )
    snapshot = MarketSnapshot(
        exchange="zonda",
        symbol="BTC-PLN",
        bid=100.0,
        bid_volume=5.0,
        ask=101.0,
        ask_volume=4.0,
        last_price=100.5,
        volume_24h=1200.0,
        vwap_24h=100.7,
        low_24h=98.0,
        high_24h=105.0,
        change_24h=0.02,
        spread=1.0,
        spread_pct=1.0 / 100.5 * 100,
        updated_at=datetime.now(UTC),
    )

    async def fake_collect(_timeout: int) -> list[MarketSnapshot]:
        return [snapshot]

    monkeypatch.setattr(service, "_collect_snapshots_with_timeout", fake_collect)
    result = await service.update_market_snapshots(timeout_seconds=5)
    assert result == 1
    assert repository.session.saved == [snapshot]
    assert repository.session.committed is True


@pytest.mark.asyncio()
async def test_update_market_snapshots_propagates_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Propagate collection errors.

    Given: Service with collection that raises RuntimeError,
    When: update_market_snapshots is called,
    Then: RuntimeError is propagated.
    """
    service = ZondaSnapshotUpdaterService(
        cast(ZondaExchangeClient, StubZondaClient([])),
        cast(DatabaseRepository, DummyRepository()),
    )

    async def fake_collect(_timeout: int) -> list[MarketSnapshot]:
        raise RuntimeError("failure")

    monkeypatch.setattr(service, "_collect_snapshots_with_timeout", fake_collect)
    with pytest.raises(RuntimeError, match="failure"):
        await service.update_market_snapshots(timeout_seconds=3)


@pytest.mark.asyncio()
async def test_async_update_snapshots_manages_lifecycle() -> None:
    """Manage client connection lifecycle.

    Given: Mocked dependencies,
    When: _async_update_snapshots is called,
    Then: Client connected, service started, client disconnected.
    """
    with (
        patch("snapper.infrastructure.market_data.zonda.get_settings") as mock_settings,
        patch("snapper.infrastructure.market_data.zonda.DatabaseRepository") as mock_repo,
        patch("snapper.infrastructure.market_data.zonda.ZondaExchangeClient") as mock_client,
        patch(
            "snapper.infrastructure.market_data.zonda.ZondaSnapshotUpdaterService"
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


def test_run_zonda_snapshot_update_invokes_asyncio_run(monkeypatch: pytest.MonkeyPatch) -> None:
    """Invoke asyncio.run with coroutine.

    Given: Mocked asyncio.run,
    When: run_zonda_snapshot_update is called,
    Then: Coroutine is passed to asyncio.run.
    """
    captured: dict[str, Coroutine[Any, Any, Any]] = {}

    def fake_run(coro: Coroutine[Any, Any, Any]) -> None:
        captured["coro"] = coro
        coro.close()

    monkeypatch.setattr(asyncio, "run", fake_run)
    run_zonda_snapshot_update()
    assert "coro" in captured


class DummyTicker(SimpleNamespace):
    """Dummy ticker for testing market data processing."""

    pass


class DummyZondaClient(ZondaExchangeClient):
    """Dummy Zonda client for testing."""

    def __init__(self, ticks: list[DummyTicker]) -> None:
        """Initialize the instance."""
        super().__init__()
        self._ticks = ticks

    async def subscribe_ticks(
        self, symbols: list[str], snapshot: bool = True
    ) -> AsyncIterator[DummyTicker]:
        """Yield configured tickers."""
        for t in self._ticks:
            yield t


class DummyRepo(SimpleNamespace):
    """Dummy repository for testing database operations."""

    def __init__(self) -> None:
        """Initialize the instance."""
        super().__init__()
        self.saved: list[Any] = []

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
        ...


@pytest.mark.asyncio
async def test_collect_snapshots_loop_invalid_symbol_skips(monkeypatch: pytest.MonkeyPatch) -> None:
    """Skip tickers with unknown symbols.

    Given: Client yielding ticker with unknown symbol,
    When: _collect_snapshots_loop is called,
    Then: Snapshots dict remains empty.
    """
    tick = DummyTicker(
        symbol="UNKNOWN",
        bid=1.0,
        ask=2.0,
        last=1.5,
        high=2.0,
        low=1.0,
        volume=10.0,
        change=0.1,
        vwap=1.5,
        bid_qty=1.0,
        ask_qty=1.0,
    )
    client = DummyZondaClient([tick])
    repo: Any = DummyRepo()
    svc = ZondaSnapshotUpdaterService(client, repo)
    snapshots: dict[str, Any] = {}
    await svc._collect_snapshots_loop(["UNKNOWN"], snapshots)
    assert snapshots == {}


@pytest.mark.asyncio
async def test_update_market_snapshots_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    """Return zero count on timeout.

    Given: Service with slow collection loop,
    When: update_market_snapshots times out,
    Then: Returns count of 0.
    """
    client = DummyZondaClient([])
    repo: Any = DummyRepo()
    svc = ZondaSnapshotUpdaterService(client, repo)

    async def slow_loop(*args: Any, **kwargs: Any) -> None:
        await asyncio.sleep(1)

    monkeypatch.setattr(svc, "_collect_snapshots_loop", slow_loop)
    count = await svc.update_market_snapshots(timeout_seconds=0.01)
    assert count == 0


@pytest.mark.asyncio
async def test_collect_snapshots_loop_exception_continues(monkeypatch: pytest.MonkeyPatch) -> None:
    """Continue processing after ticker error.

    Given: Client yielding bad then good ticker,
    When: _collect_snapshots_loop is called,
    Then: Good ticker is processed after error.
    """
    tick_good = DummyTicker(
        symbol="BTC-PLN",
        bid=100000.0,
        ask=100100.0,
        last=100050.0,
        high=101000.0,
        low=99000.0,
        volume=100.0,
        change=0.5,
        vwap=100000.0,
        bid_qty=1.0,
        ask_qty=1.0,
    )
    tick_bad = DummyTicker(
        symbol="ETH-PLN",
        bid=None,
        ask=None,
        last=5000.0,
        high=5100.0,
        low=4900.0,
        volume=50.0,
        change=0.2,
        vwap=5000.0,
        bid_qty=None,
        ask_qty=None,
    )
    client = DummyZondaClient([tick_bad, tick_good])
    repo: Any = DummyRepo()
    svc = ZondaSnapshotUpdaterService(client, repo)
    monkeypatch.setattr(
        "snapper.infrastructure.market_data.zonda.zonda_to_native",
        lambda s: s,
    )
    snapshots: dict[str, Any] = {}
    await svc._collect_snapshots_loop(["ETH-PLN", "BTC-PLN", "XRP-PLN"], snapshots)
    assert "BTC-PLN" in snapshots


@pytest.mark.asyncio
async def test_collect_snapshots_loop_logs_progress(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Log progress during collection.

    Given: Client yielding many tickers,
    When: _collect_snapshots_loop is called,
    Then: At least 10 snapshots are collected.
    """
    ticks = [
        DummyTicker(
            symbol=f"SYM{i}-PLN",
            bid=100.0,
            ask=101.0,
            last=100.5,
            high=102.0,
            low=99.0,
            volume=10.0,
            change=0.1,
            vwap=100.0,
            bid_qty=1.0,
            ask_qty=1.0,
        )
        for i in range(12)
    ]
    client = DummyZondaClient(ticks)
    repo: Any = DummyRepo()
    svc = ZondaSnapshotUpdaterService(client, repo)
    monkeypatch.setattr(
        "snapper.infrastructure.market_data.zonda.zonda_to_native",
        lambda s: s,
    )
    with caplog.at_level(logging.DEBUG):
        snapshots: dict[str, Any] = {}
        await svc._collect_snapshots_loop([f"SYM{i}-PLN" for i in range(20)], snapshots)
    assert len(snapshots) >= 10


@pytest.mark.asyncio
async def test_collect_snapshots_loop_stops_when_all_collected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stop collecting when all symbols received.

    Given: Client yielding exactly requested symbols,
    When: _collect_snapshots_loop is called,
    Then: All three symbols are in snapshots.
    """
    ticks = [
        DummyTicker(
            symbol="A-PLN",
            bid=10.0,
            ask=11.0,
            last=10.5,
            high=12.0,
            low=9.0,
            volume=5.0,
            change=0.05,
            vwap=10.0,
            bid_qty=1.0,
            ask_qty=1.0,
        ),
        DummyTicker(
            symbol="B-PLN",
            bid=20.0,
            ask=21.0,
            last=20.5,
            high=22.0,
            low=19.0,
            volume=5.0,
            change=0.05,
            vwap=20.0,
            bid_qty=1.0,
            ask_qty=1.0,
        ),
        DummyTicker(
            symbol="C-PLN",
            bid=30.0,
            ask=31.0,
            last=30.5,
            high=32.0,
            low=29.0,
            volume=5.0,
            change=0.05,
            vwap=30.0,
            bid_qty=1.0,
            ask_qty=1.0,
        ),
    ]
    client = DummyZondaClient(ticks)
    repo: Any = DummyRepo()
    svc = ZondaSnapshotUpdaterService(client, repo)
    monkeypatch.setattr(
        "snapper.infrastructure.market_data.zonda.zonda_to_native",
        lambda s: s,
    )
    snapshots: dict[str, Any] = {}
    await svc._collect_snapshots_loop(["A-PLN", "B-PLN", "C-PLN"], snapshots)
    assert len(snapshots) == 3
    assert "A-PLN" in snapshots
    assert "B-PLN" in snapshots
    assert "C-PLN" in snapshots
