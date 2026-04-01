"""Tests for Kraken Equities market data snapshot service."""

import asyncio
import logging
from collections.abc import AsyncIterator
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
from snapper.infrastructure.exchanges.implementations.kraken_equities import (
    KrakenEquitiesExchangeClient,
)
from snapper.infrastructure.market_data.kraken_equities import KrakenEquitiesSnapshotUpdaterService
from snapper.infrastructure.market_data.kraken_equities import _async_update_snapshots
from snapper.infrastructure.market_data.kraken_equities import run_kraken_equities_snapshot_update


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

    def __enter__(self) -> DummySession:
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


class StubKrakenEquitiesClient:
    """Stub Kraken Equities client for integration testing."""

    def __init__(self, tickers: list[TickerUpdate]) -> None:
        """Initialize the instance."""
        self._tickers = tickers
        self.requests: list[dict[str, Any]] = []

    async def subscribe_ticks(self, symbols: list[str]) -> AsyncIterator[TickerUpdate]:
        """Record request and yield configured tickers."""
        self.requests.append({"symbols": list(symbols)})
        for ticker in self._tickers:
            yield ticker


@pytest.mark.asyncio()
async def test_load_all_symbols_returns_mapper_data(monkeypatch: pytest.MonkeyPatch) -> None:
    """Load symbols from symbol mapper function.

    Given: Mocked get_available_kraken_equities_symbols returning symbols,
    When: load_all_symbols is called,
    Then: Returns same symbols list.
    """
    symbols = ["CLM6-NYMEX", "ESM6-CME"]

    def fake_get_symbols() -> list[str]:
        return symbols

    monkeypatch.setattr(
        "snapper.infrastructure.market_data.kraken_equities.get_available_kraken_equities_symbols",
        fake_get_symbols,
    )
    service = KrakenEquitiesSnapshotUpdaterService(
        cast(KrakenEquitiesExchangeClient, StubKrakenEquitiesClient([])),
        cast(DatabaseRepository, DummyRepository()),
    )
    result = await service.load_all_symbols()
    assert result == symbols


@pytest.mark.asyncio()
async def test_collect_snapshots_loop_creates_entries() -> None:
    """Collect snapshots from ticker stream.

    Given: Stub client yielding valid ticker,
    When: _collect_snapshots_loop is called,
    Then: Valid symbols create snapshots with spread.
    """
    ticker_valid = TickerUpdate(
        symbol="CLM6-NYMEX",
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
    client = StubKrakenEquitiesClient([ticker_valid])
    service = KrakenEquitiesSnapshotUpdaterService(
        cast(KrakenEquitiesExchangeClient, client),
        cast(DatabaseRepository, DummyRepository()),
    )
    snapshots: dict[str, MarketSnapshot] = {}
    await service._collect_snapshots_loop(["CLM6-NYMEX"], snapshots)
    assert "CLM6-NYMEX" in snapshots
    snapshot = snapshots["CLM6-NYMEX"]
    assert snapshot.instrument_public_id == "CLM6-NYMEX"
    assert snapshot.spread is not None
    assert abs((snapshot.spread or 0.0) - 1.0) < 1e-9


@pytest.mark.asyncio()
async def test_resolve_native_symbol_returns_unchanged() -> None:
    """Resolve native symbol returns the symbol unchanged.

    Given: A service instance,
    When: _resolve_native_symbol is called with a symbol,
    Then: Returns the same symbol string.
    """
    service = KrakenEquitiesSnapshotUpdaterService(
        cast(KrakenEquitiesExchangeClient, StubKrakenEquitiesClient([])),
        cast(DatabaseRepository, DummyRepository()),
    )
    assert service._resolve_native_symbol("CLM6-NYMEX") == "CLM6-NYMEX"


@pytest.mark.asyncio()
async def test_collect_snapshots_with_timeout_returns_partial_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return empty dict on timeout.

    Given: Service with mocked timeout behavior,
    When: _collect_snapshots_with_timeout times out,
    Then: Returns empty dict.
    """
    service = KrakenEquitiesSnapshotUpdaterService(
        cast(KrakenEquitiesExchangeClient, StubKrakenEquitiesClient([])),
        cast(DatabaseRepository, DummyRepository()),
    )

    async def fake_load_all_symbols() -> list[str]:
        return ["CLM6-NYMEX", "ESM6-CME"]

    class _ImmediateTimeout:
        """Context manager that immediately raises TimeoutError."""

        def __init__(self, _delay: float | None) -> None:
            pass

        async def __aenter__(self) -> _ImmediateTimeout:
            raise TimeoutError

        async def __aexit__(self, *_args: object) -> None:
            pass

    monkeypatch.setattr(service, "load_all_symbols", fake_load_all_symbols)
    monkeypatch.setattr(
        "snapper.infrastructure.market_data.kraken_equities.asyncio.timeout",
        _ImmediateTimeout,
    )
    result = await service._collect_snapshots_with_timeout(timeout_seconds=1)
    assert result == {}


@pytest.mark.asyncio()
async def test_update_market_snapshots_persists_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Persist collected snapshots to database via SCD2.

    Given: Service with mocked collection and resolution,
    When: update_market_snapshots is called,
    Then: Snapshots are resolved and persisted via SCD2.
    """
    repository = DummyRepository()
    service = KrakenEquitiesSnapshotUpdaterService(
        cast(KrakenEquitiesExchangeClient, StubKrakenEquitiesClient([])),
        cast(DatabaseRepository, repository),
    )
    snapshot = MarketSnapshot(
        instrument_public_id="CLM6-NYMEX",
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
        timestamp=datetime.now(UTC),
        session_id="test-session",
        sequence_id=1,
    )

    async def fake_collect(_timeout: int) -> dict[str, MarketSnapshot]:
        return {"CLM6-NYMEX": snapshot}

    monkeypatch.setattr(service, "_collect_snapshots_with_timeout", fake_collect)
    monkeypatch.setattr(
        service,
        "_resolve_batch_instrument_ids",
        lambda ns, ex, as_of: {"CLM6-NYMEX": "inst-cl-123"},
    )
    monkeypatch.setattr(
        service,
        "_persist_snapshots_scd2",
        lambda snaps: len(snaps),
    )
    result = await service.update_market_snapshots(timeout_seconds=5)
    assert result == 1
    assert snapshot.instrument_public_id == "inst-cl-123"


@pytest.mark.asyncio()
async def test_update_market_snapshots_propagates_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Propagate collection errors.

    Given: Service with collection that raises RuntimeError,
    When: update_market_snapshots is called,
    Then: RuntimeError is propagated.
    """
    service = KrakenEquitiesSnapshotUpdaterService(
        cast(KrakenEquitiesExchangeClient, StubKrakenEquitiesClient([])),
        cast(DatabaseRepository, DummyRepository()),
    )

    async def fake_collect(_timeout: int) -> dict[str, MarketSnapshot]:
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
        patch("snapper.infrastructure.market_data.kraken_equities.get_settings") as mock_settings,
        patch("snapper.infrastructure.market_data.kraken_equities.DatabaseRepository") as mock_repo,
        patch(
            "snapper.infrastructure.market_data.kraken_equities.KrakenEquitiesExchangeClient"
        ) as mock_client,
        patch(
            "snapper.infrastructure.market_data.kraken_equities.KrakenEquitiesSnapshotUpdaterService"
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


def test_run_kraken_equities_snapshot_update_invokes_asyncio_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Invoke asyncio.run with coroutine.

    Given: Mocked asyncio.run,
    When: run_kraken_equities_snapshot_update is called,
    Then: Coroutine is passed to asyncio.run.
    """
    captured: dict[str, Coroutine[Any, Any, Any]] = {}

    def fake_run(coro: Coroutine[Any, Any, Any]) -> None:
        captured["coro"] = coro
        coro.close()

    monkeypatch.setattr(asyncio, "run", fake_run)
    run_kraken_equities_snapshot_update()
    assert "coro" in captured


class DummyTicker(SimpleNamespace):
    """Dummy ticker for testing market data processing."""

    pass


class DummyKrakenEquitiesClient(KrakenEquitiesExchangeClient):
    """Dummy Kraken Equities client for testing."""

    def __init__(self, ticks: list[DummyTicker]) -> None:
        """Initialize the instance."""
        super().__init__()
        self._ticks = ticks

    async def _subscribe_ticks_impl(self, symbols: list[str]) -> AsyncIterator[DummyTicker]:
        """Yield configured tickers."""
        for t in self._ticks:
            yield t


@pytest.mark.asyncio
async def test_collect_snapshots_loop_exception_continues() -> None:
    """Continue processing after ticker error.

    Given: Client yielding bad then good ticker,
    When: _collect_snapshots_loop is called,
    Then: Good ticker is processed after error.
    """
    tick_good = DummyTicker(
        symbol="CLM6-NYMEX",
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
        symbol="ESM6-CME",
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
    client = DummyKrakenEquitiesClient([tick_bad, tick_good])
    repo: Any = DummyRepository()
    svc = KrakenEquitiesSnapshotUpdaterService(client, repo)
    snapshots: dict[str, Any] = {}
    await svc._collect_snapshots_loop(["ESM6-CME", "CLM6-NYMEX", "GCM6-COMEX"], snapshots)
    assert "CLM6-NYMEX" in snapshots


@pytest.mark.asyncio
async def test_collect_snapshots_loop_skips_none_resolve(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Skip tickers where _resolve_native_symbol returns None.

    Given: Client yielding a ticker and _resolve_native_symbol returning None,
    When: _collect_snapshots_loop is called,
    Then: Snapshots dict remains empty.
    """
    tick = DummyTicker(
        symbol="UNKNOWN-CME",
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
    client = DummyKrakenEquitiesClient([tick])
    repo: Any = DummyRepository()
    svc = KrakenEquitiesSnapshotUpdaterService(client, repo)
    monkeypatch.setattr(svc, "_resolve_native_symbol", lambda s: None)
    snapshots: dict[str, Any] = {}
    await svc._collect_snapshots_loop(["UNKNOWN-CME"], snapshots)
    assert snapshots == {}


@pytest.mark.asyncio
async def test_update_market_snapshots_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    """Return zero count on timeout.

    Given: Service with slow collection loop,
    When: update_market_snapshots times out,
    Then: Returns count of 0.
    """
    client = DummyKrakenEquitiesClient([])
    repo: Any = DummyRepository()
    svc = KrakenEquitiesSnapshotUpdaterService(client, repo)

    async def slow_loop(*args: Any, **kwargs: Any) -> None:
        await asyncio.sleep(1)

    monkeypatch.setattr(svc, "_collect_snapshots_loop", slow_loop)
    count = await svc.update_market_snapshots(timeout_seconds=0.01)
    assert count == 0


@pytest.mark.asyncio
async def test_collect_snapshots_loop_logs_progress(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Log progress during collection.

    Given: Client yielding many tickers,
    When: _collect_snapshots_loop is called,
    Then: At least 10 snapshots are collected.
    """
    ticks = [
        DummyTicker(
            symbol=f"SYM{i}-CME",
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
    client = DummyKrakenEquitiesClient(ticks)
    repo: Any = DummyRepository()
    svc = KrakenEquitiesSnapshotUpdaterService(client, repo)
    with caplog.at_level(logging.DEBUG):
        snapshots: dict[str, Any] = {}
        await svc._collect_snapshots_loop([f"SYM{i}-CME" for i in range(20)], snapshots)
    assert len(snapshots) >= 10


@pytest.mark.asyncio
async def test_collect_snapshots_loop_stops_when_all_collected() -> None:
    """Stop collecting when all symbols received.

    Given: Client yielding exactly requested symbols,
    When: _collect_snapshots_loop is called,
    Then: All three symbols are in snapshots.
    """
    ticks = [
        DummyTicker(
            symbol="A-CME",
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
            symbol="B-CME",
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
            symbol="C-CME",
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
    client = DummyKrakenEquitiesClient(ticks)
    repo: Any = DummyRepository()
    svc = KrakenEquitiesSnapshotUpdaterService(client, repo)
    snapshots: dict[str, Any] = {}
    await svc._collect_snapshots_loop(["A-CME", "B-CME", "C-CME"], snapshots)
    assert len(snapshots) == 3
    assert "A-CME" in snapshots
    assert "B-CME" in snapshots
    assert "C-CME" in snapshots


@pytest.mark.asyncio
async def test_collect_snapshots_loop_stamps_provenance() -> None:
    """Snapshots built in _collect_snapshots_loop carry session_id and sequence_id.

    Given: Client yielding two tickers,
    When: _collect_snapshots_loop completes,
    Then: Each snapshot has a non-empty session_id and sequence_ids are 1 and 2.
    """
    tickers = [
        DummyTicker(
            symbol="CLM6-NYMEX",
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
        ),
        DummyTicker(
            symbol="ESM6-CME",
            bid=200.0,
            ask=201.0,
            last=200.5,
            high=202.0,
            low=199.0,
            volume=5.0,
            change=0.05,
            vwap=200.0,
            bid_qty=1.0,
            ask_qty=1.0,
        ),
    ]
    client = DummyKrakenEquitiesClient(tickers)
    repo: Any = DummyRepository()
    svc = KrakenEquitiesSnapshotUpdaterService(client, repo)
    snapshots: dict[str, Any] = {}
    await svc._collect_snapshots_loop(["CLM6-NYMEX", "ESM6-CME"], snapshots)
    assert len(snapshots) == 2
    session_ids = {s.session_id for s in snapshots.values()}
    assert len(session_ids) == 1
    assert "" not in session_ids
    sequence_ids = sorted(s.sequence_id for s in snapshots.values())
    assert sequence_ids == [1, 2]


@pytest.mark.asyncio()
async def test_update_market_snapshots_skips_unresolved(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Skip symbols with no instrument resolution.

    Given: Service with collection returning two symbols but only one resolvable,
    When: update_market_snapshots is called,
    Then: Only the resolved snapshot is persisted.
    """
    service = KrakenEquitiesSnapshotUpdaterService(
        cast(KrakenEquitiesExchangeClient, StubKrakenEquitiesClient([])),
        cast(DatabaseRepository, DummyRepository()),
    )
    snap1 = MarketSnapshot(
        instrument_public_id="CLM6-NYMEX",
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
        spread_pct=1.0,
        timestamp=datetime.now(UTC),
        session_id="s",
        sequence_id=1,
    )
    snap2 = MarketSnapshot(
        instrument_public_id="NOPE-CME",
        bid=50.0,
        bid_volume=2.0,
        ask=51.0,
        ask_volume=3.0,
        last_price=50.5,
        volume_24h=600.0,
        vwap_24h=50.3,
        low_24h=49.0,
        high_24h=52.0,
        change_24h=0.01,
        spread=1.0,
        spread_pct=2.0,
        timestamp=datetime.now(UTC),
        session_id="s",
        sequence_id=2,
    )

    async def fake_collect(_timeout: int) -> dict[str, MarketSnapshot]:
        return {"CLM6-NYMEX": snap1, "NOPE-CME": snap2}

    monkeypatch.setattr(service, "_collect_snapshots_with_timeout", fake_collect)
    monkeypatch.setattr(
        service,
        "_resolve_batch_instrument_ids",
        lambda ns, ex, as_of: {"CLM6-NYMEX": "inst-cl"},
    )
    persisted: list[list[MarketSnapshot]] = []

    def _capture(snaps: list[MarketSnapshot]) -> int:
        persisted.append(list(snaps))
        return len(snaps)

    monkeypatch.setattr(
        service,
        "_persist_snapshots_scd2",
        _capture,
    )
    result = await service.update_market_snapshots(timeout_seconds=5)
    assert result == 1
    assert len(persisted[0]) == 1
    assert persisted[0][0].instrument_public_id == "inst-cl"
