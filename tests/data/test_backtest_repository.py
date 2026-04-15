"""Tests for BacktestRepository CRUD operations."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import pytest

import snapper.data.repository as repo_module
from snapper.data.backtest_repository import BacktestRepository

NOW = datetime(2026, 4, 13, 12, 0, 0, tzinfo=UTC)


async def _make_repo(tmp_path: Path, name: str = "bt") -> BacktestRepository:
    """Create a BacktestRepository backed by a fresh SQLite DB."""
    db_path = tmp_path / f"{name}.db"
    r = repo_module.SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    return BacktestRepository(r.session_factory)


@pytest.mark.asyncio
async def test_create_and_get_run(tmp_path: Path) -> None:
    """Given: insert payload, When: create_run + get_run, Then: round-trip."""
    repo = await _make_repo(tmp_path)
    row_id, pid = await repo.create_run(
        {
            "wallet_public_id": "w-1",
            "strategy_name": "sma_cross",
            "strategy_params": {"fast": 10},
            "instrument_public_id": "inst-1",
            "exchange": "kraken",
            "timeframe": "1h",
            "start_date": NOW,
            "end_date": NOW + timedelta(days=30),
            "session_id": "s1",
            "sequence_id": 1,
            "timestamp": NOW,
        },
        bus_time=NOW,
        session_id="s1",
        sequence_id=1,
    )
    assert row_id > 0
    run = await repo.get_run(pid, as_of=NOW + timedelta(seconds=1))
    assert run is not None
    assert run["strategy_name"] == "sma_cross"
    assert run["status"] == "pending"
    assert run["wallet_public_id"] == "w-1"


@pytest.mark.asyncio
async def test_get_run_not_found(tmp_path: Path) -> None:
    """Given: no runs, When: get_run, Then: None."""
    repo = await _make_repo(tmp_path)
    result = await repo.get_run("nonexistent", as_of=NOW)
    assert result is None


@pytest.mark.asyncio
async def test_list_runs_with_filters(tmp_path: Path) -> None:
    """Given: two runs, When: list with wallet filter, Then: one returned."""
    repo = await _make_repo(tmp_path)
    await repo.create_run(
        {
            "wallet_public_id": "w-1",
            "strategy_name": "sma",
            "instrument_public_id": "inst-1",
            "exchange": "kraken",
            "timeframe": "1h",
            "start_date": NOW,
            "end_date": NOW,
            "session_id": "s1",
            "sequence_id": 1,
            "timestamp": NOW,
        },
        bus_time=NOW,
        session_id="s1",
        sequence_id=1,
    )
    await repo.create_run(
        {
            "wallet_public_id": "w-2",
            "strategy_name": "rsi",
            "instrument_public_id": "inst-2",
            "exchange": "kraken",
            "timeframe": "1h",
            "start_date": NOW,
            "end_date": NOW,
            "session_id": "s1",
            "sequence_id": 2,
            "timestamp": NOW,
        },
        bus_time=NOW,
        session_id="s1",
        sequence_id=2,
    )
    all_runs = await repo.list_runs(as_of=NOW + timedelta(seconds=1))
    assert len(all_runs) == 2

    w1_runs = await repo.list_runs(as_of=NOW + timedelta(seconds=1), wallet_public_id="w-1")
    assert len(w1_runs) == 1
    assert w1_runs[0]["strategy_name"] == "sma"

    sma_runs = await repo.list_runs(as_of=NOW + timedelta(seconds=1), strategy="rsi")
    assert len(sma_runs) == 1

    pending_runs = await repo.list_runs(as_of=NOW + timedelta(seconds=1), status="pending")
    assert len(pending_runs) == 2


@pytest.mark.asyncio
async def test_update_run_status_scd2(tmp_path: Path) -> None:
    """Given: pending run, When: update to running, Then: SCD2 close-insert."""
    repo = await _make_repo(tmp_path)
    _, pid = await repo.create_run(
        {
            "wallet_public_id": "w-1",
            "strategy_name": "test",
            "instrument_public_id": "inst-1",
            "exchange": "kraken",
            "timeframe": "1h",
            "start_date": NOW,
            "end_date": NOW,
            "session_id": "s1",
            "sequence_id": 1,
            "timestamp": NOW,
        },
        bus_time=NOW,
        session_id="s1",
        sequence_id=1,
    )
    t2 = NOW + timedelta(seconds=5)
    new_id = await repo.update_run_status(
        pid, "running", bus_time=t2, session_id="s1", sequence_id=2, started_at=t2
    )
    assert new_id is not None
    run = await repo.get_run(pid, as_of=t2 + timedelta(seconds=1))
    assert run is not None
    assert run["status"] == "running"
    assert run["started_at"] == t2


@pytest.mark.asyncio
async def test_update_run_status_not_found(tmp_path: Path) -> None:
    """Given: no run, When: update, Then: None."""
    repo = await _make_repo(tmp_path)
    result = await repo.update_run_status(
        "nonexistent", "running", bus_time=NOW, session_id="s1", sequence_id=1
    )
    assert result is None


@pytest.mark.asyncio
async def test_insert_event(tmp_path: Path) -> None:
    """Given: run, When: insert event, Then: retrievable."""
    repo = await _make_repo(tmp_path)
    event_pid = await repo.insert_event(
        {
            "run_public_id": "run-1",
            "event_type": "started",
            "detail": {"candles": 500},
            "session_id": "s1",
            "sequence_id": 1,
            "timestamp": NOW,
        },
        bus_time=NOW,
        session_id="s1",
        sequence_id=1,
    )
    assert event_pid
    events = await repo.get_events("run-1", as_of=NOW + timedelta(seconds=1))
    assert len(events) == 1
    assert events[0]["event_type"] == "started"


@pytest.mark.asyncio
async def test_insert_signals_batch(tmp_path: Path) -> None:
    """Given: signal rows, When: batch insert, Then: all retrievable."""
    repo = await _make_repo(tmp_path)
    signals = [
        {
            "run_public_id": "run-1",
            "public_id": "00000000-0000-7000-8000-00000000000a",
            "signal_time": NOW,
            "signal_type": "buy",
            "instrument": "BTC-USD",
            "price": 50000.0,
            "indicators": {"sma": 49000},
            "session_id": "s1",
            "sequence_id": 1,
            "timestamp": NOW,
        },
        {
            "run_public_id": "run-1",
            "public_id": "00000000-0000-7000-8000-00000000000b",
            "signal_time": NOW + timedelta(hours=1),
            "signal_type": "sell",
            "instrument": "BTC-USD",
            "price": 51000.0,
            "indicators": {"sma": 50500},
            "session_id": "s1",
            "sequence_id": 2,
            "timestamp": NOW,
        },
    ]
    await repo.insert_signals_batch(signals, bus_time=NOW, session_id="s1", sequence_id=10)
    result = await repo.get_signals("run-1", as_of=NOW + timedelta(seconds=1))
    assert len(result) == 2
    assert result[0]["signal_type"] == "buy"
    assert result[1]["signal_type"] == "sell"
    assert {r["public_id"] for r in result} == {
        "00000000-0000-7000-8000-00000000000a",
        "00000000-0000-7000-8000-00000000000b",
    }


@pytest.mark.asyncio
async def test_insert_trades_batch(tmp_path: Path) -> None:
    """Given: trade rows, When: batch insert, Then: all retrievable."""
    repo = await _make_repo(tmp_path)
    trades = [
        {
            "run_public_id": "run-1",
            "executed_at": NOW,
            "instrument": "BTC-USD",
            "side": "buy",
            "quantity": 0.5,
            "price": 50000.0,
            "fee": 25.0,
            "pnl": None,
            "position_after": 0.5,
            "signal_public_id": None,
            "session_id": "s1",
            "sequence_id": 1,
            "timestamp": NOW,
        },
    ]
    await repo.insert_trades_batch(trades, bus_time=NOW, session_id="s1", sequence_id=20)
    result = await repo.get_trades("run-1", as_of=NOW + timedelta(seconds=1))
    assert len(result) == 1
    assert result[0]["side"] == "buy"


@pytest.mark.asyncio
async def test_insert_equity_points_batch(tmp_path: Path) -> None:
    """Given: equity rows, When: batch insert, Then: all retrievable."""
    repo = await _make_repo(tmp_path)
    points = [
        {
            "run_public_id": "run-1",
            "point_time": NOW,
            "equity": 10000.0,
            "cash": 10000.0,
            "position_value": 0.0,
            "drawdown": 0.0,
            "session_id": "s1",
            "sequence_id": 1,
            "timestamp": NOW,
        },
        {
            "run_public_id": "run-1",
            "point_time": NOW + timedelta(hours=1),
            "equity": 10500.0,
            "cash": 5500.0,
            "position_value": 5000.0,
            "drawdown": 0.0,
            "session_id": "s1",
            "sequence_id": 2,
            "timestamp": NOW,
        },
    ]
    await repo.insert_equity_points_batch(points, bus_time=NOW, session_id="s1", sequence_id=30)
    result = await repo.get_equity_points("run-1", as_of=NOW + timedelta(seconds=1))
    assert len(result) == 2
    assert result[0]["equity"] == pytest.approx(10000.0)
    assert result[1]["equity"] == pytest.approx(10500.0)


@pytest.mark.asyncio
async def test_get_equity_points_pagination(tmp_path: Path) -> None:
    """get_equity_points respects limit and after cursor."""
    repo = await _make_repo(tmp_path)
    base_time = NOW
    points = [
        {
            "run_public_id": "run-1",
            "point_time": base_time + timedelta(minutes=i),
            "equity": 10000.0 + i,
            "cash": 10000.0,
            "position_value": 0.0,
            "drawdown": 0.0,
            "session_id": "s1",
            "sequence_id": i + 1,
            "timestamp": base_time,
        }
        for i in range(5)
    ]
    await repo.insert_equity_points_batch(
        points, bus_time=base_time, session_id="s1", sequence_id=99
    )
    limited = await repo.get_equity_points("run-1", as_of=base_time + timedelta(hours=1), limit=2)
    assert len(limited) == 2
    assert limited[0]["equity"] == pytest.approx(10000.0)
    cursor = base_time + timedelta(minutes=1)
    after = await repo.get_equity_points(
        "run-1", as_of=base_time + timedelta(hours=1), after=cursor
    )
    assert [p["equity"] for p in after] == pytest.approx([10002.0, 10003.0, 10004.0])


@pytest.mark.asyncio
async def test_insert_and_get_result(tmp_path: Path) -> None:
    """Given: result metrics, When: insert + get, Then: round-trip."""
    repo = await _make_repo(tmp_path)
    pid = await repo.insert_result(
        {
            "run_public_id": "run-1",
            "total_trades": 42,
            "winning_trades": 25,
            "losing_trades": 17,
            "total_pnl": 1500.0,
            "max_drawdown": 0.12,
            "final_equity": 11500.0,
            "max_equity": 12000.0,
            "session_id": "s1",
            "sequence_id": 1,
            "timestamp": NOW,
        },
        bus_time=NOW,
        session_id="s1",
        sequence_id=40,
    )
    assert pid
    result = await repo.get_result("run-1", as_of=NOW + timedelta(seconds=1))
    assert result is not None
    assert result["total_trades"] == 42
    assert result["total_pnl"] == pytest.approx(1500.0)


@pytest.mark.asyncio
async def test_get_result_not_found(tmp_path: Path) -> None:
    """Given: no result, When: get_result, Then: None."""
    repo = await _make_repo(tmp_path)
    result = await repo.get_result("nonexistent", as_of=NOW)
    assert result is None
