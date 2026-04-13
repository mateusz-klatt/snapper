"""Tests for backtest ORM models and BacktestRunStatusEnum."""

from datetime import UTC
from datetime import datetime
from pathlib import Path

import pytest

import snapper.data.repository as repo_module
from snapper.core.types import BacktestRunStatusEnum
from snapper.data.models import BacktestEquityPoint
from snapper.data.models import BacktestEvent
from snapper.data.models import BacktestResult
from snapper.data.models import BacktestRun
from snapper.data.models import BacktestSignal
from snapper.data.models import BacktestTrade

NOW = datetime(2026, 4, 13, tzinfo=UTC)


class TestBacktestRunStatusEnum:
    """Tests for BacktestRunStatusEnum values."""

    def test_all_statuses_present(self) -> None:
        """Enum has all 6 lifecycle statuses."""
        values = {s.value for s in BacktestRunStatusEnum}
        assert values == {
            "pending",
            "running",
            "completed",
            "failed",
            "cancel_requested",
            "cancelled",
        }

    def test_string_coercion(self) -> None:
        """Enum values are plain strings."""
        assert str(BacktestRunStatusEnum.RUNNING) == "running"
        assert BacktestRunStatusEnum("completed") == BacktestRunStatusEnum.COMPLETED


@pytest.mark.asyncio
async def test_backtest_run_create_all(tmp_path: Path) -> None:
    """create_all creates backtest_runs table.

    Given: empty database,
    When: create_all is called,
    Then: BacktestRun can be inserted and read back.
    """
    db_path = tmp_path / "bt_run.db"
    r = repo_module.SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    async with r.session() as s:
        run = BacktestRun(
            wallet_public_id="wallet-1",
            strategy_name="sma_cross",
            strategy_params={"fast": 10, "slow": 30},
            instrument_public_id="inst-btc",
            exchange="kraken",
            timeframe="1h",
            start_date=NOW,
            end_date=NOW,
            initial_cash=10000.0,
            status="pending",
            session_id="s1",
            sequence_id=1,
            timestamp=NOW,
        )
        s.add(run)
        await s.commit()
        await s.refresh(run)
        assert run.id > 0
        assert run.public_id is not None
        assert run.status == "pending"
        assert run.wallet_public_id == "wallet-1"
        assert run.strategy_params == {"fast": 10, "slow": 30}


@pytest.mark.asyncio
async def test_backtest_event_insert(tmp_path: Path) -> None:
    """BacktestEvent can be inserted as append-only row."""
    db_path = tmp_path / "bt_event.db"
    r = repo_module.SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    async with r.session() as s:
        event = BacktestEvent(
            run_public_id="run-1",
            event_type="started",
            detail={"candles_loaded": 500},
            session_id="s1",
            sequence_id=1,
            timestamp=NOW,
        )
        s.add(event)
        await s.commit()
        await s.refresh(event)
        assert event.id > 0
        assert event.event_type == "started"


@pytest.mark.asyncio
async def test_backtest_result_insert(tmp_path: Path) -> None:
    """BacktestResult can be inserted with metrics."""
    db_path = tmp_path / "bt_result.db"
    r = repo_module.SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    async with r.session() as s:
        result = BacktestResult(
            run_public_id="run-1",
            total_trades=42,
            winning_trades=25,
            losing_trades=17,
            total_pnl=1500.50,
            max_drawdown=0.12,
            sharpe_ratio=1.8,
            win_rate=0.595,
            profit_factor=2.1,
            final_equity=11500.50,
            max_equity=12000.0,
            extra_metrics={},
            session_id="s1",
            sequence_id=1,
            timestamp=NOW,
        )
        s.add(result)
        await s.commit()
        await s.refresh(result)
        assert result.total_trades == 42
        assert result.sharpe_ratio == pytest.approx(1.8)


@pytest.mark.asyncio
async def test_backtest_signal_insert(tmp_path: Path) -> None:
    """BacktestSignal can be inserted with indicator data."""
    db_path = tmp_path / "bt_signal.db"
    r = repo_module.SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    async with r.session() as s:
        signal = BacktestSignal(
            run_public_id="run-1",
            signal_time=NOW,
            signal_type="buy",
            instrument="BTC-USD",
            price=50000.0,
            indicators={"sma_fast": 49500, "sma_slow": 49000},
            session_id="s1",
            sequence_id=1,
            timestamp=NOW,
        )
        s.add(signal)
        await s.commit()
        await s.refresh(signal)
        assert signal.signal_type == "buy"
        assert signal.indicators["sma_fast"] == 49500


@pytest.mark.asyncio
async def test_backtest_trade_insert(tmp_path: Path) -> None:
    """BacktestTrade can be inserted with fill data."""
    db_path = tmp_path / "bt_trade.db"
    r = repo_module.SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    async with r.session() as s:
        trade = BacktestTrade(
            run_public_id="run-1",
            executed_at=NOW,
            instrument="BTC-USD",
            side="buy",
            quantity=0.5,
            price=50000.0,
            fee=25.0,
            pnl=None,
            position_after=0.5,
            session_id="s1",
            sequence_id=1,
            timestamp=NOW,
        )
        s.add(trade)
        await s.commit()
        await s.refresh(trade)
        assert trade.side == "buy"
        assert trade.quantity == pytest.approx(0.5)


@pytest.mark.asyncio
async def test_backtest_equity_point_insert(tmp_path: Path) -> None:
    """BacktestEquityPoint can be inserted with equity data."""
    db_path = tmp_path / "bt_equity.db"
    r = repo_module.SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    async with r.session() as s:
        point = BacktestEquityPoint(
            run_public_id="run-1",
            point_time=NOW,
            equity=10500.0,
            cash=5500.0,
            position_value=5000.0,
            drawdown=0.02,
            session_id="s1",
            sequence_id=1,
            timestamp=NOW,
        )
        s.add(point)
        await s.commit()
        await s.refresh(point)
        assert point.equity == pytest.approx(10500.0)
        assert point.drawdown == pytest.approx(0.02)
