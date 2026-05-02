"""Deterministic baseline capture for cross-asset backtest refactor parity.

Runs a fixed set of ``batch_processor.process_time_batch`` scenarios with
deterministic inputs (fake strategy, fake candles, pinned snapshot time)
and serialises the resulting collector artifacts + portfolio state to
JSON. Used twice:

1. One-shot baseline capture: writes the golden snapshot to
   ``tests/application/backtest/fixtures/phase2c_baseline_snapshot.json``.
2. Regression check: ``TestPhase2cBaselineParity`` imports ``SCENARIOS``
   + ``serialise_run`` and re-runs each scenario against the same
   snapshot — any drift fails the gate.

The serialiser strips nondeterministic identifiers (uuid7 ``public_id``,
``signal_public_id``, SequenceTracker ``session_id``, ``sequence_id``)
and keeps domain-deterministic values (``signal_type``, ``instrument``,
``price``, trade ``quantity``/``pnl``, equity, cash, positions).

Run directly::

    python scripts/capture_phase2c_baseline.py

Overwrites the fixture in-place if it already exists.
"""

import asyncio
import json
from collections.abc import Awaitable
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

from snapper.application.backtest.batch_processor import CandleEvent
from snapper.application.backtest.batch_processor import process_time_batch
from snapper.application.backtest.config import BacktestConfig
from snapper.application.backtest.result_collector import ResultCollector
from snapper.application.portfolio.models import PortfolioTracker
from snapper.core.types import TradeSideEnum
from snapper.data.repository_types import CandleRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.strategies.base import BaseStrategy
from snapper.strategies.models import StrategySignal

_BASE_NOW = datetime(2026, 4, 22, 12, 0, 0, tzinfo=UTC)
"""Fixed anchor time. Every scenario derives its candles / start_date from this."""

_FIXED_SNAPSHOT_AS_OF = _BASE_NOW
"""Pinned bus_time passed to process_time_batch — deterministic timestamp column."""

_FIXTURE_PATH = (
    Path(__file__).resolve().parent.parent
    / "tests"
    / "application"
    / "backtest"
    / "fixtures"
    / "phase2c_baseline_snapshot.json"
)
"""Golden snapshot output location."""


def _candle_row(open_at: datetime, close: float, seq: int) -> CandleRow:
    """Build a minimal deterministic candle row."""
    row: CandleRow = {
        "public_id": f"candle-{seq}",
        "timestamp": _BASE_NOW,
        "session_id": "seed-session",
        "sequence_id": seq,
        "open_at": open_at,
        "timeframe": "1h",
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "volume": 1.0,
        "vwap": None,
        "trades": None,
    }
    return row


def _make_config(
    *,
    start_date: datetime,
    target_execution_exchange: str | None = None,
) -> MagicMock:
    """Build a MagicMock BacktestConfig with explicit attribute set.

    ``target_execution_exchange`` is set explicitly (None by default) so the
    cross-asset-backtest attribution ternary takes the ``event.exchange``
    fallback branch.
    """
    config = MagicMock(spec=BacktestConfig)
    config.timeframe = "1h"
    config.start_date = start_date
    config.slippage_bps = 0.0
    config.commission_bps = 0.0
    config.target_execution_exchange = target_execution_exchange
    return config


def _make_strategy(signal: StrategySignal | None) -> MagicMock:
    """Stub strategy returning a fixed signal (or None) on every candle."""
    strategy = MagicMock(spec=BaseStrategy)
    strategy._handle_candle_data = AsyncMock(return_value=signal)
    return strategy


async def _scenario_single_feed_buy_filled() -> tuple[ResultCollector, PortfolioTracker]:
    """S1: Single-feed post-warmup BUY signal → trade + signal + equity."""
    config = _make_config(start_date=_BASE_NOW)
    strategy = _make_strategy(
        StrategySignal(
            instrument="BTC-USD",
            side=TradeSideEnum.BUY,
            strength=1.0,
            reason="s1_buy",
            price=100.0,
        )
    )
    collector = ResultCollector()
    portfolio = PortfolioTracker(cash=10_000.0)
    latest_closes: dict[str, float] = {}
    tracker = SequenceTracker()
    batch = [
        CandleEvent(
            open_at=_BASE_NOW + timedelta(hours=1),
            exchange="kraken",
            instrument="BTC-USD",
            row=_candle_row(_BASE_NOW + timedelta(hours=1), 100.0, seq=1),
        )
    ]
    await process_time_batch(
        batch=batch,
        run_public_id="run-s1",
        config=config,
        strategy=strategy,
        portfolio=portfolio,
        latest_closes=latest_closes,
        collector=collector,
        tracker=tracker,
        snapshot_as_of=_FIXED_SNAPSHOT_AS_OF,
    )
    return collector, portfolio


async def _scenario_multi_timestamp_no_signals() -> tuple[ResultCollector, PortfolioTracker]:
    """S2: Two timestamp batches, strategy returns None → 0 signals, 2 equity points."""
    config = _make_config(start_date=_BASE_NOW)
    strategy = _make_strategy(None)
    collector = ResultCollector()
    portfolio = PortfolioTracker(cash=10_000.0)
    latest_closes: dict[str, float] = {}
    tracker = SequenceTracker()
    for idx, (hours_offset, close) in enumerate(((1, 100.0), (2, 110.0)), start=1):
        batch = [
            CandleEvent(
                open_at=_BASE_NOW + timedelta(hours=hours_offset),
                exchange="kraken",
                instrument="BTC-USD",
                row=_candle_row(_BASE_NOW + timedelta(hours=hours_offset), close, seq=idx),
            )
        ]
        await process_time_batch(
            batch=batch,
            run_public_id="run-s2",
            config=config,
            strategy=strategy,
            portfolio=portfolio,
            latest_closes=latest_closes,
            collector=collector,
            tracker=tracker,
            snapshot_as_of=_FIXED_SNAPSHOT_AS_OF,
        )
    return collector, portfolio


async def _scenario_warmup_gated_signal_dropped() -> tuple[ResultCollector, PortfolioTracker]:
    """S3: Candle at t < start_date → early return, nothing recorded."""
    config = _make_config(start_date=_BASE_NOW + timedelta(hours=5))
    strategy = _make_strategy(
        StrategySignal(
            instrument="BTC-USD",
            side=TradeSideEnum.BUY,
            strength=1.0,
            reason="s3_warmup",
            price=100.0,
        )
    )
    collector = ResultCollector()
    portfolio = PortfolioTracker(cash=10_000.0)
    latest_closes: dict[str, float] = {}
    tracker = SequenceTracker()
    batch = [
        CandleEvent(
            open_at=_BASE_NOW + timedelta(hours=1),
            exchange="kraken",
            instrument="BTC-USD",
            row=_candle_row(_BASE_NOW + timedelta(hours=1), 100.0, seq=1),
        )
    ]
    await process_time_batch(
        batch=batch,
        run_public_id="run-s3",
        config=config,
        strategy=strategy,
        portfolio=portfolio,
        latest_closes=latest_closes,
        collector=collector,
        tracker=tracker,
        snapshot_as_of=_FIXED_SNAPSHOT_AS_OF,
    )
    return collector, portfolio


async def _scenario_signal_emitted_fill_skipped() -> tuple[ResultCollector, PortfolioTracker]:
    """S4: SELL without position → simulate_market_fill returns None; signal still persisted."""
    config = _make_config(start_date=_BASE_NOW)
    strategy = _make_strategy(
        StrategySignal(
            instrument="BTC-USD",
            side=TradeSideEnum.SELL,
            strength=1.0,
            reason="s4_close_without_position",
            price=100.0,
        )
    )
    collector = ResultCollector()
    portfolio = PortfolioTracker(cash=10_000.0)
    latest_closes: dict[str, float] = {}
    tracker = SequenceTracker()
    batch = [
        CandleEvent(
            open_at=_BASE_NOW + timedelta(hours=1),
            exchange="kraken",
            instrument="BTC-USD",
            row=_candle_row(_BASE_NOW + timedelta(hours=1), 100.0, seq=1),
        )
    ]
    await process_time_batch(
        batch=batch,
        run_public_id="run-s4",
        config=config,
        strategy=strategy,
        portfolio=portfolio,
        latest_closes=latest_closes,
        collector=collector,
        tracker=tracker,
        snapshot_as_of=_FIXED_SNAPSHOT_AS_OF,
    )
    return collector, portfolio


@dataclass(frozen=True)
class Scenario:
    """A named baseline scenario driver."""

    name: str
    run: Callable[[], Awaitable[tuple[ResultCollector, PortfolioTracker]]]


SCENARIOS: tuple[Scenario, ...] = (
    Scenario(name="single_feed_buy_filled", run=_scenario_single_feed_buy_filled),
    Scenario(name="multi_timestamp_no_signals", run=_scenario_multi_timestamp_no_signals),
    Scenario(name="warmup_gated_signal_dropped", run=_scenario_warmup_gated_signal_dropped),
    Scenario(name="signal_emitted_fill_skipped", run=_scenario_signal_emitted_fill_skipped),
)
"""Public scenario registry. Imported by ``TestPhase2cBaselineParity``."""


def _serialise_signal(row: dict[str, Any]) -> dict[str, Any]:
    """Strip nondeterministic identifiers; keep domain values."""
    return {
        "signal_type": row["signal_type"],
        "instrument": row["instrument"],
        "price": row["price"],
        "indicators": dict(row["indicators"]),
    }


def _serialise_trade(row: dict[str, Any]) -> dict[str, Any]:
    """Strip nondeterministic identifiers; keep domain values."""
    return {
        "instrument": row["instrument"],
        "side": row["side"],
        "quantity": row["quantity"],
        "price": row["price"],
        "fee": row["fee"],
        "pnl": row["pnl"],
        "position_after": row["position_after"],
    }


def _serialise_equity(row: dict[str, Any]) -> dict[str, Any]:
    """Strip nondeterministic identifiers; keep domain values."""
    return {
        "equity": row["equity"],
        "cash": row["cash"],
        "position_value": row["position_value"],
        "drawdown": row["drawdown"],
    }


def _serialise_portfolio(portfolio: PortfolioTracker) -> dict[str, Any]:
    """Final portfolio state: cash, turnover, positions."""
    positions = {
        instrument: {
            "quantity": pos.quantity,
            "average_price": pos.average_price,
            "realized_pnl": pos.realized_pnl,
        }
        for instrument, pos in sorted(portfolio.positions.items())
    }
    return {
        "cash": portfolio.cash,
        "turnover": portfolio.turnover,
        "positions": positions,
    }


def serialise_run(collector: ResultCollector, portfolio: PortfolioTracker) -> dict[str, Any]:
    """Deterministic JSON-ready dict for one scenario run.

    Strips uuid7 / session / sequence / timestamp fields that vary per
    process lifetime; keeps all domain-deterministic values that a
    byte-identical refactor must preserve.

    Args:
        collector: Buffered artifacts from ``process_time_batch``.
        portfolio: Final portfolio state after the run.

    Returns:
        Nested dict safe to ``json.dumps(..., sort_keys=True)``.
    """
    return {
        "signals": [_serialise_signal(dict(row)) for row in collector.signals],
        "trades": [_serialise_trade(dict(row)) for row in collector.trades],
        "equity_points": [_serialise_equity(dict(row)) for row in collector.equity_points],
        "portfolio": _serialise_portfolio(portfolio),
    }


async def _run_all_scenarios() -> dict[str, Any]:
    """Execute every scenario and build the combined snapshot dict."""
    out: dict[str, Any] = {}
    for scenario in SCENARIOS:
        collector, portfolio = await scenario.run()
        out[scenario.name] = serialise_run(collector, portfolio)
    return out


def main() -> int:
    """Execute all scenarios and write the golden snapshot JSON.

    Returns:
        Process exit code (always 0 on success; exceptions propagate).
    """
    snapshot = asyncio.run(_run_all_scenarios())
    _FIXTURE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _FIXTURE_PATH.write_text(
        json.dumps(snapshot, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"wrote {_FIXTURE_PATH} ({len(SCENARIOS)} scenarios)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
