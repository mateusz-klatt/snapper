"""Pure module-level batch processor shared by Direct-DB and ZMQ replay engines.

Houses the per-timestamp candle-batch processing helper extracted from
mod:`snapper.application.backtest.direct_engine` so the forthcoming
``ZmqReplayEngine`` can reuse the same fill
simulation, signal recording, and equity sampling logic without
inheritance gymnastics. Behaviour is byte-for-byte identical to the
previous ``DirectDbEngine._process_time_batch``.
Also re-homes the candle data structures (``CandleEvent`` /
``candle_row_to_data``) here so engines can depend downward on
``batch_processor`` without circular imports — engines own loop control
this module owns per-batch semantics.
Single source of truth for
Updating ``latest_closes`` from every candle in the time batch.
Feeding candles through the strategy via ``_handle_candle_data``.
Honouring ``config.start_date`` warmup gating (signals before start_date
  are dropped before fills are simulated).
Allocating one ``signal_public_id`` per signal so the matching trade row
  carries the same value (FK-style linkage required by parity tests).
Recording an equity point per unique timestamp.
"""

from datetime import datetime
from typing import Any
from typing import NamedTuple
from typing import cast
from uuid import uuid7

from snapper.application.backtest.config import BacktestConfig
from snapper.application.backtest.fill_model import simulate_market_fill
from snapper.application.backtest.progress import BacktestProgressEmitter
from snapper.application.backtest.result_collector import ResultCollector
from snapper.application.portfolio.models import PortfolioTracker
from snapper.data.repository_types import CandleRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.data import CandleData
from snapper.strategies.base import BaseStrategy


class CandleEvent(NamedTuple):
    """A candle row with exchange/instrument context for sorting."""

    open_at: datetime
    exchange: str
    instrument: str
    row: CandleRow


def candle_row_to_data(event: CandleEvent, timeframe: str) -> CandleData:
    """Convert a CandleEvent to a CandleData schema object.

    Args:
        event: Candle event with row data.
        timeframe: Candle timeframe string.

    Returns:
        CandleData suitable for strategy consumption.
    """
    row = event.row
    return CandleData(
        public_id=row["public_id"],
        timestamp=row["timestamp"],
        session_id=row["session_id"],
        sequence_id=row["sequence_id"],
        instrument=event.instrument,
        exchange=cast(Any, event.exchange),
        timeframe=timeframe,
        open_at=row["open_at"],
        open=row["open"],
        high=row["high"],
        low=row["low"],
        close=row["close"],
        volume=row["volume"],
    )


async def process_time_batch(
    batch: list[CandleEvent],
    run_public_id: str,
    config: BacktestConfig,
    strategy: BaseStrategy,
    portfolio: PortfolioTracker,
    latest_closes: dict[str, float],
    collector: ResultCollector,
    tracker: SequenceTracker,
    snapshot_as_of: datetime,
    emitter: BacktestProgressEmitter | None = None,
) -> None:
    """Process all candle events at the same timestamp.

    Updates all close prices first, then feeds candles to the strategy
    then records equity once. Signals before ``config.start_date`` are
    dropped without simulating fills (warmup gating). Each kept signal
    receives a fresh ``uuid7`` ``signal_public_id`` shared with its trade.

    Args:
        batch: All ``CandleEvent``s at the same ``open_at``.
        run_public_id: Backtest run identifier.
        config: Backtest configuration (provides ``start_date``
            ``slippage_bps``, ``commission_bps``, ``timeframe``).
        strategy: Strategy instance.
        portfolio: Portfolio state (mutated by fills).
        latest_closes: Latest close prices per instrument (mutated).
        collector: Result collector buffering signals/trades/equity.
        tracker: Sequence tracker for monotonic ``sequence_id`` allocation.
        snapshot_as_of: Bus time / temporal anchor for all rows in this
            batch (passed instead of read from a hidden engine attribute
            so the helper stays a pure function — same value for both
            Direct-DB and ZMQ replay engines).
        emitter: Optional WS progress emitter. When supplied
            ``on_candle_processed`` is called after the equity sample
            with the current equity + cumulative signal/trade counts.
            ``None`` keeps the helper byte-identical with pre-Phase-2c
            callers.
    """
    for event in batch:
        latest_closes[event.instrument] = float(event.row["close"])

    signals_and_events: list[tuple[Any, CandleEvent]] = []
    for event in batch:
        candle_data = candle_row_to_data(event, config.timeframe)
        payload = candle_data.model_dump_json()
        signal = await strategy._handle_candle_data(event.instrument, payload)
        if signal is not None:
            signals_and_events.append((signal, event))

    if batch[0].open_at < config.start_date:
        return

    for signal, event in signals_and_events:
        sig_pid = str(uuid7())
        fill = simulate_market_fill(
            exchange=event.exchange,
            instrument=event.instrument,
            side=str(signal.side),
            close_price=float(event.row["close"]),
            fill_at=event.open_at,
            portfolio=portfolio,
            slippage_bps=config.slippage_bps,
            commission_bps=config.commission_bps,
            signal_strength=getattr(signal, "strength", None),
            signal_reason=getattr(signal, "reason", None),
        )
        if fill is not None:
            collector.record_trade(
                run_public_id=run_public_id,
                fill=fill,
                portfolio=portfolio,
                signal_public_id=sig_pid,
                session_id=tracker.session_id,
                sequence_id=tracker.next_sequence("bt"),
                bus_time=snapshot_as_of,
            )
        collector.record_signal(
            run_public_id=run_public_id,
            public_id=sig_pid,
            signal_time=event.open_at,
            signal_type=str(signal.side),
            instrument=event.instrument,
            price=float(event.row["close"]),
            indicators=getattr(signal, "indicators", {}),
            session_id=tracker.session_id,
            sequence_id=tracker.next_sequence("bt"),
            bus_time=snapshot_as_of,
        )

    collector.maybe_record_equity(
        run_public_id=run_public_id,
        point_time=batch[0].open_at,
        portfolio=portfolio,
        latest_closes=latest_closes,
        session_id=tracker.session_id,
        sequence_id=tracker.next_sequence("bt"),
        bus_time=snapshot_as_of,
    )

    if emitter is not None:
        current_equity = collector.equity_points[-1]["equity"] if collector.equity_points else 0.0
        await emitter.on_candle_processed(
            equity=current_equity,
            signals_count=len(collector.signals),
            trades_count=len(collector.trades),
        )
