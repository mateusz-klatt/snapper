"""In-memory artifact collector for backtest runs.

Buffers signals, trades, and equity points during engine execution.
On finalize(), computes aggregate metrics and persists all artifacts
atomically via BacktestRepository.
"""

from datetime import datetime

from snapper.application.backtest.fill_model import BacktestFill
from snapper.application.portfolio.models import PortfolioTracker
from snapper.data.repository_types import BacktestEquityPointInsertRow
from snapper.data.repository_types import BacktestSignalInsertRow
from snapper.data.repository_types import BacktestTradeInsertRow


class ResultCollector:
    """Buffers backtest artifacts in memory during engine execution.

    Signals, trades, and equity points are accumulated in lists.
    Events are flushed incrementally (not buffered here).
    finalize() is called by the runner after the engine completes.
    """

    def __init__(self) -> None:
        """Initialize empty artifact buffers."""
        self.signals: list[BacktestSignalInsertRow] = []
        self.trades: list[BacktestTradeInsertRow] = []
        self.equity_points: list[BacktestEquityPointInsertRow] = []
        self._last_equity_time: datetime | None = None

    def record_signal(
        self,
        run_public_id: str,
        public_id: str,
        signal_time: datetime,
        signal_type: str,
        instrument: str,
        price: float,
        indicators: dict[str, object],
        session_id: str,
        sequence_id: int,
        bus_time: datetime,
    ) -> None:
        """Buffer a strategy signal.

        Args:
            run_public_id: Backtest run identifier.
            public_id: Pre-allocated UUID7 for this signal; the matching trade
                row carries the same value in ``signal_public_id`` so engines
                emit FK-linkable artifact pairs.
            signal_time: When the signal was generated.
            signal_type: Signal direction (buy/sell/hold).
            instrument: Target instrument.
            price: Price at signal time.
            indicators: Strategy indicator values at signal time.
            session_id: Producer session ID.
            sequence_id: Sequence counter.
            bus_time: Bus time for temporal tracking.
        """
        self.signals.append(
            BacktestSignalInsertRow(
                run_public_id=run_public_id,
                public_id=public_id,
                signal_time=signal_time,
                signal_type=signal_type,
                instrument=instrument,
                price=price,
                indicators=dict(indicators),
                session_id=session_id,
                sequence_id=sequence_id,
                timestamp=bus_time,
            )
        )

    def record_trade(
        self,
        run_public_id: str,
        fill: BacktestFill,
        portfolio: PortfolioTracker,
        signal_public_id: str | None,
        session_id: str,
        sequence_id: int,
        bus_time: datetime,
    ) -> None:
        """Buffer a simulated trade fill.

        Args:
            run_public_id: Backtest run identifier.
            fill: Simulated fill from fill_model.
            portfolio: Portfolio state after fill (for position_after).
            signal_public_id: Pre-allocated public_id of the signal that triggered
                this fill. Required (no default) so callers consciously pass the
                linkage; pass None only for synthetic fills with no originating
                signal.
            session_id: Producer session ID.
            sequence_id: Sequence counter.
            bus_time: Bus time for temporal tracking.
        """
        self.trades.append(
            BacktestTradeInsertRow(
                run_public_id=run_public_id,
                executed_at=fill.fill_at,
                instrument=fill.instrument,
                side=fill.side,
                quantity=fill.size,
                price=fill.price,
                fee=fill.fee,
                pnl=fill.pnl,
                position_after=portfolio.position_qty(fill.instrument),
                signal_public_id=signal_public_id,
                session_id=session_id,
                sequence_id=sequence_id,
                timestamp=bus_time,
            )
        )

    def maybe_record_equity(
        self,
        run_public_id: str,
        point_time: datetime,
        portfolio: PortfolioTracker,
        latest_closes: dict[str, float],
        session_id: str,
        sequence_id: int,
        bus_time: datetime,
    ) -> None:
        """Buffer an equity point if the timestamp has advanced.

        Only records one equity point per unique timestamp to avoid
        duplicate entries when multiple candles share the same time.

        Args:
            run_public_id: Backtest run identifier.
            point_time: Candle timestamp.
            portfolio: Current portfolio state.
            latest_closes: Latest close prices per instrument.
            session_id: Producer session ID.
            sequence_id: Sequence counter.
            bus_time: Bus time for temporal tracking.
        """
        if self._last_equity_time == point_time:
            return
        self._last_equity_time = point_time

        equity = portfolio.equity(prices=latest_closes)
        self.equity_points.append(
            BacktestEquityPointInsertRow(
                run_public_id=run_public_id,
                point_time=point_time,
                equity=equity,
                cash=portfolio.cash,
                position_value=equity - portfolio.cash,
                drawdown=0.0,
                session_id=session_id,
                sequence_id=sequence_id,
                timestamp=bus_time,
            )
        )
