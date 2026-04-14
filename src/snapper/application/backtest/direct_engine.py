"""Direct-DB backtest engine — candle-driven simulation loop.

Reads historical candles from the database, feeds them to a strategy
instance, simulates fills via the fill model, and collects results.
Phase 1 MVP: single execution mode, no ZMQ replay.
"""

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from time import monotonic
from typing import Any
from typing import NamedTuple
from typing import cast

from loguru import logger

from snapper.application.backtest.config import BacktestConfig
from snapper.application.backtest.fill_model import simulate_market_fill
from snapper.application.backtest.result_collector import ResultCollector
from snapper.application.portfolio.models import PortfolioTracker
from snapper.core.types import BacktestRunStatusEnum
from snapper.data.backtest_repository import BacktestRepository
from snapper.data.repository import Repository
from snapper.data.repository_types import CandleRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.data import CandleData
from snapper.strategies.base import BaseStrategy
from snapper.strategies.factory import StrategyFactory
from snapper.strategies.models import StrategyConfig

_CANCEL_PROBE_TIMEOUT_S: float = 1.0


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


async def iter_sorted_candle_chunks(
    config: BacktestConfig,
    repository: Repository,
    snapshot_as_of: datetime,
) -> AsyncIterator[list[CandleEvent]]:
    """Async generator yielding sorted candle chunks.

    Loads candles per (exchange, instruments) pair and yields them
    sorted by (open_at, exchange, instrument) for deterministic ordering.

    Args:
        config: Backtest configuration with instruments and date range.
        repository: Database repository for candle queries.
        snapshot_as_of: Temporal snapshot for all reads.
        warmup_bars: Extra bars before start_date for indicator warm-up.

    Yields:
        Lists of CandleEvent sorted by (open_at, exchange, instrument).
    """
    all_events: list[CandleEvent] = []
    for exchange, instruments in config.instruments.items():
        for instrument in instruments:
            rows = await repository.get_candles(
                instrument=instrument,
                timeframe=config.timeframe,
                start=None,
                end=config.end_date,
                exchange=cast(Any, exchange),
                as_of=snapshot_as_of,
                order="asc",
            )
            for row in rows:
                all_events.append(
                    CandleEvent(
                        open_at=row["open_at"],
                        exchange=exchange,
                        instrument=instrument,
                        row=row,
                    )
                )

    all_events.sort(key=lambda e: (e.open_at, e.exchange, e.instrument))
    if all_events:
        yield all_events


class DirectDbEngine:
    """Candle-driven backtest engine using direct DB reads.

    Instantiates a fresh strategy, feeds historical candles through it,
    simulates fills, and collects results. Returns (portfolio, latest_closes)
    for the runner to finalize.
    """

    def __init__(
        self,
        repository: Repository,
        snapshot_as_of: datetime,
        bt_repo: BacktestRepository | None = None,
        cancel_poll_ms: int = 500,
    ) -> None:
        """Initialize engine with repository, snapshot time, and optional cancel polling.

        Args:
            repository: Database repository for candle/instrument queries.
            snapshot_as_of: Temporal snapshot for all DB reads.
            bt_repo: Optional BacktestRepository for cooperative cancel polling.
                When provided, the engine probes ``get_run`` between time-batches
                and raises ``asyncio.CancelledError`` on ``cancel_requested``.
                Passing None disables polling (used by the CLI which has no
                background cancel mechanism).
            cancel_poll_ms: Minimum wall-time gap between cancel probes in
                milliseconds. Probes are attempted between each time-batch but
                skipped if the previous probe was within this window.
        """
        self._repository = repository
        self._snapshot_as_of = snapshot_as_of
        self._bt_repo = bt_repo
        self._cancel_poll_ms = cancel_poll_ms
        self._last_cancel_check_ms: float = 0.0

    async def run(
        self,
        run_public_id: str,
        config: BacktestConfig,
        collector: ResultCollector,
    ) -> tuple[PortfolioTracker, dict[str, float]]:
        """Execute the backtest simulation loop.

        Args:
            run_public_id: Public ID of the backtest run.
            config: Backtest configuration.
            collector: Result collector for buffering artifacts.

        Returns:
            Tuple of (final portfolio state, latest close prices).
        """
        all_instruments: list[str] = []
        for instruments in config.instruments.values():
            all_instruments.extend(instruments)

        first_exchange = next(iter(config.instruments))
        strategy = StrategyFactory.STRATEGY_CLASSES[config.strategy_class](
            StrategyConfig(
                name=f"bt_{run_public_id[:8]}",
                strategy_class=config.strategy_class,
                inputs=[f"candles.{first_exchange}.synthetic.{config.timeframe}"],
                outputs=all_instruments,
                exchange=cast(Any, "paper"),
                params=dict(config.strategy_params),
            )
        )

        portfolio = PortfolioTracker(cash=config.initial_balance)
        latest_closes: dict[str, float] = {}
        tracker = SequenceTracker()

        async for chunk in iter_sorted_candle_chunks(
            config, self._repository, self._snapshot_as_of
        ):
            prev_time: datetime | None = None
            time_batch: list[CandleEvent] = []

            for event in chunk:
                if prev_time is not None and event.open_at != prev_time and time_batch:
                    await self._maybe_check_cancel(run_public_id)
                    await self._process_time_batch(
                        time_batch,
                        run_public_id,
                        config,
                        strategy,
                        portfolio,
                        latest_closes,
                        collector,
                        tracker,
                    )
                    time_batch = []
                time_batch.append(event)
                prev_time = event.open_at

            if time_batch:
                await self._maybe_check_cancel(run_public_id)
                await self._process_time_batch(
                    time_batch,
                    run_public_id,
                    config,
                    strategy,
                    portfolio,
                    latest_closes,
                    collector,
                    tracker,
                )

        logger.info(
            "Backtest {} complete: {} signals, {} trades, {} equity points",
            run_public_id[:8],
            len(collector.signals),
            len(collector.trades),
            len(collector.equity_points),
        )
        return portfolio, latest_closes

    async def _maybe_check_cancel(self, run_public_id: str) -> None:
        """Probe ``backtest_runs`` for cancel_requested between time batches.

        Skips the probe when polling is disabled (``bt_repo`` is None), when
        the previous probe was within ``cancel_poll_ms``, or when the run is
        not in a cancel-requested state. Raises ``asyncio.CancelledError``
        when the run is marked for cancellation so the runner's existing
        handler transitions the status to ``cancelled``.

        The DB read is bounded by ``_CANCEL_PROBE_TIMEOUT_S``: if the
        repository hangs (lock contention, slow DB), the probe is skipped
        with a warning instead of stalling the engine. A future probe will
        retry; the user-visible cancel SLA degrades gracefully rather than
        hanging behind the SQLite driver's 30s lock timeout
        (see data/repository.py).

        Args:
            run_public_id: Run to probe.

        Raises:
            asyncio.CancelledError: When the run's status is
                ``cancel_requested``.
        """
        if self._bt_repo is None:
            return
        now_ms = monotonic() * 1000.0
        if now_ms - self._last_cancel_check_ms < self._cancel_poll_ms:
            return
        self._last_cancel_check_ms = now_ms
        try:
            run = await asyncio.wait_for(
                self._bt_repo.get_run(run_public_id, as_of=datetime.now(UTC)),
                timeout=_CANCEL_PROBE_TIMEOUT_S,
            )
        except TimeoutError:
            logger.warning(
                "Backtest {} cancel probe timed out after {}s — will retry next batch",
                run_public_id[:8],
                _CANCEL_PROBE_TIMEOUT_S,
            )
            return
        if run is not None and run["status"] == BacktestRunStatusEnum.CANCEL_REQUESTED:
            raise asyncio.CancelledError()

    async def _process_time_batch(
        self,
        batch: list[CandleEvent],
        run_public_id: str,
        config: BacktestConfig,
        strategy: BaseStrategy,
        portfolio: PortfolioTracker,
        latest_closes: dict[str, float],
        collector: ResultCollector,
        tracker: SequenceTracker,
    ) -> None:
        """Process all candle events at the same timestamp.

        Updates all close prices first, then feeds candles to strategy,
        then records equity once. This ensures multi-instrument equity
        snapshots use all close prices for the same timestamp.

        Args:
            batch: All CandleEvents at the same open_at.
            run_public_id: Backtest run identifier.
            config: Backtest configuration.
            strategy: Strategy instance.
            portfolio: Portfolio state (mutated).
            latest_closes: Latest close prices (mutated).
            collector: Result collector.
            tracker: Sequence tracker.
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
                    session_id=tracker.session_id,
                    sequence_id=tracker.next_sequence("bt"),
                    bus_time=self._snapshot_as_of,
                )
            collector.record_signal(
                run_public_id=run_public_id,
                signal_time=event.open_at,
                signal_type=str(signal.side),
                instrument=event.instrument,
                price=float(event.row["close"]),
                indicators=getattr(signal, "indicators", {}),
                session_id=tracker.session_id,
                sequence_id=tracker.next_sequence("bt"),
                bus_time=self._snapshot_as_of,
            )

        collector.maybe_record_equity(
            run_public_id=run_public_id,
            point_time=batch[0].open_at,
            portfolio=portfolio,
            latest_closes=latest_closes,
            session_id=tracker.session_id,
            sequence_id=tracker.next_sequence("bt"),
            bus_time=self._snapshot_as_of,
        )
