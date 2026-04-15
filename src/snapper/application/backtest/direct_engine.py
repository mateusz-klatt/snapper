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
from typing import cast

from loguru import logger

from snapper.application.backtest.batch_processor import CandleEvent
from snapper.application.backtest.batch_processor import candle_row_to_data
from snapper.application.backtest.batch_processor import process_time_batch
from snapper.application.backtest.cancel import CancelProbe
from snapper.application.backtest.config import BacktestConfig
from snapper.application.backtest.result_collector import ResultCollector
from snapper.application.portfolio.models import PortfolioTracker
from snapper.core.types import BacktestRunStatusEnum
from snapper.data.backtest_repository import BacktestRepository
from snapper.data.repository import Repository
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.strategies.base import BaseStrategy
from snapper.strategies.factory import StrategyFactory
from snapper.strategies.models import StrategyConfig

_CANCEL_PROBE_TIMEOUT_S: float = 1.0

__all__ = [
    "CandleEvent",
    "DirectDbEngine",
    "candle_row_to_data",
    "iter_sorted_candle_chunks",
]


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
        cancel_probe: CancelProbe | None = None,
    ) -> None:
        """Initialize engine with repository, snapshot time, and optional cancel polling.

        Args:
            repository: Database repository for candle/instrument queries.
            snapshot_as_of: Temporal snapshot for all DB reads.
            bt_repo: Optional BacktestRepository — kept for backwards
                compatibility with callers that still construct an inline
                probe via ``cancel_poll_ms``. Ignored when ``cancel_probe``
                is supplied.
            cancel_poll_ms: Throttle between probes when an inline probe is
                used. Ignored when ``cancel_probe`` is supplied.
            cancel_probe: Pre-built shared probe (Phase 2b-hardening Step 3).
                When provided, the engine delegates entirely to it; both
                Direct-DB and ZMQ replay engines can share the same instance
                so cancel detection is uniform.
        """
        self._repository = repository
        self._snapshot_as_of = snapshot_as_of
        self._cancel_probe: CancelProbe | None = cancel_probe
        self._bt_repo: BacktestRepository | None = bt_repo
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
        if self._cancel_probe is not None:
            await self._cancel_probe.check()
            return
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
        """Delegate to ``batch_processor.process_time_batch`` (Phase 2b Step 3).

        Kept as a thin instance wrapper so existing call sites and unit tests
        that drive ``await engine._process_time_batch(...)`` keep working
        without translating call signatures. The pure module-level helper is
        the single source of truth shared with ``ZmqReplayEngine``.
        """
        await process_time_batch(
            batch=batch,
            run_public_id=run_public_id,
            config=config,
            strategy=strategy,
            portfolio=portfolio,
            latest_closes=latest_closes,
            collector=collector,
            tracker=tracker,
            snapshot_as_of=self._snapshot_as_of,
        )
