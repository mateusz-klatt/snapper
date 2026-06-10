"""BacktestRunnerProcess — one-shot process that orchestrates a single backtest.

Registered as a template process (never auto-started). The API handler
creates a pending run, builds a ProcessConfigModel with a unique name
(``backtest_runner_{public_id}``), and calls ``start_process(config)``
to launch the runner.

Lifecycle: pending → running → completed | failed | cancelled.
"""

import asyncio
import contextlib
import traceback
from datetime import UTC
from datetime import datetime
from typing import Any
from typing import cast

import zmq
import zmq.asyncio
from loguru import logger
from sqlalchemy.exc import IntegrityError

from snapper.application.backtest.cancel import CancelProbe
from snapper.application.backtest.config import BacktestConfig
from snapper.application.backtest.config import BacktestExecutionMode
from snapper.application.backtest.config import BacktestFillModel
from snapper.application.backtest.direct_engine import DirectDbEngine
from snapper.application.backtest.metrics import compute_metrics
from snapper.application.backtest.progress import BacktestProgressEmitter
from snapper.application.backtest.progress import PublishFn
from snapper.application.backtest.progress import noop_publish
from snapper.application.backtest.result_collector import ResultCollector
from snapper.application.backtest.zmq_engine import ZmqReplayEngine
from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.process_manager.registry import register_process
from snapper.config.settings import AppSettings
from snapper.config.settings import get_bootstrap_settings
from snapper.core.types import BacktestRunStatusEnum
from snapper.core.types import ProcessLifecycleEnum
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRoleEnum
from snapper.data.backtest_conflict import is_single_running_conflict
from snapper.data.backtest_repository import BacktestRepository
from snapper.data.repository import Repository
from snapper.data.repository import get_repository
from snapper.data.repository_types import BacktestResultInsertRow
from snapper.data.repository_types import BacktestRunRow
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.infrastructure.validated_socket import HWM_MARKET_DATA
from snapper.messaging.infrastructure.validated_socket import ValidatedPublisher
from snapper.messaging.infrastructure.validated_socket import apply_hwm
from snapper.messaging.schemas.data import BacktestProgressData

_BT_EVENTS_STREAM = "backtest_events"
_BT_STATUS_STREAM = "backtest_status"
_BT_ARTIFACTS_STREAM = "backtest_artifacts"


async def _count_expected_batches(
    config: BacktestConfig, repository: Repository, snapshot_as_of: datetime
) -> int | None:
    """Pre-compute the expected number of time-batch ticks for progress.

    The runner fires ``on_candle_processed`` once per ``process_time_batch``
    call, which is once per unique ``open_at`` timestamp across all
    (exchange, instrument) pairs in the config. This helper counts those
    unique timestamps up-front so the emitter can compute ``progress_pct``
    and trigger 25 / 50 / 75 pct milestones.
    Returns ``None`` on any query failure so the runner silently degrades
    (milestones disabled, ``progress_pct`` pinned at 0.0) instead of
    aborting the run. ``total_candles`` is pre-computed by the runner via
    a cheap repository count before ``engine.run()``; ``None`` only if the
    count query fails.
    """
    try:
        open_times: set[datetime] = set()
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
                open_times.update(row["open_at"] for row in rows)
    except Exception as exc:
        logger.warning(
            "Backtest {} pre-run candle count failed ({}) — milestones disabled",
            config.strategy_class,
            exc,
        )
        return None
    return len(open_times) or None


def run_to_config_dict(run: BacktestRunRow) -> dict[str, Any]:
    """Convert a BacktestRunRow into a dict suitable for BacktestConfig.

    Maps the flat DB row (single exchange/instrument) into the nested
    ``instruments`` dict expected by BacktestConfig.

    Args:
        run: BacktestRunRow from the repository.

    Returns:
        Dict that can be passed to ``BacktestConfig.model_validate()``.
    """
    return {
        "strategy_class": run["strategy_name"],
        "instruments": {run["exchange"]: [run["instrument_public_id"]]},
        "start_date": run["start_date"],
        "end_date": run["end_date"],
        "wallet_public_id": run["wallet_public_id"],
        "operator_public_id": run["operator_public_id"],
        "initial_balance": run["initial_cash"],
        "strategy_params": run["strategy_params"],
        "timeframe": run["timeframe"],
        "execution_mode": BacktestExecutionMode(run["execution_mode"]),
        "fill_model": BacktestFillModel(run["fill_model"]),
        "slippage_bps": run["slippage_bps"],
        "commission_bps": run["commission_bps"],
        "target_execution_exchange": run.get("target_execution_exchange"),
    }


@register_process(
    "backtest_runner",
    method="start",
    description="One-shot backtest runner (template, never auto-started)",
    priority=50,
    lifecycle=ProcessLifecycleEnum.ONE_SHOT,
    role=ProcessRoleEnum.BACKTEST,
    tags=("backtest", "strategy", "analysis"),
    enabled=False,
    mode=ProcessModeEnum.THREAD,
)
class BacktestRunnerProcess(RegisterableProcess):
    """Orchestrates a single backtest run.

    Reads config from the backtest_runs row, executes the engine,
    persists artifacts + metrics, and sets terminal status.
    """

    def __init__(
        self,
        run_public_id: str,
        db_url: str,
        progress_publish: PublishFn | None = None,
    ) -> None:
        """Initialize the runner.

        Args:
            run_public_id: Public ID of the backtest run to execute.
            db_url: Database URL for repository access.
            progress_publish: Optional override for the WS
                progress publisher. When supplied (typically by tests)
                the runner uses it verbatim. When ``None`` (the
                production path), ``start()`` constructs an owned ZMQ
                PUB socket wired to the broker XSUB endpoint from
                bootstrap settings and publishes
                ``backtest.{wallet}.{run}.{event}`` frames through a
                ``MessagePublisher``. The owned socket is closed in the
                ``start()`` ``finally`` block. Passing
                ``noop_publish`` explicitly opts out of the live
                publisher for tests that only care about lifecycle.
        """
        self._run_public_id = run_public_id
        self._db_url = db_url
        self._tracker = SequenceTracker()
        self._progress_publish_override: PublishFn | None = progress_publish
        self._owned_zmq_context: zmq.asyncio.Context | None = None
        self._owned_validated_publisher: ValidatedPublisher | None = None
        self._owned_message_publisher: MessagePublisher | None = None
        self._last_status_time: datetime | None = None

    @staticmethod
    def get_default_parameters(settings: AppSettings) -> dict[str, Any]:
        """Return empty defaults — all params come from the API handler.

        Args:
            settings: Application settings (unused).

        Returns:
            Empty parameter dict.
        """
        return {}

    def get_status(self) -> dict[str, Any]:
        """Return current runner status.

        Returns:
            Dict with run_public_id.
        """
        return {"run_public_id": self._run_public_id}

    def _next_status_time(self) -> datetime:
        """Return a non-regressing timestamp for run status transitions."""
        current = datetime.now(UTC)
        if self._last_status_time is not None:
            current = max(current, self._last_status_time)
        self._last_status_time = current
        return current

    def _resolve_progress_publish(self) -> PublishFn:
        """Return the PublishFn the emitter will use.

        Explicit test overrides win; otherwise the runner opens its own
        ZMQ PUB socket against the broker XSUB endpoint from bootstrap
        settings and returns a ``MessagePublisher.send``-shaped
        adapter. The owned context + sockets are stored on ``self`` so
        ``_teardown_owned_publisher`` can close them on exit.
        """
        if self._progress_publish_override is not None:
            return self._progress_publish_override
        bootstrap = get_bootstrap_settings()
        broker_addr = bootstrap.zmq_broker_xsub
        try:
            context = zmq.asyncio.Context()
            raw_socket = context.socket(zmq.PUB)
            apply_hwm(raw_socket, sndhwm=HWM_MARKET_DATA)
            raw_socket.connect(broker_addr)
            validated = ValidatedPublisher(raw_socket)
            msg_publisher = MessagePublisher(validated, self._tracker)
        except Exception as exc:
            logger.warning(
                "Backtest {} progress publisher wiring failed ({}) — falling back to noop",
                self._run_public_id[:8],
                exc,
            )
            return noop_publish
        self._owned_zmq_context = context
        self._owned_validated_publisher = validated
        self._owned_message_publisher = msg_publisher
        run_short = self._run_public_id[:8]

        async def _publish(topic: str, data: BacktestProgressData) -> None:
            """Send one progress frame, logging and dropping send failures."""
            try:
                await msg_publisher.send(topic, data)
            except Exception as exc:
                logger.warning(
                    "Backtest {} progress publish on {} failed ({}) — dropped",
                    run_short,
                    topic,
                    exc,
                )

        return _publish

    def _teardown_owned_publisher(self) -> None:
        """Close the owned ZMQ publisher + context if they were opened."""
        if self._owned_validated_publisher is not None:
            with contextlib.suppress(Exception):
                self._owned_validated_publisher.close()
            self._owned_validated_publisher = None
        if self._owned_zmq_context is not None:
            with contextlib.suppress(Exception):
                self._owned_zmq_context.term()
            self._owned_zmq_context = None
        self._owned_message_publisher = None

    async def start(self) -> None:
        """Execute the backtest lifecycle.

        Transitions: pending → running → completed | failed | cancelled.
        On success, persists signals, trades, equity, and result metrics.
        On failure, records error details as an event.
        On cancellation (asyncio.CancelledError), transitions to cancelled.
        """
        repository = get_repository(self._db_url)
        bt_repo = BacktestRepository(cast(Any, repository).session_factory)
        now = self._next_status_time()

        run = await bt_repo.get_run(self._run_public_id, as_of=now)
        if run is None:
            logger.error("Backtest run {} not found — aborting", self._run_public_id[:8])
            return

        if run["status"] == BacktestRunStatusEnum.CANCEL_REQUESTED:
            logger.info(
                "Backtest {} cancel_requested before runner start — short-circuiting "
                "to cancelled without starting engine",
                self._run_public_id[:8],
            )
            await bt_repo.update_run_status(
                public_id=run["public_id"],
                new_status=BacktestRunStatusEnum.CANCELLED,
                bus_time=now,
                session_id=self._tracker.session_id,
                sequence_id=self._tracker.next_sequence(_BT_STATUS_STREAM),
                completed_at=now,
            )
            return

        publish_fn = self._resolve_progress_publish()
        emitter: BacktestProgressEmitter | None = None
        try:
            config = BacktestConfig.model_validate(run_to_config_dict(run))

            evt_sid = self._tracker.session_id
            evt_seq = self._tracker.next_sequence(_BT_EVENTS_STREAM)
            await bt_repo.insert_event(
                row={
                    "run_public_id": run["public_id"],
                    "event_type": "run_started",
                    "detail": {"strategy": config.strategy_class},
                    "session_id": evt_sid,
                    "sequence_id": evt_seq,
                    "timestamp": now,
                },
                bus_time=now,
                session_id=evt_sid,
                sequence_id=evt_seq,
            )
            try:
                await bt_repo.update_run_status(
                    public_id=run["public_id"],
                    new_status="running",
                    bus_time=now,
                    session_id=self._tracker.session_id,
                    sequence_id=self._tracker.next_sequence(_BT_STATUS_STREAM),
                    started_at=now,
                )
            except IntegrityError as e:
                if not is_single_running_conflict(e):
                    raise
                fail_now = self._next_status_time()
                conflict_task = asyncio.create_task(
                    bt_repo.update_run_status(
                        public_id=run["public_id"],
                        new_status="failed",
                        bus_time=fail_now,
                        session_id=self._tracker.session_id,
                        sequence_id=self._tracker.next_sequence(_BT_STATUS_STREAM),
                        completed_at=fail_now,
                        error="another backtest is already running (uq_bt_single_running)",
                    )
                )
                with contextlib.suppress(asyncio.CancelledError):
                    await asyncio.shield(conflict_task)
                logger.info(
                    "Backtest {} rejected: another run in progress",
                    run["public_id"][:8],
                )
                return

            cancel_probe = CancelProbe(
                bt_repo=bt_repo,
                run_public_id=run["public_id"],
                cancel_poll_ms=config.cancel_poll_ms,
            )
            total_candles = await _count_expected_batches(config, repository, now)
            emitter = BacktestProgressEmitter(
                run_public_id=run["public_id"],
                wallet_public_id=run["wallet_public_id"],
                total_candles=total_candles,
                tracker=self._tracker,
                publish=publish_fn,
            )
            await emitter.on_started()
            engine: DirectDbEngine | ZmqReplayEngine
            if config.execution_mode == BacktestExecutionMode.ZMQ_REPLAY:
                engine = ZmqReplayEngine(
                    repository, now, cancel_probe=cancel_probe, emitter=emitter
                )
            else:
                engine = DirectDbEngine(repository, now, cancel_probe=cancel_probe, emitter=emitter)
            collector = ResultCollector()
            await engine.run(run["public_id"], config, collector)
            await self._persist_artifacts(bt_repo, run["public_id"], collector, config)
            final_now = self._next_status_time()
            await bt_repo.update_run_status(
                public_id=run["public_id"],
                new_status="completed",
                bus_time=final_now,
                session_id=self._tracker.session_id,
                sequence_id=self._tracker.next_sequence(_BT_STATUS_STREAM),
                completed_at=final_now,
            )
            await emitter.on_terminal("completed")
            logger.info(
                "Backtest {} completed: {} trades, {} equity points",
                self._run_public_id[:8],
                len(collector.trades),
                len(collector.equity_points),
            )
        except asyncio.CancelledError:
            cancel_now = self._next_status_time()
            cancel_task = asyncio.create_task(
                bt_repo.update_run_status(
                    public_id=run["public_id"],
                    new_status="cancelled",
                    bus_time=cancel_now,
                    session_id=self._tracker.session_id,
                    sequence_id=self._tracker.next_sequence(_BT_STATUS_STREAM),
                )
            )
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.shield(cancel_task)
            if emitter is not None:
                with contextlib.suppress(Exception):
                    await emitter.on_terminal("cancelled")
            logger.info("Backtest {} cancelled", self._run_public_id[:8])
            raise
        except Exception as exc:
            fail_now = self._next_status_time()
            error_msg = str(exc)[:1024]
            fail_evt_sid = self._tracker.session_id
            fail_evt_seq = self._tracker.next_sequence(_BT_EVENTS_STREAM)
            event_task = asyncio.create_task(
                bt_repo.insert_event(
                    row={
                        "run_public_id": run["public_id"],
                        "event_type": "run_failed",
                        "detail": {
                            "error": str(exc),
                            "traceback": traceback.format_exc()[:4096],
                        },
                        "session_id": fail_evt_sid,
                        "sequence_id": fail_evt_seq,
                        "timestamp": fail_now,
                    },
                    bus_time=fail_now,
                    session_id=fail_evt_sid,
                    sequence_id=fail_evt_seq,
                )
            )
            status_task = asyncio.create_task(
                bt_repo.update_run_status(
                    public_id=run["public_id"],
                    new_status="failed",
                    bus_time=fail_now,
                    session_id=self._tracker.session_id,
                    sequence_id=self._tracker.next_sequence(_BT_STATUS_STREAM),
                    error=error_msg,
                )
            )
            for task in (event_task, status_task):
                with contextlib.suppress(asyncio.CancelledError):
                    await asyncio.shield(task)
            if emitter is not None:
                with contextlib.suppress(Exception):
                    await emitter.on_terminal("failed")
            logger.error("Backtest {} failed: {}", self._run_public_id[:8], error_msg)
            raise
        finally:
            self._teardown_owned_publisher()

    async def _persist_artifacts(
        self,
        bt_repo: BacktestRepository,
        run_public_id: str,
        collector: ResultCollector,
        config: BacktestConfig,
    ) -> None:
        """Persist all collected artifacts and computed metrics.

        Args:
            bt_repo: Backtest repository.
            run_public_id: Run identifier.
            collector: Filled result collector.
            config: Backtest configuration (for initial_balance).
        """
        now = datetime.now(UTC)

        if collector.signals:
            await bt_repo.insert_signals_batch(
                collector.signals,
                bus_time=now,
                session_id=self._tracker.session_id,
                sequence_id=self._tracker.next_sequence(_BT_ARTIFACTS_STREAM),
            )
        if collector.trades:
            await bt_repo.insert_trades_batch(
                collector.trades,
                bus_time=now,
                session_id=self._tracker.session_id,
                sequence_id=self._tracker.next_sequence(_BT_ARTIFACTS_STREAM),
            )
        if collector.equity_points:
            await bt_repo.insert_equity_points_batch(
                collector.equity_points,
                bus_time=now,
                session_id=self._tracker.session_id,
                sequence_id=self._tracker.next_sequence(_BT_ARTIFACTS_STREAM),
            )

        metrics = compute_metrics(
            collector.equity_points,
            collector.trades,
            initial_balance=config.initial_balance,
        )
        extra_metrics: dict[str, Any] = {}
        if collector.cross_asset_blocked_fills > 0:
            extra_metrics["cross_asset_blocked_fills"] = collector.cross_asset_blocked_fills
        result_row = BacktestResultInsertRow(
            run_public_id=run_public_id,
            total_trades=metrics.total_trades,
            winning_trades=metrics.winning_trades,
            losing_trades=metrics.losing_trades,
            total_pnl=metrics.total_pnl,
            max_drawdown=metrics.max_drawdown,
            sharpe_ratio=metrics.sharpe_ratio,
            win_rate=metrics.win_rate,
            profit_factor=metrics.profit_factor,
            final_equity=metrics.final_equity,
            max_equity=metrics.max_equity,
            sortino_ratio=metrics.sortino_ratio,
            cagr=metrics.cagr,
            calmar_ratio=metrics.calmar_ratio,
            expectancy=metrics.expectancy,
            avg_trade_pnl=metrics.avg_trade_pnl,
            max_drawdown_duration_seconds=metrics.max_drawdown_duration_seconds,
            exposure_ratio=metrics.exposure_ratio,
            turnover_ratio=metrics.turnover_ratio,
            extra_metrics=extra_metrics,
            session_id=self._tracker.session_id,
            sequence_id=self._tracker.next_sequence(_BT_ARTIFACTS_STREAM),
            timestamp=now,
        )
        await bt_repo.insert_result(
            result_row,
            bus_time=now,
            session_id=self._tracker.session_id,
            sequence_id=self._tracker.next_sequence(_BT_ARTIFACTS_STREAM),
        )
        for warning in metrics.warnings:
            await bt_repo.insert_event(
                row={
                    "run_public_id": run_public_id,
                    "event_type": "metric_warning",
                    "detail": {"metric": warning.metric, "reason": warning.reason},
                    "session_id": self._tracker.session_id,
                    "sequence_id": self._tracker.next_sequence(_BT_EVENTS_STREAM),
                    "timestamp": now,
                },
                bus_time=now,
                session_id=self._tracker.session_id,
                sequence_id=self._tracker.next_sequence(_BT_EVENTS_STREAM),
            )
