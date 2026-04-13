"""BacktestRunnerProcess — one-shot process that orchestrates a single backtest.

Registered as a template process (never auto-started). The API handler
creates a pending run, builds a ProcessConfigModel with a unique name
(``backtest_runner_{public_id}``), and calls ``start_process(config)``
to launch the runner.

Lifecycle: pending → running → completed | failed | cancelled.
"""

import traceback
from datetime import UTC
from datetime import datetime
from typing import Any
from typing import cast

from loguru import logger

from snapper.application.backtest.config import BacktestConfig
from snapper.application.backtest.direct_engine import DirectDbEngine
from snapper.application.backtest.metrics import compute_metrics
from snapper.application.backtest.result_collector import ResultCollector
from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.process_manager.registry import register_process
from snapper.config.settings import AppSettings
from snapper.core.types import ProcessLifecycleEnum
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRoleEnum
from snapper.data.backtest_repository import BacktestRepository
from snapper.data.repository import get_repository
from snapper.data.repository_types import BacktestResultInsertRow
from snapper.data.repository_types import BacktestRunRow
from snapper.messaging.infrastructure.publisher import SequenceTracker

_BT_EVENTS_STREAM = "backtest_events"
_BT_STATUS_STREAM = "backtest_status"
_BT_ARTIFACTS_STREAM = "backtest_artifacts"


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

    def __init__(self, run_public_id: str, db_url: str) -> None:
        """Initialize the runner.

        Args:
            run_public_id: Public ID of the backtest run to execute.
            db_url: Database URL for repository access.
        """
        self._run_public_id = run_public_id
        self._db_url = db_url
        self._tracker = SequenceTracker()

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

    async def start(self) -> None:
        """Execute the backtest lifecycle.

        Transitions: pending → running → completed | failed | cancelled.
        On success, persists signals, trades, equity, and result metrics.
        On failure, records error details as an event.
        """
        repository = get_repository(self._db_url)
        bt_repo = BacktestRepository(cast(Any, repository).session_factory)
        now = datetime.now(UTC)

        run = await bt_repo.get_run(self._run_public_id, as_of=now)
        if run is None:
            logger.error("Backtest run {} not found — aborting", self._run_public_id[:8])
            return

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
        await bt_repo.update_run_status(
            public_id=run["public_id"],
            new_status="running",
            bus_time=now,
            session_id=self._tracker.session_id,
            sequence_id=self._tracker.next_sequence(_BT_STATUS_STREAM),
            started_at=now,
        )

        try:
            engine = DirectDbEngine(repository, now)
            collector = ResultCollector()
            await engine.run(run["public_id"], config, collector)
            await self._persist_artifacts(bt_repo, run["public_id"], collector, config)
            final_now = datetime.now(UTC)
            await bt_repo.update_run_status(
                public_id=run["public_id"],
                new_status="completed",
                bus_time=final_now,
                session_id=self._tracker.session_id,
                sequence_id=self._tracker.next_sequence(_BT_STATUS_STREAM),
                completed_at=final_now,
            )
            logger.info(
                "Backtest {} completed: {} trades, {} equity points",
                self._run_public_id[:8],
                len(collector.trades),
                len(collector.equity_points),
            )
        except Exception as exc:
            fail_now = datetime.now(UTC)
            error_msg = str(exc)[:1024]
            fail_evt_sid = self._tracker.session_id
            fail_evt_seq = self._tracker.next_sequence(_BT_EVENTS_STREAM)
            await bt_repo.insert_event(
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
            await bt_repo.update_run_status(
                public_id=run["public_id"],
                new_status="failed",
                bus_time=fail_now,
                session_id=self._tracker.session_id,
                sequence_id=self._tracker.next_sequence(_BT_STATUS_STREAM),
                error=error_msg,
            )
            logger.error("Backtest {} failed: {}", self._run_public_id[:8], error_msg)
            raise

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
            extra_metrics={
                "sortino_ratio": metrics.sortino_ratio,
                "cagr": metrics.cagr,
                "calmar_ratio": metrics.calmar_ratio,
                "expectancy": metrics.expectancy,
                "avg_trade_pnl": metrics.avg_trade_pnl,
            },
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
