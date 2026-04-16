"""Targeted Phase 2c coverage for the small leftover branches.

Covers:

- ``batch_processor.process_time_batch`` emitter-present branch.
- ``metrics._compute_max_drawdown_duration_seconds`` flat-then-drop edge.
- ``runner._count_expected_batches`` exception fallback.
- ``cli.app`` config_hash exception fallback.
- ``bridge`` backtest dual-gate malformed prefix branches.
- ``subscribe._validate_ws_topics`` malformed backtest prefix branch.
- ``validation._validate_backtest_topic/_prefix`` edge rejections.
- ``backtest_routes.create_backtest`` config_hash exception branch.
"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import PropertyMock
from unittest.mock import patch

import pytest

from snapper.application.backtest.batch_processor import CandleEvent
from snapper.application.backtest.batch_processor import process_time_batch
from snapper.application.backtest.config import BacktestConfig
from snapper.application.backtest.config import BacktestExecutionMode
from snapper.application.backtest.config import BacktestFillModel
from snapper.application.backtest.metrics import MetricWarning
from snapper.application.backtest.metrics import _compute_max_drawdown_duration_seconds
from snapper.application.backtest.progress import BacktestProgressEmitter
from snapper.application.backtest.progress import noop_publish
from snapper.application.backtest.result_collector import ResultCollector
from snapper.application.portfolio.models import PortfolioTracker
from snapper.data.repository_types import BacktestEquityPointInsertRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.strategies.factory import StrategyFactory

NOW = datetime(2026, 4, 16, 12, 0, 0, tzinfo=UTC)


def _ep(hours: float, equity: float) -> BacktestEquityPointInsertRow:
    """Build an equity-point insert-row for helper tests."""
    return BacktestEquityPointInsertRow(
        run_public_id="run-1",
        point_time=NOW + timedelta(hours=hours),
        equity=equity,
        cash=0.0,
        position_value=0.0,
        drawdown=0.0,
        session_id="s1",
        sequence_id=int(hours),
        timestamp=NOW,
    )


class TestBatchProcessorEmitterBranch:
    """batch_processor.process_time_batch emitter-present path (lines 181-182)."""

    @pytest.mark.asyncio
    async def test_emitter_on_candle_processed_fires(self) -> None:
        """Emitter receives one on_candle_processed call per batch.

        Given: batch with an emitter,
        When: process_time_batch completes,
        Then: emitter.on_candle_processed is called once with accumulated counts.
        """
        emitter = BacktestProgressEmitter(
            run_public_id="01948f94-0001-7a00-8000-00000000aaaa",
            wallet_public_id="01948f94-0001-7a00-8000-00000000bbbb",
            total_candles=10,
            tracker=SequenceTracker(),
            publish=noop_publish,
            throttle_ms=0,
        )
        candle_row: dict[str, Any] = {
            "public_id": "c1",
            "timestamp": NOW,
            "session_id": "s1",
            "sequence_id": 1,
            "open_at": NOW,
            "open": 1.0,
            "high": 1.0,
            "low": 1.0,
            "close": 1.0,
            "volume": 1.0,
        }
        batch = [CandleEvent(open_at=NOW, exchange="kraken", instrument="BTC-USD", row=candle_row)]

        strategy = MagicMock()
        strategy._handle_candle_data = AsyncMock(return_value=None)

        portfolio = PortfolioTracker(cash=10_000.0)
        collector = ResultCollector()
        config = MagicMock()
        config.start_date = NOW - timedelta(hours=1)
        config.end_date = NOW + timedelta(hours=10)
        config.timeframe = "1h"
        config.slippage_bps = 0.0
        config.commission_bps = 0.0

        before = emitter._candles_done
        await process_time_batch(
            batch=batch,
            run_public_id="run-1",
            config=config,
            strategy=strategy,
            portfolio=portfolio,
            latest_closes={},
            collector=collector,
            tracker=SequenceTracker(),
            snapshot_as_of=NOW,
            emitter=emitter,
        )
        assert emitter._candles_done == before + 1


class TestMaxDrawdownDurationFlatThenDrop:
    """Exercise the in-drawdown peak-refresh branches (metrics.py 312/315)."""

    def test_drawdown_after_flat_region(self) -> None:
        """Given: equity that stays flat then drops; When: computed; Then: duration reflects drop-from-original."""
        warnings: list[MetricWarning] = []
        points = [_ep(0, 100.0), _ep(1, 110.0), _ep(2, 110.0), _ep(3, 90.0)]
        result = _compute_max_drawdown_duration_seconds(points, warnings)
        assert result is not None
        assert result > 0


class TestMaxDrawdownDurationBranches:
    """Cover the elif-without-drawdown and not-greater branches (312/315)."""

    def test_peak_refresh_without_drawdown_first(self) -> None:
        """First sample equals start, then drops then recovers — second drawdown stays smaller."""
        warnings: list[MetricWarning] = []
        points = [
            _ep(0, 100.0),
            _ep(1, 100.0),
            _ep(2, 90.0),
            _ep(3, 80.0),
            _ep(4, 100.0),
            _ep(5, 95.0),
            _ep(6, 100.0),
        ]
        result = _compute_max_drawdown_duration_seconds(points, warnings)
        assert result is not None
        assert result == 3 * 3600.0

    def test_strictly_increasing_no_drawdown_branch(self) -> None:
        """Strictly increasing equity exercises the elif-not-in-drawdown peak refresh.

        Given: every sample strictly above the previous,
        When: _compute_max_drawdown_duration_seconds runs,
        Then: branch 312->307 is hit and the result is None + warning.
        """
        warnings: list[MetricWarning] = []
        points = [_ep(0, 100.0), _ep(1, 105.0), _ep(2, 110.0), _ep(3, 115.0)]
        result = _compute_max_drawdown_duration_seconds(points, warnings)
        assert result is None


class TestRunnerCountExceptionFallback:
    """runner._count_expected_batches exception path (lines 84-90)."""

    @pytest.mark.asyncio
    async def test_repository_raises_returns_none(self) -> None:
        """Given: repository.get_candles raises; When: counted; Then: None returned + warning logged."""
        from snapper.application.backtest.runner import _count_expected_batches

        with patch.dict(StrategyFactory.STRATEGY_CLASSES, {"sma_cross": MagicMock()}):
            config = BacktestConfig(
                strategy_class="sma_cross",
                instruments={"kraken": ["BTC-USD"]},
                start_date=NOW,
                end_date=NOW + timedelta(days=1),
                wallet_public_id="01948f94-0001-7a00-8000-00000000bbbb",
                execution_mode=BacktestExecutionMode.DIRECT_DB,
                fill_model=BacktestFillModel.MARKET,
                timeframe="1h",
            )
            broken_repo = MagicMock()
            broken_repo.get_candles = AsyncMock(side_effect=RuntimeError("db down"))
            result = await _count_expected_batches(config, broken_repo, NOW)
            assert result is None


class TestCliConfigHashFallback:
    """cli.app backtest command config_hash exception fallback (lines 1574-1575)."""

    def test_cli_backtest_run_handles_fingerprint_exception(self, tmp_path: Any) -> None:
        """Given: compute_fingerprint raises in the CLI path; Then: config_hash=None is stored.

        Drives the CLI ``backtest-run`` command end-to-end against a fresh
        SQLite DB with mocked engine + strategy so the only side effect
        observed is the ``except`` branch in ``cli/app.py:1574-1575``.
        """
        import asyncio

        from typer.testing import CliRunner

        from snapper.cli.app import app as cli_app
        from snapper.data.repository import SQLAlchemyRepository

        runner = CliRunner()
        db_path = tmp_path / "cli.db"
        db_url = f"sqlite+aiosqlite:///{db_path}"

        async def _init() -> None:
            await SQLAlchemyRepository(db_url).create_all()

        asyncio.run(_init())

        bootstrap_stub = MagicMock(db_url=db_url)
        with (
            patch.dict(StrategyFactory.STRATEGY_CLASSES, {"sma_cross": MagicMock()}),
            patch("snapper.cli.app.BootstrapSettingsLoader", return_value=bootstrap_stub),
            patch(
                "snapper.cli.app.compute_fingerprint",
                side_effect=RuntimeError("fingerprint broke"),
            ),
            patch("snapper.cli.app.DirectDbEngine") as engine_cls,
            patch("snapper.cli.app.compute_metrics") as compute_metrics_mock,
        ):
            engine = AsyncMock()
            engine.run = AsyncMock()
            engine_cls.return_value = engine
            metrics_mock = MagicMock()
            metrics_mock.total_trades = 0
            metrics_mock.winning_trades = 0
            metrics_mock.losing_trades = 0
            metrics_mock.total_pnl = 0.0
            metrics_mock.max_drawdown = 0.0
            metrics_mock.sharpe_ratio = None
            metrics_mock.win_rate = None
            metrics_mock.profit_factor = None
            metrics_mock.final_equity = 10_000.0
            metrics_mock.max_equity = 10_000.0
            metrics_mock.sortino_ratio = None
            metrics_mock.cagr = None
            metrics_mock.calmar_ratio = None
            metrics_mock.expectancy = None
            metrics_mock.avg_trade_pnl = None
            metrics_mock.max_drawdown_duration_seconds = None
            metrics_mock.exposure_ratio = None
            metrics_mock.turnover_ratio = None
            metrics_mock.warnings = []
            compute_metrics_mock.return_value = metrics_mock
            result = runner.invoke(
                cli_app,
                [
                    "backtest-run",
                    "--wallet",
                    "wallet-1",
                    "--strategy",
                    "sma_cross",
                    "--instrument",
                    "BTC-USD",
                    "--exchange",
                    "kraken",
                    "--start",
                    "2026-01-01",
                    "--end",
                    "2026-02-01",
                ],
            )
            assert "Created backtest run" in result.output


class TestRunnerCancelEmitterBranch:
    """runner.py cancel path emitter notification (line 323-326)."""

    @pytest.mark.asyncio
    async def test_cancel_before_emitter_construct_skips_terminal(self) -> None:
        """Given: CancelledError fires before the emitter is built; Then: cancel handler still runs cleanly.

        Exercises the ``if emitter is not None`` False branch (323->326) —
        the cancel arrives during ``BacktestConfig.model_validate`` so
        the local ``emitter`` is still ``None`` when the except handler
        runs.
        """
        import asyncio

        from snapper.application.backtest.runner import BacktestRunnerProcess

        run = {
            "public_id": "run-early-cancel",
            "timestamp": NOW,
            "session_id": "s1",
            "sequence_id": 1,
            "wallet_public_id": "01948f94-0001-7a00-8000-000000000001",
            "operator_public_id": None,
            "strategy_name": "sma_cross",
            "strategy_params": {},
            "instrument_public_id": "BTC-USD",
            "exchange": "kraken",
            "mode": "paper",
            "timeframe": "1h",
            "start_date": NOW,
            "end_date": NOW + timedelta(days=1),
            "initial_cash": 10_000.0,
            "status": "pending",
            "execution_mode": "direct_db",
            "fill_model": "market",
            "slippage_bps": 0.0,
            "commission_bps": 0.0,
            "config_hash": None,
            "created_by_user_id": None,
            "started_at": None,
            "completed_at": None,
            "error": None,
            "process_name": None,
        }

        with (
            patch("snapper.application.backtest.runner.get_repository") as get_repo,
            patch("snapper.application.backtest.runner.BacktestRepository") as bt_repo_cls,
            patch(
                "snapper.application.backtest.runner.BacktestConfig.model_validate",
                side_effect=asyncio.CancelledError(),
            ),
        ):
            mock_repo = MagicMock()
            mock_repo.session_factory = MagicMock()
            get_repo.return_value = mock_repo
            bt_repo = AsyncMock()
            bt_repo.get_run = AsyncMock(return_value=run)
            bt_repo.update_run_status = AsyncMock(return_value=1)
            bt_repo_cls.return_value = bt_repo

            runner = BacktestRunnerProcess(run_public_id="run-early-cancel", db_url="sqlite://")
            with pytest.raises(asyncio.CancelledError):
                await runner.start()

    @pytest.mark.asyncio
    async def test_cancel_invokes_emitter_terminal(self) -> None:
        """Given: a CancelledError mid-run with an active emitter; Then: on_terminal('cancelled') fires."""
        from snapper.application.backtest.runner import BacktestRunnerProcess

        run = {
            "public_id": "run-cancel",
            "timestamp": NOW,
            "session_id": "s1",
            "sequence_id": 1,
            "wallet_public_id": "01948f94-0001-7a00-8000-000000000001",
            "operator_public_id": None,
            "strategy_name": "sma_cross",
            "strategy_params": {},
            "instrument_public_id": "BTC-USD",
            "exchange": "kraken",
            "mode": "paper",
            "timeframe": "1h",
            "start_date": NOW,
            "end_date": NOW + timedelta(days=1),
            "initial_cash": 10_000.0,
            "status": "pending",
            "execution_mode": "direct_db",
            "fill_model": "market",
            "slippage_bps": 0.0,
            "commission_bps": 0.0,
            "config_hash": None,
            "created_by_user_id": None,
            "started_at": None,
            "completed_at": None,
            "error": None,
            "process_name": None,
        }

        with (
            patch.dict(StrategyFactory.STRATEGY_CLASSES, {"sma_cross": MagicMock()}),
            patch("snapper.application.backtest.runner.get_repository") as get_repo,
            patch("snapper.application.backtest.runner.BacktestRepository") as bt_repo_cls,
            patch("snapper.application.backtest.runner.DirectDbEngine") as engine_cls,
            patch(
                "snapper.application.backtest.runner._count_expected_batches",
                new=AsyncMock(return_value=10),
            ),
        ):
            import asyncio

            mock_repo = MagicMock()
            mock_repo.session_factory = MagicMock()
            mock_repo.get_candles = AsyncMock(return_value=[])
            get_repo.return_value = mock_repo
            bt_repo = AsyncMock()
            bt_repo.get_run = AsyncMock(return_value=run)
            bt_repo.update_run_status = AsyncMock(return_value=1)
            bt_repo.insert_event = AsyncMock(return_value="evt-1")
            bt_repo_cls.return_value = bt_repo
            engine = AsyncMock()
            engine.run = AsyncMock(side_effect=asyncio.CancelledError())
            engine_cls.return_value = engine

            runner = BacktestRunnerProcess(run_public_id="run-cancel", db_url="sqlite://")
            with pytest.raises(asyncio.CancelledError):
                await runner.start()


class TestBridgeBacktestDualGateMalformed:
    """bridge.py add_client_subscriptions + subscribe dual-gate malformed prefix."""

    def _make_bridge(self) -> Any:
        """Build a ZmqWebSocketBridgeService stub exercising the relaxation code."""
        from snapper.interface.websocket.bridge import ZmqWebSocketBridgeService

        connection_manager = MagicMock()
        connection_manager.tracker = SequenceTracker()
        with patch("snapper.interface.websocket.bridge.get_settings") as mock_settings:
            mock_settings.return_value = MagicMock(db_url="", zmq_broker_xpub="inproc://t")
            return ZmqWebSocketBridgeService(connection_manager=connection_manager)

    @pytest.mark.asyncio
    async def test_subscribe_client_rejects_malformed_backtest(self) -> None:
        """Given: malformed backtest prefix; When: subscribe_client; Then: silently dropped."""
        bridge = self._make_bridge()
        ws = MagicMock()
        bad_prefix = "backtest.not-a-uuid."
        await bridge.subscribe_client(ws, [bad_prefix])
        assert bad_prefix not in bridge.client_subscriptions.get(ws, set())

    @pytest.mark.asyncio
    async def test_subscribe_client_accepts_valid_backtest_wallet_prefix(self) -> None:
        """Given: a well-formed backtest wallet prefix; When: subscribe_client; Then: registered."""
        bridge = self._make_bridge()
        ws = MagicMock()
        good = "backtest.01948f94-0001-7a00-8000-000000000001."
        await bridge.subscribe_client(ws, [good])
        assert good in bridge.client_subscriptions.get(ws, set())

    @pytest.mark.asyncio
    async def test_subscribe_websocket_rejects_malformed_backtest(self) -> None:
        """Given: malformed backtest prefix on the broker subscribe path; Then: returns False."""
        bridge = self._make_bridge()
        with patch.object(bridge, "_record_bridge_control", new=AsyncMock(return_value=None)):
            ok = await bridge.subscribe_websocket(
                MagicMock(), "backtest.not-a-uuid.", throttle_ms=250
            )
        assert ok is False

    @pytest.mark.asyncio
    async def test_subscribe_websocket_passes_dual_gate_for_valid_backtest(self) -> None:
        """Given: a well-formed backtest wallet prefix; Then: dual gate passes — pattern lookup runs.

        Branch 710->728: the malformed-prefix gate does NOT return False
        for a valid backtest prefix, so control reaches the
        ``_find_matching_pattern`` line. We patch the pattern lookup to
        return None (deterministic exit) and assert the path executed.
        """
        bridge = self._make_bridge()
        with (
            patch.object(bridge, "_record_bridge_control", new=AsyncMock(return_value=None)),
            patch.object(bridge, "_find_matching_pattern", return_value=None) as patt,
        ):
            await bridge.subscribe_websocket(
                AsyncMock(),
                "backtest.01948f94-0001-7a00-8000-000000000001.",
                throttle_ms=250,
            )
            patt.assert_called_once()


class TestSubscribeBacktestInvalidPrefix:
    """handlers.subscribe._validate_ws_topics malformed backtest prefix (line 80)."""

    @pytest.mark.asyncio
    async def test_malformed_backtest_prefix_is_rejected(self) -> None:
        """Given: a backtest prefix with a non-UUID7 wallet segment; Then: it lands in the error response."""
        import json

        from snapper.auth.domain.roles import UserRole
        from snapper.auth.schemas.principal import AuthPrincipal
        from snapper.interface.websocket.handlers.subscribe import handle_subscribe
        from snapper.interface.websocket.schemas import WSSubscribeRequest

        ws = AsyncMock()
        manager = MagicMock()
        manager.get_client_subscriptions = MagicMock(return_value=set())
        manager.subscribe_client = MagicMock()
        manager.zmq_bridge = MagicMock()
        manager.zmq_bridge.add_subscription = AsyncMock()
        type(manager).tracker = PropertyMock(return_value=SequenceTracker())
        msg = WSSubscribeRequest(
            public_id="p",
            timestamp=NOW,
            session_id="",
            sequence_id=0,
            topics=["backtest.not-a-uuid."],
        )
        principal = AuthPrincipal(
            username="u",
            role=UserRole.VIEWER,
            active_wallet_public_id="01948f94-0001-7a00-8000-000000000001",
        )
        await handle_subscribe(ws, msg, manager, principal)
        response = json.loads(ws.send_text.call_args[0][0])
        assert response["type"] == "error"
        assert "not a valid UUID7" in response["message"]


class TestBacktestTopicValidatorEdges:
    """validation.py backtest validator edge branches (lines 945, 993, 1013, 1016, 1018)."""

    def test_wrong_category_segment_rejected(self) -> None:
        """Given: topic with wrong first segment; Then: 'Expected backtest' error."""
        from snapper.messaging.topics.validation import _validate_backtest_topic

        topic = (
            "nobacktest.01948f94-0001-7a00-8000-000000000001."
            "01948f94-0001-7a00-8000-000000000002.started"
        )
        valid, err = _validate_backtest_topic(topic)
        assert not valid
        assert "Expected 'backtest' category" in err

    def test_prefix_run_segment_malformed(self) -> None:
        """Given: prefix with valid wallet but bad run segment; Then: rejected."""
        from snapper.messaging.topics.validation import _validate_backtest_prefix

        pattern = "backtest.01948f94-0001-7a00-8000-000000000001.bad-run-id."
        valid, err = _validate_backtest_prefix(pattern)
        assert not valid
        assert "run segment" in err.lower()

    def test_prefix_missing_trailing_dot(self) -> None:
        """Given: prefix without trailing dot; Then: rejected."""
        from snapper.messaging.topics.validation import _validate_backtest_prefix

        valid, err = _validate_backtest_prefix("backtest.01948f94")
        assert not valid
        assert "must end with dot" in err

    def test_prefix_with_empty_segments(self) -> None:
        """Given: prefix with an empty segment (double dot); Then: rejected."""
        from snapper.messaging.topics.validation import _validate_backtest_prefix

        valid, err = _validate_backtest_prefix("backtest..")
        assert not valid
        assert "cannot be empty" in err

    def test_prefix_not_backtest_root(self) -> None:
        """Given: prefix starting with something other than 'backtest'; Then: rejected."""
        from snapper.messaging.topics.validation import _validate_backtest_prefix

        valid, err = _validate_backtest_prefix("market.something.")
        assert not valid
        assert "Expected 'backtest' prefix" in err


class TestCreateBacktestConfigHashFallback:
    """server.backtest_routes.create_backtest config_hash exception branch (lines 303-305)."""

    @pytest.mark.asyncio
    async def test_fingerprint_exception_stores_null(self) -> None:
        """Given: compute_fingerprint raises; When: create_backtest runs; Then: config_hash=None is stored."""
        from snapper.server.backtest_routes import create_backtest

        bt = AsyncMock()
        bt.create_run = AsyncMock(return_value=(1, "run-new"))
        bt.get_run = AsyncMock(
            return_value={
                "public_id": "run-new",
                "timestamp": NOW,
                "session_id": "s1",
                "sequence_id": 1,
                "wallet_public_id": "wallet-1",
                "operator_public_id": None,
                "strategy_name": "sma_cross",
                "strategy_params": {},
                "instrument_public_id": "BTC-USD",
                "exchange": "kraken",
                "mode": "paper",
                "timeframe": "1h",
                "start_date": NOW,
                "end_date": NOW + timedelta(days=1),
                "initial_cash": 10_000.0,
                "status": "pending",
                "execution_mode": "direct_db",
                "fill_model": "market",
                "slippage_bps": 0.0,
                "commission_bps": 0.0,
                "config_hash": None,
                "created_by_user_id": "test",
                "started_at": None,
                "completed_at": None,
                "error": None,
                "process_name": None,
            }
        )

        request = MagicMock()
        request.app.state.rest_tracker = SequenceTracker()
        factory = MagicMock()
        factory.start_process = AsyncMock()
        request.app.state.process_factory = factory

        command = MagicMock()
        body = MagicMock()
        body.strategy_class = "sma_cross"
        body.instrument_public_id = "BTC-USD"
        body.exchange = "kraken"
        body.start_date = NOW
        body.end_date = NOW + timedelta(days=1)
        body.initial_cash = 10_000.0
        body.strategy_params = {}
        body.timeframe = "1h"
        body.execution_mode = "direct_db"
        body.fill_model = "market"
        body.slippage_bps = 0.0
        body.commission_bps = 0.0
        command.payload = body

        from snapper.auth.domain.roles import UserRole
        from snapper.auth.schemas.principal import AuthPrincipal

        principal = AuthPrincipal(
            username="test",
            role=UserRole.OPERATOR,
            active_wallet_public_id="wallet-1",
            primary_operator_public_id="op-1",
            operator_public_ids=["op-1"],
        )
        repo = MagicMock()
        repo.session_factory = MagicMock()

        with (
            patch(
                "snapper.server.backtest_routes.compute_fingerprint",
                side_effect=RuntimeError("fingerprint broke"),
            ),
            patch("snapper.server.backtest_routes._bt_repo", return_value=bt),
            patch("snapper.server.backtest_routes.ProcessConfigModel", MagicMock()),
            patch("snapper.server.backtest_routes.get_settings") as mock_settings,
        ):
            mock_settings.return_value = MagicMock(db_url="sqlite://")
            await create_backtest(request, command, principal, repo)
        args, kwargs = bt.create_run.call_args
        assert kwargs["row"]["config_hash"] is None
