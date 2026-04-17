"""Wiring tests for DivergenceDetector hook sites.

Covers every observation path that the plan wires into the trade
runtime: engine created + dual_write / durable_notified publish,
outbox dispatcher durable_dispatched publish, and reconciliation
ok / failure verdicts. Plan:
``proprietary/plans/plan_shadow_write_divergence.md`` v1.3.
"""

from datetime import UTC
from datetime import datetime
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.application.engine.config import EngineConfigModel
from snapper.application.engine.service import TradingEngineService
from snapper.application.trade.divergence_detector import DivergenceDetector
from snapper.application.trade.outbox import OutboxDispatcher
from snapper.application.trade.reconciler import ReconciliationLoop
from snapper.application.trade.trade_service import TradeService
from snapper.data.repository_types import TradeCommandRow


class _SocketStub:
    """Minimal execution-socket stub matching TradingEngineService expectations."""

    def __init__(self) -> None:
        self.tracker = MagicMock(session_id="s-wire")
        self.tracker.next_sequence = MagicMock(return_value=1)
        self.sent: list[tuple[str, Any]] = []

    async def send(self, topic: str, order: Any, flags: int | None = None) -> None:
        """Capture the (topic, order) pair; ``flags`` is accepted for interface parity."""
        del flags
        self.sent.append((topic, order))


@pytest.mark.asyncio
async def test_send_order_dual_write_observes_created_and_dual_write() -> None:
    """Dual-write publish increments created + dual_write counters.

    Given: an engine with repo + detector but NO outbox,
    When: _send_order fires,
    Then: commands_created_total = 1 (post-insert_trade_command) AND
        commands_published_dual_write_total = 1 (post-ZMQ send);
        durable_notified / durable_dispatched stay at 0.
    """
    detector = DivergenceDetector()
    socket = _SocketStub()
    repo = AsyncMock()
    repo.insert_trade_command = AsyncMock(return_value=(1, "cmd-1"))
    engine = TradingEngineService(
        instrument="BTC-USD",
        execution_socket=cast(Any, socket),
        cfg=EngineConfigModel(initial_cash=1_000.0),
        exchange="paper",
        repository=repo,
        divergence_detector=detector,
    )
    await engine._send_order(side="buy", size=1.0, price=10.0, reason="test")

    snap = detector.snapshot()
    assert snap["commands_created_total"] == 1
    assert snap["commands_published_dual_write_total"] == 1
    assert snap["commands_published_durable_notified_total"] == 0
    assert snap["commands_published_durable_dispatched_total"] == 0


@pytest.mark.asyncio
async def test_send_order_durable_observes_created_and_durable_notified() -> None:
    """Durable publish increments created + durable_notified (not dual_write).

    Given: an engine with repo + outbox + detector,
    When: _send_order fires,
    Then: commands_created_total = 1 AND
        commands_published_durable_notified_total = 1;
        dual_write / durable_dispatched stay at 0.
    """
    detector = DivergenceDetector()
    socket = _SocketStub()
    repo = AsyncMock()
    repo.insert_trade_command = AsyncMock(return_value=(1, "cmd-2"))
    outbox = MagicMock()
    engine = TradingEngineService(
        instrument="BTC-USD",
        execution_socket=cast(Any, socket),
        cfg=EngineConfigModel(initial_cash=1_000.0),
        exchange="paper",
        repository=repo,
        outbox=outbox,
        divergence_detector=detector,
    )
    await engine._send_order(side="buy", size=1.0, price=10.0, reason="test")

    outbox.notify.assert_called_once()
    snap = detector.snapshot()
    assert snap["commands_created_total"] == 1
    assert snap["commands_published_durable_notified_total"] == 1
    assert snap["commands_published_dual_write_total"] == 0
    assert snap["commands_published_durable_dispatched_total"] == 0


def _trade_command_row(public_id: str = "cmd-d1") -> TradeCommandRow:
    """Build a minimal TradeCommandRow for outbox dispatch tests."""
    now = datetime.now(UTC)
    return {
        "public_id": public_id,
        "timestamp": now,
        "session_id": "s-wire",
        "sequence_id": 1,
        "command_type": "submit",
        "shard_key": "kraken.BTC-USD.live",
        "exchange": "kraken",
        "instrument": "BTC-USD",
        "mode": "live",
        "strategy_id": "unit-test",
        "client_order_id": "co-1",
        "venue_client_id": "co-1",
        "idempotency_key": None,
        "side": "buy",
        "order_type": "market",
        "quantity": 1.0,
        "price": None,
        "leverage": None,
        "reduce_only": False,
        "status": "created",
        "attempt_count": 0,
        "last_error": None,
        "created_at": now,
        "dispatched_at": None,
        "acked_at": None,
        "terminal_at": None,
        "exchange_order_id": None,
        "supersedes_command_id": None,
        "correlation_id": public_id,
        "wallet_public_id": None,
        "operator_public_id": None,
        "user_public_id": None,
    }


@pytest.mark.asyncio
async def test_outbox_dispatch_batch_observes_durable_dispatched() -> None:
    """Successful outbox publish increments durable_dispatched counter.

    Given: an OutboxDispatcher with a stub repo returning one command
        and a no-op publish_fn, wired to a DivergenceDetector,
    When: _dispatch_batch runs once,
    Then: commands_published_durable_dispatched_total = 1; other
        counters stay at 0.
    """
    detector = DivergenceDetector()
    repo = AsyncMock()
    repo.get_undispatched_commands = AsyncMock(return_value=[_trade_command_row()])
    repo.update_trade_command_status = AsyncMock()
    published: list[TradeCommandRow] = []

    async def _publish(cmd: TradeCommandRow) -> None:
        published.append(cmd)

    dispatcher = OutboxDispatcher(
        repository=repo,
        publish_fn=_publish,
        divergence_detector=detector,
    )
    await dispatcher._dispatch_batch()

    assert published
    repo.update_trade_command_status.assert_called_once()
    snap = detector.snapshot()
    assert snap["commands_published_durable_dispatched_total"] == 1
    assert snap["commands_published_dual_write_total"] == 0
    assert snap["commands_published_durable_notified_total"] == 0


@pytest.mark.asyncio
async def test_reconciliation_cycle_observes_ok_verdict() -> None:
    """Clean reconciliation cycle increments reconciliation_ok_total.

    Given: a ReconciliationLoop wired to a DivergenceDetector with a
        stub repo returning two active commands on distinct shards,
    When: _reconcile_cycle runs,
    Then: reconciliation_ok_total = 2 (one per distinct shard);
        reconciliation_failure_total stays at 0.
    """
    detector = DivergenceDetector()
    trade_service = TradeService()
    now = datetime.now(UTC)
    repo = AsyncMock()
    repo.get_active_commands_for_exchange = AsyncMock(
        return_value=[
            {
                "public_id": "c-1",
                "created_at": now,
                "status": "dispatched",
                "shard_key": "kraken.BTC-USD.live",
            },
            {
                "public_id": "c-2",
                "created_at": now,
                "status": "dispatched",
                "shard_key": "kraken.ETH-USD.live",
            },
        ]
    )
    loop = ReconciliationLoop(
        exchange_name="kraken",
        repository=repo,
        trade_service=trade_service,
        divergence_detector=detector,
    )
    await loop._reconcile_cycle()

    snap = detector.snapshot()
    assert snap["reconciliation_ok_total"] == 2
    assert snap["reconciliation_failure_total"] == 0


@pytest.mark.asyncio
async def test_reconciliation_cycle_observes_failure_verdict() -> None:
    """Failed reconciliation cycle increments reconciliation_failure_total.

    Given: a ReconciliationLoop wired to a DivergenceDetector whose
        repo raises on ``get_active_commands_for_exchange``,
    When: _reconcile_cycle runs,
    Then: reconciliation_failure_total = 1; reconciliation_ok_total
        stays at 0.
    """
    detector = DivergenceDetector()
    trade_service = TradeService()
    repo = AsyncMock()
    repo.get_active_commands_for_exchange = AsyncMock(side_effect=RuntimeError("boom"))
    loop = ReconciliationLoop(
        exchange_name="kraken",
        repository=repo,
        trade_service=trade_service,
        divergence_detector=detector,
    )
    await loop._reconcile_cycle()

    snap = detector.snapshot()
    assert snap["reconciliation_failure_total"] == 1
    assert snap["reconciliation_ok_total"] == 0


@pytest.mark.asyncio
async def test_sync_fill_observes_venue_event_when_detector_wired() -> None:
    """_sync_fill_to_trade_service observes the venue event when a detector is wired.

    Given: a TraderCoordinator with trade_service, balance_service,
        and a DivergenceDetector attached,
    When: _sync_fill_to_trade_service is called with a filled execution,
    Then: detector.venue_events_observed_total increments to 1.
    """
    from snapper.application.engine.trader import TraderCoordinator
    from snapper.application.portfolio.models import PortfolioTracker
    from snapper.application.risk.models import RiskConfigModel
    from snapper.application.risk.models import RiskEvaluator
    from snapper.application.trade.balance_service import BalanceService
    from snapper.messaging.schemas.data import ExecutionData

    detector = DivergenceDetector()
    coord = TraderCoordinator.__new__(TraderCoordinator)
    coord.trade_service = TradeService()
    coord.balance_service = BalanceService()
    coord.repository = MagicMock()
    coord._tracker = MagicMock(session_id="s-test")
    coord._tracker.next_sequence = MagicMock(return_value=1)
    coord.divergence_detector = detector

    async def _noop_cycle(
        engine: Any,
        old_qty: float,
        new_qty: float,
        fill: Any,
    ) -> None:
        del engine, old_qty, new_qty, fill

    async def _noop_checkpoint(shard_key: str) -> None:
        del shard_key

    coord._sync_position_cycle_on_fill = _noop_cycle
    coord._persist_checkpoint = _noop_checkpoint

    socket = _SocketStub()
    engine = TradingEngineService(
        instrument="BTC-USD",
        execution_socket=cast(Any, socket),
        risk=RiskEvaluator(RiskConfigModel()),
        cfg=EngineConfigModel(initial_cash=1_000.0),
        exchange="paper",
    )
    engine.portfolio = PortfolioTracker(cash=1_000.0)

    now = datetime.now(UTC)
    fill = ExecutionData(
        type="execution",
        public_id="fill-1",
        timestamp=now,
        session_id="s1",
        sequence_id=1,
        exchange="paper",
        instrument="BTC-USD",
        side="buy",
        size=0.5,
        price=50_000.0,
        last_size=0.5,
        last_price=50_000.0,
        fee=0.5,
        fee_asset="USD",
        status="filled",
        client_order_id="cid-1",
        exchange_order_id="ex-1",
        trade_id="t-1",
        executed_at=now,
    )
    await coord._sync_fill_to_trade_service(fill, engine)

    assert detector.snapshot()["venue_events_observed_total"] == 1


def test_setup_divergence_detector_flag_on_constructs_instance() -> None:
    """TraderCoordinator._setup_divergence_detector builds the detector iff flag is on.

    Given: a TraderCoordinator with settings.enable_divergence_detector=True,
    When: _setup_divergence_detector() is called,
    Then: self.divergence_detector is a DivergenceDetector instance; the
        flag-off path left it as None.
    """
    from snapper.application.engine.trader import TraderCoordinator

    coord_on = TraderCoordinator.__new__(TraderCoordinator)
    coord_on.settings = MagicMock(enable_divergence_detector=True)
    coord_on._setup_divergence_detector()
    assert isinstance(coord_on.divergence_detector, DivergenceDetector)

    coord_off = TraderCoordinator.__new__(TraderCoordinator)
    coord_off.settings = MagicMock(enable_divergence_detector=False)
    coord_off._setup_divergence_detector()
    assert coord_off.divergence_detector is None


@pytest.mark.asyncio
async def test_create_reconciliation_tasks_spawns_snapshot_loop_when_detector_set() -> None:
    """When the detector is wired, _create_reconciliation_tasks appends the snapshot loop.

    Given: a TraderCoordinator in durable-mode with a live detector,
    When: _create_reconciliation_tasks runs,
    Then: returned tasks include at least one task wrapping the
        detector's periodic_snapshot_loop coroutine; all tasks are
        cancelled cleanly afterwards to avoid RuntimeWarnings.
    """
    import asyncio
    import contextlib

    from snapper.application.engine.trader import TraderCoordinator
    from snapper.data.repository import SQLAlchemyRepository

    coord = TraderCoordinator.__new__(TraderCoordinator)
    coord.settings = MagicMock(use_durable_commands=True)
    coord.repository = MagicMock(spec=SQLAlchemyRepository)
    coord.trade_service = TradeService()
    coord.divergence_detector = DivergenceDetector(snapshot_interval_s=0.01)

    tasks = coord._create_reconciliation_tasks()
    assert tasks

    try:
        coro_names: set[str] = set()
        for task in tasks:
            coro = task.get_coro()
            if coro is not None:
                coro_names.add(getattr(coro, "__qualname__", type(coro).__name__))
        assert any("periodic_snapshot_loop" in name for name in coro_names)
    finally:
        for task in tasks:
            task.cancel()
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task


@pytest.mark.asyncio
async def test_create_reconciliation_tasks_spawns_snapshot_loop_in_dual_write_mode() -> None:
    """Pre-flight dual-write mode still spawns the detector snapshot loop.

    Given: a TraderCoordinator with use_durable_commands=False (dual-
        write mode) but a live DivergenceDetector,
    When: _create_reconciliation_tasks runs,
    Then: returned tasks include the detector snapshot loop (so
        operators can verify wiring before the durable flip per the
        docs/operations.md pre-flight flow), with no reconciliation
        tasks (those require durable mode); all tasks are cancelled
        cleanly.
    """
    import asyncio
    import contextlib

    from snapper.application.engine.trader import TraderCoordinator

    coord = TraderCoordinator.__new__(TraderCoordinator)
    coord.settings = MagicMock(use_durable_commands=False)
    coord.repository = MagicMock()
    coord.trade_service = TradeService()
    coord.divergence_detector = DivergenceDetector(snapshot_interval_s=0.01)

    tasks = coord._create_reconciliation_tasks()
    assert len(tasks) == 1, "dual-write mode must spawn the snapshot loop and no recon loops"

    try:
        coro = tasks[0].get_coro()
        name = getattr(coro, "__qualname__", type(coro).__name__) if coro is not None else ""
        assert "periodic_snapshot_loop" in name
    finally:
        for task in tasks:
            task.cancel()
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task


def test_app_settings_enable_divergence_detector_default_false() -> None:
    """enable_divergence_detector defaults to False when unset.

    Given: AppSettings with _get_db_setting stubbed to echo the default,
    When: enable_divergence_detector is read,
    Then: _get_db_setting is queried with key 'enable_divergence_detector'
        and default False; the returned value is False.
    """
    from snapper.config.app import AppSettings

    settings = AppSettings.__new__(AppSettings)
    calls: list[tuple[str, Any]] = []

    def _get(key: str, default: Any) -> Any:
        calls.append((key, default))
        return default

    settings._get_db_setting = _get
    assert settings.enable_divergence_detector is False
    assert calls == [("enable_divergence_detector", False)]
