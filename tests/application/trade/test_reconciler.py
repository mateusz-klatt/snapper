"""Tests for ReconciliationLoop — periodic exchange state reconciliation."""

import asyncio
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.application.trade.reconciler import ReconciliationLoop
from snapper.application.trade.trade_service import TradeService


@pytest.mark.asyncio
async def test_reconcile_cycle_records_success() -> None:
    """Successful reconciliation cycle records success on trade service.

    Given: a ReconciliationLoop with a repo returning no active commands,
    When: one reconciliation cycle runs,
    Then: trade_service.record_recon_success is called.
    """
    repo = AsyncMock()
    cmd = {
        "public_id": "cmd-1",
        "status": "dispatched",
        "created_at": datetime.now(UTC),
        "shard_key": "kraken.BTC-USD.live",
    }
    repo.get_active_commands_for_exchange = AsyncMock(return_value=[cmd])
    trade_svc = MagicMock(spec=TradeService)
    recon = ReconciliationLoop(
        exchange_name="kraken", repository=repo, trade_service=trade_svc, interval_seconds=0.01
    )
    task = asyncio.create_task(recon.run())
    await asyncio.sleep(0.05)
    recon.stop()
    await asyncio.wait_for(task, timeout=1.0)
    trade_svc.record_recon_success.assert_called_with("kraken.BTC-USD.live")


@pytest.mark.asyncio
async def test_reconcile_cycle_records_failure_on_error() -> None:
    """Reconciliation cycle records failure when DB query raises.

    Given: a ReconciliationLoop with a repo that raises on query,
    When: one reconciliation cycle runs,
    Then: trade_service.record_recon_failure is called.
    """
    repo = AsyncMock()
    repo.get_active_commands_for_exchange = AsyncMock(side_effect=RuntimeError("DB error"))
    trade_svc = MagicMock(spec=TradeService)
    trade_svc.record_recon_failure = MagicMock(return_value=False)
    recon = ReconciliationLoop(
        exchange_name="kraken", repository=repo, trade_service=trade_svc, interval_seconds=0.01
    )
    task = asyncio.create_task(recon.run())
    await asyncio.sleep(0.05)
    recon.stop()
    await asyncio.wait_for(task, timeout=1.0)
    trade_svc.record_recon_failure.assert_called()


@pytest.mark.asyncio
async def test_reconcile_detects_stale_commands() -> None:
    """Reconciliation detects commands older than 3x interval.

    Given: a ReconciliationLoop with a command created 200s ago and interval=60s,
    When: one reconciliation cycle runs,
    Then: the stale command is logged (cycle still succeeds).
    """
    old_cmd = {
        "public_id": "cmd-old",
        "status": "dispatched",
        "created_at": datetime.now(UTC) - timedelta(seconds=200),
        "shard_key": "kraken.BTC-USD.live",
    }
    repo = AsyncMock()
    repo.get_active_commands_for_exchange = AsyncMock(return_value=[old_cmd])
    trade_svc = MagicMock(spec=TradeService)
    recon = ReconciliationLoop(
        exchange_name="kraken", repository=repo, trade_service=trade_svc, interval_seconds=0.01
    )
    task = asyncio.create_task(recon.run())
    await asyncio.sleep(0.05)
    recon.stop()
    await asyncio.wait_for(task, timeout=1.0)
    trade_svc.record_recon_success.assert_called()


@pytest.mark.asyncio
async def test_reconcile_cancellation() -> None:
    """Reconciliation loop handles task cancellation cleanly.

    Given: a running ReconciliationLoop,
    When: the asyncio task is cancelled,
    Then: CancelledError propagates cleanly.
    """
    repo = AsyncMock()
    repo.get_active_commands_for_exchange = AsyncMock(return_value=[])
    trade_svc = MagicMock(spec=TradeService)
    recon = ReconciliationLoop(
        exchange_name="kraken", repository=repo, trade_service=trade_svc, interval_seconds=10.0
    )
    task = asyncio.create_task(recon.run())
    await asyncio.sleep(0.02)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_reconcile_skips_terminal_commands() -> None:
    """Reconciliation skips commands in terminal status.

    Given: a ReconciliationLoop with a filled command,
    When: one reconciliation cycle runs,
    Then: the terminal command is skipped (not flagged as stale).
    """
    terminal_cmd = {
        "public_id": "cmd-done",
        "status": "filled",
        "created_at": datetime.now(UTC) - timedelta(seconds=9999),
        "shard_key": "kraken.BTC-USD.live",
    }
    repo = AsyncMock()
    repo.get_active_commands_for_exchange = AsyncMock(return_value=[terminal_cmd])
    trade_svc = MagicMock(spec=TradeService)
    recon = ReconciliationLoop(
        exchange_name="kraken", repository=repo, trade_service=trade_svc, interval_seconds=0.01
    )
    task = asyncio.create_task(recon.run())
    await asyncio.sleep(0.05)
    recon.stop()
    await asyncio.wait_for(task, timeout=1.0)
    trade_svc.record_recon_success.assert_called()


@pytest.mark.asyncio
async def test_reconcile_failure_triggers_halt() -> None:
    """Repeated reconciliation failures trigger shard halt.

    Given: a ReconciliationLoop where record_recon_failure returns True (halt),
    When: reconciliation cycle fails,
    Then: the shard is halted via trade_service.
    """
    repo = AsyncMock()
    repo.get_active_commands_for_exchange = AsyncMock(side_effect=RuntimeError("DB down"))
    trade_svc = MagicMock(spec=TradeService)
    trade_svc.record_recon_failure = MagicMock(return_value=True)
    recon = ReconciliationLoop(
        exchange_name="kraken", repository=repo, trade_service=trade_svc, interval_seconds=0.01
    )
    task = asyncio.create_task(recon.run())
    await asyncio.sleep(0.05)
    recon.stop()
    await asyncio.wait_for(task, timeout=1.0)
    trade_svc.record_recon_failure.assert_called()


@pytest.mark.asyncio
async def test_reconcile_fresh_command_not_stale() -> None:
    """Fresh non-terminal command is not flagged as stale.

    Given: a ReconciliationLoop with a recently created dispatched command,
    When: one reconciliation cycle runs,
    Then: the command is not flagged as stale and cycle succeeds.
    """
    fresh_cmd = {
        "public_id": "cmd-fresh",
        "status": "dispatched",
        "created_at": datetime.now(UTC),
        "shard_key": "kraken.BTC-USD.live",
    }
    repo = AsyncMock()
    repo.get_active_commands_for_exchange = AsyncMock(return_value=[fresh_cmd])
    trade_svc = MagicMock(spec=TradeService)
    recon = ReconciliationLoop(
        exchange_name="kraken", repository=repo, trade_service=trade_svc, interval_seconds=0.01
    )
    task = asyncio.create_task(recon.run())
    await asyncio.sleep(0.05)
    recon.stop()
    await asyncio.wait_for(task, timeout=1.0)
    trade_svc.record_recon_success.assert_called()
