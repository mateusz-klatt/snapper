"""Tests for OutboxDispatcher — durable trade command delivery."""

import asyncio
from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock

import pytest

from snapper.application.trade.outbox import OutboxDispatcher
from snapper.data.repository_types import TradeCommandRow


def _make_cmd_row(public_id: str = "cmd-1") -> TradeCommandRow:
    """Build a minimal TradeCommandRow for testing.

    Args:
        public_id: Command public ID.

    Returns:
        TradeCommandRow dict with required fields.
    """
    now = datetime.now(UTC)
    return {
        "public_id": public_id,
        "timestamp": now,
        "session_id": "s1",
        "sequence_id": 1,
        "command_type": "submit",
        "shard_key": "kraken.BTC-USD.live",
        "exchange": "kraken",
        "instrument": "BTC-USD",
        "mode": "live",
        "strategy_id": "engine-buy",
        "client_order_id": "cid-1",
        "venue_client_id": "vcid-1",
        "idempotency_key": None,
        "side": "buy",
        "order_type": "market",
        "quantity": 0.5,
        "price": None,
        "status": "created",
        "attempt_count": 0,
        "last_error": None,
        "created_at": now,
        "dispatched_at": None,
        "acked_at": None,
        "terminal_at": None,
        "exchange_order_id": None,
        "supersedes_command_id": None,
        "correlation_id": "corr-1",
    }


@pytest.mark.asyncio
async def test_dispatch_publishes_and_updates_status() -> None:
    """Dispatcher publishes undispatched command and marks it dispatched.

    Given: a repository returning one undispatched command,
    When: the dispatcher runs one cycle,
    Then: publish_fn is called and update_trade_command_status sets 'dispatched'.
    """
    cmd = _make_cmd_row()
    repo = AsyncMock()
    returned_once = False

    async def _get_cmds(as_of: datetime, limit: int = 10) -> list[TradeCommandRow]:
        nonlocal returned_once
        if not returned_once:
            returned_once = True
            return [cmd]
        return []

    repo.get_undispatched_commands = AsyncMock(side_effect=_get_cmds)
    repo.update_trade_command_status = AsyncMock(return_value=1)
    publish_fn = AsyncMock()

    dispatcher = OutboxDispatcher(repository=repo, publish_fn=publish_fn, poll_interval=0.01)

    async def _run_briefly() -> None:
        task = asyncio.create_task(dispatcher.run())
        await asyncio.sleep(0.05)
        dispatcher.stop()
        await task

    await _run_briefly()
    publish_fn.assert_called_once_with(cmd)
    repo.update_trade_command_status.assert_called()
    call_kwargs = repo.update_trade_command_status.call_args_list[0].kwargs
    assert call_kwargs["new_status"] == "dispatched"


@pytest.mark.asyncio
async def test_dispatch_handles_publish_failure() -> None:
    """Dispatcher handles publish failure by keeping command as created.

    Given: a repository returning one command and publish_fn that raises,
    When: the dispatcher runs one cycle,
    Then: update_trade_command_status is called with status='created' and incremented attempt.
    """
    cmd = _make_cmd_row()
    repo = AsyncMock()
    returned_once = False

    async def _get_cmds(as_of: datetime, limit: int = 10) -> list[TradeCommandRow]:
        nonlocal returned_once
        if not returned_once:
            returned_once = True
            return [cmd]
        return []

    repo.get_undispatched_commands = AsyncMock(side_effect=_get_cmds)
    repo.update_trade_command_status = AsyncMock(return_value=1)
    publish_fn = AsyncMock(side_effect=RuntimeError("ZMQ down"))

    dispatcher = OutboxDispatcher(repository=repo, publish_fn=publish_fn, poll_interval=0.01)

    async def _run_briefly() -> None:
        task = asyncio.create_task(dispatcher.run())
        await asyncio.sleep(0.05)
        dispatcher.stop()
        await task

    await _run_briefly()
    call_kwargs = repo.update_trade_command_status.call_args_list[0].kwargs
    assert call_kwargs["new_status"] == "created"
    assert call_kwargs["attempt_count"] == 1
    assert call_kwargs["last_error"] == "dispatch failed"


@pytest.mark.asyncio
async def test_notify_wakes_dispatcher() -> None:
    """Notify signal wakes the dispatcher immediately.

    Given: a dispatcher with a long poll interval,
    When: notify() is called after a command is written,
    Then: the command is dispatched within milliseconds, not at poll interval.
    """
    cmd = _make_cmd_row()
    call_count = 0

    async def _get_cmds(as_of: datetime, limit: int = 10) -> list[TradeCommandRow]:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return [cmd]
        return []

    repo = AsyncMock()
    repo.get_undispatched_commands = AsyncMock(side_effect=_get_cmds)
    repo.update_trade_command_status = AsyncMock(return_value=1)
    publish_fn = AsyncMock()

    dispatcher = OutboxDispatcher(repository=repo, publish_fn=publish_fn, poll_interval=10.0)

    async def _run_with_notify() -> None:
        task = asyncio.create_task(dispatcher.run())
        await asyncio.sleep(0.01)
        dispatcher.notify()
        await asyncio.sleep(0.05)
        dispatcher.stop()
        await task

    await _run_with_notify()
    publish_fn.assert_called_once()


@pytest.mark.asyncio
async def test_stop_gracefully() -> None:
    """Dispatcher stops cleanly when stop() is called.

    Given: a running dispatcher,
    When: stop() is called,
    Then: the run() coroutine completes without error.
    """
    repo = AsyncMock()
    repo.get_undispatched_commands = AsyncMock(return_value=[])

    dispatcher = OutboxDispatcher(repository=repo, publish_fn=None, poll_interval=0.01)

    task = asyncio.create_task(dispatcher.run())
    await asyncio.sleep(0.03)
    dispatcher.stop()
    await asyncio.wait_for(task, timeout=1.0)


@pytest.mark.asyncio
async def test_no_publish_fn_skips_publish() -> None:
    """Dispatcher with no publish_fn still updates command status.

    Given: a dispatcher with publish_fn=None,
    When: an undispatched command is found,
    Then: update_trade_command_status is called (status='dispatched'), no error.
    """
    cmd = _make_cmd_row()
    repo = AsyncMock()
    returned_once = False

    async def _get_cmds(as_of: datetime, limit: int = 10) -> list[TradeCommandRow]:
        nonlocal returned_once
        if not returned_once:
            returned_once = True
            return [cmd]
        return []

    repo.get_undispatched_commands = AsyncMock(side_effect=_get_cmds)
    repo.update_trade_command_status = AsyncMock(return_value=1)

    dispatcher = OutboxDispatcher(repository=repo, publish_fn=None, poll_interval=0.01)

    task = asyncio.create_task(dispatcher.run())
    await asyncio.sleep(0.05)
    dispatcher.stop()
    await task

    repo.update_trade_command_status.assert_called()
    call_kwargs = repo.update_trade_command_status.call_args_list[0].kwargs
    assert call_kwargs["new_status"] == "dispatched"


@pytest.mark.asyncio
async def test_cancelled_error_propagates() -> None:
    """Dispatcher propagates CancelledError from task cancellation.

    Given: a running dispatcher with a long poll interval,
    When: the asyncio task is cancelled externally,
    Then: CancelledError is raised and the dispatcher stops cleanly.
    """
    repo = AsyncMock()
    repo.get_undispatched_commands = AsyncMock(return_value=[])

    dispatcher = OutboxDispatcher(repository=repo, publish_fn=None, poll_interval=10.0)

    task = asyncio.create_task(dispatcher.run())
    await asyncio.sleep(0.02)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_dispatch_post_publish_db_failure_no_revert() -> None:
    """Dispatcher does not revert to created after successful publish + DB failure.

    Given: a repository where publish succeeds but status update raises,
    When: the dispatcher runs one cycle,
    Then: the command is NOT reverted to 'created' (prevents replay).
    """
    cmd = _make_cmd_row()
    repo = AsyncMock()
    returned_once = False

    async def _get_cmds(as_of: datetime, limit: int = 10) -> list[TradeCommandRow]:
        nonlocal returned_once
        if not returned_once:
            returned_once = True
            return [cmd]
        return []

    repo.get_undispatched_commands = AsyncMock(side_effect=_get_cmds)
    repo.update_trade_command_status = AsyncMock(side_effect=RuntimeError("DB down"))
    publish_fn = AsyncMock()

    dispatcher = OutboxDispatcher(repository=repo, publish_fn=publish_fn, poll_interval=0.01)

    task = asyncio.create_task(dispatcher.run())
    await asyncio.sleep(0.05)
    dispatcher.stop()
    await task

    publish_fn.assert_called_once()
    assert repo.update_trade_command_status.call_count == 1
    call_kwargs = repo.update_trade_command_status.call_args_list[0].kwargs
    assert call_kwargs["new_status"] == "dispatched"


@pytest.mark.asyncio
async def test_dispatch_revert_db_failure_logged() -> None:
    """Dispatcher handles revert DB failure without crashing.

    Given: a repository where publish fails AND status revert also fails,
    When: the dispatcher runs one cycle,
    Then: no exception propagates (both failures logged).
    """
    cmd = _make_cmd_row()
    repo = AsyncMock()
    returned_once = False

    async def _get_cmds(as_of: datetime, limit: int = 10) -> list[TradeCommandRow]:
        nonlocal returned_once
        if not returned_once:
            returned_once = True
            return [cmd]
        return []

    repo.get_undispatched_commands = AsyncMock(side_effect=_get_cmds)
    repo.update_trade_command_status = AsyncMock(side_effect=RuntimeError("DB down"))
    publish_fn = AsyncMock(side_effect=RuntimeError("ZMQ down"))

    dispatcher = OutboxDispatcher(repository=repo, publish_fn=publish_fn, poll_interval=0.01)

    task = asyncio.create_task(dispatcher.run())
    await asyncio.sleep(0.05)
    dispatcher.stop()
    await task
