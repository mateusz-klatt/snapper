"""Tests for OutboxDispatcher — durable trade command delivery."""

import asyncio
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from unittest.mock import AsyncMock

import pytest

from snapper.application.trade.outbox import OutboxDispatcher
from snapper.core.types import TradeCommandStatusEnum
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
        "correlation_id": "corr-1",
        "wallet_public_id": None,
        "operator_public_id": None,
        "user_public_id": None,
        "source_surface": "strategy",
    }


@pytest.mark.asyncio
async def test_dispatch_publishes_and_updates_status() -> None:
    """Dispatcher publishes undispatched command and marks it dispatched.

    Given: a repository returning one undispatched command,
    When: the dispatcher runs one cycle,
    Then: publish_fn is called and bulk_dispatch_trade_commands marks dispatched.
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
    repo.bulk_dispatch_trade_commands = AsyncMock(return_value=1)
    publish_fn = AsyncMock()

    dispatcher = OutboxDispatcher(repository=repo, publish_fn=publish_fn, poll_interval=0.01)

    async def _run_briefly() -> None:
        task = asyncio.create_task(dispatcher.run())
        await asyncio.sleep(0.05)
        dispatcher.stop()
        await task

    await _run_briefly()
    publish_fn.assert_called_once_with(cmd)
    repo.bulk_dispatch_trade_commands.assert_called()
    repo.update_trade_command_status.assert_not_called()
    success_arg = repo.bulk_dispatch_trade_commands.call_args_list[0].args[0]
    assert len(success_arg) == 1
    assert success_arg[0]["public_id"] == cmd["public_id"]
    assert success_arg[0]["attempt_count"] == cmd["attempt_count"] + 1


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
    """Dispatcher with no publish_fn still marks command dispatched.

    Given: a dispatcher with publish_fn=None,
    When: an undispatched command is found,
    Then: bulk_dispatch_trade_commands marks it dispatched, no error.
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
    repo.bulk_dispatch_trade_commands = AsyncMock(return_value=1)

    dispatcher = OutboxDispatcher(repository=repo, publish_fn=None, poll_interval=0.01)

    task = asyncio.create_task(dispatcher.run())
    await asyncio.sleep(0.05)
    dispatcher.stop()
    await task

    repo.bulk_dispatch_trade_commands.assert_called()
    repo.update_trade_command_status.assert_not_called()
    success_arg = repo.bulk_dispatch_trade_commands.call_args_list[0].args[0]
    assert len(success_arg) == 1
    assert success_arg[0]["public_id"] == cmd["public_id"]


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

    Given: a repository where publish succeeds but bulk DB write raises,
    When: the dispatcher runs one cycle,
    Then: the command is NOT reverted to 'created' (prevents replay) —
    update_trade_command_status is NEVER called on the success path.
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
    repo.bulk_dispatch_trade_commands = AsyncMock(side_effect=RuntimeError("DB down"))
    publish_fn = AsyncMock()

    dispatcher = OutboxDispatcher(repository=repo, publish_fn=publish_fn, poll_interval=0.01)

    task = asyncio.create_task(dispatcher.run())
    await asyncio.sleep(0.05)
    dispatcher.stop()
    await task

    publish_fn.assert_called_once()
    repo.bulk_dispatch_trade_commands.assert_called_once()
    repo.update_trade_command_status.assert_not_called()


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


class TestDispatchTtl:
    """Stale CREATED commands expire instead of dispatching (#145 P0-4)."""

    def _stale_cmd(self, age_s: float = 120.0, command_type: str = "submit") -> TradeCommandRow:
        """Build a command row created age_s seconds in the past."""
        cmd = _make_cmd_row()
        cmd["command_type"] = command_type
        cmd["created_at"] = datetime.now(UTC) - timedelta(seconds=age_s)
        return cmd

    def _dispatcher(
        self,
        repo: AsyncMock,
        publish_fn: AsyncMock,
        expire_fn: AsyncMock | None,
        ttl: float | None = 30.0,
    ) -> OutboxDispatcher:
        """Build a dispatcher with the TTL gate armed."""
        return OutboxDispatcher(
            repository=repo,
            publish_fn=publish_fn,
            poll_interval=0.01,
            dispatch_ttl_s=ttl,
            expire_fn=expire_fn,
        )

    def _repo(self, cmd: TradeCommandRow) -> AsyncMock:
        """Build a repo serving the command once, with healthy defaults."""
        repo = AsyncMock()
        served = False

        async def _get_cmds(
            as_of: datetime, limit: int = 10, offset: int = 0
        ) -> list[TradeCommandRow]:
            nonlocal served
            if not served:
                served = True
                return [cmd]
            return []

        repo.get_undispatched_commands = AsyncMock(side_effect=_get_cmds)
        repo.bulk_dispatch_trade_commands = AsyncMock(return_value=1)
        repo.has_order_submit_evidence = AsyncMock(return_value=False)
        repo.cas_trade_command_status = AsyncMock(return_value=True)
        return repo

    @pytest.mark.asyncio
    async def test_stale_submit_expires_and_never_publishes(self) -> None:
        """A stale submit transitions to EXPIRED and skips the bus.

        Given: A submit row older than the TTL with no submit evidence,
        When: One dispatch cycle runs,
        Then: CAS CREATED->EXPIRED fires, the expiry callback receives
            the row, and publish_fn is never called.
        """
        cmd = self._stale_cmd()
        repo = self._repo(cmd)
        publish_fn = AsyncMock()
        expire_fn = AsyncMock()
        dispatcher = self._dispatcher(repo, publish_fn, expire_fn)
        await dispatcher._dispatch_batch()
        publish_fn.assert_not_awaited()
        kwargs = repo.cas_trade_command_status.await_args.kwargs
        assert kwargs["public_id"] == cmd["public_id"]
        assert kwargs["expected_status"] == TradeCommandStatusEnum.CREATED
        assert kwargs["new_status"] == TradeCommandStatusEnum.EXPIRED
        assert "expired by outbox dispatch TTL" in kwargs["last_error"]
        expire_fn.assert_awaited_once_with(cmd)

    @pytest.mark.asyncio
    async def test_stale_with_submit_evidence_publishes_for_dedup(self) -> None:
        """A stale row with durable evidence falls through to publish.

        Given: A stale submit whose client id has venue-event evidence
            (coordinator crashed after publishing, before the commit),
        When: One dispatch cycle runs,
        Then: The row publishes normally — expiring it would fabricate
            a terminal state for a possibly-live order; the executor's
            duplicate guard absorbs the replay.
        """
        cmd = self._stale_cmd()
        repo = self._repo(cmd)
        repo.has_order_submit_evidence = AsyncMock(return_value=True)
        publish_fn = AsyncMock()
        expire_fn = AsyncMock()
        dispatcher = self._dispatcher(repo, publish_fn, expire_fn)
        await dispatcher._dispatch_batch()
        publish_fn.assert_awaited_once_with(cmd)
        repo.cas_trade_command_status.assert_not_awaited()
        expire_fn.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_fresh_submit_publishes_normally(self) -> None:
        """A fresh row inside the TTL dispatches untouched.

        Given: A just-created submit and an armed TTL,
        When: One dispatch cycle runs,
        Then: It publishes; no evidence probe, no CAS.
        """
        cmd = self._stale_cmd(age_s=1.0)
        repo = self._repo(cmd)
        publish_fn = AsyncMock()
        dispatcher = self._dispatcher(repo, publish_fn, AsyncMock())
        await dispatcher._dispatch_batch()
        publish_fn.assert_awaited_once_with(cmd)
        repo.has_order_submit_evidence.assert_not_awaited()
        repo.cas_trade_command_status.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_stale_cancel_is_exempt(self) -> None:
        """A stale cancel still dispatches — expiring it strands a live order.

        Given: A cancel row far older than the TTL,
        When: One dispatch cycle runs,
        Then: It publishes; the TTL gate never touches cancels.
        """
        cmd = self._stale_cmd(command_type="cancel")
        repo = self._repo(cmd)
        publish_fn = AsyncMock()
        dispatcher = self._dispatcher(repo, publish_fn, AsyncMock())
        await dispatcher._dispatch_batch()
        publish_fn.assert_awaited_once_with(cmd)
        repo.cas_trade_command_status.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_lost_expiry_race_skips_callback(self) -> None:
        """Losing the CAS race never double-releases engine intent.

        Given: A stale submit whose CAS reports a concurrent transition,
        When: One dispatch cycle runs,
        Then: The expiry callback is NOT invoked and nothing publishes.
        """
        cmd = self._stale_cmd()
        repo = self._repo(cmd)
        repo.cas_trade_command_status = AsyncMock(return_value=False)
        publish_fn = AsyncMock()
        expire_fn = AsyncMock()
        dispatcher = self._dispatcher(repo, publish_fn, expire_fn)
        await dispatcher._dispatch_batch()
        publish_fn.assert_not_awaited()
        expire_fn.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_ttl_handling_failure_defers_row(self) -> None:
        """A probe/CAS failure defers the stale row, never publishes it.

        Given: The evidence probe raising (DB blip) for a stale row,
        When: One dispatch cycle runs,
        Then: The row neither publishes nor expires — it waits for the
            next tick.
        """
        cmd = self._stale_cmd()
        repo = self._repo(cmd)
        repo.has_order_submit_evidence = AsyncMock(side_effect=RuntimeError("db down"))
        publish_fn = AsyncMock()
        expire_fn = AsyncMock()
        dispatcher = self._dispatcher(repo, publish_fn, expire_fn)
        await dispatcher._dispatch_batch()
        publish_fn.assert_not_awaited()
        expire_fn.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_expire_without_callback_does_not_crash(self) -> None:
        """Expiry with no callback configured is safe.

        Given: A stale submit and expire_fn=None,
        When: One dispatch cycle runs,
        Then: The CAS fires and the cycle completes without error.
        """
        cmd = self._stale_cmd()
        repo = self._repo(cmd)
        publish_fn = AsyncMock()
        dispatcher = self._dispatcher(repo, publish_fn, None)
        await dispatcher._dispatch_batch()
        publish_fn.assert_not_awaited()
        repo.cas_trade_command_status.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_disabled_ttl_is_behavior_identical(self) -> None:
        """TTL=None keeps the legacy dispatch byte-for-byte.

        Given: A very stale submit and no TTL configured,
        When: One dispatch cycle runs,
        Then: It publishes; the evidence probe is never consulted.
        """
        cmd = self._stale_cmd(age_s=10_000.0)
        repo = self._repo(cmd)
        publish_fn = AsyncMock()
        dispatcher = self._dispatcher(repo, publish_fn, None, ttl=None)
        await dispatcher._dispatch_batch()
        publish_fn.assert_awaited_once_with(cmd)
        repo.has_order_submit_evidence.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_expire_callback_failure_does_not_kill_the_cycle(self) -> None:
        """A failing release callback is logged, never propagated.

        Given: A stale submit whose expire_fn raises after the CAS,
        When: One dispatch cycle runs,
        Then: The cycle completes (no exception escapes) — engine
            intent self-heals via its timeout valve and the loud
            CRITICAL log is the operator signal.
        """
        cmd = self._stale_cmd()
        repo = self._repo(cmd)
        publish_fn = AsyncMock()
        expire_fn = AsyncMock(side_effect=RuntimeError("release pipeline down"))
        dispatcher = self._dispatcher(repo, publish_fn, expire_fn)
        await dispatcher._dispatch_batch()
        publish_fn.assert_not_awaited()
        expire_fn.assert_awaited_once_with(cmd)
