"""Tests for OutboxDispatcher — durable trade command delivery."""

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from itertools import count
from pathlib import Path
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import call

import pytest
from loguru import logger

import snapper.application.trade.outbox as outbox_module
from snapper.application.trade.cancel_target import CancelTargetUnresolvedError
from snapper.application.trade.outbox import OutboxDispatcher
from snapper.core.command_material import TradeCommandMaterialChangedError
from snapper.core.types import TradeCommandStatusEnum
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import TradeCommandInsertRow
from snapper.data.repository_types import TradeCommandRow

_ROW_SEQUENCE = count(1)


def _make_cmd_row(public_id: str = "cmd-1") -> TradeCommandRow:
    """Build a minimal TradeCommandRow for testing.

    Args:
        public_id: Command public ID.

    Returns:
        TradeCommandRow dict with required fields.
    """
    seq = next(_ROW_SEQUENCE)
    now = datetime.now(UTC) + timedelta(microseconds=seq)
    return {
        "id": seq,
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
        "stop_price": None,
        "leverage": None,
        "reduce_only": False,
        "post_only": False,
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


async def _wait_until(predicate: Callable[[], bool], timeout: float = 5.0) -> None:
    """Poll predicate until it returns True or the deadline expires.

    Args:
        predicate: Zero-argument callable returning True once the awaited effect occurs.
        timeout: Maximum wait duration in seconds before raising TimeoutError.
    """
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.005)


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

    async def _get_cmds(
        as_of: datetime,
        limit: int = 10,
        offset: int = 0,
        *,
        after: tuple[datetime, str, int] | None = None,
    ) -> list[TradeCommandRow]:
        nonlocal returned_once
        if not returned_once:
            returned_once = True
            return [cmd]
        return []

    repo.get_undispatched_commands = AsyncMock(side_effect=_get_cmds)
    repo.cas_trade_command_status = AsyncMock(return_value=True)
    repo.update_trade_command_status = AsyncMock(return_value=1)
    repo.bulk_dispatch_trade_commands = AsyncMock(return_value=1)
    publish_fn = AsyncMock()

    dispatcher = OutboxDispatcher(repository=repo, publish_fn=publish_fn, poll_interval=0.01)

    async def _run_briefly() -> None:
        task = asyncio.create_task(dispatcher.run())
        await _wait_until(lambda: repo.bulk_dispatch_trade_commands.await_count >= 1)
        dispatcher.stop()
        await task

    await _run_briefly()
    publish_fn.assert_called_once_with(cmd)
    repo.bulk_dispatch_trade_commands.assert_called()
    repo.cas_trade_command_status.assert_not_called()
    repo.update_trade_command_status.assert_not_called()
    success_arg = repo.bulk_dispatch_trade_commands.call_args_list[0].args[0]
    assert len(success_arg) == 1
    assert success_arg[0]["public_id"] == cmd["public_id"]
    assert success_arg[0]["attempt_count"] == cmd["attempt_count"] + 1


@pytest.mark.asyncio
async def test_dispatch_handles_publish_failure() -> None:
    """Dispatcher handles publish failure by CAS-updating created attempt metadata.

    Given: a repository returning one command and publish_fn that raises,
    When: the dispatcher runs one cycle,
    Then: cas_trade_command_status is called from created to created with incremented attempt.
    """
    cmd = _make_cmd_row()
    repo = AsyncMock()
    returned_once = False

    async def _get_cmds(
        as_of: datetime,
        limit: int = 10,
        offset: int = 0,
        *,
        after: tuple[datetime, str, int] | None = None,
    ) -> list[TradeCommandRow]:
        nonlocal returned_once
        if not returned_once:
            returned_once = True
            return [cmd]
        return []

    repo.get_undispatched_commands = AsyncMock(side_effect=_get_cmds)
    repo.cas_trade_command_status = AsyncMock(return_value=True)
    repo.update_trade_command_status = AsyncMock(return_value=1)
    publish_fn = AsyncMock(side_effect=RuntimeError("ZMQ down"))

    dispatcher = OutboxDispatcher(repository=repo, publish_fn=publish_fn, poll_interval=0.01)

    async def _run_briefly() -> None:
        task = asyncio.create_task(dispatcher.run())
        await _wait_until(lambda: repo.cas_trade_command_status.await_count >= 1)
        dispatcher.stop()
        await task

    await _run_briefly()
    call_kwargs = repo.cas_trade_command_status.call_args_list[0].kwargs
    assert call_kwargs["expected_status"] == TradeCommandStatusEnum.CREATED
    assert call_kwargs["new_status"] == TradeCommandStatusEnum.CREATED
    assert call_kwargs["attempt_count"] == 1
    assert call_kwargs["last_error"] == "dispatch failed"
    repo.update_trade_command_status.assert_not_called()


@pytest.mark.asyncio
async def test_dispatch_holds_unresolved_cancel_without_status_mutation() -> None:
    """Leave an unresolved cancel CREATED without dispatch or attempt churn.

    Given: a cancel whose publisher raises `CancelTargetUnresolvedError`,
    When: one outbox batch runs,
    Then: neither dispatch nor failure-revert persistence is called.
    """
    repo = AsyncMock()
    repo.get_undispatched_commands = AsyncMock(return_value=[_make_cmd_row()])
    publish_fn = AsyncMock(side_effect=CancelTargetUnresolvedError("unresolved"))
    dispatcher = OutboxDispatcher(repository=repo, publish_fn=publish_fn)

    await dispatcher._dispatch_batch()

    repo.bulk_dispatch_trade_commands.assert_not_awaited()
    repo.cas_trade_command_status.assert_not_awaited()
    repo.update_trade_command_status.assert_not_awaited()


def _make_insert_row(
    client_order_id: str,
    command_type: str,
    status: str,
    offset_seconds: int,
    exchange_order_id: str | None = None,
) -> TradeCommandInsertRow:
    """Build a minimal TradeCommandInsertRow for SQLite outbox tests."""
    created_at = datetime(2026, 4, 10, 12, 0, tzinfo=UTC) + timedelta(seconds=offset_seconds)
    return {
        "wallet_public_id": "01975a8b-3c7d-7000-8000-aaaaaaaaaaaa",
        "command_type": command_type,
        "shard_key": "kraken.BTC-USD.live",
        "exchange": "kraken",
        "instrument": "BTC-USD",
        "mode": "live",
        "strategy_id": "engine-buy",
        "client_order_id": client_order_id,
        "venue_client_id": f"v{client_order_id}",
        "side": "buy",
        "order_type": "market",
        "quantity": 0.5,
        "price": None,
        "status": status,
        "exchange_order_id": exchange_order_id,
        "created_at": created_at,
        "correlation_id": f"corr-{client_order_id}",
        "session_id": "s1",
        "sequence_id": offset_seconds + 1,
        "timestamp": created_at,
    }


@pytest.mark.asyncio
async def test_dispatch_skips_awaiting_target_cancels_without_paging(
    tmp_path: Path,
) -> None:
    """Held awaiting_target cancels never enter single-page outbox fetches.

    Given: Eleven held awaiting_target cancels inserted before one created submit
        and one created id-bearing cancel in SQLite,
    When: One outbox tick runs with batch size ten,
    Then: get_undispatched_commands runs once without offset paging, both later
        commands dispatch in the same tick, and all held rows remain awaiting_target.
    """
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'outbox_held.db'}")
    await repo.create_all()
    try:
        held_cids: list[str] = []
        for index in range(11):
            cid = f"cid-held-{index}"
            held_cids.append(cid)
            await repo.insert_trade_command(
                _make_insert_row(
                    cid,
                    "cancel",
                    TradeCommandStatusEnum.AWAITING_TARGET.value,
                    index,
                )
            )
        await repo.insert_trade_command(
            _make_insert_row(
                "cid-create-later",
                "create",
                TradeCommandStatusEnum.CREATED.value,
                15,
            )
        )
        await repo.insert_trade_command(
            _make_insert_row(
                "cid-cancel-identified",
                "cancel",
                TradeCommandStatusEnum.CREATED.value,
                16,
                "ex-identified",
            )
        )

        orig_get = repo.get_undispatched_commands
        get_spy = AsyncMock(side_effect=orig_get)
        repo.get_undispatched_commands = get_spy
        published: list[str] = []

        async def _publish(cmd: TradeCommandRow) -> None:
            if cmd["command_type"] == "cancel" and not cmd["exchange_order_id"]:
                raise CancelTargetUnresolvedError("unresolved")
            published.append(cmd["client_order_id"])

        dispatcher = OutboxDispatcher(repository=repo, publish_fn=_publish, batch_size=10)
        await dispatcher._dispatch_batch()

        assert get_spy.await_count == 1
        assert get_spy.await_args is not None
        assert "offset" not in get_spy.await_args.kwargs
        assert "after" not in get_spy.await_args.kwargs
        assert get_spy.await_args.kwargs["limit"] == 10
        assert published == ["cid-create-later", "cid-cancel-identified"]

        for cid in held_cids:
            row = await repo.get_pending_cancel_command_for_client_order_id(cid, datetime.now(UTC))
            assert row is not None
            assert row["status"] == TradeCommandStatusEnum.AWAITING_TARGET.value
        assert await orig_get(as_of=datetime.now(UTC), limit=20) == []
    finally:
        await repo.engine.dispose()


@pytest.mark.asyncio
async def test_held_awaiting_target_cancel_hydrates_to_created_then_dispatches(
    tmp_path: Path,
) -> None:
    """Hydrating a held cancel from awaiting_target to created makes it dispatchable.

    Given: An unresolved cancel persisted in awaiting_target status in SQLite,
    When: One outbox tick runs before hydration, then the row CAS-hydrates to
        created with an exchange_order_id and another tick runs,
    Then: The first tick never fetches or publishes it and the second dispatches it.
    """
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'outbox_hydrate.db'}")
    await repo.create_all()
    try:
        _, cmd_id = await repo.insert_trade_command(
            _make_insert_row(
                "cid-hyd-1",
                "cancel",
                TradeCommandStatusEnum.AWAITING_TARGET.value,
                0,
            )
        )
        published: list[str] = []

        async def _publish(cmd: TradeCommandRow) -> None:
            published.append(cmd["client_order_id"])

        dispatcher = OutboxDispatcher(repository=repo, publish_fn=_publish, batch_size=10)
        await dispatcher._dispatch_batch()
        assert published == []
        assert await repo.get_undispatched_commands(as_of=datetime.now(UTC), limit=10) == []

        hydrated = await repo.advance_trade_command_lifecycle(
            public_id=cmd_id,
            expected_status=TradeCommandStatusEnum.AWAITING_TARGET.value,
            new_status=TradeCommandStatusEnum.CREATED.value,
            bus_time=datetime.now(UTC),
            session_id="s1",
            sequence_id=2,
            exchange_order_id="ex-hydrated-1",
        )
        assert hydrated is True

        await dispatcher._dispatch_batch()
        assert published == ["cid-hyd-1"]
        updated = await repo.get_pending_cancel_command_for_client_order_id(
            "cid-hyd-1", datetime.now(UTC)
        )
        assert updated is not None
        assert updated["status"] == TradeCommandStatusEnum.DISPATCHED.value
        assert updated["exchange_order_id"] == "ex-hydrated-1"
    finally:
        await repo.engine.dispose()


@pytest.mark.asyncio
async def test_dispatch_skips_material_barrier_without_publishing() -> None:
    """Hold a materially conflicted create while leaving the row unchanged.

    Given: A create whose material consistency check raises,
    When: One dispatch batch runs,
    Then: No publish or dispatch update occurs.
    """
    repo = AsyncMock()
    repo.get_undispatched_commands = AsyncMock(return_value=[_make_cmd_row()])
    repo.require_consistent_trade_command_material = AsyncMock(
        side_effect=TradeCommandMaterialChangedError("cmd-1", ("price",))
    )
    publish_fn = AsyncMock()
    dispatcher = OutboxDispatcher(repository=repo, publish_fn=publish_fn)

    await dispatcher._dispatch_batch()

    publish_fn.assert_not_awaited()
    repo.bulk_dispatch_trade_commands.assert_not_awaited()


@pytest.mark.asyncio
async def test_paginated_dispatch_skips_foreign_rows_and_stops_at_batch_size() -> None:
    """Skip foreign rows and stop as soon as the owned dispatch batch is full.

    Given: One foreign row followed by two owned rows and batch size one,
    When: Paginated dispatch runs,
    Then: Only the first owned command publishes and the loop breaks immediately.
    """
    foreign = _make_cmd_row("foreign")
    foreign["shard_key"] = "kraken.FOREIGN.live"
    owned = _make_cmd_row("owned-1")
    owned["shard_key"] = "kraken.MINE.live"
    extra = _make_cmd_row("owned-2")
    extra["shard_key"] = "kraken.MINE.live"
    rows = [foreign, owned, extra]
    repo = AsyncMock()

    async def _get_commands(
        as_of: datetime,
        limit: int = 10,
        offset: int = 0,
        *,
        after: tuple[datetime, str, int] | None = None,
    ) -> list[TradeCommandRow]:
        if after is not None:
            remaining = [
                row for row in rows if (row["created_at"], row["public_id"], row["id"]) > after
            ]
            return remaining[:limit]
        return rows[offset : offset + limit]

    repo.get_undispatched_commands = AsyncMock(side_effect=_get_commands)
    repo.bulk_dispatch_trade_commands = AsyncMock(return_value=1)
    publish_fn = AsyncMock()
    ownership = MagicMock()
    ownership.owns.side_effect = lambda shard_key: shard_key == "kraken.MINE.live"
    dispatcher = OutboxDispatcher(
        repository=repo,
        publish_fn=publish_fn,
        batch_size=1,
        ownership=ownership,
    )

    await dispatcher._dispatch_batch()

    publish_fn.assert_awaited_once_with(owned)
    updates = repo.bulk_dispatch_trade_commands.await_args.args[0]
    assert [update["public_id"] for update in updates] == ["owned-1"]


@pytest.mark.asyncio
async def test_notify_wakes_dispatcher() -> None:
    """Notify signal wakes the dispatcher before the poll interval.

    Given: a dispatcher with a long poll interval,
    When: notify() is called after a command is written,
    Then: the command is dispatched well before the 10 s poll interval expires.
    """
    cmd = _make_cmd_row()
    call_count = 0

    async def _get_cmds(as_of: datetime, limit: int = 10, offset: int = 0) -> list[TradeCommandRow]:
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
        await _wait_until(lambda: publish_fn.await_count >= 1)
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
    await _wait_until(lambda: repo.get_undispatched_commands.await_count >= 1)
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

    async def _get_cmds(
        as_of: datetime,
        limit: int = 10,
        offset: int = 0,
        *,
        after: tuple[datetime, str, int] | None = None,
    ) -> list[TradeCommandRow]:
        nonlocal returned_once
        if not returned_once:
            returned_once = True
            return [cmd]
        return []

    repo.get_undispatched_commands = AsyncMock(side_effect=_get_cmds)
    repo.cas_trade_command_status = AsyncMock(return_value=True)
    repo.update_trade_command_status = AsyncMock(return_value=1)
    repo.bulk_dispatch_trade_commands = AsyncMock(return_value=1)

    dispatcher = OutboxDispatcher(repository=repo, publish_fn=None, poll_interval=0.01)

    task = asyncio.create_task(dispatcher.run())
    await _wait_until(lambda: repo.bulk_dispatch_trade_commands.await_count >= 1)
    dispatcher.stop()
    await task

    repo.bulk_dispatch_trade_commands.assert_called()
    repo.cas_trade_command_status.assert_not_called()
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
    status CAS/update is NEVER called on the success path.
    """
    cmd = _make_cmd_row()
    repo = AsyncMock()
    returned_once = False

    async def _get_cmds(
        as_of: datetime,
        limit: int = 10,
        offset: int = 0,
        *,
        after: tuple[datetime, str, int] | None = None,
    ) -> list[TradeCommandRow]:
        nonlocal returned_once
        if not returned_once:
            returned_once = True
            return [cmd]
        return []

    repo.get_undispatched_commands = AsyncMock(side_effect=_get_cmds)
    repo.cas_trade_command_status = AsyncMock(return_value=True)
    repo.update_trade_command_status = AsyncMock(return_value=1)
    repo.bulk_dispatch_trade_commands = AsyncMock(side_effect=RuntimeError("DB down"))
    publish_fn = AsyncMock()

    dispatcher = OutboxDispatcher(repository=repo, publish_fn=publish_fn, poll_interval=0.01)

    task = asyncio.create_task(dispatcher.run())
    await _wait_until(lambda: repo.bulk_dispatch_trade_commands.await_count >= 1)
    dispatcher.stop()
    await task

    publish_fn.assert_called_once()
    repo.bulk_dispatch_trade_commands.assert_called_once()
    repo.cas_trade_command_status.assert_not_called()
    repo.update_trade_command_status.assert_not_called()


@pytest.mark.asyncio
async def test_dispatch_revert_db_failure_logged() -> None:
    """Dispatcher handles failure-CAS DB exception without crashing.

    Given: a repository where publish fails AND cas_trade_command_status raises,
    When: the dispatcher runs one cycle,
    Then: no exception propagates (both failures logged).
    """
    cmd = _make_cmd_row()
    repo = AsyncMock()
    returned_once = False

    async def _get_cmds(
        as_of: datetime,
        limit: int = 10,
        offset: int = 0,
        *,
        after: tuple[datetime, str, int] | None = None,
    ) -> list[TradeCommandRow]:
        nonlocal returned_once
        if not returned_once:
            returned_once = True
            return [cmd]
        return []

    repo.get_undispatched_commands = AsyncMock(side_effect=_get_cmds)
    repo.cas_trade_command_status = AsyncMock(side_effect=RuntimeError("DB down"))
    publish_fn = AsyncMock(side_effect=RuntimeError("ZMQ down"))

    dispatcher = OutboxDispatcher(repository=repo, publish_fn=publish_fn, poll_interval=0.01)

    task = asyncio.create_task(dispatcher.run())
    await _wait_until(lambda: repo.cas_trade_command_status.await_count >= 1)
    dispatcher.stop()
    await task


class TestDispatchTtl:
    """Stale CREATED commands expire instead of dispatching."""

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
            as_of: datetime,
            limit: int = 10,
            offset: int = 0,
            *,
            after: tuple[datetime, str, int] | None = None,
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


@dataclass
class _CapturedLog:
    """Minimal log record summary captured from a Loguru sink."""

    level: str
    message: str
    has_exception: bool


class _ManualClock:
    """Deterministic monotonic clock helper for backoff tests."""

    def __init__(self, initial: float = 100.0) -> None:
        """Initialize the manual clock.

        Args:
            initial: Starting monotonic timestamp in seconds.
        """
        self.now = initial

    def __call__(self) -> float:
        """Return the current monotonic timestamp."""
        return self.now

    def advance(self, delta_s: float) -> float:
        """Advance the monotonic clock by ``delta_s`` seconds.

        Args:
            delta_s: Positive offset in seconds to add.

        Returns:
            Updated monotonic timestamp.
        """
        self.now += delta_s
        return self.now


def _install_clock(monkeypatch: pytest.MonkeyPatch, initial: float = 100.0) -> _ManualClock:
    """Install a controllable monotonic clock in ``outbox`` and ``time``.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        initial: Starting monotonic timestamp in seconds.

    Returns:
        Controlled :class:`_ManualClock` instance.
    """
    clock = _ManualClock(initial)
    monkeypatch.setattr(time, "monotonic", clock)
    return clock


def _make_backoff_row(
    public_id: str,
    *,
    command_type: str = "submit",
    shard_key: str = "kraken.BTC-USD.live",
    created_at: datetime | None = None,
) -> TradeCommandRow:
    """Build a command row customized by type, shard, and creation timestamp.

    Args:
        public_id: Command public ID.
        command_type: Command type string.
        shard_key: Partition shard key.
        created_at: Optional explicit UTC creation timestamp.

    Returns:
        Configured :class:`TradeCommandRow`.
    """
    row = _make_cmd_row(public_id)
    row["command_type"] = command_type
    row["shard_key"] = shard_key
    row["client_order_id"] = f"cid-{public_id}"
    if created_at is not None:
        row["created_at"] = created_at
        row["timestamp"] = created_at
    return row


def _make_queue_repo(rows_ref: list[list[TradeCommandRow]]) -> MagicMock:
    """Build a warning-free mock repository serving keyset/offset slices from ``rows_ref[0]``.

    Args:
        rows_ref: Single-element list wrapping the current undispatched queue.

    Returns:
        Mock repository with AsyncMock methods and keyset/offset pagination support.
    """
    repo = MagicMock()

    async def _get_cmds(
        as_of: datetime,
        limit: int = 10,
        offset: int = 0,
        *,
        after: tuple[datetime, str, int] | None = None,
    ) -> list[TradeCommandRow]:
        if after is not None:
            remaining = [
                row
                for row in rows_ref[0]
                if (row["created_at"], row["public_id"], row["id"]) > after
            ]
            return remaining[:limit]
        return rows_ref[0][offset : offset + limit]

    repo.get_undispatched_commands = AsyncMock(side_effect=_get_cmds)
    repo.require_consistent_trade_command_material = AsyncMock(return_value=None)
    repo.has_order_submit_evidence = AsyncMock(return_value=False)
    repo.cas_trade_command_status = AsyncMock(return_value=True)
    repo.update_trade_command_status = AsyncMock(return_value=1)
    repo.bulk_dispatch_trade_commands = AsyncMock(return_value=1)
    return repo


def _attach_log_capture() -> tuple[int, list[_CapturedLog]]:
    """Attach a Loguru sink collecting level, message, and exception presence.

    Returns:
        Tuple of Loguru handler ID and captured log list.
    """
    records: list[_CapturedLog] = []

    def _sink(message: object) -> None:
        record = getattr(message, "record", None)
        if record is None:
            return
        records.append(
            _CapturedLog(
                level=str(record["level"].name),
                message=str(record["message"]),
                has_exception=record["exception"] is not None,
            )
        )

    handler_id = logger.add(_sink, level="DEBUG")
    return handler_id, records


class TestOutboxRetryBackoff:
    """Bounded per-command retry backoff, per-shard FIFO, logging, and pruning."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("command_type", ["create", "submit"])
    async def test_ttl_command_backoff_schedule_cap_and_single_scd2_write(
        self, monkeypatch: pytest.MonkeyPatch, command_type: str
    ) -> None:
        """TTL-bound commands double backoff from 0.25 s up to the 5 s cap.

        Given: A create or submit command whose publish callback always fails,
        When: The dispatcher ticks before and at each backoff deadline,
        Then: Retries follow 0.25, 0.5, 1, 2, 4, 5, 5 s with one SCD2 write per attempt.
        """
        clock = _install_clock(monkeypatch, initial=100.0)
        cmd = _make_backoff_row("cmd-ttl-cap", command_type=command_type)
        queue: list[list[TradeCommandRow]] = [[cmd]]
        repo = _make_queue_repo(queue)
        publish_fn = AsyncMock(side_effect=RuntimeError("ZMQ down"))
        dispatcher = OutboxDispatcher(repository=repo, publish_fn=publish_fn, dispatch_ttl_s=None)

        expected_delays = [0.25, 0.5, 1.0, 2.0, 4.0, 5.0, 5.0]
        for attempt, delay in enumerate(expected_delays, start=1):
            await dispatcher._dispatch_batch()
            assert publish_fn.await_count == attempt
            assert repo.cas_trade_command_status.await_count == attempt
            assert dispatcher._retry["cmd-ttl-cap"].failures == attempt
            assert dispatcher._retry["cmd-ttl-cap"].next_at == pytest.approx(clock.now + delay)
            clock.advance(delay - 0.01)
            await dispatcher._dispatch_batch()
            assert publish_fn.await_count == attempt
            assert repo.cas_trade_command_status.await_count == attempt
            clock.advance(0.01)

    @pytest.mark.asyncio
    async def test_non_ttl_command_backoff_schedule_hits_60s_cap(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Non-TTL commands back off exponentially up to the 60 s cap.

        Given: A cancel command whose publish callback fails repeatedly,
        When: Each due retry executes at the monotonic deadline,
        Then: Delays double from 0.25 s to 60 s and stay capped at 60 s.
        """
        clock = _install_clock(monkeypatch, initial=50.0)
        cmd = _make_backoff_row("cmd-cancel-cap", command_type="cancel")
        repo = _make_queue_repo([[cmd]])
        publish_fn = AsyncMock(side_effect=RuntimeError("ZMQ down"))
        dispatcher = OutboxDispatcher(repository=repo, publish_fn=publish_fn)

        expected_delays = [0.25, 0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 60.0, 60.0]
        for attempt, delay in enumerate(expected_delays, start=1):
            await dispatcher._dispatch_batch()
            assert repo.cas_trade_command_status.await_count == attempt
            assert dispatcher._retry["cmd-cancel-cap"].next_at == pytest.approx(clock.now + delay)
            clock.advance(delay)

    @pytest.mark.asyncio
    async def test_publish_failure_log_cadence_and_300s_error_relog(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Publish failures log ERROR first, WARNING between, and ERROR every 300 s.

        Given: A cancel command failing continuously across more than five minutes,
        When: Deferred ticks and due retries run against a controlled clock,
        Then: Attempt 1 logs ERROR with traceback, deferred ticks log nothing,
            subsequent due attempts log one-line WARNINGs with id/attempt/delay,
            and crossing 300 s re-emits ERROR with traceback.
        """
        clock = _install_clock(monkeypatch, initial=1000.0)
        cmd = _make_backoff_row("cmd-cadence", command_type="cancel")
        repo = _make_queue_repo([[cmd]])
        publish_fn = AsyncMock(side_effect=RuntimeError("ZMQ down"))
        dispatcher = OutboxDispatcher(repository=repo, publish_fn=publish_fn)
        handler_id, logs = _attach_log_capture()
        try:
            await dispatcher._dispatch_batch()
            assert [(entry.level, entry.has_exception) for entry in logs] == [("ERROR", True)]
            logs.clear()

            clock.advance(0.10)
            await dispatcher._dispatch_batch()
            assert logs == []

            clock.advance(0.15)
            delays = [0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 60.0, 60.0, 60.0, 60.0]
            for index, delay in enumerate(delays, start=2):
                await dispatcher._dispatch_batch()
                assert len(logs) == 1
                assert logs[0].level == "WARNING"
                assert "\n" not in logs[0].message
                assert "cmd-cadence" in logs[0].message
                assert f"attempt={index}" in logs[0].message
                assert f"{delay:.2f}s" in logs[0].message
                logs.clear()
                clock.advance(delay)

            assert clock.now - 1000.0 >= 300.0
            await dispatcher._dispatch_batch()
            assert [(entry.level, entry.has_exception) for entry in logs] == [("ERROR", True)]
            logs.clear()

            clock.advance(60.0)
            await dispatcher._dispatch_batch()
            assert len(logs) == 1
            assert logs[0].level == "WARNING"
            assert "attempt=14" in logs[0].message
        finally:
            logger.remove(handler_id)

    @pytest.mark.asyncio
    async def test_material_barrier_logs_error_once_and_backs_off(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Material barrier holds log ERROR once, back off, and re-log at 300 s.

        Given: A create command held by TradeCommandMaterialChangedError,
        When: Ticks fire before and at due times across 300 s,
        Then: Deferred ticks skip the material check silently, attempt 2 logs WARNING,
            and the check after 300 s re-logs ERROR.
        """
        clock = _install_clock(monkeypatch, initial=0.0)
        cmd = _make_backoff_row("cmd-barrier", command_type="create")
        repo = _make_queue_repo([[cmd]])
        repo.require_consistent_trade_command_material = AsyncMock(
            side_effect=TradeCommandMaterialChangedError("cmd-barrier", ("price",))
        )
        publish_fn = AsyncMock()
        dispatcher = OutboxDispatcher(repository=repo, publish_fn=publish_fn)
        handler_id, logs = _attach_log_capture()
        try:
            await dispatcher._dispatch_batch()
            assert repo.require_consistent_trade_command_material.await_count == 1
            assert [(entry.level, entry.has_exception) for entry in logs] == [("ERROR", False)]
            logs.clear()

            clock.advance(0.10)
            await dispatcher._dispatch_batch()
            assert repo.require_consistent_trade_command_material.await_count == 1
            assert logs == []

            clock.advance(0.15)
            await dispatcher._dispatch_batch()
            assert repo.require_consistent_trade_command_material.await_count == 2
            assert len(logs) == 1
            assert logs[0].level == "WARNING"
            assert "cmd-barrier" in logs[0].message
            assert "attempt=2" in logs[0].message
            assert "0.50s" in logs[0].message
            logs.clear()

            clock.now = 300.0
            await dispatcher._dispatch_batch()
            assert repo.require_consistent_trade_command_material.await_count == 3
            assert [(entry.level, entry.has_exception) for entry in logs] == [("ERROR", False)]
            publish_fn.assert_not_awaited()
        finally:
            logger.remove(handler_id)

    @pytest.mark.asyncio
    async def test_ttl_probe_error_backs_off_with_error_warning_cadence(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """TTL probe exceptions schedule backoff and respect the log cadence.

        Given: A stale submit whose evidence probe raises a repository error,
        When: Ticks run immediately, during backoff, and at due times,
        Then: The probe is skipped while deferred, attempt 1 logs ERROR with traceback,
            attempt 2 logs WARNING, and publishing never occurs.
        """
        clock = _install_clock(monkeypatch, initial=10.0)
        cmd = _make_backoff_row(
            "cmd-probe-err",
            command_type="submit",
            created_at=datetime.now(UTC) - timedelta(seconds=120),
        )
        repo = _make_queue_repo([[cmd]])
        repo.has_order_submit_evidence = AsyncMock(side_effect=RuntimeError("db down"))
        publish_fn = AsyncMock()
        dispatcher = OutboxDispatcher(repository=repo, publish_fn=publish_fn, dispatch_ttl_s=30.0)
        handler_id, logs = _attach_log_capture()
        try:
            await dispatcher._dispatch_batch()
            assert repo.has_order_submit_evidence.await_count == 1
            assert [(entry.level, entry.has_exception) for entry in logs] == [("ERROR", True)]
            logs.clear()

            clock.advance(0.10)
            await dispatcher._dispatch_batch()
            assert repo.has_order_submit_evidence.await_count == 1
            assert logs == []

            clock.advance(0.15)
            await dispatcher._dispatch_batch()
            assert repo.has_order_submit_evidence.await_count == 2
            assert len(logs) == 1
            assert logs[0].level == "WARNING"
            assert "cmd-probe-err" in logs[0].message
            assert "attempt=2" in logs[0].message
            publish_fn.assert_not_awaited()
        finally:
            logger.remove(handler_id)

    @pytest.mark.asyncio
    async def test_deferred_rows_consume_no_slots_and_other_shards_keep_flowing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Deferred head rows skip without consuming batch slots for other shards.

        Given: A batch size of two and three BTC commands headed by a deferred row
            followed by healthy ETH and SOL commands,
        When: The dispatcher runs while the BTC head is backing off,
        Then: All BTC rows are deferred without taking batch slots and both healthy
            other-shard commands publish in one batch.
        """
        clock = _install_clock(monkeypatch, initial=0.0)
        btc_head = _make_backoff_row("btc-1", shard_key="kraken.BTC-USD.live")
        btc_two = _make_backoff_row("btc-2", shard_key="kraken.BTC-USD.live")
        btc_three = _make_backoff_row("btc-3", shard_key="kraken.BTC-USD.live")
        eth = _make_backoff_row("eth-1", shard_key="kraken.ETH-USD.live")
        sol = _make_backoff_row("sol-1", shard_key="kraken.SOL-USD.live")
        queue: list[list[TradeCommandRow]] = [[btc_head]]
        repo = _make_queue_repo(queue)
        published: list[str] = []

        async def _publish(cmd: TradeCommandRow) -> None:
            if cmd["public_id"] == "btc-1":
                raise RuntimeError("ZMQ down")
            published.append(cmd["public_id"])

        dispatcher = OutboxDispatcher(
            repository=repo, publish_fn=AsyncMock(side_effect=_publish), batch_size=2
        )
        await dispatcher._dispatch_batch()
        assert published == []

        clock.advance(0.05)
        queue[0] = [btc_head, btc_two, btc_three, eth, sol]
        await dispatcher._dispatch_batch()
        assert published == ["eth-1", "sol-1"]

    @pytest.mark.asyncio
    async def test_per_shard_fifo_defers_later_submits_but_lets_cancels_flow_across_pages(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A deferred row defers later expirable peers across pages while cancels flow.

        Given: A failing BTC submit followed by twenty same-shard submits, a
            same-shard cancel on page two, and an ETH submit,
        When: A batch runs during the BTC backoff window and then again once due,
        Then: Same-shard submits stay deferred across keyset pages while the
            same-shard cancel and ETH submit dispatch, and once due the fast
            path resumes without keyset or offset paging.
        """
        clock = _install_clock(monkeypatch, initial=0.0)
        btc_head = _make_backoff_row(
            "btc-0", command_type="submit", shard_key="kraken.BTC-USD.live"
        )
        queue: list[list[TradeCommandRow]] = [[btc_head]]
        repo = _make_queue_repo(queue)
        published: list[str] = []
        fail_btc_head = True

        async def _publish(cmd: TradeCommandRow) -> None:
            if fail_btc_head and cmd["public_id"] == "btc-0":
                raise RuntimeError("ZMQ down")
            published.append(cmd["public_id"])

        dispatcher = OutboxDispatcher(
            repository=repo, publish_fn=AsyncMock(side_effect=_publish), batch_size=2
        )
        await dispatcher._dispatch_batch()

        clock.advance(0.05)
        btc_followers = [
            _make_backoff_row(f"btc-{i}", command_type="submit", shard_key="kraken.BTC-USD.live")
            for i in range(1, 21)
        ]
        btc_cancel = _make_backoff_row(
            "btc-cancel", command_type="cancel", shard_key="kraken.BTC-USD.live"
        )
        eth_row = _make_backoff_row("eth-1", command_type="submit", shard_key="kraken.ETH-USD.live")
        queue[0] = [btc_head, *btc_followers, btc_cancel, eth_row]
        repo.get_undispatched_commands.reset_mock()
        await dispatcher._dispatch_batch()
        assert published == ["btc-cancel", "eth-1"]
        assert repo.get_undispatched_commands.await_count >= 2
        assert all(
            "offset" not in call.kwargs for call in repo.get_undispatched_commands.call_args_list
        )
        assert repo.get_undispatched_commands.call_args_list[1].kwargs["after"] is not None

        clock.advance(0.20)
        fail_btc_head = False
        queue[0] = [btc_head]
        repo.get_undispatched_commands.reset_mock()
        await dispatcher._dispatch_batch()
        assert published == ["btc-cancel", "eth-1", "btc-0"]
        assert repo.get_undispatched_commands.await_args is not None
        assert "offset" not in repo.get_undispatched_commands.await_args.kwargs
        assert "after" not in repo.get_undispatched_commands.await_args.kwargs

    @pytest.mark.asyncio
    async def test_material_barrier_hold_does_not_block_later_rows_on_its_shard(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A barrier-held create backs off alone while later same-shard rows dispatch.

        Given: A create held by the durable material barrier followed by a
            same-shard submit and cancel,
        When: Batches run during the held create's backoff window,
        Then: The held create stays unpublished and unchecked while deferred, and
            both later same-shard rows dispatch without waiting behind it.
        """
        clock = _install_clock(monkeypatch, initial=0.0)
        held = _make_backoff_row("btc-held", command_type="create")
        later_submit = _make_backoff_row("btc-submit", command_type="submit")
        later_cancel = _make_backoff_row("btc-cancel", command_type="cancel")
        queue: list[list[TradeCommandRow]] = [[held]]
        repo = _make_queue_repo(queue)

        async def _barrier(public_id: str) -> None:
            if public_id == "btc-held":
                raise TradeCommandMaterialChangedError("btc-held", ("price",))

        repo.require_consistent_trade_command_material = AsyncMock(side_effect=_barrier)
        published: list[str] = []

        async def _publish(cmd: TradeCommandRow) -> None:
            published.append(cmd["public_id"])

        dispatcher = OutboxDispatcher(
            repository=repo, publish_fn=AsyncMock(side_effect=_publish), batch_size=2
        )
        await dispatcher._dispatch_batch()
        assert published == []

        clock.advance(0.10)
        queue[0] = [held, later_submit, later_cancel]
        await dispatcher._dispatch_batch()
        assert published == ["btc-submit", "btc-cancel"]
        assert repo.require_consistent_trade_command_material.await_count == 2

    @pytest.mark.asyncio
    async def test_failure_in_batch_skips_later_same_shard_rows_in_that_tick(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A failing command stops later same-shard rows within the same batch.

        Given: One batch holding a failing BTC submit, a later BTC submit and an
            ETH submit,
        When: The batch runs, then runs again after the failing submit's backoff
            once it succeeds,
        Then: The first tick publishes only ETH, and the later BTC submit is
            published only after the earlier one, preserving shard order.
        """
        clock = _install_clock(monkeypatch, initial=0.0)
        first = _make_backoff_row("btc-a", command_type="submit")
        second = _make_backoff_row("btc-b", command_type="submit")
        other = _make_backoff_row("eth-a", command_type="submit", shard_key="kraken.ETH-USD.live")
        queue: list[list[TradeCommandRow]] = [[first, second, other]]
        repo = _make_queue_repo(queue)
        published: list[str] = []
        fail_first = True

        async def _publish(cmd: TradeCommandRow) -> None:
            if fail_first and cmd["public_id"] == "btc-a":
                raise RuntimeError("ZMQ down")
            published.append(cmd["public_id"])

        dispatcher = OutboxDispatcher(repository=repo, publish_fn=AsyncMock(side_effect=_publish))
        await dispatcher._dispatch_batch()
        assert published == ["eth-a"]

        clock.advance(0.10)
        queue[0] = [first, second]
        await dispatcher._dispatch_batch()
        assert published == ["eth-a"]

        clock.advance(0.20)
        fail_first = False
        await dispatcher._dispatch_batch()
        assert published == ["eth-a", "btc-a", "btc-b"]

    @pytest.mark.asyncio
    async def test_repeat_failure_after_backoff_holds_submits_not_cancels(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Repeat failure keeps later submits waiting in-tick while cancels dispatch.

        Given: A BTC submit already backing off, a later BTC submit, and a later
            BTC cancel,
        When: All three appear together once due and the head fails again,
        Then: The later submit is skipped in that tick while the cancel dispatches.
        """
        clock = _install_clock(monkeypatch, initial=0.0)
        first = _make_backoff_row("btc-a", command_type="submit")
        later_submit = _make_backoff_row("btc-b", command_type="submit")
        later_cancel = _make_backoff_row("btc-cancel", command_type="cancel")
        queue: list[list[TradeCommandRow]] = [[first]]
        repo = _make_queue_repo(queue)
        published: list[str] = []

        async def _publish(cmd: TradeCommandRow) -> None:
            if cmd["public_id"] == "btc-a":
                raise RuntimeError("ZMQ down")
            published.append(cmd["public_id"])

        dispatcher = OutboxDispatcher(repository=repo, publish_fn=AsyncMock(side_effect=_publish))
        await dispatcher._dispatch_batch()

        clock.advance(0.30)
        queue[0] = [first, later_submit, later_cancel]
        await dispatcher._dispatch_batch()
        assert published == ["btc-cancel"]

    @pytest.mark.asyncio
    async def test_ttl_expiry_during_backoff_expires_without_publishing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A create whose TTL passes while backing off expires and never publishes.

        Given: A create command failing just before its dispatch TTL elapses,
        When: Wall-clock time passes the TTL before the retry becomes due,
        Then: The due retry transitions the command to EXPIRED, calls expire_fn,
            removes its retry state, and never calls publish_fn.
        """
        clock = _install_clock(monkeypatch, initial=0.0)
        base_time = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)
        wall_now = [base_time + timedelta(seconds=29.8)]

        class _FakeDateTime:
            """Controllable UTC datetime replacement for TTL progression."""

            @staticmethod
            def now(tz: object = None) -> datetime:
                """Return the controlled test timestamp."""
                return wall_now[0]

        monkeypatch.setattr(outbox_module, "datetime", _FakeDateTime)

        cmd = _make_backoff_row("cmd-ttl-expire", command_type="create", created_at=base_time)
        repo = _make_queue_repo([[cmd]])
        publish_fn = AsyncMock(side_effect=[RuntimeError("ZMQ down"), None])
        expire_fn = AsyncMock()
        dispatcher = OutboxDispatcher(
            repository=repo,
            publish_fn=publish_fn,
            dispatch_ttl_s=30.0,
            expire_fn=expire_fn,
        )

        await dispatcher._dispatch_batch()
        assert publish_fn.await_count == 1
        assert "cmd-ttl-expire" in dispatcher._retry

        clock.advance(0.25)
        wall_now[0] = base_time + timedelta(seconds=30.5)
        await dispatcher._dispatch_batch()
        assert publish_fn.await_count == 1
        assert repo.cas_trade_command_status.await_count == 2
        assert repo.cas_trade_command_status.await_args is not None
        assert (
            repo.cas_trade_command_status.await_args.kwargs["new_status"]
            == TradeCommandStatusEnum.EXPIRED
        )
        expire_fn.assert_awaited_once_with(cmd)
        assert "cmd-ttl-expire" not in dispatcher._retry

    @pytest.mark.asyncio
    async def test_retry_entry_removed_on_success_expiry_cas_loss_and_bulk_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Retry entries clear on success, expiry, CAS loss, and keep bulk-write semantics.

        Given: Commands whose retries end in success, bulk-write failure, expiry,
            or CAS loss, plus an unresolved cancel,
        When: Each due retry runs,
        Then: Retry entries are deleted in all three completion cases, bulk-write
            failure does not re-defer, and unresolved cancels stay untracked.
        """
        clock = _install_clock(monkeypatch, initial=0.0)

        cmd_ok = _make_backoff_row("cmd-ok", command_type="cancel")
        repo_ok = _make_queue_repo([[cmd_ok]])
        pub_ok = AsyncMock(side_effect=[RuntimeError("down"), None])
        disp_ok = OutboxDispatcher(repository=repo_ok, publish_fn=pub_ok)
        await disp_ok._dispatch_batch()
        assert "cmd-ok" in disp_ok._retry
        clock.advance(0.25)
        await disp_ok._dispatch_batch()
        assert "cmd-ok" not in disp_ok._retry

        cmd_bulk = _make_backoff_row("cmd-bulk", command_type="cancel")
        repo_bulk = _make_queue_repo([[cmd_bulk]])
        pub_bulk = AsyncMock(side_effect=[RuntimeError("down"), None, None])
        repo_bulk.bulk_dispatch_trade_commands = AsyncMock(side_effect=[RuntimeError("DB down"), 1])
        disp_bulk = OutboxDispatcher(repository=repo_bulk, publish_fn=pub_bulk)
        await disp_bulk._dispatch_batch()
        clock.advance(0.25)
        await disp_bulk._dispatch_batch()
        assert "cmd-bulk" not in disp_bulk._retry
        clock.advance(0.05)
        await disp_bulk._dispatch_batch()
        assert pub_bulk.await_count == 3

        stale_created = datetime.now(UTC) - timedelta(seconds=120)
        cmd_cas = _make_backoff_row("cmd-cas", command_type="submit", created_at=stale_created)
        repo_cas = _make_queue_repo([[cmd_cas]])
        repo_cas.has_order_submit_evidence = AsyncMock(side_effect=[RuntimeError("blip"), False])
        repo_cas.cas_trade_command_status = AsyncMock(return_value=False)
        expire_cas = AsyncMock()
        disp_cas = OutboxDispatcher(
            repository=repo_cas,
            publish_fn=AsyncMock(),
            dispatch_ttl_s=30.0,
            expire_fn=expire_cas,
        )
        await disp_cas._dispatch_batch()
        assert "cmd-cas" in disp_cas._retry
        clock.advance(0.25)
        await disp_cas._dispatch_batch()
        assert "cmd-cas" not in disp_cas._retry
        expire_cas.assert_not_awaited()

        cmd_hold = _make_backoff_row("cmd-hold", command_type="cancel")
        repo_hold = _make_queue_repo([[cmd_hold]])
        pub_hold = AsyncMock(side_effect=CancelTargetUnresolvedError("unresolved"))
        disp_hold = OutboxDispatcher(repository=repo_hold, publish_fn=pub_hold)
        await disp_hold._dispatch_batch()
        clock.advance(0.05)
        await disp_hold._dispatch_batch()
        assert disp_hold._retry == {}
        assert pub_hold.await_count == 2

    @pytest.mark.asyncio
    async def test_retry_last_seen_refresh_and_stale_entry_pruning(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Appearing in a fetched page refreshes last_seen and unseen entries prune at 300 s.

        Given: Two failing commands on different shards where one vanishes
            immediately and the other stays visible in a fetched page at 200 s before later vanishing,
        When: Batches run at 200 s, 300.1 s, and 600.2 s,
        Then: The absent command is pruned after 300 s while the refreshed one
            survives until it is absent for more than 300 s.
        """
        clock = _install_clock(monkeypatch, initial=0.0)
        cmd_keep = _make_backoff_row("cmd-keep", command_type="cancel")
        cmd_drop = _make_backoff_row(
            "cmd-drop", command_type="cancel", shard_key="kraken.ETH-USD.live"
        )
        queue: list[list[TradeCommandRow]] = [[cmd_keep, cmd_drop]]
        repo = _make_queue_repo(queue)
        publish_fn = AsyncMock(
            side_effect=[
                RuntimeError("down"),
                RuntimeError("down"),
                CancelTargetUnresolvedError("hold"),
                CancelTargetUnresolvedError("hold"),
            ]
        )
        dispatcher = OutboxDispatcher(repository=repo, publish_fn=publish_fn)
        await dispatcher._dispatch_batch()
        assert set(dispatcher._retry) == {"cmd-keep", "cmd-drop"}

        clock.now = 200.0
        queue[0] = [cmd_keep]
        await dispatcher._dispatch_batch()
        assert set(dispatcher._retry) == {"cmd-keep", "cmd-drop"}
        assert dispatcher._retry["cmd-keep"].last_seen == 200.0

        clock.now = 300.1
        await dispatcher._dispatch_batch()
        assert set(dispatcher._retry) == {"cmd-keep"}

        clock.now = 600.2
        queue[0] = []
        await dispatcher._dispatch_batch()
        assert dispatcher._retry == {}

    @pytest.mark.asyncio
    async def test_ownership_and_deferral_paging_respects_max_scan_rows_warning(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ownership and deferral combine in paging and warn at max_scan_rows.

        Given: Foreign rows, a deferred owned shard, and another healthy owned shard,
        When: One batch paginates with ownership and deferral, and another hits
            max_scan_rows due to foreign-owner rows,
        Then: The first batch skips foreign and deferred rows to dispatch the other
            owned shard, and the second emits the max_scan_rows warning.
        """
        clock = _install_clock(monkeypatch, initial=0.0)
        ownership = MagicMock()
        ownership.owns.side_effect = lambda shard_key: shard_key.startswith("kraken.MINE")

        mine_head = _make_backoff_row("mine-head", shard_key="kraken.MINE1.live")
        queue: list[list[TradeCommandRow]] = [[mine_head]]
        repo = _make_queue_repo(queue)
        published: list[str] = []

        async def _publish(cmd: TradeCommandRow) -> None:
            if cmd["public_id"] == "mine-head":
                raise RuntimeError("ZMQ down")
            published.append(cmd["public_id"])

        dispatcher = OutboxDispatcher(
            repository=repo,
            publish_fn=AsyncMock(side_effect=_publish),
            batch_size=2,
            ownership=ownership,
            max_scan_rows=40,
        )
        await dispatcher._dispatch_batch()

        clock.advance(0.05)
        foreign_rows = [
            _make_backoff_row(f"foreign-{i}", shard_key="kraken.FOREIGN.live") for i in range(18)
        ]
        mine_follow = _make_backoff_row(
            "mine-follow", command_type="submit", shard_key="kraken.MINE1.live"
        )
        mine_two_a = _make_backoff_row("mine-2a", shard_key="kraken.MINE2.live")
        mine_two_b = _make_backoff_row("mine-2b", shard_key="kraken.MINE2.live")
        queue[0] = [*foreign_rows, mine_head, mine_follow, mine_two_a, mine_two_b]
        await dispatcher._dispatch_batch()
        assert published == ["mine-2a", "mine-2b"]

        overflow_foreign = [
            _make_backoff_row(f"foreign-overflow-{i}", shard_key="kraken.FOREIGN.live")
            for i in range(45)
        ]
        queue[0] = [mine_head, *overflow_foreign, mine_two_a]
        handler_id, logs = _attach_log_capture()
        try:
            await dispatcher._dispatch_batch()
        finally:
            logger.remove(handler_id)
        assert published == ["mine-2a", "mine-2b"]
        assert any(
            entry.level == "WARNING" and "max_scan_rows=40" in entry.message for entry in logs
        )

    @pytest.mark.asyncio
    async def test_failing_create_does_not_block_cancels_while_failing_cancel_defers_itself(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Cancels flow past failing creates and defer only via their own retry state.

        Given: A failing create, a later same-shard create, an independent live
            cancel, an unpublished-target cancel, and a failing cancel on the same shard,
        When: Batches run in the failing tick and during the retry backoff window,
        Then: The independent cancel dispatches both in-tick and on next fetch, the
            unresolved cancel stays held without retry churn, the failing cancel
            defers only via its own retry entry, and the later create stays blocked.
        """
        clock = _install_clock(monkeypatch, initial=0.0)
        head_create = _make_backoff_row("btc-create-1", command_type="create")
        later_create = _make_backoff_row("btc-create-2", command_type="create")
        live_cancel = _make_backoff_row("btc-cancel-live", command_type="cancel")
        held_cancel = _make_backoff_row("btc-cancel-held", command_type="cancel")
        failing_cancel = _make_backoff_row("btc-cancel-fail", command_type="cancel")
        queue: list[list[TradeCommandRow]] = [
            [head_create, later_create, live_cancel, held_cancel, failing_cancel]
        ]
        repo = _make_queue_repo(queue)
        published: list[str] = []

        async def _publish(cmd: TradeCommandRow) -> None:
            pid = cmd["public_id"]
            if pid in ("btc-create-1", "btc-cancel-fail"):
                raise RuntimeError("ZMQ down")
            if pid == "btc-cancel-held":
                raise CancelTargetUnresolvedError("target unpublished")
            published.append(pid)

        dispatcher = OutboxDispatcher(
            repository=repo, publish_fn=AsyncMock(side_effect=_publish), batch_size=5
        )
        await dispatcher._dispatch_batch()
        assert published == ["btc-cancel-live"]
        assert "btc-cancel-held" not in dispatcher._retry
        assert "btc-cancel-fail" in dispatcher._retry

        clock.advance(0.05)
        second_live_cancel = _make_backoff_row("btc-cancel-live-2", command_type="cancel")
        queue[0] = [head_create, later_create, held_cancel, failing_cancel, second_live_cancel]
        await dispatcher._dispatch_batch()
        assert published == ["btc-cancel-live", "btc-cancel-live-2"]
        assert repo.cas_trade_command_status.await_count == 2

        clock.advance(0.20)
        queue[0] = [failing_cancel, later_create]
        await dispatcher._dispatch_batch()
        assert published == ["btc-cancel-live", "btc-cancel-live-2"]

    @pytest.mark.asyncio
    async def test_paging_dedupes_duplicate_public_id_keeping_first_occurrence(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Keyset paging deduplicates repeated rows by public_id, keeping the first.

        Given: A mocked repository whose second keyset page repeats a row from the
            first page, with distinct ``last_error`` markers on the two copies,
        When: The paging path runs a batch of three commands,
        Then: The repeated command is published exactly once using its first copy.
        """
        _install_clock(monkeypatch, initial=0.0)
        row_first = _make_backoff_row("dup-1", shard_key="kraken.MINE.live")
        row_first["last_error"] = "first-copy"
        row_second = _make_backoff_row("dup-1", shard_key="kraken.MINE.live")
        row_second["last_error"] = "second-copy"
        next_row = _make_backoff_row("own-2", shard_key="kraken.MINE.live")
        foreign = [
            _make_backoff_row(f"foreign-{i}", shard_key="kraken.FOREIGN.live") for i in range(19)
        ]
        pages = [[*foreign, row_first], [row_second, next_row], []]
        page_index = 0
        repo = _make_queue_repo([[]])

        async def _paged(
            as_of: datetime,
            limit: int = 10,
            offset: int = 0,
            *,
            after: tuple[datetime, str, int] | None = None,
        ) -> list[TradeCommandRow]:
            nonlocal page_index
            result = pages[page_index]
            page_index += 1
            return result

        repo.get_undispatched_commands = AsyncMock(side_effect=_paged)
        published_rows: list[TradeCommandRow] = []

        async def _publish(cmd: TradeCommandRow) -> None:
            published_rows.append(cmd)

        ownership = MagicMock()
        ownership.owns.side_effect = lambda shard_key: shard_key == "kraken.MINE.live"
        dispatcher = OutboxDispatcher(
            repository=repo,
            publish_fn=AsyncMock(side_effect=_publish),
            batch_size=3,
            ownership=ownership,
        )
        await dispatcher._dispatch_batch()
        assert [row["public_id"] for row in published_rows] == ["dup-1", "own-2"]
        assert published_rows[0]["last_error"] == "first-copy"
        assert all(
            "offset" not in call.kwargs for call in repo.get_undispatched_commands.call_args_list
        )

    @pytest.mark.asyncio
    async def test_publish_failure_cas_lost_drops_retry_and_logs_info(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Losing the CREATED->CREATED failure CAS drops retry state and logs INFO.

        Given: A command that fails publish after a concurrent actor already moved
            its status so cas_trade_command_status returns False,
        When: The dispatch batch handles the publish failure,
        Then: The retry entry is cleared and an INFO log notes the concurrent move.
        """
        _install_clock(monkeypatch, initial=0.0)
        cmd = _make_backoff_row("cmd-cas-lost", command_type="submit")
        repo = _make_queue_repo([[cmd]])
        repo.cas_trade_command_status = AsyncMock(return_value=False)
        publish_fn = AsyncMock(side_effect=RuntimeError("ZMQ down"))
        dispatcher = OutboxDispatcher(repository=repo, publish_fn=publish_fn)
        handler_id, logs = _attach_log_capture()
        try:
            await dispatcher._dispatch_batch()
        finally:
            logger.remove(handler_id)
        assert "cmd-cas-lost" not in dispatcher._retry
        assert any(
            entry.level == "INFO"
            and "cmd-cas-lost" in entry.message
            and "concurrently" in entry.message
            for entry in logs
        )

    @pytest.mark.asyncio
    async def test_publish_failure_cas_error_logs_exception_and_keeps_retry(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A raising failure CAS logs the exception and retains per-command backoff.

        Given: A command whose publish fails and whose failure CAS raises a DB error,
        When: The dispatch batch handles the failure,
        Then: The CAS error is logged with traceback and the retry entry stays active.
        """
        _install_clock(monkeypatch, initial=0.0)
        cmd = _make_backoff_row("cmd-cas-err", command_type="submit")
        repo = _make_queue_repo([[cmd]])
        repo.cas_trade_command_status = AsyncMock(side_effect=RuntimeError("DB blip"))
        publish_fn = AsyncMock(side_effect=RuntimeError("ZMQ down"))
        dispatcher = OutboxDispatcher(repository=repo, publish_fn=publish_fn)
        handler_id, logs = _attach_log_capture()
        try:
            await dispatcher._dispatch_batch()
        finally:
            logger.remove(handler_id)
        assert "cmd-cas-err" in dispatcher._retry
        assert any(
            entry.level == "ERROR"
            and entry.has_exception
            and "cmd-cas-err" in entry.message
            and "status" in entry.message
            for entry in logs
        )

    @pytest.mark.asyncio
    async def test_keyset_paging_does_not_skip_head_when_earlier_row_vanishes(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Concurrent row removal between pages cannot skip a backing-off shard head.

        Given: A twenty-row page followed by a backing-off shard head and a fresh
            same-shard follower, where the first row leaves CREATED between page
            reads,
        When: Keyset pagination runs a dispatch batch,
        Then: The second page resumes strictly after the last row of page one so
            the backing-off head is seen and the follower stays unpublished.
        """
        clock = _install_clock(monkeypatch, initial=0.0)
        foreign = [
            _make_backoff_row(f"foreign-{i}", shard_key="kraken.FOREIGN.live") for i in range(20)
        ]
        head = _make_backoff_row("btc-head", command_type="submit", shard_key="kraken.MINE.live")
        follower = _make_backoff_row(
            "btc-follow", command_type="submit", shard_key="kraken.MINE.live"
        )
        live_rows: list[TradeCommandRow] = [head]
        repo = _make_queue_repo([live_rows])
        published: list[str] = []

        async def _publish(cmd: TradeCommandRow) -> None:
            if cmd["public_id"] == "btc-head":
                raise RuntimeError("ZMQ down")
            published.append(cmd["public_id"])

        ownership = MagicMock()
        ownership.owns.side_effect = lambda shard_key: shard_key == "kraken.MINE.live"
        dispatcher = OutboxDispatcher(
            repository=repo,
            publish_fn=AsyncMock(side_effect=_publish),
            batch_size=2,
            ownership=ownership,
        )
        await dispatcher._dispatch_batch()

        clock.advance(0.05)
        live_rows[:] = [*foreign, head, follower]

        async def _keyset_with_concurrent_exit(
            as_of: datetime,
            limit: int = 10,
            offset: int = 0,
            *,
            after: tuple[datetime, str, int] | None = None,
        ) -> list[TradeCommandRow]:
            if after is None:
                page = live_rows[:limit]
                live_rows.pop(0)
                return page
            remaining = [
                row for row in live_rows if (row["created_at"], row["public_id"], row["id"]) > after
            ]
            return remaining[:limit]

        repo.get_undispatched_commands = AsyncMock(side_effect=_keyset_with_concurrent_exit)
        await dispatcher._dispatch_batch()

        assert published == []
        calls = repo.get_undispatched_commands.call_args_list
        assert len(calls) >= 2
        assert all("offset" not in call.kwargs for call in calls)
        assert calls[1].kwargs["after"] == (
            foreign[-1]["created_at"],
            foreign[-1]["public_id"],
            foreign[-1]["id"],
        )

    @pytest.mark.asyncio
    async def test_stale_deferred_follower_expires_while_fresh_follower_stays_deferred(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Stale shard-deferred followers check material then expire without publishing.

        Given: A failing cancel backing off on a shard, followed by a stale submit
            without evidence, a stale submit with durable submit evidence, a fresh
            submit, and a stale submit deferred by its own retry entry,
        When: A batch runs while the cancel and own-retry submit are backing off,
            and then runs again once the own-retry submit comes due,
        Then: Expire-only rows run material consistency before expiry checks
            without publishing; only the stale shard-deferred submit without
            evidence expires via CAS and invokes expire_fn; the evidence-bearing
            row enters shard-blocking backoff; the fresh row stays deferred; and
            the own-retry submit expires only when its own backoff comes due.
        """
        clock = _install_clock(monkeypatch, initial=0.0)
        head_cancel = _make_backoff_row("btc-cancel", command_type="cancel")
        own_retry = _make_backoff_row(
            "btc-own-retry",
            command_type="submit",
            created_at=datetime.now(UTC),
        )
        queue: list[list[TradeCommandRow]] = [[head_cancel]]
        repo = _make_queue_repo(queue)
        published: list[str] = []

        async def _publish(cmd: TradeCommandRow) -> None:
            if cmd["public_id"] in ("btc-cancel", "btc-own-retry"):
                raise RuntimeError("ZMQ down")
            published.append(cmd["public_id"])

        expire_fn = AsyncMock()
        publish_fn = AsyncMock(side_effect=_publish)
        dispatcher = OutboxDispatcher(
            repository=repo,
            publish_fn=publish_fn,
            batch_size=5,
            dispatch_ttl_s=30.0,
            expire_fn=expire_fn,
        )
        await dispatcher._dispatch_batch()
        clock.advance(0.25)
        await dispatcher._dispatch_batch()

        queue[0] = [own_retry]
        await dispatcher._dispatch_batch()
        clock.advance(0.05)

        own_retry_stale: TradeCommandRow = {
            **own_retry,
            "created_at": datetime.now(UTC) - timedelta(seconds=120),
        }
        stale = _make_backoff_row(
            "btc-stale",
            command_type="submit",
            created_at=datetime.now(UTC) - timedelta(seconds=120),
        )
        stale_with_evidence = _make_backoff_row(
            "btc-stale-evidence",
            command_type="submit",
            created_at=datetime.now(UTC) - timedelta(seconds=120),
        )
        fresh = _make_backoff_row(
            "btc-fresh",
            command_type="submit",
            created_at=datetime.now(UTC) - timedelta(seconds=5),
        )

        async def _evidence(client_order_id: str) -> bool:
            return client_order_id == stale_with_evidence["client_order_id"]

        repo.has_order_submit_evidence = AsyncMock(side_effect=_evidence)
        repo.require_consistent_trade_command_material.reset_mock()
        repo.cas_trade_command_status.reset_mock()
        expire_fn.reset_mock()
        publish_calls_before = publish_fn.await_count

        queue[0] = [head_cancel, stale, stale_with_evidence, fresh, own_retry_stale]
        await dispatcher._dispatch_batch()

        assert published == []
        assert publish_fn.await_count == publish_calls_before
        assert [
            call.args[0] for call in repo.require_consistent_trade_command_material.await_args_list
        ] == ["btc-stale", "btc-stale-evidence"]
        expire_fn.assert_awaited_once_with(stale)
        cas_kwargs = repo.cas_trade_command_status.await_args.kwargs
        assert cas_kwargs["public_id"] == "btc-stale"
        assert cas_kwargs["expected_status"] == TradeCommandStatusEnum.CREATED
        assert cas_kwargs["new_status"] == TradeCommandStatusEnum.EXPIRED
        assert "btc-own-retry" in dispatcher._retry
        assert "btc-stale-evidence" in dispatcher._retry
        assert dispatcher._retry["btc-stale-evidence"].kind is outbox_module._RetryKind.EXPIRY_PROBE

        clock.advance(0.25)
        queue[0] = [own_retry_stale]
        await dispatcher._dispatch_batch()
        assert expire_fn.await_count == 2
        assert expire_fn.await_args_list[1].args[0] == own_retry_stale

    @pytest.mark.asyncio
    async def test_healthy_shard_dispatches_past_999_blocked_same_shard_rows(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Non-stale shard-blocked rows do not exhaust max_scan_rows for other shards.

        Given: A failing cancel head followed by 999 fresh same-shard submits and
            one healthy ETH submit under max_scan_rows=1000,
        When: A dispatch batch runs while the cancel head is backing off,
        Then: The ETH submit dispatches without hitting max_scan_rows or warning.
        """
        clock = _install_clock(monkeypatch, initial=0.0)
        head = _make_backoff_row("btc-head", command_type="cancel", shard_key="kraken.BTC-USD.live")
        queue: list[list[TradeCommandRow]] = [[head]]
        repo = _make_queue_repo(queue)
        published: list[str] = []

        async def _publish(cmd: TradeCommandRow) -> None:
            if cmd["public_id"] == "btc-head":
                raise RuntimeError("ZMQ down")
            published.append(cmd["public_id"])

        dispatcher = OutboxDispatcher(
            repository=repo,
            publish_fn=AsyncMock(side_effect=_publish),
            batch_size=2,
            max_scan_rows=1000,
        )
        await dispatcher._dispatch_batch()

        clock.advance(0.05)
        blocked = [
            _make_backoff_row(f"btc-{i}", command_type="submit", shard_key="kraken.BTC-USD.live")
            for i in range(999)
        ]
        eth = _make_backoff_row("eth-1", command_type="submit", shard_key="kraken.ETH-USD.live")
        queue[0] = [head, *blocked, eth]
        handler_id, logs = _attach_log_capture()
        try:
            await dispatcher._dispatch_batch()
        finally:
            logger.remove(handler_id)

        assert published == ["eth-1"]
        assert not any(entry.level == "WARNING" for entry in logs)

    @pytest.mark.asyncio
    async def test_shard_skip_ceiling_warns_and_stops_scan(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Exceeding 10 * max_scan_rows shard-blocked skips emits a warning and stops.

        Given: A failing cancel head followed by 55 fresh same-shard submits and an
            ETH submit under max_scan_rows=5 (shard-skip ceiling 50),
        When: A dispatch batch runs during the cancel head's backoff window,
        Then: Scanning halts at 50 shard skips, emits a WARNING naming the ceiling,
            and stops before reaching the ETH command.
        """
        clock = _install_clock(monkeypatch, initial=0.0)
        head = _make_backoff_row("btc-head", command_type="cancel", shard_key="kraken.BTC-USD.live")
        queue: list[list[TradeCommandRow]] = [[head]]
        repo = _make_queue_repo(queue)
        published: list[str] = []

        async def _publish(cmd: TradeCommandRow) -> None:
            if cmd["public_id"] == "btc-head":
                raise RuntimeError("ZMQ down")
            published.append(cmd["public_id"])

        dispatcher = OutboxDispatcher(
            repository=repo,
            publish_fn=AsyncMock(side_effect=_publish),
            batch_size=2,
            max_scan_rows=5,
        )
        await dispatcher._dispatch_batch()

        clock.advance(0.05)
        blocked = [
            _make_backoff_row(f"btc-{i}", command_type="submit", shard_key="kraken.BTC-USD.live")
            for i in range(55)
        ]
        eth = _make_backoff_row("eth-1", command_type="submit", shard_key="kraken.ETH-USD.live")
        queue[0] = [head, *blocked, eth]
        handler_id, logs = _attach_log_capture()
        try:
            await dispatcher._dispatch_batch()
        finally:
            logger.remove(handler_id)

        assert published == []
        assert any(
            entry.level == "WARNING"
            and "50" in entry.message
            and "max_scan_rows=5" in entry.message
            for entry in logs
        )

    @pytest.mark.asyncio
    async def test_clock_step_does_not_hold_a_fresh_shard_follower(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A follower that becomes fresh can publish as soon as its shard clears.

        Given: A cancel head in backoff and a submit just older than its TTL,
        When: The wall clock steps back between fetching and checking expiry,
        Then: The submit stays queued without a probe or expiry, and publishes
            on the next poll once the cancel head is gone.
        """
        _install_clock(monkeypatch)
        fetched_at = datetime(2026, 1, 1, tzinfo=UTC)
        stepped_at = fetched_at - timedelta(seconds=2)
        head = _make_backoff_row(
            "btc-cancel-head", command_type="cancel", created_at=fetched_at - timedelta(minutes=1)
        )
        follower = _make_backoff_row(
            "btc-follower", command_type="submit", created_at=fetched_at - timedelta(seconds=31)
        )
        queue = [[head]]
        repo = _make_queue_repo(queue)
        publish_fn = AsyncMock(side_effect=[RuntimeError("ZMQ down"), None])
        expire_fn = AsyncMock()
        dispatcher = OutboxDispatcher(
            repository=repo, publish_fn=publish_fn, dispatch_ttl_s=30.0, expire_fn=expire_fn
        )
        wall_clock = MagicMock(return_value=fetched_at)
        monkeypatch.setattr(outbox_module, "datetime", MagicMock(now=wall_clock))
        await dispatcher._dispatch_batch()
        publish_fn.reset_mock()
        repo.cas_trade_command_status.reset_mock()

        queue[0] = [head, follower]
        wall_clock.side_effect = [fetched_at, stepped_at]
        await dispatcher._dispatch_batch()

        publish_fn.assert_not_awaited()
        repo.has_order_submit_evidence.assert_not_awaited()
        repo.cas_trade_command_status.assert_not_awaited()
        expire_fn.assert_not_awaited()

        queue[0] = [follower]
        wall_clock.side_effect = None
        wall_clock.return_value = stepped_at
        await dispatcher._dispatch_batch()

        publish_fn.assert_awaited_once_with(follower)
        repo.bulk_dispatch_trade_commands.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_stale_deferred_follower_blocked_by_material_barrier_does_not_expire(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Stale shard-deferred create blocked by material barrier is neither expired nor published.

        Given: A backing-off cancel head followed by a stale create whose material
            consistency check raises TradeCommandMaterialChangedError,
        When: A dispatch batch processes the expire-only candidate,
        Then: The create is held by its non-shard-blocking own retry entry,
            neither has_order_submit_evidence nor cas_trade_command_status runs,
            and expire_fn and publish_fn are not called.
        """
        clock = _install_clock(monkeypatch, initial=0.0)
        head_cancel = _make_backoff_row("btc-cancel-head", command_type="cancel")
        queue: list[list[TradeCommandRow]] = [[head_cancel]]
        repo = _make_queue_repo(queue)
        published: list[str] = []

        async def _publish(cmd: TradeCommandRow) -> None:
            if cmd["public_id"] == "btc-cancel-head":
                raise RuntimeError("ZMQ down")
            published.append(cmd["public_id"])

        publish_fn = AsyncMock(side_effect=_publish)
        expire_fn = AsyncMock()
        dispatcher = OutboxDispatcher(
            repository=repo,
            publish_fn=publish_fn,
            batch_size=5,
            dispatch_ttl_s=30.0,
            expire_fn=expire_fn,
        )
        await dispatcher._dispatch_batch()
        clock.advance(0.05)

        stale_create = _make_backoff_row(
            "btc-stale-create",
            command_type="create",
            created_at=datetime.now(UTC) - timedelta(seconds=120),
        )
        repo.require_consistent_trade_command_material = AsyncMock(
            side_effect=TradeCommandMaterialChangedError("btc-stale-create", ("price",))
        )
        repo.has_order_submit_evidence.reset_mock()
        repo.cas_trade_command_status.reset_mock()
        publish_calls_before = publish_fn.await_count

        queue[0] = [head_cancel, stale_create]
        await dispatcher._dispatch_batch()

        assert published == []
        assert publish_fn.await_count == publish_calls_before
        repo.require_consistent_trade_command_material.assert_awaited_once_with("btc-stale-create")
        repo.has_order_submit_evidence.assert_not_awaited()
        repo.cas_trade_command_status.assert_not_awaited()
        expire_fn.assert_not_awaited()
        assert "btc-stale-create" in dispatcher._retry
        assert (
            dispatcher._retry["btc-stale-create"].kind
            is outbox_module._RetryKind.NONBLOCKING_FAILURE
        )

    @pytest.mark.asyncio
    async def test_dispatch_batch_commits_dispatches_before_expire_only_probes(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Successful dispatches commit via bulk_dispatch before expire-only work starts.

        Given: A backing-off BTC cancel head, a stale BTC submit follower, and a
            healthy ETH submit in the same poll cycle,
        When: The dispatch batch records repository, publish, and expire calls,
        Then: ``bulk_dispatch_trade_commands`` commits the ETH publish strictly
            before any material check, evidence probe, CAS, or expire callback
            for the expire-only BTC follower.
        """
        clock = _install_clock(monkeypatch, initial=0.0)
        head_cancel = _make_backoff_row(
            "btc-cancel-head", command_type="cancel", shard_key="kraken.BTC-USD.live"
        )
        queue: list[list[TradeCommandRow]] = [[head_cancel]]
        repo = _make_queue_repo(queue)
        events: list[str] = []

        async def _publish(cmd: TradeCommandRow) -> None:
            if cmd["public_id"] == "btc-cancel-head":
                raise RuntimeError("ZMQ down")
            events.append(f"publish:{cmd['public_id']}")

        async def _material(public_id: str) -> None:
            events.append(f"material:{public_id}")

        async def _bulk_dispatch(updates: list[object]) -> int:
            events.append("bulk_dispatch")
            return len(updates)

        async def _evidence(client_order_id: str) -> bool:
            events.append(f"evidence:{client_order_id}")
            return False

        async def _cas(**kwargs: object) -> bool:
            events.append(f"cas:{kwargs['public_id']}")
            return True

        async def _expire(cmd: TradeCommandRow) -> None:
            events.append(f"expire:{cmd['public_id']}")

        repo.require_consistent_trade_command_material = AsyncMock(side_effect=_material)
        repo.bulk_dispatch_trade_commands = AsyncMock(side_effect=_bulk_dispatch)
        repo.has_order_submit_evidence = AsyncMock(side_effect=_evidence)
        repo.cas_trade_command_status = AsyncMock(side_effect=_cas)
        expire_fn = AsyncMock(side_effect=_expire)

        dispatcher = OutboxDispatcher(
            repository=repo,
            publish_fn=AsyncMock(side_effect=_publish),
            batch_size=5,
            dispatch_ttl_s=30.0,
            expire_fn=expire_fn,
        )
        await dispatcher._dispatch_batch()
        clock.advance(0.05)

        stale_btc = _make_backoff_row(
            "btc-stale",
            command_type="submit",
            shard_key="kraken.BTC-USD.live",
            created_at=datetime.now(UTC) - timedelta(seconds=120),
        )
        eth_submit = _make_backoff_row(
            "eth-1",
            command_type="submit",
            shard_key="kraken.ETH-USD.live",
        )
        events.clear()
        queue[0] = [head_cancel, stale_btc, eth_submit]
        await dispatcher._dispatch_batch()

        assert events == [
            "material:eth-1",
            "publish:eth-1",
            "bulk_dispatch",
            "material:btc-stale",
            f"evidence:{stale_btc['client_order_id']}",
            "cas:btc-stale",
            "expire:btc-stale",
        ]

    @pytest.mark.asyncio
    async def test_stale_deferred_with_submit_evidence_waits_with_info_cadence_without_reprobe(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Stale submit-evidence followers back off with INFO cadence instead of re-probing.

        Given: A failing cancel head backing off for at least 1 s and a stale
            same-shard submit whose evidence probe returns True,
        When: Ticks run initially, during the 0.25 s cooldown, and once the
            cooldown expires,
        Then: Attempt 1 probes evidence once, records an expiry probe cooldown
            entry, and emits one INFO log without ERROR/traceback; the cooldown
            tick skips the evidence probe silently; the due retry re-probes with
            WARNING cadence; and the command never publishes.
        """
        clock = _install_clock(monkeypatch, initial=100.0)
        head_cancel = _make_backoff_row("btc-cancel-head", command_type="cancel")
        queue: list[list[TradeCommandRow]] = [[head_cancel]]
        repo = _make_queue_repo(queue)
        published: list[str] = []

        async def _publish(cmd: TradeCommandRow) -> None:
            if cmd["public_id"] == "btc-cancel-head":
                raise RuntimeError("ZMQ down")
            published.append(cmd["public_id"])

        publish_fn = AsyncMock(side_effect=_publish)
        expire_fn = AsyncMock()
        dispatcher = OutboxDispatcher(
            repository=repo,
            publish_fn=publish_fn,
            batch_size=5,
            dispatch_ttl_s=30.0,
            expire_fn=expire_fn,
        )
        for step_delay in (0.25, 0.50, 1.0):
            await dispatcher._dispatch_batch()
            clock.advance(step_delay)
        await dispatcher._dispatch_batch()
        clock.advance(0.05)

        stale_evidence = _make_backoff_row(
            "btc-stale-evidence",
            command_type="submit",
            created_at=datetime.now(UTC) - timedelta(seconds=120),
        )
        repo.has_order_submit_evidence = AsyncMock(return_value=True)
        publish_calls_before = publish_fn.await_count
        queue[0] = [head_cancel, stale_evidence]

        handler_id, logs = _attach_log_capture()
        try:
            await dispatcher._dispatch_batch()
            assert repo.has_order_submit_evidence.await_count == 1
            assert published == []
            assert publish_fn.await_count == publish_calls_before
            expire_fn.assert_not_awaited()
            assert "btc-stale-evidence" in dispatcher._retry
            assert (
                dispatcher._retry["btc-stale-evidence"].kind
                is outbox_module._RetryKind.EXPIRY_PROBE
            )
            assert [(entry.level, entry.has_exception) for entry in logs] == [("INFO", False)]
            assert "stale with submit evidence; waiting behind shard head" in logs[0].message
            logs.clear()

            clock.advance(0.05)
            await dispatcher._dispatch_batch()
            assert repo.has_order_submit_evidence.await_count == 1
            assert logs == []
            assert published == []

            clock.advance(0.20)
            await dispatcher._dispatch_batch()
            assert repo.has_order_submit_evidence.await_count == 2
            assert len(logs) == 1
            assert logs[0].level == "WARNING"
            assert "btc-stale-evidence" in logs[0].message
            assert "attempt=2" in logs[0].message
            assert "0.50s" in logs[0].message
            assert published == []
        finally:
            logger.remove(handler_id)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("evidence_remains", [True, False])
    async def test_evidence_probe_cooldown_does_not_hold_dispatch_after_head_clears(
        self, monkeypatch: pytest.MonkeyPatch, evidence_remains: bool
    ) -> None:
        """Recheck and handle the stale follower as soon as its shard clears.

        Given: A cancel in backoff, a stale submit with evidence, and a later fresh create,
        When: A repeat head failure still blocks the shard, then the head disappears before
            the stale submit's expiry probe cooldown ends,
        Then: No repeat probe runs while blocked. Once clear, the submit is rechecked and
            either publishes before the create or expires before the create publishes.
        """
        clock = _install_clock(monkeypatch, initial=0.0)
        wall_now = datetime(2026, 1, 1, tzinfo=UTC)
        monkeypatch.setattr(
            outbox_module, "datetime", MagicMock(now=MagicMock(return_value=wall_now))
        )
        head = _make_backoff_row(
            "btc-cancel-head", command_type="cancel", created_at=wall_now - timedelta(minutes=3)
        )
        stale = _make_backoff_row("btc-stale", created_at=wall_now - timedelta(minutes=2))
        fresh = _make_backoff_row(
            "btc-fresh", command_type="create", created_at=wall_now - timedelta(seconds=5)
        )
        queue = [[head]]
        repo = _make_queue_repo(queue)
        repo.has_order_submit_evidence = AsyncMock(side_effect=[True, evidence_remains])
        publish_fn = AsyncMock(
            side_effect=[RuntimeError("ZMQ down"), RuntimeError("ZMQ down"), None, None]
        )
        expire_fn = AsyncMock()
        dispatcher = OutboxDispatcher(
            repository=repo, publish_fn=publish_fn, dispatch_ttl_s=30.0, expire_fn=expire_fn
        )
        await dispatcher._dispatch_batch()

        clock.advance(0.05)
        queue[0] = [head, stale, fresh]
        await dispatcher._dispatch_batch()
        publish_fn.assert_awaited_once_with(head)
        repo.has_order_submit_evidence.assert_awaited_once_with(stale["client_order_id"])
        expire_fn.assert_not_awaited()

        clock.advance(0.20)
        await dispatcher._dispatch_batch()
        assert [attempt.args[0] for attempt in publish_fn.await_args_list] == [head, head]
        repo.has_order_submit_evidence.assert_awaited_once_with(stale["client_order_id"])
        expire_fn.assert_not_awaited()

        clock.advance(0.01)
        queue[0] = [stale, fresh]
        repo.cas_trade_command_status.reset_mock()
        effects = MagicMock()
        effects.attach_mock(publish_fn, "publish")
        effects.attach_mock(expire_fn, "expire")
        await dispatcher._dispatch_batch()

        assert [attempt.args[0] for attempt in repo.has_order_submit_evidence.await_args_list] == [
            stale["client_order_id"],
            stale["client_order_id"],
        ]
        if evidence_remains:
            assert effects.mock_calls == [call.publish(stale), call.publish(fresh)]
            repo.cas_trade_command_status.assert_not_awaited()
        else:
            assert effects.mock_calls == [call.expire(stale), call.publish(fresh)]
            repo.cas_trade_command_status.assert_awaited_once()
            expiry = repo.cas_trade_command_status.await_args.kwargs
            assert expiry["public_id"] == stale["public_id"]
            assert expiry["expected_status"] == TradeCommandStatusEnum.CREATED
            assert expiry["new_status"] == TradeCommandStatusEnum.EXPIRED

    @pytest.mark.asyncio
    async def test_keyset_cursor_missing_id_raises_key_error(self) -> None:
        """Building a keyset cursor from a row without ``id`` fails loudly with KeyError.

        Given: A repository returning an unowned page row that lacks ``id``,
        When: The multi-page keyset scan advances the cursor past page one,
        Then: ``KeyError`` is raised rather than defaulting ``id`` to zero.
        """
        row = _make_backoff_row("missing-id-row", shard_key="kraken.FOREIGN.live")
        row.pop("id")
        repo = _make_queue_repo([[row]])
        ownership = MagicMock()
        ownership.owns.return_value = False
        dispatcher = OutboxDispatcher(
            repository=repo,
            publish_fn=AsyncMock(),
            batch_size=1,
            ownership=ownership,
        )
        with pytest.raises(KeyError):
            await dispatcher._fetch_owned_batch(datetime.now(UTC))
