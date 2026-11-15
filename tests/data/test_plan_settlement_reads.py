"""Unit and SQLite integration tests for `snapper.data.plan_settlement_reads`."""

from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
import pytest_asyncio

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import ExecutionPlan
from snapper.data.models import TradeCommand
from snapper.data.plan_settlement_reads import SessionPlanSettlementReader
from snapper.data.plan_settlement_reads import begin_plan_settlement_transaction
from snapper.data.plan_settlement_reads import filter_open_versions
from snapper.data.plan_settlement_reads import lock_active_execution_plans
from snapper.data.plan_settlement_reads import lock_child_create_commands
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import ExecutionPlanInsertRow
from snapper.data.repository_types import TradeCommandInsertRow
from snapper.data.repository_types import VenueEventInsertRow

_T0 = datetime(2026, 11, 15, 11, 0, 0, tzinfo=UTC)
_WALLET_ID = "01960000-0000-7000-8000-000000000001"
_INSTRUMENT_ID = "01960000-0000-7000-8000-000000000002"


@pytest_asyncio.fixture
async def sqlite_repo(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Create and dispose an isolated SQLite repository for settlement read tests.

    Args:
        tmp_path: Pytest temporary directory.

    Yields:
        Initialized `SQLAlchemyRepository`.
    """
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'reads.db'}")
    await repo.create_all()
    try:
        yield repo
    finally:
        await repo.engine.dispose()


def test_filter_open_versions_keeps_only_open_ended_rows() -> None:
    """Reject closed temporal rows, including finite future `known_to` values.

    Given: Three `ExecutionPlan` ORM objects with `KNOWN_TO_MAX`, past finite
        `known_to`, and future finite `known_to`.
    When: `filter_open_versions` filters the sequence.
    Then: Only the exact `KNOWN_TO_MAX` row is retained.
    """
    open_row = ExecutionPlan(public_id="p-open", known_to=KNOWN_TO_MAX)
    closed_past = ExecutionPlan(public_id="p-past", known_to=_T0)
    closed_future = ExecutionPlan(
        public_id="p-future",
        known_to=datetime(2099, 1, 1, 0, 0, 0, tzinfo=UTC),
    )

    kept = filter_open_versions([closed_past, open_row, closed_future])

    assert kept == [open_row]


@pytest.mark.asyncio
async def test_begin_plan_settlement_transaction_handles_supported_and_unknown_dialects() -> None:
    """Issue dialect-appropriate transaction statements and reject unknown dialects.

    Given: Mocked async sessions invoked with `sqlite`, `postgresql`, and `mysql`.
    When: `begin_plan_settlement_transaction` runs for each dialect.
    Then: `sqlite` executes `BEGIN IMMEDIATE`, `postgresql` executes
        `SET TRANSACTION ISOLATION LEVEL READ COMMITTED`, and `mysql` raises
        `NotImplementedError`.
    """
    sqlite_session = AsyncMock()
    await begin_plan_settlement_transaction(sqlite_session, dialect_name="sqlite")
    sqlite_stmt = str(sqlite_session.execute.await_args_list[0].args[0])
    assert sqlite_stmt == "BEGIN IMMEDIATE"

    pg_session = AsyncMock()
    await begin_plan_settlement_transaction(pg_session, dialect_name="postgresql")
    pg_stmt = str(pg_session.execute.await_args_list[0].args[0])
    assert pg_stmt == "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"

    with pytest.raises(NotImplementedError, match="dialect=mysql"):
        await begin_plan_settlement_transaction(AsyncMock(), dialect_name="mysql")


@pytest.mark.asyncio
async def test_lock_helpers_handle_empty_sqlite_and_duplicate_open_rows(
    sqlite_repo: SQLAlchemyRepository,
) -> None:
    """Lock open child commands and plans cleanly while failing closed on duplicates.

    Given: Seeded SQLite execution plan and child command versions alongside
        mocked sessions returning duplicate open-ended rows.
    When: `lock_child_create_commands` and `lock_active_execution_plans` execute.
    Then: Empty inputs return `{}`; valid SQLite rows return the open versions;
        duplicate open-ended rows raise `RuntimeError`.
    """
    plan_insert: ExecutionPlanInsertRow = {
        "plan_type": "manual_once",
        "created_via": "api",
        "instrument_public_id": _INSTRUMENT_ID,
        "exchange": "kraken",
        "mode": "live",
        "shard_key": f"kraken.XRP-EUR.live.{_WALLET_ID}",
        "wallet_public_id": _WALLET_ID,
        "total_quantity": 5.0,
        "side": "buy",
        "params": {"child_client_order_id": "cid-lock-1", "native_instrument": "XRP-EUR"},
        "status": "active",
        "created_at": _T0,
        "session_id": "s-lock",
        "sequence_id": 1,
        "timestamp": _T0,
    }
    _pid_num, plan_pid = await sqlite_repo.insert_execution_plan(plan_insert)
    cmd_insert: TradeCommandInsertRow = {
        "command_type": "create",
        "shard_key": f"kraken.XRP-EUR.live.{_WALLET_ID}",
        "exchange": "kraken",
        "instrument": "XRP-EUR",
        "mode": "live",
        "strategy_id": "manual_once",
        "client_order_id": "cid-lock-1",
        "venue_client_id": "cid-lock-1",
        "side": "buy",
        "order_type": "market",
        "quantity": 5.0,
        "status": "filled",
        "created_at": _T0,
        "correlation_id": plan_pid,
        "session_id": "s-lock",
        "sequence_id": 2,
        "timestamp": _T0,
        "wallet_public_id": _WALLET_ID,
        "plan_public_id": plan_pid,
    }
    await sqlite_repo.insert_trade_command(cmd_insert)

    async with sqlite_repo.session() as session:
        await begin_plan_settlement_transaction(session, dialect_name="sqlite")
        assert await lock_child_create_commands(session, child_orders=()) == {}
        assert await lock_active_execution_plans(session, plan_public_ids=()) == {}
        locked_cmds = await lock_child_create_commands(
            session,
            child_orders=[("kraken", "cid-lock-1"), ("kraken", "cid-lock-1")],
        )
        locked_plans = await lock_active_execution_plans(
            session,
            plan_public_ids=[plan_pid, plan_pid],
        )
        assert set(locked_cmds) == {("kraken", "cid-lock-1")}
        assert set(locked_plans) == {plan_pid}
        await session.rollback()

    dup_cmd_scalars = MagicMock()
    dup_cmd_scalars.all.return_value = [
        TradeCommand(exchange="kraken", client_order_id="cid-dup", known_to=KNOWN_TO_MAX),
        TradeCommand(exchange="kraken", client_order_id="cid-dup", known_to=KNOWN_TO_MAX),
    ]
    dup_cmd_result = MagicMock()
    dup_cmd_result.scalars.return_value = dup_cmd_scalars
    dup_cmd_session = AsyncMock(execute=AsyncMock(return_value=dup_cmd_result))
    with pytest.raises(RuntimeError, match="Multiple active create/submit commands"):
        await lock_child_create_commands(
            dup_cmd_session,
            child_orders=[("kraken", "cid-dup")],
        )

    dup_plan_scalars = MagicMock()
    dup_plan_scalars.all.return_value = [
        ExecutionPlan(public_id="pid-dup", known_to=KNOWN_TO_MAX),
        ExecutionPlan(public_id="pid-dup", known_to=KNOWN_TO_MAX),
    ]
    dup_plan_result = MagicMock()
    dup_plan_result.scalars.return_value = dup_plan_scalars
    dup_plan_session = AsyncMock(execute=AsyncMock(return_value=dup_plan_result))
    with pytest.raises(RuntimeError, match="Multiple active execution plan versions"):
        await lock_active_execution_plans(
            dup_plan_session,
            plan_public_ids=["pid-dup"],
        )


@pytest.mark.asyncio
async def test_session_plan_settlement_reader_executes_all_queries_on_bound_session(
    sqlite_repo: SQLAlchemyRepository,
) -> None:
    """Read plans, child commands, lifecycle events, fills, and timestamps on one session.

    Given: Seeded actionable and completed plans, a child command, lifecycle and
        fill venue events in SQLite, plus a mocked duplicate-command session.
    When: `SessionPlanSettlementReader` methods execute across empty, normal,
        chunked (`> 300` ids), status-filtered, and duplicate-command inputs.
    Then: Every query returns exact typed rows on the bound session and raises
        `RuntimeError` on duplicate open child commands.
    """
    active_insert: ExecutionPlanInsertRow = {
        "plan_type": "manual_once",
        "created_via": "api",
        "instrument_public_id": _INSTRUMENT_ID,
        "exchange": "kraken",
        "mode": "live",
        "shard_key": f"kraken.XRP-EUR.live.{_WALLET_ID}",
        "wallet_public_id": _WALLET_ID,
        "total_quantity": 8.0,
        "side": "buy",
        "params": {"child_client_order_id": "cid-read-1", "native_instrument": "XRP-EUR"},
        "status": "active",
        "created_at": _T0,
        "session_id": "s-read",
        "sequence_id": 1,
        "timestamp": _T0,
    }
    _id_active, active_pid = await sqlite_repo.insert_execution_plan(active_insert)
    done_insert = dict(active_insert)
    done_insert["status"] = "completed"
    done_insert["sequence_id"] = 2
    await sqlite_repo.insert_execution_plan(done_insert)

    cmd_insert: TradeCommandInsertRow = {
        "command_type": "create",
        "shard_key": f"kraken.XRP-EUR.live.{_WALLET_ID}",
        "exchange": "kraken",
        "instrument": "XRP-EUR",
        "mode": "live",
        "strategy_id": "manual_once",
        "client_order_id": "cid-read-1",
        "venue_client_id": "cid-read-1",
        "side": "buy",
        "order_type": "market",
        "quantity": 8.0,
        "status": "filled",
        "created_at": _T0,
        "correlation_id": active_pid,
        "session_id": "s-read",
        "sequence_id": 3,
        "timestamp": _T0,
        "wallet_public_id": _WALLET_ID,
        "plan_public_id": active_pid,
    }
    await sqlite_repo.insert_trade_command(cmd_insert)

    fill_event: VenueEventInsertRow = {
        "event_type": "fill_observed",
        "shard_key": f"kraken.XRP-EUR.live.{_WALLET_ID}",
        "exchange": "kraken",
        "instrument": "XRP-EUR",
        "mode": "live",
        "client_order_id": "cid-read-1",
        "venue_client_id": "cid-read-1",
        "exchange_order_id": "EX-READ-1",
        "exec_id": "EXEC-READ-1",
        "side": "buy",
        "status": "filled",
        "fill_size": 8.0,
        "fill_price": 2.2,
        "cum_fill_size": 8.0,
        "liquidity_role": "taker",
        "received_at": _T0 + timedelta(seconds=3),
        "session_id": "s-read",
        "sequence_id": 4,
        "timestamp": _T0 + timedelta(seconds=3),
        "wallet_public_id": _WALLET_ID,
    }
    await sqlite_repo.insert_venue_event(fill_event)

    async with sqlite_repo.session() as session:
        reader = SessionPlanSettlementReader(sqlite_repo, session)
        plans = await reader.get_active_execution_plans()
        assert [row["public_id"] for row in plans] == [active_pid]

        found_cmd = await reader.get_active_create_command_by_client_order_id(
            "cid-read-1", "kraken"
        )
        missing_cmd = await reader.get_active_create_command_by_client_order_id(
            "cid-missing", "kraken"
        )
        assert found_cmd is not None and found_cmd["client_order_id"] == "cid-read-1"
        assert missing_cmd is None

        assert await reader.get_order_lifecycle_events([]) == []
        many_ids = [f"cid-pad-{idx}" for idx in range(305)] + ["cid-read-1"]
        events = await reader.get_order_lifecycle_events(many_ids)
        assert len(events) == 1 and events[0]["event_type"] == "fill_observed"

        max_fill = await reader.get_max_cumulative_fill_venue_event("cid-read-1")
        no_fill = await reader.get_max_cumulative_fill_venue_event("cid-missing")
        assert max_fill is not None and max_fill["cum_fill_size"] == 8.0
        assert no_fill is None

        ts_unfiltered = await reader.get_latest_venue_event_received_at(
            "cid-read-1", ("fill_observed",)
        )
        ts_filtered = await reader.get_latest_venue_event_received_at(
            "cid-read-1", ("fill_observed",), statuses=("FILLED",)
        )
        assert ts_unfiltered == _T0 + timedelta(seconds=3)
        assert ts_filtered == _T0 + timedelta(seconds=3)

    dup_scalars = MagicMock()
    dup_scalars.all.return_value = [
        TradeCommand(exchange="kraken", client_order_id="cid-dup", known_to=KNOWN_TO_MAX),
        TradeCommand(exchange="kraken", client_order_id="cid-dup", known_to=KNOWN_TO_MAX),
    ]
    dup_result = MagicMock()
    dup_result.scalars.return_value = dup_scalars
    dup_session = AsyncMock(execute=AsyncMock(return_value=dup_result))
    dup_reader = SessionPlanSettlementReader(sqlite_repo, dup_session)
    with pytest.raises(RuntimeError, match="Multiple active create/submit commands"):
        await dup_reader.get_active_create_command_by_client_order_id("cid-dup", "kraken")
