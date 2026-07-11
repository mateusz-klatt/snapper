"""Lineage/notional column persistence and SCD2 successor carry.

PnL Phase 1 regression pins: ``signal_public_id``,
``ai_review_public_id``, and ``submitted_notional_usd`` must (a)
round-trip through ``insert_trade_command`` and every read
projection, and (b) survive EVERY SCD2 status-transition successor —
``update_trade_command_status``, ``advance_trade_command_lifecycle``,
and ``bulk_dispatch_trade_commands`` — via the centralized
``_trade_command_carry_kwargs`` helper. The same helper repairs the
historical ``source_surface`` loss (it was reset to its ``'rest'``
server default by the first status transition on two of the three
successor paths), so ``source_surface`` carry is pinned here too.
"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import pytest

from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import TradeCommandInsertRow


def _lineage_row(
    now: datetime,
    *,
    mode: str = "paper",
    exchange: str = "paper",
    shard_key: str = "paper.BTC-USD.paper",
    client_order_id: str = "cid-lineage-1",
    correlation_id: str = "corr-lineage-1",
    sequence_id: int = 1,
) -> TradeCommandInsertRow:
    """Build a strategy-surface insert row carrying full lineage.

    Args:
        now: Bus timestamp for created_at/timestamp.
        mode: Execution mode column (live rows feed the recent-submits
            projection; paper rows are excluded there).
        exchange: Venue column.
        shard_key: Shard key column.
        client_order_id: Client order id (also used as venue id).
        correlation_id: Correlation id column.
        sequence_id: Bus sequence for the insert.

    Returns:
        Insert row with non-default lineage, notional, and surface
        values so any successor that drops a column is caught.
    """
    return {
        "command_type": "submit",
        "shard_key": shard_key,
        "exchange": exchange,
        "instrument": "BTC-USD",
        "mode": mode,
        "strategy_id": "engine-buy",
        "client_order_id": client_order_id,
        "venue_client_id": client_order_id,
        "side": "buy",
        "order_type": "market",
        "quantity": 0.02,
        "price": 64181.3,
        "status": "created",
        "created_at": now,
        "correlation_id": correlation_id,
        "session_id": "s1",
        "sequence_id": sequence_id,
        "timestamp": now,
        "wallet_public_id": "wal-1",
        "user_public_id": "user-1",
        "source_surface": "strategy",
        "signal_public_id": "sig-abc",
        "ai_review_public_id": "rev-def",
        "submitted_notional_usd": 1283.63,
    }


async def _fresh_repo(tmp_path: Path, name: str) -> SQLAlchemyRepository:
    """Create a throwaway SQLite repository with full schema.

    Args:
        tmp_path: Pytest temporary directory.
        name: Database file name.

    Returns:
        Initialized repository.
    """
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / name}")
    await repo.create_all()
    return repo


@pytest.mark.asyncio
async def test_insert_and_projection_round_trip_lineage(tmp_path: Path) -> None:
    """Lineage/notional round-trip through insert and read projection.

    Given: an insert row carrying signal/review lineage and a
        cent-quantized admission notional,
    When: the command is inserted and fetched by public_id,
    Then: all three values (and source_surface) read back verbatim,
        with the Numeric notional projected as float.
    """
    repo = await _fresh_repo(tmp_path, "lineage_rt.db")
    now = datetime.now(UTC)
    _, cmd_pid = await repo.insert_trade_command(_lineage_row(now))
    row = await repo.get_trade_command_by_public_id(cmd_pid, as_of=now)
    assert row is not None
    assert row["signal_public_id"] == "sig-abc"
    assert row["ai_review_public_id"] == "rev-def"
    assert row["submitted_notional_usd"] == 1283.63
    assert row["source_surface"] == "strategy"


@pytest.mark.asyncio
async def test_insert_defaults_lineage_to_null(tmp_path: Path) -> None:
    """Rows inserted without lineage keys project NULL lineage.

    Given: an insert row omitting the three Phase 1 keys (cancel and
        compensation paths never set them),
    When: the command is inserted and fetched,
    Then: the projection carries None for all three.
    """
    repo = await _fresh_repo(tmp_path, "lineage_null.db")
    now = datetime.now(UTC)
    row_in = _lineage_row(now)
    del row_in["signal_public_id"]
    del row_in["ai_review_public_id"]
    del row_in["submitted_notional_usd"]
    _, cmd_pid = await repo.insert_trade_command(row_in)
    row = await repo.get_trade_command_by_public_id(cmd_pid, as_of=now)
    assert row is not None
    assert row["signal_public_id"] is None
    assert row["ai_review_public_id"] is None
    assert row["submitted_notional_usd"] is None


@pytest.mark.asyncio
async def test_update_status_successor_carries_lineage_and_surface(tmp_path: Path) -> None:
    """``update_trade_command_status`` successor keeps lineage columns.

    Given: an active command with non-default lineage, notional, and
        ``source_surface='strategy'``,
    When: the status transitions created → dispatched → accepted (two
        SCD2 close-and-insert cycles),
    Then: every successor still carries all four values — pinning the
        fix for the historical source_surface reset on this path.
    """
    repo = await _fresh_repo(tmp_path, "lineage_upd.db")
    now = datetime.now(UTC)
    _, cmd_pid = await repo.insert_trade_command(_lineage_row(now))
    t1 = now + timedelta(seconds=1)
    assert (
        await repo.update_trade_command_status(
            public_id=cmd_pid,
            new_status="dispatched",
            bus_time=t1,
            session_id="s1",
            sequence_id=2,
            dispatched_at=t1,
        )
        is not None
    )
    t2 = now + timedelta(seconds=2)
    assert (
        await repo.update_trade_command_status(
            public_id=cmd_pid,
            new_status="accepted",
            bus_time=t2,
            session_id="s1",
            sequence_id=3,
            acked_at=t2,
        )
        is not None
    )
    row = await repo.get_trade_command_by_public_id(cmd_pid, as_of=t2)
    assert row is not None
    assert row["status"] == "accepted"
    assert row["signal_public_id"] == "sig-abc"
    assert row["ai_review_public_id"] == "rev-def"
    assert row["submitted_notional_usd"] == 1283.63
    assert row["source_surface"] == "strategy"


@pytest.mark.asyncio
async def test_advance_lifecycle_successor_carries_lineage(tmp_path: Path) -> None:
    """``advance_trade_command_lifecycle`` successor keeps lineage columns.

    Given: an active command with non-default lineage values,
    When: the CAS lifecycle advance moves created → dispatched,
    Then: the successor carries lineage, notional, and surface.
    """
    repo = await _fresh_repo(tmp_path, "lineage_adv.db")
    now = datetime.now(UTC)
    _, cmd_pid = await repo.insert_trade_command(_lineage_row(now))
    t1 = now + timedelta(seconds=1)
    assert await repo.advance_trade_command_lifecycle(
        public_id=cmd_pid,
        expected_status="created",
        new_status="dispatched",
        bus_time=t1,
        session_id="s1",
        sequence_id=2,
    )
    row = await repo.get_trade_command_by_public_id(cmd_pid, as_of=t1)
    assert row is not None
    assert row["signal_public_id"] == "sig-abc"
    assert row["ai_review_public_id"] == "rev-def"
    assert row["submitted_notional_usd"] == 1283.63
    assert row["source_surface"] == "strategy"


@pytest.mark.asyncio
async def test_bulk_dispatch_successor_carries_lineage(tmp_path: Path) -> None:
    """``bulk_dispatch_trade_commands`` successor keeps lineage columns.

    Given: an active created command with non-default lineage values,
    When: the outbox bulk-dispatch transition runs,
    Then: the dispatched successor carries lineage, notional, and
        surface — pinning the fix for the source_surface reset on the
        bulk path.
    """
    repo = await _fresh_repo(tmp_path, "lineage_bulk.db")
    now = datetime.now(UTC)
    _, cmd_pid = await repo.insert_trade_command(_lineage_row(now))
    t1 = now + timedelta(seconds=1)
    applied = await repo.bulk_dispatch_trade_commands(
        [
            {
                "public_id": cmd_pid,
                "bus_time": t1,
                "session_id": "s2",
                "sequence_id": 9,
                "dispatched_at": t1,
                "attempt_count": 1,
            }
        ]
    )
    assert applied == 1
    row = await repo.get_trade_command_by_public_id(cmd_pid, as_of=t1)
    assert row is not None
    assert row["status"] == "dispatched"
    assert row["signal_public_id"] == "sig-abc"
    assert row["ai_review_public_id"] == "rev-def"
    assert row["submitted_notional_usd"] == 1283.63
    assert row["source_surface"] == "strategy"


@pytest.mark.asyncio
async def test_recent_submits_projects_submitted_notional(tmp_path: Path) -> None:
    """``get_user_recent_submits`` projects the notional snapshot.

    Given: two live submit commands for one user — one carrying an
        admission notional, one predating the snapshot (NULL),
    When: the recent-submits projection runs,
    Then: the stored notional reads back as float and the legacy row
        projects None.
    """
    repo = await _fresh_repo(tmp_path, "lineage_recent.db")
    now = datetime.now(UTC)
    await repo.insert_trade_command(
        _lineage_row(
            now,
            mode="live",
            exchange="kraken",
            shard_key="kraken.BTC-USD.live",
            client_order_id="cid-a",
            correlation_id="corr-a",
        )
    )
    legacy = _lineage_row(
        now,
        mode="live",
        exchange="kraken",
        shard_key="kraken.BTC-USD.live",
        client_order_id="cid-b",
        correlation_id="corr-b",
        sequence_id=2,
    )
    del legacy["submitted_notional_usd"]
    await repo.insert_trade_command(legacy)
    rows = await repo.get_user_recent_submits("user-1", since=now - timedelta(hours=1))
    notionals = sorted(
        (r["submitted_notional_usd"] for r in rows),
        key=lambda v: (v is None, v),
    )
    assert notionals == [1283.63, None]


@pytest.mark.asyncio
async def test_update_status_with_stale_clock_keeps_single_active_row(tmp_path: Path) -> None:
    """A lagging caller clock cannot fork or invert the SCD2 chain.

    Given: an active command whose row timestamp is AHEAD of the
        caller's clock (writer/reader skew),
    When: ``update_trade_command_status`` runs with the stale bus time,
    Then: the CURRENT row still transitions (never a historical
        version), exactly one active row remains, and its timestamp is
        clamped to the row's own (later) bus time.
    """
    repo = await _fresh_repo(tmp_path, "stale_upd.db")
    now = datetime.now(UTC)
    _, cmd_pid = await repo.insert_trade_command(_lineage_row(now))
    stale = now - timedelta(seconds=30)
    new_id = await repo.update_trade_command_status(
        public_id=cmd_pid,
        new_status="dispatched",
        bus_time=stale,
        session_id="s1",
        sequence_id=2,
        dispatched_at=stale,
    )
    assert new_id is not None
    row = await repo.get_trade_command_by_public_id(cmd_pid, as_of=now + timedelta(seconds=1))
    assert row is not None
    assert row["status"] == "dispatched"
    assert row["timestamp"] == now


@pytest.mark.asyncio
async def test_advance_lifecycle_with_stale_clock_keeps_interval_monotone(
    tmp_path: Path,
) -> None:
    """The CAS advance clamps a lagging clock to the row's bus time.

    Given: an active created command stamped at T and a CAS advance
        whose bus time reads T-30s,
    When: ``advance_trade_command_lifecycle`` runs,
    Then: the transition applies and the successor's timestamp is
        clamped to T (no ``known_to < timestamp`` inversion).
    """
    repo = await _fresh_repo(tmp_path, "stale_adv.db")
    now = datetime.now(UTC)
    _, cmd_pid = await repo.insert_trade_command(_lineage_row(now))
    stale = now - timedelta(seconds=30)
    assert await repo.advance_trade_command_lifecycle(
        public_id=cmd_pid,
        expected_status="created",
        new_status="dispatched",
        bus_time=stale,
        session_id="s1",
        sequence_id=2,
    )
    row = await repo.get_trade_command_by_public_id(cmd_pid, as_of=now + timedelta(seconds=1))
    assert row is not None
    assert row["status"] == "dispatched"
    assert row["timestamp"] == now


@pytest.mark.asyncio
async def test_origin_and_window_carry_through_successor(tmp_path: Path) -> None:
    """Replay provenance survives SCD2 status transitions.

    Given: a replay-origin command with a persisted replay window,
    When: the status transitions created → dispatched,
    Then: the successor carries origin and both window stamps — the
        outbox rebuild would otherwise dispatch a guard-invisible
        payload.
    """
    repo = await _fresh_repo(tmp_path, "origin_carry.db")
    now = datetime.now(UTC)
    row = _lineage_row(now)
    row["origin"] = "replay"
    row["replay_window_start"] = now - timedelta(days=2)
    row["replay_window_end"] = now - timedelta(days=1)
    _, cmd_pid = await repo.insert_trade_command(row)
    t1 = now + timedelta(seconds=1)
    assert (
        await repo.update_trade_command_status(
            public_id=cmd_pid,
            new_status="dispatched",
            bus_time=t1,
            session_id="s1",
            sequence_id=2,
            dispatched_at=t1,
        )
        is not None
    )
    read = await repo.get_trade_command_by_public_id(cmd_pid, as_of=t1)
    assert read is not None
    assert read["origin"] == "replay"
    assert read["replay_window_start"] == now - timedelta(days=2)
    assert read["replay_window_end"] == now - timedelta(days=1)


@pytest.mark.asyncio
async def test_origin_defaults_to_live_when_omitted(tmp_path: Path) -> None:
    """Rows inserted without provenance keys default to live.

    Given: an insert row omitting origin and window keys (manual
        REST/MCP and plan paths never set them),
    When: the command is inserted and fetched,
    Then: origin reads ``live`` with NULL windows.
    """
    repo = await _fresh_repo(tmp_path, "origin_default.db")
    now = datetime.now(UTC)
    _, cmd_pid = await repo.insert_trade_command(_lineage_row(now))
    read = await repo.get_trade_command_by_public_id(cmd_pid, as_of=now)
    assert read is not None
    assert read["origin"] == "live"
    assert read["replay_window_start"] is None
    assert read["replay_window_end"] is None
