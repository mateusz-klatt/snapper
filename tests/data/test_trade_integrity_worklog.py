"""Transactional proofs for historical trade integrity work."""

from datetime import UTC
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import uuid7

import pytest
from sqlalchemy import event
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from snapper.data.archiver import ArchiveRestorer
from snapper.data.models import Base
from snapper.data.models import Trade
from snapper.data.models import TradeIntegrityWorkItem
from snapper.data.repository import DatabaseRepository
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import TradeUpsertRow


class _ForcedRestoreRollbackError(RuntimeError):
    """Raised by the test transaction immediately before commit."""


def _trade_row(trade_id: str) -> TradeUpsertRow:
    """Build a candidate for a fixed instrument and execution time."""
    return TradeUpsertRow(
        public_id=str(uuid7()),
        instrument_public_id="44000000-0000-0000-0000-000000000001",
        timestamp=datetime(2026, 7, 29, 10, 0, tzinfo=UTC),
        executed_at=datetime(2026, 7, 28, 10, 0, tzinfo=UTC),
        price=100.0,
        size=1.0,
        side="buy",
        trade_id=trade_id,
        session_id="45000000-0000-0000-0000-000000000001",
        sequence_id=1,
    )


def _write_trade_archive(path: Path) -> None:
    """Write one valid trade archive row.

    Args:
        path: CSV path to create.
    """
    path.write_text(
        "public_id,timestamp,known_to,session_id,sequence_id,"
        "instrument_public_id,price,size,side,trade_id,executed_at\n"
        "40000000-0000-0000-0000-000000000001,"
        "2026-07-29T10:00:00+00:00,"
        "9999-12-31T23:59:59+00:00,"
        "41000000-0000-0000-0000-000000000001,"
        "1,"
        "42000000-0000-0000-0000-000000000001,"
        "100.0,1.0,buy,restore-trade-1,"
        "2026-07-29T09:59:59+00:00\n",
        encoding="utf-8",
    )


def test_rolled_back_restore_leaves_no_worklog_entry(tmp_path: Path) -> None:
    """Restore row and work obligation roll back as one transaction."""
    repository = DatabaseRepository(f"sqlite:///{tmp_path / 'restore-rollback.db'}")
    Base.metadata.create_all(repository.engine)
    with repository.engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE IF NOT EXISTS trade_integrity_worklog ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                "public_id VARCHAR(36) NOT NULL,"
                "instrument_public_id VARCHAR(36) NOT NULL,"
                "trade_id VARCHAR(64),"
                "executed_at DATETIME,"
                "enqueued_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,"
                "m1_pending BOOLEAN NOT NULL DEFAULT 1,"
                "m2_pending BOOLEAN NOT NULL DEFAULT 1"
                ")"
            )
        )
    csv_path = tmp_path / "trades.csv"
    _write_trade_archive(csv_path)
    observed_worklog_counts: list[int] = []

    def force_rollback(session: Session) -> None:
        count = int(
            session.execute(text("SELECT count(*) FROM trade_integrity_worklog")).scalar_one()
        )
        observed_worklog_counts.append(count)
        raise _ForcedRestoreRollbackError("rollback after both inserts")

    event.listen(repository.session_factory, "before_commit", force_rollback)
    try:
        s5778_value_1 = ArchiveRestorer(repository)
        with pytest.raises(_ForcedRestoreRollbackError, match="after both inserts"):
            s5778_value_1.restore(table="trades", paths=[csv_path])
    finally:
        event.remove(repository.session_factory, "before_commit", force_rollback)

    with repository.get_session() as session:
        trade_count = int(session.execute(text("SELECT count(*) FROM trades")).scalar_one())
        worklog_count = int(
            session.execute(text("SELECT count(*) FROM trade_integrity_worklog")).scalar_one()
        )

    assert observed_worklog_counts == [1]
    assert trade_count == 0
    assert worklog_count == 0
    repository.dispose()


@pytest.mark.asyncio
async def test_rolled_back_replay_upsert_leaves_no_worklog_entry(
    tmp_path: Path,
) -> None:
    """Replay trade and work obligation roll back as one transaction.

    Given: A replay trade upsert using a caller-owned session,
    When: The caller rolls back after both rows become visible,
    Then: Neither the trade nor its integrity work remains durable.
    """
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'replay-rollback.db'}")
    async with repository.engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    row = TradeUpsertRow(
        public_id="43000000-0000-0000-0000-000000000001",
        instrument_public_id="44000000-0000-0000-0000-000000000001",
        timestamp=datetime(2026, 7, 29, 10, 0, tzinfo=UTC),
        executed_at=datetime(2026, 7, 28, 10, 0, tzinfo=UTC),
        price=100.0,
        size=1.0,
        side="buy",
        trade_id="replay-trade-1",
        session_id="45000000-0000-0000-0000-000000000001",
        sequence_id=1,
    )

    async with repository.session() as session:
        inserted = await repository.upsert_trades(
            [row],
            session=session,
            enqueue_integrity_work=True,
        )
        visible_trade_count = int(
            (await session.execute(text("SELECT count(*) FROM trades"))).scalar_one()
        )
        visible_worklog_count = int(
            (
                await session.execute(text("SELECT count(*) FROM trade_integrity_worklog"))
            ).scalar_one()
        )
        await session.rollback()

    async with repository.session() as session:
        durable_trade_count = int(
            (await session.execute(text("SELECT count(*) FROM trades"))).scalar_one()
        )
        durable_worklog_count = int(
            (
                await session.execute(text("SELECT count(*) FROM trade_integrity_worklog"))
            ).scalar_one()
        )

    assert inserted == 1
    assert visible_trade_count == 1
    assert visible_worklog_count == 1
    assert durable_trade_count == 0
    assert durable_worklog_count == 0
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_replay_upsert_owned_session_commits_trade_and_worklog(
    tmp_path: Path,
) -> None:
    """Owned replay upsert commits the trade and its work obligation.

    Given: A historical trade submitted without a caller-owned session,
    When: The repository upserts it with integrity work requested,
    Then: The returned count and durable rows prove both inserts committed.
    """
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'replay-commit.db'}")
    async with repository.engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    row = TradeUpsertRow(
        public_id="49000000-0000-0000-0000-000000000001",
        instrument_public_id="4a000000-0000-0000-0000-000000000001",
        timestamp=datetime(2026, 7, 29, 10, 0, tzinfo=UTC),
        executed_at=datetime(2026, 7, 28, 10, 0, tzinfo=UTC),
        price=100.0,
        size=1.0,
        side="buy",
        trade_id="replay-trade-commit",
        session_id="4b000000-0000-0000-0000-000000000001",
        sequence_id=1,
    )

    inserted = await repository.upsert_trades(
        [row],
        enqueue_integrity_work=True,
    )

    async with repository.session() as session:
        trade_count = int((await session.execute(text("SELECT count(*) FROM trades"))).scalar_one())
        work_item = (
            await session.execute(
                text("SELECT public_id, m1_pending, m2_pending FROM trade_integrity_worklog")
            )
        ).one()

    assert inserted == 1
    assert trade_count == 1
    assert tuple(work_item) == (row["public_id"], 1, 1)
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_replay_upsert_enqueues_only_inserted_trades(tmp_path: Path) -> None:
    """Use the identities returned by a real SQLite insert.

    Given: A stored trade and a batch with that identity and two new trades.
    When: Replay requests integrity work and then resubmits the batch.
    Then: Only the two new trade rows get one exact M1 and M2 obligation each.
    """
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'replay-conflict.db'}")
    await repository.create_all()
    stored = _trade_row("stored")
    conflict = _trade_row("stored")
    first = _trade_row("first")
    second = _trade_row("second")
    del second["public_id"]
    batch = [conflict, first, second]

    try:
        assert await repository.upsert_trades([stored]) == 1
        assert await repository.upsert_trades(batch, enqueue_integrity_work=True) == 2
        assert await repository.upsert_trades(batch, enqueue_integrity_work=True) == 0

        async with repository.session() as session:
            work_items = (await session.scalars(select(TradeIntegrityWorkItem))).all()
            trades = (await session.scalars(select(Trade))).all()

        assert {trade.public_id for trade in trades} == {
            stored["public_id"],
            first["public_id"],
            second["public_id"],
        }
        assert len(work_items) == 2
        assert {
            (item.public_id, item.instrument_public_id, item.trade_id, item.executed_at)
            for item in work_items
        } == {
            (row["public_id"], row["instrument_public_id"], row["trade_id"], row["executed_at"])
            for row in (first, second)
        }
        assert all(item.m1_pending and item.m2_pending for item in work_items)
    finally:
        await repository.engine.dispose()


@pytest.mark.asyncio
async def test_postgresql_trade_insert_returns_worklog_identities(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Check PostgreSQL statement and result handling without a server.

    Given: A PostgreSQL session double returns the identity of one inserted trade.
    When: Replay requests integrity work using its caller-owned session.
    Then: The statement returns identities and work represents only the returned row.
    """
    repository = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    monkeypatch.setattr(SQLAlchemyRepository, "dialect_name", "postgresql")
    session = AsyncMock(spec=AsyncSession)
    row = _trade_row("attempted")
    returned = (row["public_id"], row["instrument_public_id"], None, row["executed_at"])
    session.execute.side_effect = [[returned], None]

    try:
        count = await repository.upsert_trades([row], session=session, enqueue_integrity_work=True)

        assert count == 1
        trade_call, worklog_call = session.execute.await_args_list
        sql = str(trade_call.args[0].compile(dialect=postgresql.dialect()))
        assert "ON CONFLICT (instrument_public_id, trade_id, executed_at) DO NOTHING" in sql
        assert (
            "RETURNING trades.public_id, trades.instrument_public_id,"
            " trades.trade_id, trades.executed_at" in sql
        )
        assert worklog_call.args[1] == [
            {
                "public_id": returned[0],
                "instrument_public_id": returned[1],
                "trade_id": None,
                "executed_at": returned[3],
                "m1_pending": False,
                "m2_pending": True,
            }
        ]
        session.commit.assert_not_awaited()
    finally:
        await repository.engine.dispose()


@pytest.mark.asyncio
async def test_replay_upsert_refuses_an_unsupported_returning_dialect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Require a reviewed insert path for historical trade obligations.

    Given: The repository dialect does not support the native conflict insert.
    When: Replay requests transactional integrity work.
    Then: The repository raises before executing a trade or worklog statement.
    """
    repository = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    monkeypatch.setattr(SQLAlchemyRepository, "dialect_name", "unsupported")
    session = AsyncMock(spec=AsyncSession)

    try:
        with pytest.raises(NotImplementedError, match="requires PostgreSQL or SQLite"):
            await repository.upsert_trades(
                [_trade_row("trade")], session=session, enqueue_integrity_work=True
            )

        session.execute.assert_not_awaited()
    finally:
        await repository.engine.dispose()
