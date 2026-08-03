"""Transactional proofs for historical trade integrity work."""

from datetime import UTC
from datetime import datetime
from pathlib import Path

import pytest
from sqlalchemy import event
from sqlalchemy import text
from sqlalchemy.orm import Session

from snapper.data.archiver import ArchiveRestorer
from snapper.data.models import Base
from snapper.data.repository import DatabaseRepository
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import TradeUpsertRow


class _ForcedRestoreRollbackError(RuntimeError):
    """Raised by the test transaction immediately before commit."""


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
