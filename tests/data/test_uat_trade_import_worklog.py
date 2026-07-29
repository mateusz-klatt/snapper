"""Transactional worklog proof for the local-UAT trade importer."""

from pathlib import Path
from types import TracebackType
from typing import cast

import asyncpg
import pytest

from proprietary.scripts.uat_refresh_db import _load_dump


class _Transaction:
    """Record the transaction boundary around the fake UAT target."""

    def __init__(self, events: list[str]) -> None:
        """Store the shared event recorder."""
        self._events = events

    async def __aenter__(self) -> None:
        """Record transaction entry."""
        self._events.append("transaction:begin")

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        """Record whether the transaction committed or rolled back."""
        _ = (exc_value, traceback)
        outcome = "rollback" if exc_type is not None else "commit"
        self._events.append(f"transaction:{outcome}")
        return False


class _Target:
    """Capture catalog, COPY, and DML calls made by the importer."""

    def __init__(self) -> None:
        """Initialize the ordered event log."""
        self.events: list[str] = []

    async def fetchval(self, query: str, table: str) -> str:
        """Report the worklog table as present."""
        _ = query
        self.events.append(f"catalog:{table}")
        return table

    def transaction(self) -> _Transaction:
        """Return a recording transaction context."""
        return _Transaction(self.events)

    async def execute(self, query: str) -> str:
        """Record one normalized SQL statement."""
        normalized = " ".join(query.split())
        self.events.append(f"execute:{normalized}")
        return "OK"

    async def copy_to_table(self, table: str, *, source: str) -> str:
        """Record the COPY call and return its inserted-row status."""
        self.events.append(f"copy:{table}:{Path(source).name}")
        return "COPY 7"


@pytest.mark.asyncio
async def test_uat_trade_import_enqueues_inside_copy_transaction(
    tmp_path: Path,
) -> None:
    """Trade COPY and monitor obligations share one explicit transaction."""
    dump_path = tmp_path / "trades.dat"
    dump_path.write_bytes(b"trade rows")
    target = _Target()

    copied = await _load_dump(
        cast(asyncpg.Connection, target),
        "trades",
        dump_path,
    )

    assert copied == 7
    assert target.events == [
        "catalog:trade_integrity_worklog",
        "transaction:begin",
        'execute:TRUNCATE TABLE "trades"',
        "copy:trades:trades.dat",
        (
            "execute:INSERT INTO trade_integrity_worklog ( public_id, "
            "instrument_public_id, trade_id, executed_at, m1_pending, m2_pending ) "
            "SELECT public_id, instrument_public_id, trade_id, executed_at, "
            "trade_id IS NOT NULL, TRUE FROM trades"
        ),
        "transaction:commit",
    ]
    assert not dump_path.exists()
