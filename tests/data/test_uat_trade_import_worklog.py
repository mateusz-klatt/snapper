"""Transactional worklog proof for the local-UAT trade importer."""

from collections.abc import Awaitable
from collections.abc import Callable
from pathlib import Path
from types import TracebackType
from typing import cast

import asyncpg
import pytest

type _LoadDump = Callable[[asyncpg.Connection, str, Path], Awaitable[int]]

_UAT_REFRESH_DB = pytest.importorskip(
    "proprietary.scripts.uat_refresh_db",
    reason="requires the materialized proprietary submodule",
    exc_type=ModuleNotFoundError,
)
_load_dump = cast(_LoadDump, getattr(_UAT_REFRESH_DB, "_load_dump"))


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
    """Trade COPY and monitor obligations share one explicit transaction.

    Given: A trade dump and a target containing the integrity worklog,
    When: The importer loads the trade dump,
    Then: The COPY and exact monitor obligations commit in one transaction.
    """
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


@pytest.mark.asyncio
async def test_uat_non_trade_import_keeps_direct_copy_path(tmp_path: Path) -> None:
    """Non-trade COPY keeps the existing direct load behavior.

    Given: A dump for a table that does not require integrity work,
    When: The importer loads that dump,
    Then: It truncates and copies without a catalog check or transaction.
    """
    dump_path = tmp_path / "ticks.dat"
    dump_path.write_bytes(b"tick rows")
    target = _Target()

    copied = await _load_dump(
        cast(asyncpg.Connection, target),
        "ticks",
        dump_path,
    )

    assert copied == 7
    assert target.events == [
        'execute:TRUNCATE TABLE "ticks"',
        "copy:ticks:ticks.dat",
    ]
    assert not dump_path.exists()
