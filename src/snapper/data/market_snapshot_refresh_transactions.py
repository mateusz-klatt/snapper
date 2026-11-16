"""Dedicated bounded fences for selected snapshot writes and uncertain settlement.

PostgreSQL server timeouts complement, but cannot prove the outcome of, a killed
client. The supervisor must retain its intent and classify uncertain writes as
non-success. Legacy snapshot writers do not use this advisory namespace. They
must remain inactive for activation unless separately brought under this fence.
The short table lock blocks legacy writes during persistence, but a legacy reader
may already hold row locks and cause a bounded lock/deadlock failure. This module
does not change any legacy writer's timeout or retry behavior.
"""

import asyncio
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from contextlib import asynccontextmanager
from typing import Protocol

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.core.market_snapshot_refresh_budget import RefreshBudget

REFRESH_DOMAIN = "market_snapshot_refresh_v1"
REFRESH_FENCE_NAMESPACE = "snapper.selected_market_snapshot_refresh"


class RefreshRepository(Protocol):
    """Minimal session provider, independent of application and transport layers."""

    @property
    def dialect_name(self) -> str:
        """Return the actual database dialect.

        Returns:
            The concrete database dialect name.
        """
        ...

    def session(self) -> AbstractAsyncContextManager[AsyncSession]:
        """Open a fresh transaction-capable async session.

        Returns:
            An asynchronous context manager for a fresh transaction-capable session.
        """
        ...


async def _postgres_fence(session: AsyncSession, budget: RefreshBudget) -> None:
    """Set transaction-local server bounds before acquiring the dedicated fence.

    Args:
        session: Caller-owned session already protected by the refresh transaction fence.
        budget: The existing absolute monotonic deadline, never renewed by this operation.
    """
    await session.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))
    timeouts = {
        "transaction_timeout": budget.timeout_ms(8.0),
        "statement_timeout": budget.timeout_ms(4.0),
        "lock_timeout": budget.timeout_ms(0.25),
        "idle_in_transaction_session_timeout": budget.timeout_ms(1.0),
    }
    for setting, milliseconds in timeouts.items():
        await session.execute(
            text("SELECT set_config(:setting, :milliseconds, true)"),
            {"setting": setting, "milliseconds": f"{milliseconds}ms"},
        )
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtext(:namespace), hashtext(:domain))"),
        {"namespace": REFRESH_FENCE_NAMESPACE, "domain": REFRESH_DOMAIN},
    )


@asynccontextmanager
async def refresh_transaction(
    repository: RefreshRepository, budget: RefreshBudget
) -> AsyncIterator[AsyncSession]:
    """Open one fenced transaction within the existing absolute job deadline.

    PostgreSQL must support transaction_timeout; older unsupported servers fail
    closed. SQLite's explicit immediate transaction serializes writers across
    independent engines. Await cancellation is not treated as a rollback receipt.

    Args:
        repository: Database dialect and fresh-session provider owned by the caller.
        budget: The existing absolute monotonic deadline, never renewed by this operation.

    Yields:
        The caller-owned fenced session, valid only within the inherited deadline.
    """
    async with asyncio.timeout(budget.remaining()):
        async with repository.session() as session:
            if repository.dialect_name == "postgresql":
                await _postgres_fence(session, budget)
            elif repository.dialect_name == "sqlite":
                milliseconds = budget.timeout_ms(0.25)
                await session.execute(text(f"PRAGMA busy_timeout={milliseconds}"))
                await session.execute(text("BEGIN IMMEDIATE"))
            else:
                raise ValueError("Snapshot refresh supports PostgreSQL and SQLite only")
            yield session


async def lock_refresh_metadata(session: AsyncSession, dialect_name: str) -> None:
    """Freeze the complete metadata closure, including newly inserted FX candidates.

    Args:
        session: Caller-owned session already protected by the refresh transaction fence.
        dialect_name: Actual database backend determining whether PostgreSQL table locks are needed.
    """
    if dialect_name == "postgresql":
        await session.execute(
            text(
                "LOCK TABLE instruments, symbols, symbol_exchange_capabilities, symbol_aliases IN SHARE MODE"
            )
        )


async def lock_refresh_snapshots(session: AsyncSession, dialect_name: str) -> None:
    """Block legacy DML for the brief atomic batch transaction, without retrying it.

    Args:
        session: Caller-owned session already protected by the refresh transaction fence.
        dialect_name: Actual database backend determining whether PostgreSQL table locks are needed.
    """
    if dialect_name == "postgresql":
        await session.execute(text("LOCK TABLE market_snapshots IN SHARE ROW EXCLUSIVE MODE"))
