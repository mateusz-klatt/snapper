"""Prove original SQLAlchemy context identity, autoflush behavior and restoration."""

from contextlib import AbstractContextManager
from typing import cast

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import Session
from sqlalchemy.orm import mapped_column

from snapper.data.session_context import session_no_autoflush


class _ProbeBase(DeclarativeBase):
    """Own only an isolated test table, independent of production schema or financial evidence."""


class _PendingRow(_ProbeBase):
    """Supply a genuine pending ORM insert whose query-triggered autoflush can be observed."""

    __tablename__ = "session_context_probe"

    id: Mapped[int] = mapped_column(primary_key=True)
    value: Mapped[str] = mapped_column()


class _ObservedAsyncSession(AsyncSession):
    """Record only the original property object returned by the real SQLAlchemy session."""

    def __init__(self, autoflush: bool) -> None:
        """Retain ordinary async-session behavior while allowing its context identity to be observed."""
        super().__init__(autoflush=autoflush)
        self.original_context: AbstractContextManager[Session] | None = None

    @property
    def no_autoflush(self) -> AbstractContextManager[Session]:
        """Forward the actual SQLAlchemy context without replacing its state or lifecycle.

        Returns:
            The original context received from the actual superclass property.
        """
        original = cast(AbstractContextManager[Session], super().no_autoflush)
        self.original_context = original
        return original


@pytest.mark.parametrize("initial_autoflush", (False, True))
@pytest.mark.parametrize("raise_error", (False, True))
async def test_original_context_identity_and_nested_autoflush_restoration(
    initial_autoflush: bool, raise_error: bool
) -> None:
    """Preserve original context identity and nested restoration on normal or exceptional exit.

    Given: A real async session starts with autoflush explicitly enabled or disabled.
    When: Its original returned context is entered twice and the inner body returns or raises.
    Then: The same synchronous session is yielded and the original setting and transaction state survive.

    Args:
        initial_autoflush: The actual session's autoflush setting before either context is entered.
        raise_error: Whether the nested body raises through both genuine context exit boundaries.
    """
    async with _ObservedAsyncSession(autoflush=initial_autoflush) as session:
        context = session_no_autoflush(session)
        assert context is session.original_context
        assert session.autoflush is initial_autoflush
        assert not session.in_transaction()

        def enter_original() -> None:
            """Exercise nested genuine contexts without inventing a wrapper or fake session."""
            with context as original:
                assert original is session.sync_session
                assert not session.autoflush
                with session_no_autoflush(session) as nested:
                    assert nested is original
                    assert not session.autoflush
                    if raise_error:
                        raise RuntimeError("controlled context exit")
                assert not session.autoflush

        if raise_error:
            with pytest.raises(RuntimeError, match="controlled context exit"):
                enter_original()
        else:
            enter_original()
        assert session.autoflush is initial_autoflush
        assert not session.in_transaction()


@pytest.mark.parametrize("explicit_flush", (False, True))
async def test_actual_queries_keep_pending_writes_unflushed_until_original_boundary(
    explicit_flush: bool,
) -> None:
    """Keep real pending inserts suppressed only for the original no-autoflush lifetime.

    Given: An actual isolated SQL session owns an uncommitted pending ORM insert.
    When: Queries run inside the typed context, with or without an explicit caller flush.
    Then: Only explicit flush writes inside, normal autoflush resumes outside, and rollback remains effective.

    Args:
        explicit_flush: Whether the caller deliberately flushes its pending insert inside the context.
    """
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    try:
        async with engine.begin() as connection:
            await connection.run_sync(_ProbeBase.metadata.create_all)
        async with AsyncSession(engine, expire_on_commit=False) as session:
            pending = _PendingRow(id=1, value="pending original insert")
            session.add(pending)
            transaction = session.get_transaction()
            assert transaction is not None
            with session_no_autoflush(session) as original:
                assert original is session.sync_session
                assert not session.autoflush
                assert list(await session.scalars(select(_PendingRow))) == []
                assert pending in session.new
                if explicit_flush:
                    await session.flush()
                    assert list(await session.scalars(select(_PendingRow))) == [pending]
                    assert pending not in session.new
            assert session.autoflush
            assert list(await session.scalars(select(_PendingRow))) == [pending]
            assert pending not in session.new
            assert session.get_transaction() is transaction
            await session.rollback()
        async with AsyncSession(engine) as observer:
            assert list(await observer.scalars(select(_PendingRow))) == []
    finally:
        await engine.dispose()
