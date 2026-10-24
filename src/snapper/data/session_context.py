"""Explicit types for SQLAlchemy's unchanged synchronous session contexts."""

from contextlib import AbstractContextManager
from typing import cast

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session


def session_no_autoflush(session: AsyncSession) -> AbstractContextManager[Session]:
    """Expose the original SQLAlchemy context without wrapping or entering it.

    The async-session proxy declares this synchronous property with an
    unconstrained return type. Its actual context yields the underlying
    synchronous session and restores the previous autoflush setting on exit.

    Args:
        session: Actual caller-owned async session whose existing context is required.

    Returns:
        The identical SQLAlchemy context object, with its existing entry and exit behavior.
    """
    return cast(AbstractContextManager[Session], session.no_autoflush)
