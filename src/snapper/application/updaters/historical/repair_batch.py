"""Retry short repair transactions after rollback without holding locks during backoff."""

import asyncio
from collections.abc import Awaitable
from collections.abc import Callable

from sqlalchemy.exc import DBAPIError
from sqlalchemy.exc import IntegrityError
from sqlalchemy.exc import OperationalError


def retryable(error: DBAPIError) -> bool:
    """Recognize conflicts, connection loss and PostgreSQL transient transaction failures.

    Args:
        error: SQLAlchemy failure after the owned transaction has rolled back.

    Returns:
        Whether a fresh bounded transaction may safely retry the batch.
    """
    return (
        isinstance(error, (OperationalError, IntegrityError))
        or error.connection_invalidated
        or getattr(error.orig, "sqlstate", None) in {"55P03", "40001", "40P01", "57014"}
    )


async def retry_batch[T](operation: Callable[[], Awaitable[T]]) -> T:
    """Try one batch at most four times, backing off outside the transaction.

    Args:
        operation: One owned, idempotent insert-only or guarded transaction.

    Returns:
        The acknowledged result; exhausted or permanent failures propagate.
    """
    attempt = 0
    while True:
        try:
            return await operation()
        except DBAPIError as error:
            if attempt == 3 or not retryable(error):
                raise
            attempt += 1
            await asyncio.sleep(0.25 * attempt)
