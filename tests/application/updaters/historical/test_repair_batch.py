"""Verify bounded repair retry policy without external databases or real waits."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.exc import DBAPIError
from sqlalchemy.exc import IntegrityError
from sqlalchemy.exc import OperationalError

from snapper.application.updaters.historical import repair_batch


@pytest.mark.parametrize(
    "failure",
    [
        OperationalError("write", None, Exception("locked")),
        IntegrityError("write", None, Exception("unique")),
        DBAPIError("write", None, Exception("disconnect"), connection_invalidated=True),
        DBAPIError("write", None, SimpleNamespace(sqlstate="55P03")),
        DBAPIError("write", None, SimpleNamespace(sqlstate="40001")),
        DBAPIError("write", None, SimpleNamespace(sqlstate="40P01")),
        DBAPIError("write", None, SimpleNamespace(sqlstate="57014")),
    ],
)
async def test_transient_failures_retry_in_fresh_transactions(
    failure: DBAPIError,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retry only after an owned operation has raised and rolled back.

    Given three transient transaction failures followed by an acknowledgement,
    When a batch is retried,
    Then exactly four calls and bounded increasing backoffs occur.
    """
    operation = AsyncMock(side_effect=[failure, failure, failure, 7])
    sleep = AsyncMock()
    monkeypatch.setattr(repair_batch.asyncio, "sleep", sleep)
    assert await repair_batch.retry_batch(operation) == 7
    assert operation.await_count == 4
    assert [call.args[0] for call in sleep.await_args_list] == [0.25, 0.5, 0.75]


@pytest.mark.parametrize("transient", [False, True])
async def test_permanent_or_exhausted_failures_propagate(
    transient: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep unsuccessful transactions out of acknowledged progress.

    Given a permanent failure or an unending lock timeout,
    When retry policy is applied,
    Then it raises after one or four attempts without reporting a successful batch.
    """
    failure = (
        OperationalError("write", None, Exception("locked"))
        if transient
        else DBAPIError("write", None, Exception("bad schema"))
    )
    operation = AsyncMock(side_effect=failure)
    sleep = AsyncMock()
    monkeypatch.setattr(repair_batch.asyncio, "sleep", sleep)
    with pytest.raises(DBAPIError):
        await repair_batch.retry_batch(operation)
    assert operation.await_count == (4 if transient else 1)
    assert sleep.await_count == (3 if transient else 0)
