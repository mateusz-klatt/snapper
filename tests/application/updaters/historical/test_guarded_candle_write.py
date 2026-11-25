"""Check shared reconciliation for different timeframes on the same instrument."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.updaters.historical.guarded_candle_write import candle_key
from snapper.application.updaters.historical.guarded_candle_write import reconcile_versions
from snapper.data.repository import Repository
from snapper.data.repository_types import CandleUpsertRow
from snapper.data.repository_types import CandleVersionRow
from tests.helpers.candle_repair import OPEN
from tests.helpers.candle_repair import RUN_TIME
from tests.helpers.candle_repair import XRP


def _candidate(timeframe: str, sequence_id: int) -> CandleUpsertRow:
    """Build a complete candidate for the same instrument and opening time."""
    return {
        "instrument_public_id": XRP,
        "timeframe": timeframe,
        "open_at": OPEN,
        "timestamp": RUN_TIME,
        "session_id": "run",
        "sequence_id": sequence_id,
        "open": 10.0,
        "high": 11.0,
        "low": 9.0,
        "close": 10.5,
        "volume": 4.5,
        "vwap": 10.25,
        "trades": 2,
        "source": "synthesized",
        "complete": True,
        "price_basis": None,
    }


@pytest.mark.asyncio
async def test_reconciliation_keeps_timeframes_distinct_and_can_require_extra_fields() -> None:
    """Accept matched payloads and keep mismatched time or price basis uncertain.

    Given: Identical opening times at different timeframes and mismatched extra fields.
    When: Reconcile the submitted rows against versions from the run.
    Then: Each timeframe is reconciled separately and extra-field mismatches stay uncertain.
    """
    first, second, time_changed, price_changed = (
        _candidate(timeframe, sequence_id)
        for sequence_id, timeframe in enumerate(("5m", "15m", "30m", "1h"), start=1)
    )
    rows = {candle_key(row): row for row in (first, second, time_changed, price_changed)}
    found = [
        (cast(CandleVersionRow, first | {"id": 1}), True),
        (cast(CandleVersionRow, second | {"id": 2}), False),
        (cast(CandleVersionRow, time_changed | {"id": 3, "timestamp": OPEN}), True),
        (cast(CandleVersionRow, price_changed | {"id": 4, "price_basis": "mid"}), True),
    ]
    session = MagicMock(spec=AsyncSession)
    session.execute = AsyncMock()

    @asynccontextmanager
    async def read_session() -> AsyncIterator[AsyncSession]:
        """Expose one caller-owned read session."""
        yield session

    repository = MagicMock(spec=Repository, dialect_name="sqlite")
    repository.session.side_effect = read_session
    repository.get_session_candle_versions = AsyncMock(return_value=found)

    outcomes = await reconcile_versions(
        repository, rows, "run", extra_fields=("timestamp", "price_basis")
    )

    assert outcomes == {
        candle_key(first): "applied",
        candle_key(second): "applied_superseded",
        candle_key(time_changed): "uncertain",
        candle_key(price_changed): "uncertain",
    }
    repository.get_session_candle_versions.assert_awaited_once_with(session, list(rows), "run")
    session.execute.assert_not_called()
