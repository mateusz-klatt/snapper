"""Writer contracts required by daily market-data partitioning."""

from datetime import UTC
from datetime import datetime
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import UniqueConstraint

from snapper.data.models import Trade
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import TradeUpsertRow


def test_trade_model_uses_non_null_u3_identity() -> None:
    """Require the partition-safe trade identity in ORM metadata.

    Given: The trade model used by repository statements,
    When: Its nullability and unique constraints are inspected,
    Then: Executed time is required and U3 is represented exactly.
    """
    unique_indexes = {
        tuple(column.name for column in index.columns)
        for index in Trade.__table__.indexes
        if index.unique
    }
    unique_constraints = {
        tuple(column.name for column in constraint.columns)
        for constraint in Trade.__table__.constraints
        if isinstance(constraint, UniqueConstraint)
    }

    assert Trade.__table__.c.executed_at.nullable is False
    assert ("instrument_public_id", "trade_id", "executed_at") in unique_indexes
    assert ("instrument_public_id", "trade_id") not in unique_constraints


@pytest.mark.asyncio
async def test_trade_upsert_targets_partition_safe_u3() -> None:
    """Route duplicate suppression through the partition-safe identity.

    Given: One normalized trade and a repository batch-upsert boundary,
    When: The public trade upsert prepares its conflict contract,
    Then: It selects instrument, venue trade id, and executed time in order.
    """
    upsert_batch = AsyncMock(return_value=1)
    repository = cast(
        SQLAlchemyRepository,
        SimpleNamespace(_upsert_batch=upsert_batch),
    )
    occurred_at = datetime(2026, 7, 30, 12, 0, tzinfo=UTC)
    row = TradeUpsertRow(
        instrument_public_id="10000000-0000-0000-0000-000000000001",
        timestamp=occurred_at,
        price=100.0,
        size=1.0,
        side="buy",
        trade_id="venue-trade-1",
        executed_at=occurred_at,
        session_id="20000000-0000-0000-0000-000000000001",
        sequence_id=1,
    )

    inserted = await SQLAlchemyRepository.upsert_trades(repository, [row])

    assert inserted == 1
    upsert_batch.assert_awaited_once_with(
        Trade,
        [row],
        ["instrument_public_id", "trade_id", "executed_at"],
        session=None,
    )
