"""Transactional worklog routing for historical paper trades."""

from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock

import pytest

from snapper.data.repository import Repository
from snapper.data.repository_types import TradeUpsertRow
from snapper.messaging.publishers.paper import PerSourcePaperPublisher


@pytest.mark.asyncio
async def test_paper_trade_replay_requests_transactional_worklog() -> None:
    """Paper replay marks every persisted trade batch for worklog enqueue."""
    upsert_trades = AsyncMock(return_value=1)
    publisher = PerSourcePaperPublisher(
        source_exchange="kraken",
        symbols=["BTC/USD"],
        start_time=1_700_000_000.0,
        end_time=1_700_003_600.0,
    )
    publisher.repository = cast(
        Repository,
        SimpleNamespace(upsert_trades=upsert_trades),
    )
    row = TradeUpsertRow(
        public_id="46000000-0000-0000-0000-000000000001",
        instrument_public_id="47000000-0000-0000-0000-000000000001",
        timestamp=publisher._candle_live_epoch(),
        executed_at=publisher._candle_live_epoch(),
        price=100.0,
        size=1.0,
        side="buy",
        trade_id="paper-replay-1",
        session_id="48000000-0000-0000-0000-000000000001",
        sequence_id=1,
    )

    await publisher._commit_trade_rows([row])

    upsert_trades.assert_awaited_once_with(
        [row],
        enqueue_integrity_work=True,
    )
