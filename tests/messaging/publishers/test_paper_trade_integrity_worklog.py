"""Transactional worklog routing for historical paper trades."""

from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.data.repository import Repository
from snapper.data.repository_types import TradeUpsertRow
from snapper.messaging.publishers.paper import PerSourcePaperPublisher


@pytest.mark.asyncio
async def test_paper_trade_replay_requests_transactional_worklog() -> None:
    """Paper replay marks every persisted trade batch for worklog enqueue.

    Given: A paper replay publisher holding one trade row to commit,
    When: The batch is committed through the trade-row persistence path,
    Then: The upsert is asked to enqueue integrity work, so replayed trades
        enter the same integrity worklog as live ones instead of bypassing it.
    """
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


@pytest.mark.asyncio
async def test_paper_trade_writer_session_requests_worklog_and_commits() -> None:
    """Paper writer sessions commit trades with integrity work requested.

    Given: A paper replay publisher with a pinned writer session,
    When: It commits one historical trade row,
    Then: The repository receives that session and the worklog flag before commit.
    """
    upsert_trades = AsyncMock(return_value=1)
    commit = AsyncMock()
    writer_session = cast(
        AsyncSession,
        SimpleNamespace(commit=commit),
    )
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
    publisher._trade_writer_session = writer_session
    row = TradeUpsertRow(
        public_id="4c000000-0000-0000-0000-000000000001",
        instrument_public_id="4d000000-0000-0000-0000-000000000001",
        timestamp=publisher._candle_live_epoch(),
        executed_at=publisher._candle_live_epoch(),
        price=100.0,
        size=1.0,
        side="buy",
        trade_id="paper-replay-writer-1",
        session_id="4e000000-0000-0000-0000-000000000001",
        sequence_id=1,
    )

    await publisher._commit_trade_rows([row])

    upsert_trades.assert_awaited_once_with(
        [row],
        session=writer_session,
        enqueue_integrity_work=True,
    )
    commit.assert_awaited_once_with()
