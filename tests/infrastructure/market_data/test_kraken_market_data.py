"""Unit tests for Kraken market data service."""

import asyncio
from collections.abc import AsyncIterator
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.data.repository import DatabaseRepository
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.implementations.kraken import KrakenExchangeClient
from snapper.infrastructure.market_data.kraken import KrakenSnapshotUpdaterService
from snapper.infrastructure.market_data.kraken import _async_update_snapshots
from snapper.infrastructure.market_data.kraken import run_snapshot_update

TEST_DB_URL = "sqlite:///:memory:"


def test_service_initialization() -> None:
    """Test KrakenSnapshotUpdaterService initialization.

    Given: mock exchange client and repository,
    When: creating KrakenSnapshotUpdaterService,
    Then: client and repository are bound to service attributes.
    """
    mock_exchange = MagicMock(spec=KrakenExchangeClient)
    mock_repo = MagicMock(spec=DatabaseRepository)
    service = KrakenSnapshotUpdaterService(exchange_client=mock_exchange, repository=mock_repo)
    assert service.exchange_client == mock_exchange
    assert service.repository == mock_repo


def test_run_snapshot_update_integration() -> None:
    """Test run_snapshot_update integration flow.

    Given: mocked client, repository, settings, and async handler,
    When: calling run_snapshot_update,
    Then: _async_update_snapshots is invoked once.
    """
    with (
        patch(
            "snapper.infrastructure.market_data.kraken.KrakenExchangeClient"
        ) as mock_client_class,
        patch("snapper.infrastructure.market_data.kraken.DatabaseRepository") as mock_repo_class,
        patch("snapper.infrastructure.market_data.kraken.get_settings") as mock_settings,
        patch("snapper.infrastructure.market_data.kraken._async_update_snapshots") as mock_async,
    ):
        mock_settings.return_value = MagicMock()
        mock_settings.return_value.db_url = TEST_DB_URL
        mock_client = AsyncMock()
        mock_client_class.return_value = mock_client
        mock_repo = MagicMock()
        mock_repo_class.return_value = mock_repo
        mock_async.return_value = None
        run_snapshot_update()
        mock_async.assert_called_once()


def test_run_snapshot_update_handles_exceptions() -> None:
    """Test run_snapshot_update exception propagation.

    Given: _async_update_snapshots raises exception,
    When: calling run_snapshot_update,
    Then: exception is propagated and handler was called.
    """
    with (
        patch("snapper.infrastructure.market_data.kraken._async_update_snapshots") as mock_async,
    ):
        mock_async.side_effect = Exception("WebSocket connection failed")
        exception_raised = False
        try:
            run_snapshot_update()
        except Exception as e:
            exception_raised = True
            assert "WebSocket connection failed" in str(e)
        assert exception_raised, "Expected exception was not raised"
        mock_async.assert_called_once()


@pytest.mark.asyncio
async def test_async_update_snapshots_success_flow() -> None:
    """Test _async_update_snapshots success flow.

    Given: mocked settings, repository, client, and service,
    When: calling _async_update_snapshots,
    Then: service.start is awaited and client.disconnect called.
    """
    settings_mock = MagicMock()
    settings_mock.db_url = TEST_DB_URL
    repository_mock = MagicMock()
    exchange_client_mock = MagicMock()
    exchange_client_mock.disconnect = AsyncMock()
    service_instance = MagicMock()
    service_instance.start = AsyncMock()
    with (
        patch(
            "snapper.infrastructure.market_data.kraken.get_settings",
            return_value=settings_mock,
        ),
        patch(
            "snapper.infrastructure.market_data.kraken.DatabaseRepository",
            return_value=repository_mock,
        ) as repo_cls,
        patch(
            "snapper.infrastructure.market_data.kraken.KrakenExchangeClient",
            return_value=exchange_client_mock,
        ) as client_cls,
        patch(
            "snapper.infrastructure.market_data.kraken.KrakenSnapshotUpdaterService",
            return_value=service_instance,
        ) as service_cls,
    ):
        await _async_update_snapshots()
    repo_cls.assert_called_once_with(TEST_DB_URL)
    client_cls.assert_called_once_with()
    service_cls.assert_called_once_with(exchange_client_mock, repository_mock)
    service_instance.start.assert_awaited_once()
    exchange_client_mock.disconnect.assert_awaited_once()


@pytest.mark.asyncio
async def test_async_update_snapshots_disconnects_on_failure() -> None:
    """Test _async_update_snapshots disconnects on failure.

    Given: mocked service.start raises RuntimeError,
    When: calling _async_update_snapshots,
    Then: client.disconnect called even on failure (finally block).
    """
    settings_mock = MagicMock()
    settings_mock.db_url = TEST_DB_URL
    repository_mock = MagicMock()
    exchange_client_mock = MagicMock()
    exchange_client_mock.disconnect = AsyncMock()
    service_instance = MagicMock()
    service_instance.start = AsyncMock(side_effect=RuntimeError("boom"))
    with (
        patch(
            "snapper.infrastructure.market_data.kraken.get_settings",
            return_value=settings_mock,
        ),
        patch(
            "snapper.infrastructure.market_data.kraken.DatabaseRepository",
            return_value=repository_mock,
        ),
        patch(
            "snapper.infrastructure.market_data.kraken.KrakenExchangeClient",
            return_value=exchange_client_mock,
        ),
        patch(
            "snapper.infrastructure.market_data.kraken.KrakenSnapshotUpdaterService",
            return_value=service_instance,
        ),
        pytest.raises(RuntimeError, match="boom"),
    ):
        await _async_update_snapshots()
    exchange_client_mock.disconnect.assert_awaited_once()


@pytest.mark.asyncio
async def test_collect_ticker_snapshots_timeout() -> None:
    """Snapshot collection returns partial results on timeout.

    Given a slow ticker stream that never reaches the 2000 target,
    When _collect_ticker_snapshots times out,
    Then it returns the snapshots collected so far and disconnects.
    """
    mock_exchange = MagicMock(spec=KrakenExchangeClient)
    mock_exchange.disconnect_websocket = AsyncMock()

    async def _slow_ticker_stream(symbols: list[str]) -> AsyncIterator[TickerUpdate]:
        """Yield one ticker then stall to trigger timeout."""
        yield TickerUpdate(
            symbol="BTC-USD",
            bid=50000.0,
            bid_qty=1.0,
            ask=50010.0,
            ask_qty=1.5,
            last=50005.0,
            volume=100.0,
            vwap=50002.0,
            low=49900.0,
            high=50100.0,
            change=100.0,
            change_pct=0.2,
        )
        await asyncio.sleep(300)

    mock_exchange.subscribe_ticks = _slow_ticker_stream
    mock_repo = MagicMock(spec=DatabaseRepository)
    service = KrakenSnapshotUpdaterService(mock_exchange, mock_repo)
    service._COLLECTION_TIMEOUT_SECONDS = 0.1
    snapshots, count = await service._collect_ticker_snapshots()
    assert count == 1
    assert len(snapshots) == 1
    assert snapshots[0].symbol == "BTC-USD"
    mock_exchange.disconnect_websocket.assert_awaited_once()


@pytest.mark.asyncio
async def test_collect_ticker_snapshots_stamps_provenance() -> None:
    """Snapshot collection stamps session_id and sequence_id on each snapshot.

    Given a ticker stream yielding two distinct symbols,
    When _collect_ticker_snapshots completes,
    Then each snapshot has a non-empty session_id and monotonically increasing sequence_id.
    """
    mock_exchange = MagicMock(spec=KrakenExchangeClient)
    mock_exchange.disconnect_websocket = AsyncMock()

    async def _two_tickers(symbols: list[str]) -> AsyncIterator[TickerUpdate]:
        """Yield two tickers for provenance verification."""
        for sym in ("BTC-USD", "ETH-USD"):
            yield TickerUpdate(
                symbol=sym,
                bid=1000.0,
                bid_qty=1.0,
                ask=1001.0,
                ask_qty=1.0,
                last=1000.5,
                volume=50.0,
                vwap=1000.2,
                low=999.0,
                high=1002.0,
                change=1.0,
                change_pct=0.1,
            )

    mock_exchange.subscribe_ticks = _two_tickers
    mock_repo = MagicMock(spec=DatabaseRepository)
    service = KrakenSnapshotUpdaterService(mock_exchange, mock_repo)
    snapshots, count = await service._collect_ticker_snapshots()
    assert count == 2
    assert len(snapshots) == 2
    session_ids = {s.session_id for s in snapshots}
    assert len(session_ids) == 1
    assert "" not in session_ids
    sequence_ids = sorted(s.sequence_id for s in snapshots)
    assert sequence_ids == [1, 2]
