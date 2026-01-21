"""Unit tests for Kraken market data service."""

from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.data.repository import DatabaseRepository
from snapper.infrastructure.exchanges.implementations.kraken import KrakenExchangeClient
from snapper.infrastructure.market_data.kraken import KrakenSnapshotUpdaterService
from snapper.infrastructure.market_data.kraken import _async_update_snapshots
from snapper.infrastructure.market_data.kraken import run_snapshot_update


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
        mock_settings.return_value.db_url = "sqlite:///test.db"
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
    settings_mock.db_url = "sqlite:///test.db"
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
    repo_cls.assert_called_once_with("sqlite:///test.db")
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
    settings_mock.db_url = "sqlite:///test.db"
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
