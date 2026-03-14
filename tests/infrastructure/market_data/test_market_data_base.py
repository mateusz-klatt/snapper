"""Unit tests for base market data service."""

from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from typing import Any
from unittest.mock import MagicMock

import pytest

from snapper.data.models import MarketSnapshot
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.market_data.base import MarketSnapshotUpdaterService
from snapper.infrastructure.market_data.kraken import KrakenSnapshotUpdaterService


class TestMarketSnapshotServiceCoverage:
    """Tests for MarketSnapshotUpdaterService base functionality."""

    @pytest.fixture
    def mock_repository(self) -> MagicMock:
        """Create mock repository for testing."""
        """Create mock repository for testing."""
        return MagicMock()

    @pytest.fixture
    def service(self, mock_repository: MagicMock) -> KrakenSnapshotUpdaterService:
        """Create KrakenSnapshotUpdaterService with mocked dependencies."""
        mock_exchange = MagicMock()
        mock_exchange.disconnect_websocket = MagicMock(return_value=None)
        mock_exchange.disconnect_websocket.__name__ = "disconnect_websocket"

        async def async_disconnect() -> None:
            """Intentionally empty async stub for testing."""
            pass

        mock_exchange.disconnect_websocket.side_effect = async_disconnect
        return KrakenSnapshotUpdaterService(mock_exchange, mock_repository)

    @pytest.fixture
    def sample_ticker_data(self) -> TickerUpdate:
        """Create sample ticker data for testing."""
        return TickerUpdate(
            symbol="BTC-USD",
            bid=50000.0,
            bid_qty=1.5,
            ask=50100.0,
            ask_qty=2.0,
            last=50050.0,
            volume=1234.56,
            vwap=50025.0,
            low=49900.0,
            high=50200.0,
            change=150.0,
            change_pct=0.3,
        )

    async def test_update_market_snapshots_processes_ticker_data(
        self,
        service: KrakenSnapshotUpdaterService,
        mock_repository: MagicMock,
        sample_ticker_data: TickerUpdate,
    ) -> None:
        """Test update_market_snapshots processes ticker data.

        Given: service with mocked repository and valid ticker,
        When: calling update_market_snapshots,
        Then: snapshot saved with correct symbol, prices, and spread.
        """
        mock_session = MagicMock()
        mock_repository.session_factory.return_value.__enter__.return_value = mock_session

        async def mock_subscribe_ticks(
            symbols: list[str], *, req_id: int | None = None
        ) -> AsyncIterator[TickerUpdate]:
            yield sample_ticker_data

        service.exchange_client.subscribe_ticks = mock_subscribe_ticks
        count = await service.update_market_snapshots()
        assert count == 1
        mock_session.bulk_save_objects.assert_called_once()
        mock_session.commit.assert_called_once()
        saved_snapshots = mock_session.bulk_save_objects.call_args[0][0]
        assert len(saved_snapshots) == 1
        snapshot = saved_snapshots[0]
        assert isinstance(snapshot, MarketSnapshot)
        assert snapshot.symbol == "BTC-USD"
        assert snapshot.bid == pytest.approx(50000.0)
        assert snapshot.ask == pytest.approx(50100.0)
        assert snapshot.spread == pytest.approx(100.0)
        assert snapshot.spread_pct is not None
        assert abs(snapshot.spread_pct - 0.1998) < 0.01

    async def test_update_market_snapshots_skips_unknown_symbols(
        self,
        service: KrakenSnapshotUpdaterService,
        mock_repository: MagicMock,
    ) -> None:
        """Test update_market_snapshots skips unknown symbols.

        Given: service with ticker having empty symbol,
        When: calling update_market_snapshots,
        Then: count is 0 and no bulk_save_objects call.
        """
        mock_session = MagicMock()
        mock_repository.session_factory.return_value.__enter__.return_value = mock_session
        ticker_data = TickerUpdate(
            symbol="",
            bid=100.0,
            bid_qty=1.0,
            ask=101.0,
            ask_qty=1.0,
            last=100.5,
            volume=10.0,
            vwap=100.25,
            low=99.0,
            high=102.0,
            change=1.0,
            change_pct=1.0,
        )

        async def mock_subscribe_ticks(
            symbols: list[str], *, req_id: int | None = None
        ) -> AsyncIterator[TickerUpdate]:
            yield ticker_data

        service.exchange_client.subscribe_ticks = mock_subscribe_ticks
        count = await service.update_market_snapshots()
        assert count == 0
        mock_session.bulk_save_objects.assert_not_called()

    async def test_update_market_snapshots_skips_empty_native_symbol(
        self,
        service: KrakenSnapshotUpdaterService,
        mock_repository: MagicMock,
    ) -> None:
        """Test update_market_snapshots skips empty native symbol.

        Given: service with ticker returning empty native symbol,
        When: calling update_market_snapshots,
        Then: count is 0 and no bulk_save_objects call.
        """
        mock_session = MagicMock()
        mock_repository.session_factory.return_value.__enter__.return_value = mock_session
        ticker_data = TickerUpdate(
            symbol="",
            bid=100.0,
            bid_qty=1.0,
            ask=101.0,
            ask_qty=1.0,
            last=100.5,
            volume=10.0,
            vwap=100.25,
            low=99.0,
            high=102.0,
            change=1.0,
            change_pct=1.0,
        )

        async def mock_subscribe_ticks(
            symbols: list[str], *, req_id: int | None = None
        ) -> AsyncIterator[TickerUpdate]:
            yield ticker_data

        service.exchange_client.subscribe_ticks = mock_subscribe_ticks
        count = await service.update_market_snapshots()
        assert count == 0
        mock_session.bulk_save_objects.assert_not_called()

    async def test_update_market_snapshots_deduplicates_symbols(
        self,
        service: KrakenSnapshotUpdaterService,
        mock_repository: MagicMock,
    ) -> None:
        """Test update_market_snapshots deduplicates symbols.

        Given: service receiving 3 tickers with same symbol,
        When: calling update_market_snapshots,
        Then: only last ticker saved (1 snapshot, latest bid).
        """
        mock_session = MagicMock()
        mock_repository.session_factory.return_value.__enter__.return_value = mock_session
        tickers = [
            TickerUpdate(
                symbol="BTC-USD",
                bid=50000.0 + i * 100,
                bid_qty=1.0,
                ask=50100.0 + i * 100,
                ask_qty=1.0,
                last=50050.0 + i * 100,
                volume=1000.0,
                vwap=50025.0,
                low=49900.0,
                high=50200.0,
                change=100.0,
                change_pct=0.2,
            )
            for i in range(3)
        ]

        async def mock_subscribe_ticks(
            symbols: list[str], *, req_id: int | None = None
        ) -> AsyncIterator[TickerUpdate]:
            for ticker in tickers:
                yield ticker

        service.exchange_client.subscribe_ticks = mock_subscribe_ticks
        count = await service.update_market_snapshots()
        assert count == 3
        saved_snapshots = mock_session.bulk_save_objects.call_args[0][0]
        assert len(saved_snapshots) == 1
        assert saved_snapshots[0].bid == pytest.approx(50200.0)

    async def test_update_market_snapshots_stops_at_2000_snapshots(
        self,
        service: KrakenSnapshotUpdaterService,
        mock_repository: MagicMock,
    ) -> None:
        """Test update_market_snapshots stops at 2000 snapshots.

        Given: service with infinite ticker generator,
        When: calling update_market_snapshots,
        Then: returns exactly 2000 (hard limit).
        """
        mock_session = MagicMock()
        mock_repository.session_factory.return_value.__enter__.return_value = mock_session

        async def infinite_tickers(
            symbols: list[str], *, req_id: int | None = None
        ) -> AsyncIterator[TickerUpdate]:
            i = 0
            while True:
                yield TickerUpdate(
                    symbol=f"SYMBOL{i}-USD",
                    bid=100.0,
                    bid_qty=1.0,
                    ask=101.0,
                    ask_qty=1.0,
                    last=100.5,
                    volume=10.0,
                    vwap=100.25,
                    low=99.0,
                    high=102.0,
                    change=1.0,
                    change_pct=1.0,
                )
                i += 1

        service.exchange_client.subscribe_ticks = infinite_tickers
        result_count = await service.update_market_snapshots()
        assert result_count == 2000

    async def test_update_market_snapshots_calculates_spread_correctly(
        self,
        service: KrakenSnapshotUpdaterService,
        mock_repository: MagicMock,
    ) -> None:
        """Test update_market_snapshots calculates spread correctly.

        Given: ticker with bid=3000, ask=3010,
        When: calling update_market_snapshots,
        Then: spread=10 and spread_pct=(10/mid)*100.
        """
        mock_session = MagicMock()
        mock_repository.session_factory.return_value.__enter__.return_value = mock_session
        ticker_data = TickerUpdate(
            symbol="ETH-USD",
            bid=3000.0,
            bid_qty=10.0,
            ask=3010.0,
            ask_qty=5.0,
            last=3005.0,
            volume=1000.0,
            vwap=3002.5,
            low=2990.0,
            high=3020.0,
            change=15.0,
            change_pct=0.5,
        )

        async def mock_subscribe_ticks(
            symbols: list[str], *, req_id: int | None = None
        ) -> AsyncIterator[TickerUpdate]:
            yield ticker_data

        service.exchange_client.subscribe_ticks = mock_subscribe_ticks
        await service.update_market_snapshots()
        saved_snapshots = mock_session.bulk_save_objects.call_args[0][0]
        snapshot = saved_snapshots[0]
        assert snapshot.spread == pytest.approx(10.0)
        mid = (3000.0 + 3010.0) / 2
        expected_spread_pct = (10.0 / mid) * 100
        assert abs(snapshot.spread_pct - expected_spread_pct) < 0.001

    async def test_update_market_snapshots_handles_zero_mid_price(
        self,
        service: KrakenSnapshotUpdaterService,
        mock_repository: MagicMock,
    ) -> None:
        """Test update_market_snapshots handles zero mid price.

        Given: ticker with bid=0, ask=0,
        When: calling update_market_snapshots,
        Then: spread_pct is 0.0 (no division by zero).
        """
        mock_session = MagicMock()
        mock_repository.session_factory.return_value.__enter__.return_value = mock_session
        ticker_data = TickerUpdate(
            symbol="NULL-USD",
            bid=0.0,
            bid_qty=0.0,
            ask=0.0,
            ask_qty=0.0,
            last=0.0,
            volume=0.0,
            vwap=0.0,
            low=0.0,
            high=0.0,
            change=0.0,
            change_pct=0.0,
        )

        async def mock_subscribe_ticks(
            symbols: list[str], *, req_id: int | None = None
        ) -> AsyncIterator[TickerUpdate]:
            yield ticker_data

        service.exchange_client.subscribe_ticks = mock_subscribe_ticks
        await service.update_market_snapshots()
        saved_snapshots = mock_session.bulk_save_objects.call_args[0][0]
        snapshot = saved_snapshots[0]
        assert snapshot.spread_pct == pytest.approx(0.0)

    async def test_update_market_snapshots_handles_exception(
        self,
        service: KrakenSnapshotUpdaterService,
    ) -> None:
        """Test update_market_snapshots handles exception.

        Given: subscribe_ticks raises RuntimeError,
        When: calling update_market_snapshots,
        Then: RuntimeError is propagated.
        """

        async def failing_subscribe_ticks(
            symbols: list[str], *, req_id: int | None = None
        ) -> AsyncIterator[TickerUpdate]:
            raise RuntimeError("WebSocket connection lost")
            yield

        service.exchange_client.subscribe_ticks = failing_subscribe_ticks
        with pytest.raises(RuntimeError, match="WebSocket connection lost"):
            await service.update_market_snapshots()

    async def test_update_market_snapshots_creates_correct_timestamp(
        self,
        service: KrakenSnapshotUpdaterService,
        mock_repository: MagicMock,
        sample_ticker_data: TickerUpdate,
    ) -> None:
        """Test update_market_snapshots creates correct timestamp.

        Given: service with valid ticker,
        When: calling update_market_snapshots,
        Then: snapshot timestamp is UTC and within before/after bounds.
        """
        mock_session = MagicMock()
        mock_repository.session_factory.return_value.__enter__.return_value = mock_session

        async def mock_subscribe_ticks(
            symbols: list[str], *, req_id: int | None = None
        ) -> AsyncIterator[TickerUpdate]:
            yield sample_ticker_data

        service.exchange_client.subscribe_ticks = mock_subscribe_ticks
        before = datetime.now(UTC)
        await service.update_market_snapshots()
        after = datetime.now(UTC)
        saved_snapshots = mock_session.bulk_save_objects.call_args[0][0]
        snapshot = saved_snapshots[0]
        assert before <= snapshot.timestamp <= after
        assert snapshot.timestamp.tzinfo == UTC


class StubMarketUpdater(MarketSnapshotUpdaterService):
    """Stub implementation of MarketSnapshotUpdaterService for testing."""

    def __init__(self) -> None:
        """Initialize the instance."""
        super().__init__(exchange_client=object(), repository=MagicMock())
        self.calls: list[dict[str, Any]] = []
        self.return_count = 0

    async def update_market_snapshots(self, **kwargs: object) -> int:
        """Record call and return configured count."""
        self.calls.append(dict(kwargs))
        return self.return_count


@pytest.mark.asyncio
async def test_start_invokes_update_and_logs(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test start() invokes update_market_snapshots and logs.

    Given: StubMarketUpdater configured to return 3,
    When: calling start(),
    Then: update called once and log messages contain class name and count.
    """
    updater = StubMarketUpdater()
    updater.return_count = 3
    messages: list[str] = []

    def _fake_info(message: str) -> None:
        messages.append(message)

    monkeypatch.setattr(
        "snapper.infrastructure.market_data.base.logger.info",
        _fake_info,
    )
    await updater.start()
    assert updater.calls == [{}]
    assert any("Starting StubMarketUpdater" in msg for msg in messages)
    assert any("StubMarketUpdater completed - updated 3 snapshots" in msg for msg in messages)
