"""Tests for PaperOrderExecutor and PaperExchangeClient."""

import math
import time
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.config.app import AppSettings
from snapper.infrastructure.exchanges.contracts import ExchangeOrderRequest
from snapper.infrastructure.exchanges.contracts import ExchangeOrderStatusEnum
from snapper.infrastructure.exchanges.contracts import ExchangeOrderTypeEnum
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.implementations.paper import PaperExchangeClient
from snapper.messaging.executors.paper import PaperOrderExecutor
from snapper.messaging.infrastructure.publisher import SequenceTracker


def _make_repo_mock() -> MagicMock:
    """Create a mock repository with async database logging methods.

    Returns:
        MagicMock with ensure_instrument, insert_order, update_order,
        insert_execution, and get_candles (a fresh reference candle so
        MARKET orders resolve a fill price) configured as AsyncMock.
    """
    repo = MagicMock()
    repo.ensure_instrument = AsyncMock(return_value=(1, "inst-pub-1"))
    repo.insert_order = AsyncMock(return_value=(1, "order-uuid-0001"))
    repo.update_order = AsyncMock()
    repo.insert_execution = AsyncMock()
    repo.get_candles = AsyncMock(return_value=[{"close": 64000.0, "open_at": datetime.now(UTC)}])
    session = AsyncMock()
    execute_result = MagicMock()
    execute_result.scalar_one_or_none = MagicMock(return_value="symbol-public-id")
    session.execute = AsyncMock(return_value=execute_result)
    session_cm = AsyncMock()
    session_cm.__aenter__ = AsyncMock(return_value=session)
    session_cm.__aexit__ = AsyncMock(return_value=None)
    repo.session = MagicMock(return_value=session_cm)
    return repo


async def fake_get_market_snapshots(
    instrument_public_ids: list[str],
    start_dt: datetime,
    end_dt: datetime,
    as_of: datetime | None = None,
) -> list[dict]:
    """Return fake market snapshot data for testing."""
    return [
        {
            "instrument_public_id": "inst-btc-usd",
            "bid": 50000.0,
            "ask": 50100.0,
            "last": 50050.0,
            "bid_volume": 1.0,
            "ask_volume": 1.0,
            "volume": 100.0,
            "vwap": 50000.0,
            "low": 49000.0,
            "high": 51000.0,
            "ts": datetime.now(tz=UTC),
        }
    ]


async def fake_iter_market_snapshots(
    instrument_public_ids: list[str],
    start_dt: datetime,
    end_dt: datetime,
    as_of: datetime | None = None,
) -> AsyncIterator[dict]:
    """Async-generator companion mirroring :meth:`fake_get_market_snapshots`."""
    for snap in await fake_get_market_snapshots(instrument_public_ids, start_dt, end_dt, as_of):
        yield snap


async def fake_resolve_instrument_public_ids(symbols: list[str], exchange: str) -> list[str]:
    """Return fake instrument_public_ids for testing."""
    return [f"inst-{s.lower()}" for s in symbols]


async def fake_get_candles(
    symbol: str,
    interval: str,
    start_dt: datetime,
    end_dt: datetime,
    exchange: str,
    as_of: datetime | None = None,
) -> list[dict]:
    """Return fake candle data for testing."""
    return [
        {
            "open_at": datetime.now(tz=UTC),
            "open": 50000.0,
            "high": 51000.0,
            "low": 49000.0,
            "close": 50500.0,
            "volume": 100.0,
            "vwap": 50250.0,
            "trades": 50,
        }
    ]


async def fake_iter_trades(
    symbol: str,
    start_dt: datetime,
    end_dt: datetime,
    exchange: str,
    as_of: datetime | None = None,
) -> AsyncIterator[dict]:
    """Stream fake trade data for testing."""
    for trade in [
        {
            "side": "buy",
            "size": 0.5,
            "price": 50000.0,
            "trade_id": 12345,
            "timestamp": datetime.now(tz=UTC),
        }
    ]:
        yield trade


class TestPaperOrderClientCoverage:
    """Tests for PaperExchangeClient order functionality."""

    @pytest.mark.asyncio
    async def test_create_order_buy_market(self) -> None:
        """Test creating a market buy order.

        Given: A connected paper exchange client,
        When: A market buy order is created,
        Then: Order is created with correct symbol, side, amount and status.
        """
        mock_repo = _make_repo_mock()
        client = PaperExchangeClient(repository=mock_repo)
        client.set_tracker(SequenceTracker())
        await client.connect()
        request = ExchangeOrderRequest(
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            type=ExchangeOrderTypeEnum.MARKET,
            amount=0.5,
            price=50000.0,
            client_order_id="test_order_123",
        )
        result = await client.create_order(request)
        assert result.symbol == "BTC-USD"
        assert result.side == OrderSideEnum.BUY
        assert result.amount == pytest.approx(0.5)
        assert result.status == ExchangeOrderStatusEnum.OPEN
        assert result.client_order_id == "test_order_123"
        assert result.price == pytest.approx(50000.0)
        await client.disconnect()

    @pytest.mark.asyncio
    async def test_create_order_sell_limit(self) -> None:
        """Test creating a limit sell order.

        Given: A connected paper exchange client,
        When: A limit sell order is created,
        Then: Order is created with correct symbol, side, amount, and price.
        """
        mock_repo = _make_repo_mock()
        client = PaperExchangeClient(repository=mock_repo)
        client.set_tracker(SequenceTracker())
        await client.connect()
        request = ExchangeOrderRequest(
            symbol="ETH-USD",
            side=OrderSideEnum.SELL,
            type=ExchangeOrderTypeEnum.LIMIT,
            amount=2.0,
            price=3500.50,
            client_order_id="test_order_456",
        )
        result = await client.create_order(request)
        assert result.symbol == "ETH-USD"
        assert result.side == OrderSideEnum.SELL
        assert result.amount == pytest.approx(2.0)
        assert result.status == ExchangeOrderStatusEnum.OPEN
        assert result.price == pytest.approx(3500.50)
        await client.disconnect()

    @pytest.mark.asyncio
    async def test_create_order_without_client_order_id(self) -> None:
        """Test creating order without client order ID.

        Given: A connected paper exchange client,
        When: Order is created without client_order_id,
        Then: Order is created successfully with OPEN status.
        """
        mock_repo = _make_repo_mock()
        client = PaperExchangeClient(repository=mock_repo)
        client.set_tracker(SequenceTracker())
        await client.connect()
        request = ExchangeOrderRequest(
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            type=ExchangeOrderTypeEnum.MARKET,
            amount=0.1,
            price=50000.0,
            client_order_id=None,
        )
        result = await client.create_order(request)
        assert result.symbol == "BTC-USD"
        assert result.status == ExchangeOrderStatusEnum.OPEN
        await client.disconnect()

    @pytest.mark.asyncio
    async def test_get_order(self) -> None:
        """Test retrieving an unknown order by ID.

        Given: A connected paper exchange client with no such order in memory,
        When: get_order is called with an unknown (post-restart) order ID,
        Then: A CANCELED terminal snapshot carrying the ID and symbol is
            returned, so recovery does not resurrect it as a zombie pending.
        """
        mock_repo = _make_repo_mock()
        client = PaperExchangeClient(repository=mock_repo)
        client.set_tracker(SequenceTracker())
        await client.connect()
        result = await client.get_order("paper_order_123", symbol="BTC-USD")
        assert result.id == "paper_order_123"
        assert result.symbol == "BTC-USD"
        assert result.status == ExchangeOrderStatusEnum.CANCELED
        await client.disconnect()

    @pytest.mark.asyncio
    async def test_get_order_not_found(self) -> None:
        """Test retrieving a non-existent order.

        Given: A connected paper exchange client,
        When: get_order is called with non-existent ID,
        Then: Order snapshot with the ID is returned.
        """
        mock_repo = _make_repo_mock()
        client = PaperExchangeClient(repository=mock_repo)
        client.set_tracker(SequenceTracker())
        await client.connect()
        result = await client.get_order("non_existent_id", symbol="ETH-USD")
        assert result.id == "non_existent_id"
        assert result.symbol == "ETH-USD"
        await client.disconnect()

    @pytest.mark.asyncio
    async def test_cancel_order(self) -> None:
        """Test canceling an order.

        Given: A connected paper exchange client,
        When: cancel_order is called,
        Then: Order is returned with CANCELED status.
        """
        mock_repo = _make_repo_mock()
        client = PaperExchangeClient(repository=mock_repo)
        client.set_tracker(SequenceTracker())
        await client.connect()
        result = await client.cancel_order("paper_order_789", symbol="BTC-USD")
        assert result.id == "paper_order_789"
        assert result.status == ExchangeOrderStatusEnum.CANCELED
        assert result.symbol == "BTC-USD"
        await client.disconnect()

    @pytest.mark.asyncio
    async def test_get_orders_list(self) -> None:
        """Test getting list of orders.

        Given: A connected paper exchange client with no orders,
        When: get_orders is called,
        Then: Empty list is returned.
        """
        mock_repo = _make_repo_mock()
        client = PaperExchangeClient(repository=mock_repo)
        client.set_tracker(SequenceTracker())
        await client.connect()
        results = await client.get_orders(symbol="BTC-USD", limit=10)
        assert results == []
        await client.disconnect()

    @pytest.mark.asyncio
    async def test_get_balance(self) -> None:
        """Test getting balance for specific currency.

        Given: A connected paper exchange client,
        When: get_balance is called for USD,
        Then: USD balance with default 10000.0 is returned.
        """
        mock_repo = _make_repo_mock()
        client = PaperExchangeClient(repository=mock_repo)
        client.set_tracker(SequenceTracker())
        await client.connect()
        result = await client.get_balance("USD")
        assert "USD" in result
        assert result["USD"].currency == "USD"
        assert result["USD"].total == pytest.approx(10000.0)
        await client.disconnect()

    @pytest.mark.asyncio
    async def test_get_balance_all_currencies(self) -> None:
        """Test getting balance for all currencies.

        Given: A connected paper exchange client,
        When: get_balance is called without currency,
        Then: Balances for USD, BTC, ETH are returned.
        """
        mock_repo = _make_repo_mock()
        client = PaperExchangeClient(repository=mock_repo)
        client.set_tracker(SequenceTracker())
        await client.connect()
        result = await client.get_balance()
        assert "USD" in result
        assert "BTC" in result
        assert "ETH" in result
        await client.disconnect()

    @pytest.mark.asyncio
    async def test_get_balance_unknown_currency(self) -> None:
        """Test getting balance for unknown currency.

        Given: A connected paper exchange client,
        When: get_balance is called for unknown currency,
        Then: Zero balance is returned for that currency.
        """
        mock_repo = _make_repo_mock()
        client = PaperExchangeClient(repository=mock_repo)
        client.set_tracker(SequenceTracker())
        await client.connect()
        result = await client.get_balance("XYZ")
        assert "XYZ" in result
        assert result["XYZ"].currency == "XYZ"
        assert result["XYZ"].total == pytest.approx(0.0)
        await client.disconnect()

    @pytest.mark.asyncio
    async def test_get_orders_with_filters(self) -> None:
        """Test getting orders with various filters.

        Given: A connected paper exchange client with multiple orders,
        When: get_orders is called with filters,
        Then: Filtered orders are returned.
        """
        mock_repo = _make_repo_mock()
        client = PaperExchangeClient(repository=mock_repo)
        client.set_tracker(SequenceTracker())
        await client.connect()
        await client.create_order(
            ExchangeOrderRequest(
                symbol="BTC-USD",
                side=OrderSideEnum.BUY,
                type=ExchangeOrderTypeEnum.MARKET,
                amount=0.1,
                price=50000.0,
                client_order_id="order1",
            )
        )
        await client.create_order(
            ExchangeOrderRequest(
                symbol="ETH-USD",
                side=OrderSideEnum.SELL,
                type=ExchangeOrderTypeEnum.LIMIT,
                amount=1.0,
                price=3000.0,
                client_order_id="order2",
            )
        )
        orders = await client.get_orders(status=ExchangeOrderStatusEnum.OPEN)
        assert len(orders) == 2
        btc_orders = await client.get_orders(symbol="BTC-USD")
        assert len(btc_orders) == 1
        assert btc_orders[0].symbol == "BTC-USD"
        limited = await client.get_orders(limit=1)
        assert len(limited) == 1
        await client.disconnect()

    @pytest.mark.asyncio
    async def test_subscribe_executions(self) -> None:
        """Test subscribing to execution updates.

        Given: A connected paper exchange client with fill delay,
        When: Order is created and executions are subscribed,
        Then: Execution update with CLOSED status is received.
        """
        mock_repo = _make_repo_mock()
        client = PaperExchangeClient(repository=mock_repo, fill_delay=0.01)
        client.set_tracker(SequenceTracker())
        await client.connect()
        await client.create_order(
            ExchangeOrderRequest(
                symbol="BTC-USD",
                side=OrderSideEnum.BUY,
                type=ExchangeOrderTypeEnum.MARKET,
                amount=0.1,
                price=50000.0,
                client_order_id="execution_test",
            )
        )
        async for execution in client.subscribe_executions():
            assert execution.symbol == "BTC-USD"
            assert execution.order_status == ExchangeOrderStatusEnum.CLOSED
            assert execution.cum_qty == pytest.approx(0.1)
            assert execution.last_price == pytest.approx(50000.0)
            break
        await client.disconnect()

    @pytest.mark.asyncio
    async def test_get_supported_pairs(self) -> None:
        """Test getting supported trading pairs.

        Given: A paper exchange client,
        When: get_supported_pairs is called,
        Then: Common pairs like BTC/USD, ETH/USD are returned.
        """
        mock_repo = _make_repo_mock()
        client = PaperExchangeClient(repository=mock_repo)
        client.set_tracker(SequenceTracker())
        pairs = client.get_supported_pairs()
        assert "BTC/USD" in pairs
        assert "ETH/USD" in pairs
        assert "EUR/USD" in pairs

    @pytest.mark.asyncio
    async def test_disconnect_cancels_tasks(self) -> None:
        """Test disconnect cancels pending tasks.

        Given: A connected paper exchange client with pending order,
        When: disconnect is called,
        Then: Client is no longer running.
        """
        mock_repo = _make_repo_mock()
        client = PaperExchangeClient(repository=mock_repo, fill_delay=0.01)
        client.set_tracker(SequenceTracker())
        await client.connect()
        await client.create_order(
            ExchangeOrderRequest(
                symbol="BTC-USD",
                side=OrderSideEnum.BUY,
                type=ExchangeOrderTypeEnum.MARKET,
                amount=0.1,
                price=50000.0,
            )
        )
        await client.disconnect()
        assert not client._running

    @pytest.mark.asyncio
    async def test_context_manager(self) -> None:
        """Test async context manager protocol.

        Given: A paper exchange client,
        When: Used as async context manager,
        Then: Client is connected inside context and disconnected after.
        """
        mock_repo = _make_repo_mock()
        paper_client = PaperExchangeClient(repository=mock_repo)
        paper_client.set_tracker(SequenceTracker())
        async with paper_client as client:
            assert client._running
            order = await client.create_order(
                ExchangeOrderRequest(
                    symbol="BTC-USD",
                    side=OrderSideEnum.BUY,
                    type=ExchangeOrderTypeEnum.MARKET,
                    amount=0.1,
                    price=50000.0,
                )
            )
            assert order.status == ExchangeOrderStatusEnum.OPEN
        assert not client._running

    @pytest.mark.asyncio
    async def test_create_order_not_connected(self) -> None:
        """Test creating order when not connected.

        Given: A paper exchange client that is not connected,
        When: create_order is called,
        Then: RuntimeError is raised.
        """
        mock_repo = _make_repo_mock()
        client = PaperExchangeClient(repository=mock_repo)
        client.set_tracker(SequenceTracker())
        request = ExchangeOrderRequest(
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            type=ExchangeOrderTypeEnum.MARKET,
            amount=0.1,
        )
        with pytest.raises(RuntimeError, match="not connected"):
            await client.create_order(request)


class TestPaperMarketDataMethods:
    """Tests for PaperExchangeClient market data methods."""

    @pytest.mark.asyncio
    async def test_get_ticker_from_repository(self) -> None:
        """Test getting ticker from repository.

        Given: A connected paper client with repository,
        When: get_ticker is called,
        Then: Ticker with bid, ask, last prices is returned.
        """
        mock_repo = SimpleNamespace(
            get_market_snapshots=fake_get_market_snapshots,
            iter_market_snapshots=fake_iter_market_snapshots,
        )
        client = PaperExchangeClient(repository=mock_repo, source_exchange="kraken")
        client._resolve_instrument_public_ids = fake_resolve_instrument_public_ids
        client.set_tracker(SequenceTracker())
        await client.connect()
        ticker = await client.get_ticker("BTC-USD")
        assert ticker.symbol == "BTC-USD"
        assert ticker.bid == pytest.approx(50000.0)
        assert ticker.ask == pytest.approx(50100.0)
        assert ticker.last == pytest.approx(50050.0)
        await client.disconnect()

    @pytest.mark.asyncio
    async def test_get_ticker_no_repository(self) -> None:
        """Test getting ticker without repository.

        Given: A connected paper client without repository,
        When: get_ticker is called,
        Then: RuntimeError is raised.
        """
        client = PaperExchangeClient(repository=None)
        client.set_tracker(SequenceTracker())
        await client.connect()
        with pytest.raises(RuntimeError, match="Repository required"):
            await client.get_ticker("BTC-USD")
        await client.disconnect()

    @pytest.mark.asyncio
    async def test_get_ohlcv_from_repository(self) -> None:
        """Test getting OHLCV data from repository.

        Given: A connected paper client with repository,
        When: get_ohlcv is called,
        Then: Candle data with OHLCV values is returned.
        """
        mock_repo = SimpleNamespace(get_candles=fake_get_candles)
        client = PaperExchangeClient(repository=mock_repo, source_exchange="kraken")
        client.set_tracker(SequenceTracker())
        await client.connect()
        candles = await client.get_ohlcv("BTC-USD", "1m", limit=10)
        assert len(candles) == 1
        assert candles[0].open == pytest.approx(50000.0)
        assert candles[0].close == pytest.approx(50500.0)
        await client.disconnect()

    @pytest.mark.asyncio
    async def test_get_ohlcv_no_repository(self) -> None:
        """Test getting OHLCV without repository.

        Given: A connected paper client without repository,
        When: get_ohlcv is called,
        Then: RuntimeError is raised.
        """
        client = PaperExchangeClient(repository=None)
        client.set_tracker(SequenceTracker())
        await client.connect()
        with pytest.raises(RuntimeError, match="Repository required"):
            await client.get_ohlcv("BTC-USD", "1m")
        await client.disconnect()

    @pytest.mark.asyncio
    async def test_subscribe_ticker_replay(self) -> None:
        """Test subscribing to ticker replay.

        Given: A connected paper client with time range and source exchange,
        When: subscribe_ticker is called,
        Then: Ticker updates are yielded from repository.
        """
        mock_repo = SimpleNamespace(
            get_market_snapshots=fake_get_market_snapshots,
            iter_market_snapshots=fake_iter_market_snapshots,
        )
        start_ts = time.time() - 3600
        end_ts = time.time()
        client = PaperExchangeClient(
            repository=mock_repo,
            start_time=start_ts,
            end_time=end_ts,
            source_exchange="kraken",
        )
        client._resolve_instrument_public_ids = fake_resolve_instrument_public_ids
        client.set_tracker(SequenceTracker())
        await client.connect()
        async for ticker in client.subscribe_ticker(["BTC-USD"]):
            assert ticker.bid == pytest.approx(50000.0)
            break
        await client.disconnect()

    @pytest.mark.asyncio
    async def test_subscribe_ticker_no_time_range(self) -> None:
        """Test subscribing to ticker without time range.

        Given: A connected paper client without time range,
        When: subscribe_ticker is called,
        Then: ValueError is raised.
        """
        mock_repo = SimpleNamespace(
            get_market_snapshots=fake_get_market_snapshots,
            iter_market_snapshots=fake_iter_market_snapshots,
        )
        client = PaperExchangeClient(repository=mock_repo, source_exchange="kraken")
        client.set_tracker(SequenceTracker())
        await client.connect()
        with pytest.raises(ValueError, match="Time range"):
            async for _ in client.subscribe_ticker(["BTC-USD"]):
                break
        await client.disconnect()

    @pytest.mark.asyncio
    async def test_subscribe_ticker_no_source_exchange(self) -> None:
        """Test subscribing to ticker without source exchange.

        Given: A connected paper client without source_exchange,
        When: subscribe_ticker is called,
        Then: ValueError is raised.
        """
        mock_repo = SimpleNamespace(
            get_market_snapshots=fake_get_market_snapshots,
            iter_market_snapshots=fake_iter_market_snapshots,
        )
        client = PaperExchangeClient(
            repository=mock_repo,
            start_time=time.time() - 3600,
            end_time=time.time(),
        )
        client.set_tracker(SequenceTracker())
        await client.connect()
        with pytest.raises(ValueError, match="source_exchange required"):
            async for _ in client.subscribe_ticker(["BTC-USD"]):
                break
        await client.disconnect()

    @pytest.mark.asyncio
    async def test_subscribe_candles_replay(self) -> None:
        """Test subscribing to candles replay.

        Given: A connected paper client with time range and source exchange,
        When: subscribe_candles is called,
        Then: Candle updates are yielded from repository.
        """
        mock_repo = SimpleNamespace(get_candles=fake_get_candles)
        start_ts = time.time() - 3600
        end_ts = time.time()
        client = PaperExchangeClient(
            repository=mock_repo,
            start_time=start_ts,
            end_time=end_ts,
            source_exchange="kraken",
        )
        client.set_tracker(SequenceTracker())
        await client.connect()
        async for candle in client.subscribe_candles(["BTC-USD"], "1m"):
            assert candle.symbol == "BTC-USD"
            assert candle.open == pytest.approx(50000.0)
            break
        await client.disconnect()

    @pytest.mark.asyncio
    async def test_subscribe_trades_replay(self) -> None:
        """Test subscribing to trades replay.

        Given: A connected paper client with time range and source exchange,
        When: subscribe_trades is called,
        Then: Trade updates are yielded from repository.
        """
        mock_repo = SimpleNamespace(
            iter_trades=fake_iter_trades,
        )
        start_ts = time.time() - 3600
        end_ts = time.time()
        client = PaperExchangeClient(
            repository=mock_repo,
            start_time=start_ts,
            end_time=end_ts,
            source_exchange="kraken",
        )
        client.set_tracker(SequenceTracker())
        await client.connect()
        async for trade in client.subscribe_trades(["BTC-USD"]):
            assert trade.symbol == "BTC-USD"
            assert trade.price == pytest.approx(50000.0)
            break
        await client.disconnect()

    @pytest.mark.asyncio
    async def test_subscribe_candles_replay_multiple_symbols_sorted(self) -> None:
        """Test candle replay supports multiple symbols in timestamp order.

        Given: A connected paper client and repository with two symbols,
        When: subscribe_candles is called with both symbols,
        Then: Candles are yielded for both symbols in ascending timestamp order.
        """

        class MultiSymbolRepo:
            async def get_candles(
                self,
                symbol: str,
                interval: str,
                start_dt: datetime,
                end_dt: datetime,
                exchange: str,
                as_of: datetime | None = None,
            ) -> list[dict]:
                _ = interval
                _ = start_dt
                _ = end_dt
                _ = exchange
                base = datetime(2024, 1, 1, tzinfo=UTC)
                if symbol == "BTC-USD":
                    return [
                        {
                            "open_at": base.replace(minute=2),
                            "open": 2.0,
                            "high": 2.1,
                            "low": 1.9,
                            "close": 2.0,
                            "volume": 1.0,
                            "vwap": 2.0,
                            "trades": 1,
                        }
                    ]
                if symbol == "ETH-USD":
                    return [
                        {
                            "open_at": base.replace(minute=1),
                            "open": 1.0,
                            "high": 1.1,
                            "low": 0.9,
                            "close": 1.0,
                            "volume": 1.0,
                            "vwap": 1.0,
                            "trades": 1,
                        }
                    ]
                return []

        start_ts = datetime(2024, 1, 1, tzinfo=UTC).timestamp()
        end_ts = datetime(2024, 1, 1, 0, 10, tzinfo=UTC).timestamp()
        client = PaperExchangeClient(
            repository=MultiSymbolRepo(),
            start_time=start_ts,
            end_time=end_ts,
            source_exchange="kraken",
        )
        client.set_tracker(SequenceTracker())
        await client.connect()
        candles: list[tuple[str, datetime]] = []
        async for candle in client.subscribe_candles(["BTC-USD", "ETH-USD"], "1m"):
            candles.append((candle.symbol, candle.interval_begin))
        await client.disconnect()
        assert candles == [
            ("ETH-USD", datetime(2024, 1, 1, 0, 1, tzinfo=UTC)),
            ("BTC-USD", datetime(2024, 1, 1, 0, 2, tzinfo=UTC)),
        ]

    @pytest.mark.asyncio
    async def test_subscribe_trades_replay_multiple_symbols_sorted(self) -> None:
        """Test trades replay supports multiple symbols in timestamp order.

        Given: A connected paper client and repository with two symbols,
        When: subscribe_trades is called with both symbols,
        Then: Trades are yielded for both symbols in ascending timestamp order.
        """

        class MultiSymbolRepo:
            async def _trades_for_symbol(self, symbol: str) -> list[dict]:
                base = datetime(2024, 1, 1, tzinfo=UTC)
                if symbol == "BTC-USD":
                    return [
                        {
                            "side": "buy",
                            "size": 1.0,
                            "price": 2.0,
                            "trade_id": 2,
                            "timestamp": base.replace(minute=2),
                        },
                        {
                            "side": "buy",
                            "size": 1.0,
                            "price": 2.5,
                            "trade_id": 4,
                            "timestamp": base.replace(minute=4),
                        },
                    ]
                if symbol == "ETH-USD":
                    return [
                        {
                            "side": "sell",
                            "size": 1.0,
                            "price": 1.0,
                            "trade_id": 1,
                            "timestamp": base.replace(minute=1),
                        },
                        {
                            "side": "sell",
                            "size": 1.0,
                            "price": 1.5,
                            "trade_id": 3,
                            "timestamp": base.replace(minute=3),
                        },
                    ]
                return []

            async def iter_trades(
                self,
                symbol: str,
                start_dt: datetime,
                end_dt: datetime,
                exchange: str,
                as_of: datetime | None = None,
            ) -> AsyncIterator[dict]:
                _ = start_dt
                _ = end_dt
                _ = exchange
                for trade in await self._trades_for_symbol(symbol):
                    yield trade

        start_ts = datetime(2024, 1, 1, tzinfo=UTC).timestamp()
        end_ts = datetime(2024, 1, 1, 0, 10, tzinfo=UTC).timestamp()
        client = PaperExchangeClient(
            repository=MultiSymbolRepo(),
            start_time=start_ts,
            end_time=end_ts,
            source_exchange="kraken",
        )
        client.set_tracker(SequenceTracker())
        await client.connect()
        trades: list[tuple[str, datetime]] = []
        async for trade in client.subscribe_trades(["BTC-USD", "ETH-USD"]):
            trades.append((trade.symbol, trade.timestamp))
        await client.disconnect()
        assert trades == [
            ("ETH-USD", datetime(2024, 1, 1, 0, 1, tzinfo=UTC)),
            ("BTC-USD", datetime(2024, 1, 1, 0, 2, tzinfo=UTC)),
            ("ETH-USD", datetime(2024, 1, 1, 0, 3, tzinfo=UTC)),
            ("BTC-USD", datetime(2024, 1, 1, 0, 4, tzinfo=UTC)),
        ]

    @pytest.mark.asyncio
    async def test_parse_interval_to_minutes(self) -> None:
        """Test parsing interval string to minutes.

        Given: A paper exchange client,
        When: Various interval formats are parsed,
        Then: Correct minute values are returned.
        """
        client = PaperExchangeClient()
        assert client._parse_interval_to_minutes("1m") == 1
        assert client._parse_interval_to_minutes("5m") == 5
        assert client._parse_interval_to_minutes("1h") == 60
        assert client._parse_interval_to_minutes("2h") == 120
        assert client._parse_interval_to_minutes("1d") == 1440
        with pytest.raises(ValueError):
            client._parse_interval_to_minutes("invalid")


class TestPaperOrderExecutor:
    """Tests for PaperOrderExecutor service."""

    def test_get_default_parameters_advertises_wallet_public_id(self) -> None:
        """``get_default_parameters`` advertises wallet param.

        Given: AppSettings instance,
        When: ``get_default_parameters`` is called,
        Then: Returns ``{"wallet_public_id": ""}`` so the process
            launcher knows the parameter exists.
        """
        settings = MagicMock(spec=AppSettings)
        assert PaperOrderExecutor.get_default_parameters(settings) == {"wallet_public_id": ""}

    @patch("snapper.messaging.executors.paper.get_repository")
    @patch("snapper.messaging.executors.base.get_settings")
    def test_create_exchange_client_raises_without_credentials(
        self,
        mock_get_settings: MagicMock,
        mock_get_repository: MagicMock,
    ) -> None:
        """PaperOrderExecutor without credentials raises RuntimeError.

        Given: PaperOrderExecutor with the default empty wallet_public_id
            and ``self._credentials`` still ``None``,
        When: ``_create_exchange_client`` is called,
        Then: A ``RuntimeError`` surfaces with an actionable message.
            Every paper wallet must carry an explicit ``initial_balance``
            in its credential envelope.
        """
        settings = SimpleNamespace(db_url="sqlite:///:memory:")
        mock_get_settings.return_value = settings
        mock_get_repository.return_value = object()
        executor = PaperOrderExecutor()
        with pytest.raises(RuntimeError, match="credentials not resolved"):
            executor._create_exchange_client()

    @patch("snapper.messaging.executors.paper.PaperExchangeClient")
    @patch("snapper.messaging.executors.paper.get_repository")
    @patch("snapper.messaging.executors.base.get_settings")
    def test_create_exchange_client_uses_credential_initial_balance(
        self,
        mock_get_settings: MagicMock,
        mock_get_repository: MagicMock,
        mock_paper_client: MagicMock,
    ) -> None:
        """Per-wallet path: initial balance comes from credential dict.

        Given: PaperOrderExecutor with a populated credential envelope
            (``initial_balance=2500.0`` as a stringified value),
        When: ``_create_exchange_client`` is called,
        Then: PaperExchangeClient receives the per-wallet balance.
        """
        settings = SimpleNamespace(db_url="sqlite:///:memory:")
        mock_get_settings.return_value = settings
        mock_get_repository.return_value = object()
        executor = PaperOrderExecutor(wallet_public_id="019d5a8b3c7d4e5f")
        executor._credentials = {"initial_balance": "2500.0"}
        executor._create_exchange_client()
        kwargs = mock_paper_client.call_args.kwargs
        assert math.isclose(kwargs["initial_balance"], 2500.0)

    @patch("snapper.messaging.executors.base.get_settings")
    def test_get_exchange_name(self, mock_get_settings: MagicMock) -> None:
        """Test _get_exchange_name returns paper.

        Given: A PaperOrderExecutor instance,
        When: _get_exchange_name is called,
        Then: 'paper' is returned.
        """
        settings = SimpleNamespace()
        mock_get_settings.return_value = settings
        executor = PaperOrderExecutor()
        assert executor._get_exchange_name() == "paper"
