"""Paper trading simulation exchange client.

This module provides PaperExchangeClient, a simulated exchange client
for testing trading strategies without real money. It implements:

Order Simulation:
    - Create, cancel orders with simulated fills
    - Configurable fill delay for realistic timing
    - Support for market and limit orders

Account Management:
    - Simulated balances for multiple currencies
    - Balance updates on order fills
    - Initial balance configuration

Features:
    - No external dependencies (fully in-memory)
    - Configurable time window for backtesting
    - Execution queue for strategy callbacks
    - Database logging for order/execution history

The paper client is ideal for:
    - Strategy backtesting
    - Integration testing
    - Development without exchange credentials
    - Risk-free experimentation
"""

import asyncio
import contextlib
import heapq
import time
import uuid
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any

from loguru import logger
from sqlalchemy import select

from snapper.core.types import AllExchange
from snapper.core.types import ExchangeEnum
from snapper.core.types import MarketDataExchange
from snapper.data.models import Instrument
from snapper.data.models import Symbol
from snapper.data.repository import Repository
from snapper.data.repository import where_active
from snapper.data.repository_types import MarketSnapshotRow
from snapper.infrastructure.exchanges.base import ExchangeClientBase
from snapper.infrastructure.exchanges.contracts import AccountBalance
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import ExchangeOrderRequest
from snapper.infrastructure.exchanges.contracts import ExchangeOrderSnapshot
from snapper.infrastructure.exchanges.contracts import ExchangeOrderStatusEnum
from snapper.infrastructure.exchanges.contracts import ExchangeOrderTypeEnum
from snapper.infrastructure.exchanges.contracts import ExecutionUpdate
from snapper.infrastructure.exchanges.contracts import OhlcvSnapshot
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.contracts import TickerSnapshot
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate

_NOT_CONNECTED_MSG = "PaperExchangeClient not connected"
_REPO_REQUIRED_MSG = "Repository required for paper market data"
_CALL_CONNECT_MSG = "Not connected - call connect() first"
_TIME_RANGE_REQUIRED_MSG = "Time range (start_time, end_time) required for paper market data replay"
_SOURCE_EXCHANGE_REQUIRED_MSG = "source_exchange required for paper market data replay"


class _EmptyInstrumentsAsyncIterator(AsyncIterator[dict[str, Any]]):
    """Async iterator that yields nothing."""

    def __aiter__(self) -> _EmptyInstrumentsAsyncIterator:
        return self

    async def __anext__(self) -> dict[str, Any]:
        raise StopAsyncIteration


class PaperExchangeClient(ExchangeClientBase):
    """Simulated exchange client for paper trading and backtesting.

    This client simulates exchange behavior without connecting to any
    real exchange. Orders are filled after a configurable delay and
    balances are tracked in-memory.

    Useful for strategy development, testing, and backtesting without
    risking real funds.

    Attributes:
        fill_delay: Delay in seconds before orders are filled.
        initial_balance: Starting balance for each currency.
        start_time: Optional start timestamp for backtesting window.
        end_time: Optional end timestamp for backtesting window.
    """

    def __init__(
        self,
        repository: Repository | None = None,
        fill_delay: float = 0.1,
        initial_balance: float = 10000.0,
        start_time: float | None = None,
        end_time: float | None = None,
        source_exchange: MarketDataExchange | None = None,
    ) -> None:
        """Initialize paper trading client.

        Args:
            repository: Database repository for order/execution logging.
            fill_delay: Delay in seconds before simulating order fills.
            initial_balance: Starting balance for each currency.
            start_time: Start timestamp for backtesting (Unix seconds).
            end_time: End timestamp for backtesting (Unix seconds).
            source_exchange: Source exchange for replay data queries.
                When set, subscribe methods query this exchange instead
                of "paper". Used by per-source paper publishers.
        """
        super().__init__(repository=repository, exchange_name=ExchangeEnum.PAPER)
        self.fill_delay = fill_delay
        self.initial_balance = initial_balance
        self.start_time = start_time
        self.end_time = end_time
        self.source_exchange = source_exchange
        self._running = False
        self._execution_queue: asyncio.Queue[ExecutionUpdate] = asyncio.Queue()
        self._orders: dict[str, ExchangeOrderSnapshot] = {}
        self._balances: dict[str, AccountBalance] = {}
        self._fill_simulator_task: asyncio.Task[None] | None = None

    async def connect(self) -> None:
        """Initialize paper trading system with default balances."""
        if self._running:
            logger.warning("PaperExchangeClient already connected")
            return
        logger.info("Connecting to Paper Trading System...")
        for currency in ["USD", "EUR", "PLN", "BTC", "ETH"]:
            self._balances[currency] = AccountBalance(
                currency=currency,
                free=self.initial_balance,
                used=0.0,
                total=self.initial_balance,
            )
        self._running = True
        logger.info("Paper Trading System connected")

    async def disconnect(self) -> None:
        """Stop paper trading and clean up resources."""
        if not self._running:
            return
        logger.info("Disconnecting from Paper Trading System...")
        self._running = False
        if self._fill_simulator_task:
            self._fill_simulator_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._fill_simulator_task
            self._fill_simulator_task = None
        logger.info("Paper Trading System disconnected")

    async def create_order(self, request: ExchangeOrderRequest) -> ExchangeOrderSnapshot:
        """Create a simulated order.

        Args:
            request: Order parameters.

        Returns:
            Created order snapshot with simulated fill pending.

        Raises:
            RuntimeError: If client not connected.
            ValueError: If the request is stop-typed — the paper fill
                simulator has no trigger logic, so accepting a stop
                would fill it IMMEDIATELY and misrepresent the
                protective semantics (#156). Pre-send rejection keeps
                the executor's definitive-reject branch honest.
        """
        if not self._running:
            raise RuntimeError(_NOT_CONNECTED_MSG)
        if request.type in (
            ExchangeOrderTypeEnum.STOP_LOSS,
            ExchangeOrderTypeEnum.STOP_LOSS_LIMIT,
        ):
            raise ValueError("Paper trading does not support stop orders (no trigger simulation)")
        order_id = str(uuid.uuid4())
        timestamp = request.signaled_at.timestamp() if request.signaled_at else time.time()
        logger.info(
            f"PAPER ORDER: {request.side.value} {request.amount} {request.symbol} @ {request.price}"
        )
        order = ExchangeOrderSnapshot(
            id=order_id,
            client_order_id=request.client_order_id,
            symbol=request.symbol,
            side=request.side,
            type=request.type,
            amount=request.amount,
            price=request.price,
            status=ExchangeOrderStatusEnum.OPEN,
            filled=0.0,
            remaining=request.amount,
            timestamp=timestamp,
            fee=None,
        )
        self._orders[order_id] = order
        db_result = await self._log_order_to_db(request, order)
        if db_result is not None:
            order.db_order_id = db_result[0]
            order.db_order_public_id = db_result[1]
        self._fill_simulator_task = asyncio.create_task(self._simulate_fill(order))
        return order

    async def _simulate_fill(self, order: ExchangeOrderSnapshot) -> None:
        try:
            await asyncio.sleep(self.fill_delay)
            if not self._running or order.id not in self._orders:
                return
            from datetime import datetime

            execution = ExecutionUpdate(
                order_id=order.id,
                exec_type="trade",
                symbol=order.symbol,
                side=order.side,
                order_type=order.type,
                order_status=ExchangeOrderStatusEnum.CLOSED,
                timestamp=datetime.fromtimestamp(order.timestamp, tz=UTC),
                order_qty=order.amount,
                cum_qty=order.amount,
                last_qty=order.amount,
                average_price=order.price or 0.0,
                last_price=order.price or 0.0,
                fee_usd_equiv=0.0,
            )
            order.status = ExchangeOrderStatusEnum.CLOSED
            order.filled = order.amount
            order.remaining = 0.0
            await self._execution_queue.put(execution)
            logger.info(f"PAPER FILL: {order.id} - {order.amount}@{order.price}")
        except Exception as e:
            logger.error(f"Error simulating fill for order {order.id}: {e}")

    async def cancel_order(self, order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        """Cancel a simulated order.

        Args:
            order_id: Order ID to cancel.
            symbol: Trading pair (optional).

        Returns:
            Snapshot of the cancelled order.

        Raises:
            RuntimeError: If client not connected.
        """
        if not self._running:
            raise RuntimeError(_NOT_CONNECTED_MSG)
        logger.info(f"PAPER CANCEL: {order_id} ({symbol})")
        if order_id in self._orders:
            order = self._orders[order_id]
            order.status = ExchangeOrderStatusEnum.CANCELED
            return order
        return ExchangeOrderSnapshot(
            id=order_id,
            client_order_id=None,
            symbol=symbol or "UNKNOWN",
            side=OrderSideEnum.BUY,
            type=ExchangeOrderTypeEnum.LIMIT,
            amount=0.0,
            price=None,
            status=ExchangeOrderStatusEnum.CANCELED,
            filled=0.0,
            remaining=0.0,
            timestamp=time.time(),
            fee=None,
        )

    async def get_order(self, order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        """Get details of a simulated order.

        Args:
            order_id: Order ID to fetch.
            symbol: Trading pair (optional).

        Returns:
            Order snapshot.

        Raises:
            RuntimeError: If client not connected.
        """
        if not self._running:
            raise RuntimeError(_NOT_CONNECTED_MSG)
        if order_id in self._orders:
            return self._orders[order_id]
        return ExchangeOrderSnapshot(
            id=order_id,
            client_order_id=None,
            symbol=symbol or "UNKNOWN",
            side=OrderSideEnum.BUY,
            type=ExchangeOrderTypeEnum.LIMIT,
            amount=0.0,
            price=None,
            status=ExchangeOrderStatusEnum.OPEN,
            filled=0.0,
            remaining=0.0,
            timestamp=time.time(),
            fee=None,
        )

    async def get_orders(
        self,
        symbol: str | None = None,
        status: ExchangeOrderStatusEnum | None = None,
        limit: int | None = None,
    ) -> list[ExchangeOrderSnapshot]:
        """Get list of simulated orders with optional filtering.

        Args:
            symbol: Filter by trading pair.
            status: Filter by order status.
            limit: Maximum orders to return.

        Returns:
            List of order snapshots.

        Raises:
            RuntimeError: If client not connected.
        """
        if not self._running:
            raise RuntimeError(_NOT_CONNECTED_MSG)
        orders = list(self._orders.values())
        if symbol:
            orders = [o for o in orders if o.symbol == symbol]
        if status:
            orders = [o for o in orders if o.status == status]
        if limit:
            orders = orders[:limit]
        logger.info(f"PAPER GET_ORDERS: {len(orders)} orders (symbol={symbol}, status={status})")
        return orders

    async def get_balance(self, currency: str | None = None) -> dict[str, AccountBalance]:
        """Get simulated account balances.

        Args:
            currency: Filter by specific currency.

        Returns:
            Dictionary of currency to balance info.

        Raises:
            RuntimeError: If client not connected.
        """
        if not self._running:
            raise RuntimeError(_NOT_CONNECTED_MSG)
        if currency:
            if currency in self._balances:
                return {currency: self._balances[currency]}
            return {
                currency: AccountBalance(
                    currency=currency,
                    free=0.0,
                    used=0.0,
                    total=0.0,
                )
            }
        return self._balances.copy()

    def subscribe_executions(self) -> AsyncIterator[ExecutionUpdate]:
        """Subscribe to simulated execution updates.

        Returns:
            AsyncIterator yielding ExecutionUpdate for each simulated order fill.

        Raises:
            RuntimeError: If client not connected.
        """
        return self._subscribe_executions_impl()

    async def _subscribe_executions_impl(self) -> AsyncIterator[ExecutionUpdate]:
        """Implement simulated execution streaming from the internal queue.

        Yields:
            ExecutionUpdate for each simulated order fill.

        Raises:
            RuntimeError: If client not connected.
        """
        if not self._running:
            raise RuntimeError(_NOT_CONNECTED_MSG)
        logger.info("PAPER: Subscribed to execution updates (via queue)")
        while self._running:
            try:
                execution = await asyncio.wait_for(self._execution_queue.get(), timeout=1.0)
                yield execution
            except TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error in paper execution subscription: {e}")
                break

    async def _resolve_instrument_public_ids(self, symbols: list[str], exchange: str) -> list[str]:
        """Resolve native symbols to instrument_public_ids via repository.

        Performs 2-hop lookup: Symbol(native_symbol) -> Symbol.public_id,
        then Instrument(symbol_public_id, exchange) -> Instrument.public_id.

        Args:
            symbols: List of native symbol strings.
            exchange: Exchange name for instrument lookup.

        Returns:
            List of resolved instrument_public_id strings (may be shorter than input).
        """
        if not self.repository:
            return []
        result: list[str] = []
        now = datetime.now(tz=UTC)
        async with self.repository.session() as s:
            for symbol in symbols:
                s_ts, s_kt = where_active(Symbol, now)
                sym_q = await s.execute(
                    select(Symbol.public_id).where(
                        Symbol.native_symbol == symbol,
                        s_ts,
                        s_kt,
                    )
                )
                symbol_pid = sym_q.scalar_one_or_none()
                if symbol_pid is None:
                    continue
                i_ts, i_kt = where_active(Instrument, now)
                inst_q = await s.execute(
                    select(Instrument.public_id).where(
                        Instrument.symbol_public_id == symbol_pid,
                        Instrument.exchange == exchange,
                        i_ts,
                        i_kt,
                    )
                )
                inst_pid = inst_q.scalar_one_or_none()
                if inst_pid is not None:
                    result.append(inst_pid)
        return result

    async def get_ticker(self, symbol: str) -> TickerSnapshot:
        """Get ticker data from repository for backtesting.

        Args:
            symbol: Trading pair.

        Returns:
            Latest ticker snapshot from historical data.

        Raises:
            RuntimeError: If repository not configured.
            ValueError: If no data found for symbol.
        """
        if not self.repository:
            raise RuntimeError(_REPO_REQUIRED_MSG)
        if not self.source_exchange:
            raise ValueError(_SOURCE_EXCHANGE_REQUIRED_MSG)
        end_dt = datetime.now(tz=UTC)
        start_dt = end_dt.replace(hour=0, minute=0, second=0, microsecond=0)
        inst_pids = await self._resolve_instrument_public_ids([symbol], self.source_exchange)
        if not inst_pids:
            raise ValueError(f"No ticker data found for {symbol}")
        snapshots = await self.repository.get_market_snapshots(
            inst_pids, start_dt, end_dt, as_of=datetime.now(UTC)
        )
        if not snapshots:
            raise ValueError(f"No ticker data found for {symbol}")
        latest = snapshots[-1]
        return TickerSnapshot(
            symbol=symbol,
            bid=latest["bid"] or 0.0,
            ask=latest["ask"] or 0.0,
            last=latest["last"] or 0.0,
            timestamp=latest["ts"].timestamp(),
        )

    async def get_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1m",
        since: int | None = None,
        limit: int | None = None,
    ) -> list[OhlcvSnapshot]:
        """Get OHLCV data from repository for backtesting.

        Args:
            symbol: Trading pair.
            timeframe: Candle interval.
            since: Start timestamp (unused in paper mode).
            limit: Maximum candles to return.

        Returns:
            List of historical OHLCV snapshots.

        Raises:
            RuntimeError: If repository not configured.
        """
        if not self.repository:
            raise RuntimeError(_REPO_REQUIRED_MSG)
        if not self.source_exchange:
            raise ValueError(_SOURCE_EXCHANGE_REQUIRED_MSG)
        end_dt = datetime.now(tz=UTC)
        if limit:
            interval_minutes = self._parse_interval_to_minutes(timeframe)
            lookback_minutes = interval_minutes * limit * 2
            start_dt = end_dt - timedelta(minutes=lookback_minutes)
        else:
            start_dt = end_dt - timedelta(hours=24)
        candles = await self.repository.get_candles(
            symbol,
            timeframe,
            start_dt,
            end_dt,
            exchange=self.source_exchange,
            as_of=datetime.now(UTC),
        )
        ohlcv_list = [
            OhlcvSnapshot(
                timestamp=c["open_at"].timestamp(),
                open=c["open"],
                high=c["high"],
                low=c["low"],
                close=c["close"],
                volume=c["volume"],
            )
            for c in candles
        ]
        if limit and len(ohlcv_list) > limit:
            return ohlcv_list[-limit:]
        return ohlcv_list

    def subscribe_ticker(self, symbols: list[str]) -> AsyncIterator[TickerUpdate]:
        """Replay historical ticker data for backtesting.

        Uses self.source_exchange (if set) to query the correct exchange data.

        Args:
            symbols: List of trading pairs to replay.

        Returns:
            AsyncIterator yielding TickerUpdate from historical data in time order.

        Raises:
            RuntimeError: If repository not configured or not connected.
            ValueError: If time range not specified.
        """
        return self._subscribe_ticker_impl(symbols)

    async def _subscribe_ticker_impl(self, symbols: list[str]) -> AsyncIterator[TickerUpdate]:
        """Implement historical ticker streaming for paper trading.

        Streams snapshots via :meth:`Repository.iter_market_snapshots`
        (bounded-memory paper backtest streaming) so multi-day windows
        keep RSS flat. The legacy materialising path
        (:meth:`Repository.get_market_snapshots`) is reserved for
        bounded UI / one-shot lookups.

        Args:
            symbols: List of trading pairs to replay.

        Yields:
            TickerUpdate from historical data in time order.

        Raises:
            RuntimeError: If repository not configured or not connected.
            ValueError: If time range not specified.
        """
        if not self.repository:
            raise RuntimeError(_REPO_REQUIRED_MSG)
        if not self._running:
            raise RuntimeError(_CALL_CONNECT_MSG)
        if self.start_time is None or self.end_time is None:
            raise ValueError(_TIME_RANGE_REQUIRED_MSG)
        start_dt = datetime.fromtimestamp(self.start_time, tz=UTC)
        end_dt = datetime.fromtimestamp(self.end_time, tz=UTC)
        if not self.source_exchange:
            raise ValueError(_SOURCE_EXCHANGE_REQUIRED_MSG)
        exchange_name = self.source_exchange
        logger.info(
            f"Replaying ticker for {symbols} from {start_dt} to {end_dt} from {exchange_name}"
        )
        inst_pids = await self._resolve_instrument_public_ids(symbols, exchange_name)
        if not inst_pids:
            return
        async for snap in self.repository.iter_market_snapshots(
            inst_pids, start_dt, end_dt, as_of=datetime.now(UTC)
        ):
            yield self._snapshot_to_ticker(snap)

    @staticmethod
    def _snapshot_to_ticker(snap: MarketSnapshotRow) -> TickerUpdate:
        """Convert a market snapshot row to a TickerUpdate.

        Args:
            snap: Market snapshot row from the repository.

        Returns:
            TickerUpdate with zero-defaults for missing values.
        """
        return TickerUpdate(
            symbol=str(snap.get("symbol", "")),
            bid=snap["bid"] or 0.0,
            bid_qty=snap["bid_volume"] or 0.0,
            ask=snap["ask"] or 0.0,
            ask_qty=snap["ask_volume"] or 0.0,
            last=snap["last"] or 0.0,
            volume=snap["volume"] or 0.0,
            vwap=snap["vwap"] or 0.0,
            low=snap["low"] or 0.0,
            high=snap["high"] or 0.0,
            change=0.0,
            change_pct=0.0,
        )

    def subscribe_candles(
        self,
        symbols: list[str],
        timeframe: str = "1m",
    ) -> AsyncIterator[CandleUpdate]:
        """Replay historical candle data for backtesting.

        Uses self.source_exchange (if set) to query the correct exchange data.

        Args:
            symbols: List of trading pairs (uses first symbol).
            timeframe: Candle interval.

        Returns:
            AsyncIterator yielding CandleUpdate from historical data.

        Raises:
            RuntimeError: If repository not configured or not connected.
            ValueError: If time range not specified.
        """
        return self._subscribe_candles_impl(symbols, timeframe=timeframe)

    async def _subscribe_candles_impl(
        self, symbols: list[str], *, timeframe: str
    ) -> AsyncIterator[CandleUpdate]:
        """Implement historical candle replay for paper trading.

        Args:
            symbols: List of trading pairs (uses first symbol).
            timeframe: Candle interval.

        Yields:
            CandleUpdate from historical data.

        Raises:
            RuntimeError: If repository not configured or not connected.
            ValueError: If time range not specified.
        """
        if not symbols:
            return
        interval = timeframe
        if not self.repository:
            raise RuntimeError(f"{_REPO_REQUIRED_MSG} - call connect() first")
        if not self._running:
            raise RuntimeError(_CALL_CONNECT_MSG)
        if self.start_time is None or self.end_time is None:
            raise ValueError(_TIME_RANGE_REQUIRED_MSG)
        start_dt = datetime.fromtimestamp(self.start_time, tz=UTC)
        end_dt = datetime.fromtimestamp(self.end_time, tz=UTC)
        if not self.source_exchange:
            raise ValueError(_SOURCE_EXCHANGE_REQUIRED_MSG)
        exchange_name = self.source_exchange
        logger.info(f"Replaying candles for {symbols}/{interval} from {start_dt} to {end_dt}")
        logger.info(f"Candle replay source exchange: {exchange_name}")
        interval_minutes = self._parse_interval_to_minutes(interval)
        replay_candles: list[CandleUpdate] = []
        for symbol in symbols:
            candles = await self.repository.get_candles(
                symbol,
                interval,
                start_dt,
                end_dt,
                exchange=exchange_name,
                as_of=datetime.now(UTC),
            )
            for candle_dict in candles:
                replay_candles.append(
                    CandleUpdate(
                        symbol=symbol,
                        interval_begin=candle_dict["open_at"],
                        interval=interval_minutes,
                        open=candle_dict["open"],
                        high=candle_dict["high"],
                        low=candle_dict["low"],
                        close=candle_dict["close"],
                        volume=candle_dict["volume"],
                        vwap=candle_dict.get("vwap") or 0.0,
                        trades=candle_dict.get("trades") or 0,
                    )
                )
        replay_candles.sort(key=lambda candle: candle.interval_begin)
        for candle in replay_candles:
            yield candle

    @staticmethod
    def _parse_interval_to_minutes(interval: str) -> int:
        """Convert interval string to minutes.

        Args:
            interval: Interval like '1m', '1h', '1d'.

        Returns:
            Number of minutes.

        Raises:
            ValueError: If format is invalid.
        """
        if interval.endswith("m"):
            return int(interval[:-1])
        if interval.endswith("h"):
            return int(interval[:-1]) * 60
        if interval.endswith("d"):
            return int(interval[:-1]) * 1440
        raise ValueError(f"Invalid interval format: {interval}")

    def subscribe_trades(self, symbols: list[str]) -> AsyncIterator[TradeUpdate]:
        """Replay historical trade data for backtesting.

        Uses self.source_exchange (if set) to query the correct exchange data.

        Args:
            symbols: List of trading pairs.

        Returns:
            AsyncIterator yielding TradeUpdate from historical data.

        Raises:
            RuntimeError: If repository not configured or not connected.
            ValueError: If time range not specified.
        """
        return self._subscribe_trades_impl(symbols)

    async def _subscribe_trades_impl(self, symbols: list[str]) -> AsyncIterator[TradeUpdate]:
        """Implement historical trades replay for paper trading.

        Streams per-symbol via :meth:`Repository.iter_trades` and
        merges the resulting time-ordered streams with a k-way async
        heap merge (bounded-memory paper backtest streaming). The
        legacy materialising path (a bounded-list fetch with an
        in-memory sort across every symbol) was OOM-prone on
        multi-day, multi-symbol replays; this version keeps RSS
        bounded by the number of symbols (one ``TradeUpdate`` in
        flight per stream).

        Args:
            symbols: List of trading pairs.

        Yields:
            TradeUpdate from historical data in event-time ASC order.

        Raises:
            RuntimeError: If repository not configured or not connected.
            ValueError: If time range not specified.
        """
        if not self.repository:
            raise RuntimeError(_REPO_REQUIRED_MSG)
        if not self._running:
            raise RuntimeError(_CALL_CONNECT_MSG)
        if self.start_time is None or self.end_time is None:
            raise ValueError(_TIME_RANGE_REQUIRED_MSG)
        start_dt = datetime.fromtimestamp(self.start_time, tz=UTC)
        end_dt = datetime.fromtimestamp(self.end_time, tz=UTC)
        if not self.source_exchange:
            raise ValueError(_SOURCE_EXCHANGE_REQUIRED_MSG)
        exchange_name = self.source_exchange
        logger.info(
            f"Replaying trades for {symbols} from {start_dt} to {end_dt} from {exchange_name}"
        )
        as_of = datetime.now(UTC)
        per_symbol_streams = [
            self._iter_trades_for_symbol(symbol, start_dt, end_dt, exchange_name, as_of)
            for symbol in symbols
        ]
        async for trade in self._heap_merge_trade_streams(per_symbol_streams):
            yield trade

    async def _iter_trades_for_symbol(
        self,
        symbol: str,
        start_dt: datetime,
        end_dt: datetime,
        exchange_name: AllExchange,
        as_of: datetime,
    ) -> AsyncIterator[TradeUpdate]:
        """Stream historical trades for one symbol as ``TradeUpdate`` objects.

        Per-stream helper for :meth:`_subscribe_trades_impl`. Yields
        in event-time ASC order (whatever :meth:`iter_trades`
        guarantees) so the k-way heap merge can rely on the same
        ordering across all input streams.

        Args:
            symbol: Trading pair (Snapper-native form).
            start_dt: Replay window start.
            end_dt: Replay window end.
            exchange_name: Source exchange to query.
            as_of: SCD2 ``known_to`` cutoff.

        Yields:
            ``TradeUpdate`` rows in time order.
        """
        assert self.repository is not None
        async for trade_dict in self.repository.iter_trades(
            symbol, start_dt, end_dt, exchange=exchange_name, as_of=as_of
        ):
            yield TradeUpdate(
                symbol=symbol,
                side=trade_dict["side"],
                quantity=trade_dict["size"],
                price=trade_dict["price"],
                ord_type="unknown",
                trade_id=trade_dict.get("trade_id"),
                timestamp=trade_dict.get("executed_at") or trade_dict["timestamp"],
            )

    @staticmethod
    async def _heap_merge_trade_streams(
        streams: list[AsyncIterator[TradeUpdate]],
    ) -> AsyncIterator[TradeUpdate]:
        """K-way async heap merge of time-ordered ``TradeUpdate`` streams.

        Mirrors the pattern in
        :mod:`snapper.application.backtest.candle_stream.merge_sorted_streams`
        but narrowed to :class:`TradeUpdate` rows keyed by
        ``timestamp``. Each input stream must yield in event-time
        ASC order; the merge yields all rows globally in the same
        order. The stream insertion index participates in the heap
        key as a deterministic tie-breaker on equal timestamps.

        Args:
            streams: Sorted async iterators of ``TradeUpdate``.

        Yields:
            ``TradeUpdate`` rows in global ascending timestamp order.
        """
        heap: list[tuple[datetime, int, TradeUpdate]] = []
        for idx, stream in enumerate(streams):
            try:
                event = await anext(stream)
            except StopAsyncIteration:
                continue
            heap.append((event.timestamp, idx, event))
        heapq.heapify(heap)
        while heap:
            _, idx, event = heapq.heappop(heap)
            yield event
            try:
                next_event = await anext(streams[idx])
            except StopAsyncIteration:
                continue
            heapq.heappush(heap, (next_event.timestamp, idx, next_event))

    def get_supported_pairs(self) -> list[str]:
        """Get list of supported trading pairs for paper trading.

        Returns:
            List of default supported pairs.
        """
        return [
            "BTC/USD",
            "ETH/USD",
            "EUR/USD",
            "EUR/PLN",
            "USD/PLN",
        ]

    def subscribe_ticks(self, symbols: list[str]) -> AsyncIterator[TickerUpdate]:
        """Subscribe to ticker updates (alias for subscribe_ticker).

        Uses self.source_exchange (if set) to query the correct exchange data.

        Args:
            symbols: List of trading pairs.

        Returns:
            AsyncIterator yielding TickerUpdate from historical data.
        """
        return self.subscribe_ticker(symbols)

    def subscribe_instruments(self, **kwargs: Any) -> AsyncIterator[dict[str, Any]]:
        """Subscribe to instrument updates (not implemented for paper).

        Args:
            **kwargs: Ignored parameters.

        Yields:
            Nothing; the iterator is always empty.
        """
        _ = kwargs
        return _EmptyInstrumentsAsyncIterator()
