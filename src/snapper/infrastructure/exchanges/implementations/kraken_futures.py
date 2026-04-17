"""Kraken Futures exchange client implementation.

This module provides KrakenFuturesExchangeClient for the Kraken Futures
exchange. It supports:

REST API Operations:
    - Market data: tickers, OHLCV candles (via CCXT krakenfutures)
    - Instrument metadata (via kraken.futures.Market)
    - Order CRUD: create, cancel, query (via kraken.futures.Trade)
    - Account: wallet balances (via kraken.futures.User)

WebSocket Subscriptions (via callback-to-queue bridge):
    - Public: tickers, trades
    - Private: fills, open_orders (authenticated)

The Kraken Futures SDK uses a callback-driven WebSocket client. This
implementation bridges callbacks to asyncio.Queue objects so that the
publisher can consume data via async iterators (matching Snapper's
established pattern).
"""

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from typing import Any
from typing import cast

import ccxt
from kraken.futures import FuturesWSClient
from kraken.futures import Market
from kraken.futures import Trade
from kraken.futures import User
from loguru import logger

from snapper.core.types import ExchangeEnum
from snapper.data.repository import Repository
from snapper.infrastructure.exchanges.adapters.kraken_futures import _ORDER_TYPE_MAP
from snapper.infrastructure.exchanges.adapters.kraken_futures import _SIDE_MAP
from snapper.infrastructure.exchanges.adapters.kraken_futures import _STATUS_MAP
from snapper.infrastructure.exchanges.adapters.kraken_futures import parse_kraken_futures_fill
from snapper.infrastructure.exchanges.adapters.kraken_futures import parse_kraken_futures_instrument
from snapper.infrastructure.exchanges.adapters.kraken_futures import parse_kraken_futures_ticker
from snapper.infrastructure.exchanges.adapters.kraken_futures import parse_kraken_futures_trade
from snapper.infrastructure.exchanges.base import ExchangeClientBase
from snapper.infrastructure.exchanges.contracts import AccountBalance
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import ExchangeOrderRequest
from snapper.infrastructure.exchanges.contracts import ExchangeOrderSnapshot
from snapper.infrastructure.exchanges.contracts import ExchangeOrderStatusEnum
from snapper.infrastructure.exchanges.contracts import ExchangeOrderTypeEnum
from snapper.infrastructure.exchanges.contracts import ExecutionUpdate
from snapper.infrastructure.exchanges.contracts import FundingRateSnapshot
from snapper.infrastructure.exchanges.contracts import InstrumentPairDescriptor
from snapper.infrastructure.exchanges.contracts import OhlcvSnapshot
from snapper.infrastructure.exchanges.contracts import OpenPositionSnapshot
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.contracts import TickerSnapshot
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate
from snapper.infrastructure.symbols.functions import kraken_futures_ws_to_native
from snapper.infrastructure.symbols.functions import native_to_ccxt
from snapper.infrastructure.symbols.functions import native_to_kraken_futures_ws

_CREDENTIALS_REQUIRED_MSG = "API credentials required for authenticated operations"
_QUEUE_DRAIN_TIMEOUT = 0.1
_QUEUE_MAX_SIZE = 10000

_TIMEFRAME_SECONDS: dict[str, int] = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
}


def _timeframe_to_seconds(timeframe: str) -> int:
    """Convert CCXT timeframe string to duration in seconds.

    Args:
        timeframe: Candle interval (e.g., ``1m``, ``1h``, ``1d``).

    Returns:
        Duration in seconds.

    Raises:
        ValueError: If timeframe is not supported.
    """
    try:
        return _TIMEFRAME_SECONDS[timeframe]
    except KeyError as exc:
        supported = ", ".join(sorted(_TIMEFRAME_SECONDS))
        raise ValueError(f"Unsupported timeframe: {timeframe}. Supported: {supported}") from exc


def _enqueue_or_drop_oldest(queue: asyncio.Queue[Any], item: Any, label: str) -> None:
    """Put item on queue, dropping the oldest if full.

    Args:
        queue: Bounded asyncio queue.
        item: Item to enqueue.
        label: Human-readable label for the warning log.
    """
    try:
        queue.put_nowait(item)
    except asyncio.QueueFull:
        logger.warning(f"{label} queue full, dropping oldest message")
        queue.get_nowait()
        queue.put_nowait(item)


def _map_kraken_order_type(raw: str) -> ExchangeOrderTypeEnum:
    """Map Kraken Futures order type string to ExchangeOrderTypeEnum.

    Args:
        raw: Kraken order type (e.g., ``lmt``, ``mkt``).

    Returns:
        Corresponding ExchangeOrderTypeEnum value.
    """
    return _ORDER_TYPE_MAP.get(raw, ExchangeOrderTypeEnum.LIMIT)


def _map_kraken_status(raw: str) -> ExchangeOrderStatusEnum:
    """Map Kraken Futures order status string to ExchangeOrderStatusEnum.

    Args:
        raw: Kraken order status (e.g., ``placed``, ``filled``).

    Returns:
        Corresponding ExchangeOrderStatusEnum value.
    """
    return _STATUS_MAP.get(raw, ExchangeOrderStatusEnum.OPEN)


def _map_kraken_side(raw: str) -> OrderSideEnum:
    """Map Kraken Futures side string to OrderSideEnum.

    Args:
        raw: Kraken side string (``buy`` or ``sell``).

    Returns:
        Corresponding OrderSideEnum value.
    """
    return _SIDE_MAP.get(raw, OrderSideEnum.BUY)


class KrakenFuturesExchangeClient(ExchangeClientBase):
    """Kraken Futures exchange client.

    Bridges the callback-driven FuturesWSClient to asyncio.Queue-based
    async iterators matching Snapper's publisher consumption pattern.
    Supports both market data (public) and order execution (authenticated).

    Attributes:
        sandbox: Whether to use the demo/sandbox environment.
        supports_websocket_executions: True when API credentials are provided.
    """

    supports_websocket_executions: bool = False

    def __init__(
        self,
        sandbox: bool = False,
        repository: Repository | None = None,
        api_key: str | None = None,
        api_secret: str | None = None,
    ) -> None:
        """Initialize Kraken Futures exchange client.

        Args:
            sandbox: Use sandbox environment for testing (default: False).
            repository: Database repository for order/execution logging.
            api_key: Kraken Futures API key (required for order operations).
            api_secret: Kraken Futures API secret (required for order operations).
        """
        super().__init__(repository=repository, exchange_name=ExchangeEnum.KRAKEN_FUTURES)
        self.sandbox = sandbox
        self._api_key = api_key
        self._api_secret = api_secret
        self._ccxt_client = cast(Any, ccxt.krakenfutures)({"sandbox": sandbox, "timeout": 30000})
        self._market_client: Market | None = None
        self._trade_client: Trade | None = None
        self._user_client: User | None = None
        self._ws_client: FuturesWSClient | None = None
        self._private_ws_client: FuturesWSClient | None = None
        self._tick_queue: asyncio.Queue[TickerUpdate] = asyncio.Queue(maxsize=_QUEUE_MAX_SIZE)
        self._trade_queue: asyncio.Queue[TradeUpdate] = asyncio.Queue(maxsize=_QUEUE_MAX_SIZE)
        self._execution_queue: asyncio.Queue[ExecutionUpdate] = asyncio.Queue(
            maxsize=_QUEUE_MAX_SIZE
        )
        if api_key and api_secret:
            self.supports_websocket_executions = True
            self._trade_client = Trade(key=api_key, secret=api_secret, sandbox=sandbox)
            self._user_client = User(key=api_key, secret=api_secret, sandbox=sandbox)

    def _require_authenticated(self) -> None:
        """Raise RuntimeError if API credentials are missing.

        Raises:
            RuntimeError: If api_key or api_secret were not provided.
        """
        if not self._api_key or not self._api_secret:
            raise RuntimeError(_CREDENTIALS_REQUIRED_MSG)

    async def connect(self) -> None:
        """Establish REST connection to Kraken Futures.

        Loads markets via CCXT and creates the Market REST client.

        Raises:
            RuntimeError: If connection fails.
        """
        try:
            await asyncio.to_thread(self._ccxt_client.load_markets)
            self._market_client = Market(sandbox=self.sandbox)
            logger.info("Kraken Futures REST connection established")
        except Exception as e:
            logger.error(f"Failed to connect to Kraken Futures: {e}")
            raise

    async def disconnect(self) -> None:
        """Close all Kraken Futures connections."""
        if self._ws_client:
            try:
                await self._ws_client.close()
            except Exception as e:
                logger.warning(f"Error closing Kraken Futures WS: {e}")
            self._ws_client = None
        if self._private_ws_client:
            try:
                await self._private_ws_client.close()
            except Exception as e:
                logger.warning(f"Error closing Kraken Futures private WS: {e}")
            self._private_ws_client = None
        logger.info("Kraken Futures connections closed")

    async def _on_ws_message(self, message: dict[str, Any]) -> None:
        """Route incoming WS messages to the appropriate queue.

        This is the callback passed to FuturesWSClient. It parses the
        feed type and dispatches to tick_queue or trade_queue.

        Args:
            message: Raw WebSocket message dictionary.
        """
        if "event" in message:
            return
        feed = message.get("feed", "")
        if feed in ("ticker", "ticker_lite"):
            try:
                ticker_data = (
                    message
                    if "symbol" in message
                    else {**message, "symbol": message.get("product_id", "")}
                )
                tick = parse_kraken_futures_ticker(ticker_data)
                _enqueue_or_drop_oldest(self._tick_queue, tick, "Tick")
            except (ValueError, KeyError) as exc:
                logger.debug(f"Skipping unparseable ticker WS message: {exc}")
        elif feed == "trade":
            product_id = message.get("product_id", "")
            for raw_trade in message.get("trades", []):
                try:
                    trade = parse_kraken_futures_trade({**raw_trade, "product_id": product_id})
                    _enqueue_or_drop_oldest(self._trade_queue, trade, "Trade")
                except (ValueError, KeyError) as exc:
                    logger.debug(f"Skipping unparseable trade WS message: {exc}")

    async def _on_execution_message(self, message: dict[str, Any]) -> None:
        """Route private WS fill messages to the execution queue.

        Only processes ``fills`` and ``fills_snapshot`` feeds. The
        ``open_orders`` feed is intentionally excluded: it carries
        cumulative order-state snapshots (not incremental fills) and
        feeding it through the executor's fill pipeline would emit
        bogus zero-size executions or double-count real fills.

        Args:
            message: Raw WebSocket message dictionary.
        """
        if "event" in message:
            return
        feed = message.get("feed", "")
        if feed in ("fills", "fills_snapshot"):
            for fill in message.get("fills", []):
                try:
                    update = parse_kraken_futures_fill(fill, kraken_futures_ws_to_native)
                    _enqueue_or_drop_oldest(self._execution_queue, update, "Fill")
                except (ValueError, KeyError) as exc:
                    logger.debug(f"Skipping unparseable fill WS message: {exc}")

    async def _ensure_ws_connected(self) -> None:
        """Connect the FuturesWSClient if not already connected."""
        if self._ws_client is not None:
            return
        self._ws_client = FuturesWSClient(callback=self._on_ws_message, sandbox=self.sandbox)
        await self._ws_client.start()
        logger.info("Kraken Futures WebSocket connected")

    async def _ensure_private_ws_connected(self) -> None:
        """Connect the authenticated FuturesWSClient if not already connected.

        Raises:
            RuntimeError: If API credentials are missing.
        """
        self._require_authenticated()
        if self._private_ws_client is not None:
            return
        self._private_ws_client = FuturesWSClient(
            key=self._api_key or "",
            secret=self._api_secret or "",
            callback=self._on_execution_message,
            sandbox=self.sandbox,
        )
        await self._private_ws_client.start()
        logger.info("Kraken Futures private WebSocket connected")

    async def get_ticker(self, symbol: str) -> TickerSnapshot:
        """Fetch current ticker via CCXT.

        Args:
            symbol: CCXT-format symbol (e.g., ``BTC/USD:USD``).

        Returns:
            TickerSnapshot with current price data.
        """
        data = await asyncio.to_thread(self._ccxt_client.fetch_ticker, symbol)
        return TickerSnapshot(
            symbol=symbol,
            bid=float(data.get("bid") or 0),
            ask=float(data.get("ask") or 0),
            last=float(data.get("last") or 0),
            timestamp=float(data.get("timestamp") or 0) / 1000,
        )

    async def get_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1m",
        since: int | None = None,
        limit: int | None = None,
    ) -> list[OhlcvSnapshot]:
        """Fetch OHLCV candles via CCXT.

        Args:
            symbol: CCXT-format symbol.
            timeframe: Candle interval (e.g., ``1m``, ``1h``).
            since: Start timestamp in milliseconds.
            limit: Maximum number of candles.

        Returns:
            List of OhlcvSnapshot objects.
        """
        raw = await asyncio.to_thread(
            self._ccxt_client.fetch_ohlcv, symbol, timeframe, since, limit
        )
        return [
            OhlcvSnapshot(
                timestamp=candle[0] / 1000,
                open=float(candle[1]),
                high=float(candle[2]),
                low=float(candle[3]),
                close=float(candle[4]),
                volume=float(candle[5]),
            )
            for candle in raw
        ]

    async def create_order(self, request: ExchangeOrderRequest) -> ExchangeOrderSnapshot:
        """Submit a new order on Kraken Futures.

        Args:
            request: Order parameters.

        Returns:
            ExchangeOrderSnapshot with the created order state.

        Raises:
            RuntimeError: If API credentials are missing.
            Exception: If order creation fails.
        """
        self._require_authenticated()
        kraken_symbol = native_to_kraken_futures_ws(request.symbol)
        supported_order_types: dict[ExchangeOrderTypeEnum, str] = {
            ExchangeOrderTypeEnum.LIMIT: "lmt",
            ExchangeOrderTypeEnum.MARKET: "mkt",
            ExchangeOrderTypeEnum.STOP_LOSS: "stp",
            ExchangeOrderTypeEnum.TAKE_PROFIT: "take_profit",
            ExchangeOrderTypeEnum.TRAILING_STOP: "trailing_stop",
        }
        if request.type not in supported_order_types:
            raise ValueError(
                f"Unsupported order type for Kraken Futures: {request.type.value}. "
                f"Supported: {', '.join(t.value for t in supported_order_types)}"
            )
        kraken_order_type = supported_order_types[request.type]
        if request.post_only and kraken_order_type == "lmt":
            kraken_order_type = "post"
        kwargs: dict[str, Any] = {
            "orderType": kraken_order_type,
            "size": request.amount,
            "symbol": kraken_symbol,
            "side": request.side.value,
        }
        if request.price is not None:
            kwargs["limitPrice"] = request.price
        if request.client_order_id:
            kwargs["cliOrdId"] = request.client_order_id
        if request.stop_price is not None:
            kwargs["stopPrice"] = request.stop_price
        if request.reduce_only:
            kwargs["reduceOnly"] = True
        result = await asyncio.to_thread(cast(Trade, self._trade_client).create_order, **kwargs)
        send_status = result.get("sendStatus", {})
        order_id = send_status.get("order_id", "")
        status_str = send_status.get("status", "placed")
        order = ExchangeOrderSnapshot(
            id=order_id,
            client_order_id=request.client_order_id,
            symbol=request.symbol,
            side=request.side,
            type=request.type,
            amount=request.amount,
            price=request.price,
            status=_map_kraken_status(status_str),
            filled=0.0,
            remaining=request.amount,
            timestamp=datetime.now(UTC).timestamp(),
        )
        db_result = await self._log_order_to_db(request, order)
        if db_result is not None:
            order.db_order_id = db_result[0]
            order.db_order_public_id = db_result[1]
        return order

    async def cancel_order(self, order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        """Cancel an existing order on Kraken Futures.

        Args:
            order_id: Exchange order ID to cancel.
            symbol: Native symbol (optional, used for response).

        Returns:
            ExchangeOrderSnapshot with cancelled status.

        Raises:
            RuntimeError: If API credentials are missing.
            Exception: If cancellation fails.
        """
        self._require_authenticated()
        result = await asyncio.to_thread(
            cast(Trade, self._trade_client).cancel_order, order_id=order_id
        )
        cancel_status = result.get("cancelStatus", {})
        status_str = cancel_status.get("status", "cancelled")
        events = cancel_status.get("orderEvents", [])
        order_data: dict[str, Any] = {}
        if events:
            order_data = events[0].get("order", {})
        if order_data:
            order_data["status"] = status_str
            return self._convert_sdk_order(order_data)
        if status_str == "notFound":
            try:
                return await self.get_order(order_id, symbol)
            except Exception:
                logger.warning("cancel_order: notFound and get_order failed for {}", order_id)
        return ExchangeOrderSnapshot(
            id=order_id,
            client_order_id=None,
            symbol=symbol or "",
            side=OrderSideEnum.BUY,
            type=ExchangeOrderTypeEnum.LIMIT,
            amount=0.0,
            price=None,
            status=_map_kraken_status(status_str),
            filled=0.0,
            remaining=0.0,
            timestamp=datetime.now(UTC).timestamp(),
        )

    async def get_order(self, order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        """Fetch order details from Kraken Futures.

        Args:
            order_id: Exchange order ID.
            symbol: Native symbol (unused, kept for interface compatibility).

        Returns:
            ExchangeOrderSnapshot with current order state.

        Raises:
            RuntimeError: If API credentials are missing.
            Exception: If order fetch fails.
        """
        self._require_authenticated()
        result = await asyncio.to_thread(
            cast(Trade, self._trade_client).get_orders_status, orderIds=[order_id]
        )
        orders = result.get("orders", [])
        if not orders:
            raise ValueError(f"Order {order_id} not found")
        entry = orders[0]
        inner = entry.get("order", {})
        if inner:
            inner["status"] = entry.get("status", inner.get("status", "placed"))
        else:
            inner = entry
        return self._convert_sdk_order(inner)

    async def get_orders(
        self,
        symbol: str | None = None,
        status: ExchangeOrderStatusEnum | None = None,
        limit: int | None = None,
    ) -> list[ExchangeOrderSnapshot]:
        """Fetch open orders from Kraken Futures.

        Args:
            symbol: Filter by native symbol (optional).
            status: Filter by order status (optional).
            limit: Maximum number of orders to return (optional).

        Returns:
            List of ExchangeOrderSnapshot objects.

        Raises:
            RuntimeError: If API credentials are missing.
            Exception: If orders fetch fails.
        """
        self._require_authenticated()
        result = await asyncio.to_thread(cast(User, self._user_client).get_open_orders)
        raw_orders: list[dict[str, Any]] = result.get("openOrders", [])
        snapshots = [self._convert_sdk_order(o) for o in raw_orders]
        if symbol:
            snapshots = [s for s in snapshots if s.symbol == symbol]
        if status:
            snapshots = [s for s in snapshots if s.status == status]
        if limit:
            snapshots = snapshots[:limit]
        return snapshots

    @staticmethod
    def _parse_coin_margin_account(
        acct_data: dict[str, Any],
    ) -> list[AccountBalance]:
        """Parse a coin-margined account (has a ``balances`` dict).

        Args:
            acct_data: Raw account dictionary from get_wallets.

        Returns:
            List of AccountBalance entries for non-zero currencies.
        """
        acct_balances: dict[str, Any] = acct_data.get("balances", {})
        margin_req = acct_data.get("marginRequirements", {})
        total_margin = float(margin_req.get("im", 0)) if isinstance(margin_req, dict) else 0.0
        non_zero: dict[str, float] = {}
        grand_total = 0.0
        for curr, amount in acct_balances.items():
            val = float(amount)
            if val == 0:
                continue
            non_zero[curr] = val
            grand_total += val
        entries: list[AccountBalance] = []
        for curr, total in non_zero.items():
            share = (total / grand_total) if grand_total > 0 else 0.0
            used = total_margin * share
            entries.append(AccountBalance(currency=curr, free=total - used, used=used, total=total))
        return entries

    @staticmethod
    def _parse_flex_account(
        acct_name: str,
        acct_data: dict[str, Any],
    ) -> AccountBalance | None:
        """Parse a flex/cash multi-collateral account.

        Args:
            acct_name: Account name (``flex`` or ``cash``).
            acct_data: Raw account dictionary from get_wallets.

        Returns:
            AccountBalance or None if balance is zero.
        """
        balance_value = float(acct_data.get("balanceValue", 0))
        if balance_value == 0:
            return None
        available = min(float(acct_data.get("availableMargin", balance_value)), balance_value)
        used = balance_value - available
        return AccountBalance(
            currency=f"{acct_name}_usd",
            free=available,
            used=max(used, 0.0),
            total=balance_value,
        )

    async def get_balance(self, currency: str | None = None) -> dict[str, AccountBalance]:
        """Fetch wallet balances from Kraken Futures.

        Args:
            currency: Filter by specific currency (optional).

        Returns:
            Dictionary of currency to AccountBalance.

        Raises:
            RuntimeError: If API credentials are missing.
            Exception: If balance fetch fails.
        """
        self._require_authenticated()
        result = await asyncio.to_thread(cast(User, self._user_client).get_wallets)
        accounts: dict[str, Any] = result.get("accounts", {})
        balances: dict[str, AccountBalance] = {}
        for acct_name, acct_data in accounts.items():
            if not isinstance(acct_data, dict):
                continue
            if acct_data.get("balances"):
                for coin_bal in self._parse_coin_margin_account(acct_data):
                    balances[coin_bal.currency] = coin_bal
            elif acct_name in ("flex", "cash"):
                flex_bal = self._parse_flex_account(acct_name, acct_data)
                if flex_bal is not None:
                    balances[flex_bal.currency] = flex_bal
        if currency:
            return {k: v for k, v in balances.items() if k == currency}
        return balances

    async def get_open_positions(self) -> list[OpenPositionSnapshot]:
        """Fetch open positions from Kraken Futures.

        Returns:
            List of open position snapshots.

        Raises:
            RuntimeError: If API credentials are missing.
        """
        self._require_authenticated()
        result = await asyncio.to_thread(cast(User, self._user_client).get_open_positions)
        raw_positions: list[dict[str, Any]] = result.get("openPositions", [])
        positions: list[OpenPositionSnapshot] = []
        for pos in raw_positions:
            kraken_symbol = (pos.get("symbol") or "").upper()
            try:
                native_symbol = kraken_futures_ws_to_native(kraken_symbol)
            except ValueError:
                native_symbol = kraken_symbol
            side_str = pos.get("side", "long")
            side = OrderSideEnum.BUY if side_str == "long" else OrderSideEnum.SELL
            positions.append(
                OpenPositionSnapshot(
                    symbol=native_symbol,
                    side=side,
                    size=float(pos.get("size", 0)),
                    entry_price=float(pos.get("price", 0)),
                    mark_price=float(pos.get("markPrice", 0)),
                    unrealized_pnl=float(pos.get("unrealizedPnl", 0)),
                    unrealized_funding=float(pos.get("unrealizedFunding", 0)),
                    timestamp=datetime.now(UTC),
                ),
            )
        return positions

    async def get_historical_funding_rates(
        self,
        symbol: str,
    ) -> list[FundingRateSnapshot]:
        """Fetch all historical funding rates for a perpetual contract.

        Calls ``Market.get_historical_funding_rates(symbol)`` on the
        Kraken Futures SDK. The SDK returns **absolute** ``fundingRate``
        (price units per contract per hour) and **relative**
        ``relativeFundingRate`` (fractional, e.g. 7e-05 ≈ 0.007%).
        We store the relative rate since it matches the
        ``max_funding_rate`` cap on InstrumentSpec.

        Args:
            symbol: Kraken WS symbol (e.g., ``PF_XBTUSD``).

        Returns:
            List of FundingRateSnapshot sorted by effective_from ascending.
        """
        if not self._market_client:
            self._market_client = Market(sandbox=self.sandbox)
        result = await asyncio.to_thread(
            self._market_client.get_historical_funding_rates,
            symbol,
        )
        if not isinstance(result, dict):
            logger.warning(f"Unexpected SDK response type for historical funding: {type(result)}")
            return []
        raw_rates = result.get("rates", [])
        if not isinstance(raw_rates, list):
            logger.warning(f"Expected list for 'rates', got {type(raw_rates)}")
            return []
        try:
            native_symbol = kraken_futures_ws_to_native(symbol.upper())
        except ValueError:
            native_symbol = symbol
        snapshots: list[FundingRateSnapshot] = []
        for entry in raw_rates:
            if not isinstance(entry, dict):
                continue
            ts_str = entry.get("timestamp", "")
            try:
                effective = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                continue
            raw_rate = entry.get("relativeFundingRate")
            if raw_rate is None:
                continue
            try:
                rate = float(raw_rate)
            except (ValueError, TypeError):
                continue
            snapshots.append(
                FundingRateSnapshot(
                    symbol=native_symbol,
                    exchange=ExchangeEnum.KRAKEN_FUTURES,
                    rate_type="perpetual_funding",
                    direction="both",
                    rate=rate,
                    effective_from=effective,
                    notional_asset="USD",
                    source="exchange_api",
                ),
            )
        snapshots.sort(key=lambda s: s.effective_from)
        return snapshots

    async def get_current_funding_rate(self, symbol: str) -> FundingRateSnapshot | None:
        """Extract the live funding rate from the ticker for a perpetual.

        Calls ``Market.get_tickers()`` and finds the ticker matching
        ``symbol``. Returns None if no ticker found or funding rate is
        absent.

        The REST ticker's ``fundingRate`` is already the **relative**
        (fractional) rate, despite sharing its name with the absolute
        rate field in the historical endpoint. Verified against live
        Kraken API: values are ~1e-05 order of magnitude, matching
        ``relativeFundingRate`` from the WS ticker feed. The REST
        ticker does NOT carry a separate ``relativeFundingRate`` field.

        Args:
            symbol: Kraken WS symbol (e.g., ``PF_XBTUSD``).

        Returns:
            FundingRateSnapshot with the current rate, or None.
        """
        if not self._market_client:
            self._market_client = Market(sandbox=self.sandbox)
        result = await asyncio.to_thread(self._market_client.get_tickers)
        if not isinstance(result, dict):
            logger.warning(f"Unexpected SDK response type for tickers: {type(result)}")
            return None
        tickers = result.get("tickers", [])
        if not isinstance(tickers, list):
            logger.warning(f"Expected list for 'tickers', got {type(tickers)}")
            return None
        ticker = self._find_ticker(tickers, symbol.upper())
        if ticker is None:
            return None
        return self._parse_funding_ticker(ticker, symbol)

    def _find_ticker(
        self,
        tickers: list[Any],
        symbol_upper: str,
    ) -> dict[str, Any] | None:
        """Find the ticker dict matching the given symbol.

        Args:
            tickers: List of ticker dicts from SDK.
            symbol_upper: Uppercase Kraken WS symbol to match.

        Returns:
            Matching ticker dict, or None.
        """
        for t in tickers:
            if not isinstance(t, dict):
                continue
            if (t.get("symbol") or "").upper() == symbol_upper:
                return t
        return None

    def _parse_funding_ticker(
        self,
        ticker: dict[str, Any],
        symbol: str,
    ) -> FundingRateSnapshot | None:
        """Parse a funding rate snapshot from a ticker dict.

        Args:
            ticker: Ticker dict from SDK containing fundingRate.
            symbol: Original Kraken WS symbol for fallback.

        Returns:
            FundingRateSnapshot, or None if rate is absent/invalid.
        """
        raw_rate = ticker.get("fundingRate")
        if raw_rate is None:
            return None
        try:
            rate = float(raw_rate)
        except (ValueError, TypeError):
            return None
        symbol_upper = symbol.upper()
        try:
            native_symbol = kraken_futures_ws_to_native(symbol_upper)
        except ValueError:
            native_symbol = symbol
        now = datetime.now(UTC)
        effective = now.replace(minute=0, second=0, microsecond=0)
        return FundingRateSnapshot(
            symbol=native_symbol,
            exchange=ExchangeEnum.KRAKEN_FUTURES,
            rate_type="perpetual_funding",
            direction="both",
            rate=rate,
            effective_from=effective,
            notional_asset="USD",
            source="exchange_api",
        )

    def _convert_sdk_order(self, data: dict[str, Any]) -> ExchangeOrderSnapshot:
        """Convert a Kraken Futures SDK order dict to ExchangeOrderSnapshot.

        Handles both documented field variants from the SDK:
        ``order_id`` / ``orderId``, ``filledSize`` / ``filled``,
        ``unfilledSize`` / ``qty`` / ``quantity``, ``orderType`` / ``type``,
        and lowercase symbols (``pf_xbtusd`` → ``PF_XBTUSD``).

        Args:
            data: Raw order dictionary from SDK.

        Returns:
            ExchangeOrderSnapshot with mapped fields.
        """
        order_id = data.get("order_id") or data.get("orderId", "")
        kraken_symbol = (data.get("symbol") or "").upper()
        try:
            native_symbol = kraken_futures_ws_to_native(kraken_symbol)
        except ValueError:
            native_symbol = kraken_symbol
        filled = float(data.get("filledSize", data.get("filled", 0)))
        unfilled = data.get("unfilledSize")
        if unfilled is not None:
            qty = filled + float(unfilled)
        elif "quantity" in data:
            qty = filled + float(data["quantity"])
        elif "size" in data:
            qty = float(data["size"])
        else:
            qty_raw = data.get("qty")
            qty = float(qty_raw) if qty_raw is not None else filled
        order_type_raw = data.get("orderType") or data.get("type", "lmt")
        return ExchangeOrderSnapshot(
            id=order_id,
            client_order_id=data.get("cliOrdId"),
            symbol=native_symbol,
            side=_map_kraken_side(data.get("side", "buy")),
            type=_map_kraken_order_type(order_type_raw),
            amount=qty,
            price=data.get("limitPrice"),
            status=_map_kraken_status(data.get("status", "placed")),
            filled=filled,
            remaining=qty - filled,
            timestamp=datetime.now(UTC).timestamp(),
        )

    def subscribe_ticks(self, symbols: list[str]) -> AsyncIterator[TickerUpdate]:
        """Subscribe to real-time ticker updates via WebSocket.

        Args:
            symbols: Native symbols (e.g., ``BTC-USD-PERP``) — converted
                to Kraken Futures product IDs internally.

        Returns:
            AsyncIterator yielding TickerUpdate for each price change.
        """
        return self._subscribe_ticks_impl(symbols)

    async def _subscribe_ticks_impl(self, symbols: list[str]) -> AsyncIterator[TickerUpdate]:
        """Implement ticker subscription via callback-to-queue bridge.

        Converts native symbols (e.g., ``BTC-USD-PERP``) to Kraken Futures
        product IDs (e.g., ``PF_XBTUSD``) before subscribing.

        Args:
            symbols: Native symbols to subscribe.

        Yields:
            TickerUpdate for each price change.

        Raises:
            ConnectionError: If the WebSocket connection is lost.
        """
        await self._ensure_ws_connected()
        if self._ws_client is None:
            raise RuntimeError("WebSocket client not connected")
        ws_symbols = [native_to_kraken_futures_ws(s) for s in symbols]
        await self._ws_client.subscribe(feed="ticker", products=ws_symbols)
        logger.info(f"Subscribed to Kraken Futures tickers: {symbols} -> {ws_symbols}")
        try:
            while True:
                if self._ws_client and getattr(self._ws_client, "exception_occur", False):
                    raise ConnectionError("Kraken Futures WS connection lost (tickers)")
                try:
                    message = await asyncio.wait_for(
                        self._tick_queue.get(), timeout=_QUEUE_DRAIN_TIMEOUT
                    )
                    yield message
                except TimeoutError:
                    await asyncio.sleep(0.01)
        finally:
            if self._ws_client:
                try:
                    await self._ws_client.unsubscribe(feed="ticker", products=ws_symbols)
                except Exception:
                    logger.debug("Failed to unsubscribe from tickers on cleanup")
                if getattr(self._ws_client, "exception_occur", False):
                    self._ws_client = None

    def subscribe_candles(
        self,
        symbols: list[str],
        timeframe: str = "1m",
    ) -> AsyncIterator[CandleUpdate]:
        """Subscribe to candle updates via REST polling.

        Kraken Futures has no WebSocket candle feed. This method polls
        ``get_ohlcv()`` at regular intervals and yields new candles as
        they appear. Deduplicates by tracking the latest ``open_at``
        timestamp per symbol.

        Args:
            symbols: Native symbols (e.g., ``BTC-USD-PERP``).
            timeframe: Candle interval (e.g., ``1m``, ``1h``).

        Returns:
            AsyncIterator yielding CandleUpdate for each new candle.
        """
        return self._poll_candles(symbols, timeframe)

    async def _poll_candles(
        self,
        symbols: list[str],
        timeframe: str,
    ) -> AsyncIterator[CandleUpdate]:
        """Poll OHLCV data and yield CandleUpdate items.

        Converts native symbols to CCXT format via the symbol mapper.
        Skips symbols that have no CCXT alias (e.g., dated futures).

        Args:
            symbols: Native symbols to poll.
            timeframe: Candle interval string.

        Yields:
            CandleUpdate for each new candle detected.
        """
        last_seen: dict[str, float] = {}
        poll_interval = 60.0
        interval_seconds = _timeframe_to_seconds(timeframe)
        ccxt_map: dict[str, str] = {}
        for sym in symbols:
            try:
                ccxt_map[sym] = native_to_ccxt(sym)
            except ValueError:
                logger.warning(f"No CCXT alias for {sym}, skipping candle polling")
        if not ccxt_map:
            logger.warning("No symbols with CCXT aliases for candle polling")
            return
        while True:
            for native_sym, ccxt_sym in ccxt_map.items():
                try:
                    since_ts = last_seen.get(native_sym)
                    since_ms = int(since_ts * 1000) if since_ts else None
                    candles = await self.get_ohlcv(
                        symbol=ccxt_sym,
                        timeframe=timeframe,
                        since=since_ms,
                        limit=5,
                    )
                    for candle in candles:
                        if candle.timestamp >= last_seen.get(native_sym, 0):
                            last_seen[native_sym] = candle.timestamp
                            yield CandleUpdate(
                                symbol=native_sym,
                                open=candle.open,
                                high=candle.high,
                                low=candle.low,
                                close=candle.close,
                                vwap=0.0,
                                trades=0,
                                volume=candle.volume,
                                interval_begin=datetime.fromtimestamp(candle.timestamp, tz=UTC),
                                interval=interval_seconds,
                            )
                except Exception:
                    logger.exception(f"Failed to poll candles for {native_sym}")
            await asyncio.sleep(poll_interval)

    def subscribe_trades(self, symbols: list[str]) -> AsyncIterator[TradeUpdate]:
        """Subscribe to real-time trade updates via WebSocket.

        Args:
            symbols: Native symbols (e.g., ``BTC-USD-PERP``) — converted
                to Kraken Futures product IDs internally.

        Returns:
            AsyncIterator yielding TradeUpdate for each trade.
        """
        return self._subscribe_trades_impl(symbols)

    async def _subscribe_trades_impl(self, symbols: list[str]) -> AsyncIterator[TradeUpdate]:
        """Implement trade subscription via callback-to-queue bridge.

        Converts native symbols to Kraken Futures product IDs before subscribing.

        Args:
            symbols: Native symbols to subscribe.

        Yields:
            TradeUpdate for each execution.

        Raises:
            ConnectionError: If the WebSocket connection is lost.
        """
        await self._ensure_ws_connected()
        if self._ws_client is None:
            raise RuntimeError("WebSocket client not connected")
        ws_symbols = [native_to_kraken_futures_ws(s) for s in symbols]
        await self._ws_client.subscribe(feed="trade", products=ws_symbols)
        logger.info(f"Subscribed to Kraken Futures trades: {symbols} -> {ws_symbols}")
        try:
            while True:
                if self._ws_client and getattr(self._ws_client, "exception_occur", False):
                    raise ConnectionError("Kraken Futures WS connection lost (trades)")
                try:
                    message = await asyncio.wait_for(
                        self._trade_queue.get(), timeout=_QUEUE_DRAIN_TIMEOUT
                    )
                    yield message
                except TimeoutError:
                    await asyncio.sleep(0.01)
        finally:
            if self._ws_client:
                try:
                    await self._ws_client.unsubscribe(feed="trade", products=ws_symbols)
                except Exception:
                    logger.debug("Failed to unsubscribe from trades on cleanup")
                if getattr(self._ws_client, "exception_occur", False):
                    self._ws_client = None

    def subscribe_executions(self) -> AsyncIterator[ExecutionUpdate]:
        """Subscribe to real-time execution updates via authenticated WebSocket.

        Yields ExecutionUpdate from the ``fills`` private channel only.
        The ``open_orders`` channel is excluded because it carries
        cumulative order-state (not incremental fills).

        Returns:
            AsyncIterator yielding ExecutionUpdate for each fill event.

        Raises:
            RuntimeError: If API credentials are missing.
        """
        return self._subscribe_executions_impl()

    async def _subscribe_executions_impl(self) -> AsyncIterator[ExecutionUpdate]:
        """Implement execution subscription via authenticated WS.

        Subscribes to the ``fills`` private channel and yields parsed
        ExecutionUpdate objects for each individual fill.

        Yields:
            ExecutionUpdate for each fill event.

        Raises:
            RuntimeError: If credentials are missing or WS connection fails.
            ConnectionError: If the private WebSocket connection is lost.
        """
        await self._ensure_private_ws_connected()
        if self._private_ws_client is None:
            raise RuntimeError("Private WebSocket client not connected")
        await self._private_ws_client.subscribe(feed="fills")
        logger.info("Subscribed to Kraken Futures private feed: fills")
        try:
            while True:
                if self._private_ws_client and getattr(
                    self._private_ws_client, "exception_occur", False
                ):
                    raise ConnectionError("Kraken Futures private WS connection lost")
                try:
                    update = await asyncio.wait_for(
                        self._execution_queue.get(), timeout=_QUEUE_DRAIN_TIMEOUT
                    )
                    yield update
                except TimeoutError:
                    await asyncio.sleep(0.01)
        finally:
            if self._private_ws_client:
                try:
                    await self._private_ws_client.unsubscribe(feed="fills")
                except Exception:
                    logger.debug("Failed to unsubscribe from private feeds on cleanup")
                if getattr(self._private_ws_client, "exception_occur", False):
                    self._private_ws_client = None

    def subscribe_instruments(self, **kwargs: Any) -> AsyncIterator[dict[str, Any]]:
        """Subscribe to instrument data via REST.

        Unlike Kraken Spot, Futures has no WS instrument feed.
        This fetches instruments once via REST and yields each.

        Returns:
            AsyncIterator yielding raw instrument dicts.
        """
        return self._subscribe_instruments_impl()

    async def _subscribe_instruments_impl(self) -> AsyncIterator[dict[str, Any]]:
        """Fetch instruments from REST and yield each as a dict.

        Yields:
            Raw instrument dict from Market.get_instruments().
        """
        if not self._market_client:
            self._market_client = Market(sandbox=self.sandbox)
        result = await asyncio.to_thread(self._market_client.get_instruments)
        instruments: list[dict[str, Any]] = result.get("instruments", [])
        for inst in instruments:
            yield inst

    def get_instruments_sync(self) -> list[dict[str, Any]]:
        """Fetch all instruments synchronously via REST.

        Returns:
            List of raw instrument dicts from Kraken Futures API.
        """
        if not self._market_client:
            self._market_client = Market(sandbox=self.sandbox)
        result = self._market_client.get_instruments()
        return list(result.get("instruments", []))

    def get_parsed_instrument(self, data: dict[str, Any]) -> InstrumentPairDescriptor:
        """Parse a raw instrument dict into InstrumentPairDescriptor.

        Args:
            data: Raw instrument dict from Kraken Futures API.

        Returns:
            Parsed InstrumentPairDescriptor.
        """
        return parse_kraken_futures_instrument(data)
