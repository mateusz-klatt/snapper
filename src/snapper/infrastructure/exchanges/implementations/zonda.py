"""Zonda (BitBay) cryptocurrency exchange client implementation.

This module provides ZondaExchangeClient for interacting with the Zonda
exchange (formerly BitBay), a Polish cryptocurrency exchange. It supports:

REST API Operations:
    - Market data: tickers, OHLCV candles
    - Order management: create, cancel, get orders
    - Account: balance inquiries

WebSocket Subscriptions:
    - Public: tickers (merged with 24h stats), trades
    - Private: executions (requires API credentials and HMAC signing)

Features:
    - CCXT integration for standard REST operations
    - Native WebSocket implementation with reconnection logic
    - Ticker/stats cache merging for complete market data
    - HMAC-SHA512 authentication for private channels
    - Candle aggregation from trade stream

The client uses internal caches to merge ticker and 24h statistics
streams since Zonda provides them on separate WebSocket channels.
"""

import asyncio
import contextlib
import hashlib
import hmac
import json
import time
import uuid
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from typing import Any
from typing import cast

import ccxt
from loguru import logger
from websockets.asyncio.client import ClientConnection
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from snapper.infrastructure.exchanges.base import ExchangeClientBase
from snapper.infrastructure.exchanges.contracts import AccountBalance
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import ExchangeOrderRequest
from snapper.infrastructure.exchanges.contracts import ExchangeOrderSnapshot
from snapper.infrastructure.exchanges.contracts import ExecutionUpdate
from snapper.infrastructure.exchanges.contracts import OhlcvSnapshot
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.contracts import OrderStatusEnum
from snapper.infrastructure.exchanges.contracts import OrderTypeEnum
from snapper.infrastructure.exchanges.contracts import TickerSnapshot
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate
from snapper.infrastructure.exchanges.schemas.zonda import ZondaExecutionsMessage
from snapper.infrastructure.exchanges.schemas.zonda import ZondaStatsMessage
from snapper.infrastructure.exchanges.schemas.zonda import ZondaTickerMessage
from snapper.infrastructure.exchanges.schemas.zonda import ZondaTransactionsMessage
from snapper.infrastructure.exchanges.schemas.zonda import create_ticker_update_from_caches
from snapper.infrastructure.symbols.functions import ccxt_to_native
from snapper.infrastructure.symbols.functions import native_to_ccxt


class ZondaExchangeClient(ExchangeClientBase):
    """Zonda (BitBay) exchange client with REST and WebSocket support.

    This client provides access to the Zonda cryptocurrency exchange,
    using CCXT for REST operations and native WebSocket for real-time
    data streaming.

    The client implements automatic reconnection with exponential backoff
    and maintains caches for merging ticker and statistics streams.

    Attributes:
        ws_url: WebSocket endpoint URL.
        api_key: Zonda API key for authenticated requests.
        api_secret: Zonda API secret for HMAC signing.
    """

    def __init__(
        self,
        api_key: str | None = None,
        api_secret: str | None = None,
        max_reconnect_attempts: int = 10,
        reconnect_delay: float = 2.0,
        enable_rate_limit: bool = True,
        repository: Any | None = None,
    ) -> None:
        """Initialize Zonda exchange client.

        Args:
            api_key: Zonda API key. Required for trading and private data.
            api_secret: Zonda API secret. Required for trading and private data.
            max_reconnect_attempts: Maximum WebSocket reconnection attempts.
            reconnect_delay: Delay between reconnection attempts in seconds.
            enable_rate_limit: Enable automatic rate limiting (default: True).
            repository: Database repository for order/execution logging.
        """
        super().__init__(repository=repository, exchange_name="zonda")
        self.ws_url = "wss://api.zondacrypto.exchange/websocket/"
        self.api_key = api_key
        self.api_secret = api_secret
        ccxt_kwargs: dict[str, Any] = {
            "apiKey": api_key,
            "secret": api_secret,
            "enableRateLimit": enable_rate_limit,
            "timeout": 30000,
            "rateLimit": 1000,
        }
        self._ccxt_client = cast(Any, ccxt.zonda)(ccxt_kwargs)
        self._ws: ClientConnection | None = None
        self._tick_queue: asyncio.Queue[TickerUpdate] = asyncio.Queue()
        self._trade_queue: asyncio.Queue[TradeUpdate] = asyncio.Queue()
        self._candle_queue: asyncio.Queue[CandleUpdate] = asyncio.Queue()
        self._execution_queue: asyncio.Queue[ExecutionUpdate] = asyncio.Queue()
        self._subscribed_symbols: list[str] = []
        self._subscribed_trade_symbols: list[str] = []
        self._subscribed_candle_symbols: list[str] = []
        self._seq_no: dict[str, int] = {}
        self._max_reconnect_attempts = max_reconnect_attempts
        self._reconnect_delay = reconnect_delay
        self._running = False
        self._message_handler_task: asyncio.Task[None] | None = None
        self._ticker_cache: dict[str, dict[str, Any]] = {}
        self._stats_cache: dict[str, dict[str, Any]] = {}
        self._candle_builder: dict[str, dict[str, Any]] = {}
        self._candle_aggregator_task: asyncio.Task[None] | None = None
        self._pending_snapshots: dict[str, str] = {}

    def get_ccxt_client(self) -> Any:
        """Get the underlying CCXT client instance.

        Returns:
            The CCXT Zonda client for direct access to CCXT methods.
        """
        return self._ccxt_client

    async def connect(self) -> None:
        """Connect to Zonda WebSocket with automatic retry.

        Raises:
            Exception: If all connection attempts fail.
        """
        for attempt in range(self._max_reconnect_attempts):
            try:
                if self._ws is None:
                    logger.info(f"Connecting to Zonda WebSocket (attempt {attempt + 1})...")
                    self._ws = await connect(self.ws_url)
                    logger.info("Connected to Zonda WebSocket")
                    return
            except Exception as e:
                logger.warning(
                    f"Connection attempt {attempt + 1} failed: {e}. "
                    f"Retrying in {self._reconnect_delay}s..."
                )
                if attempt < self._max_reconnect_attempts - 1:
                    await asyncio.sleep(self._reconnect_delay)
                else:
                    logger.error("Max reconnection attempts reached")
                    raise

    async def disconnect(self) -> None:
        """Disconnect from WebSocket and clean up resources."""
        self._running = False
        if self._message_handler_task:
            self._message_handler_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._message_handler_task
        if self._ws:
            await self._ws.close()
            self._ws = None
            logger.info("Disconnected from Zonda WebSocket")

    async def _resubscribe(self) -> None:
        """Resubscribe to all channels after reconnection."""
        assert self._ws is not None
        if self._subscribed_symbols == ["*"]:
            logger.info("Resubscribing to ALL symbols (wildcard)...")
            ticker_msg = {
                "action": "subscribe-public",
                "module": "trading",
                "path": "ticker",
            }
            await self._ws.send(json.dumps(ticker_msg))
            stats_msg = {
                "action": "subscribe-public",
                "module": "trading",
                "path": "stats",
            }
            await self._ws.send(json.dumps(stats_msg))
            logger.info("Resubscribed to ALL Zonda symbols (ticker + stats)")
        else:
            logger.info(f"Resubscribing to {len(self._subscribed_symbols)} symbols...")
            for symbol in self._subscribed_symbols:
                symbol_lower = symbol.lower()
                ticker_msg = {
                    "action": "subscribe-public",
                    "module": "trading",
                    "path": f"ticker/{symbol_lower}",
                }
                await self._ws.send(json.dumps(ticker_msg))
                stats_msg = {
                    "action": "subscribe-public",
                    "module": "trading",
                    "path": f"stats/{symbol_lower}",
                }
                await self._ws.send(json.dumps(stats_msg))
                logger.debug(f"Resubscribed to Zonda: {symbol} (ticker + stats)")
            logger.info(f"Resubscribed to {len(self._subscribed_symbols)} Zonda symbols")

    async def subscribe_ticks(
        self, symbols: list[str], snapshot: bool = False
    ) -> AsyncIterator[TickerUpdate]:
        """Subscribe to real-time ticker updates.

        Args:
            symbols: List of symbols or ['*'] for all.
            snapshot: Request initial snapshot before streaming.

        Yields:
            TickerUpdate for each price change.

        Raises:
            ValueError: If wildcard used with snapshot=True.
        """
        if snapshot and symbols == ["*"]:
            raise ValueError(
                "Zonda proxy API does not support wildcard snapshot. "
                "Use snapshot=True only with specific symbols like ['BTC-PLN', 'ETH-PLN']. "
                "For wildcard, use snapshot=False and subscribe with ['*']."
            )
        self._running = True
        self._subscribed_symbols = symbols
        while self._running:
            try:
                await self.connect()
                assert self._ws is not None
                await self._resubscribe()
                if snapshot:
                    await self._request_snapshots(symbols)
                if not self._message_handler_task or self._message_handler_task.done():
                    self._message_handler_task = asyncio.create_task(self._message_handler())
                while self._running:
                    try:
                        ticker_data = await asyncio.wait_for(self._tick_queue.get(), timeout=1.0)
                        yield ticker_data
                    except TimeoutError:
                        await asyncio.sleep(0.01)
            except ConnectionClosed as e:
                logger.warning(f"Connection closed by CloudFlare proxy: {e}. Reconnecting...")
                if self._message_handler_task and not self._message_handler_task.done():
                    self._message_handler_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await self._message_handler_task
                self._ws = None
                self._seq_no.clear()
                await asyncio.sleep(self._reconnect_delay)
            except Exception as e:
                logger.error(f"Zonda WebSocket subscription error: {e}")
                if self._running:
                    logger.info(f"Attempting reconnect in {self._reconnect_delay}s...")
                    await asyncio.sleep(self._reconnect_delay)
                else:
                    raise

    async def _message_handler(self) -> None:
        assert self._ws is not None
        try:
            async for message in self._ws:
                try:
                    data = json.loads(message)
                    action = data.get("action", "")
                    if action == "push":
                        topic = data.get("topic", "")
                        seq_no = data.get("seqNo")
                        if seq_no is not None and topic:
                            last_seq = self._seq_no.get(topic, 0)
                            if seq_no <= last_seq:
                                logger.warning(
                                    f"Out-of-order seqNo for {topic}: "
                                    f"received {seq_no}, expected > {last_seq}"
                                )
                                continue
                            self._seq_no[topic] = seq_no
                        if topic.startswith("trading/ticker"):
                            await self._parse_ticker_message(data)
                        elif topic.startswith("trading/stats"):
                            await self._parse_stats_message(data)
                        elif topic.startswith("trading/transactions/"):
                            await self._parse_transactions_message(data)
                        elif topic.startswith("trading/history/transactions"):
                            await self._parse_executions_message(data)
                    elif action == "subscribe-public-confirm":
                        module = data.get("module", "unknown")
                        path = data.get("path", "unknown")
                        logger.debug(f"Subscription confirmed: {module}/{path}")
                    elif action == "subscribe-private-confirm":
                        module = data.get("module", "unknown")
                        path = data.get("path", "unknown")
                        logger.debug(f"Private subscription confirmed: {module}/{path}")
                    elif action == "subscribe-public-error":
                        logger.error(f"Subscription error: {data}")
                    elif action == "subscribe-private-error":
                        logger.error(f"Private subscription error: {data}")
                    elif action == "proxy-response":
                        await self._parse_proxy_response(data)
                    elif action == "pong":
                        logger.debug("Pong received from server")
                    elif action == "json-error":
                        logger.error(f"JSON error from server: {data}")
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse Zonda message: {e}")
                except Exception as e:
                    logger.error(f"Error handling Zonda message: {e}")
        except ConnectionClosed as e:
            logger.warning(f"Connection closed in message handler: {e}")
            return
        except Exception as e:
            logger.error(f"Zonda message handler error: {e}")
            return

    async def _parse_ticker_message(self, data: dict[str, Any]) -> None:
        try:
            msg = ZondaTickerMessage.model_validate(data)
            symbol = msg.extract_symbol()
            if not symbol:
                logger.warning(f"Could not extract symbol from ticker message: {data}")
                return
            self._ticker_cache[symbol] = {
                "bid": msg.message.bid,
                "ask": msg.message.ask,
                "last": msg.message.last,
            }
            logger.debug(
                f"Zonda ticker: {symbol} bid={msg.message.bid:.2f} ask={msg.message.ask:.2f}"
            )
            await self._try_merge_and_emit(symbol)
        except (ValueError, KeyError) as e:
            logger.warning(f"Failed to parse Zonda ticker data: {e}, data={data}")

    async def _parse_stats_message(self, data: dict[str, Any]) -> None:
        try:
            msg = ZondaStatsMessage.model_validate(data)
            if not msg.message:
                return
            for stats_data in msg.message:
                symbol = stats_data.symbol
                if not symbol:
                    continue
                self._stats_cache[symbol] = {
                    "high": stats_data.high,
                    "low": stats_data.low,
                    "volume": stats_data.volume,
                    "rate_24h": stats_data.rate_24h,
                }
                logger.debug(
                    f"Zonda stats: {symbol} h={stats_data.high:.2f} "
                    f"l={stats_data.low:.2f} "
                    f"v={stats_data.volume:.2f}"
                )
                await self._try_merge_and_emit(symbol)
        except (ValueError, KeyError) as e:
            logger.warning(f"Failed to parse Zonda stats data: {e}, data={data}")

    async def _request_snapshots(self, symbols: list[str]) -> None:
        assert self._ws is not None
        if symbols == ["*"]:
            request_id_ticker = str(uuid.uuid4())
            request_id_stats = str(uuid.uuid4())
            self._pending_snapshots[request_id_ticker] = "ticker"
            self._pending_snapshots[request_id_stats] = "stats"
            ticker_msg = {
                "requestId": request_id_ticker,
                "action": "proxy",
                "module": "trading",
                "path": "ticker",
            }
            await self._ws.send(json.dumps(ticker_msg))
            stats_msg = {
                "requestId": request_id_stats,
                "action": "proxy",
                "module": "trading",
                "path": "stats",
            }
            await self._ws.send(json.dumps(stats_msg))
            logger.info("Requested snapshots for ALL markets (wildcard)")
        else:
            logger.info(f"Requesting snapshots for {len(symbols)} symbols...")
            for symbol in symbols:
                symbol_lower = symbol.lower()
                request_id_ticker = str(uuid.uuid4())
                request_id_stats = str(uuid.uuid4())
                self._pending_snapshots[request_id_ticker] = "ticker"
                self._pending_snapshots[request_id_stats] = "stats"
                ticker_msg = {
                    "requestId": request_id_ticker,
                    "action": "proxy",
                    "module": "trading",
                    "path": f"ticker/{symbol_lower}",
                }
                await self._ws.send(json.dumps(ticker_msg))
                stats_msg = {
                    "requestId": request_id_stats,
                    "action": "proxy",
                    "module": "trading",
                    "path": f"stats/{symbol_lower}",
                }
                await self._ws.send(json.dumps(stats_msg))
                await asyncio.sleep(0.01)
                logger.debug(f"Requested snapshots for {symbol}")

    async def _parse_proxy_response(self, data: dict[str, Any]) -> None:
        request_id = data.get("requestId")
        status_code = data.get("statusCode")
        body = data.get("body", {})
        if not request_id or request_id not in self._pending_snapshots:
            logger.warning(f"Received unexpected proxy response: {request_id}")
            return
        snapshot_type = self._pending_snapshots.pop(request_id)
        if status_code != 200:
            logger.error(f"Proxy request failed: {status_code}, body={body}")
            return
        if body.get("status") != "Ok":
            logger.warning(f"Proxy response not OK: {body}")
            return
        if snapshot_type == "ticker":
            ticker_data = body.get("ticker")
            if ticker_data:
                market_code = ticker_data.get("market", {}).get("code", "")
                push_data = {
                    "action": "push",
                    "topic": (
                        f"trading/ticker/{market_code.lower()}" if market_code else "trading/ticker"
                    ),
                    "message": ticker_data,
                }
                await self._parse_ticker_message(push_data)
                logger.debug(f"Parsed ticker snapshot: {market_code}")
        elif snapshot_type == "stats":
            stats_data = body.get("stats")
            if stats_data:
                stats_array = [stats_data] if isinstance(stats_data, dict) else stats_data
                push_data = {
                    "action": "push",
                    "topic": "trading/stats",
                    "message": stats_array,
                }
                await self._parse_stats_message(push_data)
                logger.debug(f"Parsed stats snapshot: {len(stats_array)} markets")

    async def _try_merge_and_emit(self, symbol: str) -> None:
        """Merge ticker and stats caches and emit combined update."""
        if symbol not in self._ticker_cache:
            return
        ticker = self._ticker_cache[symbol]
        stats = self._stats_cache.get(symbol, {})
        ticker_data = create_ticker_update_from_caches(symbol, ticker, stats)
        await self._tick_queue.put(ticker_data)
        if stats:
            logger.debug(
                f"Zonda merged: {symbol} bid={ticker_data.bid:.2f} "
                f"ask={ticker_data.ask:.2f} (with stats)"
            )
        else:
            logger.debug(
                f"Zonda ticker-only: {symbol} bid={ticker_data.bid:.2f} "
                f"ask={ticker_data.ask:.2f} (no stats yet)"
            )

    async def _parse_transactions_message(self, data: dict[str, Any]) -> None:
        try:
            msg = ZondaTransactionsMessage.model_validate(data)
            symbol = msg.extract_symbol()
            if not symbol:
                logger.warning(f"Could not extract symbol from transactions message: {data}")
                return
            for tx in msg.message.transactions:
                trade_data = tx.to_trade_update(symbol)
                await self._trade_queue.put(trade_data)
                logger.debug(
                    f"Zonda trade: {symbol} {tx.side} {tx.amount:.6f} @ {tx.price:.2f} "
                    f"(id: {tx.id[:8]}...)"
                )
        except (ValueError, KeyError) as e:
            logger.warning(f"Failed to parse Zonda transactions data: {e}, data={data}")

    def _sign_private_message(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not self.api_key or not self.api_secret:
            raise RuntimeError("API credentials required for private WebSocket subscriptions")
        timestamp = int(time.time())
        message = self.api_key + str(timestamp)
        signature = hmac.new(
            self.api_secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha512
        ).hexdigest()
        signed_payload = payload.copy()
        signed_payload["publicKey"] = self.api_key
        signed_payload["hashSignature"] = signature
        signed_payload["requestTimestamp"] = timestamp
        return signed_payload

    async def _parse_executions_message(self, data: dict[str, Any]) -> None:
        try:
            msg = ZondaExecutionsMessage.model_validate(data)
            for exec_data in msg.message.history:
                execution = exec_data.to_execution_update()
                await self._execution_queue.put(execution)
                logger.debug(
                    f"Zonda execution: {exec_data.market} {exec_data.user_action.lower()} "
                    f"{exec_data.quantity:.6f} @ {exec_data.price:.2f} "
                    f"(order: {exec_data.offer_id}, taker: {exec_data.was_taker})"
                )
        except (ValueError, KeyError) as e:
            logger.warning(f"Failed to parse Zonda executions data: {e}, data={data}")

    async def get_ticker(self, symbol: str) -> TickerSnapshot:
        """Fetch current ticker for a symbol from Zonda.

        Args:
            symbol: Trading pair symbol in native format.

        Returns:
            Current ticker snapshot with bid/ask/last.

        Raises:
            Exception: If API request fails.
        """
        try:
            ccxt_symbol = native_to_ccxt(symbol)
            ticker_data = await asyncio.to_thread(self._ccxt_client.fetch_ticker, ccxt_symbol)
            return TickerSnapshot(
                symbol=symbol,
                bid=float(ticker_data["bid"]) if ticker_data["bid"] else 0.0,
                ask=float(ticker_data["ask"]) if ticker_data["ask"] else 0.0,
                last=float(ticker_data["last"]) if ticker_data["last"] else 0.0,
                timestamp=(
                    float(ticker_data["timestamp"]) / 1000.0 if ticker_data["timestamp"] else 0.0
                ),
            )
        except Exception as e:
            logger.error(f"Failed to get ticker for {symbol}: {e}")
            raise

    async def get_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1m",
        since: int | None = None,
        limit: int | None = None,
    ) -> list[OhlcvSnapshot]:
        """Fetch OHLCV candles via REST API.

        Args:
            symbol: Trading pair.
            timeframe: Candle interval.
            since: Start timestamp in milliseconds.
            limit: Maximum candles to return.

        Returns:
            List of OHLCV snapshots.

        Raises:
            Exception: If OHLCV fetch fails.
        """
        try:
            ccxt_symbol = native_to_ccxt(symbol)
            ohlcv_data = await asyncio.to_thread(
                self._ccxt_client.fetch_ohlcv,
                ccxt_symbol,
                timeframe,
                since,
                limit,
            )
            return [
                OhlcvSnapshot(
                    timestamp=float(candle[0]) / 1000.0,
                    open=float(candle[1]),
                    high=float(candle[2]),
                    low=float(candle[3]),
                    close=float(candle[4]),
                    volume=float(candle[5]),
                )
                for candle in ohlcv_data
            ]
        except Exception as e:
            logger.error(f"Failed to get OhlcvSnapshot for {symbol}: {e}")
            raise

    async def create_order(self, request: ExchangeOrderRequest) -> ExchangeOrderSnapshot:
        """Create a new order on Zonda.

        Args:
            request: Order parameters.

        Returns:
            Created order snapshot.

        Raises:
            RuntimeError: If API credentials are missing.
            Exception: If order creation fails.
        """
        if not self.api_key or not self.api_secret:
            raise RuntimeError("API credentials required for trading")
        try:
            ccxt_symbol = native_to_ccxt(request.symbol)
            order_params: dict[str, Any] = {}
            if request.client_order_id:
                order_params["clientOrderId"] = request.client_order_id
            ccxt_order = await asyncio.to_thread(
                self._ccxt_client.create_order,
                ccxt_symbol,
                request.type.value,
                request.side.value,
                request.amount,
                request.price,
                order_params,
            )
            order = ExchangeOrderSnapshot(
                id=ccxt_order["id"],
                client_order_id=ccxt_order.get("clientOrderId"),
                symbol=request.symbol,
                side=OrderSideEnum(ccxt_order["side"]),
                type=OrderTypeEnum(ccxt_order["type"]),
                amount=float(ccxt_order["amount"]),
                price=float(ccxt_order["price"]) if ccxt_order.get("price") else None,
                status=OrderStatusEnum(ccxt_order["status"]),
                filled=float(ccxt_order.get("filled", 0)),
                remaining=float(ccxt_order.get("remaining", 0)),
                timestamp=float(ccxt_order["timestamp"]) / 1000.0,
                fee=float(ccxt_order["fee"]["cost"]) if ccxt_order.get("fee") else None,
            )
            await self._log_order_to_db(request, order)
            return order
        except Exception as e:
            logger.error(f"Failed to create order: {e}")
            raise

    async def cancel_order(self, order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        """Cancel an existing order.

        Args:
            order_id: Exchange order ID.
            symbol: Trading pair (optional).

        Returns:
            Cancelled order snapshot.

        Raises:
            RuntimeError: If API credentials are missing.
            Exception: If cancellation fails.
        """
        if not self.api_key or not self.api_secret:
            raise RuntimeError("API credentials required for trading")
        try:
            ccxt_symbol = native_to_ccxt(symbol) if symbol else None
            ccxt_order = await asyncio.to_thread(
                self._ccxt_client.cancel_order, order_id, ccxt_symbol
            )
            return ExchangeOrderSnapshot(
                id=ccxt_order["id"],
                client_order_id=ccxt_order.get("clientOrderId"),
                symbol=symbol or ccxt_to_native(ccxt_order["symbol"]),
                side=OrderSideEnum(ccxt_order["side"]),
                type=OrderTypeEnum(ccxt_order["type"]),
                amount=float(ccxt_order["amount"]),
                price=float(ccxt_order["price"]) if ccxt_order.get("price") else None,
                status=OrderStatusEnum(ccxt_order["status"]),
                filled=float(ccxt_order.get("filled", 0)),
                remaining=float(ccxt_order.get("remaining", 0)),
                timestamp=float(ccxt_order["timestamp"]) / 1000.0,
                fee=float(ccxt_order["fee"]["cost"]) if ccxt_order.get("fee") else None,
            )
        except Exception as e:
            logger.error(f"Failed to cancel order {order_id}: {e}")
            raise

    async def get_order(self, order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        """Fetch order details by ID.

        Args:
            order_id: Exchange order ID.
            symbol: Trading pair (optional).

        Returns:
            Order snapshot.

        Raises:
            RuntimeError: If API credentials are missing.
            Exception: If order fetch fails.
        """
        if not self.api_key or not self.api_secret:
            raise RuntimeError("API credentials required for trading")
        try:
            ccxt_symbol = native_to_ccxt(symbol) if symbol else None
            ccxt_order = await asyncio.to_thread(
                self._ccxt_client.fetch_order, order_id, ccxt_symbol
            )
            return ExchangeOrderSnapshot(
                id=ccxt_order["id"],
                client_order_id=ccxt_order.get("clientOrderId"),
                symbol=symbol or ccxt_to_native(ccxt_order["symbol"]),
                side=OrderSideEnum(ccxt_order["side"]),
                type=OrderTypeEnum(ccxt_order["type"]),
                amount=float(ccxt_order["amount"]),
                price=float(ccxt_order["price"]) if ccxt_order.get("price") else None,
                status=OrderStatusEnum(ccxt_order["status"]),
                filled=float(ccxt_order.get("filled", 0)),
                remaining=float(ccxt_order.get("remaining", 0)),
                timestamp=float(ccxt_order["timestamp"]) / 1000.0,
                fee=float(ccxt_order["fee"]["cost"]) if ccxt_order.get("fee") else None,
            )
        except Exception as e:
            logger.error(f"Failed to get order {order_id}: {e}")
            raise

    async def get_orders(
        self,
        symbol: str | None = None,
        status: OrderStatusEnum | None = None,
        limit: int | None = None,
    ) -> list[ExchangeOrderSnapshot]:
        """Fetch orders with optional filtering.

        Args:
            symbol: Filter by trading pair.
            status: Filter by order status.
            limit: Maximum orders to return.

        Returns:
            List of order snapshots.

        Raises:
            RuntimeError: If API credentials are missing.
            Exception: If orders fetch fails.
        """
        if not self.api_key or not self.api_secret:
            raise RuntimeError("API credentials required for trading")
        try:
            ccxt_symbol = native_to_ccxt(symbol) if symbol else None
            if status == OrderStatusEnum.OPEN:
                ccxt_orders = await asyncio.to_thread(
                    self._ccxt_client.fetch_open_orders, ccxt_symbol, None, limit
                )
            elif status == OrderStatusEnum.CLOSED:
                ccxt_orders = await asyncio.to_thread(
                    self._ccxt_client.fetch_closed_orders, ccxt_symbol, None, limit
                )
            else:
                ccxt_orders = await asyncio.to_thread(
                    self._ccxt_client.fetch_orders, ccxt_symbol, None, limit
                )
            return [
                ExchangeOrderSnapshot(
                    id=o["id"],
                    client_order_id=o.get("clientOrderId"),
                    symbol=symbol or ccxt_to_native(o["symbol"]),
                    side=OrderSideEnum(o["side"]),
                    type=OrderTypeEnum(o["type"]),
                    amount=float(o["amount"]),
                    price=float(o["price"]) if o.get("price") else None,
                    status=OrderStatusEnum(o["status"]),
                    filled=float(o.get("filled", 0)),
                    remaining=float(o.get("remaining", 0)),
                    timestamp=float(o["timestamp"]) / 1000.0,
                    fee=float(o["fee"]["cost"]) if o.get("fee") else None,
                )
                for o in ccxt_orders
            ]
        except Exception as e:
            logger.error(f"Failed to get orders: {e}")
            raise

    async def get_balance(self, currency: str | None = None) -> dict[str, AccountBalance]:
        """Fetch account balances.

        Args:
            currency: Filter by specific currency.

        Returns:
            Dictionary of currency to balance info.

        Raises:
            RuntimeError: If API credentials are missing.
            Exception: If balance fetch fails.
        """
        if not self.api_key or not self.api_secret:
            raise RuntimeError("API credentials required for trading")
        try:
            ccxt_balance = await asyncio.to_thread(self._ccxt_client.fetch_balance)
            balances: dict[str, AccountBalance] = {}
            for curr, bal in ccxt_balance.items():
                if curr in ("info", "free", "used", "total", "timestamp", "datetime"):
                    continue
                if currency and curr != currency:
                    continue
                balances[curr] = AccountBalance(
                    currency=curr,
                    free=float(bal.get("free", 0)),
                    used=float(bal.get("used", 0)),
                    total=float(bal.get("total", 0)),
                )
            return balances
        except Exception as e:
            logger.error(f"Failed to get balance: {e}")
            raise

    async def subscribe_trades(self, symbols: list[str]) -> AsyncIterator[TradeUpdate]:
        """Subscribe to real-time trade updates.

        Args:
            symbols: List of specific symbols (wildcard not supported).

        Yields:
            TradeUpdate for each trade.

        Raises:
            ValueError: If wildcard subscription attempted.
        """
        if symbols == ["*"]:
            raise ValueError(
                "Zonda wildcard subscription for trades is not functional. "
                "Server accepts subscription but never pushes trade data. "
                "Please provide specific symbols like ['BTC-PLN', 'ETH-EUR']"
            )
        self._running = True
        self._subscribed_trade_symbols = symbols
        while self._running:
            try:
                await self.connect()
                assert self._ws is not None
                for symbol in symbols:
                    symbol_lower = symbol.lower()
                    msg = {
                        "action": "subscribe-public",
                        "module": "trading",
                        "path": f"transactions/{symbol_lower}",
                    }
                    await self._ws.send(json.dumps(msg))
                    logger.debug(f"Subscribed to Zonda transactions: {symbol}")
                if not self._message_handler_task or self._message_handler_task.done():
                    self._message_handler_task = asyncio.create_task(self._message_handler())
                while self._running:
                    try:
                        trade_data = await asyncio.wait_for(self._trade_queue.get(), timeout=1.0)
                        yield trade_data
                    except TimeoutError:
                        await asyncio.sleep(0.01)
            except ConnectionClosed as e:
                logger.warning(f"Trades connection closed: {e}. Reconnecting...")
                if self._message_handler_task and not self._message_handler_task.done():
                    self._message_handler_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await self._message_handler_task
                self._ws = None
                self._seq_no.clear()
                await asyncio.sleep(self._reconnect_delay)
            except Exception as e:
                logger.error(f"Zonda trades subscription error: {e}")
                if self._running:
                    await asyncio.sleep(self._reconnect_delay)
                else:
                    raise

    async def subscribe_candles(
        self,
        symbols: list[str],
        timeframe: str = "1m",
    ) -> AsyncIterator[CandleUpdate]:
        """Subscribe to 1-minute candles built from trades.

        Args:
            symbols: List of symbols (wildcard not supported).
            timeframe: Must be '1m' (only supported interval).

        Yields:
            CandleUpdate for each completed candle.

        Raises:
            ValueError: If timeframe is not '1m' or wildcard used.
        """
        if timeframe != "1m":
            raise ValueError(
                f"Zonda only supports 1m candles (built from trades). "
                f"Got timeframe: {timeframe}. Use REST API get_ohlcv() for other timeframes."
            )
        if symbols == ["*"]:
            raise ValueError(
                "Zonda does not support wildcard subscription for candles. "
                "Please provide specific symbols like ['BTC-PLN', 'ETH-EUR']"
            )
        self._running = True
        self._subscribed_candle_symbols = symbols
        if not self._candle_aggregator_task or self._candle_aggregator_task.done():
            self._candle_aggregator_task = asyncio.create_task(self._candle_aggregator(symbols))
        try:
            while self._running:
                try:
                    candle_data = await asyncio.wait_for(self._candle_queue.get(), timeout=1.0)
                    yield candle_data
                except TimeoutError:
                    await asyncio.sleep(0.01)
        finally:
            if self._candle_aggregator_task and not self._candle_aggregator_task.done():
                self._candle_aggregator_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._candle_aggregator_task

    async def _candle_aggregator(self, symbols: list[str]) -> None:
        async for trade in self.subscribe_trades(symbols):
            minute_ts = int(trade.timestamp.replace(second=0, microsecond=0).timestamp())
            candle_key = f"{trade.symbol}_{minute_ts}"
            if candle_key not in self._candle_builder:
                self._candle_builder[candle_key] = {
                    "symbol": trade.symbol,
                    "open": trade.price,
                    "high": trade.price,
                    "low": trade.price,
                    "close": trade.price,
                    "volume": trade.quantity,
                    "trades": 1,
                    "vwap_sum": trade.price * trade.quantity,
                    "interval_begin": trade.timestamp.replace(second=0, microsecond=0),
                }
            else:
                candle = self._candle_builder[candle_key]
                candle["high"] = max(candle["high"], trade.price)
                candle["low"] = min(candle["low"], trade.price)
                candle["close"] = trade.price
                candle["volume"] += trade.quantity
                candle["trades"] += 1
                candle["vwap_sum"] += trade.price * trade.quantity
            current_minute = int(datetime.now(tz=UTC).replace(second=0, microsecond=0).timestamp())
            for key, candle in list(self._candle_builder.items()):
                candle_minute = int(candle["interval_begin"].timestamp())
                if candle_minute < current_minute:
                    vwap = candle["vwap_sum"] / candle["volume"] if candle["volume"] > 0 else 0.0
                    candle_data = CandleUpdate(
                        symbol=candle["symbol"],
                        open=candle["open"],
                        high=candle["high"],
                        low=candle["low"],
                        close=candle["close"],
                        vwap=vwap,
                        trades=candle["trades"],
                        volume=candle["volume"],
                        interval_begin=candle["interval_begin"],
                        interval=1,
                    )
                    await self._candle_queue.put(candle_data)
                    del self._candle_builder[key]
                    logger.debug(
                        f"Emitted 1m candle for {candle['symbol']} at {candle['interval_begin']}"
                    )

    async def subscribe_executions(self) -> AsyncIterator[ExecutionUpdate]:
        """Subscribe to user execution reports (private channel).

        Yields:
            ExecutionUpdate for each order fill.

        Raises:
            RuntimeError: If API credentials are missing.
        """
        if not self.api_key or not self.api_secret:
            raise RuntimeError(
                "API credentials required for subscribe_executions(). "
                "Provide api_key and api_secret in ZondaExchangeClient constructor."
            )
        self._running = True
        while self._running:
            try:
                await self.connect()
                assert self._ws is not None
                payload = {
                    "action": "subscribe-private",
                    "module": "trading",
                    "path": "history/transactions",
                }
                signed_msg = self._sign_private_message(payload)
                await self._ws.send(json.dumps(signed_msg))
                logger.debug(
                    "Subscribed to Zonda private executions (trading/history/transactions)"
                )
                if not self._message_handler_task or self._message_handler_task.done():
                    self._message_handler_task = asyncio.create_task(self._message_handler())
                while self._running:
                    try:
                        execution_data = await asyncio.wait_for(
                            self._execution_queue.get(), timeout=1.0
                        )
                        yield execution_data
                    except TimeoutError:
                        await asyncio.sleep(0.01)
            except ConnectionClosed as e:
                logger.warning(f"Executions connection closed: {e}. Reconnecting...")
                if self._message_handler_task and not self._message_handler_task.done():
                    self._message_handler_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await self._message_handler_task
                self._ws = None
                self._seq_no.clear()
                await asyncio.sleep(self._reconnect_delay)
            except Exception as e:
                logger.error(f"Zonda executions subscription error: {e}")
                if self._running:
                    await asyncio.sleep(self._reconnect_delay)
                else:
                    raise

    async def subscribe_instruments(self, **kwargs: Any) -> AsyncIterator[dict[str, Any]]:
        """Fetch instrument information via CCXT.

        Args:
            **kwargs: Ignored parameters.

        Yields:
            Dictionary with market info for each instrument.

        Raises:
            RuntimeError: If market loading fails.
        """
        try:
            markets = await asyncio.to_thread(self._ccxt_client.load_markets)
            if not isinstance(markets, dict):
                raise RuntimeError(f"Unexpected markets type: {type(markets)}")
            for market_data in markets.values():
                if isinstance(market_data, dict):
                    yield market_data
            logger.info(f"Yielded {len(markets)} instruments from CCXT load_markets()")
        except Exception as e:
            logger.error(f"Zonda instruments subscription error: {e}")
            raise
