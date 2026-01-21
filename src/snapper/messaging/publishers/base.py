"""Base class for market data publisher services.

Provides common functionality for ZeroMQ-based publishers that stream
real-time market data from exchanges.
"""

import asyncio
from abc import ABC
from abc import abstractmethod
from datetime import UTC
from datetime import datetime
from typing import Any

import zmq
import zmq.asyncio
from loguru import logger

from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.services.settings import SettingsService
from snapper.config.settings import get_settings
from snapper.config.settings import get_settings_service
from snapper.config.settings import get_settings_with_service
from snapper.data.repository import get_repository
from snapper.infrastructure.exchanges.base import ExchangeClientBase
from snapper.infrastructure.symbols.mapper import SymbolMapperService
from snapper.messaging.infrastructure.validated_socket import ValidatedPublisher
from snapper.messaging.infrastructure.validated_socket import ValidatedSubscriber
from snapper.messaging.schemas.messages import BarEnvelope
from snapper.messaging.schemas.messages import HeartbeatEnvelope
from snapper.messaging.schemas.messages import MarketDataEnvelope
from snapper.messaging.schemas.messages import SettingChangedEnvelope
from snapper.messaging.schemas.messages import TickEnvelope
from snapper.messaging.schemas.messages import TradeEnvelope
from snapper.utils.logging import set_log_context


class MarketDataPublisherService[T: ExchangeClientBase](RegisterableProcess, ABC):
    """Base service for publishing market data via ZMQ messaging."""

    def __init__(self, symbols: list[str]) -> None:
        """Initialize the instance.

        Args:
            symbols: List of trading symbols to subscribe to.
        """
        self.settings = get_settings()
        self._input_symbols = symbols
        self.symbols = self._validate_symbols(symbols)
        if not self.symbols:
            logger.warning(
                f"{self.__class__.__name__}: No valid symbols provided; "
                "publisher will idle until updated"
            )
        self.pub_endpoint = self.settings.zmq_broker_xsub
        self.context: zmq.asyncio.Context | None = None
        self.publisher: ValidatedPublisher | None = None
        self.subscriber: ValidatedSubscriber | None = None
        self.running = False
        self.heartbeat_seq = 0
        self._last_data_timestamps: dict[str, float] = {}
        self._unknown_symbols_logged: set[str] = set()
        self.repository: Any = None
        self._instrument_cache: dict[str, int] = {}
        self._exchange_client: T | None = None

    @abstractmethod
    def _create_exchange_client(self) -> T:
        """Create and return the exchange client instance.

        Returns:
            Configured exchange client for the specific exchange.
        """
        ...

    @abstractmethod
    def _get_exchange_name(self) -> str:
        """Return the name identifier for the exchange.

        Returns:
            Exchange name string used in topics and logging.
        """
        ...

    @abstractmethod
    def _validate_symbols(self, symbols: list[str]) -> list[str]:
        """Validate and filter symbols for the exchange.

        Args:
            symbols: List of symbols to validate.

        Returns:
            List of valid symbols accepted by the exchange.
        """
        ...

    async def _invalidate_symbol_cache(self) -> None:
        """Trigger cache invalidation for symbol mapper."""
        SymbolMapperService.get_instance().trigger_cache_invalidation(fail_fast=False)

    async def start(self) -> None:
        """Start the publisher service and connect to exchange."""
        exchange_name = self._get_exchange_name()
        process_name = f"pub:{exchange_name}"
        set_log_context(process_name)
        if self.running:
            logger.warning(f"{process_name}: Already running")
            return
        bootstrap_settings = get_settings()
        settings_service = await get_settings_service(
            bootstrap_settings.db_url,
            bootstrap_settings.zmq_broker_xpub,
            bootstrap_settings.master_password,
            bootstrap_settings.encryption_salt,
        )
        self.settings = get_settings_with_service(settings_service)
        logger.info(f"{process_name}: AppSettings initialized with database access")
        self.repository = get_repository(self.settings.db_url)
        max_symbols = self._get_max_symbols_per_connection()
        if len(self.symbols) > max_symbols > 0:
            logger.warning(
                f"{process_name} has {len(self.symbols)} symbols, but "
                f"{exchange_name} WebSocket limit is {max_symbols} symbols per connection. "
                f"Consider running multiple instances (first {max_symbols} symbols will be used)."
            )
        self.context = zmq.asyncio.Context()
        raw_pub_socket = self.context.socket(zmq.PUB)
        raw_pub_socket.connect(self.pub_endpoint)
        self.publisher = ValidatedPublisher(raw_pub_socket)
        logger.info(f"{process_name}: Connected to broker: {self.pub_endpoint}")
        raw_sub_socket = self.context.socket(zmq.SUB)
        raw_sub_socket.connect(self.settings.zmq_broker_xpub)
        self.subscriber = ValidatedSubscriber(raw_sub_socket)
        self.subscriber.subscribe("system.symbol_mappings")
        self.subscriber.subscribe("system.settings")
        logger.info(
            f"{process_name}: Subscribed to system.symbol_mappings, system.settings from "
            f"{self.settings.zmq_broker_xpub}"
        )
        self._exchange_client = self._create_exchange_client()
        await self._exchange_client.connect()
        logger.info(f"{process_name}: Exchange client connected (anonymous, public data)")
        self.running = True
        tasks: list[asyncio.Task[Any]] = []
        tasks.append(asyncio.create_task(self._heartbeat_loop()))
        tasks.append(asyncio.create_task(self._symbol_mappings_loop()))
        symbols_to_subscribe = self.symbols[:max_symbols] if max_symbols > 0 else self.symbols
        timeframes = self.settings.timeframes
        tasks.extend(
            asyncio.create_task(self._candle_loop(symbols_to_subscribe, timeframe))
            for timeframe in timeframes
        )
        tasks.append(asyncio.create_task(self._tick_loop(symbols_to_subscribe)))
        tasks.append(asyncio.create_task(self._trade_loop(symbols_to_subscribe)))
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info(f"{process_name}: Tasks cancelled")

    async def stop(self) -> None:
        """Stop the publisher service and disconnect from exchange."""
        if not self.running:
            return
        self.running = False
        if self._exchange_client:
            await self._exchange_client.disconnect()
            self._exchange_client = None
        if self.publisher:
            self.publisher.setsockopt(zmq.LINGER, 0)
            self.publisher.close()
        if self.subscriber:
            self.subscriber.setsockopt(zmq.LINGER, 0)
            self.subscriber.close()
        if self.context:
            self.context.term()
        exchange_name = self._get_exchange_name()
        logger.info(f"{exchange_name}_feed_publisher: Stopped")

    def _get_max_symbols_per_connection(self) -> int:
        """Return maximum symbols per WebSocket connection (0 = unlimited).

        Returns:
            Maximum number of symbols allowed per connection, or 0 for unlimited.
        """
        return 0

    async def _candle_loop(self, symbols: list[str], timeframe: str) -> None:
        """Subscribe to candle data and publish to ZMQ.

        Args:
            symbols: List of symbols to subscribe to for candle data.
            timeframe: Candle timeframe interval (e.g., '1m', '5m', '1h').
        """
        if not self._exchange_client:
            logger.error("Exchange client not initialized")
            return
        exchange = self._get_exchange_name()
        try:
            async for candle in self._exchange_client.subscribe_candles(symbols, timeframe):
                if not self.running:
                    break
                native_symbol = candle.symbol
                bar_msg = BarEnvelope(
                    exchange=exchange,
                    instrument=native_symbol,
                    volume=candle.volume,
                    timeframe=timeframe,
                    open=candle.open,
                    high=candle.high,
                    low=candle.low,
                    close=candle.close,
                    vwap=candle.vwap,
                    trades=candle.trades,
                )
                topic = f"market.{exchange}.{native_symbol}.candles.{timeframe}"
                await self._publish_message(topic, bar_msg)
                self._last_data_timestamps[native_symbol] = datetime.now(UTC).timestamp() * 1000
                await self._save_to_db(native_symbol, bar_msg)
        except Exception as e:
            logger.error(f"Candle loop error for {symbols}: {e}")

    async def _tick_loop(self, symbols: list[str]) -> None:
        """Subscribe to tick data and publish to ZMQ.

        Args:
            symbols: List of symbols to subscribe to for tick data.
        """
        if not self._exchange_client:
            logger.error("Exchange client not initialized")
            return
        exchange = self._get_exchange_name()
        try:
            async for message in self._exchange_client.subscribe_ticks(symbols):
                if not self.running:
                    break
                native_symbol = message.symbol
                tick_msg = TickEnvelope(
                    exchange=exchange,
                    instrument=native_symbol,
                    volume=message.volume,
                    bid=message.bid if message.bid != 0.0 else None,
                    ask=message.ask if message.ask != 0.0 else None,
                    last=message.last,
                )
                topic = f"market.{exchange}.{native_symbol}.ticks"
                await self._publish_message(topic, tick_msg)
                self._last_data_timestamps[native_symbol] = datetime.now(UTC).timestamp() * 1000
        except Exception as e:
            logger.error(f"Tick loop error for {symbols}: {e}")

    async def _trade_loop(self, symbols: list[str]) -> None:
        """Subscribe to trade data and publish to ZMQ.

        Args:
            symbols: List of symbols to subscribe to for trade data.
        """
        if not self._exchange_client:
            logger.error("Exchange client not initialized")
            return
        exchange = self._get_exchange_name()
        try:
            async for trade in self._exchange_client.subscribe_trades(symbols):
                if not self.running:
                    break
                native_symbol = trade.symbol
                trade_msg = TradeEnvelope(
                    exchange=exchange,
                    instrument=native_symbol,
                    price=trade.price,
                    volume=trade.quantity,
                    side=trade.side if trade.side in ["buy", "sell"] else None,
                )
                topic = f"market.{exchange}.{native_symbol}.trades"
                await self._publish_message(topic, trade_msg)
                self._last_data_timestamps[native_symbol] = datetime.now(UTC).timestamp() * 1000
        except Exception as e:
            logger.error(f"Trade loop error for {symbols}: {e}")

    async def _publish_message(self, topic: str, message: MarketDataEnvelope) -> None:
        """Publish market data message to ZMQ topic.

        Args:
            topic: ZMQ topic string for message routing.
            message: Market data envelope to publish.
        """
        if not self.publisher or not self.running:
            return
        try:
            await self.publisher.send_multipart(topic, message.to_json().encode("utf-8"))
            logger.debug(f"Published {message.type} for {topic}")
        except Exception as e:
            logger.error(f"Error publishing message: {e}")

    async def _heartbeat_loop(self) -> None:
        """Periodically publish heartbeat messages with status."""
        exchange = self._get_exchange_name()
        component_name = f"feed.{exchange}"
        while self.running:
            try:
                await asyncio.sleep(self.settings.zmq_heartbeat_interval_ms / 1000.0)
                if not self.running:
                    break
                self.heartbeat_seq += 1
                current_time = datetime.now(UTC).timestamp() * 1000
                max_lag_ms = 0
                for symbol in self.symbols:
                    last_data_time = self._last_data_timestamps.get(symbol, current_time)
                    lag_ms = int(current_time - last_data_time)
                    max_lag_ms = max(max_lag_ms, lag_ms)
                hb_msg = HeartbeatEnvelope(
                    component=component_name,
                    sequence=self.heartbeat_seq,
                    status="healthy",
                    lag_ms=max_lag_ms,
                    meta={
                        "symbols": list(self.symbols),
                        "symbol_count": len(self.symbols),
                        "running": self.running,
                    },
                )
                topic = f"system.heartbeats.{component_name}"
                await self._publish_heartbeat(topic, hb_msg)
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")

    async def _publish_heartbeat(self, topic: str, message: HeartbeatEnvelope) -> None:
        """Publish heartbeat message to ZMQ.

        Args:
            topic: ZMQ topic string for heartbeat routing.
            message: Heartbeat envelope containing status information.
        """
        if not self.publisher or not self.running:
            return
        try:
            await self.publisher.send_multipart(topic, message.to_json().encode("utf-8"))
        except Exception as e:
            logger.error(f"Error publishing heartbeat: {e}")

    async def _save_to_db(self, native_symbol: str, bar_msg: BarEnvelope) -> None:
        """Persist candle data to the database.

        Args:
            native_symbol: Native exchange symbol identifier.
            bar_msg: Bar envelope containing OHLCV data to persist.
        """
        try:
            base_currency: str
            quote_currency: str
            try:
                base_currency, quote_currency = native_symbol.split("-", 1)
            except ValueError:
                logger.warning(
                    f"MarketDataPublisherService: "
                    f"Unable to split symbol {native_symbol} for instrument"
                )
                return
            instrument_id = self._instrument_cache.get(native_symbol)
            if instrument_id is None:
                assert self.repository is not None, "Repository not initialized"
                instrument_id = await self.repository.upsert_instrument(
                    symbol=native_symbol,
                    base=base_currency,
                    quote=quote_currency,
                    tick_size=0.0,
                    lot_size=0.0,
                )
                self._instrument_cache[native_symbol] = instrument_id
            timeframe = bar_msg.timeframe or "1m"
            open_price = bar_msg.open if bar_msg.open is not None else bar_msg.close
            high_price = bar_msg.high if bar_msg.high is not None else bar_msg.close
            low_price = bar_msg.low if bar_msg.low is not None else bar_msg.close
            close_price = bar_msg.close if bar_msg.close is not None else 0.0
            vwap_price = bar_msg.vwap if bar_msg.vwap is not None else bar_msg.close
            trades = bar_msg.trades if bar_msg.trades is not None else 0
            candle_row: dict[str, Any] = {
                "instrument_id": instrument_id,
                "timestamp": bar_msg.timestamp,
                "timeframe": timeframe,
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": float(bar_msg.volume),
                "vwap": vwap_price,
                "trades": trades,
            }
            assert self.repository is not None, "Repository not initialized"
            await self.repository.upsert_candles([candle_row])
        except Exception as e:
            logger.error(f"Error saving bar to DB: {e}")

    async def _symbol_mappings_loop(self) -> None:
        """Listen for system messages and handle cache invalidation."""
        if not self.subscriber:
            logger.error("MarketDataPublisherService: No subscriber for system messages listener")
            return
        exchange = self._get_exchange_name()
        logger.info(f"{exchange}_feed_publisher: Starting system messages listener")
        try:
            while self.running:
                try:
                    topic, payload = await asyncio.wait_for(
                        self.subscriber.recv_multipart(), timeout=1.0
                    )
                    if topic == "system.symbol_mappings":
                        logger.info(
                            f"{exchange}_feed_publisher: Received symbol_mappings update, "
                            "refreshing cache"
                        )
                        await self._invalidate_symbol_cache()
                    elif topic == "system.settings":
                        await self._handle_settings_update(payload, exchange)
                except TimeoutError:
                    continue
                except Exception as e:
                    logger.error(f"{exchange}_feed_publisher system messages listener error: {e}")
                    await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"{exchange}_feed_publisher system messages loop crashed: {e}")

    async def _handle_settings_update(self, payload: bytes, exchange: str) -> None:
        """Handle settings update message and refresh cached settings.

        Args:
            payload: Raw bytes containing the settings change envelope.
            exchange: Exchange name for logging context.
        """
        try:
            envelope = SettingChangedEnvelope.from_json(payload.decode("utf-8"))
            settings_service = SettingsService.get_instance()
            if settings_service:
                parsed_value = settings_service._parse_value(envelope.value)
                settings_service._cache[envelope.key] = parsed_value
                logger.info(
                    f"{exchange}_feed_publisher: Setting {envelope.key} updated via ZMQ event"
                )
        except Exception as e:
            logger.error(f"{exchange}_feed_publisher: Error handling settings update: {e}")

    def get_status(self) -> dict[str, Any]:
        """Return the current status of the publisher service.

        Returns:
            Dictionary containing running state, symbols, endpoint, and metrics.
        """
        return {
            "running": self.running,
            "symbols": self.symbols,
            "broker_endpoint": self.pub_endpoint,
            "heartbeat_seq": self.heartbeat_seq,
            "exchange": self._get_exchange_name(),
        }
