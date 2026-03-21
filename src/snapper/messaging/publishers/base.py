"""Base class for market data publisher services.

Provides common functionality for ZeroMQ-based publishers that stream
real-time market data from exchanges.
"""

import asyncio
import math
from abc import ABC
from abc import abstractmethod
from datetime import UTC
from datetime import datetime
from typing import Any
from typing import cast
from uuid import uuid7

import zmq
import zmq.asyncio
from loguru import logger

from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.services.settings import SettingsService
from snapper.config.settings import get_settings
from snapper.config.settings import get_settings_service
from snapper.config.settings import get_settings_with_service
from snapper.core.types import AllExchange
from snapper.core.types import MarketDataExchange
from snapper.core.types import MarketDataType
from snapper.core.types import OrderExchange
from snapper.data.repository import Repository
from snapper.data.repository import get_repository
from snapper.infrastructure.exchanges.base import ExchangeClientBase
from snapper.infrastructure.symbols.functions import resolve_symbol_public_id
from snapper.infrastructure.symbols.mapper import SymbolMapperService
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.infrastructure.validated_socket import HWM_MARKET_DATA
from snapper.messaging.infrastructure.validated_socket import ValidatedPublisher
from snapper.messaging.infrastructure.validated_socket import ValidatedSubscriber
from snapper.messaging.infrastructure.validated_socket import apply_hwm
from snapper.messaging.schemas.data import CandleData
from snapper.messaging.schemas.data import HeartbeatData
from snapper.messaging.schemas.data import SettingChangedData
from snapper.messaging.schemas.data import TickData
from snapper.messaging.schemas.data import TradeData
from snapper.messaging.schemas.messages import MarketDataMessage
from snapper.messaging.topics.builders import heartbeat_topic_from_component
from snapper.messaging.topics.builders import market_topic
from snapper.utils.logging import set_log_context

_EXCHANGE_NOT_INIT_MSG = "Exchange client not initialized"


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
        self.msg_publisher: MessagePublisher | None = None
        self._tracker: SequenceTracker = SequenceTracker()
        self.subscriber: ValidatedSubscriber | None = None
        self.running = False
        self.heartbeat_seq = 0
        self._last_data_timestamps: dict[str, float] = {}
        self._unknown_symbols_logged: set[str] = set()
        self.repository: Repository | None = None
        self._instrument_cache: dict[str, int] = {}
        self._candle_id_cache: dict[tuple[int, str], tuple[datetime, str]] = {}
        self._exchange_client: T | None = None

    @abstractmethod
    def _create_exchange_client(self) -> T:
        """Create and return the exchange client instance.

        Returns:
            Configured exchange client for the specific exchange.
        """
        ...

    @abstractmethod
    def _get_exchange_name(self) -> AllExchange:
        """Return the name identifier for the exchange.

        Returns:
            Exchange name used in topics and logging.
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

    def _invalidate_symbol_cache(self) -> None:
        """Trigger cache invalidation for symbol mapper."""
        SymbolMapperService.get_instance().trigger_cache_invalidation(fail_fast=False)

    def _get_process_name(self) -> str:
        """Return the process name for log context.

        Override for multi-instance publishers (e.g. paper per-source).

        Returns:
            Process name string used in logging and heartbeats.
        """
        return f"pub:{self._get_exchange_name()}"

    async def start(self) -> None:
        """Start the publisher service and connect to exchange."""
        exchange_name = self._get_exchange_name()
        process_name = self._get_process_name()
        set_log_context(process_name)
        if self.running:
            logger.warning(f"{process_name}: Already running")
            return
        bootstrap_settings = get_settings()
        settings_service = await get_settings_service(
            bootstrap_settings.db_url,
            bootstrap_settings.zmq_broker_xpub,
        )
        self.settings = get_settings_with_service(settings_service)
        logger.info(f"{process_name}: AppSettings initialized with database access")
        self.repository = get_repository(self.settings.db_url)
        self._candle_id_cache = await self.repository.get_latest_candle_ids()
        logger.info(
            f"{process_name}: Loaded candle ID cache with {len(self._candle_id_cache)} entries"
        )
        max_symbols = self._get_max_symbols_per_connection()
        if len(self.symbols) > max_symbols > 0:
            logger.warning(
                f"{process_name} has {len(self.symbols)} symbols, but "
                f"{exchange_name} WebSocket limit is {max_symbols} symbols per connection. "
                f"Consider running multiple instances (first {max_symbols} symbols will be used)."
            )
        self.context = zmq.asyncio.Context()
        raw_pub_socket = self.context.socket(zmq.PUB)
        apply_hwm(raw_pub_socket, sndhwm=HWM_MARKET_DATA)
        raw_pub_socket.connect(self.pub_endpoint)
        self.publisher = ValidatedPublisher(raw_pub_socket)
        self.msg_publisher = MessagePublisher(self.publisher, self._tracker)
        logger.info(f"{process_name}: Connected to broker: {self.pub_endpoint}")
        raw_sub_socket = self.context.socket(zmq.SUB)
        raw_sub_socket.connect(self.settings.zmq_broker_xpub)
        self.subscriber = ValidatedSubscriber(raw_sub_socket)
        self.subscriber.subscribe("system.symbol_aliases")
        self.subscriber.subscribe("system.settings")
        logger.info(
            f"{process_name}: Subscribed to system.symbol_aliases, system.settings from "
            f"{self.settings.zmq_broker_xpub}"
        )
        self._exchange_client = self._create_exchange_client()
        await self._exchange_client.connect()
        logger.info(f"{process_name}: Exchange client connected (anonymous, public data)")
        self.running = True
        tasks: list[asyncio.Task[Any]] = []
        tasks.append(asyncio.create_task(self._heartbeat_loop()))
        tasks.append(asyncio.create_task(self._symbol_aliases_loop()))
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
            raise

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

    def _get_heartbeat_component(self) -> str:
        """Return component name for heartbeat messages.

        Override for compound identities (e.g. paper.kraken).

        Returns:
            Component name string for heartbeat topic and data message.
        """
        return f"feed.{self._get_exchange_name()}"

    def _build_data_topic(
        self, symbol: str, data_type: MarketDataType, *, timeframe: str | None = None
    ) -> str:
        """Build market data topic string.

        Override for non-standard topic formats (e.g. paper with source_exchange).

        Args:
            symbol: Native symbol identifier.
            data_type: Market data type (candles, ticks, trades).
            timeframe: Optional candle timeframe.

        Returns:
            Fully-qualified ZMQ topic string.
        """
        exchange = cast(OrderExchange, self._get_exchange_name())
        return market_topic(exchange, symbol, data_type, timeframe=timeframe)

    def _get_data_exchange(self) -> MarketDataExchange:
        """Return exchange name for market data envelopes.

        Override when envelope exchange differs from topic exchange
        (e.g. paper publisher reports source exchange in payloads).

        Returns:
            Exchange name for CandleData/TickData/TradeData.
        """
        return cast(MarketDataExchange, self._get_exchange_name())

    async def _ensure_instrument(self, native_symbol: str) -> int | None:
        """Resolve instrument_id for a native symbol, using cache.

        Args:
            native_symbol: Native exchange symbol (e.g. 'BTC-USD').

        Returns:
            Database instrument ID, or None if symbol cannot be split or
            the symbol has no active Symbol row.
        """
        instrument_id = self._instrument_cache.get(native_symbol)
        if instrument_id is not None:
            return instrument_id
        try:
            base_currency, quote_currency = native_symbol.split("-", 1)
        except ValueError:
            logger.warning(
                f"MarketDataPublisherService: "
                f"Unable to split symbol {native_symbol} for instrument"
            )
            return None
        assert self.repository is not None, "Repository not initialized"
        symbol_pid = await resolve_symbol_public_id(self.repository, native_symbol)
        if symbol_pid is None:
            logger.warning(f"MarketDataPublisherService: No active Symbol row for {native_symbol}")
            return None
        instrument_id = await self.repository.upsert_instrument(
            symbol_public_id=symbol_pid,
            symbol=native_symbol,
            exchange=self._get_exchange_name(),
            base=base_currency,
            quote=quote_currency,
            tick_size=0.0,
            lot_size=0.0,
            session_id=self._tracker.session_id,
            sequence_id=self._tracker.next_sequence("instruments"),
        )
        self._instrument_cache[native_symbol] = instrument_id
        return instrument_id

    def _resolve_candle_public_id(
        self, instrument_id: int, timeframe: str, open_at: datetime
    ) -> str:
        """Resolve the public_id for a candle from the in-memory cache.

        If the cache holds a matching (instrument_id, timeframe) entry whose
        open_at equals the incoming value, the existing public_id is reused.
        Otherwise a new UUID7 is generated and the cache is updated.

        Args:
            instrument_id: Database instrument primary key.
            timeframe: Candle timeframe (e.g. '1m').
            open_at: Candle interval start time.

        Returns:
            The public_id string to use for this candle on ZMQ and in the DB.
        """
        cache_key = (instrument_id, timeframe)
        cached = self._candle_id_cache.get(cache_key)
        if cached is not None and cached[0] == open_at:
            return cached[1]
        public_id = str(uuid7())
        self._candle_id_cache[cache_key] = (open_at, public_id)
        return public_id

    async def _candle_loop(self, symbols: list[str], timeframe: str) -> None:
        """Subscribe to candle data and publish to ZMQ.

        Uses the candle ID cache to ensure the same public_id is used on ZMQ
        and in the database for a given (instrument, timeframe, open_at) window.

        Args:
            symbols: List of symbols to subscribe to for candle data.
            timeframe: Candle timeframe interval (e.g., '1m', '5m', '1h').
        """
        if not self._exchange_client:
            logger.error(_EXCHANGE_NOT_INIT_MSG)
            return
        exchange = self._get_data_exchange()
        try:
            async for candle in self._exchange_client.subscribe_candles(symbols, timeframe):
                if not self.running:
                    break
                native_symbol = candle.symbol
                instrument_id = await self._ensure_instrument(native_symbol)
                if instrument_id is None:
                    continue
                public_id = self._resolve_candle_public_id(
                    instrument_id, timeframe, candle.interval_begin
                )
                topic = self._build_data_topic(native_symbol, "candles", timeframe=timeframe)
                candle_msg = CandleData(
                    public_id=public_id,
                    session_id=self._tracker.session_id,
                    sequence_id=self._tracker.next_sequence(topic),
                    exchange=exchange,
                    instrument=native_symbol,
                    volume=candle.volume,
                    timeframe=timeframe,
                    open_at=candle.interval_begin,
                    open=candle.open,
                    high=candle.high,
                    low=candle.low,
                    close=candle.close,
                    vwap=candle.vwap,
                    trades=candle.trades,
                )
                published = await self._publish_message(topic, candle_msg)
                self._last_data_timestamps[native_symbol] = datetime.now(UTC).timestamp() * 1000
                await self._save_to_db(
                    native_symbol, cast(CandleData, published) if published else candle_msg
                )
        except Exception as e:
            logger.error(f"Candle loop error for {symbols}: {e}")

    async def _tick_loop(self, symbols: list[str]) -> None:
        """Subscribe to tick data and publish to ZMQ.

        Args:
            symbols: List of symbols to subscribe to for tick data.
        """
        if not self._exchange_client:
            logger.error(_EXCHANGE_NOT_INIT_MSG)
            return
        exchange = self._get_data_exchange()
        try:
            async for message in self._exchange_client.subscribe_ticks(symbols):
                if not self.running:
                    break
                native_symbol = message.symbol
                topic = self._build_data_topic(native_symbol, "ticks")
                tick_msg = TickData(
                    session_id=self._tracker.session_id,
                    sequence_id=self._tracker.next_sequence(topic),
                    exchange=exchange,
                    instrument=native_symbol,
                    volume=message.volume,
                    bid=message.bid if not math.isclose(message.bid, 0.0) else None,
                    ask=message.ask if not math.isclose(message.ask, 0.0) else None,
                    last=message.last,
                )
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
            logger.error(_EXCHANGE_NOT_INIT_MSG)
            return
        exchange = self._get_data_exchange()
        try:
            async for trade in self._exchange_client.subscribe_trades(symbols):
                if not self.running:
                    break
                native_symbol = trade.symbol
                topic = self._build_data_topic(native_symbol, "trades")
                trade_msg = TradeData(
                    session_id=self._tracker.session_id,
                    sequence_id=self._tracker.next_sequence(topic),
                    exchange=exchange,
                    instrument=native_symbol,
                    executed_at=trade.timestamp,
                    price=trade.price,
                    volume=trade.quantity,
                    side=trade.side if trade.side in ["buy", "sell"] else None,
                )
                await self._publish_message(topic, trade_msg)
                self._last_data_timestamps[native_symbol] = datetime.now(UTC).timestamp() * 1000
        except Exception as e:
            logger.error(f"Trade loop error for {symbols}: {e}")

    async def _publish_message(
        self, topic: str, message: MarketDataMessage
    ) -> MarketDataMessage | None:
        """Send a complete market data message to ZMQ topic.

        Args:
            topic: ZMQ topic string for message routing.
            message: Complete market data message with provenance set.

        Returns:
            The message if sent, or None if not published.
        """
        if not self.msg_publisher or not self.running:
            return None
        try:
            await self.msg_publisher.send(topic, message)
            logger.debug(f"Published {message.type} for {topic}")
            return message
        except Exception as e:
            logger.error(f"Error publishing message: {e}")
            return None

    async def _heartbeat_loop(self) -> None:
        """Periodically publish heartbeat messages with status."""
        component_name = self._get_heartbeat_component()
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
                hb_topic = heartbeat_topic_from_component(component_name)
                hb_msg = HeartbeatData(
                    session_id=self._tracker.session_id,
                    sequence_id=self._tracker.next_sequence(hb_topic),
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
                await self._publish_heartbeat(hb_topic, hb_msg)
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")

    async def _publish_heartbeat(self, topic: str, message: HeartbeatData) -> None:
        """Send a complete heartbeat message to ZMQ.

        Args:
            topic: Heartbeat ZMQ topic string.
            message: Complete HeartbeatData with provenance set.
        """
        if not self.msg_publisher or not self.running:
            return
        try:
            await self.msg_publisher.send(topic, message)
        except Exception as e:
            logger.error(f"Error publishing heartbeat: {e}")

    async def _save_to_db(self, native_symbol: str, candle_msg: CandleData) -> None:
        """Persist candle data to the database.

        The candle_msg.public_id carries the public_id resolved by the candle ID
        cache, ensuring DB and ZMQ use the same identifier.

        Args:
            native_symbol: Native exchange symbol identifier.
            candle_msg: CandleData containing OHLCV data to persist.
        """
        try:
            instrument_id = await self._ensure_instrument(native_symbol)
            if instrument_id is None:
                return
            timeframe = candle_msg.timeframe or "1m"
            open_price = candle_msg.open if candle_msg.open is not None else candle_msg.close
            high_price = candle_msg.high if candle_msg.high is not None else candle_msg.close
            low_price = candle_msg.low if candle_msg.low is not None else candle_msg.close
            close_price = candle_msg.close if candle_msg.close is not None else 0.0
            vwap_price = candle_msg.vwap if candle_msg.vwap is not None else candle_msg.close
            trades = candle_msg.trades if candle_msg.trades is not None else 0
            candle_row: dict[str, Any] = {
                "public_id": candle_msg.public_id,
                "instrument_id": instrument_id,
                "open_at": candle_msg.open_at,
                "timestamp": candle_msg.timestamp,
                "timeframe": timeframe,
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": float(candle_msg.volume),
                "vwap": vwap_price,
                "trades": trades,
                "session_id": candle_msg.session_id,
                "sequence_id": candle_msg.sequence_id,
            }
            assert self.repository is not None, "Repository not initialized"
            await self.repository.upsert_candles([candle_row])
        except Exception as e:
            logger.error(f"Error saving candle to DB: {e}")

    async def _symbol_aliases_loop(self) -> None:
        """Listen for system messages and handle cache invalidation."""
        if not self.subscriber:
            logger.error("MarketDataPublisherService: No subscriber for system messages listener")
            return
        exchange = self._get_exchange_name()
        logger.info(f"{exchange}_feed_publisher: Starting system messages listener")
        try:
            while self.running:
                try:
                    async with asyncio.timeout(1.0):
                        topic, payload = await self.subscriber.recv_multipart()
                    if topic == "system.symbol_aliases":
                        logger.info(
                            f"{exchange}_feed_publisher: Received symbol_aliases update, "
                            "refreshing cache"
                        )
                        self._invalidate_symbol_cache()
                    elif topic == "system.settings":
                        self._handle_settings_update(payload, exchange)
                except TimeoutError:
                    continue
                except Exception as e:
                    logger.error(f"{exchange}_feed_publisher system messages listener error: {e}")
                    await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"{exchange}_feed_publisher system messages loop crashed: {e}")

    def _handle_settings_update(self, payload: bytes, exchange: str) -> None:
        """Handle settings update message and refresh cached settings.

        Args:
            payload: Raw bytes containing the settings change data.
            exchange: Exchange name for logging context.
        """
        try:
            envelope = SettingChangedData.from_json(payload.decode("utf-8"))
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
