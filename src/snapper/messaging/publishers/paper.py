"""Paper trading market data publisher.

Replays historical market data for backtesting and paper trading simulation.
"""

import asyncio
import math
from datetime import UTC
from datetime import datetime
from typing import Any

from loguru import logger

from snapper.application.process_manager.enums import ProcessRoleEnum
from snapper.application.process_manager.registry import register_process
from snapper.config.settings import AppSettings
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate
from snapper.infrastructure.exchanges.implementations.paper import PaperExchangeClient
from snapper.messaging.publishers.base import MarketDataPublisherService
from snapper.messaging.schemas.messages import BarEnvelope
from snapper.messaging.schemas.messages import TickEnvelope
from snapper.messaging.schemas.messages import TradeEnvelope
from snapper.messaging.topics.builders import market_topic


@register_process(
    "paper_feed_publisher",
    description="Paper trading feed publisher (historical data replay)",
    priority=30,
    role=ProcessRoleEnum.CORE,
    tags=("market-data", "publisher", "paper"),
    enabled=True,
    mode="thread",
    args=[],
)
class PaperMarketDataPublisher(MarketDataPublisherService[PaperExchangeClient]):
    """Market data publisher for paper trading with historical data replay."""

    def __init__(
        self,
        symbols: list[str],
        paper_instruments: dict[str, list[str]] | None = None,
        start_time: float | None = None,
        end_time: float | None = None,
    ) -> None:
        """Initialize the instance."""
        self.start_time = start_time
        self.end_time = end_time
        fallback_sources = {"kraken": symbols} if symbols else {}
        self.paper_instruments = self._validate_paper_instruments(
            paper_instruments or fallback_sources
        )
        replay_keys = self._build_replay_keys(self.paper_instruments)
        super().__init__(replay_keys)

    @staticmethod
    def get_default_kwargs(settings: AppSettings) -> dict[str, Any]:
        """Return default keyword arguments for publisher initialization.

        Args:
            settings: Application settings instance.

        Returns:
            Dictionary with symbols and time range configuration.
        """
        paper_instruments = settings.paper_instruments
        if not paper_instruments:
            paper_instruments = {"kraken": ["BTC-USD", "ETH-USD"]}
        replay_keys = PaperMarketDataPublisher._build_replay_keys(paper_instruments)
        return {
            "symbols": replay_keys,
            "paper_instruments": paper_instruments,
            "start_time": None,
            "end_time": None,
        }

    def _create_exchange_client(self) -> PaperExchangeClient:
        return PaperExchangeClient(
            repository=None,
            start_time=self.start_time,
            end_time=self.end_time,
        )

    def _get_exchange_name(self) -> str:
        return "paper"

    def _validate_symbols(self, symbols: list[str]) -> list[str]:
        return list(dict.fromkeys(symbols))

    def _validate_paper_instruments(
        self, paper_instruments: dict[str, list[str]]
    ) -> dict[str, list[str]]:
        validated: dict[str, list[str]] = {}
        for source_exchange, symbols in paper_instruments.items():
            if not source_exchange:
                continue
            normalized_exchange = source_exchange.lower()
            validated_symbols = self._validate_symbols(symbols)
            if validated_symbols:
                validated[normalized_exchange] = validated_symbols
        return validated

    @staticmethod
    def _build_replay_keys(paper_instruments: dict[str, list[str]]) -> list[str]:
        replay_keys: list[str] = []
        for source_exchange, symbols in paper_instruments.items():
            for symbol in symbols:
                replay_key = f"{source_exchange}:{symbol}"
                if replay_key not in replay_keys:
                    replay_keys.append(replay_key)
        return replay_keys

    async def _save_to_db(self, native_symbol: str, bar_msg: BarEnvelope) -> None:
        """Skip candle persistence for replayed paper market data."""
        _ = native_symbol
        _ = bar_msg

    async def _emit_candle(
        self, source_exchange: str, candle: CandleUpdate, timeframe: str
    ) -> None:
        native_symbol = candle.symbol
        bar_msg = BarEnvelope(
            exchange=source_exchange,
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
        topic = market_topic(
            "paper",
            native_symbol,
            "candles",
            timeframe=timeframe,
            source_exchange=source_exchange,
        )
        await self._publish_message(topic, bar_msg)
        timestamp = datetime.now(UTC).timestamp() * 1000
        self._last_data_timestamps[native_symbol] = timestamp
        self._last_data_timestamps[f"{source_exchange}:{native_symbol}"] = timestamp

    async def _emit_tick(self, source_exchange: str, tick: TickerUpdate) -> None:
        native_symbol = tick.symbol
        tick_msg = TickEnvelope(
            exchange=source_exchange,
            instrument=native_symbol,
            volume=tick.volume,
            bid=tick.bid if not math.isclose(tick.bid, 0.0) else None,
            ask=tick.ask if not math.isclose(tick.ask, 0.0) else None,
            last=tick.last,
        )
        topic = market_topic(
            "paper",
            native_symbol,
            "ticks",
            source_exchange=source_exchange,
        )
        await self._publish_message(topic, tick_msg)
        timestamp = datetime.now(UTC).timestamp() * 1000
        self._last_data_timestamps[native_symbol] = timestamp
        self._last_data_timestamps[f"{source_exchange}:{native_symbol}"] = timestamp

    async def _emit_trade(self, source_exchange: str, trade: TradeUpdate) -> None:
        native_symbol = trade.symbol
        trade_msg = TradeEnvelope(
            exchange=source_exchange,
            instrument=native_symbol,
            price=trade.price,
            volume=trade.quantity,
            side=trade.side if trade.side in ["buy", "sell"] else None,
        )
        topic = market_topic(
            "paper",
            native_symbol,
            "trades",
            source_exchange=source_exchange,
        )
        await self._publish_message(topic, trade_msg)
        timestamp = datetime.now(UTC).timestamp() * 1000
        self._last_data_timestamps[native_symbol] = timestamp
        self._last_data_timestamps[f"{source_exchange}:{native_symbol}"] = timestamp

    async def _candle_loop(self, symbols: list[str], timeframe: str) -> None:
        if not self._exchange_client:
            logger.error("PaperMarketDataPublisher: Exchange client not initialized")
            return
        _ = symbols
        if not self.paper_instruments:
            return
        tasks = [
            asyncio.create_task(
                self._run_candle_source_loop(source_exchange, source_symbols, timeframe)
            )
            for source_exchange, source_symbols in self.paper_instruments.items()
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Paper candle loop error for timeframe={timeframe}: {result}")

    async def _run_candle_source_loop(
        self, source_exchange: str, source_symbols: list[str], timeframe: str
    ) -> None:
        if not self._exchange_client:
            return
        async for candle in self._exchange_client.subscribe_candles(
            source_symbols,
            timeframe=timeframe,
            source_exchange=source_exchange,
        ):
            if not self.running:
                break
            await self._emit_candle(source_exchange, candle, timeframe)

    async def _tick_loop(self, symbols: list[str]) -> None:
        if not self._exchange_client:
            logger.error("PaperMarketDataPublisher: Exchange client not initialized")
            return
        _ = symbols
        if not self.paper_instruments:
            return
        tasks = [
            asyncio.create_task(self._run_tick_source_loop(source_exchange, source_symbols))
            for source_exchange, source_symbols in self.paper_instruments.items()
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Paper tick loop error: {result}")

    async def _run_tick_source_loop(self, source_exchange: str, source_symbols: list[str]) -> None:
        if not self._exchange_client:
            return
        async for tick in self._exchange_client.subscribe_ticks(
            source_symbols,
            source_exchange=source_exchange,
        ):
            if not self.running:
                break
            await self._emit_tick(source_exchange, tick)

    async def _trade_loop(self, symbols: list[str]) -> None:
        if not self._exchange_client:
            logger.error("PaperMarketDataPublisher: Exchange client not initialized")
            return
        _ = symbols
        if not self.paper_instruments:
            return
        tasks = [
            asyncio.create_task(self._run_trade_source_loop(source_exchange, source_symbols))
            for source_exchange, source_symbols in self.paper_instruments.items()
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Paper trade loop error: {result}")

    async def _run_trade_source_loop(self, source_exchange: str, source_symbols: list[str]) -> None:
        if not self._exchange_client:
            return
        async for trade in self._exchange_client.subscribe_trades(
            source_symbols,
            source_exchange=source_exchange,
        ):
            if not self.running:
                break
            await self._emit_trade(source_exchange, trade)
