"""Paper trading market data publisher.

Replays historical market data for backtesting and paper trading simulation.

Architecture:
    PerSourcePaperPublisher — a lightweight MarketDataPublisherService that
    handles exactly one source exchange (e.g. kraken). Overrides the base
    class hooks to emit paper-prefixed topics with source_exchange, and
    creates a PaperExchangeClient bound to that source.

    PaperMarketDataPublisher — RegisterableProcess wrapper that creates N
    PerSourcePaperPublisher instances (one per source_exchange entry in
    paper_instruments config) and orchestrates their lifecycle.
"""

import asyncio
from typing import Any
from typing import cast
from typing import get_args

from loguru import logger

from snapper.application.process_manager.enums import ProcessRoleEnum
from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.process_manager.process_parameters import PaperPublisherParameters
from snapper.application.process_manager.registry import register_process
from snapper.config.settings import AppSettings
from snapper.core.types import AllExchange
from snapper.core.types import ExchangeEnum
from snapper.core.types import MarketDataExchange
from snapper.core.types import MarketDataType
from snapper.data.repository_types import CandleUpsertRow
from snapper.infrastructure.exchanges.implementations.paper import PaperExchangeClient
from snapper.messaging.publishers.base import MarketDataPublisherService
from snapper.messaging.topics.builders import market_topic


class PerSourcePaperPublisher(MarketDataPublisherService[PaperExchangeClient]):
    """Paper publisher for a single source exchange.

    Replays historical data from one real exchange (e.g. kraken) and publishes
    it on paper-prefixed ZMQ topics. Each instance manages its own
    PaperExchangeClient bound to the source_exchange.
    """

    def __init__(
        self,
        source_exchange: MarketDataExchange,
        symbols: list[str],
        start_time: float | None = None,
        end_time: float | None = None,
    ) -> None:
        """Initialize per-source paper publisher.

        Args:
            source_exchange: Real exchange to replay data from (e.g. "kraken").
            symbols: List of trading symbols for this source exchange.
            start_time: Start timestamp for backtesting (Unix seconds).
            end_time: End timestamp for backtesting (Unix seconds).
        """
        self._source_exchange = source_exchange
        self.start_time = start_time
        self.end_time = end_time
        super().__init__(symbols)

    def _create_exchange_client(self) -> PaperExchangeClient:
        """Create paper client bound to this source exchange.

        Uses self.repository from base class (created in start() before this call).
        """
        return PaperExchangeClient(
            repository=self.repository,
            start_time=self.start_time,
            end_time=self.end_time,
            source_exchange=self._source_exchange,
        )

    def _get_exchange_name(self) -> AllExchange:
        """Return 'paper' as the trading exchange identity."""
        return ExchangeEnum.PAPER

    def _get_process_name(self) -> str:
        """Return process name including source exchange for log context."""
        return f"pub:paper:{self._source_exchange}"

    def _validate_symbols(self, symbols: list[str]) -> list[str]:
        """Accept all symbols, deduplicated."""
        return list(dict.fromkeys(symbols))

    def _get_heartbeat_component(self) -> str:
        """Return compound heartbeat identity including source exchange."""
        return f"feed.paper.{self._source_exchange}"

    def _build_data_topic(
        self, symbol: str, data_type: MarketDataType, *, timeframe: str | None = None
    ) -> str:
        """Build paper topic with source_exchange segment."""
        return market_topic(
            ExchangeEnum.PAPER,
            symbol,
            data_type,
            timeframe=timeframe,
            source_exchange=self._source_exchange,
        )

    def _get_data_exchange(self) -> MarketDataExchange:
        """Return source exchange for envelope payloads."""
        return self._source_exchange

    async def _flush_candle_batch(self, batch: list[CandleUpsertRow]) -> None:
        """Skip candle persistence for replayed paper market data."""
        _ = batch


@register_process(
    "paper_feed_publisher",
    description="Paper trading feed publisher",
    priority=30,
    role=ProcessRoleEnum.CORE,
    tags=("market-data", "publisher", "paper"),
    parameters_model=PaperPublisherParameters,
    enabled=True,
    mode="thread",
)
class PaperMarketDataPublisher(RegisterableProcess):
    """Orchestrator that manages per-source paper publishers.

    Creates one PerSourcePaperPublisher for each source exchange in the
    paper_instruments configuration and manages their lifecycle.
    """

    def __init__(
        self,
        paper_instruments: dict[str, list[str]] | None = None,
        start_time: float | None = None,
        end_time: float | None = None,
    ) -> None:
        """Initialize paper publisher orchestrator.

        Args:
            paper_instruments: Mapping of source exchange to symbol lists.
                Example: {"kraken": ["BTC-USD", "ETH-USD"]}.
                None or empty enters idle mode (no replay).
            start_time: Start timestamp for backtesting (Unix seconds).
            end_time: End timestamp for backtesting (Unix seconds).
        """
        self.paper_instruments: dict[MarketDataExchange, list[str]] = (
            self._validate_paper_instruments(paper_instruments) if paper_instruments else {}
        )
        self.start_time = start_time
        self.end_time = end_time
        self._publishers: list[PerSourcePaperPublisher] = []

    @staticmethod
    def get_default_parameters(settings: AppSettings) -> dict[str, Any]:
        """Return default parameters from application settings.

        Args:
            settings: Application settings instance.

        Returns:
            Dictionary with paper_instruments and time range configuration.
            Empty paper_instruments results in idle mode (no replay).
        """
        return {
            "paper_instruments": settings.paper_instruments or {},
            "start_time": None,
            "end_time": None,
        }

    async def start(self) -> None:
        """Start all per-source paper publishers concurrently."""
        if not self.paper_instruments:
            logger.warning("PaperMarketDataPublisher: No paper instruments configured")
            return
        self._publishers = [
            PerSourcePaperPublisher(
                source_exchange=source_exchange,
                symbols=symbols,
                start_time=self.start_time,
                end_time=self.end_time,
            )
            for source_exchange, symbols in self.paper_instruments.items()
        ]
        logger.info(
            f"PaperMarketDataPublisher: Starting {len(self._publishers)} "
            f"per-source publishers: {list(self.paper_instruments.keys())}"
        )
        async with asyncio.TaskGroup() as tg:
            for pub in self._publishers:
                tg.create_task(pub.start())

    async def stop(self) -> None:
        """Stop all per-source paper publishers."""
        for pub in self._publishers:
            await pub.stop()
        self._publishers = []

    def get_status(self) -> dict[str, Any]:
        """Return combined status from all per-source publishers.

        Returns:
            Dictionary with paper_instruments config and per-source statuses.
        """
        return {
            "paper_instruments": self.paper_instruments,
            "publishers": [pub.get_status() for pub in self._publishers],
        }

    @staticmethod
    def _validate_paper_instruments(
        paper_instruments: dict[str, list[str]],
    ) -> dict[MarketDataExchange, list[str]]:
        """Validate and normalize paper instruments configuration.

        Filters out empty exchange names, empty symbol lists, and
        exchanges not in MarketDataExchange. If all entries are
        filtered, returns empty dict (idle mode).

        Args:
            paper_instruments: Raw mapping of exchange names to symbol lists.

        Returns:
            Validated mapping with MarketDataExchange keys and deduplicated symbols.
        """
        valid_exchanges = set(get_args(MarketDataExchange))
        validated: dict[MarketDataExchange, list[str]] = {}
        for source_exchange, symbols in paper_instruments.items():
            if not source_exchange:
                continue
            normalized = source_exchange.lower()
            if normalized not in valid_exchanges:
                logger.warning(
                    f"PaperMarketDataPublisher: skipping unknown exchange '{normalized}'"
                )
                continue
            validated_symbols = list(dict.fromkeys(symbols))
            if validated_symbols:
                validated[cast(MarketDataExchange, normalized)] = validated_symbols
        return validated
