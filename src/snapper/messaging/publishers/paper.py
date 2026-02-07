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

from loguru import logger

from snapper.application.process_manager.enums import ProcessRoleEnum
from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.process_manager.registry import register_process
from snapper.config.settings import AppSettings
from snapper.data.repository import get_repository
from snapper.infrastructure.exchanges.implementations.paper import PaperExchangeClient
from snapper.messaging.publishers.base import MarketDataPublisherService
from snapper.messaging.schemas.messages import BarEnvelope
from snapper.messaging.topics.builders import MarketDataType
from snapper.messaging.topics.builders import market_topic


class PerSourcePaperPublisher(MarketDataPublisherService[PaperExchangeClient]):
    """Paper publisher for a single source exchange.

    Replays historical data from one real exchange (e.g. kraken) and publishes
    it on paper-prefixed ZMQ topics. Each instance manages its own
    PaperExchangeClient bound to the source_exchange.
    """

    def __init__(
        self,
        source_exchange: str,
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
        """Create paper client bound to this source exchange."""
        repository = get_repository(self.settings.db_url)
        return PaperExchangeClient(
            repository=repository,
            start_time=self.start_time,
            end_time=self.end_time,
            source_exchange=self._source_exchange,
        )

    def _get_exchange_name(self) -> str:
        """Return 'paper' as the trading exchange identity."""
        return "paper"

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
            "paper",
            symbol,
            data_type,
            timeframe=timeframe,
            source_exchange=self._source_exchange,
        )

    def _get_data_exchange(self) -> str:
        """Return source exchange for envelope payloads."""
        return self._source_exchange

    async def _save_to_db(self, native_symbol: str, bar_msg: BarEnvelope) -> None:
        """Skip candle persistence for replayed paper market data."""
        _ = native_symbol
        _ = bar_msg


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
                Required — ValueError raised if None or empty.
            start_time: Start timestamp for backtesting (Unix seconds).
            end_time: End timestamp for backtesting (Unix seconds).

        Raises:
            ValueError: If paper_instruments is None or empty after validation.
        """
        if paper_instruments is None:
            raise ValueError(
                "paper_instruments required: configure PAPER_INSTRUMENTS in settings "
                "(e.g. {'kraken': ['BTC-USD', 'ETH-USD']})"
            )
        self.paper_instruments = self._validate_paper_instruments(paper_instruments)
        self.start_time = start_time
        self.end_time = end_time
        self._publishers: list[PerSourcePaperPublisher] = []

    @staticmethod
    def get_default_kwargs(settings: AppSettings) -> dict[str, Any]:
        """Return default keyword arguments from application settings.

        Args:
            settings: Application settings instance.

        Returns:
            Dictionary with paper_instruments and time range configuration.

        Raises:
            ValueError: If paper_instruments not configured in settings.
        """
        paper_instruments = settings.paper_instruments
        if not paper_instruments:
            raise ValueError("paper_instruments must be configured in settings for paper trading")
        return {
            "paper_instruments": paper_instruments,
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
    ) -> dict[str, list[str]]:
        """Validate and normalize paper instruments configuration.

        Args:
            paper_instruments: Raw mapping of exchange names to symbol lists.

        Returns:
            Validated mapping with normalized exchange names and deduplicated symbols.

        Raises:
            ValueError: If no valid instruments remain after validation.
        """
        validated: dict[str, list[str]] = {}
        for source_exchange, symbols in paper_instruments.items():
            if not source_exchange:
                continue
            normalized_exchange = source_exchange.lower()
            validated_symbols = list(dict.fromkeys(symbols))
            if validated_symbols:
                validated[normalized_exchange] = validated_symbols
        if not validated:
            raise ValueError(
                "paper_instruments must contain at least one source exchange with symbols"
            )
        return validated
