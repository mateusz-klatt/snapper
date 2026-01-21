"""Polygon grouped daily backfill service module.

This module provides downloading of grouped daily aggregates from Polygon.io.
Grouped dailies contain all tickers' OHLCV data for a single day, useful
for market-wide analysis and screening.
"""

from datetime import UTC
from datetime import date
from datetime import datetime
from datetime import timedelta
from pathlib import Path

from loguru import logger

from snapper.application.process_manager.enums import ProcessLifecycleEnum
from snapper.application.process_manager.enums import ProcessRoleEnum
from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.process_manager.registry import register_process
from snapper.application.services.settings import get_settings_service
from snapper.config.settings import AppSettings
from snapper.config.settings import get_settings
from snapper.config.settings import get_settings_with_service
from snapper.infrastructure.exchanges.implementations.polygon import PolygonExchangeClient
from snapper.infrastructure.historical.polygon.loader import PolygonHistoricalLoader
from snapper.utils.logging import set_log_context

__all__ = ["PolygonGroupedDailyBackfillService"]
_CACHE_ROOT = Path("data/polygon/cache")


@register_process(
    "polygon_grouped_daily_backfill",
    method="start",
    description="Download Polygon grouped daily aggregates to CSV.gz",
    priority=33,
    lifecycle=ProcessLifecycleEnum.ONE_SHOT,
    role=ProcessRoleEnum.TASK,
    tags=("polygon", "grouped", "historical"),
    enabled=False,
    mode="thread",
    args=[],
)
class PolygonGroupedDailyBackfillService(RegisterableProcess):
    """Service for downloading Polygon grouped daily data.

    Downloads market-wide daily OHLCV data from Polygon.io grouped
    daily endpoint. Each request returns all tickers for one day.

    Supports:
    - Multiple market types (crypto, stocks, forex)
    - CSV caching to avoid re-downloads
    - Adjusted/unadjusted prices

    Registered as one-shot task process.
    """

    @staticmethod
    def get_default_kwargs(settings: AppSettings) -> dict[str, object]:
        """Get default constructor kwargs from settings.

        Args:
            settings: Application settings.

        Returns:
            Default kwargs for constructor.
        """
        return {
            "market_type": "crypto",
            "days": 3,
            "locale": "global",
            "save_csv": True,
            "adjusted": True,
        }

    def __init__(
        self,
        market_type: str = "crypto",
        days: int = 3,
        locale: str = "global",
        save_csv: bool = True,
        adjusted: bool = True,
    ) -> None:
        """Initialize the grouped daily backfill service.

        Args:
            market_type: Market type ("crypto", "stocks", "forex").
            days: Number of past days to download.
            locale: Market locale ("global", "us").
            save_csv: Whether to save CSV files.
            adjusted: Whether to use adjusted prices.
        """
        self._market_type = market_type
        self._days = days
        self._locale = locale
        self._save_csv = save_csv
        self._adjusted = adjusted
        self.settings = get_settings()
        self._loader: PolygonHistoricalLoader | None = None

    async def start(self) -> None:
        """Start the grouped daily backfill process.

        Downloads grouped daily data for configured number of days.
        Skips days that already have cached CSV files.
        """
        set_log_context("bf:poly_grp")
        settings_service = await get_settings_service(
            self.settings.db_url,
            self.settings.zmq_broker_xpub,
            self.settings.master_password,
            self.settings.encryption_salt,
        )
        self.settings = get_settings_with_service(settings_service)
        api_key = self.settings.polygon_api_key
        if not api_key:
            raise ValueError("Polygon API key not configured in settings")
        client = PolygonExchangeClient(api_key=api_key)
        self._loader = PolygonHistoricalLoader(client, cache_root=_CACHE_ROOT)
        end_day = datetime.now(UTC).date()
        for offset in range(1, self._days + 2):
            target_day = end_day - timedelta(days=offset)
            await self._fetch_day(target_day)

    async def _fetch_day(self, target_day: date) -> None:
        """Fetch grouped daily data for a specific day.

        Skips if CSV already exists. Downloads and optionally saves to CSV.

        Args:
            target_day: Date to fetch data for.
        """
        assert self._loader is not None
        csv_path = self._loader.get_grouped_csv_path(target_day, self._market_type, self._locale)
        if csv_path.exists():
            logger.info(
                f" Skipping {target_day.isoformat()} - CSV file already exists",
                path=str(csv_path),
            )
            return
        logger.info(
            "Fetching grouped daily",
            day=target_day.isoformat(),
            market_type=self._market_type,
            locale=self._locale,
        )
        await self._loader.fetch_grouped_daily(
            target_day,
            market_type=self._market_type,
            locale=self._locale,
            adjusted=self._adjusted,
            save_csv=self._save_csv,
        )
