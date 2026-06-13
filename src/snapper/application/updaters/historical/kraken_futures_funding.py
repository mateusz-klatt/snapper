"""Kraken Futures historical funding rate backfill service.

Downloads historical funding rates from the Kraken Futures REST API
via ``Market.get_historical_funding_rates()`` and persists them to
the ``funding_rates`` table. Supports all active perpetual contracts
or a caller-specified symbol list.
"""

import asyncio
from datetime import UTC
from datetime import datetime
from typing import Any

from loguru import logger
from sqlalchemy.exc import IntegrityError

from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.process_manager.process_parameters import SymbolUpdaterParameters
from snapper.application.process_manager.registry import register_process
from snapper.config.settings import AppSettings
from snapper.config.settings import get_settings
from snapper.core.types import ExchangeEnum
from snapper.core.types import ProcessLifecycleEnum
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRoleEnum
from snapper.data.repository import Repository
from snapper.data.repository import get_repository
from snapper.data.repository_types import FundingRateInsertRow
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)
from snapper.infrastructure.symbols.functions import get_available_kraken_futures_symbols
from snapper.infrastructure.symbols.functions import native_to_kraken_futures_ws
from snapper.infrastructure.symbols.functions import resolve_symbol_public_id
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.utils.logging import set_log_context

_RATE_LIMIT_DELAY = 1.0


@register_process(
    "kraken_futures_funding_backfill",
    method="start",
    description="Kraken Futures historical funding rate backfill",
    priority=33,
    lifecycle=ProcessLifecycleEnum.ONE_SHOT,
    role=ProcessRoleEnum.TASK,
    tags=("kraken_futures", "backfill", "funding"),
    parameters_model=SymbolUpdaterParameters,
    enabled=False,
    mode=ProcessModeEnum.THREAD,
)
class KrakenFuturesFundingBackfillService(RegisterableProcess):
    """Backfill historical funding rates for Kraken Futures perpetuals.

    Downloads rates via the Kraken Futures SDK and persists to the
    ``funding_rates`` table via ``repository.insert_funding_rate``.
    Duplicate inserts are idempotent (partial unique index swallows
    ``IntegrityError``).
    """

    @staticmethod
    def get_default_parameters(settings: AppSettings) -> dict[str, Any]:
        """Get default parameters from settings.

        Args:
            settings: Application settings.

        Returns:
            Default parameters with symbols and all_symbols flag.
        """
        return {
            "symbols": settings.instruments.get(ExchangeEnum.KRAKEN_FUTURES, []),
            "all_symbols": False,
        }

    def __init__(
        self,
        symbols: list[str] | None = None,
        all_symbols: bool = False,
    ) -> None:
        """Initialize the backfill service.

        Args:
            symbols: Native symbols to backfill (e.g., ``BTC-USD-PERP``).
                None uses settings default.
            all_symbols: If True, backfill all Kraken Futures symbols.
        """
        self._requested_symbols = list(symbols) if symbols else []
        self._all_symbols = all_symbols
        self.settings = get_settings()
        self._db: Repository | None = None
        self._instrument_cache: dict[str, str] = {}
        self._tracker: SequenceTracker = SequenceTracker()

    async def start(self) -> None:
        """Run the backfill process.

        Resolves perpetual symbols, connects to exchange, and fetches
        historical funding rates for each symbol sequentially with
        rate limiting between requests.
        """
        set_log_context("bf:kf_fund")
        self._db = get_repository(self.settings.db_url)
        symbols = self._resolve_symbols()
        if not symbols:
            logger.warning("No Kraken Futures symbols configured for funding backfill")
            return
        client = KrakenFuturesExchangeClient()
        try:
            for symbol in symbols:
                await self._process_symbol(client, symbol)
                await asyncio.sleep(_RATE_LIMIT_DELAY)
        finally:
            await client.disconnect()

    def _resolve_symbols(self) -> list[str]:
        """Determine which symbols to backfill.

        Only perpetual contracts (symbol containing ``PERP``) are
        included since non-perpetuals do not have funding rates.

        Wildcard ``["*"]`` in ``settings.instruments[KRAKEN_FUTURES]``
        expands the same way as the ``--all`` CLI flag — every
        currently-mapped Kraken Futures symbol filtered to perpetuals.
        This mirrors the publisher's ``_validate_symbols`` wildcard
        behavior so the single sentinel ``["*"]`` consistently means
        "all venues" regardless of which consumer reads it.

        Returns:
            List of native perpetual symbols to process.
        """
        if self._all_symbols:
            all_syms = get_available_kraken_futures_symbols()
            perps = [s for s in all_syms if "PERP" in s.upper()]
            logger.info(f"Resolved {len(perps)} Kraken Futures perpetuals for funding backfill")
            return perps
        if self._requested_symbols:
            return [s for s in self._requested_symbols if "PERP" in s.upper()]
        configured = self.settings.instruments.get(ExchangeEnum.KRAKEN_FUTURES, [])
        if configured == ["*"]:
            all_syms = get_available_kraken_futures_symbols()
            perps = [s for s in all_syms if "PERP" in s.upper()]
            logger.info(f"Resolved {len(perps)} Kraken Futures perpetuals from wildcard settings")
            return perps
        return [s for s in configured if "PERP" in s.upper()]

    async def _ensure_instrument(self, native_symbol: str) -> str | None:
        """Ensure instrument exists in database, return its public_id.

        Args:
            native_symbol: Native symbol (e.g., ``BTC-USD-PERP``).

        Returns:
            The instrument_public_id, or None if symbol not in database.
        """
        assert self._db is not None
        if native_symbol in self._instrument_cache:
            return self._instrument_cache[native_symbol]
        now = datetime.now(UTC)
        symbol_pid = await resolve_symbol_public_id(self._db, native_symbol, as_of=now)
        if symbol_pid is None:
            logger.warning(f"No active Symbol row for {native_symbol}, skipping")
            return None
        _id, instrument_public_id = await self._db.ensure_instrument(
            symbol_public_id=symbol_pid,
            exchange=ExchangeEnum.KRAKEN_FUTURES,
            session_id=self._tracker.session_id,
            sequence_id=self._tracker.next_sequence("instruments"),
            timestamp=now,
        )
        self._instrument_cache[native_symbol] = instrument_public_id
        return instrument_public_id

    async def _process_symbol(
        self,
        client: KrakenFuturesExchangeClient,
        native_symbol: str,
    ) -> None:
        """Backfill funding rates for a single perpetual symbol.

        Converts native to Kraken WS format, fetches all historical
        rates, and inserts each into the database. Duplicates are
        silently skipped by the partial unique index.

        Args:
            client: Kraken Futures exchange client (no auth needed).
            native_symbol: Native symbol (e.g., ``BTC-USD-PERP``).
        """
        assert self._db is not None
        instrument_pid = await self._ensure_instrument(native_symbol)
        if instrument_pid is None:
            return
        try:
            kraken_symbol = native_to_kraken_futures_ws(native_symbol)
        except ValueError:
            logger.warning(f"No WS alias for {native_symbol}, skipping funding backfill")
            return
        logger.info(f"Fetching historical funding rates for {native_symbol} ({kraken_symbol})")
        snapshots = await client.get_historical_funding_rates(kraken_symbol)
        if not snapshots:
            logger.info(f"No historical funding rates returned for {native_symbol}")
            return
        now = datetime.now(UTC)
        inserted = 0
        skipped = 0
        for snap in snapshots:
            row = FundingRateInsertRow(
                instrument_public_id=instrument_pid,
                exchange=ExchangeEnum.KRAKEN_FUTURES,
                rate_type=snap.rate_type,
                direction=snap.direction,
                rate=snap.rate,
                notional_asset=snap.notional_asset,
                effective_from=snap.effective_from,
                source=snap.source,
                session_id=self._tracker.session_id,
                sequence_id=self._tracker.next_sequence("funding_rates"),
                timestamp=now,
            )
            try:
                await self._db.insert_funding_rate(row)
                inserted += 1
            except IntegrityError:
                skipped += 1
        logger.info(
            f"Funding backfill for {native_symbol}: {inserted} inserted, {skipped} duplicates"
        )
