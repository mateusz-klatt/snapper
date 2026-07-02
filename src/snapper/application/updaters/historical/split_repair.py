"""Polygon split-basis repair orchestrator (``polygon-repair-splits``).

Incremental CSV fetching freezes every cached day at the price-
adjustment basis of its fetch time, so a stock split executed while
the cache is being collected leaves an unadjusted discontinuity in
both the cache and the database (2026-07-02 audit: CRWD, KLAC, PALL,
PPLT, VUG, NFLX, TQQQ and friends). This service automates the repair
procedure proven during that audit:

1. Pull split events from the Polygon reference endpoint for a
   trailing lookback window.
2. Map event tickers onto active polygon equity instruments (equities
   use the native symbol form; crypto/FX never split and are skipped
   by construction).
3. Gate on evidence: a symbol is repaired ONLY when its current 1d
   closes actually show a consecutive-close break matching that
   split's expected ratio. Symbols whose whole history was fetched
   after the split are already on a uniform basis and are skipped, so
   the command is idempotent and safe to run on every fetch cycle.
4. For confirmed symbols: re-fetch the full plan window
   (``--no-resume`` semantics, adjusted as of today), prune cache
   files older than the window (they keep the stale basis forever —
   the plan cannot re-serve them), SCD2-supersede every current
   candle row, reload the refreshed cache, re-synthesize the higher
   timeframes, and re-run the detector to confirm the matching break
   is gone.

MUST run on the host, not in a container: the fetch and prune steps
write the ``data/polygon`` CSV cache, which is mounted read-only in
containers. This is also why the service is deliberately NOT
registered with the process manager — an in-container spawn could
never write the cache.

The check is stateless: rerunning with an overlapping lookback finds
already-repaired symbols clean and skips them.
"""

import math
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import date
from datetime import datetime
from datetime import time as datetime_time
from datetime import timedelta
from pathlib import Path
from typing import Final

from loguru import logger

from snapper.application.services.settings import get_settings_service
from snapper.application.updaters.historical.aggregates import PolygonAggregatesBackfillService
from snapper.application.updaters.historical.csv_loader import PolygonCsvLoaderService
from snapper.application.updaters.historical.synthesized_candle_backfill import (
    SynthesizedCandleBackfillService,
)
from snapper.config.settings import get_settings
from snapper.config.settings import get_settings_with_service
from snapper.core.types import ExchangeEnum
from snapper.data.repository import DatabaseRepository
from snapper.data.repository import Repository
from snapper.data.repository import get_repository
from snapper.infrastructure.exchanges.implementations.polygon import PolygonExchangeClient
from snapper.infrastructure.exchanges.implementations.polygon import PolygonSplitEvent
from snapper.infrastructure.historical.polygon.loader import PolygonHistoricalLoader

DEFAULT_LOOKBACK_DAYS: Final = 45
"""Trailing window of split executions to inspect. Wide enough that a
weekly or even monthly fetch cadence cannot miss an event, cheap
because clean symbols are skipped by the evidence gate."""

DEFAULT_WINDOW_DAYS: Final = 730
"""Re-fetch window; mirrors the Polygon plan's minute-data lookback
limit (documented in ``PolygonAggregatesBackfillService``)."""

_RATIO_TOLERANCE_LOG: Final = math.log(1.15)
"""A consecutive-close ratio counts as a split-basis break when its
log distance from the split's expected ratio is under this bound —
wide enough for same-day market drift, far narrower than any organic
daily move that could fake a 2:1-or-larger split."""

_MIN_DETECTABLE_BREAK_LOG: Final = math.log(1.4)
"""Split ratios closer to 1.0 than this are indistinguishable from
ordinary daily moves at the tolerance above: an everyday flat close
would match a 20:21 or 1000:1061 event forever, repairing on every
run without ever verifying clean (the refetched data legitimately
keeps showing ordinary moves inside the band). Such micro-splits are
reported and skipped — their one-time basis error is bounded by the
ratio itself and cannot be safely automated by a price detector."""

_SYNTH_TIMEFRAMES: Final = ("5m", "15m", "30m", "1h", "4h", "1d")
_CACHE_ROOT: Final = Path("data/polygon/cache")
_TIMESPAN: Final = "minute"
_ALL_CANDLE_TIMEFRAMES: Final = ("1m", "5m", "15m", "30m", "1h", "4h", "1d")


@dataclass
class SplitRepairCandidate:
    """One split event mapped onto a local instrument.

    Attributes:
        event: The triggering split event.
        native_symbol: Native symbol of the affected instrument.
        instrument_public_id: Active instrument public id.
        symbol_public_id: Active symbol public id (archive-map key).
        break_day: Day whose 1d close broke against the previous close
            when the evidence gate confirmed the stale basis, else None.
    """

    event: PolygonSplitEvent
    native_symbol: str
    instrument_public_id: str
    symbol_public_id: str
    break_day: date | None = None


@dataclass
class SplitRepairSummary:
    """Outcome of one repair run.

    Attributes:
        splits_seen: Split events returned for the lookback window.
        candidates: Events that mapped onto active polygon instruments.
        clean: Symbols whose data showed no matching break (skipped).
        repaired: Symbols repaired and verified clean.
        unverified: Symbols repaired but still showing a matching
            break afterwards — operator attention required.
        undetectable: Symbols whose split ratio is too close to 1.0
            (or malformed) for price-based detection — skipped with a
            warning; verify manually.
        pruned_files: Stale pre-window cache files deleted.
        dry_run: Whether the run stopped after detection.
    """

    splits_seen: int = 0
    candidates: list[SplitRepairCandidate] = field(default_factory=list)
    clean: list[str] = field(default_factory=list)
    repaired: list[str] = field(default_factory=list)
    unverified: list[str] = field(default_factory=list)
    undetectable: list[str] = field(default_factory=list)
    pruned_files: int = 0
    dry_run: bool = False


class PolygonSplitRepairService:
    """Detects and repairs stale split-basis history for polygon equities."""

    def __init__(
        self,
        *,
        symbols: list[str] | None = None,
        lookback_days: int = DEFAULT_LOOKBACK_DAYS,
        window_days: int = DEFAULT_WINDOW_DAYS,
        dry_run: bool = False,
    ) -> None:
        """Configure one repair run.

        Args:
            symbols: Restrict the check to these native symbols;
                ``None`` checks every equity the split feed names.
            lookback_days: Trailing window of split executions to
                inspect.
            window_days: Re-fetch window in days (plan lookback limit).
            dry_run: Detect and report only; skip every mutating step.
        """
        self._symbols_filter = {s.upper() for s in symbols} if symbols else None
        self._lookback_days = lookback_days
        self._window_days = window_days
        self._dry_run = dry_run
        self.settings = get_settings()

    async def start(self) -> SplitRepairSummary:
        """Run detection and (unless dry-run) the full repair chain.

        Returns:
            Summary of detection and repair outcomes.

        Raises:
            ValueError: When the Polygon API key is not configured.
        """
        settings_service = await get_settings_service(
            self.settings.db_url,
            self.settings.zmq_broker_xsub,
        )
        self.settings = get_settings_with_service(settings_service)
        api_key = self.settings.polygon_api_key
        if not api_key:
            raise ValueError("Polygon API key not configured in settings")
        repo = get_repository(self.settings.db_url)
        client = PolygonExchangeClient(api_key=api_key)
        try:
            summary = await self._run(client, repo)
        finally:
            await client.disconnect()
        return summary

    async def _run(self, client: PolygonExchangeClient, repo: Repository) -> SplitRepairSummary:
        """Execute detection + repair with an open client and repository.

        Args:
            client: Connected Polygon client (caller disconnects).
            repo: Async repository handle.

        Returns:
            Summary of detection and repair outcomes.
        """
        summary = SplitRepairSummary(dry_run=self._dry_run)
        since = datetime.now(UTC).date() - timedelta(days=self._lookback_days)
        events = await client.list_splits(execution_date_gte=since)
        summary.splits_seen = len(events)
        universe = {
            row["native_symbol"]: row
            for row in await repo.list_instrument_symbols(exchange=ExchangeEnum.POLYGON)
        }
        confirmed: list[SplitRepairCandidate] = []
        for event in events:
            row = universe.get(event.ticker)
            if row is None:
                continue
            if self._symbols_filter is not None and event.ticker not in self._symbols_filter:
                continue
            if (
                not math.isfinite(event.split_from)
                or not math.isfinite(event.split_to)
                or event.split_from <= 0
                or event.split_to <= 0
                or abs(math.log(event.expected_break_ratio)) < _MIN_DETECTABLE_BREAK_LOG
            ):
                summary.undetectable.append(event.ticker)
                logger.warning(
                    "Split {d} {f}:{t} for {sym} is too close to 1.0 (or malformed) for "
                    "price-based detection - SKIPPED; verify this symbol manually",
                    d=event.execution_date.isoformat(),
                    f=event.split_from,
                    t=event.split_to,
                    sym=event.ticker,
                )
                continue
            candidate = SplitRepairCandidate(
                event=event,
                native_symbol=row["native_symbol"],
                instrument_public_id=row["instrument_public_id"],
                symbol_public_id=row["symbol_public_id"],
            )
            summary.candidates.append(candidate)
            candidate.break_day = await self._find_matching_break(repo, candidate)
            if candidate.break_day is None:
                summary.clean.append(candidate.native_symbol)
                logger.info(
                    "Split basis clean for {sym} ({d} {f}:{t}) - skipping",
                    sym=candidate.native_symbol,
                    d=event.execution_date.isoformat(),
                    f=event.split_from,
                    t=event.split_to,
                )
            else:
                confirmed.append(candidate)
                logger.warning(
                    "Stale split basis CONFIRMED for {sym}: 1d break on {b} matches "
                    "split {d} {f}:{t}",
                    sym=candidate.native_symbol,
                    b=candidate.break_day.isoformat(),
                    d=event.execution_date.isoformat(),
                    f=event.split_from,
                    t=event.split_to,
                )
        if not confirmed or self._dry_run:
            return summary
        await self._repair(repo, confirmed, summary)
        return summary

    async def _find_matching_break(
        self, repo: Repository, candidate: SplitRepairCandidate
    ) -> date | None:
        """Return the day of a 1d close break matching the split ratio.

        Scans the instrument's full current 1d history because a
        stale-basis boundary sits at the FETCH boundary between cache
        waves, not necessarily at the split's execution date. An EMPTY
        current 1d history also confirms: it is the signature of a
        repair that crashed between supersede and reload, so treating
        it as broken makes a rerun self-healing (the repair chain
        refetches and reloads the window) instead of reporting a
        hollowed-out symbol as clean.

        Args:
            repo: Async repository handle.
            candidate: Mapped split event to check.

        Returns:
            Break day (the execution date when the history is empty),
            or ``None`` when the history is consistent.
        """
        now = datetime.now(UTC)
        rows = await repo.get_candles(
            candidate.native_symbol,
            "1d",
            now - timedelta(days=self._window_days + 366),
            now,
            ExchangeEnum.POLYGON,
            now,
        )
        if not rows:
            logger.warning(
                "No current 1d history for {sym} - treating as incomplete repair",
                sym=candidate.native_symbol,
            )
            return candidate.event.execution_date
        expected = candidate.event.expected_break_ratio
        previous_close: float | None = None
        for row in rows:
            close = row["close"]
            if previous_close is not None and previous_close > 0 and close > 0:
                distance = abs(math.log(close / previous_close) - math.log(expected))
                if distance < _RATIO_TOLERANCE_LOG:
                    return row["open_at"].date()
            previous_close = close
        return None

    async def _repair(
        self,
        repo: Repository,
        confirmed: list[SplitRepairCandidate],
        summary: SplitRepairSummary,
    ) -> None:
        """Run the batch repair chain for confirmed candidates.

        Args:
            repo: Async repository handle.
            confirmed: Candidates with an evidenced stale basis.
            summary: Mutable run summary to fill in.
        """
        natives = sorted({c.native_symbol for c in confirmed})
        window_start = datetime.now(UTC).date() - timedelta(days=self._window_days)
        logger.info(
            "Repairing {n} symbol(s): {syms} (window from {w})",
            n=len(natives),
            syms=", ".join(natives),
            w=window_start.isoformat(),
        )
        refetch = PolygonAggregatesBackfillService(
            symbols=natives,
            timespan=_TIMESPAN,
            days_back=self._window_days,
            resume=False,
            save_csv=True,
        )
        await refetch.start()
        summary.pruned_files = self._prune_stale_cache(confirmed, window_start)
        for candidate in confirmed:
            for timeframe in _ALL_CANDLE_TIMEFRAMES:
                superseded = await repo.supersede_current_candles(
                    instrument_public_id=candidate.instrument_public_id,
                    timeframe=timeframe,
                )
                logger.info(
                    "Superseded {n} current {tf} rows for {sym}",
                    n=superseded,
                    tf=timeframe,
                    sym=candidate.native_symbol,
                )
        loader = PolygonCsvLoaderService(
            symbols=natives,
            timespan=_TIMESPAN,
            since=window_start,
            until=datetime.now(UTC).date(),
        )
        await loader.start()
        synth = SynthesizedCandleBackfillService(
            exchange=ExchangeEnum.POLYGON,
            start=datetime.combine(window_start, datetime_time.min, tzinfo=UTC),
            end=datetime.now(UTC),
            symbols=natives,
            all_symbols=False,
            timeframes=list(_SYNTH_TIMEFRAMES),
            cut_date=window_start,
        )
        await synth.start()
        for candidate in confirmed:
            remaining = await self._find_matching_break(repo, candidate)
            if remaining is None:
                summary.repaired.append(candidate.native_symbol)
            else:
                summary.unverified.append(candidate.native_symbol)
                logger.error(
                    "Split break STILL PRESENT for {sym} on {d} after repair - "
                    "operator attention required",
                    sym=candidate.native_symbol,
                    d=remaining.isoformat(),
                )

    def _prune_stale_cache(self, confirmed: list[SplitRepairCandidate], window_start: date) -> int:
        """Delete cache files older than the re-fetch window.

        Pre-window files keep the stale price basis forever (the plan
        cannot re-serve that range), so leaving them behind would let
        any future full-range cache load reintroduce broken data.

        Args:
            confirmed: Candidates being repaired.
            window_start: First day of the refreshed window.

        Returns:
            Number of files deleted.
        """
        archive_map = DatabaseRepository(self.settings.db_url).get_archive_symbols()
        path_helper = PolygonHistoricalLoader(None, cache_root=_CACHE_ROOT)
        pruned = 0
        for candidate in confirmed:
            archive_symbol = archive_map.get(candidate.symbol_public_id)
            if archive_symbol is None:
                logger.warning(
                    "No archive symbol for {sym} - skipping cache prune",
                    sym=candidate.native_symbol,
                )
                continue
            for csv_path, day in path_helper.iter_aggregate_csv_files(archive_symbol, _TIMESPAN):
                if day < window_start:
                    csv_path.unlink()
                    pruned += 1
        if pruned:
            logger.info("Pruned {n} stale pre-window cache file(s)", n=pruned)
        return pruned


async def run_polygon_split_repair(
    *,
    symbols: list[str] | None = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    window_days: int = DEFAULT_WINDOW_DAYS,
    dry_run: bool = False,
) -> SplitRepairSummary:
    """Build and run one split-repair pass (CLI entry point).

    Args:
        symbols: Restrict the check to these native symbols.
        lookback_days: Trailing window of split executions to inspect.
        window_days: Re-fetch window in days.
        dry_run: Detect and report only.

    Returns:
        Summary of detection and repair outcomes.
    """
    service = PolygonSplitRepairService(
        symbols=symbols,
        lookback_days=lookback_days,
        window_days=window_days,
        dry_run=dry_run,
    )
    return await service.start()
