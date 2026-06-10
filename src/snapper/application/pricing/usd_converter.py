"""USD price oracle for :class:`TradingCapsEnforcer`.

Resolves ``submission.quantity × price_usd`` for the rolling
24h-notional cap. Reads the active
:class:`~snapper.data.models.MarketSnapshot` for the instrument
and uses its ``last_price`` as the submit-time commitment value.

Scope: one-hop conversion only.
If ``Instrument.quote == "USD"``, returns ``last_price × quantity``.
If ``Instrument.quote != "USD"``, raises
:class:`PriceUnavailableError` with ``quote_currency_not_usd``.

Cache + staleness:
In-process TTL cache (60s) keyed by ``instrument_public_id`` stores
``(last_price, snapshot_timestamp, cached_at)``.
If ``snapshot_timestamp < now - 300s``, raises
:class:`PriceUnavailableError` with ``price_stale``.
"""

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from decimal import Decimal

from sqlalchemy import select

from snapper.data.models import Instrument
from snapper.data.models import MarketSnapshot
from snapper.data.models import Symbol
from snapper.data.repository import Repository
from snapper.data.repository import where_active

CACHE_TTL_SECONDS = 60
STALENESS_THRESHOLD_SECONDS = 300


class PriceUnavailableError(Exception):
    """Raised when USD pricing cannot be resolved for an instrument.

    Carries ``reason`` so the cap enforcer can map to the
    error_code ``caps_price_unavailable`` with appropriate context.
    Reasons
        ``instrument_not_found``: no active Instrument row.
        ``snapshot_missing``: Instrument exists but no active
          MarketSnapshot row (market data pipeline hasn't produced
          a tick yet).
        ``last_price_null``: MarketSnapshot row exists but
          ``last_price`` is NULL (venue never reported a trade).
        ``price_stale``: snapshot timestamp is older than 300s.
        ``quote_currency_not_usd``: one-hop scope
          non-USD quote instruments are rejected.
    """

    def __init__(self, reason: str, instrument_public_id: str, detail: str = "") -> None:
        """Initialize with structured reason + context."""
        self.reason_code: str = reason
        self.instrument_public_id = instrument_public_id
        self.detail = detail
        msg = f"price unavailable ({reason}) for instrument {instrument_public_id}"
        if detail:
            msg = f"{msg}: {detail}"
        super().__init__(msg)


@dataclass(frozen=True)
class _CacheEntry:
    """Single entry in the per-instrument TTL cache."""

    last_price: Decimal
    snapshot_timestamp: datetime
    cached_at: datetime


class USDConverter:
    """Resolve USD notional for a submitted order quantity.

    Constructor wiring:

        ``USDConverter(repository, now=...)`` — ``repository`` is
        the existing :class:`~snapper.data.repository.Repository`
        singleton injected at app startup. ``now`` is an optional
        callable returning the current UTC datetime; tests override
        it to control freshness windows deterministically.

    Thread-safety note: an ``asyncio.Lock`` serializes concurrent
    cache mutations so two callers racing on the same instrument
    don't both execute the DB round-trip. Read-only hits short-
    circuit before acquiring the lock.
    """

    def __init__(
        self,
        repository: Repository,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        """Store repository + wall-clock source for the converter."""
        self._repository = repository
        self._now: Callable[[], datetime] = now or (lambda: datetime.now(UTC))
        self._cache: dict[str, _CacheEntry] = {}
        self._cache_lock = asyncio.Lock()

    async def to_usd(
        self,
        instrument_public_id: str,
        quantity: Decimal,
    ) -> Decimal:
        """Convert ``quantity`` of the given instrument to USD notional.

        Args:
            instrument_public_id: UUID7 of the instrument. Resolved
                against the active Instrument row to read the
                ``quote`` currency.
            quantity: Submit quantity as :class:`decimal.Decimal`
                for precision-safe multiplication.

        Returns:
            ``quantity × last_price`` as :class:`decimal.Decimal`.

        Raises:
            PriceUnavailableError: on any of the reasons listed in
                the class's module docstring.
        """
        now = self._now()
        cached = self._cache.get(instrument_public_id)
        if cached is not None and (now - cached.cached_at).total_seconds() < CACHE_TTL_SECONDS:
            self._assert_fresh(cached, now, instrument_public_id)
            return quantity * cached.last_price

        async with self._cache_lock:
            cached = self._cache.get(instrument_public_id)
            if cached is not None and (now - cached.cached_at).total_seconds() < CACHE_TTL_SECONDS:
                self._assert_fresh(cached, now, instrument_public_id)
                return quantity * cached.last_price
            entry = await self._load_and_cache(instrument_public_id, now)
        return quantity * entry.last_price

    @staticmethod
    def _assert_fresh(entry: _CacheEntry, now: datetime, instrument_public_id: str) -> None:
        """Raise if the cached snapshot is beyond the staleness window."""
        age = (now - entry.snapshot_timestamp).total_seconds()
        if age > STALENESS_THRESHOLD_SECONDS:
            raise PriceUnavailableError(
                "price_stale",
                instrument_public_id,
                f"age={age:.0f}s > {STALENESS_THRESHOLD_SECONDS}s",
            )

    async def _load_and_cache(self, instrument_public_id: str, now: datetime) -> _CacheEntry:
        """Read active Instrument + MarketSnapshot, validate, cache.

        ``quote`` lives on the :class:`Symbol` table; resolved via
        temporal join on ``instruments.symbol_public_id``.
        """
        async with self._repository.session() as session:
            instr_q = await session.execute(
                select(Symbol.quote)
                .join(Instrument, Instrument.symbol_public_id == Symbol.public_id)
                .where(
                    Instrument.public_id == instrument_public_id,
                    *where_active(Instrument, now),
                    *where_active(Symbol, now),
                )
            )
            quote = instr_q.scalar_one_or_none()
            if quote is None:
                raise PriceUnavailableError("instrument_not_found", instrument_public_id)
            if quote != "USD":
                raise PriceUnavailableError(
                    "quote_currency_not_usd",
                    instrument_public_id,
                    f"quote={quote} (one-hop conversion only)",
                )
            snap_q = await session.execute(
                select(MarketSnapshot.last_price, MarketSnapshot.timestamp).where(
                    MarketSnapshot.instrument_public_id == instrument_public_id,
                    *where_active(MarketSnapshot, now),
                )
            )
            row = snap_q.one_or_none()
            if row is None:
                raise PriceUnavailableError("snapshot_missing", instrument_public_id)
            last_price, ts = row
            if last_price is None:
                raise PriceUnavailableError("last_price_null", instrument_public_id)

        entry = _CacheEntry(
            last_price=Decimal(str(last_price)),
            snapshot_timestamp=ts,
            cached_at=now,
        )
        age = (now - ts).total_seconds()
        if age > STALENESS_THRESHOLD_SECONDS:
            raise PriceUnavailableError(
                "price_stale",
                instrument_public_id,
                f"age={age:.0f}s > {STALENESS_THRESHOLD_SECONDS}s",
            )
        self._cache[instrument_public_id] = entry
        return entry

    def _invalidate(self, instrument_public_id: str) -> None:
        """Evict a single cache entry — test hook."""
        self._cache.pop(instrument_public_id, None)
