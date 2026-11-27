"""USD price oracle for :class:`TradingCapsEnforcer`.

Resolves quote-USD exposure for the rolling 24h-notional cap. Spot uses
quantity times price. Explicit linear contracts additionally apply their
base-denominated multiplier; inverse contracts use quote-denominated value per
contract independently of the current underlying price. Missing derivative
semantics never fall back to spot arithmetic.

The live candle pipeline supplies the primary price for the main instrument
and each FX plane. The newest active 1m candle supplies the sole evidence for
its instrument, including ``calculated``, ``native``, ``synthesized``, and
provisional ``complete=False`` bars. Freshness uses the bar's close time
(``open_at + 60s``, capped at ``now`` for the current minute, with ``open_at``
retained for future-opened bars). This keeps historical backfills and repairs
from refreshing old bars. The active ``market_snapshots`` row is a deprecated,
transitional fallback available only for instruments without active 1m candles.

Non-USD quotes require an independently evidenced current direct or inverse
forex plane against USD. The newest usable plane supplies the rate, with the
position-display venue order breaking timestamp ties. Disagreeing normalized
rates for the same instrument at that timestamp cause ``fx_plane_conflict``.
FX evidence is reread on every conversion. Contract notional measures exposure.

Cache + staleness:
In-process TTL cache (60s) keyed by ``instrument_public_id`` stores
the price, evidence timestamp, and original venue identity. Prices older than
300 seconds are unavailable. Contract metadata is reread on each cache hit, so
a revocation, expiry or obsolete interpretation cannot borrow the price cache's
lifetime.
"""

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from decimal import DecimalException

from snapper.application.portfolio.pnl_timeline_service import _fx_plane_tiebreak
from snapper.core.contract_valuation import ContractValuation
from snapper.core.contract_valuation import InverseContractValuation
from snapper.core.contract_valuation import LinearContractValuation
from snapper.core.contract_valuation import quote_notional
from snapper.data.repository import Repository
from snapper.data.repository import is_effective_unit_certified
from snapper.data.repository_types import ActiveCandlePriceRow
from snapper.data.repository_types import InstrumentSpecRow
from snapper.data.repository_types import InstrumentValuationMetadataRow
from snapper.data.repository_types import UsdFxCandleCandidateRow
from snapper.data.repository_types import UsdFxSnapshotCandidateRow
from snapper.infrastructure.exchanges.futures_spec_provenance import FUTURES_SPEC_ETL_VERSION
from snapper.infrastructure.exchanges.futures_spec_provenance import FUTURES_SPEC_SOURCE

CACHE_TTL_SECONDS = 60
CANDLE_INTERVAL_SECONDS = 60
STALENESS_THRESHOLD_SECONDS = 300
MAX_FUTURE_SKEW_SECONDS = 5


class PriceUnavailableError(Exception):
    """Raised when USD pricing cannot be resolved for an instrument.

    Carries ``reason`` so the cap enforcer can map to the
    error_code ``caps_price_unavailable`` with appropriate context.
    Reasons
        ``instrument_not_found``: no active Instrument row.
        ``snapshot_missing``: Instrument exists but neither an active
          1m candle nor an active MarketSnapshot row exists.
        ``last_price_null``: the applicable snapshot fallback has NULL ``last_price``.
        ``price_stale``: the selected price violates the 300s age or 5s future-skew bounds.
        ``fx_snapshot_missing``: no eligible active forex 1m candle or
          snapshot exists against USD.
        ``fx_price_stale``: no FX rate satisfies the age and clock-skew bounds.
        ``fx_price_invalid``: no positive finite FX rate or final USD value.
        ``fx_plane_conflict``: candidates for the same FX instrument at the
          newest usable timestamp disagree after normalization.
        ``last_price_invalid``: price is nonpositive or nonfinite.
        ``invalid_quantity``: quantity is not a nonnegative finite Decimal.
        ``contract_valuation_unavailable``: payoff, currency or unit evidence
          is missing, superseded or uncertified.
        ``contract_expiry_unavailable``: a dated contract has no expiry evidence.
        ``contract_expired``: the contract is at or beyond its expiry.
        ``instrument_definition_changed``: a cached identity no longer matches
          the current venue, base or quote definition.
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
    evidence_timestamp: datetime
    cached_at: datetime
    valuation_metadata: InstrumentValuationMetadataRow


@dataclass(frozen=True)
class _FxRate:
    """Normalized USD-per-quote rate retaining raw price and orientation evidence."""

    candidate: UsdFxSnapshotCandidateRow
    rate: Decimal


@dataclass(frozen=True)
class _MainPriceCandidate:
    """Parsed main-instrument price with its raw timestamp and source kind."""

    price: Decimal
    raw_timestamp: datetime
    is_candle: bool


def _candle_evidence_timestamp(open_at: datetime, now: datetime) -> datetime:
    """Return the freshness reference for a 1m candle opened at ``open_at``.

    A 1m candle opened at ``T`` covers market activity through ``T + 60s``.
    For completed bars (``T + 60s <= now``) the effective time is ``T + 60s``.
    For an in-progress current-minute bar (``T <= now < T + 60s``), capping at
    ``now`` prevents the unelapsed portion of the minute from tripping
    :data:`MAX_FUTURE_SKEW_SECONDS`. Future-opened bars (``T > now``) retain
    ``open_at`` so excessive clock skew fails closed. Naive datetimes pass
    through unchanged so :func:`_assert_timestamp_fresh` rejects them.

    Args:
        open_at: Candle interval start timestamp.
        now: Evaluation wall-clock timestamp (UTC-aware).

    Returns:
        Effective UTC timestamp for freshness checks and FX plane comparison.
    """
    if open_at.utcoffset() is None:
        return open_at
    close_at = open_at + timedelta(seconds=CANDLE_INTERVAL_SECONDS)
    return max(open_at, min(close_at, now))


def _parse_positive_decimal(raw_price: object) -> Decimal | None:
    """Parse a positive finite Decimal price, or return ``None`` when invalid.

    Args:
        raw_price: Numeric or string price candidate from repository evidence.

    Returns:
        Positive finite :class:`decimal.Decimal`, or ``None``.
    """
    if raw_price is None:
        return None
    try:
        parsed = Decimal(str(raw_price))
    except DecimalException:
        return None
    if not parsed.is_finite() or parsed <= 0:
        return None
    return parsed


def _assert_timestamp_fresh(
    timestamp: datetime, now: datetime, instrument_public_id: str, reason: str
) -> None:
    """Refuse naive, expired and excessively future-dated snapshot evidence."""
    try:
        if timestamp.utcoffset() is None:
            raise ValueError("Snapshot timestamp must be timezone-aware")
        age = (now - timestamp).total_seconds()
    except (TypeError, ValueError, OverflowError) as exc:
        raise PriceUnavailableError(reason, instrument_public_id, "invalid timestamp") from exc
    if not -MAX_FUTURE_SKEW_SECONDS <= age <= STALENESS_THRESHOLD_SECONDS:
        raise PriceUnavailableError(reason, instrument_public_id, f"snapshot age={age}s")


def _normalized_fx_rate(candidate: UsdFxSnapshotCandidateRow) -> _FxRate | None:
    """Normalize a positive raw FX price before comparison across orientations."""
    raw = _parse_positive_decimal(candidate["last_price"])
    if raw is None:
        return None
    try:
        rate = Decimal(1) / raw if candidate["base"] == "USD" else raw
        if not rate.is_finite() or rate <= 0:
            return None
        return _FxRate(candidate, rate)
    except DecimalException:
        return None


def _merge_fx_candidates(
    snapshot_rows: list[UsdFxSnapshotCandidateRow],
    candle_rows: list[UsdFxCandleCandidateRow],
    now: datetime,
) -> list[UsdFxSnapshotCandidateRow]:
    """Keep candles and deprecated transitional snapshots for candleless FX planes.

    Args:
        snapshot_rows: Active FX rows from ``market_snapshots``.
        candle_rows: Newest active 1m candle FX rows from ``candles``.
        now: Evaluation wall-clock timestamp (UTC-aware).

    Returns:
        One candidate per eligible instrument, before price and freshness checks.
    """
    candle_planes = {row["instrument_public_id"] for row in candle_rows}
    snapshot_fallbacks = [
        row for row in snapshot_rows if row["instrument_public_id"] not in candle_planes
    ]
    candle_candidates: list[UsdFxSnapshotCandidateRow] = [
        {
            "snapshot_public_id": f"candle:{row['candle_public_id']}",
            "instrument_public_id": row["instrument_public_id"],
            "exchange": row["exchange"],
            "base": row["base"],
            "quote": row["quote"],
            "last_price": row["close"],
            "timestamp": _candle_evidence_timestamp(row["open_at"], now),
        }
        for row in candle_rows
    ]
    return [*snapshot_fallbacks, *candle_candidates]


def _collect_main_candidate(
    snapshot_row: tuple[float | None, datetime] | None,
    candle_row: ActiveCandlePriceRow | None,
    instrument_public_id: str,
) -> _MainPriceCandidate:
    """Validate the candle or use the deprecated transitional snapshot fallback.

    Args:
        snapshot_row: Active ``(last_price, timestamp)`` tuple, or ``None``.
        candle_row: Newest active 1m candle row, or ``None``.
        instrument_public_id: Identity attached to any refusal.

    Returns:
        The primary or fallback price before its freshness check.

    Raises:
        PriceUnavailableError: If the selected price is missing, null, or invalid.
    """
    is_candle = candle_row is not None
    raw_price: float | None
    if candle_row is not None:
        raw_price, raw_timestamp = candle_row["close"], candle_row["open_at"]
    else:
        if snapshot_row is None:
            raise PriceUnavailableError("snapshot_missing", instrument_public_id)
        raw_price, raw_timestamp = snapshot_row
        if raw_price is None:
            raise PriceUnavailableError("last_price_null", instrument_public_id)
    price = _parse_positive_decimal(raw_price)
    if price is None:
        raise PriceUnavailableError("last_price_invalid", instrument_public_id)
    return _MainPriceCandidate(price, raw_timestamp, is_candle)


def _select_main_candidate(
    candidate: _MainPriceCandidate,
    now: datetime,
    instrument_public_id: str,
) -> tuple[Decimal, datetime]:
    """Check the freshness of the selected main-leg price.

    Args:
        candidate: Positive finite price from the chosen main-leg source.
        now: Wall-clock evaluation time after all repository awaits.
        instrument_public_id: Identity attached to any refusal.

    Returns:
        The validated price and its evidence timestamp.

    Raises:
        PriceUnavailableError: If the evidence violates the age or skew bounds.
    """
    evidence_ts = (
        _candle_evidence_timestamp(candidate.raw_timestamp, now)
        if candidate.is_candle
        else candidate.raw_timestamp
    )
    _assert_timestamp_fresh(evidence_ts, now, instrument_public_id, "price_stale")
    return candidate.price, evidence_ts


def _select_fx_rate(
    rows: list[UsdFxSnapshotCandidateRow],
    now: datetime,
    instrument_public_id: str,
    currency: str,
) -> _FxRate:
    """Select the newest usable FX rate from candles or snapshot fallbacks.

    At equal timestamps, follow the position-display fiat election. For PLN,
    Walutomat comes first, followed by other venues in lexical order. Other
    pairs use lexical venue order. Among Kraken and Walutomat, Kraken wins
    EUR-USD and GBP-USD, and Walutomat wins USD-PLN. Within a venue, direct
    currency-to-USD orientation comes first, then instrument public ID.

    Args:
        rows: Current candles and snapshot fallbacks for the USD pair.
        now: Final evaluation time for evidence freshness.
        instrument_public_id: Submitted instrument identity for error context.
        currency: Non-USD quote currency to convert.

    Returns:
        The elected USD-per-quote rate with its source evidence.

    Raises:
        PriceUnavailableError: If no usable rate exists or candidates for the
            same FX instrument disagree at the newest usable timestamp.
    """
    if not rows:
        raise PriceUnavailableError(
            "fx_snapshot_missing", instrument_public_id, f"quote={currency}"
        )
    normalized = [rate for row in rows if (rate := _normalized_fx_rate(row)) is not None]
    if not normalized:
        raise PriceUnavailableError("fx_price_invalid", instrument_public_id)
    fresh: list[_FxRate] = []
    for rate in normalized:
        try:
            _assert_timestamp_fresh(
                rate.candidate["timestamp"], now, instrument_public_id, "fx_price_stale"
            )
        except PriceUnavailableError:
            continue
        fresh.append(rate)
    if not fresh:
        raise PriceUnavailableError("fx_price_stale", instrument_public_id)
    newest = max(rate.candidate["timestamp"] for rate in fresh)
    planes = [rate for rate in fresh if rate.candidate["timestamp"] == newest]
    instrument_rates: dict[str, Decimal] = {}
    for rate in planes:
        fx_instrument_id = rate.candidate["instrument_public_id"]
        first_rate = instrument_rates.setdefault(fx_instrument_id, rate.rate)
        if first_rate != rate.rate:
            raise PriceUnavailableError("fx_plane_conflict", instrument_public_id)
    return min(
        planes,
        key=lambda rate: (
            _fx_plane_tiebreak(
                (rate.candidate["base"], rate.candidate["quote"], rate.candidate["exchange"]),
                "USD",
                prefer_walutomat=currency == "PLN",
            ),
            rate.candidate["instrument_public_id"],
        ),
    )


def _require_contract_evidence(
    spec: InstrumentSpecRow, now: datetime, instrument_public_id: str
) -> None:
    """Require fresh, current and unexpired contract quantity evidence.

    Args:
        spec: Catalog specification associated with this exact instrument.
        now: Evaluation time, including cache hits.
        instrument_public_id: Identity attached to any refusal.

    Raises:
        PriceUnavailableError: When contract evidence cannot support valuation.
    """
    current_version = (spec["spec_version"] or "").startswith(f"{FUTURES_SPEC_ETL_VERSION}:")
    certified = is_effective_unit_certified(spec, now)
    if (
        spec["spec_source"] != FUTURES_SPEC_SOURCE
        or not current_version
        or not certified
        or spec["quantity_unit"] != "contract_count"
    ):
        raise PriceUnavailableError("contract_valuation_unavailable", instrument_public_id)
    expiry = spec["expiry_at"]
    if spec["instrument_kind"] == "future" and expiry is None:
        raise PriceUnavailableError("contract_expiry_unavailable", instrument_public_id)
    if expiry is not None and (expiry.utcoffset() is None or expiry <= now):
        raise PriceUnavailableError("contract_expired", instrument_public_id)


def _requires_contract_valuation(metadata: InstrumentValuationMetadataRow) -> bool:
    """Detect derivative evidence without assuming that an unknown venue is spot.

    Args:
        metadata: Exact current venue identity and optional specification.

    Returns:
        Whether venue, quantity units, kind or payoff evidence requires contract math.
    """
    if metadata["exchange"] not in {"kraken", "walutomat", "paper", "polygon"}:
        return True
    spec = metadata["spec"]
    if spec is None:
        return False
    return (
        spec["quantity_unit"] not in {None, "base_asset"}
        or spec["instrument_kind"] not in {None, "spot", "etf"}
        or spec.get("contract_payoff_model") is not None
    )


def _contract_model(
    metadata: InstrumentValuationMetadataRow, now: datetime, instrument_public_id: str
) -> ContractValuation | None:
    """Resolve payoff semantics from explicit venue evidence, never symbol spelling.

    Args:
        metadata: Current venue identity and optional atomic specification.
        now: Evaluation time for certification and expiry.
        instrument_public_id: Identity attached to any refusal.

    Returns:
        An explicit derivative valuation model, or null for a spot instrument.

    Raises:
        PriceUnavailableError: If a derivative lacks usable monetary semantics.
    """
    spec = metadata["spec"]
    if not _requires_contract_valuation(metadata):
        return None
    if spec is None or metadata["exchange"] != "kraken_futures":
        raise PriceUnavailableError("contract_valuation_unavailable", instrument_public_id)
    if spec["instrument_kind"] not in {"perpetual", "future"}:
        raise PriceUnavailableError("contract_valuation_unavailable", instrument_public_id)
    _require_contract_evidence(spec, now, instrument_public_id)
    multiplier = spec["contract_size"]
    model = spec.get("contract_payoff_model")
    value_currency = spec.get("contract_value_currency")
    settlement_currency = spec.get("settlement_currency")
    if multiplier is None:
        raise PriceUnavailableError("contract_valuation_unavailable", instrument_public_id)
    currencies = (value_currency, settlement_currency)
    native_unit = spec.get("native_order_quantity_unit")
    if native_unit == "base_asset":
        if model != "linear" or multiplier != 1:
            raise PriceUnavailableError("contract_valuation_unavailable", instrument_public_id)
    elif native_unit != "contract_count":
        raise PriceUnavailableError("contract_valuation_unavailable", instrument_public_id)
    if model == "linear" and currencies == (metadata["base"], metadata["quote"]):
        return LinearContractValuation(multiplier)
    if model == "inverse" and currencies == (metadata["quote"], metadata["base"]):
        return InverseContractValuation(multiplier)
    raise PriceUnavailableError("contract_valuation_unavailable", instrument_public_id)


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
            Unsigned USD exposure as :class:`decimal.Decimal`, using the
            instrument's explicit payoff model when it is a derivative.

        Raises:
            PriceUnavailableError: on any of the reasons listed in
                the class's module docstring.
        """
        entry = await self._price_entry(instrument_public_id)
        currency = entry.valuation_metadata["quote"]
        fx_snapshots: list[UsdFxSnapshotCandidateRow] = []
        fx_candles: list[UsdFxCandleCandidateRow] = []
        if currency != "USD":
            fx_snapshots = await self._repository.get_active_usd_fx_snapshot_candidates(currency)
            fx_candles = await self._repository.get_active_usd_fx_candle_candidates(currency)
        now = self._now()
        self._assert_fresh(entry, now, instrument_public_id)
        notional = self._notional(entry, quantity, now, instrument_public_id)
        if currency == "USD":
            return notional
        rows = _merge_fx_candidates(fx_snapshots, fx_candles, now)
        try:
            rate = _select_fx_rate(rows, now, instrument_public_id, currency)
            result = notional * rate.rate
            if not result.is_finite():
                raise ValueError("USD notional must be finite")
            return result
        except (DecimalException, ValueError) as exc:
            raise PriceUnavailableError("fx_price_invalid", instrument_public_id) from exc

    async def _price_entry(self, instrument_public_id: str) -> _CacheEntry:
        """Coalesce cold loads while keeping metadata and FX awaits outside the lock."""
        now = self._now()
        cached = self._cache.get(instrument_public_id)
        if cached is None or (now - cached.cached_at).total_seconds() >= CACHE_TTL_SECONDS:
            async with self._cache_lock:
                now = self._now()
                cached = self._cache.get(instrument_public_id)
                if cached is None or (now - cached.cached_at).total_seconds() >= CACHE_TTL_SECONDS:
                    return await self._load_and_cache(instrument_public_id, now)
        return await self._value_cached(cached, instrument_public_id)

    async def _value_cached(self, entry: _CacheEntry, instrument_public_id: str) -> _CacheEntry:
        """Refresh current identity and payoff evidence while retaining the cached price.

        Args:
            entry: Cached price and original instrument identity.
            instrument_public_id: Exact venue-specific identity to reread.

        Returns:
            Cached main price with current identity and payoff evidence.

        Raises:
            PriceUnavailableError: If identity or payoff evidence is no longer valid.
        """
        original = entry.valuation_metadata
        current = await self._repository.get_active_instrument_valuation_metadata(
            instrument_public_id
        )
        if current is None:
            raise PriceUnavailableError("instrument_not_found", instrument_public_id)
        if (current["exchange"], current["base"], current["quote"]) != (
            original["exchange"],
            original["base"],
            original["quote"],
        ):
            raise PriceUnavailableError("instrument_definition_changed", instrument_public_id)
        if _requires_contract_valuation(original) and not _requires_contract_valuation(current):
            raise PriceUnavailableError("contract_valuation_unavailable", instrument_public_id)
        return replace(entry, valuation_metadata=current)

    @staticmethod
    def _notional(
        entry: _CacheEntry, quantity: Decimal, now: datetime, instrument_public_id: str
    ) -> Decimal:
        """Apply explicit payoff units and recheck evidence on every cached quote.

        Args:
            entry: Cached price and venue-specific payoff metadata.
            quantity: Nonnegative finite Decimal order quantity.
            now: Evaluation time for contract evidence.
            instrument_public_id: Identity attached to any refusal.

        Returns:
            Unsigned USD notional, independently of leverage or collateral.

        Raises:
            PriceUnavailableError: When quantity or contract valuation is unsafe.
        """
        if not isinstance(quantity, Decimal) or not quantity.is_finite() or quantity < 0:
            raise PriceUnavailableError("invalid_quantity", instrument_public_id)
        try:
            metadata = entry.valuation_metadata
            model = _contract_model(metadata, now, instrument_public_id)
            result = (
                quote_notional(model, quantity, entry.last_price)
                if model is not None
                else quantity * entry.last_price
            )
            if not result.is_finite():
                raise ValueError("Notional must be finite")
            return result
        except (ValueError, DecimalException) as exc:
            raise PriceUnavailableError(
                "contract_valuation_unavailable", instrument_public_id
            ) from exc

    @staticmethod
    def _assert_fresh(entry: _CacheEntry, now: datetime, instrument_public_id: str) -> None:
        """Raise if the cached price evidence is outside the freshness bounds."""
        _assert_timestamp_fresh(entry.evidence_timestamp, now, instrument_public_id, "price_stale")

    async def _load_and_cache(self, instrument_public_id: str, now: datetime) -> _CacheEntry:
        """Read and cache the current instrument's primary 1m candle price.

        ``quote`` lives on the :class:`Symbol` table; resolved via the
        repository's clock-free active-row reads (the current-version
        sentinel filter) — a caller-clock temporal read here could
        miss a freshly-written active row when this process's clock
        lags the writer's bus time (the Phase-0 kill-switch clock-skew
        class of bug). The deprecated transitional snapshot fallback is read
        only when no active 1m candle exists. Freshness is checked against
        ``self._now()`` after the repository reads finish.
        """
        quote = await self._repository.get_active_instrument_quote_currency(instrument_public_id)
        if quote is None:
            raise PriceUnavailableError("instrument_not_found", instrument_public_id)
        candle_row = await self._repository.get_active_latest_1m_candle_price(instrument_public_id)
        snapshot_row = (
            await self._repository.get_active_market_snapshot_price(instrument_public_id)
            if candle_row is None
            else None
        )
        candidate = _collect_main_candidate(snapshot_row, candle_row, instrument_public_id)
        metadata = await self._repository.get_active_instrument_valuation_metadata(
            instrument_public_id
        )
        if metadata is None or metadata["quote"] != quote:
            raise PriceUnavailableError("instrument_not_found", instrument_public_id)
        chosen_price, chosen_ts = _select_main_candidate(
            candidate, self._now(), instrument_public_id
        )
        entry = _CacheEntry(
            last_price=chosen_price,
            evidence_timestamp=chosen_ts,
            cached_at=now,
            valuation_metadata=metadata,
        )
        self._cache[instrument_public_id] = entry
        return entry

    def _invalidate(self, instrument_public_id: str) -> None:
        """Evict a single cache entry — test hook."""
        self._cache.pop(instrument_public_id, None)
