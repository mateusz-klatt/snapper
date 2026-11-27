"""USD price oracle for :class:`TradingCapsEnforcer`.

Resolves quote-USD exposure for the rolling 24h-notional cap. Spot uses
quantity times price. Explicit linear contracts additionally apply their
base-denominated multiplier; inverse contracts use quote-denominated value per
contract independently of the current underlying price. Missing derivative
semantics never fall back to spot arithmetic.

The live candle pipeline supplies the price for the main instrument
and each FX plane. The newest active 1m candle supplies the sole evidence for
its instrument, including ``calculated``, ``native``, ``synthesized``, and
provisional ``complete=False`` bars. Freshness uses the bar's close time
(``open_at + 60s``, capped at ``now`` for the current minute, with ``open_at``
retained for future-opened bars). This keeps historical backfills and repairs
from refreshing old bars.

Non-USD quotes require an independently evidenced current direct or inverse
forex plane against USD. The newest usable plane supplies the rate. Disagreeing
normalized rates at the newest timestamp cause ``fx_plane_conflict``. FX evidence
is reread on every conversion. Contract notional measures exposure.

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
from decimal import Decimal
from decimal import DecimalException

from snapper.core import candle_price
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
from snapper.infrastructure.exchanges.futures_spec_provenance import FUTURES_SPEC_ETL_VERSION
from snapper.infrastructure.exchanges.futures_spec_provenance import FUTURES_SPEC_SOURCE

CACHE_TTL_SECONDS = 60
STALENESS_THRESHOLD_SECONDS = candle_price.STALENESS_THRESHOLD_SECONDS
MAX_FUTURE_SKEW_SECONDS = 5


class PriceUnavailableError(Exception):
    """Raised when USD pricing cannot be resolved for an instrument.

    Carries ``reason`` so the cap enforcer can map to the
    error_code ``caps_price_unavailable`` with appropriate context.
    Reasons
        ``instrument_not_found``: no active Instrument row.
        ``snapshot_missing``: Instrument exists but has no active 1m candle (legacy code).
        ``price_stale``: the selected price violates the 300s age or 5s future-skew bounds.
        ``fx_snapshot_missing``: no eligible active forex 1m candle exists against USD (legacy code).
        ``fx_price_stale``: no FX rate satisfies the age and clock-skew bounds.
        ``fx_price_invalid``: no positive finite FX rate or final USD value.
        ``fx_plane_conflict``: newest FX planes disagree after normalization.
        ``last_price_invalid``: candle close is null, nonpositive or nonfinite.
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

    candle_public_id: str
    timestamp: datetime
    rate: Decimal


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
    """Refuse naive, expired and excessively future-dated candle evidence."""
    try:
        if timestamp.utcoffset() is None:
            raise ValueError("Candle timestamp must be timezone-aware")
        age = (now - timestamp).total_seconds()
    except (TypeError, ValueError, OverflowError) as exc:
        raise PriceUnavailableError(reason, instrument_public_id, "invalid timestamp") from exc
    if not -MAX_FUTURE_SKEW_SECONDS <= age <= STALENESS_THRESHOLD_SECONDS:
        raise PriceUnavailableError(reason, instrument_public_id, f"candle age={age}s")


def _normalized_fx_rate(candidate: UsdFxCandleCandidateRow, now: datetime) -> _FxRate | None:
    """Normalize a positive raw FX price before comparison across orientations."""
    raw = _parse_positive_decimal(candidate["close"])
    if raw is None:
        return None
    try:
        rate = Decimal(1) / raw if candidate["base"] == "USD" else raw
        if not rate.is_finite() or rate <= 0:
            return None
        return _FxRate(
            candidate["candle_public_id"],
            candle_price.candle_evidence_timestamp(candidate["open_at"], now),
            rate,
        )
    except DecimalException:
        return None


def _collect_main_candidate(
    candle_row: ActiveCandlePriceRow | None, instrument_public_id: str
) -> tuple[Decimal, datetime]:
    """Extract the newest candle's positive close and interval start.

    Args:
        candle_row: Newest active 1m candle, if present.
        instrument_public_id: Identity attached to any refusal.

    Returns:
        Parsed close and interval start.

    Raises:
        PriceUnavailableError: If the candle is missing or the close is invalid.
    """
    if candle_row is None:
        raise PriceUnavailableError("snapshot_missing", instrument_public_id)
    price = _parse_positive_decimal(candle_row["close"])
    if price is None:
        raise PriceUnavailableError("last_price_invalid", instrument_public_id)
    return price, candle_row["open_at"]


def _select_fx_rate(
    rows: list[UsdFxCandleCandidateRow],
    now: datetime,
    instrument_public_id: str,
    currency: str,
) -> _FxRate:
    """Select the newest usable plane, refusing disagreeing normalized rates."""
    if not rows:
        raise PriceUnavailableError(
            "fx_snapshot_missing", instrument_public_id, f"quote={currency}"
        )
    normalized = [rate for row in rows if (rate := _normalized_fx_rate(row, now)) is not None]
    if not normalized:
        raise PriceUnavailableError("fx_price_invalid", instrument_public_id)
    fresh: list[_FxRate] = []
    for rate in normalized:
        try:
            _assert_timestamp_fresh(rate.timestamp, now, instrument_public_id, "fx_price_stale")
        except PriceUnavailableError:
            continue
        fresh.append(rate)
    if not fresh:
        raise PriceUnavailableError("fx_price_stale", instrument_public_id)
    newest = max(rate.timestamp for rate in fresh)
    planes = [rate for rate in fresh if rate.timestamp == newest]
    if len({rate.rate for rate in planes}) != 1:
        raise PriceUnavailableError("fx_plane_conflict", instrument_public_id)
    return min(planes, key=lambda rate: rate.candle_public_id)


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
        fx_candles: list[UsdFxCandleCandidateRow] = []
        if currency != "USD":
            fx_candles = await self._repository.get_active_usd_fx_candle_candidates(currency)
        now = self._now()
        self._assert_fresh(entry, now, instrument_public_id)
        notional = self._notional(entry, quantity, now, instrument_public_id)
        if currency == "USD":
            return notional
        try:
            rate = _select_fx_rate(fx_candles, now, instrument_public_id, currency)
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
        class of bug). Freshness is checked after the repository reads finish.
        """
        quote = await self._repository.get_active_instrument_quote_currency(instrument_public_id)
        if quote is None:
            raise PriceUnavailableError("instrument_not_found", instrument_public_id)
        candle_row = await self._repository.get_active_latest_1m_candle_price(instrument_public_id)
        price, open_at = _collect_main_candidate(candle_row, instrument_public_id)
        metadata = await self._repository.get_active_instrument_valuation_metadata(
            instrument_public_id
        )
        if metadata is None or metadata["quote"] != quote:
            raise PriceUnavailableError("instrument_not_found", instrument_public_id)
        evaluated_at = self._now()
        evidence_timestamp = candle_price.candle_evidence_timestamp(open_at, evaluated_at)
        _assert_timestamp_fresh(
            evidence_timestamp, evaluated_at, instrument_public_id, "price_stale"
        )
        entry = _CacheEntry(
            last_price=price,
            evidence_timestamp=evidence_timestamp,
            cached_at=now,
            valuation_metadata=metadata,
        )
        self._cache[instrument_public_id] = entry
        return entry

    def _invalidate(self, instrument_public_id: str) -> None:
        """Evict a single cache entry — test hook."""
        self._cache.pop(instrument_public_id, None)
