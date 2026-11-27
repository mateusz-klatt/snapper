"""USD price oracle for :class:`TradingCapsEnforcer`.

Resolves quote-USD exposure for the rolling 24h-notional cap. Spot uses
quantity times price. Explicit linear contracts additionally apply their
base-denominated multiplier; inverse contracts use quote-denominated value per
contract independently of the current underlying price. Missing derivative
semantics never fall back to spot arithmetic.

Price sourcing (both instrument and FX legs):
For each leg, evidence is the fresher positive finite price between:

1. The newest active 1m candle close in ``candles`` for that exact
   instrument/exchange (including ``calculated``, ``native``,
   ``synthesized``, and provisional ``complete=False`` bars). Freshness is
   evaluated against the bar's close-time (``open_at + 60s``, capped at
   ``now`` while the current minute bar is still open, and retaining
   ``open_at`` when future-dated so excessive skew fails closed) rather
   than persistence ``Candle.timestamp``, so historical backfills or repairs
   cannot mark days-old bars as live prices.
2. The active ``market_snapshots`` row (``last_price``, ``timestamp``).

Non-USD quotes require an independently evidenced current direct or inverse
forex plane against USD (selecting the newest fresh candidate across both
active 1m candles and ``market_snapshots``, and refusing disagreeing normalized
rates at the newest timestamp as ``fx_plane_conflict``). FX evidence is reread
on every conversion. Contract notional is exposure, not collateral, initial
margin or account equity.

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
        ``last_price_null``: MarketSnapshot row exists with
          ``last_price`` NULL and no valid 1m candle is available.
        ``price_stale``: neither 1m candle nor snapshot evidence satisfies
          the 300s age and 5s future-skew bounds.
        ``fx_snapshot_missing``: no eligible current forex 1m candle or
          snapshot exists against USD.
        ``fx_price_stale``: no FX rate satisfies the age and clock-skew bounds.
        ``fx_price_invalid``: no positive finite FX rate or final USD value.
        ``fx_plane_conflict``: newest FX planes disagree after normalization.
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
    snapshot_timestamp: datetime
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
        Effective UTC timestamp for staleness and cross-source comparison.
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
    """Merge FX market snapshots and newest active 1m candle closes for selection.

    Candle candidates carry a distinct deterministic ``candle:<public_id>``
    identity so tie-breaking never collides with snapshot public IDs.

    Args:
        snapshot_rows: Active FX rows from ``market_snapshots``.
        candle_rows: Newest active 1m candle FX rows from ``candles``.
        now: Evaluation wall-clock timestamp (UTC-aware).

    Returns:
        Combined candidate list ready for :func:`_select_fx_rate`.
    """
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
    return [*snapshot_rows, *candle_candidates]


def _collect_main_candidates(
    snapshot_row: tuple[float | None, datetime] | None,
    candle_row: ActiveCandlePriceRow | None,
    instrument_public_id: str,
) -> list[_MainPriceCandidate]:
    """Validate and collect main-leg candidates from snapshot and 1m candle.

    Args:
        snapshot_row: Active ``(last_price, timestamp)`` tuple, or ``None``.
        candle_row: Newest active 1m candle row, or ``None``.
        instrument_public_id: Identity attached to any refusal.

    Returns:
        Non-empty list of positive finite candidates before freshness checks.

    Raises:
        PriceUnavailableError: When neither source exists, snapshot price is
            NULL with no valid candle, or no candidate has a positive finite price.
    """
    if snapshot_row is None and candle_row is None:
        raise PriceUnavailableError("snapshot_missing", instrument_public_id)
    candidates: list[_MainPriceCandidate] = []
    snapshot_null = False
    if snapshot_row is not None:
        snap_price, snap_ts = snapshot_row
        if snap_price is None:
            snapshot_null = True
        else:
            parsed_snap = _parse_positive_decimal(snap_price)
            if parsed_snap is not None:
                candidates.append(_MainPriceCandidate(parsed_snap, snap_ts, is_candle=False))
    if candle_row is not None:
        parsed_candle = _parse_positive_decimal(candle_row["close"])
        if parsed_candle is not None:
            candidates.append(
                _MainPriceCandidate(parsed_candle, candle_row["open_at"], is_candle=True)
            )
    if candidates:
        return candidates
    if snapshot_null:
        raise PriceUnavailableError("last_price_null", instrument_public_id)
    raise PriceUnavailableError("last_price_invalid", instrument_public_id)


def _select_main_candidate(
    candidates: list[_MainPriceCandidate],
    now: datetime,
    instrument_public_id: str,
) -> tuple[Decimal, datetime]:
    """Select the freshest valid main-leg price within the staleness bounds.

    Args:
        candidates: Positive finite snapshot and/or 1m candle candidates.
        now: Wall-clock evaluation time after all repository awaits.
        instrument_public_id: Identity attached to any refusal.

    Returns:
        ``(price, evidence_timestamp)`` for the freshest valid source; equal
        timestamps prefer the live 1m candle plane.

    Raises:
        PriceUnavailableError: When no candidate satisfies the age and skew bounds.
    """
    fresh: list[tuple[Decimal, datetime, bool]] = []
    errors: list[PriceUnavailableError] = []
    for candidate in candidates:
        evidence_ts = (
            _candle_evidence_timestamp(candidate.raw_timestamp, now)
            if candidate.is_candle
            else candidate.raw_timestamp
        )
        try:
            _assert_timestamp_fresh(evidence_ts, now, instrument_public_id, "price_stale")
        except PriceUnavailableError as exc:
            errors.append(exc)
            continue
        fresh.append((candidate.price, evidence_ts, candidate.is_candle))
    if not fresh:
        raise errors[-1]
    chosen_price, chosen_ts, _ = max(fresh, key=lambda item: (item[1], item[2]))
    return chosen_price, chosen_ts


def _select_fx_rate(
    rows: list[UsdFxSnapshotCandidateRow],
    now: datetime,
    instrument_public_id: str,
    currency: str,
) -> _FxRate:
    """Select the newest usable plane, refusing disagreeing normalized rates."""
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
    if len({rate.rate for rate in planes}) != 1:
        raise PriceUnavailableError("fx_plane_conflict", instrument_public_id)
    return min(planes, key=lambda rate: rate.candidate["snapshot_public_id"])


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
        """Raise if the cached snapshot is beyond the staleness window."""
        _assert_timestamp_fresh(entry.snapshot_timestamp, now, instrument_public_id, "price_stale")

    async def _load_and_cache(self, instrument_public_id: str, now: datetime) -> _CacheEntry:
        """Read the CURRENT Instrument quote, 1m candle close and MarketSnapshot.

        ``quote`` lives on the :class:`Symbol` table; resolved via the
        repository's clock-free active-row reads (the current-version
        sentinel filter) — a caller-clock temporal read here could
        miss a freshly-written active row when this process's clock
        lags the writer's bus time (the Phase-0 kill-switch clock-skew
        class of bug). Both the active ``MarketSnapshot`` row and the
        newest active 1m candle close are read clock-free; freshness is
        enforced against ``self._now()`` after all awaits and the
        fresher valid source wins.
        """
        quote = await self._repository.get_active_instrument_quote_currency(instrument_public_id)
        if quote is None:
            raise PriceUnavailableError("instrument_not_found", instrument_public_id)
        snapshot_row = await self._repository.get_active_market_snapshot_price(instrument_public_id)
        candle_row = await self._repository.get_active_latest_1m_candle_price(instrument_public_id)
        candidates = _collect_main_candidates(snapshot_row, candle_row, instrument_public_id)
        metadata = await self._repository.get_active_instrument_valuation_metadata(
            instrument_public_id
        )
        if metadata is None or metadata["quote"] != quote:
            raise PriceUnavailableError("instrument_not_found", instrument_public_id)
        chosen_price, chosen_ts = _select_main_candidate(
            candidates, self._now(), instrument_public_id
        )
        entry = _CacheEntry(
            last_price=chosen_price,
            snapshot_timestamp=chosen_ts,
            cached_at=now,
            valuation_metadata=metadata,
        )
        self._cache[instrument_public_id] = entry
        return entry

    def _invalidate(self, instrument_public_id: str) -> None:
        """Evict a single cache entry — test hook."""
        self._cache.pop(instrument_public_id, None)
