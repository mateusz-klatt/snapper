"""Operator-only activation evidence for F6 venue-minimum basket exclusions.

This module evaluates the already-shipped pure venue-minimum predicate without
feeding its result into the portfolio snapshotter. It produces a bounded report
and a fail-closed threshold decision suitable for a one-time activation review
or a recurring external scheduler. Published equity, sample rows, and API
responses remain unchanged.
"""

import asyncio
import json
import math
from bisect import bisect_right
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from sys import float_info
from typing import Annotated
from typing import Final
from typing import Literal
from typing import NoReturn

import typer

from snapper.application.portfolio.basket_realizability import ExcludedBalance
from snapper.application.portfolio.basket_realizability import RuleWithheldCause
from snapper.application.portfolio.basket_realizability import VenueOrderMinimumVersion
from snapper.application.portfolio.basket_realizability import partition_realizable_balances
from snapper.application.portfolio.basket_realizability import resolve_venue_order_minimums
from snapper.application.portfolio.pnl_snapshot_planner import evaluate_basket
from snapper.application.portfolio.pnl_snapshotter import is_futures_class_exchange
from snapper.application.portfolio.pnl_snapshotter import resolve_spot_venues
from snapper.application.process_manager.executor_topology import MINT_WALLET_PIN_SETTING_KEY
from snapper.application.process_manager.executor_topology import ExecutorCredentialDisposition
from snapper.application.process_manager.executor_topology import resolve_executor_topology
from snapper.application.services.settings import SettingsService
from snapper.config.settings import get_bootstrap_settings
from snapper.core.json_types import JsonObject
from snapper.core.json_types import JsonValue
from snapper.data.repository import PortfolioPnlSampleQuery
from snapper.data.repository import Repository
from snapper.data.repository import dispose_repositories
from snapper.data.repository import get_repository
from snapper.data.repository_types import PNL_SAMPLE_CALC_VERSION
from snapper.data.repository_types import PnlCryptoUsdPlaneRow
from snapper.data.repository_types import PortfolioPnlSampleRow
from snapper.data.repository_types import VenueAccountObservationAttemptRow
from snapper.data.repository_types import WalletCredentialRow

_MINUTE = timedelta(minutes=1)
_FINALIZATION_LAG = timedelta(minutes=2)
_PRICE_LOOKBACK = timedelta(hours=24)
_EQUITY_MAX_AGE = timedelta(minutes=5)
_MAX_WINDOW_MINUTES = 7 * 24 * 60
_ACTIVATION_WINDOW_MINUTES = 24 * 60
_USD: Final = "USD"

EXIT_REFUSED: Final = 1
EXIT_INCOMPLETE: Final = 3
EXIT_BREACH: Final = 4

pnl_minimum_gate_app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Report and evaluate the disconnected F6 venue-minimum activation bound.",
)

type GateIncompleteCause = Literal[
    "no_authoritative_minutes",
    "partial_authoritative_window",
    "no_resolved_minimum_legs",
    "rule_w_withheld",
    "missing_usd_close",
    "no_positive_complete_equity",
    "invalid_complete_equity",
    "missing_causal_equity",
    "stale_complete_equity",
]
type GateStatus = Literal["pass", "breach", "incomplete"]
type GatePurpose = Literal["activation", "standing", "inspect"]


@dataclass(frozen=True)
class ScopeCredentialEvidence:
    """Non-secret executor-topology evidence for one wallet credential."""

    public_id: str
    exchange: str
    credential_type: str
    executor_disposition: str
    spot_eligible: bool
    included_in_denominator: bool


@dataclass(frozen=True)
class LiveWalletScopeEvidence:
    """Exact live venue denominator and the non-secret inputs that produced it."""

    credential_catalog_as_of: datetime
    active_credential_catalog_size: int
    resolved_mint_wallet_public_id: str | None
    credentials: tuple[ScopeCredentialEvidence, ...]


@dataclass(frozen=True)
class GateCommandRequest:
    """CLI request before live executor-backed venue scope is resolved."""

    wallet_public_id: str
    as_of: datetime
    window_minutes: int
    purpose: GatePurpose

    def __post_init__(self) -> None:
        """Validate the horizon contract before any database read."""
        if not self.wallet_public_id:
            raise ValueError("wallet_public_id must be non-empty")
        if self.as_of.tzinfo is None or self.as_of.utcoffset() is None:
            raise ValueError("as_of must be timezone-aware")
        if not 1 <= self.window_minutes <= _MAX_WINDOW_MINUTES:
            raise ValueError(f"window_minutes must be between 1 and {_MAX_WINDOW_MINUTES}")
        if self.purpose == "activation" and self.window_minutes != _ACTIVATION_WINDOW_MINUTES:
            raise ValueError("activation report must cover exactly 1440 minutes")
        if self.purpose == "standing" and self.window_minutes < _ACTIVATION_WINDOW_MINUTES:
            raise ValueError("standing check must cover at least 1440 minutes")
        object.__setattr__(self, "as_of", self.as_of.astimezone(UTC))


@dataclass(frozen=True)
class ActivationGateRequest:
    """One bounded read-only F6 evidence request."""

    wallet_public_id: str
    mode: Literal["live", "paper"] | str
    exchanges: tuple[str, ...]
    as_of: datetime
    window_minutes: int
    purpose: GatePurpose = "inspect"
    scope_evidence: LiveWalletScopeEvidence | None = None

    def __post_init__(self) -> None:
        """Validate and canonicalize scope, horizon, and venue identity."""
        if not self.wallet_public_id:
            raise ValueError("wallet_public_id must be non-empty")
        if self.mode != "live":
            raise ValueError("F6 activation evidence is defined only for live mode")
        if self.as_of.tzinfo is None or self.as_of.utcoffset() is None:
            raise ValueError("as_of must be timezone-aware")
        if not 1 <= self.window_minutes <= _MAX_WINDOW_MINUTES:
            raise ValueError(f"window_minutes must be between 1 and {_MAX_WINDOW_MINUTES}")
        exchanges = tuple(dict.fromkeys(exchange for exchange in self.exchanges if exchange))
        if not exchanges:
            raise ValueError("at least one non-empty exchange is required")
        if self.purpose == "activation" and self.window_minutes != _ACTIVATION_WINDOW_MINUTES:
            raise ValueError("activation report must cover exactly 1440 minutes")
        if self.purpose == "standing" and self.window_minutes < _ACTIVATION_WINDOW_MINUTES:
            raise ValueError("standing check must cover at least 1440 minutes")
        object.__setattr__(self, "exchanges", exchanges)
        object.__setattr__(self, "as_of", self.as_of.astimezone(UTC))

    @property
    def minutes(self) -> tuple[datetime, ...]:
        """Return the inclusive finalized minute grid in ascending order.

        Returns:
            Finalized UTC grid minutes in ascending order.
        """
        end = self.as_of.replace(second=0, microsecond=0) - _FINALIZATION_LAG
        start = end - (self.window_minutes - 1) * _MINUTE
        return tuple(start + offset * _MINUTE for offset in range(self.window_minutes))


@dataclass(frozen=True)
class GateBasketMinute:
    """Authoritative balances or exact refusal reasons for one grid minute."""

    minute: datetime
    balances: Mapping[tuple[str, str], float] | None
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        """Require one unambiguous authority state."""
        if (self.balances is None) == (not self.reason_codes):
            raise ValueError("basket minute must carry balances or refusal reasons, exclusively")


@dataclass(frozen=True)
class UsdCloseEvidence:
    """The conservative direct-USD close selected for one bound."""

    currency: str
    close: float
    open_at: datetime
    exchange: str
    native_symbol: str
    instrument_public_id: str
    candle_id: int
    candle_public_id: str
    candle_timestamp: datetime
    age_seconds: int


@dataclass(frozen=True)
class UsdCloseSeries:
    """Usable direct-USD rows for one instrument, ordered once by provenance."""

    open_times: tuple[datetime, ...]
    rows: tuple[PnlCryptoUsdPlaneRow, ...]


@dataclass(frozen=True)
class UsdCloseIndex:
    """Direct-USD evidence indexed once by base currency and instrument."""

    series: Mapping[str, Mapping[str, UsdCloseSeries]]


@dataclass(frozen=True)
class BoundedExclusion:
    """One would-be exclusion with independent USD-bound evidence."""

    exchange: str
    currency: str
    quantity: float
    min_order_size: float
    pairs_considered: int
    minimum_instrument_public_id: str | None
    minimum_symbol_public_id: str | None
    minimum_native_symbol: str | None
    minimum_spec_public_id: str | None
    minimum_spec_source: str | None
    minimum_spec_version: str | None
    minimum_spec_observed_at: datetime | None
    price: UsdCloseEvidence | None
    bound_usd: float | None
    bound_overflow: bool


@dataclass(frozen=True)
class MinuteImpact:
    """The exact non-activating F6 result for one authoritative minute."""

    minute: datetime
    legs: tuple[BoundedExclusion, ...]
    known_bound_usd: float
    complete_bound_usd: float | None
    bound_overflow: bool
    causal_equity_floor_usd: float | None
    causal_equity_floor_minute: datetime | None
    known_bound_share: float | None
    rule_withheld: bool
    withheld_cause: RuleWithheldCause | None


@dataclass(frozen=True)
class ActivationGateReport:
    """Bounded evidence and completeness state for one activation review."""

    request: ActivationGateRequest
    authoritative_minutes: int
    unauthoritative_minutes: int
    unauthoritative_reason_counts: Mapping[str, int]
    resolved_legs_total: int
    unresolved_legs_total: int
    exclusion_minutes: int
    excluded_legs: int
    rule_withheld_minutes: int
    withheld_cause_counts: Mapping[str, int]
    equity_sample_count: int
    equity_floor_usd: float | None
    equity_floor_minute: datetime | None
    impacts: tuple[MinuteImpact, ...]
    max_known_minute_bound_usd: float
    max_known_minute_bound_share: float | None
    definite_breach: bool
    incomplete_causes: tuple[GateIncompleteCause, ...]


@dataclass(frozen=True)
class ActivationThresholdDecision:
    """One scheduler-friendly threshold verdict."""

    status: GateStatus
    max_bound_share: float
    observed_max_bound_share: float | None


@dataclass(frozen=True)
class _GateCompletenessInputs:
    """Counters needed to derive the closed activation refusal vocabulary."""

    authoritative_minutes: int
    requested_minutes: int
    resolved_legs: int
    rule_withheld_minutes: int
    impact_causes: Sequence[GateIncompleteCause]
    equity_causes: Sequence[GateIncompleteCause]


@dataclass(frozen=True)
class _EquityPoint:
    """One valid complete in-window equity denominator point."""

    minute: datetime
    equity_usd: float


@dataclass(frozen=True)
class _EquityAssessment:
    """Causal denominator evidence attached to exclusion minutes."""

    impacts: tuple[MinuteImpact, ...]
    sample_count: int
    floor_usd: float | None
    floor_minute: datetime | None
    max_known_share: float | None
    causes: tuple[GateIncompleteCause, ...]


def _indexable_close(row: PnlCryptoUsdPlaneRow) -> bool:
    """Return whether one row can ever prove a positive direct-USD bound."""
    close = row["close"]
    return (
        row["quote"] == "USD"
        and not isinstance(close, bool)
        and math.isfinite(close)
        and close > 0.0
    )


def index_usd_closes(rows: Sequence[PnlCryptoUsdPlaneRow]) -> UsdCloseIndex:
    """Index usable direct-USD rows once for all report-minute selections.

    Args:
        rows: Certified direct-USD candle rows for the complete report window.

    Returns:
        Immutable per-currency and per-instrument close series.
    """
    grouped: dict[str, dict[str, list[PnlCryptoUsdPlaneRow]]] = {}
    for row in rows:
        if not _indexable_close(row):
            continue
        grouped.setdefault(row["base"], {}).setdefault(row["instrument_public_id"], []).append(row)
    indexed: dict[str, dict[str, UsdCloseSeries]] = {}
    for currency, instruments in grouped.items():
        indexed[currency] = {}
        for instrument, instrument_rows in instruments.items():
            ordered = tuple(
                sorted(
                    instrument_rows,
                    key=lambda candidate: (
                        candidate["open_at"],
                        candidate["candle_timestamp"],
                        candidate["candle_id"],
                        candidate["candle_public_id"],
                    ),
                )
            )
            indexed[currency][instrument] = UsdCloseSeries(
                open_times=tuple(row["open_at"] for row in ordered),
                rows=ordered,
            )
    return UsdCloseIndex(series=indexed)


def _latest_indexed_closes(
    index: UsdCloseIndex,
    currency: str,
    minute: datetime,
) -> tuple[PnlCryptoUsdPlaneRow, ...]:
    """Select each instrument's latest causal close through binary search."""
    selected: list[PnlCryptoUsdPlaneRow] = []
    cutoff = minute - _MINUTE
    for series in index.series.get(currency, {}).values():
        position = bisect_right(series.open_times, cutoff) - 1
        if position < 0:
            continue
        row = series.rows[position]
        if minute - row["open_at"] <= _PRICE_LOOKBACK:
            selected.append(row)
    return tuple(selected)


def select_best_usd_close(
    index: UsdCloseIndex,
    currency: str,
    minute: datetime,
) -> UsdCloseEvidence | None:
    """Select the highest latest-per-plane direct-USD close without lookahead.

    Args:
        index: Certified direct-USD candle evidence indexed once for the report.
        currency: Balance currency whose omission bound is being measured.
        minute: Grid minute at which the close must already be finalized.

    Returns:
        Selected conservative close evidence, or ``None`` when no causal close exists.
    """
    candidates = _latest_indexed_closes(index, currency, minute)
    if not candidates:
        return None
    chosen = max(
        candidates,
        key=lambda row: (
            row["close"],
            row["open_at"],
            row["exchange"],
            row["native_symbol"],
            row["instrument_public_id"],
            row["candle_id"],
            row["candle_public_id"],
        ),
    )
    return UsdCloseEvidence(
        currency=currency,
        close=chosen["close"],
        open_at=chosen["open_at"],
        exchange=chosen["exchange"],
        native_symbol=chosen["native_symbol"],
        instrument_public_id=chosen["instrument_public_id"],
        candle_id=chosen["candle_id"],
        candle_public_id=chosen["candle_public_id"],
        candle_timestamp=chosen["candle_timestamp"],
        age_seconds=int((minute - chosen["open_at"]).total_seconds()),
    )


def _bounded_exclusion(
    excluded: ExcludedBalance,
    minute: datetime,
    prices: UsdCloseIndex,
) -> tuple[BoundedExclusion, GateIncompleteCause | None]:
    """Attach independent price evidence and a finite bound to one exclusion."""
    price = select_best_usd_close(prices, excluded.currency, minute)
    bound: float | None = None
    bound_overflow = False
    cause: GateIncompleteCause | None = None
    if price is None:
        cause = "missing_usd_close"
    else:
        bound = excluded.min_order_size * price.close
        if not math.isfinite(bound):
            bound = float_info.max
            bound_overflow = True
    return (
        BoundedExclusion(
            exchange=excluded.exchange,
            currency=excluded.currency,
            quantity=excluded.quantity,
            min_order_size=excluded.min_order_size,
            pairs_considered=excluded.pairs_considered,
            minimum_instrument_public_id=excluded.instrument_public_id,
            minimum_symbol_public_id=excluded.symbol_public_id,
            minimum_native_symbol=excluded.native_symbol,
            minimum_spec_public_id=excluded.spec_public_id,
            minimum_spec_source=excluded.spec_source,
            minimum_spec_version=excluded.spec_version,
            minimum_spec_observed_at=excluded.spec_observed_at,
            price=price,
            bound_usd=bound,
            bound_overflow=bound_overflow,
        ),
        cause,
    )


def _minute_impact(
    basket: GateBasketMinute,
    minimum_versions: Sequence[VenueOrderMinimumVersion],
    prices: UsdCloseIndex,
) -> tuple[MinuteImpact | None, int, int, tuple[GateIncompleteCause, ...]]:
    """Apply the disconnected F6 predicate and size one authoritative minute."""
    if basket.balances is None:
        return None, 0, 0, ()
    resolutions = resolve_venue_order_minimums(minimum_versions, basket.minute)
    partition = partition_realizable_balances(basket.balances, resolutions)
    if not partition.excluded and not partition.rule_withheld:
        return None, partition.resolved_legs, partition.unresolved_legs, ()
    legs: list[BoundedExclusion] = []
    causes: list[GateIncompleteCause] = []
    for excluded in partition.excluded:
        leg, cause = _bounded_exclusion(excluded, basket.minute, prices)
        legs.append(leg)
        if cause is not None:
            causes.append(cause)
    known_bound, complete_bound, bound_overflow = _bound_totals(legs)
    return (
        MinuteImpact(
            minute=basket.minute,
            legs=tuple(legs),
            known_bound_usd=known_bound,
            complete_bound_usd=complete_bound,
            bound_overflow=bound_overflow,
            causal_equity_floor_usd=None,
            causal_equity_floor_minute=None,
            known_bound_share=None,
            rule_withheld=partition.rule_withheld,
            withheld_cause=partition.withheld_cause,
        ),
        partition.resolved_legs,
        partition.unresolved_legs,
        tuple(causes),
    )


def _bound_totals(
    legs: Sequence[BoundedExclusion],
) -> tuple[float, float | None, bool]:
    """Sum finite leg bounds and retain overflow as a definite breach proof."""
    values = [leg.bound_usd for leg in legs if leg.bound_usd is not None]
    overflow = any(leg.bound_overflow for leg in legs)
    try:
        total = math.fsum(values)
    except OverflowError:
        return float_info.max, None, True
    complete = total if len(values) == len(legs) and not overflow else None
    return total, complete, overflow


def _equity_points(
    request: ActivationGateRequest,
    samples: Sequence[PortfolioPnlSampleRow],
) -> tuple[tuple[_EquityPoint, ...], bool]:
    """Select positive complete samples inside the exact report window."""
    start, end = request.minutes[0], request.minutes[-1]
    points: list[_EquityPoint] = []
    invalid_complete = False
    for sample in samples:
        minute = sample["point_time"]
        if minute.tzinfo is None or minute.utcoffset() is None:
            if sample["valuation_status"] == "complete":
                invalid_complete = True
            continue
        minute = minute.astimezone(UTC)
        if minute < start or minute > end or sample["valuation_status"] != "complete":
            continue
        cash = sample["cash_usd"]
        position = sample["position_value_usd"]
        if cash is None or position is None:
            invalid_complete = True
            continue
        equity = cash + position
        if not math.isfinite(equity) or equity <= 0.0:
            invalid_complete = True
            continue
        points.append(_EquityPoint(minute=minute, equity_usd=equity))
    return (
        tuple(sorted(points, key=lambda point: (point.minute, point.equity_usd))),
        invalid_complete,
    )


def _prefix_equity_floors(points: Sequence[_EquityPoint]) -> tuple[_EquityPoint, ...]:
    """Precompute each causal prefix's conservative equity denominator."""
    floors: list[_EquityPoint] = []
    for point in points:
        if not floors or (point.equity_usd, point.minute) < (
            floors[-1].equity_usd,
            floors[-1].minute,
        ):
            floors.append(point)
        else:
            floors.append(floors[-1])
    return tuple(floors)


def _attach_causal_equity(
    impact: MinuteImpact,
    points: Sequence[_EquityPoint],
    floors: Sequence[_EquityPoint],
) -> tuple[MinuteImpact, GateIncompleteCause | None]:
    """Attach a fresh, causal prefix floor to one actual exclusion minute."""
    times = tuple(point.minute for point in points)
    position = bisect_right(times, impact.minute)
    if position == 0:
        return impact, "missing_causal_equity"
    latest = points[position - 1]
    if impact.minute - latest.minute > _EQUITY_MAX_AGE:
        return impact, "stale_complete_equity"
    floor = floors[position - 1]
    return (
        replace(
            impact,
            causal_equity_floor_usd=floor.equity_usd,
            causal_equity_floor_minute=floor.minute,
            known_bound_share=_bound_share(impact.known_bound_usd, floor.equity_usd),
        ),
        None,
    )


def _equity_assessment(
    request: ActivationGateRequest,
    impacts: Sequence[MinuteImpact],
    samples: Sequence[PortfolioPnlSampleRow],
) -> _EquityAssessment:
    """Assess current coverage and every causal exclusion denominator."""
    points, invalid_complete = _equity_points(request, samples)
    floor = min(points, key=lambda point: (point.equity_usd, point.minute), default=None)
    exclusion_impacts = [impact for impact in impacts if impact.legs]
    causes: list[GateIncompleteCause] = []
    if invalid_complete and exclusion_impacts:
        causes.append("invalid_complete_equity")
    if not exclusion_impacts:
        return _EquityAssessment(
            impacts=tuple(impacts),
            sample_count=len(points),
            floor_usd=floor.equity_usd if floor is not None else None,
            floor_minute=floor.minute if floor is not None else None,
            max_known_share=0.0,
            causes=tuple(causes),
        )
    if not points:
        causes.append("no_positive_complete_equity")
        return _EquityAssessment(
            impacts=tuple(impacts),
            sample_count=0,
            floor_usd=None,
            floor_minute=None,
            max_known_share=None,
            causes=tuple(causes),
        )
    if request.minutes[-1] - points[-1].minute > _EQUITY_MAX_AGE:
        causes.append("stale_complete_equity")
    floors = _prefix_equity_floors(points)
    assessed: list[MinuteImpact] = []
    for impact in impacts:
        if not impact.legs:
            assessed.append(impact)
            continue
        updated, cause = _attach_causal_equity(impact, points, floors)
        assessed.append(updated)
        if cause is not None:
            causes.append(cause)
    shares = [
        impact.known_bound_share for impact in assessed if impact.known_bound_share is not None
    ]
    return _EquityAssessment(
        impacts=tuple(assessed),
        sample_count=len(points),
        floor_usd=floor.equity_usd if floor is not None else None,
        floor_minute=floor.minute if floor is not None else None,
        max_known_share=max(shares, default=None),
        causes=tuple(dict.fromkeys(causes)),
    )


def _basket_index(
    request: ActivationGateRequest,
    baskets: Sequence[GateBasketMinute],
) -> Mapping[datetime, GateBasketMinute]:
    """Validate one and only one input state per requested grid minute."""
    requested = set(request.minutes)
    indexed: dict[datetime, GateBasketMinute] = {}
    for basket in baskets:
        if basket.minute not in requested:
            raise ValueError("basket minute lies outside the requested grid")
        if basket.minute in indexed:
            raise ValueError("duplicate basket minute in activation report input")
        indexed[basket.minute] = basket
    return indexed


def _reason_counts(
    request: ActivationGateRequest,
    baskets: Mapping[datetime, GateBasketMinute],
) -> tuple[int, int, Mapping[str, int]]:
    """Count authoritative coverage and every fail-closed basket reason."""
    authoritative = 0
    reasons: dict[str, int] = {}
    for minute in request.minutes:
        basket = baskets.get(minute)
        if basket is not None and basket.balances is not None:
            authoritative += 1
            continue
        codes = basket.reason_codes if basket is not None else ("report_input_missing",)
        for code in codes:
            reasons[code] = reasons.get(code, 0) + 1
    return authoritative, request.window_minutes - authoritative, dict(sorted(reasons.items()))


def _withheld_counts(impacts: Sequence[MinuteImpact]) -> tuple[int, Mapping[str, int]]:
    """Fold Rule-W refusals into deterministic report counters."""
    counts: dict[str, int] = {}
    for impact in impacts:
        if impact.withheld_cause is not None:
            counts[impact.withheld_cause] = counts.get(impact.withheld_cause, 0) + 1
    return sum(counts.values()), dict(sorted(counts.items()))


def _incomplete_causes(inputs: _GateCompletenessInputs) -> tuple[GateIncompleteCause, ...]:
    """Build the closed, stable fail-closed cause vocabulary."""
    causes: list[GateIncompleteCause] = []
    if inputs.authoritative_minutes == 0:
        causes.append("no_authoritative_minutes")
    elif inputs.authoritative_minutes < inputs.requested_minutes:
        causes.append("partial_authoritative_window")
    if inputs.resolved_legs == 0:
        causes.append("no_resolved_minimum_legs")
    if inputs.rule_withheld_minutes:
        causes.append("rule_w_withheld")
    if "missing_usd_close" in inputs.impact_causes:
        causes.append("missing_usd_close")
    for candidate in (
        "no_positive_complete_equity",
        "invalid_complete_equity",
        "missing_causal_equity",
        "stale_complete_equity",
    ):
        if candidate in inputs.equity_causes:
            causes.append(candidate)
    return tuple(causes)


def _bound_share(
    bound: float,
    equity_floor: float,
) -> float:
    """Compute a finite alarm share while preserving overflow as a known breach."""
    ratio = bound / equity_floor
    return ratio if math.isfinite(ratio) else float_info.max


def build_activation_report(
    request: ActivationGateRequest,
    baskets: Sequence[GateBasketMinute],
    minimum_versions: Sequence[VenueOrderMinimumVersion],
    prices: Sequence[PnlCryptoUsdPlaneRow],
    equity_samples: Sequence[PortfolioPnlSampleRow],
) -> ActivationGateReport:
    """Build the pure 24-hour evidence report without changing portfolio state.

    Args:
        request: Bounded scope and temporal horizon.
        baskets: Authoritative balances or refusal reasons for requested minutes.
        minimum_versions: Temporal S1 minimum evidence for observed currencies.
        prices: Independent direct-USD closes used only to size omissions.
        equity_samples: Complete current-epoch samples used for the equity floor.

    Returns:
        Deterministic non-activating impact report and completeness evidence.
    """
    indexed = _basket_index(request, baskets)
    authoritative, unauthoritative, reason_counts = _reason_counts(request, indexed)
    price_index = index_usd_closes(prices)
    impacts: list[MinuteImpact] = []
    impact_causes: list[GateIncompleteCause] = []
    resolved_legs = 0
    unresolved_legs = 0
    for minute in request.minutes:
        basket = indexed.get(minute)
        if basket is None or basket.balances is None:
            continue
        impact, minute_resolved, minute_unresolved, causes = _minute_impact(
            basket, minimum_versions, price_index
        )
        resolved_legs += minute_resolved
        unresolved_legs += minute_unresolved
        impact_causes.extend(causes)
        if impact is not None:
            impacts.append(impact)
    excluded_legs = sum(len(impact.legs) for impact in impacts)
    equity = _equity_assessment(request, impacts, equity_samples)
    max_known_bound = max((impact.known_bound_usd for impact in impacts), default=0.0)
    withheld_minutes, withheld_counts = _withheld_counts(impacts)
    incomplete = _incomplete_causes(
        _GateCompletenessInputs(
            authoritative_minutes=authoritative,
            requested_minutes=request.window_minutes,
            resolved_legs=resolved_legs,
            rule_withheld_minutes=withheld_minutes,
            impact_causes=impact_causes,
            equity_causes=equity.causes,
        )
    )
    return ActivationGateReport(
        request=request,
        authoritative_minutes=authoritative,
        unauthoritative_minutes=unauthoritative,
        unauthoritative_reason_counts=reason_counts,
        resolved_legs_total=resolved_legs,
        unresolved_legs_total=unresolved_legs,
        exclusion_minutes=sum(bool(impact.legs) for impact in impacts),
        excluded_legs=excluded_legs,
        rule_withheld_minutes=withheld_minutes,
        withheld_cause_counts=withheld_counts,
        equity_sample_count=equity.sample_count,
        equity_floor_usd=equity.floor_usd,
        equity_floor_minute=equity.floor_minute,
        impacts=equity.impacts,
        max_known_minute_bound_usd=max_known_bound,
        max_known_minute_bound_share=equity.max_known_share,
        definite_breach=any(impact.bound_overflow for impact in impacts),
        incomplete_causes=incomplete,
    )


def evaluate_activation_threshold(
    report: ActivationGateReport,
    max_bound_share: float,
) -> ActivationThresholdDecision:
    """Return pass, breach, or incomplete for an operator-stated equity share.

    Args:
        report: Completed read-only activation report.
        max_bound_share: Operator-stated maximum bound as an equity fraction.

    Returns:
        Scheduler-friendly threshold verdict with the observed maximum share.

    Raises:
        ValueError: If the threshold is not a finite positive fraction at most one.
    """
    _validate_max_bound_share(max_bound_share)
    observed = report.max_known_minute_bound_share
    if report.definite_breach or (observed is not None and observed > max_bound_share):
        status: GateStatus = "breach"
    elif report.incomplete_causes or observed is None:
        status = "incomplete"
    else:
        status = "pass"
    return ActivationThresholdDecision(
        status=status,
        max_bound_share=max_bound_share,
        observed_max_bound_share=observed,
    )


def _validate_max_bound_share(max_bound_share: float) -> None:
    """Refuse a threshold that is not a finite positive equity fraction."""
    if (
        isinstance(max_bound_share, bool)
        or not math.isfinite(max_bound_share)
        or not 0.0 < max_bound_share <= 1.0
    ):
        raise ValueError("max_bound_share must be finite, positive, and no greater than one")


def _price_json(price: UsdCloseEvidence | None) -> JsonValue:
    """Project nullable USD-close provenance into JSON."""
    if price is None:
        return None
    return {
        "currency": price.currency,
        "close": price.close,
        "open_at": price.open_at.isoformat(),
        "exchange": price.exchange,
        "native_symbol": price.native_symbol,
        "instrument_public_id": price.instrument_public_id,
        "candle_id": price.candle_id,
        "candle_public_id": price.candle_public_id,
        "candle_timestamp": price.candle_timestamp.isoformat(),
        "age_seconds": price.age_seconds,
    }


def _leg_json(leg: BoundedExclusion) -> JsonObject:
    """Project one excluded leg and both proof planes into JSON."""
    return {
        "exchange": leg.exchange,
        "currency": leg.currency,
        "quantity": leg.quantity,
        "min_order_size": leg.min_order_size,
        "pairs_considered": leg.pairs_considered,
        "minimum_instrument_public_id": leg.minimum_instrument_public_id,
        "minimum_symbol_public_id": leg.minimum_symbol_public_id,
        "minimum_native_symbol": leg.minimum_native_symbol,
        "minimum_spec_public_id": leg.minimum_spec_public_id,
        "minimum_spec_source": leg.minimum_spec_source,
        "minimum_spec_version": leg.minimum_spec_version,
        "minimum_spec_observed_at": (
            leg.minimum_spec_observed_at.isoformat()
            if leg.minimum_spec_observed_at is not None
            else None
        ),
        "price": _price_json(leg.price),
        "bound_usd": leg.bound_usd,
        "bound_overflow": leg.bound_overflow,
    }


def _impact_json(impact: MinuteImpact) -> JsonObject:
    """Project one minute's disconnected rule result into JSON."""
    return {
        "minute": impact.minute.isoformat(),
        "legs": [_leg_json(leg) for leg in impact.legs],
        "known_bound_usd": impact.known_bound_usd,
        "complete_bound_usd": impact.complete_bound_usd,
        "bound_overflow": impact.bound_overflow,
        "causal_equity_floor_usd": impact.causal_equity_floor_usd,
        "causal_equity_floor_minute": (
            impact.causal_equity_floor_minute.isoformat()
            if impact.causal_equity_floor_minute is not None
            else None
        ),
        "known_bound_share": impact.known_bound_share,
        "rule_withheld": impact.rule_withheld,
        "withheld_cause": impact.withheld_cause,
    }


def _gate_json(decision: ActivationThresholdDecision | None) -> JsonValue:
    """Project an optional scheduler decision into JSON."""
    if decision is None:
        return None
    return {
        "status": decision.status,
        "max_bound_share": decision.max_bound_share,
        "observed_max_bound_share": decision.observed_max_bound_share,
    }


def _scope_credential_json(evidence: ScopeCredentialEvidence) -> JsonObject:
    """Project one credential classification without payloads or labels."""
    return {
        "public_id": evidence.public_id,
        "exchange": evidence.exchange,
        "credential_type": evidence.credential_type,
        "executor_disposition": evidence.executor_disposition,
        "spot_eligible": evidence.spot_eligible,
        "included_in_denominator": evidence.included_in_denominator,
    }


def _scope_json(evidence: LiveWalletScopeEvidence | None) -> JsonValue:
    """Project non-secret provenance for the live executor-backed denominator."""
    if evidence is None:
        return None
    return {
        "credential_catalog_as_of": evidence.credential_catalog_as_of.isoformat(),
        "active_credential_catalog_size": evidence.active_credential_catalog_size,
        "resolved_mint_wallet_public_id": evidence.resolved_mint_wallet_public_id,
        "credentials": [_scope_credential_json(item) for item in evidence.credentials],
    }


def _activation_eligible(
    report: ActivationGateReport,
    decision: ActivationThresholdDecision | None,
) -> bool:
    """Return whether this exact artifact can satisfy the one-time report gate."""
    return (
        report.request.purpose == "activation"
        and report.request.window_minutes == _ACTIVATION_WINDOW_MINUTES
        and report.request.scope_evidence is not None
        and decision is not None
        and decision.status == "pass"
    )


def report_as_json(
    report: ActivationGateReport,
    decision: ActivationThresholdDecision | None = None,
) -> JsonObject:
    """Serialize a report deterministically without losing audit provenance.

    Args:
        report: Evidence report to project.
        decision: Optional standing-threshold verdict.

    Returns:
        Finite JSON object containing scope, summary, impacts, and optional verdict.
    """
    minutes = report.request.minutes
    return {
        "schema_version": 2,
        "purpose": report.request.purpose,
        "activation_eligible": _activation_eligible(report, decision),
        "wallet_public_id": report.request.wallet_public_id,
        "mode": report.request.mode,
        "exchanges": list(report.request.exchanges),
        "scope_evidence": _scope_json(report.request.scope_evidence),
        "as_of": report.request.as_of.isoformat(),
        "window_start": minutes[0].isoformat(),
        "window_end": minutes[-1].isoformat(),
        "minutes_requested": report.request.window_minutes,
        "authoritative_minutes": report.authoritative_minutes,
        "unauthoritative_minutes": report.unauthoritative_minutes,
        "unauthoritative_reason_counts": dict(report.unauthoritative_reason_counts),
        "resolved_legs_total": report.resolved_legs_total,
        "unresolved_legs_total": report.unresolved_legs_total,
        "exclusion_minutes": report.exclusion_minutes,
        "excluded_legs": report.excluded_legs,
        "rule_withheld_minutes": report.rule_withheld_minutes,
        "withheld_cause_counts": dict(report.withheld_cause_counts),
        "equity_sample_count": report.equity_sample_count,
        "equity_floor_usd": report.equity_floor_usd,
        "equity_floor_minute": (
            report.equity_floor_minute.isoformat()
            if report.equity_floor_minute is not None
            else None
        ),
        "max_known_minute_bound_usd": report.max_known_minute_bound_usd,
        "max_known_minute_bound_share": report.max_known_minute_bound_share,
        "definite_breach": report.definite_breach,
        "incomplete_causes": list(report.incomplete_causes),
        "impacts": [_impact_json(impact) for impact in report.impacts],
        "gate": _gate_json(decision),
    }


def reconstruct_observation_attempts(
    request: ActivationGateRequest,
    rows: Sequence[VenueAccountObservationAttemptRow],
) -> tuple[Mapping[str, VenueAccountObservationAttemptRow], ...]:
    """Reconstruct every latest-at-minute observation map from one bounded stream.

    Args:
        request: Exact wallet, venue, mode, and minute-grid contract.
        rows: Opening seeds and bounded subsequent attempts from the repository.

    Returns:
        One latest-at-minute exchange map for every requested grid minute.
    """
    allowed_exchanges = frozenset(request.exchanges)
    end = request.minutes[-1]
    for row in rows:
        if row["wallet_public_id"] != request.wallet_public_id:
            raise ValueError("observation stream contains a foreign wallet")
        if row["mode"] != request.mode:
            raise ValueError("observation stream contains a foreign mode")
        if row["exchange"] not in allowed_exchanges:
            raise ValueError("observation stream contains an unexpected exchange")
        if row["timestamp"] > end:
            raise ValueError("observation stream extends beyond the requested window")
    ordered = sorted(rows, key=lambda row: (row["timestamp"], row["id"]))
    cursor: dict[str, VenueAccountObservationAttemptRow] = {}
    snapshots: list[Mapping[str, VenueAccountObservationAttemptRow]] = []
    position = 0
    for minute in request.minutes:
        while position < len(ordered) and ordered[position]["timestamp"] <= minute:
            row = ordered[position]
            cursor[row["exchange"]] = row
            position += 1
        snapshots.append(dict(cursor))
    return tuple(snapshots)


async def _load_baskets(
    repository: Repository,
    request: ActivationGateRequest,
) -> tuple[GateBasketMinute, ...]:
    """Load one bounded stream and authority-gate every reconstructed minute."""
    baskets: list[GateBasketMinute] = []
    minutes = request.minutes
    rows = await repository.get_venue_account_observation_attempt_stream(
        request.wallet_public_id,
        request.exchanges,
        request.mode,
        minutes[0],
        minutes[-1],
    )
    attempts = reconstruct_observation_attempts(request, rows)
    for minute, minute_attempts in zip(minutes, attempts, strict=True):
        outcome = evaluate_basket(minute, frozenset(request.exchanges), minute_attempts)
        baskets.append(
            GateBasketMinute(
                minute=minute,
                balances=dict(outcome.observed_balances) if not outcome.reason_codes else None,
                reason_codes=tuple(sorted(outcome.reason_codes)),
            )
        )
    return tuple(baskets)


def _observed_currencies(baskets: Sequence[GateBasketMinute]) -> tuple[str, ...]:
    """Return every balance currency from authoritative report minutes."""
    return tuple(
        sorted(
            {
                currency
                for basket in baskets
                if basket.balances is not None
                for _, currency in basket.balances
            }
        )
    )


async def _load_minimum_versions(
    repository: Repository,
    request: ActivationGateRequest,
    baskets: Sequence[GateBasketMinute],
) -> tuple[VenueOrderMinimumVersion, ...]:
    """Load the shipped S1 temporal evidence once for the whole report window."""
    currencies = _observed_currencies(baskets)
    if not currencies:
        return ()
    minutes = request.minutes
    rows = await repository.get_pnl_spot_order_minimum_window(
        request.exchanges,
        currencies,
        minutes[0],
        minutes[-1] + _MINUTE,
        request.as_of,
    )
    return tuple(VenueOrderMinimumVersion(**row) for row in rows)


def _excluded_currencies(report: ActivationGateReport) -> tuple[str, ...]:
    """Return currencies whose disconnected predicate actually proposed exclusion."""
    return tuple(sorted({leg.currency for impact in report.impacts for leg in impact.legs}))


async def _load_equity_samples(
    repository: Repository,
    request: ActivationGateRequest,
) -> list[PortfolioPnlSampleRow]:
    """Load every current-epoch status inside the exact denominator window."""
    anchor = await repository.get_portfolio_pnl_anchor(
        request.wallet_public_id,
        request.mode,
        _USD,
        None,
    )
    if anchor is None:
        return []
    query = PortfolioPnlSampleQuery(
        wallet_public_id=request.wallet_public_id,
        mode=request.mode,
        valuation_ccy=_USD,
        epoch_public_id=anchor["epoch_public_id"],
        calc_version=PNL_SAMPLE_CALC_VERSION,
    )
    return await repository.get_portfolio_pnl_samples(
        query,
        request.minutes[0],
        request.minutes[-1],
    )


async def run_activation_report(
    repository: Repository,
    request: ActivationGateRequest,
) -> ActivationGateReport:
    """Execute every read needed by the disconnected activation report.

    Args:
        repository: Existing Snapper repository used only through read methods.
        request: Bounded report scope and horizon.

    Returns:
        Pure report built from authority-gated balances and independent bounds.
    """
    baskets = await _load_baskets(repository, request)
    minimum_versions = await _load_minimum_versions(repository, request, baskets)
    preliminary = build_activation_report(request, baskets, minimum_versions, (), ())
    currencies = _excluded_currencies(preliminary)
    if not currencies:
        return preliminary
    price_rows, equity_samples = await asyncio.gather(
        repository.get_pnl_crypto_usd_plane_candles(
            currencies,
            request.minutes[0] - _PRICE_LOOKBACK,
            request.minutes[-1] - _MINUTE,
            request.as_of,
        ),
        _load_equity_samples(repository, request),
    )
    return build_activation_report(
        request,
        baskets,
        minimum_versions,
        price_rows,
        equity_samples,
    )


def _scope_credential_evidence(
    credential: WalletCredentialRow,
    topology_disposition: ExecutorCredentialDisposition,
) -> ScopeCredentialEvidence:
    """Classify one wallet credential without retaining its encrypted payload."""
    spot_eligible = credential["credential_type"] != "paper" and not is_futures_class_exchange(
        credential["exchange"]
    )
    included = (
        spot_eligible and topology_disposition != ExecutorCredentialDisposition.MINT_WALLET_EXCLUDED
    )
    return ScopeCredentialEvidence(
        public_id=credential["public_id"],
        exchange=credential["exchange"],
        credential_type=credential["credential_type"],
        executor_disposition=topology_disposition.value,
        spot_eligible=spot_eligible,
        included_in_denominator=included,
    )


async def resolve_live_wallet_scope(
    repository: Repository,
    wallet_public_id: str,
    as_of: datetime,
    raw_pin: JsonValue,
) -> tuple[tuple[str, ...], LiveWalletScopeEvidence]:
    """Resolve the exact live snapshotter venue denominator and safe provenance.

    Args:
        repository: Repository exposing the shared active credential catalogue.
        wallet_public_id: Exact live wallet whose denominator is required.
        as_of: Shared credential and topology evidence horizon.
        raw_pin: Fresh mint-wallet pin value consumed by shared topology policy.

    Returns:
        Sorted expected venues and non-secret credential classification evidence.
    """
    credentials = await repository.list_active_wallet_credentials(as_of)
    topology = await resolve_executor_topology(repository, credentials, raw_pin, lambda: as_of)
    venues_by_wallet = resolve_spot_venues(credentials, topology)
    if wallet_public_id not in venues_by_wallet:
        raise ValueError("wallet has no active credential catalogue entry")
    exchanges = tuple(sorted(venues_by_wallet[wallet_public_id]))
    if not exchanges:
        raise ValueError("wallet has no executor-backed live spot venue")
    wallet_credentials = [
        credential
        for credential in credentials
        if credential["wallet_public_id"] == wallet_public_id
    ]
    evidence = tuple(
        _scope_credential_evidence(credential, topology.disposition_for(credential))
        for credential in sorted(
            wallet_credentials,
            key=lambda item: (item["exchange"], item["public_id"]),
        )
    )
    return exchanges, LiveWalletScopeEvidence(
        credential_catalog_as_of=as_of,
        active_credential_catalog_size=len(credentials),
        resolved_mint_wallet_public_id=topology.mint_wallet_public_id or None,
        credentials=evidence,
    )


def _command_request(
    wallet: str,
    hours: int,
    purpose: GatePurpose,
) -> GateCommandRequest:
    """Capture one current horizon before resolving authoritative venue scope."""
    return GateCommandRequest(
        wallet_public_id=wallet,
        as_of=datetime.now(UTC),
        window_minutes=hours * 60,
        purpose=purpose,
    )


async def _run_configured_report(command: GateCommandRequest) -> ActivationGateReport:
    """Bind repository, reproduce snapshotter scope, and guarantee disposal."""
    try:
        bootstrap = get_bootstrap_settings()
        repository = get_repository(bootstrap.db_url)
        settings_service = SettingsService(bootstrap.db_url, bootstrap.zmq_broker_xsub)
        raw_pin = await settings_service.get_setting_fresh(MINT_WALLET_PIN_SETTING_KEY)
        exchanges, scope_evidence = await resolve_live_wallet_scope(
            repository,
            command.wallet_public_id,
            command.as_of,
            raw_pin,
        )
        request = ActivationGateRequest(
            wallet_public_id=command.wallet_public_id,
            mode="live",
            exchanges=exchanges,
            as_of=command.as_of,
            window_minutes=command.window_minutes,
            purpose=command.purpose,
            scope_evidence=scope_evidence,
        )
        return await run_activation_report(repository, request)
    finally:
        await dispose_repositories()


def _fatal(message: str) -> NoReturn:
    """Emit one stable refusal and stop with the operator refusal code."""
    typer.echo(f"refused: {message}", err=True)
    raise typer.Exit(code=EXIT_REFUSED)


def _execute(request: GateCommandRequest) -> ActivationGateReport:
    """Run the configured report and translate every read failure to refusal."""
    try:
        return asyncio.run(_run_configured_report(request))
    except Exception:
        _fatal("activation evidence read failed")


def _emit(report: ActivationGateReport, decision: ActivationThresholdDecision | None) -> None:
    """Write one canonical finite JSON document to standard output."""
    typer.echo(
        json.dumps(
            report_as_json(report, decision),
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    )


@pnl_minimum_gate_app.command(name="report")
def report_command(
    wallet: Annotated[str, typer.Option("--wallet", help="Wallet public id")],
    max_bound_share: Annotated[
        float,
        typer.Option("--max-bound-share", help="Maximum accepted bound as an equity fraction"),
    ],
) -> None:
    """Emit the exact threshold-bound 24-hour activation artifact.

    Args:
        wallet: Wallet public id of the P&L scope.
        max_bound_share: Maximum accepted omission bound as an equity fraction.

    Returns:
        None.
    """
    try:
        request = _command_request(wallet, 24, "activation")
        _validate_max_bound_share(max_bound_share)
    except ValueError as error:
        _fatal(str(error))
    report = _execute(request)
    decision = evaluate_activation_threshold(report, max_bound_share)
    _emit(report, decision)
    if decision.status == "breach":
        raise typer.Exit(code=EXIT_BREACH)
    if decision.status == "incomplete":
        raise typer.Exit(code=EXIT_INCOMPLETE)


@pnl_minimum_gate_app.command(name="check")
def check_command(
    wallet: Annotated[str, typer.Option("--wallet", help="Wallet public id")],
    max_bound_share: Annotated[
        float,
        typer.Option("--max-bound-share", help="Maximum accepted bound as an equity fraction"),
    ],
    hours: Annotated[int, typer.Option("--hours", help="Rolling lookback in hours")] = 24,
) -> None:
    """Evaluate the rolling report for an external scheduler or alarm.

    Args:
        wallet: Wallet public id of the P&L scope.
        max_bound_share: Maximum accepted omission bound as an equity fraction.
        hours: Current rolling window of at least 24 hours.

    Returns:
        None.
    """
    try:
        request = _command_request(wallet, hours, "standing")
        _validate_max_bound_share(max_bound_share)
    except ValueError as error:
        _fatal(str(error))
    report = _execute(request)
    decision = evaluate_activation_threshold(report, max_bound_share)
    _emit(report, decision)
    if decision.status == "breach":
        raise typer.Exit(code=EXIT_BREACH)
    if decision.status == "incomplete":
        raise typer.Exit(code=EXIT_INCOMPLETE)


@pnl_minimum_gate_app.command(name="inspect")
def inspect_command(
    wallet: Annotated[str, typer.Option("--wallet", help="Wallet public id")],
    hours: Annotated[int, typer.Option("--hours", help="Bounded diagnostic lookback")] = 1,
) -> None:
    """Emit a diagnostic artifact that is explicitly ineligible for activation.

    Args:
        wallet: Wallet public id of the P&L scope.
        hours: Number of rolling hours to inspect.

    Returns:
        None.
    """
    try:
        request = _command_request(wallet, hours, "inspect")
    except ValueError as error:
        _fatal(str(error))
    report = _execute(request)
    _emit(report, None)
    if report.incomplete_causes:
        raise typer.Exit(code=EXIT_INCOMPLETE)
