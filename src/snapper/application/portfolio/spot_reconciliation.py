"""Pure execution-replay reconciliation for live spot and FX accounts.

The evaluator performs no I/O and mutates no caller-owned input. It replays a
fixed scope-sequence range (the per-account commit-ordered execution counter)
from an immutable bootstrap inventory, compares the result with venue TOTAL
balances using exact ``Decimal`` arithmetic, and fails closed when scope,
boundary, inventory, numeric, or precision evidence is not certified.
"""

import json
import re
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from decimal import InvalidOperation

import snapper.application.portfolio.spot_precision_certification as spot_precision_certification
from snapper.data.repository_types import InstrumentSpecRow
from snapper.data.repository_types import PortfolioReconciliationEvaluationRow
from snapper.data.repository_types import SpotAssetPrecisionEvidenceRow
from snapper.data.repository_types import SpotReconciliationAnchorRow
from snapper.infrastructure.exchanges.contracts import CapabilityStatus
from snapper.messaging.schemas.data import AccountBalanceEntry
from snapper.messaging.schemas.data import PortfolioAccountState

_METHOD = "spot_execution_replay"
_MODE = "live"
_WATERMARK_KIND = "scope_sequence"
_PLAIN_DECIMAL = re.compile(r"[0-9]+(?:\.[0-9]+)?", flags=re.ASCII)


@dataclass(frozen=True)
class SpotReplayExecutionRow:
    """One account-scoped durable fill used by the bounded replay.

    ``scope_sequence`` is the fill's per-``(wallet, exchange, mode)``
    commit-ordered counter — the replay range key and the unit of the
    boundary watermark. It is unrelated to bus-provenance sequence ids.
    """

    scope_sequence: int
    wallet_public_id: str
    exchange: str
    mode: str
    status: str
    instrument_public_id: str
    symbol: str
    base_asset: str
    quote_asset: str
    side: str
    price: float
    size: float
    fee: float
    fee_asset: str
    price_decimal: str | None = None
    size_decimal: str | None = None
    fee_decimal: str | None = None
    counter_amount_decimal: str | None = None
    numeric_provenance: str | None = "legacy_float"


@dataclass(frozen=True)
class SpotReplayBoundary:
    """Caller-certified fixed replay and venue-read boundary evidence."""

    source_watermark: int
    range_complete: bool
    watermark_captured_before_balance: bool
    request_started_at: datetime
    request_completed_at: datetime
    venue_cursor: str | None = None
    venue_cursor_certified: bool = False
    inventory_complete: bool = False
    inventory_truncated: bool = False


@dataclass(frozen=True)
class SpotInstrumentIdentity:
    """Canonical identity expected for one replay instrument."""

    instrument_public_id: str
    symbol: str
    base_asset: str
    quote_asset: str


class _IncompleteError(Exception):
    """Expected fail-closed evaluator outcome carrying a stable reason."""


@dataclass(frozen=True)
class _ResolvedNumber:
    """One finite decimal operand and whether it crossed a float boundary."""

    value: Decimal
    legacy: bool


@dataclass
class _ToleranceAccumulator:
    """Mutable evaluator-owned tolerance evidence for one asset."""

    floor: Decimal
    accumulation: Decimal
    legacy_terms: int
    sources: set[str]


@dataclass(frozen=True)
class _ReplayPrecision:
    """Certified price and quantity quanta for one replay execution."""

    tick: Decimal
    quantity: Decimal
    cost: Decimal


@dataclass(frozen=True)
class _ReplayOperands:
    """Resolved exact operands and side sign for one replay execution."""

    price: _ResolvedNumber
    size: _ResolvedNumber
    fee: _ResolvedNumber
    sign: Decimal


@dataclass
class _ReplayAccumulator:
    """Mutable replay outputs owned by one evaluator invocation."""

    delta: dict[str, Decimal]
    fee_assets: set[str]
    tolerances: dict[str, _ToleranceAccumulator]


@dataclass(frozen=True)
class _PrecisionContext:
    """Inputs needed to accumulate one asset's certified tolerance."""

    exchange: str
    asset_precisions: Mapping[str, SpotAssetPrecisionEvidenceRow]
    now: datetime
    fee_assets: set[str]
    venue_legacy: dict[str, bool]
    replay: Sequence[SpotReplayExecutionRow]
    boundary: SpotReplayBoundary


@dataclass(frozen=True)
class _CashLedger:
    """Normalized per-asset inputs for deterministic cash comparison."""

    anchor: dict[str, Decimal]
    replay: dict[str, Decimal]
    liabilities: dict[str, Decimal]
    venue: dict[str, Decimal]
    tolerances: dict[str, _ToleranceAccumulator]


@dataclass
class _CashEvidence:
    """Mutable canonical evidence maps for one cash comparison."""

    expected: dict[str, object]
    actual: dict[str, object]
    difference: dict[str, object]
    tolerance: dict[str, object]
    mismatched: bool


def _decimal_string(value: Decimal) -> str:
    """Render a finite decimal canonically without exponent notation."""
    rendered = format(value, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return "0" if rendered in ("", "-0") else rendered


def _json(payload: object) -> str:
    """Serialize deterministic evidence while rejecting non-standard numbers."""
    return json.dumps(payload, allow_nan=False, separators=(",", ":"), sort_keys=True)


def _quantum(decimals: int | None) -> Decimal | None:
    """Return the positive decimal quantum for a certified decimal count."""
    if decimals is None or isinstance(decimals, bool) or decimals < 0 or decimals > 256:
        return None
    return Decimal(1).scaleb(-decimals)


def _finite_decimal_string(raw: str) -> Decimal:
    """Parse one bounded, finite exact decimal string."""
    if _PLAIN_DECIMAL.fullmatch(raw) is None:
        raise _IncompleteError("malformed_decimal")
    value = Decimal(raw)
    exponent = value.as_tuple().exponent
    if not isinstance(exponent, int) or abs(exponent) > 256:
        raise _IncompleteError("non_finite_or_unbounded_decimal")
    return value


def _resolve_number(
    legacy: float,
    raw: str | None,
    provenance: str | None,
    name: str,
) -> _ResolvedNumber:
    """Prefer raw venue evidence and validate its legacy float companion."""
    if isinstance(legacy, bool):
        raise _IncompleteError(f"invalid_{name}")
    try:
        legacy_decimal = Decimal(str(legacy))
    except InvalidOperation as exc:
        raise _IncompleteError(f"invalid_{name}") from exc
    if not legacy_decimal.is_finite():
        raise _IncompleteError(f"invalid_{name}")
    if raw is None:
        if provenance not in (None, "legacy_float", "venue_raw"):
            raise _IncompleteError("raw_numeric_provenance_conflict")
        return _ResolvedNumber(legacy_decimal, True)
    if provenance != "venue_raw":
        raise _IncompleteError("raw_numeric_provenance_conflict")
    exact = _finite_decimal_string(raw)
    if float(exact) != legacy:
        raise _IncompleteError("raw_numeric_companion_conflict")
    return _ResolvedNumber(exact, False)


def _canonical_asset(asset: str, reason: str) -> str:
    """Require a caller-resolved, non-empty canonical asset identity."""
    if not asset or asset.strip() != asset:
        raise _IncompleteError(reason)
    return asset


def _anchor_balances(anchor: SpotReconciliationAnchorRow) -> dict[str, Decimal]:
    """Strictly parse canonical sorted anchor asset totals."""

    def unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise _IncompleteError("duplicate_anchor_asset")
            result[key] = value
        return result

    try:
        parsed = json.loads(anchor["balances_json"], object_pairs_hook=unique_object)
    except json.JSONDecodeError as exc:
        raise _IncompleteError("malformed_anchor_balances") from exc
    if not isinstance(parsed, dict) or not parsed:
        raise _IncompleteError("empty_or_malformed_anchor")
    balances: dict[str, Decimal] = {}
    rendered: dict[str, str] = {}
    for asset, raw in parsed.items():
        canonical = _canonical_asset(asset, "invalid_anchor_asset")
        if not isinstance(raw, str):
            raise _IncompleteError("non_string_anchor_balance")
        amount = _finite_decimal_string(raw)
        balances[canonical] = amount
        rendered[canonical] = _decimal_string(amount)
    if anchor["balances_json"] != _json(rendered):
        raise _IncompleteError("noncanonical_anchor_balances")
    return balances


def _venue_balances(
    balances: Sequence[AccountBalanceEntry],
) -> tuple[dict[str, Decimal], dict[str, bool]]:
    """Resolve unique venue TOTAL values and raw-versus-legacy evidence."""
    totals: dict[str, Decimal] = {}
    legacy: dict[str, bool] = {}
    for row in balances:
        asset = _canonical_asset(row.currency, "invalid_venue_asset")
        if asset in totals:
            raise _IncompleteError("duplicate_venue_asset")
        resolved = _resolve_number(
            row.total,
            row.total_decimal,
            row.numeric_provenance,
            "venue_total",
        )
        totals[asset] = resolved.value
        legacy[asset] = resolved.legacy
    return totals, legacy


def is_effective_spot_precision_certified(
    exchange: str,
    spec: InstrumentSpecRow,
    evaluated_at: datetime,
) -> bool:
    """Delegate venue-bound instrument precision certification to its authority.

    Args:
        exchange: Exact account venue being reconciled.
        spec: Persisted spot instrument precision and provenance evidence.
        evaluated_at: Caller-captured certification instant.

    Returns:
        Whether every required spot precision component is fresh and valid.
    """
    return spot_precision_certification.is_spot_instrument_precision_certified(
        exchange,
        spec,
        evaluated_at,
    )


def _asset_precision(
    exchange: str,
    asset: str,
    asset_precisions: Mapping[str, SpotAssetPrecisionEvidenceRow],
    now: datetime,
    require_fee: bool,
) -> SpotAssetPrecisionEvidenceRow:
    """Return fresh certified precision for one canonical asset."""
    evidence = asset_precisions.get(asset)
    if evidence is None:
        raise _IncompleteError("missing_asset_precision")
    if not spot_precision_certification.is_spot_precision_plane_certified(
        exchange,
        asset,
        "balance",
        evidence,
        now,
    ):
        raise _IncompleteError("stale_or_uncertified_asset_precision")
    if require_fee and not spot_precision_certification.is_spot_precision_plane_certified(
        exchange,
        asset,
        "fee",
        evidence,
        now,
    ):
        raise _IncompleteError("stale_or_uncertified_asset_precision")
    return evidence


def _account_is_full(venue_account: PortfolioAccountState, now: datetime) -> bool:
    """Return whether fresh venue balance evidence is authoritative and coherent."""
    observed_at = venue_account.balance_observed_at
    authoritative_until = venue_account.authoritative_until
    observation_id = venue_account.current_attempt_observation_id
    return (
        now.utcoffset() is not None
        and venue_account.effective_status == "observed"
        and venue_account.is_authoritative is True
        and venue_account.balance_status == "observed"
        and venue_account.balances is not None
        and bool(venue_account.public_id)
        and bool(venue_account.wallet_public_id)
        and bool(str(venue_account.exchange))
        and bool(venue_account.session_id)
        and observation_id is not None
        and venue_account.balance_payload_source_observation_id == observation_id
        and observed_at is not None
        and observed_at.utcoffset() is not None
        and observed_at <= now
        and authoritative_until is not None
        and authoritative_until.utcoffset() is not None
        and now <= authoritative_until
    )


def _evaluation_row(
    venue_account: PortfolioAccountState,
    now: datetime,
    status: str,
    reason: str | None = None,
    *,
    anchor: SpotReconciliationAnchorRow | None = None,
    source_watermark: int | None = None,
    expected_json: str | None = None,
    actual_json: str | None = None,
    difference_json: str | None = None,
    tolerance_json: str | None = None,
) -> PortfolioReconciliationEvaluationRow:
    """Construct one S1 writer row, retaining full lineage only for full outcomes."""
    full = status in ("matched", "mismatched")
    return {
        "wallet_public_id": venue_account.wallet_public_id,
        "exchange": str(venue_account.exchange).lower(),
        "mode": _MODE,
        "method": _METHOD,
        "evaluation_status": status,
        "venue_account_state_public_id": venue_account.public_id if full else None,
        "venue_account_observation_id": (
            venue_account.current_attempt_observation_id if full else None
        ),
        "account_authoritative_until": venue_account.authoritative_until if full else None,
        "source_watermark_kind": _WATERMARK_KIND if full else None,
        "source_watermark": source_watermark if full else None,
        "anchor_public_id": anchor["public_id"] if full and anchor is not None else None,
        "expected_json": expected_json if full else None,
        "actual_json": actual_json if full else None,
        "difference_json": difference_json if full else None,
        "tolerance_json": tolerance_json if full else None,
        "error": reason[:512] if reason else None,
        "session_id": venue_account.session_id,
        "sequence_id": venue_account.sequence_id,
        "bus_time": now,
    }


def _validate_anchor(
    anchor: SpotReconciliationAnchorRow | None,
    venue_account: PortfolioAccountState,
    boundary: SpotReplayBoundary,
) -> SpotReconciliationAnchorRow:
    """Require one persisted, in-scope, usable bootstrap anchor."""
    if anchor is None or not anchor["public_id"]:
        raise _IncompleteError("missing_anchor")
    if (
        anchor["wallet_public_id"] != venue_account.wallet_public_id
        or anchor["exchange"] != str(venue_account.exchange).lower()
        or anchor["mode"] != _MODE
    ):
        raise _IncompleteError("foreign_anchor")
    if (
        anchor["source_watermark_kind"] != _WATERMARK_KIND
        or anchor["source_watermark"] < 0
        or anchor["source_watermark"] > boundary.source_watermark
    ):
        raise _IncompleteError("invalid_anchor_watermark")
    if (
        not anchor["venue_account_state_public_id"]
        or anchor["balance_observation_id"] <= 0
        or not anchor["session_id"]
        or not anchor["provenance"]
        or anchor["boundary_status"] not in ("cursor_certified", "double_read_equal", "uncertified")
        or anchor["inventory_status"]
        not in ("venue_reported_full", "uncertified", "suspect_partial")
        or anchor["margin_status"] not in ("cash", "unsupported_margin", "unknown")
    ):
        raise _IncompleteError("malformed_anchor")
    if anchor["inventory_status"] == "suspect_partial":
        raise _IncompleteError("suspect_partial")
    if anchor["margin_status"] != "cash":
        raise _IncompleteError("unsupported_margin")
    if (
        not isinstance(anchor["first_request_started_at"], datetime)
        or not isinstance(anchor["first_request_completed_at"], datetime)
        or not isinstance(anchor["second_request_started_at"], datetime)
        or not isinstance(anchor["second_request_completed_at"], datetime)
        or not isinstance(anchor["timestamp"], datetime)
        or anchor["first_request_started_at"].utcoffset() is None
        or anchor["first_request_completed_at"].utcoffset() is None
        or anchor["second_request_started_at"].utcoffset() is None
        or anchor["second_request_completed_at"].utcoffset() is None
        or not anchor["first_request_started_at"]
        <= anchor["first_request_completed_at"]
        <= anchor["second_request_started_at"]
        <= anchor["second_request_completed_at"]
        <= anchor["timestamp"]
    ):
        raise _IncompleteError("invalid_anchor_boundary")
    return anchor


def _validate_boundary(boundary: SpotReplayBoundary, now: datetime) -> None:
    """Require watermark-before-read ordering and a complete fixed range."""
    if (
        isinstance(boundary.source_watermark, bool)
        or boundary.source_watermark < 0
        or not boundary.range_complete
        or not boundary.watermark_captured_before_balance
    ):
        raise _IncompleteError("incomplete_replay_boundary")
    if (
        not isinstance(boundary.request_started_at, datetime)
        or not isinstance(boundary.request_completed_at, datetime)
        or boundary.request_started_at.utcoffset() is None
        or boundary.request_completed_at.utcoffset() is None
        or boundary.request_completed_at < boundary.request_started_at
        or boundary.request_completed_at > now
    ):
        raise _IncompleteError("invalid_balance_request_boundary")
    if boundary.venue_cursor_certified and not boundary.venue_cursor:
        raise _IncompleteError("invalid_venue_cursor_certificate")


def _add_floor(
    tolerances: dict[str, _ToleranceAccumulator],
    asset: str,
    quantum: Decimal,
    source: str,
) -> None:
    """Accumulate one non-zero certified floor source."""
    item = tolerances.setdefault(asset, _ToleranceAccumulator(Decimal(0), Decimal(0), 0, set()))
    item.floor = max(item.floor, quantum)
    item.sources.add(f"{source}:{_decimal_string(quantum)}")


def _add_error(tolerances: dict[str, _ToleranceAccumulator], asset: str, amount: Decimal) -> None:
    """Accumulate one legacy conversion bound."""
    item = tolerances.setdefault(asset, _ToleranceAccumulator(Decimal(0), Decimal(0), 0, set()))
    item.accumulation += amount
    item.legacy_terms += 1


def _validate_replay_row(
    row: SpotReplayExecutionRow,
    anchor: SpotReconciliationAnchorRow,
    boundary: SpotReplayBoundary,
    venue_account: PortfolioAccountState,
    seen: set[int],
) -> bool:
    """Validate replay ordering and account scope, returning whether to consume the row."""
    if isinstance(row.scope_sequence, bool):
        raise _IncompleteError("duplicate_or_invalid_execution_sequence")
    if row.scope_sequence > boundary.source_watermark:
        return False
    if row.scope_sequence in seen:
        raise _IncompleteError("duplicate_or_invalid_execution_sequence")
    seen.add(row.scope_sequence)
    if row.scope_sequence <= anchor["source_watermark"]:
        raise _IncompleteError("execution_before_anchor_watermark")
    scope_matches = (
        row.wallet_public_id == venue_account.wallet_public_id
        and row.exchange == str(venue_account.exchange).lower()
        and row.mode == _MODE
    )
    if not scope_matches:
        raise _IncompleteError("execution_scope_mismatch")
    if row.status not in ("filled", "partial"):
        raise _IncompleteError("invalid_execution_status")
    return True


def _resolve_replay_identity(
    row: SpotReplayExecutionRow,
    instruments: Mapping[str, SpotInstrumentIdentity],
) -> tuple[str, str]:
    """Validate and return the canonical base and quote assets for one execution."""
    identity = instruments.get(row.instrument_public_id)
    identity_matches = (
        identity is not None
        and identity.instrument_public_id == row.instrument_public_id
        and identity.symbol == row.symbol
        and identity.base_asset == row.base_asset
        and identity.quote_asset == row.quote_asset
    )
    if not identity_matches:
        raise _IncompleteError("unresolved_or_conflicting_instrument")
    base = _canonical_asset(row.base_asset, "invalid_base_asset")
    quote = _canonical_asset(row.quote_asset, "invalid_quote_asset")
    if base == quote:
        raise _IncompleteError("asset_alias_collision")
    return base, quote


def _resolve_replay_precision(
    row: SpotReplayExecutionRow,
    venue_account: PortfolioAccountState,
    specs: Mapping[str, InstrumentSpecRow | None],
    now: datetime,
) -> _ReplayPrecision:
    """Return certified price, quantity, and cost quanta for one execution."""
    spec = specs.get(row.instrument_public_id)
    if spec is None or spec["instrument_public_id"] != row.instrument_public_id:
        raise _IncompleteError("missing_spot_precision")
    if not is_effective_spot_precision_certified(
        str(venue_account.exchange).lower(),
        spec,
        now,
    ):
        raise _IncompleteError("stale_or_uncertified_spot_precision")
    quantity = _quantum(spec["qty_decimals"])
    cost = _quantum(spec["cost_decimals"])
    if quantity is None or cost is None:
        raise _IncompleteError("invalid_spot_precision")
    return _ReplayPrecision(Decimal(str(spec["tick_size"])), quantity, cost)


def _resolve_replay_operands(row: SpotReplayExecutionRow) -> _ReplayOperands:
    """Resolve exact execution operands and validate side and economics."""
    price = _resolve_number(
        row.price,
        row.price_decimal,
        row.numeric_provenance,
        "execution_price",
    )
    size = _resolve_number(
        row.size,
        row.size_decimal,
        row.numeric_provenance,
        "execution_size",
    )
    fee = _resolve_number(
        row.fee,
        row.fee_decimal,
        row.numeric_provenance,
        "execution_fee",
    )
    if price.value <= 0 or size.value <= 0 or fee.value < 0:
        raise _IncompleteError("invalid_execution_economics")
    if row.side == "buy":
        sign = Decimal(1)
    elif row.side == "sell":
        sign = Decimal(-1)
    else:
        raise _IncompleteError("invalid_execution_side")
    return _ReplayOperands(price, size, fee, sign)


def _apply_replay_notional(
    row: SpotReplayExecutionRow,
    assets: tuple[str, str],
    precision: _ReplayPrecision,
    operands: _ReplayOperands,
    accumulator: _ReplayAccumulator,
) -> None:
    """Apply exact base and quote deltas plus legacy numeric error bounds."""
    base, quote = assets
    accumulator.delta[base] = (
        accumulator.delta.get(base, Decimal(0)) + operands.sign * operands.size.value
    )
    _add_floor(accumulator.tolerances, base, precision.quantity, "quantity")
    _add_floor(accumulator.tolerances, quote, precision.cost, "cost")
    size_error = Decimal(0)
    if operands.size.legacy:
        size_error = precision.quantity / 2
        _add_error(accumulator.tolerances, base, size_error)
    if row.counter_amount_decimal is not None:
        if row.numeric_provenance != "venue_raw":
            raise _IncompleteError("raw_numeric_provenance_conflict")
        counter = _finite_decimal_string(row.counter_amount_decimal)
        if counter <= 0:
            raise _IncompleteError("invalid_execution_economics")
        accumulator.delta[quote] = (
            accumulator.delta.get(quote, Decimal(0)) - operands.sign * counter
        )
        return
    accumulator.delta[quote] = (
        accumulator.delta.get(quote, Decimal(0))
        - operands.sign * operands.size.value * operands.price.value
    )
    _add_floor(
        accumulator.tolerances,
        quote,
        abs(operands.size.value) * precision.tick,
        "price_tick_contribution",
    )
    price_error = precision.tick / 2 if operands.price.legacy else Decimal(0)
    if operands.size.legacy or operands.price.legacy:
        notional_error = (
            abs(operands.price.value) * size_error
            + abs(operands.size.value) * price_error
            + size_error * price_error
            + precision.cost / 2
        )
        _add_error(accumulator.tolerances, quote, notional_error)


def _apply_replay_fee(
    row: SpotReplayExecutionRow,
    operands: _ReplayOperands,
    accumulator: _ReplayAccumulator,
) -> None:
    """Apply fee deltas and retain every asset requiring fee precision."""
    if operands.fee.value != 0:
        fee_asset = _canonical_asset(row.fee_asset, "missing_fee_asset")
        accumulator.delta[fee_asset] = (
            accumulator.delta.get(fee_asset, Decimal(0)) - operands.fee.value
        )
        accumulator.fee_assets.add(fee_asset)
    elif row.fee_asset:
        accumulator.fee_assets.add(_canonical_asset(row.fee_asset, "invalid_fee_asset"))
    if operands.fee.legacy and row.fee_asset:
        accumulator.fee_assets.add(row.fee_asset)


def _replay_executions(
    replay: Sequence[SpotReplayExecutionRow],
    anchor: SpotReconciliationAnchorRow,
    boundary: SpotReplayBoundary,
    venue_account: PortfolioAccountState,
    instruments: Mapping[str, SpotInstrumentIdentity],
    specs: Mapping[str, InstrumentSpecRow | None],
    now: datetime,
    tolerances: dict[str, _ToleranceAccumulator],
) -> tuple[dict[str, Decimal], set[str]]:
    """Replay the fixed scope-sequence range and derive precision bounds."""
    accumulator = _ReplayAccumulator({}, set(), tolerances)
    seen: set[int] = set()
    ordered = sorted(replay, key=lambda item: item.scope_sequence)
    for row in ordered:
        if not _validate_replay_row(row, anchor, boundary, venue_account, seen):
            continue
        assets = _resolve_replay_identity(row, instruments)
        precision = _resolve_replay_precision(
            row,
            venue_account,
            specs,
            now,
        )
        operands = _resolve_replay_operands(row)
        _apply_replay_notional(row, assets, precision, operands, accumulator)
        _apply_replay_fee(row, operands, accumulator)
    return accumulator.delta, accumulator.fee_assets


def _normalize_liabilities(
    liability_totals: Mapping[str, Decimal],
) -> dict[str, Decimal]:
    """Validate and canonicalize caller-supplied liability totals."""
    liabilities: dict[str, Decimal] = {}
    for asset, value in liability_totals.items():
        canonical = _canonical_asset(asset, "invalid_liability_asset")
        if not isinstance(value, Decimal) or not value.is_finite():
            raise _IncompleteError("invalid_liability")
        liabilities[canonical] = value
    return liabilities


def _cash_assets(
    anchor_totals: Mapping[str, Decimal],
    replay_delta: Mapping[str, Decimal],
    fee_assets: set[str],
    liabilities: Mapping[str, Decimal],
    venue_totals: Mapping[str, Decimal],
) -> list[str]:
    """Return the deterministic union of every relevant cash asset."""
    return sorted(
        set(anchor_totals) | set(replay_delta) | fee_assets | set(liabilities) | set(venue_totals)
    )


def _accumulate_asset_precision(
    asset: str,
    context: _PrecisionContext,
    tolerances: dict[str, _ToleranceAccumulator],
) -> None:
    """Accumulate certified balance and fee precision for one asset."""
    precision = _asset_precision(
        context.exchange,
        asset,
        context.asset_precisions,
        context.now,
        asset in context.fee_assets,
    )
    balance_quantum = _quantum(precision["balance_decimals"])
    if balance_quantum is None:
        raise _IncompleteError("invalid_balance_precision")
    _add_floor(tolerances, asset, balance_quantum, "balance")
    if context.venue_legacy.get(asset, False):
        _add_error(tolerances, asset, balance_quantum / 2)
    if asset not in context.fee_assets:
        return
    fee_quantum = _quantum(precision["fee_decimals"])
    if fee_quantum is None:
        raise _IncompleteError("invalid_fee_precision")
    _add_floor(tolerances, asset, fee_quantum, "fee")
    for row in context.replay:
        if row.scope_sequence > context.boundary.source_watermark or row.fee_asset != asset:
            continue
        resolved_fee = _resolve_number(
            row.fee,
            row.fee_decimal,
            row.numeric_provenance,
            "execution_fee",
        )
        if resolved_fee.legacy:
            _add_error(tolerances, asset, fee_quantum / 2)


def _build_cash_evidence(
    assets: Sequence[str],
    ledger: _CashLedger,
) -> _CashEvidence:
    """Build canonical per-asset comparison and tolerance evidence."""
    evidence = _CashEvidence({}, {}, {}, {}, False)
    for asset in assets:
        expected = (
            ledger.anchor.get(asset, Decimal(0))
            + ledger.replay.get(asset, Decimal(0))
            - ledger.liabilities.get(asset, Decimal(0))
        )
        actual = ledger.venue.get(asset, Decimal(0))
        signed_delta = expected - actual
        absolute_delta = abs(signed_delta)
        tolerance = ledger.tolerances.get(asset)
        if tolerance is None or tolerance.floor <= 0:
            raise _IncompleteError("zero_or_missing_tolerance")
        absolute_tolerance = tolerance.floor + tolerance.accumulation
        status = "mismatched" if absolute_delta >= absolute_tolerance else "matched"
        evidence.mismatched = evidence.mismatched or status == "mismatched"
        evidence.expected[asset] = {
            "anchor_total": _decimal_string(ledger.anchor.get(asset, Decimal(0))),
            "liability_delta": _decimal_string(ledger.liabilities.get(asset, Decimal(0))),
            "replay_delta": _decimal_string(ledger.replay.get(asset, Decimal(0))),
            "total": _decimal_string(expected),
        }
        evidence.actual[asset] = {
            "absent_as_zero": asset not in ledger.venue,
            "total": _decimal_string(actual),
        }
        evidence.difference[asset] = {
            "absolute_delta": _decimal_string(absolute_delta),
            "signed_delta": _decimal_string(signed_delta),
            "status": status,
        }
        evidence.tolerance[asset] = {
            "absolute_tolerance": _decimal_string(absolute_tolerance),
            "accumulation_term": _decimal_string(tolerance.accumulation),
            "comparison": "absolute_delta_gte_tolerance_is_mismatch",
            "legacy_term_count": tolerance.legacy_terms,
            "precision_floor": _decimal_string(tolerance.floor),
            "precision_sources": sorted(tolerance.sources),
        }
    return evidence


def _cash_status(
    mismatched: bool,
    anchor: SpotReconciliationAnchorRow,
    boundary: SpotReplayBoundary,
) -> str:
    """Return the full cash status or fail closed on uncertified match evidence."""
    if mismatched:
        return "mismatched"
    if anchor["inventory_status"] != "venue_reported_full" or not boundary.inventory_complete:
        raise _IncompleteError("uncertified_inventory")
    if (
        anchor["boundary_status"] == "uncertified"
        or not boundary.venue_cursor_certified
        or not boundary.venue_cursor
    ):
        raise _IncompleteError("uncertified_boundary")
    return "matched"


def _evaluate_cash(
    anchor: SpotReconciliationAnchorRow,
    replay: Sequence[SpotReplayExecutionRow],
    boundary: SpotReplayBoundary,
    venue_account: PortfolioAccountState,
    instruments: Mapping[str, SpotInstrumentIdentity],
    specs: Mapping[str, InstrumentSpecRow | None],
    asset_precisions: Mapping[str, SpotAssetPrecisionEvidenceRow],
    previously_confirmed_assets: frozenset[str],
    liability_totals: Mapping[str, Decimal],
    now: datetime,
) -> PortfolioReconciliationEvaluationRow:
    """Evaluate one fully routed cash account with deterministic evidence."""
    anchor_totals = _anchor_balances(anchor)
    venue_rows = venue_account.balances or []
    venue_totals, venue_legacy = _venue_balances(venue_rows)
    if not venue_rows or boundary.inventory_truncated:
        raise _IncompleteError("suspect_partial")
    for asset in previously_confirmed_assets:
        _canonical_asset(asset, "invalid_confirmed_asset")
    missing_confirmed = previously_confirmed_assets - frozenset(venue_totals)
    if len(missing_confirmed) >= 2:
        raise _IncompleteError("suspect_partial")
    tolerances: dict[str, _ToleranceAccumulator] = {}
    replay_delta, fee_assets = _replay_executions(
        replay,
        anchor,
        boundary,
        venue_account,
        instruments,
        specs,
        now,
        tolerances,
    )
    liabilities = _normalize_liabilities(liability_totals)
    assets = _cash_assets(
        anchor_totals,
        replay_delta,
        fee_assets,
        liabilities,
        venue_totals,
    )
    if not assets:
        raise _IncompleteError("empty_inventory")
    precision_context = _PrecisionContext(
        exchange=str(venue_account.exchange).lower(),
        asset_precisions=asset_precisions,
        now=now,
        fee_assets=fee_assets,
        venue_legacy=venue_legacy,
        replay=replay,
        boundary=boundary,
    )
    for asset in assets:
        _accumulate_asset_precision(asset, precision_context, tolerances)
    ledger = _CashLedger(
        anchor=anchor_totals,
        replay=replay_delta,
        liabilities=liabilities,
        venue=venue_totals,
        tolerances=tolerances,
    )
    evidence = _build_cash_evidence(assets, ledger)
    status = _cash_status(evidence.mismatched, anchor, boundary)
    expected_json = _json(
        {
            "anchor_public_id": anchor["public_id"],
            "anchor_watermark": anchor["source_watermark"],
            "assets": evidence.expected,
            "source_watermark": boundary.source_watermark,
        }
    )
    actual_json = _json(
        {
            "assets": evidence.actual,
            "inventory_status": (
                "certified_full" if boundary.inventory_complete else "uncertified"
            ),
            "request_completed_at": boundary.request_completed_at.isoformat(),
            "request_started_at": boundary.request_started_at.isoformat(),
            "venue_cursor": boundary.venue_cursor,
        }
    )
    difference_json = _json({"assets": evidence.difference})
    tolerance_json = _json({"assets": evidence.tolerance})
    return _evaluation_row(
        venue_account,
        now,
        status,
        anchor=anchor,
        source_watermark=boundary.source_watermark,
        expected_json=expected_json,
        actual_json=actual_json,
        difference_json=difference_json,
        tolerance_json=tolerance_json,
    )


def _error_text(error: Exception) -> str:
    """Return a bounded, non-empty unexpected evaluator error."""
    return (str(error).strip() or "spot reconciliation evaluation failed")[:512]


def evaluate(
    anchor: SpotReconciliationAnchorRow | None,
    replay: Sequence[SpotReplayExecutionRow] | None,
    replay_boundary: SpotReplayBoundary,
    venue_account: PortfolioAccountState,
    instruments_by_public_id: Mapping[str, SpotInstrumentIdentity],
    specs_by_instrument_public_id: Mapping[str, InstrumentSpecRow | None],
    asset_precisions: Mapping[str, SpotAssetPrecisionEvidenceRow],
    previously_confirmed_assets: frozenset[str],
    liability_totals: Mapping[str, Decimal],
    margin_indicators: Sequence[str],
    position_capability: CapabilityStatus,
    now: datetime,
) -> PortfolioReconciliationEvaluationRow:
    """Replay live cash executions and compare them with venue TOTAL balances.

    Args:
        anchor: Persisted immutable bootstrap inventory, or honest absence.
        replay: Complete fixed scope-sequence range, or honest absence.
        replay_boundary: Watermark-before-balance and completeness evidence.
        venue_account: Fresh fail-closed venue account view.
        instruments_by_public_id: Canonical spot pair identities.
        specs_by_instrument_public_id: Spot precision evidence by instrument.
        asset_precisions: Certified balance and fee precision by asset.
        previously_confirmed_assets: Inventory expected to remain enumerable.
        liability_totals: Explicit liability totals retained for tripwires.
        margin_indicators: Runtime margin, borrow, debt, or leverage signals.
        position_capability: Structural venue position capability.
        now: Caller-captured evaluation and bus instant.

    Returns:
        Typed S1 evaluation evidence with full lineage only for full outcomes.

    Raises:
        ValueError: If invoked for paper, simulated, or unknown capability truth.
    """
    if str(venue_account.mode) != _MODE:
        raise ValueError("spot reconciliation requires a live account")
    if position_capability is CapabilityStatus.SIMULATED:
        raise ValueError("simulated account truth cannot be reconciled as live")
    if position_capability is CapabilityStatus.SUPPORTED:
        return _evaluation_row(venue_account, now, "unsupported", "futures_position_required")
    if position_capability is CapabilityStatus.UNSUPPORTED:
        return _evaluation_row(venue_account, now, "incomplete", "ambiguous_position_capability")
    if position_capability is not CapabilityStatus.NOT_APPLICABLE:
        raise ValueError("unknown spot position capability")
    if margin_indicators or any(value != 0 for value in liability_totals.values()):
        return _evaluation_row(venue_account, now, "incomplete", "unsupported_margin")
    if not _account_is_full(venue_account, now):
        return _evaluation_row(venue_account, now, "incomplete", "incomplete_venue_account")
    try:
        _validate_boundary(replay_boundary, now)
        valid_anchor = _validate_anchor(anchor, venue_account, replay_boundary)
        if replay is None:
            raise _IncompleteError("missing_replay")
        return _evaluate_cash(
            valid_anchor,
            replay,
            replay_boundary,
            venue_account,
            instruments_by_public_id,
            specs_by_instrument_public_id,
            asset_precisions,
            previously_confirmed_assets,
            liability_totals,
            now,
        )
    except _IncompleteError as error:
        return _evaluation_row(venue_account, now, "incomplete", _error_text(error))
    except Exception as error:
        return _evaluation_row(venue_account, now, "error", _error_text(error))
