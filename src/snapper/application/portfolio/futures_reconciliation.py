"""Pure signed-quantity reconciliation for live futures portfolios.

The evaluator compares one complete, account-scoped fill projection with one
authoritative venue position observation. It performs no reads, writes, clock
access, logging, publishing, or input mutation. All quantity arithmetic uses
``Decimal`` values converted at the float boundaries with ``Decimal(str(...))``.
Unusable quantities, ambiguous identities, incomplete provenance, and
uncertified effective units fail closed as incomplete evaluations.
"""

import json
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import NotRequired
from typing import TypedDict

from snapper.data.repository import is_effective_unit_certified
from snapper.data.repository_types import InstrumentSpecRow
from snapper.data.repository_types import PortfolioReconciliationEvaluationRow
from snapper.data.repository_types import PositionRow
from snapper.infrastructure.exchanges.contracts import CapabilityStatus
from snapper.messaging.schemas.data import AccountPositionEntry
from snapper.messaging.schemas.data import PortfolioAccountState

_METHOD = "futures_position"
_MODE = "live"
_WATERMARK_KIND = "venue_event_id"


class _ExpectedInstrument(TypedDict):
    """Canonical fill-projection evidence for one instrument."""

    instrument_public_id: str | None
    signed_qty: str | None
    source_venue_event_id: int | None
    symbol: str


class _ActualInstrument(TypedDict):
    """Canonical venue-position evidence for one instrument."""

    instrument_public_id: str | None
    side: str | None
    signed_qty: str | None
    symbol: str


class _DifferenceInstrument(TypedDict):
    """Canonical comparison outcome for one instrument."""

    instrument_public_id: str | None
    status: str
    symbol: str
    absolute_delta: NotRequired[str]
    reason: NotRequired[str]
    signed_delta: NotRequired[str]


class _ToleranceInstrument(TypedDict):
    """Canonical effective-unit evidence for one instrument."""

    instrument_public_id: str | None
    symbol: str
    absolute_tolerance: NotRequired[str]
    contract_size: NotRequired[str]
    lot_step: NotRequired[str]
    quantity_unit: NotRequired[str | None]
    reason: NotRequired[str]
    spec_observed_at: NotRequired[str]
    spec_source: NotRequired[str]
    spec_version: NotRequired[str]


class _ExpectedPayload(TypedDict):
    """Top-level internal projection evidence."""

    instruments: list[_ExpectedInstrument]


class _ActualPayload(TypedDict):
    """Top-level venue observation evidence."""

    instruments: list[_ActualInstrument]


class _DifferencePayload(TypedDict):
    """Top-level signed-difference evidence."""

    instruments: list[_DifferenceInstrument]


class _TolerancePayload(TypedDict):
    """Top-level certified tolerance evidence."""

    instruments: list[_ToleranceInstrument]
    tolerance_lots: int


@dataclass(frozen=True)
class _Candidate:
    """One resolved or unresolved instrument comparison candidate."""

    instrument_public_id: str | None
    internal: PositionRow | None
    venue: AccountPositionEntry | None
    internal_symbol: str
    venue_symbol: str
    reason: str | None


@dataclass(frozen=True)
class _CandidateEvaluation:
    """Canonical evidence and outcome for one comparison candidate."""

    expected: _ExpectedInstrument
    actual: _ActualInstrument
    difference: _DifferenceInstrument
    tolerance: _ToleranceInstrument
    status: str


def _decimal(value: float | Decimal) -> Decimal | None:
    """Convert one numeric boundary value to a finite exact decimal.

    Args:
        value: Repository or venue numeric value.

    Returns:
        A finite decimal, or ``None`` for a non-finite or invalid value.
    """
    converted = Decimal(str(value))
    return converted if converted.is_finite() else None


def _decimal_string(value: Decimal) -> str:
    """Return a canonical non-exponent base-ten decimal string.

    Args:
        value: A finite decimal.

    Returns:
        The canonical string, with insignificant fractional zeroes removed.
    """
    rendered = format(value, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return "0" if rendered in ("", "-0") else rendered


def _json(
    payload: _ExpectedPayload | _ActualPayload | _DifferencePayload | _TolerancePayload,
) -> str:
    """Serialize evidence deterministically and reject non-standard numbers.

    Args:
        payload: Typed reconciliation evidence.

    Returns:
        Canonical compact JSON.
    """
    return json.dumps(payload, allow_nan=False, separators=(",", ":"), sort_keys=True)


def _evaluation_row(
    venue_account: PortfolioAccountState,
    now: datetime,
    status: str,
    *,
    venue_account_state_public_id: str | None = None,
    venue_account_observation_id: int | None = None,
    account_authoritative_until: datetime | None = None,
    source_watermark: int | None = None,
    expected_json: str | None = None,
    actual_json: str | None = None,
    difference_json: str | None = None,
    tolerance_json: str | None = None,
    error: str | None = None,
) -> PortfolioReconciliationEvaluationRow:
    """Construct the S1 writer input without performing persistence.

    Args:
        venue_account: Venue account identity and observation envelope.
        now: Single caller-captured evaluation instant.
        status: Raw account evaluation status.
        venue_account_state_public_id: Account-state public identity.
        venue_account_observation_id: Fresh account observation identity.
        account_authoritative_until: Authority deadline for full evidence.
        source_watermark: Maximum consumed venue-event identity.
        expected_json: Canonical internal evidence.
        actual_json: Canonical venue evidence.
        difference_json: Canonical signed-difference evidence.
        tolerance_json: Canonical certification evidence.
        error: Bounded evaluation failure text.

    Returns:
        The typed S1 repository writer input.
    """
    exchange = str(venue_account.exchange).lower()
    return {
        "wallet_public_id": venue_account.wallet_public_id,
        "exchange": exchange,
        "mode": _MODE,
        "method": _METHOD,
        "evaluation_status": status,
        "venue_account_state_public_id": venue_account_state_public_id,
        "venue_account_observation_id": venue_account_observation_id,
        "account_authoritative_until": account_authoritative_until,
        "source_watermark_kind": _WATERMARK_KIND if source_watermark is not None else None,
        "source_watermark": source_watermark,
        "anchor_public_id": None,
        "expected_json": expected_json,
        "actual_json": actual_json,
        "difference_json": difference_json,
        "tolerance_json": tolerance_json,
        "error": error,
        "session_id": venue_account.session_id,
        "sequence_id": venue_account.sequence_id,
        "bus_time": now,
    }


def _account_is_full(venue_account: PortfolioAccountState, now: datetime) -> bool:
    """Return whether venue position evidence is fresh and coherent.

    Args:
        venue_account: Fail-closed account read view.
        now: Evaluation instant.

    Returns:
        Whether the account can support a full comparison.
    """
    authoritative_until = venue_account.authoritative_until
    position_observed_at = venue_account.position_observed_at
    observation_id = venue_account.current_attempt_observation_id
    return (
        now.utcoffset() is not None
        and venue_account.effective_status == "observed"
        and venue_account.is_authoritative
        and venue_account.position_status == "observed"
        and venue_account.open_positions is not None
        and bool(venue_account.public_id)
        and bool(venue_account.wallet_public_id)
        and bool(str(venue_account.exchange))
        and bool(venue_account.session_id)
        and observation_id is not None
        and venue_account.position_payload_source_observation_id == observation_id
        and authoritative_until is not None
        and authoritative_until.utcoffset() is not None
        and now <= authoritative_until
        and position_observed_at is not None
        and position_observed_at.utcoffset() is not None
        and position_observed_at <= now
    )


def _internal_reason(
    row: PositionRow,
    venue_account: PortfolioAccountState,
    instrument_public_ids_by_symbol: Mapping[str, str],
) -> str | None:
    """Validate one projection row's scope, identity, mapping, and quantity.

    Args:
        row: Fill-derived projection row.
        venue_account: Owning account identity.
        instrument_public_ids_by_symbol: Native symbol resolution supplied by the caller.

    Returns:
        A stable incomplete reason, or ``None`` when usable.
    """
    if (
        row["wallet_public_id"] != venue_account.wallet_public_id
        or row["exchange"] != str(venue_account.exchange).lower()
        or row["mode"] != _MODE
    ):
        return "projection_scope_mismatch"
    if not row["instrument_public_id"] or not row["instrument"]:
        return "invalid_projection_identity"
    mapped = instrument_public_ids_by_symbol.get(row["instrument"])
    if mapped is not None and mapped != row["instrument_public_id"]:
        return "internal_symbol_mapping_conflict"
    if _decimal(row["quantity"]) is None:
        return "invalid_internal_quantity"
    if row["quantity"] != 0 and row["source_venue_event_id"] is None:
        return "missing_source_watermark"
    return None


def _projection_watermark(projection: Sequence[PositionRow]) -> tuple[int | None, bool]:
    """Derive the aggregate venue-event watermark and provenance validity.

    Args:
        projection: Complete account-scoped projection rows.

    Returns:
        The maximum watermark, using zero when none was consumed, and whether
        every non-flat usable row carries known provenance.
    """
    watermarks: list[int] = []
    valid = True
    for row in projection:
        quantity = _decimal(row["quantity"])
        watermark = row["source_venue_event_id"]
        if watermark is not None:
            watermarks.append(watermark)
        elif quantity is None or quantity != 0:
            valid = False
    return (max(watermarks, default=0), valid)


def _group_internal_candidates(
    projection: Sequence[PositionRow],
    venue_account: PortfolioAccountState,
    instrument_public_ids_by_symbol: Mapping[str, str],
) -> tuple[dict[str, list[PositionRow]], dict[str, str], list[_Candidate]]:
    """Group internal rows and retain unresolved or invalid evidence."""
    groups: dict[str, list[PositionRow]] = {}
    reasons: dict[str, str] = {}
    unresolved: list[_Candidate] = []
    for row in projection:
        instrument_public_id = row["instrument_public_id"]
        reason = _internal_reason(row, venue_account, instrument_public_ids_by_symbol)
        if not instrument_public_id:
            unresolved.append(
                _Candidate(None, row, None, row["instrument"], row["instrument"], reason)
            )
            continue
        groups.setdefault(instrument_public_id, []).append(row)
        if reason is not None:
            reasons.setdefault(instrument_public_id, reason)
    return groups, reasons, unresolved


def _group_venue_candidates(
    venue_positions: Sequence[AccountPositionEntry],
    instrument_public_ids_by_symbol: Mapping[str, str],
) -> tuple[dict[str, list[AccountPositionEntry]], list[_Candidate]]:
    """Group venue rows and retain symbols without a stable instrument identity."""
    groups: dict[str, list[AccountPositionEntry]] = {}
    unresolved: list[_Candidate] = []
    for position in venue_positions:
        instrument_public_id = instrument_public_ids_by_symbol.get(position.symbol)
        if not instrument_public_id:
            unresolved.append(
                _Candidate(
                    None,
                    None,
                    position,
                    position.symbol,
                    position.symbol,
                    "unresolved_venue_symbol",
                )
            )
            continue
        groups.setdefault(instrument_public_id, []).append(position)
    return groups, unresolved


def _resolved_candidate(
    instrument_public_id: str,
    internal_rows: list[PositionRow],
    venue_rows: list[AccountPositionEntry],
    internal_reason: str | None,
) -> _Candidate:
    """Build one deterministic candidate for a resolved instrument identity."""
    internal = internal_rows[0] if len(internal_rows) == 1 else None
    venue = venue_rows[0] if len(venue_rows) == 1 else None
    internal_symbol = min((row["instrument"] for row in internal_rows), default="")
    venue_symbol = min((row.symbol for row in venue_rows), default="")
    reason = internal_reason
    if len(internal_rows) > 1:
        reason = "duplicate_internal_position"
    elif len(venue_rows) > 1:
        reason = "duplicate_venue_position"
    return _Candidate(
        instrument_public_id,
        internal,
        venue,
        internal_symbol or venue_symbol,
        venue_symbol or internal_symbol,
        reason,
    )


def _build_candidates(
    projection: Sequence[PositionRow],
    venue_positions: Sequence[AccountPositionEntry],
    venue_account: PortfolioAccountState,
    instrument_public_ids_by_symbol: Mapping[str, str],
) -> list[_Candidate]:
    """Build deterministic candidates while preserving every ambiguity.

    Args:
        projection: Complete internal position projection.
        venue_positions: Authoritative venue open positions.
        venue_account: Owning account identity.
        instrument_public_ids_by_symbol: Native symbol resolution.

    Returns:
        Candidates sorted by instrument identity and native symbol.
    """
    internal_groups, internal_reasons, unresolved_internal = _group_internal_candidates(
        projection,
        venue_account,
        instrument_public_ids_by_symbol,
    )
    venue_groups, unresolved_venue = _group_venue_candidates(
        venue_positions,
        instrument_public_ids_by_symbol,
    )
    candidates: list[_Candidate] = []
    instrument_ids = sorted(set(internal_groups) | set(venue_groups))
    for instrument_public_id in instrument_ids:
        internal_rows = internal_groups.get(instrument_public_id, [])
        venue_rows = venue_groups.get(instrument_public_id, [])
        candidates.append(
            _resolved_candidate(
                instrument_public_id,
                internal_rows,
                venue_rows,
                internal_reasons.get(instrument_public_id),
            )
        )
    candidates.extend(unresolved_internal)
    candidates.extend(unresolved_venue)
    return sorted(
        candidates,
        key=lambda item: (
            item.instrument_public_id is None,
            item.instrument_public_id or "",
            item.internal_symbol,
            item.venue_symbol,
        ),
    )


def _specification_evidence(
    instrument_public_id: str,
    symbol: str,
    spec: InstrumentSpecRow,
) -> tuple[_ToleranceInstrument, Decimal | None]:
    """Project safe tolerance evidence and resolve the stored contract size."""
    evidence = _ToleranceInstrument(
        instrument_public_id=instrument_public_id,
        symbol=symbol,
        quantity_unit=spec["quantity_unit"],
    )
    if spec["spec_source"]:
        evidence["spec_source"] = spec["spec_source"]
    if spec["spec_version"]:
        evidence["spec_version"] = spec["spec_version"]
    if spec["spec_observed_at"] is not None:
        evidence["spec_observed_at"] = spec["spec_observed_at"].isoformat()
    contract_size = _decimal(spec["contract_size"]) if spec["contract_size"] is not None else None
    if contract_size is not None:
        evidence["contract_size"] = _decimal_string(contract_size)
    return evidence, contract_size


def _certified_lot_step(
    spec: InstrumentSpecRow,
    now: datetime,
    contract_size: Decimal | None,
) -> tuple[Decimal | None, str | None]:
    """Return the certified lot step or its first fail-closed reason."""
    if not is_effective_unit_certified(spec, now):
        return None, "stale_or_uncertified_unit"
    if spec["quantity_unit"] != "contract_count":
        return None, "mismatched_unit"
    if contract_size is None or contract_size <= 0:
        return None, "invalid_contract_size"
    lot_step = _decimal(spec["lot_size"]) if spec["lot_size"] is not None else None
    if lot_step is None or lot_step <= 0:
        return None, "invalid_lot_step"
    if not spec["spec_source"] or not spec["spec_version"] or spec["spec_observed_at"] is None:
        return None, "missing_spec_provenance"
    if spec["status"] != "active":
        return None, "inactive_spec"
    if spec["instrument_kind"] not in ("perpetual", "future"):
        return None, "unsupported_instrument_kind"
    return lot_step, None


def _specification(
    instrument_public_id: str | None,
    symbol: str,
    spec: InstrumentSpecRow | None,
    now: datetime,
) -> tuple[Decimal | None, _ToleranceInstrument, str | None]:
    """Validate effective-unit evidence and resolve the certified lot step.

    Args:
        instrument_public_id: Resolved instrument identity.
        symbol: Native symbol used in evidence.
        spec: Caller-supplied instrument specification.
        now: Evaluation instant used by the S2a freshness predicate.

    Returns:
        Certified lot step, safe tolerance evidence, and incomplete reason.
    """
    evidence = _ToleranceInstrument(instrument_public_id=instrument_public_id, symbol=symbol)
    if instrument_public_id is None:
        evidence["reason"] = "unresolved_venue_symbol"
        return None, evidence, "unresolved_venue_symbol"
    if spec is None:
        evidence["reason"] = "missing_spec"
        return None, evidence, "missing_spec"
    if spec["instrument_public_id"] != instrument_public_id:
        evidence["reason"] = "mismatched_spec_instrument"
        return None, evidence, "mismatched_spec_instrument"
    evidence, contract_size = _specification_evidence(instrument_public_id, symbol, spec)
    lot_step, reason = _certified_lot_step(spec, now, contract_size)
    if lot_step is not None:
        rendered_lot = _decimal_string(lot_step)
        evidence["lot_step"] = rendered_lot
        evidence["absolute_tolerance"] = rendered_lot
        return lot_step, evidence, None
    resolved_reason = reason or "invalid_lot_step"
    evidence["reason"] = resolved_reason
    return None, evidence, resolved_reason


def _candidate_quantities(
    candidate: _Candidate,
) -> tuple[Decimal | None, Decimal | None, str | None]:
    """Resolve signed internal and venue quantities for one candidate.

    Args:
        candidate: Resolved or unresolved comparison candidate.

    Returns:
        Internal signed quantity, venue signed quantity, and incomplete reason.
    """
    internal_quantity: Decimal | None = Decimal(0)
    if candidate.internal is not None:
        internal_quantity = _decimal(candidate.internal["quantity"])
        if internal_quantity is None:
            return None, None, "invalid_internal_quantity"

    venue_quantity = Decimal(0)
    if candidate.venue is not None:
        size = _decimal(candidate.venue.size)
        if size is None or size <= 0:
            return internal_quantity, None, "invalid_venue_quantity"
        if candidate.venue.side == "buy":
            venue_quantity = size
        elif candidate.venue.side == "sell":
            venue_quantity = -size
        else:
            return internal_quantity, None, "invalid_venue_side"
    return internal_quantity, venue_quantity, None


def _evaluate_candidate(
    candidate: _Candidate,
    specs_by_instrument_public_id: Mapping[str, InstrumentSpecRow | None],
    now: datetime,
) -> _CandidateEvaluation:
    """Build canonical evidence and status for one comparison candidate."""
    symbol = candidate.venue_symbol or candidate.internal_symbol
    internal_quantity, venue_quantity, quantity_reason = _candidate_quantities(candidate)
    internal_watermark = (
        candidate.internal["source_venue_event_id"] if candidate.internal is not None else None
    )
    expected = _ExpectedInstrument(
        instrument_public_id=candidate.instrument_public_id,
        signed_qty=_decimal_string(internal_quantity) if internal_quantity is not None else None,
        source_venue_event_id=internal_watermark,
        symbol=candidate.internal_symbol or symbol,
    )
    actual = _ActualInstrument(
        instrument_public_id=candidate.instrument_public_id,
        side=candidate.venue.side if candidate.venue is not None else None,
        signed_qty=_decimal_string(venue_quantity) if venue_quantity is not None else None,
        symbol=candidate.venue_symbol or symbol,
    )
    spec = (
        specs_by_instrument_public_id.get(candidate.instrument_public_id)
        if candidate.instrument_public_id is not None
        else None
    )
    lot_step, tolerance, spec_reason = _specification(
        candidate.instrument_public_id,
        symbol,
        spec,
        now,
    )
    reason = candidate.reason or quantity_reason or spec_reason
    difference = _DifferenceInstrument(
        instrument_public_id=candidate.instrument_public_id,
        status="incomplete" if reason is not None else "matched",
        symbol=symbol,
    )
    if (
        reason is None
        and internal_quantity is not None
        and venue_quantity is not None
        and lot_step is not None
    ):
        signed_delta = internal_quantity - venue_quantity
        absolute_delta = abs(signed_delta)
        status = "mismatched" if absolute_delta >= lot_step else "matched"
        difference["status"] = status
        difference["signed_delta"] = _decimal_string(signed_delta)
        difference["absolute_delta"] = _decimal_string(absolute_delta)
    else:
        status = "incomplete"
        difference["reason"] = reason or "evaluation_incomplete"
        if tolerance.get("reason") is None:
            tolerance["reason"] = difference["reason"]
    return _CandidateEvaluation(expected, actual, difference, tolerance, status)


def _aggregate_status(statuses: Sequence[str], watermark_valid: bool) -> str:
    """Return the fail-closed account status for candidate outcomes."""
    if not watermark_valid or "incomplete" in statuses:
        return "incomplete"
    if "mismatched" in statuses:
        return "mismatched"
    return "matched"


def _evaluate_supported(
    projection: Sequence[PositionRow],
    venue_account: PortfolioAccountState,
    instrument_public_ids_by_symbol: Mapping[str, str],
    specs_by_instrument_public_id: Mapping[str, InstrumentSpecRow | None],
    now: datetime,
) -> PortfolioReconciliationEvaluationRow:
    """Evaluate one supported, authoritative live futures account.

    Args:
        projection: Complete fill-derived account projection.
        venue_account: Authoritative venue account view.
        instrument_public_ids_by_symbol: Native-symbol resolution.
        specs_by_instrument_public_id: Effective-unit evidence by instrument.
        now: Single caller-captured evaluation instant.

    Returns:
        Full or incomplete S1 evaluation input with canonical evidence.
    """
    watermark, watermark_valid = _projection_watermark(projection)
    candidates = _build_candidates(
        projection,
        venue_account.open_positions or [],
        venue_account,
        instrument_public_ids_by_symbol,
    )
    expected: list[_ExpectedInstrument] = []
    actual: list[_ActualInstrument] = []
    differences: list[_DifferenceInstrument] = []
    tolerances: list[_ToleranceInstrument] = []
    statuses: list[str] = []

    for candidate in candidates:
        evaluation = _evaluate_candidate(
            candidate,
            specs_by_instrument_public_id,
            now,
        )
        expected.append(evaluation.expected)
        actual.append(evaluation.actual)
        differences.append(evaluation.difference)
        tolerances.append(evaluation.tolerance)
        statuses.append(evaluation.status)

    account_status = _aggregate_status(statuses, watermark_valid)

    expected_json = _json(_ExpectedPayload(instruments=expected))
    actual_json = _json(_ActualPayload(instruments=actual))
    difference_json = _json(_DifferencePayload(instruments=differences))
    tolerance_json = _json(_TolerancePayload(instruments=tolerances, tolerance_lots=1))
    full = account_status in ("matched", "mismatched")
    return _evaluation_row(
        venue_account,
        now,
        account_status,
        venue_account_state_public_id=venue_account.public_id if full else None,
        venue_account_observation_id=(
            venue_account.current_attempt_observation_id if full else None
        ),
        account_authoritative_until=venue_account.authoritative_until if full else None,
        source_watermark=watermark if watermark_valid else None,
        expected_json=expected_json,
        actual_json=actual_json,
        difference_json=difference_json,
        tolerance_json=tolerance_json,
    )


def _error_text(error: Exception) -> str:
    """Return non-empty, trimmed S1-compatible evaluation failure text.

    Args:
        error: Unexpected evaluator failure.

    Returns:
        A non-empty message no longer than 512 characters.
    """
    message = str(error).strip() or "portfolio reconciliation evaluation failed"
    return message[:512]


def evaluate(
    projection: Sequence[PositionRow] | None,
    venue_account: PortfolioAccountState,
    instrument_public_ids_by_symbol: Mapping[str, str],
    specs_by_instrument_public_id: Mapping[str, InstrumentSpecRow | None],
    position_capability: CapabilityStatus,
    now: datetime,
) -> PortfolioReconciliationEvaluationRow:
    """Compare fill-derived and venue signed quantities without performing I/O.

    ``projection=None`` means internal truth was unavailable and is distinct
    from a complete empty projection. Only a supported live account with an
    authoritative, observed, provenance-coherent position payload can produce
    a full matched or mismatched result. A simulated capability or paper
    account is outside S1's live-only persistence domain and is rejected.

    Args:
        projection: Complete account-scoped durable position projection, or
            ``None`` when it could not be read completely.
        venue_account: Fail-closed independently observed account view.
        instrument_public_ids_by_symbol: Native symbol to instrument mapping.
        specs_by_instrument_public_id: S2a specification evidence by instrument.
        position_capability: Typed structural venue capability.
        now: Single timezone-aware evaluation and bus instant.

    Returns:
        Immutable-by-convention typed evidence for the S1 repository writer.

    Raises:
        ValueError: If invoked for paper/simulated truth or an unknown typed
            capability, both of which are caller orchestration errors.
    """
    if str(venue_account.mode) != _MODE:
        raise ValueError("futures reconciliation requires a live account")
    if position_capability is CapabilityStatus.SIMULATED:
        raise ValueError("simulated positions cannot be reconciled as live truth")
    if position_capability in (
        CapabilityStatus.UNSUPPORTED,
        CapabilityStatus.NOT_APPLICABLE,
    ):
        return _evaluation_row(venue_account, now, "unsupported")
    if position_capability is not CapabilityStatus.SUPPORTED:
        raise ValueError("unknown futures position capability")
    if projection is None or not _account_is_full(venue_account, now):
        return _evaluation_row(venue_account, now, "incomplete")
    try:
        return _evaluate_supported(
            projection,
            venue_account,
            instrument_public_ids_by_symbol,
            specs_by_instrument_public_id,
            now,
        )
    except Exception as error:
        return _evaluation_row(
            venue_account,
            now,
            "error",
            error=_error_text(error),
        )
