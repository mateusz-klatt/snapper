"""Failure-isolated write-through shadow pinning for raw fiat elections."""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from uuid import uuid7

from loguru import logger

from snapper.core.numeric import is_positive_finite
from snapper.data.fx_conversion_digests import build_decision_inputs_digest
from snapper.data.fx_conversion_digests import build_requirement_manifest_digest
from snapper.data.fx_conversion_digests import canonical_fx_refusal_reason
from snapper.data.repository import FxConversionArtifactConflictError
from snapper.data.repository import FxConversionArtifactUpgradeRequiredError
from snapper.data.repository import Repository
from snapper.data.repository_types import FxConversionArtifactRow
from snapper.data.repository_types import FxConversionCandidatePlane
from snapper.data.repository_types import FxConversionCompleteness
from snapper.data.repository_types import FxConversionElectionInsertRow
from snapper.data.repository_types import FxConversionOperation
from snapper.data.repository_types import FxConversionProofInsertRow
from snapper.data.repository_types import FxConversionScopeKind
from snapper.data.repository_types import PnlFxRatePlane
from snapper.data.repository_types import PnlFxRateRow

_ELECTION_POLICY_VERSION = "pnl-fiat-v1"


@dataclass(frozen=True)
class FxShadowEvaluation:
    """One already-computed raw fiat election to persist without influencing it."""

    scope_kind: FxConversionScopeKind
    consumer_instrument_public_id: str | None
    pair: tuple[str, str]
    target_currency: str
    required_minutes: frozenset[datetime]
    candidate_planes: frozenset[PnlFxRatePlane]
    selected_plane: PnlFxRatePlane | None


@dataclass
class FxShadowPinMetrics:
    """Process-local monotonic counters for shadow proof persistence."""

    creation: int = 0
    reuse: int = 0
    conflict: int = 0
    failure: int = 0


_METRICS = FxShadowPinMetrics()


def fx_shadow_pin_metrics() -> FxShadowPinMetrics:
    """Return an immutable-by-copy snapshot of shadow pin counters."""
    return FxShadowPinMetrics(
        creation=_METRICS.creation,
        reuse=_METRICS.reuse,
        conflict=_METRICS.conflict,
        failure=_METRICS.failure,
    )


def reset_fx_shadow_pin_metrics() -> None:
    """Reset process-local counters for isolated service tests."""
    _METRICS.creation = 0
    _METRICS.reuse = 0
    _METRICS.conflict = 0
    _METRICS.failure = 0


def _operation(plane: PnlFxRatePlane, source: str, target: str) -> FxConversionOperation:
    """Return the operation applying one oriented plane to source-to-target conversion."""
    return "direct" if plane[:2] == (source, target) else "inverse"


def canonical_candidate_planes(
    candidates: Sequence[PnlFxRatePlane],
    rows: Sequence[PnlFxRateRow],
    source: str,
    target: str,
) -> tuple[FxConversionCandidatePlane, ...]:
    """Complete discovered planes from their loaded rows and sort deterministically."""
    candidate_set = set(candidates)
    completed = {
        (
            row["exchange"],
            row["instrument_public_id"],
            row["native_symbol"],
            row["base"],
            row["quote"],
            _operation((row["base"], row["quote"], row["exchange"]), source, target),
        )
        for row in rows
        if (row["base"], row["quote"], row["exchange"]) in candidate_set
    }
    return tuple(
        FxConversionCandidatePlane(
            source_exchange=exchange,
            source_instrument_public_id=instrument,
            native_symbol=symbol,
            base=base,
            quote=quote,
            orientation=operation,
        )
        for exchange, instrument, symbol, base, quote, operation in sorted(completed)
    )


def _selected_rows(
    evaluation: FxShadowEvaluation, rows: Sequence[PnlFxRateRow]
) -> dict[datetime, PnlFxRateRow]:
    """Return unambiguous usable rows from the raw-selected plane by close minute."""
    selected = evaluation.selected_plane
    if selected is None:
        return {}
    grouped: dict[datetime, list[PnlFxRateRow]] = {}
    for row in rows:
        plane = (row["base"], row["quote"], row["exchange"])
        minute = row["open_at"] + timedelta(minutes=1)
        if plane == selected and minute in evaluation.required_minutes:
            grouped.setdefault(minute, []).append(row)
    resolved: dict[datetime, PnlFxRateRow] = {}
    for minute, minute_rows in grouped.items():
        closes = {row["close"] for row in minute_rows}
        if len(closes) == 1 and is_positive_finite(minute_rows[0]["close"]):
            resolved[minute] = min(minute_rows, key=lambda row: row["candle_id"])
    return resolved


def _reason(state: FxConversionCompleteness, missing: Sequence[datetime]) -> str | None:
    """Build a canonical stable reason without attempt-specific fields."""
    if state == "complete":
        return None
    return canonical_fx_refusal_reason(
        {
            "reason": "fx_conversion_unproven",
            "unproven_minutes": [minute.astimezone(UTC).isoformat() for minute in sorted(missing)],
        }
    )


def _proof_rate(close: float, operation: FxConversionOperation) -> Decimal:
    """Reproduce the raw float fold and preserve its stable decimal spelling."""
    folded = close if operation == "direct" else 1.0 / close
    return Decimal(str(folded))


def _build_artifact(
    evaluation: FxShadowEvaluation,
    rows: Sequence[PnlFxRateRow],
    as_of: datetime,
    calculation_version: str,
) -> tuple[FxConversionElectionInsertRow, tuple[FxConversionProofInsertRow, ...]]:
    """Build one repository artifact from an already-authoritative raw election."""
    target = evaluation.target_currency
    source = evaluation.pair[1] if evaluation.pair[0] == target else evaluation.pair[0]
    selected_rows = _selected_rows(evaluation, rows)
    proof_minutes = set(selected_rows)
    if proof_minutes == set(evaluation.required_minutes):
        state: FxConversionCompleteness = "complete"
    elif proof_minutes:
        state = "partial"
    else:
        state = "refused"
    selected = evaluation.selected_plane if state != "refused" else None
    election_id = str(uuid7())
    session_id = str(uuid7())
    candidates = canonical_candidate_planes(
        sorted(evaluation.candidate_planes), rows, source, target
    )
    selected_candidate = next(
        (
            candidate
            for candidate in candidates
            if selected is not None
            and (candidate["base"], candidate["quote"], candidate["source_exchange"]) == selected
        ),
        None,
    )
    missing = sorted(set(evaluation.required_minutes) - proof_minutes)
    election = FxConversionElectionInsertRow(
        public_id=election_id,
        session_id=session_id,
        sequence_id=1,
        timestamp=as_of,
        scope_kind=evaluation.scope_kind,
        consumer_instrument_public_id=evaluation.consumer_instrument_public_id,
        source_currency=source,
        target_currency=target,
        unordered_pair="-".join(evaluation.pair),
        required_minutes=tuple(sorted(evaluation.required_minutes)),
        requested_knowledge_at=as_of,
        resolved_knowledge_at=as_of,
        election_policy_version=_ELECTION_POLICY_VERSION,
        calculation_version=calculation_version,
        selected_source_exchange=(
            None if selected_candidate is None else selected_candidate["source_exchange"]
        ),
        selected_source_instrument_public_id=(
            None
            if selected_candidate is None
            else selected_candidate["source_instrument_public_id"]
        ),
        selected_native_symbol=(
            None if selected_candidate is None else selected_candidate["native_symbol"]
        ),
        selected_base=None if selected_candidate is None else selected_candidate["base"],
        selected_quote=None if selected_candidate is None else selected_candidate["quote"],
        selected_orientation=(
            None if selected_candidate is None else selected_candidate["orientation"]
        ),
        considered_candidate_planes=candidates,
        completeness_state=state,
        refusal_reason_json=_reason(state, missing),
    )
    proofs = tuple(
        FxConversionProofInsertRow(
            public_id=str(uuid7()),
            session_id=session_id,
            sequence_id=index,
            timestamp=as_of,
            election_public_id=election_id,
            conversion_minute=minute,
            candle_open_minute=row["open_at"],
            candle_id=row["candle_id"],
            candle_public_id=row["candle_public_id"],
            candle_session_id=row["candle_session_id"],
            candle_sequence_id=row["candle_sequence_id"],
            candle_timestamp=row["candle_timestamp"],
            candle_known_to=row["candle_known_to"],
            raw_close=Decimal(str(row["close"])),
            operation=election["selected_orientation"],
            conversion_rate=_proof_rate(row["close"], election["selected_orientation"]),
            source_instrument_public_id=row["instrument_public_id"],
        )
        for index, (minute, row) in enumerate(sorted(selected_rows.items()), start=2)
        if election["selected_orientation"] is not None
    )
    return election, proofs


def _artifact_matches_raw(
    artifact: FxConversionArtifactRow,
    election: FxConversionElectionInsertRow,
    proofs: Sequence[FxConversionProofInsertRow],
) -> bool:
    """Compare the committed canonical proof values with the raw-derived request."""
    committed_election = artifact["election"]
    election_matches = (
        committed_election["selected_source_exchange"] == election["selected_source_exchange"]
        and committed_election["selected_source_instrument_public_id"]
        == election["selected_source_instrument_public_id"]
        and committed_election["selected_native_symbol"] == election["selected_native_symbol"]
        and committed_election["selected_base"] == election["selected_base"]
        and committed_election["selected_quote"] == election["selected_quote"]
        and committed_election["selected_orientation"] == election["selected_orientation"]
        and committed_election["completeness_state"] == election["completeness_state"]
        and committed_election["refusal_reason_json"] == election["refusal_reason_json"]
        and committed_election["requirement_manifest_digest"]
        == build_requirement_manifest_digest(election["required_minutes"])
        and committed_election["decision_inputs_digest"]
        == build_decision_inputs_digest(election, proofs)
    )
    committed = tuple(
        (
            proof["conversion_minute"],
            proof["raw_close"],
            proof["operation"],
            proof["conversion_rate"],
        )
        for proof in artifact["proofs"]
    )
    requested = tuple(
        (
            proof["conversion_minute"],
            proof["raw_close"],
            proof["operation"],
            proof["conversion_rate"],
        )
        for proof in proofs
    )
    return election_matches and committed == requested


async def shadow_pin_fx_evaluations(
    repo: Repository,
    evaluations: Sequence[FxShadowEvaluation],
    rows: Sequence[PnlFxRateRow],
    as_of: datetime,
    calculation_version: str,
) -> None:
    """Persist raw fiat elections while isolating every shadow-write failure."""
    for evaluation in evaluations:
        try:
            election, proofs = _build_artifact(evaluation, rows, as_of, calculation_version)
            artifact = await repo.pin_fx_conversion_artifact(election, proofs)
            if not _artifact_matches_raw(artifact, election, proofs):
                _METRICS.failure += 1
                logger.error("FX shadow pin canonical mismatch for pair {}", evaluation.pair)
            elif artifact["election"]["public_id"] == election["public_id"]:
                _METRICS.creation += 1
            else:
                _METRICS.reuse += 1
        except (FxConversionArtifactConflictError, FxConversionArtifactUpgradeRequiredError):
            _METRICS.conflict += 1
            logger.exception("FX shadow pin conflict for pair {}", evaluation.pair)
        except Exception:
            _METRICS.failure += 1
            logger.exception("FX shadow pin failure for pair {}", evaluation.pair)
