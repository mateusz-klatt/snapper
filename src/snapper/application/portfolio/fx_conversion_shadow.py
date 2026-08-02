"""Failure-isolated write-through shadow pinning for raw fiat elections."""

import asyncio
from collections.abc import Callable
from collections.abc import Iterator
from collections.abc import Mapping
from collections.abc import Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from typing import cast
from uuid import uuid7

from loguru import logger

from snapper.application.portfolio.fx_rates import FxRateKey
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
MAX_SHADOW_MANIFEST_MINUTES = 15
SHADOW_PIN_TICK_TIMEOUT_SECONDS = 10.0


@dataclass(frozen=True)
class FxShadowEvaluation:
    """One already-computed raw fiat election to persist without influencing it.

    ``discovered_candidates`` is authoritative when the discovery boundary can
    supply full plane identities, retaining candidates without loaded rows. The
    current tuple-only discovery API leaves it null, so F2 deterministically
    completes candidate identities from rows loaded for that discovered set.
    """

    scope_kind: FxConversionScopeKind
    consumer_instrument_public_id: str | None
    pair: tuple[str, str]
    target_currency: str
    required_minutes: frozenset[datetime]
    candidate_planes: frozenset[PnlFxRatePlane]
    selected_plane: PnlFxRatePlane | None
    requested_knowledge_at: datetime
    rows: tuple[PnlFxRateRow, ...]
    authoritative_rates: Mapping[FxRateKey, float]
    discovered_candidates: tuple[FxConversionCandidatePlane, ...] | None = None


@dataclass
class FxShadowPinMetrics:
    """Process-local monotonic counters for shadow proof persistence."""

    creation: int = 0
    reuse: int = 0
    conflict: int = 0
    upgrade_required: int = 0
    mismatch: int = 0
    failure: int = 0


_METRICS = FxShadowPinMetrics()


def fx_shadow_pin_metrics() -> FxShadowPinMetrics:
    """Return an immutable-by-copy snapshot of shadow pin counters."""
    return FxShadowPinMetrics(
        creation=_METRICS.creation,
        reuse=_METRICS.reuse,
        conflict=_METRICS.conflict,
        upgrade_required=_METRICS.upgrade_required,
        mismatch=_METRICS.mismatch,
        failure=_METRICS.failure,
    )


def reset_fx_shadow_pin_metrics() -> None:
    """Reset process-local counters for isolated service tests."""
    _METRICS.creation = 0
    _METRICS.reuse = 0
    _METRICS.conflict = 0
    _METRICS.upgrade_required = 0
    _METRICS.mismatch = 0
    _METRICS.failure = 0
    _LOGGED_FAILURE_CLASSES.clear()


_LOGGED_FAILURE_CLASSES: set[str] = set()


def _log_once(failure_class: str, message: str, pair: tuple[str, str]) -> None:
    """Emit one process-local diagnostic for each shadow failure class."""
    if failure_class in _LOGGED_FAILURE_CLASSES:
        return
    _LOGGED_FAILURE_CLASSES.add(failure_class)
    logger.error(message, pair)


@dataclass
class FxShadowPinContext:
    """Opt-in per-tick collector flushed only after valuation work completes."""

    calculation_version: str
    evaluations: list[FxShadowEvaluation]

    def collect(self, factory: Callable[[], Sequence[FxShadowEvaluation]]) -> None:
        """Collect guarded bounded evaluations without exposing construction errors."""
        try:
            for evaluation in factory():
                if len(evaluation.required_minutes) > MAX_SHADOW_MANIFEST_MINUTES:
                    _log_once(
                        "oversized",
                        "FX shadow pin skipped oversized manifest for {}",
                        evaluation.pair,
                    )
                    continue
                self.evaluations.append(evaluation)
        except Exception:
            _METRICS.failure += 1
            _log_once("collection", "FX shadow pin evaluation collection failed for {}", ("?", "?"))

    async def flush(self, repo: Repository) -> None:
        """Persist every collected evaluation after the tick's valuation work."""
        pending = tuple(self.evaluations)
        self.evaluations.clear()
        await shadow_pin_fx_evaluations(repo, pending, self.calculation_version)

    async def flush_bounded(self, repo: Repository) -> None:
        """Flush under the single per-tick deadline without failing valuation."""
        try:
            await asyncio.wait_for(self.flush(repo), timeout=SHADOW_PIN_TICK_TIMEOUT_SECONDS)
        except TimeoutError:
            _METRICS.failure += 1
            _log_once("timeout", "FX shadow pin tick deadline exceeded for {}", ("*", "*"))


_ACTIVE_CONTEXT: ContextVar[FxShadowPinContext | None] = ContextVar(
    "fx_shadow_pin_context", default=None
)


def current_fx_shadow_context() -> FxShadowPinContext | None:
    """Return the explicitly activated snapshotter context for this async task."""
    return _ACTIVE_CONTEXT.get()


@contextmanager
def activate_fx_shadow_context(context: FxShadowPinContext | None) -> Iterator[None]:
    """Scope collection to an explicitly enabled durable-consumer valuation."""
    token = _ACTIVE_CONTEXT.set(context)
    try:
        yield
    finally:
        _ACTIVE_CONTEXT.reset(token)


def _operation(plane: PnlFxRatePlane, source: str, target: str) -> FxConversionOperation:
    """Return the operation applying one oriented plane to source-to-target conversion."""
    return "direct" if plane[:2] == (source, target) else "inverse"


def canonical_candidate_planes(
    candidates: Sequence[PnlFxRatePlane],
    rows: Sequence[PnlFxRateRow],
    source: str,
    target: str,
) -> tuple[FxConversionCandidatePlane, ...]:
    """Build the documented row-derived fallback for tuple-only discovery."""
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
            resolved[minute] = max(minute_rows, key=lambda row: row["candle_id"])
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


def replay_proof_rate(raw_close: Decimal, operation: FxConversionOperation) -> Decimal:
    """Replay a proof from raw close; inverse multiplication is never persisted."""
    return raw_close if operation == "direct" else Decimal(1) / raw_close


def _build_artifact(
    evaluation: FxShadowEvaluation,
    as_of: datetime,
    calculation_version: str,
) -> tuple[FxConversionElectionInsertRow, tuple[FxConversionProofInsertRow, ...]]:
    """Build one repository artifact from an already-authoritative raw election."""
    target = evaluation.target_currency
    source = evaluation.pair[1] if evaluation.pair[0] == target else evaluation.pair[0]
    rows = evaluation.rows
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
    candidates = evaluation.discovered_candidates or canonical_candidate_planes(
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
        resolved_knowledge_at=(
            max(row["candle_timestamp"] for row in selected_rows.values())
            if selected_rows
            else as_of
        ),
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
            conversion_rate=(
                Decimal(str(row["close"])) if election["selected_orientation"] == "direct" else None
            ),
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
    evaluation: FxShadowEvaluation,
) -> bool:
    """Compare committed proofs with the raw rates and winners valuation consumed."""
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
    selected_base = election["selected_base"]
    selected_quote = election["selected_quote"]
    selected_exchange = election["selected_source_exchange"]
    if artifact["proofs"] and (
        selected_base is None or selected_quote is None or selected_exchange is None
    ):
        return False
    rate_key_plane = (
        cast(str, selected_base),
        cast(str, selected_quote),
        cast(str, selected_exchange),
    )
    authoritative = tuple(
        (
            proof["conversion_minute"],
            Decimal(
                str(
                    evaluation.authoritative_rates[
                        (
                            *rate_key_plane,
                            proof["conversion_minute"],
                        )
                    ]
                )
            ),
            proof["operation"],
        )
        for proof in artifact["proofs"]
    )
    committed = tuple(
        (proof["conversion_minute"], proof["raw_close"], proof["operation"])
        for proof in artifact["proofs"]
    )
    return election_matches and committed == authoritative


async def shadow_pin_fx_evaluations(
    repo: Repository,
    evaluations: Sequence[FxShadowEvaluation],
    calculation_version: str,
) -> None:
    """Persist raw fiat elections while isolating every shadow-write failure."""
    for evaluation in evaluations:
        try:
            election, proofs = _build_artifact(
                evaluation, evaluation.requested_knowledge_at, calculation_version
            )
            artifact = await repo.pin_fx_conversion_artifact(election, proofs)
            if not _artifact_matches_raw(artifact, election, proofs, evaluation):
                _METRICS.mismatch += 1
                _log_once("mismatch", "FX shadow pin canonical mismatch for {}", evaluation.pair)
            elif artifact["election"]["public_id"] == election["public_id"]:
                _METRICS.creation += 1
            else:
                _METRICS.reuse += 1
        except FxConversionArtifactUpgradeRequiredError:
            _METRICS.upgrade_required += 1
            _log_once("upgrade_required", "FX shadow pin upgrade required for {}", evaluation.pair)
        except FxConversionArtifactConflictError:
            _METRICS.conflict += 1
            _log_once("conflict", "FX shadow pin conflict for {}", evaluation.pair)
        except Exception:
            _METRICS.failure += 1
            _log_once("failure", "FX shadow pin failure for {}", evaluation.pair)
