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
from snapper.data.fx_conversion_carry import MAX_CARRIED_MINUTES
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

FX_ELECTION_POLICY_VERSION = "pnl-fiat-v2"
"""Identity of the election ALGORITHM, not of the data it elects.

It is part of the election uniqueness key, so it is what keeps two
contradictory answers to the same question from claiming to be the same
answer. Bump it whenever a change can make identical inputs at an identical
knowledge horizon elect differently.

``v2`` (2026-08-04) made bounded carry-forward reachable: a requirement whose
only mark predates it now elects `carried` where `v1` refused. Without the bump
a replay would produce append-only evidence that disagrees with the stored
`v1` row while asserting the same policy. Raising ``MAX_CARRIED_MINUTES``
rides on the same bump for the same reason.
"""
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
    """Process-local monotonic counters for shadow proof persistence.

    ``upgrade_required`` counts only defective writers competing at the same
    evidence horizon. Normal partial-to-complete evidence growth creates a new
    resolved horizon; both versions stay active and latest-visible is authority.
    """

    creation: int = 0
    reuse: int = 0
    conflict: int = 0
    upgrade_required: int = 0
    mismatch: int = 0
    failure: int = 0
    dropped: int = 0


_METRICS = FxShadowPinMetrics()


def fx_shadow_pin_metrics() -> FxShadowPinMetrics:
    """Return an immutable-by-copy snapshot of shadow pin counters.

    Returns:
        A detached snapshot of every process-local shadow counter.
    """
    return FxShadowPinMetrics(
        creation=_METRICS.creation,
        reuse=_METRICS.reuse,
        conflict=_METRICS.conflict,
        upgrade_required=_METRICS.upgrade_required,
        mismatch=_METRICS.mismatch,
        failure=_METRICS.failure,
        dropped=_METRICS.dropped,
    )


def reset_fx_shadow_pin_metrics() -> None:
    """Reset process-local counters for isolated service tests."""
    _METRICS.creation = 0
    _METRICS.reuse = 0
    _METRICS.conflict = 0
    _METRICS.upgrade_required = 0
    _METRICS.mismatch = 0
    _METRICS.failure = 0
    _METRICS.dropped = 0
    _LOGGED_FAILURE_CLASSES.clear()


_LOGGED_FAILURE_CLASSES: set[tuple[str, tuple[str, str]]] = set()


def _log_once(failure_class: str, message: str, pair: tuple[str, str]) -> None:
    """Emit one process-local diagnostic for each failure class and pair."""
    key = (failure_class, pair)
    if key in _LOGGED_FAILURE_CLASSES:
        return
    _LOGGED_FAILURE_CLASSES.add(key)
    logger.error(message, pair)


@dataclass
class FxShadowPinContext:
    """Opt-in per-tick collector flushed only after valuation work completes."""

    calculation_version: str
    evaluations: list[FxShadowEvaluation]
    manifest_limit: int | None = MAX_SHADOW_MANIFEST_MINUTES

    def collect(self, factory: Callable[[], Sequence[FxShadowEvaluation]]) -> None:
        """Collect guarded bounded evaluations without exposing construction errors.

        Args:
            factory: Deferred construction of raw election evaluations.
        """
        try:
            for evaluation in factory():
                if (
                    self.manifest_limit is not None
                    and len(evaluation.required_minutes) > self.manifest_limit
                ):
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
        """Persist every collected evaluation after the tick's valuation work.

        Args:
            repo: Repository receiving the shadow artifacts.
        """
        while self.evaluations:
            evaluation = self.evaluations[0]
            await shadow_pin_fx_evaluations(repo, (evaluation,), self.calculation_version)
            self.evaluations.pop(0)

    async def flush_bounded(self, repo: Repository) -> None:
        """Flush under the single per-tick deadline without failing valuation.

        Args:
            repo: Repository receiving the shadow artifacts.
        """
        try:
            await asyncio.wait_for(self.flush(repo), timeout=SHADOW_PIN_TICK_TIMEOUT_SECONDS)
        except TimeoutError:
            _METRICS.failure += 1
            dropped = len(self.evaluations)
            _METRICS.dropped += dropped
            self.evaluations.clear()
            _log_once(
                "timeout",
                f"FX shadow pin tick deadline exceeded; dropped={dropped} for {{}}",
                ("*", "*"),
            )


_ACTIVE_CONTEXT: ContextVar[FxShadowPinContext | None] = ContextVar(
    "fx_shadow_pin_context", default=None
)


def current_fx_shadow_context() -> FxShadowPinContext | None:
    """Return the explicitly activated snapshotter context for this async task.

    Returns:
        The active durable-consumer context, or null outside that scope.
    """
    return _ACTIVE_CONTEXT.get()


@contextmanager
def activate_fx_shadow_context(context: FxShadowPinContext | None) -> Iterator[None]:
    """Scope collection to an explicitly enabled durable-consumer valuation.

    Args:
        context: Collector to activate, or null to explicitly disable collection.

    Yields:
        Control while the supplied context is task-local and active.
    """
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
    """Build the documented row-derived fallback for tuple-only discovery.

    Args:
        candidates: Plane identities returned by tuple-only discovery.
        rows: Loaded evidence rows that can complete candidate provenance.
        source: Conversion source currency.
        target: Conversion target currency.

    Returns:
        Canonically sorted candidate planes with complete provenance.
    """
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
    _carry_into_gaps(evaluation, rows, resolved)
    return resolved


def _carry_into_gaps(
    evaluation: FxShadowEvaluation,
    rows: Sequence[PnlFxRateRow],
    resolved: dict[datetime, PnlFxRateRow],
) -> None:
    """Fill each unresolved required minute from the nearest earlier mark.

    A venue that publishes only on change leaves whole minutes with no candle,
    so a conversion landing in such a gap has no exact evidence. The last mark
    observed before the gap is the venue's own standing quote for it; carrying
    it forward is a bounded, recorded substitution, never an invented rate. The
    proof stores both minutes, so the distance stays auditable, and anything
    older than :data:`MAX_CARRIED_MINUTES` is left unresolved so the election
    refuses rather than valuing a trade on a stale rate.

    Args:
        evaluation: Raw election result naming the selected plane and minutes.
        rows: Every loaded evidence row for the pair.
        resolved: Exact-minute resolutions, extended in place with carries.
    """
    selected = evaluation.selected_plane
    missing = sorted(set(evaluation.required_minutes) - set(resolved))
    if selected is None or not missing:
        return
    usable: dict[datetime, PnlFxRateRow] = {}
    for row in rows:
        if (row["base"], row["quote"], row["exchange"]) != selected:
            continue
        if not is_positive_finite(row["close"]):
            continue
        minute = row["open_at"] + timedelta(minutes=1)
        candidate = usable.get(minute)
        if candidate is None or row["candle_id"] > candidate["candle_id"]:
            usable[minute] = row
    for gap in missing:
        earliest = gap - timedelta(minutes=MAX_CARRIED_MINUTES)
        available = [minute for minute in usable if earliest <= minute < gap]
        if available:
            resolved[gap] = usable[max(available)]


def carried_minutes_for(conversion_minute: datetime, row: PnlFxRateRow) -> int:
    """Return how many whole minutes a mark was carried into its gap.

    Zero means the mark was observed in the conversion minute itself, which is
    the pre-carry rule the database CHECK still encodes as the base case.

    Args:
        conversion_minute: Minute the conversion is valued at.
        row: Evidence row whose close supplied the rate.

    Returns:
        Whole minutes between the row's own close minute and the conversion.
    """
    observed = row["open_at"] + timedelta(minutes=1)
    return int((conversion_minute - observed).total_seconds() // 60)


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


def fx_shadow_evaluation_completeness(
    evaluation: FxShadowEvaluation,
) -> FxConversionCompleteness:
    """Classify one raw evaluation by its exact proven-minute subset.

    Args:
        evaluation: Raw election result whose selected rows are classified.

    Returns:
        Complete, carried, partial, or refused under the F2 artifact contract.
        ``carried`` means every required minute resolved but at least one took
        its rate from an earlier mark, so an auditor can tell a fully observed
        conversion from a substituted one without reading the proof rows.
    """
    selected_rows = _selected_rows(evaluation, evaluation.rows)
    proof_minutes = set(selected_rows)
    if proof_minutes == set(evaluation.required_minutes):
        carried = any(carried_minutes_for(minute, row) > 0 for minute, row in selected_rows.items())
        return "carried" if carried else "complete"
    if proof_minutes:
        return "partial"
    return "refused"


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
    state = fx_shadow_evaluation_completeness(evaluation)
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
        election_policy_version=FX_ELECTION_POLICY_VERSION,
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
            carried_minutes=carried_minutes_for(minute, row),
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
    try:
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
    except KeyError:
        return False
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
    """Persist raw fiat elections while isolating every shadow-write failure.

    Args:
        repo: Repository receiving canonical election and proof artifacts.
        evaluations: Already-authoritative raw valuation results to shadow.
        calculation_version: Consumer calculation contract recorded on elections.
    """
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
