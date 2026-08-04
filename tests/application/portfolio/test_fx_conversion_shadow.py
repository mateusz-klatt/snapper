"""Write-through shadow witnesses for fiat FX proof pinning."""

import asyncio
import math
from collections.abc import Coroutine
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest
from sqlalchemy import func
from sqlalchemy import select

from snapper.application.portfolio.fx_conversion_shadow import FxShadowEvaluation
from snapper.application.portfolio.fx_conversion_shadow import FxShadowPinContext
from snapper.application.portfolio.fx_conversion_shadow import _artifact_matches_raw
from snapper.application.portfolio.fx_conversion_shadow import _build_artifact
from snapper.application.portfolio.fx_conversion_shadow import _log_once
from snapper.application.portfolio.fx_conversion_shadow import canonical_candidate_planes
from snapper.application.portfolio.fx_conversion_shadow import carried_minutes_for
from snapper.application.portfolio.fx_conversion_shadow import fx_shadow_evaluation_completeness
from snapper.application.portfolio.fx_conversion_shadow import fx_shadow_pin_metrics
from snapper.application.portfolio.fx_conversion_shadow import reset_fx_shadow_pin_metrics
from snapper.application.portfolio.fx_conversion_shadow import shadow_pin_fx_evaluations
from snapper.application.portfolio.fx_rates import convert_amount
from snapper.application.portfolio.fx_rates import currency_pair_key
from snapper.data.fx_conversion_carry import MAX_CARRIED_MINUTES
from snapper.data.models import FxConversionElection
from snapper.data.models import FxConversionProof
from snapper.data.repository import FxConversionArtifactUpgradeRequiredError
from snapper.data.repository import Repository
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import FxConversionArtifactRow
from snapper.data.repository_types import FxConversionCandidatePlane
from snapper.data.repository_types import FxConversionElectionInsertRow
from snapper.data.repository_types import PnlFxRateRow

_MINUTE = datetime(2026, 8, 2, 12, 0, tzinfo=UTC)
_AS_OF = datetime(2026, 8, 2, 13, 0, tzinfo=UTC)
_SESSION = "00000000-0000-7000-8000-000000000801"
_INSTRUMENT = "00000000-0000-7000-8000-000000000802"


def _row(
    minute: datetime = _MINUTE,
    exchange: str = "kraken",
    instrument: str = _INSTRUMENT,
) -> PnlFxRateRow:
    """Build one exact source row with complete temporal provenance."""
    return {
        "base": "EUR",
        "quote": "USD",
        "exchange": exchange,
        "open_at": minute - timedelta(minutes=1),
        "close": 1.125,
        "native_symbol": "EUR/USD",
        "instrument_public_id": instrument,
        "candle_id": 42,
        "candle_public_id": "00000000-0000-7000-8000-000000000803",
        "candle_session_id": _SESSION,
        "candle_sequence_id": 7,
        "candle_timestamp": minute - timedelta(minutes=1),
        "candle_known_to": datetime.max.replace(tzinfo=UTC),
    }


def _evaluation(
    minutes: frozenset[datetime] = frozenset({_MINUTE}),
    selected: tuple[str, str, str] | None = ("EUR", "USD", "kraken"),
    rows: tuple[PnlFxRateRow, ...] | None = None,
    requested_at: datetime = _AS_OF,
) -> FxShadowEvaluation:
    """Build one shared-pair raw election result."""
    evidence_rows = rows if rows is not None else (_row(),)
    rates = {
        (row["base"], row["quote"], row["exchange"], row["open_at"] + timedelta(minutes=1)): row[
            "close"
        ]
        for row in evidence_rows
    }
    return FxShadowEvaluation(
        scope_kind="shared_pair",
        consumer_instrument_public_id=None,
        pair=("EUR", "USD"),
        target_currency="USD",
        required_minutes=minutes,
        candidate_planes=frozenset({("EUR", "USD", "kraken")}),
        selected_plane=selected,
        requested_knowledge_at=requested_at,
        rows=evidence_rows,
        authoritative_rates=rates,
    )


async def _repository(tmp_path: Path) -> SQLAlchemyRepository:
    """Create an isolated model-produced repository."""
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'shadow.db'}")
    await repository.create_all()
    return repository


def test_candidate_enumeration_is_stable_across_identical_evaluations() -> None:
    """Discovery and row order cannot perturb considered candidate planes.

    Given equivalent candidate and evidence sets in different orders
    When their canonical candidate planes are built
    Then both evaluations enumerate byte-identical candidates
    """
    kraken = _row()
    walutomat = _row(exchange="walutomat", instrument="00000000-0000-7000-8000-000000000804")
    candidates = [("EUR", "USD", "kraken"), ("EUR", "USD", "walutomat")]
    first = canonical_candidate_planes(candidates, [kraken, walutomat], "EUR", "USD")
    second = canonical_candidate_planes(
        list(reversed(candidates)), [walutomat, kraken], "EUR", "USD"
    )
    assert first == second


@pytest.mark.asyncio
async def test_shadow_creation_reuse_conflict_partial_and_refusal_are_audited(
    tmp_path: Path,
) -> None:
    """All raw outcomes pin while successful identity races remain observable.

    Given complete, conflicting, partial, and refused raw elections
    When the shadow writer persists and repeats them
    Then artifacts and outcome counters preserve their distinct semantics
    """
    reset_fx_shadow_pin_metrics()
    repository = await _repository(tmp_path)
    next_minute = _MINUTE + timedelta(minutes=1)
    complete_rows = [_row(), _row(next_minute)]
    complete = _evaluation(frozenset({_MINUTE, next_minute}), rows=tuple(complete_rows))
    await shadow_pin_fx_evaluations(repository, [complete], "5A.13")
    await shadow_pin_fx_evaluations(
        repository,
        [replace(complete, requested_knowledge_at=_AS_OF + timedelta(minutes=5))],
        "5A.13",
    )
    rejected_row = _row(exchange="walutomat", instrument="00000000-0000-7000-8000-000000000805")
    conflict = replace(
        complete,
        candidate_planes=frozenset({("EUR", "USD", "kraken"), ("EUR", "USD", "walutomat")}),
    )
    conflict = replace(
        conflict,
        rows=(*complete.rows, rejected_row),
        authoritative_rates={
            **complete.authoritative_rates,
            ("EUR", "USD", "walutomat", _MINUTE): rejected_row["close"],
        },
    )
    await shadow_pin_fx_evaluations(repository, [conflict], "5A.13")
    missing = _MINUTE + timedelta(minutes=MAX_CARRIED_MINUTES + 2)
    invalid_row: PnlFxRateRow = {**_row(missing), "close": math.nan}
    await shadow_pin_fx_evaluations(
        repository,
        [
            _evaluation(
                frozenset({_MINUTE, missing}),
                rows=(_row(), invalid_row),
                requested_at=_AS_OF + timedelta(minutes=1),
            )
        ],
        "5A.13",
    )
    refusal = _evaluation(
        frozenset({missing}),
        None,
        rows=(),
        requested_at=_AS_OF + timedelta(minutes=2),
    )
    await shadow_pin_fx_evaluations(
        repository,
        [refusal],
        "5A.13",
    )
    await shadow_pin_fx_evaluations(
        repository,
        [replace(refusal, requested_knowledge_at=_AS_OF + timedelta(minutes=8))],
        "5A.13",
    )
    metrics = fx_shadow_pin_metrics()
    assert (
        metrics.creation,
        metrics.reuse,
        metrics.conflict,
        metrics.upgrade_required,
        metrics.mismatch,
        metrics.failure,
        metrics.dropped,
    ) == (3, 2, 1, 0, 0, 0, 0)
    async with repository.session() as session:
        states: dict[str, int] = dict(
            (
                await session.execute(
                    select(
                        FxConversionElection.completeness_state,
                        func.count(FxConversionElection.id),
                    ).group_by(FxConversionElection.completeness_state)
                )
            ).all()
        )
        proof_count = await session.scalar(select(func.count(FxConversionProof.id)))
        refusal_reason = await session.scalar(
            select(FxConversionElection.refusal_reason_json).where(
                FxConversionElection.completeness_state == "refused"
            )
        )
    assert states == {"complete": 1, "partial": 1, "refused": 1}
    assert proof_count == 3
    assert (
        refusal_reason
        == '{"reason":"fx_conversion_unproven","unproven_minutes":["' + missing.isoformat() + '"]}'
    )
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_shadow_failure_is_isolated_from_the_raw_caller() -> None:
    """An arbitrary persistence failure increments failure and never escapes.

    Given a repository that rejects every shadow artifact
    When a raw evaluation is shadow-pinned
    Then the failure is counted without escaping to valuation
    """

    class _FailingRepository:
        async def pin_fx_conversion_artifact(self, election: object, proofs: object) -> object:
            """Raise the storage failure the shadow boundary must contain."""
            raise RuntimeError("database unavailable")

    reset_fx_shadow_pin_metrics()
    repository = cast(Repository, _FailingRepository())
    await shadow_pin_fx_evaluations(repository, [_evaluation()], "5A.13")
    assert fx_shadow_pin_metrics().failure == 1


@pytest.mark.asyncio
async def test_shadow_canonical_mismatch_is_observed_without_escaping(tmp_path: Path) -> None:
    """A divergence from the authoritative raw rate fold increments mismatch.

    Given a committed proof whose close differs from the consumed raw map
    When the committed artifact is compared after pinning
    Then mismatch increments without becoming a persistence failure
    """
    reset_fx_shadow_pin_metrics()
    repository = await _repository(tmp_path)
    evaluation = replace(
        _evaluation(),
        authoritative_rates={("EUR", "USD", "kraken", _MINUTE): 9.0},
    )
    await shadow_pin_fx_evaluations(repository, [evaluation], "5A.13")
    assert fx_shadow_pin_metrics().mismatch == 1
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_missing_authoritative_plane_is_mismatch_not_failure(tmp_path: Path) -> None:
    """A committed plane absent from raw authority is the strongest mismatch.

    Given a pin whose selected plane has no key in the authoritative rate map
    When the committed artifact is compared with valuation output
    Then mismatch increments and generic failure remains unchanged
    """
    reset_fx_shadow_pin_metrics()
    repository = await _repository(tmp_path)
    await shadow_pin_fx_evaluations(
        repository,
        [replace(_evaluation(), authoritative_rates={})],
        "5A.13",
    )
    metrics = fx_shadow_pin_metrics()
    assert metrics.mismatch == 1
    assert metrics.failure == 0
    await repository.engine.dispose()


def test_log_once_is_scoped_by_failure_class_and_pair(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Log suppression retains one diagnostic for every affected pair.

    Given repeated mismatch diagnostics for one pair and one for another
    When pair-scoped log-once suppression is applied
    Then each distinct pair logs exactly once
    """
    reset_fx_shadow_pin_metrics()
    logged: list[tuple[str, tuple[str, str]]] = []

    def capture(message: str, pair: tuple[str, str]) -> None:
        """Capture the structured log call without depending on logger sinks."""
        logged.append((message, pair))

    monkeypatch.setattr("snapper.application.portfolio.fx_conversion_shadow.logger.error", capture)
    _log_once("mismatch", "FX mismatch for {}", ("EUR", "USD"))
    _log_once("mismatch", "FX mismatch for {}", ("EUR", "USD"))
    _log_once("mismatch", "FX mismatch for {}", ("GBP", "USD"))
    assert logged == [
        ("FX mismatch for {}", ("EUR", "USD")),
        ("FX mismatch for {}", ("GBP", "USD")),
    ]


def test_context_guards_collection_and_skips_bulk_manifests() -> None:
    """Collection exceptions and catch-up-sized manifests never reach the tick.

    Given a broken factory and repeated oversized evaluation manifests
    When the guarded context collects them
    Then no evaluation escapes and collection failure is counted once
    """
    reset_fx_shadow_pin_metrics()
    context = FxShadowPinContext(calculation_version="5B.2", evaluations=[])

    def _broken() -> list[FxShadowEvaluation]:
        """Raise before an evaluations list can escape the guarded scope."""
        raise KeyError("missing provenance")

    context.collect(_broken)
    context.collect(
        lambda: [_evaluation(frozenset(_MINUTE + timedelta(minutes=index) for index in range(16)))]
    )
    context.collect(
        lambda: [_evaluation(frozenset(_MINUTE + timedelta(minutes=index) for index in range(16)))]
    )
    assert context.evaluations == []
    assert fx_shadow_pin_metrics().failure == 1


@pytest.mark.asyncio
async def test_context_flush_deadline_is_failure_isolated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The deadline counts and reports evaluations abandoned by timeout.

    Given two collected evaluations and a wait that reaches its deadline
    When the bounded shadow flush is attempted
    Then both drops and the failure are observable without coroutine leakage
    """
    context = FxShadowPinContext(
        calculation_version="5B.2", evaluations=[_evaluation(), _evaluation()]
    )
    reset_fx_shadow_pin_metrics()

    async def _timeout(coro: Coroutine[object, object, None], timeout: float) -> None:
        """Close the pending flush before simulating the deadline."""
        assert timeout == 10.0
        coro.close()
        raise TimeoutError

    logged: list[tuple[str, tuple[str, str]]] = []

    def capture(message: str, pair: tuple[str, str]) -> None:
        """Capture the timeout diagnostic without depending on logger sinks."""
        logged.append((message, pair))

    monkeypatch.setattr(asyncio, "wait_for", _timeout)
    monkeypatch.setattr("snapper.application.portfolio.fx_conversion_shadow.logger.error", capture)
    await context.flush_bounded(cast(Repository, object()))
    metrics = fx_shadow_pin_metrics()
    assert metrics.failure == 1
    assert metrics.dropped == 2
    assert context.evaluations == []
    assert logged == [("FX shadow pin tick deadline exceeded; dropped=2 for {}", ("*", "*"))]


@pytest.mark.asyncio
async def test_duplicate_version_alignment(tmp_path: Path) -> None:
    """Pinned provenance follows valuation's duplicate-version winner.

    Given duplicate rows for one plane and minute with different candle ids
    When the evaluation is shadow-pinned
    Then the greatest candle id supplies the committed provenance
    """
    repository = await _repository(tmp_path)
    older = _row()
    newer: PnlFxRateRow = {
        **older,
        "candle_id": 99,
        "candle_public_id": "00000000-0000-7000-8000-000000000899",
    }
    evaluation = _evaluation(rows=(older, newer))
    await shadow_pin_fx_evaluations(repository, [evaluation], "5B.2")
    async with repository.session() as session:
        proof = (await session.execute(select(FxConversionProof))).scalars().one()
    assert proof.candle_id == 99
    await repository.engine.dispose()


def test_inverse_replay_divides_each_authoritative_amount() -> None:
    """Inverse replay uses production division rather than a reciprocal multiplier.

    Given many amounts and one inverse proof represented by raw close plus operation
    When each amount is replayed by division and converted by production
    Then every floating-point result is exactly identical
    """
    raw_close = Decimal("0.9000000000000001")
    rate = float(raw_close)
    rates = {("USD", "EUR", "kraken", _MINUTE): rate}
    planes = {currency_pair_key("EUR", "USD"): ("USD", "EUR", "kraken")}
    amounts = [index * 0.125 + index % 17 * 0.000_001 for index in range(1, 2001)]
    for amount in amounts:
        converted = convert_amount(amount, "EUR", "USD", _MINUTE, rates, planes)
        assert converted is not None
        assert amount / rate == converted


def test_discovered_candidates_override_row_fallback_and_resolve_from_evidence() -> None:
    """Caller discovery retains rowless planes while evidence fixes identity time.

    Given full discovery with selected and rowless candidate planes
    When the raw election artifact is constructed
    Then every candidate remains and evidence bus time defines its horizon
    """
    selected = FxConversionCandidatePlane(
        source_exchange="kraken",
        source_instrument_public_id=_INSTRUMENT,
        native_symbol="EUR/USD",
        base="EUR",
        quote="USD",
        orientation="direct",
    )
    rowless = FxConversionCandidatePlane(
        source_exchange="walutomat",
        source_instrument_public_id="00000000-0000-7000-8000-0000000008aa",
        native_symbol="EURUSD",
        base="EUR",
        quote="USD",
        orientation="direct",
    )
    evaluation = replace(_evaluation(), discovered_candidates=(selected, rowless))
    election, _ = _build_artifact(evaluation, _AS_OF, "5B.2")
    assert election["considered_candidate_planes"] == (selected, rowless)
    assert election["resolved_knowledge_at"] == _row()["candle_timestamp"]


@pytest.mark.asyncio
async def test_same_horizon_upgrade_metric_and_impossible_plane_guard(tmp_path: Path) -> None:
    """Same-horizon defects stay distinct and malformed committed planes mismatch.

    Given a defective same-horizon writer and an impossible committed plane
    When each shadow result is classified
    Then upgrade-required and mismatch remain distinct incident classes
    """

    class _UpgradeRepository:
        async def pin_fx_conversion_artifact(self, election: object, proofs: object) -> object:
            """Raise the typed operator-gated upgrade incident."""
            winner = cast(
                FxConversionArtifactRow,
                {"election": {"public_id": "winner"}, "proofs": ()},
            )
            raise FxConversionArtifactUpgradeRequiredError(winner)

    reset_fx_shadow_pin_metrics()
    await shadow_pin_fx_evaluations(cast(Repository, _UpgradeRepository()), [_evaluation()], "5B.2")
    assert fx_shadow_pin_metrics().upgrade_required == 1
    repository = await _repository(tmp_path)
    evaluation = _evaluation()
    election, proofs = _build_artifact(evaluation, _AS_OF, "5B.2")
    artifact = await repository.pin_fx_conversion_artifact(election, proofs)
    malformed = cast(FxConversionElectionInsertRow, {**election, "selected_base": None})
    assert not _artifact_matches_raw(artifact, malformed, proofs, evaluation)
    await repository.engine.dispose()


def _gap_evaluation(gap_minutes: int, mark_minutes: tuple[int, ...] = (0,)) -> FxShadowEvaluation:
    """Build an election needing a minute that only earlier marks can cover.

    Args:
        gap_minutes: Offset of the required minute that has no own mark.
        mark_minutes: Offsets of the minutes that DO carry a mark.

    Returns:
        One shared-pair evaluation whose gap minute has no exact evidence.
    """
    gap = _MINUTE + timedelta(minutes=gap_minutes)
    rows = tuple(_row(_MINUTE + timedelta(minutes=offset)) for offset in mark_minutes)
    return _evaluation(frozenset({gap}), rows=rows)


def test_carry_resolves_a_gap_within_the_bound() -> None:
    """A minute with no mark takes the last one observed before it.

    Given a required minute five minutes past its only mark
    When the raw election is classified and built
    Then it resolves as carried and records the distance it travelled
    """
    evaluation = _gap_evaluation(5)

    assert fx_shadow_evaluation_completeness(evaluation) == "carried"

    _, proofs = _build_artifact(evaluation, _AS_OF, "5A.13")
    assert len(proofs) == 1
    assert proofs[0]["carried_minutes"] == 5
    assert proofs[0]["conversion_minute"] == _MINUTE + timedelta(minutes=5)
    assert proofs[0]["candle_open_minute"] == _MINUTE - timedelta(minutes=1)


def test_carry_stops_at_the_bound() -> None:
    """The furthest admissible carry still resolves, one minute more does not.

    Given gaps exactly at and one minute beyond the bound
    When each raw election is classified
    Then the bound is inclusive and the next minute refuses
    """
    assert fx_shadow_evaluation_completeness(_gap_evaluation(MAX_CARRIED_MINUTES)) == "carried"
    assert fx_shadow_evaluation_completeness(_gap_evaluation(MAX_CARRIED_MINUTES + 1)) == "refused"


def test_exact_minute_is_complete_and_carries_nothing() -> None:
    """An observed minute stays complete so audits keep the distinction.

    Given a required minute that has its own mark
    When the raw election is classified and built
    Then it is complete and its proof records a zero carry
    """
    evaluation = _evaluation()

    assert fx_shadow_evaluation_completeness(evaluation) == "complete"

    _, proofs = _build_artifact(evaluation, _AS_OF, "5A.13")
    assert proofs[0]["carried_minutes"] == 0


def test_carry_takes_the_nearest_earlier_mark() -> None:
    """Freshness wins: the closest preceding mark supplies the rate.

    Given two marks before the gap
    When the gap is resolved
    Then the later of them is carried, not the older one
    """
    evaluation = _gap_evaluation(6, mark_minutes=(0, 4))

    _, proofs = _build_artifact(evaluation, _AS_OF, "5A.13")

    assert proofs[0]["carried_minutes"] == 2


def test_carry_uses_the_newest_duplicate_from_only_the_selected_plane() -> None:
    """Foreign planes and older duplicate candles cannot replace the winner.

    Given a foreign row and two selected-plane rows for one preceding minute
    When the gap is resolved by carrying the selected plane forward
    Then the selected row with the greatest durable candle id supplies proof
    """
    gap = _MINUTE + timedelta(minutes=5)
    previous = _MINUTE + timedelta(minutes=4)
    foreign = _row(previous, exchange="walutomat")
    newest = cast(PnlFxRateRow, {**_row(previous), "candle_id": 99})
    older = cast(PnlFxRateRow, {**_row(previous), "candle_id": 1})
    evaluation = _evaluation(frozenset({gap}), rows=(foreign, newest, older))

    _, proofs = _build_artifact(evaluation, _AS_OF, "5A.13")

    assert len(proofs) == 1
    assert proofs[0]["candle_id"] == 99


def test_carry_refuses_a_source_minute_whose_duplicates_disagree() -> None:
    """Row order must not elect a carried price the exact path would refuse.

    Given two active rows for one preceding minute quoting different closes
    When the gap is resolved by carrying the selected plane forward
    Then that minute is discarded and an older undisputed mark is used instead
    """
    gap = _MINUTE + timedelta(minutes=5)
    disputed = _MINUTE + timedelta(minutes=4)
    undisputed = _row(_MINUTE + timedelta(minutes=1))
    high = cast(PnlFxRateRow, {**_row(disputed), "candle_id": 99, "close": 9.0})
    low = cast(PnlFxRateRow, {**_row(disputed), "candle_id": 1, "close": 2.0})
    evaluation = _evaluation(frozenset({gap}), rows=(undisputed, high, low))

    _, proofs = _build_artifact(evaluation, _AS_OF, "5A.13")

    assert len(proofs) == 1
    assert proofs[0]["carried_minutes"] == 4
    assert proofs[0]["candle_open_minute"] == undisputed["open_at"]


def test_carry_never_looks_forward() -> None:
    """A later mark cannot value an earlier conversion.

    Given the only mark arrives after the required minute
    When the raw election is classified
    Then it refuses rather than borrowing a future rate
    """
    evaluation = _gap_evaluation(0, mark_minutes=(3,))

    assert fx_shadow_evaluation_completeness(evaluation) == "refused"


def test_carried_minutes_for_counts_whole_minutes_from_the_close() -> None:
    """The recorded distance is measured from the mark's own close minute.

    Given a row whose close minute precedes the conversion
    When the carried distance is derived
    Then it counts whole minutes between the two
    """
    row = _row()

    assert carried_minutes_for(_MINUTE, row) == 0
    assert carried_minutes_for(_MINUTE + timedelta(minutes=7), row) == 7
