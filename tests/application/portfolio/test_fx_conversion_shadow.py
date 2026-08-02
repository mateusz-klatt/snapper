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
from snapper.application.portfolio.fx_conversion_shadow import canonical_candidate_planes
from snapper.application.portfolio.fx_conversion_shadow import fx_shadow_pin_metrics
from snapper.application.portfolio.fx_conversion_shadow import replay_proof_rate
from snapper.application.portfolio.fx_conversion_shadow import reset_fx_shadow_pin_metrics
from snapper.application.portfolio.fx_conversion_shadow import shadow_pin_fx_evaluations
from snapper.application.portfolio.fx_rates import convert_amount
from snapper.application.portfolio.fx_rates import currency_pair_key
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
    """Discovery and row order cannot perturb considered candidate planes."""
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
    """All raw outcomes pin while successful identity races remain observable."""
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
    missing = _MINUTE + timedelta(minutes=2)
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
    ) == (3, 2, 1, 0, 0, 0)
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
        == '{"reason":"fx_conversion_unproven","unproven_minutes":["2026-08-02T12:02:00+00:00"]}'
    )
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_shadow_failure_is_isolated_from_the_raw_caller() -> None:
    """An arbitrary persistence failure increments failure and never escapes."""

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
    """A divergence from the authoritative raw rate fold increments mismatch."""
    reset_fx_shadow_pin_metrics()
    repository = await _repository(tmp_path)
    evaluation = replace(
        _evaluation(),
        authoritative_rates={("EUR", "USD", "kraken", _MINUTE): 9.0},
    )
    await shadow_pin_fx_evaluations(repository, [evaluation], "5A.13")
    assert fx_shadow_pin_metrics().mismatch == 1
    await repository.engine.dispose()


def test_context_guards_collection_and_skips_bulk_manifests() -> None:
    """Collection exceptions and catch-up-sized manifests never reach the tick."""
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
    """The single per-tick wait budget times out without leaking its coroutine."""
    context = FxShadowPinContext(calculation_version="5B.2", evaluations=[])
    reset_fx_shadow_pin_metrics()

    async def _timeout(coro: Coroutine[object, object, None], timeout: float) -> None:
        """Close the pending flush before simulating the deadline."""
        assert timeout == 10.0
        coro.close()
        raise TimeoutError

    monkeypatch.setattr(asyncio, "wait_for", _timeout)
    await context.flush_bounded(cast(Repository, object()))
    assert fx_shadow_pin_metrics().failure == 1


@pytest.mark.asyncio
async def test_duplicate_version_alignment_and_inverse_replay(tmp_path: Path) -> None:
    """Last candle identity wins and inverse replay divides by the raw close."""
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
    inverse_close = Decimal("0.9")
    replayed = replay_proof_rate(inverse_close, "inverse")
    converted = convert_amount(
        1.0,
        "EUR",
        "USD",
        _MINUTE,
        {("USD", "EUR", "kraken", _MINUTE): 0.9},
        {currency_pair_key("EUR", "USD"): ("USD", "EUR", "kraken")},
    )
    assert converted is not None
    assert float(replayed) == converted
    await repository.engine.dispose()


def test_discovered_candidates_override_row_fallback_and_resolve_from_evidence() -> None:
    """Caller-supplied discovery retains row-less planes and evidence fixes identity time."""
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
async def test_upgrade_metric_and_impossible_selected_plane_guard(tmp_path: Path) -> None:
    """Upgrade incidents stay distinct and malformed committed planes mismatch."""

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
