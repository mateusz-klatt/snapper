"""Write-through shadow witnesses for fiat FX proof pinning."""

import math
from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast
from unittest.mock import patch

import pytest
from sqlalchemy import func
from sqlalchemy import select

from snapper.application.portfolio.fx_conversion_shadow import FxShadowEvaluation
from snapper.application.portfolio.fx_conversion_shadow import canonical_candidate_planes
from snapper.application.portfolio.fx_conversion_shadow import fx_shadow_pin_metrics
from snapper.application.portfolio.fx_conversion_shadow import reset_fx_shadow_pin_metrics
from snapper.application.portfolio.fx_conversion_shadow import shadow_pin_fx_evaluations
from snapper.data.models import FxConversionElection
from snapper.data.models import FxConversionProof
from snapper.data.repository import Repository
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import FxConversionArtifactRow
from snapper.data.repository_types import FxConversionElectionInsertRow
from snapper.data.repository_types import FxConversionProofInsertRow
from snapper.data.repository_types import FxConversionProofRow
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
) -> FxShadowEvaluation:
    """Build one shared-pair raw election result."""
    return FxShadowEvaluation(
        scope_kind="shared_pair",
        consumer_instrument_public_id=None,
        pair=("EUR", "USD"),
        target_currency="USD",
        required_minutes=minutes,
        candidate_planes=frozenset({("EUR", "USD", "kraken")}),
        selected_plane=selected,
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
    complete = _evaluation(frozenset({_MINUTE, next_minute}))
    complete_rows = [_row(), _row(next_minute)]
    await shadow_pin_fx_evaluations(repository, [complete], complete_rows, _AS_OF, "5A.13")
    await shadow_pin_fx_evaluations(repository, [complete], complete_rows, _AS_OF, "5A.13")
    rejected_row = _row(exchange="walutomat", instrument="00000000-0000-7000-8000-000000000805")
    conflict = replace(
        complete,
        candidate_planes=frozenset({("EUR", "USD", "kraken"), ("EUR", "USD", "walutomat")}),
    )
    await shadow_pin_fx_evaluations(
        repository, [conflict], [*complete_rows, rejected_row], _AS_OF, "5A.13"
    )
    missing = _MINUTE + timedelta(minutes=2)
    invalid_row: PnlFxRateRow = {**_row(missing), "close": math.nan}
    await shadow_pin_fx_evaluations(
        repository,
        [_evaluation(frozenset({_MINUTE, missing}))],
        [_row(), invalid_row],
        _AS_OF + timedelta(minutes=1),
        "5A.13",
    )
    await shadow_pin_fx_evaluations(
        repository,
        [_evaluation(frozenset({missing}), None)],
        [],
        _AS_OF + timedelta(minutes=2),
        "5A.13",
    )
    metrics = fx_shadow_pin_metrics()
    assert (metrics.creation, metrics.reuse, metrics.conflict, metrics.failure) == (3, 1, 1, 0)
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
    await shadow_pin_fx_evaluations(repository, [_evaluation()], [_row()], _AS_OF, "5A.13")
    assert fx_shadow_pin_metrics().failure == 1


@pytest.mark.asyncio
async def test_shadow_canonical_mismatch_is_observed_without_escaping(tmp_path: Path) -> None:
    """A committed artifact differing from raw proof values increments failure."""
    reset_fx_shadow_pin_metrics()
    repository = await _repository(tmp_path)
    original = repository.pin_fx_conversion_artifact

    async def _mismatching_pin(
        election: FxConversionElectionInsertRow,
        proofs: Sequence[FxConversionProofInsertRow],
    ) -> FxConversionArtifactRow:
        """Return a deliberately altered reread artifact after a real commit."""
        artifact = await original(election, proofs)
        altered = cast(
            FxConversionProofRow,
            {**artifact["proofs"][0], "conversion_rate": Decimal("9")},
        )
        return {"election": artifact["election"], "proofs": (altered,)}

    with patch.object(repository, "pin_fx_conversion_artifact", _mismatching_pin):
        await shadow_pin_fx_evaluations(repository, [_evaluation()], [_row()], _AS_OF, "5A.13")
    assert fx_shadow_pin_metrics().failure == 1
    await repository.engine.dispose()
