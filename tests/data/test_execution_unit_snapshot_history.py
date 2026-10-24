"""Historical unit evidence survives supersession while refusing missing or contradictory lineage."""

from collections.abc import AsyncIterator
from datetime import timedelta
from pathlib import Path
from typing import Literal

import pytest
from sqlalchemy import delete
from sqlalchemy import select
from sqlalchemy import text

from snapper.data.execution_unit_snapshot_history import get_bound_execution_unit_snapshot
from snapper.data.execution_unit_snapshot_store import SnapshotBindingError
from snapper.data.execution_unit_snapshot_store import insert_execution_unit_snapshot
from snapper.data.models import InstrumentSpec
from snapper.data.models import Order
from snapper.data.models import TradeCommand
from snapper.data.models import VenueEvent
from snapper.data.repository import SQLAlchemyRepository
from tests.data.test_execution_unit_snapshot_store import _NOW
from tests.data.test_execution_unit_snapshot_store import _id
from tests.data.test_execution_unit_snapshot_store import _payload
from tests.data.test_execution_unit_snapshot_store import _provenance
from tests.data.test_execution_unit_snapshot_store import _seed


@pytest.fixture
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Clone the closed real schema and persist genuine original execution/order/receipt lineage."""
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'history.db'}")
    try:
        await repository.create_all()
        await _seed(repository)
        yield repository
    finally:
        await repository.engine.dispose()


@pytest.mark.parametrize("phase", ("submission", "fill"))
async def test_historical_snapshot_uses_original_lineage_after_current_rows_close(
    repository: SQLAlchemyRepository, phase: Literal["submission", "fill"]
) -> None:
    """A later supersession cannot turn a historical unit witness into a missing current catalog.

    Given: A committed snapshot with real original durable lineage,
    When: Catalog, order and command versions close after interpretation,
    Then: Historical validation succeeds without a current freshness or venue query.
    """
    payload = await _payload(repository, phase)
    result = await insert_execution_unit_snapshot(repository, payload, _provenance())
    async with repository.session() as session:
        for model in (InstrumentSpec, Order, TradeCommand):
            row = (await session.execute(select(model))).scalar_one()
            row.known_to = _NOW + timedelta(seconds=10)
        await session.commit()
    async with repository.session() as session:
        assert await get_bound_execution_unit_snapshot(session, result.public_id) == payload


async def test_missing_historical_snapshot_never_manufactures_a_unit_interpretation(
    repository: SQLAlchemyRepository,
) -> None:
    """A financial event must reference an actual immutable snapshot.

    Given: A real execution with no captured snapshot,
    When: An absent snapshot identity is requested,
    Then: No catalog fallback or synthetic evidence is returned.
    """
    async with repository.session() as session:
        with pytest.raises(SnapshotBindingError, match="historical_snapshot_missing"):
            await get_bound_execution_unit_snapshot(session, _id(77))


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    (
        ("wallet_public_id", _id(77), "projection_mismatch"),
        ("spec_source", "contradictory-source", "projection_mismatch"),
        ("content_digest", "f" * 64, "projection_mismatch"),
        ("canonical_payload", "not-json", "payload_invalid"),
        ("timestamp", "2026-10-22 23:00:00.000000", "insert_precedes_interpretation"),
    ),
)
async def test_untrusted_retained_projection_cannot_override_immutable_material(
    repository: SQLAlchemyRepository, field: str, value: str, reason: str
) -> None:
    """A deliberately corrupted isolated fixture still fails a verified historical read.

    Given: An actual captured fill whose physical guard is removed only in this fixture,
    When: One indexed copy, payload or insertion clock is corrupted,
    Then: Read validation refuses rather than trusting a matching snapshot UUID.
    """
    payload = await _payload(repository, "fill")
    result = await insert_execution_unit_snapshot(repository, payload, _provenance())
    async with repository.engine.begin() as connection:
        await connection.exec_driver_sql("DROP TRIGGER execution_unit_snapshots_reject_update")
        await connection.execute(
            text(f"UPDATE execution_unit_snapshots SET {field}=:value"), {"value": value}
        )
    async with repository.session() as session:
        with pytest.raises(SnapshotBindingError, match=reason):
            await get_bound_execution_unit_snapshot(session, result.public_id)


async def test_missing_original_fill_observation_blocks_financial_interpretation(
    repository: SQLAlchemyRepository,
) -> None:
    """Hash-valid stored units alone cannot replace the actual original execution/event lineage.

    Given: An immutable fill snapshot whose original venue event is no longer available,
    When: Accounting loads the historical witness,
    Then: The missing durable target remains explicit.
    """
    result = await insert_execution_unit_snapshot(
        repository, await _payload(repository, "fill"), _provenance()
    )
    async with repository.session() as session:
        await session.execute(delete(VenueEvent))
        await session.commit()
    async with repository.session() as session:
        with pytest.raises(SnapshotBindingError, match="fill_target_missing"):
            await get_bound_execution_unit_snapshot(session, result.public_id)


async def test_catalog_must_have_been_effective_for_the_original_durable_fill(
    repository: SQLAlchemyRepository,
) -> None:
    """A real catalog row cannot retrospectively move outside the durable execution time.

    Given: A snapshot with a retained specification closed before execution,
    When: Its historical financial interpretation is requested,
    Then: Exact temporal catalog lineage is refused despite a valid content hash.
    """
    result = await insert_execution_unit_snapshot(
        repository, await _payload(repository, "fill"), _provenance()
    )
    async with repository.session() as session:
        spec = (await session.execute(select(InstrumentSpec))).scalar_one()
        spec.known_to = _NOW - timedelta(seconds=1)
        await session.commit()
    async with repository.session() as session:
        with pytest.raises(SnapshotBindingError, match="catalog_not_effective_for_target"):
            await get_bound_execution_unit_snapshot(session, result.public_id)
