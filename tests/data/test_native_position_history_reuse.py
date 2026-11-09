"""Compose actual native history proofs without repeating a complete original source read."""

from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Literal

import pytest
from sqlalchemy import text
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

import snapper.data.accounting_native_source_store_v2 as claim_store
from snapper.application.portfolio.accounting_native_source_v2 import (
    AccountingNativeSourceClaimPayloadV2,
)
from snapper.data.accounting_native_source_triggers import install_accounting_native_source_triggers
from snapper.data.models import AccountingNativeSourceClaim
from snapper.data.native_financial_event_binding_v2 import NativeFinancialEventMaterialV2
from snapper.data.native_financial_event_family import get_native_financial_event_family
from snapper.data.native_financial_source_binding_v2 import NativeFinancialBindingError
from tests.helpers.native_position_recording_v3 import native_position_recording_scenario_v3
from tests.helpers.native_position_recording_v3 import record_native_position_scenario_v3


@dataclass(frozen=True)
class _HistoryRead:
    """Actual original source material retained only within its owning history session."""

    session: AsyncSession
    material: NativeFinancialEventMaterialV2


@pytest.fixture
async def history_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[_HistoryRead]:
    """Commit the actual SDK source graph before independently rebinding its original event."""
    async with native_position_recording_scenario_v3(tmp_path, monkeypatch) as scenario:
        recorded = await record_native_position_scenario_v3(scenario)
        async with scenario.source.repository.session() as session:
            material = await get_native_financial_event_family(
                session, recorded.position.public_id, datetime.now(UTC)
            )
            yield _HistoryRead(session, material)


async def test_bound_history_material_retains_independent_claim_validation(
    history_read: _HistoryRead, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reuse one actual original proof while keeping the independent getter's full source read.

    Given: The actual SDK financial event has already been bound in this history session.
    When: Claim composition reuses that material and the standalone getter reads it independently.
    Then: Both claims agree, only the standalone path rebinds the graph, and detached or future evidence refuses.
    """
    session, material = history_read.session, history_read.material
    clock = datetime.now(UTC)
    actual = claim_store.get_native_financial_event_material
    reads: list[str] = []

    async def observe(
        active: AsyncSession, public_id: str, known_at: datetime
    ) -> NativeFinancialEventMaterialV2:
        """Count original source reads while delegating every actual SQL and integrity check."""
        reads.append(public_id)
        return await actual(active, public_id, known_at)

    monkeypatch.setattr(claim_store, "get_native_financial_event_material", observe)
    composed = await claim_store._get_bound_native_source_claim_from_material_v2(
        session, material, clock
    )
    assert reads == []
    independent = await claim_store.get_bound_native_source_claim_v2(
        session, material.row.public_id, clock
    )
    assert composed == independent
    assert reads == [material.row.public_id]
    with pytest.raises(NativeFinancialBindingError, match="claim_provenance_mismatch"):
        await claim_store._get_bound_native_source_claim_from_material_v2(
            session, material, material.row.timestamp - timedelta(microseconds=1)
        )
    session.expunge(material.row)
    with pytest.raises(NativeFinancialBindingError, match="claim_provenance_mismatch"):
        await claim_store._get_bound_native_source_claim_from_material_v2(session, material, clock)


def _claim_damage(
    payload: AccountingNativeSourceClaimPayloadV2,
    damage: Literal["projection", "provenance", "lineage"],
) -> tuple[dict[str, object], str]:
    """Keep each restored claim coherent except for one exact actual-event invariant."""
    if damage == "projection":
        return {"content_digest": "a" * 64}, "claim_projection_mismatch"
    if damage == "provenance":
        return {"sequence_id": 999999}, "claim_provenance_mismatch"
    changed = payload.model_copy(update={"accounting_event_content_digest": "a" * 64})
    return claim_store.native_claim_columns_v2(changed), "claim_original_lineage_mismatch"


@pytest.mark.parametrize("damage", ["projection", "provenance", "lineage"])
async def test_bound_history_material_still_refuses_actual_damaged_claims(
    history_read: _HistoryRead,
    damage: Literal["projection", "provenance", "lineage"],
) -> None:
    """Reusing an already bound event never treats its claim as verified by association.

    Given: Real original SDK sources, financial event and shared claim have committed.
    When: A scoped restore damages only the claim within the original source-binding transaction.
    Then: Composition queries that actual damaged claim and refuses its exact broken invariant.
    """
    session, material = history_read.session, history_read.material
    transaction = session.get_transaction()
    assert transaction is not None
    clock = datetime.now(UTC)
    original = await claim_store._get_bound_native_source_claim_from_material_v2(
        session, material, clock
    )
    changes, expected = _claim_damage(original, damage)
    connection = await session.connection()
    await connection.execute(text("DROP TRIGGER accounting_native_source_claims_reject_update"))
    await connection.execute(
        update(AccountingNativeSourceClaim)
        .where(AccountingNativeSourceClaim.accounting_event_public_id == material.row.public_id)
        .values(**changes)
    )
    await connection.run_sync(
        install_accounting_native_source_triggers, "accounting_native_source_claims"
    )
    assert session.get_transaction() is transaction
    with pytest.raises(NativeFinancialBindingError, match=expected):
        await claim_store._get_bound_native_source_claim_from_material_v2(session, material, clock)
