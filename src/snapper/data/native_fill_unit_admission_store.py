"""Rebind retained native financial admission edges through actual source, unit and command rows."""

from dataclasses import dataclass
from datetime import datetime

from pydantic import TypeAdapter
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.execution_unit_snapshot import ExecutionUnitSnapshotPayload
from snapper.core.execution_unit_snapshot import FillSnapshotTarget
from snapper.core.native_fill_unit_admission import NativeFillUnitAdmissionEvidence
from snapper.data.account_bound_submission_admission_store import BoundAccountSubmissionAdmission
from snapper.data.execution_unit_snapshot_history import get_bound_execution_unit_snapshot
from snapper.data.execution_unit_snapshot_store import SnapshotBindingError
from snapper.data.models import Execution
from snapper.data.models import ExecutionUnitSnapshot
from snapper.data.models import VenueEvent
from snapper.data.native_fill_admission_history import bind_native_fill_admission
from snapper.data.native_position_fill_binding_v2 import BoundOriginalPositionFillV2
from snapper.data.native_position_fill_binding_v2 import OriginalPositionFillRequestV2
from snapper.data.native_position_fill_binding_v2 import bind_original_position_fill_v2
from snapper.data.native_position_source_v2 import _verify_fill_units

_INSTANT: TypeAdapter[datetime] = TypeAdapter(EvidenceInstant)


@dataclass(frozen=True, slots=True)
class BoundNativeFillUnitAdmission:
    """Actual original source and complete historical target/admission graph without a current grant."""

    source: BoundOriginalPositionFillV2
    unit: ExecutionUnitSnapshotPayload
    execution: Execution
    event: VenueEvent
    admission: BoundAccountSubmissionAdmission
    evidence: NativeFillUnitAdmissionEvidence


def _require(condition: bool, reason: str) -> None:
    """Refuse any missing or contradictory retained edge rather than selecting a replacement."""
    if not condition:
        raise SnapshotBindingError(reason)


async def _original_unit(
    session: AsyncSession, evidence: NativeFillUnitAdmissionEvidence, known_at: datetime
) -> ExecutionUnitSnapshotPayload:
    """Rebind the full original unit and every indexed reference before inspecting its target."""
    reference = evidence.unit_snapshot
    unit = await get_bound_execution_unit_snapshot(session, reference.public_id)
    row = (
        await session.execute(
            select(ExecutionUnitSnapshot).where(
                ExecutionUnitSnapshot.public_id == reference.public_id
            )
        )
    ).scalar_one()
    _require(
        (row.public_id, row.content_digest, row.timestamp)
        == (reference.public_id, reference.content_digest, reference.recorded_at),
        "native_fill_unit_original_record_reference_mismatch",
    )
    _require(row.timestamp <= known_at, "native_fill_unit_record_not_known_at_posting")
    return unit


async def _original_targets(
    session: AsyncSession, unit: ExecutionUnitSnapshotPayload
) -> tuple[Execution, VenueEvent]:
    """Load the already verified immutable fill target identities and their actual execution clock."""
    target = unit.target
    if not isinstance(target, FillSnapshotTarget):
        raise SnapshotBindingError("native_fill_unit_fill_target_required")
    execution = (
        await session.execute(
            select(Execution).where(Execution.public_id == target.execution_public_id)
        )
    ).scalar_one()
    event = (
        await session.execute(
            select(VenueEvent).where(VenueEvent.public_id == target.venue_event_public_id)
        )
    ).scalar_one()
    return execution, event


async def bind_native_fill_unit_admission(
    session: AsyncSession,
    original_fill: OriginalPositionFillRequestV2,
    evidence: NativeFillUnitAdmissionEvidence,
    known_at: datetime,
) -> BoundNativeFillUnitAdmission:
    """Rebind the durable native admission edge without substituting current account or catalog state.

    Args:
        session: Actual caller-owned historical or wallet-fenced financial transaction.
        original_fill: Original retained raw, tagged authority and authenticated ownership references.
        evidence: Exact unit and original account-admission references from the new financial envelope.
        known_at: Actual later posting or historical interpretation clock, independent of venue execution.

    Returns:
        Independently rebound source, target and original admission; no spending or current owner grant.
    """
    actual = NativeFillUnitAdmissionEvidence.model_validate(evidence)
    clock = _INSTANT.validate_python(known_at)
    unit = await _original_unit(session, actual, clock)
    execution, event = await _original_targets(session, unit)
    source = await bind_original_position_fill_v2(session, original_fill, execution.timestamp)
    _verify_fill_units(source, unit)
    _require(
        execution.exec_id == source.fill.execution_id
        and execution.executed_at == source.fill.occurred_at
        and event.exchange_order_id == source.fill.order_id
        and event.venue_timestamp == source.fill.occurred_at,
        "native_fill_unit_original_source_identity_mismatch",
    )
    _require(
        source.fill.client_order_id is None or event.venue_client_id == source.fill.client_order_id,
        "native_fill_unit_original_client_order_mismatch",
    )
    admission = await bind_native_fill_admission(
        session, source, actual.submission_admission, execution, event
    )
    return BoundNativeFillUnitAdmission(source, unit, execution, event, admission, actual)
