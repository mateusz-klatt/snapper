"""Read immutable unit evidence against its real historical target and catalog lineage.

This adapter supplies financial interpreters with validated historical evidence.
It grants no current submission authority and does not prove a raw frame selector
or account/environment identity absent from the original v1 snapshot schema.
"""

from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.core.execution_unit_snapshot import ExecutionUnitSnapshotPayload
from snapper.core.execution_unit_snapshot import FillSnapshotTarget
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance
from snapper.core.execution_unit_snapshot import SubmissionSnapshotTarget
from snapper.core.execution_unit_snapshot import canonical_snapshot_payload
from snapper.core.execution_unit_snapshot import snapshot_content_digest
from snapper.data.execution_unit_snapshot_store import SnapshotBindingError
from snapper.data.execution_unit_snapshot_store import _bind_fill
from snapper.data.execution_unit_snapshot_store import _bind_submission
from snapper.data.execution_unit_snapshot_store import _require
from snapper.data.execution_unit_snapshot_store import _verify_catalog
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import ExecutionUnitSnapshot


def _verify_historical_projection(
    row: ExecutionUnitSnapshot, payload: ExecutionUnitSnapshotPayload
) -> None:
    """Check every immutable indexed copy against the original canonical v1 material."""
    target = payload.target
    actual = (
        row.phase,
        row.wallet_public_id,
        row.exchange,
        row.mode,
        row.instrument_public_id,
        row.command_public_id,
        row.execution_public_id,
        row.spec_public_id,
        row.spec_timestamp,
        row.spec_source,
        row.spec_version,
        row.content_digest,
        row.canonical_payload,
        row.known_to,
    )
    expected = (
        target.phase,
        payload.scope.wallet_public_id,
        payload.scope.exchange,
        payload.scope.mode,
        payload.scope.instrument_public_id,
        target.command_public_id if isinstance(target, SubmissionSnapshotTarget) else None,
        target.execution_public_id if isinstance(target, FillSnapshotTarget) else None,
        payload.catalog.spec_public_id,
        payload.catalog.spec_timestamp,
        payload.catalog.source,
        payload.catalog.version,
        snapshot_content_digest(payload),
        canonical_snapshot_payload(payload),
        KNOWN_TO_MAX,
    )
    _require(actual == expected, "historical_snapshot_projection_mismatch")


async def get_bound_execution_unit_snapshot(
    session: AsyncSession, public_id: str
) -> ExecutionUnitSnapshotPayload:
    """Validate original snapshot content and actual historical durable lineage.

    Args:
        session: Caller-owned accounting or replay transaction; no new fence is acquired.
        public_id: Exact immutable snapshot UUID selected by the durable financial event.

    Returns:
        Original v1 evidence with its historical execution or command and catalog proven.

    Raises:
        SnapshotBindingError: If indexed content or original durable lineage is unavailable.
    """
    row = (
        await session.execute(
            select(ExecutionUnitSnapshot).where(ExecutionUnitSnapshot.public_id == public_id)
        )
    ).scalar_one_or_none()
    if row is None:
        raise SnapshotBindingError("historical_snapshot_missing")
    try:
        payload = ExecutionUnitSnapshotPayload.model_validate_json(row.canonical_payload)
    except ValidationError as error:
        raise SnapshotBindingError("historical_snapshot_payload_invalid") from error
    _verify_historical_projection(row, payload)
    stamp = SnapshotWriteProvenance(
        public_id=row.public_id,
        session_id=row.session_id,
        sequence_id=row.sequence_id,
        timestamp=row.timestamp,
    )
    _require(stamp.timestamp >= payload.interpreted_at, "historical_insert_precedes_interpretation")
    target = payload.target
    if isinstance(target, FillSnapshotTarget):
        effective_at = await _bind_fill(session, payload, target)
    else:
        effective_at = await _bind_submission(session, payload, target)
    _require(effective_at <= payload.interpreted_at, "interpretation_precedes_durable_target")
    await _verify_catalog(session, payload, effective_at)
    return payload
