"""Create unchanged fill-unit evidence from original native ownership and actual admitted intent.

The separate returned admission envelope must be retained by the atomic native
financial writer. This module neither commits nor creates an Execution, venue
event, source ticket, admission, reservation or physical order permission.
"""

from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.core.execution_unit_snapshot import EvidenceUUID
from snapper.core.execution_unit_snapshot import ExecutionUnitSnapshotPayload
from snapper.core.execution_unit_snapshot import FillSnapshotTarget
from snapper.core.execution_unit_snapshot import SnapshotDimensions
from snapper.core.execution_unit_snapshot import SnapshotReceipt
from snapper.core.execution_unit_snapshot import SnapshotScope
from snapper.core.execution_unit_snapshot import SnapshotVenueIdentity
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance
from snapper.core.native_fill_unit_admission import NativeFillUnitAdmissionEvidence
from snapper.core.native_fill_unit_admission import NativeUnitRecordReference
from snapper.data.execution_unit_catalog import catalog_economic_context
from snapper.data.execution_unit_catalog import catalog_rebase_context
from snapper.data.execution_unit_catalog import snapshot_catalog
from snapper.data.execution_unit_snapshot_history import get_bound_execution_unit_snapshot
from snapper.data.execution_unit_snapshot_store import SnapshotBindingError
from snapper.data.execution_unit_snapshot_store import SnapshotWriteResult
from snapper.data.execution_unit_snapshot_store import _insert_bound_snapshot
from snapper.data.execution_unit_snapshot_store import bound_execution_digest
from snapper.data.execution_unit_snapshot_store import bound_venue_event_digest
from snapper.data.models import Execution
from snapper.data.models import ExecutionUnitSnapshot
from snapper.data.models import VenueEvent
from snapper.data.native_fill_admission_history import bind_native_fill_admission
from snapper.data.native_position_fill_binding_v2 import BoundOriginalPositionFillV2
from snapper.data.native_position_fill_binding_v2 import OriginalPositionFillRequestV2
from snapper.data.native_position_fill_binding_v2 import bind_original_position_fill_v2


class NativeFillUnitCaptureRequestV2(OriginalPositionFillRequestV2):
    """Exact original source, actual existing targets and explicit original account-admission record."""

    execution_public_id: EvidenceUUID
    venue_event_public_id: EvidenceUUID
    submission_admission: NativeUnitRecordReference


@dataclass(frozen=True, slots=True)
class CapturedNativeFillUnitsV2:
    """Actual source and unit result plus the separate edge required in the new financial envelope."""

    source: BoundOriginalPositionFillV2
    payload: ExecutionUnitSnapshotPayload
    result: SnapshotWriteResult
    evidence: NativeFillUnitAdmissionEvidence


def _require(condition: bool, reason: str) -> None:
    """Refuse a contradictory original target without fabricating missing execution or source rows."""
    if not condition:
        raise SnapshotBindingError(reason)


async def _actual_targets(
    session: AsyncSession, request: NativeFillUnitCaptureRequestV2
) -> tuple[Execution, VenueEvent]:
    """Load both actual immutable target identities before binding any original source or unit."""
    execution = (
        await session.execute(
            select(Execution).where(Execution.public_id == request.execution_public_id)
        )
    ).scalar_one_or_none()
    event = (
        await session.execute(
            select(VenueEvent).where(VenueEvent.public_id == request.venue_event_public_id)
        )
    ).scalar_one_or_none()
    if execution is None or event is None:
        raise SnapshotBindingError("native_fill_actual_unit_targets_missing")
    return execution, event


def native_fill_receipt_v2(source: BoundOriginalPositionFillV2) -> SnapshotReceipt:
    """Keep the actual entire frame and original historical contract quantity dimension.

    Args:
        source: Independently rebound original private fill and actual historical catalog.

    Returns:
        The unchanged v1 venue receipt shape without an invented account or admission field.
    """
    context = catalog_economic_context(source.catalog)
    return SnapshotReceipt(
        representation="wire_bytes_base64",
        material=source.raw.material,
        endpoint_or_channel=source.raw.channel,
        observed_at=source.raw.observed_at,
        reported_quantity_unit=context.native_order_quantity_unit,
        reported_quantity_convention="not_applicable",
    )


def _dimensions(source: BoundOriginalPositionFillV2) -> SnapshotDimensions:
    """Preserve exact native operands and negative sell direction without any principal cash fold."""
    context = catalog_economic_context(source.catalog)
    fill = source.fill
    return SnapshotDimensions.model_validate(
        {
            **context.model_dump(exclude={"quantity_convention", "instrument_kind", "expiry_at"}),
            "signed_quantity_unit": context.native_order_quantity_unit,
            "side": fill.side,
            "signed_quantity": fill.quantity if fill.side == "buy" else fill.quantity.copy_negate(),
            "price": fill.price,
            "fee": fill.fee,
            "fee_currency": fill.fee_currency,
        }
    )


def _verify_original_targets(
    source: BoundOriginalPositionFillV2, execution: Execution, event: VenueEvent
) -> None:
    """Require this exact source child rather than a cumulative execution gap or another venue fill."""
    fill = source.fill
    _require(
        execution.exec_id == fill.execution_id
        and execution.executed_at == fill.occurred_at
        and event.exchange_order_id == fill.order_id
        and event.venue_timestamp == fill.occurred_at,
        "native_fill_actual_target_source_identity_mismatch",
    )
    _require(
        fill.client_order_id is None or event.venue_client_id == fill.client_order_id,
        "native_fill_actual_client_order_identity_mismatch",
    )
    _require(
        execution.price_decimal is not None
        and execution.size_decimal is not None
        and execution.fee_decimal is not None,
        "native_fill_actual_raw_decimal_operands_missing",
    )


def _payload(
    source: BoundOriginalPositionFillV2,
    execution: Execution,
    event: VenueEvent,
    provenance: SnapshotWriteProvenance,
) -> ExecutionUnitSnapshotPayload:
    """Compose unchanged authority-neutral units from actual source and durable target identities."""
    return ExecutionUnitSnapshotPayload(
        target=FillSnapshotTarget(
            execution_public_id=execution.public_id,
            execution_digest=bound_execution_digest(execution),
            venue_event_public_id=event.public_id,
            venue_event_digest=bound_venue_event_digest(event),
        ),
        scope=SnapshotScope(
            wallet_public_id=source.raw.scope.wallet_public_id,
            exchange="kraken_futures",
            mode=source.raw.scope.mode,
            instrument_public_id=source.catalog.instrument.public_id,
        ),
        catalog=snapshot_catalog(source.catalog),
        dimensions=_dimensions(source),
        venue_identity=SnapshotVenueIdentity(
            native_symbol=source.catalog.symbol.native_symbol,
            source="kraken.futures.websocket.fills",
            source_version="1",
            observed_at=source.raw.observed_at,
            wsname=source.fill.provider_symbol,
        ),
        rebase=catalog_rebase_context(source.catalog),
        receipt=native_fill_receipt_v2(source),
        interpreted_at=provenance.timestamp,
    )


async def capture_bound_native_fill_units_v2(
    session: AsyncSession,
    request: NativeFillUnitCaptureRequestV2,
    provenance: SnapshotWriteProvenance,
) -> CapturedNativeFillUnitsV2:
    """Stage actual fill units and return the admission edge for atomic native financial retention.

    Args:
        session: Actual caller-owned wallet-fenced transaction; this producer never commits.
        request: Original authenticated source, actual target rows and exact retained v3 admission.
        provenance: Stable local snapshot insertion identity and original interpretation clock.

    Returns:
        Actual source, unchanged v1 unit payload/result and separate durable-envelope evidence.
    """
    if not session.in_transaction():
        raise RuntimeError("Native fill units require the caller wallet-fenced transaction")
    actual = NativeFillUnitCaptureRequestV2.model_validate(request)
    stamp = SnapshotWriteProvenance.model_validate(provenance)
    execution, event = await _actual_targets(session, actual)
    original = OriginalPositionFillRequestV2.model_validate(
        actual.model_dump(
            exclude={"execution_public_id", "venue_event_public_id", "submission_admission"}
        )
    )
    source = await bind_original_position_fill_v2(session, original, execution.timestamp)
    _verify_original_targets(source, execution, event)
    admission = await bind_native_fill_admission(
        session, source, actual.submission_admission, execution, event
    )
    payload = _payload(source, execution, event, stamp)
    result = await _insert_bound_snapshot(session, payload, stamp)
    retained = await get_bound_execution_unit_snapshot(session, result.public_id)
    _require(retained == payload, "native_fill_unit_retained_history_changed")
    unit_recorded_at = (
        await session.execute(
            select(ExecutionUnitSnapshot.timestamp).where(
                ExecutionUnitSnapshot.public_id == result.public_id
            )
        )
    ).scalar_one()
    evidence = NativeFillUnitAdmissionEvidence(
        submission_admission=NativeUnitRecordReference(
            public_id=admission.public_id,
            content_digest=admission.content_digest,
            recorded_at=admission.recorded_at,
        ),
        unit_snapshot=NativeUnitRecordReference(
            public_id=result.public_id,
            content_digest=result.content_digest,
            recorded_at=unit_recorded_at,
        ),
    )
    return CapturedNativeFillUnitsV2(source, payload, result, evidence)
