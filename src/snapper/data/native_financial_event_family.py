"""Bind explicit native event versions while preserving exact inner-v2 financial source identity.

Version-three position records retain and independently rebind their actual
original unit/admission graph. The normalized material is only returned after
that verification; its inner operands do not replace the full outer row hash.
Legacy native-v2 codecs, canonical bytes and readers remain unchanged.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import TypeAdapter
from pydantic import field_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.portfolio.native_position_admission_v3 import AdmittedNativePositionEventV3
from snapper.application.portfolio.native_position_admission_v3 import (
    admitted_native_position_digest_v3,
)
from snapper.application.portfolio.native_position_admission_v3 import (
    canonical_admitted_native_position_v3,
)
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.execution_unit_snapshot import EvidenceUUID
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance
from snapper.data.models import ContractAccountingEvent
from snapper.data.models import is_open_ended_version
from snapper.data.native_admitted_position_guards import require_native_admitted_position_guards
from snapper.data.native_financial_event_binding_v2 import NativeFinancialEventMaterialV2
from snapper.data.native_financial_event_binding_v2 import bind_native_financial_event_material
from snapper.data.native_financial_event_binding_v2 import native_event_columns_v2
from snapper.data.native_financial_source_binding_v2 import NativeFinancialBindingError
from snapper.data.native_financial_source_binding_v2 import bind_native_financial_source
from snapper.data.native_financial_source_binding_v2 import require_native_financial_binding
from snapper.data.native_position_admission_binding_v3 import BoundAdmittedNativePositionSourceV3
from snapper.data.native_position_admission_binding_v3 import NativePositionSourceRequestV3
from snapper.data.native_position_admission_binding_v3 import (
    bind_admitted_native_position_source_v3,
)
from snapper.data.native_position_fill_binding_v2 import OriginalPositionFillRequestV2
from snapper.data.native_position_source_v2 import _verify_position_source
from snapper.data.session_context import session_no_autoflush

_UUID: TypeAdapter[str] = TypeAdapter(EvidenceUUID)
_INSTANT: TypeAdapter[datetime] = TypeAdapter(EvidenceInstant)


class _NativeEventVersion(BaseModel):
    """Select the actual declared codec without falling back after a failed source proof."""

    model_config = ConfigDict(extra="ignore", strict=True, hide_input_in_errors=True)
    schema_version: Literal[2, 3]

    @field_validator("schema_version", mode="before")
    @classmethod
    def validate_exact_version(cls, value: object) -> object:
        """Refuse string, bool and floating aliases for a financial record version.

        Args:
            value: Original immutable financial discriminator.

        Returns:
            The same exact integer before supported literal validation.
        """
        if type(value) is not int:
            raise ValueError("Native financial version requires an integer")
        return value


@dataclass(frozen=True, slots=True)
class AdmittedNativeEventMaterialV3(NativeFinancialEventMaterialV2):
    """Normalized original operands plus the actual independently rebound durable admission graph."""

    envelope: AdmittedNativePositionEventV3
    admitted: BoundAdmittedNativePositionSourceV3


def native_position_event_columns_v3(payload: AdmittedNativePositionEventV3) -> dict[str, object]:
    """Keep the same actual financial namespace while binding every new admission reference.

    Args:
        payload: Strict outer-v3 position record with unchanged inner-v2 source/arithmetic operands.

    Returns:
        All actual0058 row mirrors, retaining the full outer digest and canonical bytes.
    """
    value = AdmittedNativePositionEventV3.model_validate(payload)
    return {
        **native_event_columns_v2(value.position),
        "content_digest": admitted_native_position_digest_v3(value),
        "canonical_payload": canonical_admitted_native_position_v3(value),
    }


def admitted_position_request_v3(
    payload: AdmittedNativePositionEventV3,
) -> NativePositionSourceRequestV3:
    """Recover only exact retained original references, without choosing current credentials or units.

    Args:
        payload: Stored actual version-three envelope whose row hash is separately verified.

    Returns:
        Historical source request for the actual original fill and independent later log.
    """
    position = payload.position
    value = position.operands
    return NativePositionSourceRequestV3(
        fill=OriginalPositionFillRequestV2(
            wallet_public_id=value.scope.wallet_public_id,
            raw_receipt_public_id=value.raw_receipt_public_id,
            authority_reference=position.fill_authority_reference,
            ownership_reference=position.fill_ownership_reference,
        ),
        unit_snapshot_public_id=value.unit_snapshot_public_id,
        position_receipt_public_id=value.position_receipt_public_id,
        position_authority_reference=position.source.authority_reference,
        fill_unit_admission=payload.fill_unit_admission,
    )


def _verify_v3_row(
    session: AsyncSession,
    row: ContractAccountingEvent,
    payload: AdmittedNativePositionEventV3,
    known_at: datetime,
) -> None:
    """Verify every actual row mirror and retained lifetime before using a new admission edge."""
    stamp = SnapshotWriteProvenance.model_validate(
        {
            "public_id": row.public_id,
            "session_id": row.session_id,
            "sequence_id": row.sequence_id,
            "timestamp": row.timestamp,
        }
    )
    require_native_financial_binding(
        row in session
        and stamp.public_id == row.public_id
        and stamp.session_id == row.session_id
        and row.timestamp == payload.position.source.recorded_at
        and row.timestamp <= known_at
        and (
            row.known_to is None or is_open_ended_version(row.known_to)
            if row in session.new
            else is_open_ended_version(row.known_to)
        )
        and all(
            getattr(row, name) == value
            for name, value in native_position_event_columns_v3(payload).items()
        ),
        "native_admitted_event_projection_or_retained_clock_mismatch",
    )


async def bind_native_financial_event_family(
    session: AsyncSession, row: ContractAccountingEvent, known_at: datetime
) -> NativeFinancialEventMaterialV2:
    """Bind each supported actual financial family through one exact declared source graph.

    Args:
        session: Actual caller-owned historical or wallet-fenced financial transaction.
        row: Actual session-owned financial event, pending only for the immediate atomic claim writer.
        known_at: Actual retained knowledge cutoff, distinct from provider economic time.

    Returns:
        Normalized native operands only after full declared-family source/admission verification.
    """
    clock = _INSTANT.validate_python(known_at)
    version = _NativeEventVersion.model_validate_json(row.canonical_payload)
    if version.schema_version == 2:
        return await bind_native_financial_event_material(session, row, clock)
    await require_native_admitted_position_guards(session)
    payload = AdmittedNativePositionEventV3.model_validate_json(row.canonical_payload)
    _verify_v3_row(session, row, payload, clock)
    source = await bind_native_financial_source(session, payload.position.source)
    admitted = await bind_admitted_native_position_source_v3(
        session, admitted_position_request_v3(payload), row.timestamp
    )
    _verify_position_source(payload.position, admitted.source)
    require_native_financial_binding(
        admitted.source.financial_source == source
        and payload.position.operands.source_position == admitted.source.position
        and payload.position.operands.unit == admitted.original.unit,
        "native_admitted_event_actual_source_or_unit_mismatch",
    )
    return AdmittedNativeEventMaterialV3(row, payload.position, source, payload, admitted)


async def get_native_financial_event_family(
    session: AsyncSession, public_id: str, known_at: datetime
) -> NativeFinancialEventMaterialV2:
    """Read one real event before binding its explicit complete original financial family.

    Args:
        session: Actual caller-owned financial or historical transaction.
        public_id: Exact retained event UUID, without a version preference or discovery fallback.
        known_at: Actual retained knowledge cutoff for this read.

    Returns:
        Full actual source material, with original admission proof for version-three positions.
    """
    identifier = _UUID.validate_python(public_id)
    with session_no_autoflush(session):
        row = await session.scalar(
            select(ContractAccountingEvent).where(ContractAccountingEvent.public_id == identifier)
        )
        if row is None:
            raise NativeFinancialBindingError("native_financial_actual_event_missing")
        return await bind_native_financial_event_family(session, row, known_at)
