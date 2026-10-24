"""Bind actual native-v2 event rows to their immutable account-log, owner and transport sources.

Complete cash/position readers additionally verify anchors, ledger chains and
actual execution/unit references. This layer is shared with the native source
claim store so neither complete reader imports itself through claim binding.
"""

from dataclasses import dataclass
from datetime import datetime

from pydantic import TypeAdapter
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.portfolio.native_financial_payloads_v2 import FuturesNativeCashEventV2
from snapper.application.portfolio.native_financial_payloads_v2 import NativeFinancialEventV2
from snapper.application.portfolio.native_financial_payloads_v2 import (
    canonical_native_financial_payload_v2,
)
from snapper.application.portfolio.native_financial_payloads_v2 import (
    native_financial_payload_digest_v2,
)
from snapper.core.execution_route_authority import account_scope_digest
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.execution_unit_snapshot import EvidenceUUID
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance
from snapper.data.models import ContractAccountingEvent
from snapper.data.models import is_open_ended_version
from snapper.data.native_financial_source_binding_v2 import BoundNativeFinancialSource
from snapper.data.native_financial_source_binding_v2 import NativeFinancialBindingError
from snapper.data.native_financial_source_binding_v2 import bind_native_financial_source
from snapper.data.native_financial_source_binding_v2 import require_native_financial_binding
from snapper.data.session_context import session_no_autoflush

_EVENT: TypeAdapter[NativeFinancialEventV2] = TypeAdapter(NativeFinancialEventV2)
_INSTANT: TypeAdapter[datetime] = TypeAdapter(EvidenceInstant)
_UUID: TypeAdapter[str] = TypeAdapter(EvidenceUUID)


@dataclass(frozen=True, slots=True)
class NativeFinancialEventMaterialV2:
    """An actual session-owned row with its exact financial shape and rebound original sources."""

    row: ContractAccountingEvent
    payload: NativeFinancialEventV2
    source: BoundNativeFinancialSource


def native_event_columns_v2(payload: NativeFinancialEventV2) -> dict[str, object]:
    """Derive every indexed event mirror from its complete native-v2 canonical payload.

    Args:
        payload: Strict native wrapper containing its unchanged arithmetic/source operands.

    Returns:
        Exact immutable projection, without a caller-supplied amount or scope mirror.
    """
    value = payload.operands
    if isinstance(payload, FuturesNativeCashEventV2):
        targets: tuple[str, str | None, str | None, str | None] = (
            payload.operands.source_identity,
            None,
            None,
            None,
        )
    else:
        targets = (
            f"kraken_futures:fill:{payload.operands.venue_execution_id}",
            payload.operands.instrument_public_id,
            payload.operands.execution_public_id,
            payload.operands.unit_snapshot_public_id,
        )
    return {
        **value.scope.model_dump(mode="python"),
        "account_scope_digest": account_scope_digest(value.scope),
        "route_authority_public_id": value.route_authority_public_id,
        "anchor_public_id": value.anchor_public_id,
        "raw_receipt_public_id": value.raw_receipt_public_id,
        "ledger_sequence": value.ledger_sequence,
        "kind": payload.kind,
        "source_identity": targets[0],
        "instrument_public_id": targets[1],
        "execution_public_id": targets[2],
        "unit_snapshot_public_id": targets[3],
        "content_digest": native_financial_payload_digest_v2(payload),
        "canonical_payload": canonical_native_financial_payload_v2(payload),
    }


def _require_actual_event(
    session: AsyncSession,
    row: ContractAccountingEvent,
    payload: NativeFinancialEventV2,
    known_at: datetime,
) -> None:
    """Require the original retained clock or the actual pending row in this same transaction."""
    pending = row in session.new
    provenance = SnapshotWriteProvenance.model_validate(
        {
            "public_id": row.public_id,
            "session_id": row.session_id,
            "sequence_id": row.sequence_id,
            "timestamp": row.timestamp,
        }
    )
    require_native_financial_binding(
        (provenance.public_id, provenance.session_id) == (row.public_id, row.session_id),
        "native_financial_event_identity_noncanonical",
    )
    require_native_financial_binding(
        row in session
        and row.timestamp == payload.source.recorded_at
        and row.timestamp <= known_at
        and (
            row.known_to is None or is_open_ended_version(row.known_to)
            if pending
            else is_open_ended_version(row.known_to)
        ),
        "native_financial_event_retained_provenance_mismatch",
    )
    require_native_financial_binding(
        all(
            getattr(row, name) == value for name, value in native_event_columns_v2(payload).items()
        ),
        "native_financial_event_projection_mismatch",
    )


async def bind_native_financial_event_material(
    session: AsyncSession, row: ContractAccountingEvent, known_at: datetime
) -> NativeFinancialEventMaterialV2:
    """Bind one actual pending or retained event and its exact original account-log source graph.

    Complete readers must still check actual anchors, chain conservation and
    execution/unit references. A pending row is accepted only from this actual
    session; canonical shape alone cannot introduce an invented retained event.

    Args:
        session: Actual caller-owned historical or financial transaction.
        row: Actual session-owned row, newly pending only for its immediate atomic claim writer.
        known_at: Requested knowledge cutoff, never a replacement for the row's original timestamp.

    Returns:
        Exact row/payload/source material for exclusive claim and complete ledger readers.
    """
    clock = _INSTANT.validate_python(known_at)
    payload = _EVENT.validate_json(row.canonical_payload)
    _require_actual_event(session, row, payload, clock)
    source = await bind_native_financial_source(session, payload.source)
    entry = (
        payload.operands.source_entry
        if isinstance(payload, FuturesNativeCashEventV2)
        else payload.operands.source_position.source_entry
    )
    require_native_financial_binding(
        entry == source.entry,
        "native_financial_event_original_entry_mismatch",
    )
    return NativeFinancialEventMaterialV2(row, payload, source)


async def get_native_financial_event_material(
    session: AsyncSession, public_id: str, known_at: datetime
) -> NativeFinancialEventMaterialV2:
    """Fetch the actual immutable event before binding its native original source material.

    Args:
        session: Actual caller-owned read or financial transaction.
        public_id: Exact existing financial event identifier.
        known_at: Knowledge cutoff applied independently of embedded observed clocks.

    Returns:
        Actual source material; full cash/position readers add their chain/unit verification.
    """
    identifier = _UUID.validate_python(public_id)
    with session_no_autoflush(session):
        row = await session.scalar(
            select(ContractAccountingEvent).where(ContractAccountingEvent.public_id == identifier)
        )
        if row is None:
            raise NativeFinancialBindingError("native_financial_actual_event_missing")
        return await bind_native_financial_event_material(session, row, known_at)
