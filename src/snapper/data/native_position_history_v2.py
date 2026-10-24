"""Replay actual native-v2 positions with original execution/unit and later account-log ownership proofs."""

from dataclasses import dataclass
from datetime import datetime

from pydantic import TypeAdapter
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.portfolio.native_financial_payloads_v2 import ContractPositionAnchorV2
from snapper.application.portfolio.native_financial_payloads_v2 import ContractPositionEventV2
from snapper.application.portfolio.native_financial_payloads_v2 import (
    canonical_native_financial_payload_v2,
)
from snapper.application.portfolio.native_financial_payloads_v2 import (
    native_financial_payload_digest_v2,
)
from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.core.execution_route_authority import account_scope_digest
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.execution_unit_snapshot import EvidenceUUID
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance
from snapper.data.accounting_native_source_store_v2 import get_bound_native_source_claim_v2
from snapper.data.models import ContractAccountingAnchor
from snapper.data.models import ContractAccountingEvent
from snapper.data.models import is_open_ended_version
from snapper.data.native_financial_event_family import (
    bind_native_financial_event_family as bind_native_financial_event_material,
)
from snapper.data.native_financial_source_binding_v2 import BoundNativeFinancialSource
from snapper.data.native_financial_source_binding_v2 import NativeFinancialBindingError
from snapper.data.native_financial_source_binding_v2 import require_native_financial_binding
from snapper.data.native_position_fill_binding_v2 import BoundOriginalPositionFillV2
from snapper.data.native_position_source_v2 import bind_native_position_source_v2
from snapper.data.session_context import session_no_autoflush

_INSTANT: TypeAdapter[datetime] = TypeAdapter(EvidenceInstant)
_UUID: TypeAdapter[str] = TypeAdapter(EvidenceUUID)


@dataclass(frozen=True, slots=True)
class BoundNativePositionEventV2:
    """A real position event whose complete original source/unit and partial chain are rebound."""

    row: ContractAccountingEvent
    payload: ContractPositionEventV2
    source: BoundNativeFinancialSource
    fill: BoundOriginalPositionFillV2
    anchor: ContractPositionAnchorV2


def native_position_anchor_columns_v2(
    payload: ContractPositionAnchorV2,
) -> dict[str, object]:
    """Project actual original-fill and source-position seed coordinates into the unchanged0058 table.

    Args:
        payload: Native wrapper with actual source/policy/sidecar references and unchanged position operands.

    Returns:
        Exact immutable anchor mirrors; unknown external execution prefix stays NULL.
    """
    value = payload.operands
    return {
        **value.scope.model_dump(mode="python"),
        "account_scope_digest": account_scope_digest(value.scope),
        "route_authority_public_id": value.route_authority_public_id,
        "source_receipt_public_id": value.source_receipt_public_id,
        "anchor_kind": value.anchor_kind,
        "source_execution_prefix_status": value.source_execution_prefix_status,
        "source_execution_scope_sequence": value.source_execution_scope_sequence,
        "content_digest": native_financial_payload_digest_v2(payload),
        "canonical_payload": canonical_native_financial_payload_v2(payload),
    }


async def _bound_anchor(
    session: AsyncSession, public_id: str, known_at: datetime
) -> ContractPositionAnchorV2:
    """Bind the actual seed and its original source/unit/policy graph at its retained recording clock."""
    row = await session.scalar(
        select(ContractAccountingAnchor).where(ContractAccountingAnchor.public_id == public_id)
    )
    if row is None:
        raise NativeFinancialBindingError("native_position_actual_seed_missing")
    payload = ContractPositionAnchorV2.model_validate_json(row.canonical_payload)
    provenance = SnapshotWriteProvenance.model_validate(
        {
            "public_id": row.public_id,
            "session_id": row.session_id,
            "sequence_id": row.sequence_id,
            "timestamp": row.timestamp,
        }
    )
    require_native_financial_binding(
        (provenance.public_id, provenance.session_id) == (row.public_id, row.session_id)
        and is_open_ended_version(row.known_to)
        and row.timestamp == payload.source.recorded_at
        and row.timestamp <= known_at
        and all(
            getattr(row, name) == value
            for name, value in native_position_anchor_columns_v2(payload).items()
        ),
        "native_position_seed_projection_or_clock_mismatch",
    )
    await bind_native_position_source_v2(session, payload)
    return payload


def _require_conservation(
    events: list[BoundNativePositionEventV2],
    row: ContractAccountingEvent,
    payload: ContractPositionEventV2,
    anchor: ContractPositionAnchorV2,
) -> None:
    """Conserve actual native state and original unit profile while allowing other account facts between fills."""
    value = payload.operands
    seed = anchor.operands
    require_native_financial_binding(
        seed.scope == value.scope
        and seed.instrument_public_id == value.instrument_public_id
        and seed.unit == value.profile_unit
        and anchor.source.authority.venue_account_id == payload.source.authority.venue_account_id,
        "native_position_seed_scope_profile_or_uid_changed",
    )
    if not events:
        require_native_financial_binding(
            seed.opening == value.before
            and seed.source_position == value.source_position
            and seed.source_receipt_public_id == value.position_receipt_public_id
            and anchor.source == payload.source
            and anchor.fill_authority_reference == payload.fill_authority_reference
            and anchor.fill_ownership_reference == payload.fill_ownership_reference,
            "native_position_first_event_seed_mismatch",
        )
        return
    previous = events[-1]
    old = previous.payload.operands
    require_native_financial_binding(
        previous.row.timestamp <= row.timestamp
        and previous.row.ledger_sequence < row.ledger_sequence
        and previous.source.ownership.account_uid == payload.source.authority.venue_account_id
        and old.anchor_public_id == value.anchor_public_id
        and old.observed_after == value.before
        and old.source_position.source_entry.source_log_id
        < value.source_position.source_entry.source_log_id,
        "native_position_retained_chain_discontinuous",
    )


async def load_native_position_history_v2(
    session: AsyncSession,
    scope: ExecutionAccountScope,
    instrument_public_id: str,
    known_at: datetime,
) -> tuple[BoundNativePositionEventV2, ...]:
    """Replay one actual source-instrument position without manufacturing a flat opening or cash.

    Original fill authority and actual authenticated ownership are checked at
    real Execution knowledge. Later native log/policy/ownership is checked at
    each event's actual retained clock. Every seed, original unit, exact native
    source claim and predecessor position is rebound before returning any row.

    Args:
        session: Actual caller-owned historical or financial transaction.
        scope: Exact account wallet/exchange/mode/environment/margin namespace.
        instrument_public_id: Immutable actual contract instrument, without provider-name inference.
        known_at: Actual retained-knowledge cutoff, distinct from provider executed_at.

    Returns:
        The complete selected position prefix; no rows means unknown, not flat or trade readiness.
    """
    actual_scope = ExecutionAccountScope.model_validate(scope)
    instrument = _UUID.validate_python(instrument_public_id)
    clock = _INSTANT.validate_python(known_at)
    with session_no_autoflush(session):
        rows = (
            await session.scalars(
                select(ContractAccountingEvent)
                .where(
                    ContractAccountingEvent.account_scope_digest
                    == account_scope_digest(actual_scope),
                    ContractAccountingEvent.kind == "contract_position_fill",
                    ContractAccountingEvent.instrument_public_id == instrument,
                    ContractAccountingEvent.timestamp <= clock,
                )
                .order_by(ContractAccountingEvent.ledger_sequence)
            )
        ).all()
        result: list[BoundNativePositionEventV2] = []
        for row in rows:
            material = await bind_native_financial_event_material(session, row, clock)
            payload = material.payload
            if not isinstance(payload, ContractPositionEventV2):
                raise NativeFinancialBindingError("native_position_actual_kind_mismatch")
            require_native_financial_binding(
                payload.operands.scope == actual_scope
                and payload.operands.instrument_public_id == instrument,
                "native_position_requested_scope_or_instrument_mismatch",
            )
            bound = await bind_native_position_source_v2(session, payload)
            require_native_financial_binding(
                bound.financial_source == material.source,
                "native_position_rebound_log_changed",
            )
            anchor = await _bound_anchor(session, payload.operands.anchor_public_id, row.timestamp)
            _require_conservation(result, row, payload, anchor)
            await get_bound_native_source_claim_v2(session, row.public_id, row.timestamp)
            result.append(
                BoundNativePositionEventV2(row, payload, material.source, bound.fill, anchor)
            )
    return tuple(result)


async def get_bound_native_position_event_v2(
    session: AsyncSession, public_id: str, known_at: datetime
) -> BoundNativePositionEventV2:
    """Fetch an actual position event only after its entire real scoped instrument prefix rebinds.

    Args:
        session: Actual caller-owned historical or financial transaction.
        public_id: Exact retained position UUID, never an invented journal coordinate.
        known_at: Required actual knowledge cutoff.

    Returns:
        The fully source/unit/policy/anchor/claim-bound event, without current admission.
    """
    identifier = _UUID.validate_python(public_id)
    with session_no_autoflush(session):
        row = await session.scalar(
            select(ContractAccountingEvent).where(ContractAccountingEvent.public_id == identifier)
        )
        if row is None:
            raise NativeFinancialBindingError("native_position_actual_event_missing")
        payload = ContractPositionEventV2.model_validate_json(row.canonical_payload)
        events = await load_native_position_history_v2(
            session,
            payload.operands.scope,
            payload.operands.instrument_public_id,
            known_at,
        )
        found = next((item for item in events if item.row.public_id == identifier), None)
        if found is None:
            raise NativeFinancialBindingError("native_position_event_not_known_at_cutoff")
        return found
