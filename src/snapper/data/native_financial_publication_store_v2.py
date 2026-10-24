"""Bind native position notifications to genuine source-bound facts in unchanged0059 tables.

No cash-only row is accepted: each intent has an actual Execution, VenueEvent,
unit witness, durable original admission and position event. The event digest
is the full retained outer-v3 digest, never a rehash of normalized inner-v2
operands. Historical v1 notification readers and bytes stay unchanged.
"""

from dataclasses import dataclass

from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.core.execution_route_authority import account_scope_digest
from snapper.core.execution_unit_snapshot import EvidenceUUID
from snapper.core.execution_unit_snapshot import FillSnapshotTarget
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance
from snapper.core.native_financial_publication_v2 import NativeContractPositionPublicationV2
from snapper.core.native_financial_publication_v2 import canonical_native_position_publication_v2
from snapper.core.native_financial_publication_v2 import native_position_publication_digest_v2
from snapper.data.models import FinancialPublicationIntent
from snapper.data.models import is_open_ended_version
from snapper.data.native_admitted_position_guards import require_native_admitted_position_guards
from snapper.data.native_financial_source_binding_v2 import NativeFinancialBindingError
from snapper.data.native_financial_source_binding_v2 import require_native_financial_binding
from snapper.data.native_position_family_history import get_bound_native_position_family
from snapper.data.native_position_history_v2 import BoundNativePositionEventV2


@dataclass(frozen=True, slots=True)
class BoundNativePositionPublicationV2:
    """An actual immutable notification, publishable only after its transaction commits."""

    public_id: str
    topic: str
    payload: NativeContractPositionPublicationV2
    content_digest: str
    created: bool = False


def native_position_publication_topic_v2(
    payload: NativeContractPositionPublicationV2,
) -> str:
    """Route actual native positions outside legacy executable order/fill namespaces.

    Args:
        payload: Exact native scope and actual immutable instrument identity.

    Returns:
        A dedicated versioned topic without monetary deltas or routing aliases.
    """
    scope = payload.scope
    return f"accounting.native_position.v2.{scope.exchange}.{scope.environment}.{scope.wallet_public_id}.{payload.instrument_public_id}"


def _payload(
    bound: BoundNativePositionEventV2, provenance: SnapshotWriteProvenance
) -> NativeContractPositionPublicationV2:
    """Derive every notification operand from the verified original position graph."""
    position = bound.payload
    value = position.operands
    source = position.source
    target = value.unit.target
    if not isinstance(target, FillSnapshotTarget):
        raise NativeFinancialBindingError("native_publication_actual_fill_target_required")
    return NativeContractPositionPublicationV2(
        scope=value.scope,
        source_execution_id=value.venue_execution_id,
        execution_public_id=value.execution_public_id,
        venue_event_public_id=target.venue_event_public_id,
        unit_snapshot_public_id=value.unit_snapshot_public_id,
        position_event_public_id=bound.row.public_id,
        position_event_digest=bound.row.content_digest,
        instrument_public_id=value.instrument_public_id,
        fill_authority_reference=position.fill_authority_reference,
        fill_ownership_reference=position.fill_ownership_reference,
        position_authority_reference=source.authority_reference,
        position_ownership_reference=source.ownership_reference,
        owner_policy_reference=source.owner_evidence.policy_reference,
        ledger_sequence=value.ledger_sequence,
        executed_at=value.executed_at,
        execution_known_at=value.execution_known_at,
        position_known_at=bound.row.timestamp,
        owner_decided_at=source.owner_evidence.decided_at,
        recorded_at=provenance.timestamp,
    )


def _columns(payload: NativeContractPositionPublicationV2) -> dict[str, object]:
    """Derive all immutable0059 routing, unique-key and canonical projections."""
    return {
        **payload.scope.model_dump(mode="python"),
        "account_scope_digest": account_scope_digest(payload.scope),
        "source_execution_id": payload.source_execution_id,
        "execution_public_id": payload.execution_public_id,
        "position_event_public_id": payload.position_event_public_id,
        "topic": native_position_publication_topic_v2(payload),
        "content_digest": native_position_publication_digest_v2(payload),
        "canonical_payload": canonical_native_position_publication_v2(payload),
    }


async def _bound(
    session: AsyncSession, row: FinancialPublicationIntent
) -> BoundNativePositionPublicationV2:
    """Rebind the full actual original source graph and every copied row/header before use."""
    payload = NativeContractPositionPublicationV2.model_validate_json(row.canonical_payload)
    provenance = SnapshotWriteProvenance.model_validate(
        {
            "public_id": row.public_id,
            "session_id": row.session_id,
            "sequence_id": row.sequence_id,
            "timestamp": row.timestamp,
        }
    )
    require_native_financial_binding(
        is_open_ended_version(row.known_to)
        and row.timestamp == payload.recorded_at
        and all(getattr(row, name) == value for name, value in _columns(payload).items()),
        "native_publication_projection_or_lifetime_mismatch",
    )
    actual = await get_bound_native_position_family(
        session, row.position_event_public_id, row.timestamp
    )
    require_native_financial_binding(
        _payload(actual, provenance) == payload,
        "native_publication_actual_position_mismatch",
    )
    return BoundNativePositionPublicationV2(row.public_id, row.topic, payload, row.content_digest)


async def get_bound_native_position_publication_v2(
    session: AsyncSession, public_id: EvidenceUUID
) -> BoundNativePositionPublicationV2:
    """Read an actual native intent with full original source, claim, unit and owner history.

    Args:
        session: Actual caller-owned historical or financial transaction.
        public_id: Exact immutable notification UUID; no family discovery fallback is performed.

    Returns:
        The actual source-bound native notification, without current trading permission.
    """
    row = await session.scalar(
        select(FinancialPublicationIntent).where(FinancialPublicationIntent.public_id == public_id)
    )
    if row is None:
        raise NativeFinancialBindingError("native_publication_actual_intent_missing")
    return await _bound(session, row)


async def _existing(
    session: AsyncSession, payload: NativeContractPositionPublicationV2
) -> BoundNativePositionPublicationV2 | None:
    """Share original physical uniqueness across versions instead of allocating a new economic fact."""
    rows = (
        await session.scalars(
            select(FinancialPublicationIntent).where(
                or_(
                    FinancialPublicationIntent.execution_public_id == payload.execution_public_id,
                    FinancialPublicationIntent.position_event_public_id
                    == payload.position_event_public_id,
                    (
                        FinancialPublicationIntent.account_scope_digest
                        == account_scope_digest(payload.scope)
                    )
                    & (
                        FinancialPublicationIntent.source_execution_id
                        == payload.source_execution_id
                    ),
                )
            )
        )
    ).all()
    if not rows:
        return None
    require_native_financial_binding(len(rows) == 1, "native_publication_identity_conflict")
    original = await _bound(session, rows[0])
    expected = NativeContractPositionPublicationV2.model_validate(
        {
            **payload.model_dump(mode="python"),
            "recorded_at": original.payload.recorded_at,
        }
    )
    require_native_financial_binding(
        original.payload == expected, "native_publication_source_conflict"
    )
    return original


async def append_bound_native_position_publication_v2(
    session: AsyncSession,
    position_event_public_id: EvidenceUUID,
    provenance: SnapshotWriteProvenance,
) -> BoundNativePositionPublicationV2:
    """Flush a source-bound native intent in the same caller-fenced financial transaction.

    Args:
        session: Actual caller transaction already holding the financial wallet fence.
        position_event_public_id: Actual flushed native position event, including its source claim.
        provenance: Stable notification identity and real current retained knowledge clock.

    Returns:
        The original or new actual notification; publishing must follow successful outer commit.
    """
    if not session.in_transaction():
        raise RuntimeError("native_publication_requires_caller_transaction")
    await require_native_admitted_position_guards(session)
    stamp = SnapshotWriteProvenance.model_validate(provenance)
    bound = await get_bound_native_position_family(
        session, position_event_public_id, stamp.timestamp
    )
    payload = _payload(bound, stamp)
    original = await _existing(session, payload)
    if original is not None:
        return original
    row = FinancialPublicationIntent(**stamp.model_dump(mode="python"), **_columns(payload))
    session.add(row)
    await session.flush()
    return BoundNativePositionPublicationV2(
        row.public_id, row.topic, payload, row.content_digest, True
    )
