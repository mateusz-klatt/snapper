"""Append native positions from actual original fills and later authenticated account-log facts.

The actual Execution and unit witness already exist in the caller's financial
transaction. A position fact consumes one native account-log identity and never
turns modeled PnL, fees or position components into native cash. Current owner
admission and historical replay are distinct; an exact retained retry preserves
its original evidence without renewing financial permission.
"""

from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from decimal import Decimal
from typing import Literal
from typing import cast
from uuid import NAMESPACE_URL
from uuid import uuid5

from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.portfolio.accounting_native_source import native_source_semantic_digest
from snapper.application.portfolio.contract_position_ledger import CURRENT_POSITION_DECIMAL_POLICY
from snapper.application.portfolio.contract_position_ledger import ContractPositionAnchorPayload
from snapper.application.portfolio.contract_position_ledger import ContractPositionEventPayload
from snapper.application.portfolio.contract_position_ledger import ContractPositionValue
from snapper.application.portfolio.contract_position_ledger import fold_position_source
from snapper.application.portfolio.native_financial_payloads_v2 import ContractPositionAnchorV2
from snapper.application.portfolio.native_financial_payloads_v2 import ContractPositionEventV2
from snapper.application.portfolio.native_financial_payloads_v2 import NativeFinancialSourceEvidence
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.core.execution_route_authority import account_scope_digest
from snapper.core.execution_unit_admission import ExecutionRouteReference
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance
from snapper.core.native_account_authority import NativeAccountAuthorityPayload
from snapper.data.accounting_native_source_store_v2 import claim_new_accounting_source_v2
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.contract_position_ledger_store import ContractPositionLedgerWriteResult
from snapper.data.execution_account_authority_store import require_current_account_authority
from snapper.data.financial_account_owner_store import load_current_financial_owner_evidence
from snapper.data.models import ContractAccountingAnchor
from snapper.data.models import ContractAccountingEvent
from snapper.data.native_account_authority_store import NativeAuthorityFreshnessPolicy
from snapper.data.native_contract_accounting_guards import require_native_contract_accounting_guards
from snapper.data.native_financial_event_binding_v2 import get_native_financial_event_material
from snapper.data.native_financial_event_binding_v2 import native_event_columns_v2
from snapper.data.native_financial_source_binding_v2 import require_native_financial_binding
from snapper.data.native_position_history_v2 import BoundNativePositionEventV2
from snapper.data.native_position_history_v2 import load_native_position_history_v2
from snapper.data.native_position_history_v2 import native_position_anchor_columns_v2
from snapper.data.native_position_source_v2 import BoundNativePositionSourceV2
from snapper.data.native_position_source_v2 import NativePositionSourceRequestV2
from snapper.data.native_position_source_v2 import bind_native_position_receipt_v2
from snapper.data.session_context import session_no_autoflush


@dataclass(frozen=True, slots=True)
class _PositionAdmissionV2:
    """Actual original operands and current later-log owner decision within one real transaction."""

    request: NativePositionSourceRequestV2
    source: BoundNativePositionSourceV2
    evidence: NativeFinancialSourceEvidence


async def _current_admission(
    session: AsyncSession,
    request: NativePositionSourceRequestV2,
    source: BoundNativePositionSourceV2,
    provenance: SnapshotWriteProvenance,
    policy: NativeAuthorityFreshnessPolicy,
) -> _PositionAdmissionV2:
    """Admit only a genuinely new fact using current native source and the locked durable owner policy."""
    bound = source.financial_source.authority
    native = NativeAccountAuthorityPayload.model_validate(bound.payload)
    route = ExecutionRouteReference(
        authority_public_id=bound.public_id,
        authority_content_digest=bound.content_digest,
        scope=native.scope,
        route_revision=native.route_revision,
    )
    current = await require_current_account_authority(
        session, route, provenance.timestamp, native_policy=policy
    )
    require_native_financial_binding(
        current == bound, "native_position_current_authority_reference_changed"
    )
    owner = await load_current_financial_owner_evidence(
        session, request.position_authority_reference, provenance.timestamp
    )
    evidence = NativeFinancialSourceEvidence(
        recorded_at=provenance.timestamp,
        authority_reference=request.position_authority_reference,
        authority=native,
        owner_evidence=owner,
        ownership_reference=source.financial_source.ownership.reference,
    )
    return _PositionAdmissionV2(request, source, evidence)


async def _require_account_tip(
    session: AsyncSession, admission: _PositionAdmissionV2, known_at: datetime
) -> None:
    """Conserve actual account identity and posting chronology across cash and position kinds."""
    scope = admission.evidence.authority.scope
    row = await session.scalar(
        select(ContractAccountingEvent)
        .where(ContractAccountingEvent.account_scope_digest == account_scope_digest(scope))
        .order_by(ContractAccountingEvent.ledger_sequence.desc())
        .limit(1)
    )
    if row is None:
        return
    material = await get_native_financial_event_material(session, row.public_id, known_at)
    require_native_financial_binding(
        material.payload.operands.scope == scope
        and material.payload.source.authority.venue_account_id
        == admission.evidence.authority.venue_account_id,
        "native_position_account_tip_changed",
    )


def _result(
    row: ContractAccountingEvent, payload: ContractPositionEventV2, *, created: bool
) -> ContractPositionLedgerWriteResult:
    """Expose actual immutable coordinates and position arithmetic without cash or spending permission."""
    value = payload.operands
    return ContractPositionLedgerWriteResult(
        public_id=row.public_id,
        anchor_public_id=value.anchor_public_id,
        ledger_sequence=value.ledger_sequence,
        execution_public_id=value.execution_public_id,
        unit_snapshot_public_id=value.unit_snapshot_public_id,
        instrument_public_id=value.instrument_public_id,
        quantity_unit="contract_count",
        settlement_currency=cast(str, value.unit.dimensions.settlement_currency),
        quote_currency=value.unit.dimensions.quote_currency,
        observed=value.observed_after,
        derived=value.derived_after,
        basis_status=value.basis_status,
        created=created,
    )


def _require_same_source(
    existing: BoundNativePositionEventV2,
    request: NativePositionSourceRequestV2,
    source: BoundNativePositionSourceV2,
) -> None:
    """Permit log snapshot relocation while rejecting changed economic or original execution evidence."""
    payload = existing.payload
    value = payload.operands
    require_native_financial_binding(
        payload.fill_authority_reference == request.fill.authority_reference
        and payload.fill_ownership_reference == request.fill.ownership_reference
        and value.unit_snapshot_public_id == request.unit_snapshot_public_id
        and value.execution_public_id == source.execution.public_id
        and value.unit == source.unit
        and existing.source.ownership.account_uid == source.financial_source.ownership.account_uid
        and native_source_semantic_digest(value.source_position.source_entry)
        == native_source_semantic_digest(source.position.source_entry),
        "native_position_source_identity_conflict",
    )


async def _stage_anchor(
    session: AsyncSession,
    admission: _PositionAdmissionV2,
    provenance: SnapshotWriteProvenance,
) -> tuple[str, ContractPositionAnchorV2]:
    """Seed an explicitly partial position from the actual source old balance without invented fills."""
    request, source, evidence = admission.request, admission.source, admission.evidence
    scope = evidence.authority.scope
    identifier = str(
        uuid5(
            NAMESPACE_URL,
            f"snapper:position-anchor:v2:{account_scope_digest(scope)}:{source.unit.scope.instrument_public_id}",
        )
    )
    occupied = await session.scalar(
        select(ContractAccountingAnchor.public_id).where(
            ContractAccountingAnchor.public_id == identifier
        )
    )
    require_native_financial_binding(occupied is None, "native_position_unpaired_anchor_present")
    operands = ContractPositionAnchorPayload(
        scope=scope,
        instrument_public_id=source.unit.scope.instrument_public_id,
        route_authority_public_id=request.fill.authority_reference.public_id,
        position_route_authority_public_id=request.position_authority_reference.public_id,
        source_receipt_public_id=request.position_receipt_public_id,
        unit_snapshot_public_id=request.unit_snapshot_public_id,
        source_alias_public_id=source.fill.alias.public_id,
        source_alias_timestamp=source.fill.alias.timestamp,
        unit=source.unit,
        source_position=source.position,
        opening=ContractPositionValue(
            quantity=source.position.quantity_before,
            entry_price=source.position.entry_price_before,
            realized_pnl_since_anchor=Decimal(0),
            quote_turnover_since_anchor=Decimal(0),
        ),
    )
    payload = ContractPositionAnchorV2(
        source=evidence,
        fill_authority_reference=request.fill.authority_reference,
        fill_ownership_reference=request.fill.ownership_reference,
        operands=operands,
    )
    session.add(
        ContractAccountingAnchor(
            **{**provenance.model_dump(mode="python"), "public_id": identifier},
            **native_position_anchor_columns_v2(payload),
        )
    )
    return identifier, payload


async def _stage_event(
    session: AsyncSession,
    admission: _PositionAdmissionV2,
    anchor: tuple[str, ContractPositionAnchorV2],
    before: ContractPositionValue,
    provenance: SnapshotWriteProvenance,
) -> ContractPositionLedgerWriteResult:
    """Flush actual position arithmetic, seed and one shared native source claim atomically."""
    request, source, evidence = admission.request, admission.source, admission.evidence
    scope = evidence.authority.scope
    maximum = await session.scalar(
        select(func.max(ContractAccountingEvent.ledger_sequence)).where(
            ContractAccountingEvent.account_scope_digest == account_scope_digest(scope)
        )
    )
    derived, observed = fold_position_source(
        before, source.unit, source.position, anchor[1].operands.unit
    )
    status: Literal["equal", "source_precision_unproven"] = (
        "equal" if derived.entry_price == observed.entry_price else "source_precision_unproven"
    )
    operands = ContractPositionEventPayload(
        decimal_policy=CURRENT_POSITION_DECIMAL_POLICY,
        scope=scope,
        instrument_public_id=source.unit.scope.instrument_public_id,
        route_authority_public_id=request.fill.authority_reference.public_id,
        position_route_authority_public_id=request.position_authority_reference.public_id,
        anchor_public_id=anchor[0],
        raw_receipt_public_id=request.fill.raw_receipt_public_id,
        position_receipt_public_id=request.position_receipt_public_id,
        execution_public_id=source.execution.public_id,
        execution_scope_sequence=source.execution.scope_sequence,
        unit_snapshot_public_id=request.unit_snapshot_public_id,
        source_alias_public_id=source.fill.alias.public_id,
        source_alias_timestamp=source.fill.alias.timestamp,
        venue_execution_id=source.fill.fill.execution_id,
        executed_at=source.fill.fill.occurred_at,
        execution_known_at=source.execution.timestamp,
        ledger_sequence=int(maximum or 0) + 1,
        unit=source.unit,
        profile_unit=anchor[1].operands.unit,
        source_position=source.position,
        before=before,
        derived_after=derived,
        observed_after=observed,
        basis_status=status,
    )
    payload = ContractPositionEventV2(
        source=evidence,
        fill_authority_reference=request.fill.authority_reference,
        fill_ownership_reference=request.fill.ownership_reference,
        operands=operands,
    )
    row = ContractAccountingEvent(
        **provenance.model_dump(mode="python"), **native_event_columns_v2(payload)
    )
    session.add(row)
    await claim_new_accounting_source_v2(session, row)
    return _result(row, payload, created=True)


async def append_bound_native_position_v2(
    session: AsyncSession,
    request: NativePositionSourceRequestV2,
    provenance: SnapshotWriteProvenance,
    policy: NativeAuthorityFreshnessPolicy,
) -> ContractPositionLedgerWriteResult:
    """Compose one actual native position fact in the caller's wallet-fenced transaction.

    Args:
        session: Actual caller-owned financial transaction; failure rolls back every staged fact.
        request: Exact original Execution/unit/fill ownership and later native position-log references.
        provenance: Stable local posting identity and actual post-fence knowledge clock.
        policy: Trusted current native source freshness configuration from the application boundary.

    Returns:
        Actual new or original immutable position coordinates, leaving all holds and cash separate.
    """
    actual = NativePositionSourceRequestV2.model_validate(request)
    stamp = SnapshotWriteProvenance.model_validate(provenance)
    await require_native_contract_accounting_guards(session)
    with session_no_autoflush(session):
        source = await bind_native_position_receipt_v2(session, actual, stamp.timestamp)
        scope = source.financial_source.authority.payload.scope
        events = await load_native_position_history_v2(
            session, scope, source.unit.scope.instrument_public_id, stamp.timestamp
        )
        existing = next(
            (
                item
                for item in events
                if item.payload.operands.venue_execution_id == source.fill.fill.execution_id
            ),
            None,
        )
        if existing is not None:
            _require_same_source(existing, actual, source)
            return _result(existing.row, existing.payload, created=False)
        admission = await _current_admission(session, actual, source, stamp, policy)
        await _require_account_tip(session, admission, stamp.timestamp)
        if not events:
            anchor = await _stage_anchor(session, admission, stamp)
            before = anchor[1].operands.opening
        else:
            previous = events[-1]
            anchor = (previous.payload.operands.anchor_public_id, previous.anchor)
            before = previous.payload.operands.observed_after
            require_native_financial_binding(
                previous.payload.operands.source_position.source_entry.source_log_id
                < source.position.source_entry.source_log_id,
                "native_position_source_order_discontinuous",
            )
        return await _stage_event(session, admission, anchor, before, stamp)


async def append_native_position_from_receipts_v2(
    repository: AccountingRepository,
    request: NativePositionSourceRequestV2,
    identity: ExecutionEvidenceIdentity,
    policy: NativeAuthorityFreshnessPolicy,
) -> ContractPositionLedgerWriteResult:
    """Own the actual financial transaction and timestamp the posting only after its wallet fence.

    Args:
        repository: Actual repository with the established backend transaction and wallet-fence protocol.
        request: Actual already retained original targets and later native source references.
        identity: Stable trace identity for commit uncertainty, without a caller-supplied clock.
        policy: Trusted current native source freshness configuration.

    Returns:
        Committed new or original position coordinates, without legacy spot projection or cash posting.
    """
    actual = NativePositionSourceRequestV2.model_validate(request)
    original = ExecutionEvidenceIdentity.model_validate(identity)
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await repository._acquire_execution_fence_lock(session, actual.fill.wallet_public_id)
        stamp = SnapshotWriteProvenance(
            **original.model_dump(mode="python"), timestamp=datetime.now(UTC)
        )
        result = await append_bound_native_position_v2(session, actual, stamp, policy)
        await session.commit()
    return result
