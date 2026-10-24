"""Append actual native-v2 cash facts under current source and durable owner admission.

The actual private sidecar, prepared-key account source and locked owner policy
are independent prerequisites. Posting uses one real account-log balance leg;
modeled PnL, position components and collateral valuations never manufacture cash.
Existing source holds remain pending, and cash-only publication is a separate
versioned path rather than an invented Execution reference.
"""

from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from typing import Self
from uuid import NAMESPACE_URL
from uuid import uuid5

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import model_validator
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.portfolio.accounting_native_source import native_source_semantic_digest
from snapper.application.portfolio.futures_account_log import FuturesNativeCashLeg
from snapper.application.portfolio.futures_account_log import native_futures_cash_leg
from snapper.application.portfolio.futures_cash_ledger import FuturesNativeCashAnchor
from snapper.application.portfolio.futures_cash_ledger import FuturesNativeCashEvent
from snapper.application.portfolio.futures_cash_ledger import account_log_cash_source_identity
from snapper.application.portfolio.native_financial_payloads_v2 import FuturesNativeCashAnchorV2
from snapper.application.portfolio.native_financial_payloads_v2 import FuturesNativeCashEventV2
from snapper.application.portfolio.native_financial_payloads_v2 import NativeFinancialSourceEvidence
from snapper.core.account_authority_reference import AccountAuthorityReference
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.core.execution_route_authority import account_scope_digest
from snapper.core.execution_unit_admission import ExecutionRouteReference
from snapper.core.execution_unit_snapshot import EvidenceUUID
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance
from snapper.core.native_account_authority import NativeAccountAuthorityPayload
from snapper.data.accounting_native_source_store_v2 import claim_new_accounting_source_v2
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.execution_account_authority_store import require_current_account_authority
from snapper.data.financial_account_owner_store import load_current_financial_owner_evidence
from snapper.data.futures_cash_ledger_store import NativeCashLedgerWriteResult
from snapper.data.models import ContractAccountingAnchor
from snapper.data.models import ContractAccountingEvent
from snapper.data.native_account_authority_store import NativeAuthorityFreshnessPolicy
from snapper.data.native_cash_history_v2 import latest_native_currency_event_v2
from snapper.data.native_cash_history_v2 import load_native_cash_history_v2
from snapper.data.native_cash_history_v2 import native_cash_anchor_columns_v2
from snapper.data.native_contract_accounting_guards import require_native_contract_accounting_guards
from snapper.data.native_financial_event_binding_v2 import native_event_columns_v2
from snapper.data.native_financial_event_family import (
    get_native_financial_event_family as get_native_financial_event_material,
)
from snapper.data.native_financial_source_binding_v2 import BoundNativeFinancialSource
from snapper.data.native_financial_source_binding_v2 import bind_native_financial_receipt
from snapper.data.native_financial_source_binding_v2 import require_native_financial_binding
from snapper.data.session_context import session_no_autoflush


class NativeCashAccountingRequestV2(BaseModel):
    """Actual raw and tagged account references without caller-supplied money or ownership flags."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        revalidate_instances="always",
        hide_input_in_errors=True,
    )

    wallet_public_id: EvidenceUUID
    raw_receipt_public_id: EvidenceUUID
    authority_reference: AccountAuthorityReference

    @model_validator(mode="after")
    def require_native_reference(self) -> Self:
        """Keep explicit native financial interpretation separate from an untyped UUID guess.

        Returns:
            The same request awaiting every actual source, current policy and financial graph check.
        """
        if self.authority_reference.family != "native_accounts_v1":
            raise ValueError(
                "Native financial cash requires an explicit native authority reference"
            )
        return self


@dataclass(frozen=True, slots=True)
class _CashAdmission:
    """Actual bound original sources and current owner decision retained by the outer transaction."""

    evidence: NativeFinancialSourceEvidence
    source: BoundNativeFinancialSource
    leg: FuturesNativeCashLeg


async def _current_cash_admission(
    session: AsyncSession,
    request: NativeCashAccountingRequestV2,
    provenance: SnapshotWriteProvenance,
    policy: NativeAuthorityFreshnessPolicy,
    received: BoundNativeFinancialSource,
) -> _CashAdmission:
    """Recheck actual current account before a locked policy decision for a genuinely new cash event."""
    bound = received.authority
    native = NativeAccountAuthorityPayload.model_validate(bound.payload)
    require_native_financial_binding(
        native.scope.wallet_public_id == request.wallet_public_id,
        "native_financial_cash_current_wallet_mismatch",
    )
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
        current == bound,
        "native_financial_cash_current_reference_changed",
    )
    owner = await load_current_financial_owner_evidence(
        session, request.authority_reference, provenance.timestamp
    )
    evidence = NativeFinancialSourceEvidence(
        recorded_at=provenance.timestamp,
        authority_reference=request.authority_reference,
        authority=native,
        owner_evidence=owner,
        ownership_reference=received.ownership.reference,
    )
    return _CashAdmission(evidence, received, native_futures_cash_leg(received.entry))


def _write_result(
    row: ContractAccountingEvent, payload: FuturesNativeCashEventV2, *, created: bool
) -> NativeCashLedgerWriteResult:
    """Expose actual flushed coordinates and an observed currency amount, without spendability."""
    value = payload.operands
    return NativeCashLedgerWriteResult(
        public_id=row.public_id,
        anchor_public_id=value.anchor_public_id,
        content_digest=row.content_digest,
        ledger_sequence=value.ledger_sequence,
        native_currency=value.native_currency,
        observed_balance=value.cash_after,
        created=created,
    )


async def _require_current_account_tip(
    session: AsyncSession, admission: _CashAdmission, provenance: SnapshotWriteProvenance
) -> None:
    """Keep posting knowledge and actual UID continuity after every financial kind in the scope."""
    scope = admission.evidence.authority.scope
    latest = await session.scalar(
        select(ContractAccountingEvent)
        .where(ContractAccountingEvent.account_scope_digest == account_scope_digest(scope))
        .order_by(ContractAccountingEvent.ledger_sequence.desc())
        .limit(1)
    )
    if latest is None:
        return
    material = await get_native_financial_event_material(
        session, latest.public_id, provenance.timestamp
    )
    require_native_financial_binding(
        material.payload.operands.scope == scope
        and material.payload.source.authority.venue_account_id
        == admission.evidence.authority.venue_account_id,
        "native_financial_cash_account_tip_changed",
    )


async def _stage_anchor(
    session: AsyncSession, admission: _CashAdmission, provenance: SnapshotWriteProvenance
) -> str:
    """Stage one actual old-balance seed only when no previous proven currency chain exists."""
    evidence = admission.evidence
    scope = evidence.authority.scope
    identifier = str(
        uuid5(
            NAMESPACE_URL,
            f"snapper:futures-native-cash-anchor:v2:{account_scope_digest(scope)}:{admission.leg.native_currency}",
        )
    )
    occupied = await session.scalar(
        select(ContractAccountingAnchor.public_id).where(
            ContractAccountingAnchor.public_id == identifier
        )
    )
    require_native_financial_binding(
        occupied is None, "native_financial_cash_unpaired_anchor_present"
    )
    operands = FuturesNativeCashAnchor(
        scope=scope,
        route_authority_public_id=evidence.authority_reference.public_id,
        source_receipt_public_id=evidence.ownership_reference.raw_receipt_public_id,
        native_currency=admission.leg.native_currency,
        balance=admission.leg.entry.old_balance,
        source_entry=admission.leg.entry,
    )
    payload = FuturesNativeCashAnchorV2(source=evidence, operands=operands)
    session.add(
        ContractAccountingAnchor(
            **{**provenance.model_dump(mode="python"), "public_id": identifier},
            **native_cash_anchor_columns_v2(payload),
        )
    )
    return identifier


async def _stage_event(
    session: AsyncSession,
    admission: _CashAdmission,
    anchor_public_id: str,
    provenance: SnapshotWriteProvenance,
) -> NativeCashLedgerWriteResult:
    """Stage an actual delta and flush its anchor/event/shared native claim together, without commit."""
    evidence = admission.evidence
    scope = evidence.authority.scope
    maximum = await session.scalar(
        select(func.max(ContractAccountingEvent.ledger_sequence)).where(
            ContractAccountingEvent.account_scope_digest == account_scope_digest(scope)
        )
    )
    operands = FuturesNativeCashEvent(
        scope=scope,
        route_authority_public_id=evidence.authority_reference.public_id,
        anchor_public_id=anchor_public_id,
        raw_receipt_public_id=evidence.ownership_reference.raw_receipt_public_id,
        ledger_sequence=int(maximum or 0) + 1,
        source_identity=account_log_cash_source_identity(admission.leg.entry),
        native_currency=admission.leg.native_currency,
        cash_before=admission.leg.entry.old_balance,
        cash_after=admission.leg.entry.new_balance,
        cash_delta=admission.leg.cash_delta,
        source_entry=admission.leg.entry,
    )
    payload = FuturesNativeCashEventV2(source=evidence, operands=operands)
    row = ContractAccountingEvent(
        **provenance.model_dump(mode="python"),
        **native_event_columns_v2(payload),
    )
    session.add(row)
    await claim_new_accounting_source_v2(session, row)
    return _write_result(row, payload, created=True)


async def append_bound_native_cash_v2(
    session: AsyncSession,
    request: NativeCashAccountingRequestV2,
    provenance: SnapshotWriteProvenance,
    policy: NativeAuthorityFreshnessPolicy,
) -> NativeCashLedgerWriteResult:
    """Compose actual native cash facts in one current-owner wallet-fenced financial transaction.

    Args:
        session: Actual caller-owned wallet-fenced transaction; no inner commit is issued.
        request: Actual retained raw and tagged current native account references.
        provenance: Stable local event identity and honest post-fence knowledge clock.
        policy: Trusted current source freshness configuration from the application boundary.

    Returns:
        Actual new or semantically deduplicated currency coordinates, leaving all holds pending.
    """
    binding = NativeCashAccountingRequestV2.model_validate(request)
    stamp = SnapshotWriteProvenance.model_validate(provenance)
    await require_native_contract_accounting_guards(session)
    with session_no_autoflush(session):
        received = await bind_native_financial_receipt(
            session, binding.authority_reference, binding.raw_receipt_public_id, stamp.timestamp
        )
        require_native_financial_binding(
            received.raw.scope.wallet_public_id == binding.wallet_public_id,
            "native_financial_cash_requested_wallet_mismatch",
        )
        native = NativeAccountAuthorityPayload.model_validate(received.authority.payload)
        leg = native_futures_cash_leg(received.entry)
        events = await load_native_cash_history_v2(session, native.scope, stamp.timestamp)
        identity = account_log_cash_source_identity(leg.entry)
        existing = next(
            (item for item in events if item.payload.operands.source_identity == identity), None
        )
        if existing is not None:
            require_native_financial_binding(
                existing.source.ownership.account_uid == received.ownership.account_uid
                and native_source_semantic_digest(existing.payload.operands.source_entry)
                == native_source_semantic_digest(leg.entry),
                "native_financial_cash_source_identity_conflict",
            )
            return _write_result(existing.row, existing.payload, created=False)
        admission = await _current_cash_admission(session, binding, stamp, policy, received)
        await _require_current_account_tip(session, admission, stamp)
        previous = latest_native_currency_event_v2(events, admission.leg.native_currency)
        if previous is None:
            anchor_id = await _stage_anchor(session, admission, stamp)
        else:
            old = previous.payload.operands
            require_native_financial_binding(
                old.cash_after == admission.leg.entry.old_balance
                and old.source_entry.source_log_id < admission.leg.entry.source_log_id,
                "native_financial_cash_source_chain_discontinuous",
            )
            anchor_id = old.anchor_public_id
        return await _stage_event(session, admission, anchor_id, stamp)


async def append_native_cash_from_receipt_v2(
    repository: AccountingRepository,
    request: NativeCashAccountingRequestV2,
    identity: ExecutionEvidenceIdentity,
    policy: NativeAuthorityFreshnessPolicy,
) -> NativeCashLedgerWriteResult:
    """Own the actual wallet transaction and sample its honest knowledge clock after the fence.

    Args:
        repository: Actual repository with its existing backend transaction and wallet fence primitives.
        request: Strict original source references, never supplied cash mirrors or permission flags.
        identity: Stable producer/event identity for uncertain-commit retry, with no supplied clock.
        policy: Trusted current native account-source freshness configuration.

    Returns:
        The actual committed new or original cash event; a failure rolls back every staged financial row.
    """
    actual = NativeCashAccountingRequestV2.model_validate(request)
    original = ExecutionEvidenceIdentity.model_validate(identity)
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await repository._acquire_execution_fence_lock(session, actual.wallet_public_id)
        provenance = SnapshotWriteProvenance(
            **original.model_dump(mode="python"), timestamp=datetime.now(UTC)
        )
        result = await append_bound_native_cash_v2(session, actual, provenance, policy)
        await session.commit()
    return result
