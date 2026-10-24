"""Protect actual retained financial owner decisions from a backdated policy close.

This finite administrator-bound read enumerates financial rows before trusting
kind, scope, timestamp or policy-reference projections. It never advances a
wall clock, acquires wallet/UID locks or grants financial readiness. Actual
source and policy evidence are rebound at each original row's knowledge clock.
"""

from datetime import datetime

from pydantic import TypeAdapter
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.portfolio.contract_position_ledger import ContractPositionEventPayload
from snapper.application.portfolio.futures_cash_ledger import FuturesNativeCashEvent
from snapper.application.portfolio.native_financial_payloads_v2 import ContractPositionEventV2
from snapper.application.portfolio.native_financial_payloads_v2 import FuturesNativeCashEventV2
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance
from snapper.core.financial_account_owner import FinancialAccountOwnerEvidence
from snapper.core.financial_account_owner import FinancialAccountOwnerPolicyReference
from snapper.core.financial_account_owner import FinancialAccountOwnerUnavailableError
from snapper.data.contract_accounting_guards import require_contract_accounting_guards
from snapper.data.contract_position_ledger_store import _event_projection
from snapper.data.futures_cash_ledger_store import _verify_event_row
from snapper.data.models import ContractAccountingEvent
from snapper.data.models import is_open_ended_version
from snapper.data.native_contract_accounting_guards import require_native_contract_accounting_guards
from snapper.data.native_financial_event_binding_v2 import bind_native_financial_event_material

type _RetainedFinancialPayload = (
    FuturesNativeCashEvent
    | ContractPositionEventPayload
    | FuturesNativeCashEventV2
    | ContractPositionEventV2
)
_PAYLOAD: TypeAdapter[_RetainedFinancialPayload] = TypeAdapter(_RetainedFinancialPayload)
_INSTANT: TypeAdapter[datetime] = TypeAdapter(EvidenceInstant)


def _require_retained_row(row: ContractAccountingEvent) -> None:
    """Validate the real immutable row envelope before excluding any financial source family."""
    provenance = SnapshotWriteProvenance.model_validate(
        {
            "public_id": row.public_id,
            "session_id": row.session_id,
            "sequence_id": row.sequence_id,
            "timestamp": row.timestamp,
        }
    )
    if (
        provenance.public_id != row.public_id
        or provenance.session_id != row.session_id
        or not is_open_ended_version(row.known_to)
    ):
        raise FinancialAccountOwnerUnavailableError(
            "financial_owner_policy_retained_history_unproven"
        )


async def _bound_decision(
    session: AsyncSession,
    row: ContractAccountingEvent,
    payload: _RetainedFinancialPayload,
) -> FinancialAccountOwnerEvidence | None:
    """Verify all row mirrors and actual native source graphs before selecting a policy reference."""
    _require_retained_row(row)
    if isinstance(payload, FuturesNativeCashEventV2 | ContractPositionEventV2):
        material = await bind_native_financial_event_material(session, row, row.timestamp)
        return material.payload.source.owner_evidence
    if isinstance(payload, FuturesNativeCashEvent):
        _verify_event_row(row, payload)
    else:
        _event_projection(row, payload)
    return None


async def require_financial_owner_policy_close_clock(
    session: AsyncSession,
    reference: FinancialAccountOwnerPolicyReference,
    sampled_at: datetime,
) -> None:
    """Refuse closing an actual active policy at or before one of its retained financial decisions.

    The reserved writer already holds its real active Setting FOR UPDATE and
    shared accounting barrier. No indexed timestamp/kind/family filter can hide
    a relevant row. Strict canonical models and complete row projections are
    rebound first; native source graphs additionally require all native guards.
    Legacy records carry no versioned owner decision and retain their existing
    row/hash ABI. Full economic restore remains a separate activation boundary.

    Args:
        session: Actual reserved-policy transaction after the active Setting row lock.
        reference: Exact active policy revision that the real writer proposes to close.
        sampled_at: Unchanged actual UTC sample taken after that row lock, never a synthetic advance.
    """
    if not session.in_transaction():
        raise RuntimeError("financial_owner_policy_clock_requires_caller_transaction")
    current = FinancialAccountOwnerPolicyReference.model_validate(reference)
    clock = _INSTANT.validate_python(sampled_at)
    await require_contract_accounting_guards(session)
    with session.no_autoflush:
        rows = tuple(
            (
                await session.scalars(
                    select(ContractAccountingEvent).order_by(ContractAccountingEvent.id)
                )
            ).all()
        )
        try:
            material = tuple((row, _PAYLOAD.validate_json(row.canonical_payload)) for row in rows)
            if any(
                isinstance(payload, FuturesNativeCashEventV2 | ContractPositionEventV2)
                for _row, payload in material
            ):
                await require_native_contract_accounting_guards(session)
            decisions = [await _bound_decision(session, row, payload) for row, payload in material]
        except ValueError:
            raise FinancialAccountOwnerUnavailableError(
                "financial_owner_policy_retained_history_unproven"
            ) from None
        if any(
            decision is not None
            and decision.policy_reference == current
            and decision.decided_at >= clock
            for decision in decisions
        ):
            raise FinancialAccountOwnerUnavailableError("financial_owner_policy_clock_not_advanced")
