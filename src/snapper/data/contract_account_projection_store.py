"""Hydrate current partial contract accounts through actual immutable financial readers."""

from datetime import datetime
from typing import cast

from pydantic import TypeAdapter
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.portfolio.contract_position_ledger import ContractPositionEventPayload
from snapper.application.portfolio.futures_cash_ledger import FuturesNativeCashEvent
from snapper.application.portfolio.native_financial_payloads_v2 import ContractPositionEventV2
from snapper.application.portfolio.native_financial_payloads_v2 import FuturesNativeCashEventV2
from snapper.core.contract_account_projection import ContractAccountSnapshot
from snapper.core.contract_account_projection import ContractJournalReference
from snapper.core.contract_account_projection import ContractProjectionHold
from snapper.core.contract_account_projection import ObservedContractCash
from snapper.core.contract_account_projection import ObservedContractPosition
from snapper.core.contract_account_projection import ReportedPositionCashComponents
from snapper.core.contract_accounting_observation import AccountingObservationScope
from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.core.execution_route_authority import account_scope_digest
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.data.accounting_observation_binding import get_bound_accounting_observation
from snapper.data.contract_accounting_store import load_unresolved_accounting_holds
from snapper.data.contract_position_ledger_store import get_bound_contract_position_event
from snapper.data.futures_cash_ledger_store import load_observed_futures_cash
from snapper.data.models import AccountingRawReceipt
from snapper.data.models import AccountingScopeHold
from snapper.data.models import ContractAccountingEvent
from snapper.data.native_cash_history_v2 import load_native_cash_history_v2
from snapper.data.native_position_history_v2 import get_bound_native_position_event_v2

_POSITION: TypeAdapter[ContractPositionEventPayload | ContractPositionEventV2] = TypeAdapter(
    ContractPositionEventPayload | ContractPositionEventV2
)
_CASH: TypeAdapter[FuturesNativeCashEvent | FuturesNativeCashEventV2] = TypeAdapter(
    FuturesNativeCashEvent | FuturesNativeCashEventV2
)


class ContractAccountProjectionError(ValueError):
    """Retained facts cannot establish the requested partial account projection."""


def _position(payload: ContractPositionEventPayload, public_id: str) -> ObservedContractPosition:
    """Project actual native quantity and explicitly name the separate modeled totals."""
    observed = payload.observed_after
    return ObservedContractPosition(
        instrument_public_id=payload.instrument_public_id,
        latest_event_public_id=public_id,
        anchor_public_id=payload.anchor_public_id,
        unit_snapshot_public_id=payload.unit_snapshot_public_id,
        route_authority_public_id=payload.position_route_authority_public_id,
        ledger_sequence=payload.ledger_sequence,
        quantity=observed.quantity,
        entry_price=observed.entry_price,
        settlement_currency=cast(str, payload.unit.dimensions.settlement_currency),
        quote_currency=payload.unit.dimensions.quote_currency,
        modeled_realized_pnl_since_anchor=observed.realized_pnl_since_anchor,
        modeled_quote_turnover_since_anchor=observed.quote_turnover_since_anchor,
        basis_status=payload.basis_status,
    )


def _components(
    payload: ContractPositionEventPayload, public_id: str
) -> ReportedPositionCashComponents:
    """Retain reported operands without manufacturing native cash or a pairing assertion."""
    entry = payload.source_position.source_entry
    return ReportedPositionCashComponents(
        position_event_public_id=public_id,
        source_receipt_public_id=payload.position_receipt_public_id,
        target_json_pointer=entry.target_json_pointer,
        source_log_id=str(entry.source_log_id),
        booking_uid=entry.booking_uid,
        execution_id=payload.venue_execution_id,
        reported_collateral_currency=entry.collateral,
        fee=entry.fee,
        realized_pnl=entry.realized_pnl,
        realized_funding=entry.realized_funding,
        liquidation_fee=entry.liquidation_fee,
        funding_rate=entry.funding_rate,
        conversion_spread_percentage=entry.conversion_spread_percentage,
    )


def _cash(payload: FuturesNativeCashEvent, public_id: str) -> ObservedContractCash:
    """Keep the absolute observed currency balance instead of summing source components."""
    return ObservedContractCash(
        native_currency=payload.native_currency,
        balance=payload.cash_after,
        latest_event_public_id=public_id,
        anchor_public_id=payload.anchor_public_id,
        route_authority_public_id=payload.route_authority_public_id,
        ledger_sequence=payload.ledger_sequence,
    )


async def _holds(
    session: AsyncSession, scope: ExecutionAccountScope, as_of: datetime
) -> tuple[ContractProjectionHold, ...]:
    """Read actual unresolved refusals and reject source knowledge newer than this read clock."""
    observation_scope = AccountingObservationScope.model_validate(scope.model_dump())
    holds = await load_unresolved_accounting_holds(session, observation_scope)
    result: list[ContractProjectionHold] = []
    for hold in holds:
        row = (
            await session.execute(
                select(AccountingScopeHold).where(AccountingScopeHold.public_id == hold.public_id)
            )
        ).scalar_one()
        raw = await get_bound_accounting_observation(session, hold.raw_receipt_public_id)
        retained = (
            await session.execute(
                select(AccountingRawReceipt.timestamp).where(
                    AccountingRawReceipt.public_id == hold.raw_receipt_public_id
                )
            )
        ).scalar_one()
        if row.timestamp > as_of or retained > as_of or raw.observed_at > as_of:
            raise ContractAccountProjectionError("contract_projection_hold_knowledge_unavailable")
        if (
            raw.scope.model_dump(exclude={"margin_account"})
            != scope.model_dump(exclude={"margin_account"})
            or raw.scope.margin_account not in {None, scope.margin_account}
            or row.margin_account != raw.scope.margin_account
        ):
            raise ContractAccountProjectionError("contract_projection_hold_scope_mismatch")
        result.append(
            ContractProjectionHold(
                public_id=hold.public_id,
                raw_receipt_public_id=hold.raw_receipt_public_id,
                reason=hold.reason,
                margin_account=hold.margin_account,
            )
        )
    return tuple(sorted(result, key=lambda item: item.public_id))


async def _verify_position_graphs(
    session: AsyncSession, rows: tuple[ContractAccountingEvent, ...], as_of: datetime
) -> None:
    """Bind each instrument's latest event, which independently replays its entire actual chain."""
    latest: dict[str, ContractAccountingEvent] = {}
    for row in rows:
        if row.kind == "contract_position_fill":
            if row.instrument_public_id is None:
                raise ContractAccountProjectionError(
                    "contract_projection_position_instrument_missing"
                )
            latest[row.instrument_public_id] = row
    for row in latest.values():
        public_id = row.public_id
        payload = _POSITION.validate_json(row.canonical_payload)
        if isinstance(payload, ContractPositionEventV2):
            await get_bound_native_position_event_v2(session, public_id, as_of)
        else:
            await get_bound_contract_position_event(session, public_id)


def _project_rows(rows: tuple[ContractAccountingEvent, ...], scope: ExecutionAccountScope) -> tuple[
    tuple[ContractJournalReference, ...],
    tuple[ObservedContractPosition, ...],
    tuple[ObservedContractCash, ...],
    tuple[ReportedPositionCashComponents, ...],
]:
    """Copy already bound immutable facts into explicit partial projection dimensions."""
    journal: list[ContractJournalReference] = []
    positions: dict[str, ObservedContractPosition] = {}
    cash: dict[str, ObservedContractCash] = {}
    components: list[ReportedPositionCashComponents] = []
    for row in rows:
        position = None
        currency = None
        if row.kind == "contract_position_fill":
            material = _POSITION.validate_json(row.canonical_payload)
            payload = (
                material.operands if isinstance(material, ContractPositionEventV2) else material
            )
            position = payload.instrument_public_id
            positions[position] = _position(payload, row.public_id)
            components.append(_components(payload, row.public_id))
            actual_scope = payload.scope
        else:
            observed = _CASH.validate_json(row.canonical_payload)
            native = (
                observed.operands if isinstance(observed, FuturesNativeCashEventV2) else observed
            )
            currency = native.native_currency
            cash[currency] = _cash(native, row.public_id)
            actual_scope = native.scope
        if actual_scope != scope:
            raise ContractAccountProjectionError("contract_projection_requested_scope_mismatch")
        journal.append(
            ContractJournalReference(
                public_id=row.public_id,
                content_digest=row.content_digest,
                ledger_sequence=row.ledger_sequence,
                recorded_at=row.timestamp,
                kind=("contract_position_fill" if position is not None else "native_cash"),
                instrument_public_id=position,
                native_currency=currency,
            )
        )
    return (
        tuple(journal),
        tuple(positions[key] for key in sorted(positions)),
        tuple(cash[key] for key in sorted(cash)),
        tuple(components),
    )


async def _verify_cash_graphs(
    session: AsyncSession,
    rows: tuple[ContractAccountingEvent, ...],
    scope: ExecutionAccountScope,
    as_of: datetime,
) -> None:
    """Rebind the complete actual cash prefix through its explicit schema before projecting it."""
    first = next((row for row in rows if row.kind == "native_cash"), None)
    if first is None:
        return
    value = _CASH.validate_json(first.canonical_payload)
    if isinstance(value, FuturesNativeCashEventV2):
        await load_native_cash_history_v2(session, scope, as_of)
    else:
        await load_observed_futures_cash(session, scope)


async def load_bound_contract_account_snapshot(
    session: AsyncSession, scope: ExecutionAccountScope, as_of: EvidenceInstant
) -> ContractAccountSnapshot:
    """Hydrate the current retained account under the caller's actual wallet execution fence.

    This is a current fenced read, not an arbitrary historical query. It refuses
    any selected fact newer than the supplied local knowledge clock. Existing
    financial readers prove source, unit, claim and predecessor lineage without
    treating old evidence as current spending authority. Startup restore
    integrity remains a separate mandatory activation boundary.

    Args:
        session: Actual repository session with an outer transaction and wallet fence.
        scope: Exact declared wallet, exchange, mode, environment and margin namespace.
        as_of: Aware local clock sampled after the caller acquired the wallet fence.

    Returns:
        A verified partial account with explicit holds and unproven reconciliation.
    """
    if not session.in_transaction():
        raise RuntimeError("contract_projection_requires_caller_transaction")
    proven = ExecutionAccountScope.model_validate(scope)
    knowledge: datetime = TypeAdapter(EvidenceInstant).validate_python(as_of)
    rows = tuple(
        (
            await session.scalars(
                select(ContractAccountingEvent)
                .where(ContractAccountingEvent.account_scope_digest == account_scope_digest(proven))
                .order_by(ContractAccountingEvent.ledger_sequence)
            )
        ).all()
    )
    if any(row.timestamp > knowledge for row in rows):
        raise ContractAccountProjectionError("contract_projection_retained_knowledge_unavailable")
    if any(row.kind not in {"native_cash", "contract_position_fill"} for row in rows):
        raise ContractAccountProjectionError("contract_projection_event_kind_unsupported")
    await _verify_cash_graphs(session, rows, proven, knowledge)
    await _verify_position_graphs(session, rows, knowledge)
    journal, positions, cash, components = _project_rows(rows, proven)
    return ContractAccountSnapshot(
        scope=proven,
        as_of=knowledge,
        journal=journal,
        positions=positions,
        native_cash=cash,
        reported_cash_components=components,
        unresolved_holds=await _holds(session, proven, knowledge),
    )
