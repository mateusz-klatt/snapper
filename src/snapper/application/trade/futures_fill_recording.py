"""Atomically record actual Futures position facts and their recoverable notification intent.

The raw observations and unresolved holds already exist before this transaction.
This recording seam does not spend collateral, synthesize cash, release holds,
publish a legacy spot fill or enable order submission.
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from math import isfinite
from typing import cast
from uuid import UUID
from uuid import uuid5

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.trade.private_fill_source import parse_futures_private_fill
from snapper.application.trade.unit_snapshot_fill import BoundFuturesFillSource
from snapper.application.trade.unit_snapshot_fill import FillUnitCaptureRequest
from snapper.application.trade.unit_snapshot_fill import FuturesFillSourceRequest
from snapper.application.trade.unit_snapshot_fill import bind_futures_fill_source
from snapper.application.trade.unit_snapshot_fill import capture_bound_futures_fill_units
from snapper.application.trade.unit_snapshot_fill import futures_fill_receipt
from snapper.core.execution_unit_snapshot import EvidenceUUID
from snapper.core.execution_unit_snapshot import FillSnapshotTarget
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance
from snapper.data.accounting_observation_binding import get_bound_accounting_observation
from snapper.data.contract_position_ledger_store import ContractPositionAccountingRequest
from snapper.data.contract_position_ledger_store import append_bound_contract_position
from snapper.data.contract_position_ledger_store import find_bound_contract_position_event
from snapper.data.execution_unit_snapshot_store import SnapshotBindingError
from snapper.data.financial_publication_store import BoundFinancialPublication
from snapper.data.financial_publication_store import append_bound_financial_publication
from snapper.data.futures_cash_ledger_store import _require_account_continuity
from snapper.data.models import ContractAccountingEvent
from snapper.data.models import Execution
from snapper.data.models import Order
from snapper.data.models import TradeCommand
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import ExecutionInsertRow
from snapper.data.repository_types import VenueEventInsertRow


class FuturesPositionRecordingRequest(FuturesFillSourceRequest):
    """Actual independent source receipts, with no caller-supplied financial target or amount."""

    position_receipt_public_id: EvidenceUUID
    position_route_authority_public_id: EvidenceUUID | None = None


@dataclass(frozen=True)
class RecordedFuturesPosition:
    """Committed actual financial identities and one recoverable accounting notification."""

    execution_public_id: str
    unit_snapshot_public_id: str
    position_event_public_id: str
    publication: BoundFinancialPublication
    created: bool


def _child_stamp(parent: SnapshotWriteProvenance, kind: str) -> SnapshotWriteProvenance:
    """Keep distinct transaction-child identities stable across an exact local transaction retry."""
    return SnapshotWriteProvenance(
        public_id=str(uuid5(UUID(parent.public_id), kind)),
        session_id=parent.session_id,
        sequence_id=parent.sequence_id,
        timestamp=parent.timestamp,
    )


def _mirror(value: Decimal) -> float:
    """Create only a compatible legacy display mirror while retaining exact source operands."""
    result = float(value)
    if not isfinite(result) or (value != 0 and result == 0):
        raise SnapshotBindingError("financial_float_projection_unrepresentable")
    return result


async def _require_unrecorded_execution(
    session: AsyncSession, source: BoundFuturesFillSource
) -> None:
    """Refuse a legacy or partial execution whose environment and accounting linkage are unproven."""
    existing = (
        await session.execute(
            select(Execution.id)
            .where(
                Execution.wallet_public_id == source.raw.scope.wallet_public_id,
                Execution.exchange == source.raw.scope.exchange,
                Execution.mode == source.raw.scope.mode,
                Execution.exec_id == source.fill.execution_id,
            )
            .limit(1)
        )
    ).first()
    if existing is not None:
        raise SnapshotBindingError("financial_existing_execution_without_bound_accounting")


async def _correlated_order(
    session: AsyncSession, source: BoundFuturesFillSource, knowledge_at: datetime
) -> Order:
    """Require actual temporal order lineage from exact provider identity and native instrument."""
    rows = (
        await session.scalars(
            select(Order).where(
                Order.wallet_public_id == source.raw.scope.wallet_public_id,
                Order.instrument_public_id == source.catalog.instrument.public_id,
                Order.mode == source.raw.scope.mode,
                Order.exchange_order_id == source.fill.order_id,
                Order.timestamp <= knowledge_at,
                Order.known_to > knowledge_at,
            )
        )
    ).all()
    if len(rows) != 1:
        raise SnapshotBindingError("financial_actual_order_correlation_unproven")
    order = rows[0]
    if (
        source.fill.client_order_id is not None
        and source.fill.client_order_id != order.client_order_id
    ):
        raise SnapshotBindingError("financial_actual_client_order_contradiction")
    return order


async def _correlated_command(
    session: AsyncSession, source: BoundFuturesFillSource, order: Order, knowledge_at: datetime
) -> TradeCommand:
    """Resolve one actual original command without inventing a request from the incoming fill."""
    rows = (
        await session.scalars(
            select(TradeCommand).where(
                TradeCommand.wallet_public_id == source.raw.scope.wallet_public_id,
                TradeCommand.exchange == source.raw.scope.exchange,
                TradeCommand.mode == source.raw.scope.mode,
                TradeCommand.instrument == source.catalog.symbol.native_symbol,
                TradeCommand.client_order_id == order.client_order_id,
                TradeCommand.command_type.in_(("create", "submit")),
                TradeCommand.timestamp <= knowledge_at,
                TradeCommand.known_to > knowledge_at,
            )
        )
    ).all()
    if len(rows) != 1:
        raise SnapshotBindingError("financial_actual_command_correlation_unproven")
    return rows[0]


def _execution_row(
    source: BoundFuturesFillSource, order: Order, provenance: SnapshotWriteProvenance
) -> ExecutionInsertRow:
    """Record this exact source fill, preserving native decimal operands and both actual clocks."""
    fill = source.fill
    return {
        "wallet_public_id": source.raw.scope.wallet_public_id,
        "order_public_id": order.public_id,
        "timestamp": provenance.timestamp,
        "executed_at": fill.occurred_at,
        "session_id": provenance.session_id,
        "sequence_id": provenance.sequence_id,
        "side": fill.side,
        "status": "filled" if fill.remaining_quantity == 0 else "partial",
        "price": _mirror(fill.price),
        "size": _mirror(fill.quantity),
        "fee": _mirror(fill.fee),
        "fee_asset": fill.fee_currency,
        "exec_id": fill.execution_id,
        "price_decimal": str(fill.price),
        "size_decimal": str(fill.quantity),
        "fee_decimal": str(fill.fee),
        "numeric_provenance": "venue_raw",
        "liquidity_role": {"maker": "maker", "taker": "taker", "takerAfterEdit": "taker"}.get(
            fill.fill_type, "unknown"
        ),
    }


def _venue_row(
    source: BoundFuturesFillSource,
    command: TradeCommand,
    provenance: SnapshotWriteProvenance,
) -> VenueEventInsertRow:
    """Retain the actual source child as one event rather than an absorbed cumulative gap."""
    fill = source.fill
    return {
        "wallet_public_id": source.raw.scope.wallet_public_id,
        "command_public_id": command.public_id,
        "event_type": "fill_observed",
        "shard_key": command.shard_key,
        "exchange": source.raw.scope.exchange,
        "instrument": source.catalog.symbol.native_symbol,
        "mode": source.raw.scope.mode,
        "timestamp": provenance.timestamp,
        "received_at": source.raw.observed_at,
        "venue_timestamp": fill.occurred_at,
        "session_id": provenance.session_id,
        "sequence_id": provenance.sequence_id,
        "side": fill.side,
        "fill_size": _mirror(fill.quantity),
        "fill_price": _mirror(fill.price),
        "fee": _mirror(fill.fee),
        "fee_asset": fill.fee_currency,
        "exec_id": fill.execution_id,
        "exchange_order_id": fill.order_id,
        "client_order_id": command.client_order_id,
        "venue_client_id": fill.client_order_id,
        "payload_json": futures_fill_receipt(source).model_dump_json(),
    }


async def _original_recording(
    session: AsyncSession,
    source: BoundFuturesFillSource,
    request: FuturesPositionRecordingRequest,
    provenance: SnapshotWriteProvenance,
) -> RecordedFuturesPosition | None:
    """Reuse original financial targets only after comparing exact source facts and account continuity."""
    position = await find_bound_contract_position_event(
        session, source.authority.scope, source.fill.execution_id
    )
    if position is None:
        return None
    await _require_account_continuity(
        session, position.route_authority_public_id, request.route_authority_public_id
    )
    raw = await get_bound_accounting_observation(session, position.raw_receipt_public_id)
    original = parse_futures_private_fill(raw)
    transport = {"subscription_sequence", "target_json_pointer", "feed"}
    if original.model_dump(exclude=transport) != source.fill.model_dump(exclude=transport):
        raise SnapshotBindingError("financial_source_redelivery_conflict")
    target = cast(FillSnapshotTarget, position.unit.target)
    position_authority = request.position_route_authority_public_id or (
        position.position_route_authority_public_id
        if request.position_receipt_public_id == position.position_receipt_public_id
        else request.route_authority_public_id
    )
    await append_bound_contract_position(
        session,
        ContractPositionAccountingRequest(
            fill=FillUnitCaptureRequest(
                wallet_public_id=request.wallet_public_id,
                raw_receipt_public_id=position.raw_receipt_public_id,
                route_authority_public_id=position.route_authority_public_id,
                execution_public_id=position.execution_public_id,
                venue_event_public_id=target.venue_event_public_id,
            ),
            unit_snapshot_public_id=position.unit_snapshot_public_id,
            position_receipt_public_id=request.position_receipt_public_id,
            position_route_authority_public_id=position_authority,
        ),
        _child_stamp(provenance, "position"),
    )
    row = (
        await session.execute(
            select(ContractAccountingEvent).where(
                ContractAccountingEvent.execution_public_id == position.execution_public_id
            )
        )
    ).scalar_one()
    publication = await append_bound_financial_publication(
        session, row.public_id, _child_stamp(provenance, "publication")
    )
    return RecordedFuturesPosition(
        position.execution_public_id,
        position.unit_snapshot_public_id,
        row.public_id,
        publication,
        False,
    )


async def _record_new_position(
    repository: SQLAlchemyRepository,
    session: AsyncSession,
    request: FuturesPositionRecordingRequest,
    source: BoundFuturesFillSource,
    provenance: SnapshotWriteProvenance,
) -> RecordedFuturesPosition:
    """Flush all real financial targets and the publication intent before the only outer commit."""
    await _require_unrecorded_execution(session, source)
    order = await _correlated_order(session, source, provenance.timestamp)
    command = await _correlated_command(session, source, order, provenance.timestamp)
    execution = await repository.insert_execution_in_transaction(
        session, _execution_row(source, order, provenance)
    )
    event = await repository.insert_venue_event_in_transaction(
        session, _venue_row(source, command, provenance)
    )
    fill = FillUnitCaptureRequest(
        wallet_public_id=request.wallet_public_id,
        raw_receipt_public_id=request.raw_receipt_public_id,
        route_authority_public_id=request.route_authority_public_id,
        execution_public_id=execution.public_id,
        venue_event_public_id=event.public_id,
    )
    unit = await capture_bound_futures_fill_units(session, fill, _child_stamp(provenance, "unit"))
    position = await append_bound_contract_position(
        session,
        ContractPositionAccountingRequest(
            fill=fill,
            unit_snapshot_public_id=unit.result.public_id,
            position_receipt_public_id=request.position_receipt_public_id,
            position_route_authority_public_id=request.position_route_authority_public_id,
        ),
        _child_stamp(provenance, "position"),
    )
    publication = await append_bound_financial_publication(
        session, position.public_id, _child_stamp(provenance, "publication")
    )
    return RecordedFuturesPosition(
        execution.public_id, unit.result.public_id, position.public_id, publication, True
    )


async def record_futures_position(
    repository: SQLAlchemyRepository,
    request: FuturesPositionRecordingRequest,
    provenance: SnapshotWriteProvenance,
) -> RecordedFuturesPosition:
    """Commit actual source-correlated execution, unit, position and publication intent atomically.

    Args:
        repository: Actual database implementation with the established wallet execution fence.
        request: Two separately retained original source receipts and actual historical authority.
        provenance: Actual local financial knowledge clock and independent transaction identity.

    Returns:
        Committed original or new financial identities. No message is published by this writer.
    """
    request = FuturesPositionRecordingRequest.model_validate(request)
    provenance = SnapshotWriteProvenance.model_validate(provenance)
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await repository._acquire_execution_fence_lock(session, request.wallet_public_id)
        source = await bind_futures_fill_source(session, request, provenance.timestamp)
        result = await _original_recording(session, source, request, provenance)
        if result is None:
            result = await _record_new_position(repository, session, request, source, provenance)
        await session.commit()
        return result
