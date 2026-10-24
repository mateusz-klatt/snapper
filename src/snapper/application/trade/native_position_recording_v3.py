"""Commit actual native targets, original admitted units, position and notification together.

This seam consumes only already committed original private observations. It
never sends an order, resolves an unknown fill, invents an admission, converts
position components to cash or publishes a legacy spot fill. Missing account,
owner, sidecar or original admission evidence refuses before outer commit.
"""

from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from decimal import Decimal
from math import isfinite
from typing import Self
from uuid import UUID
from uuid import uuid5

from pydantic import model_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.trade.native_fill_units_v2 import NativeFillUnitCaptureRequestV2
from snapper.application.trade.native_fill_units_v2 import capture_bound_native_fill_units_v2
from snapper.application.trade.native_fill_units_v2 import native_fill_receipt_v2
from snapper.core.account_authority_reference import AccountAuthorityReference
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.core.execution_route_authority import account_scope_digest
from snapper.core.execution_unit_snapshot import EvidenceUUID
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance
from snapper.core.native_fill_unit_admission import NativeUnitRecordReference
from snapper.data.account_bound_submission_admission_store import (
    get_bound_account_submission_admission,
)
from snapper.data.contract_position_ledger_store import ContractPositionLedgerWriteResult
from snapper.data.models import ContractAccountingEvent
from snapper.data.models import Execution
from snapper.data.models import Order
from snapper.data.models import TradeCommand
from snapper.data.native_account_authority_store import NativeAuthorityFreshnessPolicy
from snapper.data.native_admitted_position_guards import require_native_admitted_position_guards
from snapper.data.native_financial_event_family import AdmittedNativeEventMaterialV3
from snapper.data.native_financial_event_family import admitted_position_request_v3
from snapper.data.native_financial_event_family import get_native_financial_event_family
from snapper.data.native_financial_publication_store_v2 import BoundNativePositionPublicationV2
from snapper.data.native_financial_publication_store_v2 import (
    append_bound_native_position_publication_v2,
)
from snapper.data.native_financial_source_binding_v2 import NativeFinancialBindingError
from snapper.data.native_financial_source_binding_v2 import require_native_financial_binding
from snapper.data.native_position_admission_binding_v3 import NativePositionSourceRequestV3
from snapper.data.native_position_fill_binding_v2 import BoundOriginalPositionFillV2
from snapper.data.native_position_fill_binding_v2 import OriginalPositionFillRequestV2
from snapper.data.native_position_fill_binding_v2 import bind_original_position_fill_v2
from snapper.data.native_position_store_v3 import append_bound_native_position_v3
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import ExecutionInsertRow
from snapper.data.repository_types import VenueEventInsertRow


class NativePositionRecordingRequestV3(OriginalPositionFillRequestV2):
    """Actual original fill, correlated order and explicit durable account-bound admission."""

    order_public_id: EvidenceUUID
    submission_admission: NativeUnitRecordReference
    position_receipt_public_id: EvidenceUUID
    position_authority_reference: AccountAuthorityReference

    @model_validator(mode="after")
    def require_native_log_family(self) -> Self:
        """Keep later log authority explicitly tagged before any actual graph is interpreted.

        Returns:
            The unchanged strict source request, without creating permission or financial facts.
        """
        if self.position_authority_reference.family != "native_accounts_v1":
            raise ValueError("Native position recording requires a native later-log authority")
        return self


@dataclass(frozen=True, slots=True)
class RecordedNativePositionV3:
    """Actual committed target coordinates and recoverable notification, without a cash delta."""

    execution_public_id: str
    unit_snapshot_public_id: str
    position: ContractPositionLedgerWriteResult
    publication: BoundNativePositionPublicationV2
    created: bool


@dataclass(frozen=True, slots=True)
class _NativeRecordingContextV3:
    """Trusted application policy and actual post-wallet knowledge for this one transaction."""

    identity: ExecutionEvidenceIdentity
    policy: NativeAuthorityFreshnessPolicy
    known_at: datetime


def _stamp(
    identity: ExecutionEvidenceIdentity, kind: str, known_at: datetime
) -> SnapshotWriteProvenance:
    """Allocate stable transaction children without claiming that allocated IDs are persisted rows."""
    return SnapshotWriteProvenance(
        public_id=str(uuid5(UUID(identity.public_id), kind)),
        session_id=identity.session_id,
        sequence_id=identity.sequence_id,
        timestamp=known_at,
    )


def _original_request(request: NativePositionRecordingRequestV3) -> OriginalPositionFillRequestV2:
    """Select exact original source references without passing an extended model as a V2 authority."""
    return OriginalPositionFillRequestV2(
        wallet_public_id=request.wallet_public_id,
        raw_receipt_public_id=request.raw_receipt_public_id,
        authority_reference=request.authority_reference,
        ownership_reference=request.ownership_reference,
    )


def _mirror(value: Decimal) -> float:
    """Retain only a finite compatible legacy display mirror alongside exact raw decimal fields."""
    result = float(value)
    require_native_financial_binding(
        isfinite(result) and (value == 0 or result != 0),
        "native_recording_display_mirror_unrepresentable",
    )
    return result


async def _actual_order_command(
    session: AsyncSession,
    request: NativePositionRecordingRequestV3,
    source: BoundOriginalPositionFillV2,
    known_at: datetime,
) -> tuple[Order, TradeCommand]:
    """Use the explicit admission's actual command rather than inferring an attempt from a CID."""
    admission = await get_bound_account_submission_admission(
        session, request.submission_admission.public_id
    )
    reference = request.submission_admission
    require_native_financial_binding(
        (admission.public_id, admission.content_digest, admission.recorded_at)
        == (reference.public_id, reference.content_digest, reference.recorded_at)
        and admission.recorded_at <= known_at,
        "native_recording_original_admission_reference_changed",
    )
    material = admission.payload.units.request_material
    command = await session.scalar(
        select(TradeCommand).where(
            TradeCommand.public_id == material.command_public_id,
            TradeCommand.timestamp <= admission.payload.units.interpreted_at,
            TradeCommand.known_to > admission.payload.units.interpreted_at,
        )
    )
    order = await session.scalar(
        select(Order).where(
            Order.public_id == request.order_public_id,
            Order.wallet_public_id == request.wallet_public_id,
            Order.instrument_public_id == source.catalog.instrument.public_id,
            Order.mode == source.raw.scope.mode,
            Order.exchange_order_id == source.fill.order_id,
            Order.timestamp <= known_at,
            Order.known_to > known_at,
        )
    )
    if command is None or order is None:
        raise NativeFinancialBindingError(
            "native_recording_actual_order_or_admitted_command_missing"
        )
    require_native_financial_binding(
        command.command_type in {"create", "submit"}
        and order.client_order_id == material.client_order_id
        and command.client_order_id == material.client_order_id
        and material.wallet_public_id == request.wallet_public_id
        and material.native_symbol == source.catalog.symbol.native_symbol
        and material.side == source.fill.side
        and (
            source.fill.client_order_id is None
            or source.fill.client_order_id == material.client_order_id
        ),
        "native_recording_actual_correlation_contradiction",
    )
    return order, command


def _execution_values(
    source: BoundOriginalPositionFillV2, order: Order, stamp: SnapshotWriteProvenance
) -> ExecutionInsertRow:
    """Preserve exact original operands and provider time separately from real local knowledge."""
    fill = source.fill
    return {
        "wallet_public_id": source.raw.scope.wallet_public_id,
        "order_public_id": order.public_id,
        "timestamp": stamp.timestamp,
        "executed_at": fill.occurred_at,
        "session_id": stamp.session_id,
        "sequence_id": stamp.sequence_id,
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


def _event_values(
    source: BoundOriginalPositionFillV2, command: TradeCommand, stamp: SnapshotWriteProvenance
) -> VenueEventInsertRow:
    """Retain the actual original whole frame for this single source child, without cumulative gaps."""
    fill = source.fill
    return {
        "wallet_public_id": source.raw.scope.wallet_public_id,
        "command_public_id": command.public_id,
        "event_type": "fill_observed",
        "shard_key": command.shard_key,
        "exchange": source.raw.scope.exchange,
        "instrument": source.catalog.symbol.native_symbol,
        "mode": source.raw.scope.mode,
        "timestamp": stamp.timestamp,
        "received_at": source.raw.observed_at,
        "venue_timestamp": fill.occurred_at,
        "session_id": stamp.session_id,
        "sequence_id": stamp.sequence_id,
        "side": fill.side,
        "fill_size": _mirror(fill.quantity),
        "fill_price": _mirror(fill.price),
        "fee": _mirror(fill.fee),
        "fee_asset": fill.fee_currency,
        "exec_id": fill.execution_id,
        "exchange_order_id": fill.order_id,
        "client_order_id": command.client_order_id,
        "venue_client_id": fill.client_order_id,
        "payload_json": native_fill_receipt_v2(source).model_dump_json(),
    }


async def _original_recording(
    session: AsyncSession,
    request: NativePositionRecordingRequestV3,
    source: BoundOriginalPositionFillV2,
    context: _NativeRecordingContextV3,
) -> RecordedNativePositionV3 | None:
    """Rebind actual incoming facts even when returning an original admitted economic event."""
    stamp = _stamp(context.identity, "position", context.known_at)
    scope = source.authority.payload.scope
    row = await session.scalar(
        select(ContractAccountingEvent).where(
            ContractAccountingEvent.account_scope_digest == account_scope_digest(scope),
            ContractAccountingEvent.kind == "contract_position_fill",
            ContractAccountingEvent.source_identity
            == f"kraken_futures:fill:{source.fill.execution_id}",
        )
    )
    if row is None:
        return None
    material = await get_native_financial_event_family(session, row.public_id, stamp.timestamp)
    if not isinstance(material, AdmittedNativeEventMaterialV3):
        raise NativeFinancialBindingError("native_recording_prior_admission_migration_required")
    original = material.admitted.original
    transport = {"subscription_sequence", "target_json_pointer", "feed"}
    require_native_financial_binding(
        original.execution.order_public_id == request.order_public_id
        and original.evidence.submission_admission == request.submission_admission
        and original.source.fill.model_dump(exclude=transport)
        == source.fill.model_dump(exclude=transport)
        and original.source.ownership.account_uid == source.ownership.account_uid,
        "native_recording_source_redelivery_conflict",
    )
    retained = admitted_position_request_v3(material.envelope)
    position = await append_bound_native_position_v3(
        session,
        NativePositionSourceRequestV3(
            fill=retained.fill,
            unit_snapshot_public_id=retained.unit_snapshot_public_id,
            position_receipt_public_id=request.position_receipt_public_id,
            position_authority_reference=request.position_authority_reference,
            fill_unit_admission=retained.fill_unit_admission,
        ),
        stamp,
        context.policy,
    )
    publication = await append_bound_native_position_publication_v2(
        session, position.public_id, _stamp(context.identity, "publication", context.known_at)
    )
    return RecordedNativePositionV3(
        original.execution.public_id, retained.unit_snapshot_public_id, position, publication, False
    )


async def _new_recording(
    repository: SQLAlchemyRepository,
    session: AsyncSession,
    request: NativePositionRecordingRequestV3,
    source: BoundOriginalPositionFillV2,
    context: _NativeRecordingContextV3,
) -> RecordedNativePositionV3:
    """Stage every actual target, original unit, position and notification before one outer commit."""
    occupied = await session.scalar(
        select(Execution.id)
        .where(
            Execution.wallet_public_id == request.wallet_public_id,
            Execution.exchange == source.raw.scope.exchange,
            Execution.mode == source.raw.scope.mode,
            Execution.exec_id == source.fill.execution_id,
        )
        .limit(1)
    )
    require_native_financial_binding(
        occupied is None, "native_recording_existing_execution_without_admitted_position"
    )
    order, command = await _actual_order_command(session, request, source, context.known_at)
    execution = await repository.insert_execution_in_transaction(
        session,
        _execution_values(source, order, _stamp(context.identity, "execution", context.known_at)),
    )
    event = await repository.insert_venue_event_in_transaction(
        session,
        _event_values(source, command, _stamp(context.identity, "venue_event", context.known_at)),
    )
    original = _original_request(request)
    unit = await capture_bound_native_fill_units_v2(
        session,
        NativeFillUnitCaptureRequestV2(
            **original.model_dump(mode="python"),
            execution_public_id=execution.public_id,
            venue_event_public_id=event.public_id,
            submission_admission=request.submission_admission,
        ),
        _stamp(context.identity, "unit", context.known_at),
    )
    position = await append_bound_native_position_v3(
        session,
        NativePositionSourceRequestV3(
            fill=original,
            unit_snapshot_public_id=unit.result.public_id,
            position_receipt_public_id=request.position_receipt_public_id,
            position_authority_reference=request.position_authority_reference,
            fill_unit_admission=unit.evidence,
        ),
        _stamp(context.identity, "position", context.known_at),
        context.policy,
    )
    publication = await append_bound_native_position_publication_v2(
        session, position.public_id, _stamp(context.identity, "publication", context.known_at)
    )
    return RecordedNativePositionV3(
        execution.public_id, unit.result.public_id, position, publication, True
    )


async def record_native_position_v3(
    repository: SQLAlchemyRepository,
    request: NativePositionRecordingRequestV3,
    identity: ExecutionEvidenceIdentity,
    policy: NativeAuthorityFreshnessPolicy,
) -> RecordedNativePositionV3:
    """Commit actual correlated native financial evidence without a legacy spot fold or source claim split.

    The caller supplies trusted application freshness configuration and the
    exact retained original admission. It cannot manufacture either from a
    wire amount, command CID, source label or a nominal current route. Exact
    historical redelivery rebinds incoming facts and retains the original
    targets even after owner revocation; only a genuinely new financial fact
    enters the locked current owner-policy path.

    Args:
        repository: Actual real transaction, contiguous Execution allocator and wallet fence provider.
        request: Retained original fill, explicit order/admission and independent later-log references.
        identity: Stable original transaction identity, without a caller-supplied decision clock.
        policy: Trusted application-owned native authority freshness configuration.

    Returns:
        Actual committed Execution/unit/position/notification coordinates with an explicit creation flag.
    """
    actual = NativePositionRecordingRequestV3.model_validate(request)
    trace = ExecutionEvidenceIdentity.model_validate(identity)
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await repository._acquire_execution_fence_lock(session, actual.wallet_public_id)
        context = _NativeRecordingContextV3(trace, policy, datetime.now(UTC))
        await require_native_admitted_position_guards(session)
        source = await bind_original_position_fill_v2(
            session, _original_request(actual), context.known_at
        )
        result = await _original_recording(session, actual, source, context)
        if result is None:
            result = await _new_recording(repository, session, actual, source, context)
        await session.commit()
    return result
