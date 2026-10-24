"""Bind native position operands to actual original execution, unit, fill and later account-log evidence."""

from dataclasses import dataclass
from datetime import datetime
from typing import Self

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import TypeAdapter
from pydantic import model_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.portfolio.futures_position_source import FuturesPositionSourceLeg
from snapper.application.portfolio.futures_position_source import native_futures_position_leg
from snapper.application.portfolio.native_financial_payloads_v2 import ContractPositionAnchorV2
from snapper.application.portfolio.native_financial_payloads_v2 import ContractPositionEventV2
from snapper.core.account_authority_reference import AccountAuthorityReference
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.execution_unit_snapshot import EvidenceUUID
from snapper.core.execution_unit_snapshot import ExecutionUnitSnapshotPayload
from snapper.core.execution_unit_snapshot import FillSnapshotTarget
from snapper.data.execution_unit_catalog import catalog_economic_context
from snapper.data.execution_unit_snapshot_history import get_bound_execution_unit_snapshot
from snapper.data.models import Execution
from snapper.data.models import ExecutionUnitSnapshot
from snapper.data.native_financial_source_binding_v2 import BoundNativeFinancialSource
from snapper.data.native_financial_source_binding_v2 import NativeFinancialBindingError
from snapper.data.native_financial_source_binding_v2 import bind_native_financial_receipt
from snapper.data.native_financial_source_binding_v2 import bind_native_financial_source
from snapper.data.native_financial_source_binding_v2 import require_native_financial_binding
from snapper.data.native_position_fill_binding_v2 import BoundOriginalPositionFillV2
from snapper.data.native_position_fill_binding_v2 import OriginalPositionFillRequestV2
from snapper.data.native_position_fill_binding_v2 import bind_original_position_fill_v2

type NativePositionPayloadV2 = ContractPositionAnchorV2 | ContractPositionEventV2
_POSITION: TypeAdapter[NativePositionPayloadV2] = TypeAdapter(NativePositionPayloadV2)
_INSTANT: TypeAdapter[datetime] = TypeAdapter(EvidenceInstant)


class NativePositionSourceRequestV2(BaseModel):
    """Actual original fill/unit and later native log references without caller-supplied balances."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        revalidate_instances="always",
        hide_input_in_errors=True,
    )

    fill: OriginalPositionFillRequestV2
    unit_snapshot_public_id: EvidenceUUID
    position_receipt_public_id: EvidenceUUID
    position_authority_reference: AccountAuthorityReference

    @model_validator(mode="after")
    def require_native_position_log_reference(self) -> Self:
        """Keep the later financial source explicitly native while preserving the original fill family.

        Returns:
            The same reference request awaiting actual source, unit and financial owner binding.
        """
        if self.position_authority_reference.family != "native_accounts_v1":
            raise ValueError(
                "Native position posting requires an explicit native account-log authority"
            )
        return self


@dataclass(frozen=True, slots=True)
class BoundNativePositionSourceV2:
    """Actual execution/unit/fill and later native log, with no current admission or synthetic source."""

    execution: Execution
    unit: ExecutionUnitSnapshotPayload
    fill: BoundOriginalPositionFillV2
    position: FuturesPositionSourceLeg
    financial_source: BoundNativeFinancialSource


async def _actual_execution(
    session: AsyncSession, unit: ExecutionUnitSnapshotPayload, known_at: datetime
) -> Execution:
    """Load the actual fill target after the full immutable unit/Event/Order/catalog reader succeeds."""
    target = unit.target
    if not isinstance(target, FillSnapshotTarget):
        raise NativeFinancialBindingError("native_position_actual_fill_unit_required")
    execution = await session.scalar(
        select(Execution).where(Execution.public_id == target.execution_public_id)
    )
    if execution is None:
        raise NativeFinancialBindingError("native_position_actual_execution_missing")
    require_native_financial_binding(
        execution.timestamp <= known_at,
        "native_position_execution_not_known_at_posting",
    )
    return execution


def _verify_fill_units(
    fill: BoundOriginalPositionFillV2, unit: ExecutionUnitSnapshotPayload
) -> None:
    """Bind exact source operands and whole original receipt without relying on a parsed float mirror."""
    actual = fill.fill
    context = catalog_economic_context(fill.catalog)
    require_native_financial_binding(
        (
            unit.scope.instrument_public_id,
            unit.venue_identity.wsname,
            unit.dimensions.side,
            unit.dimensions.signed_quantity,
            unit.dimensions.signed_quantity_unit,
            unit.dimensions.price,
            unit.dimensions.fee,
            unit.dimensions.fee_currency,
        )
        == (
            fill.catalog.instrument.public_id,
            actual.provider_symbol,
            actual.side,
            actual.quantity if actual.side == "buy" else actual.quantity.copy_negate(),
            context.native_order_quantity_unit,
            actual.price,
            actual.fee,
            actual.fee_currency,
        ),
        "native_position_unit_original_fill_operands_mismatch",
    )
    require_native_financial_binding(
        (
            unit.receipt.representation,
            unit.receipt.material,
            unit.receipt.endpoint_or_channel,
            unit.receipt.observed_at,
            unit.receipt.reported_quantity_unit,
        )
        == (
            "wire_bytes_base64",
            fill.raw.material,
            fill.raw.channel,
            fill.raw.observed_at,
            context.native_order_quantity_unit,
        ),
        "native_position_unit_original_receipt_mismatch",
    )


def _verify_position_source(
    payload: NativePositionPayloadV2, bound: BoundNativePositionSourceV2
) -> None:
    """Keep economic source identity and actual UID/environment/profile continuity explicit."""
    value = payload.operands
    fill = bound.fill
    source = bound.financial_source
    require_native_financial_binding(
        value.unit == bound.unit
        and value.scope == fill.authority.payload.scope == source.authority.payload.scope
        and source.ownership.account_uid == fill.ownership.account_uid
        and value.source_position == bound.position
        and value.source_alias_public_id == fill.alias.public_id
        and value.source_alias_timestamp == fill.alias.timestamp
        and source.entry.execution == fill.fill.execution_id
        and bound.execution.exec_id == fill.fill.execution_id
        and bound.execution.executed_at == fill.fill.occurred_at,
        "native_position_original_execution_or_later_log_mismatch",
    )
    if isinstance(payload, ContractPositionEventV2):
        event = payload.operands
        require_native_financial_binding(
            event.raw_receipt_public_id == payload.fill_ownership_reference.raw_receipt_public_id
            and event.position_receipt_public_id == source.ownership.reference.raw_receipt_public_id
            and event.execution_public_id == bound.execution.public_id
            and event.execution_scope_sequence == bound.execution.scope_sequence
            and event.execution_known_at == bound.execution.timestamp
            and event.venue_execution_id == fill.fill.execution_id
            and event.executed_at == fill.fill.occurred_at,
            "native_position_event_actual_target_mismatch",
        )


async def bind_native_position_receipt_v2(
    session: AsyncSession, request: NativePositionSourceRequestV2, known_at: datetime
) -> BoundNativePositionSourceV2:
    """Bind actual original targets and the later native log before current financial admission.

    Args:
        session: Actual caller-owned historical or wallet-fenced financial transaction.
        request: Actual retained original fill, unit and independently selected native log references.
        known_at: Local posting knowledge clock; the original fill uses actual Execution.timestamp.

    Returns:
        Complete original target/source material without creating financial rows or granting admission.
    """
    actual = NativePositionSourceRequestV2.model_validate(request)
    clock = _INSTANT.validate_python(known_at)
    unit = await get_bound_execution_unit_snapshot(session, actual.unit_snapshot_public_id)
    recorded_at = await session.scalar(
        select(ExecutionUnitSnapshot.timestamp).where(
            ExecutionUnitSnapshot.public_id == actual.unit_snapshot_public_id
        )
    )
    require_native_financial_binding(
        recorded_at is not None and recorded_at <= clock and unit.interpreted_at <= clock,
        "native_position_unit_not_retained_at_posting",
    )
    execution = await _actual_execution(session, unit, clock)
    fill = await bind_original_position_fill_v2(session, actual.fill, execution.timestamp)
    source = await bind_native_financial_receipt(
        session,
        actual.position_authority_reference,
        actual.position_receipt_public_id,
        clock,
    )
    _verify_fill_units(fill, unit)
    require_native_financial_binding(
        fill.authority.payload.scope == source.authority.payload.scope
        and fill.ownership.account_uid == source.ownership.account_uid
        and source.entry.execution == fill.fill.execution_id
        and execution.exec_id == fill.fill.execution_id
        and execution.executed_at == fill.fill.occurred_at,
        "native_position_original_execution_or_later_log_mismatch",
    )
    position = native_futures_position_leg(
        source.entry, fill.alias.exchange_symbol, fill.catalog.instrument.public_id
    )
    return BoundNativePositionSourceV2(execution, unit, fill, position, source)


async def bind_native_position_source_v2(
    session: AsyncSession, payload: NativePositionPayloadV2
) -> BoundNativePositionSourceV2:
    """Rebind one native position event or seed against every actual original target and source.

    Unit evidence can be retained after the original Execution and before the
    later posting. Original fill ownership must instead already be retained by
    Execution.timestamp. Account-log ownership/authority may legitimately
    arrive later, provided it is retained by the original posting clock.

    Args:
        session: Actual caller-owned historical or financial transaction.
        payload: Exact native wrapper containing separate original-fill and later-log references.

    Returns:
        Actual source and target rows, without current policy admission or any new posting.
    """
    actual = _POSITION.validate_python(payload)
    value = actual.operands
    source = await bind_native_financial_source(session, actual.source)
    bound = await bind_native_position_receipt_v2(
        session,
        NativePositionSourceRequestV2(
            fill=OriginalPositionFillRequestV2(
                wallet_public_id=value.scope.wallet_public_id,
                raw_receipt_public_id=actual.fill_ownership_reference.raw_receipt_public_id,
                authority_reference=actual.fill_authority_reference,
                ownership_reference=actual.fill_ownership_reference,
            ),
            unit_snapshot_public_id=value.unit_snapshot_public_id,
            position_receipt_public_id=actual.source.ownership_reference.raw_receipt_public_id,
            position_authority_reference=actual.source.authority_reference,
        ),
        actual.source.recorded_at,
    )
    require_native_financial_binding(
        source == bound.financial_source,
        "native_position_rebound_financial_source_changed",
    )
    _verify_position_source(actual, bound)
    return bound
