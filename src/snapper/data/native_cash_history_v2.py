"""Replay actual native-v2 cash rows with source, owner, sidecar, anchor and claim conservation.

Each currency remains a partial observed chain. No modeled fill profit, fee
component, collateral valuation or missing currency creates cash. These readers
never upgrade legacy financial history or certify a complete account inventory.
"""

from dataclasses import dataclass
from datetime import datetime

from pydantic import TypeAdapter
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.portfolio.native_financial_payloads_v2 import FuturesNativeCashAnchorV2
from snapper.application.portfolio.native_financial_payloads_v2 import FuturesNativeCashEventV2
from snapper.application.portfolio.native_financial_payloads_v2 import (
    canonical_native_financial_payload_v2,
)
from snapper.application.portfolio.native_financial_payloads_v2 import (
    native_financial_payload_digest_v2,
)
from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.core.execution_route_authority import account_scope_digest
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance
from snapper.data.accounting_native_source_store_v2 import get_bound_native_source_claim_v2
from snapper.data.models import ContractAccountingAnchor
from snapper.data.models import ContractAccountingEvent
from snapper.data.models import is_open_ended_version
from snapper.data.native_financial_event_binding_v2 import bind_native_financial_event_material
from snapper.data.native_financial_source_binding_v2 import BoundNativeFinancialSource
from snapper.data.native_financial_source_binding_v2 import NativeFinancialBindingError
from snapper.data.native_financial_source_binding_v2 import bind_native_financial_source
from snapper.data.native_financial_source_binding_v2 import require_native_financial_binding

_INSTANT: TypeAdapter[datetime] = TypeAdapter(EvidenceInstant)


@dataclass(frozen=True, slots=True)
class BoundNativeCashEventV2:
    """An actual fully rebound cash event, retaining its incomplete original currency seed."""

    row: ContractAccountingEvent
    payload: FuturesNativeCashEventV2
    source: BoundNativeFinancialSource
    anchor: FuturesNativeCashAnchorV2


def native_cash_anchor_columns_v2(payload: FuturesNativeCashAnchorV2) -> dict[str, object]:
    """Project the actual partial old-balance source without inventing execution alignment.

    Args:
        payload: Native-v2 anchor containing its exact original policy and private source proof.

    Returns:
        All immutable0058 anchor mirrors, with unavailable prefix and a NULL sequence.
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
    session: AsyncSession, event: FuturesNativeCashEventV2, known_at: datetime
) -> FuturesNativeCashAnchorV2:
    """Require the actual retained seed and its original policy interval, independently of current policy."""
    row = await session.scalar(
        select(ContractAccountingAnchor).where(
            ContractAccountingAnchor.public_id == event.operands.anchor_public_id
        )
    )
    if row is None:
        raise NativeFinancialBindingError("native_financial_cash_anchor_missing")
    payload = FuturesNativeCashAnchorV2.model_validate_json(row.canonical_payload)
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
            for name, value in native_cash_anchor_columns_v2(payload).items()
        ),
        "native_financial_cash_anchor_projection_or_clock_mismatch",
    )
    source = await bind_native_financial_source(session, payload.source)
    require_native_financial_binding(
        source.entry == payload.operands.source_entry
        and (payload.operands.scope, payload.operands.native_currency)
        == (event.operands.scope, event.operands.native_currency)
        and payload.source.authority.venue_account_id == event.source.authority.venue_account_id,
        "native_financial_cash_anchor_original_source_mismatch",
    )
    return payload


def latest_native_currency_event_v2(
    events: tuple[BoundNativeCashEventV2, ...] | list[BoundNativeCashEventV2], currency: str
) -> BoundNativeCashEventV2 | None:
    """Find the latest observed balance in one exact native currency.

    Args:
        events: Already fully bound cash prefix from the actual requested scope.
        currency: Exact native source denomination; no conversion or case alias is inferred.

    Returns:
        Its latest event, absent when that currency's balance is unknown.
    """
    return next(
        (item for item in reversed(events) if item.payload.operands.native_currency == currency),
        None,
    )


def _require_conservation(
    events: list[BoundNativeCashEventV2],
    row: ContractAccountingEvent,
    payload: FuturesNativeCashEventV2,
    anchor: FuturesNativeCashAnchorV2,
) -> None:
    """Conserve native balances and known owner identity while permitting unrelated ledger interleaving."""
    value = payload.operands
    if events:
        require_native_financial_binding(
            events[-1].row.ledger_sequence < row.ledger_sequence
            and events[-1].row.timestamp <= row.timestamp
            and events[-1].payload.source.authority.venue_account_id
            == payload.source.authority.venue_account_id,
            "native_financial_cash_account_history_discontinuous",
        )
    previous = latest_native_currency_event_v2(events, value.native_currency)
    if previous is None:
        require_native_financial_binding(
            anchor.operands.balance == value.cash_before
            and anchor.operands.source_entry == value.source_entry
            and anchor.operands.source_receipt_public_id == value.raw_receipt_public_id
            and anchor.source == payload.source,
            "native_financial_cash_first_source_anchor_mismatch",
        )
        return
    earlier = previous.payload.operands
    require_native_financial_binding(
        earlier.anchor_public_id == value.anchor_public_id
        and earlier.cash_after == value.cash_before
        and earlier.source_entry.source_log_id < value.source_entry.source_log_id,
        "native_financial_cash_currency_history_discontinuous",
    )


async def load_native_cash_history_v2(
    session: AsyncSession, scope: ExecutionAccountScope, known_at: datetime
) -> tuple[BoundNativeCashEventV2, ...]:
    """Rebind the actual cash prefix for one explicit account environment and margin profile.

    Other financial kinds may interleave in the global local sequence. Missing
    currencies stay unknown, and legacy cash history requires explicit migration.
    Every selected row is verified against its original recorded time, sources,
    partial anchor and exclusive native source claim before returning any value.

    Args:
        session: Actual caller-owned historical or wallet-fenced financial transaction.
        scope: Exact actual account/margin/environment requested by the reader.
        known_at: Retained-knowledge cutoff; provider occurrence times do not move rows into the past.

    Returns:
        Fully source-bound observed cash events; the account inventory remains incomplete.
    """
    actual_scope = ExecutionAccountScope.model_validate(scope)
    clock = _INSTANT.validate_python(known_at)
    with session.no_autoflush:
        rows = (
            await session.scalars(
                select(ContractAccountingEvent)
                .where(
                    ContractAccountingEvent.account_scope_digest
                    == account_scope_digest(actual_scope),
                    ContractAccountingEvent.kind == "native_cash",
                    ContractAccountingEvent.timestamp <= clock,
                )
                .order_by(ContractAccountingEvent.ledger_sequence)
            )
        ).all()
        result: list[BoundNativeCashEventV2] = []
        for row in rows:
            material = await bind_native_financial_event_material(session, row, clock)
            payload = material.payload
            if not isinstance(payload, FuturesNativeCashEventV2):
                raise NativeFinancialBindingError("native_financial_cash_actual_kind_mismatch")
            require_native_financial_binding(
                payload.operands.scope == actual_scope,
                "native_financial_cash_requested_scope_mismatch",
            )
            anchor = await _bound_anchor(session, payload, row.timestamp)
            _require_conservation(result, row, payload, anchor)
            await get_bound_native_source_claim_v2(session, row.public_id, row.timestamp)
            result.append(BoundNativeCashEventV2(row, payload, material.source, anchor))
    return tuple(result)


async def get_bound_native_cash_event_v2(
    session: AsyncSession, public_id: str, known_at: datetime
) -> BoundNativeCashEventV2:
    """Fetch a real cash event only after replaying its complete actual scoped cash prefix.

    Args:
        session: Actual caller-owned read or financial transaction.
        public_id: Exact retained native-v2 financial event UUID.
        known_at: Required retained-knowledge cutoff.

    Returns:
        The actual full event with source, policy, sidecar, anchor and native claim conservation.
    """
    with session.no_autoflush:
        row = await session.scalar(
            select(ContractAccountingEvent).where(ContractAccountingEvent.public_id == public_id)
        )
        if row is None:
            raise NativeFinancialBindingError("native_financial_actual_cash_event_missing")
        payload = FuturesNativeCashEventV2.model_validate_json(row.canonical_payload)
        events = await load_native_cash_history_v2(session, payload.operands.scope, known_at)
        found = next((item for item in events if item.row.public_id == public_id), None)
        if found is None:
            raise NativeFinancialBindingError("native_financial_cash_event_not_known_at_cutoff")
        return found
