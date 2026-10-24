"""Append real source-position transitions in the same transaction as execution and unit proof.

No command, execution, zero position or cash movement is fabricated here. The
first book seed comes from the exact native log associated with the actual
source fill. Publication and hold resolution remain the outer orchestrator's
responsibility after its complete financial transaction commits.
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Literal
from typing import cast
from uuid import NAMESPACE_URL
from uuid import uuid5

from pydantic import BaseModel
from pydantic import ConfigDict
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.portfolio.contract_position_ledger import CURRENT_POSITION_DECIMAL_POLICY
from snapper.application.portfolio.contract_position_ledger import ContractPositionAnchorPayload
from snapper.application.portfolio.contract_position_ledger import ContractPositionEventPayload
from snapper.application.portfolio.contract_position_ledger import ContractPositionValue
from snapper.application.portfolio.contract_position_ledger import canonical_position_payload
from snapper.application.portfolio.contract_position_ledger import fold_position_source
from snapper.application.portfolio.contract_position_ledger import position_payload_digest
from snapper.application.portfolio.futures_account_log import parse_futures_account_log
from snapper.application.portfolio.futures_position_source import FuturesPositionSourceLeg
from snapper.application.portfolio.futures_position_source import native_futures_position_leg
from snapper.application.trade.unit_snapshot_fill import BoundFuturesFillSource
from snapper.application.trade.unit_snapshot_fill import FillUnitCaptureRequest
from snapper.application.trade.unit_snapshot_fill import FuturesFillSourceRequest
from snapper.application.trade.unit_snapshot_fill import bind_futures_fill_source
from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.core.execution_route_authority import account_scope_digest
from snapper.core.execution_unit_snapshot import EvidenceUUID
from snapper.core.execution_unit_snapshot import ExecutionUnitSnapshotPayload
from snapper.core.execution_unit_snapshot import FillSnapshotTarget
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance
from snapper.data.accounting_native_source_store import claim_new_accounting_source
from snapper.data.accounting_native_source_store import get_bound_native_source_claim
from snapper.data.accounting_observation_binding import get_bound_accounting_observation
from snapper.data.contract_accounting_guards import acquire_contract_accounting_write_barrier
from snapper.data.execution_route_authority_store import get_bound_route_authority
from snapper.data.execution_unit_snapshot_history import get_bound_execution_unit_snapshot
from snapper.data.futures_cash_ledger_store import NativeCashAccountingRequest
from snapper.data.futures_cash_ledger_store import _require_account_continuity
from snapper.data.futures_cash_ledger_store import _verify_raw_route
from snapper.data.models import AccountingRawReceipt
from snapper.data.models import ContractAccountingAnchor
from snapper.data.models import ContractAccountingEvent
from snapper.data.models import Execution
from snapper.data.models import ExecutionRouteAuthority
from snapper.data.models import ExecutionUnitSnapshot


class ContractPositionAccountingError(ValueError):
    """Source, native account, execution or predecessor evidence cannot establish this position."""


class ContractPositionAccountingRequest(BaseModel):
    """Original actual fill/unit references plus a separately retained native position log."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    fill: FillUnitCaptureRequest
    unit_snapshot_public_id: EvidenceUUID
    position_receipt_public_id: EvidenceUUID
    position_route_authority_public_id: EvidenceUUID | None = None


@dataclass(frozen=True, slots=True)
class ContractPositionLedgerWriteResult:
    """Flushed source-position coordinates, usable only after the outer transaction commits."""

    public_id: str
    anchor_public_id: str
    ledger_sequence: int
    execution_public_id: str
    unit_snapshot_public_id: str
    instrument_public_id: str
    quantity_unit: Literal["contract_count"]
    settlement_currency: str
    quote_currency: str
    observed: ContractPositionValue
    derived: ContractPositionValue
    basis_status: Literal["equal", "source_precision_unproven"]
    created: bool


@dataclass(frozen=True, slots=True)
class _PositionSource:
    """Exact source and native position facts read under the caller's actual wallet fence."""

    request: ContractPositionAccountingRequest
    fill: BoundFuturesFillSource
    execution: Execution
    unit: ExecutionUnitSnapshotPayload
    position: FuturesPositionSourceLeg
    position_authority_public_id: str


@dataclass(frozen=True, slots=True)
class _PositionEvent:
    """One retained immutable event whose source and indexed copies are independently checked."""

    row: ContractAccountingEvent
    payload: ContractPositionEventPayload


def _require(condition: bool, reason: str) -> None:
    """Refuse interpretation while already committed source receipts and holds remain durable."""
    if not condition:
        raise ContractPositionAccountingError(reason)


def _verify_fill_units(source: BoundFuturesFillSource, unit: ExecutionUnitSnapshotPayload) -> None:
    """Keep actual source quantities, price and native fees bound to the immutable unit witness."""
    fill = source.fill
    signed = fill.quantity if fill.side == "buy" else fill.quantity.copy_negate()
    _require(
        (
            unit.scope.instrument_public_id,
            unit.venue_identity.wsname,
            unit.dimensions.side,
            unit.dimensions.signed_quantity,
            unit.dimensions.price,
            unit.dimensions.fee,
            unit.dimensions.fee_currency,
        )
        == (
            source.catalog.instrument.public_id,
            fill.provider_symbol,
            fill.side,
            signed,
            fill.price,
            fill.fee,
            fill.fee_currency,
        ),
        "position_fill_units_contradict_actual_source",
    )
    _require(
        (
            unit.receipt.representation,
            unit.receipt.material,
            unit.receipt.endpoint_or_channel,
            unit.receipt.observed_at,
        )
        == ("wire_bytes_base64", source.raw.material, source.raw.channel, source.raw.observed_at),
        "position_original_fill_receipt_mismatch",
    )


async def _native_position_source(
    session: AsyncSession,
    request: ContractPositionAccountingRequest,
    fill: BoundFuturesFillSource,
    known_at: datetime,
) -> tuple[FuturesPositionSourceLeg, str]:
    """Bind a separate native log to the same actual external account and exact source execution."""
    authority_id = (
        request.position_route_authority_public_id or request.fill.route_authority_public_id
    )
    authority = await get_bound_route_authority(session, authority_id)
    await _require_account_continuity(session, request.fill.route_authority_public_id, authority_id)
    raw = await get_bound_accounting_observation(session, request.position_receipt_public_id)
    _require(
        raw.observed_at <= known_at and authority.observed_at <= known_at,
        "position_log_evidence_not_yet_observed",
    )
    _verify_raw_route(
        NativeCashAccountingRequest(
            wallet_public_id=request.fill.wallet_public_id,
            raw_receipt_public_id=request.position_receipt_public_id,
            route_authority_public_id=authority_id,
        ),
        raw,
        authority,
    )
    entry = parse_futures_account_log(raw)
    _require(entry.execution == fill.fill.execution_id, "position_log_execution_identity_mismatch")
    _require(entry.margin_account == authority.scope.margin_account, "position_log_margin_mismatch")
    known = {
        authority.venue_account_id,
        fill.authority.venue_account_id,
        fill.fill.venue_account_id,
    }
    known.discard(None)
    _require(
        len(known) <= 1
        and (entry.venue_account_id is None or not known or entry.venue_account_id in known),
        "position_log_known_account_mismatch",
    )
    position = native_futures_position_leg(
        entry, fill.alias.exchange_symbol, fill.catalog.instrument.public_id
    )
    return position, authority_id


async def _source(
    session: AsyncSession, request: ContractPositionAccountingRequest, known_at: datetime
) -> _PositionSource:
    """Read genuine financial rows and their original source evidence without creating any target."""
    execution = (
        await session.execute(
            select(Execution).where(Execution.public_id == request.fill.execution_public_id)
        )
    ).scalar_one_or_none()
    if execution is None:
        raise ContractPositionAccountingError("position_actual_execution_missing")
    fill = await bind_futures_fill_source(
        session,
        FuturesFillSourceRequest(
            wallet_public_id=request.fill.wallet_public_id,
            raw_receipt_public_id=request.fill.raw_receipt_public_id,
            route_authority_public_id=request.fill.route_authority_public_id,
        ),
        execution.timestamp,
    )
    unit = await get_bound_execution_unit_snapshot(session, request.unit_snapshot_public_id)
    target = unit.target
    _require(
        isinstance(target, FillSnapshotTarget)
        and target.execution_public_id == execution.public_id
        and target.venue_event_public_id == request.fill.venue_event_public_id
        and execution.exec_id == fill.fill.execution_id
        and execution.executed_at == fill.fill.occurred_at,
        "position_actual_execution_target_mismatch",
    )
    _verify_fill_units(fill, unit)
    position, authority_id = await _native_position_source(session, request, fill, known_at)
    source = _PositionSource(request, fill, execution, unit, position, authority_id)
    await _verify_retained_clocks(session, source, known_at)
    return source


async def _verify_retained_clocks(
    session: AsyncSession, source: _PositionSource, known_at: datetime
) -> None:
    """Keep later native log/authority/unit knowledge separate from the original fill's clock."""
    raw_at = await session.scalar(
        select(AccountingRawReceipt.timestamp).where(
            AccountingRawReceipt.public_id == source.request.position_receipt_public_id
        )
    )
    authority_at = await session.scalar(
        select(ExecutionRouteAuthority.timestamp).where(
            ExecutionRouteAuthority.public_id == source.position_authority_public_id
        )
    )
    unit_at = await session.scalar(
        select(ExecutionUnitSnapshot.timestamp).where(
            ExecutionUnitSnapshot.public_id == source.request.unit_snapshot_public_id
        )
    )
    _require(
        all(
            instant is not None and instant <= known_at
            for instant in (raw_at, authority_at, unit_at)
        )
        and source.execution.timestamp <= known_at,
        "position_accounting_precedes_retained_source_knowledge",
    )


def _event_projection(row: ContractAccountingEvent, payload: ContractPositionEventPayload) -> None:
    """Verify every indexed account/source/target copy against the complete immutable position event."""
    scope = payload.scope
    _require(
        (
            row.account_scope_digest,
            row.wallet_public_id,
            row.exchange,
            row.mode,
            row.environment,
            row.margin_account,
            row.route_authority_public_id,
            row.anchor_public_id,
            row.raw_receipt_public_id,
            row.ledger_sequence,
            row.kind,
            row.source_identity,
            row.instrument_public_id,
            row.execution_public_id,
            row.unit_snapshot_public_id,
            row.content_digest,
            row.canonical_payload,
        )
        == (
            account_scope_digest(scope),
            scope.wallet_public_id,
            scope.exchange,
            scope.mode,
            scope.environment,
            scope.margin_account,
            payload.route_authority_public_id,
            payload.anchor_public_id,
            payload.raw_receipt_public_id,
            payload.ledger_sequence,
            payload.kind,
            f"kraken_futures:fill:{payload.venue_execution_id}",
            payload.instrument_public_id,
            payload.execution_public_id,
            payload.unit_snapshot_public_id,
            position_payload_digest(payload),
            canonical_position_payload(payload),
        ),
        "position_event_projection_mismatch",
    )


async def _anchor(
    session: AsyncSession, public_id: str, known_at: datetime
) -> ContractPositionAnchorPayload:
    """Read an exact source seed without recreating a missing opening position."""
    row = (
        await session.execute(
            select(ContractAccountingAnchor).where(ContractAccountingAnchor.public_id == public_id)
        )
    ).scalar_one_or_none()
    if row is None:
        raise ContractPositionAccountingError("position_source_anchor_missing")
    _require(row.timestamp <= known_at, "position_anchor_not_yet_known")
    payload = ContractPositionAnchorPayload.model_validate_json(row.canonical_payload)
    scope = payload.scope
    _require(
        (
            row.account_scope_digest,
            row.wallet_public_id,
            row.exchange,
            row.mode,
            row.environment,
            row.margin_account,
            row.route_authority_public_id,
            row.source_receipt_public_id,
            row.anchor_kind,
            row.source_execution_prefix_status,
            row.source_execution_scope_sequence,
            row.content_digest,
            row.canonical_payload,
        )
        == (
            account_scope_digest(scope),
            scope.wallet_public_id,
            scope.exchange,
            scope.mode,
            scope.environment,
            scope.margin_account,
            payload.route_authority_public_id,
            payload.source_receipt_public_id,
            payload.anchor_kind,
            payload.source_execution_prefix_status,
            payload.source_execution_scope_sequence,
            position_payload_digest(payload),
            canonical_position_payload(payload),
        ),
        "position_anchor_projection_mismatch",
    )
    return payload


async def _retained_events(
    session: AsyncSession, scope: ExecutionAccountScope, instrument_public_id: str
) -> tuple[_PositionEvent, ...]:
    """Replay exact source transitions in native booking order rather than inventing flat state."""
    rows = (
        await session.execute(
            select(ContractAccountingEvent)
            .where(
                ContractAccountingEvent.account_scope_digest == account_scope_digest(scope),
                ContractAccountingEvent.instrument_public_id == instrument_public_id,
                ContractAccountingEvent.kind == "contract_position_fill",
            )
            .order_by(ContractAccountingEvent.ledger_sequence)
        )
    ).scalars()
    result: list[_PositionEvent] = []
    for row in rows:
        payload = ContractPositionEventPayload.model_validate_json(row.canonical_payload)
        _event_projection(row, payload)
        request = ContractPositionAccountingRequest(
            fill=FillUnitCaptureRequest(
                wallet_public_id=scope.wallet_public_id,
                raw_receipt_public_id=payload.raw_receipt_public_id,
                route_authority_public_id=payload.route_authority_public_id,
                execution_public_id=payload.execution_public_id,
                venue_event_public_id=cast(
                    FillSnapshotTarget, payload.unit.target
                ).venue_event_public_id,
            ),
            unit_snapshot_public_id=payload.unit_snapshot_public_id,
            position_receipt_public_id=payload.position_receipt_public_id,
            position_route_authority_public_id=payload.position_route_authority_public_id,
        )
        source = await _source(session, request, row.timestamp)
        _require(
            payload.scope == scope
            and payload.scope == source.fill.authority.scope
            and payload.instrument_public_id == instrument_public_id
            and payload.unit == source.unit
            and payload.source_position == source.position
            and payload.execution_scope_sequence == source.execution.scope_sequence
            and payload.executed_at == source.fill.fill.occurred_at
            and payload.execution_known_at == source.execution.timestamp
            and payload.source_alias_public_id == source.fill.alias.public_id
            and payload.source_alias_timestamp == source.fill.alias.timestamp,
            "position_event_source_mismatch",
        )
        anchor = await _anchor(session, payload.anchor_public_id, row.timestamp)
        if result:
            await _require_account_continuity(
                session,
                result[-1].payload.route_authority_public_id,
                payload.route_authority_public_id,
            )
        _verify_chain(result, payload, anchor)
        await get_bound_native_source_claim(session, row.public_id)
        result.append(_PositionEvent(row, payload))
    return tuple(result)


def _verify_chain(
    events: list[_PositionEvent],
    payload: ContractPositionEventPayload,
    anchor: ContractPositionAnchorPayload,
) -> None:
    """Require the same native opening, immutable unit profile and conserved predecessor position."""
    _require(
        anchor.scope == payload.scope
        and anchor.instrument_public_id == payload.instrument_public_id
        and anchor.unit == payload.profile_unit,
        "position_anchor_scope_or_profile_changed",
    )
    if not events:
        _require(
            anchor.opening == payload.before
            and anchor.source_position == payload.source_position
            and anchor.source_receipt_public_id == payload.position_receipt_public_id,
            "position_first_event_source_seed_mismatch",
        )
        return
    previous = events[-1].payload
    _require(
        previous.anchor_public_id == payload.anchor_public_id
        and previous.observed_after == payload.before
        and previous.source_position.source_entry.source_log_id
        < payload.source_position.source_entry.source_log_id,
        "position_retained_native_chain_discontinuous",
    )


async def _insert_anchor(
    session: AsyncSession, source: _PositionSource, provenance: SnapshotWriteProvenance
) -> tuple[str, ContractPositionAnchorPayload]:
    """Seed this instrument from its exact observed native old position and actual unit profile."""
    scope = source.fill.authority.scope
    public_id = str(
        uuid5(
            NAMESPACE_URL,
            f"snapper:position-anchor:v1:{account_scope_digest(scope)}:{source.request.position_receipt_public_id}",
        )
    )
    payload = ContractPositionAnchorPayload(
        scope=scope,
        instrument_public_id=source.unit.scope.instrument_public_id,
        route_authority_public_id=source.request.fill.route_authority_public_id,
        position_route_authority_public_id=source.position_authority_public_id,
        source_receipt_public_id=source.request.position_receipt_public_id,
        unit_snapshot_public_id=source.request.unit_snapshot_public_id,
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
    session.add(
        ContractAccountingAnchor(
            **{**provenance.model_dump(), "public_id": public_id},
            **scope.model_dump(),
            account_scope_digest=account_scope_digest(scope),
            route_authority_public_id=payload.route_authority_public_id,
            source_receipt_public_id=payload.source_receipt_public_id,
            anchor_kind=payload.anchor_kind,
            source_execution_prefix_status=payload.source_execution_prefix_status,
            source_execution_scope_sequence=payload.source_execution_scope_sequence,
            content_digest=position_payload_digest(payload),
            canonical_payload=canonical_position_payload(payload),
        )
    )
    await session.flush()
    return public_id, payload


def _result(item: _PositionEvent, *, created: bool) -> ContractPositionLedgerWriteResult:
    """Expose exact retained ledger coordinates without releasing source or reconciliation holds."""
    payload = item.payload
    return ContractPositionLedgerWriteResult(
        public_id=item.row.public_id,
        anchor_public_id=payload.anchor_public_id,
        ledger_sequence=payload.ledger_sequence,
        execution_public_id=payload.execution_public_id,
        unit_snapshot_public_id=payload.unit_snapshot_public_id,
        instrument_public_id=payload.instrument_public_id,
        quantity_unit="contract_count",
        settlement_currency=cast(str, payload.unit.dimensions.settlement_currency),
        quote_currency=payload.unit.dimensions.quote_currency,
        observed=payload.observed_after,
        derived=payload.derived_after,
        basis_status=payload.basis_status,
        created=created,
    )


def _same_position_source(
    first: FuturesPositionSourceLeg, second: FuturesPositionSourceLeg
) -> bool:
    """Compare native source facts while allowing an original event's transport child to move."""
    excluded: dict[str, set[str]] = {"source_entry": {"target_json_pointer"}}
    return first.model_dump(exclude=excluded) == second.model_dump(exclude=excluded)


async def _append_event(
    session: AsyncSession,
    source: _PositionSource,
    anchor: tuple[str, ContractPositionAnchorPayload],
    before: ContractPositionValue,
    provenance: SnapshotWriteProvenance,
) -> _PositionEvent:
    """Flush one real source fill's position outcome using the account-wide ledger sequence."""
    scope = source.fill.authority.scope
    maximum = await session.scalar(
        select(func.max(ContractAccountingEvent.ledger_sequence)).where(
            ContractAccountingEvent.account_scope_digest == account_scope_digest(scope)
        )
    )
    derived, observed = fold_position_source(before, source.unit, source.position, anchor[1].unit)
    status: Literal["equal", "source_precision_unproven"] = (
        "equal" if derived.entry_price == observed.entry_price else "source_precision_unproven"
    )
    payload = ContractPositionEventPayload(
        decimal_policy=CURRENT_POSITION_DECIMAL_POLICY,
        scope=scope,
        instrument_public_id=source.unit.scope.instrument_public_id,
        route_authority_public_id=source.request.fill.route_authority_public_id,
        position_route_authority_public_id=source.position_authority_public_id,
        anchor_public_id=anchor[0],
        raw_receipt_public_id=source.request.fill.raw_receipt_public_id,
        position_receipt_public_id=source.request.position_receipt_public_id,
        execution_public_id=source.execution.public_id,
        execution_scope_sequence=source.execution.scope_sequence,
        unit_snapshot_public_id=source.request.unit_snapshot_public_id,
        source_alias_public_id=source.fill.alias.public_id,
        source_alias_timestamp=source.fill.alias.timestamp,
        venue_execution_id=source.fill.fill.execution_id,
        executed_at=source.fill.fill.occurred_at,
        execution_known_at=source.execution.timestamp,
        ledger_sequence=int(maximum or 0) + 1,
        unit=source.unit,
        profile_unit=anchor[1].unit,
        source_position=source.position,
        before=before,
        derived_after=derived,
        observed_after=observed,
        basis_status=status,
    )
    row = ContractAccountingEvent(
        **provenance.model_dump(),
        **scope.model_dump(),
        account_scope_digest=account_scope_digest(scope),
        route_authority_public_id=payload.route_authority_public_id,
        anchor_public_id=payload.anchor_public_id,
        raw_receipt_public_id=payload.raw_receipt_public_id,
        ledger_sequence=payload.ledger_sequence,
        kind=payload.kind,
        source_identity=f"kraken_futures:fill:{payload.venue_execution_id}",
        instrument_public_id=payload.instrument_public_id,
        execution_public_id=payload.execution_public_id,
        unit_snapshot_public_id=payload.unit_snapshot_public_id,
        content_digest=position_payload_digest(payload),
        canonical_payload=canonical_position_payload(payload),
    )
    session.add(row)
    await claim_new_accounting_source(session, row)
    return _PositionEvent(row, payload)


async def append_bound_contract_position(
    session: AsyncSession,
    request: ContractPositionAccountingRequest,
    provenance: SnapshotWriteProvenance,
) -> ContractPositionLedgerWriteResult:
    """Join the actual execution/unit/outbox transaction without an inner commit or publication.

    Args:
        session: Existing caller-owned financial transaction with the actual wallet fence held.
        request: Original real fill, captured unit and paired native position-log references.
        provenance: Stable local event identity and actual accounting knowledge clock.

    Returns:
        Flushed position-only outcome. The caller must commit the complete transaction
        before publication and must independently resolve cash/reconciliation holds.
    """
    binding = ContractPositionAccountingRequest.model_validate(request)
    stamp = SnapshotWriteProvenance.model_validate(provenance)
    await acquire_contract_accounting_write_barrier(session)
    source = await _source(session, binding, stamp.timestamp)
    _require(
        stamp.timestamp >= source.unit.interpreted_at,
        "position_accounting_knowledge_precedes_unit_interpretation",
    )
    scope = source.fill.authority.scope
    events = await _retained_events(session, scope, source.unit.scope.instrument_public_id)
    existing = next(
        (item for item in events if item.payload.execution_public_id == source.execution.public_id),
        None,
    )
    if existing is not None:
        _require(
            _same_position_source(existing.payload.source_position, source.position)
            and existing.payload.unit_snapshot_public_id == binding.unit_snapshot_public_id,
            "position_source_execution_conflict",
        )
        return _result(existing, created=False)
    latest = (
        await session.execute(
            select(ContractAccountingEvent)
            .where(ContractAccountingEvent.account_scope_digest == account_scope_digest(scope))
            .order_by(ContractAccountingEvent.ledger_sequence.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    if latest is not None:
        _require(
            latest.timestamp <= stamp.timestamp,
            "position_accounting_knowledge_precedes_account_ledger",
        )
        await _require_account_continuity(
            session, latest.route_authority_public_id, binding.fill.route_authority_public_id
        )
    if events:
        previous = events[-1].payload
        _require(
            previous.source_position.source_entry.source_log_id
            < source.position.source_entry.source_log_id,
            "position_native_source_sequence_discontinuous",
        )
        anchor = (
            previous.anchor_public_id,
            await _anchor(session, previous.anchor_public_id, stamp.timestamp),
        )
        before = previous.observed_after
    else:
        anchor = await _insert_anchor(session, source, stamp)
        before = anchor[1].opening
    item = await _append_event(session, source, anchor, before, stamp)
    return _result(item, created=True)


async def load_observed_contract_position(
    session: AsyncSession, scope: ExecutionAccountScope, instrument_public_id: str
) -> ContractPositionLedgerWriteResult | None:
    """Reconstruct the actual source position without using a synthetic flat checkpoint.

    Args:
        session: Caller-owned read/replay transaction, granting no current spending authority.
        scope: Exact actual account/environment/margin namespace.
        instrument_public_id: Immutable actual source instrument UUID.

    Returns:
        Last verified native position or None when no source seed exists. Absence
        never means zero position, zero cash or permission to create an order.
    """
    events = await _retained_events(session, scope, instrument_public_id)
    return _result(events[-1], created=False) if events else None


async def get_bound_contract_position_event(
    session: AsyncSession, public_id: str
) -> ContractPositionEventPayload:
    """Verify the real retained event, unit/source targets and complete native predecessor chain.

    Args:
        session: Caller-owned financial/outbox or replay transaction.
        public_id: Exact immutable position-event UUID, including an uncommitted caller-owned row.

    Returns:
        The actual independently verified financial event, without granting account readiness.

    Raises:
        ContractPositionAccountingError: If the actual event is absent or its proof is inconsistent.
    """
    row = (
        await session.execute(
            select(ContractAccountingEvent).where(ContractAccountingEvent.public_id == public_id)
        )
    ).scalar_one_or_none()
    if row is None:
        raise ContractPositionAccountingError("position_event_missing")
    payload = ContractPositionEventPayload.model_validate_json(row.canonical_payload)
    _event_projection(row, payload)
    events = await _retained_events(session, payload.scope, payload.instrument_public_id)
    return {item.row.public_id: item.payload for item in events}[public_id]


async def find_bound_contract_position_event(
    session: AsyncSession, scope: ExecutionAccountScope, venue_execution_id: str
) -> ContractPositionEventPayload | None:
    """Find a source fill's original financial event before creating any duplicate execution row.

    Args:
        session: Caller-owned transaction; the outer financial writer must hold the wallet fence.
        scope: Exact actual account/environment/margin namespace.
        venue_execution_id: Opaque exact provider fill identity, never a synthesized cumulative gap.

    Returns:
        The fully verified existing event or None, independently of later raw observation UUIDs.
    """
    row = (
        await session.execute(
            select(ContractAccountingEvent).where(
                ContractAccountingEvent.account_scope_digest == account_scope_digest(scope),
                ContractAccountingEvent.kind == "contract_position_fill",
                ContractAccountingEvent.source_identity
                == f"kraken_futures:fill:{venue_execution_id}",
            )
        )
    ).scalar_one_or_none()
    if row is None:
        return None
    payload = await get_bound_contract_position_event(session, row.public_id)
    _require(
        payload.scope == scope and payload.venue_execution_id == venue_execution_id,
        "position_source_lookup_identity_mismatch",
    )
    return payload
