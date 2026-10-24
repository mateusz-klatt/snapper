"""Append source-bound native cash facts while preserving reconciliation holds.

The standalone writer owns the real wallet execution fence. The session writer
joins the caller's existing financial transaction so execution, unit evidence,
cash facts and publication intent can commit together. Neither path resolves a
hold or declares a complete account inventory merely because one currency leg
was understood. Historical route authority proves observed facts; it does not
grant current permission to reserve funds or send a new order.
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from uuid import NAMESPACE_URL
from uuid import uuid5

from pydantic import BaseModel
from pydantic import ConfigDict
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.portfolio.futures_account_log import FuturesNativeCashLeg
from snapper.application.portfolio.futures_account_log import native_futures_cash_leg
from snapper.application.portfolio.futures_account_log import parse_futures_account_log
from snapper.application.portfolio.futures_cash_ledger import FuturesNativeCashAnchor
from snapper.application.portfolio.futures_cash_ledger import FuturesNativeCashEvent
from snapper.application.portfolio.futures_cash_ledger import account_log_cash_source_identity
from snapper.application.portfolio.futures_cash_ledger import canonical_native_cash_anchor
from snapper.application.portfolio.futures_cash_ledger import canonical_native_cash_event
from snapper.application.portfolio.futures_cash_ledger import native_cash_anchor_digest
from snapper.application.portfolio.futures_cash_ledger import native_cash_event_digest
from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.core.execution_route_authority import ExecutionRouteAuthorityPayload
from snapper.core.execution_route_authority import account_scope_digest
from snapper.core.execution_route_authority import client_selection_digest
from snapper.core.execution_route_authority import client_selection_from_authority
from snapper.core.execution_unit_snapshot import EvidenceUUID
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance
from snapper.data.accounting_native_source_store import claim_new_accounting_source
from snapper.data.accounting_native_source_store import get_bound_native_source_claim
from snapper.data.accounting_observation_binding import AccountingObservationBindingError
from snapper.data.accounting_observation_binding import get_bound_accounting_observation
from snapper.data.contract_accounting_guards import acquire_contract_accounting_write_barrier
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.execution_route_authority_store import get_bound_route_authority
from snapper.data.models import AccountingRawReceipt
from snapper.data.models import ContractAccountingAnchor
from snapper.data.models import ContractAccountingEvent
from snapper.data.models import ExecutionRouteAuthority


class NativeCashAccountingError(ValueError):
    """Retained evidence cannot establish this exact account's native cash transition."""


class NativeCashAccountingRequest(BaseModel):
    """Exact durable receipt and historical authority references, with no supplied money mirrors."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    wallet_public_id: EvidenceUUID
    raw_receipt_public_id: EvidenceUUID
    route_authority_public_id: EvidenceUUID


@dataclass(frozen=True, slots=True)
class NativeCashLedgerWriteResult:
    """Committed source coordinates, never a certification of spendable account funds."""

    public_id: str
    anchor_public_id: str
    content_digest: str
    ledger_sequence: int
    native_currency: str
    observed_balance: Decimal
    created: bool


@dataclass(frozen=True, slots=True)
class ObservedNativeCurrency:
    """An incomplete native currency chain requiring independent account reconciliation."""

    native_currency: str
    balance: Decimal
    anchor_public_id: str
    latest_event_public_id: str
    latest_ledger_sequence: int


@dataclass(frozen=True, slots=True)
class _BoundCashSource:
    """Source operands whose raw frame, selector and actual historical route have been checked."""

    request: NativeCashAccountingRequest
    observation: RawAccountingObservation
    authority: ExecutionRouteAuthorityPayload
    leg: FuturesNativeCashLeg


@dataclass(frozen=True, slots=True)
class _BoundCashEvent:
    """An immutable event row and its verified complete source projection."""

    row: ContractAccountingEvent
    payload: FuturesNativeCashEvent
    authority: ExecutionRouteAuthorityPayload


def _require(condition: bool, reason: str) -> None:
    """Refuse accounting while leaving already committed Stage A evidence and holds intact."""
    if not condition:
        raise NativeCashAccountingError(reason)


def _verify_raw_route(
    request: NativeCashAccountingRequest,
    raw: RawAccountingObservation,
    authority: ExecutionRouteAuthorityPayload,
) -> None:
    """Compare the actual client identity without inventing an unknown margin label."""
    scope = authority.scope
    actual = (
        request.wallet_public_id,
        raw.scope.wallet_public_id,
        raw.scope.exchange,
        raw.scope.mode,
        raw.scope.environment,
        raw.credential_public_id,
        raw.credential_timestamp,
        raw.route_revision,
    )
    expected = (
        scope.wallet_public_id,
        scope.wallet_public_id,
        scope.exchange,
        scope.mode,
        scope.environment,
        authority.credential_public_id,
        authority.credential_timestamp,
        client_selection_digest(client_selection_from_authority(authority)),
    )
    _require(actual == expected, "native_cash_actual_client_route_mismatch")
    _require(
        authority.credential_timestamp is None or authority.credential_timestamp <= raw.observed_at,
        "native_cash_credential_revision_from_future",
    )
    _require(
        raw.scope.margin_account in {None, scope.margin_account},
        "native_cash_raw_margin_account_mismatch",
    )
    _require(
        raw.route_authority_public_id in {None, request.route_authority_public_id},
        "native_cash_raw_authority_mismatch",
    )


async def _bound_source(
    session: AsyncSession, request: NativeCashAccountingRequest
) -> _BoundCashSource:
    """Establish native currency, exact source selection and actual account before any posting."""
    try:
        raw = await get_bound_accounting_observation(session, request.raw_receipt_public_id)
    except AccountingObservationBindingError as error:
        raise NativeCashAccountingError(str(error)) from error
    authority = await get_bound_route_authority(session, request.route_authority_public_id)
    _verify_raw_route(request, raw, authority)
    entry = parse_futures_account_log(raw)
    _require(
        entry.venue_account_id is None
        or authority.venue_account_id is None
        or entry.venue_account_id == authority.venue_account_id,
        "native_cash_source_account_header_mismatch",
    )
    _require(
        entry.margin_account == authority.scope.margin_account,
        "native_cash_source_margin_account_mismatch",
    )
    source_id = {
        "booking_uid": entry.booking_uid,
        "venue_execution_id": entry.execution,
    }.get(raw.source_identity_kind or "unproven")
    _require(
        raw.source_identity_kind in {None, "unproven"} or raw.source_identity == source_id,
        "native_cash_source_identity_mismatch",
    )
    return _BoundCashSource(request, raw, authority, native_futures_cash_leg(entry))


async def _require_source_knowledge(
    session: AsyncSession, source: _BoundCashSource, known_at: datetime
) -> None:
    """Require actual source observation and retention before financial posting knowledge."""
    raw_at = await session.scalar(
        select(AccountingRawReceipt.timestamp).where(
            AccountingRawReceipt.public_id == source.request.raw_receipt_public_id
        )
    )
    authority_at = await session.scalar(
        select(ExecutionRouteAuthority.timestamp).where(
            ExecutionRouteAuthority.public_id == source.request.route_authority_public_id
        )
    )
    _require(
        source.observation.observed_at <= known_at
        and source.authority.observed_at <= known_at
        and all(instant is not None and instant <= known_at for instant in (raw_at, authority_at)),
        "native_cash_posting_precedes_source_knowledge",
    )


def _verify_event_row(row: ContractAccountingEvent, payload: FuturesNativeCashEvent) -> None:
    """Reject indexed copies that disagree with immutable posting payload and digest."""
    scope = payload.scope
    actual = (
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
    expected = (
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
        payload.source_identity,
        None,
        None,
        None,
        native_cash_event_digest(payload),
        canonical_native_cash_event(payload),
    )
    _require(actual == expected, "native_cash_event_projection_mismatch")


async def _bound_anchor(
    session: AsyncSession, event: FuturesNativeCashEvent, known_at: datetime
) -> FuturesNativeCashAnchor:
    """Require an exact source anchor without silently certifying an execution prefix."""
    row = (
        await session.execute(
            select(ContractAccountingAnchor).where(
                ContractAccountingAnchor.public_id == event.anchor_public_id
            )
        )
    ).scalar_one_or_none()
    if row is None:
        raise NativeCashAccountingError("native_cash_anchor_missing")
    _require(row.timestamp <= known_at, "native_cash_anchor_knowledge_from_future")
    payload = FuturesNativeCashAnchor.model_validate_json(row.canonical_payload)
    scope = payload.scope
    actual = (
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
    expected = (
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
        native_cash_anchor_digest(payload),
        canonical_native_cash_anchor(payload),
    )
    _require(actual == expected, "native_cash_anchor_projection_mismatch")
    _require(
        (payload.scope, payload.native_currency) == (event.scope, event.native_currency),
        "native_cash_anchor_scope_mismatch",
    )
    return payload


async def _require_account_continuity(
    session: AsyncSession,
    previous_public_id: str,
    current_public_id: str,
) -> None:
    """Require actual account continuity when the selected credential or authority changes."""
    before = await get_bound_route_authority(session, previous_public_id)
    after = await get_bound_route_authority(session, current_public_id)
    _require(before.scope == after.scope, "native_cash_account_scope_changed")
    both_known = before.venue_account_id is not None and after.venue_account_id is not None
    if both_known:
        _require(
            before.venue_account_id == after.venue_account_id,
            "native_cash_external_account_changed",
        )
    same_credential = (before.credential_public_id, before.credential_timestamp) == (
        after.credential_public_id,
        after.credential_timestamp,
    )
    _require(
        same_credential or both_known,
        "native_cash_credential_continuity_unproven",
    )


async def _events_for_scope(
    session: AsyncSession, scope: ExecutionAccountScope
) -> tuple[_BoundCashEvent, ...]:
    """Replay only complete, source-bound cash events within the exact actual account scope."""
    rows = (
        await session.execute(
            select(ContractAccountingEvent)
            .where(
                ContractAccountingEvent.account_scope_digest == account_scope_digest(scope),
                ContractAccountingEvent.kind == "native_cash",
            )
            .order_by(ContractAccountingEvent.ledger_sequence)
        )
    ).scalars()
    result: list[_BoundCashEvent] = []
    for row in rows:
        payload = FuturesNativeCashEvent.model_validate_json(row.canonical_payload)
        _verify_event_row(row, payload)
        _require(payload.scope == scope, "native_cash_event_scope_mismatch")
        source = await _bound_source(
            session,
            NativeCashAccountingRequest(
                wallet_public_id=scope.wallet_public_id,
                raw_receipt_public_id=payload.raw_receipt_public_id,
                route_authority_public_id=payload.route_authority_public_id,
            ),
        )
        await _require_source_knowledge(session, source, row.timestamp)
        _require(source.leg.entry == payload.source_entry, "native_cash_event_source_changed")
        anchor = await _bound_anchor(session, payload, row.timestamp)
        if result:
            _require(
                result[-1].row.timestamp <= row.timestamp,
                "native_cash_retained_knowledge_discontinuous",
            )
            await _require_account_continuity(
                session,
                result[-1].payload.route_authority_public_id,
                payload.route_authority_public_id,
            )
        _verify_source_account_history(result, source)
        _check_cash_chain(result, row, payload, anchor)
        await get_bound_native_source_claim(session, row.public_id)
        result.append(_BoundCashEvent(row, payload, source.authority))
    return tuple(result)


def _latest_currency_event(
    events: tuple[_BoundCashEvent, ...] | list[_BoundCashEvent], currency: str
) -> _BoundCashEvent | None:
    """Find a native currency's latest exact posting without pooling different units."""
    return next(
        (item for item in reversed(events) if item.payload.native_currency == currency), None
    )


def _check_cash_chain(
    events: list[_BoundCashEvent],
    row: ContractAccountingEvent,
    event: FuturesNativeCashEvent,
    anchor: FuturesNativeCashAnchor,
) -> None:
    """Require source order and exact old/new conservation throughout the retained currency chain."""
    previous = _latest_currency_event(events, event.native_currency)
    if previous is None:
        _require(
            anchor.balance == event.cash_before
            and anchor.source_entry == event.source_entry
            and anchor.source_receipt_public_id == row.raw_receipt_public_id,
            "native_cash_first_event_anchor_mismatch",
        )
        return
    _require(
        previous.payload.anchor_public_id == event.anchor_public_id
        and previous.payload.cash_after == event.cash_before
        and previous.payload.source_entry.source_log_id < event.source_entry.source_log_id,
        "native_cash_retained_chain_discontinuous",
    )


def _same_source(first: FuturesNativeCashEvent, source: _BoundCashSource) -> bool:
    """Compare financial source facts across observation UUIDs and canonical sibling pointers."""
    return first.source_entry.model_dump(exclude={"target_json_pointer", "venue_account_id"}) == (
        source.leg.entry.model_dump(exclude={"target_json_pointer", "venue_account_id"})
    )


def _verify_source_account_history(
    events: tuple[_BoundCashEvent, ...] | list[_BoundCashEvent], source: _BoundCashSource
) -> None:
    """Keep every known original-frame account identity pinned across later unknown headers."""
    current = source.leg.entry.venue_account_id or source.authority.venue_account_id
    if current is None:
        return
    previous = {
        identity
        for item in events
        for identity in (
            item.payload.source_entry.venue_account_id,
            item.authority.venue_account_id,
        )
    }
    previous.discard(None)
    _require(
        not previous or previous == {current},
        "native_cash_known_source_account_changed",
    )


def _write_result(item: _BoundCashEvent, *, created: bool) -> NativeCashLedgerWriteResult:
    """Return exact accounting coordinates while keeping full-account readiness unclaimed."""
    return NativeCashLedgerWriteResult(
        public_id=item.row.public_id,
        anchor_public_id=item.payload.anchor_public_id,
        content_digest=item.row.content_digest,
        ledger_sequence=item.payload.ledger_sequence,
        native_currency=item.payload.native_currency,
        observed_balance=item.payload.cash_after,
        created=created,
    )


async def _insert_anchor(
    session: AsyncSession, source: _BoundCashSource, provenance: SnapshotWriteProvenance
) -> str:
    """Seed only this observed currency from the source's actual old balance."""
    scope = source.authority.scope
    scope_digest = account_scope_digest(scope)
    public_id = str(
        uuid5(
            NAMESPACE_URL,
            f"snapper:native-cash-anchor:v1:{scope_digest}:{source.request.raw_receipt_public_id}",
        )
    )
    payload = FuturesNativeCashAnchor(
        scope=scope,
        route_authority_public_id=source.request.route_authority_public_id,
        source_receipt_public_id=source.request.raw_receipt_public_id,
        native_currency=source.leg.native_currency,
        balance=source.leg.entry.old_balance,
        source_entry=source.leg.entry,
    )
    session.add(
        ContractAccountingAnchor(
            **{**provenance.model_dump(mode="python"), "public_id": public_id},
            **scope.model_dump(mode="python"),
            account_scope_digest=scope_digest,
            route_authority_public_id=payload.route_authority_public_id,
            source_receipt_public_id=payload.source_receipt_public_id,
            anchor_kind=payload.anchor_kind,
            source_execution_prefix_status=payload.source_execution_prefix_status,
            source_execution_scope_sequence=payload.source_execution_scope_sequence,
            content_digest=native_cash_anchor_digest(payload),
            canonical_payload=canonical_native_cash_anchor(payload),
        )
    )
    await session.flush()
    return public_id


async def _append_event(
    session: AsyncSession,
    source: _BoundCashSource,
    anchor_public_id: str,
    provenance: SnapshotWriteProvenance,
) -> _BoundCashEvent:
    """Allocate a global account ledger sequence inside the caller's wallet write fence."""
    scope = source.authority.scope
    scope_digest = account_scope_digest(scope)
    maximum = await session.scalar(
        select(func.max(ContractAccountingEvent.ledger_sequence)).where(
            ContractAccountingEvent.account_scope_digest == scope_digest
        )
    )
    payload = FuturesNativeCashEvent(
        scope=scope,
        route_authority_public_id=source.request.route_authority_public_id,
        anchor_public_id=anchor_public_id,
        raw_receipt_public_id=source.request.raw_receipt_public_id,
        ledger_sequence=int(maximum or 0) + 1,
        source_identity=account_log_cash_source_identity(source.leg.entry),
        native_currency=source.leg.native_currency,
        cash_before=source.leg.entry.old_balance,
        cash_after=source.leg.entry.new_balance,
        cash_delta=source.leg.cash_delta,
        source_entry=source.leg.entry,
    )
    row = ContractAccountingEvent(
        **provenance.model_dump(mode="python"),
        **scope.model_dump(mode="python"),
        account_scope_digest=scope_digest,
        route_authority_public_id=payload.route_authority_public_id,
        anchor_public_id=anchor_public_id,
        raw_receipt_public_id=payload.raw_receipt_public_id,
        ledger_sequence=payload.ledger_sequence,
        kind=payload.kind,
        source_identity=payload.source_identity,
        instrument_public_id=None,
        execution_public_id=None,
        unit_snapshot_public_id=None,
        content_digest=native_cash_event_digest(payload),
        canonical_payload=canonical_native_cash_event(payload),
    )
    session.add(row)
    await claim_new_accounting_source(session, row)
    return _BoundCashEvent(row, payload, source.authority)


async def _require_account_knowledge(
    session: AsyncSession, source: _BoundCashSource, known_at: datetime
) -> None:
    """Keep new cash knowledge after all recorded account facts, including actual positions."""
    latest = await session.scalar(
        select(ContractAccountingEvent)
        .where(
            ContractAccountingEvent.account_scope_digest
            == account_scope_digest(source.authority.scope)
        )
        .order_by(
            ContractAccountingEvent.timestamp.desc(),
            ContractAccountingEvent.ledger_sequence.desc(),
        )
        .limit(1)
    )
    if latest is None:
        return
    _require(latest.timestamp <= known_at, "native_cash_posting_precedes_account_knowledge")
    await _require_account_continuity(
        session, latest.route_authority_public_id, source.request.route_authority_public_id
    )


async def append_bound_futures_cash(
    session: AsyncSession,
    request: NativeCashAccountingRequest,
    provenance: SnapshotWriteProvenance,
) -> NativeCashLedgerWriteResult:
    """Join an existing wallet-fenced transaction without committing or releasing any hold.

    Args:
        session: Caller-owned financial transaction with the actual wallet fence held.
        request: Exact committed raw receipt and actual historical route references.
        provenance: Frozen local event identity and insertion metadata.

    Returns:
        The appended or semantically deduplicated observed cash coordinates.
        This leaves reconciliation, missing positions and all source holds pending.
    """
    binding = NativeCashAccountingRequest.model_validate(request)
    insertion = SnapshotWriteProvenance.model_validate(provenance)
    await acquire_contract_accounting_write_barrier(session)
    source = await _bound_source(session, binding)
    await _require_source_knowledge(session, source, insertion.timestamp)
    events = await _events_for_scope(session, source.authority.scope)
    if events:
        await _require_account_continuity(
            session, events[-1].payload.route_authority_public_id, binding.route_authority_public_id
        )
    _verify_source_account_history(events, source)
    identity = account_log_cash_source_identity(source.leg.entry)
    existing = next((item for item in events if item.payload.source_identity == identity), None)
    if existing is not None:
        _require(_same_source(existing.payload, source), "native_cash_source_identity_conflict")
        return _write_result(existing, created=False)
    await _require_account_knowledge(session, source, insertion.timestamp)
    previous = _latest_currency_event(events, source.leg.native_currency)
    if previous is None:
        anchor_id = await _insert_anchor(session, source, insertion)
    else:
        _require(
            previous.payload.cash_after == source.leg.entry.old_balance
            and previous.payload.source_entry.source_log_id < source.leg.entry.source_log_id,
            "native_cash_source_chain_discontinuous",
        )
        anchor_id = previous.payload.anchor_public_id
    item = await _append_event(session, source, anchor_id, insertion)
    return _write_result(item, created=True)


async def append_futures_cash_from_receipt(
    repository: AccountingRepository,
    request: NativeCashAccountingRequest,
    provenance: SnapshotWriteProvenance,
) -> NativeCashLedgerWriteResult:
    """Own one real financial fence and atomically commit an observed native cash leg.

    Args:
        repository: Actual repository transaction and wallet execution fence provider.
        request: Exact source receipt, actual wallet and historical route authority.
        provenance: Stable local event identity retained on a transaction retry.

    Returns:
        Committed ledger coordinates with all independent source holds still pending.
    """
    binding = NativeCashAccountingRequest.model_validate(request)
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await repository._acquire_execution_fence_lock(session, binding.wallet_public_id)
        result = await append_bound_futures_cash(session, binding, provenance)
        await session.commit()
        return result


async def load_observed_futures_cash(
    session: AsyncSession, scope: ExecutionAccountScope
) -> tuple[ObservedNativeCurrency, ...]:
    """Replay observed native balances without claiming complete inventory or available margin.

    Args:
        session: Caller-owned read transaction; no current account capability is granted.
        scope: Exact actual account and margin profile, independent of credential rotation.

    Returns:
        One independently verified chain per observed currency. Missing currencies
        remain unknown; an empty tuple never means zero cash or a flat account.
    """
    proven = ExecutionAccountScope.model_validate(scope)
    events = await _events_for_scope(session, proven)
    latest: dict[str, ObservedNativeCurrency] = {}
    for item in events:
        payload = item.payload
        latest[payload.native_currency] = ObservedNativeCurrency(
            native_currency=payload.native_currency,
            balance=payload.cash_after,
            anchor_public_id=payload.anchor_public_id,
            latest_event_public_id=item.row.public_id,
            latest_ledger_sequence=payload.ledger_sequence,
        )
    return tuple(latest[name] for name in sorted(latest))
