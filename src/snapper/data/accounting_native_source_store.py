"""Bind one native log identity to one actual newly inserted accounting event.

Claims and financial events join the same outer wallet-fenced transaction.
Historical unclaimed events are refused rather than silently certified by a
new empty claim table. This module deliberately avoids the public cash and
position readers, which themselves require the resulting claim during replay.
Writers interleave each financial event add and claim immediately. They do not
stage multiple unclaimed financial events before calling this writer, and any
claim failure must roll back the complete surrounding financial transaction.
"""

from dataclasses import dataclass
from uuid import NAMESPACE_URL
from uuid import uuid5

from sqlalchemy import and_
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.portfolio.accounting_native_source import (
    AccountingNativeSourceClaimPayload,
)
from snapper.application.portfolio.accounting_native_source import canonical_native_source_claim
from snapper.application.portfolio.accounting_native_source import native_source_claim_digest
from snapper.application.portfolio.accounting_native_source import native_source_family_digest
from snapper.application.portfolio.accounting_native_source import native_source_identity_digest
from snapper.application.portfolio.accounting_native_source import native_source_log_id
from snapper.application.portfolio.accounting_native_source import native_source_semantic_digest
from snapper.application.portfolio.contract_position_ledger import ContractPositionEventPayload
from snapper.application.portfolio.contract_position_ledger import canonical_position_payload
from snapper.application.portfolio.contract_position_ledger import position_payload_digest
from snapper.application.portfolio.futures_account_log import FuturesAccountLogEntry
from snapper.application.portfolio.futures_account_log import parse_futures_account_log
from snapper.application.portfolio.futures_cash_ledger import FuturesNativeCashEvent
from snapper.application.portfolio.futures_cash_ledger import canonical_native_cash_event
from snapper.application.portfolio.futures_cash_ledger import native_cash_event_digest
from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.core.execution_route_authority import ExecutionRouteAuthorityPayload
from snapper.core.execution_route_authority import account_scope_digest
from snapper.core.execution_route_authority import client_selection_digest
from snapper.core.execution_route_authority import client_selection_from_authority
from snapper.data.accounting_observation_binding import get_bound_accounting_observation
from snapper.data.contract_accounting_guards import require_contract_accounting_guards
from snapper.data.execution_route_authority_store import get_bound_route_authority
from snapper.data.models import AccountingNativeSourceClaim
from snapper.data.models import ContractAccountingEvent
from snapper.data.models import is_open_ended_version
from snapper.data.session_context import session_no_autoflush


class AccountingNativeSourceError(ValueError):
    """Native identity ownership, retained lineage or legacy financial history is unproven."""


@dataclass(frozen=True, slots=True)
class _NativeEvent:
    """Canonical financial payload and its exact selected native account-log coordinates."""

    payload: FuturesNativeCashEvent | ContractPositionEventPayload
    entry: FuturesAccountLogEntry
    receipt_public_id: str
    authority_public_id: str
    canonical: str
    digest: str


def _require(condition: bool, reason: str) -> None:
    """Keep already committed raw evidence and holds while refusing financial promotion."""
    if not condition:
        raise AccountingNativeSourceError(reason)


def _native_event(row: ContractAccountingEvent) -> _NativeEvent:
    """Interpret only the two supported native event families without trusting indexed mirrors."""
    if row.kind == "native_cash":
        cash = FuturesNativeCashEvent.model_validate_json(row.canonical_payload)
        return _NativeEvent(
            cash,
            cash.source_entry,
            cash.raw_receipt_public_id,
            cash.route_authority_public_id,
            canonical_native_cash_event(cash),
            native_cash_event_digest(cash),
        )
    _require(row.kind == "contract_position_fill", "native_source_event_kind_unsupported")
    position = ContractPositionEventPayload.model_validate_json(row.canonical_payload)
    return _NativeEvent(
        position,
        position.source_position.source_entry,
        position.position_receipt_public_id,
        position.position_route_authority_public_id,
        canonical_position_payload(position),
        position_payload_digest(position),
    )


def _event_targets(material: _NativeEvent) -> tuple[str, str | None, str | None, str | None]:
    """Derive actual indexed execution targets from the complete financial payload."""
    payload = material.payload
    if isinstance(payload, FuturesNativeCashEvent):
        return payload.source_identity, None, None, None
    return (
        f"kraken_futures:fill:{payload.venue_execution_id}",
        payload.instrument_public_id,
        payload.execution_public_id,
        payload.unit_snapshot_public_id,
    )


def _verify_event_projection(row: ContractAccountingEvent, material: _NativeEvent) -> None:
    """Require the actual event row to agree with all immutable financial payload projections."""
    payload = material.payload
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
        (
            row.source_identity,
            row.instrument_public_id,
            row.execution_public_id,
            row.unit_snapshot_public_id,
        ),
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
        _event_targets(material),
        material.digest,
        material.canonical,
    )
    _require(actual == expected, "native_source_actual_event_projection_mismatch")


def _verify_raw_route(
    raw: RawAccountingObservation,
    authority: ExecutionRouteAuthorityPayload,
    material: _NativeEvent,
) -> None:
    """Bind the retained account log to the actual authenticated client and margin authority."""
    scope = authority.scope
    _require(
        (
            raw.scope.wallet_public_id,
            raw.scope.exchange,
            raw.scope.mode,
            raw.scope.environment,
            raw.credential_public_id,
            raw.credential_timestamp,
            raw.route_revision,
        )
        == (
            scope.wallet_public_id,
            scope.exchange,
            scope.mode,
            scope.environment,
            authority.credential_public_id,
            authority.credential_timestamp,
            client_selection_digest(client_selection_from_authority(authority)),
        )
        and raw.scope.margin_account in {None, scope.margin_account}
        and raw.route_authority_public_id in {None, material.authority_public_id},
        "native_source_actual_client_route_mismatch",
    )
    source_id = {
        "booking_uid": material.entry.booking_uid,
        "venue_execution_id": material.entry.execution,
    }.get(raw.source_identity_kind or "unproven")
    _require(
        raw.source_identity_kind in {None, "unproven"} or raw.source_identity == source_id,
        "native_source_identity_projection_mismatch",
    )


async def _claim_payload(
    session: AsyncSession, event: ContractAccountingEvent
) -> AccountingNativeSourceClaimPayload:
    """Rebind actual raw, authority and event evidence without granting fresh spending authority."""
    material = _native_event(event)
    _verify_event_projection(event, material)
    raw = await get_bound_accounting_observation(session, material.receipt_public_id)
    authority = await get_bound_route_authority(session, material.authority_public_id)
    _verify_raw_route(raw, authority, material)
    _require(
        parse_futures_account_log(raw) == material.entry,
        "native_source_event_original_entry_mismatch",
    )
    return AccountingNativeSourceClaimPayload(
        scope=material.payload.scope,
        accounting_event_public_id=event.public_id,
        accounting_event_content_digest=material.digest,
        raw_receipt_public_id=material.receipt_public_id,
        route_authority_public_id=material.authority_public_id,
        kind=material.payload.kind,
        authority=authority,
        source_entry=material.entry,
    )


def _claim_public_id(event_public_id: str) -> str:
    """Name a claim from its actual financial event, without inventing an event reference."""
    return str(uuid5(NAMESPACE_URL, f"snapper:accounting-native-source-claim:v1:{event_public_id}"))


def _claim_columns(payload: AccountingNativeSourceClaimPayload) -> dict[str, object]:
    """Produce exact indexed immutable mirrors from a complete validated payload."""
    return {
        **payload.scope.model_dump(),
        "scope_digest": native_source_family_digest(payload.scope),
        "account_scope_digest": account_scope_digest(payload.scope),
        "source_domain": payload.source_domain,
        "source_log_id": native_source_log_id(payload.source_entry),
        "source_identity_digest": native_source_identity_digest(payload.source_entry),
        "semantic_digest": native_source_semantic_digest(payload.source_entry),
        "accounting_event_public_id": payload.accounting_event_public_id,
        "raw_receipt_public_id": payload.raw_receipt_public_id,
        "route_authority_public_id": payload.route_authority_public_id,
        "kind": payload.kind,
        "booking_uid": payload.source_entry.booking_uid,
        "content_digest": native_source_claim_digest(payload),
        "canonical_payload": canonical_native_source_claim(payload),
    }


async def _bound_claim(
    session: AsyncSession, row: AccountingNativeSourceClaim
) -> AccountingNativeSourceClaimPayload:
    """Verify immutable claim projections and the exact original actual financial event."""
    payload = AccountingNativeSourceClaimPayload.model_validate_json(row.canonical_payload)
    _require(
        all(getattr(row, name) == value for name, value in _claim_columns(payload).items())
        and row.public_id == _claim_public_id(payload.accounting_event_public_id)
        and is_open_ended_version(row.known_to),
        "native_source_claim_projection_mismatch",
    )
    event = await session.scalar(
        select(ContractAccountingEvent).where(
            ContractAccountingEvent.public_id == payload.accounting_event_public_id
        )
    )
    if event is None:
        raise AccountingNativeSourceError("native_source_actual_event_missing")
    _require(
        (row.timestamp, row.session_id, row.sequence_id)
        == (event.timestamp, event.session_id, event.sequence_id)
        and is_open_ended_version(event.known_to),
        "native_source_claim_provenance_mismatch",
    )
    _require(
        await _claim_payload(session, event) == payload, "native_source_claim_lineage_mismatch"
    )
    return payload


async def get_bound_native_source_claim(
    session: AsyncSession, accounting_event_public_id: str
) -> AccountingNativeSourceClaimPayload:
    """Require exclusive source ownership during financial replay and publication binding.

    Args:
        session: Caller-owned read or financial transaction.
        accounting_event_public_id: Actual retained accounting event UUID.

    Returns:
        The independently verified original claim. Unclaimed legacy events refuse.
    """
    row = await session.scalar(
        select(AccountingNativeSourceClaim).where(
            AccountingNativeSourceClaim.accounting_event_public_id == accounting_event_public_id
        )
    )
    if row is None:
        raise AccountingNativeSourceError("native_source_legacy_event_unclaimed")
    return await _bound_claim(session, row)


async def _require_claimed_history(session: AsyncSession, scope: ExecutionAccountScope) -> None:
    """Prevent an empty new claim table from silently certifying historical native ownership."""
    rows = await session.execute(
        select(ContractAccountingEvent.public_id, AccountingNativeSourceClaim)
        .outerjoin(
            AccountingNativeSourceClaim,
            AccountingNativeSourceClaim.accounting_event_public_id
            == ContractAccountingEvent.public_id,
        )
        .where(
            ContractAccountingEvent.wallet_public_id == scope.wallet_public_id,
            ContractAccountingEvent.exchange == scope.exchange,
            ContractAccountingEvent.mode == scope.mode,
            ContractAccountingEvent.environment == scope.environment,
            ContractAccountingEvent.kind.in_(("native_cash", "contract_position_fill")),
        )
    )
    for event_public_id, row in rows:
        if row is None:
            raise AccountingNativeSourceError("native_source_legacy_family_history_unclaimed")
        payload = await _bound_claim(session, row)
        _require(
            payload.accounting_event_public_id == event_public_id
            and native_source_family_digest(payload.scope) == native_source_family_digest(scope),
            "native_source_family_history_claim_mismatch",
        )


async def _require_source_unused(
    session: AsyncSession, payload: AccountingNativeSourceClaimPayload
) -> None:
    """Check exact provider identity across cash, positions, instruments and margin profiles."""
    scope = payload.scope
    existing = await session.scalar(
        select(AccountingNativeSourceClaim).where(
            AccountingNativeSourceClaim.source_domain == payload.source_domain,
            or_(
                and_(
                    AccountingNativeSourceClaim.wallet_public_id == scope.wallet_public_id,
                    AccountingNativeSourceClaim.exchange == scope.exchange,
                    AccountingNativeSourceClaim.mode == scope.mode,
                    AccountingNativeSourceClaim.environment == scope.environment,
                    AccountingNativeSourceClaim.source_log_id
                    == native_source_log_id(payload.source_entry),
                ),
                and_(
                    AccountingNativeSourceClaim.scope_digest == native_source_family_digest(scope),
                    AccountingNativeSourceClaim.source_identity_digest
                    == native_source_identity_digest(payload.source_entry),
                ),
            ),
        )
    )
    if existing is not None:
        await _bound_claim(session, existing)
        raise AccountingNativeSourceError("native_source_already_consumed")


async def claim_new_accounting_source(
    session: AsyncSession, event: ContractAccountingEvent
) -> AccountingNativeSourceClaimPayload:
    """Atomically insert exclusive source ownership with an actual newly pending financial event.

    Args:
        session: Caller-owned transaction holding the actual wallet execution fence.
            Each event add must be immediately followed by this claim call, with no
            other unclaimed financial events staged ahead of it. A failure must
            abort the entire outer financial transaction.
        event: Actual pending financial row, already validated by its cash or position writer.

    Returns:
        The retained claim after both rows flush together. No inner commit or hold release occurs.

    Raises:
        AccountingNativeSourceError: For prior source ownership or unclaimed historical facts.
    """
    _require(event in session.new, "native_source_claim_requires_new_pending_event")
    with session_no_autoflush(session):
        await require_contract_accounting_guards(session)
        payload = await _claim_payload(session, event)
        await _require_claimed_history(session, payload.scope)
        await _require_source_unused(session, payload)
        row = AccountingNativeSourceClaim(
            public_id=_claim_public_id(event.public_id),
            session_id=event.session_id,
            sequence_id=event.sequence_id,
            timestamp=event.timestamp,
            **_claim_columns(payload),
        )
        session.add(row)
    await session.flush()
    return payload
