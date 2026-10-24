"""Audit every retained native financial fact once at the startup or restore boundary."""

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from typing import Literal
from typing import cast

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import field_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.portfolio.accounting_native_source import native_source_family_digest
from snapper.application.portfolio.accounting_native_source import native_source_log_id
from snapper.application.portfolio.contract_position_ledger import ContractPositionEventPayload
from snapper.application.portfolio.futures_cash_ledger import FuturesNativeCashEvent
from snapper.application.portfolio.native_financial_payloads_v2 import ContractPositionEventV2
from snapper.application.portfolio.native_financial_payloads_v2 import FuturesNativeCashEventV2
from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.core.execution_route_authority import account_scope_digest
from snapper.data.accounting_native_source_store import get_bound_native_source_claim
from snapper.data.accounting_native_source_store_v2 import get_bound_native_source_claim_v2
from snapper.data.contract_accounting_guards import ContractAccountingIntegrityError
from snapper.data.contract_accounting_guards import PhysicalAccountingGuard
from snapper.data.contract_accounting_guards import acquire_contract_accounting_restore_barrier
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.contract_position_ledger_store import _event_projection
from snapper.data.contract_position_ledger_store import get_bound_contract_position_event
from snapper.data.financial_publication_delivery import _verify_delivery
from snapper.data.financial_publication_store import BoundFinancialPublication
from snapper.data.financial_publication_store import get_bound_financial_publication
from snapper.data.futures_cash_ledger_store import _verify_event_row
from snapper.data.futures_cash_ledger_store import load_observed_futures_cash
from snapper.data.models import AccountingHoldResolution
from snapper.data.models import AccountingNativeSourceClaim
from snapper.data.models import ContractAccountingAnchor
from snapper.data.models import ContractAccountingEvent
from snapper.data.models import FinancialPublicationDelivery
from snapper.data.models import FinancialPublicationIntent
from snapper.data.models import is_open_ended_version
from snapper.data.native_cash_history_v2 import load_native_cash_history_v2
from snapper.data.native_contract_accounting_guards import (
    acquire_native_contract_accounting_restore_barrier,
)
from snapper.data.native_financial_event_binding_v2 import get_native_financial_event_material
from snapper.data.native_position_history_v2 import get_bound_native_position_event_v2


class _NativeKind(BaseModel):
    """Read only the canonical discriminator before full source-bound payload validation."""

    model_config = ConfigDict(extra="ignore", strict=True)

    kind: Literal["native_cash", "contract_position_fill"]
    schema_version: Literal[1, 2] = 1

    @field_validator("schema_version", mode="before")
    @classmethod
    def strict_schema_version(cls, value: object) -> object:
        """Refuse noninteger schema aliases before selecting an actual financial reader.

        Args:
            value: Original immutable payload discriminator, with legacy absence retaining version one.

        Returns:
            The unchanged actual integer awaiting the supported-version check.
        """
        if type(value) is not int:
            raise ValueError("Accounting restore schema version must be an integer")
        return value


@dataclass(frozen=True, slots=True)
class ContractAccountingRestoreReport:
    """Describe a successful finite retained-graph audit without granting financial readiness."""

    audited_at: datetime
    event_count: int
    claim_count: int
    anchor_count: int
    publication_count: int
    delivery_count: int
    scopes: tuple[ExecutionAccountScope, ...]
    physical_guards: tuple[PhysicalAccountingGuard, ...]
    retained_graph_digest: str


@dataclass(frozen=True, slots=True)
class _FinancialInventory:
    """Every retained actual row and canonical account scope before any filtered replay."""

    events: tuple[ContractAccountingEvent, ...]
    scopes: tuple[ExecutionAccountScope, ...]
    anchor_public_ids: frozenset[str]
    native_guards: tuple[PhysicalAccountingGuard, ...] = ()


def _require(condition: bool, reason: str) -> None:
    """Refuse the restore boundary without changing any financial row or releasing a hold."""
    if not condition:
        raise ContractAccountingIntegrityError(reason)


async def _startup_barrier(
    session: AsyncSession,
) -> tuple[PhysicalAccountingGuard, ...]:
    """Protect a single actual global audit and prevent financial writes until its transaction ends."""
    return await acquire_contract_accounting_restore_barrier(session)


async def _cash_scope(
    session: AsyncSession, row: ContractAccountingEvent, version: int, as_of: datetime
) -> ExecutionAccountScope:
    """Bind actual versioned cash row mirrors before collecting its complete replay scope."""
    if version == 1:
        cash = FuturesNativeCashEvent.model_validate_json(row.canonical_payload)
        _verify_event_row(row, cash)
        return cash.scope
    material = await get_native_financial_event_material(session, row.public_id, as_of)
    _require(
        isinstance(material.payload, FuturesNativeCashEventV2),
        "accounting_restore_native_cash_kind_mismatch",
    )
    return material.payload.operands.scope


async def _position_scope(
    session: AsyncSession, row: ContractAccountingEvent, version: int, as_of: datetime
) -> tuple[ExecutionAccountScope, str]:
    """Select the actual strict position family before replaying its complete original source graph."""
    if version == 1:
        position = ContractPositionEventPayload.model_validate_json(row.canonical_payload)
        _event_projection(row, position)
        return position.scope, position.instrument_public_id
    material = await get_native_financial_event_material(session, row.public_id, as_of)
    _require(
        isinstance(material.payload, ContractPositionEventV2),
        "accounting_restore_native_position_kind_mismatch",
    )
    payload = cast(ContractPositionEventV2, material.payload)
    return payload.operands.scope, payload.operands.instrument_public_id


async def _financial_inventory(session: AsyncSession, as_of: datetime) -> _FinancialInventory:
    """Enumerate all actual rows before selecting any canonical kind, account family or margin."""
    events = tuple(
        (
            await session.scalars(
                select(ContractAccountingEvent).order_by(ContractAccountingEvent.id)
            )
        ).all()
    )
    native_guards = (
        await acquire_native_contract_accounting_restore_barrier(session)
        if any(
            _NativeKind.model_validate_json(row.canonical_payload).schema_version == 2
            for row in events
        )
        else ()
    )
    latest_positions: dict[tuple[int, str, str], ContractAccountingEvent] = {}
    cash_scopes: dict[tuple[int, str], ExecutionAccountScope] = {}
    scopes: dict[str, ExecutionAccountScope] = {}
    for row in events:
        _require(
            is_open_ended_version(row.known_to) and row.timestamp <= as_of,
            "accounting_restore_event_knowledge_invalid",
        )
        discriminator = _NativeKind.model_validate_json(row.canonical_payload)
        if discriminator.kind == "native_cash":
            scope = await _cash_scope(session, row, discriminator.schema_version, as_of)
            cash_scopes[(discriminator.schema_version, account_scope_digest(scope))] = scope
        else:
            scope, instrument = await _position_scope(
                session, row, discriminator.schema_version, as_of
            )
            key = (
                discriminator.schema_version,
                account_scope_digest(scope),
                instrument,
            )
            previous = latest_positions.get(key)
            if previous is None or previous.ledger_sequence < row.ledger_sequence:
                latest_positions[key] = row
        scopes[account_scope_digest(scope)] = scope
    for (version, _digest), scope in cash_scopes.items():
        if version == 1:
            await load_observed_futures_cash(session, scope)
        else:
            await load_native_cash_history_v2(session, scope, as_of)
    for (version, _digest, _instrument), row in latest_positions.items():
        if version == 1:
            await get_bound_contract_position_event(session, row.public_id)
        else:
            await get_bound_native_position_event_v2(session, row.public_id, as_of)
    return _FinancialInventory(
        events,
        tuple(scopes[key] for key in sorted(scopes)),
        frozenset(row.anchor_public_id for row in events),
        native_guards,
    )


async def _claims(session: AsyncSession, inventory: _FinancialInventory) -> tuple[str, ...]:
    """Bind every actual claim and detect global native-ID aliases independently of restored indexes."""
    rows = (
        await session.scalars(
            select(AccountingNativeSourceClaim).order_by(AccountingNativeSourceClaim.id)
        )
    ).all()
    identities: set[tuple[str, str, str]] = set()
    targets: set[str] = set()
    fingerprints: list[str] = []
    for row in rows:
        discriminator = _NativeKind.model_validate_json(row.canonical_payload)
        payload = (
            await get_bound_native_source_claim_v2(
                session, row.accounting_event_public_id, row.timestamp
            )
            if discriminator.schema_version == 2
            else await get_bound_native_source_claim(session, row.accounting_event_public_id)
        )
        key = (
            native_source_family_digest(payload.scope),
            payload.source_domain,
            native_source_log_id(payload.source_entry),
        )
        _require(key not in identities, "accounting_restore_native_source_duplicated")
        identities.add(key)
        targets.add(payload.accounting_event_public_id)
        fingerprints.append(row.content_digest)
    _require(
        len(rows) == len(inventory.events)
        and targets == {row.public_id for row in inventory.events},
        "accounting_restore_claim_inventory_mismatch",
    )
    return tuple(fingerprints)


async def _anchors(
    session: AsyncSession, inventory: _FinancialInventory
) -> tuple[ContractAccountingAnchor, ...]:
    """Reject orphaned or temporally rewritten seeds after full event readers bind all used anchors."""
    rows = tuple(
        (
            await session.scalars(
                select(ContractAccountingAnchor).order_by(ContractAccountingAnchor.id)
            )
        ).all()
    )
    _require(
        all(is_open_ended_version(row.known_to) for row in rows)
        and {row.public_id for row in rows} == inventory.anchor_public_ids,
        "accounting_restore_anchor_inventory_mismatch",
    )
    return rows


async def _publications(
    session: AsyncSession, as_of: datetime
) -> tuple[tuple[FinancialPublicationIntent, ...], tuple[FinancialPublicationDelivery, ...]]:
    """Verify all existing actual outbox graphs without requiring fake v1 references for cash."""
    rows = tuple(
        (
            await session.scalars(
                select(FinancialPublicationIntent).order_by(FinancialPublicationIntent.id)
            )
        ).all()
    )
    bound: dict[str, BoundFinancialPublication] = {}
    for row in rows:
        _require(
            is_open_ended_version(row.known_to) and row.timestamp <= as_of,
            "accounting_restore_publication_knowledge_invalid",
        )
        bound[row.public_id] = await get_bound_financial_publication(session, row.public_id)
    deliveries = tuple(
        (
            await session.scalars(
                select(FinancialPublicationDelivery).order_by(FinancialPublicationDelivery.id)
            )
        ).all()
    )
    for delivery in deliveries:
        intent = bound.get(delivery.publication_public_id)
        if intent is None:
            raise ContractAccountingIntegrityError("accounting_restore_delivery_intent_missing")
        _require(
            is_open_ended_version(delivery.known_to) and delivery.timestamp <= as_of,
            "accounting_restore_delivery_knowledge_invalid",
        )
        _verify_delivery(delivery, intent)
    return rows, deliveries


def _graph_digest(parts: tuple[tuple[str, ...], ...]) -> str:
    """Identify checked retained evidence without pretending to authenticate missing external history."""
    material = json.dumps(parts, separators=(",", ":"))
    return hashlib.sha256(
        b"snapper:contract-accounting-restore:v1\x00" + material.encode()
    ).hexdigest()


async def audit_contract_accounting_restore(
    session: AsyncSession,
) -> ContractAccountingRestoreReport:
    """Audit the complete retained native graph under an actual startup transaction barrier.

    Every financial row is selected before trusting its indexed kind or scope.
    Canonical payloads then choose the existing strict cash or position binder.
    The audit validates all native claims, used anchors and existing publication
    and delivery graphs. Cash events need no invented Execution or position
    notification. Missing raw receipts with no financial interpretation remain
    retained Stage A evidence and unresolved holds, not fabricated journal facts.

    Args:
        session: Caller-owned startup transaction; SQLite must already hold BEGIN IMMEDIATE.

    Returns:
        Evidence of this completed retained-graph audit. It never releases holds,
        proves full external inventory or grants trading readiness.
    """
    if not session.in_transaction():
        raise RuntimeError("accounting_restore_requires_caller_transaction")
    guards = await _startup_barrier(session)
    as_of = datetime.now(UTC)
    unresolved_policy = await session.scalar(select(AccountingHoldResolution.public_id).limit(1))
    _require(
        unresolved_policy is None,
        "accounting_restore_hold_resolution_policy_unimplemented",
    )
    inventory = await _financial_inventory(session, as_of)
    guards = inventory.native_guards or guards
    claims = await _claims(session, inventory)
    anchors = await _anchors(session, inventory)
    publications, deliveries = await _publications(session, as_of)
    return ContractAccountingRestoreReport(
        audited_at=as_of,
        event_count=len(inventory.events),
        claim_count=len(claims),
        anchor_count=len(anchors),
        publication_count=len(publications),
        delivery_count=len(deliveries),
        scopes=inventory.scopes,
        physical_guards=guards,
        retained_graph_digest=_graph_digest(
            (
                tuple(row.content_digest for row in inventory.events),
                claims,
                tuple(row.content_digest for row in anchors),
                tuple(row.content_digest for row in publications),
                tuple(
                    f"{row.public_id}:{row.publication_digest}:{row.published_at.isoformat()}"
                    for row in deliveries
                ),
                tuple(guard.definition_digest for guard in guards),
            )
        ),
    )


async def run_contract_accounting_restore_audit(
    repository: AccountingRepository,
) -> ContractAccountingRestoreReport:
    """Run the actual finite boot or restored-database boundary before financial activation.

    Args:
        repository: Real database transaction provider; invocation precedes contract runtime activation.

    Returns:
        A committed audit report, not a reusable readiness capability or cache certification.
    """
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        report = await audit_contract_accounting_restore(session)
        await session.commit()
        return report
