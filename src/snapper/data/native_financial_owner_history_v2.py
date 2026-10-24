"""Rebind UID-linked financial history before allowing a native owner decision to proceed.

Initial safe identity roots discover candidate wallets; they never assign
financial ownership. Each retained native fact must bind its actual private UID
and original policy. Unsupported legacy history remains a refusal. Actual native positions
require the complete original fill/unit and later-log reader composed here. No wallet or UID lock is acquired
inside this historical verifier, preserving the administrator's Setting-lock
order and the financial writer's wallet/global/UID/Setting order.
"""

from datetime import datetime

from pydantic import TypeAdapter
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from snapper.application.portfolio.native_financial_payloads_v2 import FuturesNativeCashEventV2
from snapper.application.portfolio.native_financial_payloads_v2 import NativeFinancialEventV2
from snapper.application.portfolio.native_position_admission_v3 import AdmittedNativePositionEventV3
from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.core.execution_route_authority import account_scope_digest
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.financial_account_owner import FinancialAccountOwnerKey
from snapper.data.models import AccountingNativeSourceClaim
from snapper.data.models import ContractAccountingAnchor
from snapper.data.models import ContractAccountingEvent
from snapper.data.native_cash_history_v2 import BoundNativeCashEventV2
from snapper.data.native_cash_history_v2 import load_native_cash_history_v2
from snapper.data.native_financial_event_family import bind_native_financial_event_family
from snapper.data.native_financial_source_binding_v2 import NativeFinancialBindingError
from snapper.data.native_financial_source_binding_v2 import require_native_financial_binding
from snapper.data.native_position_history_v2 import BoundNativePositionEventV2
from snapper.data.native_position_history_v2 import load_native_position_history_v2
from snapper.data.session_context import session_no_autoflush

_INSTANT: TypeAdapter[datetime] = TypeAdapter(EvidenceInstant)
_EVENT: TypeAdapter[NativeFinancialEventV2 | AdmittedNativePositionEventV3] = TypeAdapter(
    NativeFinancialEventV2 | AdmittedNativePositionEventV3
)
type _NativeBookEvent = BoundNativeCashEventV2 | BoundNativePositionEventV2
type _FinancialModel = (
    type[ContractAccountingEvent]
    | type[ContractAccountingAnchor]
    | type[AccountingNativeSourceClaim]
)


def _family_filters(
    model: _FinancialModel, key: FinancialAccountOwnerKey, wallets: tuple[str, ...]
) -> tuple[ColumnElement[bool], ...]:
    """Discover every actual financial row in the UID-linked families without a kind/profile filter."""
    return (
        model.wallet_public_id.in_(wallets),
        model.exchange == key.exchange,
        model.mode == key.mode,
        model.environment == key.environment,
    )


async def _bound_native_books(
    session: AsyncSession, rows: tuple[ContractAccountingEvent, ...], known_at: datetime
) -> tuple[_NativeBookEvent, ...]:
    """Replay each actual currency scope or instrument position once using the full source reader."""
    cash: dict[str, ExecutionAccountScope] = {}
    positions: dict[tuple[str, str], ExecutionAccountScope] = {}
    for row in rows:
        require_native_financial_binding(
            row.timestamp <= known_at, "native_financial_owner_history_after_decision"
        )
        try:
            payload = _EVENT.validate_json(row.canonical_payload)
        except ValidationError:
            raise NativeFinancialBindingError(
                "native_financial_owner_history_or_migration_unproven"
            ) from None
        if isinstance(payload, AdmittedNativePositionEventV3):
            material = await bind_native_financial_event_family(session, row, known_at)
            payload = material.payload
        scope = payload.operands.scope
        digest = account_scope_digest(scope)
        if isinstance(payload, FuturesNativeCashEventV2):
            cash[digest] = scope
        else:
            positions[(digest, payload.operands.instrument_public_id)] = scope
    result: list[_NativeBookEvent] = []
    for scope in cash.values():
        result.extend(await load_native_cash_history_v2(session, scope, known_at))
    for (_digest, instrument), scope in positions.items():
        result.extend(await load_native_position_history_v2(session, scope, instrument, known_at))
    return tuple(result)


def _require_owned_book(
    events: tuple[_NativeBookEvent, ...],
    key: FinancialAccountOwnerKey,
    owner_wallet: str,
) -> None:
    """Pin a prior actual private source book to its original local owner instead of current config alone."""
    for item in events:
        require_native_financial_binding(
            item.source.ownership.account_uid == key.account_uid
            and item.payload.source.authority.venue_account_id == key.account_uid
            and item.row.wallet_public_id == owner_wallet
            and item.payload.source.owner_evidence.assignment.owner_wallet_public_id
            == owner_wallet,
            "native_financial_owner_history_transfer_or_uid_change_unsupported",
        )


def _require_exact_inventory(
    rows: tuple[ContractAccountingEvent, ...],
    anchors: tuple[ContractAccountingAnchor, ...],
    claims: tuple[AccountingNativeSourceClaim, ...],
    events: tuple[_NativeBookEvent, ...],
) -> None:
    """Refuse orphaned, duplicate or hidden-kind financial facts instead of blessing a partial inventory."""
    event_ids = {item.row.public_id for item in events}
    anchor_ids = {item.payload.operands.anchor_public_id for item in events}
    require_native_financial_binding(
        len(rows) == len(events) == len(event_ids)
        and {row.public_id for row in rows} == event_ids
        and len(claims) == len(event_ids)
        and {row.accounting_event_public_id for row in claims} == event_ids
        and len(anchors) == len(anchor_ids)
        and {row.public_id for row in anchors} == anchor_ids,
        "native_financial_owner_history_inventory_incomplete",
    )


async def require_uid_native_financial_history_v2(
    session: AsyncSession,
    key: FinancialAccountOwnerKey,
    wallets: tuple[str, ...],
    owner_wallet: str,
    known_at: datetime,
) -> None:
    """Verify actual historical ownership under the caller's already established account barriers.

    Caller discovery must use the actual bound initial identity roots for this
    UID, not every issued account read. Privileged index damage belongs to the
    finite startup/restore audit; this method scans only the discovered UID's
    wallet families. Empty history permits explicit initial configuration but
    does not itself grant native financial readiness.

    Args:
        session: Actual current-owner or administrator transaction, with its required locks held.
        key: Exact safe native UID/environment/source domain already bound from current authority.
        wallets: Actual root-discovered wallet families, including aliases without profile authorities.
        owner_wallet: Explicit proposed/current financial owner, never first/oldest/writing inference.
        known_at: Actual post-lock decision clock; future retained financial rows cannot be ignored.
    """
    actual_key = FinancialAccountOwnerKey.model_validate(key)
    clock = _INSTANT.validate_python(known_at)
    require_native_financial_binding(bool(wallets), "native_financial_owner_roots_missing")
    with session_no_autoflush(session):
        rows = tuple(
            (
                await session.scalars(
                    select(ContractAccountingEvent).where(
                        *_family_filters(ContractAccountingEvent, actual_key, wallets)
                    )
                )
            ).all()
        )
        anchors = tuple(
            (
                await session.scalars(
                    select(ContractAccountingAnchor).where(
                        *_family_filters(ContractAccountingAnchor, actual_key, wallets)
                    )
                )
            ).all()
        )
        claims = tuple(
            (
                await session.scalars(
                    select(AccountingNativeSourceClaim).where(
                        *_family_filters(AccountingNativeSourceClaim, actual_key, wallets)
                    )
                )
            ).all()
        )
        events = await _bound_native_books(session, rows, clock)
        _require_owned_book(events, actual_key, owner_wallet)
        _require_exact_inventory(rows, anchors, claims, events)
