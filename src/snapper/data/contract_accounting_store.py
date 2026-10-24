"""Commit raw accounting evidence and durable holds before interpretation starts.

Stage A never requires a fresh catalog, successful account query or financial
interpretation. Every new observation survives as immutable evidence together
with its hold. Exact transaction retries reuse their original observation UUID;
a changed payload for that UUID is retained separately and opens a conflict hold.
Stage B must prove the actual route, execution, unit and cash bindings before
resolving any hold. These writers never resolve holds or move account cash.
"""

from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import Protocol
from uuid import NAMESPACE_URL
from uuid import uuid5

from sqlalchemy import Select
from sqlalchemy import and_
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.core.contract_accounting_observation import AccountingObservationScope
from snapper.core.contract_accounting_observation import AccountingWriteProvenance
from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.core.contract_accounting_observation import accounting_hold_scope_digest
from snapper.core.contract_accounting_observation import accounting_observation_digest
from snapper.core.contract_accounting_observation import canonical_accounting_observation
from snapper.data.models import AccountingHoldResolution
from snapper.data.models import AccountingRawReceipt
from snapper.data.models import AccountingScopeHold


class AccountingEvidenceConflictError(ValueError):
    """An existing immutable evidence row contradicts its claimed exact identity."""


AccountingEvidenceConflict = AccountingEvidenceConflictError


class AccountingRepository(Protocol):
    """Established repository transaction primitives for capture and replay."""

    def session(self) -> AbstractAsyncContextManager[AsyncSession]:
        """Open a session whose transaction completion remains caller controlled.

        Returns:
            A context manager for the actual repository database session.
        """

    async def _begin_execution_insert_transaction(self, session: AsyncSession, /) -> None:
        """Reserve SQLite writes or require PostgreSQL READ COMMITTED first."""

    async def _acquire_execution_fence_lock(
        self, session: AsyncSession, wallet_public_id: str, /
    ) -> None:
        """Acquire the transaction-scoped wallet execution fence."""


@dataclass(frozen=True, slots=True)
class AccountingHoldView:
    """A durable unresolved refusal independent of engine/checkpoint state."""

    public_id: str
    raw_receipt_public_id: str
    reason: str
    margin_account: str | None


class AccountingScopeUnavailableError(RuntimeError):
    """Financial reads and new creates lack resolved account-wide evidence."""

    def __init__(self, holds: tuple[AccountingHoldView, ...]) -> None:
        """Retain exact durable reasons without changing any venue lifecycle facts."""
        super().__init__("accounting_scope_unavailable")
        self._holds = holds

    @property
    def holds(self) -> tuple[AccountingHoldView, ...]:
        """Expose the immutable unresolved hold identities.

        Returns:
            The retained source hold references and refusal reasons blocking this scope.
        """
        return self._holds


AccountingScopeUnavailable = AccountingScopeUnavailableError


@dataclass(frozen=True, slots=True)
class RawAccountingWriteResult:
    """Identity of a committed receipt and the associated persistent refusal."""

    id: int
    public_id: str
    claimed_public_id: str
    content_digest: str
    hold_public_id: str
    created: bool
    conflicted: bool


@dataclass(frozen=True, slots=True)
class _ObservationInsert:
    """Validated fixed input shared by the atomic receipt and hold writes."""

    observation: RawAccountingObservation
    provenance: AccountingWriteProvenance
    canonical_payload: str
    content_digest: str


def _hold_identity(receipt_public_id: str, reason: str) -> str:
    """Give an observation's refusal a stable local identity across transaction retry."""
    return str(uuid5(NAMESPACE_URL, f"snapper:accounting-hold:v1:{receipt_public_id}:{reason}"))


async def _ensure_hold(
    session: AsyncSession, record: AccountingRawReceipt, data: _ObservationInsert, reason: str
) -> str:
    """Insert one immutable refusal in the same transaction as its raw receipt."""
    public_id = _hold_identity(record.public_id, reason)
    existing = (
        await session.execute(
            select(AccountingScopeHold).where(AccountingScopeHold.public_id == public_id)
        )
    ).scalar_one_or_none()
    if existing is not None:
        scope = data.observation.scope
        if (
            existing.raw_receipt_public_id,
            existing.reason,
            existing.scope_digest,
            existing.wallet_public_id,
            existing.exchange,
            existing.mode,
            existing.environment,
            existing.margin_account,
        ) != (
            record.public_id,
            reason,
            accounting_hold_scope_digest(scope),
            scope.wallet_public_id,
            scope.exchange,
            scope.mode,
            scope.environment,
            scope.margin_account,
        ):
            raise AccountingEvidenceConflict("Accounting hold identity contradicts its receipt")
        return existing.public_id
    scope = data.observation.scope
    session.add(
        AccountingScopeHold(
            **{**data.provenance.model_dump(mode="python"), "public_id": public_id},
            scope_digest=accounting_hold_scope_digest(scope),
            wallet_public_id=scope.wallet_public_id,
            exchange=scope.exchange,
            mode=scope.mode,
            environment=scope.environment,
            margin_account=scope.margin_account,
            raw_receipt_public_id=record.public_id,
            reason=reason,
        )
    )
    await session.flush()
    return public_id


def _same_receipt(record: AccountingRawReceipt, data: _ObservationInsert) -> bool:
    """Compare the canonical payload and indexed scope copies on an exact retry."""
    scope = data.observation.scope
    return (
        record.content_digest == data.content_digest
        and record.canonical_payload == data.canonical_payload
        and record.claimed_public_id == data.provenance.public_id
        and (
            record.scope_digest,
            record.wallet_public_id,
            record.exchange,
            record.mode,
            record.environment,
            record.margin_account,
            record.kind,
            record.observed_at,
            record.route_authority_public_id,
        )
        == (
            accounting_hold_scope_digest(scope),
            scope.wallet_public_id,
            scope.exchange,
            scope.mode,
            scope.environment,
            scope.margin_account,
            data.observation.kind,
            data.observation.observed_at,
            data.observation.route_authority_public_id,
        )
    )


async def _insert_receipt(
    session: AsyncSession, data: _ObservationInsert, public_id: str
) -> AccountingRawReceipt:
    """Retain uninterpreted provider material without allocating a venue fact identity."""
    scope = data.observation.scope
    record = AccountingRawReceipt(
        **{**data.provenance.model_dump(mode="python"), "public_id": public_id},
        claimed_public_id=data.provenance.public_id,
        scope_digest=accounting_hold_scope_digest(scope),
        wallet_public_id=scope.wallet_public_id,
        exchange=scope.exchange,
        mode=scope.mode,
        environment=scope.environment,
        margin_account=scope.margin_account,
        route_authority_public_id=data.observation.route_authority_public_id,
        kind=data.observation.kind,
        observed_at=data.observation.observed_at,
        content_digest=data.content_digest,
        canonical_payload=data.canonical_payload,
    )
    session.add(record)
    await session.flush()
    return record


async def _capture_under_fence(
    session: AsyncSession, data: _ObservationInsert
) -> RawAccountingWriteResult:
    """Resolve transaction retries and retain changed-identity observations as conflicts."""
    claimed = data.provenance.public_id
    record = (
        await session.execute(
            select(AccountingRawReceipt).where(AccountingRawReceipt.public_id == claimed)
        )
    ).scalar_one_or_none()
    conflicted = record is not None and not _same_receipt(record, data)
    created = False
    if conflicted:
        conflict_id = str(
            uuid5(NAMESPACE_URL, f"snapper:accounting-conflict:v1:{claimed}:{data.content_digest}")
        )
        record = (
            await session.execute(
                select(AccountingRawReceipt).where(AccountingRawReceipt.public_id == conflict_id)
            )
        ).scalar_one_or_none()
        if record is not None and not _same_receipt(record, data):
            raise AccountingEvidenceConflict(
                "Accounting conflict identity already has different content"
            )
        if record is None:
            record = await _insert_receipt(session, data, conflict_id)
            created = True
    elif record is None:
        record = await _insert_receipt(session, data, claimed)
        created = True
    reason = "observation_identity_conflict" if conflicted else "uninterpreted_observation"
    hold_id = await _ensure_hold(session, record, data, reason)
    return RawAccountingWriteResult(
        record.id, record.public_id, claimed, data.content_digest, hold_id, created, conflicted
    )


async def capture_accounting_observation(
    repository: AccountingRepository,
    observation: RawAccountingObservation,
    provenance: AccountingWriteProvenance,
) -> RawAccountingWriteResult:
    """Commit raw evidence and a durable refusal before attempting financial interpretation.

    Args:
        repository: Real database transaction and wallet-fence provider.
        observation: Frozen source receipt, including unknown or expired authority.
        provenance: Stable observation identity retained by the caller on retry.

    Returns:
        The committed receipt/hold identities. ``conflicted`` means the changed
        source was retained but is ineligible for ordinary accounting application.
    """
    evidence = RawAccountingObservation.model_validate(observation)
    stamp = AccountingWriteProvenance.model_validate(provenance)
    data = _ObservationInsert(
        evidence,
        stamp,
        canonical_accounting_observation(evidence),
        accounting_observation_digest(evidence),
    )
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await repository._acquire_execution_fence_lock(session, evidence.scope.wallet_public_id)
        result = await _capture_under_fence(session, data)
        await session.commit()
        return result


def _hold_query(scope: AccountingObservationScope) -> Select[AccountingScopeHold]:
    """Keep unresolved holds visible independently of temporal and replay filters."""
    statement = (
        select(AccountingScopeHold)
        .outerjoin(
            AccountingHoldResolution,
            and_(
                AccountingHoldResolution.hold_public_id == AccountingScopeHold.public_id,
                AccountingHoldResolution.wallet_public_id == AccountingScopeHold.wallet_public_id,
                AccountingHoldResolution.exchange == AccountingScopeHold.exchange,
                AccountingHoldResolution.mode == AccountingScopeHold.mode,
                AccountingHoldResolution.environment == AccountingScopeHold.environment,
                AccountingHoldResolution.margin_account.is_not_distinct_from(
                    AccountingScopeHold.margin_account
                ),
            ),
        )
        .where(
            AccountingScopeHold.scope_digest == accounting_hold_scope_digest(scope),
            AccountingHoldResolution.id.is_(None),
        )
        .order_by(AccountingScopeHold.id.asc())
    )
    if scope.margin_account is not None:
        statement = statement.where(
            or_(
                AccountingScopeHold.margin_account.is_(None),
                AccountingScopeHold.margin_account == scope.margin_account,
            )
        )
    return statement


async def load_unresolved_accounting_holds(
    session: AsyncSession, scope: AccountingObservationScope
) -> tuple[AccountingHoldView, ...]:
    """Load exact account-family refusals without hiding closed or older evidence.

    Args:
        session: Existing session; CREATE callers must already hold the wallet fence.
        scope: Current actual route, with an unknown margin account affecting all profiles.

    Returns:
        All unresolved holds for this shared financial account, independent of
        credential revision, instrument, checkpoint or ordinary engine halt state.
    """
    evidence = AccountingObservationScope.model_validate(scope)
    result = await session.execute(_hold_query(evidence))
    return tuple(
        AccountingHoldView(row.public_id, row.raw_receipt_public_id, row.reason, row.margin_account)
        for row in result.scalars().all()
    )


async def require_accounting_scope_available(
    session: AsyncSession, scope: AccountingObservationScope
) -> None:
    """Refuse financial certification while any relevant durable hold remains.

    Args:
        session: A fenced CREATE transaction or the financial read's own session.
        scope: Actual account scope; symbol and credential rotation do not split it.
    """
    holds = await load_unresolved_accounting_holds(session, scope)
    if holds:
        raise AccountingScopeUnavailable(holds)
