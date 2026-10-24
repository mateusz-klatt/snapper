"""Retain redacted selected-key identity and its immutable first observed owner.

Every proof belongs to an actual committed account-read attempt. The initial
proof anchors the direct wallet family independently of credential revision;
later conflicting account UIDs remain evidence rather than replacing that root.
Historical rebinding makes no claim about present permissions or collateral.
"""

from dataclasses import dataclass
from datetime import UTC
from datetime import datetime

from pydantic import ValidationError
from sqlalchemy import Select
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.core.execution_route_authority import client_selection_digest
from snapper.core.native_account_identity import NativeAccountIdentityProofPayload
from snapper.core.native_account_identity import NativeAccountIdentityRootReference
from snapper.core.native_account_identity import NativeAccountPublicIdentity
from snapper.core.native_account_identity import canonical_native_account_identity
from snapper.core.native_account_identity import native_account_identity_digest
from snapper.core.native_account_read import native_account_read_attempt_digest
from snapper.core.native_account_read import native_account_read_family_digest
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import NativeAccountIdentityProof
from snapper.data.native_account_read_store import get_bound_native_account_read_attempt
from snapper.data.native_account_read_store import get_bound_native_account_read_outcome


class NativeAccountIdentityBindingError(ValueError):
    """An actual attempt, original owner or indexed public proof cannot be rebound."""


@dataclass(frozen=True, slots=True)
class BoundNativeAccountIdentityProof:
    """Verified public identity with its actual attempt and independently rebound owner root."""

    public_id: str
    payload: NativeAccountIdentityProofPayload


def _require(condition: bool, reason: str) -> None:
    """Refuse inconsistent evidence without selecting a replacement owner."""
    if not condition:
        raise NativeAccountIdentityBindingError(reason)


def _root_query(selection: ExecutionClientSelection) -> Select[NativeAccountIdentityProof]:
    """Find the first retained owner using direct family columns, without credential partitioning."""
    return select(NativeAccountIdentityProof).where(
        NativeAccountIdentityProof.wallet_public_id == selection.wallet_public_id,
        NativeAccountIdentityProof.exchange == selection.exchange,
        NativeAccountIdentityProof.mode == selection.mode,
        NativeAccountIdentityProof.environment == selection.environment,
        NativeAccountIdentityProof.owner_root_public_id.is_(None),
    )


def _verify_projection(
    row: NativeAccountIdentityProof, payload: NativeAccountIdentityProofPayload
) -> None:
    """Check every indexed field and immutable temporal coordinate against canonical public bytes."""
    observation = payload.observation
    selected = observation.client_selection
    root = payload.owner_root
    _require(
        (
            row.wallet_public_id,
            row.exchange,
            row.mode,
            row.environment,
            row.margin_account,
            row.scope_digest,
            row.family_sequence,
            row.client_selection_digest,
            row.known_to,
        )
        == (
            selected.wallet_public_id,
            selected.exchange,
            selected.mode,
            selected.environment,
            None,
            native_account_read_family_digest(selected),
            payload.attempt.family_sequence,
            client_selection_digest(selected),
            KNOWN_TO_MAX,
        ),
        "native_identity_scope_projection_mismatch",
    )
    _require(
        (
            row.attempt_public_id,
            row.attempt_content_digest,
            row.owner_root_public_id,
            row.owner_root_content_digest,
            row.owner_root_account_uid,
            row.source_policy,
            row.account_uid,
            row.general_access,
            row.transfer_access,
            row.request_started_at,
            row.observed_at,
            row.timestamp,
            row.content_digest,
            row.canonical_payload,
        )
        == (
            payload.attempt_public_id,
            payload.attempt_content_digest,
            None if root is None else root.public_id,
            None if root is None else root.content_digest,
            None if root is None else root.account_uid,
            observation.source_policy,
            observation.account_uid,
            observation.general_access,
            observation.transfer_access,
            observation.request_started_at,
            observation.observed_at,
            payload.recorded_at,
            native_account_identity_digest(payload),
            canonical_native_account_identity(payload),
        ),
        "native_identity_public_projection_mismatch",
    )


async def _bound_material(
    session: AsyncSession, row: NativeAccountIdentityProof
) -> BoundNativeAccountIdentityProof:
    """Rebind canonical identity, all indexes and actual request before considering its root."""
    try:
        payload = NativeAccountIdentityProofPayload.model_validate_json(row.canonical_payload)
    except ValidationError:
        raise NativeAccountIdentityBindingError("native_identity_payload_invalid") from None
    _verify_projection(row, payload)
    attempt = await get_bound_native_account_read_attempt(session, payload.attempt_public_id)
    _require(attempt.payload == payload.attempt, "native_identity_actual_attempt_mismatch")
    return BoundNativeAccountIdentityProof(row.public_id, payload)


async def _bound_identity(
    session: AsyncSession, row: NativeAccountIdentityProof
) -> BoundNativeAccountIdentityProof:
    """Require the single actual first owner rather than trusting a caller-authored root reference."""
    bound = await _bound_material(session, row)
    payload = bound.payload
    actual_root = (
        await session.execute(_root_query(payload.observation.client_selection))
    ).scalar_one_or_none()
    if actual_root is None:
        raise NativeAccountIdentityBindingError("native_identity_owner_root_missing")
    if payload.owner_root is None:
        _require(actual_root is row, "native_identity_initial_owner_mismatch")
        return bound
    root = await _bound_material(session, actual_root)
    _require(
        root.payload.owner_root is None
        and root.payload.recorded_at <= payload.recorded_at
        and payload.owner_root
        == NativeAccountIdentityRootReference(
            public_id=root.public_id,
            content_digest=native_account_identity_digest(root.payload),
            account_uid=root.payload.observation.account_uid,
        ),
        "native_identity_actual_owner_root_mismatch",
    )
    return bound


async def get_bound_native_account_identity(
    session: AsyncSession, public_id: str
) -> BoundNativeAccountIdentityProof:
    """Reload one historical proof and independently verify its actual request and original owner.

    Args:
        session: Caller-owned database transaction without an implicit current-credential claim.
        public_id: Retained public proof identity, never a caller-authored account observation.

    Returns:
        Historical public proof, preserving an honestly conflicting observed owner when present.
    """
    row = (
        await session.execute(
            select(NativeAccountIdentityProof).where(
                NativeAccountIdentityProof.public_id == public_id
            )
        )
    ).scalar_one_or_none()
    if row is None:
        raise NativeAccountIdentityBindingError("native_identity_proof_missing")
    return await _bound_identity(session, row)


async def find_native_account_identity(
    session: AsyncSession, attempt_public_id: str
) -> BoundNativeAccountIdentityProof | None:
    """Find the one proof owned by an actual attempt without substituting an older credential proof.

    Args:
        session: Caller-owned historical or wallet-fenced read transaction.
        attempt_public_id: Exact account-read attempt whose identity preflight is being inspected.

    Returns:
        The verified proof, or explicit absence if this attempt has no retained key identity.
    """
    row = (
        await session.execute(
            select(NativeAccountIdentityProof).where(
                NativeAccountIdentityProof.attempt_public_id == attempt_public_id
            )
        )
    ).scalar_one_or_none()
    return None if row is None else await _bound_identity(session, row)


async def _owner_reference(
    session: AsyncSession, selection: ExecutionClientSelection
) -> NativeAccountIdentityRootReference | None:
    """Keep the first retained owner stable even when a lower-ordinal request completes later."""
    row = (await session.execute(_root_query(selection))).scalar_one_or_none()
    if row is None:
        return None
    root = await _bound_identity(session, row)
    return NativeAccountIdentityRootReference(
        public_id=root.public_id,
        content_digest=native_account_identity_digest(root.payload),
        account_uid=root.payload.observation.account_uid,
    )


def _proof_row(
    payload: NativeAccountIdentityProofPayload, identity: ExecutionEvidenceIdentity
) -> NativeAccountIdentityProof:
    """Project only strict whitelisted proof material into immutable indexed storage."""
    observation = payload.observation
    selected = observation.client_selection
    return NativeAccountIdentityProof(
        **identity.model_dump(mode="python"),
        timestamp=payload.recorded_at,
        wallet_public_id=selected.wallet_public_id,
        exchange=selected.exchange,
        mode=selected.mode,
        environment=selected.environment,
        margin_account=None,
        scope_digest=native_account_read_family_digest(selected),
        family_sequence=payload.attempt.family_sequence,
        client_selection_digest=client_selection_digest(selected),
        attempt_public_id=payload.attempt_public_id,
        attempt_content_digest=payload.attempt_content_digest,
        owner_root_public_id=None if payload.owner_root is None else payload.owner_root.public_id,
        owner_root_content_digest=(
            None if payload.owner_root is None else payload.owner_root.content_digest
        ),
        owner_root_account_uid=(
            None if payload.owner_root is None else payload.owner_root.account_uid
        ),
        source_policy=observation.source_policy,
        account_uid=observation.account_uid,
        general_access=observation.general_access,
        transfer_access=observation.transfer_access,
        request_started_at=observation.request_started_at,
        observed_at=observation.observed_at,
        content_digest=native_account_identity_digest(payload),
        canonical_payload=canonical_native_account_identity(payload),
    )


async def _retain_under_fence(
    session: AsyncSession,
    attempt_public_id: str,
    observation: NativeAccountPublicIdentity,
    identity: ExecutionEvidenceIdentity,
) -> BoundNativeAccountIdentityProof:
    """Retain source knowledge once, before the corresponding account read may finish."""
    now = datetime.now(UTC)
    attempt = await get_bound_native_account_read_attempt(session, attempt_public_id)
    existing = await find_native_account_identity(session, attempt_public_id)
    if existing is not None:
        _require(
            existing.public_id == identity.public_id
            and existing.payload.observation == observation,
            "native_identity_attempt_proof_changed",
        )
        _require(existing.payload.recorded_at <= now, "native_identity_knowledge_clock_regressed")
        return existing
    outcome = await get_bound_native_account_read_outcome(session, attempt_public_id)
    _require(outcome is None, "native_identity_attempt_already_finished")
    occupied = (
        await session.execute(
            select(NativeAccountIdentityProof.public_id).where(
                NativeAccountIdentityProof.public_id == identity.public_id
            )
        )
    ).scalar_one_or_none()
    _require(occupied is None, "native_identity_public_id_already_used")
    root = await _owner_reference(session, attempt.payload.client_selection)
    if root is not None:
        previous = await get_bound_native_account_identity(session, root.public_id)
        _require(previous.payload.recorded_at <= now, "native_identity_knowledge_clock_regressed")
    payload = NativeAccountIdentityProofPayload(
        attempt_public_id=attempt.public_id,
        attempt_content_digest=native_account_read_attempt_digest(attempt.payload),
        attempt=attempt.payload,
        observation=observation,
        recorded_at=now,
        owner_root=root,
    )
    row = _proof_row(payload, identity)
    session.add(row)
    await session.flush()
    return await _bound_identity(session, row)


async def retain_native_account_identity(
    repository: AccountingRepository,
    attempt_public_id: str,
    observation: NativeAccountPublicIdentity,
    identity: ExecutionEvidenceIdentity,
) -> BoundNativeAccountIdentityProof:
    """Commit safe key identity before accounts I/O while preserving an immutable owner root.

    Args:
        repository: Actual transaction provider and shared wallet execution fence.
        attempt_public_id: Previously committed attempt that owns this closed producer's key check.
        observation: Whitelisted result of the actual selected-key HTTP operation.
        identity: Stable local proof identity for retry after an uncertain commit acknowledgement.

    Returns:
        The committed public proof, with conflicts visible and exact retries retaining original clocks.
    """
    proven = NativeAccountPublicIdentity.model_validate(observation)
    reference = ExecutionEvidenceIdentity.model_validate(identity)
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await repository._acquire_execution_fence_lock(
            session, proven.client_selection.wallet_public_id
        )
        bound = await _retain_under_fence(session, attempt_public_id, proven, reference)
        await session.commit()
        return bound
