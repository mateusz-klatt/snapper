"""Bind private receive ownership inside the actual raw-and-hold capture transaction.

Only an actually issued original receive ticket can create an actual_receive
row. Recovery names a direct original retained sidecar. Historical readers
independently verify every raw, key-proof, root and source-clock copy without
using a later key check or current selected credential to promote older bytes.
"""

import base64
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime

from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.trade.execution_observation_recovery import _child_identity
from snapper.application.trade.execution_observation_recovery import _frame_basis
from snapper.application.trade.execution_observation_recovery import recoverable_frame_children
from snapper.core.contract_accounting_observation import AccountingObservationScope
from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.core.contract_accounting_observation import accounting_observation_digest
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.core.execution_route_authority import client_selection_digest
from snapper.core.native_account_identity import native_account_identity_digest
from snapper.core.private_frame_ticket import PrivateSourceFrameContext
from snapper.core.private_frame_ticket import require_authenticated_private_frame
from snapper.core.private_source_ownership import PrivateSourceOwnershipPayload
from snapper.core.private_source_ownership import PrivateSourceOwnershipReference
from snapper.core.private_source_ownership import private_source_ownership_digest
from snapper.core.private_source_ownership import private_source_ownership_projection
from snapper.data.accounting_observation_binding import AccountingObservationBindingError
from snapper.data.accounting_observation_binding import get_bound_accounting_observation
from snapper.data.contract_accounting_capture_session import StagedAccountingObservationError
from snapper.data.contract_accounting_capture_session import require_staged_accounting_observation
from snapper.data.models import AccountingRawReceipt
from snapper.data.models import PrivateSourceOwnershipReceipt
from snapper.data.models import is_open_ended_version
from snapper.data.native_account_identity_store import NativeAccountIdentityBindingError
from snapper.data.native_account_identity_store import get_bound_native_account_identity


class PrivateSourceOwnershipBindingError(ValueError):
    """The exact retained private source cannot establish its original authenticated relation."""


@dataclass(frozen=True)
class BoundPrivateSourceOwnership:
    """One independently verified historical relation and its actual key-proof-derived account UID."""

    public_id: str
    content_digest: str
    recorded_at: datetime
    payload: PrivateSourceOwnershipPayload
    account_uid: str

    @property
    def reference(self) -> PrivateSourceOwnershipReference:
        """Expose all six immutable source copies for a separate financial binder.

        Returns:
            Exact retained identity, raw identity, selector and actual recording clock.
        """
        return PrivateSourceOwnershipReference(
            public_id=self.public_id,
            content_digest=self.content_digest,
            raw_receipt_public_id=self.payload.raw_receipt_public_id,
            raw_content_digest=self.payload.raw_content_digest,
            target_json_pointer=self.payload.target_json_pointer,
            recorded_at=self.recorded_at,
        )


@dataclass(frozen=True)
class _RawSource:
    """Actual immutable raw row and its already verified original envelope."""

    row: AccountingRawReceipt
    payload: RawAccountingObservation


@dataclass(frozen=True)
class _VerifiedRelation:
    """Verified local relation plus its full original frame for direct-origin comparison."""

    bound: BoundPrivateSourceOwnership
    raw: RawAccountingObservation


def _require(condition: bool, reason: str) -> None:
    """Refuse one contradictory retained relation without inventing ownership or financial facts."""
    if not condition:
        raise PrivateSourceOwnershipBindingError(reason)


def _known_clock(known_at: datetime) -> None:
    """Require an explicit aware interpretation clock without filtering contradictory future rows."""
    _require(known_at.utcoffset() is not None, "private_ownership_clock_not_aware")


async def _raw_source(session: AsyncSession, public_id: str, known_at: datetime) -> _RawSource:
    """Load the actual row without a temporal filter and verify its complete indexed raw projection."""
    row = await session.scalar(
        select(AccountingRawReceipt).where(AccountingRawReceipt.public_id == public_id)
    )
    if row is None:
        raise PrivateSourceOwnershipBindingError("private_ownership_raw_missing")
    try:
        raw = await get_bound_accounting_observation(session, public_id)
    except AccountingObservationBindingError:
        raise PrivateSourceOwnershipBindingError("private_ownership_actual_raw_unproven") from None
    _require(
        raw.observed_at <= row.timestamp <= known_at and is_open_ended_version(row.known_to),
        "private_ownership_raw_knowledge_mismatch",
    )
    return _RawSource(row, raw)


def _verify_frame(raw: RawAccountingObservation, frame: PrivateSourceFrameContext) -> None:
    """Require the exact original client, whole wire frame and actual parser-selected child membership."""
    selected = frame.client_selection
    expected_scope = AccountingObservationScope(
        wallet_public_id=selected.wallet_public_id,
        exchange=selected.exchange,
        mode=selected.mode,
        environment=selected.environment,
    )
    _require(
        raw.scope == expected_scope
        and raw.route_revision == client_selection_digest(selected)
        and (raw.credential_public_id, raw.credential_timestamp)
        == (selected.credential_public_id, selected.credential_timestamp)
        and raw.channel == f"kraken_futures.ws.{frame.raw_feed}"
        and raw.representation == "wire_bytes_base64",
        "private_ownership_actual_raw_selection_mismatch",
    )
    try:
        children = recoverable_frame_children(raw)
    except (ValueError, UnicodeError, RecursionError):
        raise PrivateSourceOwnershipBindingError(
            "private_ownership_original_frame_unproven"
        ) from None
    _require(
        raw.target_json_pointer is None
        or any(child.target_json_pointer == raw.target_json_pointer for child in children),
        "private_ownership_selected_child_missing",
    )


async def _bound_owner(session: AsyncSession, frame: PrivateSourceFrameContext) -> str:
    """Derive the source account UID only from the original independently bound key proof and root."""
    try:
        actual = await get_bound_native_account_identity(session, frame.key_proof_public_id)
    except NativeAccountIdentityBindingError:
        raise PrivateSourceOwnershipBindingError(
            "private_ownership_original_key_proof_missing"
        ) from None
    proof = actual.payload
    digest = native_account_identity_digest(proof)
    root_id = actual.public_id if proof.owner_root is None else proof.owner_root.public_id
    try:
        root = await get_bound_native_account_identity(session, root_id)
    except NativeAccountIdentityBindingError:
        raise PrivateSourceOwnershipBindingError(
            "private_ownership_original_key_root_missing"
        ) from None
    root_digest = native_account_identity_digest(root.payload)
    _require(
        proof.matches_owner
        and proof.observation.client_selection == frame.client_selection
        and digest == frame.key_proof_content_digest
        and proof.recorded_at == frame.key_proof_recorded_at
        and proof.recorded_at <= frame.auth_encoded_at
        and root.public_id == frame.key_owner_root_public_id
        and root_digest == frame.key_owner_root_content_digest
        and root.payload.owner_root is None
        and root.payload.recorded_at <= frame.auth_encoded_at
        and root.payload.observation.account_uid == proof.observation.account_uid,
        "private_ownership_original_key_proof_mismatch",
    )
    return proof.observation.account_uid


async def _verify_material(
    session: AsyncSession, payload: PrivateSourceOwnershipPayload, known_at: datetime
) -> tuple[_RawSource, str]:
    """Rebind actual raw and safe owner facts before any new sidecar can be staged."""
    raw = await _raw_source(session, payload.raw_receipt_public_id, known_at)
    _verify_frame(raw.payload, payload.frame_context)
    _require(
        (
            raw.row.content_digest,
            raw.payload.target_json_pointer,
            raw.payload.observed_at,
            raw.row.timestamp,
            accounting_observation_digest(_frame_basis(raw.payload)),
        )
        == (
            payload.raw_content_digest,
            payload.target_json_pointer,
            payload.raw_observed_at,
            payload.raw_recorded_at,
            payload.frame_basis_digest,
        )
        and payload.recorded_at <= known_at,
        "private_ownership_actual_raw_projection_mismatch",
    )
    account_uid = await _bound_owner(session, payload.frame_context)
    return raw, account_uid


async def _verify_local_row(
    session: AsyncSession, row: PrivateSourceOwnershipReceipt, known_at: datetime
) -> _VerifiedRelation:
    """Check every indexed copy and original source without following an arbitrary origin chain."""
    try:
        payload = PrivateSourceOwnershipPayload.model_validate_json(row.canonical_payload)
    except ValidationError:
        raise PrivateSourceOwnershipBindingError("private_ownership_payload_invalid") from None
    projection = private_source_ownership_projection(payload)
    _require(
        all(getattr(row, field) == value for field, value in projection.items())
        and is_open_ended_version(row.known_to),
        "private_ownership_indexed_projection_mismatch",
    )
    raw, account_uid = await _verify_material(session, payload, known_at)
    bound = BoundPrivateSourceOwnership(
        row.public_id, row.content_digest, row.timestamp, payload, account_uid
    )
    return _VerifiedRelation(bound, raw.payload)


async def _verify_origin(
    session: AsyncSession,
    payload: PrivateSourceOwnershipPayload,
    raw: RawAccountingObservation,
    known_at: datetime,
) -> None:
    """Require one direct actual_receive origin and its full unchanged frame, clocks and selected scope."""
    if payload.binding_kind == "actual_receive":
        return
    row = await session.scalar(
        select(PrivateSourceOwnershipReceipt).where(
            PrivateSourceOwnershipReceipt.public_id == payload.origin_sidecar_public_id
        )
    )
    if row is None:
        raise PrivateSourceOwnershipBindingError("private_ownership_original_sidecar_missing")
    original = await _verify_local_row(session, row, known_at)
    origin = original.bound.payload
    _require(
        origin.binding_kind == "actual_receive"
        and origin.origin_sidecar_public_id is None
        and origin.origin_sidecar_content_digest is None
        and original.bound.content_digest == payload.origin_sidecar_content_digest
        and original.bound.recorded_at <= payload.recorded_at
        and origin.frame_context == payload.frame_context
        and origin.raw_observed_at == payload.raw_observed_at
        and origin.frame_basis_digest == payload.frame_basis_digest
        and _frame_basis(original.raw) == _frame_basis(raw),
        "private_ownership_direct_original_mismatch",
    )
    children = recoverable_frame_children(original.raw)
    _require(
        raw in children and payload.raw_receipt_public_id == _child_identity(raw),
        "private_ownership_recovered_child_changed",
    )


async def _bound_row(
    session: AsyncSession, row: PrivateSourceOwnershipReceipt, known_at: datetime
) -> BoundPrivateSourceOwnership:
    """Bind a row and at most one direct original, never a recursive or digest-selected provenance chain."""
    local = await _verify_local_row(session, row, known_at)
    _require(
        local.bound.public_id != local.bound.payload.origin_sidecar_public_id,
        "private_ownership_self_origin_refused",
    )
    await _verify_origin(session, local.bound.payload, local.raw, known_at)
    return local.bound


async def get_bound_private_source_ownership(
    session: AsyncSession, raw_receipt_public_id: str, known_at: datetime
) -> BoundPrivateSourceOwnership:
    """Read the exact raw-linked historical ownership relation without granting current account access.

    Args:
        session: Caller-owned read or financial transaction; no current credential is substituted.
        raw_receipt_public_id: Exact actual local raw receipt, never a frame-basis digest lookup.
        known_at: Actual consumer knowledge clock; future contradictory rows are refused explicitly.

    Returns:
        Independently verified sidecar, original source identity and key-proof-derived account UID.
    """
    _known_clock(known_at)
    row = await session.scalar(
        select(PrivateSourceOwnershipReceipt).where(
            PrivateSourceOwnershipReceipt.raw_receipt_public_id == raw_receipt_public_id
        )
    )
    if row is None:
        raise PrivateSourceOwnershipBindingError("private_ownership_sidecar_missing")
    return await _bound_row(session, row, known_at)


def _original_source_material(payload: PrivateSourceOwnershipPayload) -> dict[str, object]:
    """Compare all source facts while preserving the first valid retained origin and recording identity."""
    material: dict[str, object] = payload.model_dump(mode="python")
    for name in (
        "recorded_at",
        "binding_kind",
        "origin_sidecar_public_id",
        "origin_sidecar_content_digest",
    ):
        material.pop(name)
    return material


async def _persist_relation(
    session: AsyncSession,
    payload: PrivateSourceOwnershipPayload,
    identity: ExecutionEvidenceIdentity,
) -> BoundPrivateSourceOwnership:
    """Stage one fully verified relation or preserve the first exact existing raw relation and clock."""
    try:
        staged = require_staged_accounting_observation(session, payload.raw_receipt_public_id)
    except StagedAccountingObservationError:
        raise PrivateSourceOwnershipBindingError(
            "private_ownership_same_capture_transaction_required"
        ) from None
    _require(
        not staged.conflicted and staged.content_digest == payload.raw_content_digest,
        "private_ownership_actual_capture_result_mismatch",
    )
    raw, account_uid = await _verify_material(session, payload, payload.recorded_at)
    _require(
        identity.public_id != payload.origin_sidecar_public_id,
        "private_ownership_self_origin_refused",
    )
    await _verify_origin(session, payload, raw.payload, payload.recorded_at)
    occupied = await session.scalar(
        select(PrivateSourceOwnershipReceipt).where(
            PrivateSourceOwnershipReceipt.public_id == identity.public_id
        )
    )
    _require(
        occupied is None or occupied.raw_receipt_public_id == payload.raw_receipt_public_id,
        "private_ownership_public_identity_changed",
    )
    _require(
        occupied is None
        or (occupied.session_id, occupied.sequence_id)
        == (identity.session_id, identity.sequence_id),
        "private_ownership_insertion_identity_changed",
    )
    existing = await session.scalar(
        select(PrivateSourceOwnershipReceipt).where(
            PrivateSourceOwnershipReceipt.raw_receipt_public_id == payload.raw_receipt_public_id
        )
    )
    if existing is not None:
        original = await _bound_row(session, existing, payload.recorded_at)
        _require(
            _original_source_material(payload) == _original_source_material(original.payload),
            "private_ownership_existing_material_changed",
        )
        return original
    _require(staged.created, "private_ownership_preexisting_unowned_raw_refused")
    row = PrivateSourceOwnershipReceipt(
        **identity.model_dump(mode="python"), **private_source_ownership_projection(payload)
    )
    session.add(row)
    await session.flush()
    return BoundPrivateSourceOwnership(
        row.public_id,
        private_source_ownership_digest(payload),
        payload.recorded_at,
        payload,
        account_uid,
    )


async def capture_private_source_ownership(
    session: AsyncSession,
    raw_receipt_public_id: str,
    ticket: object,
    identity: ExecutionEvidenceIdentity,
) -> BoundPrivateSourceOwnership:
    """Stage original ownership only from an actually issued ticket bound to the exact retained wire.

    Args:
        session: Active caller-owned wallet-fenced raw-and-hold transaction; this helper never commits.
        raw_receipt_public_id: ACTUAL successful raw capture UUID; conflicted capture must not call here.
        ticket: Opaque issued original receive ticket; a public context or constructor is insufficient.
        identity: Stable producer identity for uncertain-commit retry, with no caller-selected clock.

    Returns:
        The staged verified relation, or the first identical retained relation on exact source reuse.
    """
    if not session.in_transaction():
        raise RuntimeError("Private source ownership requires the caller wallet-fenced transaction")
    proven = ExecutionEvidenceIdentity.model_validate(identity)
    now = datetime.now(UTC)
    raw = await _raw_source(session, raw_receipt_public_id, now)
    _require(
        raw.payload.representation == "wire_bytes_base64",
        "private_ownership_original_wire_required",
    )
    try:
        frame = require_authenticated_private_frame(
            ticket, base64.b64decode(raw.payload.material, validate=True)
        )
    except ValueError:
        raise PrivateSourceOwnershipBindingError(
            "private_ownership_actual_ticket_unproven"
        ) from None
    try:
        payload = PrivateSourceOwnershipPayload(
            binding_kind="actual_receive",
            frame_context=frame,
            raw_receipt_public_id=raw.row.public_id,
            raw_content_digest=raw.row.content_digest,
            target_json_pointer=raw.payload.target_json_pointer,
            frame_basis_digest=accounting_observation_digest(_frame_basis(raw.payload)),
            raw_observed_at=raw.payload.observed_at,
            raw_recorded_at=raw.row.timestamp,
            recorded_at=now,
        )
    except ValidationError:
        raise PrivateSourceOwnershipBindingError(
            "private_ownership_actual_receive_chronology_unproven"
        ) from None
    return await _persist_relation(session, payload, proven)


async def capture_recovered_private_source_ownership(
    session: AsyncSession,
    raw_receipt_public_id: str,
    origin_sidecar_public_id: str,
    identity: ExecutionEvidenceIdentity,
) -> BoundPrivateSourceOwnership:
    """Stage one parser-enumerated child against an explicitly supplied direct original receive row.

    Args:
        session: Active caller-owned wallet-fenced child raw-and-hold transaction; never committed here.
        raw_receipt_public_id: Actual recovered child raw UUID returned by its successful capture.
        origin_sidecar_public_id: Exact original actual_receive sidecar, never a later proof or digest.
        identity: Stable new projection identity without a caller-selected recording clock.

    Returns:
        The staged direct-original projection with its actual later raw recording clock.
    """
    if not session.in_transaction():
        raise RuntimeError("Private source recovery requires the caller wallet-fenced transaction")
    proven = ExecutionEvidenceIdentity.model_validate(identity)
    now = datetime.now(UTC)
    origin_row = await session.scalar(
        select(PrivateSourceOwnershipReceipt).where(
            PrivateSourceOwnershipReceipt.public_id == origin_sidecar_public_id
        )
    )
    if origin_row is None:
        raise PrivateSourceOwnershipBindingError("private_ownership_original_sidecar_missing")
    origin = await _bound_row(session, origin_row, now)
    _require(
        origin.payload.binding_kind == "actual_receive",
        "private_ownership_projection_origin_refused",
    )
    raw = await _raw_source(session, raw_receipt_public_id, now)
    try:
        payload = PrivateSourceOwnershipPayload(
            binding_kind="recovery_projection",
            frame_context=origin.payload.frame_context,
            raw_receipt_public_id=raw.row.public_id,
            raw_content_digest=raw.row.content_digest,
            target_json_pointer=raw.payload.target_json_pointer,
            frame_basis_digest=accounting_observation_digest(_frame_basis(raw.payload)),
            raw_observed_at=raw.payload.observed_at,
            raw_recorded_at=raw.row.timestamp,
            recorded_at=now,
            origin_sidecar_public_id=origin.public_id,
            origin_sidecar_content_digest=origin.content_digest,
        )
    except ValidationError:
        raise PrivateSourceOwnershipBindingError(
            "private_ownership_recovery_chronology_unproven"
        ) from None
    return await _persist_relation(session, payload, proven)
