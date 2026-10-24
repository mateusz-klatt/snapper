"""Bind additive admission envelopes to exact account families without rewriting older evidence.

Historical reads select their explicit family. Current writes independently
require unique current discovery and the latest eligible native source under
the caller's wallet fence. Neither path grants financial ownership, funding,
order permission or permission to repeat a venue request.
"""

from dataclasses import dataclass
from datetime import datetime

from pydantic import TypeAdapter
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.core.account_authority_reference import AccountAuthorityFamily
from snapper.core.account_authority_reference import AccountAuthorityPayload
from snapper.core.account_authority_reference import AccountAuthorityReference
from snapper.core.account_bound_submission_admission import AccountBoundSubmissionAdmissionPayload
from snapper.core.account_bound_submission_admission import account_bound_admission_digest
from snapper.core.account_bound_submission_admission import canonical_account_bound_admission
from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.execution_route_authority import client_selection_from_authority
from snapper.core.execution_unit_admission import SubmissionUnitAdmissionPayload
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance
from snapper.core.native_account_authority import NativeAccountAuthorityPayload
from snapper.core.native_account_authority import client_selection_from_native_authority
from snapper.data.execution_account_authority_store import BoundAccountAuthority
from snapper.data.execution_account_authority_store import get_bound_account_authority_reference
from snapper.data.execution_account_authority_store import require_current_account_authority
from snapper.data.execution_route_authority_store import get_bound_route_authority
from snapper.data.execution_unit_admission_store import UnitAdmissionConflictError
from snapper.data.execution_unit_admission_store import _current_command
from snapper.data.execution_unit_admission_store import admission_material_projection
from snapper.data.execution_unit_admission_store import admission_record_values
from snapper.data.execution_unit_admission_store import admission_row_projection
from snapper.data.execution_unit_admission_store import bind_submission_unit_graph
from snapper.data.execution_unit_admission_store import get_bound_submission_admission
from snapper.data.execution_unit_snapshot_store import SnapshotBindingError
from snapper.data.execution_unit_snapshot_store import SnapshotWriteResult
from snapper.data.models import ExecutionUnitSnapshot
from snapper.data.models import SubmissionUnitAdmission
from snapper.data.models import is_open_ended_version
from snapper.data.native_account_authority_store import NativeAuthorityFreshnessPolicy

_ADMISSION_ADAPTER: TypeAdapter[
    SubmissionUnitAdmissionPayload | AccountBoundSubmissionAdmissionPayload
] = TypeAdapter(SubmissionUnitAdmissionPayload | AccountBoundSubmissionAdmissionPayload)


@dataclass(frozen=True, slots=True)
class BoundAccountSubmissionAdmission:
    """Exact retained envelope and independently rebound source with original local knowledge."""

    public_id: str
    content_digest: str
    recorded_at: datetime
    payload: AccountBoundSubmissionAdmissionPayload
    authority: BoundAccountAuthority


@dataclass(frozen=True, slots=True)
class _HistoricalAdmissionAccount:
    """A version-specific verified prior route used only for command/account continuity."""

    family: AccountAuthorityFamily
    units: SubmissionUnitAdmissionPayload
    authority: AccountAuthorityPayload


def _require(condition: bool, reason: str) -> None:
    """Refuse an incomplete graph without replacing any original retained source."""
    if not condition:
        raise SnapshotBindingError(reason)


def _authority_reference(bound: BoundAccountAuthority) -> AccountAuthorityReference:
    """Project actual bound row knowledge rather than accepting a frame-provided family."""
    return AccountAuthorityReference(
        family=bound.family,
        public_id=bound.public_id,
        content_digest=bound.content_digest,
        recorded_at=bound.recorded_at,
    )


def _verify_route(
    payload: AccountBoundSubmissionAdmissionPayload, bound: BoundAccountAuthority
) -> None:
    """Require exact family, original knowledge, account scope and selected route."""
    route = payload.units.route
    _require(
        _authority_reference(bound) == payload.authority
        and (bound.payload.scope, bound.payload.route_revision)
        == (route.scope, route.route_revision),
        "account_admission_authority_reference_mismatch",
    )


def _verify_projection(
    row: SubmissionUnitAdmission, payload: AccountBoundSubmissionAdmissionPayload
) -> None:
    """Bind all actual indexed values and immutable clocks to the complete outer envelope."""
    actual = admission_row_projection(row) + (row.content_digest, row.canonical_payload)
    expected = admission_material_projection(payload.units) + (
        account_bound_admission_digest(payload),
        canonical_account_bound_admission(payload),
    )
    _require(actual == expected, "account_admission_projection_mismatch")
    _require(
        is_open_ended_version(row.known_to)
        and row.timestamp.utcoffset() is not None
        and row.timestamp >= payload.units.interpreted_at,
        "account_admission_immutable_knowledge_mismatch",
    )


async def get_bound_account_submission_admission(
    session: AsyncSession, public_id: str
) -> BoundAccountSubmissionAdmission:
    """Read the exact historical family without applying current eligibility or new credentials.

    Args:
        session: Caller-owned replay or retained-source interpretation transaction.
        public_id: Exact immutable version-three admission identity.

    Returns:
        Verified original account/unit evidence and its actual local recording clock.
    """
    row = await session.scalar(
        select(SubmissionUnitAdmission).where(SubmissionUnitAdmission.public_id == public_id)
    )
    if row is None:
        raise SnapshotBindingError("account_admission_missing")
    payload = AccountBoundSubmissionAdmissionPayload.model_validate_json(row.canonical_payload)
    _verify_projection(row, payload)
    authority = await get_bound_account_authority_reference(session, payload.authority)
    _verify_route(payload, authority)
    return BoundAccountSubmissionAdmission(
        row.public_id, row.content_digest, row.timestamp, payload, authority
    )


async def _historical_account(
    session: AsyncSession, row: SubmissionUnitAdmission
) -> _HistoricalAdmissionAccount:
    """Dispatch by a strict outer version while retaining both original family readers."""
    candidate = _ADMISSION_ADAPTER.validate_json(row.canonical_payload)
    if isinstance(candidate, AccountBoundSubmissionAdmissionPayload):
        bound = await get_bound_account_submission_admission(session, row.public_id)
        return _HistoricalAdmissionAccount(
            bound.authority.family, bound.payload.units, bound.authority.payload
        )
    units = await get_bound_submission_admission(session, row.public_id)
    legacy = await get_bound_route_authority(session, units.route.authority_public_id)
    return _HistoricalAdmissionAccount("legacy_route_v1", units, legacy)


def _client_selection(authority: AccountAuthorityPayload) -> ExecutionClientSelection:
    """Use the corresponding source-specific projection without merging canonical payloads."""
    if isinstance(authority, NativeAccountAuthorityPayload):
        return client_selection_from_native_authority(authority)
    return client_selection_from_authority(authority)


def _verify_account_continuity(
    previous: _HistoricalAdmissionAccount,
    current: BoundAccountAuthority,
    payload: AccountBoundSubmissionAdmissionPayload,
) -> None:
    """Pin family, account and environment independently of source freshness or credential age."""
    _require(
        previous.family == current.family, "account_admission_command_authority_family_changed"
    )
    _require(
        previous.units.route.scope == payload.units.route.scope,
        "account_admission_command_account_changed",
    )
    old = previous.authority
    new = current.payload
    known_account = old.venue_account_id is not None and new.venue_account_id is not None
    same_account = old.venue_account_id == new.venue_account_id
    _require(not known_account or same_account, "account_admission_venue_account_changed")
    _require(
        _client_selection(old) == _client_selection(new) or (known_account and same_account),
        "account_admission_credential_rotation_account_unproven",
    )


async def _require_account_continuity(
    session: AsyncSession,
    payload: AccountBoundSubmissionAdmissionPayload,
    authority: BoundAccountAuthority,
    as_of: datetime,
) -> None:
    """Rebind every prior command/CID witness instead of prioritizing an authority family."""
    request = payload.units.request_material
    rows = (
        await session.scalars(
            select(SubmissionUnitAdmission).where(
                or_(
                    SubmissionUnitAdmission.command_public_id == request.command_public_id,
                    (SubmissionUnitAdmission.exchange == request.exchange)
                    & (SubmissionUnitAdmission.client_order_id == request.client_order_id),
                )
            )
        )
    ).all()
    for row in rows:
        _require(row.timestamp <= as_of, "account_admission_prior_attempt_not_yet_known")
        previous = await _historical_account(session, row)
        _verify_account_continuity(previous, authority, payload)


async def _bind_current(
    session: AsyncSession,
    payload: AccountBoundSubmissionAdmissionPayload,
    provenance: SnapshotWriteProvenance,
    native_policy: NativeAuthorityFreshnessPolicy | None,
) -> None:
    """Recheck actual current account and economic evidence before both insertion and retry."""
    _require(
        provenance.timestamp >= payload.units.interpreted_at,
        "account_admission_insert_precedes_interpretation",
    )
    authority = await require_current_account_authority(
        session, payload.units.route, provenance.timestamp, native_policy
    )
    _verify_route(payload, authority)
    command = await _current_command(session, payload.units.request_material, provenance.timestamp)
    await _require_account_continuity(session, payload, authority, provenance.timestamp)
    await bind_submission_unit_graph(session, payload.units, command, provenance.timestamp)
    snapshot_recorded_at = (
        await session.execute(
            select(ExecutionUnitSnapshot.timestamp).where(
                ExecutionUnitSnapshot.public_id == payload.units.command_snapshot_public_id
            )
        )
    ).scalar_one()
    _require(
        snapshot_recorded_at <= payload.units.interpreted_at,
        "account_admission_original_snapshot_not_yet_known",
    )


async def _existing_attempt(
    session: AsyncSession,
    payload: AccountBoundSubmissionAdmissionPayload,
    provenance: SnapshotWriteProvenance,
) -> SnapshotWriteResult | None:
    """Keep attempt uniqueness across versions and refuse a conflicting insertion UUID."""
    rows = (
        await session.scalars(
            select(SubmissionUnitAdmission).where(
                or_(
                    SubmissionUnitAdmission.attempt_public_id == payload.units.attempt_public_id,
                    SubmissionUnitAdmission.public_id == provenance.public_id,
                )
            )
        )
    ).all()
    for row in rows:
        if row.attempt_public_id != payload.units.attempt_public_id:
            raise UnitAdmissionConflictError("account_admission_public_identity_conflict")
    if not rows:
        return None
    row = rows[0]
    digest = account_bound_admission_digest(payload)
    if row.content_digest != digest:
        raise UnitAdmissionConflictError("account_admission_attempt_evidence_changed")
    _verify_projection(row, payload)
    return SnapshotWriteResult(row.id, row.public_id, digest, False)


async def insert_bound_account_submission_admission(
    session: AsyncSession,
    payload: AccountBoundSubmissionAdmissionPayload,
    provenance: SnapshotWriteProvenance,
    native_policy: NativeAuthorityFreshnessPolicy | None,
) -> SnapshotWriteResult:
    """Insert one source-bound envelope inside an existing execution transaction.

    Args:
        session: Caller-owned transaction already holding the actual wallet execution fence.
        payload: Actual attempt with the explicit independently rebound account reference.
        provenance: Stable insertion identity and actual local producer knowledge clock.
        native_policy: Trusted native freshness policy; absence refuses native current binding.

    Returns:
        The immutable original admission identity without committing or granting venue I/O.
    """
    proven = AccountBoundSubmissionAdmissionPayload.model_validate(payload)
    stamp = SnapshotWriteProvenance.model_validate(provenance)
    await _bind_current(session, proven, stamp, native_policy)
    existing = await _existing_attempt(session, proven, stamp)
    if existing is not None:
        return existing
    digest = account_bound_admission_digest(proven)
    row = SubmissionUnitAdmission(
        **admission_record_values(proven.units, stamp),
        content_digest=digest,
        canonical_payload=canonical_account_bound_admission(proven),
    )
    session.add(row)
    await session.flush()
    return SnapshotWriteResult(row.id, row.public_id, digest, True)
