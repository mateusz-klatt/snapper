"""Capture additive family-qualified account/unit evidence without performing venue I/O."""

from datetime import UTC
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.trade.unit_snapshot_submission import (
    SubmissionUnitSnapshotUnavailableError,
)
from snapper.application.trade.unit_snapshot_submission import _admission_payload
from snapper.application.trade.unit_snapshot_submission import _capture_original
from snapper.application.trade.unit_snapshot_submission import _current_command
from snapper.application.trade.unit_snapshot_submission import _provenance
from snapper.core.account_authority_reference import AccountAuthorityReference
from snapper.core.account_bound_submission_admission import AccountBoundSubmissionAdmissionPayload
from snapper.core.execution_unit_admission import ExecutionRouteReference
from snapper.core.execution_unit_admission import SubmissionEvidenceIdentity
from snapper.data.account_bound_submission_admission_store import (
    get_bound_account_submission_admission,
)
from snapper.data.account_bound_submission_admission_store import (
    insert_bound_account_submission_admission,
)
from snapper.data.execution_account_authority_store import BoundAccountAuthority
from snapper.data.execution_account_authority_store import require_current_account_authority
from snapper.data.execution_unit_admission_store import observe_submission_catalogs
from snapper.data.execution_unit_admission_store import require_submission_command_history
from snapper.data.execution_unit_snapshot_store import SnapshotBindingError
from snapper.data.execution_unit_snapshot_store import SnapshotRepository
from snapper.data.execution_unit_snapshot_store import SnapshotWriteResult
from snapper.data.models import SubmissionUnitAdmission
from snapper.data.native_account_authority_store import NativeAuthorityFreshnessPolicy
from snapper.messaging.schemas.data import OrderRequestData


def _reference(authority: BoundAccountAuthority) -> AccountAuthorityReference:
    """Use only actual verified row knowledge for the immutable account reference."""
    return AccountAuthorityReference(
        family=authority.family,
        public_id=authority.public_id,
        content_digest=authority.content_digest,
        recorded_at=authority.recorded_at,
    )


async def _retained_envelope(
    session: AsyncSession, current: AccountBoundSubmissionAdmissionPayload
) -> AccountBoundSubmissionAdmissionPayload:
    """Preserve the original attempt's clocks only after independently checking current evidence."""
    public_id = await session.scalar(
        select(SubmissionUnitAdmission.public_id).where(
            SubmissionUnitAdmission.attempt_public_id == current.units.attempt_public_id
        )
    )
    if public_id is None:
        return current
    retained = await get_bound_account_submission_admission(session, public_id)
    comparison = retained.payload.model_copy(
        update={
            "units": retained.payload.units.model_copy(
                update={"interpreted_at": current.units.interpreted_at}
            )
        }
    )
    if comparison != current:
        raise SnapshotBindingError("account_submission_retained_attempt_context_changed")
    return retained.payload


async def capture_bound_account_submission_admission(
    session: AsyncSession,
    order: OrderRequestData,
    route: ExecutionRouteReference,
    identity: SubmissionEvidenceIdentity,
    native_policy: NativeAuthorityFreshnessPolicy | None,
) -> SnapshotWriteResult:
    """Compose original units and actual source-family evidence in the caller's transaction.

    The caller holds the wallet execution fence before entering this function.
    Financial-owner assignment, funds, reservations, current order permission
    and durable venue dispatch remain independent requirements. An exact retry
    never grants another CREATE, and this producer performs no network call.

    Args:
        session: Actual caller-owned transaction after acquiring the execution fence.
        order: Outgoing frame whose exact operands and authorship must match durable intent.
        route: Actual selected account/environment and immutable authority identity.
        identity: Distinct original-snapshot/admission provenance and stable submit-attempt UUID.
        native_policy: Trusted source freshness configuration; absence refuses native binding.

    Returns:
        Immutable account/unit evidence without committing the broader financial transaction.

    Raises:
        SubmissionUnitSnapshotUnavailableError: When current account/source/unit proof is incomplete.
    """
    command_id: str | None = None
    try:
        actual = ExecutionRouteReference.model_validate(route)
        stamp = SubmissionEvidenceIdentity.model_validate(identity)
        interpreted_at = datetime.now(UTC)
        authority = await require_current_account_authority(
            session, actual, interpreted_at, native_policy
        )
        command = await _current_command(session, order, actual, interpreted_at)
        command_id = command.public_id
        first_known = await require_submission_command_history(session, command)
        observations = await observe_submission_catalogs(
            session, command, first_known, interpreted_at
        )
        baseline = await _capture_original(
            session, command, observations, _provenance(stamp.command_snapshot, interpreted_at)
        )
        current = AccountBoundSubmissionAdmissionPayload(
            units=_admission_payload(actual, stamp, baseline, command, observations),
            authority=_reference(authority),
        )
        payload = await _retained_envelope(session, current)
        return await insert_bound_account_submission_admission(
            session, payload, _provenance(stamp.admission, interpreted_at), native_policy
        )
    except Exception as error:
        raise SubmissionUnitSnapshotUnavailableError(
            command_id, "current_submission_account_source_unit_evidence_unavailable"
        ) from error


async def capture_account_submission_admission(
    repository: SnapshotRepository,
    order: OrderRequestData,
    route: ExecutionRouteReference,
    identity: SubmissionEvidenceIdentity,
    native_policy: NativeAuthorityFreshnessPolicy | None,
) -> SnapshotWriteResult:
    """Commit source/unit evidence when no broader financial transaction is being authored.

    Args:
        repository: Actual transaction and wallet-execution-fence provider.
        order: Outgoing frame retaining the original durable intent and authorship.
        route: Actual selected account/environment with its immutable authority reference.
        identity: Stable submit attempt and distinct immutable record identities.
        native_policy: Trusted source freshness configuration for native account binding.

    Returns:
        Committed evidence independent of order permission, funding or permission to resend.
    """
    actual = ExecutionRouteReference.model_validate(route)
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await repository._acquire_execution_fence_lock(session, actual.scope.wallet_public_id)
        result = await capture_bound_account_submission_admission(
            session, order, actual, identity, native_policy
        )
        await session.commit()
        return result
