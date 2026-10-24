"""Capture account-route authority from the observer's actual just-persisted response.

The executor supplies public metadata from the exact client it actually used,
including the selected credential revision and actual environment/API origin.
This producer does not infer those facts from current configuration or an old
unscoped account row. It performs no credential decryption or venue request.
"""

from dataclasses import dataclass
from datetime import UTC
from datetime import datetime

from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.core.execution_route_authority import ExecutionRouteAuthorityPayload
from snapper.core.execution_route_authority import ExecutionRouteSelection
from snapper.core.execution_route_authority import route_authority_content_digest
from snapper.core.execution_route_authority import route_selection_digest
from snapper.core.execution_unit_admission import ExecutionRouteReference
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance
from snapper.data.execution_route_authority_store import RouteAuthorityBindingError
from snapper.data.execution_route_authority_store import insert_execution_route_authority
from snapper.data.execution_unit_snapshot_store import SnapshotRepository
from snapper.data.execution_unit_snapshot_store import SnapshotWriteResult
from snapper.data.models import VenueAccountObservation
from snapper.data.models import VenueAccountState
from snapper.data.models import is_open_ended_version
from snapper.data.temporal_selection import one_or_none


@dataclass(frozen=True)
class CapturedExecutionRoute:
    """A committed authority and the exact reference the actual executor may recheck."""

    result: SnapshotWriteResult
    payload: ExecutionRouteAuthorityPayload
    reference: ExecutionRouteReference


async def _actual_observer_response(
    session: AsyncSession, selection: ExecutionRouteSelection, state_id: int
) -> tuple[VenueAccountState, VenueAccountObservation]:
    """Bind the same actual observer cycle rather than an older successful balance.

    Database time only narrows selection. The raw projected marker proves current
    membership without sampling the producer's later authoritative observation clock.
    """
    scope = selection.scope
    candidates = (
        await session.execute(
            select(VenueAccountState, VenueAccountState.known_to).where(
                VenueAccountState.id == state_id,
                VenueAccountState.known_to > func.current_timestamp(),
                VenueAccountState.wallet_public_id == scope.wallet_public_id,
                VenueAccountState.exchange == scope.exchange,
                VenueAccountState.mode == scope.mode,
            )
        )
    ).all()
    state = one_or_none(
        [candidate for candidate, known_to in candidates if is_open_ended_version(known_to)]
    )
    if state is None:
        raise RouteAuthorityBindingError("actual_observer_state_identity_unproven")
    if state.balance_payload_source_observation_id != state.current_attempt_observation_id:
        raise RouteAuthorityBindingError("actual_observer_response_not_fresh")
    observation = (
        await session.execute(
            select(VenueAccountObservation).where(
                VenueAccountObservation.id == state.current_attempt_observation_id
            )
        )
    ).scalar_one_or_none()
    if observation is None:
        raise RouteAuthorityBindingError("actual_observer_response_missing")
    return state, observation


def _authority_payload(
    selection: ExecutionRouteSelection,
    state: VenueAccountState,
    observation: VenueAccountObservation,
    observed_at: datetime,
) -> ExecutionRouteAuthorityPayload:
    """Copy actual response clocks/material without manufacturing an account identity or window."""
    return ExecutionRouteAuthorityPayload.model_validate(
        {
            "scope": selection.scope,
            "credential_public_id": selection.credential_public_id,
            "credential_timestamp": selection.credential_timestamp,
            "public_api_origin": selection.public_api_origin,
            "venue_account_id": None,
            "account_observation_public_id": observation.public_id,
            "account_observation_timestamp": observation.timestamp,
            "balance_observed_at": observation.balance_observed_at,
            "balance_receipt_json": observation.balances_json,
            "observed_at": observed_at,
            "valid_until": state.authoritative_until,
            "route_revision": route_selection_digest(
                selection.scope,
                selection.credential_public_id,
                selection.credential_timestamp,
                selection.public_api_origin,
            ),
        }
    )


async def capture_observed_execution_route(
    repository: SnapshotRepository,
    selection: ExecutionRouteSelection,
    account_state_id: int,
    identity: ExecutionEvidenceIdentity,
) -> CapturedExecutionRoute:
    """Commit authority for the response obtained by the exact actual selected client.

    The caller passes the state id returned by its own immediately preceding
    account-observer write and attests that the response came through this
    exact public client selection. Financial admission still independently
    checks current freshness/selection after this capture; the result never
    grants permission to place or resend an order.

    Args:
        repository: Actual account-observation and execution-fence provider.
        selection: Exact public route selected by the client that performed the read.
        account_state_id: Current state id returned by that actual observer cycle.
        identity: Immutable authority insertion identity, excluding its sampled clock.

    Returns:
        Committed authority, preserved decoded source material and an actual-route reference.
    """
    actual = ExecutionRouteSelection.model_validate(selection)
    stamp = ExecutionEvidenceIdentity.model_validate(identity)
    async with repository.session() as session:
        state, observation = await _actual_observer_response(session, actual, account_state_id)
    observed_at = datetime.now(UTC)
    payload = _authority_payload(actual, state, observation, observed_at)
    result = await insert_execution_route_authority(
        repository,
        payload,
        SnapshotWriteProvenance(
            public_id=stamp.public_id,
            session_id=stamp.session_id,
            sequence_id=stamp.sequence_id,
            timestamp=observed_at,
        ),
    )
    reference = ExecutionRouteReference(
        authority_public_id=result.public_id,
        authority_content_digest=route_authority_content_digest(payload),
        scope=actual.scope,
        route_revision=payload.route_revision,
    )
    return CapturedExecutionRoute(result, payload, reference)
