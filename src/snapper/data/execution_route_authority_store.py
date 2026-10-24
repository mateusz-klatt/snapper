"""Fenced immutable account-route witnesses with separate historical and current reads.

Only public credential metadata is queried. A runtime adapter must attest that
the persisted account observation came through its actual selected client;
an old account-observation row alone cannot prove that transport binding.
"""

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.core.execution_route_authority import ExecutionRouteAuthorityPayload
from snapper.core.execution_route_authority import account_scope_digest
from snapper.core.execution_route_authority import canonical_route_authority_payload
from snapper.core.execution_route_authority import route_authority_content_digest
from snapper.core.execution_unit_admission import ExecutionRouteReference
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance
from snapper.data.execution_unit_snapshot_store import SnapshotRepository
from snapper.data.execution_unit_snapshot_store import SnapshotWriteResult
from snapper.data.models import ExecutionRouteAuthority
from snapper.data.models import VenueAccountObservation
from snapper.data.models import VenueAccountState
from snapper.data.models import Wallet
from snapper.data.models import WalletCredential
from snapper.data.models import is_open_ended_version
from snapper.data.temporal_selection import one_or_none

_API_ORIGINS = {
    ("kraken", "production"): "https://api.kraken.com",
    ("kraken_futures", "production"): "https://futures.kraken.com",
    ("kraken_futures", "demo"): "https://demo-futures.kraken.com",
    ("walutomat", "production"): "https://api.walutomat.pl",
}


class RouteAuthorityBindingError(ValueError):
    """Actual wallet, selected credential or current account evidence cannot prove authority."""


class RouteAuthorityConflictError(ValueError):
    """An immutable route authority identity already has different evidence."""


def _require(condition: bool, reason: str) -> None:
    """Fail closed without interpreting a stale or missing response as account truth."""
    if not condition:
        raise RouteAuthorityBindingError(reason)


async def _verify_selected_credential(
    session: AsyncSession, payload: ExecutionRouteAuthorityPayload, as_of: datetime
) -> None:
    """Read public credential metadata without selecting or decrypting its secret payload."""
    scope = payload.scope
    result = await session.execute(
        select(
            WalletCredential.public_id,
            WalletCredential.timestamp,
            WalletCredential.credential_type,
            WalletCredential.known_to,
        ).where(
            WalletCredential.wallet_public_id == scope.wallet_public_id,
            WalletCredential.exchange == scope.exchange,
            WalletCredential.known_to > as_of,
            WalletCredential.timestamp <= as_of,
        )
    )
    row = one_or_none(
        [candidate for candidate in result.all() if is_open_ended_version(candidate.known_to)]
    )
    if payload.credential_public_id is None:
        _require(scope.mode == "paper" and row is None, "selected_credential_identity_omitted")
        return
    if row is None:
        raise RouteAuthorityBindingError("selected_credential_revision_missing")
    _require(
        (row.public_id, row.timestamp)
        == (payload.credential_public_id, payload.credential_timestamp),
        "selected_credential_revision_changed",
    )
    _require(
        (row.credential_type == "paper") == (scope.mode == "paper"),
        "selected_credential_mode_mismatch",
    )


async def _verify_wallet_and_origin(
    session: AsyncSession, payload: ExecutionRouteAuthorityPayload, as_of: datetime
) -> None:
    """Verify the current wallet mode and the explicit supported API environment."""
    scope = payload.scope
    candidates = (
        await session.execute(
            select(Wallet, Wallet.known_to).where(
                Wallet.public_id == scope.wallet_public_id,
                Wallet.known_to > as_of,
                Wallet.timestamp <= as_of,
            )
        )
    ).all()
    wallet = one_or_none(
        [candidate for candidate, known_to in candidates if is_open_ended_version(known_to)]
    )
    if wallet is None:
        raise RouteAuthorityBindingError("route_wallet_missing")
    _require(wallet.is_paper == (scope.mode == "paper"), "route_wallet_mode_mismatch")
    if scope.mode == "paper":
        _require(payload.public_api_origin is None, "paper_route_origin_mismatch")
    else:
        expected = _API_ORIGINS.get((scope.exchange, scope.environment))
        _require(
            expected is not None and payload.public_api_origin == expected,
            "actual_api_environment_unproven",
        )
    await _verify_selected_credential(session, payload, as_of)


async def _verify_observation(
    session: AsyncSession, payload: ExecutionRouteAuthorityPayload
) -> VenueAccountObservation:
    """Bind the exact successful balance observation and its preserved decoded material."""
    observation = (
        await session.execute(
            select(VenueAccountObservation).where(
                VenueAccountObservation.public_id == payload.account_observation_public_id,
                VenueAccountObservation.timestamp == payload.account_observation_timestamp,
            )
        )
    ).scalar_one_or_none()
    if observation is None:
        raise RouteAuthorityBindingError("route_account_observation_missing")
    scope = payload.scope
    status = "simulated" if scope.mode == "paper" else "observed"
    _require(
        (
            observation.wallet_public_id,
            observation.exchange,
            observation.mode,
            observation.attempt_status,
            observation.balance_status,
            observation.balance_observed_at,
            observation.balances_json,
        )
        == (
            scope.wallet_public_id,
            scope.exchange,
            scope.mode,
            status,
            status,
            payload.balance_observed_at,
            payload.balance_receipt_json,
        ),
        "route_account_observation_mismatch",
    )
    return observation


async def _verify_current_account_state(
    session: AsyncSession,
    payload: ExecutionRouteAuthorityPayload,
    observation: VenueAccountObservation,
    as_of: datetime,
) -> None:
    """Refuse retained/failed account states and authority beyond the actual source window."""
    scope = payload.scope
    candidates = (
        await session.execute(
            select(VenueAccountState, VenueAccountState.known_to).where(
                VenueAccountState.wallet_public_id == scope.wallet_public_id,
                VenueAccountState.exchange == scope.exchange,
                VenueAccountState.mode == scope.mode,
                VenueAccountState.known_to > as_of,
                VenueAccountState.timestamp <= as_of,
            )
        )
    ).all()
    state = one_or_none(
        [candidate for candidate, known_to in candidates if is_open_ended_version(known_to)]
    )
    if state is None:
        raise RouteAuthorityBindingError("route_current_account_state_missing")
    status = "simulated" if scope.mode == "paper" else "observed"
    _require(
        (
            state.sync_status,
            state.balance_status,
            state.current_attempt_observation_id,
            state.balance_payload_source_observation_id,
            state.balance_observed_at,
            state.balances_json,
        )
        == (
            status,
            status,
            observation.id,
            observation.id,
            payload.balance_observed_at,
            payload.balance_receipt_json,
        ),
        "route_current_account_evidence_changed",
    )
    _require(
        state.authoritative_until is not None and payload.valid_until <= state.authoritative_until,
        "route_authority_exceeds_source_window",
    )


async def _verify_current_payload(
    session: AsyncSession, payload: ExecutionRouteAuthorityPayload, as_of: datetime
) -> None:
    """Require actual current selection and fresh source evidence before a new financial write."""
    _require(as_of.utcoffset() is not None, "route_authority_clock_not_aware")
    _require(payload.observed_at <= as_of < payload.valid_until, "route_authority_not_current")
    await _verify_wallet_and_origin(session, payload, as_of)
    observation = await _verify_observation(session, payload)
    await _verify_current_account_state(session, payload, observation, as_of)


def _verify_projection(
    row: ExecutionRouteAuthority, payload: ExecutionRouteAuthorityPayload
) -> None:
    """Check indexed copies against the complete immutable canonical evidence."""
    scope = payload.scope
    actual = (
        row.wallet_public_id,
        row.exchange,
        row.mode,
        row.environment,
        row.margin_account,
        row.account_scope_digest,
        row.credential_public_id,
        row.credential_timestamp,
        row.venue_account_id,
        row.account_observation_public_id,
        row.account_observation_timestamp,
        row.observed_at,
        row.valid_until,
        row.route_revision,
        row.content_digest,
        row.canonical_payload,
    )
    expected = (
        scope.wallet_public_id,
        scope.exchange,
        scope.mode,
        scope.environment,
        scope.margin_account,
        account_scope_digest(scope),
        payload.credential_public_id,
        payload.credential_timestamp,
        payload.venue_account_id,
        payload.account_observation_public_id,
        payload.account_observation_timestamp,
        payload.observed_at,
        payload.valid_until,
        payload.route_revision,
        route_authority_content_digest(payload),
        canonical_route_authority_payload(payload),
    )
    _require(actual == expected, "route_authority_projection_mismatch")


async def get_bound_route_authority(
    session: AsyncSession, public_id: str
) -> ExecutionRouteAuthorityPayload:
    """Read immutable historical authority without incorrectly requiring current freshness.

    Args:
        session: Caller-owned transaction; this read acquires no new account fence.
        public_id: Exact immutable route authority identity.

    Returns:
        Complete verified historical route evidence for replay or raw-observation binding.
    """
    row = (
        await session.execute(
            select(ExecutionRouteAuthority).where(ExecutionRouteAuthority.public_id == public_id)
        )
    ).scalar_one_or_none()
    if row is None:
        raise RouteAuthorityBindingError("route_authority_missing")
    payload = ExecutionRouteAuthorityPayload.model_validate_json(row.canonical_payload)
    _verify_projection(row, payload)
    return payload


async def require_current_route_authority(
    session: AsyncSession, reference: ExecutionRouteReference, as_of: datetime
) -> ExecutionRouteAuthorityPayload:
    """Require fresh authority for the account and route actually selected by the executor.

    Args:
        session: Caller-owned transaction already holding its wallet execution fence.
        reference: Immutable authority and actual public client-route identity.
        as_of: Actual financial interpretation/admission clock.

    Returns:
        Current verified account authority without treating it as permission to resend.
    """
    actual = ExecutionRouteReference.model_validate(reference)
    payload = await get_bound_route_authority(session, actual.authority_public_id)
    _require(
        (
            route_authority_content_digest(payload),
            payload.scope,
            payload.route_revision,
        )
        == (actual.authority_content_digest, actual.scope, actual.route_revision),
        "actual_execution_route_mismatch",
    )
    await _verify_current_payload(session, payload, as_of)
    return payload


async def insert_execution_route_authority(
    repository: SnapshotRepository,
    payload: ExecutionRouteAuthorityPayload,
    provenance: SnapshotWriteProvenance,
) -> SnapshotWriteResult:
    """Commit one actual route/account observation under the established wallet fence.

    Args:
        repository: Real transaction and wallet-fence provider.
        payload: Actual client route plus exact fresh durable account observation.
        provenance: Insertion identity and actual producer knowledge timestamp.

    Returns:
        The immutable authority identity, preserving an exact same-content retry.
    """
    proven = ExecutionRouteAuthorityPayload.model_validate(payload)
    stamp = SnapshotWriteProvenance.model_validate(provenance)
    digest = route_authority_content_digest(proven)
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await repository._acquire_execution_fence_lock(session, proven.scope.wallet_public_id)
        await _verify_current_payload(session, proven, stamp.timestamp)
        identity = (
            await session.execute(
                select(ExecutionRouteAuthority).where(
                    ExecutionRouteAuthority.public_id == stamp.public_id
                )
            )
        ).scalar_one_or_none()
        if identity is not None and identity.content_digest != digest:
            raise RouteAuthorityConflictError("route_authority_public_identity_changed")
        existing = (
            await session.execute(
                select(ExecutionRouteAuthority).where(
                    ExecutionRouteAuthority.content_digest == digest
                )
            )
        ).scalar_one_or_none()
        if existing is not None:
            _verify_projection(existing, proven)
            return SnapshotWriteResult(existing.id, existing.public_id, digest, False)
        scope = proven.scope
        record = ExecutionRouteAuthority(
            **stamp.model_dump(mode="python"),
            wallet_public_id=scope.wallet_public_id,
            exchange=scope.exchange,
            mode=scope.mode,
            environment=scope.environment,
            margin_account=scope.margin_account,
            account_scope_digest=account_scope_digest(scope),
            credential_public_id=proven.credential_public_id,
            credential_timestamp=proven.credential_timestamp,
            venue_account_id=proven.venue_account_id,
            account_observation_public_id=proven.account_observation_public_id,
            account_observation_timestamp=proven.account_observation_timestamp,
            observed_at=proven.observed_at,
            valid_until=proven.valid_until,
            route_revision=proven.route_revision,
            content_digest=digest,
            canonical_payload=canonical_route_authority_payload(proven),
        )
        session.add(record)
        await session.flush()
        result = SnapshotWriteResult(record.id, record.public_id, digest, True)
        await session.commit()
        return result
