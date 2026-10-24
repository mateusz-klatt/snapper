"""Produce native account authority from independently retained closed-reader evidence.

This producer performs no network I/O, accepts no account amounts or provider
UID, and never creates an alternative authority from an earlier successful
request. The store independently binds the source graph and current policy
inside its wallet-fenced insertion transaction.
"""

from dataclasses import dataclass
from datetime import UTC
from datetime import datetime

from pydantic import BaseModel
from pydantic import ConfigDict
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.trade.futures_native_accounts import BoundFuturesNativeAccounts
from snapper.application.trade.futures_native_accounts import FuturesNativeAccountProfile
from snapper.application.trade.futures_native_accounts import FuturesNativeAccountsRequest
from snapper.application.trade.futures_native_accounts import get_bound_futures_native_accounts
from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.core.execution_route_authority import route_selection_digest
from snapper.core.execution_unit_admission import ExecutionRouteReference
from snapper.core.execution_unit_snapshot import EvidenceText
from snapper.core.execution_unit_snapshot import EvidenceUUID
from snapper.core.native_account_authority import NativeAccountAuthorityPayload
from snapper.core.native_account_authority import NativeAuthorityOwnerReference
from snapper.core.native_account_authority import NativeAuthorityProfile
from snapper.core.native_account_authority import NativeAuthorityReadReference
from snapper.core.native_account_authority import native_account_authority_digest
from snapper.core.native_account_identity import native_account_identity_digest
from snapper.core.native_account_read import NativeAccountReadCapturedSource
from snapper.core.native_account_read import native_account_read_outcome_digest
from snapper.data.execution_unit_snapshot_store import SnapshotRepository
from snapper.data.execution_unit_snapshot_store import SnapshotWriteResult
from snapper.data.native_account_authority_store import NativeAccountAuthorityBindingError
from snapper.data.native_account_authority_store import NativeAuthorityFreshnessPolicy
from snapper.data.native_account_authority_store import insert_native_account_authority
from snapper.data.native_account_authority_store import native_authority_profile_digest
from snapper.data.native_account_identity_store import BoundNativeAccountIdentityProof
from snapper.data.native_account_identity_store import find_native_account_identity
from snapper.data.native_account_read_store import BoundNativeAccountReadOutcome
from snapper.data.native_account_read_store import get_bound_native_account_read_outcome


class NativeAuthorityCaptureRequest(BaseModel):
    """The actual completed read and exact observed wallet key, without supplied source facts."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    read_attempt_public_id: EvidenceUUID
    native_account_key: EvidenceText


@dataclass(frozen=True, slots=True)
class CapturedNativeAccountAuthority:
    """A committed immutable account identity and its exact account-route reference."""

    result: SnapshotWriteResult
    payload: NativeAccountAuthorityPayload
    reference: ExecutionRouteReference


@dataclass(frozen=True, slots=True)
class _CaptureSources:
    """Original independently loaded source records, never caller-authored account observations."""

    owner: BoundNativeAccountIdentityProof
    outcome: BoundNativeAccountReadOutcome
    accounts: BoundFuturesNativeAccounts
    profile: FuturesNativeAccountProfile
    raw: NativeAccountReadCapturedSource


async def _capture_sources(
    session: AsyncSession, request: NativeAuthorityCaptureRequest, known_at: datetime
) -> _CaptureSources:
    """Load the exact successful read and safe owner proof before choosing one literal profile."""
    outcome = await get_bound_native_account_read_outcome(session, request.read_attempt_public_id)
    if outcome is None or not isinstance(outcome.payload.result, NativeAccountReadCapturedSource):
        raise NativeAccountAuthorityBindingError("native_authority_capture_read_unsuccessful")
    owner = await find_native_account_identity(session, request.read_attempt_public_id)
    if owner is None or not owner.payload.matches_owner:
        raise NativeAccountAuthorityBindingError("native_authority_capture_owner_unproven")
    accounts = await get_bound_futures_native_accounts(
        session,
        FuturesNativeAccountsRequest(
            raw_receipt_public_id=outcome.payload.result.raw_receipt_public_id,
            client_selection=outcome.payload.attempt.client_selection,
        ),
        known_at,
    )
    matches = tuple(
        profile
        for profile in accounts.snapshot.profiles
        if profile.native_account_key == request.native_account_key
    )
    if len(matches) != 1:
        raise NativeAccountAuthorityBindingError("native_authority_capture_exact_profile_missing")
    return _CaptureSources(owner, outcome, accounts, matches[0], outcome.payload.result)


def _capture_payload(
    sources: _CaptureSources, policy: NativeAuthorityFreshnessPolicy
) -> NativeAccountAuthorityPayload:
    """Copy original evidence and derive expiry only from the original source window."""
    outcome = sources.outcome.payload
    raw = sources.raw
    selected = outcome.attempt.client_selection
    owner = sources.owner.payload
    profile = sources.profile
    scope = ExecutionAccountScope(
        wallet_public_id=selected.wallet_public_id,
        exchange=selected.exchange,
        mode=selected.mode,
        environment=selected.environment,
        margin_account=profile.native_account_key,
    )
    read = NativeAuthorityReadReference(
        attempt_public_id=outcome.attempt_public_id,
        attempt_content_digest=outcome.attempt_content_digest,
        family_sequence=outcome.attempt.family_sequence,
        outcome_public_id=sources.outcome.public_id,
        outcome_content_digest=native_account_read_outcome_digest(outcome),
        raw_receipt_public_id=raw.raw_receipt_public_id,
        raw_content_digest=raw.raw_content_digest,
        request_started_at=raw.request_started_at,
        source_observed_at=raw.source_observed_at,
        source_recorded_at=raw.source_recorded_at,
        server_time_text=sources.accounts.snapshot.server_time_text,
        server_time=sources.accounts.snapshot.server_time,
        parser_policy=sources.accounts.snapshot.source_policy,
    )
    return NativeAccountAuthorityPayload.model_validate(
        {
            "scope": scope,
            "credential_public_id": selected.credential_public_id,
            "credential_timestamp": selected.credential_timestamp,
            "public_api_origin": selected.public_api_origin,
            "venue_account_id": owner.observation.account_uid,
            "owner_proof": NativeAuthorityOwnerReference(
                public_id=sources.owner.public_id,
                content_digest=native_account_identity_digest(owner),
                read_attempt_public_id=owner.attempt_public_id,
                account_uid=owner.observation.account_uid,
                observed_at=owner.observation.observed_at,
            ),
            "native_read": read,
            "native_profile": NativeAuthorityProfile(
                native_account_key=profile.native_account_key,
                account_type=profile.account_type,
                settlement_currency=profile.settlement_currency,
                content_digest=native_authority_profile_digest(profile),
            ),
            "observed_at": raw.source_observed_at,
            "valid_until": min(owner.observation.observed_at, raw.source_observed_at)
            + policy.maximum_source_age,
            "route_revision": route_selection_digest(
                scope,
                selected.credential_public_id,
                selected.credential_timestamp,
                selected.public_api_origin,
            ),
        }
    )


async def capture_native_account_authority(
    repository: SnapshotRepository,
    request: NativeAuthorityCaptureRequest,
    identity: ExecutionEvidenceIdentity,
    policy: NativeAuthorityFreshnessPolicy,
) -> CapturedNativeAccountAuthority:
    """Retain one exact native profile from the actual successful closed-reader attempt.

    Args:
        repository: Actual immutable source repository and wallet-fence provider.
        request: Actual read attempt and literal native profile key selected by the coordinator.
        identity: New insertion identity, with no supplied source or local recording clock.
        policy: Trusted server freshness bounds, never a user-selected permission or age override.

    Returns:
        The committed native identity and route reference, without financial or dispatch permission.
    """
    actual = NativeAuthorityCaptureRequest.model_validate(request)
    stamp = ExecutionEvidenceIdentity.model_validate(identity)
    async with repository.session() as session:
        sources = await _capture_sources(session, actual, datetime.now(UTC))
    payload = _capture_payload(sources, policy)
    result = await insert_native_account_authority(
        repository,
        payload,
        stamp,
        policy,
    )
    reference = ExecutionRouteReference(
        authority_public_id=result.public_id,
        authority_content_digest=native_account_authority_digest(payload),
        scope=payload.scope,
        route_revision=payload.route_revision,
    )
    return CapturedNativeAccountAuthority(result, payload, reference)
