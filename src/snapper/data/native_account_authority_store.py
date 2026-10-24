"""Bind immutable native authority to independently retained selected-key and account sources.

The authority graph preserves actual source and durable knowledge clocks,
literal native profile identity and the selected-key proof's immutable owner.
This module grants neither financial completeness nor physical dispatch rights.
"""

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta

from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.trade.futures_native_accounts import BoundFuturesNativeAccounts
from snapper.application.trade.futures_native_accounts import FuturesNativeAccountProfile
from snapper.application.trade.futures_native_accounts import FuturesNativeAccountsRequest
from snapper.application.trade.futures_native_accounts import get_bound_futures_native_accounts
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.core.execution_route_authority import account_scope_digest
from snapper.core.execution_unit_admission import ExecutionRouteReference
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance
from snapper.core.native_account_authority import NativeAccountAuthorityPayload
from snapper.core.native_account_authority import canonical_native_account_authority
from snapper.core.native_account_authority import client_selection_from_native_authority
from snapper.core.native_account_authority import native_account_authority_digest
from snapper.core.native_account_identity import native_account_identity_digest
from snapper.core.native_account_read import NativeAccountReadCapturedSource
from snapper.core.native_account_read import NativeAccountReadOutcomePayload
from snapper.core.native_account_read import native_account_read_attempt_digest
from snapper.core.native_account_read import native_account_read_outcome_digest
from snapper.data.execution_unit_snapshot_store import SnapshotRepository
from snapper.data.execution_unit_snapshot_store import SnapshotWriteResult
from snapper.data.models import ExecutionRouteAuthority
from snapper.data.models import NativeAccountAuthority
from snapper.data.models import is_open_ended_version
from snapper.data.native_account_identity_store import BoundNativeAccountIdentityProof
from snapper.data.native_account_identity_store import get_bound_native_account_identity
from snapper.data.native_account_read_store import get_bound_native_account_read_outcome
from snapper.data.native_account_read_store import get_latest_native_account_read

_PROFILE_DOMAIN = b"snapper:native-account-profile:v1\x00"


class NativeAccountAuthorityBindingError(ValueError):
    """Actual immutable source, same-attempt ownership or current read eligibility is unproven."""


@dataclass(frozen=True, slots=True)
class NativeAuthorityFreshnessPolicy:
    """Current trusted configuration, never a caller-supplied source or financial permit."""

    maximum_source_age: timedelta
    maximum_server_clock_skew: timedelta

    def __post_init__(self) -> None:
        """Refuse a disabled or negative native observation window at configuration time."""
        if self.maximum_source_age <= timedelta(0) or self.maximum_server_clock_skew < timedelta(0):
            raise ValueError("Native authority freshness bounds must be positive and nonnegative")


@dataclass(frozen=True, slots=True)
class BoundNativeAccountAuthority:
    """Verified immutable native identity with its actual durable row knowledge clock."""

    public_id: str
    content_digest: str
    recorded_at: datetime
    payload: NativeAccountAuthorityPayload


@dataclass(frozen=True, slots=True)
class _AuthoritySourceGraph:
    """Historical independently loaded records, without a current or spending verdict."""

    owner: BoundNativeAccountIdentityProof
    outcome: NativeAccountReadOutcomePayload
    accounts: BoundFuturesNativeAccounts


def _require(condition: bool, reason: str) -> None:
    """Refuse an unproven binding without choosing a preferred or older fallback."""
    if not condition:
        raise NativeAccountAuthorityBindingError(reason)


def native_authority_profile_digest(profile: FuturesNativeAccountProfile) -> str:
    """Digest the full exact parsed profile without converting native amounts to float.

    Args:
        profile: Actual retained source profile produced by its strict raw reader.

    Returns:
        Versioned canonical digest including native quantities and unclassified components.
    """
    proven = FuturesNativeAccountProfile.model_validate(profile)
    material = json.dumps(proven.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(_PROFILE_DOMAIN + material.encode()).hexdigest()


def _authority_projection(payload: NativeAccountAuthorityPayload) -> dict[str, object]:
    """Describe the proposed physical projection without inventing legacy account observations."""
    scope = payload.scope
    return {
        "wallet_public_id": scope.wallet_public_id,
        "exchange": scope.exchange,
        "mode": scope.mode,
        "environment": scope.environment,
        "margin_account": scope.margin_account,
        "account_scope_digest": account_scope_digest(scope),
        "credential_public_id": payload.credential_public_id,
        "credential_timestamp": payload.credential_timestamp,
        "public_api_origin": payload.public_api_origin,
        "venue_account_id": payload.venue_account_id,
        "owner_proof_public_id": payload.owner_proof.public_id,
        "read_attempt_public_id": payload.native_read.attempt_public_id,
        "read_outcome_public_id": payload.native_read.outcome_public_id,
        "raw_receipt_public_id": payload.native_read.raw_receipt_public_id,
        "observed_at": payload.observed_at,
        "valid_until": payload.valid_until,
        "route_revision": payload.route_revision,
        "content_digest": native_account_authority_digest(payload),
        "canonical_payload": canonical_native_account_authority(payload),
    }


def _verify_projection(row: NativeAccountAuthority, payload: NativeAccountAuthorityPayload) -> None:
    """Verify every proposed stored copy against canonical native-family payload and provenance."""
    expected = _authority_projection(payload)
    _require(
        all(getattr(row, field) == value for field, value in expected.items())
        and is_open_ended_version(row.known_to),
        "native_authority_projection_mismatch",
    )


async def _bound_owner(
    session: AsyncSession, payload: NativeAccountAuthorityPayload
) -> BoundNativeAccountIdentityProof:
    """Require the actual same-attempt safe key proof and its independently rebound owner root."""
    owner = await get_bound_native_account_identity(session, payload.owner_proof.public_id)
    proof = owner.payload
    reference = payload.owner_proof
    selected = client_selection_from_native_authority(payload)
    _require(
        owner.public_id == reference.public_id
        and native_account_identity_digest(proof) == reference.content_digest
        and proof.attempt_public_id == reference.read_attempt_public_id
        and proof.observation.client_selection == selected
        and proof.observation.account_uid == reference.account_uid == payload.venue_account_id
        and proof.observation.observed_at == reference.observed_at
        and proof.matches_owner,
        "native_authority_owner_proof_mismatch",
    )
    return owner


async def _bound_outcome(
    session: AsyncSession, payload: NativeAccountAuthorityPayload
) -> NativeAccountReadOutcomePayload:
    """Bind only an actually successful accounts result from the exact committed read attempt."""
    reference = payload.native_read
    bound = await get_bound_native_account_read_outcome(session, reference.attempt_public_id)
    if bound is None:
        raise NativeAccountAuthorityBindingError("native_authority_accounts_read_pending")
    outcome = bound.payload
    source = outcome.result
    if not isinstance(source, NativeAccountReadCapturedSource):
        raise NativeAccountAuthorityBindingError("native_authority_accounts_read_unsuccessful")
    _require(
        bound.public_id == reference.outcome_public_id
        and native_account_read_outcome_digest(outcome) == reference.outcome_content_digest
        and outcome.attempt_public_id == reference.attempt_public_id
        and outcome.attempt_content_digest == reference.attempt_content_digest
        and native_account_read_attempt_digest(outcome.attempt) == reference.attempt_content_digest
        and outcome.attempt.family_sequence == reference.family_sequence
        and outcome.attempt.client_selection == client_selection_from_native_authority(payload),
        "native_authority_accounts_attempt_mismatch",
    )
    _require(
        (
            source.raw_receipt_public_id,
            source.raw_content_digest,
            source.request_started_at,
            source.source_observed_at,
            source.source_recorded_at,
        )
        == (
            reference.raw_receipt_public_id,
            reference.raw_content_digest,
            reference.request_started_at,
            reference.source_observed_at,
            reference.source_recorded_at,
        ),
        "native_authority_accounts_source_mismatch",
    )
    return outcome


async def _bound_accounts(
    session: AsyncSession, payload: NativeAccountAuthorityPayload, known_at: datetime
) -> BoundFuturesNativeAccounts:
    """Replay the actual retained original response and rebind one exact native wallet profile."""
    source = await get_bound_futures_native_accounts(
        session,
        FuturesNativeAccountsRequest(
            raw_receipt_public_id=payload.native_read.raw_receipt_public_id,
            client_selection=client_selection_from_native_authority(payload),
        ),
        known_at,
    )
    reference = payload.native_read
    _require(
        (
            source.source_observed_at,
            source.source_recorded_at,
            source.snapshot.source_policy,
            source.snapshot.server_time_text,
            source.snapshot.server_time,
        )
        == (
            reference.source_observed_at,
            reference.source_recorded_at,
            reference.parser_policy,
            reference.server_time_text,
            reference.server_time,
        ),
        "native_authority_original_source_metadata_mismatch",
    )
    matches = tuple(
        profile
        for profile in source.snapshot.profiles
        if profile.native_account_key == payload.native_profile.native_account_key
    )
    _require(len(matches) == 1, "native_authority_literal_profile_missing_or_ambiguous")
    profile = matches[0]
    _require(
        (
            profile.account_type,
            profile.settlement_currency,
            native_authority_profile_digest(profile),
        )
        == (
            payload.native_profile.account_type,
            payload.native_profile.settlement_currency,
            payload.native_profile.content_digest,
        ),
        "native_authority_literal_profile_changed",
    )
    return source


async def _verify_source_graph(
    session: AsyncSession, payload: NativeAccountAuthorityPayload, known_at: datetime
) -> _AuthoritySourceGraph:
    """Rebind immutable references and honest durable chronology without current-source checks."""
    try:
        owner = await _bound_owner(session, payload)
        outcome = await _bound_outcome(session, payload)
        accounts = await _bound_accounts(session, payload, known_at)
    except NativeAccountAuthorityBindingError:
        raise
    except (ValueError, UnicodeError, RecursionError):
        raise NativeAccountAuthorityBindingError("native_authority_source_graph_unproven") from None
    _require(
        owner.payload.attempt == outcome.attempt
        and owner.payload.recorded_at <= payload.native_read.request_started_at
        and owner.payload.recorded_at <= known_at
        and outcome.completed_at <= known_at
        and accounts.source_recorded_at <= known_at,
        "native_authority_source_knowledge_mismatch",
    )
    return _AuthoritySourceGraph(owner, outcome, accounts)


async def get_bound_native_account_authority(
    session: AsyncSession, public_id: str
) -> BoundNativeAccountAuthority:
    """Read complete immutable native history without choosing a current or older successful source.

    Args:
        session: Caller-owned transaction used for immutable replay.
        public_id: Exact native authority record identity, not an inferred source UUID.

    Returns:
        Independently rebound native payload with its original durable knowledge clock.
    """
    row = await session.scalar(
        select(NativeAccountAuthority).where(NativeAccountAuthority.public_id == public_id)
    )
    if row is None:
        raise NativeAccountAuthorityBindingError("native_authority_missing")
    try:
        payload = NativeAccountAuthorityPayload.model_validate_json(row.canonical_payload)
    except ValidationError:
        raise NativeAccountAuthorityBindingError("native_authority_payload_invalid") from None
    _verify_projection(row, payload)
    await _verify_source_graph(session, payload, row.timestamp)
    return BoundNativeAccountAuthority(row.public_id, row.content_digest, row.timestamp, payload)


def _verify_freshness(
    payload: NativeAccountAuthorityPayload,
    policy: NativeAuthorityFreshnessPolicy,
    as_of: datetime,
) -> None:
    """Bound expiry by original source clocks and current trusted policy, never poll or insert time."""
    _require(as_of.utcoffset() is not None, "native_authority_clock_not_aware")
    _require(
        payload.observed_at <= as_of < payload.valid_until,
        "native_authority_not_current",
    )
    for observed in (
        payload.owner_proof.observed_at,
        payload.native_read.source_observed_at,
    ):
        _require(
            timedelta(0) <= as_of - observed < policy.maximum_source_age
            and payload.valid_until - observed <= policy.maximum_source_age,
            "native_authority_exceeds_original_source_window",
        )
    server_offset = payload.native_read.server_time - payload.native_read.source_observed_at
    _require(
        -policy.maximum_server_clock_skew <= server_offset <= policy.maximum_server_clock_skew,
        "native_authority_server_clock_outside_policy",
    )


async def _verify_current_payload(
    session: AsyncSession,
    payload: NativeAccountAuthorityPayload,
    policy: NativeAuthorityFreshnessPolicy,
    as_of: datetime,
) -> None:
    """Require actual current public selection and exact latest successful read under the wallet fence."""
    _require(as_of.utcoffset() is not None, "native_authority_clock_not_aware")
    graph = await _verify_source_graph(session, payload, as_of)
    _require(
        graph.accounts.snapshot.source_policy == "kraken_futures_accounts_prepared_key_v2",
        "native_authority_original_prepared_key_unproven",
    )
    selected = client_selection_from_native_authority(payload)
    try:
        latest = await get_latest_native_account_read(session, selected, as_of)
    except (ValueError, UnicodeError, RecursionError):
        raise NativeAccountAuthorityBindingError(
            "native_authority_current_selection_unproven"
        ) from None
    _require(
        latest is not None
        and latest.attempt.public_id == payload.native_read.attempt_public_id
        and latest.outcome is not None
        and latest.outcome.public_id == payload.native_read.outcome_public_id,
        "native_authority_newest_attempt_changed_or_pending",
    )
    _verify_freshness(payload, policy, as_of)


async def require_current_native_account_authority(
    session: AsyncSession,
    reference: ExecutionRouteReference,
    policy: NativeAuthorityFreshnessPolicy,
    as_of: datetime,
) -> BoundNativeAccountAuthority:
    """Recheck exact immutable identity and current eligibility without issuing a dispatch capability.

    Args:
        session: Actual transaction already holding the wallet execution fence.
        reference: Public account/route selected by the actual coordinator.
        policy: Trusted current freshness bounds, never supplied as observed source facts.
        as_of: Actual interpretation or physical dispatch recheck clock.

    Returns:
        The verified current native identity; financial and physical dispatch gates remain separate.
    """
    actual = ExecutionRouteReference.model_validate(reference)
    _require(as_of.utcoffset() is not None, "native_authority_clock_not_aware")
    bound = await get_bound_native_account_authority(session, actual.authority_public_id)
    _require(bound.recorded_at <= as_of, "native_authority_not_yet_known")
    legacy = await session.scalar(
        select(ExecutionRouteAuthority.public_id).where(
            ExecutionRouteAuthority.public_id == actual.authority_public_id
        )
    )
    _require(legacy is None, "native_authority_public_identity_ambiguous")
    _require(
        (bound.content_digest, bound.payload.scope, bound.payload.route_revision)
        == (actual.authority_content_digest, actual.scope, actual.route_revision),
        "native_authority_actual_route_reference_mismatch",
    )
    await _verify_current_payload(session, bound.payload, policy, as_of)
    return bound


async def _reuse_native_authority(
    session: AsyncSession,
    row: NativeAccountAuthority,
    payload: NativeAccountAuthorityPayload,
    known_at: datetime,
) -> SnapshotWriteResult:
    """Preserve the first source/profile row only after full historical and current-source binding."""
    original = await get_bound_native_account_authority(session, row.public_id)
    _require(original.recorded_at <= known_at, "native_authority_knowledge_clock_regressed")
    legacy = await session.scalar(
        select(ExecutionRouteAuthority.public_id).where(
            ExecutionRouteAuthority.public_id == original.public_id
        )
    )
    _require(legacy is None, "native_authority_public_identity_ambiguous")
    _require(
        original.payload == payload,
        "native_authority_source_profile_material_changed",
    )
    return SnapshotWriteResult(row.id, original.public_id, original.content_digest, False)


async def insert_native_account_authority(
    repository: SnapshotRepository,
    payload: NativeAccountAuthorityPayload,
    identity: ExecutionEvidenceIdentity,
    policy: NativeAuthorityFreshnessPolicy,
) -> SnapshotWriteResult:
    """Insert one source-bound native identity under the real execution transaction and wallet fence.

    Args:
        repository: Real transaction and execution-fence provider.
        payload: Native producer material, independently rebound against actual immutable sources.
        identity: Producer identity; actual recording time is sampled inside the wallet fence.
        policy: Trusted current source-age/server-clock policy.

    Returns:
        Actual immutable row identity, with exact retry rechecked before return.
    """
    proven = NativeAccountAuthorityPayload.model_validate(payload)
    reference = ExecutionEvidenceIdentity.model_validate(identity)
    digest = native_account_authority_digest(proven)
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await repository._acquire_execution_fence_lock(session, proven.scope.wallet_public_id)
        stamp = SnapshotWriteProvenance(**reference.model_dump(), timestamp=datetime.now(UTC))
        await _verify_current_payload(session, proven, policy, stamp.timestamp)
        legacy = await session.scalar(
            select(ExecutionRouteAuthority.public_id).where(
                ExecutionRouteAuthority.public_id == stamp.public_id
            )
        )
        _require(legacy is None, "native_authority_public_identity_occupied_by_legacy")
        occupied = await session.scalar(
            select(NativeAccountAuthority).where(
                NativeAccountAuthority.public_id == stamp.public_id
            )
        )
        _require(
            occupied is None or occupied.content_digest == digest,
            "native_authority_public_identity_changed",
        )
        source_profile = await session.scalar(
            select(NativeAccountAuthority).where(
                NativeAccountAuthority.read_attempt_public_id
                == proven.native_read.attempt_public_id,
                NativeAccountAuthority.margin_account == proven.scope.margin_account,
            )
        )
        if source_profile is not None:
            return await _reuse_native_authority(session, source_profile, proven, stamp.timestamp)
        existing = await session.scalar(
            select(NativeAccountAuthority).where(NativeAccountAuthority.content_digest == digest)
        )
        if existing is not None:
            return await _reuse_native_authority(session, existing, proven, stamp.timestamp)
        record = NativeAccountAuthority(
            **stamp.model_dump(mode="python"), **_authority_projection(proven)
        )
        session.add(record)
        await session.flush()
        result = SnapshotWriteResult(record.id, record.public_id, digest, True)
        await session.commit()
        return result
