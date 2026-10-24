"""Explicit authority-family dispatch with original immutable knowledge clocks.

One database statement discovers an untyped identity across both families.
Family-qualified historical references select only their declared source
family, preserving replay when an unrelated later row shares their public UUID.
Each payload retains its original source-specific canonical hash domain.
"""

from dataclasses import dataclass
from datetime import datetime

from pydantic import TypeAdapter
from sqlalchemy import literal
from sqlalchemy import select
from sqlalchemy import union_all
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.core.account_authority_reference import AccountAuthorityFamily
from snapper.core.account_authority_reference import AccountAuthorityPayload
from snapper.core.account_authority_reference import AccountAuthorityReference
from snapper.core.execution_route_authority import ExecutionRouteAuthorityPayload
from snapper.core.execution_route_authority import route_authority_content_digest
from snapper.core.execution_unit_admission import ExecutionRouteReference
from snapper.data.execution_route_authority_store import get_bound_route_authority
from snapper.data.execution_route_authority_store import require_current_route_authority
from snapper.data.models import ExecutionRouteAuthority
from snapper.data.models import NativeAccountAuthority
from snapper.data.models import is_open_ended_version
from snapper.data.native_account_authority_store import BoundNativeAccountAuthority
from snapper.data.native_account_authority_store import NativeAuthorityFreshnessPolicy
from snapper.data.native_account_authority_store import get_bound_native_account_authority
from snapper.data.native_account_authority_store import require_current_native_account_authority

_IDENTITY_ROW = TypeAdapter(tuple[AccountAuthorityFamily, datetime, datetime, str])


class AccountAuthorityBindingError(ValueError):
    """The untyped identity or its actual immutable knowledge cannot be resolved uniquely."""


@dataclass(frozen=True, slots=True)
class BoundAccountAuthority:
    """A verified family-specific payload with the actual retained row knowledge clock."""

    family: AccountAuthorityFamily
    public_id: str
    content_digest: str
    recorded_at: datetime
    payload: AccountAuthorityPayload


async def _discover_authority(
    session: AsyncSession, public_id: str
) -> tuple[AccountAuthorityFamily, datetime, datetime, str]:
    """Observe both tables in one database statement without preferring a family."""
    statement = union_all(
        select(
            literal("legacy_route_v1"),
            ExecutionRouteAuthority.timestamp,
            ExecutionRouteAuthority.known_to,
            ExecutionRouteAuthority.content_digest,
        ).where(ExecutionRouteAuthority.public_id == public_id),
        select(
            literal("native_accounts_v1"),
            NativeAccountAuthority.timestamp,
            NativeAccountAuthority.known_to,
            NativeAccountAuthority.content_digest,
        ).where(NativeAccountAuthority.public_id == public_id),
    )
    rows = (await session.execute(statement)).all()
    if len(rows) != 1:
        raise AccountAuthorityBindingError("account_authority_missing_or_ambiguous")
    return _checked_identity(tuple(rows[0]))


def _checked_identity(
    values: tuple[object, ...],
) -> tuple[AccountAuthorityFamily, datetime, datetime, str]:
    """Validate actual selected-row metadata without normalizing an immutable lifetime."""
    identity = _IDENTITY_ROW.validate_python(values)
    if identity[1].utcoffset() is None or not is_open_ended_version(identity[2]):
        raise AccountAuthorityBindingError("account_authority_invalid_immutable_clock")
    return identity


def _native_bound(
    public_id: str,
    identity: tuple[AccountAuthorityFamily, datetime, datetime, str],
    native: BoundNativeAccountAuthority,
) -> BoundAccountAuthority:
    """Keep actual native knowledge and digest identical to the single discovery statement."""
    family, recorded_at, _known_to, digest = identity
    if (native.public_id, native.recorded_at, native.content_digest) != (
        public_id,
        recorded_at,
        digest,
    ):
        raise AccountAuthorityBindingError("account_authority_discovery_changed")
    return BoundAccountAuthority(family, public_id, digest, recorded_at, native.payload)


def _legacy_bound(
    public_id: str,
    identity: tuple[AccountAuthorityFamily, datetime, datetime, str],
    legacy: ExecutionRouteAuthorityPayload,
) -> BoundAccountAuthority:
    """Preserve the original legacy canonicalizer without serializing a merged payload."""
    family, recorded_at, _known_to, digest = identity
    if route_authority_content_digest(legacy) != digest:
        raise AccountAuthorityBindingError("account_authority_discovery_changed")
    return BoundAccountAuthority(family, public_id, digest, recorded_at, legacy)


async def get_bound_account_authority(
    session: AsyncSession, public_id: str
) -> BoundAccountAuthority:
    """Resolve an untyped immutable authority without selecting current credentials or freshness.

    Args:
        session: Caller-owned transaction used for immutable evidence interpretation.
        public_id: Exact persisted authority UUID requiring unambiguous family discovery.

    Returns:
        The family-specific verified payload and actual original durable knowledge timestamp.
    """
    identity = await _discover_authority(session, public_id)
    if identity[0] == "native_accounts_v1":
        native = await get_bound_native_account_authority(session, public_id)
        return _native_bound(public_id, identity, native)
    legacy = await get_bound_route_authority(session, public_id)
    return _legacy_bound(public_id, identity, legacy)


async def _legacy_reference_identity(
    session: AsyncSession, reference: AccountAuthorityReference
) -> tuple[AccountAuthorityFamily, datetime, datetime, str]:
    """Read only the explicitly selected legacy table's original knowledge metadata."""
    row = (
        await session.execute(
            select(
                ExecutionRouteAuthority.timestamp,
                ExecutionRouteAuthority.known_to,
                ExecutionRouteAuthority.content_digest,
            ).where(ExecutionRouteAuthority.public_id == reference.public_id)
        )
    ).one_or_none()
    if row is None:
        raise AccountAuthorityBindingError("account_authority_reference_missing")
    return _checked_identity((reference.family, *tuple(row)))


async def get_bound_account_authority_reference(
    session: AsyncSession, reference: AccountAuthorityReference
) -> BoundAccountAuthority:
    """Replay an explicit family without allowing an unrelated later UUID collision to redirect it.

    Args:
        session: Caller-owned transaction used to rebind retained historical source evidence.
        reference: Immutable family, row UUID, digest and original durable knowledge timestamp.

    Returns:
        The exact historical graph from its selected source family, independent of current freshness.
    """
    actual = AccountAuthorityReference.model_validate(reference)
    if actual.family == "native_accounts_v1":
        native = await get_bound_native_account_authority(session, actual.public_id)
        bound = BoundAccountAuthority(
            actual.family,
            native.public_id,
            native.content_digest,
            native.recorded_at,
            native.payload,
        )
    else:
        identity = await _legacy_reference_identity(session, actual)
        legacy = await get_bound_route_authority(session, actual.public_id)
        bound = _legacy_bound(actual.public_id, identity, legacy)
    if (bound.public_id, bound.content_digest, bound.recorded_at) != (
        actual.public_id,
        actual.content_digest,
        actual.recorded_at,
    ):
        raise AccountAuthorityBindingError("account_authority_reference_mismatch")
    return bound


async def require_current_account_authority(
    session: AsyncSession,
    reference: ExecutionRouteReference,
    as_of: datetime,
    native_policy: NativeAuthorityFreshnessPolicy | None = None,
) -> BoundAccountAuthority:
    """Require the unique current source under the caller's actual wallet execution fence.

    Args:
        session: Existing transaction holding the intended wallet execution fence.
        reference: Exact source digest, account scope and selected route of this operation.
        as_of: Actual local admission or interpretation clock.
        native_policy: Trusted native freshness configuration; absence refuses native admission.

    Returns:
        The rebound current account identity, without certifying order permission or collateral.
    """
    actual = ExecutionRouteReference.model_validate(reference)
    if as_of.utcoffset() is None:
        raise AccountAuthorityBindingError("account_authority_clock_not_aware")
    public_id = actual.authority_public_id
    identity = await _discover_authority(session, public_id)
    if identity[1] > as_of:
        raise AccountAuthorityBindingError("account_authority_not_yet_known")
    if identity[0] == "native_accounts_v1":
        if native_policy is None:
            raise AccountAuthorityBindingError("native_authority_freshness_policy_missing")
        native = await require_current_native_account_authority(
            session, actual, native_policy, as_of
        )
        return _native_bound(public_id, identity, native)
    legacy = await require_current_route_authority(session, actual, as_of)
    return _legacy_bound(public_id, identity, legacy)
