"""Shared DB-backed user deactivation fallback helpers."""

import asyncio
import contextlib
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Coroutine
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Final

from loguru import logger

from snapper.auth.domain.roles import UserRole
from snapper.data.repository import Repository

DEACTIVATION_FALLBACK_SCAN_INTERVAL_S: Final[float] = 5.0
"""Seconds between DB fallback scans for inactive users."""

_deactivation_fallback_sleep = asyncio.sleep


@dataclass(slots=True, frozen=True)
class OperatorMembershipClaim:
    """Operator memberships asserted by one authenticated credential.

    Attributes:
        user_public_id: Stable user identity whose memberships are checked.
        role: Signed role claim retained for diagnostics and cache inspection.
        has_global_operator_authority: Whether the effective token grant includes
            global operator impersonation authority.
        operator_public_ids: Operator identities asserted by the credential.
        operator_membership_public_ids: Sorted ``(operator, membership)``
            generation pairs asserted by the credential. Empty keeps legacy
            operator-only reconciliation.
    """

    user_public_id: str
    role: UserRole
    has_global_operator_authority: bool
    operator_public_ids: tuple[str, ...]
    operator_membership_public_ids: tuple[tuple[str, str], ...] = ()


def _group_explicit_membership_claims(
    claims: Sequence[OperatorMembershipClaim],
) -> dict[str, list[OperatorMembershipClaim]]:
    """Group non-global, non-empty claims by stable user identity."""
    claims_by_user: dict[str, list[OperatorMembershipClaim]] = {}
    for claim in claims:
        if claim.has_global_operator_authority:
            continue
        if not claim.operator_public_ids and not claim.operator_membership_public_ids:
            continue
        claims_by_user.setdefault(claim.user_public_id, []).append(claim)
    return claims_by_user


def _operator_membership_claim_is_stale(
    claim: OperatorMembershipClaim,
    active_operator_public_ids: set[str],
    active_membership_public_ids: dict[str, str],
) -> bool:
    """Return whether one explicit claim exceeds current membership versions."""
    claimed_operator_public_ids = set(claim.operator_public_ids)
    if not claimed_operator_public_ids.issubset(active_operator_public_ids):
        return True
    claimed_membership_public_ids = dict(claim.operator_membership_public_ids)
    if not claimed_membership_public_ids:
        return False
    if set(claimed_membership_public_ids) != claimed_operator_public_ids:
        return True
    return any(
        active_membership_public_ids.get(operator_public_id) != membership_public_id
        for operator_public_id, membership_public_id in claimed_membership_public_ids.items()
    )


def start_deactivation_fallback_task(
    current_task: asyncio.Task[None] | None,
    loop_factory: Callable[[], Coroutine[object, object, None]],
) -> asyncio.Task[None]:
    """Return a running fallback task, reusing a healthy existing one.

    Args:
        current_task: Existing scan task, if one has already been created.
        loop_factory: Callable that builds the fallback loop coroutine.

    Returns:
        The still-running existing task, or a newly-created task.
    """
    if current_task is not None and not current_task.done():
        return current_task
    return asyncio.create_task(loop_factory())


async def stop_deactivation_fallback_task(
    current_task: asyncio.Task[None] | None,
) -> None:
    """Cancel and await a fallback task if it exists.

    Args:
        current_task: Existing scan task to stop.
    """
    if current_task is None:
        return
    current_task.cancel()
    with contextlib.suppress(asyncio.CancelledError, Exception):
        await current_task


async def run_deactivation_fallback_loop(
    scan_once: Callable[[], Awaitable[None]],
    *,
    component_name: str,
) -> None:
    """Periodically run one deactivation fallback scan until cancelled.

    Args:
        scan_once: Callback that performs a single fallback scan.
        component_name: Name included in cancellation logs.
    """
    try:
        while True:
            await scan_once()
            await _deactivation_fallback_sleep(DEACTIVATION_FALLBACK_SCAN_INTERVAL_S)
    except asyncio.CancelledError:
        logger.info("{}: deactivation fallback scan loop cancelled", component_name)
        raise


async def list_inactive_user_public_ids(
    repository_factory: Callable[[], Repository] | None,
    user_public_ids: list[str],
    *,
    component_name: str,
) -> list[str]:
    """Return inactive user ids from repository lookup, tolerating fallback failures.

    Args:
        repository_factory: Callable that creates repositories for fallback lookups.
        user_public_ids: Candidate user ids to check.
        component_name: Name included in warning logs.

    Returns:
        Inactive candidate ids, or an empty list when the lookup is disabled or fails.
    """
    if repository_factory is None or not user_public_ids:
        return []
    try:
        return await repository_factory().list_inactive_user_public_ids(user_public_ids)
    except AttributeError:
        return []
    except Exception as exc:
        logger.warning("{} deactivation fallback scan failed: {}", component_name, exc)
        return []


async def list_users_with_stale_operator_memberships(
    repository_factory: Callable[[], Repository] | None,
    claims: Sequence[OperatorMembershipClaim],
    as_of: datetime,
    *,
    component_name: str,
) -> list[str]:
    """Return users whose non-admin operator claims exceed active memberships.

    The fallback is deliberately tolerant: repository doubles predating desk
    membership support and transient database failures return no findings so
    the periodic scanner remains alive. Request-path token verification applies
    its own fail-closed rule and does not rely on this tolerant scanner.

    Args:
        repository_factory: Callable creating repositories for fallback reads.
        claims: Cached or connected credential claims to reconcile.
        as_of: UTC temporal boundary for active membership reads.
        component_name: Name included in warning logs.

    Returns:
        Sorted unique user identities with at least one stale operator claim.
    """
    stale_claims = await list_stale_operator_membership_claims(
        repository_factory,
        claims,
        as_of,
        component_name=component_name,
    )
    return sorted({claim.user_public_id for claim in stale_claims})


async def list_stale_operator_membership_claims(
    repository_factory: Callable[[], Repository] | None,
    claims: Sequence[OperatorMembershipClaim],
    as_of: datetime,
    *,
    component_name: str,
) -> list[OperatorMembershipClaim]:
    """Return each non-global credential claim that exceeds live memberships.

    Args:
        repository_factory: Callable creating repositories for fallback reads.
        claims: Cached or connected credential claims to reconcile.
        as_of: UTC temporal boundary for active membership reads.
        component_name: Name included in warning logs.

    Returns:
        Each stale claim, preserving distinct generations for one user.
    """
    if repository_factory is None or not claims:
        return []
    claims_by_user = _group_explicit_membership_claims(claims)
    if not claims_by_user:
        return []
    try:
        repository = repository_factory()
        stale_claims: list[OperatorMembershipClaim] = []
        for user_public_id, user_claims in sorted(claims_by_user.items()):
            memberships = await repository.get_user_operator_memberships(user_public_id, as_of)
            active_operator_public_ids = {
                membership["operator_public_id"] for membership in memberships
            }
            active_membership_public_ids = {
                membership["operator_public_id"]: membership["public_id"]
                for membership in memberships
            }
            stale_claims.extend(
                claim
                for claim in user_claims
                if _operator_membership_claim_is_stale(
                    claim,
                    active_operator_public_ids,
                    active_membership_public_ids,
                )
            )
        return stale_claims
    except AttributeError:
        return []
    except Exception as exc:
        logger.warning("{} membership fallback scan failed: {}", component_name, exc)
        return []


async def list_active_user_token_jtis_by_user(
    repository_factory: Callable[[], Repository] | None,
    user_public_ids: Sequence[str],
    *,
    component_name: str,
) -> dict[str, set[str]]:
    """Return active token inventories grouped by user for successful lookups.

    Args:
        repository_factory: Callable creating repositories for fallback reads.
        user_public_ids: User identities whose token inventories are required.
        component_name: Name included in warning logs.

    Returns:
        Active JTI sets by user, or an empty mapping when lookup is unavailable.
    """
    unique_user_public_ids = sorted(set(user_public_ids))
    if repository_factory is None or not unique_user_public_ids:
        return {}
    try:
        repository = repository_factory()
        active_jtis_by_user: dict[str, set[str]] = {}
        for user_public_id in unique_user_public_ids:
            active_jtis_by_user[user_public_id] = set(
                await repository.list_active_user_token_jtis(user_public_id)
            )
        return active_jtis_by_user
    except AttributeError:
        return {}
    except Exception as exc:
        logger.warning("{} token fallback scan failed: {}", component_name, exc)
        return {}
