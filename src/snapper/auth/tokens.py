"""Token management module.

This module provides JWT token creation, verification, and lifecycle
management including blacklisting and WebSocket token rotation.
"""

import asyncio
import contextlib
import hashlib
import heapq
import json as json_mod
import secrets
import uuid
from collections.abc import Callable
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Final

import jwt
import zmq
import zmq.asyncio
from loguru import logger
from pydantic import ValidationError

from snapper.application.services.settings import SettingsService
from snapper.auth.deactivation_fallback import OperatorMembershipClaim
from snapper.auth.deactivation_fallback import list_active_user_token_jtis_by_user
from snapper.auth.deactivation_fallback import list_inactive_user_public_ids
from snapper.auth.deactivation_fallback import list_users_with_stale_operator_memberships
from snapper.auth.deactivation_fallback import run_deactivation_fallback_loop
from snapper.auth.deactivation_fallback import start_deactivation_fallback_task
from snapper.auth.deactivation_fallback import stop_deactivation_fallback_task
from snapper.auth.domain.permissions import ROLE_PERMISSIONS
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.permissions import get_effective_permissions
from snapper.auth.domain.permissions import has_effective_permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.schemas.tokens import TokenClaims
from snapper.auth.schemas.tokens import TokenPair
from snapper.config.settings import AppSettings
from snapper.config.settings import get_settings
from snapper.config.settings import get_settings_with_service
from snapper.data.repository import Repository
from snapper.data.repository_types import UserActiveTokenInsertRow
from snapper.data.repository_types import UserActiveTokenVerificationRow
from snapper.messaging.infrastructure.validated_socket import HWM_AUDIT
from snapper.messaging.infrastructure.validated_socket import ValidatedSubscriber
from snapper.messaging.infrastructure.validated_socket import apply_hwm
from snapper.messaging.schemas.admin import MembershipRevokedData
from snapper.messaging.schemas.data import UserDeactivatedData

BLACKLIST_GRACE_PERIOD_SECONDS: Final[float] = 10.0
"""Seconds a blacklisted token remains usable to handle concurrent requests."""

BLACKLIST_CLEANUP_MULTIPLIER: Final[int] = 2
"""Factor applied to grace period when deciding when to purge old entries."""

BLACKLIST_CLEANUP_BATCH_SIZE: Final[int] = 64
"""Maximum stale blacklist heap entries processed during one cleanup pass."""

VERIFY_CACHE_TTL_SECONDS: Final[float] = 30.0
"""Seconds one inventory read is reused from the LRU before re-reading.

Deliberately NOT "seconds a verdict is reused": the LRU holds
:class:`_InventoryFacts`, and every hit re-runs the full acceptance
predicate with the current caller's ``expected_token_type``. What this
window bounds is the staleness of the DB read, not the reuse of an
answer.
"""

VERIFY_CACHE_MAX_ENTRIES: Final[int] = 10000
"""Upper bound on the verify-cache size before an opportunistic prune runs."""

ROTATION_GRACE_TTL_SECONDS: Final[float] = BLACKLIST_GRACE_PERIOD_SECONDS
"""Seconds a just-redeemed refresh JTI may idempotently re-collect its successor.

Concurrent refreshes with the same single-use token are a NORMAL client
pattern (rapid F5 aborts an in-flight refresh whose Set-Cookie the browser
never processed; parallel 401 handlers race each other). The CAS loser
re-presents a JTI the winner already redeemed; within this window the
route returns the SAME successor pair instead of 401, per the OAuth2
Security BCP allowance for a short replay grace.

Deliberately EQUAL to ``BLACKLIST_GRACE_PERIOD_SECONDS``: past the
blacklist grace, verification rejects the old JWT before rotation is
even reached, so a longer memory would only widen the window in which a
STOLEN already-redeemed token (replayed by an in-flight request that
verified before the blacklist armed) could collect the successor pair —
an accepted single-user-deployment risk that this alignment keeps as
narrow as the existing verify layer already allows.
"""

ROTATION_GRACE_MAX_ENTRIES: Final[int] = 1024
"""Upper bound on remembered successor pairs before the oldest are pruned."""

BLACKLIST_MAX_ENTRIES: Final[int] = 50000
"""Hard cap on in-memory JTI blacklist entries.

Mass deactivation can bulk-add many active JTIs to the blacklist.
Opportunistic cleanup runs on the verify path, so low-traffic nodes
also enforce this hard cap via :meth:`_enforce_blacklist_cap` to
bound memory growth.
"""

_ADMIN_USER_DEACTIVATED_TOPIC: Final[str] = "admin.user_deactivated"
"""Bus topic that ``UserService.deactivate_user`` publishes under."""

_ADMIN_MEMBERSHIP_REVOKED_TOPIC: Final[str] = "admin.membership_revoked"
"""Bus topic that desk detach publishes after closing a membership."""

_ADMIN_LISTEN_RECV_BACKOFF_S: Final[float] = 0.1
"""Backoff after a non-cancellation recv error so the loop cannot tight-spin."""


TOKEN_TYPE_ACCESS: Final[str] = "access"
"""Inventory ``token_type`` of a credential mintable as a bearer."""

TOKEN_TYPE_REFRESH: Final[str] = "refresh"
"""Inventory ``token_type`` of a credential redeemable ONLY at refresh rotation."""

REFRESH_JTI_PREFIX: Final[str] = "refresh_"
"""Prefix :meth:`TokenManager.create_tokens` stamps on a refresh JWT's ``jti``.

The prefix is the ONLY purpose marker that has ever been signed, and it
was never consulted at verification. It is now required to CORROBORATE
the inventory row rather than to decide on its own: a signed ``jti``
that disagrees with the row's ``token_type`` means the two records of
one credential's purpose disagree, and a credential whose purpose is
ambiguous is rejected.
"""

TOKEN_PURPOSE_MISMATCH_EVENT: Final[str] = "token_purpose_mismatch"
"""Structured security-log event name for every purpose rejection."""

PURPOSE_MISMATCH_JTI: Final[str] = "jti_mismatch"
"""Reported ``actual`` when the row's ``jti`` differs from the signed ``jti``."""

PURPOSE_MISMATCH_JTI_PREFIX: Final[str] = "jti_prefix_mismatch"
"""Reported ``actual`` when the signed ``jti`` prefix contradicts the row type."""


@dataclass(slots=True, frozen=True)
class _InventoryFacts:
    """What ``user_active_tokens`` says about one hash, and nothing more.

    Deliberately holds FACTS, never a verdict. A verdict computed for
    one caller's purpose ("this token is valid") is context-free the
    moment it is stored: reused by a caller asking a DIFFERENT
    question it launders a refresh credential into an access grant.
    Storing the facts instead forces every consumer — cache hit and
    cache miss alike — through the same acceptance predicate with its
    own ``expected_token_type`` in hand.

    ``row_exists`` distinguishes an absent row (no such credential was
    ever issued, or it has been cleaned up) from a present-but-dead
    row, which is what separates the ``invalid`` and
    ``user_deactivated`` rejection reasons.
    """

    row_exists: bool
    is_revoked: bool
    row_expires_at_ts: float
    user_is_active: bool
    user_public_id: str
    token_type: str
    jti: str


_ABSENT_INVENTORY_FACTS: Final[_InventoryFacts] = _InventoryFacts(
    row_exists=False,
    is_revoked=True,
    row_expires_at_ts=0.0,
    user_is_active=False,
    user_public_id="",
    token_type="",
    jti="",
)
"""Facts for a hash with no inventory row — fails every gate by construction."""


@dataclass(slots=True, frozen=True)
class _VerifyCacheEntry:
    """Frozen inventory facts memoised by the DB-backed verify path.

    Shape carries ``facts.user_public_id`` so the admin-bus subscriber
    can evict every cached token for a deactivated user without
    scanning the raw JWTs (which we never retain). ``expires_at_ts``
    holds the JWT ``exp`` claim as a unix timestamp so expired entries
    are short-circuited on lookup even if they linger past the TTL.

    The entry stores :class:`_InventoryFacts` rather than a boolean
    verdict precisely so a hit cannot answer a question the miss was
    never asked. See that class for why.

    ``role``, ``operator_public_ids`` and ``operator_membership_public_ids``
    preserve the signed authority claim for broker-independent fallback
    reconciliation. Membership public IDs are the generation fence that
    distinguishes a continuous grant from detach followed by re-attachment.
    ``membership_claim_is_current`` is the live DB verdict computed on
    cache fill and re-applied on every hit. Its default is deliberately
    fail-closed so a future constructor cannot accidentally bypass the
    membership gate.
    """

    facts: _InventoryFacts
    expires_at_ts: float
    cached_at_ts: float
    role: UserRole = UserRole.VIEWER
    operator_public_ids: tuple[str, ...] = ()
    operator_membership_public_ids: tuple[tuple[str, str], ...] = ()
    has_global_operator_authority: bool = False
    membership_claim_is_current: bool = False


@dataclass(slots=True, frozen=True)
class _VerifyCacheGuard:
    """Race and authority verdict sampled before one cache write."""

    generation_before: int
    membership_claim_is_current: bool = False


REJECTION_REASON_USER_DEACTIVATED: Final[str] = "user_deactivated"
"""Rejection reason emitted when the token owner's active user row is inactive."""

REJECTION_REASON_INVALID: Final[str] = "invalid"
"""Rejection reason for every other failure mode (signature, expiry, blacklist, missing row, revoked)."""


@dataclass(slots=True, frozen=True)
class VerifyOutcome:
    """Verdict + rejection reason returned by the DB-backed verify path.

    ``rejection_reason`` is returned alongside ``claims`` so
    callers can branch directly on the failure type without
    re-reading cache state.
    ``rejection_reason`` is ``None`` on success, one of
    :data:`REJECTION_REASON_USER_DEACTIVATED` /
    :data:`REJECTION_REASON_INVALID` on failure.
    """

    claims: TokenClaims | None
    rejection_reason: str | None


def _inventory_facts(row: UserActiveTokenVerificationRow | None) -> _InventoryFacts:
    """Project one inventory read into the facts the verifier reasons over.

    Args:
        row: The ``user_active_tokens`` verification projection, or
            ``None`` when the presented hash matched no row at all.

    Returns:
        :data:`_ABSENT_INVENTORY_FACTS` for a missing row, otherwise
        the row's lifecycle and purpose fields as plain values.
    """
    if row is None:
        return _ABSENT_INVENTORY_FACTS
    return _InventoryFacts(
        row_exists=True,
        is_revoked=row["revoked_at"] is not None,
        row_expires_at_ts=row["expires_at"].timestamp(),
        user_is_active=row["user_is_active"],
        user_public_id=row["user_public_id"],
        token_type=row["token_type"],
        jti=row["jti"],
    )


def _log_purpose_mismatch(
    expected_token_type: str,
    actual: str,
    token_data: TokenClaims,
) -> None:
    """Emit the structured ``token_purpose_mismatch`` security event.

    Names the expected and the actual purpose so an operator can tell
    a refresh-as-bearer replay from a corrupted inventory row. Never
    names the credential: the raw JWT and its hash stay out of logs,
    and the ``jti`` recorded here is the public correlation id the
    inventory and the blacklist already key by.

    Unlike :func:`_log_inventory_rejection` this fires on the cache-hit
    path too, and that asymmetry is deliberate rather than an oversight.
    A lifecycle rejection is a property of the ROW, identical for every
    caller, so damping it to the DB-read path still logs each distinct
    rejection once. A purpose rejection is a property of the CALLER: the
    entry that a refresh rotation legitimately populated is first
    mismatched by the bearer replay that hits it, so the same damping
    would silence precisely the first occurrence of the attack this
    gate exists to catch. The flood risk it trades away is bounded —
    :meth:`TokenManager.verify_token` rejects unsigned, expired and
    blacklisted tokens before this is reachable, so an attacker must
    already hold a genuinely valid credential to emit even one line.

    Args:
        expected_token_type: Purpose the caller demanded.
        actual: Purpose the inventory row asserts, or one of
            :data:`PURPOSE_MISMATCH_JTI` /
            :data:`PURPOSE_MISMATCH_JTI_PREFIX` when the row and the
            signed ``jti`` disagree about the same credential.
        token_data: Verified claims of the presented JWT.

    Returns:
        None.
    """
    logger.bind(
        event=TOKEN_PURPOSE_MISMATCH_EVENT,
        expected_token_type=expected_token_type,
        actual_token_type=actual,
        user_public_id=token_data.user_public_id,
        jti=token_data.jti,
    ).warning(
        "{}: expected={} actual={} user={} jti={}",
        TOKEN_PURPOSE_MISMATCH_EVENT,
        expected_token_type,
        actual,
        token_data.user_public_id,
        token_data.jti,
    )


def _purpose_accepted(
    facts: _InventoryFacts,
    token_data: TokenClaims,
    expected_token_type: str,
) -> bool:
    """Decide whether one live inventory row may answer for ``expected_token_type``.

    Three independent agreements are required, and each is checked
    because the others cannot stand in for it:

        1. The row's ``token_type`` is the purpose the caller asked
           for. This is the load-bearing check — the row is written by
           the issuer and bound to the token hash, so it is positive
           evidence of what the credential was minted as, and it is
           already populated for every credential minted since the
           inventory gained the column.
        2. The row's ``jti`` is the ``jti`` that was actually signed.
           Without it the hash bond alone would let a row be trusted
           to describe a token it does not name.
        3. The signed ``jti``'s ``refresh_`` prefix agrees with the
           row's type. The prefix is the historical purpose marker;
           if the two disagree, one of the two records has been
           tampered with or the issuer is inconsistent, and neither
           outcome may be resolved in the credential's favour.

    Deliberately NOT checked: any purpose CLAIM inside the JWT. No such
    claim has ever been signed, and requiring one would reject every
    long-lived delegate credential minted before it existed.

    Args:
        facts: Inventory facts for the presented token hash.
        token_data: Verified claims of the presented JWT.
        expected_token_type: Purpose the call site demands, either
            :data:`TOKEN_TYPE_ACCESS` or :data:`TOKEN_TYPE_REFRESH`.

    Returns:
        ``True`` only when all three agreements hold. Every ``False``
        return has already emitted the security event.
    """
    if facts.token_type != expected_token_type:
        _log_purpose_mismatch(expected_token_type, facts.token_type, token_data)
        return False
    if facts.jti != token_data.jti:
        _log_purpose_mismatch(expected_token_type, PURPOSE_MISMATCH_JTI, token_data)
        return False
    if facts.jti.startswith(REFRESH_JTI_PREFIX) != (facts.token_type == TOKEN_TYPE_REFRESH):
        _log_purpose_mismatch(expected_token_type, PURPOSE_MISMATCH_JTI_PREFIX, token_data)
        return False
    return True


def _outcome_for(
    facts: _InventoryFacts,
    token_data: TokenClaims,
    expected_token_type: str,
    now_ts: float,
    membership_claim_is_current: bool = True,
) -> VerifyOutcome:
    """Apply the full acceptance predicate to one set of inventory facts.

    The single funnel every verify path runs through, cache hit and
    cache miss alike, so a memoised read can never skip a gate the
    fresh read applied.

    Args:
        facts: Inventory facts for the presented token hash.
        token_data: Verified claims of the presented JWT.
        expected_token_type: Purpose the call site demands.
        now_ts: The caller's single clock sample.
        membership_claim_is_current: Whether every non-admin operator claim
            remains backed by an active DB membership.

    Returns:
        Claims on acceptance, otherwise ``claims=None`` with
        :data:`REJECTION_REASON_USER_DEACTIVATED` when a real row's
        owner is inactive and :data:`REJECTION_REASON_INVALID`
        otherwise. A purpose rejection reports ``invalid`` on purpose:
        the caller is told the credential does not work here, not what
        it would have worked for.
    """
    alive = facts.row_exists and not facts.is_revoked and facts.row_expires_at_ts > now_ts
    if not alive or not facts.user_is_active:
        reason = (
            REJECTION_REASON_USER_DEACTIVATED
            if facts.user_public_id and not facts.user_is_active
            else REJECTION_REASON_INVALID
        )
        return VerifyOutcome(claims=None, rejection_reason=reason)
    if not membership_claim_is_current:
        return VerifyOutcome(claims=None, rejection_reason=REJECTION_REASON_INVALID)
    if not _purpose_accepted(facts, token_data, expected_token_type):
        return VerifyOutcome(claims=None, rejection_reason=REJECTION_REASON_INVALID)
    return VerifyOutcome(claims=token_data, rejection_reason=None)


async def _operator_membership_claim_is_current(
    repository: Repository,
    facts: _InventoryFacts,
    token_data: TokenClaims,
    as_of: datetime,
) -> bool:
    """Validate non-global operator claims against current membership versions.

    Empty claims are valid and credentials with effective global impersonation
    authority bypass explicit membership checks. A non-empty claim fails closed
    when the inventory row has no user identity, the membership lookup fails,
    or any claimed operator is absent. New credentials additionally bind each
    operator to the active membership row's ``public_id`` so a detach followed
    by re-attachment cannot resurrect an older token. A completely empty
    version mapping remains the deliberate compatibility path for legacy JWTs;
    a partial mapping fails closed.

    Args:
        repository: Repository used by the DB-backed token verification path.
        facts: Inventory facts naming the authoritative token owner.
        token_data: Signed role and operator claims.
        as_of: UTC temporal boundary for active membership reads.

    Returns:
        Whether the credential's operator claim remains fully authorized.
    """
    claimed_operator_public_ids = set(token_data.operator_public_ids)
    claimed_membership_public_ids = token_data.operator_membership_public_ids
    has_global_operator_authority = has_effective_permission(
        token_data.role,
        token_data.permissions,
        token_data.permission_scope_version,
        Permission.IMPERSONATE_OPERATOR,
    )
    if has_global_operator_authority:
        return True
    if not claimed_operator_public_ids:
        return not claimed_membership_public_ids
    if not facts.user_public_id:
        logger.warning(
            "verify_token_with_db: non-empty operator claim has no inventory user — jti={}",
            token_data.jti,
        )
        return False
    try:
        memberships = await repository.get_user_operator_memberships(
            facts.user_public_id,
            as_of,
        )
    except Exception as exc:
        logger.warning(
            "verify_token_with_db: membership lookup failed closed — user={} err={}",
            facts.user_public_id,
            exc,
        )
        return False
    active_operator_public_ids = {membership["operator_public_id"] for membership in memberships}
    operators_are_current = claimed_operator_public_ids.issubset(active_operator_public_ids)
    versions_are_current = True
    if claimed_membership_public_ids:
        active_membership_public_ids = {
            membership["operator_public_id"]: membership["public_id"] for membership in memberships
        }
        versions_are_current = set(
            claimed_membership_public_ids
        ) == claimed_operator_public_ids and all(
            active_membership_public_ids.get(operator_public_id) == membership_public_id
            for operator_public_id, membership_public_id in claimed_membership_public_ids.items()
        )
    is_current = operators_are_current and versions_are_current
    if not is_current:
        logger.warning(
            "verify_token_with_db: stale operator claim rejected — user={} "
            "claimed={} active={} versioned={}",
            facts.user_public_id,
            sorted(claimed_operator_public_ids),
            sorted(active_operator_public_ids),
            bool(claimed_membership_public_ids),
        )
    return is_current


def _log_inventory_rejection(
    facts: _InventoryFacts,
    token_data: TokenClaims,
    now_ts: float,
) -> None:
    """Log the lifecycle reason a freshly-read inventory row was refused.

    Runs only on the DB-read path so a 30-second burst of replays does
    not multiply one rejection into a log flood. Every lifecycle gate
    that can refuse is represented: a rejection nobody can see in the
    log is indistinguishable from a client that simply stopped calling.

    Args:
        facts: Inventory facts just read for the presented hash.
        token_data: Verified claims of the presented JWT.
        now_ts: The caller's single clock sample.

    Returns:
        None.
    """
    if not facts.row_exists:
        logger.warning(
            "verify_token_with_db: token not in inventory — user={} jti={}",
            token_data.user_public_id,
            token_data.jti,
        )
        return
    if facts.is_revoked:
        logger.info(
            "verify_token_with_db: token revoked — user={} jti={}",
            token_data.user_public_id,
            token_data.jti,
        )
        return
    if facts.row_expires_at_ts <= now_ts:
        logger.info(
            "verify_token_with_db: inventory row expired — user={} jti={}",
            token_data.user_public_id,
            token_data.jti,
        )
        return
    if not facts.user_is_active:
        logger.info(
            "verify_token_with_db: user deactivated — user={} jti={}",
            token_data.user_public_id,
            token_data.jti,
        )


LONG_LIVED_TOKEN_EXPIRE_DAYS: Final[int] = 90
"""Access-token lifetime for long-lived AI service-principal tokens.

Ninety days matches the industry default for fine-grained personal
access tokens (GitHub, GitLab, Azure app secrets). Operators who need
longer-lived tokens should rotate them on the schedule that fits
their key-management hygiene; revocation still works via the per-JTI
blacklist + the ``user_active_tokens.revoked_at`` inventory flip, so
this window is a ceiling, not a commitment.

The constant was 3650 (ten years) before 2026-05-27; that was reduced
because a ten-year default lifetime is excessive — revocation works
but a ten-year default is a liability for tokens that may live in CI
secret stores or IDE config.
"""

PERMISSION_SCOPE_VERSION: Final[int] = 3
"""Version marker for refresh tokens carrying an intentional access scope."""


class PermissionScopeError(ValueError):
    """Raised when a requested token scope exceeds its role ceiling."""


def _resolve_permission_scope(
    role: UserRole,
    requested_permissions: Iterable[Permission | str] | None,
) -> list[Permission]:
    """Validate and resolve the permission grant for a newly minted token.

    Args:
        role: Role whose permissions form the immutable grant ceiling.
        requested_permissions: Optional narrower permission selection. An
            omitted selection resolves to the role's complete grant.

    Returns:
        Deduplicated requested permissions, or the full role grant when the
        selection is omitted.

    Raises:
        PermissionScopeError: If the selection contains a permission the
            role does not hold.
        ValueError: If a permission string is not a known permission value.
    """
    role_permissions = ROLE_PERMISSIONS.get(role, set())
    if requested_permissions is None:
        return list(role_permissions)
    requested = list(dict.fromkeys(Permission(value) for value in requested_permissions))
    invalid = set(requested) - role_permissions
    if invalid:
        values = ", ".join(sorted(permission.value for permission in invalid))
        raise PermissionScopeError(
            f"Permissions [{values}] are not granted to role '{role.value}'."
        )
    return requested


@dataclass(slots=True, frozen=True)
class LongLivedTokenResult:
    """Result of minting a long-lived AI service-principal access token.

    Returned by :meth:`TokenManager.create_delegate_access_token`.
    Distinct from :class:`~snapper.auth.schemas.tokens.TokenPair`
    because there is no refresh token — callers would otherwise have
    to check for a sentinel value on every use. A dedicated result
    type also keeps the existing ``TokenPair`` shape invariant for
    every non-PAT caller.

    Attributes:
        access_token: The freshly-minted JWT.
        expires_at: UTC timestamp when the access token's ``exp``
            claim lapses. Used by the caller to populate
            ``UserActiveToken.expires_at``.
        jti: The unique JTI claim in the JWT. Used by the caller to
            populate ``UserActiveToken.jti``.
        expires_in: Access-token lifetime in seconds — mirrors the
            :class:`TokenPair.expires_in` field so REST response
            shape stays consistent between rotating and long-lived
            delegate creation.
    """

    access_token: str
    expires_at: datetime
    jti: str
    expires_in: int


def hash_token(raw_token: str) -> str:
    """Return the SHA-256 hex digest of a raw JWT.

    Centralised so the inventory insert, the DB-backed verify lookup
    and the kill-switch revocation paths all agree on the hash shape
    used as the lookup key in ``user_active_tokens``.

    Args:
        raw_token: The JWT string exactly as emitted by
            ``TokenManager.create_tokens``.

    Returns:
        64-char lowercase hex digest suitable for the ``token_hash``
        column.
    """
    return hashlib.sha256(raw_token.encode("utf-8")).hexdigest()


class TokenManager:
    """JWT token manager singleton.

    Handles creation, verification, and blacklisting of JWT tokens.
    Uses the configured JWT signing algorithm for token signing
    (HS256 by default).

    Features:
    - Token pair creation (access + refresh)
    - Token verification with expiration checking
    - Token blacklisting with grace period
    - Automatic blacklist cleanup
    """

    _instance: TokenManager | None = None
    _initialized: bool = False

    def __new__(cls) -> TokenManager:
        """Create or return singleton token manager instance.

        Returns:
            TokenManager singleton instance.
        """
        if cls._instance is None:
            instance = super().__new__(cls)
            cls._instance = instance
        return cls._instance

    def __init__(self) -> None:
        """Initialize the token manager."""
        if self._initialized:
            return
        self._initialized = True
        self._settings: AppSettings | None = None
        self._blacklisted_tokens: dict[str, float] = {}
        self._blacklist_cleanup_heap: list[tuple[float, str, float]] = []
        self._next_blacklist_cleanup_ts = float("inf")
        self._blacklist_grace_period = BLACKLIST_GRACE_PERIOD_SECONDS
        self._verify_cache: dict[str, _VerifyCacheEntry] = {}
        self._rotation_grace: dict[str, tuple[TokenPair, float]] = {}
        self._rotation_lock = asyncio.Lock()
        self._user_cache_generations: dict[str, int] = {}
        self._admin_listener_lock = asyncio.Lock()
        self._admin_listen_task: asyncio.Task[None] | None = None
        self._admin_subscriber: ValidatedSubscriber | None = None
        self._admin_zmq_context: zmq.asyncio.Context | None = None
        self._admin_running = False
        self._deactivation_repository_factory: Callable[[], Repository] | None = None
        self._deactivation_scan_task: asyncio.Task[None] | None = None

    def set_settings_service(self, settings_service: SettingsService) -> None:
        """Set settings service for configuration.

        Args:
            settings_service: Settings service instance.
        """
        self._settings = get_settings_with_service(settings_service)

    def set_deactivation_repository_factory(
        self,
        repository_factory: Callable[[], Repository] | None,
    ) -> None:
        """Inject repository factory for broker-independent deactivation scans.

        Args:
            repository_factory: Callable that returns a fresh
                repository for fallback deactivation lookups, or
                ``None`` to disable the fallback scanner.
        """
        self._deactivation_repository_factory = repository_factory

    @property
    def settings(self) -> AppSettings:
        """Get application settings, lazy-loading if needed.

        Returns:
            Application settings object.
        """
        if self._settings is None:
            self._settings = get_settings()
        return self._settings

    def create_tokens(
        self,
        user: AuthPrincipal,
        remember_me: bool = False,
        *,
        session_id: str | None = None,
        permissions: Iterable[Permission | str] | None = None,
    ) -> TokenPair:
        """Create access and refresh token pair.

        Args:
            user: User profile to create tokens for.
            remember_me: If True, extends refresh token lifetime.
            session_id: Optional session ID for token rotation.
            permissions: Optional access-token grant. When omitted, the
                complete role grant is used. A supplied grant must be a
                subset of the role grant.

        Returns:
            TokenPair containing access and refresh tokens.
        """
        permission_scope = _resolve_permission_scope(user.role, permissions)
        permission_values = [permission.value for permission in permission_scope]
        now = datetime.now(UTC)
        issued_at = int(now.timestamp()) - 1
        jti = str(uuid.uuid4())
        session_identifier = session_id or str(uuid.uuid4())
        access_token_expires = timedelta(minutes=self.settings.auth_access_token_expire_minutes)
        access_token_payload = TokenClaims(
            sub=user.username,
            username=user.username,
            role=user.role,
            permissions=permission_values,
            permission_scope_version=PERMISSION_SCOPE_VERSION,
            exp=int((now + access_token_expires).timestamp()),
            iat=issued_at,
            jti=jti,
            sid=session_identifier,
            user_public_id=user.user_public_id,
            operator_public_ids=user.operator_public_ids,
            operator_membership_public_ids=user.operator_membership_public_ids,
            primary_operator_public_id=user.primary_operator_public_id,
            active_wallet_public_id=user.active_wallet_public_id,
        )
        refresh_token_expires_days = (
            self.settings.auth_refresh_token_expire_days_extended
            if remember_me
            else self.settings.auth_refresh_token_expire_days
        )
        refresh_token_expires = timedelta(days=refresh_token_expires_days)
        refresh_token_payload = TokenClaims(
            sub=user.username,
            username=user.username,
            role=user.role,
            permissions=permission_values,
            permission_scope_version=PERMISSION_SCOPE_VERSION,
            exp=int((now + refresh_token_expires).timestamp()),
            iat=issued_at,
            jti=f"{REFRESH_JTI_PREFIX}{jti}",
            sid=session_identifier,
            user_public_id=user.user_public_id,
            operator_public_ids=user.operator_public_ids,
            operator_membership_public_ids=user.operator_membership_public_ids,
            primary_operator_public_id=user.primary_operator_public_id,
            active_wallet_public_id=user.active_wallet_public_id,
        )
        access_token = jwt.encode(
            access_token_payload.model_dump(),
            self.settings.auth_secret_key,
            algorithm=self.settings.auth_algorithm,
        )
        refresh_token = jwt.encode(
            refresh_token_payload.model_dump(),
            self.settings.auth_secret_key,
            algorithm=self.settings.auth_algorithm,
        )
        logger.info(f"Created token pair for user {user.username} (remember_me={remember_me})")
        return TokenPair(
            access_token=access_token,
            refresh_token=refresh_token,
            expires_in=int(access_token_expires.total_seconds()),
        )

    def create_delegate_access_token(
        self,
        user: AuthPrincipal,
        issued_at: datetime,
        *,
        session_id: str | None = None,
        permissions: Iterable[Permission | str] | None = None,
    ) -> LongLivedTokenResult:
        """Mint a long-lived access token for an AI service principal.

        Distinct from :meth:`create_tokens` in three ways:
        (1) no refresh token is issued;
        (2) the access-token ``exp`` claim is set
            :data:`LONG_LIVED_TOKEN_EXPIRE_DAYS` days out (default
            90, ~3 months) instead of the short-lived access TTL;
        (3) the JTI is a plain ``uuid4()`` and carries no
            ``refresh_`` prefix so refresh-token-only code paths
            (e.g. :meth:`refresh_tokens`) never match on it.

        Boundary time is passed in (not minted inside the helper)
        for timestamp discipline. Provisioning services compute ``now``
        at their transaction boundary and thread it into this helper so
        every inserted row, audit entry, and token claim agrees on the
        same instant.

        Args:
            user: The AI service principal the token is issued to.
            issued_at: UTC boundary time supplied by the caller.
                Drives the ``iat`` claim and the ``exp`` derivation.
            session_id: Optional session identifier to carry through
                to the ``sid`` claim; defaults to a fresh uuid4 when
                absent.
            permissions: Optional access-token grant. When omitted, the
                complete role grant is used. A supplied grant must be a
                subset of that role grant.

        Returns:
            :class:`LongLivedTokenResult` with the JWT, the UTC
            ``expires_at`` datetime, the JTI, and the lifetime in
            seconds (for REST envelope ``expires_in``).
        """
        permission_scope = _resolve_permission_scope(user.role, permissions)
        permission_values = [permission.value for permission in permission_scope]
        jti = str(uuid.uuid4())
        session_identifier = session_id or str(uuid.uuid4())
        expires_at = issued_at + timedelta(days=LONG_LIVED_TOKEN_EXPIRE_DAYS)
        iat = int(issued_at.timestamp()) - 1
        exp = int(expires_at.timestamp())
        claims = TokenClaims(
            sub=user.username,
            username=user.username,
            role=user.role,
            permissions=permission_values,
            permission_scope_version=PERMISSION_SCOPE_VERSION,
            exp=exp,
            iat=iat,
            jti=jti,
            sid=session_identifier,
            user_public_id=user.user_public_id,
            operator_public_ids=user.operator_public_ids,
            operator_membership_public_ids=user.operator_membership_public_ids,
            primary_operator_public_id=user.primary_operator_public_id,
            active_wallet_public_id=user.active_wallet_public_id,
        )
        access_token = jwt.encode(
            claims.model_dump(),
            self.settings.auth_secret_key,
            algorithm=self.settings.auth_algorithm,
        )
        expires_in = int(timedelta(days=LONG_LIVED_TOKEN_EXPIRE_DAYS).total_seconds())
        logger.info(
            f"Created long-lived access token for user {user.username} "
            f"(jti={jti}, expires_at={expires_at.isoformat()})"
        )
        return LongLivedTokenResult(
            access_token=access_token,
            expires_at=expires_at,
            jti=jti,
            expires_in=expires_in,
        )

    def decode_fresh_token(self, token: str) -> TokenClaims:
        """Public alias for :meth:`_decode_fresh_token`.

        Use this from external services (e.g. DelegateService) that
        need to project freshly-minted JWT claims into inventory
        rows. Keeps the underscore-prefixed method name as a
        backward-compatible internal alias.

        Args:
            token: JWT string emitted by ``create_tokens``.

        Returns:
            Typed :class:`TokenClaims` for the decoded payload.
        """
        return self._decode_fresh_token(token)

    def _decode_fresh_token(self, token: str) -> TokenClaims:
        """Decode a token we just minted, returning typed claims.

        Trust context: the token was produced inside the same
        process by ``create_tokens`` using our secret and algorithm
        so signature failure here would be a programming error, not
        an auth failure. We still pass through :mod:`jwt.decode` so
        the exp/iat validation behaviour matches the verify path and
        signing-key rotation surfaces a clear exception
        instead of silent misbehaviour. No blacklist or DB check is
        performed — this helper is exclusively for
        :meth:`persist_tokens` extracting ``jti``/``iat``/``exp``
        from a freshly-minted pair.

        Args:
            token: JWT string emitted by ``create_tokens``.

        Returns:
            Typed :class:`TokenClaims` for the decoded payload.
        """
        payload = jwt.decode(
            token,
            self.settings.auth_secret_key,
            algorithms=[self.settings.auth_algorithm],
        )
        return TokenClaims.model_validate_json(json_mod.dumps(payload))

    def _build_inventory_rows(
        self,
        pair: TokenPair,
        user_public_id: str,
    ) -> list[UserActiveTokenInsertRow]:
        """Decode ``pair`` into the two inventory rows (access + refresh).

        Shared by :meth:`persist_tokens` (fresh login) and
        :meth:`rotate_tokens` (refresh rotation) so both callers
        agree on the row shape + ``token_hash`` + ``expires_at``
        projection.
        """
        access_claims = self._decode_fresh_token(pair.access_token)
        refresh_claims = self._decode_fresh_token(pair.refresh_token)
        issued_at = datetime.fromtimestamp(access_claims.iat, tz=UTC)
        access_exp = datetime.fromtimestamp(access_claims.exp, tz=UTC)
        refresh_exp = datetime.fromtimestamp(refresh_claims.exp, tz=UTC)
        return [
            UserActiveTokenInsertRow(
                public_id=str(uuid.uuid7()),
                user_public_id=user_public_id,
                jti=access_claims.jti,
                token_hash=hash_token(pair.access_token),
                token_type=TOKEN_TYPE_ACCESS,
                issued_at=issued_at,
                expires_at=access_exp,
            ),
            UserActiveTokenInsertRow(
                public_id=str(uuid.uuid7()),
                user_public_id=user_public_id,
                jti=refresh_claims.jti,
                token_hash=hash_token(pair.refresh_token),
                token_type=TOKEN_TYPE_REFRESH,
                issued_at=issued_at,
                expires_at=refresh_exp,
            ),
        ]

    async def persist_tokens(
        self,
        pair: TokenPair,
        user_public_id: str,
        repository: Repository,
    ) -> None:
        """Persist both JWTs of ``pair`` into ``user_active_tokens``.

        Called by the login route handler immediately after
        ``create_tokens`` returns so every outstanding token is
        reflected in the DB inventory. This is the precondition for
        the DB-backed ``verify_token`` path: any JWT without
        a matching row fails verification, so pre-existing JWTs
        issued before the inventory migration have no row and
        ``verify_token()`` will 401 them.
        Insertion is batched through
        meth:`Repository.insert_user_active_tokens` so both the
        access and refresh rows land in one transaction. If the
        batch fails the caller's transactional scope surfaces the
        exception — the route handler then returns 500 and the
        client must retry login.
        The refresh-rotation path uses :meth:`rotate_tokens`
        instead; this method is reserved for the login (no old JTI
        to revoke) case.

        Args:
            pair: The freshly-minted :class:`TokenPair` from
                meth:`create_tokens`.
            user_public_id: UUID of the authenticated user — the
                inventory's foreign key to ``users``.
            repository: Active :class:`Repository` bound to the
                caller's transactional scope.
        """
        rows = self._build_inventory_rows(pair, user_public_id)
        await repository.insert_user_active_tokens(rows)
        logger.debug(
            "persist_tokens: user={} access_jti={} refresh_jti={}",
            user_public_id,
            rows[0]["jti"],
            rows[1]["jti"],
        )

    async def rotate_tokens(
        self,
        pair: TokenPair,
        user_public_id: str,
        old_refresh_jti: str,
        repository: Repository,
    ) -> TokenPair | None:
        """Atomic refresh-rotation: revoke ``old_refresh_jti`` + persist ``pair``.

        The refresh route must treat "revoke old JTI" and "persist
        new pair" as a single transaction so that:

            1. A replayed refresh token (old row already revoked or
               absent) cannot mint ANOTHER successor pair — the CAS
               loses, NOTHING is inserted, and the caller either
               re-serves the winner's pair (grace window below) or
               returns 401.
            2. A transient DB error during new-row insert rolls
               back the old-row revoke so the user retries with the
               original refresh JWT instead of getting stranded.

        Concurrent-refresh grace: rapid F5 and parallel 401 handlers
        legitimately race the same single-use refresh token, and an
        aborted page load can lose the winner's ``Set-Cookie``
        forever. When the CAS loses but this manager rotated the
        same JTI within ``ROTATION_GRACE_TTL_SECONDS``, the WINNER'S
        pair is returned so the route can respond idempotently
        (both racers end up with the same, valid cookie set). The
        cache is per-process — matching the in-memory blacklist and
        verify-cache, which already assume single-instance
        deployment. The whole CAS-plus-remember (and the loser's
        CAS-plus-lookup) runs under ``_rotation_lock``: without it,
        the event loop may resume the CAS loser BEFORE the winner's
        continuation records its pair, and the loser would 401
        despite the grace window — the exact symptom this exists to
        remove.

        The in-memory blacklist is NOT seeded here — that is the
        caller's responsibility after a FRESH rotation (identity
        ``result is pair``), so the blacklist grace period starts
        post-commit and a grace replay does not re-arm it.

        Args:
            pair: The freshly-minted :class:`TokenPair` (successor
                to the redeemed refresh JWT).
            user_public_id: UUID of the user — consistent across
                redeem and rotate per JWT claim.
            old_refresh_jti: JTI of the refresh JWT being redeemed.
            repository: Active :class:`Repository`.

        Returns:
            ``pair`` when the rotation committed (rowcount == 1);
            the REMEMBERED successor pair when this exact JTI was
            already rotated within the grace window (idempotent
            replay); ``None`` when the JTI is unknown or the grace
            expired, in which case the caller MUST return 401.
        """
        rows = self._build_inventory_rows(pair, user_public_id)
        async with self._rotation_lock:
            rotated = await repository.rotate_user_active_token(
                old_refresh_jti, rows, datetime.now(UTC)
            )
            if not rotated:
                remembered = self._remembered_rotation(old_refresh_jti)
                if remembered is not None:
                    logger.info(
                        "rotate_tokens: concurrent redeem of jti={} within grace"
                        " — re-serving the winner's successor pair (user={})",
                        old_refresh_jti,
                        user_public_id,
                    )
                    return remembered
                logger.warning(
                    "rotate_tokens: refresh JTI replay/missing — user={} old_jti={}",
                    user_public_id,
                    old_refresh_jti,
                )
                return None
            self._remember_rotation(old_refresh_jti, pair)
        logger.debug(
            "rotate_tokens: user={} old_jti={} new_access_jti={} new_refresh_jti={}",
            user_public_id,
            old_refresh_jti,
            rows[0]["jti"],
            rows[1]["jti"],
        )
        return pair

    def _remember_rotation(self, old_refresh_jti: str, pair: TokenPair) -> None:
        """Record ``old_refresh_jti`` -> successor ``pair`` for the grace window.

        Prunes expired entries on every insert and, if the cache still
        exceeds ``ROTATION_GRACE_MAX_ENTRIES``, evicts the oldest — the
        cache stays bounded regardless of refresh volume.

        Args:
            old_refresh_jti: JTI of the refresh JWT that was just redeemed.
            pair: The successor pair persisted by the winning rotation.
        """
        now = datetime.now(UTC).timestamp()
        expired = [
            jti
            for jti, (_, redeemed_at) in self._rotation_grace.items()
            if now - redeemed_at >= ROTATION_GRACE_TTL_SECONDS
        ]
        for jti in expired:
            del self._rotation_grace[jti]
        self._rotation_grace[old_refresh_jti] = (pair, now)
        while len(self._rotation_grace) > ROTATION_GRACE_MAX_ENTRIES:
            oldest = min(self._rotation_grace, key=lambda k: self._rotation_grace[k][1])
            del self._rotation_grace[oldest]

    def _remembered_rotation(self, old_refresh_jti: str) -> TokenPair | None:
        """Return the successor pair for a JTI redeemed within the grace window.

        Args:
            old_refresh_jti: JTI presented by the CAS-losing refresh call.

        Returns:
            The winner's :class:`TokenPair` while the grace window is
            open; ``None`` when the JTI was never remembered or expired.
        """
        entry = self._rotation_grace.get(old_refresh_jti)
        if entry is None:
            return None
        pair, redeemed_at = entry
        if datetime.now(UTC).timestamp() - redeemed_at >= ROTATION_GRACE_TTL_SECONDS:
            del self._rotation_grace[old_refresh_jti]
            return None
        return pair

    def _is_token_blacklisted(self, jti: str) -> bool:
        """Check if token is blacklisted (past grace period).

        Args:
            jti: JWT ID to check.

        Returns:
            True if token is blacklisted and past grace period.
        """
        if jti not in self._blacklisted_tokens:
            return False
        blacklist_time = self._blacklisted_tokens[jti]
        current_time = datetime.now(UTC).timestamp()
        if current_time - blacklist_time >= self._blacklist_grace_period:
            return True
        else:
            logger.debug(f"Token {jti} in grace period, allowing use")
            return False

    def _blacklist_cleanup_deadline(self, blacklist_time: float) -> float:
        """Return the timestamp when a blacklist entry can be purged.

        Args:
            blacklist_time: Unix timestamp when the JTI was blacklisted.

        Returns:
            Unix timestamp when cleanup may remove the entry.
        """
        return blacklist_time + (self._blacklist_grace_period * BLACKLIST_CLEANUP_MULTIPLIER)

    def _record_blacklisted_token(self, jti: str, blacklist_time: float) -> None:
        """Record a JTI blacklist entry and index its cleanup deadline.

        Args:
            jti: JWT ID to blacklist.
            blacklist_time: Unix timestamp when the JTI was blacklisted.
        """
        self._blacklisted_tokens[jti] = blacklist_time
        cleanup_at = self._blacklist_cleanup_deadline(blacklist_time)
        heapq.heappush(self._blacklist_cleanup_heap, (cleanup_at, jti, blacklist_time))
        if cleanup_at < self._next_blacklist_cleanup_ts:
            self._next_blacklist_cleanup_ts = cleanup_at

    def _cleanup_old_blacklist_entries(self) -> None:
        """Remove an indexed batch of expired entries from blacklist."""
        current_time = datetime.now(UTC).timestamp()
        if current_time < self._next_blacklist_cleanup_ts:
            return
        popped = 0
        while (
            self._blacklist_cleanup_heap
            and self._blacklist_cleanup_heap[0][0] <= current_time
            and popped < BLACKLIST_CLEANUP_BATCH_SIZE
        ):
            _cleanup_at, jti, blacklist_time = heapq.heappop(self._blacklist_cleanup_heap)
            popped += 1
            if self._blacklisted_tokens.get(jti) == blacklist_time:
                del self._blacklisted_tokens[jti]
                logger.debug(f"Cleaned up expired blacklist entry: {jti}")
        self._next_blacklist_cleanup_ts = (
            self._blacklist_cleanup_heap[0][0] if self._blacklist_cleanup_heap else float("inf")
        )

    def verify_token(self, token: str) -> TokenClaims | None:
        """Verify and decode a JWT token.

        Args:
            token: JWT token string.

        Returns:
            TokenClaims if valid, None if invalid or expired.
        """
        try:
            payload = jwt.decode(
                token,
                self.settings.auth_secret_key,
                algorithms=[self.settings.auth_algorithm],
            )
            token_data = TokenClaims.model_validate_json(json_mod.dumps(payload))
            self._cleanup_old_blacklist_entries()
            if self._is_token_blacklisted(token_data.jti):
                logger.warning(f"Attempted use of blacklisted token: {token_data.jti}")
                return None
            if datetime.now(UTC).timestamp() > token_data.exp:
                logger.info(f"Token expired for user {token_data.username}")
                return None
            return token_data
        except jwt.ExpiredSignatureError:
            logger.info("Token expired")
            return None
        except jwt.PyJWTError as e:
            logger.warning(f"Token validation failed: {e}")
            return None
        except ValidationError as exc:
            logger.warning(f"Token payload validation failed: {exc}")
            return None

    async def verify_token_with_db(
        self,
        token: str,
        repository: Repository,
        *,
        expected_token_type: str,
    ) -> TokenClaims | None:
        """Return only ``claims`` from :meth:`verify_token_with_reason`.

        Backward-compatible thin wrapper preserved for the 3
        cookie/JWT-only callers that don't need the rejection reason
        (``get_current_user``, refresh route, ``verify_session_cookie``).
        The MCP middleware uses :meth:`verify_token_with_reason`
        directly so it can branch on the exact failure mode without
        re-reading the cache.
        See :meth:`verify_token_with_reason` for the full contract.

        Args:
            token: JWT string presented by the client.
            repository: Active :class:`Repository` for the DB-backed
                verify path.
            expected_token_type: Purpose this call site accepts. Has
                NO default deliberately — a transport that forgets to
                state what it accepts must fail to typecheck rather
                than silently accept anything, which is exactly the
                defect this argument exists to close.

        Returns:
            The :class:`TokenClaims` on success, ``None`` on any
            rejection (signature, expiry, blacklist, not-in-inventory
            revoked, user deactivated, wrong purpose).
        """
        outcome = await self.verify_token_with_reason(
            token,
            repository,
            expected_token_type=expected_token_type,
        )
        return outcome.claims

    async def verify_token_with_reason(
        self,
        token: str,
        repository: Repository,
        *,
        expected_token_type: str,
    ) -> VerifyOutcome:
        """DB-backed verify with 30s LRU cache.

        Fully validates a JWT against the ``user_active_tokens``
        inventory so that the kill switch propagates to
        every request on the NEXT call instead of waiting for the
        access-token expiry. The request-path sequence is
            1. Run :meth:`verify_token` for the cheap checks
               (signature, expiry, JTI blacklist). A failure short
               circuits so we never touch the DB or the cache for
               malformed / forged / blacklisted tokens.
            2. Hash the token and consult the 30-second LRU cache.
               A hit within TTL reuses the memoised INVENTORY FACTS —
               never a verdict — and re-runs the full acceptance
               predicate, ``expected_token_type`` included, against
               them. A cached dead-row fact set short-circuits the DB
               read so repeated replays don't amplify load.
            3. On cache miss, call
               meth:`Repository.get_active_token_by_hash` which
               joins ``user_active_tokens`` with the SCD2-active
               ``users`` row. The projection carries
               ``user_is_active`` so the deactivated-user state
               surfaces in one round-trip, and ``token_type`` +
               ``jti`` so the credential's purpose does too.
            4. Cache the facts for ``VERIFY_CACHE_TTL_SECONDS``
               (live AND dead rows — bounded cost for replayed
               invalid tokens) and return the claims when every
               gate passes.
        Cross-instance invariant: the DB-backed deactivation
        fallback scanner is the staleness ceiling; the admin-bus
        subscriber calls meth:`invalidate_user_cache` on
        ``admin.user_deactivated`` so kill-switch latency collapses
        to one bus-message round trip when the broker is healthy.

        Purpose invariant: the cache is keyed by token hash alone and
        is shared by every transport, so a refresh rotation and a REST
        request can hit the same entry within one TTL. Storing facts
        rather than a verdict is what makes that safe — see
        :class:`_InventoryFacts`.

        Args:
            token: JWT string presented by the client.
            repository: Active :class:`Repository` bound to the
                caller's transactional scope. A sync caller lives
                under the route handler's DB dep; the MCP
                middleware and WS auth pass the same singleton so
                connection-pool semantics match REST.
            expected_token_type: Purpose this call site accepts,
                either :data:`TOKEN_TYPE_ACCESS` or
                :data:`TOKEN_TYPE_REFRESH`. No default: see
                :meth:`verify_token_with_db`.

        Returns:
            A :class:`VerifyOutcome` with ``claims`` populated on
            success (``rejection_reason=None``) OR ``claims=None``
            plus a populated ``rejection_reason`` of
            data:`REJECTION_REASON_USER_DEACTIVATED` or
            data:`REJECTION_REASON_INVALID`. Callers that only
            need the success-path claims can use
            meth:`verify_token_with_db` for the backward
            compatible ``TokenClaims | None`` shape.
        """
        token_data = self.verify_token(token)
        if token_data is None:
            return VerifyOutcome(claims=None, rejection_reason=REJECTION_REASON_INVALID)
        th = hash_token(token)
        now_ts = datetime.now(UTC).timestamp()
        cached = self._verify_cache.get(th)
        if cached is not None and cached.cached_at_ts + VERIFY_CACHE_TTL_SECONDS > now_ts:
            return _outcome_for(
                cached.facts,
                token_data,
                expected_token_type,
                now_ts,
                cached.membership_claim_is_current,
            )
        claim_sample_key = token_data.user_public_id
        gen_before = (
            self._user_cache_generations.get(claim_sample_key, 0) if claim_sample_key else 0
        )
        facts = _inventory_facts(await repository.get_active_token_by_hash(th))
        membership_claim_is_current = await _operator_membership_claim_is_current(
            repository,
            facts,
            token_data,
            datetime.fromtimestamp(now_ts, UTC),
        )
        self._cache_inventory_facts(
            th,
            facts=facts,
            token_data=token_data,
            now_ts=now_ts,
            guard=_VerifyCacheGuard(
                generation_before=gen_before,
                membership_claim_is_current=membership_claim_is_current,
            ),
        )
        _log_inventory_rejection(facts, token_data, now_ts)
        return _outcome_for(
            facts,
            token_data,
            expected_token_type,
            now_ts,
            membership_claim_is_current,
        )

    def _cache_inventory_facts(
        self,
        token_hash: str,
        *,
        facts: _InventoryFacts,
        token_data: TokenClaims,
        now_ts: float,
        guard: _VerifyCacheGuard,
    ) -> None:
        """Memoise one read's inventory facts, prune on overflow, skip on stale generation.

        The ``gen_before`` parameter guards against a race:
        if the caller sampled the per-user generation
        before the DB read and a concurrent
        :meth:`invalidate_user_cache` incremented it during that
        read, the facts we are about to cache may reflect a user
        state that an admin event has already superseded. In that
        case we do NOT cache — the next verify hit re-reads the DB
        rather than serving a stale positive from the LRU.
        Legacy tokens pre-dating this flow can have a blank
        claim ``user_public_id=""``. The sample for the race guard
        MUST come from a key sampled BEFORE the DB await so a
        concurrent ``invalidate_user_cache`` that bumps the real
        user id during the await is observable as a mismatch. When
        the claim is blank we cannot know the row's id without
        reading the DB first, which defeats the guard entirely.
        The safe resolution is to skip caching blank-claim tokens
        completely — fail-closed. These legacy tokens pay a perf
        penalty (always DB-backed) but cannot slip a stale positive
        into cache during a race. Login and delegate issuance always
        populate the claim from the DB-backed principal, but
        ``TokenClaims.user_public_id`` still defaults to ``""`` for
        decode tolerance and ``TokenManager.refresh_tokens`` re-mints
        new pairs from old claims, so blank-claim tokens remain
        representable. The exposure window is not the 15-minute
        access TTL alone: refresh tokens and long-lived delegate
        tokens live for days, bounded by
        :data:`LONG_LIVED_TOKEN_EXPIRE_DAYS`. This skip MUST remain
        as long as a blank claim can decode — removing it
        reintroduces the stale-positive-during-deactivation race.

        Args:
            token_hash: SHA-256 hex digest of the token (cache key).
            facts: Inventory facts just read for ``token_hash``,
                including the row's purpose. Facts and not a verdict,
                so a hit can be re-judged under a different
                ``expected_token_type`` than the miss was.
            token_data: Verified :class:`TokenClaims` used for the
                cache entry's expiry stamp.
            now_ts: Monotonic-ish "now" the caller also used to read
                the cache — keeps the TTL anchored to one clock
                sample per request.
            guard: Generation sampled before DB reads plus the live membership
                verdict. A generation mismatch skips the write.
        """
        if not token_data.user_public_id:
            logger.debug(
                "verify_cache skip: blank-claim legacy token cannot be guarded "
                "against invalidate-during-DB-read races"
            )
            return
        sample_key = token_data.user_public_id
        gen_now = self._user_cache_generations.get(sample_key, 0)
        if gen_now != guard.generation_before:
            logger.debug(
                "verify_cache skip: generation advanced during DB read — user={}",
                sample_key,
            )
            return
        self._verify_cache[token_hash] = _VerifyCacheEntry(
            facts=facts,
            expires_at_ts=float(token_data.exp),
            cached_at_ts=now_ts,
            role=token_data.role,
            operator_public_ids=tuple(token_data.operator_public_ids),
            operator_membership_public_ids=tuple(
                sorted(token_data.operator_membership_public_ids.items())
            ),
            has_global_operator_authority=has_effective_permission(
                token_data.role,
                token_data.permissions,
                token_data.permission_scope_version,
                Permission.IMPERSONATE_OPERATOR,
            ),
            membership_claim_is_current=guard.membership_claim_is_current,
        )
        if len(self._verify_cache) > VERIFY_CACHE_MAX_ENTRIES:
            self._prune_verify_cache(now_ts)

    def _prune_verify_cache(self, now_ts: float) -> None:
        """Drop stale entries AND hard-evict the oldest to stay bounded.

        Called opportunistically when the cache exceeds
        data:`VERIFY_CACHE_MAX_ENTRIES`. Two passes
            1. Stale pass — drop every entry whose 30-second TTL has
               lapsed or whose JWT ``exp`` has passed. Cheap and
               usually enough under steady-state traffic.
            2. Hard-cap pass — if the cache is STILL over the
               threshold (burst of fresh unique tokens all within
               TTL), evict the ``overflow`` oldest entries by
               ``cached_at_ts`` via :func:`heapq.nsmallest`
               (O(n log overflow), typically O(n) for
               overflow=1). Guarantees bounded memory even under
                pathological spray-of-fresh-tokens load without
                paying the O(n log n) cost of a full sort on every
               insert at capacity.
        """
        stale_keys: list[str] = [
            key
            for key, entry in self._verify_cache.items()
            if entry.cached_at_ts + VERIFY_CACHE_TTL_SECONDS <= now_ts
            or entry.expires_at_ts <= now_ts
        ]
        for key in stale_keys:
            del self._verify_cache[key]
        hard_evicted = 0
        if len(self._verify_cache) > VERIFY_CACHE_MAX_ENTRIES:
            overflow = len(self._verify_cache) - VERIFY_CACHE_MAX_ENTRIES
            victims = heapq.nsmallest(
                overflow,
                self._verify_cache.items(),
                key=lambda item: item[1].cached_at_ts,
            )
            for key, _entry in victims:
                del self._verify_cache[key]
            hard_evicted = len(victims)
        logger.debug(
            "verify_cache prune: size_after={} stale_evicted={} hard_evicted={}",
            len(self._verify_cache),
            len(stale_keys),
            hard_evicted,
        )

    def invalidate_user_cache(self, user_public_id: str) -> int:
        """Evict every cache entry whose ``user_public_id`` matches.

        Called by the admin-bus subscriber on receipt of
        ``admin.user_deactivated`` so a cross-instance deactivation
        propagates to this TokenManager's LRU without waiting for
        the 30-second TTL.
        The user's
        generation counter is bumped FIRST so any ``verify_token_with_reason``
        that is mid-flight on this user (already past the DB read)
        sees a generation mismatch in :meth:`_cache_inventory_facts` and
        skips the cache write. Without the bump, a concurrent
        verify could repopulate a freshly-evicted entry with a
        stale positive verdict and admit a deactivated user for up
        to the 30-second TTL.

        Args:
            user_public_id: UUID of the user whose cache entries
                should be evicted. Unknown users are a no-op (count
                of 0 returned, not an error).

        Returns:
            Number of entries evicted. Observable via the return
            value for tests — no additional metrics surface
            is required.
        """
        self._user_cache_generations[user_public_id] = (
            self._user_cache_generations.get(user_public_id, 0) + 1
        )
        matching_keys = [
            key
            for key, entry in self._verify_cache.items()
            if entry.facts.user_public_id == user_public_id
        ]
        for key in matching_keys:
            del self._verify_cache[key]
        if matching_keys:
            logger.info(
                "invalidate_user_cache: user={} evicted={}",
                user_public_id,
                len(matching_keys),
            )
        return len(matching_keys)

    def refresh_tokens(self, refresh_token: str) -> TokenPair | None:
        """Refresh token pair using refresh token.

        Blacklists the old refresh token and creates new pair.

        Args:
            refresh_token: Valid refresh token.

        Returns:
            New TokenPair or None if refresh token invalid.
        """
        token_data = self.verify_token(refresh_token)
        if not token_data:
            return None
        if not token_data.jti.startswith(REFRESH_JTI_PREFIX):
            logger.warning("Attempted to refresh with non-refresh token")
            return None
        self.blacklist_token(token_data.jti)
        principal = AuthPrincipal(
            username=token_data.username,
            role=token_data.role,
            user_public_id=token_data.user_public_id,
            operator_public_ids=token_data.operator_public_ids,
            operator_membership_public_ids=token_data.operator_membership_public_ids,
            primary_operator_public_id=token_data.primary_operator_public_id,
            active_wallet_public_id=token_data.active_wallet_public_id,
            permissions=token_data.permissions,
        )
        permissions: Iterable[Permission | str] | None = None
        if token_data.permission_scope_version is not None:
            permissions = get_effective_permissions(
                token_data.role,
                token_data.permissions or [],
                token_data.permission_scope_version,
            )
        new_tokens = self.create_tokens(
            principal,
            session_id=token_data.sid,
            permissions=permissions,
        )
        logger.info(f"Refreshed tokens for user {principal.username}")
        return new_tokens

    def blacklist_token(self, jti: str) -> None:
        """Blacklist a token with grace period.

        Token can still be used during grace period to handle
        concurrent requests.

        Args:
            jti: JWT ID to blacklist.
        """
        current_time = datetime.now(UTC).timestamp()
        self._record_blacklisted_token(jti, current_time)
        self._enforce_blacklist_cap()
        logger.info(f"Blacklisted token with grace period: {jti}")

    def blacklist_token_immediately(self, jti: str) -> None:
        """Blacklist a token with immediate effect.

        Token is invalid immediately without grace period.

        Args:
            jti: JWT ID to blacklist.
        """
        past_time = datetime.now(UTC).timestamp() - self._blacklist_grace_period - 1
        self._record_blacklisted_token(jti, past_time)
        self._enforce_blacklist_cap()
        logger.info(f"Immediately blacklisted token: {jti}")

    def _enforce_blacklist_cap(self) -> None:
        """Hard-cap the in-memory JTI blacklist.

        Opportunistic cleanup via :meth:`_cleanup_old_blacklist_entries`
        only runs on the verify path, so a low-traffic instance that
        absorbs a mass deactivation can leak memory until every grace
        period expires.
        The cap uses the same ``heapq.nsmallest`` pattern
        as the verify-cache prune: when the set exceeds
        data:`BLACKLIST_MAX_ENTRIES`, evict the oldest overflow so
        the set stays bounded by a predictable multiplier of the
        expected live-token population.
        A bounded eviction window can drop an entry that is still
        inside its grace period; that is acceptable because the
        underlying ``user_active_tokens`` inventory + SCD2-active
        ``users.is_active`` check in :meth:`verify_token_with_db`
        remain authoritative. The in-memory blacklist is a fast
        path, not the source of truth.
        """
        overflow = len(self._blacklisted_tokens) - BLACKLIST_MAX_ENTRIES
        if overflow <= 0:
            return
        victims = heapq.nsmallest(
            overflow,
            self._blacklisted_tokens.items(),
            key=lambda kv: kv[1],
        )
        for key, _ts in victims:
            del self._blacklisted_tokens[key]
        logger.warning(
            "blacklist cap: evicted {} oldest entries (size_after={})",
            overflow,
            len(self._blacklisted_tokens),
        )

    async def revoke_user_sessions(
        self,
        user_public_id: str,
        repository: Repository,
        *,
        immediate: bool = False,
    ) -> int:
        """Revoke every active session for a user.

        Two-step revocation pushes state into BOTH the DB inventory
        AND the in-memory fast-path blacklist so ``verify_token``
        rejects the next request regardless of which layer it
        consults first
            1. Load every unrevoked JTI from ``user_active_tokens``
               via :meth:`Repository.list_active_user_token_jtis`.
            2. Flip ``revoked_at=NOW()`` on those rows via
               meth:`Repository.revoke_user_active_tokens` — atomic
               per SQLAlchemy UPDATE.
            3. For every JTI loaded in step 1, add it to
               attr:`_blacklisted_tokens`, using the normal grace period
               unless ``immediate=True`` requests a hard authority barrier.
        Caller (``UserService.deactivate_user``) publishes the
        ``admin.user_deactivated`` bus event AFTER commit — this
        method deliberately does NOT publish the event itself so
        the single-publisher contract
        is preserved: any cross-instance TokenManager will evict on
        receipt of the bus event, NOT on a competing publish from
        here.

        Args:
            user_public_id: UUID of the user whose sessions are
                being revoked.
            repository: Active :class:`Repository` used for DB
                state. Passed in (not looked up via singleton) so
                the caller's transactional scope is respected — for
                example the kill-switch can be driven from a
                migration script that attaches its own session.
            immediate: Whether blacklist entries reject without the normal
                concurrent-request grace period. Defaults to ``False`` to
                preserve user-deactivation and logout semantics.

        Returns:
            Count of rows revoked (0 if user had no active
            sessions — not an error).
        """
        jtis = await repository.list_active_user_token_jtis(user_public_id)
        revoked_at = datetime.now(UTC)
        count = await repository.revoke_user_active_tokens(user_public_id, revoked_at)
        for jti in jtis:
            if immediate:
                self.blacklist_token_immediately(jti)
            else:
                self.blacklist_token(jti)
        logger.info(
            "revoke_user_sessions: user={} revoked_rows={} blacklisted_jtis={} immediate={}",
            user_public_id,
            count,
            len(jtis),
            immediate,
        )
        return count

    def invalidate_token(self, token: str) -> None:
        """Invalidate a token immediately.

        Args:
            token: JWT token string to invalidate.
        """
        token_data = self.verify_token(token)
        if token_data:
            self.blacklist_token_immediately(token_data.jti)

    def invalidate_user_tokens(self, user_id: str) -> None:
        """Invalidate all tokens for a user.

        Args:
            user_id: User ID whose tokens to invalidate.
        """
        logger.info(f"Invalidated all tokens for user: {user_id}")

    def create_csrf_token(self) -> str:
        """Create a new CSRF token.

        Returns:
            URL-safe random token string.
        """
        return secrets.token_urlsafe(32)

    def verify_csrf_token(self, token: str, expected: str) -> bool:
        """Verify CSRF token matches expected value.

        Args:
            token: Token to verify.
            expected: Expected token value.

        Returns:
            True if tokens match.
        """
        return secrets.compare_digest(token, expected)

    async def start_admin_listener(self, zmq_broker_xpub: str) -> None:
        """Open the admin-bus subscriber and start the dispatch task.

        Subscribes to ``admin.user_deactivated`` so a cross-instance
        kill-switch event published by ``UserService.deactivate_user``
        collapses the fallback DB-scan ceiling to one bus-message
        round-trip when the broker is healthy.
        On receipt, :meth:`invalidate_user_cache` walks
        ``_verify_cache`` and drops every entry whose cached
        ``user_public_id`` matches the deactivated user's UUID.
        When lifespan injected a repository factory, a companion DB
        fallback scanner also walks cached users and evicts those whose
        SCD2-active ``users`` row is inactive, so broker outages cannot
        leave stale positive verdicts resident until token expiry.
        Idempotent + restart-safe via ``_admin_listener_lock``
        a second call while a healthy listener is running is a
        no-op; a second call after the previous task finished
        early reaps the dead task and re-allocates so the
        cross-instance eviction stays live across single-listener
        failures.

        Args:
            zmq_broker_xpub: Address of the broker's XPUB endpoint.
                Empty string skips the listener entirely (test
                mode / single-instance deployments where the TTL
                alone is sufficient).
        """
        async with self._admin_listener_lock:
            if self._admin_listen_task is not None and not self._admin_listen_task.done():
                self._start_deactivation_fallback_unlocked()
                return
            if self._admin_listen_task is not None:
                await self._reap_admin_listener_unlocked()
            self._start_deactivation_fallback_unlocked()
            if not zmq_broker_xpub:
                logger.info("TokenManager: empty broker XPUB, skipping admin listener")
                return
            self._admin_zmq_context = zmq.asyncio.Context()
            raw_sub_socket = self._admin_zmq_context.socket(zmq.SUB)
            apply_hwm(raw_sub_socket, rcvhwm=HWM_AUDIT)
            raw_sub_socket.connect(zmq_broker_xpub)
            self._admin_subscriber = ValidatedSubscriber(raw_sub_socket)
            self._admin_subscriber.subscribe(_ADMIN_USER_DEACTIVATED_TOPIC)
            self._admin_subscriber.subscribe(_ADMIN_MEMBERSHIP_REVOKED_TOPIC)
            self._admin_running = True
            self._admin_listen_task = asyncio.create_task(self._admin_listen_loop())
            logger.info(
                "TokenManager: admin-bus listener subscribed to {} + {} on {}",
                _ADMIN_USER_DEACTIVATED_TOPIC,
                _ADMIN_MEMBERSHIP_REVOKED_TOPIC,
                zmq_broker_xpub,
            )

    def _start_deactivation_fallback_unlocked(self) -> None:
        """Start the DB-backed deactivation fallback scanner when configured."""
        if self._deactivation_repository_factory is None:
            return
        self._deactivation_scan_task = start_deactivation_fallback_task(
            self._deactivation_scan_task,
            self._deactivation_fallback_scan_loop,
        )

    async def stop_admin_listener(self) -> None:
        """Cancel the dispatch task, close the subscriber, terminate the context.

        Serialised against :meth:`start_admin_listener` via
        ``_admin_listener_lock`` so an overlapping start cannot
        allocate a new socket while we are tearing the old one
        down. Idempotent.
        """
        async with self._admin_listener_lock:
            await self._reap_admin_listener_unlocked()

    async def _reap_admin_listener_unlocked(self) -> None:
        """Tear down listener resources. Caller MUST hold ``_admin_listener_lock``.

        Captures every resource reference into locals BEFORE
        clearing the attributes so a follow-up
        meth:`start_admin_listener` (which runs after we release
        the lock) sees a fully-clean slate and cannot interfere
        with the close + term calls below.
        """
        self._admin_running = False
        task = self._admin_listen_task
        scan_task = self._deactivation_scan_task
        subscriber = self._admin_subscriber
        context = self._admin_zmq_context
        self._admin_listen_task = None
        self._deactivation_scan_task = None
        self._admin_subscriber = None
        self._admin_zmq_context = None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        await stop_deactivation_fallback_task(scan_task)
        if subscriber is not None:
            with contextlib.suppress(Exception):
                subscriber.close()
        if context is not None:
            with contextlib.suppress(Exception):
                context.term()

    async def _admin_listen_loop(self) -> None:
        """Receive admin-bus events and dispatch to per-topic handlers.

        Per-message failures (parse errors, handler exceptions,
        recv errors) are caught + logged so a single bad frame
        can never silently stop the listener. Only
        ``asyncio.CancelledError`` from
        :meth:`stop_admin_listener` unwinds the loop.
        """
        subscriber = self._admin_subscriber
        if subscriber is None:
            return
        try:
            while self._admin_running:
                frame = await self._admin_recv_one_frame(subscriber)
                if frame is None:
                    continue
                await self._admin_dispatch_frame(*frame)
        except asyncio.CancelledError:
            logger.info("TokenManager: admin listen loop cancelled")
            raise

    async def _deactivation_fallback_scan_loop(self) -> None:
        """Periodically evict cached users whose DB row is inactive."""
        await run_deactivation_fallback_loop(
            self._scan_deactivated_cached_users_once,
            component_name="TokenManager",
        )

    async def _scan_deactivated_cached_users_once(self) -> None:
        """Cross-check cached users against user, token, and membership state."""
        cache_snapshot = tuple(self._verify_cache.values())
        user_public_ids = sorted(
            {entry.facts.user_public_id for entry in cache_snapshot if entry.facts.user_public_id}
        )
        inactive_user_public_ids = await list_inactive_user_public_ids(
            self._deactivation_repository_factory,
            user_public_ids,
            component_name="TokenManager",
        )
        active_jtis_by_user = await list_active_user_token_jtis_by_user(
            self._deactivation_repository_factory,
            user_public_ids,
            component_name="TokenManager",
        )
        stale_token_user_public_ids = {
            entry.facts.user_public_id
            for entry in cache_snapshot
            if entry.facts.user_public_id in active_jtis_by_user
            and entry.facts.jti not in active_jtis_by_user[entry.facts.user_public_id]
        }
        for user_public_id in set(inactive_user_public_ids) | stale_token_user_public_ids:
            self.invalidate_user_cache(user_public_id)
        membership_claims = [
            OperatorMembershipClaim(
                user_public_id=entry.facts.user_public_id,
                role=entry.role,
                has_global_operator_authority=entry.has_global_operator_authority,
                operator_public_ids=entry.operator_public_ids,
                operator_membership_public_ids=entry.operator_membership_public_ids,
            )
            for entry in cache_snapshot
            if entry.facts.user_public_id
        ]
        stale_membership_user_public_ids = await list_users_with_stale_operator_memberships(
            self._deactivation_repository_factory,
            membership_claims,
            datetime.now(UTC),
            component_name="TokenManager",
        )
        for user_public_id in stale_membership_user_public_ids:
            self.invalidate_user_cache(user_public_id)

    async def _admin_recv_one_frame(
        self, subscriber: ValidatedSubscriber
    ) -> tuple[str, str] | None:
        """Receive and decode one admin-bus frame.

        Returns ``None`` (after a small backoff) when recv raises
        a non-cancellation error OR when the decoded bytes are
        not valid UTF-8 — both cases let the caller simply
        ``continue`` instead of letting a malformed frame unwind
        the listener loop. Decode is INSIDE the ``try`` so a
        :exc:`UnicodeDecodeError` cannot escape the helper.
        """
        try:
            topic_bytes, payload_bytes = await subscriber.recv_multipart()
            topic = topic_bytes.decode() if isinstance(topic_bytes, bytes) else str(topic_bytes)
            payload = (
                payload_bytes.decode() if isinstance(payload_bytes, bytes) else str(payload_bytes)
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("TokenManager admin listener recv failed: {}", exc)
            await asyncio.sleep(_ADMIN_LISTEN_RECV_BACKOFF_S)
            return None
        return topic, payload

    async def _admin_dispatch_frame(self, topic: str, payload: str) -> None:
        """Route one decoded admin-bus frame to its typed handler.

        Handler exceptions other than ``CancelledError`` are
        logged + swallowed so one bad frame cannot stop the
        listener. The ``await asyncio.sleep(0)`` yield point is
        deliberate: under a burst of admin events it prevents the
        dispatch loop from starving other tasks on the event loop
        between frames.
        """
        await asyncio.sleep(0)
        try:
            if topic == _ADMIN_USER_DEACTIVATED_TOPIC:
                self._handle_user_deactivated(UserDeactivatedData.from_json(payload))
            elif topic == _ADMIN_MEMBERSHIP_REVOKED_TOPIC:
                self._handle_membership_revoked(MembershipRevokedData.from_json(payload))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                "TokenManager admin handler failed: topic={} err={}",
                topic,
                exc,
            )

    def _handle_user_deactivated(self, data: UserDeactivatedData) -> None:
        """Evict every cache entry for ``data.user_public_id``.

        Thin shim over :meth:`invalidate_user_cache`; exists as a
        seam so tests + the listener share one dispatch surface.
        Synchronous because :meth:`invalidate_user_cache` does no
        I/O — walking the dict + deleting matching entries is
        pure in-process work.
        """
        self.invalidate_user_cache(data.user_public_id)

    def _handle_membership_revoked(self, data: MembershipRevokedData) -> None:
        """Evict every cached credential for the detached desk member.

        Args:
            data: Typed membership-revocation event naming the affected user.
        """
        self.invalidate_user_cache(data.user_public_id)

    @classmethod
    def get_instance(cls) -> TokenManager:
        """Get singleton instance.

        Returns:
            TokenManager singleton instance.
        """
        if cls._instance is None:
            cls._instance = TokenManager()
        return cls._instance

    @classmethod
    def clear_instance(cls) -> None:
        """Clear singleton instance for testing."""
        cls._instance = None


class WebSocketTokenRotator:
    """WebSocket token rotation manager singleton.

    Manages token lifecycle for WebSocket connections including
    registration, rotation, and expiration checking.
    """

    _instance: WebSocketTokenRotator | None = None
    _initialized: bool = False

    def __new__(cls, token_manager: TokenManager | None = None) -> WebSocketTokenRotator:
        """Create or return singleton WebSocket token rotator instance.

        Args:
            token_manager: Optional token manager instance.

        Returns:
            WebSocketTokenRotator singleton instance.
        """
        if cls._instance is None:
            instance = super().__new__(cls)
            cls._instance = instance
        return cls._instance

    def __init__(self, token_manager: TokenManager | None = None) -> None:
        """Initialize the rotator.

        Args:
            token_manager: Optional token manager instance.
        """
        if self._initialized:
            return
        self._initialized = True
        self.token_manager = token_manager or TokenManager.get_instance()
        self._connection_tokens: dict[str, str] = {}

    def register_connection(self, connection_id: str, token: str) -> bool:
        """Register a WebSocket connection with token.

        Args:
            connection_id: Unique connection identifier.
            token: JWT access token.

        Returns:
            True if registration successful.
        """
        token_data = self.token_manager.verify_token(token)
        if not token_data:
            return False
        self._connection_tokens[connection_id] = token
        logger.info(f"Registered WS connection {connection_id} for user {token_data.username}")
        return True

    def update_connection_token(self, connection_id: str, token: str) -> None:
        """Update token for existing connection.

        Args:
            connection_id: Connection identifier.
            token: New JWT token.
        """
        self._connection_tokens[connection_id] = token

    def get_connection_token(self, connection_id: str) -> str | None:
        """Get token for a connection.

        Args:
            connection_id: Connection identifier.

        Returns:
            Token string or None if not registered.
        """
        return self._connection_tokens.get(connection_id)

    def should_rotate_token(self, connection_id: str) -> bool:
        """Check if connection token should be rotated.

        Args:
            connection_id: Connection identifier.

        Returns:
            True if token expires within 5 minutes.
        """
        if connection_id not in self._connection_tokens:
            return False
        token = self._connection_tokens[connection_id]
        token_data = self.token_manager.verify_token(token)
        if not token_data:
            return True
        expires_in = token_data.exp - datetime.now(UTC).timestamp()
        return expires_in < 300

    def rotate_connection_token(self, connection_id: str, refresh_token: str) -> str | None:
        """Rotate token for a connection.

        Args:
            connection_id: Connection identifier.
            refresh_token: Refresh token for rotation.

        Returns:
            New access token or None if rotation failed.
        """
        new_tokens = self.token_manager.refresh_tokens(refresh_token)
        if not new_tokens:
            return None
        self._connection_tokens[connection_id] = new_tokens.access_token
        logger.info(f"Rotated token for WS connection {connection_id}")
        return new_tokens.access_token

    def unregister_connection(self, connection_id: str) -> None:
        """Unregister a WebSocket connection.

        Args:
            connection_id: Connection identifier to remove.
        """
        if connection_id in self._connection_tokens:
            del self._connection_tokens[connection_id]
            logger.info(f"Unregistered WS connection {connection_id}")

    @classmethod
    def get_instance(cls) -> WebSocketTokenRotator:
        """Get singleton instance.

        Returns:
            WebSocketTokenRotator singleton instance.
        """
        if cls._instance is None:
            cls._instance = WebSocketTokenRotator()
        return cls._instance

    @classmethod
    def clear_instance(cls) -> None:
        """Clear singleton instance for testing."""
        cls._instance = None


def get_token_manager() -> TokenManager:
    """Get TokenManager singleton.

    Returns:
        TokenManager instance.
    """
    return TokenManager.get_instance()


def get_ws_token_rotator() -> WebSocketTokenRotator:
    """Get WebSocketTokenRotator singleton.

    Returns:
        WebSocketTokenRotator instance.
    """
    return WebSocketTokenRotator.get_instance()
