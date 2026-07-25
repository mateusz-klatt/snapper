"""Permission definitions module.

This module defines granular permissions and role-permission mappings
for the authorization system.
"""

from enum import StrEnum

from snapper.auth.domain.roles import UserRole


class Permission(StrEnum):
    """Permission enumeration.

    Defines granular permissions following resource:action pattern.
    Permissions are grouped by resource type:
    - market_data: Market data access
    - orders: Order management
    - positions: Position management
    - strategies: Strategy lifecycle
    - system_status: System monitoring
    - processes: Process management
    - system: System configuration
    - users: User management
    """

    READ_MARKET_DATA = "read:market_data"
    READ_MARKET_VIEWS = "read:market_views"
    SUBMIT_MARKET_VIEW = "submit:market_view"
    SUBMIT_AI_REVIEW_DECISION = "submit:ai_review_decision"
    READ_ORDERS = "read:orders"
    CREATE_ORDERS = "create:orders"
    CANCEL_ORDERS = "cancel:orders"
    READ_POSITIONS = "read:positions"
    MANAGE_POSITIONS = "manage:positions"
    READ_ACCOUNT_STATE = "read:account_state"
    READ_STRATEGIES = "read:strategies"
    READ_SIGNALS = "read:signals"
    START_STRATEGIES = "start:strategies"
    STOP_STRATEGIES = "stop:strategies"
    CONFIGURE_STRATEGIES = "configure:strategies"
    READ_SYSTEM_STATUS = "read:system_status"
    MANAGE_RUNTIME_DIAGNOSTICS = "manage:runtime_diagnostics"
    READ_PROCESSES = "read:processes"
    MANAGE_PROCESSES = "manage:processes"
    READ_AI_REVIEWS = "read:ai_reviews"
    READ_AI_INTEGRATION = "read:ai_integration"
    MANAGE_AI_INTEGRATION = "manage:ai_integration"
    CONFIGURE_SYSTEM = "configure:system"
    MANAGE_USERS = "manage:users"
    READ_WALLET_CREDENTIALS = "read:wallet_credentials"
    MANAGE_WALLET_CREDENTIALS = "manage:wallet_credentials"
    MANAGE_SCOPE_GRANTS = "manage:scope_grants"
    IMPERSONATE_OPERATOR = "impersonate:operator"
    READ_BACKTESTS = "read:backtests"
    CREATE_BACKTEST_COMPARISONS = "create:backtest_comparisons"
    MANAGE_BACKTESTS = "manage:backtests"
    READ_NOTIFICATIONS = "read:notifications"
    MANAGE_NOTIFICATION_DEVICES = "manage:notification_devices"
    MANAGE_PAIRED_EXECUTION = "manage:paired_execution"


RESOURCE_PERMISSIONS: dict[str, frozenset[Permission]] = {
    "overview": frozenset(),
    "market": frozenset({Permission.READ_MARKET_DATA}),
    "processes": frozenset({Permission.READ_PROCESSES}),
    "strategies": frozenset({Permission.READ_STRATEGIES}),
    "orders": frozenset({Permission.READ_ORDERS}),
    "positions": frozenset({Permission.READ_POSITIONS}),
    "accounts": frozenset({Permission.READ_ACCOUNT_STATE}),
    "signals": frozenset({Permission.READ_SIGNALS}),
    "health": frozenset({Permission.READ_SYSTEM_STATUS}),
    "admin": frozenset({Permission.MANAGE_USERS}),
    "settings": frozenset({Permission.CONFIGURE_SYSTEM}),
    "backtests": frozenset({Permission.READ_BACKTESTS}),
    "ai-integration": frozenset({Permission.READ_AI_INTEGRATION}),
    "ai-reviews": frozenset(
        {
            Permission.READ_AI_REVIEWS,
            Permission.SUBMIT_AI_REVIEW_DECISION,
        }
    ),
    "notifications": frozenset({Permission.READ_NOTIFICATIONS}),
}
"""Permission alternatives that expose each authenticated client resource.

Each frozenset uses any-of semantics. An empty requirement exposes an
authenticated-only resource, while a non-empty requirement exposes the
resource when at least one listed permission is effective for the token.
"""


ROLE_PERMISSIONS: dict[UserRole, set[Permission]] = {
    UserRole.AI_RESEARCHER: {
        Permission.READ_MARKET_DATA,
        Permission.READ_MARKET_VIEWS,
        Permission.SUBMIT_MARKET_VIEW,
    },
    UserRole.AI_REVIEWER: {
        Permission.READ_MARKET_DATA,
        Permission.READ_MARKET_VIEWS,
        Permission.READ_ORDERS,
        Permission.READ_POSITIONS,
        Permission.READ_STRATEGIES,
        Permission.READ_SIGNALS,
        Permission.READ_SYSTEM_STATUS,
        Permission.MANAGE_RUNTIME_DIAGNOSTICS,
        Permission.READ_BACKTESTS,
        Permission.CREATE_BACKTEST_COMPARISONS,
        Permission.SUBMIT_AI_REVIEW_DECISION,
    },
    UserRole.AI_DELEGATE: {
        Permission.READ_MARKET_DATA,
        Permission.READ_MARKET_VIEWS,
        Permission.READ_ORDERS,
        Permission.CREATE_ORDERS,
        Permission.CANCEL_ORDERS,
        Permission.READ_POSITIONS,
        Permission.MANAGE_POSITIONS,
        Permission.READ_STRATEGIES,
        Permission.READ_SIGNALS,
        Permission.READ_SYSTEM_STATUS,
        Permission.MANAGE_RUNTIME_DIAGNOSTICS,
        Permission.READ_BACKTESTS,
        Permission.CREATE_BACKTEST_COMPARISONS,
        Permission.SUBMIT_AI_REVIEW_DECISION,
    },
    UserRole.VIEWER: {
        Permission.READ_MARKET_DATA,
        Permission.READ_ORDERS,
        Permission.READ_POSITIONS,
        Permission.READ_ACCOUNT_STATE,
        Permission.READ_STRATEGIES,
        Permission.READ_SIGNALS,
        Permission.READ_MARKET_VIEWS,
        Permission.READ_SYSTEM_STATUS,
        Permission.READ_PROCESSES,
        Permission.READ_AI_REVIEWS,
        Permission.READ_AI_INTEGRATION,
        Permission.READ_BACKTESTS,
        Permission.READ_NOTIFICATIONS,
        Permission.MANAGE_NOTIFICATION_DEVICES,
    },
    UserRole.OPERATOR: {
        Permission.READ_MARKET_DATA,
        Permission.READ_MARKET_VIEWS,
        Permission.READ_ORDERS,
        Permission.CREATE_ORDERS,
        Permission.CANCEL_ORDERS,
        Permission.READ_POSITIONS,
        Permission.MANAGE_POSITIONS,
        Permission.READ_ACCOUNT_STATE,
        Permission.READ_STRATEGIES,
        Permission.START_STRATEGIES,
        Permission.STOP_STRATEGIES,
        Permission.CONFIGURE_STRATEGIES,
        Permission.READ_SIGNALS,
        Permission.READ_SYSTEM_STATUS,
        Permission.MANAGE_RUNTIME_DIAGNOSTICS,
        Permission.READ_PROCESSES,
        Permission.MANAGE_PROCESSES,
        Permission.READ_AI_REVIEWS,
        Permission.READ_AI_INTEGRATION,
        Permission.MANAGE_AI_INTEGRATION,
        Permission.MANAGE_PAIRED_EXECUTION,
        Permission.READ_BACKTESTS,
        Permission.CREATE_BACKTEST_COMPARISONS,
        Permission.MANAGE_BACKTESTS,
        Permission.READ_NOTIFICATIONS,
        Permission.MANAGE_NOTIFICATION_DEVICES,
    },
    UserRole.ADMIN: set(Permission),
}


CATEGORY_PERMISSIONS: dict[str, frozenset[Permission]] = {
    "market": frozenset({Permission.READ_MARKET_DATA}),
    "trade": frozenset({Permission.CREATE_ORDERS}),
    "trade_events": frozenset({Permission.READ_ORDERS}),
    "signals": frozenset({Permission.READ_SIGNALS}),
    "strategy": frozenset({Permission.START_STRATEGIES}),
    "strategies_read": frozenset({Permission.READ_STRATEGIES}),
    "system": frozenset({Permission.READ_SYSTEM_STATUS}),
    "processes_admin": frozenset({Permission.MANAGE_PROCESSES}),
    "admin": frozenset({Permission.MANAGE_USERS}),
    "backtest": frozenset({Permission.READ_BACKTESTS}),
    "account_state": frozenset({Permission.READ_ACCOUNT_STATE}),
    "ai_reviews": frozenset({Permission.READ_SIGNALS, Permission.CREATE_ORDERS}),
    "ai_research": frozenset({Permission.SUBMIT_MARKET_VIEW}),
    "notifications": frozenset({Permission.READ_NOTIFICATIONS}),
}
"""Permission sets required for each WS topic category.

A role is allowed a category when it holds **all** permissions in the
category's frozenset. Command categories remain stricter than REST read
access, while read-side event families such as ``trade_events`` and
``account_state`` deliberately use the matching REST read permission.

The ``signals`` category is intentionally split from ``strategy``: an
AI_DELEGATE (holding ``READ_SIGNALS`` but NOT ``START_STRATEGIES``)
must be able to subscribe to ``signals.*`` for wallet-scope
filtered read access, without gaining the operator-level
strategy-management surface that ``strategy.*`` carries.

Using frozensets instead of single permissions makes the model
extensible: adding a second permission to a category's set is a
one-line change, and ``get_role_allowed_categories`` already handles it.
"""


_PERMISSION_SCOPE_V3_ADDITIONS: frozenset[Permission] = frozenset(
    {
        Permission.READ_PROCESSES,
        Permission.READ_AI_REVIEWS,
        Permission.READ_AI_INTEGRATION,
        Permission.MANAGE_AI_INTEGRATION,
        Permission.MANAGE_RUNTIME_DIAGNOSTICS,
        Permission.CREATE_BACKTEST_COMPARISONS,
    }
)
"""Catalog entries introduced after version-two token scopes were minted."""


_PERMISSION_SCOPE_V3_ROLE_ADDITIONS: dict[UserRole, frozenset[Permission]] = {
    UserRole.OPERATOR: frozenset({Permission.CONFIGURE_STRATEGIES}),
}
"""Existing catalog entries newly granted to a named set in scope v3."""


_NON_DOWNSCOPABLE_PERMISSIONS: frozenset[Permission] = frozenset({Permission.IMPERSONATE_OPERATOR})
"""Structural grants that historical explicit scopes could not remove.

Before token-effective permissions became the sole capability input, global
operator and wallet visibility consulted the canonical role set directly.
Retaining these grants during token intersection preserves that established
scope contract while allowing every runtime decision to consume effective
permissions instead of a role identity.
"""


def role_grants_permission(role: UserRole, permission: Permission) -> bool:
    """Return whether a named role permission set contains one permission.

    Args:
        role: Role whose canonical permission set is inspected.
        permission: Permission whose role-level grant is queried.

    Returns:
        True when ``ROLE_PERMISSIONS`` grants the permission to the role.
    """
    return permission in ROLE_PERMISSIONS.get(role, set())


def get_effective_permissions(
    role: UserRole,
    token_permissions: list[str] | None = None,
    permission_scope_version: int | None = None,
) -> set[Permission]:
    """Return the permission grant enforced for one authenticated token.

    The role mapping is always the authorization ceiling. An absent token
    permissions claim preserves the historical full-role grant for older
    tokens, while a supplied current claim can only narrow that role grant.
    Version-two tokens are migrated by capability equivalence: an exact old
    full-role grant adopts the current role set, while narrowed operational
    grants inherit only the permissions that replaced their retained v2
    signal and process capabilities. Narrow viewer grants remain exact.

    Args:
        role: Authenticated principal's role.
        token_permissions: Permission strings carried by the JWT, or
            ``None`` when the claim was absent.
        permission_scope_version: Permission-scope version carried by the
            JWT, or ``None`` when the claim was absent.

    Returns:
        Role-bounded effective permissions after applying the applicable
        token-scope compatibility projection.
    """
    role_permissions = ROLE_PERMISSIONS.get(role, set())
    if token_permissions is None:
        return set(role_permissions)
    token_permission_values = set(token_permissions)
    effective_permissions = {
        permission for permission in role_permissions if permission.value in token_permission_values
    }
    effective_permissions.update(role_permissions & _NON_DOWNSCOPABLE_PERMISSIONS)
    if permission_scope_version != 2:
        return effective_permissions

    v2_role_permissions = (
        role_permissions
        - _PERMISSION_SCOPE_V3_ADDITIONS
        - _PERMISSION_SCOPE_V3_ROLE_ADDITIONS.get(role, frozenset())
    )
    if token_permission_values == {permission.value for permission in v2_role_permissions}:
        return set(role_permissions)

    has_operational_migration = role_grants_permission(
        role,
        Permission.MANAGE_AI_INTEGRATION,
    )
    if (
        has_operational_migration
        and Permission.READ_SIGNALS in effective_permissions
        and role_grants_permission(role, Permission.READ_AI_REVIEWS)
    ):
        effective_permissions.add(Permission.READ_AI_REVIEWS)
    if has_operational_migration and Permission.MANAGE_PROCESSES in effective_permissions:
        process_replacements = {
            Permission.READ_PROCESSES,
            Permission.READ_AI_INTEGRATION,
            Permission.MANAGE_AI_INTEGRATION,
            Permission.START_STRATEGIES,
            Permission.STOP_STRATEGIES,
            Permission.CONFIGURE_STRATEGIES,
        }
        effective_permissions.update(
            permission
            for permission in process_replacements
            if role_grants_permission(role, permission)
        )
    compatibility_replacements = {
        Permission.READ_BACKTESTS: Permission.CREATE_BACKTEST_COMPARISONS,
        Permission.READ_SYSTEM_STATUS: Permission.MANAGE_RUNTIME_DIAGNOSTICS,
    }
    effective_permissions.update(
        replacement
        for historical, replacement in compatibility_replacements.items()
        if historical in effective_permissions and role_grants_permission(role, replacement)
    )
    return effective_permissions


def has_effective_permission(
    role: UserRole,
    token_permissions: list[str] | None,
    permission_scope_version: int | None,
    permission: Permission,
) -> bool:
    """Return whether one authenticated token effectively grants a capability.

    Runtime authorization and visibility decisions use this projection. Role
    identities enter only through the canonical named permission-set ceiling
    inside :func:`get_effective_permissions`.

    Args:
        role: Authenticated principal's named permission set.
        token_permissions: Explicit permission strings carried by the token.
        permission_scope_version: Permission-scope compatibility version.
        permission: Capability being decided.

    Returns:
        True when the role-bounded, compatibility-projected token grant
        contains ``permission``.
    """
    return permission in get_effective_permissions(
        role,
        token_permissions,
        permission_scope_version,
    )


def is_ai_review_decision_capable(
    role: UserRole,
    token_permissions: list[str] | None,
    permission_scope_version: int | None,
) -> bool:
    """Return whether one token can submit an AI review decision.

    The compatibility branch is deliberately limited to legacy
    non-user-administration permission sets that grant decision submission
    and whose token retained CREATE_ORDERS. A token minted BEFORE
    permission-scope versioning carries no ``permission_scope_version``
    claim at all, so an absent version is treated exactly like v1 — the
    production delegate token predates versioning, and requiring a literal
    ``1`` silently hid decision submission from it. The branch does not add
    CREATE_ORDERS to effective permissions or alias that permission for any
    authorization surface outside AI review decisions.

    Args:
        role: Authenticated principal's role.
        token_permissions: Permission strings carried by the JWT, or
            ``None`` when the claim was absent.
        permission_scope_version: Permission-scope version from the JWT,
            or ``None`` when the claim was absent.

    Returns:
        True when the effective grant contains the decision permission,
        or when the narrow v1 permission-set compatibility rule applies.
    """
    effective_permissions = get_effective_permissions(
        role,
        token_permissions,
        permission_scope_version,
    )
    if Permission.SUBMIT_AI_REVIEW_DECISION in effective_permissions:
        return True
    return (
        permission_scope_version in (None, 1)
        and role_grants_permission(role, Permission.SUBMIT_AI_REVIEW_DECISION)
        and not role_grants_permission(role, Permission.MANAGE_USERS)
        and Permission.CREATE_ORDERS in effective_permissions
    )


def get_role_allowed_categories(
    role: UserRole,
    token_permissions: list[str] | None = None,
    permission_scope_version: int | None = None,
) -> set[str]:
    """Derive allowed WS topic categories from effective permissions.

    A token is allowed a category when its role-bounded effective grant
    holds every permission listed in ``CATEGORY_PERMISSIONS`` for that
    category.

    Args:
        role: User role to check.
        token_permissions: Permission strings carried by the JWT, or
            ``None`` for the backward-compatible full-role grant.
        permission_scope_version: Permission-scope version carried by the
            JWT, or ``None`` when the claim was absent.

    Returns:
        Set of allowed WS topic category names.
    """
    effective_permissions = get_effective_permissions(
        role,
        token_permissions,
        permission_scope_version,
    )
    return {
        category
        for category, required in CATEGORY_PERMISSIONS.items()
        if required <= effective_permissions
    }
