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
    MANAGE_PROCESSES = "manage:processes"
    CONFIGURE_SYSTEM = "configure:system"
    MANAGE_USERS = "manage:users"
    READ_WALLET_CREDENTIALS = "read:wallet_credentials"
    MANAGE_WALLET_CREDENTIALS = "manage:wallet_credentials"
    MANAGE_SCOPE_GRANTS = "manage:scope_grants"
    IMPERSONATE_OPERATOR = "impersonate:operator"
    READ_BACKTESTS = "read:backtests"
    MANAGE_BACKTESTS = "manage:backtests"
    READ_NOTIFICATIONS = "read:notifications"
    MANAGE_NOTIFICATION_DEVICES = "manage:notification_devices"
    MANAGE_PAIRED_EXECUTION = "manage:paired_execution"


RESOURCE_PERMISSIONS: dict[str, Permission | None] = {
    "overview": None,
    "market": Permission.READ_MARKET_DATA,
    "processes": Permission.MANAGE_PROCESSES,
    "strategies": Permission.READ_STRATEGIES,
    "orders": Permission.READ_ORDERS,
    "positions": Permission.READ_POSITIONS,
    "signals": Permission.READ_MARKET_DATA,
    "health": Permission.READ_SYSTEM_STATUS,
    "admin": Permission.MANAGE_USERS,
    "settings": Permission.CONFIGURE_SYSTEM,
    "backtests": Permission.READ_BACKTESTS,
    "ai-integration": Permission.MANAGE_PROCESSES,
    "ai-reviews": Permission.READ_SIGNALS,
    "notifications": Permission.READ_NOTIFICATIONS,
}


ROLE_PERMISSIONS: dict[UserRole, set[Permission]] = {
    UserRole.AI_DELEGATE: {
        Permission.READ_MARKET_DATA,
        Permission.READ_ORDERS,
        Permission.CREATE_ORDERS,
        Permission.CANCEL_ORDERS,
        Permission.READ_POSITIONS,
        Permission.MANAGE_POSITIONS,
        Permission.READ_STRATEGIES,
        Permission.READ_SIGNALS,
        Permission.READ_SYSTEM_STATUS,
        Permission.READ_BACKTESTS,
    },
    UserRole.VIEWER: {
        Permission.READ_MARKET_DATA,
        Permission.READ_ORDERS,
        Permission.READ_POSITIONS,
        Permission.READ_ACCOUNT_STATE,
        Permission.READ_STRATEGIES,
        Permission.READ_SYSTEM_STATUS,
        Permission.READ_BACKTESTS,
        Permission.READ_NOTIFICATIONS,
        Permission.MANAGE_NOTIFICATION_DEVICES,
    },
    UserRole.OPERATOR: {
        Permission.READ_MARKET_DATA,
        Permission.READ_ORDERS,
        Permission.CREATE_ORDERS,
        Permission.CANCEL_ORDERS,
        Permission.READ_POSITIONS,
        Permission.MANAGE_POSITIONS,
        Permission.READ_ACCOUNT_STATE,
        Permission.READ_STRATEGIES,
        Permission.START_STRATEGIES,
        Permission.STOP_STRATEGIES,
        Permission.READ_SIGNALS,
        Permission.READ_SYSTEM_STATUS,
        Permission.MANAGE_PROCESSES,
        Permission.MANAGE_PAIRED_EXECUTION,
        Permission.READ_BACKTESTS,
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
    "ai_reviews": frozenset({Permission.READ_SIGNALS, Permission.CREATE_ORDERS}),
    "notifications": frozenset({Permission.READ_NOTIFICATIONS}),
}
"""Permission sets required for each WS topic category.

A role is allowed a category when it holds **all** permissions in the
category's frozenset.  This is intentionally stricter than REST read
access: a VIEWER can GET /orders (READ_ORDERS) but cannot subscribe
to live order events (requires CREATE_ORDERS, i.e. OPERATOR+).

The ``signals`` category is intentionally split from ``strategy``: an
AI_DELEGATE (holding ``READ_SIGNALS`` but NOT ``START_STRATEGIES``)
must be able to subscribe to ``signals.*`` for wallet-scope
filtered read access, without gaining the operator-level
strategy-management surface that ``strategy.*`` carries.

Using frozensets instead of single permissions makes the model
extensible: adding a second permission to a category's set is a
one-line change, and ``get_role_allowed_categories`` already handles it.
"""


def get_role_allowed_categories(role: UserRole) -> set[str]:
    """Derive allowed WS topic categories from ROLE_PERMISSIONS.

    A role is allowed a category when it holds every permission
    listed in ``CATEGORY_PERMISSIONS`` for that category.

    Args:
        role: User role to check.

    Returns:
        Set of allowed WS topic category names.
    """
    role_perms = ROLE_PERMISSIONS.get(role, set())
    return {
        category for category, required in CATEGORY_PERMISSIONS.items() if required <= role_perms
    }
