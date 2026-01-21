"""Permission definitions module.

This module defines granular permissions and role-permission mappings
for the authorization system.
"""

from enum import Enum

from snapper.auth.domain.roles import UserRole


class Permission(str, Enum):
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
    READ_STRATEGIES = "read:strategies"
    START_STRATEGIES = "start:strategies"
    STOP_STRATEGIES = "stop:strategies"
    CONFIGURE_STRATEGIES = "configure:strategies"
    READ_SYSTEM_STATUS = "read:system_status"
    MANAGE_PROCESSES = "manage:processes"
    CONFIGURE_SYSTEM = "configure:system"
    MANAGE_USERS = "manage:users"


ROLE_PERMISSIONS: dict[UserRole, set[Permission]] = {
    UserRole.VIEWER: {
        Permission.READ_MARKET_DATA,
        Permission.READ_ORDERS,
        Permission.READ_POSITIONS,
        Permission.READ_STRATEGIES,
        Permission.READ_SYSTEM_STATUS,
    },
    UserRole.OPERATOR: {
        Permission.READ_MARKET_DATA,
        Permission.READ_ORDERS,
        Permission.CREATE_ORDERS,
        Permission.CANCEL_ORDERS,
        Permission.READ_POSITIONS,
        Permission.MANAGE_POSITIONS,
        Permission.READ_STRATEGIES,
        Permission.START_STRATEGIES,
        Permission.STOP_STRATEGIES,
        Permission.READ_SYSTEM_STATUS,
        Permission.MANAGE_PROCESSES,
    },
    UserRole.ADMIN: set(Permission),
}
