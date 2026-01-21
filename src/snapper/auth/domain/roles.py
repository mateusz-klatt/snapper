"""User role definitions module.

This module defines the user role enumeration for the authentication
and authorization system.
"""

from enum import Enum


class UserRole(str, Enum):
    """User role enumeration.

    Defines the available roles in the system with hierarchical
    access levels:
    - VIEWER: Read-only access to market data and positions
    - OPERATOR: Can execute trades and manage strategies
    - ADMIN: Full system access including user management
    """

    VIEWER = "viewer"
    OPERATOR = "operator"
    ADMIN = "admin"
