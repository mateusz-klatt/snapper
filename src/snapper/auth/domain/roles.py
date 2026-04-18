"""User role definitions module.

This module defines the user role enumeration for the authentication
and authorization system.
"""

from enum import StrEnum


class UserRole(StrEnum):
    """User role enumeration.

    Defines the available roles in the system with hierarchical
    access levels:

    - AI_DELEGATE: Narrow permission set for AI-integration users
      (observe market/signals/orders, submit/cancel trades via MCP).
      **Ordinally below VIEWER** in the role_hierarchy dicts consulted
      by ``require_role()`` (``snapper.auth.dependencies``) and
      ``WebSocketAuthManager.has_permission()``
      (``snapper.auth.websocket_auth``) so any
      ``require_role(>= VIEWER)`` guard rejects AI_DELEGATE by
      ordinal comparison — intentional: access beyond the narrow
      permission set MUST go through
      ``require_permission(specific_perm)``.
      See ``plan_ai_integration_phase_a.md`` §3.4.
    - VIEWER: Read-only access to market data and positions.
    - OPERATOR: Can execute trades and manage strategies.
    - ADMIN: Full system access including user management.
    """

    AI_DELEGATE = "ai_delegate"
    VIEWER = "viewer"
    OPERATOR = "operator"
    ADMIN = "admin"
