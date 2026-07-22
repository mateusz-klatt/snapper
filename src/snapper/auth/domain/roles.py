"""User role definitions module.

This module defines the user role enumeration for the authentication
and authorization system.
"""

from enum import StrEnum


class UserRole(StrEnum):
    """User role enumeration.

    Roles are stable names for permission sets. They may be used as user
    management domain values and display labels, while authorization
    decisions are derived from ``ROLE_PERMISSIONS`` and effective token
    permissions.

    AI_RESEARCHER: Research-only AI integration user that can observe
      market data and persist market views without signal or trading authority.
    AI_REVIEWER: Review-only AI integration user that can observe the
      delegate read surface and submit AI review decisions without
      order execution or position-management authority.
    AI_DELEGATE: Elevated permission set for AI-integration users
      (observe market/signals/orders, submit/cancel trades via MCP).
    VIEWER: Read-only operator with the complete operator read surface.
    OPERATOR: Can execute trades and manage strategies.
    ADMIN: Full system access including user management.
    """

    AI_RESEARCHER = "ai_researcher"
    AI_REVIEWER = "ai_reviewer"
    AI_DELEGATE = "ai_delegate"
    VIEWER = "viewer"
    OPERATOR = "operator"
    ADMIN = "admin"


AI_REVIEW_PRINCIPAL_ROLES: set[UserRole] = {
    UserRole.AI_REVIEWER,
    UserRole.AI_DELEGATE,
}
"""Roles backed by the shared AI delegate operational lifecycle."""
