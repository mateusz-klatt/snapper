"""WebSocket helper functions for topic and permission management.

This module provides utility functions for managing WebSocket connections
including topic filtering, role-based access control, and origin validation.
"""

from collections.abc import Iterable

from fastapi import WebSocket

from snapper.auth.domain.roles import UserRole
from snapper.config.app import AppSettings
from snapper.interface.websocket.schemas import WSAuthFailedResponse
from snapper.messaging.topics.schemas import get_topics_by_category

__all__ = [
    "get_allowed_topics_for_role",
    "has_trading_permission",
    "role_allowed_categories",
    "filter_topics",
    "determine_topic_category",
    "build_allowed_origins",
    "validate_origin",
]


def build_allowed_origins(settings: AppSettings, server_port: int = 8000) -> set[str]:
    """Build set of allowed CORS origins from settings.

    Includes default localhost origins plus configured UI origin and session domain.

    Args:
        settings: Application settings.
        server_port: Server port for localhost origin.

    Returns:
        Set of allowed origin URLs.
    """
    allowed_origins: set[str] = {
        f"http://localhost:{server_port}",
        "http://localhost:8000",
        "http://localhost:3000",
        "https://holzera.klatt.ie",
    }
    try:
        configured_origin = settings.ui_origin
        if configured_origin:
            allowed_origins.add(configured_origin.rstrip("/"))
    except RuntimeError:
        pass
    try:
        session_domain = settings.session_domain
        if session_domain:
            session_domain = session_domain.strip()
            if session_domain.startswith("http"):
                allowed_origins.add(session_domain.rstrip("/"))
            else:
                allowed_origins.add(f"https://{session_domain}".rstrip("/"))
                allowed_origins.add(f"http://{session_domain}".rstrip("/"))
    except RuntimeError:
        pass
    return {origin.rstrip("/") for origin in allowed_origins if origin}


async def validate_origin(websocket: WebSocket, allowed_origins: set[str]) -> bool:
    """Validate WebSocket connection origin header.

    Sends auth_failed and closes connection if origin is not allowed.

    Args:
        websocket: The WebSocket connection.
        allowed_origins: Set of allowed origin URLs.

    Returns:
        True if origin is valid, False if rejected.
    """
    origin = (websocket.headers.get("origin") or "").rstrip("/")
    if origin and origin not in allowed_origins:
        auth_failed = WSAuthFailedResponse(reason="origin_forbidden")
        await websocket.send_text(auth_failed.model_dump_json())
        await websocket.close(code=4403, reason="Origin not allowed")
        return False
    return True


def role_allowed_categories(role: UserRole) -> set[str]:
    """Get topic categories allowed for a user role.

    Args:
        role: User role to check.

    Returns:
        Set of allowed category names.
    """
    if role == UserRole.VIEWER:
        return {"market", "system"}
    if role == UserRole.OPERATOR:
        return {"market", "system", "trade", "strategy"}
    if role == UserRole.ADMIN:
        return {"market", "system", "trade", "strategy"}
    return {"market", "system"}


def get_allowed_topics_for_role(role: UserRole) -> list[str]:
    """Get list of topic names allowed for a user role.

    Args:
        role: User role to check.

    Returns:
        Sorted list of allowed topic names.
    """
    allowed_categories = role_allowed_categories(role)
    topics: list[str] = []
    for category in allowed_categories:
        category_topics = get_topics_by_category(category)
        topics.extend(category_topics.keys())
    return sorted(set(topics))


def filter_topics(
    topics: Iterable[str],
    allowed_topics: set[str],
    allowed_categories: set[str],
) -> tuple[list[str], list[str]]:
    """Filter topics into allowed and denied lists.

    Args:
        topics: Topics to filter.
        allowed_topics: Explicitly allowed topic names.
        allowed_categories: Allowed topic categories.

    Returns:
        Tuple of (allowed_topics, denied_topics) lists.
    """
    allowed: list[str] = []
    denied: list[str] = []
    seen: set[str] = set()
    for raw_topic in topics:
        topic = str(raw_topic)
        if not topic or topic in seen:
            continue
        seen.add(topic)
        if topic in allowed_topics:
            allowed.append(topic)
            continue
        category = determine_topic_category(topic)
        if category and category in allowed_categories:
            allowed.append(topic)
        else:
            denied.append(topic)
    return allowed, denied


def determine_topic_category(topic: str) -> str | None:
    """Determine category from topic name.

    Uses prefix mapping for dotted names (e.g., 'market.BTCUSD.tick')
    or keyword mapping for simple names (e.g., 'tick').
    Supports two-level prefixes for orders.commands and orders.events.

    Args:
        topic: Topic name to categorize.

    Returns:
        Category name or None if unknown.
    """
    prefix_map = {
        "market": "market",
        "orders.commands": "trade",
        "orders.events": "trade",
        "signals": "strategy",
        "strategy": "strategy",
        "system": "system",
        "admin": "admin",
    }
    parts = topic.split(".")
    if len(parts) >= 2:
        two_level = f"{parts[0]}.{parts[1]}"
        if two_level in prefix_map:
            return prefix_map[two_level]
        return prefix_map.get(parts[0])
    category_map = {
        "bar": "market",
        "tick": "market",
        "signal": "strategy",
        "order": "trade",
        "fill": "trade",
        "heartbeat": "system",
    }
    result = prefix_map.get(topic)
    if result is not None:
        return result
    return category_map.get(topic)


def has_trading_permission(role: UserRole) -> bool:
    """Check if a role has trading permissions.

    Args:
        role: User role to check.

    Returns:
        True if role can trade, False otherwise.
    """
    return role in {UserRole.OPERATOR, UserRole.ADMIN}
