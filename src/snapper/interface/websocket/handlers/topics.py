"""WebSocket topic suggestions handler.

This module handles topic autocomplete requests, providing filtered
suggestions based on user role and search prefix.
"""

from fastapi import WebSocket

from snapper.auth.domain.roles import UserRole
from snapper.interface.websocket.connection_manager import WebSocketConnectionManager
from snapper.interface.websocket.helpers import determine_topic_category
from snapper.interface.websocket.helpers import get_allowed_topics_for_role
from snapper.interface.websocket.helpers import role_allowed_categories
from snapper.interface.websocket.schemas import WSGetTopicSuggestionsRequest
from snapper.interface.websocket.schemas import WSTopicSuggestionsResponse
from snapper.messaging.topics.schemas import get_all_topic_names

__all__ = [
    "handle_get_topic_suggestions",
]


async def handle_get_topic_suggestions(
    websocket: WebSocket,
    _manager: WebSocketConnectionManager,
    message: WSGetTopicSuggestionsRequest,
    role: UserRole,
) -> None:
    """Handle topic suggestions request from client.

    Returns filtered list of topics matching the prefix that the user
    has permission to access.

    Args:
        websocket: The WebSocket connection.
        _manager: WebSocket connection manager (reserved for interface compatibility).
        message: The suggestions request with prefix.
        role: User's role for permission filtering.
    """
    prefix = message.prefix
    all_topics = get_all_topic_names()
    suggestions = [t for t in all_topics if t.lower().startswith(prefix.lower())]
    allowed_topics = set(get_allowed_topics_for_role(role))
    allowed_categories = role_allowed_categories(role)
    filtered = [
        suggestion
        for suggestion in suggestions
        if suggestion in allowed_topics
        or determine_topic_category(suggestion) in allowed_categories
    ]
    response = WSTopicSuggestionsResponse(
        prefix=prefix,
        suggestions=filtered,
        session_id=_manager.tracker.session_id,
        sequence_id=_manager.tracker.next_sequence("control"),
    )
    await websocket.send_text(response.model_dump_json())
