"""Response presentation for verified legacy iOS authentication decoders.

Client identification selects a response contract only. Authentication,
signed claims, permission ceilings, and persisted accounts remain canonical.
The installed build 30 signature was observed on authentication and WebSocket
requests. Its released decoder rejects auditor while accepting unknown fields.
"""

import re

from fastapi import Request
from fastapi import Response

from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.user import UserProfile

_LEGACY_IOS_USER_AGENT = re.compile(
    r"Snapper/30 CFNetwork/[0-9]+(?:\.[0-9]+)* Darwin/[0-9]+(?:\.[0-9]+)*",
    flags=re.ASCII,
)


def client_response_role(user_agent: str, role: UserRole) -> UserRole:
    """Adapt the auditor display name for the confirmed legacy iOS build.

    Args:
        user_agent: Complete client hint used only for response presentation.
        role: Canonical role already used for authentication and authorization.

    Returns:
        Viewer for a build 30 auditor, otherwise the unchanged canonical role.
    """
    if role is UserRole.AUDITOR and _LEGACY_IOS_USER_AGENT.fullmatch(user_agent) is not None:
        return UserRole.VIEWER
    return role


def set_session_response_headers(response: Response, canonical_role: UserRole) -> None:
    """Retain canonical session identity and prevent caching across client contracts.

    Args:
        response: Outgoing HTTP response.
        canonical_role: Authenticated role before any presentation adaptation.
    """
    response.headers["X-Snapper-Canonical-Role"] = canonical_role.value
    response.headers["Cache-Control"] = "no-store"
    response.headers.append("Vary", "User-Agent")


def session_response_profile(
    request: Request, response: Response, user: UserProfile
) -> UserProfile:
    """Present a supported role name without changing session authority.

    Complete product/build matching excludes browser version hints, malformed
    signatures, and future builds while allowing numeric OS version changes.
    A matching auditor is presented as viewer because released Swift enums
    reject the newer role. Its complete effective permission grant survives.
    The canonical role remains explicit in the response header; old clients
    may consequently show a viewer badge and hide administrative screens.

    Args:
        request: Client request used solely to select response presentation.
        response: Outgoing headers retaining canonical identity and cache policy.
        user: Canonical profile after authenticated capabilities were resolved.

    Returns:
        Original canonical profile or an independent legacy presentation copy.
    """
    set_session_response_headers(response, user.role)
    role = client_response_role(request.headers.get("user-agent", ""), user.role)
    if role is not user.role:
        return user.model_copy(update={"role": role})
    return user
