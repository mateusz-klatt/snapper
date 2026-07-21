"""REST signal authorization boundary tests for AI researchers."""

from collections.abc import Callable
from typing import cast

import pytest
from fastapi import HTTPException
from fastapi.routing import APIRoute

from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.server.app import _create_candles_signals_router


def test_signals_route_requires_read_signals_and_rejects_researcher() -> None:
    """The signal history route does not accept market-data-only researchers.

    Given: The dependency bound to GET /signals and an AI researcher principal.
    When: The route's authentication dependency checks the principal.
    Then: It rejects with the READ_SIGNALS permission requirement.
    """
    route = next(
        candidate
        for candidate in _create_candles_signals_router().routes
        if isinstance(candidate, APIRoute) and candidate.path == "/signals"
    )
    auth_dependency = next(
        dependency for dependency in route.dependant.dependencies if dependency.name == "_auth"
    )
    assert auth_dependency.call is not None
    permission_checker = cast(
        Callable[[AuthPrincipal], AuthPrincipal],
        auth_dependency.call,
    )
    principal = AuthPrincipal(
        username="researcher-1",
        role=UserRole.AI_RESEARCHER,
        is_active=True,
    )

    with pytest.raises(HTTPException) as exc:
        permission_checker(principal)

    assert exc.value.status_code == 403
    assert exc.value.detail == "Permission 'read:signals' required"
