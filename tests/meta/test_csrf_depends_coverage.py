"""Meta-test: every state-changing FastAPI route has CSRF protection.

CSRF protection in this project is wired as a per-route ``Depends``
injection (``validate_csrf_token``), NOT global middleware. A
state-changing route added without that dependency is a silent CSRF
gap — the cookie-authenticated browser session can be tricked into
executing it from a foreign origin.

This meta-test walks every route registered on the FastAPI app and
asserts each ``POST``/``PUT``/``DELETE``/``PATCH`` route either:

* Has ``validate_csrf_token`` somewhere in its dependency tree, or
* Is on the explicit ``CSRF_EXEMPT`` allowlist (auth bootstrap routes
  that have no session yet, Bearer-token sub-apps where CSRF is
  irrelevant, etc.).

A route added without protection AND not on the allowlist fails the
test loud — preventing a Sonnet-style "13+ injection points" audit
finding from sliding into the codebase.
"""

import inspect
from collections.abc import Iterable
from typing import Final

from fastapi import FastAPI
from fastapi.routing import APIRoute

from snapper.auth.dependencies import validate_csrf_token
from snapper.server.app import create_app

_STATE_CHANGING_METHODS: Final[frozenset[str]] = frozenset({"POST", "PUT", "DELETE", "PATCH"})

CSRF_EXEMPT: Final[frozenset[tuple[str, str]]] = frozenset(
    {
        ("POST", "/api/auth/login"),
        ("POST", "/api/auth/logout"),
        ("POST", "/api/auth/refresh"),
        ("POST", "/api/auth/ws_token"),
    }
)


def _route_has_csrf_dependency(route: APIRoute) -> bool:
    """Return ``True`` when ``validate_csrf_token`` appears in the route's dependants.

    FastAPI builds a tree of :class:`fastapi.dependencies.models.Dependant`
    objects on every ``APIRoute``; the root carries top-level
    ``Depends(...)`` injections plus the parameter-derived ones, and
    each one's nested ``dependencies`` list expands to the full
    transitive graph. CSRF is registered as a top-level
    ``Depends(validate_csrf_token)`` on every state-changing endpoint
    we want covered.
    """
    pending = list(route.dependant.dependencies)
    while pending:
        dependant = pending.pop()
        if dependant.call is validate_csrf_token:
            return True
        pending.extend(dependant.dependencies)
    return False


def _state_changing_routes(app: FastAPI) -> Iterable[APIRoute]:
    """Yield each main-app ``APIRoute`` whose methods include a state-changer.

    Skips mounted sub-apps (e.g. the MCP sub-app at ``/api/mcp/*``)
    and routes without HTTP methods (e.g. WebSocket endpoints).
    """
    for route in app.routes:
        if not isinstance(route, APIRoute):
            continue
        methods = route.methods or set()
        if methods & _STATE_CHANGING_METHODS:
            yield route


class TestCsrfDependsCoverage:
    """Every state-changing route is CSRF-protected or explicitly allowlisted."""

    def test_validate_csrf_token_dependency_present_or_route_allowlisted(self) -> None:
        """Walk ``app.routes`` and assert each state-changer has CSRF or is exempt.

        On regression the failure message lists every offending
        ``(method, path)`` pair so the developer who added the route
        sees exactly what to fix.
        """
        app = create_app()
        violations: list[str] = []
        for route in _state_changing_routes(app):
            for method in sorted(route.methods or set()):
                if method not in _STATE_CHANGING_METHODS:
                    continue
                if (method, route.path) in CSRF_EXEMPT:
                    continue
                if _route_has_csrf_dependency(route):
                    continue
                violations.append(f"{method} {route.path}")
        assert not violations, (
            "State-changing routes missing validate_csrf_token "
            "(add Depends(validate_csrf_token) or extend CSRF_EXEMPT in "
            "tests/meta/test_csrf_depends_coverage.py if Bearer-only): "
            + "\n  ".join(["", *violations])
        )

    def test_allowlist_entries_are_real_routes(self) -> None:
        """Every ``CSRF_EXEMPT`` entry MUST resolve to a registered route.

        Prevents an exempt entry from rotting away after the underlying
        route is renamed or deleted.
        """
        app = create_app()
        registered: set[tuple[str, str]] = set()
        for route in app.routes:
            if not isinstance(route, APIRoute):
                continue
            for method in route.methods or set():
                registered.add((method, route.path))
        missing = [
            f"{method} {path}"
            for method, path in sorted(CSRF_EXEMPT)
            if (method, path) not in registered
        ]
        assert not missing, (
            "CSRF_EXEMPT entries not found among registered routes "
            "(remove from the allowlist if intentional): " + ", ".join(missing)
        )

    def test_validate_csrf_token_is_not_a_class_attribute(self) -> None:
        """Sanity-check that ``validate_csrf_token`` is a callable function.

        The detection in :func:`_route_has_csrf_dependency` matches by
        identity (``dependant.call is validate_csrf_token``); if a
        future refactor moves CSRF into a class with ``__call__`` the
        ``call`` slot would point at the bound method, and the meta-
        test must be updated. This guard catches that drift early.
        """
        guidance = "update the meta-test identity check"
        message = f"validate_csrf_token is no longer a function — {guidance}"
        assert inspect.isfunction(validate_csrf_token), message
